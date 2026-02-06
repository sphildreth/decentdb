"""Import PostgreSQL dump files into DecentDB.

Supports plain SQL dumps and gzipped dumps.
Parses CREATE TABLE, ALTER TABLE (constraints), CREATE INDEX, and COPY statements.
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import json
import os
import re
import sys
from collections import defaultdict, deque
from typing import Any, Iterator, Sequence

import decentdb


@dataclasses.dataclass(frozen=True)
class PgColumn:
    name: str
    pg_type: str
    not_null: bool
    has_default: bool
    default_value: str | None = None


@dataclasses.dataclass(frozen=True)
class PgForeignKey:
    from_column: str
    to_table: str
    to_column: str
    on_delete: str | None = None


@dataclasses.dataclass(frozen=True)
class PgIndex:
    name: str
    table: str
    columns: list[str]
    unique: bool


@dataclasses.dataclass(frozen=True)
class SkippedIndex:
    name: str
    table: str
    reason: str


@dataclasses.dataclass(frozen=True)
class PgTable:
    name: str
    schema: str
    columns: list[PgColumn]
    primary_key: list[str] | None = None
    foreign_keys: list[PgForeignKey] = dataclasses.field(default_factory=list)
    indexes: list[PgIndex] = dataclasses.field(default_factory=list)
    skipped_indexes: list[SkippedIndex] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ConversionReport:
    pg_dump_path: str
    decentdb_path: str
    identifier_case: str = "lower"
    table_name_map: dict[str, str] = dataclasses.field(default_factory=dict)
    column_name_map: dict[str, dict[str, str]] = dataclasses.field(default_factory=dict)
    tables: list[str] = dataclasses.field(default_factory=list)
    rows_copied: dict[str, int] = dataclasses.field(default_factory=dict)
    indexes_created: list[str] = dataclasses.field(default_factory=list)
    unique_columns_added: list[str] = dataclasses.field(default_factory=list)
    skipped_indexes: list[SkippedIndex] = dataclasses.field(default_factory=list)
    warnings: list[str] = dataclasses.field(default_factory=list)
    unsupported_types: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    skipped_tables: list[str] = dataclasses.field(default_factory=list)
    rows_skipped: int = 0


class ConversionError(RuntimeError):
    pass


def _normalize_ident(name: str, *, identifier_case: str) -> str:
    if identifier_case == "preserve":
        return name
    if identifier_case == "lower":
        return name.lower()
    raise ValueError(f"Unknown identifier_case: {identifier_case}")


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _map_pg_type_to_decentdb(pg_type: str) -> str:
    """Map PostgreSQL type to DecentDB type.

    DecentDB supports: INT64, BOOL, FLOAT64, TEXT, BLOB
    """
    t = pg_type.lower().strip()

    # Remove type modifiers like (255) from varchar(255)
    base_type = re.sub(r"\(.*\)", "", t).strip()

    # Integer types
    if base_type in (
        "integer",
        "int",
        "int4",
        "smallint",
        "int2",
        "bigint",
        "int8",
        "serial",
        "bigserial",
    ):
        return "INT64"

    # Boolean
    if base_type in ("boolean", "bool"):
        return "BOOL"

    # Floating point
    if base_type in ("real", "float4", "double precision", "float8", "float"):
        return "FLOAT64"

    # Numeric/Decimal - store as TEXT to preserve precision
    if base_type in ("numeric", "decimal"):
        return "TEXT"

    # Binary data
    if base_type in ("bytea",):
        return "BLOB"

    # Character types
    if base_type in (
        "character varying",
        "varchar",
        "character",
        "char",
        "text",
        "name",
    ):
        return "TEXT"

    # Date/Time types - store as TEXT
    if base_type in (
        "timestamp",
        "timestamptz",
        "timestamp with time zone",
        "timestamp without time zone",
        "date",
        "time",
        "timetz",
        "time with time zone",
        "time without time zone",
        "interval",
    ):
        return "TEXT"

    # UUID - store as TEXT
    if base_type == "uuid":
        return "TEXT"

    # JSON types - store as TEXT
    if base_type in ("json", "jsonb"):
        return "TEXT"

    # Arrays - store as TEXT (PostgreSQL array literal format)
    if base_type.endswith("[]"):
        return "TEXT"

    # Network types - store as TEXT
    if base_type in ("inet", "cidr", "macaddr", "macaddr8"):
        return "TEXT"

    # Text search types - store as TEXT
    if base_type in ("tsvector", "tsquery"):
        return "TEXT"

    # XML - store as TEXT
    if base_type == "xml":
        return "TEXT"

    # Bit strings - store as TEXT
    if base_type in ("bit", "bit varying", "varbit"):
        return "TEXT"

    # Money - store as TEXT
    if base_type == "money":
        return "TEXT"

    # OID and other system types - store as TEXT
    if base_type in ("oid", "regclass", "regtype", "regproc"):
        return "TEXT"

    # Default to TEXT for unknown types
    return "TEXT"


def _is_unsupported_type(pg_type: str) -> bool:
    """Check if a PostgreSQL type might lose information when converted."""
    t = pg_type.lower().strip()
    base_type = re.sub(r"\(.*\)", "", t).strip()

    unsupported = {
        "numeric",
        "decimal",  # Precision loss
        "timestamp",
        "timestamptz",
        "timestamp with time zone",
        "timestamp without time zone",
        "date",
        "time",
        "timetz",
        "interval",
        "bytea",  # Binary data - might need special handling
        "json",
        "jsonb",
        "uuid",
        "inet",
        "cidr",
        "macaddr",
        "macaddr8",
        "tsvector",
        "tsquery",
        "xml",
        "bit",
        "bit varying",
        "varbit",
        "money",
        "oid",
        "regclass",
        "regtype",
        "regproc",
    }

    if base_type in unsupported:
        return True

    # Arrays
    if base_type.endswith("[]"):
        return True

    return False


class PgDumpParser:
    """Parser for PostgreSQL dump files."""

    def __init__(self, file_path: str, *, progress=None, console=None):
        self.file_path = file_path
        self.tables: dict[str, PgTable] = {}
        self.indexes: list[PgIndex] = []
        self.foreign_keys: list[tuple[str, PgForeignKey]] = []  # (table_name, fk)
        self.copy_statements: dict[
            str, tuple[list[str], list[tuple]]
        ] = {}  # table_name -> (columns, rows)
        self._current_table: str | None = None
        self._copy_buffer: list[tuple] = []
        self._copy_columns: list[str] = []
        self._in_copy = False
        self._progress = progress
        self._console = console
        self._parse_task = None

    def _get_file_size(self) -> int:
        """Get file size for progress tracking."""
        try:
            return os.path.getsize(self.file_path)
        except OSError:
            return 0

    def _open_file(self) -> Iterator[str]:
        """Open file, handling gzip compression."""
        if self.file_path.endswith(".gz"):
            with gzip.open(
                self.file_path, "rt", encoding="utf-8", errors="replace"
            ) as f:
                for line in f:
                    yield line
        else:
            with open(self.file_path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    yield line

    def _parse_column_def(self, col_def: str) -> PgColumn:
        """Parse a column definition from CREATE TABLE."""
        # Match: "ColumnName" type [NOT NULL] [DEFAULT ...]
        # Handle quoted identifiers
        match = re.match(r'^"([^"]+)"\s+(.+)$', col_def.strip())
        if not match:
            # Try unquoted
            match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s+(.+)$", col_def.strip())

        if not match:
            raise ConversionError(f"Cannot parse column definition: {col_def}")

        col_name = match.group(1)
        rest = match.group(2).strip()

        # Check for DEFAULT
        has_default = False
        default_value = None
        default_match = re.search(r"\s+DEFAULT\s+(.+)$", rest, re.IGNORECASE)
        if default_match:
            has_default = True
            default_value = default_match.group(1).strip()
            rest = rest[: default_match.start()].strip()

        # Check for NOT NULL
        not_null = False
        if re.search(r"\s+NOT\s+NULL\s*$", rest, re.IGNORECASE):
            not_null = True
            rest = re.sub(r"\s+NOT\s+NULL\s*$", "", rest, flags=re.IGNORECASE).strip()

        # Check for NULL (explicit)
        if re.search(r"\s+NULL\s*$", rest, re.IGNORECASE):
            rest = re.sub(r"\s+NULL\s*$", "", rest, flags=re.IGNORECASE).strip()

        # Remove any remaining constraints (PRIMARY KEY, UNIQUE, REFERENCES, CHECK)
        # These are usually at the end
        rest = re.split(
            r"\s+(?:PRIMARY|UNIQUE|REFERENCES|CHECK)\s+", rest, flags=re.IGNORECASE
        )[0].strip()

        # The rest is the type
        pg_type = rest

        return PgColumn(
            name=col_name,
            pg_type=pg_type,
            not_null=not_null,
            has_default=has_default,
            default_value=default_value,
        )

    def _parse_create_table(self, line: str) -> tuple[str, str] | None:
        """Parse CREATE TABLE statement. Returns (schema, table) or None."""
        # Match: CREATE TABLE schema."TableName" (
        # Note: columns are on subsequent lines, so we don't try to match them here
        match = re.match(r'CREATE\s+TABLE\s+(\w+)\."([^"]+)"\s*\(', line, re.IGNORECASE)
        if not match:
            # Try unquoted table name
            match = re.match(
                r"CREATE\s+TABLE\s+(\w+)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\(",
                line,
                re.IGNORECASE,
            )

        if not match:
            return None

        schema = match.group(1)
        table = match.group(2)

        return schema, table

    def _process_table_columns(
        self, lines: list[str], start_idx: int
    ) -> tuple[list[PgColumn], int]:
        """Process column definitions starting at start_idx. Returns (columns, next_idx)."""
        columns = []
        i = start_idx
        paren_depth = 1  # We start inside the CREATE TABLE (...)

        while i < len(lines) and paren_depth > 0:
            line = lines[i].strip()

            # Count parentheses
            paren_depth += line.count("(") - line.count(")")

            if paren_depth <= 0:
                # End of CREATE TABLE
                # Remove trailing );
                line = line.rstrip(")").rstrip().rstrip(";")
                if line:
                    # Check if it's a table constraint (starts with CONSTRAINT)
                    if not re.match(r"^\s*CONSTRAINT\s+", line, re.IGNORECASE):
                        try:
                            col = self._parse_column_def(line)
                            columns.append(col)
                        except ConversionError:
                            pass  # Skip constraints
                i += 1
                break

            # Remove trailing comma
            line = line.rstrip(",").strip()

            # Skip empty lines and comments
            if not line or line.startswith("--"):
                i += 1
                continue

            # Check if it's a table constraint (CONSTRAINT, PRIMARY KEY, FOREIGN KEY, etc.)
            if re.match(
                r"^(?:CONSTRAINT|PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK)\s+",
                line,
                re.IGNORECASE,
            ):
                i += 1
                continue

            try:
                col = self._parse_column_def(line)
                columns.append(col)
            except ConversionError:
                pass  # Skip if we can't parse

            i += 1

        return columns, i

    def _parse_alter_table_primary_key(
        self, line: str
    ) -> tuple[str, str, list[str]] | None:
        """Parse ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY."""
        # Match: ALTER TABLE [ONLY] schema."TableName" ADD CONSTRAINT "Name" PRIMARY KEY ("Col1", "Col2");
        match = re.match(
            r'ALTER\s+TABLE\s+(?:ONLY\s+)?(\w+)\."?([^"]+)"?\s+ADD\s+CONSTRAINT\s+"?[^"]+"?\s+PRIMARY\s+KEY\s*\(([^)]+)\)',
            line,
            re.IGNORECASE,
        )
        if not match:
            return None

        schema = match.group(1)
        table = match.group(2)
        cols_str = match.group(3)

        # Parse column list
        columns = []
        for col in cols_str.split(","):
            col = col.strip().strip('"')
            if col:
                columns.append(col)

        return schema, table, columns

    def _parse_alter_table_foreign_key(
        self, line: str
    ) -> tuple[str, str, PgForeignKey] | None:
        """Parse ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY."""
        # Match: ALTER TABLE [ONLY] schema."TableName" ADD CONSTRAINT "Name" FOREIGN KEY ("Col")
        #        REFERENCES schema."OtherTable"("OtherCol") [ON DELETE ...];
        match = re.match(
            r'ALTER\s+TABLE\s+(?:ONLY\s+)?(\w+)\."?([^"]+)"?\s+ADD\s+CONSTRAINT\s+"?[^"]+"?\s+FOREIGN\s+KEY\s*\("?([^"]+)"?\)\s+'
            r'REFERENCES\s+(?:\w+\.)?"?([^"]+)"?\s*\("?([^"]+)"?\)'
            r"(?:\s+ON\s+DELETE\s+(\w+))?",
            line,
            re.IGNORECASE,
        )
        if not match:
            return None

        schema = match.group(1)
        table = match.group(2)
        from_col = match.group(3).strip('"')
        to_table = match.group(4).strip('"')
        to_col = match.group(5).strip('"')
        on_delete = match.group(6)

        fk = PgForeignKey(
            from_column=from_col,
            to_table=to_table,
            to_column=to_col,
            on_delete=on_delete.upper() if on_delete else None,
        )

        return schema, table, fk

    def _parse_create_index(self, line: str) -> PgIndex | None:
        """Parse CREATE INDEX statement."""
        # Match: CREATE [UNIQUE] INDEX "Name" ON schema."Table" USING method ("Col1", "Col2");
        match = re.match(
            r'CREATE\s+(UNIQUE\s+)?INDEX\s+"?([^"]+)"?\s+ON\s+(?:\w+\.)?"?([^"]+)"?(?:\s+USING\s+\w+)?\s*\(([^)]+)\)',
            line,
            re.IGNORECASE,
        )
        if not match:
            return None

        unique = bool(match.group(1))
        idx_name = match.group(2)
        table = match.group(3)
        cols_str = match.group(4)

        # Parse column list
        columns = []
        for col in cols_str.split(","):
            col = col.strip()
            # Remove any ASC/DESC/COLLATE before stripping quotes
            col = re.sub(
                r"\s+(?:ASC|DESC|COLLATE\s+\w+).*", "", col, flags=re.IGNORECASE
            ).strip().strip('"')
            if col:
                columns.append(col)

        return PgIndex(name=idx_name, table=table, columns=columns, unique=unique)

    def _parse_copy_statement(self, line: str) -> tuple[str, str, list[str]] | None:
        """Parse COPY statement. Returns (schema, table, columns) or None."""
        # Match: COPY schema."Table" ("Col1", "Col2") FROM stdin;
        match = re.match(
            r'COPY\s+(\w+)\."?([^"]+)"?\s*\(([^)]+)\)\s+FROM\s+stdin',
            line,
            re.IGNORECASE,
        )
        if not match:
            return None

        schema = match.group(1)
        table = match.group(2)
        cols_str = match.group(3)

        # Parse column list
        columns = []
        for col in cols_str.split(","):
            col = col.strip().strip('"')
            if col:
                columns.append(col)

        return schema, table, columns

    def _parse_copy_line(self, line: str) -> tuple | None:
        """Parse a line from COPY data. Returns tuple of values or None if end marker."""
        line = line.rstrip("\n")

        # End of COPY data
        if line == "\\.":
            return None

        # Parse tab-separated values
        values = []
        i = 0
        current = []

        while i < len(line):
            char = line[i]

            if char == "\t":
                # End of field
                val = "".join(current)
                values.append(self._unescape_copy_value(val))
                current = []
                i += 1
            elif char == "\\":
                # Escape sequence
                if i + 1 < len(line):
                    next_char = line[i + 1]
                    if next_char == "n":
                        current.append("\n")
                        i += 2
                    elif next_char == "t":
                        current.append("\t")
                        i += 2
                    elif next_char == "r":
                        current.append("\r")
                        i += 2
                    elif next_char == "b":
                        current.append("\b")
                        i += 2
                    elif next_char == "f":
                        current.append("\f")
                        i += 2
                    elif next_char == "\\":
                        current.append("\\")
                        i += 2
                    elif next_char == ".":
                        # \. is end marker, shouldn't appear in data
                        current.append(".")
                        i += 2
                    elif next_char == "N":
                        # \N is the NULL marker - preserve for _unescape_copy_value
                        current.append("\\")
                        current.append("N")
                        i += 2
                    else:
                        current.append(next_char)
                        i += 2
                else:
                    i += 1
            else:
                current.append(char)
                i += 1

        # Add last field
        val = "".join(current)
        values.append(self._unescape_copy_value(val))

        return tuple(values)

    def _unescape_copy_value(self, val: str) -> Any:
        """Unescape and convert a COPY value."""
        # NULL is represented as \N
        if val == "\\N":
            return None

        # Boolean values
        if val == "t":
            return True
        if val == "f":
            return False

        # Try integer
        try:
            return int(val)
        except ValueError:
            pass

        # Try float
        try:
            return float(val)
        except ValueError:
            pass

        # Return as string
        return val

    def parse(self) -> None:
        """Parse the entire dump file."""
        # Show initial progress message
        if self._console is not None:
            self._console.print(f"[dim]Reading {self.file_path}...[/dim]")

        lines = list(self._open_file())
        total_lines = len(lines)

        # Setup progress for parsing phase
        if self._progress is not None:
            self._parse_task = self._progress.add_task(
                "Parse dump file", total=total_lines
            )

        i = 0
        last_update = 0
        update_interval = max(1, total_lines // 100)  # Update ~100 times

        while i < len(lines):
            line = lines[i]

            # Skip empty lines and comments
            if not line.strip() or line.strip().startswith("--"):
                i += 1
                continue

            # CREATE TABLE
            if re.match(r"CREATE\s+TABLE\s+", line, re.IGNORECASE):
                result = self._parse_create_table(line)
                if result:
                    schema, table = result
                    columns, i = self._process_table_columns(lines, i + 1)

                    pg_table = PgTable(name=table, schema=schema, columns=columns)
                    self.tables[table] = pg_table
                    continue

            # ALTER TABLE ... PRIMARY KEY / FOREIGN KEY
            if re.match(r"ALTER\s+TABLE\s+", line, re.IGNORECASE):
                # Accumulate lines until we have a complete statement (ends with ;)
                stmt_lines = [line]
                while i + 1 < len(lines) and not stmt_lines[-1].strip().endswith(";"):
                    i += 1
                    next_line = lines[i]
                    if next_line.strip() and not next_line.strip().startswith("--"):
                        stmt_lines.append(next_line)

                full_stmt = " ".join(line.strip() for line in stmt_lines)

                pk_result = self._parse_alter_table_primary_key(full_stmt)
                if pk_result:
                    schema, table, pk_cols = pk_result
                    if table in self.tables:
                        self.tables[table] = dataclasses.replace(
                            self.tables[table], primary_key=pk_cols
                        )
                    i += 1
                    continue

                fk_result = self._parse_alter_table_foreign_key(full_stmt)
                if fk_result:
                    schema, table, fk = fk_result
                    self.foreign_keys.append((table, fk))
                    i += 1
                    continue

            # CREATE INDEX
            if re.match(r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+", line, re.IGNORECASE):
                idx = self._parse_create_index(line)
                if idx:
                    self.indexes.append(idx)
                i += 1
                continue

            # COPY
            copy_result = self._parse_copy_statement(line)
            if copy_result:
                schema, table, columns = copy_result
                self._in_copy = True
                self._copy_columns = columns
                self._copy_buffer = []

                # Read data lines until \.
                i += 1
                while i < len(lines):
                    data_line = lines[i]
                    parsed = self._parse_copy_line(data_line)
                    if parsed is None:
                        # End of COPY
                        break
                    self._copy_buffer.append(parsed)
                    i += 1

                self.copy_statements[table] = (self._copy_columns, self._copy_buffer)
                self._in_copy = False
                i += 1

                # Update progress after COPY block
                if (
                    self._progress is not None
                    and self._parse_task is not None
                    and i - last_update >= update_interval
                ):
                    self._progress.update(self._parse_task, completed=i)
                    last_update = i

                continue

            i += 1

            # Update progress periodically
            if (
                self._progress is not None
                and self._parse_task is not None
                and i - last_update >= update_interval
            ):
                self._progress.update(self._parse_task, completed=i)
                last_update = i

        # Final progress update
        if self._progress is not None and self._parse_task is not None:
            self._progress.update(self._parse_task, completed=total_lines)

        # Associate foreign keys and indexes with tables
        for table_name, fk in self.foreign_keys:
            if table_name in self.tables:
                existing_fks = list(self.tables[table_name].foreign_keys)
                existing_fks.append(fk)
                self.tables[table_name] = dataclasses.replace(
                    self.tables[table_name], foreign_keys=existing_fks
                )

        # Associate indexes with tables
        for idx in self.indexes:
            if idx.table in self.tables:
                existing_indexes = list(self.tables[idx.table].indexes)
                existing_indexes.append(idx)
                self.tables[idx.table] = dataclasses.replace(
                    self.tables[idx.table], indexes=existing_indexes
                )


def _build_name_maps(
    tables: list[PgTable], *, identifier_case: str
) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """Build mappings from source to destination names."""
    table_map: dict[str, str] = {}
    used_tables: dict[str, str] = {}

    for t in tables:
        dst = _normalize_ident(t.name, identifier_case=identifier_case)
        if dst in used_tables and used_tables[dst] != t.name:
            raise ConversionError(
                f"Table name collision after normalization: '{t.name}' and '{used_tables[dst]}' -> '{dst}'"
            )
        used_tables[dst] = t.name
        table_map[t.name] = dst

    col_map: dict[str, dict[str, str]] = {}
    for t in tables:
        used_cols: dict[str, str] = {}
        per: dict[str, str] = {}
        for c in t.columns:
            dst = _normalize_ident(c.name, identifier_case=identifier_case)
            if dst in used_cols and used_cols[dst] != c.name:
                raise ConversionError(
                    f"Column name collision after normalization: '{t.name}.{c.name}' and '{t.name}.{used_cols[dst]}' -> '{dst}'"
                )
            used_cols[dst] = c.name
            per[c.name] = dst
        col_map[t.name] = per

    return table_map, col_map


def _validate_supported(table: PgTable) -> str | None:
    """Validate that a table can be imported. Returns error message if not supported, None if OK."""
    # Check foreign keys - multiple FKs from one column
    fk_by_from: dict[str, list[PgForeignKey]] = defaultdict(list)
    for fk in table.foreign_keys:
        fk_by_from[fk.from_column].append(fk)

    for from_col, fks in fk_by_from.items():
        if len(fks) > 1:
            return f"Multiple foreign key targets from one column not supported: {from_col}"

    return None


def _toposort_tables(tables: list[PgTable]) -> list[PgTable]:
    """Sort tables by foreign key dependencies."""
    by_name = {t.name: t for t in tables}
    deps: dict[str, set[str]] = {t.name: set() for t in tables}
    rev: dict[str, set[str]] = {t.name: set() for t in tables}

    # Track self-referencing FKs to handle them separately
    self_refs: dict[str, list[PgForeignKey]] = {}

    for t in tables:
        for fk in t.foreign_keys:
            # Skip self-referencing FKs - they don't affect ordering
            if fk.to_table == t.name:
                if t.name not in self_refs:
                    self_refs[t.name] = []
                self_refs[t.name].append(fk)
                continue
            if fk.to_table in by_name:
                deps[t.name].add(fk.to_table)
                rev[fk.to_table].add(t.name)

    indeg = {name: len(d) for name, d in deps.items()}
    q = deque([name for name, d in indeg.items() if d == 0])
    out: list[str] = []

    while q:
        n = q.popleft()
        out.append(n)
        for child in rev[n]:
            indeg[child] -= 1
            if indeg[child] == 0:
                q.append(child)

    if len(out) != len(tables):
        cycle = [name for name, d in indeg.items() if d > 0]
        raise ConversionError(
            f"Foreign key cycle detected among tables: {', '.join(sorted(cycle))}"
        )

    # Add self-referencing FKs back to tables
    for table_name, fks in self_refs.items():
        if table_name in by_name:
            existing_fks = list(by_name[table_name].foreign_keys)
            for fk in fks:
                if fk not in existing_fks:
                    existing_fks.append(fk)
            by_name[table_name] = dataclasses.replace(
                by_name[table_name], foreign_keys=existing_fks
            )

    return [by_name[name] for name in out]


def _create_table(
    conn: decentdb.Connection,
    table: PgTable,
    *,
    table_name_map: dict[str, str],
    column_name_map: dict[str, dict[str, str]],
) -> None:
    """Create a table in DecentDB."""
    fk_map: dict[str, PgForeignKey] = {fk.from_column: fk for fk in table.foreign_keys}
    pk_set = set(table.primary_key) if table.primary_key else set()

    dst_table = table_name_map[table.name]
    col_defs: list[str] = []
    for col in table.columns:
        dst_col = column_name_map[table.name][col.name]

        decent_type = _map_pg_type_to_decentdb(col.pg_type)
        parts: list[str] = [_quote_ident(dst_col), decent_type]

        # Primary key (single-column only; composite PKs use table-level constraint)
        if col.name in pk_set and len(pk_set) == 1:
            parts.append("PRIMARY KEY")
        else:
            # Not null (composite PK columns are implicitly NOT NULL)
            if col.not_null or col.name in pk_set:
                parts.append("NOT NULL")

        # Foreign key - added inline since tables are created in dependency order
        # Skip self-referencing FKs - they'll be added later if possible
        fk = fk_map.get(col.name)
        if fk is not None and fk.to_table != table.name:
            dst_to_table = table_name_map.get(fk.to_table, fk.to_table)
            dst_to_col = column_name_map.get(fk.to_table, {}).get(
                fk.to_column, fk.to_column
            )
            parts.append(
                f"REFERENCES {_quote_ident(dst_to_table)}({_quote_ident(dst_to_col)})"
            )

        col_defs.append(" ".join(parts))

    # Composite primary key as table-level constraint
    if table.primary_key and len(pk_set) > 1:
        pk_cols = ", ".join(
            _quote_ident(column_name_map[table.name][c]) for c in table.primary_key
        )
        col_defs.append(f"PRIMARY KEY ({pk_cols})")

    sql = "CREATE TABLE " + _quote_ident(dst_table) + " (" + ", ".join(col_defs) + ")"
    conn.execute(sql)


def _add_foreign_keys(
    conn: decentdb.Connection,
    table: PgTable,
    *,
    table_name_map: dict[str, str],
    column_name_map: dict[str, dict[str, str]],
) -> None:
    """Add foreign key constraints via ALTER TABLE."""
    for fk in table.foreign_keys:
        dst_table = table_name_map[table.name]
        dst_from_col = column_name_map[table.name].get(fk.from_column, fk.from_column)
        dst_to_table = table_name_map.get(fk.to_table, fk.to_table)
        dst_to_col = column_name_map.get(fk.to_table, {}).get(
            fk.to_column, fk.to_column
        )

        sql = (
            f"ALTER TABLE {_quote_ident(dst_table)} "
            f"ADD FOREIGN KEY ({_quote_ident(dst_from_col)}) "
            f"REFERENCES {_quote_ident(dst_to_table)}({_quote_ident(dst_to_col)})"
        )
        conn.execute(sql)


def _copy_table_data(
    *,
    conn: decentdb.Connection,
    table: PgTable,
    columns: list[str],
    rows: list[tuple],
    progress=None,
    update_every: int = 200,
    commit_every: int = 5_000,
    table_name_map: dict[str, str] | None = None,
    column_name_map: dict[str, dict[str, str]] | None = None,
    console=None,
    verbose: bool = False,
) -> tuple[int, int]:
    """Copy data from parsed COPY statement into DecentDB."""
    if table_name_map is None or column_name_map is None:
        raise AssertionError("_copy_table_data requires name maps")

    dst_table = table_name_map[table.name]

    # Map source columns to destination columns
    col_mapping = []
    for col_name in columns:
        if col_name in column_name_map[table.name]:
            dst_col = column_name_map[table.name][col_name]
            col_mapping.append((col_name, dst_col))

    if not col_mapping:
        return 0, 0

    dst_cols = [c[1] for c in col_mapping]
    cols_sql = ", ".join(_quote_ident(c) for c in dst_cols)
    placeholders = ", ".join(["?"] * len(dst_cols))
    insert_sql = (
        f"INSERT INTO {_quote_ident(dst_table)} ({cols_sql}) VALUES ({placeholders})"
    )

    # Build column index mapping for type conversion
    col_idx_map = {}
    for i, (src_col, _) in enumerate(col_mapping):
        for col in table.columns:
            if col.name == src_col:
                col_idx_map[i] = col
                break

    total = len(rows)
    task_id = None
    if progress is not None:
        task_id = progress.add_task(f"Copy {table.name}", total=total)

    if verbose and console is not None:
        console.print(
            f"[dim]Copying {total:,} rows into {table_name_map.get(table.name, table.name)}...[/dim]"
        )

    cur = conn.cursor()
    n = 0
    skipped = 0
    last_logged = 0
    log_interval = max(1, min(10000, total // 10))  # Log every 10% or 10000 rows
    in_tx = False

    if commit_every and commit_every > 0:
        conn.execute("BEGIN")
        in_tx = True

    for row in rows:
        # Convert row values based on column types
        converted = []
        for i, val in enumerate(row):
            if i >= len(col_mapping):
                break

            if val is None:
                converted.append(None)
                continue

            # Get column type for conversion
            col = col_idx_map.get(i)
            if col:
                decent_type = _map_pg_type_to_decentdb(col.pg_type)

                if decent_type == "BOOL":
                    if isinstance(val, bool):
                        converted.append(val)
                    elif isinstance(val, int):
                        converted.append(bool(val))
                    elif val == "t":
                        converted.append(True)
                    elif val == "f":
                        converted.append(False)
                    else:
                        converted.append(bool(val))
                elif decent_type == "INT64":
                    try:
                        converted.append(int(val))
                    except (ValueError, TypeError):
                        converted.append(val)
                elif decent_type == "FLOAT64":
                    try:
                        converted.append(float(val))
                    except (ValueError, TypeError):
                        converted.append(val)
                else:
                    converted.append(str(val) if val is not None else None)
            else:
                converted.append(val)

        try:
            cur.execute(insert_sql, converted)
            n += 1
        except (decentdb.IntegrityError, decentdb.InternalError):
            # Skip rows that violate constraints (e.g., FK violations, missing parent)
            skipped += 1

        if in_tx and commit_every and commit_every > 0 and (n % commit_every == 0):
            cur.close()
            conn.execute("COMMIT")
            conn.execute("BEGIN")
            cur = conn.cursor()

        if (
            progress is not None
            and task_id is not None
            and (n % update_every == 0 or n == total)
        ):
            progress.update(task_id, completed=n)

        # Verbose logging
        if verbose and console is not None and n - last_logged >= log_interval:
            console.print(
                f"  [dim]{table.name}: {n:,}/{total:,} rows ({100 * n // total}%)...[/dim]"
            )
            last_logged = n

    if in_tx:
        cur.close()
        conn.execute("COMMIT")

    if progress is not None and task_id is not None:
        progress.update(task_id, completed=n)

    if verbose and console is not None:
        console.print(
            f"  [dim]{table.name}: completed {n:,} rows ({skipped:,} skipped)[/dim]"
        )

    return n, skipped


def _create_indexes(
    conn: decentdb.Connection,
    table: PgTable,
    *,
    table_name_map: dict[str, str],
    column_name_map: dict[str, dict[str, str]],
    report: ConversionReport | None = None,
) -> None:
    """Create indexes for a table."""
    for idx in table.indexes:
        dst_table = table_name_map.get(idx.table, idx.table)
        dst_cols = [
            _quote_ident(column_name_map.get(idx.table, {}).get(c, c))
            for c in idx.columns
        ]
        dst_idx = _normalize_ident(
            idx.name, identifier_case=report.identifier_case if report else "lower"
        )

        sql = (
            "CREATE INDEX "
            + _quote_ident(dst_idx)
            + " ON "
            + _quote_ident(dst_table)
            + "("
            + ", ".join(dst_cols)
            + ")"
        )
        conn.execute(sql)

        if report is not None:
            report.indexes_created.append(dst_idx)


def report_to_dict(report: ConversionReport) -> dict[str, Any]:
    return {
        "pg_dump_path": report.pg_dump_path,
        "decentdb_path": report.decentdb_path,
        "identifier_case": report.identifier_case,
        "table_name_map": dict(report.table_name_map),
        "column_name_map": {k: dict(v) for k, v in report.column_name_map.items()},
        "tables": list(report.tables),
        "rows_copied": dict(report.rows_copied),
        "indexes_created": list(report.indexes_created),
        "unique_columns_added": list(report.unique_columns_added),
        "skipped_indexes": [dataclasses.asdict(s) for s in report.skipped_indexes],
        "skipped_tables": list(report.skipped_tables),
        "rows_skipped": report.rows_skipped,
        "warnings": list(report.warnings),
        "unsupported_types": dict(report.unsupported_types),
    }


def write_report_json(report: ConversionReport, path: str) -> None:
    payload = json.dumps(
        report_to_dict(report), ensure_ascii=False, indent=2, sort_keys=True
    )
    if path == "-":
        print(payload)
        return
    with open(path, "w", encoding="utf-8") as f:
        f.write(payload)
        f.write("\n")


def convert_pg_dump_to_decentdb(
    *,
    pg_dump_path: str,
    decentdb_path: str,
    overwrite: bool = False,
    show_progress: bool = True,
    identifier_case: str = "lower",
    commit_every: int = 5_000,
    cache_pages: int | None = None,
    cache_mb: int | None = None,
    verbose: bool = False,
) -> ConversionReport:
    """Convert a PostgreSQL dump file to DecentDB."""
    import time

    start_time = time.time()

    if not os.path.exists(pg_dump_path):
        raise FileNotFoundError(pg_dump_path)

    if os.path.exists(decentdb_path):
        if not overwrite:
            raise ConversionError(
                f"Destination already exists: {decentdb_path} (pass overwrite=True to replace)"
            )
        os.remove(decentdb_path)
        if os.path.exists(decentdb_path + "-wal"):
            os.remove(decentdb_path + "-wal")

    # Setup progress early so parsing can show feedback
    progress = None
    console = None
    overall_start_time = None
    if show_progress:
        from rich.console import Console
        from rich.progress import (
            BarColumn,
            Progress,
            SpinnerColumn,
            TaskProgressColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        )
        from rich.progress import Task, TaskID
        from datetime import timedelta

        class TotalElapsedColumn(TimeElapsedColumn):
            """Shows total elapsed time since import started."""

            def __init__(self, start_time):
                super().__init__()
                self.start_time = start_time

            def render(self, task: Task) -> str:
                elapsed = task.get_time() - self.start_time
                return f"[{timedelta(seconds=int(elapsed))}]"

        overall_start_time = time.monotonic()
        console = Console()
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}[/bold]"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TotalElapsedColumn(overall_start_time),
            transient=False,
        )
        progress.start()

    # Parse the dump file
    parser = PgDumpParser(pg_dump_path, progress=progress, console=console)
    parser.parse()

    if progress is not None:
        progress.stop()
        progress = None

    if not parser.tables:
        raise ConversionError("No tables found in PostgreSQL dump file")

    report = ConversionReport(
        pg_dump_path=pg_dump_path,
        decentdb_path=decentdb_path,
        identifier_case=identifier_case,
    )

    tables = list(parser.tables.values())

    # Validate tables and filter out unsupported ones
    supported_tables = []
    for t in tables:
        error = _validate_supported(t)
        if error:
            report.warnings.append(f"Skipping table '{t.name}': {error}")
            report.skipped_tables.append(t.name)
        else:
            supported_tables.append(t)

    if not supported_tables:
        raise ConversionError("No supported tables found in PostgreSQL dump file")

    tables = supported_tables

    # Analyze data to detect type mismatches and adjust column types if needed
    for t in tables:
        if t.name in parser.copy_statements:
            columns, rows = parser.copy_statements[t.name]
            # Build column index mapping
            col_by_name = {col.name: col for col in t.columns}
            col_indices = {}
            for i, col_name in enumerate(columns):
                if col_name in col_by_name:
                    col_indices[i] = col_by_name[col_name]

            # Check for type mismatches in numeric columns
            # Values are already converted by _unescape_copy_value, so use
            # isinstance checks instead of re-parsing (which can fail on
            # booleans, empty strings, or already-converted float objects).
            needs_text_upgrade: set[str] = set()
            for row in rows:
                for i, val in enumerate(row):
                    if val is None or i not in col_indices:
                        continue
                    col = col_indices[i]
                    decent_type = _map_pg_type_to_decentdb(col.pg_type)
                    if decent_type == "INT64":
                        if isinstance(val, bool) or isinstance(val, int):
                            pass  # bool is subclass of int in Python
                        elif isinstance(val, float):
                            pass  # float from PG int is ok, converted at insert
                        elif isinstance(val, str):
                            try:
                                int(val)
                            except (ValueError, TypeError):
                                needs_text_upgrade.add(col.name)
                        else:
                            needs_text_upgrade.add(col.name)
                    elif decent_type == "FLOAT64":
                        if isinstance(val, (int, float, bool)):
                            pass
                        elif isinstance(val, str):
                            try:
                                float(val)
                            except (ValueError, TypeError):
                                needs_text_upgrade.add(col.name)
                        else:
                            needs_text_upgrade.add(col.name)

            # Upgrade columns with bad data to TEXT
            if needs_text_upgrade:
                new_columns = []
                for col in t.columns:
                    if col.name in needs_text_upgrade:
                        report.warnings.append(
                            f"Column '{t.name}.{col.name}' contains non-numeric data, converting to TEXT"
                        )
                        new_columns.append(dataclasses.replace(col, pg_type="text"))
                    else:
                        new_columns.append(col)
                # Update the table in the list
                idx = tables.index(t)
                tables[idx] = dataclasses.replace(t, columns=new_columns)

    # Sort by dependencies
    ordered = _toposort_tables(tables)

    # Build name maps
    table_name_map, column_name_map = _build_name_maps(
        ordered, identifier_case=identifier_case
    )
    report.table_name_map = dict(table_name_map)
    report.column_name_map = {k: dict(v) for k, v in column_name_map.items()}
    report.tables = [table_name_map[t.name] for t in ordered]

    # Build set of primary key columns for FK validation
    # FKs referencing a column within a composite PK are skipped because
    # DecentDB FK enforcement requires the referenced column to be a single-column PK.
    pk_columns: set[tuple[str, str]] = set()  # (table, column)
    composite_pk_tables: set[str] = set()
    for t in ordered:
        if t.primary_key:
            if len(t.primary_key) == 1:
                pk_columns.add((t.name, t.primary_key[0]))
            else:
                composite_pk_tables.add(t.name)

    # Filter out FKs that reference non-PK columns or composite PK columns
    for i, t in enumerate(ordered):
        valid_fks = []
        for fk in t.foreign_keys:
            if (fk.to_table, fk.to_column) in pk_columns:
                valid_fks.append(fk)
            elif fk.to_table in composite_pk_tables:
                report.warnings.append(
                    f"Skipping FK on '{t.name}.{fk.from_column}': "
                    f"references '{fk.to_table}.{fk.to_column}' (composite primary key)"
                )
            else:
                report.warnings.append(
                    f"Skipping FK on '{t.name}.{fk.from_column}': "
                    f"references '{fk.to_table}.{fk.to_column}' which is not a primary key"
                )
        if len(valid_fks) != len(t.foreign_keys):
            ordered[i] = dataclasses.replace(t, foreign_keys=valid_fks)

    # Track unsupported types
    unsupported_types: dict[str, list[str]] = defaultdict(list)
    for t in ordered:
        for col in t.columns:
            if _is_unsupported_type(col.pg_type):
                dst_type = _map_pg_type_to_decentdb(col.pg_type)
                key = f"{col.pg_type} -> {dst_type}"
                unsupported_types[key].append(f"{t.name}.{col.name}")
    report.unsupported_types = dict(unsupported_types)

    # Collect skipped indexes
    for t in ordered:
        report.skipped_indexes.extend(t.skipped_indexes)

    # Re-setup progress for schema creation and data copy phases
    if show_progress:
        from rich.console import Console
        from rich.progress import (
            BarColumn,
            Progress,
            SpinnerColumn,
            TaskProgressColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        )
        from rich.progress import Task as RichTask
        from datetime import timedelta

        class TotalElapsedColumn2(TimeElapsedColumn):
            """Shows total elapsed time since import started."""

            def __init__(self, start_time):
                super().__init__()
                self.start_time = start_time

            def render(self, task: RichTask) -> str:
                elapsed = task.get_time() - self.start_time
                return f"[{timedelta(seconds=int(elapsed))}]"

        if console is None:
            console = Console()
        if overall_start_time is None:
            overall_start_time = time.monotonic()
        if progress is None:
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold]{task.description}[/bold]"),
                BarColumn(bar_width=None),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                TotalElapsedColumn2(overall_start_time),
                transient=False,
            )

    # Connect to DecentDB
    connect_kwargs: dict[str, object] = {}
    if cache_pages is not None:
        connect_kwargs["cache_pages"] = int(cache_pages)
    if cache_mb is not None:
        connect_kwargs["cache_mb"] = int(cache_mb)

    conn = decentdb.connect(decentdb_path, **connect_kwargs)

    try:
        if progress is not None:
            progress.start()

        if progress is not None:
            schema_task = progress.add_task("Create schema", total=len(ordered))
        else:
            schema_task = None

        # Create tables (with FK constraints inline - dependencies resolved by toposort)
        conn.execute("BEGIN")
        for i, t in enumerate(ordered, start=1):
            _create_table(
                conn,
                t,
                table_name_map=table_name_map,
                column_name_map=column_name_map,
            )
            if progress is not None and schema_task is not None:
                progress.update(schema_task, completed=i)
        conn.execute("COMMIT")

        # Copy data
        total_skipped_rows = 0
        for t in ordered:
            if t.name in parser.copy_statements:
                columns, rows = parser.copy_statements[t.name]
                n, skipped = _copy_table_data(
                    conn=conn,
                    table=t,
                    columns=columns,
                    rows=rows,
                    progress=progress,
                    commit_every=commit_every,
                    table_name_map=table_name_map,
                    column_name_map=column_name_map,
                    console=console,
                    verbose=verbose,
                )
                report.rows_copied[table_name_map[t.name]] = n
                total_skipped_rows += skipped
            else:
                report.rows_copied[table_name_map[t.name]] = 0
        report.rows_skipped = total_skipped_rows

        # Create indexes
        if progress is not None:
            idx_task = progress.add_task(
                "Create indexes", total=sum(len(t.indexes) for t in ordered)
            )
        else:
            idx_task = None

        created = 0
        conn.execute("BEGIN")
        for t in ordered:
            _create_indexes(
                conn,
                t,
                table_name_map=table_name_map,
                column_name_map=column_name_map,
                report=report,
            )
            created += len(t.indexes)
            if progress is not None and idx_task is not None:
                progress.update(idx_task, completed=created)
        conn.execute("COMMIT")

        # Stop progress
        if progress is not None:
            progress.stop()
            progress = None

        # Print summary
        if console is not None:
            from rich.panel import Panel
            from rich.table import Table as RichTable

            elapsed = time.time() - start_time
            elapsed_str = f"{int(elapsed // 60)}:{int(elapsed % 60):02d}"

            summary = RichTable.grid(padding=(0, 1))
            summary.add_column(justify="right", style="bold")
            summary.add_column()
            summary.add_row("From", pg_dump_path)
            summary.add_row("To", decentdb_path)
            summary.add_row("Tables", str(len(ordered)))
            summary.add_row("Rows", str(sum(report.rows_copied.values())))
            if report.rows_skipped > 0:
                summary.add_row("Rows skipped", str(report.rows_skipped))
            summary.add_row("Indexes", str(len(report.indexes_created)))
            summary.add_row("Elapsed", elapsed_str)

            console.print(
                Panel(summary, title="PostgreSQL → DecentDB", border_style="green")
            )

            if report.skipped_tables:
                skipped_tbl = RichTable(title="Skipped Tables", show_lines=False)
                skipped_tbl.add_column("Table", style="cyan")
                skipped_tbl.add_column("Reason", style="yellow")
                for warning in report.warnings:
                    if warning.startswith("Skipping table"):
                        # Parse "Skipping table 'Name': reason"
                        match = re.match(r"Skipping table '([^']+)': (.+)", warning)
                        if match:
                            skipped_tbl.add_row(match.group(1), match.group(2))
                console.print(skipped_tbl)

            if report.skipped_indexes:
                skipped_idx_tbl = RichTable(title="Skipped Indexes", show_lines=False)
                skipped_idx_tbl.add_column("Table", style="cyan")
                skipped_idx_tbl.add_column("Name")
                skipped_idx_tbl.add_column("Reason", style="yellow")
                for s in report.skipped_indexes:
                    skipped_idx_tbl.add_row(s.table, s.name, s.reason)
                console.print(skipped_idx_tbl)

            if report.unsupported_types:
                types_tbl = RichTable(
                    title="Type Conversions (Informational)", show_lines=False
                )
                types_tbl.add_column("PostgreSQL → DecentDB", style="cyan")
                types_tbl.add_column("Columns")
                for type_conv, cols in report.unsupported_types.items():
                    types_tbl.add_row(type_conv, f"{len(cols)} columns")
                console.print(types_tbl)

            # Show other warnings (FKs, type conversions, etc.)
            other_warnings = [
                w for w in report.warnings if not w.startswith("Skipping table")
            ]
            if other_warnings:
                warn_tbl = RichTable(title="Warnings", show_lines=False)
                warn_tbl.add_column("Message", style="yellow")
                for w in other_warnings[:20]:  # Limit to first 20
                    warn_tbl.add_row(w)
                if len(other_warnings) > 20:
                    warn_tbl.add_row(
                        f"... and {len(other_warnings) - 20} more warnings"
                    )
                console.print(warn_tbl)

            console.print(f"[green]Converted[/green] {pg_dump_path} -> {decentdb_path}")

    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        if progress is not None:
            progress.stop()
        conn.close()

    return report


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Convert a PostgreSQL dump file into a DecentDB database file"
    )
    p.add_argument(
        "pg_dump_path", help="Path to the PostgreSQL dump file (.sql or .sql.gz)"
    )
    p.add_argument("decentdb_path", help="Path to the output DecentDB .db file")
    p.add_argument(
        "--overwrite", action="store_true", help="Overwrite destination if it exists"
    )
    p.add_argument(
        "--no-progress", action="store_true", help="Disable rich progress output"
    )
    p.add_argument(
        "--preserve-case",
        action="store_true",
        help="Preserve original PostgreSQL identifier casing (requires quoting in SQL)",
    )
    p.add_argument(
        "--report-json",
        default=None,
        help="Write a JSON conversion report to this path (use '-' for stdout)",
    )
    p.add_argument(
        "--commit-every",
        type=int,
        default=5_000,
        help="Commit every N inserted rows per table (0 disables chunking)",
    )
    p.add_argument(
        "--cache-mb",
        type=int,
        default=None,
        help="Override DecentDB cache size in MB (e.g. 256)",
    )
    p.add_argument(
        "--cache-pages",
        type=int,
        default=None,
        help="Override DecentDB cache size in pages (DefaultPageSize pages)",
    )
    p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output for debugging",
    )
    args = p.parse_args(argv)

    report = convert_pg_dump_to_decentdb(
        pg_dump_path=args.pg_dump_path,
        decentdb_path=args.decentdb_path,
        overwrite=bool(args.overwrite),
        show_progress=not bool(args.no_progress),
        identifier_case=("preserve" if bool(args.preserve_case) else "lower"),
        commit_every=int(args.commit_every),
        cache_mb=args.cache_mb,
        cache_pages=args.cache_pages,
        verbose=bool(args.verbose),
    )

    if args.report_json:
        write_report_json(report, str(args.report_json))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
