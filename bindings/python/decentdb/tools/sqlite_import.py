from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sqlite3
from collections import defaultdict, deque
from typing import Any, Iterable, Iterator, Sequence

import decentdb


@dataclasses.dataclass(frozen=True)
class SqliteColumn:
    name: str
    declared_type: str
    not_null: bool
    pk: bool
    unique: bool = False


@dataclasses.dataclass(frozen=True)
class SqliteForeignKey:
    from_column: str
    to_table: str
    to_column: str


@dataclasses.dataclass(frozen=True)
class SqliteIndex:
    name: str
    table: str
    column: str
    unique: bool


@dataclasses.dataclass(frozen=True)
class SkippedIndex:
    name: str
    table: str
    reason: str


@dataclasses.dataclass(frozen=True)
class SqliteTable:
    name: str
    columns: list[SqliteColumn]
    foreign_keys: list[SqliteForeignKey]
    indexes: list[SqliteIndex]
    skipped_indexes: list[SkippedIndex]


@dataclasses.dataclass
class ConversionReport:
    sqlite_path: str
    decentdb_path: str
    identifier_case: str = "lower"  # "lower" or "preserve"
    table_name_map: dict[str, str] = dataclasses.field(default_factory=dict)
    column_name_map: dict[str, dict[str, str]] = dataclasses.field(default_factory=dict)
    tables: list[str] = dataclasses.field(default_factory=list)
    rows_copied: dict[str, int] = dataclasses.field(default_factory=dict)
    indexes_created: list[str] = dataclasses.field(default_factory=list)
    unique_columns_added: list[str] = dataclasses.field(default_factory=list)
    skipped_indexes: list[SkippedIndex] = dataclasses.field(default_factory=list)
    warnings: list[str] = dataclasses.field(default_factory=list)


def report_to_dict(report: ConversionReport) -> dict[str, Any]:
    return {
        "sqlite_path": report.sqlite_path,
        "decentdb_path": report.decentdb_path,
        "identifier_case": report.identifier_case,
        "table_name_map": dict(report.table_name_map),
        "column_name_map": {k: dict(v) for k, v in report.column_name_map.items()},
        "tables": list(report.tables),
        "rows_copied": dict(report.rows_copied),
        "indexes_created": list(report.indexes_created),
        "unique_columns_added": list(report.unique_columns_added),
        "skipped_indexes": [dataclasses.asdict(s) for s in report.skipped_indexes],
        "warnings": list(report.warnings),
    }


def _normalize_ident(name: str, *, identifier_case: str) -> str:
    if identifier_case == "preserve":
        return name
    if identifier_case == "lower":
        return name.lower()
    raise ValueError(f"Unknown identifier_case: {identifier_case}")


def _build_name_maps(tables: list[SqliteTable], *, identifier_case: str) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
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


def write_report_json(report: ConversionReport, path: str) -> None:
    payload = json.dumps(report_to_dict(report), ensure_ascii=False, indent=2, sort_keys=True)
    if path == "-":
        print(payload)
        return
    with open(path, "w", encoding="utf-8") as f:
        f.write(payload)
        f.write("\n")


class ConversionError(RuntimeError):
    pass


def _quote_ident(name: str) -> str:
    # Double-quote identifiers (Postgres-style). Escape embedded quotes.
    return '"' + name.replace('"', '""') + '"'


def _map_declared_type_to_decentdb(declared_type: str) -> str:
    # DecentDB supports: INT64, BOOL, FLOAT64, TEXT, BLOB
    # SQLite uses affinities; map best-effort.
    t = (declared_type or "").strip().upper()
    if not t:
        return "TEXT"

    # Order matters.
    if "BOOL" in t:
        return "BOOL"
    if "INT" in t:
        return "INT64"
    if any(k in t for k in ["REAL", "FLOA", "DOUB"]):
        return "FLOAT64"
    if "BLOB" in t:
        return "BLOB"
    if "UUID" in t:
        return "UUID"
    if any(k in t for k in ["DECIMAL", "NUMERIC"]):
        # Preserve precision/scale if present (e.g. DECIMAL(10,2))
        # Map NUMERIC to DECIMAL
        mapped = t.replace("NUMERIC", "DECIMAL")
        if "(" in mapped:
             return mapped
        return "DECIMAL(18,6)"
    if any(k in t for k in ["CHAR", "CLOB", "TEXT", "VARCHAR"]):
        return "TEXT"

    # SQLite allows arbitrary type names; default to TEXT for safety.
    return "TEXT"


def _iter_user_tables(sqlite_conn: sqlite3.Connection) -> Iterator[str]:
    cur = sqlite_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    for (name,) in cur.fetchall():
        yield str(name)


def _load_table_schema(sqlite_conn: sqlite3.Connection, table: str) -> SqliteTable:
    # Columns
    cols: list[SqliteColumn] = []
    # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
    for row in sqlite_conn.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall():
        col_name = str(row[1])
        declared_type = str(row[2] or "")
        not_null = bool(row[3])
        pk = int(row[5]) > 0
        cols.append(
            SqliteColumn(name=col_name, declared_type=declared_type, not_null=not_null, pk=pk)
        )

    if not cols:
        raise ConversionError(f"Table has no columns: {table}")

    # Foreign keys
    fks: list[SqliteForeignKey] = []
    # PRAGMA foreign_key_list: id, seq, table, from, to, on_update, on_delete, match
    for row in sqlite_conn.execute(f"PRAGMA foreign_key_list({_quote_ident(table)})").fetchall():
        to_table = str(row[2])
        from_col = str(row[3])
        to_col = str(row[4])
        fks.append(SqliteForeignKey(from_column=from_col, to_table=to_table, to_column=to_col))

    # Indexes (best-effort): only single-column indexes are imported.
    # NOTE: DecentDB enforces uniqueness via column constraints (col.unique) + auto-created unique indexes.
    # Standalone CREATE UNIQUE INDEX currently does not participate in INSERT-time uniqueness checks.
    # So we translate SQLite unique single-column indexes (including sqlite_autoindex_* from UNIQUE constraints)
    # into column-level UNIQUE constraints.
    indexes: list[SqliteIndex] = []
    skipped: list[SkippedIndex] = []
    # PRAGMA index_list: seq, name, unique, origin, partial
    col_pos = {c.name: i for i, c in enumerate(cols)}

    for row in sqlite_conn.execute(f"PRAGMA index_list({_quote_ident(table)})").fetchall():
        idx_name = str(row[1])
        unique = bool(row[2])

        # Skip implicit PK indexes in SQLite (origin='pk') because DecentDB creates PK/unique indexes itself.
        origin = str(row[3] or "")
        if origin.lower() == "pk":
            continue

        cols_rows = sqlite_conn.execute(f"PRAGMA index_info({_quote_ident(idx_name)})").fetchall()
        # index_info: seqno, cid, name
        if len(cols_rows) != 1:
            if unique:
                skipped.append(
                    SkippedIndex(
                        name=idx_name,
                        table=table,
                        reason="Composite UNIQUE constraint/index not supported (single-column only)",
                    )
                )
            else:
                skipped.append(
                    SkippedIndex(
                        name=idx_name,
                        table=table,
                        reason="Composite index not imported (single-column only)",
                    )
                )
            continue
        col_name = str(cols_rows[0][2])

        if unique and col_name in col_pos:
            # Mark as UNIQUE constraint on the column.
            i = col_pos[col_name]
            cols[i] = dataclasses.replace(cols[i], unique=True)
            continue

        indexes.append(SqliteIndex(name=idx_name, table=table, column=col_name, unique=False))

    return SqliteTable(name=table, columns=cols, foreign_keys=fks, indexes=indexes, skipped_indexes=skipped)


def _validate_supported(table: SqliteTable) -> None:
    pk_cols = [c for c in table.columns if c.pk]
    if len(pk_cols) > 1:
        raise ConversionError(
            f"Composite primary key not supported by DecentDB: {table.name} ({', '.join(c.name for c in pk_cols)})"
        )

    fk_by_from: dict[str, list[SqliteForeignKey]] = defaultdict(list)
    for fk in table.foreign_keys:
        fk_by_from[fk.from_column].append(fk)

    for from_col, fks in fk_by_from.items():
        if len(fks) > 1:
            raise ConversionError(
                f"Multiple foreign key targets from one column not supported: {table.name}.{from_col}"
            )

    # NOTE: DecentDB supports single-column UNIQUE, but not multi-column unique constraints.
    # We only recreate explicit single-column indexes; table-level/multi-col constraints are skipped by design.


def _toposort_tables(tables: list[SqliteTable]) -> list[SqliteTable]:
    by_name = {t.name: t for t in tables}
    deps: dict[str, set[str]] = {t.name: set() for t in tables}
    rev: dict[str, set[str]] = {t.name: set() for t in tables}

    for t in tables:
        for fk in t.foreign_keys:
            if fk.to_table not in by_name:
                # External/unknown table; we can't satisfy the FK. Let DecentDB enforce later.
                # But creating the FK inline would make inserts impossible.
                raise ConversionError(
                    f"Foreign key references missing table: {t.name}.{fk.from_column} -> {fk.to_table}({fk.to_column})"
                )
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
        raise ConversionError(f"Foreign key cycle detected among tables: {', '.join(sorted(cycle))}")

    return [by_name[name] for name in out]


def _create_table(conn: decentdb.Connection, table: SqliteTable) -> None:
    raise AssertionError("_create_table must be called via _create_table_mapped")


def _create_table_mapped(
    conn: decentdb.Connection,
    table: SqliteTable,
    *,
    table_name_map: dict[str, str],
    column_name_map: dict[str, dict[str, str]],
) -> None:
    fk_map: dict[str, SqliteForeignKey] = {fk.from_column: fk for fk in table.foreign_keys}

    col_defs: list[str] = []
    for col in table.columns:
        dst_table = table_name_map[table.name]
        dst_col = column_name_map[table.name][col.name]
        parts: list[str] = [_quote_ident(dst_col), _map_declared_type_to_decentdb(col.declared_type)]

        if col.pk:
            parts.append("PRIMARY KEY")
        else:
            if col.unique:
                parts.append("UNIQUE")
            if col.not_null:
                parts.append("NOT NULL")

        fk = fk_map.get(col.name)
        if fk is not None:
            parts.append(
                "REFERENCES "
                + _quote_ident(table_name_map[fk.to_table])
                + "(" + _quote_ident(column_name_map[fk.to_table][fk.to_column]) + ")"
            )

        col_defs.append(" ".join(parts))

    sql = "CREATE TABLE " + _quote_ident(dst_table) + " (" + ", ".join(col_defs) + ")"
    conn.execute(sql)


def _table_row_count(sqlite_conn: sqlite3.Connection, table: str) -> int:
    (n,) = sqlite_conn.execute(f"SELECT COUNT(*) FROM {_quote_ident(table)}").fetchone()
    return int(n)


def _iter_sqlite_rows(sqlite_conn: sqlite3.Connection, table: SqliteTable) -> Iterator[tuple[Any, ...]]:
    col_list = ", ".join(_quote_ident(c.name) for c in table.columns)
    sql = f"SELECT {col_list} FROM {_quote_ident(table.name)}"
    cur = sqlite_conn.execute(sql)
    while True:
        rows = cur.fetchmany(1000)
        if not rows:
            break
        for r in rows:
            yield tuple(r)


def _adapt_row(table: SqliteTable, row: Sequence[Any]) -> list[Any]:
    # sqlite3 returns ints/floats/str/bytes/None.
    # If column declared as BOOL, map 0/1 to bool to preserve type mapping in DecentDB.
    out: list[Any] = []
    for col, v in zip(table.columns, row):
        if v is None:
            out.append(None)
            continue
        if _map_declared_type_to_decentdb(col.declared_type) == "BOOL":
            if isinstance(v, bool):
                out.append(v)
            elif isinstance(v, int) and v in (0, 1):
                out.append(bool(v))
            else:
                # Best-effort: keep original and let DecentDB type checking decide.
                out.append(v)
            continue
        out.append(v)
    return out


def _copy_table_data(
    *,
    sqlite_conn: sqlite3.Connection,
    decent_conn: decentdb.Connection,
    table: SqliteTable,
    progress=None,
    update_every: int = 200,
    commit_every: int = 5_000,
    table_name_map: dict[str, str] | None = None,
    column_name_map: dict[str, dict[str, str]] | None = None,
) -> None:
    if table_name_map is None or column_name_map is None:
        raise AssertionError("_copy_table_data requires name maps")

    dst_table = table_name_map[table.name]
    col_names = [c.name for c in table.columns]
    dst_cols = [column_name_map[table.name][c] for c in col_names]
    cols_sql = ", ".join(_quote_ident(c) for c in dst_cols)
    
    placeholders = []
    for c in table.columns:
        dtype = _map_declared_type_to_decentdb(c.declared_type)
        if dtype.startswith("DECIMAL") or dtype.startswith("NUMERIC"):
             placeholders.append(f"CAST(? AS {dtype})")
        elif dtype == "UUID":
             placeholders.append("CAST(? AS UUID)")
        else:
             placeholders.append("?")
             
    insert_sql = f"INSERT INTO {_quote_ident(dst_table)} ({cols_sql}) VALUES ({', '.join(placeholders)})"

    total = _table_row_count(sqlite_conn, table.name)
    task_id = None
    if progress is not None:
        task_id = progress.add_task(f"Copy {table.name}", total=total)

    cur = decent_conn.cursor()
    n = 0
    in_tx = False
    if commit_every and commit_every > 0:
        decent_conn.execute("BEGIN")
        in_tx = True
    for row in _iter_sqlite_rows(sqlite_conn, table):
        cur.execute(insert_sql, _adapt_row(table, row))
        n += 1
        if in_tx and commit_every and commit_every > 0 and (n % commit_every == 0):
            cur.close()
            decent_conn.execute("COMMIT")
            decent_conn.execute("BEGIN")
            cur = decent_conn.cursor()
        if progress is not None and task_id is not None and (n % update_every == 0 or n == total):
            progress.update(task_id, completed=n)

    if in_tx:
        cur.close()
        decent_conn.execute("COMMIT")
        in_tx = False

    if progress is not None and task_id is not None:
        progress.update(task_id, completed=total)


def _create_indexes(conn: decentdb.Connection, indexes: list[SqliteIndex], *, report: ConversionReport | None = None) -> None:
    for idx in indexes:
        raise AssertionError("_create_indexes must be called via _create_indexes_mapped")


def _create_indexes_mapped(
    conn: decentdb.Connection,
    table: SqliteTable,
    *,
    table_name_map: dict[str, str],
    column_name_map: dict[str, dict[str, str]],
    report: ConversionReport | None = None,
) -> None:
    for idx in table.indexes:
        # Uniqueness is enforced via column constraints; imported indexes are always non-unique.
        uniq = ""
        dst_table = table_name_map[idx.table]
        dst_col = column_name_map[idx.table][idx.column]
        dst_idx = _normalize_ident(idx.name, identifier_case=report.identifier_case if report else "lower")
        sql = (
            "CREATE "
            + uniq
            + "INDEX "
            + _quote_ident(dst_idx)
            + " ON "
            + _quote_ident(dst_table)
            + "(" + _quote_ident(dst_col) + ")"
        )
        conn.execute(sql)
        if report is not None:
            report.indexes_created.append(dst_idx)


def convert_sqlite_to_decentdb(
    *,
    sqlite_path: str,
    decentdb_path: str,
    overwrite: bool = False,
    show_progress: bool = True,
    identifier_case: str = "lower",
    commit_every: int = 5_000,
    cache_pages: int | None = None,
    cache_mb: int | None = None,
) -> ConversionReport:
    if not os.path.exists(sqlite_path):
        raise FileNotFoundError(sqlite_path)

    if os.path.exists(decentdb_path):
        if not overwrite:
            raise ConversionError(
                f"Destination already exists: {decentdb_path} (pass overwrite=True to replace)"
            )
        os.remove(decentdb_path)
        if os.path.exists(decentdb_path + "-wal"):
            os.remove(decentdb_path + "-wal")

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    # Ensure FK metadata is consistent.
    sqlite_conn.execute("PRAGMA foreign_keys=ON")

    report = ConversionReport(sqlite_path=sqlite_path, decentdb_path=decentdb_path, identifier_case=identifier_case)

    tables = [_load_table_schema(sqlite_conn, t) for t in _iter_user_tables(sqlite_conn)]
    for t in tables:
        _validate_supported(t)

    ordered = _toposort_tables(tables)
    table_name_map, column_name_map = _build_name_maps(ordered, identifier_case=identifier_case)
    report.table_name_map = dict(table_name_map)
    report.column_name_map = {k: dict(v) for k, v in column_name_map.items()}
    report.tables = [table_name_map[t.name] for t in ordered]
    for t in ordered:
        for col in t.columns:
            if col.unique and not col.pk:
                report.unique_columns_added.append(
                    f"{table_name_map[t.name]}.{column_name_map[t.name][col.name]}"
                )
        report.skipped_indexes.extend(t.skipped_indexes)

    progress = None
    console = None
    if show_progress:
        # Rich is optional at runtime, but required by the feature request.
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

        console = Console()
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}[/bold]"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=False,
        )

    connect_kwargs: dict[str, object] = {}
    if cache_pages is not None:
        connect_kwargs["cache_pages"] = int(cache_pages)
    if cache_mb is not None:
        connect_kwargs["cache_mb"] = int(cache_mb)
    decent_conn = decentdb.connect(decentdb_path, **connect_kwargs)

    try:
        if progress is not None:
            progress.start()

        if progress is not None:
            schema_task = progress.add_task("Create schema", total=len(ordered))
        else:
            schema_task = None

        # Create tables first (FK-safe due to ordering)
        decent_conn.execute("BEGIN")
        for i, t in enumerate(ordered, start=1):
            _create_table_mapped(
                decent_conn,
                t,
                table_name_map=table_name_map,
                column_name_map=column_name_map,
            )
            if progress is not None and schema_task is not None:
                progress.update(schema_task, completed=i)

        decent_conn.execute("COMMIT")

        # Copy data (chunked commits to avoid exhausting the page cache)
        for t in ordered:
            _copy_table_data(
                sqlite_conn=sqlite_conn,
                decent_conn=decent_conn,
                table=t,
                progress=progress,
                commit_every=commit_every,
                table_name_map=table_name_map,
                column_name_map=column_name_map,
            )
            report.rows_copied[table_name_map[t.name]] = _table_row_count(sqlite_conn, t.name)

        # Create indexes after load for speed.
        if progress is not None:
            idx_task = progress.add_task("Create indexes", total=sum(len(t.indexes) for t in ordered))
        else:
            idx_task = None

        created = 0
        decent_conn.execute("BEGIN")
        for t in ordered:
            _create_indexes_mapped(
                decent_conn,
                t,
                table_name_map=table_name_map,
                column_name_map=column_name_map,
                report=report,
            )
            created += len(t.indexes)
            if progress is not None and idx_task is not None:
                progress.update(idx_task, completed=created)

        decent_conn.execute("COMMIT")

        # Stop progress rendering before printing summary tables.
        # Printing to a separate Console while Progress is active can produce
        # garbled/duplicated output depending on terminal capabilities.
        if progress is not None:
            progress.stop()
            progress = None

        if console is not None:
            from rich.panel import Panel
            from rich.table import Table

            summary = Table.grid(padding=(0, 1))
            summary.add_column(justify="right", style="bold")
            summary.add_column()
            summary.add_row("From", sqlite_path)
            summary.add_row("To", decentdb_path)
            summary.add_row("Tables", str(len(ordered)))
            summary.add_row("Rows", str(sum(report.rows_copied.values())))
            summary.add_row("Indexes", str(len(report.indexes_created)))
            summary.add_row("Unique cols", str(len(report.unique_columns_added)))

            console.print(Panel(summary, title="SQLite → DecentDB", border_style="green"))

            if report.skipped_indexes:
                skipped_tbl = Table(title="Skipped Indexes/Constraints", show_lines=False)
                skipped_tbl.add_column("Table", style="cyan")
                skipped_tbl.add_column("Name")
                skipped_tbl.add_column("Reason", style="yellow")
                for s in report.skipped_indexes:
                    skipped_tbl.add_row(s.table, s.name, s.reason)
                console.print(skipped_tbl)

            console.print(f"[green]Converted[/green] {sqlite_path} -> {decentdb_path}")

    except Exception:
        try:
            decent_conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        if progress is not None:
            progress.stop()
        decent_conn.close()
        sqlite_conn.close()

    return report


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Convert a SQLite database file into a DecentDB database file")
    p.add_argument("sqlite_path", help="Path to the SQLite .db file")
    p.add_argument("decentdb_path", help="Path to the output DecentDB .db file")
    p.add_argument("--overwrite", action="store_true", help="Overwrite destination if it exists")
    p.add_argument("--no-progress", action="store_true", help="Disable rich progress output")
    p.add_argument(
        "--preserve-case",
        action="store_true",
        help="Preserve original SQLite identifier casing (requires quoting in SQL)",
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
    args = p.parse_args(argv)

    report = convert_sqlite_to_decentdb(
        sqlite_path=args.sqlite_path,
        decentdb_path=args.decentdb_path,
        overwrite=bool(args.overwrite),
        show_progress=not bool(args.no_progress),
        identifier_case=("preserve" if bool(args.preserve_case) else "lower"),
        commit_every=int(args.commit_every),
        cache_mb=args.cache_mb,
        cache_pages=args.cache_pages,
    )

    if args.report_json:
        write_report_json(report, str(args.report_json))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
