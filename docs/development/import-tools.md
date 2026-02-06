# Import Tools

DecentDB provides Python-based import tools for migrating data from SQLite and PostgreSQL databases.

## Overview

Both tools follow the same pattern:
1. Parse source database schema (tables, columns, indexes, foreign keys)
2. Create equivalent schema in DecentDB
3. Copy data in batches
4. Create indexes after data load (for performance)
5. Generate a detailed conversion report

## SQLite Import (`decentdb-sqlite-import`)

### Installation

The tool is included with the Python bindings:

```bash
cd bindings/python
pip install -e .
```

### Usage

```bash
decentdb-sqlite-import <sqlite_path> <decentdb_path> [options]
```

### Options

| Option | Description |
|--------|-------------|
| `--overwrite` | Replace destination if it exists |
| `--no-progress` | Disable rich progress output |
| `--preserve-case` | Keep original identifier casing (requires quoting in SQL) |
| `--report-json <path>` | Write JSON conversion report (use `-` for stdout) |
| `--commit-every <n>` | Commit every N rows per table (default: 5000) |
| `--cache-mb <n>` | Cache size in MB |
| `--cache-pages <n>` | Cache size in pages |

### Example

```bash
# Basic conversion
decentdb-sqlite-import myapp.sqlite myapp.ddb

# With progress disabled and JSON report
decentdb-sqlite-import myapp.sqlite myapp.ddb \
  --no-progress \
  --report-json conversion.json
```

### Limitations

- **Composite primary keys**: Not supported (tables with multi-column PKs are skipped)
- **Composite unique constraints**: Not supported (converted to single-column where possible)
- **Composite indexes**: Skipped (only single-column indexes are created)

## PostgreSQL Import (`decentdb-pgbak-import`)

### Installation

Same as SQLite import - included with Python bindings.

### Usage

```bash
decentdb-pgbak-import <pg_dump_path> <decentdb_path> [options]
```

### Supported Input Formats

- Plain SQL dump files (`.sql`)
- Gzipped SQL dump files (`.sql.gz`)

### Options

| Option | Description |
|--------|-------------|
| `--overwrite` | Replace destination if it exists |
| `--no-progress` | Disable rich progress output |
| `--preserve-case` | Keep original PostgreSQL identifier casing |
| `--report-json <path>` | Write JSON conversion report (use `-` for stdout) |
| `--commit-every <n>` | Commit every N rows per table (default: 5000) |
| `--cache-mb <n>` | Cache size in MB |
| `--cache-pages <n>` | Cache size in pages |

### Example

```bash
# Import gzipped dump
decentdb-pgbak-import production.sql.gz production.ddb

# Import with progress disabled
decentdb-pgbak-import production.sql production.ddb \
  --no-progress \
  --overwrite

# Using Python module syntax
PYTHONPATH="$PWD/bindings/python" python3 -m decentdb.tools.pgbak_import \
  production.sql.gz production.ddb --overwrite
```

### Type Mapping

PostgreSQL types are mapped to DecentDB types as follows:

| PostgreSQL Type | DecentDB Type | Notes |
|----------------|---------------|-------|
| `integer`, `bigint`, `smallint`, `serial` | `INT64` | |
| `boolean` | `BOOL` | |
| `real`, `double precision` | `FLOAT64` | |
| `numeric`, `decimal` | `TEXT` | Precision preserved as text |
| `varchar`, `char`, `text` | `TEXT` | |
| `timestamp`, `timestamptz`, `date` | `TEXT` | ISO format preserved |
| `uuid` | `TEXT` | |
| `json`, `jsonb` | `TEXT` | |
| `bytea` | `BLOB` | Binary data |
| Arrays (e.g., `text[]`) | `TEXT` | PostgreSQL array literal format |
| All other types | `TEXT` | Fallback |

### Limitations

- **Composite primary keys**: Tables with multi-column PKs are skipped
- **Foreign keys to non-PK columns**: Skipped (DecentDB requires FKs to reference primary keys)
- **Self-referencing FKs**: Deferred (not yet supported by DecentDB)
- **Composite indexes**: Skipped (only single-column indexes)
- **CHECK constraints**: Not imported
- **Triggers, views, functions**: Not supported

### Handling Data Issues

The tool handles common data quality issues:

1. **Type mismatches**: Columns containing non-numeric data in numeric columns are automatically converted to TEXT
2. **Orphaned FK rows**: Rows referencing non-existent parent rows are skipped (counted in report)
3. **NULL handling**: PostgreSQL `\N` is correctly converted to SQL NULL

### Progress Output

When progress is enabled (default), you'll see:

```
Create schema ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100%
Copy albums   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 45,601/45,601
Copy songs    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 410,000/410,000
Create indexes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100%
```

### Summary Output

After conversion, a rich summary is displayed:

```
╭─────────────── PostgreSQL → DecentDB ───────────────╮
│ From   production.sql.gz                            │
│ To     production.ddb                               │
│ Tables 43                                           │
│ Rows   482,907                                      │
│ Indexes 12                                          │
╰─────────────────────────────────────────────────────╯

Skipped Tables
┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Table                   ┃ Reason                         ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ PartySessionParticipants│ Composite primary key not      │
│                         │ supported                      │
└─────────────────────────┴────────────────────────────────┘

Warnings
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Message                                                 ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Column 'charts.year' contains non-numeric data,         │
│ converting to TEXT                                      │
└─────────────────────────────────────────────────────────┘

[green]Converted[/green] production.sql.gz -> production.ddb
```

### Conversion Report

The JSON report contains:

```json
{
  "pg_dump_path": "production.sql.gz",
  "decentdb_path": "production.ddb",
  "tables": ["albums", "artists", "songs", ...],
  "rows_copied": {"albums": 45601, "artists": 19407, ...},
  "indexes_created": ["ix_albums_artistid", ...],
  "skipped_tables": ["PartySessionParticipants"],
  "rows_skipped": 1234,
  "warnings": [...],
  "unsupported_types": {
    "numeric -> TEXT": ["albums.price", "songs.rating"]
  }
}
```

## Performance Tips

1. **Use `--commit-every`**: For large imports, commit every 5,000-10,000 rows to balance performance and memory
2. **Disable progress for scripting**: Use `--no-progress` when running in automated pipelines
3. **Increase cache**: For very large imports, use `--cache-mb 512` or higher
4. **Import order**: Tables are automatically sorted by FK dependencies (parents before children)

## Troubleshooting

### "Referenced column must be indexed uniquely"

This error occurs when a foreign key references a non-primary-key column. The import tool will skip these FKs and report them in the warnings.

### "Type mismatch" errors

If you see type mismatch errors, the tool will automatically convert the problematic column to TEXT and continue. Check the warnings table in the output.

### High memory usage

For very large dumps:
- Reduce `--commit-every` to commit more frequently
- Use `--cache-mb` to limit cache size
- Consider splitting the dump into smaller files

### Slow import

- Ensure progress is enabled to monitor bottlenecks
- Index creation is typically the slowest phase - this is normal
- The "Create indexes" phase builds all indexes after data load

## Differences from Native DecentDB Import

These Python import tools provide convenience and compatibility but may be slower than native DecentDB import mechanisms:

- They use the Python DB-API driver (ctypes overhead)
- Data conversion happens in Python
- No parallel processing

For maximum performance with large datasets, consider:
1. Using the native `decentdb bulk-load` command for CSV data
2. Writing a custom Nim importer using the C API directly
