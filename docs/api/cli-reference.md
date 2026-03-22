# CLI Reference

The `decentdb` CLI provides all database operations through subcommands.

## General Usage

Most commands require `--db=<path>`. Use `:memory:` (case-insensitive) for an ephemeral in-memory database:

```bash
decentdb exec --db=myapp.ddb --sql="SELECT * FROM users"
decentdb exec --db=:memory: --sql="SELECT 1 + 1"
```

Use `--help` on any command for the authoritative, generated help:

```bash
decentdb --help
decentdb <command> --help
```

## Commands

### bulk-load

Bulk load data from CSV (high-throughput ingest).

```bash
decentdb bulk-load --db=<path> --table=<name> --input=<path> [options]
```

Options:
- `--batchSize=<n>` - Rows per batch (default: 10000)
- `--syncInterval=<n>` - Batches between fsync when durability is deferred (default: 10)
- `--durability=<full|deferred|none>` - Durability mode (default: deferred)
- `--disableIndexes` - Disable indexes during load (default: true)
- `--noCheckpoint` - Skip checkpoint after load completes

### checkpoint

Force WAL checkpoint.

```bash
decentdb checkpoint --db=<path> [--warnings] [--verbose]
```

### completion

Emit a basic shell completion script.

```bash
decentdb completion [--shell=bash|zsh]
```

### describe

Show table structure.

```bash
decentdb describe --db=<path> --table=<name>
```

Options:
- `--table=<name>` - Table name to describe

### dump

Dump the database as SQL statements.

```bash
decentdb dump --db=<path> [--output=<path>]
```

### dump-header

Dump raw database header fields and checksum status.

```bash
decentdb dump-header --db=<path>
```

### exec

Execute SQL statements.

```bash
decentdb exec --db=<path> --sql=<sql> [options]
```

Notes:
- `--sql` may contain multiple `;`-separated statements.
- Statements are **parsed/bound up front** against the starting schema. DDL followed by dependent statements in the same `--sql` string can fail; use separate `exec` calls or `decentdb repl`.

Options:
- `--sql=<sql>` - SQL to execute
- `--params=<type:value>` - Bind parameters (repeatable, positional). Examples: `int:1`, `text:Alice`, `null`
- `--openClose` - Open and close the database without executing SQL (testing mode)
- `--timing` - Include a detailed `timing` object in JSON output (JSON output always includes `elapsed_ms`)
- `--format=<json|csv|table>` - Output format (default: json). Note: `--timing/--warnings/--verbose/--noRows` currently require `--format=json`
- `--noRows` - Execute a single `SELECT` and discard result rows (returns the row count)
- `--cachePages=<n>` - Cache size in 4KB pages (default: 1024)
- `--cacheMb=<n>` - Cache size in megabytes (overrides `--cachePages` if non-zero)
- `--heartbeatMs=<n>` - Print periodic progress to stderr while a long query is running

Diagnostics and management flags:
- `--checkpoint` - Force a WAL checkpoint (with `--sql`: after execution; without: checkpoint and exit)
- `--dbInfo` - Print database info and exit
- `--readerCount` - Print active reader count and exit
- `--longReaders=<ms>` - Print readers older than this threshold and exit
- `--warnings` - Include WAL warnings in output
- `--verbose` - Include verbose diagnostics (LSN, readers, cache) in output

WAL tuning/testing flags:
- `--checkpointBytes=<n>` - Auto-checkpoint when WAL reaches N bytes
- `--checkpointMs=<n>` - Auto-checkpoint when N ms elapse since last checkpoint
- `--readerWarnMs=<n>` - Warn when readers are older than N ms
- `--readerTimeoutMs=<n>` - Trigger timeout behavior for readers older than N ms
- `--forceTruncateOnTimeout` - Force WAL truncation when `--readerTimeoutMs` triggers (dangerous; testing only)
- `--walFailpoints=<spec>` - Set WAL failpoints (repeatable). Format: `label:kind[:bytes][:count]`
- `--clearWalFailpoints` - Clear all WAL failpoints before executing

Examples:
```bash
decentdb exec --db=my.ddb --sql="SELECT * FROM users"
decentdb exec --db=my.ddb --sql="INSERT INTO users VALUES (\$1, \$2)" --params=int:1 --params=text:Alice
decentdb exec --db=my.ddb --sql="SELECT * FROM users" --format=table

# Assuming schema already exists, execute multiple statements in one call:
decentdb exec --db=my.ddb --sql="BEGIN; INSERT INTO users (name) VALUES ('Alice'); COMMIT"

# Execute SQL and immediately checkpoint to the main DB file
decentdb exec --db=my.ddb --sql="CREATE INDEX ix_name ON users(name)" --checkpoint
```

### export

Export table to CSV.

```bash
decentdb export --db=<path> --table=<name> --output=<path> [options]
```

Options:
- `--table=<name>` - Table name to export
- `--output=<path>` - Output file path
- `--format=<csv|json>` - Output format (default: csv)

### import

Import data from CSV or JSON.

```bash
decentdb import --db=<path> --table=<name> --input=<path> [options]
```

Options:
- `--table=<name>` - Target table
- `--input=<path>` - Input file path
- `--format=<csv|json>` - Input format (default: csv)
- `--batchSize=<n>` - Rows per batch (default: 10000)

### info

Show detailed database information (configured settings and schema).

```bash
decentdb info --db=<path> [--schema-summary]
```

Options:
- `--schema-summary` - Include schema summary (tables, columns, indexes)

Use `info` to inspect static database configuration (like format version and page size) or get a high-level summary of the database schema layout.

### list-indexes

List all indexes.

```bash
decentdb list-indexes --db=<path> [--table=<name>]
```

Options:
- `--table=<name>` - Filter by table (optional)

### list-tables

List all tables in the database.

```bash
decentdb list-tables --db=<path>
```

### list-views

List all views in the database.

```bash
decentdb list-views --db=<path>
```

### rebuild-index

Rebuild an index (compaction).

```bash
decentdb rebuild-index --db=<path> --index=<name>
```

Options:
- `--index=<name>` - Index name to rebuild

### rebuild-indexes

Rebuild all indexes in the database.

```bash
decentdb rebuild-indexes --db=<path> [--table=<name>]
```

Options:
- `--table=<name>` - Filter indexes by table (optional)

### repl

Interactive Read-Eval-Print Loop (REPL) mode.

```bash
decentdb repl --db=<path> [--format=<json|csv|table>]
```

### save-as

Export the database to a new on-disk file (snapshot backup). Works with both file-based and `:memory:` databases.

```bash
decentdb save-as --db=<path> --output=<dest>
decentdb save-as --db=:memory: --output=/tmp/snapshot.ddb
```

Options:
- `--output=<dest>` - Destination file path (required, must not already exist)

The command performs a full WAL checkpoint, then copies all pages to the destination atomically.

### stats

Show runtime database statistics (current memory usage and physical size).

```bash
decentdb stats --db=<path>
```

Use `stats` to monitor the current memory usage of the database engine (how full the cache is) or check the physical page layout size of the database file on disk.

### vacuum

Rewrite the database into a new file to reclaim space.

```bash
decentdb vacuum --db=<path> --output=<path> [--overwrite] [--cachePages=<n>] [--cacheMb=<n>]
```

### verify-header

Verify database header magic and checksum.

```bash
decentdb verify-header --db=<path>
```

### verify-index

Verify index integrity.

```bash
decentdb verify-index --db=<path> --index=<name>
```

## Output Formats

### JSON (default)

JSON output is intended for machine consumption. It always includes `elapsed_ms`.

```json
{
  "ok": true,
  "error": null,
  "rows": [
    "1|Alice|alice@example.com",
    "2|Bob|bob@example.com"
  ],
  "elapsed_ms": 0.3216
}
```

When `--checkpoint` is used with `--sql`, the response includes the checkpoint LSN:

```json
{
  "ok": true,
  "error": null,
  "rows": [],
  "elapsed_ms": 0.4682,
  "checkpoint_lsn": 550651
}
```

### CSV

CSV output is a simple rendering of row values (no header row):

```
1,Alice,alice@example.com
2,Bob,bob@example.com
```

### Table

Table output is a simple rendering of row values (no header row):

```
1 | Alice | alice@example.com
2 | Bob | bob@example.com
```

## Error Handling

When a command fails, the exit code is non-zero and output contains error details:

```json
{
  "ok": false,
  "error": {
    "code": "ERR_SQL",
    "message": "Table not found",
    "context": "users"
  },
  "rows": [],
  "elapsed_ms": 0.3594
}
```

## Environment Variables

- `DECENTDB` - Path to decentdb CLI (used by test harness)
- `PGDATABASE` - PostgreSQL connection for differential tests
