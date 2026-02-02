# CLI Reference

The `decentdb` CLI provides all database operations through subcommands.

## General Usage

Most commands require `--db=<path>`.

Use `--help` on any command for the authoritative, generated help:

```bash
decentdb --help
decentdb <command> --help
```

## Commands

### exec

Execute SQL statements.

```bash
decentdb exec --db=<path> --sql=<statement> [options]
```

Options:
- `--sql=<statement>` - SQL statement to execute
- `--params=<type:value>` - Bind parameters (repeatable, positional). Examples: `int:1`, `text:Alice`, `null`
- `--timing` - Include `elapsed_ms` in JSON output
- `--format=<json|csv|table>` - Output format (default: json). Note: `--timing/--warnings/--verbose` currently require `--format=json`
- `--cachePages=<n>` - Cache size in 4KB pages (default: 1024)
- `--cacheMb=<n>` - Cache size in megabytes (overrides `--cachePages` if non-zero)

Diagnostics and management flags (these run and exit, ignoring `--sql` when applicable):
- `--checkpoint` - Force a WAL checkpoint and exit
- `--dbInfo` - Print database info and exit
- `--readerCount` - Print active reader count and exit
- `--longReaders=<ms>` - Print readers older than this threshold and exit

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
```

### list-tables

List all tables in the database.

```bash
decentdb list-tables --db=<path>
```

### describe

Show table structure.

```bash
decentdb describe --db=<path> --table=<name>
```

Options:
- `--table=<name>` - Table name to describe

### list-indexes

List all indexes.

```bash
decentdb list-indexes --db=<path> [--table=<name>]
```

Options:
- `--table=<name>` - Filter by table (optional)

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

### verify-index

Verify index integrity.

```bash
decentdb verify-index --db=<path> --index=<name>
```

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

### export

Export table to CSV.

```bash
decentdb export --db=<path> --table=<name> --output=<path> [options]
```

Options:
- `--table=<name>` - Table name to export
- `--output=<path>` - Output file path
- `--format=<csv|json>` - Output format (default: csv)

### dump

Dump the database as SQL statements.

```bash
decentdb dump --db=<path> [--output=<path>]
```

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

### stats

Show database statistics.

```bash
decentdb stats --db=<path>
```

### info

Show detailed database information.

```bash
decentdb info --db=<path> [--schema-summary]
```

Options:
- `--schema-summary` - Include schema summary (tables, columns, indexes)

### vacuum

Rewrite the database into a new file to reclaim space.

```bash
decentdb vacuum --db=<path> --output=<path> [--overwrite] [--cachePages=<n>] [--cacheMb=<n>]
```

### dump-header

Dump raw database header fields and checksum status.

```bash
decentdb dump-header --db=<path>
```

### verify-header

Verify database header magic and checksum.

```bash
decentdb verify-header --db=<path>
```

### repl

Interactive REPL mode.

```bash
decentdb repl --db=<path> [--format=<json|csv|table>]
```

### completion

Emit a basic shell completion script.

```bash
decentdb completion [--shell=bash|zsh]
```

## Output Formats

### JSON (default)

```json
{
  "ok": true,
  "error": null,
  "rows": [
    "1|Alice|alice@example.com",
    "2|Bob|bob@example.com"
  ]
}
```

### CSV

```
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

### Table

```
id | name  | email
---|-------|------------------
1  | Alice | alice@example.com
2  | Bob   | bob@example.com
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
  "rows": []
}
```

## Environment Variables

- `DECENTDB` - Path to decentdb CLI (used by test harness)
- `PGDATABASE` - PostgreSQL connection for differential tests
