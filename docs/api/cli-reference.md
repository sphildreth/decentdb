# CLI Reference

The Rust CLI is implemented in `crates/decentdb-cli` and shipped as the `decentdb` binary.

Most commands take `--db=<path>`. Use `:memory:` for an ephemeral in-memory database.

## Commands

### version

```bash
decentdb version
```

### exec

Execute one or more SQL statements.

```bash
decentdb exec --db=<path> --sql="<sql>" [options]
```

Supported options:
- `--params=<type:value>` repeatable positional parameters (`int:1`, `float:1.5`, `bool:true`, `text:hello`, `blob:deadbeef`, `timestamp:1700000000`, `null`)
- `--format=<json|csv|table>` output format, default `json`
- `--checkpoint` checkpoint after execution, or checkpoint-and-exit if `--sql` is omitted
- `--openClose` open and close the database without executing SQL
- `--dbInfo` print storage info and exit
- `--noRows` discard result rows and report only the affected row count
- `--cachePages=<n>` cache size in 4KB pages
- `--cacheMb=<n>` cache size in megabytes, overrides `--cachePages`

### repl

Interactive SQL shell with:
- multi-line input
- persistent history in `~/.decentdb_history`
- transaction-aware prompt state

```bash
decentdb repl --db=<path> [--format=<json|csv|table>]
```

Special commands:
- `.help`
- `.quit`
- `.exit`

### import

Import CSV data into a table using the bulk-load path.

```bash
decentdb import --db=<path> --table=<name> --input=<file.csv> [--batchSize=<n>]
```

Current Rust CLI scope: CSV import only.

### export

Export a table as CSV or JSON.

```bash
decentdb export --db=<path> --table=<name> --output=<path> [--format=<csv|json>]
```

### bulk-load

Bulk load CSV data with explicit bulk-load options.

```bash
decentdb bulk-load --db=<path> --table=<name> --input=<file.csv> [options]
```

Supported options:
- `--batchSize=<n>`
- `--syncInterval=<n>`
- `--disableIndexes`
- `--noCheckpoint`

### checkpoint

```bash
decentdb checkpoint --db=<path>
```

### save-as

Write a checkpointed snapshot into a new on-disk database file.

```bash
decentdb save-as --db=<path> --output=<dest>
```

### migrate

Check a database file and assist in migrating unsupported legacy formats.
Currently, this command detects the source version and provides a helpful message explaining the manual logical dump/restore path if the engine cannot natively upgrade it.

```bash
decentdb migrate --db=<path>
```

### info

Show storage-level information.

```bash
decentdb info --db=<path> [--schema-summary] [--format=<json|csv|table>]
```

### describe

Describe one table.

```bash
decentdb describe --db=<path> --table=<name> [--format=<json|csv|table>]
```

### list-tables

```bash
decentdb list-tables --db=<path> [--format=<json|csv|table>]
```

### list-indexes

```bash
decentdb list-indexes --db=<path> [--table=<name>] [--format=<json|csv|table>]
```

### list-views

```bash
decentdb list-views --db=<path> [--format=<json|csv|table>]
```

### dump

Dump the current catalog and table contents as deterministic SQL.

```bash
decentdb dump --db=<path> [--output=<path>]
```

### dump-header

Decode and print the fixed page-1 header.

```bash
decentdb dump-header --db=<path> [--format=<json|csv|table>]
```

### rebuild-index

```bash
decentdb rebuild-index --db=<path> --index=<name>
```

### rebuild-indexes

```bash
decentdb rebuild-indexes --db=<path> [--table=<name>]
```

### completion

Emit a small static shell completion script.

```bash
decentdb completion [--shell=<bash|zsh>]
```

### stats

Show page and cache sizing information.

```bash
decentdb stats --db=<path> [--format=<json|csv|table>]
```

### vacuum

Checkpoint and rewrite the database into a new output file.

```bash
decentdb vacuum --db=<path> --output=<path> [--overwrite]
```

### verify-header

Open the database, validate the fixed header, and print the decoded fields.

```bash
decentdb verify-header --db=<path> [--format=<json|csv|table>]
```

### verify-index

Rebuild a named index logically and compare entry counts against the current runtime copy.

```bash
decentdb verify-index --db=<path> --index=<name> [--format=<json|csv|table>]
```

## Output Formats

`json` renders machine-readable tables or command results.

`csv` renders comma-separated rows.

`table` renders an aligned plain-text table with headers.
