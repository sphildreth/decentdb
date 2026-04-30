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

### doctor (new in v2.3)

Run a diagnostic health check against a database file. Doctor is **read-only by default**
and does not mutate the database unless `--fix` is present.

```bash
decentdb doctor --db=<path> [options]
```

Supported options:

- `--format=<json|markdown>` output format, default `markdown`
- `--checks=<all|header,storage,wal,fragmentation,schema,statistics,indexes,compatibility>` limit checks to selected categories, default `all`
- `--verify-index=<name>` repeatable, run expensive logical verification for named indexes
- `--verify-indexes` run expensive logical verification for all indexes up to `--max-index-verify`
- `--max-index-verify=<n>` safety cap for `--verify-indexes`, default `32`
- `--fail-on=<info|warning|error>` minimum severity that makes the process exit non-zero, default `error`
- `--include-recommendations` include safe recommendation text and commands, default `true`
- `--path-mode=<absolute|basename|redacted>` controls path rendering in output, default `absolute`
- `--fix` apply v1 auto-fixable actions after diagnosis, then re-run diagnosis

**Exit codes:**

| Code | Meaning |
|---|---|
| 0 | No findings, or findings below `--fail-on` threshold |
| 1 | Unexpected error (invalid args, engine failure) |
| 2 | Findings at or above `--fail-on` threshold |

**Basic Markdown example:**

```bash
decentdb doctor --db=my.ddb
```

Sample output:

```markdown
# DecentDB Doctor Report

## Status

Overall status: WARNING

## Database

| Field | Value |
|---|---|
| Path | my.ddb |
| Format version | 10 |
| Page size | 4096 |
...

## Summary

| Severity | Count |
|---|---:|
| Error | 0 |
| Warning | 1 |
| Info | 2 |

## Findings

### WARNING wal.large_file -- WAL file is large relative to the database

...
```

**JSON / CI example:**

```bash
decentdb doctor --db=my.ddb --format=json --fail-on=warning
```

The JSON output includes `schema_version`, `mode`, `status`, `database`, `summary`,
`pre_fix_findings`, `findings`, `fixes`, and `collected` objects. This format is the
stable integration surface for CI pipelines and tooling.

**Index verification example:**

```bash
decentdb doctor --db=my.ddb --verify-index=users_name_idx --verify-index=items_sku_idx
```

Expensive verification is **always opt-in**. Default runs do not verify any indexes.

**Fix mode example:**

```bash
decentdb doctor --db=my.ddb --fix
```

When `--fix` is present, doctor:

1. Collects diagnostic findings.
2. Plans eligible fixes from the v1 fix action catalog.
3. Applies fixes in deterministic order.
4. Re-collects facts and re-runs checks.
5. Reports `mode="fix"`, `pre_fix_findings`, `findings`, and `fixes`.

**v1 auto-fixable findings:**

| Fix action | Trigger finding | Precondition |
|---|---|---|
| `fix.checkpoint` | `wal.large_file` | No active readers |
| `fix.rebuild_stale_index` | `schema.index_not_fresh` | Index still exists |
| `fix.rebuild_invalid_index` | `index.verify_failed` | Verification was requested in the same run |

**v1 non-auto-fixable findings** (recommendations only):

- `header.unreadable`
- `database.open_failed`
- `compatibility.format_version_unknown`
- `wal.many_versions`
- `wal.long_readers_present`
- `wal.reader_warnings_recorded`
- `wal.shared_enabled`
- `fragmentation.high`
- `fragmentation.moderate`
- `schema.no_user_tables`
- `schema.many_indexes_on_table`
- `index.verify_error`
- `index.verify_skipped_limit`

Fragmentation is deliberately not auto-fixed because the safe vacuum workflow
writes a separate output database. Use `decentdb vacuum` for that case.

**Recommendation safety:**

- Doctor suggests safe actions by default (e.g., checkpoint commands).
- When `--fix` is present, only the explicit v1 fix action catalog is executed.
- Doctor never overwrites or replaces the source database.
- No destructive operations (source-overwriting vacuum, unsafe compaction) are
  performed.

## Output Formats

`json` renders machine-readable tables or command results.

`csv` renders comma-separated rows.

`table` renders an aligned plain-text table with headers.
