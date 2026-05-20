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
- `--as-of=<snapshot-name>` execute read-only SQL against a named snapshot
- `--as-of-lsn=<lsn>` execute read-only SQL against a retained WAL LSN
- `--branch=<branch>` execute against a branch

`--as-of` and `--as-of-lsn` are read-only time-travel modes. Mutating SQL,
transaction control, PRAGMA commands, and `--checkpoint` are rejected in this
mode.

### repl

Interactive SQL shell with:
- multi-line input
- persistent history in `~/.decentdb_history`
- transaction-aware prompt state

```bash
decentdb repl --db=<path> [--format=<json|csv|table|markdown>]
```

Special commands:
- help aliases: `help`, `\?`, `/?`, `/help`, `\help`, `.help`
- quit aliases: `.quit`, `.exit`, `\q`
- schema inspection: `.tables`, `.dt`, `.d <table>`, `.schema [object]`, `.indexes [table]`, `.views`
- output controls: `.mode`, `.headers`, `.nullvalue`, `.width`, `.timer`
- file workflows: `.read`, `.output`, `.once`, `.import`, `.export`
- query helpers: `.explain`, `.plan`, `.explain-analyze`, `.param`
- session helpers: `.g`, `.s`, `.branch`, `.checkout`

See [Interactive SQL Shell](../user-guide/repl.md) for the full user guide.

### serve

Start a local HTTP API and lightweight Web Console for a database.

```bash
decentdb serve --db=<path> [options]
decentdb serve <path> [options]
```

Supported options:
- `--host=<host>` bind host, default `127.0.0.1`
- `--port=<port>` bind port, default `7373`
- `--read-only` reject mutating SQL
- `--open` open the default browser
- `--max-result-rows=<n>` maximum rows returned per result set, default `1000`
- `--query-timeout=<duration>` query timeout reporting limit, default `30s`
- `--max-body-size=<size>` maximum request body size, default `4mb`
- `--max-concurrent-requests=<n>` concurrent request cap, default `32`
- `--busy-timeout=<duration>` busy timeout configuration, default `5s`
- `--token-env=<name>` environment variable containing the bearer token
- `--show-token` print the bearer token for API clients/debugging
- `--no-auth` disable auth for localhost-only debugging
- `--cors-origin=<origin>` allow one explicit CORS origin
- `--log-format=<text|json>` request log format, default `text`

The default localhost workflow uses transparent ephemeral auth. The Web Console
receives the token in the initial local page; API calls without the token are
rejected. Non-localhost binding requires `--token-env`, and `--no-auth` is
accepted only for localhost binding.

See [Built-In Web Console](../user-guide/web-console.md) for the full user
guide and HTTP API routes.

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

### snapshot

Manage named time-travel snapshots. A named snapshot records the current durable
`main` state and keeps the required history retained until the snapshot is
deleted.

```bash
decentdb snapshot create --db=<path> --name=<snapshot> [--format=<json|table>]
decentdb snapshot list --db=<path> [--format=<json|table>]
decentdb snapshot delete --db=<path> --name=<snapshot> [--format=<json|table>]
decentdb exec --db=<path> --as-of=<snapshot> --sql="SELECT ..."
```

Snapshot rows include `name`, `snapshot_lsn`, `created_at_micros`, `branch_id`,
and `head_id`.

### branch

Manage branch metadata and branch state. Branch creation is metadata-only after
the source state is checkpointed for durability.

```bash
decentdb branch create --db=<path> --name=<branch> [--from=<main|branch|snapshot|head>] [--format=<json|table>]
decentdb branch list --db=<path> [--format=<json|table>]
decentdb branch commit --db=<path> --name=<branch> --message=<message> [--format=<json|table>]
decentdb branch log --db=<path> --name=<branch> [--format=<json|table>]
decentdb branch diff --db=<path> --left=<main|branch|snapshot|head> --right=<main|branch|snapshot|head> [--format=<json|table>]
decentdb branch restore --db=<path> --name=<branch> --to=<branch|snapshot|head> (--dry-run|--confirm) [--format=<json|table>]
decentdb branch merge --db=<path> --source=<branch> --target=<main|branch> (--dry-run|--confirm) [--format=<json|table>]
decentdb branch rename --db=<path> --name=<branch> --new-name=<new-name> [--format=<json|table>]
decentdb branch delete --db=<path> --name=<branch> [--format=<json|table>]
decentdb exec --db=<path> --branch=<branch> --sql="SELECT ..."
decentdb repl --db=<path> --branch=<branch>
```

Branch rows include `name`, `branch_id`, `current_head_id`, `base_head_id`,
`created_at_micros`, and `updated_at_micros`.

Branch-local writes are isolated from `main` until an explicit merge. Diff and
merge operate on primary-key tables; unsupported schema/table cases are reported
as conflicts instead of being applied implicitly. `restore` currently moves a
non-`main` branch head to a branch, snapshot, or head target.

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
- `--include-recommendations[=true|false]` include safe recommendation text and commands, default `true`
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

## Sync commands

The `sync` command group manages local-first replication state. It covers
replica initialization, peer and scope catalogs, batch export/import, HTTP run
transport, conflict inspection, journal pruning, and operational reporting.

Most sync commands emit JSON for machine consumption and tables/key-value rows
for human inspection. Use `--format json` when you need stable downstream
parsing.

### sync init / enable / disable / status / pending

```bash
decentdb sync init --db=<path> --replica-id=<replica>
decentdb sync enable --db=<path>
decentdb sync disable --db=<path>
decentdb sync status --db=<path> [--format=<json|table>]
decentdb sync pending --db=<path> [--since=<n>] [--limit=<n>] [--format=<json|table>]
```

- `sync init` initializes the replica ID, enables sync capture, and opens the
  durable journal sidecar.
- The database file must already exist. Create it first with a normal command
  such as `decentdb exec --db=app.ddb --sql="CREATE TABLE ..."` if needed.
- `sync enable` / `sync disable` toggle capture without changing the replica
  ID.
- `sync status` prints `enabled`, `replica_id`, `next_sequence`,
  `journal_path`, and `journal_size_bytes`.
- `sync pending` shows journal records after a sequence watermark.

Example:

```bash
decentdb sync init --db=app.ddb --replica-id=node-a
decentdb sync status --db=app.ddb --format=table
decentdb sync pending --db=app.ddb --since=0 --limit=5 --format=json
```

Expected `sync status` JSON shape:

```json
{
  "enabled": true,
  "replica_id": "node-a",
  "next_sequence": 2,
  "journal_path": "app.ddb.sync-journal",
  "journal_size_bytes": 512
}
```

### sync export / import

```bash
decentdb sync export --db=<path> --since=<n> --output=<batch.json> [--limit=<n>] [--format=json]
decentdb sync import --db=<path> --input=<batch.json>
```

- `sync export` writes a JSON `SyncChangeBatch` file.
- `sync import` reads that batch file and applies it locally.
- `sync export` currently supports JSON only.

Example:

```bash
decentdb sync export --db=app-a.ddb --since=0 --limit=100 --output=out.batch.json
decentdb sync import --db=app-b.ddb --input=out.batch.json
```

Expected `sync import` summary:

```text
seen=1, applied=1, skipped=0, conflicted=0
```

### sync peer

```bash
decentdb sync peer add --db=<path> --name=<peer> --endpoint=<http-or-https> [--token-env=<ENV>] [--format=<json|table>]
decentdb sync peer remove --db=<path> --name=<peer> [--format=<json|table>]
decentdb sync peer list --db=<path> [--format=<json|table>]
```

- `endpoint` must start with `http://` or `https://`.
- `token-env` stores the environment variable name, not the secret value.

Example:

```bash
decentdb sync peer add --db=app.ddb --name=central --endpoint=http://127.0.0.1:43123
decentdb sync peer list --db=app.ddb --format=table
```

### sync scope

```bash
decentdb sync scope create --db=<path> --name=<scope> --include=<table1,table2> [--row-filter=<expr>] [--format=<json|table>]
decentdb sync scope drop --db=<path> --name=<scope> [--format=<json|table>]
decentdb sync scope list --db=<path> [--format=<json|table>]
decentdb sync scope bind --db=<path> --peer=<peer> --scope=<scope> [--format=<json|table>]
decentdb sync scope unbind --db=<path> --peer=<peer> [--format=<json|table>]
decentdb sync scope bindings --db=<path> [--format=<json|table>]
```

- `include` is a comma-separated list of table names.
- `row-filter` is validated at create time.
- `sync scope unbind` removes the current binding for the peer.

Example:

```bash
decentdb sync scope create --db=app.ddb --name=tenant_42 --include=accounts,orders --row-filter="tenant_id = 42"
decentdb sync scope bind --db=app.ddb --peer=central --scope=tenant_42
decentdb sync scope bindings --db=app.ddb --format=table
```

### sync run / serve

```bash
decentdb sync run --db=<path> --peer=<peer> [--direction=<push|pull|both>] [--limit=<n>] [--retries=<n>] [--conflict-policy=<record|stop|last-writer-wins|origin-priority>] [--format=<json|table>]
decentdb sync serve --db=<path> --bind=<host:port> [--scope=<scope>] [--token-env=<ENV>] [--conflict-policy=<record|stop|last-writer-wins|origin-priority>] [--ready-file=<path>] [--max-requests=<n>]
```

- `sync run` uses the registered peer endpoint and the peer-to-scope binding.
- `sync serve` is a dev/test HTTP endpoint. It is not a hardened public server.
- `direction` defaults to `both`.
- `limit` controls batch size per phase.
- `retries` applies to retryable HTTP failures.

Example:

```bash
decentdb sync run --db=app.ddb --peer=central --direction=both --format=table
decentdb sync serve --db=app.ddb --bind=127.0.0.1:0 --max-requests=3
```

Expected `sync run` table output includes:

- `peer_name`
- `direction`
- `remote_replica_id`
- `retry_count`
- `pushed_batch_id`
- `pulled_batch_id`
- `pushed`
- `pulled`

### sync conflicts / conflict

```bash
decentdb sync conflicts --db=<path> [--all] [--format=<json|table>]
decentdb sync conflict show --db=<path> --id=<conflict-id> [--format=<json|table>]
decentdb sync conflict resolve --db=<path> --id=<conflict-id> --action=<keep-local|apply-remote> [--by=<user>] [--note=<text>] [--format=<json|table>]
decentdb sync conflict reopen --db=<path> --id=<conflict-id> [--format=<json|table>]
decentdb sync conflict policy get --db=<path> [--format=<json|table>]
decentdb sync conflict policy set --db=<path> --policy=<record|stop|last-writer-wins|origin-priority> [--origin-priority=<peer1,peer2,...>] [--format=<json|table>]
```

- `sync conflicts` lists unresolved conflicts by default.
- `--all` includes resolved conflicts too.
- `policy get` / `set` operate on the default conflict policy plus optional
  origin priority list.

Example:

```bash
decentdb sync conflicts --db=app.ddb --format=table
decentdb sync conflict show --db=app.ddb --id=1 --format=json
decentdb sync conflict resolve --db=app.ddb --id=1 --action=keep-local --by=ops --note="manual override"
decentdb sync conflict reopen --db=app.ddb --id=1
```

### sync doctor / prune

```bash
decentdb sync doctor --db=<path> [--format=<json|table>]
decentdb sync prune --db=<path> --through=<sequence> [--dry-run] [--allow-data-loss] [--format=<json|table>]
```

- `sync doctor` aggregates journal integrity, retention, peer lag, unresolved
  conflicts, recent sessions, and guidance strings.
- `sync prune --dry-run` reports what would be removed without rewriting the
  journal.
- `--allow-data-loss` permits pruning beyond the safe watermark.

Example:

```bash
decentdb sync doctor --db=app.ddb --format=table
decentdb sync prune --db=app.ddb --through=42 --dry-run --format=table
```

## Output Formats

`json` renders machine-readable tables or command results.

`csv` renders comma-separated rows.

`table` renders an aligned plain-text table with headers.
