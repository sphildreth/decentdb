# Doctor

`decentdb doctor` inspects a DecentDB database and returns a deterministic
health report for humans, automation, and support workflows. It is read-only by
default and can optionally run a small, explicit catalog of safe repair actions
with `--fix`.

Use Doctor when you want to answer questions like:

- Is this database readable by the current engine?
- Is the WAL growing because checkpoints are blocked or delayed?
- Are indexes stale or logically inconsistent?
- Is the file fragmented enough to justify a vacuum workflow?
- Which findings should fail CI or deployment checks?
- Which safe maintenance action should be run next?

Doctor is a diagnostic and maintenance assistant. It is not a backup tool, does
not replace recovery testing, and does not perform destructive compaction or
source-file replacement.

## Quick start

Run the default human-readable report:

```bash
decentdb doctor --db app.ddb
```

Write a machine-readable report:

```bash
decentdb doctor --db app.ddb --format json > doctor-report.json
```

Fail CI when warnings or errors are present:

```bash
decentdb doctor --db app.ddb --format json --fail-on warning
```

Verify all indexes up to the default cap:

```bash
decentdb doctor --db app.ddb --verify-indexes
```

Run the safe v1 fix catalog:

```bash
decentdb doctor --db app.ddb --fix
```

## Command shape

```bash
decentdb doctor --db <path> [options]
```

| Option | Default | Description |
|---|---:|---|
| `--format <markdown|json|table|csv>` | `markdown` | Output format. Doctor v1 renders `table` and `csv` as Markdown. |
| `--checks <list>` | `all` | Comma-separated category filter. |
| `--verify-index <name>` | none | Verify one named index. May be repeated. |
| `--verify-indexes` | `false` | Verify all indexes up to `--max-index-verify`. |
| `--max-index-verify <n>` | `32` | Cap for `--verify-indexes`. |
| `--fail-on <error|warning|info>` | `error` | Exit with code `2` when a finding at or above this severity exists. |
| `--include-recommendations[=true|false]` | `true` | Include or suppress recommendation text and commands. |
| `--path-mode <absolute|basename|redacted>` | `absolute` | Controls how paths appear in reports. |
| `--fix` | `false` | Run the constrained v1 fix catalog and report before/after state. |

Category names accepted by `--checks` are:

```text
header,storage,wal,fragmentation,schema,statistics,indexes,compatibility
```

Use `all` to run every available category:

```bash
decentdb doctor --db app.ddb --checks all
```

Run only WAL and index checks:

```bash
decentdb doctor --db app.ddb --checks wal,indexes --verify-indexes
```

## Exit codes

| Exit code | Meaning |
|---:|---|
| `0` | Command completed and no finding met the `--fail-on` threshold. |
| `1` | Command-line parsing or report rendering failed. |
| `2` | Doctor completed, but at least one finding met the `--fail-on` threshold. |

The default threshold is `error`, so warning and info findings do not fail the
command unless you choose a stricter threshold.

Examples:

```bash
# Only fail on errors.
decentdb doctor --db app.ddb --fail-on error

# Fail on warnings and errors.
decentdb doctor --db app.ddb --fail-on warning

# Fail on any finding, including informational findings.
decentdb doctor --db app.ddb --fail-on info
```

## Report status and severity

Doctor findings use three severities:

| Severity | Report status effect | Typical meaning |
|---|---|---|
| `error` | `status: "error"` | The database cannot be inspected fully, an index verification failed, or a fix action failed. |
| `warning` | `status: "warning"` | Maintenance is recommended, such as checkpointing a large WAL or rebuilding a stale index. |
| `info` | `status: "ok"` when no warnings/errors exist | Informational state or non-urgent advice. |

Findings are sorted deterministically by severity, category, and finding ID, so
JSON reports are stable enough for automation and diffing.

## Markdown output

Markdown is the default output and is intended for terminals, support tickets,
and human review:

```bash
decentdb doctor --db app.ddb
```

The report contains:

1. Overall status.
2. Database summary.
3. Severity summary.
4. Fix records when `--fix` is used.
5. Findings with evidence and recommendations.

Example shape:

```markdown
# DecentDB Doctor Report

## Status

Overall status: WARNING

## Database

| Field | Value |
|---|---|
| Path | app.ddb |
| Format version | 10 |
| Page size | 4096 |
| Page count | 128 |
| Schema cookie | 4 |

## Summary

| Severity | Count |
|---|---:|
| Error | 0 |
| Warning | 1 |
| Info | 1 |
```

## JSON output

Use JSON when another tool needs to parse Doctor results:

```bash
decentdb doctor --db app.ddb --format json
```

Top-level fields:

| Field | Description |
|---|---|
| `schema_version` | Doctor report schema version. v1 reports use `1`. |
| `mode` | `check` or `fix`. |
| `status` | `ok`, `warning`, or `error`. |
| `database` | Database path, WAL path, format version, page size, page count, and schema cookie. |
| `summary` | Finding counts, highest severity, and checked categories. |
| `pre_fix_findings` | Findings observed before `--fix` actions. Empty in check mode. |
| `findings` | Current findings after checks, or after fixes in fix mode. |
| `fixes` | Planned/applied/skipped/failed fix records. Empty in check mode. |
| `collected` | Stable facts collected from engine metadata, including verified-index records. |

Minimal example:

```json
{
  "schema_version": 1,
  "mode": "check",
  "status": "warning",
  "database": {
    "path": "app.ddb",
    "wal_path": "app.ddb.wal",
    "format_version": 10,
    "page_size": 4096,
    "page_count": 128,
    "schema_cookie": 4
  },
  "summary": {
    "info_count": 1,
    "warning_count": 1,
    "error_count": 0,
    "highest_severity": "warning",
    "checked_categories": ["header", "storage", "wal", "fragmentation", "schema", "statistics", "indexes", "compatibility"]
  },
  "pre_fix_findings": [],
  "findings": [],
  "fixes": [],
  "collected": {
    "storage": {},
    "header": {},
    "schema": {},
    "indexes_verified": []
  }
}
```

## Using Doctor in CI

For CI, prefer JSON plus an explicit `--fail-on` threshold:

```bash
decentdb doctor --db app.ddb --format json --fail-on warning > doctor.json
```

A common policy is:

- Development smoke checks: `--fail-on error`
- Release gates: `--fail-on warning`
- Strict observability baselines: `--fail-on info`

When paths in CI logs should not expose workspace or tenant names, combine JSON
with `--path-mode basename` or `--path-mode redacted`:

```bash
decentdb doctor \
  --db /var/lib/my-app/customer-123/app.ddb \
  --format json \
  --path-mode redacted \
  --fail-on warning
```

## Path rendering modes

`--path-mode` controls paths in the report:

| Mode | Behavior |
|---|---|
| `absolute` | Render the path exactly as provided. |
| `basename` | Render only file names, such as `app.ddb` and `app.ddb.wal`. |
| `redacted` | Render `<redacted>` for database and WAL paths. |

Use `basename` for local logs where the file name is helpful but the full
directory is not. Use `redacted` for multi-tenant logs, support bundles, or any
environment where paths may contain customer identifiers.

## Recommendations

By default, findings include safe recommendation text and, when useful, command
snippets:

```bash
decentdb doctor --db app.ddb
```

Suppress recommendations when a machine consumer only needs finding IDs,
severities, and evidence:

```bash
decentdb doctor --db app.ddb --format json --include-recommendations=false
```

Suppressing recommendations does not suppress findings, evidence, fixes, or
exit-code behavior.

## Index verification

Normal Doctor runs use metadata and do not perform a full logical scan of every
index. Add index verification when you need stronger index-integrity evidence.

Verify one index:

```bash
decentdb doctor --db app.ddb --verify-index users_email_idx
```

Verify several indexes:

```bash
decentdb doctor \
  --db app.ddb \
  --verify-index users_email_idx \
  --verify-index orders_user_id_idx
```

Verify all indexes up to the default cap:

```bash
decentdb doctor --db app.ddb --verify-indexes
```

Raise the cap for large schemas:

```bash
decentdb doctor --db app.ddb --verify-indexes --max-index-verify 128
```

When the all-index request exceeds the cap, Doctor reports
`index.verify_skipped_limit` and verifies only the capped subset. This prevents a
surprise full-schema verification from turning a quick health check into a long
maintenance job.

Verified index summaries appear in JSON under:

```json
{
  "collected": {
    "indexes_verified": [
      {
        "index": "users_email_idx",
        "expected_entries": 1000,
        "actual_entries": 1000
      }
    ]
  }
}
```

If verification fails, Doctor emits an `index.verify_failed` error with expected
and actual entry counts. In `--fix` mode, this finding is eligible for
`fix.rebuild_invalid_index`.

## Fix mode

Doctor is read-only unless `--fix` is provided:

```bash
decentdb doctor --db app.ddb --fix
```

Fix mode follows this sequence:

1. Collect facts and run selected checks.
2. Store the initial findings in `pre_fix_findings`.
3. Plan only v1-supported fix actions.
4. Execute the planned fix actions.
5. Re-collect facts and re-run selected checks.
6. Store current findings in `findings`.
7. Store every planned/applied/skipped/failed action in `fixes`.

Fix records use these statuses:

| Status | Meaning |
|---|---|
| `planned` | The action was selected before execution. Final reports should normally show a terminal status instead. |
| `applied` | The fix action completed. |
| `skipped` | Doctor deliberately did not run the action, usually because a safety condition was not met. |
| `failed` | The action returned an error. Doctor also emits a `fix.failed` error finding. |

### Auto-fixable findings in v1

| Finding | Fix action | Behavior |
|---|---|---|
| `wal.large_file` | `fix.checkpoint` | Runs a checkpoint when no active readers are present. |
| `schema.index_not_fresh` | `fix.rebuild_stale_index` | Rebuilds the stale index named in finding evidence. |
| `index.verify_failed` | `fix.rebuild_invalid_index` | Rebuilds the invalid index named in verification evidence. |

Doctor does not auto-vacuum fragmented databases. The safe vacuum workflow writes
a separate output database, so Doctor recommends an explicit command instead:

```bash
decentdb vacuum --db app.ddb --output app-vacuumed.ddb
```

Doctor also does not replace the source database, delete files, rewrite the file
format, or run unsafe compaction.

### Recommended fix workflow

For production databases:

1. Take or verify a backup using your normal backup process.
2. Run Doctor without fixes and save the report.
3. Review the findings and planned safe actions.
4. Run Doctor with `--fix`.
5. Run Doctor again without `--fix` and compare the report.

Example:

```bash
decentdb doctor --db app.ddb --format json > before.json
decentdb doctor --db app.ddb --fix --format json > fixed.json
decentdb doctor --db app.ddb --format json --fail-on warning > after.json
```

## Common findings

| Finding ID | Severity | Meaning | Typical action |
|---|---|---|---|
| `header.unreadable` | `error` | The file header cannot be read. | Check path, permissions, and file type. |
| `database.open_failed` | `error` | Header was readable, but the engine could not fully open the database. | Use `decentdb info`, migration tooling, or a compatible engine version. |
| `compatibility.format_version_unknown` | `warning` | Header format differs from the current engine format but opened successfully. | Confirm the expected engine version. |
| `wal.large_file` | `warning` | WAL size exceeds the configured v1 threshold. | Checkpoint when readers are not holding snapshots; `--fix` may do this safely. |
| `wal.many_versions` | `warning` | Many page versions are retained in the WAL. | Look for long readers or checkpoint starvation. |
| `wal.long_readers_present` | `warning` | Active readers are holding WAL space. | Close long readers before checkpoint-sensitive operations. |
| `wal.reader_warnings_recorded` | `warning` | Reader warnings have been recorded. | Inspect read transaction lifetime. |
| `wal.shared_enabled` | `info` | Shared WAL mode is enabled. | Usually informational. |
| `fragmentation.high` | `warning` | Free-list pages are high relative to total pages. | Consider `decentdb vacuum --db <path> --output <new-path>`. |
| `fragmentation.moderate` | `info` | Some reusable free-list space exists. | Monitor; vacuum only if file size matters. |
| `schema.no_user_tables` | `info` | No persistent user tables were found. | Usually expected for new databases. |
| `schema.many_indexes_on_table` | `info` | A table has many indexes. | Review write overhead and query patterns. |
| `schema.index_not_fresh` | `warning` | Index metadata says the index is stale. | Rebuild the index; `--fix` may do this safely. |
| `index.verify_skipped_limit` | `info` | `--verify-indexes` exceeded the verification cap. | Verify specific indexes or raise `--max-index-verify`. |
| `index.verify_failed` | `error` | Logical index entry count differs from table expectations. | Rebuild the index; `--fix` may do this safely. |
| `index.verify_error` | `error` | Verification could not run for the index. | Check the index name and database state. |
| `fix.failed` | `error` | A requested fix action failed. | Review the fix record and rerun Doctor without `--fix`. |

## Troubleshooting

### Doctor exits with code 2

Exit code `2` means Doctor completed and found something at or above your
`--fail-on` threshold. Inspect `findings` in JSON or the Findings section in
Markdown. Lower the threshold only if your policy allows it:

```bash
decentdb doctor --db app.ddb --fail-on error
```

### A large WAL finding remains after `--fix`

`fix.checkpoint` is skipped when active readers are present. Close long-running
readers and rerun:

```bash
decentdb doctor --db app.ddb --fix --checks wal
```

### Index verification takes longer than a normal Doctor run

Index verification is opt-in because it performs stronger logical checks than
metadata-only diagnostics. Use named verification for targeted checks:

```bash
decentdb doctor --db app.ddb --verify-index users_email_idx
```

### The report hides paths

Check `--path-mode`. `basename` intentionally removes parent directories and
`redacted` intentionally replaces paths with `<redacted>`.

### Recommendations are missing

Check whether the command used:

```bash
--include-recommendations=false
```

When disabled, findings and evidence remain present but recommendation text and
commands are omitted.

