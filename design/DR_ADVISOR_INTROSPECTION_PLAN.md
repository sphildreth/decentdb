# DecentDB Doctor / Advisor / Introspection v1 Plan

**Status:** Proposed implementation plan  
**Roadmap item:** Doctor / Advisor / Introspection v1  
**Primary repositories:** `decentdb` engine and CLI  
**Primary surfaces:** Rust API, DecentDB CLI, JSON/Markdown diagnostics  
**Non-primary surfaces for v1:** Decent Bench UI, broad `sys.*` virtual tables,
sync/branch diagnostics, query history, slow-query capture, destructive repair

---

## Slice Map

All slices start as **Pending**. Update this table as implementation progresses.

Status values:

- `Pending` — not started.
- `In Progress` — active implementation.
- `Blocked` — cannot proceed; blocker documented in the Notes column.
- `Done` — implementation, tests, docs, and quality checks completed.

| Slice | Status | Owner surface | Depends on | Summary | Notes |
|---|---|---|---|---|---|
| DR-00 | Done | Design | none | Confirm scope, constants, output contract, and `--fix` policy | This document is the initial plan |
| DR-01 | Done | Rust engine | DR-00 | Add doctor domain model and report serialization | No CLI yet |
| DR-02 | Pending | Rust engine | DR-01 | Implement read-only fact collection from existing metadata | Must support partial header-only report |
| DR-03 | Pending | Rust engine | DR-02 | Implement v1 rule engine and finding catalog | Use exact IDs from this plan |
| DR-04 | Pending | Rust engine | DR-03 | Add optional index verification integration | Must be opt-in and capped |
| DR-05 | Pending | Rust engine | DR-03, DR-04 | Add constrained `--fix` planner and executor | Only actions in Section 6.6 |
| DR-06 | Pending | CLI | DR-03, DR-05 | Add `decentdb doctor` command, options, JSON output, fix mode, and exit codes | Markdown may be stubbed only if DR-07 follows immediately |
| DR-07 | Pending | CLI | DR-06 | Add Markdown renderer and optional table summary | Markdown must render same report data as JSON |
| DR-08 | Pending | Tests | DR-01 through DR-07 | Add unit, integration, golden, fix, and no-mutation tests | Includes CLI exit-code tests |
| DR-09 | Pending | Docs | DR-06 through DR-08 | Update README/docs/help text and examples | Include CI/agent and `--fix` examples |
| DR-10 | Pending | Quality | DR-09 | Run final quality gates and update slice map | Cannot be Done until checks pass |

---

## 1. Executive Summary

Doctor / Advisor / Introspection v1 makes DecentDB easier to trust, debug, and
automate by turning existing inspection commands into one coherent diagnostic
surface.

The first release must provide:

1. A diagnostic engine in the Rust crate that is read-only unless explicit
   `--fix` execution is requested.
2. A `decentdb doctor` CLI command with stable JSON and Markdown outputs.
3. A small, explicit ruleset focused on storage health, WAL/checkpoint pressure,
   fragmentation, index freshness/integrity, schema/statistics hygiene, and
   compatibility/version visibility.
4. Agent-friendly finding records with deterministic IDs, severity,
   machine-readable evidence, and safe recommendations.
5. Unit tests, CLI tests, golden-output tests, and quality gates as part of the
   definition of done for every implementation slice.

This feature is intentionally not a general repair system. However, v1 must
include `--fix` because users reasonably expect a doctor command to handle
straightforward maintenance issues. The v1 `--fix` contract is deliberately
limited to safe, explicit, engine-supported actions:

- checkpoint when no active reader blocks useful progress
- rebuild stale or verified-invalid indexes
- run `ANALYZE` when missing statistics can be detected reliably

All other findings remain advisory in v1.

---

## 2. Product Goal

### 2.1 One-line story

**DecentDB can explain the health of a database file in a way humans, CI, and
coding agents can act on safely.**

### 2.2 Why this belongs in DecentDB vNext

DecentDB already exposes several useful inspection commands:

- `info`
- `stats`
- `verify-header`
- `verify-index`
- `rebuild-index`
- `rebuild-indexes`
- `vacuum`
- `checkpoint`
- schema listing commands
- rich schema snapshot JSON through the Rust/FFI surfaces
- storage state JSON through the Rust/FFI surfaces

The problem is that these are separate tools. A user or coding agent has to know
which command to run, interpret thresholds manually, and decide what is safe to
do next. Doctor / Advisor / Introspection v1 turns those ingredients into a
single diagnostic report with conservative, explicit recommendations.

### 2.3 Target users

1. **Application developers** embedding DecentDB who need to understand local
   database health without becoming storage experts.
2. **CI pipelines** that need pass/warn/fail behavior before shipping a database
   artifact or migration.
3. **Coding agents** that need structured findings and safe suggested actions.
4. **Support/debug workflows** where a user can attach a redacted Markdown or
   JSON report.
5. **Decent Bench** and other tools that need machine-readable introspection
   without reimplementing engine-specific heuristics.

---

## 3. Non-Goals for v1

The first version must stay deliberately narrow.

The following are **out of scope** for v1:

1. Unbounded automatic repair outside the explicit `--fix` action catalog.
2. Destructive or source-overwriting vacuum/compaction.
3. Any repair that requires guessing user intent.
4. Long-running workload capture or persistent query history.
5. Slow-query advisor based on historical query plans.
6. Usage-based unused-index detection.
7. Hot JSON path detection.
8. Branch, restore, diff, or time-travel diagnostics.
9. Sync lag, publication, subscription, or conflict diagnostics.
10. Full `sys.*` virtual table suite.
11. `PRAGMA doctor` as the primary v1 deliverable.
12. C ABI additions unless the Rust/CLI implementation discovers a required
    stable contract gap.
13. Binding-specific UI work.
14. Security policy/TDE recommendations.
15. External process coordination diagnostics beyond what existing storage/WAL
    metadata can prove.

Important: v1 recommendations must never claim that an operation is safe unless
the engine already guarantees the safety of the referenced command.

---

## 4. Existing Surfaces to Reuse

Implementation agents must reuse existing metadata and command paths before
adding new ones.

### 4.1 Existing CLI commands

| Command | Current role | Doctor reuse |
|---|---|---|
| `decentdb info --db <path> [--schema-summary] --format ...` | Fast storage/header view; graceful fallback for some open failures | Source for storage/header summary and compatibility visibility |
| `decentdb stats --db <path> --format ...` | Full database stats, fragmentation, object counts | Source for physical/logical summary and fragmentation thresholds |
| `decentdb verify-header --db <path>` | Header decoding/verification path | Source for header diagnostic |
| `decentdb verify-index --db <path> --index <name>` | Logical index verification | Source for index integrity checks |
| `decentdb list-tables` | Table names and row counts | Source for schema counts and empty/large table evidence |
| `decentdb list-indexes [--table <name>]` | Index metadata including freshness | Source for stale index findings |
| `decentdb list-views` | View metadata | Source for object summary |
| `decentdb rebuild-index` / `rebuild-indexes` | Explicit repair commands | Recommended action and `--fix` implementation path for stale or verified-invalid indexes |
| `decentdb checkpoint` | WAL checkpoint command | Recommended action and `--fix` implementation path for checkpointable WAL findings |
| `ANALYZE` | SQL statistics refresh | Recommended action and `--fix` implementation path when missing stats can be detected reliably |
| `decentdb vacuum --output <path>` | Rewrite into a compacted output file | Recommended action only in v1; `--fix` must not overwrite or replace the source database |

### 4.2 Existing Rust metadata types

Doctor v1 should collect from existing public types first:

- `StorageInfo`
  - `path`
  - `wal_path`
  - `format_version`
  - `page_size`
  - `cache_size_mb`
  - `page_count`
  - `schema_cookie`
  - `wal_end_lsn`
  - `wal_file_size`
  - `last_checkpoint_lsn`
  - `active_readers`
  - `wal_versions`
  - `warning_count`
  - `shared_wal`
- `HeaderInfo`
  - `magic_hex`
  - `format_version`
  - `page_size`
  - `header_checksum`
  - `schema_cookie`
  - `catalog_root_page_id`
  - `freelist_root_page_id`
  - `freelist_head_page_id`
  - `freelist_page_count`
  - `last_checkpoint_lsn`
- `SchemaSnapshot`
  - `snapshot_version`
  - `schema_cookie`
  - `tables`
  - `views`
  - `indexes`
  - `triggers`
- `IndexVerification`
  - `name`
  - `valid`
  - `expected_entries`
  - `actual_entries`

Doctor v1 may also parse the existing `inspect_storage_state_json()` payload if
that is the least invasive way to reuse memory/deferred-load metrics already
exposed by the engine. If typed access is needed, add a typed Rust structure in
the engine rather than adding another ad hoc string parser.

### 4.3 Existing FFI surfaces

The C ABI already exposes:

- `ddb_db_get_schema_snapshot_json`
- `ddb_db_inspect_storage_state_json`

Doctor v1 does not need to add C ABI functions unless the final Rust API creates
a durable typed diagnostic report intended for bindings. If C ABI exposure is
added, it must be a narrow JSON-export function and must update
`include/decentdb.h`, binding docs, and binding smoke tests.

---

## 5. v1 Design Principles

1. **Read-only by default, explicit mutation with `--fix`.**
   - Doctor must not checkpoint, rebuild, analyze, vacuum, or write metadata
     unless `--fix` is present.
   - Even with `--fix`, only actions listed in Section 6.6 are allowed.
2. **No broad guesses.**
   - A finding must be based on direct metadata, deterministic thresholds, or a
     check result.
3. **Conservative severity.**
   - If a condition is not known to threaten correctness, it should be `info` or
     `warning`, not `error`.
4. **Machine-readable first.**
   - JSON is the canonical report shape.
   - Markdown is a rendering of the same report.
5. **Deterministic output for tests.**
   - Stable ordering.
   - Stable finding IDs.
   - No timestamps in default output.
   - No absolute paths in golden tests unless normalized.
6. **Action recommendations plus constrained execution.**
   - Without `--fix`, recommendations must be explicit commands or SQL
     statements the user may choose to run.
   - With `--fix`, every attempted action must be reported with status,
     evidence, and any error.
7. **Graceful degradation.**
   - If the database cannot fully open but the header can be read, doctor should
     return a partial report with an `error` finding explaining the open failure.
8. **Coding-agent safe.**
   - Every finding must include enough evidence and a safe next step.
   - Do not ask agents to infer thresholds from prose.

---

## 6. Command-Line Interface Contract

### 6.1 New command

Add a new top-level CLI subcommand:

```bash
decentdb doctor --db <path> [options]
```

### 6.2 Required options

| Option | Type | Default | Required | Behavior |
|---|---:|---:|---:|---|
| `--db <path>` | string path | none | yes | Database file to inspect |
| `--format <json|markdown|table>` | enum | `markdown` | no | Output format |
| `--checks <list>` | comma-separated enum list | `all` | no | Limit checks to selected categories |
| `--verify-indexes` | bool | `false` | no | Run expensive logical verification for all selected indexes |
| `--verify-index <name>` | repeatable string | empty | no | Run expensive logical verification for specific index names |
| `--max-index-verify <n>` | integer | `32` | no | Safety cap for `--verify-indexes` |
| `--fail-on <info|warning|error>` | severity enum | `error` | no | Minimum severity that makes process exit non-zero |
| `--include-recommendations` | bool | `true` | no | Include safe recommendation text and commands |
| `--path-mode <absolute|basename|redacted>` | enum | `absolute` | no | Controls path rendering in output |
| `--fix` | bool | `false` | no | Apply v1 auto-fixable actions after the initial diagnosis, then re-run diagnosis |

`--fix` must never imply that all findings can be repaired automatically. It
applies only the explicit v1 fix action catalog in Section 6.6.

### 6.3 Check category names

The `--checks` option must accept these values:

- `all`
- `header`
- `storage`
- `wal`
- `fragmentation`
- `schema`
- `statistics`
- `indexes`
- `compatibility`

Invalid category names must return a non-zero CLI error before opening the
database.

### 6.4 Output formats

#### JSON

JSON is the canonical format and must be stable enough for CI and agents.

#### Markdown

Markdown is the default human-readable format. It must render the same report
data as JSON and must not contain findings that are absent from JSON.

#### Table

Table output is optional for v1 but recommended for consistency with existing
CLI commands. If implemented, table output may summarize findings rather than
rendering full evidence.

### 6.5 Exit codes

| Condition | Exit code |
|---|---:|
| Command-line parse error | Clap default |
| Database inspected and no finding at or above `--fail-on` | `0` |
| Database inspected and at least one finding at or above `--fail-on` | `2` |
| Doctor itself failed unexpectedly before producing a report | `1` |

If the database cannot fully open but a partial header report can be produced,
the command should produce JSON/Markdown and then apply `--fail-on` to the
resulting `error` finding.

When `--fix` is used, exit-code evaluation must use the **post-fix** findings
plus any failed fix actions. A failed fix action must be represented as an
`error` finding so CI cannot silently pass a failed repair attempt.

### 6.6 v1 fix action catalog

The `--fix` implementation may execute only the actions in this table. Agents
must not add additional fix actions without updating this plan first.

| Fix action ID | Trigger finding(s) | Required precondition | Operation | Post-fix verification | If skipped |
|---|---|---|---|---|---|
| `fix.checkpoint` | `wal.large_file` | Database opens normally and `active_readers == 0` | Call existing checkpoint path | Re-collect storage facts and recompute WAL findings | Record skipped fix with reason `active_readers_present` or `open_failed` |
| `fix.rebuild_stale_index` | `schema.index_not_fresh` | Index still exists and database opens normally | Call existing `rebuild_index` for each stale index | Re-collect index metadata and ensure `fresh == true` | Record skipped fix with reason `index_missing` or `open_failed` |
| `fix.rebuild_invalid_index` | `index.verify_failed` | Invalidity was produced by explicit `--verify-index` or `--verify-indexes` in the same run | Call existing `rebuild_index` for that index | Re-run `verify_index` for that index | Record skipped fix with reason `verification_not_requested` or `index_missing` |
| `fix.analyze` | `statistics.missing_analyze` | Missing statistics can be detected by a typed engine helper | Execute `ANALYZE` through the normal SQL execution path | Re-check statistics helper | Record skipped fix with reason `stats_detection_unavailable` |

The following findings are **not auto-fixable in v1**:

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
- `index.verify_ok`

Fragmentation is deliberately not auto-fixed because the current safe compaction
workflow writes a separate output database. v1 may recommend
`decentdb vacuum --db <path> --output <new-path>`, but `--fix` must not replace
or overwrite the source database.

### 6.7 Fix execution report contract

When `--fix` is absent:

- `mode` must be `"check"`.
- `fixes` must be an empty array.
- `pre_fix_findings` may be omitted or an empty array.

When `--fix` is present:

- `mode` must be `"fix"`.
- Doctor must collect findings before applying fixes.
- Doctor must plan eligible fixes from the pre-fix findings.
- Doctor must apply eligible fixes in this order:
  1. `fix.checkpoint`
  2. `fix.rebuild_stale_index`
  3. `fix.rebuild_invalid_index`
  4. `fix.analyze`
- Doctor must re-collect facts and re-run the selected checks after fixes.
- `pre_fix_findings` must contain the findings from before fix execution.
- `findings` must contain the post-fix findings.
- `fixes` must contain every planned, applied, skipped, and failed fix action.

Each fix record must include:

```json
{
  "id": "fix.checkpoint",
  "finding_id": "wal.large_file",
  "status": "applied",
  "message": "Checkpoint completed.",
  "evidence_before": [
    { "field": "wal_file_size", "value": 104857600, "unit": "bytes" }
  ],
  "evidence_after": [
    { "field": "wal_file_size", "value": 32, "unit": "bytes" }
  ]
}
```

Allowed fix statuses:

- `planned`
- `applied`
- `skipped`
- `failed`

The final report must remain useful even when one fix fails. Do not abort the
entire run after the first failed fix unless continuing would risk additional
damage or misleading output. A failed fix must add an `error` finding with ID
`fix.failed`.

---

## 7. Rust API Contract

### 7.1 New module

Add a new engine module:

```rust
crate::doctor
```

Public exports should be re-exported from `crates/decentdb/src/lib.rs` only
after the types are stable enough for CLI use.

### 7.2 Required public types

The names below are the implementation target. Agents should not invent
alternative type names unless a compiler conflict requires it.

```rust
pub struct DoctorOptions {
    pub checks: DoctorCheckSelection,
    pub verify_indexes: DoctorIndexVerification,
    pub include_recommendations: bool,
    pub path_mode: DoctorPathMode,
    pub fix: bool,
}

pub struct DoctorReport {
    pub schema_version: u32,
    pub mode: DoctorMode,
    pub status: DoctorStatus,
    pub database: DoctorDatabaseSummary,
    pub summary: DoctorSummary,
    pub pre_fix_findings: Vec<DoctorFinding>,
    pub findings: Vec<DoctorFinding>,
    pub fixes: Vec<DoctorFix>,
    pub collected: DoctorCollectedFacts,
}

pub enum DoctorMode {
    Check,
    Fix,
}

pub enum DoctorStatus {
    Ok,
    Warning,
    Error,
}

pub enum DoctorSeverity {
    Info,
    Warning,
    Error,
}

pub enum DoctorCategory {
    Header,
    Storage,
    Wal,
    Fragmentation,
    Schema,
    Statistics,
    Indexes,
    Compatibility,
}

pub struct DoctorFinding {
    pub id: String,
    pub severity: DoctorSeverity,
    pub category: DoctorCategory,
    pub title: String,
    pub message: String,
    pub evidence: Vec<DoctorEvidence>,
    pub recommendation: Option<DoctorRecommendation>,
}

pub struct DoctorFix {
    pub id: String,
    pub finding_id: String,
    pub status: DoctorFixStatus,
    pub message: String,
    pub evidence_before: Vec<DoctorEvidence>,
    pub evidence_after: Vec<DoctorEvidence>,
}

pub enum DoctorFixStatus {
    Planned,
    Applied,
    Skipped,
    Failed,
}
```

The actual implementation may add fields, but it must not omit the above
semantic concepts.

### 7.3 Report schema version

Use:

```text
schema_version = 1
```

Increment only when changing the JSON report contract in a backward-incompatible
way.

### 7.4 Error handling

The doctor engine must return `Result<DoctorReport, DbError>` or the local
repository equivalent. Do not use stringly typed broad errors.

The report may contain an `error` finding for database-level issues. Reserve
function-level errors for failures that prevent even a partial report from being
constructed.

---

## 8. Canonical JSON Report Shape

JSON output must follow this shape in v1:

```json
{
  "schema_version": 1,
  "mode": "fix",
  "status": "ok",
  "database": {
    "path": "app.ddb",
    "wal_path": "app.ddb.wal",
    "format_version": 1,
    "page_size": 4096,
    "page_count": 128,
    "schema_cookie": 7
  },
  "summary": {
    "info_count": 0,
    "warning_count": 0,
    "error_count": 0,
    "highest_severity": "ok",
    "checked_categories": [
      "header",
      "storage",
      "wal",
      "fragmentation",
      "schema",
      "statistics",
      "indexes",
      "compatibility"
    ]
  },
  "findings": [],
  "pre_fix_findings": [
    {
      "id": "wal.large_file",
      "severity": "warning",
      "category": "wal",
      "title": "WAL file is large relative to the database",
      "message": "The WAL file is larger than the configured threshold.",
      "evidence": [
        {
          "field": "wal_file_size",
          "value": 104857600,
          "unit": "bytes"
        }
      ],
      "recommendation": {
        "summary": "Run a checkpoint when no long reader is active.",
        "commands": [
          "decentdb checkpoint --db app.ddb"
        ],
        "safe_to_automate": false
      }
    }
  ],
  "fixes": [
    {
      "id": "fix.checkpoint",
      "finding_id": "wal.large_file",
      "status": "applied",
      "message": "Checkpoint completed.",
      "evidence_before": [
        {
          "field": "wal_file_size",
          "value": 104857600,
          "unit": "bytes"
        }
      ],
      "evidence_after": [
        {
          "field": "wal_file_size",
          "value": 32,
          "unit": "bytes"
        }
      ]
    }
  ],
  "collected": {
    "storage": {},
    "header": {},
    "schema": {},
    "indexes_verified": []
  }
}
```

Rules:

1. `findings` and `pre_fix_findings` must be sorted by:
   1. severity rank: `error`, `warning`, `info`
   2. category name
   3. finding ID
2. `checked_categories` must be sorted in the order defined in this plan.
3. `path_mode=basename` must render only file names in `database.path`,
   `database.wal_path`, and recommendation commands.
4. `path_mode=redacted` must render `"<redacted>"` for paths and must omit
   path-bearing commands from recommendations unless a safe placeholder can be
   used.
5. `fixes` must be sorted by execution order.
6. `collected` may initially contain only the fields needed by tests and
   downstream tools, but it must be valid JSON and stable.

---

## 9. Markdown Report Shape

Markdown output must be concise but complete.

Required sections:

````markdown
# DecentDB Doctor Report

## Status

Overall status: WARNING

## Database

| Field | Value |
|---|---|
| Path | app.ddb |
| Format version | 1 |
| Page size | 4096 |
| Page count | 128 |
| Schema cookie | 7 |

## Summary

| Severity | Count |
|---|---:|
| Error | 0 |
| Warning | 1 |
| Info | 2 |

## Fixes

| Fix | Finding | Status | Message |
|---|---|---|---|
| fix.checkpoint | wal.large_file | applied | Checkpoint completed. |

## Findings

### WARNING wal.large_file — WAL file is large relative to the database

The WAL file is larger than the configured threshold.

Evidence:

| Field | Value | Unit |
|---|---:|---|
| wal_file_size | 104857600 | bytes |

Recommendation:

Run a checkpoint when no long reader is active.

```bash
decentdb checkpoint --db app.ddb
```
````

If there are no findings, render:

```markdown
## Findings

No findings.
```

If `--fix` was not used, render:

```markdown
## Fixes

No fixes requested.
```

If `--fix` was used but no finding was auto-fixable, render:

```markdown
## Fixes

No auto-fixable findings were found.
```

---

## 10. v1 Finding Catalog

Implementation agents must use these exact finding IDs for v1.

### 10.1 Header and compatibility findings

| ID | Severity | Category | Condition | Evidence fields | Recommendation |
|---|---|---|---|---|---|
| `header.unreadable` | error | header | Header cannot be read at all | `path`, error message | Verify path, permissions, and file type |
| `database.open_failed` | error | compatibility | Loose header read succeeded but full engine open failed | `format_version`, `page_size`, error message | Use `decentdb info`, migration tooling, or compatible engine version |
| `compatibility.format_version_unknown` | warning | compatibility | Header format version differs from current supported version but open still succeeds | `format_version` | Confirm the engine version expected by the application |

If the existing open path already rejects unsupported versions, use
`database.open_failed` rather than trying to continue with full checks.

### 10.2 WAL findings

| ID | Severity | Category | Condition | Evidence fields | Recommendation |
|---|---|---|---|---|---|
| `wal.large_file` | warning | wal | `wal_file_size >= max(64 MiB, database_physical_bytes / 4)` | `wal_file_size`, `physical_bytes`, threshold | Run `decentdb checkpoint --db <path>` when safe |
| `wal.many_versions` | warning | wal | `wal_versions >= 100000` | `wal_versions` | Check for long readers; checkpoint when safe |
| `wal.long_readers_present` | warning | wal | `active_readers > 0` and WAL is non-empty | `active_readers`, `wal_file_size` | Close long-running readers before checkpoint-sensitive operations |
| `wal.reader_warnings_recorded` | warning | wal | `warning_count > 0` | `warning_count` | Inspect application read transaction lifetime |
| `wal.shared_enabled` | info | wal | `shared_wal == true` | `shared_wal` | Informational; shared WAL is enabled |

The thresholds above are v1 constants. Do not make them configurable until a
later slice unless tests require injecting thresholds.

### 10.3 Fragmentation findings

| ID | Severity | Category | Condition | Evidence fields | Recommendation |
|---|---|---|---|---|---|
| `fragmentation.high` | warning | fragmentation | `page_count >= 128` and `freelist_page_count / page_count >= 0.25` | `page_count`, `freelist_page_count`, `fragmentation_percent` | Consider `decentdb vacuum --db <path> --output <new-path>` |
| `fragmentation.moderate` | info | fragmentation | `page_count >= 128` and ratio is `>= 0.10` and `< 0.25` | same | Monitor; vacuum only if file size matters |

Do not recommend overwriting the source database in v1. Use a new output path in
recommendation text.

### 10.4 Schema and statistics findings

| ID | Severity | Category | Condition | Evidence fields | Recommendation |
|---|---|---|---|---|---|
| `schema.no_user_tables` | info | schema | Schema snapshot has zero persistent user tables | `table_count` | Informational for empty databases |
| `schema.many_indexes_on_table` | info | schema | A table has more than 8 indexes | `table`, `index_count` | Review write overhead; keep indexes that match query patterns |
| `schema.index_not_fresh` | warning | indexes | Any index metadata has `fresh == false` | `index`, `table` | Run `decentdb rebuild-index --db <path> --index <index>` |
| `statistics.missing_analyze` | info | statistics | Persistent tables exist but no persisted stats can be observed | `table_count` | Run `ANALYZE` if query planning quality matters |

If the current public metadata cannot reliably detect persisted stats, implement
`statistics.missing_analyze` only after adding a typed, tested engine helper.
Do not infer missing stats from row counts alone.

### 10.5 Index verification findings

These findings require explicit `--verify-index` or `--verify-indexes`.

| ID | Severity | Category | Condition | Evidence fields | Recommendation |
|---|---|---|---|---|---|
| `index.verify_failed` | error | indexes | `verify_index` returns invalid result | `index`, `expected_entries`, `actual_entries` | Run `decentdb rebuild-index --db <path> --index <index>`, then rerun doctor |
| `index.verify_error` | error | indexes | `verify_index` returns an error for a selected index | `index`, error message | Inspect index name and database integrity |
| `index.verify_skipped_limit` | info | indexes | `--verify-indexes` selected more than `--max-index-verify` indexes | `selected_count`, `max_index_verify` | Verify specific indexes or increase cap |
| `index.verify_ok` | info | indexes | A selected index verified successfully and verbose/collected output records it | `index`, `expected_entries`, `actual_entries` | No action |

By default, `index.verify_ok` should appear in `collected.indexes_verified`, not
as a top-level finding, unless table output needs a visible summary.

### 10.6 Fix execution findings

| ID | Severity | Category | Condition | Evidence fields | Recommendation |
|---|---|---|---|---|---|
| `fix.failed` | error | compatibility | A requested `--fix` action failed | `fix_id`, `finding_id`, error message | Review the failed action, rerun doctor without `--fix`, and apply the recommended command manually if safe |

`fix.failed` is the only generic fix execution finding in v1. Skipped fixes are
reported in the `fixes` array, not as findings, unless the skipped condition is
itself already represented by another finding such as
`wal.long_readers_present`.

## 12. Slice Details and Acceptance Criteria

### DR-00 — Scope and constants confirmation

**Goal:** Lock the v1 behavior so implementation agents do not make product
decisions mid-stream.

Required work:

1. Keep this document as the source of truth for v1 scope.
2. If implementation reveals a necessary behavior change, update this document
   before changing code.
3. Do not add fix actions beyond the explicit v1 catalog in Section 6.6.
4. Do not add new dependencies.
5. Do not introduce on-disk format changes.
6. Do not change WAL/checkpoint semantics.

Acceptance criteria:

- The slice map remains present.
- Every changed behavior has a slice and acceptance criteria.
- Any new public API surface is explicitly tied to a slice.

Definition of done:

- Documentation change reviewed for internal consistency.
- Local Markdown links resolve.

### DR-01 — Doctor domain model

**Goal:** Add typed doctor report structures that can be serialized to JSON and
rendered to Markdown.

Required work:

1. Add `crates/decentdb/src/doctor.rs`.
2. Define typed severity, category, status, finding, evidence, recommendation,
   summary, database summary, collected facts, options, and report structures.
3. Use `serde::Serialize` for JSON output.
4. Implement deterministic sorting for findings.
5. Implement summary calculation from findings.
6. Re-export stable public types from `lib.rs` only when needed by the CLI.

Required tests:

- Unit test: report with no findings has `DoctorStatus::Ok`.
- Unit test: report with warning has `DoctorStatus::Warning`.
- Unit test: report with error has `DoctorStatus::Error`.
- Unit test: sorting is `error` before `warning` before `info`, then category,
  then ID.
- Unit test: JSON serialization uses expected lowercase enum strings.

Definition of done:

- Tests pass for the new module.
- No `unwrap()` or `expect()` in library paths except narrowly justified test
  code.
- `cargo fmt --check` passes.

### DR-02 — Read-only fact collection

**Goal:** Gather all facts needed for v1 findings without mutating the database.

Required work:

1. Implement a collector that accepts a database path and `DoctorOptions`.
2. First attempt loose header read with `Db::read_header_info`.
3. Then attempt normal `Db::open`.
4. If open succeeds, collect:
   - `StorageInfo`
   - `HeaderInfo`
   - `SchemaSnapshot`
   - list of indexes
   - optional storage-state memory/deferred-load details if available through a
     typed helper
5. If loose header succeeds but open fails, construct a partial report with
   `database.open_failed`.
6. If loose header fails, construct a report with `header.unreadable` if enough
   context exists; otherwise return an explicit error.
7. Ensure collection does not call:
   - `checkpoint`
   - `save_as`
   - `rebuild_index`
   - `rebuild_indexes`
   - `ANALYZE`
   - `VACUUM`

Required tests:

- Unit/integration test: empty valid database produces a complete report.
- Unit/integration test: missing or invalid file produces `header.unreadable` or
  a clear command error.
- Unit/integration test: a readable header with an open failure produces
  `database.open_failed` if such fixture can be created safely.
- Regression test: doctor collection does not change `last_checkpoint_lsn`,
  `schema_cookie`, or WAL file size on an unchanged database.

Definition of done:

- Read-only behavior is tested.
- Partial report behavior is tested where feasible.
- No new file-format, WAL, or checkpoint behavior is introduced.

### DR-03 — Rule engine and v1 finding catalog

**Goal:** Convert collected facts into deterministic findings using the exact
finding IDs and thresholds in this document.

Required work:

1. Implement each finding in Section 10 that can be supported by current typed
   metadata.
2. If `statistics.missing_analyze` lacks reliable support, leave it unimplemented
   and document the missing helper in code comments and this plan before
   marking DR-03 Done.
3. Evidence fields must use stable names.
4. Recommendations must be safe and must clearly indicate whether a finding is
   auto-fixable by v1 `--fix`.
5. Recommendations must include commands only when `include_recommendations` is
   true and `path_mode` permits safe path rendering.

Required tests:

- Unit test for every rule condition.
- Unit test for every threshold boundary:
  - just below threshold
  - exactly at threshold
  - above threshold
- Unit test for recommendation suppression when `include_recommendations=false`.
- Unit test for `path_mode=basename`.
- Unit test for `path_mode=redacted`.

Definition of done:

- Every implemented v1 finding ID has a test.
- No unplanned finding IDs are introduced.
- Finding IDs remain stable and documented.

### DR-04 — Opt-in index verification

**Goal:** Integrate expensive logical index verification only when explicitly
requested.

Required work:

1. `--verify-index <name>` verifies named indexes.
2. `--verify-indexes` verifies all indexes up to `--max-index-verify`.
3. If selected indexes exceed the cap, verify none beyond the cap and emit
   `index.verify_skipped_limit`.
4. Invalid verification produces `index.verify_failed`.
5. Verification errors produce `index.verify_error`.
6. Successful verification is recorded in `collected.indexes_verified`.

Required tests:

- Integration test: valid index selected with `--verify-index` records success.
- Integration test: selected unknown index records or returns a clear error as
  specified by the final implementation contract.
- Unit test: `--verify-indexes` cap produces `index.verify_skipped_limit`.
- Regression test: default doctor run does not call `verify_index`.

Definition of done:

- Expensive verification is opt-in.
- Verification cap is enforced.
- CLI and Rust options are aligned.

### DR-05 — Constrained fix planner and executor

**Goal:** Add `--fix` support without turning doctor into an unbounded repair
system.

Required work:

1. Add a fix planner that maps pre-fix findings to the exact fix action catalog
   in Section 6.6.
2. Add a fix executor that applies actions in the exact order from Section 6.7.
3. Reuse existing engine paths:
   - checkpoint path for `fix.checkpoint`
   - `rebuild_index` for index repairs
   - normal SQL execution path for `ANALYZE`
4. Re-collect facts and re-run selected checks after fix execution.
5. Populate:
   - `mode`
   - `pre_fix_findings`
   - `findings`
   - `fixes`
6. Add `fix.failed` error findings for failed fixes.
7. Skip, rather than force, any fix whose precondition is not met.
8. Do not implement source-overwriting vacuum or destructive compaction.

Required tests:

- Unit test: each auto-fixable finding plans the expected fix action.
- Unit test: non-auto-fixable findings plan no fix.
- Unit test: fix execution order is deterministic.
- Integration test: `--fix` checkpoint reduces or updates WAL state when no
  active reader blocks progress, if a small deterministic fixture is feasible.
- Integration test: stale index repair rebuilds the index and post-fix findings
  no longer contain `schema.index_not_fresh`, if a small deterministic fixture is
  feasible.
- Unit/integration test: fix precondition failure records a skipped fix.
- Unit/integration test: fix execution failure records `fix.failed`.
- Regression test: doctor without `--fix` does not mutate database state.

Definition of done:

- `--fix` mutates only through allowed v1 actions.
- Every fix action has a test or a documented fixture blocker.
- Fix failures cannot produce a success-shaped report.
- Post-fix findings drive exit-code behavior.

### DR-06 — CLI command and JSON output

**Goal:** Add `decentdb doctor` with complete JSON behavior and exit codes.

Required work:

1. Add `Doctor(DoctorCommand)` to `Commands`.
2. Add `DoctorCommand` with all options in Section 6.
3. Parse check categories before opening the database.
4. Call the Rust doctor engine.
5. Wire `--fix` to the fix planner/executor.
6. Render canonical JSON for `--format json`.
7. Apply `--fail-on` to process exit code using post-fix findings when
   `--fix` is present.
8. Ensure unexpected doctor failure returns exit code `1`.
9. Ensure findings at/above threshold return exit code `2`.

Required tests:

- CLI test: `decentdb doctor --db valid.ddb --format json` emits valid JSON.
- CLI test: JSON contains `schema_version=1`.
- CLI test: default `--fail-on error` exits `0` for warning-only report.
- CLI test: `--fail-on warning` exits `2` for warning report.
- CLI test: `--fix` output has `mode="fix"`, `pre_fix_findings`, and `fixes`.
- CLI test: failed fix exits `2` through `fix.failed`.
- CLI test: invalid `--checks` fails before opening database.
- CLI test: `--format json` output is parseable by `serde_json`.

Definition of done:

- CLI help includes the new command and options.
- JSON output is golden-tested or structurally asserted.
- Exit-code behavior is tested.

### DR-07 — Markdown renderer and optional table summary

**Goal:** Provide human-readable output without diverging from JSON semantics.

Required work:

1. Add Markdown renderer for `DoctorReport`.
2. Set CLI default format to Markdown unless local CLI conventions require table.
3. Render required sections from Section 9.
4. Ensure Markdown renders fix results when `--fix` is used.
5. Ensure Markdown renders no findings when JSON has no findings.
6. Optional: implement table summary for `--format table`.

Required tests:

- Unit/golden test: no-finding Markdown report.
- Unit/golden test: warning report with evidence and recommendation.
- Unit/golden test: fix report with applied, skipped, and failed fixes.
- Unit/golden test: path redaction applies to Markdown.
- CLI test: default output is Markdown.

Definition of done:

- Markdown golden tests are deterministic.
- Markdown and JSON derive from the same `DoctorReport`.

### DR-08 — Test fixtures and quality coverage

**Goal:** Build enough fixture coverage that future agents can change rules
without breaking output contracts silently.

Required work:

1. Add small fixture builders in Rust tests for:
   - empty valid database
   - database with tables and indexes
   - database with stale/rebuilt index scenario if feasible
   - database with fragmentation if feasible without slow tests
   - database with WAL growth if feasible without slow tests
2. Add golden JSON and Markdown fixtures with path normalization.
3. Add tests for check filtering.
4. Add tests for deterministic ordering.
5. Add tests proving doctor does not mutate database state unless `--fix` is
   present.
6. Add tests proving `--fix` mutates only through the allowed fix action catalog.

Required validation commands:

```bash
cargo fmt --check
cargo check -p decentdb
cargo test -p decentdb -- doctor
cargo test -p decentdb-cli -- doctor
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

If `cargo test -p decentdb-cli -- doctor` is not a valid crate-level command
after implementation, replace it with the closest targeted CLI test command and
document the exact command in this plan.

Definition of done:

- All targeted doctor tests pass.
- Formatting passes.
- Clippy passes with `-D warnings`.
- Any skipped fixture is explained in this document before the slice is marked
  Done.

### DR-09 — User docs and examples

**Goal:** Make the feature discoverable and safe to use.

Required work:

1. Update CLI README or docs with:
   - basic Markdown example
   - JSON/CI example
   - `--fail-on warning` example
   - index verification example
   - `--fix` example
   - explanation that doctor is read-only unless `--fix` is present
   - list of v1 auto-fixable and non-auto-fixable findings
2. Add sample output snippets.
3. Document recommendation safety:
   - doctor suggests actions by default
   - doctor runs only the v1 fix action catalog when `--fix` is present
4. If Decent Bench will consume JSON later, document that JSON is the stable
   integration surface.

Required tests/checks:

- Link check if a documentation link checker exists.
- Otherwise manually verify referenced files and commands exist.

Definition of done:

- Docs include read-only-by-default guarantee.
- Docs include CI/agent JSON example.
- Docs include expensive verification warning.
- Docs include `--fix` safety boundaries.

### DR-10 — Final quality gate and slice map update

**Goal:** Ensure the implementation is complete and the plan reflects reality.

Required work:

1. Update the slice map statuses.
2. Ensure any intentionally deferred finding is documented.
3. Run final validation.
4. Capture any known limitations.

Required validation commands:

```bash
cargo fmt --check
cargo check -p decentdb
cargo test -p decentdb -- doctor
cargo test -p decentdb-cli -- doctor
cargo clippy --workspace --all-targets --all-features -- -D warnings
python ./scripts/do-pre-commit-checks.py --mode fast
```

If a command is unavailable in the environment, document:

- exact command attempted
- failure reason
- whether the failure is environmental or product-related

Definition of done:

- All implemented slices are `Done`.
- No open doctor-specific TODOs remain without an owner slice.
- Fast pre-commit validation passes or a clear environmental blocker is
  documented.

---

## 13. Overall Definition of Done

Doctor / Advisor / Introspection v1 is complete only when all of the following
are true:

1. `decentdb doctor --db <path>` exists.
2. JSON output follows schema version `1`.
3. Markdown output is available and is the default human-readable report.
4. The command is read-only unless `--fix` is explicitly present.
5. The command can produce a partial report for open failures when a loose header
   can be read.
6. `--checks` filtering works.
7. `--fail-on` exit-code behavior works.
8. `--fix` applies only the v1 fix action catalog and reports before/after
   findings plus fix action statuses.
9. Index verification is opt-in and capped.
10. Every implemented finding ID is documented.
11. Every implemented finding ID has at least one test.
12. Every implemented fix action ID is documented.
13. Every implemented fix action ID has at least one test or documented fixture
    blocker.
14. Unit tests cover rule thresholds.
15. CLI tests cover JSON parseability and exit codes.
16. Golden tests or structural assertions cover Markdown.
17. Documentation explains read-only-by-default behavior, `--fix` boundaries,
    and safe recommendations.
18. These commands pass:

```bash
cargo fmt --check
cargo check -p decentdb
cargo test -p decentdb -- doctor
cargo test -p decentdb-cli -- doctor
cargo clippy --workspace --all-targets --all-features -- -D warnings
python ./scripts/do-pre-commit-checks.py --mode fast
```

19. If a final full repository validation is requested for release, run:

```bash
python ./scripts/do-pre-commit-checks.py
```

---

## 14. Agent Implementation Rules

Agents implementing this plan must follow these rules:

1. Do not add dependencies unless explicitly approved.
2. Do not add fix actions beyond the explicit v1 fix action catalog.
3. Do not mutate database state during doctor collection unless `--fix` is
   explicitly present.
4. Do not introduce new on-disk format records for v1.
5. Do not change WAL/checkpoint semantics.
6. Do not expose a C ABI doctor function unless a slice is added for binding
   validation.
7. Do not rename finding IDs after tests are written unless this document is
   updated first.
8. Do not add broad `catch all` logic that converts real errors into success.
9. Do not silently omit a requested check; if unsupported, emit a clear finding
   or command error.
10. Keep CLI output deterministic enough for tests.

---

## 15. Future Work After v1

These are intentionally deferred:

1. `PRAGMA doctor`.
2. `sys.doctor_findings` or broader `sys.*` virtual tables.
3. Decent Bench doctor panel.
4. Optional explicit `doctor --fix-plan` that emits a script without executing
   it.
5. Additional fix actions beyond checkpoint, index rebuild, and `ANALYZE`.
6. Query-plan advisor.
7. Missing-index advisor based on observed query workload.
8. Unused-index advisor based on observed query workload.
9. JSON path advisor.
10. Branch/restore/diff diagnostics.
11. Sync diagnostics.
12. Binding-level doctor report APIs.
13. Diagnostic bundles for support workflows.
