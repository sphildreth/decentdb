# Runtime Tracing, Advisors, And Doctor Integration

**Date:** 2026-06-09
**Status:** TODO
**Future Version:** vNext+1
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Document Type:** Future Win SPEC
**Audience:** Core engine maintainers, planner/executor maintainers, WAL and write-queue maintainers, process coordination maintainers, Doctor/tooling authors, C ABI maintainers, binding maintainers, WASM/browser/mobile maintainers, Decent Bench authors, docs authors, coding agents

## 1. Executive Summary

DecentDB already exposes stable, cheap operational snapshots through `sys.*`
views and already has a deterministic `decentdb doctor` workflow for static
database health checks. Those surfaces answer questions such as "what is the WAL
state right now?", "which readers exist right now?", and "is this database file
structurally healthy?" They do not yet answer runtime questions such as:

- Which statements were slow in the last minute?
- Which lock source caused this write to wait?
- Which index actually served this workload?
- Which index was never used during a representative run?
- Which Doctor findings should be projected into SQL rows for automation?
- Which advisor recommendations are safe to apply automatically, and which must
  become reviewed migration work?

This win adds an opt-in runtime tracing layer, advisor model, and Doctor
integration that builds on the shipped operational metrics contract without
turning DecentDB into a telemetry daemon.

The design is intentionally bounded:

- tracing is disabled by default unless an ADR explicitly approves a cheaper
  default for a narrow event family;
- trace buffers are in memory, fixed size, and resettable;
- default telemetry never records raw parameter values;
- SQL text capture is redacted and policy-controlled;
- no trace event writes to the database file, WAL, or a sidecar log by default;
- advisors produce reviewable findings, evidence, and fix plans rather than
  surprise schema changes;
- Doctor remains deterministic and read-only unless the user explicitly requests
  a narrow, documented safe fix.

The feature is a strong fit for DecentDB when implemented as embedded,
bounded, opt-in diagnostics. It is not a fit as always-on persistent telemetry,
external observability plumbing, server-style session management, or automatic
schema rewriting.

## 2. Current Source Of Truth

This specification expands the roadmap item in
[`FUTURE_WINS.md`](FUTURE_WINS.md): "Runtime tracing, advisors, and Doctor
integration."

It depends on these shipped design inputs:

- [`adr/0163-operational-sys-metrics.md`](adr/0163-operational-sys-metrics.md)
  for stable read-only operational `sys.*` metrics.
- [`adr/0185-rich-structured-error-diagnostics-contract.md`](adr/0185-rich-structured-error-diagnostics-contract.md)
  for structured diagnostics, redaction, and Doctor handoff expectations.
- [`WIN_RICH_STRUCTURED_ERRORS_DEVELOPER_DIAGNOSTICS_SPEC.md`](_archive/WIN_RICH_STRUCTURED_ERRORS_DEVELOPER_DIAGNOSTICS_SPEC.md)
  for implemented structured error payload behavior and the "diagnostics are
  cheap handoffs, not live tracing" boundary.
- [`docs/api/sql-functions.md`](../docs/api/sql-functions.md) for the current
  operational inspection SQL surface.
- [`docs/user-guide/doctor.md`](../docs/user-guide/doctor.md) for Doctor CLI
  behavior, categories, fix behavior, and JSON output shape.
- [`WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`](_archive/WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md)
  for write/process coordination context that lock-wait tracing must classify.
- [`adr/0184-default-fast-planner-and-runtime-contract.md`](adr/0184-default-fast-planner-and-runtime-contract.md)
  for the default-fast expectation that runtime diagnostics must not undermine.

This specification is governed by these accepted ADRs:

- [`adr/0186-runtime-tracing-contract-and-redaction.md`](adr/0186-runtime-tracing-contract-and-redaction.md)
  for opt-in bounded tracing, redaction defaults, overhead targets, and
  persistent/export telemetry boundaries.
- [`adr/0187-runtime-sys-views-and-doctor-projection.md`](adr/0187-runtime-sys-views-and-doctor-projection.md)
  for `sys.sessions`, `sys.slow_queries`, `sys.lock_waits`,
  `sys.index_usage`, `sys.doctor_findings`, and explicit Doctor refresh
  semantics.
- [`adr/0188-runtime-advisors-and-fix-plan-policy.md`](adr/0188-runtime-advisors-and-fix-plan-policy.md)
  for advisor findings, confidence, evidence, recommendations, and Doctor
  `--fix-plan` automation boundaries.
- [`adr/0189-runtime-tracing-api-and-binding-contract.md`](adr/0189-runtime-tracing-api-and-binding-contract.md)
  for Rust, C ABI, binding, CLI, Decent Bench, browser, mobile, and WASM
  exposure.

## 3. ADR Coverage And Follow-Up Decisions

ADR 0186-0189 establish the initial implementation contract for this win. The
following decision areas are covered by that ADR set:

1. The disabled-by-default tracing contract, including exactly which event
   families are off by default, which are cheap enough to be always-on counters,
   and how the default-fast profile is preserved.
2. The explicit hot-path overhead budget for disabled tracing and enabled
   tracing. This must include read-heavy, write-heavy, and mixed workloads.
3. The in-memory ring buffer model, including per-connection versus per-`Db`
   storage, cross-connection visibility, maximum sizes, eviction semantics,
   memory accounting, and reset behavior.
4. The SQL text redaction and fingerprinting policy, including whether template
   SQL without literal values is captured by default.
5. The parameter redaction policy. The default must not store parameter values.
6. The statement identity model across direct execution, prepared statements,
   internal statements, triggers if added later, and binding surfaces.
7. The timestamp and duration model, including monotonic clocks for durations
   and optional wall-clock timestamps for human reports.
8. Lock-wait source classification across write queue, WAL/checkpoint locks,
   process coordination locks, transaction locks, schema/index rebuild locks,
   and VFS/file-lock waits.
9. Index-usage attribution semantics across the planner, executor, index
   maintenance, fast paths, virtual/system tables, future JSONB indexes, future
   FTS/trigram/spatial indexes, and fallback scans.
10. Doctor report schema versioning if new categories, runtime evidence, or
    fix-plan objects change the JSON shape.
11. The `sys.doctor_findings` projection contract and whether it is a projection
    of the last Doctor report, an on-demand virtual report, or both.
12. The `PRAGMA doctor` contract, if exposed, including whether it returns rows,
    JSON, or delegates to `sys.doctor_findings`.
13. The `doctor --fix-plan` safety boundary, output format, and relationship to
    existing `doctor --fix` behavior.
14. The C ABI surface for tracing configuration and snapshots. Prefer SQL views
    for read access at first, but any direct C ABI additions need versioning.
15. Binding configuration conventions for Python, Node, Go, Java, .NET, and
    Dart if tracing can be enabled at open time.
16. Interaction with TDE, path redaction, support bundles, and audit policy.
17. Whether future external export bridges such as OpenTelemetry are explicitly
    excluded from this phase or introduced behind a separate extension point.

Follow-up ADRs are still required for persistent telemetry tables, WAL or file
format changes, durable trace sidecars, raw parameter capture, cross-process
trace sharing, external telemetry export, automatic schema mutations, or broad
new C ABI telemetry surfaces.

## 4. Product Goals

### 4.1 Operational Visibility

Users should be able to inspect recent runtime behavior from inside the
database connection using stable SQL views:

```sql
SELECT * FROM sys.slow_queries;
SELECT * FROM sys.lock_waits;
SELECT * FROM sys.index_usage;
SELECT * FROM sys.sessions;
SELECT * FROM sys.doctor_findings;
```

The views should help support and application developers answer common embedded
database questions without attaching a profiler:

- "Which query was slow?"
- "Was it slow because of planning, execution, I/O, lock waiting, or row count?"
- "Was the writer blocked by readers, WAL checkpointing, process coordination,
  or the application transaction lifecycle?"
- "Which indexes mattered during this workload?"
- "Which expensive table scan has a plausible index recommendation?"
- "What does Doctor currently think about this database, in a queryable form?"

### 4.2 Advisor Quality

Advisor output must be evidence based. Each recommendation must include:

- a stable advisor identifier;
- severity;
- confidence;
- affected object names when safe to disclose;
- observed evidence;
- user impact;
- recommended action;
- whether the action is safe to automate;
- an optional fix-plan action identifier;
- explicit limitations when the evidence is incomplete.

An advisor without evidence is worse than no advisor. The design should prefer a
small number of high-confidence findings over a large number of noisy hints.

### 4.3 Doctor Integration

Doctor should consume runtime tracing when available, but Doctor must not depend
on tracing being enabled. Static Doctor checks continue to work for cold
database files, support bundles, and read-only diagnostics.

Runtime-aware Doctor should add:

- runtime/performance categories or subcategories;
- trace-window metadata;
- findings derived from recent slow query, lock-wait, and index-usage evidence;
- stable JSON for automation;
- `sys.doctor_findings` projection for SQL tooling;
- `doctor --fix-plan` output for reviewed remediation workflows.

### 4.4 Embedded-First Shape

DecentDB is an embedded database. This feature should feel like a lightweight
debugging and support surface, not a server monitoring stack.

The intended usage model is:

1. Enable tracing for a workload, test run, support session, benchmark, or
   temporary production diagnostic window.
2. Run representative workload.
3. Inspect `sys.*` views, Doctor JSON, or Decent Bench panels.
4. Review advisor findings.
5. Apply explicit migration, index rebuild, checkpoint, or configuration
   changes under user control.
6. Disable tracing or reset buffers.

## 5. Non-Goals

This win does not introduce:

- persistent telemetry tables written to the user database;
- WAL records for trace events;
- a telemetry sidecar file by default;
- external observability export such as OpenTelemetry;
- server-style connection pooling or global session management;
- automatic schema rewrites;
- automatic `CREATE INDEX` or `DROP INDEX` behavior;
- plan hints, plan pinning, or forced plan baselines;
- raw parameter capture by default;
- always-on full SQL capture by default;
- stack traces on normal query execution;
- per-row tracing;
- full flamegraph/profiling support;
- distributed tracing across processes;
- browser/mobile telemetry upload;
- public stability for internal advisor implementation details.

External export, support-bundle packaging, and OpenTelemetry-style integrations
can build on this later, but they are out of scope for this feature.

## 6. Existing Foundation

### 6.1 Shipped Operational Metrics

ADR 0163 delivered read-only `sys.*` metrics as virtual inspection surfaces.
Those views are cheap snapshots and have a deliberately conservative stability
contract. Documentation currently promises that the `SELECT * FROM sys.name`
form is stable; arbitrary predicates, joins, and projections are not required
to have full optimizer support.

This win should follow the same approach for new runtime views:

- they are virtual read-only inspection views;
- they do not create user-visible schema objects;
- their rows are generated from engine state;
- the documented `SELECT *` form is the primary compatibility promise;
- unsupported write attempts fail with structured diagnostics;
- implementation avoids recursive calls into SQL execution while holding
  internal locks.

### 6.2 Shipped Doctor

Doctor already provides:

- deterministic health reports;
- text and JSON output;
- path redaction modes;
- selectable check categories;
- index verification controls;
- recommendations;
- narrow explicit safe fixes;
- report schema versioning.

Existing categories include:

- `header`
- `storage`
- `wal`
- `fragmentation`
- `schema`
- `statistics`
- `indexes`
- `compatibility`

Existing severities include:

- `info`
- `warning`
- `error`

Existing fixable findings are intentionally narrow, such as large WAL
checkpoint recommendations and stale/invalid index rebuilds.

Runtime advisors should reuse these concepts rather than inventing a separate
finding model.

### 6.3 Shipped Structured Diagnostics

Structured diagnostics already include:

- stable machine-readable error codes;
- SQLSTATE mapping;
- severity;
- operation context;
- redacted paths;
- hints;
- Doctor handoff commands and SQL;
- JSON access through the C ABI.

The diagnostics contract explicitly avoids live tracing and stack capture on the
hot path. Runtime tracing should preserve that distinction:

- error diagnostics describe one failure;
- runtime tracing describes recent behavior over a bounded window;
- Doctor/advisors convert facts into findings and recommendations.

### 6.4 Shipped Coordination Metrics

Cross-process coordination and write queue surfaces already provide facts that
future lock-wait tracing can reference:

- process coordination state;
- known readers;
- process lock metrics;
- write queue metrics;
- WAL metrics;
- storage metrics.

Runtime lock-wait events should not duplicate every existing counter. They
should capture the temporal "this wait happened" evidence that point-in-time
metrics cannot retain.

## 7. Design Principles

### 7.1 Disabled Means Cheap

When runtime tracing is disabled:

- statement execution must not allocate trace event objects;
- slow-query buffers must not allocate;
- lock-wait events must not allocate;
- index-usage events must not allocate;
- branch checks should be predictable and local;
- no SQL text normalization should run for telemetry only;
- no additional locks should be acquired solely for tracing;
- no atomics should be added to the hottest inner loops unless an ADR approves
  the measured overhead.

Existing always-on counters may remain always-on when they are already part of
the shipped metrics contract, but this win must not quietly turn trace history
into always-on work.

### 7.2 Enabled Means Bounded

When runtime tracing is enabled:

- memory use is bounded by explicit buffer sizes;
- event payloads have fixed maximum sizes;
- SQL text capture has byte limits;
- object name capture has byte limits;
- eviction is deterministic;
- reset is explicit;
- snapshots do not block writers for long periods;
- large result sets are truncated with visible metadata;
- every view exposes enough metadata to explain truncation and eviction.

### 7.3 Evidence Before Advice

Advisor findings should be generated from concrete evidence:

- observed slow-query events;
- observed lock waits;
- observed index use or non-use over a stated window;
- catalog metadata;
- row-count and statistics facts;
- Doctor structural checks;
- explicit benchmark/advisor runs.

Advisors should not pretend that an observation window proves a universal truth.
For example, "index was not used during a 30 second trace window" is evidence,
not proof that the index is never useful.

### 7.4 No Recursive Telemetry Writes

Trace capture must not write to user tables, system tables, WAL, or sidecar
files by default. A query against `sys.slow_queries` must not itself create
slow-query trace events unless an ADR explicitly chooses a carefully bounded
debug mode.

Internal inspection queries must be marked as internal and excluded from normal
runtime advisor conclusions.

### 7.5 Redaction By Default

Default runtime tracing should be safe enough for routine support use:

- no raw parameter values;
- no raw SQL literals unless explicitly enabled;
- redacted paths;
- bounded object names;
- no secret connection strings;
- no encryption keys;
- no unredacted application metadata;
- no raw values from rows scanned or returned.

## 8. Runtime Tracing Model

### 8.1 Three Tiers

Runtime observability should have three tiers.

**Tier 1: cheap snapshots**

These are existing or future always-cheap counters and state snapshots exposed
through `sys.*` views. Examples include WAL metrics, storage metrics, process
coordination state, and write queue metrics.

Tier 1 is appropriate for state that can be collected cheaply and does not
require a history buffer.

**Tier 2: opt-in trace history**

These are bounded in-memory event buffers. Examples include slow query events,
lock wait events, session lifecycle events, and index usage events.

Tier 2 is appropriate for "what happened recently?" questions.

**Tier 3: advisors and Doctor**

These are analysis surfaces that consume Tier 1 snapshots, Tier 2 trace history,
catalog metadata, statistics, and direct verification checks. They produce
findings and recommendations.

Tier 3 is appropriate for "what should I do?" questions.

### 8.2 Scope Of Trace State

The initial implementation should prefer process-local in-memory trace state.
No trace state is durable across process exit. No trace state is shared across
unrelated processes unless a later ADR explicitly adds a shared diagnostic
transport.

Within a process, trace state must be scoped carefully:

- connection/session state belongs to the connection/session that owns it;
- statement events belong to the `Db` handle or connection that executed them;
- shared process-level lock events may be visible to all handles that share the
  same underlying database coordinator;
- path identities must be redacted according to existing path mode policy;
- temporary schemas and connection-local state must not leak into unrelated
  connections.

The spec uses `session_id` for a logical connection/session identity and
`connection_id` for the concrete engine handle identity. If DecentDB does not
distinguish those today, the initial implementation can make them identical and
reserve the distinction for future pooling/binding layers.

### 8.3 Configuration Surface

ADR 0189 establishes Rust-owned typed configuration and SQL-first read access.
The implementation should support a config shape similar to:

```rust
pub struct RuntimeTracingConfig {
    pub enabled: bool,
    pub slow_query: SlowQueryTraceConfig,
    pub lock_wait: LockWaitTraceConfig,
    pub index_usage: IndexUsageTraceConfig,
    pub sessions: SessionTraceConfig,
    pub sql_text: SqlTextTraceConfig,
    pub memory_budget_bytes: usize,
}
```

Individual event families should be independently configurable:

- slow query tracing;
- lock-wait tracing;
- index-usage tracing;
- session lifecycle tracing;
- advisor event tracing, if needed for debugging;
- Doctor projection cache, if any.

Configuration must be possible before opening a database. Runtime changes are
useful but not required in Phase 1. If runtime changes are supported, they must
be synchronized without races and must define what happens to existing buffers.

### 8.4 SQL Configuration Surface

SQL-level configuration is useful for interactive diagnosis, but it must be
designed carefully because DecentDB is embedded and bindings may prefer open
options.

Candidate SQL controls:

```sql
PRAGMA runtime_tracing;
PRAGMA runtime_tracing = ON;
PRAGMA runtime_tracing = OFF;
PRAGMA runtime_tracing_reset;
PRAGMA runtime_tracing_reset('slow_queries');
PRAGMA slow_query_threshold_ms = 50;
```

These names are illustrative. ADR 0186 and ADR 0189 require explicit
configuration and safe synchronization; the implementation spec should finalize
names and decide whether `PRAGMA` state is connection-local,
database-handle-local, or shared for the process.

The default should be:

- runtime tracing off;
- slow query threshold unset or conservative;
- no raw SQL literals;
- no parameter values;
- bounded ring buffers with small defaults when enabled.

### 8.5 Suggested Default Limits When Enabled

Exact defaults require benchmarking. ADR 0186 requires bounded buffers and a
global tracing memory budget; the implementation should benchmark this starting
profile:

| Buffer | Default When Enabled | Hard Minimum | Hard Maximum Without Explicit Override |
|---|---:|---:|---:|
| slow queries | 256 events | 16 | 16,384 |
| lock waits | 512 events | 16 | 65,536 |
| index usage | 1,024 aggregate rows | 16 | 65,536 |
| sessions | 256 current/recent sessions | 16 | 16,384 |
| Doctor findings cache | 1 latest report | 0 | 8 reports |
| SQL template bytes per event | 512 bytes | 0 | 8 KiB |
| object name bytes per event | 256 bytes | 0 | 2 KiB |

The implementation should enforce a global trace memory budget so combinations
of options cannot allocate unbounded memory.

## 9. Common Event Envelope

All trace events should share a common internal envelope so snapshots, SQL
projections, Doctor evidence, and JSON output can use consistent metadata.

Candidate fields:

| Field | Type | Notes |
|---|---|---|
| `event_id` | `u64` | Monotonic process-local event identifier. |
| `event_family` | enum/string | `statement`, `lock_wait`, `index_usage`, `session`, `doctor`, etc. |
| `session_id` | `u64` | Logical session/connection identity. |
| `connection_id` | `u64` | Concrete handle identity, if distinct. |
| `thread_id_hash` | text/integer | Optional redacted thread identity for debugging. |
| `started_at_unix_ms` | integer/null | Optional wall-clock timestamp. |
| `started_at_monotonic_us` | integer | Monotonic process-local timestamp. |
| `duration_us` | integer/null | Duration when event has a duration. |
| `database_id_hash` | text | Redacted database identity. |
| `path_mode` | text | Path redaction mode used for any path-like evidence. |
| `status` | text | `ok`, `error`, `cancelled`, `truncated`, etc. |
| `error_code` | text/null | Structured error code when relevant. |
| `internal` | boolean | Whether event came from internal engine work. |
| `evicted_before_event_id` | integer/null | Snapshot metadata can expose oldest retained id. |

Durations must use monotonic clocks. Wall-clock timestamps are for humans and
reports only. If wall-clock capture is too expensive or too platform-specific
for some targets, the implementation may expose monotonic-only rows and leave
wall-clock fields null.

## 10. SQL Text And Statement Identity

### 10.1 Statement Fingerprint

Every statement event should have a stable statement fingerprint when possible.
The fingerprint should be derived from redacted or normalized structure, not
from raw parameter values.

Candidate fingerprint inputs:

- parser-stable normalized SQL form;
- statement kind;
- schema generation or plan invalidation generation;
- relevant connection-local schema generation for temp objects;
- security/audit policy generation where it can affect plan shape;
- placeholder arity and parameter type classes, not values.

If parser-stable normalization is not available cheaply, Phase 1 can use a hash
of the exact SQL text after redaction policy is applied. This is less powerful
for grouping but safer than inventing brittle string normalization.

### 10.2 SQL Template Capture

Default SQL capture should be one of:

- `none`: only hash/fingerprint is stored;
- `template`: SQL with literals replaced by placeholders;
- `redacted`: SQL truncated and literal values redacted;
- `full`: raw SQL text, explicit debug opt-in only.

The recommended default is `template` only if the parser can produce it safely
without meaningful hot-path cost when tracing is enabled. Otherwise use `none`
or `redacted` for Phase 1.

Parameter values must not be stored by default. Even explicit debug modes should
require clear naming and documentation because SQL parameters frequently contain
secrets, personal data, tokens, document text, and user content.

### 10.3 Internal Statements

Internal SQL or internal execution paths must be labeled. Examples include:

- `sys.*` view materialization;
- Doctor queries;
- schema introspection;
- migration parser checks;
- index verification;
- benchmark harness metadata queries.

By default, internal statements should be excluded from advisors and either
excluded from slow-query views or shown only with `internal = true` so users do
not tune the engine around its own inspection work.

## 11. Slow Query Tracing

### 11.1 Purpose

`sys.slow_queries` should expose recent statements whose end-to-end execution
time exceeds a configurable threshold. It should also support sampling of all
statements in development mode if the ADR approves it.

The view should answer:

- what ran slowly;
- how slow it was;
- whether the time was planning, lock waiting, execution, or result production;
- whether the statement succeeded or failed;
- how many rows were produced or affected when known;
- what plan summary and index usage were associated with the statement.

### 11.2 Candidate Columns

```text
event_id INTEGER NOT NULL
session_id INTEGER NOT NULL
connection_id INTEGER NOT NULL
started_at_unix_ms INTEGER
duration_us INTEGER NOT NULL
threshold_us INTEGER NOT NULL
statement_kind TEXT NOT NULL
read_only BOOLEAN NOT NULL
sql_fingerprint TEXT NOT NULL
sql_template TEXT
sql_text_mode TEXT NOT NULL
database_id_hash TEXT NOT NULL
schema_generation INTEGER
plan_generation INTEGER
prepare_cache_hit BOOLEAN
planning_us INTEGER
lock_wait_us INTEGER
execution_us INTEGER
rows_returned INTEGER
rows_affected INTEGER
page_reads INTEGER
page_writes INTEGER
wal_bytes INTEGER
used_indexes TEXT
plan_summary TEXT
status TEXT NOT NULL
error_code TEXT
internal BOOLEAN NOT NULL
truncated BOOLEAN NOT NULL
```

The initial implementation does not need every column, but it should reserve a
compatible story for each category of information. If a field cannot be
collected cheaply, return null rather than guessing.

### 11.3 Timing Breakdown

End-to-end duration should be captured first. Detailed breakdowns are optional
and can land in later phases:

- parse duration;
- bind duration;
- plan duration;
- prepared-statement lookup duration;
- write queue wait duration;
- lock wait duration;
- execution duration;
- row materialization duration;
- commit/checkpoint duration.

Breakdowns should be additive only when measured with a coherent clock model.
If not additive, names and docs must make that clear.

### 11.4 Threshold Policy

The default threshold must avoid noise. Candidate policies:

- disabled until user sets a threshold;
- enabled with a high default such as 100 ms when tracing is enabled;
- adaptive threshold based on benchmark mode, only for Decent Bench.

ADR 0186 selects the safest Phase 1 default: slow-query tracing remains disabled
until the user sets a threshold or enables a named diagnostic profile.

### 11.5 Interaction With Prepared Statements

Prepared statement execution should report:

- whether a prepared plan/cache entry was used;
- whether it was invalidated and recompiled;
- whether execution observed a schema/security generation mismatch;
- the stable statement fingerprint.

This is useful evidence for the separate plan caching win but must not change
prepared statement semantics.

## 12. Lock-Wait Tracing

### 12.1 Purpose

`sys.lock_waits` should expose recent waits that are long enough to matter.
Point-in-time process lock metrics can show current contention; lock-wait traces
show historical contention and source classification.

The view should answer:

- what waited;
- what it waited for;
- how long it waited;
- whether the wait was caused by application transaction scope, readers, WAL,
  checkpointing, process coordination, VFS/file locks, schema work, or index
  maintenance;
- whether the wait completed, timed out, or failed;
- what statement or operation was affected.

### 12.2 Candidate Columns

```text
event_id INTEGER NOT NULL
session_id INTEGER NOT NULL
connection_id INTEGER NOT NULL
started_at_unix_ms INTEGER
wait_duration_us INTEGER NOT NULL
wait_source TEXT NOT NULL
lock_kind TEXT NOT NULL
operation TEXT NOT NULL
statement_event_id INTEGER
sql_fingerprint TEXT
database_id_hash TEXT NOT NULL
owner_session_id INTEGER
owner_process_id_hash TEXT
owner_thread_id_hash TEXT
reader_count INTEGER
writer_queue_depth INTEGER
checkpoint_generation INTEGER
wal_generation INTEGER
status TEXT NOT NULL
error_code TEXT
internal BOOLEAN NOT NULL
truncated BOOLEAN NOT NULL
```

### 12.3 Source Classification

Initial source values should be boring and explicit:

- `write_queue`
- `sql_write_lock`
- `transaction_writer`
- `wal_append`
- `wal_checkpoint`
- `process_coordination`
- `process_reader`
- `schema_lock`
- `index_build`
- `index_verify`
- `storage_io`
- `vfs_file_lock`
- `unknown`

Do not overclassify before the engine can prove the source. `unknown` is
acceptable and honest. Misclassification creates bad advice.

### 12.4 Wait Event Threshold

Not every lock acquisition should become an event. The implementation should use
one or more thresholds:

- minimum wait duration;
- maximum events per second;
- sampling rate;
- only waits that affect user-visible statements;
- always capture timeout/deadline failures.

The threshold must apply before expensive event payload construction.

### 12.5 Lock Safety

Tracing must not hold internal locks while formatting SQL text, allocating large
objects, running advisors, or calling back into SQL. Capture minimal primitive
facts while the lock context is active, then finish richer event construction
after releasing the hot lock.

## 13. Index Usage Tracing

### 13.1 Purpose

`sys.index_usage` should expose index usage over a bounded observation window.
It should support both operational inspection and advisor recommendations.

The view should answer:

- which indexes were used;
- how they were used;
- when they were last used;
- how often they were used;
- whether they were used for reads, uniqueness checks, constraint checks,
  ordering, covering reads, joins, maintenance, or writes;
- whether writes paid maintenance cost for indexes that did not help reads
  during the observed window;
- which table scans might be candidates for new indexes.

### 13.2 Candidate Columns

```text
database_id_hash TEXT NOT NULL
schema_name TEXT
table_name TEXT NOT NULL
index_name TEXT NOT NULL
index_id INTEGER
index_kind TEXT NOT NULL
first_seen_unix_ms INTEGER
last_seen_unix_ms INTEGER
read_uses INTEGER NOT NULL
write_maintenance_uses INTEGER NOT NULL
constraint_uses INTEGER NOT NULL
ordering_uses INTEGER NOT NULL
covering_uses INTEGER NOT NULL
join_uses INTEGER NOT NULL
planner_candidate_uses INTEGER
planner_rejected_uses INTEGER
last_statement_fingerprint TEXT
last_statement_event_id INTEGER
estimated_rows_read INTEGER
estimated_rows_skipped INTEGER
observed_rows_read INTEGER
observed_rows_returned INTEGER
observation_window_us INTEGER NOT NULL
evicted BOOLEAN NOT NULL
truncated BOOLEAN NOT NULL
```

The first implementation can start with much less:

- table name;
- index name;
- read use count;
- write maintenance count;
- last used timestamp;
- observation window;
- last statement fingerprint.

### 13.3 Use Attribution

Index usage should distinguish at least these categories:

- read lookup;
- range scan;
- full index scan;
- covering lookup;
- ordering;
- join lookup when joins become richer;
- uniqueness/constraint enforcement;
- foreign-key support when applicable;
- write maintenance.

This distinction matters because "unused for reads" does not mean "safe to
drop" if an index enforces uniqueness or supports constraints.

### 13.4 Index Usage And Future Index Families

The design must leave room for future index types:

- B-tree indexes;
- expression indexes;
- partial indexes;
- covering indexes;
- JSONB path indexes;
- FTS indexes;
- trigram indexes;
- vector indexes if added later;
- spatial indexes if added later.

Advisor text must name the index kind and state which capabilities it
understands. For example, a Phase 1 unused-index advisor should not claim to
understand future FTS index utility unless it actually does.

### 13.5 Missing Index Evidence

Missing index recommendations should require multiple facts:

- a slow statement or high-cost statement was observed;
- the plan performed a large table scan or expensive sort/filter;
- predicates or ordering columns were identified from the parsed plan;
- table cardinality/statistics make the scan meaningful;
- an existing index did not already cover the shape;
- write amplification risk is considered;
- the confidence level reflects uncertainty.

A single slow query is not automatically enough to recommend an index. Sometimes
the correct advice is to update statistics, rewrite a query, fix transaction
scope, or reduce lock contention.

## 14. Session Tracing

### 14.1 Purpose

`sys.sessions` should expose current and recent connection/session lifecycle
state without implying that DecentDB has a server pool manager.

The view should answer:

- which engine handles are active in this process;
- which are in a transaction;
- which are holding or waiting for write access;
- which recently closed;
- which have tracing enabled;
- which host language or binding opened the connection when known;
- which path redaction policy applies.

### 14.2 Candidate Columns

```text
session_id INTEGER NOT NULL
connection_id INTEGER NOT NULL
database_id_hash TEXT NOT NULL
opened_at_unix_ms INTEGER
closed_at_unix_ms INTEGER
state TEXT NOT NULL
binding TEXT
thread_id_hash TEXT
transaction_state TEXT
read_transaction_age_us INTEGER
write_transaction_age_us INTEGER
current_statement_event_id INTEGER
current_sql_fingerprint TEXT
tracing_enabled BOOLEAN NOT NULL
slow_query_threshold_us INTEGER
lock_wait_threshold_us INTEGER
last_activity_unix_ms INTEGER
internal BOOLEAN NOT NULL
```

### 14.3 Lifecycle Boundaries

Session tracing must be robust across:

- normal close;
- panic/unwind in Rust callers;
- FFI caller errors;
- language binding finalizers;
- process exit where no close event is possible;
- connections opened before tracing is enabled.

Rows for closed sessions should be retained only in a bounded recent-session
buffer. Current active sessions may be backed by live state, but snapshots must
not keep connection objects alive accidentally.

## 15. Doctor Findings SQL Projection

### 15.1 Purpose

`sys.doctor_findings` should expose Doctor findings as rows so SQL tooling,
bindings, tests, and Decent Bench can consume health/advisor output without
shelling out to the CLI.

### 15.2 Projection Model

ADR 0187 selects the hybrid projection model.

**Option A: on-demand projection**

`SELECT * FROM sys.doctor_findings` runs a bounded Doctor collection and returns
rows. This is simple for users but risks expensive work behind a query.

**Option B: latest report projection**

Doctor runs explicitly through CLI/API/PRAGMA, and `sys.doctor_findings` returns
the latest report cached in memory. This avoids surprise work but requires users
to know how to refresh.

**Option C: hybrid**

`sys.doctor_findings` returns the latest cached report by default, while
`PRAGMA doctor` or a table-valued function refreshes it explicitly.

Option C is accepted because it keeps the row projection cheap while still
giving SQL users an explicit refresh path.

### 15.3 Candidate Columns

```text
report_id TEXT NOT NULL
report_schema_version INTEGER NOT NULL
generated_at_unix_ms INTEGER
mode TEXT NOT NULL
status TEXT NOT NULL
finding_id TEXT NOT NULL
severity TEXT NOT NULL
category TEXT NOT NULL
title TEXT NOT NULL
message TEXT NOT NULL
evidence_json TEXT
recommendation_summary TEXT
recommendation_command TEXT
recommendation_sql TEXT
safe_to_automate BOOLEAN NOT NULL
advisor_id TEXT
confidence TEXT
source TEXT NOT NULL
source_event_id INTEGER
trace_window_start_unix_ms INTEGER
trace_window_end_unix_ms INTEGER
path_mode TEXT NOT NULL
```

### 15.4 Doctor Category Extensions

If runtime findings become first-class Doctor categories, consider adding:

- `runtime`
- `performance`
- `contention`
- `advisors`

ADR 0188 adds advisor categories to the finding model. If implementation adds
new Doctor category values or changes the report JSON shape, it should increment
the report schema version as needed and update compatibility tests.

### 15.5 PRAGMA Doctor

The roadmap calls out `PRAGMA doctor`. A useful shape is:

```sql
PRAGMA doctor;
PRAGMA doctor('summary');
PRAGMA doctor('refresh');
PRAGMA doctor('runtime');
PRAGMA doctor('fix_plan');
```

The final syntax must match DecentDB's PRAGMA conventions. The key design
choice is that refresh and fix-plan generation are explicit. A plain `SELECT`
from a virtual view should not unexpectedly run expensive index verification or
deep runtime analysis.

## 16. Advisor Model

### 16.1 Finding Identity

Advisor findings need stable identifiers. Candidate naming:

```text
advisor.query.slow_full_scan
advisor.query.expensive_sort
advisor.query.plan_regression
advisor.index.missing_candidate
advisor.index.unused_observed
advisor.index.redundant_prefix
advisor.index.write_amplification
advisor.schema.missing_primary_key
advisor.schema.suspicious_affinity
advisor.contention.long_writer_wait
advisor.contention.reader_retention
advisor.wal.checkpoint_lag
advisor.stats.stale_table_stats
advisor.sync.unsupported_schema_shape
```

Finding IDs should be stable once documented. Internal rule names can change;
public finding IDs should not churn.

### 16.2 Severity

Reuse existing Doctor severity where possible:

- `info`: worth knowing or useful during tuning;
- `warning`: likely performance, operational, or maintainability issue;
- `error`: correctness, durability, compatibility, or severe availability risk.

Most performance advisor findings should be `info` or `warning`. An index
advisor should almost never emit `error` unless there is a correctness risk such
as invalid index verification, which Doctor already handles.

### 16.3 Confidence

Advisor confidence should be explicit:

- `low`: plausible but weak evidence;
- `medium`: useful evidence but incomplete context;
- `high`: strong evidence and low ambiguity.

Confidence is not severity. A high-confidence `info` can be less urgent than a
low-confidence `warning`.

### 16.4 Evidence

Evidence should be machine-readable and bounded. It may include:

- trace event IDs;
- statement fingerprints;
- plan summaries;
- table/index names;
- row-count estimates;
- observed row counts;
- lock wait counts and durations;
- observation window;
- benchmark run identifiers;
- Doctor fact identifiers.

Evidence must not include raw parameter values by default.

### 16.5 Recommendation

Recommendations should be actionable and reviewable. They may include:

- command text for existing safe Doctor fixes;
- SQL text for a suggested migration;
- a natural-language summary;
- risk notes;
- expected benefit;
- required preconditions;
- rollback notes;
- a flag indicating whether automation is safe.

For schema changes, `safe_to_automate` should normally be `false`.

## 17. Advisor Families

### 17.1 Query-Plan Advisor

The query-plan advisor should inspect slow-query traces, plan summaries, and
available statistics.

Candidate findings:

- repeated slow full table scan on a large table;
- expensive sort that could use an index;
- plan changed after schema/statistics update and became slower;
- prepared statement frequently recompiles because schema generation changes;
- query spends most time waiting for locks rather than executing;
- query returns many rows and is slow by design, so no index recommendation is
  made.

The advisor should explicitly distinguish query execution cost from lock wait.
A query blocked behind a writer should not produce a missing-index suggestion
unless there is independent scan evidence.

### 17.2 Missing Index Advisor

The missing index advisor should propose candidate indexes, not create them.

Evidence requirements:

- one or more slow or high-cost statements;
- predicate/order/group columns extracted from the plan or parser;
- table cardinality above a threshold;
- no existing equivalent usable index;
- estimated benefit above write maintenance cost;
- observation window included in output.

Recommendation output may include SQL:

```sql
CREATE INDEX idx_example_col ON example(col);
```

Generated SQL must be clearly marked as a suggestion. It should preserve
quoting rules and avoid names that collide with existing objects. The advisor
should not emit SQL if it cannot name objects safely.

### 17.3 Unused Index Advisor

The unused index advisor should be conservative.

It may report that an index had no observed read usage during a trace window,
but it must consider:

- observation window length;
- whether the workload was representative;
- uniqueness or constraint role;
- write maintenance count;
- table write volume;
- last known use;
- index creation age;
- whether stats were reset recently;
- future index families the advisor does not understand.

Recommendation should usually be:

- review usage over a longer window;
- inspect dependent constraints;
- consider dropping in a migration if confirmed.

It should not mark dropping an index as safe to automate in Phase 1.

### 17.4 Redundant Index Advisor

The redundant index advisor can identify obvious prefix redundancy for indexes
with the same table, same collation, same expression semantics, same partial
predicate, and compatible uniqueness properties.

It must not treat indexes as redundant when:

- one is unique and the other is not;
- partial predicates differ;
- collations differ;
- sort directions matter for supported plans;
- expression definitions differ;
- one index supports a constraint;
- future index type semantics are unknown.

This advisor is likely a later phase because it requires careful catalog
semantics.

### 17.5 Schema Lint Advisor

Schema lint should catch maintainability and portability problems, especially as
DecentDB adds sync, branch, browser, and mobile features.

Candidate findings:

- table without primary key where future sync requires stable identity;
- very wide table likely to cause poor cache locality;
- excessive indexes on write-heavy tables;
- stale statistics;
- incompatible schema features for browser/mobile packaging;
- object names that are valid but likely to confuse bindings;
- migration patterns that require expensive rebuilds.

Schema lint must separate "currently invalid" from "future feature may not
support this." The latter should be `info` unless there is immediate impact.

### 17.6 Contention Advisor

The contention advisor should combine lock-wait traces, write queue metrics,
process coordination metrics, and session lifecycle state.

Candidate findings:

- long writer waits caused by long read transactions;
- frequent write queue buildup;
- checkpoint blocked by readers;
- writer held open by application transaction scope;
- internal index verification or rebuild causing user-visible waits;
- VFS/file-lock waits outside engine control.

Recommendations may include:

- shorten application transactions;
- close idle readers;
- adjust checkpoint cadence;
- run maintenance during a quieter window;
- inspect process coordination state;
- collect a support bundle when VFS/file-lock waits persist.

### 17.7 WAL And Storage Advisor

Doctor already reports large WAL files and storage facts. Runtime tracing can
improve recommendations by showing whether WAL growth correlates with:

- active readers;
- failed checkpoints;
- write bursts;
- long transactions;
- process coordination locks;
- storage I/O delays.

This advisor should not change checkpoint semantics. It should provide better
evidence for existing maintenance commands.

### 17.8 JSON Path Advisor

After JSONB and JSON path indexing exist, runtime tracing should support a JSON
path advisor.

Candidate findings:

- repeated JSON path extraction on large tables;
- repeated filter on the same JSON path;
- JSON path index candidate;
- JSON path expression not indexable;
- stale JSON path index statistics.

This is out of scope until JSONB/indexing semantics exist, but the trace event
model should leave room for expression/path evidence without raw value capture.

### 17.9 Sync, Branch, Browser, And Mobile Diagnostics

Future sync, branch, browser, and mobile features will need diagnostics that
combine runtime facts with static schema facts.

Potential findings:

- schema shape not compatible with sync conflict handling;
- branch metadata drift;
- browser storage quota risk;
- mobile transaction too large for expected device profile;
- unsupported extension in a target runtime;
- cache size or WAL settings unsuitable for mobile lifecycle.

These advisors should remain optional and feature-gated until those subsystems
are stable.

## 18. Doctor Fix Plans

### 18.1 Purpose

`doctor --fix-plan` should let users and automation inspect what Doctor would
recommend doing without applying changes. It is a bridge between passive
findings and explicit remediation.

### 18.2 Output Shape

The fix plan should be available in text and JSON. JSON should include:

```json
{
  "schema_version": 1,
  "database": {},
  "generated_at_unix_ms": 0,
  "actions": [
    {
      "action_id": "fix.checkpoint",
      "finding_id": "wal.large_file",
      "category": "wal",
      "safe_to_automate": true,
      "destructive": false,
      "requires_exclusive_access": false,
      "command": "decentdb doctor path --fix --checks wal",
      "sql": null,
      "preconditions": [],
      "risks": [],
      "rollback": null
    }
  ]
}
```

The final shape should align with the existing Doctor JSON report and avoid
duplicating fields unnecessarily.

### 18.3 Automation Boundary

Fix actions should be classified:

- safe automated maintenance;
- safe but requires exclusive access;
- migration suggestion, manual review required;
- destructive, never automatic;
- unsupported in current runtime.

Examples:

- checkpointing a large WAL can be safe to automate if existing Doctor rules say
  so;
- rebuilding a stale index can be safe when existing preconditions pass;
- creating a new index is a migration suggestion and should not be automatic in
  Phase 1;
- dropping an unused index should not be automatic in Phase 1;
- changing durability settings should not be automatic.

### 18.4 Relationship To Existing `--fix`

`doctor --fix` should continue to apply only narrow, documented safe fixes.
`doctor --fix-plan` should be broader and may include manual suggestions.

The presence of a fix-plan action must not imply that `--fix` will execute it.
JSON should make this explicit.

## 19. CLI And Tooling Surface

### 19.1 Doctor CLI

Candidate CLI additions:

```bash
decentdb doctor path/to.db --include-runtime
decentdb doctor path/to.db --trace-window 60s
decentdb doctor path/to.db --advisors query,index,contention
decentdb doctor path/to.db --fix-plan
decentdb doctor path/to.db --format json --include-runtime
```

The CLI must remain useful for offline database files. Runtime-only options
should report clearly when no live trace state is available.

### 19.2 Decent Bench

Decent Bench should be able to show:

- slow-query panel;
- lock-wait panel;
- index-usage panel;
- advisor findings;
- Doctor summary;
- plan-diff or regression report when benchmark runs are comparable.

Benchmark integration must distinguish benchmark overhead from engine overhead.
If Decent Bench enables tracing, benchmark reports should state that tracing was
enabled and include config values.

### 19.3 Support Bundles

Support bundles, if added later, can include redacted trace snapshots and Doctor
JSON. That is not required for this win, but the data model should make safe
export possible:

- no raw params by default;
- redacted paths;
- bounded SQL templates;
- explicit trace-window metadata;
- config included;
- engine version included;
- report schema version included.

## 20. Rust API Surface

The final API needs an ADR, but a coherent shape is:

```rust
impl Db {
    pub fn runtime_tracing_config(&self) -> RuntimeTracingConfigSnapshot;
    pub fn configure_runtime_tracing(&self, config: RuntimeTracingConfig) -> Result<()>;
    pub fn reset_runtime_traces(&self, scope: RuntimeTraceResetScope) -> Result<()>;
    pub fn runtime_trace_snapshot(&self, options: RuntimeTraceSnapshotOptions) -> Result<RuntimeTraceSnapshot>;
    pub fn runtime_advisor_report(&self, options: RuntimeAdvisorOptions) -> Result<RuntimeAdvisorReport>;
}
```

If runtime reconfiguration adds locking complexity or unsafe lifetime issues,
Phase 1 should support open-time configuration only and expose snapshots through
SQL views.

### 20.1 Snapshot Types

Snapshots should be owned data structures detached from live locks. A snapshot
must not borrow from mutable engine state after locks are released.

Snapshot APIs should expose:

- capture time;
- config;
- oldest retained event ID;
- newest retained event ID;
- eviction count;
- truncation count;
- rows/events.

### 20.2 Error Handling

Runtime tracing APIs must use typed errors and structured diagnostics. Examples:

- tracing disabled;
- unsupported target;
- buffer size exceeds configured memory budget;
- SQL text capture mode not allowed by policy;
- Doctor runtime analysis requested without trace state;
- advisor unavailable because required statistics are missing.

Avoid panics in library code.

## 21. C ABI And Binding Surface

### 21.1 First Principle

Prefer SQL views for read access in Phase 1. Every binding can already run SQL,
and SQL keeps the first ABI surface smaller.

Direct C ABI functions are still useful for configuration and JSON snapshots,
but each addition carries binding and stability cost.

### 21.2 Candidate C ABI Additions

If needed, an ADR can define:

```c
ddb_result ddb_runtime_tracing_set(ddb_database_t *db, const char *json_config);
ddb_result ddb_runtime_tracing_get(ddb_database_t *db, char **json_config_out);
ddb_result ddb_runtime_tracing_reset(ddb_database_t *db, const char *scope);
ddb_result ddb_runtime_trace_snapshot_json(ddb_database_t *db, const char *json_options, char **json_out);
ddb_result ddb_runtime_advisor_report_json(ddb_database_t *db, const char *json_options, char **json_out);
```

This JSON-shaped ABI is easy to version but less type-safe. It should be used
only if SQL views and open options are insufficient.

### 21.3 Binding Conventions

Bindings should expose:

- open-time tracing config where natural;
- SQL access to runtime views;
- a Doctor/advisor helper only if that binding already has a diagnostics helper;
- redaction defaults matching Rust/C ABI behavior;
- no binding-specific telemetry semantics.

Binding smoke tests should cover at least:

- tracing disabled by default;
- enabling slow-query tracing;
- reading `sys.slow_queries`;
- redaction of parameter values;
- JSON Doctor/advisor report shape if exposed.

## 22. Internal Architecture

### 22.1 Event Capture Points

Candidate capture points:

- connection/session open and close;
- statement prepare;
- statement execute start/end;
- planner selected plan;
- executor actual index access;
- write queue wait start/end;
- WAL append wait;
- checkpoint wait;
- process coordination lock wait;
- schema/index build start/end;
- index verification start/end;
- Doctor report generation.

Capture points should be added only where they can be measured and tested. Do
not scatter ad hoc tracing calls without a shared event API.

### 22.2 Trace Sink

Use a small internal trace sink abstraction:

```rust
trait RuntimeTraceSink {
    fn enabled(&self, family: RuntimeTraceFamily) -> bool;
    fn record_statement(&self, event: StatementTraceEvent);
    fn record_lock_wait(&self, event: LockWaitTraceEvent);
    fn record_index_usage(&self, event: IndexUsageEvent);
    fn record_session(&self, event: SessionTraceEvent);
}
```

The disabled sink should be trivial. The enabled sink owns bounded buffers and
aggregation state.

Avoid dynamic dispatch in hot paths if it benchmarks poorly. A cheap enum or
inline branch may be better.

### 22.3 Ring Buffers

Ring buffers should:

- use preallocated capacity where practical;
- overwrite oldest events when full;
- increment eviction counters;
- store owned, bounded payloads;
- avoid holding engine locks while formatting rows;
- support snapshot copying under short locks;
- expose oldest/newest event IDs;
- support reset by family.

The implementation should consider `VecDeque`, custom ring buffers, or a small
local helper. Adding an external dependency for this requires ADR approval under
repo rules.

### 22.4 Aggregates Versus Events

Some data is better as events. Some is better as aggregates.

Events:

- slow queries;
- lock waits;
- session lifecycle changes;
- Doctor report generation.

Aggregates:

- index usage counts;
- per-statement fingerprint summary;
- per-lock-source summary;
- per-advisor rule counts.

`sys.index_usage` should likely be aggregate-first because per-index events can
be too noisy. `sys.slow_queries` and `sys.lock_waits` should be event-first.

### 22.5 Snapshot Isolation

SQL views should materialize a snapshot of trace data before returning rows.
They should not iterate live buffers while the executor is also writing new
events. Snapshotting must have bounded memory and bounded lock time.

If a trace view is queried while events are being overwritten, the view should
return a consistent snapshot and include truncation/eviction metadata.

## 23. Memory And Performance Budget

### 23.1 Disabled Overhead Target

ADR 0186 sets the initial target, and the implementation should treat it as
strict:

- no measurable throughput regression on core read benchmarks beyond noise;
- no measurable write throughput regression beyond noise;
- no allocations on disabled statement execution paths;
- no SQL normalization solely for disabled tracing;
- no extra lock acquisitions solely for disabled tracing.

As a starting point, disabled overhead should be below 0.5 percent on p50/p95
for standard benchmarks, with benchmark noise documented. If this cannot be met,
the implementation should be redesigned before shipping.

### 23.2 Enabled Overhead Target

Enabled tracing is allowed to cost more, but it must be predictable:

- slow query event recording should be below 2 percent overhead for statements
  that cross the threshold in a diagnostic workload;
- lock-wait tracing should be dominated by waits already happening;
- index usage aggregate increments should be cheap enough for benchmark mode;
- SQL text template capture should be separately benchmarked;
- all overhead should be visible in Decent Bench reports.

These numbers are starting targets, not a substitute for ADR-approved budgets.

### 23.3 Memory Budget

Trace memory must count against a configured tracing budget, not the page cache
budget. However, docs should explain total process memory impact.

If DecentDB has an overall memory profile in the future, tracing should be part
of that profile and should degrade predictably when memory is constrained.

## 24. Security, Privacy, And Redaction

### 24.1 Default Redaction Rules

Default runtime tracing must not capture:

- parameter values;
- raw SQL literals;
- row values;
- encryption keys;
- credentials;
- connection strings;
- unredacted database paths;
- application-provided secret metadata.

Default runtime tracing may capture:

- statement kind;
- redacted or template SQL if enabled;
- SQL fingerprint;
- placeholder count;
- parameter type classes if already known and not sensitive;
- object names;
- row counts;
- durations;
- structured error codes;
- redacted path/database identity hash.

### 24.2 Path Redaction

Doctor already supports path modes. Runtime tracing should align with that
policy. Candidate path modes:

- `none` or `redacted`;
- `basename`;
- `absolute`, explicit debug only.

Do not introduce a separate path redaction vocabulary if the existing Doctor
model can be reused.

### 24.3 Object Names

Table and index names can still be sensitive in some applications. The ADR
should decide whether object names are always shown, hashable under a privacy
mode, or controlled by a trace redaction setting.

The pragmatic default is to show object names because SQL diagnostics are much
less useful without them, while allowing a stricter support-bundle mode later.

### 24.4 SQL Text Modes

Suggested modes:

- `none`: no SQL text or template, only fingerprint;
- `template`: literals replaced, comments removed or redacted;
- `redacted`: original SQL shape with literal redaction and truncation;
- `full`: raw SQL, explicit debug-only mode.

`full` should require explicit configuration and should be documented as unsafe
for sensitive workloads.

## 25. Compatibility And Stability

### 25.1 File Format

This win should not require a file format change.

If an implementation later wants persistent index usage statistics or durable
trace history, that is a separate ADR and may trigger file format/migration
requirements.

### 25.2 WAL Format

This win should not require a WAL format change.

Trace events must not become WAL records in this phase.

### 25.3 SQL Surface Stability

Follow ADR 0163:

- documented `SELECT * FROM sys.name` forms are stable;
- column additions require doc updates and compatibility review;
- unsupported writes fail;
- predicates/projections may work but are not the main stability promise unless
  explicitly documented.

### 25.4 JSON Stability

Doctor/advisor JSON should be versioned. Additive fields are acceptable within a
schema version only if existing consumers tolerate them. Breaking changes should
increment schema version.

### 25.5 Binding Stability

Bindings should not invent different field names or severity/confidence values.
They should either return rows from SQL views or pass through versioned JSON.

## 26. Testing Strategy

### 26.1 Unit Tests

Add focused tests for:

- disabled trace sink does not allocate event payloads;
- ring buffer eviction;
- reset semantics;
- redaction of SQL literals and parameters;
- SQL fingerprint stability for chosen mode;
- lock-wait classification helpers;
- index usage aggregate increments;
- advisor confidence/severity classification;
- Doctor finding projection rows;
- JSON schema versioning.

### 26.2 Integration Tests

Add integration tests for:

- enable tracing, run slow query, inspect `sys.slow_queries`;
- run lock contention scenario, inspect `sys.lock_waits`;
- create and use index, inspect `sys.index_usage`;
- open/close sessions, inspect `sys.sessions`;
- run Doctor/advisors, inspect `sys.doctor_findings`;
- reset buffers and verify rows disappear or counters reset;
- query `sys.*` views while workload continues;
- internal Doctor/sys queries do not pollute advisor output;
- tracing disabled by default.

### 26.3 Concurrency Tests

Add tests for:

- one writer, many readers;
- long reader blocking checkpoint;
- writer queue buildup;
- simultaneous trace snapshot and event writes;
- closing a connection while snapshotting sessions;
- FFI/binding lifecycle where finalizers close handles;
- no deadlock when Doctor reads runtime snapshots.

### 26.4 Redaction Tests

Redaction tests must include:

- string literals;
- numeric literals;
- blob literals;
- comments;
- parameterized SQL;
- paths;
- object names if privacy mode supports hashing;
- error payloads linked from trace events;
- Doctor JSON and SQL row projections.

Tests should assert that sensitive example values do not appear in rows or JSON.

### 26.5 Benchmark Tests

Benchmark both disabled and enabled modes:

- point lookup;
- range scan;
- insert;
- update;
- transaction batch;
- prepared statement loop;
- write contention scenario;
- checkpoint-heavy workload;
- `sys.*` snapshot reads;
- Decent Bench advisor run.

Benchmark output should include:

- tracing mode;
- buffer sizes;
- SQL text mode;
- slow query threshold;
- event counts;
- evictions;
- memory use estimate.

## 27. Documentation Requirements

Update docs in the same PR or implementation series:

- `docs/api/sql-functions.md` with new `sys.*` views;
- `docs/user-guide/doctor.md` with runtime and fix-plan behavior;
- binding docs for enabling tracing if exposed;
- benchmark docs for advisor panels and overhead notes;
- troubleshooting docs for common findings;
- C header comments if ABI changes;
- release notes in `docs/about/changelog.md`, not root `CHANGELOG.md`.

Documentation must include:

- tracing disabled by default;
- privacy/redaction defaults;
- how to enable tracing;
- how to reset buffers;
- memory overhead;
- view stability contract;
- examples for slow queries, lock waits, index usage, sessions, and Doctor
  findings;
- warning that advisors are recommendations, not proof.

## 28. Implementation Phases

### Phase 0: Measurement Plan And Contract Review

Deliverables:

- review ADR 0186-0189 against the implementation slice;
- benchmark plan;
- redaction policy;
- SQL surface draft;
- Doctor schema change plan;
- binding impact assessment.

No engine code should land before this phase's measurement plan and scoped
contract review are complete.

### Phase 1: Trace Infrastructure And Sessions

Deliverables:

- internal trace sink abstraction;
- disabled sink;
- bounded ring buffer helper;
- configuration structure;
- session IDs and session lifecycle tracking;
- `sys.sessions`;
- reset behavior;
- disabled-overhead benchmarks.

This phase proves the infrastructure without deep planner/executor coupling.

### Phase 2: Slow Query Tracing

Deliverables:

- statement event capture;
- slow query threshold;
- SQL fingerprint or redacted template mode;
- `sys.slow_queries`;
- timing fields available from current execution pipeline;
- redaction tests;
- prepared statement metadata where cheap.

This phase should avoid broad query-plan advisor logic. It captures facts first.

### Phase 3: Lock-Wait Tracing

Deliverables:

- wait source classification;
- lock-wait event capture at approved points;
- `sys.lock_waits`;
- contention test scenarios;
- no-lock-held formatting guarantee;
- integration with process coordination and write queue metrics.

This phase enables the contention advisor later.

### Phase 4: Index Usage Aggregates

Deliverables:

- planner/executor attribution hooks;
- index usage aggregate store;
- write maintenance counts;
- `sys.index_usage`;
- reset behavior;
- tests for constraint indexes and normal read indexes;
- benchmarks for aggregate update overhead.

This phase should be conservative and avoid unused-index recommendations until
the observation semantics are trustworthy.

### Phase 5: Advisor Engine

Deliverables:

- shared advisor finding model;
- query-plan advisor;
- missing-index candidate advisor;
- conservative unused-index advisor;
- contention advisor;
- WAL/storage advisor improvements;
- severity/confidence/evidence tests;
- no destructive auto-fix behavior.

Advisors should consume trace snapshots and existing `sys.*` facts through Rust
APIs, not by recursively executing arbitrary SQL under internal locks.

### Phase 6: Doctor Integration

Deliverables:

- Doctor runtime options;
- optional Doctor category additions;
- Doctor report schema version update if needed;
- `sys.doctor_findings`;
- `PRAGMA doctor` if accepted by ADR;
- `doctor --fix-plan`;
- JSON snapshot tests;
- CLI docs.

Doctor must remain useful without runtime traces.

### Phase 7: Bindings, Decent Bench, And Docs

Deliverables:

- binding open options where appropriate;
- binding smoke tests;
- Decent Bench panels;
- docs updates;
- changelog entry in `docs/about/changelog.md`;
- final overhead report.

## 29. Definition Of Done

This win is complete when:

- an ADR is accepted;
- tracing is disabled by default;
- disabled overhead meets the ADR-approved budget;
- enabled overhead is benchmarked and documented;
- trace memory is bounded and resettable;
- SQL text and parameters are redacted by default;
- `sys.slow_queries` works for representative slow statements;
- `sys.lock_waits` works for representative contention;
- `sys.index_usage` reports observed index usage and maintenance;
- `sys.sessions` reports active/recent sessions without server-pool semantics;
- `sys.doctor_findings` exposes Doctor/advisor findings as rows;
- Doctor can include runtime findings when traces exist;
- `doctor --fix-plan` reports safe and manual actions without applying them;
- advisors include severity, confidence, evidence, and recommendation;
- no advisor performs destructive automatic schema changes;
- tests cover redaction, reset, eviction, concurrency, and Doctor projection;
- docs cover usage, privacy, overhead, and stability;
- binding impacts are handled or explicitly deferred.

## 30. Risks And Mitigations

### 30.1 Hot-Path Regression

Risk: tracing branches, atomics, or allocations slow down normal execution.

Mitigation:

- disabled sink;
- strict benchmarks;
- no payload construction unless enabled;
- narrow capture points;
- feature flags only if needed;
- fail the win if overhead budget is not met.

### 30.2 Sensitive Data Leakage

Risk: SQL text, parameters, paths, or object names leak through trace views or
Doctor JSON.

Mitigation:

- no parameter values by default;
- SQL template/fingerprint defaults;
- path redaction reuse;
- redaction tests;
- explicit unsafe full-SQL mode;
- support-bundle privacy review before export features.

### 30.3 Advisor Noise

Risk: advisors emit low-quality recommendations and train users to ignore them.

Mitigation:

- evidence requirements;
- confidence values;
- conservative thresholds;
- no missing-index recommendation from lock-wait evidence alone;
- no unused-index drop recommendation from a short window;
- snapshot tests for known workloads.

### 30.4 Deadlocks Or Recursive Introspection

Risk: trace views or Doctor collect facts while holding locks needed by the
workload.

Mitigation:

- snapshot owned data under short locks;
- do not run SQL recursively while holding internal locks;
- exclude internal introspection from normal trace capture;
- concurrency tests with Doctor/sys views during writes.

### 30.5 Stability Burden

Risk: too many columns or ABI functions become stable too early.

Mitigation:

- SQL view stability follows ADR 0163;
- prefer nullable fields over premature guarantees;
- use JSON schema versions for Doctor/advisors;
- keep direct C ABI small in Phase 1.

## 31. Open Questions

1. Should slow-query tracing be enabled by `runtime_tracing = ON`, or should it
   require an explicit threshold?
2. Should `sys.slow_queries` include failed statements that crossed the
   threshold?
3. Should `sys.lock_waits` include waits below threshold when the statement later
   fails with timeout?
4. Should SQL template capture use parser output, or should Phase 1 use
   fingerprint-only mode?
5. Should object names be hashable under a strict privacy mode?
6. Should `sys.doctor_findings` run Doctor on demand or project the latest
   explicit report?
7. Should Doctor runtime analysis be available for a live in-process database
   only, or can CLI attach to another process's trace state later?
8. Should `PRAGMA doctor` be a stable user surface or a convenience alias around
   SQL views/CLI behavior?
9. Should trace buffers be per connection, per database handle, or per shared
   database coordinator?
10. Should Decent Bench enable tracing by default for benchmark advisor panels,
    or require a diagnostic profile?
11. What minimum observation window is required before unused-index advisors can
    emit anything above `info`?
12. How should future browser/mobile builds expose trace snapshots without
    increasing bundle size too much?

## 32. Minimal Acceptable First Slice

If implementation scope needs to be reduced, the smallest useful slice is:

1. ADR and overhead budget.
2. Disabled trace sink.
3. Bounded slow-query ring buffer.
4. `sys.slow_queries` with fingerprint-only or redacted-template SQL.
5. Redaction tests proving parameter values do not appear.
6. Reset behavior.
7. Benchmarks proving disabled overhead is within budget.
8. Documentation explaining that lock waits, index usage, advisors, and Doctor
   projection remain future phases.

This first slice would still be valuable because it establishes the runtime
tracing contract without taking on planner advisor complexity.

## 33. Preferred Full Scope

The preferred full scope for this win is:

- trace infrastructure;
- sessions;
- slow queries;
- lock waits;
- index usage;
- advisor model;
- query/index/contention advisors;
- Doctor runtime integration;
- `sys.doctor_findings`;
- `doctor --fix-plan`;
- Decent Bench panels;
- binding smoke tests;
- docs.

This is a large feature and should be delivered incrementally, but the
architecture should be designed once so each phase adds evidence without
rewriting the previous phase.
