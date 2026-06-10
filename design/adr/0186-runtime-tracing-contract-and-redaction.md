# Runtime Tracing Contract And Redaction
**Date:** 2026-06-09
**Status:** Accepted

### Decision

DecentDB will add runtime tracing as an explicit, bounded, in-memory diagnostic
facility layered on top of the shipped operational `sys.*` metrics contract.

Runtime tracing is disabled by default. When disabled, statement execution,
lock acquisition, index access, and session lifecycle paths must not allocate
trace payloads, normalize SQL solely for tracing, acquire extra telemetry locks,
or write trace data. Disabled tracing overhead must be measured and must stay
below the approved default-fast budget for read-heavy, write-heavy, prepared
statement, and mixed workloads.

The initial disabled overhead target is:

- no allocations solely for runtime tracing;
- no trace ring-buffer initialization on ordinary statement execution;
- no SQL text redaction or normalization solely for tracing;
- no additional hot lock acquisition solely for tracing;
- less than 0.5 percent throughput or p95 latency regression on standard
  benchmarks, with benchmark noise documented.

Enabled tracing is allowed to cost more, but it must be bounded and visible.
The initial enabled overhead target is:

- slow-query event recording below 2 percent overhead for statements that cross
  the configured threshold in diagnostic workloads;
- lock-wait event cost dominated by waits that already occurred;
- index-usage aggregate updates benchmarked separately before advisor use;
- SQL text capture benchmarked separately from fingerprint-only capture;
- tracing configuration included in benchmark reports when tracing is enabled.

Runtime tracing has three tiers:

1. Existing cheap snapshots and counters, such as `sys.wal_metrics`,
   `sys.write_queue_metrics`, and process coordination metrics.
2. Opt-in in-memory trace history for recent slow queries, lock waits, sessions,
   and index usage.
3. Advisor and Doctor analysis that consumes snapshots, trace history, catalog
   facts, and statistics.

Trace history is process-local in v1. It is not persisted in the database file,
WAL, sidecar, or a telemetry log. It is not shared across independent OS
processes. Cross-process lock and WAL metrics may contribute facts, but the
trace event buffers themselves are local to the process that captured them.

Trace state is scoped as follows:

- session lifecycle state belongs to the connection/session that owns it;
- statement events belong to the executing `Db` handle or logical connection;
- lock-wait events may be stored in a shared process-local database coordinator
  when multiple handles share the same opened database;
- index usage aggregates may be shared per process-local database coordinator;
- temporary schema and connection-local state must not leak into unrelated
  connections;
- all path-like identities use the same redaction policy family as Doctor.

The initial event families are:

- `statement`
- `lock_wait`
- `index_usage`
- `session`
- `doctor`
- `advisor`

Each event family must have explicit enablement and capacity controls. Enabling
one event family must not implicitly enable all others unless the caller selects
a named diagnostic profile.

Trace buffers are fixed-size and in memory. They overwrite oldest entries when
full, increment eviction counters, and expose snapshot metadata that identifies
oldest retained event ID, newest retained event ID, eviction count, truncation
count, capture time, and active configuration. Reset must be explicit and
family-scoped where practical.

Runtime tracing must use monotonic clocks for durations. Wall-clock timestamps
may be included for human reports, but duration calculations must not depend on
wall-clock time.

Default runtime tracing must not capture:

- SQL parameter values;
- raw row values;
- encryption keys;
- credentials;
- connection strings;
- unredacted database paths;
- raw audit context values;
- full SQL text containing literal values.

The stable SQL text capture modes are:

- `none`: no SQL text or template, only fingerprint and statement metadata;
- `template`: parser-derived template with literals removed or replaced;
- `redacted`: truncated SQL shape with literals redacted;
- `full`: raw SQL text, explicit debug-only opt-in.

The default mode when tracing is enabled is `none` until a parser-backed
template redactor is implemented and benchmarked. After that implementation is
available, a future implementation PR may make `template` the default for
explicitly enabled diagnostic profiles only if redaction tests and overhead
benchmarks pass. `full` must never be the default.

Parameter values must not be captured in any default profile. A future explicit
debug-only parameter capture mode would require a follow-up ADR because it
changes the privacy boundary.

Statement fingerprints must not include parameter values. They may use
parser-stable normalized SQL, statement kind, placeholder count, parameter type
classes, schema generation, temp schema generation, relevant security/policy
generation, and plan invalidation generation. If parser-stable normalization is
not available, the implementation may use a hash of redacted or exact SQL text
according to the configured SQL text policy.

Internal inspection work must be labeled. Queries or operations used to
materialize `sys.*` views, run Doctor, inspect schema, verify indexes, or
produce advisor reports must not recursively generate ordinary user-facing
slow-query/advisor evidence by default.

Runtime tracing must not write trace events recursively to user tables,
system tables, WAL, coordination sidecars, or telemetry files in this phase.
Persistent trace history, external telemetry export, support-bundle export, or
OpenTelemetry-style integration requires a follow-up ADR.

### Rationale

ADR 0163 deliberately shipped cheap operational snapshots without runtime
history. The next useful step is bounded runtime evidence: what statement was
slow, what lock source caused a wait, which index was used, and which session
was active. That evidence is valuable only if it does not weaken DecentDB's
embedded and durable-default posture.

Disabled-by-default tracing protects normal embedded workloads. Many DecentDB
deployments will run in application processes where database overhead directly
affects user-facing latency. Observability must be something users choose for a
diagnostic window, benchmark run, support session, or development profile.

In-memory bounded buffers match the embedded model. They avoid file-format
changes, WAL changes, migration obligations, recovery complexity, and privacy
risks from durable telemetry artifacts.

Redaction must be a core contract rather than a later patch. SQL literals,
parameters, file paths, and audit context can contain secrets or regulated data.
Runtime tracing will be used during production support, so the default must be
safe enough to inspect without exposing application values.

### Alternatives Considered

1. **Always-on slow-query history.** Rejected. It adds hot-path cost and memory
   use to every application, even when diagnostics are not needed.
2. **Persist trace events in hidden system tables.** Rejected. This would create
   recursive write paths, WAL traffic, file-format or catalog questions, and
   privacy risk. Durable telemetry can be reconsidered with a separate ADR.
3. **Write trace events to a sidecar log.** Rejected for v1. It still creates a
   durable telemetry artifact and file lifecycle obligations.
4. **Capture full SQL and parameters by default.** Rejected. This is convenient
   for debugging but unsafe for routine support and inconsistent with structured
   diagnostic redaction rules.
5. **Expose only existing counters and no history.** Rejected. Counters cannot
   answer which statement or wait caused an observed problem.
6. **Share trace buffers across OS processes through the coordination sidecar.**
   Rejected for v1. Cross-process trace sharing is a separate transport and
   privacy problem. Existing cross-process metrics remain available as facts.

### Trade-offs

- Fingerprint-only default tracing is less friendly than showing SQL templates,
  but it is safe before a parser-backed redactor is implemented and measured.
- In-memory buffers lose history on process exit. That is acceptable for v1 and
  avoids durable telemetry complexity.
- Bounded buffers can evict important events under high load. Eviction counters
  and buffer sizing make that visible.
- Per-process trace state means CLI Doctor cannot automatically inspect another
  process's live trace buffer unless a later shared transport is added.
- Strict disabled-overhead requirements may force simpler capture points and
  defer some detailed timing breakdowns.

### Consequences

- The runtime tracing implementation must begin with a disabled sink and
  benchmarks before adding advisor logic.
- Runtime trace memory must be separately budgeted and must not silently consume
  page-cache memory.
- Redaction tests must assert that parameters, literals, paths, and sensitive
  example values do not appear in trace rows or JSON.
- Querying runtime trace views must snapshot owned data under short locks and
  must not iterate mutable buffers while events are being written.
- Any persistent telemetry, support-bundle export, external export bridge, raw
  parameter capture, or cross-process trace sharing needs a follow-up ADR.

### References

- `design/WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md`
- `design/FUTURE_WINS.md`
- `design/adr/0163-operational-sys-metrics.md`
- `design/adr/0174-local-data-security-tde-policies-masking-audit-context.md`
- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
- `design/adr/0185-rich-structured-error-diagnostics-contract.md`
- `docs/user-guide/doctor.md`

