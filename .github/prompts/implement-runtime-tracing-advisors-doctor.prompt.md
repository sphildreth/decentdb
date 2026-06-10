---
agent: agent
name: Implement Runtime Tracing Advisors And Doctor Integration
description: "Use when fully implementing design/WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md, including runtime tracing, sys.* views, advisors, Doctor integration, fix plans, tests, docs, and binding-facing surfaces."
argument-hint: "Optionally name the phase, subsystem, or validation scope to start with"
---

# Implement Runtime Tracing, Advisors, And Doctor Integration

Fully implement
[design/WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md](../../design/WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md)
in DecentDB.

This is architecture-sensitive engine work. Before editing, read the repository
instructions and the accepted ADRs that govern this feature:

- [Runtime tracing contract and redaction](../../design/adr/0186-runtime-tracing-contract-and-redaction.md)
- [Runtime sys views and Doctor projection](../../design/adr/0187-runtime-sys-views-and-doctor-projection.md)
- [Runtime advisors and fix-plan policy](../../design/adr/0188-runtime-advisors-and-fix-plan-policy.md)
- [Runtime tracing API and binding contract](../../design/adr/0189-runtime-tracing-api-and-binding-contract.md)
- [Operational sys metrics](../../design/adr/0163-operational-sys-metrics.md)
- [Default-fast planner and runtime contract](../../design/adr/0184-default-fast-planner-and-runtime-contract.md)
- [Rich structured diagnostics](../../design/adr/0185-rich-structured-error-diagnostics-contract.md)
- [Doctor user guide](../../docs/user-guide/doctor.md)
- [SQL functions and sys views](../../docs/api/sql-functions.md)

Use the repository's Rust workflow and constraints from
[the Rust code generation skill](../skills/rust-code-generation/SKILL.md).

Load deeper references as needed:

- [Error handling patterns](../skills/rust-code-generation/references/errors.md)
- [Async and concurrency patterns](../skills/rust-code-generation/references/async.md)
- [FFI and layout safety patterns](../skills/rust-code-generation/references/ffi.md)
- [Performance and allocation patterns](../skills/rust-code-generation/references/performance.md)
- [Testing and validation patterns](../skills/rust-code-generation/references/testing.md)

## Primary Goal

Implement the full runtime diagnostics feature set:

- opt-in bounded runtime tracing;
- disabled-by-default low-overhead trace sink;
- redacted statement identity and slow-query tracing;
- lock-wait tracing with source classification;
- session lifecycle visibility;
- index-usage aggregates;
- read-only runtime `sys.*` views;
- Doctor findings projection through `sys.doctor_findings`;
- explicit Doctor refresh behavior;
- runtime advisors with severity, confidence, evidence, and recommendations;
- `doctor --fix-plan`;
- Rust API, CLI, C ABI/open-options, and binding-facing behavior where needed;
- tests, benchmarks, docs, and changelog entry in `docs/about/changelog.md`.

Do not weaken durability, WAL semantics, file-format compatibility, or the
one-writer/many-readers model. Do not add persistent telemetry tables, WAL trace
records, durable trace sidecars, external telemetry export, raw parameter
capture, cross-process trace sharing, or automatic schema mutation unless a new
ADR explicitly authorizes that scope.

## Required Workflow

1. Read the win spec and ADR 0186-0189 completely.
2. Inspect the current implementations of:
   - `sys.*` view dispatch and row materialization;
   - `doctor.rs` and CLI Doctor command handling;
   - statement prepare/execute paths;
   - write queue and process coordination wait paths;
   - planner/executor index access paths;
   - C ABI open options and diagnostics;
   - binding smoke tests.
3. Write a short implementation plan with phases and files likely to change.
4. Confirm whether the requested slice is the full feature or a smaller phase.
   If no slice is specified, implement in the phases below.
5. Implement from infrastructure upward.
6. Keep each change coherent and tested before moving to the next phase.
7. Prefer SQL views as the cross-language read surface.
8. Add public Rust/C ABI or binding APIs only when the SQL/open-options path is
   insufficient.
9. Update docs and tests in the same implementation series.
10. Run the smallest relevant validation while iterating, then broader
    validation before declaring completion.

## Hard Constraints

- Runtime tracing is disabled by default.
- Disabled tracing must not allocate trace payloads on ordinary hot paths.
- Disabled tracing must not normalize or redact SQL solely for telemetry.
- Disabled tracing must not acquire extra hot locks solely for telemetry.
- Trace buffers are in-memory, bounded, resettable, and process-local.
- Trace events are not written to the database file, WAL, catalog tables, or
  sidecar files.
- Default tracing does not store parameter values, row values, raw literals,
  credentials, encryption keys, unredacted paths, or raw audit context.
- SQL text mode `full` is explicit debug-only and must never be the default.
- Internal `sys.*`, Doctor, advisor, and schema-inspection work is labeled or
  excluded so it does not recursively create normal user-facing evidence.
- `sys.*` runtime views are read-only virtual inspection surfaces.
- `sys.doctor_findings` projects the latest explicit Doctor report; plain
  `SELECT * FROM sys.doctor_findings` must not run expensive Doctor checks.
- Advisors produce evidence-based findings and recommendations; they do not
  mutate schema.
- `doctor --fix-plan` plans; it does not apply changes.
- `doctor --fix` remains limited to documented safe fixes.
- Generated migration SQL from advisors is review-only and normally has
  `safe_to_automate = false`.
- No new major dependency without explicit approval and an ADR if required.
- No file-format, WAL-format, broad C ABI, or concurrency-semantic change
  without checking ADR obligations first.
- Avoid `unwrap()` and `expect()` in library code unless there is a narrow,
  documented invariant.

## Implementation Phases

### Phase 0: Measurement Plan And Contract Review

Deliver:

- a short implementation map against ADR 0186-0189;
- benchmark plan for disabled and enabled tracing;
- redaction test plan;
- SQL view column plan;
- Doctor report schema impact plan;
- C ABI/binding impact assessment.

Do not begin broad engine edits until the scoped plan is coherent.

### Phase 1: Trace Infrastructure And Sessions

Implement:

- typed runtime tracing config;
- disabled sink;
- enabled sink abstraction;
- bounded ring buffer or aggregate helper;
- event IDs and snapshot metadata;
- explicit reset by family;
- session/connection IDs;
- session lifecycle tracking;
- `sys.sessions`;
- tests for disabled default, session rows, reset, eviction, and snapshots.

Hot-path checks:

- disabled sink does not allocate event payloads;
- no SQL formatting or normalization when tracing is disabled;
- snapshotting does not hold long-lived engine locks.

### Phase 2: Slow Query Tracing

Implement:

- statement execution start/end capture;
- slow-query threshold config;
- status/error-code capture;
- monotonic duration capture;
- statement kind and read-only metadata;
- SQL fingerprint with no parameter values;
- SQL text modes with safe default;
- `sys.slow_queries`;
- redaction tests for literals, parameters, comments, paths, and errors;
- tests for successful and failed slow statements;
- prepared-statement metadata where cheap.

Do not make parser-backed SQL template capture the default unless the redactor
and overhead are tested.

### Phase 3: Lock-Wait Tracing

Implement:

- wait source enum and source classification;
- thresholded wait capture;
- timeout/cancellation capture;
- statement-event linkage where available;
- `sys.lock_waits`;
- tests for write queue waits, reader/checkpoint blockers, timeouts, unknown
  classification, and concurrent snapshotting.

Capture primitive facts while in lock-sensitive code and finish formatting
after releasing hot locks.

### Phase 4: Index Usage Aggregates

Implement:

- planner/executor attribution hooks for actual index use;
- aggregate index usage store;
- read-use counts;
- write-maintenance counts;
- constraint/ordering/covering categories where known;
- observation-window metadata;
- reset and eviction/truncation accounting;
- `sys.index_usage`;
- tests for ordinary read indexes, primary/unique/constraint indexes, write
  maintenance, reset, and unsupported/unknown use categories.

Do not treat observation-window non-use as proof that an index is globally
unused.

### Phase 5: Advisor Engine

Implement:

- shared advisor finding model;
- severity and confidence values;
- bounded redacted evidence JSON;
- query-plan advisor;
- missing-index candidate advisor;
- conservative unused-index advisor;
- contention advisor;
- WAL/storage advisor improvements;
- tests for true positives and false positives.

Required false-positive tests:

- lock-wait slow query does not become a missing-index recommendation by
  itself;
- short observation window does not produce an overconfident unused-index
  finding;
- unique/constraint index is not suggested as safe to drop;
- generated SQL is omitted when names cannot be safely quoted.

### Phase 6: Doctor Integration

Implement:

- runtime-aware Doctor options;
- explicit Doctor refresh path from SQL/PRAGMA or equivalent accepted syntax;
- latest report cache for `sys.doctor_findings`;
- `sys.doctor_findings` projection;
- report schema updates if needed;
- advisor findings in Doctor JSON;
- `doctor --include-runtime` or equivalent CLI option;
- `doctor --advisors` selection if included in the final CLI design;
- `doctor --fix-plan` text and JSON output;
- tests proving `sys.doctor_findings` projection does not implicitly run deep
  checks;
- tests proving `doctor --fix` automatic action set remains narrow.

### Phase 7: Rust, C ABI, Bindings, CLI, Docs, And Bench

Implement or update:

- Rust public APIs only where needed;
- C ABI open-options/configuration only where SQL/open options are insufficient;
- C ABI version and binding ABI expectations if direct ABI functions are added;
- binding smoke tests for enabled slow-query tracing and redaction;
- CLI docs and command help;
- Decent Bench panels or runtime diagnostics output where the benchmark tooling
  exists;
- `docs/api/sql-functions.md`;
- `docs/user-guide/doctor.md`;
- relevant binding docs;
- `docs/about/changelog.md`.

Do not update root `CHANGELOG.md`.

## Required SQL Surfaces

Implement these read-only virtual views unless the implementation plan records a
specific reason to phase one out:

```sql
SELECT * FROM sys.sessions;
SELECT * FROM sys.slow_queries;
SELECT * FROM sys.lock_waits;
SELECT * FROM sys.index_usage;
SELECT * FROM sys.doctor_findings;
```

Follow ADR 0163:

- documented `SELECT *` forms are stable;
- unsupported writes fail clearly;
- no durable `sys` schema is created;
- snapshot rows are owned and detached from live locks;
- columns and nullability are documented.

## Required Validation

While iterating, use the smallest relevant subset:

- `cargo fmt --check`
- `cargo check -p decentdb`
- `cargo test -p decentdb -- <filter>`
- `cargo clippy -p decentdb --all-targets --all-features -- -D warnings`

Before completion, run the broader relevant set:

- `cargo fmt --check`
- `cargo check -p decentdb`
- `cargo lint`
- targeted tests for runtime tracing, `sys.*` views, Doctor, advisors, and C ABI
  surfaces touched;
- binding smoke tests for impacted bindings;
- benchmark checks for disabled tracing overhead and enabled tracing overhead.

If the full suite is too expensive in the current environment, run the widest
practical subset and report exactly what was skipped and why.

## Required Tests

Add tests for:

- tracing disabled by default;
- enabling each event family;
- bounded buffers and eviction counters;
- reset by event family;
- session lifecycle rows;
- slow query capture;
- failed slow statement capture;
- SQL fingerprint excludes parameter values;
- redaction of literals, parameters, comments, paths, and sensitive examples;
- lock wait source classification;
- timeout/cancellation lock waits;
- index read usage;
- index write maintenance;
- constraint/unique index safety in advisors;
- `sys.*` views are read-only;
- `sys.*` snapshotting during concurrent workload;
- internal `sys.*`/Doctor/advisor work does not pollute normal traces;
- Doctor latest-report projection;
- explicit Doctor refresh;
- `doctor --fix-plan` JSON;
- `doctor --fix` does not apply advisor schema changes;
- binding-facing tracing behavior if exposed.

## Performance Requirements

Benchmark disabled tracing before and after core capture points are added.

At minimum measure:

- point lookup;
- range scan;
- insert;
- update;
- transaction batch;
- prepared statement loop;
- write contention scenario;
- checkpoint-heavy workload where practical;
- `sys.*` snapshot reads.

Benchmark reports must include:

- tracing enabled/disabled;
- enabled event families;
- buffer sizes;
- SQL text mode;
- slow-query threshold;
- event counts;
- evictions;
- estimated trace memory.

If disabled overhead exceeds the ADR 0186 target, stop and redesign the capture
path before expanding feature scope.

## Documentation Requirements

Update documentation with:

- tracing disabled-by-default behavior;
- how to enable tracing;
- how to reset trace buffers;
- memory and performance costs;
- redaction defaults and unsafe debug modes;
- every new `sys.*` view and column;
- Doctor runtime options;
- `doctor --fix-plan`;
- advisor severity/confidence/evidence semantics;
- examples for slow queries, lock waits, index usage, sessions, and Doctor
  findings;
- binding-specific configuration where exposed.

## Output Expectations

When complete, report:

1. Implementation phases completed
2. Files changed and why
3. Public surfaces added or changed
4. Tests added
5. Benchmarks and validation actually run
6. Any skipped checks and why
7. Residual risks or follow-up ADRs needed

If implementation cannot be completed safely in one run, stop at a clean phase
boundary, leave the tree buildable, and report the next concrete phase.
