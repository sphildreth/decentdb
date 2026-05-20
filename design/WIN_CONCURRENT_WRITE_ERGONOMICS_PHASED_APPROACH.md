# Concurrent Write Ergonomics: Phased Implementation Approach

**Date:** 2026-05-20  
**Status:** Proposed implementation approach  
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md) priority #1  
**Document Type:** Phased delivery plan; not a final ADR  
**Audience:** Core engine developers, WAL/storage maintainers, C ABI maintainers, binding maintainers, CLI maintainers, benchmark maintainers, documentation authors, coding agents  
**Related inputs:** [`FUTURE_WINS.md`](FUTURE_WINS.md), [`adr/0135-async-commit-wal-group-commit.md`](adr/0135-async-commit-wal-group-commit.md), [`adr/0037-group-commit-wal-batching.md`](adr/0037-group-commit-wal-batching.md), [`SPEC.md`](SPEC.md), [`bindings/SQLC_SUPPORT.md`](bindings/SQLC_SUPPORT.md), [`bindings/SQLALCHEMY_SUPPORT.md`](bindings/SQLALCHEMY_SUPPORT.md), [`bindings/DAPPER_SUPPORT.md`](bindings/DAPPER_SUPPORT.md), [`bindings/NODEJS_SUPPORT.md`](bindings/NODEJS_SUPPORT.md)

---

## 1. Executive Summary

The roadmap item "Concurrent write ergonomics: write queue plus strict group commit" should be delivered as an end-to-end product feature, not as a core-only primitive that bindings may or may not adopt later.

The feature should make DecentDB's one-writer model pleasant under application concurrency:

1. Applications and bindings submit write work into an engine-owned queue.
2. The engine admits work according to bounded capacity, timeout, and cancellation rules.
3. The engine executes admitted work serially through the existing single writer.
4. Committed queued transactions may share one physical WAL sync.
5. In strict durable mode, callers receive success only after the WAL sync covering their transaction has completed.
6. Every maintained binding exposes the same semantics in idiomatic language terms.

The key product rule:

```text
The feature is not done until maintained bindings can use it and tests prove
that they benefit from it without hiding weaker durability semantics.
```

The key performance rule:

```text
The direct low-contention path remains excellent. Benchmark utilities,
single-writer applications, and native Rust callers must not be forced through
a slower queue path just because the engine supports queued writes.
```

This document intentionally describes a phased implementation, but the phases are work packages inside one tracked feature. Public completion requires the core engine, C ABI, maintained bindings, tests, benchmarks, and docs to land together or behind one coherent release gate.

---

## 2. Why This Feature Exists

DecentDB's single-process, one-writer/many-readers model is the right durability and implementation model for the engine. The problem is ergonomics, not the core invariant.

Without an engine-owned write queue, every binding and application must answer the same questions independently:

- What happens when two user threads try to write at once?
- Do we spin, block, fail, or retry?
- Which timeout applies: command timeout, busy timeout, context deadline, or transaction timeout?
- Can queued work be canceled safely?
- How are queue-full and timeout errors represented?
- Can small independent commits share sync cost without weakening durability?
- How can users observe contention and queue behavior?

If these policies live only in bindings, DecentDB gets inconsistent behavior across .NET, Python, Go, Java, Node, Dart, and web runtimes. If the policies live in the engine and are exposed through the C ABI, bindings can remain thin, consistent, and easier to test.

The feature should therefore centralize write scheduling in Rust while allowing each binding to expose the contract idiomatically.

---

## 3. Important Distinction From Existing Async Commit

ADR 0135 added `WalSyncMode::AsyncCommit { interval_ms }`. That mode improves commit latency by acknowledging commits after WAL bytes are written but before they are fsynced. It is useful, explicit, and intentionally weakens post-crash durability within the configured interval.

This feature is different.

Strict queued group commit should preserve caller-visible durable commit semantics:

```text
tx1 writes commit record
tx2 writes commit record
tx3 writes commit record
fsync WAL once
ack tx1, tx2, tx3
```

The sync is shared. The durable-success acknowledgement is not moved earlier.

The feature must not turn `WalSyncMode::Full` into async commit by accident or by default. If a caller wants ADR 0135 behavior, they should explicitly choose `WalSyncMode::AsyncCommit`. If a caller chooses strict queued writes under `WalSyncMode::Full`, success means the transaction is durable.

---

## 4. Product Principles

### 4.1 Durability First

The queue schedules transactions. It does not redefine commit. The WAL, recovery, checksum, frame boundaries, commit publication, and fsync rules remain the source of truth for durability.

### 4.2 One Writer Remains The Architecture

The queue does not introduce hidden multi-writer MVCC. It makes a single writer easier to use by moving contention management into the engine.

### 4.3 Direct Path Remains A First-Class Path

Benchmarks, embedded utilities, bulk-load tools, and native Rust callers often already know they have one writer. They should not pay for queue worker allocation, channel hops, timers, or scheduler handoff unless they opt into a queued API or binding mode that needs it.

### 4.4 Bindings Are In Scope

The feature is not a core-only milestone. The C ABI and maintained bindings are part of the implementation task.

### 4.5 Bounded And Observable

Every waiting state must have bounded behavior:

- bounded queue capacity
- bounded timeout behavior
- explicit cancellation states
- clear error classes
- counters and diagnostics sufficient to debug contention

### 4.6 No Silent Behavioral Drift

Existing binding APIs should keep working. New queue-backed behavior must be documented and configurable. Error mappings should be stable enough for users to handle them deliberately.

---

## 5. Goals

1. Add an engine-owned write queue for in-process write concurrency.
2. Preserve the existing direct write path for low-contention native use.
3. Add strict durable group commit for queued writes.
4. Provide bounded queue admission, timeout, and cancellation semantics.
5. Add typed engine errors for queue-full, queue-timeout, and canceled-before-run outcomes.
6. Add minimal metrics needed to benchmark and debug queue behavior.
7. Expose the queue contract through the stable C ABI.
8. Update all maintained bindings to configure, use, and test the queue.
9. Add cross-binding smoke/stress tests for concurrent writes.
10. Add benchmark coverage that protects direct-path and queued-path behavior.
11. Document the feature in user-facing and binding-specific docs.

---

## 6. Non-Goals

The first complete version of this feature should not include:

1. Cross-process write coordination. That is roadmap item #10.
2. Multi-writer MVCC.
3. Distributed transactions.
4. Transparent network queueing or server mode.
5. A general durable job queue.
6. Arbitrary host-language callbacks executed while the Rust writer lock is held through the C ABI.
7. Binding rewrites unrelated to write scheduling.
8. A mandatory queue for direct Rust or benchmark workloads.
9. A hidden downgrade from `WalSyncMode::Full` to `AsyncCommit`.
10. Multi-tab browser ownership semantics. Browser production coordination is a separate roadmap item, though the web binding should expose the in-runtime queue contract where supported.

---

## 7. Definitions

**Direct write path:** The current low-level engine path where a caller executes write work directly against a `Db` handle and the engine serializes through its existing writer lock/transaction machinery.

**Queued write path:** A new API path where a caller submits a self-contained write request to an engine-owned queue and waits for a result.

**Admission:** The point at which the engine accepts a request into the bounded queue. Failure before admission means no transaction ran.

**Execution:** The phase where the single writer dequeues the request and runs it through normal SQL, constraint, transaction, pager, WAL, and recovery-safe commit logic.

**Commit publication:** The point where the engine has assigned/published the transaction's committed state according to existing visibility rules.

**Strict group commit:** A batching policy in which several committed queued transactions share one physical WAL sync, and each caller receives success only after the sync covering that transaction is complete.

**Durable acknowledgement:** The returned success result for a write whose WAL commit is known to be on stable storage under the selected sync mode.

**Cancellation-before-run:** Caller cancellation while the request is waiting in the queue. This is safe to report as canceled because no transaction has run.

**Commit-in-flight:** A state where cancellation has arrived after the engine has started committing. The engine must return a definitive committed or failed result; it must not pretend the transaction was canceled if commit may have succeeded.

---

## 8. Target End State

### 8.1 User Experience

Users should be able to write application code that fires multiple concurrent write requests without hand-building a writer mutex:

```text
many app tasks/threads
  -> submit writes
  -> DecentDB queues bounded work
  -> one writer executes serially
  -> strict group commit batches syncs
  -> each caller receives result/error/canceled/timeout
```

When contention is low, users who call the direct path should see the same performance profile they had before this feature.

### 8.2 Binding Experience

Each binding should expose the queue through existing idioms:

- .NET: connection string/options, command timeout, async cancellation tokens, ADO.NET and EF Core behavior.
- Python: DB-API connection options, SQLAlchemy dialect integration, thread behavior, DB-API exception mapping.
- Go: `database/sql` DSN options, `ExecContext`, `BeginTx`, context deadline and cancellation behavior.
- Java: JDBC URL properties, `Statement.setQueryTimeout`, transaction behavior, SQL exception classes.
- Node: Promise APIs, Knex integration, AbortSignal/timeouts, no event-loop blocking.
- Dart: `Future`-based APIs, timeout/cancel conventions, native and web runtime behavior.
- C: smoke tests and direct C ABI examples.
- Web/WASM: worker/OPFS runtime integration where durable sync semantics are supported by the runtime; unsupported pieces must fail explicitly.

### 8.3 Operational Experience

Users and maintainers should be able to answer:

- Is the write queue enabled?
- What is the queue capacity?
- What is the current queue depth?
- How many writes were admitted, rejected, timed out, canceled, committed, and failed?
- What is the average and p95 queue wait time?
- What is the group commit batch size distribution?
- How many physical WAL syncs were saved?
- Did direct-path performance regress?

Roadmap item #2 should eventually expose these through `sys.*` surfaces, but this feature needs enough internal and test-visible metrics to prove behavior.

---

## 9. Proposed Architecture Shape

This section is a design sketch for planning. The implementation ADR must make the exact choices.

### 9.1 Two Public Execution Paths

DecentDB should retain two paths:

1. **Direct path:** existing APIs remain available and remain optimized for simple embedded use.
2. **Queued path:** new APIs submit work to the engine-owned queue.

Bindings may choose the queued path as their default for concurrent/pool-style usage, but native benchmarks and advanced users should be able to stay direct.

### 9.2 Queue Ownership

There should be one logical write queue per in-process shared database/WAL handle, not one queue per binding connection. This prevents each binding from inventing separate fairness and contention rules.

The queue should be lazy:

- no queue worker allocation until queued mode/API is used;
- no queue metrics allocation beyond cheap counters until needed;
- no direct-path channel hop.

### 9.3 Request Shape

The queue should accept self-contained write requests. Candidate request forms:

- one SQL statement with positional parameters;
- a SQL batch that runs as one transaction;
- a prepared statement execution request;
- a Rust-only closure API for native users, if it can be kept safe and well-bounded.

The C ABI should avoid arbitrary host-language callbacks running inside the writer. Bindings should pass declarative work to the engine rather than asking Rust to call back into managed runtimes while holding transaction state.

### 9.4 Explicit Transactions

Explicit transaction support needs careful design because users expect multiple statements to share one transaction.

Acceptable approaches for the first version:

1. **Queued transaction batch:** a binding submits a multi-statement batch that runs as one transaction on the queue.
2. **Queued writer lease:** a binding acquires a writer lease from the queue, runs a bounded transaction, and releases it. The lease must have a timeout and must not allow unbounded host callback behavior through C without clear safety rules.

The ADR should choose one primary approach and define how existing `begin/commit/rollback` APIs interact with queued mode.

### 9.5 Strict Group Commit Coordinator

The group commit coordinator should batch commits that are already ready rather than sleeping by default just to hope for more work. This protects single-writer/single-reader performance.

Recommended default:

```text
max_group_commit_delay_us = 0
```

With zero delay, the writer drains currently ready commit records, syncs once, and acknowledges the covered callers. A non-zero delay can be an explicit tuning option for workloads that prefer throughput over tail latency while still retaining strict durable acknowledgement.

### 9.6 Sync Modes

The design must keep these concepts separate:

- `WalSyncMode::Full`: durable acknowledgement requires sync.
- `WalSyncMode::Normal`: follows its documented weaker sync behavior.
- `WalSyncMode::AsyncCommit`: opt-in async durability behavior from ADR 0135.
- queued strict group commit: scheduling/batching for queued writes; does not imply async acknowledgement.

The ADR should define whether queued group commit is available under every sync mode or only under selected modes. The conservative first version should focus on `WalSyncMode::Full` because that is where strict batching matters most.

---

## 10. Cancellation And Timeout Contract

This feature should make cancellation behavior explicit and consistent.

### 10.1 Before Admission

If capacity is exhausted or the caller's deadline expires before admission, the request does not run.

Possible outcomes:

- queue full
- queue admission timeout
- canceled before admission

### 10.2 Waiting In Queue

If the request has been admitted but not started, cancellation is safe:

```text
outcome = canceled_before_execution
transaction_effect = none
```

### 10.3 During Execution

Once execution starts, cancellation semantics depend on where execution is:

- If cancellation is checked before mutation, the request may fail as canceled.
- If mutation has begun but commit has not started, the engine may roll back and report canceled if rollback succeeds.
- If commit has started, the engine must drive to a definitive commit/failure result.

### 10.4 During WAL Sync

Cancellation during strict group commit sync must not produce a misleading "canceled" result. The caller should receive the durable commit result or a real commit/sync failure.

### 10.5 Binding Mapping

Bindings should map native states to language idioms without losing the transaction truth:

- timeout before run: timeout/busy/operational exception;
- canceled before run: cancellation exception or canceled result;
- committed despite late cancellation: success or committed result;
- execution error: existing SQL/constraint/transaction errors;
- sync failure: I/O error with enough context to show commit failed or could not be acknowledged.

---

## 11. Error And Configuration Surface

### 11.1 Engine Errors

The core engine should add typed errors for at least:

- write queue full
- write queue timeout
- write canceled before execution
- write queue closed/shutting down

The ADR should decide whether commit-in-flight needs a public error. Prefer returning a definitive success/failure whenever possible.

### 11.2 C ABI Status Codes

The C ABI should expose stable numeric error codes rather than mapping queue outcomes to generic transaction errors.

Candidate additions:

```text
DDB_ERR_BUSY
DDB_ERR_TIMEOUT
DDB_ERR_CANCELED
DDB_ERR_QUEUE_FULL
```

The final set should be minimal but expressive enough for bindings to preserve semantics.

### 11.3 Configuration

Candidate config fields:

```text
write_queue_enabled = false | true | auto
write_queue_capacity = <positive integer>
write_queue_default_timeout_ms = <milliseconds, 0 means no default>
write_queue_group_commit = off | strict
write_queue_max_batch = <positive integer>
write_queue_max_group_delay_us = <microseconds, default 0>
```

Configuration must be available through:

- Rust `DbConfig`;
- C ABI open options;
- binding connection strings/options;
- docs and examples.

The final ADR should decide whether `auto` is allowed. If it is allowed, it must be precisely defined and benchmarked. A vague "auto" mode is worse than an explicit mode.

---

## 12. Performance Guardrails

Performance protection is part of the feature, not a follow-up.

### 12.1 Direct Path Budgets

The existing direct path should be benchmarked before implementation and after each major phase.

Required scenarios:

1. single writer, no reader, autocommit inserts;
2. single writer, one steady reader;
3. single writer, long reader holding a snapshot;
4. explicit transaction with many writes;
5. benchmark harness workloads that currently use direct APIs.

Proposed regression budgets:

- Direct single-writer throughput: no statistically significant regression; use 2 percent as an initial local benchmark tripwire.
- Direct single-reader latency under writer load: no statistically significant regression; use 5 percent p95 as an initial tripwire.
- Direct explicit transaction throughput: no statistically significant regression; use 2 percent as an initial tripwire.

The exact budgets should be finalized in the ADR after measuring baseline noise.

### 12.2 Queued Path Budgets

Queued single-writer performance may pay a small scheduling cost when the queued API is explicitly used, but that overhead must be bounded and measured.

Required scenarios:

1. queued single writer, no contention;
2. queued single writer plus one reader;
3. many concurrent writers submitting small autocommit transactions;
4. many concurrent writers with queue capacity pressure;
5. cancellation storm while writes continue;
6. group commit batch size distribution under write burst.

Proposed success metrics:

- queued single-writer overhead remains small enough to document honestly;
- many-writer throughput improves materially versus external busy retry loops;
- group commit reduces physical WAL sync count under bursts;
- tail latency remains bounded by configured timeout and group delay;
- no reader starvation.

### 12.3 Thread And Allocation Guardrails

The direct path should not allocate queue workers, per-request channels, timers, or host-language futures.

The queued path should avoid unbounded allocation:

- preallocated or bounded request structures where practical;
- bounded queue capacity;
- bounded waiter state;
- no per-request OS thread.

---

## 13. Phased Delivery Plan

The phases below are ordered work packages. The public feature is complete only after Phase 8.

### Phase 0: ADR, Scope Lock, And Baseline Measurements

**Goal:** Finish the design before touching concurrency-critical code.

Deliverables:

1. ADR for engine-owned write queue and strict group commit.
2. Explicit decision on direct path versus queued path.
3. Explicit transaction semantics.
4. Cancellation/timeout state machine.
5. Error-code proposal for Rust and C ABI.
6. Config proposal.
7. Binding adoption matrix.
8. Benchmark baseline report.

Exit criteria:

- ADR accepted.
- Baseline benchmarks checked into `.tmp/` during design work and summarized in the ADR or a design note.
- Maintained binding list is explicit.
- Definition of done includes binding adoption.

### Phase 1: Benchmark And Test Harness Expansion

**Goal:** Install regression tripwires before implementation.

Deliverables:

1. Native benchmark scenarios for direct single-writer and read-under-write cases.
2. Native benchmark scenarios for queued writes, initially behind placeholder or ignored tests if API is not present yet.
3. Cross-binding test scenario definition for concurrent writes.
4. Fault-injection test plan for grouped commits.
5. CI/pre-commit check keys for the new test slices.

Exit criteria:

- Direct-path baseline can be reproduced.
- A future queue implementation has a clear pass/fail benchmark target.
- Binding smoke test shape is agreed before bindings are edited.

### Phase 2: Core Queue Without Group Commit

**Goal:** Add bounded engine-owned queue semantics while preserving existing commit behavior.

Deliverables:

1. Lazy per-shared-WAL in-process queue.
2. Request admission with capacity and timeout.
3. Cancellation-before-execution support.
4. FIFO fairness for same-priority requests.
5. Result propagation preserving existing SQL/constraint/I/O errors.
6. Tests for queue ordering, queue full, timeout, cancellation, and shutdown.
7. Direct-path benchmark comparison.

Exit criteria:

- Direct path remains within regression budget.
- Queued writes produce the same durable results as direct writes.
- No unbounded queue growth.
- No reader starvation in read-under-write tests.

### Phase 3: Strict Durable Group Commit

**Goal:** Allow already-ready queued commits to share a physical WAL sync without weakening durable acknowledgement.

Deliverables:

1. Commit batch coordinator.
2. Per-transaction committed LSN/result tracking.
3. One sync covering multiple committed queued transactions.
4. Acknowledgement only after covering sync completes under strict durable mode.
5. Recovery/fault tests for partial batches and sync failure.
6. Metrics for group size and physical sync count.
7. Benchmarks showing sync-count reduction under concurrent write bursts.

Exit criteria:

- Crash recovery accepts only complete durable commits.
- `WalSyncMode::Full` semantics are not weakened.
- Single queued writer does not sleep by default waiting for more work.
- Concurrent queued writers show measurable sync batching under bursty load.

### Phase 4: Engine Metrics, Diagnostics, And Documentation Hooks

**Goal:** Make the feature operable and prepare for roadmap item #2 observability.

Deliverables:

1. Internal counters for queue depth, admissions, rejections, timeouts, cancellations, executions, commits, failures, syncs, and batch sizes.
2. Debug/test accessors or JSON diagnostics for tests and CLI.
3. Initial docs for queue behavior and limitations.
4. Clear distinction between queued strict group commit and async commit.

Exit criteria:

- Tests can assert group commit actually happened without relying on timing guesses.
- Users can diagnose obvious queue pressure.
- Observability work can later project the same counters into `sys.*`.

### Phase 5: C ABI Contract

**Goal:** Expose the queue through the stable shared boundary used by bindings.

Deliverables:

1. Header updates in `include/decentdb.h`.
2. ABI version bump if required by project convention.
3. Stable status codes for queue outcomes.
4. Open options for queue configuration.
5. One or more queued execution APIs.
6. C smoke tests for queued writes, timeout, cancellation if representable, and group commit diagnostics.
7. C docs/examples.

Exit criteria:

- Bindings do not need private Rust APIs.
- Queue outcomes are not collapsed into generic transaction errors.
- Existing C ABI calls keep working.
- Direct C smoke tests pass.

Candidate C ABI surfaces should be evaluated in the ADR. The final API should not make JSON the only hot path for common write execution unless benchmarks prove it is acceptable.

### Phase 6: Maintained Binding Adoption

**Goal:** Update all maintained bindings to fully implement and benefit from the queue contract.

This phase may be split by language internally, but public completion requires all maintained bindings to pass their adoption tests.

#### 6.1 Shared Binding Requirements

Every maintained binding should:

1. expose queue configuration;
2. map existing busy/command timeout concepts to queue timeout where appropriate;
3. surface queue-full, timeout, and cancellation distinctly;
4. preserve direct/low-contention execution options where the binding exposes them;
5. avoid spinning internally when the engine can block or fail deliberately;
6. add concurrent write tests;
7. update binding docs.

#### 6.2 .NET: ADO.NET, EF Core, Dapper

Work items:

1. Add native P/Invoke declarations for new C ABI functions and status codes.
2. Extend `DecentDBConnectionStringBuilder` with queue options.
3. Map `Busy Timeout` and `Command Timeout` deliberately.
4. Make async command paths honor `CancellationToken` before execution.
5. Ensure late cancellation during commit returns definitive transaction state.
6. Update ADO.NET exceptions with specific queue timeout/cancel mappings.
7. Validate EF Core `SaveChanges` and migration paths under queued mode.
8. Validate Dapper concurrent `Execute` workloads.
9. Add tests in `bindings/dotnet/tests`.

Acceptance:

- ADO.NET users can enable queued writes through connection string options.
- EF Core and Dapper benefit without provider-specific writer mutexes.
- Existing direct/native tests still pass.

#### 6.3 Python: DB-API And SQLAlchemy

Work items:

1. Add connection options to `decentdb.connect`.
2. Add ctypes declarations for new C ABI functions/status codes.
3. Map queue timeout/cancel outcomes to DB-API-compatible exceptions.
4. Ensure thread tests use the engine queue rather than ad hoc retry loops.
5. Update SQLAlchemy dialect configuration if it exposes busy/timeout options.
6. Add concurrent writer tests in `bindings/python/tests`.
7. Update Python README examples.

Acceptance:

- Python users can enable queued writes through connection options.
- SQLAlchemy users get predictable operational errors/timeouts.
- Existing statement cache and fast-path optimizations still work.

#### 6.4 Go: `database/sql`

Work items:

1. Extend DSN parsing with queue options.
2. Add cgo bindings for new C ABI functions/status codes.
3. Map `context.Context` deadline/cancellation to queue behavior.
4. Ensure `ExecContext`, `QueryContext` for write statements, and `BeginTx` semantics are defined.
5. Add typed/sentinel errors where idiomatic.
6. Add concurrent writer tests in `bindings/go/decentdb-go`.
7. Update Go README.

Acceptance:

- Concurrent `database/sql` users no longer need a separate single-writer dispatcher.
- Context cancellation is honored before execution and never lies about committed work.

#### 6.5 Java: JDBC And Related Tooling

Work items:

1. Extend JNI/native layer for new C ABI functions/status codes.
2. Add JDBC URL properties for queue configuration.
3. Map `Statement.setQueryTimeout` and connection properties to queue timeout where appropriate.
4. Use `SQLTimeoutException`, `SQLTransientException`, or project-specific subclasses consistently.
5. Define transaction behavior for queued mode.
6. Add concurrent writer smoke tests in `tests/bindings/java` or binding tests.
7. Update Java docs and DBeaver notes if the extension surfaces options.

Acceptance:

- JDBC users can configure queued writes without custom external locks.
- Tooling receives standard SQL exceptions for timeout/cancel cases.

#### 6.6 Node And Knex

Work items:

1. Expose queue options in the Node package and Knex client configuration.
2. Ensure Promise APIs do not block the event loop while waiting in queue.
3. Map cancellation/timeout to Promise rejection shapes documented by the binding.
4. Support `AbortSignal` if the binding already has or adds cancellation conventions.
5. Add concurrent writer tests in `tests/bindings/node` and `bindings/node/knex-decentdb/test`.
6. Update Node and Knex READMEs.

Acceptance:

- Knex users can run concurrent write workloads without ad hoc serialization.
- Event-loop behavior remains acceptable under queue waits.

#### 6.7 Dart

Work items:

1. Bind new C ABI functions/status codes through FFI.
2. Add queue options to `Database.open`/create APIs.
3. Map timeout/cancellation to `Future`-based Dart idioms.
4. Validate native prepared statements and `Database.inTransaction` behavior.
5. Add concurrent writer tests in `tests/bindings/dart`.
6. Update Dart README.

Acceptance:

- Dart users can enable queued writes through normal database options.
- UI-style applications can avoid hand-rolled writer dispatch.

#### 6.8 Web/WASM

Work items:

1. Determine whether the web binding can support strict durable group commit under the current OPFS/worker runtime.
2. Expose the same configuration shape where supported.
3. Fail explicitly where runtime durability primitives are insufficient.
4. Keep multi-tab coordination out of this feature; document it as part of the production browser roadmap item.
5. Add Playwright smoke coverage for supported queued write behavior.
6. Update `bindings/web/README.md` and `docs/api/wasm.md` if applicable.

Acceptance:

- Web users get the same in-runtime queue semantics where the platform supports them.
- Unsupported durability/group-commit behavior is explicit, not silently weak.

### Phase 7: Cross-Binding Validation

**Goal:** Prove the feature works consistently across languages.

Deliverables:

1. Shared test scenario in `tests/bindings`:
   - create table;
   - launch many concurrent write tasks;
   - verify all committed rows;
   - verify no duplicate primary keys;
   - verify queue metrics if exposed;
   - verify timeout behavior with tiny capacity/timeout.
2. Language-specific implementations for C, .NET, Python, Go, Java, Node, Dart, and web where supported.
3. Pre-commit check keys for fast smoke and full/paranoid runs.
4. Failure diagnostics that identify queue timeout versus generic failure.

Exit criteria:

- All maintained binding smoke tests pass.
- Missing toolchains skip gracefully under existing repository conventions.
- Binding docs match observed behavior.

### Phase 8: Release Docs, Migration Guidance, And Completion Gate

**Goal:** Ship the feature as a coherent contract.

Deliverables:

1. User guide page for write concurrency.
2. C ABI documentation.
3. Binding-specific docs.
4. Benchmark summary.
5. Troubleshooting section for queue full, timeout, cancellation, and long transactions.
6. Clear comparison of direct writes, queued strict group commit, and async commit.
7. Roadmap update marking the feature complete only after all maintained bindings are updated.

Exit criteria:

- Docs explain that DecentDB optimizes one writer rather than pretending to be a server database.
- Users can choose direct, queued strict, or async behavior deliberately.
- Release notes list new config options and error mappings.

---

## 14. Definition Of Done

The roadmap item is complete only when all of the following are true:

1. ADR accepted.
2. Core queue implemented.
3. Strict durable group commit implemented.
4. Direct path preserved and benchmarked.
5. Rust tests pass.
6. Crash/fault tests cover grouped commit boundaries.
7. C ABI exposes configuration, execution, errors, and diagnostics needed by bindings.
8. C smoke tests pass.
9. .NET binding updated and tested.
10. Python binding updated and tested.
11. Go binding updated and tested.
12. Java binding updated and tested.
13. Node/Knex binding updated and tested.
14. Dart binding updated and tested.
15. Web binding updated or explicitly documented as unsupported for the strict portion, with tests for supported behavior.
16. Cross-binding smoke tests pass.
17. User docs and binding docs updated.
18. `FUTURE_WINS.md` is updated to mark the item complete or in progress with accurate source-of-truth links.

---

## 15. Validation Matrix

| Surface | Direct Path | Queued Path | Timeout | Cancellation | Group Commit | Docs |
|---|---:|---:|---:|---:|---:|---:|
| Rust core | Required | Required | Required | Required | Required | Required |
| C ABI | Required | Required | Required | If representable | Required diagnostics | Required |
| .NET | Required | Required | Required | Required | Via C ABI | Required |
| Python | Required | Required | Required | If representable | Via C ABI | Required |
| Go | Required | Required | Required | Required | Via C ABI | Required |
| Java | Required | Required | Required | If representable | Via C ABI | Required |
| Node/Knex | Required | Required | Required | Preferred | Via C ABI | Required |
| Dart | Required | Required | Required | Preferred | Via C ABI | Required |
| Web/WASM | Required where supported | Required where supported | Required where supported | Preferred | Platform-dependent | Required |

---

## 16. Risks And Mitigations

### 16.1 Direct-Path Regression

Risk: queue infrastructure accidentally adds overhead to direct writes.

Mitigation:

- lazy queue initialization;
- no direct-path channel hop;
- direct-path benchmark gate;
- inspect allocation/thread behavior.

### 16.2 Durability Confusion

Risk: users confuse strict queued group commit with ADR 0135 async commit.

Mitigation:

- separate configuration names;
- docs with examples;
- tests that assert strict mode waits for covering sync;
- no default downgrade from `Full`.

### 16.3 Binding Inconsistency

Risk: each binding maps timeout/cancel differently.

Mitigation:

- C ABI status codes are specific;
- binding adoption matrix is part of done;
- cross-binding tests share scenarios.

### 16.4 Transaction Lifetime Bugs

Risk: queued explicit transactions hold the writer too long or call back into host runtimes unsafely.

Mitigation:

- prefer declarative queued SQL batches or bounded writer leases;
- define timeout rules;
- avoid arbitrary C ABI callbacks inside transaction state.

### 16.5 Starvation

Risk: a stream of queued writes harms readers or long-running write batches starve small writes.

Mitigation:

- preserve reader snapshot semantics;
- bound transaction lease duration where applicable;
- test reader latency under queued write load;
- expose queue wait metrics.

### 16.6 Browser Runtime Mismatch

Risk: web/OPFS cannot provide identical strict durable behavior.

Mitigation:

- document platform limits;
- expose unsupported cases explicitly;
- keep multi-tab coordination in the browser runtime roadmap item.

---

## 17. Open Questions For The ADR

1. Should the queued path be opt-in for every binding, or default-on for high-level binding APIs with direct escape hatches?
2. Should the first version support queued explicit transaction leases, or only queued transaction batches?
3. What is the final C ABI shape for queued prepared statements?
4. Which queue outcomes deserve distinct C ABI status codes?
5. Should `write_queue_enabled = auto` exist, and if so what exactly triggers queue use?
6. What group commit delay defaults are acceptable for user-facing bindings?
7. Should group commit be available under `WalSyncMode::Normal` and `AsyncCommit`, or only under `Full` initially?
8. How should queue diagnostics be exposed before the full `sys.*` observability item lands?
9. What is the exact benchmark noise floor for direct-path regression budgets?
10. Which browser durability primitives are strong enough for strict group commit in the web binding?

---

## 18. Recommended First Implementation Slice

Even though completion requires bindings, the first code slice should be narrow and reversible:

1. Add benchmark baselines.
2. Add the core queue behind an internal/unstable Rust feature or hidden config.
3. Prove direct-path zero-regression locally.
4. Add strict group commit.
5. Add C ABI surface.
6. Update one binding as a reference implementation.
7. Use that reference binding to correct the C ABI before updating the rest.
8. Update remaining bindings before declaring the roadmap item complete.

This sequence avoids designing six binding integrations against an unproven C ABI while still making binding adoption part of the same feature.

The project should not announce the feature as complete after only the core implementation.
