# ADR 0162: Engine-Owned Write Queue And Strict Group Commit

**Date:** 2026-05-20

**Status:** Accepted

## Context

DecentDB intentionally keeps a single-process, one-writer/many-readers model.
That model is durable and understandable, but host-language callers should not
have to invent their own writer mutexes, retry loops, queue limits, timeout
rules, or busy-error mappings.

ADR 0135 already provides `WalSyncMode::AsyncCommit`, which acknowledges writes
before the covering fsync. The write-queue feature needs a different contract:
several queued commits may share a physical sync, but successful queued callers
must not be acknowledged until the sync covering their commit has completed
under synchronous WAL modes.

## Decision

Add an explicit engine-owned write queue with strict queued group commit.

Key decisions:

1. Keep direct Rust execution direct. The queue is used by explicit queued APIs
   and by bindings that choose those APIs.
2. Use a lazy per-`Db` queue. No queue state is allocated until queued APIs or
   metrics are used. Cloned `Db` handles share the queue; separately opened
   handles still serialize through the shared WAL writer lock but own separate
   queue admission state. This avoids a global queue lifetime that would need to
   coordinate host-runtime callbacks, prepared statement ownership, and shutdown
   across independent binding connections. A future ADR may move the queue to
   the shared WAL registry if cross-handle fairness becomes a measured need.
3. Do not spawn a background writer thread. The first waiter that can acquire
   the queue executor role drains a bounded FIFO batch on its own thread, which
   avoids worker lifecycle problems, reference cycles, and per-request OS
   threads.
4. Bound admission by `write_queue_capacity`. `0` is clamped to `1`.
5. Support queue timeout and cancellation-before-execution. Once execution has
   started, the engine returns the definitive execution/commit result rather
   than reporting a misleading cancellation.
6. Reject explicit transaction-control SQL (`BEGIN`, `COMMIT`, `ROLLBACK`, and
   savepoint control) on the queued path for the first public contract. Long
   explicit transactions continue to use direct transaction APIs. This avoids
   leaving handle-owned transaction state behind after a queued timeout or
   cancellation.
7. Implement strict group commit by a scoped thread-local WAL sync deferral
   guard that is only activated by the queue executor. Direct commits still sync
   exactly as before.
8. Use `max_group_commit_delay_us = 0` by default. The queue drains work that is
   already ready without sleeping on the single-writer path.
9. Expose queue outcomes as stable typed errors and C ABI status codes:
   `BUSY`, `TIMEOUT`, `CANCELED`, `QUEUE_FULL`, and `QUEUE_CLOSED`.
10. Expose queue metrics through Rust and C ABI snapshots so tests and bindings
    can diagnose queue depth, admissions, timeouts, cancellations, failures,
    grouped batches, physical syncs, and estimated syncs saved.
11. Treat self-contained queued SQL statements and batches as the first stable
    binding contract. Bindings must not invent independent typed parameter
    marshaling for prepared-statement queueing; providers that need automatic
    queued prepared execution should wait for a dedicated C ABI extension.

## Consequences

The direct path keeps its current low-contention performance profile. Queued
callers get bounded admission, FIFO execution for admitted requests, durable
success acknowledgements under synchronous WAL modes, and observable counters.

The first release does not provide queued explicit transaction leases. That is a
deliberate safety tradeoff: the C ABI must not call back into host runtimes while
holding writer transaction state, and a timeout must never leave ambiguous
transaction ownership. A future ADR can add bounded queued writer leases if a
binding has a measured need that cannot be satisfied by self-contained queued
statements or batches.

Some high-level binding operations continue to use direct prepared statements
even when queue configuration is available. That is intentional for this ADR:
prepared statements already carry typed parameter state inside the native
statement handle, while `ddb_db_execute_queued` accepts declarative SQL plus C
ABI values. A future queued prepared-statement API can bridge that gap without
duplicating type conversion in every binding.

`WalSyncMode::AsyncCommit` remains a separate opt-in durability mode. Queued
strict group commit does not silently downgrade `WalSyncMode::Full`.
