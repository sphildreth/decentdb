# Async Commit (Group Commit) WAL Sync Mode
**Date:** 2026-04-20
**Status:** Accepted

### Decision

Add a third WAL sync policy, `WalSyncMode::AsyncCommit { interval_ms: u32 }`,
that decouples commit acknowledgement from WAL durability. Under AsyncCommit:

1. Commit acknowledgement happens as soon as the WAL frame is *written* to the
   page cache via `write_at`. The fsync (`sync_data`) is deferred.
2. A dedicated background flusher thread, owned by `SharedWalInner`, wakes on
   a configurable interval (`interval_ms`, default `10`) and calls
   `sync_data` whenever the most-recently-written WAL LSN exceeds the
   most-recently-fsynced LSN.
3. A new public `Db::sync()` method blocks until all currently-committed LSNs
   are durable on disk. It is a barrier, not a fence: it does not prevent
   subsequent commits from being deferred.
4. On `SharedWalInner::drop` the flusher thread is signaled to shut down, the
   thread is joined, and a final synchronous `sync_data` is performed before
   the file handle is released.

`WalSyncMode::Full` remains the default and is unaffected. AsyncCommit is
strictly opt-in via `DbConfig::wal_sync_mode`.

### Rationale

- `Full` mode pays one `fdatasync` per commit (~30 ms on typical NVMe). For
  workloads that commit frequently (DDL bursts, small write transactions in
  bulk-load harnesses, embedded UI apps doing many tiny commits), this is the
  dominant cost.
- The Dart `console_complex` benchmark previously paid 10 fdatasyncs in the
  schema-creation phase (~325 ms). Wrapping the DDL in a single transaction
  reclaimed most of that cost (-95 %), but only because the workload is
  trivially mergeable. Real applications often cannot batch logically
  independent commits.
- Group commit is a textbook database technique (PostgreSQL's
  `synchronous_commit = off`, SQLite's `PRAGMA synchronous = NORMAL`, MySQL's
  `innodb_flush_log_at_trx_commit = 2`). It trades a bounded window of
  post-crash durability for substantial throughput gains without giving up
  atomicity, isolation, or consistency.
- Putting the flusher on a single dedicated thread per WAL handle (not per
  database connection) ensures correctness under DecentDB's
  one-writer / many-reader / shared-WAL-registry model. There is exactly one
  flusher per canonical database path regardless of how many `Db` handles are
  open.

### Durability Contract

- **Atomicity**: Unchanged. Each commit is a single WAL frame write; partial
  frames are rejected by recovery (`format::WalFrame::decode` validates
  per-frame length and checksum).
- **Consistency**: Unchanged. The page-version index and reader visibility
  semantics are independent of when fsync runs.
- **Isolation**: Unchanged. Snapshot reads see the post-write LSN regardless
  of fsync state.
- **Durability**: *Weakened by design.* A successful commit under AsyncCommit
  guarantees the data is durable only after one of:
  1. `Db::sync()` returns successfully, or
  2. the next background flush tick (≤ `interval_ms` later) runs to
     completion, or
  3. the database is closed cleanly (`Drop` flushes synchronously).

  A power loss or process kill within the interval window may lose all
  commits since the last flush tick. Recovery still produces a consistent
  state; it just rolls back to the last frame whose checksum is intact and
  whose preceding bytes are entirely on disk. **Process crashes** that do not
  involve OS/host failure do *not* lose data because the kernel page cache
  retains the writes.

### Concurrency and Shared-WAL Implications

- `WalHandle::acquire` is keyed by canonical database path; a second `Db`
  open against the same file reuses the existing `SharedWalInner`. The first
  open's `wal_sync_mode` wins. This is documented as a known limitation and
  matches the existing behavior for sync-mode reuse (no change).
- The flusher thread reads `dirty_lsn` and `durable_lsn` (both
  `AtomicU64`) and calls `file.sync_data()` outside any `Mutex`. It does not
  contend with writers on the index lock.
- Shutdown uses an `AtomicBool` flag plus a `Condvar` to allow timely wakeup
  on Drop without polling.

### Alternatives Considered

- **Catalog-only coalescing.** Bypass fsync only for DDL/catalog commits. Too
  narrow: real workloads' commit-rate bottleneck is data writes, not DDL.
  Implementation also requires the writer to know whether a commit touches
  catalog pages only, which couples WAL semantics to higher-level
  transaction state.
- **Per-commit caller-controlled durability flag.** Pass `durable: bool` to
  every commit call. Bloats the public API; verbose for the common case where
  a session wants the same policy throughout its lifetime.
- **Group-commit on the writer thread itself** (no background flusher).
  Coalesces only commits already queued at the moment of fsync; does nothing
  for serialized single-writer workloads where each commit waits for the
  previous fsync. The background flusher is the only design that benefits
  the single-writer steady state.

### Trade-offs

- **Pros**: dramatically lower commit latency for write-heavy workloads;
  zero impact when not opted in; clean barrier API for callers that need
  point-in-time durability; well-understood industry pattern.
- **Cons**: weakens the durability contract; introduces a long-lived
  background thread per WAL handle (resource cost); requires careful Drop
  ordering to avoid losing the final fsync.

### Implementation Notes

- `WalSyncMode` becomes:
  ```rust
  pub enum WalSyncMode {
      Full,
      Normal,
      AsyncCommit { interval_ms: u32 },
      TestingOnlyUnsafeNoSync,
  }
  ```
  The variant carries its own interval to avoid a parallel
  `DbConfig::async_commit_interval_ms` field that could drift out of sync
  with the mode.
- `SharedWalInner` gains an `Option<AsyncCommitState>` and the flusher is
  spawned in `build_handle` when the variant matches.
- `Db::sync()` calls into `WalHandle::flush_to_durable()` which busy-waits on
  `durable_lsn >= dirty_lsn` (with `Condvar` notify-on-flush). For
  non-AsyncCommit modes it is a no-op (commits are already durable).

### References

- ADR 0002: WAL commit record format
- ADR 0004: WAL checkpoint strategy
- ADR 0019: WAL retention for active readers
- `crates/decentdb/src/wal/writer.rs::sync_for_mode`
- `crates/decentdb/src/wal/shared.rs::build_handle`
