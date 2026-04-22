# Size-Based Auto-Checkpoint Trigger
**Date:** 2026-04-22
**Status:** Accepted

### Decision

Add two writer-side automatic checkpoint triggers to `wal::writer::commit_pages`
and `wal::writer::commit_pages_if_latest`, evaluated *after* `wal_end_lsn` is
published and *after* the writer state guard is released:

1. **Page-count threshold** — `DbConfig::wal_checkpoint_threshold_pages: u32`
   (default `4096`). A successful commit increments
   `SharedWalInner::pages_since_checkpoint: AtomicU32` by the number of dirty
   pages in that commit. When the counter exceeds the threshold the writer
   triggers a synchronous checkpoint.
2. **Byte threshold** — `DbConfig::wal_checkpoint_threshold_bytes: u64`
   (default `64 * 1024 * 1024`, 64 MB). Computed as `wal_end_lsn -
   WAL_HEADER_SIZE`; no new counter required.

Both triggers are gated on `reader_registry.active_reader_count() == 0` and
on `checkpoint_pending` being false. If readers are active the triggers are
skipped silently — the engine retains the existing ADR 0019 reader-aware
retention behavior. If `checkpoint_pending` is true (a checkpoint is already
in flight on another path), the trigger is also skipped to avoid re-entry.

Setting either threshold to `0` disables that trigger. Setting both to `0`
restores pre-ADR behavior (timeout-based checkpointing only, per ADR 0004).

### Rationale

The probe captured in
[`design/2026-04-22.ENGINE-MEMORY-PLAN.md`](../2026-04-22.ENGINE-MEMORY-PLAN.md)
demonstrates that under sustained writes the existing checkpoint policy
(`checkpoint_timeout_sec: 30`) does not fire frequently enough to bound
in-memory WAL state. A 6.5-second 5 M-row load produced
`wal_versions = 47 173`, `wal_file_size = 192 MB`,
`last_checkpoint_lsn = 0` — i.e., the writer never auto-checkpointed, and
the in-memory `WalIndex` retained ≈ 184 MB of `Arc<[u8]>` page payloads.
This dominates engine memory growth and amplifies allocator fragmentation
(see ADR 0138).

Size-based triggers bound the in-memory WAL footprint to a configurable
fraction of host RAM that is independent of write rate. Combined with the
existing reader-aware checkpoint logic (ADR 0019) and pruning (ADR 0056),
this gives embedders a single knob with predictable units.

### Alternatives Considered

- **Background checkpoint worker.** ADR 0058 covers this and is currently
  Deferred. A background worker is the structural answer for the
  active-reader case but adds concurrency surface area; this ADR is the
  small, synchronous-only step that delivers most of the benefit without
  new threading.
- **Lower the timeout default.** `checkpoint_timeout_sec` is wall-clock and
  unaware of WAL size; on a fast NVMe, 30 seconds of sustained inserts can
  produce gigabytes of WAL. A wall-clock-only knob doesn't bound memory.
- **Per-commit checkpoint.** Cheapest in memory, prohibitively expensive in
  fsync count (would defeat ADR 0037 group-commit batching).

### Trade-offs

- **Pros:** bounded in-memory WAL footprint with predictable units; preserves
  reader-aware semantics; opt-out via `0`; no on-disk format change; no new
  threading.
- **Cons:** adds one synchronous checkpoint inside the commit path when the
  threshold is crossed (latency spike on that single commit); active-reader
  workloads still grow unbounded until the reader completes (Phase 4 work);
  thresholds are per-handle but shared via the WAL registry — first opener
  wins (matches existing behavior for `sync_mode` and `page_size`).
- **Throughput impact:** expected ≤ 10 % regression on the rust-baseline
  `seed_songs` step at the default thresholds; benched as part of slice M1
  acceptance.

### Implementation Notes

- The writer must drop its `write_lock` guard before calling
  `checkpoint::checkpoint()` because `checkpoint::checkpoint()` re-acquires
  the same lock.
- Counters reset inside `checkpoint::checkpoint()` after the index is
  cleared/pruned, under the same lock that publishes `last_checkpoint_lsn`.
- The trigger is best-effort: if the threshold is exceeded but readers are
  active, the next reader-free commit will trigger. No queue, no retry
  bookkeeping.
- Defaults assume the default 4 KB `page_size`. With larger pages the
  byte threshold dominates; with smaller payloads the page threshold
  dominates. Both being active simultaneously gives the right behavior for
  any combination.

### References

- design/2026-04-22.ENGINE-MEMORY-PLAN.md (slice M1)
- design/adr/0004-wal-checkpoint-strategy.md
- design/adr/0019-wal-retention-for-active-readers.md
- design/adr/0037-group-commit-wal-batching.md
- design/adr/0056-wal-index-pruning-on-checkpoint.md
- design/adr/0058-background-incremental-checkpoint-worker.md
