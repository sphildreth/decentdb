## Background / Incremental Checkpoint Worker
**Date:** 2026-02-02
**Status:** Deferred

### Decision
Draft a design for an incremental/background checkpoint worker that can spread checkpoint work over time while preserving Snapshot Isolation and the one-writer/many-reader model.

### Rationale
Checkpointing can be latency-sensitive, and long-running readers can delay truncation. A background/incremental approach may improve tail latency and smooth write throughput, but it affects scheduling, locking, and potentially durability semantics.

### Alternatives Considered
- Keep synchronous checkpoints only (status quo).
- Trigger checkpoints based on WAL growth thresholds only.
- Incremental checkpointing in small page batches with cooperative yielding.

### Trade-offs
- Improved latency/throughput stability vs increased complexity and new concurrency edge cases.
- Requires careful interaction with reader snapshots and WAL retention rules.

### References
- design/adr/0004-wal-checkpoint-strategy.md
- design/adr/0019-wal-retention-for-active-readers.md
