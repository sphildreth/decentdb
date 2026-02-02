## Prune WAL In-Memory Index Entries After Checkpoint
**Date:** 2026-02-01
**Status:** Proposed

### Decision

During checkpoint completion, when we cannot safely truncate the WAL file (e.g. because new commits occurred during the checkpoint I/O phase), we will still prune the **in-memory WAL index** (`wal.index`) for pages that were successfully written to the main DB file.

Concretely:
- For each page image written during the checkpoint, remove `wal.index[pageId]` entries with `lsn <= safeLsn`.
- Preserve any entries with `lsn > safeLsn` (these represent commits that happened during/after the checkpoint snapshot).

### Rationale

- The WAL index is an in-memory accelerator for snapshot reads and late-commit visibility after cache eviction.
- In the "new commits during checkpoint" case, truncation is unsafe, but retaining all historical per-page versions in `wal.index` is unnecessary and can lead to unbounded memory growth.
- Once a page image at or before `safeLsn` has been copied to the main DB file, older index entries `<= safeLsn` for that page are no longer required for correctness:
  - Readers pinned at `safeLsn` (or later) can read the correct page image from the main DB file.
  - Newer commits (`> safeLsn`) remain served via WAL overlay/index.

### Alternatives Considered

1. **Do nothing**
   - Simplest, but allows `wal.index` to grow without bound in sustained write workloads where checkpoints overlap commits.
2. **Full index rebuild / compaction**
   - More work and potentially higher latency.
3. **Time/size retention policies**
   - Would require aborting readers or changing isolation guarantees; more complex and ADR-worthy on its own.

### Trade-offs

**Pros**
- Bounds memory growth of `wal.index` in steady-state, especially under frequent checkpoints.
- Preserves Snapshot Isolation semantics (no WAL truncation beyond reader safety).

**Cons**
- Some snapshot reads may fall back to reading from the main DB file rather than WAL overlay for pages pruned from the index.

### References

- ADR-0004: WAL Checkpoint Strategy
- ADR-0019: WAL Retention for Active Readers
- ADR-0023: Isolation Level Specification
