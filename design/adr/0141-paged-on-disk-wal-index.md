# Paged On-Disk WAL Index with Bounded In-Memory Hot Set
**Date:** 2026-04-22
**Status:** Accepted (2026-04-23) — scaffolding lands; full sidecar format remains a follow-up.

### Decision (proposed)

Persist the WAL page-version index to a dedicated on-disk structure with a
bounded in-memory hot set, replacing the unbounded
`HashMap<PageId, Vec<WalVersion>>` currently held in `SharedWalInner::index`.

This ADR is explicitly **Deferred** until ADR 0140 (`WalVersion`
discriminated payload) has shipped and been measured. The expected benefit
of ADR 0140 is large enough that this ADR may not be needed; if it is, the
post-0140 measurements will inform the on-disk index design.

### Rationale

After ADR 0140 the per-version *payload* cost is paid only for the hot K
versions per page plus active-reader retention. The remaining cost is the
**index entry itself** — `(PageId, lsn, payload-discriminator)` — which is
~24 bytes per dirty page kept in a `HashMap`. For workloads that touch
many millions of distinct pages between checkpoints (large bulk loads with
small commit cadence, long-lived reader sessions per ADR 0019), this
in-memory cost can still reach hundreds of MB.

A paged on-disk index would:

1. cap in-memory state at a configurable hot-set size;
2. survive process restart faster than re-scanning the WAL frames;
3. enable the background checkpoint worker (ADR 0058) to process WAL
   regions independently of the writer thread.

### Why Deferred

- Slices M1, M2, M4 (ADRs 0137, 0138, 0140) collectively reduce the
  observed memory footprint by an estimated 20×. Deferring this ADR until
  those land lets us measure whether a further reduction is needed, and
  what the realistic upper bound on dirty-page count between checkpoints
  actually is in production workloads.
- An on-disk WAL index is a non-trivial format change (new file, new
  recovery path, new corruption surface). Doing it before establishing
  the empirical need would be premature.
- Coordinated change with ADR 0058 (background checkpoint worker)
  preferred — both touch the same boundary.

### Open Questions

- **File layout.** Sidecar file (e.g. `*.wal-idx`) or interleaved with the
  WAL?
- **Recovery semantics.** Rebuild from WAL on missing/corrupt index? (Yes,
  almost certainly — keeps the index a pure cache.)
- **Hot-set eviction policy.** LRU keyed on page id, or recency-weighted?
- **Interaction with ADR 0067 mmap reads.** If the index is sidecar, can
  it also be `mmap`-backed?

### References

- design/2026-04-25.ENGINE-MEMORY-WORK.md (Phase 4)
- design/adr/0019-wal-retention-for-active-readers.md
- design/adr/0033-wal-frame-format.md
- design/adr/0056-wal-index-pruning-on-checkpoint.md
- design/adr/0058-background-incremental-checkpoint-worker.md
- design/adr/0067-wal-mmap-write-path.md
- design/adr/0137-size-based-auto-checkpoint-trigger.md
- design/adr/0138-post-checkpoint-heap-release.md
- design/adr/0140-walversion-discriminated-payload.md
