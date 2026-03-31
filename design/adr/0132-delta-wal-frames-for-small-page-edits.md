## ADR-0132: Delta WAL Frames for Small Page Edits
**Date:** 2026-03-31
**Status:** Proposed

### Decision

Introduce an optional WAL frame form for small in-page mutations so durable commits
do not always serialize full 4 KiB page images when only tens or hundreds of
bytes changed.

1. Add a new WAL frame type for **page deltas** that targets an existing page id
   and records one or more `(offset, length, bytes)` edits against a visible
   base page image.
2. Restrict delta frames to updates of pages that already exist at the current
   snapshot. New page allocation, page-free operations, and any change that
   re-links page chains must continue to use full page frames.
3. Keep durability semantics unchanged: commits still append to the WAL and
   fsync before returning in `FULL` durability mode.
4. Keep the main database format unchanged in this slice. Checkpoint and
   recovery must materialize delta frames back into full page images before
   applying them to the pager.
5. Use full-page WAL frames as the fallback whenever delta encoding would be too
   large, too complex, or would complicate crash recovery beyond the narrow
   allowed cases.

### Rationale

The current durable single-row commit path is structurally limited by full-page
WAL framing rather than by parser or transaction-bookkeeping overhead.

After the safe Phase 0 benchmark work completed on 2026-03-31, release custom
spot runs still showed `durable_commit_single.commit_p95_us` at roughly
`3.0 ms`, while `durable_commit_batch.rows_per_sec` and
`storage_efficiency.space_amplification` improved materially. Investigation of
the remaining single-row limiter showed that one append-only durable commit
still writes roughly three full WAL page frames:

- one table overflow page for the appended row payload,
- one manifest overflow page because the persisted table state changes, and
- one catalog root page because the manifest checksum / pointer state changes.

For the manifest and root pages, the logical mutation is usually only a small
byte patch, but the WAL still emits complete page images. That fixed per-commit
cost keeps single-row durable latency near the fsync floor even after
transaction-path cleanup.

Delta WAL frames directly attack that remaining write amplification without
weakening durability-by-default or forcing an immediate redesign of the main
database file format.

### Alternatives Considered

#### Keep full-page WAL frames only

Rejected. The benchmark gap is now structural. Further local cleanup in the SQL
execution path does not remove the fixed cost of writing complete page images
for tiny metadata edits.

#### Group commit / commit batching

Rejected for this slice. ADR-0037 already defers group commit and WAL batching
until later because it changes latency behavior, adds batching logic, and
complicates crash testing. It also does not help isolated single-connection
sequential commit latency without intentionally delaying commits.

#### Manifest / root persistence redesign

Deferred. A redesign that removes the per-commit manifest/root rewrite
requirement may be worth doing later, but it is a broader persistence-semantics
change than this slice. Delta WAL frames are a narrower attack on the current
I/O volume and do not require a main-file format migration.

#### Main-file format changes (for example inline manifest payloads)

Rejected for now. Changing root-page or manifest layout crosses the product's
format-compatibility boundary and should stay separate from the WAL-frame
decision.

### Trade-offs

- Positive: reduces WAL bytes written for small page mutations, especially the
  manifest and root pages in append-only OLTP workloads.
- Positive: preserves current durability mode semantics and the main-file page
  format.
- Positive: keeps a straightforward fallback to existing full-page frames for
  complex updates.
- Negative: recovery and checkpoint logic become more complex because delta
  frames must be applied onto a visible base page image.
- Negative: snapshot handling must ensure a delta chain is always anchored by a
  recoverable full page image.
- Negative: crash testing burden increases because recovery must tolerate torn
  histories that include both full-page and delta frames.
- Negative: this does not eliminate the fsync itself; it only attacks the fixed
  write volume around that sync.

### References

- `design/PRD.md`
- `design/adr/0033-wal-frame-format.md`
- `design/adr/0037-group-commit-wal-batching.md`
- `design/adr/0122-phase0-table-manifest-persistence.md`
- `crates/decentdb/src/wal/writer.rs`
- `crates/decentdb/src/exec/mod.rs`
