# ADR 0145: Paged Table Row Source
**Date:** 2026-04-23
**Status:** Accepted

## Context

ADR 0143 Phase B made `Db::open` cheap by deferring table materialization, and
ADR 0144 made deferred `WHERE id = ?` point lookups cheap through a persistent
primary-key locator tree. The main remaining memory problem is large-table scan
workloads.

Today `EngineRuntime` stores loaded base-table rows as:

- `tables: Arc<BTreeMap<String, Arc<TableData>>>`
- `TableData { rows: Vec<StoredRow> }`

That forces any scan, join build, aggregate input, or wide filter over a large
table to decode the whole table payload into `Vec<StoredRow>`. This is the last
major barrier to keeping resident memory proportional to the working set rather
than total table size.

## Decision

We will introduce a **table row-source abstraction** and evolve base-table
storage toward an **append-only paged row format**.

### 1. Runtime abstraction

`EngineRuntime.tables` will no longer directly expose `Arc<TableData>`.
Instead it will store:

```rust
pub(crate) enum TableRowSource {
    Resident(Arc<TableData>),
    Paged(Arc<TablePageManifest>),
}
```

`Resident` preserves today's fast path for small tables and incremental
rollout. `Paged` becomes the snapshot-stable scan source for large persisted
tables once the on-disk format lands.

### 2. On-disk shape

Persisted large-table row data will move from one overflow-backed payload per
table to an append-only page-chunk layout described by a `TablePageManifest`.

The initial Phase D layout is:

- append-only row pages,
- tombstones for deletes / rewritten rows,
- periodic compaction to prune dead versions,
- manifest-driven iteration under a pinned snapshot.

### 3. Rollout strategy

Phase D ships in slices:

1. introduce `TableRowSource` with `Resident` only and migrate call sites away
   from direct `Arc<TableData>` assumptions,
2. add manifest-backed paged iteration for read paths,
3. move selected scan-heavy operators onto paged iteration,
4. add append-only paged persistence + migration support,
5. keep resident promotion for small tables / temp tables / hot paths where it
   remains the right trade-off.

The first slice is intentionally behavior-preserving and does **not** change the
on-disk format yet.

## Rationale

- It breaks the largest refactor into mechanically safe steps.
- It preserves fast small-table behavior while making room for large-table
  streaming.
- It fits the repository's single-writer durability model and existing snapshot
  readers.
- It lets Phase C's persistent PK tree remain useful while scans migrate
  separately.

## Alternatives Considered

### Keep `Arc<TableData>` until the full paged format is ready

Rejected. That would force a single giant refactor across storage, executor, and
tests with poor reviewability and high regression risk.

### Switch directly to slotted pages with in-place updates

Rejected for the first Phase D implementation. It increases write-path and
recovery complexity before the engine has even proven the row-source seam.

### Stream directly from the current overflow payload format

Rejected as the end state. It helps some reads, but it does not solve the write
amplification and whole-table rewrite costs that remain after ADR 0144.

## Trade-offs

### Positive

- Reduces architectural coupling to `Vec<StoredRow>`.
- Enables scan/aggregate memory work without regressing point lookups.
- Supports incremental migration of executor hot paths.

### Negative

- Adds an intermediate abstraction that is initially resident-only.
- Phase D still requires a later format bump and migration path.
- Append-only pages introduce compaction and tombstone accounting work.

## References

- `design/2026-04-22.ENGINE-MEMORY-PLAN.md`
- `design/2026-04-23.ENGINE-MEMORY-REMAINING-WORK.md`
- `design/adr/0143-on-disk-row-scan-executor.md`
- `design/adr/0144-persistent-primary-key-index.md`
- `crates/decentdb/src/exec/mod.rs`
