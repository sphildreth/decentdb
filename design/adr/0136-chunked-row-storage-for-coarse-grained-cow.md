# Chunked Row Storage for Finer-Grained Copy-on-Write

**Date:** 2026-04-20
**Status:** Proposed

### Context

Each `EngineRuntime` holds tables as
`Arc<BTreeMap<String, Arc<TableData>>>` where `TableData.rows` is a
`Vec<StoredRow>` and each `StoredRow` is a `Vec<Value>`. The runtime is
shared with read-only snapshots and with active write transactions through
`Arc` cloning at `Db::build_sql_txn_state`.

ADR 0134 (per-entry table `Arc`) already eliminated cross-table
contamination: the first write in a transaction now clones only the
*targeted* table's `TableData`, not every table in the database. Step 1 of
the follow-up plan (per-entry index `Arc`) extends the same pattern to the
`indexes` map.

The remaining bottleneck is the per-table row vector itself. The first
mutation on table `T` in a transaction calls `Arc::make_mut` on
`Arc<TableData>`, which clones the entire `Vec<StoredRow>` (and every
`StoredRow`'s inner `Vec<Value>`) before the mutation can proceed. For a
1 000-row table this costs roughly 70 µs per first-write transaction in
the Python `bench_complex` workload; for a 100 000-row table the same
single-row UPDATE pays a 100 000-row deep clone before doing any useful
work. SQLite's UPDATE p50 sits at ~3.5 µs because it mutates pages in
place (with WAL for rollback) and never deep-clones.

### Goals

- Reduce the per-transaction cloning cost from O(rows in mutated table) to
  O(rows in mutated chunk) on the first write per transaction.
- Preserve current read snapshot isolation: existing readers must continue
  to see the version of the rows they were handed.
- Preserve current rollback semantics: dropping the writer's view must be
  enough to abandon all uncommitted changes.
- Keep iteration throughput on hot read paths (point lookup, range scan,
  sequential scan, FK probes) within a small constant factor of today's
  contiguous-`Vec` performance.
- Avoid changes to the on-disk format, the WAL format, the C ABI, or the
  catalog schema.

### Non-Goals

- True in-place mutation with an undo journal (that would be Approach D in
  the design discussion and is a larger change to the transaction model).
- A persistent / immutable data structure such as `im::Vector` or a hand-
  rolled RRB-tree (Approach B). Pulling in such a structure adds a
  dependency or a meaningful amount of new code, and changes iteration
  cost characteristics across every read path.
- A mutation overlay that layers a dirty-row map on top of an immutable
  base (Approach C). The rubber-duck review during the
  per-table-Arc work flagged that overlays would have to be visible to
  reads, FK checks, triggers, savepoints, and rollback - a much larger
  semantic change than this proposal.

### Decision

Replace `TableData.rows: Vec<StoredRow>` with a chunked representation:

```rust
const ROW_CHUNK_LEN: usize = 256; // tunable, fixed at compile time

pub(crate) struct TableData {
    pub(crate) chunks: Vec<Arc<RowChunk>>,
    pub(crate) len: usize,
    // ...existing per-table fields (dirty bookkeeping, etc.) stay as-is
}

pub(crate) struct RowChunk {
    pub(crate) rows: Vec<StoredRow>, // length <= ROW_CHUNK_LEN
}
```

Mutation on a single row touches one chunk:

1. `Arc::make_mut(&mut chunks[chunk_idx])` clones at most `ROW_CHUNK_LEN`
   rows.
2. The row is mutated in place inside the cloned chunk.
3. Adjacent chunks remain shared with any concurrent reader.

Append/insert appends to the trailing chunk (cloning it via
`Arc::make_mut`) and pushes a new `Arc<RowChunk>` once the trailing chunk
is full.

Random-position deletes shrink the affected chunk; chunks are not
rebalanced eagerly. A periodic compaction step (only at the end of bulk
DDL operations and at checkpoint time) collapses sparse chunks to
preserve scan locality.

Iteration is implemented via a flat-iterator helper that yields
`&StoredRow` from each chunk in order, so existing callers that consume
`TableData.rows.iter()` migrate to a `TableData.iter()` method. The
helper also exposes `iter_with_rowid` and `get(row_idx)` to preserve the
small set of indexed-position accesses that exist today.

### Trade-offs

- **Memory overhead:** one additional `Arc` header per chunk (16 bytes)
  and one `Vec<Arc<RowChunk>>` outer vector. With `ROW_CHUNK_LEN = 256`
  the overhead is well under 0.1 % for tables that matter.
- **Iteration cost:** one extra pointer dereference per chunk boundary.
  Modern CPUs hide this through prefetch on sequential scans; we will
  validate with the `bench_complex` and Dart `console_complex`
  benchmarks. If the regression on cold scans exceeds ~5 % we will tune
  `ROW_CHUNK_LEN` (probably toward 1 024) before merging.
- **First-write cost:** drops from O(table rows) to O(`ROW_CHUNK_LEN`) -
  approximately 4×–400× depending on table size. The expected projection
  for the `bench_complex` 1 000-row UPDATE is ~70 µs → ~5 µs Rust-side,
  bringing the Python end-to-end UPDATE p50 within roughly 2× of SQLite.
- **Range deletes / TRUNCATE:** still O(rows) because every chunk needs
  to be rewritten or dropped, but TRUNCATE already takes that hit today.
- **Bulk INSERT:** stays roughly the same; the trailing chunk is cloned
  once per transaction and then mutated in place for subsequent rows in
  the same transaction.

### Compatibility Notes

- **On-disk format:** unchanged. `TableData` is an in-memory structure;
  persisted page payloads are unaffected.
- **WAL format:** unchanged.
- **C ABI:** unchanged. No public types reference `Vec<StoredRow>`.
- **Catalog / schema:** unchanged.
- **Concurrency model:** unchanged. The single-writer / multi-reader
  invariant is preserved because chunk-level `Arc::make_mut` produces the
  same observable effect as today's table-level `Arc::make_mut`, just on
  a smaller granularity.

### Implementation Plan

1. Introduce `RowChunk` and a `TableData::iter()` API behind the existing
   field name. Migrate internal consumers off direct `rows.iter()`,
   `rows.len()`, and `rows[idx]` access patterns.
2. Switch the storage to `Vec<Arc<RowChunk>>` and rewrite the mutation
   helpers (`entry_table_data_mut`, ALTER TABLE row rewrites, bulk load
   append, DELETE row removal, UPDATE row rewrite) to clone only the
   affected chunk.
3. Add per-chunk COW isolation tests (mutate one chunk; assert sibling
   chunks' `strong_count` is unchanged) alongside the existing per-table
   tests added in the previous refactor.
4. Run the Rust micro-probe and `bindings/python/benchmarks/bench_complex.py`
   to confirm the projected UPDATE/DELETE wins and to detect any read
   regression on Dart `console_complex`.
5. If iteration cost regresses, tune `ROW_CHUNK_LEN` upward and re-measure
   before merging.

### Out of Scope (Deferred)

- A row-id → chunk-index acceleration structure. Current code finds rows
  by linear scan within a table's `rows` vector; that loop becomes a
  two-level scan with chunking. If profiling shows the chunk-walk cost
  matters, a small `Vec<u64>` of `(first_row_id_in_chunk)` can be added
  later without changing the on-disk format.
- Page-aligned chunk sizing tied to `page_size`. We deliberately keep
  `ROW_CHUNK_LEN` as a fixed row count, decoupled from the disk page
  layout.
- True in-place mutation with undo logging (Approach D). That remains a
  potential future ADR if chunked COW does not close enough of the
  remaining gap to SQLite on UPDATE-heavy workloads.

### Validation Strategy

- Existing unit and integration tests must pass without modification
  beyond construction-site adaptation (`TableData { rows: vec![...] }`
  literal call sites become `TableData::from_rows(vec![...])`).
- New tests:
  - Per-chunk COW isolation (mutating chunk *i* leaves chunks *!= i*
    pointer-equal to the snapshot held by a reader).
  - Iteration order across chunk boundaries, including the case where
    one chunk is empty after a delete.
  - Round-trip persistence: serialize, reload, and verify the chunk
    layout reconstructs the same logical row sequence.
- Benchmarks: `bindings/python/benchmarks/bench_complex.py` UPDATE p50
  must drop materially (target < 30 µs); `bench_complex.py` point-lookup
  and range-scan p50 must not regress more than 5 % at the chosen
  `ROW_CHUNK_LEN`.

### Status Tracking

- This ADR is **Proposed**. Implementation is gated on explicit approval
  per AGENTS.md §7 ("major architectural shifts in planner, storage, or
  binding strategy"). Although the change is contained to in-memory
  layout, it touches the hot read path for every query and the mutation
  path for every DML statement, which is broad enough to warrant an ADR
  before implementation.
