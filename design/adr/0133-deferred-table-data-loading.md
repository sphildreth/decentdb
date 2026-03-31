## ADR-0133: Deferred Table Data Loading
**Date:** 2026-03-31
**Status:** Proposed

### Decision

Defer the loading of table row data from `Db::open()` to first statement
execution so that database open latency is proportional to schema size rather
than total row count.

1. During `decode_manifest_payload`, read table schemas and overflow pointers
   but **skip** `read_overflow` and `decode_table_payload` for each table's row
   data. Store the overflow pointer and checksum in `PersistedTableState` so
   the data can be loaded later.
2. Track unloaded tables in a `deferred_tables` set on `EngineRuntime`.
3. **Skip** `rebuild_indexes` at the end of `load_from_storage`. Index
   construction requires row data, so it must be deferred alongside the data.
4. Before the first statement execution, load all deferred table data and
   rebuild indexes in a single pass. The `Db` layer acquires a write lock on
   the engine, constructs a `SnapshotPageStore`, and calls
   `EngineRuntime::load_deferred_tables`.
5. After loading completes, execution proceeds as before: all rows are in
   memory, indexes are built, and the runtime is fully populated.
6. Correctness invariant: no query or DML path may observe a `TableData` for a
   table that is still in the deferred set. All loading gates execute before any
   code that reads `self.tables`.

### Rationale

The `point_lookup_cold` benchmark measures end-to-end latency from `Db::open()`
through a batch of point lookups. Profiling shows that ~90% of the cold-open
cost is spent inside `decode_manifest_payload` eagerly reading and decoding
every table's overflow chain, even though the subsequent queries may only touch
one table.

For a 100K-row single-table database the eager load takes ~45 ms of a ~50 ms
total cold batch. The point lookups themselves complete in ~1–2 µs each because
data is already in memory.

Deferring table data loading separates the fast schema-only open from the
heavier data materialization:

- `Db::open()` reads only the catalog root page and manifest (~1–3 ms).
- Data loading happens once, on first use, amortized over the first statement.

For multi-table databases the benefit is larger: tables the application never
queries in a given session are never loaded at all.

This change does **not** introduce per-row or per-page lazy loading. All rows
for a table are still loaded in bulk when the table is first accessed. That
keeps the in-memory execution model, index rebuild strategy, and persistence
path unchanged.

### Alternatives Considered

#### Per-table lazy loading (load only the accessed table)

Deferred for a follow-up. Per-table loading requires threading a `PageStore`
reference through `table_data_in_scope` or converting read paths from `&self` to
`&mut self`. The simpler bulk-deferred approach captures most of the cold-open
benefit without refactoring the read-path signatures.

#### Per-row / per-page on-demand loading

Rejected for this slice. Moving from an in-memory table model to a page-level
access model would change the execution architecture, query evaluation, and
persistence contract. It is a separate, larger project if ever pursued.

#### Persist indexes to disk and skip rebuild

Deferred. Persisting runtime indexes would remove the `rebuild_indexes` cost on
load, but requires a new on-disk index format and invalidation strategy. It
complements deferred loading and could be layered on top later.

#### Load tables during `prepare()` instead of `execute()`

Considered but not adopted as the primary mechanism. `prepare()` validates
schema but does not need row data. Loading during prepare would move cost
without reducing it, and would couple parsing to storage I/O.

### Trade-offs

- Positive: `Db::open()` latency drops from ~45 ms to ~3 ms for a 100K-row
  database.
- Positive: multi-table databases load only tables that are actually queried.
- Positive: the in-memory execution model, persistence path, and index strategy
  remain unchanged after loading completes.
- Positive: `EngineRuntime::clone()` of a deferred runtime is cheap (no row
  data to clone).
- Negative: the first statement execution pays the full loading cost. Total
  open-to-first-result latency is similar for single-table workloads.
- Negative: adds a deferred-state check on every execution entry point
  (fast-path: one `is_empty()` check on a `BTreeSet`).
- Negative: `EngineRuntime` gains a new invariant: code that accesses
  `self.tables` must ensure the table is not in the deferred set. This is
  enforced at the `Db` layer, not inside `EngineRuntime` methods.

### References

- `design/PRD.md`
- `design/SPEC.md`
- `design/adr/0122-phase0-table-manifest-persistence.md`
- `crates/decentdb/src/exec/mod.rs` — `decode_manifest_payload`, `load_from_storage`
- `crates/decentdb/src/db.rs` — `Db::execute_read_statement`, `Db::engine_snapshot`
