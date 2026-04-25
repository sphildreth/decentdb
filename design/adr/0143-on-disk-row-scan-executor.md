# On-Disk Row-Scan Executor (eliminate full-table in-memory materialization)
**Date:** 2026-04-23
**Status:** Accepted — **Phase A done, Phase B shipped with default-on deferred materialization (2026-04-23 follow-up)**; Phases C/D/E remaining

### Decision

Eliminate the executor's reliance on full-table in-memory `Vec<StoredRow>`
materialization. Replace `EngineRuntime::tables: BTreeMap<String,
Arc<TableData>>` with an iterator-based row-source that streams rows from
on-disk pages through the page cache, so resident memory tracks
`cache_size_mb` and the executor's working set, not total row count.

### Background — how E1 was diagnosed

The Engine Memory Plan ([`design/2026-04-25.ENGINE-MEMORY-WORK.md`](../2026-04-25.ENGINE-MEMORY-WORK.md))
slice E1 set out to explain why a re-opened 36 MB DB consumed **1 144 MB
RSS** before any user query ran. After landing Phase 1 (auto-checkpoint
trigger, post-checkpoint `malloc_trim`, mimalloc opt-in, pooled checkpoint
scratch) the WAL retention was confirmed bounded (`wal_versions = 0`) yet
RSS at re-open was unchanged. Reading
[`crates/decentdb/src/exec/mod.rs`](../../crates/decentdb/src/exec/mod.rs)
revealed the architectural cause:

```rust
// exec/mod.rs around line 470
pub(crate) struct EngineRuntime {
    pub(crate) tables: Arc<BTreeMap<String, Arc<TableData>>>,
    // ...
}

// exec/mod.rs around line 130
pub(crate) struct TableData {
    pub(crate) rows: Vec<StoredRow>,   // every row, every table, in RAM
}

pub(crate) struct StoredRow {
    pub(crate) row_id: i64,
    pub(crate) values: Vec<Value>,
}
```

`Db::open` calls `EngineRuntime::load_from_storage`, which in turn calls
`materialize_deferred_tables_with_store(...)` and `rebuild_indexes(...)`.
Both walk the on-disk overflow chain for every persisted table, decode the
entire payload into `Vec<StoredRow>`, and insert it into `tables`. The
disk format is therefore a *checkpoint of an in-memory snapshot*, not a
queryable on-disk row layout.

### Implications

- **Resident memory ≈ Σ(rows × value-bytes)**, *not* `cache_size_mb`.
- **`cache_size_mb` controls the page cache**, which is used only for the
  serialized payload pages on the path between disk and `TableData` —
  irrelevant once `TableData` is built.
- **Re-open is full materialization**: opening a 5 M-row DB allocates
  ~1 GB no matter how small the user's working set is.
- **WAL bounding (M1) and allocator changes (M3, ADR 0142) shave only
  the transient slope** — the steady-state floor is set by `TableData`.
- **Concurrent readers each fork via `Arc<TableData>` clone-on-write** —
  for any table whose row vector is `Arc::make_mut`'d during a write
  transaction, two copies live in RAM.

### Why this dominates Phases 1–4 of the memory plan

Measured at 5 M rows, ~80 B per row:

| component | residency at end-of-load |
|---|---:|
| `tables` `Vec<StoredRow>` (E1, this ADR) | **~700 MB** |
| `cached_payloads` LRU (`config.cached_payloads_max_entries`) | up to ~50 MB |
| Page cache (`cache_size_mb`) | bounded at 4 MB |
| WAL index after auto-checkpoint (M1 active) | < 16 MB |
| Allocator overhead (mitigated by M2 / M3) | ~30–80 MB |
| **total observed** | **~936 MB** |

`tables` alone is ~75 % of the steady-state memory. Until E1 is fixed,
ADRs 0140 (`WalVersion` discriminated), 0141 (paged on-disk WAL index),
and 0142 (per-engine allocator) attack the remaining 25 %.

### Phased implementation

This ADR is the umbrella for a multi-PR effort. It is **not** all
landing in one slice. The phases below replace `tables` incrementally
without breaking SQL semantics.

#### Phase A — instrumentation (no behaviour change)

- Expose per-table residency in `Db::inspect_storage_state_json`:
  `tables_in_memory_bytes`, `rows_in_memory_count`, per-table breakdown.
- Add a `decentdb-memory-probe` integration test that asserts
  `tables_in_memory_bytes / db_file_bytes < 1.5` once Phases B + C land
  (today's ratio is ~25:1).

#### Phase B — bounded row cache, on-demand load

- Convert `EngineRuntime::tables` from "always fully resident" to "lazy
  range-loaded". Introduce `TableRowSource` enum:
  - `Resident(Arc<TableData>)` — current behaviour, used for small tables
    below a configurable threshold (`DbConfig::table_resident_max_rows`,
    default 10_000).
  - `Paged { manifest: TablePageManifest }` — rows live on-disk; reads
    take a `SnapshotPageStore` and a row-id range and stream rows.
- Update the executor's table-scan path to use `TableRowSource::iter()`
  instead of `&tables[name].rows`. Sequential scans become page-streamed.
- Update single-row lookups (`row_index_by_id`) to use a row-id → page
  index that lives on disk (see Phase C).

#### Phase C — index-on-disk

- Persist the primary-key index (today rebuilt in `rebuild_indexes`) as a
  B+Tree on disk so that `WHERE id = ?` does not require materializing
  the row vector.
- Expand to secondary indexes; the existing `rebuild_indexes` path
  becomes a recovery/repair tool, not the steady-state code path.

#### Phase D — write path

- Rewrite the write path (`persist_to_db`, `append_only_dirty_tables`)
  to apply per-row diffs to the on-disk B+Tree rather than serializing
  the entire `TableData` to overflow chains on each commit.
- This unlocks the mid-term goal of single-row update cost being O(log n)
  in DB pages, not O(table size) in serialized bytes.

#### Phase E — remove `tables`

- Once Phases B + C + D ship, `EngineRuntime::tables` is reduced to a
  cache of small-resident tables and DROP TABLE state. Deprecate the
  `materialize_deferred_tables_with_store` path entirely.

### Acceptance

- **Phase A** complete when the new metric appears in the JSON snapshot. ✅ **Done (2026-04-23)** — `tables_in_memory_bytes`, `rows_in_memory_count`, `loaded_table_count`, `deferred_table_count` are now in `Db::inspect_storage_state_json()`. Test: `db::tests::inspect_storage_state_json_reports_table_memory_totals`.
- **Phase B** complete when `DbConfig::defer_table_materialization` skips eager materialize at open by default and concurrent checkpoints cannot invalidate first-use overflow reads. ✅ **Done (2026-04-23 follow-up)** — `load_from_storage` honors the flag, the read paths target only referenced tables when safe, and snapshot pinning now keeps the persisted-table manifest read and overflow payload reads on one WAL snapshot. Tests: `db::tests::default_defer_table_materialization_skips_eager_load_at_open`, `db::tests::per_table_load_skips_large_table`, `engine_lifecycle_tests::concurrent_writer_reader_overflow_consistency_with_deferred_materialization`.
- **Phase C** complete when re-opening the 5 M-row probe DB consumes < 200 MB RSS before any query runs *and* point lookups against a huge table avoid full-table materialization. ⏳ **Remaining** — requires persisted on-disk indexing and row-source work so large-table point lookups no longer force a full `Vec<StoredRow>`.
- **Phase D** complete when peak RSS during a 10 M-row load with `cache_size_mb=64` stays below 1 GB (today: 2.6+ GB). ⏳ **Remaining** — requires new on-disk row format.

### Risks

- **SQL semantics**: any change to row scanning has a wide blast radius
  on the executor; full pgvector-style query coverage required before
  cutting over.
- **Compatibility**: the on-disk row format must remain readable by older
  installations. Per `decentdb-migrate`, a forward migration is required
  (and a read-only back-migrator, per ADR 0131).
- **Performance**: a poorly tuned page-streamed scan can be 10× slower
  than the current `Vec<StoredRow>` scan. Prefetching and sequential
  cache hints are mandatory for Phase B to ship.

### Out of Scope

- The CLI / bindings ABI is unchanged. ABI consumers see the same
  `ddb_*` calls and the same SQL behaviour.
- WAL format is unchanged. ADR 0140 / 0141 still apply on top of this
  ADR — they reduce the *secondary* memory components.

### References

- design/2026-04-25.ENGINE-MEMORY-WORK.md (E1 — Open-path RSS investigation)
- design/adr/0136-chunked-row-storage-for-coarse-grained-cow.md
- design/adr/0140-walversion-discriminated-payload.md
- design/adr/0141-paged-on-disk-wal-index.md
- design/adr/0142-per-engine-allocator.md
- crates/decentdb/src/exec/mod.rs::load_from_storage
- crates/decentdb/src/exec/mod.rs::materialize_deferred_tables_with_store
