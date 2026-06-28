# DELETE_BATCH: cascade / bulk delete performance on resident-storage profiles

**Status:** Living design note (Phase A/B/C landed for the resident
`embedded_fast` cascade target)
**Date:** 2026-06-26
**Owners:** storage / executor
**Related:** ADR 0196, ADR 0199, ADR 0200, ADR 0195, ADR 0143, ADR 0145, ADR 0131

This document explains, in depth, why `MovieDB scratch Cascade delete batch` was
slow on DecentDB, what was measured, what the format-14 change (ADR 0200)
fixed, and how the follow-on in-memory and sparse-persist phases made DecentDB
beat SQLite on this workload without weakening durability.

It is intentionally detailed so future maintainers can understand the design
constraints and avoid regressing the resident delete path.

---

## 1. The workload under test

Source: `bindings/python/benchmarks/bench_complex.py`, `--workload movie
--movie-scale scratch`, driven by `scripts/benchmark_runner.py --profile full`.

Schema (`setup_movie_schema`), all child FKs `ON DELETE CASCADE`:

| table | rows (scratch) | FK → Movies | child index on MovieId |
|---|---:|---|---|
| `Movies` | 50,000 | (parent) | PK on `Id` |
| `Roles` | 250,000 | `MovieId`, `PersonId` | `ix_roles_movie` (explicit) |
| `Reviews` | 500,000 | `MovieId` | `ix_reviews_movie` (explicit) |
| `MovieTags` | 150,000 | `MovieId`, `TagId` | PK `(MovieId, TagId)` leading col |
| `Watchlist` | 100,000 | `MovieId` | auto FK index (`fk_...`) |

The benchmarked operation (`run_deletes`):

```sql
BEGIN;
-- repeated 10 times, one execute() per parent id:
DELETE FROM Movies WHERE Id = CAST(? AS UUID);
COMMIT;
```

So: **10 single-row parent deletes inside one explicit transaction**, each
cascading to ~5 roles, ~10 reviews, ~3 movie tags, ~2 watchlist rows. Total
deleted ≈ 10 parents + ~210 children, scattered across the child tables by
random parent id.

Profile: the runner passes the `embedded_fast` options string, i.e.
`paged_row_storage=false` (resident single-payload storage), `cache_size=64MB`,
`retain_paged_row_sources_after_commit=true`, `wal_autocheckpoint=0`,
`process_coordination=single_process_unsafe`,
`wal_sync_mode=async_commit:10`. Durability is **not** weakened by this work and
must stay that way (ADR 0037/0184).

### Fresh baseline (machine-local, `.tmp/cascade-opt-20260625-210600/`)

| Engine | `Cascade delete batch` |
|---|---:|
| SQLite | ~0.118–0.142 s |
| DecentDB (pre-change) | ~2.33–2.41 s |
| Gap | ~17–20x |

---

## 2. Why it is slow — measured root causes

Two independent, additive costs dominate. Both were measured with temporary
phase timers and a per-table dirty-byte counter (since removed); the numbers
below are representative of the pre-change build.

### 2.1 Commit re-encode + WAL (~1.46 s of ~2.39 s)

A resident table is persisted as a **single contiguous overflow payload**:

```
[magic "DDBTBL01"][physical_row_count u32]{ [row_id i64][row_body_len u32][row_body…] }*
```

The delete-only persist path used `splice_deleted_rows_payload_in_place`
(`crates/decentdb/src/exec/mod.rs`), which removes a row by **shifting every
byte after it**, so the dirty byte range is `[first_deleted_byte-1 .. end]`.
Because cascade-deleted child rows are scattered, the first deletion lands near
the start of each table, so almost the entire payload is rewritten:

| table | rows | payload | dirty bytes | dirty % |
|---|---:|---:|---:|---:|
| reviews | 499,891 | 135,799,929 | 135,014,272 | 99.4% |
| roles | 249,950 | 24,096,166 | 23,999,747 | 99.6% |
| watchlist | 99,979 | 9,398,038 | 8,830,741 | 94.0% |
| movietags | 149,974 | 8,548,530 | 8,226,587 | 96.2% |
| movies (parent) | 49,990 | 10,032,299 | 9,832,642 | 98.0% |

Deleting ~220 rows rewrote **~185 MB**. This split as `persist_to_db` ~463 ms
(re-encode/splice) + WAL frame write ~999 ms. `persist_to_db` alone already
exceeds SQLite's entire runtime.

### 2.2 In-statement copy-on-write clone (~0.5–0.8 s)

DecentDB uses snapshot isolation. A shared transaction
(`Db::begin_transaction` → `build_sql_txn_state`) clones the `EngineRuntime`,
which only `Arc::clone`s the `tables: Arc<BTreeMap<String, TableRowSource>>`
map; the per-table `Arc<TableData>` is shared with the committed base.

The cascade delete path is:

```
execute_prepared_simple_delete
  └─ apply_parent_delete_actions_rows           (per parent row)
       ├─ matching_foreign_key_children_for_parent_rows   (FK index lookup; cheap)
       ├─ incremental_delete_indexes                      (runtime btree delete)
       └─ apply_row_changes_to_table_row_source
            └─ apply_row_changes_to_resident_table_data
                 └─ TableData::retain_rows                (full O(n) pass)
```

The first delete that touches each large child table calls `table_data_mut` →
`entry_table_data_mut` → `resident_data_mut` → `Arc::make_mut(Arc<TableData>)`,
which **deep-clones the entire `Vec<StoredRow>`** (~317 ms for the four child
tables: ~1 M rows with `Vec<Value>`/`String` payloads), and the first runtime
index mutation likewise clones the index btrees (~106 ms). Subsequent statements
re-run `retain_rows` (full O(n) passes), ~13 ms each.

`TableData::should_retain_rows_for_pure_delete` selects the full `retain_rows`
pass when `delete_count > 1 && table_rows >= 4096`, so each statement scans the
whole child table.

### 2.3 Net

```
in-statement (10 deletes)  ≈ 0.5–0.8 s   (COW clone + retain + index clone)
commit (one)               ≈ 1.46 s      (splice re-encode + WAL of ~185 MB)
                           ≈ 2.0–2.3 s total   (plus binding overhead)
```

ADR 0199's "batch the in-memory compaction" idea alone cannot win: it removes
the repeated `retain_rows` passes (~0.12 s) but not the one-time clone (~0.4 s)
and does nothing for the ~1.46 s commit. `persist_to_db` is the binding
constraint and it is a **storage-format** problem.

---

## 3. Phase A (landed): persisted in-place delete tombstones (format 14)

ADR 0200. The goal of Phase A is to stop a delete from rewriting the whole
payload.

### 3.1 On-disk format

Framing is unchanged. New rule: a row slot whose `row_body_len` field has its
**high bit set** (`TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG = 1 << 31`) is a logically
deleted (tombstoned) slot. The low 31 bits keep the real body length so the slot
is still traversable; the body bytes are retained as dead space. A delete then
patches **4 bytes per row** (the length field) instead of shifting the tail.

Why the length-field high bit rather than a sentinel `row_id`:

- `row_id` can be any `i64` for `INTEGER PRIMARY KEY` rowid-alias tables
  (including negative / `i64::MIN`), so no row-id value is a safe sentinel.
- The length field is engine-controlled; encoded bodies are far below 2³¹ bytes.
  `encode_table_payload` now asserts `row_body_len < 2³¹`.
- **Fail-loud:** a reader that forgot to mask the flag reads an oversized length
  and errors (`read_slice` underflow) rather than returning wrong data. This
  bounded the blast radius of touching every reader.

`physical_row_count` (header) keeps counting physical slots; the **live** count
is derived by skipping tombstones. A full re-encode (`encode_table_payload`)
drops tombstones, which is how compaction reclaims dead space.

### 3.2 Readers updated to skip tombstones

`split_table_payload_row_len(raw) -> (is_tombstone, len)` was added and applied
to every table-payload row-stream reader in `exec/mod.rs`:

- `decode_table_payload` (test) / `decode_table_payload_rows`
- `visit_table_payload_rows_from_bytes` / `_from_pointer`
- `visit_table_payload_projected_values_from_bytes` / `_from_pointer`
- `visit_table_payload_int64_column_from_bytes` / `_from_pointer`
- `scan_table_payload_row_ids`
- `read_row_from_table_payload_by_id`, `decode_row_by_locator_from_payload`
- `build_row_locator_entries`, `append_paged_row_locator_entries`,
  `append_cached_paged_row_locators`, `decode_compressed_table_payload_lookup_entry`
- the paged manifest base/overlay scanners (`from_chunks`,
  `table_page_entries_for_chunk`, `row_bytes_from_tombstoned_base`)

The legacy `decode_runtime_payload`/`decode_manifest_payload` are left untouched
(legacy files never contain tombstones).

`read_table_payload_live_row_count_from_bytes` was added (scans + skips
tombstones) and is used by `read_persisted_table_row_count` so `COUNT(*)`
metadata reflects live rows. `read_table_payload_row_count_from_bytes` still
returns the physical header count for the append-guard and paged accounting.

### 3.3 Persist path

The resident delete-only branch of `persist_to_db` now:

1. reads the previous payload (cached or via `read_overflow`),
2. computes `physical` (header) and `live` (`data.row_count()`),
3. if `live > 0 && dead_after <= live` (not over-fragmented), calls
   `tombstone_deleted_rows_payload_in_place(payload, deleted_row_ids)` which
   scans the slots and sets the tombstone flag on each deleted row's length
   field, returning the small set of 4-byte dirty ranges;
4. otherwise (over-fragmented, or a deleted id is missing) falls back to the
   compacting splice / full re-encode (which reclaims dead slots).

The append path (`append_table_payload`) now full-re-encodes when the previous
payload has tombstones (`previous_physical != data.row_count() - append_count`),
because the append fast path assumes physical-slot/live parity.

### 3.4 Eligibility gate (important)

The tombstone path is **gated to `paged_row_storage = false`** (the
`embedded_fast` / `tuned_durable` resident profiles). Under the default *paged*
profile, a small table that currently persists as a resident single payload can
be promoted to a paged manifest by a later write or checkpoint; mixing in-place
tombstones with that promotion/manifest-template machinery is unsafe and was
observed to produce a stale paged manifest pointer (a real durability bug found
during implementation). Resident profiles never promote, so the gate makes the
change provably safe there. This matches the benchmark profile.

Additional fallbacks (inherit ADR 0199/0196): DELETE triggers with
order-sensitive semantics, sync/reactive capture needing materialized rows,
branch/snapshot states, savepoints, and paged-manifest tables (which already
tombstone per chunk).

### 3.5 Migration (ADR 0131)

`DB_FORMAT_VERSION = 14`. `decentdb-migrate` gained a v13→14 path: because v13
payloads contain no tombstone slots and are read unchanged by the v14 engine,
the migration is a header-version patch + WAL-sidecar carry-forward (same shape
as v10/v11). A v13 round-trip test was added.

### 3.6 Result and validation

- All 3048 `decentdb` tests pass; `cargo fmt --check` and
  `cargo clippy -p decentdb --all-targets --all-features` are clean.
- New tests: tombstone payload round-trip through readers, persist+reopen
  durability (incl. a second delete over an already-tombstoned payload),
  rollback, and the previously-failing `persist_update_and_delete` /
  `persist_delete_and_reinsert`.
- Benchmark (`--workload movie --movie-scale scratch`):
  **DecentDB 2.39 s → 1.36 s (1.76x)**; the delete-commit WAL dropped from
  ~1 s to ~1 ms.

**Phase A alone did not beat SQLite (~0.14 s).** Re-profiling the 1.36 s showed:

```
in-statement (COW clone + retains)   ≈ 0.8 s   (unchanged by Phase A)
commit persist_to_db                 ≈ 0.5 s   (now read+crc of full child payloads, tiny WAL)
```

The WAL volume problem was solved; two CPU/IO costs remained and drove Phases B
and C.

---

## 4. Phase B/C implementation

The follow-on work kept the ADR 0200 file-format decision intact: no additional
format bump, WAL semantics change, checksum representation change, or benchmark
profile weakening was required. The implementation stays gated to resident
single-payload tables under `paged_row_storage = false`. The Phase C sparse
overflow patch path additionally requires `persistent_pk_index = false`; when a
persistent PK index is enabled, persist falls back to the Phase A payload path
so PK-index rebuild/reuse remains conservative.

### 4.1 Phase B: logical in-memory tombstones

`TableData` now stores resident rows as shared storage plus a transient tombstone
set:

```rust
pub(crate) struct TableData {
    rows: Arc<Vec<StoredRow>>,
    tombstoned_row_ids: BTreeSet<i64>,
    cached_heap_bytes: usize,
}
```

Deletes mark row ids in `tombstoned_row_ids` instead of compacting the resident
row vector. Cloning a transaction-local `TableData` now clones the row `Arc` and
the small tombstone set, avoiding the previous deep clone of up to ~1M child
rows during the cascade batch.

Resident query and mutation entry points were routed through live-row helpers:
`row_count`, `visible_rows`, `row_by_id`, `row_index_by_id`,
`row_ids_in_range`, projected scans, visible table row sources, and uniqueness
maintenance skip tombstoned rows. Inserts/reinserts clear matching tombstones.
Compacting paths still use the physical row vector when they intentionally
re-encode live rows and drop dead slots.

The lower-risk index strategy was chosen: runtime indexes are still maintained
on deletes, while uniqueness cleanup also removes stale tombstoned unique-index
entries when needed. This preserves index-only behavior without relying on
stale index filtering as the primary correctness mechanism.

### 4.2 Phase C: sparse resident overflow patching

The commit-time problem after Phase A was no longer WAL volume; it was finding,
reading, and checksumming huge resident overflow payloads to flip a few
tombstone bits. Phase C added the resident locator and sparse patch path:

- resident tombstone locators map `row_id -> row_body_len byte offset`;
- `prepare_resident_payload_offset_caches` builds overflow chain caches at
  checkpoint boundaries for resident single-payload tables;
- runtime cloning preserves overflow chain caches keyed by persisted overflow
  pointers, so an explicit transaction can reuse checkpoint-prepared offsets;
- `tombstone_deleted_rows_overflow_by_locator` validates each row id and length
  field with small page reads, then patches only the 4-byte row-length flags;
- `rewrite_overflow_cached_with_sparse_byte_patches` rewrites only touched
  overflow pages;
- `crc32c_patch_bytes` updates the existing whole-payload CRC32C from old/new
  byte ranges, avoiding a full payload checksum pass.

`Db::execute_pragma_wal_checkpoint` also prepares these resident offset caches,
because the Python MovieDB benchmark performs an explicit checkpoint before the
delete workload. Without that preparation, the delete transaction could still
spend most of the win rebuilding the overflow chain cache on the hot path.

If any locator, page-cache, row-id, tombstone, persistent-PK-index, or
fragmentation precondition fails, persist falls back to the Phase A full-payload
tombstone path and then to the existing compacting/full re-encode paths.

### 4.3 Combined result

```
in-statement   ~0.8 s → ~0.03 s   (Phase B)
commit         ~0.5 s → ~0.03 s   (Phase C)
total          ~1.36 s → < 0.1 s  (beats SQLite ~0.14 s)
```

Final targeted validation:

```bash
cargo build -p decentdb --release
python3 bindings/python/benchmarks/bench_complex.py \
  --workload movie \
  --engine all \
  --engine-order decentdb-first \
  --movie-scale scratch \
  --decentdb-options profile=embedded_fast \
  --sqlite-profile wal_normal \
  --db-prefix .tmp/movie-cascade-overlay \
  --json-output .tmp/movie-cascade-overlay.json \
  --strict-equivalence
```

Result:

| Engine | `MovieDB Cascade delete batch` |
|---|---:|
| DecentDB | 0.088936 s |
| SQLite | 0.133104 s |
| Winner | DecentDB, 1.497x faster/lower |

`MovieDB result equivalence: ok`.

---

## 5. Correctness requirements

The optimized path must continue to satisfy the same semantic requirements as
the materializing fallback:

- Repeated parent deletes in one transaction remove all matching child rows and
  are visible to later statements in the same transaction (ADR 0199).
- Rollback restores parents and children; savepoint rollback restores the
  per-savepoint tombstone set.
- Duplicate / missing parent ids produce the same affected-row counts as the
  fallback path.
- RESTRICT / NO ACTION checks run before any destructive effect becomes visible.
- Child FK indexes (and all indexes) are correct after commit and after
  compaction; no index-only read returns a tombstoned row.
- Branch/snapshot visibility is preserved or the operation falls back.
- Crash/recovery: tombstoned slots are ordinary committed payload bytes; reopen
  + WAL replay yields the same live set. Add crash-injection coverage for a
  delete that tombstones then a partial checkpoint.
- `COUNT(*)`, aggregates, joins, point lookups, range scans, and uniqueness
  checks all reflect live rows.
- No durability downgrade: `WalSyncMode::Full` remains the durable default.

## 6. Testing strategy

- Unit: `split_table_payload_row_len`, `tombstone_deleted_rows_payload_in_place`
  (applied / missing-id fallback / over-fragmentation), live-count scanner.
- Differential: optimized tombstone delete vs the materialized splice path on
  random delete sets, comparing full table scans, point lookups, index lookups,
  aggregates, and `COUNT(*)`.
- Persistence: insert → delete → reopen; delete → delete (already-tombstoned) →
  reopen; delete → insert/reinsert → reopen; update → delete → reopen.
- Transaction: in-transaction visibility across statements; rollback; nested
  savepoint rollback of tombstones.
- FK cascade: RESTRICT ordering, multi-level cascade, set-null where supported,
  trigger/sync fallback.
- Crash/recovery and branch/snapshot fallback.
- Benchmark: targeted MovieDB validation must keep
  `MovieDB Cascade delete batch` below SQLite with `equivalence = ok`. Broader
  `python scripts/benchmark_runner.py --profile full` runs should confirm the
  row appears under `DecentDB better at:` and catch unrelated regressions.

## 7. Risks and mitigations

| Risk | Mitigation |
|---|---|
| A read path forgets to skip a tombstone → returns deleted row | Route query reads through `VisibleTableRowSource`; differential tests; gate to resident profile; tombstone set usually empty (no-op fast path) |
| Index-only reads return stale ids | Keep incremental index maintenance on delete; uniqueness cleanup removes tombstoned unique-index entries before insert/reinsert |
| Paged-promotion/manifest interaction (the Phase A bug) | Hard gate `paged_row_storage == false`; never tombstone a table that can be promoted |
| Sparse path accidentally bypasses persistent PK-index correctness | Sparse overflow patching is gated to `persistent_pk_index == false`; persistent-PK-index profiles use the conservative payload path |
| Dead-space growth | Compaction triggers (vacuum / fragmentation / checkpoint) |
| Checksum optimization silently diverges | `crc32c_patch_bytes` is tested against full rehash and composes multiple patches; sparse persist falls back on invalid preconditions |
| Savepoint rollback of tombstones | `tombstones` lives in `TableData`, already cloned per savepoint |
| Memory: tombstoned rows stay resident | Bounded by compaction; tombstones are transient between delete and flush |

## 8. Alternatives considered (and rejected)

- **Switch `embedded_fast` to paged/chunked storage** so deletes are chunk-local:
  rejected by ADR 0195/0199 — abandons the resident read fast path.
- **In-memory cascade batching only (ADR 0199)**: necessary but insufficient;
  cannot touch the commit cost and leaves the one-time clone.
- **Persisted tombstone sidecar** (separate deleted-id structure): adds a second
  persistent structure and a second read merge; the in-place length-bit reuses
  the existing framing and decoder.
- **Keep splicing**: the measured root cause; rewrites ~99% of each payload.

## 9. Status / next steps checklist

- [x] ADR 0200 + format 14 + `decentdb-migrate` v13 path + tests
- [x] Phase A: persisted in-place tombstones, readers, persist gate, validation
      (3048 tests green, clippy/fmt clean, 1.76x on the target)
- [x] Phase B: `TableData` `Arc<Vec>` + tombstone set, tombstone-aware reads,
      deferred compaction, delete/cascade wiring, savepoint rollback, tests
- [x] Phase C: locator-driven page-targeted patching + CRC32C byte-patch update
- [x] Targeted MovieDB validation:
      `.tmp/movie-cascade-overlay.json`, DecentDB 0.088936 s vs SQLite 0.133104 s
- [x] Final broad `scripts/benchmark_runner.py --profile full --report-only`
      confirmation: `.tmp/perf-validate/delete-batch-overlay/`, MovieDB scratch
      cascade 0.078941 s vs SQLite 0.115281 s

## 10. Evidence / references

- Measured baseline, phase timing, dirty-byte table:
  `.tmp/cascade-opt-20260625-210600/FINDINGS.md`
- Phase A benchmark logs: `.tmp/cascade-opt-20260625-210600/phaseA/`
- Final targeted MovieDB validation: `.tmp/movie-cascade-overlay.json`
- Final benchmark-runner validation:
  `.tmp/perf-validate/delete-batch-overlay/benchmark_6_moviedb_scratch_scale.log`
- `design/adr/0200-resident-table-delete-tombstones-and-format-14.md`
- `design/adr/0199-transaction-local-cascade-delete-batching.md`
- `design/adr/0196-persisted-dml-and-cascade-delete-performance.md`
- `design/adr/0195-embedded-fast-profile-and-resident-read-fast-path.md`
- Code: `crates/decentdb/src/exec/mod.rs` (`persist_to_db`,
  `tombstone_deleted_rows_payload_in_place`, `split_table_payload_row_len`,
  `tombstone_deleted_rows_overflow_by_locator`,
  `prepare_resident_payload_offset_caches`, the `visit_table_payload_*`
  readers), `crates/decentdb/src/exec/dml.rs`
  (`execute_prepared_simple_delete`, `apply_parent_delete_actions_rows`,
  tombstone-aware uniqueness cleanup),
  `crates/decentdb/src/record/overflow.rs`
  (`rewrite_overflow_cached_with_sparse_byte_patches`),
  `crates/decentdb/src/storage/checksum.rs` (`crc32c_patch_bytes`),
  `crates/decentdb/src/db.rs` (checkpoint cache preparation),
  `crates/decentdb/src/storage/header.rs` (`DB_FORMAT_VERSION`),
  `crates/decentdb-migrate/src/main.rs` (`migrate_v13_file`).
