# ADR 0195: Embedded-Fast Profile and Resident Read Fast Path for Autocommit Workloads

**Date:** 2026-06-18
**Status:** Accepted

## Context

DecentDB's default `DbConfig` ships with `defer_table_materialization = true`
and `paged_row_storage = true` (ADR 0143, ADR 0145). To bound resident memory
on long-lived handles, the engine re-defers resident paged tables immediately
after each autocommit write commit whenever
`retain_paged_row_sources_after_commit = false` (the default).

### The performance cliff this created

A fair side-by-side comparison against SQLite
(`decentdb-vs-sqlite` harness, see
`design/_archive/2026-06-PERF_TESTING_RESULTS.md`) measured autocommit write and
non-fast-path read throughput at medium scale (500 users / 100 projects /
10,000 issues / ~46,670 comments) using the default `balanced()` profile:

| Workload | SQLite | DecentDB (balanced) | Ratio |
|---|---:|---:|---:|
| single_row_insert | 34 µs | 5,036 µs | 147× |
| update_workload | 26 µs | 19,855 µs | 758× |
| delete_workload | 40 µs | 47,132 µs | 1,172× |
| mixed_read_write | 22 µs | 5,868 µs | 265× |
| filtered_query | 43 µs | 10,058 µs | 235× |
| aggregate_count_by_status | 715 µs | 9,052 µs | 12.7× |

Two root causes:

1. **Autocommit write reload cliff.** After every autocommit commit the
   engine dropped the just-written table's row source back to the deferred
   set (`redefer_all_persisted_paged_tables` on the commit path). The *next*
   autocommit statement on the same table reloaded the entire table from
   disk (`load_simple_write_row_sources_at_latest_snapshot` →
   `ensure_table_row_sources_loaded_at_snapshot`). Per-statement cost became
   O(table size), so throughput collapsed as tables grew.
2. **Per-statement read reload.** The non-fast-path autocommit read path
   (`execute_nontransaction_read_statement`) begins a WAL reader and
   materializes referenced tables on *every* statement, even when those
   tables were already resident from a prior same-handle write or load.

The same comparison showed DecentDB **winning** on `primary_key_lookup`
(4× faster) and `bulk_insert` (at scale) precisely because those paths
bypass row-source materialization or amortize the reload across one
transaction.

## Decision

Two additive, backward-compatible changes. No default behavior is flipped;
existing presets and tests are unchanged.

### 1. New `DbConfig::embedded_fast()` preset

Add a documented preset for the common single-process embedded-app pattern
(one handle, autocommit writes on a hot working set of tables):

```rust
pub fn embedded_fast() -> Self {
    Self {
        cache_size_mb: 32,
        retain_paged_row_sources_after_commit: true,
        paged_row_storage: false,
        wal_checkpoint_threshold_pages: 0,
        wal_checkpoint_threshold_bytes: 0,
        ..Self::default()
    }
}
```

- `retain_paged_row_sources_after_commit = true` disables the eager
  post-commit redefer, so the just-written table stays resident and the next
  autocommit statement on the same table skips the O(table size) reload.
- `paged_row_storage = false` selects the legacy single-payload row source,
  whose incremental persist path (`append_uncompressed_with_first_page_patch`
  for appends, row-update splice for updates) is cheapest for autocommit
  writes while still producing compact on-disk files.
- `wal_checkpoint_threshold_* = 0` disables size-based auto-checkpoints so
  bulk loads are not interrupted mid-flight.
- Full durable WAL sync (`WalSyncMode::Full`) is preserved.

`balanced()`, `low_memory()`, and `tuned_durable()` are unchanged and remain
the right choices for long-lived handles that touch many tables and must
minimize resident memory between statements.

### 2. Resident read fast path

Add `Db::try_resident_read_for_statement`, which returns a read guard over
the resident runtime when the statement's base tables are all already
resident at the latest snapshot LSN (no WAL reader, no reload). Wire it as an
early-out in `execute_nontransaction_read_statement`, gated off when Lua
extensions are active (the deferred path loads extension catalog tables
before execution). This lets filtered/aggregate/pagination reads on
already-loaded tables skip the per-statement WAL-reader + row-source load.

The on-disk durable state and the durable file format are unchanged. No
public API is removed or renamed.

## Consequences

### Positive

- The autocommit write cliff is closed: `single_row_insert` 147×→4.4×,
  `update_workload` 758×→28×, `delete_workload` 1172×→84×,
  `mixed_read_write` 265×→2.6× at medium scale. `mixed_read_write` reaches
  near-parity.
- Read workloads on resident tables skip the per-statement reload:
  `aggregate_count_by_status` 12.7×→4.6×, `indexed_query` 1.02×→0.82× (now
  wins), `pagination` 3.5×→1.9×.
- Existing wins are preserved or improved: `primary_key_lookup` 0.24×→0.23×,
  `bulk_insert` 0.71×→0.73×, `database_file_size` 0.94×→0.89×.
- No existing test changes: the default presets and their memory-bounding
  behavior are untouched.

### Negative

- `embedded_fast` raises resident memory by the hot working set's row data
  (bounded by the 32 MiB cache plus the retained row sources for the tables
  in the working set). This is the explicit trade for autocommit write
  throughput; `balanced()`/`low_memory()` remain available for
  memory-constrained handles.
- The remaining gaps (planner compound-index selection for `filtered_query`,
  full-table re-encode persist for `delete`, cached-payload miss for
  autocommit `update`, generic aggregate executor, per-page `save_as` copy,
  per-statement DDL parse/commit for `cold_start`) are not closed by this
  ADR; they are root-caused and tracked in the §6 change log of
  `design/_archive/2026-06-PERF_TESTING_RESULTS.md` as follow-on planner/storage work.

### Compatibility

Additive only. `DbConfig::embedded_fast()` is a new preset. The read fast
path is an early-out that falls back to the existing deferred path when
tables are not resident or extensions are active. No durable format change,
no C ABI change.

## References

- `design/_archive/2026-06-PERF_TESTING_RESULTS.md` — the comparison that surfaced
  the cliff and the measured before/after.
- ADR 0143 — On-Disk Row-Scan Executor (deferred materialization).
- ADR 0145 — Paged Table Row Source.
- ADR 0184 — Default Fast Planner and Runtime Contract.
- `crates/decentdb/src/config.rs` — `embedded_fast`.
- `crates/decentdb/src/db.rs` — `try_resident_read_for_statement`,
  `execute_nontransaction_read_statement` early-out.