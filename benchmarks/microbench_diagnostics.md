# DecentDB Microbenchmark Diagnostics

This document maps macro KPI scenarios to targeted microbenchmarks implemented in:

- `crates/decentdb/benches/micro_hot_paths.rs` (Criterion)
- `crates/decentdb/benches/micro_hot_paths_callgrind.rs` (iai-callgrind)

These microbenches are additive diagnostics. They do not replace `decentdb-benchmark` macro KPI runs.

## 1. Why This Exists

When `decentdb-benchmark compare` flags regressions or high-priority target gaps, this suite helps localize likely hot paths quickly.

Use order:

1. run macro benchmark (`decentdb-benchmark run`)
2. compare to baseline (`decentdb-benchmark compare`)
3. take top opportunities and profile matching kernels here

## 2. Macro KPI -> Microbench Mapping

- `durable_commit_single.*`:
  - `wal_append_page_frame_4k`
  - `wal_frame_encode_page_4k`
  - `wal_frame_decode_page_4k`
  - `crc32c_64k`
  - `page_copy_4k`
  - `btree_insert_split_1k_small_page`
  - `record_row_encode_mixed_fields`
  - `record_row_decode_mixed_fields`

- `durable_commit_batch.*`:
  - `wal_append_page_frame_4k`
  - `btree_insert_split_1k_small_page`
  - `record_row_encode_mixed_fields`

- `point_lookup_warm.lookup_p95_us`:
  - `btree_seek_point_lookup_warm_100k`
  - `record_index_key_encode/*`

- `range_scan_warm.rows_per_sec`:
  - `btree_seek_point_lookup_warm_100k`

- trigram-related read/search regressions:
  - `trigram_tokenization`
  - `trigram_postings_intersection`

## 3. Run Commands

## 3.1 Criterion suite

```bash
cargo bench -p decentdb --features bench-internals --bench micro_hot_paths -- --noplot
```

Single benchmark:

```bash
cargo bench -p decentdb --features bench-internals --bench micro_hot_paths -- btree_seek_point_lookup_warm_100k --noplot
```

Other useful singles:

```bash
cargo bench -p decentdb --features bench-internals --bench micro_hot_paths -- wal_append_page_frame_4k --noplot
cargo bench -p decentdb --features bench-internals --bench micro_hot_paths -- btree_insert_split_1k_small_page --noplot
cargo bench -p decentdb --features bench-internals --bench micro_hot_paths -- trigram_postings_intersection --noplot
```

## 3.2 iai-callgrind suite (optional deterministic CI metrics)

```bash
cargo bench -p decentdb --features bench-internals --bench micro_hot_paths_callgrind
```

If you only want to ensure compilation of benches in CI without running callgrind collection:

```bash
cargo check -p decentdb --features bench-internals --benches
```

## 4. Notes

- `micro_hot_paths` uses Criterion’s adaptive sampling and is best for local relative comparisons.
- `micro_hot_paths_callgrind` is for instruction-level deterministic trends on compatible hosts.
- Keep running macro benchmark comparisons as source-of-truth for product KPIs.
