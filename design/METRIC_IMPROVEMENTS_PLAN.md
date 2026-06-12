# Metric Improvements Plan

Date: 2026-06-11

This document tracks the performance work needed to improve DecentDB against the
metrics used in the public README benchmark charts and the rust-baseline
SQLite comparison workload.

The task is complete when the priority metrics below have improved from this
baseline without regressing durability or correctness.

## Mission Context

DecentDB started as a fun embedded-database project, but it has progressed into
a useful Rust-native engine with capabilities that make it more than a SQLite
clone: native types, modern language bindings, durable WAL behavior, richer
developer ergonomics, and room for engine-level features that SQLite does not
provide directly.

The current mission is to make DecentDB performance credible on the same terms
users already use to judge SQLite:

- DecentDB should be at least on par with SQLite for core embedded database
  operations.
- Where DecentDB has architectural or feature advantages, the goal is to beat
  SQLite rather than merely avoid being slow.
- Current wins in bulk insert throughput, counts, aggregates, grouped Top-N
  queries, and total rust-baseline runtime must be protected.
- The remaining credibility gaps are the operations users expect SQLite to be
  excellent at: point lookup latency, indexed/range scans, joins, and view
  expansion.
- Public-facing proof matters. Improvements should show up in the README chart
  metrics and in the rust-baseline SQLite comparison, not only in isolated
  microbenchmarks.
- Benchmark-only tricks are not acceptable. Do not weaken durability, bypass
  correctness, add SQLite comparison behavior to the engine core, or optimize
  only a binding when the bottleneck is in the engine.

## Baseline Sources

| Source | Path / command | Role | Baseline state |
|---|---|---|---|
| Public README benchmark summary | `data/bench_summary.json` | Source for README chart metrics | Matches local `main:data/bench_summary.json` at `9826f6e387a843745958c6bfbabd979e8f90ee3d` |
| Public README chart renderers | `scripts/make_readme_chart.py`, `scripts/visualize_alternative.py` | Normalize chart values vs SQLite and render assets | SQLite baseline is `sqlite`; higher normalized score is better |
| Native chart workload | `cargo bench -p decentdb --bench embedded_compare` | Generates the public benchmark summary | 5 statistical runs per engine |
| Rust diagnostic workload | `benchmarks/rust-baseline` | Large music-library apples-to-apples DecentDB vs SQLite comparison | Fresh run in `.tmp/rust-baseline-sqlite-compare-20260611-152618/results` |

Public chart ratios below use the same convention as the README speedup chart:
`> 1.00x` means DecentDB is faster or more efficient than SQLite. For latency
metrics this is `sqlite_latency / decentdb_latency`; for throughput it is
`decentdb_throughput / sqlite_throughput`.

## Public README Metrics Baseline

These are the metrics currently used by the public benchmark images.

| Priority | Metric | Workload meaning | SQLite baseline | DecentDB balanced | Balanced vs SQLite | DecentDB tuned | Tuned vs SQLite | Current status |
|---:|---|---|---:|---:|---:|---:|---:|---|
| 1 | `read_p95_ms` | p95 prepared point lookup latency | 0.002841 ms | 0.015485 ms | 0.18x | 0.001997 ms | 1.42x | Tuned wins; balanced loses |
| 2 | `range_scan_p95_ms` | p95 ordered 50-row range scan latency | 0.011001 ms | 0.625215 ms | 0.02x | 0.012359 ms | 0.89x | Tuned slightly behind |
| 3 | `join_p95_ms` | p95 prepared inner join lookup latency | 0.003222 ms | 0.028585 ms | 0.11x | 0.003350 ms | 0.96x | Tuned near parity, still behind |
| 4 | `commit_p95_ms` | p95 durable single-row auto-commit insert latency | 0.488442 ms | 0.906035 ms | 0.54x | 0.462217 ms | 1.06x | Tuned wins narrowly |
| 5 | `concurrent_read_p95_ms` | p95 point lookup latency across 4 reader threads | 0.038827 ms | 0.045370 ms | 0.86x | 0.004815 ms | 8.06x | Tuned wins strongly |
| 6 | `aggregate_p95_ms` | p95 prepared `COUNT/SUM` aggregate latency | 0.035156 ms | 0.127360 ms | 0.28x | 0.030653 ms | 1.15x | Tuned wins |
| 7 | `insert_rows_per_sec` | prepared single-row insert loop inside one explicit transaction | 2,089,870 rows/s | 2,657,466 rows/s | 1.27x | 3,251,229 rows/s | 1.56x | DecentDB wins |

Notes:

- The public charts currently include multiple DecentDB profiles. The tuned row
  is the strongest public performance story, but balanced and low-memory rows
  are still visible and must not regress.
- Storage size, WAL size, and metric standard deviations exist in
  `data/bench_summary.json`, but they are not currently rendered in the README
  images.

## Current Worktree Public Metrics

Latest local public benchmark:

```bash
cargo bench -p decentdb --bench embedded_compare
```

Output summary: `data/bench_summary.json`, generated on 2026-06-12 from the
`pk-lookup-profiled` worktree.

| Metric | SQLite | Balanced | Balanced vs SQLite | Low-memory | Low-memory vs SQLite | Tuned | Tuned vs SQLite | Current status |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `insert_rows_per_sec` | 2,083,405 rows/s | 2,370,472 rows/s | 1.14x | 2,327,752 rows/s | 1.12x | 2,641,010 rows/s | 1.27x | DecentDB wins |
| `read_p95_ms` | 0.002579 ms | 0.001156 ms | 2.23x | 0.001050 ms | 2.46x | 0.000870 ms | 2.97x | DecentDB wins |
| `commit_p95_ms` | 3.063863 ms | 3.068071 ms | 0.999x | 3.070023 ms | 0.998x | 3.067321 ms | 0.999x | At parity, not a beyond-noise win |
| `join_p95_ms` | 0.003122 ms | 0.001583 ms | 1.97x | 0.001412 ms | 2.21x | 0.001106 ms | 2.82x | DecentDB wins |
| `range_scan_p95_ms` | 0.014485 ms | 0.009546 ms | 1.52x | 0.008852 ms | 1.64x | 0.006103 ms | 2.37x | DecentDB wins |
| `aggregate_p95_ms` | 0.030896 ms | 0.000629 ms | 49.09x | 0.000577 ms | 53.53x | 0.000569 ms | 54.26x | DecentDB wins |
| `concurrent_read_p95_ms` | 0.015523 ms | 0.009334 ms | 1.66x | 0.008715 ms | 1.78x | 0.009109 ms | 1.70x | DecentDB wins |

Interpretation:

- The public README read-side metrics now exceed SQLite for every DecentDB
  profile in this worktree: point lookup, indexed range scan, join lookup,
  aggregate, concurrent read, and bulk insert throughput are all ahead.
- Durable commit p95 is at the same single-`fsync` floor as SQLite. Multiple
  local runs have moved both engines by several microseconds, and the latest
  run has DecentDB 0.1-0.2% behind SQLite with overlapping standard deviations.
  This must stay tracked as the remaining public metric blocker because it is
  not a beyond-noise DecentDB win.
- The current no-ADR commit-path work reduced engine overhead without changing
  durability: batched WAL writes now pass through VFS wrappers, no-failpoint
  VFS operations avoid failpoint-registry mutexes, no-op reactive publish
  returns before hub lookup, and prepared auto-commit inserts skip redundant
  post-commit re-deferral when no touched table is paged.
- Further durable-commit improvement likely needs either a clearly measured
  syscall-level optimization or an ADR-backed WAL/recovery change. Do not relax
  `WalSyncMode::Full`, skip the WAL header end-offset update, or otherwise
  weaken ACID semantics to win this metric.

## Rust-Baseline SQLite Comparison

The rust-baseline workload is not the public README chart input. It is a larger
diagnostic workload that has been useful for finding engine bottlenecks in
realistic joins, views, and grouped aggregates.

Fresh all-scale comparison from
`.tmp/rust-baseline-sqlite-compare-20260611-152618/results`:

| Scale | DecentDB total | SQLite total | SQLite / DecentDB | Winner | Current interpretation |
|---|---:|---:|---:|---|---|
| smoke | 0.052037 s | 0.085686 s | 1.65x | DecentDB | DecentDB wins total runtime |
| medium | 0.326903 s | 0.662650 s | 2.03x | DecentDB | DecentDB wins total runtime |
| full | 3.248880 s | 6.628345 s | 2.04x | DecentDB | DecentDB wins total runtime |
| huge | 24.200240 s | 33.569571 s | 1.39x | DecentDB | DecentDB wins total runtime |

Important remaining rust-baseline losses:

| Scale | `query_artist_by_id` SQLite / DecentDB | `query_view_first_1000` SQLite / DecentDB | `query_songs_for_artist_via_view` SQLite / DecentDB | Interpretation |
|---|---:|---:|---:|---|
| smoke | 0.36x | 0.08x | 0.04x | SQLite wins tiny lookup and view paths |
| medium | 0.44x | 0.03x | 0.05x | SQLite wins tiny lookup and view paths |
| full | 0.64x | 0.03x | 0.08x | SQLite wins tiny lookup and view paths |
| huge | 0.94x | 0.03x | 0.06x | PK lookup nearly tied; view paths still behind |

Important rust-baseline wins to protect:

| Scale | `seed_songs` SQLite / DecentDB | `query_count_songs` SQLite / DecentDB | `query_aggregate_durations` SQLite / DecentDB | `query_top10_artists_by_songs` SQLite / DecentDB | `query_top10_albums_by_songs` SQLite / DecentDB |
|---|---:|---:|---:|---:|---:|
| smoke | 1.89x | 1.03x | 3.82x | 7.51x | 2.45x |
| medium | 2.01x | 35.93x | 4.51x | 11.85x | 2.38x |
| full | 1.88x | 232.10x | 4.96x | 15.54x | 2.77x |
| huge | 1.15x | 1504.83x | 3.80x | 12.77x | 2.93x |

## 2026-06-11 Worktree Update: Point Lookup

Implemented worktree optimizations for the point-lookup priority:

- Cache prepared row-id projection metadata so prepared point reads do not
  resolve projection columns on every execution.
- Add an `execute_with_params` single-statement fast path before batch splitting
  for simple `SELECT ... WHERE rowid_alias = $n` and `COUNT(*)` queries.
- Use the existing identity-hashed `Int64Map` for deferred paged row locators.
- Retain a bounded 8 MiB per-table cache of already verified paged chunk
  payloads in `DeferredPagedRowLocatorCache`, avoiding repeated overflow reads
  and CRC checks for small dimension tables.
- Avoid a lowercased SQL allocation in the simple row-id parser.
- Avoid common-path security-table allocation and duplicate runtime read locks.
- Move decoded owned rows directly into `QueryRow` for identity projections.
- Split validated and resolved simple row-id execution so the unprepared fast
  path does not repeat table/view/temp/generated-column validation.

Validation run:

- `cargo fmt --check`
- `cargo check -p decentdb`
- `cargo clippy -p decentdb --all-targets --all-features -- -D warnings`
- `cargo test -p decentdb prepared_row_id_point_lookup_keeps_deferred_table_unloaded -- --nocapture`
- `cargo test -p decentdb prepared_row_id_range_uses_deferred_locator_cache -- --nocapture`
- `cargo test -p decentdb fast_path -- --nocapture`

Rust-baseline all-scale comparison from
`.tmp/rust-baseline-point-lookup-20260611-final/results`:

| Scale | Original DecentDB `query_artist_by_id` | Current DecentDB | Current SQLite | SQLite / DecentDB | DecentDB change vs original | Status |
|---|---:|---:|---:|---:|---:|---|
| smoke | 48.66 us | 23.98 us | 22.87 us | 0.95x | -50.7% | Large improvement; not a clean SQLite win |
| medium | 73.21 us | 35.33 us | 42.77 us | 1.21x | -51.7% | DecentDB wins this run |
| full | 89.72 us | 39.34 us | 61.94 us | 1.57x | -56.1% | DecentDB wins this run |
| huge | 87.37 us | 41.16 us | 90.31 us | 2.19x | -52.9% | DecentDB wins this run |

Smoke repeat check from
`.tmp/rust-baseline-point-lookup-20260611-smoke-repeats-v6`:

| Engine | Runs | Median | Q1 | Q3 | Interpretation |
|---|---:|---:|---:|---:|---|
| DecentDB | 24 | 23.83 us | 22.87 us | 26.08 us | Much improved but still behind SQLite median |
| SQLite | 24 | 20.58 us | 19.46 us | 22.53 us | Still faster on tiny fixed-overhead smoke lookup |

Current point-lookup status:

- The original rust-baseline DecentDB point lookup was improved by roughly
  51-56% on medium/full and 53% on huge, now beating SQLite by 1.21x, 1.57x,
  and 2.19x respectively in the latest all-scale run.
- Smoke improved by roughly 51%, but median smoke still trails SQLite by about
  14% in repeated runs. The remaining gap is fixed per-query overhead rather
  than row retrieval from storage.
- The public `embedded_compare` chart benchmark has not been rerun for this
  worktree update yet. Because the prepared path was optimized, `read_p95_ms`
  should be rerun before marking priority 1 complete.

Next point-lookup follow-ups:

- Profile the remaining fixed overhead in `begin_reader_with_pager`,
  `refresh_engine_from_snapshot`, security rule checks, and result construction
  before attempting a more invasive change.
- Add selective row decoding for partial projections such as public
  `SELECT name FROM users WHERE id = $1`; this should help public
  `read_p95_ms` more than rust-baseline `query_artist_by_id`, which projects
  every `artists` column.
- Rerun `cargo bench -p decentdb --bench embedded_compare` and update the
  public README metric table before declaring point lookup complete.

## Recommended Priority Order

| Rank | Priority metric / area | Public chart coverage | Rust-baseline coverage | Baseline status | Target |
|---:|---|---|---|---|---|
| 1 | Durable commit latency | `commit_p95_ms` | Not directly represented in rust-baseline totals | Current worktree is at parity but still 0.1-0.2% behind SQLite in the latest public run | Find a no-durability-regression win beyond sync noise, or document that an ADR-level WAL change is required |
| 2 | Rust-baseline view lookup latency | Not directly charted | `query_view_first_1000`, `query_songs_for_artist_via_view` | Public join now wins, but rust-baseline view paths still lose strongly | Reduce view expansion/materialization overhead without regressing public join/range wins |
| 3 | Point lookup latency | `read_p95_ms` | `query_artist_by_id` | Public metric now wins across profiles; rust-baseline medium/full/huge win, smoke median remains close | Protect public wins and close remaining smoke fixed-overhead gap opportunistically |
| 4 | Range scan latency | `range_scan_p95_ms` | Partial overlap through indexed scans and view paths | Public metric now wins across profiles | Protect; optimize only if shared view/range work helps rust-baseline |
| 5 | Join latency | `join_p95_ms` | View and join query shapes | Public metric now wins across profiles | Protect public win while improving rust-baseline views |
| 6 | Concurrent read latency | `concurrent_read_p95_ms` | Not directly represented in rust-baseline | Public metric now wins across profiles | Protect; watch for reader-cache or locking regressions |
| 7 | Aggregate latency | `aggregate_p95_ms` | `query_aggregate_durations`, grouped Top-N queries | Public and rust-baseline aggregate paths win strongly | Protect wins; optimize only if shared hot-path work helps higher priorities |
| 8 | Insert throughput | `insert_rows_per_sec` | `seed_songs` and seed loops | DecentDB wins public and rust-baseline insert paths | Protect wins; avoid trading write durability for chart gains |
| 9 | Size and memory | Stored in summary, not charted | RSS, DB size, WAL size in rust-baseline JSON | Not public-charted today | Track opportunistically; consider adding public visibility later |

## Execution Plan

1. Profile point lookup and indexed lookup paths.
   - Use `cargo bench -p decentdb --bench embedded_compare` for `read_p95_ms`.
   - Use `benchmarks/rust-baseline` for `query_artist_by_id`.
   - Candidate areas: prepared plan dispatch, rowid lookup, B-tree traversal,
     row materialization, and result construction.

2. Profile range scan and join/view paths.
   - Use public metrics `range_scan_p95_ms` and `join_p95_ms`.
   - Use rust-baseline view steps to catch larger real-query behavior.
   - Candidate areas: indexed range iteration, deferred row retrieval, view
     expansion, join execution, and repeated small-query planning overhead.

3. Preserve durable write behavior.
   - Any change touching WAL, commit, checkpoint, or sync behavior must protect
     `commit_p95_ms` and pass crash/durability validation.
   - Do not relax durability settings to improve charts.

4. Re-run both benchmark surfaces after each meaningful optimization.
   - Public chart surface:
     `cargo bench -p decentdb --bench embedded_compare`
   - Merge and render, only in the release benchmark lane:
     `python scripts/aggregate_benchmarks.py`
     `python scripts/make_readme_chart.py`
     `python scripts/visualize_alternative.py`
   - Diagnostic surface:
     `benchmarks/rust-baseline` all scales for `--engine decentdb` and
     `--engine sqlite`.

5. Update this document after every accepted improvement.
   - Record commit or worktree label.
   - Record benchmark command and output directory.
   - Update baseline, current, ratio, and status columns.
   - Note regressions explicitly, even when the headline metric improves.

## Completion Criteria

This task is complete when:

- The public README metric set has no priority regression versus this baseline.
- Tuned DecentDB is at or above SQLite on all public chart metrics, or any
  remaining exception has a documented reason and a follow-up issue.
- Balanced and low-memory DecentDB profiles are not worsened by tuned-profile
  improvements.
- Rust-baseline retains DecentDB total-runtime wins at all scales.
- Rust-baseline tiny lookup and view-path losses are materially reduced, with
  updated numbers in this document.
- Durability semantics remain unchanged unless covered by an ADR and matching
  migration/recovery validation.
