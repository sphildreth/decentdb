# Metric Improvements Plan

Date: 2026-06-28

This document tracks the performance work needed to improve DecentDB against the
metrics used in the public README benchmark charts and the rust-baseline
SQLite comparison workload.

The task is complete when the priority metrics below remain at or above the
current public benchmark baseline, and any remaining rust-baseline diagnostic
gaps are materially reduced without regressing durability or correctness.

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
| Public README benchmark summary | `data/bench_summary.json` | Source for README chart metrics | Current local summary aggregated 2026-06-20T15:11:48Z, run id `1781967814749` |
| Public README chart renderers | `scripts/make_readme_chart.py`, `scripts/visualize_alternative.py` | Normalize chart values vs SQLite and render assets | SQLite baseline is `sqlite`; higher normalized score is better |
| Native chart workload | `cargo bench -p decentdb --bench embedded_compare` | Generates the public benchmark summary | 5 statistical runs per engine |
| Rust diagnostic workload | `benchmarks/rust-baseline` | Large music-library apples-to-apples DecentDB vs SQLite comparison | Latest checked-in DecentDB run is `benchmarks/rust-baseline/results/2026-06-23-*`; latest available SQLite reference remains `.tmp/rust-baseline-sqlite-compare-20260611-152618/results` |

Public chart ratios below use the same convention as the README speedup chart:
`> 1.00x` means DecentDB is faster or more efficient than SQLite. For latency
metrics this is `sqlite_latency / decentdb_latency`; for throughput it is
`decentdb_throughput / sqlite_throughput`.

## Current Public README Metrics

These are the metrics currently used by the public benchmark images. Values
come from `data/bench_summary.json` aggregated on 2026-06-20. Ratios use
SQLite as the baseline; `> 1.00x` means the DecentDB profile is faster or has
higher throughput.

| Priority | Metric | Workload meaning | SQLite baseline | DecentDB balanced | Balanced vs SQLite | DecentDB tuned | Tuned vs SQLite | Current status |
|---:|---|---|---:|---:|---:|---:|---:|---|
| 1 | `read_p95_ms` | p95 prepared point lookup latency | 0.002853 ms | 0.000695 ms | 4.10x | 0.000904 ms | 3.16x | DecentDB wins across profiles |
| 2 | `range_scan_p95_ms` | p95 ordered 50-row range scan latency | 0.011069 ms | 0.009708 ms | 1.14x | 0.007442 ms | 1.49x | DecentDB wins across profiles |
| 3 | `join_p95_ms` | p95 prepared inner join lookup latency | 0.003330 ms | 0.001240 ms | 2.68x | 0.001066 ms | 3.12x | DecentDB wins across profiles |
| 4 | `commit_p95_ms` | p95 durable single-row auto-commit insert latency | 0.530732 ms | 0.482606 ms | 1.10x | 0.510881 ms | 1.04x | DecentDB wins across profiles |
| 5 | `concurrent_read_p95_ms` | p95 point lookup latency across 4 reader threads | 0.029393 ms | 0.002196 ms | 13.39x | 0.002258 ms | 13.02x | DecentDB wins strongly |
| 6 | `aggregate_p95_ms` | p95 prepared `COUNT/SUM` aggregate latency | 0.035109 ms | 0.000437 ms | 80.34x | 0.000439 ms | 80.01x | DecentDB wins strongly |
| 7 | `insert_rows_per_sec` | prepared single-row insert loop inside one explicit transaction | 2,099,462 rows/s | 2,528,153 rows/s | 1.20x | 2,973,892 rows/s | 1.42x | DecentDB wins across profiles |

Notes:

- The public charts currently include multiple DecentDB profiles. In the
  current summary, balanced, low-memory, and tuned all beat SQLite on every
  rendered metric.
- Storage size, WAL size, and metric standard deviations exist in
  `data/bench_summary.json`, but they are not currently rendered in the README
  images.

## Current Worktree Public Metrics

Latest local public benchmark:

```bash
cargo bench -p decentdb --bench embedded_compare
```

Output summary: `data/bench_summary.json`, aggregated on 2026-06-20 from run id
`1781967814749`.

| Metric | SQLite | Balanced | Balanced vs SQLite | Low-memory | Low-memory vs SQLite | Tuned | Tuned vs SQLite | Current status |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `insert_rows_per_sec` | 2,099,462 rows/s | 2,528,153 rows/s | 1.20x | 2,529,952 rows/s | 1.21x | 2,973,892 rows/s | 1.42x | DecentDB wins |
| `read_p95_ms` | 0.002853 ms | 0.000695 ms | 4.10x | 0.001090 ms | 2.62x | 0.000904 ms | 3.16x | DecentDB wins |
| `commit_p95_ms` | 0.530732 ms | 0.482606 ms | 1.10x | 0.511951 ms | 1.04x | 0.510881 ms | 1.04x | DecentDB wins |
| `join_p95_ms` | 0.003330 ms | 0.001240 ms | 2.68x | 0.001154 ms | 2.89x | 0.001066 ms | 3.12x | DecentDB wins |
| `range_scan_p95_ms` | 0.011069 ms | 0.009708 ms | 1.14x | 0.009873 ms | 1.12x | 0.007442 ms | 1.49x | DecentDB wins |
| `aggregate_p95_ms` | 0.035109 ms | 0.000437 ms | 80.34x | 0.000565 ms | 62.12x | 0.000439 ms | 80.01x | DecentDB wins |
| `concurrent_read_p95_ms` | 0.029393 ms | 0.002196 ms | 13.39x | 0.002467 ms | 11.92x | 0.002258 ms | 13.02x | DecentDB wins |

Interpretation:

- The public README metric set now exceeds SQLite for every DecentDB profile in
  this summary: point lookup, indexed range scan, join lookup, aggregate,
  concurrent read, durable commit p95, and bulk insert throughput are all ahead.
- Durable commit p95 is still close to the single-`fsync` floor, so regressions
  should be watched carefully. It is no longer the public metric blocker in the
  current summary.
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

Current comparison uses the latest checked-in DecentDB all-scale run from
`benchmarks/rust-baseline/results/2026-06-23-*` and the latest available local
SQLite reference from
`.tmp/rust-baseline-sqlite-compare-20260611-152618/results`. The two sides were
not rerun in the same directory, so treat this as a current diagnostic
comparison, not a fresh paired benchmark run.

| Scale | DecentDB total | SQLite total | SQLite / DecentDB | Winner | Current interpretation |
|---|---:|---:|---:|---|---|
| smoke | 0.069637 s | 0.085686 s | 1.23x | DecentDB | DecentDB still wins total runtime, but the margin narrowed |
| medium | 0.343060 s | 0.662650 s | 1.93x | DecentDB | DecentDB wins total runtime |
| full | 3.513508 s | 6.628345 s | 1.89x | DecentDB | DecentDB wins total runtime |
| huge | 25.999784 s | 33.569571 s | 1.29x | DecentDB | DecentDB wins total runtime |

Important remaining rust-baseline losses:

| Scale | `query_artist_by_id` SQLite / DecentDB | `query_view_first_1000` SQLite / DecentDB | `query_songs_for_artist_via_view` SQLite / DecentDB | Interpretation |
|---|---:|---:|---:|---|
| smoke | 0.50x | 0.05x | 0.10x | SQLite wins tiny lookup and view paths |
| medium | 1.06x | 0.19x | 0.18x | DecentDB barely wins point lookup; SQLite still wins view paths |
| full | 1.37x | 0.14x | 0.23x | DecentDB wins point lookup; SQLite still wins view paths |
| huge | 1.87x | 0.14x | 0.18x | DecentDB wins point lookup; SQLite still wins view paths |

Important rust-baseline wins and near-term watch items:

| Scale | `seed_songs` SQLite / DecentDB | `query_count_songs` SQLite / DecentDB | `query_aggregate_durations` SQLite / DecentDB | `query_top10_artists_by_songs` SQLite / DecentDB | `query_top10_albums_by_songs` SQLite / DecentDB |
|---|---:|---:|---:|---:|---:|
| smoke | 1.73x | 0.51x | 2.00x | 5.72x | 1.34x |
| medium | 2.24x | 22.50x | 3.03x | 10.73x | 2.25x |
| full | 2.01x | 62.92x | 3.21x | 12.15x | 2.22x |
| huge | 1.19x | 97.50x | 2.85x | 11.01x | 2.37x |

Notes:

- `seed_songs`, aggregates, and grouped Top-N query shapes still favor DecentDB
  at every scale.
- `query_count_songs` is now a smoke-scale watch item: SQLite is faster at the
  tiny smoke scale, while DecentDB wins strongly at medium/full/huge.
- The current rust-baseline point lookup story is no longer a broad loss.
  DecentDB wins medium/full/huge and loses only the tiny smoke case:

| Scale | Current DecentDB `query_artist_by_id` | SQLite reference | SQLite / DecentDB | Status |
|---|---:|---:|---:|---|
| smoke | 34.77 us | 17.50 us | 0.50x | SQLite wins tiny fixed-overhead case |
| medium | 30.25 us | 31.96 us | 1.06x | DecentDB wins narrowly |
| full | 41.92 us | 57.64 us | 1.37x | DecentDB wins |
| huge | 43.90 us | 82.27 us | 1.87x | DecentDB wins |

## Recommended Priority Order

| Rank | Priority metric / area | Public chart coverage | Rust-baseline coverage | Baseline status | Target |
|---:|---|---|---|---|---|
| 1 | Rust-baseline view lookup latency | Not directly charted | `query_view_first_1000`, `query_songs_for_artist_via_view` | SQLite still wins all scales on view expansion/execution paths | Reduce view expansion/materialization overhead without regressing public join/range wins |
| 2 | Tiny fixed-overhead reads | `read_p95_ms`, `aggregate_p95_ms` | smoke `query_artist_by_id`, smoke `query_count_songs` | Public metrics win, but smoke-scale rust-baseline lookup/count still favors SQLite | Profile fixed per-query overhead without trading away medium/full/huge wins |
| 3 | Durable commit latency | `commit_p95_ms` | Write suites, not total-runtime rust-baseline | Public metric now wins across balanced/low-memory/tuned, but remains close to the sync floor | Protect the current win; any further work must preserve `WalSyncMode::Full` semantics |
| 4 | Range and join latency | `range_scan_p95_ms`, `join_p95_ms` | View and join query shapes | Public metrics win across profiles; view paths remain the related diagnostic weakness | Protect public wins while improving rust-baseline views |
| 5 | Concurrent read latency | `concurrent_read_p95_ms` | Not directly represented in rust-baseline | Public metric wins strongly across profiles | Protect; watch for reader-cache or locking regressions |
| 6 | Aggregate latency | `aggregate_p95_ms` | `query_aggregate_durations`, grouped Top-N queries | Public aggregate wins ~62-80x; rust-baseline aggregate/grouped queries still win | Protect wins; optimize only if shared hot-path work helps higher priorities |
| 7 | Insert throughput | `insert_rows_per_sec` | `seed_songs` and seed loops | DecentDB wins public and rust-baseline insert paths | Protect wins; avoid trading write durability for chart gains |
| 8 | Size and memory | Stored in summary, not charted | RSS, DB size, WAL size in rust-baseline JSON | Not public-charted today | Track opportunistically; consider adding public visibility later |

## Execution Plan

1. Profile rust-baseline view paths first.
   - Use public metrics `range_scan_p95_ms` and `join_p95_ms`.
   - Use rust-baseline view steps to catch larger real-query behavior.
   - Candidate areas: indexed range iteration, deferred row retrieval, view
     expansion, join execution, and repeated small-query planning overhead.

2. Profile tiny fixed-overhead reads only after view work is understood.
   - Use `cargo bench -p decentdb --bench embedded_compare` for public
     `read_p95_ms` and `aggregate_p95_ms` regression checks.
   - Use `benchmarks/rust-baseline` smoke runs for `query_artist_by_id` and
     `query_count_songs`.
   - Candidate areas: prepared plan dispatch, reader setup, security/policy
     checks, row materialization, and result construction.

3. Preserve durable write behavior and the current public commit win.
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

- The public README metric set has no priority regression versus the 2026-06-20
  `data/bench_summary.json` summary.
- Balanced, low-memory, and tuned DecentDB remain at or above SQLite on all
  public chart metrics, or any exception has a documented reason and a follow-up
  issue.
- Balanced and low-memory DecentDB profiles are not worsened by tuned-profile
  improvements.
- Rust-baseline retains DecentDB total-runtime wins at all scales.
- Rust-baseline view-path losses and tiny fixed-overhead losses are materially
  reduced, with updated numbers in this document.
- Durability semantics remain unchanged unless covered by an ADR and matching
  migration/recovery validation.
