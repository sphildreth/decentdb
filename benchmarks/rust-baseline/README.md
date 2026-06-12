# DecentDB rust-baseline benchmark

This benchmark is the apples-to-apples Rust runner for the music-library
workload used to compare DecentDB against SQLite. By default it runs DecentDB
directly through the Rust crate. With `--engine sqlite`, it runs the same schema,
seed plan, and query shapes through `rusqlite`.

The SQLite path exists only in this benchmark crate. It does not add SQLite
tests, dependencies, or comparison behavior to the DecentDB engine core.

For the current cross-benchmark performance plan, see
`../../design/METRIC_IMPROVEMENTS_PLAN.md`. The public README charts are driven
by `cargo bench -p decentdb --bench embedded_compare` and
`data/bench_summary.json`; this rust-baseline runner is the larger diagnostic
surface for music-library totals, point lookups, joins, views, and grouped
aggregates.

The default DecentDB path links the `decentdb` crate directly (path-dep against
`../../crates/decentdb`) and uses the engine's hot-path API:

- `Db::create()` to make a fresh database
- `db.transaction()` to acquire an exclusive `SqlTransaction`
- `txn.prepare(sql)` once per INSERT shape
- `prepared.execute_in(&mut txn, &[Value::..., ...])` per row
- `txn.commit()` per logical batch

There is **no FFI, no marshalling, no LINQ, no parameter rewriter**, and no
ADO.NET command/connection layer — so the timings here represent the
theoretical engine ceiling that any binding could approach but never beat.

The SQLite path uses `rusqlite` against the same generated workload, with
`journal_mode=WAL`, `synchronous=FULL`, and `wal_autocheckpoint=0`. Each seed
phase runs in one explicit `BEGIN IMMEDIATE` transaction. After seeding, both
engines run a measured WAL checkpoint before query timing starts: DecentDB uses
`Db::checkpoint_wal()` and SQLite uses `PRAGMA wal_checkpoint(TRUNCATE)`. Query
timing materializes every returned column before counting a row.

## Schema and queries

- `artists`, `albums`, `songs` tables with the same columns/PKs.
- 5 secondary indexes (`idx_albums_artist`, `idx_songs_album`, etc.).
- `v_artist_songs` view joining all three.
- 13 instrumented steps: `connect_open`, `schema_create`, three seed loops,
  `checkpoint_after_seed`, and seven query shapes including `COUNT(*)`,
  aggregates, by-id lookup, Top-10 artists/albums by song count, and view
  scans.

## Scales

Mirror `Scale.cs` for `smoke` / `medium` / `full`, with an additional
benchmark-only `huge` scale at 5x `full`:

| name   | artists | albums (target) | songs cap |
|--------|--------:|----------------:|----------:|
| smoke  |     500 |          5,000  |    50,000 |
| medium |   5,000 |         50,000  |   500,000 |
| full   |  50,000 |        500,000  | 5,000,000 |
| huge   | 250,000 |      2,500,000  |25,000,000 |

The **seed plan** uses a SplitMix64 RNG seeded with 42 (deterministic, but
distinct from .NET's `System.Random`), so the actual song counts differ
slightly across the two test families even at the same scale name. This is
intentional and unavoidable without re-implementing .NET's `Random`; the
counts are reported as `Plan: artists=… total_albums=… total_songs=…`.

## Build & run

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
cargo build --release
./target/release/rust-baseline --engine decentdb --benchmark
./target/release/rust-baseline --engine sqlite --benchmark
./target/release/rust-baseline --engine decentdb --scale smoke
./target/release/rust-baseline --engine decentdb --scale medium
./target/release/rust-baseline --engine decentdb --scale full
./target/release/rust-baseline --engine decentdb --scale huge
./target/release/rust-baseline --engine sqlite --scale smoke
./target/release/rust-baseline --engine decentdb --scale full --profile resident-hot-read
./target/release/rust-baseline --report
./target/release/rust-baseline --report --report-file /tmp/rust-baseline-report.html
```

Use `--benchmark` to run all scales in order (`smoke`, `medium`, `full`,
`huge`) for the selected engine/profile and then generate the same HTML report
as `--report`. Suite mode uses the default per-engine/per-scale database paths
and rejects `--db-path`; use single-scale mode when you need to pin an exact
database file.

To run the full DecentDB-vs-SQLite comparison into a temporary output
directory, use:

```bash
cd /home/steven/src/github/decentdb/benchmarks/rust-baseline
cargo build --release
OUT="$PWD/../../.tmp/rust-baseline-compare/results"
mkdir -p "$OUT"
./target/release/rust-baseline --engine decentdb --benchmark --out-dir "$OUT"
./target/release/rust-baseline \
  --engine sqlite \
  --benchmark \
  --out-dir "$OUT" \
  --report-file "$OUT/report.html"
```

## Profiles

`--profile` applies only to `--engine decentdb`. The default profile uses
`DbConfig::default()`: durable WAL, deferred table
materialization, and paged row storage with post-commit re-deferral. It is the
low-memory profile and should remain the default historical comparison.

`--profile resident-hot-read` is a durable tuned profile for workloads that bulk
load data and immediately run read-heavy analytics on the same handle. It sets
`retain_paged_row_sources_after_commit=true`, keeping just-written paged row
sources resident after commit instead of dropping them back to the deferred set.
This is a fair profile only when reported separately from default because it
trades higher process memory for lower repeated read cost.

SQLite runs always use benchmark profile `sqlite-wal-full` and reject
DecentDB-only profiles.

## Results

JSON reports are written to
`results/<datetime>-rust-baseline-<profile>-<scale>.json` where `<datetime>` is
`YYYY-MM-DD-HHMM` (e.g., `2026-04-26-1430`). DecentDB default runs use
`default`; tuned DecentDB runs use their selected profile name; SQLite runs use
`sqlite-wal-full`. Older checked-in reports omit the profile segment and are
treated as the default profile. This timestamped naming enables historical
comparisons across multiple runs:

```
results/
├── 2026-03-24-1200-rust-baseline-full.json
├── 2026-04-26-1430-rust-baseline-default-full.json
├── 2026-06-11-1215-rust-baseline-sqlite-wal-full-full.json
└── ...
```

Each JSON report records `binding`, `benchmark_profile`, `engine_version`,
database/WAL size after the run, peak RSS, total runtime, and every
instrumented step. The `checkpoint_after_seed` step records checkpoint duration
plus WAL/database bytes before and after the checkpoint in its `extra` object.
Use `binding` to separate DecentDB (`RustRaw`) from SQLite (`SQLiteRusqlite`)
when comparing runs programmatically.

### Historical HTML report

`--report` is a **report-only** mode when used by itself: it does not run a
benchmark. Instead it loads every `*.json` result in `results/`, groups runs by
scale (`smoke`, `medium`, `full`, `huge`), and writes a static HTML report to
`results/report.html` by default. `--benchmark` runs the suite first and then
performs this report generation step automatically.

The generated report includes:

- overview cards summarizing run counts and latest results
- one section per scale in chronological order
- charts for total runtime, peak RSS, per-step duration trends, and seed
  throughput trends
- raw run-history tables and per-step summary tables so regressions and
  improvements are easy to spot over time

Use `--report-file <path>` with `--report` or `--benchmark` to override the
output path.

## Engine memory observation (worth filing)

The Rust baseline's **peak RSS climbs to 2.2 GB** on `full` while the
engine is processing read queries (aggregates, top-N, view), even though
the database on disk is only 145 MB. The database file itself is
memory-mapped so most of that RSS is shared with the page cache — but the
fact that RSS climbs sharply *during query evaluation* and stays elevated
suggests intermediate result buffers (group-by hash tables, sort buffers)
are not being released until the `Db` is dropped. This is engine-side
behavior visible to every binding, and is a candidate for an engine
backlog item alongside the existing `COUNT(*)` cold-start latency work.
