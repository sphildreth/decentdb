# DecentDB raw-engine baseline benchmark

This is a **raw Rust baseline** for the same benchmark suite the .NET tests
in `..` exercise. It links the `decentdb` crate directly (path-dep against
`../../crates/decentdb`) and uses the engine's hot-path API:

- `Db::create()` to make a fresh database
- `db.transaction()` to acquire an exclusive `SqlTransaction`
- `txn.prepare(sql)` once per INSERT shape
- `prepared.execute_in(&mut txn, &[Value::..., ...])` per row
- `txn.commit()` per logical batch

There is **no FFI, no marshalling, no LINQ, no parameter rewriter**, and no
ADO.NET command/connection layer — so the timings here represent the
theoretical engine ceiling that any binding could approach but never beat.

## Schema and queries

- `artists`, `albums`, `songs` tables with the same columns/PKs.
- 5 secondary indexes (`idx_albums_artist`, `idx_songs_album`, etc.).
- `v_artist_songs` view joining all three.
- 12 instrumented steps: `connect_open`, `schema_create`, three seed loops,
  and seven query shapes including `COUNT(*)`, aggregates, by-id lookup,
  Top-10 artists/albums by song count, and view scans.

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
cd /home/steven/source/decentdb/benchmarks/rust-baseline
cargo build --release
./target/release/rust-baseline --scale smoke
./target/release/rust-baseline --scale medium
./target/release/rust-baseline --scale full
./target/release/rust-baseline --scale huge
```

## Results

JSON reports are written to `results/<datetime>-rust-baseline-<scale>.json` where
`<datetime>` is `YYYY-MM-DD-HHMM` (e.g., `2026-04-26-1430`). This timestamped
naming enables historical comparisons across multiple runs:

```
results/
├── 2026-03-24-1200-rust-baseline-full.json
├── 2026-04-01-0900-rust-baseline-full.json
├── 2026-04-26-1430-rust-baseline-full.json
└── ...
```

## Headline numbers (engine 2.3.1, scale=`full`, ≈2.75M songs)

| metric                       | RustRaw   |
|------------------------------|----------:|
| `seed_artists` r/s           |   792,664 |
| `seed_albums` r/s            |   786,594 |
| `seed_songs`  r/s            |   672,241 |
| `seed_songs` slowdown vs raw |    1.00×  |
| `query_top10_albums` (s)     |     3.235 |
| peak RSS                     |    2.2 GB |
| DB size                      |  144.9 MB |

## Engine memory observation (worth filing)

The Rust baseline's **peak RSS climbs to 2.2 GB** on `full` while the
engine is processing read queries (aggregates, top-N, view), even though
the database on disk is only 145 MB. The database file itself is
memory-mapped so most of that RSS is shared with the page cache — but the
fact that RSS climbs sharply *during query evaluation* and stays elevated
suggests intermediate result buffers (group-by hash tables, sort buffers)
are not being released until the `Db` is dropped. This is engine-side
behavior visible to every binding, and is a candidate for an engine
backlog item alongside the existing `COUNT(*)` cold-start latency note in
`design/2026-04-22-NET-REVIEW-FINDINGS-PLAN.md` §6.
