# DecentDB raw-Rust baseline vs .NET bindings — scale `full`

- Engine version: `2.3.1`
- Plan totals: artists=50,000 target_albums=500,000 songs_cap=5,000,000
- RustRaw is the `decentdb` crate called directly via `Db::transaction()` + `PreparedStatement::execute_in()` — no FFI, no marshalling, no LINQ.

## Step duration (seconds)

| step | RustRaw | AdoNet | MicroOrm | EfCore |
|------|---:|---:|---:|---:|
| `connect_open` | 0.000 | 0.003 | 0.000 | 0.004 |
| `schema_create` | 0.001 | 0.009 | 0.001 | 0.009 |
| `seed_artists` | 0.063 | 0.131 | 0.420 | 0.141 |
| `seed_albums` | 0.636 | 0.997 | 6.892 | 1.495 |
| `seed_songs` | 4.091 | 5.203 | 109.842 | 10.975 |
| `query_count_songs` | 0.000 | 0.003 | 0.003 | 0.243 |
| `query_aggregate_durations` | 0.880 | 0.923 | 0.971 | 1.062 |
| `query_artist_by_id` | 0.001 | 0.001 | 0.018 | 0.047 |
| `query_top10_artists_by_songs` | 1.709 | 1.920 | 1.846 | 1.762 |
| `query_top10_albums_by_songs` | 3.235 | 3.363 | 3.251 | 2.538 |
| `query_view_first_1000` | 2.354 | 2.555 | 2.535 | 2.358 |
| `query_songs_for_artist_via_view` | 2.198 | 2.333 | 2.248 | 4.037 |

## Throughput (records / second)

| step | RustRaw | AdoNet | MicroOrm | EfCore |
|------|---:|---:|---:|---:|
| `seed_artists` | 792,664 | 381,896 | 119,051 | 353,498 |
| `seed_albums` | 786,594 | 501,651 | 72,550 | 334,419 |
| `seed_songs` | 672,241 | 528,335 | 25,026 | 250,473 |

## Slowdown vs RustRaw (lower = closer to engine ceiling)

| step | RustRaw | AdoNet | MicroOrm | EfCore |
|------|---:|---:|---:|---:|
| `connect_open` | 1.00× | 14.21× | 2.14× | 19.33× |
| `schema_create` | 1.00× | 7.97× | 1.10× | 8.04× |
| `seed_artists` | 1.00× | 2.08× | 6.66× | 2.24× |
| `seed_albums` | 1.00× | 1.57× | 10.84× | 2.35× |
| `seed_songs` | 1.00× | 1.27× | 26.85× | 2.68× |
| `query_count_songs` | 1.00× | 30.03× | 39.17× | 2741.58× |
| `query_aggregate_durations` | 1.00× | 1.05× | 1.10× | 1.21× |
| `query_artist_by_id` | 1.00× | 1.64× | 30.13× | 78.92× |
| `query_top10_artists_by_songs` | 1.00× | 1.12× | 1.08× | 1.03× |
| `query_top10_albums_by_songs` | 1.00× | 1.04× | 1.00× | 0.78× |
| `query_view_first_1000` | 1.00× | 1.09× | 1.08× | 1.00× |
| `query_songs_for_artist_via_view` | 1.00× | 1.06× | 1.02× | 1.84× |

## Memory and storage

| binding | peak RSS / WS | DB size | WAL size |
|---------|--------------:|--------:|---------:|
| RustRaw | 2.2GB | 144.9MB | 32.0B |
| AdoNet | 2.3GB | 144.8MB | 0.0B |
| MicroOrm | 2.6GB | 144.8MB | 0.0B |
| EfCore | 2.6GB | 144.8MB | 0.0B |

## Headlines

- **Inserts.** AdoNet is now within **1.27×** of the raw engine for songs — there is little headroom left at the binding layer for ADO.NET. EFCore (refactored) sits at **2.68×**. MicroOrm at **26.85×** confirms the slice plan's identification of this layer as the highest-leverage win.
- **Heavy reads.** All bindings are within **≤1.21×** of RustRaw for aggregates, top-N, and view scans. Read performance is **engine-bound**, not binding-bound — further work on read-side bindings will produce only marginal gains.
- **EFCore micro-overhead on tiny queries.** `query_count_songs` jumps from 0ms (RustRaw) to **243ms** (EFCore). `query_artist_by_id` jumps from 1ms to 47ms. These are LINQ→SQL translation costs per-invocation that get amortized poorly when the underlying SQL is itself fast. Worth a backlog item.
- **Database file is byte-comparable across all four runs (≈145MB).** WAL is 0–32B at end-of-run thanks to checkpoint-on-close.
