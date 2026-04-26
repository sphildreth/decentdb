# DecentDB Rust-baseline Benchmark Over Time

This file tracks performance changes over time. The **baseline** column represents the
canonical first run. Subsequent columns show the multiplier relative to baseline:

- **Throughput steps** (`seed_*`): `Nx` = `baseline_rps / current_rps` (lower = faster)
- **Duration steps** (`query_*`, `connect_*`, `schema_*`): `Nx` = `current_s / baseline_s` (lower = faster)

**Legend:**
- ✅ = improvement over baseline (and vs previous run if applicable)
- 🔴 = regression vs baseline

To add a new run:
1. Run `./target/release/rust-baseline --scale <smoke|medium|full>`
2. Note the result JSON filename (e.g., `2026-05-01`)
3. Calculate the multiplier vs baseline and add a column to the appropriate table
4. Add ✅ or 🔴 based on whether the value is < 1.00x (better) or > 1.00x (worse)

---

## Smoke scale (500 artists / 5,000 albums / 50,000 songs cap)

| step | baseline (1x) | PREV | CURRENT |
|------|--------------:|-----:|--------:|
| **Throughput (r/s)** ||
| `seed_artists` | - | | |
| `seed_albums` | - | | |
| `seed_songs` | - | | |
| **Duration (s)** ||
| `connect_open` | - | | |
| `schema_create` | - | | |
| `query_count_songs` | - | | |
| `query_aggregate_durations` | - | | |
| `query_artist_by_id` | - | | |
| `query_top10_artists_by_songs` | - | | |
| `query_top10_albums_by_songs` | - | | |
| `query_view_first_1000` | - | | |
| `query_songs_for_artist_via_view` | - | | |

---

## Medium scale (5,000 artists / 50,000 albums / 500,000 songs cap)

| step | baseline (1x) | PREV | CURRENT |
|------|--------------:|-----:|--------:|
| **Throughput (r/s)** ||
| `seed_artists` | - | | |
| `seed_albums` | - | | |
| `seed_songs` | - | | |
| **Duration (s)** ||
| `connect_open` | - | | |
| `schema_create` | - | | |
| `query_count_songs` | - | | |
| `query_aggregate_durations` | - | | |
| `query_artist_by_id` | - | | |
| `query_top10_artists_by_songs` | - | | |
| `query_top10_albums_by_songs` | - | | |
| `query_view_first_1000` | - | | |
| `query_songs_for_artist_via_view` | - | | |

---

## Full scale (50,000 artists / 500,000 albums / 5,000,000 songs cap)

| step | baseline (1x) | PREV | CURRENT |
|------|--------------:|-----:|--------:|
| **Throughput (r/s)** ||
| `seed_artists` | - | | |
| `seed_albums` | - | | |
| `seed_songs` | - | | |
| **Duration (s)** ||
| `connect_open` | - | | |
| `schema_create` | - | | |
| `query_count_songs` | - | | |
| `query_aggregate_durations` | - | | |
| `query_artist_by_id` | - | | |
| `query_top10_artists_by_songs` | - | | |
| `query_top10_albums_by_songs` | - | | |
| `query_view_first_1000` | - | | |
| `query_songs_for_artist_via_view` | - | | |

---

## Storage (full scale)

| metric | baseline | PREV | CURRENT |
|--------|---------:|-----:|--------:|
| DB size (MB) | - | | |
| WAL size (B) | - | | |
| Peak RSS (GB) | - | | |

---

## Adding a new run

Example: after running `rust-baseline --scale full` which produced
`2026-05-01-1200-rust-baseline-full.json`, rename the PREV column to your date and add a new PREV column:

```markdown
## Full scale (50,000 artists / 500,000 albums / 5,000,000 songs cap)

| step | baseline (1x) | 2026-05-01 | CURRENT |
|------|--------------:|------------:|--------:|
| `seed_artists` | 792,664 | ✅ 0.98x | |
| `seed_albums` | 786,594 | 🔴 1.02x | |
| `seed_songs` | 672,241 | ✅ 0.95x | |
```

The multiplier is calculated as:
- **Throughput**: `baseline / current` (shows how many times slower; <1.00x = improvement)
- **Duration**: `current / baseline` (shows how many times slower; <1.00x = improvement)

**Legend:**
- ✅ = improvement (multiplier < 1.00x)
- 🔴 = regression (multiplier > 1.00x)
- Empty = no change or no data yet
