# SQLite size gaps vs DecentDB (current state)

**Date:** 2026-01-31

This document explains why a DecentDB file produced by converting an existing SQLite database can currently be **~2–3× larger** on disk, and outlines an ADR-first plan to materially close the gap.

## Executive summary

- The SQLite → DecentDB conversion preserves *logical* data, but DecentDB’s current on-disk structures are not yet as space-efficient as SQLite’s.
- Some gap is operational/maintenance-related (WAL, fragmentation, rebuild behavior). We now have tooling to address this (checkpoint + `vacuum`).
- The remaining gap is structural: SQLite uses very compact encodings (varints, record headers, rowid/PK special-casing, mature btree packing). Closing this requires **persistent-format work** → **ADR required** before implementation.

## Observed sample: `artistSearchEngine` (real files)

All files below come from `/mnt/incoming/melodee_test/search-engine-storage`.

### Raw file sizes

- SQLite: `artistSearchEngine.db` ≈ **11 MB**
- DecentDB (pre-vacuum): `artistSearchEngine-decentdb.ddb` ≈ **30 MB** (WAL: 0 MB after checkpoint)
- DecentDB (post-vacuum): `artistSearchEngine-decentdb.vacuum.ddb` ≈ **25 MB** (WAL: 0 MB after vacuum)

### Page stats

- SQLite:
  - `page_size = 4096`
  - `page_count = 2713` → ~**10.6 MiB** of pages (plus header/overhead)
  - `freelist_count = 0` (already tightly packed)
- DecentDB:
  - pre-vacuum: `page_size = 4096`, `page_count = 7632` → ~**29.8 MiB** of pages
  - post-vacuum: `page_size = 4096`, `page_count = 6210` → ~**24.2 MiB** of pages

### Schema objects (SQLite)

- `sqlite_schema`: **3 tables**, **11 indexes**
  - The 3rd table is `sqlite_sequence` (AUTOINCREMENT bookkeeping). This is not imported into DecentDB.

## What we’ve already identified/fixed

### WAL can distort comparisons

- SQLite uses a separate WAL (`-wal`) file when `journal_mode=wal`.
- DecentDB also uses a `-wal` sidecar.
- Comparing “main db file” sizes without accounting for WAL can lead to misleading “>100% larger” results.

**Rule:** always compare after a checkpoint/vacuum so WAL is truncated.

### Index rebuild previously caused file growth

`rebuild-index` historically reset the root page, but did not free the old btree pages. That meant index rebuilds could leave unreachable pages behind and cause long-term file bloat.

This has been fixed by freeing the existing btree pages (and any leaf overflow chains) before rebuild.

## Why we are still ~2× larger than SQLite (root causes)

Even after vacuuming, DecentDB remains significantly larger because:

### 1) SQLite’s record encoding is extremely compact

SQLite stores integers using a variable-length encoding (varint). Small integers (IDs, enums, years) commonly use 1–3 bytes rather than 8 bytes.

DecentDB previously stored `INT64` as fixed-width 8 bytes in the record encoding. For “mostly small ints” datasets, that alone is a big multiplier.

Update (2026-01-31): DecentDB now uses ZigZag + varint encoding for `vkInt64` record payloads (see `design/adr/0034-compact-int64-record-payload.md`). This materially reduces size for integer-heavy schemas.

### 2) SQLite’s rowid / INTEGER PRIMARY KEY special-casing

In SQLite, `INTEGER PRIMARY KEY` is the rowid. That design reduces redundancy and can avoid separate index structures.

In DecentDB, tables are stored in a btree keyed by rowid, and we also maintain explicit btree indexes for constraints (PK/UNIQUE), which can create additional on-disk structures compared to SQLite’s rowid model.

### 3) Secondary index overhead, especially for TEXT keys

Many real workloads (including this one) have multiple secondary indexes over text columns.

SQLite’s btree format is highly space-optimized and benefits from decades of tuning.

DecentDB’s btree format historically used fixed-width per-cell headers, which increased index size.

Update (2026-01-31): DecentDB now uses a compact varint-based btree page layout (see `design/adr/0035-btree-page-layout-v2.md`). This reduces per-cell overhead and increases fan-out.

### 4) Page utilization / splits

Import patterns can cause suboptimal btree packing:

- inserting in “natural order” that doesn’t align with index order causes more splits
- small page fill factors lead to more pages for the same content

Vacuum helps (it rewrites), but without key compression and tighter cell formats, the gap remains.

### 5) `VARCHAR` vs `TEXT` does not reduce size today

In DecentDB today:

- `VARCHAR`, `TEXT`, and `CHARACTER VARYING` all map to the same internal type (`ctText`).
- There is no length-based packing change for `VARCHAR(n)`.

In SQLite:

- “type affinity” is mostly about coercion rules; the storage format is still based on the actual stored bytes.

So changing `TEXT` → `VARCHAR(…)` will not materially change file size in either engine as currently implemented.

## Plan to close the gap (ADR-first)

These items are ordered by expected impact and risk.

### Phase 0: Operational mitigations (now / low risk)

- Always checkpoint after large loads (already available).
- Use `decentdb vacuum` after bulk imports or churn to reclaim free pages.
- Prefer bulk-load paths that disable indexes during ingest and rebuild once (already supported in bulk loader).

### Phase 1: Compact integer encoding (high impact; ADR required)

**Goal:** make small integers cheap.

- Implemented: ZigZag + varint encoding for `vkInt64` record payloads (see `design/adr/0034-compact-int64-record-payload.md`).

**Notes:**
- This affects the persistent record format and possibly index formats.
- Requires a format versioning strategy and migration story.

### Phase 2: Reduce btree per-cell overhead (high impact; ADR required)

**Goal:** reduce btree page overhead and improve fan-out.

- Implemented: compact varint-based btree page layout (see `design/adr/0035-btree-page-layout-v2.md`).

Further work (still ADR-required) may include slot directories and/or prefix compression for ordered TEXT keys if/when TEXT keys become first-class ordered btree keys.

### Phase 3: Re-evaluate PK / rowid redundancy (medium-to-high impact; ADR required)

**Goal:** avoid paying twice for identity.

Options to evaluate:
- Special-case `PRIMARY KEY` when it is the rowid key (avoid separate PK index).
- Track UNIQUE/PK constraints via table btree properties rather than separate btrees where possible.

### Phase 4: Optional compression for large TEXT/BLOB (selective; ADR required)

**Goal:** reduce storage for large payload columns.

- Per-column compression (e.g., zstd) for TEXT/BLOB.
- Must be opt-in and compatible with fast reads and snapshot isolation.

## Concrete next steps

1. Write ADR: record encoding + format bump strategy (varint for INT64 at minimum).
2. Write ADR: btree leaf layout changes + prefix compression strategy.
3. Write ADR: PK/rowid redundancy decision.
4. Add a repeatable “size regression” benchmark that converts SQLite → DecentDB and tracks:
   - `.ddb` size
   - page_count
   - index count and per-index build cost

## Performance notes (don’t misread the units)

We also observed what looked like a large point-lookup speed difference at first glance, but the two numbers below are actually in the same ballpark.

### Observed point lookup: `Artists WHERE Id = 22`

SQLite CLI (`.timer on`) reports:

- `Run Time: real 0.000341 ...`

Important: that `real` value is **seconds**, not milliseconds.

So:

- `0.000341 s` = `0.341 ms`

DecentDB CLI currently reports:

- `{"elapsed_ms":0.3618}`

So in that run:

- SQLite ≈ **0.341 ms**
- DecentDB ≈ **0.362 ms**

That’s not “orders of magnitude”; it’s ~6% apart, which can easily be noise depending on CPU frequency scaling, cache warmth, and measurement overhead.

### Making benchmarks apples-to-apples

The CLI environment can dominate sub-millisecond queries.

To compare engine performance rather than CLI overhead:

- Measure **cold vs warm cache** separately (first run after `echo 3 | sudo tee /proc/sys/vm/drop_caches` vs subsequent runs).
- Avoid “one query per process” timing. Prefer a long-running process (REPL) or a harness that keeps the DB open.
- Run N iterations and report percentiles (p50/p95/p99), not single shots.
- Ensure both engines use the same effective plan (PK lookup vs full scan; correct casing/quoting; same predicate).
- For DecentDB CLI microbenchmarks where you don’t care about returned rows, use `decentdb exec --format json --noRows ...` so the timing reflects execution rather than JSON row materialization.

### Substring LIKE (`LIKE '%needle%'`) needs a trigram index

For substring predicates, a normal btree index generally cannot be used (SQLite also does a full scan for `LIKE '%needle%'`).

DecentDB’s intended fast path for substring LIKE is a trigram index. Without it, DecentDB will also scan and evaluate LIKE row-by-row, which can look much slower than SQLite in CLI benchmarks.

Practical guidance:

- If you run substring LIKE queries on a column, create a trigram index:
  - `CREATE INDEX artists_namenormalized_trgm ON artists USING trigram (namenormalized);`
- Re-run the benchmark with that index in place; this is the apples-to-apples comparison for “search-like workloads”.

### What we can do about performance

Near-term (no format changes):

- Add a small built-in micro-benchmark harness for common query shapes (PK lookup, range scan, secondary index lookup, join).
- Add prepared statement caching / reuse in the CLI/harness for repeated queries.
- Reduce per-row formatting overhead in the CLI JSON output for large result sets.

Longer-term (often tied to the same work that reduces size):

- More compact encodings reduce I/O and improve cache residency.
- Better btree page packing and (eventually) prefix compression can reduce page reads for indexed lookups.

## Appendix: what vacuum guarantees / does not guarantee

- Vacuum rewrites into a new file, so it can reclaim free pages and reduce fragmentation.
- Vacuum does **not** change the fundamental encoding efficiency (ints and text keys are still stored the same way), so it cannot fully close the gap to SQLite by itself.
- Add a `bench`/`perf` harness that runs repeated point lookups and reports p50/p95.

The larger “space efficiency” work items (varints, prefix compression, etc.) will also typically help performance via fewer cache misses and less IO.

## Performance notes (SQLite vs DecentDB)

This section captures an early “sanity check” query timing comparison and what it does (and doesn’t) tell us.

### Observed example: point lookup by `Id`

Commands:

- SQLite:
  - `sqlite3 artistSearchEngine.db` with `.timer on`
  - `SELECT * FROM Artists WHERE Id = 22;`
  - Reported: `Run Time: real 0.000341` seconds
- DecentDB:
  - `./decentdb exec -d artistSearchEngine-decentdb.ddb -s "SELECT * FROM artists WHERE Id = 22;"`
  - Reported: `"elapsed_ms": 0.3618`

Unit check:

- `0.000341` seconds = `0.341` milliseconds
- DecentDB shows `0.3618` milliseconds

These are in the **same ballpark** (within ~6%). So this specific example is **not** showing a big performance regression.

### Apples-to-apples caveats

Be careful comparing one-off CLI invocations:

- `decentdb exec` currently opens the database, runs the query, formats JSON, and closes the database each time. That includes fixed overheads that are not the same as an embedded caller or a long-lived connection.
- SQLite’s `.timer` measures within the SQLite CLI process; depending on how you run it, you may be measuring different overheads (e.g. startup, pager warm/cold state).

For meaningful comparisons, we should measure:

- Warm-cache and cold-cache separately
- “query execution only” vs “query + result formatting”
- Batched/looped queries (to average out fixed overhead)

### What we can do about performance gaps

Short-term (no format changes required):

- Benchmark using a long-lived session (`./decentdb repl`) to avoid open/close overhead per query.
- When benchmarking, minimize formatting costs (large JSON output can dominate runtimes). Consider adding a benchmark mode (e.g. `--quiet` / `--no-rows`) if needed.
- Use indexes for point lookups. For this schema, the PK/unique indexes exist in DecentDB (verify with `list-indexes`) and should be used.
- Prefer bulk-load paths that disable indexes during ingest and rebuild once; this significantly reduces import time and avoids excessive btree splits.

Medium-term (engine work; some may require ADR depending on surface area):

- Improve planner/executor to ensure index seeks are chosen consistently for equality predicates.
- Reduce allocations in row decoding and result rendering.
- Add prepared statement support in the CLI (or a simple “loop N times” benchmarking command) so query parse/bind overhead is amortized.

Long-term (ties to size work; ADR required):

- Smaller encodings (varints, prefix compression) reduce IO and improve cache density, which often improves read performance as well as size.
