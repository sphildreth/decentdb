# Performance Issues: Movie Workload Gaps Versus SQLite

**Date:** 2026-06-20
**Status:** Draft investigation and implementation plan
**Audience:** Core engine maintainers, planner/executor maintainers, storage
maintainers, benchmark maintainers, documentation authors, coding agents

This document records the remaining DecentDB performance gaps found while
reviewing the .NET movie database comparison project at:

```text
/home/steven/src/scratch/decentdb-vs-sqlite/MovieDbDemo
```

The immediate benchmark setup problem was fixed first. The original DecentDB
mutation numbers were misleading because the DecentDB connection did not use
the tuned embedded profile and the native prepared-statement mutation loop
reused a statement without `Reset().ClearBindings()` between executions. After
fixing those issues, DecentDB no longer looks catastrophically slow for batched
updates. However, SQLite is still much faster on the join, aggregation, search,
and cascade-delete parts of this workload.

The goal of this document is to turn those remaining gaps into a concrete plan
and task list. The target is not parity. The target is for DecentDB to beat
SQLite on this workload while preserving durable ACID semantics.

## 1. Related Design Inputs

- `design/PRD.md`: performance must beat SQLite without compromising ACID.
- `design/SPEC.md`: benchmark and memory tracking requirements.
- `design/TESTING_STRATEGY.md`: deterministic benchmark and regression testing
  expectations.
- `design/adr/0014-performance-targets.md`: point lookup, join, substring
  search, bulk load, and recovery latency targets.
- `design/adr/0112-cost-based-optimizer-with-stats.md`: accepted direction for
  persisted stats, cost-based index selection, and join reordering.
- `design/WIN_PERFORMANCE_IMPROVEMENTS_01.md`: broader plan for streaming
  executor, cost-based planning, and durable commit fast paths.
- `design/2026-06-PERF_TESTING_RESULTS.md`: prior issue-tracker benchmark
  evidence and current branch status.

## 2. Workload Summary

The .NET movie demo creates about 1.07 million rows across:

- `Movies`
- `People`
- `Roles`
- `Reviews`
- `Tags`
- `MovieTags`
- `Watchlist`

It exercises common embedded database operations:

- bulk load in one transaction;
- primary-key point reads by UUID;
- grouped ranking queries;
- tag search through a many-to-many table;
- high-cardinality role counts;
- watchlist queries with `LEFT JOIN` and `AVG`;
- batched updates;
- cascade deletes;
- checkpoint, vacuum/compact, and final file-size comparison.

SQLite is tuned with WAL, `synchronous=NORMAL`, memory temp store, 256 MiB mmap,
64 MiB cache, foreign keys enabled, and `WITHOUT ROWID` tables. DecentDB should
therefore be compared using an explicit embedded performance profile, not the
low-memory default.

## 3. Corrected Benchmark Baseline

### 3.1 Setup Fixes Applied

The DecentDB backend was adjusted to use the practical tuned profile for this
workload:

```csharp
CacheSize = "64MB";
RetainPagedRowSourcesAfterCommit = true;
PagedRowStorage = false;
ProcessCoordination = "single_process_unsafe";
WalAutoCheckpoint = "0";
```

The native mutation loops were also corrected from this shape:

```csharp
stmt.BindDecimal(1, boxOffice).BindGuid(2, id).StepRowsAffected();
```

to this shape:

```csharp
stmt.Reset()
    .ClearBindings()
    .BindDecimal(1, boxOffice)
    .BindGuid(2, id)
    .StepRowsAffected();
```

This matters because reusable native prepared statements do not implicitly
reset the cursor or clear old bindings after each execution.

### 3.2 Measured Results After Setup Fixes

The following numbers are from a corrected local Release run on 2026-06-20.
They should be treated as an investigation baseline, not a formal published
benchmark, because the scratch harness still has methodological issues listed
in section 4.

| Operation | SQLite | DecentDB fixed | DDB/SQLite | Result |
|---|---:|---:|---:|---|
| Bulk load, 1.07M rows | 16.44 s | 7.01 s | 0.43x | DecentDB wins |
| Point reads, 1,000 UUID PK | 18.2 ms | 21.2 ms | 1.17x | Near parity |
| Update 1k box-office values | 18.3 ms | 39.3 ms | 2.15x | SQLite ahead, but no longer catastrophic |
| Top-rated movies by year | 25.0 ms | 1.25 s | 49.9x | SQLite much faster |
| Search movies by tag | 1.1 ms | 524 ms | 473x | SQLite much faster |
| Busiest people | 52.1 ms | 354 ms | 6.8x | SQLite faster |
| Watchlist query | 1.3 ms | 1.42 s | 1074x | SQLite much faster |
| Delete 10 movies with cascade | 206 ms | 2.61 s | 12.7x | SQLite faster |
| Checkpoint after mutations | 1.8 ms | 3.09 s | large | SQLite faster in this harness |
| Vacuum/compact | 2.07 s | 94 ms | 0.05x | DecentDB wins |
| Final file size | 229 MiB | 172 MiB | 0.75x | DecentDB wins |

The fixed update result is the most important correction: the original report
said DecentDB needed about 34 seconds for 1,000 updates. With the tuned profile
and correct native statement reuse, it completed in about 39 milliseconds.

The remaining problem is therefore narrower and more actionable: DecentDB still
falls behind badly on generic join, aggregate, search, cascade, and checkpoint
paths in this relational workload.

## 4. Benchmark Harness Caveats

These caveats do not erase the remaining gaps, but they matter before using the
numbers as formal product claims.

- The timing helper pre-executes several query operations to compute row counts,
  then times a second execution. This gives both engines warm plans/cache state
  but makes the measured operation different from a true cold query.
- SQLite gets several explicit PRAGMAs plus `WITHOUT ROWID`; DecentDB needs an
  equally explicit profile to be a fair comparison.
- SQLite stores money as `REAL`; DecentDB stores money as `DECIMAL`. This is a
  semantic difference and can affect both CPU and storage costs.
- SQLite uses BLOB UUID primary keys; DecentDB uses native UUID columns. That is
  a reasonable product comparison, but it is not identical physical encoding.
- DecentDB batch mutation methods count attempted ids rather than summing
  affected rows. That is acceptable for the current demo but not for a benchmark
  harness.
- The dataset generator itself does expensive in-memory de-duplication while
  building join rows. That affects total wall time outside database timings.
- The harness runs SQLite first and DecentDB second. For formal numbers, engine
  order should be randomized or alternated.

Before declaring victory or failure, create a reproducible benchmark harness
that controls these issues and produces machine-readable output.

## 5. Query-Level Findings

### 5.1 Top-Rated Movies By Year

Shape:

```sql
SELECT m.Id, m.Title, m.ReleaseYear, AVG(r.Score), COUNT(r.Id)
FROM Movies m
JOIN Reviews r ON r.MovieId = m.Id
WHERE m.ReleaseYear = ?
GROUP BY m.Id
HAVING COUNT(r.Id) >= ?
ORDER BY AVG(r.Score) DESC, m.Title
LIMIT ?
```

Observed result: DecentDB fixed run took about 1.25 s; SQLite took about
25 ms.

Likely causes:

- Missing or unused index on `Movies(ReleaseYear)`.
- Join order may start from a large table instead of applying the year filter
  first.
- Aggregate execution likely materializes too many joined rows before grouping.
- `ORDER BY aggregate LIMIT` likely sorts more rows than needed.
- Projection may decode full movie rows before the final Top-N is known.

Required direction:

- Push `m.ReleaseYear = ?` before the join.
- Use stats to choose whether to scan filtered movies then seek reviews, or
  scan reviews then join movies.
- Execute `GROUP BY movie_id` as a streaming/hash aggregate with only the
  required columns.
- Add a bounded Top-N sort for `ORDER BY ... LIMIT`.

### 5.2 Search Movies By Tag

Shape:

```sql
SELECT m.*
FROM Movies m
JOIN MovieTags mt ON mt.MovieId = m.Id
JOIN Tags t ON t.Id = mt.TagId
WHERE t.Name = ?
ORDER BY m.ReleaseYear DESC
LIMIT ?
```

Observed result: DecentDB fixed run took about 524 ms; SQLite took about
1.1 ms.

Likely causes:

- The unique index on `Tags(Name)` may not be selected early enough.
- The `MovieTags(TagId)` index may not drive the join efficiently.
- Fetching `Movies` by ids from `MovieTags` may materialize too much row data.
- `ORDER BY m.ReleaseYear DESC LIMIT ?` is not pushed into a Top-N plan.
- The generic join path may allocate/clones rows instead of passing row ids and
  late materializing final movie rows.

Required direction:

- Plan as `Tags.Name -> TagId -> MovieTags.TagId -> MovieId -> Movies`.
- Use row-id/primary-key lookups for `Movies`.
- Decode only `ReleaseYear` and final projected columns.
- Avoid sorting the entire matching set when only `LIMIT 50` is requested.

### 5.3 Busiest People

Shape:

```sql
SELECT p.Id, p.FullName, p.BirthDate, p.Biography, COUNT(r.Id)
FROM People p
JOIN Roles r ON r.PersonId = p.Id
GROUP BY p.Id
ORDER BY COUNT(r.Id) DESC
LIMIT ?
```

Observed result: DecentDB fixed run took about 354 ms; SQLite took about
52 ms.

Likely causes:

- The natural plan should scan `Roles(PersonId)` and count per person before
  fetching `People`.
- DecentDB may materialize all joined `People x Roles` rows first.
- `Biography` is only needed for the final Top-N, but may be decoded for many
  rows before ranking.

Required direction:

- Add grouped-count execution over an index prefix.
- Late materialize `People` rows only after selecting the Top-N person ids.
- Use bounded Top-N instead of full sort.

### 5.4 Watchlist Query

Shape:

```sql
SELECT m.Id, m.Title, w.Priority, AVG(r.Score)
FROM Watchlist w
JOIN Movies m ON m.Id = w.MovieId
LEFT JOIN Reviews r ON r.MovieId = m.Id
WHERE w.UserHandle = ?
GROUP BY m.Id
ORDER BY w.Priority DESC, AVG(r.Score) DESC NULLS LAST
LIMIT ?
```

Observed result: DecentDB fixed run took about 1.42 s; SQLite took about
1.3 ms.

This is the worst remaining query gap in the movie workload.

Likely causes:

- The filter `Watchlist(UserHandle)` should produce a very small row set, but
  the generic `LEFT JOIN` + aggregate path likely touches far more `Reviews`
  rows than needed.
- The engine may not recognize that only reviews for the filtered watchlist
  movies are needed.
- `LEFT JOIN` semantics prevent arbitrary reordering, but the left side can
  still be filtered and reduced first.
- `AVG(r.Score)` over `Reviews(MovieId)` should be an indexed lookup per small
  watchlist movie set, or a semi-join aggregate keyed by those movie ids.

Required direction:

- Filter watchlist first using `ix_watchlist_user`.
- Fetch movie rows by primary key only for filtered watchlist rows.
- Aggregate reviews only for the selected movie ids.
- Preserve `LEFT JOIN` null semantics while avoiding full reviews scan.
- Bound sort to Top-N.

### 5.5 Cascade Delete

Shape:

```sql
DELETE FROM Movies WHERE Id = ?
```

with cascading children in:

- `Roles(MovieId)`
- `Reviews(MovieId)`
- `MovieTags(MovieId, TagId)`
- `Watchlist(MovieId)`

Observed result for 10 movie deletes: DecentDB fixed run took about 2.61 s;
SQLite took about 206 ms.

Likely causes:

- Cascade execution may scan child tables rather than using child foreign-key
  indexes consistently.
- Cascades may execute as repeated row-by-row deletes with full row-source
  persistence between child tables.
- Some child indexes are not symmetric with the cascade workload:
  `MovieTags` has primary key `(MovieId, TagId)`, which should be useful, but
  `Watchlist` only has `UserHandle` in the scratch schema. There is no explicit
  `Watchlist(MovieId)` index.
- FK validation and cascade enforcement may reload or re-materialize child row
  sources too often.

Required direction:

- Verify FK cascade planner always seeks child rows through child-key indexes
  when available.
- Add benchmark variants with and without missing child indexes to separate
  schema defects from engine defects.
- Add batched cascade execution per parent id rather than repeated generic
  delete execution.
- Preserve FK correctness and rollback behavior.

### 5.6 Checkpoint After Mutations

Observed result: DecentDB fixed run took about 3.09 s; SQLite took about
1.8 ms.

This timing is not directly comparable because the engines expose different
checkpoint semantics and the benchmark only measures the API calls. Still, the
large gap should be investigated because checkpoint cost affects perceived
write latency and benchmark wall time.

Likely causes:

- DecentDB may rewrite or compact more table state during checkpoint.
- Row-source layout and retained row sources may interact with checkpoint
  work.
- The scratch workload performs bulk load, feature showcase mutation, 1,000
  updates, and cascade deletes before checkpoint; the accumulated WAL and dirty
  state need profiling.

Required direction:

- Instrument checkpoint phases: WAL scan, page writes, fsync, compaction,
  metadata update, heap release.
- Compare `PagedRowStorage=true` and `false`.
- Separate "flush for durability" from "compact/vacuum-like maintenance" in
  benchmark reporting if the APIs do not mean the same thing.

## 6. Cross-Cutting Root Cause Hypotheses

The likely engine-level causes span several modules:

1. **Planner lacks enough cost-based choices in these query shapes.**
   ADR 0112 defines the direction, but the movie workload needs concrete join
   order, index selection, aggregate, and Top-N choices.

2. **Generic executor still materializes too eagerly.**
   Join and aggregate paths appear to decode and allocate many rows before
   filters, grouping, ordering, and limits reduce the result.

3. **Late materialization is incomplete.**
   Queries often need row ids and a few key columns until the final projection,
   but the engine likely materializes complete rows too early.

4. **Aggregate operators are not specialized enough.**
   `COUNT`, `AVG`, grouped counts, grouped Top-N, and aggregate-over-index
   plans need first-class physical operators.

5. **Top-N sort is not pushed down.**
   Many workload queries use `ORDER BY ... LIMIT`. Full sort is unnecessary
   when a bounded heap or index order can satisfy the query.

6. **Cascade delete is not sufficiently index-driven or batched.**
   FK cascades should be planned as indexed child lookups and batch child row
   removal, not as generic repeated deletes.

7. **Benchmark and docs did not make the optimized profile obvious enough.**
   This part is already being addressed with `Performance Profile` and
   `embedded_fast` documentation, but defaults/profile policy remains open.

## 7. Plan To Beat SQLite On This Workload

### Phase 0: Make The Benchmark Decision-Grade

- [ ] Move or recreate the movie workload as an in-repo benchmark under
  `.tmp` output discipline and checked-in source.
- [ ] Emit machine-readable JSON for all timings, row counts, file sizes,
  profile settings, SQLite PRAGMAs, and engine versions.
- [ ] Alternate engine order or run both orders.
- [ ] Add warm and cold query modes.
- [ ] Count affected rows accurately for updates and deletes.
- [ ] Add schema variants for missing and present cascade indexes, especially
  `Watchlist(MovieId)`.
- [ ] Add explain/analyze capture for every query.
- [ ] Add benchmark gates for the four target query classes:
  join/aggregate, tag search, watchlist aggregate, cascade delete.

Acceptance criteria:

- [ ] Benchmark can be run with one command from repo root.
- [ ] Results include ratios versus SQLite for every operation.
- [ ] Harness records DecentDB connection profile and SQLite PRAGMAs.
- [ ] Logical result equivalence is checked before timing results are accepted.

### Phase 1: Planner Visibility And Diagnostics

- [ ] Add `EXPLAIN ANALYZE` output for actual rows, loops, elapsed time, and
  whether a node materialized rows.
- [ ] Expose whether each table access used a primary-key seek, secondary-index
  seek, full scan, or deferred row-source load.
- [ ] Expose join order and join algorithm in explain output.
- [ ] Expose aggregate algorithm in explain output.
- [ ] Add runtime tracing spans for:
  table load, index seek, row decode, join, aggregate, sort, cascade child
  lookup, checkpoint phase.
- [ ] Add a doctor/advisor warning for foreign-key cascades without child-key
  indexes.

Acceptance criteria:

- [ ] For each slow movie query, maintainers can identify the chosen access
  paths and row counts without attaching a profiler.
- [ ] Explain output makes it obvious whether DecentDB is scanning/reloading a
  large table where SQLite is seeking.

### Phase 2: Cost-Based Planning For Movie Queries

- [ ] Ensure `ANALYZE` or incremental stats provide table cardinality for all
  workload tables.
- [ ] Ensure index stats include distinct counts for:
  `Tags(Name)`, `MovieTags(TagId)`, `Roles(PersonId)`, `Reviews(MovieId)`,
  `Watchlist(UserHandle)`.
- [ ] Implement or complete cost-based selection for equality predicates on
  secondary indexes.
- [ ] Implement inner join reordering for the tag search query.
- [ ] Preserve safe `LEFT JOIN` order while pushing filters below the join for
  the watchlist query.
- [ ] Prefer plans that reduce row count before aggregation.
- [ ] Add planner regression tests for the exact movie query shapes.

Acceptance criteria:

- [ ] Tag search plan starts from `Tags(Name)` and `MovieTags(TagId)`.
- [ ] Top-rated plan applies `Movies.ReleaseYear` before joining reviews.
- [ ] Busiest-people plan groups roles by `PersonId` before fetching full
  people rows.
- [ ] Watchlist plan filters `Watchlist(UserHandle)` before touching reviews.

### Phase 3: Streaming And Late-Materialized Execution

- [ ] Replace eager generic join paths for these query shapes with physical
  operators that pass row ids and projected columns.
- [ ] Add late materialization for final row projection after Top-N.
- [ ] Add streaming hash aggregate for `GROUP BY key` with `COUNT`, `SUM`, and
  `AVG`.
- [ ] Add grouped aggregate over index prefix where possible.
- [ ] Add bounded Top-N sort for `ORDER BY ... LIMIT`.
- [ ] Avoid decoding large text columns such as `Biography`, `Synopsis`, and
  review `Text` until the final projection needs them.
- [ ] Add memory counters for intermediate row buffers and cloned `Value`
  instances.

Acceptance criteria:

- [ ] Top-rated by year is at least 20x faster than the fixed baseline and
  within 1.25x SQLite.
- [ ] Tag search is at least 100x faster than the fixed baseline and within
  1.25x SQLite.
- [ ] Watchlist query is at least 100x faster than the fixed baseline and
  within 1.25x SQLite.
- [ ] Busiest people is at least 4x faster than the fixed baseline and within
  1.25x SQLite.
- [ ] Peak intermediate memory for these queries is bounded and reported.

### Phase 4: Cascade Delete Fast Path

- [ ] Audit FK metadata to ensure child-key indexes are discoverable and used
  for cascade plans.
- [ ] Add a cascade delete physical plan that batches child lookups per parent
  id.
- [ ] Add child-table delete by row-id/index range rather than generic table
  scan where possible.
- [ ] Add rollback tests for multi-table cascade delete failures.
- [ ] Add benchmark variants:
  one parent, 10 parents, 100 parents; with and without child indexes.
- [ ] Add documentation warning that production FK child columns should be
  indexed, and add a schema advisor for missing indexes.

Acceptance criteria:

- [ ] Delete 10 movies with indexed child keys is within 1.25x SQLite.
- [ ] Delete 10 movies with the scratch schema is at least 5x faster than the
  fixed baseline.
- [ ] FK cascade correctness tests pass under rollback and crash/recovery
  scenarios.

### Phase 5: Checkpoint Profiling And Policy

- [ ] Instrument checkpoint phase timings.
- [ ] Separate benchmark rows for:
  WAL flush/checkpoint, compact/save-as, and vacuum-like maintenance.
- [ ] Compare default, `embedded_fast`, tuned durable, paged row storage on/off,
  and persistent PK index on/off.
- [ ] Determine whether checkpoint latency is an engine issue, an API semantics
  mismatch, or a benchmark labeling issue.
- [ ] Document the recommended benchmark operation for SQLite
  `PRAGMA wal_checkpoint(TRUNCATE)` versus DecentDB checkpoint/compact APIs.

Acceptance criteria:

- [ ] Checkpoint benchmark reports comparable semantics.
- [ ] If DecentDB is still slower for equivalent work, a follow-up storage/WAL
  plan is filed with phase-level timing evidence.

### Phase 6: Documentation And Defaults

- [ ] Keep low-memory durable defaults unless product leadership decides the
  default should prioritize embedded-fast behavior.
- [ ] Document `Performance Profile=embedded_fast` in .NET quickstart,
  performance guide, and C ABI options.
- [ ] Document native prepared-statement reuse:
  call `Reset().ClearBindings()` per repeated execution unless using
  `Rebind*Execute` or `ExecuteBatch*`.
- [ ] Add a troubleshooting entry:
  "DecentDB is much slower than SQLite in .NET benchmark" with profile,
  statement reuse, transaction, index, and explain checklist.
- [ ] Add advisor/doctor output for:
  low cache size on large DB, missing FK child indexes, missing stats, and
  generic aggregate fallback.
- [ ] Decide whether `embedded_fast` should become the default for .NET
  file-backed local databases or remain opt-in.

Acceptance criteria:

- [ ] A new .NET user comparing against a tuned SQLite connection can find the
  DecentDB tuned setup without reading source or benchmark code.
- [ ] Documentation states the tradeoffs: memory, row-source retention,
  checkpoint behavior, and persistent PK index costs.

## 8. Initial Task List

Use this as the first execution checklist.

- [x] Create an in-repo movie benchmark target from the scratch project.
  Implemented in `bindings/python/benchmarks/bench_complex.py` as the
  `--workload movie` path with `--movie-scale scratch` for the original
  50k/25k/250k/500k/500/150k/100k dimensions.
- [x] Create an in-repo benchmark target from the second GLM52 showdown
  project at `/home/steven/src/scratch/decentdb-vs-sqlite-glm52`.
  Implemented in `bindings/python/benchmarks/bench_complex.py` as the
  `--workload showdown` path, with `--showdown-scale glm52` for the 20k movie
  scale used by that project.
- [ ] Add JSON output and result-equivalence checks.
- [ ] Add `EXPLAIN ANALYZE` capture for all slow queries.
- [ ] Add missing `Watchlist(MovieId)` variant to separate schema and engine
  cascade costs.
- [ ] Add planner tests for tag search join order.
- [ ] Add planner tests for watchlist filter pushdown under `LEFT JOIN`.
- [ ] Add planner tests for the Showdown multi-CTE `STRING_AGG` director query.
- [ ] Add optimizer/executor tests for offset pagination over ordered primary
  keys.
- [ ] Add benchmark regression targets for trigram index build and fulltext BM25
  query latency.
- [ ] Add benchmark regression targets for `INSERT ... RETURNING`,
  `UPDATE ... RETURNING`, UPSERT, bulk update, and bulk range delete.
- [ ] Add grouped-count over secondary index prototype for busiest people.
- [ ] Add bounded Top-N sort operator.
- [ ] Add streaming hash aggregate for `AVG` and `COUNT`.
- [ ] Add late materialization for final movie/person projection.
- [ ] Add cascade delete profiling spans.
- [ ] Add indexed cascade delete fast path.
- [ ] Add checkpoint phase tracing.
- [ ] Update docs after each confirmed improvement.
- [ ] Promote any file-format, WAL, C ABI, unsafe, or dependency-impacting
  decision to an ADR before implementation.

## 8.1 In-Repo Movie Benchmark Path

`bindings/python/benchmarks/bench_complex.py` now includes the MovieDB workload
from the out-of-repo .NET harness.

Quick smoke:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload movie \
  --db-prefix .tmp/bench_complex_smoke
```

Scratch-sized reproduction:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload movie \
  --movie-scale scratch \
  --db-prefix .tmp/bench_complex_scratch \
  --keep-db
```

Coverage added:

- Movie/People/Roles/Reviews/Tags/MovieTags/Watchlist schema with foreign keys
  and `ON DELETE CASCADE`.
- Bulk load of the full dataset.
- Initial checkpoint.
- UUID primary-key point reads.
- Top-rated movies by year.
- Movie search by tag through `MovieTags`.
- Busiest people by role count.
- Watchlist query with `LEFT JOIN Reviews`, `AVG`, `GROUP BY`, and `NULLS LAST`.
- 1k box-office batch update.
- 10 movie batch delete with cascades.
- Checkpoint after mutations.
- Vacuum/compact.
- Final original database file size.

Smoke run on 2026-06-20 with 43,100 rows showed the same categories of gaps:

- SQLite was about 38x faster on top-rated-by-year.
- SQLite was about 43x faster on tag search.
- SQLite was about 244x faster on the watchlist query.
- SQLite was about 12x faster on cascade delete.
- DecentDB still produced the smaller database file and faster compact step.

The Python path uses `CAST(? AS UUID)` for DecentDB UUID parameters because the
Python binding currently binds `uuid.UUID` through the blob binder. This keeps
queries semantically correct while avoiding per-call text UUID parsing.

## 8.2 In-Repo GLM52 Showdown Benchmark Path

`bindings/python/benchmarks/bench_complex.py` now also includes the broader
integer-key movie benchmark from the second out-of-repo project:

```text
/home/steven/src/scratch/decentdb-vs-sqlite-glm52
```

Run only this workload:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --db-prefix .tmp/bench_complex_showdown
```

Run at the second project's default scale:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-scale glm52 \
  --db-prefix .tmp/bench_complex_showdown_glm52 \
  --keep-db
```

Coverage added:

- Integer-key `people`, `movies`, `genres`, `movie_genres`, `roles`, `reviews`,
  `keywords`, and `movie_keywords` schema.
- SQLite WAL + `synchronous=NORMAL`, memory temp store, mmap, and cache tuning.
- DecentDB embedded-fast open options and larger statement cache.
- B-tree index build timing.
- DecentDB trigram/fulltext index build and SQLite FTS5 rebuild timing.
- Point lookup by integer primary key.
- Full table scan, filtered range scan, and indexed date/rating range query.
- Keyset and offset pagination.
- Movie-to-genre and movie-to-cast/crew 3-table joins.
- Review aggregate join with `LEFT JOIN`, `COUNT`, `AVG`, `MIN`, and `MAX`.
- Person filmography with `COUNT(DISTINCT ...)`.
- Genre popularity, yearly release counts, and computed-decade grouping.
- Window functions: `RANK`, `DENSE_RANK`, `ROW_NUMBER`, `LAG`, and a rolling
  `ROWS BETWEEN` frame.
- Recursive CTE generation.
- Multi-CTE highly-rated director query with `HAVING` and `STRING_AGG`; this is
  the C# project scenario reported as a major DecentDB planner edge case.
- Substring `LIKE '%Shadow%'` search to exercise DecentDB trigram versus SQLite
  scan behavior.
- Fulltext BM25 search over `war OR revenge OR sacrifice`.
- `UNION`.
- `INSERT ... RETURNING`, `UPDATE ... RETURNING`, UPSERT, bulk update, and bulk
  range delete.
- DecentDB statistical aggregates (`STDDEV`, `VARIANCE`, `MEDIAN`) as a
  DecentDB-only feature row.
- Checkpoint and final file size.

A reduced validation run on 2026-06-20 used 700 movies, one person per movie,
up to two reviews per movie, and 100 point reads. It completed successfully and
showed the expected failure shape:

- SQLite was about 6.6x faster on cast/crew 3-table join.
- SQLite was about 3.3x faster on review aggregate join.
- SQLite was about 16x faster on the multi-CTE director `STRING_AGG` query.
- SQLite was about 5.6x faster on fulltext BM25 query latency.
- SQLite was about 21x faster on `INSERT ... RETURNING`.
- SQLite was about 20x faster on `UPDATE ... RETURNING`.
- SQLite was about 44x faster on UPSERT.
- SQLite was about 22x faster on bulk update.
- SQLite was about 21x faster on bulk range delete.
- DecentDB produced a smaller final file.

The Python benchmark uses a SQLite text date literal for the Showdown indexed
date range query and a DecentDB `DATE` cast for the same predicate. Python's
`sqlite3` handling of `CAST('2010-01-01' AS DATE)` produced different filter
cardinality, so the benchmark uses engine-specific equivalent literals to keep
row counts comparable.

## 9. Success Criteria

DecentDB should be considered successful for this plan only when a reproducible
benchmark run shows:

- DecentDB bulk load remains faster than SQLite.
- DecentDB point reads are at or faster than SQLite.
- DecentDB update batch is at or faster than SQLite.
- DecentDB tag search is at or faster than SQLite.
- DecentDB top-rated aggregate query is at or faster than SQLite.
- DecentDB busiest-people grouped query is at or faster than SQLite.
- DecentDB watchlist aggregate query is at or faster than SQLite.
- DecentDB cascade delete with proper child indexes is at or faster than
  SQLite.
- DecentDB Showdown multi-CTE, fulltext BM25, window-frame, pagination, and
  `RETURNING`/UPSERT/bulk-DML rows are at or faster than SQLite.
- DecentDB file size remains smaller than SQLite.
- Crash/recovery and FK correctness tests remain green.

If DecentDB only wins after disabling durability, skipping FK checks, using
benchmark-specific query rewrites, or relying on undocumented profiles, the
plan has failed.

## 10. Open Questions

- Should `embedded_fast` become the default for .NET local file-backed
  databases, or remain an explicit profile?
- Should DecentDB automatically create indexes on FK child columns, warn only,
  or preserve current explicit-index behavior?
- Should the planner create temporary hash tables for small filtered row sets,
  or should all work remain B+Tree/index driven for now?
- Should DecentDB add persistent aggregate/stat summaries, or first make normal
  aggregate execution competitive?
- What exact checkpoint operation should be compared to SQLite
  `PRAGMA wal_checkpoint(TRUNCATE)` in public benchmark tables?
- Should DECIMAL-versus-REAL benchmark variants be reported separately?
