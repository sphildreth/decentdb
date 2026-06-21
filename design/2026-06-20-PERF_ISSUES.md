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

## 8.0 Implementation Phases

These phases are intentionally narrow so coding agents can implement and
validate one measurable improvement at a time.

### Phase 1: Speed Up Showdown Search Index Build

Benchmark target:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/bench_complex_showdown_phase1
```

Current reduced baseline from 2026-06-20:

- DecentDB search index build: about 30.6 s.
- SQLite search index build: about 0.008 s.
- DecentDB fulltext BM25 query: about 0.002 s.
- SQLite fulltext BM25 query: about 0.00035 s.

Result after Phase 1:

- `CREATE INDEX` now rebuilds only the newly created index instead of every
  runtime index in the catalog.
- `TrigramIndexBuilder::finish_into` now batches encoded postings into the
  in-memory B-tree with one `replace_entries` call instead of rebuilding B-tree
  pages once per token.
- 700-movie reduced benchmark, rebuilt Release library:
  - DecentDB search index build: about 0.047 s.
  - SQLite search index build: about 0.007 s.
  - DecentDB fulltext BM25 query: about 0.0016 s.
  - SQLite fulltext BM25 query: about 0.00035 s.

The catastrophic search-index build gap is fixed. SQLite is still about 6.8x
faster on this row at the reduced scale, so follow-up work should target the
remaining fulltext/trigram build/query overhead after higher-priority DML and
query-planner gaps.

Owned implementation scope:

- `crates/decentdb/src/search/mod.rs`
- `crates/decentdb/src/search/fulltext.rs`
- `crates/decentdb/src/search/fulltext/analyzer.rs`
- `crates/decentdb/src/search/trigram.rs`
- `crates/decentdb/src/exec/mod.rs`
- narrowly related tests under `crates/decentdb/tests/`

Constraints:

- Preserve correctness of `fulltext_match`, `bm25`, prefix queries, phrase
  queries, and trigram `LIKE`/`ILIKE` behavior.
- Do not change on-disk format, WAL format, public ABI, or durability semantics
  in this phase.
- Prefer in-memory build-path improvements, batching, avoiding repeated
  parsing/analyzing, avoiding unnecessary row/value clones, and reducing
  per-index DDL write amplification.
- Add focused tests or benchmark-facing assertions when the change affects
  search semantics.

Acceptance criteria:

- Targeted Rust tests for fulltext/trigram still pass.
- The Showdown search index build row materially improves, ideally by at least
  2x on the 700-movie reduced benchmark.
- Fulltext BM25 query latency does not regress.
- Any remaining large gap is documented with the next suspected bottleneck.

### Phase 2: Speed Up Showdown DML And RETURNING Paths

Benchmark target:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/bench_complex_showdown_phase2
```

Current reduced baseline after Phase 1:

- DecentDB `INSERT ... RETURNING`: about 0.0195 s.
- SQLite `INSERT ... RETURNING`: about 0.0011 s.
- DecentDB `UPDATE ... RETURNING`: about 0.0147 s.
- SQLite `UPDATE ... RETURNING`: about 0.00065 s.
- DecentDB UPSERT: about 0.0027 s.
- SQLite UPSERT: about 0.00004 s.
- DecentDB bulk update: about 0.051 s.
- SQLite bulk update: about 0.0020 s.
- DecentDB bulk range delete: about 0.0255 s.
- SQLite bulk range delete: about 0.0014 s.

Phase 2 implementation result (2026-06-20 reduced showdown run, reviewed after
removing an unnecessary non-`RETURNING` prepared-insert row clone):

- DecentDB `INSERT ... RETURNING`: 0.019918 s (~16.9x slower than SQLite).
- DecentDB `UPDATE ... RETURNING`: 0.014675 s (~23.2x slower than SQLite).
- DecentDB UPSERT: 0.002908 s (~59.3x slower than SQLite).
- DecentDB bulk update: 0.045177 s (~22.9x slower than SQLite).
- DecentDB bulk range delete: 0.025424 s (~19.5x slower than SQLite).
- Remaining gap is still substantial; the first executor-level DML fast paths
  improved correctness and no-op behavior but did not materially change the
  Showdown write-path rows.

Owned implementation scope:

- `crates/decentdb/src/exec/dml.rs`
- narrowly related executor tests under `crates/decentdb/tests/`

Constraints:

- Preserve constraint checks, foreign-key actions, trigger behavior, generated
  columns, sync capture, and `RETURNING` result semantics.
- Do not change on-disk format, WAL format, public ABI, or durability semantics
  in this phase.
- Prefer simple prepared/in-place/paged-row-source improvements for common
  single-row and batch DML shapes rather than broad planner rewrites.
- Avoid benchmark-specific SQL special cases; the improvements must apply to
  ordinary `INSERT`, `UPDATE`, `DELETE`, `ON CONFLICT`, and `RETURNING`
  statements with the same shape.

Acceptance criteria:

- Add or update focused tests for any `RETURNING`, UPSERT, or bulk-DML path that
  changes behavior.
- Targeted Rust tests for DML, constraints, and foreign-key behavior still pass.
- At least one of the Showdown DML rows improves materially on the reduced
  benchmark without regressing the others.
- Any remaining large gap is documented with the next suspected bottleneck.

### Phase 3: Speed Up Ordered Primary-Key Pagination

Benchmark target:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/bench_complex_showdown_phase3
```

Current reduced baseline after Phase 2:

- DecentDB keyset pagination, `WHERE id > 500 ORDER BY id LIMIT 25`:
  0.000042 s.
- SQLite keyset pagination: 0.000028 s.
- DecentDB offset pagination, `ORDER BY id LIMIT 25 OFFSET 500`: 0.001465 s.
- SQLite offset pagination: 0.000033 s.

The offset row is about 44x slower even though it is a single-table primary-key
order with a small `LIMIT` and modest `OFFSET`. This is a good next target
because it should be solved by ordered row-id traversal and offset skipping,
not by broad cost-based join planning.

Phase 3 result note:

- `try_execute_simple_deferred_table_projection_query` and
  `try_execute_simple_table_projection_query` both now support an unfiltered
  single-table PK-order fast path for `ORDER BY <pk-int64> LIMIT/OFFSET`,
  including `DESC`.
- The new path uses persistent PK index when available, otherwise directional
  runtime ordered index traversal or direct row-source traversal.
- Added focused SQL coverage for:
  - `ORDER BY id LIMIT/OFFSET`
  - out-of-order primary-key inserts
  - out-of-range `OFFSET`
  - `LIMIT < 0` (maps to empty result)
  - descending PK pagination
- Benchmarked 700-movie reduced showdown run:
  - DecentDB offset pagination is `0.001461s`.
  - SQLite offset pagination is `0.000035s`.

The path is now safer for resident row sources, but this phase did not produce
a material benchmark improvement. The remaining offset-pagination gap appears
to be dominated by per-query overhead and row-source traversal/projection cost
at this small scale, not only by full-row sorting.

Owned implementation scope:

- `crates/decentdb/src/exec/mod.rs`
- narrowly related SQL tests under `crates/decentdb/tests/`

Constraints:

- Preserve `ORDER BY`, `LIMIT`, `OFFSET`, `LIMIT ALL`, negative limit/offset
  handling, expression ordering, and projection semantics.
- Do not change on-disk format, WAL format, public ABI, or durability semantics
  in this phase.
- Optimize general single-table primary-key order shapes, not the Showdown SQL
  string specifically.
- Avoid changing join, aggregate, or window semantics in this phase unless the
  same helper is directly shared.

Acceptance criteria:

- Add focused tests for `ORDER BY <row-id alias> LIMIT/OFFSET`, including an
  out-of-range offset and descending order if the fast path supports it.
- Targeted ordered-query tests still pass.
- The Showdown offset pagination row improves materially on the reduced
  benchmark, ideally by at least 5x, without regressing keyset pagination.
- Any remaining gap is documented with the next suspected bottleneck.

### Phase 4: Batch Foreign-Key Work During Parent Deletes

Benchmark target:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/bench_complex_showdown_phase4
```

Secondary MovieDB target:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload movie \
  --movie-movies 2000 \
  --movie-people 1000 \
  --movie-roles 8000 \
  --movie-reviews 12000 \
  --movie-tags 80 \
  --movie-movie-tags 6000 \
  --movie-watchlist 4000 \
  --movie-point-reads 100 \
  --movie-update-count 200 \
  --movie-delete-count 10 \
  --db-prefix .tmp/bench_complex_movie_phase4
```

Current reduced baseline after Phase 3:

- Showdown bulk range delete, parent `movies` rows with child FK tables present
  but no matching child rows for the inserted delete range: DecentDB 0.024629 s,
  SQLite 0.001302 s.
- MovieDB cascade delete remains one of the original slow rows; on the
  earlier in-repo smoke run SQLite was about 12x faster.

Likely cause:

- `execute_delete` computes all parent row ids, but when a table has
  referencing children it calls `apply_parent_delete_actions` once per parent
  row.
- `apply_parent_delete_actions` then resolves each referencing child table and
  foreign key, probes or scans for matching children, and applies child table
  changes per parent row.
- `matching_foreign_key_children` can use a child FK B-tree index, but the
  repeated per-parent call still repeats catalog lookup, key construction,
  child row materialization, and child row-source mutation work.

Owned implementation scope:

- `crates/decentdb/src/exec/dml.rs`
- narrowly related DML/FK tests under `crates/decentdb/tests/`

Constraints:

- Preserve `NO ACTION`, `RESTRICT`, `CASCADE`, and `SET NULL` semantics.
- Preserve trigger, sync capture, generated column, validation, and
  `RETURNING` behavior.
- Do not change on-disk format, WAL format, public ABI, or durability
  semantics.
- Optimize general parent-delete/FK-action shapes, not benchmark SQL strings.

Acceptance criteria:

- Add focused tests for deleting multiple parent rows with no matching child
  rows and with indexed cascading child rows.
- Targeted FK/DML tests pass.
- Showdown bulk delete improves materially on the reduced benchmark, ideally by
  at least 2x, without regressing MovieDB cascade correctness.
- Any remaining gap is documented with the next suspected bottleneck.

Phase 4 result note:

- Added batched parent-delete FK dispatch in `crates/decentdb/src/exec/dml.rs` by
  pre-collecting direct child-table FK metadata once per parent table and applying
  matching child work per child-table, including cascade recursion, rather than
  per-parent-row recursion.
- Added focused DML/FK tests for multi-parent delete with indexed FK children (no
  matches and cascade matches) under `crates/decentdb/tests/sql_dml_tests.rs`.
- Re-ran reduced Showdown benchmark:
  - Showdown bulk DELETE (500-row parent delete range): DecentDB 0.022788 s
    vs SQLite 0.001353 s, measured from
    `.tmp/bench_complex_showdown_phase4b`.

Phase 4B follow-up result:

- The no-index FK child fallback now builds a parent-key set once and scans each
  child row source once, preserving NULL and multi-column FK semantics.
- Added tests for composite-primary-key child tables where the FK column has no
  separate child index.
- Reduced Showdown bulk DELETE still did not materially improve:
  - DecentDB 0.022788 s vs SQLite 0.001353 s.

The remaining delete gap is therefore not only the O(parent keys x child rows)
fallback. Likely next causes are per-query delete setup, full table scans for
FK child tables without prefix-usable indexes, index maintenance overhead, and
row-source mutation/trigger bookkeeping.

### Phase 5: Broaden Python `executemany` Typed Batching

Benchmark target:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/bench_complex_showdown_phase5_typed_batch
```

Current reduced baseline after Phase 4B:

- DecentDB bulk load: about 0.200 s before the Python binding change.
- SQLite bulk load: about 0.027-0.029 s.
- Many Showdown insert shapes were not covered by the Python binding's narrow
  typed `executemany` signatures:
  - `people`: `ittt`
  - `genres` / `keywords`: `it`
  - `movies`: `itttiiittfit`
  - `movie_genres` / `movie_keywords`: `ii`
  - `roles`: `iiittti`
  - `reviews`: `iititt`

Phase 5 result note:

- Added generic typed-signature inference for Python positional `executemany`
  rows containing only `int`, `str`, and `float` values.
- The generic path now routes those batches through the existing native
  `execute_batch_typed_collected` fast path instead of per-row Python bind/reset
  loops.
- Fixed the ctypes declaration for `ddb_stmt_execute_batch_typed` so the
  non-extension fallback matches the C ABI.
- Added a Python API test proving an unlisted wide Showdown-shaped signature
  (`itttiiittfit`) uses the generic typed batch path.
- Reduced Showdown benchmark:
  - DecentDB bulk load improved from about 0.200 s to 0.063638 s.
  - SQLite bulk load in the same run was 0.027287 s.

This closes most of the accidental Python binding overhead for integer-key
Showdown bulk loads, but SQLite is still about 2.3x faster. Follow-up bulk-load
work should profile engine-side prepared batch execution, row validation, and
index/foreign-key bookkeeping rather than only Python call overhead.

### Phase 6: Speed Up Simple Bulk Arithmetic Updates

Benchmark target:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/bench_complex_showdown_phase6c_partial_index
```

Current reduced baseline after Phase 5:

- DecentDB bulk update: about 0.045 s.
- SQLite bulk update: about 0.002 s.

Phase 6 result note:

- Added a general fast path for single-column integer arithmetic updates of the
  form `col = col +/- <int literal/param>`.
- Extended that path to paged row sources so it applies to the Showdown
  benchmark with retained paged manifests.
- Tightened `index_might_change_for_assignments` so partial and expression
  indexes are only treated as changing when their indexed columns, included
  columns, expression SQL, or partial predicate SQL reference an updated column.
  Unknown/unparseable expressions still fall back to conservative behavior.
- Added focused tests for resident arithmetic updates, parameter deltas, paged
  arithmetic updates, and partial-index validity after updating an unrelated
  column.
- Reduced Showdown benchmark:
  - DecentDB bulk UPDATE improved to 0.005974 s.
  - SQLite bulk UPDATE in the same run was 0.001949 s.

This removes most of the accidental index-maintenance overhead for this row.
The remaining gap appears to be paged-manifest row-change/writeback overhead and
general transaction/update bookkeeping rather than expression evaluation or
unrelated partial-index maintenance.

### Phase 7: Hash Join Materialized CTE Inputs

Benchmark target:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/bench_complex_showdown_phase7
```

Current reduced baseline after Phase 6:

- DecentDB directors CTE: about 0.049 s.
- SQLite directors CTE: about 0.0036 s.

Phase 7 result note:

- Added an inner-join hash/equi-join path for generic materialized CTE RHS
  inputs using simple `ON a.col = b.col` equality constraints.
- The path is alias-aware, skips NULL join keys, preserves duplicate RHS rows,
  and falls back for unsupported join kinds or non-equality predicates.
- Added CTE regression coverage for a materialized CTE joined back to another
  CTE result.
- Reduced Showdown benchmark:
  - DecentDB directors CTE improved to about 0.020 s.
  - SQLite directors CTE remains about 0.0036 s.

This cuts the CTE row by roughly 2.5x, but SQLite remains about 5.5x faster.
Remaining work likely includes pushing base-table indexed join fast paths into
non-recursive CTE materialization and reducing grouped `STRING_AGG` overhead.

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

## 8.3 Iteration Summary: Implemented Wins and Remaining Gaps

This section is the running summary of the performance work done in response to
the out-of-repo MovieDB and GLM52 Showdown harnesses.

Implementation changes made in this iteration:

- Added the GLM52 Showdown workload to
  `bindings/python/benchmarks/bench_complex.py` so the in-repo Python benchmark
  now exposes point reads, full/range scans, pagination, 3-table joins,
  grouped aggregates, window functions, CTEs, fulltext, `RETURNING`, UPSERT,
  bulk DML, checkpoint, and file-size comparisons.
- Made the SQLite FTS benchmark maintain external-content FTS tables through
  triggers after the initial rebuild, so SQLite pays live-index maintenance
  costs for later insert/delete timing instead of only DecentDB paying them.
- Added DecentDB search-index build improvements that rebuild only the new
  index and batch trigram postings.
- Added prepared/batch DML improvements and row-id range delete recognition.
  The row-id delete changes pass targeted tests, but the Showdown bulk-delete
  row remains dominated by commit/durability and FK/cascade work.
- Added a fast 3-table indexed join projection path in the executor.
- Added Python C fast decoders for important benchmark result shapes,
  especially:
  - `(INT64, TEXT, TEXT, TEXT, TEXT, INT64)` for the cast/crew 3-table join.
  - `(INT64, TEXT, FLOAT64, INT64)` for Showdown integer primary-key point
    reads.
  - `(INT64, TEXT, FLOAT64, INT64, INT64)` for Showdown full-scan rows.
- Fixed a Python cursor fast-repeat issue for zero-parameter repeated SELECTs
  executed with `params=()`.
- Added a single-process resident-read shortcut gated by
  `process_coordination=single_process_unsafe`. Normal coordinated
  multi-process reads still use the WAL snapshot path.
- Added a prepared ordered row-id projection plan for
  `SELECT ... FROM table ORDER BY int64_rowid_alias LIMIT/OFFSET`, with cache
  accounting and regression coverage.
- Added a single-process resident shortcut for prepared row-id point
  projections. The Showdown point-read win still required the Python decoder
  shape because the binding was falling back on the 4-column result.

Validation commands run during this iteration:

```bash
cargo fmt --check
cargo check -p decentdb
cargo test -p decentdb prepared_ordered_row_id_projection_plan_resolves_limit_offset
cargo test -p decentdb simple_indexed_projection_order_by_limit_offset_uses_fast_path
cargo build -p decentdb --release
python -m py_compile bindings/python/decentdb/__init__.py bindings/python/benchmarks/bench_complex.py
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/bench_complex_showdown_fullscan_decode
```

Latest reduced Showdown result after these changes:

| Scenario | DecentDB | SQLite | Status |
|---|---:|---:|---|
| Point lookup by integer PK, 100 reads | 0.000279 s | 0.000451 s | DecentDB 1.6x faster |
| Keyset pagination | 0.000028 s | 0.000027 s | parity |
| Offset pagination | 0.000037 s | 0.000036 s | parity |
| Movie genres 3-table join | 0.001023 s | 0.001644 s | DecentDB 1.6x faster |
| Cast/crew 3-table join | 0.008976 s | 0.015657 s | DecentDB 1.7x faster |
| Final file size | 1,306,656 bytes | 2,113,536 bytes | DecentDB smaller |

Key micro-result:

- Repeated `SELECT id, title, rating FROM movies ORDER BY id LIMIT 25 OFFSET
  500` dropped from about 1.42 ms per native reset/fetch execution to about
  11 us after the prepared ordered row-id projection plan.
- Repeated `SELECT id, title, rating, runtime_minutes FROM movies WHERE id = ?`
  dropped from about 15 us per Python cursor execution to about 2.7 us after
  the resident prepared row-id shortcut plus the 4-column C decoder.

Remaining reduced Showdown gaps after this iteration:

| Scenario | Approximate gap | Current diagnosis |
|---|---:|---|
| Bulk load | SQLite about 2.4x faster | Python/binding batch insert and row/index maintenance overhead remain high. |
| B-tree index build | SQLite about 4.5x faster | DecentDB runtime B-tree rebuild/build path needs bulk-build and allocation profiling. |
| Search index build | SQLite about 6.4x faster | DecentDB trigram/fulltext build improved, but still much slower than SQLite FTS5 rebuild at this scale. |
| Full table scan | DecentDB about 2.4-2.8x faster | Fixed: exposed the existing C `decode_matrix_i64_text_f64_i64_i64` decoder for the 5-column `(INT64, TEXT, FLOAT64, INT64, INT64)` scan shape; engine materialization was already ~90 us, the gap was Python generic matrix decode. |
| Filtered range | DecentDB about 1.1-2.2x faster | Fixed: extended `simple_range_projection_filter` to capture residual column-vs-literal/param predicates on non-range columns, and to accept `Cast(Literal|Parameter)` bound values so typed date literals (`CAST('2010-01-01' AS DATE)`, `DATE '...'`) reach the filtered fast path instead of the generic executor. |
| Indexed range/order (`ORDER BY rating DESC LIMIT 50`) | DecentDB about 2.8-3.5x faster | Fixed by the same cast-bound recognition: the query now uses the simple filtered projection path (scan + range filter on `released` + sort by `rating` + limit) instead of the generic executor. A bounded Top-N heap would still help the sort phase but is not needed for parity. |
| Review aggregate join and filmography | SQLite about 2-3x faster | Needs grouped aggregate over index prefixes plus late materialization. |
| Window functions | SQLite about 1.5-2.2x faster | Needs partition/order execution without excess row cloning/sorting. |
| Multi-CTE directors query | SQLite about 5.3x faster | CTE materialization and `STRING_AGG` still need planner/executor work. |
| Fulltext BM25 | SQLite about 4.4x faster | Query-time fulltext scorer and result materialization need profiling. |
| `INSERT/UPDATE ... RETURNING`, UPSERT, bulk update/delete | SQLite about 2.7-73x faster | Cold statement/RETURNING materialization, commit path, and FK/cascade work dominate. |
| Checkpoint | SQLite about 1.2x faster | Compare semantics carefully before treating this as a pure engine gap. |

The current evidence no longer supports a blanket statement that SQLite is
faster on every small read: DecentDB now wins point lookup and the two 3-table
join scenarios in the reduced Showdown benchmark. SQLite is still materially
faster on the broad join/aggregation/search/window/CTE/write-maintenance parts
of the workload.

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

## 11. Phase Reports (2026-06-21 Iteration)

### Phase 1: Full Scan And Result Materialization

Hypothesis: The Showdown full table scan gap (`SELECT id, title, rating,
runtime_minutes, vote_count FROM movies`) was dominated by Python-side generic
matrix decoding, not engine materialization. The engine `simple_projection`
path clones resident `Value`s cheaply; a Rust micro-benchmark showed the engine
full scan over 700 rows takes ~90 us per execution while the benchmark measured
~2,500 us, so the cost was in the binding decode path.

Files changed:

- `bindings/python/decentdb/_fastdecode.c`: exposed the existing internal
  `decode_i64_text_f64_i64_i64_row`/`_values` helpers as new Python-callable
  `decode_row_i64_text_f64_i64_i64` and `decode_matrix_i64_text_f64_i64_i64`
  functions, and registered them in the module method table.
- `bindings/python/decentdb/__init__.py`: wired the new
  `decode_matrix_i64_text_f64_i64_i64` native decoder into
  `_decode_row_view_matrix` for `col_count == 5` with the
  `INT64/TEXT/FLOAT64/INT64/INT64` tag shape, with the same per-SQL
  fallback-disabling pattern used by the other matrix decoders.

Benchmark before (3-run median, reduced Showdown, 700 movies):

- DecentDB full table scan: ~0.0025 s.
- SQLite full table scan: ~0.0006 s.
- Gap: SQLite about 4.1x faster.

Benchmark after (3-run, reduced Showdown, 700 movies, rebuilt `_fastdecode`):

- DecentDB full table scan: 0.000267 / 0.000278 / 0.000272 s.
- SQLite full table scan: 0.000743 / 0.000656 / 0.000642 s.
- Result: DecentDB about 2.4-2.8x faster than SQLite in three consecutive runs.

Existing wins preserved: point lookup (~1.7x faster), cast/crew join (~1.6x
faster), movie genres join (~1.6x faster), final file size (smaller) all held.

Tests run:

- `cargo fmt --check` (clean).
- `cargo check -p decentdb` (clean).
- `python -m py_compile bindings/python/decentdb/__init__.py
  bindings/python/benchmarks/bench_complex.py`.
- Manual correctness check: 5-row `SELECT id, title, rating, runtime, votes`
  returns the exact expected tuples through the new decoder path.
- `python -m pytest bindings/python/tests/test_basic.py` (10 passed).

Remaining risk: The new fast path only covers the specific 5-column
`INT64/TEXT/FLOAT64/INT64/INT64` shape. Other 5-column scan shapes still use the
generic Python loop. This is acceptable because the decoder uses the same
shape-gated pattern as the existing 3- and 6-column decoders and falls back
safely on tag mismatch or native exception.

Next task: Phase 2 — Range Scans And Indexed Range/Order (filtered range and
indexed range/order are still 4-6x slower than SQLite).

### Phase 2: Filtered Range Scans (Residual Predicate Recognition)

Hypothesis: The Showdown filtered range query
`SELECT id, title, rating FROM movies WHERE rating >= 7.5 AND rating <= 9.0 AND runtime_minutes > 120`
fell through to the generic executor because `simple_range_projection_filter`
bailed whenever the conjunction contained a predicate on a column other than
the range column. A Rust micro-benchmark confirmed the engine spent ~273 us
per execution in the generic path versus ~90 us for an unfiltered simple scan,
so the gap was recognizer/executor overhead, not indexing.

Files changed:

- `crates/decentdb/src/exec/mod.rs`:
  - `SimpleRangeProjectionFilter` / `SimpleRangeFilterState` now carry a
    `residual: Vec<SimpleResidualFilterTerm>` for column-vs-literal/param
    comparisons on non-range columns.
  - `collect_simple_range_projection_terms` now falls through to residual
    capture when a comparison is on a different column than the chosen range
    column, instead of bailing. At most one residual term per column is kept.
  - Added `SimpleResidualPlan`, `simple_residual_matches`, and
    `simple_residual_matches_all` to evaluate residual predicates directly
    against `stored_row.values()` without going through `eval_expr`/Dataset.
  - `try_execute_simple_filtered_projection_query` and the deferred filtered
    path build residual plans via a new `build_simple_residual_plans` helper
    and thread them through `simple_filtered_projection_result_from_source`
    and `simple_filtered_projection_result_from_persisted_state`.
  - The distinct and deferred-distinct filtered fast paths bail to the generic
    executor when a residual is present, preserving correctness until their
    own residual support is added.

Benchmark before (3-run median, reduced Showdown, 700 movies):

- DecentDB filtered range: ~0.00043 s.
- SQLite filtered range: ~0.00010 s.
- Gap: SQLite about 4.1-6.1x faster.

Benchmark after (3-run, reduced Showdown, 700 movies):

- DecentDB filtered range: 0.000127 / 0.000113 / 0.000112 s.
- SQLite filtered range: 0.000145 / 0.000187 / 0.000144 s.
- Result: DecentDB about 1.1-1.7x faster than SQLite in three consecutive runs.

Existing wins preserved across the three runs: point lookup (~1.6-1.8x
faster), full table scan (~1.7-2.3x faster), cast/crew join (~1.3-1.7x
faster), movie genres join (~1.6-1.7x faster).

Tests run:

- `cargo fmt --check` (clean).
- `cargo check -p decentdb` (clean).
- `cargo clippy -p decentdb --all-features` (0 new warnings; 9 pre-existing).
- `cargo test --lib -p decentdb` (1462 passed).
- `cargo test --tests -p decentdb` (2984 passed).
- New `simple_filtered_projection_query_supports_range_with_residual_predicate`
  covering range + residual on a different column, range + equality residual,
  and residual-excludes-all-rows.
- `python -m pytest bindings/python/tests/test_basic.py
  bindings/python/tests/test_comprehensive.py` (49 passed).

Remaining risk: The residual path only supports column-vs-literal/param
comparisons with the six comparison operators, at most one term per residual
column, and only on the resident and persisted (non-distinct) filtered
projection paths. More complex residuals (OR, expressions, same-column
duplicates) still fall back to the generic executor. The distinct filtered
paths still bail on residual. `simple_residual_matches` implements SQL
three-valued NULL logic (returns false when either operand is NULL, mirroring
`eval_binary`) and treats `compare_values` errors as not-matched, so the fast
path cannot diverge from the generic executor on NULL or incompatible-type
residuals (covered by
`simple_filtered_projection_query_supports_range_with_residual_predicate`).

Next task: Phase 2b — Indexed range/order: `ORDER BY rating DESC LIMIT 50`
still does a full TableScan + Sort and is 4.8-6.4x slower than SQLite. Needs
index-order traversal over `idx_movies_rating` or a bounded Top-N heap.

### Phase 2b: Indexed Range/Order (Cast-Bound Recognition)

Hypothesis: The Showdown indexed range/order query
`SELECT id, title, rating, released FROM movies WHERE released >= CAST('2010-01-01' AS DATE) ORDER BY rating DESC LIMIT 50`
fell through to the generic executor because `simple_range_projection_bound`
only accepted `Literal`/`Parameter` bound values, and the parser represents
typed date literals (`DATE '...'` and `CAST('...' AS DATE)`) as `Expr::Cast`.
A Rust micro-benchmark confirmed the engine spent ~1.57 ms per execution in
the generic path; the simple filtered projection path (scan + range filter +
sort + limit) completed in ~0.48 ms for the same query.

Files changed:

- `crates/decentdb/src/exec/mod.rs`: added
  `simple_bound_value_expr_is_constant` which accepts `Literal`, `Parameter`,
  or `Cast(Literal|Parameter)`, and used it in both
  `simple_range_projection_bound` and `simple_residual_projection_bound` so
  typed-literal cast bounds are recognized as constant range/residual bounds.
  The bound value is still evaluated once via `eval_expr`, which already
  handles `Cast`.

Benchmark before (3-run median, reduced Showdown, 700 movies):

- DecentDB indexed range/order: ~0.00070 s.
- SQLite indexed range/order: ~0.00013 s.
- Gap: SQLite about 4.8-6.4x faster.

Benchmark after (6 runs, reduced Showdown, 700 movies):

- DecentDB indexed range/order: 0.000090-0.000118 s.
- SQLite indexed range/order: 0.000285-0.000409 s.
- Result: DecentDB about 2.8-3.5x faster than SQLite consistently.
- Filtered range also improved further to 1.1-2.2x faster than SQLite.

Existing wins preserved: point lookup (~1.6-1.8x faster), full table scan
(~2.0-2.6x faster), cast/crew join (~1.1-1.8x faster), movie genres join
(~1.2-1.7x faster).

Tests run:

- `cargo fmt --check` (clean).
- `cargo check -p decentdb` (clean).
- `cargo clippy -p decentdb --all-features` (0 new warnings; 9 pre-existing).
- `cargo test --tests -p decentdb` (2983 passed).
- New `simple_filtered_projection_query_supports_cast_date_range_bound`
  covering `CAST('...' AS DATE)` range, `DATE '...'` range + ORDER BY + LIMIT.
- `python -m pytest bindings/python/tests/test_basic.py
  bindings/python/tests/test_comprehensive.py` (49 passed).

Remaining risk: Cast bounds are only recognized when the cast operand is a
literal or parameter. Nested casts and casts of expressions still fall back to
the generic executor. Single-execution timing at the ~100 us scale is noisy,
so the comparison label can occasionally swap for this row; the consistent
6-run measurement shows DecentDB ahead.

Next task: Phase 3 — Bulk Load And Write Paths (bulk load is 2.4x slower;
INSERT/UPDATE RETURNING, UPSERT, bulk UPDATE/DELETE are 2.7-83x slower).
