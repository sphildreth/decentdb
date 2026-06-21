# Coding Agent Prompt: Close Remaining SQLite Performance Gaps

You are working in `/home/steven/src/github/decentdb`.

Your mission is to eliminate the remaining DecentDB performance gaps exposed by the reduced Python `bench_complex.py` Showdown benchmark and the out-of-repo .NET movie benchmarks. Work iteratively: measure, profile, improve, validate, document, then repeat until DecentDB is at parity with or faster than SQLite for the measured workload, or until a specific engine-level blocker is proven and documented with evidence.

## Current Problem Statement

Remaining gaps are still significant: bulk load, B-tree/search index build, full scans, range scans, aggregates, CTEs, fulltext BM25, window functions, and most write/RETURNING/UPSERT/delete paths are still SQLite-faster in this reduced benchmark.

Recent work has already improved some small-read paths, so do not treat this as a blanket "SQLite is faster everywhere" investigation. Current evidence shows DecentDB can beat SQLite on selected indexed point and join projections, but SQLite still materially leads on broad scans, grouped/aggregate execution, CTEs, fulltext scoring, index creation, and write maintenance.

## Non-Negotiable Constraints

- Preserve DecentDB's priority order: durable ACID writes first, fast reads second, stable bindings third.
- Do not weaken durability, foreign-key enforcement, cascade behavior, or correctness to win a benchmark.
- Do not hide failing benchmark cases, remove scenarios, or change the benchmark to make DecentDB look better unless the benchmark itself is proven unfair. If you adjust benchmark semantics, document the exact reason.
- Do not compare unsafe DecentDB settings against durable SQLite settings without clearly labeling that configuration.
- Do not make file format, WAL format, major concurrency, broad C ABI, or large architectural changes without an ADR.
- Do not run `git commit`, `git push`, or other git write operations.
- Do not revert unrelated user or agent changes. The worktree may already be dirty.
- Use `.tmp/` for benchmark output, traces, flamegraphs, scratch scripts, and logs.
- Use `rg`/`rg --files` for search.
- Use `apply_patch` for manual file edits.
- Keep changes incremental and tied to a measured gap.
- Update `design/2026-06-20-PERF_ISSUES.md` after every completed phase with before/after numbers and what changed.

## Primary Files And Surfaces

- Benchmark harness: `bindings/python/benchmarks/bench_complex.py`
- Python binding: `bindings/python/decentdb/__init__.py`
- Python native binding wrapper: `bindings/python/decentdb/native.py`
- Python fast decode extension: `bindings/python/decentdb/_fastdecode.c`
- Core engine: `crates/decentdb/src/`
- Executor paths: `crates/decentdb/src/exec/`
- Search/fulltext/trigram: `crates/decentdb/src/search/`
- Design running summary: `design/2026-06-20-PERF_ISSUES.md`

## Establish The Baseline First

Before changing code, build and run the current reduced Showdown benchmark at least three times. Save raw output under a timestamped `.tmp/perf-agent/` directory.

```bash
cargo build -p decentdb --release

python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/perf-agent/showdown-baseline
```

If the benchmark supports a larger or fuller movie workload, also run it as a secondary validation after each major phase. Use unique `.tmp/` prefixes for every run.

## Known Current Wins

Recent changes have already achieved the following on the reduced Showdown workload:

- Point lookup: DecentDB is about 1.6x faster than SQLite.
- Keyset pagination: DecentDB is near parity with SQLite.
- Offset pagination: DecentDB is near parity with SQLite.
- Movie genres join: DecentDB is about 1.6x faster than SQLite.
- Cast/crew join: DecentDB is about 1.7x faster than SQLite.
- Final file size: DecentDB is smaller than SQLite.

Do not regress these wins while pursuing the remaining gaps.

## Remaining Gaps To Close

Use the current benchmark output as the source of truth, but the last documented reduced Showdown run showed these approximate gaps:

| Area | Approximate Current Gap | Initial Suspicion |
|---|---:|---|
| Bulk load | SQLite about 2.4x faster | Python/binding batch insert plus row/index maintenance overhead |
| B-tree index build | SQLite about 4.5x faster | Runtime B-tree rebuild/build path needs bulk build and allocation profiling |
| Search index build | SQLite about 6.4x faster | Trigram/fulltext build slower than SQLite FTS5 |
| Full table scan | SQLite about 3.8x faster | Engine result materialization dominates, not Python decoding |
| Filtered range and indexed range/order | SQLite about 4-6x faster | Range plans and recognizer overhead need optimization |
| Review aggregate join and filmography | SQLite about 2-3x faster | Need grouped aggregate over index prefixes and late materialization |
| Window functions | SQLite about 1.5-2.2x faster | Partition/order execution does excess cloning/sorting |
| Multi-CTE directors query | SQLite about 5.3x faster | CTE materialization and STRING_AGG need work |
| Fulltext BM25 | SQLite about 4.4x faster | Scorer and result materialization need profiling |
| INSERT/UPDATE RETURNING, UPSERT, bulk update/delete | SQLite about 2.7-73x faster | Cold statements, RETURNING materialization, commit, FK, and cascade overhead dominate |
| Checkpoint | SQLite about 1.2x faster | Compare semantics carefully before optimizing |

## Required Workflow

Work one gap family at a time. For each phase:

1. Reproduce the gap and record the exact query or operation.
2. Add a focused microbenchmark or trace if the current benchmark is too broad.
3. Determine whether the cost is in Python binding, C ABI, SQL planning, executor, storage, index maintenance, commit/checkpoint, or result materialization.
4. Make the smallest targeted change that addresses the measured bottleneck.
5. Add or update correctness tests for the touched behavior.
6. Build in release mode.
7. Rerun the reduced Showdown benchmark.
8. Compare before/after numbers.
9. Update `design/2026-06-20-PERF_ISSUES.md` with:
   - hypothesis
   - files changed
   - before/after benchmark numbers
   - tests run
   - remaining risk
   - next recommended task

If an optimization is benchmark-specific and not generally correct, do not merge it into engine behavior. Prefer general execution improvements over shape-only hacks, but shape-specific fast paths are acceptable when they recognize a common SQL pattern safely and have correctness tests.

## Prioritized Phase Plan

### Phase 1: Full Scan And Result Materialization

Goal: Make DecentDB full table scans competitive with SQLite.

Investigate:

- Whether simple scan queries allocate/clones rows excessively.
- Whether `QueryResult` materialization forces all rows into owned values before the binding can consume them.
- Whether a direct projection path can stream or cheaply expose rows for simple scans.
- Whether Python fast decoders are bypassed by engine-side overhead.

Candidate improvements:

- Add a lower-allocation simple scan projection path.
- Avoid repeated `Value` cloning for simple column projections.
- Reuse row buffers where safe.
- Add a fast native fetch shape only after engine materialization is proven not to dominate.

Success criteria:

- Full table scan scenario is at parity with or faster than SQLite in three consecutive reduced Showdown runs.
- Existing point lookup and join projection wins do not regress materially.

### Phase 2: Range Scans And Indexed Range/Order

Goal: Close the 4-6x gap on filtered range and indexed range/order queries.

Investigate:

- Plan recognition overhead for prepared statements.
- Whether range predicates use indexes consistently.
- Whether sorted/ranged queries perform unnecessary full scans or full sorts.
- Whether LIMIT/OFFSET and top-N can terminate early.

Candidate improvements:

- Cache prepared simple range plans.
- Add dedicated indexed range projection plans for common predicates.
- Use index order directly for `ORDER BY ... LIMIT` where possible.
- Avoid full recognizer chains after a statement has been classified.

Success criteria:

- Filtered range and indexed range/order benchmark cases reach parity or better.
- Prepared statement reuse remains correct across parameter values.

### Phase 3: Bulk Load And Write Paths

Goal: Close bulk insert, `INSERT RETURNING`, `UPDATE RETURNING`, UPSERT, bulk update, and bulk delete gaps.

Investigate:

- Python `executemany` overhead and native batch APIs.
- Prepared statement reset/bind/step overhead.
- Row insertion cost with secondary indexes present.
- RETURNING row materialization overhead.
- UPSERT conflict lookup and update path.
- Delete cascade and foreign-key lookup cost.
- Commit and WAL/checkpoint cost under comparable durability semantics.

Candidate improvements:

- Add or improve typed batch insert/update/delete paths.
- Batch index maintenance when safe inside one transaction.
- Fast-path RETURNING for single-row and batch DML.
- Optimize UPSERT conflict probe and update.
- Ensure cascade deletes use available child indexes and avoid repeated full scans.
- Add row-id range delete/update recognition where applicable.

Success criteria:

- Bulk load is at parity or faster than SQLite.
- All write/RETURNING/UPSERT/delete scenarios are at parity or faster than SQLite without disabling durability or constraints.

### Phase 4: Runtime B-tree Index Build

Goal: Make `CREATE INDEX` and equivalent runtime B-tree builds competitive.

Investigate:

- Whether index build inserts keys one at a time through normal mutation paths.
- Allocation and cloning profiles during index creation.
- Whether entries can be collected, sorted, and bulk-loaded into pages.
- Whether page splits dominate.

Candidate improvements:

- Implement a sorted bulk-build path for runtime B-tree index creation.
- Reduce temporary key/value clones.
- Batch page allocation and serialization.
- Reuse existing index build helpers if present.

Success criteria:

- B-tree index build benchmark reaches parity or better.
- Index correctness tests pass for uniqueness, composite keys, NULL behavior, range scans, and order scans.

### Phase 5: Search Index Build And Fulltext BM25

Goal: Close trigram/fulltext build and BM25 search gaps.

Investigate:

- Tokenization and trigram extraction costs.
- Posting-list allocation and merge behavior.
- Whether search index build can batch by term.
- BM25 scorer hot loops and result sorting/top-K.
- Result materialization after scoring.

Candidate improvements:

- Batch postings during search-index creation.
- Avoid duplicate token/trigram allocations.
- Add top-K scoring instead of sorting all candidates when query has LIMIT.
- Store or compute field length/statistics more cheaply.
- Avoid materializing unused columns for BM25 result ranking.

Success criteria:

- Search index build and fulltext BM25 benchmark cases reach parity or better.
- Fulltext and trigram correctness tests still pass.

### Phase 6: Aggregates, Joins, And Filmography Queries

Goal: Close grouped aggregate and filmography-style query gaps.

Investigate:

- Whether grouped aggregates scan and materialize too many rows.
- Whether joins are materialized before aggregation unnecessarily.
- Whether aggregate keys can be processed in index order.
- COUNT DISTINCT, AVG, MIN/MAX, and top-N behavior.

Candidate improvements:

- Aggregate over index prefixes where possible.
- Late materialize non-grouping columns.
- Use bounded top-N heaps for ordered aggregate limits.
- Reduce hash key allocation and row cloning in grouped aggregation.

Success criteria:

- Review aggregate join and filmography scenarios reach parity or better.
- Existing optimized join projections remain fast.

### Phase 7: CTEs And STRING_AGG

Goal: Close the multi-CTE directors query gap.

Investigate:

- Whether CTEs are always materialized.
- Whether predicates can be pushed into CTE producers.
- Whether repeated CTE scans clone rows.
- `STRING_AGG` accumulation and ordering costs.

Candidate improvements:

- Inline or stream non-recursive CTEs when safe.
- Push filters and projections into CTE inputs.
- Reduce materialized row width.
- Optimize `STRING_AGG` buffer growth and separator handling.

Success criteria:

- Multi-CTE directors benchmark reaches parity or better.
- CTE correctness tests cover recursive and non-recursive behavior.

### Phase 8: Window Functions

Goal: Close ROW_NUMBER, RANK, LAG, and related window function gaps.

Investigate:

- Partition sorting and cloning.
- Whether existing indexes can satisfy partition/order requirements.
- Whether frames are recomputed per row.
- Whether window output materializes more columns than needed.

Candidate improvements:

- Stream partition-ordered input where possible.
- Reuse partition buffers.
- Compute simple ROW_NUMBER/RANK/LAG in one pass.
- Avoid full sort when input is already ordered.

Success criteria:

- Window scenarios reach parity or better.
- Window correctness tests cover ordering, partitioning, ties, and NULLs.

### Phase 9: Checkpoint And Compact

Goal: Optimize checkpoint only after confirming semantics are comparable.

Investigate:

- What SQLite checkpoint mode is being timed.
- What DecentDB checkpoint guarantees are being timed.
- Whether DecentDB is doing extra durable work.

Candidate improvements:

- Only optimize comparable work.
- If semantics differ, document the difference in benchmark output and design notes.

Success criteria:

- Checkpoint benchmark is fair and either at parity or documented as stronger semantics.

## Validation Commands

Run the smallest relevant set while iterating, then the broader set before declaring success.

```bash
cargo fmt --check
cargo check -p decentdb
cargo test -p decentdb <relevant-test-filter>
cargo build -p decentdb --release
python -m py_compile bindings/python/decentdb/__init__.py bindings/python/benchmarks/bench_complex.py
```

If `_fastdecode.c` changes, rebuild it before Python benchmark runs:

```bash
gcc -O3 -shared -fPIC $(python3-config --includes) -Iinclude \
  bindings/python/decentdb/_fastdecode.c \
  -o "bindings/python/decentdb/_fastdecode$(python3-config --extension-suffix)"
```

Run the reduced Showdown benchmark after every meaningful change:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --workload showdown \
  --showdown-movies 700 \
  --showdown-people-mult 1 \
  --showdown-reviews-per-movie 2 \
  --showdown-point-reads 100 \
  --db-prefix .tmp/perf-agent/showdown-after-change
```

Before declaring the work complete, also run any existing Python binding tests impacted by binding or fast decode changes.

## Acceptance Criteria

The work is complete only when all of the following are true:

- Every reduced Showdown benchmark scenario listed in "Remaining Gaps To Close" is at parity with or faster than SQLite across three consecutive runs, or a blocker is documented with exact root cause and evidence.
- Existing DecentDB wins on point lookup, pagination, selected joins, and file size are preserved.
- Correctness tests pass for every changed engine behavior.
- Python binding tests pass for every changed binding behavior.
- `design/2026-06-20-PERF_ISSUES.md` contains the final before/after table and remaining caveats.
- Any user-facing behavior change is reflected in the appropriate docs. Do not update `CHANGELOG.md`; use `docs/about/changelog.md` if a changelog entry is required.

## Agent Delegation Guidance

If `phase_executor_spark` or `phase_executor` agents are available, delegate one bounded phase at a time. The main agent remains responsible for integration, final validation, and documentation.

Each delegated task should include:

- The exact benchmark case and current gap.
- The relevant files to inspect.
- The hypothesis to test.
- The required validation command.
- A request for before/after numbers and a concise patch summary.

Do not delegate broad "make DecentDB faster" tasks. Delegate narrowly scoped work such as "profile and optimize full scan materialization in the Showdown benchmark" or "make runtime B-tree index creation use a sorted bulk-build path."

## Required Reporting Format

After each phase, report in this format:

```text
Phase:
Hypothesis:
Files changed:
Benchmark before:
Benchmark after:
Tests run:
Result:
Remaining risk:
Next task:
```

Keep the design summary document current as the source of record.
