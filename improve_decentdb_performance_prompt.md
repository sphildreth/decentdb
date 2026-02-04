# Prompt for a coding agent: iteratively close the DecentDB vs SQLite benchmark gap

## Mission
You are working in the DecentDB repository. Your goal is to iteratively improve DecentDB performance until the embedded benchmark chart shows DecentDB is at most **3× worse than SQLite** on each of these README metrics:

- Durable commit latency p95 (`commit_p95_ms`)
- Insert throughput (`insert_rows_per_sec`) — within 3× of SQLite (higher is better)
- Join latency p95 (`join_p95_ms`)
- Point read latency p95 (`read_p95_ms`)

Current baseline (from benchmarks/embedded_compare/data/bench_summary.json):

- DecentDB commit p95: 0.282 ms vs SQLite 0.01 ms (≈28× slower)
- DecentDB insert: 4011 rows/s vs SQLite 17186 rows/s (≈4.3× slower)
- DecentDB join p95: 1.00 ms vs SQLite 0.31 ms (≈3.2× slower)
- DecentDB point read p95: 0.012 ms vs SQLite 0.003 ms (≈4× slower)

You must work **iteratively** (many small steps): measure → profile → patch → quick correctness → re-measure → repeat. Do not do one big rewrite.

## Non-negotiable constraints (follow these)
- Do not add new dependencies.
- Do not do drive-by refactors or formatting.
- Preserve ACID durability and snapshot isolation semantics.
- If a change could affect **persistent formats** (db header, page layout, WAL frame format), **checkpoint/truncation strategy**, **locking/concurrency semantics**, **isolation guarantees**, or **SQL dialect behavior**: STOP and draft an ADR in design/adr/ before implementing.
- Keep changes small and test-backed.

## First fix: ensure the benchmarks are real and comparable
Before optimizing, verify the benchmark pipeline is actually measuring DecentDB vs SQLite **from the same run**.

Notes:
- The Nim benchmark runner at benchmarks/embedded_compare/run_benchmarks.nim currently only implements a DecentDB insert benchmark and leaves SQLite/DuckDB stubbed.
- The pipeline task `nimble bench_embedded_pipeline` can still succeed even if stale SQLite raw data remains in benchmarks/embedded_compare/raw/sample.

Your first deliverable is a trustworthy benchmark loop:

1) Update the runner so that it generates fresh raw results for **both DecentDB and SQLite** for the metrics used by the aggregator:
   - `insert`
   - `commit_latency`
   - `point_read`
   - `join`

2) Make the runner clear (or write into a fresh run-id subfolder of) the output directory so old raw records cannot be mixed in.

3) Ensure SQLite is configured for a fair durability comparison for commit latency (explicitly set appropriate pragmas; do not compare DecentDB durable commits against SQLite non-durable commits).

Only after you can regenerate benchmarks and see SQLite numbers coming from the current run should you start engine optimizations.

## Iteration loop (repeat until goal or diminishing returns)
Run this loop repeatedly. Each loop should be a small, isolated change.

### A) Measure
Run a fast, repeatable benchmark subset frequently:

- Build runner: `nimble bench_embedded`
- Run fresh raw output: `./build/run_benchmarks /tmp/bench_out --engines=decentdb,sqlite`
- Aggregate: `python3 benchmarks/embedded_compare/scripts/aggregate_benchmarks.py --input /tmp/bench_out --output benchmarks/embedded_compare/data/bench_summary.json`

At milestones (after a few successful iterations), run the full pipeline:

- `nimble bench_embedded_pipeline`

Record the new metrics and the relative ratios vs SQLite after every iteration.

### B) Profile (be surgical)
When a metric is far from target, profile the relevant path:

- Use `perf stat` and `perf record` on the benchmark runner.
- Use `strace -c` (or equivalent) to count syscalls and fsync/write frequency for commit latency.

Write down the top 3 hotspots for the currently-worst metric before patching.

### C) Patch (one thing at a time)
Pick the smallest change that plausibly improves the currently-worst metric.

Strong candidates that usually do NOT require format changes:

- Reduce allocations on the hot path (especially in WAL commit / frame encoding / record encoding).
- Reduce per-row string/seq churn in SQL parameter binding and value construction.
- Reduce redundant lock acquisitions while preserving the one-writer/many-readers model.
- Coalesce adjacent writes while preserving identical WAL bytes on disk.
- Avoid expensive error-string building on hot paths (keep errors actionable but cold).

If you think you need to change WAL contents, page layout, checkpoint rules, or durability semantics (e.g., “group commit”, skipping fsync, changing commit markers): STOP and write an ADR.

### D) Quick correctness gate
After each change, run the narrowest relevant tests first:

- `nimble test_nim`

If you touched WAL/pager durability-sensitive code, also run:

- `nimble test_py`

Do not continue optimizing if correctness is in doubt.

## Optimization focus order (start with the biggest gap)
1) Commit latency (currently ≈28× worse)
   - Primary suspects: syscall count (write+fsync), lock overhead, WAL frame encode allocations, CRC computation cost, redundant flushes.
2) Insert throughput
   - Primary suspects: per-row allocations (Value bytes), SQL planning overhead, btree insert path, WAL overhead.
3) Point read p95
   - Primary suspects: plan caching, page cache hit path, record decode/alloc, predicate evaluation.
4) Join p95
   - Primary suspects: join algorithm overhead, sorting/spilling, temp allocations, record materialization.

## Output requirements
Maintain a short performance journal in this file as you work (append-only):

- What changed (files + symbols)
- Why (hypothesis)
- Proof (before/after metrics and perf evidence)
- Tests run (commands)
- Next move (what you’ll do if this doesn’t move the needle)

Stop only when either:

- DecentDB is within **3×** of SQLite on all chart metrics, OR
- You’ve hit a clear wall and can justify an ADR-worthy change as the next step.
