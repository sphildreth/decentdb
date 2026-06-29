# Performance Improvements 01: Streaming Executor, Cost-Based Planning, And Durable Commit Fast Path

**Date:** 2026-06-14
**Status:** In Progress - Phase 2 deferred-view slice partially delivered
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Document Type:** Future Win SPEC
**Audience:** Core engine maintainers, planner and executor maintainers, WAL and
storage maintainers, benchmark maintainers, documentation authors, coding
agents

This document is the implementation source of truth for the first broad
performance-improvement program after the delivered default-fast and query-plan
cache work. It is intentionally prescriptive. Coding agents should implement
the work described here, not reinterpret the strategy or substitute unrelated
micro-optimizations.

**Governing ADRs:**

- [`adr/0112-cost-based-optimizer-with-stats.md`](adr/0112-cost-based-optimizer-with-stats.md)
  governs the cost-based optimizer, persisted statistics, plan annotation,
  index costing, and inner-join reordering direction.
- [`adr/0143-on-disk-row-scan-executor.md`](adr/0143-on-disk-row-scan-executor.md)
  governs the existing on-disk/deferred row-scan foundations this work must
  build on.
- [`adr/0144-persistent-primary-key-index.md`](adr/0144-persistent-primary-key-index.md)
  governs persistent primary-key lookup behavior.
- [`adr/0145-paged-table-row-source.md`](adr/0145-paged-table-row-source.md)
  governs the paged table row-source model used by deferred materialization.
- [`adr/0162-engine-owned-write-queue-strict-group-commit.md`](adr/0162-engine-owned-write-queue-strict-group-commit.md)
  governs the write queue and strict durable group-commit contract.
- [`adr/0184-default-fast-planner-and-runtime-contract.md`](adr/0184-default-fast-planner-and-runtime-contract.md)
  governs the default-fast planner/runtime contract.
- [`adr/0190-query-plan-cache-scope-key-and-lifecycle.md`](adr/0190-query-plan-cache-scope-key-and-lifecycle.md)
  through [`adr/0194-query-plan-cache-prepared-plan-reuse.md`](adr/0194-query-plan-cache-prepared-plan-reuse.md)
  govern the delivered plan-cache and prepared-plan reuse foundation that must
  keep working after this performance work.

**Required follow-up ADRs before implementation:**

- Any WAL format, WAL header, recovery-order, checkpoint semantic, or durable
  acknowledgement change.
- Any file-format/layout/version change, including new persistent row encodings
  or page formats.
- Any stable C ABI or maintained-binding cursor API that changes result
  ownership, cancellation, or lifetime semantics.
- Any new parallel execution model, worker pool, cross-thread result ownership,
  or snapshot-lifetime behavior.
- Any major dependency addition in the planner, executor, WAL, storage, or
  benchmark path.

**Related inputs:**

- [`FUTURE_WINS.md`](FUTURE_WINS.md)
- Historical metric tracker:
  [`METRIC_IMPROVEMENTS_PLAN.md`](_archive/METRIC_IMPROVEMENTS_PLAN.md)
- [`WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md`](_archive/WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md)
- [`WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md`](_archive/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md)
- [`BENCHMARKING_GUIDE.md`](BENCHMARKING_GUIDE.md)
- [`PRD.md`](PRD.md)
- [`SPEC.md`](SPEC.md)
- [`TESTING_STRATEGY.md`](TESTING_STRATEGY.md)
- [`benchmarks/rust-baseline/README.md`](../benchmarks/rust-baseline/README.md)
- [`benchmarks/targets.toml`](../benchmarks/targets.toml)
- [`docs/user-guide/performance.md`](../docs/user-guide/performance.md)
- [`data/bench_summary.json`](../data/bench_summary.json)

---

## 1. Executive Summary

DecentDB has already delivered several targeted performance wins:

- default-fast durable profiles and executor fast paths;
- prepared insert batching;
- deferred table materialization;
- persistent primary-key lookup support;
- scalar aggregate fast paths;
- indexed join and view-specific shortcuts;
- default-on connection-local plan caching and prepared-plan reuse;
- native benchmark guardrails and the larger rust-baseline comparison suite.

Those wins made the public README benchmark metrics faster than SQLite across
the balanced, low-memory, and tuned DecentDB profiles in the 2026-06-20
`data/bench_summary.json` summary. The remaining performance credibility gap is
now narrower: rust-baseline still exposes SQLite-faster view expansion/execution
paths and tiny smoke-scale fixed-overhead reads. The remaining bottleneck is
that DecentDB is fastest only when a query happens to match a narrow specialized
path. When a query falls back to the generic executor, it still tends to build
full intermediate `Dataset` values, `Vec<Vec<Value>>` row buffers, and
`Vec<QueryRow>` output buffers. It clones rows, decodes columns that may not be
needed, and keeps intermediate query state alive longer than necessary. At the
same time, the planner is still primarily structural/rule-based even though
`ANALYZE` and persisted statistics exist. Public durable commit p95 is currently
ahead of SQLite, but it remains close enough to the single-`fsync` floor that it
must stay a regression guardrail rather than an invitation to weaken durability.

This win has three implementation streams:

1. **Streaming, late-materialized executor.** Replace the most important
   generic eager materialization paths with iterator/cursor-style physical
   operators that decode only required columns, avoid unnecessary `Value`
   cloning, propagate `LIMIT`, and bound intermediate memory.
2. **Cost-based planner and first-class join/view operators.** Complete the
   accepted ADR 0112 direction so the planner consumes persisted statistics,
   annotates cost/cardinality, chooses access paths by cost, reorders inner
   joins, and emits explicit indexed/hash join and view-pushdown plan nodes
   instead of relying on ad hoc executor recognition.
3. **Durability-preserving WAL/pager commit fast path.** Reduce commit-path
   CPU, allocation, mutex, syscall, and wrapper overhead around the required
   durable sync without weakening `WalSyncMode::Full`, recovery, or crash
   guarantees.

This is a benchmark-proven win. Implementation is not complete until before
and after benchmark results show marked, significant improvements on the
public benchmark surface and the rust-baseline surface described in
[`benchmarks/rust-baseline/README.md`](../benchmarks/rust-baseline/README.md).
If the benchmark data does not show significant improvement, this effort is
not considered successful.

### 1.1 Implementation Status As Of 2026-06-29

This document is still the active implementation source of truth, but the work
is not complete. The current branch has delivered a focused Workstream A /
Phase 2 slice for deferred SQL view execution:

- cached parsed view queries in the runtime, seeded by `CREATE VIEW`;
- projected deferred reads for `v_artist_songs`-style view filter and limit
  paths, so base rows decode only the columns needed for join keys and final
  output;
- streaming deferred view joins that avoid building `Vec<Vec<StoredRow>>`
  intermediate join results;
- precomputed output projection positions and join-key projection positions
  for those view paths;
- deferred-view projected row decoding now stops after all requested columns
  are decoded, trims fetched join columns that are already proven by the
  lookup index, and uses a specialized linear three-table walker for the
  common root-to-child-to-grandchild inner-join view shape;
- a lower deferred-view limit fast-path threshold (`10_000` persisted rows)
  so smoke-scale view LIMIT queries no longer fall back to the eager generic
  executor;
- bounded top-N postprocessing for simple projection + `ORDER BY` + `LIMIT`
  shapes;
- planner `EXPLAIN` output now marks simple expanded-view filter,
  projection, and safe limit pushdown opportunities;
- passive-path bypasses in the faulty VFS wrapper, preserving failpoint
  behavior while avoiding wrapper overhead when no active failpoints apply.

The current branch has **not** completed the full WIN01 definition of done.
Remaining work still includes full smoke/medium/full/huge rust-baseline runs,
public benchmark guardrail runs, full/huge RSS proof, planner/explain
integration beyond the existing planner scaffold, durable commit-path profiling
and optimization, documentation finalization, and the final paranoid
pre-commit suite.

## 2. Product Goals

- Make DecentDB's default core engine performance less dependent on narrow
  one-off fast paths.
- Preserve the current public benchmark wins without weakening durability:
  point lookup, range scan, join, aggregate, concurrent read, insert throughput,
  and durable commit latency.
- Materially improve rust-baseline view query latency, tiny smoke-scale
  fixed-overhead reads, peak RSS, and per-step read-query memory growth while
  preserving full and huge total-runtime wins.
- Make ordinary SQL views competitive by pushing predicates, projections,
  ordering, and limits through view expansion where semantics allow it.
- Make complex joins competitive through cost-based join ordering and explicit
  join operators rather than nested-loop fallback.
- Reduce memory pressure in bindings, browser, mobile, and native Rust by
  avoiding full result/intermediate materialization when streaming is possible.
- Preserve DecentDB's durability-first identity. Performance wins must not
  skip sync, weaken WAL recovery, bypass checksums, drop policies/masks, or
  hide unsafe modes behind benchmark profiles.
- Preserve delivered plan-cache behavior and diagnostics. Faster execution
  must compose with prepared-plan reuse rather than replacing it.
- Produce benchmark artifacts that let maintainers inspect before/after
  results, per-step changes, peak RSS changes, and SQLite comparison ratios.

## 3. Non-Goals

- No durability downgrade. Do not change default `WalSyncMode::Full`, skip
  required WAL/header syncs, acknowledge commits before durable persistence, or
  rely on async commit for default benchmark wins.
- No benchmark-only behavior. Do not add code paths keyed to benchmark names,
  benchmark schemas, SQLite comparison mode, or rust-baseline dataset names.
- No new persistent format in this win unless a separate ADR is accepted first.
- No broad intra-query parallelism in this win. Parallel execution is a later
  multiplier after streaming, memory limits, cancellation, and cost modeling
  are stable.
- No materialized views or incrementally maintained projections in this win.
  Normal SQL views must become faster first.
- No broad binding rewrite. Binding performance may be measured as a guardrail,
  but this win targets the Rust core engine.
- No hidden memory growth to win CPU metrics. RSS and allocated intermediate
  buffers are first-class benchmark outputs.
- No planner hints or plan pinning in this win. The planner must improve its
  default choices using statistics and deterministic rules.
- No always-on tracing that adds hot-path overhead. Profiling instrumentation
  used during implementation must be compile-time, opt-in, or removed before
  delivery unless governed by an existing tracing ADR.

## 4. Current Baseline And Evidence

The current performance picture has two benchmark surfaces.

### 4.1 Public Benchmark Surface

The public benchmark surface is generated by:

```bash
cargo bench -p decentdb --bench embedded_compare
python scripts/aggregate_benchmarks.py
python scripts/make_readme_chart.py
python scripts/visualize_alternative.py
```

The current public summary is `data/bench_summary.json`, aggregated on
2026-06-20 from run id `1781967814749`. It shows DecentDB ahead of SQLite on
every rendered metric for balanced, low-memory, and tuned profiles. These
metrics are no longer the primary gap list; they are guardrails that must not
regress:

| Metric | Required Direction |
|---|---|
| `insert_rows_per_sec` | Increase |
| `read_p95_ms` | Decrease |
| `range_scan_p95_ms` | Decrease |
| `join_p95_ms` | Decrease |
| `commit_p95_ms` | Decrease without weakening durability |
| `aggregate_p95_ms` | Decrease or hold already-large win |
| `concurrent_read_p95_ms` | Decrease |

This spec treats all seven metrics as active guardrails. A change that improves
rust-baseline view paths or tiny smoke-scale reads by regressing a public
headline metric is not complete.

### 4.2 Rust-Baseline Surface

The rust-baseline benchmark is documented in
[`benchmarks/rust-baseline/README.md`](../benchmarks/rust-baseline/README.md).
It is the apples-to-apples native Rust comparison against SQLite using the
music-library workload. It exercises:

- schema creation;
- bulk seed loops;
- checkpoint after seed;
- `COUNT(*)`;
- aggregate duration queries;
- point lookup by artist id;
- grouped Top-N queries;
- `v_artist_songs` view scans;
- `songs for artist via view` lookup paths;
- smoke, medium, full, and huge scale factors;
- peak RSS and per-step duration reporting.

The README explicitly notes that query timing materializes every returned
column before counting rows and that peak RSS can climb sharply during query
evaluation. This is direct evidence that eager intermediate materialization is
a core-engine performance and memory bottleneck visible to every binding.

The current in-progress diagnostic comparison uses the pre-work DecentDB run in
`.tmp/perf01-before-rust-baseline/results` (`2026-06-28-2256`), the latest
current-branch DecentDB smoke/medium run in
`.tmp/perf01-after-view-linear-walker-smoke-medium/results`
(`2026-06-29-0153`), and the paired SQLite smoke/medium reference in the same
directory.

| Scale | Metric | Before DecentDB | Current DecentDB | Current SQLite | Current DecentDB / SQLite | Status |
|---|---:|---:|---:|---:|---:|---|
| smoke | `query_view_first_1000` | 0.003272s | 0.000545s | 0.000367s | 1.48x slower | Improved 6.0x, still SQLite-faster |
| smoke | `query_songs_for_artist_via_view` | 0.000285s | 0.000146s | 0.000055s | 2.65x slower | Improved 2.0x, still SQLite-faster |
| smoke | `query_artist_by_id` | 0.000025s | 0.000032s | 0.000023s | 1.39x slower | Small fixed-overhead loss remains |
| medium | `query_view_first_1000` | 0.002089s | 0.000859s | 0.000389s | 2.21x slower | Improved 2.4x, still SQLite-faster |
| medium | `query_songs_for_artist_via_view` | 0.000340s | 0.000150s | 0.000071s | 2.11x slower | Improved 2.3x, still SQLite-faster |
| medium | `query_artist_by_id` | 0.000035s | 0.000033s | 0.000050s | 0.66x faster | DecentDB faster than SQLite in this run |

The largest focused win in this slice came from routing smoke-scale view LIMIT
queries through the deferred-view fast path instead of the eager generic
executor: a threshold experiment improved smoke `query_view_first_1000` from
0.003344s to 0.000724s. Subsequent projected-decode trimming and the linear
deferred-view walker brought the latest smoke run to 0.000545s, still well
ahead of the original baseline but not yet ahead of SQLite.

Full and huge rust-baseline reruns are still pending for this win. Until those
complete, the previous checked-in full/huge comparisons remain historical
diagnostics rather than accepted current evidence.

### 4.3 Code Hotspots That Must Be Addressed

The following code areas are the starting points. Coding agents should inspect
these first and keep changes scoped to them unless profiling proves another
hotspot.

| Area | Current Shape | Required Direction |
|---|---|---|
| `crates/decentdb/src/exec/mod.rs` generic datasets | Many paths build `Dataset::with_rows`, `Vec<Vec<Value>>`, and `Vec<QueryRow>` intermediates | Introduce streaming/late-materialized operators for the measured hot paths |
| `crates/decentdb/src/exec/mod.rs` joins | Specialized indexed/hash-like join helpers exist, but generic join still materializes full sides and outputs | Promote useful join strategies into planner-owned physical nodes |
| `crates/decentdb/src/exec/mod.rs` views | View-specific fast paths exist but are shape-dependent | Push filters/projections/limits/order through view expansion in a general planned way |
| `crates/decentdb/src/planner/mod.rs` | Estimate and join/view plan scaffolding exists, but execution still relies heavily on executor recognition | Complete ADR 0112 cost-driven access-path, join-order, and view-pushdown choices and wire them to execution paths |
| `crates/decentdb/src/planner/physical.rs` | `estRows`/`estCost`, `HashJoin`, `IndexedJoin`, `StreamingAggregate`, and view nodes exist | Make these planned operators authoritative for execution and add stronger `EXPLAIN`/path-selection coverage |
| WAL/pager commit path | Durable commit p95 is at the fsync floor plus engine overhead | Remove avoidable overhead while preserving durability |

## 5. Required Benchmark Protocol

Every implementation phase must produce before/after benchmark evidence. Use
`.tmp/` for temporary result directories.

### 5.1 Mandatory Before Baseline

Before changing code for this win, record the baseline in `.tmp/`:

```bash
cargo bench -p decentdb --bench embedded_compare

cd benchmarks/rust-baseline
cargo build --release
OUT="$PWD/../../.tmp/perf01-before-rust-baseline/results"
mkdir -p "$OUT"
./target/release/rust-baseline --engine decentdb --benchmark --out-dir "$OUT"
./target/release/rust-baseline \
  --engine sqlite \
  --benchmark \
  --out-dir "$OUT" \
  --report-file "$OUT/report.html"
```

If full `--benchmark` is too slow during early iteration, agents may use smoke
and medium for local profiling, but the work cannot be marked complete without
the full smoke/medium/full/huge suite.

### 5.2 Mandatory After Runs

After each meaningful implementation slice, run:

```bash
cargo bench -p decentdb --bench embedded_compare

cd benchmarks/rust-baseline
cargo build --release
OUT="$PWD/../../.tmp/perf01-after-rust-baseline/results"
mkdir -p "$OUT"
./target/release/rust-baseline --engine decentdb --benchmark --out-dir "$OUT"
./target/release/rust-baseline \
  --engine sqlite \
  --benchmark \
  --out-dir "$OUT" \
  --report-file "$OUT/report.html"
```

For executor and planner work, also run focused single-scale loops while
profiling:

```bash
cd benchmarks/rust-baseline
./target/release/rust-baseline --engine decentdb --scale smoke --out-dir ../../.tmp/perf01-smoke
./target/release/rust-baseline --engine decentdb --scale full --out-dir ../../.tmp/perf01-full
./target/release/rust-baseline --engine sqlite --scale full --out-dir ../../.tmp/perf01-full
```

### 5.3 Benchmark Report Requirements

The final implementation report must include:

- command lines used;
- machine/profile notes;
- before and after public benchmark table;
- before and after rust-baseline smoke/medium/full/huge total runtime;
- before and after rust-baseline per-step timings for:
  - `query_artist_by_id`;
  - `query_view_first_1000`;
  - `query_songs_for_artist_via_view`;
  - `query_top10_artists_by_songs`;
  - `query_top10_albums_by_songs`;
  - `query_aggregate_durations`;
  - `checkpoint_after_seed`;
  - `seed_songs`;
- before and after peak RSS for each rust-baseline scale;
- DecentDB-vs-SQLite ratios for every reported row;
- explicit list of regressions, even when the headline metric improves.

## 6. Definition Of Significant Improvement

This win is only successful if the after benchmarks show marked improvement.
The following thresholds define "marked/significant" for this document.

### 6.1 Whole-Program Gates

All of these must be true:

- Public benchmark headline metrics show no regression greater than 3% against
  the recorded before baseline.
- Balanced, low-memory, and tuned DecentDB remain at or above SQLite on every
  public headline metric, or any exception is explicitly documented as
  measurement noise with a follow-up task.
- `commit_p95_ms` remains at or above SQLite under `WalSyncMode::Full`, or the
  final report proves with syscall-level profiling that any remaining gap is
  the durable sync floor and that non-sync engine overhead did not regress.
- Rust-baseline default DecentDB total runtime improves by at least:
  - 5% on smoke;
  - 10% on medium;
  - 15% on full;
  - 15% on huge.
- Rust-baseline full and huge peak RSS decrease by at least 25%.
- Rust-baseline DecentDB remains faster than SQLite in total runtime at every
  scale.
- Rust-baseline view-path losses are materially reduced, and the tiny
  smoke-scale `query_artist_by_id` / `query_count_songs` fixed-overhead losses
  are either eliminated or explicitly profiled with a bounded follow-up.
- No correctness, crash-recovery, or durability test is weakened, skipped, or
  reclassified to pass the performance work.

### 6.2 Stream-Specific Gates

Each implementation stream has its own gates.

**Streaming executor gates:**

- Full and huge rust-baseline peak RSS decrease by at least 25%.
- `query_top10_artists_by_songs`, `query_top10_albums_by_songs`, and
  `query_aggregate_durations` collectively improve by at least 15% geometric
  mean on full and huge.
- Public `range_scan_p95_ms`, `aggregate_p95_ms`, and
  `concurrent_read_p95_ms` do not regress; at least two improve by 10% or more.

**Cost-based planner and join/view gates:**

- `query_view_first_1000` and `query_songs_for_artist_via_view` improve by at
  least 2x on full and huge, unless the before baseline already beats SQLite
  by at least 1.25x. If the before baseline already beats SQLite by at least
  1.25x, each view step must still improve by at least 15%.
- Public `join_p95_ms` and `range_scan_p95_ms` improve by at least 10% or
  remain within 3% while rust-baseline view paths improve by the required 2x.
- `EXPLAIN` for affected queries shows the intended planned operator instead
  of hiding the improvement in an executor-only shortcut.

**Durable commit gates:**

- Public `commit_p95_ms` stays at or above SQLite, or meets the documented
  durable-sync-floor exception in §6.1.
- `insert_rows_per_sec` does not regress.
- Crash/recovery tests prove the same committed/uncommitted visibility and WAL
  replay semantics as before the change.
- No benchmark profile uses weaker durability to claim the win.

If these thresholds are not met, the work is incomplete. Agents must either
continue optimizing, revert the ineffective slice, or document why the slice
should be abandoned.

## 7. Workstream A: Streaming, Late-Materialized Executor

### 7.1 Problem Statement

The executor contains many efficient special cases, but the generic path still
uses eager materialization. Typical problematic shapes include:

- building a `Dataset` with all input rows before filtering or projecting;
- cloning `Vec<Value>` rows for filter evaluation;
- decoding all columns even when only one projected column is needed;
- building right-side join vectors and join-output vectors before applying
  final projection, `ORDER BY`, `LIMIT`, or `DISTINCT`;
- returning `Vec<QueryRow>` even when an internal operator only needs to count,
  aggregate, or feed another operator;
- keeping query intermediate buffers alive until the `Db` or runtime is
  dropped.

This produces avoidable CPU work, allocation pressure, and RSS growth.

### 7.2 Required Design

Implement an internal streaming execution layer that can be used by the
existing result materialization API. Public APIs may continue returning
`QueryResult` in this phase, but internal execution must stop eagerly building
full intermediate datasets for the measured hot paths.

The internal model must have these concepts:

| Concept | Required Behavior |
|---|---|
| Row source | Pulls rows one at a time from resident tables, deferred paged row sources, row-id sets, or index scans |
| Row view | Represents a row without cloning every `Value`; may borrow from resident row storage or own decoded deferred values |
| Projection map | Lists exactly which columns/expressions are needed by downstream operators |
| Predicate operator | Evaluates filters against a row view without materializing unneeded output columns |
| Project operator | Builds output values only after filters, joins, and limits have accepted a row |
| Limit operator | Stops upstream iteration as soon as enough rows are produced when ordering/distinct semantics allow it |
| Aggregate operator | Accumulates directly from row views without storing all input rows |
| Sort/hash buffer | Owns only necessary keys and payload values, with explicit memory accounting |

The first implementation does not need to expose a public cursor API. It must
be structured so a future public cursor API can reuse it.

### 7.3 Required Implementation Order

Implement in this order:

1. Add an internal `RowView`/`ExecRow` representation that can expose values by
   column index without cloning the full row. It must support:
   - resident table rows;
   - deferred stored rows;
   - projected deferred values;
   - joined rows composed from left and right row views.
2. Add projection pruning for simple table/index scans. A scan must know which
   physical columns are required by:
   - filters;
   - join keys;
   - grouping keys;
   - aggregate arguments;
   - order keys;
   - final projection.
3. Convert simple filtered projection and row-id/range projection paths to use
   the new row-view layer.
4. Convert scalar and grouped aggregate fast paths to consume row views and
   avoid building `Dataset` inputs.
5. Convert view limit/filter paths that currently match rust-baseline view
   queries to consume streaming rows and push `LIMIT` as far down as semantics
   allow.
6. Convert generic join fallback only after the simple scan/projection and
   aggregate paths are stable.

Do not begin with a broad rewrite of every executor path. Start with the
measured rust-baseline and public benchmark hot paths.

### 7.4 Exact Behavioral Requirements

- Query results must remain byte-for-byte equivalent at the SQL value level.
- Column names, aliases, wildcard expansion, `USING`/`NATURAL` join output
  shape, and projection mask behavior must not change.
- Row policies and projection masks must still run before unauthorized values
  can be returned or used in unauthorized ways.
- `ORDER BY`, `DISTINCT`, `DISTINCT ON`, `GROUP BY`, `HAVING`, `LIMIT`, and
  `OFFSET` must preserve current SQL semantics.
- Deferred table reads must stay snapshot-consistent with the active reader.
- TDE and compressed overflow/deferred payload handling must remain unchanged.
- The new internal row-view layer must not introduce dangling references. If a
  row cannot be safely borrowed for the required lifetime, own that row's
  minimal projected values.
- Memory accounting must be explicit for any operator that can buffer rows.

### 7.5 Tests Required

Add or update tests for:

- simple filtered projection uses streaming row-view path;
- row-id lookup with partial projection decodes only requested columns;
- range scan with `LIMIT` stops early;
- grouped aggregate consumes rows without materializing a `Dataset`;
- view projection with `LIMIT` pushes limit through the view join chain;
- view filter by root table key uses streaming/indexed prefilter;
- row policies/masks still apply on streaming paths;
- deferred paged/compressed rows return the same values as the old path;
- `EXPLAIN ANALYZE` actual row counts remain correct for converted paths.

Tests must assert both results and plan/path selection where the engine exposes
that path.

## 8. Workstream B: Cost-Based Planner And First-Class Join/View Operators

### 8.1 Problem Statement

ADR 0112 is accepted, and DecentDB can persist table/index statistics through
`ANALYZE`. The planner still mostly builds structural plans through hardcoded
rules. The executor contains several high-value join and view shortcuts, but
those shortcuts are not first-class physical plan nodes and are not chosen by a
cost model.

This creates three problems:

- nearby query shapes can fall off a fast path and become much slower;
- `EXPLAIN` does not fully reveal the operator that made a query fast;
- benchmark wins are fragile because the planner is not comparing alternatives.

### 8.2 Required Physical Plan Changes

Extend `crates/decentdb/src/planner/physical.rs` with first-class estimated
cardinality and cost.

Use this structure unless a smaller equivalent local pattern already exists:

```rust
pub(crate) struct PlanEstimate {
    pub(crate) rows: u64,
    pub(crate) cost: f64,
}
```

Every `PhysicalPlan` node must either carry a `PlanEstimate` directly or expose
one through a side table produced by the planner. Prefer direct node
annotation if it keeps `EXPLAIN` simple and does not create excessive enum
churn.

Add explicit physical node variants for:

- `HashJoin`;
- `IndexedJoin`;
- `ViewScan` or `ExpandedView` with pushed predicates/projections/limits;
- `StreamingAggregate` if the executor uses a distinct streaming aggregate
  path;
- `StreamingProject` only if it is useful to distinguish from existing
  `Project` in `EXPLAIN`.

Do not remove existing `NestedLoopJoin`. It remains the fallback for join
shapes that cannot safely use an indexed or hash strategy.

### 8.3 Required Cost Model

Implement the ADR 0112 cost model, with these concrete rules:

- Table scan rows:
  - use persisted table row count when available;
  - otherwise use `1000`.
- Index equality selectivity:
  - use `1 / distinct_key_count` when index stats exist and
    `distinct_key_count > 0`;
  - otherwise use `0.10`.
- Range selectivity:
  - use `0.30` unless richer stats exist.
- Conjunction selectivity:
  - multiply selectivities.
- Disjunction selectivity:
  - `sel_a + sel_b - sel_a * sel_b`, capped at `1.0`.
- Limit:
  - reduce estimated rows to `min(limit, input_rows)` when limit is a constant.
- Join:
  - inner equi-join cardinality must account for available distinct-key stats;
  - otherwise use the ADR heuristic.

Cost units are relative and only need to be stable enough to compare plan
alternatives. They must be deterministic for the same catalog/statistics state.

### 8.4 Required Planner Choices

The planner must make these decisions by cost:

1. Table scan vs row-id lookup vs B+Tree index seek vs covering index seek.
2. Outer/input side for indexed joins when both orientations are legal.
3. Hash join vs indexed join vs nested loop for inner equi-joins.
4. Inner join ordering for left-deep plans up to six inner-join inputs.
5. Predicate pushdown into expanded views.
6. Projection pruning through expanded views.
7. Limit pushdown through views when no `ORDER BY`, `DISTINCT`, aggregate, or
   outer join semantics prevent it.

For more than six inner-join inputs, use a deterministic greedy ordering by
estimated cardinality and then join cost. Do not attempt a bushy-plan search in
this win.

### 8.5 View Expansion Requirements

Normal SQL views are in scope. Materialized views and incrementally maintained
projections are out of scope.

For simple views over table joins, the planner must:

- expand the view into a relational subtree;
- bind outer predicates to the correct view output expressions;
- push predicates to base tables when the predicate references a single base
  table expression and the pushdown is semantics-preserving;
- push projection requirements so base scans decode only needed columns;
- push constant `LIMIT` when there is no semantic blocker;
- preserve aliases and output column names;
- preserve row policy, mask, temp schema, CTE, and branch visibility behavior.

The rust-baseline `v_artist_songs` view is the required first target shape.
The implementation must not hardcode `artists`, `albums`, `songs`, or
`v_artist_songs`.

### 8.6 EXPLAIN Requirements

`EXPLAIN` must show the new planned operators and estimates.

Required examples:

```text
HashJoin(kind=Inner, estRows=..., estCost=..., on=...)
IndexedJoin(kind=Inner, estRows=..., estCost=..., index=...)
ExpandedView(name=v_artist_songs, pushedFilter=..., pushedLimit=...)
```

`EXPLAIN ANALYZE` must continue reporting actual row counts and execution time.
When estimates are present, the output must make estimated vs actual rows
visible enough for Doctor/advisor work to consume later.

### 8.7 Tests Required

Add or update tests for:

- `ANALYZE` stats are consumed by planner estimates;
- missing stats fall back deterministically;
- selective predicate chooses index seek;
- unselective predicate chooses scan when scan is cheaper;
- two-table inner join chooses indexed join when a useful index exists;
- two-table inner join chooses hash join when no useful index exists;
- three-table inner join reorders to the lower-cost left-deep order;
- left/full/right joins are not illegally reordered;
- view predicate pushdown preserves results;
- view limit pushdown preserves results;
- `EXPLAIN` contains estimates and selected join/view operators;
- plan cache invalidates or refuses stale plans when `ANALYZE` changes
  statistics generation.

## 9. Workstream C: Durability-Preserving WAL/Pager Commit Fast Path

### 9.1 Problem Statement

Durable commit latency is close to SQLite and often dominated by the host
filesystem's sync behavior. That does not mean DecentDB should stop improving
the commit path. The goal is to remove avoidable engine overhead around the
required durable sync so DecentDB is clearly better when there is overhead to
remove and no worse when both engines are at the same sync floor.

### 9.2 Required Profiling First

Before editing WAL/pager code, collect a syscall and CPU profile for durable
single-row commit and batched commit.

Required profiling artifacts:

- total commit time;
- time spent in durable sync calls;
- number of `write`, `pwrite`, `fdatasync`, `fsync`, lock, and metadata syscalls
  per commit;
- allocations per commit if available;
- mutex/lock acquisition counts on the hot path if available.

Use `.tmp/perf01-commit-profile/` for generated artifacts.

### 9.3 Allowed Optimizations Without New ADR

The following are allowed if tests prove identical durability semantics:

- remove no-op checks from the hot path when the configured feature is absent;
- avoid failpoint registry lookups in non-failpoint builds or inactive paths;
- reuse commit buffers to avoid per-commit allocation;
- reduce redundant length/header calculations;
- reduce duplicate page-cache dirty-list walks;
- collapse adjacent in-memory copies before the same durable write;
- avoid reactive/sync/metrics work when there are no subscribers or enabled
  sinks;
- specialize single-row auto-commit and explicit transaction commit paths when
  the specialization preserves identical WAL records and sync order;
- improve VFS wrapper dispatch where it is measurably expensive.

### 9.4 ADR-Required Changes

Do not implement any of these without a new ADR:

- changing WAL record format;
- changing WAL header end-offset semantics;
- changing checkpoint recovery order;
- changing when a commit is acknowledged relative to durable sync;
- replacing full sync with normal sync or no sync;
- adding a persistent group-commit metadata format;
- changing cross-process lock semantics;
- changing crash recovery behavior;
- changing file format version.

### 9.5 Required Tests

Add or update tests for:

- single-row auto-commit remains durable after reopen;
- explicit transaction commit remains durable after reopen;
- rollback remains invisible after reopen;
- crash/failpoint recovery around WAL append, WAL header update, and checkpoint
  still produces the same result as before;
- group commit remains strict: every acknowledged caller has durable data;
- cross-process coordination tests still pass;
- TDE-enabled commits still recover correctly.

### 9.6 Benchmark Requirements

The commit fast path must be measured with:

- public `commit_p95_ms`;
- public `insert_rows_per_sec`;
- benchmark target scenarios in `benchmarks/targets.toml` where available:
  - `durable_commit_single.txn_p95_us`;
  - `durable_commit_single.commit_p95_us`;
  - `durable_commit_batch.rows_per_sec`;
  - `durable_commit_batch.batch_commit_p95_us`;
- any existing write-queue/group-commit benchmarks.

## 10. Implementation Phases

### Current Phase Status

As of 2026-06-29:

| Phase | Status | Notes |
|---|---|---|
| Phase 0: Baseline And Profiling | Partial | Smoke/medium before/after artifacts exist in `.tmp/perf01-*`; full/huge and public benchmark baselines still need a clean final run. |
| Phase 1: Internal Row Views And Projection Pruning | Partial | Several projected deferred read helpers exist, but there is not yet a general `RowView`/`ExecRow` layer across simple scans, aggregates, and joins. |
| Phase 2: Streaming Aggregates And View Hot Paths | Partial | Deferred view filter/LIMIT paths now use projected reads, trimmed projection sets, linear-chain streaming joins, and bounded grouped Top-N postprocessing; the view paths still trail SQLite and not all view/grouped shapes are complete. |
| Phase 3: Cost-Based Planner And Join Operators | Partial | Planner estimate/operator scaffolding exists and `EXPLAIN` now exposes simple expanded-view pushdown metadata; cost-based execution selection and broader path-selection tests remain. |
| Phase 4: Commit Fast Path | Partial | Faulty VFS passive-path overhead was reduced; required syscall/CPU profiling and commit benchmark proof remain. |
| Phase 5: Final Benchmark And Documentation Sweep | Not complete | This status update and changelog entry are in progress; full final benchmark report and paranoid pre-commit are still required. |

### Phase 0: Baseline And Profiling

Required outputs:

- `.tmp/perf01-before-rust-baseline/results/report.html`;
- before public benchmark table;
- profile notes identifying top executor allocations/clones;
- profile notes identifying commit sync vs non-sync time;
- list of exact query shapes that miss fast paths.

No code changes count toward completion until Phase 0 artifacts exist.

### Phase 1: Internal Row Views And Projection Pruning

Implement:

- `RowView`/`ExecRow` abstraction;
- projection requirement analysis for simple scans;
- partial decode for row-id lookup and range scan;
- streaming simple filtered projection;
- early limit for simple unordered scans.

Benchmarks expected to improve:

- public `read_p95_ms`;
- public `range_scan_p95_ms`;
- rust-baseline `query_artist_by_id`;
- rust-baseline peak RSS at smoke/medium;
- no regression in insert or commit.

### Phase 2: Streaming Aggregates And View Hot Paths

Implement:

- streaming scalar aggregates;
- streaming grouped aggregates for the rust-baseline Top-N shapes;
- view projection/filter/limit pushdown for the `v_artist_songs`-style shape;
- bounded buffers for sort/group state.

Benchmarks expected to improve:

- public `aggregate_p95_ms`;
- public `concurrent_read_p95_ms`;
- rust-baseline `query_aggregate_durations`;
- rust-baseline `query_top10_artists_by_songs`;
- rust-baseline `query_top10_albums_by_songs`;
- rust-baseline peak RSS full/huge.

### Phase 3: Cost-Based Planner And Join Operators

Implement:

- estimates on physical plans;
- stats-consuming cost model;
- cost-based scan/index choice;
- cost-based inner join order;
- first-class `IndexedJoin` and `HashJoin`;
- `EXPLAIN` estimates and operator names.

Benchmarks expected to improve:

- public `join_p95_ms`;
- public `range_scan_p95_ms`;
- rust-baseline view queries;
- complex join microbenchmarks added with this phase.

### Phase 4: Commit Fast Path

Implement only optimizations allowed by §9.3 unless a new ADR has already been
accepted.

Benchmarks expected to improve:

- public `commit_p95_ms`;
- public `insert_rows_per_sec`;
- durable commit target scenarios.

### Phase 5: Final Benchmark And Documentation Sweep

Required outputs:

- final before/after public benchmark table;
- final before/after rust-baseline report;
- updated current-baseline and active-gap sections in this spec with accepted
  numbers;
- updated `FUTURE_WINS.md` status if this win is added to the roadmap;
- updated `docs/user-guide/performance.md` only for user-visible behavior or
  new configuration;
- changelog entry when implementation lands.

## 11. Quality Gates

Run the smallest relevant checks while iterating, then the full suite before
completion.

Minimum iteration checks:

```bash
cargo fmt --check
cargo check -p decentdb
cargo clippy -p decentdb --all-targets --all-features -- -D warnings
cargo test -p decentdb -- planner
cargo test -p decentdb -- fast_path
cargo test -p decentdb -- join
cargo test -p decentdb -- deferred
cargo test -p decentdb -- wal
```

Final required checks:

```bash
cargo fmt --check
cargo check -p decentdb
cargo lint
cargo test-all
python scripts/do-pre-commit-checks.py --mode paranoid
```

If a binding, C ABI, WASM, browser, mobile, or docs surface changes, run the
corresponding checks from `scripts/do-pre-commit-checks.py --list`.

## 12. Agent Implementation Rules

Coding agents implementing this spec must follow these rules:

- Start with benchmark baselines. Do not guess.
- Keep each phase independently measurable.
- Prefer replacing generic eager materialization with reusable streaming
  infrastructure over adding another query-name-specific shortcut.
- Do not hardcode rust-baseline schema names, table names, index names, or SQL
  strings.
- Do not regress durability to win `commit_p95_ms`.
- Do not hide regressions by updating baselines without explanation.
- Do not remove existing fast paths until the new planned/streaming path is
  faster and covered by tests.
- Keep old and new paths side-by-side during migration when that makes
  correctness easier to prove.
- Add `EXPLAIN` coverage for planner changes so future agents can see which
  path is active.
- Treat memory as a performance metric. A CPU improvement that materially
  increases RSS is not accepted without an explicit tradeoff in the final
  report.

## 13. Definition Of Done

This win is complete only when all of the following are true:

- Phase 0 through Phase 5 are complete.
- The final benchmark report includes before/after data from the public
  benchmark surface and from rust-baseline as documented in
  [`benchmarks/rust-baseline/README.md`](../benchmarks/rust-baseline/README.md).
- The significant-improvement thresholds in §6 are met.
- DecentDB remains faster than SQLite in rust-baseline total runtime at every
  scale.
- Rust-baseline view paths are materially improved and no longer represent an
  obvious credibility gap.
- Full and huge rust-baseline peak RSS are materially lower.
- Public `commit_p95_ms` is improved or proven to be sync-floor-bound with
  reduced non-sync engine overhead.
- Public read-side benchmark metrics improve or remain within the no-regression
  threshold while the benchmark program as a whole improves.
- All new planner/executor behavior is covered by correctness tests and
  `EXPLAIN`/path-selection tests where applicable.
- Crash/recovery and durability tests pass after commit-path changes.
- `python scripts/do-pre-commit-checks.py --mode paranoid` passes.
- Documentation and changelog entries are updated when implementation lands.

If the required marked/significant before/after benchmark improvements are not
present, the work is not done even if the code is cleaner or the architecture
looks better.
