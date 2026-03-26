# Improve DecentDB Benchmark Leadership

You are working in the DecentDB repository.

Your goal is to improve DecentDB's benchmark results in the Python embedded comparison harness and work iteratively until DecentDB leads every benchmark category if that can be achieved without compromising correctness, durability, API stability, or fairness.

## Current benchmark picture

Use [docs/user-guide/benchmarks.md](../../docs/user-guide/benchmarks.md) as the starting reference for the current standings.

At the current `500`-operation sweep point, DecentDB is:

- Leading:
  - `workload_c / full_scan`
- Competitive but not leading:
  - `workload_c / point_select`
  - `workload_a / point_select`
- Trailing:
  - `workload_a / aggregate`
  - `workload_a / join`
  - `workload_a / range_scan`
  - `workload_a / delete`
  - `workload_a / update`

Treat those lagging and near-leading categories as the prioritized target list.

## Hard constraints

Do not game the benchmark.

You must preserve:

- ACID and durable-write expectations
- correctness of query results
- fairness of the comparison harness
- documented SQL behavior
- stable public APIs and bindings unless explicitly required

Do not:

- special-case benchmark SQL in the engine
- alter the benchmark workload to make DecentDB look better unless the change is fairness-driven, documented, and applied consistently to all engines
- disable durability, safety, or correctness checks just to win a benchmark
- hide regressions in non-targeted benchmarks

## Expected workflow

Work in tight iterations.

For each iteration:

1. Re-read relevant benchmark data and identify the worst DecentDB gap.
2. Trace likely root cause in engine code, planner, executor, storage, indexing, pager, WAL, or bindings overhead.
3. Form a concrete hypothesis for why DecentDB is behind.
4. Make the smallest plausible code change to test that hypothesis.
5. Run targeted validation first:
   - `cargo check`
   - `cargo clippy`
   - smallest relevant Rust tests
   - any directly impacted binding or harness smoke tests
6. Run quick validation with `bench_complex.py` for immediate feedback across all metric types before running full comparison framework.
7. Run the narrowest benchmark slice that proves or disproves change.
8. Record before/after outcome.
9. Decide whether to keep iterating on the same bottleneck or move to the next one.

Prefer several small, validated improvements over a broad speculative rewrite.

## Prioritization guidance

Start with the highest-leverage deficits:

1. `workload_a / update`
2. `workload_a / delete`
3. `workload_a / range_scan`
4. `workload_a / join`
5. `workload_a / aggregate`
6. `workload_c / point_select`
7. `workload_a / point_select`

Reasoning:

- `update` and `delete` are the worst current standings.
- `range_scan`, `join`, and `aggregate` likely expose deeper planner, index, or executor limitations.
- `point_select` is already close enough that smaller improvements may flip the lead.

## Quick validation approach

Before running the full comparison framework, use `bench_complex.py` for rapid iteration feedback:

- **Purpose**: Single-script comprehensive benchmark that covers all major metric types tested in python_embedded_compare
- **Coverage**: Point lookups, range scans, joins, aggregates, updates, deletes, and full table scans
- **Advantages**: Faster execution, simpler setup, immediate feedback on multiple metrics
- **Relationship**: If DecentDB leads in all `bench_complex.py` metrics, it is highly likely to be the leader in the full comparison framework

Use `bench_complex.py` as your primary validation tool during optimization iterations. Run the full python_embedded_compare framework less frequently (e.g., when major milestones are reached or for final validation).

## Files and areas to inspect first

Benchmark and docs context:

- [docs/user-guide/benchmarks.md](../../docs/user-guide/benchmarks.md)
- `bindings/python/benchmarks/bench_complex.py` - Comprehensive single-script benchmark covering all major metric types
- `benchmarks/python_embedded_compare/README.md`
- `benchmarks/python_embedded_compare/comparison_runner.py`
- `benchmarks/python_embedded_compare/scenarios/canonical_workloads.py`
- `benchmarks/python_embedded_compare/out/workload-a-all-engines/charts/chart_data.json`
- `benchmarks/python_embedded_compare/out/workload-c-all-engines/charts/chart_data.json`

Likely engine hot paths:

- `crates/decentdb/src/`
- planner / executor code
- storage / pager / WAL code
- B+Tree and index lookup paths
- update and delete execution paths
- range-scan and join execution paths

## Benchmark commands

Use targeted runs before full sweeps.

**Quick validation with bench_complex.py:**

For rapid feedback during iteration, use the comprehensive bench_complex.py script which tests all major metric categories in a single run:

```bash
python bindings/python/benchmarks/bench_complex.py \
  --engine sqlite,decentdb \
  --users 1000 --items 200 --orders 1000 \
  --point-lookups 1000 --range-scans 1000 --joins 1000 \
  --aggregates 1000 --updates 500 --deletes 500 --table-scans 100
```

This script tests point lookups, range scans, joins, aggregates, updates, deletes, and table scans. If DecentDB leads in all these metrics, it is highly likely to be the leader in the full comparison framework.

**Full comparison runner:**

```bash
/home/steven/source/decentdb/.venv-mkdocs/bin/python /home/steven/source/decentdb/benchmarks/python_embedded_compare/comparison_runner.py \
  --config /home/steven/source/decentdb/benchmarks/python_embedded_compare/config/database_configs.yaml \
  --engines sqlite,decentdb \
  --workload workload_a \
  --customers 100 --orders 500 --events 500 \
  --ops-list 10,50,100,250,500 \
  --warmup 10 \
  --output /home/steven/source/decentdb/benchmarks/python_embedded_compare/out/workload-a-head-to-head
```

When a change looks promising, rerun the broader comparison set:

```bash
/home/steven/source/decentdb/.venv-mkdocs/bin/python /home/steven/source/decentdb/benchmarks/python_embedded_compare/comparison_runner.py \
  --config /home/steven/source/decentdb/benchmarks/python_embedded_compare/config/database_configs.yaml \
  --engines sqlite,duckdb,decentdb,h2,derby,hsqldb \
  --workload workload_a \
  --customers 100 --orders 500 --events 500 \
  --ops-list 10,50,100,250,500 \
  --warmup 10 \
  --output /home/steven/source/decentdb/benchmarks/python_embedded_compare/out/workload-a-all-engines
```

And similarly for `workload_c`.

## Validation expectations

Every accepted change should include:

- passing `cargo check`
- passing `cargo clippy`
- relevant unit or integration coverage if behavior changes
- targeted `bench_complex.py` rerun showing improvement across relevant metric types
- selective python_embedded_compare framework rerun for final validation
- notes on regressions, if any

If you discover that a benchmark gap is caused by a genuine architectural limitation, say so plainly and explain the tradeoff rather than masking it.

## Output format for each iteration

When you report back, include:

1. The benchmark target you worked on
2. The root-cause hypothesis
3. The code changed
4. `bench_complex.py` validation run results (before/after comparison)
5. Selective python_embedded_compare framework validation (if applicable)
6. Before/after ranking or latency deltas
7. Whether change should be kept, revised, or reverted
8. The next highest-value target

## Definition of success

Success means moving DecentDB toward leadership across all benchmark categories without violating correctness, durability, or fairness.

- **Quick validation**: Achieve leadership in all metrics tested by `bench_complex.py` (point lookups, range scans, joins, aggregates, updates, deletes, and table scans)
- **Full validation**: Achieve leadership in all python_embedded_compare framework workloads (workload_a, workload_b, workload_c)

Use `bench_complex.py` as the primary indicator of progress. When DecentDB leads in all `bench_complex.py` metrics, it is highly likely to be the leader in the full comparison framework.

If full leadership is not yet achieved, continue iteratively on the highest-value remaining gap rather than stopping after one improvement.