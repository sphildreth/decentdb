# Agent Benchmark Loop

**Date:** 2026-03-30  
**Status:** Active workflow guidance  
**Audience:** engine maintainers, performance engineers, coding agents

## 1. Purpose

This document defines the standard benchmark-driven optimization loop for coding
agents working in the DecentDB repository.

The goal is to let agents work quickly against the Rust-native benchmark harness
while still improving the real product-facing metrics that matter most:

- durable write performance
- read latency and throughput
- storage efficiency
- complex query performance
- Python `bindings/python/benchmarks/bench_complex.py` outcomes by inheritance

This workflow exists because the Rust benchmark harness is much faster to run
iteratively than the full Python benchmark suite, while still exercising the
same engine behaviors. In particular, the `complex_ecommerce` benchmark scenario
in `crates/decentdb-benchmark` mirrors the workload family and primary metric
names used by `bindings/python/benchmarks/bench_complex.py`.

This means:

- **`smoke`** is for very fast correctness/perf sanity checks while editing
- **`dev`** is the main iterative optimization loop for coding agents
- **`nightly`** is the authoritative recurring benchmark gate

The critical rule is:

- **Agents optimize against `release` builds with `dev` scale**
- **Only `nightly` should be treated as authoritative for final color and release claims**

## 2. Why This Loop Exists

The benchmark system now covers both:

- the original engine KPI set such as durable commit, point lookup, range scan,
  checkpoint, recovery, read-under-write, and storage efficiency
- the `complex_ecommerce` scenario, which tracks the same workload family as
  the Python complex benchmark:
  - `orders_insert_rps`
  - `report_query_s`
  - `history_p95_ms`
  - `update_p95_ms`
  - `delete_p95_ms`

Those complex metrics are now promoted into:

- benchmark target grading in `benchmarks/targets.toml`
- the report KPI shortlist in `crates/decentdb-benchmark/src/report.rs`

That promotion matters because it makes the complex benchmark metrics first-class
optimization priorities instead of burying them in a long diagnostics table.

## 3. Non-Negotiable Rules

Any coding agent following this loop should obey the following rules.

### 3.1 Build mode

Use `--release` for optimization runs, even when the benchmark profile is `dev`.

Reason:

- `dev` controls dataset size and workload scale
- `--release` controls code generation quality and benchmark validity
- running `dev` in debug mode is useful only for quick local sanity checks, not
  for serious optimization work

### 3.2 Baseline discipline

Do not compare arbitrary runs by eye.

Always compare a candidate run against a named branch-local baseline.

Do not replace the baseline after every run. Update it only when the new run is
clearly a net improvement with no meaningful targeted regressions.

### 3.3 Scope discipline

Agents should work on one small performance idea at a time.

Do not allow an agent to:

- make multiple unrelated performance changes in one iteration
- change benchmark targets just to get a better grade
- optimize against only one metric if it causes meaningful regression elsewhere
- claim a `dev` grade is authoritative

### 3.4 Escalation discipline

Use `nightly` only when:

- the branch appears meaningfully better on `dev`
- a change is ready for review
- a maintainer wants an authoritative benchmark checkpoint

Do not force agents into `nightly` on every small iteration.

## 4. PRD Alignment

This loop supports all 7 pillars in `design/PRD.md`.

1. **ACID Compliance is Forefront**
   The loop keeps durable write and recovery metrics in the optimization set.
2. **Uncompromising Performance**
   The loop exists specifically to drive measurable performance improvement.
3. **Minimal Disk Footprint**
   Storage efficiency remains a first-class graded KPI.
4. **World-Class Documentation**
   The benchmark process is documented and repeatable rather than implicit.
5. **Best-in-Class Tooling & Bindings**
   The loop explicitly optimizes engine behavior that feeds Python and other bindings.
6. **Fantastic CLI Experience**
   The benchmark CLI and reports are part of the developer-facing tooling story.
7. **Fast Developer Feedback Loop**
   `dev` is the iterative optimization rung; `nightly` is reserved for longer authoritative runs.

## 5. Current Priority Metrics

At the time of this document, the following metrics should be treated as the
most important iterative optimization targets.

### 5.1 Core engine KPIs

- `durable_commit_single.txn_p95_us`
- `durable_commit_single.commit_p95_us`
- `durable_commit_batch.rows_per_sec`
- `durable_commit_batch.batch_commit_p95_us`
- `point_lookup_warm.lookup_p95_us`
- `point_lookup_cold.first_read_p95_us`
- `point_lookup_cold.cold_batch_p95_ms`
- `range_scan_warm.rows_per_sec`
- `checkpoint.checkpoint_ms`
- `recovery_reopen.reopen_p95_ms`
- `recovery_reopen.first_query_p95_ms`
- `read_under_write.reader_p95_degradation_ratio`
- `read_under_write.writer_throughput_degradation_ratio`
- `storage_efficiency.space_amplification`

### 5.2 Complex benchmark KPIs

- `complex_ecommerce.orders_insert_rps`
- `complex_ecommerce.report_query_s`
- `complex_ecommerce.history_p95_ms`
- `complex_ecommerce.update_p95_ms`
- `complex_ecommerce.delete_p95_ms`

These metrics are now part of target grading and should be visible in targeted
reports after new runs are produced.

## 6. Standard Workflow

The standard agent loop has five phases:

1. create a branch-local baseline
2. run a focused benchmark after each small change
3. run the full `dev` suite in release mode
4. compare against the baseline and inspect the report
5. update the baseline only after a clear net win

## 7. Commands

### 7.1 Create a branch-local baseline

Start from the current branch state:

```bash
cargo run -p decentdb-benchmark --release -- run --profile dev --all
summary=$(ls -t build/bench/runs/*/summary.json | head -n1)
cargo run -p decentdb-benchmark -- baseline set --name branch-dev --input "$summary"
```

This creates the baseline snapshot under `build/bench/baselines/`.

Use a more specific name if you want multiple local baselines, for example:

```bash
baseline_name=my-branch-dev
```

### 7.2 Fast focused iteration

After a small performance change, first run only the most relevant scenario.

Examples:

```bash
cargo run -p decentdb-benchmark --release -- run --profile smoke --scenario complex_ecommerce
```

```bash
cargo run -p decentdb-benchmark --release -- run --profile smoke --scenario durable_commit_single
```

```bash
cargo run -p decentdb-benchmark --release -- run --profile smoke --scenario storage_efficiency
```

Use this focused run only to reject obviously bad changes quickly.

### 7.3 Full iterative run

If the focused result looks promising, run the full iterative suite:

```bash
cargo run -p decentdb-benchmark --release -- run --profile dev --all
candidate=$(ls -t build/bench/runs/*/summary.json | head -n1)
```

### 7.4 Compare against the branch baseline

```bash
cargo run -p decentdb-benchmark -- compare --candidate "$candidate" --baseline-name branch-dev
```

Then render a short agent-oriented report:

```bash
cargo run -p decentdb-benchmark -- report --latest-compare --format markdown --audience agent_brief --output .tmp/dev-compare-agent.md
```

You can also render a human-oriented HTML compare report if desired:

```bash
cargo run -p decentdb-benchmark -- report --latest-compare --format html --output .tmp/dev-compare.html
```

### 7.5 Promote the baseline only after a real win

If the candidate is a clear net improvement:

```bash
cargo run -p decentdb-benchmark -- baseline set --name branch-dev --input "$candidate"
```

If not, keep the old baseline and continue iterating.

### 7.6 Run the authoritative benchmark checkpoint

When the branch appears ready for review or you want an authoritative truth run:

```bash
./scripts/run-and-display-benchmark-report.sh --profile nightly
```

Or manually:

```bash
cargo run -p decentdb-benchmark --release -- run --profile nightly --all
cargo run -p decentdb-benchmark -- report --latest-run --format html --output build/bench/reports/today-dashboard.html
```

## 8. How Agents Should Prioritize Work

Agents should prioritize benchmark findings in this exact order.

### 8.1 Priority order

1. Targeted metrics with `below_floor`
2. Targeted metrics with `below_target`
3. Regressions in the compare artifact beyond noise thresholds
4. Highest-ranked optimization opportunities in the compare artifact
5. Diagnostic metrics that are not targeted yet but obviously matter

### 8.2 Current priority guidance

In practice, agents should usually start with whichever of these is worst:

- `durable_commit_single.txn_p95_us`
- `durable_commit_batch.rows_per_sec`
- `storage_efficiency.space_amplification`
- `complex_ecommerce.orders_insert_rps`
- `complex_ecommerce.update_p95_ms`
- `complex_ecommerce.delete_p95_ms`
- `complex_ecommerce.history_p95_ms`

### 8.3 What agents should read before choosing work

For each iteration, the agent should read:

- the latest compare report
- the latest candidate `summary.json`
- the specific scenario artifact for the top priority metric
- the code paths most likely to own that metric

Examples:

- `build/bench/runs/.../summary.json`
- `build/bench/runs/.../scenarios/complex_ecommerce.json`
- `build/bench/runs/.../scenarios/durable_commit_single.json`
- `build/bench/runs/.../scenarios/storage_efficiency.json`

## 9. How Agents Should Decide Whether a Change Is Good

An agent should treat a candidate run as a win only when all of the following
are true:

- the primary target metric improved meaningfully
- no higher-priority targeted metric regressed meaningfully
- no new `below_floor` result was introduced
- the change is technically coherent and not a benchmark-specific hack

An agent should not claim success just because:

- one diagnostic number improved slightly
- the HTML looked nicer
- a `dev` run remained green while a more important metric regressed

## 10. Recommended Agent Prompt

The following prompt is the recommended starting point for iterative
benchmark-driven optimization work.

```text
Use the DecentDB benchmark loop in design/AGENT_BENCHMARK_LOOP.md.

Work against release-built benchmark runs, not debug runs.

Inputs to read:
- the latest .tmp/dev-compare-agent.md
- the latest candidate build/bench/runs/.../summary.json
- the scenario artifact for the top 1-2 priority metrics

Prioritize in this order:
1. targeted metrics below floor
2. targeted metrics below target
3. regressions beyond noise in the compare artifact
4. highest optimization opportunities
5. complex_ecommerce metrics that drive bench_complex.py outcomes

Rules:
- make one small performance change at a time
- run a focused release smoke scenario first
- then run release dev --all
- compare against baseline branch-dev
- report exact metric deltas, likely owners, and any regressions
- do not change benchmark targets
- do not update the baseline unless the run is a clear net win
- escalate to nightly only when the branch looks ready
```

## 11. Recommended Maintainer-to-Agent Sequence

This is the shortest reliable operator loop for a maintainer driving an agent.

### 11.1 Prepare the branch

```bash
cargo run -p decentdb-benchmark --release -- run --profile dev --all
summary=$(ls -t build/bench/runs/*/summary.json | head -n1)
cargo run -p decentdb-benchmark -- baseline set --name branch-dev --input "$summary"
```

### 11.2 After each agent patch

```bash
cargo run -p decentdb-benchmark --release -- run --profile smoke --scenario complex_ecommerce
cargo run -p decentdb-benchmark --release -- run --profile dev --all
candidate=$(ls -t build/bench/runs/*/summary.json | head -n1)
cargo run -p decentdb-benchmark -- compare --candidate "$candidate" --baseline-name branch-dev
cargo run -p decentdb-benchmark -- report --latest-compare --format markdown --audience agent_brief --output .tmp/dev-compare-agent.md
```

### 11.3 If the branch clearly improved

```bash
cargo run -p decentdb-benchmark -- baseline set --name branch-dev --input "$candidate"
```

### 11.4 When ready for authoritative evaluation

```bash
./scripts/run-and-display-benchmark-report.sh --profile nightly
```

## 12. Anti-Patterns

Do not use this workflow incorrectly.

### 12.1 Bad benchmarking habits

- running only debug-mode benchmarks and treating them as truth
- comparing runs manually from memory
- changing many hot-path behaviors before rerunning benchmarks
- optimizing for a single metric while ignoring target regressions
- updating the baseline after every tiny change

### 12.2 Bad agent habits

- asking the agent to "make everything elite" without picking the next metric
- allowing the agent to modify targets to make reports look better
- treating `today-dashboard.html` from an old run as if it reflects current code
- forgetting that target promotion requires a new benchmark run to appear in the dashboard

## 13. Maintenance Notes

If the benchmark surface changes, update this document when any of the following happen:

- a new scenario becomes part of `--all`
- new target metrics are added or removed
- benchmark profile semantics change
- compare report fields or audience modes change
- the benchmark-driven optimization priority order changes

This document should remain a practical operating guide, not a historical essay.
