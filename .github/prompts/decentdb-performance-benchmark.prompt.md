---
description: "Drive DecentDB benchmark work toward beating collected embedded engine numbers while preserving durability and correctness"
name: "DecentDB Performance Push"
argument-hint: "Optional focus area, metric, or subsystem to target"
agent: "agent"
model: "GPT-5 (copilot)"
---
Work on DecentDB performance with the explicit goal of beating the collected benchmark numbers from the other embedded database engines, without regressing ACID guarantees, benchmark fairness, or code quality.

If the user supplied a focus area, metric, or subsystem, treat it as the optimization target. Otherwise, choose the highest-value bottleneck from the current benchmark data.

Start by reading the benchmark contract and current numbers:
- [design/BENCHMARKING_GUIDE.md](../../design/BENCHMARKING_GUIDE.md)
- [design/COMPARISON_BENCHMARK_PLAN.md](../../design/COMPARISON_BENCHMARK_PLAN.md)
- [design/PRD.md](../../design/PRD.md)
- [design/TESTING_STRATEGY.md](../../design/TESTING_STRATEGY.md)
- [data/bench_summary.json](../../data/bench_summary.json)
- [AGENTS.md](../../AGENTS.md)
- [.github/copilot-instructions.md](../copilot-instructions.md)

Current native benchmark snapshot to beat:
- DecentDB: `read_p95_ms=0.048`, `join_p95_ms=0.013`, `commit_p95_ms=4.078`, `insert_rows_per_sec=423635.53`, `db_size_mb=0.492`
- DuckDB: `read_p95_ms=0.154`, `join_p95_ms=0.333`, `commit_p95_ms=3.053`, `insert_rows_per_sec=7120.40`, `db_size_mb=3.262`
- SQLite: `read_p95_ms=0.002`, `join_p95_ms=0.002`, `commit_p95_ms=3.022`, `insert_rows_per_sec=1752322.90`, `db_size_mb=1.832`

Primary objective:
- Improve DecentDB so the measured benchmark deltas move toward category-leading results, with special attention to the metrics where DecentDB is still behind the collected competitors.

Hard constraints:
- ACID durability is non-negotiable. Do not trade away correctness or `fsync` semantics for benchmark wins.
- Respect the benchmark fairness contract. Do not manipulate harness settings to produce misleading comparisons.
- Keep changes small, explicit, and idiomatic Rust.
- Do not add dependencies without an ADR and user approval.
- Avoid `unwrap()`/`expect()` in library code unless there is a compelling, localized reason.

Mandatory PRD tenets (all 7 must be upheld):
1. ACID compliance is forefront: durability and crash safety over raw speed.
2. Uncompromising performance: improvements should move DecentDB toward beating SQLite-level baselines.
3. Minimal disk footprint: avoid performance changes that bloat on-disk representation without strong justification.
4. World-class documentation: update docs when behavior, knobs, or benchmark interpretation changes.
5. Best-in-class tooling and bindings: avoid core changes that break or degrade the C-ABI and downstream bindings.
6. Fantastic CLI experience: avoid regressions that worsen CLI behavior, query usability, or error clarity.
7. Fast developer feedback loop: keep validation practical for PR workflows and avoid unnecessary CI cost.

Execution workflow:
1. Identify the target metric and explain why it is the best optimization candidate based on the current benchmark numbers.
2. Inspect the relevant benchmark harness and engine code paths before editing anything.
3. Form a concise plan that names:
   - the bottleneck hypothesis,
   - the files/modules to change,
   - the validation steps,
   - the risk to correctness or benchmark fairness.
4. Implement the smallest credible fix for the root cause.
5. Run focused validation first, then the most relevant benchmark or benchmark slice you can execute in this environment.
6. Report the before/after metrics and clearly state whether DecentDB improved, matched, or beat the comparison numbers.
7. If the target is still not beaten, propose the next bottleneck in ranked order instead of padding the change with speculative refactors.

Optimization priorities:
- Prioritize durable commit latency, point read latency, and insert throughput, because those are the clearest gaps in the current collected numbers.
- Preserve or improve DecentDB's strong join latency and file-size advantages.
- Prefer algorithmic or layout improvements over superficial micro-optimizations.
- Treat allocator churn, unnecessary copies, sync frequency, hot-path branching, cache misses, page layout inefficiencies, and avoidable serialization work as prime suspects.

Validation requirements:
- Run `cargo clippy` on the affected crate if your change touches Rust code.
- Run targeted tests for the touched subsystem when practical.
- If you run benchmarks, state exactly which command was executed and whether the result is directly comparable to `data/bench_summary.json`.
- Do not update published benchmark artifacts unless you actually reran the relevant benchmark.

Output requirements:
- Lead with the target metric and the bottleneck you chose.
- Show the concrete code changes made.
- Summarize validation and measured results.
- Call out any remaining gap versus SQLite, DuckDB, or both.
- Include a brief "7-tenet compliance" checklist stating how the work satisfied each PRD tenet (or explicitly note any tenet not impacted).
- End with the single highest-value next step if more work is needed.