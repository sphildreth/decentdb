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
- DecentDB: `read_p95_ms=0.045`, `join_p95_ms=0.009`, `commit_p95_ms=4.068`, `insert_rows_per_sec=520253.03`, `db_size_mb=0.492`
- DuckDB: `read_p95_ms=0.143`, `join_p95_ms=0.327`, `commit_p95_ms=3.040`, `insert_rows_per_sec=7375.14`, `db_size_mb=3.262`
- SQLite: `read_p95_ms=0.002`, `join_p95_ms=0.002`, `commit_p95_ms=3.008`, `insert_rows_per_sec=1849972.90`, `db_size_mb=1.832`

Important benchmark interpretation notes:
- This harness is a `single_thread_prepared_statement_oltp` profile, not a bulk-ingest benchmark.
- The JSON field `commit_p95_ms` measures `prepared_single_row_auto_commit_insert_p95`, not a bare standalone `COMMIT`.
- SQLite is intentionally self-verified at runtime for fairness: `journal_mode=WAL`, `synchronous=FULL`, and `wal_autocheckpoint=0`.
- Treat `data/bench_summary.json` as the artifact baseline, but rerun before drawing strong conclusions: recent audited reruns have shown DecentDB insert throughput varying in roughly the low-`500k` to mid-`500k` rows/sec range.

Already-landed wins and dead ends:
- The old `O(N^2)` behavior from rebuilding all indexes after each mutation has already been removed from the hot insert path. Inserts now maintain runtime BTREE indexes incrementally or rebuild only when stale.
- Repeated SQL reparsing is no longer the primary bottleneck: DecentDB has a bounded parsed-statement cache plus public prepared statements, and the benchmark uses prepared execution for DecentDB and SQLite.
- Join numbers are real now, not placeholder values. DecentDB already has a narrow indexed inner-join fast path and currently wins that benchmark category comfortably. Do not regress this just to chase insert throughput.
- Phase 0 persistence already moved from a single whole-runtime blob to a manifest plus per-table overflow payloads. This removed the worst whole-database rewrite cliff, but it did not eliminate the deeper storage-architecture limit.
- A public `SqlTransaction` API exists, but using it directly in the audited insert benchmark was slower than the existing prepared statement + explicit `BEGIN`/`COMMIT` path. Do not assume it is the faster benchmark path without remeasuring.
- Runtime BTREEs now specialize common single-column non-null `INT64` keys to typed `i64` maps, which produced a meaningful recent insert-throughput improvement. Avoid redoing that work under a different name.

Current DecentDB performance hurdles / issues / concerns:
- The biggest remaining write-path limit is still structural: DecentDB remains in a Phase 0/early Phase 1 hybrid where tables live in in-memory `TableData` structures and persistence still rewrites encoded table payload blobs plus a manifest, rather than updating page-resident table BTREEs directly.
- Insert throughput is still roughly `3.5x` behind SQLite on the audited explicit-transaction workload (`~520k` vs `~1.85M` rows/sec). Parser overhead and join execution are no longer the dominant suspects; executor/storage hot-path work is.
- Auto-commit insert p95 remains noticeably behind SQLite (`~4.07 ms` vs `~3.01 ms`) even after the manifest persistence improvement. Suspects include encoded table-payload rewrite cost, WAL/page-store copy churn, and remaining commit-path bookkeeping.
- Point-read p95 is still far slower than SQLite in relative terms (`~0.045 ms` vs `~0.002 ms`), even though the absolute number is already small. Likely causes are executor/materialization overhead and the fact that runtime indexes/tables are still not backed by the final page-resident layout.
- File size and join latency are current DecentDB strengths in this harness. Performance work should preserve those advantages unless a regression is clearly justified and documented.
- Because this environment may be shared and benchmark artifacts can change between runs, verify the current worktree and rerun the benchmark you are comparing against before claiming an improvement.

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
- Highest-value remaining architectural target: `phase1-table-btree-persistence` — move table persistence off manifest-managed row blobs and onto page-backed table BTREE ownership, building on `crates/decentdb/src/btree/table.rs` and `design/adr/0123-phase1-table-btree-foundation.md`.

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
