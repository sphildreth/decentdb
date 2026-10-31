# Design Archive

**Status:** Historical reference material

**Updated:** 2026-10-31

This directory holds design notes, prompts, and planning documents that are no
longer the active source of truth but still explain useful project context.

Active roadmap and backlog live in Hamstik project `DDB`. Retired
roadmap and spec documents are indexed in `../HISTORY.md` and attached to their
DDB work items.

## Contents

| File | Why Archived |
|---|---|
| `2026-05-28.RELEASE_PLANS.md` | Historical release-process reset proposal. Current release behavior should be read from workflows, scripts, and release docs rather than this dated plan. |
| `2026-06-18_REVIEW.md` | Historical repository and binding review snapshot. The implementation follow-up has been completed and current quality expectations live in `AGENTS.md`, docs, and validation scripts. |
| `2026-06-18_REVIEW_IMPLEMENTATION_PLAN.md` | Completed implementation plan for the 2026-06-18 review. All phases are marked complete, so it is no longer active planning material. |
| `2026-06-20-PERF_ISSUES.md` | Historical performance investigation log with many completed phases. Active performance direction now lives in `../HISTORY.md` and benchmark docs. |
| `2026-06-20-PERF_ISSUES_PROMPT.md` | Historical coding-agent prompt for a completed performance push. Useful as prompt provenance, but not an active task definition. |
| `2026-06-PERF_TESTING_RESULTS.md` | Historical benchmark investigation that informed ADR 0195. Current performance evidence should be read from `../HISTORY.md` and benchmark outputs. |
| `BRANCH_CHECKS_PRE_PR_PROMPT.md` | Historical agent prompt for pre-PR branch checks. Useful as a checklist seed, but current validation commands live in `AGENTS.md`, `.cargo/config.toml`, and repo scripts. |
| `COMPARISON_BENCHMARK_PLAN.md` | Historical Python-based embedded database comparison plan. The active benchmark overview is `design/BENCHMARKING_GUIDE.md`; this file preserves the original comparison rationale and workload notes. |
| `DECENTDB_SHOWCASE_BENCHMARK_PROMPT.md` | Historical prompt template for generating language-specific benchmark showcases. Useful for future sample generation, but not an authoritative design contract. |
| `DELETE_BATCH.md` | Completed resident cascade/bulk-delete performance design note. ADR 0200 and engine tests are now the active contract; this preserves implementation history and measurements. |
| `METRIC_IMPROVEMENTS_PLAN.md` | Historical metric tracker. Public chart metrics now beat SQLite across DecentDB profiles in the recorded summary; remaining active diagnostic gaps were folded into `../HISTORY.md`. |
| `RUST_BASELINE_IMPROVEMENTS.md` | Completed rust-baseline improvement plan. The active benchmark surface is now documented in `benchmarks/rust-baseline/README.md` and `../BENCHMARKING_GUIDE.md`. |
| `RUST_BENCHMARK_PLAN.md` | Historical Rust-native benchmark system proposal. The benchmark crate and `design/BENCHMARKING_GUIDE.md` are now the active references. |

## Archive Policy

- Archive documents that still explain useful historical context but are no
  longer the current implementation source of truth.
- Delete only documents that are both obsolete and provide no useful rationale,
  checklist, migration context, or reusable prompt material.
- Keep ADRs in `design/adr/`; ADRs are historical records by definition.
