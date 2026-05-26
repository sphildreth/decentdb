# Design Archive

**Status:** Historical reference material

**Updated:** 2026-05-26

This directory holds design notes, prompts, and planning documents that are no
longer the active source of truth but still explain useful project context.

Active roadmap, architecture, and validation inputs remain in the parent
`design/` directory and `design/adr/`.

## Contents

| File | Why Archived |
|---|---|
| `BRANCH_CHECKS_PRE_PR_PROMPT.md` | Historical agent prompt for pre-PR branch checks. Useful as a checklist seed, but current validation commands live in `AGENTS.md`, `.cargo/config.toml`, and repo scripts. |
| `COMPARISON_BENCHMARK_PLAN.md` | Historical Python-based embedded database comparison plan. The active benchmark overview is `design/BENCHMARKING_GUIDE.md`; this file preserves the original comparison rationale and workload notes. |
| `DECENTDB_SHOWCASE_BENCHMARK_PROMPT.md` | Historical prompt template for generating language-specific benchmark showcases. Useful for future sample generation, but not an authoritative design contract. |
| `RUST_BENCHMARK_PLAN.md` | Historical Rust-native benchmark system proposal. The benchmark crate and `design/BENCHMARKING_GUIDE.md` are now the active references. |

## Archive Policy

- Archive documents that still explain useful historical context but are no
  longer the current implementation source of truth.
- Delete only documents that are both obsolete and provide no useful rationale,
  checklist, migration context, or reusable prompt material.
- Keep ADRs in `design/adr/`; ADRs are historical records by definition.
