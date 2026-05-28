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
| `WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md` | Delivered implementation plan for the queued SQL/write-queue foundation. Future concurrency prioritization now lives in `design/FUTURE_WINS.md` and ADR 0162. |
| `WIN_LUA_EXTENSION_RUNTIME_SPEC.md` | Delivered implementation spec for the Lua extension runtime/package foundation. ADRs 0169-0173 and user docs are now the active contract. |
| `WIN_PRODUCTION_BROWSER_RUNTIM.md` | Delivered implementation spec for the production browser runtime foundation. ADRs 0161 and 0165 plus docs are now the active contract. |
| `WIN_PRODUCTION_RELAY_SPEC.md` | Delivered implementation spec for production sync relay and public changesets. ADRs 0166-0168 plus sync docs are now the active contract. |
| `WIN_REACTIVE_QUERY_SUBSCRIPTIONS_CHANGE_STREAMS_SPEC.md` | Delivered implementation spec for reactive subscriptions and change streams. ADR 0164 and API docs are now the active contract. |
| `WIN_SQL_PRAGMA_COMPATIBILITY_QUICK_WINS_SPEC.md` | Delivered implementation spec for SQL/PRAGMA compatibility quick wins. Remaining SQL compatibility work is tracked in `design/WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`. |

## Archive Policy

- Archive documents that still explain useful historical context but are no
  longer the current implementation source of truth.
- Delete only documents that are both obsolete and provide no useful rationale,
  checklist, migration context, or reusable prompt material.
- Keep ADRs in `design/adr/`; ADRs are historical records by definition.
