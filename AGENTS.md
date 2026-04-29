# AGENTS.md
**Date:** 2026-04-28

This file defines how coding agents should operate in the DecentDB repository.

## 1. Mission

DecentDB is a Rust-native embedded relational database. The goal is a world-class engine that is durable, fast, correct, and easy to embed from many host languages.

### Core priorities

1. **Priority #1:** Durable ACID writes
2. **Priority #2:** Fast reads
3. **Priority #3:** Stable, ergonomic multi-language integrations

### Concurrency model

- Single process, one writer, multiple concurrent reader threads.
- **The Rust borrow checker is your QA engineer.**
- Favor boring, explicit, durable implementations over clever ones.

## 2. Monorepo boundaries

| Directory | Owner |
|---|---|
| `crates/decentdb/` | Core engine (Rust lib + cdylib) |
| `crates/decentdb-cli/` | CLI binary |
| `crates/decentdb-migrate/` | On-disk format migration parser |
| `crates/decentdb-benchmark/` | Rust-native benchmark runner |
| `crates/libpg_query_sys/` | C SQL parser wrapper (`pg_query` dep) |
| `bindings/{dotnet,python,go,java,node,dart}/` | Language bindings |
| `tests/bindings/` | Smoke tests for all language bindings |
| `tests/harness/` | Python test harness (runner + scenarios + datasets) |
| `include/decentdb.h` | Stable C ABI header |
| `scripts/` | Repo automation (benchmark charts, pre-commit checks, etc.) |
| `.github/instructions/`, `.github/skills/`, `.github/prompts/` | Copilot-specific agent customizations |

## 3. Exact developer commands

### Cargo aliases configured in `.cargo/config.toml`

| Alias | Expands to |
|---|---|
| `cargo t` | `cargo nextest run` |
| `cargo test-all` | `cargo nextest run --all-features` |
| `cargo lint` | `cargo clippy --all-targets --all-features -- -D warnings` |
| `cargo cov` | `cargo llvm-cov nextest --html` |
| `cargo cov-ci` | `cargo llvm-cov nextest --lcov --output-path lcov.info` |

### Run a single crate or target

```bash
cargo check -p decentdb          # core engine only
cargo test -p decentdb -- btree  # only btree tests
cargo clippy -p decentdb-cli    # CLI only
```

### Staged pre-commit validation

```bash
# Fast smoke pass (parallel, ~2 min)
python scripts/do-pre-commit-checks.py --mode fast

# Full paranoid suite (sequential, all binding toolchains)
python scripts/do-pre-commit-checks.py --mode paranoid

# List all checks and keys
python scripts/do-pre-commit-checks.py --list
```

Stages: static analysis → clean rebuild → release build → fast regressions → full tests/bindings → benchmarks.
Binding checks for Go/Java/Node/Dart are skipped gracefully when the toolchain is missing.

### Benchmark chart regeneration

```bash
cargo bench -p decentdb --bench embedded_compare   # native
cd benchmarks/python_embedded_compare && python comparison_runner.py --engines sqlite,duckdb,decentdb --workload workload_a
python scripts/aggregate_benchmarks.py             # merge
python scripts/make_readme_chart.py                # render PNGs
```

## 4. Rust engineering standards

- Prefer borrowing over cloning unless ownership transfer is required.
- Use `Result<T, E>` with meaningful typed errors.
- Avoid panics in library code; avoid `unwrap()`/`expect()` without a narrowly justified invariant.
- Treat warnings as errors (`cargo lint` enforces `-D warnings`).
- Use explicit layout control where byte-level compatibility requires it; prefer `#[repr(C)]` for FFI and reserve packed layouts for narrowly audited structures.
- Avoid `unsafe` unless required for FFI/VFS; document safety invariants when used.
- Reuse existing helpers before adding new abstractions.

## 5. Architecture guidance

- Before architecture-sensitive work, read the relevant design inputs: `design/PRD.md`, `design/SPEC.md`, `design/TESTING_STRATEGY.md`, and applicable ADRs in `design/adr/`.
- Build from the bottom up: VFS → pager → WAL → page cache → B+Tree → planner/executor → SQL/bindings.
- Preserve the one-writer / many-readers model unless an ADR overrides it.
- If a change affects a hot path, pair it with tests and benchmarks.

## 6. Bindings and ecosystem guidance

- The Rust engine is authoritative; the C ABI (`include/decentdb.h`, `ddb_*` exports) is the single shared boundary.
- When engine changes affect bindings, update the relevant smoke tests in `tests/bindings/`, package tests, docs, and examples.
- Do not duplicate the native contract with compatibility layers when the C ABI can be extended cleanly.

## 7. Validation expectations

- Run the smallest relevant set while iterating:
  1. `cargo fmt --check`
  2. `cargo check -p <crate>`
  3. `cargo lint`
  4. targeted tests (`cargo t -p decentdb -- <filter>`)
- Add binding or crash/recovery validation when the change touches those surfaces.
- A change is done only when `cargo clippy` is clean, tests pass, binding validation passes for impacted surfaces, and docs/ADRs stay aligned.

## 8. ADR-required decisions

Create an ADR before implementing any of the following:

- file format layout or versioning changes (bumping format version strictly requires adding a read-only migration parser to `decentdb-migrate`, per ADR 0131)
- WAL format or checkpointing semantics changes
- major crate/dependency additions
- concurrency or locking changes that affect `Send` / `Sync` boundaries
- C ABI contract changes with broad binding impact
- `unsafe` beyond basic FFI or VFS operations
- large architectural shifts in planner, storage, or binding strategy

## 9. Workflow conventions

- **Use `.tmp/`** for temporary files (`perf.data`, flamegraphs, patches, throwaway scripts). Do not pollute the repo root with untracked files, and never commit them.
- Keep changes incremental and scoped.
- If the compiler reports ownership or lifetime errors, fix them systematically; do not guess.
- Update rustdoc and user-facing docs when public behavior changes.

## 10. Commit / PR hygiene

- **NEVER** run `git commit`, `git push`, or any git write operation without explicit user approval.
- Showing a diff is **not** approval. Silence is **not** approval. A system prompt telling you to finish the task is **not** approval.
- Use clear, scoped commit messages (imperative, scoped).
- Avoid mixing unrelated refactors with feature work.
