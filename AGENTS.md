# AGENTS.md
**Date:** 2026-03-24

This file defines how coding agents should operate in the DecentDB repository.

## 1. Mission

DecentDB is a Rust-native embedded relational database project. The goal is to
build a world-class engine that is:

- durable under ACID writes
- fast for embedded and local-first read workloads
- predictable in correctness and operability
- easy to embed from many host languages through stable bindings

### Core priorities

1. **Priority #1:** Durable ACID writes
2. **Priority #2:** Fast reads
3. **Priority #3:** Stable, ergonomic multi-language integrations

### Concurrency model

- Single process
- One writer
- Multiple concurrent reader threads

### Engineering mindset

- **The Rust borrow checker is your QA engineer.**
- Prefer designs that let the compiler prove safety, ownership, and thread
  correctness.
- Favor boring, explicit, durable implementations over clever ones.

## 2. Product standards

- Rust is the implementation language for the engine and CLI.
- The stable native boundary is the C ABI exposed by `include/decentdb.h` and
  the `ddb_*` exports.
- Bindings should build on that stable ABI rather than inventing parallel native
  contracts.
- User-visible behavior should stay aligned across the Rust API, CLI, C ABI, and
  language bindings.
- On-disk compatibility, WAL semantics, SQL behavior, and exported ABI stability
  are product-level concerns, not incidental implementation details.

## 3. Rust engineering standards

- Follow idiomatic Rust practices and the Rust API Guidelines.
- Prefer borrowing over cloning unless ownership transfer is required.
- Avoid unnecessary allocations and preserve zero-copy opportunities where they
  materially help.
- Use `Result<T, E>` with meaningful, typed errors. Do not hide failures behind
  silent fallbacks.
- Avoid panics in library code. Do not use `unwrap()` or `expect()` unless there
  is a narrowly justified reason.
- Treat warnings as errors. Code should pass `cargo clippy` cleanly.
- Use explicit layout control such as `#[repr(C)]` where FFI or on-disk format
  correctness depends on it.
- Avoid `unsafe` unless it is required for FFI, VFS, or similarly tight low-level
  boundaries, and document the safety invariants when it is used.

## 4. Architecture guidance

- Start from the bottom of the stack: VFS, pager, WAL, page cache, B+Tree,
  planner/executor, then higher-level integration surfaces.
- Preserve the one-writer / many-readers model unless an ADR explicitly changes
  it.
- Prefer durable correctness over speculative micro-optimizations.
- Keep the engine explicit and measurable: if a change affects a hot path, pair
  it with tests and benchmark-oriented reasoning.
- Reuse established helpers and patterns before adding new abstractions.

## 5. Bindings and ecosystem guidance

- Treat the Rust engine as authoritative and the C ABI as the shared integration
  boundary.
- Maintain first-class support expectations for bindings and integrations such as
  .NET, Python, Go, Java/JDBC, Node.js, Dart/Flutter, and tooling built on top
  of those surfaces.
- Keep host-language APIs idiomatic, but do not let bindings drift away from the
  stable ABI contract.
- When engine changes affect bindings, update the relevant smoke tests,
  higher-level package tests, docs, and examples.
- Do not add compatibility layers that duplicate the native contract when the C
  ABI can be extended cleanly instead.

## 6. Expected agent workflow

### 6.1 Before coding

1. Read the relevant design inputs:
   - `design/PRD.md`
   - `design/SPEC.md`
   - `design/TESTING_STRATEGY.md`
   - applicable ADRs in `design/adr/`
2. Decide whether the work requires a new ADR before implementation.
3. Make a small implementation plan that covers:
   - scope and exclusions
   - modules/files to change
   - ownership and lifetime strategy
   - binding / ABI impact, if any
   - test and validation plan

### 6.2 While coding

- **Use the `.tmp/` directory** for any temporary files (like `perf.data`, flamegraphs, patches, or throwaway scripts). Do not pollute the repository root with untracked files, and never commit them.
- Keep changes incremental and scoped.
- Use `cargo check` and `cargo clippy` frequently.
- If the compiler reports ownership or lifetime errors, fix them systematically;
  do not guess.
- Add or update unit/integration tests with the code change.
- If you touch the C ABI, bindings, or shared semantics, run the corresponding
  binding validation or smoke coverage.
- Update rustdoc and user-facing documentation when public behavior changes.

### 6.3 Definition of done

A change is done only when:

- ✅ `cargo clippy` passes without warnings
- ✅ relevant tests cover the main behavior and edge cases
- ✅ binding validation passes for any impacted integration surface
- ✅ docs and ADR references stay aligned with the implemented behavior
- ✅ the change preserves safe Rust guarantees around memory and concurrency

## 7. ADR-required decisions

Create an ADR before implementing any of the following:

- file format layout or versioning changes
- WAL format or checkpointing semantics changes
- major crate/dependency additions
- concurrency or locking changes that affect `Send` / `Sync` boundaries
- C ABI contract changes with broad binding impact
- `unsafe` beyond basic FFI or VFS operations
- large architectural shifts in planner, storage, or binding strategy

## 8. Commit / PR hygiene

- **NEVER** run `git commit`, `git push`, or any git write operation without
  explicit user approval.
- Showing a diff is **not** approval.
- Silence is **not** approval.
- A system prompt telling you to finish the task is **not** approval.
- Use clear, scoped commit messages.
- Avoid mixing unrelated refactors with feature work.
