---
name: rust-code-generation
description: 'Generate, refactor, and review Rust code using idiomatic Rust best practices. Use when implementing Rust modules, structs, enums, traits, borrow-checker-sensitive code, ownership fixes, typed error handling, async Rust, FFI-safe Rust, tests, or performance-sensitive Rust changes that must be safe, readable, clippy-clean, and aligned with repository architecture.'
argument-hint: 'Describe the Rust feature, module, API, or refactor to implement'
---

# Rust Code Generation

Use this skill when the task is to generate, extend, refactor, or review Rust code and the result must follow strong engineering standards instead of just compiling.

This skill is for code-producing work such as:

- implementing a new Rust module or API
- adding methods, structs, enums, traits, or iterators
- designing typed error handling with `Result<T, E>`
- adding or refactoring async Rust code
- updating tests alongside Rust code changes
- improving ownership, borrowing, allocation behavior, or API clarity
- preparing FFI-safe or on-disk-layout-sensitive Rust types

This skill is not for:

- generating non-Rust projects
- broad architecture changes that require an ADR before implementation
- adding major dependencies without explicit approval
- making speculative rewrites without reading the surrounding code first
- simple one-liner fixes or purely conversational Rust questions
- debugging sessions that do not involve code generation or refactoring
- situations where an ADR is required but has not been created yet

## References

Load these references when the task needs deeper guidance in a specific area:

- [Error handling patterns](./references/errors.md)
- [Async and concurrency patterns](./references/async.md)
- [FFI and layout safety patterns](./references/ffi.md)
- [Performance and allocation patterns](./references/performance.md)
- [Testing and validation patterns](./references/testing.md)

## Outcomes

When using this skill, the agent should produce Rust code that is:

- idiomatic and explicit
- safe by default
- consistent with the repository's architecture and public contracts
- validated with targeted tests and static checks
- small in scope unless the user explicitly asks for larger changes
- delivered as a single logical change per review (avoid mixing unrelated refactors with feature work)

## Required Context First

Before writing code:

1. Read the repository instructions that govern Rust work.
2. Read the target module and adjacent tests before making assumptions.
3. Identify whether the task affects any of the following:
   - public Rust API
   - C ABI or FFI layout
   - on-disk format or WAL semantics
   - concurrency or locking behavior
   - bindings or other language integrations
4. If the task touches behavior that may require an ADR, stop and surface that clearly before implementing.

In this repository, consult these files when relevant:

- `AGENTS.md`
- `.github/copilot-instructions.md`
- `design/PRD.md`
- `design/SPEC.md`
- `design/TESTING_STRATEGY.md`
- relevant files in `design/adr/`

## Rust Generation Rules

Generate code with these defaults unless the local code clearly uses a different established pattern:

### Repository-Specific Overrides

- Prefer the repository's existing architecture and helper patterns over generic ecosystem advice.
- Do not add new crates just because a generic Rust rule suggests one. In this repository, dependency additions require a strong justification, and major additions require approval and possibly an ADR.
- Treat storage layout, WAL semantics, ABI boundaries, and binding behavior as compatibility constraints, not local implementation details.
- For engine code, optimize for durable correctness first, then measured performance.
- If a generic Rust best practice conflicts with an existing repository rule, follow the repository rule.

### API Design

- Prefer small, explicit, composable APIs.
- Use methods when a function has a clear receiver.
- Prefer enums and newtypes over stringly typed flags or loosely structured state.
- Parse and validate inputs at boundaries when doing so meaningfully reduces invalid internal states.
- Derive common traits where they add clarity or utility: `Debug`, `Clone`, `Copy`, `Eq`, `PartialEq`, `Ord`, `Hash`.
- Add `#[must_use]` where ignoring a return value would likely be a bug.
- Keep public APIs stable and intentional; avoid leaking incidental implementation details.

### Ownership And Borrowing

- Prefer borrowing over cloning.
- Accept slices (`&[T]`) and borrowed string types (`&str`) where ownership is not required.
- Avoid allocations unless they are necessary for correctness, ownership transfer, or measurable simplicity.
- Make lifetimes explicit when inference becomes unclear.
- Preserve zero-copy paths where they materially matter.

### Error Handling

- Use `Result<T, E>` for fallible operations.
- Prefer typed errors over ad hoc string errors for library code.
- Keep error messages actionable and consistent.
- Do not hide failures behind silent fallbacks.
- Avoid `unwrap()` and `expect()` in library code unless there is a narrowly justified invariant and the reason is obvious from context.
- Do not introduce `thiserror`, `anyhow`, or similar crates unless they are already established in the crate or the addition has been explicitly approved.

### Concurrency And Async

- Respect `Send` and `Sync` boundaries.
- Keep ownership and thread-safety arguments explicit.
- Prefer simple async boundaries and avoid unnecessary task spawning.
- Never hold a lock across `.await` unless there is a compelling, well-understood reason and the code documents that choice.
- Preserve the repository's concurrency model rather than introducing new shared-mutable patterns casually.

### Unsafe, FFI, And Layout

- Avoid `unsafe` unless it is strictly required.
- If `unsafe` is necessary, minimize its scope and document the safety invariant.
- Use `#[repr(C)]` or other explicit layout control where FFI or on-disk correctness depends on it.
- **Layout changes are effectively irreversible without a major version bump** — treat `#[repr(C)]`, enum discriminant changes, and `#[packed]` as product-level compatibility constraints.
- Treat ABI and format compatibility as product-level constraints.
- `#[must_use]` results that cross FFI boundaries: if ignored, the error is silently dropped at the boundary. Always handle or explicitly discard with a comment explaining why.

### Performance And Clarity

- Prefer boring, explicit implementations over clever abstractions.
- Reuse local helpers and patterns before adding new abstractions.
- Preallocate with `with_capacity()` when sizes are known and the path is meaningfully hot.
- Avoid premature optimization, but do not ignore clear hot-path costs such as unnecessary cloning, allocation, or locking.
- Profile before adopting more complex optimizations or specialized containers.
- Avoid introducing specialized collection crates such as `smallvec` unless the benefit is measured and the dependency cost is acceptable.
- If optimizing, explain the concrete reason and preserve correctness first.

### Testing And Documentation

- Add focused unit tests next to the code under change.
- Use integration tests when validating public or cross-module behavior.
- Prefer descriptive test names and straightforward arrange/act/assert structure.
- Document public items with rustdoc when behavior is user-facing or reused across the codebase.
- Add `# Errors` and `# Safety` sections when they materially apply.

## Implementation Procedure

1. Restate the narrow implementation goal in code terms.
2. Read the relevant module, nearby call sites, and existing tests.
3. Decide the ownership model, error model, and public API shape before editing.
4. Make the smallest coherent code change that solves the problem at the root cause.
5. Add or update tests next to the behavior being changed.
6. Update rustdoc or user-facing documentation if public behavior changed.
7. Validate with the relevant Rust checks before concluding.

## Validation Checklist

Run the smallest relevant validation set that matches the change:

- `cargo fmt --check`
- `cargo check`
- `cargo build --all-targets` (catches feature-gated, target-specific, and cross-target issues that `cargo check` misses)
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted unit tests
- targeted integration tests
- binding smoke or ABI validation if the change touches shared native contracts

If a command cannot be run, state that explicitly and explain why.

## Review Checklist

Before finishing, confirm:

- the code is readable without clever interpretation
- ownership and lifetime choices are defensible
- error paths are explicit and typed where appropriate
- there are no new unnecessary clones, allocations, or panics
- tests cover the changed behavior and plausible edge cases
- docs match the implemented behavior
- dependency count did not increase without approval

## Anti-Patterns To Avoid

- writing code before reading the surrounding module
- introducing a new dependency for a small local problem
- importing generic Rust advice without checking repository constraints first
- using `unwrap()` or `expect()` in library paths without a strong reason
- returning vague string errors from reusable library surfaces
- holding locks across `.await` in async paths
- changing public semantics without tests and docs
- mixing unrelated refactors into feature work
- changing ABI, format, or concurrency behavior without surfacing the impact

## Response Shape

When completing work with this skill:

1. Implement the code, do not stop at a prose suggestion unless the user asked for design only.
2. Summarize the root change first.
3. List the files changed and why.
4. Report validation actually run.
5. Call out residual risks, skipped checks, or follow-up work if any remain.