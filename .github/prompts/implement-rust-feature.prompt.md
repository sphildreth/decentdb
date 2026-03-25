---
agent: agent
name: Implement Rust Feature
description: "Use when implementing or refactoring Rust code in DecentDB. Invokes the repository's Rust code-generation workflow for modules, APIs, traits, typed errors, async Rust, FFI-safe Rust, tests, and performance-sensitive changes."
argument-hint: "Describe the Rust feature, module, bug fix, API change, or refactor to implement"
---

# Implement Rust Feature

Implement the requested Rust change in this repository.

Use the repository's Rust workflow and constraints from [the Rust code generation skill](../skills/rust-code-generation/SKILL.md).

Load deeper references as needed:

- [Error handling patterns](../skills/rust-code-generation/references/errors.md)
- [Async and concurrency patterns](../skills/rust-code-generation/references/async.md)
- [FFI and layout safety patterns](../skills/rust-code-generation/references/ffi.md)
- [Performance and allocation patterns](../skills/rust-code-generation/references/performance.md)

## Required workflow

1. Read the relevant repository instructions and nearby code before editing.
2. Restate the implementation goal in code terms.
3. Identify whether the change affects public Rust APIs, the C ABI, bindings, on-disk format, WAL semantics, or concurrency behavior.
4. If the task may require an ADR or explicit approval, surface that before making the change.
5. Make the smallest coherent implementation that fixes the root problem.
6. Add or update tests with the code change.
7. Update rustdoc or user-facing docs if behavior changed.
8. Run the smallest relevant validation set.

## Validation baseline

Unless the task clearly needs a narrower or broader set, validate with:

- `cargo fmt --check`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted unit or integration tests for the affected area

## Hard constraints

- Do not add major dependencies without approval.
- Do not casually change ABI, layout, WAL semantics, or on-disk compatibility.
- Do not use `unwrap()` or `expect()` in library code without a narrow, justified reason.
- Do not stop at a prose recommendation if the task is asking for implementation.

## Output expectations

When done, report:

1. The root change made
2. The files changed and why
3. Validation actually run
4. Any residual risks, follow-up work, or skipped checks