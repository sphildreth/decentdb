---
agent: agent
name: Debug Rust Failure
description: "Use when debugging Rust compiler errors, borrow checker failures, clippy warnings, failing Rust tests, ownership/lifetime issues, async lock bugs, or FFI-safety problems in DecentDB."
argument-hint: "Describe the Rust error, failing test, compiler message, clippy warning, or problematic file"
---

# Debug Rust Failure

Debug the requested Rust failure in this repository.

Use the repository's Rust workflow and references from [the Rust code generation skill](../skills/rust-code-generation/SKILL.md).

Load deeper references as needed:

- [Error handling patterns](../skills/rust-code-generation/references/errors.md)
- [Async and concurrency patterns](../skills/rust-code-generation/references/async.md)
- [FFI and layout safety patterns](../skills/rust-code-generation/references/ffi.md)
- [Performance and allocation patterns](../skills/rust-code-generation/references/performance.md)
- [Testing and validation patterns](../skills/rust-code-generation/references/testing.md)

## Required workflow

1. Reproduce or inspect the actual failure first.
2. Read the affected module and nearby tests before proposing a fix.
3. Classify the failure:
   - compiler or borrow checker error
   - clippy or formatting failure
   - unit or integration test failure
   - concurrency or async bug
   - FFI, ABI, or layout issue
4. Identify the root cause instead of patching symptoms.
5. Make the smallest coherent fix.
6. Add or update regression tests when appropriate.
7. Run the smallest relevant validation set to prove the fix.

## Debug priorities

Prefer fixes that:

- simplify ownership or lifetime flow
- remove unnecessary clones, locks, or mutable state
- preserve repository architecture and compatibility constraints
- keep error handling explicit
- avoid new dependencies or broad refactors

## Validation baseline

Use the smallest relevant subset of:

- `cargo fmt --check`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted unit or integration tests
- binding smoke or ABI validation when relevant

## Output expectations

When done, report:

1. The root cause
2. The fix made
3. The files changed and why
4. Validation actually run
5. Any residual risks or follow-up checks