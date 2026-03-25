---
name: Rust Repository Rules
description: "Use when writing, editing, debugging, or reviewing Rust code in DecentDB. Covers repository-specific Rust constraints for safety, ABI stability, ownership, testing, validation, and dependency discipline."
applyTo: "**/*.rs"
---

# Rust Repository Rules

Apply these rules whenever changing Rust code in this repository.

## Core priorities

- Durable ACID writes come before performance work.
- Fast reads matter, but not at the expense of correctness or compatibility.
- Stable multi-language integration matters; treat binding-facing behavior as a product contract.

## Required defaults

- Prefer small, explicit, incremental changes.
- Prefer borrowing over cloning unless ownership transfer is required.
- Use `Result<T, E>` with meaningful typed errors.
- Avoid panics in library code.
- Avoid `unwrap()` and `expect()` unless there is a narrowly justified invariant.
- Avoid `unsafe` unless it is strictly required; document safety invariants when used.
- Reuse existing helpers and patterns before adding new abstractions.

## Compatibility-sensitive surfaces

Treat these as compatibility boundaries, not local implementation details:

- C ABI exposed through `include/decentdb.h` and `ddb_*`
- on-disk format
- WAL semantics
- binding-visible behavior
- concurrency and locking behavior

If a change materially affects one of those surfaces, check whether docs, tests, bindings, or an ADR are needed.

## Dependency discipline

- Do not add a major dependency without approval.
- Do not introduce a new crate just because it is a common Rust pattern if the problem can be solved cleanly with local code.

## Validation expectations

For Rust changes, run the smallest relevant validation set:

- `cargo fmt --check`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted tests for the touched behavior

Add binding validation or crash/recovery validation when the change affects those surfaces.

## Review checklist

- Is the ownership model defensible?
- Did the change avoid unnecessary allocation, cloning, and locking?
- Are error paths explicit?
- Are tests covering the changed behavior?
- Does the change preserve ABI, format, and concurrency guarantees?