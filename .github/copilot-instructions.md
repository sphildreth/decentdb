# DecentDB Copilot Instructions

These instructions define how coding agents should operate in the DecentDB repository.
DecentDB is currently undergoing a complete rewrite from Nim to Rust. **The Rust Borrow Checker is our QA Engineer.**

## 1. North Star & Agent Mandate
- **Priority #1:** Durable ACID writes
- **Priority #2:** Fast reads
- **Concurrency model:** Single process with **one writer** and **multiple concurrent reader threads**.
- Correctness is enforced via **tests from day one** (unit + property + crash-injection + differential testing against SQLite and the old Nim version).
- **NEVER** write code that hallucinates Nim syntax. This is a 100% Rust rewrite.

## 2. Rust Coding Conventions and Best Practices

Follow idiomatic Rust practices and community standards when writing Rust code. These instructions are based on [The Rust Book](https://doc.rust-lang.org/book/), [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/), and the broader Rust community.

### General Instructions
- Always prioritize readability, safety, and zero-cost abstractions to compete with SQLite.
- Ensure structs have precise byte-level control where necessary (e.g. `#[repr(C)]` or `#[repr(packed)]` for disk pages) to minimize disk bloat.
- Handle errors gracefully using `Result<T, E>` and custom error types via `thiserror`. Provide meaningful context in error messages.
- Ensure code compiles without warnings. Treat warnings as errors.
- Never use `unwrap()` or `expect()` unless absolutely necessary—prefer proper error handling with `?`.
- Avoid panics in library code—return `Result` instead.

### Ownership, Borrowing, and Lifetimes
- Prefer borrowing (`&T`) over cloning unless ownership transfer is necessary.
- Explicitly annotate lifetimes when the compiler cannot infer them.
- Avoid unnecessary allocations—prefer borrowing and zero-copy operations.
- Avoid `unsafe` unless strictly required for mmap, VFS, or tight C-ABI interop, and ALWAYS fully document the safety invariants.

### Type Safety and Predictability
- Use newtypes to provide static distinctions.
- Functions with a clear receiver should be methods.
- Eagerly implement common traits where appropriate (`Copy`, `Clone`, `Eq`, `PartialEq`, `Debug`, `Display`).
- Note: `Send` and `Sync` must be strictly respected. Let the compiler enforce the single-writer/multi-reader concurrency model.

## 3. Scope Boundaries & ADRs (Architecture Decision Records)
- Keep changes small and incremental.
- **Avoid adding dependencies.** If you must add a dependency (especially for major things like SQL parser, compression, hashing), create an ADR in `design/adr/` and ask the user for approval first.
- Prefer boring, explicit implementations over clever ones.
- Start at the bottom: VFS/Pager/WAL, then B+Tree, then higher layers.

## 4. Testing and Documentation
- Write comprehensive unit tests using `#[cfg(test)]` modules and `#[test]` annotations alongside the code they test.
- Use property tests where applicable for database invariants.
- If durability/format-sensitive: ensure crash-injection tests and differential tests are considered.
- Document all public APIs with rustdoc (`///` comments) following API Guidelines.
- Write clear and concise comments focusing on *why* complex logic (like B-Tree splits) is done, not *what*.

## 5. Commit / PR Hygiene
- **NEVER run `git commit`, `git push`, or any git write operation without EXPLICIT user approval.** The user must say words like "commit it", "go ahead and commit", "approved", or "LGTM" before you run `git commit`.
- Showing a diff is NOT approval. Silence is NOT approval.
- Use clear commit messages (imperative, scoped).
- Avoid mixing unrelated refactors with feature work.