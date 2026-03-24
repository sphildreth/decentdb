# DecentDB Copilot Instructions

These instructions define how coding agents should operate in the DecentDB
repository.

## 1. Mission

DecentDB is a Rust-native embedded relational database project. The goal is to
build a world-class engine that is durable, fast, correct, and easy to embed
from many host languages.

- **Priority #1:** Durable ACID writes
- **Priority #2:** Fast reads
- **Priority #3:** Stable, ergonomic multi-language integrations
- **Concurrency model:** Single process with **one writer** and **multiple
  concurrent reader threads**
- **The Rust borrow checker is your QA engineer**

## 2. Product standards

- Rust is the implementation language for the engine and CLI.
- The stable native boundary is the C ABI exposed by `include/decentdb.h` and
  the `ddb_*` exports.
- Bindings should build on that ABI instead of creating parallel native
  contracts.
- User-visible behavior should stay aligned across the Rust API, CLI, C ABI, and
  language bindings.

## 3. Rust coding conventions

Follow idiomatic Rust practices and community standards. These instructions are
grounded in [The Rust Book](https://doc.rust-lang.org/book/), the
[Rust API Guidelines](https://rust-lang.github.io/api-guidelines/), and normal
Rust ecosystem expectations.

### General instructions

- Prioritize readability, safety, and zero-cost abstractions.
- Ensure structs have precise byte-level control where necessary (for example
  `#[repr(C)]` for disk pages or FFI layouts).
- Handle errors with `Result<T, E>` and meaningful typed errors.
- Ensure code compiles without warnings. Treat warnings as errors.
- Avoid panics in library code.
- Avoid `unwrap()` and `expect()` unless there is a narrowly justified reason.

### Ownership, borrowing, and lifetimes

- Prefer borrowing (`&T`) over cloning unless ownership transfer is required.
- Annotate lifetimes explicitly when inference is not enough.
- Avoid unnecessary allocations and preserve zero-copy paths where they matter.
- Avoid `unsafe` unless strictly required for FFI, VFS, or similarly low-level
  boundaries, and fully document the safety invariants.

### Type safety and predictability

- Use newtypes to provide static distinctions where helpful.
- Functions with a clear receiver should be methods.
- Implement common traits where appropriate (`Copy`, `Clone`, `Eq`,
  `PartialEq`, `Debug`, `Display`).
- Respect `Send` and `Sync` boundaries so the compiler can enforce the
  single-writer / multi-reader model.

## 4. Scope boundaries and architecture

- Keep changes small and incremental.
- **Avoid adding dependencies.** If you must add a major dependency, create an
  ADR in `design/adr/` and ask the user for approval first.
- Prefer boring, explicit implementations over clever ones.
- Start at the bottom of the stack: VFS, pager, WAL, page cache, B+Tree, then
  higher layers.
- Treat on-disk format, WAL semantics, SQL behavior, and ABI stability as
  product-level compatibility concerns.

## 5. Bindings and documentation

- Treat the Rust engine as authoritative and the C ABI as the shared binding
  boundary.
- Keep support expectations high for .NET, Python, Go, Java/JDBC, Node.js,
  Dart/Flutter, and related tooling.
- When engine work affects bindings, update the relevant binding tests, docs,
  and examples.
- Document public APIs with rustdoc (`///` comments) and keep user-facing docs
  aligned with behavior.

## 6. Validation expectations

- Write comprehensive unit tests next to the code they exercise.
- Use integration, property, crash-injection, and binding tests where relevant.
- Run `cargo check`, `cargo clippy`, and the relevant targeted test suites while
  working.
- If a change touches the C ABI or binding semantics, run the corresponding
  binding validation or smoke coverage.

## 7. Commit / PR hygiene

- **NEVER** run `git commit`, `git push`, or any git write operation without
  explicit user approval.
- Showing a diff is **not** approval.
- Silence is **not** approval.
- Use clear commit messages (imperative, scoped).
- Avoid mixing unrelated refactors with feature work.
