# Contributing

Thanks for helping improve DecentDB.

## Getting started

```bash
git clone https://github.com/sphildreth/decentdb.git
cd decentdb
cargo build --workspace
cargo test --workspace
```

If you plan to work on the CLI directly, the binary lives in
`crates/decentdb-cli/`. If you plan to work on bindings, the shared library is
built by `cargo build -p decentdb`.

## Typical workflow

1. Create a branch for your change.
2. Make a focused edit.
3. Add or update tests.
4. Run the relevant validation commands.
5. Open a pull request with a clear description of the change and the checks you ran.

## Required local checks

For most Rust changes, run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features
cargo test --workspace
```

If you touched bindings or integration tooling, also run the relevant
package-local suite and matching `tests/bindings/` smoke path described in the
[testing guide](testing.md).

If you touched the storage harness or crash behavior, run at least one relevant
scenario from `tests/harness/scenarios/`.

## Coding expectations

- Follow idiomatic Rust patterns and let `rustfmt` handle layout.
- Prefer safe, explicit code over clever shortcuts.
- Avoid adding new dependencies unless they are clearly justified.
- Keep changes small and scoped.
- Document new public APIs.
- Add tests for bug fixes and new behavior.

## Pull request guidance

A good PR description should cover:

- what changed
- why it changed
- how you validated it
- any follow-up work that remains

Keeping docs, tests, and implementation aligned is especially valuable in a
database project.
