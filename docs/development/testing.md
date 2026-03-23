# Testing

DecentDB's Rust rewrite uses several complementary test layers.

## Test layout

```text
crates/decentdb/src/           # unit tests next to engine modules
crates/decentdb/tests/         # engine integration tests
crates/decentdb-cli/tests/     # CLI integration tests
tests/harness/                 # crash/storage scenario runner + datasets
tests/bindings/                # binding smoke/validation programs
bindings/dart/dart/test/       # packaged Dart wrapper tests
```

## Core workspace validation

Run the main workspace suites from the repository root:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
```

If you use the Cargo aliases from `.cargo/config.toml`, the equivalent shortcuts
are:

```bash
cargo t
cargo lint
```

## Focused Rust test commands

Engine library/unit tests:

```bash
cargo test -p decentdb --lib
```

Engine integration tests:

```bash
cargo test -p decentdb --test relational_phase3_tests
```

CLI tests:

```bash
cargo test -p decentdb-cli
```

## Crash/storage harness

The storage harness lives under `tests/harness/`.

Run one of the checked-in scenarios with:

```bash
python tests/harness/runner.py tests/harness/scenarios/short_crash.json
python tests/harness/runner.py tests/harness/scenarios/soak_storage.json
```

The runner shells out to `cargo run -p decentdb --bin decentdb-test-harness`.
Scenario fixtures live under `tests/harness/scenarios/`, and reusable datasets
live under `tests/harness/datasets/`.

## Binding validation

Binding smoke/validation programs live under `tests/bindings/`:

- `tests/bindings/python/`
- `tests/bindings/dotnet/`
- `tests/bindings/c/`
- `tests/bindings/go/`
- `tests/bindings/java/`
- `tests/bindings/node/`
- `tests/bindings/dart/`

See `tests/bindings/README.md` for the shared expectation that the Rust `cdylib`
has been built first.

## Dart package validation

The in-tree Dart package has its own suite in `bindings/dart/dart/test/`.

Run it with:

```bash
bindings/dart/scripts/run_tests.sh
```

That script builds `target/debug/libdecentdb.*`, runs `dart pub get`, and then
executes `dart test` for the packaged wrapper.

## Benchmarks

The repository currently ships the `release_metrics` benchmark target:

```bash
cargo bench -p decentdb
```

## Adding new tests

- Add unit tests next to the Rust module they exercise via `#[cfg(test)]`.
- Add engine integration tests under `crates/decentdb/tests/`.
- Add CLI integration tests under `crates/decentdb-cli/tests/`.
- Add new crash/storage scenarios under `tests/harness/scenarios/`.
- Add binding smoke coverage under `tests/bindings/<language>/`.

Every behavior-changing code change should include tests at the narrowest layer
that proves the behavior.
