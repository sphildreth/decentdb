# Testing

DecentDB uses several complementary test layers.

## Test layout

```text
crates/decentdb/src/           # unit tests next to engine modules
crates/decentdb/tests/         # engine integration tests
crates/decentdb-cli/tests/     # CLI integration tests
tests/harness/                 # crash/storage scenario runner + datasets
tests/bindings/                # binding smoke/validation programs
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

## Release-oriented pre-commit validation

The staged pre-commit runner has two modes:

```bash
python scripts/do-pre-commit-checks.py --mode fast
python scripts/do-pre-commit-checks.py --mode paranoid
```

Use `paranoid` before release candidates. It runs the normal Rust and binding
matrix, then adds release metadata/version checks, structured diagnostic
guardrails, remaining workspace crate tests, package-local Knex and web
typecheck/build coverage, storage crash/soak harnesses, release metrics,
rustdoc/API docs, MkDocs, and browser WASM OPFS smoke coverage.

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

## Package-local binding suites

Several language integrations also keep package-local tests under `bindings/`.

### .NET packages

```bash
cd bindings/dotnet
dotnet test DecentDB.NET.sln -v minimal
```

### Python package

```bash
python3 -m pip install -e bindings/python
pytest -q bindings/python/tests
```

### Go package

```bash
cd bindings/go/decentdb-go
go test ./...
```

### Java / JDBC package

```bash
cd bindings/java
./gradlew :driver:test
```

### Node packages

```bash
cd bindings/node/decentdb
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm test

cd ../knex-decentdb
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm test
```

### Dart package

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

For benchmark runs that produce files, write outputs under `.tmp/` or another
explicitly ignored directory:

- `--out-dir` (JSON/HTML reports): `.tmp/benchmarks/...`
- Flamegraph / profiling artifacts: `.tmp/profiling/...`
- Temporary benchmark scripts, throwaway analysis files, and exploratory command
  wrappers: `.tmp/` workspaces

The Rust-native benchmark runner also owns the Phase 1 concurrent-write
regression hooks:

```bash
cargo run -p decentdb-benchmark -- run --profile smoke \
  --scenario durable_commit_single --scenario read_under_write
cargo run -p decentdb-benchmark -- run --profile smoke \
  --scenario queued_writer_single --scenario queued_writer_read_under_write
```

The benchmark output for those hooks should also be written to `.tmp/` (for
example `.tmp/review-implementation/benchmarks/phase-1/`).

The queued-writer scenarios execute the engine-owned write queue directly:
`queued_writer_single` measures the single-writer queued overhead with zero
group delay, and `queued_writer_read_under_write` runs concurrent queued
writers beside point readers while reporting strict group-commit counters.

Pre-commit exposes those slices as:

```bash
python scripts/do-pre-commit-checks.py --only rust-benchmark-phase1-native
python scripts/do-pre-commit-checks.py --only rust-benchmark-phase1-queued-writers
```

## Adding new tests

- Add unit tests next to the Rust module they exercise via `#[cfg(test)]`.
- Add engine integration tests under `crates/decentdb/tests/`.
- Add CLI integration tests under `crates/decentdb-cli/tests/`.
- Add new crash/storage scenarios under `tests/harness/scenarios/`.
- Add binding smoke coverage under `tests/bindings/<language>/`.
- Add package-local binding tests under the relevant `bindings/<language>/`
  tree when the change affects a shipped host-language surface.

Every behavior-changing code change should include tests at the narrowest layer
that proves the behavior.
