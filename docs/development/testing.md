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

Install the supply-chain checker used by CI before running the pre-commit suite:

```bash
cargo install cargo-deny --locked
```

The staged pre-commit runner has two modes:

```bash
python scripts/do-pre-commit-checks.py --mode fast
python scripts/do-pre-commit-checks.py --mode paranoid
```

Both modes run `cargo deny check` against `deny.toml` for the main workspace,
the standalone Rust baseline benchmark, and the independent fuzz workspace,
checking security advisories, licenses, dependency bans, and dependency sources. A missing
`cargo-deny` installation fails the check. Run this check on its own with
`python scripts/do-pre-commit-checks.py --only rust-supply-chain`.

Fast mode also checks staged/working-tree whitespace, vendored C ABI headers,
release metadata, structured diagnostics, and the repository's Python automation
tests before its native build and regression suites. Independent checks within
a fast stage may run in parallel.

Use `paranoid` before release candidates. It runs sequentially and adds all Rust
workspace tests, independent fuzz/benchmark formatting and Clippy checks, standalone
benchmark tests, Python benchmark framework tests, every desktop binding suite,
standalone .NET smoke/helper projects, JDBC/DBeaver packaging, Java FFM smoke,
Dart package/example analysis, and Flutter package tests/example analysis. It also
runs storage crash/soak harnesses, benchmark smoke checks, rustdoc/API docs, strict
MkDocs validation, and browser WASM OPFS smoke coverage.

Selected checks are required: missing toolchains fail preflight. Install the
toolchains described in [Building](building.md), including a full JDK with JNI
headers, Flutter, and a DBeaver SDK whose `plugins` directory is selected with
`DBEAVER_PLUGIN_DIR`. Cargo operations use `--locked`, Node packages use `npm ci`,
Node engine requirements are enforced, and Dart/Flutter restores enforce their
lockfiles. Changes to tracked dependency files during validation fail the run.
Compiler/analyzer warnings are rejected for
Rust, .NET, Java/JNI, C, and Dart/Flutter; Python tests reject unexpected warnings,
and documentation builds reject warnings too.
The FFM smoke test explicitly acknowledges the restricted native APIs in its
three native-call methods; execution still requires `--enable-native-access`.

Use `--only CHECK` for focused checks. An intentional exclusion requires
`--skip CHECK`; excluded checks appear in an **INCOMPLETE** result, even if the
executed checks succeed. Excluding every selected check fails. Each check has a
30-minute timeout; override it with `--timeout-seconds SECONDS`. Timeout or Ctrl+C
terminates the check's child processes. Logs and a machine-readable `summary.json`
are written under `.tmp/pre-commit-checks/`.

SQLite benchmark comparisons are informational because local timings depend on
the host and background load. Their per-run comparison JSON is saved beside the
logs, and saved baselines remain unchanged unless explicitly accepted with
`--update-benchmark-baselines`. Benchmark command failures still fail validation.
Windows/macOS/mobile artifact builds and longer sanitizer, Miri, and fuzz runs
remain separate GitHub Actions checks; a local pass does not establish those results.

## Focused Rust test commands

The indexed-seek regression suites retain result and query-plan assertions
under LLVM coverage and sanitizer instrumentation. Wall-clock performance gates
apply only to uninstrumented runs: `LLVM_PROFILE_FILE` identifies coverage
instrumentation and `DECENTDB_SANITIZER` identifies sanitizer runs. Run those
tests normally as well when validating query performance.

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

## Memory-safety nightly

`.github/workflows/memory-safety-nightly.yml` runs Rust address/leak sanitizers,
Miri, fuzzing, C/Python Memcheck probes, and binding lifecycle tests. Reproduce
the C and Python probe commands from that workflow after building the debug
native library.

Both Memcheck jobs load `.github/valgrind/rust.supp`. Its exceptions cover only
possible leaks at the known Rust bootstrap thread-state and static coordination
registry allocation sites. Those process-lifetime allocations can be reached
through interior pointers; reader reservations and file descriptors still close
normally, while registry capacity is retained for reuse. The Python probe also
loads `.github/valgrind/python.supp` for interpreter allocations.

Keep `--errors-for-leak-kinds=definite,possible` and `--error-exitcode=1` enabled.
When changing suppressions, compare reports from short and long connection
churn runs to check that retained state stays bounded, and confirm that a probe
with a deliberate unsuppressed leak still exits with an error. Do not suppress
an entire library or all possible leaks.

## Package-local binding suites

Several language integrations also keep package-local tests under `bindings/`.

### .NET packages

```bash
cd bindings/dotnet
dotnet test --solution DecentDB.NET.sln -v minimal
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
