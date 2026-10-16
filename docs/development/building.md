# Building from Source

This repository contains the DecentDB Rust engine. The supported local workflow
is Cargo-based.

## Prerequisites

- [Rust via rustup](https://rustup.rs)
- A normal platform toolchain capable of building Rust crates with native code
  dependencies (for example `build-essential` on Debian/Ubuntu or Xcode command
  line tools on macOS)

## Dependency toolchains

The stable dependency refresh uses Rust 1.99.1+, Node 24.22.0 LTS with npm
12.2.0, .NET 10 LTS (SDK selected by `global.json`), Java 25 LTS with Gradle
9.8.1, Go 1.27.2, Python 3.15, and Flutter 3.47.7 / Dart 3.13.5. Android
builds use API 37 and NDK r30 LTS (30.0.16248370). Python
consumers require 3.12+; JDBC and DBeaver bytecode still target Java 17 and 21.
The web build requires wasm-bindgen CLI 0.2.129, matching the Rust crate exactly.
Browser CI installs the official prebuilt CLI and verifies its published SHA256.

Install the binding's test extras with `python -m pip install -e
'bindings/python[test]'`. Benchmark tooling additionally uses
`benchmarks/python_embedded_compare/requirements.txt`; building JPype on Python
3.15 requires a full JDK, Ant, CMake, and a C++ compiler. Use
`docs/requirements.txt` for the documentation toolchain.

Build the optional Python decoder for the active interpreter with
`python scripts/build_python_fastdecode.py`; this uses the host C compiler and
the setuptools version in the test extras. .NET tests use xUnit v3 and the
Microsoft.Testing.Platform runner selected by `global.json`; invoke them with
`dotnet test --solution bindings/dotnet/DecentDB.NET.sln`. The pre-commit smoke check builds
it automatically before importing it.

Run `python scripts/do-pre-commit-checks.py --mode paranoid` for the full local
suite. `python scripts/validate_dependency_platform.py` additionally builds and
checks every desktop binding, standalone .NET helper, and the DBeaver extension;
it requires all listed desktop toolchains and downloads the DBeaver 26.2.2 SDK
under `.tmp/`. GitHub's **Dependency Platform Validation** workflow runs it on
Linux x64/arm64, Windows x64, and macOS x64/arm64. **Mobile Native Artifacts** builds
Android/iOS native artifacts and the unsigned Flutter example applications.
Neither pull-request workflow publishes a release or package.

Dependencies constrained by upstream packages remain visible in the lockfiles:
DuckDB's build dependency currently requires the already-locked `zip` 6.0.0
(which upstream has yanked), while stable Flutter pins `material_color_utilities`
and `test_api`. Do not delete the lockfile or force incompatible overrides.
The Rust advisory checks remain enabled without ignored advisories. NuGet
transitive packages follow the supported EF Core and BenchmarkDotNet dependency
graphs; their lower version requirements are not forced across major versions.
The fuzz audit explicitly allows the NCSA license used by LLVM libFuzzer.

## Build the workspace

From the repository root:

```bash
cargo build --workspace
```

That builds:

- `crates/decentdb` — the core engine crate plus the shared library artifact
- `crates/decentdb-cli` — the `decentdb` command-line tool

## Useful build targets

Build just the native library crate:

```bash
cargo build -p decentdb
```

Build just the CLI binary:

```bash
cargo build -p decentdb-cli
```

Optimized release build:

```bash
cargo build --workspace --release
```

## Shared library outputs

`cargo build -p decentdb` produces the Rust `cdylib` used by the binding smoke
programs and the in-tree Dart package:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

Release builds use the same names under `target/release/`.

## Install the CLI locally

```bash
cargo install --path crates/decentdb-cli
```

That places `decentdb` in Cargo's bin directory (typically `~/.cargo/bin`).

## Helpful Cargo aliases

The repository ships a few convenience aliases in `.cargo/config.toml`:

```bash
cargo t         # nextest run
cargo test-all  # nextest run --all-features
cargo lint      # clippy --all-targets --all-features -- -D warnings
```

Use plain `cargo build`, `cargo test`, and `cargo clippy` if you prefer not to
rely on aliases.
