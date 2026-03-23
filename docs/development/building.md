# Building from Source

This repository is the Rust rewrite of DecentDB. The supported local workflow is
Cargo-based.

## Prerequisites

- [Rust via rustup](https://rustup.rs)
- A normal platform toolchain capable of building Rust crates with native code
  dependencies (for example `build-essential` on Debian/Ubuntu or Xcode command
  line tools on macOS)

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
