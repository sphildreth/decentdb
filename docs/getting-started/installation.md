# Installation

## Prefer prebuilt releases

GitHub Releases publish native archives for:

- `decentdb-<tag>-Linux-x64.tar.gz`
- `decentdb-<tag>-Linux-arm64.tar.gz`
- `decentdb-<tag>-macOS-arm64.tar.gz`
- `decentdb-<tag>-Windows-x64.zip`

For Dart/Flutter desktop consumers, Releases also publish native-library-only
archives:

- `decentdb-dart-native-<tag>-Linux-x64.tar.gz`
- `decentdb-dart-native-<tag>-Linux-arm64.tar.gz`
- `decentdb-dart-native-<tag>-macOS-arm64.tar.gz`
- `decentdb-dart-native-<tag>-Windows-x64.zip`

The main `decentdb-<tag>-...` archives contain the CLI plus the native shared
library used by bindings. The `decentdb-dart-native-<tag>-...` archives contain
only the shared library for Flutter/Dart desktop packaging. Extract the archive
that matches your use case and place `decentdb` (or `decentdb.exe`) on your
`PATH` when you install the full CLI archive.

## Build from source

### 1. Install Rust

Follow the official Rust installation guide at [rustup.rs](https://rustup.rs).

### 2. Clone the repository

```bash
git clone https://github.com/sphildreth/decentdb.git
cd decentdb
```

### 3. Build the workspace

```bash
cargo build --workspace
```

### 4. Install the CLI (optional)

```bash
cargo install --path crates/decentdb-cli
```

### 5. Verify the CLI

```bash
decentdb --help
```

If you did not install the CLI, you can also run the debug binary directly:

```bash
./target/debug/decentdb --help
```

## Native library for bindings

Build the core crate to produce the shared library used by bindings:

```bash
cargo build -p decentdb
```

That produces:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

See the binding-specific docs for higher-level usage:

- [.NET](../api/dotnet.md)
- [Go](../api/go.md)
- [Python](../api/python.md)
- [Node](../api/node.md)
- [Dart](../api/dart.md)
- [JDBC](../api/jdbc.md)
