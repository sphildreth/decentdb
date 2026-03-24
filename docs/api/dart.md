# Dart Binding

DecentDB ships two Dart-facing validation layers:

- `tests/bindings/dart/` for the narrow smoke test over the raw `ddb_*` C ABI
- `bindings/dart/dart/` for the packaged Dart wrapper API

The packaged wrapper exposes:

- `Database` lifecycle, query, transaction, checkpoint, and `saveAs()` helpers
- a Dart-side `Statement` convenience wrapper for parameter binding and paging
- `Schema` helpers backed by metadata JSON exported from the stable C ABI

## Build the native library

From the repository root:

```bash
cargo build -p decentdb
```

This produces the shared library used by both the smoke test and the packaged
Dart wrapper:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

## Run the release smoke test

```bash
cargo build -p decentdb
cd tests/bindings/dart
dart pub get
dart run smoke.dart
```

## Run the packaged Dart suite

```bash
bindings/dart/scripts/run_tests.sh
```

Equivalent manual commands:

```bash
cargo build -p decentdb
cd bindings/dart/dart
dart pub get
DECENTDB_NATIVE_LIB=../../../target/debug/libdecentdb.so dart test --reporter expanded
```

See `bindings/dart/README.md` for the higher-level package API and example code.
