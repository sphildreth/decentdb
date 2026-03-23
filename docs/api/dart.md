# Dart Smoke Coverage

The Dart smoke test uses `dart:ffi` over the stable DecentDB C ABI.

Files:

```text
tests/bindings/dart/pubspec.yaml
tests/bindings/dart/smoke.dart
```

It proves:
- library load
- database open
- one write
- one read
- one error path

## Run locally

```bash
cargo build -p decentdb
cd tests/bindings/dart
dart pub get
dart run smoke.dart
```
