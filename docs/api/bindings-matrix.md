# Binding Compatibility Matrix

| Binding surface | Current Rust-rewrite scope | Source of truth |
|---|---|---|
| C ABI | Stable handle/result core surface | `include/decentdb.h`, `crates/decentdb/src/c_api.rs` |
| Python | Validation suite over the C ABI | `tests/bindings/python/test_ffi.py` |
| .NET | Validation suite over the C ABI | `tests/bindings/dotnet/Smoke/` |
| Go | Release smoke over the C ABI | `tests/bindings/go/smoke.go` |
| Java | Release smoke over the C ABI | `tests/bindings/java/Smoke.java` |
| Node | Release smoke over the C ABI | `tests/bindings/node/` |
| Dart smoke | Release smoke over the C ABI | `tests/bindings/dart/` |
| Dart package | In-tree packaged wrapper validated against the C ABI | `bindings/dart/dart/`, `bindings/dart/dart/test/` |

The Rust rewrite still treats the C ABI as the authoritative native surface.
Language-specific packages either validate that ABI directly or layer tested,
idiomatic wrappers on top of it.

For Dart specifically, there are now two checked layers:

- `tests/bindings/dart/` for the narrow release smoke path
- `bindings/dart/dart/` for the packaged `Database` / `Statement` / `Schema`
  wrapper
