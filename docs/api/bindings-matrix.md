# Binding Compatibility Matrix

| Binding surface | Scope in Rust Phase 4 | Source of truth |
|---|---|---|
| C ABI | Stable handle-based core surface | `include/decentdb.h`, `crates/decentdb/src/c_api.rs` |
| Python | Validation suite over C ABI | `tests/bindings/python/test_ffi.py` |
| .NET | Validation suite over C ABI | `tests/bindings/dotnet/Smoke/` |
| Go | Release smoke | `tests/bindings/go/smoke.go` |
| Java | Release smoke | `tests/bindings/java/Smoke.java` |
| Node | Release smoke | `tests/bindings/node/` |
| Dart | Release smoke | `tests/bindings/dart/` |

All non-C bindings in the Rust rewrite currently validate the C ABI directly. Higher-level packaged language APIs remain post-Phase-4 work.

Packaged language integrations will live under `bindings/` as they are ported.
`tests/bindings/` remains the validation and smoke-test layer.
