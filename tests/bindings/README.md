# Binding Smoke and Validation

- `python/` and `dotnet/` contain the stronger Phase 4.2 validation suites over the stable C ABI.
- `c/`, `go/`, `java/`, `node/`, and `dart/` contain the narrow Phase 4.3 release smoke programs.
- All binding tests assume the Rust cdylib has already been built at `target/debug/libdecentdb.so` unless `DECENTDB_NATIVE_LIB` overrides the Python loader.
- Cross-binding concurrent write shape for queue-backed writes is defined in
  [`concurrent-write-scenario-definition.md`](concurrent-write-scenario-definition.md).
