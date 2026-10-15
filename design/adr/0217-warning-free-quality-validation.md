# ADR 0217: Warning-Free Quality Validation

**Date:** 2026-10-13
**Status:** Accepted

## Context

The Bun integration must pass the complete pre-commit suite without build
warnings or errors. Native-only bootstrap and coordination code currently
produces warnings in WASM builds. The old Callgrind benchmark dependency also
introduces a future compiler error through unmaintained proc-macro-error2.

## Decision

- Compile native bootstrap and process-coordination initialization helpers only
  for native targets. Preserve existing WASM unsupported-mode errors and native
  durability, retry, and coordination behavior. Do not silence compiler lints.
- Replace the dev-only iai-callgrind dependency with its maintained successor,
  Gungraun 0.20.0, which supports the workspace Rust 1.88 MSRV. Keep the existing
  Callgrind benchmark scenarios and use the matching Gungraun runner version.
  This adds no dependency to the shipped engine or bindings.
- Execute pre-commit commands in a non-login shell with the runner's explicit
  environment. Validation must not depend on shell startup scripts or prompt
  initialization. Detect usable installed JDKs by their JNI headers.
- Update the .NET benchmark Microsoft.Data.Sqlite patch release to 10.0.13,
  which resolves a patched native SQLite bundle. Keep NuGet auditing enabled.
- Generate Rust API documentation for the engine library. The CLI executable
  uses its existing user-facing CLI documentation and does not generate a second
  rustdoc target named `decentdb` that would overwrite the engine API docs.

No persistent format, WAL semantics, C ABI, or locking-contract changes.

## Validation

Run all pre-commit checks in paranoid mode, compile native and WASM targets with
warnings denied, check the Rust 1.88 MSRV, and execute the Callgrind benchmark
with the matching runner. Retain the existing bootstrap and coordination tests
and browser smoke coverage.
