# ADR-0039: .NET C API Design (P/Invoke over C ABI)

**Status**: Accepted
**Date**: 2026-01-30

## Context
DecentDB needs to be callable from .NET (ADO.NET provider + Micro-ORM) without a server process. This requires a native interop surface that is:
- stable and portable across Linux/Windows/macOS
- fast for streaming SELECT results
- safe with explicit ownership and lifetimes

## Decision
- Expose a C ABI from the Nim engine and bind it from .NET via P/Invoke.
- Use opaque handles for database and prepared statement lifetimes.
- Implement streaming reads via `prepare/bind/step/column_*` to back `DbDataReader` without materializing full result sets.
- Error reporting uses numeric error codes plus a UTF-8 message accessible via `decentdb_last_error_code` / `decentdb_last_error_message`.

## Consequences
- **Pros**: No toolchain requirements beyond a shared library; works with any .NET runtime; supports forward-only streaming reads efficiently.
- **Cons**: Requires careful pointer lifetime rules (borrowed string/blob views must be copied immediately); cross-thread statement usage is unsafe.

## References
- design/adr/0010-error-handling-strategy.md
- design/adr/0011-memory-management-strategy.md
- design/DAPPER_SUPPORT.md (Phase 1 / Phase 2)
- src/c_api.nim
- bindings/dotnet/src/DecentDb.Native/NativeMethods.cs
