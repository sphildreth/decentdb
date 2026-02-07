# ADR-0043: .NET String Encoding (UTF-8 end-to-end)

**Status**: Accepted
**Date**: 2026-01-30

## Context
DecentDB uses UTF-8 for TEXT and error messages at the C ABI boundary. .NET uses UTF-16 internally, so the provider must define how strings are encoded/decoded across the boundary in a portable way.

## Decision
- Encode .NET strings as UTF-8 bytes when calling native functions (`open`, `prepare`, `bind_text`).
- Decode native TEXT and error messages as UTF-8.
- Enforce optional .NET-side length guardrails (for `[MaxLength]` / parameter `Size`) in **UTF-8 bytes**, not “characters”.

## Consequences
- **Pros**: Portable across platforms; matches storage encoding; avoids ambiguous Unicode length semantics.
- **Cons**: Some APIs require measuring UTF-8 byte count, which is O(n) in string length (only performed on write/bind paths).

## References
- design/DAPPER_SUPPORT.md (Phase 4: Unicode and Encoding)
- bindings/dotnet/src/DecentDB.AdoNet/DecentDBCommand.cs
- bindings/dotnet/src/DecentDB.Native/DecentDB.cs
