# ADR-0040: .NET Type System Mapping

**Status**: Accepted
**Date**: 2026-01-30

## Context
Dapper and other ADO.NET consumers expect predictable parameter and result type behavior. DecentDB’s storage types differ from .NET’s rich type system (DateTime/Guid/etc.), so the provider must define a stable mapping that preserves correctness and enables indexing and efficient comparisons.

## Decision
- Map integral .NET numeric types (`short`, `int`, `long`, and unsigned variants) to DecentDB INT64.
- Map `bool` to DecentDB BOOL stored as INT64 0/1 at the ABI boundary.
- Map `float`/`double` to DecentDB FLOAT64.
- Map `string` to DecentDB TEXT (UTF-8), with optional .NET-side length guardrails enforced via parameter `Size` in **UTF-8 bytes**.
- Map `byte[]` to DecentDB BLOB.
- Map `Guid` to BLOB(16) using `Guid.ToByteArray()` / `new Guid(byte[])`.
- Map temporal types to INT64:
  - `DateTime` and `DateTimeOffset`: Unix epoch milliseconds (UTC)
  - `DateOnly`: days since Unix epoch
  - `TimeOnly`: ticks since midnight
  - `TimeSpan`: ticks
- Map `decimal` to TEXT using invariant string representation for precision preservation.
- Map enums to INT64 via underlying numeric value.

## Consequences
- **Pros**: Fast comparisons and range queries for time types; predictable behavior across platforms; minimal storage types.
- **Cons**: `decimal` is not natively numeric; inspecting temporal values in raw SQL is less human-friendly.

## References
- design/adr/0005-sql-parameterization-style.md
- design/DAPPER_SUPPORT.md (Phase 4)
- bindings/dotnet/src/DecentDB.AdoNet/DecentDBCommand.cs
- bindings/dotnet/src/DecentDB.AdoNet/DecentDBDataReader.cs
