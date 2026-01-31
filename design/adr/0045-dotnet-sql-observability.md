# ADR-0045: .NET SQL Observability (events + zero-cost when disabled)

**Status**: Accepted
**Date**: 2026-01-30

## Context
Embedded databases still need observability for debugging and production metrics. However, DecentDB’s performance targets require that observability adds effectively zero overhead when disabled.

## Decision
- Provide ADO.NET-layer SQL observability via events:
  - `SqlExecuting`
  - `SqlExecuted`
- Enable/disable behavior is controlled by connection string settings:
  - `Logging=0|1` (default `0`)
  - `LogLevel=Verbose|Debug|Info|Warning|Error` (default `Debug`)
- Zero-cost guarantee:
  - when `Logging=0` and there are no event subscribers, the hot path executes with a single predictable branch and no allocations.

## Consequences
- **Pros**: Works with Dapper and any ADO.NET consumer; easy to attach metrics; low overhead when off.
- **Cons**: When enabled, allocations occur to snapshot parameters and capture timing (acceptable by design).

## References
- design/DAPPER_SUPPORT.md (SQL Logging and Observability)
- bindings/dotnet/src/DecentDb.AdoNet/DecentDbConnection.cs
- bindings/dotnet/src/DecentDb.AdoNet/DecentDbCommand.cs
