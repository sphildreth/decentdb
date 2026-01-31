# ADR-0046: .NET Connection String Design

**Status**: Accepted
**Date**: 2026-01-30

## Context
Dapper expects a normal `DbConnection` accepting a connection string. DecentDB also needs configuration knobs (cache sizing, timeouts, observability) with clear defaults and validation.

## Decision
- Use a semicolon-separated key/value connection string:
  - Required: `Data Source=/path/to.db` (aliases: `Filename`, `Database`)
  - Supported options (MVP):
    - `Cache Size=<pages|NNMB>` (default `1024` pages)
    - `Logging=0|1` (default `0`)
    - `LogLevel=...` (default `Debug`)
    - `Command Timeout=<seconds>` (default `30` for commands)
- Keys are case-insensitive; whitespace around keys/values is ignored.
- Unknown keys are ignored by the engine options parser but are still available to managed-layer features.

## Consequences
- **Pros**: Familiar to ADO.NET users; supports future extension; allows managed-only features like logging without native changes.
- **Cons**: Requires consistent parsing and validation rules across ADO.NET and Micro-ORM.

## References
- design/DAPPER_SUPPORT.md (Connection String Parameters)
- bindings/dotnet/src/DecentDb.AdoNet/DecentDbConnection.cs
- src/c_api.nim (native options parsing)
