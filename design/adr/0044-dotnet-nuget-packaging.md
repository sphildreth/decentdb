# ADR-0044: .NET / NuGet Packaging Strategy

**Status**: Accepted
**Date**: 2026-01-30

## Context
For a good Dapper experience, .NET users should be able to add a single NuGet package that brings:
- managed assemblies (ADO.NET provider, Micro-ORM, native interop)
- the correct native library for their RID under `runtimes/{rid}/native/`

This repository currently supports local builds and test runs without external packaging.

## Decision
- The 0.x baseline continues to support local builds (repo native build output + MSBuild copy targets).
- The planned distribution shape is a meta-package (e.g. `DecentDB.NET`) that references managed assemblies and includes platform-specific native binaries under `runtimes/`.
- CI packaging and RID matrix publication are deferred until the native build pipeline is finalized.

## Consequences
- **Pros**: Keeps the 0.x baseline focused on correctness and API compatibility.
- **Cons**: End-user experience requires either local native library placement or a future NuGet release.

## References
- design/DAPPER_SUPPORT.md (NuGet Package Distribution)
- bindings/dotnet/src/DecentDB.Native/DecentDB.targets
