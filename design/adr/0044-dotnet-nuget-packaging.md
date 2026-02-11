# ADR-0044: .NET / NuGet Packaging Strategy

**Status**: Accepted (Implemented)
**Date**: 2026-01-30
**Updated**: 2026-02-11

## Context
For a good Dapper experience, .NET users should be able to add a single NuGet package that brings:
- managed assemblies (ADO.NET provider, Micro-ORM, native interop)
- the correct native library for their RID under `runtimes/{rid}/native/`

This repository also supports local builds and test runs from source.

## Decision
- We ship a single published NuGet package: **`DecentDB.MicroOrm`**.
  - It includes `DecentDB.MicroOrm`, `DecentDB.AdoNet`, and `DecentDB.Native` under `lib/net10.0/`.
  - It includes the native engine under `runtimes/{rid}/native/`.
- CI builds and publishes a RID matrix package for `linux-x64`, `osx-x64`, and `win-x64` via `.github/workflows/nuget.yml`.
- Local-from-source workflows remain supported (repo native build output + MSBuild copy targets).

## Consequences
- **Pros**: One-package install for .NET apps; native assets resolved via standard NuGet RID selection.
- **Cons**: Native assets are currently limited to the published RID set; additional RIDs require extending CI.

## References
- design/DAPPER_SUPPORT.md (NuGet Package Distribution)
- .github/workflows/nuget.yml
- bindings/dotnet/src/DecentDB.Native/DecentDB.targets
