# ADR-0044: .NET / NuGet Packaging Strategy

**Status**: Superseded by ADR-0093
**Date**: 2026-01-30
**Updated**: 2026-02-13

## Context
For a good Dapper experience, .NET users should be able to add a single NuGet package that brings:
- managed assemblies (ADO.NET provider, Micro-ORM, native interop)
- the correct native library for their RID under `runtimes/{rid}/native/`

This repository also supports local builds and test runs from source.

## Decision
- Historical decision (2026-01-30): publish a single package (`DecentDB.MicroOrm`) containing managed + native layers.
- This ADR is now superseded by ADR-0093 (2026-02-13), which changes .NET publishing to:
  - publish both `DecentDB.MicroOrm` and `DecentDB.AdoNet`
  - keep the same RID runtime asset matrix (`linux-x64`, `osx-x64`, `win-x64`)
  - keep package versions in sync via CI-supplied `PackageVersion`
  - accept short-term native asset duplication across both packages

## Consequences
- **Pros**: EF-provider consumers can depend on `DecentDB.AdoNet` directly without taking `DecentDB.MicroOrm`; MicroOrm users keep the one-package install.
- **Cons**: Two packages must be published per release and native assets are duplicated in the short term.

## References
- design/DAPPER_SUPPORT.md (NuGet Package Distribution)
- .github/workflows/nuget.yml
- bindings/dotnet/src/DecentDB.Native/DecentDB.targets
- design/adr/0093-dotnet-nuget-packaging-adonet-package.md
