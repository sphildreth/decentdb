## ADR-0093: Publish DecentDB.AdoNet as a NuGet Package
**Date:** 2026-02-13
**Status:** Accepted

### Decision

- Publish `DecentDB.AdoNet` as a standalone NuGet package.
  - It must include:
    - `DecentDB.AdoNet` managed assembly
    - `DecentDB.Native` managed assembly
    - native engine assets under `runtimes/{rid}/native/` for the same RID matrix currently published
- EF Core provider package(s) (`DecentDB.EntityFrameworkCore*`) depend on `DecentDB.AdoNet` (not on `DecentDB.MicroOrm`).
- `DecentDB.MicroOrm` should remain the “one-liner” package for MicroOrm users, but should not be required for EF Core scenarios.
- Packaging policy for Phase 0:
  - `DecentDB.AdoNet` includes `DecentDB.Native.dll` and ships native runtime assets directly.
  - `DecentDB.MicroOrm` remains self-contained and also ships native runtime assets.
  - Short-term runtime asset duplication across both packages is explicitly accepted to minimize churn.
- Versioning policy for .NET NuGet packages:
  - `DecentDB.MicroOrm` and `DecentDB.AdoNet` use the same package version.
  - Version is sourced in CI (`dotnet pack -p:PackageVersion=...` from the release tag), not hard-coded in each `.csproj`.

### Rationale

- EF Core providers are fundamentally ADO.NET-based; depending on `DecentDB.AdoNet` matches the conceptual layering.
- Keeps dependency graphs smaller and avoids pulling `DecentDB.MicroOrm` into EF-only apps (a common review concern).
- Makes it possible to use DecentDB via ADO.NET (and Dapper) without taking the MicroOrm layer.

### Alternatives Considered

- Keep the single-package strategy (ADR-0044) and have the EF provider depend on `DecentDB.MicroOrm`.
  - Rejected: forces MicroOrm onto EF consumers and creates avoidable dependency/review churn.
- Publish a separate “runtime/native assets” package and keep `DecentDB.AdoNet` managed-only.
  - Deferred: may be worthwhile if native asset reuse becomes a problem, but increases package count and complexity now.
- Publish `DecentDB.Native` as its own NuGet package and have `DecentDB.AdoNet` depend on it.
  - Viable, but increases package count; only do this if it materially simplifies packing/build.

### Trade-offs

- More packages to version and publish (CI/workflow changes).
- Runtime assets are duplicated in both .NET packages in the short term (larger total download footprint).
- If native assets move to `DecentDB.AdoNet`, existing “single package contains everything” messaging needs updating.

### References

- `design/adr/0044-dotnet-nuget-packaging.md` (current single-package strategy)
- `design/EF_CORE_PROVIDER_PLAN.md` (EF Core provider plan; packaging prerequisite)
- `.github/workflows/nuget.yml` (current NuGet publishing pipeline)
