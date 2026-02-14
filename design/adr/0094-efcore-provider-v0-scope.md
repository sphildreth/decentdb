## ADR-0094: EF Core Provider v0 Scope
**Date:** 2026-02-13
**Status:** Accepted

### Decision

- Introduce a Phase 1 provider skeleton package: `DecentDB.EntityFrameworkCore`.
- v0 scope for this first provider increment is limited to:
  - `UseDecentDB(connectionString, optionsAction)` and `UseDecentDB(DbConnection, optionsAction)` entrypoints.
  - EF Core options extension + relational provider service registration wiring.
  - basic provider smoke coverage proving configuration and connection open through EF relational connection services.
- Explicitly out of scope for v0 Phase 1:
  - query translation and SQL generation
  - save/update pipeline
  - migrations and design-time tooling
  - NodaTime extension package

### Rationale

- Keeps the first EF Core milestone small and testable while unblocking issue #20 (`UseDecentDB` provider entrypoint).
- Aligns with the phased plan by delivering provider wiring before translation/update/migrations layers.
- Reduces risk by using existing `DecentDB.AdoNet` connection behavior for initial connectivity.

### Alternatives Considered

- Build query translation and SaveChanges support in the same phase.
  - Rejected: too large for a first integration slice and harder to debug.
- Delay provider code until all relational components are ready.
  - Rejected: postpones API validation and CI integration feedback.

### Trade-offs

- Early provider package compiles and configures but is intentionally feature-incomplete.
- Additional phases are required before EF Core usage beyond connectivity smoke tests.

### References

- `design/EF_CORE_PROVIDER_PLAN.md`
- `design/EF_CORE_PROVIDER_IMPLEMENATION_PLAN.md`
- `design/adr/0093-dotnet-nuget-packaging-adonet-package.md`
- https://github.com/sphildreth/decentdb/issues/20
