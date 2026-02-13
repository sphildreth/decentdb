# EF Core Provider Phased Implementation Plan (DecentDB)

This document is the execution checklist for `design/EF_CORE_PROVIDER_PLAN.md`. It breaks the work into phases with concrete deliverables, tests, and documentation updates.

Conventions:
- Each phase starts with a checkbox; keep it unchecked until the phase exit criteria are met.
- Any dependency/architecture change requires an ADR under `design/adr/`.
- Code locations are for this repository layout: .NET bindings under `bindings/dotnet/`.

---

## Phase 0: Prereqs, ADRs, and Packaging Foundation

- [ ] Phase 0 complete
- [ ] Accept ADR-0093 (`design/adr/0093-dotnet-nuget-packaging-adonet-package.md`) and update/supersede ADR-0044 (`design/adr/0044-dotnet-nuget-packaging.md`)
- [ ] Decide and document the NuGet layout for native runtime assets when publishing `DecentDB.AdoNet` (RID matrix, file paths, duplication policy)
- [ ] Make `bindings/dotnet/src/DecentDB.AdoNet/DecentDB.AdoNet.csproj` packable and publishable (PackageId/metadata)
- [ ] Ensure `DecentDB.AdoNet` NuGet includes native engine assets under `runtimes/{rid}/native/` (same RID set as today)
- [ ] Ensure `DecentDB.AdoNet` consumers get `DecentDB.Native` correctly (dependency or included assembly)
- [ ] Update CI to build/test/pack/publish `DecentDB.AdoNet` (and keep `DecentDB.MicroOrm` publishing intact)
- [ ] Add/confirm a versioning policy for multiple .NET packages published from this repo
- [ ] Documentation: update `README.md` or `docs/` to mention `DecentDB.AdoNet` as a published package

Exit criteria:
- [ ] CI can produce/publish a `DecentDB.AdoNet` package with correct runtime assets
- [ ] Existing .NET tests still pass

---

## Phase 1: v0 Scope ADR + Provider Skeleton (M0 Start)

- [ ] Phase 1 complete
- [ ] Create and accept the v0 scope ADR (`design/adr/00xx-efcore-provider-v0-scope.md`)
- [ ] Create `bindings/dotnet/src/DecentDB.EntityFrameworkCore/DecentDB.EntityFrameworkCore.csproj`
- [ ] Add project to `bindings/dotnet/DecentDB.NET.sln` and CI build/test scripts
- [ ] Add `UseDecentDb(connectionString, optionsAction)` and (optional) `UseDecentDb(DbConnection, optionsAction)`
- [ ] Implement provider `IDbContextOptionsExtension` and service registration (Relational provider wiring)
- [ ] Basic connection validation and “can connect” smoke test
- [ ] Unit tests: create `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests` (or similar) with xUnit; add first smoke tests
- [ ] Documentation: add a minimal usage snippet (DI + `AddDbContextFactory` example) and link DecentDB issue #20

Exit criteria:
- [ ] `DbContext` can be configured with `UseDecentDb(...)` and open a connection
- [ ] Tests run in CI on Windows/macOS/Linux

---

## Phase 2: Relational Type Mapping (M0)

- [ ] Phase 2 complete
- [ ] Implement `RelationalTypeMappingSource` for DecentDB-supported types
- [ ] Implement SQL literal generation/quoting helpers
- [ ] Align date/time storage conventions with existing .NET rules (epoch/ticks/day-number) and document them
- [ ] Validate DECIMAL/UUID behavior against `design/adr/0091-decimal-uuid-implementation.md`
- [ ] Unit tests: type mapping round-trip tests (CLR -> parameter -> storage -> materialization)
- [ ] Documentation: provider type mapping table (initial v0 set)

Exit criteria:
- [ ] Common CLR types map deterministically and round-trip in tests

---

## Phase 3: Query Translation + SQL Generation (M0)

- [ ] Phase 3 complete
- [ ] Implement query SQL generator pipeline (expression factory + query SQL generator + translators)
- [ ] Ensure generated SQL targets DecentDB’s supported subset (`design/SPEC.md`), not “full PostgreSQL”
- [ ] Implement paging translation using `LIMIT/OFFSET` (avoid `OFFSET ... FETCH` unless engine support is added)
- [ ] Implement null/bool semantics (`IS NULL`, `TRUE/FALSE`) consistent with DecentDB
- [ ] Implement common string ops (`Contains`, `StartsWith`, `EndsWith`) with correct LIKE escaping
- [ ] Define a policy for large `IN (...)` lists (max size error vs alternate rewrite; document)
- [ ] Unit tests: SQL generation tests for supported LINQ patterns (golden SQL + parameters)
- [ ] Integration tests: execute a small set of translated queries against a real DecentDB file

Exit criteria:
- [ ] Basic LINQ queries work end-to-end with parameters and correct results
- [ ] A conformance test seed set is running (even if tiny)

---

## Phase 4: Execution, Diagnostics, and Error Mapping (M0)

- [ ] Phase 4 complete
- [ ] Implement `RelationalConnection` integration using `DecentDB.AdoNet`
- [ ] Implement command/parameter creation aligned with DecentDB positional parameters (`$1`, `$2`, ...) (see `design/adr/0005-sql-parameterization-style.md`)
- [ ] Implement transaction integration (explicit `BeginTransaction` and best-effort `TransactionScope`)
- [ ] Implement error/exception mapping:
- [ ] Define and document the mapping policy (table or code-first)
- [ ] Map concurrency/update failures to EF exceptions (`DbUpdateException`, `DbUpdateConcurrencyException`) where applicable
- [ ] Define a conservative transient failure policy; add `IExecutionStrategy` only if justified
- [ ] Add provider diagnostics/logging hooks (EF Core logging + timings where feasible)
- [ ] Unit tests: exception mapping tests (error code -> expected EF exception type)
- [ ] Integration tests: transaction tests (commit/rollback) and exception mapping with real engine errors

Exit criteria:
- [ ] EF diagnostics show executed SQL and parameters (at least at debug logging)
- [ ] Exceptions surfaced to EF callers are predictable and tested

---

## Phase 5: Conformance Ramp (M0 Exit)

- [ ] Phase 5 complete
- [ ] Choose and pin the EF Core provider test source:
- [ ] Vendor a pinned subset from EF Core repo, or implement an equivalent harness
- [ ] Create and maintain a skip list (every skip has an issue link + reason)
- [ ] Expand conformance coverage for:
- [ ] Basic query translation
- [ ] Parameters
- [ ] Includes + `AsSplitQuery`
- [ ] Transactions
- [ ] Add CI gates for the conformance suite subset
- [ ] Documentation: testing/CI notes for running the EF provider tests locally

Exit criteria:
- [ ] “M0” definition in `design/EF_CORE_PROVIDER_PLAN.md` is met with repeatable tests in CI

---

## Phase 6: SaveChanges / Update Pipeline (M1)

- [ ] Phase 6 complete
- [ ] Implement `UpdateSqlGenerator` and modification command batching
- [ ] Support identity key propagation via `INSERT ... RETURNING`
- [ ] Support database defaults via `INSERT ... RETURNING` where DecentDB returns computed values
- [ ] Implement concurrency token checks
- [ ] Confirm behavior for affected rows and concurrency exceptions is EF-correct
- [ ] Conformance tests: EF update pipeline subset (insert/update/delete, concurrency)
- [ ] Integration tests: multi-row insert/update/delete and transaction interactions
- [ ] Documentation: supported generated value behaviors and known limitations

Exit criteria:
- [ ] `SaveChanges` works for typical entity graphs with generated keys and concurrency

---

## Phase 7: Migrations Runtime (M2)

- [ ] Phase 7 complete
- [ ] Implement `IRelationalDatabaseCreator` with idempotent `EnsureCreated/EnsureDeleted` semantics
- [ ] Implement `IHistoryRepository` for `__EFMigrationsHistory`
- [ ] Implement `IMigrationsSqlGenerator` emitting DecentDB DDL (limited to DecentDB-supported DDL subset)
- [ ] Fail-fast for unsupported DDL with actionable error messages
- [ ] (Optional, post-M2) ADR + engine validation for any “table rebuild” behavior; add crash/recovery tests before enabling
- [ ] Integration tests: apply migrations to empty DB, then upgrade an existing DB
- [ ] Documentation: supported DDL operations and migration limitations/workarounds

Exit criteria:
- [ ] Migrations can be applied programmatically (`context.Database.Migrate()`) with tested outcomes

---

## Phase 8: Migrations Design-Time (M2 Prereq)

- [ ] Phase 8 complete
- [ ] Create `DecentDB.EntityFrameworkCore.Design` package and register `IDesignTimeServices`
- [ ] Ensure `dotnet ef migrations add` works for a minimal sample project
- [ ] Ensure `dotnet ef database update` works via the provider
- [ ] Add a “getting started” docs page for EF provider migrations (commands + minimal configuration)
- [ ] Tests: add at least one integration test project that exercises design-time tooling in CI (as feasible)

Exit criteria:
- [ ] `dotnet ef migrations add` and `dotnet ef database update` work with DecentDB provider in a representative sample

---

## Phase 9: Scaffolding / Reverse Engineering (Optional for v1+)

- [ ] Phase 9 complete
- [ ] Implement `IDatabaseModelFactory` using DecentDB schema discovery
- [ ] Generate DbContext/entity code consistent with provider type mappings
- [ ] Handle key schema edge cases (composite keys, indexes, nullability, defaults)
- [ ] Tests: schema discovery + scaffolding output validation (non-snapshot where possible)
- [ ] Documentation: scaffolding usage and limitations

Exit criteria:
- [ ] `dotnet ef dbcontext scaffold` (or equivalent) produces usable models for common schemas

---

## Phase 10: NodaTime Extension (Parallel Track)

- [ ] Phase 10 complete
- [ ] Create `DecentDB.EntityFrameworkCore.NodaTime` package
- [ ] Implement `UseNodaTime()` extension wiring
- [ ] Add NodaTime type mappings + converters aligned with DecentDB storage conventions
- [ ] Tests: NodaTime round-trip integration tests
- [ ] Documentation: which NodaTime types are supported and their storage representation

Exit criteria:
- [ ] NodaTime types round-trip correctly in EF queries and SaveChanges

---

## Phase 11: GA Hardening (M3)

- [ ] Phase 11 complete
- [ ] Expand conformance suite coverage toward “expected provider” behavior (GroupBy/aggregates, set ops, correlated subqueries) per `design/EF_CORE_PROVIDER_PLAN.md`
- [ ] Keep skip list small and justified; track gaps as issues with clear engine/provider prerequisites
- [ ] Performance: implement and run the benchmark suite:
- [ ] Translation time
- [ ] End-to-end latency vs raw `DecentDB.AdoNet`
- [ ] Allocations per query
- [ ] Validate `AddDbContextPool` correctness and throughput impact
- [ ] Documentation: supported feature matrix, known gaps, and compatibility notes

Exit criteria:
- [ ] Provider meets M3 “GA” criteria in `design/EF_CORE_PROVIDER_PLAN.md`

---

## Phase 12: Release and Version Bump (DecentDB 1.1.0)

- [ ] Phase 12 complete
- [ ] Bump DecentDB version to `1.1.0` across:
  - [ ] Nim package metadata (`decentdb.nimble`)
  - [ ] .NET package versions for all published packages (`DecentDB.MicroOrm`, `DecentDB.AdoNet`, `DecentDB.EntityFrameworkCore*`)
  - [ ] Any version references in docs/examples (search repo-wide for previous version strings)
- [ ] Update `CHANGELOG.md` with a `1.1.0` entry describing:
  - [ ] EF Core provider introduction and major supported features (query pipeline, SaveChanges, migrations)
  - [ ] New/changed NuGet publishing model (standalone `DecentDB.AdoNet` if adopted)
  - [ ] Notable limitations / known gaps (with issue links)
- [ ] Documentation sweep:
  - [ ] Add EF Core “getting started” and “migrations” docs (if not already done)
  - [ ] Add package matrix and installation guidance (MicroOrm vs AdoNet vs EF provider)
  - [ ] Ensure docs reflect DecentDB’s SQL subset and any provider-specific translation notes
- [ ] Final CI validation for release:
  - [ ] Tests green (engine + .NET)
  - [ ] NuGet packaging artifacts include correct runtime assets

Exit criteria:
- [ ] Repo version and documentation consistently reflect `1.1.0`
- [ ] Release notes are complete and accurate for users upgrading to `1.1.0`
