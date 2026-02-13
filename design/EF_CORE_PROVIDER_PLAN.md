# EF Core Provider Plan (DecentDB)

This document describes a pragmatic plan for building a **high-performance EF Core relational provider** for DecentDB.

Key intent:
- Deliver **EF Core API compatibility** without sacrificing DecentDB performance transparency.
- Reuse existing DecentDB implementation work wherever possible.
- Keep the initial scope tight (v0), then expand based on real usage.

Related upstream issues this plan should address:
- DecentDB issue #20: EF Core `DbContextOptionsBuilder` extension (`UseDecentDb`) for EF Core compatibility.
- DecentDB issue #19: NodaTime support for date/time values (requested for MicroOrm; also relevant as an EF provider extension).

Target end state (what “done” means for this plan):
- A first-class EF Core relational provider (`DecentDB.EntityFrameworkCore`) that supports:
  - Full query pipeline (LINQ translation + Includes + split queries).
  - Full update pipeline (`SaveChanges` inserts/updates/deletes with generated values).
  - EF Core migrations end-to-end (`dotnet ef migrations add`, `dotnet ef database update`, history table).
  - Design-time services (migrations + scaffolding/reverse engineering).
  - Provider-expected relational behaviors (transactions, batching, concurrency checks, parameters, diagnostics).
- NuGet packaging supports this cleanly:
  - `DecentDB.AdoNet` is published as a standalone package (with native runtime assets).
  - `DecentDB.EntityFrameworkCore` depends on `DecentDB.AdoNet` (not on `DecentDB.MicroOrm`).
  - `DecentDB.MicroOrm` remains the “one-liner” MicroOrm package, but should not be required for EF scenarios.

Note: EF Core providers execute via **ADO.NET abstractions** (`DbConnection`/`DbCommand`/`DbDataReader`). Even if DecentDB.MicroOrm is the preferred end-user API for DecentDB, the EF Core provider will primarily route execution through **`DecentDB.AdoNet`**, while aligning type/storage semantics with the conventions already established in DecentDB.MicroOrm.

---

## 0. Milestones and Success Criteria

Milestone 0 (M0) is successful when:
- A `DbContext` can connect via `UseDecentDb(...)`.
- Basic LINQ queries translate to correct DecentDB SQL and execute correctly.
- Pagination uses `LIMIT`/`OFFSET` and parameters are used (no SQL injection footguns).
- Transactions work (explicit and ambient).
  - Note: `TransactionScope` behavior can vary across platforms; treat it as best-effort until validated on Linux/macOS in CI.
- NodaTime support is available as an optional extension package.
- `AsSplitQuery` works for typical `Include(...)` patterns.
- Provider passes a meaningful subset of EF Core relational conformance tests, covering at least:
  - basic query translation
  - parameters
  - include/split query behavior
  - transactions

Milestone 1 (M1) is successful when:
- `SaveChanges` supports inserts/updates/deletes with:
  - server-generated values (identity) via `RETURNING` where appropriate
  - concurrency token checks
  - command batching (with a pragmatic initial cap)
- Provider passes a broader subset of EF Core relational “update pipeline” tests.

Milestone 2 (M2) is successful when:
- EF Core migrations are supported end-to-end:
  - migration scaffolding works (`dotnet ef migrations add`)
  - applying migrations works (`dotnet ef database update`)
  - `__EFMigrationsHistory` is created/maintained
  - provider emits DecentDB DDL using PostgreSQL-like syntax and DecentDB’s supported DDL subset
- Unsupported DDL operations are handled with a documented strategy:
  - default fail-fast with actionable errors
  - optional, ADR-backed table rebuild only for explicitly-supported operations (post-M2)
- Note: `dotnet ef migrations add` requires design-time services; Step 10’s migrations design-time work is an M2 prerequisite.

Milestone 3 (M3 / “GA”) is successful when:
- Provider passes a substantial subset of EF Core relational specification tests with a small, well-documented skip list.
- The remaining gaps are either:
  - confirmed out-of-scope by ADR, or
  - tracked in issues with clear engine/provider prerequisites.

---

## 1. Define v0 Scope in an ADR (Required)

Create an ADR that pins down exactly what “v0” means and what is explicitly out-of-scope.

Recommended v0 scope:
- No migrations.
- Basic LINQ:
  - `Where`, `Select`, `OrderBy`, `ThenBy`, `Skip`, `Take`
  - joins: `Join`/`GroupJoin` translating to `INNER JOIN` and `LEFT JOIN` only
  - `Any`, `Count`, basic aggregates (as feasible)
- Includes (targeted subset):
  - `Include(...)` for common patterns
  - `AsSplitQuery()` for typical `Include(...)` shapes
- Raw SQL:
  - `FromSqlRaw`, `ExecuteSqlRaw` (parameterized)
- Transactions:
  - `BeginTransaction`, `UseTransaction`, `TransactionScope` interop as possible
- NodaTime:
  - in an optional package (see step 6)

Non-goals for v0 (examples):
- Migrations / scaffolding / reverse engineering
- Spatial, JSON, full-text, advanced window functions
- join types beyond `INNER`/`LEFT` (`FULL OUTER`, `CROSS`) unless explicitly added later with tests
- Provider-specific query hints
- `ExecuteUpdate`/`ExecuteDelete` (unless trivially supported)

Suggested ADR name:
- `design/adr/ADR-XXXX-efcore-provider-v0-scope.md`

---

## 2. Create `DecentDB.EntityFrameworkCore` Package

Deliverables:
- New project/package: `DecentDB.EntityFrameworkCore`
- Public entrypoints:
  - `UseDecentDb(connectionString, optionsAction)`
  - `UseDecentDb(DbConnection, optionsAction)` (optional but often useful for tests/hosting)
- Provider registration:
  - Implement EF Core `IDbContextOptionsExtension`
  - Register provider services via `EntityFrameworkRelationalServicesBuilder`

Issue linkage:
- This step is the core deliverable for DecentDB issue #20 (the `.UseDecentDb(...)` provider entrypoint analogous to `.UseSqlite(...)`).

Provider surface area should mirror other relational providers:
- `DecentDbDbContextOptionsBuilder` (provider-specific options)
- `DecentDbOptionsExtension` (stores connection string/options)
- `AddEntityFrameworkDecentDb()` (internal wiring hook)

Repo layout note:
- In this repository, .NET bindings live under `bindings/dotnet/`. The provider projects should be created under `bindings/dotnet/src/` and added to `bindings/dotnet/DecentDB.NET.sln`.
- CI/build wiring:
  - Ensure new projects are included in `.github/workflows/*` and any pack/test scripts so CI builds, tests, and publishes them.

Packaging decision (ADR required):
- The provider must be shippable via NuGet, but `DecentDB.AdoNet` is currently marked `<IsPackable>false</IsPackable>`.
- Chosen approach: publish `DecentDB.AdoNet` as a standalone NuGet package (alongside `DecentDB.MicroOrm`) and have `DecentDB.EntityFrameworkCore` depend on `DecentDB.AdoNet`.
  - Rationale:
    - EF Core provider consumers should not be forced to pull `DecentDB.MicroOrm`.
    - Keeps dependency graphs smaller and avoids “why is MicroOrm referenced?” review churn.
  - Blocking prerequisite:
    - ADR-0093 must be accepted and `DecentDB.AdoNet` must be published before the EF Core provider is considered ready for external consumption (otherwise early adopters end up on a moving packaging target).
  - Requirements:
    - Adopt/accept ADR-0093 (`design/adr/0093-dotnet-nuget-packaging-adonet-package.md`, currently Proposed) and update/supersede ADR-0044 (`design/adr/0044-dotnet-nuget-packaging.md`), which currently documents the single-package strategy.
    - Make `DecentDB.AdoNet` packable and ensure it carries the native runtime under `runtimes/{rid}/native/` (same RID matrix as current publish).
    - Ensure `DecentDB.Native` is available to consumers (either as a dependency package or included in the `DecentDB.AdoNet` package output).
  - Follow-up (strongly recommended):
    - Adjust `DecentDB.MicroOrm` packaging so it depends on `DecentDB.AdoNet` instead of embedding the same assemblies/runtime, avoiding duplicate assemblies when apps reference both MicroOrm and EF provider.

---

## 3. Build Relational Type Mapping

Goal: Map CLR types to DecentDB storage and SQL literals consistently, including existing DecentDB epoch/ticks/day-number conventions.

Deliverables:
- `DecentDbTypeMappingSource : RelationalTypeMappingSource`
- `DecentDbSqlGenerationHelper : RelationalSqlGenerationHelper` (quoting, delimiters)
- Value converters where needed to keep storage stable and performant.

Key decisions to pin down (documented in the ADR and/or provider docs):
- Integer sizes (`int` vs `long`) and overflow behavior
- Decimal/numeric precision rules (confirm DecentDB’s actual precision/scale constraints before finalizing; see `design/adr/0091-decimal-uuid-implementation.md`)
- `DateTime`/`DateTimeOffset` semantics (UTC vs local handling)
- `Guid` storage (text vs bytes) and collation/comparison behavior
- String collation/case-sensitivity expectations

Initial mapping targets (v0):
- `bool`, `byte`, `short`, `int`, `long`
- `float`, `double`, `decimal`
- `string`
- `byte[]`
- `Guid`
- `DateTime`, `DateTimeOffset`, `TimeSpan` (as supported by DecentDB)

---

## 4. Implement Query SQL Generation

Goal: Translate EF Core expression trees into correct, idiomatic DecentDB SQL with good parameterization and minimal overhead.

Core components (typical EF relational provider shape):
- `DecentDbSqlExpressionFactory`
- `DecentDbQuerySqlGenerator` (+ factory)
- Query translation/visitors:
  - method translating visitor factory
  - member translating visitor provider
  - type mapping post-processing hooks

Translation targets DecentDB’s **PostgreSQL-like dialect** (see `design/SPEC.md` for the supported subset). Do not assume full PostgreSQL compatibility; every operator/function used by the provider should be:
- Confirmed supported by DecentDB (SPEC/ADRs + integration tests), or
- Rewritten to an equivalent supported form, or
- Rejected with an actionable translation error and tracked as a gap.

Known syntax/semantics that the provider should prefer (where supported by DecentDB):
- Pagination: `LIMIT` / `OFFSET`
  - Boolean/null semantics: `TRUE` / `FALSE`, `IS NULL`, `IS NOT NULL`
- String concatenation: `||`
  - Avoid ANSI `OFFSET ... FETCH` until/unless DecentDB explicitly supports it; prefer `LIMIT/OFFSET` to match DecentDB’s SQL subset.

v0 translation focus:
- Predicates: equality, inequality, comparisons
- Null semantics: `== null`, `!= null`, nullable columns
- Basic string ops: `Contains`, `StartsWith`, `EndsWith` (translate to `LIKE` with correct escaping)
- Basic math ops
- `IN` for `Contains` over constant lists (parameterization strategy must be deliberate)
  - Define a policy for large lists (to avoid huge SQL/parameter payloads):
    - enforce a max list size with a clear error, or
    - rewrite to a derived table/temporary table approach if/when DecentDB supports it efficiently

Performance notes:
- Prefer parameterization over literal embedding.
- Avoid per-row/per-entity reflection; lean on compiled query pipelines and cached type mappings.

---

## 5. Hook Command Execution Through `DecentDB.AdoNet`

Goal: Reuse DecentDB’s existing `DbConnection`/`DbCommand`/`DbDataReader` plumbing rather than inventing a second execution stack.

Deliverables:
- `DecentDbRelationalConnection : RelationalConnection` that creates/uses `DecentDB.AdoNet` connections
- `DecentDbCommandBuilder` integration (as needed) for parameter creation
- Transaction integration:
  - `RelationalTransaction` wrapping `DbTransaction`
  - correct behavior for nested/ambient transactions (document limitations)
- Error/exception mapping:
  - Map DecentDB error codes/messages into EF Core-friendly exception types where appropriate (`DbUpdateException`, `DbUpdateConcurrencyException`).
  - Define “transient” vs “non-transient” failures for EF execution strategies/interceptors (if DecentDB has no transient class initially, be explicit and conservative).
  - If retries are supported, implement an EF `IExecutionStrategy` with a small, explicit transient set (avoid broad retry-by-default).
  - Document the mapping policy (table or code-first) so provider behavior is predictable for consumers.
- Connection string surface:
  - Prefer passing configuration via DecentDB connection string options (see ADR-0046: `design/adr/0046-dotnet-connection-string-design.md`) rather than introducing provider-only knobs.
- Connection/pooling semantics:
  - Rely on `DecentDB.AdoNet` connection pooling (if enabled) and keep provider state scoped correctly (no static/shared connection state).
  - Ensure `AddDbContextPool` is supported by keeping `DbContext` and connections free of per-request mutable state that survives pooling.
  - Validate pooling behavior with provider tests; if pooling is not safe for some scenarios, document the limitation and disable/avoid it by default in the provider.

Instrumentation (optional but recommended early):
- Capture DB execution time separately from materialization overhead.
- Expose hooks for logging via EF Core diagnostics.

---

## 6. Add `DecentDB.EntityFrameworkCore.NodaTime` Package

Goal: Keep the core provider free of a NodaTime dependency, and offer NodaTime support via an opt-in extension.

Deliverables:
- New project/package: `DecentDB.EntityFrameworkCore.NodaTime`
- Public extension:
  - `UseNodaTime()` on `DecentDbDbContextOptionsBuilder` (or provider options builder)
- Adds:
  - NodaTime type mappings
  - value converters for stable storage aligned with DecentDB conventions

Issue linkage:
- DecentDB issue #19 requests NodaTime support specifically for **DecentDB.MicroOrm**. This plan includes NodaTime support for the **EF Core provider** via an extension package, and it should intentionally reuse the same storage conventions (epoch/ticks/day-number) as MicroOrm.
- Recommended sequencing to close issue #19 quickly: implement NodaTime support in MicroOrm first (requested scope), then mirror the same mappings/converters in `DecentDB.EntityFrameworkCore.NodaTime`.
- Parallelism note: the core EF provider (M0/M1) can proceed without NodaTime; the NodaTime extension can be built in parallel once the base date/time storage conventions are locked.

Initial type targets:
- `Instant`
- `LocalDate`, `LocalDateTime`
- `OffsetDateTime` and/or `ZonedDateTime` only if storage semantics are unambiguous

---

## 7. Testing: Conformance First, Then Provider Tests

Goal: Make correctness regressions hard and keep the provider honest against EF expectations.

Deliverables:
- Conformance tests:
  - Run a subset of EF Core relational provider tests (the “specification tests” pattern)
  - Maintain a skip list with explicit reasons (tracked and minimized)
- Provider tests:
  - DecentDB-specific integration tests (types, transactions, pagination, split query behavior)

Test environment principles:
- Tests must be deterministic and runnable on Windows/macOS/Linux CI.
- Prefer small, targeted integration tests over large end-to-end harnesses.
- Be explicit about the test source:
  - Either vendor a pinned subset of EF Core relational specification tests from the EF Core repository, or
  - Use an equivalent provider test harness if EF’s tests are not consumable as packages.
- Skip list hygiene:
  - Every skipped test should have an issue link and a short reason.

---

## 8. Implement the SaveChanges (Update) Pipeline

Goal: Support EF Core inserts/updates/deletes with correct generated-value handling, batching, and concurrency checks.

Deliverables (typical EF relational provider shape):
- Update SQL generation:
  - `DecentDbUpdateSqlGenerator : UpdateSqlGenerator`
  - `DecentDbModificationCommandBatchFactory` and batching configuration
- Value generation:
  - Identity/auto-increment keys via `INSERT ... RETURNING`
  - Database defaults (e.g., `DEFAULT ...`) via `INSERT ... RETURNING` where DecentDB can return the computed value
  - Client-generated GUIDs where appropriate
- Correct parameter handling and command reuse.
- Transaction behavior consistent with EF Core expectations.

Key behavioral decisions (ADR if non-trivial):
- Maximum batch size defaults
- Whether multiple statements are sent as a single command text or as separate commands
- How to surface provider-specific errors (preserve DecentDB error codes where possible)

---

## 9. Implement EF Core Migrations (Runtime)

Goal: Full EF Core migrations support, including applying migrations and maintaining history.

Deliverables (runtime):
- `IRelationalDatabaseCreator` implementation (file-backed database creation/deletion semantics as appropriate)
- Ensure `EnsureCreated`/`EnsureDeleted` semantics are idempotent and safe under concurrent runs (fail predictably or serialize where required).
- `IHistoryRepository` implementation for `__EFMigrationsHistory`
- `IMigrationsSqlGenerator` implementation emitting DecentDB DDL
- Provider annotations:
  - `IRelationalAnnotationProvider` / annotation conventions needed for migrations to round-trip
Milestone linkage:
- M2 requires both runtime migrations support (this step) and migrations design-time services (Step 10).

DDL support strategy:
- Prefer native DecentDB DDL where supported (see `design/SPEC.md` DDL section).
- For operations DecentDB does not support directly (e.g., `ADD CONSTRAINT` post-create), adopt a documented strategy that is safe under DecentDB’s durability model:
  - Default behavior: fail fast with an actionable error message (what operation, why unsupported, suggested workaround).
  - Optional behavior (post-M2, requires ADR and strong test coverage): table rebuild approach (SQLite-style) for a small, explicitly-supported set of operations where it is safe and predictable.
    - Must be transactional, WAL-safe, and tested for crash recovery and correctness.
    - Requires explicit engine support/locking semantics; do not attempt this “in provider code only” without an ADR and engine-side validation.

Engine prerequisites tracking:
- Migrations will naturally pressure missing DDL operations and schema introspection surfaces.
- Track any required engine work explicitly (issues + ADRs) rather than papering over it in provider code.

---

## 10. Design-Time Tooling and Scaffolding (dotnet-ef)

Goal: Make the provider usable with standard EF Core tooling (`dotnet ef`).

Deliverables (design-time):
- A design-time package (commonly `DecentDB.EntityFrameworkCore.Design`) that:
  - Registers provider design-time services via `IDesignTimeServices`
  - Enables migrations scaffolding and database update tooling
- Scaffolding / reverse engineering (expected “major feature”):
  - Implement `IDatabaseModelFactory` using DecentDB schema discovery
  - Generate DbContext and entity types consistent with provider mappings

Pragmatic sequencing:
- Migrations design-time services first (unblocks M2: `dotnet ef migrations add`).
- Scaffolding later (valuable, but a larger surface and more schema edge cases).

---

## 11. Performance and Benchmarking

Goal: Keep provider overhead visible and avoid performance regressions.

Deliverables:
- Micro-benchmarks:
  - SQL generation/translation throughput for common query shapes
  - parameter binding overhead for typical parameter counts
- Integration benchmarks:
  - compare EF provider query execution vs direct `DecentDB.AdoNet` execution for identical SQL
  - track allocations and wall-clock timings separately (translation vs DB vs materialization)
  - capture end-to-end query latency for representative scenarios (simple predicates, includes/split queries, paging)
  - verify `AddDbContextPool` improves throughput without correctness regressions (connection state reset, transaction state, parameters)

---

## 12. Expand Query Translation Toward “Expected Provider” Coverage

After M0/M1 correctness is established, expand coverage to match typical EF Core provider expectations:
- Grouping/aggregation: `GroupBy`, `Sum`, `Min`, `Max`, `Avg`
- Set ops: `Union`, `Concat`, `Intersect`, `Except` (as supported)
- `Distinct`, `Skip/Take` combinations, correlated subqueries
- Richer string operations with correct escaping and collation expectations
- `DateTime`/`DateTimeOffset` member translations consistent with stored representation

Each addition should be accompanied by:
- Conformance tests (EF spec tests when possible)
- DecentDB-specific integration tests for edge cases

Milestone guidance (tentative):
- Grouping/aggregation: M2+
- Set ops and correlated subqueries: M3

---

## Pragmatic Note (NodaTime vs Provider First)

This is a significantly larger project than adding NodaTime support directly to DecentDB.MicroOrm.

Recommended choice:
- If the immediate need is **NodaTime support in DecentDB apps**, do **MicroOrm first**.
- If the immediate need is **EF Core API compatibility**, do **provider-first** and keep NodaTime as an extension package.
