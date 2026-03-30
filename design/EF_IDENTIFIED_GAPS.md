# DecentDB .NET / EF Core Identified Gaps
**Date:** 2026-03-30  
**Status:** Active slice plan

This document captures the important gaps identified while auditing the
DecentDB .NET bindings, EF Core provider, and the
`bindings/dotnet/examples/DecentDb.ShowCase/` sample.

The goal is not to relitigate whether DecentDB already supports a broad .NET
surface. It does. The goal here is to identify what is still **not shown**,
**not validated**, or **not fully implemented** in a way that would make the
.NET story feel complete for serious EF Core users.

This is intentionally written as an implementation document rather than a loose
backlog. Each slice is scoped so it can be delivered incrementally, tested
independently, and documented clearly.

---

## Slice Map

Status legend:

- `Completed`: already done and locked for this planning pass
- `Planned`: ready to implement in dependency order
- `Blocked`: intentionally waits on one or more earlier slices
- `Deferred`: important, but intentionally not part of the immediate execution line

| Slice | Title | Status | Primary Area | Depends On | Outcome |
|---|---|---|---|---|---|
| S0 | Audit baseline and scope lock | Completed | docs + provider | none | This document and a stable list of real gaps |
| S1 | Showcase scenario harness refactor | Planned | showcase | S0 | Reusable sample infrastructure so new slices add coverage without turning `Program.cs` into a monolith |
| S2 | Migrations and schema-lifecycle coverage | Planned | EF Core provider + showcase | S1 | The .NET story covers real migrations, not only `EnsureCreated()` |
| S3 | Advanced EF Core modeling coverage | Planned | EF Core provider + showcase | S1 | Owned/complex types, keyless/query types, inheritance, and richer model configuration are proven or explicitly rejected |
| S4 | Query translation and execution completeness | Planned | engine + provider + showcase | S1 | Missing LINQ/query surfaces are either implemented or deliberately marked unsupported with proof |
| S5 | Runtime and operational binding coverage | Planned | ADO.NET + EF Core + showcase | S1 | Transactions, savepoints, maintenance, bulk update, streaming, and operational knobs are demonstrated and validated |
| S6 | Failure-path and correctness matrix | Blocked | engine + provider + showcase tests | S2, S3, S4, S5 | Constraint failures, rollback, concurrency conflicts, and edge-case behavior are verified instead of assumed |
| S7 | Docs, support matrix, and release alignment | Blocked | docs | S2, S3, S4, S5, S6 | Public docs describe the actual supported .NET surface without drift |
| S8 | Nice-to-have provider ergonomics | Deferred | provider | S4, S5 | Lower-priority provider polish after the critical showcase and validation gaps are closed |
| S9 | Performance sanity checks | Deferred | provider + showcase + tests | S4, S5, S6 | Basic evidence that the supported .NET surface is not only correct but operationally reasonable |

---

## Why This Document Exists

The current showcase is already strong in the following areas:

- basic CRUD
- filtering, projection, ordering, grouping, pagination
- string and math translation
- temporal translation
- NodaTime integration
- primitive collections
- transactions and change tracking
- `Include` / `ThenInclude`
- explicit joins and subqueries
- composite keys and composite foreign keys
- provider-specific window functions via `EF.Functions`
- raw SQL query materialization
- schema introspection

That is a strong baseline, but it is still not the same thing as a complete
“.NET + EF Core support story.”

The main audit conclusion is:

> The current gaps are now less about “basic support is broken” and more about
> “advanced, operational, and failure-path coverage is missing or incomplete.”

Those remaining gaps matter because advanced EF Core consumers do not judge a
provider by the happy path alone. They judge it by whether real migrations
work, whether complex modeling patterns are supported, whether operational
maintenance flows exist, and whether failure paths behave predictably.

This gap line is also informed by prior user-reported .NET / EF Core pain
points, not only by internal audit. The intent is to close the highest-value
real-world seams first instead of optimizing for theoretical provider parity.

---

## Scope

### In scope

- gaps in the showcase sample itself
- gaps in the EF Core provider surface
- gaps in the ADO.NET binding surface as exposed to .NET users
- missing validation for supported behavior
- doc drift caused by the above gaps

### Out of scope

- unrelated engine work that does not materially improve the .NET story
- speculative provider parity with every database/EF provider on earth
- large new dependencies
- design churn without executable tests and sample coverage

---

## Target Baseline

This document assumes the current repository baseline:

- .NET target: `net10.0`
- EF Core package line: `Microsoft.EntityFrameworkCore.*` `10.0.3`

That matters because modeling and translation behavior can differ across EF Core
major versions. Unless a slice explicitly broadens or narrows compatibility, the
default implementation target for this plan is the EF Core 10 line currently
used by the DecentDB provider in-repo.

If the provider later adds multi-targeted EF Core support, this document should
be updated so slice decisions remain version-anchored instead of drifting into
generic EF advice.

---

## S0. Audit Baseline and Scope Lock

**Status:** Completed

This slice is the current change set.

### Deliverables

- `design/EF_IDENTIFIED_GAPS.md`
- the audited gap list below
- a dependency-ordered implementation line instead of an unordered backlog

### Locked findings

The most meaningful gaps identified are:

1. **Migrations are not showcased as a first-class workflow**
   - The sample proves runtime schema creation and query behavior.
   - It does not prove that a realistic `dotnet ef migrations` workflow is
     stable across create, alter, upgrade, and rerun scenarios.

2. **Advanced EF Core modeling patterns are not demonstrated**
   - Owned/complex types are not exercised.
   - Keyless query types are not exercised.
   - Inheritance/discriminator mapping is not exercised.
   - Skip-navigation many-to-many is not exercised.
   - Shadow properties and richer value-generation patterns are not exercised.

3. **Query coverage is broad but not complete**
   - The showcase covers many LINQ shapes, but not the deeper edges that matter
     for provider trust:
     - server-side set operations
     - broader bulk update patterns
     - more of the provider-specific window-function API surface
     - additional raw SQL execution patterns

4. **Operational/runtime scenarios are underrepresented**
   - Savepoints are not demonstrated.
   - Explicit isolation-level choice is not demonstrated.
   - Database maintenance operations such as checkpoint or save-as are not
     shown in the sample.
   - Async streaming-oriented usage is not shown.

5. **Failure-path coverage is too shallow**
   - There is little direct proof of:
     - uniqueness failure behavior
     - FK violation behavior
     - rollback semantics under failure
     - concurrency conflict handling and recovery
     - negative-path error messages and provider exceptions

### Exit criteria

- this document names the gaps concretely
- slices are ordered by implementation value and dependency
- future coding work can proceed without re-auditing the whole sample first

---

## S1. Showcase Scenario Harness Refactor

**Status:** Planned

### Why this slice exists

`bindings/dotnet/examples/DecentDb.ShowCase/Program.cs` already demonstrates a
large amount of functionality. Adding many more scenarios directly into the same
top-level file will quickly turn the showcase into a hard-to-maintain pile of
seed logic, ad hoc assertions, and repeated setup/teardown.

Before adding more advanced scenarios, we need to make the sample easier to
extend safely.

### Files to change

- `bindings/dotnet/examples/DecentDb.ShowCase/Program.cs`
- `bindings/dotnet/examples/DecentDb.ShowCase/ShowcaseDbContext.cs`
- new helper files under `bindings/dotnet/examples/DecentDb.ShowCase/`
  as needed, for example:
  - `ShowcaseScenarioRunner.cs`
  - `ShowcaseSeeder.cs`
  - `ShowcaseAssertions.cs`
  - `Scenarios/` subfolder if that fits the local style cleanly

### Required implementation

1. Extract repeated seeding and logging helpers out of `Program.cs`.
2. Separate “prepare data” from “run scenario” from “print outcome”.
3. Add a lightweight convention for showcase sections so every new slice can add
   a scenario without duplicating the same boilerplate.
4. Keep the sample runnable as one console program. Do not convert it into a
   unit-test project.
5. Keep output readable and deterministic. The showcase is both a demo and a
   smoke-check, so output stability matters.
6. Preserve the current scenarios while refactoring. This slice is about
   making future work safer, not changing the supported behavior surface.
7. Keep complex assertions and correctness-heavy validation in the real test
   suites under `bindings/dotnet/tests/`; the showcase should remain focused on
   educational happy-path execution and concise smoke output.

### Proposed scenario convention

Use a deliberately small convention rather than an open-ended abstraction
framework.

Static ordering is sufficient for the current showcase. The point of this
convention is maintainability, not dynamic discovery.

Recommended shape:

1. A small immutable descriptor, for example:
   - `ShowcaseScenario(string Title, Func<ShowcaseScenarioContext, Task> RunAsync)`
2. A narrow execution context that exposes only the helpers the sample actually
   needs, such as:
   - `DbPath`
   - `TextWriter Output`
   - helper factory methods like `CreateContext()` or `CreateOpenConnection()`
3. A single ordered `IReadOnlyList<ShowcaseScenario>` in `Program.cs` or a
   nearby scenario-registration file.
4. Shared seed/reset helpers live in explicit helper classes, not in the
   descriptor itself.

This keeps the shape concrete:

- one title
- one execution function
- one narrow context

That is enough to stop the file from sprawling without inventing a plugin
system, dependency injection graph, or test-runner clone.

If this still feels too heavy during implementation, plain method extraction is
an acceptable fallback as long as the resulting code preserves:

- one obvious ordered execution path
- one shared context/helper surface
- minimal repeated setup/teardown code

The descriptor shape is recommended, not mandatory dogma.

### Do not do

- do not rewrite the showcase into a mini-framework
- do not add unnecessary abstractions that hide what each scenario is doing
- do not move logic into test-only projects when the sample itself should
  remain self-explanatory

### Validation

- `dotnet build bindings/dotnet/examples/DecentDb.ShowCase/DecentDb.ShowCase.csproj --nologo`
- `dotnet run --project bindings/dotnet/examples/DecentDb.ShowCase/DecentDb.ShowCase.csproj --no-build`
- verify that the existing section output is still present and meaningful

### Exit criteria

- new showcase slices can add scenarios with minimal copy/paste
- `Program.cs` is smaller or at least materially easier to reason about
- current showcase output still runs end-to-end successfully

---

## S2. Migrations and Schema-Lifecycle Coverage

**Status:** Planned

### Why this slice exists

Supporting EF Core in practice means more than supporting `EnsureCreated()`.
Teams expect model changes to flow through migrations, SQL generation, and
repeatable database upgrades. The provider has made meaningful progress here,
but the showcase does not yet prove the full workflow.

### Files to change

- `bindings/dotnet/examples/DecentDb.ShowCase/`
  - add a small migration-backed sample path or companion example flow
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/`
  - expand migration runtime and SQL generation coverage
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Migrations/Internal/`
  - only if missing SQL generation support is found

### Required implementation

0. Audit DecentDB engine DDL capabilities before writing new migration scenarios:
   - identify the actual `ALTER TABLE` / constraint / rename subset the engine supports today
   - map common EF migration operations onto that subset
   - identify which gaps are provider-only versus engine-level
   - decide whether any uncovered migration shape would require an ADR before implementation
1. Add a showcase section or companion example path that demonstrates:
   - applying an initial migration
   - applying a follow-up migration
   - verifying the schema after upgrade
2. Cover common schema changes:
   - add column
   - rename object if supported
   - add/drop index
   - add/drop constraint
   - alter nullability/default behavior if supported
3. Verify migration-generated SQL against DecentDB semantics rather than only
   checking for string output.
4. Ensure self-referencing FKs and composite FKs stay covered inside migration
   workflows, not just create-table paths.
5. If some migration patterns are still unsupported, classify them precisely and
   add explicit tests that fail for the right reason. Do not leave them vague.

### Decision gate: native DDL vs provider rebuild strategy

Before broadening the migration story, decide explicitly how DecentDB should
handle migration operations that EF users expect but the engine may not support
natively.

The two acceptable strategies are:

1. **Native engine DDL parity**
   - implement the required engine/provider support directly
2. **Provider-managed table rebuild workflow**
   - for eligible shapes, use the classic migration workaround:
     create replacement table -> copy data -> drop old table -> rename new table

This decision must be made deliberately per migration category. Do not slide
into accidental partial rebuild behavior without documenting the contract.

### Implementation notes

- Prefer runtime migration tests over snapshot-only confidence.
- Reuse the provider’s existing migration SQL generator rather than building
  ad hoc SQL in the sample.
- If a migration shape requires engine support, fix the engine/provider root
  cause rather than documenting around it.
- Do not assume EF migration expectations are automatically valid for DecentDB;
  prove the engine DDL subset first and build the supported workflow around that proof.
- If provider-managed rebuilds are introduced, test them as product behavior, not
  as hidden implementation details.

### Validation

- targeted `dotnet test` for migration SQL generation and runtime migration paths
- showcase execution for the migration-backed sample path, if added

### Exit criteria

- DecentDB can present a credible EF Core migration story
- supported migration shapes are proven with runtime coverage
- unsupported shapes, if any remain, are narrowly defined and justified

---

## S3. Advanced EF Core Modeling Coverage

**Status:** Planned

### Why this slice exists

The current sample proves relational basics well, but advanced EF Core users
often rely on richer mapping patterns. If DecentDB supports them, we should
show them. If it does not, we should know exactly where and why.

### Files to change

- `bindings/dotnet/examples/DecentDb.ShowCase/Entities/`
- `bindings/dotnet/examples/DecentDb.ShowCase/ShowcaseDbContext.cs`
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/`

### Sub-scope

This slice should evaluate and, where practical, implement or demonstrate:

1. **Owned or complex types**
   - Example: `CustomerProfile` or `AddressValueObject` owned by `Customer`
   - Decide whether to use classic owned entities, EF Core complex properties,
     or both depending on framework support in this repository

2. **Keyless/query types**
   - Example: map a read-only projection over a SQL query or view-like shape
   - Demonstrate raw SQL materialization into a keyless type

3. **Inheritance**
   - Prefer a narrow TPH example first
   - Do not attempt TPT/TPC unless TPH is working and the extra complexity is
     justified
   - Validate discriminator-column indexing and query translation behavior before
     promoting inheritance as a showcase-supported pattern

4. **Skip-navigation many-to-many**
   - The showcase currently demonstrates explicit join entities
   - Add a true skip-navigation scenario if the provider supports it cleanly

5. **Shadow properties / model-only properties**
   - Validate whether provider metadata and update paths handle them correctly

6. **Concurrency tokens**
   - Validate explicit concurrency-token patterns such as:
     - `[ConcurrencyCheck]`
     - row-version style properties where the provider supports them
     - model-configured concurrency tokens, including shadow-property cases if practical

7. **Value generation / computed defaults**
   - Verify store-generated or provider-generated patterns where relevant

### Required implementation

1. Choose one realistic entity cluster per modeling feature instead of synthetic
   one-off examples.
2. Add showcase sections only for the patterns that are actually supported and
   stable enough to teach.
3. Add provider tests for each modeled feature before documenting it as working.
4. If a feature is not supported, document the precise failing seam:
   - model building
   - SQL generation
   - runtime materialization
   - change tracking
   - update pipeline

### Do not do

- do not add every advanced EF feature in a single giant entity model
- do not hide unsupported behavior behind client-side workarounds and call it
  support

### Validation

- targeted provider tests per modeling feature
- showcase run for every modeling scenario exposed publicly

### Exit criteria

- advanced EF patterns are either proven and demonstrated or precisely rejected
- the sample covers at least one supported representative for each important
  modeling category that DecentDB wants to claim

---

## S4. Query Translation and Execution Completeness

**Status:** Planned

### Why this slice exists

The provider now covers a strong set of LINQ translations, but users will still
hit confidence gaps if the sample leaves important query shapes unexplored.

This slice is not about adding random LINQ operators for completeness theater.
It is about closing the gap between “the provider works for common demos” and
“the provider is trustworthy under broader EF Core query pressure.”

### Files to change

- `bindings/dotnet/examples/DecentDb.ShowCase/Program.cs`
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/`
- provider query translation code under
  `bindings/dotnet/src/DecentDB.EntityFrameworkCore/Query/Internal/`
- engine code in `crates/decentdb/src/` only if new SQL support is genuinely required

### Sub-scope

1. **Server-side set operations**
   - Determine whether `Union`, `Concat`, `Intersect`, and `Except` can be
     translated and executed server-side for representative shapes
   - If not, identify whether the gap is provider translation, SQL generation,
     or engine execution

2. **Full provider-specific window function surface**
   - Showcase and test:
     - `PercentRank`
     - `FirstValue`
     - `LastValue`
     - `NthValue`
   - Preserve the already-working ranking and offset examples

3. **Bulk updates**
   - Add `ExecuteUpdateAsync` coverage if supported
   - Add `ExecuteDeleteAsync` coverage alongside it so bulk mutation coverage is symmetrical
   - If only one path can be landed early, prioritize `ExecuteDeleteAsync` because embedded cleanup/pruning workloads benefit from it immediately
   - If either path is unsupported, identify whether the gap is translation, update-pipeline handling, or engine DML capability

4. **Additional raw SQL paths**
   - demonstrate parameterized non-query execution, not just query materialization
   - include at least one mutation path (`INSERT`, `UPDATE`, or `DELETE`)

5. **Streaming-friendly query usage**
   - determine whether `AsAsyncEnumerable()` and related patterns behave well
   - if supported, add a narrow sample and regression coverage

### Required implementation

1. Start from provider tests, not showcase code.
2. Only promote a feature into the showcase once tests prove it.
3. Use representative query shapes with realistic data, not trivial one-row cases.
4. If provider-only work is insufficient, fix the engine root cause instead of
   masking it at the sample layer.

### Validation

- targeted provider translation tests
- targeted engine tests if new SQL execution support is added
- showcase execution for any newly promoted query scenarios

### Exit criteria

- the remaining meaningful query gaps are either implemented or reduced to a
  short, specific list with proof
- the showcase demonstrates the provider’s strongest supported query surfaces

---

## S5. Runtime and Operational Binding Coverage

**Status:** Planned

### Why this slice exists

An EF Core provider is not only judged by query translation. Operational
behavior matters too: transaction semantics, maintenance, maintenance-related
APIs, batching, and interaction patterns that real applications depend on.

### Files to change

- `bindings/dotnet/examples/DecentDb.ShowCase/Program.cs`
- `bindings/dotnet/src/DecentDB.AdoNet/`
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore/`
- `bindings/dotnet/tests/`

### Sub-scope

This slice should be executed in two explicit tracks so operational work stays
clear instead of mixing unrelated concerns.

#### Track A: ADO.NET operational behaviors

1. **Savepoints**
   - expose and demonstrate savepoint usage if the underlying ADO.NET provider
     supports it
   - validate rollback-to-savepoint behavior

2. **Explicit isolation levels**
   - confirm supported isolation levels
   - demonstrate one or more explicit selections instead of relying only on defaults

3. **Maintenance APIs**
   - demonstrate checkpoint and save-as behavior from .NET if exposed
   - verify behavior after reopen, not just immediate success return values

4. **Connection and command configuration**
   - document timeout behavior and any important command-level knobs exposed publicly
   - treat connection pooling as not a primary goal for the embedded single-process model unless shared native-resource behavior proves a real need; if it remains irrelevant, document that plainly rather than leaving it ambiguous
   - validate that async ADO.NET paths do not create surprising thread-pool blocking behavior beyond what is inherent to the current embedded/native execution model

#### Track B: EF Core operational behaviors

5. **Bulk mutation operational paths**
   - validate batching, rowcount semantics, and statement reuse where relevant
   - cover both `ExecuteUpdateAsync` and `ExecuteDeleteAsync` if supported

6. **SaveChanges/update-pipeline operational behavior**
   - validate the provider’s batching and write-path behavior under realistic multi-row operations
   - confirm whether any important operational knobs should be documented for EF users specifically

### Required implementation

1. Inventory the ADO.NET binding surface first.
2. Promote only the operational capabilities that are stable and useful to
   application developers.
3. Keep operational examples short and concrete; they should teach behavior, not
   reproduce internal test logic.
4. For any API exposed publicly, add at least one validation path proving it
   works under reopen or error conditions where relevant.

### Validation

- targeted ADO.NET tests
- targeted EF Core tests when the feature crosses the provider boundary
- showcase run for promoted operational sections

### Exit criteria

- users can see how to perform the important operational tasks from .NET
- operational APIs that exist publicly are exercised, not merely declared

---

## S6. Failure-Path and Correctness Matrix

**Status:** Blocked

**Depends on:** S2, S3, S4, S5

### Why this slice exists

Happy-path demos create false confidence if they are not matched by proof that
constraints, errors, and rollback semantics behave correctly.

This slice turns “it seems to work” into “we know how it fails, and the failure
behavior is acceptable.”

### Exception contract baseline

The following contract should be treated as the starting point for S6 until a
more formal API document supersedes it:

| Error category | Expected .NET surface |
|---|---|
| Native engine failure surfaced directly through the native/ADO.NET layer | `DecentDB.Native.DecentDBException` |
| Native/ADO.NET read or query execution failure | `DecentDB.Native.DecentDBException` |
| EF Core write/update pipeline failure caused by database execution | `DbUpdateException` with inner `DecentDB.Native.DecentDBException` |
| EF Core optimistic concurrency rowcount mismatch | `DbUpdateConcurrencyException` |
| Unsupported or non-translatable LINQ shape rejected by EF Core/provider translation | `InvalidOperationException` |

S6 should either confirm this table with tests or deliberately replace it with a
better-documented contract. What S6 must not do is leave exception expectations
implicit.

### ABI prerequisite

Before implementing fine-grained exception mapping, audit the C ABI and current
native binding surface first.

At the time of writing, `include/decentdb.h` exposes coarse top-level error
categories such as:

- `DDB_ERR_IO`
- `DDB_ERR_CORRUPTION`
- `DDB_ERR_CONSTRAINT`
- `DDB_ERR_TRANSACTION`
- `DDB_ERR_SQL`
- `DDB_ERR_INTERNAL`
- `DDB_ERR_PANIC`
- `DDB_ERR_UNSUPPORTED_FORMAT_VERSION`

That may be sufficient for broad exception categorization, but it is not
automatically sufficient for precise relational mapping such as distinguishing
unique violations from foreign-key violations without additional structured
detail.

S6 must therefore:

1. verify whether current native error codes plus message/context are enough
2. avoid brittle string-matching as the long-term contract
3. explicitly call out any need for richer C ABI/native error detail before
   claiming robust provider-side exception mapping

### Files to change

- `include/decentdb.h` if richer structured error reporting is required
- `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/`
- `bindings/dotnet/tests/DecentDB.AdoNet.Tests/` if needed
- `docs/api/error-codes.md` if the public error contract changes
- showcase sample only for concise, user-facing negative examples where it helps

### Required implementation

1. Add explicit validation for:
   - unique index/constraint violations
   - foreign key violations
   - check constraint violations if supported
   - deferred-constraint timing behavior if it ever becomes supported; otherwise assert/document that deferred constraint timing is currently outside the supported DecentDB contract
   - rollback after failed writes
   - rollback to savepoint if S5 adds it
   - concurrency conflict behavior and retry guidance
2. Verify exception mapping:
   - provider exception type
   - engine/native error code preservation
   - actionable error message text
   - inner-exception shape where EF wraps native failures
3. Add boundary-value coverage where useful:
   - large numeric values
   - null-heavy aggregate shapes
   - temporal boundary cases
4. Avoid broad “catch any exception” tests. Assert the specific contract.

### Implementation notes

- this slice should mostly be tests, not new product features
- if negative tests expose product bugs, fix them in the appropriate earlier slice

### Validation

- targeted failure-mode provider tests
- targeted engine tests for any newly found root-cause bugs

### Exit criteria

- the .NET stack has an explicit negative-path confidence story
- public docs can refer to supported behavior with less hand-waving

---

## S7. Docs, Support Matrix, and Release Alignment

**Status:** Blocked

**Depends on:** S2, S3, S4, S5, S6

### Why this slice exists

DecentDB has already suffered from stale .NET limitation docs. We should not
repeat that pattern by implementing slices without updating the public story.

### Files to change

- `bindings/dotnet/examples/README.md`
- `docs/api/dotnet.md`
- `docs/api/bindings-matrix.md`
- `docs/about/changelog.md`
- any newly introduced migration or operational docs under `docs/api/` if a slice exposes a new public workflow
- release notes or workflow docs if provider validation changes

### Required implementation

1. Refresh the support matrix to reflect the results of S2-S6.
2. Remove stale “unsupported” claims only when runtime/test evidence exists.
3. Add new limitations only when they are:
   - current
   - narrow
   - reproducible
   - described with an honest workaround
4. Update examples and snippets so they match the showcase as shipped.
5. Add changelog entries when supported .NET surface expands materially.

### Exit criteria

- public docs match real provider behavior
- the sample README becomes a trustworthy support summary instead of a drifting note

---

## S8. Nice-to-Have Provider Ergonomics

**Status:** Deferred

### Why this slice exists

Some improvements would make the provider feel more polished, but they are not
the highest-value gaps compared with migrations, advanced modeling, and
failure-path proof.

Low-risk ergonomics may still be folded into earlier slices when they are direct,
adjacent improvements to the code already being changed. The point of deferring
S8 is to avoid spending a whole slice on polish before the core support story is
locked down.

### Candidate items

- deeper provider ergonomics around convenience APIs
- more aggressive query-shape parity
- additional sample polish and output formatting
- extra performance-oriented examples once correctness coverage is stronger

### Rule

Do not start this slice until S2-S7 are in a materially better place.

---

## S9. Performance Sanity Checks

**Status:** Deferred

### Why this slice exists

Correctness is the priority for this plan, but a credible EF Core story also
benefits from lightweight evidence that common usage patterns are not obviously
pathological.

This is not a benchmark-program rewrite and it is not a demand for full ORM
performance parity analysis. It is a future slice for basic confidence checks.

The current `DecentDb.ShowCase` sample already acts as a **proto-S9 baseline**
because it includes a `PERFORMANCE PATTERNS` section and some timing-oriented
demonstrations. So S9 is not “start performance work from zero.”

S9 is specifically about turning that existing loose smoke coverage into a more
intentional sanity-check slice with:

- clearer goals
- representative scenarios
- repeatable interpretation
- explicit guidance about what the sample is and is not claiming

### Candidate scope

- N+1-sensitive showcase/query patterns and whether the sample teaches the right defaults
- change-tracking overhead sanity checks for representative write/read flows
- bulk insert and bulk delete sanity checks for realistic embedded workloads
- statement reuse or batching effects where those materially affect .NET behavior
- lightweight validation of async query/update paths so `.ToListAsync()` and
  related APIs are not making obviously misleading performance claims for the
  embedded execution model

### Rule

Do not let this slice delay correctness, migration support, or failure-path
coverage. It becomes valuable after S4-S6 have reduced the risk of measuring the
wrong thing.

---

## Recommended Execution Order

1. **S1 first**
   - Without this, the showcase will become harder to extend safely.

2. **S2 and S4 next**
   - These are the highest-confidence gaps visible to real EF Core users:
     migrations and broader query support.

3. **S3 and S5 after that**
   - Advanced modeling and operational runtime coverage deepen the provider story.

4. **S6 then S7**
   - Once new support exists, lock in negative-path confidence and refresh docs.

---

## Definition of Done For This Gap Line

This identified-gap effort is complete only when:

- the showcase demonstrates the important supported EF Core scenarios DecentDB
  wants to claim publicly
- provider and engine tests prove those scenarios
- negative-path behavior is verified for the key relational failure modes
- the README and changelog match reality
- the remaining unsupported list, if any, is short, specific, and justified

Until then, the .NET story is improved, but not yet fully rounded out.
