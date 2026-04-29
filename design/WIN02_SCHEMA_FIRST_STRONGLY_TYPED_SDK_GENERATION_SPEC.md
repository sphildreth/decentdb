# DecentDB / Decent Bench SPEC: Schema-First, Strongly-Typed SDK Generation

- **Status:** Proposed
- **Primary product home:** Decent Bench, the official DecentDB workbench/IDE
- **DecentDB responsibility:** Stable schema metadata, query-contract validation
  primitives, ABI/binding guarantees, and any low-level metadata export needed by
  Decent Bench.
- **Audience:** Core DecentDB maintainers, Decent Bench maintainers, SDK/codegen contributors, documentation contributors, coding agents
- **Related roadmap item:** `3. Schema-First, Strongly-Typed SDK Generation`
- **Suggested ADRs:**
  - ADR: Decent Bench owns schema-first SDK generation workflow
  - ADR: Canonical schema metadata model and intermediate representation (IR)
  - ADR: Generated SDK support matrix and maturity policy
  - ADR: Query contract strategy for typed result generation
  - ADR: Documentation and examples strategy for generated SDKs

---

## 1. Executive Summary

This SPEC defines a **schema-first code generation system** for DecentDB that
produces **strongly-typed SDK artifacts** from a DecentDB database schema and
related query contracts.

The preferred product home for the generator UX is **Decent Bench**, the
official DecentDB desktop workbench and IDE. Decent Bench already owns schema
inspection, imports from many external formats, SQL editing, and export
workflows, so it is the natural place for interactive generation, previews,
drift reports, and project-oriented output. DecentDB should remain the
authoritative engine and contract provider.

The goal is to make DecentDB feel less like “an embedded SQL engine you manually wire up” and more like **an embedded application platform with first-class, cross-language developer ergonomics**.

The resulting developer experience should make common tasks predictable and low-friction:

- inspect an existing database schema
- generate language-native models and helpers
- generate typed query/result contracts
- detect schema drift and incompatible changes
- regenerate safely in CI and local workflows
- follow polished, example-rich documentation for each supported language

This SPEC is intentionally written to be **coding-agent ready**. It includes architecture, phased slices, work breakdown, contracts, CLI behavior, documentation requirements, acceptance criteria, and explicit non-goals.

---

## 2. Problem Statement

Today, developers integrating an embedded database often face several recurring pain points:

1. **Manual schema duplication** across application code and database definitions.
2. **Weak typing at boundaries**, especially for SQL query results and parameters.
3. **Ad hoc code generation** that varies by language and lacks consistent guarantees.
4. **Poor change visibility**, where schema changes silently break applications.
5. **Sparse documentation**, which leads to trial-and-error integration and weak onboarding.

For DecentDB, this is a strategic opportunity.

A strong schema-first code generation story can help DecentDB compete on a dimension where many embedded engines still feel hand-built:

- predictable app integration
- multi-language consistency
- safer refactors and migrations
- agent-friendly workflows
- better onboarding and examples

---

## 3. Goals

### 3.1 Primary goals

Build a DecentDB code generation system that:

- Generates **strongly-typed SDK artifacts** from an existing DecentDB schema, with Decent Bench owning the primary user workflow.
- Supports a **canonical intermediate representation (IR)** so language generators are consistent.
- Supports **multiple target languages** through a shared pipeline.
- Provides **typed models**, parameter helpers, and query result contracts.
- Detects and reports **schema drift** and **breaking schema changes**.
- Integrates cleanly into **Decent Bench workflows**, **CLI workflows**, **CI**, and **coding agent workflows**.
- Ships with **high-quality user documentation** and **ample end-to-end examples**.

### 3.2 Secondary goals

- Provide optional repository/query wrapper generation where it adds value.
- Make generated code deterministic and regeneration-safe.
- Produce output suitable for human developers and coding agents.
- Leave room for future support of schema manifests and contract files stored in source control.

---

## 4. Non-Goals

The initial implementation is **not** trying to do all of the following:

- Replace all ORM functionality in every language.
- Infer arbitrary business-domain semantics beyond schema and explicit query contracts.
- Parse and type arbitrary dynamic SQL written in application code without explicit registration.
- Solve distributed schema migration orchestration.
- Guarantee identical idiomatic ergonomics across all languages in the first release.
- Provide a full remote service SDK generator; the scope is SDK generation for DecentDB schema access patterns.
- Generate every language at launch.

---

## 5. Product Thesis

The differentiator is not merely “generate classes from tables.”

The DecentDB win is:

> **Use the database schema as the canonical source of truth, then generate safe, strongly-typed, regeneration-friendly SDK artifacts and documentation across languages.**

This should make the workflow feel like:

- database-first without chaos
- strongly typed without hand-maintained wrappers
- multi-language without fragmented generators
- documented enough that a coding agent can implement or consume it safely

---

## 6. Scope

### 6.1 In scope for initial delivery

- Canonical schema inspection/export pipeline.
- Canonical IR for tables, columns, constraints, indexes, enums/domains if supported, and selected query contracts.
- Decent Bench command(s) to generate SDKs in GUI/headless workflows.
- Initial language support for:
  - **C#/.NET**
  - **TypeScript/Node**
  - **Python**
- Deterministic output layout.
- Model/type generation from tables/views.
- Parameter binding helpers.
- Query contract definition and typed result generation for explicit named queries.
- Schema drift and breaking-change detection.
- Example projects and polished user docs.

### 6.2 In scope for later slices

- Additional targets:
  - Go
  - Java
  - Rust
- Optional repository wrappers.
- Optional validation helpers.
- Optional migration compatibility reports.
- Optional per-language package publishing helpers.

### 6.3 Explicitly out of scope for first implementation

- Implementing the visual Decent Bench generation workflow inside the core DecentDB engine or CLI.
- Runtime ORM with change tracking.
- Automatic arbitrary SQL parsing from code repositories.
- Full LINQ-like query DSL generation.

---

## 7. Users and Use Cases

### 7.1 Primary users

- Application developers embedding DecentDB in desktop, CLI, service, or local-first apps.
- Polyglot teams using more than one language.
- Coding agents asked to scaffold or maintain data access layers.
- Maintainers who need reliable regeneration after schema changes.

### 7.2 Example use cases

#### Use case A: New app bootstrap
A developer has a newly created DecentDB database and wants typed models and query contracts for a C# app.

#### Use case B: Regeneration after migration
A schema changes in a feature branch. The team wants deterministic regeneration and a clear breaking-change report.

#### Use case C: Agent-assisted implementation
A coding agent is asked to build a feature using the generated SDK rather than inventing table mappings by hand.

#### Use case D: Cross-language integration
A TypeScript admin app and a Python utility both need safe schema-aware access based on the same DecentDB file.

#### Use case E: Documentation-led adoption
A user evaluating DecentDB wants polished docs with examples showing how generation works end to end.

---

## 8. Design Principles

1. **Schema is the source of truth.**
2. **Generated output must be deterministic.**
3. **Language generators must share a common IR.**
4. **Regeneration must be safe and obvious.**
5. **Documentation is part of the feature, not an afterthought.**
6. **Agent-readable outputs matter.**
7. **Breaking changes must be explicit.**
8. **Start with strong core support for a few languages before broadening.**

---

## 9. Functional Requirements

### 9.1 Schema inspection

The system shall:

- Inspect a DecentDB database file and extract schema metadata.
- Capture at minimum:
  - schemas/namespaces if supported
  - tables
  - views
  - columns
  - data types
  - nullability
  - default values
  - primary keys
  - foreign keys
  - unique constraints
  - indexes
  - generated columns if supported
- Emit a canonical schema metadata representation.

### 9.2 Intermediate representation (IR)

The system shall define a canonical IR that represents:

- database identity and version metadata
- tables/views
- columns and native types
- key and constraint information
- relation metadata
- supported query contracts
- generator options
- compatibility metadata

The IR must be:

- serializable to JSON
- stable enough for tests and snapshots
- explicit about unsupported/unknown constructs

### 9.3 Decent Bench and CLI generation

The primary command surface should live with Decent Bench. A headless command is
still useful for CI and agents, but it should be exposed as part of the
workbench/tooling layer rather than as a core engine command:

```bash
dbench generate --lang csharp --schema ./app.ddb --out ./Generated
dbench generate --lang typescript --schema ./app.ddb --out ./src/generated
dbench generate --lang python --schema ./app.ddb --out ./client
dbench generate --lang csharp,typescript --schema ./app.ddb --out-root ./generated
dbench generate --lang csharp --schema ./app.ddb --queries ./decentdb-queries.sql --out ./Generated
```

The Decent Bench command shall support:

- one or more languages
- input database path
- output path or output root
- optional query contract input
- overwrite behavior flags
- dry-run mode
- deterministic mode (default on)
- schema drift check mode
- machine-readable output mode (JSON)

DecentDB may expose lower-level schema metadata export or validation helpers if
Decent Bench needs them, but generated SDK layout, language selection, project
templates, drift report presentation, and user-facing generator workflows belong
to Decent Bench.

### 9.4 Generated artifacts

For supported languages, the system shall generate:

- language-native models/types for tables and optionally views
- enum-like representations if applicable
- parameter types or helpers for named queries
- typed result contract types for named queries
- mapping/binding helpers
- metadata/version file for regeneration tracking
- README or usage note within generated output where appropriate

### 9.5 Query contracts

The first version shall support **explicit named query contracts**.

These may be defined in one or more of the following forms, with one chosen as MVP and others deferred:

- annotated SQL file
- DecentDB-specific query manifest file
- schema comments/metadata if supported

Minimum supported behaviors:

- unique query name
- declared input parameters
- inferred or declared result columns
- deterministic result contract generation
- validation against actual schema

Example conceptual input:

```sql
-- @query GetCustomerById
-- @param customer_id UUID
SELECT customer_id, display_name, email
FROM customers
WHERE customer_id = :customer_id;
```

### 9.6 Schema drift detection

The system shall support comparing:

- current schema vs. previous generated metadata
- current schema vs. checked-in IR snapshot
- current query contracts vs. current schema

The system shall detect and report:

- removed tables/views
- removed columns
- incompatible type changes
- nullability tightening
- renamed objects where detectable or probable
- changed query result shapes
- changed parameter contracts

### 9.7 Breaking change classification

The system shall classify findings into at least:

- info
- warning
- breaking

Example:

- adding nullable column: warning or info
- removing column used in generated query: breaking
- changing `INT64` to `TEXT`: breaking
- widening metadata comments only: info

### 9.8 Documentation generation hooks

The system should optionally emit structured metadata that documentation tooling can consume.

At minimum, documentation should be able to reflect:

- generated language support
- type mapping tables
- query contract examples
- regeneration workflow

---

## 10. User Experience Requirements

### 10.1 UX principles

The user experience must be:

- predictable
- explicit
- safe on regeneration
- understandable by non-experts
- useful for coding agents

### 10.2 Expected user journey

#### Basic workflow
1. Create or open a DecentDB database.
2. Run `dbench generate` or use the equivalent Decent Bench UI workflow.
3. Review generated files.
4. Use generated types and query helpers in application code.
5. Re-run generation after schema changes.
6. Review drift/breaking-change output.

#### Query workflow
1. Add or update query contract file.
2. Run generation.
3. Receive typed query input/output artifacts.
4. Use these in application code.

### 10.3 Error messaging

Errors must be actionable.

Bad:
- `Generation failed`

Good:
- `Query GetCustomerById references column customers.email_address which does not exist in schema ./app.ddb.`
- `Type change detected: orders.total changed from DECIMAL(18,2) to TEXT. This is classified as BREAKING for generated SDKs.`

---

## 11. Architecture

### 11.1 High-level architecture

The implementation should be split into the following components:

1. **Schema Inspector**
2. **IR Builder**
3. **Query Contract Parser/Validator**
4. **Compatibility Analyzer**
5. **Language Generator(s)**
6. **CLI Orchestrator**
7. **Docs/Examples Assets**

### 11.2 Recommended project/module layout

Suggested conceptual layout:

```text
/src
  /DecentDB.CodeGen.Core
    SchemaInspection/
    IR/
    QueryContracts/
    Compatibility/
    Generation/
  /DecentDB.CodeGen.Cli
  /DecentDB.CodeGen.CSharp
  /DecentDB.CodeGen.TypeScript
  /DecentDB.CodeGen.Python
/docs
  /schema-first-sdk-generation
/examples
  /codegen-csharp-basic
  /codegen-typescript-basic
  /codegen-python-basic
/design
  /adr
/tests
  /DecentDB.CodeGen.Core.Tests
  /DecentDB.CodeGen.Cli.Tests
  /DecentDB.CodeGen.CSharp.Tests
  /DecentDB.CodeGen.TypeScript.Tests
  /DecentDB.CodeGen.Python.Tests
  /GoldenFiles
```

### 11.3 Canonical pipeline

```text
DecentDB file
  -> schema inspection
  -> canonical IR
  -> query contract merge/validation
  -> compatibility analysis
  -> target language generation
  -> generated output + metadata + docs references
```

---

## 12. Data and Type Mapping

### 12.1 Minimum DecentDB type coverage

The first release must clearly define mappings for supported DecentDB types, including at minimum:

- `NULL`
- `INT64`
- `BOOL`
- `FLOAT64`
- `TEXT`
- `BLOB`
- `TIMESTAMP`
- `UUID`
- `DECIMAL`
- JSON-related types or JSON-typed columns if represented distinctly

### 12.2 Per-language mapping tables

Each supported language must have documented mappings.

Example conceptually:

| DecentDB Type | C# | TypeScript | Python |
|---|---|---|---|
| INT64 | long | bigint or number strategy | int |
| BOOL | bool | boolean | bool |
| FLOAT64 | double | number | float |
| TEXT | string | string | str |
| BLOB | byte[] | Uint8Array | bytes |
| UUID | Guid | string or branded type | UUID/str policy |
| TIMESTAMP | DateTime / DateTimeOffset policy | Date/string policy | datetime |
| DECIMAL | decimal | string/decimal lib policy | Decimal |

The implementation must not leave ambiguous type mappings undocumented.

### 12.3 Nullability handling

The generation system must preserve nullability and emit idiomatic nullable constructs where possible.

### 12.4 Type mapping policy decisions

Type mapping policies that may vary by ecosystem must be explicit and configurable where practical, for example:

- TypeScript `bigint` vs `number`
- TypeScript `Date` vs ISO string handling
- Python `datetime` parsing policy
- decimal handling strategy

---

## 13. Query Contract Model

### 13.1 MVP recommendation

Use a **named SQL file with lightweight annotations** as the MVP because it is easy for humans, agents, diffs, and CI.

Suggested example:

```sql
-- @query ListActiveCustomers
-- @description Returns active customers ordered by display name
SELECT customer_id, display_name, email
FROM customers
WHERE is_active = true
ORDER BY display_name;

-- @query GetInvoiceById
-- @param invoice_id UUID
SELECT invoice_id, customer_id, total_amount, issued_at
FROM invoices
WHERE invoice_id = :invoice_id;
```

### 13.2 Query contract rules

- Query names must be unique.
- Parameter names must be unique per query.
- Parameter types must be declared or inferrable.
- Result columns must be deterministically inferable or explicitly declared if needed.
- Unsupported SQL constructs must return actionable validation errors.

### 13.3 Generated query outputs

For each named query, the generator should produce:

- input parameter contract type/helper
- result row type
- optional execution helper scaffold where feasible
- documentation snippet or generated reference metadata

---

## 14. CLI Specification

### 14.1 Core commands

#### Generate
```bash
dbench generate [options]
```

#### Check / drift detection
```bash
dbench generate --check [options]
```

#### Export IR
```bash
dbench generate --emit-ir ./schema.ir.json [options]
```

### 14.2 CLI options

Minimum options to support:

- `--lang <value>`
- `--schema <path>`
- `--out <path>`
- `--out-root <path>`
- `--queries <path>`
- `--emit-ir <path>`
- `--check`
- `--format json|text`
- `--dry-run`
- `--force`
- `--clean`
- `--namespace <value>` where applicable
- `--package-name <value>` where applicable
- `--config <path>` for future-friendly extensibility

### 14.3 Exit codes

Recommended behavior:

- `0`: success, no blocking issues
- `1`: operational failure
- `2`: check mode found warnings only (optional design choice)
- `3`: breaking changes detected / validation failed

Keep exit code behavior documented and stable.

---

## 15. Generated File Layout

### 15.1 Determinism

Generated file names and directory layout must be deterministic.

### 15.2 Suggested output structure

Example for C#:

```text
Generated/
  Models/
  Queries/
  Metadata/
  README.md
```

Example for multi-language root:

```text
generated/
  csharp/
  typescript/
  python/
```

### 15.3 Safe regeneration

The generator should:

- only write within target output directories
- support clean regeneration mode
- avoid modifying hand-authored files outside generated directories
- include a generated header comment when idiomatic

---

## 16. Observability and Diagnostics

The system shall produce useful diagnostics for:

- schema inspection failures
- unsupported schema constructs
- query validation failures
- generator failures by language
- drift and compatibility findings

Diagnostics should support:

- human-readable text
- machine-readable JSON

This is important for CI and for agent workflows.

---

## 17. Performance Requirements

### 17.1 Targets

For reasonable schema sizes typical of embedded applications, generation should feel fast.

Initial targets:

- Small schema: sub-second to a few seconds on a typical developer machine
- Medium schema: low-single-digit seconds
- Drift check: comparable to or faster than full generation where possible

### 17.2 Constraints

- Avoid loading unrelated large data payloads; generation should operate from metadata.
- Query validation should be bounded and not require scanning large tables.
- Golden file and snapshot tests should verify deterministic output.

---

## 18. Security and Safety Considerations

- Treat schema and query files as untrusted inputs for parsing purposes.
- Avoid unsafe path handling in output generation.
- Prevent writes outside explicit output roots.
- Make sure generated code does not silently embed secrets.
- If config files are introduced, clearly define supported keys and validation.

---

## 19. Testing Strategy

### 19.1 Test categories

The implementation must include at minimum:

- unit tests for schema inspection
- unit tests for IR generation
- unit tests for query contract parsing/validation
- unit tests for compatibility classification
- language-specific generation tests
- CLI integration tests
- golden file snapshot tests
- documentation example validation tests where practical

### 19.2 Golden files

Golden file tests should verify:

- generated file names
- generated contents
- deterministic ordering
- error output snapshots where useful

### 19.3 Documentation validation

At least selected docs examples must be validated automatically where feasible.

Examples:

- command invocations remain current
- generated output fragments referenced in docs match actual output
- sample projects compile or run in CI

---

## 20. Documentation Requirements

This feature is not complete without strong documentation.

### 20.1 Documentation deliverables

The implementation must include detailed and useful user documentation with ample examples.

Required docs deliverables:

1. **Feature overview page**
2. **Quickstart guide**
3. **Concepts page**
4. **CLI reference**
5. **Type mapping reference**
6. **Query contracts guide**
7. **Schema drift and breaking changes guide**
8. **Per-language usage guides**
9. **Troubleshooting guide**
10. **Examples index**

### 20.2 Documentation content requirements

#### Feature overview page
Must explain:

- what schema-first SDK generation is
- why a user would use it
- supported languages
- high-level workflow
- limitations and current scope

#### Quickstart guide
Must include a full end-to-end example:

- create/open sample database
- create sample query contract file
- run generator
- inspect output
- use output in sample app code

#### Concepts page
Must explain:

- schema as source of truth
- IR concept
- query contracts
- regeneration model
- drift detection model
- breaking change classification

#### CLI reference
Must document every supported flag with examples.

#### Type mapping reference
Must show clear type mapping tables for all supported languages.

#### Query contracts guide
Must explain:

- query annotation syntax
- parameter declaration
- result contract generation
- limitations
- examples of valid and invalid queries

#### Schema drift guide
Must explain:

- what is classified as breaking
- what is warning/info
- how to use `--check`
- how to interpret output

#### Per-language guides
Must exist for:

- C#
- TypeScript
- Python

Each guide must include:

- generation command
- project integration example
- sample generated model
- sample query usage
- regeneration workflow
- common pitfalls

#### Troubleshooting guide
Must cover at minimum:

- unsupported schema constructs
- missing output files
- query contract validation failures
- path/configuration mistakes
- regeneration collisions

### 20.3 Example requirements

The docs must include **ample examples**, not just toy fragments.

Minimum example set:

- one shared sample schema used across docs
- one C# sample app
- one TypeScript sample app
- one Python sample app
- one drift/breaking change example
- one query contract example set

Examples should be:

- realistic enough to teach patterns
- small enough to understand quickly
- stable enough for CI validation

### 20.4 Documentation quality bar

Documentation should be:

- accurate
- copy-pasteable
- opinionated where helpful
- explicit about limitations
- usable by both humans and coding agents

---

## 21. Rollout Plan

### 21.1 Release strategy

Use phased delivery rather than attempting all languages and advanced features at once.

### 21.2 Suggested release sequence

- Phase 1: Core IR + C# generation + docs foundation
- Phase 2: TypeScript generation + query contracts
- Phase 3: Python generation + drift detection hardening
- Phase 4: advanced polish + more examples + CI/documentation validation expansion

---

## 22. Sliced Implementation Plan

This work is large enough that it should be delivered in slices.

## Slice 0: Discovery and ADR foundation

### Objective
Make the key design decisions explicit before implementation sprawls.

### Tasks
- [ ] Draft ADR for schema-first generation strategy.
- [ ] Draft ADR for canonical IR design.
- [ ] Draft ADR for initial supported languages and support policy.
- [ ] Draft ADR for query contract format MVP.
- [ ] Draft ADR for breaking-change classification model.
- [ ] Define sample schema used across tests and docs.

### Exit criteria
- ADRs reviewed and accepted.
- Initial scope frozen for Slice 1.

---

## Slice 1: Core schema inspection and IR

### Objective
Create the stable foundation every generator will depend on.

### Tasks
- [ ] Implement schema inspector for DecentDB metadata.
- [ ] Capture tables, views, columns, nullability, defaults, keys, foreign keys, indexes.
- [ ] Define canonical IR classes/models.
- [ ] Add JSON serialization for IR.
- [ ] Add unit tests for schema inspection.
- [ ] Add snapshot/golden tests for IR output.
- [ ] Add CLI support for `--emit-ir`.
- [ ] Document the IR concept at a user-facing level.

### Exit criteria
- IR can be emitted deterministically from sample schemas.
- Tests cover representative schema patterns.

---

## Slice 2: C# generator MVP

### Objective
Ship the first high-quality target language.

### Tasks
- [ ] Define C# naming and file layout policy.
- [ ] Define C# type mapping policy.
- [ ] Generate table/view model types.
- [ ] Generate metadata/version artifact.
- [ ] Add generated header comments.
- [ ] Add golden file tests for C# output.
- [ ] Create C# example project consuming generated output.
- [ ] Write C# user guide with realistic examples.

### Exit criteria
- User can generate and compile a C# sample app from sample schema.
- Docs are published and example-verified.

---

## Slice 3: Query contracts MVP

### Objective
Move beyond table models into typed query contracts.

### Tasks
- [ ] Implement annotated SQL parser for named queries.
- [ ] Validate query names and parameters.
- [ ] Validate referenced schema objects.
- [ ] Infer result column shapes for supported query patterns.
- [ ] Generate typed input/output artifacts for C#.
- [ ] Add validation errors with actionable messaging.
- [ ] Add docs page for query contracts.
- [ ] Add query examples for valid/invalid cases.

### Exit criteria
- At least several supported query patterns work end to end.
- Documentation clearly explains supported and unsupported cases.

---

## Slice 4: TypeScript generator

### Objective
Deliver a strong JS/TS ecosystem story.

### Tasks
- [ ] Define TypeScript output structure.
- [ ] Define TypeScript numeric/date/decimal policy.
- [ ] Generate models/types and query contracts.
- [ ] Add TypeScript golden tests.
- [ ] Create TypeScript sample project.
- [ ] Write TypeScript guide with examples.
- [ ] Add type mapping reference entries.

### Exit criteria
- TypeScript sample project builds and uses generated artifacts.
- Docs cover policy decisions clearly.

---

## Slice 5: Python generator

### Objective
Deliver a strong scripting and tooling story.

### Tasks
- [ ] Define Python package/module output layout.
- [ ] Define Python datetime/decimal/UUID policy.
- [ ] Generate models/types and query contracts.
- [ ] Add Python golden tests.
- [ ] Create Python sample app/tool.
- [ ] Write Python guide with examples.
- [ ] Add type mapping reference entries.

### Exit criteria
- Python sample app runs against sample schema.
- Docs and examples are CI-validated where practical.

---

## Slice 6: Drift detection and compatibility analysis

### Objective
Make regeneration safe and refactors visible.

### Tasks
- [ ] Define comparison model between current schema and previous metadata/IR.
- [ ] Implement change classification logic.
- [ ] Add `--check` mode.
- [ ] Define machine-readable output format.
- [ ] Add tests for info/warning/breaking cases.
- [ ] Document compatibility rules and examples.
- [ ] Add CI example using `--check`.

### Exit criteria
- Breaking changes are reported clearly and consistently.
- Docs show practical regeneration workflows.

---

## Slice 7: Documentation, examples, and agent readiness hardening

### Objective
Make the feature genuinely adoptable.

### Tasks
- [ ] Publish feature overview page.
- [ ] Publish quickstart.
- [ ] Publish concepts page.
- [ ] Publish CLI reference.
- [ ] Publish type mapping reference.
- [ ] Publish query contracts guide.
- [ ] Publish drift guide.
- [ ] Publish troubleshooting guide.
- [ ] Add examples index page.
- [ ] Ensure all examples are copy-pasteable.
- [ ] Add documentation QA checklist.
- [ ] Add CI validation for selected examples and commands.
- [ ] Add “using with coding agents” section describing safe workflows.

### Exit criteria
- Documentation set is complete, discoverable, and accurate.
- A new user can follow quickstart and succeed without tribal knowledge.

---

## 23. Coding Agent Implementation Notes

This section is intentionally direct for coding agents.

### 23.1 Guardrails

- Do not skip the IR layer and generate directly from ad hoc schema reads.
- Do not mix hand-authored and generated files in the same directories.
- Do not broaden supported SQL query patterns silently; document what is unsupported.
- Do not leave type mappings implicit.
- Do not mark the feature complete without the docs deliverables.

### 23.2 Expected contribution pattern

For each slice, agents should produce:

- implementation changes
- tests
- documentation updates
- examples where required
- brief design notes/ADR updates if scope changed

### 23.3 Definition of done for agent-delivered slices

A slice is not done unless:

- implementation exists
- tests pass
- docs are updated
- examples are updated
- edge cases and limitations are called out

---

## 24. Risks and Mitigations

### Risk: IR churn causes generator instability
**Mitigation:** lock IR shape early, version it, and use snapshot tests.

### Risk: Query typing becomes too ambitious too fast
**Mitigation:** start with explicit named query contracts and documented supported patterns.

### Risk: Type mapping differences create confusing output
**Mitigation:** document policy decisions clearly per language and expose limited configuration where needed.

### Risk: Documentation lags implementation
**Mitigation:** make docs part of slice acceptance criteria and CI validation.

### Risk: Too many languages dilute quality
**Mitigation:** ship a strong first wave for C#, TypeScript, and Python before expanding.

### Risk: Generated code becomes hard to trust
**Mitigation:** deterministic output, clear generated headers, sample apps, and golden tests.

---

## 25. Acceptance Criteria

The feature is ready for initial public release when all of the following are true:

- [ ] A user can inspect a DecentDB schema and generate SDK artifacts through the CLI.
- [ ] C# generation is production-quality for supported patterns.
- [ ] TypeScript generation is production-quality for supported patterns.
- [ ] Python generation is production-quality for supported patterns.
- [ ] Query contracts work for the documented supported subset.
- [ ] Drift detection and breaking-change classification work and are documented.
- [ ] Generated output is deterministic and snapshot-tested.
- [ ] At least one sample app exists per supported language.
- [ ] The full required documentation set exists and includes ample examples.
- [ ] Selected examples and docs commands are validated in CI.

---

## 26. Definition of Done

The entire initiative is done for the first major release when:

- core architecture is in place
- supported languages ship with docs and examples
- the CLI is stable and documented
- drift detection is useful and reliable
- coding agents can safely use the feature based on docs and generated outputs
- user-facing documentation is detailed, example-rich, and maintainable

---

## 27. Suggested Follow-on Work After MVP

After the first release, likely next steps include:

- Go generator
- Java generator
- Rust generator
- optional repository wrappers
- config file support for generator customization
- package publishing helpers
- richer query typing support
- docs generator integration from IR metadata

---

## 28. Appendix A: Example End-to-End Workflow

```bash
# Generate C# SDK artifacts
dbench generate \
  --lang csharp \
  --schema ./sample.ddb \
  --queries ./queries.sql \
  --out ./Generated

# Check for drift / breaking changes
dbench generate \
  --lang csharp \
  --schema ./sample.ddb \
  --queries ./queries.sql \
  --out ./Generated \
  --check

# Emit canonical IR snapshot
dbench generate \
  --schema ./sample.ddb \
  --emit-ir ./schema.ir.json
```

---

## 29. Appendix B: Minimum Documentation File Set

Suggested docs file set:

```text
/docs/schema-first-sdk-generation/overview.md
/docs/schema-first-sdk-generation/quickstart.md
/docs/schema-first-sdk-generation/concepts.md
/docs/schema-first-sdk-generation/cli-reference.md
/docs/schema-first-sdk-generation/type-mappings.md
/docs/schema-first-sdk-generation/query-contracts.md
/docs/schema-first-sdk-generation/drift-and-compatibility.md
/docs/schema-first-sdk-generation/csharp-guide.md
/docs/schema-first-sdk-generation/typescript-guide.md
/docs/schema-first-sdk-generation/python-guide.md
/docs/schema-first-sdk-generation/troubleshooting.md
/docs/schema-first-sdk-generation/examples.md
```

---

## 30. Appendix C: Recommended Initial Sample Schema Domains

To avoid boring e-commerce-only examples, use a sample schema that still feels realistic and teaches relationships well.

Recommended sample domain: **field operations / service work orders**.

Suggested entities:

- technicians
- customers
- locations
- work_orders
- work_order_notes
- parts
- invoices
- sync_batches

This domain works well for:

- relational modeling
- query contracts
- offline/local-first narratives
- multiple language samples
- future sync and branch demos
