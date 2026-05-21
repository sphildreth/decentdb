# ADR 0173: Lua Extension Complete Function And Persistence Scope
**Date:** 2026-05-21
**Status:** Accepted

## Context

The Lua extension spec describes scalar functions, table-valued functions,
aggregates, and collations. Those function kinds have very different execution,
planner, memory, persistence, and indexing implications.

The 2.6.0 branch is already large. Lua is being considered as the final
additional feature. The product decision for Future Win #2 is that the Lua
extension runtime and package model is complete only when the full extension
surface is implemented end to end. A scalar-only subset is not complete enough
for this roadmap item.

## Decision

The Lua extension runtime scope for Future Win #2 is the **complete package
lifecycle plus all supported SQL extension function kinds**:

- scalar functions;
- table-valued functions;
- aggregate functions;
- collations;
- deterministic persisted schema expressions;
- extension dependency tracking for persisted objects and indexes;
- docs, examples, CLI, C ABI, and binding coverage for the whole surface.

The feature is not done until all of these parts are implemented, tested, and
documented. Implementation may still proceed internally in slices, but the
roadmap item must not be marked complete after only scalar functions or package
lifecycle work.

### 1. Scalar Functions

The runtime supports manifest-declared scalar functions with:

- strict typed arguments and return values;
- configured NULL handling;
- deterministic metadata;
- resource limits;
- ordinary expression execution;
- no database handles;
- no writes from Lua.

### 2. Table-Valued Functions

Lua table-valued functions are in scope. They use manifest-declared static
schemas and integrate with the existing table-valued function executor path.

Required behavior:

- row ownership and streaming vs materialization;
- static schema validation;
- row count and row byte limits;
- interaction with lateral references;
- predicate pushdown policy;
- cancellation between yielded rows;
- error handling after partial row production;
- memory accounting.

Decision:

- Table-valued functions must declare a static output schema in the manifest.
- Dynamic output schemas are rejected.
- Rows are produced through a bounded iterator/yield bridge rather than by
  requiring the full result to materialize before the first row is returned.
- The executor may materialize internally when an existing plan node requires
  it, but the Lua contract is streaming.
- Predicate pushdown into Lua is not required for completion; predicates are
  evaluated by DecentDB after row production unless a safe pushdown contract is
  explicitly added.
- Lateral references are not required for completion unless the native
  table-valued function executor already exposes a stable lateral contract.

### 3. Aggregate Functions

Lua aggregate functions are in scope.

Required behavior:

- aggregate state representation;
- memory limits and spill policy;
- grouping lifecycle;
- NULL handling in step/final;
- deterministic finalization;
- error behavior during partial aggregation;
- planner costing.

Decision:

- Aggregate state lives inside the extension runtime boundary and is accounted
  against an explicit per-aggregate memory budget.
- Aggregate state is not allowed to hold database handles or direct references
  into DecentDB pages, rows, cursors, or transactions.
- If aggregate state exceeds the configured memory budget, the query fails with
  a SQL error. Spill-to-disk is not part of the Lua aggregate contract.
- Step/final errors leave statement and transaction state coherent with ordinary
  statement errors.

### 4. Collations

Lua-backed collations are in scope.

Required behavior:

- whether Lua collations can participate in persistent indexes;
- collation identity in catalog metadata;
- extension dependency tracking for indexes and views;
- rebuild behavior after package upgrade;
- deterministic and locale rules;
- comparison resource limits;
- dump/reopen semantics.

Decision:

- Lua collations may be used for query-time sort and comparison.
- Deterministic Lua collations may participate in persisted indexes only when
  the index records an exact dependency on extension name, package hash,
  collation name, and collation version metadata.
- If the required extension package is missing, disabled, untrusted, or hash
  mismatched, affected indexes are not usable for reads or writes until the
  dependency is restored or the index is rebuilt.
- Collation comparison must be resource-bounded and return only `-1`, `0`, or
  `1`.

### 5. Persisted Schema Expressions

Deterministic Lua scalar functions are in scope for persisted schema
expressions when DecentDB can record and validate the exact extension
dependency.

Allowed persisted use:

- generated columns;
- CHECK constraints;
- expression indexes;
- partial-index predicates;
- view definitions with extension dependency metadata.

DEFAULT expressions may use Lua functions only when they are deterministic and
do not require database handles, external time, randomness, filesystem,
network, process access, or mutable host state.

Persisted schema objects that reference Lua functions must record:

- extension name;
- package hash;
- exported function name;
- function signature;
- function determinism metadata;
- package API version.

If a persisted object dependency is unavailable or mismatched, DecentDB must
fail with a precise SQL error instead of silently using a different function
body. Indexes with missing or mismatched Lua dependencies must be marked
unusable until rebuilt or until the exact dependency is restored.

### 6. Completion Boundary

Future Win #2 is complete only when:

- package validation, install, enable, disable, purge, trust, and inspection are
  implemented;
- scalar functions work in ordinary and persisted expression contexts as
  allowed above;
- table-valued functions work in `FROM`;
- aggregate functions work with grouped queries;
- Lua-backed collations work for query-time comparison and deterministic
  persisted index use;
- dependency metadata, reopen behavior, dump/restore, backups, branches, and
  support diagnostics account for extension dependencies;
- CLI, Rust API, C ABI, and maintained bindings expose the complete lifecycle
  and invocation model;
- docs and examples cover every supported function kind and trust workflow.

## Rationale

A scalar-only runtime is useful, but it is not the full Lua extension runtime
and package model described by the roadmap. Completing the feature means
handling the hard planner, memory, and persistence contracts now rather than
creating a half-feature that needs another roadmap item to become credible.

The conservative part of this decision is not to defer function kinds; it is to
make each function kind explicit and to require exact dependency metadata before
Lua participates in persisted schema or index behavior.

## Consequences

- The Lua extension feature is larger than a scalar-only runtime.
- Planner, executor, catalog, dump/restore, backup, branch, and diagnostics code
  must understand extension dependencies.
- Package upgrades need explicit dependency handling and index/schema rebuild
  workflows.
- Implementation can be sliced internally, but release readiness is judged by
  the complete surface above.
- Tests and docs must cover every function kind, not only scalar functions.

## Alternatives Considered

1. **Ship package lifecycle plus scalar functions only.** Rejected because the
   accepted product scope for Future Win #2 is complete Lua extensions, not an
   initial slice.
2. **Ship table-valued functions with materialized rows only.** Rejected because
   it would be unnecessarily limiting and would still need memory,
   cancellation, and error policy.
3. **Ship scan/sort-only Lua collations.** Rejected because a complete
   extension model must define persisted index behavior and dependency
   tracking.
4. **Forbid Lua in persisted schema expressions.** Rejected because it leaves
   deterministic extension functions unable to participate in generated
   columns, checks, and expression indexes.
5. **Allow persisted Lua dependencies without exact package hashes.** Rejected
   because reopen, upgrades, backups, and index correctness need stable
   identity.

## Validation Requirements

Implementation is not complete until tests cover:

- scalar functions execute successfully;
- table-valued functions execute in `FROM`;
- table-valued functions enforce static schemas, row limits, row byte limits,
  type validation, cancellation, and partial-error behavior;
- aggregate functions enforce state memory limits, NULL handling, step/final
  lifecycle, cancellation, and error behavior;
- Lua collations execute for query-time sorting/comparison;
- deterministic Lua collations can be used by persisted indexes with exact
  dependency metadata;
- persisted schema expressions using deterministic Lua scalar functions reopen
  correctly when dependencies are trusted and available;
- missing, disabled, untrusted, or hash-mismatched extension dependencies fail
  precisely and do not silently use another package;
- expression indexes and collation indexes can be rebuilt after an extension
  package upgrade;
- dump/restore, backup, branch/snapshot, and support diagnostics preserve or
  report extension dependencies coherently;
- docs include examples for scalar, table-valued, aggregate, collation, and
  persisted deterministic use.

## References

- `design/FUTURE_WINS.md`
- `design/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`
- `design/adr/0111-table-valued-functions.md`
- `design/adr/0171-lua-extension-sql-type-and-planner-contract.md`
