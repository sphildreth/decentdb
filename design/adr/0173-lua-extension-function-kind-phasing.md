# ADR 0173: Lua Extension Complete Function And Persistence Scope
**Date:** 2026-05-21
**Status:** Accepted

## Context

The Lua extension spec describes scalar functions, table-valued functions,
aggregates, and collations. Those function kinds have very different execution,
planner, memory, persistence, and indexing implications.

The 2.6.0 branch is already large. Lua is the final additional feature. The
product decision for Future Win #2 is that the Lua extension runtime and
package model is complete only when the supported extension surface is
implemented end to end. A scalar-only subset is not complete enough for this
roadmap item.

## Decision

The Lua extension runtime scope for Future Win #2 is the **complete package
lifecycle plus all supported SQL extension function kinds**:

- scalar functions;
- table-valued functions;
- aggregate functions;
- collations;
- extension dependency inspection and rebuild reporting;
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
- Rows are validated against row-count and row-byte limits at the extension
  boundary. The current executor materializes the bounded table result into its
  existing `Dataset` path before downstream planning.
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

- deterministic and locale rules;
- comparison resource limits;
- dump/reopen semantics.

Decision:

- Lua collations may be used for query-time sort and comparison.
- Persistent column collations and persistent index collations remain rejected
  in 2.6.0. DecentDB does not store B+Tree keys whose order depends on
  executable package code.
- Collation comparison must be resource-bounded and return only `-1`, `0`, or
  `1`.

### 5. Persisted Schema Expressions

Deterministic Lua scalar functions are not accepted in persisted schema
expressions in 2.6.0. Runtime expressions may call Lua when the current
connection trusts the extension, but generated columns, CHECK constraints,
DEFAULT expressions, expression indexes, partial-index predicates, and
persistent collation indexes must not depend on Lua code.

This is the complete 2.6.0 persistence boundary for Lua extensions. Persisted
Lua-dependent schema objects would require a separate storage/catalog decision
because they must define exact dependency metadata, reopen behavior, dump and
restore semantics, branch behavior, and index rebuild rules before they can be
made durable.

### 6. Completion Boundary

Future Win #2 is complete only when:

- package validation, install, enable, disable, purge, trust, and inspection are
  implemented;
- scalar functions work in ordinary expression contexts;
- table-valued functions work in `FROM`;
- aggregate functions work with grouped queries;
- Lua-backed collations work for query-time comparison and ordering;
- persistent Lua schema/index uses are rejected explicitly;
- dependency inspection and rebuild-reporting APIs are exposed;
- CLI, Rust API, and C ABI expose the complete lifecycle and invocation model;
- docs and examples cover every supported function kind and trust workflow.

## Rationale

A scalar-only runtime is useful, but it is not the full Lua extension runtime
and package model described by the roadmap. Completing the feature means
shipping every supported function kind, lifecycle, trust, docs, and ABI surface
now.

The conservative part of this decision is the persistence boundary: Lua can run
in query execution, but DecentDB does not persist schema/index behavior that
depends on executable extension code until a storage-specific ADR accepts that
larger contract.

## Consequences

- The Lua extension feature is larger than a scalar-only runtime.
- Planner and executor code must route scalar, table-valued, aggregate, and
  query-time collation invocation through the extension runtime.
- Package upgrades use explicit lifecycle, dependency inspection, and rebuild
  reporting workflows.
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
3. **Allow Lua collations in persisted indexes in 2.6.0.** Rejected because
   index key ordering must not depend on executable package code without a
   broader catalog/index dependency contract.
4. **Allow Lua in persisted schema expressions in 2.6.0.** Rejected for the
   same reason: stored values and constraints must have durable reopen semantics
   independent of connection-local trust.
5. **Allow persisted Lua dependencies without exact package hashes.** Rejected
   because reopen, upgrades, backups, and index correctness need stable
   identity.

## Validation Requirements

Implementation is not complete until tests cover:

- scalar functions execute successfully;
- table-valued functions execute in `FROM`;
- table-valued functions enforce static schemas, row limits, row byte limits,
  type validation, and error behavior;
- aggregate functions enforce state memory limits, NULL handling, step/final
  lifecycle, and error behavior;
- Lua collations execute for query-time sorting/comparison;
- persistent Lua collations and persisted Lua schema expressions are rejected
  explicitly;
- missing, disabled, untrusted, or hash-mismatched extension packages fail
  before Lua execution;
- docs include examples for scalar, table-valued, aggregate, collation, trust,
  and package lifecycle use.

## References

- `design/FUTURE_WINS.md`
- `design/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`
- `design/adr/0111-table-valued-functions.md`
- `design/adr/0171-lua-extension-sql-type-and-planner-contract.md`
