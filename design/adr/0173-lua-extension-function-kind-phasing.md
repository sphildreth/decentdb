# ADR 0173: Lua Extension Function-Kind Phasing
**Date:** 2026-05-21
**Status:** Accepted

## Context

The Lua extension spec describes scalar functions, table-valued functions,
aggregates, and collations. Those function kinds have very different execution,
planner, memory, persistence, and indexing implications.

The 2.6.0 branch is already large. Lua is being considered as the final
additional feature. The implementation needs a bounded v1 scope that is useful
without forcing every future function kind into the same release.

## Decision

The 2.6.0 Lua extension runtime scope is **package lifecycle plus sandboxed
scalar functions**. Table-valued functions, aggregate functions, and collations
are explicitly deferred.

### 1. Scalar functions are v1

The v1 runtime supports manifest-declared scalar functions with:

- strict typed arguments and return values;
- configured NULL handling;
- deterministic metadata;
- resource limits;
- ordinary expression execution;
- no database handles;
- no writes from Lua.

### 2. Table-valued functions are deferred

Lua table-valued functions require a later ADR before implementation. That ADR
must decide:

- row ownership and streaming vs materialization;
- static schema validation;
- row count and row byte limits;
- interaction with lateral references;
- predicate pushdown policy;
- cancellation between yielded rows;
- error handling after partial row production;
- memory accounting.

The v1 runtime may validate table-function manifest entries only to reject them
as unsupported.

### 3. Aggregate functions are deferred

Lua aggregate functions require a later ADR before implementation. That ADR
must decide:

- aggregate state representation;
- memory limits and spill policy;
- grouping lifecycle;
- NULL handling in step/final;
- deterministic finalization;
- error behavior during partial aggregation;
- planner costing.

### 4. Collations are deferred

Lua-backed collations require a later ADR before implementation. That ADR must
decide:

- whether Lua collations can participate in persistent indexes;
- collation identity in catalog metadata;
- extension dependency tracking for indexes and views;
- rebuild behavior after package upgrade;
- deterministic and locale rules;
- comparison resource limits;
- dump/reopen semantics.

Until that ADR exists, Lua collations are not available, and persisted index
semantics remain limited to native DecentDB collations.

### 5. Persisted schema expressions remain native-only

Even scalar Lua functions are not allowed in generated columns, CHECK
constraints, DEFAULT expressions, expression indexes, partial-index predicates,
or other persisted schema expressions in v1.

## Rationale

Scalar functions deliver immediate extension value while avoiding the hardest
planner/storage contracts. Table-valued functions affect scan ownership,
streaming, row limits, and lateral execution. Aggregates affect memory and group
state. Collations can affect persistent indexes and therefore require a much
stronger dependency and rebuild model.

Deferring those function kinds keeps Lua viable for 2.6.0 without weakening
DecentDB's durability, planner, or indexing contracts.

## Consequences

- The first shipped Lua extension feature is smaller but still useful.
- The package manifest can reserve syntax for future function kinds but v1
  execution rejects unsupported kinds clearly.
- Future table, aggregate, and collation work has explicit ADR gates.
- DecentDB avoids committing to index or planner semantics before they are
  designed.

## Alternatives Considered

1. **Ship every function kind in v1.** Rejected as too much risk for the 2.6.0
   branch.
2. **Ship table-valued functions with materialized rows only.** Rejected because
   it would still need row ownership, memory, cancellation, and lateral policy.
3. **Ship scan/sort-only Lua collations.** Rejected because users would expect
   persistent index behavior unless the contract is much clearer.
4. **Defer Lua entirely until every function kind is designed.** Rejected
   because scalar functions plus package trust are independently useful.

## Validation Requirements

Implementation is not complete until tests cover:

- scalar functions execute successfully;
- table-valued manifest entries are rejected or reported unsupported;
- aggregate manifest entries are rejected or reported unsupported;
- collation manifest entries are rejected or reported unsupported;
- Lua scalar functions are rejected in persisted schema expressions;
- docs clearly identify deferred function kinds.

## References

- `design/FUTURE_WINS.md`
- `design/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`
- `design/adr/0111-table-valued-functions.md`
- `design/adr/0171-lua-extension-sql-type-and-planner-contract.md`
