# Advanced SQL Compatibility Surface

**Document Status:** Backlog / spec seed  
**Future Version:** Later  
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)  
**Purpose:** Capture advanced SQL compatibility work that improves adoption and migration ergonomics without turning DecentDB into a PostgreSQL clone.

## Positioning

DecentDB already has a broad practical SQL surface for an embedded engine:
recursive CTEs, generated columns, savepoints, temp tables, partial and
expression indexes, `RETURNING`, `INSERT ... ON CONFLICT`, rich scalar types,
JSON functions, views, triggers, and broader raw `ALTER TABLE` coverage than
SQLite in several areas.

This is the remaining advanced compatibility backlog. It should help ORMs,
application migrations, PostgreSQL-adjacent SQL, and power users, but it should
not displace the higher-priority product identity work around local-first sync,
branchable data workflows, browser support, Lua extensions, observability, and
storage fundamentals.

## Scope Rules

- Keep the embedded single-process model intact.
- Prefer narrow, durable SQL subsets over broad compatibility claims.
- Require an ADR before changing catalog ownership, file format semantics,
  planner contracts, C ABI behavior, or transaction/constraint timing.
- Keep role/user security, remote authorization, and server-style access control
  out of this track unless the product model changes.
- Treat full-text search and geospatial work as separate roadmap wins, not as
  sub-slices of this track.
- Treat the Lua extension runtime and package model as a separate roadmap win,
  not as a sub-slice of this track.

## Slice Order

| Track Priority | Implementation State | Slice | First Useful Deliverable |
|---:|---|---|---|
| 1 | Not Started | Schema-qualified namespaces | Qualified object names such as `app.users` in DDL and DML |
| 2 | Not Started | Explicit sequence objects | `CREATE SEQUENCE`, `NEXTVAL`, `CURRVAL`, and sequence-backed defaults |
| 3 | Not Started | Materialized views | Manual `CREATE MATERIALIZED VIEW` and `REFRESH MATERIALIZED VIEW` |
| 4 | Not Started | Covering-index execution | Planner/executor support for index-only reads using existing `INCLUDE (...)` metadata |
| 5 | Not Started | SQL-defined functions | Optional `CREATE FUNCTION ... LANGUAGE SQL` helper surface if it remains useful after Lua extensions |
| 6 | Not Started | Deferred constraint timing | `DEFERRABLE`, `INITIALLY DEFERRED`, and commit-time validation |
| 7 | Not Started | User-defined types | Decide and implement the first narrow type surface: domains, enums, composites, or aliases |
| 8 | Not Started | Exclusion constraints | Limited index-backed `EXCLUDE` semantics for selected operator/type pairs |

## Moved Out Of This Track

- SQL and PRAGMA compatibility quick wins are delivered in
  [`WIN_SQL_PRAGMA_COMPATIBILITY_QUICK_WINS_SPEC.md`](_archive/WIN_SQL_PRAGMA_COMPATIBILITY_QUICK_WINS_SPEC.md)
  as the complete compatibility-polish milestone.
- Full-text search with BM25 ranking is tracked in
  [`WIN_FULL_TEXT_SEARCH_BM25_SPEC.md`](_archive/WIN_FULL_TEXT_SEARCH_BM25_SPEC.md) as
  its own roadmap item.
- Native geospatial types and spatial indexes are shipped foundations covered by
  ADR 0124 through ADR 0128 and the user-guide data type/index docs.
- Lua extensions are tracked in [`WIN_LUA_EXTENSION_RUNTIME_SPEC.md`](_archive/WIN_LUA_EXTENSION_RUNTIME_SPEC.md) as the extension runtime and package model.
- Access control, `GRANT`, and `REVOKE` belong with policy-aware embedded SQL only if DecentDB later adds a product-level role or policy model.

## 1. Schema-Qualified Namespaces

### Current State

`CREATE SCHEMA` registers namespace names and persists them. The delivered
quick-win layer supports narrow `main.` and `temp.` qualified local object
names for compatibility. General application schemas such as `app.users` are
still not object owners, and there is no search-path policy or namespace-aware
catalog ownership model beyond simple schema-name registration.

### Target Scope

- Support schema-qualified names such as `app.users` in core DDL and DML.
- Persist schema ownership for tables, views, indexes, and triggers.
- Resolve qualified object lookup deterministically.
- Keep unqualified lookup simple and predictable for embedded workloads.

### Out Of Scope

- Role-specific search paths.
- Access control semantics.
- Cross-database federation.

### Validation

- Create, query, alter, and drop schema-qualified objects.
- Persistence and reopen coverage.
- SQL dump and introspection output for qualified names.
- Collision handling for similarly named objects in different schemas.

## 2. Explicit Sequence Objects

### Current State

DecentDB supports implicit integer primary-key auto-increment behavior but does
not expose standalone sequence objects or sequence functions.

### Target Scope

- `CREATE SEQUENCE`, `DROP SEQUENCE`, and a narrow `ALTER SEQUENCE` subset.
- `NEXTVAL`, `CURRVAL`, and `SETVAL`.
- Sequence-backed column defaults.
- Persistence and transaction semantics that match the chosen compatibility
  model.

### Out Of Scope

- Multi-process sequence caching.
- Cross-database sequence federation.

### Validation

- Monotonic allocation behavior.
- Reopen and persistence correctness.
- Rollback semantics documented and tested explicitly.
- Interaction with column defaults and `INSERT ... RETURNING`.

## 3. Materialized Views

### Current State

Regular views exist and are expanded at query time. There is no persisted
materialized-view surface.

### Target Scope

- `CREATE MATERIALIZED VIEW`.
- `REFRESH MATERIALIZED VIEW`.
- Storage ownership and invalidation semantics.
- Predictable interaction with transactions and checkpointing.

### Out Of Scope

- Automatic incremental refresh in the first slice.
- Cross-session background refresh workers.

### Validation

- Create, refresh, and query behavior.
- Persistence and reopen correctness.
- Dependency tracking for referenced tables and views.
- Clear rules around staleness and transaction visibility.

## 4. Covering-Index Execution

### Current State

`CREATE INDEX ... INCLUDE (...)` columns are parsed, persisted, exposed through
metadata, and rendered in SQL dumps. The engine does not yet define a dedicated
index-only execution contract that uses included payload columns to avoid base
table fetches.

### Target Scope

- Planner recognition of index-only eligible queries.
- Executor support for reading projected payload columns from the covering
  index.
- Costing and plan rendering that explain when `INCLUDE` is actually used.

### Out Of Scope

- A broad cost-based optimizer rewrite unrelated to `INCLUDE`.
- Claims that every possible index-only scan pattern is supported on day one.

### Validation

- `EXPLAIN` output showing index-only plans when eligible.
- Projection and filter coverage using only key plus included columns.
- Correct fallback to base-table lookup when the index cannot satisfy a query.

## 5. SQL-Defined Functions

### Current State

The engine exposes built-in scalar, aggregate, table, and trigger helper
surfaces. Lua extensions are the primary planned path for user-authored
procedural SQL-visible functions. A separate SQL-defined function surface may
still be useful for simple expression wrappers that should be dumpable as SQL
and do not require Lua.

### Target Scope

- Optional `CREATE FUNCTION ... LANGUAGE SQL` support for expression-only
  helpers.
- Clear separation between SQL-defined functions and Lua extension functions.
- Determinism metadata where optimizer behavior depends on it.
- Argument and return-type validation.

### Out Of Scope

- Unrestricted native code execution inside the engine.
- Sandboxed procedural languages in this track. Lua extension work belongs in
  `design/_archive/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`.

### Validation

- SQL-defined scalar function creation and invocation if this slice remains in
  scope.
- Error propagation and NULL semantics.
- Persistence rules for SQL-defined functions.
- Dump/restore or migration behavior, depending on the chosen design.

## 6. Deferred Constraint Timing

### Current State

`CHECK`, `UNIQUE`, and foreign-key enforcement is eager. There is no
`DEFERRABLE`, `INITIALLY DEFERRED`, or `SET CONSTRAINTS` support.

### Target Scope

- `DEFERRABLE` and `NOT DEFERRABLE`.
- `INITIALLY IMMEDIATE` and `INITIALLY DEFERRED`.
- Transaction-scoped `SET CONSTRAINTS`.
- Commit-time validation for deferred constraints.

### Out Of Scope

- Multi-process coordination.
- Arbitrary per-statement custom enforcement hooks.

### Validation

- Deferred foreign-key and unique/check scenarios.
- Commit-time failure behavior.
- Savepoint interaction.
- Clear error messages showing which deferred constraint failed.

## 7. User-Defined Types

### Current State

DecentDB supports a growing built-in type set, but there is no user-defined type
system for custom enums, domains, or composite types.

### Target Scope

- Decide whether the first step is domains, enums, composites, or a limited
  type-alias model.
- Catalog persistence for custom type definitions.
- Planner and executor awareness of custom types in casts and comparisons.

### Out Of Scope

- Arbitrary binary codec plugins in the first slice.
- Full PostgreSQL-style extensible type/operator-class systems.

### Validation

- Type creation and catalog reload behavior.
- Cast/coercion and comparison semantics.
- SQL dump and schema introspection coverage.

## 8. Exclusion Constraints

### Current State

There is no row-level exclusion constraint surface similar to PostgreSQL
`EXCLUDE USING ...`.

### Target Scope

- Limited `EXCLUDE` syntax with a deliberately narrow supported operator set.
- Index-backed enforcement strategy.
- Clear overlap semantics for supported types.

### Out Of Scope

- Full PostgreSQL operator-class extensibility in the first slice.
- Broad arbitrary-expression support.

### Validation

- Conflict detection for overlapping rows.
- NULL handling and operator semantics.
- Index persistence and rebuild correctness.

## Definition Of Done

A slice in this track is complete only when all of the following are true:

1. Parser, normalization, planner, and executor behavior are coherent for the
   supported surface.
2. Persistence and reopen tests cover the catalog/storage state introduced by
   the slice.
3. Relevant integration tests cover the main path and edge cases.
4. SQL dump, schema introspection, and binding impacts are reviewed.
5. User-facing docs and examples are updated when public behavior changes.
