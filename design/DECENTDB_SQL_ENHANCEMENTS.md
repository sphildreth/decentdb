# DecentDB Advanced SQL Enhancements Backlog

**Document Status:** Active Backlog  
**Created:** 2026-03-25  
**Last Updated:** 2026-03-26  
**Purpose:** Track the remaining advanced SQL backlog beyond the completed S1-S12 slices

---

## Scope

Slices S1-S12 are now treated as code-complete and are covered by engine tests,
user-facing docs, and the changelog. This document no longer attempts to
catalog those completed surfaces. It now focuses only on the former S13
"advanced features" bucket and breaks that backlog into explicit sub-slices.

This file should be read as a forward-looking backlog for optional or
higher-complexity SQL surface area, not as the canonical record of already
shipped SQL features.

---

## Already Landed From Former S13

These items were previously grouped under S13 but already have meaningful
implementation:

- `CREATE SCHEMA` namespace registration with `IF NOT EXISTS` and persistence.
  Current limitation: schema-qualified object names such as `app.users` are not
  yet supported.
- Covering index syntax `CREATE INDEX ... INCLUDE (...)` for BTREE key-column
  indexes, including parser support, catalog persistence, metadata exposure, and
  SQL dump rendering.
  Current limitation: this does not yet imply dedicated index-only scan planning
  or a costed execution strategy that prefers `INCLUDE` payload reads.

Those follow-on gaps are tracked below as active slices instead of being treated
as fully closed backlog items.

---

## Slice Map

| Slice | Category | Priority | Status | Est. Effort |
|-------|----------|----------|--------|-------------|
| S13-A | Schema-Qualified Namespaces | Medium | 🔴 Not Started | Medium |
| S13-B | Explicit Sequence Objects | Medium | 🔴 Not Started | Medium |
| S13-C | User-Defined Functions | Medium | 🔴 Not Started | High |
| S13-D | User-Defined Types | Low | 🔴 Not Started | High |
| S13-E | Materialized Views | Medium | 🔴 Not Started | High |
| S13-F | Deferred Constraint Timing | Low | 🔴 Not Started | Medium |
| S13-G | Exclusion Constraints | Low | 🔴 Not Started | High |
| S13-H | Full-Text Search Surface | Medium | 🔴 Not Started | High |
| S13-I | Covering Index Execution | Low | 🔴 Not Started | Medium |
| S13-J | Access Control (`GRANT` / `REVOKE`) | Low | ⚪ Deferred | High |
| S13-K | Geospatial Surface | Low | ⚪ Deferred | Very High |

**Status Legend:**
- 🔴 Not Started
- 🟡 In Progress
- 🟢 Completed
- ⚪ Deferred

---

## S13-A: Schema-Qualified Namespaces

### Current State

`CREATE SCHEMA` registers namespace names and persists them, but relation names
cannot be qualified in DDL or DML. There is no schema resolver, search path, or
namespace-aware object lookup beyond simple name registration.

### Target Scope

- Support schema-qualified names such as `app.users` in core DDL and DML
- Persist schema ownership of tables, views, indexes, and triggers
- Resolve object lookup deterministically when names are qualified
- Keep unqualified lookup simple and predictable for embedded workloads

### Out Of Scope

- Role-specific search paths
- Access control semantics
- Cross-database federation

### Validation

- Create, query, alter, and drop schema-qualified objects
- Persistence/reopen coverage
- SQL dump and introspection output for qualified names
- Collision handling between similarly named objects in different schemas

---

## S13-B: Explicit Sequence Objects

### Current State

DecentDB supports implicit integer primary-key auto-increment behavior but does
not expose standalone sequence objects or sequence functions.

### Target Scope

- `CREATE SEQUENCE`, `DROP SEQUENCE`, and `ALTER SEQUENCE` subset
- `NEXTVAL`, `CURRVAL`, and `SETVAL`
- Sequence-backed defaults for table columns
- Persistence and transactional semantics that match the chosen compatibility
  model

### Out Of Scope

- Multi-process sequence caching
- Cross-database sequence federation

### Validation

- Monotonic allocation behavior
- Reopen/persistence correctness
- Rollback semantics documented and tested explicitly
- Interaction with column defaults and `INSERT ... RETURNING`

---

## S13-C: User-Defined Functions

### Current State

The engine exposes built-in scalar, aggregate, table, and trigger helper
surfaces, but users cannot define their own SQL-visible functions.

### Target Scope

- A constrained first-class UDF surface
- Clear separation between SQL-defined functions and host-language registered
  functions
- Determinism metadata where optimizer behavior depends on it
- Argument and return type validation

### Out Of Scope

- Unrestricted native code execution inside the engine
- Sandboxed procedural languages in v0

### Validation

- Scalar UDF registration and invocation
- Error propagation and NULL semantics
- Persistence rules for SQL-defined functions
- Dump/restore or migration behavior, depending on chosen design

---

## S13-D: User-Defined Types

### Current State

DecentDB supports a growing built-in type set, but there is no user-defined
type system for custom enums, domains, or composite types.

### Target Scope

- Decide whether the first step is domains, enums, composites, or a limited
  type-alias model
- Catalog persistence for custom type definitions
- Planner/executor awareness of custom types in casts and comparisons

### Out Of Scope

- Arbitrary binary codec plugins in the first slice
- Full PostgreSQL-style extensible type/operator class system

### Validation

- Type creation and catalog reload behavior
- Cast/coercion and comparison semantics
- SQL dump and schema introspection coverage

---

## S13-E: Materialized Views

### Current State

Regular views exist and are expanded at query time, but there is no persisted
materialized-view surface.

### Target Scope

- `CREATE MATERIALIZED VIEW`
- `REFRESH MATERIALIZED VIEW`
- Storage ownership and invalidation semantics
- Predictable interaction with transactions and checkpointing

### Out Of Scope

- Automatic incremental refresh in the first slice
- Cross-session background refresh workers

### Validation

- Create, refresh, and query behavior
- Persistence/reopen correctness
- Dependency tracking for referenced tables/views
- Clear rules around staleness and transaction visibility

---

## S13-F: Deferred Constraint Timing

### Current State

`CHECK`, `UNIQUE`, and foreign-key enforcement is eager. There is no
`DEFERRABLE`, `INITIALLY DEFERRED`, or `SET CONSTRAINTS` support.

### Target Scope

- `DEFERRABLE` / `NOT DEFERRABLE`
- `INITIALLY IMMEDIATE` / `INITIALLY DEFERRED`
- Transaction-scoped `SET CONSTRAINTS`
- Commit-time validation for deferred constraints

### Out Of Scope

- Multi-process coordination
- Arbitrary per-statement custom enforcement hooks

### Validation

- Deferred foreign-key and unique/check scenarios
- Commit-time failure behavior
- Savepoint interaction
- Clear error messages showing which deferred constraint failed

---

## S13-G: Exclusion Constraints

### Current State

There is no row-level exclusion constraint surface similar to PostgreSQL
`EXCLUDE USING ...`.

### Target Scope

- Limited `EXCLUDE` syntax with a deliberately narrow supported operator set
- Index-backed enforcement strategy
- Clear overlap semantics for supported types

### Out Of Scope

- Full PostgreSQL operator-class extensibility in the first slice
- Broad arbitrary-expression support

### Validation

- Conflict detection for overlapping rows
- NULL handling and operator semantics
- Index persistence and rebuild correctness

---

## S13-H: Full-Text Search Surface

### Current State

DecentDB already has trigram indexing and text-pattern acceleration, but it does
not yet expose a first-class full-text search query surface with tokenization,
ranking, or document-query semantics.

### Target Scope

- A user-facing FTS surface beyond trigram-backed `LIKE`/`ILIKE`
- Tokenization and indexing strategy appropriate for embedded/local-first use
- Ranking/snippet primitives if the design requires them

### Out Of Scope

- Full PostgreSQL text-search compatibility in the first slice
- Language-specific stemming for a large locale matrix on day one

### Validation

- Tokenization correctness and persistence
- Search result ordering/ranking expectations
- Update/delete maintenance costs and durability semantics

---

## S13-I: Covering Index Execution

### Current State

`INCLUDE (...)` columns are parsed, persisted, and rendered, but the current
implementation does not define a dedicated index-only execution contract that
uses included payload columns to avoid base-table fetches.

### Target Scope

- Planner recognition of index-only eligible queries
- Executor support for reading projected payload columns from the covering index
- Costing and plan rendering that explain when `INCLUDE` is actually used

### Out Of Scope

- Cost-based optimizer overhaul unrelated to `INCLUDE`
- Overly broad promises about all index-only scan patterns on day one

### Validation

- `EXPLAIN` output showing index-only plans when eligible
- Projection/filter coverage using only key plus included columns
- Correct fallback to base-table lookup when the index cannot satisfy a query

---

## S13-J: Access Control (`GRANT` / `REVOKE`)

### Current State

There is no role/user model. This is a weak fit for the current single-process,
embedded-first architecture.

### Deferred Rationale

- The product is intentionally single-process and embedded
- There is no broader authentication/authorization framework to attach SQL
  grants to
- File-system or host-application controls are the intended security boundary

This slice should remain deferred unless the product model changes materially.

---

## S13-K: Geospatial Surface

### Current State

There are no geospatial data types, functions, indexes, or query semantics.

### Deferred Rationale

- Very large scope compared with current product priorities
- Requires a clear type, function, and index strategy
- Risks pulling the engine toward a different product category

This slice should remain deferred unless there is strong user demand and an ADR
defining a deliberately narrow first milestone.

---

## Prioritization Guidance

If S13 work resumes, the most pragmatic order is:

1. `S13-A` Schema-qualified namespaces
2. `S13-B` Explicit sequence objects
3. `S13-E` Materialized views
4. `S13-H` Full-text search surface
5. `S13-I` Covering index execution

The most likely long-term deferrals are `S13-J` access control and `S13-K`
geospatial support unless the product scope expands beyond its current
embedded-first model.

---

## Definition Of Done For Any S13 Slice

A slice is only complete when all of the following are true:

1. Parser, normalization, planner, and executor behavior are implemented
   coherently for the supported surface
2. Relevant integration and persistence tests cover the main path and edge
   cases
3. User-facing docs and examples are updated
4. Any affected bindings/ABI surfaces are reviewed and updated as needed
5. The slice map status and scope notes in this document remain truthful

---

## Changelog

| Date | Author | Changes |
|------|--------|---------|
| 2026-03-26 | Codex | Refocused document from all SQL slices to active advanced-feature backlog only; split former S13 into explicit sub-slices and preserved already-landed S13 increments |
| 2026-03-25 | Copilot | Initial document creation |
