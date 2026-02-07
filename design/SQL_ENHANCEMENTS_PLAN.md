# DecentDB SQL Enhancements Plan (Pre-1.0)
**Date:** 2026-02-06  
**Status:** Implemented in 0.x baseline (except explicitly deferred extension ecosystem)

## 1. Purpose
DecentDB’s current SQL surface is intentionally scoped. This document defines the **planned** SQL and operational enhancements required to remove “not in current baseline” items from user-facing documentation, while preserving DecentDB’s priorities:

- **Priority #1:** durable ACID writes
- **Priority #2:** fast reads
- **Concurrency model:** single process, **one writer**, many concurrent reader threads
- **Definition of Done:** tests from day one (unit + property + crash-injection + differential)

This is a roadmap/plan document only. It does **not** change behavior by itself.

## 1.1 Implementation status snapshot (2026-02-07)
- ✅ 5.1 Richer expression language and built-ins
- ✅ 5.2 UPSERT and DML conveniences
- ✅ 5.3 CTEs (non-recursive)
- ✅ 5.4 Set operations (`UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`)
- ✅ 5.5 Advanced constraints (CHECK + FK `CASCADE`/`SET NULL`)
- ✅ 5.6 Advanced index options (multi-column + partial v0 subset + expression v0 subset)
- ✅ 5.7 Broader ALTER TABLE (`ADD COLUMN`, `RENAME COLUMN`, `DROP COLUMN`, `ALTER COLUMN TYPE`)
- ✅ 5.8 Triggers (AFTER-row + INSTEAD OF-row subsets)
- ✅ 5.9 Window functions (v0 `ROW_NUMBER()` subset)
- ✅ 5.10 Operational capabilities:
  - introspection via existing CLI schema/info commands
  - `EXPLAIN` implemented
  - loadable extension ecosystem remains intentionally deferred (see issue link in §5.10)

## 2. Non-negotiables and gating rules
1. **No persistent format or recovery changes without an ADR**.
   - Includes: catalog formats, page layouts, WAL frame formats, checkpoint/truncation rules.
2. **SQL dialect semantics changes require an ADR before implementation**.
   - Examples: NULL truth tables, type coercion, operator precedence, LIKE escaping behavior, DISTINCT semantics.
3. **Avoid new dependencies**. If a dependency is necessary (e.g., for a SQL function library), propose it with an ADR and clear justification.
4. **Small diffs, incremental merges**.
   - Implement in thin vertical slices: parse → bind/type → plan/exec → tests.

## 3. What counts as “implemented”
An item is considered implemented only when:
- It is documented (SPEC + user docs as appropriate).
- It has **unit tests** for edge cases.
- It has at least one of: **differential tests** (vs PostgreSQL / SQLite / DuckDB as appropriate) or **property tests**.
- If it touches DDL/catalog semantics: it has **crash-injection** coverage.
- Performance: existing benchmark thresholds remain within bounds; new benchmarks added where they meaningfully guard regressions.

## 4. Roadmap overview (what comes first)
### Decision: implement **Richer expression language and built-ins** first
Rationale:
- It unblocks a large fraction of practical application SQL and is a prerequisite for clean semantics in later features (UPSERT predicates, CHECK constraints, partial indexes, triggers, etc.).
- It is mostly query-layer work (parser/binder/exec) and can be delivered in small, testable increments.

## 5. Planned items (ordered)
The ordering is optimized for application value per complexity, and for minimizing cross-cutting rework.

### 5.1 Richer expression language and built-ins (**Must-have**)
*Note: Parser choice is settled (ADR-0035 libpg_query); "parser" work below refers to AST mapping and validation.*

Scope (deliver in slices):
1. **NULL semantics & predicates**
   - `IS NULL`, `IS NOT NULL`
   - Define three-valued logic interaction for `AND`/`OR` and comparisons (ADR required).
2. **Core functions**
   - `COALESCE`, `NULLIF`
   - `length`, `lower`, `upper`, `trim`
3. **Expression features**
   - `CASE WHEN ... THEN ... ELSE ... END`
   - `CAST(x AS type)` with a narrow initial type matrix (ADR required).
4. **Operators and precedence**
   - Ensure consistent precedence rules (ADR required if it differs from current behavior).
   - String concatenation operator `||`.
5. **Predicate forms**
   - `BETWEEN`, `IN (...)` (non-subquery form first)
   - `EXISTS (...)` (requires subqueries; may be deferred behind CTE/subquery groundwork).
6. **LIKE edge cases**
   - Escaping and NULL behavior clarified and tested (ADR required).

Tests:
- Unit tests for parser + binder + exec.
- Differential tests against PostgreSQL for supported subset.
- Property tests for equivalences (e.g., `COALESCE(a,b)` vs `CASE` forms) where applicable.

### 5.2 UPSERT and DML conveniences (**Must-have**)
Scope:
- `INSERT ... ON CONFLICT ...` (at minimum: `DO NOTHING`, later `DO UPDATE`).
- `RETURNING` for DML (start with `RETURNING *` or explicit columns; decide via ADR).

Notes:
- Requires precise conflict target rules and constraint/index interaction (ADR required).
- May require planner/executor changes and careful WAL/constraint ordering.

Tests:
- Differential against PostgreSQL for supported forms.
- Crash tests for torn writes around conflict handling.

### 5.3 CTEs (`WITH ...`) (**Must-have**)
Scope:
- Non-recursive CTEs first.
- Recursive CTEs are explicitly deferred until non-recursive semantics are stable.

Notes:
- Requires well-defined name resolution and scoping rules (ADR likely).

Tests:
- Unit tests for scoping, shadowing, and evaluation.
- Differential tests for query results.

### 5.4 Set operations (**Must-have**)
Scope:
- `UNION ALL` first, then `UNION` (distinctness semantics ADR), then `INTERSECT`, `EXCEPT`.

Tests:
- Differential tests for ordering + duplicates.

### 5.5 Advanced constraints (CHECK, richer FK actions) (**Partially must-have**)
Scope:
- `CHECK` constraints first (high value).
- FK actions beyond current set (e.g., CASCADE/SET NULL) only after CHECK is stable.

Gating:
- Affects correctness/isolation/durability; ADR required.

Tests:
- Unit + property invariants.
- Crash tests for DDL + enforcement on recovery.

### 5.6 Advanced index options (multi-column / partial / expression indexes)
Scope:
- Multi-column indexes first (ADR-0069 Accepted; note: hash-based, no range scans).
- Partial indexes next (requires predicate semantics ADR).
- Expression indexes last (type system + expression stability prerequisite).

Tests:
- Planner tests to ensure access path selection works.
- Differential where feasible.

### 5.7 Broader ALTER TABLE
Scope (implement in order):
1. **ADD COLUMN**
   - `ALTER TABLE t ADD COLUMN col type [DEFAULT ...] [NOT NULL?]`
   - Start with the safest subset: nullable columns with no DEFAULT expression evaluation.
2. **RENAME COLUMN**
   - `ALTER TABLE t RENAME COLUMN old TO new`
   - Catalog-only change, but must be dependency-safe (indexes, constraints, views, prepared statements).
3. **DROP COLUMN**
   - `ALTER TABLE t DROP COLUMN col`
   - Often implies a table rewrite/row format change depending on storage layout; treat as format/durability sensitive.
4. **ALTER COLUMN TYPE**
   - `ALTER TABLE t ALTER COLUMN col TYPE newType`
   - Potential data rewrite + conversion semantics; requires a clearly specified conversion matrix and failure behavior.

Notes:
- Each operation has different catalog, rewrite, and durability implications. Prefer shipping each as a standalone slice with its own ADR, tests, and crash coverage.

Gating:
- Often touches catalog + possibly data rewrite/migration; ADR required.
- Must include crash/durability coverage.

### 5.8 Triggers (including INSTEAD OF triggers)
Scope:
- Start with AFTER triggers for DML; defer INSTEAD OF until trigger core is solid.
- Updatable views via INSTEAD OF triggers is an explicit follow-on.

Gating:
- Large surface area, impacts transactional semantics; ADR required.

### 5.9 Window functions
Scope:
- Start with a narrow, high-demand subset (e.g., `ROW_NUMBER()` over partition/order) once sort/aggregate infra is stable.

Gating:
- Planner/executor complexity; ADR likely.

### 5.10 Operational capabilities (not strictly SQL)
These are commonly expected for usability, but should not compromise core guarantees.

1. **Introspection and settings**
   - Catalog-like queries, schema introspection commands, minimal settings surface.
2. **Explain and profiling tooling**
   - `EXPLAIN` (plan structure) (ADR-0050 Accepted), and optional profiling hooks.
3. **Loadable extension ecosystem** **deferred, see https://github.com/sphildreth/decentdb/issues/7**
   - This is the largest design commitment.
   - Requires an ADR defining API/ABI stability expectations, sandboxing, and durability/correctness constraints.
   - If pursued pre-1.0, it must be deliberately minimal and safe.

## 6. Documentation updates policy
- User-facing docs should only claim support once an item meets Section 3.
- The comparison page should link to this document for the roadmap rather than listing every not-yet-implemented detail inline.

## 7. Open questions (require ADRs before implementation)
- Exact NULL truth tables and comparison semantics.
- Type coercion rules and `CAST` failure behavior.
- DISTINCT semantics (`UNION`, `SELECT DISTINCT`, `COUNT(DISTINCT ...)` if/when added).
- LIKE escaping syntax and behavior.
- Subquery support surface needed for `EXISTS` and `IN (subquery)`.
- Extension/plugin architecture (if pursued), including ABI stability and safety.
