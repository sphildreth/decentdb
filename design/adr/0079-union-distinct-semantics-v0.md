## Set-Operation DISTINCT Semantics (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement distinct set-operation semantics in 0.x for `UNION`, `INTERSECT`, and `EXCEPT` (non-`ALL` forms) with these rules:

1. Duplicate elimination rule
- Rows are duplicates when they have the same arity and each corresponding value is equal under SQL set-op distinctness.
- `NULL` values are considered equal to `NULL` for duplicate elimination.
- This matches PostgreSQL set-operation behavior for `UNION`.

2. Scope
- Applies to set-operation distinctness for `UNION`, `INTERSECT`, and `EXCEPT`.
- `UNION ALL` keeps all rows.
- `INTERSECT ALL` and `EXCEPT ALL` remain deferred.
- `SELECT DISTINCT` and `COUNT(DISTINCT ...)` remain separate decisions.

3. Execution model
- Implemented in executor as value-tuple distinct filtering over row outputs from both branches.
- No catalog/storage/WAL/recovery format changes.

4. v0 constraints
- Branches must produce matching column counts.
- ORDER stability is not guaranteed without explicit `ORDER BY` (standard SQL behavior).

### Rationale

- Roadmap requires an ADR for DISTINCT semantics before implementing `UNION` and related distinct set operations.
- Using value-tuple equality with `NULL == NULL` for set semantics aligns with PostgreSQL behavior and user expectation.
- Isolating this decision to `UNION` keeps scope narrow and reduces risk.

### Alternatives Considered

1. Treat `NULL` as always distinct
- Rejected; diverges from PostgreSQL set-operation semantics.

2. Defer set operations until all DISTINCT surfaces are implemented
- Rejected; roadmap orders `UNION ALL` then `UNION` then `INTERSECT`/`EXCEPT`.

3. Reuse row-id based dedupe used by OR-plan union internals
- Rejected; row-id identity is not SQL row-value distinctness.

### Trade-offs

- Distinct filtering materializes branch outputs in v0; this is simple but may use more memory for large results.
- Broader DISTINCT surfaces still need separate ADR/design work.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.4, open question on DISTINCT semantics)
- Planner/exec internals: `src/planner/planner.nim`, `src/exec/exec.nim`
