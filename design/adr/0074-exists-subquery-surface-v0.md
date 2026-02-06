## EXISTS Subquery Surface (v0)
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Add initial SQL subquery support only for `EXISTS (subquery)` with strict limits:

1. Supported form
- `EXISTS (SELECT ...)` in expressions/predicates.
- Subquery must parse and bind as a `SELECT`.

2. Correlation scope
- **Non-correlated only** in v0.
- Outer references inside the subquery are rejected during binding/execution.

3. Execution semantics
- `EXISTS` returns:
  - `TRUE` if subquery returns at least one row
  - `FALSE` otherwise
- No `NULL` result for `EXISTS` itself (standard SQL semantics).

4. Out of scope for v0
- `IN (subquery)` and scalar subqueries
- Correlated subqueries
- LATERAL semantics

### Rationale

- Roadmap requires eventual `EXISTS` implementation and explicitly calls out subquery surface decisions.
- A non-correlated `EXISTS` subset provides practical value with controlled complexity.
- This avoids immediate planner/executor rewrites needed for full correlated semantics.

### Alternatives Considered

1. **Implement all subquery forms at once**
- Rejected due complexity and risk for a single slice.

2. **Defer all subqueries**
- Rejected because roadmap explicitly includes `EXISTS` in expression work.

3. **Allow correlated EXISTS immediately**
- Rejected for now; requires additional binding/planning machinery and careful performance controls.

### Trade-offs

- Some valid PostgreSQL queries (correlated EXISTS) remain unsupported in 0.x v0 surface.
- Behavior is explicit and testable, and can be extended later without storage changes.

### References

- SQL enhancements roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (EXISTS + subquery open question)
- Parser ADR: `design/adr/0035-sql-parser-libpg-query.md`
