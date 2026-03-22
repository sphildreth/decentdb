## Window Functions: `ROW_NUMBER()` Subset (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement a narrow window-function surface for 0.x:

1. Supported syntax
- `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`
- `PARTITION BY` is optional.
- `ORDER BY` inside `OVER (...)` is required in v0.

2. Unsupported in v0
- All other window functions (`RANK`, `DENSE_RANK`, `LAG`, etc.).
- Window frame clauses / frame option tuning.
- Window expressions outside `SELECT` projection (e.g., `WHERE`, `HAVING`, join predicates, top-level `ORDER BY`).

3. Execution semantics
- `ROW_NUMBER` is computed after row retrieval/filtering and before final projection output.
- Numbering resets per partition and increments by 1 in window order.
- Deterministic tie-break for equal partition/order keys uses row position as final stable comparator.

4. Durability and format impact
- Query-layer only feature.
- No catalog/page/WAL/checkpoint format changes.

### Rationale

- Roadmap section 5.9 asks for a narrow, high-demand starting point.
- `ROW_NUMBER` provides practical value for pagination/reporting while keeping planner/executor changes contained.
- Requiring `ORDER BY` avoids surprising nondeterministic numbering in v0.

### Alternatives Considered

1. Full SQL window suite in one slice
- Rejected due large planner/executor surface and testing scope.

2. Allow `ROW_NUMBER` without `ORDER BY`
- Rejected for v0 to avoid ambiguous result ordering semantics.

### Trade-offs

- Scope is intentionally smaller than PostgreSQL.
- Queries needing richer window semantics remain deferred to later ADR/slices.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.9)
