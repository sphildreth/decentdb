## Window Functions: RANK, DENSE_RANK, LAG, LEAD
**Date:** 2026-02-23
**Status:** Accepted

### Decision

Extend the window function surface (ADR-0086 introduced `ROW_NUMBER` only) with four additional functions:

1. `RANK() OVER (PARTITION BY ... ORDER BY ...)`
2. `DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)`
3. `LAG(expr [, offset [, default]]) OVER (PARTITION BY ... ORDER BY ...)`
4. `LEAD(expr [, offset [, default]]) OVER (PARTITION BY ... ORDER BY ...)`

Constraints carried forward from ADR-0086:
- `PARTITION BY` is optional; `ORDER BY` inside `OVER(...)` is required.
- Window functions are only allowed in the `SELECT` projection list (not in WHERE, HAVING, JOIN, GROUP BY, or top-level ORDER BY).
- Window frame clauses (`ROWS BETWEEN ...`, `RANGE BETWEEN ...`) are out of scope.

Implementation approach:
- Add `windowFunc` (string) and `windowArgs` (seq[Expr]) fields to the existing `ekWindowRowNumber` Expr variant rather than creating new ExprKind values, to mirustise diff.
- RANK/DENSE_RANK reuse the same partition-sort buffer as ROW_NUMBER with different counter logic.
- LAG/LEAD perform O(1) index lookups on the sorted partition buffer.

### Rationale

- These four functions are the most commonly requested analytical window functions.
- The existing ROW_NUMBER infrastructure (partition, sort, enumerate) handles 90% of the work; extending it is low-risk.
- No persistent format changes — query-layer only (same as ADR-0086).
- No new dependencies.

### Alternatives Considered

1. Add four new ExprKind variants (`ekWindowRank`, etc.)
   - Rejected: duplicates three fields across five variants and touches 23+ call sites for the rename.

2. Implement full SQL:2003 window function suite
   - Rejected: `NTILE`, `PERCENT_RANK`, `CUME_DIST`, `NTH_VALUE`, `FIRST_VALUE`, `LAST_VALUE` can be added incrementally in future ADRs.

3. Allow window functions without ORDER BY
   - Rejected for v1.5: RANK/DENSE_RANK are meaningless without ORDER BY; LAG/LEAD need deterministic row ordering.

### Trade-offs

- The `ekWindowRowNumber` variant name becomes slightly misleading since it now carries all window functions. Acceptable vs. a 23-site rename.
- LAG/LEAD offset must be a non-negative integer literal in v1.5 (no expression offsets). This matches common usage and avoids runtime type-check overhead.
- NULL ordering follows existing `compareValues` behaviour (NULL < everything), which differs from PostgreSQL's default NULLS LAST. This is consistent with ROW_NUMBER behaviour already shipped.

### References

- ADR-0086: Window Functions: ROW_NUMBER() Subset (v0)
- Issue #22: Window functions: RANK, DENSE_RANK, LAG, LEAD
- PostgreSQL window function documentation
