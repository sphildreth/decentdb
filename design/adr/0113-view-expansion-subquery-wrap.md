## View Expansion via Subquery Wrapping
**Date:** 2026-02-26
**Status:** Accepted

### Decision

Views and CTEs whose body contains GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET,
or DISTINCT ON are now expanded as **derived tables** (subqueries in FROM)
rather than being merge-inlined into the outer query.

Previously, `expandSelectViews` in `binder.nim` used `pushDownQuery()` to
merge-inline all view bodies.  This merge strategy cannot preserve result-set
semantics for views that aggregate, sort, or limit rows.  The binder rejected
such views at bind time with an error.

The new approach adds a `needsSubqueryWrap(stmt)` predicate.  When it returns
true, the view body is wrapped as a `fromSubquery` (or `joinSubqueries[i]` for
JOIN sources).  The outer query then sees the view as a regular derived table
with its own scope.

The same logic is applied to CTE references in `expandSelectCteRefs`.

Additionally, the HAVING clause now has its aggregate expressions collected and
substituted before evaluation, fixing a related bug where aggregate functions
in HAVING hit the scalar evaluator and failed.

### Rationale

- GROUP BY/HAVING/ORDER BY/LIMIT are standard SQL features in view definitions.
  PostgreSQL, SQLite, and MySQL all support them.  Rejecting them was a
  significant usability gap.
- Derived tables (subqueries in FROM) were already fully supported by the
  executor and planner.  Wrapping complex views as derived tables reuses
  existing infrastructure with minimal new code.
- The merge-inline path is preserved for simple views (no GROUP BY, ORDER BY,
  etc.) where it produces better plans by avoiding a subquery boundary.

### Alternatives Considered

1. **Extend pushDownQuery to handle GROUP BY/ORDER BY**: Would require
   the merge logic to understand when to preserve vs combine grouping,
   ordering, and limiting semantics.  Significantly more complex and
   error-prone than wrapping as a subquery.

2. **Always wrap views as subqueries**: Simpler code but degrades plan
   quality for simple views where merge-inlining produces better results
   (fewer intermediate result sets, better predicate pushdown).

### Trade-offs

- **Pro**: Views with GROUP BY, HAVING, ORDER BY, LIMIT, and DISTINCT ON now
  work correctly when queried.
- **Pro**: Minimal code change — adds a predicate and wrapping logic; does not
  alter the existing merge path.
- **Con**: Complex views wrapped as subqueries may inhibit certain optimizer
  rewrites (e.g., predicate pushdown into the view body).  This is acceptable
  at the current optimizer maturity level.
- **Known limitation**: DISTINCT ON in subqueries does not fully work because
  DISTINCT ON is applied as post-processing in `execSql` rather than in the
  subquery executor.  Views with DISTINCT ON will wrap correctly but the
  DISTINCT ON semantics are not preserved in the inner query.

### References

- `src/sql/binder.nim` — `needsSubqueryWrap`, `expandSelectViews`, `expandSelectCteRefs`
- `src/exec/exec.nim` — `collectAggSpecsFromExpr`, `aggregateRows` (HAVING fix)
