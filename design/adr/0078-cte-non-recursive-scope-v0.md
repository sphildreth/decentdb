## Non-Recursive CTE Semantics and Scope (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement `WITH` for **non-recursive CTEs** in `SELECT` statements with the following v0 rules:

1. Supported surface
- `WITH cte_name [(col1, ...)] AS (SELECT ...) [, ...] SELECT ...`
- CTE bodies must be `SELECT` statements.
- `WITH RECURSIVE` is rejected.

2. Name resolution and scope
- CTE names are resolved in declaration order.
- A CTE may reference previously declared CTEs in the same `WITH` list.
- Forward references are not supported.
- CTE names shadow catalog tables/views of the same name inside the statement scope.

3. v0 execution model
- CTEs are implemented via binder-time expansion/rewrite (inline expansion), not persisted and not materialized as catalog objects.
- No storage, WAL, page, or recovery format changes.

4. v0 shape restrictions for CTE bodies
- CTE body expansion follows the existing safe expansion path used for view inlining.
- CTE bodies with `GROUP BY`/`HAVING`, `ORDER BY`, or `LIMIT/OFFSET` are rejected in v0.

### Rationale

- The roadmap requires non-recursive CTEs before recursive semantics.
- Binder-time expansion gives immediate query capability without touching durability-critical layers.
- Reusing the existing expansion model keeps scope controlled and testable.

### Alternatives Considered

1. Full CTE materialization/executor nodes first
- Rejected for v0 due larger planner/executor surface and regression risk.

2. Support recursive CTEs immediately
- Rejected; recursion semantics and cycle handling are significantly larger scope.

3. Allow all CTE body shapes in v0
- Rejected because safe rewrite with grouping/ordering/limits requires additional planner/executor work.

### Trade-offs

- Some PostgreSQL-valid CTE queries are deferred in v0 due body-shape restrictions.
- Rewrite semantics avoid storage changes and enable incremental delivery.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.3)
- Parser ADR: `design/adr/0035-sql-parser-libpg_query.md`
- View semantics ADR: `design/adr/0070-views-catalog-and-semantics.md`
