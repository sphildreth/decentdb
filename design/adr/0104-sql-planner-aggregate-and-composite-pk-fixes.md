# ADR 0104: SQL Planner — Aggregate ORDER BY, Composite PK, and UNION Subquery Fixes

## Status

Accepted

## Context

Three related query planner and binder issues surfaced during EF Core integration testing:

1. **ORDER BY aggregate alias**: `SELECT COUNT(*) AS "AlbumCount" ... GROUP BY ... ORDER BY "AlbumCount"` failed because the binder replaced the alias reference with a clone of the aggregate expression. The sort node then tried to re-evaluate `COUNT(*)` against individual rows instead of the materialized aggregate output.

2. **Composite primary key rowid seek**: Tables with composite PKs (e.g., `(AlbumId, ArtistId)`) were incorrectly treated as having a rowid alias on the first integer PK column. The planner generated a rowid seek instead of an index seek, returning wrong results.

3. **UNION subquery derived tables**: `FROM (SELECT ... UNION SELECT ...)` failed in `subqueryTableMeta` because the function tried to derive columns from the UNION node's (empty) select items instead of recursing into the left operand.

## Decision

1. **Binder**: When an ORDER BY alias matches a SELECT item whose expression is an aggregate function (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP_CONCAT`, `STRING_AGG`), keep the ORDER BY as a column reference to the alias instead of cloning the aggregate expression. **Planner**: When building a Sort node above an Aggregate node, rewrite any ORDER BY items that contain aggregate expressions to column references matching the SELECT alias.

2. **Planner**: `isRowidPkColumn` now counts PK columns first. If the table has more than one PK column (composite key), no column is treated as a rowid alias.

3. **Binder**: `subqueryTableMeta` recursively follows `setOpLeft` for UNION/INTERSECT/EXCEPT nodes to find the actual SELECT items that define the result columns. Also updated `selectToCanonicalSql` to serialize `fromSubquery` and `joinSubqueries` instead of emitting empty `FROM` clauses.

## Consequences

- EF Core queries with `ORDER BY` on aggregated columns work correctly
- Composite PK tables use index seeks instead of incorrect rowid seeks
- Subqueries using set operations (UNION, INTERSECT, EXCEPT) work as derived tables
- No performance impact — these are correctness fixes in cold (planning) paths
