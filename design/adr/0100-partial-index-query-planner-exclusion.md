# ADR 0100: Partial Index Query Planner Exclusion

## Status

Accepted

## Context

DecentDB supports partial (filtered) indexes created with `CREATE UNIQUE INDEX ... WHERE predicate`. The index only contains rows matching the predicate. Prior to this change, the query planner's `getBtreeIndexForColumn` and `getIndexForColumn` functions returned the first matching B-tree index for a column without checking whether the index was partial.

This caused incorrect query results: a query like `SELECT ... WHERE Type = 3` would use a partial index with predicate `WHERE Type != 3`. Since the index only contains rows where `Type != 3`, seeking `Type = 3` returned zero results — a correctness bug.

## Decision

Skip partial indexes (those with non-empty `predicateSql`) in `getBtreeIndexForColumn` and `getIndexForColumn`. When only a partial index exists for a column, these functions return `none`, causing the query planner to fall back to a table scan.

This is the simplest correct approach. A more sophisticated approach would evaluate whether the seek value satisfies the index predicate, but this adds complexity and the table scan fallback is correct for all cases.

## Consequences

- **Correctness**: Queries whose filter doesn't match a partial index predicate now return correct results via table scan.
- **Performance**: Queries that *could* benefit from a partial index (where the seek value matches the predicate) will use a table scan instead. This is a conservative trade-off favoring correctness over optimization.
- **Future work**: A smarter approach could pass the seek value to the index selection function and evaluate it against the predicate, allowing partial index use when the value is known to satisfy the predicate.
- **No format changes**: This is a query planner behavior change only; no persistent format or WAL changes.
