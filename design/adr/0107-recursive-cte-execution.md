## Recursive CTE Execution Strategy
**Date:** 2026-02-24
**Status:** Accepted

### Decision

Implement `WITH RECURSIVE` using **iterative fixpoint evaluation in the executor**, reusing the existing CTE inline expansion model for the non-recursive (anchor) term and adding a new execution loop for the recursive term.

1. **Parsing** — Remove the `WITH RECURSIVE` rejection in `sql.nim`. Add a `recursive: bool` flag to the `Statement` type for `skSelect`, propagated from `parseWithClause`.

2. **Binding** — For recursive CTEs, skip the standard inline expansion. Instead, preserve the CTE definition for executor-time evaluation. The recursive CTE query body must be a `UNION ALL` (or `UNION`) of exactly two branches: the **anchor** (non-self-referencing) and the **recursive** (self-referencing) term.

3. **Execution** — Implement iterative fixpoint evaluation directly in the executor:
   - Execute the anchor term to produce the initial **working table**.
   - Repeatedly execute the recursive term using the working table as input, producing new rows.
   - For `UNION ALL`: append new rows to the result.
   - For `UNION`: deduplicate new rows against all previously seen rows.
   - Terminate when the recursive term produces zero new rows (**empty working table**).
   - Enforce a configurable iteration limit (default: 1000) to prevent infinite loops.

4. **Scope restrictions (v0)**
   - Only one recursive CTE per `WITH` clause.
   - The recursive term must reference the CTE name exactly once.
   - Aggregates, `ORDER BY`, `LIMIT`, `DISTINCT`, window functions, and subqueries in the recursive term are rejected.
   - Mutual recursion (CTE A references CTE B which references CTE A) is not supported.

5. **No storage/format changes** — Recursive CTEs are fully evaluated in memory. No WAL, page layout, or persistent format changes.

### Rationale

- Iterative fixpoint is the SQL standard approach and matches PostgreSQL/SQLite behavior.
- In-executor evaluation avoids planner complexity (no new plan node type needed for v0).
- The iteration limit provides a safety net against infinite recursion without requiring cycle detection.
- Scope restrictions keep v0 manageable while covering the most common use cases (tree/graph traversal, sequence generation).

### Alternatives Considered

1. **New plan node (`pkRecursiveCTE`)** — Adds planner complexity. Deferred; can be added later for optimization.
2. **Cycle detection via `CYCLE` clause** — SQL:2011 feature. Deferred to a future version.
3. **Materialized CTE execution for all CTEs** — Larger scope; would change non-recursive CTE behavior.

### Trade-offs

- v0 scope restrictions exclude some valid recursive queries (e.g., with aggregates in recursive term).
- The iteration limit is a blunt instrument; future versions could add `CYCLE` clause support.
- In-memory evaluation means very large recursive results consume proportional memory.

### References

- Non-recursive CTE ADR: `design/adr/0078-cte-non-recursive-scope-v0.md`
- Parser ADR: `design/adr/0035-sql-parser-libpg-query.md`
- PostgreSQL WITH RECURSIVE docs: https://www.postgresql.org/docs/current/queries-with.html
