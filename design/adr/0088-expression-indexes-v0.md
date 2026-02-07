## Expression Indexes (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement a narrow expression-index surface in 0.x:

1. Supported SQL shape
- `CREATE INDEX idx ON table ((expr))`
- BTREE only
- exactly one expression (no mixed expression + column lists)
- non-`UNIQUE`
- non-partial

2. Supported expression forms (deterministic subset)
- `column`
- `LOWER(column)`, `UPPER(column)`, `TRIM(column)`, `LENGTH(column)`
- `CAST(column AS type)` where type is one of `INT64`, `FLOAT64`, `TEXT`, `BOOL`

3. Planner usage
- Planner may use expression indexes for equality predicates where one side canonical-matches the indexed expression and the other side is non-correlated.
- Example: index on `LOWER(name)` can serve `WHERE LOWER(name) = 'alice'`.

4. Persistence
- No catalog format version bump.
- Expression metadata is stored in existing index-column metadata using an internal `expr:` prefix token.
- No DB header/page/WAL/checkpoint semantics changes.

5. Known v0 constraints
- No `UNIQUE` expression indexes.
- No partial expression indexes.
- No trigram expression indexes.
- `ALTER TABLE` operations on tables with expression indexes are rejected in v0 (to avoid stale expression metadata rewrite semantics in this slice).

### Rationale

- Roadmap section 5.6 requires expression indexes after multi-column and partial indexes.
- A narrow deterministic subset delivers practical value (case-insensitive lookups and simple normalization) with bounded planner/executor complexity.
- Reusing existing index metadata encoding avoids persistent-format churn.

### Alternatives Considered

1. Full PostgreSQL expression-index surface
- Rejected for v0 due broad expression and rewrite/dependency complexity.

2. New catalog field/schema for expression AST
- Rejected in this slice; existing metadata representation is sufficient for constrained v0 scope.

3. Defer expression indexes entirely
- Rejected because it leaves the advanced-index roadmap phase incomplete.

### Trade-offs

- Surface is intentionally narrower than PostgreSQL.
- Expression parse/evaluation path is constrained and explicit.
- Blocking `ALTER TABLE` on expression-indexed tables is conservative but safe for v0.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.6)
- Multi-column index ADR: `design/adr/0069-composite-primary-keys-and-indexes.md`
- Partial index ADR: `design/adr/0082-partial-indexes-v0.md`
