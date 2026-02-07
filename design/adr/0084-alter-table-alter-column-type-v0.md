## ALTER TABLE ALTER COLUMN TYPE (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement a constrained `ALTER TABLE ... ALTER COLUMN ... TYPE ...` in 0.x:

1. Supported scope
- Syntax: `ALTER TABLE t ALTER COLUMN c TYPE <type>`.
- Supported source and target column kinds: `INT64`, `FLOAT64`, `TEXT`, `BOOL`.
- Uses per-row conversion semantics aligned with existing SQL `CAST` behavior.

2. Unsupported in v0
- `BLOB`, `DECIMAL`, `UUID` conversions.
- Altering PRIMARY KEY columns.
- Altering FK child columns (columns with `REFERENCES` metadata).
- Altering columns referenced by FKs in other tables.
- Tables with CHECK constraints remain excluded by existing 0.x ALTER limitation.

3. Execution and failure behavior
- Operation rewrites table rows to a new table root, converting the target column value per row.
- `NULL` remains `NULL`.
- Any conversion failure aborts the statement (no partial schema/data change committed).
- Indexes on the table are rebuilt after rewrite.

4. Durability/persistence
- Uses existing catalog/table save + WAL-backed DDL flow.
- No persistent format, page layout, WAL frame, or checkpoint rule changes.

### Rationale

- Roadmap section 5.7 requires ALTER COLUMN TYPE with explicit conversion/failure semantics.
- A narrow type matrix reduces correctness risk while unlocking practical migration workflows.
- Reusing CAST-compatible conversion behavior keeps semantics predictable.

### Alternatives Considered

1. Support all existing types immediately (including DECIMAL/UUID/BLOB)
- Rejected due significantly larger conversion and validation surface.

2. Metadata-only type flip without data rewrite
- Rejected; unsafe because existing stored values may violate target type semantics.

3. Allow PK/FK-participating columns in v0
- Rejected to avoid key/constraint invariants risk in initial slice.

### Trade-offs

- Restrictive initial scope may require multi-step migrations for complex schemas.
- Rewrite cost is proportional to table size and can be expensive for large tables.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.7)
- CAST semantics baseline: `design/adr/0072-sql-cast-coercion-and-failure-semantics.md`
