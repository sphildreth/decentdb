## ALTER TABLE ADD COLUMN (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement `ALTER TABLE <table> ADD COLUMN <col> <type>` in 0.x with this behavior:

1. Scope
- Support adding columns to base tables.
- **Restriction:** New columns must be nullable. `DEFAULT` clauses and `NOT NULL` constraints are **not** supported in this initial version (v0).
- Keep existing 0.x `ALTER TABLE` restriction: tables with CHECK constraints are rejected.

2. Implementation Strategy
- **Full Table Rewrite**:
  1. Create a new B+Tree root.
  2. Scan the original table.
  3. For each row, decode, append the new NULL value, normalize, and insert into the new B+Tree.
  4. Update table metadata (root page ID, column list) in the catalog.
  5. Commit via standard transaction/WAL path.

3. Durability/Persistence
- The rewrite generates WAL frames for the new pages.
- The operation is atomic: either the catalog update commits (pointing to the new root), or it rolls back (retaining the old root).
- Crash safety is ensured by the WAL.

### Rationale

- Roadmap section 5.7 calls for `ADD COLUMN`.
- A rewrite strategy is simple and robust for v0, avoiding complex page layout versioning or schema-on-read logic.
- Restricting to nullable columns avoids the need to evaluate default expressions during the rewrite, simplifying the implementation.

### Alternatives Considered

1. Schema-on-read (lazy add)
- Store a schema version in the page/row and handle missing columns at read time.
- Rejected for v0 to maintain strict "what you see is what is stored" simplicity in the row format and avoid read-path complexity.

2. In-place update
- Attempt to append to existing records if space permits.
- Rejected because B+Tree keys/values are tightly packed; shifting data is complex and prone to fragmentation. Rewrite is cleaner.

### Trade-offs

- Performance: O(N) cost proportional to table size.
- Concurrency: Exclusive lock required during the rewrite (handled by single-writer model).

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.7)
