## ALTER TABLE DROP COLUMN (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement `ALTER TABLE <table> DROP COLUMN <col>` in 0.x with this behavior:

1. Scope
- Support dropping columns from base tables.
- Keep existing 0.x `ALTER TABLE` restriction: tables with CHECK constraints are rejected.

2. Dependency Safety
- **Indexes**: Indexes defined *solely* on the dropped column are automatically dropped. Indexes referencing the column as part of a multi-column key (if supported) would also need to be dropped or modified (current scope assumes simple index drop).
- **Views**: Reject drop if the table has dependent views. Users must drop dependent views first.

3. Implementation Strategy
- **Full Table Rewrite**:
  1. Create a new B+Tree root.
  2. Scan the original table.
  3. For each row, decode, remove the target value, normalize, and insert into the new B+Tree.
  4. Drop associated single-column indexes.
  5. Rebuild remaining indexes (since row locations/rowids are preserved, but index rebuild ensures consistency).
  6. Update table metadata (root page ID, column list) in the catalog.
  7. Commit via standard transaction/WAL path.

4. Durability/Persistence
- The rewrite generates WAL frames.
- The operation is atomic via the WAL.

### Rationale

- Roadmap section 5.7 calls for `DROP COLUMN`.
- Physical removal of data (rewrite) recovers space immediately and avoids "zombie" columns in the row format.

### Alternatives Considered

1. Logical Delete (Soft Drop)
- Mark column as "hidden" in metadata but leave data on disk.
- Rejected: Wastes space and complicates `SELECT *` expansion and limit checks.

### Trade-offs

- Performance: O(N) cost proportional to table size.
- Concurrency: Exclusive lock required.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.7)
