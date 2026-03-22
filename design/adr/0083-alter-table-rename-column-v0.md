## ALTER TABLE RENAME COLUMN (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement `ALTER TABLE <table> RENAME COLUMN <old> TO <new>` in 0.x with this behavior:

1. Scope
- Support column rename for base tables.
- Keep existing 0.x `ALTER TABLE` restriction: tables with CHECK constraints remain rejected.

2. Dependency safety
- Reject rename when the table has dependent views.
- Prepared statements continue to be invalidated via schema-cookie change.

3. Metadata updates
- Rename target column in table metadata.
- Update index metadata column lists for indexes on the renamed table.
- Update FK metadata in all tables where `refTable == renamedTable` and `refColumn == old`.
- For v0 partial indexes, update stored predicate SQL for the supported shape (`<col> IS NOT NULL`).

4. Durability/persistence
- Use existing catalog save/update flow; no WAL/page/checkpoint format changes.
- Operation is transactional under existing DDL write semantics.

### Rationale

- Roadmap section 5.7 calls for `RENAME COLUMN` as a standalone slice.
- This is a high-value migration primitive with modest storage risk if limited to metadata updates.
- Explicit dependency rejection for views avoids silently breaking dependent objects.

### Alternatives Considered

1. Rewrite dependent view definitions automatically
- Rejected for v0 due higher semantic risk and parser/rewrite complexity.

2. Allow rename on CHECK tables and rewrite CHECK SQL text
- Rejected for v0; CHECK-alter support remains deferred.

3. Defer rename entirely
- Rejected; roadmap requires broader ALTER TABLE progression.

### Trade-offs

- Conservative behavior (view dependency rejection) may require users to drop/recreate views.
- CHECK-table restriction limits immediate usability but keeps behavior predictable in 0.x.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.7)
- Existing ALTER baseline: `design/SPEC.md` (Section 15.3)
