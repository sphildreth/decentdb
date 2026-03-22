## INSERT ON CONFLICT DO NOTHING (v0)
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Add initial UPSERT support for `INSERT ... ON CONFLICT ... DO NOTHING` with strict limits:

1. Supported forms
- `INSERT ... ON CONFLICT DO NOTHING`
- `INSERT ... ON CONFLICT (col [, ...]) DO NOTHING`
- `INSERT ... ON CONFLICT ON CONSTRAINT constraint_name DO NOTHING`

2. Conflict target resolution
- `ON CONFLICT DO NOTHING` (no target):
  - Suppress any unique/primary-key conflict raised by the insert path.
- `ON CONFLICT (col [, ...]) DO NOTHING`:
  - Column list must exactly match a unique key surface in table metadata:
    - single-column `UNIQUE`
    - single-column `PRIMARY KEY`
    - unique index metadata with matching ordered columns
  - If conflict occurs on a different unique key than the target, return constraint error.
- `ON CONFLICT ON CONSTRAINT name DO NOTHING`:
  - `name` resolves to an existing unique index metadata name in DecentDB catalog.
  - If not found or not unique, bind-time error.

3. Error scope
- `DO NOTHING` only suppresses unique/PK conflict errors.
- `NOT NULL`, foreign key, type, and other SQL/storage errors are not suppressed.

4. Out of scope in v0
- `ON CONFLICT ... DO UPDATE`
- DML `RETURNING`
- Predicate/index-expression conflict targets

### Rationale

- Roadmap requires UPSERT starting with `DO NOTHING`.
- This provides immediate user value with low execution risk and no persistence-format changes.
- Keeping target matching explicit avoids ambiguous behavior and keeps compatibility surface testable.

### Alternatives Considered

1. **Implement `DO UPDATE` in the same slice**
- Rejected due significantly larger planner/executor and semantic surface.

2. **Support only targetless `ON CONFLICT DO NOTHING`**
- Rejected because explicit targeting is expected in practical SQL usage.

3. **Map `ON CONSTRAINT` to SQL table constraint names**
- Rejected for v0 because current catalog metadata tracks unique index names, not full SQL constraint naming.

### Trade-offs

- `ON CONSTRAINT` naming follows DecentDB unique index metadata names in v0, which may differ from PostgreSQL expectation for named table constraints.
- Some PostgreSQL-valid inference targets remain unsupported in v0.
- Behavior is explicit, bounded, and can be extended later without storage/WAL changes.

### References

- SQL enhancements roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.2)
- Existing uniqueness/index metadata behavior: `src/engine.nim`, `src/catalog/catalog.nim`
