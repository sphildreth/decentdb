## INSERT RETURNING (v0)
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Add initial DML `RETURNING` support as a narrow subset:

1. Supported scope
- `INSERT ... RETURNING ...` only
- Works for plain INSERT and `INSERT ... ON CONFLICT ...` paths
- `RETURNING *` and explicit returning expressions/columns are supported

2. Unsupported in this slice
- `UPDATE ... RETURNING`
- `DELETE ... RETURNING`

3. Row production semantics
- Plain INSERT success returns one row from the inserted values.
- `ON CONFLICT DO NOTHING` returns zero rows when no write occurs.
- `ON CONFLICT ... DO UPDATE`:
  - returns one row when update executes
  - returns zero rows when `DO UPDATE ... WHERE` evaluates to `FALSE` or `NULL`

4. Expression scope
- `RETURNING` expressions bind against target table columns (same as PostgreSQL baseline for INSERT RETURNING).
- `EXCLUDED` is not part of RETURNING scope.

### Rationale

- Roadmap requires DML RETURNING but allows starting with a constrained surface.
- INSERT RETURNING gives immediate app-level value with low risk to storage/WAL semantics.
- Deferring UPDATE/DELETE RETURNING keeps this slice focused and testable.

### Alternatives Considered

1. **Implement RETURNING for INSERT/UPDATE/DELETE together**
- Rejected for this slice to keep changes small and reduce semantic risk.

2. **Support only `RETURNING *`**
- Rejected; explicit expressions/columns are common and low incremental cost.

3. **Keep RETURNING unsupported until full DML coverage**
- Rejected; delays practical value and contradicts incremental roadmap delivery.

### Trade-offs

- SQL surface is intentionally partial in v0.
- C API non-select stepping path does not expose RETURNING rows in this slice.

### References

- SQL enhancements roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.2)
- UPSERT ADRs: `design/adr/0075-insert-on-conflict-do-nothing-v0.md`, `design/adr/0076-insert-on-conflict-do-update-v0.md`
