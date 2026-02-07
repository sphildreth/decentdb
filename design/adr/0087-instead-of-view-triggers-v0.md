## INSTEAD OF View Triggers (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Extend the trigger surface with a constrained `INSTEAD OF` subset for views in 0.x.

1. Supported scope
- `CREATE TRIGGER ... INSTEAD OF ... ON <view> FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('<sql>')`
- `DROP TRIGGER ... ON <view>`
- Events: `INSERT`, `UPDATE`, `DELETE` (single or `OR` combinations).
- `FOR EACH ROW` only.

2. View DML behavior
- Views remain read-only unless a matching `INSTEAD OF` trigger exists for the operation.
- `INSERT INTO view ...` fires matching `INSTEAD OF INSERT` triggers once per inserted row.
- `UPDATE view ...` and `DELETE FROM view ...` fire matching `INSTEAD OF` triggers once per affected view row (row count determined by the view query + statement predicate).
- Trigger actions execute in the same transaction as the parent statement.

3. Action and semantics limits
- Action function remains `decentdb_exec_sql('<single DML>')`.
- Action SQL must still be exactly one parameterless DML statement (`INSERT`/`UPDATE`/`DELETE`).
- `NEW`/`OLD` row references are still not supported in v0.
- `INSERT ... ON CONFLICT` and `INSERT ... RETURNING` are not supported for view targets in this slice.

4. Persistence and compatibility
- No new catalog record type and no format bump.
- Trigger timing is encoded using the existing PostgreSQL timing bit (`64`) in persisted `eventsMask`.
- No DB header/page layout/WAL/checkpoint changes.

### Rationale

- Roadmap section 5.8 calls out `INSTEAD OF` triggers and updatable views as the next trigger step after AFTER-trigger core.
- Reusing the existing trigger action bridge keeps complexity bounded while providing practical view-write interception.
- Encoding timing in existing event metadata avoids persistent-format churn.

### Alternatives Considered

1. Full `NEW`/`OLD` support in this slice
- Rejected for scope/complexity; requires row-value plumbing and additional SQL interpolation semantics.

2. Keep views strictly read-only
- Rejected because it leaves the roadmap trigger phase incomplete.

3. New trigger metadata schema version
- Rejected; existing event-mask storage already carries required timing information.

### Trade-offs

- `INSTEAD OF` behavior is narrower than PostgreSQL due missing `NEW`/`OLD`.
- Updatable views are practical for action patterns that do not need row-value interpolation.
- Additional statement-level counting work for `UPDATE`/`DELETE` on views adds overhead, but only on this constrained path.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.8)
- Prior trigger ADR: `design/adr/0085-after-triggers-v0.md`
