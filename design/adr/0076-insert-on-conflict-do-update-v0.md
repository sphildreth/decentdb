## INSERT ON CONFLICT DO UPDATE (v0)
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Add initial `INSERT ... ON CONFLICT ... DO UPDATE` support with a narrow, explicit subset:

1. Supported forms
- `INSERT ... ON CONFLICT (col [, ...]) DO UPDATE SET ...`
- `INSERT ... ON CONFLICT ON CONSTRAINT name DO UPDATE SET ...`

2. Explicitly unsupported in this slice
- `INSERT ... ON CONFLICT DO UPDATE ...` without a conflict target
- Conflict-target predicates/expressions (`ON CONFLICT (...) WHERE ...`)
- `RETURNING`

3. Conflict target resolution
- Reuse ADR-0075 target validation:
  - column-list targets must match a unique surface exactly
  - `ON CONSTRAINT name` resolves to a DecentDB unique index metadata name

4. `DO UPDATE` expression scope
- Assignment targets must be columns in the inserted table.
- Assignment/`WHERE` expressions may reference:
  - target table columns
  - `EXCLUDED.col`
- Unqualified column references in `DO UPDATE` expressions are bound to the target table.

5. Conflict handling behavior
- If the specified conflict target matches an existing row:
  - evaluate optional `DO UPDATE ... WHERE ...`
  - if condition is `TRUE`, update that conflicting row
  - if condition is `FALSE` or `NULL`, do nothing
- If the specified conflict target does not match, normal insert is attempted and non-target conflicts still error.
- Constraint checks after update use existing update paths:
  - `NOT NULL`
  - uniqueness (excluding the updated rowid)
  - foreign keys
  - parent-side RESTRICT checks

### Rationale

- Roadmap requires adding `DO UPDATE` after `DO NOTHING`.
- Requiring explicit conflict targets keeps behavior deterministic and avoids ambiguous arbiter selection in v0.
- Reusing existing update constraint checks minimizes risk and avoids storage/WAL changes.

### Alternatives Considered

1. **Support targetless `ON CONFLICT DO UPDATE` now**
- Rejected in v0 to avoid ambiguous conflict-row selection semantics and keep this slice small.

2. **Allow broader PostgreSQL inference/predicate targets**
- Rejected for v0 due larger semantic and parser/binder scope.

3. **Implement `RETURNING` together with `DO UPDATE`**
- Rejected; kept for a follow-on slice with its own ADR-gated SQL surface.

### Trade-offs

- This subset is narrower than PostgreSQL (explicit target required).
- Predictability and safety improve, but some valid PostgreSQL UPSERT forms remain unsupported for now.

### References

- SQL enhancements roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.2)
- Prior conflict-target ADR: `design/adr/0075-insert-on-conflict-do-nothing-v0.md`
