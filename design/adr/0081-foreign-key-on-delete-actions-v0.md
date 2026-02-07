## Foreign Key Actions (CASCADE / SET NULL) v0
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement v0 foreign-key action expansion with this scope:

1. Supported actions
- Existing behavior remains for `NO ACTION` / `RESTRICT`.
- Add support for `ON DELETE CASCADE`.
- Add support for `ON DELETE SET NULL`.
- Add support for `ON UPDATE CASCADE`.
- Add support for `ON UPDATE SET NULL`.

2. Enforcement timing
- Keep statement-time FK enforcement model (existing 0.x behavior).
- `ON DELETE` and `ON UPDATE` actions execute within the same statement/transactional write path as the parent change.

3. Validation rules
- `ON DELETE SET NULL` and `ON UPDATE SET NULL` are rejected for `NOT NULL` child FK columns in v0.

4. Persistence
- Persist FK action metadata with column metadata in table catalog records.
- Backward compatible for older catalogs that do not contain action metadata.
- No WAL/page/checkpoint/recovery semantic changes.

### Rationale

- Roadmap section 5.5 requires richer FK actions after CHECK constraints stabilize.
- `ON DELETE` and `ON UPDATE` actions unlock common lifecycle workflows with moderate implementation risk.

### Alternatives Considered

1. Allow `SET NULL` on `NOT NULL` columns and fail at runtime
- Rejected for v0 simplicity and earlier error detection.

2. Keep FK action metadata out of catalog and infer defaults only
- Rejected; actions must persist durably to preserve semantics after reopen.

### Trade-offs

- Cascading deletes/updates can increase write amplification for parent deletes.
- DDL-time rejection for `SET NULL` + `NOT NULL` is stricter than some engines.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.5)
- Existing FK baseline: `design/SPEC.md` (Section 7.2)
