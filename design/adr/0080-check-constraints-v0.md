## CHECK Constraints (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement v0 `CHECK` constraints with this scoped behavior:

1. Surface
- Support `CHECK (...)` in `CREATE TABLE` for both:
  - column constraints (`col type CHECK (...)`)
  - table constraints (`CHECK (...)`)
- Constraint names are accepted from parser metadata but not exposed as a durable user-facing contract in v0.

2. Enforcement semantics
- Enforce on every row write for `INSERT` and `UPDATE` (including `INSERT ... ON CONFLICT DO UPDATE`).
- A CHECK expression **fails only when it evaluates to `FALSE`**.
- `TRUE` and `NULL` both pass (SQL-standard CHECK semantics).

3. Expression scope
- CHECK expressions must bind only to columns of the same table.
- Parameters are not allowed.
- Subquery-backed/engine-context-dependent forms (notably `EXISTS`) are rejected for v0.
- Aggregate functions in CHECK are rejected.
- Supported scalar expression behavior is the existing engine evaluator surface.

4. Catalog/persistence
- Persist CHECK metadata in table catalog records as part of `TableMeta` in a backward-compatible extension.
- Existing databases without CHECK metadata continue to open unchanged.
- No WAL/page/checkpoint format changes.

5. Deferred
- `ALTER TABLE ... ADD CONSTRAINT CHECK`
- DEFERRABLE/INITIALLY DEFERRED CHECK
- `NOT VALID` / `VALIDATE CONSTRAINT`
- Cross-row or cross-table assertions

### Rationale

- Roadmap section 5.5 requires CHECK first, before richer FK actions.
- CHECK is high-value and mostly query-layer/catalog metadata work.
- Using SQL-standard pass/fail (`FALSE` only fails) matches PostgreSQL behavior and expected application semantics.
- Storing constraints in table metadata keeps recovery semantics simple and consistent with existing DDL persistence flow.

### Alternatives Considered

1. Reject `NULL` in CHECK as failure
- Rejected; diverges from SQL-standard and PostgreSQL behavior.

2. Enforce only at statement end or transaction commit
- Rejected for v0; current engine enforces constraints at statement-time and this keeps behavior consistent.

3. Store CHECK as separate catalog record type
- Rejected for v0 simplicity; extending table metadata record is smaller and sufficient.

4. Permit subqueries/EXISTS inside CHECK
- Rejected for v0 due execution-context complexity and higher correctness risk.

### Trade-offs

- Runtime evaluation adds per-row write cost.
- v0 intentionally limits CHECK expression surface to reduce semantic risk.
- Extending table metadata increases catalog payload size modestly.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.5)
- Spec baseline: `design/SPEC.md`
- Existing expression semantics ADRs: `design/adr/0071-sql-null-three-valued-logic.md`, `design/adr/0072-sql-cast-coercion-and-failure-semantics.md`, `design/adr/0073-like-escape-and-null-semantics.md`
