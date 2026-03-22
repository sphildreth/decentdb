## AFTER Triggers (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement a constrained trigger surface for 0.x:

1. Supported scope
- `CREATE TRIGGER` and `DROP TRIGGER` for base tables.
- Timing: `AFTER` only.
- Events: `INSERT`, `UPDATE`, `DELETE` (single or `OR` combinations).
- Firing mode: `FOR EACH ROW` only.

2. Trigger action surface (v0)
- PostgreSQL-style trigger syntax is accepted via libpg_query, but action execution is limited to:
  - `EXECUTE FUNCTION decentdb_exec_sql('<sql>')`
- `<sql>` must parse to exactly one DML statement (`INSERT`, `UPDATE`, or `DELETE`) with no parameters.
- `NEW`/`OLD` row references are not supported in v0.
- `INSTEAD OF` triggers are deferred.

3. Execution semantics
- Trigger actions run in the same transaction as the mutating statement.
- AFTER triggers fire once per affected row.
- If any trigger action fails, the parent statement fails and is rolled back under existing transaction semantics.
- Trigger recursion is bounded by a fixed depth limit to prevent infinite loops.

4. Ordering and naming
- Trigger names are unique within a table in v0.
- When multiple triggers match the same event, firing order is deterministic by trigger name (ascending).

5. Durability/persistence
- Trigger metadata is persisted in catalog records as an additive record type.
- No DB header, page layout, WAL frame, or checkpoint semantics changes.

### Rationale

- Roadmap section 5.8 requires shipping AFTER triggers before INSTEAD OF.
- Full PostgreSQL trigger-function infrastructure is out of scope for 0.x; the `decentdb_exec_sql('<sql>')` bridge provides practical trigger behavior with bounded complexity.
- Keeping actions to parameterless DML avoids row-value interpolation semantics (`NEW`/`OLD`) in this slice.

### Alternatives Considered

1. Full PostgreSQL trigger functions (`CREATE FUNCTION ... RETURNS trigger`)
- Rejected for 0.x due major language/runtime surface and security implications.

2. SQLite-style trigger bodies (`BEGIN ... END`)
- Rejected because parser baseline is PostgreSQL AST via libpg_query.

3. Statement-level triggers first
- Rejected for v0 initial scope; per-row behavior aligns better with common trigger expectations.

### Trade-offs

- v0 triggers are intentionally narrower than PostgreSQL.
- Missing `NEW`/`OLD` significantly limits audit/use-case expressiveness.
- Recursive trigger graphs are possible but bounded by depth guard.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.8)
- Parser baseline: `design/adr/0035-libpg-query-parser-adoption.md`
