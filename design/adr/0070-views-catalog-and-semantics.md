## Views Catalog Record Format and 0.x Semantics
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Add non-materialized, read-only SQL views to DecentDB 0.x with bind-time expansion and strict dependency enforcement.

1. **Persistent catalog record format**
- Add a new catalog record kind: `"view"`.
- Encode a view record as an existing `record` payload with fields:
  - field 0: `"view"`
  - field 1: `view_name`
  - field 2: `sql_text` (canonical defining `SELECT`)
  - field 3: `column_names` as `;`-joined list
  - field 4: `dependencies` as `;`-joined list of normalized object names
- The catalog key remains `crc32c("view:" & view_name)`.
- Dependencies are name-based in 0.x (no object IDs).

2. **Compatibility/versioning**
- **No DB header format bump** for this change.
- Rationale: catalog records are self-describing by `kind` and this is an additive catalog extension.
- Current engine behavior for old DBs: unchanged; DBs without view records open normally.
- Older engines opening DBs containing view records are out of compatibility scope for 0.x.

3. **Dependency semantics (strict, 0.x)**
- `DROP TABLE` is `RESTRICT`: fail if dependent views exist.
- `DROP VIEW` is `RESTRICT`: fail if dependent views exist.
- `CREATE OR REPLACE VIEW` recomputes dependencies atomically and revalidates all transitive dependents before commit.
- If any transitive dependent would become invalid, replacement fails (no silent breakage).
- Cycles are rejected at DDL time.

4. **Bind-time expansion model**
- View expansion happens in binder via AST substitution (not SQL text substitution).
- Planner and executor keep the non-view hot path unchanged.
- Guardrails:
  - max expansion depth = **16**
  - max expanded AST node budget = **10,000**
- Violations return `ERR_SQL` with context.

5. **View definition constraints (0.x)**
- Defining statement must be a pure `SELECT`.
- Parameters are forbidden in view definitions (`$1`, etc.): reject with `ERR_SQL`.
- Views are read-only; `INSERT/UPDATE/DELETE` targeting a view fail with `ERR_SQL`.

6. **Output column naming policy**
- If explicit column list is provided, it is authoritative.
- Otherwise resolve per select item:
  - use explicit select-item alias when present
  - else for bare column refs use underlying column name
  - else use stable synthetic name `colN` (1-based)
- If explicit count mismatches actual output count, return `ERR_SQL` with context `(view_name, expected_count, actual_count)`.
- Duplicate output names are rejected with `ERR_SQL` and context `(view_name, column_name)`.

7. **Namespace + rename semantics**
- Tables and views share one namespace (no collisions).
- `ALTER VIEW ... RENAME TO ...` is rename-only and `RESTRICT`ed if any dependent views exist.
- On allowed rename, update view identity and dependency graph atomically.
- 0.x does not rewrite dependent SQL text during rename.

### Rationale

These decisions keep ACID durability and schema safety as the primary goal:
- view metadata is durable in the existing catalog/WAL transaction path,
- DDL enforces strict dependency correctness,
- read performance for non-view queries remains unchanged because expansion is binder-only.

Name-based dependencies plus `RESTRICT` rename minimize persistent-format complexity for 0.x while preventing silent schema drift.

### Alternatives Considered

- **Object-ID-based dependencies now**: more robust for future rename/refactor workflows, but requires larger persistent-format and migration surface.
- **Plan-time or runtime view expansion**: increases planner/executor complexity and risks hot-path branching overhead.
- **Allow dependent breakage on replace/drop (SQLite-like lax policy)**: simpler DDL, but violates strict-correctness goals.
- **Header format bump for views**: stronger old-engine fail-fast behavior, but larger compatibility churn for this additive 0.x feature.

### Trade-offs

- Name-based dependencies require conservative rename policy (`RESTRICT` with dependents).
- Strict dependent revalidation increases DDL-time cost, but keeps runtime predictable and safe.
- Not bumping header format means old binaries are not guaranteed safe on DBs containing views.

### References

- `design/VIEWS_IMPLEMENTATION_PLAN.md`
- `design/AGENTS.md`
- `design/SPEC.md`
- `design/adr/README.md`
- ADR-0010 (error handling)
- ADR-0023 (isolation)
