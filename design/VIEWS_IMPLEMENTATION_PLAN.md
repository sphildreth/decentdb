# DecentDB Views Implementation Plan
**Date:** 2026-02-06  
**Status:** Draft (v0.1)

## 1. Purpose
Define the initial requirements and implementation plan for SQL views in DecentDB, using SQLite and DuckDB as behavioral baselines while preserving DecentDB priorities:
- Priority #1: durable ACID writes
- Priority #2: fast reads
- Single-process model: one writer, many concurrent readers

This document is intentionally scoped to 0.x (pre-1.0) and focuses on non-materialized, read-only views.

## 2. Goals and Non-Goals
### 2.1 Goals
1. Add practical view support for common application patterns.
2. Align with familiar behavior from SQLite and DuckDB where feasible.
3. Keep existing benchmark metrics stable (no material regressions).
4. Keep implementation explicit and testable (unit + crash + differential).

### 2.2 Non-Goals (initial release)
1. Materialized views.
2. Updatable views (`INSERT/UPDATE/DELETE` against views).
3. `INSTEAD OF` triggers.
4. Multi-schema authorization/security semantics (`SECURITY DEFINER`, etc.).
5. Cost-based view rewrite/optimization beyond current rule-based planning.

## 3. Baseline: SQLite and DuckDB View Capabilities
The table below is based on local CLI verification (`sqlite3`, `duckdb`) and official engine documentation.

| Capability | SQLite | DuckDB | Notes for DecentDB |
|---|---|---|---|
| `CREATE VIEW name AS SELECT ...` | Yes | Yes | Required |
| `CREATE VIEW name(col1, ...) AS ...` | Yes | Yes | Required |
| `CREATE VIEW IF NOT EXISTS ...` | Yes | Yes | Required |
| `CREATE OR REPLACE VIEW ...` | No | Yes | Required in DecentDB for ergonomic schema migration |
| `CREATE TEMP/TEMPORARY VIEW ...` | Yes | Yes | Deferred (see scope) |
| `DROP VIEW name` | Yes | Yes | Required |
| `DROP VIEW IF EXISTS ...` | Yes | Yes | Required |
| `ALTER VIEW ...` | No direct `ALTER VIEW` syntax | Supports rename (`ALTER VIEW ... RENAME TO ...`) | Required (rename-only) |
| View storage model | Non-materialized | Non-materialized | Required |
| DML through views | Not directly updatable | Not directly updatable | Disallow (read-only) |
| Dependency strictness on `DROP TABLE` | Not strict | Not strict | DecentDB will be strict (`RESTRICT`) in 0.x |

## 4. DecentDB Initial Scope (0.x)
### 4.1 SQL Surface (in scope)
1. `CREATE VIEW [IF NOT EXISTS] view_name [(column_list)] AS <select>`
2. `CREATE OR REPLACE VIEW view_name [(column_list)] AS <select>`
3. `DROP VIEW [IF EXISTS] view_name`
4. `ALTER VIEW view_name RENAME TO new_view_name`
5. `SELECT` from views anywhere a table name is currently accepted (`FROM`, `JOIN`).

### 4.2 SQL Surface (out of scope for first release)
1. `TEMP` / `TEMPORARY` views (requires connection-local catalog semantics).
2. Recursive views (`CREATE RECURSIVE VIEW`-style semantics).

### 4.3 Semantic Requirements
1. Views are logical only; no persisted row storage.
2. Views are read-only; write statements targeting a view return `ERR_SQL`.
3. `CREATE VIEW` validates referenced objects at create time.
4. Replacement (`CREATE OR REPLACE`) is atomic within a transaction.
5. Circular view references are rejected.
6. Maximum view expansion depth is capped (default: 16) to prevent pathological plans.
7. Column naming:
   - If explicit column list exists, it is authoritative.
   - Else derive from select-list aliases/column names.
   - Reject mismatched explicit column counts.
8. `ALTER VIEW ... RENAME TO ...` updates the view identity and dependency graph atomically, with no change to defining SQL.

### 4.4 SQL Dialect Clarification
1. DecentDB view statements follow DecentDB's PostgreSQL-like SQL dialect (same as the rest of the engine SQL surface).
2. SQLite and DuckDB are used as feature/capability baselines, not as the primary dialect authority.
3. Any SQLite- or DuckDB-specific behavior not explicitly listed in this plan is out of scope unless later added to DecentDB SPEC/ADR.

## 5. Performance Guardrails (Must Pass)
Views must not degrade existing performance metrics beyond current thresholds in `tests/bench/thresholds.json`:

1. `point_lookup` p95 increase <= 10%
2. `fk_join` p95 increase <= 20%
3. `substring_search` p95 increase <= 20%
4. `order_by_sort` p95 increase <= 20%
5. `bulk_load` p95 increase <= 15%

Additional view-specific guardrails:
1. `SELECT` via simple view vs equivalent base-table SQL:
   - Warm-cache p95 overhead target: <= 5%
   - First-run (compile/bind) overhead target: <= 15%
2. Queries that do not reference views must have zero additional runtime branching in exec hot loops.

## 6. Architecture and Module Changes
### 6.1 Catalog Layer (`src/catalog/catalog.nim`)
Add `ViewMeta` and in-memory map:
- `name: string`
- `sqlText: string` (canonical defining `SELECT`)
- `columnNames: seq[string]` (resolved output column names)
- `dependencies: seq[string]` (table/view names, normalized)

Add catalog persistence record kind:
- `kind = "view"` with fields for name/sql/columns/dependencies.

Required APIs:
1. `createViewMeta`
2. `saveViewMeta`
3. `dropView`
4. `getView`
5. `listDependentViews(objectName)`

### 6.2 SQL AST + Parser (`src/sql/sql.nim`)
Add statement kinds:
1. `skCreateView`
2. `skDropView`
3. `skAlterView`

Add statement payload fields:
1. View name
2. `ifNotExists`
3. `orReplace`
4. Optional explicit column list
5. Defining `SELECT` statement / serialized SQL text
6. `ifExists` for drop
7. Old and new view names for rename

Parser requirements:
1. Parse `CREATE VIEW` / `CREATE OR REPLACE VIEW`.
2. Parse `DROP VIEW`.
3. Parse `ALTER VIEW ... RENAME TO ...`.
4. Reject unsupported options with clear errors.

### 6.3 Binder (`src/sql/binder.nim`)
Responsibilities:
1. Validate view definitions on create:
   - All referenced tables/views exist.
   - No cycles.
   - Output columns resolvable.
2. Expand view references in read queries.
3. Apply expansion depth limits and node-budget guardrails.
4. Enforce read-only behavior for view targets in DML.

Implementation note:
- Current AST is table-name based for `FROM`/`JOIN`.
- To support general views cleanly, add a derived-source representation in AST/binder output (table or expanded subquery source), rather than brittle string substitution.

### 6.4 Planner (`src/planner/planner.nim`)
1. Plan expanded/derived sources without changing existing access-path selection logic for base tables.
2. Preserve index-seek and trigram-seek opportunities after view expansion.
3. Keep non-view query planning unchanged.

### 6.5 Executor (`src/exec/exec.nim`)
1. Ensure execution operators remain unchanged for non-view plans.
2. If needed for derived sources, add a minimal adapter node that materializes no extra rows unless required by query semantics.

### 6.6 Engine DDL Routing (`src/engine.nim`)
1. Handle `skCreateView` / `skDropView` in write transaction path.
2. Handle `skAlterView` (rename-only) in write transaction path.
3. Ensure rename rejects collisions with existing table/view names.
4. Bump schema cookie on create/replace/drop/rename view.
5. Invalidate SQL plan cache via existing schema-cookie mechanism.
6. Enforce dependency checks on object drops.

## 7. Dependency Policy (DecentDB 0.x)
DecentDB will use stricter dependency semantics than SQLite/DuckDB in initial release:

1. `DROP TABLE` fails if dependent views exist (`RESTRICT` behavior).
2. `DROP VIEW` fails if other views depend on it (unless `IF EXISTS` only suppresses missing-object errors, not dependency errors).
3. `CREATE OR REPLACE VIEW` revalidates and rewrites dependency graph atomically.
4. `ALTER VIEW ... RENAME TO ...` rewrites dependency names atomically.

Rationale:
- Prevent silent schema drift and runtime surprises.
- Cost is DDL-time only; no read-path regression.

## 8. Durability and Compatibility Requirements
1. View metadata changes must be fully transactional via existing WAL flow.
2. Crash during view DDL must recover to pre-transaction or post-commit state (never partial catalog state).
3. Any persistent catalog format extension requires:
   - New ADR before implementation
   - Format version bump decision documented in `design/SPEC.md`
   - Compatibility tests (open pre-view DB; open DB with views using current engine)

## 9. Testing Plan
### 9.1 Unit Tests (Nim)
Add/extend tests for:
1. Parser:
   - `CREATE VIEW`, `CREATE OR REPLACE VIEW`, `DROP VIEW`, `ALTER VIEW ... RENAME TO ...`, flags (`IF EXISTS`, `IF NOT EXISTS`)
2. Binder:
   - valid/invalid dependencies
   - cycle detection
   - column list validation
   - read-only DML rejection
3. Catalog:
   - encode/decode/persist/reload for `ViewMeta`
   - dependency index correctness
4. Planner:
   - view-expanded plans still choose indexes/trigram when eligible
5. Engine DDL:
   - rename conflict detection and dependency rewrite behavior

### 9.2 Property Tests
1. Randomized create/drop/replace view sequences preserve catalog invariants.
2. Expanded query result equivalence:
   - `SELECT ... FROM view` == inlined defining query for supported subset.

### 9.3 Crash-Injection Tests (Python harness + FaultyVFS)
1. Crash between catalog record write and commit marker for `CREATE VIEW`.
2. Crash during `DROP VIEW`.
3. Crash during `CREATE OR REPLACE VIEW`.
4. Crash during `ALTER VIEW ... RENAME TO ...`.
5. Reopen and verify catalog + queryability invariants.

### 9.4 Differential Tests
For shared subset behavior:
1. Compare DecentDB against SQLite and DuckDB on:
   - create/drop/replace where applicable
   - rename semantics (`ALTER VIEW ... RENAME TO ...`) where supported
   - nested view select correctness
   - error behavior for invalid references

## 10. Benchmark Plan
Extend `tests/bench/bench.nim` with view scenarios:
1. `view_point_lookup`: simple projection view over indexed table.
2. `view_join_lookup`: view containing join used by outer filter.
3. `view_like_trigram`: view over trigram-indexed text column.

Measure:
1. cold compile+execute
2. warm cached execution (via existing `sqlCache`)

Acceptance:
1. Existing non-view benchmark thresholds remain green.
2. View scenarios stay within Section 5 targets.

## 11. Rollout Plan
### Phase 1: Metadata + DDL Skeleton
1. Parser + AST support.
2. Catalog `ViewMeta` persistence.
3. Engine create/drop/replace/rename routing.
4. Unit + crash tests for catalog durability.

### Phase 2: Query Expansion
1. Binder expansion for view sources.
2. Cycle/depth/dependency enforcement.
3. Planner compatibility work.
4. Differential tests for result equivalence.

### Phase 3: Performance Hardening
1. SQL cache-aware optimization for expanded views.
2. Benchmark tuning for no-regression goals.
3. Documentation updates (`design/SPEC.md`, SQL reference docs).

## 12. Risks and Mitigations
1. Risk: View expansion reduces index usage.
   - Mitigation: planner tests asserting index/trigram operator selection.
2. Risk: Deep/nested views cause compile-time blowups.
   - Mitigation: depth/node budgets and explicit errors.
3. Risk: Catalog format drift creates compatibility bugs.
   - Mitigation: ADR + compatibility tests + format decision documentation.
4. Risk: Added branching affects non-view hot paths.
   - Mitigation: keep non-view fast path unchanged; benchmark gate.

## 13. ADR Checklist (Required Before Implementation)
Create an ADR before coding to lock:
1. Catalog record format for views (`kind="view"` payload schema).
2. Compatibility/versioning policy for databases that include views.
3. Dependency policy (`RESTRICT` semantics and cycle handling).
4. AST/planner representation choice for derived view sources.

## 14. Deliverables
1. `design/VIEWS_IMPLEMENTATION_PLAN.md` (this document)
2. Follow-up ADR: `design/adr/NNNN-views-catalog-and-semantics.md`
3. SPEC updates after implementation lands

## 15. References
1. SQLite `CREATE VIEW`: https://www.sqlite.org/lang_createview.html
2. SQLite `DROP VIEW`: https://www.sqlite.org/lang_dropview.html
3. DuckDB `CREATE VIEW`: https://duckdb.org/docs/stable/sql/statements/create_view
4. DuckDB `ALTER VIEW`: https://duckdb.org/docs/stable/sql/statements/alter_view.html
5. DuckDB `DROP`: https://duckdb.org/docs/stable/sql/statements/drop
6. Local CLI verification (`sqlite3`, `duckdb`) executed on 2026-02-06.
