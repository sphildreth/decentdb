## EXPLAIN Statement (Query Plan Introspection)
**Date:** 2026-02-01
**Status:** Accepted

### Decision

Add SQL support for `EXPLAIN <statement>` as a debug/observability feature.

This ADR defines the exact behavior for the initial version:

- **Supported inner statements:** `SELECT` only.
- **Execution:** `EXPLAIN` **does not execute** the inner statement; it only parses, binds, and plans.
- **Result shape:** `EXPLAIN` returns a result set with:
  - exactly **one TEXT column** named `query_plan`
  - **N rows**, one line per row
- **Output format:** deterministic, stable, ASCII-only plan lines in a simple tree format.
- **Non-goals (explicitly out of scope for this ADR):**
  - `EXPLAIN ANALYZE`
  - `EXPLAIN (options ...)`
  - runtime statistics (timings, row counts)
  - support for non-SELECT statements

### Rationale

- DecentDB needs a built-in way to answer “what plan did the optimizer choose?” to support:
  - diagnosing performance (e.g., scan vs index seek vs trigram seek)
  - correctness debugging (predicate pushdown and join order)
  - client integration (drivers/ORMs commonly rely on EXPLAIN-like behavior for diagnostics)

- The engine already has a concrete physical plan tree (`Plan` in `src/planner/planner.nim`).
  - Exposing it via SQL is low-risk to durability and storage formats because it requires **no changes** to on-disk data.

- Returning a simple result set (single TEXT column) keeps the feature universally usable:
  - CLI can print it without special casing.
  - Python/.NET/Go drivers can return it like any other query.

### Alternatives Considered

1. **CLI-only plan printing flag** (e.g., `decentdb exec --explain`)
   - Pros: no SQL dialect change; can ship quickly.
   - Cons: not accessible through drivers/ORMs; less composable.

2. **SQLite-style `EXPLAIN QUERY PLAN <stmt>` keyword and output**
   - Pros: familiar to SQLite users.
   - Cons: DecentDB is Postgres-like; conflicts with SQLAlchemy/Dapper expectations; implies matching SQLite’s output contract.

3. **Postgres-like `EXPLAIN (FORMAT JSON) ...`**
   - Pros: structured output.
   - Cons: requires designing a stable JSON schema and options parsing; larger API surface.

4. **`EXPLAIN ANALYZE` first**
   - Pros: most useful for deep performance work.
   - Cons: instrumentation design is larger; would require careful work to avoid hot-path overhead.

Chosen approach: (1) but via **SQL**: `EXPLAIN <SELECT>` returning plan lines.

### Trade-offs

- **SELECT-only** keeps scope small and avoids implying we have a complete physical planner for all statements.
- **Text output** is easy to ship but less machine-readable than JSON.
  - This ADR intentionally avoids committing to a JSON schema.
- **Deterministic plan lines** are testable and friendly to tooling, but they may constrain future refactors.
  - Mitigation: keep output minimal (operator + key fields) and avoid printing nondeterministic or internal IDs.

### References

- Implementation playbook: [design/EXPLAIN_IMPLEMENTATION.md](../EXPLAIN_IMPLEMENTATION.md)
- Planner types: `Plan`, `PlanKind` in `src/planner/planner.nim`
- Engine execution entry: `execSql` in `src/engine.nim`
- Repo workflow: [AGENTS.md](../../AGENTS.md)
