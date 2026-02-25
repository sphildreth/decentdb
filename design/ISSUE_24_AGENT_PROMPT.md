# Coding Agent Prompt: Close Issue #24 (Cost-based optimizer + statistics)

**Target issue:** https://github.com/sphildreth/decentdb/issues/24

## Mission
Implement a **cost-based query optimizer with persisted statistics** so that DecentDB chooses predictable, efficient plans for joins and selective predicates based on real data distribution.

This work should satisfy the Issue #24 acceptance criteria:
- `ANALYZE` computes **per-table row counts** and **per-index cardinality**, stored in the catalog.
- Row counts are maintained incrementally on `INSERT`/`UPDATE`/`DELETE` (exact or approximate).
- Planner has a **cost model** (per operator).
- Planner performs **selectivity estimation** for `WHERE` predicates (equality, range, `LIKE`).
- **Join reordering**: evaluate multiple join orderings and select lowest-cost plan (exhaustive for ≤ 6 tables; heuristic fallback above).
- **Index selection**: cost-based choice between scan vs seek.
- `EXPLAIN ANALYZE` shows **estimated vs actual** row counts for plan validation.
- **Backward compatibility**: no format breakage for users who never run `ANALYZE`.
- All existing tests pass; differential tests vs PostgreSQL continue to match.
- Write benchmarks show no regression; read benchmarks improve for multi-join queries.

## Non-negotiable repo constraints
- Follow `AGENTS.md` DoD and the repo’s “smallest diff” philosophy.
- **Persistent format impact is expected** (catalog stats). Per `AGENTS.md` and `.github/copilot-instructions.md`, you **must** address this via ADR work before implementation.
- Avoid new dependencies. If you believe you need one (e.g., HLL), stop and propose an ADR + justification.

## Phase 0 — Architecture Decision Record (ADR) updates (required before coding)
This repo already contains an ADR deferring cost-based optimization:
- `design/adr/0038-cost-based-optimization-deferred.md`

And an ADR that commits to heuristic-only stats for 0.x:
- `design/adr/0013-index-statistics-strategy.md`

**Task 0.1 (required):** Draft the “go-forward” ADR for implementing Issue #24.
- Preferred approach: **Supersede** ADR-0038 and update ADR-0013 accordingly.
  - Either:
    - Create a new ADR (next available number) like `design/adr/NNNN-cost-based-optimizer-with-stats.md` that **supersedes** ADR-0038, or
    - Replace ADR-0038’s “deferral” with an implementation-ready design and update its status.
- The ADR must cover:
  - Catalog persistence design for stats (record types/keys, schema, encoding).
  - Format/version strategy and backward compatibility rules.
  - Transaction semantics for incremental stats maintenance (commit/rollback behavior).
  - Cost model formulas + selectivity estimation rules + fallback behavior when stats missing.
  - Join reordering algorithm and thresholds.
  - `EXPLAIN ANALYZE` output format changes.
  - Validation plan: unit + differential + benchmarks.

**Stop point:** Do not proceed to implementation until the ADR design is reviewed/approved.

## Phase 1 — Statistics storage + `ANALYZE`
### Goal
Introduce a minimal stats subsystem that is persisted, backward compatible, and safe.

### Design constraints (implementation expectations)
- **Do not break existing databases that never run `ANALYZE`.**
  - Practical requirement: the DB file format may evolve, but opening a pre-existing DB and running queries must continue to work.
  - If a user never runs `ANALYZE`, no new catalog records should be created.
- Prefer adding **new catalog record types/keys** over rewriting existing table/index records.

### Suggested storage model (you may adjust, but document it in the ADR)
- Add new catalog record types handled by `parseCatalogRecord()` in:
  - `src/catalog/catalog.nim`
- Suggested keys:
  - `stats:table:<normalizedTableName>`
  - `stats:index:<normalizedIndexName>`
- Suggested payloads:
  - `table_stats`: rowCount (int64), optional lastAnalyzeTxn / lastAnalyzeEpoch
  - `index_stats`: entryCount (int64), distinctKeyCount (int64) (define what “cardinality” means; prefer “distinct full index key count”)

### Parser/Binder/Engine wiring
Add a new SQL statement kind and execution path:
- `src/sql/sql.nim`: extend `StatementKind` (e.g., `skAnalyze`) and parse `ANALYZE`.
  - Supported forms (minimum): `ANALYZE <tableName>` and `ANALYZE` (all tables).
  - If you support index-specific analyze, document it.
- `src/sql/binder.nim`: bind `ANALYZE` and validate table/index existence.
- `src/engine.nim`: implement execution:
  - Scan target tables to compute row counts.
  - For each index on analyzed tables, compute cardinality.
  - Persist stats via catalog update APIs.

### Implementation touch points
- `src/catalog/catalog.nim`
  - Add in-memory caches (tableStats/indexStats) and public getters.
  - Add persistence read/write support (new record types).
- `src/sql/sql.nim`, `src/sql/binder.nim`, `src/engine.nim`

### Tests (required)
Add Nim unit tests validating:
- `ANALYZE` parsing and binding.
- Stats are persisted and reloaded on reopen.
- `ANALYZE` on empty table yields rowCount=0.
- Stats for indexes update when new rows inserted and re-`ANALYZE` is run.

Suggested test file(s):
- `tests/nim/test_analyze_stats.nim`

## Phase 1.5 — Incremental row-count maintenance (DML)
### Goal
Keep table row counts reasonably fresh without requiring frequent `ANALYZE`.

### Requirements
- Update row counts during `INSERT`/`DELETE` and potentially `UPDATE` (only if UPDATE changes whether a row qualifies for a partial index; table row count usually unchanged).
- Must be transactionally correct:
  - Rolled-back statements/transactions must not permanently change stats.
  - Prefer storing per-transaction deltas and applying at commit.

### Likely touch points
- `src/exec/exec.nim` (DML execution)
- `src/engine.nim` (transaction commit/rollback hooks)
- `src/catalog/catalog.nim` (apply deltas/persist)

### Tests (required)
- Row count changes after committed INSERT/DELETE.
- Row count does **not** change after rollback.

## Phase 2 — Cost model + selectivity estimation
### Goal
Attach estimated cardinalities to plans and compute comparable operator costs.

### Planner changes
- `src/planner/planner.nim`
  - Add estimation fields to `Plan` (e.g., `estRows`, `estCost`) or attach a side table keyed by node identity.
  - Implement cost functions for each operator (at least scans, seeks, filter, join, sort, aggregate, limit).
  - Implement selectivity estimation:
    - Equality: use distinct counts where available; fallback heuristic when unknown.
    - Range: min/max if you collect them; otherwise heuristic.
    - LIKE: for trigram-backed patterns, use postings count if available; otherwise heuristic.
  - When stats are missing, use stable heuristics so planning remains deterministic.

### EXPLAIN ANALYZE output
- `src/planner/explain.nim`
  - Extend output to show **estimated vs actual rows**.
  - Minimum acceptable: show root estimated rows vs overall actual rows.
  - Preferred: show estimates per operator and actual per operator (requires executor instrumentation).

### Tests (required)
- Unit tests for selectivity estimation behavior (with and without stats).
- A small end-to-end test where `ANALYZE` changes the chosen plan (index seek vs scan).

## Phase 3 — Cost-based index selection
### Goal
Choose between `TableScan` and `IndexSeek`/`TrigramSeek` based on estimated cost.

### Requirements
- Correctness must be identical; only plan choice changes.
- Ensure partial indexes and expression indexes remain correct.

### Tests (required)
- Queries with highly selective predicate pick seek.
- Queries with low selectivity pick scan.

## Phase 4 — Join reordering (≤ 6 tables exhaustive)
### Goal
Implement Selinger-style DP for left-deep join trees (or document if bushy) up to N tables, with heuristic fallback.

### Requirements
- Exhaustive for N ≤ 6 (configurable constant is fine).
- Heuristic fallback for N > 6 (e.g., greedy by estimated output size).
- Must preserve join semantics:
  - Respect `LEFT JOIN` ordering constraints (cannot reorder arbitrarily across outer joins).
  - For now, it’s acceptable to restrict reordering to **inner joins only**; document this explicitly and keep behavior correct.

### Tests (required)
- Multi-table inner join where best order differs from SQL order.
- Ensure left joins are not illegally reordered.

## Phase 5 — Validation: differential + benchmarks
### Differential
Run and keep passing the repo’s differential tests vs PostgreSQL for the supported subset.
- Identify the existing harness entry point(s) in `tests/harness/` and/or `scripts/` and include the exact commands in your PR description.

### Benchmarks
- Confirm no write regression from incremental stats maintenance.
- Show read improvement for at least one multi-join query.
- Use existing benchmarks under `benchmarks/` and/or `python_embedded_compare/`.

## Deliverables checklist (Definition of Done)
- ADR(s) updated/added and linked, covering persistent format and behavior changes.
- `ANALYZE` implemented end-to-end with persisted stats.
- Incremental row-count maintenance implemented with commit/rollback correctness.
- Cost model + selectivity estimation implemented with stable fallbacks.
- Index selection improved via costs.
- Join reordering implemented for inner joins (≤ 6 tables exhaustive).
- `EXPLAIN ANALYZE` shows estimated vs actual rows.
- Unit tests added for new behavior + key edge cases.
- Differential tests vs PostgreSQL still pass.
- Benchmarks demonstrate no write regression and at least one read improvement.

## Notes / known doc mismatches
- `design/PRD.md` currently lists “Advanced query optimizer (cost-based, statistics-driven)” as a 1.0 non-goal; if this work is landing now, update docs (PRD/SPEC) to reflect current scope and avoid conflicting statements.
