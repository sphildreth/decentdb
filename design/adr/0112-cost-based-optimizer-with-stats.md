# Cost-Based Optimizer with Persisted Statistics
**Date:** 2026-02-25
**Status:** Accepted

**Supersedes:** ADR-0038 (Cost-Based Optimization and Full Statistics — Post-1.0 Deferral)
**Updates:** ADR-0013 (Index Statistics Strategy)

---

## Decision

Implement a cost-based query optimizer with persisted table/index statistics, incremental row-count maintenance, selectivity estimation, cost-based index selection, and inner-join reordering.

This supersedes ADR-0038 ("deferred post-1.0") and updates ADR-0013 ("heuristics only").

---

## 1. Catalog Stats Persistence

### 1.1 New record types

Two new catalog record types are introduced alongside the existing `table`, `index`, `view`, and `trigger` types. They are stored in the same B+Tree catalog under deterministic CRC32C keys:

| Logical key (string used for CRC32C hash) | Record type tag | Fields |
|---|---|---|
| `stats:table:<normalizedName>` | `"stats:table"` | rowCount (int64) |
| `stats:index:<normalizedName>` | `"stats:index"` | entryCount (int64), distinctKeyCount (int64) |

`normalizedName` = `toLowerAscii()` of the table/index name, matching all other catalog key conventions.

### 1.2 Encoded format

Records use the existing `encodeRecord` / `decodeRecord` machinery from `record.rs`. Fields are positional:

**`stats:table`** (3 fields):
1. `vkText` — `"stats:table"`
2. `vkText` — normalized table name
3. `vkInt64` — rowCount

**`stats:index`** (4 fields):
1. `vkText` — `"stats:index"`
2. `vkText` — normalized index name
3. `vkInt64` — entryCount
4. `vkInt64` — distinctKeyCount

### 1.3 Backward compatibility

- A database that has never had `ANALYZE` run contains no `stats:table` or `stats:index` records; all planning falls back to heuristics. No migration is needed.
- Opening a new version of DecentDB against an old database file is safe: unknown catalog record type tags produce an `ERR_CORRUPTION` error from `parseCatalogRecord` only if explicitly read; the catalog loader skips unknown record kinds silently (they are treated as unknown and ignored).
- Stats records are never required for correctness; they are advisory only.

### 1.4 `parseCatalogRecord` extension

`parseCatalogRecord` is extended with two new arms:

```rust
if recordType == "stats:table":
  # fields: [tag, name, rowCount]
  ...
if recordType == "stats:index":
  # fields: [tag, name, entryCount, distinctKeyCount]
  ...
```

Unknown record types are silently skipped (no error) so that future additions do not break older builds opening newer databases.

### 1.5 In-memory caches

`Catalog` gains two new fields:

```rust
tableStats*: Table[string, TableStats]
indexStats*: Table[string, IndexStats]
```

These are populated at open time from `parseCatalogRecord` and updated by `ANALYZE` / incremental maintenance.

---

## 2. `ANALYZE` Statement

### 2.1 SQL syntax

Mirustum supported forms:

```sql
ANALYZE tableName   -- analyze one table and all its indexes
ANALYZE             -- analyze all user tables and their indexes
```

`ANALYZE` is not supported inside an explicit transaction (returns an error if an explicit transaction is active). It runs as an implicit auto-commit operation.

### 2.2 Parser

libpg_query parses `ANALYZE tableName` as a `VacuumStmt` JSON node with `is_vacuumcmd = false`. A `parseVacuumStmt` proc is added to `sql.rs` that detects this and emits `skAnalyze`. Bare `ANALYZE` (no table) is also supported.

### 2.3 New `StatementKind`

```rust
skAnalyze
```

Statement fields: `analyzeTable: string` (empty = all tables).

### 2.4 Execution

`ANALYZE` execution:
1. For each target table: scan the B+Tree and count rows.
2. For each B+Tree index on the table: scan the index and count entries and distinct first-column keys.
3. Persist `TableStats` and `IndexStats` via `saveTableStats` / `saveIndexStats`.
4. Returns no rows (empty result).

Stats computation is O(n) in table/index size. No background work; `ANALYZE` is synchronous and blocking.

---

## 3. Incremental Row-Count Maintenance

### 3.1 Per-transaction deltas

The `Catalog` gains a `rowCountDeltas: Table[string, int64]` field (normalized table name → delta). DML execution in `exec.rs` increments or decrements this per committed row change.

### 3.2 Commit semantics

On commit, the engine applies all non-zero deltas: for each table with a delta, if `tableStats[t]` exists, update `rowCount += delta` and persist. If no stats record exists yet, the delta is silently discarded (stats are never auto-created; only `ANALYZE` creates them).

### 3.3 Rollback semantics

On rollback, `rowCountDeltas` is cleared without applying them. This is safe because the WAL is also rolled back.

### 3.4 Constraint

Row-count deltas are applied at the in-memory level immediately on commit. Persistence (catalog B+Tree update) happens as part of the normal commit write path.

---

## 4. Cost Model

### 4.1 Plan node annotation

`Plan` gains two fields:

```rust
estRows*: int64    # estimated output cardinality (0 = unknown / not estimated)
estCost*: float64  # estimated relative cost (0.0 = not estimated)
```

These are filled in by a new `annotatePlan` pass called from `plan*` after the structural plan is built.

### 4.2 Operator cost formulas

All costs are in arbitrary "page units" relative to a full table scan = `rowCount / rowsPerPage` where `rowsPerPage = 100` (constant).

| Operator | estRows | estCost |
|---|---|---|
| TableScan | tableStats.rowCount (or 1000 heuristic) | ceil(estRows / 100) |
| RowidSeek | 1 | 1 + log2(estRows_of_table) |
| IndexSeek | `tableStats.rowCount * selectivity` | `distinctKeyCount == 0 ? 1 : ceil(estRows_parent / distinctKeyCount)` + filter cost |
| TrigramSeek | heuristic 10 | 10 |
| Filter | `left.estRows * selectivity` | left.estCost |
| Join (inner) | `left.estRows * right.estRows * joinSelectivity` | left.estCost + left.estRows * right.estCost |
| Sort | `left.estRows` | left.estCost + left.estRows * log2(max(left.estRows,2)) / 100 |
| Aggregate | groupBy count estimate | left.estCost |
| Limit | min(limit, left.estRows) | min(limit, left.estRows) / max(left.estRows, 1) * left.estCost |

### 4.3 Selectivity estimation

| Predicate | With stats | Without stats (heuristic) |
|---|---|---|
| `col = val` (B+Tree index) | `1 / max(distinctKeyCount, 1)` | 0.10 |
| `col = val` (no index) | 0.10 | 0.10 |
| Range `col > val` | 0.30 | 0.30 |
| `LIKE '%pat%'` (trigram) | 0.05 | 0.05 |
| AND of two predicates | product of selectivities | product |
| OR of two predicates | sel_A + sel_B − sel_A*sel_B | sum capped at 1.0 |

When stats are missing, stable heuristics are used so plan choices are deterministic.

### 4.4 Fallback behavior

If `tableStats` is missing for a table, use `rowCount = 1000` as the heuristic base. This matches pre-existing behaviour and ensures planning always produces a complete plan.

---

## 5. Cost-Based Index Selection

When both `TableScan` and `IndexSeek` are candidates for the access path on the primary table:

- Compute `scanCost = ceil(tableRowCount / 100)`
- Compute `seekCost = 1 + log2(max(tableRowCount, 2)) * selectivity * tableRowCount / 100`
- Choose `IndexSeek` only if `seekCost < scanCost`

When stats are absent, the existing rule-based preference for index seek is preserved.

---

## 6. Join Reordering

### 6.1 Scope

Only **inner joins** are reordered. `LEFT JOIN` / `FULL OUTER JOIN` chains are never reordered (correctness requirement).

### 6.2 Algorithm

- For N ≤ 6 inner joins: exhaustive left-deep DP (Selinger-style). Enumerate all orderings of the N join tables; for each ordering compute total estimated join cost and pick the mirustum.
- For N > 6: greedy heuristic — order tables by estimated cardinality ascending (smallest-first).

### 6.3 Implementation location

Added to `planSelect` in `planner.rs` after all inner join tables are collected, before building the join tree.

### 6.4 Constraint preservation

`LEFT JOIN` tables are placed in their original SQL order after all reordered inner-join tables. This is a conservative safe approximation.

---

## 7. `EXPLAIN ANALYZE` Output Changes

`explainPlanLines` annotates each node with `estRows` when the plan has been annotated (non-zero). Output line format change:

```
TableScan(table=orders alias= estRows=50000)
```

`explainAnalyzePlanLines` adds a per-root comparison:

```
---
Estimated Rows: 1200
Actual Rows: 1187
Actual Time: 3.241 ms
```

---

## 8. Validation Plan

- Unit tests: `tests/rust/test_analyze_stats.rs`
  - ANALYZE parse/bind/execute
  - Stats persisted and reloaded after DB reopen
  - Empty table → rowCount=0
  - Re-ANALYZE after inserts → updated rowCount
  - Incremental maintenance: INSERT commits increase count; rollback does not
  - Index seek chosen over scan for selective predicate when stats exist
  - Join reorder changes plan for inner joins
- Differential tests vs PostgreSQL: existing harness in `tests/harness/`
- Benchmarks: existing benchmarks in `benchmarks/`

---

## 9. References

- ADR-0013: Index Statistics Strategy (now superseded by incremental maintenance)
- ADR-0038: Cost-Based Optimization Deferred (now superseded)
- ADR-0050: EXPLAIN Statement
- ADR-0023: Isolation Level Specification
- Issue #24: Cost-based optimizer + statistics
