# Outstanding Performance Issues

**Date:** 2026-02-04  
**Status:** Analysis Complete, Implementation Pending

## Summary

After several optimization iterations, DecentDB has achieved parity with SQLite on **commit latency** and **insert throughput**, but two metrics remain outside the 3× target:

| Metric | DecentDB | SQLite | Ratio | Target | Status |
|--------|----------|--------|-------|--------|--------|
| commit_p95_ms | 3.03 | 3.01 | **1.01×** | ≤3× | ✅ Met |
| insert_rows_per_sec | 298 | 448 | **1.50×** | ≤3× | ✅ Met |
| join_p95_ms | 2.44 | 0.37 | **6.6×** | ≤3× | ❌ Gap |
| read_p95_ms | 0.064 | 0.002 | **32×** | ≤3× | ❌ Gap |

This document analyzes the root causes and proposes solutions for the two outstanding gaps.

---

## Issue #1: Point Read Latency (32× gap)

### Current State

- **DecentDB**: ~64 µs per point read (p95)
- **SQLite**: ~2 µs per point read (p95)
- **Gap**: 32×, target is ≤3×

### Root Cause Analysis

The gap is caused by an **API-level mismatch** between how the benchmarks exercise each database:

| Operation | SQLite | DecentDB |
|-----------|--------|----------|
| Parse SQL | Once (prepare) | Cached, but cache lookup overhead |
| Bind parameters | `sqlite3_bind_*` (~10 ns) | `Value` construction (~100 ns) |
| Execute | `sqlite3_step` (~1.5 µs) | `execSql` full path (~60 µs) |
| Result access | `sqlite3_column_*` (direct) | `seq[string]` allocation |

#### Breakdown of DecentDB `execSql` overhead per call:

1. **Cache lookup** (~2 µs): Hash table lookup + LRU touch
2. **WAL transaction begin** (~5 µs): Lock acquire, reader info setup
3. **Plan execution** (~10 µs): B-tree traversal, record decode
4. **Result formatting** (~40 µs): `valueToString` for each column, `seq[string]` construction
5. **WAL transaction end** (~3 µs): Lock release, reader cleanup

The actual B-tree lookup is only ~10 µs. The remaining ~54 µs is API overhead.

### Proposed Solutions

#### Option A: Add Prepared Statement API (Recommended)

Add a `PreparedStatement` type that separates preparation from execution:

```nim
# New API surface
type PreparedStatement* = ref object
  db: Db
  plan: Plan
  statement: Statement
  paramCount: int

proc prepare*(db: Db, sql: string): Result[PreparedStatement]
proc execute*(stmt: PreparedStatement, params: seq[Value]): Result[seq[Row]]
proc executeScalar*(stmt: PreparedStatement, params: seq[Value]): Result[Value]
proc finalize*(stmt: PreparedStatement)
```

**Benefits:**
- Skips SQL cache lookup per call
- Reuses parsed statement and plan directly
- Returns `seq[Row]` instead of `seq[string]` (avoids formatting)
- Estimated improvement: 32× → ~5-8×

**ADR Required:** Yes — new public API surface with lifetime semantics.

#### Option B: Add "Raw Row" Result Mode

Keep `execSql` but add an option to return raw `Row` objects instead of formatted strings:

```nim
proc execSqlRows*(db: Db, sql: string, params: seq[Value]): Result[seq[Row]]
```

**Benefits:**
- Avoids `valueToString` overhead (~40 µs saved)
- No new lifetime semantics
- Estimated improvement: 32× → ~8-10×

**ADR Required:** Probably not — minor API addition, no format/durability impact.

#### Option C: Optimize `valueToString` and Cache Lookup

Further micro-optimize the existing path:
- Pre-size string buffers
- Use faster integer-to-string conversion
- Inline cache lookup hot path

**Benefits:**
- No API changes
- Estimated improvement: 32× → ~20-25×

**ADR Required:** No

### Recommendation

**Start with Option B** (add `execSqlRows`), then evaluate if Option A is needed. Option B provides significant improvement without ADR overhead.

---

## Issue #2: Join Latency (6.6× gap)

### Current State

- **DecentDB**: ~2.4 ms per join query (p95)
- **SQLite**: ~0.37 ms per join query (p95)
- **Gap**: 6.6×, target is ≤3×

### Benchmark Query

```sql
SELECT u.name, SUM(o.amount) 
FROM users u 
INNER JOIN orders o ON u.id = o.user_id 
GROUP BY u.id, u.name
```

- 100 users, 1000 orders
- Result: 100 rows (one per user with aggregated sum)

### Root Cause Analysis

#### 1. Aggregation Key Allocation (~1.5 ms overhead)

In `aggregateRows()` (exec.nim:1455-1538), for each of the 1000 joined rows:

```nim
for row in rows:
  for expr in groupBy:
    keyParts.add(valueToString(evalRes.value))  # String alloc per group column
  let key = keyParts.join("|")                   # String concat alloc
```

With 2 GROUP BY columns and 1000 rows = **3000 string allocations** per query.

#### 2. Row Materialization (~0.8 ms overhead)

In the join loop (exec.nim:2138-2152), for each matched pair:

```nim
let merged = Row(columns: cols, values: lrow.values & rrow.values)  # Seq concat
resultRows.add(merged)
```

With 1000 matches = **1000 seq concatenations** (values) + 1000 Row objects.

#### 3. No Early Termination

Even with LIMIT, the join materializes all matches before applying the limit:
- Join produces 1000 rows
- Aggregation reduces to 100 rows
- LIMIT would apply after (benchmark doesn't use LIMIT)

### Proposed Solutions

#### Option A: Optimize Group Key Construction (No ADR)

Replace string-based group keys with integer-based hashing:

```nim
# Current: string key
let key = keyParts.join("|")

# Proposed: hash-based key
var key: Hash = 0
for expr in groupBy:
  let evalRes = evalExpr(row, expr, params)
  key = key !& hash(evalRes.value)  # Nim's hash combining
key = !$key
```

**Benefits:**
- Eliminates `valueToString` and `join` allocations
- O(1) hash computation vs O(n) string building
- Estimated improvement: 6.6× → ~4×

**ADR Required:** No

#### Option B: Pre-allocate Merged Row Storage

Reuse a single `Row` buffer instead of allocating per match:

```nim
var mergedBuffer = Row(columns: mergedColumns, values: newSeq[Value](leftCols + rightCols))

for lrow in leftRes.value:
  for rrow in rightRows:
    # Copy into buffer instead of concatenating
    for i, v in lrow.values: mergedBuffer.values[i] = v
    for i, v in rrow.values: mergedBuffer.values[leftCols + i] = v
    # Process immediately instead of collecting
```

**Benefits:**
- Eliminates `lrow.values & rrow.values` allocation per match
- Can enable streaming aggregation
- Estimated improvement: additional ~20%

**ADR Required:** No

#### Option C: Streaming Aggregation (Reduces Peak Memory)

Instead of:
1. Join → collect all 1000 rows
2. Aggregate → reduce to 100 rows

Do:
1. Join → stream rows directly to aggregator
2. Aggregator maintains running state
3. Never materialize intermediate 1000 rows

**Benefits:**
- O(groups) memory instead of O(matches)
- Enables early termination for LIMIT after GROUP BY
- Estimated improvement: 6.6× → ~3-4×

**ADR Required:** Possibly — changes execution model for joins

#### Option D: Index-Driven Aggregation

For this specific pattern (GROUP BY primary key with SUM), SQLite likely uses an optimized path:
- Scan orders table once
- For each order, increment sum in hash table keyed by user_id
- No explicit join needed

This requires query plan optimization, not execution optimization.

**ADR Required:** Yes — cost-based optimizer changes (deferred per ADR-0038)

### Recommendation

**Implement Options A + B together** (hash keys + buffer reuse). These are low-risk, no-ADR changes that should bring the gap from 6.6× to ~3-4×.

If still above 3×, evaluate Option C (streaming aggregation).

---

## Implementation Plan

### Phase 1: Quick Wins (No ADR Required)

- [ ] **P1.1**: Add `execSqlRows` API returning `seq[Row]` instead of `seq[string]`
- [ ] **P1.2**: Replace string group keys with hash-based keys in `aggregateRows`
- [ ] **P1.3**: Pre-allocate merged row buffer in join loop
- [ ] **P1.4**: Benchmark and measure improvement

**Expected outcome:** Read 32× → ~10×, Join 6.6× → ~3-4×

### Phase 2: API Enhancement (ADR Required)

- [ ] **P2.1**: Draft ADR for Prepared Statement API
- [ ] **P2.2**: Implement `PreparedStatement` type
- [ ] **P2.3**: Add prepare/execute/finalize methods
- [ ] **P2.4**: Update benchmarks to use prepared statements
- [ ] **P2.5**: Benchmark and measure improvement

**Expected outcome:** Read ~10× → ~3×

### Phase 3: Execution Model (ADR May Be Required)

- [ ] **P3.1**: Evaluate streaming aggregation feasibility
- [ ] **P3.2**: Draft ADR if execution model changes needed
- [ ] **P3.3**: Implement streaming join→aggregate pipeline
- [ ] **P3.4**: Benchmark and measure improvement

**Expected outcome:** Join ~4× → ≤3×

---

## Verification Criteria

After each phase, run:

```bash
nimble bench_embedded
./build/run_benchmarks /tmp/bench_out --engines=decentdb,sqlite --data-dir=/path/to/real/disk
python3 benchmarks/embedded_compare/scripts/aggregate_benchmarks.py \
  --input /tmp/bench_out \
  --output benchmarks/embedded_compare/data/bench_summary.json
```

Target metrics:
- `read_p95_ms`: DecentDB ≤ 3× SQLite
- `join_p95_ms`: DecentDB ≤ 3× SQLite

---

## References

- ADR-0014: Performance Targets
- ADR-0037: Group Commit (deferred)
- ADR-0038: Cost-Based Optimization (deferred)
- `src/engine.nim`: `execSql` implementation
- `src/exec/exec.nim`: Join and aggregation execution
- `benchmarks/embedded_compare/run_benchmarks.nim`: Benchmark implementation
