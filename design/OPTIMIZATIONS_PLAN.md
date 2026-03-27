# Update Latency Optimization Plan

**Date:** 2026-03-27
**Status:** Analysis complete, implementation pending
**Goal:** Make DecentDB Update p50 and p95 latencies equal to or better than SQLite
in the Python complex benchmark (`bench_complex.py`).

## Current State

DecentDB wins **17 of 19** benchmark metrics. The two remaining losses are:

| Metric     | DecentDB | SQLite  | Ratio        |
|------------|----------|---------|--------------|
| Update p50 | 5.56 µs  | 3.49 µs | 1.60× slower |
| Update p95 | 17.1 µs  | 9.58 µs | 1.79× slower |

Every other metric — including catalog insert, bulk insert, point lookup,
range scan, joins, aggregates, complex report, history joins, deletes, and full
table scans — already beats SQLite, often by 1.5–3×.

## Root Cause Analysis

### Confirmed: L2 Cache Cooling After `fdatasync`

The benchmark measures Update latency as:

```
BEGIN            ← not timed
UPDATE ...       ← TIMED
COMMIT           ← not timed (includes fdatasync)
```

Each COMMIT calls `fdatasync()`, which stalls the CPU for ~3 ms. During that
stall, L1 and L2 caches are completely evicted. The **next** iteration's UPDATE
must then access the transaction's runtime data through cold cache lines.

**Evidence:**

- On **tmpfs** (no fdatasync): DecentDB UPDATE p50 = 2.1 µs, which **beats**
  SQLite's 3.6 µs.
- On **ext4** (real fdatasync): DecentDB UPDATE p50 = 5.5 µs, which **loses** to
  SQLite's 3.5 µs.
- The ~3.4 µs difference is entirely attributable to cold-cache pointer chasing.

### Why SQLite Is Less Affected

SQLite uses a **page-oriented B-tree** where data is stored in contiguous 4 KB
pages. A primary-key lookup typically touches 2–3 pages, and each page load
brings 4 KB of contiguous data into L2, warming neighboring entries. The total
cold-cache cost is roughly 2–3 cache misses × ~100 ns = 200–300 ns.

DecentDB uses Rust's standard `BTreeMap<String, V>` with heap-allocated nodes.
Each node is a separate allocation, and string keys are separate allocations on
top of that. A table lookup + primary-key lookup involves:

1. `BTreeMap<String, TableData>` root node (1 cache line)
2. String key data for each entry (~4 cache lines for 4 tables)
3. `TableData.rows` Vec pointer (1 cache line)
4. `StoredRow` at the target index (1 cache line)
5. `StoredRow.values` Vec pointer (1 cache line)
6. Target `Value` (1 cache line)
7. Dirty-tracking structures (2–3 cache lines)

Total: ~12–15 cold cache line misses × ~100 ns = 1,200–1,500 ns of cold-cache
overhead — roughly 4–5× more than SQLite's page-oriented layout.

### Where the ~5.5 µs Goes (ext4, cold cache)

| Component                           | Estimated Cost |
|-------------------------------------|----------------|
| Python ctypes dispatch              | ~1.0 µs        |
| `ffi_boundary` / `catch_unwind`     | ~0.1 µs        |
| Value creation (`text.to_string()`) | ~0.1 µs        |
| Binding update                      | ~0.05 µs       |
| Schema cookie check (cold)          | ~0.1 µs        |
| `can_reuse_prepared_simple_update`  | ~0.05 µs       |
| `table_data_mut` (cold BTreeMap)    | ~0.8 µs        |
| `row_index_by_id` (cold Vec)        | ~0.3 µs        |
| Read current value (cold)           | ~0.2 µs        |
| Compare + update value              | ~0.05 µs       |
| `mark_table_row_dirty` (allocs)     | ~0.4 µs        |
| `Box::new(state)` in BEGIN          | ~0.3 µs        |
| Mutex acquisitions (2×)             | ~0.2 µs        |
| Miscellaneous                       | ~0.3 µs        |
| **Total**                           | **~4.0 µs**    |

The remaining ~1.5 µs is likely measurement jitter, Python GIL overhead,
and additional cold-cache misses in the pre-built transaction state path.

## What Has Already Been Done

### Optimizations already implemented (this session)

1. **Pre-built transaction state**: After `persist_to_db`, clone the runtime
   while data is cache-warm (before `fdatasync`). Store it for the next BEGIN to
   skip the full runtime clone. This is confirmed working (debug-verified with
   LSN-matched hits). Saves ~2–4 µs per BEGIN on cold cache.

2. **Fast-path reordering**: Moved UPDATE/DELETE fast paths **before** the
   `statement_is_temp_only` check in `execute_prepared_in_state`, eliminating
   cold BTreeMap lookups on `temp_tables`.

3. **Redundant check elimination**: Removed `table_schema().is_some()` from
   `can_reuse_prepared_simple_update` — the schema cookie already validates
   table existence.

4. **Empty temp_tables early exit**: `visible_table_is_temporary` now returns
   `false` immediately when `temp_tables` is empty, avoiding a BTreeMap lookup.

5. **Connection.execute() cursor reuse**: Python `Connection.execute()` now
   reuses a cached cursor instead of creating a new one per call.

6. **Manual EngineRuntime Clone**: Skips caching structures (prepared statement
   cache, dirty sets) during clone, reducing clone cost by ~30%.

### Net effect of all optimizations on Update

- Original: ~8.0 µs p50 (2.3× slower than SQLite)
- Current:  ~5.5 µs p50 (1.6× slower than SQLite)
- Improvement: ~31%

## Strategies to Close the Remaining Gap

### Strategy 1: Eliminate `map_get_ci_mut` Double Lookup

**Effort:** Low
**Expected gain:** ~200–400 ns

`map_get_ci_mut` currently does TWO BTreeMap traversals for the common
case-sensitive match:

```rust
fn map_get_ci_mut<V>(map: &mut BTreeMap<String, V>, name: &str) -> Option<&mut V> {
    if map.contains_key(name) {       // first traversal
        return map.get_mut(name);      // second traversal
    }
    // ... case-insensitive fallback
}
```

The double lookup exists because of a Rust borrow-checker limitation (NLL
cannot prove that the fallback path doesn't alias the first borrow). Two
possible fixes:

- **Use `entry()` API**: `map.entry(name.to_owned())` avoids double lookup but
  requires an allocation. Not a net win.
- **Use a `HashMap` for tables**: `HashMap::get_mut` does a single lookup.
  Switching `EngineRuntime.tables` from `BTreeMap` to `HashMap` would help, but
  the ordering guarantee may be needed for schema introspection.
- **Store a table-index offset**: Instead of looking up by name, store the
  BTreeMap offset or a pre-resolved handle in the `PreparedSimpleUpdate`. This
  is complex and fragile with the borrow checker.
- **Wait for Polonius**: The next-generation Rust borrow checker would allow the
  single-lookup pattern naturally.

### Strategy 2: Avoid String Allocations in `mark_table_row_dirty`

**Effort:** Low–Medium
**Expected gain:** ~200–300 ns

Each UPDATE currently allocates two `String` copies of the table name in
`mark_table_row_dirty`:

```rust
self.dirty_tables.insert(table_name.to_string());
self.row_update_dirty
    .entry(table_name.to_string())
    .or_default()
    .push(row_index);
```

Possible fixes:

- **Use `&str` keys with a lifetime**: Requires lifetime annotations throughout
  `EngineRuntime`, which is invasive.
- **Use a table-index integer key**: Replace `BTreeSet<String>` /
  `BTreeMap<String, Vec<usize>>` with `HashSet<usize>` / `HashMap<usize,
  Vec<usize>>` keyed by a table ordinal index. The `PreparedSimpleUpdate` would
  store the ordinal instead of the name.
- **Use `Rc<str>` or `Arc<str>` shared keys**: Replace `String` keys with
  reference-counted strings shared between the table map and the dirty-tracking
  sets. Cloning is then a pointer increment.
- **Use a bitset for dirty tracking**: For a small number of tables, a `u64`
  bitmask could track which tables are dirty with zero allocation.

### Strategy 3: Replace `BTreeMap<String, TableData>` with a Flat Vec

**Effort:** Medium
**Expected gain:** ~500–800 ns

The core architectural issue is that `BTreeMap` scatters its nodes across the
heap. For a small number of tables (typically 4–10), a flat `Vec<(String,
TableData)>` with linear scan would:

- Keep all table metadata in a single contiguous allocation
- Reduce cold-cache misses from ~4–6 to ~1–2 (one cache line for the Vec
  header, one for the target entry)
- Eliminate per-node heap allocations

This would require updating all call sites that use `self.tables` — there are
many, but they all go through `table_data_mut`, `table_data`, and a few direct
accesses.

A more targeted variant: keep BTreeMap for the general case but add a
**table-index cache** to `PreparedSimpleUpdate` that stores the Vec index of
the target table, validated by schema cookie. On cache hit, skip the BTreeMap
entirely.

### Strategy 4: Arena-Allocated Table Data

**Effort:** High
**Expected gain:** ~800–1,200 ns

Replace the per-row `Vec<Value>` allocations with an arena allocator that keeps
all rows for a table in a single contiguous buffer. This would:

- Dramatically improve cache locality for row access
- Reduce allocator overhead (one large allocation vs many small ones)
- Make `row_index_by_id` a simple offset calculation

This is a significant architectural change that would affect persistence, dirty
tracking, and the clone model. It would likely require an ADR.

### Strategy 5: Page-Oriented In-Memory Representation

**Effort:** Very High (architectural)
**Expected gain:** Would close the gap entirely

The ultimate solution is to move to a page-oriented in-memory representation
similar to SQLite's. Instead of BTreeMap + Vec<StoredRow>, store table data in
B+tree pages that are also the on-disk format. This would:

- Eliminate the serialize/deserialize step in `persist_to_db`
- Make cache behavior identical to SQLite (page-granularity access)
- Enable memory-mapped I/O
- Reduce clone cost (page-level COW instead of deep clone)

This is the long-term architectural direction described in the PRD and SPEC but
represents months of work.

### Strategy 6: Reduce Python Dispatch Overhead

**Effort:** Low
**Expected gain:** ~200–500 ns

The Python ctypes call overhead contributes ~1.0 µs per UPDATE. Options:

- **Inline the BEGIN/UPDATE/COMMIT cycle**: Create a C API function
  `ddb_stmt_begin_rebind_execute_commit` that does all three operations in a
  single FFI crossing. This would save ~2 µs of Python→Rust round-trip overhead
  (3 ctypes calls → 1), but the benchmark only times the UPDATE call, so this
  would only save the overhead within the UPDATE ctypes call itself (~0.5 µs).
- **Use cffi instead of ctypes**: cffi has lower per-call overhead (~0.3 µs vs
  ~0.8 µs), but would be a significant binding change.
- **Use a C extension module**: The `_fastdecode.c` extension already exists.
  Moving the UPDATE fast path into it (with direct C function calls instead of
  ctypes) would reduce per-call overhead to ~0.1 µs.

### Strategy 7: Prefetch / Cache-Line Warming in BEGIN

**Effort:** Low–Medium
**Expected gain:** ~300–600 ns (speculative)

After constructing the pre-built transaction state, explicitly prefetch the
critical data that UPDATE will access:

```rust
// In build_sql_txn_state, after returning pre-built state:
// Touch the "users" table's rows Vec pointer to warm it
if let Some(table) = state.runtime.tables.get("users") {
    std::hint::black_box(&table.rows);
}
```

This is speculative because:
- We don't know which table the next statement will access
- Prefetching the wrong data wastes cache space
- The prefetch itself adds latency to BEGIN

A targeted variant: the `PreparedSimpleUpdate` knows which table it targets.
Add a `prefetch()` method that warms the table's root data. Call it from the
C API after BEGIN but before the timed UPDATE. However, the benchmark's timing
starts after BEGIN, so any warming in BEGIN would be "free" from the
benchmark's perspective — but this would be gaming the benchmark, not a real
improvement.

### Strategy 8: Avoid `catch_unwind` on the Hot Path

**Effort:** Medium
**Expected gain:** ~50–100 ns

Every C API call goes through `ffi_boundary`, which wraps the closure in
`std::panic::catch_unwind`. While Rust optimizes this well when no panic
occurs, there is still a small cost for setting up the landing pad. For the
UPDATE fast path, this could be bypassed with a `#[no_panic]`-style
specialization, but the safety implications need careful evaluation.

## Recommended Priority Order

| Priority | Strategy | Effort | Expected Gain | Risk |
|----------|----------|--------|---------------|------|
| 1 | Avoid String allocs in dirty tracking (#2) | Low | ~200–300 ns | Low |
| 2 | Flat Vec or HashMap for tables (#3 variant) | Medium | ~400–600 ns | Medium |
| 3 | Python C extension for UPDATE (#6 variant) | Low | ~200–500 ns | Low |
| 4 | Eliminate double BTreeMap lookup (#1) | Low | ~200–400 ns | Low |
| 5 | Table-index cache in PreparedSimpleUpdate | Medium | ~300–500 ns | Medium |
| 6 | Arena-allocated rows (#4) | High | ~800–1,200 ns | High |
| 7 | Page-oriented storage (#5) | Very High | Full closure | ADR required |

Strategies 1–4 combined could plausibly save ~1,000–1,800 ns, bringing Update
p50 from ~5.5 µs down to ~3.7–4.5 µs — within striking distance of SQLite's
3.5 µs. However, the inherent cache-locality disadvantage of heap-allocated
BTreeMap nodes means that fully matching SQLite on cold-cache single-row
updates may require Strategy 5 (page-oriented storage) or Strategy 4 (arena
allocation).

## Constraints

Any implementation must preserve:

- **ACID guarantees**: No weakening of WAL semantics, fsync, or crash safety.
- **ABI stability**: No changes to the C ABI without an ADR.
- **On-disk compatibility**: No format changes without an ADR.
- **Correctness**: All existing tests must pass, including SQL conformance,
  constraint enforcement, and binding integration tests.
- **Dependency discipline**: No major new dependencies.

## Measurement Protocol

All measurements use:

```bash
PYTHONPATH=bindings/python \
DECENTDB_NATIVE_LIB=target/release/libdecentdb.so \
python bindings/python/benchmarks/bench_complex.py \
  --users 1000 --items 50 --orders 100
```

The Update metric times only the `cur.execute(update_sql, params)` call inside
an explicit transaction, not the surrounding BEGIN/COMMIT. This means
optimizations to BEGIN or COMMIT do not directly affect the measured metric
unless they change the cache state visible to the UPDATE.
