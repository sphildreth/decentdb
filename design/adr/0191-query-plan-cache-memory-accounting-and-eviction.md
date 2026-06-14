# Query Plan Cache: Memory Accounting, Hard Limits, LRU Eviction, And Default Size

**Date:** 2026-06-13
**Status:** Accepted
**Related spec:** [`../WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md`](../WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md)
**Companion ADRs:** [0190](./0190-query-plan-cache-scope-key-and-lifecycle.md), [0192](./0192-query-plan-cache-security-generation-and-tde.md), [0193](./0193-query-plan-cache-c-abi-surface-and-binding-contract.md)

## Decision

The connection-local plan cache uses a size-bounded LRU eviction policy
with the following memory-accounting rules and limits:

1. **Accounted bytes per entry** include the SQL key text, the
   parameter shape vector, the persistent/temp/policy-mask
   generation values, the cached AST or plan object, the cache-key
   hash for `sys.plan_cache` reporting, and the per-entry
   `HashMap` / `VecDeque` overhead.

2. **Not accounted:** runtime state (parameter values, intermediate
   results, transaction context, snapshot context, page-cache
   contents), `Arc` reference counts to shared catalog metadata
   *attributed* to the plan, and any page the plan causes the
   executor to touch. Reference-counted catalog metadata shared
   across many plans is bounded by the database size, not by
   this cache, and is reported separately in `sys.plan_cache_summary`
   as "shared_metadata_bytes" only when a measurable bound is
   available.

3. **Accounting method:** a fixed per-entry overhead constant
   (measured, validated against a representative workload, and
   asserted by a unit test) **plus** a recursive size helper
   applied to the cached AST or plan. The recursive helper
   accounts for the AST/plan's own heap-allocated children but
   does not traverse into `Arc`-shared substructure. This is a
   deliberate approximation, not a precise accounting: the goal
   is a predictable bound, not a `malloc_info`-grade report.

4. **Hard limit:** `max_size_bytes` from `PlanCacheConfig`
   (default 256 KiB in Phase 1A, configurable through `DbConfig`
   and the C ABI `plan_cache_max_bytes` open option). When an
   insertion would exceed the limit, the cache evicts LRU entries
   until the insertion fits. If a single entry is larger than
   `max_size_bytes`, the cache refuses to store it and the next
   miss re-plans the statement; the refused entry is reported in
   `sys.plan_cache_summary.total_oversized_refusals`.

5. **Default 256 KiB.** Chosen as a conservative low-memory
   default that fits inside the existing 4 MiB low-memory page
   cache budget defined in the default-fast performance spec
   (ADR 0184) without leaving the page cache budget starved.
   The default is a `const` and is named in
   `PLAN_CACHE_DEFAULT_MAX_BYTES` so it can be referenced by
   tests and benchmarks.

6. **Capacity is independent of the page cache.** Plan cache
   memory is not deducted from the page cache. The two budgets
   are reported separately in `sys.plan_cache_summary` and
   `sys.storage_metrics`.

7. **Eviction does not require locks held across parse, plan, or
   execute.** The cache mutex is held only for the duration of
   the lookup, validation, and (if needed) insertion or eviction.
   The `PlanCacheInvalidator` trait (`0190` §"Invalidation
   surface") follows the same rule.

8. **Counters reset on `PRAGMA flush_plan_cache`.** Hit, miss,
   and eviction counters are reset on explicit flush. Entry
   contents are evicted; counters are reset.

## Rationale

Embedded engines live inside someone else's memory budget. A plan
cache that grows without bound is worse than no plan cache: it can
starve the page cache, the executor, or the host application. The
shipped default-fast performance profile uses a 4 MiB page cache as
its low-memory default, and the plan cache must coexist with that
budget without making it harder to recommend DecentDB for small
devices and embedded hosts.

The 256 KiB default is conservative on purpose. The shipped
`StatementCache` and `PreparedInsertCache` (`db.rs:666-731` and
`db.rs:740-786`) use 128-entry caps that empirically hold well
under a few hundred KiB for typical OLTP workloads. A 256 KiB
default leaves headroom for the documented `PreparedSimple*`
projection plans (which are larger than parsed ASTs but still
modest) without inviting unbounded growth. A user who needs more
can raise the limit; a user who needs less can lower it. The
embedded-host default is "safe and invisible," not "maximal."

The accounting method is a fixed overhead plus a recursive
helper because `std::mem::size_of` does not capture heap
children. The recursive helper is not exact — Rust's ownership
model makes a malloc-grade accounting impossible without
instrumenting the allocator — but it is conservative enough
to make the hard limit a true bound. The "oversized
refusals" counter exposes the case where a single AST or plan
exceeds the configured budget, which is the most common way
an honest accounting model can fail its contract.

Lock-free or RCU eviction is intentionally not in scope. The
`Mutex<PlanCache>` model composes with the existing
`Mutex<StatementCache>` and `Mutex<PreparedInsertCache>` shape
and avoids introducing a second synchronization primitive in
the same `DbInner`. A future ADR may revisit this if
profiling on hot paths demands it.

## Accounting details

### Per-entry fixed overhead

A measured constant calibrated against a representative OLTP
workload. The constant includes:

- the `HashMap` entry overhead (key + value pointer + hash)
- the `VecDeque` entry slot
- the `Arc<SqlStatement>` or `Arc<PlanObject>` strong-count slot
  for the cached object
- the cache-key hash buffer
- the SQL text (length-prefixed, exact bytes)
- the parameter shape vector (arity + per-placeholder type tag)

The constant is `const PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES: usize`
in `plan_cache.rs` and is asserted by a unit test against
realistic plan objects (parsed AST, `PreparedSimpleInsert`,
`PreparedSimpleRowIdProjection`).

### Recursive size helper

A `size_of_plan_entry(entry: &PlanCacheEntry) -> usize` helper
walks the cached AST or plan and sums the heap-allocated
children it owns. For Phase 1A this is the parsed AST; for
Phase 1B it is the cached plan object. The helper does not
follow `Arc` references into shared catalog metadata.

### Oversized entries

If `size_of_plan_entry(entry) > max_size_bytes`, the cache
refuses to store the entry, increments
`total_oversized_refusals`, and re-plans the statement on
every miss. The refusal is reported in
`sys.plan_cache_summary` so users can identify
`plan_cache_max_bytes` values that are too small for their
workload.

## Validation

The §8.3 matrix in the spec requires the following tests for the
memory subsystem:

- A unit test that asserts the hard limit is not exceeded
  under a 10,000-distinct-statement workload.
- A unit test that asserts the per-entry fixed overhead
  constant is non-zero and bounded.
- A unit test that asserts an oversized entry is refused
  rather than stored.
- A benchmark that asserts the page-cache hit rate is not
  regressed by the default 256 KiB plan cache on the
  low-memory profile workload.
- A benchmark that asserts no pathological eviction pattern
  appears on the p99-under-churn workload.

## Alternatives considered

1. **Cap by entry count instead of bytes.** Rejected. A 128-entry
   cap is the existing `StatementCache` shape; for a generalized
   plan cache that may hold larger `PreparedSimple*` plans, an
   entry-count cap lets one or two large plans consume a
   disproportionate share of the budget.
2. **Use the allocator's `malloc_info` to track real bytes.**
   Rejected. `malloc_info` is not portable to WASM, mobile
   allocators, or the binding-language runtimes. A constant
   plus recursive helper is portable, predictable, and good
   enough.
3. **Skip the recursive helper and just use the fixed overhead.**
   Rejected. Parsed ASTs vary by 10x or more across query
   shapes; a per-entry constant would either over-count small
   statements (wasting budget) or under-count large ones
   (allowing the budget to be exceeded).
4. **Default to 1 MiB.** Rejected. Too aggressive for the
   low-memory profile. Users who want 1 MiB can set it.
5. **Lock-free RCU eviction.** Rejected for v1. The existing
   `Mutex<StatementCache>` model is well-understood and
   composes. A future ADR may revisit this if profiling
   demands it.

## Trade-offs

- **The accounting method is approximate, not exact.** A truly
  exact accounting would require either an instrumented
  allocator or a custom `Drop` trail on every allocation,
  neither of which is acceptable in the engine. The
  approximation is conservative: if the helper under-counts,
  the cache may hold slightly more memory than reported. The
  default 256 KiB is small enough that the under-count, if
  any, is bounded.
- **Oversized entries pay full re-plan cost on every miss.**
  Accepted. The alternative is to evict the entire cache on
  oversized-entry detection, which is worse for the common
  case.
- **No automatic rebalancing with the page cache.** The two
  budgets are independent. A future resource-governance ADR
  may unify them.

## Consequences

- A new `plan_cache.rs` module in `crates/decentdb/src/` holds
  the cache struct, the LRU, the accounting helpers, and the
  constants.
- `DbInner` grows a `plan_cache: Mutex<PlanCache>` field sized
  at construction by `DbConfig::plan_cache().max_size_bytes`.
- `sys.plan_cache_summary` exposes
  `total_size_bytes`, `max_size_bytes`, `total_entries`,
  `total_oversized_refusals`, and the existing `total_hits`,
  `total_misses`, `total_evictions`, and `hit_rate` columns.
- A new unit-test module `plan_cache_tests.rs` covers the
  accounting and the refusal path.
- The default 256 KiB is asserted by a benchmark in
  `crates/decentdb/benches/plan_cache.rs` that runs the
  low-memory profile workload with the default and verifies
  the page-cache hit rate is not regressed.

## References

- `design/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md` §5.4,
  §8.2, §8.3
- `design/adr/0190-query-plan-cache-scope-key-and-lifecycle.md`
- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
  (default-fast performance profile; 4 MiB page-cache budget)
- `crates/decentdb/src/db.rs:666-731` (existing `StatementCache`,
  LRU shape)
- `crates/decentdb/src/db.rs:740-786` (existing
  `PreparedInsertCache`, LRU shape)
- `docs/user-guide/performance.md`
