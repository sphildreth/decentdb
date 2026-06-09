# Query Plan Caching And Prepared-Statement Reuse

**Date:** 2026-06-09
**Status:** TODO
**Future Version:** vNext+1
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Document Type:** Future Win SPEC
**Audience:** Core engine maintainers, planner and executor maintainers, WAL and
storage maintainers, C ABI maintainers, binding maintainers, WASM/browser
mainters, benchmark maintainers, documentation authors, coding agents

**Governing ADRs:**

- Needs ADR before implementation.

**ADR-required decisions before implementation:**

- Plan cache memory accounting, eviction policy, and hard limits.
- Cross-connection plan sharing semantics, including invalidation guarantees
  when the catalog changes in one connection and a plan is cached in another.
- Schema-cookie or catalog-generation invalidation: when to evict cached plans
  and whether invalidation is eager (evict on change) or lazy (evict on next
  use).
- Interaction between plan caching and TDE row policies, projection masks, and
  security scoping.
- C ABI surface: whether plan caching is transparent, opt-in, or configurable
  through open options, and whether the C ABI version needs a bump.
- Whether the plan cache covers DDL, DML, and read queries uniformly, or
  whether write-path statement reuse follows a different contract.
- Whether prepared-statement parameter binding changes invalidate cached plans
  or whether plans are parameter-type-aware.
- Interaction between the plan cache and the existing write queue, group commit,
  and cross-process coordination.

**Related inputs:**

- [`FUTURE_WINS.md`](FUTURE_WINS.md)
- [`WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md`](WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md)
- [`adr/0184-default-fast-planner-and-runtime-contract.md`](adr/0184-default-fast-planner-and-runtime-contract.md)
- [`adr/0162-engine-owned-write-queue-strict-group-commit.md`](adr/0162-engine-owned-write-queue-strict-group-commit.md)
- [`adr/0163-operational-sys-metrics.md`](adr/0163-operational-sys-metrics.md)
- [`adr/0177-cross-process-coordination-sidecar-and-locking.md`](adr/0177-cross-process-coordination-sidecar-and-locking.md)
- [`adr/0174-local-data-security-tde-policies-masking-audit-context.md`](adr/0174-local-data-security-tde-policies-masking-audit-context.md)
- [`BENCHMARKING_GUIDE.md`](BENCHMARKING_GUIDE.md)
- [`docs/user-guide/performance.md`](../docs/user-guide/performance.md)
- [`docs/api/configuration.md`](../docs/api/configuration.md)
- [`include/decentdb.h`](../include/decentdb.h)

---

## 1. Executive Summary

DecentDB currently parses, resolves, and plans every SQL statement from scratch
unless the caller uses a prepared statement that the executor replays. Even
prepared statements re-validate schema cookies and re-bind parameters on every
execution. ORMs, binding layers, and application frameworks commonly execute the
same parameterized queries thousands of times in a session. Each redundant parse
and plan cycle costs CPU, increases p99 latency, and makes DecentDB harder to
recommend for high-throughput embedded workloads where SQLite and PostgreSQL
already cache compiled plans.

This win adds a query plan cache that stores compiled execution plans keyed by
SQL text and parameter shape, invalidates them correctly on schema changes, and
reuses them across prepared-statement executions and connections. The goal is
not a general adaptive query optimizer. The goal is boring, correct plan reuse
that eliminates redundant parse and planner work for repeated statements.

The work is intentionally measurement driven. Plan caching must not regress
one-shot queries, must not exceed accepted memory bounds for low-memory profiles,
and must not introduce hidden contention on the write path.

## 2. Product Goals

- Eliminate redundant parse and planner work for repeated parameterized queries
  within and across connections.
- Preserve correct query results when the schema, statistics, policies, masks,
  or security context change.
- Keep plan cache overhead measurable and bounded for embedded memory profiles.
- Make prepared-statement throughput competitive with SQLite and PostgreSQL for
  high-frequency OLTP workloads.
- Expose cache diagnostics through `sys.*` views and Doctor.
- Make plan caching configurable: opt-in, opt-out, and size-limited.
- Preserve the one-writer/many-readers model. Plan caching must not introduce
  hidden write-path contention.
- Maintain correctness under cross-process coordination, branch operations, sync
  apply, and DDL changes.

## 3. Non-Goals

- No adaptive query optimization or plan stability hints. This win is about
  caching the same plan that would be produced without a cache, not about
  choosing different plans based on parameter values or execution history.
- No persistent plan cache across database restarts. Plans are in-memory only.
- No plan cache for DDL statements. DDL changes are infrequent and must
  invalidate relevant cache entries immediately.
- No cross-process plan sharing. Plans are process-local. Cross-process
  coordination already coordinates WAL and metadata; sharing cached plans across
  processes would require serialization, versioning, and invalidation complexity
  that is not justified by measured demand.
- No mandatory plan caching. Embedded hosts with very tight memory or
  one-shot workloads must be able to disable it entirely.
- No change to file format, WAL format, checkpoint semantics, or durability
  behavior.
- No change to the write queue, group commit, or cross-process coordination
  contract beyond ensuring plan cache invalidation is correct.
- No generalized adaptive statistics system. This win uses existing `ANALYZE`
  statistics when present and falls back to heuristics when absent, consistent
  with ADR 0184 and the default-fast performance spec.

## 4. Current Context

Relevant shipped foundations:

- ADR 0184 governs planner and runtime fast-path contracts, including covering
  index execution, deferred table materialization, and prepared insert batching.
  Plan caching must compose with these fast paths and must not bypass them.
- ADR 0162 governs the engine-owned write queue and strict group commit. Plan
  caching must not delay queue admission or introduce contention on the write
  path.
- ADR 0163 governs `sys.*` operational metrics. Plan cache metrics should be
  added through the same stable metrics contract.
- ADR 0174 governs row policies, projection masks, and audit context. Cached
  plans must be invalidated when policies or masks change, and must not bypass
  policy enforcement during execution.
- ADR 0177-0180 govern cross-process coordination. Plan caching is
  process-local; cross-process coordination does not need to coordinate plan
  caches, but DDL and metadata changes visible to other processes must trigger
  local invalidation.
- Prepared statements exist in the engine and are exposed through the C ABI and
  maintained bindings. They currently re-validate schema cookies on every
  execution.

Current limitations:

- Every SQL statement incurs full parse, resolve, and plan cost on every
  execution.
- Prepared statements re-validate schema cookies but do not cache the compiled
  plan across executions beyond the statement lifetime.
- There is no connection-level or process-level plan cache.
- There is no mechanism to reuse a compiled plan for the same SQL text across
  prepared statements or connections.
- Binding layers that prepare and execute the same query repeatedly pay the
  planner cost each time.
- No `sys.*` view or Doctor finding exists for plan cache behavior.

## 5. Plan Cache Design

### 5.1 Cache Scope And Lifecycle

The plan cache has two scope levels:

| Level | Scope | Lifecycle | Invalidation |
|---|---|---|---|
| Connection-local | Single database connection | Created on connection open, destroyed on connection close | Schema cookie or catalog generation change, DDL, policy/mask change, `ANALYZE` |
| Process-global | Shared across all connections in the same process | Created on first use, destroyed when the last database handle closes or on explicit flush | Schema cookie or catalog generation change, DDL, policy/mask change, `ANALYZE`, `PRAGMA flush_plan_cache` |

Connection-local plans are always valid because they share the same catalog view
as the connection that created them. Process-global plans must be validated
against the current schema cookie before reuse; if the cookie differs, the plan
is evicted and recompiled.

Process-global plan caching is opt-in for the first release. Connection-local
caching is enabled by default but can be disabled.

### 5.2 Cache Keys

A plan cache key consists of:

| Component | Meaning |
|---|---|
| Normalized SQL text | Whitespace-normalized, parameter-placeholder-preserving SQL string |
| Parameter shape | Number of parameters, parameter type classes if they affect plan selection |
| Schema cookie or catalog generation | Opaque generation value used for invalidation, not stored in the key but checked before reuse |
| Security context tag | Hash of the applicable row policy, mask, and audit context identifiers that affect plan selection |

Normalized SQL text preserves parameter placeholders (`?`, `:name`, `$1`) in
their original form but normalizes whitespace, casing for keywords, and trivial
syntactic variations that do not change semantics. Two SQL statements that are
semantically identical but differ in whitespace or keyword casing should produce
the same normalized key.

The security context tag is required because row policies, projection masks, and
audit context can change which rows and columns are visible, which can change
plan selection. Plans cached under one security context must not be reused under
a different security context.

### 5.3 Invalidation

Plans must be invalidated when:

| Event | Scope | Behavior |
|---|---|---|
| Any DDL statement (CREATE, ALTER, DROP, CREATE INDEX, DROP INDEX) | All caches in the process | Evict all entries that reference the affected objects. If full-object tracking is impractical for the first release, evict the entire connection-local cache or the process-global cache. |
| Schema cookie or catalog generation change | All caches in the process | Evict all entries or validate on next use. Prefer eager eviction for DDL; prefer lazy validation for `ANALYZE`. |
| `ANALYZE` on a table or index | Entries referencing the analyzed table | Evict plans that reference the analyzed table so the planner can consider updated statistics. |
| Row policy, mask, or audit context change | Entries with the affected security context tag | Evict all entries with the previous security context tag. |
| Branch switch, restore, or merge | All caches in the process | Evict all entries because the entire catalog may have changed. |
| Sync changeset apply with DDL | Same as DDL invalidation | Same as DDL invalidation. |
| Extension load or unload | All caches in the process | Evict all entries because extension functions and collations may affect resolution. |
| `PRAGMA flush_plan_cache` | Targeted cache | Evict all entries in the targeted cache (connection, process, or all). |

For the first release, eager full evictions on DDL and branch operations are
acceptable. Finer-grained invalidation that tracks which objects a plan
references is a follow-up optimization.

### 5.4 Eviction Policy

The plan cache uses a size-bounded LRU eviction policy:

- Maximum cache size is configurable through `DbConfig` and C ABI open options.
- Default maximum cache size targets the low-memory profile: 4 MiB for
  connection-local plans, 16 MiB for process-global plans when enabled.
- Cache size accounting includes the compiled plan structure, normalized SQL key,
  parameter shape, and security context tag. It does not include runtime state
  such as parameter values, intermediate results, or transaction context.
- When the cache is full, the least-recently-used entry is evicted.
- Eviction must not require holding locks that block the write path or the read
  path for longer than O(1) amortized time.
- Cache memory is accounted separately from the page cache. It does not reduce
  the page cache budget.

### 5.5 Plan Reuse Lifecycle

When a SQL statement is executed:

1. Normalize the SQL text and compute the cache key.
2. Look up the cache key in the connection-local cache (always) and the
   process-global cache (if enabled).
3. If a cached plan is found:
   a. Validate the schema cookie or catalog generation against the current
      catalog.
   b. Validate the security context tag against the current security context.
   c. If either validation fails, evict the entry and fall through to step 4.
   d. If validation passes, skip parse and planning. Proceed to parameter
      binding and execution.
4. If no cached plan is found or validation fails:
   a. Parse and plan the statement normally.
   b. Store the compiled plan in the connection-local cache.
   c. If process-global caching is enabled, store a copy in the process-global
      cache.
5. Execute the plan with parameter binding.

When a prepared statement is executed:

1. The prepared statement holds a reference to a plan in the connection-local
   cache.
2. On each execution, validate the schema cookie against the current catalog
   (current behavior).
3. If the schema cookie matches, reuse the plan without re-parsing or
   re-planning.
4. If the schema cookie does not match, re-compile the plan and update the
   cache entry.

This means prepared statements benefit twice: their existing schema-cookie
validation is sufficient for plan reuse, and they avoid the lookup overhead of
the general cache when the plan is already referenced.

### 5.6 Interaction With The Write Queue

Plan caching must not delay write queue admission or introduce contention on
the write path:

- Plan cache lookups and insertions happen before write queue admission.
- Plan cache invalidation triggered by DDL happens inside the write transaction
  after the DDL is committed, not during write queue admission.
- Plan cache eviction must not acquire exclusive locks. It must use lock-free
  or read-copy-update patterns if the cache is accessed from reader threads.
- Process-global cache access must be safe under the one-writer/many-readers
  model. Reader threads may look up plans concurrently. Only the writer thread
  modifies the catalog and triggers invalidation.

### 5.7 Interaction With Security

Row policies, projection masks, and audit context affect plan selection because
they determine which rows and columns are visible. Plan caching must handle
security context correctly:

- Plans cached under one row policy configuration must not be reused when the
  policy changes.
- Plans cached under one projection mask must not be reused when the mask
  changes.
- Plans cached under one audit context must not be reused when the audit context
  changes, because audit context can affect which rows are visible through
  policy conditions.
- The security context tag in the cache key must capture all security-relevant
  configuration that affects plan selection. If computing this tag is expensive,
  it should be cached as part of the connection security state and updated only
  when security configuration changes.
- If TDE is enabled, plan caching must not bypass TDE page decryption. Plans
  are logical execution plans; they do not contain decrypted page content.

## 6. SQL Diagnostics

Add read-only `sys.*` views:

### 6.1 `sys.plan_cache`

| Column | Type | Meaning |
|---|---|---|
| `scope` | TEXT | `connection` or `process` |
| `cache_key_hash` | TEXT | Stable hash of the normalized SQL text, parameter shape, and security context tag. Not the full SQL text. |
| `schema_cookie` | INT64 | Catalog generation at cache time. |
| `hit_count` | INT64 | Number of times this plan was reused from the cache. |
| `last_used_at` | TEXT | Timestamp of last cache hit. |
| `plan_size_bytes` | INT64 | Approximate memory used by this cached plan. |
| `statement_category` | TEXT | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, or `OTHER`. |

`sys.plan_cache` does not expose the full SQL text of cached statements. It
exposes only the hashed cache key, hit count, size, and metadata. This prevents
sensitive SQL or parameter values from leaking through diagnostics.

### 6.2 `sys.plan_cache_summary`

| Column | Type | Meaning |
|---|---|---|
| `scope` | TEXT | `connection` or `process` |
| `total_entries` | INT64 | Total number of cached plans. |
| `total_hits` | INT64 | Total cache hits since last reset. |
| `total_misses` | INT64 | Total cache misses since last reset. |
| `total_evictions` | INT64 | Total evictions since last reset. |
| `total_size_bytes` | INT64 | Total memory used by cached plans. |
| `max_size_bytes` | INT64 | Configured maximum cache size. |
| `hit_rate` | REAL | Cache hit rate as a percentage. |

### 6.3 `PRAGMA flush_plan_cache`

```sql
PRAGMA flush_plan_cache;           -- flush connection-local and process-global caches
PRAGMA flush_plan_cache = local;   -- flush connection-local cache only
PRAGMA flush_plan_cache = global;   -- flush process-global cache only
```

This `PRAGMA` is useful for benchmarking, debugging, and forced re-planning
after manual `ANALYZE`.

## 7. Rust API And C ABI Contract

### 7.1 Rust

Add:

```rust
pub struct PlanCacheConfig {
    pub enabled: bool,
    pub max_size_bytes: u64,
    pub global_enabled: bool,
    pub global_max_size_bytes: u64,
}

impl DbConfig {
    pub fn plan_cache(&self) -> &PlanCacheConfig;
}
```

Default values:

| Setting | Default | Rationale |
|---|---|---|
| `enabled` | `true` | Connection-local caching is safe and beneficial by default. |
| `max_size_bytes` | 4 MiB | Fits the low-memory profile; proportional to the default 4 MiB page cache. |
| `global_enabled` | `false` | Process-global caching requires cross-connection validation and is opt-in for the first release. |
| `global_max_size_bytes` | 16 MiB | Reasonable bound for shared plan reuse. |

### 7.2 C ABI

Use existing open-with-options functions for:

```text
plan_cache_enabled=true|false
plan_cache_max_bytes=4194304
plan_cache_global_enabled=true|false
plan_cache_global_max_bytes=16777216
```

The C ABI version must be bumped if the plan cache changes the lifetime or
ownership semantics of prepared statements. If the first implementation
keeps existing prepared-statement lifetime semantics unchanged, no C ABI
version bump is required for connection-local caching.

Process-global caching may require a C ABI version bump if it changes how
database handles share state across connections. The ADR must address this
before implementation.

### 7.3 Binding Requirements

Binding maintainers should document:

- whether the binding reuses prepared statements internally (and benefits from
  plan caching automatically)
- whether the binding exposes the plan cache configuration through connection
  options
- whether the binding provides plan cache hit/miss/diagnostic access through
  the `sys.*` views

No binding should implement its own plan cache on top of the C ABI. The engine
owns the plan caching contract.

## 8. Benchmark And Regression Plan

### 8.1 Required Benchmarks

Add the following benchmarks to the native suite:

| Benchmark | What It Measures |
|---|---|
| Prepared point lookup (cache cold) | Latency of first execution of a parameterized point lookup |
| Prepared point lookup (cache warm) | Latency of repeated execution of the same parameterized point lookup |
| Prepared insert (cache warm) | Throughput of repeated parameterized inserts with plan reuse |
| Prepared range scan (cache warm) | Latency of repeated parameterized range scans |
| Cache hit rate under OLTP workload | Percentage of plan cache hits in a mixed OLTP workload |
| Cache eviction under memory pressure | Behavior when the plan cache is smaller than the working set |
| One-shot query (cache disabled vs enabled) | Overhead of plan cache lookup for a query that is not repeated |
| Process-global cache sharing | Throughput gain when multiple connections execute the same queries |

### 8.2 Guardrails

- Connection-local caching must add no more than 2% p95 overhead to one-shot
  queries that are not repeated, measured with caching enabled versus disabled.
- Process-global caching must add no more than 5% p95 overhead to one-shot
  queries when enabled, measured with caching enabled versus disabled.
- Prepared-statement throughput with caching enabled must improve by at least 30%
  for repeated parameterized point lookups compared to the current baseline.
- Plan cache memory must stay within the configured `max_size_bytes` limit.
- Plan cache memory must not exceed 1% of the configured page cache size when
  using default settings on the default-fast benchmark workload.
- DDL and invalidation must not add more than 1 ms to the average DDL execution
  time compared to the no-cache baseline.

### 8.3 Validation

Validation for each implementation slice:

| Change Area | Required Validation |
|---|---|
| Plan cache lookup and insertion | Unit tests for cache key normalization, hit/miss behavior, eviction |
| Invalidation | Unit tests for DDL, ANALYZE, policy, branch, and extension invalidation |
| Security context | Tests proving plans are not shared across different policy/mask configurations |
| Process-global cache | Multi-connection tests proving cross-connection reuse and invalidation |
| Write queue interaction | Tests proving plan caching does not delay write queue admission or group commit |
| Cross-process coordination | Tests proving plan caching does not interfere with WAL coordination; DDL in one process invalidates local plans on next use |
| Binding performance | At least Python binding benchmark for prepared-statement throughput with and without caching |
| Memory bounds | Tests proving cache stays within `max_size_bytes` under load |
| One-shot overhead | Benchmark proving one-shot query latency does not regress beyond 2% |

## 9. Implementation Phases

### Phase 1: Connection-Local Plan Cache

- Add the `PlanCacheConfig` to `DbConfig`.
- Implement connection-local plan cache with LRU eviction.
- Implement cache key normalization (SQL text, parameter shape, security context
  tag).
- Implement schema-cookie and catalog-generation validation before plan reuse.
- Implement eager invalidation on DDL, branch switch, and extension changes.
- Implement `sys.plan_cache` and `sys.plan_cache_summary` views.
- Implement `PRAGMA flush_plan_cache`.
- Add C ABI open options for `plan_cache_enabled` and `plan_cache_max_bytes`.
- Add benchmarks for prepared-statement throughput with and without caching.
- Add one-shot query overhead benchmarks.

Phase 1 must not change C ABI lifetime or ownership semantics. Connection-local
caching must compose with existing prepared-statement behavior without breaking
any binding.

### Phase 2: Process-Global Plan Cache

- Add process-global plan cache behind `plan_cache_global_enabled` open option.
- Implement cross-connection plan sharing with schema-cookie validation.
- Implement process-global LRU eviction with lock-free or read-copy-update
  read access.
- Add `sys.plan_cache` rows for `scope = 'process'`.
- Add multi-connection benchmarks.
- ADR must address C ABI version implications if process-global caching changes
  how database handles share state.

### Phase 3: Binding Integration And Diagnostics

- Update binding documentation for plan cache configuration.
- Add binding-level throughput benchmarks (Python first, then Node or Dart).
- Add Doctor findings for plan cache hit rate, eviction storms, oversized
  plans, and configurations that suggest tuning.
- Add CLI `decentdb doctor` plan cache findings.
- Update `docs/user-guide/performance.md` and `docs/api/configuration.md`.

### Phase 4: Finer-Grained Invalidation

- Track which database objects (tables, indexes, columns) each cached plan
  references.
- Implement object-level invalidation for DDL, `ANALYZE`, and policy changes
  instead of full cache eviction.
- Add `sys.plan_cache` columns for referenced object names (optional, behind a
  diagnostic flag).
- Measure invalidation granularity improvement on workloads with frequent DDL.

Phase 4 is a follow-up optimization. Phase 1 must ship with eager full eviction
on DDL and produce correct results. Object-level invalidation can be added
when DDL invalidation cost measurement justifies the complexity.

## 10. Definition Of Done

This win is complete only when all of these are true:

- Connection-local plan caching is enabled by default with documented memory
  bounds.
- Prepared-statement throughput improves by at least 30% for repeated
  parameterized point lookups in native benchmarks.
- One-shot query latency regresses by no more than 2% with caching enabled.
- Cache hit rate, eviction, and memory use are visible through `sys.*` views.
- DDL, `ANALYZE`, branch switch, policy change, and extension invalidation
  produce correct results.
- Plans cached under one security context are never reused under a different
  security context.
- Plan caching does not delay write queue admission or group commit.
- C ABI open options are documented and tested.
- Binding documentation covers caching behavior.
- Benchmarks are checked into the release suite with guardrails.
- Process-global caching is opt-in and tested if included in the release scope.
- Doctor reports plan cache findings when the configuration or hit rate
  suggests tuning.

## 11. Compatibility Rules

- Plan caching must not change the results of any query.
- Plan caching must not change ordering guarantees that are not already
  nondeterministic.
- Disabling the plan cache (`plan_cache_enabled=false`) must produce identical
  query results to enabling it.
- The C ABI plan cache open options must be additive: old binaries that do not
  set them must get the default behavior (connection-local caching enabled).
- `PRAGMA flush_plan_cache` is a new `PRAGMA` that does not conflict with
  existing `PRAGMA` names.
- `sys.plan_cache` and `sys.plan_cache_summary` are new views that do not
  change existing `sys.*` views.

## 12. Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Plan cache adds measurable overhead to one-shot queries | Measure and guardrail one-shot latency; keep cache lookup O(1); allow disabling the cache entirely. |
| Stale plans produce incorrect results after DDL | Eager full eviction on DDL for Phase 1; object-level invalidation in Phase 4. Tests must prove correctness under interleaved DDL and DML. |
| Security context mismatch causes rows to leak | Security context tag in cache key; invalidation on policy/mask change; test matrix proving non-reuse under different policies. |
| Process-global cache introduces cross-connection contention | Process-global is opt-in in Phase 2; lock-free read access; writer-only invalidation. |
| Plan cache memory grows unbounded on embedded hosts | Hard size limit with LRU eviction; low-memory default (4 MiB); diagnostics for eviction and hit rate. |
| Plan cache interacts incorrectly with TDE page decryption | Plans are logical; they do not contain decrypted content. TDE decryption happens at the page-reader level. Tests must verify TDE-enabled workloads. |
| Plan cache interacts incorrectly with cross-process coordination | Plan cache is process-local. Cross-process DDL invalidation is lazy (next use). Tests must prove correct behavior under multiprocess DDL. |
| Binding authors implement their own plan caches | Document that the engine owns plan caching; add binding guidance. |

## 13. Documentation Requirements

Update:

- `docs/user-guide/performance.md` for plan caching defaults, configuration,
  and tuning guidance.
- `docs/api/configuration.md` for plan cache open options.
- `docs/api/sql-functions.md` for `sys.plan_cache` and
  `sys.plan_cache_summary` views.
- `docs/api/cli-reference.md` for `PRAGMA flush_plan_cache` and `decentdb
  doctor` plan cache findings.
- Binding docs for plan caching behavior and configuration.
- `docs/about/changelog.md` on implementation.