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
- Cross-connection plan sharing semantics, if a later phase implements a
  process-global cache. That ADR must define database identity partitioning and
  invalidation guarantees when the catalog changes in one connection and a plan
  is cached in another.
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
except for existing narrow caches: the connection-local parsed statement cache
and prepared simple-insert fast path. Prepared statements still re-validate
schema cookies and re-bind parameters on every execution. ORMs, binding layers,
and application frameworks commonly execute the same parameterized queries
thousands of times in a session. Redundant resolve and plan work costs CPU,
increases p99 latency, and makes DecentDB harder to recommend for
high-throughput embedded workloads where SQLite and PostgreSQL already cache
compiled work.

This win adds a connection-local, per-`Db` reuse layer for parsed statements and
for concrete reusable plan objects that DecentDB owns safely. Phase 1 is scoped
to exact SQL or parser-stable canonical keys, current prepared-statement
semantics, temp schema invalidation, and security/audit generation checks. The
goal is not a general adaptive query optimizer. The goal is boring, correct
statement reuse that eliminates redundant parse, resolve, and planner work for
repeated statements without changing query semantics.

The work is intentionally measurement driven. Plan caching must not regress
one-shot queries, must not exceed accepted memory bounds for low-memory profiles,
and must not introduce hidden contention on the write path.

## 2. Product Goals

- Eliminate redundant parse, resolve, and planner work for repeated
  parameterized queries within a `Db` handle.
- Preserve correct query results when persistent schema, temp schema,
  statistics, policies, masks, audit context, or other security state changes.
- Keep plan cache overhead measurable and bounded for embedded memory profiles.
- Make prepared-statement throughput competitive with SQLite and PostgreSQL for
  high-frequency OLTP workloads.
- Expose cache diagnostics through `sys.*` views and Doctor.
- Make plan caching configurable: opt-in, opt-out, and size-limited.
- Preserve the one-writer/many-readers model. Plan caching must not introduce
  hidden write-path contention.
- Maintain correctness under cross-process coordination, branch operations, sync
  apply, and DDL changes.
- Leave cross-connection plan sharing to a later optional phase after Phase 1 is
  measured and the required ADR is accepted.

## 3. Non-Goals

- No adaptive query optimization or plan stability hints. This win is about
  caching the same plan that would be produced without a cache, not about
  choosing different plans based on parameter values or execution history.
- No persistent plan cache across database restarts. Plans are in-memory only.
- No plan cache for DDL statements. DDL changes are infrequent and must
  invalidate relevant cache entries immediately.
- No Phase 1 cross-connection plan sharing. Process-global caching is optional
  future work and must be partitioned by database identity or fingerprint if it
  is ever implemented.
- No cross-process plan sharing. Plans are process-local. Cross-process
  coordination already coordinates WAL and metadata; sharing cached plans across
  processes would require serialization, versioning, and invalidation complexity
  that is not justified by measured demand.
- No semantic SQL normalization in Phase 1. Exact SQL text or parser-stable
  canonical forms are allowed; broad rewriting of equivalent SQL strings is
  future work.
- No change to the Rust prepared-statement invalidation contract. Stale Rust
  prepared statements continue to require the current caller behavior unless a
  future ADR explicitly changes it.
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
  maintained bindings. They currently validate persistent and temp schema
  cookies on execution.

Current limitations:

- DecentDB already has a connection-local parsed statement cache and a prepared
  simple-insert cache.
- General statements still lack a reusable resolved or compiled plan cache.
- Prepared statements re-validate schema cookies but do not provide a general
  compiled-plan reuse contract beyond current statement-lifetime behavior.
- There is no connection-level general plan cache.
- There is no mechanism to reuse a compiled plan for the same SQL text across
  independently prepared statements.
- Binding layers that repeatedly create native prepared handles for the same
  query still pay planner cost on each prepare.
- No `sys.*` view or Doctor finding exists for plan cache behavior.

## 5. Plan Cache Design

### 5.1 Cache Scope And Lifecycle

Phase 1 has one cache scope:

| Level | Scope | Lifecycle | Invalidation |
|---|---|---|---|
| Connection-local | Single `Db` handle / database connection | Created on connection open, destroyed on connection close | Persistent schema cookie, temp schema cookie, or catalog generation change; DDL; policy/mask/audit generation change; `ANALYZE`; explicit flush |

Connection-local entries are simpler than shared entries because they use the
same catalog view as the connection that created them. They still must validate
persistent schema, temp schema, and security/audit generations before reuse
because those values can change during the connection lifetime.

Phase 1 cacheable payloads are:

- parsed statement / AST entries already safe to reuse by exact key
- simple DML prepared plans that have explicit reusable plan objects
- selected read plans only after the planner owns a concrete reusable plan type
  with clear validation and execution-state separation

Process-global caching is not in Phase 1. If a later phase adds it, the cache
must be partitioned by database identity or fingerprint, must not share
temp-schema-dependent plans unless proven safe, and must validate the same
schema and security generations as the connection-local cache.

### 5.2 Cache Keys

A plan cache key consists of:

| Component | Meaning |
|---|---|
| SQL identity | Exact SQL text, or a parser-stable canonical SQL/AST key when the parser provides one |
| Parameter shape | Number of parameters, parameter type classes if they affect plan selection |
| Persistent schema cookie or catalog generation | Opaque generation value checked before reuse |
| Temp schema cookie or generation | Connection-local generation checked before reuse |
| Security/audit generation | Hash or generation for row policies, masks, and audit context values that affect plan selection |

Phase 1 does not require semantic SQL normalization. Exact SQL text is
acceptable and preferred until the parser can produce a stable canonical key
without changing SQL meaning. Parser-stable canonical keys must preserve
parameter placeholder identity and must respect quoted identifiers, string
literals, collations, and other syntax that can affect resolution.

The security/audit generation is required because row policies, projection
masks, and audit context values can change which rows and columns are visible,
which can change plan selection. Plans cached under one security/audit
generation must not be reused under a different generation.

Database identity is not part of the Phase 1 key because the cache is
connection-local. It is mandatory for any later process-global key.

### 5.3 Invalidation

Plans must be invalidated when:

| Event | Scope | Behavior |
|---|---|---|
| Any persistent DDL statement (CREATE, ALTER, DROP, CREATE INDEX, DROP INDEX) | Current connection-local cache | Evict all entries. Object-level invalidation is a later optimization. |
| Temp schema DDL or temp schema cookie change | Current connection-local cache | Evict all entries that may resolve through the temp schema. Phase 1 may evict the full connection-local cache. |
| Persistent schema cookie or catalog generation change | Current connection-local cache | Evict all entries or validate lazily on next use. Prefer eager eviction for local DDL. |
| Temp schema cookie or generation change | Current connection-local cache | Evict all entries or validate lazily on next use. |
| `ANALYZE` on a table or index | Current connection-local cache | Evict plans that reference the analyzed table so the planner can consider updated statistics. Phase 1 may evict the full connection-local cache. |
| Row policy, mask, or audit context value/generation change | Current connection-local cache | Evict entries compiled under the previous security/audit generation. Phase 1 may evict the full connection-local cache. |
| Branch switch, restore, or merge | Current connection-local cache | Evict all entries because the entire catalog may have changed. |
| Sync changeset apply with DDL | Same as DDL invalidation | Same as DDL invalidation. |
| Extension load or unload | Current connection-local cache | Evict all entries because extension functions and collations may affect resolution. |
| `PRAGMA flush_plan_cache` | Current connection-local cache | Evict all entries in the connection-local cache. |

For the first release, eager full evictions on DDL and branch operations are
acceptable. Finer-grained invalidation that tracks which objects a plan
references is a follow-up optimization.

### 5.4 Eviction Policy

The plan cache uses a size-bounded LRU eviction policy:

- Maximum cache size is configurable through `DbConfig` and C ABI open options.
- The default maximum cache size is a conservative low-memory profile value
  chosen by the implementation ADR and validated by benchmarks.
- Cache size accounting includes the cached AST or compiled plan structure, SQL
  key, parameter shape, schema generations, and security/audit generation. It
  does not include runtime state such as parameter values, intermediate results,
  or transaction context.
- When the cache is full, the least-recently-used entry is evicted.
- Eviction must not require holding locks that block the write path or the read
  path for longer than a short bounded critical section.
- Cache memory is accounted separately from the page cache. It does not reduce
  the page cache budget.

### 5.5 Plan Reuse Lifecycle

When a SQL statement is executed:

1. Compute the exact SQL key or parser-stable canonical key.
2. Look up the key in the connection-local cache.
3. If a cached plan is found:
   a. Validate the persistent schema cookie or catalog generation against the
      current catalog.
   b. Validate the temp schema cookie or generation against the current
      connection.
   c. Validate the security/audit generation against the current connection
      security state.
   d. If any validation fails, evict the entry and fall through to step 4.
   e. If validation passes, skip the cached parse, resolve, or planning work.
      Proceed to parameter binding and execution.
4. If no cached plan is found or validation fails:
   a. Parse, resolve, and plan the statement normally.
   b. Store the cacheable parsed or planned work in the connection-local cache.
5. Execute the plan with parameter binding.

When a prepared statement is executed:

1. The prepared statement may hold a reference to a connection-local cached AST
   or reusable plan object.
2. On each execution, validate the persistent schema cookie, temp schema cookie,
   and security/audit generation.
3. If validation succeeds, reuse the cached work without re-parsing or
   re-planning.
4. If validation fails, evict the cached work and follow the current Rust
   prepared-statement invalidation contract. Rust prepared statements must not
   gain broad auto-reprepare behavior unless a future ADR explicitly changes
   that contract.

This means prepared statements benefit when their existing validation path can
also validate cached AST or plan work, and they avoid the lookup overhead of the
general cache when cached work is already referenced. Existing C ABI or binding
wrappers may keep wrapper-level auto-reprepare behavior if they already provide
it without changing the Rust contract.

### 5.6 Interaction With The Write Queue

Plan caching must not delay write queue admission or introduce contention on
the write path:

- Plan cache lookups and insertions happen before write queue admission.
- Plan cache invalidation triggered by DDL happens inside the write transaction
  after the DDL is committed, not during write queue admission.
- Plan cache eviction must use the existing one-writer/many-readers model and
  must not hold cache locks across parse, planning, write queue admission, or
  execution.
- Lock-free or read-copy-update designs are optional future optimizations, not
  Phase 1 requirements.

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
- The security/audit generation in the cache key must capture all
  security-relevant configuration and audit context values that affect plan
  selection. If computing this generation is expensive, it should be cached as
  part of the connection security state and updated only when security
  configuration or audit context values change.
- If TDE is enabled, plan caching must not bypass TDE page decryption. Plans
  are logical execution plans; they do not contain decrypted page content.

## 6. SQL Diagnostics

Add read-only `sys.*` views:

### 6.1 `sys.plan_cache`

| Column | Type | Meaning |
|---|---|---|
| `scope` | TEXT | `connection` in Phase 1; `process` is reserved for any later process-global cache |
| `cache_key_hash` | TEXT | Stable hash of the SQL identity, parameter shape, schema generations, and security/audit generation. Not the full SQL text. |
| `persistent_schema_cookie` | INT64 | Persistent catalog generation at cache time. |
| `temp_schema_cookie` | INT64 | Temp schema generation at cache time. |
| `security_audit_generation` | TEXT | Hash or generation of security/audit state used for validation. |
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
| `scope` | TEXT | `connection` in Phase 1; `process` is reserved for any later process-global cache |
| `total_entries` | INT64 | Total number of cached plans. |
| `total_hits` | INT64 | Total cache hits since last reset. |
| `total_misses` | INT64 | Total cache misses since last reset. |
| `total_evictions` | INT64 | Total evictions since last reset. |
| `total_size_bytes` | INT64 | Total memory used by cached plans. |
| `max_size_bytes` | INT64 | Configured maximum cache size. |
| `hit_rate` | REAL | Cache hit rate as a percentage. |

### 6.3 `PRAGMA flush_plan_cache`

```sql
PRAGMA flush_plan_cache;           -- flush connection-local cache
PRAGMA flush_plan_cache = local;   -- flush connection-local cache only
```

This `PRAGMA` is useful for benchmarking, debugging, and forced re-planning
after manual `ANALYZE`. A later process-global phase may add
`PRAGMA flush_plan_cache = global`.

## 7. Rust API And C ABI Contract

### 7.1 Rust

Add:

```rust
pub struct PlanCacheConfig {
    pub enabled: bool,
    pub max_size_bytes: u64,
}

impl DbConfig {
    pub fn plan_cache(&self) -> &PlanCacheConfig;
}
```

Default values:

| Setting | Default | Rationale |
|---|---|---|
| `enabled` | `true` | Connection-local caching is safe and beneficial by default. |
| `max_size_bytes` | TBD by ADR | Conservative low-memory default validated by benchmarks. |

### 7.2 C ABI

Use existing open-with-options functions for:

```text
plan_cache_enabled=true|false
plan_cache_max_bytes=<bytes>
```

The C ABI version must be bumped if the plan cache changes the lifetime or
ownership semantics of prepared statements. If the first implementation
keeps existing prepared-statement lifetime semantics unchanged, no C ABI
version bump is required for connection-local caching.

Existing C ABI or binding wrappers that already provide wrapper-level
auto-reprepare may keep that behavior as long as the Rust prepared-statement
contract remains unchanged.

Process-global caching is not part of the Phase 1 C ABI. A later process-global
phase may require a C ABI version bump if it changes how database handles share
state across connections. The ADR must address this before implementation.

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

### 8.2 Guardrails

- Connection-local caching must add no more than 2% p95 overhead to one-shot
  queries that are not repeated, measured with caching enabled versus disabled.
- Prepared-statement throughput with caching enabled must improve by at least 30%
  for repeated parameterized point lookups compared to the current baseline.
- Plan cache memory must stay within the configured `max_size_bytes` limit.
- Default plan cache memory must pass the low-memory profile benchmarks without
  starving the page cache or increasing eviction storms.
- DDL and invalidation must not add more than 1 ms to the average DDL execution
  time compared to the no-cache baseline.

### 8.3 Validation

Validation for each implementation slice:

| Change Area | Required Validation |
|---|---|
| Plan cache lookup and insertion | Unit tests for exact-key or parser-stable-key behavior, hit/miss behavior, eviction |
| Invalidation | Unit tests for persistent schema, temp schema, DDL, ANALYZE, policy, audit context, branch, and extension invalidation |
| Security context | Tests proving plans are not shared across different policy/mask/audit value generations |
| Write queue interaction | Tests proving plan caching does not delay write queue admission or group commit |
| Cross-process coordination | Tests proving plan caching does not interfere with WAL coordination; DDL in one process invalidates local plans on next use |
| Binding performance | At least Python binding benchmark for prepared-statement throughput with and without caching |
| Memory bounds | Tests proving cache stays within `max_size_bytes` under load |
| One-shot overhead | Benchmark proving one-shot query latency does not regress beyond 2% |

## 9. Implementation Phases

### Phase 1: Connection-Local Plan Cache

- Add the `PlanCacheConfig` to `DbConfig`.
- Implement connection-local plan cache with LRU eviction.
- Implement exact SQL or parser-stable cache keys with parameter shape,
  persistent schema generation, temp schema generation, and security/audit
  generation.
- Reuse parsed AST entries and simple DML prepared plans where DecentDB already
  owns safe reusable objects.
- Add selected read-plan caching only after reusable read-plan objects exist
  with execution state separated from the cached plan.
- Implement persistent schema, temp schema, and security/audit generation
  validation before plan reuse.
- Implement eager invalidation on DDL, temp schema changes, branch switch, and
  extension changes.
- Implement `sys.plan_cache` and `sys.plan_cache_summary` views.
- Implement `PRAGMA flush_plan_cache`.
- Add C ABI open options for `plan_cache_enabled` and `plan_cache_max_bytes`.
- Add benchmarks for prepared-statement throughput with and without caching.
- Add one-shot query overhead benchmarks.

Phase 1 must not change C ABI lifetime or ownership semantics. Connection-local
caching must compose with existing prepared-statement behavior without breaking
any binding.

### Optional Later Phase: Process-Global Plan Cache

- Add process-global plan cache behind `plan_cache_global_enabled` open option.
- Partition cache entries by database identity or fingerprint. Two different
  database files must never share cached plans just because SQL text and schema
  cookies match.
- Share only plans whose validation state is safe across connections. Exclude
  temp-schema-dependent plans unless the planner proves the statement resolves
  only against persistent objects.
- Validate persistent schema generation and security/audit generation before
  reuse. Temp schema validation remains connection-local.
- Implement process-global LRU eviction with measured synchronization overhead.
  Lock-free or read-copy-update access is optional future optimization.
- Add `sys.plan_cache` rows for `scope = 'process'`.
- Add multi-connection benchmarks.
- ADR must address C ABI version implications if process-global caching changes
  how database handles share state.

### Phase 2: Binding Integration And Diagnostics

- Update binding documentation for plan cache configuration.
- Add binding-level throughput benchmarks (Python first, then Node or Dart).
- Add Doctor findings for plan cache hit rate, eviction storms, oversized
  plans, and configurations that suggest tuning.
- Add CLI `decentdb doctor` plan cache findings.
- Update `docs/user-guide/performance.md` and `docs/api/configuration.md`.

### Phase 3: Finer-Grained Invalidation

- Track which database objects (tables, indexes, columns) each cached plan
  references.
- Implement object-level invalidation for DDL, `ANALYZE`, and policy changes
  instead of full cache eviction.
- Add `sys.plan_cache` columns for referenced object names (optional, behind a
  diagnostic flag).
- Measure invalidation granularity improvement on workloads with frequent DDL.

Phase 3 is a follow-up optimization. Phase 1 must ship with eager full eviction
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
- DDL, temp schema changes, `ANALYZE`, branch switch, policy/audit context
  change, and extension invalidation produce correct results.
- Plans cached under one security/audit generation are never reused under a
  different security/audit generation.
- Rust prepared-statement invalidation semantics remain unchanged unless a
  future ADR explicitly changes them.
- Plan caching does not delay write queue admission or group commit.
- C ABI open options are documented and tested.
- Binding documentation covers caching behavior.
- Benchmarks are checked into the release suite with guardrails.
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
- Rust prepared statements keep their existing stale-schema behavior. C ABI or
  binding wrapper-level auto-reprepare may continue only where already provided.
- `PRAGMA flush_plan_cache` is a new `PRAGMA` that does not conflict with
  existing `PRAGMA` names.
- `sys.plan_cache` and `sys.plan_cache_summary` are new views that do not
  change existing `sys.*` views.

## 12. Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Plan cache adds measurable overhead to one-shot queries | Measure and guardrail one-shot latency; keep cache lookup O(1); allow disabling the cache entirely. |
| Stale plans produce incorrect results after DDL | Eager full eviction on DDL for Phase 1; object-level invalidation in Phase 3. Tests must prove correctness under interleaved DDL and DML. |
| Temp schema mismatch causes a stale object binding | Include temp schema generation in validation; evict on temp DDL; test temp table shadowing and drop/recreate cases. |
| Security context mismatch causes rows to leak | Security/audit generation in cache key; invalidation on policy/mask/audit value change; test matrix proving non-reuse under different policies and audit values. |
| Process-global cache introduces cross-connection contention or incorrect sharing | Process-global is optional later work; require ADR, database identity partitioning, temp-schema exclusions, and measured synchronization overhead. |
| Plan cache memory grows unbounded on embedded hosts | Hard size limit with LRU eviction; conservative ADR-chosen default; diagnostics for eviction and hit rate. |
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
