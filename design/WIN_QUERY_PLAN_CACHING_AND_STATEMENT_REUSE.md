# Query Plan Caching And Prepared-Statement Reuse

**Date:** 2026-06-13
**Status:** DONE
**Delivered Version:** 2.13.0
**Roadmap:** [`FUTURE_WINS.md`](FUTURE_WINS.md)
**Document Type:** Future Win SPEC
**Audience:** Core engine maintainers, planner and executor maintainers, WAL and
storage maintainers, C ABI maintainers, binding maintainers, WASM/browser
maintainers, benchmark maintainers, documentation authors, coding agents

**Governing ADRs:** see the ADRs linked below. The decisions those
ADRs record (cache key, eviction policy, security generation, C ABI
surface) are the answers to the previously-open "ADR-required
decisions" list and are referenced by name throughout this spec.

- [ADR 0190](./adr/0190-query-plan-cache-scope-key-and-lifecycle.md): Scope,
  cache key, invalidation surface, lifecycle, and binding contract for the
  connection-local plan cache.
- [ADR 0191](./adr/0191-query-plan-cache-memory-accounting-and-eviction.md):
  Memory accounting, hard limits, LRU eviction, and default size for the
  connection-local plan cache.
- [ADR 0192](./adr/0192-query-plan-cache-security-generation-and-tde.md):
  Security/audit generation counter, policy/mask invalidation, TDE interaction,
  and the audit-context-as-observable-but-not-cache-key decision.
- [ADR 0193](./adr/0193-query-plan-cache-c-abi-surface-and-binding-contract.md):
  C ABI open options, default-on vs default-off, the additive-vs-version-bump
  call, and binding responsibilities.
- [ADR 0194](./adr/0194-query-plan-cache-prepared-plan-reuse.md):
  Phase 1B prepared-plan bundle reuse for `PreparedSimple*` and simple DML
  plans.

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
except for two existing narrow caches: the connection-local `StatementCache`
(parsed AST keyed by exact SQL text) and the `PreparedInsertCache` (simple
insert plans keyed by SQL text + cookies). Prepared statements still
re-validate schema cookies and re-bind parameters on every execution. ORMs,
binding layers, and application frameworks commonly execute the same
parameterized queries thousands of times in a session. Redundant resolve and
plan work costs CPU, increases p99 latency, and makes DecentDB harder to
recommend for high-throughput embedded workloads where SQLite and PostgreSQL
already cache compiled work.

This win generalizes the existing AST cache and adds a connection-local
reusable-plan layer for the concrete plan types DecentDB already owns safely
(`PreparedSimpleInsert`, `PreparedSimpleUpdate`, `PreparedSimpleDelete`,
`PreparedSimpleRowIdProjection`, `PreparedSimpleRowIdRangeProjection`,
`PreparedSimpleRowIdJoinProjection`, `PreparedSimpleScalarFilteredAggregate`).
Phase 1A ships the cache, the key, and the invalidation surface. Phase 1B
adds the executor integration that lets the cache store *executable* read
plans once that integration lands. The goal is not a general adaptive query
optimizer. The goal is boring, correct statement reuse that eliminates
redundant parse, resolve, and planner work for repeated statements without
changing query semantics.

The work is intentionally measurement driven. Plan caching must not regress
one-shot queries, must not exceed accepted memory bounds for low-memory
profiles, and must not introduce hidden contention on the write path.

### Phasing correction relative to the previous draft

The previous draft (2026-06-09) bundled a broad "selected read-plan caching"
item into Phase 1 alongside the AST-cache generalization. This revised spec
splits that work:

- **Phase 1A** — AST cache generalization + invalidation surface + diagnostics
  + C ABI open options. This is the shippable, well-bounded slice and the only
  phase that needs the umbrella ADR 0190.
- **Phase 1B** — Caching of executable read plans whose runtime state is
  already separable (`PreparedSimple*` projections). This needs its own ADR
  because the planner/executor integration is materially different from AST
  reuse.
- **Phase 2** — Process-global cache (ADR 0190 §10).
- **Phase 3** — Finer-grained object-level invalidation.
- **Phase 4** — Binding integration, CLI surface, Doctor findings.

## 2. Product Goals

- Eliminate redundant parse, resolve, and planner work for repeated
  parameterized queries within a `Db` handle.
- Preserve correct query results when persistent schema, temp schema,
  statistics, policies, masks, or audit context change.
- Keep plan cache overhead measurable and bounded for embedded memory profiles.
- Make prepared-statement throughput competitive with SQLite and PostgreSQL
  for high-frequency OLTP workloads.
- Expose cache diagnostics through `sys.*` views and Doctor.
- Make plan caching configurable: opt-in, opt-out, and size-limited.
- Preserve the one-writer/many-readers model. Plan caching must not introduce
  hidden write-path contention.
- Maintain correctness under cross-process coordination, branch operations,
  sync apply, and DDL changes.
- Leave cross-connection plan sharing to a later optional phase after Phase 1
  is measured and the umbrella ADR is accepted.

## 3. Non-Goals

- No adaptive query optimization or plan stability hints. This win is about
  caching the same plan that would be produced without a cache, not about
  choosing different plans based on parameter values or execution history.
- No persistent plan cache across database restarts. Plans are in-memory only.
- No plan cache for DDL statements. DDL changes are infrequent and must
  invalidate relevant cache entries immediately.
- No Phase 1 cross-connection plan sharing. Process-global caching is optional
  future work and must be partitioned by database identity or fingerprint if
  it is ever implemented.
- No cross-process plan sharing. Plans are process-local. Cross-process
  coordination already coordinates WAL and metadata; sharing cached plans
  across processes would require serialization, versioning, and invalidation
  complexity that is not justified by measured demand.
- No semantic SQL normalization in Phase 1. Exact SQL text is acceptable and
  preferred until the parser can produce a stable canonical key without
  changing SQL meaning.
- No change to the Rust prepared-statement invalidation contract. Stale Rust
  prepared statements continue to require the current caller behavior unless
  a future ADR explicitly changes it.
- No mandatory plan caching. Embedded hosts with very tight memory or
  one-shot workloads must be able to disable it entirely.
- No change to file format, WAL format, checkpoint semantics, or durability
  behavior.
- No change to the write queue, group commit, or cross-process coordination
  contract beyond ensuring plan cache invalidation is correct.
- No generalized adaptive statistics system. This win uses existing `ANALYZE`
  statistics when present and falls back to heuristics when absent.
- No `EXPLAIN` result caching. `EXPLAIN` always plans fresh so users can use
  it to debug plan changes without a separate `PRAGMA` knob.
- No caching of plans whose plan-shape selection depends on parameter *values*
  (only parameter *types and arity* participate in the key, see §5.2 and
  ADR 0190).

## 4. Current Context

Relevant shipped foundations:

- ADR 0184 governs planner and runtime fast-path contracts, including covering
  index execution, deferred table materialization, and prepared insert
  batching. Plan caching must compose with these fast paths and must not
  bypass them. ADR 0184 explicitly permits plan caching keyed by schema cookie,
  temp schema cookie, and statistics generation, and requires invalidation on
  catalog, temp schema, policy/mask, and extension changes.
- ADR 0162 governs the engine-owned write queue and strict group commit. Plan
  caching must not delay queue admission or introduce contention on the write
  path.
- ADR 0163 governs `sys.*` operational metrics. Plan cache metrics should be
  added through the same stable metrics contract and exposed through the same
  parsed-SQL dispatch path used by `sys.sync_status` and friends
  (`db.rs:13390-13469`).
- ADR 0174 governs row policies, projection masks, and audit context. Cached
  plans must be invalidated when policies or masks change, and must not bypass
  policy enforcement during execution. ADR 0192 is the security/audit
  decision companion to this spec.
- ADR 0177-0180 govern cross-process coordination. Plan caching is
  process-local; cross-process coordination does not need to coordinate plan
  caches, but DDL and metadata changes visible to other processes must trigger
  local invalidation.
- Prepared statements exist in the engine and are exposed through the C ABI
  and maintained bindings. They currently validate persistent and temp schema
  cookies on execution.

Current limitations (concrete code references):

- `crates/decentdb/src/db.rs:666-731` — `StatementCache` is keyed by exact SQL
  text only, with capacity 128, and does not include schema cookies, temp
  cookies, security state, or parameter shape in the key.
- `crates/decentdb/src/db.rs:740-786` — `PreparedInsertCache` is keyed by
  (sql, schema_cookie, temp_schema_cookie), capacity 128, and is the only
  existing plan cache that validates against catalog state.
- `crates/decentdb/src/db.rs:195-274` — `PreparedSimple*` projection types
  (row-id projection, row-id range projection, row-id join projection,
  scalar filtered aggregate) are reusable plan objects that Phase 1B can
  extend to be cacheable without inventing new plan types.
- `crates/decentdb/src/db.rs:3257` — `Db::schema_cookie` returns the
  connection's current schema cookie; `current_schema_cookie_at_snapshot` at
  `db.rs:8186` returns the cookie at a specific snapshot LSN. The cache key
  uses the *connection-local* current cookie, not a snapshot cookie; this is
  a deliberate choice documented in ADR 0190 §6.
- `crates/decentdb/src/db.rs:3145` and `db.rs:3225-3240` — the audit context
  is an `Arc<Mutex<AuditContext>>`. Per ADR 0192, the audit context is a
  *diagnostic observable* and a TDE/policy/mask invalidation trigger source,
  but it is **not** part of the cache key.
- `crates/decentdb/src/db.rs:98` — `table_touch_generation` is a per-table
  "did anything change" tracker used for deferred table materialization. It
  is not part of the Phase 1 cache key (Phase 1 uses the catalog-wide
  schema cookie for invalidation) and is the natural hook for Phase 3
  object-level invalidation.
- `crates/decentdb/src/db.rs:507-508` — `statement_cache` and
  `prepared_insert_cache` are `Mutex`-protected. The new cache lives in the
  same `DbInner` struct and follows the same locking model.

## 5. Plan Cache Design

### 5.1 Cache Scope And Lifecycle

Phase 1 has one cache scope:

| Level | Scope | Lifecycle | Invalidation |
|---|---|---|---|
| Connection-local | Single `Db` handle / database connection | Created on connection open, destroyed on connection close | Persistent schema cookie, temp schema cookie, or catalog generation change; DDL; policy/mask/audit generation change; `ANALYZE`; explicit flush |

Connection-local entries are simpler than shared entries because they use the
same catalog view as the connection that created them. They still must
validate persistent schema, temp schema, and policy/mask generations before
reuse because those values can change during the connection lifetime.

Phase 1 cacheable payloads are:

- Phase 1A: parsed AST entries keyed by (SQL text, parameter shape,
  persistent schema cookie, temp schema cookie, policy/mask generation). This
  is a generalization of the existing `StatementCache`.
- Phase 1B: pre-resolved simple-DML prepared plans and `PreparedSimple*`
  projection plans, with their existing cookie validation reused.
- Phase 2+: read plans whose runtime state is fully separable from the cached
  plan object.

Process-global caching is not in Phase 1. If a later phase adds it, the cache
must be partitioned by database identity or fingerprint, must not share
temp-schema-dependent plans unless proven safe, and must validate the same
schema and policy/mask generations as the connection-local cache.

### 5.2 Cache Keys

A plan cache key consists of:

| Component | Meaning | Notes |
|---|---|---|
| SQL identity | Exact SQL text in Phase 1 | Parser-stable canonical forms are a follow-up once the parser exposes them |
| Parameter shape | Number of `$n` / `?` placeholders and the SQL type class of each (INTEGER, REAL, TEXT, BLOB, NULL) | "Type class" refers to the column affinity / declared parameter type, not the runtime value. Two queries whose placeholders resolve to the same shape class share an entry. |
| Persistent schema cookie | `Db::schema_cookie()` at cache time (`db.rs:3257`) | The connection-local current cookie, not a snapshot cookie (see ADR 0190 §6) |
| Temp schema cookie | Connection-local temp schema cookie | Bumped on temp-table DDL or temp cookie change |
| Policy/mask generation | `u32` counter bumped on every CREATE/DROP/ALTER POLICY and every projection-mask change | Distinct from the schema cookie; see ADR 0192 |

**What is deliberately not in the key:**

- **Audit context values.** `set_audit_context` writes to the audit log
  (`db.rs:3732-3733`) and does not change which rows are visible to a query.
  Per ADR 0192, the audit context is observable through the existing
  `audit_context_snapshot` API and through the audit events table, but it is
  not a plan-cache key component. If a future change makes `set_audit_context`
  affect row visibility, that change must come with its own ADR that adds
  audit context to the cache key.
- **TDE state.** TDE is enforced at the page-reader level and does not affect
  plan shape. Plans are logical execution plans; they do not contain decrypted
  page content.
- **`table_touch_generation` values.** Phase 1 invalidates the whole cache on
  any persistent schema change. Phase 3 introduces object-level invalidation
  using `table_touch_generation` per-table.
- **Statistics (`ANALYZE`) state.** Statistics do not change plan *shape* in
  Phase 1; they only influence planner choice. A future planner may include
  statistics generation in the key, but that change requires its own ADR.
- **Branch identity, snapshot identity, LSN.** The connection-local cache
  is invalidated wholesale on branch switch / restore / merge. Stale-LSN
  plans are addressed by validating against the connection's current schema
  cookie, not by encoding LSN in the key.
- **Database identity.** Not part of the Phase 1 key because the cache is
  connection-local. Mandatory for any later process-global key.

Parameter "shape" means arity and declared type class — never parameter
*values*. Two queries `SELECT * FROM t WHERE id = ?` and
`SELECT * FROM t WHERE id = ?` with different parameter values (e.g. `1`
vs `9999999999`) hit the same cache entry.

### 5.3 Invalidation

Plans must be invalidated when:

| Event | Scope | Behavior | Call site (representative) |
|---|---|---|---|
| Any persistent DDL statement (CREATE, ALTER, DROP, CREATE INDEX, DROP INDEX) | Current connection-local cache | Evict all entries. Object-level invalidation is Phase 3. | `db.rs` DDL execution paths |
| Temp schema DDL or temp schema cookie change | Current connection-local cache | Evict all entries that may resolve through the temp schema. Phase 1 may evict the full connection-local cache. | Temp state mutex sites |
| Persistent schema cookie or catalog generation change | Current connection-local cache | Evict all entries. Eager eviction is preferred for local DDL. | `Db::schema_cookie` accessor + WAL header parse |
| Temp schema cookie or generation change | Current connection-local cache | Evict all entries. | Temp state mutex sites |
| `ANALYZE` on a table or index | Current connection-local cache | Evict plans that reference the analyzed table. Phase 1 may evict the full connection-local cache. | `ANALYZE` execution path |
| Policy/mask change (CREATE/DROP/ALTER POLICY, projection mask change) | Current connection-local cache | Evict all entries; bump policy/mask generation counter (ADR 0192). | Policy DDL paths in `db.rs:2192-2193` and `db.rs:3628-3634` |
| `SET AUDIT CONTEXT` write | Current connection-local cache | **No invalidation.** Audit context does not affect plan shape. Audit context is observable through the existing audit events table and `audit_context_snapshot`. | `db.rs:3225-3240` — emits audit event but does not call cache invalidation |
| Branch switch, restore, or merge | Current connection-local cache | Evict all entries because the entire catalog may have changed. | Branch module |
| Sync changeset apply with **DDL** | Current connection-local cache | Same as DDL invalidation (apply bumps the schema cookie). | Sync apply path |
| Sync changeset apply with **DML only** | Current connection-local cache | **No invalidation.** Catalog cookie is unchanged. DML is reflected in data, not in plan shape. | Sync apply path |
| Cross-process DDL observed via coordination sidecar | Current connection-local cache | Lazy: validated on next use by comparing cached persistent cookie against current cookie. | Cross-process read paths |
| Extension load or unload | Current connection-local cache | Evict all entries because extension functions and collations may affect resolution. | Lua extension lifecycle |
| `PRAGMA flush_plan_cache` | Current connection-local cache | Evict all entries in the connection-local cache. | New `PRAGMA` dispatch path |

For the first release, eager full evictions on DDL and branch operations
are acceptable. Finer-grained invalidation that tracks which objects a plan
references is the Phase 3 follow-up optimization.

#### 5.3.1 Invalidation sink

The cache exposes a single internal trait that catalog / DDL / policy / sync
code calls when a relevant event happens:

```rust
pub(crate) trait PlanCacheInvalidator: Send {
    fn on_persistent_ddl(&self);
    fn on_temp_schema_change(&self);
    fn on_analyze(&self, _table: &str) { self.on_persistent_ddl(); } // default: full evict
    fn on_policy_mask_change(&self);
    fn on_branch_switch(&self);
    fn on_extension_change(&self);
    fn on_explicit_flush(&self);
}
```

`DbInner` implements this trait and dispatches to the cache. Audit-context
writes deliberately do not call into this sink. The current
`StatementCache` and `PreparedInsertCache` continue to live as they do
today; Phase 1A generalizes `StatementCache` to participate in the
invalidator without removing the older caches.

### 5.4 Eviction Policy

The plan cache uses a size-bounded LRU eviction policy. Details and the
memory-accounting method are governed by ADR 0191. The high-level rules:

- Maximum cache size is configurable through `DbConfig` and C ABI open
  options.
- The default maximum cache size is a conservative low-memory profile value
  chosen by the implementation ADR and validated by benchmarks.
- When the cache is full, the least-recently-used entry is evicted.
- Eviction must not require holding locks that block the write path or the
  read path for longer than a short bounded critical section.
- Cache memory is accounted separately from the page cache. It does not
  reduce the page cache budget.

### 5.5 Plan Reuse Lifecycle

When a SQL statement is executed:

1. Compute the cache key (see §5.2).
2. Look up the key in the connection-local cache.
3. If a cached entry is found:
   a. Validate the persistent schema cookie against `Db::schema_cookie()`.
   b. Validate the temp schema cookie against the connection's temp state.
   c. Validate the policy/mask generation against the current policy/mask
      generation.
   d. If any validation fails, evict the entry and fall through to step 4.
   e. If validation passes, skip parse (and resolve/planning work, for
      Phase 1B). Proceed to parameter binding and execution.
4. If no cached entry is found or validation fails:
   a. Parse, resolve, and (for Phase 1B) plan the statement normally.
   b. Store the cacheable work in the connection-local cache.
5. Execute the plan with parameter binding.

When a prepared statement is executed:

1. The prepared statement may hold a reference to a connection-local cached
   AST or reusable plan object.
2. On each execution, validate the persistent schema cookie, temp schema
   cookie, and policy/mask generation.
3. If validation succeeds, reuse the cached work without re-parsing or
   re-planning.
4. If validation fails, evict the cached work and follow the current Rust
   prepared-statement invalidation contract. Rust prepared statements must
   not gain broad auto-reprepare behavior unless a future ADR explicitly
   changes that contract.

Prepared statements benefit when their existing validation path can also
validate cached AST or plan work, and they avoid the lookup overhead of the
general cache when cached work is already referenced. Existing C ABI or
binding wrappers may keep wrapper-level auto-reprepare behavior if they
already provide it without changing the Rust contract.

### 5.6 Interaction With The Write Queue

Plan caching must not delay write queue admission or introduce contention
on the write path:

- Plan cache lookups and insertions happen before write queue admission.
- Plan cache invalidation triggered by DDL happens inside the write
  transaction after the DDL is committed, not during write queue admission.
  Writer connections invalidate their own cache at DDL commit time so a
  `db.prepare(...)` call immediately after `CREATE TABLE` never observes a
  stale plan.
- Plan cache eviction must use the existing one-writer/many-readers model
  and must not hold cache locks across parse, planning, write queue
  admission, or execution.
- Lock-free or read-copy-update designs are optional future optimizations,
  not Phase 1 requirements.

### 5.7 Interaction With Security

The detailed security decisions live in ADR 0192. The summary:

- The cache key includes a **policy/mask generation** counter (u32, bumped on
  CREATE/DROP/ALTER POLICY and on projection-mask changes), not an audit
  context hash.
- `SET AUDIT CONTEXT` does not affect the cache key. It writes an audit
  event and is observable through `db.audit_context_snapshot()`. A future
  change that makes audit context affect plan shape must come with its own
  ADR and add the audit context to the key.
- TDE is enforced at the page-reader level. Cached plans do not contain
  decrypted page content and do not bypass TDE.
- The plan cache holds no plan that contains row data or column values.
  Cached plans are logical execution plans.

## 6. SQL Diagnostics

Add read-only `sys.*` views. The dispatch path follows the existing
`SyncInspectionQuery`-style pattern at `db.rs:13390-13469` and registers a
new `PlanCacheQuery` enum variant.

### 6.1 `sys.plan_cache`

| Column | Type | Meaning |
|---|---|---|
| `scope` | TEXT | `connection` in Phase 1; `process` is reserved for any later process-global cache |
| `cache_key_hash` | TEXT | SipHash-2-4 of (SQL identity, parameter shape, schema generations, policy/mask generation). Not the full SQL text. The hash is for diagnostic matching only; it is not a contract and may change across engine versions. |
| `persistent_schema_cookie` | INT64 | Persistent catalog generation at cache time. |
| `temp_schema_cookie` | INT64 | Temp schema generation at cache time. |
| `policy_mask_generation` | INT64 | Policy/mask generation at cache time. |
| `hit_count` | INT64 | Number of times this entry was reused from the cache. |
| `last_used_at` | TEXT | Timestamp of last cache hit. |
| `plan_size_bytes` | INT64 | Approximate memory used by this cached entry (per ADR 0191). |
| `statement_category` | TEXT | Closed enum: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `PRAGMA`, `EXPLAIN`, `SET`, `OTHER`. `EXPLAIN` rows are not cached in Phase 1; the column is reserved for the future. |

`sys.plan_cache` does not expose the full SQL text of cached statements. It
exposes only the hashed cache key, hit count, size, and metadata. This
prevents sensitive SQL or parameter values from leaking through diagnostics.
The `cache_key_hash` is suitable for matching cache entries against an
external query log but is **not** stable across engine versions; do not
persist it.

### 6.2 `sys.plan_cache_summary`

| Column | Type | Meaning |
|---|---|---|
| `scope` | TEXT | `connection` in Phase 1; `process` is reserved for any later process-global cache |
| `total_entries` | INT64 | Total number of cached entries. |
| `total_hits` | INT64 | Total cache hits since last reset. |
| `total_misses` | INT64 | Total cache misses since last reset. |
| `total_evictions` | INT64 | Total evictions since last reset. |
| `total_size_bytes` | INT64 | Total memory used by cached entries. |
| `max_size_bytes` | INT64 | Configured maximum cache size. |
| `hit_rate` | REAL | Cache hit rate as a percentage. |

`PRAGMA flush_plan_cache` resets hit/miss/eviction counters in addition to
evicting entries.

### 6.3 `PRAGMA flush_plan_cache`

```sql
PRAGMA flush_plan_cache;           -- flush connection-local cache
PRAGMA flush_plan_cache = local;   -- flush connection-local cache only
```

This `PRAGMA` is useful for benchmarking, debugging, and forced
re-planning after manual `ANALYZE`. A later process-global phase may add
`PRAGMA flush_plan_cache = global`. The PRAGMA does not conflict with
existing PRAGMA names; it is a new PRAGMA dispatched through the existing
PRAGMA parser.

### 6.4 CLI surface

The CLI exposes:

```text
decentdb plan-cache stats [--json]    # sys.plan_cache_summary as text or JSON
decentdb plan-cache list  [--json]    # sys.plan_cache (one row per entry)
decentdb plan-cache reset             # PRAGMA flush_plan_cache on every handle
```

`decentdb plan-cache reset` opens a writer connection (or uses an existing
one), runs `PRAGMA flush_plan_cache`, and exits. Resetting from a second
process is best-effort: it is documented as a debug operator action, not a
runtime hot-path knob.

## 7. Rust API And C ABI Contract

### 7.1 Rust

Add:

```rust
pub struct PlanCacheConfig {
    pub enabled: bool,
    pub max_size_bytes: u64,
}

impl DbConfig {
    pub fn with_plan_cache<F: FnOnce(&mut PlanCacheConfig)>(&mut self, f: F) -> &mut Self;
    pub fn plan_cache(&self) -> &PlanCacheConfig;
}
```

The builder-style `with_plan_cache` accessor composes with the existing
`DbConfig` builder. `plan_cache()` returns an immutable view for
introspection.

Default values:

| Setting | Default | Rationale |
|---|---|---|
| `enabled` | `true` | Connection-local caching is safe and beneficial by default. Existing C ABI binaries that do not set the option get the default. This is a deliberate behavior change (see ADR 0193). |
| `max_size_bytes` | TBD by ADR 0191 | Conservative low-memory default validated by benchmarks. |

### 7.2 C ABI

Use existing open-with-options functions for:

```text
plan_cache_enabled=true|false
plan_cache_max_bytes=<bytes>
```

The C ABI version is **not** bumped for Phase 1. The C ABI plan cache open
options are additive; old binaries that do not set them get the default
behavior (connection-local caching enabled, default max size). The
rationale and binding-contract implications are in ADR 0193.

Process-global caching is not part of the Phase 1 C ABI. A later
process-global phase may require a C ABI version bump if it changes how
database handles share state across connections. The ADR for that phase
must address the version-bump question before implementation.

### 7.3 Binding Requirements

Binding maintainers should document:

- whether the binding reuses prepared statements internally (and benefits
  from plan caching automatically);
- whether the binding exposes the plan cache configuration through
  connection options;
- whether the binding provides plan cache hit/miss/diagnostic access
  through the `sys.*` views;
- how the binding surfaces the `decentdb plan-cache` CLI subcommands, or
  whether the binding provides its own equivalent.

No binding should implement its own plan cache on top of the C ABI. The
engine owns the plan caching contract.

### 7.4 WASM / browser note

`plan_cache_max_bytes` is a *per-connection* budget. Browser apps that
spawn N Web Workers per tab should set `plan_cache_max_bytes` to
`low_memory_budget / N_workers` to keep total plan-cache memory predictable
across the tab. The default may be too high for a 4-worker tab. This is
documented in `docs/user-guide/performance.md`.

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
| p99 latency under churn | A workload that issues 1,000 distinct prepared statements repeatedly and measures p99 in the warm steady state. Catches pathological eviction patterns that a single-statement warm benchmark would miss. |

### 8.2 Guardrails

- **One-shot overhead.** Connection-local caching must add no more than 2% p95
  overhead to one-shot queries that are not repeated, measured with caching
  enabled versus disabled.
- **Warm throughput.** Prepared-statement throughput with caching enabled
  must improve by at least 30% for repeated parameterized point lookups
  compared to the current baseline. The 30% floor is intentionally easy to
  hit so it cannot be satisfied by cosmetic changes; the *real* signal is
  the p99-under-churn benchmark and the cross-binding throughput benchmarks.
- **Memory bounds.** Plan cache memory must stay within the configured
  `max_size_bytes` limit. Tests must prove the bound under load.
- **Eviction storms.** Default plan cache memory must pass the low-memory
  profile benchmarks without starving the page cache or producing
  pathological eviction patterns.
- **DDL overhead.** DDL overhead from cache invalidation is measured
  *relatively*: invalidation of N entries must complete in less than 5% of
  the underlying DDL statement's wall-clock time, or in less than 5 ms
  absolute, whichever is smaller. The previous draft's 1 ms absolute bound
  was unmeasurable on small tables and irrelevant on large ones.
- **p99 under churn.** p99 latency of the warm churn workload must not
  regress compared to the cache-disabled baseline. This guards against
  caches that win the synthetic benchmark and lose real workloads.

### 8.3 Validation

Validation for each implementation slice:

| Change Area | Required Validation |
|---|---|
| Plan cache lookup and insertion | Unit tests for exact-key behavior, hit/miss behavior, eviction under pressure |
| Invalidation | Unit tests for persistent schema, temp schema, DDL, ANALYZE, policy/mask, branch, extension, and explicit-flush invalidation |
| Audit context round-trip | Test: set audit context A, prepare, set audit context B, do not execute, set audit context A back, execute. The plan from A must be reused. This guards against subtle security-context generation bugs. |
| Policy/mask round-trip | Test matrix proving non-reuse under different policy generations. |
| Write queue interaction | Tests proving plan caching does not delay write queue admission or group commit. |
| Cross-process coordination | Tests proving plan caching does not interfere with WAL coordination; DDL in one process invalidates local plans on next use. |
| Binding performance | At least Python binding benchmark for prepared-statement throughput with and without caching. |
| Memory bounds | Tests proving cache stays within `max_size_bytes` under load. |
| One-shot overhead | Benchmark proving one-shot query latency does not regress beyond 2%. |
| p99 under churn | Benchmark proving p99 latency does not regress under repeated distinct-statement workload. |

## 9. Implementation Phases

### Phase 1A: AST Cache Generalization And Invalidation Surface

- Generalize the existing `StatementCache` (`db.rs:666-731`) to key by
  (SQL text, parameter shape, persistent schema cookie, temp schema
  cookie, policy/mask generation). Keep the existing LRU shape and
  capacity default.
- Add the `PlanCacheInvalidator` trait (see §5.3.1) and wire the catalog,
  DDL, policy, and sync code paths to call into it.
- Add `PlanCacheConfig` to `DbConfig` (see §7.1).
- Add `sys.plan_cache` and `sys.plan_cache_summary` views through the
  existing parsed-SQL dispatch path.
- Add `PRAGMA flush_plan_cache`.
- Add the `decentdb plan-cache` CLI subcommands.
- Add C ABI open options `plan_cache_enabled` and `plan_cache_max_bytes`.
- Add the AST-cache benchmarks from §8.1.
- Add the p99-under-churn benchmark.
- Validate against the §8.3 matrix.

Phase 1A must not change C ABI lifetime or ownership semantics. The
existing `PreparedStatement` lifetime contract is preserved.

### Phase 1B: Simple-Plan Reuse (`PreparedSimple*` projections)

- Extend the existing `PreparedSimple*` projection types
  (`db.rs:195-274`) so they can be stored in the connection-local cache
  keyed by (SQL text, parameter shape, persistent schema cookie, temp
  schema cookie, policy/mask generation).
- Reuse the existing `prepared_insert_*` helpers (`db.rs:4171-4471`) for
  invalidation; do not duplicate the helper logic.
- Add a Phase 1B benchmark for prepared point lookup, prepared insert
  throughput, and prepared range scan latency.

Phase 1B requires its own sub-ADR (to be written during implementation)
because the planner/executor integration is materially different from
AST reuse. The umbrella ADR 0190 covers the cache shape; the sub-ADR
covers the cacheable-plan-type inventory and the executor integration
test matrix.

### Phase 2: Process-Global Plan Cache

- Add process-global plan cache behind `plan_cache_global_enabled` open
  option.
- Partition cache entries by database identity or fingerprint. Two
  different database files must never share cached entries just because
  SQL text and schema cookies match.
- Share only entries whose validation state is safe across connections.
  Exclude temp-schema-dependent entries unless the planner proves the
  statement resolves only against persistent objects.
- Validate persistent schema generation and policy/mask generation before
  reuse. Temp schema validation remains connection-local.
- Implement process-global LRU eviction with measured synchronization
  overhead. Lock-free or read-copy-update access is optional future
  optimization.
- Add `sys.plan_cache` rows for `scope = 'process'`.
- Add multi-connection benchmarks.
- ADR must address C ABI version implications if process-global caching
  changes how database handles share state.

### Phase 3: Finer-Grained Invalidation

- Track which database objects (tables, indexes, columns) each cached
  entry references. Use the existing `table_touch_generation` per-table
  counter as the natural source of object-level change events.
- Implement object-level invalidation for DDL, `ANALYZE`, and policy
  changes instead of full cache eviction.
- Add `sys.plan_cache` columns for referenced object names (optional,
  behind a diagnostic flag).
- Measure invalidation granularity improvement on workloads with
  frequent DDL.

Phase 3 is a follow-up optimization. Phase 1A must ship with eager full
eviction on DDL and produce correct results. Object-level invalidation
can be added when DDL invalidation cost measurement justifies the
complexity.

### Phase 4: Binding Integration, CLI, And Doctor

- Update binding documentation for plan cache configuration.
- Add binding-level throughput benchmarks (Python first, then Node or
  Dart).
- Add Doctor findings for plan cache hit rate, eviction storms, oversized
  entries, and configurations that suggest tuning.
- Update `docs/user-guide/performance.md` and `docs/api/configuration.md`.

## 10. Definition Of Done

This win is complete (Phases 1A + 1B) only when all of these are true:

- Connection-local plan caching is enabled by default with documented
  memory bounds.
- Prepared-statement throughput improves by at least 30% for repeated
  parameterized point lookups in native benchmarks.
- One-shot query latency regresses by no more than 2% with caching
  enabled.
- p99 latency under churn does not regress compared to the cache-disabled
  baseline.
- Cache hit rate, eviction, and memory use are visible through `sys.*`
  views and the `decentdb plan-cache` CLI subcommands.
- DDL, temp schema changes, `ANALYZE`, branch switch, policy/mask change,
  extension change, and explicit `PRAGMA flush_plan_cache` produce
  correct results.
- Plans cached under one policy/mask generation are never reused under
  a different policy/mask generation.
- `SET AUDIT CONTEXT` does not affect the cache key, and audit-context
  round-trip (set A, prepare, set B, do not execute, set A back, execute)
  reuses the plan from A.
- Rust prepared-statement invalidation semantics remain unchanged unless
  a future ADR explicitly changes them.
- Plan caching does not delay write queue admission or group commit.
- C ABI open options are documented and tested; the C ABI version is
  not bumped for Phase 1.
- Binding documentation covers caching behavior.
- Benchmarks are checked into the release suite with guardrails.
- Doctor reports plan cache findings when the configuration or hit rate
  suggests tuning.

### 10.1 Implementation Results

Delivered in 2.13.0:

- Connection-local parsed AST cache keyed by exact prepared SQL text,
  parameter shape, persistent schema cookie, temp schema cookie, and
  policy/mask generation.
- Connection-local prepared-plan bundle cache for the existing simple DML and
  `PreparedSimple*` read-plan inventory, governed by ADR 0194.
- Default-on 256 KiB total plan-cache budget, split between parsed AST and
  prepared-plan entries, with combined public diagnostics.
- Eager full invalidation for persistent DDL, temp schema changes, `ANALYZE`,
  policy/mask changes, branch changes, extension changes, and explicit flush.
- `SET AUDIT CONTEXT` intentionally does not invalidate cached plans.
- `sys.plan_cache`, `sys.plan_cache_summary`, `sys.doctor_findings` plan-cache
  guidance, `PRAGMA flush_plan_cache`, C ABI diagnostics, C ABI open options,
  and `decentdb plan-cache stats|list|reset`.
- Parsed AST second-use admission for parameterized statements and a
  zero-parameter literal-execution bypass so one-shot SQL does not churn the
  generalized cache.

Final local benchmark evidence:

```text
cargo bench -p decentdb --bench plan_cache
  repeated_prepare_point_lookup: enabled 8.07 us, disabled 12.59 us
  one_shot_query: enabled 2.77 ms, disabled 2.78 ms
  churn_p95_p99: enabled 9.68 us, disabled 25.55 us

cargo run --manifest-path benchmarks/rust-baseline/Cargo.toml \
  --release --bin rust-baseline -- \
  --plan-cache-benchmark --out-dir .tmp/rust-baseline-plan-cache
  repeated_prepare_point_lookup: enabled 564,509 ops/s, disabled 165,490 ops/s
  one_shot_query: enabled 418 ops/s, disabled 425 ops/s
  churn_prepare_p95_p99: enabled 100,368 ops/s, disabled 45,477 ops/s
```

## 11. Compatibility Rules

- Plan caching must not change the results of any query.
- Plan caching must not change ordering guarantees that are not already
  nondeterministic.
- Disabling the plan cache (`plan_cache_enabled=false`) must produce
  identical query results to enabling it.
- The C ABI plan cache open options must be additive: old binaries that
  do not set them must get the default behavior (connection-local caching
  enabled, default max size). This is a deliberate, documented behavior
  change for old C ABI binaries on upgrade. Existing C ABI binaries that
  do not set the options will silently start using the cache; binary
  authors who depend on absolute no-cache behavior must set
  `plan_cache_enabled=false`. This trade-off is justified in ADR 0193
  because the cache is correctness-preserving and the default is the
  right one for nearly every host.
- Rust prepared statements keep their existing stale-schema behavior.
  C ABI or binding wrapper-level auto-reprepare may continue only where
  already provided.
- `PRAGMA flush_plan_cache` is a new `PRAGMA` that does not conflict
  with existing `PRAGMA` names.
- `sys.plan_cache` and `sys.plan_cache_summary` are new views that do
  not change existing `sys.*` views.
- `EXPLAIN` is never served from the cache. `EXPLAIN` always plans
  fresh so users can use it to debug plan changes.

## 12. Risks And Mitigations

| Risk | Mitigation |
|---|---|
| Plan cache adds measurable overhead to one-shot queries | Measure and guardrail one-shot latency; keep cache lookup O(1); allow disabling the cache entirely. |
| Stale plans produce incorrect results after DDL | Eager full eviction on DDL for Phase 1; object-level invalidation in Phase 3. Tests must prove correctness under interleaved DDL and DML. |
| Temp schema mismatch causes a stale object binding | Include temp schema cookie in validation; evict on temp DDL; test temp table shadowing and drop/recreate cases. |
| Policy/mask mismatch causes rows to leak | Policy/mask generation in cache key; invalidation on CREATE/DROP/ALTER POLICY and projection-mask change; test matrix proving non-reuse under different policy generations. |
| Audit-context round-trip bug | Test that audit context changes do not affect the cache key (round-trip reuse) and that audit context changes are observable through `audit_context_snapshot`. |
| Process-global cache introduces cross-connection contention or incorrect sharing | Process-global is Phase 2; require ADR, database identity partitioning, temp-schema exclusions, and measured synchronization overhead. |
| Plan cache memory grows unbounded on embedded hosts | Hard size limit with LRU eviction (ADR 0191); conservative ADR-chosen default; diagnostics for eviction and hit rate. |
| Plan cache interacts incorrectly with TDE page decryption | Plans are logical; they do not contain decrypted content. TDE decryption happens at the page-reader level. Tests must verify TDE-enabled workloads. |
| Plan cache interacts incorrectly with cross-process coordination | Plan cache is process-local. Cross-process DDL invalidation is lazy (next use) and uses the catalog cookie observed via the coordination sidecar (ADR 0177-0180). Tests must prove correct behavior under multiprocess DDL. |
| `schema_cookie: u32` overflow on a long-lived connection doing thousands of DDL operations | The current `u32` cookie in `db.rs:180` is a pre-existing limitation, not introduced by this win. The new `policy_mask_generation` counter is `u32` for symmetry. A future ADR may move either to `u64`. |
| Binding authors implement their own plan caches | Document that the engine owns plan caching; add binding guidance in `docs/api/bindings.md`. |
| C ABI silent default change breaks a host that depended on no-cache behavior | Document the default change; require `plan_cache_enabled=false` opt-out for hosts that need it; ADR 0193 records the trade-off. |
| WASM worker fan-out multiplies plan-cache memory | Document the per-worker budget guidance; default may be too high for multi-worker tabs. |

## 13. Documentation Requirements

Update:

- `docs/user-guide/performance.md` for plan caching defaults,
  configuration, and tuning guidance, including the WASM worker
  budget caveat.
- `docs/api/configuration.md` for plan cache open options, including
  the additive C ABI behavior and the `plan_cache_enabled=false`
  opt-out.
- `docs/api/sql-functions.md` for `sys.plan_cache` and
  `sys.plan_cache_summary` views.
- `docs/api/cli-reference.md` for `PRAGMA flush_plan_cache` and the
  `decentdb plan-cache` subcommands.
- Binding docs for plan caching behavior and configuration.
- `docs/about/changelog.md` on implementation.
