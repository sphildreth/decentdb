# Query Plan Cache: Scope, Key, Invalidation, And Lifecycle

**Date:** 2026-06-13
**Status:** Accepted
**Related spec:** [`../WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md`](../WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md)
**Companion ADRs:** [0191](./0191-query-plan-cache-memory-accounting-and-eviction.md), [0192](./0192-query-plan-cache-security-generation-and-tde.md), [0193](./0193-query-plan-cache-c-abi-surface-and-binding-contract.md)

## Decision

DecentDB will ship a connection-local, in-memory plan cache that reuses
parsed statements and the concrete reusable plan objects the engine already
owns. The cache is keyed by (SQL text, parameter shape, persistent schema
cookie, temp schema cookie, policy/mask generation) and is invalidated
eagerly on DDL, branch operations, extension changes, and explicit flush.
Cross-connection, cross-process, persistent, and adaptive optimizer
behaviors are explicitly out of scope for this ADR.

The cache is enabled by default for new `Db` handles, sized by a
configurable byte budget governed by ADR 0191, exposed through new
`sys.plan_cache` and `sys.plan_cache_summary` views, controllable via
`PRAGMA flush_plan_cache` and a new `decentdb plan-cache` CLI surface,
and surfaced through the C ABI as additive `plan_cache_enabled` and
`plan_cache_max_bytes` open options (no C ABI version bump; see
ADR 0193).

The cache replaces the current narrow `StatementCache`
(`crates/decentdb/src/db.rs:666-731`) and `PreparedInsertCache`
(`crates/decentdb/src/db.rs:740-786`) only by *generalizing* them: the
existing LRU shape, capacity defaults, and call sites stay; the new
behavior adds schema, temp, and policy/mask keys plus a single
`PlanCacheInvalidator` trait that DDL, sync, and policy code paths call
into. No prepared-statement lifetime or ownership semantics change.

## Rationale

Prepared-statement throughput is one of the largest unforced performance
gaps between DecentDB and SQLite/PostgreSQL for high-throughput
embedded workloads. ORMs and binding layers execute the same
parameterized queries thousands of times per session. Today, every
execution re-parses, re-resolves, and re-plans the statement; the
existing `StatementCache` and `PreparedInsertCache` are too narrow to
help the general case. Without a generalized plan cache, DecentDB's
prepared-statement p99 latency has a measurable floor that binding
layers cannot work around, and DecentDB cannot credibly claim
prepared-statement parity with SQLite and PostgreSQL.

ADR 0184 explicitly permits plan caching keyed by schema cookie, temp
schema cookie, and statistics generation, and explicitly requires
invalidation on catalog, temp schema, policy/mask, and extension
changes. This ADR is the implementation commitment to that permission
and the boundary that ADR 0184 deliberately left open.

The decision to make the cache connection-local, in-memory, and
non-adaptive is deliberate. The DecentDB identity is a durable
embedded engine, not a server. Adaptive query optimization, plan
hints, persistent plan caches, and cross-connection plan sharing are
all attractive in the abstract, but each one widens the cache's
correctness surface, its memory budget, and its binding contract.
This ADR scopes the cache to the smallest unit that delivers the
biggest performance win: same SQL text, same connection, same
schema, same policy state, repeated execution.

The decision to invalidate eagerly on DDL and branch operations
(Phase 1A) is also deliberate. Finer-grained object-level
invalidation is correct and faster, but it is materially more complex
and it can be added later without changing the public contract. The
Phase 1A contract is "DDL, branch, extension, explicit flush, or
policy/mask change ⇒ evict all." That contract is trivially correct
and the DDL cost of full eviction is small relative to the DDL cost
itself (see §"DDL overhead" guardrail in the spec).

## Cache key

The cache key is the tuple:

```
(sql_text, parameter_shape, persistent_schema_cookie, temp_schema_cookie, policy_mask_generation)
```

- `sql_text` is the exact SQL text. Parser-stable canonical forms are a
  follow-up once the parser exposes a stable canonicalizer.
- `parameter_shape` is the arity plus the SQL type class of each
  placeholder (`INTEGER`, `REAL`, `TEXT`, `BLOB`, or `NULL`). Type
  class is the column affinity / declared parameter type, never the
  runtime value. Two queries with placeholders that resolve to the
  same shape class share an entry.
- `persistent_schema_cookie` is the connection-local current cookie
  (`Db::schema_cookie()` at `crates/decentdb/src/db.rs:3257`), not a
  snapshot cookie. Snapshot-isolated readers must revalidate against
  their snapshot cookie separately at execution time; that revalidation
  is unchanged by this ADR.
- `temp_schema_cookie` is the connection-local temp schema cookie.
- `policy_mask_generation` is a `u32` counter maintained in `DbInner`,
  bumped on every `CREATE POLICY`, `DROP POLICY`, `ALTER POLICY`, and
  every projection-mask change. It is distinct from the schema cookie
  and is governed by ADR 0192.

**Deliberately not in the key:** audit context values, TDE state,
`table_touch_generation` per-table counters, statistics generations,
branch/snapshot/LSN identity, and database identity. Each exclusion
is justified in §5.2 of the spec; the audit-context exclusion is
specifically justified in ADR 0192.

## Invalidation surface

The cache exposes a single internal trait:

```rust
pub(crate) trait PlanCacheInvalidator: Send {
    fn on_persistent_ddl(&self);
    fn on_temp_schema_change(&self);
    fn on_analyze(&self, _table: &str) { self.on_persistent_ddl(); }
    fn on_policy_mask_change(&self);
    fn on_branch_switch(&self);
    fn on_extension_change(&self);
    fn on_explicit_flush(&self);
}
```

`DbInner` implements this trait and dispatches to the cache. Existing
DDL, sync, policy, branch, and extension call sites are updated to
invoke the corresponding method. `SET AUDIT CONTEXT` is **not** in
the invalidator; that decision is in ADR 0192.

The invalidator is the only mechanism that evicts entries. There is
no second eviction path, no time-based expiry, and no "soft"
invalidation. This keeps the contract boring: "if a relevant event
happened, the entry is gone."

## Cross-process invalidation

Cross-process DDL is observed via the existing coordination sidecar
(ADR 0177-0180). When process B reads process A's DDL by observing a
new schema cookie, process B's plan cache validates the cached
persistent cookie against the current cookie on the next cache hit
and evicts the entry on mismatch. This is the same lazy-validation
pattern already used by `PreparedStatement`; the plan cache reuses
it.

DML-only sync changeset apply does **not** invalidate the cache: the
catalog cookie is unchanged and plan shape is unaffected. DDL
changeset apply follows the same path as local DDL.

## Phasing

| Phase | Scope | ADR needed |
|---|---|---|
| 1A | Generalize `StatementCache` to key by (sql, parameter_shape, persistent, temp, policy/mask); add `PlanCacheInvalidator`; add `sys.*` views; add `PRAGMA`; add CLI; add C ABI open options. | This ADR (0190) |
| 1B | Cache `PreparedSimple*` projection plans (`db.rs:195-274`) keyed the same way. | Sub-ADR (to be written during implementation) |
| 2 | Process-global cache with database identity partitioning. | Future ADR (out of scope here) |
| 3 | Object-level invalidation using `table_touch_generation`. | Future ADR (out of scope here) |
| 4 | Binding integration, CLI surface, Doctor findings. | Doc-only work |

## Alternatives considered

1. **Make the cache process-global from the start.** Rejected.
   Cross-connection plan sharing requires database identity
   partitioning, temp-schema exclusions, and a synchronization
   strategy that does not exist today. Connection-local caching
   delivers the highest-ROI subset of the win with the smallest
   contract surface.
2. **Make the cache persistent across restarts.** Rejected. A
   persistent plan cache requires a serialization format, a format
   version, a migration parser per ADR 0131, and a security review of
   what the persisted plan could leak about a database's contents.
   None of that is justified for a v1 win.
3. **Add adaptive plan selection as part of the cache.** Rejected.
   Adaptive selection is a separate product surface with its own
   correctness contract. This ADR caches the *same* plan that would
   be produced without a cache.
4. **Cache only DML, never reads.** Rejected. DML is the
   highest-throughput prepared-statement case but reads are the
   most-issued case for ORMs and binding layers. The Phase 1B
   beachhead is the existing `PreparedSimple*` projection types,
   which are already designed for reuse.
5. **Ship a `plan_cache_enabled` open option that defaults to
   `false`.** Rejected. Default-off makes the cache invisible to the
   most common workloads and defeats the perf win; ADR 0193
   documents the trade-off for default-on and the additive C ABI
   behavior.
6. **Reuse the existing `StatementCache` and `PreparedInsertCache`
   data structures verbatim and add a second cache alongside them.**
   Rejected. Two parallel caches will drift. The right shape is to
   generalize the existing structures; the new behavior is
   superset-compatible.

## Trade-offs

- **Eager full eviction on DDL is wasteful for large caches with
  little DDL activity.** Accepted for v1; Phase 3 introduces
  object-level invalidation.
- **Connection-local only.** Cross-connection workloads that open many
  `Db` handles do not benefit. Process-global is Phase 2 and
  requires its own ADR.
- **`u32` schema cookie and `u32` policy/mask generation.** A
  long-lived connection doing thousands of DDL operations could
  theoretically wrap. The current `u32` cookie at
  `crates/decentdb/src/db.rs:180` is a pre-existing limitation; this
  ADR does not change it. A future ADR may move either field to
  `u64` and must address the migration cost.
- **The cache adds a tiny amount of always-on overhead to one-shot
  queries.** Measured and bounded by the §8.2 guardrail
  (≤ 2% p95 regression). The trade-off is accepted because the
  high-throughput case is the binding-driven default.
- **`EXPLAIN` is never cached.** This costs a small amount of
  repeated planning for users who call `EXPLAIN` in tight loops,
  but the alternative (a separate `PRAGMA` to opt out) is a worse
  user experience than just always planning fresh.

## Consequences

- `DbInner` grows a `plan_cache: Mutex<PlanCache>` field and a
  `policy_mask_generation: AtomicU32` field.
- The existing `StatementCache` and `PreparedInsertCache` are
  generalized, not duplicated. The `PreparedStatement` struct
  (`db.rs:177-192`) is unchanged.
- DDL, sync, policy, branch, and extension call sites call into
  `PlanCacheInvalidator` at the points listed in the spec's
  §5.3. Adding new call sites is a follow-up engineering task with
  a small blast radius.
- `sys.plan_cache` and `sys.plan_cache_summary` views are added
  through the existing parsed-SQL dispatch path (see
  `db.rs:13390-13469`); the new variants follow the
  `SyncInspectionQuery` enum pattern.
- `PRAGMA flush_plan_cache` is a new PRAGMA dispatched through the
  existing PRAGMA parser.
- The `decentdb plan-cache stats|list|reset` CLI subcommands are
  added in `crates/decentdb-cli`.
- The C ABI open-option parser is extended with
  `plan_cache_enabled` and `plan_cache_max_bytes`. No C ABI version
  bump for Phase 1.
- All maintained bindings that depend on prepared statements get
  the cache benefit transparently; bindings that wrap the C ABI
  with their own auto-reprepare logic continue to work as today.

## References

- `design/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md`
- `design/FUTURE_WINS.md`
- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
- `design/adr/0162-engine-owned-write-queue-strict-group-commit.md`
- `design/adr/0163-operational-sys-metrics.md`
- `design/adr/0174-local-data-security-tde-policies-masking-audit-context.md`
- `design/adr/0177-cross-process-coordination-sidecar-and-locking.md`
- `crates/decentdb/src/db.rs:666-731` (existing `StatementCache`)
- `crates/decentdb/src/db.rs:740-786` (existing `PreparedInsertCache`)
- `crates/decentdb/src/db.rs:195-274` (`PreparedSimple*` projection
  types)
- `crates/decentdb/src/db.rs:3257` (`Db::schema_cookie` accessor)
- `crates/decentdb/src/db.rs:13390-13469` (existing parsed-SQL
  dispatch path for `sys.*` views)
- `docs/user-guide/performance.md`
- `docs/api/configuration.md`
