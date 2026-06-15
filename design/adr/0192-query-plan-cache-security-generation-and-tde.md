# Query Plan Cache: Security Generation, Policy/Mask Invalidation, Audit Context Boundary, And TDE

**Date:** 2026-06-13
**Status:** Accepted
**Related spec:** [`../WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md`](../WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md)
**Companion ADRs:** [0190](./0190-query-plan-cache-scope-key-and-lifecycle.md), [0191](./0191-query-plan-cache-memory-accounting-and-eviction.md), [0193](./0193-query-plan-cache-c-abi-surface-and-binding-contract.md)

## Decision

The connection-local plan cache treats row policies, projection masks,
TDE state, and audit context as four distinct categories and handles
each one explicitly:

1. **Row policies and projection masks** participate in the cache
   key as a single `u32` counter called `policy_mask_generation`,
   maintained in `DbInner` and bumped on every event that changes
   the visible-row or visible-column semantics of a query.

2. **TDE state** does **not** participate in the cache key. TDE
   is enforced at the page-reader level; cached plans are
   logical execution plans and contain no decrypted page content.
   TDE is *observable* through the existing
   `Db::tde_enabled` accessor and the `sys.tde_metrics` view
   and is a binding-level concern, not a plan-cache-key concern.

3. **Audit context values** do **not** participate in the cache
   key. `SET AUDIT CONTEXT` writes a row to the
   `__decentdb_audit_events` table and updates the
   `Arc<Mutex<AuditContext>>` on the connection
   (`crates/decentdb/src/db.rs:513`,
   `db.rs:3225-3240`,
   `db.rs:3628-3634`,
   `db.rs:3732-3733`),
   but it does not change which rows a query can see or which
   columns a query can return. The plan cache reuses a cached
   plan across audit-context changes; this is correct.

4. **Policy and mask change events** call
   `PlanCacheInvalidator::on_policy_mask_change` (defined in
   ADR 0190), which evicts all entries in the connection-local
   cache. Audit-context change events do **not** call any
   cache method.

5. **The audit context remains an observable.** A binding or
   operator can read the current audit context through
   `Db::audit_context_snapshot()` and can query
   `__decentdb_audit_events` directly. The plan cache is not
   the source of truth for audit observability.

## Rationale

The four categories look superficially similar (they are all
"security-adjacent state") and a naive plan cache could lump them
together under a single "security generation" counter. That would
be wrong in two directions:

- **Lumping in audit context** would force the cache to evict on
  every `SET AUDIT CONTEXT` call. Audit context is the most
  frequently mutated security-adjacent state in a busy
  multi-tenant or multi-actor application. Forcing full cache
  eviction on every write would destroy the cache's hit rate on
  the workloads that benefit most from it, and would do so
  without any correctness benefit: the audit context does not
  change query results.

- **Lumping out policies and masks** would let cached plans
  outlive a `DROP POLICY` and return rows that should now be
  hidden. This is a real correctness failure mode and the
  historical worst case for plan caches in any database that
  ships row-level security.

The split also matches the codebase. Looking at the existing
call sites:

- `db.rs:2192-2193` — `parse_set_audit_context` is dispatched
  through the security module, but the executor at
  `db.rs:3732-3733` only writes the audit event and updates the
  `Arc<Mutex<AuditContext>>`; it does not change plan shape.
- `db.rs:3628-3634` — `execute_set_audit_context` calls
  `set_audit_context_value` / `clear_audit_context_value`,
  neither of which changes row visibility.
- The actual row-visibility-relevant policy DDL goes through
  the security module's `parse_create_policy`,
  `parse_drop_policy`, and `parse_alter_policy` at
  `crates/decentdb/src/security.rs:171-195`, which is a
  separate code path.

So the codebase already separates "policy/mask DDL" from "audit
context writes." This ADR aligns the plan cache with that
separation.

The audit-context exclusion is also deliberately *visible*: the
spec's §10 Definition of Done includes a round-trip test that
proves audit context changes do not evict the cache. If a future
change ever makes audit context affect row visibility, that
change must come with its own ADR that adds audit context to the
cache key — and the round-trip test will fail at that point,
which is the desired signal.

## Policy/mask generation

A single `AtomicU32` counter on `DbInner`, named
`policy_mask_generation`. It is bumped on:

- every `CREATE POLICY` execution that succeeds,
- every `DROP POLICY` execution that succeeds,
- every `ALTER POLICY` execution that succeeds,
- every projection-mask change (the mask surface is currently
  the same DDL family and bumps the same counter).

The counter is read whenever a cache entry is created or
validated. The counter does not need to be persisted; it is
rebuilt on connection open from the catalog's policy and mask
state. The initial value on a fresh connection is the count of
policy DDL operations recorded in the catalog metadata, which
is monotone per the catalog format. If a future change makes
the initial-value calculation non-trivial, that change requires
its own ADR.

The counter is `u32` for symmetry with the existing
`schema_cookie: u32` field at `db.rs:180`. Overflow on a
long-lived connection doing millions of policy DDL operations
is theoretically possible but is a pre-existing concern (the
`schema_cookie` has the same shape) and is not addressed by
this ADR. A future ADR may move either to `u64` and must
address the migration cost.

## TDE

TDE v1 (ADR 0174) is enforced at the page-reader level. Cached
plans do not contain page contents and cannot bypass TDE
because they never hold plaintext. TDE state (enabled,
key-id, key-version) is observable through `Db::tde_enabled`
and the `sys.tde_metrics` view, but does not change which
plan is selected for a given SQL text. Therefore TDE is not
in the cache key.

The risk row in the spec's §12 ("Plan cache interacts
incorrectly with TDE page decryption") is preserved as a
test obligation: TDE-enabled workloads must be exercised by
the §8.3 validation matrix, and a regression test must prove
that the cache does not bypass decryption.

## Audit context

`SET AUDIT CONTEXT <key> = <value>` and
`SET AUDIT CONTEXT <key> = NULL` go through
`db.rs:3628-3634` → `set_audit_context_value` /
`clear_audit_context_value` at `db.rs:3225-3240`. These methods
update the `Arc<Mutex<AuditContext>>` and (for non-NULL values)
write an audit event to `__decentdb_audit_events`. Neither
method changes query results.

The cache therefore:

- does not include the audit context in the cache key;
- does not call `PlanCacheInvalidator` on audit-context writes;
- continues to reuse a cached plan across audit-context
  changes within the same connection.

The round-trip test in the spec's §10 DoD enforces this: a test
sets audit context A, prepares a query, sets audit context B,
does not execute, sets audit context A back, and executes. The
plan from A must be reused. The test is the executable
guarantee of this ADR.

## Binding-level audit-context exposure

Bindings that want to surface audit context to applications
should continue to use the existing `Db::audit_context_snapshot`
method and the `__decentdb_audit_events` table. Bindings must
not assume that a `SET AUDIT CONTEXT` call invalidates cached
plans; the plan cache is correctness-preserving across audit
context changes, and bindings that promise otherwise are
promising something the engine does not promise.

## Alternatives considered

1. **Single "security generation" counter covering policies,
   masks, and audit context.** Rejected. Forces the cache to
   evict on every audit-context write, which destroys the
   cache's hit rate on multi-actor applications and provides
   no correctness benefit.
2. **No security state in the cache key; rely on the schema
   cookie to catch policy/mask changes.** Rejected. The
   schema cookie is bumped on policy DDL (because policy DDL
   is DDL), so it would catch policy changes, but it would
   also catch a much broader set of events than necessary and
   would couple policy invalidation to the schema cookie's
   invalidation surface. A dedicated counter is cleaner and
   easier to test.
3. **Hash the entire policy/mask DDL history into the key.**
   Rejected. The key would grow without bound and the hash
   would not be stable across format versions.
4. **Include the audit context in the cache key as a
   `BTreeMap<String, Value>` hash.** Rejected. The audit
   context is mutable, connection-local, and does not affect
   plan shape. Including it would force eviction on every
   audit-context write, exactly the failure mode the split
   prevents.
5. **Treat TDE key-id and key-version as cache key
   components.** Rejected. TDE is enforced at the page-reader
   level; cached plans contain no plaintext. Key rotation
   does not change plan shape and is observable through
   `sys.tde_metrics` independently of the plan cache.

## Trade-offs

- **The audit context exclusion is a correctness claim.** If a
  future change makes `SET AUDIT CONTEXT` affect row visibility
  (e.g., a future "policy based on actor claim" feature), that
  change must be paired with an ADR that adds the audit context
  to the cache key. The round-trip test in the spec's DoD will
  fail at that point, which is the desired signal.
- **The policy/mask generation counter is monotone, not a
  hash.** This means a `CREATE POLICY` followed by a `DROP
  POLICY` leaves the counter at 2, not 0. That's fine: the
  counter is a "things have changed" signal, not a content
  fingerprint.
- **The counter is `u32`, not `u64`.** Same overflow
  concern as the existing `schema_cookie`. A future ADR may
  move either to `u64`.

## Consequences

- `DbInner` grows a `policy_mask_generation: AtomicU32` field
  initialized from the catalog at connection open.
- Policy DDL call sites (`parse_create_policy`,
  `parse_drop_policy`, `parse_alter_policy`,
  `set_audit_context_value`, `clear_audit_context_value`) are
  instrumented: the first three bump
  `policy_mask_generation` and call
  `PlanCacheInvalidator::on_policy_mask_change`; the last two
  do neither.
- A new round-trip test in
  `crates/decentdb/src/plan_cache_tests.rs` enforces the
  audit-context exclusion.
- A new test in the same module enforces the policy/mask
  inclusion: a `CREATE POLICY` followed by a query
  revalidation must evict the cached plan.
- A new TDE-enabled regression test in the same module
  exercises the cache under TDE and asserts the plan is
  reused and decryption is still applied.
- The spec's §10 DoD list grows two entries:
  "Plans cached under one policy/mask generation are never
  reused under a different policy/mask generation" and
  "Audit context changes do not affect the cache key, and
  the round-trip test passes."

## References

- `design/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md` §5.7,
  §8.3, §10
- `design/adr/0190-query-plan-cache-scope-key-and-lifecycle.md`
  (`PlanCacheInvalidator` trait)
- `design/adr/0174-local-data-security-tde-policies-masking-audit-context.md`
  (TDE v1 boundary; security DDL surface)
- `crates/decentdb/src/db.rs:180` (existing `schema_cookie: u32`)
- `crates/decentdb/src/db.rs:513` (existing audit context storage)
- `crates/decentdb/src/db.rs:2192-2193` (`parse_set_audit_context` dispatch)
- `crates/decentdb/src/db.rs:3225-3240` (`set_audit_context_value` /
  `clear_audit_context_value`)
- `crates/decentdb/src/db.rs:3628-3634` (`execute_set_audit_context`)
- `crates/decentdb/src/db.rs:3732-3733` (audit event write site)
- `crates/decentdb/src/security.rs:171-195` (policy DDL parsers)
- `docs/user-guide/security.md`
- `docs/api/error-codes.md`
