# ADR 0194: Query Plan Cache Prepared-Plan Reuse

**Date:** 2026-06-14
**Status:** Accepted

## Context

ADR 0190 defines the connection-local plan cache key, lifecycle, and
invalidation contract. Phase 1A caches parsed SQL statements. The follow-on
Phase 1B slice needs to reuse executable prepared-statement work without
changing DecentDB's prepared-statement invalidation semantics or introducing
cross-connection sharing.

The existing engine already has reusable prepared-plan structures for simple
DML and read fast paths:

- `PreparedSimpleInsert`
- `PreparedSimpleUpdate`
- `PreparedSimpleDelete`
- `PreparedSimpleRowIdProjection`
- `PreparedSimpleRowIdRangeProjection`
- `PreparedSimpleRowIdJoinProjection`
- `PreparedSimpleScalarFilteredAggregate`

These objects are logical plan state. They do not own row data, decrypted page
content, or open cursors.

## Decision

DecentDB stores a connection-local prepared-plan bundle beside the parsed AST
cache. The prepared-plan cache uses the same key components as ADR 0190:

- exact prepared SQL text
- parameter shape
- persistent schema cookie
- temp schema cookie
- policy/mask generation

The bundle contains the parsed statement, the applicable `PreparedSimple*`
objects, simple DML plans, and the read-only flag needed to construct a fresh
`PreparedStatement` on cache hit.

On `Db::prepare(...)`, DecentDB checks the prepared-plan cache before opening
an execution runtime when no explicit SQL transaction is active. On a hit it
returns a new `PreparedStatement` backed by the cached bundle. On a miss it
prepares normally, then stores the bundle if the statement category is
cacheable.

The prepared-plan cache:

- is connection-local only;
- is bounded by the configured plan-cache memory budget;
- participates in the same eager invalidation sink as the parsed cache;
- validates schema cookies and policy/mask generation before reuse;
- resets counters only on explicit flush;
- does not change Rust prepared-statement stale-plan behavior.

The configured `plan_cache_max_bytes` budget is split between the parsed AST
cache and the prepared-plan cache. The public summary reports the combined
budget, size, entries, hits, misses, evictions, and oversized-entry refusals.

## Consequences

Repeated `Db::prepare(...)` calls for the same parameterized statement can skip
parse, resolve, and simple-plan construction work. This is the primary
performance target for ORM and binding workloads that create prepared
statements frequently.

One-shot literal SQL must not pay heavy cache churn costs. Parsed AST admission
therefore uses second-use admission for parameterized statements and bypasses
the generalized parsed cache for zero-parameter literal execution, leaving
those statements to the existing narrow parser cache.

The design does not introduce any on-disk format, WAL format, C ABI version,
or cross-process coordination change.

## Validation

The implementation is validated by:

- unit tests for hit/miss accounting, memory-bound eviction, oversized-entry
  refusal, audit-context reuse, DDL invalidation, explicit flush, and Doctor
  projection;
- `cargo bench -p decentdb --bench plan_cache`, covering repeated prepare,
  one-shot overhead, and warm churn p95/p99;
- `benchmarks/rust-baseline --plan-cache-benchmark`, which writes a JSON
  enabled-vs-disabled report for the same guardrail scenarios.
