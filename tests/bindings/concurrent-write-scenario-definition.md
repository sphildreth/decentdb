# Concurrent Write Scenario Definition

This document defines the cross-binding concurrent write acceptance shape that
bindings use to validate engine-owned queued writes.

## Objective

Prove that all bindings can execute many concurrent small write transactions without
requiring ad-hoc host-level serialization in user code, while preserving current
direct-path correctness and durability guarantees.

## Current Scope

- The concurrent-write benchmark entry points and smoke tests are contract text
  for every binding.
- Bindings should use explicit queued helpers where available and C ABI queued
  execution where the high-level provider has not yet grown automatic queued
  prepared-statement execution.
- Direct-path variants remain valid regression coverage because direct writes
  intentionally remain first-class.

## Scenario: `concurrent_writes_smoke`

### Setup

- Create table `bench_concurrent_writes (id INT64 PRIMARY KEY, tenant INT64, payload TEXT NOT NULL)`.
- Seed the table with 1-2 rows to verify both inserts and conflict handling.
- Create one logical writer per worker. Bindings may route those workers through
  one shared queued handle, a provider-level queue mode, or the C ABI queued
  helper depending on the binding's ownership model.

### Workload

- Use **N=8** concurrent workers.
- Each worker runs **100** transactions.
- Every transaction inserts one row with deterministic keying:
  `id = base + worker_id * 1000 + iteration`.
- Writes use the binding's queued self-contained SQL path where available. If a
  binding only exposes queued execution through the C ABI today, the smoke may
  call that ABI directly and keep provider prepared statements on the direct
  path.

### Assertions

- Run completes with all worker tasks finishing (or bounded worker timeout).
- Final row count equals initial count + 800 and no duplicate-key failures for generated IDs.
- Reopen and re-query by `count(*)` and a random sample subset to verify durability.
- The test may execute reads after completion only; no concurrent read load is required
  in this smoke scenario.

## Scenario: `concurrent_writes_with_readers`

### Setup

- Same schema as smoke.
- Start **4** concurrent readers once writers begin.
- Readers run point lookups against a fixed key set.

### Workload

- Same worker model as smoke.
- Readers and writers run for the same fixed wall-clock window.

### Assertions

- Smoke assertions above.
- Read path remains bounded (no hard crash or deadlock).
- Final table size and sampled key lookups match expected committed writes.

## Cross-binding expected output contract

- Scenario names and key fields should be logged in a per-binding report artifact.
- Result shape for each run:
  - `scenario`: scenario identifier
  - `status`: `passed | failed`
  - `attempted_writes`
  - `successful_writes`
  - `row_count_after_reopen`
  - `elapsed_ms`
  - `errors_by_type`

## Backward Compatibility Path

Direct API usage remains acceptable as a control scenario. Shared lock behavior
and single-writer semantics should match existing engine guarantees, and the
same row-count/durability assertions apply.
