# SQLite Size Gap Resolution Plan (V2)

**Date:** 2026-01-31  
**Status:** In progress (Phases 1–2 implemented; remaining work tracked below)  
**Supersedes:** [design/SQLITE_GAPS_PLAN.md](design/SQLITE_GAPS_PLAN.md) (older draft contains C-centric pseudocode that does not match this repo)  
**Primary reference:** [design/SQLITE_GAPS.md](design/SQLITE_GAPS.md)

## Goals

- Reduce SQLite→DecentDB file size gap (currently ~2–3× on the sample dataset).
- Preserve DecentDB “North Star” priorities:
  - Durable ACID writes
  - Fast reads
- Keep MVP concurrency semantics unchanged (single process; one writer; many readers).

## Non-goals (explicitly out of scope without a new ADR)

- Changing WAL frame formats, checkpointing strategy, or isolation semantics.
- Adding major dependencies.
- Expanding SQL dialect (new DDL syntax) as part of “size work”.

## Current reality (code & formats)

These are the actual on-disk formats that matter for the size gap today:

- Global file format version gate: [src/pager/db_header.nim](src/pager/db_header.nim) defines `FormatVersion` and the engine rejects any DB whose header `formatVersion != FormatVersion`.
- Record encoding: [src/record/record.nim](src/record/record.nim)
  - Record = `[count varint][value…]`.
  - Value = `[kind u8][len varint][payload bytes]` (per [design/adr/0030-record-format.md](design/adr/0030-record-format.md)).
  - INT64 payload uses **ZigZag + varint** encoding (per [design/adr/0034-compact-int64-record-payload.md](design/adr/0034-compact-int64-record-payload.md)).
- B+Tree page layout: [src/btree/btree.nim](src/btree/btree.nim)
  - Leaf/internal cells use a **compact varint-based layout** (per [design/adr/0035-btree-page-layout-v2.md](design/adr/0035-btree-page-layout-v2.md)).
  - This reduces per-cell overhead and increases fan-out vs the prior fixed-width header.

Related implemented behaviors that affect the size gap:

- Integer PRIMARY KEY optimization: [design/adr/0036-integer-primary-key.md](design/adr/0036-integer-primary-key.md)
  - `INT64 PRIMARY KEY` can be used as the table btree key (rowid) directly, avoiding a redundant PK index.
- Optional value compression exists (storage-internal; no SQL surface area) via [design/adr/0048-optional-value-compression.md](design/adr/0048-optional-value-compression.md).

## Phase 0: Operational & documentation (done)

This phase is already addressed in [design/SQLITE_GAPS.md](design/SQLITE_GAPS.md):

- WAL and rebuild-index bloat pitfalls documented.
- Vacuum/checkpoint guidance documented.

No code work required here.

## Phase 1: Compact INT64 in record payloads (ADR required)

### Status

Implemented (see [design/adr/0034-compact-int64-record-payload.md](design/adr/0034-compact-int64-record-payload.md)).

### Why

This is the highest-impact structural fix for ID-heavy schemas:

- Previously: each INT64 consumed 8 bytes (+ 1 kind byte + 1+ length varint).
- Current: small integers commonly consume ~1–3 bytes in payload (plus outer record framing).

### ADR notes

This required a persistent-format change and is tracked/justified by ADR 0034.

### Encoding options (must pick one in ADR)

- Option A (minimal change): `payload = encodeVarint(cast[uint64](int64Val))`.
  - Pros: simple; reuses existing `encodeVarint`.
  - Cons: negative ints almost always encode “large” (many bytes).
- Option B (recommended for generality): zigzag + varint.
  - Define `zigzag(x:int64): uint64 = (uint64(x) shl 1) xor uint64(x shr 63)`.
  - Payload = `encodeVarint(zigzag(int64Val))`.
  - Pros: small negative and small positive both compact.
  - Cons: must update spec/tests; affects ordering only if used outside record payload.

Important: DecentDB’s current varint implementation is LEB128-style over `uint64` and has a **max encoded length of 10 bytes** for full `uint64`.

### Implementation surface

- Update `vkInt64` in `encodeValue`/`decodeValue` in [src/record/record.nim](src/record/record.nim).
- Update any other code paths that assume `vkInt64` payload len must be 8.
- Update documentation describing record formats if needed (e.g., [docs/architecture/storage.md](docs/architecture/storage.md) if it is intended to be authoritative).

### Tests

- Unit tests (extend existing suites rather than adding perf gates):
  - Round-trip for boundary values (0, 1, 127, 128, 16383, …, int64.high, int64.low).
  - Ensure `encodeValue(vkInt64, small)` produces payload length < 8.
  - Corruption tests: truncated varint, overflow varint.
- Crash-injection tests:
  - Insert values that exercise 1-, 2-, 3-, and 10-byte encodings, crash at various points, verify recovery.
- Backward compatibility fixture test:
  - Commit a small “golden” vCurrent-1 database file into `tests/data/` (or generated deterministically) and assert it opens correctly.

Avoid: perf thresholds like “>1M ops/sec” (too flaky across CI/machines).

## Phase 2: Reduce B+Tree leaf overhead (ADR required)

### Status

Implemented (see [design/adr/0035-btree-page-layout-v2.md](design/adr/0035-btree-page-layout-v2.md)).

### Why

SQLite’s btree layout is extremely space efficient. DecentDB’s fixed per-cell headers contribute to page count and file size.

### ADR notes

This required a persistent-format change and is tracked/justified by ADR 0035.

### Candidate improvements (choose a coherent subset)

- Encode `value_len` as a varint instead of `u32`.
- Store `overflow_page` only when needed:
  - e.g., a flag bit in a small header, then conditionally include `overflow_page`.
- Move from “sequential cells” to a slot directory layout (enables denser packing and easier in-page compaction).
- (If/when ordered TEXT keys become real keys) consider prefix compression with restart points. Note: current btree keys are `uint64` in [src/btree/btree.nim](src/btree/btree.nim), so TEXT key prefix compression is not directly applicable today.

### Tests

- Round-trip encode/decode of leaf pages across a wide range of value sizes.
- Page split/merge invariants (no lost keys, no orphaned overflow chains).
- Crash tests that exercise:
  - leaf split
  - overflow chain allocation/free
  - rebuild-index

## Phase 3: Eliminate PK/rowid redundancy (ADR required)

### Status

Partially implemented:

- `INT64 PRIMARY KEY` uses the table btree key directly (see [design/adr/0036-integer-primary-key.md](design/adr/0036-integer-primary-key.md)).

Remaining work in this phase is about reducing additional redundancy and write amplification beyond the integer-PK case.

Next ADR to unlock Phase 3 work:

- [design/adr/0049-constraint-index-deduplication.md](design/adr/0049-constraint-index-deduplication.md) (proposed)

### Why

If we can avoid duplicating “row identity” across a table btree and a separate PK/UNIQUE btree in the common cases, size drops and write amplification improves.

### ADR scope

- Define which PK/UNIQUE cases can be represented without a separate index.
- Define catalog metadata changes (if any) and how existing DBs migrate.
- Confirm correctness with foreign keys and snapshot isolation.

### Tests

- Constraint enforcement: PK/UNIQUE + FK RESTRICT/NO ACTION.
- Differential tests vs Postgres for supported SQL subset where applicable.
- Crash tests around constraint enforcement and index maintenance.

## Phase 4: Optional compression

### Status

Implemented (storage-internal; no SQL surface) and covered by unit tests.

### Remaining work

- Ensure ADR 0048 is consistent with the implemented behavior and current `FormatVersion` gating.
- Decide whether compression remains “always opportunistic” or becomes explicitly opt-in (this is an API/behavior decision and should be ADR’d if exposed).

## Size regression harness (required)

### Status

Implemented (see [tests/nim/test_size_regression.nim](tests/nim/test_size_regression.nim)).

### Requirements

Maintain a deterministic, in-repo size regression test that:

- Generates a synthetic dataset with:
  - many small ints
  - mixed text
  - multiple indexes
- Compares:
  - `.ddb` size after checkpoint + vacuum
  - page_count/freelist_count stats

Avoid hard “must be < X MB” for external real DBs; prefer relative improvements and invariants.

## Definition of Done (per repo)

- ADR(s) accepted for any persistent-format change.
- Unit + crash-injection tests for new formats.
- Narrow performance measurements as evidence (no flaky pass/fail perf gates).
- SPEC/ADR/docs updated where behavior or formats change.
