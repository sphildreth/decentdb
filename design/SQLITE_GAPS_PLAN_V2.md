# SQLite Size Gap Resolution Plan (V2)

**Date:** 2026-01-31  
**Status:** Draft (ADR-first; Nim implementation plan)  
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

- Global file format version gate: [src/pager/db_header.nim](src/pager/db_header.nim) defines `FormatVersion` and the engine currently rejects any DB whose header `formatVersion != FormatVersion`.
- Record encoding: [src/record/record.nim](src/record/record.nim)
  - Record = `[count varint][value…]`.
  - Value = `[kind u8][len varint][payload bytes]` (per [design/adr/0030-record-format.md](design/adr/0030-record-format.md)).
  - **INT64 payload is currently fixed 8 bytes little-endian**, which is the primary “small ints waste space” issue called out in [design/SQLITE_GAPS.md](design/SQLITE_GAPS.md).
- B+Tree page layout: [src/btree/btree.nim](src/btree/btree.nim)
  - Leaf cell header is fixed-width: `key u64 + value_len u32 + overflow_page u32` (16 bytes) plus inline value bytes (per [design/adr/0032-btree-page-layout.md](design/adr/0032-btree-page-layout.md)).
  - This fixed 16-byte-per-cell overhead contributes to “not as tightly packed as SQLite”, especially for small rows and/or many secondary index entries.

## Phase 0: Operational & documentation (done)

This phase is already addressed in [design/SQLITE_GAPS.md](design/SQLITE_GAPS.md):

- WAL and rebuild-index bloat pitfalls documented.
- Vacuum/checkpoint guidance documented.

No code work required here.

## Phase 1: Compact INT64 in record payloads (ADR required)

### Why

This is the highest-impact structural fix for ID-heavy schemas:

- Today: each INT64 consumes 8 bytes (+ 1 kind byte + 1+ length varint).
- Target: small integers consume ~1–3 bytes in payload.

### ADR requirements

Create a new ADR before any implementation:

- Define **the exact encoding for `vkInt64` payload**.
- Define **format versioning**: how `FormatVersion` changes, and what versions are readable/writable.
- Define the **migration story** (vacuum-upgrades, open-old-files behavior, test fixtures).

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

### Likely implementation surface

- Update `vkInt64` in `encodeValue`/`decodeValue` in [src/record/record.nim](src/record/record.nim).
- Update any other code paths that assume `vkInt64` payload len must be 8.
- Update documentation describing record formats if needed (e.g., [docs/architecture/storage.md](docs/architecture/storage.md) if it is intended to be authoritative).

### Tests (stable & durable)

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

### Why

SQLite’s btree layout is extremely space efficient. DecentDB’s fixed per-cell headers contribute to page count and file size.

### ADR requirements

Create a new ADR to change btree leaf cell layout (and migration/versioning).

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

## Phase 4: Optional compression (defer; ADR + dependency approval)

Compression can reduce size for large TEXT/BLOB, but it is intentionally **deferred**:

- Requires a new dependency (or an internal implementation) and clear performance trade-offs.
- If pursued, split into:
  1) Storage-internal “compressed value container” (no SQL syntax changes)
  2) Optional SQL surface area (DDL) as a separate ADR and project

## Size regression harness (required)

Add a deterministic, in-repo size regression test that:

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
