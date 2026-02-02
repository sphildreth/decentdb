# 2026-02-01 Triage, Fixes, and Plan

## Issue Map (Progress)

### High-signal concerns
- [x] WAL index growth when checkpoints overlap new commits (prune `wal.index` entries after checkpoint)
- [x] Trigram postings memory spikes (bounded decode + safe fallback to scan)
- [x] Thread-safety / snapshot context ambiguity (documented contract)

### ADRs
- [x] ADR-0055: Thread-Safety Contract and Snapshot Context Handling
- [x] ADR-0056: Prune WAL In-Memory Index Entries After Checkpoint
- [ ] Draft ADR: Background / incremental checkpoint worker
- [ ] Draft ADR: Page cache contention strategy (if locking/invariants change)
- [ ] Draft ADR: B+Tree prefix compression (page layout change)
- [ ] Draft ADR: Trigram postings paging/streaming storage format

### Testing & validation20260201-Deepseek-Concerns-Plan
- [x] Run: `tests/nim/test_wal_checkpoint_race.nim`
- [x] Run: `tests/nim/test_trigram.nim`
- [x] Run: `tests/nim/test_exec.nim`
- [ ] Run: full Nim suite (`nimble test_nim`)
- [ ] Run: Python harness (`nimble test_py`)
- [ ] Add targeted stress/bench reproducer(s) for long-running readers + sustained writes

## Context
A review flagged several DecentDB performance and ACID-adherence concerns. This document records:
- Which concerns were validated in code
- What was fixed immediately (no persistent format or semantics changes)
- Which topics require ADRs (locking/semantics/format changes)
- Next-step plan and test coverage

## Summary of Findings

### High-signal / validated
1. **WAL in-memory index growth can become unbounded**
   - In the checkpoint completion path where new commits occur during the checkpoint I/O phase (`hadNewCommits`), `dirtySinceCheckpoint` was trimmed, but `wal.index` entries were not pruned.
   - This can cause steady growth of per-page version lists in `wal.index`, increasing memory use and checkpoint work.

2. **Trigram postings decoding can cause memory spikes**
   - Trigram seek uses postings lists that are decoded fully into memory.
   - For high-frequency trigrams, postings can be very large; decoding everything is unnecessary when heuristics will fall back to table scan anyway.

3. **Thread-safety / snapshot context ambiguity**
   - Snapshot selection during statement execution relies on a mutable connection-scoped state (`Pager.overlaySnapshot`).
   - Concurrent use of a single `Db`/connection across multiple threads could violate Snapshot Isolation unless explicitly constrained or redesigned.

### Lower-signal / needs evidence or already partly addressed
- **“LIKE doesn’t use trigram”**: execution does use trigram seek plans (plus a `LIKE` verification filter); the remaining question is planner heuristics, not `likeMatch` itself.
- **Pager tombstones / cache “leak”**: tombstones are periodically compacted; this looks like tuning/perf, not a leak.
- **Write-write conflict detection**: DecentDB is single-writer by design; conflicts across writers are out of scope unless the concurrency model changes.

## Changes Implemented (Immediate Fixes)

### 1) WAL index pruning on checkpoint completion
**Goal:** Bound `wal.index` growth without weakening Snapshot Isolation or changing WAL truncation rules.

**Change:** When a checkpoint completes but cannot truncate due to `hadNewCommits`, prune `wal.index[pageId]` entries with `lsn <= safeLsn` for pages that were written to the DB file during this checkpoint.

**Files:**
- src/wal/wal.nim

**Notes:**
- This is an in-memory structure change only; it does not alter WAL frame format or on-disk layouts.
- This aligns with the existing ADR direction that truncation must not discard frames needed by active readers.

### 2) Bounded trigram postings decode + safe fallback
**Goal:** Avoid large allocations when trigram postings lists are huge.

**Change:**
- Added a bounded postings decoder to stop decoding after a limit and report `truncated=true`.
- Added a bounded trigram postings fetch that applies in-memory deltas while avoiding unbounded decode.
- Updated trigram seek execution paths to use bounded postings loads and fall back to table scan + `LIKE` filter when any postings list is too large.

**Files:**
- src/search/search.nim
- src/storage/storage.nim
- src/exec/exec.nim

**Correctness note:** bounded postings loads still apply trigram deltas, including when the base postings list is empty (common when deltas have not yet been flushed).

## ADRs Created (Design Decisions)

### ADR 0055: Thread-Safety Contract and Snapshot Context Handling
**File:** design/adr/0055-thread-safety-and-snapshot-context.md

**Purpose:**
- Clarifies that a single `Db` connection is not safe for concurrent use from multiple threads.
- Recommends one connection per thread for concurrent reads.
- Defers “single connection, many concurrent readers” to a future explicit snapshot-context redesign.

### ADR 0056: Prune WAL In-Memory Index Entries After Checkpoint
**File:** design/adr/0056-wal-index-pruning-on-checkpoint.md

**Purpose:**
- Captures the checkpoint/index-pruning decision and rationale.
- Differentiates pruning in-memory index entries from unsafe WAL truncation.

## Test/Validation

### Tests executed during this work
- nim c --hints:off -r tests/nim/test_wal_checkpoint_race.nim
- nim c --hints:off -r tests/nim/test_trigram.nim
- nim c --hints:off -r tests/nim/test_exec.nim

These tests cover:
- checkpoint behavior with pinned readers and “new commits during checkpoint” scenarios
- trigram index correctness vs scan for LIKE
- exec plan paths that involve trigram seek fallbacks

## Remaining Concerns and Plan

### A) Background / incremental checkpointing
**Status:** Not implemented (ADR required).

**Why ADR-required:** Changing checkpoint scheduling and interleaving affects durability, throughput, and potentially locking semantics.

**Proposed next steps:**
1. Draft ADR: “Incremental/background checkpoint worker”
2. Add microbench + targeted unit tests for checkpoint latency under sustained writes + readers

### B) Page cache contention / shard eviction policies
**Status:** Not implemented (needs benchmarks; may be ADR-required depending on locking changes).

**Proposed next steps:**
1. Measure contention/hot-path costs with existing `bench_large` patterns
2. If changes alter locking or invariants, draft ADR before implementation

### C) B+Tree prefix compression
**Status:** Not implemented (ADR required; persistent format change).

**Why ADR-required:** Would alter page layout; must version formats and add compatibility tests.

**Proposed next steps:**
1. Draft ADR for page layout compression strategy (prefix compression, delta encoding, etc.)
2. Add compatibility tests + recovery tests if accepted

### D) Trigram postings paging / streaming storage format
**Status:** Partially mitigated (bounded decode + fallback). Full paging is ADR-required.

**Proposed next steps:**
1. Observe whether bounded decode reduces memory spikes sufficiently
2. If not, draft ADR extending trigram postings storage strategy (paging/overflow)

## Risks / Follow-ups
- WAL index pruning correctness depends on the property: after checkpointing a page at/before `safeLsn`, older versions `<= safeLsn` are no longer required for correctness. This is consistent with Snapshot Isolation expectations when readers are pinned at `safeLsn` or later.
- If the project intends “multiple concurrent reader threads on the same connection,” ADR 0055 flags that this likely requires snapshot context redesign (explicit snapshot parameters or thread-local snapshot state).

## References
- design/adr/0019-wal-retention-for-active-readers.md
- design/adr/0023-isolation-level-specification.md
- design/adr/0055-thread-safety-and-snapshot-context.md
- design/adr/0056-wal-index-pruning-on-checkpoint.md
