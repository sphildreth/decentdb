# 2026-02-01 Triage, Fixes, and Plan

## Issue Map (Progress)

### High-signal concerns
- [x] WAL index growth when checkpoints overlap new commits (prune `wal.index` entries after checkpoint)
- [x] Trigram postings memory spikes (bounded decode + safe fallback to scan)
- [x] Thread-safety / snapshot context ambiguity (documented contract)
- [ ] Atomicity risk: dirty page eviction must not write uncommitted bytes to the main DB file
- [ ] Atomicity risk: freelist/header updates are persisted outside WAL during transactions
- [ ] Correctness risk: TEXT/BLOB index keys use CRC32C hash (collision safety + constraints)
- [ ] Correctness bug: JOIN fallback can return empty results for large left inputs
- [ ] Crash harness drift: scenario labels/kinds and WAL file naming mismatch

### ADRs
- [x] ADR-0055: Thread-Safety Contract and Snapshot Context Handling
- [x] ADR-0056: Prune WAL In-Memory Index Entries After Checkpoint
- [ ] Draft ADR: Background / incremental checkpoint worker
- [ ] Draft ADR: Page cache contention strategy (if locking/invariants change)
- [ ] Draft ADR: WAL-safe page cache eviction / flush pipeline (dirty eviction + commit/checkpoint semantics)
- [ ] Draft ADR: Transactional freelist/header updates (crash-safe invariants)
- [ ] Draft ADR: Typed/comparable index key encoding for TEXT/BLOB (ordering/collation + uniqueness semantics)
- [ ] Draft ADR: B+Tree prefix compression (page layout change)
- [ ] Draft ADR: Trigram postings paging/streaming storage format

### Testing & validation
- [x] Run: `tests/nim/test_wal_checkpoint_race.nim`
- [x] Run: `tests/nim/test_trigram.nim`
- [x] Run: `tests/nim/test_exec.nim`
- [ ] Run: full Nim suite (`nimble test_nim`)
- [ ] Run: Python harness (`nimble test_py`)
- [ ] Add targeted stress/bench reproducer(s) for long-running readers + sustained writes (force cache pressure/eviction + checkpoint overlap)
- [ ] Add targeted tests: tiny cache eviction mid-tx + rollback/crash should not modify DB file
- [ ] Add targeted tests: JOIN with left input > 100 rows on non-indexed right path
- [ ] Validate crash scenarios actually hit current failpoints + WAL file naming

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
- **WAL commit ordering “atomicity gap”**: current visibility is gated by `walEnd` (Acquire/Release). Updating `walEnd` *before* populating `wal.index` would allow readers to take a snapshot and then miss overlay data, so the proposed reordering is not correct for the current design.
- **“Mid-commit” snapshot capture**: with the current design, readers do not interpret raw WAL bytes; they use `snapshot=walEnd` plus `wal.index`. A snapshot should not observe partially committed state unless other issues (e.g., unsafe page cache flushes) leak uncommitted bytes to the main DB.
- **Constraint checking overhead**: UNIQUE/FK validation costs are expected (index lookups / FK existence checks). Treat as a profiling topic for bulk load workloads, not a correctness gap, unless tests show behavior drift.
- **“Read-write locks for better concurrency”**: too unspecific to action; consider only if profiling shows a clear contention hotspot (ADR required if locking/invariants change).
- **“Handle phantom reads better”**: snapshot isolation does not prevent predicate phantoms in general; strengthening isolation implies SSI/predicate locking and is ADR-required (isolation guarantee change).
- **“More frequent fsync”**: treat as a configurability/perf tradeoff (commit policy / group commit), not an unconditional “more is better”; ADR-required if durability semantics change.

### Additional validated concerns (from review)
These items were confirmed by direct code inspection and should be treated as correctness/ACID work, not just performance tuning.

1. **Atomicity breach risk: dirty page eviction can write uncommitted bytes to the main DB file**
   - `evictIfNeededLocked` may evict unpinned pages and calls `flushEntry` on dirty entries.
   - `flushEntry` defaults to writing to `pager.file` (main DB) when `pager.flushHandler == nil`.
   - This creates a window where a transaction has dirty, evictable pages, but commit has not yet WAL-logged them.

2. **Atomicity risk: freelist/header updates are persisted outside WAL**
   - `allocatePage` / `freePage` update freelist structures and then call `updateHeader`.
   - `updateHeader` calls `writeHeader`, which fsyncs immediately.
   - Rollback “compensation” via page tracking is not equivalent to WAL-logging header/freelist mutation for crash semantics.

3. **Correctness risk: TEXT/BLOB indexes use CRC32C as the key**
   - `indexKeyFromValue` uses `uint64(crc32c(value.bytes))` for `vkText`/`vkBlob`.
   - Without mandatory post-verification, collisions can cause false positives/negatives in seeks and constraint checks.

4. **Execution correctness bug: JOIN fallback can silently return empty results**
   - `pkJoin` caches the right side only when `left.len <= MaxLeftRowsForCache` and right isn’t an index seek.
   - When caching is disabled, the non-index-seek path uses `cachedRight` (empty), leading to incorrect results.

5. **Crash harness drift: scenario/failpoint mismatch and WAL filename mismatch**
   - Engine WAL filename uses `path & "-wal"`, while crash runner cleans up `db_path + ".wal"`.
   - WAL failpoints support kinds `{wfNone,wfError,wfPartial}` and specific labels (e.g. `wal_write_frame`, `wal_fsync`), so scenario JSONs must match exactly.

6. **Checkpoint/read coordination + cache coherence risk**
   - Checkpoint samples `minReaderSnapshot` early and releases the WAL lock during the I/O phase.
   - Readers can begin during checkpoint I/O, but truncation decisions later use the earlier `minSnap` sample.
   - Checkpoint writes pages to the DB file via a direct write path (`writePageDirectFile`), which bypasses the page cache; without explicit invalidation, readers can continue serving stale cached pages.
   - This is fundamentally a correctness issue (Snapshot Isolation expectations + “reader never sees inconsistent pages”) and should be fixed in a way consistent with ADR-0019.

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

### P0) ACID / correctness fixes (ADR-required)

#### 1) Make page-cache eviction WAL-safe
**Status:** Not implemented (ADR required).

**Why ADR-required:** Any change here can affect durability/atomicity/isolation guarantees and recovery semantics.

**Proposed options:**
- Disallow eviction of dirty pages while a transaction is in progress (simple but can cause “cache full” behavior).
- Set `pager.flushHandler` during normal operation to ensure dirty evictions append to WAL (or otherwise avoid touching the main DB).

**Tests to add:** tiny cache + write txn large enough to force eviction + rollback/crash → DB file must not reflect uncommitted bytes.

#### 2) Make freelist/header updates transactional
**Status:** Not implemented (ADR required).

**Why ADR-required:** This touches on-disk structural invariants and when they become durable/visible (allocation/free, header updates, checkpoint ordering).

**Proposed direction:** Treat freelist/header mutations as transactional state that becomes durable through WAL commit + checkpoint (or WAL-log allocation/free intents and replay during recovery).

#### 3) Fix TEXT/BLOB indexing semantics
**Status:** Not implemented (ADR required for the full solution).

**Why ADR-required:** Proper typed/comparable key encoding implies ordering/collation rules and impacts uniqueness semantics.

**Interim correctness patch (quick win candidate):** If hashing is retained temporarily, enforce mandatory post-filtering by full value for equality seeks and constraint checks (still leaves ordering/range semantics undefined for text).

### P0) Correctness bugs (no ADR expected)

#### 3b) Make checkpoint truncation safe w.r.t late-starting readers and cache coherence
**Status:** Not implemented.

**Proposed fix direction:**
- Re-check active readers (and/or `minReaderSnapshot`) immediately before any truncation/index clearing.
- If any readers are active, defer truncation entirely (0.x simplicity; consistent with ADR-0019).
- Ensure checkpoint writes do not leave stale pages in cache (invalidate affected cached pages, or route checkpoint writes through a cache-aware path).

**Tests to add:** begin a read transaction during checkpoint I/O; ensure it never observes stale pages after checkpoint and never fails due to missing overlay data.

#### 4) Fix JOIN fallback behavior
**Status:** Not implemented.

**Proposed fix:** When `canCacheRight == false` and right isn’t an index seek, compute the right side per-left-row (slow but correct) until a real join algorithm exists.

**Tests to add:** differential or unit test where left side > 100 rows and join predicate cannot be turned into `pkIndexSeek`.

### P1) Reliability and harness correctness

#### 5) Align crash-injection harness with current failpoints + WAL naming
**Status:** Not validated end-to-end.

**Proposed next steps:**
- Update crash runner cleanup to remove the active WAL filename convention (`-wal`, and optionally also `.wal` for backwards compatibility).
- Audit scenario JSON failpoint labels/kinds against the current WAL failpoint implementation.

### P1) Reader contention / performance

#### 6) Shorten `wal.indexLock` critical section for readers
**Status:** Not implemented.

**Proposed fix:** Under lock, find the best `(lsn, offset)` entry; release lock; then `readFrame` outside the lock.

#### 7) Monitoring and diagnostics improvements (low-risk)
**Status:** Not implemented.

**Rationale:** Several review concerns are best validated/triaged with lightweight counters rather than speculation (cache eviction stalls, overlay hit/miss, WAL growth due to long-lived readers, checkpoint wait/latency).

**Proposed next steps:**
- Add counters for: eviction attempts/flushes, eviction blocked-by-pin, overlay lookups (hit/miss), checkpoint duration breakdown (lock hold vs I/O), and WAL growth reasons (writer vs active readers).
- Expose via existing debug/CLI hooks if present; otherwise gate behind a compile-time flag to remain zero-cost when disabled.

### P1) Durability/perf: redundant fsync patterns
**Status:** Not validated.

**Proposed fix:** Split header write into a non-syncing write + caller-controlled fsync to avoid double-fsync in checkpoint paths.

### P2) B+Tree hot-path allocations/copies
**Status:** Not addressed.

### P2) B+Tree write error handling + rollback page reuse safety
**Status:** Not addressed.

**Notes:**
- B+Tree split paths currently `discard` the result of `writePage`; write failures should propagate to avoid silent corruption.
- On rollback, pages returned to freelist should be invalidated in the page cache to prevent reuse of stale/dirty buffers.

**Proposed fix:** Avoid copying full pages into `seq[byte]` for varint decoding; decode directly from the page buffer or reuse scratch buffers.

### P2) Trigram index durability semantics (known gap)
**Status:** Known and currently accepted (requires explicit documentation and/or ADR if tightened).

**Note:** Trigram deltas are deferred to checkpoint; after a crash, trigram indexes may be stale until rebuilt/checkpointed.

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
