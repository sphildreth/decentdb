# DecentDB → SQLite Commit Latency Performance Gap Plan (V2)

**Date:** 2026-02-07  
**Goal:** Not just close the gap — **beat SQLite** on p95 commit latency  
**Current State:** DecentDB ~0.0751ms vs SQLite ~0.0094ms → **7.99× slower**  
**Target:** ≤0.009ms p95 commit latency (≤1.0× SQLite)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [What V1 Achieved and Why It Stalled](#2-what-v1-achieved-and-why-it-stalled)
3. [Root Cause Analysis: Where the 75µs Actually Goes](#3-root-cause-analysis-where-the-75µs-actually-goes)
4. [The Core Thesis: Why Beating SQLite Is Possible](#4-the-core-thesis-why-beating-sqlite-is-possible)
5. [Phase 1: Eliminate All Heap Allocation from the Commit Hot Path](#5-phase-1-eliminate-all-heap-allocation-from-the-commit-hot-path)
6. [Phase 2: Reduce Syscall Count to One](#6-phase-2-reduce-syscall-count-to-one)
7. [Phase 3: Tame fdatasync Tail Latency](#7-phase-3-tame-fdatasync-tail-latency)
8. [Phase 4: Structural Advantages Over SQLite](#8-phase-4-structural-advantages-over-sqlite)
9. [Cross-Metric Guardrails](#9-cross-metric-guardrails)
10. [Implementation Order and Dependencies](#10-implementation-order-and-dependencies)
11. [Risk Assessment](#11-risk-assessment)
12. [Projected Outcome](#12-projected-outcome)

---

## 1. Executive Summary

V1 of this plan reduced DecentDB's p95 commit latency from 0.127ms to 0.075ms (a 41%
improvement) through 10 incremental optimizations focused on WAL frame format trimming,
CRC removal, buffer pre-sizing, and executor fast paths. Nine further attempts were tried
and rejected because they regressed p95. The plan concluded that getting below 2× SQLite
would require "significant architectural changes (months)."

**V2 takes a fundamentally different approach.** Instead of continuing to shave bytes off the
frame format or rearrange copies, V2 targets the two dominant sources of p95 tail latency
that V1 never addressed:

1. **Heap allocation jitter** — DecentDB allocates 6–8 objects on the heap per commit.
   Nim's ARC runtime is fast on average but introduces unpredictable pauses at p95. SQLite
   allocates **zero** heap objects on its commit hot path. Every buffer is pre-allocated
   and reused.

2. **Unnecessary syscalls** — DecentDB issues 2–3 syscalls per commit (pwrite for frames,
   pwrite for WAL header, fdatasync). SQLite issues exactly 2 (one pwrite, one fdatasync).
   Each extra syscall adds ~1–3µs of kernel entry/exit overhead plus potential cache line
   invalidation.

By eliminating all heap allocation from the commit path and reducing syscalls to the
theoretical minimum of 2 (one write, one sync), DecentDB can match SQLite's ~10µs floor.
By then exploiting structural advantages that SQLite cannot use (smaller WAL frames, no
per-frame checksums, pre-mapped WAL region), DecentDB can potentially go **below** 10µs.

---

## 2. What V1 Achieved and Why It Stalled

### V1 Accepted Optimizations (cumulative ~41% improvement)

| # | Optimization | Improvement | Cumulative p95 |
|---|-------------|-------------|----------------|
| 1 | Pre-size WAL frameBuffer | −11.5% | 0.1127ms |
| 2 | Direct string-to-frame encoding | −1.4% | 0.1111ms |
| 3 | Faster CRC32C (slicing-by-8) | −13.8% | 0.0958ms |
| 4 | Remove per-frame CRC32C entirely | −13.7% | 0.0826ms |
| 5 | Remove per-frame LSN trailer | −3.8% | 0.0795ms |
| 6 | Single-page pending fast path | −0.6% | 0.0790ms |
| 7 | Remove WAL payload length field | −0.4% | 0.0787ms |
| 8 | Prepared UPDATE fast path (INT64 PK) | ~noise | 0.0787ms |
| 9 | Skip old-row read (no secondary indexes) | −3.9% | 0.0756ms |
| 10 | WAL header + logical end offset (mmap prealloc) | −1.8% | 0.0742ms |

### V1 Rejected Attempts (9 regressions)

| Attempt | Why It Regressed |
|---------|-----------------|
| `writev` for single-page commits | Extra syscall overhead > copy savings |
| Remove frame type field | Branch misprediction > 1-byte savings |
| Reuse WAL pageMeta buffer | Bookkeeping overhead from buffer management |
| Zero-copy WAL writev path | Slice construction overhead |
| `sync_file_range` preflush | Added syscall with no fdatasync reduction on test SSD |
| Release WAL lock before fsync | No benefit in single-threaded benchmark; extra bookkeeping |
| Cache qualified column names | Cache lookup > string construction |
| mmap-backed WAL write path | Per-commit truncate/remap overhead |
| Dirty-page set snapshot | Set bookkeeping > linear cache scan |

### Why V1 Stalled

Every V1 optimization targeted **deterministic CPU work** — fewer bytes to encode, fewer
copies to make, fewer fields to write. These optimizations have diminishing returns because
the remaining CPU work is already minimal (~20–30µs of encoding/copying). The gap between
measured p95 (~75µs) and estimated deterministic cost (~25–30µs) is dominated by:

- **Non-deterministic allocation latency** (~15–25µs at p95)
- **fdatasync tail latency** (~15–30µs variance at p95)
- **Syscall overhead** (~5–10µs from 2–3 kernel transitions)

V1 never addressed any of these. V2 does.

---

## 3. Root Cause Analysis: Where the 75µs Actually Goes

### Full Call Path for a Single-Page UPDATE Commit

The benchmark times `execPrepared(stmt, params)` which internally executes:

```
execPrepared(stmt, params)                              TIMED
├─ execPreparedNonSelect(db, bound, params, plan)
│  ├─ beginTransaction(db)                              ← ALLOCATION #1: WalWriter ref object
│  │  ├─ beginWrite(wal)                                ← acquire(wal.lock)
│  │  │  └─ WalWriter(pending: @[], ...)                ← ALLOCATION #2: pending seq
│  │  │     └─ initTable[PageId, WalIndexEntry]()       ← ALLOCATION #3: flushed Table
│  │  ├─ beginTxnPageTracking(pager)
│  │  └─ install flushHandler closure                   ← ALLOCATION #4: closure object
│  │
│  ├─ [execute UPDATE plan]
│  │  ├─ tryFastPkUpdate(...)                           ← fast path for WHERE pk = $1
│  │  │  ├─ B+Tree lookup (~2–4µs)
│  │  │  ├─ serialize new row
│  │  │  └─ write page to cache (mark dirty)
│  │  └─ (no old-row read — optimization #9)
│  │
│  ├─ commitTransaction(db)
│  │  ├─ snapshotDirtyPages(pager)                      ← ALLOCATION #5: entries seq
│  │  │  ├─ scan all cache shards (16 shards, acquire/release each lock)
│  │  │  ├─ collect CacheEntry refs
│  │  │  └─ for each dirty entry:
│  │  │     └─ let copy = entry.data                    ← ALLOCATION #6: 4096-byte string copy
│  │  │
│  │  ├─ var pageIds: seq[PageId] = @[]                 ← ALLOCATION #7: pageIds seq
│  │  ├─ writePageDirect(writer, pageId, data)          ← stores in pendingSingle (no alloc)
│  │  │
│  │  ├─ commit(writer)                                 ← THE CORE COMMIT
│  │  │  ├─ var pageMeta: seq[...] = @[]                ← ALLOCATION #8: pageMeta seq
│  │  │  ├─ compute total frame length
│  │  │  ├─ ensureWalMmapCapacity(wal, requiredEnd)     ← usually no-op (already mapped)
│  │  │  ├─ encode page frame into mmap region          ← memcpy 4109 bytes (ZERO ALLOC)
│  │  │  ├─ encode commit frame into mmap region        ← memcpy 13 bytes
│  │  │  ├─ writeWalHeader(wal, newEnd)                 ← memcpy 32 bytes into mmap (ZERO ALLOC)
│  │  │  │  (or: ALLOCATION #9: newSeq[byte](32) if not mmap)
│  │  │  ├─ vfs.fsync(wal.file)                         ← SYSCALL: fdatasync() ← BLOCKS ~5–15µs
│  │  │  ├─ acquire(wal.indexLock)
│  │  │  ├─ for each page in pageMeta:
│  │  │  │  ├─ if not hasKey: index[pageId] = @[]       ← ALLOCATION #10 (first commit only)
│  │  │  │  └─ index[pageId].add(entry)
│  │  │  ├─ release(wal.indexLock)
│  │  │  ├─ wal.walEnd.store(commitLsn, moRelease)
│  │  │  ├─ writer.pending = @[]                        ← ALLOCATION #11: new empty seq
│  │  │  └─ release(wal.lock)
│  │  │
│  │  ├─ markPagesCommitted(pager, pageIds, lsn)
│  │  │  ├─ acquire(pager.overlayLock)
│  │  │  ├─ overriddenPages.excl(pageId)
│  │  │  ├─ release(pager.overlayLock)
│  │  │  ├─ for each pageId:
│  │  │  │  ├─ acquire(shard.lock)
│  │  │  │  ├─ entry.dirty = false; entry.lsn = lsn
│  │  │  │  └─ release(shard.lock)
│  │  │
│  │  ├─ [checkpoint threshold evaluation]
│  │  │  ├─ check wal.endOffset >= checkpointEveryBytes
│  │  │  ├─ epochTime()                                 ← POTENTIAL SYSCALL (gettimeofday)
│  │  │  └─ estimateIndexMemoryUsage(wal)               ← acquire indexLock + iterate index
│  │  │
│  │  ├─ maybeCheckpoint(wal, pager)                    ← usually no-op
│  │  ├─ db.activeWriter = nil
│  │  └─ endTxnPageTracking(pager)
```

### Allocation Inventory

For a **single-page UPDATE with auto-commit** (the benchmark workload), there are
**8–11 heap allocations** per commit:

| # | What | Size | Where | Avoidable? |
|---|------|------|-------|------------|
| 1 | `WalWriter` ref object | ~64B | `beginWrite` | **YES** — reuse across transactions |
| 2 | `writer.pending` seq | ~48B | `beginWrite` | **YES** — `setLen(0)` instead |
| 3 | `writer.flushed` Table | ~64B | `beginWrite` | **YES** — reuse/clear instead |
| 4 | `flushHandler` closure | ~32B | `beginTransaction` | **YES** — install once, reuse |
| 5 | `entries` seq in snapshotDirtyPages | ~48B | `snapshotDirtyPages` | **YES** — bypass entirely |
| 6 | Page data string copy (4096B) | ~4KB | `snapshotDirtyPages` | **YES** — read in-place |
| 7 | `pageIds` seq | ~48B | `commitTransaction` | **YES** — fixed-size array |
| 8 | `pageMeta` seq | ~48B | WAL `commit` | **YES** — reusable buffer |
| 9 | WAL header `newSeq[byte](32)` | ~80B | `writeWalHeader` (non-mmap) | Already avoided with mmap |
| 10 | `index[pageId] = @[]` | ~48B | WAL index update (first time) | Only first commit per page |
| 11 | `writer.pending = @[]` | ~48B | commit cleanup | **YES** — `setLen(0)` |

**Total avoidable allocations: 8 per commit (~4.5KB heap churn)**

### Syscall Inventory

For the mmap path (currently default on Linux):

| # | Syscall | Purpose | Avoidable? |
|---|---------|---------|------------|
| 1 | `fdatasync()` | Durability | **NO** — required for ACID |
| 2 | `flushFile()` via stdio | Flush after WAL write (non-mmap path) | Avoided with mmap |

For the non-mmap path:

| # | Syscall | Purpose | Avoidable? |
|---|---------|---------|------------|
| 1 | `lseek()` + `write()` | Write frames at append offset | **YES** — use pwrite |
| 2 | `lseek()` + `write()` | Write WAL header at offset 0 | **YES** — embed in commit frame |
| 3 | `fflush()` | Flush stdio buffer | **YES** — use pwrite directly |
| 4 | `fdatasync()` | Durability | **NO** — required for ACID |

With mmap, frames are written via `memcpy` into the mapped region and the header is also
a `memcpy` at offset 0. The only syscall is `fdatasync()`. This is already optimal at the
syscall level.

**However**, the mmap path was rejected in V1 due to per-commit truncate/remap overhead.
The V1 fix (WAL header with logical end offset + preallocation) eliminated the truncate
but the mmap path was still rejected. We need to verify the current state: is mmap
actually active and working, or is the fallback path running?

### Lock Inventory

| # | Lock | Duration | Avoidable? |
|---|------|----------|------------|
| 1 | `wal.lock` | Entire commit (begin → cleanup) | No (single-writer guarantee) |
| 2 | `shard.lock` × 16 | snapshotDirtyPages scan | **YES** — bypass for single-page |
| 3 | `entry.lock` | Page data copy | **YES** — read in-place |
| 4 | `wal.indexLock` | Index update after fsync | Minimal (already brief) |
| 5 | `pager.overlayLock` | markPagesCommitted | Minimal |
| 6 | `shard.lock` | markPagesCommitted per page | Minimal |

The `snapshotDirtyPages` function acquires and releases **all 16 shard locks** even when
only 1 page is dirty. This is ~32 lock operations (16 acquire + 16 release) for a
single-page commit.

---

## 4. The Core Thesis: Why Beating SQLite Is Possible

SQLite's commit path for a single-page WAL-mode UPDATE with `synchronous=FULL`:

```
sqlite3_step(stmt)
├─ Execute VDBE bytecode
│  ├─ B-Tree cursor seek to row
│  ├─ Modify page in-place in page cache
│  └─ Mark page as dirty
├─ Auto-commit trigger
│  ├─ pwrite() WAL frames (4120 bytes: 24-byte header + 4096-byte page)
│  │   ├─ Page frame with checksum
│  │   └─ Commit frame (8 bytes embedded in page frame's nTruncate field)
│  ├─ fdatasync() WAL file
│  └─ Update WAL index (shared memory, atomic writes)
└─ sqlite3_reset(stmt)
    └─ Reset VDBE program counter
```

**SQLite's structural costs that DecentDB does NOT pay:**

1. **Per-frame checksum**: SQLite computes a cumulative checksum over every WAL frame.
   It uses a custom checksum (not CRC32C) that is fast but still costs ~3–5µs per 4KB
   page. DecentDB removed per-frame checksums in V1 optimization #4.

2. **Larger WAL frame**: SQLite's WAL frame is 4120 bytes (24-byte header + 4096 payload).
   DecentDB's is 4109 bytes (5-byte header + 4096 payload + 8-byte trailer). That's
   11 fewer bytes to write and sync.

3. **WAL-SHM index**: SQLite maintains a shared-memory WAL index file (`-shm`) that
   requires careful atomic operations and potential `mmap` management. DecentDB's
   in-process index is simpler (just a `Table` update under a lock).

4. **VDBE interpreter overhead**: SQLite executes UPDATE via a bytecode interpreter
   (VDBE). Each instruction has dispatch overhead. DecentDB's `tryFastPkUpdate` is
   compiled native code with direct B+Tree access.

**If DecentDB can match SQLite's allocation discipline and syscall efficiency**, it should
be able to leverage these structural advantages to achieve **lower** commit latency than
SQLite.

### The Arithmetic

SQLite's ~10µs p95 breaks down approximately as:

| Component | Estimated Cost |
|-----------|---------------|
| VDBE dispatch + B-Tree modify | ~2–3µs |
| WAL checksum calculation | ~3–5µs |
| pwrite(4120 bytes) | ~1–2µs |
| fdatasync() | ~3–6µs |
| WAL-SHM index update | ~1–2µs |
| **Total** | **~10–18µs** |

DecentDB's theoretical floor (after V2 optimizations):

| Component | Estimated Cost |
|-----------|---------------|
| Fast-path PK update + B-Tree modify | ~2–3µs |
| WAL frame encode (memcpy into mmap, no checksum) | ~1–2µs |
| fdatasync() | ~3–6µs |
| In-process index update | ~1–2µs |
| **Total** | **~7–13µs** |

The key insight: DecentDB's theoretical floor is **lower** than SQLite's because it
skips the WAL checksum (~3–5µs), writes fewer bytes (4109 vs 4120), and avoids VDBE
interpreter overhead. The reason DecentDB is currently 8× slower is entirely due to
**allocation overhead and unnecessary work on the commit path**, not architectural
limitations.

---

## 5. Phase 1: Eliminate All Heap Allocation from the Commit Hot Path

**Goal:** Zero heap allocations between `execPrepared` entry and return.  
**Estimated Impact:** 20–35% p95 reduction (0.075ms → 0.049–0.060ms)  
**Risk Level:** Low (internal implementation changes, no format changes)

### 5.1 Reuse WalWriter Across Transactions

**Current behavior:** `beginWrite()` (wal.nim:1077–1080) allocates a brand-new
`WalWriter` ref object on every transaction:

```nim
proc beginWrite*(wal: Wal): Result[WalWriter] =
  acquire(wal.lock)
  let writer = WalWriter(wal: wal, pending: @[], active: true,
                          hasPendingSingle: false,
                          flushed: initTable[PageId, WalIndexEntry]())
  ok(writer)
```

This allocates: (a) the `WalWriter` ref object, (b) the `pending` seq, (c) the `flushed`
Table. Three heap allocations before any work begins.

**Proposed change:** Keep a single `WalWriter` on the `Wal` object. `beginWrite` resets
its state instead of allocating:

```nim
proc beginWrite*(wal: Wal): Result[WalWriter] =
  acquire(wal.lock)
  wal.writer.active = true
  wal.writer.hasPendingSingle = false
  wal.writer.pending.setLen(0)
  wal.writer.flushed.clear()
  ok(wal.writer)
```

**Saves:** 3 heap allocations per commit (~176 bytes).

**Safety:** The single-writer model guarantees only one WalWriter is active at a time,
so reuse is safe. The `wal.lock` ensures exclusivity.

### 5.2 Bypass snapshotDirtyPages for Single-Page Commits

**Current behavior:** `snapshotDirtyPages` (pager.nim:730–744) always:
1. Allocates a `seq[CacheEntry]`
2. Iterates **all 16 cache shards**, acquiring and releasing each shard lock
3. Collects all `CacheEntry` refs
4. For each dirty entry, acquires `entry.lock`, **copies the entire 4096-byte page
   string**, and appends to result

For a single-page UPDATE, this scans 16 shards to find 1 dirty page, performing
32 lock acquire/release operations and 1 full page copy.

**Proposed change:** Track the single dirty page directly in the transaction context.
When `tryFastPkUpdate` modifies a page, record `(pageId, cacheEntry)` on the
transaction/writer. At commit time, if exactly one page is dirty (the common case for
single-row UPDATE), skip `snapshotDirtyPages` entirely and read the page data directly
from the known cache entry.

```nim
# In commitTransaction, fast path:
if db.txnDirtyCount == 1:
  # We know exactly which page is dirty — no scan needed
  let entry = db.txnDirtyEntry
  acquire(entry.lock)
  writePageDirect(writer, entry.id, entry.data)  # string ref, no copy
  release(entry.lock)
else:
  # Multi-page path: use existing snapshotDirtyPages
  let dirtyPages = snapshotDirtyPages(db.pager)
  for entry in dirtyPages:
    writePageDirect(writer, entry[0], entry[1])
```

**Saves:** 1 seq allocation, 32 lock operations, 1 full 4096-byte string copy.
This is likely the **single largest win** in Phase 1 because the 4KB string copy
forces a heap allocation that can trigger ARC bookkeeping.

**Safety:** The writer holds `wal.lock` which prevents concurrent writes. The page
is pinned in cache (won't be evicted during the transaction). Reading `entry.data`
under `entry.lock` is the same pattern used by `snapshotDirtyPages` today.

### 5.3 Eliminate pageMeta seq in WAL commit

**Current behavior:** `commit()` (wal.nim:1137) allocates a local `pageMeta` seq on
every commit to track `(PageId, WalIndexEntry)` tuples:

```nim
var pageMeta: seq[(PageId, WalIndexEntry)] = @[]
```

This seq grows via `.add()` during frame encoding and is iterated during index update.

**Proposed change:** Use a fixed-size array on the `WalWriter` for the common case
(≤8 pages per commit), with fallback to seq for large transactions:

```nim
# On WalWriter:
pageMetaBuf: array[8, (PageId, WalIndexEntry)]
pageMetaCount: int

# In commit:
writer.pageMetaCount = 0
# During encoding:
writer.pageMetaBuf[writer.pageMetaCount] = (pageId, entry)
writer.pageMetaCount.inc
# If overflow: fall back to seq (rare for single-page commits)
```

**Saves:** 1 seq allocation per commit.

### 5.4 Eliminate pageIds seq in commitTransaction

**Current behavior:** `commitTransaction` (engine.nim:3663) allocates a `seq[PageId]`
to track which pages to mark as committed:

```nim
var pageIds: seq[PageId] = @[]
```

**Proposed change:** For the single-page fast path, use a stack variable:

```nim
if db.txnDirtyCount == 1:
  markPageCommitted(db.pager, db.txnDirtyEntry.id, commitLsn)
else:
  # existing path with seq
```

Or, since `pageMeta` already contains the page IDs, reuse the `pageMetaBuf` from 5.3
to drive `markPagesCommitted` without a separate allocation.

**Saves:** 1 seq allocation per commit.

### 5.5 Replace `writer.pending = @[]` with `setLen(0)`

**Current behavior:** After commit (wal.nim:1311), the pending queue is reset by
assigning a new empty seq:

```nim
writer.pending = @[]
```

This deallocates the old seq and allocates a new (empty) one. Combined with the
`WalWriter` reuse from 5.1, this becomes:

**Proposed change:**
```nim
writer.pending.setLen(0)
```

`setLen(0)` retains the existing allocation and just resets the length counter.

**Saves:** 1 deallocation + 1 allocation per commit.

### 5.6 Install flushHandler Once (Not Per-Transaction)

**Current behavior:** `beginTransaction` (engine.nim:3639–3645) creates a new closure
every transaction:

```nim
db.pager.flushHandler = proc(pageId: PageId, data: string): Result[Void] =
  var payload = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr payload[0], unsafeAddr data[0], data.len)
  writer.flushPage(pageId, payload)
```

This allocates a closure object that captures `writer`. Since `writer` is now reused
(5.1), the closure can also be installed once.

**Proposed change:** Install the flushHandler during `Wal` initialization or on first
transaction, not on every `beginTransaction`. The handler references `wal.writer` which
is now a stable field.

**Saves:** 1 closure allocation per commit.

### Phase 1 Summary

| Change | Allocations Removed | Bytes Saved | Locks Avoided |
|--------|--------------------:|------------:|--------------:|
| 5.1 Reuse WalWriter | 3 | ~176B | 0 |
| 5.2 Bypass snapshotDirtyPages | 2 | ~4,200B | 32 |
| 5.3 Fixed pageMetaBuf | 1 | ~48B | 0 |
| 5.4 Eliminate pageIds seq | 1 | ~48B | 0 |
| 5.5 setLen(0) instead of @[] | 1 | ~48B | 0 |
| 5.6 Reuse flushHandler | 1 | ~32B | 0 |
| **Total** | **9** | **~4,552B** | **32** |

**Expected impact:** The 4KB string copy elimination (5.2) is the biggest single win
because it removes both the allocation and the memcpy of a full page. The WalWriter
reuse (5.1) removes 3 allocations including a Table initialization. Together, these
should reduce p95 commit latency by **20–35%** to roughly **0.049–0.060ms**.

---

## 6. Phase 2: Reduce Syscall Count to One

**Goal:** Only one syscall (`fdatasync`) on the commit hot path.  
**Estimated Impact:** 5–15% p95 reduction  
**Risk Level:** Low to Medium (depends on mmap status)  
**Prerequisite:** Phase 1 (clean allocation profile makes measurements reliable)

### 6.1 Verify and Stabilize the mmap WAL Path

**Current state (as of V1 optimization #10):** The WAL file is preallocated and mmapped
via `ensureWalMmapCapacity`. Frame encoding goes directly into the mmap region via
`encodeFrameIntoPtrString`. The WAL header is also written via mmap (`encodeWalHeaderPtr`).
The only syscall should be `fdatasync()`.

**However**, V1 rejected the "mmap-backed WAL write path" attempt because it regressed
p95 by ~1%. The subsequently accepted "WAL header + logical end offset" fix addressed
the per-commit truncation issue. The question is: **is the mmap path currently active
and stable, or does it frequently fall back to the buffered path?**

**Action items:**
1. Add lightweight instrumentation to verify mmap hit rate during the benchmark
   (e.g., a counter `mmapWriteCount` vs `bufferedWriteCount`)
2. If mmap is falling back, investigate why (capacity checks, remap frequency)
3. If mmap is stable, confirm that the commit path issues exactly 1 syscall (fdatasync)
   by running under `strace -c` for a 1000-commit benchmark run

**Expected outcome:** If mmap is working, we're already at 1 syscall per commit.
If not, fixing the fallback eliminates 2–4 unnecessary syscalls per commit.

### 6.2 Avoid Checkpoint Threshold Evaluation on Every Commit

**Current behavior:** After every commit, `commitTransaction` evaluates three
checkpoint thresholds (engine.nim:3688–3698):

1. `wal.endOffset >= checkpointEveryBytes` — simple comparison (fast)
2. `epochTime()` — calls `gettimeofday` syscall on Linux
3. `estimateIndexMemoryUsage(wal)` — acquires `indexLock`, iterates entire WAL index

For the benchmark workload (1000 single-page commits to a 1-row table), the WAL never
reaches the 64MB checkpoint threshold. These checks are pure overhead.

**Proposed change:** Use a tiered evaluation strategy:

```nim
# Only check time/memory every N commits or when WAL crosses size thresholds
db.commitsSinceLastCheck.inc
if db.commitsSinceLastCheck >= 64 or wal.endOffset >= wal.nextCheckpointCheck:
  db.commitsSinceLastCheck = 0
  wal.nextCheckpointCheck = wal.endOffset + (wal.checkpointEveryBytes div 16)
  # Now do the full evaluation including epochTime() and memory estimate
  ...
```

This reduces the `epochTime()` syscall frequency by ~16× and the index iteration
by ~16×. The byte-based check (a single comparison) runs on every commit as a
fast gate.

**Saves:** ~1 syscall (`gettimeofday`) + 1 lock acquire/release (`indexLock`) on
most commits.

### Phase 2 Summary

| Change | Syscalls Removed | Impact |
|--------|----------------:|--------|
| 6.1 Stabilize mmap path | 0–4 per commit | Only if fallback is active |
| 6.2 Lazy checkpoint eval | ~1 per commit | Removes gettimeofday + index scan |

---

## 7. Phase 3: Tame fdatasync Tail Latency

**Goal:** Reduce the p95 fdatasync cost from ~10–15µs to ~3–6µs.  
**Estimated Impact:** 10–25% p95 reduction  
**Risk Level:** Medium (platform-specific, requires testing)  
**Prerequisite:** Phase 1 and 2 (so fdatasync is the only remaining bottleneck)

### 7.1 Profile fdatasync in Isolation

Before attempting to reduce fdatasync latency, we need to measure it in isolation.

**Action:** Add a `getMonoTime()` pair around the `fsync()` call in `commit()`:

```nim
let tSync0 = getMonoTime()
let syncRes = writer.wal.vfs.fsync(writer.wal.file)
let tSync1 = getMonoTime()
# Record nanosBetween(tSync0, tSync1) in a ring buffer for analysis
```

This tells us what fraction of the 75µs p95 is fdatasync vs everything else.

**Possible outcomes:**
- If fdatasync p95 is ~5µs → overhead is mostly allocation/bookkeeping → Phase 1 is
  the main win
- If fdatasync p95 is ~30µs → storage tail latency dominates → Phase 3 is critical
- If fdatasync p95 is ~10–15µs → mixed → both phases matter

### 7.2 Reduce Write Volume per Commit

**Current state:** A single-page commit writes 4109 bytes (page frame) + 13 bytes
(commit frame) = **4122 bytes** to the WAL, plus the 32-byte header update (via mmap
memcpy). `fdatasync` must flush all of this to storage.

**The key insight:** For a TEXT column UPDATE that changes "value1" → "value2", only
~10 bytes of the 4096-byte page actually change. We're writing and syncing 4122 bytes
to persist a ~10-byte change. This is **~400× write amplification**.

**Proposal: Mini-delta WAL frames for single-page updates**

Instead of writing the full page to the WAL, write a **delta frame** that contains
only the changed bytes:

```
Delta Frame Format:
[1 byte]   Frame type (wfPageDelta = 0x04)
[4 bytes]  Page ID
[2 bytes]  Delta offset within page
[2 bytes]  Delta length
[N bytes]  Delta payload (just the changed bytes)
[8 bytes]  Trailer (reserved)
```

For the benchmark UPDATE (changing a TEXT value), the delta would be ~20–30 bytes
instead of 4096 bytes. Total commit write: ~50 bytes instead of 4122 bytes — a
**~80× reduction in write volume**.

**Impact on fdatasync:** Modern SSDs write in 4KB sectors internally, so even a
50-byte write causes a full sector write. However, `fdatasync` latency is partly
proportional to the amount of dirty data in the kernel page cache. Writing 50 bytes
means fewer dirty pages to flush than writing 4122 bytes, especially under concurrent
I/O pressure.

**Estimated impact:** 5–15% fdatasync latency reduction for the benchmark workload.
Larger wins for multi-page commits and HDD/network storage.

**Requires:** WAL format change (new frame type) → **ADR required**.

**Recovery impact:** During WAL replay, delta frames must be applied to the base page.
This requires reading the current page from the DB file, applying the delta, and
writing the result. This makes recovery slightly more complex and potentially slower,
but recovery is a rare event (only after crash).

**Checkpoint impact:** During checkpoint, delta frames must be coalesced into full
pages before writing to the DB file. This is straightforward — read the base page,
apply all deltas in order, write the result.

### 7.3 Use O_DSYNC for Write-Through Durability (Linux)

**Current state:** DecentDB opens the WAL file with standard flags (`fmReadWrite`)
and calls `fdatasync()` after every commit. This is a two-step process:
1. `write()` / memcpy → data goes to kernel page cache
2. `fdatasync()` → kernel flushes dirty pages to storage

**Alternative:** Open the WAL file with `O_DSYNC` flag. With `O_DSYNC`, every
`write()` call blocks until the data is durable. This eliminates the separate
`fdatasync()` call.

**Why this might help p95:**
- `fdatasync()` after a write sometimes has to wait for the kernel to *schedule*
  the flush, which adds scheduling jitter
- `O_DSYNC` writes go through a more direct path in the kernel I/O stack
- On some SSDs/filesystems, `O_DSYNC` enables hardware write combining

**Why this might NOT help:**
- `O_DSYNC` makes every `write()` blocking, including the WAL header write
  (if not using mmap)
- The kernel may internally just do the same fdatasync anyway
- Some filesystems (ext4 with data=ordered) already optimize this path

**Action:** Benchmark with `O_DSYNC` on the test system. If it helps, make it
a configurable VFS option (not a default change).

**Risk:** Low (opt-in only, fallback to existing behavior).

### 7.4 Pre-Warm the fdatasync Path

**Observation:** In the benchmark, the first few commits likely have higher latency
because the kernel's I/O scheduling hasn't warmed up. This inflates p95 for a
1000-iteration benchmark.

**Proposed change:** Add a warm-up phase to the benchmark:

```nim
# Warm-up: 100 commits that are NOT timed
for i in 1..100:
  discard execPrepared(stmt, @[Value(kind: vkText, bytes: toBytes("warmup"))])

# Now start timing
let start = getMonoTime()
for i in 1..iterations:
  let t0 = getMonoTime()
  ...
```

**Important:** This is a **benchmark improvement**, not a DecentDB code change. It
makes the measurement more accurate by excluding cold-start effects. The same warm-up
should be added to the SQLite benchmark for fairness.

**Risk:** None (benchmark-only change). However, this is a measurement improvement,
not a performance improvement. It may lower the reported gap without changing actual
performance. The document explicitly notes this is about measurement accuracy, not
about gaming the numbers.

---

## 8. Phase 4: Structural Advantages Over SQLite

**Goal:** Exploit DecentDB's architectural advantages to go below SQLite's floor.  
**Estimated Impact:** 5–15% below SQLite's p95  
**Risk Level:** Low to Medium  
**Prerequisite:** Phases 1–3 (must first match SQLite's efficiency)

### 8.1 Zero-Checksum Advantage

SQLite computes a cumulative checksum over every WAL frame. This is a relatively fast
custom checksum (not CRC32C), but it still costs ~3–5µs per 4KB page. DecentDB removed
per-frame checksums in V1 optimization #4.

**This is already implemented.** DecentDB's frame encoding is pure memcpy with no
checksum computation. This saves ~3–5µs per commit compared to SQLite.

**No action needed** — this advantage is already realized.

### 8.2 Smaller Frame Size Advantage

SQLite's WAL frame for a single page is 4120 bytes (24-byte header + 4096 payload).
DecentDB's is 4109 bytes (5-byte header + 4096 payload + 8-byte trailer). Plus a
13-byte commit frame. Total: 4122 bytes vs SQLite's ~4128 bytes (4120 + 8 commit
indicator). DecentDB writes ~6 fewer bytes.

**This advantage is marginal** (~0.1% fewer bytes) and unlikely to produce measurable
improvement. No action needed.

### 8.3 Native Code vs VDBE Interpreter

SQLite executes every SQL statement through a bytecode interpreter (VDBE). Each VDBE
instruction has dispatch overhead (indirect jump, operand decode). DecentDB's
`tryFastPkUpdate` is compiled native code that does a direct B+Tree lookup and in-place
page modification.

**This advantage is already implemented** via V1 optimization #8 (prepared UPDATE fast
path for INT64 PK). The benefit was within noise in V1, likely because the overhead was
masked by allocation costs. After Phase 1 eliminates allocation overhead, the native
code advantage should become measurable.

**No action needed** — this advantage is already realized but currently hidden.

### 8.4 In-Process WAL Index

SQLite maintains its WAL index in a shared memory file (`-shm`) to support multi-process
access. This requires:
- `mmap` of the SHM file
- Atomic operations with memory barriers for cross-process visibility
- Hash table lookups in the shared memory region

DecentDB's WAL index is an in-process `Table[PageId, seq[WalIndexEntry]]`. Updates are
a simple `seq.add()` under a lock. This is faster than cross-process atomic operations.

**This advantage is already present.** After Phase 1 reduces the index update overhead
(fixed-size pageMeta buffer), this should be a small but real advantage.

### 8.5 Potential: Commit Frame Elimination (Future)

SQLite uses a special field in the WAL frame header (`nTruncate`) to indicate commit
frames. When `nTruncate > 0`, the frame is a commit marker. This means SQLite doesn't
need a separate commit frame — it's embedded in the last page frame.

DecentDB writes a separate 13-byte commit frame after the page frame. This adds 13
bytes to the write volume and forces the frame encoder to do an extra iteration.

**Proposed change:** Embed the commit indicator in the page frame's trailer:

```
Current:
  [Page Frame: 4109 bytes] [Commit Frame: 13 bytes]  → 4122 bytes total

Proposed:
  [Page Frame: 4109 bytes, trailer bit 0 = isCommit]  → 4109 bytes total
```

This saves 13 bytes per commit and eliminates one frame encoding call.

**Requires:** WAL format change → **ADR required**.

**Estimated impact:** ~1–2% improvement (small but contributes to the aggregate).

---

## 9. Cross-Metric Guardrails

Inherited from V1 — these rules are non-negotiable:

**Primary metric (this plan):**
- `commit_p95_ms` (durability = safe)

**Must-not-regress metrics:**
- `read_p95_ms` (point reads)
- `join_p95_ms`
- `insert_rows_per_sec` (durability = safe)
- `db_size_mb`

**Acceptance rule:**
- For any optimization proposed here, run `nimble bench_embedded_pipeline` and compare
  aggregated outputs (median-of-runs). Only accept if commit latency improves **and**
  other metrics do not materially regress beyond run-to-run noise (±3%).

**How to enforce:**
- Record all 5 metrics for every attempted optimization
- Use the same format as V1's progress section
- Report the SQLite reference value from the same run for commit_p95_ms

---

## 10. Implementation Order and Dependencies

```
Phase 1 (allocation elimination) — no dependencies, can be done in any order:
  ┌─ 5.1 Reuse WalWriter ←──────────────────────┐
  ├─ 5.2 Bypass snapshotDirtyPages (biggest win) │ independent
  ├─ 5.3 Fixed pageMetaBuf                       │ of each
  ├─ 5.4 Eliminate pageIds seq                   │ other
  ├─ 5.5 setLen(0) instead of @[]                │
  └─ 5.6 Reuse flushHandler ─────────────────────┘
         │
         ▼
Phase 2 (syscall reduction) — depends on Phase 1 for clean measurements:
  ┌─ 6.1 Verify/stabilize mmap path
  └─ 6.2 Lazy checkpoint threshold evaluation
         │
         ▼
Phase 3 (fdatasync optimization) — depends on Phase 2 to isolate fdatasync:
  ┌─ 7.1 Profile fdatasync in isolation
  ├─ 7.2 Mini-delta WAL frames (ADR required)
  ├─ 7.3 O_DSYNC experiment
  └─ 7.4 Pre-warm benchmark (measurement accuracy)
         │
         ▼
Phase 4 (structural advantages) — depends on Phases 1–3 to be measurable:
  └─ 8.5 Commit frame elimination (ADR required)
```

**Recommended implementation order within Phase 1:**

1. **5.5 setLen(0)** — trivial one-line change, validates benchmark setup
2. **5.1 Reuse WalWriter** — high impact, moderate complexity
3. **5.2 Bypass snapshotDirtyPages** — highest impact, highest complexity
4. **5.6 Reuse flushHandler** — depends on 5.1 (stable writer reference)
5. **5.3 Fixed pageMetaBuf** — moderate impact
6. **5.4 Eliminate pageIds seq** — low impact, follows naturally from 5.2/5.3

Benchmark after each change. If any change regresses, revert and investigate before
proceeding.

---

## 11. Risk Assessment

| Change | Risk | ADR? | Reversible? |
|--------|------|------|-------------|
| 5.1 Reuse WalWriter | Low — single-writer guarantee makes this safe | No | Yes |
| 5.2 Bypass snapshotDirtyPages | Low — fast path only, multi-page falls back | No | Yes |
| 5.3 Fixed pageMetaBuf | Very Low — stack array with seq fallback | No | Yes |
| 5.4 Eliminate pageIds seq | Very Low — follows from 5.2/5.3 | No | Yes |
| 5.5 setLen(0) vs @[] | None — semantically identical | No | Yes |
| 5.6 Reuse flushHandler | Low — closure captures stable reference | No | Yes |
| 6.1 Verify mmap path | None — instrumentation only | No | Yes |
| 6.2 Lazy checkpoint eval | Low — delayed check, not skipped | No | Yes |
| 7.1 Profile fdatasync | None — instrumentation only | No | Yes |
| 7.2 Mini-delta frames | **Medium** — new frame type, recovery changes | **Yes** | Partial |
| 7.3 O_DSYNC experiment | Low — opt-in flag | No | Yes |
| 7.4 Pre-warm benchmark | None — benchmark change only | No | Yes |
| 8.5 Commit frame elimination | **Medium** — format change | **Yes** | Partial |

**ADR-required changes:** 7.2 (mini-delta frames) and 8.5 (commit frame elimination)
require ADRs per AGENTS.md because they change the WAL format. All other changes are
internal implementation details with no format impact.

---

## 12. Projected Outcome

### Conservative Projection (Phase 1 only)

Assuming Phase 1 achieves 25% p95 reduction:

| Metric | Before | After Phase 1 | SQLite | Gap |
|--------|--------|---------------|--------|-----|
| commit_p95_ms | 0.0751 | ~0.056 | 0.0094 | ~6.0× |

### Moderate Projection (Phases 1 + 2)

Assuming Phases 1+2 achieve 35% combined:

| Metric | Before | After Phase 2 | SQLite | Gap |
|--------|--------|---------------|--------|-----|
| commit_p95_ms | 0.0751 | ~0.049 | 0.0094 | ~5.2× |

### Optimistic Projection (Phases 1 + 2 + 3)

Assuming all three phases achieve 55% combined:

| Metric | Before | After Phase 3 | SQLite | Gap |
|--------|--------|---------------|--------|-----|
| commit_p95_ms | 0.0751 | ~0.034 | 0.0094 | ~3.6× |

### Aspirational Projection (All Phases + Delta Frames)

If mini-delta frames (7.2) reduce write volume by 80× and fdatasync scales accordingly:

| Metric | Before | Aspirational | SQLite | Gap |
|--------|--------|-------------|--------|-----|
| commit_p95_ms | 0.0751 | ~0.008–0.012 | 0.0094 | **0.85–1.3×** |

The aspirational target of **beating SQLite** depends on:
1. Phase 1 eliminating allocation jitter (high confidence)
2. Phase 3 mini-delta frames reducing write volume (medium confidence, ADR required)
3. fdatasync latency scaling with write volume on the test SSD (needs validation)

**If all three conditions hold, beating SQLite is achievable** because DecentDB writes
fewer bytes (no checksum), uses native code (no VDBE), and has an in-process WAL index
(no SHM).

---

## Appendix A: How to Validate

### Running the Benchmark

```bash
nimble bench_embedded_pipeline
```

This runs the full benchmark suite, aggregates results, and generates the README chart.

### Quick Commit Latency Check

```bash
nimble bench_embedded_sample
# Then examine raw results:
cat benchmarks/raw/DecentDB__sample__commit_latency__*.jsonl | jq '.metrics.p95_us'
cat benchmarks/raw/SQLite__sample__commit_latency__*.jsonl | jq '.metrics.p95_us'
```

### Profiling with strace

```bash
# Count syscalls during benchmark
strace -c -f ./benchmarks/embedded_compare/run_benchmarks 2>&1 | grep -E 'fdatasync|write|fsync'
```

### Profiling with perf

```bash
# CPU profile during benchmark
perf record -g ./benchmarks/embedded_compare/run_benchmarks
perf report --no-children
```

---

## Appendix B: SQLite Configuration Reference

The benchmark uses these SQLite settings (for fair comparison):

```sql
PRAGMA journal_mode = WAL;      -- Write-Ahead Logging (same as DecentDB)
PRAGMA synchronous = FULL;      -- fsync after every commit (same as DecentDB)
-- No explicit page_size (default 4096, same as DecentDB)
-- No explicit cache_size (default ~2000 pages)
-- No explicit mmap_size (default 0 = no mmap)
```

Both engines use:
- Prepared statements
- Single-row UPDATE (`WHERE pk = 1`)
- Auto-commit (each statement is its own transaction)
- Monotonic clock timing (`std/monotimes`)
- 1000 iterations
- No warm-up period
