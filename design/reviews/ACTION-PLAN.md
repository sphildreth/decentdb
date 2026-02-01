# DecentDB Unified Action Plan

**Date:** 2026-02-01  
**Document:** Consolidated action plan addressing all review findings  
**Status:** Implementation Ready  

---

## Executive Summary

This document consolidates findings from four comprehensive code reviews (G3PRO, KIMI-K2.5, OPUS45, Qwen) into a single prioritized action plan. Issues have been deduplicated and organized by severity, with detailed implementation guidance for each item.

### Testing Policy (NON-NEGOTIABLE)

**Unit testing is IMPERATIVE for all changes. The following rules apply:**

1. **NO TEST PATCHING**: Existing tests MUST NOT be modified to accommodate changes. If a change breaks an existing test, the implementation is wrong, not the test.

2. **MANDATORY NEW TESTS**: Every fix must include:
   - Unit tests covering the main behavior
   - Edge case tests
   - Regression tests that would have caught the original bug
   - For durability fixes: Crash-injection tests

3. **EXISTING TESTS MUST PASS**: All existing tests must continue to pass without modification. This includes:
   - Nim unit tests
   - Python crash-injection tests
   - Differential tests vs PostgreSQL
   - Property-based tests

4. **TEST-FIRST APPROACH**: Write tests that reproduce the bug BEFORE implementing the fix. The test should fail before the fix and pass after.

5. **VERIFICATION REQUIREMENTS**:
   - Run full test suite before submitting changes
   - Run crash-injection tests for any durability-related changes
   - Run differential tests for any SQL behavior changes
   - Benchmark performance-sensitive changes

**FAILURE TO FOLLOW THESE TESTING REQUIREMENTS WILL RESULT IN REJECTION OF THE CHANGE.**

### Severity Summary

| Severity | Count | Description |
|----------|-------|-------------|
| **CRITICAL** | 6 | Data loss or corruption risks, must fix before production |
| **HIGH** | 6 | Performance bottlenecks, reliability issues |
| **MEDIUM** | 6 | Optimization opportunities, architectural improvements |
| **LOW** | 5 | Documentation, monitoring, long-term enhancements |

---

## Part I: Critical Issues (Immediate Action Required)

### CRIT-001: fsync Implementation Does Not Provide True Durability

**Severity:** CRITICAL  
**Category:** ACID - Durability  
**References:** OPUS45 F-001, QWEN ACID-003  
**Files:** `src/vfs/os_vfs.nim:127-133`

#### Problem Statement

The current `fsync` implementation uses Nim's `flushFile()` which only flushes userspace buffers to the OS page cache. It does NOT invoke POSIX `fsync()` or `fdatasync()`, meaning committed transactions are NOT guaranteed to survive power failures or kernel panics.

#### Impact

- Committed transactions since the last OS-level flush (typically 30 seconds on Linux) can be lost
- Database provides durability only against application crashes, not system crashes
- Silent data loss for critical operations

#### Implementation Steps

1. **Modify `src/vfs/os_vfs.nim`**:
   ```nim
   when defined(posix):
     proc fsync(fd: cint): cint {.importc, header: "<unistd.h>".}
     proc fdatasync(fd: cint): cint {.importc, header: "<unistd.h>".}
   when defined(windows):
     proc FlushFileBuffers(hFile: Handle): WINBOOL {.importc, stdcall, dynlib: "kernel32".}
   
   method fsync*(vfs: OsVfs, file: VfsFile): Result[Void] =
     withFileLock(file):
       try:
         # First flush userspace buffers
         flushFile(file.file)
         
         # Then force to stable storage
         when defined(windows):
           let handle = get_osfhandle(file.file.getFileHandle())
           if FlushFileBuffers(handle) == 0:
             return err[Void](ERR_IO, "Fsync failed", file.path)
         else:
           let fd = cast[cint](file.file.getFileHandle())
           # Use fdatasync when available (faster, doesn't sync metadata)
           when defined(linux):
             if fdatasync(fd) != 0:
               return err[Void](ERR_IO, "Fsync failed", file.path)
           else:
             if fsync(fd) != 0:
               return err[Void](ERR_IO, "Fsync failed", file.path)
       except OSError:
         return err[Void](ERR_IO, "Fsync failed", file.path)
     okVoid()
   ```

2. **Add durability levels configuration**:
   ```nim
   type DurabilityLevel* = enum
     dlFull        # fsync on every commit (safest)
     dlNormal      # fdatasync on commit (faster, still durable)
     dlRelaxed     # fsync every N commits or M milliseconds
     dlNone        # No fsync (testing only)
   ```

3. **Testing Requirements**:
   - Add fault injection test that simulates power failure
   - Verify data survives actual fsync using sync_file_range()
   - Benchmark performance impact of proper fsync

#### Success Criteria

- [ ] fsync() system call is invoked on every commit
- [ ] Data survives simulated power failure
- [ ] No more than 20% TPS degradation with dlNormal vs dlNone

---

### CRIT-002: Checkpoint Race Condition Loses Committed Data

**Severity:** CRITICAL  
**Category:** ACID - Durability/Consistency  
**References:** KIMIK25 ACID-002, OPUS45 F-002  
**Files:** `src/wal/wal.nim:210-358`

#### Problem Statement

The checkpoint releases the WAL lock during I/O operations (line 268), allowing writers to proceed. However, pages written during this window are not captured by the checkpoint and will be lost when the WAL is truncated.

**Timeline of Failure:**
1. Checkpoint begins, calculates `safeLsn = 1000`
2. Checkpoint releases lock for I/O (line 268)
3. Writer commits page P at LSN 1001
4. Checkpoint copies page P (LSN 1000 version) to DB file
5. Checkpoint writes CHECKPOINT frame for LSN 1000
6. WAL is truncated, losing LSN 1001
7. Database file contains stale data (LSN 1000)

#### Implementation Steps

1. **Snapshot dirtySinceCheckpoint before releasing lock**:
   ```nim
   proc checkpoint*(wal: Wal, pager: Pager): Result[uint64] =
     acquire(wal.lock)
     wal.checkpointPending = true
     let lastCommit = wal.walEnd.load(moAcquire)
     
     # ... calculate safeLsn, handle timeouts ...
     
     # SNAPSHOT: Capture the state we need to checkpoint
     var toCheckpointSnapshot: seq[(PageId, uint64)] = @[]  # (pageId, targetLsn)
     acquire(wal.indexLock)
     for pageId, entry in wal.dirtySinceCheckpoint.pairs:
       if entry.lsn <= safeLsn:
         toCheckpointSnapshot.add((pageId, entry.lsn))
     release(wal.indexLock)
     
     # Now release the main lock for I/O
     release(wal.lock)
     
     # Perform I/O using the snapshot - read SPECIFIC LSN version
     for (pageId, targetLsn) in toCheckpointSnapshot:
       let frameRes = readFrameAtLsn(wal, pageId, targetLsn)
       # ... write to DB file ...
     
     # Re-acquire lock to finalize
     acquire(wal.lock)
     # ... write CHECKPOINT frame, truncate WAL ...
   ```

2. **Add helper to read specific LSN version**:
   ```nim
   proc readFrameAtLsn(wal: Wal, pageId: PageId, targetLsn: uint64): Result[seq[byte]] =
     # Scan WAL file for frame with matching pageId and LSN
     # Return frame payload
   ```

3. **Alternative: Two-phase checkpoint with intent marker**:
   - Write checkpoint-intent frame before I/O
   - Write checkpoint-complete frame after I/O
   - Recovery treats incomplete checkpoints as failed

#### Testing Requirements

- Crash test: Start checkpoint, write during checkpoint, crash, verify all committed data present
- Stress test: Continuous writes + frequent checkpoints, verify consistency
- Fault injection: Inject I/O delays during checkpoint to maximize race window

#### Success Criteria

- [ ] No committed data lost during checkpoint
- [ ] Concurrent writes during checkpoint are properly handled
- [ ] Crash during checkpoint results in recoverable state

---

### CRIT-003: Reader Timeout Allows Zombie Access to Stale Snapshots

**Severity:** CRITICAL  
**Category:** ACID - Snapshot Isolation  
**References:** KIMIK25 ACID-001  
**Files:** `src/wal/wal.nim:436-439`, `src/wal/wal.nim:448-471`

#### Problem Statement

When a reader times out, it is added to `abortedReaders` set but can continue reading pages using its original snapshot LSN. The `readGuard` check only happens at the start of cached reads, not in the core read path `getPageAtOrBefore()`.

#### Implementation Steps

1. **Add abort check to `getPageAtOrBefore()`**:
   ```nim
   proc getPageAtOrBefore*(wal: Wal, pageId: PageId, snapshot: uint64, 
                          readerId: Option[int] = none(int)): Option[seq[byte]] =
     # Check if this reader has been aborted
     if readerId.isSome:
       acquire(wal.readerLock)
       if readerId.get in wal.abortedReaders:
         release(wal.readerLock)
         return none(seq[byte])  # Signal abort to caller
       release(wal.readerLock)
     
     acquire(wal.indexLock)
     defer: release(wal.indexLock)
     # ... rest of existing logic
   ```

2. **Propagate readerId through read path**:
   - Add `readerId` parameter to `readPageWithSnapshot()`
   - Update all callers to pass reader ID
   - Handle "aborted" case by returning error

3. **Invalidate snapshot on abort (alternative)**:
   ```nim
   type AbortedReaderInfo = object
     id: int
     invalidatedSnapshot: uint64  # Set to 0 when aborted
   
   # In wal.nim line 231:
   wal.abortedReaders[info.id] = AbortedReaderInfo(id: info.id, invalidatedSnapshot: 0)
   ```

#### Testing Requirements

- Crash test: Start reader, timeout reader, verify reader gets error on next read
- Property test: Concurrent readers with timeouts, verify no zombie reads

#### Success Criteria

- [ ] Aborted readers cannot read any pages after timeout
- [ ] Error returned immediately on aborted reader access attempt
- [ ] No stale data visible after reader timeout

---

### CRIT-004: Transaction Rollback Cache Timing Gap

**Severity:** CRITICAL  
**Category:** ACID - Atomicity  
**References:** QWEN ACID-002, KIMIK25 ACID-003  
**Files:** `src/engine.nim:1669-1676`

#### Problem Statement

During rollback, the WAL is rolled back (line 1670) and `activeWriter` is set to nil (line 1671), but cache eviction happens later (line 1676). Between these steps, dirty pages remain in cache and could be accessed.

#### Implementation Steps

1. **Move cache eviction immediately after WAL rollback**:
   ```nim
   proc rollbackTransaction*(db: Db): Result[Void] =
     if not db.isOpen:
       return err[Void](ERR_INTERNAL, "Database not open")
     if db.activeWriter == nil:
       return err[Void](ERR_TRANSACTION, "No active transaction")
     
     # Acquire pager lock to prevent concurrent access
     acquire(db.pager.lock)
     defer: release(db.pager.lock)
     
     let dirtyPages = snapshotDirtyPages(db.pager)
     let rollbackRes = rollback(db.activeWriter)
     if not rollbackRes.ok:
       db.activeWriter = nil
       return err[Void](rollbackRes.err.code, rollbackRes.err.message, rollbackRes.err.context)
     
     # IMMEDIATELY evict dirty pages - don't wait
     if dirtyPages.len > 0:
       rollbackCache(db.pager)
     
     # Clear other in-memory state
     db.activeWriter = nil
     db.catalog.clearTrigramDeltas()
     
     # Reload header and catalog to revert in-memory changes
     let page1Res = readPage(db.pager, PageId(1))
     # ... reload catalog ...
   ```

#### Testing Requirements

- Unit test: Start transaction, modify pages, rollback, verify cache doesn't contain uncommitted data
- Property test: Random transaction sequences, verify cache consistency

#### Success Criteria

- [ ] No dirty pages in cache after rollback completes
- [ ] No uncommitted data visible during or after rollback

---

### CRIT-005: Trigram Index Write Amplification

**Severity:** CRITICAL  
**Category:** Performance - Write Throughput  
**References:** G3PRO Section 2.A, KIMIK25 PERF-001  
**Files:** `src/storage/storage.nim`, `src/engine.nim:1623-1628`

#### Problem Statement

The trigram inverted index uses read-modify-write on entire postings lists. For common trigrams (stop words), this is O(N) where N is the number of documents containing that trigram. Adding the 1,000,001st document with trigram "the" requires reading and rewriting 1M existing row IDs.

#### Implementation Steps

**Option A: Segmented Postings Lists (Recommended)**

1. **Change schema from single blob to segments**:
   ```nim
   # Current: Key(Trigram_Hash) -> Value[RowID_1, RowID_2, ..., RowID_N]
   # New:     Key(Trigram_Hash, Segment_ID) -> Value[RowID_List]
   
   const MaxSegmentSize = 8192  # 8KB segments
   ```

2. **Modify `storePostings` to append to last segment or create new**:
   ```nim
   proc storePostingsSegmented(pager: Pager, trigram: TrigramHash, 
                               rowIds: seq[uint64]): Result[Void] =
     # Read last segment for this trigram
     let lastSegmentOpt = getLastSegment(pager, trigram)
     
     if lastSegmentOpt.isSome:
       let lastSegment = lastSegmentOpt.get
       if lastSegment.size + rowIds.len * 8 <= MaxSegmentSize:
         # Append to existing segment
         appendToSegment(pager, trigram, lastSegment.id, rowIds)
       else:
         # Create new segment
         let newSegmentId = lastSegment.id + 1
         createSegment(pager, trigram, newSegmentId, rowIds)
     else:
       # First segment for this trigram
       createSegment(pager, trigram, 0, rowIds)
   ```

3. **Modify `getTrigramPostingsWithDeltas` to read all segments**:
   ```nim
   proc getTrigramPostingsSegmented(pager: Pager, trigram: TrigramHash): Result[seq[uint64]] =
     var allRowIds: seq[uint64] = @[]
     var segmentId = 0
     
     while true:
       let segmentOpt = readSegment(pager, trigram, segmentId)
       if segmentOpt.isNone:
         break
       allRowIds.add(segmentOpt.get.rowIds)
       segmentId.inc
     
     ok(allRowIds)
   ```

**Option B: Delta-Log (LSM) Approach** (Alternative)

1. Write delta records instead of merging: `(Trigram_Hash, Transaction_ID) -> [+RowID_A, +RowID_B]`
2. Background thread periodically merges deltas into base records
3. Faster writes, slower reads (merge on the fly)

#### Testing Requirements

- Benchmark: Measure TPS with segmented vs non-segmented trigram index
- Verify index consistency with 1M+ documents
- Test stop word handling (trigrams appearing in 90%+ of docs)

#### Success Criteria

- [ ] Trigram updates are O(1) relative to total list size (not O(N))
- [ ] Write throughput maintains >1000 TPS with trigram indexes
- [ ] No regression in query performance for trigram searches

---

### CRIT-006: WAL Global Lock Bottleneck

**Severity:** CRITICAL  
**Category:** Performance - Concurrency  
**References:** QWEN ACID-001  
**Files:** `src/wal/wal.nim` (multiple locations)

#### Problem Statement

The WAL uses a single global lock (`wal.lock`) for all operations, serializing:
- Transaction begin/commit/rollback
- Page writes
- Checkpoint operations
- Reader management

This creates severe write throughput limitations.

#### Implementation Steps

**Phase 1: Document Lock Hierarchy**

1. **Add lock ordering documentation to SPEC.md**:
   ```markdown
   ## Lock Hierarchy (Total Order)
   
   All locks must be acquired in this order:
   1. **wal.lock** - Main WAL writer lock
   2. **wal.indexLock** - WAL index mutations  
   3. **wal.readerLock** - Reader tracking
   4. **pager.lock** - Pager metadata
   5. **shard.lock** - Per cache shard (16 shards)
   6. **entry.lock** - Per page entry
   ```

**Phase 2: Fine-Grained Locking (Post-1.0)**

1. **Split into multiple locks**:
   ```nim
   type Wal* = ref object
     commitLock*: Lock          # Separate lock for commit operations
     indexLock*: Lock           # Lock for WAL index updates (already exists)
     readerLock*: Lock          # Lock for reader management (already exists)
     checkpointLock*: Lock      # Lock for checkpoint coordination
   ```

2. **Implement lock-free pending write queue**:
   ```nim
   type PendingWrite = object
     pageId: PageId
     data: seq[byte]
     txnId: uint64
   
   # Use atomic operations to add to pending queue
   # Single coordinator thread commits batches
   ```

**Note:** The single-writer model is intentional for 0.x. This fix targets post-1.0 multi-writer support.

#### Testing Requirements

- Performance test: Measure throughput with increasing concurrent operations
- Verify no deadlocks with new lock scheme
- Ensure ACID properties maintained under concurrent load

#### Success Criteria

- [ ] Lock hierarchy documented and enforced
- [ ] No deadlocks in multi-threaded scenarios
- [ ] Throughput scales with concurrent readers (not writers - single-writer model)

---

## Part II: High Priority Issues

### HIGH-001: Unbounded WAL Index Memory Growth

**Severity:** HIGH  
**Category:** Performance - Memory  
**References:** KIMIK25 PERF-003, OPUS45 F-006  
**Files:** `src/wal/wal.nim:43-44`

#### Problem Statement

The WAL maintains two unbounded hash tables (`index` and `dirtySinceCheckpoint`) that grow linearly with writes until checkpoint. A 10M row bulk load can consume 840MB+ memory.

#### Implementation Steps

1. **Add memory-based checkpoint trigger**:
   ```nim
   type Wal* = ref object
     # ... existing fields ...
     maxIndexMemoryBytes: int64  # New: configurable limit
   
   proc maybeCheckpoint*(wal: Wal, pager: Pager): Result[bool] =
     var trigger = false
     
     # Existing triggers
     if wal.checkpointEveryBytes > 0 and wal.endOffset >= wal.checkpointEveryBytes:
       trigger = true
     if wal.checkpointEveryMs > 0:
       let elapsedMs = int64((epochTime() - wal.lastCheckpointAt) * 1000)
       if elapsedMs >= wal.checkpointEveryMs:
         trigger = true
     
     # NEW: Memory-based trigger
     if wal.maxIndexMemoryBytes > 0:
       let estimatedMemory = estimateWalIndexMemory(wal)
       if estimatedMemory >= wal.maxIndexMemoryBytes:
         trigger = true
         wal.recordWarningLocked("Checkpoint triggered due to memory pressure: " & 
                                $estimatedMemory & " bytes")
     
     if not trigger:
       return ok(false)
     checkpoint(wal, pager)
   
   proc estimateWalIndexMemory(wal: Wal): int64 =
     let indexOverhead = wal.index.len.int64 * 64
     let dirtyOverhead = wal.dirtySinceCheckpoint.len.int64 * 64
     var entriesSize: int64 = 0
     for pageId, entries in wal.index.pairs:
       entriesSize += entries.len.int64 * 16
     indexOverhead + dirtyOverhead + entriesSize
   ```

2. **Add configuration option**:
   ```nim
   proc setCheckpointConfig*(wal: Wal, everyBytes: int64, everyMs: int64, 
                            readerWarnMs: int64 = 0, readerTimeoutMs: int64 = 0,
                            maxMemoryBytes: int64 = 256 * 1024 * 1024) =  # 256MB default
   ```

#### Testing Requirements

- Memory benchmark: Track WAL index memory during bulk load
- Stress test: Verify checkpoint triggers when memory limit reached
- Verify no OOM with 100M row dataset

#### Success Criteria

- [ ] Checkpoint triggers when memory exceeds threshold
- [ ] No OOM on write-heavy workloads
- [ ] Configurable memory limit

---

### HIGH-002: Freelist Operations Not Atomic

**Severity:** HIGH  
**Category:** ACID - Internal Consistency  
**References:** OPUS45 F-004  
**Files:** `src/pager/pager.nim:521-579`

#### Problem Statement

Freelist management modifies multiple structures (freelist pages, header) without atomicity. A crash between operations can leave the freelist inconsistent, causing space leaks or double allocation.

#### Implementation Steps

**Option A: Self-Describing Freelist (Recommended)**

1. **Make freelist self-consistent**:
   ```nim
   # Freelist page format:
   # [0:4]   - Magic number (0xFREE)
   # [4:8]   - Next page pointer
   # [8:12]  - Count of entries in THIS page
   # [12:16] - Checksum of this page
   # [16:N]  - Page IDs
   
   proc validateFreelist*(pager: Pager): Result[uint32] =
     ## Walk the freelist and return the actual count
     var count: uint32 = 0
     var current = pager.header.freelistHead
     while current != 0:
       let (next, pageCount, _) = readFreelistPage(pager, current)
       count += pageCount
       current = next
     ok(count)
   
   proc repairFreelist*(pager: Pager): Result[Void] =
     ## Repair header count if inconsistent
     let actualCount = validateFreelist(pager)
     if actualCount.value != pager.header.freelistCount:
       pager.header.freelistCount = actualCount.value
       updateHeader(pager)
   ```

2. **Call repair on startup**:
   ```nim
   proc openDb*(...): Result[Db] =
     # ... existing code ...
     let repairRes = repairFreelist(pager)
     if not repairRes.ok:
       return err[Db](repairRes.err.code, "Freelist repair failed")
   ```

#### Testing Requirements

- Crash test: Simulate crash during freelist modification
- Verify repair on startup fixes inconsistencies
- Property test: Random allocate/free sequences

#### Success Criteria

- [ ] Freelist self-consistent without relying on header count
- [ ] Automatic repair on startup
- [ ] No space leaks from freelist inconsistencies

---

### HIGH-003: Orphaned Page Allocation on Rollback

**Severity:** HIGH  
**Category:** ACID - Resource Management  
**References:** OPUS45 F-005  
**Files:** `src/engine.nim`, `src/storage/storage.nim`

#### Problem Statement

When a transaction allocates pages but then rolls back, those pages are not returned to the freelist. They become "orphaned" - consuming space but unreachable.

#### Implementation Steps

**Option A: Transaction-Local Allocation Tracking**

1. **Track allocations during transaction**:
   ```nim
   type Transaction = object
     allocatedPages: seq[PageId]  # Pages allocated during this tx
     freedPages: seq[PageId]      # Pages freed during this tx
   
   proc allocatePageInTx*(tx: var Transaction, pager: Pager): Result[PageId] =
     let pageRes = allocatePage(pager)
     if pageRes.ok:
       tx.allocatedPages.add(pageRes.value)
     pageRes
   
   proc rollbackTransaction*(db: Db): Result[Void] =
     # ... existing rollback code ...
     
     # Return allocated pages to freelist
     for pageId in db.currentTx.allocatedPages:
       discard freePage(db.pager, pageId)
     
     db.currentTx.allocatedPages = @[]
     db.currentTx.freedPages = @[]
   ```

**Option B: Garbage Collection (Pragmatic)**

1. **Provide GC utility for offline cleanup**:
   ```nim
   proc collectGarbage*(db: Db): Result[int] =
     ## Find and reclaim orphaned pages
     var reachable = initHashSet[PageId]()
     
     # Mark phase: walk all reachable pages
     markReachablePages(db.pager, PageId(1), reachable)  # Header
     markReachablePages(db.pager, db.catalog.rootPage, reachable)  # Catalog
     for table in db.catalog.tables.values:
       markReachablePages(db.pager, table.rootPage, reachable)  # Tables
     for index in db.catalog.indexes.values:
       markReachablePages(db.pager, index.rootPage, reachable)  # Indexes
     
     # Sweep phase: find pages not in reachable set or freelist
     var orphaned: seq[PageId] = @[]
     for pageId in 2'u32 .. db.pager.pageCount:
       if PageId(pageId) notin reachable and not isInFreelist(db.pager, PageId(pageId)):
         orphaned.add(PageId(pageId))
     
     # Reclaim orphaned pages
     for pageId in orphaned:
       freePage(db.pager, pageId)
     
     ok(orphaned.len)
   ```

#### Testing Requirements

- Unit test: Allocate pages in transaction, rollback, verify pages returned to freelist
- GC test: Create orphaned pages, run GC, verify reclamation

#### Success Criteria

- [ ] No orphaned pages after rollback
- [ ] GC utility available for offline cleanup
- [ ] Space reclaimed after failed transactions

---

### HIGH-004: WAL Recovery Incompleteness

**Severity:** HIGH  
**Category:** ACID - Recovery  
**References:** OPUS45 F-003, QWEN ACID-006  
**Files:** `src/wal/wal.nim:376-411`

#### Problem Statement

The WAL recovery process:
1. Ignores checkpoint frames (doesn't process them)
2. Doesn't verify DB header consistency after recovery
3. `overriddenPages` set is lost on restart

This can lead to stale reads or index inconsistencies.

#### Implementation Steps

1. **Process checkpoint frames during recovery**:
   ```nim
   proc recover*(wal: Wal): Result[Void] =
     # ... existing code ...
     var lastCheckpointLsn: uint64 = 0
     
     while true:
       # ... read frame ...
       case frameType
       of wfCheckpoint:
         let payloadLsn = readU64LE(payload, 0)
         lastCheckpointLsn = max(lastCheckpointLsn, payloadLsn)
         # Clear index entries older than checkpoint LSN
         for pageId, entries in wal.index.mpairs:
           entries = entries.filterIt(it.lsn > lastCheckpointLsn)
       # ... other cases ...
   ```

2. **Verify DB header consistency**:
   ```nim
   # In openDb(), after recover():
   if pager.header.lastCheckpointLsn != wal.lastRecoveredCheckpointLsn:
     # Inconsistency detected
     if pager.header.lastCheckpointLsn > wal.lastRecoveredCheckpointLsn:
       return err[Db](ERR_CORRUPTION, "WAL/Header checkpoint mismatch")
   ```

3. **Add post-recovery verification**:
   ```nim
   proc verifyRecovery*(db: Db): Result[Void] =
     # Read page 1 (header) - must always be valid
     let headerRes = readPage(db.pager, PageId(1))
     if not headerRes.ok:
       return err[Void](ERR_CORRUPTION, "Header page unreadable after recovery")
     
     # Verify catalog root is accessible
     if db.pager.header.rootCatalog != 0:
       let catalogRes = readPage(db.pager, PageId(db.pager.header.rootCatalog))
       if not catalogRes.ok:
         return err[Void](ERR_CORRUPTION, "Catalog root unreadable after recovery")
     
     okVoid()
   ```

#### Testing Requirements

- Crash test: Crash during checkpoint, verify proper recovery
- Corruption test: Manually corrupt header, verify detection
- Property test: Random sequences of writes + crashes

#### Success Criteria

- [ ] Checkpoint frames processed during recovery
- [ ] Header/WAL consistency verified
- [ ] Corruption detected early with clear error messages

---

### HIGH-005: Constraint Checking Performance Impact

**Severity:** HIGH  
**Category:** Performance - Bulk Operations  
**References:** QWEN ACID-005, KIMIK25 ACID-004  
**Files:** `src/engine.nim`

#### Problem Statement

Constraint checking happens synchronously during each operation with individual index lookups. This severely impacts bulk operations performance.

#### Implementation Steps

1. **Batch constraint validation**:
   ```nim
   proc validateConstraintsBatch(catalog: Catalog, pager: Pager, 
                                 table: TableMeta, 
                                 rows: seq[seq[Value]]): Result[Void] =
     # Collect all values that need constraint checking
     var uniqueValues: Table[string, seq[(int, Value)]]  # column -> [(rowIdx, value)]
     var fkValues: Table[string, seq[(int, Value)]]      # column -> [(rowIdx, value)]
     
     for rowIdx, row in rows:
       for colIdx, col in table.columns:
         if col.unique or col.primaryKey:
           uniqueValues[col.name].add((rowIdx, row[colIdx]))
         if col.foreignKey.isSome:
           fkValues[col.name].add((rowIdx, row[colIdx]))
     
     # Perform batch lookups
     for colName, values in uniqueValues:
       let duplicates = findDuplicatesBatch(pager, table, colName, values)
       if duplicates.len > 0:
         return err[Void](ERR_CONSTRAINT, "Unique constraint violation")
     
     # Validate FKs in batch using range scan
     for colName, values in fkValues:
       let violations = validateForeignKeysBatch(pager, table, colName, values)
       if violations.len > 0:
         return err[Void](ERR_CONSTRAINT, "Foreign key constraint violation")
     
     okVoid()
   ```

2. **Document current statement-time enforcement**:
   - Update SPEC.md to clarify FK constraints are enforced at statement time
   - Note this differs from standard SQL transaction-time enforcement
   - Provide workarounds for complex migrations

#### Testing Requirements

- Benchmark: Compare bulk insert with/without batch constraint checking
- Verify constraint violations still detected correctly
- Test with 100K+ row batches

#### Success Criteria

- [ ] Bulk operations validate constraints in batches
- [ ] 10x+ improvement in bulk insert performance
- [ ] All constraints still enforced correctly

---

### HIGH-006: Long-Running Reader Memory Management

**Severity:** HIGH  
**Category:** Performance - Resource Management  
**References:** QWEN ACID-007  
**Files:** `src/wal/wal.nim`

#### Problem Statement

Long-running readers prevent WAL truncation, causing memory and disk growth. Current timeout mechanisms may not be sufficient.

#### Implementation Steps

1. **Enhance resource management**:
   ```nim
   type ReaderInfo = object
     id: int
     snapshot: uint64
     startTime: float
     lastAccessTime: float
     memoryEstimate: int64  # Track estimated memory usage
     warningCount: int      # Number of warnings issued
   
   proc checkReaderResources(wal: Wal) =
     let now = epochTime()
     for reader in wal.readers.values:
       let duration = now - reader.startTime
       let idle = now - reader.lastAccessTime
       
       # Soft limit: warning
       if duration > wal.readerWarnMs.float / 1000.0:
         if reader.warningCount == 0:
           logWarning("Reader " & $reader.id & " has been active for " & $duration & "s")
           reader.warningCount.inc
       
       # Hard limit: abort
       if duration > wal.readerTimeoutMs.float / 1000.0:
         abortReader(wal, reader.id)
   ```

2. **Add configurable policies**:
   ```nim
   type ReaderPolicy* = enum
     rpLenient    # Long timeouts, many warnings
     rpNormal     # Balanced
     rpStrict     # Short timeouts, quick abort
   ```

#### Testing Requirements

- Stress test: Long-running reader with continuous writes
- Verify WAL truncation eventually occurs
- Test resource limits enforcement

#### Success Criteria

- [ ] Resource tracking per reader
- [ ] Configurable policies for different workloads
- [ ] WAL truncation not blocked indefinitely

---

## Part III: Medium Priority Issues

### MED-001: B-Tree Internal Node Search Algorithm

**Severity:** MEDIUM  
**Category:** Performance - Query  
**References:** KIMIK25 PERF-004  
**Files:** `src/btree/btree.nim`

#### Problem Statement

B-Tree internal node search may be implemented as linear search (O(n)) instead of binary search (O(log n)). With 100-500 keys per node, this is ~28x more comparisons.

#### Implementation Steps

1. **Verify current implementation**:
   - Review `src/btree/btree.nim` for search implementation
   - Check if `findChildPage` uses binary search

2. **Implement binary search if needed**:
   ```nim
   proc findChildPage(keys: seq[uint64], target: uint64): int =
     # Binary search for the child pointer
     var left = 0
     var right = keys.len
     while left < right:
       let mid = (left + right) div 2
       if keys[mid] <= target:
         left = mid + 1
       else:
         right = mid
     return left  # This is the correct child index
   ```

3. **Ensure keys are sorted**:
   - Verify `readInternalCells` returns sorted keys
   - Add assertion in debug builds

#### Testing Requirements

- Microbenchmark: B-Tree lookup latency vs dataset size
- Profile: Check if findChildPage is a hot function
- Compare: Linear vs binary search performance

#### Success Criteria

- [ ] Binary search used for internal node navigation
- [ ] P95 point lookup < 10ms on 9.5M rows (per PRD)

---

### MED-002: Cache Shard Hotspotting

**Severity:** MEDIUM  
**Category:** Performance - Cache  
**References:** KIMIK25 PERF-002  
**Files:** `src/pager/pager.nim:112-114`

#### Problem Statement

The pager uses 16 shards with simple modulo hashing. Sequential access patterns (e.g., table scans) can concentrate on few shards, causing uneven utilization.

#### Implementation Steps

1. **Use better hash function**:
   ```nim
   proc splitmix64(x: uint64): uint64 =
     var z = x + 0x9e3779b97f4a7c15'u64
     z = (z xor (z shr 30)) * 0xbf58476d1ce4e5b9'u64
     z = (z xor (z shr 27)) * 0x94d049bb133111eb'u64
     z xor (z shr 31)
   
   proc shardFor(cache: PageCache, pageId: PageId): PageCacheShard =
     let mixed = splitmix64(uint64(pageId))
     let idx = int(mixed mod uint64(cache.shards.len))
     cache.shards[idx]
   ```

2. **Consider larger shard count**:
   - Increase from 16 to 64 or 128 shards
   - Minimal code change, slightly more memory overhead

#### Testing Requirements

- Benchmark: Sequential scan performance vs random access
- Measure: Cache hit rate under different access patterns
- Compare: Different hash functions

#### Success Criteria

- [ ] Even cache distribution across shards
- [ ] No performance degradation on sequential scans

---

### MED-003: Trigram Delta Flush on Every Commit

**Severity:** MEDIUM  
**Category:** Performance - Write Throughput  
**References:** KIMIK25 PERF-001  
**Files:** `src/engine.nim:1623-1628`

#### Problem Statement

Every `commitTransaction` unconditionally flushes trigram deltas, causing severe write throughput degradation. 1000 small transactions/sec becomes 100 TPS with trigram indexes.

#### Implementation Steps

**Option A: Move Trigram Flush to Checkpoint (Recommended)**

1. **Remove trigram flush from commit path**:
   ```nim
   proc commitTransaction*(db: Db): Result[Void] =
     # ... existing validation and WAL write ...
     
     # REMOVED: trigram flush from critical path
     # let trigramFlushRes = flushTrigramDeltas(db.pager, db.catalog)
     
     # Add trigram metadata to commit record for recovery
     let trigramMeta = db.catalog.getTrigramDeltaMetadata()
     # ... include in WAL commit frame ...
     
     okVoid()
   ```

2. **Add trigram flush to checkpoint**:
   ```nim
   proc checkpoint*(wal: Wal, pager: Pager): Result[uint64] =
     # ... existing checkpoint logic ...
     
     # ADDED: Flush trigrams during checkpoint (not on every commit)
     let trigramFlushRes = flushTrigramDeltas(pager, catalog)
     if not trigramFlushRes.ok:
       return err[uint64](trigramFlushRes.err.code, "Trigram flush failed during checkpoint")
     
     # ... continue with WAL truncation ...
   ```

3. **Add recovery/rebuild logic**:
   - On startup, check if trigram deltas were lost (crash between commit and checkpoint)
   - Provide index rebuild utility for offline reconstruction

**Trade-off:** If crash occurs between commits, trigram deltas may be lost. Queries might miss results until index is rebuilt. **Acceptable for 0.x**.

#### Testing Requirements

- Benchmark: Compare TPS with/without trigram indexes
- Benchmark: Measure trigram flush latency impact on commit time
- Property test: Verify index consistency with deferred flush

#### Success Criteria

- [ ] Trigram flush moved out of commit critical path
- [ ] 10x improvement in write throughput with trigram indexes
- [ ] Recovery/rebuild utility available

---

### MED-004: Clock Eviction O(N) Deletion

**Severity:** MEDIUM  
**Category:** Performance - Cache  
**References:** OPUS45 F-007  
**Files:** `src/pager/pager.nim:116-151`

#### Problem Statement

Clock eviction maintains a `seq[PageId]` for the clock hand. Deleting from the middle is O(N) as it shifts all subsequent elements.

#### Implementation Steps

1. **Use mark-and-compact approach**:
   ```nim
   type ClockEntry = object
     pageId: PageId
     valid: bool  # false = tombstone
   
   # On eviction, just mark as invalid
   shard.clock[currentIndex].valid = false
   shard.tombstoneCount.inc
   
   # Periodically compact when tombstones exceed threshold
   if shard.tombstoneCount > shard.clock.len div 4:
     compactClock(shard)
   
   proc compactClock(shard: PageCacheShard) =
     shard.clock = shard.clock.filterIt(it.valid)
     shard.tombstoneCount = 0
     if shard.clockHand >= shard.clock.len:
       shard.clockHand = 0
   ```

#### Testing Requirements

- Benchmark: Cache churn with large cache (10K+ pages)
- Measure: Eviction latency during high churn

#### Success Criteria

- [ ] O(1) eviction marking
- [ ] Periodic compaction keeps tombstones bounded
- [ ] No latency spikes during cache eviction

---

### MED-005: Lock Contention During Commit

**Severity:** MEDIUM  
**Category:** Performance - Concurrency  
**References:** OPUS45 F-008  
**Files:** `src/pager/pager.nim:370-391`

#### Problem Statement

`flushAll` and `snapshotDirtyPages` iterate through all cache shards holding each shard lock. During commit, this blocks concurrent readers.

#### Implementation Steps

1. **Use read-write locks**:
   ```nim
   type PageCacheShard = ref object
     rwlock: RWLock  # Readers don't block each other
     # ... existing fields ...
   ```

2. **Implement copy-on-write dirty tracking**:
   ```nim
   type PageCacheShard = ref object
     dirtySet: Atomic[ptr HashSet[PageId]]  # Swapped atomically
   ```

**Note:** Lower priority since single-writer model means limited concurrency anyway.

#### Testing Requirements

- Stress test: Concurrent readers during large transaction commit
- Measure: Reader stall durations

#### Success Criteria

- [ ] Reduced reader stalls during commit
- [ ] No deadlock scenarios

---

### MED-006: WAL Index O(N) Lookup

**Severity:** MEDIUM  
**Category:** Performance - Read  
**References:** OPUS45 F-006  
**Files:** `src/wal/wal.nim:448-471`

#### Problem Statement

`getPageAtOrBefore` performs linear scan through all WAL entries for a page to find version at or before snapshot LSN. For frequently updated pages, this is O(V) where V is number of versions.

#### Implementation Steps

1. **Keep entries sorted and use binary search**:
   ```nim
   proc getPageAtOrBefore*(wal: Wal, pageId: PageId, snapshot: uint64): Option[seq[byte]] =
     acquire(wal.indexLock)
     defer: release(wal.indexLock)
     
     if not wal.index.hasKey(pageId):
       return none(seq[byte])
     
     let entries = wal.index[pageId]
     
     # Binary search for largest LSN <= snapshot
     var lo = 0
     var hi = entries.len
     while lo < hi:
       let mid = (lo + hi) div 2
       if entries[mid].lsn <= snapshot:
         lo = mid + 1
       else:
         hi = mid
     
     if lo == 0:
       return none(seq[byte])
     
     let bestEntry = entries[lo - 1]
     let frameRes = readFrame(wal.vfs, wal.file, bestEntry.offset)
     # ...
   ```

2. **Ensure entries are added in sorted order** (they should be since LSNs increase)

#### Testing Requirements

- Benchmark: Page read latency with 10K versions
- Compare: Linear vs binary search

#### Success Criteria

- [ ] O(log N) lookup instead of O(N)
- [ ] Improved read performance for hot pages

---

## Part IV: Low Priority Issues

### LOW-001: Document Snapshot Isolation Phantom Reads

**Severity:** LOW  
**Category:** Documentation  
**References:** QWEN ACID-004  

#### Problem Statement

The database implements snapshot isolation which allows phantom read anomalies. This is acceptable but needs documentation.

#### Implementation Steps

1. **Update SPEC.md**:
   ```markdown
   ## Isolation Levels
   
   DecentDB implements **Snapshot Isolation** (also known as MVCC).
   
   **Guarantees:**
   - No dirty reads
   - No non-repeatable reads
   - No phantom reads within a snapshot
   
   **Limitations:**
   - Phantom reads can occur between different queries in the same transaction
   - Not serializable (write skew possible)
   
   **Future:** Serializable Snapshot Isolation (SSI) may be added post-1.0.
   ```

---

### LOW-002: B+Tree Page Splitting Efficiency

**Severity:** LOW  
**Category:** Performance  
**References:** QWEN PERF-001  
**Files:** `src/btree/btree.nim`

#### Problem Statement

B+Tree may suffer from frequent page splits during insert-heavy workloads, causing fragmentation.

#### Implementation Steps

1. **Implement optimistic splits**:
   - Pre-allocate space during bulk operations
   - Use more efficient split algorithms
   - Consider page fill factor tuning

**Note:** Lower priority - current implementation is acceptable for 0.x.

---

### LOW-003: Cache Replacement Algorithm

**Severity:** LOW  
**Category:** Performance  
**References:** QWEN PERF-002  
**Files:** `src/pager/pager.nim`

#### Problem Statement

Current clock algorithm may not be optimal for all access patterns.

#### Implementation Steps

1. **Consider Adaptive Replacement Cache (ARC)**:
   - Adapts to workload patterns
   - Better hit rates for temporal locality

**Note:** Lower priority - only if cache hit rates prove problematic.

---

### LOW-004: Overflow Chain Management

**Severity:** LOW  
**Category:** Performance  
**References:** QWEN PERF-003  
**Files:** `src/record/record.nim`

#### Problem Statement

Large values in overflow chains may not be efficiently cached or accessed.

#### Implementation Steps

1. **Implement overflow chain prefetching**:
   - When reading first overflow page, prefetch subsequent pages
   - Reduces I/O round trips for large values

**Note:** Lower priority - depends on workload characteristics.

---

### LOW-005: Comprehensive Metrics/Telemetry

**Severity:** LOW  
**Category:** Observability  
**References:** QWEN ARCH-001  

#### Problem Statement

Database lacks comprehensive metrics for production monitoring.

#### Implementation Steps

1. **Add metrics collection hooks**:
   ```nim
   type DbMetrics = object
     transactionCount: Atomic[int64]
     commitCount: Atomic[int64]
     rollbackCount: Atomic[int64]
     cacheHits: Atomic[int64]
     cacheMisses: Atomic[int64]
     walFramesWritten: Atomic[int64]
     checkpointCount: Atomic[int64]
     avgCommitLatencyMs: Atomic[int64]
   ```

2. **Expose metrics via callback or query interface**:
   ```nim
   proc getMetrics*(db: Db): DbMetrics =
     db.metrics
   ```

**Note:** Lower priority - operational enhancement for post-1.0.

---

## Part V: Implementation Roadmap

### Phase 1: Critical Fixes (Week 1-2) 
**Goal:** Address data loss and corruption risks

1. **CRIT-001**: Implement proper fsync() - Days 1-2
2. **CRIT-002**: Fix checkpoint race condition - Days 3-5
3. **CRIT-003**: Fix reader timeout zombie access - Days 6-7
4. **CRIT-004**: Fix rollback cache timing - Days 8-9
5. **Testing**: Add crash tests for all critical fixes - Days 10-14

### Phase 2: Performance Optimization (Week 3-6)
**Goal:** Address major performance bottlenecks

1. **CRIT-005**: Implement segmented trigram postings - Week 3-4
2. **HIGH-001**: Add memory-based checkpoint trigger - Week 4
3. **HIGH-005**: Batch constraint validation - Week 5
4. **MED-001**: Verify/fix B-Tree binary search - Week 5
5. **MED-006**: Implement binary search in WAL index - Week 6

### Phase 3: Reliability Improvements (Week 7-10)
**Goal:** Improve internal consistency and recovery

1. **HIGH-002**: Self-describing freelist - Week 7
2. **HIGH-003**: Transaction-local page tracking - Week 8
3. **HIGH-004**: Complete WAL recovery - Week 9
4. **HIGH-006**: Enhanced reader resource management - Week 10

### Phase 4: Polish and Optimization (Week 11-12)
**Goal:** Cache optimization and documentation

1. **MED-002**: Better cache shard hashing - Week 11
2. **MED-003**: Move trigram flush to checkpoint - Week 11
3. **MED-004**: Mark-and-compact clock eviction - Week 12
4. **LOW-001**: Document isolation levels - Week 12

### Phase 5: Future (Post-1.0)

1. **CRIT-006**: Fine-grained WAL locking
2. **MED-005**: Lock contention improvements
3. **LOW-002** through **LOW-005**: Enhanced features

---

## Part VI: Testing Requirements and Procedures

### Testing Philosophy

**ALL IMPLEMENTATIONS MUST INCLUDE TESTS. NO EXCEPTIONS.**

Testing is not optional. Each implementation agent must:
1. Write tests that reproduce the bug/issue BEFORE implementing the fix
2. Verify the test fails with the current code
3. Implement the fix
4. Verify the test passes with the fix
5. Run ALL existing tests to ensure no regressions
6. Never modify existing tests to make them pass (this indicates the fix is wrong)

### Test Categories

#### 1. Unit Tests (Nim)

**Location**: `tests/` directory (follow existing structure)

**Requirements**:
- Test the specific function/method being fixed
- Test edge cases and boundary conditions
- Test error handling paths
- Mock dependencies where appropriate

**Example for CRIT-001 (fsync)**:
```nim
# tests/test_vfs_fsync.nim
import unittest
import ../src/vfs/os_vfs

suite "fsync durability":
  test "fsync actually calls fsync system call":
    let vfs = newOsVfs()
    let file = vfs.open("test.db", fmReadWrite).value
    
    # Write data
    let writeRes = file.write("test data")
    check writeRes.ok
    
    # Call fsync
    let fsyncRes = vfs.fsync(file)
    check fsyncRes.ok
    
    # Verify durability by checking system call was made
    # (Use strace or similar in CI)
    
  test "fsync returns error on failure":
    # Mock failure scenario
    let vfs = newOsVfs()
    let file = vfs.open("test.db", fmReadWrite).value
    
    # Inject fault
    vfs.injectFault("fsync")
    
    let fsyncRes = vfs.fsync(file)
    check not fsyncRes.ok
    check fsyncRes.err.code == ERR_IO
```

#### 2. Crash-Injection Tests (Python)

**Location**: `tests/crash/` directory

**Requirements**:
- Use FaultyVFS to simulate crashes
- Test all durability fixes
- Verify data integrity after simulated crashes
- Test partial write scenarios

**Example for CRIT-002 (checkpoint race)**:
```python
# tests/crash/test_checkpoint_race.py
import decentdb
import faulty_vfs

def test_no_data_loss_during_checkpoint():
    """
    Simulate writes during checkpoint and verify no data loss.
    """
    vfs = faulty_vfs.FaultyVFS()
    db = decentdb.open_db("test.db", vfs=vfs)
    
    # Start a transaction
    db.begin_transaction()
    
    # Insert data
    db.execute("INSERT INTO test VALUES (1, 'data')")
    
    # Trigger checkpoint in background
    # (Simulate with thread or subprocess)
    checkpoint_thread = threading.Thread(target=db.checkpoint)
    checkpoint_thread.start()
    
    # Continue writing during checkpoint
    time.sleep(0.01)  # Small delay to get into checkpoint I/O phase
    db.execute("INSERT INTO test VALUES (2, 'more data')")
    
    # Commit
    db.commit()
    checkpoint_thread.join()
    
    # Simulate crash by reopening
    db.close()
    db = decentdb.open_db("test.db", vfs=vfs)
    
    # Verify all committed data is present
    result = db.execute("SELECT COUNT(*) FROM test")
    assert result[0][0] == 2, f"Expected 2 rows, got {result[0][0]}"
```

#### 3. Differential Tests (Python)

**Location**: `tests/differential/` directory

**Requirements**:
- Compare behavior with PostgreSQL
- Ensure SQL semantics match (within documented limitations)
- Run for any SQL behavior changes

**Example**:
```python
# tests/differential/test_sql_semantics.py
import decentdb
import psycopg2

def test_fk_enforcement_matches_postgres():
    """
    Verify FK behavior matches PostgreSQL (statement-time enforcement).
    """
    # Run same operations on both databases
    # Compare results
```

#### 4. Property-Based Tests (Nim)

**Location**: `tests/property/` directory

**Requirements**:
- Generate random operation sequences
- Verify invariants hold
- Use for complex stateful systems

**Example**:
```nim
# tests/property/test_wal_invariants.nim
import quickcheck
import ../src/wal/wal

proc prop_wal_invariant(wal: Wal, operations: seq[Operation]): bool =
  # Apply random operations
  # Verify invariants hold
  # e.g., wal.index always consistent with wal file
```

#### 5. Performance Benchmarks (Nim)

**Location**: `benchmarks/` directory

**Requirements**:
- Benchmark before and after changes
- Measure TPS, latency, memory usage
- Compare against targets
- Use for performance-related fixes

**Example for CRIT-005 (trigram optimization)**:
```nim
# benchmarks/bench_trigram.nim
import times
import ../src/engine

proc benchmark_trigram_insert() =
  let db = openDb("bench.db")
  
  # Create table with trigram index
  db.execSql("CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)")
  db.execSql("CREATE TRIGRAM INDEX idx_content ON docs(content)")
  
  let start = cpuTime()
  
  # Insert 1000 documents
  db.beginTransaction()
  for i in 0..<1000:
    db.execSql("INSERT INTO docs VALUES (?, ?)", i, "sample text content")
  db.commit()
  
  let elapsed = cpuTime() - start
  let tps = 1000.0 / elapsed
  
  echo "Trigram insert TPS: ", tps
  assert tps > 1000, "Expected > 1000 TPS, got " & $tps
```

### Test Execution Checklist

Before submitting any change, verify:

- [ ] **New unit tests written** - Cover the fix and edge cases
- [ ] **New tests fail before fix** - Confirm test catches the bug
- [ ] **New tests pass after fix** - Confirm fix resolves the issue
- [ ] **All existing unit tests pass** - Run `nimble test` or equivalent
- [ ] **Crash-injection tests pass** - Run Python crash tests
- [ ] **Differential tests pass** - Compare with PostgreSQL
- [ ] **Property tests pass** - Verify invariants hold
- [ ] **Performance benchmarks pass** - Meet or exceed targets
- [ ] **No test patching** - Existing tests unchanged

### Test Failure Policy

**If any existing test fails:**

1. **STOP** - Do not modify the test
2. **ANALYZE** - Understand why the test failed
3. **FIX** - Correct the implementation, not the test
4. **RE-RUN** - Verify all tests pass

**If a test cannot pass with the correct implementation:**

This indicates a fundamental issue. Escalate to maintainers for review.

### Continuous Integration Requirements

All changes must pass in CI:

1. **Linux build** - GCC/Clang
2. **macOS build** - Apple Clang
3. **Windows build** - MSVC/MinGW
4. **Test suite** - All tests pass
5. **Crash tests** - Python fault injection
6. **Differential tests** - PostgreSQL comparison
7. **Performance tests** - Benchmarks meet targets
8. **Lint** - Code style checks

### Documentation Requirements

For each fix, document:

1. **What was fixed** - Brief description
2. **Why it was fixed** - The bug/issue
3. **How it was tested** - Test approach
4. **Test results** - Before/after metrics
5. **Breaking changes** - Any API or behavior changes

---

## Appendix A: Testing Requirements Summary

### Critical Tests (Must Pass Before Production)

| Test | Type | Purpose |
|------|------|---------|
| fsync_power_loss | Fault Injection | Verify data survives power failure |
| checkpoint_race | Concurrent + Crash | No data loss during checkpoint |
| reader_timeout | Concurrent | Aborted readers get errors |
| rollback_atomic | Unit | No dirty pages after rollback |
| trigram_throughput | Benchmark | 1000+ TPS with trigram indexes |

### Performance Benchmarks

| Benchmark | Target | Current (Estimated) |
|-----------|--------|---------------------|
| Bulk insert (no indexes) | 10,000 TPS | Unknown |
| Bulk insert (trigram) | 1,000 TPS | ~100 TPS |
| Point lookup P95 | < 10ms | Unknown |
| Sequential scan | 100 MB/s | Unknown |
| Checkpoint latency | < 1s | Unknown |

### Crash Injection Scenarios

1. Power failure during commit
2. Power failure during checkpoint
3. Kernel panic mid-transaction
4. Disk full during commit
5. Slow I/O during checkpoint
6. Concurrent writes during checkpoint

---

## Appendix B: File Locations Quick Reference

| Issue | File | Lines | Function |
|-------|------|-------|----------|
| CRIT-001 | `src/vfs/os_vfs.nim` | 127-133 | fsync |
| CRIT-002 | `src/wal/wal.nim` | 210-358 | checkpoint |
| CRIT-003 | `src/wal/wal.nim` | 436-471 | isAborted, getPageAtOrBefore |
| CRIT-004 | `src/engine.nim` | 1669-1676 | rollbackTransaction |
| CRIT-005 | `src/storage/storage.nim` | - | flushTrigramDeltas |
| CRIT-006 | `src/wal/wal.nim` | Throughout | Lock acquisition |
| HIGH-001 | `src/wal/wal.nim` | 43-44 | index, dirtySinceCheckpoint |
| HIGH-002 | `src/pager/pager.nim` | 521-579 | allocatePage, freePage |
| HIGH-003 | `src/engine.nim` | 1663-1694 | rollbackTransaction |
| HIGH-004 | `src/wal/wal.nim` | 376-411 | recover |
| HIGH-005 | `src/engine.nim` | 324-358 | enforceForeignKeys |
| MED-001 | `src/btree/btree.nim` | - | findChildPage |
| MED-002 | `src/pager/pager.nim` | 112-114 | shardFor |
| MED-003 | `src/engine.nim` | 1623-1628 | commitTransaction |
| MED-004 | `src/pager/pager.nim` | 116-151 | Clock eviction |
| MED-006 | `src/wal/wal.nim` | 448-471 | getPageAtOrBefore |

---

## Appendix C: Dependencies and Prerequisites

### Before Starting

- [ ] Read PRD.md, SPEC.md, TESTING_STRATEGY.md
- [ ] Understand single-writer concurrency model
- [ ] Review existing fault injection tests
- [ ] Set up performance benchmarking environment

### During Implementation

- [ ] Add unit tests for each fix
- [ ] Add crash tests for durability fixes
- [ ] Run differential tests vs PostgreSQL
- [ ] Benchmark before/after performance
- [ ] Update SPEC.md for any behavior changes
- [ ] Create ADR for file format changes

### Before Merging

- [ ] All critical tests passing
- [ ] **NO EXISTING TESTS MODIFIED** (test patching is prohibited)
- [ ] New unit tests added for the fix
- [ ] Crash-injection tests added for durability fixes
- [ ] Property tests added for complex state changes
- [ ] No regressions in differential tests
- [ ] Performance benchmarks meet targets
- [ ] Documentation updated
- [ ] Code review completed
- [ ] Test-first approach verified (test fails before fix, passes after)

---

## Appendix D: Quick Reference for Implementation Agents

### When Starting a Fix

1. **Read this action plan** - Understand the issue and approach
2. **Read existing code** - Understand current implementation
3. **Write failing test** - Create test that reproduces the bug
4. **Verify test fails** - Run test to confirm it catches the issue
5. **Implement fix** - Make minimal, focused changes
6. **Verify test passes** - Run test to confirm fix works
7. **Run full test suite** - Ensure no regressions
8. **Document** - Update comments, SPEC.md if needed

### Red Flags (STOP and Reconsider)

- You need to modify an existing test to make it pass
- The fix touches more than 5 files
- You're adding dependencies
- You're changing file formats without ADR
- Tests are flaky or non-deterministic

### Testing Commands

```bash
# Run all Nim unit tests
nimble test

# Run specific test file
nim c -r tests/test_wal.nim

# Run crash-injection tests
python tests/crash/run_all.py

# Run differential tests
python tests/differential/run_all.py

# Run performance benchmarks
nim c -r benchmarks/bench_trigram.nim

# Full test suite (what CI runs)
./scripts/run_full_test_suite.sh
```

### Test Template

```nim
# tests/test_<feature>_<scenario>.nim
import unittest
import ../src/<module>

suite "<Issue ID>: <Brief Description>":
  setup:
    # Setup code
    
  teardown:
    # Cleanup code
    
  test "should <expected behavior>":
    # Given: Initial state
    # When: Action
    # Then: Expected result
    check actual == expected
    
  test "should handle <edge case>":
    # Edge case test
    
  test "should return error when <error condition>":
    # Error handling test
```

---

**End of Action Plan**
