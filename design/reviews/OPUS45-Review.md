# Code Review: Performance & ACID Compliance Analysis

**Date:** 2025-02-01  
**Reviewer:** OPUS45  
**Scope:** Full codebase review with focus on durability, crash recovery, and performance  
**Files Analyzed:**
- `src/engine.nim` (transaction control, SQL execution)
- `src/wal/wal.nim` (write-ahead logging, checkpointing, recovery)
- `src/pager/pager.nim` (page cache, I/O operations)
- `src/vfs/os_vfs.nim` (filesystem abstraction layer)
- `src/btree/btree.nim` (B+Tree storage engine)
- `src/storage/storage.nim` (row storage, index operations)

---

## Executive Summary

DecentDB demonstrates a well-architected design with WAL-based durability, snapshot isolation, and B+Tree storage. The codebase shows careful attention to error handling via Result monads and proper separation of concerns through the VFS abstraction layer.

However, this review has identified several issues that **compromise ACID guarantees**, particularly around durability, and multiple performance bottlenecks that will impact production workloads. The most critical finding is that the current `fsync` implementation does not actually call the POSIX `fsync()` system call, meaning committed transactions are not guaranteed to survive a power failure.

### Risk Assessment Matrix

| Finding | Severity | ACID Property | Likelihood | Impact |
|---------|----------|---------------|------------|--------|
| F-001: fsync not calling fsync | **CRITICAL** | Durability | Certain | Data Loss |
| F-002: Checkpoint ordering | **CRITICAL** | Durability | Moderate | Corruption |
| F-003: WAL recovery incompleteness | **HIGH** | Consistency | Low | Stale reads |
| F-004: Freelist atomicity | **MODERATE** | Atomicity | Low | Space leak |
| F-005: Orphaned page allocation | **MODERATE** | Atomicity | Low | Space leak |
| F-006: WAL index O(N) lookup | **MODERATE** | N/A (Perf) | Certain | Slow reads |
| F-007: Clock eviction O(N) | **LOW** | N/A (Perf) | Moderate | Cache thrashing |
| F-008: Lock contention | **LOW** | N/A (Perf) | Moderate | Write stalls |

---

## Critical Findings (ACID Violations)

### F-001: fsync Implementation Does Not Provide True Durability

**Severity:** CRITICAL  
**ACID Property Violated:** Durability  
**File:** `src/vfs/os_vfs.nim` lines 127-133

#### Description

The `fsync` method in the OS VFS implementation uses Nim's `flushFile()` procedure, which only flushes userspace buffers to the operating system's page cache. It does **NOT** invoke the POSIX `fsync()` system call, which is required to ensure data has been written to stable storage (disk platters, flash cells, etc.).

#### Current Implementation

```nim
method fsync*(vfs: OsVfs, file: VfsFile): Result[Void] =
  withFileLock(file):
    try:
      flushFile(file.file)  # WARNING: This is NOT fsync!
    except OSError:
      return err[Void](ERR_IO, "Fsync failed", file.path)
  okVoid()
```

#### Technical Analysis

The Nim `flushFile()` procedure maps to C's `fflush()`, which has the following semantics:

1. **fflush()** - Flushes userspace stdio buffers to the kernel
2. **fsync()** - Flushes kernel buffers to stable storage and waits for completion
3. **fdatasync()** - Like fsync() but skips metadata (faster on some systems)

The critical difference is the **durability boundary**:

```
Application Memory → [fflush] → Kernel Page Cache → [fsync] → Disk
                     ^^^^^^^^                       ^^^^^^^
                     Current                        Required
```

Without a true fsync, the following failure modes result in data loss:

| Failure Scenario | Data Status | Expected Behavior | Actual Behavior |
|------------------|-------------|-------------------|-----------------|
| Application crash | In kernel cache | Committed tx survives | Committed tx survives |
| Kernel panic | In kernel cache | Committed tx survives | **DATA LOST** |
| Power failure | In kernel cache | Committed tx survives | **DATA LOST** |
| Disk controller failure | In disk cache | Committed tx survives | **DATA LOST** |

#### Impact

This is the most severe finding in this review. Any database that claims ACID compliance **must** ensure committed transactions survive power loss. Currently:

- All "committed" transactions since the last OS-level flush (which happens on an unpredictable schedule, typically 30 seconds on Linux) can be lost
- The database provides durability only against application crashes, not system crashes
- Users who rely on commit acknowledgment for critical operations (financial transactions, etc.) will experience silent data loss

#### Evidence

Testing this is straightforward:

```bash
# 1. Insert 1000 rows with explicit commits
# 2. Force kernel panic: echo c > /proc/sysrq-trigger
# 3. Reboot and count rows - will likely be < 1000
```

#### Recommended Remediation

**Option A: Direct POSIX fsync (Recommended)**

Replace the current implementation with direct system calls:

```nim
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

**Option B: Add Durability Modes (Defense in Depth)**

Some applications prefer to trade durability for performance. Add explicit configuration:

```nim
type DurabilityLevel* = enum
  dlFull        # fsync on every commit (safest)
  dlNormal      # fdatasync on commit (faster, still durable)
  dlRelaxed     # fsync every N commits or M milliseconds
  dlNone        # No fsync (testing only)
```

**Required Imports:**

```nim
when defined(posix):
  proc fsync(fd: cint): cint {.importc, header: "<unistd.h>".}
  proc fdatasync(fd: cint): cint {.importc, header: "<unistd.h>".}
when defined(windows):
  proc FlushFileBuffers(hFile: Handle): WINBOOL {.importc, stdcall, dynlib: "kernel32".}
```

**Testing Requirements:**

After implementing the fix:

1. Write a test that uses `sync_file_range()` or similar to verify data reaches disk
2. Add integration tests with simulated power failure (e.g., using dm-flakey on Linux)
3. Document the durability guarantees in user-facing documentation

---

### F-002: Checkpoint Operation Ordering Violates Write-Ahead Logging Protocol

**Severity:** CRITICAL  
**ACID Property Violated:** Durability, Consistency  
**File:** `src/wal/wal.nim` lines 210-358

#### Description

The checkpoint operation writes pages to the main database file, then fsyncs the database, then writes the checkpoint frame to the WAL, then fsyncs the WAL. This ordering creates a window where a crash can leave the database in an inconsistent state that cannot be properly recovered.

#### Current Implementation Flow

```
checkpoint():
  Phase 1: Determine pages to checkpoint (under lock)
  Phase 2: Write pages to DB file (lock released!)
           for entry in toCheckpoint:
             writePageDirectFile(pager, entry[0], payloadStr)  ← DB modified
           fsync(pager.file)                                   ← DB durable
  Phase 3: Write checkpoint frame to WAL (re-acquire lock)
           appendFrame(wal, wfCheckpoint, ...)                 ← WAL modified
           fsync(wal.file)                                     ← WAL durable
           update header.lastCheckpointLsn                     ← Header modified
           fsync(pager.file)                                   ← Header durable
```

#### Technical Analysis

The Write-Ahead Logging protocol requires that log records be durable **before** the corresponding data pages are written to the main database. This is the fundamental invariant that enables crash recovery:

> **WAL Invariant:** Before any modified page is written to the database file, all log records describing modifications to that page must be on stable storage.

The current checkpoint implementation violates this in a subtle way. Consider this crash scenario:

**Timeline of Crash Scenario:**

```
T1: Checkpoint begins
T2: Page P1 written to DB file (contains LSN=100)
T3: Page P2 written to DB file (contains LSN=100)
T4: fsync(DB file) completes - P1, P2 on disk
T5: --- CRASH OCCURS HERE ---
T6: appendFrame(checkpoint) never executes
T7: WAL still contains frames for LSN 90-100, no checkpoint marker
```

**State After Recovery:**

```
DB File: Contains pages at LSN=100 (post-checkpoint state)
WAL: Contains frames for LSN 90-100, last checkpoint at LSN=80
Recovery: Will try to replay LSN 81-100 onto already-modified pages
```

The recovery process will:
1. Read `lastCheckpointLsn=80` from the header
2. Replay all WAL frames from LSN 81 onwards
3. Apply these changes to pages that are already at LSN=100
4. Result: Potential corruption if the replay semantics aren't perfectly idempotent

#### Impact

- **Corruption Risk:** Pages may contain a mix of pre-checkpoint and post-checkpoint data
- **Recovery Failure:** The WAL index won't match the actual DB state
- **Silent Data Loss:** Users may not realize their data is corrupted until they query it

#### Evidence

The code at lines 270-317 shows the problematic ordering:

```nim
# Phase 2: Perform I/O operations without holding the main lock
for entry in toCheckpoint:
  ...
  let writeRes = writePageDirectFile(pager, entry[0], payloadStr)  # Line 293
  ...
pager.header.lastCheckpointLsn = safeLsn  # Line 305
let headerRes = writeHeader(pager.vfs, pager.file, pager.header)  # Line 306
let syncRes = pager.vfs.fsync(pager.file)  # Line 312 - DB durable

# Phase 3: Re-acquire lock to finalize checkpoint state
acquire(wal.lock)
let chkRes = appendFrame(wal, wfCheckpoint, 0, encodeCheckpointPayload(safeLsn))  # Line 321
...
let walSync = wal.vfs.fsync(wal.file)  # Line 333 - WAL durable AFTER DB!
```

#### Recommended Remediation

Reorder the checkpoint operations to maintain the WAL invariant:

```nim
proc checkpoint*(wal: Wal, pager: Pager): Result[uint64] =
  # Phase 1: Determine what to checkpoint (unchanged)
  ...
  
  # Phase 2: Write checkpoint-intent frame to WAL FIRST
  acquire(wal.lock)
  let intentLsn = wal.nextLsn
  let intentRes = appendFrame(wal, wfCheckpointIntent, 0, encodeCheckpointPayload(safeLsn))
  if not intentRes.ok:
    release(wal.lock)
    return err[uint64](...)
  
  # Fsync WAL to ensure intent is durable
  let walSyncRes = wal.vfs.fsync(wal.file)
  if not walSyncRes.ok:
    release(wal.lock)
    return err[uint64](...)
  release(wal.lock)
  
  # Phase 3: Now safe to write pages to DB file
  for entry in toCheckpoint:
    let writeRes = writePageDirectFile(pager, entry[0], payloadStr)
    if not writeRes.ok:
      # Checkpoint failed - recovery will ignore the intent frame
      return err[uint64](...)
  
  # Fsync DB file
  let dbSyncRes = pager.vfs.fsync(pager.file)
  if not dbSyncRes.ok:
    return err[uint64](...)
  
  # Phase 4: Write checkpoint-complete frame to WAL
  acquire(wal.lock)
  let completeRes = appendFrame(wal, wfCheckpointComplete, 0, encodeCheckpointPayload(safeLsn))
  ...
  
  # Update header
  pager.header.lastCheckpointLsn = safeLsn
  let headerRes = writeHeader(...)
  let headerSyncRes = pager.vfs.fsync(pager.file)
  
  # Final WAL fsync
  let finalWalSync = wal.vfs.fsync(wal.file)
  
  # Phase 5: Truncate WAL (only if fully complete)
  ...
```

**Alternative: Two-Phase Checkpoint Protocol**

A more robust approach used by production databases:

1. **Fuzzy Checkpoint:** Write a checkpoint-start marker, then asynchronously flush dirty pages
2. **Checkpoint End:** When all pages are flushed, write checkpoint-end marker
3. **Recovery:** If checkpoint-start exists without checkpoint-end, treat checkpoint as incomplete

This allows concurrent writes during checkpoint while maintaining safety.

---

## High Severity Findings

### F-003: WAL Recovery Does Not Fully Restore Database State

**Severity:** HIGH  
**ACID Property Affected:** Consistency  
**File:** `src/wal/wal.nim` lines 376-411, `src/engine.nim` lines 139-183

#### Description

The WAL recovery process (`recover()`) rebuilds the in-memory WAL index but does not ensure that the pager and catalog see a consistent view of the database. While the page overlay mechanism handles most reads correctly, there are edge cases where stale data can be served.

#### Current Implementation

```nim
proc recover*(wal: Wal): Result[Void] =
  wal.index.clear()
  wal.dirtySinceCheckpoint.clear()
  var pending: seq[(PageId, uint64, int64)] = @[]
  var lastCommit: uint64 = 0
  var offset: int64 = 0
  
  while true:
    let frameRes = readFrame(wal.vfs, wal.file, frameOffset)
    if not frameRes.ok:
      break  # End of WAL or corruption
    ...
    case frameType
    of wfPage:
      pending.add((PageId(pageId), lsn, frameOffset))
    of wfCommit:
      # Add all pending pages to index
      for entry in pending:
        wal.index[entry[0]].add(WalIndexEntry(lsn: entry[1], offset: entry[2]))
        wal.dirtySinceCheckpoint[entry[0]] = ...
      pending = @[]
    of wfCheckpoint:
      discard  # Checkpoint frames are not processed!
    ...
  
  wal.endOffset = offset
  wal.walEnd.store(lastCommit, moRelease)
  wal.nextLsn = max(wal.nextLsn, lastCommit + 1)
  okVoid()
```

#### Technical Analysis

**Issue 1: Checkpoint Frames Are Ignored**

The recovery process reads checkpoint frames but does nothing with them (`discard`). This means:

- The `lastCheckpointLsn` in the database header may not match the WAL state
- If the WAL was truncated after a checkpoint, the index won't know about pre-checkpoint pages

**Issue 2: Overlay Depends on WAL Not Being Truncated**

The page overlay in `engine.nim` lines 147-169 assumes WAL entries exist for all committed changes:

```nim
pager.setPageOverlay(0, proc(pageId: PageId): Option[string] =
  ...
  let pageOpt = wal.getPageAtOrBefore(pageId, snap)
  if pageOpt.isNone:
    return none(string)  # Falls through to disk read
  ...
)
```

If the WAL has been truncated after a checkpoint, `getPageAtOrBefore` returns `none`, and the pager reads from the main database file. This is correct **only if** the checkpoint actually wrote all pages to disk.

**Issue 3: `overriddenPages` Set Not Persisted**

The `overriddenPages` set tracks pages that have been written directly to the database file (bypassing WAL). This set is in-memory only and lost on restart:

```nim
if isOverridden:
  return none(string)  # Read from disk
```

After recovery, this set is empty, so pages that were checkpointed but not in the WAL will still be read from disk (which is correct), but pages that were written via `dmNone` durability mode will not be tracked.

#### Impact

- **Stale Reads:** Under specific conditions (checkpoint + WAL truncation + crash before next commit), queries may return data from a previous state
- **Phantom Writes:** Changes made with `dmNone` durability may or may not be visible after restart
- **Index Inconsistency:** If an index page was checkpointed but the corresponding table page wasn't, lookups may fail

#### Recommended Remediation

**Step 1: Process Checkpoint Frames During Recovery**

```nim
proc recover*(wal: Wal): Result[Void] =
  ...
  var lastCheckpointLsn: uint64 = 0
  
  while true:
    ...
    case frameType
    of wfCheckpoint:
      let payloadLsn = readU64LE(payload, 0)
      lastCheckpointLsn = max(lastCheckpointLsn, payloadLsn)
      # Clear index entries older than checkpoint LSN
      # (they're now in the main DB file)
      for pageId, entries in wal.index.mpairs:
        entries = entries.filterIt(it.lsn > lastCheckpointLsn)
    ...
```

**Step 2: Verify DB Header Consistency**

After recovery, verify that the database header's `lastCheckpointLsn` matches the WAL's view:

```nim
# In openDb(), after recover():
if pager.header.lastCheckpointLsn != wal.lastRecoveredCheckpointLsn:
  # Inconsistency detected - decide on recovery strategy
  if pager.header.lastCheckpointLsn > wal.lastRecoveredCheckpointLsn:
    # Header claims newer checkpoint than WAL knows about
    # WAL may have been truncated - this is dangerous
    return err[Db](ERR_CORRUPTION, "WAL/Header checkpoint mismatch")
```

**Step 3: Add Recovery Verification**

Add a post-recovery verification step that reads a sample of pages and verifies checksums:

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

---

## Moderate Severity Findings

### F-004: Freelist Operations Are Not Atomic

**Severity:** MODERATE  
**ACID Property Affected:** Atomicity (internal consistency)  
**File:** `src/pager/pager.nim` lines 521-579

#### Description

The freelist management operations (`allocatePage`, `freePage`) modify multiple data structures (freelist pages, header) without atomicity guarantees. A crash between these operations can leave the freelist in an inconsistent state.

#### Current Implementation

```nim
proc allocatePage*(pager: Pager): Result[PageId] =
  if pager.header.freelistCount == 0 or pager.header.freelistHead == 0:
    return appendBlankPage(pager)
  
  let headId = PageId(pager.header.freelistHead)
  var next, count: uint32
  var ids: seq[uint32]
  let readRes = readFreelistPage(pager, headId, next, count, ids)
  ...
  
  let id = ids[^1]
  ids.setLen(ids.len - 1)
  
  # Step 1: Update header count (in memory)
  pager.header.freelistCount = pager.header.freelistCount - 1
  
  # Step 2: Write updated freelist page
  let writeRes = writeFreelistPage(pager, headId, next, ids)
  # CRASH WINDOW: Header count decremented but freelist page not updated
  
  if ids.len == 0:
    pager.header.freelistHead = next
  
  # Step 3: Write header
  let headerRes = updateHeader(pager)
  # CRASH WINDOW: Freelist page updated but header not updated
  
  ok(PageId(id))
```

#### Technical Analysis

**Crash Scenario 1: Between Steps 1 and 2**

```
State Before: freelistCount=5, freelist=[10,11,12,13,14]
Step 1: freelistCount=4 (in memory only)
--- CRASH ---
After Recovery: freelistCount=5, freelist=[10,11,12,13,14]
Result: Consistent (no problem)
```

**Crash Scenario 2: Between Steps 2 and 3**

```
State Before: freelistCount=5, freelist=[10,11,12,13,14]
Step 2: freelist=[10,11,12,13] written to disk
--- CRASH ---
After Recovery: freelistCount=5, freelist=[10,11,12,13]
Result: INCONSISTENT - count says 5, actual is 4
        Page 14 is now unreachable (space leak)
```

**Crash Scenario 3: During `freePage` with new list page**

```nim
proc freePage*(pager: Pager, pageId: PageId): Result[Void] =
  ...
  # Need to allocate a new freelist page
  let newListPage = appendBlankPage(pager)  # New page allocated
  pager.header.freelistHead = uint32(newHead)  # In memory
  pager.header.freelistCount = pager.header.freelistCount + 1  # In memory
  let writeRes = writeFreelistPage(pager, newHead, oldHead, @[uint32(pageId)])
  # CRASH: New freelist page written, but header still points to old head
  return updateHeader(pager)
```

#### Impact

- **Space Leaks:** Pages can become unreachable if the freelist count/head doesn't match reality
- **Double Allocation:** In extreme cases, a page could be allocated while still on the freelist
- **Gradual Database Growth:** Leaked pages accumulate over time, increasing file size

#### Recommended Remediation

**Option A: Make Freelist Operations WAL-Aware**

The freelist modifications should be part of the transaction's WAL record:

```nim
proc allocatePage*(pager: Pager): Result[PageId] =
  # Freelist changes will be captured in dirty pages
  # and committed atomically with the rest of the transaction
  ...
  
  # Don't write header directly - let it be part of the WAL commit
  pager.header.freelistCount -= 1
  # The header page (page 1) will be marked dirty and written to WAL
```

**Option B: Freelist Transaction Log**

Maintain a separate mini-log for freelist operations:

```nim
type FreelistOp = object
  kind: enum { foAlloc, foFree }
  pageId: PageId
  timestamp: uint64

proc allocatePage*(pager: Pager): Result[PageId] =
  # Write intent to freelist log first
  let op = FreelistOp(kind: foAlloc, pageId: targetPage, timestamp: now())
  writeFreelistLog(op)
  fsync(freelistLog)
  
  # Now safe to modify freelist
  ...
  
  # After commit, truncate freelist log
```

**Option C: Self-Describing Freelist (Recommended)**

Make the freelist self-consistent without relying on the header count:

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

---

### F-005: Page Allocation During Failed Transactions Creates Orphans

**Severity:** MODERATE  
**ACID Property Affected:** Atomicity (resource management)  
**File:** `src/engine.nim`, `src/storage/storage.nim`

#### Description

When a transaction allocates new pages (for B+Tree splits, overflow chains, new tables, etc.) but then fails or is rolled back, those pages are not returned to the freelist. This creates "orphaned" pages that consume space but are unreachable.

#### Technical Analysis

Consider this sequence:

```nim
proc createTable(...):
  # Allocates a new page for the table root
  let rootRes = initTableRoot(db.pager)  # Allocates page 100
  ...
  # Later in the transaction, something fails
  rollbackTransaction(db)  # Page 100 is NOT freed
```

The rollback process in `engine.nim` lines 1663-1694:

```nim
proc rollbackTransaction*(db: Db): Result[Void] =
  ...
  let rollbackRes = rollback(db.activeWriter)  # Discards WAL writes
  ...
  rollbackCache(db.pager)  # Evicts dirty pages from cache
  
  # Reload header and catalog
  let page1Res = readPage(db.pager, PageId(1))
  ...
  let reloadRes = initCatalog(db.pager)
  ...
```

The problem: `rollbackCache` evicts dirty pages, which correctly discards uncommitted changes to existing pages. But pages that were **newly allocated** during the transaction:

1. Were allocated from the freelist (reducing `freelistCount`)
2. The freelist modification may or may not have been flushed
3. The new pages are orphaned - not in the freelist, not referenced by any structure

#### Impact

- **Space Leak:** Each failed transaction that allocated pages leaks those pages
- **File Growth:** Over time, the database file grows with unreachable pages
- **VACUUM Required:** Periodic offline maintenance needed to reclaim space

#### Recommended Remediation

**Option A: Transaction-Local Allocation Tracking**

Track allocations during a transaction and free them on rollback:

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
  ...
  # Return allocated pages to freelist
  for pageId in db.currentTx.allocatedPages:
    discard freePage(db.pager, pageId)
  
  # Re-allocate freed pages (they shouldn't have been freed)
  # This is trickier - may need to mark them as "pending free"
  ...
```

**Option B: Deferred Allocation**

Don't actually allocate pages until commit time:

```nim
type PendingAllocation = object
  placeholder: int  # Virtual page ID used during transaction
  purpose: string   # For debugging

proc allocatePageDeferred*(tx: var Transaction): int =
  result = tx.nextPlaceholder
  tx.nextPlaceholder.inc
  tx.pendingAllocations.add(PendingAllocation(placeholder: result))

proc commitTransaction*(db: Db): Result[Void] =
  # Now actually allocate pages
  var placeholderToReal = initTable[int, PageId]()
  for pending in db.tx.pendingAllocations:
    let realPage = allocatePage(db.pager)
    placeholderToReal[pending.placeholder] = realPage.value
  
  # Rewrite pages with real page IDs
  ...
```

**Option C: Garbage Collection (Pragmatic)**

Accept that orphans may occur and provide a GC mechanism:

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

---

### F-006: WAL Index Lookup is O(N) Per Page

**Severity:** MODERATE  
**Category:** Performance  
**File:** `src/wal/wal.nim` lines 448-471

#### Description

The `getPageAtOrBefore` function performs a linear scan through all WAL entries for a given page to find the version at or before a snapshot LSN. For pages that are frequently updated, this becomes a significant performance bottleneck.

#### Current Implementation

```nim
proc getPageAtOrBefore*(wal: Wal, pageId: PageId, snapshot: uint64): Option[seq[byte]] =
  acquire(wal.indexLock)
  defer: release(wal.indexLock)
  
  if not wal.index.hasKey(pageId):
    return none(seq[byte])
  
  let entries = wal.index[pageId]  # All versions of this page
  var bestLsn: uint64 = 0
  var bestOffset: int64 = -1
  
  for entry in entries:  # LINEAR SCAN
    if entry.lsn <= snapshot and entry.lsn >= bestLsn:
      bestLsn = entry.lsn
      bestOffset = entry.offset
  
  if bestOffset < 0:
    return none(seq[byte])
  
  # Read from WAL file
  let frameRes = readFrame(wal.vfs, wal.file, bestOffset)
  ...
```

#### Technical Analysis

**Time Complexity:**
- Single page read: O(V) where V = number of versions of that page in WAL
- Full table scan of N pages: O(N * V)

**Worst Case Scenario:**

Consider a "hot" page (e.g., a B+Tree root or a frequently-updated row):

```
Transaction 1: Update page 100 → WAL has 1 entry for page 100
Transaction 2: Update page 100 → WAL has 2 entries for page 100
...
Transaction 10000: Update page 100 → WAL has 10000 entries for page 100

Reader at snapshot 5000:
  - Must scan all 10000 entries to find best match
  - Even though only ~5000 are relevant
```

**Benchmark Impact:**

With 10,000 updates to a single page:
- Current: ~50,000 comparisons per read (scan all, even those > snapshot)
- Optimal: ~13 comparisons (binary search in sorted list)

#### Recommended Remediation

**Option A: Keep Entries Sorted by LSN (Recommended)**

Maintain entries in sorted order and use binary search:

```nim
type WalIndexEntry = object
  lsn: uint64
  offset: int64

# Entries are kept sorted by lsn (ascending)
proc addEntry(index: var Table[PageId, seq[WalIndexEntry]], pageId: PageId, entry: WalIndexEntry) =
  if not index.hasKey(pageId):
    index[pageId] = @[entry]
  else:
    # Insert in sorted position (entries are added in LSN order, so append is usually correct)
    index[pageId].add(entry)

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
    return none(seq[byte])  # All entries are > snapshot
  
  let bestEntry = entries[lo - 1]
  let frameRes = readFrame(wal.vfs, wal.file, bestEntry.offset)
  ...
```

**Option B: Version Chain with Skip Pointers**

For very long version chains, add skip pointers:

```nim
type WalIndexEntry = object
  lsn: uint64
  offset: int64
  skipBack: int  # Index of entry ~100 versions earlier

# Allows O(V/100 + log(100)) = O(V/100) lookup
```

**Option C: Hierarchical Index**

For extreme cases, use a B+Tree for the version index itself:

```nim
# Each page has its own mini B+Tree of versions
type PageVersionIndex = object
  root: ptr VersionNode

type VersionNode = object
  case isLeaf: bool
  of true:
    entries: array[16, WalIndexEntry]
  of false:
    keys: array[15, uint64]
    children: array[16, ptr VersionNode]
```

---

## Low Severity Findings

### F-007: Clock Eviction Algorithm Has O(N) Deletion

**Severity:** LOW  
**Category:** Performance  
**File:** `src/pager/pager.nim` lines 116-151

#### Description

The clock eviction algorithm maintains a `seq[PageId]` for the clock hand. When a page is evicted, it's deleted from the middle of this sequence, which is O(N).

```nim
shard.clock.delete(currentIndex)  # O(N) - shifts all subsequent elements
if shard.clockHand > currentIndex:
  shard.clockHand.dec
```

#### Impact

With a large cache (e.g., 10,000 pages per shard), each eviction requires shifting up to 10,000 elements. During periods of high cache churn, this adds measurable latency.

#### Recommended Remediation

Use a doubly-linked list or mark-and-compact approach:

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
```

---

### F-008: Lock Contention During Commit

**Severity:** LOW  
**Category:** Performance  
**File:** `src/pager/pager.nim` lines 370-391

#### Description

The `flushAll` and `snapshotDirtyPages` functions iterate through all cache shards while holding each shard lock. During commit, this can block concurrent readers.

```nim
proc snapshotDirtyPages*(pager: Pager): seq[(PageId, string)] =
  for shard in cache.shards:
    acquire(shard.lock)  # Blocks readers of this shard
    for _, entry in shard.pages:
      entries.add(entry)
    release(shard.lock)
  
  # Then acquire entry locks...
  for entry in entries:
    acquire(entry.lock)
    ...
```

#### Impact

During commit of a large transaction, readers may experience brief stalls as they wait for shard locks.

#### Recommended Remediation

Use read-write locks or optimistic concurrency:

```nim
# Option A: Read-write locks
type PageCacheShard = ref object
  rwlock: RWLock  # Readers don't block each other

# Option B: Copy-on-write dirty tracking
type PageCacheShard = ref object
  dirtySet: Atomic[ptr HashSet[PageId]]  # Swapped atomically
```

---

## Recommendations Summary

### Immediate Actions (Before Production Use)

1. **Fix fsync implementation** (F-001) - This is a data loss bug
2. **Fix checkpoint ordering** (F-002) - This can cause corruption
3. **Add recovery verification** (F-003) - Detect inconsistencies early

### Short-Term Improvements (Next Release)

4. **Make freelist operations atomic** (F-004)
5. **Track transaction-local allocations** (F-005)
6. **Optimize WAL index lookup** (F-006)

### Long-Term Enhancements

7. **Improve clock eviction** (F-007)
8. **Reduce lock contention** (F-008)
9. **Add comprehensive crash testing** with fault injection

---

## Testing Recommendations

### Durability Testing

```bash
# Test 1: Power failure simulation
# Use dm-flakey on Linux to simulate sudden power loss
dmsetup create flakey --table "0 $(blockdev --getsz /dev/loop0) flakey /dev/loop0 0 0 0"

# Test 2: Kernel panic injection
echo c > /proc/sysrq-trigger  # Immediate crash

# Test 3: Process kill during fsync
# Use strace to identify fsync calls, then kill -9 mid-syscall
```

### Consistency Testing

```nim
# Automated consistency check after every test
proc checkDatabaseConsistency*(db: Db): Result[Void] =
  # 1. Freelist count matches actual
  # 2. All B+Tree pages are reachable
  # 3. No page is both in freelist and referenced
  # 4. All index entries point to valid rows
  # 5. WAL index matches WAL file contents
```

---

## Appendix: Code References

### Critical Paths

| Operation | Files | Lines |
|-----------|-------|-------|
| Commit | `engine.nim` | 1616-1661 |
| WAL Write | `wal.nim` | 499-538 |
| Checkpoint | `wal.nim` | 210-358 |
| Recovery | `wal.nim` | 376-411 |
| Page Read | `pager.nim` | 153-180, 284-295 |
| fsync | `os_vfs.nim` | 127-133 |

### Data Structures

| Structure | Purpose | Location |
|-----------|---------|----------|
| `Wal` | WAL state and index | `wal.nim:36-59` |
| `Pager` | Page cache management | `pager.nim:34-46` |
| `WalWriter` | Transaction write buffer | `wal.nim:483-487` |
| `CacheEntry` | Single cached page | `pager.nim:14-22` |
