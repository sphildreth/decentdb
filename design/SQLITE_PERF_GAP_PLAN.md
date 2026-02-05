# DecentDB → SQLite Commit Latency Performance Gap Plan

**Current Status:**
- DecentDB p95 commit latency: ~0.0787ms (after optimizations)
- SQLite p95 commit latency: ~0.00981ms
- **Gap:** ~8.02x slower

**Goal:** Define the architectural changes needed to achieve <2x SQLite's commit latency (<0.020ms)

**Non-Goals:**
- Matching SQLite's `synchronous=OFF` mode (no durability)
- Sacrificing crash-safety guarantees for performance
- Multi-process concurrency optimizations (out of scope for 0.x)

**Note on Impact Estimates:** Throughout this document, improvement percentages are **multiplicative, not additive**. For example, a 40% improvement followed by a 20% improvement yields `0.6 × 0.8 = 0.48` (52% total improvement), not 60%.

## Cross-metric guardrails (do not regress the rest of the chart)

This plan targets **commit latency** specifically, but changes must not “win” by shifting cost into other benchmarked dimensions.

**Primary metric (this doc):**
- `commit_p95_ms` (durability = safe)

**Must-not-regress metrics (README chart suite):**
- `read_p95_ms` (point reads)
- `join_p95_ms`
- `insert_rows_per_sec` (durability = safe)
- `db_size_mb`

**Acceptance rule:**
- For any optimization proposed here, re-run the full embedded benchmark pipeline and only accept the change if commit latency improves **and** the other benchmarked metrics do not materially regress beyond run-to-run noise.
- If a tradeoff is genuinely unavoidable, it must be documented explicitly (what regresses, why) and treated as a deliberate product decision rather than an incidental side effect.

**How to enforce:**
- Use the existing benchmark pipeline (`nimble bench_embedded_pipeline`) and compare aggregated outputs (median-of-runs) rather than single-run results.
- Always report the “full metric surface” for a change (not just commit latency) when updating the plan or implementing items from it.

---

## 1. WAL Frame Format Overhead

### Current State (DecentDB)

**Frame Structure (per page written):**
```
[1 byte]   Frame type (wfPage = 0x01, wfCommit = 0x02, wfCheckpoint = 0x03)
[4 bytes]  Page ID (uint32)
[N bytes]  Page data (typically 4096 bytes)
[8 bytes]  Checksum (reserved, zero in v5+)
────────────────────────────
Total overhead per frame: 13 bytes
```

**For a single-page commit (typical UPDATE):**
- Bytes written: 13 (frame) + 13 (commit frame) = 26 bytes overhead
- Plus 4096 bytes of actual page data = 4122 bytes total
- No per-frame CRC32C in v6 (checksum field reserved)

### SQLite Approach

**Frame Structure (per page):**
```
[4 bytes]  Page number (uint32)
[4 bytes]  Commit marker / DB size (for commit frames)
[N bytes]  Page content (unaligned, direct from cache)
────────────────────────────
Total overhead per frame: ~4-8 bytes
```

**Key Differences:**
1. **No frame type field**: SQLite infers frame type from position/context
2. **No length field (DecentDB v7 matches)**: Page size is known from database header
3. **No per-frame LSN (DecentDB v6 matches)**: LSN is implicit from WAL header + frame position
4. **No per-frame CRC32C (DecentDB v5+ matches)**: SQLite uses:
   - 32-bit salt in WAL header for each transaction
   - Per-transaction checksum (not per-page)
   - OS-level write guarantees (power loss is the main concern)

### Required Changes

**Short Term (safe, minimal format change):**
1. Remove LSN from frame trailer (8 bytes saved per frame)
   - LSN is redundant - can be derived from frame position + header
2. Inline frame encoding for single-page commits (avoid frameBuffer allocation)

**Medium Term (requires format version bump):**
1. Replace CRC32C with per-transaction checksum only
2. Simplify frame header: remove length field, use database page size
3. Remove frame type field - infer from position

**Impact Estimate:**
- Remove per-frame CRC32C: ~10-15% latency reduction (CRC32C is ~5-10us per frame)
- Remove per-frame allocations: ~5-10% latency reduction
- Total potential: 15-25% improvement (0.115ms → 0.086-0.098ms)

---

## 2. Memory Copying and Buffer Allocation

### Current State (Data Flow)

**Path for a single-page UPDATE:**
```
1. Page Cache (dirty page: string)
        ↓ writePageDirect()
2. Pending Queue (seq[byte] allocation + copy)
        ↓ commit()
3. frameBuffer (encoding + copy)
        ↓ vfs.write()
4. Kernel Page Cache
        ↓ fsync()
5. Disk
```

**Allocations per commit:**
1. `newSeq[byte](data.len)` in `writePageDirect` for each dirty page
2. `frameBuffer` may resize during encoding
3. VFS writeBuffer allocation (if using buffered I/O)

### SQLite Approach

**Path for a single-page UPDATE:**
```
1. Page Cache (dirty page: direct memory)
        ↓ pwrite() syscall
2. Kernel Page Cache (direct from DB cache buffer)
        ↓ fdatasync()
3. Disk
```

**Key differences:**
1. **No intermediate buffers**: SQLite uses `pwrite()` directly from page cache
2. **Memory-mapped I/O**: When enabled, SQLite mmaps the database file
3. **Zero-copy WAL writes**: Pages are written directly from cache to WAL
4. **Thread-local caches**: SQLite avoids contention with per-thread page caches

### Required Changes

**Short Term:**
1. **Direct string write**: Add `writePageString()` that accepts `string` directly
   - Avoid string → seq[byte] conversion
   - Encode directly from string into frameBuffer
2. **Resize frameBuffer once**: Pre-size frameBuffer based on number of dirty pages
3. **Avoid pending queue for single-page**: Write directly to frameBuffer

**Medium Term (requires architecture changes):**
1. **Unified page representation**: Store pages as `seq[byte]` in cache instead of `string`
   - Eliminates the representation mismatch
   - Enables zero-copy throughout the stack
2. **Memory-mapped WAL**: Use `mmap()` for WAL file on supported platforms
   - Write frames by copying to mmap region
   - Kernel handles fsync via `MAP_SHARED`
3. **Pre-allocated WAL buffers**: Use ring buffer for small transactions
   - Avoid dynamic allocation entirely
   - Reuse same buffer for consecutive commits

**Impact Estimate:**
- Direct string write: ~10-15% improvement (eliminates one copy)
- Pre-sized frameBuffer: ~5% improvement
- Zero-copy page cache: ~15-20% improvement
- **Total potential: 30-40% improvement (0.115ms → 0.069-0.081ms)**

---

## 3. Locking and Synchronization

> **Note:** The concurrency optimizations in this section primarily benefit **multi-threaded workloads**. For the single-threaded p95 latency benchmark, only "Release lock before fsync" provides meaningful improvement.

### Current State (DecentDB)

**Locks acquired during commit:**
```
1. beginWrite():
   - acquire(wal.lock) [held for entire commit]

2. commit():
   - (no lock changes during encoding)
   
3. After fsync:
   - acquire(wal.indexLock) [briefly]
   - release(wal.indexLock)
   - atomically update wal.walEnd
   - release(wal.lock)
```

**Problems:**
1. **wal.lock held during I/O**: While `fsync()` is running, other writers are blocked
2. **Two lock acquisitions**: Both lock and indexLock are acquired per commit
3. **Atomic operations**: `moRelease` memory ordering may impose barriers

### SQLite Approach

**SQLite's WAL mode commit:**
```
1. Acquire WAL write lock (exclusive)
   - If busy, wait or return BUSY

2. Append frames to WAL (no fsync yet!)
   - Calculate checksums incrementally
   - Update in-memory WAL index

3. Release WAL write lock
   - Other writers can proceed immediately

4. Optional: fsync WAL (if PRAGMA synchronous=FULL)
   - Done AFTER releasing the lock
   - Concurrent with other writers

5. Commit marker visible to readers
   - Atomic update of header
```

**Key differences:**
1. **fsync happens after releasing lock**: Writers don't block during I/O
2. **Single lock**: Only the WAL write lock is needed
3. **Concurrent commits**: Multiple connections can be fsyncing simultaneously
4. **WAL header atomics**: Uses atomic operations for commit visibility, not locks

### Required Changes

**Short Term:**
1. **Release lock before fsync**: Reorder operations so `wal.lock` is released before `fsync()`
   - Requires careful handling of the "commit window" between lock release and fsync
   - Crash during this window must be recoverable

**Medium Term:**
1. **Separate commit phases**:
   - Phase 1 (locked): Write frames to WAL, update in-memory index
   - Phase 2 (unlocked): fsync WAL, make durable
   - Phase 3 (locked briefly): Publish commit to readers

2. **Use lock-free commit publishing**:
   - Replace indexLock + table updates with atomic pointer swaps
   - Readers see new commits via atomic.load() not lock acquisition

3. **Concurrent WAL writers**:
   - Multiple threads can write to different WAL regions concurrently
   - Use atomic compare-and-swap to claim WAL space
   - Only the commit ordering needs synchronization

**Impact Estimate:**
- Lock release before fsync: ~5-10% improvement (allows pipelining)
- Lock-free commit publishing: ~10-15% improvement in high-contention scenarios
- Concurrent writers: ~20-30% improvement for multi-threaded workloads
- **Total potential: 15-25% improvement for single-threaded (0.115ms → 0.086-0.098ms)**

---

## 4. Fsync Patterns and OS Interaction

> **TODO:** Verify current behavior. If DecentDB already avoids DB file fsync on commit (only fsyncing during checkpoint), the "Remove DB fsync" optimization is already implemented and should be removed from this plan.

### Current State (DecentDB)

**Fsync pattern:**
```nim
# commit() in wal.nim:
let syncRes = writer.wal.vfs.fsync(writer.wal.file)  # fsync after every commit
```

**VFS fsync implementation** (`os_vfs.nim`):
```nim
when defined(macosx) or defined(ios):
    if fsync(fd) != 0:  # macOS: full fsync
else:
    # Linux: prefer fdatasync
    if fdatasync(fd) != 0:
        if fsync(fd) != 0:  # fallback to full fsync
```

**Problems:**
1. `fdatasync` still syncs metadata (inode timestamps, etc.) not just data
2. Single fsync per commit - no batching opportunity
3. No use of `sync_file_range()` on Linux for finer control

### SQLite Approach

**SQLite fsync patterns:**

1. **PRAGMA synchronous=FULL** (default durability):
   ```c
   // SQLite approach
   write(wal_fd, frames, size);  // Write frames
   fsync(wal_fd);                 // Sync WAL only
   // Note: No fsync of main DB! WAL recovery handles that
   ```

2. **PRAGMA synchronous=NORMAL** (faster, still safe):
   ```c
   write(wal_fd, frames, size);
   // No fsync! Assume OS flushes within 1 second
   // Checkpoint handles durability
   ```

3. **Linux-specific optimizations**:
   ```c
   // Uses sync_file_range() if available
   sync_file_range(fd, offset, size, SYNC_FILE_RANGE_WRITE);
   // Then fdatasync() only the range that matters
   ```

**Key differences:**
1. **No DB fsync on commit**: SQLite only fsyncs WAL, never the DB file on each commit
   - DB file is updated during checkpoint, which happens less frequently
   - WAL contains all recovery information
   - Much less I/O per commit

2. **Range-based syncing**: On Linux, uses `sync_file_range()` to only sync new writes

3. **Metadata avoidance**: SQLite carefully avoids touching inode metadata (no file size changes on commit)

4. **Sequential patterns**: WAL is append-only, enabling better disk scheduling

### Required Changes

**Medium Term (requires protocol changes):**

1. **Separate commit durability from DB file writes**:
   ```
   Current:  Commit writes WAL → fsync WAL → maybe checkpoint → fsync DB
   Proposed: Commit writes WAL → fsync WAL only
              Checkpoint periodically fsyncs DB separately
   ```
   - This is the **biggest win** (could reduce I/O by 50%)
   - Requires WAL format that can fully reconstruct database
   - Requires WAL replay on recovery before DB is usable

2. **Use sync_file_range on Linux**:
   ```nim
   when defined(linux):
     # After write, tell kernel to start flushing to disk
     discard sync_file_range(fd, startOffset, writeLen, SYNC_FILE_RANGE_WRITE)
     # Then only fdatasync metadata
     if fdatasync(fd) != 0:
       return err[Void](ERR_IO, "fdatasync failed")
   ```
   - Overlaps I/O with computation
   - Reduces latency of fsync

3. **Lazy DB file updates**:
   - Don't write dirty pages to DB file on every commit
   - Accumulate dirty pages in memory
   - Write to DB only during checkpoint
   - This is what SQLite does in WAL mode

**Impact Estimate:**
- Eliminate DB fsync on commit: **40-50% improvement** (biggest win)
  - Current: ~2 fsyncs per commit (WAL + DB during checkpoint)
  - Target: ~1 fsync per commit (WAL only)
- sync_file_range: **10-15% improvement**
- Lazy DB writes: **20-30% improvement** (allows write coalescing)
- **Total potential: 70-95% improvement (0.115ms → 0.006-0.035ms, competitive with SQLite)**

---

## 5. Zero-Copy Architecture

> **Relationship to Section 2:** This section expands on the buffer management issues with a focus on achieving zero-copy I/O, which requires deeper architectural changes.

### Current State

**Data flow for a page write:**
```
Page Cache (string)
    ↓ copyMem (allocation: newSeq[byte])
Pending Queue (seq[byte])
    ↓ encodeFrameInto (copy to frameBuffer)
WAL File (disk)
```

**Allocations per single-page commit:**
1. Page cache stores data as `string`
2. `writePageDirect` allocates `seq[byte]` and copies
3. `commit()` may resize `frameBuffer` 
4. Frame encoding copies into frameBuffer
5. VFS allocates write buffer

**Total: 2-3 heap allocations + 2-3 memory copies**

### SQLite Approach

**Data flow:**
```
Page Cache (byte array in mmap region)
    ↓ pwrite() syscall
WAL File (disk)
```

**Allocations: 0 (if using mmap)**
**Copies: 0** (DMA from page cache to disk)

**Key techniques:**
1. **Memory-mapped I/O (mmap)**:
   - Database file mapped directly into address space
   - WAL file also mapped
   - "Writes" are just `memcpy()` to mapped region
   - Kernel flushes dirty pages asynchronously
   - `msync()` for durability instead of `fsync()`

2. **Zero-copy page cache**:
   - SQLite's page cache is `unsigned char *` (byte array)
   - Aligns with OS page boundaries
   - Direct I/O from cache to disk via `pwritev()` (vectored I/O)

3. **Scatter-gather I/O**:
   ```c
   struct iovec iov[MAX_PAGES_PER_COMMIT];
   for (i = 0; i < nPages; i++) {
       iov[i].iov_base = pageCache[i].data;
       iov[i].iov_len = pageSize;
   }
   writev(walFd, iov, nPages);
   ```
   - Writes multiple pages in one syscall
   - No need to copy into intermediate buffer
   - Kernel optimizes sequential writes

### Required Changes

**Medium Term:**

1. **Add mmap-based VFS backend**:
   ```nim
   type MmapVfs* = ref object of Vfs
     mappings: Table[string, pointer]  # path -> mmap base
   
   method write(file: VfsFile, offset: int64, data: openArray[byte]): ...
     # If file is mmap'd: just memcpy
     else: fall back to pwrite()
   ```

2. **Change page cache to use `seq[byte]`**: 
   ```nim
   # Current
   type Page* = object
     data*: string  # String representation
   
   # Target
   type Page* = object
     data*: seq[byte]  # Byte representation
   ```
   - Eliminates string → seq conversion
   - Aligns with VFS expectations

3. **Vectored I/O (writev)** for multi-page commits:
   ```nim
   when defined(linux):
     proc writeWalVectored(wal: Wal, pages: seq[(PageId, seq[byte])]): ...
       # Construct iovec array
       # Single writev() syscall
   ```

**Impact Estimate:**
- Mmap for WAL: **30-40% reduction** in write latency (no kernel buffer copy)
- Zero-copy page cache: **20-30% reduction** (eliminates one allocation + copy)
- Vectored I/O: **10-20% improvement** for multi-page commits
- **Total: 60-90% improvement possible (0.115ms → 0.012-0.046ms)**

---

## 6. Durability Strategy and Checkpoint Design

### Current State

**Per-commit fsync pattern:**
```
write WAL frames
fsync WAL file        ← 1st fsync (durability)
release locks         ← point of durability
maybe checkpoint
  write DB pages
  fsync DB file       ← 2nd fsync (if triggered)
  fsync WAL again     ← 3rd fsync (checkpoint frame)
```

**Issues:**
1. **Double fsync on checkpoint**: When a checkpoint triggers during commit, we fsync WAL, then DB, then WAL again
2. **DB file synchronization**: SQLite never fsyncs the DB file on commit (only during checkpoint)
3. **Synchronous metadata**: `fdatasync` still updates inode timestamps, causing unnecessary I/O

### SQLite Approach

**PRAGMA synchronous=FULL (default durability):**
```
write WAL frames
fsync WAL file        ← Only fsync!
release locks         ← Durability achieved
                      ← DB file IS NOT SYNCED
                      ← Readers see changes via WAL

[Separate checkpoint thread/process]
When threshold met:
  write DB pages
  fsync DB file
  checkpoint frame to WAL
  fsync WAL file
  truncate WAL
```

**Key insight:** SQLite only syncs the WAL on commit. The DB file is updated asynchronously during checkpoint. This is safe because:
1. WAL contains all committed data
2. Crash recovery replays WAL before opening DB
3. DB file is just a cache of the WAL

**PRAGMA synchronous=NORMAL:**
```
write WAL frames
[No fsync!]          ← Just write to OS page cache
release locks
                     ← OS flushes within ~1 second
                     ← Or checkpoint forces flush
```

> ⚠️ **Durability Warning:** This mode can lose committed transactions on power failure. Per DecentDB's north star ("Priority #1: Durable ACID writes"), this mode should be **opt-in only** and clearly documented as trading durability for speed.

This is still safe because:
- Power loss loses only uncheckpointed data (usually <<1 second)
- Applications that need durability use FULL mode
- Most embedded apps are OK with ~1 second durability window

### Required Changes

**Medium Term (requires recovery protocol changes):**

1. **Remove DB fsync from commit path**:
   ```nim
   proc commitTransaction(db: Db): Result[Void] =
     # ... write to WAL ...
     let syncRes = wal.vfs.fsync(wal.file)  # Only fsync WAL!
     # DO NOT fsync DB file here
     # Update in-memory state
     # Release locks
     # Checkpoint happens asynchronously
   ```

2. **Add synchronous=NORMAL mode**:
   ```nim
   type DurabilityMode* = enum
     dmFull      # fsync on every commit
     dmNormal    # Write to OS cache, let OS flush
     dmOff       # No durability (testing only)
   ```

3. **Separate checkpoint thread**:
   ```nim
   type CheckpointThread* = ref object
     db*: Db
     intervalMs*: int64
     thread*: Thread[CheckpointThread]
   
   proc checkpointThreadProc(ctx: CheckpointThread) =
     while running:
       sleep(ctx.intervalMs)
       maybeCheckpoint(ctx.db.wal, ctx.db.pager)
   ```

**Impact Estimate:**
- Remove DB fsync from commit: **40-50% improvement** (most impactful!)
  - Single fsync instead of 1-3 fsyncs per commit
  - Matches SQLite behavior
- Normal durability mode: **70-80% improvement** (matches SQLite NORMAL mode)
  - But weaker durability guarantees
  - May not be acceptable for all use cases
- **With both FULL mode changes: 40-50% improvement possible**
- **With NORMAL mode: 70-80% improvement possible**

---

## 7. Checksum Calculation Overhead

> ⚠️ **ADR Required:** Removing per-frame CRC32C is a **format-breaking change** that affects corruption detection semantics. Per AGENTS.md, this requires an ADR before implementation. See design/adr/README.md.

### Current State

**Per-frame CRC32C:**
```nim
let checksum = uint64(crc32c(dest.toOpenArray(offset, offset + HeaderSize + payload.len - 1)))
writeU64LE(dest, offset + HeaderSize + payload.len, checksum)
```

**Cost of CRC32C:**
- Software implementation: ~10-20 cycles per byte on modern CPUs
- For 4096-byte page: ~40,000-80,000 cycles
- At 4GHz: ~10-20 microseconds per page
- For single-page commit: ~10-20us of overhead

**Comparison:**
- DecentDB total commit: ~115us
- CRC32C overhead: ~10-20us (~9-17% of total)

### SQLite Approach

**SQLite has NO per-frame checksums!**

1. **WAL Header Salt:**
   ```c
   struct WalHdr {
     u32 iVersion;
     u32 iUnused1;
     u32 iSalt[2];  // Random salt, changes when WAL resets
     u32 aFrameCksum[2];  // Checksum of header only!
   };
   ```

2. **Per-Frame Structure:**
   ```c
   struct WalFrame {
     u32 pgno;      // Page number
     u32 nDbSize;   // DB size after commit (for commit frame)
     // NO CHECKSUM!
     // Page content follows immediately
   };
   ```

3. **Integrity Guarantees:**
   - Power loss: OS/filesystem handles partial writes
   - Torn writes: Detected by salt mismatch (WAL header salt vs frame salt)
   - Corruption application failure: Only affects uncommitted transactions

**Why this is safe:**
- Modern filesystems (ext4, xfs, APFS) have their own checksums
  - ⚠️ **Caveat:** ext4 requires `metadata_csum` mount option; not all deployments have this
- Disk controllers have ECC
- SQLite's approach has been battle-tested for 15+ years
- Most corruption comes from application bugs, not disk failures

> **Note on CRC32C Performance:** The 10-20 cycles/byte estimate assumes software CRC32C. With Intel SSE4.2 hardware acceleration (`crc32` instruction), throughput is ~1 cycle/byte. If DecentDB uses hardware CRC32C, the savings from removal may be smaller (~5us vs ~15us).

### Required Changes

**Medium Term (requires format change):**

1. **Remove per-frame CRC32C:**
   ```nim
   # Old:
   let checksum = uint64(crc32c(...))
   writeU64LE(dest, offset + HeaderSize + payload.len, checksum)
   writeU64LE(dest, offset + HeaderSize + payload.len + 8, lsn)
   
   # New:
   # No checksum - rely on filesystem
   writeU64LE(dest, offset + HeaderSize + payload.len, lsn)
   ```

2. **Add corruption detection via salt:**
   ```nim
   type WalHdr = object
     version: uint32
     salt: array[2, uint32]  # Random salt, changes on WAL reset
     frameCount: uint32
     lastCheckpoint: uint64
   
   # On recovery, verify frame count matches
   # If mismatch, WAL was partially written
```

3. **Validate frame integrity differently:**
   ```nim
   # Check that page numbers are valid (not 0, within bounds)
   # Check that frame doesn't exceed WAL file size
   # These catch most corruption cases
   ```

**Impact Estimate:**
- Remove CRC32C calculation: ~10-15% improvement
  - Saves ~10-20us per commit for single-page updates
- Reduced frame size: ~5% improvement (fewer bytes to write)
- **Total: 10-20% improvement (0.115ms → 0.092-0.104ms)**

---

## 8. OS/Filesystem Interaction Patterns

### Current State

**Current syscall pattern per commit:**
```
1. pwrite() - write one or more frames to WAL
2. fdatasync() - ensure durability
3. (Optional) pwrite() multiple times during checkpoint
4. (Optional) fdatasync() of DB file
5. (Optional) pwrite() - checkpoint frame
6. (Optional) fdatasync() of WAL again
```

**Problem:** Each syscall has overhead (context switch, kernel entry/exit)

### SQLite Optimizations

**1. Vectored I/O (writev)**:
```c
// Write multiple frames in single syscall
struct iovec frames[10];
for (i = 0; i < nFrames; i++) {
    frames[i].iov_base = pageData[i];
    frames[i].iov_len = pageSize;
}
writev(walFd, frames, nFrames);  // One syscall for 10 pages
```
- Reduces syscalls from N to 1 for N pages
- Kernel optimizes sequential writes

**2. Linux AIO/io_uring** (SQLite 3.40+):
```c
// Asynchronous I/O using io_uring
struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
io_uring_prep_write(sqe, fd, buf, len, offset);
io_uring_submit(&ring);
// Continue processing, don't wait for I/O
```
- Overlaps computation with I/O
- Can batch multiple fsyncs

**3. F2FS/ext4 specific hints**:
```c
// Tell filesystem this is a WAL
fcntl(fd, F_SET_RW_HINT, RWH_WRITE_LIFE_SHORT);
// Or use direct I/O for WAL
open(path, O_DIRECT | O_RDWR);
```

### Required Changes

**Short Term**:
1. **Add vectored write to VFS**:
   ```nim
   method writev*(vfs: Vfs, file: VfsFile, buffers: seq[(int64, openArray[byte])]): Result[Void]
   ```
   - Write multiple buffers in single syscall
   - Implementation uses `writev()` on POSIX

**Medium Term**:
2. **Integrate io_uring on Linux**:
   - Add async I/O path for Linux 5.1+
   - Batch multiple commits' fsyncs together
   - Configurable: use blocking I/O or io_uring

3. **Use write hints**:
   - Mark WAL as short-lived data
   - Use direct I/O option for write-heavy workloads

**Impact Estimate:**
- Vectored I/O: ~5-10% for multi-page commits
- io_uring: ~10-20% on modern Linux (overlaps I/O)
- Write hints: ~5% on filesystems that support them
- **Total: 15-30% improvement potential**

---

## 9. Summary: Path to SQLite Performance

### Current Gap
- **DecentDB**: 0.115ms p95 commit
- **SQLite**: 0.010ms p95 commit
- **Gap**: 11.5x
- **Benchmark Context**: Single-threaded, single-page UPDATE commits

### Achievable Improvements by Category

> **Legend:** 🧵 = Single-threaded impact, 🔀 = Multi-threaded only

| Optimization | Effort | Impact | Target Latency | Scope |
|-------------|--------|--------|----------------|-------|
| Remove DB fsync on commit | Medium | 40-50% | 0.058-0.069ms | 🧵 |
| Remove per-frame CRC32C | Medium | 10-15% | 0.098-0.104ms | 🧵 |
| Lock release before fsync | Medium | 5-10% | 0.104-0.109ms | 🧵🔀 |
| Zero-copy page writes | High | 15-20% | 0.092-0.098ms | 🧵 |
| io_uring on Linux | High | 10-20% | 0.092-0.104ms | 🧵 |
| Lock-free commit publishing | High | 10-15% | 0.098-0.104ms | 🔀 only |

**If ALL optimizations are implemented:**
- Conservative: 0.115ms × 0.5 × 0.85 × 0.9 × 0.8 × 0.8 × 0.85 ≈ **0.048ms** (2.4× SQLite)
- Optimistic: 0.115ms × 0.5 × 0.85 × 0.9 × 0.85 × 0.8 × 0.85 ≈ **0.042ms** (2.1× SQLite)

### Most Impactful Changes (Priority Order)

1. **Remove DB fsync on commit** (40-50% improvement)
   - Only fsync WAL on commit
   - Fsync DB only during checkpoint
   - Requires WAL format that can fully recover DB
   - **Effort**: Medium (3-5 days)
   - **Risk**: MEDIUM - changes recovery semantics

2. **Remove per-frame CRC32C** (10-15% improvement)
   - Rely on filesystem checksums (ext4, xfs, zfs all have them)
   - Add corruption detection via salt
   - **Effort**: Medium (2-3 days)
   - **Risk**: LOW - well-established approach
   - **⚠️ Requires ADR** (format change per AGENTS.md)

3. **Release lock before fsync** (5-10% improvement + better concurrency)
   - Allows multiple connections to fsync concurrently
   - Reduces lock contention
   - **Effort**: Medium (2-3 days)
   - **Risk**: LOW - SQLite does this

4. **Zero-copy page writes** (15-20% improvement)
   - Change page cache to use `seq[byte]` throughout
   - Write directly from cache to disk
   - **Effort**: High (1-2 weeks)
   - **Risk**: MEDIUM - touches many modules

5. **Linux io_uring integration** (10-20% improvement)
   - Asynchronous I/O for large operations
   - Batch multiple operations
   - **Effort**: High (1-2 weeks)
   - **Risk**: LOW - opt-in feature

### SQLite's 0.010ms Secret

How does SQLite achieve 0.010ms commits? Let's trace:

1. **WAL write**: ~2-5us (write 2 frames: page + commit)
2. **CRC calculation**: ~5us (simple checksum, not CRC32C)
3. **fdatasync**: ~5-8us (SSD with write caching)
4. **Memory management**: ~0us (no allocations in hot path)

**Total**: ~12-18us per commit → p95 is 10us (some commits faster)

**DecentDB breakdown:**
1. **WAL write**: ~5-10us (more overhead, CRC32C)
2. **CRC32C calculation**: ~15-20us (CRC32C is expensive)
3. **fdatasync**: ~5-8us
4. **frameBuffer allocation/resizing**: ~5-10us
5. **pending queue allocation**: ~5-10us
6. **DB checkpoint fsync** (if triggered): ~50-100us

**Total**: ~90-158us per commit → p95 is 115us

### The Path Forward

To get from 0.115ms to ~0.020ms (2x SQLite), DecentDB would need:

1. **Remove CRC32C** (-15-20us) [MEDIUM effort]
2. **Remove DB fsync on commit** (-50-100us when triggered) [MEDIUM effort]
3. **Zero allocations in hot path** (-10-20us) [HIGH effort]
4. **Simpler frame format** (-5-10us) [MEDIUM effort]

**Expected result**: ~65-110us → ~20-40us (2.75×-5.5× improvement)

To match SQLite exactly (0.010ms), would also need:
5. **mmap for WAL** (-5-10us) [HIGH effort]
6. **Lock-free commit publishing** (-5-10us) [HIGH effort]
7. **Per-thread WAL buffers** [HIGH effort]

**Conclusion**: Getting to 2× SQLite performance is achievable with medium effort (weeks). Getting to parity requires significant architectural changes (months) that sacrifice some of DecentDB's safety guarantees.

---

## Progress (2026-02-05)

**Baseline (run_id: 20260205_174417)**  
DecentDB vs SQLite (commit latency gap: **14.61×**)

| Metric | DecentDB | SQLite |
|---|---:|---:|
| commit_p95_ms | 0.127369 | 0.008716 |
| read_p95_ms | 0.001252 | 0.001854 |
| join_p95_ms | 0.493286 | 0.371568 |
| insert_rows_per_sec | 200,964.63 | 121,787.85 |
| db_size_mb (bytes/1e6) | 0.086016 | 0.045056 |

### 1) Pre-size WAL frameBuffer (Section 2: Memory Copying and Buffer Allocation)
**Change:** Pre-size `wal.frameBuffer` per commit based on pending frames to avoid incremental growth during encoding.  
**Bench (run_id: 20260205_185403)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.127369 | 0.1127165 | **Improved** (~11.5%) |
| read_p95_ms | 0.001252 | 0.001197 | Improved |
| join_p95_ms | 0.493286 | 0.497524 | +0.86% (within noise) |
| insert_rows_per_sec | 200,964.63 | 195,020.60 | -2.95% (within noise) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009798 → gap **11.50×**  
**Correctness/Durability:** No changes to WAL semantics or recovery.  
**Follow-ups:** Next low-risk item: direct string-to-frame encoding (avoid string→seq copy).

### 2) Direct string-to-frame encoding (Section 2: Memory Copying and Buffer Allocation)
**Change:** Store pending pages as strings and encode directly into WAL frames to avoid string→seq allocation.  
**Bench (run_id: 20260205_190231)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.1127165 | 0.1111285 | **Improved** (~1.4%) |
| read_p95_ms | 0.001197 | 0.0011825 | Improved |
| join_p95_ms | 0.497524 | 0.496702 | Improved |
| insert_rows_per_sec | 195,020.60 | 199,036.46 | Improved |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009117 → gap **12.19×**  
**Correctness/Durability:** No changes to WAL semantics or recovery.  
**Follow-ups:** Next low-risk item: release WAL lock before fsync (careful with commit window).

### 3) Faster CRC32C (slicing-by-8) (Section 1: WAL Frame Format Overhead)
**Change:** Replace byte-at-a-time CRC32C with slicing-by-8 table implementation (same polynomial/semantics).  
**Bench (run_id: 20260205_191231)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.1111285 | 0.09579 | **Improved** (~13.8%) |
| read_p95_ms | 0.0011825 | 0.001177 | Improved |
| join_p95_ms | 0.496702 | 0.4711145 | Improved |
| insert_rows_per_sec | 199,036.46 | 196,183.58 | -1.43% (within noise) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009508 → gap **10.07×**  
**Correctness/Durability:** CRC32C semantics preserved; added slow-vs-fast test for parity.  
**Follow-ups:** Next medium effort: release lock before fsync (ADR required) or CRC32C removal (ADR required).

### Rejected: Single-page direct WAL write via `writev`
**Change:** Added streaming CRC + `writev` path for single-page commits to avoid copying payload into `frameBuffer`.  
**Bench (run_id: 20260205_192133)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.09579 | 0.0968015 | **Regressed** (~1.1%) |
| read_p95_ms | 0.001177 | 0.0011825 | Flat |
| join_p95_ms | 0.4711145 | 0.473148 | Flat |
| insert_rows_per_sec | 196,183.58 | 200,588.67 | Improved |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**Decision:** Reverted due to commit latency regression (primary metric).  
**Notes:** Possible syscall overhead from `writev` path offset the copy savings in this workload.

### 4) Remove per-frame WAL CRC32C (format v5) (Section 1: WAL Frame Format Overhead)
**Change:** Stop computing/validating per-frame CRC32C. The checksum field is now reserved and written as zero in format v5.  
**Bench (run_id: 20260205_192941)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.09579 | 0.082635 | **Improved** (~13.7%) |
| read_p95_ms | 0.001177 | 0.001187 | +0.85% (within noise) |
| join_p95_ms | 0.4711145 | 0.4578895 | Improved |
| insert_rows_per_sec | 196,183.58 | 195,730.08 | -0.23% (within noise) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.008917 → gap **9.27×**  
**Correctness/Durability:** Per-frame corruption detection removed; recovery now validates frame invariants only. WAL format version bumped to v5 (new DBs only).  
**Follow-ups:** Next medium effort: remove per-frame LSN trailer (ADR required) or release wal lock before fsync (ADR required).

### 5) Remove per-frame WAL LSN trailer (format v6) (Section 1: WAL Frame Format Overhead)
**Change:** Remove LSN from frame trailer; LSNs are derived from WAL byte offsets (frame end offset).  
**Bench (run_id: 20260205_193932)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.082635 | 0.0794645 | **Improved** (~3.8%) |
| read_p95_ms | 0.001187 | 0.0011725 | Improved |
| join_p95_ms | 0.4578895 | 0.44195 | Improved |
| insert_rows_per_sec | 195,730.08 | 201,869.72 | Improved |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009408 → gap **8.45×**  
**Correctness/Durability:** LSNs now derived from WAL byte offsets; WAL format version bumped to v6 (new DBs only).  
**Follow-ups:** Next medium effort: simplify frame header (remove payload length / frame type) or unify page representation for zero-copy WAL writes (ADR required).

### 6) Single-page pending fast path (Section 2: Memory Copying and Buffer Allocation)
**Change:** Avoid allocating the pending queue for single-page commits by storing the first pending page inline.  
**Bench (run_id: 20260205_194416)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.0794645 | 0.0790035 | **Improved** (~0.6%) |
| read_p95_ms | 0.0011725 | 0.001172 | Flat |
| join_p95_ms | 0.44195 | 0.4491985 | +1.6% (within noise) |
| insert_rows_per_sec | 201,869.72 | 201,193.70 | -0.3% (within noise) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009799 → gap **8.06×**  
**Correctness/Durability:** No changes to WAL semantics or recovery.  
**Follow-ups:** Next medium effort: simplify frame header (remove payload length / frame type) or unify page representation for zero-copy WAL writes (ADR required).

### 7) Remove WAL payload length field (format v7) (Section 1: WAL Frame Format Overhead)
**Change:** Remove `payload_length` from frame headers; payload sizes are derived from frame type and page size.  
**Bench (run_id: 20260205_195032)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.0790035 | 0.078693 | **Improved** (~0.4%) |
| read_p95_ms | 0.001172 | 0.001177 | +0.4% (within noise) |
| join_p95_ms | 0.4491985 | 0.4447445 | Improved |
| insert_rows_per_sec | 201,193.70 | 200,154.99 | -0.5% (within noise) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009808 → gap **8.02×**  
**Correctness/Durability:** WAL format version bumped to v7; payload size derived from frame type and page size.  
**Follow-ups:** Next medium effort: remove frame type field (infer from position) or unify page representation for zero-copy WAL writes (ADR required).

### Rejected: Remove WAL frame type field (format v8)
**Change:** Infer frame type from page_id sentinel values to drop the explicit frame_type byte.  
**Bench (run_id: 20260205_195622)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.078693 | 0.0792585 | **Regressed** (~0.7%) |
| read_p95_ms | 0.001177 | 0.001172 | Improved |
| join_p95_ms | 0.4447445 | 0.445491 | Flat |
| insert_rows_per_sec | 200,154.99 | 194,412.84 | -2.9% (likely noise, but commit regressed) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**Decision:** Reverted due to commit latency regression (primary metric).  
**Notes:** The header byte reduction did not offset the additional branching in this workload.

### Rejected: Reuse WAL pageMeta buffer (commit allocation)
**Change:** Reuse a WAL-level `pageMeta` buffer to avoid per-commit allocations.  
**Bench (run_id: 20260205_200412)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.078693 | 0.0795445 | **Regressed** (~1.1%) |
| read_p95_ms | 0.001177 | 0.0011725 | Improved |
| join_p95_ms | 0.4447445 | 0.437396 | Improved |
| insert_rows_per_sec | 200,154.99 | 197,863.71 | -1.1% (within noise, but commit regressed) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**Decision:** Reverted due to commit latency regression (primary metric).  
**Notes:** Allocation reuse did not translate into lower p95 latency in this workload.

### Rejected: Zero-copy WAL writev path (single-page commit)
**Change:** Attempted to avoid copying page payload into frameBuffer by writing header + payload + trailer via `writev`-style slices.  
**Bench (run_id: 20260205_215248)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.0786825 | 0.079329 | **Regressed** (~0.8%) |
| read_p95_ms | 0.001182 | 0.0013225 | **Regressed** (~11.9%) |
| join_p95_ms | 0.447451 | 0.448642 | +0.3% (within noise) |
| insert_rows_per_sec | 198,258.17 | 197,726.38 | -0.3% (within noise) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009739 → gap **8.15×**  
**Decision:** Reverted due to commit latency regression (primary metric).  
**Notes:** The slice-based write path increased overhead in this workload.

### Rejected: Linux `sync_file_range` preflush (WAL write path)
**Change:** Attempted best-effort `sync_file_range(..., SYNC_FILE_RANGE_WRITE)` after WAL append to overlap flush with fsync.  
**Bench (run_id: 20260205_220612)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.0786825 | 0.0823095 | **Regressed** (~4.6%) |
| read_p95_ms | 0.001182 | 0.0011975 | +1.3% (within noise) |
| join_p95_ms | 0.447451 | 0.4472595 | Flat |
| insert_rows_per_sec | 198,258.17 | 195,880.78 | -1.2% (within noise) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009668 → gap **8.51×**  
**Decision:** Reverted due to commit latency regression (primary metric).  
**Notes:** Preflush did not reduce fsync latency on this system; added overhead outweighed benefits.

### Rejected: Release WAL lock before fsync
**Change:** Publish WAL index before fsync and release `wal.lock` to overlap fsync with other writers.  
**Bench (run_id: 20260205_222821)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.0786825 | 0.0806015 | **Regressed** (~2.4%) |
| read_p95_ms | 0.001182 | 0.001197 | +1.3% (within noise) |
| join_p95_ms | 0.447451 | 0.444224 | Improved |
| insert_rows_per_sec | 198,258.17 | 198,198.80 | Flat |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009447 → gap **8.53×**  
**Decision:** Reverted due to commit latency regression (primary metric).  
**Notes:** No benefit in single-threaded commit benchmark; extra bookkeeping likely dominates.

### 8) Prepared UPDATE fast path for INT64 PK (Executor)
**Change:** Detect `WHERE pk = $param|literal` and bypass planner/rowid scan for a direct single-row lookup/update.  
**Bench (run_id: 20260205_211350)**  

| Metric | Before | After | Notes |
|---|---:|---:|---|
| commit_p95_ms | 0.078693 | 0.0786825 | Improved (within noise) |
| read_p95_ms | 0.001177 | 0.001182 | +0.4% (within noise) |
| join_p95_ms | 0.4447445 | 0.447451 | +0.6% (within noise) |
| insert_rows_per_sec | 200,154.99 | 198,258.17 | -0.95% (within noise) |
| db_size_mb (bytes/1e6) | 0.086016 | 0.086016 | Unchanged |

**SQLite reference (same run):** commit_p95_ms = 0.009227 → gap **8.53×**  
**Correctness/Durability:** No changes to WAL semantics or recovery.  
**Follow-ups:** Next high-ROI remains OS-level sync optimizations or zero-copy page cache.
