import os
import locks
import tables
import options
import ../errors
import ../vfs/types
import sets
import ./db_header

type PageId* = uint32
type PageOverlay* = proc(pageId: PageId): Option[string]
type ReadGuard* = proc(): Result[Void]

type CacheEntry* = ref object
  id*: PageId
  data*: string
  lsn*: uint64
  dirty*: bool
  pinCount*: int
  refBit*: bool
  lock*: Lock

type PageCacheShard = ref object
  capacity: int
  pages: Table[PageId, CacheEntry]
  clock: seq[PageId]
  clockHand: int
  lock*: Lock
  clockTombstones: int  # Count of deleted entries in clock (for triggering compaction)

type PageCache* = ref object
  capacity*: int
  shards: seq[PageCacheShard]

type Pager* = ref object
  vfs*: Vfs
  file*: VfsFile
  header*: DbHeader
  pageSize*: int
  pageCount*: uint32
  cache*: PageCache
  lock*: Lock
  overlay*: PageOverlay
  overlaySnapshot*: uint64
  readGuard*: ReadGuard
  overriddenPages*: HashSet[PageId]
  overlayLock*: Lock
  rollbackLock*: Lock    # Guards against seeing dirty state during rollback
  txnAllocatedPages*: seq[PageId]  # Pages allocated during current transaction (HIGH-003)
  inTransaction*: bool             # Whether a transaction is active (HIGH-003)
  flushHandler*: proc(pageId: PageId, data: string): Result[Void]

const DefaultCacheShards = 16

proc newPageCache*(capacity: int): PageCache =
  let cap = if capacity <= 0: 1 else: capacity
  var shardCount = min(DefaultCacheShards, cap)
  if shardCount <= 0:
    shardCount = 1
  let baseCap = cap div shardCount
  let remainder = cap mod shardCount
  var shards: seq[PageCacheShard] = @[]
  for i in 0 ..< shardCount:
    let shardCap = baseCap + (if i < remainder: 1 else: 0)
    let shard = PageCacheShard(
      capacity: max(1, shardCap), 
      pages: initTable[PageId, CacheEntry](), 
      clock: @[], 
      clockHand: 0,
      clockTombstones: 0
    )
    initLock(shard.lock)
    shards.add(shard)
  let cache = PageCache(capacity: cap, shards: shards)
  cache

proc newPager*(vfs: Vfs, file: VfsFile, cachePages: int = 1024): Result[Pager] =
  let headerRes = readHeader(vfs, file)
  if not headerRes.ok:
    return err[Pager](headerRes.err.code, headerRes.err.message, headerRes.err.context)
  let header = headerRes.value
  if header.formatVersion != FormatVersion:
    return err[Pager](ERR_CORRUPTION, "Unsupported format version", "page_id=1")
  if header.pageSize != DefaultPageSize:
    return err[Pager](ERR_CORRUPTION, "Unsupported page size", "page_id=1")
  let pageSize = int(header.pageSize)
  let fileInfo = getFileInfo(file.path)
  if fileInfo.size > 0 and (fileInfo.size mod pageSize) != 0:
    return err[Pager](ERR_CORRUPTION, "File size not aligned to page size", file.path)
  let count = if fileInfo.size == 0: 0'u32 else: uint32(fileInfo.size div pageSize)
  let cache = newPageCache(cachePages)
  var pager = Pager(vfs: vfs, file: file, header: header, pageSize: pageSize, pageCount: count, cache: cache, overlaySnapshot: 0)
  pager.overriddenPages = initHashSet[PageId]()
  initLock(pager.lock)
  initLock(pager.overlayLock)
  initLock(pager.rollbackLock)
  ok(pager)

proc pageOffset(pager: Pager, pageId: PageId): int64 =
  int64(pageId - 1) * int64(pager.pageSize)

proc ensurePageId(pager: Pager, pageId: PageId): Result[Void] =
  if pageId < 1 or pageId > pager.pageCount:
    return err[Void](ERR_IO, "Page id out of bounds", "page_id=" & $pageId)
  okVoid()

proc flushEntry(pager: Pager, entry: CacheEntry): Result[Void] =
  if not entry.dirty:
    return okVoid()
  acquire(entry.lock)
  defer: release(entry.lock)

  if pager.flushHandler != nil:
    let res = pager.flushHandler(entry.id, entry.data)
    if not res.ok:
      return res
    
    acquire(pager.overlayLock)
    pager.overriddenPages.excl(entry.id)
    release(pager.overlayLock)
  else:
    let offset = pageOffset(pager, entry.id)
    let res = pager.vfs.writeStr(pager.file, offset, entry.data)
    if not res.ok:
      return err[Void](res.err.code, res.err.message, res.err.context)
    if res.value < pager.pageSize:
      return err[Void](ERR_IO, "Short write on page", "page_id=" & $entry.id)

    acquire(pager.overlayLock)
    pager.overriddenPages.incl(entry.id)
    release(pager.overlayLock)

  entry.dirty = false
  okVoid()

proc splitmix64(x: uint64): uint64 =
  ## Splitmix64 hash function for better hash distribution.
  ## This is a high-quality hash function that provides good
  ## distribution properties for hash table indexing.
  var z = x
  z = (z + 0x9e3779b97f4a7c15'u64)
  z = (z xor (z shr 30)) * 0xbf58476d1ce4e5b9'u64
  z = (z xor (z shr 27)) * 0x94d049bb133111eb'u64
  z = z xor (z shr 31)
  return z

proc shardFor(cache: PageCache, pageId: PageId): PageCacheShard =
  ## Select shard using splitmix64 hash for better distribution.
  ## This reduces lock contention compared to simple modulo hashing.
  let hash = splitmix64(uint64(pageId))
  let idx = int(hash mod uint64(cache.shards.len))
  cache.shards[idx]

const
  # Trigger compaction when tombstones exceed this percentage of clock size
  ClockTombstoneThresholdPct = 25

proc compactClock(shard: PageCacheShard) =
  ## Remove tombstones from clock array to reclaim space.
  ## Called periodically when tombstone count exceeds threshold.
  var newClock: seq[PageId] = @[]
  # Pre-allocate capacity to minimize reallocations
  newClock.setLen(0)
  for pageId in shard.clock:
    if pageId != PageId(0):  # 0 is tombstone sentinel
      newClock.add(pageId)
  shard.clock = newClock
  shard.clockTombstones = 0
  if shard.clockHand >= shard.clock.len:
    shard.clockHand = 0

proc evictIfNeededLocked(pager: Pager, shard: PageCacheShard): Result[Void] =
  ## Clock eviction with mark-and-compact instead of O(N) deletion.
  ## Uses PageId(0) as tombstone sentinel and compacts periodically.
  while shard.pages.len >= shard.capacity:
    if shard.clock.len == 0:
      break
    
    # Trigger compaction if too many tombstones
    if shard.clockTombstones > 0 and shard.clock.len > 0:
      let tombstonePct = (shard.clockTombstones * 100) div shard.clock.len
      if tombstonePct >= ClockTombstoneThresholdPct:
        compactClock(shard)
    
    var scanned = 0
    var evicted = false
    var anyUnpinned = false
    let scanLimit = (shard.clock.len - shard.clockTombstones) * 2  # Scan only non-tombstones
    
    while scanned < scanLimit and not evicted:
      if shard.clockHand >= shard.clock.len:
        shard.clockHand = 0
      
      let pageId = shard.clock[shard.clockHand]
      let currentIndex = shard.clockHand
      shard.clockHand.inc
      
      # Skip tombstones
      if pageId == PageId(0):
        continue
      
      scanned.inc
      let entry = shard.pages.getOrDefault(pageId, nil)
      
      if entry == nil:
        # Entry was removed but not marked as tombstone - mark it now
        shard.clock[currentIndex] = PageId(0)
        shard.clockTombstones.inc
        continue
      
      if entry.pinCount > 0:
        continue
      
      anyUnpinned = true
      if entry.refBit:
        entry.refBit = false
        continue
      
      let flushRes = flushEntry(pager, entry)
      if not flushRes.ok:
        return flushRes
      
      # Mark as tombstone instead of O(N) delete
      shard.pages.del(pageId)
      shard.clock[currentIndex] = PageId(0)
      shard.clockTombstones.inc
      evicted = true
    
    if not evicted:
      if not anyUnpinned:
        return err[Void](ERR_INTERNAL, "No evictable page in cache")
      continue
  
  okVoid()

proc pinPage*(pager: Pager, pageId: PageId): Result[CacheEntry] =
  let bound = ensurePageId(pager, pageId)
  if not bound.ok:
    return err[CacheEntry](bound.err.code, bound.err.message, bound.err.context)
  let cache = pager.cache
  let shard = shardFor(cache, pageId)
  
  # Fast path: check cache
  acquire(shard.lock)
  if shard.pages.hasKey(pageId):
    let entry = shard.pages[pageId]
    entry.pinCount.inc
    entry.refBit = true
    release(shard.lock)
    return ok(entry)
  release(shard.lock)

  # Slow path: load data (without holding lock)
  var data = newString(pager.pageSize)
  var loaded = false
  
  if pager.overlay != nil:
    let overlayRes = pager.overlay(pageId)
    if overlayRes.isSome:
      data = overlayRes.get
      loaded = true
  
  if not loaded:
    let offset = pageOffset(pager, pageId)
    let readRes = pager.vfs.readStr(pager.file, offset, data)
    if not readRes.ok:
      return err[CacheEntry](readRes.err.code, readRes.err.message, readRes.err.context)
    if readRes.value < pager.pageSize:
      return err[CacheEntry](ERR_CORRUPTION, "Short read on page", "page_id=" & $pageId)

  # Re-acquire lock to insert
  acquire(shard.lock)
  # Check if someone else loaded it while we were reading
  if shard.pages.hasKey(pageId):
    let entry = shard.pages[pageId]
    entry.pinCount.inc
    entry.refBit = true
    release(shard.lock)
    return ok(entry)
    
  let evictRes = evictIfNeededLocked(pager, shard)
  if not evictRes.ok:
    release(shard.lock)
    return err[CacheEntry](evictRes.err.code, evictRes.err.message, evictRes.err.context)

  let entry = CacheEntry(id: pageId, data: data, lsn: pager.header.lastCheckpointLsn, dirty: false, pinCount: 1, refBit: true)
  initLock(entry.lock)
  shard.pages[pageId] = entry
  shard.clock.add(pageId)
  release(shard.lock)
  ok(entry)

proc unpinPage*(pager: Pager, entry: CacheEntry, dirty: bool = false): Result[Void] =
  let cache = pager.cache
  let shard = shardFor(cache, entry.id)
  acquire(shard.lock)
  defer: release(shard.lock)
  if entry.pinCount > 0:
    entry.pinCount.dec
  if dirty:
    entry.dirty = true
  okVoid()

proc readPageDirect*(pager: Pager, pageId: PageId): Result[string]

proc cloneString(buf: string): string =
  if buf.len == 0:
    return ""
  result = newString(buf.len)
  copyMem(addr result[0], unsafeAddr buf[0], buf.len)

proc withPageRoCached*[T](pager: Pager, pageId: PageId, snapshot: Option[uint64], body: proc(page: string): Result[T]): Result[T] =
  ## Invoke `body` with a borrowed view of a cached page when safe.
  ##
  ## This avoids allocating/copying a full page image on the hot read path by
  ## pinning the cache entry and holding the entry lock while the callback runs.
  ##
  ## If a snapshot is provided and the cached page is not valid for that snapshot
  ## (dirty or newer LSN), this falls back to a direct file read.
  
  # Wait if rollback is in progress to avoid seeing dirty state
  acquire(pager.rollbackLock)
  release(pager.rollbackLock)
  
  if pager.readGuard != nil:
    let guardRes = pager.readGuard()
    if not guardRes.ok:
      return err[T](guardRes.err.code, guardRes.err.message, guardRes.err.context)
  let pinRes = pinPage(pager, pageId)
  if not pinRes.ok:
    return err[T](pinRes.err.code, pinRes.err.message, pinRes.err.context)
  let entry = pinRes.value
  acquire(entry.lock)
  var pinned = true
  defer:
    if pinned:
      release(entry.lock)
      discard unpinPage(pager, entry)
  if snapshot.isSome:
    let snap = snapshot.get
    if snap > 0 and (entry.dirty or entry.lsn > snap):
      release(entry.lock)
      discard unpinPage(pager, entry)
      pinned = false
      let directRes = readPageDirect(pager, pageId)
      if not directRes.ok:
        return err[T](directRes.err.code, directRes.err.message, directRes.err.context)
      return body(directRes.value)
  body(entry.data)

proc readPageCachedCopy(pager: Pager, pageId: PageId, snapshot: Option[uint64]): Result[string] =
  let pinRes = pinPage(pager, pageId)
  if not pinRes.ok:
    return err[string](pinRes.err.code, pinRes.err.message, pinRes.err.context)
  let entry = pinRes.value
  acquire(entry.lock)
  let entryDirty = entry.dirty
  let entryLsn = entry.lsn
  let entryData = entry.data
  release(entry.lock)
  discard unpinPage(pager, entry)
  if snapshot.isSome:
    let snap = snapshot.get
    if snap > 0 and (entryDirty or entryLsn > snap):
      return readPageDirect(pager, pageId)
  ok(cloneString(entryData))

proc readPageCachedShared(pager: Pager, pageId: PageId, snapshot: Option[uint64]): Result[string] =
  let pinRes = pinPage(pager, pageId)
  if not pinRes.ok:
    return err[string](pinRes.err.code, pinRes.err.message, pinRes.err.context)
  let entry = pinRes.value
  acquire(entry.lock)
  let entryDirty = entry.dirty
  let entryLsn = entry.lsn
  let entryData = entry.data
  release(entry.lock)
  discard unpinPage(pager, entry)
  if snapshot.isSome:
    let snap = snapshot.get
    if snap > 0 and (entryDirty or entryLsn > snap):
      return readPageDirect(pager, pageId)
  ok(entryData)

proc withPageRo*[T](pager: Pager, pageId: PageId, body: proc(page: string): Result[T]): Result[T] =
  ## Call `body` with a read-only view of `pageId` without forcing a page-sized copy.
  ##
  ## If a WAL/page overlay is installed, it is consulted first.
  
  # Wait if rollback is in progress to avoid seeing dirty state
  acquire(pager.rollbackLock)
  release(pager.rollbackLock)
  
  if pager.readGuard != nil:
    let guardRes = pager.readGuard()
    if not guardRes.ok:
      return err[T](guardRes.err.code, guardRes.err.message, guardRes.err.context)
  if pager.overlay != nil:
    let overlayRes = pager.overlay(pageId)
    if overlayRes.isSome:
      return body(overlayRes.get)
    return withPageRoCached(pager, pageId, some(pager.overlaySnapshot), body)
  withPageRoCached(pager, pageId, none(uint64), body)

proc readPage*(pager: Pager, pageId: PageId): Result[string] =
  # Wait if rollback is in progress to avoid seeing dirty state
  acquire(pager.rollbackLock)
  release(pager.rollbackLock)
  
  if pager.readGuard != nil:
    let guardRes = pager.readGuard()
    if not guardRes.ok:
      return err[string](guardRes.err.code, guardRes.err.message, guardRes.err.context)
  if pager.overlay != nil:
    let overlayRes = pager.overlay(pageId)
    if overlayRes.isSome:
      return ok(overlayRes.get)
    if pager.overlaySnapshot > 0:
      return readPageCachedCopy(pager, pageId, some(pager.overlaySnapshot))
  readPageCachedCopy(pager, pageId, none(uint64))

proc readPageRo*(pager: Pager, pageId: PageId): Result[string] =
  ## Read page without copying when safe (treat returned string as immutable).
  if pager.readGuard != nil:
    let guardRes = pager.readGuard()
    if not guardRes.ok:
      return err[string](guardRes.err.code, guardRes.err.message, guardRes.err.context)
  if pager.overlay != nil:
    let overlayRes = pager.overlay(pageId)
    if overlayRes.isSome:
      return ok(overlayRes.get)
    return readPageCachedShared(pager, pageId, some(pager.overlaySnapshot))
  readPageCachedShared(pager, pageId, none(uint64))

proc readPageDirect*(pager: Pager, pageId: PageId): Result[string] =
  # Wait if rollback is in progress to avoid seeing dirty state
  acquire(pager.rollbackLock)
  release(pager.rollbackLock)
  
  if pager.readGuard != nil:
    let guardRes = pager.readGuard()
    if not guardRes.ok:
      return err[string](guardRes.err.code, guardRes.err.message, guardRes.err.context)
  let bound = ensurePageId(pager, pageId)
  if not bound.ok:
    return err[string](bound.err.code, bound.err.message, bound.err.context)
  var buf = newString(pager.pageSize)
  let offset = pageOffset(pager, pageId)
  let res = pager.vfs.readStr(pager.file, offset, buf)
  if not res.ok:
    return err[string](res.err.code, res.err.message, res.err.context)
  if res.value < pager.pageSize:
    return err[string](ERR_IO, "Short read on page", "page_id=" & $pageId)
  ok(buf)

proc setPageOverlay*(pager: Pager, snapshot: uint64, overlay: PageOverlay) =
  pager.overlay = overlay
  pager.overlaySnapshot = snapshot

proc clearPageOverlay*(pager: Pager) =
  pager.overlay = nil
  pager.overlaySnapshot = 0

proc setReadGuard*(pager: Pager, guard: ReadGuard) =
  pager.readGuard = guard

proc clearReadGuard*(pager: Pager) =
  pager.readGuard = nil

proc writePage*(pager: Pager, pageId: PageId, data: string): Result[Void] =
  if data.len != pager.pageSize:
    return err[Void](ERR_IO, "Page write size mismatch", "page_id=" & $pageId)
  let pinRes = pinPage(pager, pageId)
  if not pinRes.ok:
    return err[Void](pinRes.err.code, pinRes.err.message, pinRes.err.context)
  let entry = pinRes.value
  acquire(entry.lock)
  entry.data = data
  entry.dirty = true
  release(entry.lock)
  discard unpinPage(pager, entry, dirty = true)
  okVoid()

proc writePageDirectFile*(pager: Pager, pageId: PageId, data: string): Result[Void] =
  ## Write a page image directly to the database file (bypasses cache).
  if data.len != pager.pageSize:
    return err[Void](ERR_IO, "Page write size mismatch", "page_id=" & $pageId)
  let bound = ensurePageId(pager, pageId)
  if not bound.ok:
    return err[Void](bound.err.code, bound.err.message, bound.err.context)
  let offset = pageOffset(pager, pageId)
  let res = pager.vfs.writeStr(pager.file, offset, data)
  if not res.ok:
    return err[Void](res.err.code, res.err.message, res.err.context)
  if res.value < pager.pageSize:
    return err[Void](ERR_IO, "Short write on page", "page_id=" & $pageId)
  okVoid()

proc flushAll*(pager: Pager): Result[Void] =
  let cache = pager.cache
  var dirtyEntries: seq[CacheEntry] = @[]
  for shard in cache.shards:
    acquire(shard.lock)
    for _, entry in shard.pages:
      if entry.dirty:
        dirtyEntries.add(entry)
    release(shard.lock)
  # Fast path: if nothing was dirtied, do not issue an fsync.
  # This matters a lot for read-only workloads (e.g. CLI benchmarks) where
  # an unconditional fsync dominates latency.
  if dirtyEntries.len == 0:
    return okVoid()
  for entry in dirtyEntries:
    let res = flushEntry(pager, entry)
    if not res.ok:
      return res
  let syncRes = pager.vfs.fsync(pager.file)
  if not syncRes.ok:
    return err[Void](syncRes.err.code, syncRes.err.message, syncRes.err.context)
  okVoid()

proc snapshotDirtyPages*(pager: Pager): seq[(PageId, string)] =
  let cache = pager.cache
  var entries: seq[CacheEntry] = @[]
  for shard in cache.shards:
    acquire(shard.lock)
    for _, entry in shard.pages:
      entries.add(entry)
    release(shard.lock)
  for entry in entries:
    if not entry.dirty:
      continue
    acquire(entry.lock)
    let copy = entry.data
    release(entry.lock)
    result.add((entry.id, copy))

proc markPagesCommitted*(pager: Pager, pageIds: seq[PageId], lsn: uint64) =
  let cache = pager.cache
  if pageIds.len > 0:
    acquire(pager.overlayLock)
    for pageId in pageIds:
      pager.overriddenPages.excl(pageId)
    release(pager.overlayLock)
  for pageId in pageIds:
    let shard = shardFor(cache, pageId)
    acquire(shard.lock)
    if shard.pages.hasKey(pageId):
      let entry = shard.pages[pageId]
      acquire(entry.lock)
      entry.dirty = false
      entry.lsn = lsn
      release(entry.lock)
    release(shard.lock)

proc isDirty*(pager: Pager, pageId: PageId): bool =
  let shard = shardFor(pager.cache, pageId)
  acquire(shard.lock)
  defer: release(shard.lock)
  if shard.pages.hasKey(pageId):
    return shard.pages[pageId].dirty
  return false

proc cacheLoadedCount*(cache: PageCache): int =
  var count = 0
  for shard in cache.shards:
    acquire(shard.lock)
    count += shard.pages.len
    release(shard.lock)
  count

proc clearCache*(pager: Pager) =
  let cache = pager.cache
  for shard in cache.shards:
    acquire(shard.lock)
    shard.pages.clear()
    shard.clock = @[]
    shard.clockHand = 0
    shard.clockTombstones = 0
    release(shard.lock)

proc rollbackCacheLocked*(pager: Pager) =
  ## Evict all dirty pages from the cache.
  ##
  ## Caller must hold `pager.rollbackLock`.
  ## Uses tombstones (PageId 0) for O(1) clock cleanup instead of O(N).
  let cache = pager.cache
  for shard in cache.shards:
    acquire(shard.lock)
    var toRemove: seq[PageId] = @[]
    for id, entry in shard.pages:
      if entry.dirty:
        toRemove.add(id)

    # Build a set for O(1) lookup when scanning clock
    var removeSet = initHashSet[PageId]()
    for id in toRemove:
      removeSet.incl(id)

    # Mark removed pages as tombstones in the clock array
    for i in 0 ..< shard.clock.len:
      if shard.clock[i] in removeSet:
        shard.clock[i] = PageId(0)
        shard.clockTombstones.inc

    # Remove from pages table
    for id in toRemove:
      shard.pages.del(id)

    release(shard.lock)

proc rollbackCache*(pager: Pager) =
  ## Evict all dirty pages from the cache atomically.
  ##
  ## This is used during rollback to ensure the cache does not contain
  ## uncommitted changes. The rollbackLock is held during eviction to
  ## prevent readers from seeing partial dirty state.
  acquire(pager.rollbackLock)
  rollbackCacheLocked(pager)
  release(pager.rollbackLock)

proc isRollbackInProgress*(pager: Pager): bool =
  ## Check if a rollback is currently in progress.
  ## This is a non-blocking check that returns immediately.
  ## Readers should check this before accessing potentially dirty pages.
  acquire(pager.rollbackLock)
  release(pager.rollbackLock)
  false  # If we acquired the lock, rollback is not in progress

proc closePager*(pager: Pager): Result[Void] =
  let flushRes = flushAll(pager)
  if not flushRes.ok:
    return flushRes
  okVoid()

proc appendBlankPage(pager: Pager): Result[PageId] =
  let newId = pager.pageCount + 1
  var data = newString(pager.pageSize)
  let offset = pageOffset(pager, newId)
  let res = pager.vfs.writeStr(pager.file, offset, data)
  if not res.ok:
    return err[PageId](res.err.code, res.err.message, res.err.context)
  if res.value < pager.pageSize:
    return err[PageId](ERR_IO, "Short write on new page", "page_id=" & $newId)
  pager.pageCount = newId
  ok(newId)

proc freelistCapacity(pager: Pager): int =
  (pager.pageSize - 8) div 4

proc readFreelistPage(pager: Pager, pageId: PageId, nextOut: var uint32, countOut: var uint32, idsOut: var seq[uint32]): Result[Void] =
  let pageRes = readPageRo(pager, pageId)
  if not pageRes.ok:
    return err[Void](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  let page = pageRes.value
  nextOut = readU32LE(page, 0)
  countOut = readU32LE(page, 4)
  idsOut = @[]
  let capacity = freelistCapacity(pager)
  let count = min(int(countOut), capacity)
  for i in 0 ..< count:
    let offset = 8 + i * 4
    idsOut.add(readU32LE(page, offset))
  okVoid()

proc writeFreelistPage(pager: Pager, pageId: PageId, next: uint32, ids: seq[uint32]): Result[Void] =
  var buf = newString(pager.pageSize)
  writeU32LE(buf, 0, next)
  writeU32LE(buf, 4, uint32(ids.len))
  for i, id in ids:
    let offset = 8 + i * 4
    writeU32LE(buf, offset, id)
  writePage(pager, pageId, buf)

proc updateHeader(pager: Pager): Result[Void] =
  writeHeader(pager.vfs, pager.file, pager.header)

proc allocatePage*(pager: Pager): Result[PageId] =
  if pager.header.freelistCount == 0 or pager.header.freelistHead == 0:
    let res = appendBlankPage(pager)
    if res.ok and pager.inTransaction:
      pager.txnAllocatedPages.add(res.value)
    return res
  let headId = PageId(pager.header.freelistHead)
  var next: uint32
  var count: uint32
  var ids: seq[uint32]
  let readRes = readFreelistPage(pager, headId, next, count, ids)
  if not readRes.ok:
    return err[PageId](readRes.err.code, readRes.err.message, readRes.err.context)
  if ids.len == 0:
    pager.header.freelistHead = next
    let headerRes = updateHeader(pager)
    if not headerRes.ok:
      return err[PageId](headerRes.err.code, headerRes.err.message, headerRes.err.context)
    let res = appendBlankPage(pager)
    if res.ok and pager.inTransaction:
      pager.txnAllocatedPages.add(res.value)
    return res
  let id = ids[^1]
  ids.setLen(ids.len - 1)
  pager.header.freelistCount = pager.header.freelistCount - 1
  let writeRes = writeFreelistPage(pager, headId, next, ids)
  if not writeRes.ok:
    return err[PageId](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  if ids.len == 0:
    pager.header.freelistHead = next
  let headerRes = updateHeader(pager)
  if not headerRes.ok:
    return err[PageId](headerRes.err.code, headerRes.err.message, headerRes.err.context)
  let pageId = PageId(id)
  if pager.inTransaction:
    pager.txnAllocatedPages.add(pageId)
  ok(pageId)

proc freePage*(pager: Pager, pageId: PageId): Result[Void] =
  if pageId < 2 or pageId > pager.pageCount:
    return err[Void](ERR_IO, "Cannot free page id", "page_id=" & $pageId)
  let capacity = freelistCapacity(pager)
  if pager.header.freelistHead != 0:
    let headId = PageId(pager.header.freelistHead)
    var next: uint32
    var count: uint32
    var ids: seq[uint32]
    let readRes = readFreelistPage(pager, headId, next, count, ids)
    if not readRes.ok:
      return err[Void](readRes.err.code, readRes.err.message, readRes.err.context)
    if ids.len < capacity:
      ids.add(uint32(pageId))
      pager.header.freelistCount = pager.header.freelistCount + 1
      let writeRes = writeFreelistPage(pager, headId, next, ids)
      if not writeRes.ok:
        return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
      return updateHeader(pager)
  let oldHead = pager.header.freelistHead
  let newListPage = appendBlankPage(pager)
  if not newListPage.ok:
    return err[Void](newListPage.err.code, newListPage.err.message, newListPage.err.context)
  let newHead = newListPage.value
  pager.header.freelistHead = uint32(newHead)
  pager.header.freelistCount = pager.header.freelistCount + 1
  let writeRes = writeFreelistPage(pager, newHead, oldHead, @[uint32(pageId)])
  if not writeRes.ok:
    return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  updateHeader(pager)

proc beginTxnPageTracking*(pager: Pager) =
  ## Begin tracking page allocations for the current transaction.
  ## Call this when a transaction begins.
  pager.inTransaction = true
  pager.txnAllocatedPages = @[]

proc endTxnPageTracking*(pager: Pager) =
  ## End tracking page allocations for the current transaction.
  ## Call this when a transaction commits (pages become permanent).
  pager.inTransaction = false
  pager.txnAllocatedPages = @[]

proc rollbackTxnPageAllocations*(pager: Pager): Result[Void] =
  ## Return all pages allocated during the current transaction to the freelist.
  ## Call this when a transaction rolls back to prevent orphaned pages.
  if not pager.inTransaction or pager.txnAllocatedPages.len == 0:
    endTxnPageTracking(pager)
    return okVoid()
  
  # Return each allocated page to the freelist
  for pageId in pager.txnAllocatedPages:
    let freeRes = freePage(pager, pageId)
    if not freeRes.ok:
      # Log error but continue trying to free other pages
      stderr.writeLine("Warning: failed to free page " & $pageId & " during rollback: " & freeRes.err.message)
  
  endTxnPageTracking(pager)
  okVoid()
