import os
import locks
import tables
import options
import sequtils
import ../errors
import ../vfs/types
import ./db_header

type PageId* = uint32
type PageOverlay* = proc(pageId: PageId): Option[seq[byte]]
type ReadGuard* = proc(): Result[Void]

type CacheEntry* = ref object
  id*: PageId
  data*: seq[byte]
  dirty*: bool
  pinCount*: int
  refBit*: bool
  lock*: Lock

type PageCache* = ref object
  capacity*: int
  pages*: Table[PageId, CacheEntry]
  clock*: seq[PageId]
  clockHand*: int
  lock*: Lock

type Pager* = ref object
  vfs*: Vfs
  file*: VfsFile
  header*: DbHeader
  pageSize*: int
  pageCount*: uint32
  cache*: PageCache
  lock*: Lock
  overlay*: PageOverlay
  readGuard*: ReadGuard

proc newPageCache*(capacity: int): PageCache =
  let cap = if capacity <= 0: 1 else: capacity
  let cache = PageCache(capacity: cap, pages: initTable[PageId, CacheEntry](), clock: @[], clockHand: 0)
  initLock(cache.lock)
  cache

proc newPager*(vfs: Vfs, file: VfsFile, cachePages: int = 64): Result[Pager] =
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
  var pager = Pager(vfs: vfs, file: file, header: header, pageSize: pageSize, pageCount: count, cache: cache)
  initLock(pager.lock)
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
  let offset = pageOffset(pager, entry.id)
  let res = pager.vfs.write(pager.file, offset, entry.data)
  if not res.ok:
    return err[Void](res.err.code, res.err.message, res.err.context)
  if res.value < pager.pageSize:
    return err[Void](ERR_IO, "Short write on page", "page_id=" & $entry.id)
  entry.dirty = false
  okVoid()

proc evictIfNeeded(pager: Pager): Result[Void] =
  let cache = pager.cache
  while cache.pages.len >= cache.capacity:
    if cache.clock.len == 0:
      break
    var scanned = 0
    var evicted = false
    var anyUnpinned = false
    while scanned < cache.clock.len * 2 and not evicted:
      if cache.clockHand >= cache.clock.len:
        cache.clockHand = 0
      let pageId = cache.clock[cache.clockHand]
      let entry = cache.pages.getOrDefault(pageId, nil)
      let currentIndex = cache.clockHand
      cache.clockHand.inc
      scanned.inc
      if entry == nil:
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
      cache.pages.del(pageId)
      cache.clock.delete(currentIndex)
      if cache.clockHand > currentIndex:
        cache.clockHand.dec
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
  acquire(cache.lock)
  defer: release(cache.lock)
  if cache.pages.hasKey(pageId):
    let entry = cache.pages[pageId]
    entry.pinCount.inc
    entry.refBit = true
    return ok(entry)
  let evictRes = evictIfNeeded(pager)
  if not evictRes.ok:
    return err[CacheEntry](evictRes.err.code, evictRes.err.message, evictRes.err.context)
  var data = newSeq[byte](pager.pageSize)
  let offset = pageOffset(pager, pageId)
  let readRes = pager.vfs.read(pager.file, offset, data)
  if not readRes.ok:
    return err[CacheEntry](readRes.err.code, readRes.err.message, readRes.err.context)
  if readRes.value < pager.pageSize:
    return err[CacheEntry](ERR_CORRUPTION, "Short read on page", "page_id=" & $pageId)
  let entry = CacheEntry(id: pageId, data: data, dirty: false, pinCount: 1, refBit: true)
  initLock(entry.lock)
  cache.pages[pageId] = entry
  cache.clock.add(pageId)
  ok(entry)

proc unpinPage*(pager: Pager, entry: CacheEntry, dirty: bool = false): Result[Void] =
  let cache = pager.cache
  acquire(cache.lock)
  defer: release(cache.lock)
  if entry.pinCount > 0:
    entry.pinCount.dec
  if dirty:
    entry.dirty = true
  okVoid()

proc readPageDirect*(pager: Pager, pageId: PageId): Result[seq[byte]]

proc readPageCached(pager: Pager, pageId: PageId): Result[seq[byte]] =
  let pinRes = pinPage(pager, pageId)
  if not pinRes.ok:
    return err[seq[byte]](pinRes.err.code, pinRes.err.message, pinRes.err.context)
  let entry = pinRes.value
  acquire(entry.lock)
  var snapshot = newSeq[byte](entry.data.len)
  if entry.data.len > 0:
    copyMem(addr snapshot[0], unsafeAddr entry.data[0], entry.data.len)
  release(entry.lock)
  discard unpinPage(pager, entry)
  ok(snapshot)

proc readPage*(pager: Pager, pageId: PageId): Result[seq[byte]] =
  if pager.readGuard != nil:
    let guardRes = pager.readGuard()
    if not guardRes.ok:
      return err[seq[byte]](guardRes.err.code, guardRes.err.message, guardRes.err.context)
  if pager.overlay != nil:
    let overlayRes = pager.overlay(pageId)
    if overlayRes.isSome:
      return ok(overlayRes.get)
    return readPageCached(pager, pageId)
  readPageCached(pager, pageId)

proc readPageDirect*(pager: Pager, pageId: PageId): Result[seq[byte]] =
  if pager.readGuard != nil:
    let guardRes = pager.readGuard()
    if not guardRes.ok:
      return err[seq[byte]](guardRes.err.code, guardRes.err.message, guardRes.err.context)
  let bound = ensurePageId(pager, pageId)
  if not bound.ok:
    return err[seq[byte]](bound.err.code, bound.err.message, bound.err.context)
  var buf = newSeq[byte](pager.pageSize)
  let offset = pageOffset(pager, pageId)
  let res = pager.vfs.read(pager.file, offset, buf)
  if not res.ok:
    return err[seq[byte]](res.err.code, res.err.message, res.err.context)
  if res.value < pager.pageSize:
    return err[seq[byte]](ERR_IO, "Short read on page", "page_id=" & $pageId)
  ok(buf)

proc setPageOverlay*(pager: Pager, overlay: PageOverlay) =
  pager.overlay = overlay

proc clearPageOverlay*(pager: Pager) =
  pager.overlay = nil

proc setReadGuard*(pager: Pager, guard: ReadGuard) =
  pager.readGuard = guard

proc clearReadGuard*(pager: Pager) =
  pager.readGuard = nil

proc writePage*(pager: Pager, pageId: PageId, data: openArray[byte]): Result[Void] =
  if data.len != pager.pageSize:
    return err[Void](ERR_IO, "Page write size mismatch", "page_id=" & $pageId)
  let pinRes = pinPage(pager, pageId)
  if not pinRes.ok:
    return err[Void](pinRes.err.code, pinRes.err.message, pinRes.err.context)
  let entry = pinRes.value
  acquire(entry.lock)
  entry.data = @data
  release(entry.lock)
  discard unpinPage(pager, entry, dirty = true)
  okVoid()

proc flushAll*(pager: Pager): Result[Void] =
  let cache = pager.cache
  acquire(cache.lock)
  defer: release(cache.lock)
  for _, entry in cache.pages:
    let res = flushEntry(pager, entry)
    if not res.ok:
      return res
  let syncRes = pager.vfs.fsync(pager.file)
  if not syncRes.ok:
    return err[Void](syncRes.err.code, syncRes.err.message, syncRes.err.context)
  okVoid()

proc snapshotDirtyPages*(pager: Pager): seq[(PageId, seq[byte])] =
  let cache = pager.cache
  acquire(cache.lock)
  let entries = cache.pages.values.toSeq()
  release(cache.lock)
  for entry in entries:
    if not entry.dirty:
      continue
    acquire(entry.lock)
    var copy = newSeq[byte](entry.data.len)
    if entry.data.len > 0:
      copyMem(addr copy[0], unsafeAddr entry.data[0], entry.data.len)
    release(entry.lock)
    result.add((entry.id, copy))

proc markPagesClean*(pager: Pager, pageIds: seq[PageId]) =
  let cache = pager.cache
  acquire(cache.lock)
  for pageId in pageIds:
    if cache.pages.hasKey(pageId):
      let entry = cache.pages[pageId]
      entry.dirty = false
  release(cache.lock)

proc clearCache*(pager: Pager) =
  let cache = pager.cache
  acquire(cache.lock)
  cache.pages.clear()
  cache.clock = @[]
  cache.clockHand = 0
  release(cache.lock)

proc closePager*(pager: Pager): Result[Void] =
  let flushRes = flushAll(pager)
  if not flushRes.ok:
    return flushRes
  okVoid()

proc appendBlankPage(pager: Pager): Result[PageId] =
  let newId = pager.pageCount + 1
  var data = newSeq[byte](pager.pageSize)
  let offset = pageOffset(pager, newId)
  let res = pager.vfs.write(pager.file, offset, data)
  if not res.ok:
    return err[PageId](res.err.code, res.err.message, res.err.context)
  if res.value < pager.pageSize:
    return err[PageId](ERR_IO, "Short write on new page", "page_id=" & $newId)
  pager.pageCount = newId
  ok(newId)

proc freelistCapacity(pager: Pager): int =
  (pager.pageSize - 8) div 4

proc readFreelistPage(pager: Pager, pageId: PageId, nextOut: var uint32, countOut: var uint32, idsOut: var seq[uint32]): Result[Void] =
  let pageRes = readPage(pager, pageId)
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
  var buf = newSeq[byte](pager.pageSize)
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
    return appendBlankPage(pager)
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
    return appendBlankPage(pager)
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
  ok(PageId(id))

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
