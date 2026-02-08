import os
import locks
import tables
import sets
import atomics
import options
import times
import ../errors
import ../pager/pager
import ../pager/db_header
import ../vfs/types
import ../utils/perf

type WalFrameType* = enum
  wfPage = 0
  wfCommit = 1
  wfCheckpoint = 2

type WalIndexEntry* = object
  lsn*: uint64
  offset*: int64

type ReadTxn* = object
  id*: int
  snapshot*: uint64
  aborted*: ptr Atomic[bool]  # Atomic flag for lock-free abort check

type WalFailpointKind* = enum
  wfNone
  wfError
  wfPartial

type WalFailpoint* = object
  kind*: WalFailpointKind
  partialBytes*: int
  remaining*: int

type WalPendingPage = object
  pageId: PageId
  bytes: seq[byte]
  str: string
  isString: bool

type ReaderInfo* = object
  ## Extended information about a reader for resource management
  snapshot*: uint64
  started*: float
  lastWarningAt*: float  # When the last warning was issued
  bytesAtStart*: int64   # WAL size when reader started
  abortedFlag*: ptr Atomic[bool]  # Shared atomic flag for lock-free abort check

type
  WalWriter* = ref object
    wal*: Wal
    pending*: seq[WalPendingPage]
    pendingSingle*: WalPendingPage
    hasPendingSingle*: bool
    active*: bool
    flushed*: Table[PageId, WalIndexEntry]
    # Reusable buffer for page metadata during commit (avoids per-commit seq allocation)
    pageMeta*: seq[(PageId, WalIndexEntry)]

  Wal* = ref object
    vfs*: Vfs
    file*: VfsFile
    path*: string
    pageSize*: uint32
    walEnd*: Atomic[uint64]
    endOffset*: int64
    index*: Table[PageId, seq[WalIndexEntry]]
    dirtySinceCheckpoint: Table[PageId, WalIndexEntry]
    lock*: Lock
    indexLock*: Lock
    readerLock*: Lock
    readers*: Table[int, ReaderInfo]
    abortedReaders*: HashSet[int]
    nextReaderId*: int
    failpoints*: Table[string, WalFailpoint]
    checkpointPending*: bool
    lastCheckpointAt*: float
    checkpointEveryBytes*: int64
    checkpointEveryMs*: int64
    readerWarnMs*: int64
    readerTimeoutMs*: int64
    forceTruncateOnTimeout*: bool
    warnings*: seq[string]
    # Memory tracking for WAL index
    indexMemoryBytes*: int64
    checkpointMemoryThreshold*: int64  # Trigger checkpoint when index exceeds this
    # Optimization: Reusable buffer for frame encoding to avoid allocations
    frameBuffer*: seq[byte]
    # Optimization: Optional mmap-backed WAL write path
    mmapEnabled*: bool
    mmapPtr*: pointer
    mmapLen*: int
    # HIGH-006: Long-running reader resource management
    maxWalBytesPerReader*: int64  # Max WAL bytes a reader can pin (0 = disabled)
    readerCheckIntervalMs*: int64  # How often to check readers (0 = check every operation)
    lastReaderCheckAt*: float  # Last time reader check was performed
    totalReadersAborted*: int64  # Stats: total readers aborted due to timeout
    totalWarningsIssued*: int64  # Stats: total warnings issued
    # Optimization: Lazy checkpoint evaluation counters
    commitsSinceCheckpointCheck*: int64  # Commits since last full checkpoint check
    checkpointCheckInterval*: int64      # Check time/memory every N commits (0 = always)
    # Optimization: Cached WalWriter to avoid per-transaction allocation
    cachedWriter*: WalWriter

const HeaderSize = 1 + 4
const TrailerSize = 8
const WalHeaderMagic = "DDBWAL01"
const WalHeaderVersion = 1'u32
const WalHeaderSize* = 32
const WalMmapInitialSize = 1024 * 1024

proc writeU32LEPtr(buf: ptr UncheckedArray[byte], offset: int, value: uint32) =
  buf[offset] = byte(value and 0xFF)
  buf[offset + 1] = byte((value shr 8) and 0xFF)
  buf[offset + 2] = byte((value shr 16) and 0xFF)
  buf[offset + 3] = byte((value shr 24) and 0xFF)

proc writeU64LEPtr(buf: ptr UncheckedArray[byte], offset: int, value: uint64) =
  buf[offset] = byte(value and 0xFF)
  buf[offset + 1] = byte((value shr 8) and 0xFF)
  buf[offset + 2] = byte((value shr 16) and 0xFF)
  buf[offset + 3] = byte((value shr 24) and 0xFF)
  buf[offset + 4] = byte((value shr 32) and 0xFF)
  buf[offset + 5] = byte((value shr 40) and 0xFF)
  buf[offset + 6] = byte((value shr 48) and 0xFF)
  buf[offset + 7] = byte((value shr 56) and 0xFF)

proc encodeWalHeader(buf: var openArray[byte], pageSize: uint32, walEnd: uint64) =
  for i in 0 ..< 8:
    buf[i] = byte(WalHeaderMagic[i])
  writeU32LE(buf, 8, WalHeaderVersion)
  writeU32LE(buf, 12, pageSize)
  writeU64LE(buf, 16, walEnd)
  writeU64LE(buf, 24, 0)

proc encodeWalHeaderPtr(buf: ptr UncheckedArray[byte], pageSize: uint32, walEnd: uint64) =
  for i in 0 ..< 8:
    buf[i] = byte(WalHeaderMagic[i])
  writeU32LEPtr(buf, 8, WalHeaderVersion)
  writeU32LEPtr(buf, 12, pageSize)
  writeU64LEPtr(buf, 16, walEnd)
  writeU64LEPtr(buf, 24, 0)

proc decodeWalHeader(buf: openArray[byte]): Result[(uint32, uint64)] =
  if buf.len < WalHeaderSize:
    return err[(uint32, uint64)](ERR_CORRUPTION, "Short WAL header")
  for i in 0 ..< 8:
    if buf[i] != byte(WalHeaderMagic[i]):
      return err[(uint32, uint64)](ERR_CORRUPTION, "Invalid WAL header magic")
  let version = readU32LE(buf, 8)
  if version != WalHeaderVersion:
    return err[(uint32, uint64)](ERR_CORRUPTION, "Unsupported WAL header version", "version=" & $version)
  let pageSize = readU32LE(buf, 12)
  let walEnd = readU64LE(buf, 16)
  ok((pageSize, walEnd))

proc payloadSizeFor(frameType: WalFrameType, pageSize: uint32): int =
  case frameType
  of wfPage:
    int(pageSize)
  of wfCommit:
    0
  of wfCheckpoint:
    8

proc encodeFrameInto(dest: var seq[byte], offset: int, frameType: WalFrameType, pageId: uint32, payload: openArray[byte]): int =
  let needed = HeaderSize + payload.len + TrailerSize
  if dest.len < offset + needed:
    dest.setLen(max(dest.len * 2, offset + needed))
  
  dest[offset] = byte(frameType)
  writeU32LE(dest, offset + 1, pageId)
  if payload.len > 0:
    copyMem(addr dest[offset + HeaderSize], unsafeAddr payload[0], payload.len)
  
  writeU64LE(dest, offset + HeaderSize + payload.len, 0)
  result = needed

proc encodeFrameIntoString(dest: var seq[byte], offset: int, frameType: WalFrameType, pageId: uint32, payload: string): int =
  let needed = HeaderSize + payload.len + TrailerSize
  if dest.len < offset + needed:
    dest.setLen(max(dest.len * 2, offset + needed))
  
  dest[offset] = byte(frameType)
  writeU32LE(dest, offset + 1, pageId)
  if payload.len > 0:
    copyMem(addr dest[offset + HeaderSize], unsafeAddr payload[0], payload.len)
  
  writeU64LE(dest, offset + HeaderSize + payload.len, 0)
  result = needed

proc encodeFrameIntoPtr(dest: ptr UncheckedArray[byte], offset: int, frameType: WalFrameType, pageId: uint32, payload: openArray[byte]): int =
  let needed = HeaderSize + payload.len + TrailerSize
  dest[offset] = byte(frameType)
  writeU32LEPtr(dest, offset + 1, pageId)
  if payload.len > 0:
    copyMem(addr dest[offset + HeaderSize], unsafeAddr payload[0], payload.len)
  writeU64LEPtr(dest, offset + HeaderSize + payload.len, 0)
  result = needed

proc encodeFrameIntoPtrString(dest: ptr UncheckedArray[byte], offset: int, frameType: WalFrameType, pageId: uint32, payload: string): int =
  let needed = HeaderSize + payload.len + TrailerSize
  dest[offset] = byte(frameType)
  writeU32LEPtr(dest, offset + 1, pageId)
  if payload.len > 0:
    copyMem(addr dest[offset + HeaderSize], unsafeAddr payload[0], payload.len)
  writeU64LEPtr(dest, offset + HeaderSize + payload.len, 0)
  result = needed

proc encodeFrame(frameType: WalFrameType, pageId: uint32, payload: openArray[byte]): seq[byte] =
  result = newSeq[byte](HeaderSize + payload.len + TrailerSize)
  discard encodeFrameInto(result, 0, frameType, pageId, payload)

proc readFrame(vfs: Vfs, file: VfsFile, offset: int64, pageSize: uint32): Result[(WalFrameType, uint32, seq[byte], uint64, int64)] =
  var header = newSeq[byte](HeaderSize)
  let headerRes = vfs.read(file, offset, header)
  if not headerRes.ok:
    return err[(WalFrameType, uint32, seq[byte], uint64, int64)](headerRes.err.code, headerRes.err.message, headerRes.err.context)
  if headerRes.value < HeaderSize:
    return err[(WalFrameType, uint32, seq[byte], uint64, int64)](ERR_IO, "Short header read")
  let rawFrameType = header[0]
  if rawFrameType > 2:
    return err[(WalFrameType, uint32, seq[byte], uint64, int64)](ERR_CORRUPTION, "Invalid frame type", "type=" & $rawFrameType)
  let frameType = WalFrameType(rawFrameType)
  let pageId = readU32LE(header, 1)
  let payloadSize = payloadSizeFor(frameType, pageSize)
  var payload = newSeq[byte](payloadSize)
  if payloadSize > 0:
    let payloadRes = vfs.read(file, offset + HeaderSize, payload)
    if not payloadRes.ok:
      return err[(WalFrameType, uint32, seq[byte], uint64, int64)](payloadRes.err.code, payloadRes.err.message, payloadRes.err.context)
    if payloadRes.value < payloadSize:
      return err[(WalFrameType, uint32, seq[byte], uint64, int64)](ERR_IO, "Short payload read")
  var trailer = newSeq[byte](TrailerSize)
  let trailerRes = vfs.read(file, offset + HeaderSize + payloadSize, trailer)
  if not trailerRes.ok:
    return err[(WalFrameType, uint32, seq[byte], uint64, int64)](trailerRes.err.code, trailerRes.err.message, trailerRes.err.context)
  if trailerRes.value < TrailerSize:
    return err[(WalFrameType, uint32, seq[byte], uint64, int64)](ERR_IO, "Short trailer read")
  let nextOffset = offset + HeaderSize + payloadSize + TrailerSize
  let lsn = uint64(nextOffset)
  ok((frameType, pageId, payload, lsn, nextOffset))

proc newWal*(vfs: Vfs, path: string, pageSize: uint32 = DefaultPageSize): Result[Wal] =
  let fileRes = vfs.open(path, fmReadWrite, true)
  if not fileRes.ok:
    return err[Wal](fileRes.err.code, fileRes.err.message, fileRes.err.context)
  let info = getFileInfo(path)
  var endOffset = int64(0)
  var headerWalEnd: uint64 = 0
  if info.size == 0:
    var header = newSeq[byte](WalHeaderSize)
    encodeWalHeader(header, pageSize, 0)
    let writeRes = vfs.write(fileRes.value, 0, header)
    if not writeRes.ok:
      return err[Wal](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    let truncRes = vfs.truncate(fileRes.value, WalHeaderSize)
    if not truncRes.ok:
      return err[Wal](truncRes.err.code, truncRes.err.message, truncRes.err.context)
    endOffset = WalHeaderSize
  else:
    if info.size < WalHeaderSize:
      return err[Wal](ERR_CORRUPTION, "WAL header missing", path)
    var header = newSeq[byte](WalHeaderSize)
    let readRes = vfs.read(fileRes.value, 0, header)
    if not readRes.ok:
      return err[Wal](readRes.err.code, readRes.err.message, readRes.err.context)
    if readRes.value < WalHeaderSize:
      return err[Wal](ERR_CORRUPTION, "Short WAL header read", path)
    let headerRes = decodeWalHeader(header)
    if not headerRes.ok:
      return err[Wal](headerRes.err.code, headerRes.err.message, headerRes.err.context)
    let (headerPageSize, walEnd) = headerRes.value
    if headerPageSize != pageSize:
      return err[Wal](ERR_CORRUPTION, "WAL page size mismatch", "wal=" & $headerPageSize & " db=" & $pageSize)
    if walEnd != 0 and walEnd < uint64(WalHeaderSize):
      return err[Wal](ERR_CORRUPTION, "Invalid WAL end offset", "wal_end=" & $walEnd)
    if walEnd > uint64(info.size):
      return err[Wal](ERR_CORRUPTION, "WAL end exceeds file size", "wal_end=" & $walEnd & " size=" & $info.size)
    headerWalEnd = walEnd
    endOffset = max(int64(walEnd), int64(WalHeaderSize))
  
  let wal = Wal(
    vfs: vfs,
    file: fileRes.value,
    path: path,
    pageSize: pageSize,
    endOffset: endOffset,
    index: initTable[PageId, seq[WalIndexEntry]](),
    dirtySinceCheckpoint: initTable[PageId, WalIndexEntry](),
    readers: initTable[int, ReaderInfo](),
    abortedReaders: initHashSet[int](),
    failpoints: initTable[string, WalFailpoint](),
    warnings: @[],
    # Optimization: Pre-allocate buffer (4KB * 4 + overhead)
    frameBuffer: newSeq[byte](16384),
    mmapEnabled: vfs.supportsMmap(),
    mmapPtr: nil,
    mmapLen: 0,
    lastCheckpointAt: epochTime(),
    lastReaderCheckAt: epochTime(),
    # HIGH-006: Disabled by default (zero-cost when not configured)
    maxWalBytesPerReader: 0,
    readerCheckIntervalMs: 0,
    totalReadersAborted: 0,
    totalWarningsIssued: 0
  )
  initLock(wal.lock)
  initLock(wal.indexLock)
  initLock(wal.readerLock)
  wal.walEnd.store(headerWalEnd, moRelaxed)
  ok(wal)

proc setFailpoint*(wal: Wal, label: string, fp: WalFailpoint) =
  wal.failpoints[label] = fp

proc clearFailpoints*(wal: Wal) =
  wal.failpoints.clear()

proc recordWarningLocked(wal: Wal, message: string) =
  wal.warnings.add(message)

proc takeWarnings*(wal: Wal): seq[string] =
  acquire(wal.lock)
  result = wal.warnings
  wal.warnings = @[]
  release(wal.lock)

proc applyFailpoint(wal: Wal, label: string, length: int): Result[int] =
  if not wal.failpoints.hasKey(label):
    return ok(length)
  var fp = wal.failpoints[label]
  if fp.remaining > 0:
    fp.remaining.dec
    if fp.remaining == 0:
      wal.failpoints.del(label)
    else:
      wal.failpoints[label] = fp
  case fp.kind
  of wfNone:
    ok(length)
  of wfError:
    err[int](ERR_IO, "Injected WAL failpoint", label)
  of wfPartial:
    let part = min(fp.partialBytes, length)
    ok(part)

proc unmapWalIfMapped*(wal: Wal) =
  if wal.mmapPtr == nil or wal.mmapLen <= 0:
    return
  discard wal.vfs.unmap(MmapRegion(base: wal.mmapPtr, len: wal.mmapLen))
  wal.mmapPtr = nil
  wal.mmapLen = 0

proc ensureWalMmapCapacity(wal: Wal, requiredEnd: int64): Result[bool] =
  if not wal.mmapEnabled:
    return ok(false)
  if not wal.vfs.supportsMmap():
    wal.mmapEnabled = false
    return ok(false)
  if requiredEnd <= 0:
    return ok(false)

  var fileSize = if wal.mmapLen > 0: int64(wal.mmapLen) else: getFileInfo(wal.path).size
  if fileSize < WalHeaderSize:
    fileSize = WalHeaderSize

  if requiredEnd > fileSize:
    var targetSize = max(requiredEnd, int64(WalMmapInitialSize))
    if wal.mmapLen > 0:
      targetSize = max(targetSize, int64(wal.mmapLen) * 2)
    let truncRes = wal.vfs.truncate(wal.file, targetSize)
    if not truncRes.ok:
      return err[bool](truncRes.err.code, truncRes.err.message, truncRes.err.context)
    fileSize = targetSize

  if wal.mmapPtr != nil and wal.mmapLen >= int(fileSize):
    return ok(true)

  if wal.mmapPtr != nil:
    unmapWalIfMapped(wal)

  let mapRes = wal.vfs.mapWritable(wal.file, fileSize)
  if not mapRes.ok:
    wal.mmapEnabled = false
    return ok(false)
  wal.mmapPtr = mapRes.value.base
  wal.mmapLen = mapRes.value.len
  ok(true)

proc writeWalHeader(wal: Wal, walEnd: uint64): Result[Void] =
  if wal.mmapPtr != nil:
    let dest = cast[ptr UncheckedArray[byte]](wal.mmapPtr)
    encodeWalHeaderPtr(dest, wal.pageSize, walEnd)
    return okVoid()
  var header = newSeq[byte](WalHeaderSize)
  encodeWalHeader(header, wal.pageSize, walEnd)
  let writeRes = wal.vfs.write(wal.file, 0, header)
  if not writeRes.ok:
    return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  if writeRes.value < WalHeaderSize:
    return err[Void](ERR_IO, "Short WAL header write", wal.path)
  okVoid()

proc appendFrame(wal: Wal, frameType: WalFrameType, pageId: uint32, payload: seq[byte]): Result[(uint64, int64)] =
  perf.WalGrowthWriter.inc()
  let frame = encodeFrame(frameType, pageId, payload)
  let offset = wal.endOffset
  let fpRes = applyFailpoint(wal, "wal_write_frame", frame.len)
  if not fpRes.ok:
    return err[(uint64, int64)](fpRes.err.code, fpRes.err.message, fpRes.err.context)
  let writeLen = fpRes.value
  let writeRes = wal.vfs.write(wal.file, offset, frame[0 ..< writeLen])
  if not writeRes.ok:
    return err[(uint64, int64)](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  if writeLen < frame.len:
    return err[(uint64, int64)](ERR_IO, "Partial frame write", "wal_write_frame")
  wal.endOffset += int64(frame.len)
  ok((uint64(wal.endOffset), offset))

proc encodeCheckpointPayload(lsn: uint64): seq[byte] =
  result = newSeq[byte](8)
  writeU64LE(result, 0, lsn)

proc readersOverThreshold*(wal: Wal, thresholdMs: int64): seq[tuple[id: int, snapshot: uint64, ageMs: int64]] =
  ## Find all readers that have been running longer than thresholdMs.
  ## This is used for warning and timeout detection.
  if thresholdMs <= 0:
    return @[]
  let now = epochTime()
  acquire(wal.readerLock)
  for id, info in wal.readers:
    let elapsedMs = int64((now - info.started) * 1000)
    if elapsedMs >= thresholdMs:
      result.add((id: id, snapshot: info.snapshot, ageMs: elapsedMs))
  release(wal.readerLock)

proc readersExceedingWalLimit*(wal: Wal): seq[tuple[id: int, snapshot: uint64, bytesPinned: int64, ageMs: int64]] =
  ## HIGH-006: Find readers that are pinning too much WAL data.
  ## Returns readers where (current WAL size - bytes at start) > maxWalBytesPerReader.
  ## Zero-cost when maxWalBytesPerReader is 0 (disabled).
  if wal.maxWalBytesPerReader <= 0:
    return @[]
  
  let now = epochTime()
  let currentWalSize = wal.endOffset
  
  acquire(wal.readerLock)
  defer: release(wal.readerLock)
  
  for id, info in wal.readers:
    let bytesPinned = currentWalSize - info.bytesAtStart
    if bytesPinned > wal.maxWalBytesPerReader:
      let ageMs = int64((now - info.started) * 1000)
      result.add((id: id, snapshot: info.snapshot, bytesPinned: bytesPinned, ageMs: ageMs))

proc readerWalSize*(wal: Wal, readerId: int): int64 =
  ## Calculate the amount of WAL data pinned by a specific reader.
  ## Returns 0 if reader doesn't exist or reader has been cleaned up.
  acquire(wal.readerLock)
  defer: release(wal.readerLock)
  
  if not wal.readers.hasKey(readerId):
    return 0
  
  let info = wal.readers[readerId]
  let currentWalSize = wal.endOffset
  result = currentWalSize - info.bytesAtStart
  if result < 0:
    result = 0  # Shouldn't happen, but be safe

proc getReaderStats*(wal: Wal): tuple[activeReaders: int, oldestReaderAgeMs: int64, totalWalPinned: int64, totalAborted: int64, totalWarnings: int64] =
  ## Get statistics about current readers and WAL retention.
  ## This helps monitor WAL growth and reader behavior.
  acquire(wal.readerLock)
  defer: release(wal.readerLock)
  
  let now = epochTime()
  var oldestStart = now
  var totalPinned: int64 = 0
  let currentWalSize = wal.endOffset
  
  for id, info in wal.readers:
    if info.started < oldestStart:
      oldestStart = info.started
    let pinned = currentWalSize - info.bytesAtStart
    if pinned > 0:
      totalPinned += pinned
  
  let oldestAgeMs = if wal.readers.len > 0: int64((now - oldestStart) * 1000) else: 0
  
  result = (
    activeReaders: wal.readers.len,
    oldestReaderAgeMs: oldestAgeMs,
    totalWalPinned: totalPinned,
    totalAborted: wal.totalReadersAborted,
    totalWarnings: wal.totalWarningsIssued
  )

proc minReaderSnapshot*(wal: Wal): Option[uint64] =
  acquire(wal.readerLock)
  var minSnap: uint64 = 0
  var has = false
  for _, info in wal.readers:
    if not has or info.snapshot < minSnap:
      minSnap = info.snapshot
      has = true
  release(wal.readerLock)
  if has: some(minSnap) else: none(uint64)

proc shouldCheckReaders*(wal: Wal): bool =
  ## HIGH-006: Determine if we should perform reader checks now.
  ## Zero-cost check when readerCheckIntervalMs is 0 (always check) or features disabled.
  if wal.readerWarnMs <= 0 and wal.readerTimeoutMs <= 0 and wal.maxWalBytesPerReader <= 0:
    return false
  if wal.readerCheckIntervalMs <= 0:
    return true
  let now = epochTime()
  let elapsedMs = int64((now - wal.lastReaderCheckAt) * 1000)
  return elapsedMs >= wal.readerCheckIntervalMs

proc checkpoint*(wal: Wal, pager: Pager): Result[uint64] =
  let start = epochTime()
  defer:
    let duration = int64((epochTime() - start) * 1_000_000)
    perf.CheckpointDurationUs.add(duration)

  # Phase 1: Acquire lock, determine what to checkpoint, then release lock
  acquire(wal.lock)
  wal.checkpointPending = true
  let lastCommit = wal.walEnd.load(moAcquire)
  if lastCommit == 0:
    wal.checkpointPending = false
    release(wal.lock)
    wal.lastCheckpointAt = epochTime()
    return ok(lastCommit)
  
  # HIGH-006: Check if it's time to check readers
  let shouldCheck = wal.shouldCheckReaders()
  if shouldCheck:
    wal.lastReaderCheckAt = epochTime()
  
  # Check for long-running readers (warnings)
  if shouldCheck and wal.readerWarnMs > 0:
    let warningReaders = wal.readersOverThreshold(wal.readerWarnMs)
    acquire(wal.readerLock)
    for info in warningReaders:
      if wal.readers.hasKey(info.id):
        let readerInfo = wal.readers[info.id]
        # Only warn once per reader (or at intervals)
        let timeSinceLastWarning = epochTime() - readerInfo.lastWarningAt
        if readerInfo.lastWarningAt == 0.0 or timeSinceLastWarning >= 60.0:  # Warn at most once per minute
          wal.readers[info.id].lastWarningAt = epochTime()
          wal.totalWarningsIssued.inc
          let bytesPinned = wal.endOffset - readerInfo.bytesAtStart
          wal.recordWarningLocked("Long-running reader id=" & $info.id & 
            " snapshot=" & $info.snapshot & " age_ms=" & $info.ageMs & 
            " wal_pinned_bytes=" & $bytesPinned)
    release(wal.readerLock)
  
  # Check for timed-out readers (abort)
  if shouldCheck and wal.readerTimeoutMs > 0:
    let timedOut = wal.readersOverThreshold(wal.readerTimeoutMs)
    if timedOut.len > 0:
      acquire(wal.readerLock)
      for info in timedOut:
        wal.abortedReaders.incl(info.id)
        if wal.readers.hasKey(info.id):
          let readerInfo = wal.readers[info.id]
          let bytesPinned = wal.endOffset - readerInfo.bytesAtStart
          # Set atomic abort flag for lock-free checking
          if readerInfo.abortedFlag != nil:
            readerInfo.abortedFlag[].store(true, moRelease)
          wal.readers.del(info.id)
          wal.totalReadersAborted.inc
          wal.recordWarningLocked("Reader timeout id=" & $info.id & 
            " snapshot=" & $info.snapshot & " age_ms=" & $info.ageMs &
            " wal_pinned_bytes=" & $bytesPinned)
      release(wal.readerLock)
  
  # HIGH-006: Check for readers exceeding WAL size limit
  if shouldCheck and wal.maxWalBytesPerReader > 0:
    let oversizedReaders = wal.readersExceedingWalLimit()
    if oversizedReaders.len > 0:
      acquire(wal.readerLock)
      for info in oversizedReaders:
        wal.abortedReaders.incl(info.id)
        if wal.readers.hasKey(info.id):
          # Set atomic abort flag for lock-free checking
          if wal.readers[info.id].abortedFlag != nil:
            wal.readers[info.id].abortedFlag[].store(true, moRelease)
          wal.readers.del(info.id)
          wal.totalReadersAborted.inc
          wal.recordWarningLocked("Reader WAL limit exceeded id=" & $info.id &
            " snapshot=" & $info.snapshot & " wal_pinned_bytes=" & $info.bytesPinned &
            " limit=" & $wal.maxWalBytesPerReader)
      release(wal.readerLock)
  
  let minSnap = wal.minReaderSnapshot()
  let safeLsn =
    if minSnap.isNone:
      lastCommit
    else:
      min(minSnap.get, lastCommit)
  var toCheckpoint: seq[(PageId, int64)] = @[]
  if safeLsn == lastCommit:
    acquire(wal.indexLock)
    for pageId, entry in wal.dirtySinceCheckpoint.pairs:
      if entry.lsn <= safeLsn:
        toCheckpoint.add((pageId, entry.offset))
    release(wal.indexLock)
  else:
    acquire(wal.indexLock)
    # Slow path: active readers pin an earlier safe LSN. Avoid scanning the full
    # WAL index by only considering pages that have changed since the last
    # checkpoint, then selecting the best version <= safeLsn for each.
    for pageId, _ in wal.dirtySinceCheckpoint.pairs:
      if not wal.index.hasKey(pageId):
        continue
      let entries = wal.index[pageId]
      var bestLsn: uint64 = 0
      var bestOffset: int64 = -1
      for entry in entries:
        if entry.lsn <= safeLsn and entry.lsn >= bestLsn:
          bestLsn = entry.lsn
          bestOffset = entry.offset
      if bestLsn != 0 and bestOffset >= 0:
        toCheckpoint.add((pageId, bestOffset))
    release(wal.indexLock)
  
  # Release the main WAL lock to allow writers to proceed during I/O
  release(wal.lock)

  # Phase 2: Perform I/O operations without holding the main lock
  for entry in toCheckpoint:
    let frameRes = readFrame(wal.vfs, wal.file, entry[1], wal.pageSize)
    if not frameRes.ok:
      acquire(wal.lock)
      wal.checkpointPending = false
      release(wal.lock)
      return err[uint64](frameRes.err.code, frameRes.err.message, frameRes.err.context)
    let (frameType, framePageId, bestPayload, _, _) = frameRes.value
    if frameType != wfPage or framePageId != uint32(entry[0]):
      acquire(wal.lock)
      wal.checkpointPending = false
      release(wal.lock)
      return err[uint64](ERR_CORRUPTION, "Checkpoint frame mismatch", "page_id=" & $entry[0])
    let failRes = applyFailpoint(wal, "checkpoint_write_page", bestPayload.len)
    if not failRes.ok:
      acquire(wal.lock)
      wal.checkpointPending = false
      release(wal.lock)
      return err[uint64](failRes.err.code, failRes.err.message, failRes.err.context)
    var payloadStr = newString(bestPayload.len)
    if bestPayload.len > 0:
      copyMem(addr payloadStr[0], unsafeAddr bestPayload[0], bestPayload.len)
    let writeRes = writePageDirectFile(pager, entry[0], payloadStr)
    if not writeRes.ok:
      acquire(wal.lock)
      wal.checkpointPending = false
      release(wal.lock)
      return err[uint64](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    # Ensure readers don't see stale pages in cache now that DB file is updated
    pager.invalidatePage(entry[0])
  let fsyncFail = applyFailpoint(wal, "checkpoint_fsync", 0)
  if not fsyncFail.ok:
    acquire(wal.lock)
    wal.checkpointPending = false
    release(wal.lock)
    return err[uint64](fsyncFail.err.code, fsyncFail.err.message, fsyncFail.err.context)
  pager.header.lastCheckpointLsn = safeLsn
  let headerRes = writeHeader(pager.vfs, pager.file, pager.header)
  if not headerRes.ok:
    acquire(wal.lock)
    wal.checkpointPending = false
    release(wal.lock)
    return err[uint64](headerRes.err.code, headerRes.err.message, headerRes.err.context)
  let syncRes = pager.vfs.fsync(pager.file)
  if not syncRes.ok:
    acquire(wal.lock)
    wal.checkpointPending = false
    release(wal.lock)
    return err[uint64](syncRes.err.code, syncRes.err.message, syncRes.err.context)
  
  # Phase 3: Re-acquire lock to finalize checkpoint state
  acquire(wal.lock)
  let chkRes = appendFrame(wal, wfCheckpoint, 0, encodeCheckpointPayload(safeLsn))
  if not chkRes.ok:
    wal.checkpointPending = false
    release(wal.lock)
    return err[uint64](chkRes.err.code, chkRes.err.message, chkRes.err.context)
  let walSyncFail = applyFailpoint(wal, "checkpoint_wal_fsync", 0)
  if not walSyncFail.ok:
    wal.checkpointPending = false
    release(wal.lock)
    return err[uint64](walSyncFail.err.code, walSyncFail.err.message, walSyncFail.err.context)
  release(wal.lock)
  
  let walSync = wal.vfs.fsync(wal.file)
  if not walSync.ok:
    acquire(wal.lock)
    wal.checkpointPending = false
    release(wal.lock)
    return err[uint64](walSync.err.code, walSync.err.message, walSync.err.context)
  
  acquire(wal.lock)
  wal.lastCheckpointAt = epochTime()
  
  # Check if new commits occurred during checkpoint I/O phase
  # If so, we cannot truncate the WAL or clear the index completely
  let currentEnd = wal.walEnd.load(moAcquire)
  let hadNewCommits = currentEnd > lastCommit
  
  # Re-check active readers (in case new ones started during I/O)
  let currentMinSnap = wal.minReaderSnapshot()
  
  # Only truncate if:
  # 1. No active readers (currentMinSnap.isNone) OR all readers are at or past lastCommit
  # 2. AND no new commits occurred during checkpoint
  let canTruncate = (currentMinSnap.isNone or currentMinSnap.get >= lastCommit) and not hadNewCommits
  
  if canTruncate:
    release(wal.lock)
    let truncRes = wal.vfs.truncate(wal.file, WalHeaderSize)
    if not truncRes.ok:
      acquire(wal.lock)
      wal.checkpointPending = false
      release(wal.lock)
      return err[uint64](truncRes.err.code, truncRes.err.message, truncRes.err.context)
    let headerRes = writeWalHeader(wal, 0)
    if not headerRes.ok:
      acquire(wal.lock)
      wal.checkpointPending = false
      release(wal.lock)
      return err[uint64](headerRes.err.code, headerRes.err.message, headerRes.err.context)
    unmapWalIfMapped(wal)
    acquire(wal.lock)
    acquire(wal.indexLock)
    wal.index.clear()
    wal.dirtySinceCheckpoint.clear()
    release(wal.indexLock)
    wal.endOffset = WalHeaderSize
    wal.walEnd.store(0, moRelease)
  elif hadNewCommits:
    # New commits occurred during checkpoint - cannot truncate
    # Keep the index entries for pages with LSN > safeLsn
    acquire(wal.indexLock)
    # Remove only entries with LSN <= safeLsn from dirtySinceCheckpoint
    # but keep entries with LSN > safeLsn (newer commits during checkpoint)
    var toRemove: seq[PageId] = @[]
    for pageId, entry in wal.dirtySinceCheckpoint.pairs:
      if entry.lsn <= safeLsn:
        toRemove.add(pageId)
    for pageId in toRemove:
      wal.dirtySinceCheckpoint.del(pageId)

    # Prune WAL index entries that are now checkpointed (<= safeLsn) for pages
    # written to the main DB file during this checkpoint. This bounds index
    # growth when checkpoints overlap with new commits.
    for entry in toCheckpoint:
      let pageId = entry[0]
      if not wal.index.hasKey(pageId):
        continue
      let entries = wal.index[pageId]
      var cut = 0
      while cut < entries.len and entries[cut].lsn <= safeLsn:
        cut.inc
      if cut <= 0:
        continue
      if cut >= entries.len:
        wal.index.del(pageId)
      else:
        wal.index[pageId] = entries[cut .. ^1]

    # Note: wal.index is not cleared - it still contains entries for newer commits
    release(wal.indexLock)
  wal.checkpointPending = false
  release(wal.lock)
  ok(safeLsn)

# Forward declaration
proc estimateIndexMemoryUsage*(wal: Wal): int64

proc maybeCheckpoint*(wal: Wal, pager: Pager): Result[bool] =
  var trigger = false
  
  # Fast path: Check bytes threshold first (cheap)
  if wal.checkpointEveryBytes > 0 and wal.endOffset >= wal.checkpointEveryBytes:
    trigger = true
  
  # Check time/memory thresholds
  if not trigger and (wal.checkpointEveryMs > 0 or wal.checkpointMemoryThreshold > 0):
    var shouldCheckTimeMemory = true
    
    # Lazy evaluation: Only check time/memory every N commits (if interval configured)
    if wal.checkpointCheckInterval > 0:
      wal.commitsSinceCheckpointCheck.inc
      if wal.commitsSinceCheckpointCheck < wal.checkpointCheckInterval:
        shouldCheckTimeMemory = false
      else:
        wal.commitsSinceCheckpointCheck = 0
    
    if shouldCheckTimeMemory:
      if wal.checkpointEveryMs > 0:
        let elapsedMs = int64((epochTime() - wal.lastCheckpointAt) * 1000)
        if elapsedMs >= wal.checkpointEveryMs:
          trigger = true
      
      if not trigger and wal.checkpointMemoryThreshold > 0:
        let memUsage = estimateIndexMemoryUsage(wal)
        if memUsage >= wal.checkpointMemoryThreshold:
          trigger = true
  
  if not trigger:
    return ok(false)
  
  # Reset counter after triggering
  wal.commitsSinceCheckpointCheck = 0
  
  let chkRes = checkpoint(wal, pager)
  if not chkRes.ok:
    return err[bool](chkRes.err.code, chkRes.err.message, chkRes.err.context)
  ok(true)

proc recover*(wal: Wal): Result[Void] =
  ## Recover WAL state after restart/crash.
  ## 
  ## Processes all frames to rebuild the index and validate invariants.
  ## Checkpoint frames are tracked to establish the safe recovery point.
  wal.index.clear()
  wal.dirtySinceCheckpoint.clear()
  var pending: seq[(PageId, uint64, int64)] = @[]
  var lastCommit: uint64 = 0
  var lastCheckpointLsn: uint64 = 0
  let headerEnd = wal.walEnd.load(moAcquire)
  if headerEnd == 0:
    wal.endOffset = WalHeaderSize
    return okVoid()
  if headerEnd < uint64(WalHeaderSize):
    return err[Void](ERR_CORRUPTION, "Invalid WAL end offset", "wal_end=" & $headerEnd)
  var offset: int64 = WalHeaderSize
  var frameCount: int = 0
  var commitCount: int = 0
  var checkpointCount: int = 0
  
  while offset < int64(headerEnd):
    let frameOffset = offset
    let frameRes = readFrame(wal.vfs, wal.file, frameOffset, wal.pageSize)
    if not frameRes.ok:
      break
    let (frameType, pageId, payload, lsn, nextOffset) = frameRes.value
    frameCount.inc
    
    case frameType
    of wfPage:
      # Validate page frame invariants
      if pageId == 0:
        return err[Void](ERR_CORRUPTION, "Invalid page ID in WAL frame", "offset=" & $frameOffset)
      pending.add((PageId(pageId), lsn, frameOffset))
      
    of wfCommit:
      commitCount.inc
      var commitMeta: seq[(PageId, WalIndexEntry)] = @[]
      for entry in pending:
        if not wal.index.hasKey(entry[0]):
          wal.index[entry[0]] = @[]
        wal.index[entry[0]].add(WalIndexEntry(lsn: entry[1], offset: entry[2]))
        commitMeta.add((entry[0], WalIndexEntry(lsn: entry[1], offset: entry[2])))
        if entry[1] > lastCommit:
          lastCommit = entry[1]
      for m in commitMeta:
        wal.dirtySinceCheckpoint[m[0]] = m[1]
      pending = @[]
      if lsn > lastCommit:
        lastCommit = lsn
        
    of wfCheckpoint:
      checkpointCount.inc
      # Decode checkpoint payload to get the safe LSN
      if payload.len >= 8:
        let chkLsn = readU64LE(payload, 0)
        if chkLsn > lastCheckpointLsn:
          lastCheckpointLsn = chkLsn
      # Discard any pending frames before this checkpoint
      # They have been safely written to the main database file
      pending = @[]
      
    offset = nextOffset
  
  # Validation: Ensure LSN ordering
  if lastCheckpointLsn > lastCommit:
    return err[Void](ERR_CORRUPTION, "Checkpoint LSN exceeds commit LSN", 
                    "checkpoint=" & $lastCheckpointLsn & " commit=" & $lastCommit)
  
  # Warn about incomplete transactions (pages without commits)
  if pending.len > 0:
    # These are uncommitted changes - they will be discarded
    stderr.writeLine("Warning: " & $pending.len & " uncommitted WAL frames discarded during recovery")
  
  wal.endOffset = max(offset, int64(WalHeaderSize))
  wal.walEnd.store(lastCommit, moRelease)
  
  # Validation: Ensure we have a consistent state
  if wal.index.len > 0 and lastCommit == 0:
    return err[Void](ERR_CORRUPTION, "WAL index non-empty but no commits found")
  
  # Log recovery summary
  when defined(decentdbDebugLogging):
    stderr.writeLine("WAL recovery complete: " & $frameCount & " frames, " & 
                     $commitCount & " commits, " & $checkpointCount & " checkpoints")
  
  okVoid()

proc beginRead*(wal: Wal): ReadTxn =
  ## Begin a read transaction.
  ## Tracks reader start time and WAL size for resource management (HIGH-006).
  let snapshot = wal.walEnd.load(moAcquire)
  acquire(wal.readerLock)
  let readerId = wal.nextReaderId
  wal.nextReaderId.inc
  let now = epochTime()
  # Allocate atomic flag for lock-free abort checking
  let abortFlag = cast[ptr Atomic[bool]](alloc0(sizeof(Atomic[bool])))
  abortFlag[].store(false, moRelaxed)
  wal.readers[readerId] = ReaderInfo(
    snapshot: snapshot, 
    started: now,
    lastWarningAt: 0.0,
    bytesAtStart: wal.endOffset,
    abortedFlag: abortFlag
  )
  wal.abortedReaders.excl(readerId)
  release(wal.readerLock)
  ReadTxn(id: readerId, snapshot: snapshot, aborted: abortFlag)

proc endRead*(wal: Wal, txn: ReadTxn) =
  acquire(wal.readerLock)
  if wal.readers.hasKey(txn.id):
    # Free the atomic flag
    if wal.readers[txn.id].abortedFlag != nil:
      dealloc(wal.readers[txn.id].abortedFlag)
    wal.readers.del(txn.id)
  wal.abortedReaders.excl(txn.id)
  release(wal.readerLock)

proc readerCount*(wal: Wal): int =
  acquire(wal.readerLock)
  let count = wal.readers.len
  release(wal.readerLock)
  count

proc isAborted*(wal: Wal, txn: ReadTxn): bool =
  ## Check if a read transaction has been aborted.
  ## Uses atomic flag for lock-free hot-path checking.
  if txn.aborted != nil:
    return txn.aborted[].load(moAcquire)
  # Fallback for legacy txns without atomic flag
  acquire(wal.readerLock)
  result = txn.id in wal.abortedReaders
  release(wal.readerLock)

proc setCheckpointConfig*(wal: Wal, everyBytes: int64, everyMs: int64, readerWarnMs: int64 = 0, readerTimeoutMs: int64 = 0, forceTruncateOnTimeout: bool = false, memoryThreshold: int64 = 0, maxWalBytesPerReader: int64 = 0, readerCheckIntervalMs: int64 = 0, checkpointCheckInterval: int64 = 0) =
  ## Configure checkpoint and reader management settings.
  ## 
  ## HIGH-006 parameters:
  ## - maxWalBytesPerReader: Maximum WAL bytes a single reader can pin (0 = disabled)
  ## - readerCheckIntervalMs: Minimum time between reader checks (0 = check every operation)
  ## - checkpointCheckInterval: Only evaluate time/memory thresholds every N commits (0 = always)
  wal.checkpointEveryBytes = everyBytes
  wal.checkpointEveryMs = everyMs
  wal.readerWarnMs = readerWarnMs
  wal.readerTimeoutMs = readerTimeoutMs
  wal.forceTruncateOnTimeout = forceTruncateOnTimeout
  wal.checkpointMemoryThreshold = memoryThreshold
  # HIGH-006: Long-running reader resource management
  wal.maxWalBytesPerReader = maxWalBytesPerReader
  wal.readerCheckIntervalMs = readerCheckIntervalMs
  wal.checkpointCheckInterval = checkpointCheckInterval

proc estimateIndexMemoryUsage*(wal: Wal): int64 =
  ## Estimate memory usage of the WAL index in bytes.
  ## This includes the index table and dirtySinceCheckpoint table.
  acquire(wal.indexLock)
  defer: release(wal.indexLock)
  
  var totalBytes: int64 = 0
  # Account for table overhead (approximately)
  totalBytes += int64(wal.index.len) * (sizeof(PageId) + sizeof(pointer) * 2)
  totalBytes += int64(wal.dirtySinceCheckpoint.len) * (sizeof(PageId) + sizeof(WalIndexEntry) + sizeof(pointer) * 2)
  
  # Account for entry sequences
  for pageId, entries in wal.index:
    totalBytes += int64(entries.len) * sizeof(WalIndexEntry)
    totalBytes += sizeof(seq[int])  # seq overhead
  
  wal.indexMemoryBytes = totalBytes
  totalBytes

proc findBestEntryBinarySearch(entries: seq[WalIndexEntry], snapshot: uint64): Option[tuple[lsn: uint64, offset: int64]] =
  ## Binary search to find the entry with the largest LSN <= snapshot.
  ## Entries are sorted by LSN in ascending order.
  ## Returns none if no entry satisfies the condition.
  if entries.len == 0:
    return none(tuple[lsn: uint64, offset: int64])
  
  # Check if all entries are too new
  if entries[0].lsn > snapshot:
    return none(tuple[lsn: uint64, offset: int64])
  
  # Binary search for the rightmost entry <= snapshot
  var lo = 0
  var hi = entries.len - 1
  var bestIdx = -1
  
  while lo <= hi:
    let mid = (lo + hi) shr 1
    if entries[mid].lsn <= snapshot:
      bestIdx = mid
      lo = mid + 1
    else:
      hi = mid - 1
  
  if bestIdx < 0:
    return none(tuple[lsn: uint64, offset: int64])
  
  let best = entries[bestIdx]
  some((lsn: best.lsn, offset: best.offset))

proc getPageAtOrBefore*(wal: Wal, pageId: PageId, snapshot: uint64): Option[seq[byte]] =
  acquire(wal.indexLock)
  defer:
    release(wal.indexLock)
  if not wal.index.hasKey(pageId):
    return none(seq[byte])

  let entries = wal.index[pageId]
  let bestEntryOpt = findBestEntryBinarySearch(entries, snapshot)
  
  if bestEntryOpt.isNone:
    return none(seq[byte])
  
  let bestOffset = bestEntryOpt.get.offset
  let frameRes = readFrame(wal.vfs, wal.file, bestOffset, wal.pageSize)
  if not frameRes.ok:
    return none(seq[byte])
  let (frameType, framePageId, payload, _, _) = frameRes.value
  if frameType != wfPage or framePageId != uint32(pageId):
    return none(seq[byte])
  some(payload)

proc readFramePayload*(wal: Wal, offset: int64): Option[string] =
  ## Read just the payload of a frame at a known offset.
  ## Used for reading flushed-but-uncommitted pages.
  let res = readFrame(wal.vfs, wal.file, offset, wal.pageSize)
  if not res.ok:
    return none(string)
  let (_, _, payload, _, _) = res.value
  var s = newString(payload.len)
  if payload.len > 0:
    copyMem(addr s[0], unsafeAddr payload[0], payload.len)
  some(s)

proc readPageWithSnapshot*(pager: Pager, wal: Wal, snapshot: uint64, pageId: PageId, readerId: int = -1): Result[string] =
  # Check if this reader has been aborted (timeout)
  if readerId >= 0:
    acquire(wal.readerLock)
    let isAborted = readerId in wal.abortedReaders
    release(wal.readerLock)
    if isAborted:
      return err[string](ERR_TRANSACTION, "Read transaction aborted (timeout)")
  
  let overlay = wal.getPageAtOrBefore(pageId, snapshot)
  if overlay.isSome:
    let payload = overlay.get
    var s = newString(payload.len)
    if payload.len > 0:
      copyMem(addr s[0], unsafeAddr payload[0], payload.len)
    return ok(s)
  readPageDirect(pager, pageId)

proc beginWrite*(wal: Wal): Result[WalWriter] =
  acquire(wal.lock)
  if wal.cachedWriter == nil:
    wal.cachedWriter = WalWriter(wal: wal, pending: @[], active: true, hasPendingSingle: false, flushed: initTable[PageId, WalIndexEntry](), pageMeta: @[])
  else:
    wal.cachedWriter.pending.setLen(0)
    wal.cachedWriter.hasPendingSingle = false
    wal.cachedWriter.flushed.clear()
    wal.cachedWriter.pageMeta.setLen(0)
    wal.cachedWriter.active = true
  ok(wal.cachedWriter)

proc addPending(writer: WalWriter, entry: WalPendingPage) =
  if writer.pending.len > 0:
    writer.pending.add(entry)
  elif writer.hasPendingSingle:
    writer.pending = @[writer.pendingSingle, entry]
    writer.hasPendingSingle = false
  else:
    writer.pendingSingle = entry
    writer.hasPendingSingle = true

proc writePage*(writer: WalWriter, pageId: PageId, data: seq[byte]): Result[Void] =
  if not writer.active:
    return err[Void](ERR_TRANSACTION, "No active transaction")
  writer.addPending(WalPendingPage(pageId: pageId, bytes: data, str: "", isString: false))
  okVoid()

proc writePageDirect*(writer: WalWriter, pageId: PageId, data: string): Result[Void] =
  ## Write page data directly without intermediate allocation.
  ## Stores string data directly to avoid extra allocation/copy before encoding.
  if not writer.active:
    return err[Void](ERR_TRANSACTION, "No active transaction")
  writer.addPending(WalPendingPage(pageId: pageId, bytes: @[], str: data, isString: true))
  okVoid()

proc noteFlushedPage*(writer: WalWriter, pageId: PageId, lsn: uint64, offset: int64) =
  ## Record a page that was flushed to WAL during the transaction.
  ## These pages will be added to the index at commit time.
  if writer.active:
    writer.flushed[pageId] = WalIndexEntry(lsn: lsn, offset: offset)

proc getFlushedPage*(writer: WalWriter, pageId: PageId): Option[WalIndexEntry] =
  ## Get metadata for a page flushed during the current transaction.
  if writer.active and writer.flushed.hasKey(pageId):
    some(writer.flushed[pageId])
  else:
    none(WalIndexEntry)

proc flushPage*(writer: WalWriter, pageId: PageId, data: seq[byte]): Result[Void] =
  ## Flush a dirty page to the WAL immediately without committing.
  ## Used to handle cache pressure during large transactions.
  let res = writer.wal.appendFrame(wfPage, uint32(pageId), data)
  if not res.ok:
    return err[Void](res.err.code, res.err.message, res.err.context)
  let (lsn, offset) = res.value
  writer.noteFlushedPage(pageId, lsn, offset)
  okVoid()

proc commit*(writer: WalWriter): Result[uint64] =
  if not writer.active:
    release(writer.wal.lock)
    return err[uint64](ERR_TRANSACTION, "No active transaction")
  
  # Optimization: Batch all writes into a single buffer and single fsync
  # This reduces lock contention and system calls significantly.
  
  writer.pageMeta.setLen(0)
  let startOffset = max(writer.wal.endOffset, int64(WalHeaderSize))
  var currentOffset = startOffset
  
  # Compute total length upfront (commit frame included)
  var totalLen = HeaderSize + TrailerSize # commit frame
  if writer.hasPendingSingle:
    let payloadLen = if writer.pendingSingle.isString: writer.pendingSingle.str.len else: writer.pendingSingle.bytes.len
    totalLen += HeaderSize + TrailerSize + payloadLen
  for entry in writer.pending:
    let payloadLen = if entry.isString: entry.str.len else: entry.bytes.len
    totalLen += HeaderSize + TrailerSize + payloadLen

  let fpRes = applyFailpoint(writer.wal, "wal_write_frame", totalLen)
  if not fpRes.ok:
    writer.active = false
    release(writer.wal.lock)
    return err[uint64](fpRes.err.code, fpRes.err.message, fpRes.err.context)

  let writeLen = fpRes.value
  if writeLen < totalLen:
    # Partial write path (simulate torn write)
    if writer.wal.frameBuffer.len < totalLen:
      writer.wal.frameBuffer.setLen(totalLen)
    var bufferOffset = 0
    if writer.hasPendingSingle:
      perf.WalGrowthWriter.inc()
      let len =
        if writer.pendingSingle.isString:
          encodeFrameIntoString(writer.wal.frameBuffer, bufferOffset, wfPage, uint32(writer.pendingSingle.pageId), writer.pendingSingle.str)
        else:
          encodeFrameInto(writer.wal.frameBuffer, bufferOffset, wfPage, uint32(writer.pendingSingle.pageId), writer.pendingSingle.bytes)
      bufferOffset += len
    for entry in writer.pending:
      perf.WalGrowthWriter.inc()
      let len =
        if entry.isString:
          encodeFrameIntoString(writer.wal.frameBuffer, bufferOffset, wfPage, uint32(entry.pageId), entry.str)
        else:
          encodeFrameInto(writer.wal.frameBuffer, bufferOffset, wfPage, uint32(entry.pageId), entry.bytes)
      bufferOffset += len
    perf.WalGrowthWriter.inc()
    discard encodeFrameInto(writer.wal.frameBuffer, bufferOffset, wfCommit, 0, [])

    let writeRes = writer.wal.vfs.write(writer.wal.file, startOffset, writer.wal.frameBuffer.toOpenArray(0, writeLen - 1))
    if not writeRes.ok:
      writer.active = false
      release(writer.wal.lock)
      return err[uint64](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    writer.active = false
    release(writer.wal.lock)
    return err[uint64](ERR_IO, "Partial frame write", "wal_write_frame")

  var useMmap = false
  if totalLen > 0:
    let mmapRes = ensureWalMmapCapacity(writer.wal, startOffset + int64(totalLen))
    if not mmapRes.ok:
      writer.active = false
      release(writer.wal.lock)
      return err[uint64](mmapRes.err.code, mmapRes.err.message, mmapRes.err.context)
    useMmap = mmapRes.value

  if useMmap:
    let dest = cast[ptr UncheckedArray[byte]](writer.wal.mmapPtr)
    var mapOffset = int(startOffset)
    if writer.hasPendingSingle:
      perf.WalGrowthWriter.inc()
      let frameStart = currentOffset
      let len =
        if writer.pendingSingle.isString:
          encodeFrameIntoPtrString(dest, mapOffset, wfPage, uint32(writer.pendingSingle.pageId), writer.pendingSingle.str)
        else:
          encodeFrameIntoPtr(dest, mapOffset, wfPage, uint32(writer.pendingSingle.pageId), writer.pendingSingle.bytes)
      let frameEnd = frameStart + int64(len)
      writer.pageMeta.add((writer.pendingSingle.pageId, WalIndexEntry(lsn: uint64(frameEnd), offset: frameStart)))
      mapOffset += len
      currentOffset = frameEnd

    for entry in writer.pending:
      perf.WalGrowthWriter.inc()
      let len =
        if entry.isString:
          encodeFrameIntoPtrString(dest, mapOffset, wfPage, uint32(entry.pageId), entry.str)
        else:
          encodeFrameIntoPtr(dest, mapOffset, wfPage, uint32(entry.pageId), entry.bytes)
      let frameEnd = currentOffset + int64(len)
      writer.pageMeta.add((entry.pageId, WalIndexEntry(lsn: uint64(frameEnd), offset: currentOffset)))
      mapOffset += len
      currentOffset += int64(len)

    perf.WalGrowthWriter.inc()
    let commitLen = encodeFrameIntoPtr(dest, mapOffset, wfCommit, 0, [])
    mapOffset += commitLen
    currentOffset += int64(commitLen)
  else:
    if writer.wal.frameBuffer.len < totalLen:
      writer.wal.frameBuffer.setLen(totalLen)
    var bufferOffset = 0
    if writer.hasPendingSingle:
      perf.WalGrowthWriter.inc()
      let frameStart = currentOffset
      let len =
        if writer.pendingSingle.isString:
          encodeFrameIntoString(writer.wal.frameBuffer, bufferOffset, wfPage, uint32(writer.pendingSingle.pageId), writer.pendingSingle.str)
        else:
          encodeFrameInto(writer.wal.frameBuffer, bufferOffset, wfPage, uint32(writer.pendingSingle.pageId), writer.pendingSingle.bytes)
      let frameEnd = frameStart + int64(len)
      writer.pageMeta.add((writer.pendingSingle.pageId, WalIndexEntry(lsn: uint64(frameEnd), offset: frameStart)))
      bufferOffset += len
      currentOffset = frameEnd

    for entry in writer.pending:
      perf.WalGrowthWriter.inc()
      let len =
        if entry.isString:
          encodeFrameIntoString(writer.wal.frameBuffer, bufferOffset, wfPage, uint32(entry.pageId), entry.str)
        else:
          encodeFrameInto(writer.wal.frameBuffer, bufferOffset, wfPage, uint32(entry.pageId), entry.bytes)
      let frameEnd = currentOffset + int64(len)
      writer.pageMeta.add((entry.pageId, WalIndexEntry(lsn: uint64(frameEnd), offset: currentOffset)))
      bufferOffset += len
      currentOffset += int64(len)

    perf.WalGrowthWriter.inc()
    let commitLen = encodeFrameInto(writer.wal.frameBuffer, bufferOffset, wfCommit, 0, [])
    bufferOffset += commitLen
    currentOffset += int64(commitLen)

    if totalLen > 0:
      let writeRes = writer.wal.vfs.write(writer.wal.file, startOffset, writer.wal.frameBuffer.toOpenArray(0, totalLen - 1))
      if not writeRes.ok:
        writer.active = false
        release(writer.wal.lock)
        return err[uint64](writeRes.err.code, writeRes.err.message, writeRes.err.context)

  let newEnd = startOffset + int64(totalLen)
  let headerRes = writeWalHeader(writer.wal, uint64(newEnd))
  if not headerRes.ok:
    writer.active = false
    release(writer.wal.lock)
    return err[uint64](headerRes.err.code, headerRes.err.message, headerRes.err.context)
  writer.wal.endOffset = newEnd
  
  let syncFail = applyFailpoint(writer.wal, "wal_fsync", 0)
  if not syncFail.ok:
    writer.active = false
    release(writer.wal.lock)
    return err[uint64](syncFail.err.code, syncFail.err.message, syncFail.err.context)
  let syncRes = writer.wal.vfs.fsync(writer.wal.file)
  if not syncRes.ok:
    writer.active = false
    release(writer.wal.lock)
    return err[uint64](syncRes.err.code, syncRes.err.message, syncRes.err.context)
  acquire(writer.wal.indexLock)
  
  # Add flushed pages to index first
  for pageId, entry in writer.flushed:
    if not writer.wal.index.hasKey(pageId):
      writer.wal.index[pageId] = @[]
    writer.wal.index[pageId].add(entry)
    writer.wal.dirtySinceCheckpoint[pageId] = entry
  
  # Add pending pages to index
  for entry in writer.pageMeta:
    if not writer.wal.index.hasKey(entry[0]):
      writer.wal.index[entry[0]] = @[]
    writer.wal.index[entry[0]].add(entry[1])
    writer.wal.dirtySinceCheckpoint[entry[0]] = entry[1]

  release(writer.wal.indexLock)
  let commitLsn = uint64(writer.wal.endOffset)
  writer.wal.walEnd.store(commitLsn, moRelease)
  writer.active = false
  release(writer.wal.lock)
  writer.pending.setLen(0)
  writer.hasPendingSingle = false
  ok(commitLsn)

proc rollback*(writer: WalWriter): Result[Void] =
  writer.pending.setLen(0)
  writer.hasPendingSingle = false
  writer.flushed.clear()
  writer.active = false
  release(writer.wal.lock)
  okVoid()
