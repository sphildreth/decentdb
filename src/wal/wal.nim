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

type WalFrameType* = enum
  wfPage = 0
  wfCommit = 1
  wfCheckpoint = 2

type WalIndexEntry = object
  lsn: uint64
  offset: int64

type ReadTxn* = object
  id*: int
  snapshot*: uint64

type WalFailpointKind* = enum
  wfNone
  wfError
  wfPartial

type WalFailpoint* = object
  kind*: WalFailpointKind
  partialBytes*: int
  remaining*: int

type Wal* = ref object
  vfs*: Vfs
  file*: VfsFile
  path*: string
  nextLsn*: uint64
  walEnd*: Atomic[uint64]
  endOffset*: int64
  index*: Table[PageId, seq[WalIndexEntry]]
  dirtySinceCheckpoint: Table[PageId, WalIndexEntry]
  lock*: Lock
  indexLock*: Lock
  readerLock*: Lock
  readers*: Table[int, tuple[snapshot: uint64, started: float]]
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

const HeaderSize = 1 + 4 + 4
const TrailerSize = 8 + 8

proc encodeFrame(frameType: WalFrameType, pageId: uint32, payload: openArray[byte], lsn: uint64): seq[byte] =
  var buf = newSeq[byte](HeaderSize + payload.len + TrailerSize)
  buf[0] = byte(frameType)
  writeU32LE(buf, 1, pageId)
  writeU32LE(buf, 5, uint32(payload.len))
  if payload.len > 0:
    copyMem(addr buf[HeaderSize], unsafeAddr payload[0], payload.len)
  let checksum = uint64(crc32c(buf[0 ..< HeaderSize + payload.len]))
  writeU64LE(buf, HeaderSize + payload.len, checksum)
  writeU64LE(buf, HeaderSize + payload.len + 8, lsn)
  buf

proc readFrame(vfs: Vfs, file: VfsFile, offset: int64): Result[(WalFrameType, uint32, seq[byte], uint64, int64)] =
  var header = newSeq[byte](HeaderSize)
  let headerRes = vfs.read(file, offset, header)
  if not headerRes.ok:
    return err[(WalFrameType, uint32, seq[byte], uint64, int64)](headerRes.err.code, headerRes.err.message, headerRes.err.context)
  if headerRes.value < HeaderSize:
    return err[(WalFrameType, uint32, seq[byte], uint64, int64)](ERR_IO, "Short header read")
  let frameType = WalFrameType(header[0])
  let pageId = readU32LE(header, 1)
  let payloadSize = int(readU32LE(header, 5))
  var payload = newSeq[byte](payloadSize)
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
  let checksum = readU64LE(trailer, 0)
  let lsn = readU64LE(trailer, 8)
  let computed = uint64(crc32c(header & payload))
  if checksum != computed:
    return err[(WalFrameType, uint32, seq[byte], uint64, int64)](ERR_CORRUPTION, "Checksum mismatch")
  let nextOffset = offset + HeaderSize + payloadSize + TrailerSize
  ok((frameType, pageId, payload, lsn, nextOffset))

proc newWal*(vfs: Vfs, path: string): Result[Wal] =
  let fileRes = vfs.open(path, fmReadWrite, true)
  if not fileRes.ok:
    return err[Wal](fileRes.err.code, fileRes.err.message, fileRes.err.context)
  let wal = Wal(
    vfs: vfs,
    file: fileRes.value,
    path: path,
    endOffset: getFileInfo(path).size,
    index: initTable[PageId, seq[WalIndexEntry]](),
    dirtySinceCheckpoint: initTable[PageId, WalIndexEntry](),
    readers: initTable[int, tuple[snapshot: uint64, started: float]](),
    abortedReaders: initHashSet[int](),
    failpoints: initTable[string, WalFailpoint](),
    warnings: @[],
    lastCheckpointAt: epochTime()
  )
  initLock(wal.lock)
  initLock(wal.indexLock)
  initLock(wal.readerLock)
  wal.nextLsn = 1
  wal.walEnd.store(0, moRelaxed)
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

proc applyFailpoint(wal: Wal, label: string, buffer: seq[byte]): Result[int] =
  if not wal.failpoints.hasKey(label):
    return ok(buffer.len)
  var fp = wal.failpoints[label]
  if fp.remaining > 0:
    fp.remaining.dec
    if fp.remaining == 0:
      wal.failpoints.del(label)
    else:
      wal.failpoints[label] = fp
  case fp.kind
  of wfNone:
    ok(buffer.len)
  of wfError:
    err[int](ERR_IO, "Injected WAL failpoint", label)
  of wfPartial:
    let part = min(fp.partialBytes, buffer.len)
    ok(part)

proc appendFrame(wal: Wal, frameType: WalFrameType, pageId: uint32, payload: seq[byte]): Result[(uint64, int64)] =
  let lsn = wal.nextLsn
  wal.nextLsn.inc
  let frame = encodeFrame(frameType, pageId, payload, lsn)
  let offset = wal.endOffset
  let fpRes = applyFailpoint(wal, "wal_write_frame", frame)
  if not fpRes.ok:
    return err[(uint64, int64)](fpRes.err.code, fpRes.err.message, fpRes.err.context)
  let writeLen = fpRes.value
  let writeRes = wal.vfs.write(wal.file, offset, frame[0 ..< writeLen])
  if not writeRes.ok:
    return err[(uint64, int64)](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  if writeLen < frame.len:
    return err[(uint64, int64)](ERR_IO, "Partial frame write", "wal_write_frame")
  wal.endOffset += int64(frame.len)
  ok((lsn, offset))

proc encodeCheckpointPayload(lsn: uint64): seq[byte] =
  result = newSeq[byte](8)
  writeU64LE(result, 0, lsn)

proc readersOverThreshold*(wal: Wal, thresholdMs: int64): seq[tuple[id: int, snapshot: uint64, ageMs: int64]] =
  if thresholdMs <= 0:
    return @[]
  let now = epochTime()
  acquire(wal.readerLock)
  for id, info in wal.readers:
    let elapsedMs = int64((now - info.started) * 1000)
    if elapsedMs >= thresholdMs:
      result.add((id: id, snapshot: info.snapshot, ageMs: elapsedMs))
  release(wal.readerLock)

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

proc checkpoint*(wal: Wal, pager: Pager): Result[uint64] =
  # Phase 1: Acquire lock, determine what to checkpoint, then release lock
  acquire(wal.lock)
  wal.checkpointPending = true
  let lastCommit = wal.walEnd.load(moAcquire)
  if lastCommit == 0:
    wal.checkpointPending = false
    release(wal.lock)
    wal.lastCheckpointAt = epochTime()
    return ok(lastCommit)
  if wal.readerWarnMs > 0:
    let warningReaders = wal.readersOverThreshold(wal.readerWarnMs)
    for info in warningReaders:
      wal.recordWarningLocked("Long-running reader id=" & $info.id & " snapshot=" & $info.snapshot & " age_ms=" & $info.ageMs)
  if wal.readerTimeoutMs > 0:
    let timedOut = wal.readersOverThreshold(wal.readerTimeoutMs)
    if timedOut.len > 0:
      for info in timedOut:
        wal.recordWarningLocked("Reader timeout id=" & $info.id & " snapshot=" & $info.snapshot & " age_ms=" & $info.ageMs)
      acquire(wal.readerLock)
      for info in timedOut:
        wal.abortedReaders.incl(info.id)
        if wal.readers.hasKey(info.id):
          wal.readers.del(info.id)
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
    let frameRes = readFrame(wal.vfs, wal.file, entry[1])
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
    let failRes = applyFailpoint(wal, "checkpoint_write_page", bestPayload)
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
  let fsyncFail = applyFailpoint(wal, "checkpoint_fsync", @[])
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
  let walSyncFail = applyFailpoint(wal, "checkpoint_wal_fsync", @[])
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
  if minSnap.isNone or minSnap.get >= lastCommit:
    release(wal.lock)
    let truncRes = wal.vfs.truncate(wal.file, 0)
    if not truncRes.ok:
      acquire(wal.lock)
      wal.checkpointPending = false
      release(wal.lock)
      return err[uint64](truncRes.err.code, truncRes.err.message, truncRes.err.context)
    acquire(wal.lock)
    acquire(wal.indexLock)
    wal.index.clear()
    wal.dirtySinceCheckpoint.clear()
    release(wal.indexLock)
    wal.endOffset = 0
  wal.checkpointPending = false
  release(wal.lock)
  ok(safeLsn)

proc maybeCheckpoint*(wal: Wal, pager: Pager): Result[bool] =
  var trigger = false
  if wal.checkpointEveryBytes > 0:
    if wal.endOffset >= wal.checkpointEveryBytes:
      trigger = true
  if wal.checkpointEveryMs > 0:
    let elapsedMs = int64((epochTime() - wal.lastCheckpointAt) * 1000)
    if elapsedMs >= wal.checkpointEveryMs:
      trigger = true
  if not trigger:
    return ok(false)
  let chkRes = checkpoint(wal, pager)
  if not chkRes.ok:
    return err[bool](chkRes.err.code, chkRes.err.message, chkRes.err.context)
  ok(true)

proc recover*(wal: Wal): Result[Void] =
  wal.index.clear()
  wal.dirtySinceCheckpoint.clear()
  var pending: seq[(PageId, uint64, int64)] = @[]
  var lastCommit: uint64 = 0
  var offset: int64 = 0
  while true:
    let frameOffset = offset
    let frameRes = readFrame(wal.vfs, wal.file, frameOffset)
    if not frameRes.ok:
      break
    let (frameType, pageId, payload, lsn, nextOffset) = frameRes.value
    case frameType
    of wfPage:
      pending.add((PageId(pageId), lsn, frameOffset))
    of wfCommit:
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
      discard
    offset = nextOffset
  wal.endOffset = offset
  wal.walEnd.store(lastCommit, moRelease)
  wal.nextLsn = max(wal.nextLsn, lastCommit + 1)
  okVoid()

proc beginRead*(wal: Wal): ReadTxn =
  let snapshot = wal.walEnd.load(moAcquire)
  acquire(wal.readerLock)
  let readerId = wal.nextReaderId
  wal.nextReaderId.inc
  wal.readers[readerId] = (snapshot: snapshot, started: epochTime())
  wal.abortedReaders.excl(readerId)
  release(wal.readerLock)
  ReadTxn(id: readerId, snapshot: snapshot)

proc endRead*(wal: Wal, txn: ReadTxn) =
  acquire(wal.readerLock)
  if wal.readers.hasKey(txn.id):
    wal.readers.del(txn.id)
  wal.abortedReaders.excl(txn.id)
  release(wal.readerLock)

proc readerCount*(wal: Wal): int =
  acquire(wal.readerLock)
  let count = wal.readers.len
  release(wal.readerLock)
  count

proc isAborted*(wal: Wal, txn: ReadTxn): bool =
  acquire(wal.readerLock)
  result = txn.id in wal.abortedReaders
  release(wal.readerLock)

proc setCheckpointConfig*(wal: Wal, everyBytes: int64, everyMs: int64, readerWarnMs: int64 = 0, readerTimeoutMs: int64 = 0, forceTruncateOnTimeout: bool = false) =
  wal.checkpointEveryBytes = everyBytes
  wal.checkpointEveryMs = everyMs
  wal.readerWarnMs = readerWarnMs
  wal.readerTimeoutMs = readerTimeoutMs
  wal.forceTruncateOnTimeout = forceTruncateOnTimeout

proc getPageAtOrBefore*(wal: Wal, pageId: PageId, snapshot: uint64): Option[seq[byte]] =
  acquire(wal.indexLock)
  defer:
    release(wal.indexLock)
  if not wal.index.hasKey(pageId):
    return none(seq[byte])
  let entries = wal.index[pageId]
  var bestLsn: uint64 = 0
  var bestOffset: int64 = -1
  for entry in entries:
    if entry.lsn <= snapshot and entry.lsn >= bestLsn:
      bestLsn = entry.lsn
      bestOffset = entry.offset
  if bestLsn == 0 or bestOffset < 0:
    return none(seq[byte])
  let frameRes = readFrame(wal.vfs, wal.file, bestOffset)
  if not frameRes.ok:
    return none(seq[byte])
  let (frameType, framePageId, payload, _, _) = frameRes.value
  if frameType != wfPage or framePageId != uint32(pageId):
    return none(seq[byte])
  some(payload)

proc readPageWithSnapshot*(pager: Pager, wal: Wal, snapshot: uint64, pageId: PageId): Result[string] =
  let overlay = wal.getPageAtOrBefore(pageId, snapshot)
  if overlay.isSome:
    let payload = overlay.get
    var s = newString(payload.len)
    if payload.len > 0:
      copyMem(addr s[0], unsafeAddr payload[0], payload.len)
    return ok(s)
  readPageDirect(pager, pageId)

type WalWriter* = ref object
  wal*: Wal
  pending*: seq[(PageId, seq[byte])]
  active*: bool

proc beginWrite*(wal: Wal): Result[WalWriter] =
  acquire(wal.lock)
  let writer = WalWriter(wal: wal, pending: @[], active: true)
  ok(writer)

proc writePage*(writer: WalWriter, pageId: PageId, data: seq[byte]): Result[Void] =
  if not writer.active:
    return err[Void](ERR_TRANSACTION, "No active transaction")
  writer.pending.add((pageId, data))
  okVoid()

proc commit*(writer: WalWriter): Result[uint64] =
  if not writer.active:
    release(writer.wal.lock)
    return err[uint64](ERR_TRANSACTION, "No active transaction")
  var pageMeta: seq[(uint64, int64)] = @[]
  for entry in writer.pending:
    let lsnRes = appendFrame(writer.wal, wfPage, uint32(entry[0]), entry[1])
    if not lsnRes.ok:
      writer.active = false
      release(writer.wal.lock)
      return err[uint64](lsnRes.err.code, lsnRes.err.message, lsnRes.err.context)
    pageMeta.add(lsnRes.value)
  let commitRes = appendFrame(writer.wal, wfCommit, 0, @[])
  if not commitRes.ok:
    writer.active = false
    release(writer.wal.lock)
    return err[uint64](commitRes.err.code, commitRes.err.message, commitRes.err.context)
  let syncFail = applyFailpoint(writer.wal, "wal_fsync", @[])
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
  for i, entry in writer.pending:
    if not writer.wal.index.hasKey(entry[0]):
      writer.wal.index[entry[0]] = @[]
    let idxEntry = WalIndexEntry(lsn: pageMeta[i][0], offset: pageMeta[i][1])
    writer.wal.index[entry[0]].add(idxEntry)
    writer.wal.dirtySinceCheckpoint[entry[0]] = idxEntry
  release(writer.wal.indexLock)
  writer.wal.walEnd.store(commitRes.value[0], moRelease)
  writer.active = false
  release(writer.wal.lock)
  ok(commitRes.value[0])

proc rollback*(writer: WalWriter): Result[Void] =
  writer.pending = @[]
  writer.active = false
  release(writer.wal.lock)
  okVoid()
