import os
import locks
import tables
import atomics
import options
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
  payload: seq[byte]

type WalFailpointKind* = enum
  wfNone
  wfError
  wfPartial

type WalFailpoint* = object
  kind*: WalFailpointKind
  partialBytes*: int

type Wal* = ref object
  vfs*: Vfs
  file*: VfsFile
  path*: string
  nextLsn*: uint64
  walEnd*: Atomic[uint64]
  index*: Table[PageId, seq[WalIndexEntry]]
  lock*: Lock
  readerLock*: Lock
  readers*: Table[int, uint64]
  nextReaderId*: int
  failpoints*: Table[string, WalFailpoint]

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
  let wal = Wal(vfs: vfs, file: fileRes.value, path: path, index: initTable[PageId, seq[WalIndexEntry]](), readers: initTable[int, uint64](), failpoints: initTable[string, WalFailpoint]())
  initLock(wal.lock)
  initLock(wal.readerLock)
  wal.nextLsn = 1
  wal.walEnd.store(0, moRelaxed)
  ok(wal)

proc setFailpoint*(wal: Wal, label: string, fp: WalFailpoint) =
  wal.failpoints[label] = fp

proc clearFailpoints*(wal: Wal) =
  wal.failpoints.clear()

proc applyFailpoint(wal: Wal, label: string, buffer: seq[byte]): Result[int] =
  if not wal.failpoints.hasKey(label):
    return ok(buffer.len)
  let fp = wal.failpoints[label]
  case fp.kind
  of wfNone:
    ok(buffer.len)
  of wfError:
    err[int](ERR_IO, "Injected WAL failpoint", label)
  of wfPartial:
    let part = min(fp.partialBytes, buffer.len)
    ok(part)

proc appendFrame(wal: Wal, frameType: WalFrameType, pageId: uint32, payload: seq[byte]): Result[uint64] =
  let lsn = wal.nextLsn
  wal.nextLsn.inc
  let frame = encodeFrame(frameType, pageId, payload, lsn)
  let fileInfo = getFileInfo(wal.path)
  let offset = fileInfo.size
  let fpRes = applyFailpoint(wal, "wal_write_frame", frame)
  if not fpRes.ok:
    return err[uint64](fpRes.err.code, fpRes.err.message, fpRes.err.context)
  let writeLen = fpRes.value
  let writeRes = wal.vfs.write(wal.file, offset, frame[0 ..< writeLen])
  if not writeRes.ok:
    return err[uint64](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  if writeLen < frame.len:
    return err[uint64](ERR_IO, "Partial frame write", "wal_write_frame")
  ok(lsn)

proc recover*(wal: Wal): Result[Void] =
  wal.index.clear()
  var pending: seq[(PageId, seq[byte], uint64)] = @[]
  var lastCommit: uint64 = 0
  var offset: int64 = 0
  while true:
    let frameRes = readFrame(wal.vfs, wal.file, offset)
    if not frameRes.ok:
      break
    let (frameType, pageId, payload, lsn, nextOffset) = frameRes.value
    case frameType
    of wfPage:
      pending.add((PageId(pageId), payload, lsn))
    of wfCommit:
      for entry in pending:
        if not wal.index.hasKey(entry[0]):
          wal.index[entry[0]] = @[]
        wal.index[entry[0]].add(WalIndexEntry(lsn: entry[2], payload: entry[1]))
        if entry[2] > lastCommit:
          lastCommit = entry[2]
      pending = @[]
      if lsn > lastCommit:
        lastCommit = lsn
    of wfCheckpoint:
      discard
    offset = nextOffset
  wal.walEnd.store(lastCommit, moRelease)
  wal.nextLsn = max(wal.nextLsn, lastCommit + 1)
  okVoid()

proc beginRead*(wal: Wal): uint64 =
  let snapshot = wal.walEnd.load(moAcquire)
  acquire(wal.readerLock)
  let readerId = wal.nextReaderId
  wal.nextReaderId.inc
  wal.readers[readerId] = snapshot
  release(wal.readerLock)
  snapshot

proc endRead*(wal: Wal, snapshot: uint64) =
  acquire(wal.readerLock)
  for key, value in wal.readers.pairs:
    if value == snapshot:
      wal.readers.del(key)
      break
  release(wal.readerLock)

proc getPageAtOrBefore*(wal: Wal, pageId: PageId, snapshot: uint64): Option[seq[byte]] =
  if not wal.index.hasKey(pageId):
    return none(seq[byte])
  let entries = wal.index[pageId]
  var bestLsn: uint64 = 0
  var bestPayload: seq[byte] = @[]
  for entry in entries:
    if entry.lsn <= snapshot and entry.lsn >= bestLsn:
      bestLsn = entry.lsn
      bestPayload = entry.payload
  if bestLsn == 0:
    return none(seq[byte])
  some(bestPayload)

proc readPageWithSnapshot*(pager: Pager, wal: Wal, snapshot: uint64, pageId: PageId): Result[seq[byte]] =
  let overlay = wal.getPageAtOrBefore(pageId, snapshot)
  if overlay.isSome:
    return ok(overlay.get)
  readPage(pager, pageId)

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
  var pageLsns: seq[uint64] = @[]
  for entry in writer.pending:
    let lsnRes = appendFrame(writer.wal, wfPage, uint32(entry[0]), entry[1])
    if not lsnRes.ok:
      writer.active = false
      release(writer.wal.lock)
      return err[uint64](lsnRes.err.code, lsnRes.err.message, lsnRes.err.context)
    pageLsns.add(lsnRes.value)
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
  for i, entry in writer.pending:
    if not writer.wal.index.hasKey(entry[0]):
      writer.wal.index[entry[0]] = @[]
    writer.wal.index[entry[0]].add(WalIndexEntry(lsn: pageLsns[i], payload: entry[1]))
  writer.wal.walEnd.store(commitRes.value, moRelease)
  writer.active = false
  release(writer.wal.lock)
  ok(commitRes.value)

proc rollback*(writer: WalWriter): Result[Void] =
  writer.pending = @[]
  writer.active = false
  release(writer.wal.lock)
  okVoid()
