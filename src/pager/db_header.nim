import ../errors
import ../vfs/types
import std/strutils

const
  HeaderSize* = 128
  MagicBytes* = "DECENTDB"
  MagicPadded* = MagicBytes & repeat('\0', 16 - MagicBytes.len)
  FormatVersion* = 2'u32
  DefaultPageSize* = 4096'u32

type DbHeader* = object
  formatVersion*: uint32
  pageSize*: uint32
  schemaCookie*: uint32
  rootCatalog*: uint32
  rootFreelist*: uint32
  freelistHead*: uint32
  freelistCount*: uint32
  lastCheckpointLsn*: uint64

proc writeU32LE*(buf: var openArray[byte], offset: int, value: uint32) =
  buf[offset] = byte(value and 0xFF)
  buf[offset + 1] = byte((value shr 8) and 0xFF)
  buf[offset + 2] = byte((value shr 16) and 0xFF)
  buf[offset + 3] = byte((value shr 24) and 0xFF)

proc writeU32LE*(buf: var string, offset: int, value: uint32) =
  buf[offset] = char(byte(value and 0xFF))
  buf[offset + 1] = char(byte((value shr 8) and 0xFF))
  buf[offset + 2] = char(byte((value shr 16) and 0xFF))
  buf[offset + 3] = char(byte((value shr 24) and 0xFF))

proc writeU64LE*(buf: var openArray[byte], offset: int, value: uint64) =
  buf[offset] = byte(value and 0xFF)
  buf[offset + 1] = byte((value shr 8) and 0xFF)
  buf[offset + 2] = byte((value shr 16) and 0xFF)
  buf[offset + 3] = byte((value shr 24) and 0xFF)
  buf[offset + 4] = byte((value shr 32) and 0xFF)
  buf[offset + 5] = byte((value shr 40) and 0xFF)
  buf[offset + 6] = byte((value shr 48) and 0xFF)
  buf[offset + 7] = byte((value shr 56) and 0xFF)

proc writeU64LE*(buf: var string, offset: int, value: uint64) =
  buf[offset] = char(byte(value and 0xFF))
  buf[offset + 1] = char(byte((value shr 8) and 0xFF))
  buf[offset + 2] = char(byte((value shr 16) and 0xFF))
  buf[offset + 3] = char(byte((value shr 24) and 0xFF))
  buf[offset + 4] = char(byte((value shr 32) and 0xFF))
  buf[offset + 5] = char(byte((value shr 40) and 0xFF))
  buf[offset + 6] = char(byte((value shr 48) and 0xFF))
  buf[offset + 7] = char(byte((value shr 56) and 0xFF))

proc readU32LE*(buf: openArray[byte], offset: int): uint32 =
  uint32(buf[offset]) or
    (uint32(buf[offset + 1]) shl 8) or
    (uint32(buf[offset + 2]) shl 16) or
    (uint32(buf[offset + 3]) shl 24)

proc readU32LE*(buf: string, offset: int): uint32 =
  uint32(byte(buf[offset])) or
    (uint32(byte(buf[offset + 1])) shl 8) or
    (uint32(byte(buf[offset + 2])) shl 16) or
    (uint32(byte(buf[offset + 3])) shl 24)

proc readU64LE*(buf: openArray[byte], offset: int): uint64 =
  uint64(buf[offset]) or
    (uint64(buf[offset + 1]) shl 8) or
    (uint64(buf[offset + 2]) shl 16) or
    (uint64(buf[offset + 3]) shl 24) or
    (uint64(buf[offset + 4]) shl 32) or
    (uint64(buf[offset + 5]) shl 40) or
    (uint64(buf[offset + 6]) shl 48) or
    (uint64(buf[offset + 7]) shl 56)

proc readU64LE*(buf: string, offset: int): uint64 =
  uint64(byte(buf[offset])) or
    (uint64(byte(buf[offset + 1])) shl 8) or
    (uint64(byte(buf[offset + 2])) shl 16) or
    (uint64(byte(buf[offset + 3])) shl 24) or
    (uint64(byte(buf[offset + 4])) shl 32) or
    (uint64(byte(buf[offset + 5])) shl 40) or
    (uint64(byte(buf[offset + 6])) shl 48) or
    (uint64(byte(buf[offset + 7])) shl 56)

proc crc32cTables(): array[8, array[256, uint32]] =
  const Poly = 0x82F63B78'u32
  var tables: array[8, array[256, uint32]]
  for i in 0 .. 255:
    var crc = uint32(i)
    for _ in 0 .. 7:
      if (crc and 1) == 1:
        crc = (crc shr 1) xor Poly
      else:
        crc = crc shr 1
    tables[0][i] = crc
  for i in 0 .. 255:
    var crc = tables[0][i]
    for j in 1 .. 7:
      crc = (crc shr 8) xor tables[0][int(crc and 0xFF'u32)]
      tables[j][i] = crc
  tables

let Crc32cTables = crc32cTables()

proc crc32c*(data: openArray[byte]): uint32 =
  var crc = 0xFFFFFFFF'u32
  var i = 0
  let n = data.len
  while i + 8 <= n:
    crc = crc xor readU32LE(data, i)
    crc =
      Crc32cTables[7][int(crc and 0xFF'u32)] xor
      Crc32cTables[6][int((crc shr 8) and 0xFF'u32)] xor
      Crc32cTables[5][int((crc shr 16) and 0xFF'u32)] xor
      Crc32cTables[4][int((crc shr 24) and 0xFF'u32)] xor
      Crc32cTables[3][int(data[i + 4])] xor
      Crc32cTables[2][int(data[i + 5])] xor
      Crc32cTables[1][int(data[i + 6])] xor
      Crc32cTables[0][int(data[i + 7])]
    i += 8
  while i < n:
    let idx = (crc xor uint32(data[i])) and 0xFF'u32
    crc = (crc shr 8) xor Crc32cTables[0][int(idx)]
    i.inc
  crc xor 0xFFFFFFFF'u32

proc crc32c*(data: string): uint32 =
  var crc = 0xFFFFFFFF'u32
  var i = 0
  let n = data.len
  while i + 8 <= n:
    crc = crc xor readU32LE(data, i)
    crc =
      Crc32cTables[7][int(crc and 0xFF'u32)] xor
      Crc32cTables[6][int((crc shr 8) and 0xFF'u32)] xor
      Crc32cTables[5][int((crc shr 16) and 0xFF'u32)] xor
      Crc32cTables[4][int((crc shr 24) and 0xFF'u32)] xor
      Crc32cTables[3][int(byte(data[i + 4]))] xor
      Crc32cTables[2][int(byte(data[i + 5]))] xor
      Crc32cTables[1][int(byte(data[i + 6]))] xor
      Crc32cTables[0][int(byte(data[i + 7]))]
    i += 8
  while i < n:
    let idx = (crc xor uint32(byte(data[i]))) and 0xFF'u32
    crc = (crc shr 8) xor Crc32cTables[0][int(idx)]
    i.inc
  crc xor 0xFFFFFFFF'u32

proc computeHeaderChecksum(buf: openArray[byte]): uint32 =
  var combined = newSeq[byte](24 + (HeaderSize - 28))
  for i in 0 .. 23:
    combined[i] = buf[i]
  var dest = 24
  for i in 28 .. 127:
    combined[dest] = buf[i]
    dest.inc
  crc32c(combined)

proc headerMagicOk*(buf: openArray[byte]): bool =
  if buf.len < 16:
    return false
  for i in 0 .. 15:
    if buf[i] != byte(MagicPadded[i]):
      return false
  true

proc headerChecksumActual*(buf: openArray[byte]): uint32 =
  computeHeaderChecksum(buf)

proc headerChecksumExpected*(buf: openArray[byte]): uint32 =
  if buf.len < 28:
    return 0'u32
  readU32LE(buf, 24)

proc decodeHeaderUnsafe*(buf: openArray[byte]): Result[DbHeader] =
  if buf.len < HeaderSize:
    return err[DbHeader](ERR_CORRUPTION, "Header too short", "page_id=1")
  ok(DbHeader(
    formatVersion: readU32LE(buf, 16),
    pageSize: readU32LE(buf, 20),
    schemaCookie: readU32LE(buf, 28),
    rootCatalog: readU32LE(buf, 32),
    rootFreelist: readU32LE(buf, 36),
    freelistHead: readU32LE(buf, 40),
    freelistCount: readU32LE(buf, 44),
    lastCheckpointLsn: readU64LE(buf, 48)
  ))

proc encodeHeader*(header: DbHeader): array[HeaderSize, byte] =
  var buf: array[HeaderSize, byte]
  for i, ch in MagicPadded:
    buf[i] = byte(ch)
  writeU32LE(buf, 16, header.formatVersion)
  writeU32LE(buf, 20, header.pageSize)
  writeU32LE(buf, 28, header.schemaCookie)
  writeU32LE(buf, 32, header.rootCatalog)
  writeU32LE(buf, 36, header.rootFreelist)
  writeU32LE(buf, 40, header.freelistHead)
  writeU32LE(buf, 44, header.freelistCount)
  writeU64LE(buf, 48, header.lastCheckpointLsn)
  let checksum = computeHeaderChecksum(buf)
  writeU32LE(buf, 24, checksum)
  buf

proc decodeHeader*(buf: openArray[byte]): Result[DbHeader] =
  if buf.len < HeaderSize:
    return err[DbHeader](ERR_CORRUPTION, "Header too short", "page_id=1")
  for i in 0 .. 15:
    if buf[i] != byte(MagicPadded[i]):
      return err[DbHeader](ERR_CORRUPTION, "Bad header magic", "page_id=1")
  let expected = readU32LE(buf, 24)
  let actual = computeHeaderChecksum(buf)
  if expected != actual:
    return err[DbHeader](ERR_CORRUPTION, "Header checksum mismatch", "page_id=1")
  ok(DbHeader(
    formatVersion: readU32LE(buf, 16),
    pageSize: readU32LE(buf, 20),
    schemaCookie: readU32LE(buf, 28),
    rootCatalog: readU32LE(buf, 32),
    rootFreelist: readU32LE(buf, 36),
    freelistHead: readU32LE(buf, 40),
    freelistCount: readU32LE(buf, 44),
    lastCheckpointLsn: readU64LE(buf, 48)
  ))

proc decodeHeader*(buf: string): Result[DbHeader] =
  if buf.len < HeaderSize:
    return err[DbHeader](ERR_CORRUPTION, "Header too short", "page_id=1")
  # Convert string to openArray[byte]
  var bytes = newSeq[byte](buf.len)
  if buf.len > 0: copyMem(addr bytes[0], unsafeAddr buf[0], buf.len)
  decodeHeader(bytes)

proc readHeader*(vfs: Vfs, file: VfsFile): Result[DbHeader] =
  var buf = newSeq[byte](HeaderSize)
  let res = vfs.read(file, 0, buf)
  if not res.ok:
    return err[DbHeader](res.err.code, res.err.message, res.err.context)
  if res.value < HeaderSize:
    return err[DbHeader](ERR_CORRUPTION, "Header too short", "page_id=1")
  decodeHeader(buf)

proc writeHeader*(vfs: Vfs, file: VfsFile, header: DbHeader): Result[Void] =
  let buf = encodeHeader(header)
  let res = vfs.write(file, 0, buf)
  if not res.ok:
    return err[Void](res.err.code, res.err.message, res.err.context)
  if res.value < HeaderSize:
    return err[Void](ERR_IO, "Short write on header", "page_id=1")
  vfs.fsync(file)
