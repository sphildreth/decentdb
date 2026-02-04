import zip/zlib
import ../errors
import ../pager/pager
import ../pager/db_header

type ValueKind* = enum
  vkNull
  vkInt64
  vkBool
  vkFloat64
  vkText
  vkBlob
  vkTextOverflow
  vkBlobOverflow
  vkTextCompressed
  vkBlobCompressed
  vkTextCompressedOverflow
  vkBlobCompressedOverflow

type Value* = object
  kind*: ValueKind
  int64Val*: int64
  boolVal*: bool
  float64Val*: float64
  bytes*: seq[byte]
  overflowPage*: PageId
  overflowLen*: uint32

func zigzagEncode(n: int64): uint64 =
  (cast[uint64](n) shl 1) xor cast[uint64](n shr 63)

func zigzagDecode(n: uint64): int64 =
  let shifted = n shr 1
  if (n and 1) == 0:
    result = cast[int64](shifted)
  else:
    result = cast[int64](shifted xor (not 0'u64))

proc encodeVarint*(value: uint64): seq[byte] =
  var v = value
  result = @[]
  while true:
    var b = byte(v and 0x7F)
    v = v shr 7
    if v != 0:
      b = b or 0x80
    result.add(b)
    if v == 0:
      break

proc decodeVarint*(data: openArray[byte], offset: var int): Result[uint64] =
  var shift = 0
  var value: uint64 = 0
  while offset < data.len:
    let b = data[offset]
    offset.inc
    value = value or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      return ok(value)
    shift += 7
    if shift > 63:
      return err[uint64](ERR_CORRUPTION, "Varint overflow")
  err[uint64](ERR_CORRUPTION, "Unexpected end of varint")

proc decodeVarint*(data: string, offset: var int): Result[uint64] =
  ## Decode a varint from a raw byte string.
  ## This avoids copying pager page buffers into seq[byte] just to decode varints.
  var shift = 0
  var value: uint64 = 0
  while offset < data.len:
    let b = byte(data[offset])
    offset.inc
    value = value or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      return ok(value)
    shift += 7
    if shift > 63:
      return err[uint64](ERR_CORRUPTION, "Varint overflow")
  err[uint64](ERR_CORRUPTION, "Unexpected end of varint")

proc decodeVarintFast*(data: openArray[byte], offset: var int, valOut: var uint64): bool {.inline.} =
  ## Optimized varint decoder avoiding Result overhead.
  var shift = 0
  var value: uint64 = 0
  var i = offset
  let L = data.len
  while i < L:
    let b = data[i]
    i.inc
    value = value or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      valOut = value
      offset = i
      return true
    shift += 7
    if shift > 63:
      return false
  return false

proc decodeVarintFast*(data: string, offset: var int, valOut: var uint64): bool {.inline.} =
  ## Optimized varint decoder avoiding Result overhead.
  var shift = 0
  var value: uint64 = 0
  var i = offset
  let L = data.len
  while i < L:
    let b = byte(data[i])
    i.inc
    value = value or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      valOut = value
      offset = i
      return true
    shift += 7
    if shift > 63:
      return false
  return false

proc compressData(data: seq[byte]): seq[byte] =
  if data.len == 0: return @[]
  var s = newString(data.len)
  if data.len > 0:
    copyMem(addr s[0], unsafeAddr data[0], data.len)
  
  let compressed = zlib.compress(s, stream=ZlibStream)
  
  result = newSeq[byte](compressed.len)
  if compressed.len > 0:
    copyMem(addr result[0], unsafeAddr compressed[0], compressed.len)

proc decompressData(data: seq[byte]): Result[seq[byte]] =
  if data.len == 0: return ok(newSeq[byte]())
  var s = newString(data.len)
  if data.len > 0:
    copyMem(addr s[0], unsafeAddr data[0], data.len)
  
  try:
    let decompressed = zlib.uncompress(s, stream=ZlibStream)
    var res = newSeq[byte](decompressed.len)
    if decompressed.len > 0:
      copyMem(addr res[0], unsafeAddr decompressed[0], decompressed.len)
    ok(res)
  except:
    let e = getCurrentException()
    return err[seq[byte]](ERR_CORRUPTION, "Decompression failed: " & e.msg)

proc compressValue*(value: Value): Value =
  if value.kind notin {vkText, vkBlob}:
    return value
  
  if value.bytes.len <= 128:
    return value
    
  let compressed = compressData(value.bytes)
  if float(compressed.len) < float(value.bytes.len) * 0.9:
    var newVal = value
    newVal.kind = if value.kind == vkText: vkTextCompressed else: vkBlobCompressed
    newVal.bytes = compressed
    return newVal
  
  return value

proc encodeValue*(value: Value): seq[byte] =
  var payload: seq[byte] = @[]
  case value.kind
  of vkNull:
    payload = @[]
  of vkBool:
    payload = @[byte(if value.boolVal: 1 else: 0)]
  of vkInt64:
    payload = encodeVarint(zigzagEncode(value.int64Val))
  of vkFloat64:
    payload = newSeq[byte](8)
    writeU64LE(payload, 0, cast[uint64](value.float64Val))
  of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
    payload = if value.bytes.len == 0: @[] else: value.bytes
  of vkTextOverflow, vkBlobOverflow, vkTextCompressedOverflow, vkBlobCompressedOverflow:
    payload = newSeq[byte](8)
    writeU32LE(payload, 0, uint32(value.overflowPage))
    writeU32LE(payload, 4, value.overflowLen)
  result = @[byte(value.kind)]
  result.add(encodeVarint(uint64(payload.len)))
  result.add(payload)

proc decodeValue*(data: openArray[byte], offset: var int): Result[Value] =
  if offset >= data.len:
    return err[Value](ERR_CORRUPTION, "Unexpected end of record")
  let kindValue = int(data[offset])
  if kindValue < 0 or kindValue > ord(high(ValueKind)):
    return err[Value](ERR_CORRUPTION, "Unknown value kind")
  let kind = ValueKind(kindValue)
  offset.inc
  let lenRes = decodeVarint(data, offset)
  if not lenRes.ok:
    return err[Value](lenRes.err.code, lenRes.err.message, lenRes.err.context)
  let length = int(lenRes.value)
  if offset + length > data.len:
    return err[Value](ERR_CORRUPTION, "Record field length out of bounds")
  let payload = data[offset ..< offset + length]
  offset += length
  var value = Value(kind: kind)
  case kind
  of vkNull:
    discard
  of vkBool:
    if payload.len != 1:
      return err[Value](ERR_CORRUPTION, "Invalid BOOL length")
    value.boolVal = payload[0] != 0
  of vkInt64:
    var pOffset = 0
    let vRes = decodeVarint(payload, pOffset)
    if not vRes.ok:
      return err[Value](vRes.err.code, vRes.err.message, vRes.err.context)
    value.int64Val = zigzagDecode(vRes.value)
  of vkFloat64:
    if payload.len != 8:
      return err[Value](ERR_CORRUPTION, "Invalid FLOAT64 length")
    value.float64Val = cast[float64](readU64LE(payload, 0))
  of vkText, vkBlob:
    value.bytes = @payload
  of vkTextCompressed, vkBlobCompressed:
    let decompRes = decompressData(@payload)
    if not decompRes.ok:
      return err[Value](decompRes.err.code, decompRes.err.message, decompRes.err.context)
    value.bytes = decompRes.value
    value.kind = if kind == vkTextCompressed: vkText else: vkBlob
  of vkTextOverflow, vkBlobOverflow, vkTextCompressedOverflow, vkBlobCompressedOverflow:
    if payload.len != 8:
      return err[Value](ERR_CORRUPTION, "Invalid overflow pointer length")
    value.overflowPage = PageId(readU32LE(payload, 0))
    value.overflowLen = readU32LE(payload, 4)
  ok(value)

proc encodeRecord*(values: seq[Value]): seq[byte] =
  result = @[]
  result.add(encodeVarint(uint64(values.len)))
  for value in values:
    result.add(encodeValue(value))

proc decodeRecord*(data: openArray[byte]): Result[seq[Value]] =
  var offset = 0
  let countRes = decodeVarint(data, offset)
  if not countRes.ok:
    return err[seq[Value]](countRes.err.code, countRes.err.message, countRes.err.context)
  let count = int(countRes.value)
  var values: seq[Value] = @[]
  for _ in 0 ..< count:
    let valueRes = decodeValue(data, offset)
    if not valueRes.ok:
      return err[seq[Value]](valueRes.err.code, valueRes.err.message, valueRes.err.context)
    values.add(valueRes.value)
  ok(values)

proc writeOverflowChain*(pager: Pager, data: openArray[byte]): Result[PageId] =
  if data.len == 0:
    return ok(PageId(0))
  let payloadSize = pager.pageSize - 8
  var offset = 0
  var firstPage: PageId = 0
  var prevPage: PageId = 0
  while offset < data.len:
    let chunkSize = min(payloadSize, data.len - offset)
    let pageRes = allocatePage(pager)
    if not pageRes.ok:
      return err[PageId](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    let pageId = pageRes.value
    var buf = newString(pager.pageSize)
    writeU32LE(buf, 0, 0)
    writeU32LE(buf, 4, uint32(chunkSize))
    for i in 0 ..< chunkSize:
      buf[8 + i] = char(data[offset + i])
    let writeRes = writePage(pager, pageId, buf)
    if not writeRes.ok:
      return err[PageId](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    if prevPage != 0:
      let prevRes = readPage(pager, prevPage)
      if not prevRes.ok:
        return err[PageId](prevRes.err.code, prevRes.err.message, prevRes.err.context)
      var prevBuf = prevRes.value
      writeU32LE(prevBuf, 0, uint32(pageId))
      let prevWrite = writePage(pager, prevPage, prevBuf)
      if not prevWrite.ok:
        return err[PageId](prevWrite.err.code, prevWrite.err.message, prevWrite.err.context)
    else:
      firstPage = pageId
    prevPage = pageId
    offset += chunkSize
  ok(firstPage)

proc readOverflowChain*(pager: Pager, start: PageId, totalLen: uint32): Result[seq[byte]] =
  if start == 0:
    return ok(newSeq[byte]())
  var output = newSeq[byte](int(totalLen))
  var written = 0
  var current = start
  while current != 0 and written < output.len:
    let pageRes = readPageRo(pager, current)
    if not pageRes.ok:
      return err[seq[byte]](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    let page = pageRes.value
    let next = readU32LE(page, 0)
    let chunkLen = int(readU32LE(page, 4))
    if 8 + chunkLen > page.len:
      return err[seq[byte]](ERR_CORRUPTION, "Overflow page length invalid", "page_id=" & $current)
    let copyLen = min(chunkLen, output.len - written)
    for i in 0 ..< copyLen:
      output[written + i] = byte(page[8 + i])
    written += copyLen
    current = PageId(next)
  ok(output)

proc readOverflowChainAll*(pager: Pager, start: PageId): Result[seq[byte]] =
  ## Read an overflow chain of unknown total length by following next pointers.
  ##
  ## This is used by B+Tree value overflow, which stores large values out-of-line
  ## without tracking a total length in the leaf cell.
  if start == 0:
    return ok(newSeq[byte]())
  var output: seq[byte] = @[]
  var current = start
  while current != 0:
    let pageRes = readPageRo(pager, current)
    if not pageRes.ok:
      return err[seq[byte]](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    let page = pageRes.value
    let next = readU32LE(page, 0)
    let chunkLen = int(readU32LE(page, 4))
    if 8 + chunkLen > page.len:
      return err[seq[byte]](ERR_CORRUPTION, "Overflow page length invalid", "page_id=" & $current)
    let oldLen = output.len
    output.setLen(oldLen + chunkLen)
    for i in 0 ..< chunkLen:
      output[oldLen + i] = byte(page[8 + i])
    current = PageId(next)
  ok(output)

proc freeOverflowChain*(pager: Pager, start: PageId): Result[Void] =
  ## Free an overflow chain by returning pages to the freelist.
  ##
  ## Note: This is used by B+Tree value overflow to avoid leaking overflow pages
  ## when values are updated or deleted.
  var current = start
  while current != 0:
    let pageRes = readPageRo(pager, current)
    if not pageRes.ok:
      return err[Void](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    let next = PageId(readU32LE(pageRes.value, 0))
    let freeRes = freePage(pager, current)
    if not freeRes.ok:
      return err[Void](freeRes.err.code, freeRes.err.message, freeRes.err.context)
    current = next
  okVoid()

proc decodeRecordWithOverflow*(pager: Pager, data: openArray[byte]): Result[seq[Value]] =
  let decoded = decodeRecord(data)
  if not decoded.ok:
    return decoded
  var values = decoded.value
  for i in 0 ..< values.len:
    if values[i].kind in {vkTextOverflow, vkBlobOverflow, vkTextCompressedOverflow, vkBlobCompressedOverflow}:
      let readRes = readOverflowChain(pager, values[i].overflowPage, values[i].overflowLen)
      if not readRes.ok:
        return err[seq[Value]](readRes.err.code, readRes.err.message, readRes.err.context)
      values[i].bytes = readRes.value
      
      if values[i].kind in {vkTextCompressedOverflow, vkBlobCompressedOverflow}:
        let decompRes = decompressData(values[i].bytes)
        if not decompRes.ok:
          return err[seq[Value]](decompRes.err.code, decompRes.err.message, decompRes.err.context)
        values[i].bytes = decompRes.value
        values[i].kind = if values[i].kind == vkTextCompressedOverflow: vkText else: vkBlob
      else:
        values[i].kind = if values[i].kind == vkTextOverflow: vkText else: vkBlob
  ok(values)
