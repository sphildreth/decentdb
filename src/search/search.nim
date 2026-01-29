import ../record/record
import ../errors
import algorithm
import sequtils
import strutils

const
  DefaultPostingsThreshold* = 100000

proc canonicalize*(text: string): string =
  result = text.toUpperAscii()

proc packTrigram(a: char, b: char, c: char): uint32 =
  (uint32(byte(a)) shl 16) or (uint32(byte(b)) shl 8) or uint32(byte(c))

proc trigrams*(text: string): seq[uint32] =
  let norm = canonicalize(text)
  if norm.len < 3:
    return @[]
  for i in 0 ..< norm.len - 2:
    result.add(packTrigram(norm[i], norm[i + 1], norm[i + 2]))

proc encodePostingsSorted*(rowids: seq[uint64]): seq[byte] =
  ## Encode postings assuming `rowids` are sorted ascending.
  var prev: uint64 = 0
  for id in rowids:
    let delta = id - prev
    result.add(encodeVarint(delta))
    prev = id

proc encodePostings*(rowids: seq[uint64]): seq[byte] =
  var sorted = rowids
  sorted.sort()
  encodePostingsSorted(sorted)

proc decodePostings*(data: openArray[byte]): Result[seq[uint64]] =
  var offset = 0
  var current: uint64 = 0
  var output: seq[uint64] = @[]
  while offset < data.len:
    let deltaRes = decodeVarint(data, offset)
    if not deltaRes.ok:
      return err[seq[uint64]](deltaRes.err.code, deltaRes.err.message, deltaRes.err.context)
    current += deltaRes.value
    output.add(current)
  ok(output)

proc postingsCount*(data: openArray[byte]): int =
  let decoded = decodePostings(data)
  if not decoded.ok:
    return 0
  decoded.value.len

proc addRowid*(data: openArray[byte], rowid: uint64): Result[seq[byte]] =
  let decoded = decodePostings(data)
  if not decoded.ok:
    return err[seq[byte]](decoded.err.code, decoded.err.message, decoded.err.context)
  var ids = decoded.value
  if rowid in ids:
    return ok(@data)
  ids.add(rowid)
  ok(encodePostings(ids))

proc removeRowid*(data: openArray[byte], rowid: uint64): Result[seq[byte]] =
  let decoded = decodePostings(data)
  if not decoded.ok:
    return err[seq[byte]](decoded.err.code, decoded.err.message, decoded.err.context)
  var ids = decoded.value
  ids = ids.filterIt(it != rowid)
  ok(encodePostings(ids))

proc intersectPostings*(lists: seq[seq[uint64]]): seq[uint64] =
  if lists.len == 0:
    return @[]
  var sorted = lists
  sorted.sort(proc(a, b: seq[uint64]): int = cmp(a.len, b.len))
  var resultSet = sorted[0]
  for i in 1 ..< sorted.len:
    var next: seq[uint64] = @[]
    var i1 = 0
    var i2 = 0
    let b = sorted[i]
    while i1 < resultSet.len and i2 < b.len:
      if resultSet[i1] == b[i2]:
        next.add(resultSet[i1])
        i1.inc
        i2.inc
      elif resultSet[i1] < b[i2]:
        i1.inc
      else:
        i2.inc
    resultSet = next
    if resultSet.len == 0:
      break
  resultSet
