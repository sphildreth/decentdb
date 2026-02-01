import unittest
import os

import engine
import record/record
import storage/storage

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Storage More":
  test "normalizeValues promotes large text to overflow":
    let path = makeTempDb("decentdb_storage_normalize.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let bigLen = int(db.pageSize) * 2
    var payload: seq[byte] = newSeq[byte](bigLen)
    var s: uint32 = 0x12345678'u32
    for i in 0 ..< payload.len:
      s = s * 1103515245'u32 + 12345'u32
      payload[i] = byte((s shr 24) and 0xFF'u32)
    let val = Value(kind: vkText, bytes: payload)
    let res = normalizeValues(db.pager, @[val])
    check res.ok
    check res.value[0].kind in {vkTextOverflow, vkTextCompressed, vkTextCompressedOverflow}

    let rec = encodeRecord(res.value)
    let decoded = decodeRecordWithOverflow(db.pager, rec)
    check decoded.ok
    check decoded.value.len == 1
    check decoded.value[0].kind == vkText
    check decoded.value[0].bytes == payload

    let smallRes = normalizeValues(db.pager, @[Value(kind: vkText, bytes: @['a'.byte])])
    check smallRes.ok
    check smallRes.value[0].kind == vkText

    discard closeDb(db)

  test "indexSeek reports missing table/index errors":
    let path = makeTempDb("decentdb_storage_errors.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let errTable = indexSeek(db.pager, db.catalog, "missing", "id", Value(kind: vkInt64, int64Val: 1))
    check not errTable.ok

    discard closeDb(db)
