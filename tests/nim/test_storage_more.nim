import unittest
import os

import engine
import record/record
import storage/storage

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
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
    for i in 0 ..< payload.len:
      payload[i] = byte(i and 0xFF)
    let val = Value(kind: vkText, bytes: payload)
    let res = normalizeValues(db.pager, @[val])
    check res.ok
    check res.value[0].kind == vkTextOverflow

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
