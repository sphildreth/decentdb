import unittest
import os
import engine
import pager/pager
import record/record

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

suite "Record":
  test "encode/decode basic types":
    let values = @[
      Value(kind: vkNull),
      Value(kind: vkInt64, int64Val: int64.high),
      Value(kind: vkInt64, int64Val: int64.low),
      Value(kind: vkBool, boolVal: true),
      Value(kind: vkBool, boolVal: false),
      Value(kind: vkFloat64, float64Val: 3.14159),
      Value(kind: vkText, bytes: @[byte('h'), byte('e'), byte('l'), byte('l'), byte('o')]),
      Value(kind: vkBlob, bytes: @[byte 1, 2, 3, 4])
    ]
    let encoded = encodeRecord(values)
    let decoded = decodeRecord(encoded)
    check decoded.ok
    let decodedValues = decoded.value
    check decodedValues.len == values.len
    check decodedValues[1].int64Val == int64.high
    check decodedValues[2].int64Val == int64.low
    check decodedValues[3].boolVal == true
    check decodedValues[4].boolVal == false
    check decodedValues[5].float64Val == 3.14159
    check decodedValues[6].bytes == @[byte('h'), byte('e'), byte('l'), byte('l'), byte('o')]
    check decodedValues[7].bytes == @[byte 1, 2, 3, 4]

  test "overflow chain roundtrip":
    let path = makeTempDb("decentdb_record_overflow.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    var data = newSeq[byte](pager.pageSize * 2 + 25)
    for i in 0 ..< data.len:
      data[i] = byte(i mod 251)
    let overflowRes = writeOverflowChain(pager, data)
    check overflowRes.ok
    let value = Value(kind: vkBlobOverflow, overflowPage: overflowRes.value, overflowLen: uint32(data.len))
    let encoded = encodeRecord(@[value])
    let decoded = decodeRecordWithOverflow(pager, encoded)
    check decoded.ok
    check decoded.value.len == 1
    check decoded.value[0].bytes == data
    discard closePager(pager)
    discard closeDb(db)
