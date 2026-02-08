import unittest
import os
import engine
import pager/pager
import record/record

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
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

  test "compact int64 encoding":
    # Small positive — uses compact vkInt1 kind + zero-length payload
    var v = Value(kind: vkInt64, int64Val: 1)
    var enc = encodeValue(v)
    check enc.len == 2  # kind + length(0)
    
    # Small negative — still uses full format
    v = Value(kind: vkInt64, int64Val: -1)
    enc = encodeValue(v)
    # 1 byte kind + 1 byte len + 1 byte payload (zigzag(-1) = 1)
    check enc.len == 3

    # Zero — uses compact vkInt0 kind + zero-length payload
    v = Value(kind: vkInt64, int64Val: 0)
    enc = encodeValue(v)
    check enc.len == 2  # kind + length(0)

    # Large value (int64.high)
    v = Value(kind: vkInt64, int64Val: int64.high)
    enc = encodeValue(v)
    # 1 kind + 1 len + 10 payload (max varint) = 12 bytes? 
    # Zigzag(high) is huge, so 10 bytes varint.
    # Len of payload is 10, which fits in 1 byte varint (values < 128).
    check enc.len == 12

    # Roundtrip check for boundary values
    let boundaries = @[
      0'i64, 1, -1, 63, -64, 64, -65, # 1 byte boundaries (zigzag)
      127, -128, 128, -129,           # varint boundaries
      int64.high, int64.low
    ]
    for val in boundaries:
      let rec = @[Value(kind: vkInt64, int64Val: val)]
      let e = encodeRecord(rec)
      let d = decodeRecord(e)
      check d.ok
      check d.value[0].int64Val == val

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
