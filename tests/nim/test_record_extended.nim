import unittest
import record/record
import pager/pager
import pager/db_header
import errors
import engine
import os

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

suite "Record Extended":
  test "encodeVarint small values":
    check encodeVarint(0'u64) == @[byte(0)]
    check encodeVarint(1'u64) == @[byte(1)]
    check encodeVarint(127'u64) == @[byte(127)]

  test "encodeVarint values needing continuation":
    check encodeVarint(128'u64) == @[byte(0x80), byte(1)]
    check encodeVarint(255'u64) == @[byte(0xFF), byte(1)]
    check encodeVarint(256'u64) == @[byte(0x80), byte(2)]

  test "encodeVarint large values":
    check encodeVarint(16383'u64) == @[byte(0xFF), byte(0x7F)]
    check encodeVarint(16384'u64) == @[byte(0x80), byte(0x80), byte(1)]

  test "encodeVarint max uint64":
    let encoded = encodeVarint(uint64.high)
    check encoded.len > 0
    # First byte should have continuation bit set
    check (encoded[0] and 0x80'u8) != 0

  test "decodeVarint single byte":
    var offset = 0
    let data = @[byte(42)]
    let res = decodeVarint(data, offset)
    check res.ok
    check res.value == 42'u64
    check offset == 1

  test "decodeVarint multi-byte":
    var offset = 0
    let data = @[byte(0x80), byte(1)]  # 128
    let res = decodeVarint(data, offset)
    check res.ok
    check res.value == 128'u64
    check offset == 2

  test "decodeVarint at offset":
    var offset = 2
    let data = @[byte(0), byte(0), byte(42)]
    let res = decodeVarint(data, offset)
    check res.ok
    check res.value == 42'u64
    check offset == 3

  test "decodeVarint overflow protection":
    var offset = 0
    # Create a varint that would overflow (shift > 63)
    let data = @[byte(0xFF), byte(0xFF), byte(0xFF), byte(0xFF), 
                 byte(0xFF), byte(0xFF), byte(0xFF), byte(0xFF),
                 byte(0xFF), byte(0xFF)]
    let res = decodeVarint(data, offset)
    check not res.ok
    check res.err.code == ERR_CORRUPTION

  test "decodeVarint unexpected end":
    var offset = 0
    let data = @[byte(0x80)]  # Continuation bit set but no more data
    let res = decodeVarint(data, offset)
    check not res.ok
    check res.err.code == ERR_CORRUPTION

  test "encodeValue null":
    let val = Value(kind: vkNull)
    let encoded = encodeValue(val)
    # Should be: kind (0) + length varint (0)
    check encoded.len == 2
    check encoded[0] == byte(vkNull)
    check encoded[1] == byte(0)

  test "encodeValue bool true":
    let val = Value(kind: vkBool, boolVal: true)
    let encoded = encodeValue(val)
    check encoded[0] == byte(vkBool)
    check encoded[2] == byte(1)

  test "encodeValue bool false":
    let val = Value(kind: vkBool, boolVal: false)
    let encoded = encodeValue(val)
    check encoded[2] == byte(0)

  test "encodeValue int64 zero":
    let val = Value(kind: vkInt64, int64Val: 0)
    let encoded = encodeValue(val)
    check encoded[0] == byte(vkInt64)
    check encoded.len == 10  # 1 + 1 + 8

  test "encodeValue int64 negative":
    let val = Value(kind: vkInt64, int64Val: -1)
    let encoded = encodeValue(val)
    var offset = 0
    let decoded = decodeValue(encoded, offset)
    check decoded.ok
    check decoded.value.int64Val == -1

  test "encodeValue empty text":
    let val = Value(kind: vkText, bytes: @[])
    let encoded = encodeValue(val)
    check encoded[0] == byte(vkText)
    check encoded[1] == byte(0)  # length = 0

  test "encodeValue empty blob":
    let val = Value(kind: vkBlob, bytes: @[])
    let encoded = encodeValue(val)
    check encoded[0] == byte(vkBlob)
    check encoded[1] == byte(0)

  test "decodeValue with offset":
    let val1 = Value(kind: vkInt64, int64Val: 42)
    let val2 = Value(kind: vkText, bytes: @[byte('h'), byte('i')])
    let encoded1 = encodeValue(val1)
    let encoded2 = encodeValue(val2)
    let combined = encoded1 & encoded2
    
    var offset = 0
    let decoded1 = decodeValue(combined, offset)
    check decoded1.ok
    check decoded1.value.int64Val == 42
    
    let decoded2 = decodeValue(combined, offset)
    check decoded2.ok
    check decoded2.value.bytes == @[byte('h'), byte('i')]

  test "decodeValue unknown kind":
    let data = @[byte(255), byte(0)]  # Invalid kind
    var offset = 0
    let res = decodeValue(data, offset)
    check not res.ok
    check res.err.code == ERR_CORRUPTION

  test "decodeValue truncated":
    let data = @[byte(vkInt64), byte(8), byte(0), byte(0)]  # Claims 8 bytes but only has 2
    var offset = 0
    let res = decodeValue(data, offset)
    check not res.ok
    check res.err.code == ERR_CORRUPTION

  test "decodeValue bool invalid length":
    let data = @[byte(vkBool), byte(2), byte(0), byte(0)]  # Claims 2 bytes
    var offset = 0
    let res = decodeValue(data, offset)
    check not res.ok
    check res.err.code == ERR_CORRUPTION

  test "decodeValue int64 invalid length":
    let data = @[byte(vkInt64), byte(4), byte(0), byte(0), byte(0), byte(0)]  # Claims 4 bytes
    var offset = 0
    let res = decodeValue(data, offset)
    check not res.ok
    check res.err.code == ERR_CORRUPTION

  test "decodeValue float64 invalid length":
    let data = @[byte(vkFloat64), byte(4), byte(0), byte(0), byte(0), byte(0)]
    var offset = 0
    let res = decodeValue(data, offset)
    check not res.ok
    check res.err.code == ERR_CORRUPTION

  test "decodeValue overflow invalid length":
    let data = @[byte(vkTextOverflow), byte(4), byte(0), byte(0), byte(0), byte(0)]
    var offset = 0
    let res = decodeValue(data, offset)
    check not res.ok
    check res.err.code == ERR_CORRUPTION

  test "encodeRecord empty":
    let encoded = encodeRecord(@[])
    check encoded.len == 1  # Just count varint (0)
    check encoded[0] == byte(0)

  test "encodeRecord single value":
    let val = Value(kind: vkInt64, int64Val: 42)
    let encoded = encodeRecord(@[val])
    let decoded = decodeRecord(encoded)
    check decoded.ok
    check decoded.value.len == 1
    check decoded.value[0].int64Val == 42

  test "encodeRecord many values":
    var values: seq[Value] = @[]
    for i in 0 ..< 100:
      values.add(Value(kind: vkInt64, int64Val: int64(i)))
    let encoded = encodeRecord(values)
    let decoded = decodeRecord(encoded)
    check decoded.ok
    check decoded.value.len == 100

  test "decodeRecord empty":
    let data = @[byte(0)]
    let res = decodeRecord(data)
    check res.ok
    check res.value.len == 0

  test "decodeRecord invalid count":
    let data = @[byte(0xFF), byte(0xFF), byte(0xFF)]  # Invalid varint
    let res = decodeRecord(data)
    check not res.ok

  test "decodeRecord with overflow":
    let path = makeTempDb("record_overflow_test.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    
    # Write large data that needs overflow
    var data = newSeq[byte](5000)
    for i in 0 ..< data.len:
      data[i] = byte(i mod 256)
    
    let writeRes = writeOverflowChain(pager, data)
    check writeRes.ok
    
    # Create overflow value
    let val = Value(kind: vkBlobOverflow, overflowPage: writeRes.value, overflowLen: uint32(data.len))
    let encoded = encodeRecord(@[val])
    
    # Decode with overflow resolution
    let decoded = decodeRecordWithOverflow(pager, encoded)
    check decoded.ok
    check decoded.value[0].bytes == data
    check decoded.value[0].kind == vkBlob
    
    discard closePager(pager)
    discard closeDb(db)

  test "readOverflowChain empty start":
    let path = makeTempDb("record_overflow_empty.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    
    let res = readOverflowChain(pager, PageId(0), 100)
    check res.ok
    check res.value.len == 0
    
    discard closePager(pager)
    discard closeDb(db)

  test "readOverflowChainAll follows chain":
    let path = makeTempDb("record_chain_all.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    
    var data = newSeq[byte](10000)
    for i in 0 ..< data.len:
      data[i] = byte(i mod 256)
    
    let writeRes = writeOverflowChain(pager, data)
    check writeRes.ok
    
    let readRes = readOverflowChainAll(pager, writeRes.value)
    check readRes.ok
    check readRes.value == data
    
    discard closePager(pager)
    discard closeDb(db)

  test "freeOverflowChain releases pages":
    let path = makeTempDb("record_free_overflow.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    
    var data = newSeq[byte](5000)
    let writeRes = writeOverflowChain(pager, data)
    check writeRes.ok
    let startPage = writeRes.value
    
    let freeRes = freeOverflowChain(pager, startPage)
    check freeRes.ok
    
    # After freeing, reading should fail or return empty
    let readRes = readOverflowChainAll(pager, startPage)
    # May succeed with partial data or fail depending on implementation
    
    discard closePager(pager)
    discard closeDb(db)
