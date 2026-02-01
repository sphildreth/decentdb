import unittest
import os
import strutils

import engine
import pager/pager
import pager/db_header
import record/record
import errors

proc makeTempDb(name: string): string =
  let normalizedName =
    if name.len >= 3 and name[name.len - 3 .. ^1] == ".db":
      name[0 .. ^4] & ".ddb"
    else:
      name
  let path = getTempDir() / normalizedName
  if fileExists(path):
    removeFile(path)
  path

suite "Record Comprehensive":
  test "encodeVarint edge cases":
    check encodeVarint(0'u64) == @[byte(0)]
    check encodeVarint(127'u64) == @[byte(127)]  # Max single byte
    check encodeVarint(128'u64) == @[byte(0x80), byte(1)]  # Min two bytes
    check encodeVarint(uint64.high).len > 0  # Should encode max value

  test "decodeVarint with various inputs":
    var offset = 0
    let data = @[byte(0x80), byte(0x80), byte(0x02)]  # 32768
    let result = decodeVarint(data, offset)
    check result.ok
    check result.value == 32768'u64
    check offset == 3

  test "encodeValue with all value types":
    # Test each value type
    let nullVal = Value(kind: vkNull)
    let nullEncoded = encodeValue(nullVal)
    check nullEncoded[0] == byte(vkNull)
    
    let boolVal = Value(kind: vkBool, boolVal: true)
    let boolEncoded = encodeValue(boolVal)
    check boolEncoded[0] == byte(vkBool)
    
    let intVal = Value(kind: vkInt64, int64Val: 123)
    let intEncoded = encodeValue(intVal)
    check intEncoded[0] == byte(vkInt64)
    
    let floatVal = Value(kind: vkFloat64, float64Val: 3.14)
    let floatEncoded = encodeValue(floatVal)
    check floatEncoded[0] == byte(vkFloat64)
    
    let textVal = Value(kind: vkText, bytes: @[byte('h'), byte('i')])
    let textEncoded = encodeValue(textVal)
    check textEncoded[0] == byte(vkText)
    
    let blobVal = Value(kind: vkBlob, bytes: @[byte(1), byte(2)])
    let blobEncoded = encodeValue(blobVal)
    check blobEncoded[0] == byte(vkBlob)

  test "decodeValue with all value types":
    var offset = 0
    
    # Test null
    let nullData = @[byte(vkNull), byte(0)]
    offset = 0
    let nullResult = decodeValue(nullData, offset)
    check nullResult.ok
    check nullResult.value.kind == vkNull
    
    # Test bool
    let boolData = @[byte(vkBool), byte(1), byte(1)]
    offset = 0
    let boolResult = decodeValue(boolData, offset)
    check boolResult.ok
    check boolResult.value.kind == vkBool
    check boolResult.value.boolVal == true
    
    # Test int64
    let intVal = Value(kind: vkInt64, int64Val: 42)
    let intData = encodeValue(intVal)
    offset = 0
    let intResult = decodeValue(intData, offset)
    check intResult.ok
    check intResult.value.kind == vkInt64
    check intResult.value.int64Val == 42

  test "decodeValue with truncated data":
    var offset = 0
    # Valid header but truncated payload
    let data = @[byte(vkInt64), byte(8), byte(0), byte(0)]  # Says 8 bytes but only has 2
    let result = decodeValue(data, offset)
    check not result.ok
    check result.err.code == ERR_CORRUPTION

  test "decodeValue with unknown kind":
    var offset = 0
    let data = @[byte(255), byte(0)]  # Invalid kind
    let result = decodeValue(data, offset)
    check not result.ok
    check result.err.code == ERR_CORRUPTION

  test "encodeRecord with multiple values":
    let values = @[
      Value(kind: vkNull),
      Value(kind: vkInt64, int64Val: 123),
      Value(kind: vkText, bytes: @[byte('t'), byte('e'), byte('s'), byte('t')])
    ]
    let encoded = encodeRecord(values)
    let decoded = decodeRecord(encoded)
    check decoded.ok
    check decoded.value.len == 3
    check decoded.value[0].kind == vkNull
    check decoded.value[1].int64Val == 123
    check decoded.value[2].bytes == @[byte('t'), byte('e'), byte('s'), byte('t')]

  test "decodeRecord with invalid field count":
    let data = @[byte(0xFF), byte(0xFF), byte(0xFF), byte(0xFF), byte(0x0F)]  # Very large varint
    let result = decodeRecord(data)
    check not result.ok

  test "decodeRecord with truncated field":
    let data = @[byte(1), byte(vkInt64), byte(8), byte(0), byte(0)]  # Says 8 bytes but only has 2 after header
    let result = decodeRecord(data)
    check not result.ok

  test "writeOverflowChain with empty data":
    let path = makeTempDb("decentdb_write_overflow_empty.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    let result = writeOverflowChain(pager, @[])
    check result.ok
    check result.value == PageId(0)  # Should return 0 for empty data

    discard closePager(pager)
    discard closeDb(db)

  test "readOverflowChain with zero start page":
    let path = makeTempDb("decentdb_read_overflow_zero.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    let result = readOverflowChain(pager, PageId(0), 100)
    check result.ok
    check result.value.len == 0

    discard closePager(pager)
    discard closeDb(db)

  test "readOverflowChain with invalid page":
    let path = makeTempDb("decentdb_read_overflow_invalid.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    # Try to read from a page that doesn't exist
    let result = readOverflowChain(pager, PageId(999999), 100)
    check not result.ok

    discard closePager(pager)
    discard closeDb(db)

  test "readOverflowChainAll with zero start page":
    let path = makeTempDb("decentdb_read_overflow_all_zero.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    let result = readOverflowChainAll(pager, PageId(0))
    check result.ok
    check result.value.len == 0

    discard closePager(pager)
    discard closeDb(db)

  test "readOverflowChainAll with invalid page":
    let path = makeTempDb("decentdb_read_overflow_all_invalid.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    # Try to read from a page that doesn't exist
    let result = readOverflowChainAll(pager, PageId(999999))
    check not result.ok

    discard closePager(pager)
    discard closeDb(db)

  test "freeOverflowChain with zero start":
    let path = makeTempDb("decentdb_free_overflow_zero.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    let result = freeOverflowChain(pager, PageId(0))
    check result.ok

    discard closePager(pager)
    discard closeDb(db)

  test "freeOverflowChain with invalid page":
    let path = makeTempDb("decentdb_free_overflow_invalid.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    # Try to free a page that doesn't exist
    let result = freeOverflowChain(pager, PageId(999999))
    check not result.ok

    discard closePager(pager)
    discard closeDb(db)

  test "decodeRecordWithOverflow with normal values":
    let values = @[Value(kind: vkInt64, int64Val: 42)]
    let encoded = encodeRecord(values)
    let path = makeTempDb("decentdb_decode_normal.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    let result = decodeRecordWithOverflow(pager, encoded)
    check result.ok
    check result.value.len == 1
    check result.value[0].int64Val == 42

    discard closePager(pager)
    discard closeDb(db)

  test "decodeRecordWithOverflow with invalid record":
    let path = makeTempDb("decentdb_decode_invalid.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    let result = decodeRecordWithOverflow(pager, @[byte(0xFF), byte(0xFF), byte(0xFF)])  # Invalid varint
    check not result.ok

    discard closePager(pager)
    discard closeDb(db)

  test "decodeRecordWithOverflow with overflow read error":
    let path = makeTempDb("decentdb_decode_overflow_error.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value

    # Create a value that references a non-existent overflow page
    let overflowVal = Value(kind: vkTextOverflow, overflowPage: PageId(999999), overflowLen: 100)
    let encoded = encodeRecord(@[overflowVal])

    let result = decodeRecordWithOverflow(pager, encoded)
    check not result.ok  # Should fail when trying to read the overflow page

    discard closePager(pager)
    discard closeDb(db)

  test "encodeValue with overflow types":
    let textOverflowVal = Value(kind: vkTextOverflow, overflowPage: PageId(123), overflowLen: 456)
    let encoded = encodeValue(textOverflowVal)
    check encoded[0] == byte(vkTextOverflow)
    check encoded.len == 10  # 1 kind + 1 length + 8 payload (4 page + 4 len)

    let blobOverflowVal = Value(kind: vkBlobOverflow, overflowPage: PageId(789), overflowLen: 1011)
    let encoded2 = encodeValue(blobOverflowVal)
    check encoded2[0] == byte(vkBlobOverflow)

  test "decodeValue with overflow types":
    var offset = 0
    var data = newSeq[byte](10)  # 1 kind + 1 length + 8 payload
    data[0] = byte(vkTextOverflow)
    data[1] = byte(8)  # Length of payload
    writeU32LE(data, 2, 123)  # Page ID
    writeU32LE(data, 6, 456)  # Length
    let result = decodeValue(data, offset)
    check result.ok
    check result.value.kind == vkTextOverflow
    check result.value.overflowPage == PageId(123)
    check result.value.overflowLen == 456

  test "decodeRecord with empty data":
    let result = decodeRecord(@[])
    check not result.ok

  test "decodeRecord with just count varint":
    let result = decodeRecord(@[byte(1)])  # Says 1 value but no values follow
    check not result.ok