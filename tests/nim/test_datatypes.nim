import unittest
import os
import strutils
import options

import ../../src/engine
import ../../src/exec/exec
import ../../src/record/record
import ../../src/sql/sql

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path): removeFile(path)
  if fileExists(path & "-wal"): removeFile(path & "-wal")
  path

proc textValue(text: string): Value =
  var bytes: seq[byte] = @[]
  for ch in text:
    bytes.add(byte(ch))
  Value(kind: vkText, bytes: bytes)

proc setupDummy(db: Db) =
  let c = execSql(db, "CREATE TABLE dummy (id INT)", @[])
  if not c.ok: echo "Dummy Create Error: ", c.err.message
  let i = execSql(db, "INSERT INTO dummy VALUES (1)", @[])
  if not i.ok: echo "Dummy Insert Error: ", i.err.message

suite "Datatypes - UUID":
  test "gen_random_uuid":
    let dbPath = makeTempDb("test_uuid_gen.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: 
      discard closeDb(db)
      removeFile(dbPath)
      if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")

    setupDummy(db)
    let res = execSqlRows(db, "SELECT GEN_RANDOM_UUID() FROM dummy", @[])
    if not res.ok:
       echo "Error: ", res.err.message
    check res.ok
    check res.value.len == 1
    let val = res.value[0].values[0]
    check val.kind == vkBlob
    check val.bytes.len == 16
    
    # Check version 4 (0100xxxx)
    check (val.bytes[6] and 0xF0) == 0x40
    # Check variant (10xxxxxx)
    check (val.bytes[8] and 0xC0) == 0x80

  test "uuid parse and string":
    let dbPath = makeTempDb("test_uuid_parse.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    
    setupDummy(db)

    let uuidStr = "550e8400-e29b-41d4-a716-446655440000"
    
    # Parse
    var uuidBytes = newSeq[byte](uuidStr.len)
    if uuidStr.len > 0: copyMem(addr uuidBytes[0], unsafeAddr uuidStr[0], uuidStr.len)
    let res = execSqlRows(db, "SELECT UUID_PARSE($1) FROM dummy", @[Value(kind: vkText, bytes: uuidBytes)])
    if not res.ok:
      echo "UUID_PARSE failed. Code: ", res.err.code, " Msg: ", res.err.message.escape
    check res.ok
    check res.value.len == 1
    let val = res.value[0].values[0]
    check val.kind == vkBlob
    check val.bytes.len == 16
    
    # Stringify
    let res2 = execSqlRows(db, "SELECT UUID_TO_STRING($1) FROM dummy", @[val])
    check res2.ok
    let val2 = res2.value[0].values[0]
    check val2.kind == vkText
    var s2 = newString(val2.bytes.len)
    if val2.bytes.len > 0: copyMem(addr s2[0], unsafeAddr val2.bytes[0], val2.bytes.len)
    check s2 == uuidStr

  test "uuid cast":
    let dbPath = makeTempDb("test_uuid_cast.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    setupDummy(db)

    let res = execSqlRows(db, "SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) FROM dummy", @[])
    check res.ok
    check res.value[0].values[0].kind == vkBlob
    
    # Cast back
    let res2 = execSqlRows(db, "SELECT CAST(CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS TEXT) FROM dummy", @[])
    check res2.ok
    let val = res2.value[0].values[0]
    var s = newString(val.bytes.len)
    if val.bytes.len > 0: copyMem(addr s[0], unsafeAddr val.bytes[0], val.bytes.len)
    check s == "550e8400-e29b-41d4-a716-446655440000"

suite "Datatypes - DECIMAL":
  test "decimal arithmetic":
    let dbPath = makeTempDb("test_decimal.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    setupDummy(db)

    # 1.2 + 2.3 = 3.5
    let res = execSqlRows(db, "SELECT CAST('1.2' AS DECIMAL(10,2)) + CAST('2.3' AS DECIMAL(10,2)) FROM dummy", @[])
    check res.ok
    let val = res.value[0].values[0]
    check val.kind == vkDecimal
    check val.int64Val == 350 # 3.50
    check val.decimalScale == 2

    # Division 10 / 3 -> 3.3333... (scale 2)
    # 10.00 / 3.00 (scale 2) -> max scale 2.
    let res2 = execSqlRows(db, "SELECT CAST('10.00' AS DECIMAL(10,2)) / CAST('3.00' AS DECIMAL(10,2)) FROM dummy", @[])
    check res2.ok
    let val2 = res2.value[0].values[0]
    check val2.kind == vkDecimal
    check val2.int64Val == 333
    check val2.decimalScale == 2

  test "decimal storage and table":
    let dbPath = makeTempDb("test_decimal_table.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)

    let createRes = execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, amount DECIMAL(10,2))", @[])
    if not createRes.ok: echo "Create Error: ", createRes.err.message
    check createRes.ok
    
    let ins1 = execSql(db, "INSERT INTO t VALUES (1, CAST('12.34' AS DECIMAL(10,2)))", @[])
    if not ins1.ok: echo "Insert1 Error: ", ins1.err.message
    check ins1.ok
    
    let ins2 = execSql(db, "INSERT INTO t VALUES (2, CAST('5.6' AS DECIMAL(10,2)))", @[])
    if not ins2.ok: echo "Insert2 Error: ", ins2.err.message
    check ins2.ok # Should coerce 5.6 to 5.60
    
    let res = execSqlRows(db, "SELECT amount FROM t ORDER BY id", @[])
    check res.ok
    check res.value[0].values[0].int64Val == 1234
    check res.value[1].values[0].int64Val == 560
    check res.value[1].values[0].decimalScale == 2

  test "decimal comparison":
    let dbPath = makeTempDb("test_decimal_cmp.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    setupDummy(db)

    let res = execSqlRows(db, "SELECT CAST('1.2' AS DECIMAL(10,2)) = CAST('1.20' AS DECIMAL(10,2)) FROM dummy", @[])
    check res.ok
    check res.value[0].values[0].boolVal == true
    
    let res2 = execSqlRows(db, "SELECT CAST('1.2' AS DECIMAL(10,2)) < CAST('1.21' AS DECIMAL(10,2)) FROM dummy", @[])
    check res2.ok
    check res2.value[0].values[0].boolVal == true

  test "decimal casts":
    let dbPath = makeTempDb("test_decimal_casts.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    setupDummy(db)

    # To Text
    let resTxt = execSqlRows(db, "SELECT CAST(CAST('12.34' AS DECIMAL(10,2)) AS TEXT) FROM dummy", @[])
    check resTxt.ok
    check valueToString(resTxt.value[0].values[0]) == "12.34"

    let resTxtNeg = execSqlRows(db, "SELECT CAST(CAST('-0.05' AS DECIMAL(10,2)) AS TEXT) FROM dummy", @[])
    check resTxtNeg.ok
    check valueToString(resTxtNeg.value[0].values[0]) == "-0.05"

    # To Int (Truncate)
    let resInt = execSqlRows(db, "SELECT CAST(CAST('12.34' AS DECIMAL(10,2)) AS INT) FROM dummy", @[])
    check resInt.ok
    check resInt.value[0].values[0].int64Val == 12

    let resIntNeg = execSqlRows(db, "SELECT CAST(CAST('-12.99' AS DECIMAL(10,2)) AS INT) FROM dummy", @[])
    check resIntNeg.ok
    check resIntNeg.value[0].values[0].int64Val == -13 # Rounded away from zero/nearest depending on impl (Nim div is trunc, scaleDecimal rounds)

    # To Float
    let resFloat = execSqlRows(db, "SELECT CAST(CAST('12.5' AS DECIMAL(10,1)) AS FLOAT) FROM dummy", @[])
    check resFloat.ok
    check resFloat.value[0].values[0].float64Val == 12.5

    # To Bool
    let resBoolTrue = execSqlRows(db, "SELECT CAST(CAST('0.01' AS DECIMAL(10,2)) AS BOOL) FROM dummy", @[])
    check resBoolTrue.ok
    check resBoolTrue.value[0].values[0].boolVal == true

    let resBoolFalse = execSqlRows(db, "SELECT CAST(CAST('0.00' AS DECIMAL(10,2)) AS BOOL) FROM dummy", @[])
    check resBoolFalse.ok
    check resBoolFalse.value[0].values[0].boolVal == false

  test "decimal edge cases":
    let dbPath = makeTempDb("test_decimal_edges.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    setupDummy(db)

    # Zero
    let resZero = execSqlRows(db, "SELECT CAST('0' AS DECIMAL(10,2)) FROM dummy", @[])
    check resZero.ok
    check resZero.value[0].values[0].int64Val == 0
    check valueToString(resZero.value[0].values[0]) == "0.00"

    # Scale up
    let resScaleUp = execSqlRows(db, "SELECT CAST(CAST('1.2' AS DECIMAL(10,1)) AS DECIMAL(10,3)) FROM dummy", @[])
    check resScaleUp.ok
    check resScaleUp.value[0].values[0].int64Val == 1200 # 1.200
    check resScaleUp.value[0].values[0].decimalScale == 3

    # Scale down (round/truncate? impl uses rounding logic in scaleDecimal)
    # 1.25 -> 1.3 (if round half up?) or 1.2?
    # scaleDecimal logic: if abs(rem) * 2 >= divi: inc
    # 125 (scale 2) -> scale 1. divi=10. rem=5. 5*2 >= 10. inc. -> 13.
    let resScaleDown = execSqlRows(db, "SELECT CAST(CAST('1.25' AS DECIMAL(10,2)) AS DECIMAL(10,1)) FROM dummy", @[])
    check resScaleDown.ok
    check resScaleDown.value[0].values[0].int64Val == 13 # 1.3


  test "syntax error message":
    let dbPath = makeTempDb("test_syntax_error.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    
    let res = execSqlRows(db, "SELECT ?", @[])
    check not res.ok
    # Expect readable error message
    check "syntax error" in res.err.message or "at or near" in res.err.message

suite "Datatypes - Basic":
  test "integer aliases and limits":
    let dbPath = makeTempDb("test_int.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)

    let c = execSql(db, """
      CREATE TABLE ints (
        id INT PRIMARY KEY,
        v1 INTEGER,
        v2 INT64
      )
    """, @[])
    check c.ok
    
    # Test insertion of large integers
    let maxInt = 9223372036854775807'i64
    let minInt = -9223372036854775808'i64 # Nim literal quirk: this might need check
    
    # Insert via params
    let i1 = execSql(db, "INSERT INTO ints VALUES (1, $1, $2)", 
                     @[Value(kind: vkInt64, int64Val: maxInt), Value(kind: vkInt64, int64Val: minInt)])
    check i1.ok
    
    let s1 = execSqlRows(db, "SELECT v1, v2 FROM ints WHERE id = 1", @[])
    check s1.ok
    check s1.value[0].values[0].int64Val == maxInt
    check s1.value[0].values[1].int64Val == minInt
    
    # Verify metadata aliases (if exposed via introspection, or just behavior)
    # Just checking insert/select behavior works for all aliases is sufficient here.

  test "text aliases and unicode":
    let dbPath = makeTempDb("test_text.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)

    let c = execSql(db, """
      CREATE TABLE texts (
        id INT PRIMARY KEY,
        t1 TEXT,
        t2 VARCHAR(10),
        t3 CHARACTER VARYING
      )
    """, @[])
    check c.ok
    
    let unicodeStr = "Hello 🌍! 🚀"
    let i1 = execSql(db, "INSERT INTO texts VALUES (1, $1, $2, $3)", 
                     @[textValue(unicodeStr), textValue(unicodeStr), textValue(unicodeStr)])
    check i1.ok
    
    # VARCHAR limit is currently ignored by engine (as per docs), verify it accepts longer string
    let longStr = "123456789012345"
    let i2 = execSql(db, "INSERT INTO texts VALUES (2, $1, $2, $3)", 
                     @[textValue("short"), textValue(longStr), textValue("short")])
    check i2.ok # Should succeed even though t2 is VARCHAR(10)
    
    let s1 = execSqlRows(db, "SELECT t1 FROM texts WHERE id = 1", @[])
    check s1.ok
    check valueToString(s1.value[0].values[0]) == unicodeStr

  test "blob storage":
    let dbPath = makeTempDb("test_blob.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)

    let c = execSql(db, "CREATE TABLE blobs (id INT PRIMARY KEY, b BLOB)", @[])
    check c.ok
    
    var data = newSeq[byte](256)
    for i in 0..255: data[i] = byte(i)
    
    let i1 = execSql(db, "INSERT INTO blobs VALUES (1, $1)", @[Value(kind: vkBlob, bytes: data)])
    check i1.ok
    
    let s1 = execSqlRows(db, "SELECT b FROM blobs WHERE id = 1", @[])
    check s1.ok
    check s1.value[0].values[0].kind == vkBlob
    check s1.value[0].values[0].bytes == data

  test "boolean aliases and casting":
    let dbPath = makeTempDb("test_bool.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)

    let c = execSql(db, """
      CREATE TABLE bools (
        id INT PRIMARY KEY,
        b1 BOOLEAN,
        b2 BOOL
      )
    """, @[])
    check c.ok
    
    let i1 = execSql(db, "INSERT INTO bools VALUES (1, CAST(1 AS BOOL), CAST(0 AS BOOL))", @[])
    check i1.ok
    
    let s1 = execSqlRows(db, "SELECT b1, b2 FROM bools WHERE id = 1", @[])
    check s1.ok
    check s1.value[0].values[0].boolVal == true
    check s1.value[0].values[1].boolVal == false
    
    # Casting strings
    let c1 = execSqlRows(db, "SELECT CAST('true' AS BOOL), CAST('T' AS BOOL), CAST('1' AS BOOL)", @[])
    check c1.ok
    check c1.value[0].values[0].boolVal == true
    check c1.value[0].values[1].boolVal == true
    check c1.value[0].values[2].boolVal == true
    
    let c2 = execSqlRows(db, "SELECT CAST('false' AS BOOL), CAST('F' AS BOOL), CAST('0' AS BOOL)", @[])
    check c2.ok
    check c2.value[0].values[0].boolVal == false
    check c2.value[0].values[1].boolVal == false
    check c2.value[0].values[2].boolVal == false

  test "float aliases":
    let dbPath = makeTempDb("test_float.ddb")
    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)

    let c = execSql(db, """
      CREATE TABLE floats (
        id INT PRIMARY KEY,
        f1 FLOAT,
        f2 FLOAT64,
        f3 REAL,
        f4 DOUBLE
      )
    """, @[])
    check c.ok
    
    let val = 1.23456789
    let i1 = execSql(db, "INSERT INTO floats VALUES (1, $1, $1, $1, $1)", 
                     @[Value(kind: vkFloat64, float64Val: val)])
    check i1.ok
    
    let s1 = execSqlRows(db, "SELECT f1, f2, f3, f4 FROM floats WHERE id = 1", @[])
    check s1.ok
    for i in 0..3:
      check s1.value[0].values[i].float64Val == val




