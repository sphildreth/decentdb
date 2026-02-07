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


