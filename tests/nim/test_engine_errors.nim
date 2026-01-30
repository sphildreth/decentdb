import unittest
import os
import strutils

import engine
import catalog/catalog
import record/record
import storage/storage
import search/search
import vfs/os_vfs
import vfs/faulty_vfs
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "Engine Constraint Error Paths":
  test "NOT NULL constraint violation error":
    let path = makeTempDb("decentdb_engine_notnull.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)")
    
    let insertNull = execSql(db, "INSERT INTO users VALUES (1, NULL)")
    check not insertNull.ok
    check insertNull.err.code == ERR_CONSTRAINT
    check insertNull.err.message.find("NOT NULL") >= 0
    
    discard closeDb(db)

  test "UNIQUE constraint violation error":
    let path = makeTempDb("decentdb_engine_unique.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64 PRIMARY KEY, value INT64 UNIQUE)")
    discard execSql(db, "INSERT INTO items VALUES (1, 100)")
    
    let insertDup = execSql(db, "INSERT INTO items VALUES (2, 100)")
    check not insertDup.ok
    check insertDup.err.code == ERR_CONSTRAINT
    check insertDup.err.message.find("UNIQUE") >= 0
    
    discard closeDb(db)

  test "FOREIGN KEY constraint violation error":
    let path = makeTempDb("decentdb_engine_fk.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE parent (id INT64 PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64, FOREIGN KEY (parent_id) REFERENCES parent(id))")
    
    let insertBadFk = execSql(db, "INSERT INTO child VALUES (1, 999)")
    check not insertBadFk.ok
    check insertBadFk.err.code == ERR_CONSTRAINT
    check insertBadFk.err.message.find("FOREIGN KEY") >= 0
    
    discard closeDb(db)

  test "FOREIGN KEY RESTRICT violation on delete":
    let path = makeTempDb("decentdb_engine_fk_delete.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE parent (id INT64 PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64, FOREIGN KEY (parent_id) REFERENCES parent(id))")
    discard execSql(db, "INSERT INTO parent VALUES (100)")
    discard execSql(db, "INSERT INTO child VALUES (1, 100)")
    
    let deleteParent = execSql(db, "DELETE FROM parent WHERE id = 100")
    check not deleteParent.ok
    check deleteParent.err.code == ERR_CONSTRAINT
    check deleteParent.err.message.find("RESTRICT") >= 0
    
    discard closeDb(db)

suite "Engine Open Error Paths":
  test "openDb with read error on header":
    let path = makeTempDb("decentdb_engine_badheader.db")
    
    # Create file with corrupt header
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    discard vfs.write(file, 0, @[byte(0xFF), byte(0xFF)])
    discard vfs.close(file)
    
    # Try to open - should fail with corruption error
    let dbRes = openDb(path)
    check not dbRes.ok
    check dbRes.err.code == ERR_CORRUPTION
    
  test "openDb with short header":
    let path = makeTempDb("decentdb_engine_shortheader.db")
    
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    discard vfs.write(file, 0, @[byte('D'), byte('E'), byte('D')])
    discard vfs.close(file)
    
    let dbRes = openDb(path)
    check not dbRes.ok
    check dbRes.err.code == ERR_CORRUPTION
    
  test "openDb with unsupported format version":
    let path = makeTempDb("decentdb_engine_badversion.db")
    
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    var header = newSeq[byte](100)
    header[0] = 'D'
    header[1] = 'B'
    header[2] = 0xFF
    header[3] = 0xFF
    discard vfs.write(file, 0, header)
    discard vfs.close(file)
    
    let dbRes = openDb(path)
    check not dbRes.ok
    check dbRes.err.code == ERR_CORRUPTION

suite "Engine Transaction Error Paths":
  test "transaction with rollback on error":
    let path = makeTempDb("decentdb_engine_rollback.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64 PRIMARY KEY, value INT64)")
    discard execSql(db, "INSERT INTO items VALUES (1, 100)")
    
    let txRes = db.beginTransaction()
    check txRes.ok
    
    # Insert a duplicate key
    let badInsert = execSql(db, "INSERT INTO items VALUES (1, 200)")
    check not badInsert.ok
    
    # Rollback
    check db.rollbackTransaction().ok
    
    # Verify original data still there
    let queryRes = execSql(db, "SELECT * FROM items")
    check queryRes.ok
    check queryRes.value.len == 1
    
    discard closeDb(db)

  test "typeCheckValue for all column types":
    # Int64 column with valid values
    check typeCheckValue(ctInt64, Value(kind: vkInt64, int64Val: 1)).ok
    check typeCheckValue(ctInt64, Value(kind: vkNull)).ok
    check not typeCheckValue(ctInt64, Value(kind: vkText, bytes: toBytes("bad"))).ok
    
    # Bool column
    check typeCheckValue(ctBool, Value(kind: vkBool, boolVal: true)).ok
    check typeCheckValue(ctBool, Value(kind: vkNull)).ok
    check not typeCheckValue(ctBool, Value(kind: vkInt64, int64Val: 1)).ok
    
    # Float64 column (accepts int64)
    check typeCheckValue(ctFloat64, Value(kind: vkFloat64, float64Val: 1.5)).ok
    check typeCheckValue(ctFloat64, Value(kind: vkInt64, int64Val: 1)).ok
    check typeCheckValue(ctFloat64, Value(kind: vkNull)).ok
    check not typeCheckValue(ctFloat64, Value(kind: vkText, bytes: toBytes("bad"))).ok
    
    # Text column
    check typeCheckValue(ctText, Value(kind: vkText, bytes: toBytes("hello"))).ok
    check typeCheckValue(ctText, Value(kind: vkNull)).ok
    check not typeCheckValue(ctText, Value(kind: vkInt64, int64Val: 1)).ok
    
    # Blob column
    check typeCheckValue(ctBlob, Value(kind: vkBlob, bytes: @[1'u8, 2'u8])).ok
    check typeCheckValue(ctBlob, Value(kind: vkNull)).ok
    check not typeCheckValue(ctBlob, Value(kind: vkInt64, int64Val: 1)).ok

  test "valuesEqual for all types":
    check valuesEqual(Value(kind: vkNull), Value(kind: vkNull))
    check not valuesEqual(Value(kind: vkNull), Value(kind: vkInt64, int64Val: 0))
    
    check valuesEqual(Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 1))
    check not valuesEqual(Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 2))
    
    check valuesEqual(Value(kind: vkBool, boolVal: true), Value(kind: vkBool, boolVal: true))
    check not valuesEqual(Value(kind: vkBool, boolVal: true), Value(kind: vkBool, boolVal: false))
    
    check valuesEqual(Value(kind: vkFloat64, float64Val: 1.5), Value(kind: vkFloat64, float64Val: 1.5))
    check not valuesEqual(Value(kind: vkFloat64, float64Val: 1.5), Value(kind: vkFloat64, float64Val: 2.5))
    
    check valuesEqual(Value(kind: vkText, bytes: toBytes("hello")), Value(kind: vkText, bytes: toBytes("hello")))
    check not valuesEqual(Value(kind: vkText, bytes: toBytes("hello")), Value(kind: vkText, bytes: toBytes("world")))
    
    check valuesEqual(Value(kind: vkBlob, bytes: @[1'u8, 2'u8]), Value(kind: vkBlob, bytes: @[1'u8, 2'u8]))
    check not valuesEqual(Value(kind: vkBlob, bytes: @[1'u8, 2'u8]), Value(kind: vkBlob, bytes: @[1'u8, 3'u8]))
