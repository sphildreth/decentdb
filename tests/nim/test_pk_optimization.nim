import unittest
import os
import options
import engine
import storage/storage
import pager/pager
import catalog/catalog
import record/record
import errors
import sql/sql

proc createTestDb(path: string): Db =
  if fileExists(path):
    removeFile(path)
  let dbRes = openDb(path)
  doAssert dbRes.ok
  dbRes.value

suite "Primary Key Optimization":
  test "INT64 PRIMARY KEY uses rowid directly":
    let dbPath = "test_pk_opt.db"
    var db = createTestDb(dbPath)
    defer:
      discard closeDb(db)
      removeFile(dbPath)
    
    # 1. Create table with INT64 PK
    let createSql = "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)"
    let createRes = execSql(db, createSql)
    check createRes.ok

    # Verify no secondary index created for 'id'
    let indexOpt = db.catalog.getIndexByName("pk_users_id_idx")
    check indexOpt.isNone
    
    # 2. Insert row with specific ID
    let insertSql = "INSERT INTO users (id, name) VALUES (100, 'alice')"
    let insertRes = execSql(db, insertSql)
    if not insertRes.ok:
      echo "Insert failed: ", insertRes.err.message
    check insertRes.ok
    # check insertRes.value == 1 # Rows affected

    # 3. Verify storage location
    # We need to access storage directly to check rowid
    let tableRes = db.catalog.getTable("users")
    check tableRes.ok
    let table = tableRes.value
    
    let rowRes = readRowAt(db.pager, table, 100)
    check rowRes.ok
    check rowRes.value.rowid == 100
    check rowRes.value.values[1].kind == vkText
    # Convert string to bytes for comparison
    var aliceBytes: seq[byte] = @[]
    for c in "alice": aliceBytes.add(byte(c))
    check rowRes.value.values[1].bytes == aliceBytes

    # 4. Insert another row
    check execSql(db, "INSERT INTO users (id, name) VALUES (200, 'bob')").ok
    let row2Res = readRowAt(db.pager, table, 200)
    check row2Res.ok
    
    # 5. Test UPDATE of PK
    # Move alice from 100 to 300
    let updateSql = "UPDATE users SET id = 300 WHERE id = 100"
    let updateRes = execSql(db, updateSql)
    check updateRes.ok
    # check updateRes.value == 1

    # Verify old location empty
    let oldRowRes = readRowAt(db.pager, table, 100)
    check not oldRowRes.ok 

    # Verify new location
    let newRowRes = readRowAt(db.pager, table, 300)
    check newRowRes.ok
    check newRowRes.value.rowid == 300
    check newRowRes.value.values[1].bytes == aliceBytes

    # 6. Test PK Conflict on Update
    # Try to move bob (200) to 300 (alice)
    let conflictSql = "UPDATE users SET id = 300 WHERE id = 200"
    let conflictRes = execSql(db, conflictSql)
    check not conflictRes.ok
    check conflictRes.err.message == "UNIQUE constraint failed" or conflictRes.err.message == "Unique constraint failed: Primary Key conflict"

  test "Non-INT64 PK still creates index":
    let dbPath = "test_pk_text.db"
    var db = createTestDb(dbPath)
    defer:
      discard closeDb(db)
      removeFile(dbPath)

    check execSql(db, "CREATE TABLE products (code TEXT PRIMARY KEY, price INT64)").ok
    
    # Check index exists
    let indexOpt = db.catalog.getIndexByName("pk_products_code_idx")
    check indexOpt.isSome
    check indexOpt.get.kind == ikBtree
