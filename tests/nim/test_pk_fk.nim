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

suite "Primary Key Optimization - Foreign Keys":
  test "FK works with INT64 PRIMARY KEY (Optimized)":
    let dbPath = "test_pk_fk.ddb"
    var db = createTestDb(dbPath)
    defer:
      discard closeDb(db)
      removeFile(dbPath)
    
    # 1. Create tables
    let pRes = execSql(db, "CREATE TABLE parents (id INT64 PRIMARY KEY, name TEXT)")
    check pRes.ok
    if not pRes.ok: echo "Create parents failed: ", pRes.err.message

    let cRes = execSql(db, "CREATE TABLE children (id INT64 PRIMARY KEY, pid INT64 REFERENCES parents(id))")
    check cRes.ok
    if not cRes.ok: echo "Create children failed: ", cRes.err.message

    # 2. Insert parent
    check execSql(db, "INSERT INTO parents (id, name) VALUES (10, 'parent1')").ok

    # 3. Insert child with valid parent
    let validRes = execSql(db, "INSERT INTO children (id, pid) VALUES (100, 10)")
    check validRes.ok

    # 4. Insert child with invalid parent
    let invalidRes = execSql(db, "INSERT INTO children (id, pid) VALUES (200, 99)")
    check not invalidRes.ok
    check invalidRes.err.code == ERR_CONSTRAINT
    check invalidRes.err.message == "FOREIGN KEY constraint failed"

  test "FK Restrict on Delete":
    let dbPath = "test_pk_fk_restrict.db"
    var db = createTestDb(dbPath)
    defer:
      discard closeDb(db)
      removeFile(dbPath)

    check execSql(db, "CREATE TABLE parents (id INT64 PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE children (pid INT64 REFERENCES parents(id))").ok

    check execSql(db, "INSERT INTO parents (id) VALUES (10)").ok
    check execSql(db, "INSERT INTO children (pid) VALUES (10)").ok

    # Delete parent should fail
    let delRes = execSql(db, "DELETE FROM parents WHERE id = 10")
    check not delRes.ok
    check delRes.err.message == "FOREIGN KEY RESTRICT violation"
