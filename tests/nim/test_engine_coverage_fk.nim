import unittest
import os
import strutils
import options
import engine
import errors
import record/record
import catalog/catalog
import sql/binder
import sql/sql

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Engine enforceRestrictOnParent Coverage":
  test "ON UPDATE RESTRICT violation":
    let path = makeTempDb("fk_restrict.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON UPDATE RESTRICT)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    
    let updRes = execSql(db, "UPDATE parent SET id = 2 WHERE id = 1")
    check not updRes.ok
    check updRes.err.code == ERR_CONSTRAINT
    check "FOREIGN KEY RESTRICT violation" in updRes.err.message
    discard closeDb(db)

  test "ON UPDATE CASCADE success":
    let path = makeTempDb("fk_cascade.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON UPDATE CASCADE)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    
    let updRes = execSql(db, "UPDATE parent SET id = 2 WHERE id = 1")
    check updRes.ok
    
    let selRes = execSqlRows(db, "SELECT pid FROM child WHERE id = 10", @[])
    check selRes.ok
    check selRes.value[0].values[0].int64Val == 2
    discard closeDb(db)

  test "ON UPDATE SET NULL success":
    let path = makeTempDb("fk_setnull.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON UPDATE SET NULL)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    
    let updRes = execSql(db, "UPDATE parent SET id = 2 WHERE id = 1")
    check updRes.ok
    
    let selRes = execSqlRows(db, "SELECT pid FROM child WHERE id = 10", @[])
    check selRes.ok
    check selRes.value[0].values[0].kind == vkNull
    discard closeDb(db)

