import unittest
import os
import strutils
import options
import engine
import record/record
import errors
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

suite "Engine findConflictRowidOnTarget Coverage":
  test "targetCols empty":
    let path = makeTempDb("find_conflict_empty.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    
    let prep = prepare(db, "INSERT INTO t VALUES (1, 'a') ON CONFLICT DO NOTHING")
    check prep.ok
    let execRes = execPrepared(prep.value, @[])
    check execRes.ok

    discard closeDb(db)

  test "targetCols unknown column":
    let path = makeTempDb("find_conflict_unknown.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    
    let prep = prepare(db, "INSERT INTO t VALUES (1, 'a') ON CONFLICT (unknown_col) DO NOTHING")
    check not prep.ok # Usually fails at binder level, but we want to test engine level directly or just make sure it fails

    discard closeDb(db)

  test "targetCols single INT64 PK match":
    let path = makeTempDb("find_conflict_single_pk.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    
    let prep = prepare(db, "INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET val = 'c'")
    check prep.ok
    let execRes = execPrepared(prep.value, @[])
    check execRes.ok
    
    let selRes = execSqlRows(db, "SELECT val FROM t WHERE id = 1", @[])
    if selRes.ok and selRes.value.len > 0:
      check cast[string](selRes.value[0].values[0].bytes) == "c"

    discard closeDb(db)

  test "targetCols composite match":
    let path = makeTempDb("find_conflict_composite.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (a INT, b INT, val TEXT, PRIMARY KEY (a, b))")
    discard execSql(db, "INSERT INTO t VALUES (1, 2, 'a')")
    
    let prep = prepare(db, "INSERT INTO t VALUES (1, 2, 'b') ON CONFLICT (a, b) DO UPDATE SET val = 'c'")
    check prep.ok
    let execRes = execPrepared(prep.value, @[])
    check execRes.ok
    
    let selRes = execSqlRows(db, "SELECT val FROM t WHERE a = 1 AND b = 2", @[])
    if selRes.ok and selRes.value.len > 0:
      check cast[string](selRes.value[0].values[0].bytes) == "c"

    discard closeDb(db)

  test "targetCols composite no index match":
    let path = makeTempDb("find_conflict_composite_no_idx.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (a INT, b INT, val TEXT)")
    
    let prep = prepare(db, "INSERT INTO t VALUES (1, 2, 'b') ON CONFLICT (a, b) DO UPDATE SET val = 'c'")
    check not prep.ok # Binder might catch it, if not engine will.

    discard closeDb(db)
