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

suite "Engine Bulk Load Coverage":
  test "bulkLoad successful batches":
    let path = makeTempDb("bulk_load_success.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT UNIQUE)")
    
    var opts = defaultBulkLoadOptions()
    opts.batchSize = 2 # Trigger multiple batches
    
    let rows: seq[seq[Value]] = @[
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: cast[seq[byte]]("a"))],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: cast[seq[byte]]("b"))],
      @[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: cast[seq[byte]]("c"))]
    ]

    let bulkRes = bulkLoad(db, "t", rows, opts)
    check bulkRes.ok

    let cntRes = execSqlRows(db, "SELECT COUNT(*) FROM t", @[])
    check cntRes.ok
    check cntRes.value[0].values[0].int64Val == 3

    discard closeDb(db)

  test "bulkLoad empty batch":
    let path = makeTempDb("bulk_load_empty.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT UNIQUE)")
    
    var opts = defaultBulkLoadOptions()
    let bulkRes = bulkLoad(db, "t", @[], opts)
    check bulkRes.ok

    discard closeDb(db)

  test "bulkLoad type error aborts":
    let path = makeTempDb("bulk_load_typeerr.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT UNIQUE)")
    
    var opts = defaultBulkLoadOptions()
    opts.batchSize = 2
    
    let rows: seq[seq[Value]] = @[
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: cast[seq[byte]]("a"))],
      @[Value(kind: vkText, bytes: cast[seq[byte]]("bad_id")), Value(kind: vkText, bytes: cast[seq[byte]]("b"))] # Type error on second row
    ]

    let bulkRes = bulkLoad(db, "t", rows, opts)
    check not bulkRes.ok
    
    # First row should be rolled back as part of the failed batch/transaction
    let cntRes = execSqlRows(db, "SELECT COUNT(*) FROM t", @[])
    check cntRes.ok
    check cntRes.value[0].values[0].int64Val == 0

    discard closeDb(db)

  test "bulkLoad UNIQUE constraint error aborts":
    let path = makeTempDb("bulk_load_uniqueerr.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT UNIQUE)")
    
    var opts = defaultBulkLoadOptions()
    opts.batchSize = 2
    
    let rows: seq[seq[Value]] = @[
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: cast[seq[byte]]("a"))],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: cast[seq[byte]]("a"))] # Unique error on second row
    ]

    let bulkRes = bulkLoad(db, "t", rows, opts)
    check not bulkRes.ok
    
    let cntRes = execSqlRows(db, "SELECT COUNT(*) FROM t", @[])
    check cntRes.ok
    check cntRes.value[0].values[0].int64Val == 0

    discard closeDb(db)
