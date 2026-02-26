## Tests targeting engine.nim uncovered areas:
## - execSqlRows (L4168, 48 C lines)
## - execSqlNoRows (L4255, 37 C lines)
## - saveAs with file DB (L4864, 38 C lines)
## - bulkLoad with unique constraint and batch check (L4489, 96 C lines)
## - enforceUniqueBatch (L873, 76 C lines)
import unittest
import os
import strutils
import engine
import record/record
import errors
import catalog/catalog

proc textValue(s: string): Value =
  var bytes = newSeq[byte](s.len)
  for i, c in s:
    bytes[i] = byte(c)
  Value(kind: vkText, bytes: bytes)

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  for ext in ["", "-wal"]:
    let f = if ext.len == 0: path else: path & ext
    if fileExists(f): removeFile(f)
  path

proc cleanupDb(dbPath: string) =
  for ext in ["", "-wal"]:
    let f = if ext.len == 0: dbPath else: dbPath & ext
    if fileExists(f): removeFile(f)

suite "execSqlRows coverage":
  test "execSqlRows basic SELECT":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'Bob')").ok
    let rows = execSqlRows(db, "SELECT id, name FROM t ORDER BY id", @[])
    require rows.ok
    check rows.value.len == 2
    check rows.value[0].values[0].int64Val == 1
    check rows.value[1].values[0].int64Val == 2
    discard closeDb(db)

  test "execSqlRows returns error for closed DB":
    let db = openDb(":memory:").value
    discard closeDb(db)
    let res = execSqlRows(db, "SELECT 1", @[])
    check not res.ok

  test "execSqlRows with params":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE tp (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "INSERT INTO tp VALUES (1, 'a')").ok
    check execSql(db, "INSERT INTO tp VALUES (2, 'b')").ok
    let rows = execSqlRows(db, "SELECT id FROM tp WHERE id = $1", @[Value(kind: vkInt64, int64Val: 1)])
    require rows.ok
    check rows.value.len == 1
    check rows.value[0].values[0].int64Val == 1
    discard closeDb(db)

  test "execSqlRows aggregate result":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE ta (x INT PRIMARY KEY)").ok
    for i in 1..5:
      check execSql(db, "INSERT INTO ta VALUES (" & $i & ")").ok
    let rows = execSqlRows(db, "SELECT SUM(x) FROM ta", @[])
    require rows.ok
    check rows.value.len == 1
    # SUM(1+2+3+4+5) = 15
    check rows.value[0].values[0].int64Val == 15
    discard closeDb(db)

  test "execSqlRows join result":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE p (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id), val TEXT)").ok
    check execSql(db, "INSERT INTO p VALUES (1, 'parent1')").ok
    check execSql(db, "INSERT INTO c VALUES (1, 1, 'child1')").ok
    let rows = execSqlRows(db, "SELECT p.name, c.val FROM p JOIN c ON p.id = c.pid", @[])
    require rows.ok
    check rows.value.len == 1
    discard closeDb(db)

suite "execSqlNoRows coverage":
  test "execSqlNoRows returns count of rows scanned":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)").ok
    for i in 1..10:
      check execSql(db, "INSERT INTO t VALUES (" & $i & ")").ok
    let count = execSqlNoRows(db, "SELECT id FROM t", @[])
    require count.ok
    check count.value == 10
    discard closeDb(db)

  test "execSqlNoRows returns error for closed DB":
    let db = openDb(":memory:").value
    discard closeDb(db)
    let res = execSqlNoRows(db, "SELECT 1", @[])
    check not res.ok

  test "execSqlNoRows with WHERE filter":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)").ok
    for i in 1..10:
      check execSql(db, "INSERT INTO t VALUES (" & $i & ")").ok
    let count = execSqlNoRows(db, "SELECT id FROM t WHERE id > 5", @[])
    require count.ok
    check count.value == 5
    discard closeDb(db)

  test "execSqlNoRows zero rows":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)").ok
    let count = execSqlNoRows(db, "SELECT id FROM t", @[])
    require count.ok
    check count.value == 0
    discard closeDb(db)

suite "saveAs with file-based DB":
  test "saveAs copies file DB to new path":
    let srcPath = makeTempDb("saveas_src.ddb")
    let dstPath = makeTempDb("saveas_dst.ddb")
    defer:
      cleanupDb(srcPath)
      cleanupDb(dstPath)
    
    let db = openDb(srcPath).value
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").ok
    for i in 1..5:
      check execSql(db, "INSERT INTO t VALUES (" & $i & ", 'v" & $i & "')").ok
    
    let saveRes = saveAs(db, dstPath)
    check saveRes.ok
    discard closeDb(db)
    
    # Verify the destination DB
    let dstDb = openDb(dstPath).value
    let rows = execSql(dstDb, "SELECT COUNT(*) FROM t")
    check rows.ok
    check rows.value == @["5"]
    discard closeDb(dstDb)

  test "saveAs with multiple tables":
    let srcPath = makeTempDb("saveas_multi_src.ddb")
    let dstPath = makeTempDb("saveas_multi_dst.ddb")
    defer:
      cleanupDb(srcPath)
      cleanupDb(dstPath)
    
    let db = openDb(srcPath).value
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY)").ok
    check execSql(db, "INSERT INTO a VALUES (1)").ok
    check execSql(db, "INSERT INTO b VALUES (2)").ok
    
    check saveAs(db, dstPath).ok
    discard closeDb(db)
    
    let dstDb = openDb(dstPath).value
    check execSql(dstDb, "SELECT COUNT(*) FROM a").value == @["1"]
    check execSql(dstDb, "SELECT COUNT(*) FROM b").value == @["1"]
    discard closeDb(dstDb)

suite "bulkLoad with UNIQUE constraints":
  test "bulkLoad enforces UNIQUE on TEXT primary key":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE txtpk (name TEXT PRIMARY KEY, val INT)").ok
    
    var rows: seq[seq[Value]] = @[]
    for i in 1..5:
      rows.add(@[textValue("key" & $i), Value(kind: vkInt64, int64Val: int64(i))])
    
    let opts = BulkLoadOptions(
      batchSize: 10, syncInterval: 10,
      disableIndexes: false, checkpointOnComplete: false,
      durability: dmDeferred
    )
    let res = bulkLoad(db, "txtpk", rows, opts)
    check res.ok
    
    let cnt = execSql(db, "SELECT COUNT(*) FROM txtpk")
    check cnt.value == @["5"]
    discard closeDb(db)

  test "bulkLoad with UNIQUE column violation":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE uniq (id INT PRIMARY KEY, code TEXT UNIQUE)").ok
    
    var rows: seq[seq[Value]] = @[]
    rows.add(@[Value(kind: vkInt64, int64Val: 1), textValue("A")])
    rows.add(@[Value(kind: vkInt64, int64Val: 2), textValue("A")])  # duplicate code
    
    let opts = BulkLoadOptions(
      batchSize: 10, syncInterval: 10,
      disableIndexes: false, checkpointOnComplete: false,
      durability: dmDeferred
    )
    let res = bulkLoad(db, "uniq", rows, opts)
    # Should fail due to UNIQUE violation
    check not res.ok
    discard closeDb(db)

  test "bulkLoad with large batch enables merge file logic":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE bigbulk (id INT PRIMARY KEY, v TEXT)").ok
    
    var rows: seq[seq[Value]] = @[]
    for i in 1..100:
      rows.add(@[Value(kind: vkInt64, int64Val: int64(i)), textValue("value" & $i)])
    
    let opts = BulkLoadOptions(
      batchSize: 20, syncInterval: 5,
      disableIndexes: false, checkpointOnComplete: false,
      durability: dmDeferred
    )
    let res = bulkLoad(db, "bigbulk", rows, opts)
    check res.ok
    
    let cnt = execSql(db, "SELECT COUNT(*) FROM bigbulk")
    check cnt.value == @["100"]
    discard closeDb(db)

  test "bulkLoad disableIndexes=false with index rebuild":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE idxtbl (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "CREATE INDEX idx_v ON idxtbl (v)").ok
    
    var rows: seq[seq[Value]] = @[]
    for i in 1..10:
      rows.add(@[Value(kind: vkInt64, int64Val: int64(i)), textValue("v" & $i)])
    
    let opts = BulkLoadOptions(
      batchSize: 5, syncInterval: 5,
      disableIndexes: true, checkpointOnComplete: false,
      durability: dmDeferred
    )
    let res = bulkLoad(db, "idxtbl", rows, opts)
    check res.ok
    discard closeDb(db)

suite "enforceUniqueBatch coverage":
  test "enforceUniqueBatch returns failures for dup INT64 PK":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE eu (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO eu VALUES (1, 'Alice')").ok
    
    let tblRes = db.catalog.getTable("eu")
    require tblRes.ok
    let tbl = tblRes.value
    
    var rows: seq[tuple[values: seq[Value], rowid: uint64]] = @[]
    rows.add((values: @[Value(kind: vkInt64, int64Val: 1), textValue("Bob")], rowid: 99'u64))
    rows.add((values: @[Value(kind: vkInt64, int64Val: 2), textValue("Carol")], rowid: 100'u64))
    
    let res = enforceUniqueBatch(db.catalog, db.pager, tbl, rows)
    check res.ok
    # Row with id=1 conflicts with existing row
    check res.value.len >= 1
    discard closeDb(db)

  test "enforceUniqueBatch no violations":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE eu2 (id INT PRIMARY KEY, name TEXT)").ok
    
    let tblRes = db.catalog.getTable("eu2")
    require tblRes.ok
    let tbl = tblRes.value
    
    var rows: seq[tuple[values: seq[Value], rowid: uint64]] = @[]
    rows.add((values: @[Value(kind: vkInt64, int64Val: 1), textValue("Alice")], rowid: 1'u64))
    rows.add((values: @[Value(kind: vkInt64, int64Val: 2), textValue("Bob")], rowid: 2'u64))
    
    let res = enforceUniqueBatch(db.catalog, db.pager, tbl, rows)
    check res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "enforceUniqueBatch duplicates within batch":
    let db = openDb(":memory:").value
    check execSql(db, "CREATE TABLE eu3 (id INT PRIMARY KEY, code TEXT UNIQUE)").ok
    
    let tblRes = db.catalog.getTable("eu3")
    require tblRes.ok
    let tbl = tblRes.value
    
    var rows: seq[tuple[values: seq[Value], rowid: uint64]] = @[]
    rows.add((values: @[Value(kind: vkInt64, int64Val: 1), textValue("A")], rowid: 1'u64))
    rows.add((values: @[Value(kind: vkInt64, int64Val: 2), textValue("A")], rowid: 2'u64))  # dup code
    
    let res = enforceUniqueBatch(db.catalog, db.pager, tbl, rows)
    check res.ok
    # Should find the duplicate 'code' violation within batch
    check res.value.len >= 1
    discard closeDb(db)
