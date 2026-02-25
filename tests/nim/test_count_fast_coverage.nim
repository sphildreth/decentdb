## Coverage tests for tryCountNoRowsFast fast paths:
## - pkRowidSeek (primary key lookup)
## - pkIndexSeek (btree non-pk index equality)
## - pkTrigramSeek (trigram index LIKE COUNT)
## Targets exec.nim L1191-1340.
import unittest
import os
import strutils
import engine
import errors

proc freshDb(name: string): Db =
  let path = getTempDir() / name
  for ext in ["", "-wal"]:
    let f = (if ext.len == 0: path else: path & ext)
    if fileExists(f): removeFile(f)
  openDb(path).value

proc col0(rows: seq[string]): string =
  if rows.len == 0: return ""
  rows[0].split("|")[0]

# ---------------------------------------------------------------------------
# pkRowidSeek fast count (L1238-1252)
# ---------------------------------------------------------------------------
suite "execSqlNoRows pkRowidSeek fast count":
  test "count by primary key, key exists":
    # L1238: pkRowidSeek path in tryCountNoRowsFast
    let db = freshDb("tcountpk1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'b')")
    let res = execSqlNoRows(db, "SELECT * FROM t WHERE id = 1", @[])
    require res.ok
    check res.value == 1
    discard closeDb(db)

  test "count by primary key, key missing":
    let db = freshDb("tcountpk2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    let res = execSqlNoRows(db, "SELECT * FROM t WHERE id = 99", @[])
    require res.ok
    check res.value == 0
    discard closeDb(db)

  test "count by primary key, empty table":
    let db = freshDb("tcountpk3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    let res = execSqlNoRows(db, "SELECT * FROM t WHERE id = 1", @[])
    require res.ok
    check res.value == 0
    discard closeDb(db)

  test "count by primary key with LIMIT":
    let db = freshDb("tcountpk4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    for i in 1..5:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ", 'v" & $i & "')")
    let res = execSqlNoRows(db, "SELECT * FROM t WHERE id = 1 LIMIT 1", @[])
    require res.ok
    check res.value == 1
    discard closeDb(db)

# ---------------------------------------------------------------------------
# pkIndexSeek fast count (L1191-1236)
# ---------------------------------------------------------------------------
suite "execSqlNoRows pkIndexSeek fast count":
  test "count by btree indexed column, value exists":
    # L1191: pkIndexSeek path in tryCountNoRowsFast
    let db = freshDb("tcountidx1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat INT)")
    discard execSql(db, "CREATE INDEX t_cat_idx ON t(cat)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    discard execSql(db, "INSERT INTO t VALUES (2, 10)")
    discard execSql(db, "INSERT INTO t VALUES (3, 20)")
    let res = execSqlNoRows(db, "SELECT * FROM t WHERE cat = 10", @[])
    require res.ok
    check res.value == 2
    discard closeDb(db)

  test "count by btree indexed column, value missing":
    let db = freshDb("tcountidx2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat INT)")
    discard execSql(db, "CREATE INDEX t_cat2_idx ON t(cat)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let res = execSqlNoRows(db, "SELECT * FROM t WHERE cat = 99", @[])
    require res.ok
    check res.value == 0
    discard closeDb(db)

  test "count by text btree index":
    let db = freshDb("tcountidx3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, status TEXT)")
    discard execSql(db, "CREATE INDEX t_status_idx ON t(status)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'active')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'active')")
    discard execSql(db, "INSERT INTO t VALUES (3, 'inactive')")
    let res = execSqlNoRows(db, "SELECT * FROM t WHERE status = 'active'", @[])
    require res.ok
    check res.value == 2
    discard closeDb(db)

  test "count pkIndexSeek with LIMIT":
    let db = freshDb("tcountidx4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE INDEX t_v_idx ON t(v)")
    for i in 1..5:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ", 1)")
    let res = execSqlNoRows(db, "SELECT * FROM t WHERE v = 1 LIMIT 3", @[])
    require res.ok
    check res.value <= 3
    discard closeDb(db)

# ---------------------------------------------------------------------------
# pkTrigramSeek fast count (L1254-1340)
# ---------------------------------------------------------------------------
suite "execSqlNoRows pkTrigramSeek fast count":
  test "COUNT LIKE with trigram index":
    # L1254: pkTrigramSeek path in tryCountNoRowsFast
    let db = freshDb("tcounttrig1.ddb")
    discard execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
    discard execSql(db, "CREATE INDEX docs_content ON docs USING TRIGRAM (content)")
    discard execSql(db, "INSERT INTO docs VALUES (1, 'hello world')")
    discard execSql(db, "INSERT INTO docs VALUES (2, 'hello nim programming')")
    discard execSql(db, "INSERT INTO docs VALUES (3, 'goodbye world')")
    let res = execSqlNoRows(db, "SELECT * FROM docs WHERE content LIKE '%hello%'", @[])
    require res.ok
    check res.value == 2
    discard closeDb(db)

  test "COUNT LIKE with trigram index, no match":
    let db = freshDb("tcounttrig2.ddb")
    discard execSql(db, "CREATE TABLE docs2 (id INT PRIMARY KEY, content TEXT)")
    discard execSql(db, "CREATE INDEX docs2_content ON docs2 USING TRIGRAM (content)")
    discard execSql(db, "INSERT INTO docs2 VALUES (1, 'hello world')")
    let res = execSqlNoRows(db, "SELECT * FROM docs2 WHERE content LIKE '%xyz123%'", @[])
    require res.ok
    check res.value == 0
    discard closeDb(db)

  test "COUNT LIKE with short pattern (table scan fallback)":
    # Pattern shorter than 3 chars -> falls through to countLikeTableScan
    let db = freshDb("tcounttrig3.ddb")
    discard execSql(db, "CREATE TABLE docs3 (id INT PRIMARY KEY, content TEXT)")
    discard execSql(db, "CREATE INDEX docs3_content ON docs3 USING TRIGRAM (content)")
    discard execSql(db, "INSERT INTO docs3 VALUES (1, 'hi there')")
    discard execSql(db, "INSERT INTO docs3 VALUES (2, 'bye there')")
    let res = execSqlNoRows(db, "SELECT * FROM docs3 WHERE content LIKE '%hi%'", @[])
    require res.ok
    check res.value == 1
    discard closeDb(db)

  test "COUNT LIKE with many matching rows":
    let db = freshDb("tcounttrig4.ddb")
    discard execSql(db, "CREATE TABLE docs4 (id INT PRIMARY KEY, content TEXT)")
    discard execSql(db, "CREATE INDEX docs4_content ON docs4 USING TRIGRAM (content)")
    for i in 1..10:
      discard execSql(db, "INSERT INTO docs4 VALUES (" & $i & ", 'test document number " & $i & "')")
    let res = execSqlNoRows(db, "SELECT * FROM docs4 WHERE content LIKE '%test%'", @[])
    require res.ok
    check res.value == 10
    discard closeDb(db)

  test "SELECT rows from trigram index table":
    # Exercises trigramSeekRows path in execPlan
    let db = freshDb("tcounttrig5.ddb")
    discard execSql(db, "CREATE TABLE docs5 (id INT PRIMARY KEY, content TEXT)")
    discard execSql(db, "CREATE INDEX docs5_content ON docs5 USING TRIGRAM (content)")
    discard execSql(db, "INSERT INTO docs5 VALUES (1, 'hello world')")
    discard execSql(db, "INSERT INTO docs5 VALUES (2, 'world of nim')")
    discard execSql(db, "INSERT INTO docs5 VALUES (3, 'goodbye')")
    let res = execSql(db, "SELECT id FROM docs5 WHERE content LIKE '%world%' ORDER BY id")
    require res.ok
    check res.value.len == 2
    check col0(res.value) == "1"
    discard closeDb(db)

  test "SELECT COUNT(*) from trigram-indexed table":
    let db = freshDb("tcounttrig6.ddb")
    discard execSql(db, "CREATE TABLE docs6 (id INT PRIMARY KEY, content TEXT)")
    discard execSql(db, "CREATE INDEX docs6_content ON docs6 USING TRIGRAM (content)")
    discard execSql(db, "INSERT INTO docs6 VALUES (1, 'hello world')")
    discard execSql(db, "INSERT INTO docs6 VALUES (2, 'hello nim')")
    # Use execSqlNoRows to count matching rows
    let cntRes = execSqlNoRows(db, "SELECT * FROM docs6 WHERE content LIKE '%hello%'", @[])
    require cntRes.ok
    check cntRes.value == 2
    discard closeDb(db)
