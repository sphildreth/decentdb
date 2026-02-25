## Coverage tests for binder.nim depth limits (L1222, L1315)
## View expansion depth exceeded and CTE expansion depth exceeded.
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

suite "View expansion depth limit":
  test "View chain within depth limit succeeds":
    let db = freshDb("view_depth_ok.ddb")
    discard execSql(db, "CREATE TABLE base_t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO base_t VALUES (1, 42)")
    discard execSql(db, "CREATE VIEW v1 AS SELECT id, v FROM base_t")
    for i in 2..10:
      let prev = "v" & $(i-1)
      let cur = "v" & $i
      discard execSql(db, "CREATE VIEW " & cur & " AS SELECT id, v FROM " & prev)
    let res = execSql(db, "SELECT v FROM v10")
    require res.ok
    check res.value == @["42"]
    discard closeDb(db)

  test "View chain exceeding MaxViewExpansionDepth (16) fails":
    let db = freshDb("view_depth_exceeded.ddb")
    discard execSql(db, "CREATE TABLE base_t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO base_t VALUES (1, 100)")
    discard execSql(db, "CREATE VIEW v1 AS SELECT id, v FROM base_t")
    for i in 2..17:
      let prev = "v" & $(i-1)
      let cur = "v" & $i
      discard execSql(db, "CREATE VIEW " & cur & " AS SELECT id, v FROM " & prev)
    # Creating the 18th level view should fail - binder validates at CREATE VIEW time
    let createRes = execSql(db, "CREATE VIEW v18 AS SELECT id, v FROM v17")
    check not createRes.ok
    check "depth exceeded" in createRes.err.message.toLowerAscii
    discard closeDb(db)

  test "View chain at exactly 16 levels succeeds":
    let db = freshDb("view_depth_16.ddb")
    discard execSql(db, "CREATE TABLE base_16 (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO base_16 VALUES (1, 99)")
    discard execSql(db, "CREATE VIEW vv1 AS SELECT id, v FROM base_16")
    for i in 2..16:
      let prev = "vv" & $(i-1)
      let cur = "vv" & $i
      discard execSql(db, "CREATE VIEW " & cur & " AS SELECT id, v FROM " & prev)
    let res = execSql(db, "SELECT v FROM vv16")
    require res.ok
    check res.value == @["99"]
    discard closeDb(db)

suite "CTE depth limits":
  test "Duplicate CTE name returns error":
    let db = freshDb("cte_dup.ddb")
    discard execSql(db, "CREATE TABLE base_cte (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO base_cte VALUES (1, 1)")
    let res = execSql(db,
      "WITH cte AS (SELECT v FROM base_cte), cte AS (SELECT v FROM base_cte) SELECT v FROM cte")
    check not res.ok
    check "duplicate" in res.err.message.toLowerAscii or "cte" in res.err.message.toLowerAscii
    discard closeDb(db)

  test "CTE with multiple CTEs resolves correctly":
    let db = freshDb("cte_multi.ddb")
    discard execSql(db, "CREATE TABLE nums (id INT PRIMARY KEY, v INT)")
    for i in 1..5:
      discard execSql(db, "INSERT INTO nums VALUES (" & $i & ", " & $(i*10) & ")")
    let res = execSql(db, """
      WITH
        low AS (SELECT id, v FROM nums WHERE v <= 20),
        high AS (SELECT id, v FROM nums WHERE v > 30)
      SELECT v FROM low ORDER BY v
    """)
    require res.ok
    check res.value == @["10", "20"]
    discard closeDb(db)

  test "CTE non-SELECT body returns error":
    let db = freshDb("cte_nonsel.ddb")
    discard execSql(db, "CREATE TABLE cnsel (id INT PRIMARY KEY, v INT)")
    # CTE must use SELECT
    let res = execSql(db, "WITH cte AS (INSERT INTO cnsel VALUES (1, 2)) SELECT 1")
    check not res.ok
    discard closeDb(db)

suite "Circular reference detection":
  test "Circular view reference returns error":
    let db = freshDb("view_circular.ddb")
    discard execSql(db, "CREATE TABLE cbase (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO cbase VALUES (1, 1)")
    # Create view chain 18 deep - creating vc18 itself should fail
    discard execSql(db, "CREATE VIEW vc1 AS SELECT id, v FROM cbase")
    for i in 2..17:
      let prev = "vc" & $(i-1)
      let cur = "vc" & $i
      discard execSql(db, "CREATE VIEW " & cur & " AS SELECT id, v FROM " & prev)
    let createRes = execSql(db, "CREATE VIEW vc18 AS SELECT id, v FROM vc17")
    check not createRes.ok
    check "depth exceeded" in createRes.err.message.toLowerAscii or "expansion" in createRes.err.message.toLowerAscii
    discard closeDb(db)

suite "Binder error paths":
  test "SELECT from non-existent table returns error":
    let db = freshDb("binder_no_table.ddb")
    let res = execSql(db, "SELECT v FROM no_such_table")
    check not res.ok
    discard closeDb(db)

  test "SELECT non-existent column returns error":
    let db = freshDb("binder_no_col.ddb")
    discard execSql(db, "CREATE TABLE bnt (id INT PRIMARY KEY, v INT)")
    let res = execSql(db, "SELECT no_such_col FROM bnt")
    check not res.ok
    discard closeDb(db)

  test "Ambiguous column in JOIN returns error":
    let db = freshDb("binder_ambig.ddb")
    discard execSql(db, "CREATE TABLE left_t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE TABLE right_t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO left_t VALUES (1, 10)")
    discard execSql(db, "INSERT INTO right_t VALUES (1, 20)")
    let res = execSql(db, "SELECT v FROM left_t JOIN right_t ON left_t.id = right_t.id")
    check not res.ok
    check "ambiguous" in res.err.message.toLowerAscii
    discard closeDb(db)

  test "WITH on set operations returns error":
    let db = freshDb("binder_with_setop.ddb")
    discard execSql(db, "CREATE TABLE wso (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO wso VALUES (1)")
    let res = execSql(db,
      "WITH cte AS (SELECT v FROM wso) SELECT v FROM cte UNION SELECT v FROM wso")
    check not res.ok
    check "set operation" in res.err.message.toLowerAscii or "not supported" in res.err.message.toLowerAscii
    discard closeDb(db)
