## Coverage tests for:
## - DROP TABLE with existing indexes (engine.nim L2853-2856)
## - Partial UNIQUE INDEX (engine.nim L2903-2906, L2927-2930)
## - UPDATE WHERE 5 = id (reversed PK condition, engine.nim L3437-3440)
## - ROLLBACK TO SAVEPOINT and RELEASE SAVEPOINT via execSql
## - openRowCursor pkIndexSeek path (exec.nim L916-940)
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
# DROP TABLE with existing indexes (engine.nim L2853-2856)
# ---------------------------------------------------------------------------
suite "DROP TABLE with indexes":
  test "DROP TABLE with btree index removes index too":
    # L2853-2856: iterates catalog.indexes, finds indexes for table
    let db = freshDb("tdrop_idx1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE INDEX t_v_idx ON t(v)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    # Now drop the table - should also drop the index
    let dropRes = execSql(db, "DROP TABLE t")
    require dropRes.ok
    # Table should be gone
    let selRes = execSql(db, "SELECT * FROM t")
    check not selRes.ok
    discard closeDb(db)

  test "DROP TABLE with trigram index removes it too":
    let db = freshDb("tdrop_idx2.ddb")
    discard execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
    discard execSql(db, "CREATE INDEX docs_content ON docs USING TRIGRAM (content)")
    discard execSql(db, "INSERT INTO docs VALUES (1, 'hello')")
    let dropRes = execSql(db, "DROP TABLE docs")
    require dropRes.ok
    let selRes = execSql(db, "SELECT * FROM docs")
    check not selRes.ok
    discard closeDb(db)

  test "DROP TABLE with multiple indexes":
    let db = freshDb("tdrop_idx3.ddb")
    discard execSql(db, "CREATE TABLE t3 (id INT PRIMARY KEY, a INT, b TEXT)")
    discard execSql(db, "CREATE INDEX t3_a ON t3(a)")
    discard execSql(db, "CREATE INDEX t3_b ON t3 USING TRIGRAM (b)")
    let dropRes = execSql(db, "DROP TABLE t3")
    require dropRes.ok
    discard closeDb(db)

  test "DROP TABLE IF EXISTS nonexistent silently succeeds":
    let db = freshDb("tdrop_idx4.ddb")
    let res = execSql(db, "DROP TABLE IF EXISTS nonexistent_table")
    require res.ok
    discard closeDb(db)

# ---------------------------------------------------------------------------
# Partial UNIQUE INDEX (engine.nim L2903-2906, L2927-2930)
# ---------------------------------------------------------------------------
suite "Partial unique index":
  test "CREATE UNIQUE INDEX with WHERE predicate (single col)":
    # L2903-2906: partial index predicate evaluation
    let db = freshDb("tpuniq1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, active INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10, 1)")
    discard execSql(db, "INSERT INTO t VALUES (2, 20, 1)")
    discard execSql(db, "INSERT INTO t VALUES (3, 10, 0)")  # same v but inactive
    let res = execSql(db, "CREATE UNIQUE INDEX t_v_active ON t(v) WHERE active = 1")
    require res.ok
    discard closeDb(db)

  test "Partial unique index detects violation among predicate-matching rows":
    let db = freshDb("tpuniq2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, active INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10, 1)")
    discard execSql(db, "INSERT INTO t VALUES (2, 10, 1)")  # DUPLICATE among active
    let res = execSql(db, "CREATE UNIQUE INDEX t_v_a ON t(v) WHERE active = 1")
    check not res.ok  # should fail: duplicate v among active rows
    discard closeDb(db)

  test "Partial unique index allows duplicates outside predicate":
    # Rows with active=0 don't count for uniqueness even if same v
    let db = freshDb("tpuniq3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, flag INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10, 0)")
    discard execSql(db, "INSERT INTO t VALUES (2, 10, 0)")  # duplicate but outside predicate
    let res = execSql(db, "CREATE UNIQUE INDEX t_v_flag ON t(v) WHERE flag = 1")
    require res.ok  # no active rows, so no conflict
    discard closeDb(db)

  test "CREATE UNIQUE INDEX with WHERE predicate (composite col)":
    # L2927-2930: partial index predicate evaluation for composite index
    let db = freshDb("tpuniq4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 1, 2, 1)")
    discard execSql(db, "INSERT INTO t VALUES (2, 3, 4, 1)")
    discard execSql(db, "INSERT INTO t VALUES (3, 1, 2, 0)")  # duplicate (a,b) but c=0
    let res = execSql(db, "CREATE UNIQUE INDEX t_ab_c ON t(a, b) WHERE c = 1")
    require res.ok
    discard closeDb(db)

# ---------------------------------------------------------------------------
# UPDATE WHERE 5 = id (reversed PK condition, engine.nim L3437-3440)
# ---------------------------------------------------------------------------
suite "UPDATE with reversed PK condition":
  test "UPDATE WHERE literal = pk_col":
    # L3437: tryFastPkUpdate handles right-side PK column
    let db = freshDb("trevpk1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    discard execSql(db, "INSERT INTO t VALUES (2, 20)")
    let res = execSql(db, "UPDATE t SET v = 99 WHERE 1 = id")
    require res.ok
    let selRes = execSql(db, "SELECT v FROM t WHERE id = 1")
    require selRes.ok
    check col0(selRes.value) == "99"
    discard closeDb(db)

  test "UPDATE WHERE literal = pk_col, non-existent pk":
    let db = freshDb("trevpk2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let res = execSql(db, "UPDATE t SET v = 99 WHERE 99 = id")
    require res.ok
    # No rows affected, original unchanged
    let selRes = execSql(db, "SELECT v FROM t WHERE id = 1")
    require selRes.ok
    check col0(selRes.value) == "10"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# SAVEPOINT / RELEASE / ROLLBACK TO (engine.nim L4116-4153)
# ---------------------------------------------------------------------------
suite "SAVEPOINT operations":
  test "SAVEPOINT + RELEASE":
    let db = freshDb("tsp_rel.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let spRes = execSql(db, "SAVEPOINT sp1")
    require spRes.ok
    discard execSql(db, "INSERT INTO t VALUES (2, 20)")
    let relRes = execSql(db, "RELEASE SAVEPOINT sp1")
    require relRes.ok
    let selRes = execSql(db, "SELECT COUNT(*) FROM t")
    require selRes.ok
    discard closeDb(db)

  test "SAVEPOINT + ROLLBACK TO":
    let db = freshDb("tsp_rb.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    discard execSql(db, "BEGIN")
    let spRes = execSql(db, "SAVEPOINT sp2")
    require spRes.ok
    discard execSql(db, "INSERT INTO t VALUES (2, 20)")
    let rbRes = execSql(db, "ROLLBACK TO SAVEPOINT sp2")
    require rbRes.ok
    discard execSql(db, "COMMIT")
    # Row 2 should be rolled back
    let selRes = execSql(db, "SELECT id FROM t")
    require selRes.ok
    check selRes.value.len == 1
    check col0(selRes.value) == "1"
    discard closeDb(db)

  test "RELEASE nonexistent SAVEPOINT errors":
    let db = freshDb("tsp_relerr.ddb")
    let res = execSql(db, "RELEASE SAVEPOINT nonexistent_sp")
    check not res.ok
    discard closeDb(db)

  test "ROLLBACK TO nonexistent SAVEPOINT errors":
    let db = freshDb("tsp_rberr.ddb")
    let res = execSql(db, "ROLLBACK TO SAVEPOINT nonexistent_sp")
    check not res.ok
    discard closeDb(db)

  test "nested SAVEPOINTs + ROLLBACK to outer":
    let db = freshDb("tsp_nest.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "BEGIN")
    discard execSql(db, "SAVEPOINT outer_sp")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    discard execSql(db, "SAVEPOINT inner_sp")
    discard execSql(db, "INSERT INTO t VALUES (2)")
    let rbRes = execSql(db, "ROLLBACK TO SAVEPOINT outer_sp")
    require rbRes.ok
    discard execSql(db, "COMMIT")
    let selRes = execSql(db, "SELECT COUNT(*) FROM t")
    require selRes.ok
    check col0(selRes.value) == "0"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# openRowCursor pkIndexSeek (exec.nim L916-940)
# ---------------------------------------------------------------------------
suite "pkIndexSeek cursor":
  test "SELECT with btree index on non-pk column":
    # L916: openRowCursor pkIndexSeek path
    let db = freshDb("tpkidx1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat INT, v TEXT)")
    discard execSql(db, "CREATE INDEX t_cat_idx ON t(cat)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10, 'a')")
    discard execSql(db, "INSERT INTO t VALUES (2, 10, 'b')")
    discard execSql(db, "INSERT INTO t VALUES (3, 20, 'c')")
    let res = execSql(db, "SELECT v FROM t WHERE cat = 10 ORDER BY v")
    require res.ok
    check res.value.len == 2
    check col0(res.value) == "a"
    discard closeDb(db)

  test "SELECT with text btree index":
    let db = freshDb("tpkidx2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, status TEXT, v INT)")
    discard execSql(db, "CREATE INDEX t_status_idx ON t(status)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'active', 100)")
    discard execSql(db, "INSERT INTO t VALUES (2, 'inactive', 200)")
    discard execSql(db, "INSERT INTO t VALUES (3, 'active', 300)")
    let res = execSql(db, "SELECT v FROM t WHERE status = 'active' ORDER BY v")
    require res.ok
    check res.value.len == 2
    check col0(res.value) == "100"
    discard closeDb(db)
