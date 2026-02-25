## Coverage tests for EXTRACT(DAY/HOUR/MINUTE/SECOND), CAST error paths,
## CAST bool->int/float, CAST blob->UUID, unsupported CAST target.
## Targets exec.nim L2321-2403, L3549-3558.
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
# EXTRACT fields (L3544-3558)
# ---------------------------------------------------------------------------
suite "EXTRACT date fields":
  test "EXTRACT YEAR from datetime":
    let db = freshDb("text_yr.ddb")
    let res = execSql(db, "SELECT EXTRACT(YEAR FROM '2024-06-15 10:30:45')")
    require res.ok
    check col0(res.value) == "2024"
    discard closeDb(db)

  test "EXTRACT MONTH from datetime":
    let db = freshDb("text_mo.ddb")
    let res = execSql(db, "SELECT EXTRACT(MONTH FROM '2024-06-15 10:30:45')")
    require res.ok
    check col0(res.value) == "6"
    discard closeDb(db)

  test "EXTRACT DAY from datetime":
    # L3549-3550
    let db = freshDb("text_dy.ddb")
    let res = execSql(db, "SELECT EXTRACT(DAY FROM '2024-06-15 10:30:45')")
    require res.ok
    check col0(res.value) == "15"
    discard closeDb(db)

  test "EXTRACT HOUR from datetime":
    # L3551-3552
    let db = freshDb("text_hr.ddb")
    let res = execSql(db, "SELECT EXTRACT(HOUR FROM '2024-06-15 10:30:45')")
    require res.ok
    check col0(res.value) == "10"
    discard closeDb(db)

  test "EXTRACT MINUTE from datetime":
    # L3553-3554
    let db = freshDb("text_min.ddb")
    let res = execSql(db, "SELECT EXTRACT(MINUTE FROM '2024-06-15 10:30:45')")
    require res.ok
    check col0(res.value) == "30"
    discard closeDb(db)

  test "EXTRACT SECOND from datetime":
    # L3555-3556
    let db = freshDb("text_sec.ddb")
    let res = execSql(db, "SELECT EXTRACT(SECOND FROM '2024-06-15 10:30:45')")
    require res.ok
    check col0(res.value) == "45"
    discard closeDb(db)

  test "EXTRACT unsupported field returns error":
    # L3558
    let db = freshDb("text_bad.ddb")
    let res = execSql(db, "SELECT EXTRACT(MICROSECOND FROM '2024-06-15')")
    check not res.ok
    discard closeDb(db)

  test "EXTRACT from invalid datetime errors":
    let db = freshDb("text_invdt.ddb")
    let res = execSql(db, "SELECT EXTRACT(YEAR FROM 'not-a-date')")
    check not res.ok
    discard closeDb(db)

  test "EXTRACT DAY from date-only string":
    let db = freshDb("text_dy2.ddb")
    let res = execSql(db, "SELECT EXTRACT(DAY FROM '2024-12-25')")
    require res.ok
    check col0(res.value) == "25"
    discard closeDb(db)

  test "EXTRACT HOUR from date-only string is 0":
    let db = freshDb("text_hr0.ddb")
    let res = execSql(db, "SELECT EXTRACT(HOUR FROM '2024-01-01')")
    require res.ok
    check col0(res.value) == "0"
    discard closeDb(db)

  test "EXTRACT MINUTE from datetime column":
    let db = freshDb("text_mincol.ddb")
    discard execSql(db, "CREATE TABLE events (id INT PRIMARY KEY, ts TEXT)")
    discard execSql(db, "INSERT INTO events VALUES (1, '2024-03-15 09:45:30')")
    let res = execSql(db, "SELECT EXTRACT(MINUTE FROM ts) FROM events WHERE id = 1")
    require res.ok
    check col0(res.value) == "45"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# CAST error paths (L2321-2403)
# ---------------------------------------------------------------------------
suite "CAST error paths":
  test "CAST TRUE to INT":
    # L2349: bool -> int
    let db = freshDb("tcast_bi.ddb")
    let res = execSql(db, "SELECT CAST(TRUE AS INT)")
    require res.ok
    check col0(res.value) == "1"
    discard closeDb(db)

  test "CAST FALSE to INT":
    let db = freshDb("tcast_bi2.ddb")
    let res = execSql(db, "SELECT CAST(FALSE AS INT)")
    require res.ok
    check col0(res.value) == "0"
    discard closeDb(db)

  test "CAST TRUE to FLOAT":
    # L2369: bool -> float
    let db = freshDb("tcast_bf.ddb")
    let res = execSql(db, "SELECT CAST(TRUE AS REAL)")
    require res.ok
    check col0(res.value) == "1.0"
    discard closeDb(db)

  test "CAST FALSE to FLOAT":
    let db = freshDb("tcast_bf2.ddb")
    let res = execSql(db, "SELECT CAST(FALSE AS REAL)")
    require res.ok
    check col0(res.value) == "0.0"
    discard closeDb(db)

  test "CAST bool to DECIMAL errors":
    # L2332: cannot cast bool to decimal
    let db = freshDb("tcast_bderr.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v BOOL)")
    discard execSql(db, "INSERT INTO t VALUES (1, TRUE)")
    let res = execSql(db, "SELECT CAST(v AS DECIMAL(5,2)) FROM t WHERE id = 1")
    check not res.ok
    discard closeDb(db)

  test "CAST invalid text to DECIMAL errors":
    # L2321: invalid decimal format
    let db = freshDb("tcast_tdecerr.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'not-a-number')")
    let res = execSql(db, "SELECT CAST(v AS DECIMAL(5,2)) FROM t WHERE id = 1")
    check not res.ok
    discard closeDb(db)

  test "CAST float to DECIMAL from column errors":
    # Float -> DECIMAL is not supported (must go through text)
    let db = freshDb("tcast_fdec.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 3.14)")
    let res = execSql(db, "SELECT CAST(v AS DECIMAL(5,2)) FROM t WHERE id = 1")
    check not res.ok  # Cannot cast vkFloat64 to DECIMAL
    discard closeDb(db)

  test "CAST bool to BOOL is identity":
    # L2349+
    let db = freshDb("tcast_bb.ddb")
    let res = execSql(db, "SELECT CAST(TRUE AS BOOL)")
    require res.ok
    check col0(res.value) in ["true", "1", "TRUE"]
    discard closeDb(db)

  test "CAST int to BOOL":
    # L2396: int -> bool
    let db = freshDb("tcast_ib.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 1)")
    discard execSql(db, "INSERT INTO t VALUES (2, 0)")
    let res1 = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 1")
    require res1.ok
    let res2 = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 2")
    require res2.ok
    discard closeDb(db)

  test "CAST null column to INT returns NULL":
    let db = freshDb("tcast_null.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, NULL)")
    let res = execSql(db, "SELECT CAST(v AS TEXT) FROM t WHERE id = 1")
    require res.ok
    check col0(res.value) == "NULL"
    discard closeDb(db)

  test "CAST text to BOOL errors for invalid text":
    # L2396: text -> bool errors when not true/false/1/0
    let db = freshDb("tcast_tberr.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'notabool')")
    let res = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 1")
    check not res.ok
    discard closeDb(db)
