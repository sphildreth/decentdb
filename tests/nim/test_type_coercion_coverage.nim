## Coverage tests for engine.nim typeCheckValue and type coercion:
## - typeCheckValue error paths (type mismatch errors)
## - DECIMAL precision overflow
## - FLOAT64→INT64 coercion
## - BOOL type checking
## - Various INSERT type errors
## - UPDATE type mismatch
import unittest
import os
import strutils
import engine
import errors

proc freshDb(name: string): Db =
  let path = getTempDir() / name
  for ext in ["", "-wal"]:
    let f = if ext.len == 0: path else: path & ext
    if fileExists(f): removeFile(f)
  openDb(path).value

suite "typeCheckValue INT64 column":
  test "INT64 accepts integer value":
    let db = freshDb("tc_int1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let res = execSql(db, "INSERT INTO t VALUES (42)")
    check res.ok
    discard closeDb(db)

  test "INT64 accepts float that is exact integer":
    let db = freshDb("tc_int2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let res = execSql(db, "INSERT INTO t VALUES (3.0)")
    check res.ok
    discard closeDb(db)

  test "INT64 rejects float that is not exact integer":
    let db = freshDb("tc_int3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let res = execSql(db, "INSERT INTO t VALUES (3.5)")
    check not res.ok
    check "mismatch" in res.err.message.toLowerAscii or "int" in res.err.message.toLowerAscii
    discard closeDb(db)

  test "INT64 accepts NULL":
    let db = freshDb("tc_int4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    let res = execSql(db, "INSERT INTO t VALUES (1, NULL)")
    check res.ok
    discard closeDb(db)

  test "INT64 accepts BOOL as 0/1":
    let db = freshDb("tc_int5.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    let r1 = execSql(db, "INSERT INTO t VALUES (1, true)")
    check r1.ok
    let r2 = execSql(db, "INSERT INTO t VALUES (2, false)")
    check r2.ok
    let sel = execSql(db, "SELECT v FROM t ORDER BY id")
    require sel.ok
    check sel.value[0] == "1"
    check sel.value[1] == "0"
    discard closeDb(db)

suite "typeCheckValue FLOAT64 column":
  test "FLOAT64 accepts float":
    let db = freshDb("tc_fl1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT64)")
    let res = execSql(db, "INSERT INTO t VALUES (1, 3.14)")
    check res.ok
    discard closeDb(db)

  test "FLOAT64 accepts integer (coerces)":
    let db = freshDb("tc_fl2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT64)")
    let res = execSql(db, "INSERT INTO t VALUES (1, 42)")
    check res.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    check "42" in sel.value[0]
    discard closeDb(db)

  test "FLOAT64 rejects TEXT":
    let db = freshDb("tc_fl3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT64)")
    let res = execSql(db, "INSERT INTO t VALUES (1, 'notanumber')")
    check not res.ok
    discard closeDb(db)

  test "FLOAT64 accepts NULL":
    let db = freshDb("tc_fl4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT64)")
    let res = execSql(db, "INSERT INTO t VALUES (1, NULL)")
    check res.ok
    discard closeDb(db)

suite "typeCheckValue BOOL column":
  test "BOOL accepts bool literal":
    let db = freshDb("tc_bo1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v BOOL)")
    let r1 = execSql(db, "INSERT INTO t VALUES (1, true)")
    check r1.ok
    let r2 = execSql(db, "INSERT INTO t VALUES (2, false)")
    check r2.ok
    discard closeDb(db)

  test "BOOL accepts integer (0 = false, non-0 = true)":
    let db = freshDb("tc_bo2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v BOOL)")
    let r1 = execSql(db, "INSERT INTO t VALUES (1, 1)")
    check r1.ok
    let r2 = execSql(db, "INSERT INTO t VALUES (2, 0)")
    check r2.ok
    discard closeDb(db)

  test "BOOL rejects TEXT":
    let db = freshDb("tc_bo3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v BOOL)")
    let res = execSql(db, "INSERT INTO t VALUES (1, 'yes')")
    check not res.ok
    discard closeDb(db)

  test "BOOL accepts NULL":
    let db = freshDb("tc_bo4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v BOOL)")
    let res = execSql(db, "INSERT INTO t VALUES (1, NULL)")
    check res.ok
    discard closeDb(db)

suite "typeCheckValue TEXT column":
  test "TEXT accepts text":
    let db = freshDb("tc_tx1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    let res = execSql(db, "INSERT INTO t VALUES (1, 'hello')")
    check res.ok
    discard closeDb(db)

  test "TEXT rejects integer":
    let db = freshDb("tc_tx2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    let res = execSql(db, "INSERT INTO t VALUES (1, 42)")
    check not res.ok
    check "mismatch" in res.err.message.toLowerAscii or "text" in res.err.message.toLowerAscii
    discard closeDb(db)

  test "TEXT accepts NULL":
    let db = freshDb("tc_tx3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    let res = execSql(db, "INSERT INTO t VALUES (1, NULL)")
    check res.ok
    discard closeDb(db)

suite "DECIMAL precision and scale":
  test "DECIMAL accepts exact value":
    let db = freshDb("tc_dc1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(6,2))")
    let res = execSql(db, "INSERT INTO t VALUES (1, 99.99)")
    check res.ok
    discard closeDb(db)

  test "DECIMAL precision overflow rejected":
    let db = freshDb("tc_dc2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(4,2))")
    let res = execSql(db, "INSERT INTO t VALUES (1, 12345.67)")
    check not res.ok
    check "precision" in res.err.message.toLowerAscii or "overflow" in res.err.message.toLowerAscii
    discard closeDb(db)

  test "DECIMAL accepts INT64 (scaled to DECIMAL)":
    let db = freshDb("tc_dc3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(8,2))")
    let res = execSql(db, "INSERT INTO t VALUES (1, 42)")
    check res.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    check "42" in sel.value[0]
    discard closeDb(db)

  test "DECIMAL accepts FLOAT64 (coercion)":
    let db = freshDb("tc_dc4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(8,3))")
    let res = execSql(db, "INSERT INTO t VALUES (1, 3.14159)")
    check res.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    check "3.14" in sel.value[0]
    discard closeDb(db)

  test "DECIMAL rescaling within table":
    let db = freshDb("tc_dc5.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(8,2))")
    discard execSql(db, "INSERT INTO t VALUES (1, 5.555)")
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    # After rescaling from scale 3 to scale 2, value should be 5.56 or 5.55
    check "5.5" in sel.value[0]
    discard closeDb(db)

  test "DECIMAL accepts NULL":
    let db = freshDb("tc_dc6.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(8,2))")
    let res = execSql(db, "INSERT INTO t VALUES (1, NULL)")
    check res.ok
    discard closeDb(db)

suite "UPDATE type checking":
  test "UPDATE with type mismatch rejected":
    let db = freshDb("tc_upd1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let res = execSql(db, "UPDATE t SET v = 'not-an-int' WHERE id = 1")
    check not res.ok
    discard closeDb(db)

  test "UPDATE with correct type succeeds":
    let db = freshDb("tc_upd2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let res = execSql(db, "UPDATE t SET v = 20 WHERE id = 1")
    check res.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    check sel.value[0] == "20"
    discard closeDb(db)

  test "UPDATE FLOAT64 column with int (coercion)":
    let db = freshDb("tc_upd3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT64)")
    discard execSql(db, "INSERT INTO t VALUES (1, 1.5)")
    let res = execSql(db, "UPDATE t SET v = 99 WHERE id = 1")
    check res.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    check "99" in sel.value[0]
    discard closeDb(db)

suite "NOT NULL constraint":
  test "NOT NULL column rejects NULL insert":
    let db = freshDb("tc_nn1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)")
    let res = execSql(db, "INSERT INTO t VALUES (1, NULL)")
    check not res.ok
    discard closeDb(db)

  test "NOT NULL column accepts non-null value":
    let db = freshDb("tc_nn2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)")
    let res = execSql(db, "INSERT INTO t VALUES (1, 42)")
    check res.ok
    discard closeDb(db)

  test "NOT NULL constraint on UPDATE":
    let db = freshDb("tc_nn3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let res = execSql(db, "UPDATE t SET v = NULL WHERE id = 1")
    check not res.ok
    discard closeDb(db)
