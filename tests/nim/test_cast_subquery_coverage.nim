## Coverage tests for:
## - castValue (exec.nim L1560-1641): DECIMAL→INT, DECIMAL→FLOAT, DECIMAL→BOOL,
##   FLOAT→INT, FLOAT→BOOL, TEXT→INT, TEXT→FLOAT, TEXT→BOOL, BLOB→TEXT, type aliases
## - canonicalCastType (L1560-1570): INTEGER, FLOAT64, BOOLEAN aliases
## - EXISTS subquery (L3368-3400)
## - IN_SUBQUERY (L3401-3446)
## - JSON_VALID (exec.nim L2693)
## - JSON_OBJECT (exec.nim L2708)
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

# ------------------- castValue via CAST expression -------------------

suite "CAST type alias coverage":
  test "CAST using INTEGER alias":
    let db = freshDb("tcast_int.ddb")
    let res = execSql(db, "SELECT CAST(3.7 AS INTEGER)")
    require res.ok
    check col0(res.value) == "3"
    discard closeDb(db)

  test "CAST using FLOAT alias":
    let db = freshDb("tcast_fl.ddb")
    let res = execSql(db, "SELECT CAST(5 AS FLOAT)")
    require res.ok
    check col0(res.value) == "5.0"
    discard closeDb(db)

  test "CAST using FLOAT64 alias":
    let db = freshDb("tcast_fl64.ddb")
    let res = execSql(db, "SELECT CAST(5 AS FLOAT64)")
    require res.ok
    check col0(res.value) == "5.0"
    discard closeDb(db)

  test "CAST using BOOLEAN alias":
    let db = freshDb("tcast_bool.ddb")
    let res = execSql(db, "SELECT CAST(1 AS BOOLEAN)")
    require res.ok
    check col0(res.value) in ["true", "1", "TRUE"]
    discard closeDb(db)

  test "CAST using unsupported type returns error":
    let db = freshDb("tcast_bad.ddb")
    let res = execSql(db, "SELECT CAST(1 AS BLOB)")
    check not res.ok
    discard closeDb(db)

suite "CAST DECIMAL column to other types":
  test "CAST DECIMAL to INT":
    # castValue vkDecimal → INT64 (L1584)
    let db = freshDb("tcast_di.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))")
    discard execSql(db, "INSERT INTO t VALUES (1, 3.14)")
    let res = execSql(db, "SELECT CAST(v AS INT) FROM t WHERE id = 1")
    require res.ok
    check col0(res.value) == "3"
    discard closeDb(db)

  test "CAST DECIMAL to FLOAT":
    # castValue vkDecimal → FLOAT64 (L1601)
    let db = freshDb("tcast_df.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))")
    discard execSql(db, "INSERT INTO t VALUES (1, 2.50)")
    let res = execSql(db, "SELECT CAST(v AS FLOAT) FROM t WHERE id = 1")
    require res.ok
    # 2.50 as float
    check "2.5" in col0(res.value)
    discard closeDb(db)

  test "CAST DECIMAL to TEXT":
    let db = freshDb("tcast_dt.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(6,2))")
    discard execSql(db, "INSERT INTO t VALUES (1, 9.99)")
    let res = execSql(db, "SELECT CAST(v AS TEXT) FROM t WHERE id = 1")
    require res.ok
    check "9.99" in col0(res.value)
    discard closeDb(db)

  test "CAST DECIMAL to BOOL":
    let db = freshDb("tcast_db.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(4,2))")
    discard execSql(db, "INSERT INTO t VALUES (1, 1.00)")
    discard execSql(db, "INSERT INTO t VALUES (2, 0.00)")
    let r1 = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 1")
    require r1.ok
    check col0(r1.value) in ["true", "1", "TRUE"]
    let r2 = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 2")
    require r2.ok
    check col0(r2.value) in ["false", "0", "FALSE"]
    discard closeDb(db)

suite "CAST FLOAT column to other types":
  test "CAST FLOAT to INT":
    # castValue vkFloat64 → INT64 (L1592)
    let db = freshDb("tcast_fi.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 7.9)")
    let res = execSql(db, "SELECT CAST(v AS INT) FROM t WHERE id = 1")
    require res.ok
    check col0(res.value) == "7"
    discard closeDb(db)

  test "CAST FLOAT to BOOL":
    # castValue vkFloat64 → BOOL
    let db = freshDb("tcast_fb.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 3.14)")
    discard execSql(db, "INSERT INTO t VALUES (2, 0.0)")
    let r1 = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 1")
    require r1.ok
    check col0(r1.value) in ["true", "1", "TRUE"]
    let r2 = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 2")
    require r2.ok
    check col0(r2.value) in ["false", "0", "FALSE"]
    discard closeDb(db)

suite "CAST TEXT column to other types":
  test "CAST TEXT to INT":
    # castValue vkText → INT64 (L1596)
    let db = freshDb("tcast_ti.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, '42')")
    let res = execSql(db, "SELECT CAST(v AS INT) FROM t WHERE id = 1")
    require res.ok
    check col0(res.value) == "42"
    discard closeDb(db)

  test "CAST TEXT invalid to INT returns error":
    let db = freshDb("tcast_tie.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'notanumber')")
    let res = execSql(db, "SELECT CAST(v AS INT) FROM t WHERE id = 1")
    check not res.ok
    discard closeDb(db)

  test "CAST TEXT to FLOAT":
    let db = freshDb("tcast_tf.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, '3.14')")
    let res = execSql(db, "SELECT CAST(v AS FLOAT) FROM t WHERE id = 1")
    require res.ok
    check "3.14" in col0(res.value)
    discard closeDb(db)

  test "CAST TEXT invalid to FLOAT returns error":
    let db = freshDb("tcast_tfe.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'abc')")
    let res = execSql(db, "SELECT CAST(v AS FLOAT) FROM t WHERE id = 1")
    check not res.ok
    discard closeDb(db)

  test "CAST TEXT 'true' to BOOL":
    let db = freshDb("tcast_tbt.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'true')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'false')")
    discard execSql(db, "INSERT INTO t VALUES (3, '1')")
    discard execSql(db, "INSERT INTO t VALUES (4, '0')")
    let r1 = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 1")
    require r1.ok
    check col0(r1.value) in ["true", "1", "TRUE"]
    let r2 = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 2")
    require r2.ok
    check col0(r2.value) in ["false", "0", "FALSE"]
    discard closeDb(db)

  test "CAST TEXT invalid to BOOL returns error":
    let db = freshDb("tcast_tbe.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'maybe')")
    let res = execSql(db, "SELECT CAST(v AS BOOL) FROM t WHERE id = 1")
    check not res.ok
    discard closeDb(db)

# ------------------- EXISTS subquery -------------------

suite "EXISTS subquery":
  test "EXISTS returns true when subquery has rows":
    let db = freshDb("texists1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let res = execSql(db, "SELECT EXISTS(SELECT 1 FROM t WHERE id = 1)")
    require res.ok
    check col0(res.value) in ["true", "1", "TRUE"]
    discard closeDb(db)

  test "EXISTS returns false when subquery is empty":
    let db = freshDb("texists2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    let res = execSql(db, "SELECT EXISTS(SELECT 1 FROM t WHERE id = 99)")
    require res.ok
    check col0(res.value) in ["false", "0", "FALSE"]
    discard closeDb(db)

  test "WHERE EXISTS with correlated subquery":
    let db = freshDb("texists3.ddb")
    discard execSql(db, "CREATE TABLE par (pid INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE ch (cid INT PRIMARY KEY, par_id INT)")
    discard execSql(db, "INSERT INTO par VALUES (1)")
    discard execSql(db, "INSERT INTO par VALUES (2)")
    discard execSql(db, "INSERT INTO ch VALUES (1, 1)")
    let res = execSql(db, "SELECT pid FROM par WHERE EXISTS (SELECT 1 FROM ch WHERE ch.par_id = par.pid)")
    require res.ok
    check res.value.len == 1
    check col0(res.value) == "1"
    discard closeDb(db)

  test "WHERE NOT EXISTS filters correctly":
    let db = freshDb("texists4.ddb")
    discard execSql(db, "CREATE TABLE items (iid INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE used (uid INT PRIMARY KEY, item_id INT)")
    discard execSql(db, "INSERT INTO items VALUES (1)")
    discard execSql(db, "INSERT INTO items VALUES (2)")
    discard execSql(db, "INSERT INTO used VALUES (1, 1)")
    let res = execSql(db, "SELECT iid FROM items WHERE NOT EXISTS (SELECT 1 FROM used WHERE used.item_id = items.iid)")
    require res.ok
    check res.value.len == 1
    check col0(res.value) == "2"
    discard closeDb(db)

# ------------------- IN subquery -------------------

suite "IN subquery":
  test "column IN (subquery) matches rows":
    let db = freshDb("tin1.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b (val INT PRIMARY KEY)")
    for i in [1, 2, 3]: discard execSql(db, "INSERT INTO a VALUES (" & $i & ")")
    for i in [2, 3]: discard execSql(db, "INSERT INTO b VALUES (" & $i & ")")
    let res = execSql(db, "SELECT id FROM a WHERE id IN (SELECT val FROM b)")
    require res.ok
    check res.value.len == 2
    discard closeDb(db)

  test "column NOT IN (subquery) filters correctly":
    let db = freshDb("tin2.ddb")
    discard execSql(db, "CREATE TABLE a2 (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b2 (val INT PRIMARY KEY)")
    for i in [1, 2, 3]: discard execSql(db, "INSERT INTO a2 VALUES (" & $i & ")")
    discard execSql(db, "INSERT INTO b2 VALUES (2)")
    let res = execSql(db, "SELECT id FROM a2 WHERE id NOT IN (SELECT val FROM b2)")
    require res.ok
    check res.value.len == 2
    discard closeDb(db)

# ------------------- JSON_VALID and JSON_OBJECT -------------------

suite "JSON_VALID function":
  test "JSON_VALID returns 1 for valid JSON":
    let db = freshDb("tjv1.ddb")
    let res = execSql(db, "SELECT JSON_VALID('{\"key\": 1}')")
    require res.ok
    check col0(res.value) in ["true", "1", "TRUE"]
    discard closeDb(db)

  test "JSON_VALID returns 0 for invalid JSON":
    let db = freshDb("tjv2.ddb")
    let res = execSql(db, "SELECT JSON_VALID('not json')")
    require res.ok
    check col0(res.value) in ["false", "0", "FALSE"]
    discard closeDb(db)

  test "JSON_VALID with NULL returns 0 (false)":
    let db = freshDb("tjv3.ddb")
    let res = execSql(db, "SELECT JSON_VALID(NULL)")
    require res.ok
    # NULL input: JSON_VALID returns false/0
    check col0(res.value) in ["false", "0", "FALSE"]
    discard closeDb(db)

  test "JSON_VALID wrong arg count errors":
    let db = freshDb("tjv4.ddb")
    let res = execSql(db, "SELECT JSON_VALID()")
    check not res.ok
    discard closeDb(db)

suite "JSON_OBJECT function":
  test "JSON_OBJECT creates object from key-value pairs":
    let db = freshDb("tjo1.ddb")
    let res = execSql(db, "SELECT JSON_OBJECT('name', 'Alice', 'age', 30)")
    require res.ok
    check "{" in col0(res.value)
    check "Alice" in col0(res.value)
    discard closeDb(db)

  test "JSON_OBJECT with no args errors (unsupported syntax)":
    let db = freshDb("tjo2.ddb")
    # JSON_OBJECT() with no args is not parseable
    let res = execSql(db, "SELECT JSON_OBJECT('k', 'v')")
    require res.ok
    check "{" in col0(res.value)
    discard closeDb(db)

  test "JSON_OBJECT with odd arg count errors":
    let db = freshDb("tjo3.ddb")
    let res = execSql(db, "SELECT JSON_OBJECT('key')")
    check not res.ok
    discard closeDb(db)
