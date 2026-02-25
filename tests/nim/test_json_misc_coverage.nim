## Coverage tests for json_type JBool/JInt/JFloat/JString types,
## json_each error paths, json_each with array type info,
## HEX of blob, IN list with NULL, GROUP_CONCAT with floats.
## Targets exec.nim L2544-2554, L2684-2691, L3444-3446, L2776-2785,
## L4972, L5005.
import unittest
import os
import strutils
import sequtils
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
# json_type with scalar JSON values (L2684-2687)
# ---------------------------------------------------------------------------
suite "json_type scalar types":
  test "json_type of boolean JSON":
    # L2684: JBool -> "boolean"
    let db = freshDb("tjt_bool.ddb")
    let res = execSql(db, "SELECT json_type('true')")
    require res.ok
    check col0(res.value) == "boolean"
    discard closeDb(db)

  test "json_type of integer JSON":
    # L2685: JInt -> "integer"
    let db = freshDb("tjt_int.ddb")
    let res = execSql(db, "SELECT json_type('42')")
    require res.ok
    check col0(res.value) == "integer"
    discard closeDb(db)

  test "json_type of float JSON":
    # L2686: JFloat -> "real"
    let db = freshDb("tjt_float.ddb")
    let res = execSql(db, "SELECT json_type('3.14')")
    require res.ok
    check col0(res.value) == "real"
    discard closeDb(db)

  test "json_type of string JSON":
    # L2687: JString -> "text"
    let db = freshDb("tjt_str.ddb")
    let res = execSql(db, """SELECT json_type('"hello"')""")
    require res.ok
    check col0(res.value) == "text"
    discard closeDb(db)

  test "json_type of null JSON":
    let db = freshDb("tjt_null.ddb")
    let res = execSql(db, "SELECT json_type('null')")
    require res.ok
    check col0(res.value) == "null"
    discard closeDb(db)

  test "json_type of invalid JSON returns null":
    # L2691: except JsonParsingError -> null
    let db = freshDb("tjt_inv.ddb")
    let res = execSql(db, "SELECT json_type('not-valid-json')")
    require res.ok
    check col0(res.value) == "NULL"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# json_each error paths (L4972, L5005)
# ---------------------------------------------------------------------------
suite "json_each error paths":
  test "json_each on invalid JSON errors":
    # L4972: JsonParsingError
    let db = freshDb("tje_inv.ddb")
    let res = execSql(db, "SELECT key FROM json_each('not-valid-json')")
    check not res.ok
    discard closeDb(db)

  test "json_each on plain number errors":
    # L5005: json_each requires object or array
    let db = freshDb("tje_num.ddb")
    let res = execSql(db, "SELECT key FROM json_each('42')")
    check not res.ok
    discard closeDb(db)

  test "json_each on plain string errors":
    let db = freshDb("tje_str.ddb")
    let res = execSql(db, """SELECT key FROM json_each('"hello"')""")
    check not res.ok
    discard closeDb(db)

  test "json_each array type column is correct":
    # Covers json_each array path type detection
    let db = freshDb("tje_arrtype.ddb")
    let res = execSql(db, """SELECT key, type FROM json_each('[true, 42, 3.14, "hi", null]')""")
    require res.ok
    check res.value.len == 5
    # Check type values
    let types = res.value.mapIt(it.split("|")[1])
    check types[0] == "boolean"
    check types[1] == "number"
    check types[2] == "number"
    check types[3] == "string"
    check types[4] == "null"
    discard closeDb(db)

  test "json_each array indices are correct":
    let db = freshDb("tje_arridx.ddb")
    let res = execSql(db, "SELECT key FROM json_each('[10,20,30]')")
    require res.ok
    check res.value.len == 3
    check col0(res.value) == "0"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# HEX of blob (L2544-2554)
# ---------------------------------------------------------------------------
suite "HEX function":
  test "HEX of blob column":
    # L2544-2547: HEX of blob
    let db = freshDb("thex_blob.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, data BLOB)")
    discard execSql(db, "INSERT INTO t VALUES (1, X'DEADBEEF')")
    let res = execSql(db, "SELECT HEX(data) FROM t WHERE id = 1")
    require res.ok
    check col0(res.value).toUpperAscii() == "DEADBEEF"
    discard closeDb(db)

  test "HEX of empty blob":
    let db = freshDb("thex_eblob.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, data BLOB)")
    discard execSql(db, "INSERT INTO t VALUES (1, X'')")
    let res = execSql(db, "SELECT HEX(data) FROM t WHERE id = 1")
    require res.ok
    check col0(res.value) == ""
    discard closeDb(db)

  test "HEX of text string":
    let db = freshDb("thex_txt.ddb")
    let res = execSql(db, "SELECT HEX('AB')")
    require res.ok
    check col0(res.value).toUpperAscii() == "4142"
    discard closeDb(db)

  test "HEX of integer":
    let db = freshDb("thex_int.ddb")
    let res = execSql(db, "SELECT HEX(255)")
    require res.ok
    check col0(res.value).toUpperAscii() == "FF"
    discard closeDb(db)

  test "HEX of null returns null":
    let db = freshDb("thex_null.ddb")
    let res = execSql(db, "SELECT HEX(NULL)")
    require res.ok
    check col0(res.value) == "NULL"
    discard closeDb(db)

  test "HEX of float errors":
    # L2554: HEX requires integer, text, or blob
    let db = freshDb("thex_ferr.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 3.14)")
    let res = execSql(db, "SELECT HEX(v) FROM t WHERE id = 1")
    check not res.ok
    discard closeDb(db)

# ---------------------------------------------------------------------------
# IN list with NULL (3-value logic, L3509-3511)
# ---------------------------------------------------------------------------
suite "IN list with NULL three-value logic":
  test "x IN (1, NULL, 2) with x not matching returns UNKNOWN (filters out)":
    # L3509-3510: sawNull=true, no match -> returns NULL (UNKNOWN)
    # In WHERE clause, UNKNOWN is treated as false -> row filtered out
    let db = freshDb("tin_null1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (5)")
    let res = execSql(db, "SELECT id FROM t WHERE id IN (1, NULL, 2)")
    require res.ok
    check res.value.len == 0  # 5 is not in (1, 2), NULL makes it UNKNOWN -> filtered
    discard closeDb(db)

  test "x IN (1, NULL, 2) with x matching returns true":
    let db = freshDb("tin_null2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    let res = execSql(db, "SELECT id FROM t WHERE id IN (1, NULL, 2)")
    require res.ok
    check res.value.len == 1  # 1 matches directly, no need to check NULL
    discard closeDb(db)

  test "x IN (NULL) with any x returns UNKNOWN":
    let db = freshDb("tin_null3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    discard execSql(db, "INSERT INTO t VALUES (2)")
    let res = execSql(db, "SELECT id FROM t WHERE id IN (NULL)")
    require res.ok
    check res.value.len == 0  # All UNKNOWN -> all filtered
    discard closeDb(db)

  test "NULL NOT IN (1, 2) returns UNKNOWN":
    let db = freshDb("tin_null4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, NULL)")
    let res = execSql(db, "SELECT id FROM t WHERE v IN (1, 2)")
    require res.ok
    check res.value.len == 0  # NULL IN anything is UNKNOWN
    discard closeDb(db)

# ---------------------------------------------------------------------------
# GROUP_CONCAT with float values (L2776-2785)
# ---------------------------------------------------------------------------
suite "GROUP_CONCAT with float values":
  test "GROUP_CONCAT of REAL column":
    # L2776: vkFloat64 case in GROUP_CONCAT
    let db = freshDb("tgcf.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 1.5)")
    discard execSql(db, "INSERT INTO t VALUES (2, 2.5)")
    let res = execSql(db, "SELECT GROUP_CONCAT(v) FROM t")
    require res.ok
    let result = col0(res.value)
    check "1.5" in result
    check "2.5" in result
    discard closeDb(db)

  test "GROUP_CONCAT float with custom separator":
    let db = freshDb("tgcf2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 3.14)")
    discard execSql(db, "INSERT INTO t VALUES (2, 2.71)")
    let res = execSql(db, "SELECT GROUP_CONCAT(v, '|') FROM t")
    require res.ok
    # rows[0] is the raw concatenated string; col0 would split on '|'
    check res.value.len == 1
    check "|" in res.value[0]
    discard closeDb(db)
