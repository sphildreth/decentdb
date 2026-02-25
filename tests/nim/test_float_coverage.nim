## Coverage tests for float64 operations, ILIKE, float arithmetic,
## math functions on floats/decimals, compareValues float-vs-text.
## Targets exec.nim L156, L169, L2047-2066, L1745-1769, L3068-3073,
## L3109-3128, L3147, L3185-3192, L3350.
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

proc row0(rows: seq[string]): seq[string] =
  if rows.len == 0: return @[]
  rows[0].split("|")

# ---------------------------------------------------------------------------
# Float column creation and output (exercises L156 vkFloat64 -> string)
# ---------------------------------------------------------------------------
suite "Float column operations":
  test "SELECT from REAL column produces float string":
    let db = freshDb("tfloat_sel.ddb")
    discard execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, price REAL)")
    discard execSql(db, "INSERT INTO items VALUES (1, 3.14)")
    discard execSql(db, "INSERT INTO items VALUES (2, 2.718)")
    let res = execSql(db, "SELECT price FROM items ORDER BY id")
    require res.ok
    check res.value.len == 2
    check col0(res.value) == "3.14"
    check res.value[1].split("|")[0] == "2.718"
    discard closeDb(db)

  test "REAL column zero value":
    let db = freshDb("tfloat_zero.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 0.0)")
    let res = execSql(db, "SELECT v FROM t WHERE id = 1")
    require res.ok
    check col0(res.value) == "0.0"
    discard closeDb(db)

  test "REAL column negative value":
    let db = freshDb("tfloat_neg.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, -1.5)")
    let res = execSql(db, "SELECT v FROM t WHERE id = 1")
    require res.ok
    check col0(res.value) == "-1.5"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# Float arithmetic (exercises L2047-2066)
# ---------------------------------------------------------------------------
suite "Float arithmetic":
  test "float addition":
    let db = freshDb("tfloat_add.ddb")
    let res = execSql(db, "SELECT 1.5 + 2.5")
    require res.ok
    check col0(res.value) == "4.0"
    discard closeDb(db)

  test "float subtraction":
    let db = freshDb("tfloat_sub.ddb")
    let res = execSql(db, "SELECT 5.0 - 1.5")
    require res.ok
    check col0(res.value) == "3.5"
    discard closeDb(db)

  test "float multiplication":
    let db = freshDb("tfloat_mul.ddb")
    let res = execSql(db, "SELECT 2.0 * 3.0")
    require res.ok
    check col0(res.value) == "6.0"
    discard closeDb(db)

  test "float division":
    let db = freshDb("tfloat_div.ddb")
    let res = execSql(db, "SELECT 10.0 / 4.0")
    require res.ok
    check col0(res.value) == "2.5"
    discard closeDb(db)

  test "float modulo":
    let db = freshDb("tfloat_mod.ddb")
    let res = execSql(db, "SELECT 7.0 % 3.0")
    require res.ok
    check col0(res.value) == "1.0"
    discard closeDb(db)

  test "float division by zero errors":
    let db = freshDb("tfloat_divz.ddb")
    let res = execSql(db, "SELECT 1.0 / 0.0")
    check not res.ok
    discard closeDb(db)

  test "float mod by zero errors":
    let db = freshDb("tfloat_modz.ddb")
    let res = execSql(db, "SELECT 1.0 % 0.0")
    check not res.ok
    discard closeDb(db)

  test "float unsupported operator via column arithmetic":
    let db = freshDb("tfloat_uop.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 2.0)")
    # float with bitwise AND (unsupported) returns error
    let res = execSql(db, "SELECT v & 1 FROM t WHERE id = 1")
    check not res.ok
    discard closeDb(db)

  test "int + float promotes to float":
    let db = freshDb("tfloat_promo.ddb")
    let res = execSql(db, "SELECT 2 + 1.5")
    require res.ok
    check col0(res.value) == "3.5"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# Float column comparisons
# ---------------------------------------------------------------------------
suite "Float column comparisons":
  test "WHERE float_col > literal":
    let db = freshDb("tfloat_cmp1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 1.0)")
    discard execSql(db, "INSERT INTO t VALUES (2, 3.0)")
    discard execSql(db, "INSERT INTO t VALUES (3, 5.0)")
    let res = execSql(db, "SELECT id FROM t WHERE v > 2.0 ORDER BY id")
    require res.ok
    check res.value.len == 2
    check col0(res.value) == "2"
    discard closeDb(db)

  test "ORDER BY float column":
    let db = freshDb("tfloat_ord.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 3.0)")
    discard execSql(db, "INSERT INTO t VALUES (2, 1.0)")
    discard execSql(db, "INSERT INTO t VALUES (3, 2.0)")
    let res = execSql(db, "SELECT v FROM t ORDER BY v")
    require res.ok
    check res.value.len == 3
    check col0(res.value) == "1.0"
    check res.value[2].split("|")[0] == "3.0"
    discard closeDb(db)

  test "compareValues float column vs text literal (numeric text)":
    # Exercises the float-vs-text branch in compareValues (L1750)
    let db = freshDb("tfloat_cvtx.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 3.14)")
    # Comparing float column to text string that parses as number
    let res = execSql(db, "SELECT id FROM t WHERE v = '3.14'")
    require res.ok
    # May or may not return rows depending on type coercion, but must not error
    discard closeDb(db)

  test "compareValues text column vs float literal":
    # Exercises the text-vs-float branch in compareValues (L1764)
    let db = freshDb("tfloat_cvtx2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, '3.14')")
    let res = execSql(db, "SELECT id FROM t WHERE name = 3.14")
    require res.ok
    discard closeDb(db)

  test "compareValues float vs non-numeric text":
    # Text can't be parsed as number; exercises L1756-1757
    let db = freshDb("tfloat_cvtx3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 3.14)")
    let res = execSql(db, "SELECT id FROM t WHERE v = 'abc'")
    require res.ok
    check res.value.len == 0  # no match
    discard closeDb(db)

# ---------------------------------------------------------------------------
# Math functions on float/decimal (L3068-3073, L3109-3128, L3147, L3185-3192)
# ---------------------------------------------------------------------------
suite "Math functions on floats":
  test "ABS of float positive":
    let db = freshDb("tmf_absf1.ddb")
    let res = execSql(db, "SELECT ABS(2.5)")
    require res.ok
    check col0(res.value) == "2.5"
    discard closeDb(db)

  test "ABS of float negative":
    let db = freshDb("tmf_absf2.ddb")
    let res = execSql(db, "SELECT ABS(-2.5)")
    require res.ok
    check col0(res.value) == "2.5"
    discard closeDb(db)

  test "ABS of DECIMAL column":
    let db = freshDb("tmf_absd.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))")
    discard execSql(db, "INSERT INTO t VALUES (1, -3.50)")
    let res = execSql(db, "SELECT ABS(v) FROM t WHERE id = 1")
    require res.ok
    check col0(res.value) == "3.50"
    discard closeDb(db)

  test "ABS of non-numeric returns error":
    let db = freshDb("tmf_abserr.ddb")
    let res = execSql(db, "SELECT ABS('hello')")
    check not res.ok
    discard closeDb(db)

  test "ROUND of non-numeric returns error":
    let db = freshDb("tmf_rnderr.ddb")
    let res = execSql(db, "SELECT ROUND('hello', 1)")
    check not res.ok
    discard closeDb(db)

  test "CEIL of non-numeric returns error":
    let db = freshDb("tmf_cellerr.ddb")
    let res = execSql(db, "SELECT CEIL('hello')")
    check not res.ok
    discard closeDb(db)

  test "FLOOR of non-numeric returns error":
    let db = freshDb("tmf_florr.ddb")
    let res = execSql(db, "SELECT FLOOR('hello')")
    check not res.ok
    discard closeDb(db)

  test "SIGN of float positive":
    # Exercises L3185+
    let db = freshDb("tmf_signf1.ddb")
    let res = execSql(db, "SELECT SIGN(3.14)")
    require res.ok
    check col0(res.value) == "1"
    discard closeDb(db)

  test "SIGN of float negative":
    let db = freshDb("tmf_signf2.ddb")
    let res = execSql(db, "SELECT SIGN(-3.14)")
    require res.ok
    check col0(res.value) == "-1"
    discard closeDb(db)

  test "SIGN of float zero":
    let db = freshDb("tmf_signf0.ddb")
    let res = execSql(db, "SELECT SIGN(0.0)")
    require res.ok
    check col0(res.value) == "0"
    discard closeDb(db)

  test "SIGN of non-numeric errors":
    let db = freshDb("tmf_signerr.ddb")
    let res = execSql(db, "SELECT SIGN('hello')")
    check not res.ok
    discard closeDb(db)

  test "SQRT of float":
    let db = freshDb("tmf_sqrt.ddb")
    let res = execSql(db, "SELECT SQRT(4.0)")
    require res.ok
    check col0(res.value) == "2.0"
    discard closeDb(db)

  test "SQRT of non-numeric errors":
    let db = freshDb("tmf_sqrterr.ddb")
    let res = execSql(db, "SELECT SQRT('hello')")
    check not res.ok
    discard closeDb(db)

  test "MOD with non-numeric errors":
    # L3350: MOD requires numeric arguments
    let db = freshDb("tmf_moderr.ddb")
    let res = execSql(db, "SELECT MOD('a', 'b')")
    check not res.ok
    discard closeDb(db)

  test "CEIL of float":
    let db = freshDb("tmf_ceil.ddb")
    let res = execSql(db, "SELECT CEIL(2.1)")
    require res.ok
    check col0(res.value) == "3.0"
    discard closeDb(db)

  test "FLOOR of float":
    let db = freshDb("tmf_floor.ddb")
    let res = execSql(db, "SELECT FLOOR(2.9)")
    require res.ok
    check col0(res.value) == "2.0"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# ILIKE (case-insensitive LIKE, exercises L169)
# ---------------------------------------------------------------------------
suite "ILIKE case-insensitive":
  test "ILIKE matches case-insensitively":
    let db = freshDb("tilike1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'Hello World')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'HELLO NIM')")
    discard execSql(db, "INSERT INTO t VALUES (3, 'goodbye')")
    let res = execSql(db, "SELECT id FROM t WHERE name ILIKE '%hello%' ORDER BY id")
    require res.ok
    check res.value.len == 2
    check col0(res.value) == "1"
    discard closeDb(db)

  test "ILIKE no match":
    let db = freshDb("tilike2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'goodbye')")
    let res = execSql(db, "SELECT id FROM t WHERE name ILIKE '%hello%'")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "ILIKE with _ wildcard":
    let db = freshDb("tilike3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'Abc')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'xyz')")
    let res = execSql(db, "SELECT id FROM t WHERE name ILIKE 'a__'")
    require res.ok
    check res.value.len == 1
    check col0(res.value) == "1"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# GROUP BY with SUM on float column (exercises L2052+ float aggregation)
# ---------------------------------------------------------------------------
suite "Float aggregation":
  test "SUM of REAL column":
    let db = freshDb("tfloatagr.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a', 1.5)")
    discard execSql(db, "INSERT INTO t VALUES (2, 'a', 2.5)")
    discard execSql(db, "INSERT INTO t VALUES (3, 'b', 3.0)")
    let res = execSql(db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY cat")
    require res.ok
    check res.value.len == 2
    let r0 = res.value[0].split("|")
    check r0[0] == "a"
    check r0[1] == "4.0"
    discard closeDb(db)

  test "AVG of REAL column":
    let db = freshDb("tfloatagr2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v REAL)")
    discard execSql(db, "INSERT INTO t VALUES (1, 2.0)")
    discard execSql(db, "INSERT INTO t VALUES (2, 4.0)")
    let res = execSql(db, "SELECT AVG(v) FROM t")
    require res.ok
    check col0(res.value) == "3.0"
    discard closeDb(db)
