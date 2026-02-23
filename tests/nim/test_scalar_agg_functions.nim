import unittest
import os
import strutils
import engine
import record/record
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  for ext in ["-wal", ".wal", ""]:
    let f = path & ext
    if fileExists(f): removeFile(f)
  path

proc splitRow(row: string): seq[string] =
  if row.len == 0: return @[]
  row.split("|")

suite "Scalar & Aggregate Functions (issue #37)":
  # ── Math: SQRT ──────────────────────────────────────────
  test "SQRT of integer":
    let db = openDb(makeTempDb("test_funcs_sqrt.ddb")).value
    let res = execSql(db, "SELECT SQRT(16)")
    check res.ok
    check splitRow(res.value[0])[0] == "4.0"
    discard closeDb(db)

  test "SQRT of float":
    let db = openDb(makeTempDb("test_funcs_sqrt2.ddb")).value
    let res = execSql(db, "SELECT SQRT(2.25)")
    check res.ok
    check splitRow(res.value[0])[0] == "1.5"
    discard closeDb(db)

  test "SQRT of NULL returns NULL":
    let db = openDb(makeTempDb("test_funcs_sqrt_null.ddb")).value
    let res = execSql(db, "SELECT SQRT(NULL)")
    check res.ok
    check splitRow(res.value[0])[0] == "NULL"
    discard closeDb(db)

  test "SQRT of negative errors":
    let db = openDb(makeTempDb("test_funcs_sqrt_neg.ddb")).value
    let res = execSql(db, "SELECT SQRT(-1)")
    check not res.ok
    discard closeDb(db)

  # ── Math: POWER / POW ──────────────────────────────────
  test "POWER(2, 10) = 1024":
    let db = openDb(makeTempDb("test_funcs_power.ddb")).value
    let res = execSql(db, "SELECT POWER(2, 10)")
    check res.ok
    check splitRow(res.value[0])[0] == "1024.0"
    discard closeDb(db)

  test "POW alias works":
    let db = openDb(makeTempDb("test_funcs_pow.ddb")).value
    let res = execSql(db, "SELECT POW(3, 2)")
    check res.ok
    check splitRow(res.value[0])[0] == "9.0"
    discard closeDb(db)

  test "POWER with NULL returns NULL":
    let db = openDb(makeTempDb("test_funcs_power_null.ddb")).value
    let res = execSql(db, "SELECT POWER(NULL, 2)")
    check res.ok
    check splitRow(res.value[0])[0] == "NULL"
    discard closeDb(db)

  # ── Math: MOD function ─────────────────────────────────
  test "MOD(10, 3) = 1":
    let db = openDb(makeTempDb("test_funcs_mod.ddb")).value
    let res = execSql(db, "SELECT MOD(10, 3)")
    check res.ok
    check splitRow(res.value[0])[0] == "1"
    discard closeDb(db)

  test "MOD with floats":
    let db = openDb(makeTempDb("test_funcs_modf.ddb")).value
    let res = execSql(db, "SELECT MOD(10.5, 3.0)")
    check res.ok
    check splitRow(res.value[0])[0] == "1.5"
    discard closeDb(db)

  test "MOD divide by zero errors":
    let db = openDb(makeTempDb("test_funcs_mod0.ddb")).value
    let res = execSql(db, "SELECT MOD(10, 0)")
    check not res.ok
    discard closeDb(db)

  # ── Operator: % ─────────────────────────────────────────
  test "% operator integer":
    let db = openDb(makeTempDb("test_funcs_pct.ddb")).value
    let res = execSql(db, "SELECT 17 % 5")
    check res.ok
    check splitRow(res.value[0])[0] == "2"
    discard closeDb(db)

  test "% operator float":
    let db = openDb(makeTempDb("test_funcs_pctf.ddb")).value
    let res = execSql(db, "SELECT 10.5 % 3.0")
    check res.ok
    check splitRow(res.value[0])[0] == "1.5"
    discard closeDb(db)

  test "% divide by zero errors":
    let db = openDb(makeTempDb("test_funcs_pct0.ddb")).value
    let res = execSql(db, "SELECT 10 % 0")
    check not res.ok
    discard closeDb(db)

  # ── String: INSTR ───────────────────────────────────────
  test "INSTR finds substring":
    let db = openDb(makeTempDb("test_funcs_instr.ddb")).value
    let res = execSql(db, "SELECT INSTR('hello world', 'world')")
    check res.ok
    check splitRow(res.value[0])[0] == "7"
    discard closeDb(db)

  test "INSTR returns 0 when not found":
    let db = openDb(makeTempDb("test_funcs_instr2.ddb")).value
    let res = execSql(db, "SELECT INSTR('hello', 'xyz')")
    check res.ok
    check splitRow(res.value[0])[0] == "0"
    discard closeDb(db)

  test "INSTR with NULL returns NULL":
    let db = openDb(makeTempDb("test_funcs_instrn.ddb")).value
    let res = execSql(db, "SELECT INSTR(NULL, 'a')")
    check res.ok
    check splitRow(res.value[0])[0] == "NULL"
    discard closeDb(db)

  # ── String: CHR / CHAR ─────────────────────────────────
  test "CHR(65) = A":
    let db = openDb(makeTempDb("test_funcs_chr.ddb")).value
    let res = execSql(db, "SELECT CHR(65)")
    check res.ok
    check splitRow(res.value[0])[0] == "A"
    discard closeDb(db)

  test "CHR of zero codepoint":
    let db = openDb(makeTempDb("test_funcs_char.ddb")).value
    let res = execSql(db, "SELECT CHR(48)")
    check res.ok
    check splitRow(res.value[0])[0] == "0"
    discard closeDb(db)

  test "CHR with NULL returns NULL":
    let db = openDb(makeTempDb("test_funcs_chrn.ddb")).value
    let res = execSql(db, "SELECT CHR(NULL)")
    check res.ok
    check splitRow(res.value[0])[0] == "NULL"
    discard closeDb(db)

  # ── String: HEX ─────────────────────────────────────────
  test "HEX of integer":
    let db = openDb(makeTempDb("test_funcs_hex.ddb")).value
    let res = execSql(db, "SELECT HEX(255)")
    check res.ok
    check splitRow(res.value[0])[0] == "FF"
    discard closeDb(db)

  test "HEX of text":
    let db = openDb(makeTempDb("test_funcs_hext.ddb")).value
    let res = execSql(db, "SELECT HEX('AB')")
    check res.ok
    check splitRow(res.value[0])[0] == "4142"
    discard closeDb(db)

  test "HEX of NULL returns NULL":
    let db = openDb(makeTempDb("test_funcs_hexn.ddb")).value
    let res = execSql(db, "SELECT HEX(NULL)")
    check res.ok
    check splitRow(res.value[0])[0] == "NULL"
    discard closeDb(db)

  # ── Aggregate: TOTAL ────────────────────────────────────
  test "TOTAL on empty table returns 0.0":
    let db = openDb(makeTempDb("test_funcs_total_empty.ddb")).value
    check execSql(db, "CREATE TABLE t (val INT)").ok
    let res = execSql(db, "SELECT TOTAL(val) FROM t")
    check res.ok
    check splitRow(res.value[0])[0] == "0.0"
    discard closeDb(db)

  test "TOTAL sums values as float":
    let db = openDb(makeTempDb("test_funcs_total.ddb")).value
    check execSql(db, "CREATE TABLE t (val INT)").ok
    check execSql(db, "INSERT INTO t (val) VALUES (1)").ok
    check execSql(db, "INSERT INTO t (val) VALUES (2)").ok
    check execSql(db, "INSERT INTO t (val) VALUES (3)").ok
    let res = execSql(db, "SELECT TOTAL(val) FROM t")
    check res.ok
    check splitRow(res.value[0])[0] == "6.0"
    discard closeDb(db)

  test "TOTAL with NULLs ignores them":
    let db = openDb(makeTempDb("test_funcs_total_null.ddb")).value
    check execSql(db, "CREATE TABLE t (val INT)").ok
    check execSql(db, "INSERT INTO t (val) VALUES (5)").ok
    check execSql(db, "INSERT INTO t (val) VALUES (NULL)").ok
    check execSql(db, "INSERT INTO t (val) VALUES (10)").ok
    let res = execSql(db, "SELECT TOTAL(val) FROM t")
    check res.ok
    check splitRow(res.value[0])[0] == "15.0"
    discard closeDb(db)

  test "TOTAL vs SUM on empty table":
    let db = openDb(makeTempDb("test_funcs_total_vs_sum.ddb")).value
    check execSql(db, "CREATE TABLE t (val INT)").ok
    let totalRes = execSql(db, "SELECT TOTAL(val) FROM t")
    check totalRes.ok
    check splitRow(totalRes.value[0])[0] == "0.0"
    let sumRes = execSql(db, "SELECT SUM(val) FROM t")
    check sumRes.ok
    check splitRow(sumRes.value[0])[0] == "NULL"
    discard closeDb(db)

  # ── Math in table context ───────────────────────────────
  test "math functions on table column":
    let db = openDb(makeTempDb("test_funcs_math_col.ddb")).value
    check execSql(db, "CREATE TABLE t (val FLOAT)").ok
    check execSql(db, "INSERT INTO t (val) VALUES (25.0)").ok
    check execSql(db, "INSERT INTO t (val) VALUES (4.0)").ok
    let res = execSql(db, "SELECT SQRT(val), POWER(val, 2) FROM t ORDER BY val")
    check res.ok
    let row0 = splitRow(res.value[0])
    check row0[0] == "2.0"
    check row0[1] == "16.0"
    let row1 = splitRow(res.value[1])
    check row1[0] == "5.0"
    check row1[1] == "625.0"
    discard closeDb(db)
