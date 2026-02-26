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

  # ── Date/Time Functions (Phase 2) ────────────────────────────
  test "NOW() returns timestamp":
    let db = openDb(makeTempDb("test_funcs_now.ddb")).value
    let res = execSql(db, "SELECT NOW()")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].len == 26  # YYYY-MM-DD HH:MM:SS.ffffff
    check row[0][4] == '-'
    check row[0][7] == '-'
    check row[0][10] == ' '
    check row[0][13] == ':'
    discard closeDb(db)

  test "CURRENT_TIMESTAMP returns timestamp":
    let db = openDb(makeTempDb("test_funcs_current_timestamp.ddb")).value
    let res = execSql(db, "SELECT CURRENT_TIMESTAMP")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].len == 26  # YYYY-MM-DD HH:MM:SS.ffffff
    check row[0][4] == '-'
    discard closeDb(db)

  test "CURRENT_DATE returns date":
    let db = openDb(makeTempDb("test_funcs_current_date.ddb")).value
    let res = execSql(db, "SELECT CURRENT_DATE")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].len == 10  # YYYY-MM-DD
    check row[0][4] == '-'
    check row[0][7] == '-'
    discard closeDb(db)

  test "CURRENT_TIME returns time":
    let db = openDb(makeTempDb("test_funcs_current_time.ddb")).value
    let res = execSql(db, "SELECT CURRENT_TIME")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].len == 8  # HH:MM:SS
    check row[0][2] == ':'
    check row[0][5] == ':'
    discard closeDb(db)

  test "date('now') returns date":
    let db = openDb(makeTempDb("test_funcs_date_now.ddb")).value
    let res = execSql(db, "SELECT date('now')")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].len == 10
    check row[0][4] == '-'
    discard closeDb(db)

  test "datetime('now') returns timestamp":
    let db = openDb(makeTempDb("test_funcs_datetime_now.ddb")).value
    let res = execSql(db, "SELECT datetime('now')")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].len == 19
    check row[0][4] == '-'
    discard closeDb(db)

  test "EXTRACT YEAR FROM timestamp":
    let db = openDb(makeTempDb("test_funcs_extract_year.ddb")).value
    let res = execSql(db, "SELECT EXTRACT(YEAR FROM CURRENT_TIMESTAMP)")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].len == 4  # Should be 2026
    discard closeDb(db)

  test "EXTRACT MONTH FROM date":
    let db = openDb(makeTempDb("test_funcs_extract_month.ddb")).value
    let res = execSql(db, "SELECT EXTRACT(MONTH FROM CURRENT_DATE)")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].parseInt in 1..12
    discard closeDb(db)

  test "strftime format":
    let db = openDb(makeTempDb("test_funcs_strftime.ddb")).value
    let res = execSql(db, "SELECT strftime('%Y-%m-%d', 'now')")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].len == 10
    check row[0][4] == '-'
    discard closeDb(db)

  # ── Phase 3: Additional Math Functions ────────────────────────
  test "SIGN of positive":
    let db = openDb(makeTempDb("test_funcs_sign_pos.ddb")).value
    let res = execSql(db, "SELECT SIGN(42)")
    check res.ok
    check splitRow(res.value[0])[0] == "1"
    discard closeDb(db)

  test "SIGN of negative":
    let db = openDb(makeTempDb("test_funcs_sign_neg.ddb")).value
    let res = execSql(db, "SELECT SIGN(-42)")
    check res.ok
    check splitRow(res.value[0])[0] == "-1"
    discard closeDb(db)

  test "SIGN of zero":
    let db = openDb(makeTempDb("test_funcs_sign_zero.ddb")).value
    let res = execSql(db, "SELECT SIGN(0)")
    check res.ok
    check splitRow(res.value[0])[0] == "0"
    discard closeDb(db)

  test "LN of e":
    let db = openDb(makeTempDb("test_funcs_ln.ddb")).value
    let res = execSql(db, "SELECT LN(2.718281828)")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].parseFloat > 0.99 and row[0].parseFloat < 1.01
    discard closeDb(db)

  test "LOG10 of 100":
    let db = openDb(makeTempDb("test_funcs_log10.ddb")).value
    let res = execSql(db, "SELECT LOG(100)")
    check res.ok
    check splitRow(res.value[0])[0] == "2.0"
    discard closeDb(db)

  test "EXP(1)":
    let db = openDb(makeTempDb("test_funcs_exp.ddb")).value
    let res = execSql(db, "SELECT EXP(1)")
    check res.ok
    let row = splitRow(res.value[0])
    check row[0].parseFloat > 2.71 and row[0].parseFloat < 2.72
    discard closeDb(db)

  test "RANDOM returns float in [0,1)":
    let db = openDb(makeTempDb("test_funcs_random.ddb")).value
    let res = execSql(db, "SELECT RANDOM()")
    check res.ok
    let row = splitRow(res.value[0])
    let v = row[0].parseFloat
    check v >= 0.0 and v < 1.0
    discard closeDb(db)

  # ── Phase 3: String Functions ─────────────────────────────────
  test "LTRIM removes leading spaces":
    let db = openDb(makeTempDb("test_funcs_ltrim.ddb")).value
    let res = execSql(db, "SELECT LTRIM('  hello')")
    check res.ok
    check splitRow(res.value[0])[0] == "hello"
    discard closeDb(db)

  test "RTRIM removes trailing spaces":
    let db = openDb(makeTempDb("test_funcs_rtrim.ddb")).value
    let res = execSql(db, "SELECT RTRIM('hello  ')")
    check res.ok
    check splitRow(res.value[0])[0] == "hello"
    discard closeDb(db)

  test "LEFT returns first n chars":
    let db = openDb(makeTempDb("test_funcs_left.ddb")).value
    let res = execSql(db, "SELECT LEFT('hello', 3)")
    check res.ok
    check splitRow(res.value[0])[0] == "hel"
    discard closeDb(db)

  test "RIGHT returns last n chars":
    let db = openDb(makeTempDb("test_funcs_right.ddb")).value
    let res = execSql(db, "SELECT RIGHT('hello', 3)")
    check res.ok
    check splitRow(res.value[0])[0] == "llo"
    discard closeDb(db)

  test "LPAD pads left":
    let db = openDb(makeTempDb("test_funcs_lpad.ddb")).value
    let res = execSql(db, "SELECT LPAD('hi', 5, '*')")
    check res.ok
    check splitRow(res.value[0])[0] == "***hi"
    discard closeDb(db)

  test "RPAD pads right":
    let db = openDb(makeTempDb("test_funcs_rpad.ddb")).value
    let res = execSql(db, "SELECT RPAD('hi', 5, '*')")
    check res.ok
    check splitRow(res.value[0])[0] == "hi***"
    discard closeDb(db)

  test "REPEAT repeats string":
    let db = openDb(makeTempDb("test_funcs_repeat.ddb")).value
    let res = execSql(db, "SELECT REPEAT('ab', 3)")
    check res.ok
    check splitRow(res.value[0])[0] == "ababab"
    discard closeDb(db)

  test "REVERSE reverses string":
    let db = openDb(makeTempDb("test_funcs_reverse.ddb")).value
    let res = execSql(db, "SELECT REVERSE('hello')")
    check res.ok
    check splitRow(res.value[0])[0] == "olleh"
    discard closeDb(db)

  # ── DISTINCT aggregates ──────────────────────────────────

  test "COUNT(DISTINCT) returns unique count":
    let db = openDb(makeTempDb("test_count_distinct.ddb")).value
    discard execSql(db, "CREATE TABLE td (id INTEGER PRIMARY KEY, val INTEGER)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (1, 10)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (2, 10)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (3, 20)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (4, 20)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (5, 30)")
    let res = execSql(db, "SELECT COUNT(DISTINCT val) FROM td")
    check res.ok
    check res.value == @["3"]
    discard closeDb(db)

  test "SUM(DISTINCT) returns sum of unique values":
    let db = openDb(makeTempDb("test_sum_distinct.ddb")).value
    discard execSql(db, "CREATE TABLE td (id INTEGER PRIMARY KEY, val INTEGER)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (1, 10)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (2, 10)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (3, 20)")
    let res = execSql(db, "SELECT SUM(DISTINCT val) FROM td")
    check res.ok
    check res.value == @["30"]
    discard closeDb(db)

  test "AVG(DISTINCT) returns average of unique values":
    let db = openDb(makeTempDb("test_avg_distinct.ddb")).value
    discard execSql(db, "CREATE TABLE td (id INTEGER PRIMARY KEY, val INTEGER)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (1, 10)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (2, 10)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (3, 20)")
    let res = execSql(db, "SELECT AVG(DISTINCT val) FROM td")
    check res.ok
    check res.value == @["15.0"]
    discard closeDb(db)

  test "COUNT(DISTINCT) with NULLs excludes NULLs":
    let db = openDb(makeTempDb("test_count_distinct_null.ddb")).value
    discard execSql(db, "CREATE TABLE td (id INTEGER PRIMARY KEY, val INTEGER)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (1, 10)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (2, NULL)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (3, 10)")
    discard execSql(db, "INSERT INTO td (id, val) VALUES (4, NULL)")
    let res = execSql(db, "SELECT COUNT(DISTINCT val) FROM td")
    check res.ok
    check res.value == @["1"]
    discard closeDb(db)

  test "COUNT(DISTINCT) on empty set returns 0":
    let db = openDb(makeTempDb("test_count_distinct_empty.ddb")).value
    discard execSql(db, "CREATE TABLE td (id INTEGER PRIMARY KEY, val INTEGER)")
    let res = execSql(db, "SELECT COUNT(DISTINCT val) FROM td")
    check res.ok
    check res.value == @["0"]
    discard closeDb(db)

  # ── LOG two-argument form ────────────────────────────────

  test "LOG(base, x) arbitrary base":
    let db = openDb(makeTempDb("test_funcs_log2arg.ddb")).value
    let res = execSql(db, "SELECT LOG(2, 8)")
    check res.ok
    let val = parseFloat(splitRow(res.value[0])[0])
    check abs(val - 3.0) < 0.001
    discard closeDb(db)

  test "LOG(base, x) with NULL returns NULL":
    let db = openDb(makeTempDb("test_funcs_log2arg_null.ddb")).value
    let res = execSql(db, "SELECT LOG(2, NULL)")
    check res.ok
    check splitRow(res.value[0])[0] == "NULL"
    discard closeDb(db)

  # ── JSON functions ───────────────────────────────────────

  test "-> operator extracts JSON value":
    let db = openDb(makeTempDb("test_json_arrow.ddb")).value
    let res = execSql(db, "SELECT '{\"a\":1}'->'a'")
    check res.ok
    check splitRow(res.value[0])[0] == "1"
    discard closeDb(db)

  test "->> operator extracts as TEXT":
    let db = openDb(makeTempDb("test_json_arrow2.ddb")).value
    let res = execSql(db, """SELECT '{"a":"hello"}'->>'a'""")
    check res.ok
    # ->> unquotes string values
    check splitRow(res.value[0])[0] == "hello"
    discard closeDb(db)

  test "-> with NULL returns NULL":
    let db = openDb(makeTempDb("test_json_arrow_null.ddb")).value
    let res = execSql(db, "SELECT NULL->'a'")
    check res.ok
    check splitRow(res.value[0])[0] == "NULL"
    discard closeDb(db)

  test "-> with missing key returns NULL":
    let db = openDb(makeTempDb("test_json_arrow_miss.ddb")).value
    let res = execSql(db, "SELECT '{\"a\":1}'->'b'")
    check res.ok
    check splitRow(res.value[0])[0] == "NULL"
    discard closeDb(db)

  test "json_type returns correct type":
    let db = openDb(makeTempDb("test_json_type.ddb")).value
    let res = execSql(db, "SELECT json_type('{\"a\":1}')")
    check res.ok
    check splitRow(res.value[0])[0] == "object"
    discard closeDb(db)

  test "json_type of NULL returns NULL":
    let db = openDb(makeTempDb("test_json_type_null.ddb")).value
    let res = execSql(db, "SELECT json_type(NULL)")
    check res.ok
    check splitRow(res.value[0])[0] == "NULL"
    discard closeDb(db)

  test "json_valid returns 1 for valid JSON":
    let db = openDb(makeTempDb("test_json_valid.ddb")).value
    let res = execSql(db, "SELECT json_valid('{\"a\":1}')")
    check res.ok
    check splitRow(res.value[0])[0] == "1"
    discard closeDb(db)

  test "json_valid returns 0 for invalid JSON":
    let db = openDb(makeTempDb("test_json_invalid.ddb")).value
    let res = execSql(db, "SELECT json_valid('not json')")
    check res.ok
    check splitRow(res.value[0])[0] == "0"
    discard closeDb(db)

  test "json_object creates JSON object":
    let db = openDb(makeTempDb("test_json_object.ddb")).value
    let res = execSql(db, "SELECT json_object('key', 'val')")
    check res.ok
    let v = splitRow(res.value[0])[0]
    check v.contains("key")
    check v.contains("val")
    discard closeDb(db)

  test "json_array creates JSON array":
    let db = openDb(makeTempDb("test_json_array.ddb")).value
    let res = execSql(db, "SELECT JSON_ARRAY(1, 2, 3)")
    check res.ok
    check res.value == @["[1,2,3]"]
    discard closeDb(db)

  test "json_array with NULL":
    let db = openDb(makeTempDb("test_json_array_null.ddb")).value
    let res = execSql(db, "SELECT JSON_ARRAY(1, NULL, 3)")
    check res.ok
    check res.value == @["[1,null,3]"]
    discard closeDb(db)

  test "json_array empty":
    let db = openDb(makeTempDb("test_json_array_empty.ddb")).value
    let res = execSql(db, "SELECT JSON_ARRAY()")
    check res.ok
    check res.value == @["[]"]
    discard closeDb(db)
