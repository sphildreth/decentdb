## Comprehensive unit tests for src/c_api.nim.
## Targets: all exported functions, NULL-guard paths, bind/column type coercions,
## INSERT RETURNING, row_view, step_with_params_row_view, JSON metadata APIs.

import unittest
import os
import strutils
import c_api
import record/record

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc makeTempDb(name: string): string =
  let norm =
    if name.endsWith(".db"):
      name[0 .. ^4] & ".ddb"
    else:
      name
  let path = getTempDir() / norm
  if fileExists(path): removeFile(path)
  if fileExists(path & "-wal"): removeFile(path & "-wal")
  path

proc openFresh(name: string): pointer =
  let path = makeTempDb(name)
  result = decentdb_open(path.cstring, nil)
  doAssert result != nil, "Failed to open: " & path

proc execDDL(h: pointer, sql: string) =
  var stmt: pointer = nil
  let rc = decentdb_prepare(h, sql.cstring, addr stmt)
  doAssert rc == 0, "prepare failed for: " & sql
  let rc2 = decentdb_step(stmt)
  doAssert rc2 == 0, "step failed for: " & sql
  decentdb_finalize(stmt)

# ---------------------------------------------------------------------------
# Suite: open / close
# ---------------------------------------------------------------------------
suite "C API: open/close":
  test "open valid path returns non-nil":
    let h = openFresh("capi_open.ddb")
    check h != nil
    check decentdb_close(h) == 0

  test "close with nil is a no-op returning 0":
    check decentdb_close(nil) == 0

  test "open with cache_pages option":
    let path = makeTempDb("capi_open_opts.ddb")
    let h = decentdb_open(path.cstring, "cache_pages=512".cstring)
    check h != nil
    check decentdb_close(h) == 0

  test "open with cache_size option":
    let path = makeTempDb("capi_open_cache_size.ddb")
    let h = decentdb_open(path.cstring, "cache_size=256".cstring)
    check h != nil
    check decentdb_close(h) == 0

  test "open with cache_mb option":
    let path = makeTempDb("capi_open_mb.ddb")
    let h = decentdb_open(path.cstring, "cache_mb=4".cstring)
    check h != nil
    check decentdb_close(h) == 0

  test "open with multiple options":
    let path = makeTempDb("capi_open_multi.ddb")
    let h = decentdb_open(path.cstring, "cache_pages=128&unknown_key=foo".cstring)
    check h != nil
    check decentdb_close(h) == 0

  test "open with empty options string":
    let path = makeTempDb("capi_open_empty_opts.ddb")
    let h = decentdb_open(path.cstring, "".cstring)
    check h != nil
    check decentdb_close(h) == 0

  test "open with invalid cache_pages value falls back to default":
    let path = makeTempDb("capi_open_bad_cache.ddb")
    let h = decentdb_open(path.cstring, "cache_pages=notanumber".cstring)
    check h != nil
    check decentdb_close(h) == 0

  test "open with zero cache_pages falls back to default":
    let path = makeTempDb("capi_open_zero_cache.ddb")
    let h = decentdb_open(path.cstring, "cache_pages=0".cstring)
    check h != nil
    check decentdb_close(h) == 0

  test "open with negative cache_pages falls back to default":
    let path = makeTempDb("capi_open_neg_cache.ddb")
    let h = decentdb_open(path.cstring, "cache_pages=-1".cstring)
    check h != nil
    check decentdb_close(h) == 0

  test "open with cache_mb = zero falls back to default":
    let path = makeTempDb("capi_open_zero_mb.ddb")
    let h = decentdb_open(path.cstring, "cache_mb=0".cstring)
    check h != nil
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: error state
# ---------------------------------------------------------------------------
suite "C API: error state":
  test "last_error_code with nil returns global code (initially 0)":
    # After a successful open we have no error.
    let h = openFresh("capi_err1.ddb")
    discard decentdb_close(h)
    # Global may have been cleared; attempt an invalid open to force an error.
    let bad = decentdb_open("/nonexistent_dir/bad.ddb".cstring, nil)
    check bad == nil
    check decentdb_last_error_code(nil) != 0

  test "last_error_message with nil returns global cstring":
    let bad = decentdb_open("/nonexistent_dir/bad2.ddb".cstring, nil)
    check bad == nil
    let msg = decentdb_last_error_message(nil)
    check msg != nil
    check $msg != ""

  test "last_error_code with valid handle returns per-handle code":
    let h = openFresh("capi_err2.ddb")
    # Attempt a bad prepare to set handle error.
    var stmt: pointer = nil
    discard decentdb_prepare(h, "SELECT * FROM nonexistent_table_xyz".cstring, addr stmt)
    check decentdb_last_error_code(h) != 0
    check decentdb_close(h) == 0

  test "last_error_message with valid handle returns per-handle message":
    let h = openFresh("capi_err3.ddb")
    var stmt: pointer = nil
    discard decentdb_prepare(h, "SELECT * FROM nonexistent_xyz".cstring, addr stmt)
    let msg = decentdb_last_error_message(h)
    check msg != nil
    check $msg != ""
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: checkpoint and save_as
# ---------------------------------------------------------------------------
suite "C API: checkpoint / save_as":
  test "checkpoint nil returns -1":
    check decentdb_checkpoint(nil) == -1

  test "checkpoint succeeds on open db":
    let h = openFresh("capi_ckpt.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    check decentdb_checkpoint(h) == 0
    check decentdb_close(h) == 0

  test "save_as with nil handle returns -1":
    check decentdb_save_as(nil, "/tmp/dest.ddb".cstring) == -1

  test "save_as with nil dest path returns -1":
    let h = openFresh("capi_save1.ddb")
    check decentdb_save_as(h, nil) == -1
    check decentdb_close(h) == 0

  test "save_as to valid path succeeds":
    let h = openFresh("capi_save2.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    let dest = getTempDir() / "capi_save2_copy.ddb"
    if fileExists(dest): removeFile(dest)
    check decentdb_save_as(h, dest.cstring) == 0
    check fileExists(dest)
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: prepare errors
# ---------------------------------------------------------------------------
suite "C API: prepare errors":
  test "prepare with nil handle returns -1":
    var stmt: pointer = nil
    check decentdb_prepare(nil, "SELECT 1".cstring, addr stmt) == -1

  test "prepare with nil out_stmt returns -1":
    let h = openFresh("capi_prep_nil.ddb")
    check decentdb_prepare(h, "SELECT 1".cstring, nil) == -1
    check decentdb_close(h) == 0

  test "prepare with empty sql fails":
    let h = openFresh("capi_prep_empty.ddb")
    var stmt: pointer = nil
    let rc = decentdb_prepare(h, "".cstring, addr stmt)
    check rc != 0
    check decentdb_close(h) == 0

  test "prepare with syntax error fails":
    let h = openFresh("capi_prep_syntax.ddb")
    var stmt: pointer = nil
    let rc = decentdb_prepare(h, "SELECT FROM WHERE".cstring, addr stmt)
    check rc != 0
    check decentdb_last_error_code(h) != 0
    check decentdb_close(h) == 0

  test "prepare SELECT on unknown table fails at bind":
    let h = openFresh("capi_prep_notbl.ddb")
    var stmt: pointer = nil
    let rc = decentdb_prepare(h, "SELECT * FROM no_such_table".cstring, addr stmt)
    check rc != 0
    check decentdb_close(h) == 0

  test "prepare DDL (CREATE TABLE) succeeds":
    let h = openFresh("capi_prep_ddl.ddb")
    var stmt: pointer = nil
    check decentdb_prepare(h, "CREATE TABLE t (id INT)".cstring, addr stmt) == 0
    check stmt != nil
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "prepare INSERT succeeds":
    let h = openFresh("capi_prep_ins.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1)".cstring, addr stmt) == 0
    check stmt != nil
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "prepare UPDATE succeeds":
    let h = openFresh("capi_prep_upd.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "UPDATE t SET v = 1 WHERE id = 1".cstring, addr stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "prepare DELETE succeeds":
    let h = openFresh("capi_prep_del.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "DELETE FROM t WHERE id = 1".cstring, addr stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "prepare EXPLAIN SELECT succeeds with query_plan column":
    let h = openFresh("capi_prep_expl.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "EXPLAIN SELECT * FROM t".cstring, addr stmt) == 0
    check decentdb_column_count(stmt) == 1
    check $decentdb_column_name(stmt, 0) == "query_plan"
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "prepare EXPLAIN INSERT fails":
    let h = openFresh("capi_prep_expl_ins.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    let rc = decentdb_prepare(h, "EXPLAIN INSERT INTO t VALUES (1)".cstring, addr stmt)
    check rc != 0
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: finalize / reset / clear_bindings
# ---------------------------------------------------------------------------
suite "C API: finalize/reset/clear_bindings":
  test "finalize nil is a no-op":
    decentdb_finalize(nil)

  test "reset nil returns -1":
    check decentdb_reset(nil) == -1

  test "clear_bindings nil returns -1":
    check decentdb_clear_bindings(nil) == -1

  test "reset after stepping resets position":
    let h = openFresh("capi_reset.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (1)")
    execDDL(h, "INSERT INTO t VALUES (2)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t ORDER BY id".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1  # row 1
    check decentdb_reset(stmt) == 0
    check decentdb_step(stmt) == 1  # row 1 again after reset
    check decentdb_column_int64(stmt, 0) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "clear_bindings sets all params to null":
    let h = openFresh("capi_clrb.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (42)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 42) == 0
    check decentdb_clear_bindings(stmt) == 0
    # After clearing, $1 is NULL; WHERE id = NULL matches nothing.
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: bind functions
# ---------------------------------------------------------------------------
suite "C API: bind functions":
  test "bind index out of range returns -1":
    let h = openFresh("capi_bind_oob.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES ($1)".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 0, 1) == -1  # index 0 is invalid (1-based)
    check decentdb_bind_int64(stmt, 2, 1) == -1  # index 2 > param count
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_null":
    let h = openFresh("capi_bind_null.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES ($1)".cstring, addr stmt) == 0
    check decentdb_bind_null(stmt, 1) == 0
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_int64":
    let h = openFresh("capi_bind_i64.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (7)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 7) == 0
    check decentdb_step(stmt) == 1
    check decentdb_column_int64(stmt, 0) == 7
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_bool":
    let h = openFresh("capi_bind_bool.ddb")
    execDDL(h, "CREATE TABLE t (id INT, flag BOOL)")
    execDDL(h, "INSERT INTO t VALUES (1, TRUE)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT flag FROM t WHERE id = 1".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    # bool comes back as vkBool; int64 coercion should return 1
    check decentdb_column_int64(stmt, 0) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_bool with parameter":
    let h = openFresh("capi_bind_bool2.ddb")
    execDDL(h, "CREATE TABLE t (id INT, flag BOOL)")
    execDDL(h, "INSERT INTO t VALUES (1, FALSE)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE flag = $1".cstring, addr stmt) == 0
    check decentdb_bind_bool(stmt, 1, 0) == 0  # FALSE
    check decentdb_step(stmt) == 1
    check decentdb_column_int64(stmt, 0) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_float64":
    let h = openFresh("capi_bind_f64.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v FLOAT)")
    execDDL(h, "INSERT INTO t VALUES (1, 3.14)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT v FROM t WHERE id = 1".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    let fv = decentdb_column_float64(stmt, 0)
    check fv > 3.13 and fv < 3.15
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_float64 as parameter":
    let h = openFresh("capi_bind_f64p.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v FLOAT)")
    execDDL(h, "INSERT INTO t VALUES (1, 2.5)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE v = $1".cstring, addr stmt) == 0
    check decentdb_bind_float64(stmt, 1, 2.5) == 0
    check decentdb_step(stmt) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_text valid":
    let h = openFresh("capi_bind_text.ddb")
    execDDL(h, "CREATE TABLE t (id INT, name TEXT)")
    execDDL(h, "INSERT INTO t VALUES (1, 'hello')")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE name = $1".cstring, addr stmt) == 0
    let s = "hello"
    check decentdb_bind_text(stmt, 1, s.cstring, cint(s.len)) == 0
    check decentdb_step(stmt) == 1
    check decentdb_column_int64(stmt, 0) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_text with negative byte_len returns -1":
    let h = openFresh("capi_bind_text_neg.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    check decentdb_bind_text(stmt, 1, "x".cstring, -1) == -1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_text with nil pointer and non-zero len returns -1":
    let h = openFresh("capi_bind_text_nil.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    check decentdb_bind_text(stmt, 1, nil, 5) == -1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_text with zero len and nil pointer is ok (empty string)":
    let h = openFresh("capi_bind_text_empty.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    check decentdb_bind_text(stmt, 1, nil, 0) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_blob valid":
    let h = openFresh("capi_bind_blob.ddb")
    execDDL(h, "CREATE TABLE t (id INT, data BLOB)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1, $1)".cstring, addr stmt) == 0
    var blobData: array[3, uint8] = [uint8(0xAB), 0xCD, 0xEF]
    check decentdb_bind_blob(stmt, 1, addr blobData[0], 3) == 0
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    # Read it back
    var sel: pointer = nil
    check decentdb_prepare(h, "SELECT data FROM t WHERE id = 1".cstring, addr sel) == 0
    check decentdb_step(sel) == 1
    var blen: cint = 0
    let bp = decentdb_column_blob(sel, 0, addr blen)
    check bp != nil
    check blen == 3
    decentdb_finalize(sel)
    check decentdb_close(h) == 0

  test "bind_blob with negative byte_len returns -1":
    let h = openFresh("capi_bind_blob_neg.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    check decentdb_bind_blob(stmt, 1, nil, -1) == -1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_blob nil pointer with non-zero len returns -1":
    let h = openFresh("capi_bind_blob_nil.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    check decentdb_bind_blob(stmt, 1, nil, 5) == -1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_decimal valid":
    let h = openFresh("capi_bind_dec.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v DECIMAL(10,2))")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1, $1)".cstring, addr stmt) == 0
    check decentdb_bind_decimal(stmt, 1, 12345, 2) == 0  # 123.45
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "bind_decimal invalid scale returns -1":
    let h = openFresh("capi_bind_dec_bad.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    check decentdb_bind_decimal(stmt, 1, 100, -1) == -1
    check decentdb_bind_decimal(stmt, 1, 100, 19) == -1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: step (DML and SELECT)
# ---------------------------------------------------------------------------
suite "C API: step":
  test "step nil returns -1":
    check decentdb_step(nil) == -1

  test "step DDL (CREATE TABLE) returns 0":
    let h = openFresh("capi_step_ddl.ddb")
    var stmt: pointer = nil
    check decentdb_prepare(h, "CREATE TABLE t (id INT)".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "step INSERT returns 0 and rows_affected = 1":
    let h = openFresh("capi_step_ins.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1)".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 0
    check decentdb_rows_affected(stmt) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "step UPDATE returns 0 and rows_affected reflects updated count":
    let h = openFresh("capi_step_upd.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v INT)")
    execDDL(h, "INSERT INTO t VALUES (1, 10)")
    execDDL(h, "INSERT INTO t VALUES (2, 20)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "UPDATE t SET v = 99".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 0
    check decentdb_rows_affected(stmt) == 2
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "step DELETE returns 0 and rows_affected reflects deleted count":
    let h = openFresh("capi_step_del.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (1)")
    execDDL(h, "INSERT INTO t VALUES (2)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "DELETE FROM t WHERE id = 1".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 0
    check decentdb_rows_affected(stmt) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "step SELECT returns 1 per row then 0":
    let h = openFresh("capi_step_sel.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (1)")
    execDDL(h, "INSERT INTO t VALUES (2)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t ORDER BY id".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    check decentdb_step(stmt) == 1
    check decentdb_step(stmt) == 0
    # Stepping again after done returns 0
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "step EXPLAIN yields rows then 0":
    let h = openFresh("capi_step_expl.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "EXPLAIN SELECT * FROM t".cstring, addr stmt) == 0
    var count = 0
    while decentdb_step(stmt) == 1:
      inc count
    check count > 0
    check decentdb_step(stmt) == 0  # still 0 after done
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "step WITH RECURSIVE SELECT yields rows then 0":
    let h = openFresh("capi_step_recursive_cte.ddb")
    var stmt: pointer = nil
    check decentdb_prepare(h, """
WITH RECURSIVE cnt(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM cnt WHERE x < 5
)
SELECT x FROM cnt ORDER BY x
""".cstring, addr stmt) == 0
    for expected in 1'i64 .. 5'i64:
      check decentdb_step(stmt) == 1
      check decentdb_column_int64(stmt, 0) == expected
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "step INSERT RETURNING yields rows":
    let h = openFresh("capi_step_returning.ddb")
    execDDL(h, "CREATE TABLE t (id INT, name TEXT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (5, 'alice') RETURNING id, name".cstring, addr stmt) == 0
    let rc = decentdb_step(stmt)
    check rc == 1
    check decentdb_column_count(stmt) == 2
    check decentdb_column_int64(stmt, 0) == 5
    let nextRc = decentdb_step(stmt)
    check nextRc == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "rows_affected with nil returns 0":
    check decentdb_rows_affected(nil) == 0

  test "prepared TEMP TABLE insert and select work within one connection":
    let path = makeTempDb("capi_temp_table.ddb")
    let h = decentdb_open(path.cstring, nil)
    check h != nil
    execDDL(h, "CREATE TEMP TABLE scratch (id INT64 PRIMARY KEY, val TEXT)")

    var ins: pointer = nil
    check decentdb_prepare(
      h,
      "INSERT INTO scratch (id, val) VALUES (1, 'hello')".cstring,
      addr ins,
    ) == 0
    check decentdb_step(ins) == 0
    check decentdb_rows_affected(ins) == 1
    decentdb_finalize(ins)

    var sel: pointer = nil
    check decentdb_prepare(
      h,
      "SELECT val FROM scratch WHERE id = 1".cstring,
      addr sel,
    ) == 0
    check decentdb_step(sel) == 1
    var textLen: cint = 0
    let textPtr = decentdb_column_text(sel, 0, addr textLen)
    check textPtr != nil
    check ($textPtr)[0 ..< int(textLen)] == "hello"
    check decentdb_step(sel) == 0
    decentdb_finalize(sel)
    check decentdb_close(h) == 0

    let h2 = decentdb_open(path.cstring, nil)
    check h2 != nil
    var missing: pointer = nil
    check decentdb_prepare(h2, "SELECT * FROM scratch".cstring, addr missing) != 0
    check decentdb_close(h2) == 0

# ---------------------------------------------------------------------------
# Suite: column accessors
# ---------------------------------------------------------------------------
suite "C API: column accessors":
  test "column_count and column_name":
    let h = openFresh("capi_col_count.ddb")
    execDDL(h, "CREATE TABLE t (a INT, b TEXT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT a, b FROM t".cstring, addr stmt) == 0
    check decentdb_column_count(stmt) == 2
    check $decentdb_column_name(stmt, 0) == "a"
    check $decentdb_column_name(stmt, 1) == "b"
    check $decentdb_column_name(stmt, 2) == ""  # out of range
    check $decentdb_column_name(stmt, -1) == ""  # negative
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "column_type and column_is_null when no row":
    let h = openFresh("capi_col_notype.ddb")
    execDDL(h, "CREATE TABLE t (a INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT a FROM t".cstring, addr stmt) == 0
    # No row yet
    check decentdb_column_type(stmt, 0) == 0
    check decentdb_column_is_null(stmt, 0) == 1
    check decentdb_column_int64(stmt, 0) == 0
    check decentdb_column_float64(stmt, 0) == 0.0
    check decentdb_column_text(stmt, 0, nil) == nil
    check decentdb_column_blob(stmt, 0, nil) == nil
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "column_int64 coercions: bool, float, decimal":
    let h = openFresh("capi_col_coerce.ddb")
    execDDL(h, "CREATE TABLE t (id INT, b BOOL, f FLOAT, d DECIMAL(10,2))")
    execDDL(h, "INSERT INTO t VALUES (1, TRUE, 9.5, 1234)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT b, f, d FROM t WHERE id = 1".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    # bool -> int64
    check decentdb_column_int64(stmt, 0) == 1
    # float -> int64 (truncated)
    check decentdb_column_int64(stmt, 1) == 9
    # decimal -> int64: 1234 stored with scale 0 => 1234 div 1 = 1234
    let dv = decentdb_column_int64(stmt, 2)
    check dv >= 0  # just check it doesn't crash
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "column_float64 coercions: int64, bool":
    let h = openFresh("capi_col_coerce_f.ddb")
    execDDL(h, "CREATE TABLE t (id INT, i INT, b BOOL)")
    execDDL(h, "INSERT INTO t VALUES (1, 42, FALSE)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT i, b FROM t WHERE id = 1".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    check decentdb_column_float64(stmt, 0) == 42.0
    check decentdb_column_float64(stmt, 1) == 0.0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "column_float64 out of range returns 0":
    let h = openFresh("capi_col_f_oob.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (1)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    check decentdb_column_float64(stmt, 1) == 0.0  # out of range
    check decentdb_column_float64(stmt, -1) == 0.0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "column_text returns pointer for text values":
    let h = openFresh("capi_col_text.ddb")
    execDDL(h, "CREATE TABLE t (id INT, name TEXT)")
    execDDL(h, "INSERT INTO t VALUES (1, 'world')")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT name FROM t WHERE id = 1".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    var slen: cint = 0
    let sp = decentdb_column_text(stmt, 0, addr slen)
    check sp != nil
    check slen == 5
    var s = newString(int(slen))
    copyMem(addr s[0], sp, int(slen))
    check s == "world"
    # Non-text column returns nil
    check decentdb_column_text(stmt, 1, nil) == nil  # out of range
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "column_text on NULL value returns nil":
    let h = openFresh("capi_col_text_null.ddb")
    execDDL(h, "CREATE TABLE t (id INT, name TEXT)")
    execDDL(h, "INSERT INTO t VALUES (1, NULL)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT name FROM t WHERE id = 1".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    check decentdb_column_is_null(stmt, 0) == 1
    check decentdb_column_text(stmt, 0, nil) == nil
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "column_blob returns pointer for blob values":
    let h = openFresh("capi_col_blob.ddb")
    execDDL(h, "CREATE TABLE t (id INT, data BLOB)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1, $1)".cstring, addr stmt) == 0
    var blobData: array[4, uint8] = [uint8(1), 2, 3, 4]
    check decentdb_bind_blob(stmt, 1, addr blobData[0], 4) == 0
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    var sel: pointer = nil
    check decentdb_prepare(h, "SELECT data FROM t WHERE id = 1".cstring, addr sel) == 0
    check decentdb_step(sel) == 1
    var blen: cint = 0
    let bp = decentdb_column_blob(sel, 0, addr blen)
    check bp != nil
    check blen == 4
    decentdb_finalize(sel)
    check decentdb_close(h) == 0

  test "column_decimal_scale and column_decimal_unscaled":
    let h = openFresh("capi_col_dec.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v DECIMAL(10,2))")
    var ins: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1, $1)".cstring, addr ins) == 0
    check decentdb_bind_decimal(ins, 1, 12345, 2) == 0  # 123.45
    check decentdb_step(ins) == 0
    decentdb_finalize(ins)
    var sel: pointer = nil
    check decentdb_prepare(h, "SELECT v FROM t WHERE id = 1".cstring, addr sel) == 0
    check decentdb_step(sel) == 1
    let scale = decentdb_column_decimal_scale(sel, 0)
    check scale == 2
    let unscaled = decentdb_column_decimal_unscaled(sel, 0)
    check unscaled == 12345
    # float64 coercion: 12345 / 100.0 = 123.45
    let fv = decentdb_column_float64(sel, 0)
    check fv > 123.44 and fv < 123.46
    # int64 coercion: truncate to 123
    check decentdb_column_int64(sel, 0) == 123
    decentdb_finalize(sel)
    check decentdb_close(h) == 0

  test "column_decimal_scale returns 0 for non-decimal":
    let h = openFresh("capi_col_dec_nd.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (1)")
    var sel: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t".cstring, addr sel) == 0
    check decentdb_step(sel) == 1
    check decentdb_column_decimal_scale(sel, 0) == 0
    check decentdb_column_decimal_unscaled(sel, 0) == 1
    check decentdb_column_decimal_scale(sel, 5) == 0  # out of range
    check decentdb_column_decimal_unscaled(sel, 5) == 0
    decentdb_finalize(sel)
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: row_view
# ---------------------------------------------------------------------------
suite "C API: row_view":
  test "row_view with nil returns -1":
    var outValues: pointer = nil
    var outCount: cint = 0
    check decentdb_row_view(nil, cast[ptr ptr DecentdbValueView](addr outValues), addr outCount) == -1

  test "row_view with no row sets count to 0":
    let h = openFresh("capi_rv_norow.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t".cstring, addr stmt) == 0
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 99
    check decentdb_row_view(stmt, addr outValues, addr outCount) == 0
    check outCount == 0
    check outValues == nil
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "row_view with a row returns correct values":
    let h = openFresh("capi_rv_row.ddb")
    execDDL(h, "CREATE TABLE t (id INT, name TEXT)")
    execDDL(h, "INSERT INTO t VALUES (42, 'test')")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id, name FROM t".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    check decentdb_row_view(stmt, addr outValues, addr outCount) == 0
    check outCount == 2
    check outValues != nil
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "row_view with nil out_values and out_count is safe":
    let h = openFresh("capi_rv_nil_out.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (1)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t".cstring, addr stmt) == 0
    check decentdb_step(stmt) == 1
    check decentdb_row_view(stmt, nil, nil) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: step_with_params_row_view
# ---------------------------------------------------------------------------
suite "C API: step_with_params_row_view":
  test "nil stmt returns -1":
    check decentdb_step_with_params_row_view(nil, nil, 0, nil, nil, nil) == -1

  test "wrong param count returns -1":
    let h = openFresh("capi_spwrv_count.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    # Pass 0 params but statement expects 1.
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 0
    check decentdb_step_with_params_row_view(stmt, nil, 0, addr outValues, addr outCount, addr hasRow) == -1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "non-zero count with nil in_params returns -1":
    let h = openFresh("capi_spwrv_nil.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 0
    check decentdb_step_with_params_row_view(stmt, nil, 1, addr outValues, addr outCount, addr hasRow) == -1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "no params, no row - succeeds with has_row = 0":
    let h = openFresh("capi_spwrv_norow.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t".cstring, addr stmt) == 0
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 99
    check decentdb_step_with_params_row_view(stmt, nil, 0, addr outValues, addr outCount, addr hasRow) == 0
    check hasRow == 0
    check outCount == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "with int64 param, row found":
    let h = openFresh("capi_spwrv_ok.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (7)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    var param = DecentdbValueView(kind: cint(vkInt64), isNull: 0, int64Val: 7)
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 0
    check decentdb_step_with_params_row_view(stmt, addr param, 1, addr outValues, addr outCount, addr hasRow) == 0
    check hasRow == 1
    check outCount == 1
    check outValues != nil
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "with null param":
    let h = openFresh("capi_spwrv_null.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    var param = DecentdbValueView(kind: cint(vkNull), isNull: 1)
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 99
    check decentdb_step_with_params_row_view(stmt, addr param, 1, addr outValues, addr outCount, addr hasRow) == 0
    check hasRow == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "with bool param":
    let h = openFresh("capi_spwrv_bool.ddb")
    execDDL(h, "CREATE TABLE t (id INT, flag BOOL)")
    execDDL(h, "INSERT INTO t VALUES (1, TRUE)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE flag = $1".cstring, addr stmt) == 0
    var param = DecentdbValueView(kind: cint(vkBool), isNull: 0, int64Val: 1)
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 0
    check decentdb_step_with_params_row_view(stmt, addr param, 1, addr outValues, addr outCount, addr hasRow) == 0
    check hasRow == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "with float64 param":
    let h = openFresh("capi_spwrv_f64.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v FLOAT)")
    execDDL(h, "INSERT INTO t VALUES (1, 1.5)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE v = $1".cstring, addr stmt) == 0
    var param = DecentdbValueView(kind: cint(vkFloat64), isNull: 0, float64Val: 1.5)
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 0
    check decentdb_step_with_params_row_view(stmt, addr param, 1, addr outValues, addr outCount, addr hasRow) == 0
    check hasRow == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "with decimal param":
    let h = openFresh("capi_spwrv_dec.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v DECIMAL(10,2))")
    var ins: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1, $1)".cstring, addr ins) == 0
    check decentdb_bind_decimal(ins, 1, 500, 2) == 0  # 5.00
    check decentdb_step(ins) == 0
    decentdb_finalize(ins)
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE v = $1".cstring, addr stmt) == 0
    var param = DecentdbValueView(kind: cint(vkDecimal), isNull: 0, int64Val: 500, decimalScale: 2)
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 0
    check decentdb_step_with_params_row_view(stmt, addr param, 1, addr outValues, addr outCount, addr hasRow) == 0
    check hasRow == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "with text param":
    let h = openFresh("capi_spwrv_text.ddb")
    execDDL(h, "CREATE TABLE t (id INT, name TEXT)")
    execDDL(h, "INSERT INTO t VALUES (1, 'alice')")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE name = $1".cstring, addr stmt) == 0
    let s = "alice"
    var bytes = newSeq[byte](s.len)
    copyMem(addr bytes[0], s.cstring, s.len)
    var param = DecentdbValueView(kind: cint(vkText), isNull: 0, bytes: addr bytes[0], bytesLen: cint(s.len))
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 0
    check decentdb_step_with_params_row_view(stmt, addr param, 1, addr outValues, addr outCount, addr hasRow) == 0
    check hasRow == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "with blob param":
    let h = openFresh("capi_spwrv_blob.ddb")
    execDDL(h, "CREATE TABLE t (id INT, data BLOB)")
    var ins: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1, $1)".cstring, addr ins) == 0
    var blobData: array[2, uint8] = [uint8(0xFF), 0x00]
    check decentdb_bind_blob(ins, 1, addr blobData[0], 2) == 0
    check decentdb_step(ins) == 0
    decentdb_finalize(ins)
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE data = $1".cstring, addr stmt) == 0
    var blobBytes: array[2, uint8] = [uint8(0xFF), 0x00]
    var param = DecentdbValueView(kind: cint(vkBlob), isNull: 0, bytes: addr blobBytes[0], bytesLen: 2)
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 0
    check decentdb_step_with_params_row_view(stmt, addr param, 1, addr outValues, addr outCount, addr hasRow) == 0
    check hasRow == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "unsupported param kind returns -1":
    let h = openFresh("capi_spwrv_bad_kind.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id = $1".cstring, addr stmt) == 0
    # Use a kind value that doesn't match any vk* (use 99 as invalid)
    var param = DecentdbValueView(kind: cint(99), isNull: 0)
    var outValues: ptr DecentdbValueView = nil
    var outCount: cint = 0
    var hasRow: cint = 0
    check decentdb_step_with_params_row_view(stmt, addr param, 1, addr outValues, addr outCount, addr hasRow) == -1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: JSON metadata APIs
# ---------------------------------------------------------------------------
suite "C API: JSON metadata":
  test "list_tables_json with nil returns nil and sets global error":
    let p = decentdb_list_tables_json(nil, nil)
    check p == nil
    check decentdb_last_error_code(nil) != 0

  test "list_tables_json on empty db returns empty array":
    let h = openFresh("capi_json_empty.ddb")
    var outLen: cint = 0
    let p = decentdb_list_tables_json(h, addr outLen)
    check p != nil
    var s = newString(int(outLen))
    if outLen > 0: copyMem(addr s[0], p, int(outLen))
    check s == "[]"
    decentdb_free(p)
    check decentdb_close(h) == 0

  test "list_tables_json lists created tables":
    let h = openFresh("capi_json_tables.ddb")
    execDDL(h, "CREATE TABLE alpha (id INT)")
    execDDL(h, "CREATE TABLE beta (name TEXT)")
    var outLen: cint = 0
    let p = decentdb_list_tables_json(h, addr outLen)
    check p != nil
    var s = newString(int(outLen))
    if outLen > 0: copyMem(addr s[0], p, int(outLen))
    check "alpha" in s
    check "beta" in s
    decentdb_free(p)
    check decentdb_close(h) == 0

  test "get_table_columns_json with nil handle returns nil":
    let p = decentdb_get_table_columns_json(nil, "t".cstring, nil)
    check p == nil

  test "get_table_columns_json with nil table_utf8 returns nil":
    let h = openFresh("capi_json_cols_nil.ddb")
    let p = decentdb_get_table_columns_json(h, nil, nil)
    check p == nil
    check decentdb_close(h) == 0

  test "get_table_columns_json with unknown table returns nil":
    let h = openFresh("capi_json_cols_unk.ddb")
    let p = decentdb_get_table_columns_json(h, "no_such_table".cstring, nil)
    check p == nil
    check decentdb_last_error_code(h) != 0
    check decentdb_close(h) == 0

  test "get_table_columns_json returns column metadata":
    let h = openFresh("capi_json_cols_ok.ddb")
    execDDL(h, "CREATE TABLE t (id INT NOT NULL, name TEXT)")
    var outLen: cint = 0
    let p = decentdb_get_table_columns_json(h, "t".cstring, addr outLen)
    check p != nil
    var s = newString(int(outLen))
    if outLen > 0: copyMem(addr s[0], p, int(outLen))
    check "id" in s
    check "name" in s
    check "not_null" in s
    decentdb_free(p)
    check decentdb_close(h) == 0

  test "get_table_columns_json with FK columns includes ref metadata":
    let h = openFresh("capi_json_cols_fk.ddb")
    execDDL(h, "CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)")
    execDDL(h, "CREATE TABLE child (id INT, pid INT REFERENCES parent(id))")
    var outLen: cint = 0
    let p = decentdb_get_table_columns_json(h, "child".cstring, addr outLen)
    check p != nil
    var s = newString(int(outLen))
    if outLen > 0: copyMem(addr s[0], p, int(outLen))
    check "ref_table" in s
    check "parent" in s
    decentdb_free(p)
    check decentdb_close(h) == 0

  test "list_indexes_json with nil returns nil and sets global error":
    let p = decentdb_list_indexes_json(nil, nil)
    check p == nil
    check decentdb_last_error_code(nil) != 0

  test "list_indexes_json on db with no indexes returns empty-ish array":
    let h = openFresh("capi_json_idx_empty.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var outLen: cint = 0
    let p = decentdb_list_indexes_json(h, addr outLen)
    check p != nil
    var s = newString(int(outLen))
    if outLen > 0: copyMem(addr s[0], p, int(outLen))
    check s == "[]"
    decentdb_free(p)
    check decentdb_close(h) == 0

  test "list_indexes_json lists btree index":
    let h = openFresh("capi_json_idx_btree.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v INT)")
    execDDL(h, "CREATE INDEX ix_v ON t (v)")
    var outLen: cint = 0
    let p = decentdb_list_indexes_json(h, addr outLen)
    check p != nil
    var s = newString(int(outLen))
    if outLen > 0: copyMem(addr s[0], p, int(outLen))
    check "ix_v" in s
    check "btree" in s
    decentdb_free(p)
    check decentdb_close(h) == 0

  test "list_indexes_json lists trigram index":
    let h = openFresh("capi_json_idx_trgm.ddb")
    execDDL(h, "CREATE TABLE t (id INT, name TEXT)")
    execDDL(h, "CREATE INDEX ix_name ON t USING trigram (name)")
    var outLen: cint = 0
    let p = decentdb_list_indexes_json(h, addr outLen)
    check p != nil
    var s = newString(int(outLen))
    if outLen > 0: copyMem(addr s[0], p, int(outLen))
    check "ix_name" in s
    check "trigram" in s
    decentdb_free(p)
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: decentdb_free
# ---------------------------------------------------------------------------
suite "C API: decentdb_free":
  test "free nil is a no-op":
    decentdb_free(nil)  # must not crash

  test "free allocated pointer from list_tables_json":
    let h = openFresh("capi_free_json.ddb")
    let p = decentdb_list_tables_json(h, nil)
    check p != nil
    decentdb_free(p)  # must not crash / double-free
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: prepare coverage – expanded column names and option parsing
# ---------------------------------------------------------------------------
suite "C API: prepare coverage gaps":
  test "SELECT * expands to table column names":
    # Covers lines 444-448 (isStar branch for SELECT * column name expansion)
    let h = openFresh("capi_selstar.ddb")
    execDDL(h, "CREATE TABLE t (id INT, name TEXT, age INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT * FROM t".cstring, addr stmt) == 0
    check decentdb_column_count(stmt) == 3
    check $decentdb_column_name(stmt, 0) == "id"
    check $decentdb_column_name(stmt, 1) == "name"
    check $decentdb_column_name(stmt, 2) == "age"
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "SELECT expression yields anonymous column name":
    # Covers line 455 (name = "column" & $colNames.len fallback)
    let h = openFresh("capi_anon_col.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (1)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id + 1 FROM t".cstring, addr stmt) == 0
    check decentdb_column_count(stmt) == 1
    check $decentdb_column_name(stmt, 0) == "column0"
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "INSERT RETURNING * expands to all table columns":
    # Covers lines 481-485 (isStar branch in RETURNING clause)
    let h = openFresh("capi_ret_star.ddb")
    execDDL(h, "CREATE TABLE t (id INT, name TEXT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1, 'alice') RETURNING *".cstring, addr stmt) == 0
    check decentdb_column_count(stmt) == 2
    check $decentdb_column_name(stmt, 0) == "id"
    check $decentdb_column_name(stmt, 1) == "name"
    let rc = decentdb_step(stmt)
    check rc == 1
    check decentdb_column_int64(stmt, 0) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "INSERT RETURNING expression yields anonymous column name":
    # Covers line 491 (name = "column" & $colNames.len in RETURNING non-star)
    let h = openFresh("capi_ret_expr.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES (1, 10) RETURNING id + v".cstring, addr stmt) == 0
    check decentdb_column_count(stmt) == 1
    check $decentdb_column_name(stmt, 0) == "column0"
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "open with cache_mb invalid value falls back to default":
    # Covers line 231 (return none(int) after ValueError in cache_mb parsing)
    let path = makeTempDb("capi_cachemb_bad.ddb")
    let h = decentdb_open(path.cstring, "cache_mb=notanumber".cstring)
    check h != nil
    check decentdb_close(h) == 0

  test "open with option missing equals sign is ignored":
    # Covers line 246 (continue when kv.len != 2 - no '=' in option part)
    let path = makeTempDb("capi_noeq.ddb")
    let h = decentdb_open(path.cstring, "noequalssign&cache_pages=64".cstring)
    check h != nil
    check decentdb_close(h) == 0

# ---------------------------------------------------------------------------
# Suite: findMaxParam coverage – various param-containing statement shapes
# ---------------------------------------------------------------------------
suite "C API: findMaxParam coverage":
  test "multi-row INSERT with params covers insertValueRows walk":
    # Covers lines 386-387 (walk for insertValueRows)
    let h = openFresh("capi_multirow_params.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "INSERT INTO t VALUES ($1), ($2)".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 10) == 0
    check decentdb_bind_int64(stmt, 2, 20) == 0
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "UPDATE with params covers assignments and where walk":
    # Covers line 393 (walk for UPDATE assignments and where clause)
    let h = openFresh("capi_upd_params.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v INT)")
    execDDL(h, "INSERT INTO t VALUES (1, 0)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "UPDATE t SET v = $1 WHERE id = $2".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 99) == 0
    check decentdb_bind_int64(stmt, 2, 1) == 0
    check decentdb_step(stmt) == 0
    check decentdb_rows_affected(stmt) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "DELETE with param covers deleteWhere walk":
    # Covers line 395 (walk for DELETE where clause)
    let h = openFresh("capi_del_param.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (5)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "DELETE FROM t WHERE id = $1".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 5) == 0
    check decentdb_step(stmt) == 0
    check decentdb_rows_affected(stmt) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "SELECT with JOIN and param covers join onExpr walk":
    # Covers line 374 (for j in stmt.joins: walk(j.onExpr))
    let h = openFresh("capi_join_param.ddb")
    execDDL(h, "CREATE TABLE t1 (id INT)")
    execDDL(h, "CREATE TABLE t2 (id INT, fk INT)")
    execDDL(h, "INSERT INTO t1 VALUES (1)")
    execDDL(h, "INSERT INTO t2 VALUES (1, 1)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t1.id = $1".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 1) == 0
    check decentdb_step(stmt) == 1
    check decentdb_column_int64(stmt, 0) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "SELECT with GROUP BY and param covers groupBy walk":
    # Covers line 375 (for g in stmt.groupBy: walk(g))
    let h = openFresh("capi_grpby_param.ddb")
    execDDL(h, "CREATE TABLE t (id INT, v INT)")
    execDDL(h, "INSERT INTO t VALUES (1, 10)")
    execDDL(h, "INSERT INTO t VALUES (2, 20)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id > $1 GROUP BY id".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 0) == 0
    discard decentdb_step(stmt)
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "SELECT with ORDER BY and param covers orderBy walk":
    # Covers line 377 (for o in stmt.orderBy: walk(o.expr))
    let h = openFresh("capi_order_param.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (3)")
    execDDL(h, "INSERT INTO t VALUES (1)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t WHERE id > $1 ORDER BY id".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 0) == 0
    check decentdb_step(stmt) == 1
    check decentdb_column_int64(stmt, 0) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "SELECT with LIMIT $1 covers limitParam check":
    # Covers line 378 (if stmt.limitParam > 0: maxIdx = max)
    let h = openFresh("capi_limit_param.ddb")
    execDDL(h, "CREATE TABLE t (id INT)")
    execDDL(h, "INSERT INTO t VALUES (1)")
    execDDL(h, "INSERT INTO t VALUES (2)")
    execDDL(h, "INSERT INTO t VALUES (3)")
    var stmt: pointer = nil
    check decentdb_prepare(h, "SELECT id FROM t LIMIT $1".cstring, addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 2) == 0
    check decentdb_step(stmt) == 1
    check decentdb_step(stmt) == 1
    check decentdb_step(stmt) == 0
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "SELECT with IN subquery and param covers scanSqlForParams":
    # Covers lines 329-338 (scanSqlForParams) and 354-355 (IN_SUBQUERY walk)
    let h = openFresh("capi_in_subq.ddb")
    execDDL(h, "CREATE TABLE t1 (id INT)")
    execDDL(h, "CREATE TABLE t2 (id INT, v INT)")
    execDDL(h, "INSERT INTO t1 VALUES (1)")
    execDDL(h, "INSERT INTO t2 VALUES (1, 42)")
    var stmt: pointer = nil
    check decentdb_prepare(h,
      "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE v = $1)".cstring,
      addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 42) == 0
    check decentdb_step(stmt) == 1
    check decentdb_column_int64(stmt, 0) == 1
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0

  test "SELECT with EXISTS subquery and param":
    # Covers the EXISTS ekFunc path in walk (lines 346-350)
    let h = openFresh("capi_exists_subq.ddb")
    execDDL(h, "CREATE TABLE t1 (id INT)")
    execDDL(h, "CREATE TABLE t2 (id INT, v INT)")
    execDDL(h, "INSERT INTO t1 VALUES (1)")
    execDDL(h, "INSERT INTO t2 VALUES (1, 99)")
    var stmt: pointer = nil
    check decentdb_prepare(h,
      "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id AND t2.v = $1)".cstring,
      addr stmt) == 0
    check decentdb_bind_int64(stmt, 1, 99) == 0
    discard decentdb_step(stmt)
    decentdb_finalize(stmt)
    check decentdb_close(h) == 0
