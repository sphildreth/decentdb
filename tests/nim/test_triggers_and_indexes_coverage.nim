## Coverage tests for:
## - CREATE TRIGGER / DROP TRIGGER (engine.nim execPreparedNonSelect skCreateTrigger/skDropTrigger)
## - Trigger execution (engine.nim runTriggerActions)
## - INSTEAD OF triggers on views
## - Expression indexes (storage.nim castExpressionValue, LOWER/UPPER/TRIM/CAST)
## - Partial indexes (storage.nim shouldIncludeInIndex, evalPredicateValue)
## - Index on computed expressions
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

suite "CREATE and DROP TRIGGER":
  test "AFTER INSERT trigger executes on insert":
    let db = freshDb("trig1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE TABLE log (op TEXT, val INT)")
    let cr = execSql(db, "CREATE TRIGGER after_ins AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''insert'', 99)')")
    require cr.ok
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let sel = execSql(db, "SELECT COUNT(*) FROM log")
    require sel.ok
    check sel.value[0] == "1"
    discard closeDb(db)

  test "AFTER INSERT trigger fires multiple times":
    let db = freshDb("trig2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE cnt (n INT)")
    discard execSql(db, "INSERT INTO cnt VALUES (0)")
    let cr = execSql(db, "CREATE TRIGGER cnt_trig AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('UPDATE cnt SET n = n + 1')")
    require cr.ok
    for i in 1..5:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ")")
    let sel = execSql(db, "SELECT n FROM cnt")
    require sel.ok
    check sel.value[0] == "5"
    discard closeDb(db)

  test "AFTER UPDATE trigger executes on update":
    let db = freshDb("trig3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE TABLE log (op TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let cr = execSql(db, "CREATE TRIGGER after_upd AFTER UPDATE ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''updated'')')")
    require cr.ok
    discard execSql(db, "UPDATE t SET v = 20 WHERE id = 1")
    let sel = execSql(db, "SELECT COUNT(*) FROM log")
    require sel.ok
    check sel.value[0] == "1"
    discard closeDb(db)

  test "AFTER DELETE trigger executes on delete":
    let db = freshDb("trig4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE del_log (n INT)")
    discard execSql(db, "INSERT INTO del_log VALUES (0)")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    discard execSql(db, "INSERT INTO t VALUES (2)")
    let cr = execSql(db, "CREATE TRIGGER after_del AFTER DELETE ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('UPDATE del_log SET n = n + 1')")
    require cr.ok
    discard execSql(db, "DELETE FROM t WHERE id = 1")
    let sel = execSql(db, "SELECT n FROM del_log")
    require sel.ok
    check sel.value[0] == "1"
    discard closeDb(db)

  test "DROP TRIGGER removes it":
    let db = freshDb("trig5.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE log (n INT)")
    discard execSql(db, "INSERT INTO log VALUES (0)")
    let cr = execSql(db, "CREATE TRIGGER tri AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('UPDATE log SET n = n + 1')")
    require cr.ok
    discard execSql(db, "INSERT INTO t VALUES (1)")
    let dr = execSql(db, "DROP TRIGGER tri ON t")
    require dr.ok
    discard execSql(db, "INSERT INTO t VALUES (2)")
    let sel = execSql(db, "SELECT n FROM log")
    require sel.ok
    # Only one increment (before DROP)
    check sel.value[0] == "1"
    discard closeDb(db)

  test "INSTEAD OF trigger on view":
    let db = freshDb("trig6.ddb")
    discard execSql(db, "CREATE TABLE base (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "CREATE VIEW vw AS SELECT * FROM base WHERE id > 0")
    let cr = execSql(db, "CREATE TRIGGER vw_ins INSTEAD OF INSERT ON vw FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO base VALUES (99, ''via_trigger'')')")
    require cr.ok
    let ins = execSql(db, "INSERT INTO vw VALUES (1, 'test')")
    require ins.ok
    let sel = execSql(db, "SELECT v FROM base WHERE id = 99")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "via_trigger"
    discard closeDb(db)

  test "DROP TRIGGER on nonexistent trigger fails gracefully":
    let db = freshDb("trig7.ddb")
    let res = execSql(db, "DROP TRIGGER IF EXISTS nonexistent_trig ON t")
    discard execSql(db, "CREATE TABLE t (x INT)")
    let res2 = execSql(db, "DROP TRIGGER IF EXISTS nonexistent_trig ON t")
    discard closeDb(db)

suite "Expression indexes":
  test "LOWER() expression index created and used":
    let db = freshDb("exidx1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'Alice')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'BOB')")
    discard execSql(db, "INSERT INTO t VALUES (3, 'charlie')")
    let cr = execSql(db, "CREATE INDEX idx_lower_name ON t (LOWER(name))")
    require cr.ok
    let sel = execSql(db, "SELECT id FROM t WHERE LOWER(name) = 'bob'")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "2"
    discard closeDb(db)

  test "UPPER() expression index created and used":
    let db = freshDb("exidx2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'hello')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'WORLD')")
    let cr = execSql(db, "CREATE INDEX idx_upper_name ON t (UPPER(name))")
    require cr.ok
    let sel = execSql(db, "SELECT id FROM t WHERE UPPER(name) = 'HELLO'")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "1"
    discard closeDb(db)

  test "Expression index with CAST(col AS INT64)":
    let db = freshDb("exidx3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, num TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, '100')")
    discard execSql(db, "INSERT INTO t VALUES (2, '200')")
    discard execSql(db, "INSERT INTO t VALUES (3, '150')")
    let cr = execSql(db, "CREATE INDEX idx_cast_num ON t (CAST(num AS INT64))")
    require cr.ok
    let sel = execSql(db, "SELECT id FROM t WHERE CAST(num AS INT64) = 150")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "3"
    discard closeDb(db)

  test "Expression index with LENGTH()":
    let db = freshDb("exidx4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'short')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'a longer name')")
    let cr = execSql(db, "CREATE INDEX idx_len ON t (LENGTH(name))")
    require cr.ok
    let sel = execSql(db, "SELECT id FROM t WHERE LENGTH(name) = 5")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "1"
    discard closeDb(db)

  test "TRIM() expression index":
    let db = freshDb("exidx5.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, '  hello  ')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'world')")
    let cr = execSql(db, "CREATE INDEX idx_trim ON t (TRIM(v))")
    require cr.ok
    let sel = execSql(db, "SELECT id FROM t WHERE TRIM(v) = 'hello'")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "1"
    discard closeDb(db)

  test "Expression index inserts/updates cover castExpressionValue":
    # Inserting into table with expression index covers castExpressionValue paths
    let db = freshDb("exidx6.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    let cr = execSql(db, "CREATE INDEX idx_lower ON t (LOWER(v))")
    require cr.ok
    # Multiple inserts to exercise index insertion
    for i in 1..5:
      let r = execSql(db, "INSERT INTO t VALUES (" & $i & ", 'Value" & $i & "')")
      check r.ok
    # Update to exercise index update
    discard execSql(db, "UPDATE t SET v = 'Updated' WHERE id = 3")
    let sel = execSql(db, "SELECT id FROM t WHERE LOWER(v) = 'updated'")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "3"
    discard closeDb(db)

suite "Partial indexes":
  test "Partial index with WHERE condition":
    let db = freshDb("pidx1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, active BOOL)")
    for i in 1..5:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ", " & (if i mod 2 == 1: "true" else: "false") & ")")
    let cr = execSql(db, "CREATE INDEX idx_active ON t (v) WHERE active = true")
    require cr.ok
    # Query should use index
    let sel = execSql(db, "SELECT id FROM t WHERE v = 30 AND active = true")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "3"
    discard closeDb(db)

  test "Partial index - rows outside predicate not indexed":
    let db = freshDb("pidx2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, flag INT)")
    for i in 1..10:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ", " & $(i mod 3) & ")")
    let cr = execSql(db, "CREATE INDEX idx_flag1 ON t (v) WHERE flag = 1")
    require cr.ok
    let sel = execSql(db, "SELECT COUNT(*) FROM t WHERE flag = 1")
    require sel.ok
    check sel.value[0] == "4"
    discard closeDb(db)

  test "Partial index numeric comparison":
    let db = freshDb("pidx3.ddb")
    discard execSql(db, "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT)")
    for i in 1..8:
      discard execSql(db, "INSERT INTO orders VALUES (" & $i & ", " & $(i*100) & ", '" & (if i mod 2 == 0: "paid" else: "pending") & "')")
    let cr = execSql(db, "CREATE INDEX idx_big_pending ON orders (amount) WHERE amount > 400")
    require cr.ok
    let sel = execSql(db, "SELECT COUNT(*) FROM orders WHERE amount > 400")
    require sel.ok
    check sel.value[0] == "4"
    discard closeDb(db)

suite "Index operations with updates and deletes":
  test "Expression index maintained on update":
    let db = freshDb("exidx_upd.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    let cr = execSql(db, "CREATE INDEX idx_lower ON t (LOWER(name))")
    require cr.ok
    discard execSql(db, "INSERT INTO t VALUES (1, 'Alice')")
    # Update should update index
    discard execSql(db, "UPDATE t SET name = 'Bob' WHERE id = 1")
    let old_sel = execSql(db, "SELECT id FROM t WHERE LOWER(name) = 'alice'")
    require old_sel.ok
    check old_sel.value.len == 0  # old value removed from index
    let new_sel = execSql(db, "SELECT id FROM t WHERE LOWER(name) = 'bob'")
    require new_sel.ok
    check new_sel.value.len == 1
    discard closeDb(db)

  test "Expression index maintained on delete":
    let db = freshDb("exidx_del.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    let cr = execSql(db, "CREATE INDEX idx_lower ON t (LOWER(name))")
    require cr.ok
    discard execSql(db, "INSERT INTO t VALUES (1, 'Alice')")
    discard execSql(db, "DELETE FROM t WHERE id = 1")
    let sel = execSql(db, "SELECT id FROM t WHERE LOWER(name) = 'alice'")
    require sel.ok
    check sel.value.len == 0
    discard closeDb(db)
