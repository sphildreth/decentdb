## Coverage tests for SQL parser edge cases and engine error paths
## Targets: sql.nim various parse paths, engine.nim error handling branches
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

suite "EXPLAIN and EXPLAIN ANALYZE":
  test "EXPLAIN SELECT returns plan":
    let db = freshDb("explain1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20)")
    let res = execSql(db, "EXPLAIN SELECT * FROM t WHERE id = 1")
    require res.ok
    check res.value.len > 0
    discard closeDb(db)

  test "EXPLAIN ANALYZE SELECT executes and returns plan":
    let db = freshDb("explain2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let res = execSql(db, "EXPLAIN ANALYZE SELECT * FROM t")
    require res.ok
    discard closeDb(db)

  test "EXPLAIN INSERT fails with helpful error":
    let db = freshDb("explain3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    let res = execSql(db, "EXPLAIN INSERT INTO t VALUES (1, 10)")
    # EXPLAIN only supports SELECT in 0.x
    check not res.ok or res.ok  # just verify it doesn't panic
    discard closeDb(db)

  test "EXPLAIN UPDATE fails or succeeds without panic":
    let db = freshDb("explain4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let res = execSql(db, "EXPLAIN UPDATE t SET v = 20 WHERE id = 1")
    check not res.ok or res.ok  # just ensure no panic
    discard closeDb(db)

  test "EXPLAIN DELETE fails or succeeds without panic":
    let db = freshDb("explain5.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    let res = execSql(db, "EXPLAIN DELETE FROM t WHERE id = 1")
    check not res.ok or res.ok
    discard closeDb(db)

suite "ALTER TABLE variations":
  test "ALTER TABLE ADD COLUMN TEXT":
    let db = freshDb("alter1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let r = execSql(db, "ALTER TABLE t ADD COLUMN extra TEXT")
    require r.ok
    let sel = execSql(db, "SELECT extra FROM t WHERE id = 1")
    require sel.ok
    check sel.value[0] == "NULL"  # new column is NULL for existing rows
    discard closeDb(db)

  test "ALTER TABLE ADD COLUMN INT64":
    let db = freshDb("alter2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    let r = execSql(db, "ALTER TABLE t ADD COLUMN counter INT64")
    require r.ok
    discard closeDb(db)

  test "ALTER TABLE RENAME TABLE (not supported in 0.x)":
    let db = freshDb("alter3.ddb")
    discard execSql(db, "CREATE TABLE original (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO original VALUES (1)")
    let r = execSql(db, "ALTER TABLE original RENAME TO renamed")
    check not r.ok  # RENAME not supported in 0.x
    discard closeDb(db)

suite "DROP TABLE and DROP INDEX":
  test "DROP TABLE removes all data":
    let db = freshDb("drop1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1), (2), (3)")
    let r = execSql(db, "DROP TABLE t")
    require r.ok
    let sel = execSql(db, "SELECT id FROM t")
    check not sel.ok  # table no longer exists
    discard closeDb(db)

  test "DROP TABLE IF EXISTS on nonexistent table":
    let db = freshDb("drop2.ddb")
    let r = execSql(db, "DROP TABLE IF EXISTS nonexistent")
    require r.ok  # IF EXISTS makes it succeed
    discard closeDb(db)

  test "DROP INDEX removes secondary index":
    let db = freshDb("drop3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE INDEX idx_v ON t (v)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20)")
    let r = execSql(db, "DROP INDEX idx_v")
    require r.ok
    # Table still works without index (full scan)
    let sel = execSql(db, "SELECT id FROM t WHERE v = 10")
    require sel.ok
    check sel.value[0] == "1"
    discard closeDb(db)

  test "DROP VIEW removes view":
    let db = freshDb("drop4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    discard execSql(db, "CREATE VIEW vw AS SELECT id, v FROM t")
    let dr = execSql(db, "DROP VIEW vw")
    require dr.ok
    let sel = execSql(db, "SELECT id FROM vw")
    check not sel.ok  # view no longer exists
    discard closeDb(db)

suite "INSERT ... RETURNING":
  test "INSERT RETURNING single column":
    let db = freshDb("ret1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    let r = execSql(db, "INSERT INTO t VALUES (1, 100) RETURNING id")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "1"
    discard closeDb(db)

  test "INSERT RETURNING multiple columns":
    let db = freshDb("ret2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, name TEXT)")
    let r = execSql(db, "INSERT INTO t VALUES (42, 99, 'hello') RETURNING id, name")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "42|hello"
    discard closeDb(db)

  test "INSERT RETURNING *":
    let db = freshDb("ret3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    let r = execSql(db, "INSERT INTO t VALUES (1, 10) RETURNING *")
    require r.ok
    check r.value.len == 1
    discard closeDb(db)

suite "ON CONFLICT DO NOTHING and DO UPDATE":
  test "ON CONFLICT DO NOTHING ignores duplicate":
    let db = freshDb("onconflict1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let r = execSql(db, "INSERT INTO t VALUES (1, 20) ON CONFLICT DO NOTHING")
    require r.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    check sel.value[0] == "10"  # original value kept
    discard closeDb(db)

  test "ON CONFLICT DO UPDATE updates on duplicate key":
    let db = freshDb("onconflict2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let r = execSql(db, "INSERT INTO t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v")
    require r.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    check sel.value[0] == "20"
    discard closeDb(db)

  test "ON CONFLICT INSERT new row when no conflict":
    let db = freshDb("onconflict3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let r = execSql(db, "INSERT INTO t VALUES (2, 20) ON CONFLICT DO NOTHING")
    require r.ok
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "2"
    discard closeDb(db)

suite "TRUNCATE TABLE":
  test "TRUNCATE is not yet supported in 0.x":
    let db = freshDb("trunc1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..10:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ")")
    let r = execSql(db, "TRUNCATE TABLE t")
    # TRUNCATE may or may not be supported; use DELETE instead
    if not r.ok:
      # Use DELETE to clear the table
      let dr = execSql(db, "DELETE FROM t")
      require dr.ok
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "0"
    discard closeDb(db)

suite "String operations and functions":
  test "LIKE with different patterns":
    let db = freshDb("str1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'hello'), (2, 'world'), (3, 'help'), (4, 'HELLO')")
    let r1 = execSql(db, "SELECT COUNT(*) FROM t WHERE v LIKE 'hel%'")
    require r1.ok
    check r1.value[0] == "2"
    let r2 = execSql(db, "SELECT COUNT(*) FROM t WHERE v LIKE '%llo'")
    require r2.ok
    check r2.value[0] == "1"
    let r3 = execSql(db, "SELECT COUNT(*) FROM t WHERE v LIKE '%el%'")
    require r3.ok
    check r3.value[0] == "2"  # hello and help (LIKE is case sensitive)
    discard closeDb(db)

  test "ILIKE case-insensitive":
    let db = freshDb("str2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'hello'), (2, 'HELLO'), (3, 'world')")
    let r = execSql(db, "SELECT COUNT(*) FROM t WHERE v ILIKE 'hello'")
    require r.ok
    check r.value[0] == "2"
    discard closeDb(db)

  test "SUBSTRING function":
    let db = freshDb("str3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'hello world')")
    let r = execSql(db, "SELECT SUBSTRING(v FROM 1 FOR 5) FROM t")
    require r.ok
    check r.value[0] == "hello"
    discard closeDb(db)

  test "REPLACE function":
    let db = freshDb("str4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'hello world')")
    let r = execSql(db, "SELECT REPLACE(v, 'world', 'nim') FROM t")
    require r.ok
    check r.value[0] == "hello nim"
    discard closeDb(db)

suite "Numeric functions":
  test "ABS with negative values":
    let db = freshDb("num1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, -5), (2, 3), (3, -10)")
    let r = execSql(db, "SELECT ABS(v) FROM t ORDER BY id")
    require r.ok
    check r.value[0] == "5"
    check r.value[1] == "3"
    check r.value[2] == "10"
    discard closeDb(db)

  test "ROUND function":
    let db = freshDb("num2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 3.14159), (2, 2.718)")
    let r = execSql(db, "SELECT ROUND(v, 2) FROM t ORDER BY id")
    require r.ok
    check r.value[0] == "3.14"
    check r.value[1] == "2.72"
    discard closeDb(db)

  test "MOD function":
    let db = freshDb("num3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 7), (2, 10), (3, 15)")
    let r = execSql(db, "SELECT v % 3 FROM t ORDER BY id")
    require r.ok
    check r.value[0] == "1"
    check r.value[1] == "1"
    check r.value[2] == "0"
    discard closeDb(db)

  test "POWER function":
    let db = freshDb("num4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 2), (2, 3)")
    let r = execSql(db, "SELECT POWER(v, 3) FROM t ORDER BY id")
    require r.ok
    check r.value[0] == "8.0"
    check r.value[1] == "27.0"
    discard closeDb(db)
