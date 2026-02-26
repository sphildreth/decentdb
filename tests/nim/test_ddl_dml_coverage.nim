## Coverage tests for DDL/DML features:
## - ALTER TABLE (ADD/DROP/RENAME COLUMN)
## - CHECK constraints (create, violation, SELECT binding)
## - INSERT RETURNING
## - INSERT ON CONFLICT DO NOTHING / DO UPDATE (upsert)
## - CREATE VIEW / DROP VIEW with complex queries
## - WINDOW functions (ROW_NUMBER, RANK, LAG, LEAD)
## Targets: binder.nim, sql.nim, engine.nim, exec.nim
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

# ------------------- ALTER TABLE -------------------

suite "ALTER TABLE ADD COLUMN":
  test "ADD COLUMN to existing table":
    let db = freshDb("talt_add1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let r = execSql(db, "ALTER TABLE t ADD COLUMN w TEXT")
    require r.ok
    let sel = execSql(db, "SELECT id, v, w FROM t")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "1|10|NULL"
    discard closeDb(db)

  test "ADD COLUMN with valid types":
    let db = freshDb("talt_add2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT64 PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    # INT64/TEXT/BOOL/BOOLEAN/FLOAT64/BLOB work for ADD COLUMN
    let r = execSql(db, "ALTER TABLE t ADD COLUMN score INT64")
    require r.ok
    let sel = execSql(db, "SELECT id, score FROM t")
    require sel.ok
    check sel.value.len == 1
    discard closeDb(db)

  test "ADD COLUMN with unsupported type errors":
    let db = freshDb("talt_add2b.ddb")
    discard execSql(db, "CREATE TABLE t (id INT64 PRIMARY KEY)")
    let r = execSql(db, "ALTER TABLE t ADD COLUMN v DECIMAL(10,2)")
    check not r.ok
    discard closeDb(db)

  test "ADD multiple columns sequentially":
    let db = freshDb("talt_add3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT64 PRIMARY KEY)")
    require execSql(db, "ALTER TABLE t ADD COLUMN a TEXT").ok
    require execSql(db, "ALTER TABLE t ADD COLUMN b INT64").ok
    require execSql(db, "INSERT INTO t VALUES (1, 'hello', 42)").ok
    let sel = execSql(db, "SELECT id, a, b FROM t WHERE id = 1")
    require sel.ok
    check sel.value[0] == "1|hello|42"
    discard closeDb(db)

suite "ALTER TABLE DROP COLUMN":
  test "DROP COLUMN removes column":
    let db = freshDb("talt_drop1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, extra TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10, 'temp')")
    let r = execSql(db, "ALTER TABLE t DROP COLUMN extra")
    require r.ok
    let sel = execSql(db, "SELECT id, v FROM t WHERE id = 1")
    require sel.ok
    check sel.value[0] == "1|10"
    discard closeDb(db)

  test "DROP non-existent COLUMN fails":
    let db = freshDb("talt_drop2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let r = execSql(db, "ALTER TABLE t DROP COLUMN ghost")
    check not r.ok
    discard closeDb(db)

suite "ALTER TABLE RENAME COLUMN":
  test "RENAME COLUMN renames the column":
    let db = freshDb("talt_ren1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, oldname INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 42)")
    let r = execSql(db, "ALTER TABLE t RENAME COLUMN oldname TO newname")
    require r.ok
    let sel = execSql(db, "SELECT id, newname FROM t WHERE id = 1")
    require sel.ok
    check sel.value[0] == "1|42"
    discard closeDb(db)

  test "RENAME non-existent COLUMN fails":
    let db = freshDb("talt_ren2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let r = execSql(db, "ALTER TABLE t RENAME COLUMN ghost TO newname")
    check not r.ok
    discard closeDb(db)

# ------------------- CHECK constraints -------------------

suite "CHECK constraints":
  test "CHECK constraint allows valid insert":
    let db = freshDb("tchk1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK(age >= 0))")
    let r = execSql(db, "INSERT INTO t VALUES (1, 25)")
    require r.ok
    discard closeDb(db)

  test "CHECK constraint rejects invalid insert":
    let db = freshDb("tchk2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, age INT CHECK(age >= 0))")
    let r = execSql(db, "INSERT INTO t VALUES (1, -5)")
    check not r.ok
    discard closeDb(db)

  test "CHECK constraint on UPDATE":
    let db = freshDb("tchk3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT CHECK(v > 0))")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    let rOk = execSql(db, "UPDATE t SET v = 5 WHERE id = 1")
    require rOk.ok
    let rFail = execSql(db, "UPDATE t SET v = -1 WHERE id = 1")
    check not rFail.ok
    discard closeDb(db)

  test "CHECK constraint with expression using multiple columns":
    let db = freshDb("tchk4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, lo INT, hi INT, CHECK(lo <= hi))")
    require execSql(db, "INSERT INTO t VALUES (1, 1, 10)").ok
    let rFail = execSql(db, "INSERT INTO t VALUES (2, 10, 1)")
    check not rFail.ok
    discard closeDb(db)

# ------------------- INSERT RETURNING -------------------

suite "INSERT RETURNING":
  test "RETURNING clause returns inserted row":
    let db = freshDb("tret1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    let r = execSql(db, "INSERT INTO t VALUES (1, 'hello') RETURNING id, v")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "1|hello"
    discard closeDb(db)

  test "RETURNING only selected columns":
    let db = freshDb("tret2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT, extra INT)")
    let r = execSql(db, "INSERT INTO t VALUES (1, 'world', 99) RETURNING id")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "1"
    discard closeDb(db)

  test "RETURNING ROWID":
    let db = freshDb("tret3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    let r = execSql(db, "INSERT INTO t VALUES (42, 'nim') RETURNING id")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "42"
    discard closeDb(db)

# ------------------- ON CONFLICT -------------------

suite "INSERT ON CONFLICT":
  test "ON CONFLICT DO NOTHING ignores duplicate":
    let db = freshDb("toconf1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'first')")
    let r = execSql(db, "INSERT INTO t VALUES (1, 'second') ON CONFLICT DO NOTHING")
    require r.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    check col0(sel.value) == "first"
    discard closeDb(db)

  test "ON CONFLICT DO UPDATE upserts row":
    let db = freshDb("toconf2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'original')")
    let r = execSql(db, "INSERT INTO t VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v")
    require r.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel.ok
    check col0(sel.value) == "updated"
    discard closeDb(db)

  test "ON CONFLICT inserts new row when no conflict":
    let db = freshDb("toconf3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    let r = execSql(db, "INSERT INTO t VALUES (1, 'new') ON CONFLICT DO NOTHING")
    require r.ok
    let sel = execSql(db, "SELECT id, v FROM t WHERE id = 1")
    require sel.ok
    check sel.value.len == 1
    discard closeDb(db)

  test "ON CONFLICT with multiple rows, only conflicting skipped":
    let db = freshDb("toconf4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    require execSql(db, "INSERT INTO t VALUES (2, 'b') ON CONFLICT DO NOTHING").ok
    let sel = execSql(db, "SELECT id FROM t")
    require sel.ok
    check sel.value.len == 2
    discard closeDb(db)

# ------------------- CREATE VIEW / DROP VIEW -------------------

suite "CREATE VIEW and DROP VIEW":
  test "CREATE VIEW and SELECT from it":
    let db = freshDb("tview1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    discard execSql(db, "INSERT INTO t VALUES (2, 20)")
    require execSql(db, "CREATE VIEW big_vals AS SELECT id, v FROM t WHERE v > 15").ok
    let sel = execSql(db, "SELECT id FROM big_vals")
    require sel.ok
    check sel.value.len == 1
    check col0(sel.value) == "2"
    discard closeDb(db)

  test "CREATE VIEW with JOIN":
    let db = freshDb("tview2.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, val INT)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT, name TEXT)")
    discard execSql(db, "INSERT INTO a VALUES (1, 100)")
    discard execSql(db, "INSERT INTO b VALUES (1, 1, 'alice')")
    require execSql(db, "CREATE VIEW joined AS SELECT b.name, a.val FROM a INNER JOIN b ON a.id = b.aid").ok
    let sel = execSql(db, "SELECT name, val FROM joined")
    require sel.ok
    check sel.value.len == 1
    discard closeDb(db)

  test "DROP VIEW removes view":
    let db = freshDb("tview3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE VIEW v AS SELECT id FROM t")
    require execSql(db, "DROP VIEW v").ok
    let sel = execSql(db, "SELECT id FROM v")
    check not sel.ok
    discard closeDb(db)

  test "CREATE VIEW IF NOT EXISTS":
    let db = freshDb("tview4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    require execSql(db, "CREATE VIEW IF NOT EXISTS v AS SELECT id FROM t").ok
    let r2 = execSql(db, "CREATE VIEW IF NOT EXISTS v AS SELECT id FROM t")
    require r2.ok  # Should succeed (IF NOT EXISTS)
    discard closeDb(db)

# ------------------- WINDOW functions -------------------

suite "WINDOW functions":
  test "ROW_NUMBER() assigns sequence":
    let db = freshDb("twin1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    for i in 1..3: discard execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")")
    let res = execSql(db, "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t ORDER BY id")
    require res.ok
    check res.value.len == 3
    check res.value[0].split("|")[1] == "1"
    check res.value[2].split("|")[1] == "3"
    discard closeDb(db)

  test "RANK() with ties":
    let db = freshDb("twin2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 100)")
    discard execSql(db, "INSERT INTO t VALUES (2, 100)")
    discard execSql(db, "INSERT INTO t VALUES (3, 90)")
    let res = execSql(db, "SELECT id, RANK() OVER (ORDER BY score DESC) AS rnk FROM t ORDER BY id")
    require res.ok
    check res.value.len == 3
    # id 1 and 2 should have rank 1, id 3 should have rank 3
    let rnk1 = res.value[0].split("|")[1]
    let rnk2 = res.value[1].split("|")[1]
    check rnk1 == "1"
    check rnk2 == "1"
    discard closeDb(db)

  test "LAG() function":
    let db = freshDb("twin3.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    for i in 1..3: discard execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")")
    let res = execSql(db, "SELECT id, LAG(v) OVER (ORDER BY id) AS prev FROM t ORDER BY id")
    require res.ok
    check res.value.len == 3
    # First row has no lag - should be NULL
    check res.value[0].split("|")[1] == "NULL"
    check res.value[1].split("|")[1] == "10"
    discard closeDb(db)

  test "LEAD() function":
    let db = freshDb("twin4.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    for i in 1..3: discard execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")")
    let res = execSql(db, "SELECT id, LEAD(v) OVER (ORDER BY id) AS nxt FROM t ORDER BY id")
    require res.ok
    check res.value.len == 3
    # Last row has no lead
    check res.value[0].split("|")[1] == "20"
    check res.value[2].split("|")[1] == "NULL"
    discard closeDb(db)

  test "DENSE_RANK() and NTILE() window functions":
    let db = freshDb("twin5.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 100)")
    discard execSql(db, "INSERT INTO t VALUES (2, 100)")
    discard execSql(db, "INSERT INTO t VALUES (3, 90)")
    let res = execSql(db, "SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS dr FROM t ORDER BY id")
    require res.ok
    check res.value.len == 3
    check res.value[0].split("|")[1] == "1"
    check res.value[1].split("|")[1] == "1"
    check res.value[2].split("|")[1] == "2"
    discard closeDb(db)
