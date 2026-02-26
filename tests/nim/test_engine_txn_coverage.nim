## Additional engine coverage tests:
## - Savepoints and nested transactions
## - Prepared statements with various types
## - Complex multi-table query patterns
## - Error paths for constraint violations
import unittest
import strutils
import engine
import record/record

proc db(): Db = openDb(":memory:").value

suite "Transaction and savepoint coverage":
  test "SAVEPOINT and ROLLBACK TO restores state":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "BEGIN")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    discard execSql(d, "SAVEPOINT sp1")
    discard execSql(d, "INSERT INTO t VALUES (2)")
    discard execSql(d, "ROLLBACK TO SAVEPOINT sp1")
    discard execSql(d, "COMMIT")
    let r = execSql(d, "SELECT COUNT(*) FROM t")
    require r.ok
    check r.value[0] == "1"

  test "RELEASE SAVEPOINT commits savepoint":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "BEGIN")
    discard execSql(d, "SAVEPOINT sp1")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    discard execSql(d, "RELEASE SAVEPOINT sp1")
    discard execSql(d, "COMMIT")
    let r = execSql(d, "SELECT COUNT(*) FROM t")
    require r.ok
    check r.value[0] == "1"

  test "ROLLBACK discards all uncommitted changes":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    discard execSql(d, "BEGIN")
    discard execSql(d, "INSERT INTO t VALUES (2), (3)")
    discard execSql(d, "ROLLBACK")
    let r = execSql(d, "SELECT COUNT(*) FROM t")
    require r.ok
    check r.value[0] == "1"

  test "Multiple savepoints in one transaction":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "BEGIN")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    discard execSql(d, "SAVEPOINT sp1")
    discard execSql(d, "INSERT INTO t VALUES (2)")
    discard execSql(d, "SAVEPOINT sp2")
    discard execSql(d, "INSERT INTO t VALUES (3)")
    discard execSql(d, "ROLLBACK TO SAVEPOINT sp2")
    discard execSql(d, "ROLLBACK TO SAVEPOINT sp1")
    discard execSql(d, "COMMIT")
    let r = execSql(d, "SELECT COUNT(*) FROM t")
    require r.ok
    check r.value[0] == "1"

suite "Prepared statement types":
  test "SELECT with FLOAT param returns rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, price FLOAT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 9.99), (2, 19.99), (3, 4.99)")
    let stmt = prepare(d, "SELECT id FROM t WHERE price < $1")
    require stmt.ok
    let r = execPrepared(stmt.value, @[Value(kind: vkFloat64, float64Val: 10.0)])
    require r.ok
    # execPrepared returns rows joined by \n in one string per statement
    let rows = r.value[0].split("\n")
    check rows.len == 2

  test "SELECT with TEXT param LIKE":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'Alice'),(2,'Bob'),(3,'Alvin')")
    let stmt = prepare(d, "SELECT id FROM t WHERE name LIKE $1")
    require stmt.ok
    let r = execPrepared(stmt.value, @[Value(kind: vkText, bytes: cast[seq[byte]]("Al%"))])
    require r.ok
    let rows = r.value[0].split("\n")
    check rows.len == 2

  test "INSERT with all types via prepared":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT, active INT)")
    discard execSql(d, "BEGIN")
    let stmt = prepare(d, "INSERT INTO t VALUES ($1, $2, $3, $4)")
    require stmt.ok
    let r = execPrepared(stmt.value, @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: cast[seq[byte]]("test")),
      Value(kind: vkFloat64, float64Val: 3.14),
      Value(kind: vkInt64, int64Val: 1)
    ])
    require r.ok
    discard execSql(d, "COMMIT")
    let rows = execSql(d, "SELECT * FROM t WHERE id = 1")
    require rows.ok
    check rows.value.len == 1

suite "Complex query patterns":
  test "Subquery in FROM clause (derived table)":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    for i in 1..10:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")")
    let r = execSql(d, "SELECT id FROM (SELECT * FROM t WHERE val > 50) AS sub ORDER BY id")
    require r.ok
    check r.value.len == 5

  test "SELECT DISTINCT ON with ORDER BY":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'b'),(2,'a'),(3,'b'),(4,'c'),(5,'a')")
    let r = execSql(d, "SELECT DISTINCT ON (cat) cat FROM t ORDER BY cat")
    require r.ok
    check r.value.len == 3

  test "ORDER BY multiple columns":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'x',3),(2,'x',1),(3,'y',2),(4,'y',4)")
    let r = execSql(d, "SELECT id FROM t ORDER BY a ASC, b DESC")
    require r.ok
    check r.value.len == 4
    check r.value[0] == "1"
    check r.value[1] == "2"

  test "Multiple aggregate functions":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,5),(2,10),(3,15),(4,20)")
    let r = execSql(d, "SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM t")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "4|50|5|20"

  test "INNER JOIN with WHERE filter":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE a (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "CREATE TABLE b (id INT PRIMARY KEY, aid INT, extra INT)")
    for i in 1..10:
      discard execSql(d, "INSERT INTO a VALUES (" & $i & ", " & $i & ")")
    for i in 1..20:
      discard execSql(d, "INSERT INTO b VALUES (" & $i & ", " & $((i mod 10)+1) & ", " & $i & ")")
    let r = execSql(d, "SELECT COUNT(*) FROM a INNER JOIN b ON b.aid = a.id WHERE a.val > 5")
    require r.ok
    let cnt = parseInt(r.value[0])
    check cnt > 0

  test "LEFT JOIN with NULL on right side":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE l (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE r (id INT PRIMARY KEY, lid INT, val TEXT)")
    discard execSql(d, "INSERT INTO l VALUES (1),(2),(3)")
    discard execSql(d, "INSERT INTO r VALUES (1, 1, 'match')")
    let rows = execSql(d, "SELECT l.id, r.val FROM l LEFT JOIN r ON r.lid = l.id ORDER BY l.id")
    require rows.ok
    check rows.value.len == 3
    check rows.value[0] == "1|match"

suite "UNIQUE constraint edge cases":
  test "Multiple NULLs in UNIQUE column allowed":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)")
    discard execSql(d, "INSERT INTO t VALUES (1, NULL)")
    discard execSql(d, "INSERT INTO t VALUES (2, NULL)")
    discard execSql(d, "INSERT INTO t VALUES (3, NULL)")
    let r = execSql(d, "SELECT COUNT(*) FROM t WHERE code IS NULL")
    require r.ok
    check r.value[0] == "3"

  test "UNIQUE violation on non-NULL value":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'abc')")
    let r = execSql(d, "INSERT INTO t VALUES (2, 'abc')")
    check not r.ok

suite "Error paths":
  test "DROP TABLE IF EXISTS for missing table succeeds":
    let d = db()
    defer: discard closeDb(d)
    let r = execSql(d, "DROP TABLE IF EXISTS t_missing")
    require r.ok

  test "CREATE TABLE IF NOT EXISTS when table missing succeeds":
    let d = db()
    defer: discard closeDb(d)
    let r = execSql(d, "CREATE TABLE IF NOT EXISTS new_table (id INT PRIMARY KEY)")
    require r.ok

  test "CREATE TABLE when table exists fails":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    let r = execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    check not r.ok

  test "FK child insert without parent fails":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))")
    discard execSql(d, "INSERT INTO parent VALUES (1)")
    let r = execSql(d, "INSERT INTO child VALUES (1, 999)")
    check not r.ok

  test "FK parent delete with child rows fails":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))")
    discard execSql(d, "INSERT INTO parent VALUES (1)")
    discard execSql(d, "INSERT INTO child VALUES (1, 1)")
    let r = execSql(d, "DELETE FROM parent WHERE id = 1")
    check not r.ok

  test "INSERT INTO ... SELECT from same table works":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE src (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "CREATE TABLE dst (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO src VALUES (1,10),(2,20),(3,30)")
    let r = execSql(d, "INSERT INTO dst SELECT * FROM src WHERE val > 10")
    require r.ok
    let cnt = execSql(d, "SELECT COUNT(*) FROM dst")
    require cnt.ok
    check cnt.value[0] == "2"

  test "ON CONFLICT DO NOTHING avoids duplicate error":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 10)")
    let r = execSql(d, "INSERT INTO t VALUES (1, 99) ON CONFLICT DO NOTHING")
    require r.ok
    # Value should still be the original
    let rows = execSql(d, "SELECT val FROM t WHERE id = 1")
    require rows.ok
    check rows.value[0] == "10"
