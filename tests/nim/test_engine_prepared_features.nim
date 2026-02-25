## Coverage tests for engine.nim paths:
## - INSERT INTO...SELECT via execPrepared (L2375-2455)
## - DISTINCT ON in SELECT (L2662-2676, L2702-2717)
## - DROP VIEW with dependent views error (L3063-3073)
## - ALTER VIEW RENAME TO (L3074-3086)
import unittest
import strutils
import engine

proc db(): Db = openDb(":memory:").value

suite "INSERT INTO...SELECT via execPrepared":
  test "prepare INSERT INTO...SELECT and execPrepared copies rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE src (id INT PRIMARY KEY, v TEXT)")
    discard execSql(d, "CREATE TABLE dst (id INT PRIMARY KEY, v TEXT)")
    discard execSql(d, "INSERT INTO src VALUES (1, 'alpha')")
    discard execSql(d, "INSERT INTO src VALUES (2, 'beta')")
    let p = prepare(d, "INSERT INTO dst SELECT id, v FROM src")
    require p.ok
    discard execPrepared(prepare(d, "BEGIN").value, @[])
    let r = execPrepared(p.value, @[])
    require r.ok
    discard execPrepared(prepare(d, "COMMIT").value, @[])
    let q = execSql(d, "SELECT id, v FROM dst ORDER BY id")
    require q.ok
    check q.value.len == 2
    check "1|alpha" in q.value
    check "2|beta" in q.value

  test "INSERT INTO...SELECT via execPrepared with column list":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE src (id INT PRIMARY KEY, name TEXT, score INT)")
    discard execSql(d, "CREATE TABLE dst (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO src VALUES (1, 'Alice', 90)")
    discard execSql(d, "INSERT INTO src VALUES (2, 'Bob', 85)")
    let p = prepare(d, "INSERT INTO dst (id, name) SELECT id, name FROM src ORDER BY id")
    require p.ok
    discard execPrepared(prepare(d, "BEGIN").value, @[])
    let r = execPrepared(p.value, @[])
    require r.ok
    discard execPrepared(prepare(d, "COMMIT").value, @[])
    let q = execSql(d, "SELECT id, name FROM dst ORDER BY id")
    require q.ok
    check q.value.len == 2
    check q.value[0] == "1|Alice"
    check q.value[1] == "2|Bob"

  test "INSERT INTO...SELECT respects WHERE clause":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE src (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "CREATE TABLE dst (id INT PRIMARY KEY, val INT)")
    for i in 1..5:
      discard execSql(d, "INSERT INTO src VALUES (" & $i & ", " & $(i*10) & ")")
    let p = prepare(d, "INSERT INTO dst SELECT id, val FROM src WHERE val > 30")
    require p.ok
    discard execPrepared(prepare(d, "BEGIN").value, @[])
    let r = execPrepared(p.value, @[])
    require r.ok
    discard execPrepared(prepare(d, "COMMIT").value, @[])
    let q = execSql(d, "SELECT COUNT(*) FROM dst")
    require q.ok
    check q.value[0] == "2"

  test "INSERT INTO...SELECT with empty source table":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE src (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE dst (id INT PRIMARY KEY)")
    let p = prepare(d, "INSERT INTO dst SELECT id FROM src")
    require p.ok
    discard execPrepared(prepare(d, "BEGIN").value, @[])
    let r = execPrepared(p.value, @[])
    require r.ok
    discard execPrepared(prepare(d, "COMMIT").value, @[])
    let q = execSql(d, "SELECT COUNT(*) FROM dst")
    require q.ok
    check q.value[0] == "0"

  test "INSERT INTO...SELECT via execSql also works":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE src (id INT PRIMARY KEY, v TEXT)")
    discard execSql(d, "CREATE TABLE dst (id INT PRIMARY KEY, v TEXT)")
    discard execSql(d, "INSERT INTO src VALUES (1, 'x'), (2, 'y')")
    let r = execSql(d, "INSERT INTO dst SELECT id, v FROM src")
    require r.ok
    let q = execSql(d, "SELECT COUNT(*) FROM dst")
    require q.ok
    check q.value[0] == "2"

suite "SELECT DISTINCT ON":
  test "DISTINCT ON keeps first row per key":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'A', 10)")
    discard execSql(d, "INSERT INTO t VALUES (2, 'A', 20)")
    discard execSql(d, "INSERT INTO t VALUES (3, 'B', 30)")
    let r = execSql(d, "SELECT DISTINCT ON (cat) id, cat, v FROM t ORDER BY cat")
    require r.ok
    check r.value.len == 2

  test "DISTINCT ON with all unique keys returns all rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'Alice')")
    discard execSql(d, "INSERT INTO t VALUES (2, 'Bob')")
    discard execSql(d, "INSERT INTO t VALUES (3, 'Carol')")
    let r = execSql(d, "SELECT DISTINCT ON (name) id, name FROM t ORDER BY name")
    require r.ok
    check r.value.len == 3

  test "DISTINCT ON with NULL values":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, NULL)")
    discard execSql(d, "INSERT INTO t VALUES (2, NULL)")
    discard execSql(d, "INSERT INTO t VALUES (3, 'X')")
    let r = execSql(d, "SELECT DISTINCT ON (cat) id, cat FROM t ORDER BY cat")
    require r.ok
    # NULLs form one group, 'X' another
    check r.value.len == 2

  test "DISTINCT ON on integer column":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, val TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 1, 'a')")
    discard execSql(d, "INSERT INTO t VALUES (2, 1, 'b')")
    discard execSql(d, "INSERT INTO t VALUES (3, 2, 'c')")
    let r = execSql(d, "SELECT DISTINCT ON (grp) grp, val FROM t ORDER BY grp")
    require r.ok
    check r.value.len == 2

suite "DROP VIEW with dependent views":
  test "DROP VIEW fails when another view depends on it":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 10)")
    discard execSql(d, "CREATE VIEW v1 AS SELECT id, v FROM t")
    discard execSql(d, "CREATE VIEW v2 AS SELECT id FROM v1")
    let r = execSql(d, "DROP VIEW v1")
    check not r.ok
    check "dependent" in r.err.message.toLowerAscii or "Cannot drop" in r.err.message

  test "DROP VIEW succeeds when no dependents":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(d, "CREATE VIEW v1 AS SELECT id, v FROM t")
    let r = execSql(d, "DROP VIEW v1")
    require r.ok

  test "DROP VIEW IF EXISTS on non-existent view succeeds":
    let d = db()
    defer: discard closeDb(d)
    let r = execSql(d, "DROP VIEW IF EXISTS nonexistent_view_xyz")
    require r.ok

  test "DROP VIEW on non-existent view without IF EXISTS fails":
    let d = db()
    defer: discard closeDb(d)
    let r = execSql(d, "DROP VIEW nonexistent_view_xyz")
    check not r.ok

suite "ALTER VIEW RENAME TO":
  test "ALTER VIEW RENAME TO renames the view":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 42)")
    discard execSql(d, "CREATE VIEW old_name AS SELECT id, v FROM t")
    let r = execSql(d, "ALTER VIEW old_name RENAME TO new_name")
    require r.ok
    # Old name no longer works
    let r2 = execSql(d, "SELECT * FROM old_name")
    check not r2.ok
    # New name works
    let r3 = execSql(d, "SELECT id, v FROM new_name")
    require r3.ok
    check r3.value[0] == "1|42"

  test "ALTER VIEW RENAME TO fails when name exists":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE VIEW v1 AS SELECT id FROM t")
    discard execSql(d, "CREATE VIEW v2 AS SELECT id FROM t")
    let r = execSql(d, "ALTER VIEW v1 RENAME TO v2")
    check not r.ok

  test "ALTER VIEW RENAME TO fails when dependent views exist":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE VIEW base_v AS SELECT id FROM t")
    discard execSql(d, "CREATE VIEW dep_v AS SELECT id FROM base_v")
    let r = execSql(d, "ALTER VIEW base_v RENAME TO base_v2")
    check not r.ok
    check "dependent" in r.err.message.toLowerAscii or "Cannot rename" in r.err.message
