## Coverage tests for storage.nim ALTER COLUMN TYPE conversions
## Targets: alterColumnTypeInTable (L1628-1667)
import unittest
import engine

proc db(): Db = openDb(":memory:").value

suite "ALTER TABLE ALTER COLUMN TYPE":
  test "INT to TEXT conversion preserves values":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 42), (2, 100), (3, -5)")
    let r = execSql(d, "ALTER TABLE t ALTER COLUMN val TYPE TEXT")
    require r.ok
    let rows = execSql(d, "SELECT id, val FROM t ORDER BY id")
    require rows.ok
    check rows.value.len == 3
    check rows.value[0] == "1|42"

  test "FLOAT to INT truncates decimal":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, price FLOAT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 3.14), (2, 9.99), (3, 0.0)")
    let r = execSql(d, "ALTER TABLE t ALTER COLUMN price TYPE INT")
    require r.ok
    let rows = execSql(d, "SELECT price FROM t ORDER BY id")
    require rows.ok
    check rows.value[0] == "3"
    check rows.value[1] == "9"
    check rows.value[2] == "0"

  test "TEXT numeric to INT":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, num TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, '100'), (2, '200')")
    let r = execSql(d, "ALTER TABLE t ALTER COLUMN num TYPE INT")
    require r.ok
    let rows = execSql(d, "SELECT num FROM t ORDER BY id")
    require rows.ok
    check rows.value[0] == "100"
    check rows.value[1] == "200"

  test "TEXT numeric to FLOAT":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, num TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, '3.14'), (2, '2.71')")
    let r = execSql(d, "ALTER TABLE t ALTER COLUMN num TYPE FLOAT")
    require r.ok
    let rows = execSql(d, "SELECT num FROM t ORDER BY id")
    require rows.ok
    check rows.value[0] == "3.14"

  test "INT to FLOAT conversion":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 7), (2, 42)")
    let r = execSql(d, "ALTER TABLE t ALTER COLUMN val TYPE FLOAT")
    require r.ok
    let rows = execSql(d, "SELECT val FROM t ORDER BY id")
    require rows.ok
    check rows.value[0] == "7.0"

  test "FLOAT to TEXT conversion":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 1.5), (2, 2.5)")
    let r = execSql(d, "ALTER TABLE t ALTER COLUMN val TYPE TEXT")
    require r.ok
    let rows = execSql(d, "SELECT val FROM t ORDER BY id")
    require rows.ok
    check rows.value[0] == "1.5"

  test "Multiple column type changes in sequence":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b FLOAT, c TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 5, 2.5, '99')")
    # Change a: INT->TEXT
    var r = execSql(d, "ALTER TABLE t ALTER COLUMN a TYPE TEXT")
    require r.ok
    # Change b: FLOAT->INT
    r = execSql(d, "ALTER TABLE t ALTER COLUMN b TYPE INT")
    require r.ok
    # Change c: TEXT->INT
    r = execSql(d, "ALTER TABLE t ALTER COLUMN c TYPE INT")
    require r.ok
    let rows = execSql(d, "SELECT a, b, c FROM t")
    require rows.ok
    check rows.value[0] == "5|2|99"

  test "ALTER COLUMN on empty table succeeds":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    let r = execSql(d, "ALTER TABLE t ALTER COLUMN val TYPE TEXT")
    require r.ok

  test "Add column then query":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    let r = execSql(d, "ALTER TABLE t ADD COLUMN extra TEXT")
    require r.ok
    let rows = execSql(d, "SELECT id, extra FROM t")
    require rows.ok
    check rows.value.len == 1
