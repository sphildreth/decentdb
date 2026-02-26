## Coverage tests for exec.nim substituteAggResult ekBinary/ekUnary/ekInList paths
## (exec.nim L4065-4078) and related aggregate expression handling.
import unittest
import strutils
import engine

suite "Aggregate expressions in SELECT":
  test "SUM * literal in SELECT (ekBinary path in substituteAggResult)":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").ok
    # SUM(v) * 2 forces ekBinary recursion in substituteAggResult
    let r = execSql(db, "SELECT SUM(v) * 2 FROM t")
    check r.ok
    check r.value == @["120"]

  test "SUM + COUNT as ekBinary with two aggregates":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE nums (id INT PRIMARY KEY, n INT)").ok
    check execSql(db, "INSERT INTO nums VALUES (1, 5), (2, 10), (3, 15)").ok
    # SUM + COUNT both as aggregates — ekBinary where both children are aggregates
    let r = execSql(db, "SELECT SUM(n) + COUNT(*) FROM nums")
    check r.ok
    check r.value == @["33"]

  test "MAX - MIN as ekBinary with two aggregates":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE rng (id INT PRIMARY KEY, x INT)").ok
    check execSql(db, "INSERT INTO rng VALUES (1, 3), (2, 7), (3, 12)").ok
    let r = execSql(db, "SELECT MAX(x) - MIN(x) FROM rng")
    check r.ok
    check r.value == @["9"]

  test "GROUP BY with SUM * literal per group":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE sales (id INT PRIMARY KEY, cat TEXT, amt INT)").ok
    check execSql(db, "INSERT INTO sales VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 5)").ok
    let r = execSql(db, "SELECT cat, SUM(amt) * 2 FROM sales GROUP BY cat ORDER BY cat")
    check r.ok
    check r.value.len == 2
    # A: (10+20)*2=60, B: 5*2=10 — values packed in row strings
    check r.value[0].contains("60")
    check r.value[1].contains("10")

  test "AVG * literal in SELECT":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE scores (id INT PRIMARY KEY, s FLOAT)").ok
    check execSql(db, "INSERT INTO scores VALUES (1, 4.0), (2, 6.0)").ok
    let r = execSql(db, "SELECT AVG(s) * 10.0 FROM scores")
    check r.ok
    check r.value == @["50.0"]

  test "COUNT(*) + COUNT(*) as ekBinary":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO items VALUES (1, 1), (2, 2), (3, 3)").ok
    let r = execSql(db, "SELECT COUNT(*) + COUNT(*) FROM items")
    check r.ok
    check r.value == @["6"]

  test "Aggregate inside CASE expression in SELECT":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t2 (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t2 VALUES (1, 5), (2, 15)").ok
    # CASE containing aggregate result — nested aggregate inside CASE (ekFunc "CASE")
    let r = execSql(db, "SELECT CASE WHEN SUM(v) > 10 THEN 'big' ELSE 'small' END FROM t2")
    check r.ok
    check r.value == @["big"]

  test "SUM + SUM per group with HAVING using non-aggregate":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE grp (id INT PRIMARY KEY, cat TEXT, a INT, b INT)").ok
    check execSql(db, "INSERT INTO grp VALUES (1, 'X', 1, 2), (2, 'X', 3, 4), (3, 'Y', 1, 1)").ok
    let r = execSql(db, "SELECT cat, SUM(a) + SUM(b) FROM grp GROUP BY cat HAVING cat = 'X'")
    check r.ok
    check r.value.len == 1
    check r.value[0].contains("10")  # (1+3)+(2+4)=10

  test "Multiple aggregates: COUNT, SUM, AVG combined":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE mv (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO mv VALUES (1, 10), (2, 20), (3, 30)").ok
    # This exercises multiple aggregate substitution in substituteAggResult for a single row
    let r = execSql(db, "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM mv")
    check r.ok
    check r.value[0].contains("3")
    check r.value[0].contains("60")

suite "Aggregate expression edge cases":
  test "SUM of empty group returns NULL":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE empty_t (id INT PRIMARY KEY, v INT)").ok
    let r = execSql(db, "SELECT SUM(v) FROM empty_t")
    check r.ok
    check r.value == @["NULL"]

  test "MIN and MAX of single value group":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE sing (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO sing VALUES (1, 42)").ok
    let r = execSql(db, "SELECT MIN(v), MAX(v) FROM sing")
    check r.ok
    check r.value[0].contains("42")

  test "GROUP_CONCAT aggregates multiple rows":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE words (id INT PRIMARY KEY, w TEXT)").ok
    check execSql(db, "INSERT INTO words VALUES (1, 'hello'), (2, 'world')").ok
    let r = execSql(db, "SELECT GROUP_CONCAT(w) FROM words")
    check r.ok
    check r.value[0].contains("hello")
    check r.value[0].contains("world")

  test "TOTAL of empty table returns 0.0":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE et2 (id INT PRIMARY KEY, v INT)").ok
    let r = execSql(db, "SELECT TOTAL(v) FROM et2")
    check r.ok
    check r.value == @["0.0"]
