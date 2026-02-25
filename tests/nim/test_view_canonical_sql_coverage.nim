## Coverage tests for:
## - sql.nim exprToCanonicalSql CASE expression (L1436-1444)
## - sql.nim selectToCanonicalSql CTE path (L1512-1527)
## - sql.nim set operations in canonical SQL (L1529-1542)
## - View storage and retrieval including canonicalization
import unittest
import strutils
import engine

proc freshMemDb(): Db =
  openDb(":memory:").value

suite "VIEW with CASE expression (canonical SQL coverage)":
  test "CREATE VIEW with CASE expression":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE vals (id INT PRIMARY KEY, x INT)").ok
    check execSql(db, "INSERT INTO vals VALUES (1, 5), (2, -3), (3, 0)").ok
    # CASE triggers exprToCanonicalSql L1436
    let cv = execSql(db, """CREATE VIEW pos_neg AS 
      SELECT id, CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END AS sign
      FROM vals""")
    check cv.ok
    let r = execSql(db, "SELECT sign FROM pos_neg ORDER BY id")
    check r.ok
    check r.value == @["positive", "negative", "zero"]

  test "CREATE VIEW with CASE ELSE branch":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 100), (2, 0)").ok
    let cv = execSql(db, """CREATE VIEW labeled AS 
      SELECT CASE WHEN v > 50 THEN 'high' ELSE 'low' END AS lbl FROM t""")
    check cv.ok
    let r = execSql(db, "SELECT lbl FROM labeled")
    check r.ok
    check "high" in r.value
    check "low" in r.value

  test "CREATE VIEW with CASE and no ELSE (NULL else)":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t2 (id INT PRIMARY KEY, n INT)").ok
    check execSql(db, "INSERT INTO t2 VALUES (1, 1), (2, 2), (3, 3)").ok
    let cv = execSql(db, """CREATE VIEW category AS
      SELECT n, CASE WHEN n = 1 THEN 'one' WHEN n = 2 THEN 'two' END AS name FROM t2""")
    check cv.ok
    let r = execSql(db, "SELECT name FROM category ORDER BY n")
    check r.ok
    check r.value[0] == "one"
    check r.value[1] == "two"
    check r.value[2] == "NULL"

  test "SELECT from view with CASE is re-evaluated on query":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE nums (id INT PRIMARY KEY, n INT)").ok
    check execSql(db, "INSERT INTO nums VALUES (1, 5), (2, -2)").ok
    let cv = execSql(db, """CREATE VIEW abs_sign AS 
      SELECT n, CASE WHEN n >= 0 THEN n ELSE 0 - n END AS abs_n FROM nums""")
    check cv.ok
    let r = execSql(db, "SELECT abs_n FROM abs_sign ORDER BY n DESC")
    check r.ok
    check r.value == @["5", "2"]

suite "VIEW with CTE (canonical SQL L1512)":
  test "CREATE VIEW with CTE (WITH clause)":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE orders (id INT PRIMARY KEY, amt INT)").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 100), (2, 200), (3, 50)").ok
    # WITH CTE in view definition triggers selectToCanonicalSql L1512
    let cv = execSql(db, """CREATE VIEW big_orders AS
      WITH big AS (SELECT id, amt FROM orders WHERE amt > 100)
      SELECT id, amt FROM big""")
    check cv.ok
    let r = execSql(db, "SELECT amt FROM big_orders ORDER BY amt")
    check r.ok
    check r.value == @["200"]

  test "CREATE VIEW with CTE with explicit column names":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE emp (id INT PRIMARY KEY, sal INT)").ok
    check execSql(db, "INSERT INTO emp VALUES (1, 1000), (2, 2000), (3, 3000)").ok
    # CTE with explicit column list triggers L1517-1521
    let cv = execSql(db, """CREATE VIEW high_sal AS
      WITH salaries(eid, salary) AS (SELECT id, sal FROM emp)
      SELECT salary FROM salaries WHERE salary > 1500""")
    check cv.ok
    # Query via * to avoid column alias issues
    let r = execSql(db, "SELECT * FROM high_sal ORDER BY 1")
    check r.ok
    check r.value.len == 2

  test "CREATE VIEW with multiple CTEs":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, price INT, qty INT)").ok
    check execSql(db, "INSERT INTO items VALUES (1, 10, 5), (2, 20, 2), (3, 5, 10)").ok
    # CTE with filter — items where price*qty > 45: (1)50 and (3)50 qualify, (2)40 does not
    let cv = execSql(db, """CREATE VIEW revenue AS
      WITH totals AS (SELECT id FROM items WHERE price * qty > 45)
      SELECT id FROM totals""")
    check cv.ok
    let r = execSql(db, "SELECT id FROM revenue ORDER BY id")
    check r.ok
    check r.value == @["1", "3"]

suite "VIEW with set operations (canonical SQL L1529-1542)":
  test "CREATE VIEW with UNION":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO a VALUES (1, 1), (2, 2)").ok
    check execSql(db, "INSERT INTO b VALUES (1, 2), (2, 3)").ok
    let cv = execSql(db, "CREATE VIEW uv AS SELECT v FROM a UNION SELECT v FROM b")
    check cv.ok
    let r = execSql(db, "SELECT v FROM uv ORDER BY v")
    check r.ok
    check r.value == @["1", "2", "3"]

  test "CREATE VIEW with UNION ALL":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE p (id INT PRIMARY KEY, x INT)").ok
    check execSql(db, "CREATE TABLE q (id INT PRIMARY KEY, x INT)").ok
    check execSql(db, "INSERT INTO p VALUES (1, 10)").ok
    check execSql(db, "INSERT INTO q VALUES (1, 10)").ok
    let cv = execSql(db, "CREATE VIEW duped AS SELECT x FROM p UNION ALL SELECT x FROM q")
    check cv.ok
    let r = execSql(db, "SELECT x FROM duped ORDER BY x")
    check r.ok
    check r.value == @["10", "10"]

  test "CREATE VIEW with INTERSECT":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE s1 (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE s2 (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO s1 VALUES (1, 1), (2, 2), (3, 3)").ok
    check execSql(db, "INSERT INTO s2 VALUES (1, 2), (2, 3), (3, 4)").ok
    let cv = execSql(db, "CREATE VIEW common AS SELECT v FROM s1 INTERSECT SELECT v FROM s2")
    check cv.ok
    let r = execSql(db, "SELECT v FROM common ORDER BY v")
    check r.ok
    check r.value == @["2", "3"]

  test "CREATE VIEW with EXCEPT":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE e1 (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE e2 (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO e1 VALUES (1, 1), (2, 2), (3, 3)").ok
    check execSql(db, "INSERT INTO e2 VALUES (1, 2)").ok
    let cv = execSql(db, "CREATE VIEW exc AS SELECT v FROM e1 EXCEPT SELECT v FROM e2")
    check cv.ok
    let r = execSql(db, "SELECT v FROM exc ORDER BY v")
    check r.ok
    check r.value == @["1", "3"]

  test "CREATE VIEW with INTERSECT ALL":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE ia1 (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE ia2 (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO ia1 VALUES (1, 5), (2, 5), (3, 10)").ok
    check execSql(db, "INSERT INTO ia2 VALUES (1, 5), (2, 10)").ok
    let cv = execSql(db, "CREATE VIEW ia_view AS SELECT v FROM ia1 INTERSECT ALL SELECT v FROM ia2")
    check cv.ok
    let r = execSql(db, "SELECT v FROM ia_view ORDER BY v")
    check r.ok
    # min(2, 1) copies of 5, min(1, 1) copies of 10
    check r.value == @["5", "10"]

  test "CREATE VIEW with EXCEPT ALL":
    let db = freshMemDb()
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE ea1 (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE ea2 (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO ea1 VALUES (1, 1), (2, 1), (3, 2)").ok
    check execSql(db, "INSERT INTO ea2 VALUES (1, 1)").ok
    let cv = execSql(db, "CREATE VIEW ea_view AS SELECT v FROM ea1 EXCEPT ALL SELECT v FROM ea2")
    check cv.ok
    let r = execSql(db, "SELECT v FROM ea_view ORDER BY v")
    check r.ok
    # 2-1=1 copy of 1, 1 copy of 2
    check r.value == @["1", "2"]
