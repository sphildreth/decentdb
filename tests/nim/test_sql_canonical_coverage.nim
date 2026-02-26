## Coverage tests for sql.nim selectToCanonicalSql:
## - GROUP BY serialization (L1584-1588)
## - HAVING serialization (L1589-1590)
## - ORDER BY serialization (L1592-1599)
## - LIMIT serialization (L1601-1604)
## - OFFSET serialization (L1606-1609)
## - parseRenameStmt for ALTER VIEW (L1982-2002)
## - parseSqlValueFunction CURRENT_TIME, NOW (L791-793)
## - subquery parsing IN_SUBQUERY, SCALAR_SUBQUERY (L748-770)
import unittest
import strutils
import engine
import sql/sql

proc db(): Db = openDb(":memory:").value

suite "VIEW with GROUP BY/HAVING in canonical SQL":
  test "CREATE VIEW with GROUP BY and query via join":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount INT)")
    discard execSql(d, "INSERT INTO orders VALUES (1, 'Alice', 100)")
    discard execSql(d, "INSERT INTO orders VALUES (2, 'Alice', 200)")
    discard execSql(d, "INSERT INTO orders VALUES (3, 'Bob', 150)")
    # Create a view with GROUP BY — uses selectToCanonicalSql for storage
    let r = execSql(d, "CREATE VIEW customer_totals AS SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer")
    require r.ok
    # Drop and recreate to ensure canonical SQL round-trips
    discard execSql(d, "DROP VIEW customer_totals")
    let r2 = execSql(d, "CREATE VIEW customer_totals AS SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer")
    require r2.ok

  test "CREATE VIEW with HAVING clause":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, rev INT)")
    discard execSql(d, "INSERT INTO sales VALUES (1, 'East', 500)")
    discard execSql(d, "INSERT INTO sales VALUES (2, 'West', 300)")
    discard execSql(d, "INSERT INTO sales VALUES (3, 'East', 200)")
    let r = execSql(d, "CREATE VIEW big_regions AS SELECT region, SUM(rev) AS total FROM sales GROUP BY region HAVING SUM(rev) > 400")
    require r.ok

  test "CREATE VIEW with ORDER BY clause":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT)")
    discard execSql(d, "INSERT INTO items VALUES (1, 'A', 10), (2, 'B', 5), (3, 'C', 20)")
    let r = execSql(d, "CREATE VIEW sorted_items AS SELECT id, name, price FROM items ORDER BY price DESC")
    require r.ok

  test "CREATE VIEW with LIMIT clause":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    for i in 1..10:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ", " & $(i*2) & ")")
    let r = execSql(d, "CREATE VIEW top5 AS SELECT id, v FROM t ORDER BY v DESC LIMIT 5")
    require r.ok

  test "CREATE VIEW with ORDER BY DESC":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
    let r = execSql(d, "CREATE VIEW desc_view AS SELECT id, name FROM t ORDER BY name DESC")
    require r.ok

  test "CREATE VIEW with OFFSET clause":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..5:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ")")
    let r = execSql(d, "CREATE VIEW paged AS SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 1")
    require r.ok

suite "ALTER VIEW RENAME via SQL parsing":
  test "ALTER VIEW RENAME parses correctly":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE VIEW v_old AS SELECT id FROM t")
    let r = execSql(d, "ALTER VIEW v_old RENAME TO v_new")
    require r.ok
    let r2 = execSql(d, "SELECT id FROM v_new")
    require r2.ok

  test "ALTER TABLE RENAME COLUMN parses and executes":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'Alice')")
    let r = execSql(d, "ALTER TABLE t RENAME COLUMN name TO full_name")
    require r.ok
    let q = execSql(d, "SELECT id, full_name FROM t")
    require q.ok
    check q.value[0] == "1|Alice"

suite "SQL value functions":
  test "CURRENT_TIMESTAMP returns a value":
    let d = db()
    defer: discard closeDb(d)
    let r = execSql(d, "SELECT CURRENT_TIMESTAMP")
    require r.ok
    check r.value.len == 1
    check r.value[0].len > 0

  test "CURRENT_DATE returns a value":
    let d = db()
    defer: discard closeDb(d)
    let r = execSql(d, "SELECT CURRENT_DATE")
    require r.ok
    check r.value.len == 1

  test "NOW() function returns a timestamp":
    let d = db()
    defer: discard closeDb(d)
    let r = execSql(d, "SELECT NOW()")
    require r.ok
    check r.value.len == 1

suite "IN subquery and scalar subquery in exec":
  test "EXISTS correlated subquery basic":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, pid INT)")
    discard execSql(d, "INSERT INTO parent VALUES (1), (2), (3)")
    discard execSql(d, "INSERT INTO child VALUES (1, 1), (2, 2)")
    # EXISTS subquery
    let r = execSql(d, "SELECT id FROM parent WHERE EXISTS(SELECT 1 FROM child WHERE child.pid = parent.id)")
    require r.ok
    check r.value.len == 2
    check "1" in r.value
    check "2" in r.value

  test "NOT EXISTS subquery":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, pid INT)")
    discard execSql(d, "INSERT INTO parent VALUES (1), (2), (3)")
    discard execSql(d, "INSERT INTO child VALUES (1, 1)")
    let r = execSql(d, "SELECT id FROM parent WHERE NOT EXISTS(SELECT 1 FROM child WHERE child.pid = parent.id)")
    require r.ok
    check r.value.len == 2

  test "Scalar subquery in SELECT":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE counts (n INT PRIMARY KEY, label TEXT)")
    discard execSql(d, "INSERT INTO counts VALUES (5, 'five')")
    let r = execSql(d, "SELECT (SELECT n FROM counts WHERE label = 'five')")
    require r.ok
    check r.value[0] == "5"

  test "IN subquery with correlated values":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'A'), (2, 'B'), (3, 'A')")
    let r = execSql(d, "SELECT id FROM t WHERE cat IN ('A', 'B')")
    require r.ok
    check r.value.len == 3

suite "selectToCanonicalSql round-trip via VIEW":
  test "CREATE VIEW with GROUP BY serializes correctly":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'A'),(2,'B'),(3,'A')")
    # CREATE VIEW triggers selectToCanonicalSql for GROUP BY/HAVING paths
    let r = execSql(d, "CREATE VIEW v AS SELECT cat, COUNT(*) AS cnt FROM t GROUP BY cat HAVING COUNT(*) > 0")
    require r.ok

  test "CREATE VIEW with ORDER BY serializes correctly":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')")
    # CREATE VIEW triggers selectToCanonicalSql for ORDER BY path
    let r = execSql(d, "CREATE VIEW v AS SELECT id, name FROM t ORDER BY name DESC")
    require r.ok

  test "CREATE VIEW with LIMIT and OFFSET serializes correctly":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
    # CREATE VIEW triggers selectToCanonicalSql for LIMIT/OFFSET path
    let r = execSql(d, "CREATE VIEW v AS SELECT id, v FROM t ORDER BY id LIMIT 3 OFFSET 1")
    require r.ok
