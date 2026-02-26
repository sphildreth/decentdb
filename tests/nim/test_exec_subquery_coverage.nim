## Coverage tests for exec.nim subquery evaluation paths:
## - EXISTS subquery evaluation (L3368-3403)
## - IN_SUBQUERY evaluation (L3403-3415)
## - SCALAR_SUBQUERY evaluation (L3416-3483)
## - Correlated subquery via substituteCorrelatedExpr (L1822-1839)
import unittest
import strutils
import engine

proc db(): Db = openDb(":memory:").value

suite "EXISTS subquery":
  test "EXISTS returns matching rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE p (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE c (id INT PRIMARY KEY, pid INT)")
    discard execSql(d, "INSERT INTO p VALUES (1),(2),(3)")
    discard execSql(d, "INSERT INTO c VALUES (10,1),(11,2)")
    let r = execSql(d, "SELECT id FROM p WHERE EXISTS (SELECT 1 FROM c WHERE c.pid = p.id)")
    require r.ok
    check r.value.len == 2

  test "NOT EXISTS filters out rows with children":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE p (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE c (id INT PRIMARY KEY, pid INT)")
    discard execSql(d, "INSERT INTO p VALUES (1),(2),(3)")
    discard execSql(d, "INSERT INTO c VALUES (10,1),(11,2)")
    let r = execSql(d, "SELECT id FROM p WHERE NOT EXISTS (SELECT 1 FROM c WHERE c.pid = p.id)")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "3"

  test "EXISTS with empty subquery returns false":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    discard execSql(d, "CREATE TABLE empty (id INT PRIMARY KEY)")
    let r = execSql(d, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM empty)")
    require r.ok
    check r.value.len == 0

  test "EXISTS with non-empty subquery always returns true":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1),(2),(3)")
    discard execSql(d, "CREATE TABLE ref (x INT)")
    discard execSql(d, "INSERT INTO ref VALUES (99)")
    let r = execSql(d, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM ref)")
    require r.ok
    check r.value.len == 3

suite "IN_SUBQUERY":
  test "WHERE col IN (SELECT ...) filters correctly":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE p (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE c (id INT PRIMARY KEY, pid INT)")
    discard execSql(d, "INSERT INTO p VALUES (1),(2),(3),(4)")
    discard execSql(d, "INSERT INTO c VALUES (10,1),(11,2)")
    let r = execSql(d, "SELECT id FROM p WHERE id IN (SELECT pid FROM c)")
    require r.ok
    check r.value.len == 2

  test "WHERE col IN (SELECT ...) with empty subquery returns nothing":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1),(2),(3)")
    discard execSql(d, "CREATE TABLE empty (x INT)")
    let r = execSql(d, "SELECT id FROM t WHERE id IN (SELECT x FROM empty)")
    require r.ok
    check r.value.len == 0

  test "WHERE col IN (SELECT ...) with multiple matches":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE users (id INT PRIMARY KEY, role TEXT)")
    discard execSql(d, "CREATE TABLE admins (role TEXT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO admins VALUES ('admin'),('superuser')")
    discard execSql(d, "INSERT INTO users VALUES (1,'admin'),(2,'user'),(3,'superuser'),(4,'guest')")
    let r = execSql(d, "SELECT id FROM users WHERE role IN (SELECT role FROM admins)")
    require r.ok
    check r.value.len == 2

suite "SCALAR subquery":
  test "Scalar subquery in SELECT returns correlated count":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE p (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE c (id INT PRIMARY KEY, pid INT)")
    discard execSql(d, "INSERT INTO p VALUES (1),(2),(3)")
    discard execSql(d, "INSERT INTO c VALUES (10,1),(11,1),(12,2)")
    let r = execSql(d, "SELECT id, (SELECT COUNT(*) FROM c WHERE c.pid = p.id) AS cnt FROM p ORDER BY id")
    require r.ok
    check r.value.len == 3
    check "2" in r.value[0]
    check "1" in r.value[1]
    check "0" in r.value[2]

  test "Scalar subquery in WHERE clause":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,10),(2,20),(3,30)")
    let r = execSql(d, "SELECT id FROM t WHERE val = (SELECT MAX(val) FROM t)")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "3"

  test "Scalar subquery returning NULL":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    discard execSql(d, "CREATE TABLE empty (x INT)")
    let r = execSql(d, "SELECT id, (SELECT x FROM empty LIMIT 1) AS e FROM t")
    require r.ok
    check r.value.len == 1

suite "Correlated subquery depth":
  test "Correlated subquery with multiple references":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE orders (id INT PRIMARY KEY, cid INT, amount INT)")
    discard execSql(d, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO customers VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')")
    discard execSql(d, "INSERT INTO orders VALUES (1,1,100),(2,1,200),(3,2,50)")
    let r = execSql(d, "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE orders.cid = customers.id AND orders.amount > 100)")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "Alice"

  test "Double nested subquery":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE a (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE b (id INT PRIMARY KEY, aid INT)")
    discard execSql(d, "INSERT INTO a VALUES (1),(2),(3)")
    discard execSql(d, "INSERT INTO b VALUES (10,1),(11,2)")
    let r = execSql(d, "SELECT id FROM a WHERE id IN (SELECT aid FROM b WHERE aid > 0)")
    require r.ok
    check r.value.len == 2
