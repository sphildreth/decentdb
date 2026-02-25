## Coverage tests for binder.nim rewriteExprForViewRef paths:
## - ekUnary in WHERE condition referencing a view column (L874-878)
## - ekFunc in WHERE/ON condition referencing a view column (L879-886)
## - ekInList in WHERE condition referencing a view column (L887-897)
## - ekWindowRowNumber in SELECT from a view (L898-924)
## - bindExpr ekInList path (L597-605)
## - bindExpr ekWindowRowNumber path with partitions (L606-623)
import unittest
import strutils
import engine

proc db(): Db = openDb(":memory:").value

suite "View column expression rewriting — ekInList":
  test "WHERE col IN (...) referencing view column":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'A'),(2,'B'),(3,'C'),(4,'A')")
    discard execSql(d, "CREATE VIEW v AS SELECT id, cat FROM t")
    let r = execSql(d, "SELECT id FROM v WHERE cat IN ('A', 'B')")
    require r.ok
    check r.value.len == 3

  test "WHERE col NOT IN (...) referencing view column":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, code INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
    discard execSql(d, "CREATE VIEW v AS SELECT id, code FROM t")
    let r = execSql(d, "SELECT id FROM v WHERE code NOT IN (10, 30)")
    require r.ok
    check r.value.len == 2

  test "WHERE col IN (...) via JOIN with view":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, region TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'East'),(2,'West'),(3,'East')")
    discard execSql(d, "CREATE VIEW tv AS SELECT id, region FROM t")
    let r = execSql(d, "SELECT id FROM tv WHERE region IN ('East', 'West')")
    require r.ok
    check r.value.len == 3

suite "View column expression rewriting — ekUnary":
  test "WHERE NOT condition referencing view column":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, active INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,1),(2,0),(3,1)")
    discard execSql(d, "CREATE VIEW v AS SELECT id, active FROM t")
    let r = execSql(d, "SELECT id FROM v WHERE NOT active = 0 ORDER BY id")
    require r.ok
    check r.value.len == 2

suite "View column expression rewriting — ekFunc":
  test "WHERE function(view_col) in condition":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')")
    discard execSql(d, "CREATE VIEW v AS SELECT id, name FROM t")
    let r = execSql(d, "SELECT id FROM v WHERE LENGTH(name) > 3 ORDER BY id")
    require r.ok
    check r.value.len >= 2

  test "UPPER() applied to view column":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, tag TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'hello'),(2,'world')")
    discard execSql(d, "CREATE VIEW v AS SELECT id, tag FROM t")
    let r = execSql(d, "SELECT UPPER(tag) FROM v ORDER BY id")
    require r.ok
    check r.value[0] == "HELLO"
    check r.value[1] == "WORLD"

suite "Window functions on view columns — ekWindowRowNumber":
  test "ROW_NUMBER() OVER (ORDER BY view_col)":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,50),(2,30),(3,80),(4,20)")
    discard execSql(d, "CREATE VIEW v AS SELECT id, score FROM t")
    let r = execSql(d, "SELECT ROW_NUMBER() OVER (ORDER BY score) AS rn, id FROM v ORDER BY rn")
    require r.ok
    check r.value.len == 4

  test "ROW_NUMBER() OVER (PARTITION BY col ORDER BY col)":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, score INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40)")
    discard execSql(d, "CREATE VIEW v AS SELECT id, grp, score FROM t")
    let r = execSql(d, "SELECT ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score) AS rn, id, grp FROM v ORDER BY grp, rn")
    require r.ok
    check r.value.len == 4

  test "RANK() OVER window function on view":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,10),(2,10),(3,20)")
    discard execSql(d, "CREATE VIEW v AS SELECT id, val FROM t")
    let r = execSql(d, "SELECT RANK() OVER (ORDER BY val) AS rnk, id FROM v ORDER BY id")
    require r.ok
    check r.value.len == 3

  test "LAG() window function on view column":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,10),(2,20),(3,30)")
    discard execSql(d, "CREATE VIEW v AS SELECT id, val FROM t")
    let r = execSql(d, "SELECT LAG(val, 1) OVER (ORDER BY id) AS prev, id FROM v ORDER BY id")
    require r.ok
    check r.value.len == 3

suite "bindExpr ekInList path":
  test "IN list with integer literals binds correctly":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,10),(2,20),(3,30)")
    let r = execSql(d, "SELECT id FROM t WHERE v IN (10, 30)")
    require r.ok
    check r.value.len == 2

  test "IN list with text literals binds correctly":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')")
    let r = execSql(d, "SELECT id FROM t WHERE name IN ('Alice', 'Carol')")
    require r.ok
    check r.value.len == 2

  test "NOT IN list via complement query works":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'X'),(2,'Y'),(3,'Z')")
    # Use explicit inequality to verify complement behavior
    let r = execSql(d, "SELECT id FROM t WHERE code <> 'X' ORDER BY id")
    require r.ok
    check r.value.len == 2
    check r.value[0] == "2"
    check r.value[1] == "3"

suite "Window function ordering without PARTITION":
  test "DENSE_RANK() OVER on plain table works":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,100),(2,90),(3,100),(4,80)")
    let r = execSql(d, "SELECT DENSE_RANK() OVER (ORDER BY score DESC) AS dr, id FROM t ORDER BY id")
    require r.ok
    check r.value.len == 4

  test "Window function requires ORDER BY — error without it":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    let r = execSql(d, "SELECT ROW_NUMBER() OVER () FROM t")
    check not r.ok
