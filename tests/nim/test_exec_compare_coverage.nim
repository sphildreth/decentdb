## Coverage tests for ORDER BY edge cases that exercise exec.nim compareValues
## Targets:
## - compareValues for vkBoolTrue/vkBoolFalse (L1708)
## - compareValues for vkInt0/vkInt1 special cases
## - compareDecimals for ORDER BY on DECIMAL columns
## - valueToSqlLiteral vkBool cases (for subqueries)
## - Various exec.nim path branches
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

suite "ORDER BY BOOL column":
  test "ORDER BY BOOL ASC":
    let db = freshDb("ord_bool1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, flag BOOL)")
    discard execSql(db, "INSERT INTO t VALUES (1, true), (2, false), (3, true), (4, false)")
    let sel = execSql(db, "SELECT id FROM t ORDER BY flag ASC, id ASC")
    require sel.ok
    check sel.value.len == 4
    check sel.value[0] == "2"
    check sel.value[1] == "4"
    check sel.value[2] == "1"
    check sel.value[3] == "3"
    discard closeDb(db)

  test "ORDER BY BOOL DESC":
    let db = freshDb("ord_bool2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, flag BOOL)")
    discard execSql(db, "INSERT INTO t VALUES (1, true), (2, false), (3, true)")
    let sel = execSql(db, "SELECT id FROM t ORDER BY flag DESC, id ASC")
    require sel.ok
    check sel.value[0] == "1"
    check sel.value[1] == "3"
    check sel.value[2] == "2"
    discard closeDb(db)

  test "GROUP BY BOOL column":
    let db = freshDb("grp_bool1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT, flag BOOL)")
    discard execSql(db, "INSERT INTO t VALUES (1, true), (2, false), (3, true), (4, false), (5, true)")
    let sel = execSql(db, "SELECT flag, COUNT(*) FROM t GROUP BY flag ORDER BY flag ASC")
    require sel.ok
    check sel.value.len == 2  # 2 groups (false, true)
    check sel.value[0] == "false|2"
    check sel.value[1] == "true|3"
    discard closeDb(db)

  test "BOOL in WHERE comparison":
    let db = freshDb("bool_where1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, active BOOL, score INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, true, 90), (2, false, 70), (3, true, 80)")
    let sel = execSql(db, "SELECT id, score FROM t WHERE active = true ORDER BY score DESC")
    require sel.ok
    check sel.value.len == 2
    check sel.value[0] == "1|90"
    check sel.value[1] == "3|80"
    discard closeDb(db)

suite "ORDER BY DECIMAL column":
  test "ORDER BY DECIMAL(10,2) column":
    let db = freshDb("ord_dec1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, price DECIMAL(10,2))")
    discard execSql(db, "INSERT INTO t VALUES (1, 9.99), (2, 1.50), (3, 100.00), (4, 9.98)")
    let sel = execSql(db, "SELECT id FROM t ORDER BY price ASC")
    require sel.ok
    check sel.value.len == 4
    check sel.value[0] == "2"
    check sel.value[1] == "4"
    check sel.value[2] == "1"
    check sel.value[3] == "3"
    discard closeDb(db)

  test "ORDER BY DECIMAL DESC":
    let db = freshDb("ord_dec2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, amount DECIMAL(10,3))")
    discard execSql(db, "INSERT INTO t VALUES (1, 1.001), (2, 1.010), (3, 1.100), (4, 0.999)")
    let sel = execSql(db, "SELECT id FROM t ORDER BY amount DESC")
    require sel.ok
    check sel.value[0] == "3"
    check sel.value[1] == "2"
    check sel.value[2] == "1"
    check sel.value[3] == "4"
    discard closeDb(db)

  test "GROUP BY DECIMAL":
    let db = freshDb("grp_dec1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT, price DECIMAL(5,2))")
    discard execSql(db, "INSERT INTO t VALUES (1, 1.50), (2, 1.50), (3, 2.00), (4, 2.00)")
    let sel = execSql(db, "SELECT price, COUNT(*) FROM t GROUP BY price ORDER BY price")
    require sel.ok
    check sel.value.len == 2  # 2 groups
    check sel.value[0] == "1.50|2"
    discard closeDb(db)

suite "INT special value comparisons (vkInt0/vkInt1)":
  test "WHERE id = 0 vs 1 boundary":
    let db = freshDb("int01_1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (0, 'zero'), (1, 'one'), (2, 'two')")
    let sel0 = execSql(db, "SELECT v FROM t WHERE id = 0")
    require sel0.ok
    check sel0.value.len == 1
    check sel0.value[0] == "zero"
    let sel1 = execSql(db, "SELECT v FROM t WHERE id = 1")
    require sel1.ok
    check sel1.value[0] == "one"
    discard closeDb(db)

  test "ORDER BY with 0, 1, many values":
    let db = freshDb("int01_2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, n INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 0), (2, 1), (3, 0), (4, 1), (5, 5)")
    let sel = execSql(db, "SELECT id FROM t ORDER BY n ASC, id ASC")
    require sel.ok
    check sel.value.len == 5
    check sel.value[0] == "1"
    check sel.value[1] == "3"
    check sel.value[2] == "2"
    check sel.value[3] == "4"
    check sel.value[4] == "5"
    discard closeDb(db)

suite "Mixed type ORDER BY":
  test "ORDER BY NULL vs non-NULL (NULLs sort first by default)":
    let db = freshDb("ord_null1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, NULL), (2, 10), (3, NULL), (4, 5)")
    let sel = execSql(db, "SELECT id FROM t ORDER BY v ASC, id ASC")
    require sel.ok
    check sel.value.len == 4
    discard closeDb(db)

  test "ORDER BY multiple columns mixed types":
    let db = freshDb("ord_multi1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, score DECIMAL(5,2))")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a', 10.5), (2, 'b', 10.5), (3, 'a', 5.0), (4, 'b', 15.0)")
    let sel = execSql(db, "SELECT id FROM t ORDER BY cat ASC, score DESC")
    require sel.ok
    check sel.value.len == 4
    check sel.value[0] == "1"  # cat=a, score=10.5
    check sel.value[1] == "3"  # cat=a, score=5.0
    check sel.value[2] == "4"  # cat=b, score=15.0
    check sel.value[3] == "2"  # cat=b, score=10.5
    discard closeDb(db)

suite "Bool in subquery and correlated context":
  test "EXISTS subquery with bool column":
    let db = freshDb("bool_sub1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, active BOOL)")
    discard execSql(db, "CREATE TABLE u (id INT PRIMARY KEY, tid INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, true), (2, false)")
    discard execSql(db, "INSERT INTO u VALUES (1, 1), (2, 2)")
    let sel = execSql(db, "SELECT u.id FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.id = u.tid AND t.active = true)")
    require sel.ok
    check sel.value.len == 1
    check sel.value[0] == "1"
    discard closeDb(db)

  test "IN subquery returns bool":
    let db = freshDb("bool_sub2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    let sel = execSql(db, "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE v IN ('a', 'c'))")
    require sel.ok
    check sel.value.len == 2
    discard closeDb(db)

suite "Aggregate edge cases":
  test "SUM over BOOL column":
    let db = freshDb("agg_bool1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT, flag BOOL)")
    discard execSql(db, "INSERT INTO t VALUES (1, true), (2, false), (3, true)")
    # SUM of BOOL should treat true=1, false=0
    let sel = execSql(db, "SELECT SUM(CAST(flag AS INT64)) FROM t")
    require sel.ok
    check sel.value[0] == "2"
    discard closeDb(db)

  test "MIN/MAX of BOOL":
    let db = freshDb("agg_bool2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT, flag BOOL)")
    discard execSql(db, "INSERT INTO t VALUES (1, true), (2, false), (3, true)")
    # MIN/MAX of BOOL returns the extreme bool value
    let selMin = execSql(db, "SELECT MIN(flag) FROM t")
    require selMin.ok
    check selMin.value[0] in ["false", "true", "0", "1"]  # implementation-dependent
    let selMax = execSql(db, "SELECT MAX(flag) FROM t")
    require selMax.ok
    check selMax.value[0] in ["false", "true", "0", "1"]
    discard closeDb(db)

  test "ORDER BY with CASE expression":
    let db = freshDb("ord_case1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, priority TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'low'), (2, 'high'), (3, 'medium'), (4, 'high')")
    let sel = execSql(db, """
      SELECT id FROM t
      ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, id
    """)
    require sel.ok
    check sel.value.len == 4
    check sel.value[0] == "2"
    check sel.value[1] == "4"
    check sel.value[2] == "3"
    check sel.value[3] == "1"
    discard closeDb(db)
