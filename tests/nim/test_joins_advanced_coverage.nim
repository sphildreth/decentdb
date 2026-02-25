## Coverage tests for planner.nim makeJoinPlan:
## - RIGHT JOIN (jtRight → swap operands)
## - FULL OUTER JOIN (jtFull)
## - FULL OUTER JOIN with indexed right side (falls back to table scan)
## - CROSS JOIN
## - Self-join
## - Multi-table join chains
## - LEFT JOIN with NULL propagation
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

suite "RIGHT JOIN":
  test "RIGHT JOIN returns all right-table rows":
    let db = freshDb("trj1.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "CREATE TABLE b (bid INT PRIMARY KEY, aid INT, w TEXT)")
    discard execSql(db, "INSERT INTO a VALUES (1, 'alpha')")
    discard execSql(db, "INSERT INTO a VALUES (2, 'beta')")
    discard execSql(db, "INSERT INTO b VALUES (10, 1, 'X')")
    discard execSql(db, "INSERT INTO b VALUES (20, 3, 'Y')")  # no matching a
    let res = execSql(db, "SELECT a.v, b.w FROM a RIGHT JOIN b ON a.id = b.aid ORDER BY b.bid")
    require res.ok
    check res.value.len == 2
    check res.value[0].split("|")[0] == "alpha"
    check res.value[1].split("|")[0] == "NULL"
    check res.value[1].split("|")[1] == "Y"
    discard closeDb(db)

  test "RIGHT JOIN with all rows matching":
    let db = freshDb("trj2.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)")
    for i in 1..3:
      discard execSql(db, "INSERT INTO a VALUES (" & $i & ", " & $(i*10) & ")")
      discard execSql(db, "INSERT INTO b VALUES (" & $i & ", " & $(i*100) & ")")
    let res = execSql(db, "SELECT a.v, b.v FROM a RIGHT JOIN b ON a.id = b.id ORDER BY a.v")
    require res.ok
    check res.value.len == 3
    check res.value[0] == "10|100"
    discard closeDb(db)

  test "RIGHT JOIN with empty left table":
    let db = freshDb("trj3.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO b VALUES (1, 'only_b')")
    let res = execSql(db, "SELECT a.id, b.v FROM a RIGHT JOIN b ON a.id = b.id")
    require res.ok
    check res.value.len == 1
    check res.value[0].split("|")[0] == "NULL"
    check res.value[0].split("|")[1] == "only_b"
    discard closeDb(db)

  test "RIGHT JOIN empty right table returns no rows":
    let db = freshDb("trj4.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO a VALUES (1)")
    let res = execSql(db, "SELECT a.id, b.id FROM a RIGHT JOIN b ON a.id = b.id")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

suite "FULL OUTER JOIN":
  test "FULL OUTER JOIN all combinations":
    let db = freshDb("tfoj1.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO a VALUES (1, 'a1')")
    discard execSql(db, "INSERT INTO a VALUES (2, 'a2')")
    discard execSql(db, "INSERT INTO b VALUES (2, 'b2')")
    discard execSql(db, "INSERT INTO b VALUES (3, 'b3')")
    let res = execSql(db, "SELECT a.v, b.v FROM a FULL OUTER JOIN b ON a.id = b.id ORDER BY COALESCE(a.id, b.id)")
    require res.ok
    check res.value.len == 3
    # a1|NULL, a2|b2, NULL|b3
    let v = res.value
    check v[0].split("|")[0] == "a1"
    check v[0].split("|")[1] == "NULL"
    check v[1] == "a2|b2"
    check v[2].split("|")[0] == "NULL"
    check v[2].split("|")[1] == "b3"
    discard closeDb(db)

  test "FULL OUTER JOIN with indexed column on right":
    # Tests jtFull branch where right.kind == pkIndexSeek → fallback to table scan
    let db = freshDb("tfoj2.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, x INT)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, y INT)")
    discard execSql(db, "INSERT INTO a VALUES (1, 10)")
    discard execSql(db, "INSERT INTO a VALUES (2, 20)")
    discard execSql(db, "INSERT INTO b VALUES (1, 100)")
    discard execSql(db, "INSERT INTO b VALUES (3, 300)")
    # b.id is a PK index; FULL JOIN should fall back to scan for correct semantics
    let res = execSql(db, "SELECT a.x, b.y FROM a FULL OUTER JOIN b ON a.id = b.id")
    require res.ok
    check res.value.len == 3
    discard closeDb(db)

  test "FULL OUTER JOIN on empty tables":
    let db = freshDb("tfoj3.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY)")
    let res = execSql(db, "SELECT a.id, b.id FROM a FULL OUTER JOIN b ON a.id = b.id")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

suite "CROSS JOIN":
  test "CROSS JOIN produces cartesian product":
    let db = freshDb("tcj1.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY)")
    for i in 1..3: discard execSql(db, "INSERT INTO a VALUES (" & $i & ")")
    for i in 1..2: discard execSql(db, "INSERT INTO b VALUES (" & $i & ")")
    let res = execSql(db, "SELECT a.id, b.id FROM a CROSS JOIN b ORDER BY a.id, b.id")
    require res.ok
    check res.value.len == 6
    check res.value[0] == "1|1"
    check res.value[5] == "3|2"
    discard closeDb(db)

  test "CROSS JOIN with empty table gives empty result":
    let db = freshDb("tcj2.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO a VALUES (1)")
    let res = execSql(db, "SELECT a.id, b.id FROM a CROSS JOIN b")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

suite "Self-join and multi-table":
  test "Self-join on same table with aliases":
    let db = freshDb("tsj1.ddb")
    discard execSql(db, "CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, mgr INT)")
    discard execSql(db, "INSERT INTO emp VALUES (1, 'Alice', NULL)")
    discard execSql(db, "INSERT INTO emp VALUES (2, 'Bob', 1)")
    discard execSql(db, "INSERT INTO emp VALUES (3, 'Carol', 1)")
    let res = execSql(db, "SELECT e.name, m.name FROM emp e JOIN emp m ON e.mgr = m.id ORDER BY e.id")
    require res.ok
    check res.value.len == 2
    check res.value[0] == "Bob|Alice"
    check res.value[1] == "Carol|Alice"
    discard closeDb(db)

  test "Three-table chain join using subquery":
    let db = freshDb("tmj1.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT)")
    discard execSql(db, "CREATE TABLE c (id INT PRIMARY KEY, bid INT, w TEXT)")
    discard execSql(db, "INSERT INTO a VALUES (1, 'X')")
    discard execSql(db, "INSERT INTO b VALUES (10, 1)")
    discard execSql(db, "INSERT INTO c VALUES (100, 10, 'Z')")
    # Use subquery to chain joins
    let res = execSql(db, "SELECT ab.v, c.w FROM (SELECT a.v, b.id AS bid FROM a JOIN b ON a.id = b.aid) ab JOIN c ON ab.bid = c.bid")
    require res.ok
    check res.value.len == 1
    check res.value[0] == "X|Z"
    discard closeDb(db)

  test "LEFT JOIN with all NULLs on right":
    let db = freshDb("tlj1.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT)")
    discard execSql(db, "INSERT INTO a VALUES (1, 'only')")
    let res = execSql(db, "SELECT a.v, b.id FROM a LEFT JOIN b ON a.id = b.aid")
    require res.ok
    check res.value.len == 1
    check res.value[0].split("|")[1] == "NULL"
    discard closeDb(db)

suite "OR-to-union index optimization":
  test "OR on indexed column uses union-distinct plan":
    # Tests the pkUnionDistinct planning path
    let db = freshDb("tor1.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    for i in 1..10: discard execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")")
    # OR on PK = equality: planner may use union of seeks
    let res = execSql(db, "SELECT id FROM t WHERE id = 2 OR id = 5 OR id = 8 ORDER BY id")
    require res.ok
    check res.value.len == 3
    check res.value[0] == "2"
    check res.value[1] == "5"
    check res.value[2] == "8"
    discard closeDb(db)

  test "OR with non-indexed second condition uses scan":
    let db = freshDb("tor2.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    for i in 1..5: discard execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")")
    let res = execSql(db, "SELECT id FROM t WHERE id = 1 OR v = 30 ORDER BY id")
    require res.ok
    check res.value.len == 2
    check res.value[0] == "1"
    check res.value[1] == "3"
    discard closeDb(db)
