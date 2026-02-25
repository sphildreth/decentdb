## Coverage tests for btree.nim - internal node splits, large datasets,
## various scan patterns, delete paths, and edge cases.
## Targets: btree.nim L1751-1772 (append cache), L1998-2038 (splits),
##          L2083-2115 (parent splits), delete path btree.nim
import unittest
import strutils
import engine

proc db(): Db = openDb(":memory:").value

suite "B-tree with large datasets":
  test "1000 sequential inserts then full scan":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..1000:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ")")
    let r = execSql(d, "SELECT COUNT(*) FROM t")
    require r.ok
    check r.value[0] == "1000"

  test "1000 inserts with random-ish order":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    # Insert in a pattern that exercises splits from both ends
    for i in 1..500:
      discard execSql(d, "INSERT INTO t VALUES (" & $(i*2) & ", 'even" & $i & "')")
    for i in 1..500:
      discard execSql(d, "INSERT INTO t VALUES (" & $(i*2-1) & ", 'odd" & $i & "')")
    let r = execSql(d, "SELECT COUNT(*) FROM t")
    require r.ok
    check r.value[0] == "1000"

  test "Large text values force early splits":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, data TEXT)")
    let chunk = 'X'.repeat(200)
    for i in 1..200:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ", '" & chunk & $i & "')")
    let r = execSql(d, "SELECT COUNT(*) FROM t")
    require r.ok
    check r.value[0] == "200"
    # Range scan
    let r2 = execSql(d, "SELECT id FROM t WHERE id >= 100 AND id <= 110 ORDER BY id")
    require r2.ok
    check r2.value.len == 11

  test "Sequential deletes after large insert":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..500:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ")")
    for i in 1..250:
      discard execSql(d, "DELETE FROM t WHERE id = " & $i)
    let r = execSql(d, "SELECT COUNT(*) FROM t")
    require r.ok
    check r.value[0] == "250"

  test "Delete then re-insert fills gaps":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    for i in 1..100:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ", " & $i & ")")
    for i in 1..100:
      if i mod 2 == 0:
        discard execSql(d, "DELETE FROM t WHERE id = " & $i)
    for i in 1..100:
      if i mod 2 == 0:
        discard execSql(d, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")")
    let r = execSql(d, "SELECT COUNT(*) FROM t")
    require r.ok
    check r.value[0] == "100"

  test "ORDER BY DESC on large table":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..100:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ")")
    let r = execSql(d, "SELECT id FROM t ORDER BY id DESC LIMIT 5")
    require r.ok
    check r.value.len == 5
    check r.value[0] == "100"
    check r.value[4] == "96"

  test "LIMIT and OFFSET with large table":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..200:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ")")
    let r = execSql(d, "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 100")
    require r.ok
    check r.value.len == 10
    check r.value[0] == "101"
    check r.value[9] == "110"

suite "Secondary index with large datasets":
  test "1000 inserts with secondary index scan":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)")
    discard execSql(d, "CREATE INDEX t_cat_idx ON t(cat)")
    for i in 1..1000:
      let cat = if i mod 3 == 0: "A" elif i mod 3 == 1: "B" else: "C"
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ", '" & cat & "', " & $i & ")")
    let r = execSql(d, "SELECT COUNT(*) FROM t WHERE cat = 'A'")
    require r.ok
    let count = parseInt(r.value[0])
    check count > 300

  test "Index on multiple columns, range query":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, score INT)")
    discard execSql(d, "CREATE INDEX t_score_idx ON t(score)")
    for i in 1..200:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ", " & $(i * 3) & ")")
    let r = execSql(d, "SELECT id FROM t WHERE score >= 300 AND score <= 600 ORDER BY id LIMIT 5")
    require r.ok
    check r.value.len == 5

suite "UPDATE on large dataset":
  test "Bulk UPDATE with WHERE clause":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT, cat TEXT)")
    for i in 1..200:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ", " & $i & ", 'original')")
    let upd = execSql(d, "UPDATE t SET cat = 'updated' WHERE val > 100")
    require upd.ok
    let r = execSql(d, "SELECT COUNT(*) FROM t WHERE cat = 'updated'")
    require r.ok
    check r.value[0] == "100"

  test "UPDATE changes primary key collision error":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 10), (2, 20)")
    let r = execSql(d, "UPDATE t SET id = 2 WHERE id = 1")
    check not r.ok

suite "JOIN with large datasets":
  test "INNER JOIN produces correct count":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, pid INT)")
    for i in 1..50:
      discard execSql(d, "INSERT INTO parent VALUES (" & $i & ")")
    for i in 1..200:
      discard execSql(d, "INSERT INTO child VALUES (" & $i & ", " & $(((i-1) mod 50) + 1) & ")")
    let r = execSql(d, "SELECT COUNT(*) FROM child INNER JOIN parent ON child.pid = parent.id")
    require r.ok
    check r.value[0] == "200"

  test "LEFT JOIN includes all left rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE a (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "CREATE TABLE b (id INT PRIMARY KEY, aid INT, val INT)")
    for i in 1..20:
      discard execSql(d, "INSERT INTO a VALUES (" & $i & ", 'item" & $i & "')")
    for i in 1..10:
      discard execSql(d, "INSERT INTO b VALUES (" & $i & ", " & $i & ", " & $(i*10) & ")")
    let r = execSql(d, "SELECT COUNT(*) FROM a LEFT JOIN b ON b.aid = a.id")
    require r.ok
    check r.value[0] == "20"
