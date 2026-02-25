## Coverage tests for set operations INTERSECT and EXCEPT including edge cases.
import unittest
import os
import strutils
import sequtils
import algorithm
import engine
import errors

proc freshDb(name: string): Db =
  let path = getTempDir() / name
  for ext in ["", "-wal"]:
    let f = (if ext.len == 0: path else: path & ext)
    if fileExists(f): removeFile(f)
  openDb(path).value

proc col0(rows: seq[string]): string =
  if rows.len == 0: return ""
  rows[0].split("|")[0]

suite "INTERSECT set operation":
  test "INTERSECT with table data":
    let db = freshDb("tsetop_i1.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY)")
    for i in [1, 2, 3]: discard execSql(db, "INSERT INTO a VALUES (" & $i & ")")
    for i in [2, 3, 4]: discard execSql(db, "INSERT INTO b VALUES (" & $i & ")")
    let res = execSql(db, "SELECT id FROM a INTERSECT SELECT id FROM b")
    require res.ok
    let vals = res.value.mapIt(it.split("|")[0]).sorted()
    check vals == @["2", "3"]
    discard closeDb(db)

  test "INTERSECT with no common elements returns empty":
    let db = freshDb("tsetop_i2.ddb")
    discard execSql(db, "CREATE TABLE a2 (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b2 (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO a2 VALUES (1)")
    discard execSql(db, "INSERT INTO b2 VALUES (2)")
    let res = execSql(db, "SELECT id FROM a2 INTERSECT SELECT id FROM b2")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "INTERSECT with empty left returns empty":
    let db = freshDb("tsetop_i3.ddb")
    discard execSql(db, "CREATE TABLE ea (id INT PRIMARY KEY)")
    let res = execSql(db, "SELECT id FROM ea INTERSECT SELECT 1")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "INTERSECT with text columns":
    let db = freshDb("tsetop_i5.ddb")
    discard execSql(db, "CREATE TABLE words_a (w TEXT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE words_b (w TEXT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO words_a VALUES ('hello')")
    discard execSql(db, "INSERT INTO words_a VALUES ('world')")
    discard execSql(db, "INSERT INTO words_b VALUES ('world')")
    discard execSql(db, "INSERT INTO words_b VALUES ('nim')")
    let res = execSql(db, "SELECT w FROM words_a INTERSECT SELECT w FROM words_b")
    require res.ok
    check res.value.len == 1
    check col0(res.value) == "world"
    discard closeDb(db)

  test "INTERSECT result is distinct":
    let db = freshDb("tsetop_i6.ddb")
    discard execSql(db, "CREATE TABLE dup (x INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO dup VALUES (5)")
    # INTERSECT deduplicates: SELECT 5 INTERSECT SELECT 5 gives one row
    let res = execSql(db, "SELECT 5 INTERSECT SELECT x FROM dup")
    require res.ok
    check res.value.len == 1
    discard closeDb(db)

suite "EXCEPT set operation":
  test "EXCEPT removes common rows":
    let db = freshDb("tsetop_e1.ddb")
    discard execSql(db, "CREATE TABLE a3 (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b3 (id INT PRIMARY KEY)")
    for i in [1, 2, 3]: discard execSql(db, "INSERT INTO a3 VALUES (" & $i & ")")
    discard execSql(db, "INSERT INTO b3 VALUES (2)")
    let res = execSql(db, "SELECT id FROM a3 EXCEPT SELECT id FROM b3")
    require res.ok
    let vals = res.value.mapIt(it.split("|")[0]).sorted()
    check vals == @["1", "3"]
    discard closeDb(db)

  test "EXCEPT with no overlap returns all left":
    let db = freshDb("tsetop_e2.ddb")
    discard execSql(db, "CREATE TABLE a4 (id INT PRIMARY KEY)")
    for i in [1, 2]: discard execSql(db, "INSERT INTO a4 VALUES (" & $i & ")")
    let res = execSql(db, "SELECT id FROM a4 EXCEPT SELECT 3")
    require res.ok
    check res.value.len == 2
    discard closeDb(db)

  test "EXCEPT removes all left when all in right":
    let db = freshDb("tsetop_e3.ddb")
    discard execSql(db, "CREATE TABLE a5 (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO a5 VALUES (1)")
    let res = execSql(db, "SELECT id FROM a5 EXCEPT SELECT 1")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "EXCEPT with empty right returns all left":
    let db = freshDb("tsetop_e4.ddb")
    discard execSql(db, "CREATE TABLE a6 (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE b6 (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO a6 VALUES (1)")
    discard execSql(db, "INSERT INTO a6 VALUES (2)")
    let res = execSql(db, "SELECT id FROM a6 EXCEPT SELECT id FROM b6")
    require res.ok
    check res.value.len == 2
    discard closeDb(db)

  test "EXCEPT with text column":
    let db = freshDb("tsetop_e5.ddb")
    discard execSql(db, "CREATE TABLE words_c (w TEXT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE words_d (w TEXT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO words_c VALUES ('apple')")
    discard execSql(db, "INSERT INTO words_c VALUES ('banana')")
    discard execSql(db, "INSERT INTO words_d VALUES ('apple')")
    let res = execSql(db, "SELECT w FROM words_c EXCEPT SELECT w FROM words_d")
    require res.ok
    check res.value.len == 1
    check col0(res.value) == "banana"
    discard closeDb(db)

  test "EXCEPT result is distinct":
    # EXCEPT deduplicates the left side too
    let db = freshDb("tsetop_e6.ddb")
    discard execSql(db, "CREATE TABLE dup2 (x INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO dup2 VALUES (7)")
    let res = execSql(db, "SELECT x FROM dup2 EXCEPT SELECT 99")
    require res.ok
    check res.value.len == 1
    check col0(res.value) == "7"
    discard closeDb(db)
