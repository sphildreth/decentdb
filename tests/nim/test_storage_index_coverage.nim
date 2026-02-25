## Coverage tests for storage.nim castExpressionValue (L178) and evalPredicateValue (L334)
## Expression indexes with CAST and partial indexes with WHERE predicates.
import unittest
import os
import strutils
import engine
import errors

proc freshDb(name: string): Db =
  let path = getTempDir() / name
  for ext in ["", "-wal"]:
    let f = (if ext.len == 0: path else: path & ext)
    if fileExists(f): removeFile(f)
  openDb(path).value

suite "Expression index with CAST":
  test "CAST(TEXT AS INT) expression index - query uses index":
    let db = freshDb("cast_int.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
    discard execSql(db, "CREATE INDEX idx_cast_int ON t (CAST(s AS INT))")
    discard execSql(db, "INSERT INTO t VALUES (1, '10')")
    discard execSql(db, "INSERT INTO t VALUES (2, '20')")
    discard execSql(db, "INSERT INTO t VALUES (3, '30')")
    let res = execSql(db, "SELECT id FROM t WHERE CAST(s AS INT) = 20")
    require res.ok
    check res.value == @["2"]
    discard closeDb(db)

  test "CAST(TEXT AS FLOAT) expression index":
    let db = freshDb("cast_float.ddb")
    discard execSql(db, "CREATE TABLE tf (id INT PRIMARY KEY, s TEXT)")
    discard execSql(db, "CREATE INDEX idx_cast_float ON tf (CAST(s AS FLOAT))")
    discard execSql(db, "INSERT INTO tf VALUES (1, '1.5')")
    discard execSql(db, "INSERT INTO tf VALUES (2, '2.5')")
    let res = execSql(db, "SELECT id FROM tf WHERE CAST(s AS FLOAT) > 2.0")
    require res.ok
    check res.value == @["2"]
    discard closeDb(db)

  test "CAST(TEXT AS TEXT) expression index":
    let db = freshDb("cast_text.ddb")
    discard execSql(db, "CREATE TABLE tt (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE INDEX idx_cast_text ON tt (CAST(v AS TEXT))")
    discard execSql(db, "INSERT INTO tt VALUES (1, 100)")
    discard execSql(db, "INSERT INTO tt VALUES (2, 200)")
    let res = execSql(db, "SELECT id FROM tt WHERE CAST(v AS TEXT) = '100'")
    require res.ok
    check res.value == @["1"]
    discard closeDb(db)

  test "Multiple rows with CAST expression index scan":
    let db = freshDb("cast_scan.ddb")
    discard execSql(db, "CREATE TABLE ts (id INT PRIMARY KEY, s TEXT)")
    discard execSql(db, "CREATE INDEX idx_ts ON ts (CAST(s AS INT))")
    for i in 1..10:
      discard execSql(db, "INSERT INTO ts VALUES (" & $i & ", '" & $(i * 10) & "')")
    let res = execSql(db, "SELECT id FROM ts WHERE CAST(s AS INT) > 50 ORDER BY id")
    require res.ok
    check res.value.len == 5
    check res.value[0] == "6"
    discard closeDb(db)

suite "Partial index with WHERE predicate":
  test "Partial index WHERE active = TRUE":
    let db = freshDb("partial_bool.ddb")
    discard execSql(db, "CREATE TABLE p (id INT PRIMARY KEY, v INT, active BOOL)")
    discard execSql(db, "CREATE INDEX idx_active ON p (v) WHERE active = TRUE")
    discard execSql(db, "INSERT INTO p VALUES (1, 10, TRUE)")
    discard execSql(db, "INSERT INTO p VALUES (2, 20, FALSE)")
    discard execSql(db, "INSERT INTO p VALUES (3, 30, TRUE)")
    let res = execSql(db, "SELECT id FROM p WHERE v > 0 AND active = TRUE ORDER BY id")
    require res.ok
    check res.value == @["1", "3"]
    discard closeDb(db)

  test "Partial index WHERE v > constant":
    let db = freshDb("partial_gt.ddb")
    discard execSql(db, "CREATE TABLE pg2 (id INT PRIMARY KEY, v INT, score INT)")
    discard execSql(db, "CREATE INDEX idx_score ON pg2 (score) WHERE v > 5")
    discard execSql(db, "INSERT INTO pg2 VALUES (1, 3, 100)")
    discard execSql(db, "INSERT INTO pg2 VALUES (2, 7, 200)")
    discard execSql(db, "INSERT INTO pg2 VALUES (3, 10, 300)")
    let res = execSql(db, "SELECT id FROM pg2 WHERE score > 150 AND v > 5 ORDER BY id")
    require res.ok
    check res.value == @["2", "3"]
    discard closeDb(db)

  test "Partial index WHERE col IS NOT NULL":
    let db = freshDb("partial_notnull.ddb")
    discard execSql(db, "CREATE TABLE pn (id INT PRIMARY KEY, v INT, tag TEXT)")
    discard execSql(db, "CREATE INDEX idx_tag_nn ON pn (v) WHERE tag IS NOT NULL")
    discard execSql(db, "INSERT INTO pn VALUES (1, 10, 'x')")
    discard execSql(db, "INSERT INTO pn VALUES (2, 20, NULL)")
    discard execSql(db, "INSERT INTO pn VALUES (3, 30, 'y')")
    let res = execSql(db, "SELECT id FROM pn WHERE v > 0 AND tag IS NOT NULL ORDER BY id")
    require res.ok
    check res.value == @["1", "3"]
    discard closeDb(db)

  test "Partial index WHERE col IS NULL":
    let db = freshDb("partial_null.ddb")
    discard execSql(db, "CREATE TABLE pnu (id INT PRIMARY KEY, v INT, tag TEXT)")
    discard execSql(db, "CREATE INDEX idx_tag_null ON pnu (v) WHERE tag IS NULL")
    discard execSql(db, "INSERT INTO pnu VALUES (1, 10, 'x')")
    discard execSql(db, "INSERT INTO pnu VALUES (2, 20, NULL)")
    discard execSql(db, "INSERT INTO pnu VALUES (3, 30, NULL)")
    let res = execSql(db, "SELECT id FROM pnu WHERE v > 0 AND tag IS NULL ORDER BY id")
    require res.ok
    check res.value == @["2", "3"]
    discard closeDb(db)

  test "Partial index with AND predicate":
    let db = freshDb("partial_and.ddb")
    discard execSql(db, "CREATE TABLE pa (id INT PRIMARY KEY, a INT, b INT, v INT)")
    discard execSql(db, "CREATE INDEX idx_and ON pa (v) WHERE a > 0 AND b > 0")
    discard execSql(db, "INSERT INTO pa VALUES (1, 1, 1, 100)")
    discard execSql(db, "INSERT INTO pa VALUES (2, 0, 1, 200)")
    discard execSql(db, "INSERT INTO pa VALUES (3, 1, 0, 300)")
    discard execSql(db, "INSERT INTO pa VALUES (4, 2, 2, 400)")
    let res = execSql(db, "SELECT id FROM pa WHERE a > 0 AND b > 0 ORDER BY id")
    require res.ok
    check res.value == @["1", "4"]
    discard closeDb(db)

  test "Insert into table with partial index - rows not matching predicate":
    let db = freshDb("partial_nomatch.ddb")
    discard execSql(db, "CREATE TABLE pnm (id INT PRIMARY KEY, v INT, active BOOL)")
    discard execSql(db, "CREATE INDEX idx_active2 ON pnm (v) WHERE active = TRUE")
    # Insert rows that don't match the partial index predicate
    for i in 1..5:
      discard execSql(db, "INSERT INTO pnm VALUES (" & $i & ", " & $(i*10) & ", FALSE)")
    let res = execSql(db, "SELECT COUNT(*) FROM pnm")
    require res.ok
    check res.value == @["5"]
    discard closeDb(db)
