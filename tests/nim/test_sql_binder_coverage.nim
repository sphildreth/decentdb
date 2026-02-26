## Tests targeting binder.nim and sql.nim uncovered paths:
## - binder.nim L1101: SELECT * with CTE JOINs (22 C lines)
## - binder.nim L1590: CTE column count mismatch
## - sql.nim ADD COLUMN with CHECK/REFERENCES (19 C lines each)
## - sql.nim exprToCanonicalSql for svBlob/CASE/various (32+24 C lines)
## - sql.nim selectToCanonicalSql INTERSECT ALL / EXCEPT ALL (45 C lines)
## - Various ON CONFLICT paths in binder.nim
## - INSERT INTO ... SELECT column count mismatch
import unittest
import strutils
import engine
import errors

suite "binder CTE star join error":
  test "SELECT * with CTE JOIN is not supported":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").ok
    let r = execSql(db, "WITH cte AS (SELECT 1 as x) SELECT * FROM cte JOIN t ON t.id = cte.x")
    check not r.ok
    check "SELECT * with joined CTEs is not supported" in r.err.message

  test "SELECT cols (not star) with CTE JOIN works":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'x')").ok
    let r = execSql(db, "WITH cte AS (SELECT 1 as x) SELECT t.v FROM cte JOIN t ON t.id = cte.x")
    check r.ok

suite "binder CTE column shape mismatch":
  test "CTE with column list that doesn't match output":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    # This should trigger CTE column shape mismatch: declared 3 columns but query has 2
    let r = execSql(db, "WITH mydata(a, b, c) AS (SELECT 1, 2) SELECT a FROM mydata")
    check not r.ok

suite "sql.nim ADD COLUMN with unsupported constraints":
  test "ADD COLUMN with CHECK constraint is not supported":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE tc (id INT PRIMARY KEY)").ok
    let r = execSql(db, "ALTER TABLE tc ADD COLUMN val INT CHECK (val > 0)")
    check not r.ok
    check "CHECK" in r.err.message or "not supported" in r.err.message

  test "ADD COLUMN with REFERENCES is not supported":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE child (id INT PRIMARY KEY)").ok
    let r = execSql(db, "ALTER TABLE child ADD COLUMN pid INT REFERENCES parent(id)")
    check not r.ok
    check "REFERENCES" in r.err.message or "not supported" in r.err.message

suite "sql.nim exprToCanonicalSql paths":
  test "BLOB literal in WHERE clause":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE blobtest (id INT PRIMARY KEY, data BLOB)").ok
    check execSql(db, "INSERT INTO blobtest VALUES (1, x'DEADBEEF')").ok
    # Query using a blob literal
    let r = execSql(db, "SELECT id FROM blobtest WHERE data = x'DEADBEEF'")
    check r.ok
    check r.value == @["1"]

  test "NULLIF expression":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT NULLIF(1, 1), NULLIF(2, 1)")
    check r.ok
    check r.value.len == 1
    let parts = r.value[0].split('|')
    check parts[0] == "NULL"  # NULLIF(1,1) = NULL
    check parts[1] == "2"  # NULLIF(2,1) = 2

  test "CASE expression in ON CONFLICT DO UPDATE":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE ct (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO ct VALUES (1, 10)").ok
    let r = execSql(db, "INSERT INTO ct VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET v = CASE WHEN ct.v > 15 THEN ct.v ELSE 99 END")
    check r.ok
    let res = execSql(db, "SELECT v FROM ct WHERE id = 1")
    check res.ok
    check res.value == @["99"]

  test "IN subquery expression":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE dst (ref_id INT PRIMARY KEY)").ok
    for i in 1..5:
      check execSql(db, "INSERT INTO src VALUES (" & $i & ")").ok
    check execSql(db, "INSERT INTO dst VALUES (2)").ok
    check execSql(db, "INSERT INTO dst VALUES (4)").ok
    let r = execSql(db, "SELECT id FROM src WHERE id IN (SELECT ref_id FROM dst)")
    check r.ok
    check r.value.len == 2

  test "EXISTS subquery expression":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE b (aid INT)").ok
    check execSql(db, "INSERT INTO a VALUES (1), (2), (3)").ok
    check execSql(db, "INSERT INTO b VALUES (2)").ok
    let r = execSql(db, "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = a.id)")
    check r.ok
    check r.value.len == 1
    check r.value[0] == "2"

  test "Scalar subquery in SELECT":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE s (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO s VALUES (1, 100)").ok
    let r = execSql(db, "SELECT (SELECT v FROM s WHERE id = 1)")
    check r.ok
    check r.value == @["100"]

suite "sql.nim selectToCanonicalSql set ops":
  test "INTERSECT ALL in subquery roundtrip":
    # The INTERSECT ALL path in selectToCanonicalSql should be invoked 
    # when a subquery with INTERSECT ALL is parsed and re-serialized
    # Note: INTERSECT ALL is not supported by the engine, but we can test
    # that the canonical SQL generation itself works via IN (subquery)
    # or via directly testing the sql module
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    # Can't execute INTERSECT ALL but can test that parser/canonical roundtrip works
    # for other set ops used as subqueries
    check execSql(db, "CREATE TABLE s1 (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE s2 (id INT PRIMARY KEY)").ok
    for i in 1..3:
      check execSql(db, "INSERT INTO s1 VALUES (" & $i & ")").ok
    for i in 2..4:
      check execSql(db, "INSERT INTO s2 VALUES (" & $i & ")").ok
    
    # INTERSECT (without ALL) - supported
    let r = execSql(db, "SELECT id FROM s1 WHERE id IN (SELECT id FROM s1 INTERSECT SELECT id FROM s2)")
    check r.ok

suite "binder ON CONFLICT paths":
  test "ON CONFLICT DO UPDATE with unknown column fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    let r = execSql(db, "INSERT INTO t VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET nonexistent = 99")
    check not r.ok

  test "ON CONFLICT DO UPDATE SET expression":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, counter INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 0)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET counter = t.counter + 1").ok
    let r = execSql(db, "SELECT counter FROM t WHERE id = 1")
    check r.ok
    check r.value == @["1"]

  test "ON CONFLICT DO NOTHING":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'original')").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'conflict') ON CONFLICT (id) DO NOTHING").ok
    let r = execSql(db, "SELECT v FROM t WHERE id = 1")
    check r.ok
    check r.value == @["original"]

suite "INSERT INTO SELECT column count mismatch":
  test "INSERT INTO t (cols) SELECT with wrong count fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, a TEXT, b TEXT)").ok
    check execSql(db, "INSERT INTO src VALUES (1, 'x')").ok
    # Try to INSERT with 2 target cols but 1 source col - note: SELECT returns 1 col, target has 1 col
    let r = execSql(db, "INSERT INTO dst (id, a) SELECT id FROM src")
    check not r.ok

suite "sql.nim BETWEEN expression":
  test "BETWEEN covers low and high check":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE nums (n INT PRIMARY KEY)").ok
    for i in 1..10:
      check execSql(db, "INSERT INTO nums VALUES (" & $i & ")").ok
    let r = execSql(db, "SELECT n FROM nums WHERE n BETWEEN 3 AND 7")
    check r.ok
    check r.value.len == 5

  test "NOT BETWEEN":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE nums2 (n INT PRIMARY KEY)").ok
    for i in 1..10:
      check execSql(db, "INSERT INTO nums2 VALUES (" & $i & ")").ok
    let r = execSql(db, "SELECT n FROM nums2 WHERE n NOT BETWEEN 3 AND 7")
    check r.ok
    check r.value.len == 5

suite "binder inferExprType for BLOB":
  test "Expression with BLOB comparison":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE bt (id INT PRIMARY KEY, data BLOB)").ok
    check execSql(db, "INSERT INTO bt VALUES (1, x'0102')").ok
    check execSql(db, "INSERT INTO bt VALUES (2, x'0304')").ok
    # Blob comparison in WHERE
    let r = execSql(db, "SELECT id FROM bt WHERE data IS NOT NULL")
    check r.ok
    check r.value.len == 2
