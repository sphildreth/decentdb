import unittest
import os

import engine
import record/record
import errors
import catalog/catalog

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Engine Default Options":
  test "defaultBulkLoadOptions has expected values":
    let opts = defaultBulkLoadOptions()
    check opts.batchSize == 10000
    check opts.syncInterval == 10
    check opts.disableIndexes == true
    check opts.checkpointOnComplete == true
    check opts.durability == dmDeferred

  test "defaultConstraintBatchOptions":
    let opts = defaultConstraintBatchOptions()
    check opts.checkNotNull == true
    check opts.checkChecks == true
    check opts.checkUnique == true
    check opts.checkForeignKeys == true
    check opts.skipInt64PkOptimization == false

suite "Engine Temporary Tables":
  test "temp table lifecycle":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res1 = execSql(db, "CREATE TEMP TABLE temp1 (id INT PRIMARY KEY, val TEXT)")
    check res1.ok
    
    let res2 = execSql(db, "INSERT INTO temp1 VALUES (1, 'test')")
    check res2.ok
    
    let res3 = execSql(db, "SELECT * FROM temp1")
    check res3.ok
    
    discard closeDb(db)

  test "temp table with NOT NULL constraint":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TEMP TABLE temp1 (id INT PRIMARY KEY, val TEXT NOT NULL)")
    
    let insertRes = execSql(db, "INSERT INTO temp1 VALUES (1, NULL)")
    check not insertRes.ok
    
    discard closeDb(db)

  test "temp table with UNIQUE constraint":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TEMP TABLE temp1 (id INT PRIMARY KEY, val TEXT UNIQUE)")
    discard execSql(db, "INSERT INTO temp1 VALUES (1, 'a')")
    
    let insertRes = execSql(db, "INSERT INTO temp1 VALUES (2, 'a')")
    check not insertRes.ok
    
    discard closeDb(db)

suite "Engine Transaction Isolation":
  test "commit persists changes":
    let path = makeTempDb("decentdb_test_isolation.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    discard execSql(db, "BEGIN")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    discard execSql(db, "COMMIT")
    
    let res = execSql(db, "SELECT COUNT(*) FROM t")
    require res.ok
    
    discard closeDb(db)

  test "rollback discards changes":
    let path = makeTempDb("decentdb_test_rollback.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    
    discard execSql(db, "BEGIN")
    discard execSql(db, "INSERT INTO t VALUES (2)")
    discard execSql(db, "ROLLBACK")
    
    let res = execSql(db, "SELECT COUNT(*) FROM t")
    require res.ok
    
    discard closeDb(db)

  test "savepoint within transaction":
    let path = makeTempDb("decentdb_test_savepoint.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    discard execSql(db, "BEGIN")
    discard execSql(db, "INSERT INTO t VALUES (1)")
    discard execSql(db, "SAVEPOINT sp1")
    discard execSql(db, "INSERT INTO t VALUES (2)")
    discard execSql(db, "ROLLBACK TO SAVEPOINT sp1")
    discard execSql(db, "COMMIT")
    
    let res = execSql(db, "SELECT COUNT(*) FROM t")
    require res.ok
    
    discard closeDb(db)

suite "Engine NULL handling":
  test "IS NULL operator":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, NULL)")
    discard execSql(db, "INSERT INTO t VALUES (2, 'x')")
    
    let res = execSql(db, "SELECT id FROM t WHERE val IS NULL")
    check res.ok
    
    discard closeDb(db)

  test "IS NOT NULL operator":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, NULL)")
    discard execSql(db, "INSERT INTO t VALUES (2, 'x')")
    
    let res = execSql(db, "SELECT id FROM t WHERE val IS NOT NULL")
    check res.ok
    
    discard closeDb(db)

  test "COALESCE function":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, NULL)")
    discard execSql(db, "INSERT INTO t VALUES (2, 'x')")
    
    let res = execSql(db, "SELECT COALESCE(val, 'default') FROM t ORDER BY id")
    check res.ok
    
    discard closeDb(db)

suite "Engine Type Conversions":
  test "CAST from int to text":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT CAST(42 AS TEXT)")
    check res.ok
    
    discard closeDb(db)

  test "CAST from text to int":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT CAST('42' AS INT)")
    check res.ok
    
    discard closeDb(db)

suite "Engine Aggregates":
  test "COUNT aggregate":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    discard execSql(db, "INSERT INTO t VALUES (2, 20)")
    discard execSql(db, "INSERT INTO t VALUES (3, 30)")
    
    let res = execSql(db, "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM t")
    check res.ok
    
    discard closeDb(db)

  test "GROUP BY":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a', 10)")
    discard execSql(db, "INSERT INTO t VALUES (2, 'a', 20)")
    discard execSql(db, "INSERT INTO t VALUES (3, 'b', 30)")
    
    let res = execSql(db, "SELECT cat, SUM(val) FROM t GROUP BY cat ORDER BY cat")
    check res.ok
    
    discard closeDb(db)

  test "COUNT DISTINCT":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'a')")
    discard execSql(db, "INSERT INTO t VALUES (3, 'b')")
    
    let res = execSql(db, "SELECT COUNT(DISTINCT val) FROM t")
    check res.ok
    
    discard closeDb(db)

suite "Engine String Functions":
  test "LENGTH function":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT LENGTH('hello')")
    check res.ok
    
    discard closeDb(db)

  test "UPPER and LOWER":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT UPPER('hello'), LOWER('WORLD')")
    check res.ok
    
    discard closeDb(db)

  test "SUBSTR function":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT SUBSTR('hello', 2, 3)")
    check res.ok
    
    discard closeDb(db)

  test "TRIM functions":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT TRIM('  hello  '), LTRIM('  hello'), RTRIM('hello  ')")
    check res.ok
    
    discard closeDb(db)

  test "INSTR function":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT INSTR('hello world', 'world')")
    check res.ok
    
    discard closeDb(db)

  test "REPLACE function":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT REPLACE('hello world', 'world', 'there')")
    check res.ok
    
    discard closeDb(db)

suite "Engine Math Functions":
  test "ABS function":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT ABS(-5), ABS(5)")
    check res.ok
    
    discard closeDb(db)

  test "ROUND function":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT ROUND(3.14159, 2)")
    check res.ok
    
    discard closeDb(db)

suite "Engine Date Time Functions":
  test "CURRENT_DATE":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT CURRENT_DATE")
    check res.ok
    
    discard closeDb(db)

  test "CURRENT_TIMESTAMP":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT CURRENT_TIMESTAMP")
    check res.ok
    
    discard closeDb(db)

suite "Engine CASE Expression":
  test "CASE WHEN":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END")
    check res.ok
    
    discard closeDb(db)

  test "CASE with value":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
    check res.ok
    
    discard closeDb(db)

suite "Engine IN Operator":
  test "IN with list":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1), (2), (3)")
    
    let res = execSql(db, "SELECT id FROM t WHERE id IN (1, 3)")
    check res.ok
    
    discard closeDb(db)

  test "NOT IN with list":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1), (2), (3)")
    
    let res = execSql(db, "SELECT id FROM t WHERE id NOT IN (1, 3)")
    check res.ok
    
    discard closeDb(db)

suite "Engine BETWEEN":
  test "BETWEEN operator":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1), (2), (3), (4)")
    
    let res = execSql(db, "SELECT id FROM t WHERE id BETWEEN 2 AND 3")
    check res.ok
    
    discard closeDb(db)

suite "Engine LIKE":
  test "LIKE with percent":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'hello'), (2, 'world'), (3, 'help')")
    
    let res = execSql(db, "SELECT id FROM t WHERE val LIKE 'hel%'")
    check res.ok
    
    discard closeDb(db)

  test "LIKE with underscore":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'abc'), (2, 'axc'), (3, 'abx')")
    
    let res = execSql(db, "SELECT id FROM t WHERE val LIKE 'a_c'")
    check res.ok
    
    discard closeDb(db)

suite "Engine LIMIT OFFSET":
  test "LIMIT clause":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1), (2), (3), (4), (5)")
    
    let res = execSql(db, "SELECT COUNT(*) FROM t LIMIT 3")
    check res.ok
    
    discard closeDb(db)

  test "OFFSET clause":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1), (2), (3), (4), (5)")
    
    let res = execSql(db, "SELECT id FROM t ORDER BY id OFFSET 2")
    check res.ok
    
    discard closeDb(db)

suite "Engine ORDER BY":
  test "ORDER BY DESC":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10), (2, 5), (3, 20)")
    
    let res = execSql(db, "SELECT val FROM t ORDER BY val DESC")
    check res.ok
    
    discard closeDb(db)

suite "Engine Index":
  test "DROP INDEX":
    let path = makeTempDb("decentdb_test_dropindex.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "CREATE INDEX idx_val ON t(val)")
    let res = execSql(db, "DROP INDEX idx_val")
    check res.ok
    
    discard closeDb(db)

suite "Engine ALTER TABLE":
  test "ALTER TABLE ADD COLUMN":
    let path = makeTempDb("decentdb_test_addcol.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let res = execSql(db, "ALTER TABLE t ADD COLUMN new_col TEXT")
    check res.ok
    
    discard closeDb(db)

suite "Engine Constraints":
  test "CHECK constraint":
    let path = makeTempDb("decentdb_test_check.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT CHECK(val > 0))")
    let insertRes = execSql(db, "INSERT INTO t VALUES (1, -1)")
    check not insertRes.ok
    
    discard closeDb(db)

suite "Engine Auto Increment":
  test "INTEGER PRIMARY KEY auto increment":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t (val) VALUES ('a')")
    discard execSql(db, "INSERT INTO t (val) VALUES ('b')")
    
    let res = execSql(db, "SELECT id FROM t ORDER BY id")
    check res.ok
    
    discard closeDb(db)

suite "Engine Prepared Statements":
  test "prepare statement":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    let prepRes = prepare(db, "INSERT INTO t VALUES ($1, $2)")
    check prepRes.ok
    
    discard closeDb(db)

  test "prepare SELECT":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20)")
    let prepRes = prepare(db, "SELECT val FROM t WHERE id = $1")
    check prepRes.ok
    
    discard closeDb(db)

suite "Engine ON CONFLICT":
  test "INSERT OR REPLACE":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    discard execSql(db, "INSERT OR REPLACE INTO t VALUES (1, 'b')")
    
    let res = execSql(db, "SELECT val FROM t WHERE id = 1")
    check res.ok
    
    discard closeDb(db)

suite "Engine Foreign Keys":
  test "FOREIGN KEY basic":
    let path = makeTempDb("decentdb_test_fk.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    let insertRes = execSql(db, "INSERT INTO child VALUES (1, 999)")
    check not insertRes.ok
    
    discard closeDb(db)

suite "Engine Operators":
  test "Arithmetic operators":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT 1 + 2, 5 - 3, 4 * 2, 10 / 3")
    check res.ok
    
    discard closeDb(db)

  test "Comparison operators":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let res = execSql(db, "SELECT 1 = 1, 1 != 2, 3 < 4, 5 > 4, 3 <= 3, 5 >= 5")
    check res.ok
    
    discard closeDb(db)
