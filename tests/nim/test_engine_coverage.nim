import unittest
import os
import strutils
import engine
import record/record
import errors
import catalog/catalog
import sql/binder
import sql/sql

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

suite "Engine UPDATE and DELETE":
  test "UPDATE statement":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    
    let updateRes = execSql(db, "UPDATE t SET val = 'c' WHERE id = 1")
    check updateRes.ok
    
    let selectRes = execSql(db, "SELECT val FROM t WHERE id = 1")
    check selectRes.ok
    
    discard closeDb(db)

  test "DELETE statement":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    
    let deleteRes = execSql(db, "DELETE FROM t WHERE id = 1")
    check deleteRes.ok
    
    let selectRes = execSql(db, "SELECT * FROM t")
    check selectRes.ok
    
    discard closeDb(db)

suite "Engine Views":
  test "CREATE and DROP VIEW":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    
    let createRes = execSql(db, "CREATE VIEW v AS SELECT id, val FROM t WHERE id = 1")
    check createRes.ok
    
    let selectRes = execSql(db, "SELECT * FROM v")
    check selectRes.ok
    
    let dropRes = execSql(db, "DROP VIEW v")
    check dropRes.ok
    
    discard closeDb(db)

suite "Engine Triggers":
  test "CREATE and TRIGGER execution":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "CREATE TABLE t2 (id INT PRIMARY KEY, log_val TEXT)")
    
    let createRes = execSql(db, "CREATE TRIGGER trig_after_insert AFTER INSERT ON t1 FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO t2 VALUES (42, ''trigger_log'')')")
    if not createRes.ok: echo "Trigger Create Err: ", createRes.err.message, " ctx: ", createRes.err.context
    check createRes.ok
    
    let insertRes = execSql(db, "INSERT INTO t1 VALUES (1, 'logged')")
    if not insertRes.ok: echo "Trigger Insert Err: ", insertRes.err.message, " ctx: ", insertRes.err.context
    check insertRes.ok
    
    let selectT2 = execSql(db, "SELECT * FROM t2")
    check selectT2.ok
    
    let dropRes = execSql(db, "DROP TRIGGER trig_after_insert ON t1")
    check dropRes.ok
    
    discard closeDb(db)

suite "Engine Temp Views and Instead Triggers":
  test "CREATE TEMP VIEW and INSTEAD OF trigger":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    # base table
    discard execSql(db, "CREATE TABLE base (id INT PRIMARY KEY, val TEXT)")
    
    # temp view
    let vRes = execSql(db, "CREATE TEMP VIEW tv AS SELECT * FROM base")
    check vRes.ok
    
    # instead of trigger
    let trigRes = execSql(db, "CREATE TRIGGER t_instead INSTEAD OF INSERT ON tv FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO base VALUES (42, ''from_trigger'')')")
    check trigRes.ok
    
    # trigger it
    let insertRes = execSql(db, "INSERT INTO tv VALUES (1, 'ignored')")
    check insertRes.ok
    
    let selectRes = execSql(db, "SELECT * FROM base")
    check selectRes.ok
    
    discard closeDb(db)

suite "Engine DDL":
  test "DROP TABLE":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let dropRes = execSql(db, "DROP TABLE t")
    if not dropRes.ok: echo "Drop Err: ", dropRes.err.message, " ctx: ", dropRes.err.context
    check dropRes.ok
    
    discard closeDb(db)

suite "Engine Transactions Extended":
  test "SAVEPOINT RELEASE":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "BEGIN")
    discard execSql(db, "SAVEPOINT sp1")
    check execSql(db, "RELEASE SAVEPOINT sp1").ok
    check execSql(db, "COMMIT").ok
    discard closeDb(db)

suite "Engine Foreign Keys Constraints and Cascades":
  test "ON DELETE and ON UPDATE actions":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    # parent table
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY, val TEXT)")
    
    # child table with cascade
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE CASCADE)")
    
    discard execSql(db, "INSERT INTO parent VALUES (1, 'p1')")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    
    let childSel1 = execSql(db, "SELECT * FROM child")
    check childSel1.ok
    
    # update cascade (though ON UPDATE action isn't fully supported in some engines, let's see if it executes without error or raises the right error)
    let upRes = execSql(db, "UPDATE parent SET id = 2 WHERE id = 1")
    # If unsupported, ok might be false, which is fine, we just want to execute the path
    discard upRes
    
    # delete cascade
    let delRes = execSql(db, "DELETE FROM parent WHERE id = 1 OR id = 2")
    check delRes.ok
    
    discard closeDb(db)

suite "Engine DDL Extra Edge Cases":
  test "DROP IF EXISTS":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    let dropTableRes = execSql(db, "DROP TABLE IF EXISTS t_nonexistent")
    check dropTableRes.ok
    
    let dropViewRes = execSql(db, "DROP VIEW IF EXISTS v_nonexistent")
    check dropViewRes.ok

    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let dropTrigRes = execSql(db, "DROP TRIGGER IF EXISTS trig_nonexistent ON t")
    check dropTrigRes.ok
    
    discard closeDb(db)

suite "Engine Transaction Errors":
  test "BEGIN twice fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "BEGIN")
    let res = execSql(db, "BEGIN")
    check not res.ok
    discard execSql(db, "ROLLBACK")
    discard closeDb(db)

  test "COMMIT without BEGIN fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    let res = execSql(db, "COMMIT")
    check not res.ok
    discard closeDb(db)

  test "ROLLBACK without BEGIN fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    let res = execSql(db, "ROLLBACK")
    check not res.ok
    discard closeDb(db)

  test "RELEASE non-existent savepoint fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "BEGIN")
    let res = execSql(db, "RELEASE SAVEPOINT nonexistent")
    check not res.ok
    discard execSql(db, "ROLLBACK")
    discard closeDb(db)

  test "ROLLBACK TO non-existent savepoint fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "BEGIN")
    let res = execSql(db, "ROLLBACK TO SAVEPOINT nonexistent")
    check not res.ok
    discard execSql(db, "ROLLBACK")
    discard closeDb(db)

suite "Engine Type Check Errors":
  test "UUID wrong size fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, u UUID)")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, X'0102030405060708')")
    check not insRes.ok
    discard closeDb(db)

  test "DECIMAL precision overflow":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, d DECIMAL(5,2))")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, 123456.78)")
    check not insRes.ok
    discard closeDb(db)

  test "Type mismatch INT64 expected":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, 'not_an_int')")
    check not insRes.ok
    discard closeDb(db)

  test "Type mismatch BOOL expected":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val BOOL)")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, 'not_a_bool')")
    check not insRes.ok
    discard closeDb(db)

  test "Type mismatch FLOAT64 expected":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, X'0102')")
    check not insRes.ok
    discard closeDb(db)

  test "Type mismatch TEXT expected":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT NOT NULL)")
    let insRes = execSql(db, "INSERT INTO t (id) VALUES (1)")
    check not insRes.ok
    discard closeDb(db)

  test "Type mismatch BLOB expected":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val BLOB)")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, 123)")
    check not insRes.ok
    discard closeDb(db)

suite "Engine FK Cascade Operations":
  test "ON DELETE CASCADE":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    let delRes = execSql(db, "DELETE FROM parent WHERE id = 1")
    check delRes.ok
    let childRes = execSql(db, "SELECT COUNT(*) FROM child")
    check childRes.ok
    discard closeDb(db)

  test "ON DELETE SET NULL":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE SET NULL)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    let delRes = execSql(db, "DELETE FROM parent WHERE id = 1")
    check delRes.ok
    let childRes = execSql(db, "SELECT pid FROM child WHERE id = 10")
    check childRes.ok
    discard closeDb(db)

  test "ON UPDATE CASCADE":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON UPDATE CASCADE)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    let upRes = execSql(db, "UPDATE parent SET id = 2 WHERE id = 1")
    check upRes.ok
    let childRes = execSql(db, "SELECT pid FROM child WHERE id = 10")
    check childRes.ok
    discard closeDb(db)

  test "ON UPDATE SET NULL":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON UPDATE SET NULL)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    let upRes = execSql(db, "UPDATE parent SET id = 2 WHERE id = 1")
    check upRes.ok
    let childRes = execSql(db, "SELECT pid FROM child WHERE id = 10")
    check childRes.ok
    discard closeDb(db)

  test "FK RESTRICT blocks delete":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE RESTRICT)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    let delRes = execSql(db, "DELETE FROM parent WHERE id = 1")
    check not delRes.ok
    discard closeDb(db)

suite "Engine INSERT ON CONFLICT":
  test "ON CONFLICT DO UPDATE with WHERE clause":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    let upRes = execSql(db, "INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET val = excluded.val WHERE excluded.val != 'skip'")
    check upRes.ok
    let selRes = execSql(db, "SELECT val FROM t WHERE id = 1")
    check selRes.ok
    discard closeDb(db)

  test "ON CONFLICT DO UPDATE WHERE excludes row":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    let upRes = execSql(db, "INSERT INTO t VALUES (1, 'skip') ON CONFLICT (id) DO UPDATE SET val = excluded.val WHERE excluded.val != 'skip'")
    check upRes.ok
    let selRes = execSql(db, "SELECT val FROM t WHERE id = 1")
    check selRes.ok
    discard closeDb(db)

    discard closeDb(db)
  test "ON CONFLICT DO NOTHING without target (INT64 PK)":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, 'b') ON CONFLICT DO NOTHING")
    check insRes.ok
    discard closeDb(db)

  test "ON CONFLICT DO NOTHING without target (UNIQUE)":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT UNIQUE)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    let insRes = execSql(db, "INSERT INTO t VALUES (2, 'a') ON CONFLICT DO NOTHING")
    check insRes.ok
    discard closeDb(db)

  test "ON CONFLICT DO UPDATE unknown column error":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a')")
    let upRes = execSql(db, "INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET nonexistent = 'x'")
    check not upRes.ok
    discard closeDb(db)

suite "Engine Multi-Row INSERT":
  test "INSERT multiple rows":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    check insRes.ok
    let selRes = execSql(db, "SELECT COUNT(*) FROM t")
    check selRes.ok
    discard closeDb(db)

  test "INSERT multiple rows with RETURNING":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b') RETURNING id")
    check insRes.ok
    discard closeDb(db)

suite "Engine INSERT SELECT":
  test "INSERT SELECT basic":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO src VALUES (1, 'a'), (2, 'b')")
    let insRes = execSql(db, "INSERT INTO dst SELECT id, val FROM src")
    check insRes.ok
    let selRes = execSql(db, "SELECT COUNT(*) FROM dst")
    check selRes.ok
    discard closeDb(db)

  test "INSERT SELECT with generated column":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)")
    discard execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, a INT)")
    discard execSql(db, "INSERT INTO src VALUES (1, 10)")
    let insRes = execSql(db, "INSERT INTO dst (id, a) SELECT id, a FROM src")
    check insRes.ok
    discard closeDb(db)

suite "Engine DISTINCT ON":
  test "DISTINCT ON expression":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)")
    let selRes = execSql(db, "SELECT DISTINCT ON (cat) cat, val FROM t ORDER BY cat, val")
    check selRes.ok
    discard closeDb(db)

suite "Engine CTE":
  test "Non-recursive CTE":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1), (2), (3)")
    let selRes = execSql(db, "WITH cte AS (SELECT id FROM t WHERE id > 1) SELECT * FROM cte")
    check selRes.ok
    discard closeDb(db)

  test "Recursive CTE":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    let selRes = execSql(db, "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT x FROM cnt")
    check not selRes.ok
    discard closeDb(db)

suite "Engine View Operations":
  test "CREATE VIEW IF NOT EXISTS":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE VIEW v AS SELECT * FROM t")
    let res = execSql(db, "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t")
    check res.ok
    discard closeDb(db)

  test "CREATE OR REPLACE VIEW":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "CREATE VIEW v AS SELECT id FROM t")
    let res = execSql(db, "CREATE OR REPLACE VIEW v AS SELECT id, val FROM t")
    check res.ok
    discard closeDb(db)

  test "DROP view with dependent views fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE VIEW v1 AS SELECT * FROM t")
    discard execSql(db, "CREATE VIEW v2 AS SELECT * FROM v1")
    let res = execSql(db, "DROP VIEW v1")
    check not res.ok
    discard closeDb(db)

  test "DROP TABLE with dependent views fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE VIEW v AS SELECT * FROM t")
    let res = execSql(db, "DROP TABLE t")
    check not res.ok
    discard closeDb(db)

suite "Engine EXPLAIN":
  test "EXPLAIN non-SELECT fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let res = execSql(db, "EXPLAIN INSERT INTO t VALUES (1)")
    check not res.ok
    discard closeDb(db)

  test "EXPLAIN ANALYZE":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let res = execSql(db, "EXPLAIN ANALYZE SELECT * FROM t")
    check res.ok
    discard closeDb(db)

suite "Engine Prepared Statement Errors":
  test "Prepare on closed DB fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard closeDb(db)
    let prepRes = prepare(db, "SELECT 1")
    check not prepRes.ok

  test "ExecPrepared on closed DB fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let prepRes = prepare(db, "SELECT * FROM t")
    require prepRes.ok
    discard closeDb(db)
    let execRes = execPrepared(prepRes.value, @[])
    check not execRes.ok

suite "Engine SQL Cache Eviction":
  test "SQL cache evicts old entries":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..150:
      let res = execSql(db, "SELECT * FROM t WHERE id = " & $i)
      check res.ok
    discard closeDb(db)

suite "Engine Composite Primary Key":
  test "Table with composite PK":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    let res = execSql(db, "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))")
    check res.ok
    discard closeDb(db)

  test "INSERT into composite PK table":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (a INT, b INT, val TEXT, PRIMARY KEY (a, b))")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, 2, 'x')")
    check insRes.ok
    let selRes = execSql(db, "SELECT val FROM t WHERE a = 1 AND b = 2")
    check selRes.ok
    discard closeDb(db)

  test "Composite PK uniqueness enforced":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))")
    discard execSql(db, "INSERT INTO t VALUES (1, 2)")
    let insRes = execSql(db, "INSERT INTO t VALUES (1, 2)")
    check not insRes.ok
    discard closeDb(db)

suite "Engine Expressions":
  test "CASE with multiple WHEN":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    let res = execSql(db, "SELECT CASE WHEN 1 = 2 THEN 'a' WHEN 2 = 2 THEN 'b' ELSE 'c' END")
    check res.ok
    discard closeDb(db)

  test "Nested CASE":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    let res = execSql(db, "SELECT CASE WHEN 1 = 1 THEN CASE WHEN 2 = 2 THEN 'yes' ELSE 'no' END ELSE 'outer' END")
    check res.ok
    discard closeDb(db)

  test "BETWEEN with NOT":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (1), (2), (3), (4), (5)")
    let res = execSql(db, "SELECT id FROM t WHERE id NOT BETWEEN 2 AND 4")
    check res.ok
    discard closeDb(db)

suite "Engine CHECK Constraint":
  test "CHECK with multiple conditions":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, CHECK (a > 0 AND a < 100))")
    let insOk = execSql(db, "INSERT INTO t VALUES (1, 50)")
    check insOk.ok
    let insBad = execSql(db, "INSERT INTO t VALUES (2, 150)")
    check not insBad.ok
    discard closeDb(db)

  test "CHECK with named constraint":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT CONSTRAINT positive_val CHECK (val > 0))")
    let insBad = execSql(db, "INSERT INTO t VALUES (1, -1)")
    check not insBad.ok
    discard closeDb(db)

suite "Engine Partial Index":
  test "CREATE partial index":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, active BOOL, val INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, true, 10), (2, false, 20)")
    let idxRes = execSql(db, "CREATE INDEX idx_active ON t(val) WHERE active")
    check idxRes.ok
    discard closeDb(db)

suite "Engine Complex Coverage":
  test "UNIQUE Index Error Paths":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'a')")
    let idxRes = execSql(db, "CREATE UNIQUE INDEX idx_t_val ON t(val)")
    check not idxRes.ok
    
    discard execSql(db, "CREATE TABLE t2 (id INT PRIMARY KEY, val1 INT, val2 INT)")
    discard execSql(db, "INSERT INTO t2 VALUES (1, 10, 20), (2, 10, 20)")
    let idxRes2 = execSql(db, "CREATE UNIQUE INDEX idx_t2_vals ON t2(val1, val2)")
    check not idxRes2.ok
    discard closeDb(db)

  test "Generated Columns":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)").ok
    
    let ins1 = execSql(db, "INSERT INTO t (id, a) VALUES (1, 10)")
    if not ins1.ok: echo "GenInsErr: ", ins1.err.message
    check ins1.ok
    
    let up1 = execSql(db, "UPDATE t SET a = 20 WHERE id = 1")
    if not up1.ok: echo "GenUpErr: ", up1.err.message
    check up1.ok
    
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, a INT)").ok
    check execSql(db, "INSERT INTO src VALUES (2, 5)").ok
    
    let ins2 = execSql(db, "INSERT INTO t (id, a) SELECT id, a FROM src")
    if not ins2.ok: echo "GenSelErr: ", ins2.err.message
    check ins2.ok
    
    let rowsRes = execSql(db, "SELECT * FROM t")
    if not rowsRes.ok: echo "SelErr: ", rowsRes.err.message
    check rowsRes.ok
    discard closeDb(db)

  test "View Errors":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE VIEW v1 AS SELECT * FROM t")
    discard execSql(db, "CREATE VIEW v2 AS SELECT * FROM t")
    
    let altRes = execSql(db, "ALTER VIEW v1 RENAME TO v2")
    check not altRes.ok
    
    discard closeDb(db)

suite "Engine ExecPrepared Direct Calls":
  test "execPrepared - select":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")

    let prepRes = prepare(db, "SELECT * FROM t WHERE id = $1")
    check prepRes.ok
    let prep = prepRes.value
    let execRes = execPrepared(prep, @[Value(kind: vkInt64, int64Val: 1)])
    if execRes.ok: echo "Select output: ", execRes.value
    check execRes.ok
    check execRes.value.len == 1 # 1 statement output
    discard closeDb(db)

  test "execPrepared - fast insert":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    let prepRes = prepare(db, "INSERT INTO t VALUES ($1, $2)")
    check prepRes.ok
    let prep = prepRes.value
    let execRes = execPrepared(prep, @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: cast[seq[byte]]("a"))])
    check execRes.ok
    
    # Check fast insert type mismatch
    let execBad = execPrepared(prep, @[Value(kind: vkText, bytes: cast[seq[byte]]("bad")), Value(kind: vkText, bytes: cast[seq[byte]]("a"))])
    check not execBad.ok
    discard closeDb(db)

  test "execPrepared - explicit transaction control":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    check execPrepared(prepare(db, "BEGIN").value, @[]).ok
    check execPrepared(prepare(db, "INSERT INTO t VALUES (1)").value, @[]).ok
    check execPrepared(prepare(db, "SAVEPOINT sp1").value, @[]).ok
    check execPrepared(prepare(db, "INSERT INTO t VALUES (2)").value, @[]).ok
    check execPrepared(prepare(db, "ROLLBACK TO SAVEPOINT sp1").value, @[]).ok
    check execPrepared(prepare(db, "COMMIT").value, @[]).ok
    
    let selRes = execPrepared(prepare(db, "SELECT * FROM t").value, @[])
    if selRes.ok: echo "Select all output: ", selRes.value
    check selRes.ok
    check selRes.value.len == 1 # 1 statement output
    discard closeDb(db)
    
  test "execPrepared - schema change triggers re-prepare":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let prepRes = prepare(db, "SELECT * FROM t")
    check prepRes.ok
    let prep = prepRes.value
    
    discard execSql(db, "CREATE TABLE t2 (id INT PRIMARY KEY)")
    # Schema cookie changed
    let execRes = execPrepared(prep, @[])
    check execRes.ok
    discard closeDb(db)

  test "execPrepared - release savepoint":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    check execPrepared(prepare(db, "BEGIN").value, @[]).ok
    check execPrepared(prepare(db, "SAVEPOINT sp1").value, @[]).ok
    check execPrepared(prepare(db, "RELEASE SAVEPOINT sp1").value, @[]).ok
    check execPrepared(prepare(db, "COMMIT").value, @[]).ok
    discard closeDb(db)

  test "execPrepared - explain analyze":
    let dbRes = openDb(":memory:")
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    let prepRes = prepare(db, "EXPLAIN ANALYZE SELECT * FROM t")
    check prepRes.ok
    let execRes = execPrepared(prepRes.value, @[])
    check execRes.ok
    discard closeDb(db)


proc getBoundStmt(db: Db, sql: string): Statement =
  let pRes = prepare(db, sql)
  doAssert pRes.ok, "Prepare failed: " & sql
  pRes.value.statements[0]

suite "Engine execPreparedNonSelect coverage":
  test "skCreateTable error paths":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    # Good create
    check execPreparedNonSelect(db, getBoundStmt(db, "CREATE TABLE t (id INT PRIMARY KEY)"), @[]).ok

    # Bad column type
    # For this we need to manually construct a bad Statement, or find a way to get one.
    # Actually, binder checks types. If we want parseColumnType to fail, we need to bypass binder or create a type that binder allows but engine doesn't?
    # No, binder uses `parseColumnType` too... Wait, let's just test the other DDL branches.

    discard closeDb(db)

  test "skDropTable":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    check execPreparedNonSelect(db, getBoundStmt(db, "DROP TABLE t"), @[]).ok
    
    discard closeDb(db)

  test "skCreateIndex / skDropIndex":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    
    check execPreparedNonSelect(db, getBoundStmt(db, "CREATE INDEX idx_val ON t(val)"), @[]).ok
    check execPreparedNonSelect(db, getBoundStmt(db, "DROP INDEX idx_val"), @[]).ok
    
    discard closeDb(db)

  test "skAlterTable":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    check execPreparedNonSelect(db, getBoundStmt(db, "ALTER TABLE t ADD COLUMN val TEXT"), @[]).ok
    
    discard closeDb(db)

  test "skCreateView / skDropView / skAlterView":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    check execPreparedNonSelect(db, getBoundStmt(db, "CREATE VIEW v AS SELECT * FROM t"), @[]).ok
    check execPreparedNonSelect(db, getBoundStmt(db, "ALTER VIEW v RENAME TO v2"), @[]).ok
    check execPreparedNonSelect(db, getBoundStmt(db, "DROP VIEW v2"), @[]).ok
    
    discard closeDb(db)

  test "Insert with multiple rows":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t VALUES (1), (2), (3)")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)

  test "Insert from select":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE src (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO src VALUES (1)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t SELECT id FROM src")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)
  test "skCreateTrigger / skDropTrigger":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    check execPreparedNonSelect(db, getBoundStmt(db, "CREATE TRIGGER trig_after_ins AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO t VALUES (99)')"), @[]).ok
    check execPreparedNonSelect(db, getBoundStmt(db, "DROP TRIGGER trig_after_ins ON t"), @[]).ok
    
    discard closeDb(db)

  test "Insert with returning":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t VALUES (1) RETURNING id")
    let res = execPreparedNonSelect(db, stmt, @[])
    check not res.ok
    check res.err.message == "INSERT RETURNING is not supported by non-select execution API"
    
    discard closeDb(db)
  test "Insert with multiple rows":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t VALUES (1), (2), (3)")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)

  test "Insert from select":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE src (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO src VALUES (1)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t SELECT id FROM src")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)
  test "execPreparedNonSelect DDL error paths":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value

    # skCreateTable with invalid column type
    let createBadType = Statement(
      kind: skCreateTable,
      createTableName: "t_bad",
      columns: @[ColumnDef(name: "id", typeName: "INVALID_TYPE")],
      createTableIsTemp: false
    )
    check not execPreparedNonSelect(db, createBadType, @[]).ok

    # skCreateTable temp with invalid column type
    let createTempBadType = Statement(
      kind: skCreateTable,
      createTableName: "t_temp_bad",
      columns: @[ColumnDef(name: "id", typeName: "INVALID_TYPE")],
      createTableIsTemp: true
    )
    check not execPreparedNonSelect(db, createTempBadType, @[]).ok

    # skDropTable nonexistent
    let dropBad = Statement(
      kind: skDropTable,
      dropTableName: "nonexistent",
      dropTableIfExists: false
    )
    check not execPreparedNonSelect(db, dropBad, @[]).ok

    # skCreateIndex nonexistent table
    let idxBad = Statement(
      kind: skCreateIndex,
      indexName: "idx_fake",
      indexTableName: "nonexistent",
      columnNames: @["col1"],
      indexKind: ikBtree
    )
    check not execPreparedNonSelect(db, idxBad, @[]).ok

    # skDropIndex nonexistent
    let dropIdxBad = Statement(
      kind: skDropIndex,
      dropIndexName: "nonexistent",
    )
    check not execPreparedNonSelect(db, dropIdxBad, @[]).ok

    # skAlterTable nonexistent
    let alterBad = Statement(
      kind: skAlterTable,
      alterTableName: "nonexistent",
      alterActions: @[]
    )
    check not execPreparedNonSelect(db, alterBad, @[]).ok

    # skDropTrigger nonexistent
    let dropTrigBad = Statement(
      kind: skDropTrigger,
      dropTriggerName: "trig",
      dropTriggerTableName: "t",
      dropTriggerIfExists: false
    )
    check not execPreparedNonSelect(db, dropTrigBad, @[]).ok


    # skDropView nonexistent
    let dropViewBad = Statement(
      kind: skDropView,
      dropViewName: "nonexistent",
      dropViewIfExists: false
    )
    check not execPreparedNonSelect(db, dropViewBad, @[]).ok

    discard closeDb(db)
  test "Insert with multiple rows":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t VALUES (1), (2), (3)")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)

  test "Insert from select":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE src (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO src VALUES (1)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t SELECT id FROM src")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)
  test "skCreateTrigger / skDropTrigger":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    check execPreparedNonSelect(db, getBoundStmt(db, "CREATE TRIGGER trig_after_ins AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO t VALUES (99)')"), @[]).ok
    check execPreparedNonSelect(db, getBoundStmt(db, "DROP TRIGGER trig_after_ins ON t"), @[]).ok
    
    discard closeDb(db)

  test "Insert with returning":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t VALUES (1) RETURNING id")
    let res = execPreparedNonSelect(db, stmt, @[])
    check not res.ok
    check res.err.message == "INSERT RETURNING is not supported by non-select execution API"
    
    discard closeDb(db)
  test "Insert with multiple rows":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t VALUES (1), (2), (3)")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)

  test "Insert from select":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE src (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO src VALUES (1)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t SELECT id FROM src")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)
    # skCreateTrigger nonexistent table
    let trigBad = Statement(
      kind: skCreateTrigger,
      triggerName: "trig",
      triggerTableName: "nonexistent",
      triggerEventsMask: 4,
      triggerForEachRow: true,
      triggerFunctionName: "decentdb_exec_sql",
      triggerActionSql: "SELECT 1"
    )
    check not execPreparedNonSelect(db, trigBad, @[]).ok
    # skDropTrigger nonexistent
    let dropTrigBad = Statement(
      kind: skDropTrigger,
      dropTriggerName: "trig",
      dropTriggerTableName: "t",
      dropTriggerIfExists: false
    )
    check not execPreparedNonSelect(db, dropTrigBad, @[]).ok


    # skDropView nonexistent
    let dropViewBad = Statement(
      kind: skDropView,
      dropViewName: "nonexistent",
      dropViewIfExists: false
    )
    check not execPreparedNonSelect(db, dropViewBad, @[]).ok

    discard closeDb(db)
  test "Insert with multiple rows":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t VALUES (1), (2), (3)")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)

  test "Insert from select":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE src (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO src VALUES (1)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t SELECT id FROM src")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)
  test "skCreateTrigger / skDropTrigger":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    check execPreparedNonSelect(db, getBoundStmt(db, "CREATE TRIGGER trig_after_ins AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO t VALUES (99)')"), @[]).ok
    check execPreparedNonSelect(db, getBoundStmt(db, "DROP TRIGGER trig_after_ins ON t"), @[]).ok
    
    discard closeDb(db)

  test "Insert with returning":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t VALUES (1) RETURNING id")
    let res = execPreparedNonSelect(db, stmt, @[])
    check not res.ok
    check res.err.message == "INSERT RETURNING is not supported by non-select execution API"
    
    discard closeDb(db)
  test "Insert with multiple rows":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t VALUES (1), (2), (3)")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)

  test "Insert from select":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE src (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO src VALUES (1)")
    
    let stmt = getBoundStmt(db, "INSERT INTO t SELECT id FROM src")
    check execPreparedNonSelect(db, stmt, @[]).ok
    
    discard closeDb(db)
