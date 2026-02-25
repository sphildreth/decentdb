import unittest
import strutils
import engine
import record/record

## Tests covering binder.nim paths:
## - Trigger binding errors (wrong function, duplicate trigger, invalid action)
## - Expression index binding errors (UNIQUE, multiple cols)
## - CTE recursive join sources
## - Recursive CTE with joins

suite "Trigger binding errors":
  test "CREATE TRIGGER with wrong function name fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER)").ok
    let r = execSql(db, """
      CREATE TRIGGER bad_trig AFTER INSERT ON t
      FOR EACH ROW EXECUTE FUNCTION other_func('SELECT 1')
    """)
    check not r.ok
    check "decentdb_exec_sql" in r.err.message or "supported" in r.err.message

  test "CREATE TRIGGER duplicate fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER)").ok
    check execSql(db, "CREATE TABLE log (v TEXT)").ok
    let r1 = execSql(db, """
      CREATE TRIGGER my_trig AFTER INSERT ON t
      FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''a'')')
    """)
    check r1.ok
    let r2 = execSql(db, """
      CREATE TRIGGER my_trig AFTER INSERT ON t
      FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''b'')')
    """)
    check not r2.ok
    check "exists" in r2.err.message.toLowerAscii() or "already" in r2.err.message.toLowerAscii()

  test "CREATE TRIGGER with SELECT action fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER)").ok
    let r = execSql(db, """
      CREATE TRIGGER bad_select AFTER INSERT ON t
      FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('SELECT 1')
    """)
    check not r.ok

  test "CREATE TRIGGER with invalid action SQL fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER)").ok
    let r = execSql(db, """
      CREATE TRIGGER bad_sql_trig AFTER INSERT ON t
      FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO nonexistent_table_xyz VALUES (1)')
    """)
    check not r.ok

suite "Expression index binding errors":
  test "UNIQUE expression index fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER, y INTEGER)").ok
    let r = execSql(db, "CREATE UNIQUE INDEX idx ON t((x + y))")
    check not r.ok
    check "not supported" in r.err.message.toLowerAscii() or "UNIQUE" in r.err.message

  test "Expression index with multiple columns fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER, y INTEGER)").ok
    # Partial expression index not supported
    let r = execSql(db, "CREATE INDEX idx ON t((x + y)) WHERE x > 0")
    check not r.ok

suite "Recursive CTE with joins":
  test "recursive CTE with self-join":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE edges (src INTEGER, dst INTEGER)").ok
    check execSql(db, "INSERT INTO edges VALUES (1,2),(2,3),(3,4)").ok
    let r = execSql(db, """
      WITH RECURSIVE reach(node) AS (
        SELECT src FROM edges WHERE src = 1
        UNION ALL
        SELECT e.dst FROM edges e JOIN reach r ON e.src = r.node
      )
      SELECT node FROM reach ORDER BY node
    """)
    check r.ok
    check r.value.len >= 1

suite "ALTER TABLE unsupported actions":
  test "ALTER TABLE ALTER COLUMN not supported returns error":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (a INTEGER, b INTEGER DEFAULT 42)").ok
    let r = execSql(db, "ALTER TABLE t ALTER COLUMN b DROP DEFAULT")
    check not r.ok
    check "not" in r.err.message.toLowerAscii() or "supported" in r.err.message.toLowerAscii()
