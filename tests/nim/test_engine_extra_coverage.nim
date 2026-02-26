import unittest
import strutils
import engine
import record/record

## Tests covering engine.nim paths that were identified as uncovered:
## INSERT INTO...SELECT, SAVEPOINT not found, CREATE UNIQUE INDEX with data,
## various engine.nim statement handling paths.

suite "Engine INSERT INTO SELECT":
  test "INSERT INTO SELECT basic":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE src (a INTEGER, b TEXT)").ok
    check execSql(db, "INSERT INTO src VALUES (1, 'x'), (2, 'y'), (3, 'z')").ok
    check execSql(db, "CREATE TABLE dst (a INTEGER, b TEXT)").ok
    let r = execSql(db, "INSERT INTO dst SELECT a, b FROM src")
    check r.ok
    let rows = execSql(db, "SELECT a, b FROM dst ORDER BY a").value
    check rows.len == 3
    check rows[0] == "1|x"
    check rows[2] == "3|z"

  test "INSERT INTO SELECT with column mapping":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE src (a INTEGER, b TEXT)").ok
    check execSql(db, "INSERT INTO src VALUES (10, 'hello')").ok
    check execSql(db, "CREATE TABLE dst (b TEXT, a INTEGER)").ok
    let r = execSql(db, "INSERT INTO dst (a, b) SELECT a, b FROM src")
    check r.ok
    let rows = execSql(db, "SELECT a, b FROM dst").value
    check rows.len == 1
    check rows[0] == "10|hello"

  test "INSERT INTO SELECT with WHERE filter":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE src (x INTEGER)").ok
    check execSql(db, "INSERT INTO src VALUES (1),(2),(3),(4),(5)").ok
    check execSql(db, "CREATE TABLE dst (x INTEGER)").ok
    let r = execSql(db, "INSERT INTO dst SELECT x FROM src WHERE x > 3")
    check r.ok
    let rows = execSql(db, "SELECT x FROM dst ORDER BY x").value
    check rows.len == 2
    check rows[0] == "4"
    check rows[1] == "5"

  test "INSERT INTO SELECT empty source":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE src (x INTEGER)").ok
    check execSql(db, "CREATE TABLE dst (x INTEGER)").ok
    let r = execSql(db, "INSERT INTO dst SELECT x FROM src")
    check r.ok
    let rows = execSql(db, "SELECT COUNT(*) FROM dst").value
    check rows[0] == "0"

suite "Engine SAVEPOINT not found":
  test "RELEASE SAVEPOINT on nonexistent savepoint fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, "RELEASE SAVEPOINT nosuchsp")
    check not r.ok
    check "SAVEPOINT" in r.err.message or "not found" in r.err.message.toLowerAscii()

  test "ROLLBACK TO SAVEPOINT on nonexistent fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, "ROLLBACK TO SAVEPOINT nosuchsp")
    check not r.ok
    check "SAVEPOINT" in r.err.message or "not found" in r.err.message.toLowerAscii()

  test "RELEASE valid then nonexistent SAVEPOINT":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "SAVEPOINT sp1").ok
    check execSql(db, "RELEASE SAVEPOINT sp1").ok
    let r = execSql(db, "RELEASE SAVEPOINT sp1")
    check not r.ok  # Already released

  test "SAVEPOINT create and rollback to it":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER)").ok
    check execSql(db, "BEGIN").ok
    check execSql(db, "INSERT INTO t VALUES (1)").ok
    check execSql(db, "SAVEPOINT sp1").ok
    check execSql(db, "INSERT INTO t VALUES (2)").ok
    check execSql(db, "ROLLBACK TO SAVEPOINT sp1").ok
    check execSql(db, "COMMIT").ok
    let rows = execSql(db, "SELECT COUNT(*) FROM t").value
    check rows[0] == "1"

suite "Engine CREATE UNIQUE INDEX with existing data":
  test "CREATE UNIQUE INDEX single column on table with non-dup data":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (a INTEGER, b TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'z')").ok
    let r = execSql(db, "CREATE UNIQUE INDEX idx_a ON t(a)")
    check r.ok
    # Index should work for lookups
    let rows = execSql(db, "SELECT b FROM t WHERE a = 2").value
    check rows == @["y"]

  test "CREATE UNIQUE INDEX single column fails on duplicate":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (a INTEGER, b TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1,'x'),(1,'y')").ok
    let r = execSql(db, "CREATE UNIQUE INDEX idx_a ON t(a)")
    check not r.ok
    check "UNIQUE" in r.err.message

  test "CREATE UNIQUE INDEX multi-column on non-dup data succeeds":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (a INTEGER, b TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1,'x'),(1,'y'),(2,'x')").ok
    # (a,b) combos are all unique
    let r = execSql(db, "CREATE UNIQUE INDEX idx_ab ON t(a, b)")
    check r.ok

  test "CREATE UNIQUE INDEX multi-column fails on duplicate":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (a INTEGER, b TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1,'x'),(1,'x')").ok
    let r = execSql(db, "CREATE UNIQUE INDEX idx_ab ON t(a, b)")
    check not r.ok
    check "UNIQUE" in r.err.message

  test "CREATE UNIQUE INDEX on empty table succeeds":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (a INTEGER)").ok
    let r = execSql(db, "CREATE UNIQUE INDEX idx_a ON t(a)")
    check r.ok

  test "CREATE UNIQUE INDEX ignores NULL values in single col":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (a INTEGER)").ok
    check execSql(db, "INSERT INTO t VALUES (NULL),(NULL),(1)").ok
    let r = execSql(db, "CREATE UNIQUE INDEX idx_a ON t(a)")
    check r.ok  # NULLs should not trigger unique violation

suite "Engine misc paths":
  test "execSql with closed db returns error":
    let db = openDb(":memory:").value
    discard closeDb(db)
    let r = execSql(db, "SELECT 1")
    check not r.ok

  test "INSERT with default value column":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (a INTEGER, b INTEGER DEFAULT 42)").ok
    check execSql(db, "INSERT INTO t (a) VALUES (5)").ok
    let rows = execSql(db, "SELECT a, b FROM t").value
    check rows.len == 1
    check rows[0] == "5|42"
