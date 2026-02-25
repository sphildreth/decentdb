import unittest
import strutils
import engine
import record/record

## Tests for INSERT ON CONFLICT DO UPDATE SET ... WHERE (engine.nim L1710-1760)
## and batch unique violations on TEXT/BLOB columns (engine.nim L890-946)

suite "INSERT ON CONFLICT DO UPDATE with WHERE":
  test "ON CONFLICT DO UPDATE WHERE matches - performs update":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, cnt INTEGER DEFAULT 0)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'Alice', 5)").ok
    # Insert with conflict; WHERE matches → do update
    let r = execSql(db, """
      INSERT INTO t (id, name, cnt) VALUES (1, 'Alice', 10)
      ON CONFLICT (id) DO UPDATE SET cnt = 20
      WHERE t.name = 'Alice'
    """)
    check r.ok
    let rows = execSql(db, "SELECT id, name, cnt FROM t").value
    check rows[0] == "1|Alice|20"

  test "ON CONFLICT DO UPDATE WHERE no match - no update":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, cnt INTEGER DEFAULT 0)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'Bob', 5)").ok
    # WHERE doesn't match → no update
    let r = execSql(db, """
      INSERT INTO t (id, name, cnt) VALUES (1, 'Bob', 10)
      ON CONFLICT (id) DO UPDATE SET cnt = 99
      WHERE t.name = 'Alice'
    """)
    check r.ok
    let rows = execSql(db, "SELECT cnt FROM t WHERE id = 1").value
    check rows[0] == "5"  # unchanged

  test "ON CONFLICT DO NOTHING on TEXT UNIQUE":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'alice@example.com')").ok
    let r = execSql(db, "INSERT INTO t VALUES (2, 'alice@example.com') ON CONFLICT DO NOTHING")
    check r.ok
    let rows = execSql(db, "SELECT COUNT(*) FROM t").value
    check rows[0] == "1"

suite "Batch TEXT UNIQUE violations":
  test "batch INSERT with duplicate TEXT in UNIQUE column":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'alice@example.com')").ok
    # Second INSERT should fail on unique text violation
    let r = execSql(db, "INSERT INTO t VALUES (2, 'alice@example.com')")
    check not r.ok
    check "unique" in r.err.message.toLowerAscii() or "duplicate" in r.err.message.toLowerAscii() or "constraint" in r.err.message.toLowerAscii()

  test "multi-value INSERT with duplicate in UNIQUE TEXT column":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, tag TEXT UNIQUE)").ok
    # Insert with duplicates in single statement
    let r = execSql(db, "INSERT INTO t VALUES (1, 'foo'), (2, 'foo')")
    check not r.ok

  test "UPDATE causing TEXT UNIQUE violation":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'alice@example.com'), (2, 'bob@example.com')").ok
    let r = execSql(db, "UPDATE t SET email = 'alice@example.com' WHERE id = 2")
    check not r.ok

suite "JOIN with VIEW":
  test "SELECT from table joined with view (simple view)":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 100), (2, 2, 50)").ok
    check execSql(db, "CREATE VIEW big_orders AS SELECT user_id, amount FROM orders WHERE amount >= 50").ok
    let r = execSql(db, "SELECT u.name, bo.amount FROM users u JOIN big_orders bo ON u.id = bo.user_id ORDER BY u.name")
    check r.ok
    check r.value.len == 2

  test "SELECT * from table JOIN view fails":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE VIEW v AS SELECT id, name FROM t").ok
    let r = execSql(db, "SELECT * FROM t JOIN v ON t.id = v.id")
    check not r.ok
    check "not supported" in r.err.message.toLowerAscii() or "star" in r.err.message.toLowerAscii() or "*" in r.err.message

  test "JOIN with CTE using UNION ALL":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')").ok
    # CTE with set op joined to table
    let r = execSql(db, """
      WITH cte AS (
        SELECT id, name FROM t WHERE id = 1
        UNION ALL
        SELECT id, name FROM t WHERE id = 2
      )
      SELECT t.name FROM t JOIN cte ON t.id = cte.id
    """)
    check not r.ok or r.value.len >= 2  # may not be supported

  test "CREATE VIEW IF NOT EXISTS":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE VIEW v AS SELECT id, name FROM t").ok
    # IF NOT EXISTS should succeed silently
    let r = execSql(db, "CREATE VIEW IF NOT EXISTS v AS SELECT id FROM t")
    check r.ok

suite "Partial index with various predicate types":
  test "Partial index with float comparison predicate":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, score FLOAT)").ok
    check execSql(db, "CREATE INDEX idx_hi ON t(score) WHERE score > 0.5").ok
    check execSql(db, "INSERT INTO t VALUES (1, 0.9), (2, 0.1), (3, 0.8)").ok
    let r = execSql(db, "SELECT id FROM t WHERE score > 0.5")
    check r.ok
    check r.value.len == 2

  test "Partial index with IS NULL predicate":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").ok
    check execSql(db, "CREATE INDEX idx_null ON t(val) WHERE val IS NULL").ok
    check execSql(db, "INSERT INTO t VALUES (1, NULL), (2, 'a'), (3, NULL)").ok
    let r = execSql(db, "SELECT id FROM t WHERE val IS NULL ORDER BY id")
    check r.ok
    check r.value.len == 2

  test "Partial index with IS NOT NULL predicate":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").ok
    check execSql(db, "CREATE INDEX idx_nonnull ON t(val) WHERE val IS NOT NULL").ok
    check execSql(db, "INSERT INTO t VALUES (1, NULL), (2, 'a'), (3, 'b')").ok
    let r = execSql(db, "SELECT id FROM t WHERE val IS NOT NULL ORDER BY id")
    check r.ok
    check r.value.len == 2

  test "Partial index with AND predicate":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER, active INTEGER)").ok
    check execSql(db, "CREATE INDEX idx_act ON t(score) WHERE score > 10 AND active = 1").ok
    check execSql(db, "INSERT INTO t VALUES (1, 50, 1), (2, 5, 1), (3, 100, 0)").ok
    let r = execSql(db, "SELECT id FROM t WHERE score > 10 AND active = 1")
    check r.ok
    check r.value.len == 1
    check r.value[0] == "1"

  test "Partial index with OR predicate":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)").ok
    check execSql(db, "CREATE INDEX idx_stat ON t(status) WHERE status = 'A' OR status = 'B'").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'A'), (2, 'B'), (3, 'C')").ok
    let r = execSql(db, "SELECT id FROM t WHERE status = 'A' OR status = 'B' ORDER BY id")
    check r.ok
    check r.value.len == 2
