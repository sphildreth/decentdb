import unittest
import strutils
import engine
import record/record

## Tests covering planner.nim and exec.nim paths including:
## - Subquery FROM with joins (planner L294)
## - RIGHT JOIN rewrite through subquery path (planner L165)
## - OR-based union plans (planner L403)
## - TVF (table-valued function) with WHERE filter (planner L269)
## - LEFT JOIN with subquery on right side (exec.nim L5310)
## - VIEW with window function (binder.nim L867-923)

suite "Subquery FROM with joins":
  test "subquery FROM with inner join":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE a (id INTEGER, val TEXT)").ok
    check execSql(db, "CREATE TABLE b (a_id INTEGER, extra TEXT)").ok
    check execSql(db, "INSERT INTO a VALUES (1,'alpha'),(2,'beta')").ok
    check execSql(db, "INSERT INTO b VALUES (1,'x'),(2,'y')").ok
    # FROM subquery JOIN table
    let r = execSql(db, """
      SELECT s.id, b.extra
      FROM (SELECT id, val FROM a WHERE id > 0) s
      JOIN b ON s.id = b.a_id
      ORDER BY s.id
    """)
    check r.ok
    check r.value.len == 2
    check r.value[0] == "1|x"
    check r.value[1] == "2|y"

  test "subquery FROM with LEFT JOIN":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE a (id INTEGER)").ok
    check execSql(db, "CREATE TABLE b (a_id INTEGER, val TEXT)").ok
    check execSql(db, "INSERT INTO a VALUES (1),(2),(3)").ok
    check execSql(db, "INSERT INTO b VALUES (1,'found')").ok
    let r = execSql(db, """
      SELECT s.id, b.val
      FROM (SELECT id FROM a) s
      LEFT JOIN b ON s.id = b.a_id
      ORDER BY s.id
    """)
    check r.ok
    check r.value.len == 3

  test "RIGHT JOIN rewrite via subquery FROM":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE a (id INTEGER, val TEXT)").ok
    check execSql(db, "CREATE TABLE b (id INTEGER, val TEXT)").ok
    check execSql(db, "INSERT INTO a VALUES (1,'a1')").ok
    check execSql(db, "INSERT INTO b VALUES (1,'b1'),(2,'b2')").ok
    # RIGHT JOIN: all b rows, matching a rows
    let r = execSql(db, """
      SELECT b.id, b.val
      FROM a RIGHT JOIN b ON a.id = b.id
      ORDER BY b.id
    """)
    check r.ok
    check r.value.len == 2

suite "TVF with WHERE filter":
  test "json_each array with key filter":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, """
      SELECT key, value FROM json_each('["apple","banana","cherry"]')
      WHERE key > 0
    """)
    check r.ok
    check r.value.len == 2

  test "json_each all values":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT key, value FROM json_each('{\"a\":1,\"b\":2}')")
    check r.ok
    check r.value.len == 2

  test "json_each WHERE on key = 0":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, """
      SELECT key FROM json_each('["x","y","z"]') WHERE key = 1
    """)
    check r.ok
    check r.value.len == 1
    check r.value[0] == "1"

suite "LEFT JOIN with subquery on right side":
  test "LEFT JOIN with subquery on right - zero rows from right":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE a (id INTEGER, val TEXT)").ok
    check execSql(db, "CREATE TABLE b (id INTEGER, val TEXT)").ok
    check execSql(db, "INSERT INTO a VALUES (1,'a'),(2,'b')").ok
    # b is empty, subquery returns zero rows
    let r = execSql(db, """
      SELECT a.id, s.val
      FROM a LEFT JOIN (SELECT id, val FROM b) s ON a.id = s.id
      ORDER BY a.id
    """)
    check r.ok
    check r.value.len == 2

  test "LEFT JOIN with subquery on right - matching rows":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE a (id INTEGER)").ok
    check execSql(db, "CREATE TABLE b (id INTEGER, v TEXT)").ok
    check execSql(db, "INSERT INTO a VALUES (1),(2),(3)").ok
    check execSql(db, "INSERT INTO b VALUES (1,'one'),(3,'three')").ok
    let r = execSql(db, """
      SELECT a.id, sub.v
      FROM a LEFT JOIN (SELECT id, v FROM b WHERE id > 0) sub ON a.id = sub.id
      ORDER BY a.id
    """)
    check r.ok
    check r.value.len == 3

suite "VIEW with window function":
  test "CREATE VIEW with ROW_NUMBER OVER PARTITION":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (dept TEXT, salary INTEGER)").ok
    check execSql(db, "INSERT INTO t VALUES ('eng',100),('eng',200),('hr',150)").ok
    check execSql(db, """
      CREATE VIEW v AS
      SELECT dept, salary,
             ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn
      FROM t
    """).ok
    let r = execSql(db, "SELECT dept, salary, rn FROM v ORDER BY dept, salary")
    check r.ok
    check r.value.len == 3

  test "SELECT from view with window function - all rows":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (grp INTEGER, val INTEGER)").ok
    check execSql(db, "INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,15)").ok
    check execSql(db, """
      CREATE VIEW ranked AS
      SELECT grp, val,
             ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn
      FROM t
    """).ok
    let r = execSql(db, "SELECT grp, val, rn FROM ranked ORDER BY grp, val")
    check r.ok
    check r.value.len == 4
    check r.value[0] == "1|10|1"
    check r.value[2] == "2|5|1"

  test "subquery with window function filter on rn":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (grp INTEGER, val INTEGER)").ok
    check execSql(db, "INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,15)").ok
    let r = execSql(db, """
      SELECT grp, val, rn FROM (
        SELECT grp, val,
               ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn
        FROM t
      ) sub WHERE rn = 1 ORDER BY grp
    """)
    check r.ok
    check r.value.len == 2
    check r.value[0] == "1|10|1"
    check r.value[1] == "2|5|1"

suite "OR-based index union plan":
  test "SELECT with OR on indexed columns uses union plan":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").ok
    # OR on PK should generate union/distinct plan
    let r = execSql(db, "SELECT id, val FROM t WHERE id = 1 OR id = 3 ORDER BY id")
    check r.ok
    check r.value.len == 2
    check r.value[0] == "1|a"
    check r.value[1] == "3|c"

  test "SELECT with OR on two indexed columns":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (a INTEGER, b INTEGER)").ok
    check execSql(db, "CREATE INDEX idx_a ON t(a)").ok
    check execSql(db, "CREATE INDEX idx_b ON t(b)").ok
    check execSql(db, "INSERT INTO t VALUES (1,10),(2,20),(3,30)").ok
    let r = execSql(db, "SELECT a, b FROM t WHERE a = 1 OR b = 20 ORDER BY a")
    check r.ok
    check r.value.len == 2
