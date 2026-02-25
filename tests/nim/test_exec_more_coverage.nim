## Tests targeting more exec.nim, engine.nim, and btree.nim uncovered paths:
## - LAG, LEAD, FIRST_VALUE, LAST_VALUE window functions
## - json_each, json_tree TVFs  
## - LIKE modes: lmPrefix, lmSuffix, lmGeneric (_)
## - GROUP_CONCAT / STRING_AGG aggregate
## - UNION / UNION ALL set ops
## - MIN/MAX with NULL handling
## - Decimal arithmetic operations
## - HAVING aggregate filter
import unittest
import strutils
import engine
import errors

suite "LIKE pattern modes":
  test "LIKE suffix pattern (lmSuffix)":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'Bob')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'Charlie')").ok
    let r = execSql(db, "SELECT name FROM t WHERE name LIKE '%ice'")
    check r.ok
    check r.value == @["Alice"]

  test "LIKE prefix pattern (lmPrefix)":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'pre_one')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'pre_two')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'other')").ok
    let r = execSql(db, "SELECT name FROM t WHERE name LIKE 'pre%'")
    check r.ok
    check r.value.len == 2

  test "LIKE single-char wildcard triggers lmGeneric":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'abc')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'aXc')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'xyz')").ok
    let r = execSql(db, "SELECT name FROM t WHERE name LIKE 'a_c'")
    check r.ok
    check r.value.len == 2

  test "ILIKE case-insensitive contains":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'Hello World')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'HELLO THERE')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'goodbye')").ok
    let r = execSql(db, "SELECT name FROM t WHERE name ILIKE '%hello%'")
    check r.ok
    check r.value.len == 2

  test "LIKE backslash triggers lmGeneric":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, '100%')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'none')").ok
    # Backslash makes it lmGeneric
    let r = execSql(db, "SELECT name FROM t WHERE name LIKE '%\\%%'")
    check r.ok

suite "Window functions LAG LEAD FIRST_VALUE LAST_VALUE":
  test "LAG function":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    for i in 1..5:
      check execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")").ok
    let r = execSql(db, "SELECT id, v, LAG(v) OVER (ORDER BY id) FROM t")
    check r.ok
    check r.value.len == 5
    # First row LAG should be NULL
    check r.value[0].endsWith("NULL")
    # Second row LAG should be 10
    check r.value[1].endsWith("10")

  test "LEAD function":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    for i in 1..5:
      check execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")").ok
    let r = execSql(db, "SELECT id, v, LEAD(v) OVER (ORDER BY id) FROM t")
    check r.ok
    check r.value.len == 5
    # Last row LEAD should be NULL
    check r.value[4].endsWith("NULL")
    # First row LEAD should be 20
    check r.value[0].endsWith("20")

  test "FIRST_VALUE and LAST_VALUE over partition":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'A', 100)").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'A', 200)").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'A', 300)").ok
    check execSql(db, "INSERT INTO t VALUES (4, 'B', 400)").ok
    let r = execSql(db, """
      SELECT id, 
        FIRST_VALUE(v) OVER (PARTITION BY cat ORDER BY id),
        LAST_VALUE(v) OVER (PARTITION BY cat ORDER BY id)
      FROM t
    """)
    check r.ok
    check r.value.len == 4
    # For partition A, rows 1-3: FIRST_VALUE should be 100
    check r.value[0].contains("100")
    check r.value[1].contains("100")

  test "NTH_VALUE window function":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    for i in 1..5:
      check execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")").ok
    let r = execSql(db, "SELECT id, NTH_VALUE(v, 2) OVER (ORDER BY id) FROM t")
    check r.ok
    check r.value.len == 5

suite "json_each and json_tree TVFs":
  test "json_each on JSON object":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, """SELECT key, value, type FROM json_each('{"name":"Alice","age":30}')""")
    check r.ok
    check r.value.len == 2

  test "json_each on JSON array":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, """SELECT key, value, type FROM json_each('[10,20,30]')""")
    check r.ok
    check r.value.len == 3
    # Keys should be indices 0,1,2
    check r.value[0].startsWith("0")

  test "json_each NULL input returns empty":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT key, value FROM json_each(NULL)")
    check r.ok
    check r.value.len == 0

  test "json_each with boolean and null values":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, """SELECT key, type FROM json_each('{"a":true,"b":false,"c":null,"d":42}')""")
    check r.ok
    check r.value.len == 4

suite "GROUP_CONCAT and STRING_AGG":
  test "GROUP_CONCAT without separator":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'a')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'b')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'c')").ok
    let r = execSql(db, "SELECT GROUP_CONCAT(name) FROM t")
    check r.ok
    check r.value.len == 1
    check "a" in r.value[0]
    check "b" in r.value[0]

  test "GROUP_CONCAT per group":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'A', 'alice')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'A', 'adam')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'B', 'bob')").ok
    let r = execSql(db, "SELECT cat, GROUP_CONCAT(name) FROM t GROUP BY cat")
    check r.ok
    check r.value.len == 2

suite "HAVING and aggregate filter":
  test "HAVING with non-aggregate filter":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE sales (id INT PRIMARY KEY, cat TEXT, amt INT)").ok
    check execSql(db, "INSERT INTO sales VALUES (1, 'A', 10)").ok
    check execSql(db, "INSERT INTO sales VALUES (2, 'A', 20)").ok
    check execSql(db, "INSERT INTO sales VALUES (3, 'B', 5)").ok
    # HAVING with non-aggregate column filter
    let r = execSql(db, "SELECT cat, COUNT(*) FROM sales GROUP BY cat HAVING cat = 'A'")
    check r.ok
    check r.value.len == 1
    check "A" in r.value[0]

  test "COUNT DISTINCT":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'A')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'A')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'B')").ok
    let r = execSql(db, "SELECT COUNT(DISTINCT cat) FROM t")
    check r.ok
    check r.value == @["2"]

suite "UNION set operations":
  test "UNION ALL preserves duplicates":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY)").ok
    for i in 1..3:
      check execSql(db, "INSERT INTO a VALUES (" & $i & ")").ok
    for i in 2..4:
      check execSql(db, "INSERT INTO b VALUES (" & $i & ")").ok
    let r = execSql(db, "SELECT id FROM a UNION ALL SELECT id FROM b")
    check r.ok
    check r.value.len == 6

  test "UNION removes duplicates":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY)").ok
    for i in 1..3:
      check execSql(db, "INSERT INTO a VALUES (" & $i & ")").ok
    for i in 2..4:
      check execSql(db, "INSERT INTO b VALUES (" & $i & ")").ok
    let r = execSql(db, "SELECT id FROM a UNION SELECT id FROM b")
    check r.ok
    check r.value.len == 4

suite "Decimal arithmetic":
  test "Decimal division":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE d (id INT PRIMARY KEY, v DECIMAL(10,2))").ok
    check execSql(db, "INSERT INTO d VALUES (1, 10.00)").ok
    check execSql(db, "INSERT INTO d VALUES (2, 20.00)").ok
    let r = execSql(db, "SELECT v / 2 FROM d WHERE id = 1")
    check r.ok
    check r.value.len == 1
    check "5" in r.value[0]

  test "Decimal comparison with integer":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE d (id INT PRIMARY KEY, v DECIMAL(10,2))").ok
    check execSql(db, "INSERT INTO d VALUES (1, 1.50)").ok
    check execSql(db, "INSERT INTO d VALUES (2, 2.75)").ok
    check execSql(db, "INSERT INTO d VALUES (3, 0.99)").ok
    # Compare Decimal vs Decimal column to column
    let r = execSql(db, "SELECT a.id FROM d a, d b WHERE a.id = 1 AND b.id = 3 AND a.v > b.v")
    check r.ok
    # 1.50 > 0.99 should be true, so row 1 qualifies
    check r.value.len == 1
    check r.value[0] == "1"
