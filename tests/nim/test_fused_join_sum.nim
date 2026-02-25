import unittest
import strutils
import algorithm
import engine
import record/record

## Tests for tryFuseJoinSumAggregate (exec.nim L4255-4600)
## The fused path is triggered by:
##   SELECT left.name, SUM(right.amount) FROM left INNER JOIN right ON left.id = right.fk
##   GROUP BY left.id, left.name
## where left.id is INTEGER PRIMARY KEY and right.amount is INTEGER or FLOAT.

proc sorted(rows: seq[string]): seq[string] =
  result = rows
  result.sort()

suite "Fused JOIN SUM aggregate":
  test "basic fused join SUM with integer amounts":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 50), (3, 2, 200), (4, 3, 75)").ok
    let r = execSql(db, """
      SELECT u.name, SUM(o.amount)
      FROM users u INNER JOIN orders o ON u.id = o.user_id
      GROUP BY u.id, u.name
    """)
    check r.ok
    let rows = sorted(r.value)
    check rows.len == 3
    # Alice = 150, Bob = 200, Carol = 75
    check rows[0].startsWith("Alice|") or rows[0].startsWith("Bob|") or rows[0].startsWith("Carol|")
    var names: seq[string] = @[]
    var sums: seq[float] = @[]
    for row in r.value:
      let parts = row.split('|')
      names.add(parts[0])
      sums.add(parseFloat(parts[1]))
    check "Alice" in names
    check "Bob" in names
    check "Carol" in names
    let aliceSum = sums[names.find("Alice")]
    let bobSum = sums[names.find("Bob")]
    let carolSum = sums[names.find("Carol")]
    check aliceSum == 150.0
    check bobSum == 200.0
    check carolSum == 75.0

  test "fused join SUM with float amounts":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE sales (id INTEGER PRIMARY KEY, product_id INTEGER, revenue FLOAT)").ok
    check execSql(db, "INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget')").ok
    check execSql(db, "INSERT INTO sales VALUES (1, 1, 9.99), (2, 1, 19.98), (3, 2, 5.50)").ok
    let r = execSql(db, """
      SELECT p.name, SUM(s.revenue)
      FROM products p INNER JOIN sales s ON p.id = s.product_id
      GROUP BY p.id, p.name
    """)
    check r.ok
    check r.value.len == 2

  test "fused join SUM with no matching orders":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").ok
    # Bob has no orders
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 100)").ok
    let r = execSql(db, """
      SELECT u.name, SUM(o.amount)
      FROM users u INNER JOIN orders o ON u.id = o.user_id
      GROUP BY u.id, u.name
    """)
    check r.ok
    check r.value.len == 1  # Only Alice has orders

  test "fused join SUM SUM first (sum, name order)":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 42)").ok
    # Sum first, then name
    let r = execSql(db, """
      SELECT SUM(o.amount), u.name
      FROM users u INNER JOIN orders o ON u.id = o.user_id
      GROUP BY u.id, u.name
    """)
    check r.ok
    check r.value.len == 1

  test "fused join SUM empty tables":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    # Empty tables
    let r = execSql(db, """
      SELECT u.name, SUM(o.amount)
      FROM users u INNER JOIN orders o ON u.id = o.user_id
      GROUP BY u.id, u.name
    """)
    check r.ok
    check r.value.len == 0

  test "non-fused: HAVING clause falls back to normal path":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 100), (2, 2, 50)").ok
    # HAVING with non-aggregate condition disables fused path and tests HAVING
    let r = execSql(db, """
      SELECT u.name, SUM(o.amount)
      FROM users u INNER JOIN orders o ON u.id = o.user_id
      GROUP BY u.id, u.name
      HAVING u.name != 'Bob'
    """)
    check r.ok
    check r.value.len == 1  # Only Alice

  test "non-fused: COUNT instead of SUM falls back":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 100)").ok
    let r = execSql(db, """
      SELECT u.name, COUNT(o.amount)
      FROM users u INNER JOIN orders o ON u.id = o.user_id
      GROUP BY u.id, u.name
    """)
    check r.ok
    check r.value.len == 1

  test "fused join SUM with many rows (sparse path with large keys)":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE big_users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE big_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    # Insert users with large id values to trigger sparse path
    check execSql(db, "INSERT INTO big_users VALUES (1000001, 'LargeKey1'), (1000002, 'LargeKey2')").ok
    check execSql(db, "INSERT INTO big_orders VALUES (1, 1000001, 500), (2, 1000002, 300)").ok
    let r = execSql(db, """
      SELECT u.name, SUM(o.amount)
      FROM big_users u INNER JOIN big_orders o ON u.id = o.user_id
      GROUP BY u.id, u.name
    """)
    check r.ok
    check r.value.len == 2

  test "fused join SUM with NULL amounts":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, NULL), (2, 1, 100)").ok
    let r = execSql(db, """
      SELECT u.name, SUM(o.amount)
      FROM users u INNER JOIN orders o ON u.id = o.user_id
      GROUP BY u.id, u.name
    """)
    check r.ok
    # SUM of NULL + 100 = 100 (NULL treated as 0 in sum)
    check r.value.len == 1

  test "non-fused: LEFT JOIN (not INNER)":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 100)").ok
    let r = execSql(db, """
      SELECT u.name, SUM(o.amount)
      FROM users u LEFT JOIN orders o ON u.id = o.user_id
      GROUP BY u.id, u.name
    """)
    check r.ok

  test "non-fused: GROUP BY without key column":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 100)").ok
    # GROUP BY name only (no id) → falls back
    let r = execSql(db, """
      SELECT u.name, SUM(o.amount)
      FROM users u INNER JOIN orders o ON u.id = o.user_id
      GROUP BY u.name
    """)
    check r.ok
    check r.value.len == 1

  test "fused join SUM on reversed join condition (right.id = left.user_id)":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 50), (3, 2, 200)").ok
    # Reversed join condition
    let r = execSql(db, """
      SELECT u.name, SUM(o.amount)
      FROM users u INNER JOIN orders o ON o.user_id = u.id
      GROUP BY u.id, u.name
    """)
    check r.ok
    check r.value.len == 2
