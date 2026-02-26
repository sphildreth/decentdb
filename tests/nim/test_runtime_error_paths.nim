import unittest
import strutils
import engine
import record/record

proc toBytes(text: string): seq[byte] =
  result = newSeq[byte](text.len)
  for i, c in text:
    result[i] = c.byte

suite "Runtime error paths":
  ## Tests that trigger runtime errors during SQL evaluation, covering
  ## execPlan and evalExpr failure/cleanup paths.

  test "LIKE pattern too long triggers runtime error in scan":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES ('hello')").ok
    var longPattern = "a%"
    for i in 0..4100:
      longPattern.add('x')
    longPattern.add("%b")
    let res = execSql(db, "SELECT * FROM t WHERE name LIKE $1",
      @[Value(kind: vkText, bytes: toBytes(longPattern))])
    check not res.ok
    check "LIKE pattern too long" in res.err.message

  test "LIKE too many wildcards triggers runtime error":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES ('hello')").ok
    var manyWildcards = ""
    for i in 0..130:
      manyWildcards.add("%x%")
    let res = execSql(db, "SELECT * FROM t WHERE name LIKE $1",
      @[Value(kind: vkText, bytes: toBytes(manyWildcards))])
    check not res.ok
    check "wildcards" in res.err.message

  test "float division by zero returns error":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x FLOAT)").ok
    check execSql(db, "INSERT INTO t VALUES (1.5)").ok
    let r = execSql(db, "SELECT x / 0.0 FROM t")
    check not r.ok
    check "division by zero" in r.err.message.toLowerAscii() or "Division by zero" in r.err.message

  test "integer division by zero returns error":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT 5 / 0")
    check not r.ok
    check "division by zero" in r.err.message.toLowerAscii() or "Division by zero" in r.err.message

  test "decimal division by zero returns error":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x DECIMAL(10,2))").ok
    check execSql(db, "INSERT INTO t VALUES (1.50)").ok
    let r = execSql(db, "SELECT x / CAST('0.00' AS DECIMAL(10,2)) FROM t")
    check not r.ok
    check "division by zero" in r.err.message.toLowerAscii() or "Division by zero" in r.err.message

  test "LIKE_ESCAPE function with escape character":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LIKE_ESCAPE('foo%bar', '!')")
    check r.ok
    check r.value.len == 1
    check "foo%bar" in r.value[0]

  test "LIKE_ESCAPE function with null pattern returns null":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (NULL)").ok
    let r = execSql(db, "SELECT LIKE_ESCAPE(x, '!') FROM t")
    check r.ok
    check "null" in r.value[0].toLowerAscii()

  test "EXISTS subquery returns true when rows found":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER)").ok
    check execSql(db, "INSERT INTO t VALUES (1),(2)").ok
    let r = execSql(db, "SELECT EXISTS(SELECT 1 FROM t WHERE x = 1)")
    check r.ok
    check r.value == @["true"]

  test "EXISTS subquery returns false when no rows":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER)").ok
    check execSql(db, "INSERT INTO t VALUES (1)").ok
    let r = execSql(db, "SELECT EXISTS(SELECT 1 FROM t WHERE x = 99)")
    check r.ok
    check r.value == @["false"]

  test "IN subquery filters rows correctly":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER)").ok
    check execSql(db, "INSERT INTO t VALUES (1),(2),(3)").ok
    let r = execSql(db, "SELECT x FROM t WHERE x IN (SELECT x FROM t WHERE x > 1) ORDER BY x")
    check r.ok
    check r.value == @["2", "3"]

  test "division by zero in WHERE clause triggers execPlan error":
    let db = openDb(":memory:").value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE t (x INTEGER)").ok
    check execSql(db, "INSERT INTO t VALUES (1),(2)").ok
    let r = execSql(db, "SELECT x FROM t WHERE x / 0 = 1")
    check not r.ok
