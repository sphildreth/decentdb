## Coverage tests for exec.nim math/function evaluation:
## - LN with decimal/float64 args, domain errors (L3196-3213)
## - LOG(base, x) with float64/decimal base and arg, domain errors (L3214-3262)
## - EXP with float64/decimal args (L3264-3280)
## - RANDOM function (L3282-3292)
## - POWER/POW with float64/decimal exponent (L3294-3320)
## - MOD with float64 args, division by zero (L3322-3346)
## - LIKE_ESCAPE edge cases (L3354-3366)
import unittest
import strutils
import engine

proc freshDb(): Db =
  openDb(":memory:").value

suite "LN function edge cases":
  test "LN with float64 argument":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LN(10.0)")
    require r.ok
    check r.value[0].startsWith("2.")

  test "LN with decimal (NUMERIC) column value":
    let db = freshDb()
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (x NUMERIC(10,4))")
    discard execSql(db, "INSERT INTO t VALUES (2.7183)")
    let r = execSql(db, "SELECT LN(x) FROM t")
    require r.ok
    check r.value[0].startsWith("1.")

  test "LN of zero returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LN(0)")
    check not r.ok
    check "positive" in r.err.message

  test "LN of negative returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LN(-1)")
    check not r.ok

  test "LN with NULL returns NULL":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LN(NULL)")
    require r.ok
    check r.value[0] == "NULL"

  test "LN with non-numeric argument returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LN('hello')")
    check not r.ok

suite "LOG function edge cases":
  test "LOG(x) with float64 argument":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(100.0)")
    require r.ok
    check r.value[0].startsWith("2.")

  test "LOG(x) with decimal (NUMERIC) argument":
    let db = freshDb()
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (x NUMERIC(10,2))")
    discard execSql(db, "INSERT INTO t VALUES (100.00)")
    let r = execSql(db, "SELECT LOG(x) FROM t")
    require r.ok
    check r.value[0].startsWith("2.")

  test "LOG(x) with zero returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(0)")
    check not r.ok

  test "LOG(x) with negative returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(-5.0)")
    check not r.ok

  test "LOG(base, x) with float64 base":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(2.0, 8.0)")
    require r.ok
    check r.value[0].startsWith("3.")

  test "LOG(base, x) with decimal base":
    let db = freshDb()
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (b NUMERIC(5,1), x NUMERIC(5,1))")
    discard execSql(db, "INSERT INTO t VALUES (2.0, 8.0)")
    let r = execSql(db, "SELECT LOG(b, x) FROM t")
    require r.ok
    check r.value[0].startsWith("3.")

  test "LOG(base, x) with float64 arg":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(10, 100.0)")
    require r.ok
    check r.value[0].startsWith("2.")

  test "LOG(base, x) with decimal arg":
    let db = freshDb()
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (x NUMERIC(5,1))")
    discard execSql(db, "INSERT INTO t VALUES (100.0)")
    let r = execSql(db, "SELECT LOG(10, x) FROM t")
    require r.ok
    check r.value[0].startsWith("2.")

  test "LOG(base=0, x) returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(0, 100)")
    check not r.ok
    check "positive" in r.err.message

  test "LOG(base=1, x) returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(1, 100)")
    check not r.ok
    check "1" in r.err.message

  test "LOG(base, x=0) returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(2, 0)")
    check not r.ok
    check "positive" in r.err.message

  test "LOG(base, x) with NULL base returns NULL":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(NULL, 100)")
    require r.ok
    check r.value[0] == "NULL"

  test "LOG(base, x) with NULL x returns NULL":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG(2, NULL)")
    require r.ok
    check r.value[0] == "NULL"

  test "LOG10 with float64 argument":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LOG10(1000.0)")
    require r.ok
    check r.value[0].startsWith("3.")

  test "LOG10 with decimal argument":
    let db = freshDb()
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (x NUMERIC(10,2))")
    discard execSql(db, "INSERT INTO t VALUES (1000.00)")
    let r = execSql(db, "SELECT LOG10(x) FROM t")
    require r.ok
    check r.value[0].startsWith("3.")

suite "EXP function edge cases":
  test "EXP with float64 argument":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT EXP(1.0)")
    require r.ok
    check r.value[0].startsWith("2.71")

  test "EXP with decimal argument":
    let db = freshDb()
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (x NUMERIC(5,2))")
    discard execSql(db, "INSERT INTO t VALUES (1.00)")
    let r = execSql(db, "SELECT EXP(x) FROM t")
    require r.ok
    check r.value[0].startsWith("2.71")

  test "EXP with NULL returns NULL":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT EXP(NULL)")
    require r.ok
    check r.value[0] == "NULL"

  test "EXP with non-numeric returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT EXP('abc')")
    check not r.ok

suite "POWER function with float/decimal":
  test "POWER(float, float)":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT POWER(2.0, 3.0)")
    require r.ok
    check r.value[0].startsWith("8.")

  test "POWER with decimal exponent from column":
    let db = freshDb()
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (b NUMERIC(4,1), e NUMERIC(4,1))")
    discard execSql(db, "INSERT INTO t VALUES (2.0, 3.0)")
    let r = execSql(db, "SELECT POWER(b, e) FROM t")
    require r.ok
    check r.value[0].startsWith("8.")

  test "POW alias works with float":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT POW(2.0, 10.0)")
    require r.ok
    check r.value[0].startsWith("1024.")

  test "POWER(NULL, 2) returns NULL":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT POWER(NULL, 2)")
    require r.ok
    check r.value[0] == "NULL"

  test "POWER(2, NULL) returns NULL":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT POWER(2, NULL)")
    require r.ok
    check r.value[0] == "NULL"

suite "MOD function with float args":
  test "MOD(float, float)":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT MOD(10.5, 3.5)")
    require r.ok
    check r.value[0].startsWith("0.")

  test "MOD(int, float)":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT MOD(7, 2.5)")
    require r.ok

  test "MOD(float, int)":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT MOD(7.5, 2)")
    require r.ok
    check r.value[0].startsWith("1.5")

  test "MOD(float, 0.0) returns division by zero error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT MOD(7.0, 0.0)")
    check not r.ok
    check "zero" in r.err.message.toLowerAscii

  test "MOD(float, 0) returns division by zero error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT MOD(7.0, 0)")
    check not r.ok

  test "MOD(NULL, 2.0) returns NULL":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT MOD(NULL, 2.0)")
    require r.ok
    check r.value[0] == "NULL"

suite "LIKE_ESCAPE function edge cases":
  test "LIKE_ESCAPE with NULL pattern returns NULL":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LIKE_ESCAPE(NULL, '!')")
    require r.ok
    check r.value[0] == "NULL"

  test "LIKE_ESCAPE with NULL escape returns NULL":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LIKE_ESCAPE('foo%', NULL)")
    require r.ok
    check r.value[0] == "NULL"

  test "LIKE_ESCAPE normalizes pattern correctly":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LIKE_ESCAPE('foo!%bar', '!')")
    require r.ok
    check "foo" in r.value[0]

  test "LIKE_ESCAPE wrong arg count returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT LIKE_ESCAPE('foo')")
    check not r.ok

suite "RANDOM function":
  test "RANDOM() returns a float between 0 and 1":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT RANDOM()")
    require r.ok
    let v = parseFloat(r.value[0])
    check v >= 0.0
    check v <= 1.0

  test "RANDOM() with args returns error":
    let db = freshDb()
    defer: discard closeDb(db)
    let r = execSql(db, "SELECT RANDOM(1)")
    check not r.ok
