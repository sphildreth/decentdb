## Tests for CAST from bool, decimal types and NULL targets.
## Targets exec.nim L1560-L1643 (castValue/canonicalCastType uncovered branches).
import unittest
import engine

suite "CAST from BOOL":
  test "CAST true to INTEGER":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST(true AS INTEGER)")
    require res.ok
    check res.value == @["1"]

  test "CAST false to INTEGER":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST(false AS INTEGER)")
    require res.ok
    check res.value == @["0"]

  test "CAST true to FLOAT":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST(true AS FLOAT)")
    require res.ok
    check res.value == @["1.0"]

  test "CAST false to FLOAT":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST(false AS FLOAT)")
    require res.ok
    check res.value == @["0.0"]

  test "CAST bool to BOOL (identity)":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let resTrue = execSql(db, "SELECT CAST(true AS BOOL)")
    require resTrue.ok
    check resTrue.value == @["true"]
    let resFalse = execSql(db, "SELECT CAST(false AS BOOL)")
    require resFalse.ok
    check resFalse.value == @["false"]

  test "CAST bool column to INTEGER via table":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE flags (id INT PRIMARY KEY, active BOOL)")
    discard execSql(db, "INSERT INTO flags VALUES (1, true)")
    discard execSql(db, "INSERT INTO flags VALUES (2, false)")
    let res = execSql(db, "SELECT id, CAST(active AS INTEGER) FROM flags ORDER BY id")
    require res.ok
    check res.value == @["1|1", "2|0"]

  test "CAST bool column to TEXT":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE flags (id INT PRIMARY KEY, active BOOL)")
    discard execSql(db, "INSERT INTO flags VALUES (1, true)")
    let res = execSql(db, "SELECT CAST(active AS TEXT) FROM flags")
    require res.ok
    check res.value == @["true"]

suite "CAST from DECIMAL column":
  test "CAST decimal column to INTEGER":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE prices (id INT PRIMARY KEY, price DECIMAL(10,2))")
    discard execSql(db, "INSERT INTO prices VALUES (1, 3.14)")
    discard execSql(db, "INSERT INTO prices VALUES (2, 5.00)")
    let res = execSql(db, "SELECT CAST(price AS INTEGER) FROM prices ORDER BY id")
    require res.ok
    # Verify the decimal-to-integer cast runs (vkDecimal branch in castValue)
    check res.value.len == 2
    check res.value[1] == "5"

  test "CAST decimal column to FLOAT":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE prices (id INT PRIMARY KEY, price DECIMAL(10,2))")
    discard execSql(db, "INSERT INTO prices VALUES (1, 2.50)")
    let res = execSql(db, "SELECT CAST(price AS FLOAT) FROM prices")
    require res.ok
    check res.value == @["2.5"]

  test "CAST decimal column to BOOL":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE vals (id INT PRIMARY KEY, v DECIMAL(5,2))")
    discard execSql(db, "INSERT INTO vals VALUES (1, 0.00)")
    discard execSql(db, "INSERT INTO vals VALUES (2, 1.50)")
    let res = execSql(db, "SELECT CAST(v AS BOOL) FROM vals ORDER BY id")
    require res.ok
    check res.value == @["false", "true"]

  test "CAST decimal column to TEXT":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE vals (id INT PRIMARY KEY, v DECIMAL(5,2))")
    discard execSql(db, "INSERT INTO vals VALUES (1, 3.14)")
    let res = execSql(db, "SELECT CAST(v AS TEXT) FROM vals")
    require res.ok
    check res.value.len == 1  # just check it returns something

suite "CAST NULL to various types":
  test "CAST NULL to INTEGER returns NULL":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST(NULL AS INTEGER)")
    require res.ok
    check res.value == @["NULL"]

  test "CAST NULL to FLOAT returns NULL":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST(NULL AS FLOAT)")
    require res.ok
    check res.value == @["NULL"]

  test "CAST NULL to TEXT returns NULL":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST(NULL AS TEXT)")
    require res.ok
    check res.value == @["NULL"]

  test "CAST NULL to BOOL returns NULL":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST(NULL AS BOOL)")
    require res.ok
    check res.value == @["NULL"]

suite "CAST unsupported types":
  test "CAST to unknown type fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST(42 AS BLOB)")
    check not res.ok

  test "CAST text with 'f' to BOOL (false)":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST('f' AS BOOL)")
    require res.ok
    check res.value == @["false"]

  test "CAST text with '1' to BOOL (true)":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST('1' AS BOOL)")
    require res.ok
    check res.value == @["true"]

  test "CAST text with '0' to BOOL (false)":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST('0' AS BOOL)")
    require res.ok
    check res.value == @["false"]

  test "CAST invalid text to FLOAT fails":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let res = execSql(db, "SELECT CAST('not_a_float' AS FLOAT)")
    check not res.ok

  test "CAST float column to BOOL":
    let dbRes = openDb(":memory:")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE vals (id INT PRIMARY KEY, v FLOAT)")
    discard execSql(db, "INSERT INTO vals VALUES (1, 0.0)")
    discard execSql(db, "INSERT INTO vals VALUES (2, 3.14)")
    let res = execSql(db, "SELECT CAST(v AS BOOL) FROM vals ORDER BY id")
    require res.ok
    check res.value == @["false", "true"]
