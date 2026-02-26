## Tests for TEMP VIEW, CREATE OR REPLACE VIEW, ALTER VIEW, DROP VIEW edge cases.
## Targets engine.nim L3809-L3886 (view operations in execPreparedNonSelect).
import unittest
import os
import engine

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path): removeFile(path)
  if fileExists(path & "-wal"): removeFile(path & "-wal")
  path

suite "TEMP VIEW operations":
  test "CREATE TEMP VIEW and SELECT from it":
    let path = makeTempDb("temp_view1.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'hello')")
    discard execSql(db, "INSERT INTO t VALUES (2, 'world')")
    let createRes = execSql(db, "CREATE TEMP VIEW tv AS SELECT id, val FROM t WHERE id > 0")
    require createRes.ok
    let selRes = execSql(db, "SELECT * FROM tv")
    require selRes.ok
    check selRes.value.len == 2

  test "CREATE TEMP VIEW with named columns":
    let path = makeTempDb("temp_view2.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price FLOAT)")
    discard execSql(db, "INSERT INTO items VALUES (1, 'apple', 1.5)")
    discard execSql(db, "INSERT INTO items VALUES (2, 'banana', 0.75)")
    let createRes = execSql(db, "CREATE TEMP VIEW cheap_items AS SELECT id, name FROM items WHERE price < 1.0")
    require createRes.ok
    let selRes = execSql(db, "SELECT name FROM cheap_items")
    require selRes.ok
    check selRes.value == @["banana"]

  test "TEMP VIEW and permanent VIEW co-exist":
    let path = makeTempDb("temp_view3.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, x INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    discard execSql(db, "INSERT INTO t VALUES (2, 20)")
    # Permanent view
    discard execSql(db, "CREATE VIEW perm_v AS SELECT id FROM t")
    # Temp view
    let tempRes = execSql(db, "CREATE TEMP VIEW temp_v AS SELECT x FROM t WHERE x > 5")
    require tempRes.ok
    let permSel = execSql(db, "SELECT * FROM perm_v")
    require permSel.ok
    check permSel.value.len == 2
    let tempSel = execSql(db, "SELECT * FROM temp_v")
    require tempSel.ok
    check tempSel.value.len == 2

suite "CREATE OR REPLACE VIEW":
  test "CREATE OR REPLACE VIEW replaces existing view":
    let path = makeTempDb("create_replace_view1.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 100)")
    discard execSql(db, "INSERT INTO t VALUES (2, 200)")
    discard execSql(db, "INSERT INTO t VALUES (3, 300)")
    # Create initial view
    discard execSql(db, "CREATE VIEW v AS SELECT id FROM t WHERE val < 200")
    let sel1 = execSql(db, "SELECT * FROM v")
    require sel1.ok
    check sel1.value.len == 1
    # Replace view with different query
    let replRes = execSql(db, "CREATE OR REPLACE VIEW v AS SELECT id FROM t WHERE val > 100")
    require replRes.ok
    let sel2 = execSql(db, "SELECT * FROM v")
    require sel2.ok
    check sel2.value.len == 2  # rows 2 and 3

  test "CREATE OR REPLACE VIEW on non-existent view creates it":
    let path = makeTempDb("create_replace_view2.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO t VALUES (42)")
    # CREATE OR REPLACE on brand-new view
    let createRes = execSql(db, "CREATE OR REPLACE VIEW new_view AS SELECT id FROM t")
    require createRes.ok
    let selRes = execSql(db, "SELECT * FROM new_view")
    require selRes.ok
    check selRes.value == @["42"]

suite "ALTER VIEW (rename)":
  test "ALTER VIEW RENAME TO renames the view":
    let path = makeTempDb("alter_view1.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 'x')")
    discard execSql(db, "CREATE VIEW old_name AS SELECT id, v FROM t")
    let renameRes = execSql(db, "ALTER VIEW old_name RENAME TO new_name")
    require renameRes.ok
    # old_name no longer accessible
    let oldSel = execSql(db, "SELECT * FROM old_name")
    check not oldSel.ok
    # new_name accessible
    let newSel = execSql(db, "SELECT * FROM new_name")
    require newSel.ok
    check newSel.value.len == 1

  test "ALTER VIEW to existing name fails":
    let path = makeTempDb("alter_view2.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE VIEW v1 AS SELECT id FROM t")
    discard execSql(db, "CREATE VIEW v2 AS SELECT id FROM t")
    let renameRes = execSql(db, "ALTER VIEW v1 RENAME TO v2")
    check not renameRes.ok

suite "DROP VIEW edge cases":
  test "DROP VIEW without IF EXISTS on non-existent fails":
    let path = makeTempDb("drop_view_err1.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    let dropRes = execSql(db, "DROP VIEW nonexistent_view")
    check not dropRes.ok

  test "DROP VIEW with dependent views fails":
    let path = makeTempDb("drop_view_dep1.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(db, "INSERT INTO t VALUES (1, 10)")
    discard execSql(db, "CREATE VIEW v1 AS SELECT id, val FROM t")
    discard execSql(db, "CREATE VIEW v2 AS SELECT * FROM v1 WHERE val > 5")
    # Cannot drop v1 while v2 depends on it
    let dropRes = execSql(db, "DROP VIEW v1")
    check not dropRes.ok
    # v2 can be dropped first, then v1
    let dropV2 = execSql(db, "DROP VIEW v2")
    require dropV2.ok
    let dropV1 = execSql(db, "DROP VIEW v1")
    require dropV1.ok

  test "DROP VIEW with dependent views - ALTER fails too":
    let path = makeTempDb("drop_view_dep2.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE VIEW v1 AS SELECT id FROM t")
    discard execSql(db, "CREATE VIEW v2 AS SELECT * FROM v1")
    # ALTER VIEW when dependent views exist fails
    let alterRes = execSql(db, "ALTER VIEW v1 RENAME TO v1_new")
    check not alterRes.ok
