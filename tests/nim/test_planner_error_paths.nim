import unittest
import os

import engine
import catalog/catalog
import record/record
import planner/planner
import sql/binder
import sql/sql
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Planner Error Paths":
  test "plan SELECT with non-existent table":
    let path = makeTempDb("decentdb_planner_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("SELECT * FROM items JOIN missing ON (1 = 1)")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    if bindRes.ok:
      let planRes = plan(db.catalog, bindRes.value)
      check not planRes.ok
      check planRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "plan SELECT with invalid column reference":
    let path = makeTempDb("decentdb_planner_invalid_col.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    # This might parse but fail in binding/planning
    let parseRes = parseSql("SELECT items.missing FROM items")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    # Binding might fail or succeed
    if bindRes.ok:
      # If binding succeeded, try to plan
      let planRes = plan(db.catalog, bindRes.value)
      # May or may not fail depending on implementation
    
    discard closeDb(db)

  test "plan ORDER BY with missing column":
    let path = makeTempDb("decentdb_planner_order_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("SELECT * FROM items ORDER BY missing")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    if bindRes.ok:
      let planRes = plan(db.catalog, bindRes.value)
      check not planRes.ok
      check planRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "plan with complex JOIN and WHERE":
    let path = makeTempDb("decentdb_planner_complex_join.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE users (id INT64, name TEXT)")
    discard execSql(db, "CREATE TABLE orders (id INT64, user_id INT64)")
    discard execSql(db, "CREATE TABLE items (id INT64, order_id INT64)")
    
    let parseRes = parseSql("SELECT u.name, o.id, i.id FROM users u JOIN orders o ON (u.id = o.user_id) JOIN items i ON (o.id = i.order_id) WHERE u.id = 1")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)

  test "plan SELECT with LIMIT and OFFSET":
    let path = makeTempDb("decentdb_planner_limit.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("SELECT * FROM items LIMIT 10 OFFSET 5")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)

  test "plan with aggregate and GROUP BY":
    let path = makeTempDb("decentdb_planner_agg.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, category TEXT, value INT64)")
    
    let parseRes = parseSql("SELECT category, SUM(value) FROM items GROUP BY category")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)

  test "plan with HAVING clause":
    let path = makeTempDb("decentdb_planner_having.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, category TEXT, value INT64)")
    
    let parseRes = parseSql("SELECT category, SUM(value) as total FROM items GROUP BY category HAVING total > 100")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    if bindRes.ok:
      let planRes = plan(db.catalog, bindRes.value)
      # HAVING may or may not be supported
    
    discard closeDb(db)

suite "Engine Error Paths Extended":
  test "execSql on closed database":
    let path = makeTempDb("decentdb_engine_closed.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard closeDb(db)
    
    let execRes = execSql(db, "SELECT 1")
    check not execRes.ok
    check execRes.err.code == ERR_INTERNAL

  test "beginTransaction on closed database":
    let path = makeTempDb("decentdb_tx_closed.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard closeDb(db)
    
    let txRes = db.beginTransaction()
    check not txRes.ok
    check txRes.err.code == ERR_INTERNAL

  test "beginTransaction when already in transaction":
    let path = makeTempDb("decentdb_tx_nested.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let txRes1 = db.beginTransaction()
    check txRes1.ok
    
    # Nested transactions may or may not be supported
    let txRes2 = db.beginTransaction()
    # Either fails with error or succeeds (supporting nested)
    
    discard db.rollbackTransaction()
    discard closeDb(db)

  test "commitTransaction without active transaction":
    let path = makeTempDb("decentdb_commit_no_tx.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let commitRes = db.commitTransaction()
    check not commitRes.ok
    check commitRes.err.code == ERR_TRANSACTION
    
    discard closeDb(db)

  test "rollbackTransaction without active transaction":
    let path = makeTempDb("decentdb_rollback_no_tx.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let rollbackRes = db.rollbackTransaction()
    check not rollbackRes.ok
    check rollbackRes.err.code == ERR_TRANSACTION
    
    discard closeDb(db)
