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

suite "Planner Coverage":
  test "plan simple SELECT all":
    let path = makeTempDb("decentdb_planner_simple.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("SELECT * FROM items")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)

  test "plan SELECT with WHERE":
    let path = makeTempDb("decentdb_planner_where.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value INT64)")
    
    let parseRes = parseSql("SELECT * FROM items WHERE id = 1")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)

  test "plan SELECT with ORDER BY":
    let path = makeTempDb("decentdb_planner_order.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value INT64)")
    
    let parseRes = parseSql("SELECT * FROM items ORDER BY value")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    check planRes.value.kind == pkSort
    
    discard closeDb(db)

  test "plan SELECT with LIMIT":
    let path = makeTempDb("decentdb_planner_limit2.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value INT64)")
    
    let parseRes = parseSql("SELECT * FROM items LIMIT 10")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    check planRes.value.kind == pkLimit
    
    discard closeDb(db)

  test "plan SELECT with LIMIT and OFFSET":
    let path = makeTempDb("decentdb_planner_limit_offset.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value INT64)")
    
    let parseRes = parseSql("SELECT * FROM items LIMIT 10 OFFSET 5")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)

  test "plan SELECT with aggregate COUNT":
    let path = makeTempDb("decentdb_planner_count.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value INT64)")
    
    let parseRes = parseSql("SELECT COUNT(*) FROM items")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    check planRes.value.kind == pkAggregate
    
    discard closeDb(db)

  test "plan SELECT with SUM":
    let path = makeTempDb("decentdb_planner_sum.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value INT64)")
    
    let parseRes = parseSql("SELECT SUM(value) FROM items")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    check planRes.value.kind == pkAggregate
    
    discard closeDb(db)

  test "plan SELECT with GROUP BY":
    let path = makeTempDb("decentdb_planner_group.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, category TEXT, value INT64)")
    
    let parseRes = parseSql("SELECT category, COUNT(*) FROM items GROUP BY category")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    check planRes.value.kind == pkAggregate
    
    discard closeDb(db)

  test "plan INNER JOIN":
    let path = makeTempDb("decentdb_planner_inner_join.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE users (id INT64, name TEXT)")
    discard execSql(db, "CREATE TABLE orders (id INT64, user_id INT64)")
    
    let parseRes = parseSql("SELECT * FROM users INNER JOIN orders ON (users.id = orders.user_id)")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)

  test "plan LEFT JOIN":
    let path = makeTempDb("decentdb_planner_left_join.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE users (id INT64, name TEXT)")
    discard execSql(db, "CREATE TABLE orders (id INT64, user_id INT64)")
    
    let parseRes = parseSql("SELECT * FROM users LEFT JOIN orders ON (users.id = orders.user_id)")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)

  test "plan multiple tables JOIN":
    let path = makeTempDb("decentdb_planner_multi_join.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE users (id INT64, name TEXT)")
    discard execSql(db, "CREATE TABLE orders (id INT64, user_id INT64)")
    discard execSql(db, "CREATE TABLE items (id INT64, order_id INT64)")
    
    let parseRes = parseSql("SELECT * FROM users JOIN orders ON (users.id = orders.user_id) JOIN items ON (orders.id = items.order_id)")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)

  test "plan SELECT with multiple WHERE conditions":
    let path = makeTempDb("decentdb_planner_multi_where.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value INT64, active BOOL)")
    
    let parseRes = parseSql("SELECT * FROM items WHERE id = 1 AND active = TRUE")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok
    let planRes = plan(db.catalog, bindRes.value)
    check planRes.ok
    
    discard closeDb(db)
