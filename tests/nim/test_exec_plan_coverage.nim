import unittest
import os

import engine
import exec/exec
import planner/planner
import record/record
import sql/sql

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Exec Plan Coverage":
  test "index seek plan returns matching rows":
    let path = makeTempDb("decentdb_exec_plan_index.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE items (id INT, name TEXT)").ok
    check execSql(db, "CREATE INDEX items_id_idx ON items (id)").ok
    discard execSql(db, "INSERT INTO items (id, name) VALUES (1, 'alpha')")
    discard execSql(db, "INSERT INTO items (id, name) VALUES (2, 'beta')")

    let plan = Plan(kind: pkIndexSeek, table: "items", column: "id",
      valueExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 2)))
    let rowsRes = execPlan(db.pager, db.catalog, plan, @[])
    check rowsRes.ok
    check rowsRes.value.len == 1
    check rowsRes.value[0].values[0].int64Val == 2

    discard closeDb(db)

  test "trigram seek plan filters using trigram index":
    let path = makeTempDb("decentdb_exec_plan_trigram.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE docs (id INT, body TEXT)").ok
    check execSql(db, "CREATE INDEX docs_body_trgm ON docs USING trigram (body)").ok
    discard execSql(db, "INSERT INTO docs (id, body) VALUES (1, 'alphabet')")
    discard execSql(db, "INSERT INTO docs (id, body) VALUES (2, 'beta')")

    let plan = Plan(kind: pkTrigramSeek, table: "docs", column: "body",
      likeExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "%bet%")),
      likeInsensitive: true)
    let rowsRes = execPlan(db.pager, db.catalog, plan, @[])
    check rowsRes.ok
    check rowsRes.value.len == 2

    discard closeDb(db)

  test "filter and project plans compose to shape rows":
    let path = makeTempDb("decentdb_exec_plan_filter.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE metrics (id INT, value INT)").ok
    discard execSql(db, "INSERT INTO metrics (id, value) VALUES (1, 10)")
    discard execSql(db, "INSERT INTO metrics (id, value) VALUES (2, 20)")

    let predicate = Expr(
      kind: ekBinary,
      op: "=",
      left: Expr(kind: ekColumn, table: "metrics", name: "id"),
      right: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 2))
    )
    let selectItem = SelectItem(expr: Expr(kind: ekColumn, table: "metrics", name: "value"), alias: "score")
    let plan = Plan(kind: pkProject, projections: @[selectItem],
      left: Plan(kind: pkFilter, predicate: predicate,
        left: Plan(kind: pkTableScan, table: "metrics")))

    let rowsRes = execPlan(db.pager, db.catalog, plan, @[])
    check rowsRes.ok
    check rowsRes.value.len == 1
    check rowsRes.value[0].columns[0] == "score"
    check rowsRes.value[0].values[0].int64Val == 20

    discard closeDb(db)

  test "join plan drives index seek on the right side":
    let path = makeTempDb("decentdb_exec_plan_join.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE users (id INT, name TEXT)").ok
    check execSql(db, "CREATE TABLE posts (id INT, user_id INT, title TEXT)").ok
    check execSql(db, "CREATE INDEX posts_user_idx ON posts (user_id)").ok
    discard execSql(db, "INSERT INTO users (id, name) VALUES (1, 'alice')")
    discard execSql(db, "INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'entry')")

    let joinOn = Expr(
      kind: ekBinary,
      op: "=",
      left: Expr(kind: ekColumn, table: "users", name: "id"),
      right: Expr(kind: ekColumn, table: "posts", name: "user_id")
    )
    let rightPlan = Plan(kind: pkIndexSeek, table: "posts", column: "user_id",
      valueExpr: Expr(kind: ekColumn, table: "users", name: "id"))
    let plan = Plan(kind: pkJoin, joinType: jtInner, joinOn: joinOn,
      left: Plan(kind: pkTableScan, table: "users"), right: rightPlan)

    let rowsRes = execPlan(db.pager, db.catalog, plan, @[])
    check rowsRes.ok
    check rowsRes.value.len == 1
    check rowsRes.value[0].values[0].int64Val == 1
    check rowsRes.value[0].values[2].int64Val == 10

    discard closeDb(db)

  test "sort and limit nodes respect ordering":
    let path = makeTempDb("decentdb_exec_plan_sort.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE scores (id INT, value INT)").ok
    discard execSql(db, "INSERT INTO scores (id, value) VALUES (1, 100)")
    discard execSql(db, "INSERT INTO scores (id, value) VALUES (2, 50)")
    discard execSql(db, "INSERT INTO scores (id, value) VALUES (3, 75)")

    let order = @[OrderItem(expr: Expr(kind: ekColumn, table: "scores", name: "value"), asc: false)]
    let sortPlan = Plan(kind: pkSort, orderBy: order,
      left: Plan(kind: pkTableScan, table: "scores"))
    let plan = Plan(kind: pkLimit, limit: 1, offset: 1, left: sortPlan)

    let rowsRes = execPlan(db.pager, db.catalog, plan, @[])
    check rowsRes.ok
    check rowsRes.value.len == 1
    check rowsRes.value[0].values[1].int64Val == 75

    discard closeDb(db)

  test "statement plan returns no rows":
    let path = makeTempDb("decentdb_exec_plan_statement.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let plan = Plan(kind: pkStatement)
    let rowsRes = execPlan(db.pager, db.catalog, plan, @[])
    check rowsRes.ok
    check rowsRes.value.len == 0

    discard closeDb(db)
