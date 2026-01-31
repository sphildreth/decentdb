import unittest
import os
import strutils

import engine
import exec/exec
import sql/sql
import planner/planner
import record/record

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

proc splitRow(row: string): seq[string] =
  if row.len == 0:
    return @[]
  row.split("|")

suite "Exec Helpers":
  test "likeMatch patterns and case handling":
    check likeMatch("Hello", "H%o", false)
    check likeMatch("Hello", "H_llo", false)
    check not likeMatch("Hello", "H%z", false)
    check likeMatch("Hello", "h%O", true)

  test "evalExpr handles params, NULL, and ambiguity":
    let row = Row(columns: @["t.a", "u.a"], values: @[Value(kind: vkNull), Value(kind: vkInt64, int64Val: 1)])

    let isNullExpr = Expr(
      kind: ekBinary,
      op: "IS",
      left: Expr(kind: ekColumn, table: "t", name: "a"),
      right: Expr(kind: ekLiteral, value: SqlValue(kind: svNull))
    )
    let isNullRes = evalExpr(row, isNullExpr, @[])
    check isNullRes.ok
    check isNullRes.value.kind == vkBool
    check isNullRes.value.boolVal

    let isNotExpr = Expr(
      kind: ekBinary,
      op: "IS NOT",
      left: Expr(kind: ekColumn, table: "t", name: "a"),
      right: Expr(kind: ekLiteral, value: SqlValue(kind: svNull))
    )
    let isNotRes = evalExpr(row, isNotExpr, @[])
    check isNotRes.ok
    check not isNotRes.value.boolVal

    let paramExpr = Expr(kind: ekParam, index: 1)
    let paramRes = evalExpr(row, paramExpr, @[])
    check not paramRes.ok

    let ambigExpr = Expr(kind: ekColumn, table: "", name: "a")
    let ambigRes = evalExpr(row, ambigExpr, @[])
    check not ambigRes.ok

    let notExpr = Expr(kind: ekUnary, unOp: "NOT", expr: Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: true)))
    let notRes = evalExpr(row, notExpr, @[])
    check notRes.ok
    check not notRes.value.boolVal

suite "Exec Plans":
  test "left join produces NULLs for missing rows":
    let path = makeTempDb("decentdb_exec_left_join.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE a (id INT)").ok
    check execSql(db, "CREATE TABLE b (aid INT)").ok
    discard execSql(db, "INSERT INTO a (id) VALUES (1)")
    discard execSql(db, "INSERT INTO a (id) VALUES (2)")
    discard execSql(db, "INSERT INTO b (aid) VALUES (1)")

    let res = execSql(db, "SELECT a.id, b.aid FROM a LEFT JOIN b ON a.id = b.aid ORDER BY a.id")
    check res.ok
    check res.value.len == 2
    check splitRow(res.value[0]) == @["1", "1"]
    check splitRow(res.value[1])[0] == "2"
    check splitRow(res.value[1])[1] == "NULL"

    discard closeDb(db)

  test "aggregate functions with group by and having":
    let path = makeTempDb("decentdb_exec_agg.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE sales (user_id INT, amount INT)").ok
    discard execSql(db, "INSERT INTO sales (user_id, amount) VALUES (1, 10)")
    discard execSql(db, "INSERT INTO sales (user_id, amount) VALUES (1, 30)")
    discard execSql(db, "INSERT INTO sales (user_id, amount) VALUES (2, 5)")

    let groupExpr = Expr(kind: ekColumn, name: "user_id")
    let sumExpr = Expr(kind: ekFunc, funcName: "SUM", args: @[Expr(kind: ekColumn, name: "amount")])
    let avgExpr = Expr(kind: ekFunc, funcName: "AVG", args: @[Expr(kind: ekColumn, name: "amount")])
    let minExpr = Expr(kind: ekFunc, funcName: "MIN", args: @[Expr(kind: ekColumn, name: "amount")])
    let maxExpr = Expr(kind: ekFunc, funcName: "MAX", args: @[Expr(kind: ekColumn, name: "amount")])
    let havingExpr = Expr(
      kind: ekBinary,
      op: "=",
      left: Expr(kind: ekColumn, name: "expr"),
      right: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1))
    )
    let plan = Plan(
      kind: pkAggregate,
      projections: @[
        SelectItem(expr: groupExpr),
        SelectItem(expr: sumExpr),
        SelectItem(expr: avgExpr),
        SelectItem(expr: minExpr),
        SelectItem(expr: maxExpr)
      ],
      groupBy: @[groupExpr],
      having: havingExpr,
      left: Plan(kind: pkTableScan, table: "sales")
    )
    let execRes = execPlan(db.pager, db.catalog, plan, @[])
    check execRes.ok
    check execRes.value.len == 1
    let rowVals = execRes.value[0].values
    check rowVals.len == 5
    check rowVals[0].kind == vkInt64
    check rowVals[0].int64Val == 1
    check rowVals[1].float64Val == 80.0
    check rowVals[2].float64Val == 40.0
    check rowVals[3].int64Val == 0
    check rowVals[4].int64Val == 30

    discard closeDb(db)

  test "trigram seek falls back to scan on short pattern":
    let path = makeTempDb("decentdb_exec_trigram_short.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE docs (id INT, body TEXT)").ok
    check execSql(db, "CREATE INDEX docs_body_trgm ON docs USING trigram (body)").ok
    discard execSql(db, "INSERT INTO docs (id, body) VALUES (1, 'ABCD')")
    discard execSql(db, "INSERT INTO docs (id, body) VALUES (2, 'ZZAB')")

    let res = execSql(db, "SELECT id FROM docs WHERE body LIKE '%AB%' ORDER BY id")
    check res.ok
    check res.value.len == 2
    check splitRow(res.value[0])[0] == "1"
    check splitRow(res.value[1])[0] == "2"

    discard closeDb(db)
