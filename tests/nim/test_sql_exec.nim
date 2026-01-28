import unittest
import os
import strutils
import engine
import record/record

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

proc splitRow(row: string): seq[string] =
  if row.len == 0:
    return @[]
  row.split("|")

suite "SQL Exec":
  test "basic DDL/DML and params":
    let path = makeTempDb("decentdb_sql_exec.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE users (id INT, name TEXT, active BOOL, score FLOAT)").ok
    check execSql(db, "CREATE TABLE posts (user_id INT, title TEXT)").ok
    check execSql(db, "CREATE INDEX users_id_idx ON users (id)").ok
    discard execSql(db, "INSERT INTO users (id, name, active, score) VALUES ($1, $2, $3, $4)", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: @['A'.byte]),
      Value(kind: vkBool, boolVal: true),
      Value(kind: vkFloat64, float64Val: 1.5)
    ])
    discard execSql(db, "INSERT INTO users (id, name, active, score) VALUES ($1, $2, $3, $4)", @[
      Value(kind: vkInt64, int64Val: 2),
      Value(kind: vkText, bytes: @['B'.byte]),
      Value(kind: vkBool, boolVal: false),
      Value(kind: vkFloat64, float64Val: 3.0)
    ])
    discard execSql(db, "INSERT INTO posts (user_id, title) VALUES ($1, $2)", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: @['P'.byte])
    ])
    let selectRes = execSql(db, "SELECT name FROM users WHERE id = 1")
    check selectRes.ok
    check splitRow(selectRes.value[0])[0] == "A"
    let joinRes = execSql(db, "SELECT u.name, p.title FROM users u LEFT JOIN posts p ON (u.id = p.user_id) ORDER BY u.id")
    check joinRes.ok
    check joinRes.value.len == 2
    let aggRes = execSql(db, "SELECT COUNT(*), SUM(score) FROM users")
    check aggRes.ok
    let aggRow = splitRow(aggRes.value[0])
    check aggRow[0] == "2"
    check aggRow[1] == "4.5"
    check execSql(db, "UPDATE users SET score = 2.5 WHERE id = 1").ok
    let updRes = execSql(db, "SELECT score FROM users WHERE id = 1")
    check updRes.ok
    check splitRow(updRes.value[0])[0] == "2.5"
    check execSql(db, "DELETE FROM users WHERE id = 2").ok
    let countRes = execSql(db, "SELECT COUNT(*) FROM users")
    check countRes.ok
    check splitRow(countRes.value[0])[0] == "1"
    discard closeDb(db)

  test "statement rollback on bind error":
    let path = makeTempDb("decentdb_sql_rollback.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t (id, name) VALUES (1, 'X')").ok
    let badUpdate = execSql(db, "UPDATE t SET missing = 1 WHERE id = 1")
    check badUpdate.ok == false
    let rows = execSql(db, "SELECT name FROM t WHERE id = 1")
    check rows.ok
    check splitRow(rows.value[0])[0] == "X"
    discard closeDb(db)
