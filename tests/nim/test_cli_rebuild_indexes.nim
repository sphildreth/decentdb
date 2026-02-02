import unittest
import os
import strutils

import engine
import decentdb_cli

proc makeTempDb(name: string): string =
  let normalizedName =
    if name.len >= 4 and name[name.len - 4 .. ^1] == ".ddb":
      name
    else:
      name & ".ddb"
  let path = getTempDir() / normalizedName
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "CLI rebuild-indexes":
  test "rebuildAllIndexes rebuilds all indexes":
    let path = makeTempDb("decentdb_rebuild_indexes")

    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    block:
      check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").ok
      check execSql(db, "CREATE INDEX idx_t_a ON t(a)").ok
      check execSql(db, "CREATE INDEX idx_t_b ON t(b)").ok

      let res = rebuildAllIndexes(db)
      check res.ok
      check res.value.len == 2

      var sawA = false
      var sawB = false
      for line in res.value:
        if line.contains("idx_t_a"):
          sawA = true
        if line.contains("idx_t_b"):
          sawB = true
      check sawA
      check sawB

    discard closeDb(db)

  test "rebuildAllIndexes can filter by table":
    let path = makeTempDb("decentdb_rebuild_indexes_table_filter")

    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    block:
      check execSql(db, "CREATE TABLE t1 (id INT PRIMARY KEY, a INT)").ok
      check execSql(db, "CREATE TABLE t2 (id INT PRIMARY KEY, b INT)").ok
      check execSql(db, "CREATE INDEX idx_t1_a ON t1(a)").ok
      check execSql(db, "CREATE INDEX idx_t2_b ON t2(b)").ok

      let res = rebuildAllIndexes(db, table = "t1")
      check res.ok
      check res.value.len == 1
      check res.value[0].contains("idx_t1_a")

    discard closeDb(db)
