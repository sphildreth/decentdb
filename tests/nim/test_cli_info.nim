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

suite "CLI info":
  test "info --schema-summary includes tables and indexes":
    let path = makeTempDb("decentdb_info_schema_summary")

    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    block:
      let createParent = execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT NOT NULL)")
      check createParent.ok
      let createChild = execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id), payload BLOB)")
      check createChild.ok
      let createIdx = execSql(db, "CREATE INDEX idx_child_parent_id ON child(parent_id)")
      check createIdx.ok

      let rows = collectInfoRows(db, schema_summary = true)

      var foundSchemaHeader = false
      var foundParent = false
      var foundChild = false
      var foundIndex = false
      for line in rows:
        if line == "Schema summary":
          foundSchemaHeader = true
        if line == "Table: parent":
          foundParent = true
        if line == "Table: child":
          foundChild = true
        if line.startsWith("Index: idx_child_parent_id "):
          foundIndex = true

      check foundSchemaHeader
      check foundParent
      check foundChild
      check foundIndex

    discard closeDb(db)
