import unittest
import os
import strutils
import engine
import record/record
import decentdb_cli

proc makeTempDb(name: string): string =
  let path = getTempDir() / name & ".ddb"
  if fileExists(path): removeFile(path)
  if fileExists(path & "-wal"): removeFile(path & "-wal")
  path

suite "CLI schema dump with FK ON DELETE/ON UPDATE":
  test "formatSchemaSummary includes FK ON DELETE CASCADE":
    let path = makeTempDb("test_cli_fk_schema")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").ok
    check execSql(db, """
      CREATE TABLE child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE
      )
    """).ok
    let info = collectInfoRows(db, true)
    let joined = info.join("\n")
    check "parent" in joined
    check "child" in joined
    check "CASCADE" in joined or "DELETE" in joined

  test "formatSchemaSummary includes FK ON DELETE SET NULL":
    let path = makeTempDb("test_cli_fk_setnull")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    defer: discard closeDb(db)
    check execSql(db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").ok
    check execSql(db, """
      CREATE TABLE child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES parent(id) ON DELETE SET NULL ON UPDATE CASCADE
      )
    """).ok
    let info = collectInfoRows(db, true)
    let joined = info.join("\n")
    check "SET NULL" in joined or "CASCADE" in joined

  test "infoCmd with schema_summary and FK table":
    let path = makeTempDb("test_cli_fk_info")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    check execSql(db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").ok
    check execSql(db, """
      CREATE TABLE child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE
      )
    """).ok
    discard closeDb(db)
    let rc = infoCmd(db = path, schema_summary = true)
    check rc == 0
