import unittest
import os
import strutils
import engine
import record/record
import decentdb_cli
import pager/db_header

proc makeTempDb(name: string): string =
  let path = getTempDir() / name & ".ddb"
  if fileExists(path): removeFile(path)
  if fileExists(path & "-wal"): removeFile(path & "-wal")
  path

suite "CLI dump and verify header":
  test "dumpHeader on valid database":
    let path = makeTempDb("test_dump_hdr")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    discard closeDb(db)
    let rc = dumpHeader(db = path)
    check rc == 0

  test "verifyHeader on valid database":
    let path = makeTempDb("test_verify_hdr")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    discard closeDb(db)
    let rc = verifyHeader(db = path)
    check rc == 0

  test "verifyHeader on corrupt checksum":
    let path = makeTempDb("test_verify_bad_chk")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    discard closeDb(db)
    # Corrupt checksum byte
    var f = open(path, fmReadWriteExisting)
    f.setFilePos(30)
    let badByte: byte = 0xFF
    discard f.writeBuffer(unsafeAddr badByte, 1)
    f.close()
    let rc = verifyHeader(db = path)
    check rc != 0

  test "dumpHeader missing db arg":
    let rc = dumpHeader(db = "")
    check rc != 0

  test "verifyHeader missing db arg":
    let rc = verifyHeader(db = "")
    check rc != 0

  test "schemaListTables missing db arg":
    let rc = schemaListTables(db = "")
    check rc != 0

suite "CLI schemaListIndexes and schemaListTables":
  test "schemaListTables on db with tables":
    let path = makeTempDb("test_schema_list")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    check execSql(db, "CREATE TABLE a (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE b (id INTEGER PRIMARY KEY)").ok
    discard closeDb(db)
    let rc = schemaListTables(db = path)
    check rc == 0

  test "schemaListIndexes on db with indexes":
    let path = makeTempDb("test_schema_idx")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE INDEX name_idx ON t(name)").ok
    discard closeDb(db)
    let rc = schemaListIndexes(db = path)
    check rc == 0

  test "schemaListIndexes on specific table":
    let path = makeTempDb("test_schema_idx_table")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE INDEX name_idx ON t(name)").ok
    discard closeDb(db)
    let rc = schemaListIndexes(db = path, table = "t")
    check rc == 0

suite "CLI dumpSql with CHECK constraints and FK ON UPDATE":
  test "dumpSql with FK ON DELETE CASCADE ON UPDATE CASCADE":
    let path = makeTempDb("test_dump_fk")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    check execSql(db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").ok
    check execSql(db, """
      CREATE TABLE child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE ON UPDATE CASCADE
      )
    """).ok
    discard closeDb(db)
    let rc = dumpSql(db = path, output = "stdout")
    check rc == 0

  test "dumpSql with CHECK constraint":
    let path = makeTempDb("test_dump_check")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER CONSTRAINT valid_score CHECK (score >= 0 AND score <= 100))").ok
    discard closeDb(db)
    let rc = dumpSql(db = path, output = "stdout")
    check rc == 0

  test "dumpSql missing db":
    let rc = dumpSql(db = "")
    check rc != 0

suite "CLI checkpointCmd":
  test "checkpointCmd on valid db":
    let path = makeTempDb("test_checkpoint_cmd")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "INSERT INTO t VALUES (1)").ok
    discard closeDb(db)
    let rc = checkpointCmd(db = path)
    check rc == 0

  test "checkpointCmd with verbose":
    let path = makeTempDb("test_checkpoint_verbose")
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    let db = openDb(path).value
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    discard closeDb(db)
    let rc = checkpointCmd(db = path, verbose = true)
    check rc == 0
