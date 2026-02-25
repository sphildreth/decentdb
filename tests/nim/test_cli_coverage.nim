import unittest
import os
import strutils
import json

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

proc runCapture(p: proc(): int): int =
  # A simple helper to run a proc. In nim `echo` outputs to stdout. test runner captures it if not verbose,
  # but here we just run it and return the code.
  p()

suite "CLI Coverage":
  proc setupDb(dbPath: string) =
    let dbRes = openDb(dbPath)
    if dbRes.ok:
      let db = dbRes.value
      check execSql(db, "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT NOT NULL)").ok
      check execSql(db, "INSERT INTO t1 VALUES (1, 'Alice')").ok
      check execSql(db, "CREATE INDEX idx_t1_name ON t1(name)").ok
      discard closeDb(db)

  proc cleanupDb(dbPath: string) =
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")
    if fileExists(dbPath & ".wal"): removeFile(dbPath & ".wal")

  test "cliMain basic operations":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    # Missing db
    check cliMain(db = "") == 1
    # Checkpoint
    check cliMain(db = dbPath, checkpoint = true) == 0
    # SQL query
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", format = "json") == 0
    # SQL query with timing
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", timing = true, format = "json") == 0
    # SQL query with CSV format
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", format = "csv") == 0
    # SQL query with parameter
    check cliMain(db = dbPath, sql = "SELECT * FROM t1 WHERE id = $1", params = @["int:1"]) == 0
    # Error executing
    check cliMain(db = dbPath, sql = "SELECT * FROM t2") == 1
    # Invalid param
    check cliMain(db = dbPath, sql = "SELECT * FROM t1 WHERE id = $1", params = @["int:abc"]) == 1

  test "schema commands":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    # schemaListTables
    check schemaListTables(db = "") == 1
    check schemaListTables(db = "non_existent_db.ddb") == 0
    check schemaListTables(db = dbPath) == 0

    # schemaDescribe
    check schemaDescribe(db = "", table = "t1") == 1
    # this fails because the table doesn't exist in the empty DB
    check schemaDescribe(db = "non_existent_db.ddb", table = "t1") == 1
    check schemaDescribe(db = dbPath, table = "") == 1
    check schemaDescribe(db = dbPath, table = "t2") == 1 # non-existent
    check schemaDescribe(db = dbPath, table = "t1") == 0

    # schemaListIndexes
    check schemaListIndexes(db = "") == 1
    check schemaListIndexes(db = "non_existent_db.ddb") == 0
    check schemaListIndexes(db = dbPath) == 0
    check schemaListIndexes(db = dbPath, table = "t1") == 0

  test "index maintenance commands":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    # cmdRebuildIndex
    check cmdRebuildIndex(db = "") == 1
    # fails because index idx doesn't exist
    check cmdRebuildIndex(db = "non_existent_db.ddb", index = "idx") == 1
    check cmdRebuildIndex(db = dbPath, index = "") == 1
    check cmdRebuildIndex(db = dbPath, index = "non_existent") == 1
    check cmdRebuildIndex(db = dbPath, index = "idx_t1_name") == 0

    # cmdRebuildIndexes
    check cmdRebuildIndexes(db = "") == 1
    check cmdRebuildIndexes(db = "non_existent_db.ddb") == 0
    check cmdRebuildIndexes(db = dbPath) == 0
    check cmdRebuildIndexes(db = dbPath, table = "t1") == 0

    # cmdVerifyIndex
    check cmdVerifyIndex(db = "") == 1
    check cmdVerifyIndex(db = "non_existent_db.ddb", index = "idx") == 1
    check cmdVerifyIndex(db = dbPath, index = "") == 1
    check cmdVerifyIndex(db = dbPath, index = "non_existent") == 1
    check cmdVerifyIndex(db = dbPath, index = "idx_t1_name") == 0

  test "export and dump commands":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    let outCsv = getTempDir() / "test_out_dump.csv"
    let outJson = getTempDir() / "test_out_dump.json"
    let outSql = getTempDir() / "test_out_dump.sql"

    # exportData
    check exportData(db = "", table = "t1", output = outCsv) == 1
    check exportData(db = dbPath, table = "", output = outCsv) == 1
    check exportData(db = dbPath, table = "t1", output = "") == 1
    check exportData(db = dbPath, table = "nonexistent", output = outCsv) == 1
    check exportData(db = dbPath, table = "t1", output = outCsv, format = "invalid") == 1
    
    check exportData(db = dbPath, table = "t1", output = outCsv, format = "csv") == 0
    check fileExists(outCsv)
    check exportData(db = dbPath, table = "t1", output = outJson, format = "json") == 0
    check fileExists(outJson)

    # dumpSql
    check dumpSql(db = "") == 1
    check dumpSql(db = dbPath, output = "") == 0
    check dumpSql(db = "non_existent_db.ddb", output = outSql) == 0
    check dumpSql(db = dbPath, output = outSql) == 0
    check fileExists(outSql)
    
    if fileExists(outCsv): removeFile(outCsv)
    if fileExists(outJson): removeFile(outJson)
    if fileExists(outSql): removeFile(outSql)

  test "diagnostics commands":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    check checkpointCmd(db = "") == 1
    check checkpointCmd(db = dbPath) == 0

    check infoCmd(db = "") == 1
    check infoCmd(db = dbPath) == 0
    check infoCmd(db = dbPath, schema_summary = true) == 0

    check statsCmd(db = "") == 1
    check statsCmd(db = dbPath) == 0

  test "vacuum command":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    let outDb = makeTempDb("decentdb_cli_vacuum_out")

    check vacuumCmd(db = "") == 1
    check vacuumCmd(db = dbPath, output = "") == 1
    check vacuumCmd(db = dbPath, output = dbPath) == 1 # same file
    check vacuumCmd(db = "", output = outDb) == 1

    check vacuumCmd(db = dbPath, output = outDb) == 0
    check fileExists(outDb)

    # Without overwrite, should fail for existing output
    check vacuumCmd(db = dbPath, output = outDb) == 1
    
    # With overwrite, should succeed
    check vacuumCmd(db = dbPath, output = outDb, overwrite = true) == 0

    if fileExists(outDb): removeFile(outDb)
    if fileExists(outDb & "-wal"): removeFile(outDb & "-wal")
    if fileExists(outDb & ".wal"): removeFile(outDb & ".wal")

  test "header forensics":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    check dumpHeader(db = "") == 1
    check dumpHeader(db = dbPath) == 0
    
    check verifyHeader(db = "") == 1
    check verifyHeader(db = dbPath) == 0

  test "importData command":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    let inCsv = getTempDir() / "test_in.csv"
    let inJson = getTempDir() / "test_in.json"

    writeFile(inCsv, "id,name\n2,Bob\n3,Charlie\n")
    writeFile(inJson, """[[4, "Dave"], [5, "Eve"]]""")

    check importData(db = "", table = "t1", input = inCsv) == 1
    check importData(db = dbPath, table = "", input = inCsv) == 1
    check importData(db = dbPath, table = "t1", input = "") == 1
    check importData(db = dbPath, table = "nonexistent", input = inCsv) == 1
    check importData(db = dbPath, table = "t1", input = inCsv, format = "invalid") == 1
    
    check importData(db = dbPath, table = "t1", input = inCsv, format = "csv") == 0
    check importData(db = dbPath, table = "t1", input = inJson, format = "json") == 0

    if fileExists(inCsv): removeFile(inCsv)
    if fileExists(inJson): removeFile(inJson)

  test "cliMain param parsing edge cases":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    # Various valid parameter types
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", params = @["null", "", "int64:123", "float64:12.3", "bool:true", "text:hello", "blob:0x0102"]) == 0
    # Invalid floats
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", params = @["float:abc"]) == 1
    # Invalid blobs
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", params = @["blob:0x01FG"]) == 1
    # Unknown type
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", params = @["unknown:123"]) == 1

  test "cliMain format outputs":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    # table format
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", format = "table") == 0
    # csv format
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", format = "csv") == 0
    # json format
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", format = "json") == 0
    # Format error execution (the binary handles format silently to stdout as error JSON, exit code 0)
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", format = "invalid_format") == 0
    
    # Verbose and warnings
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", warnings = true) == 0
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", verbose = true) == 0
    check cliMain(db = dbPath, checkpoint = true, warnings = true) == 0
    
    # Heartbeat
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", heartbeatMs = 1) == 0
    
    # No rows
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", noRows = true, format = "json") == 0
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", noRows = true, format = "csv") == 1
    
    # Wal failpoints
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", walFailpoints = @["test:error:0:1"]) == 0
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", walFailpoints = @["test:invalid_kind"]) == 1
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", clearWalFailpoints = true) == 0
    
    # Other flags
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", readerCount = true) == 0
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", longReaders = 1000) == 0
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", dbInfo = true) == 0
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", checkpointBytes = 1024) == 0
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", checkpointMs = 1000) == 0
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", readerWarnMs = 1000) == 0
    check cliMain(db = dbPath, sql = "SELECT * FROM t1", readerTimeoutMs = 1000, forceTruncateOnTimeout = true) == 0
    check cliMain(db = dbPath, openClose = true) == 0

  test "bulkLoadCsv command":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    let inCsv = getTempDir() / "test_bulk_in.csv"
    writeFile(inCsv, "id,name\n2,Bob\n3,Charlie\n")

    check bulkLoadCsv(db = "", table = "t1", input = inCsv) == 1
    check bulkLoadCsv(db = dbPath, table = "", input = inCsv) == 1
    check bulkLoadCsv(db = dbPath, table = "t1", input = "") == 1
    check bulkLoadCsv(db = dbPath, table = "nonexistent", input = inCsv) == 1
    
    check bulkLoadCsv(db = dbPath, table = "t1", input = inCsv, durability = "invalid_d") == 1

    check bulkLoadCsv(db = dbPath, table = "t1", input = inCsv) == 0
    # test options
    check bulkLoadCsv(db = dbPath, table = "t1", input = inCsv, disableIndexes = false, noCheckpoint = true) == 1

    if fileExists(inCsv): removeFile(inCsv)

  test "saveAsCmd and completion":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    let outDb = makeTempDb("decentdb_cli_saveas_out")
    
    check saveAsCmd(db = "") == 1
    check saveAsCmd(db = dbPath, output = "") == 1
    check saveAsCmd(db = dbPath, output = outDb) == 0
    
    if fileExists(outDb): removeFile(outDb)
    
    check completion("bash") == 0
    check completion("zsh") == 0
    check completion("fish") == 1
    
    # repl error path
    check repl(db = "") == 1

  test "importData param parsing with diverse types":
    let dbPath = makeTempDb("decentdb_cli_coverage")
    setupDb(dbPath)
    defer: cleanupDb(dbPath)
    
    let dbRes = openDb(dbPath)
    if dbRes.ok:
      let db = dbRes.value
      check execSql(db, "CREATE TABLE t_all (id INT PRIMARY KEY, col_bool BOOL, col_float FLOAT64, col_blob BLOB, col_dec DECIMAL(10,2), col_uuid UUID)").ok
      discard closeDb(db)
      
    let inCsv = getTempDir() / "test_types_in.csv"
    let inJson = getTempDir() / "test_types_in.json"

    # CSV for t_all
    # t_all: id INT, col_bool BOOL, col_float FLOAT64, col_blob BLOB, col_dec DECIMAL, col_uuid UUID
    writeFile(inCsv, "id,col_bool,col_float,col_blob,col_dec,col_uuid\n2,true,2.71,0x0102,12.3,-123\n3,0,abc,-,12,-124\n")
    check importData(db = dbPath, table = "t_all", input = inCsv, format = "csv") == 0

    # JSON for t_all
    writeFile(inJson, """[
      [4, false, 1.414, "0x01", "12.3", "123e4567-e89b-12d3-a456-426614174000"],
      {"id": 5, "col_bool": "true", "col_float": "1.2", "col_blob": "0x01", "col_dec": 12.3, "col_uuid": "123e4567-e89b-12d3-a456-426614174000"},
      [6, 1, 1, "0102", 12.3, "123"],
      {"id": 7, "col_bool": true, "col_float": 1.2, "col_blob": "0x12", "col_dec": "12.3", "col_uuid": "123e4567-e89b-12d3-a456-426614174000"}
    ]""")
    check importData(db = dbPath, table = "t_all", input = inJson, format = "json") == 0
    
    # Empty CSV and JSON
    writeFile(inCsv, "id,col_bool,col_float,col_blob,col_dec,col_uuid\n")
    check importData(db = dbPath, table = "t_all", input = inCsv, format = "csv") == 0
    writeFile(inJson, "[]")
    check importData(db = dbPath, table = "t_all", input = inJson, format = "json") == 0

    # Test JSON with non-array root
    writeFile(inJson, """{"id": 1}""")
    check importData(db = dbPath, table = "t_all", input = inJson, format = "json") == 1
    
    # Test JSON with bad row types (string instead of array/obj)
    writeFile(inJson, """["just_string"]""")
    check importData(db = dbPath, table = "t_all", input = inJson, format = "json") == 1

    # Test JSON column count mismatch (array has wrong len)
    writeFile(inJson, """[[1,2]]""")
    check importData(db = dbPath, table = "t_all", input = inJson, format = "json") == 1

    # Test CSV column count mismatch
    writeFile(inCsv, "id,name\n1,2,3\n")
    check importData(db = dbPath, table = "t_all", input = inCsv, format = "csv") == 1

    if fileExists(inCsv): removeFile(inCsv)
    if fileExists(inJson): removeFile(inJson)

