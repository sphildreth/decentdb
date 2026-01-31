import unittest
import os
import engine
import pager/pager
import btree/btree
import pager/db_header
import errors
import record/record
import sql/sql
import sql/binder
import catalog/catalog
import storage/storage
import exec/exec
import planner/planner

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  path

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "Engine Comprehensive Extended":
  test "execSql with complex transaction scenarios":
    let path = makeTempDb("decentdb_engine_complex_tx.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create a table
    let createRes = execSql(db, "CREATE TABLE test_tx (id INT PRIMARY KEY, name TEXT)")
    check createRes.ok

    # Begin explicit transaction
    let beginRes = execSql(db, "BEGIN")
    check beginRes.ok

    # Insert some data
    let insertRes = execSql(db, "INSERT INTO test_tx VALUES (1, 'Alice')")
    check insertRes.ok

    # Query within transaction
    let selectRes = execSql(db, "SELECT * FROM test_tx")
    check selectRes.ok
    check selectRes.value.len == 1
    check selectRes.value[0] == "1|Alice"

    # Commit transaction
    let commitRes = execSql(db, "COMMIT")
    check commitRes.ok

    # Verify data persists after commit
    let afterCommitRes = execSql(db, "SELECT * FROM test_tx")
    check afterCommitRes.ok
    check afterCommitRes.value.len == 1
    check afterCommitRes.value[0] == "1|Alice"

    # Test nested transaction error
    let begin2Res = execSql(db, "BEGIN")
    check begin2Res.ok

    let nestedBeginRes = execSql(db, "BEGIN")  # Should fail
    check not nestedBeginRes.ok

    let rollbackRes = execSql(db, "ROLLBACK")
    check rollbackRes.ok

    let closeRes = closeDb(db)
    check closeRes.ok

  test "execSql with parameterized queries":
    let path = makeTempDb("decentdb_engine_params.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table
    let createRes = execSql(db, "CREATE TABLE params_test (id INT PRIMARY KEY, name TEXT, age INT)")
    check createRes.ok

    # Insert with parameters
    let insertRes = execSql(db, "INSERT INTO params_test VALUES ($1, $2, $3)", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: toBytes("John")),
      Value(kind: vkInt64, int64Val: 25)
    ])
    check insertRes.ok

    # Select with parameters
    let selectRes = execSql(db, "SELECT * FROM params_test WHERE id = $1", @[
      Value(kind: vkInt64, int64Val: 1)
    ])
    check selectRes.ok
    check selectRes.value.len == 1
    check selectRes.value[0] == "1|John|25"

    # Update with parameters
    let updateRes = execSql(db, "UPDATE params_test SET age = $1 WHERE id = $2", @[
      Value(kind: vkInt64, int64Val: 30),
      Value(kind: vkInt64, int64Val: 1)
    ])
    check updateRes.ok

    # Verify update
    let verifyRes = execSql(db, "SELECT age FROM params_test WHERE id = 1")
    check verifyRes.ok
    check verifyRes.value.len == 1
    check verifyRes.value[0] == "30"

    let closeRes = closeDb(db)
    check closeRes.ok

  test "execPreparedNonSelect functionality":
    let path = makeTempDb("decentdb_engine_prepared.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table
    let createRes = execSql(db, "CREATE TABLE prepared_test (id INT PRIMARY KEY, value TEXT)")
    check createRes.ok

    # Parse and bind a statement
    let parseRes = parseSql("INSERT INTO prepared_test VALUES ($1, $2)")
    check parseRes.ok
    check parseRes.value.statements.len == 1

    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check bindRes.ok

    # Execute prepared non-select
    let execRes = execPreparedNonSelect(db, bindRes.value, @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: toBytes("test"))
    ])
    check execRes.ok
    check execRes.value == 1  # One row affected

    # Verify insertion
    let verifyRes = execSql(db, "SELECT * FROM prepared_test")
    check verifyRes.ok
    check verifyRes.value.len == 1
    check verifyRes.value[0] == "1|test"

    let closeRes = closeDb(db)
    check closeRes.ok

  test "bulkLoad with various options":
    let path = makeTempDb("decentdb_engine_bulk.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table
    let createRes = execSql(db, "CREATE TABLE bulk_test (id INT PRIMARY KEY, name TEXT)")
    check createRes.ok

    # Prepare some data for bulk load
    var rows: seq[seq[Value]] = @[]
    for i in 1 .. 100:
      rows.add(@[
        Value(kind: vkInt64, int64Val: i),
        Value(kind: vkText, bytes: toBytes("Name" & $i))
      ])

    # Test bulk load with default options
    var opts = defaultBulkLoadOptions()
    opts.batchSize = 10
    opts.durability = dmDeferred
    let bulkRes = bulkLoad(db, "bulk_test", rows, opts)
    check bulkRes.ok

    # Verify bulk loaded data
    let countRes = execSql(db, "SELECT COUNT(*) FROM bulk_test")
    check countRes.ok
    check countRes.value.len == 1
    check countRes.value[0] == "100"

    let closeRes = closeDb(db)
    check closeRes.ok

  test "bulkLoad with constraints":
    let path = makeTempDb("decentdb_engine_bulk_constraints.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table with unique constraint
    let createRes = execSql(db, "CREATE TABLE unique_test (id INT PRIMARY KEY, email TEXT UNIQUE)")
    check createRes.ok

    # Try to bulk load data with duplicate emails (should fail)
    let rows = @[
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("test@example.com"))],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: toBytes("test@example.com"))]  # Duplicate
    ]

    var opts = defaultBulkLoadOptions()
    opts.durability = dmNone
    let bulkRes = bulkLoad(db, "unique_test", rows, opts)
    check not bulkRes.ok  # Should fail due to unique constraint

    # Bulk load valid data
    let validRows = @[
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("alice@example.com"))],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: toBytes("bob@example.com"))]
    ]
    let validBulkRes = bulkLoad(db, "unique_test", validRows, opts)
    check validBulkRes.ok

    # Verify valid data was loaded
    let countRes = execSql(db, "SELECT COUNT(*) FROM unique_test")
    check countRes.ok
    check countRes.value.len == 1
    check countRes.value[0] == "2"

    let closeRes = closeDb(db)
    check closeRes.ok

  test "transaction rollback functionality":
    let path = makeTempDb("decentdb_engine_rollback.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table
    let createRes = execSql(db, "CREATE TABLE rollback_test (id INT PRIMARY KEY, value TEXT)")
    check createRes.ok

    # Insert initial data
    let insertInitRes = execSql(db, "INSERT INTO rollback_test VALUES (1, 'initial')")
    check insertInitRes.ok

    # Begin transaction
    let beginRes = beginTransaction(db)
    check beginRes.ok

    # Insert data within transaction
    let insertTxRes = execSql(db, "INSERT INTO rollback_test VALUES (2, 'during_tx')")
    check insertTxRes.ok

    # Verify data is visible within transaction
    let selectDuringRes = execSql(db, "SELECT * FROM rollback_test ORDER BY id")
    check selectDuringRes.ok
    check selectDuringRes.value.len == 2
    check selectDuringRes.value[0] == "1|initial"
    check selectDuringRes.value[1] == "2|during_tx"

    # Rollback transaction
    let rollbackRes = rollbackTransaction(db)
    check rollbackRes.ok

    # Verify rolled back data is not visible
    let selectAfterRes = execSql(db, "SELECT * FROM rollback_test ORDER BY id")
    check selectAfterRes.ok
    check selectAfterRes.value.len == 1
    check selectAfterRes.value[0] == "1|initial"

    let closeRes = closeDb(db)
    check closeRes.ok

  test "transaction commit functionality":
    let path = makeTempDb("decentdb_engine_commit.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table
    let createRes = execSql(db, "CREATE TABLE commit_test (id INT PRIMARY KEY, value TEXT)")
    check createRes.ok

    # Begin transaction
    let beginRes = beginTransaction(db)
    check beginRes.ok

    # Insert data within transaction
    let insertTxRes = execSql(db, "INSERT INTO commit_test VALUES (1, 'committed')")
    check insertTxRes.ok

    # Commit transaction
    let commitRes = commitTransaction(db)
    check commitRes.ok

    # Verify committed data is visible
    let selectAfterRes = execSql(db, "SELECT * FROM commit_test ORDER BY id")
    check selectAfterRes.ok
    check selectAfterRes.value.len == 1
    check selectAfterRes.value[0] == "1|committed"

    let closeRes = closeDb(db)
    check closeRes.ok

  test "foreign key constraint enforcement":
    let path = makeTempDb("decentdb_engine_fk.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create parent table
    let createParentRes = execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)")
    check createParentRes.ok

    # Create child table with foreign key
    let createChildRes = execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id), value TEXT)")
    check createChildRes.ok

    # Insert parent data
    let insertParentRes = execSql(db, "INSERT INTO parent VALUES (1, 'parent1')")
    check insertParentRes.ok

    # Insert child data with valid foreign key
    let insertValidChildRes = execSql(db, "INSERT INTO child VALUES (1, 1, 'child1')")
    check insertValidChildRes.ok

    # Try to insert child with invalid foreign key (should fail)
    let insertInvalidChildRes = execSql(db, "INSERT INTO child VALUES (2, 999, 'child2')")
    check not insertInvalidChildRes.ok  # Should fail due to FK constraint

    # Try to delete parent with children (RESTRICT should prevent this)
    let deleteParentRes = execSql(db, "DELETE FROM parent WHERE id = 1")
    check not deleteParentRes.ok  # Should fail due to FK RESTRICT

    # Update parent with children (RESTRICT should prevent this)
    let updateParentRes = execSql(db, "UPDATE parent SET id = 2 WHERE id = 1")
    check not updateParentRes.ok  # Should fail due to FK RESTRICT

    let closeRes = closeDb(db)
    check closeRes.ok

  test "closeDb on already closed database":
    let path = makeTempDb("decentdb_engine_close_closed.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Close the database
    let firstCloseRes = closeDb(db)
    check firstCloseRes.ok

    # Try to close again (should handle gracefully)
    let secondCloseRes = closeDb(db)
    # This behavior depends on implementation - may or may not succeed

    if fileExists(path):
      removeFile(path)
    if fileExists(path & "-wal"):
      removeFile(path & "-wal")

  test "execSql on closed database":
    let path = makeTempDb("decentdb_engine_closed.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Close the database
    let closeRes = closeDb(db)
    check closeRes.ok

    # Try to execute SQL on closed database (should fail)
    let execRes = execSql(db, "SELECT 1")
    check not execRes.ok

    if fileExists(path):
      removeFile(path)
    if fileExists(path & "-wal"):
      removeFile(path & "-wal")

  test "checkpointDb functionality":
    let path = makeTempDb("decentdb_engine_checkpoint.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create and populate table
    let createRes = execSql(db, "CREATE TABLE checkpoint_test (id INT PRIMARY KEY, data TEXT)")
    check createRes.ok

    for i in 1 .. 10:
      let insertRes = execSql(db, "INSERT INTO checkpoint_test VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: i),
        Value(kind: vkText, bytes: toBytes("data" & $i))
      ])
      check insertRes.ok

    # Force a checkpoint
    let checkpointRes = checkpointDb(db)
    check checkpointRes.ok

    # Verify data still accessible after checkpoint
    let selectRes = execSql(db, "SELECT COUNT(*) FROM checkpoint_test")
    check selectRes.ok
    check selectRes.value.len == 1
    check selectRes.value[0] == "10"

    let closeRes = closeDb(db)
    check closeRes.ok

  test "bulkLoad with disabled indexes":
    let path = makeTempDb("decentdb_engine_bulk_no_indexes.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table with index
    let createRes = execSql(db, "CREATE TABLE no_index_test (id INT PRIMARY KEY, value TEXT)")
    check createRes.ok

    let createIdxRes = execSql(db, "CREATE INDEX idx_value ON no_index_test(value)")
    check createIdxRes.ok

    # Prepare data
    var rows: seq[seq[Value]] = @[]
    for i in 1 .. 50:
      rows.add(@[
        Value(kind: vkInt64, int64Val: i),
        Value(kind: vkText, bytes: toBytes("value" & $i))
      ])

    # Bulk load with indexes disabled (for performance)
    var opts = defaultBulkLoadOptions()
    opts.disableIndexes = true
    opts.durability = dmNone
    let bulkRes = bulkLoad(db, "no_index_test", rows, opts)
    check bulkRes.ok

    # Verify data was loaded
    let countRes = execSql(db, "SELECT COUNT(*) FROM no_index_test")
    check countRes.ok
    check countRes.value.len == 1
    check countRes.value[0] == "50"

    let closeRes = closeDb(db)
    check closeRes.ok

  test "execPreparedNonSelect with various statement types":
    let path = makeTempDb("decentdb_engine_prepared_various.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Test CREATE TABLE
    let createParseRes = parseSql("CREATE TABLE prepared_various (id INT PRIMARY KEY, name TEXT)")
    check createParseRes.ok
    let createBindRes = bindStatement(db.catalog, createParseRes.value.statements[0])
    check createBindRes.ok
    let createExecRes = execPreparedNonSelect(db, createBindRes.value, @[])
    check createExecRes.ok

    # Test INSERT
    let insertParseRes = parseSql("INSERT INTO prepared_various VALUES ($1, $2)")
    check insertParseRes.ok
    let insertBindRes = bindStatement(db.catalog, insertParseRes.value.statements[0])
    check insertBindRes.ok
    let insertExecRes = execPreparedNonSelect(db, insertBindRes.value, @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: toBytes("test"))
    ])
    check insertExecRes.ok
    check insertExecRes.value == 1

    # Test UPDATE
    let updateParseRes = parseSql("UPDATE prepared_various SET name = $1 WHERE id = $2")
    check updateParseRes.ok
    let updateBindRes = bindStatement(db.catalog, updateParseRes.value.statements[0])
    check updateBindRes.ok
    let updateExecRes = execPreparedNonSelect(db, updateBindRes.value, @[
      Value(kind: vkText, bytes: toBytes("updated")),
      Value(kind: vkInt64, int64Val: 1)
    ])
    check updateExecRes.ok
    check updateExecRes.value == 1

    # Test DELETE
    let deleteParseRes = parseSql("DELETE FROM prepared_various WHERE id = $1")
    check deleteParseRes.ok
    let deleteBindRes = bindStatement(db.catalog, deleteParseRes.value.statements[0])
    check deleteBindRes.ok
    let deleteExecRes = execPreparedNonSelect(db, deleteBindRes.value, @[
      Value(kind: vkInt64, int64Val: 1)
    ])
    check deleteExecRes.ok
    check deleteExecRes.value == 1

    let closeRes = closeDb(db)
    check closeRes.ok

  test "execPreparedNonSelect with invalid statement type":
    let path = makeTempDb("decentdb_engine_prepared_invalid.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table first
    let createRes = execSql(db, "CREATE TABLE invalid_test (id INT PRIMARY KEY)")
    check createRes.ok

    # Try to use SELECT with execPreparedNonSelect (should fail)
    let selectParseRes = parseSql("SELECT * FROM invalid_test")
    check selectParseRes.ok
    let selectBindRes = bindStatement(db.catalog, selectParseRes.value.statements[0])
    check selectBindRes.ok
    let selectExecRes = execPreparedNonSelect(db, selectBindRes.value, @[])
    check not selectExecRes.ok  # Should fail because it's a SELECT

    let closeRes = closeDb(db)
    check closeRes.ok