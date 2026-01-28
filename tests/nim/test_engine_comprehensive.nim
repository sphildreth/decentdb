import unittest
import os
import strutils

import engine
import pager/db_header
import vfs/os_vfs
import sql/sql
import record/record

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Engine Comprehensive":
  test "execSql handles UPDATE statements":
    let path = makeTempDb("decentdb_engine_update.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a table
    let createRes = execSql(db, "CREATE TABLE users (id INT, name TEXT)")
    check createRes.ok
    
    # Insert some data
    let insertRes = execSql(db, "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
    check insertRes.ok
    
    # Update a record
    let updateRes = execSql(db, "UPDATE users SET name = 'Charlie' WHERE id = 1")
    check updateRes.ok
    
    # Verify the update worked
    let selectRes = execSql(db, "SELECT name FROM users WHERE id = 1")
    check selectRes.ok
    check selectRes.value.len == 1
    check selectRes.value[0] == "Charlie"
    
    discard closeDb(db)

  test "execSql handles DELETE statements":
    let path = makeTempDb("decentdb_engine_delete.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a table
    let createRes = execSql(db, "CREATE TABLE users (id INT, name TEXT)")
    check createRes.ok
    
    # Insert some data
    let insertRes = execSql(db, "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
    check insertRes.ok
    
    # Delete a record
    let deleteRes = execSql(db, "DELETE FROM users WHERE id = 1")
    check deleteRes.ok
    
    # Verify the deletion worked
    let selectRes = execSql(db, "SELECT COUNT(*) FROM users")
    check selectRes.ok
    check selectRes.value.len == 1
    check split(selectRes.value[0], "|")[0] == "1"
    
    discard closeDb(db)

  test "execSql handles CREATE/DROP INDEX":
    let path = makeTempDb("decentdb_engine_index.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a table
    let createRes = execSql(db, "CREATE TABLE users (id INT, name TEXT)")
    check createRes.ok
    
    # Create an index
    let createIdxRes = execSql(db, "CREATE INDEX users_name_idx ON users (name)")
    check createIdxRes.ok
    
    # Drop the index
    let dropIdxRes = execSql(db, "DROP INDEX users_name_idx")
    check dropIdxRes.ok
    
    discard closeDb(db)

  test "execSql handles DROP TABLE":
    let path = makeTempDb("decentdb_engine_drop_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a table
    let createRes = execSql(db, "CREATE TABLE users (id INT, name TEXT)")
    check createRes.ok
    
    # Drop the table
    let dropRes = execSql(db, "DROP TABLE users")
    check dropRes.ok
    
    discard closeDb(db)

  test "Constraint enforcement - NOT NULL":
    let path = makeTempDb("decentdb_engine_not_null.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a table with NOT NULL constraint
    let createRes = execSql(db, "CREATE TABLE users (id INT, name TEXT NOT NULL)")
    check createRes.ok
    
    # Try to insert a row with NULL in NOT NULL column - should fail
    let insertRes = execSql(db, "INSERT INTO users (id, name) VALUES (1, NULL)")
    check not insertRes.ok
    
    discard closeDb(db)

  test "Constraint enforcement - UNIQUE":
    let path = makeTempDb("decentdb_engine_unique.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a table with UNIQUE constraint
    let createRes = execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE)")
    check createRes.ok
    
    # Insert first row
    let insert1Res = execSql(db, "INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
    check insert1Res.ok
    
    # Try to insert duplicate email - should fail
    let insert2Res = execSql(db, "INSERT INTO users (id, email) VALUES (2, 'alice@example.com')")
    check not insert2Res.ok
    
    discard closeDb(db)

  test "bulkLoad functionality":
    let path = makeTempDb("decentdb_engine_bulk_load.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a table
    let createRes = execSql(db, "CREATE TABLE users (id INT, name TEXT)")
    check createRes.ok
    
    # Prepare some data for bulk loading
    let rows = @[
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: @['A', 'l', 'i', 'c', 'e'])],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: @['B', 'o', 'b'])],
      @[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: @['C', 'h', 'a', 'r', 'l', 'i', 'e'])]
    ]
    
    # Perform bulk load
    let options = defaultBulkLoadOptions()
    let bulkRes = bulkLoad(db, "users", rows, options)
    check bulkRes.ok
    
    # Verify the data was loaded
    let selectRes = execSql(db, "SELECT COUNT(*) FROM users")
    check selectRes.ok
    check selectRes.value.len == 1
    check split(selectRes.value[0], "|")[0] == "3"
    
    discard closeDb(db)

  test "bulkLoad with constraints":
    let path = makeTempDb("decentdb_engine_bulk_load_constraints.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a table with constraints
    let createRes = execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE NOT NULL)")
    check createRes.ok
    
    # Prepare data with valid entries
    let rows = @[
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: @['a', '@', 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm'])],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: @['b', '@', 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm'])]
    ]
    
    # Perform bulk load - should succeed
    let options = defaultBulkLoadOptions()
    let bulkRes = bulkLoad(db, "users", rows, options)
    check bulkRes.ok
    
    # Try to load duplicate data - should fail
    let dupRows = @[
      @[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: @['a', '@', 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm'])]
    ]
    let dupBulkRes = bulkLoad(db, "users", dupRows, options)
    check not dupBulkRes.ok
    
    discard closeDb(db)

  test "closeDb on closed database":
    let path = makeTempDb("decentdb_engine_close_closed.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Close the database
    let closeRes = closeDb(db)
    check closeRes.ok
    
    # Try to close again - should handle gracefully
    let closeAgainRes = closeDb(db)
    check closeAgainRes.ok
    
    # Check that db is properly closed
    check not db.isOpen

  test "execSql with parameters":
    let path = makeTempDb("decentdb_engine_params.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a table
    let createRes = execSql(db, "CREATE TABLE users (id INT, name TEXT)")
    check createRes.ok
    
    # Insert with parameters
    let insertRes = execSql(db, "INSERT INTO users (id, name) VALUES (?, ?)", @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: @['T', 'e', 's', 't'])])
    check insertRes.ok
    
    # Verify insertion
    let selectRes = execSql(db, "SELECT name FROM users WHERE id = 1")
    check selectRes.ok
    check selectRes.value.len == 1
    check split(selectRes.value[0], "|")[0] == "Test"
    
    discard closeDb(db)

  test "Database with invalid path":
    let path = "/invalid/path/that/should/not/exist/decentdb_test.db"
    let dbRes = openDb(path)
    check not dbRes.ok

  test "execSql on closed database":
    let path = makeTempDb("decentdb_engine_closed_exec.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Close the database
    let closeRes = closeDb(db)
    check closeRes.ok
    
    # Try to execute SQL on closed database - should fail
    let execRes = execSql(db, "SELECT 1")
    check not execRes.ok
    
    # Reopen for cleanup
    let reopenRes = openDb(path)
    check reopenRes.ok
    discard closeDb(reopenRes.value)