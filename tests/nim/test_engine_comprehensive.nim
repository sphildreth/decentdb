import unittest
import os
import strutils

import engine
import record/record

proc makeTempDb(name: string): string =
  let normalizedName =
    if name.len >= 3 and name[name.len - 3 .. ^1] == ".db":
      name[0 .. ^4] & ".ddb"
    else:
      name
  let path = getTempDir() / normalizedName
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

proc bytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

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
    check execSql(db, "INSERT INTO users (id, name) VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO users (id, name) VALUES (2, 'Bob')").ok
    
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
    check execSql(db, "INSERT INTO users (id, name) VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO users (id, name) VALUES (2, 'Bob')").ok
    
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
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: bytes("Alice"))],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: bytes("Bob"))],
      @[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: bytes("Charlie"))]
    ]
    
    # Perform bulk load
    let options = defaultBulkLoadOptions()
    let bulkRes = bulkLoad(db, "users", rows, options, db.wal)
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
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: bytes("a@example.com"))],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: bytes("b@example.com"))]
    ]
    
    # Perform bulk load - should succeed
    let options = defaultBulkLoadOptions()
    let bulkRes = bulkLoad(db, "users", rows, options, db.wal)
    check bulkRes.ok
    
    # Try to load duplicate data - should fail
    let dupRows = @[
      @[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: bytes("a@example.com"))]
    ]
    let dupBulkRes = bulkLoad(db, "users", dupRows, options, db.wal)
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
    let insertRes = execSql(db, "INSERT INTO users (id, name) VALUES ($1, $2)", @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: bytes("Test"))])
    check insertRes.ok
    
    # Verify insertion
    let selectRes = execSql(db, "SELECT name FROM users WHERE id = 1")
    check selectRes.ok
    check selectRes.value.len == 1
    check split(selectRes.value[0], "|")[0] == "Test"
    
    discard closeDb(db)

  test "Database with invalid path":
    let path = "/invalid/path/that/should/not/exist/decentdb_test.ddb"
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

  test "explicit transactions handle nested begin/commit and rollback":
    let path = makeTempDb("decentdb_engine_txn_lifecycle.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE counters (id INT)").ok

    check beginTransaction(db).ok
    check not beginTransaction(db).ok
    check execSql(db, "INSERT INTO counters (id) VALUES (1)").ok
    check commitTransaction(db).ok

    check beginTransaction(db).ok
    check execSql(db, "INSERT INTO counters (id) VALUES (2)").ok
    check rollbackTransaction(db).ok
    check not rollbackTransaction(db).ok

    let countRes = execSql(db, "SELECT COUNT(*) FROM counters")
    check countRes.ok
    check split(countRes.value[0], "|")[0] == "1"

    discard closeDb(db)

  test "foreign key restrict blocks parent update/delete":
    let path = makeTempDb("decentdb_engine_fk_restrict.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))").ok
    check execSql(db, "INSERT INTO parent (id) VALUES (1)").ok
    check execSql(db, "INSERT INTO child (id, parent_id) VALUES (1, 1)").ok

    let updateRes = execSql(db, "UPDATE parent SET id = 2 WHERE id = 1")
    check not updateRes.ok
    check updateRes.err.message.contains("FOREIGN KEY")

    let deleteRes = execSql(db, "DELETE FROM parent WHERE id = 1")
    check not deleteRes.ok
    check deleteRes.err.message.contains("FOREIGN KEY")

    discard closeDb(db)

  test "bulkLoad rebuilds indexes when durability is full":
    let path = makeTempDb("decentdb_engine_bulk_load_indexes.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE NOT NULL)").ok
    let rows = @[
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: bytes("a@example.com"))],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: bytes("b@example.com"))],
      @[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: bytes("c@example.com"))]
    ]

    var opts = defaultBulkLoadOptions()
    opts.disableIndexes = false
    opts.durability = dmFull
    opts.batchSize = 2
    opts.syncInterval = 1
    opts.checkpointOnComplete = false

    let bulkRes = bulkLoad(db, "users", rows, opts, db.wal)
    check bulkRes.ok

    let selectRes = execSql(db, "SELECT COUNT(*) FROM users")
    check selectRes.ok
    check split(selectRes.value[0], "|")[0] == "3"

    discard closeDb(db)

  test "bulkLoad skips WAL when durability is none":
    let path = makeTempDb("decentdb_engine_bulk_load_nosync.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE items (id INT, label TEXT)").ok
    let rows = @[
      @[Value(kind: vkInt64, int64Val: 10), Value(kind: vkText, bytes: bytes("x"))],
      @[Value(kind: vkInt64, int64Val: 20), Value(kind: vkText, bytes: bytes("y"))]
    ]

    var opts = defaultBulkLoadOptions()
    opts.durability = dmNone

    let bulkRes = bulkLoad(db, "items", rows, opts, nil)
    check bulkRes.ok

    let selectRes = execSql(db, "SELECT COUNT(*) FROM items")
    check selectRes.ok
    check split(selectRes.value[0], "|")[0] == "2"

    discard closeDb(db)

  test "INSERT auto-increment for INTEGER PRIMARY KEY":
    let path = makeTempDb("decentdb_engine_auto_inc.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let createRes = execSql(db, "CREATE TABLE autoinc (id INTEGER PRIMARY KEY, name TEXT)")
    check createRes.ok

    # Insert without specifying id — should auto-assign
    let ins1 = execSql(db, "INSERT INTO autoinc (name) VALUES ('Alice')")
    check ins1.ok

    let ins2 = execSql(db, "INSERT INTO autoinc (name) VALUES ('Bob')")
    check ins2.ok

    # Insert with explicit id
    let ins3 = execSql(db, "INSERT INTO autoinc (id, name) VALUES (10, 'Carol')")
    check ins3.ok

    # Check all rows
    let selRes = execSql(db, "SELECT id, name FROM autoinc ORDER BY id")
    check selRes.ok
    check selRes.value.len == 3

    # Auto-assigned ids should be 1 and 2
    let row0 = split(selRes.value[0], "|")
    check row0[0] == "1"
    check row0[1] == "Alice"

    let row1 = split(selRes.value[1], "|")
    check row1[0] == "2"
    check row1[1] == "Bob"

    let row2 = split(selRes.value[2], "|")
    check row2[0] == "10"
    check row2[1] == "Carol"

    discard closeDb(db)

  test "INSERT RETURNING with auto-increment":
    let path = makeTempDb("decentdb_engine_auto_inc_ret.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let createRes = execSql(db, "CREATE TABLE rettest (id INTEGER PRIMARY KEY, val TEXT)")
    check createRes.ok

    let insRes = execSql(db, "INSERT INTO rettest (val) VALUES ('hello') RETURNING id")
    check insRes.ok
    check insRes.value.len == 1
    let returnedId = split(insRes.value[0], "|")[0]
    check returnedId == "1"

    let ins2Res = execSql(db, "INSERT INTO rettest (val) VALUES ('world') RETURNING id, val")
    check ins2Res.ok
    check ins2Res.value.len == 1
    let row = split(ins2Res.value[0], "|")
    check row[0] == "2"
    check row[1] == "world"

    discard closeDb(db)

  test "auto-increment does not skip NOT NULL on non-PK columns":
    let path = makeTempDb("decentdb_engine_auto_inc_notnull.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let createRes = execSql(db, "CREATE TABLE strict (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    check createRes.ok

    # Omitting name (NOT NULL) should fail
    let insRes = execSql(db, "INSERT INTO strict (id) VALUES (1)")
    check not insRes.ok

    discard closeDb(db)
