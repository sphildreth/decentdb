import unittest
import os
import std/monotimes
import ../../src/engine
import ../../src/errors

suite "Engine In-Memory Tests":
  test "openDb with :memory: succeeds":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value
    require(db != nil)
    discard db.closeDb()

  test "multiple in-memory dbs are distinct":
    let db1Res = openDb(":memory:")
    require(db1Res.ok)
    let db2Res = openDb(":memory:")
    require(db2Res.ok)
    
    let db1 = db1Res.value
    let db2 = db2Res.value
    
    # Write to db1
    let createRes = execSql(db1, "CREATE TABLE t1 (id INT PRIMARY KEY)")
    require(createRes.ok)
    let insertRes = execSql(db1, "INSERT INTO t1 (id) VALUES (1)")
    require(insertRes.ok)
    
    # Read from db1 should work
    let sel1Res = execSql(db1, "SELECT id FROM t1")
    require(sel1Res.ok)
    
    # Read from db2 should fail (table doesn't exist)
    let sel2Res = execSql(db2, "SELECT id FROM t1")
    require(not sel2Res.ok)
    
    discard db1.closeDb()
    discard db2.closeDb()

  test "full DDL/DML lifecycle on :memory:":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value

    # CREATE TABLE
    require(execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT)").ok)

    # INSERT
    require(execSql(db, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)").ok)
    require(execSql(db, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)").ok)
    require(execSql(db, "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)").ok)

    # SELECT
    let selRes = execSql(db, "SELECT name FROM users ORDER BY id")
    require(selRes.ok)
    check(selRes.value.len == 3)
    check(selRes.value[0] == "Alice")
    check(selRes.value[1] == "Bob")
    check(selRes.value[2] == "Charlie")

    # UPDATE
    require(execSql(db, "UPDATE users SET age = 31 WHERE id = 1").ok)
    let updRes = execSql(db, "SELECT age FROM users WHERE id = 1")
    require(updRes.ok)
    check(updRes.value[0] == "31")

    # DELETE
    require(execSql(db, "DELETE FROM users WHERE id = 2").ok)
    let delRes = execSql(db, "SELECT id FROM users ORDER BY id")
    require(delRes.ok)
    check(delRes.value.len == 2)
    check(delRes.value[0] == "1")
    check(delRes.value[1] == "3")

    # closeDb
    let closeRes = db.closeDb()
    check(closeRes.ok)

  test "checkpoint round-trip on :memory:":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value

    require(execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, val TEXT)").ok)
    for i in 1..20:
      require(execSql(db, "INSERT INTO items (id, val) VALUES (" & $i & ", 'item" & $i & "')").ok)

    # Force checkpoint — exercises WAL->pager writeback through MemVfs without mmap
    let ckRes = db.checkpointDb()
    require(ckRes.ok)

    # Verify data survives checkpoint
    let selRes = execSql(db, "SELECT val FROM items WHERE id = 10")
    require(selRes.ok)
    check(selRes.value.len == 1)
    check(selRes.value[0] == "item10")

    # Insert more after checkpoint and verify
    require(execSql(db, "INSERT INTO items (id, val) VALUES (100, 'post-ckpt')").ok)
    let sel2Res = execSql(db, "SELECT val FROM items WHERE id = 100")
    require(sel2Res.ok)
    check(sel2Res.value[0] == "post-ckpt")

    discard db.closeDb()

  test "new :memory: after close is fresh":
    # First DB — create and populate
    let db1Res = openDb(":memory:")
    require(db1Res.ok)
    let db1 = db1Res.value
    require(execSql(db1, "CREATE TABLE t1 (id INT PRIMARY KEY)").ok)
    require(execSql(db1, "INSERT INTO t1 (id) VALUES (42)").ok)
    discard db1.closeDb()

    # Second DB — must be completely fresh
    let db2Res = openDb(":memory:")
    require(db2Res.ok)
    let db2 = db2Res.value
    let selRes = execSql(db2, "SELECT id FROM t1")
    check(not selRes.ok)  # table must not exist
    discard db2.closeDb()

  test "transaction commit and rollback on :memory:":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value
    require(execSql(db, "CREATE TABLE t1 (id INT PRIMARY KEY, v INT)").ok)

    # Commit path
    require(db.beginTransaction().ok)
    require(execSql(db, "INSERT INTO t1 (id, v) VALUES (1, 10)").ok)
    require(db.commitTransaction().ok)
    let sel1 = execSql(db, "SELECT v FROM t1 WHERE id = 1")
    require(sel1.ok)
    check(sel1.value[0] == "10")

    # Rollback path
    require(db.beginTransaction().ok)
    require(execSql(db, "INSERT INTO t1 (id, v) VALUES (2, 20)").ok)
    require(db.rollbackTransaction().ok)
    let sel2 = execSql(db, "SELECT id FROM t1")
    require(sel2.ok)
    check(sel2.value.len == 1)  # only id=1 committed

    discard db.closeDb()

  test "secondary index on :memory:":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value
    require(execSql(db, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)").ok)
    require(execSql(db, "CREATE INDEX idx_price ON products (price)").ok)
    require(execSql(db, "INSERT INTO products (id, name, price) VALUES (1, 'A', 50)").ok)
    require(execSql(db, "INSERT INTO products (id, name, price) VALUES (2, 'B', 30)").ok)
    require(execSql(db, "INSERT INTO products (id, name, price) VALUES (3, 'C', 50)").ok)

    let selRes = execSql(db, "SELECT name FROM products WHERE price = 50 ORDER BY name")
    require(selRes.ok)
    check(selRes.value.len == 2)
    check(selRes.value[0] == "A")
    check(selRes.value[1] == "C")

    discard db.closeDb()

  test "JOIN on :memory:":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value
    require(execSql(db, "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT)").ok)
    require(execSql(db, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)").ok)
    require(execSql(db, "INSERT INTO customers (id, name) VALUES (1, 'Alice')").ok)
    require(execSql(db, "INSERT INTO customers (id, name) VALUES (2, 'Bob')").ok)
    require(execSql(db, "INSERT INTO orders (id, customer_id) VALUES (10, 1)").ok)
    require(execSql(db, "INSERT INTO orders (id, customer_id) VALUES (11, 1)").ok)
    require(execSql(db, "INSERT INTO orders (id, customer_id) VALUES (12, 2)").ok)

    let selRes = execSql(db, "SELECT c.name, o.id FROM orders o INNER JOIN customers c ON c.id = o.customer_id ORDER BY o.id")
    require(selRes.ok)
    check(selRes.value.len == 3)  # 3 rows (pipe-delimited columns)
    check(selRes.value[0] == "Alice|10")
    check(selRes.value[1] == "Alice|11")
    check(selRes.value[2] == "Bob|12")

    discard db.closeDb()

  # =========================================================================
  # SaveAs tests
  # =========================================================================

  test "saveAs exports :memory: to disk and reopens":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value
    require(execSql(db, "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)").ok)
    require(execSql(db, "INSERT INTO t1 (id, name) VALUES (1, 'Alice')").ok)
    require(execSql(db, "INSERT INTO t1 (id, name) VALUES (2, 'Bob')").ok)

    let destPath = "/tmp/test_saveas_basic_" & $getMonoTime().ticks & ".ddb"
    let saRes = db.saveAs(destPath)
    require(saRes.ok)
    discard db.closeDb()

    # Reopen from disk and verify
    let db2Res = openDb(destPath)
    require(db2Res.ok)
    let db2 = db2Res.value
    let selRes = execSql(db2, "SELECT name FROM t1 ORDER BY id")
    require(selRes.ok)
    check(selRes.value.len == 2)
    check(selRes.value[0] == "Alice")
    check(selRes.value[1] == "Bob")
    discard db2.closeDb()

    try: removeFile(destPath)
    except: discard
    try: removeFile(destPath & "-wal")
    except: discard

  test "saveAs preserves indexes":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value
    require(execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, price INT, name TEXT)").ok)
    require(execSql(db, "CREATE INDEX idx_price ON items (price)").ok)
    require(execSql(db, "INSERT INTO items (id, price, name) VALUES (1, 100, 'A')").ok)
    require(execSql(db, "INSERT INTO items (id, price, name) VALUES (2, 200, 'B')").ok)
    require(execSql(db, "INSERT INTO items (id, price, name) VALUES (3, 100, 'C')").ok)

    let destPath = "/tmp/test_saveas_indexes_" & $getMonoTime().ticks & ".ddb"
    require(db.saveAs(destPath).ok)
    discard db.closeDb()

    let db2Res = openDb(destPath)
    require(db2Res.ok)
    let db2 = db2Res.value
    let selRes = execSql(db2, "SELECT name FROM items WHERE price = 100 ORDER BY name")
    require(selRes.ok)
    check(selRes.value.len == 2)
    check(selRes.value[0] == "A")
    check(selRes.value[1] == "C")
    discard db2.closeDb()

    try: removeFile(destPath)
    except: discard
    try: removeFile(destPath & "-wal")
    except: discard

  test "saveAs empty database":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value

    let destPath = "/tmp/test_saveas_empty_" & $getMonoTime().ticks & ".ddb"
    let saRes = db.saveAs(destPath)
    require(saRes.ok)
    discard db.closeDb()

    let db2Res = openDb(destPath)
    require(db2Res.ok)
    let db2 = db2Res.value
    let selRes = execSql(db2, "SELECT id FROM nonexistent")
    check(not selRes.ok)
    discard db2.closeDb()

    try: removeFile(destPath)
    except: discard
    try: removeFile(destPath & "-wal")
    except: discard

  test "saveAs errors if dest already exists":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value

    let destPath = "/tmp/test_saveas_exists_" & $getMonoTime().ticks & ".ddb"
    writeFile(destPath, "placeholder")

    let saRes = db.saveAs(destPath)
    check(not saRes.ok)
    check(saRes.err.code == ERR_IO)
    discard db.closeDb()

    try: removeFile(destPath)
    except: discard

  test "saveAs errors during active transaction":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value
    require(execSql(db, "CREATE TABLE t1 (id INT PRIMARY KEY)").ok)
    require(db.beginTransaction().ok)
    require(execSql(db, "INSERT INTO t1 (id) VALUES (1)").ok)

    let destPath = "/tmp/test_saveas_txn_" & $getMonoTime().ticks & ".ddb"
    let saRes = db.saveAs(destPath)
    check(not saRes.ok)
    check(saRes.err.code == ERR_TRANSACTION)

    require(db.rollbackTransaction().ok)
    discard db.closeDb()

  test "saveAs after checkpoint":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value
    require(execSql(db, "CREATE TABLE t1 (id INT PRIMARY KEY, v TEXT)").ok)
    for i in 1..50:
      require(execSql(db, "INSERT INTO t1 (id, v) VALUES (" & $i & ", 'row" & $i & "')").ok)

    require(db.checkpointDb().ok)

    let destPath = "/tmp/test_saveas_postckpt_" & $getMonoTime().ticks & ".ddb"
    require(db.saveAs(destPath).ok)
    discard db.closeDb()

    let db2Res = openDb(destPath)
    require(db2Res.ok)
    let db2 = db2Res.value
    let selRes = execSql(db2, "SELECT v FROM t1 WHERE id = 25")
    require(selRes.ok)
    check(selRes.value[0] == "row25")
    let cntRes = execSql(db2, "SELECT id FROM t1")
    require(cntRes.ok)
    check(cntRes.value.len == 50)
    discard db2.closeDb()

    try: removeFile(destPath)
    except: discard
    try: removeFile(destPath & "-wal")
    except: discard

  test "saveAs with multiple tables and foreign keys":
    let dbRes = openDb(":memory:")
    require(dbRes.ok)
    let db = dbRes.value
    require(execSql(db, "CREATE TABLE authors (id INT PRIMARY KEY, name TEXT NOT NULL)").ok)
    require(execSql(db, "CREATE TABLE books (id INT PRIMARY KEY, title TEXT, author_id INT REFERENCES authors(id))").ok)
    require(execSql(db, "INSERT INTO authors (id, name) VALUES (1, 'Tolkien')").ok)
    require(execSql(db, "INSERT INTO authors (id, name) VALUES (2, 'Asimov')").ok)
    require(execSql(db, "INSERT INTO books (id, title, author_id) VALUES (10, 'The Hobbit', 1)").ok)
    require(execSql(db, "INSERT INTO books (id, title, author_id) VALUES (11, 'Foundation', 2)").ok)

    let destPath = "/tmp/test_saveas_fk_" & $getMonoTime().ticks & ".ddb"
    require(db.saveAs(destPath).ok)
    discard db.closeDb()

    let db2Res = openDb(destPath)
    require(db2Res.ok)
    let db2 = db2Res.value
    let selRes = execSql(db2, "SELECT b.title, a.name FROM books b INNER JOIN authors a ON a.id = b.author_id ORDER BY b.id")
    require(selRes.ok)
    check(selRes.value.len == 2)
    check(selRes.value[0] == "The Hobbit|Tolkien")
    check(selRes.value[1] == "Foundation|Asimov")
    discard db2.closeDb()

    try: removeFile(destPath)
    except: discard
    try: removeFile(destPath & "-wal")
    except: discard
