import unittest
import os
import strutils
import ../../src/engine

proc makeTempDb(name: string): string =
  let dir = getTempDir()
  let path = dir / name
  if fileExists(path): removeFile(path)
  let walPath = path & "-wal"
  if fileExists(walPath): removeFile(walPath)
  path

suite "Composite Primary Keys":
  test "CREATE TABLE with composite PK and INSERT":
    let path = makeTempDb("decentdb_composite_pk_basic.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let createRes = execSql(db, "CREATE TABLE enrollment (student_id INT NOT NULL, course_id INT NOT NULL, grade TEXT, PRIMARY KEY (student_id, course_id))")
    check createRes.ok

    let ins1 = execSql(db, "INSERT INTO enrollment (student_id, course_id, grade) VALUES (1, 101, 'A')")
    check ins1.ok

    let ins2 = execSql(db, "INSERT INTO enrollment (student_id, course_id, grade) VALUES (1, 102, 'B')")
    check ins2.ok

    let ins3 = execSql(db, "INSERT INTO enrollment (student_id, course_id, grade) VALUES (2, 101, 'C')")
    check ins3.ok

    let selRes = execSql(db, "SELECT student_id, course_id, grade FROM enrollment ORDER BY student_id, course_id")
    check selRes.ok
    check selRes.value.len == 3
    check selRes.value[0] == "1|101|A"
    check selRes.value[1] == "1|102|B"
    check selRes.value[2] == "2|101|C"

    discard closeDb(db)
    removeFile(path)

  test "composite PK rejects duplicate combination":
    let path = makeTempDb("decentdb_composite_pk_dup.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let createRes = execSql(db, "CREATE TABLE enrollment (student_id INT NOT NULL, course_id INT NOT NULL, PRIMARY KEY (student_id, course_id))")
    check createRes.ok

    let ins1 = execSql(db, "INSERT INTO enrollment (student_id, course_id) VALUES (1, 101)")
    check ins1.ok

    # Same combination should fail
    let ins2 = execSql(db, "INSERT INTO enrollment (student_id, course_id) VALUES (1, 101)")
    check not ins2.ok
    check "UNIQUE constraint failed" in ins2.err.message

    discard closeDb(db)
    removeFile(path)

  test "composite PK allows same value in different columns":
    let path = makeTempDb("decentdb_composite_pk_partial.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let createRes = execSql(db, "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))")
    check createRes.ok

    let ins1 = execSql(db, "INSERT INTO t (a, b) VALUES (1, 2)")
    check ins1.ok
    let ins2 = execSql(db, "INSERT INTO t (a, b) VALUES (1, 3)")
    check ins2.ok
    let ins3 = execSql(db, "INSERT INTO t (a, b) VALUES (2, 2)")
    check ins3.ok

    let selRes = execSql(db, "SELECT a, b FROM t ORDER BY a, b")
    check selRes.ok
    check selRes.value.len == 3

    discard closeDb(db)
    removeFile(path)

  test "UPDATE on composite PK table":
    let path = makeTempDb("decentdb_composite_pk_update.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE enrollment (student_id INT NOT NULL, course_id INT NOT NULL, grade TEXT, PRIMARY KEY (student_id, course_id))")
    discard execSql(db, "INSERT INTO enrollment (student_id, course_id, grade) VALUES (1, 101, 'B')")
    discard execSql(db, "INSERT INTO enrollment (student_id, course_id, grade) VALUES (1, 102, 'C')")

    let upd = execSql(db, "UPDATE enrollment SET grade = 'A' WHERE student_id = 1 AND course_id = 101")
    check upd.ok

    let selRes = execSql(db, "SELECT grade FROM enrollment WHERE student_id = 1 AND course_id = 101")
    check selRes.ok
    check selRes.value.len == 1
    check selRes.value[0] == "A"

    discard closeDb(db)
    removeFile(path)

  test "DELETE on composite PK table":
    let path = makeTempDb("decentdb_composite_pk_delete.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE enrollment (student_id INT NOT NULL, course_id INT NOT NULL, grade TEXT, PRIMARY KEY (student_id, course_id))")
    discard execSql(db, "INSERT INTO enrollment (student_id, course_id, grade) VALUES (1, 101, 'A')")
    discard execSql(db, "INSERT INTO enrollment (student_id, course_id, grade) VALUES (1, 102, 'B')")
    discard execSql(db, "INSERT INTO enrollment (student_id, course_id, grade) VALUES (2, 101, 'C')")

    let del = execSql(db, "DELETE FROM enrollment WHERE student_id = 1 AND course_id = 101")
    check del.ok

    let selRes = execSql(db, "SELECT student_id, course_id FROM enrollment ORDER BY student_id, course_id")
    check selRes.ok
    check selRes.value.len == 2
    check selRes.value[0] == "1|102"
    check selRes.value[1] == "2|101"

    discard closeDb(db)
    removeFile(path)

  test "composite PK with mixed types (INT + TEXT)":
    let path = makeTempDb("decentdb_composite_pk_mixed.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let createRes = execSql(db, "CREATE TABLE tags (entity_type TEXT NOT NULL, entity_id INT NOT NULL, tag TEXT, PRIMARY KEY (entity_type, entity_id))")
    check createRes.ok

    let ins1 = execSql(db, "INSERT INTO tags (entity_type, entity_id, tag) VALUES ('user', 1, 'admin')")
    check ins1.ok
    let ins2 = execSql(db, "INSERT INTO tags (entity_type, entity_id, tag) VALUES ('post', 1, 'featured')")
    check ins2.ok
    # Same type+id should fail
    let ins3 = execSql(db, "INSERT INTO tags (entity_type, entity_id, tag) VALUES ('user', 1, 'editor')")
    check not ins3.ok

    let selRes = execSql(db, "SELECT entity_type, entity_id, tag FROM tags ORDER BY entity_type, entity_id")
    check selRes.ok
    check selRes.value.len == 2

    discard closeDb(db)
    removeFile(path)

suite "Composite Indexes":
  test "CREATE INDEX on multiple columns":
    let path = makeTempDb("decentdb_composite_idx.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE orders (customer_id INT NOT NULL, product_id INT NOT NULL, qty INT)")
    discard execSql(db, "INSERT INTO orders (customer_id, product_id, qty) VALUES (1, 10, 5)")
    discard execSql(db, "INSERT INTO orders (customer_id, product_id, qty) VALUES (1, 20, 3)")
    discard execSql(db, "INSERT INTO orders (customer_id, product_id, qty) VALUES (2, 10, 7)")

    let idxRes = execSql(db, "CREATE INDEX idx_orders_cust_prod ON orders (customer_id, product_id)")
    check idxRes.ok

    # Verify data is still queryable
    let selRes = execSql(db, "SELECT customer_id, product_id, qty FROM orders ORDER BY customer_id, product_id")
    check selRes.ok
    check selRes.value.len == 3

    discard closeDb(db)
    removeFile(path)

  test "CREATE UNIQUE INDEX on multiple columns rejects duplicates":
    let path = makeTempDb("decentdb_composite_uniq_idx.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c TEXT)")
    discard execSql(db, "INSERT INTO t (a, b, c) VALUES (1, 1, 'x')")
    discard execSql(db, "INSERT INTO t (a, b, c) VALUES (1, 2, 'y')")

    let idxRes = execSql(db, "CREATE UNIQUE INDEX idx_t_ab ON t (a, b)")
    check idxRes.ok

    # Duplicate should fail
    let ins = execSql(db, "INSERT INTO t (a, b, c) VALUES (1, 1, 'z')")
    check not ins.ok
    check "UNIQUE constraint failed" in ins.err.message

    # Non-duplicate should succeed
    let ins2 = execSql(db, "INSERT INTO t (a, b, c) VALUES (1, 3, 'w')")
    check ins2.ok

    discard closeDb(db)
    removeFile(path)

  test "CREATE UNIQUE INDEX on existing data with duplicates fails":
    let path = makeTempDb("decentdb_composite_uniq_dup.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard execSql(db, "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)")
    discard execSql(db, "INSERT INTO t (a, b) VALUES (1, 1)")
    discard execSql(db, "INSERT INTO t (a, b) VALUES (1, 1)")

    let idxRes = execSql(db, "CREATE UNIQUE INDEX idx_t_ab ON t (a, b)")
    check not idxRes.ok
    check "UNIQUE index creation failed" in idxRes.err.message

    discard closeDb(db)
    removeFile(path)

  test "composite PK persists across reopen":
    let path = makeTempDb("decentdb_composite_pk_persist.db")
    block:
      let dbRes = openDb(path)
      check dbRes.ok
      let db = dbRes.value
      discard execSql(db, "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c TEXT, PRIMARY KEY (a, b))")
      discard execSql(db, "INSERT INTO t (a, b, c) VALUES (1, 2, 'hello')")
      discard execSql(db, "INSERT INTO t (a, b, c) VALUES (3, 4, 'world')")
      discard closeDb(db)

    block:
      let dbRes = openDb(path)
      check dbRes.ok
      let db = dbRes.value

      # Data should be there
      let selRes = execSql(db, "SELECT a, b, c FROM t ORDER BY a")
      check selRes.ok
      check selRes.value.len == 2
      check selRes.value[0] == "1|2|hello"
      check selRes.value[1] == "3|4|world"

      # Composite PK should still be enforced
      let dupRes = execSql(db, "INSERT INTO t (a, b, c) VALUES (1, 2, 'dup')")
      check not dupRes.ok

      discard closeDb(db)

    removeFile(path)
