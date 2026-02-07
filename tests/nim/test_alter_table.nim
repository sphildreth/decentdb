import unittest
import os
import engine
import options
import catalog/catalog
import record/record
import storage/storage
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "ALTER TABLE":
  test "add column fills existing rows and preserves indexes":
    let path = makeTempDb("decentdb_alter_add.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE INDEX users_name_idx ON users (name)").ok
    check execSql(db, "INSERT INTO users (id, name) VALUES (1, 'Alice')").ok

    let alterRes = execSql(db, "ALTER TABLE users ADD COLUMN age INT64")
    check alterRes.ok

    let tableRes = db.catalog.getTable("users")
    check tableRes.ok
    check tableRes.value.columns.len == 3

    let res = execSql(db, "SELECT id, name, age FROM users ORDER BY id")
    check res.ok
    check res.value == @["1|Alice|NULL"]

    check execSql(db, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30)").ok
    let res2 = execSql(db, "SELECT id, name, age FROM users ORDER BY id")
    check res2.ok
    check res2.value == @["1|Alice|NULL", "2|Bob|30"]

    let seekRes = indexSeek(db.pager, db.catalog, "users", "name", Value(kind: vkText, bytes: toBytes("Alice")))
    check seekRes.ok
    check seekRes.value == @[1'u64]

    discard closeDb(db)

  test "drop column removes indexes and data":
    let path = makeTempDb("decentdb_alter_drop.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE people (id INT PRIMARY KEY, name TEXT, age INT)").ok
    check execSql(db, "CREATE INDEX people_name_idx ON people (name)").ok
    check execSql(db, "CREATE INDEX people_age_idx ON people (age)").ok
    check execSql(db, "INSERT INTO people (id, name, age) VALUES (1, 'Ada', 37)").ok
    check execSql(db, "INSERT INTO people (id, name, age) VALUES (2, 'Bob', 28)").ok

    let alterRes = execSql(db, "ALTER TABLE people DROP COLUMN age")
    check alterRes.ok

    let tableRes = db.catalog.getTable("people")
    check tableRes.ok
    check tableRes.value.columns.len == 2

    check isNone(db.catalog.getBtreeIndexForColumn("people", "age"))
    check isSome(db.catalog.getBtreeIndexForColumn("people", "name"))

    let res = execSql(db, "SELECT id, name FROM people ORDER BY id")
    check res.ok
    check res.value == @["1|Ada", "2|Bob"]

    let seekRes = indexSeek(db.pager, db.catalog, "people", "name", Value(kind: vkText, bytes: toBytes("Ada")))
    check seekRes.ok
    check seekRes.value == @[1'u64]

    discard closeDb(db)

  test "alter column default returns error":
    let path = makeTempDb("decentdb_alter_default.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE defaults (id INT, name TEXT)").ok
    let alterRes = execSql(db, "ALTER TABLE defaults ALTER COLUMN name SET DEFAULT 'x'")
    check not alterRes.ok
    check alterRes.err.code == ERR_INTERNAL

    discard closeDb(db)

  test "rename column updates indexes and foreign key metadata":
    let path = makeTempDb("decentdb_alter_rename.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE UNIQUE INDEX parent_id_uq ON parent (id)").ok
    check execSql(db, "CREATE INDEX parent_name_idx ON parent (name)").ok
    check execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))").ok
    check execSql(db, "INSERT INTO parent (id, name) VALUES (1, 'Ada')").ok

    let renameRes = execSql(db, "ALTER TABLE parent RENAME COLUMN name TO full_name")
    check renameRes.ok

    let tableRes = db.catalog.getTable("parent")
    check tableRes.ok
    check tableRes.value.columns[1].name == "full_name"

    check isSome(db.catalog.getBtreeIndexForColumn("parent", "full_name"))
    check isNone(db.catalog.getBtreeIndexForColumn("parent", "name"))

    let oldNameQuery = execSql(db, "SELECT name FROM parent")
    check not oldNameQuery.ok
    let newNameQuery = execSql(db, "SELECT full_name FROM parent")
    check newNameQuery.ok
    check newNameQuery.value == @["Ada"]

    let childMeta = db.catalog.getTable("child")
    check childMeta.ok
    check childMeta.value.columns[1].refColumn == "id"

    let renamePkRes = execSql(db, "ALTER TABLE parent RENAME COLUMN id TO parent_id_key")
    check renamePkRes.ok
    let childMeta2 = db.catalog.getTable("child")
    check childMeta2.ok
    check childMeta2.value.columns[1].refColumn == "parent_id_key"

    check execSql(db, "INSERT INTO child (id, parent_id) VALUES (10, 1)").ok
    let fkStillWorks = execSql(db, "INSERT INTO child (id, parent_id) VALUES (11, 999)")
    check not fkStillWorks.ok

    discard closeDb(db)

  test "rename column blocked when dependent view exists":
    let path = makeTempDb("decentdb_alter_rename_view_block.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "CREATE VIEW v AS SELECT id FROM t").ok
    let renameRes = execSql(db, "ALTER TABLE t RENAME COLUMN id TO id2")
    check not renameRes.ok
    check renameRes.err.code == ERR_SQL

    discard closeDb(db)

  test "alter column type rewrites rows and preserves index usability":
    let path = makeTempDb("decentdb_alter_set_type_success.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)").ok
    check execSql(db, "CREATE INDEX t_val_idx ON t (val)").ok
    check execSql(db, "INSERT INTO t VALUES (1, '10')").ok
    check execSql(db, "INSERT INTO t VALUES (2, '20')").ok
    check execSql(db, "INSERT INTO t VALUES (3, NULL)").ok

    let alterRes = execSql(db, "ALTER TABLE t ALTER COLUMN val TYPE INT")
    check alterRes.ok

    let tableRes = db.catalog.getTable("t")
    check tableRes.ok
    check tableRes.value.columns.len == 2
    check tableRes.value.columns[1].kind == ctInt64

    let res = execSql(db, "SELECT id, val FROM t ORDER BY id")
    check res.ok
    check res.value == @["1|10", "2|20", "3|NULL"]

    let seekRes = indexSeek(db.pager, db.catalog, "t", "val", Value(kind: vkInt64, int64Val: 20))
    check seekRes.ok
    check seekRes.value == @[2'u64]

    discard closeDb(db)

  test "alter column type conversion failure keeps prior schema and rows":
    let path = makeTempDb("decentdb_alter_set_type_failure.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)").ok
    check execSql(db, "CREATE INDEX t_val_idx ON t (val)").ok
    check execSql(db, "INSERT INTO t VALUES (1, '10')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'bad')").ok

    let alterRes = execSql(db, "ALTER TABLE t ALTER COLUMN val TYPE INT")
    check not alterRes.ok
    check alterRes.err.code == ERR_SQL

    let tableRes = db.catalog.getTable("t")
    check tableRes.ok
    check tableRes.value.columns[1].kind == ctText

    let rowsRes = execSql(db, "SELECT id, val FROM t ORDER BY id")
    check rowsRes.ok
    check rowsRes.value == @["1|10", "2|bad"]

    let seekRes = indexSeek(db.pager, db.catalog, "t", "val", Value(kind: vkText, bytes: toBytes("bad")))
    check seekRes.ok
    check seekRes.value == @[2'u64]

    discard closeDb(db)
