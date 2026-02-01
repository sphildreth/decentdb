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
