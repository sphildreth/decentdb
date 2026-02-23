# Case-Insensitive Identifier Resolution Tests (ADR-0096)
#
# Verifies that DecentDB follows PostgreSQL semantics where unquoted identifiers
# are case-insensitive. Tables, columns, and indexes created with quoted identifiers
# must be accessible via unquoted (lowercased) identifiers and vice versa.

import unittest
import os
import strutils

import engine

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  let walPath = path & "-wal"
  if fileExists(walPath):
    removeFile(walPath)
  path

suite "Case-insensitive table names":
  var db: Db

  setup:
    let path = makeTempDb("test_case_ident_tables.ddb")
    let openRes = openDb(path, cachePages = 64)
    check openRes.ok
    db = openRes.value

  teardown:
    discard closeDb(db)

  test "quoted CREATE TABLE accessible via unquoted SELECT":
    check execSql(db, """CREATE TABLE "MyTable" ("Id" INTEGER PRIMARY KEY, "Name" TEXT)""").ok
    check execSql(db, """INSERT INTO "MyTable" ("Id", "Name") VALUES (1, 'Alice')""").ok

    let sel = execSql(db, "SELECT id, name FROM mytable WHERE id = 1")
    check sel.ok
    check sel.value.len == 1
    check sel.value[0] == "1|Alice"

  test "unquoted CREATE TABLE accessible via quoted SELECT":
    check execSql(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)").ok
    check execSql(db, "INSERT INTO users (id, email) VALUES (1, 'a@b.com')").ok

    let sel = execSql(db, """SELECT "id", "email" FROM "users" WHERE "id" = 1""")
    check sel.ok
    check sel.value.len == 1
    check sel.value[0] == "1|a@b.com"

  test "mixed case CREATE TABLE accessible both ways":
    check execSql(db, """CREATE TABLE "ArtistStaging" ("ArtistId" INTEGER PRIMARY KEY, "ArtistName" TEXT)""").ok
    check execSql(db, """INSERT INTO "ArtistStaging" ("ArtistId", "ArtistName") VALUES (1, 'Beatles')""").ok

    # Unquoted access
    let sel1 = execSql(db, "SELECT artistid, artistname FROM artiststaging")
    check sel1.ok
    check sel1.value.len == 1
    check sel1.value[0] == "1|Beatles"

    # Mixed quoting
    let sel2 = execSql(db, """SELECT "ArtistId", artistname FROM "ArtistStaging" """)
    check sel2.ok
    check sel2.value.len == 1

suite "Case-insensitive column names":
  var db: Db

  setup:
    let path = makeTempDb("test_case_ident_columns.ddb")
    let openRes = openDb(path, cachePages = 64)
    check openRes.ok
    db = openRes.value

  teardown:
    discard closeDb(db)

  test "INSERT with unquoted columns into quoted-CREATE table":
    check execSql(db, """CREATE TABLE "Items" ("ItemId" INTEGER PRIMARY KEY, "ItemName" TEXT, "Price" REAL)""").ok

    let ins = execSql(db, "INSERT INTO items (itemid, itemname, price) VALUES (1, 'Widget', 9.99)")
    check ins.ok

    let sel = execSql(db, "SELECT itemname, price FROM items WHERE itemid = 1")
    check sel.ok
    check sel.value.len == 1
    check "Widget" in sel.value[0]

  test "UPDATE with unquoted columns on quoted-CREATE table":
    check execSql(db, """CREATE TABLE "Products" ("ProductId" INTEGER PRIMARY KEY, "ProductName" TEXT)""").ok
    check execSql(db, """INSERT INTO "Products" ("ProductId", "ProductName") VALUES (1, 'Old')""").ok

    check execSql(db, "UPDATE products SET productname = 'New' WHERE productid = 1").ok

    let sel = execSql(db, "SELECT productname FROM products WHERE productid = 1")
    check sel.ok
    check sel.value.len == 1
    check sel.value[0] == "New"

  test "DELETE with unquoted WHERE on quoted-CREATE table":
    check execSql(db, """CREATE TABLE "Logs" ("LogId" INTEGER PRIMARY KEY, "Message" TEXT)""").ok
    check execSql(db, """INSERT INTO "Logs" ("LogId", "Message") VALUES (1, 'keep')""").ok
    check execSql(db, """INSERT INTO "Logs" ("LogId", "Message") VALUES (2, 'delete')""").ok

    check execSql(db, "DELETE FROM logs WHERE logid = 2").ok

    let sel = execSql(db, "SELECT logid FROM logs")
    check sel.ok
    check sel.value.len == 1

suite "Case-insensitive INSERT ON CONFLICT":
  var db: Db

  setup:
    let path = makeTempDb("test_case_ident_conflict.ddb")
    let openRes = openDb(path, cachePages = 64)
    check openRes.ok
    db = openRes.value

  teardown:
    discard closeDb(db)

  test "ON CONFLICT with unquoted column targets on quoted-CREATE table":
    check execSql(db, """CREATE TABLE "Settings" ("Key" TEXT PRIMARY KEY, "Value" TEXT)""").ok
    check execSql(db, """INSERT INTO "Settings" ("Key", "Value") VALUES ('color', 'red')""").ok

    check execSql(db, "INSERT INTO settings (key, value) VALUES ('color', 'blue') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value").ok

    let sel = execSql(db, "SELECT value FROM settings WHERE key = 'color'")
    check sel.ok
    check sel.value[0] == "blue"

suite "Case-insensitive index names":
  var db: Db

  setup:
    let path = makeTempDb("test_case_ident_indexes.ddb")
    let openRes = openDb(path, cachePages = 64)
    check openRes.ok
    db = openRes.value

  teardown:
    discard closeDb(db)

  test "CREATE INDEX with quoted table/column, query with unquoted":
    check execSql(db, """CREATE TABLE "Events" ("EventId" INTEGER PRIMARY KEY, "EventDate" TEXT, "Title" TEXT)""").ok
    check execSql(db, """CREATE INDEX "idx_events_date" ON "Events" ("EventDate")""").ok
    check execSql(db, "INSERT INTO events (eventid, eventdate, title) VALUES (1, '2025-01-01', 'New Year')").ok

    let sel = execSql(db, "SELECT title FROM events WHERE eventdate = '2025-01-01'")
    check sel.ok
    check sel.value.len == 1
    check sel.value[0] == "New Year"

suite "Case-insensitive JOINs":
  var db: Db

  setup:
    let path = makeTempDb("test_case_ident_joins.ddb")
    let openRes = openDb(path, cachePages = 64)
    check openRes.ok
    db = openRes.value

  teardown:
    discard closeDb(db)

  test "JOIN with mixed quoting":
    check execSql(db, """CREATE TABLE "Authors" ("AuthorId" INTEGER PRIMARY KEY, "AuthorName" TEXT)""").ok
    check execSql(db, """CREATE TABLE "Books" ("BookId" INTEGER PRIMARY KEY, "AuthorId" INTEGER, "Title" TEXT)""").ok
    check execSql(db, """INSERT INTO "Authors" ("AuthorId", "AuthorName") VALUES (1, 'Tolkien')""").ok
    check execSql(db, """INSERT INTO "Books" ("BookId", "AuthorId", "Title") VALUES (1, 1, 'The Hobbit')""").ok

    let sel = execSql(db, """
      SELECT a.authorname, b.title 
      FROM authors a 
      INNER JOIN books b ON a.authorid = b.authorid
    """)
    check sel.ok
    check sel.value.len == 1
    check sel.value[0] == "Tolkien|The Hobbit"

suite "Case-insensitive subqueries":
  var db: Db

  setup:
    let path = makeTempDb("test_case_ident_subquery.ddb")
    let openRes = openDb(path, cachePages = 64)
    check openRes.ok
    db = openRes.value

  teardown:
    discard closeDb(db)

  test "INSERT...SELECT with mixed quoting":
    check execSql(db, """CREATE TABLE "Staging" ("StagingId" INTEGER PRIMARY KEY, "Val" TEXT)""").ok
    check execSql(db, """CREATE TABLE "Final" ("FinalId" INTEGER PRIMARY KEY, "Val" TEXT)""").ok
    check execSql(db, """INSERT INTO "Staging" ("StagingId", "Val") VALUES (1, 'data')""").ok

    check execSql(db, "INSERT INTO final (finalid, val) SELECT stagingid, val FROM staging").ok

    let sel = execSql(db, "SELECT val FROM final")
    check sel.ok
    check sel.value.len == 1
    check sel.value[0] == "data"
