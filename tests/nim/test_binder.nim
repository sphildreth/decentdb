import unittest
import os

import engine
import sql/sql
import sql/binder
import catalog/catalog
import storage/storage

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

proc addTable(db: Db, name: string, columns: seq[Column]): TableMeta =
  let rootRes = initTableRoot(db.pager)
  check rootRes.ok
  let meta = TableMeta(name: name, rootPage: rootRes.value, nextRowId: 1, columns: columns)
  check db.catalog.saveTable(db.pager, meta).ok
  meta

proc addIndex(db: Db, name: string, table: string, column: string, unique: bool): IndexMeta =
  let rootRes = initTableRoot(db.pager)
  check rootRes.ok
  let meta = IndexMeta(name: name, table: table, column: column, rootPage: rootRes.value, kind: ikBtree, unique: unique)
  check db.catalog.createIndexMeta(meta).ok
  meta

proc parseSingle(sqlText: string): Statement =
  let astRes = parseSql(sqlText)
  check astRes.ok
  check astRes.value.statements.len == 1
  astRes.value.statements[0]

suite "Binder":
  test "bind errors for unknown table and ambiguous column":
    let path = makeTempDb("decentdb_binder_errors.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "a", @[Column(name: "id", kind: ctInt64)])
    discard addTable(db, "b", @[Column(name: "id", kind: ctInt64)])

    let stmtUnknown = parseSingle("SELECT id FROM missing")
    let bindUnknown = bindStatement(db.catalog, stmtUnknown)
    check not bindUnknown.ok

    let stmtAmbig = parseSingle("SELECT id FROM a INNER JOIN b ON a.id = b.id")
    let bindAmbig = bindStatement(db.catalog, stmtAmbig)
    check not bindAmbig.ok

    discard closeDb(db)

  test "bind insert/update column validation":
    let path = makeTempDb("decentdb_binder_cols.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])

    let stmtInsert = parseSingle("INSERT INTO t (id) VALUES (1, 2)")
    let bindInsert = bindStatement(db.catalog, stmtInsert)
    check not bindInsert.ok

    let stmtUpdate = parseSingle("UPDATE t SET missing = 1")
    let bindUpdate = bindStatement(db.catalog, stmtUpdate)
    check not bindUpdate.ok

    let stmtType = parseSingle("INSERT INTO t (id, name) VALUES ('bad', 'ok')")
    let bindType = bindStatement(db.catalog, stmtType)
    check not bindType.ok

    discard closeDb(db)

  test "bind create table and index constraints":
    let path = makeTempDb("decentdb_binder_create.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "parent", @[Column(name: "id", kind: ctInt64)])

    let stmtPk = parseSingle("CREATE TABLE bad (a INT PRIMARY KEY, b INT PRIMARY KEY)")
    let bindPk = bindStatement(db.catalog, stmtPk)
    check not bindPk.ok

    let stmtFk = parseSingle("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))")
    let bindFk = bindStatement(db.catalog, stmtFk)
    check not bindFk.ok

    discard addIndex(db, "parent_id_idx", "parent", "id", false)
    let stmtFk2 = parseSingle("CREATE TABLE child2 (id INT, parent_id INT REFERENCES parent(id))")
    let bindFk2 = bindStatement(db.catalog, stmtFk2)
    check not bindFk2.ok

    discard addIndex(db, "parent_id_uq", "parent", "id", true)
    let stmtFk3 = parseSingle("CREATE TABLE child3 (id INT, parent_id INT REFERENCES parent(id))")
    let bindFk3 = bindStatement(db.catalog, stmtFk3)
    check bindFk3.ok

    let stmtIdxBad = parseSingle("CREATE INDEX t_trgm ON parent USING trigram (id)")
    let bindIdxBad = bindStatement(db.catalog, stmtIdxBad)
    check not bindIdxBad.ok

    let stmtIdxUniq = parseSingle("CREATE UNIQUE INDEX t_trgm2 ON parent USING trigram (id)")
    let bindIdxUniq = bindStatement(db.catalog, stmtIdxUniq)
    check not bindIdxUniq.ok

    discard closeDb(db)
