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
  let meta = IndexMeta(name: name, table: table, columns: @[column], rootPage: rootRes.value, kind: ikBtree, unique: unique)
  check db.catalog.createIndexMeta(meta).ok
  meta

proc addView(db: Db, name: string, sqlText: string, columnNames: seq[string], dependencies: seq[string]): ViewMeta =
  let meta = ViewMeta(name: name, sqlText: sqlText, columnNames: columnNames, dependencies: dependencies)
  check db.catalog.createViewMeta(meta).ok
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
    check bindPk.ok  # composite PKs are now supported

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

  test "bind create view and select expansion":
    let path = makeTempDb("decentdb_binder_view_basic.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])

    let createStmt = parseSingle("CREATE VIEW v AS SELECT id AS x, name FROM t")
    let createBind = bindStatement(db.catalog, createStmt)
    check createBind.ok
    check createBind.value.kind == skCreateView
    check createBind.value.createViewColumns == @["x", "name"]

    discard addView(db, "v", "SELECT id AS x, name FROM t", @["x", "name"], @["t"])
    let selectStmt = parseSingle("SELECT x FROM v WHERE x = 1")
    let selectBind = bindStatement(db.catalog, selectStmt)
    check selectBind.ok
    check selectBind.value.kind == skSelect
    check selectBind.value.fromTable == "t"

    discard closeDb(db)

  test "bind view validations and DML rejection":
    let path = makeTempDb("decentdb_binder_view_validation.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])
    discard addView(db, "v", "SELECT id FROM t", @["id"], @["t"])

    let mismatch = bindStatement(db.catalog, parseSingle("CREATE VIEW vm (a) AS SELECT id, name FROM t"))
    check not mismatch.ok

    let duplicate = bindStatement(db.catalog, parseSingle("CREATE VIEW vd AS SELECT id, id FROM t"))
    check not duplicate.ok

    let withParam = bindStatement(db.catalog, parseSingle("CREATE VIEW vp AS SELECT id FROM t WHERE id = $1"))
    check not withParam.ok

    let insertView = bindStatement(db.catalog, parseSingle("INSERT INTO v (id) VALUES (1)"))
    check not insertView.ok
    let updateView = bindStatement(db.catalog, parseSingle("UPDATE v SET id = 2"))
    check not updateView.ok
    let deleteView = bindStatement(db.catalog, parseSingle("DELETE FROM v"))
    check not deleteView.ok

    discard closeDb(db)

  test "bind strict dependency semantics":
    let path = makeTempDb("decentdb_binder_view_dependencies.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64)])
    discard addView(db, "v1", "SELECT id FROM t", @["id"], @["t"])
    discard addView(db, "v2", "SELECT id FROM v1", @["id"], @["v1"])

    let dropTable = bindStatement(db.catalog, parseSingle("DROP TABLE t"))
    check not dropTable.ok

    let dropView = bindStatement(db.catalog, parseSingle("DROP VIEW v1"))
    check not dropView.ok

    let renameView = bindStatement(db.catalog, parseSingle("ALTER VIEW v1 RENAME TO v1_new"))
    check not renameView.ok

    let cycleReplace = bindStatement(db.catalog, parseSingle("CREATE OR REPLACE VIEW v1 AS SELECT id FROM v2"))
    check not cycleReplace.ok

    discard closeDb(db)
