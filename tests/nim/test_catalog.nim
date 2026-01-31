import unittest
import os

import engine
import options
import catalog/catalog
import pager/pager
import storage/storage

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
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

proc addIndex(db: Db, name: string, table: string, column: string, kind: IndexKind, root: PageId): IndexMeta =
  let meta = IndexMeta(name: name, table: table, column: column, rootPage: root, kind: kind, unique: false)
  check db.catalog.createIndexMeta(meta).ok
  meta

suite "Catalog":
  test "save/get/drop tables and indexes":
    let path = makeTempDb("decentdb_catalog_basic.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    let cols = @[
      Column(name: "id", kind: ctInt64, notNull: true, primaryKey: true),
      Column(name: "name", kind: ctText, unique: true),
      Column(name: "artist_id", kind: ctInt64, refTable: "artist", refColumn: "id")
    ]
    let tableMeta = addTable(db, "album", cols)
    let idxRoot = initTableRoot(db.pager)
    check idxRoot.ok
    discard addIndex(db, "album_name_idx", "album", "name", ikBtree, idxRoot.value)

    let tableRes = db.catalog.getTable("album")
    check tableRes.ok
    check tableRes.value.name == tableMeta.name
    check tableRes.value.columns.len == 3

    let idxOpt = db.catalog.getIndexByName("album_name_idx")
    check isSome(idxOpt)
    check idxOpt.get.table == "album"

    check db.catalog.dropIndex("album_name_idx").ok
    check db.catalog.dropTable("album").ok
    check not db.catalog.getTable("album").ok
    check not db.catalog.dropTable("album").ok

    discard closeDb(db)

  test "catalog persists flags across reopen":
    let path = makeTempDb("decentdb_catalog_persist.db")
    block:
      let dbRes = openDb(path)
      check dbRes.ok
      let db = dbRes.value
      let cols = @[
        Column(name: "id", kind: ctInt64, notNull: true, primaryKey: true),
        Column(name: "name", kind: ctText, unique: true),
        Column(name: "artist_id", kind: ctInt64, refTable: "artist", refColumn: "id")
      ]
      discard addTable(db, "album", cols)
      let idxRoot = initTableRoot(db.pager)
      check idxRoot.ok
      discard addIndex(db, "album_name_trgm", "album", "name", ikTrigram, idxRoot.value)
      discard closeDb(db)

    let reopenRes = openDb(path)
    check reopenRes.ok
    let db2 = reopenRes.value
    let tableRes = db2.catalog.getTable("album")
    check tableRes.ok
    check tableRes.value.columns.len == 3
    check tableRes.value.columns[0].notNull
    check tableRes.value.columns[0].primaryKey
    check tableRes.value.columns[1].unique
    check tableRes.value.columns[2].refTable == "artist"
    check tableRes.value.columns[2].refColumn == "id"

    let idxOpt = db2.catalog.getTrigramIndexForColumn("album", "name")
    check isSome(idxOpt)
    check idxOpt.get.kind == ikTrigram

    discard closeDb(db2)

  test "saveIndexMeta updates root":
    let path = makeTempDb("decentdb_catalog_update_index.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let cols = @[Column(name: "id", kind: ctInt64)]
    discard addTable(db, "t", cols)

    let root1 = initTableRoot(db.pager)
    check root1.ok
    let idx = addIndex(db, "t_id_idx", "t", "id", ikBtree, root1.value)
    let root2 = initTableRoot(db.pager)
    check root2.ok
    var updated = idx
    updated.rootPage = root2.value
    check db.catalog.saveIndexMeta(updated).ok

    let idxOpt = db.catalog.getIndexByName("t_id_idx")
    check isSome(idxOpt)
    check idxOpt.get.rootPage == root2.value

    discard closeDb(db)

  test "parseColumnType supports VARCHAR as TEXT alias":
    # Test that VARCHAR is parsed as TEXT
    let varcharRes = parseColumnType("VARCHAR")
    check varcharRes.ok
    check varcharRes.value == ctText

    # Test that CHARACTER VARYING is parsed as TEXT
    let charVaryingRes = parseColumnType("CHARACTER VARYING")
    check charVaryingRes.ok
    check charVaryingRes.value == ctText

    # Test that VARCHAR(255) is parsed as TEXT (ignoring length specification)
    let varcharWithLengthRes = parseColumnType("VARCHAR(255)")
    check varcharWithLengthRes.ok
    check varcharWithLengthRes.value == ctText

    # Test that CHARACTER VARYING(100) is parsed as TEXT (ignoring length specification)
    let charVaryingWithLengthRes = parseColumnType("CHARACTER VARYING(100)")
    check charVaryingWithLengthRes.ok
    check charVaryingWithLengthRes.value == ctText

    # Test case insensitivity
    let lowerCaseRes = parseColumnType("varchar")
    check lowerCaseRes.ok
    check lowerCaseRes.value == ctText

    let mixedCaseRes = parseColumnType("VarChar(100)")
    check mixedCaseRes.ok
    check mixedCaseRes.value == ctText
