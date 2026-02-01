import unittest
import os

import engine
import catalog/catalog
import record/record
import storage/storage
import search/search

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

proc createTable(db: Db, name: string, columns: seq[Column]): TableMeta =
  let rootRes = initTableRoot(db.pager)
  check rootRes.ok
  let meta = TableMeta(name: name, rootPage: rootRes.value, nextRowId: 1, columns: columns)
  check db.catalog.saveTable(db.pager, meta).ok
  meta

proc createIndex(db: Db, name: string, table: string, column: string, kind: IndexKind): IndexMeta =
  let rootRes = initTableRoot(db.pager)
  check rootRes.ok
  let meta = IndexMeta(name: name, table: table, column: column, rootPage: rootRes.value, kind: kind, unique: false)
  check db.catalog.createIndexMeta(meta).ok
  meta

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "Storage":
  test "btree index insert/update/delete happy path":
    let path = makeTempDb("decentdb_storage_btree.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard createTable(db, "users", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])
    let idx = createIndex(db, "users_id_idx", "users", "id", ikBtree)

    let row1 = insertRow(db.pager, db.catalog, "users", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: toBytes("Alice"))
    ])
    check row1.ok
    let row2 = insertRow(db.pager, db.catalog, "users", @[
      Value(kind: vkInt64, int64Val: 2),
      Value(kind: vkText, bytes: toBytes("Bob"))
    ])
    check row2.ok

    let seekRes = indexSeek(db.pager, db.catalog, "users", "id", Value(kind: vkInt64, int64Val: 2))
    check seekRes.ok
    check seekRes.value == @[row2.value]

    let hasAny = indexHasAnyKey(db.pager, idx, indexKeyFromValue(Value(kind: vkInt64, int64Val: 1)))
    check hasAny.ok
    check hasAny.value

    let hasOther = indexHasOtherRowid(db.pager, idx, indexKeyFromValue(Value(kind: vkInt64, int64Val: 1)), row1.value)
    check hasOther.ok
    check not hasOther.value

    check updateRow(db.pager, db.catalog, "users", row2.value, @[
      Value(kind: vkInt64, int64Val: 3),
      Value(kind: vkText, bytes: toBytes("Bobby"))
    ]).ok

    let seekOld = indexSeek(db.pager, db.catalog, "users", "id", Value(kind: vkInt64, int64Val: 2))
    check seekOld.ok
    check seekOld.value.len == 0
    let seekNew = indexSeek(db.pager, db.catalog, "users", "id", Value(kind: vkInt64, int64Val: 3))
    check seekNew.ok
    check seekNew.value == @[row2.value]

    check deleteRow(db.pager, db.catalog, "users", row1.value).ok
    let readDeleted = readRowAt(db.pager, db.catalog.getTable("users").value, row1.value)
    check not readDeleted.ok

    discard closeDb(db)

  test "trigram index updates on update and delete":
    let path = makeTempDb("decentdb_storage_trigram.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard createTable(db, "docs", @[Column(name: "id", kind: ctInt64), Column(name: "body", kind: ctText)])
    let idx = createIndex(db, "docs_body_trgm", "docs", "body", ikTrigram)

    let rowRes = insertRow(db.pager, db.catalog, "docs", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: toBytes("HELLO"))
    ])
    check rowRes.ok
    check flushTrigramDeltas(db.pager, db.catalog).ok

    let gramsOld = trigrams("HELLO")
    check gramsOld.len > 0
    let postingsOld = getTrigramPostings(db.pager, idx, gramsOld[0])
    check postingsOld.ok
    check rowRes.value in postingsOld.value

    check updateRow(db.pager, db.catalog, "docs", rowRes.value, @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: toBytes("WORLD"))
    ]).ok
    check flushTrigramDeltas(db.pager, db.catalog).ok

    let postingsOldAfter = getTrigramPostings(db.pager, idx, gramsOld[0])
    check postingsOldAfter.ok
    check rowRes.value notin postingsOldAfter.value

    let gramsNew = trigrams("WORLD")
    check gramsNew.len > 0
    let postingsNew = getTrigramPostings(db.pager, idx, gramsNew[0])
    check postingsNew.ok
    check rowRes.value in postingsNew.value

    check deleteRow(db.pager, db.catalog, "docs", rowRes.value).ok
    check flushTrigramDeltas(db.pager, db.catalog).ok
    let postingsAfterDelete = getTrigramPostings(db.pager, idx, gramsNew[0])
    check postingsAfterDelete.ok
    check rowRes.value notin postingsAfterDelete.value

    discard closeDb(db)

  test "edge cases for insert/update and index build":
    let path = makeTempDb("decentdb_storage_edges.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "items", @[Column(name: "id", kind: ctInt64), Column(name: "payload", kind: ctBlob)])
    discard table

    let badInsert = insertRow(db.pager, db.catalog, "items", @[Value(kind: vkInt64, int64Val: 1)])
    check not badInsert.ok

    let rowRes = insertRow(db.pager, db.catalog, "items", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkBlob, bytes: @[1'u8, 2'u8, 3'u8])
    ])
    check rowRes.ok

    let badUpdate = updateRow(db.pager, db.catalog, "items", rowRes.value, @[Value(kind: vkInt64, int64Val: 1)])
    check not badUpdate.ok

    let idxRootRes = initTableRoot(db.pager)
    check idxRootRes.ok
    let buildBad = buildIndexForColumn(db.pager, db.catalog, "items", "missing", idxRootRes.value)
    check not buildBad.ok

    discard closeDb(db)
