import unittest
import os

import engine
import options
import record/record
import catalog/catalog
import search/search
import storage/storage

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Storage Extras":
  test "rebuild btree index keeps entries searchable":
    let path = makeTempDb("decentdb_storage_rebuild_btree.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE logs (id INT PRIMARY KEY, message TEXT)").ok
    check execSql(db, "CREATE INDEX logs_id_idx ON logs (id)").ok
    discard execSql(db, "INSERT INTO logs (id, message) VALUES (1, 'alpha')")
    discard execSql(db, "INSERT INTO logs (id, message) VALUES (2, 'beta')")

    let idxOpt = db.catalog.getIndexByName("logs_id_idx")
    check isSome(idxOpt)
    let idx = idxOpt.get
    check rebuildIndex(db.pager, db.catalog, idx).ok

    let seekRes = indexSeek(db.pager, db.catalog, "logs", "id", Value(kind: vkInt64, int64Val: 2))
    check seekRes.ok
    check seekRes.value == @[2'u64]

    discard closeDb(db)

  test "trigram index rebuild maintains postings":
    let path = makeTempDb("decentdb_storage_rebuild_trigram.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").ok
    check execSql(db, "CREATE INDEX docs_body_trgm ON docs USING trigram (body)").ok
    discard execSql(db, "INSERT INTO docs (id, body) VALUES (1, 'trigram')")

    let idxOpt = db.catalog.getIndexByName("docs_body_trgm")
    check isSome(idxOpt)
    var idx = idxOpt.get
    let grams = trigrams("trigram")
    check grams.len > 0
    let postings = getTrigramPostings(db.pager, idx, grams[0])
    check postings.ok
    check postings.value.len == 1

    check rebuildIndex(db.pager, db.catalog, idx).ok

    let postingsAfter = getTrigramPostings(db.pager, idx, grams[0])
    check postingsAfter.ok
    check postingsAfter.value.len == 1

    let buildRes = buildTrigramIndexForColumn(db.pager, db.catalog, idx.table, idx.column, idx.rootPage)
    check buildRes.ok

    discard closeDb(db)
