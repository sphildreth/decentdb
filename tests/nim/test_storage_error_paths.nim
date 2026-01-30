import unittest
import os
import options

import engine
import catalog/catalog
import record/record
import storage/storage
import search/search
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "Storage Error Paths":
  test "insertRow with non-existent table":
    let path = makeTempDb("decentdb_storage_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let insertBad = insertRow(db.pager, db.catalog, "nonexistent", @[Value(kind: vkInt64, int64Val: 1)])
    check not insertBad.ok
    check insertBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "insertRow with column count mismatch":
    let path = makeTempDb("decentdb_storage_col_mismatch.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let insertBad = insertRow(db.pager, db.catalog, "items", @[Value(kind: vkInt64, int64Val: 1)])
    check not insertBad.ok
    check insertBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "updateRow with non-existent table":
    let path = makeTempDb("decentdb_storage_update_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let updateBad = updateRow(db.pager, db.catalog, "nonexistent", 1, @[Value(kind: vkInt64, int64Val: 1)])
    check not updateBad.ok
    check updateBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "updateRow with column count mismatch":
    let path = makeTempDb("decentdb_storage_update_col_mismatch.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    let rowRes = insertRow(db.pager, db.catalog, "items", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: toBytes("test"))
    ])
    check rowRes.ok
    
    let updateBad = updateRow(db.pager, db.catalog, "items", rowRes.value, @[Value(kind: vkInt64, int64Val: 1)])
    check not updateBad.ok
    check updateBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "deleteRow with non-existent table":
    let path = makeTempDb("decentdb_storage_delete_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let deleteBad = deleteRow(db.pager, db.catalog, "nonexistent", 1)
    check not deleteBad.ok
    check deleteBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "readRowAt with non-existent rowid":
    let path = makeTempDb("decentdb_storage_read_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let tableRes = db.catalog.getTable("items")
    check tableRes.ok
    let readBad = readRowAt(db.pager, tableRes.value, 999)
    check not readBad.ok
    
    discard closeDb(db)

  test "indexSeek with non-existent table":
    let path = makeTempDb("decentdb_storage_seek_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let seekBad = indexSeek(db.pager, db.catalog, "nonexistent", "id", Value(kind: vkInt64, int64Val: 1))
    check not seekBad.ok
    check seekBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "indexSeek with no index":
    let path = makeTempDb("decentdb_storage_seek_no_index.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let seekBad = indexSeek(db.pager, db.catalog, "items", "id", Value(kind: vkInt64, int64Val: 1))
    check not seekBad.ok
    check seekBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "buildIndexForColumn with non-existent table":
    let path = makeTempDb("decentdb_storage_build_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let rootRes = initTableRoot(db.pager)
    check rootRes.ok
    
    let buildBad = buildIndexForColumn(db.pager, db.catalog, "nonexistent", "id", rootRes.value)
    check not buildBad.ok
    check buildBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "buildIndexForColumn with non-existent column":
    let path = makeTempDb("decentdb_storage_build_bad_col.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let rootRes = initTableRoot(db.pager)
    check rootRes.ok
    
    let buildBad = buildIndexForColumn(db.pager, db.catalog, "items", "nonexistent", rootRes.value)
    check not buildBad.ok
    check buildBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "buildTrigramIndexForColumn with non-existent table":
    let path = makeTempDb("decentdb_storage_trigram_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let rootRes = initTableRoot(db.pager)
    check rootRes.ok
    
    let buildBad = buildTrigramIndexForColumn(db.pager, db.catalog, "nonexistent", "body", rootRes.value)
    check not buildBad.ok
    check buildBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "buildTrigramIndexForColumn with non-existent column":
    let path = makeTempDb("decentdb_storage_trigram_bad_col.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let rootRes = initTableRoot(db.pager)
    check rootRes.ok
    
    let buildBad = buildTrigramIndexForColumn(db.pager, db.catalog, "items", "nonexistent", rootRes.value)
    check not buildBad.ok
    check buildBad.err.code == ERR_SQL
    
    discard closeDb(db)

  test "indexKeyFromValue for all types":
    check indexKeyFromValue(Value(kind: vkInt64, int64Val: 42)) == 42'u64
    check indexKeyFromValue(Value(kind: vkBool, boolVal: true)) == 1'u64
    check indexKeyFromValue(Value(kind: vkBool, boolVal: false)) == 0'u64
    check indexKeyFromValue(Value(kind: vkFloat64, float64Val: 3.14)) == cast[uint64](3.14)
    check indexKeyFromValue(Value(kind: vkText, bytes: toBytes("test"))) != 0'u64
    check indexKeyFromValue(Value(kind: vkBlob, bytes: @[1'u8, 2'u8])) != 0'u64
    check indexKeyFromValue(Value(kind: vkNull)) == 0'u64
    check indexKeyFromValue(Value(kind: vkTextOverflow)) == 0'u64
    check indexKeyFromValue(Value(kind: vkBlobOverflow)) == 0'u64

  test "getTrigramPostings returns empty for missing trigram":
    let path = makeTempDb("decentdb_storage_postings_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE docs (id INT64, body TEXT)")
    let idxRes = execSql(db, "CREATE INDEX idx_body ON docs USING trigram (body)")
    check idxRes.ok
    
    let tableRes = db.catalog.getTable("docs")
    check tableRes.ok
    let idxOpt = db.catalog.getTrigramIndexForColumn("docs", "body")
    check idxOpt != none(IndexMeta)
    let idx = idxOpt.unsafeGet()
    
    let postings = getTrigramPostings(db.pager, idx, 12345'u32)
    check postings.ok
    check postings.value.len == 0
    
    discard closeDb(db)

  test "indexHasAnyKey with index":
    let path = makeTempDb("decentdb_storage_has_any.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64 PRIMARY KEY, value INT64)")
    discard execSql(db, "CREATE INDEX idx_value ON items (value)")
    discard execSql(db, "INSERT INTO items VALUES (1, 100)")
    
    let idxOpt = db.catalog.getBtreeIndexForColumn("items", "value")
    check idxOpt != none(IndexMeta)
    let idx = idxOpt.unsafeGet()
    
    let hasKey = indexHasAnyKey(db.pager, idx, 100'u64)
    check hasKey.ok
    check hasKey.value == true
    
    let hasNoKey = indexHasAnyKey(db.pager, idx, 999'u64)
    check hasNoKey.ok
    check hasNoKey.value == false
    
    discard closeDb(db)

  test "indexHasOtherRowid with index":
    let path = makeTempDb("decentdb_storage_has_other.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64 PRIMARY KEY, value INT64)")
    discard execSql(db, "CREATE INDEX idx_value ON items (value)")
    let row1 = insertRow(db.pager, db.catalog, "items", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkInt64, int64Val: 100)
    ])
    check row1.ok
    
    let idxOpt = db.catalog.getBtreeIndexForColumn("items", "value")
    check idxOpt != none(IndexMeta)
    let idx = idxOpt.unsafeGet()
    
    let hasOther = indexHasOtherRowid(db.pager, idx, 100'u64, row1.value)
    check hasOther.ok
    check hasOther.value == false
    
    discard closeDb(db)
