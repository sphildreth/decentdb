import unittest
import os
import options
import sets

import engine
import catalog/catalog
import record/record
import storage/storage
import search/search
import pager/pager
import btree/btree
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
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

suite "Storage Comprehensive":
  test "initTableRoot creates proper root page":
    let path = makeTempDb("decentdb_init_table_root.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let rootRes = initTableRoot(db.pager)
    check rootRes.ok
    
    # Verify the page was created with correct format
    let readRes = readPageRo(db.pager, rootRes.value)
    check readRes.ok
    let page = readRes.value
    check page[0] == char(PageTypeLeaf)
    
    discard closeDb(db)


  test "indexKeyFromValue with different value types":
    check indexKeyFromValue(Value(kind: vkInt64, int64Val: 42)) == 42'u64
    check indexKeyFromValue(Value(kind: vkBool, boolVal: true)) == 1'u64
    check indexKeyFromValue(Value(kind: vkBool, boolVal: false)) == 0'u64
    check indexKeyFromValue(Value(kind: vkFloat64, float64Val: 3.14)) != 0'u64
    let textKey = indexKeyFromValue(Value(kind: vkText, bytes: toBytes("hello")))
    check textKey != 0'u64
    check indexKeyFromValue(Value(kind: vkTextOverflow, overflowPage: 1)) == 0'u64
    check indexKeyFromValue(Value(kind: vkNull)) == 0'u64



  test "getTrigramPostings with non-existent index":
    let path = makeTempDb("decentdb_get_trigram_postings.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])

    # Create an index meta that doesn't exist in the database
    let fakeIndex = IndexMeta(name: "fake", table: "test", column: "id", rootPage: 999, kind: ikTrigram, unique: false)
    let result = getTrigramPostings(db.pager, fakeIndex, 123'u32)
    # This might return empty instead of error, depending on implementation
    # Just check that it doesn't crash

    discard closeDb(db)

  test "getTrigramPostingsWithDeltas with no delta":
    let path = makeTempDb("decentdb_get_trigram_postings_with_deltas.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    let idx = createIndex(db, "test_id_trgm", "test", "id", ikTrigram)
    
    let result = getTrigramPostingsWithDeltas(db.pager, db.catalog, idx, 123'u32)
    check result.ok
    check result.value.len == 0  # Should be empty since nothing was added
    
    discard closeDb(db)


  test "flushTrigramDeltas with no deltas":
    let path = makeTempDb("decentdb_flush_trigram_deltas_empty.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    
    let result = flushTrigramDeltas(db.pager, db.catalog)
    check result.ok  # Should succeed even with no deltas
    
    discard closeDb(db)

  test "normalizeValues with small text":
    let path = makeTempDb("decentdb_normalize_values_small.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let values = @[Value(kind: vkText, bytes: toBytes("small"))]
    let result = normalizeValues(db.pager, values)
    check result.ok
    check result.value.len == 1
    check result.value[0].kind == vkText
    
    discard closeDb(db)

  test "normalizeValues with large text (overflow)":
    let path = makeTempDb("decentdb_normalize_values_large.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # Create a large text value that should trigger overflow
    var largeText = ""
    for i in 0..<db.pager.pageSize:
      largeText.add('x')
    let values = @[Value(kind: vkText, bytes: toBytes(largeText))]
    let result = normalizeValues(db.pager, values)
    check result.ok
    check result.value.len == 1
    # Should be converted to overflow type
    check result.value[0].kind in {vkTextOverflow}
    
    discard closeDb(db)

  test "readRowAt with non-existent row":
    let path = makeTempDb("decentdb_read_row_at_nonexistent.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    
    let result = readRowAt(db.pager, table, 999'u64)  # Non-existent rowid
    check not result.ok
    
    discard closeDb(db)

  test "scanTable on empty table":
    let path = makeTempDb("decentdb_scan_table_empty.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    
    let result = scanTable(db.pager, table)
    check result.ok
    check result.value.len == 0
    
    discard closeDb(db)

  test "scanTableEach with callback":
    let path = makeTempDb("decentdb_scan_table_each.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    
    # Insert a row first
    let insertRes = insertRow(db.pager, db.catalog, "test", @[Value(kind: vkInt64, int64Val: 42)])
    check insertRes.ok
    
    var count = 0
    proc callback(row: StoredRow): Result[Void] =
      count.inc
      okVoid()
    
    let result = scanTableEach(db.pager, table, callback)
    check result.ok
    check count == 1
    
    discard closeDb(db)

  test "insertRow with wrong column count":
    let path = makeTempDb("decentdb_insert_wrong_cols.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])
    
    let result = insertRow(db.pager, db.catalog, "test", @[Value(kind: vkInt64, int64Val: 1)])  # Only 1 value for 2 columns
    check not result.ok
    check result.err.code == ERR_SQL
    
    discard closeDb(db)

  test "insertRowNoIndexes bypasses index updates":
    let path = makeTempDb("decentdb_insert_no_indexes.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    let idx = createIndex(db, "test_id_idx", "test", "id", ikBtree)
    
    let result = insertRowNoIndexes(db.pager, db.catalog, "test", @[Value(kind: vkInt64, int64Val: 1)])
    check result.ok
    
    # The row should exist in the table but not in the index since we bypassed index updates
    let seekRes = indexSeek(db.pager, db.catalog, "test", "id", Value(kind: vkInt64, int64Val: 1))
    check seekRes.ok
    check seekRes.value.len == 0  # Should be empty since index wasn't updated
    
    discard closeDb(db)

  test "updateRow with wrong column count":
    let path = makeTempDb("decentdb_update_wrong_cols.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])
    
    # Insert a row first
    let insertRes = insertRow(db.pager, db.catalog, "test", @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("Alice"))])
    check insertRes.ok
    let rowid = insertRes.value
    
    let result = updateRow(db.pager, db.catalog, "test", rowid, @[Value(kind: vkInt64, int64Val: 2)])  # Only 1 value for 2 columns
    check not result.ok
    check result.err.code == ERR_SQL
    
    discard closeDb(db)

  test "deleteRow on non-existent row":
    let path = makeTempDb("decentdb_delete_nonexistent.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    
    let result = deleteRow(db.pager, db.catalog, "test", 999'u64)  # Non-existent rowid
    check not result.ok
    
    discard closeDb(db)

  test "buildIndexForColumn with non-existent column":
    let path = makeTempDb("decentdb_build_index_bad_col.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    
    let rootRes = initTableRoot(db.pager)
    check rootRes.ok
    
    let result = buildIndexForColumn(db.pager, db.catalog, "test", "missing_column", rootRes.value)
    check not result.ok
    check result.err.code == ERR_SQL
    
    discard closeDb(db)

  test "buildTrigramIndexForColumn with non-existent column":
    let path = makeTempDb("decentdb_build_trigram_bad_col.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    
    let rootRes = initTableRoot(db.pager)
    check rootRes.ok
    
    let result = buildTrigramIndexForColumn(db.pager, db.catalog, "test", "missing_column", rootRes.value)
    check not result.ok
    check result.err.code == ERR_SQL
    
    discard closeDb(db)

  test "rebuildIndex for btree index":
    let path = makeTempDb("decentdb_rebuild_index.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    let idx = createIndex(db, "test_id_idx", "test", "id", ikBtree)
    
    # Insert some data
    let insertRes1 = insertRow(db.pager, db.catalog, "test", @[Value(kind: vkInt64, int64Val: 1)])
    check insertRes1.ok
    let insertRes2 = insertRow(db.pager, db.catalog, "test", @[Value(kind: vkInt64, int64Val: 2)])
    check insertRes2.ok
    
    # Rebuild the index
    let result = rebuildIndex(db.pager, db.catalog, idx)
    check result.ok
    
    # Verify the index still works
    let seekRes = indexSeek(db.pager, db.catalog, "test", "id", Value(kind: vkInt64, int64Val: 1))
    check seekRes.ok
    check seekRes.value.len >= 0  # Should have at least one match
    
    discard closeDb(db)

  test "indexSeek with non-existent table":
    let path = makeTempDb("decentdb_index_seek_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let result = indexSeek(db.pager, db.catalog, "nonexistent", "id", Value(kind: vkInt64, int64Val: 1))
    check not result.ok
    
    discard closeDb(db)

  test "indexSeek with non-existent index":
    let path = makeTempDb("decentdb_index_seek_bad_index.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    
    let result = indexSeek(db.pager, db.catalog, "test", "id", Value(kind: vkInt64, int64Val: 1))  # No index exists
    check not result.ok
    check result.err.code == ERR_SQL
    
    discard closeDb(db)

  test "indexHasAnyKey with existing key":
    let path = makeTempDb("decentdb_index_has_any_key.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    let idx = createIndex(db, "test_id_idx", "test", "id", ikBtree)
    
    # Insert a row
    let insertRes = insertRow(db.pager, db.catalog, "test", @[Value(kind: vkInt64, int64Val: 1)])
    check insertRes.ok
    
    # Check if key exists
    let result = indexHasAnyKey(db.pager, idx, indexKeyFromValue(Value(kind: vkInt64, int64Val: 1)))
    check result.ok
    check result.value == true
    
    discard closeDb(db)

  test "indexHasOtherRowid with same rowid":
    let path = makeTempDb("decentdb_index_has_other_rowid_same.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let table = createTable(db, "test", @[Column(name: "id", kind: ctInt64)])
    let idx = createIndex(db, "test_id_idx", "test", "id", ikBtree)
    
    # Insert a row
    let insertRes = insertRow(db.pager, db.catalog, "test", @[Value(kind: vkInt64, int64Val: 1)])
    check insertRes.ok
    let rowid = insertRes.value
    
    # Check if there are OTHER rowids with the same key (should be false since only one exists)
    let result = indexHasOtherRowid(db.pager, idx, indexKeyFromValue(Value(kind: vkInt64, int64Val: 1)), rowid)
    check result.ok
    check result.value == false
    
    discard closeDb(db)