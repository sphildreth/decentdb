import unittest
import os
import options
import sets

import engine
import catalog/catalog
import record/record
import planner/planner
import sql/binder
import sql/sql
import errors
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
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Search Functions":
  test "trigrams of empty string":
    let grams = trigrams("")
    check grams.len == 0

  test "trigrams of short string (< 3 chars)":
    let grams1 = trigrams("a")
    check grams1.len == 0
    let grams2 = trigrams("ab")
    check grams2.len == 0

  test "trigrams of exactly 3 chars":
    let grams = trigrams("abc")
    check grams.len == 1

  test "trigrams of longer string":
    let grams = trigrams("hello")
    check grams.len > 0
    check grams.len == 3

  test "encodePostings with empty list":
    let encoded = encodePostingsSorted(@[])
    check encoded.len == 0

  test "encodePostings with single value":
    let encoded = encodePostingsSorted(@[1'u64, 2'u64])
    check encoded.len > 0

  test "encodePostings with sorted values":
    let values = @[1'u64, 2'u64, 3'u64, 100'u64]
    let encoded = encodePostingsSorted(values)
    check encoded.len > 0

  test "decodePostings with empty data":
    let decoded = decodePostings(@[])
    check decoded.ok
    check decoded.value.len == 0

  test "decodePostings roundtrip":
    let values = @[1'u64, 2'u64, 3'u64, 100'u64, 1000'u64]
    let encoded = encodePostingsSorted(values)
    let decoded = decodePostings(encoded)
    check decoded.ok
    check decoded.value == values

  test "addRowid to encoded data":
    let postings = @[1'u64, 5'u64, 10'u64]
    let encoded = encodePostingsSorted(postings)
    let result = addRowid(encoded, 7'u64)
    check result.ok
    let decoded = decodePostings(result.value)
    check decoded.ok
    check 7'u64 in decoded.value

  test "addRowid to empty data":
    let encoded = encodePostingsSorted(@[])
    let result = addRowid(encoded, 1'u64)
    check result.ok
    let decoded = decodePostings(result.value)
    check decoded.ok
    check 1'u64 in decoded.value

suite "Catalog Public API Extended":
  test "getTable returns error for non-existent table":
    let path = makeTempDb("decentdb_catalog_get_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let res = db.catalog.getTable("nonexistent")
    check not res.ok
    check res.err.code == ERR_SQL
    
    discard closeDb(db)

  test "dropTable with non-existent table":
    let path = makeTempDb("decentdb_catalog_drop_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let res = db.catalog.dropTable("nonexistent")
    check not res.ok
    check res.err.code == ERR_SQL
    
    discard closeDb(db)

  test "dropIndex with non-existent index":
    let path = makeTempDb("decentdb_catalog_drop_idx_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let res = db.catalog.dropIndex("nonexistent")
    check not res.ok
    check res.err.code == ERR_SQL
    
    discard closeDb(db)

  test "getIndexByName returns none for non-existent index":
    let path = makeTempDb("decentdb_catalog_idx_none.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let res = db.catalog.getIndexByName("nonexistent")
    check isNone(res)
    
    discard closeDb(db)

  test "getBtreeIndexForColumn returns none when no index":
    let path = makeTempDb("decentdb_catalog_btree_none.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let res = db.catalog.getBtreeIndexForColumn("items", "value")
    check isNone(res)
    
    discard closeDb(db)

  test "getTrigramIndexForColumn returns none when no index":
    let path = makeTempDb("decentdb_catalog_trigram_none.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let res = db.catalog.getTrigramIndexForColumn("items", "value")
    check isNone(res)
    
    discard closeDb(db)

  test "trigramBuffer operations":
    let path = makeTempDb("decentdb_catalog_trigram_ops.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    db.catalog.trigramBufferAdd("test_idx", 12345'u32, 100'u64)
    db.catalog.trigramBufferAdd("test_idx", 12345'u32, 200'u64)
    
    let delta = db.catalog.trigramDelta("test_idx", 12345'u32)
    check isSome(delta)
    let deltaVal = delta.unsafeGet()
    check deltaVal.adds.card == 2
    check 100'u64 in deltaVal.adds
    check 200'u64 in deltaVal.adds
    
    discard closeDb(db)

  test "clearTrigramDeltas":
    let path = makeTempDb("decentdb_catalog_clear_delta.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    db.catalog.trigramBufferAdd("test_idx", 12345'u32, 100'u64)
    db.catalog.clearTrigramDeltas()
    
    let delta = db.catalog.trigramDelta("test_idx", 12345'u32)
    check isNone(delta)
    
    discard closeDb(db)
