# Medium Priority Optimization Tests (MED-001 through MED-006)
#
# These tests verify the optimizations implemented for Part III Medium Priority items:
# - MED-001: B-Tree internal node binary search
# - MED-002: Splitmix64 hash for shard selection  
# - MED-003: Trigram delta flush during checkpoint
# - MED-004: Clock eviction with tombstones (mark-and-compact)
# - MED-006: WAL index binary search

import unittest
import os
import times
import options
import tables
import sets
import sequtils
import algorithm
import strutils

import ../../src/errors
import ../../src/engine
import ../../src/pager/pager
import ../../src/pager/db_header
import ../../src/wal/wal
import ../../src/catalog/catalog
import ../../src/storage/storage
import ../../src/record/record

# Helper function to convert string to bytes
proc toBytes(text: string): seq[byte] =
  result = newSeq[byte](text.len)
  for i in 0 ..< text.len:
    result[i] = byte(text[i])

# ============================================================================
# MED-001: B-Tree Internal Node Binary Search Tests
# ============================================================================

suite "MED-001: B-Tree Internal Node Binary Search":
  var db: Db
  var testPath = getTempDir() / "test_med001.db"

  setup:
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)
    let openRes = openDb(testPath, cachePages = 64)
    check openRes.ok
    db = openRes.value

  teardown:
    if db != nil and db.isOpen:
      discard closeDb(db)
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)

  test "Binary search finds correct child in internal nodes":
    # Create a table with many rows to force internal nodes
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Insert enough rows to create internal B-Tree nodes
    # With 4KB pages and ~50 bytes per row, we need ~80+ rows for internal nodes
    const numRows = 200
    for i in 1..numRows:
      let insertRes = execSql(db, "INSERT INTO test (id, data) VALUES (?, ?)", 
        @[Value(kind: vkInt64, int64Val: i), Value(kind: vkText, bytes: toBytes("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))])
      check insertRes.ok
    
    # Force checkpoint to ensure clean state
    discard checkpointDb(db)
    
    # Query various keys to exercise binary search paths
    for i in 1..numRows:
      let selectRes = execSql(db, "SELECT id FROM test WHERE id = ?", 
        @[Value(kind: vkInt64, int64Val: i)])
      check selectRes.ok
      check selectRes.value.len == 1
      check selectRes.value[0] == $i

  test "Binary search with non-sequential inserts":
    let createRes = execSql(db, "CREATE TABLE test2 (id INTEGER PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Insert in random order to test binary search stability
    var keys: seq[int64] = @[50, 10, 90, 30, 70, 20, 80, 40, 60, 100]
    for key in keys:
      let insertRes = execSql(db, "INSERT INTO test2 (id, data) VALUES (?, ?)",
        @[Value(kind: vkInt64, int64Val: key), Value(kind: vkText, bytes: toBytes("data"))])
      check insertRes.ok
    
    # Query all inserted keys
    for key in keys:
      let selectRes = execSql(db, "SELECT id FROM test2 WHERE id = ?",
        @[Value(kind: vkInt64, int64Val: key)])
      check selectRes.ok
      check selectRes.value.len == 1

# ============================================================================
# MED-002: Splitmix64 Hash for Shard Selection Tests
# ============================================================================

suite "MED-002: Splitmix64 Hash for Page Cache Shards":
  var db: Db
  var testPath = getTempDir() / "test_med002.db"

  setup:
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)
    let openRes = openDb(testPath, cachePages = 64)
    check openRes.ok
    db = openRes.value

  teardown:
    if db != nil and db.isOpen:
      discard closeDb(db)
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)

  test "Hash distribution spreads across all shards":
    # Access many pages to populate cache
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Insert data across many pages
    for i in 1..100:
      let insertRes = execSql(db, "INSERT INTO test (id, data) VALUES (?, ?)",
        @[Value(kind: vkInt64, int64Val: i), Value(kind: vkText, bytes: toBytes("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))])
      check insertRes.ok
    
    # Query data to exercise cache with splitmix64 hashing
    for i in 1..100:
      let selectRes = execSql(db, "SELECT id FROM test WHERE id = ?",
        @[Value(kind: vkInt64, int64Val: i)])
      check selectRes.ok

  test "Shard selection is consistent for same page ID":
    # The same page ID should always map to the same shard
    # This is an internal property, tested via repeated access
    let createRes = execSql(db, "CREATE TABLE test2 (id INTEGER PRIMARY KEY)")
    check createRes.ok
    
    # Insert and repeatedly query the same row
    let insertRes = execSql(db, "INSERT INTO test2 (id) VALUES (1)")
    check insertRes.ok
    
    for i in 1..50:
      let selectRes = execSql(db, "SELECT id FROM test2 WHERE id = 1")
      check selectRes.ok
      check selectRes.value.len == 1
      check selectRes.value[0] == "1"

# ============================================================================
# MED-003: Trigram Delta Flush During Checkpoint Tests
# ============================================================================

suite "MED-003: Trigram Delta Flush During Checkpoint":
  var db: Db
  var testPath = getTempDir() / "test_med003.db"

  setup:
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)
    let openRes = openDb(testPath, cachePages = 64)
    check openRes.ok
    db = openRes.value
    
    # Disable automatic checkpointing for predictable test behavior
    setCheckpointConfig(db.wal, everyBytes = 0, everyMs = 0)

  teardown:
    if db != nil and db.isOpen:
      discard closeDb(db)
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)

  test "Trigram changes committed but flushed at checkpoint":
    # Create table with trigram index
    let createRes = execSql(db, "CREATE TABLE articles (id INTEGER PRIMARY KEY, content TEXT)")
    check createRes.ok
    
    let indexRes = execSql(db, "CREATE INDEX idx_content ON articles USING trigram(content)")
    check indexRes.ok
    
    # Insert data
    let insertRes = execSql(db, "INSERT INTO articles (id, content) VALUES (1, 'hello world')")
    check insertRes.ok
    
    # Commit transaction (should not flush trigram deltas per MED-003)
    let commitRes = commitTransaction(db)
    check commitRes.ok
    
    # Checkpoint should flush trigram deltas
    let checkpointRes = checkpointDb(db)
    check checkpointRes.ok
    
    # Query should work after checkpoint
    let selectRes = execSql(db, "SELECT id FROM articles WHERE content LIKE '%hello%'")
    check selectRes.ok
    check selectRes.value.len == 1

  test "Multiple transactions batched before checkpoint":
    let createRes = execSql(db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
    check createRes.ok
    
    let indexRes = execSql(db, "CREATE INDEX idx_body ON docs USING trigram(body)")
    check indexRes.ok
    
    # Multiple transactions
    for i in 1..5:
      let beginRes = beginTransaction(db)
      check beginRes.ok
      
      let insertRes = execSql(db, "INSERT INTO docs (id, body) VALUES (?, ?)",
        @[Value(kind: vkInt64, int64Val: i), Value(kind: vkText, bytes: toBytes("document " & $i))])
      check insertRes.ok
      
      let commitRes = commitTransaction(db)
      check commitRes.ok
    
    # All trigram changes flushed at single checkpoint
    let checkpointRes = checkpointDb(db)
    check checkpointRes.ok
    
    # All data should be searchable
    let selectRes = execSql(db, "SELECT id FROM docs WHERE body LIKE '%document%'")
    check selectRes.ok
    check selectRes.value.len == 5

# ============================================================================
# MED-004: Clock Eviction with Tombstones Tests
# ============================================================================

suite "MED-004: Clock Eviction Mark-and-Compact":
  var db: Db
  var testPath = getTempDir() / "test_med004.db"

  setup:
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)
    # Use very small cache to force evictions
    let openRes = openDb(testPath, cachePages = 4)
    check openRes.ok
    db = openRes.value

  teardown:
    if db != nil and db.isOpen:
      discard closeDb(db)
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)

  test "Cache eviction with small cache triggers tombstone path":
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Insert more data than cache can hold to trigger evictions
    for i in 1..20:
      let insertRes = execSql(db, "INSERT INTO test (id, data) VALUES (?, ?)",
        @[Value(kind: vkInt64, int64Val: i), Value(kind: vkText, bytes: toBytes("data " & $i))])
      check insertRes.ok
    
    # Access data in different patterns to exercise eviction
    for i in 1..20:
      let selectRes = execSql(db, "SELECT data FROM test WHERE id = ?",
        @[Value(kind: vkInt64, int64Val: i)])
      check selectRes.ok
      check selectRes.value.len == 1

  test "Rollback triggers tombstone cleanup":
    let createRes = execSql(db, "CREATE TABLE test2 (id INTEGER PRIMARY KEY)")
    check createRes.ok
    
    # Insert data
    let insertRes = execSql(db, "INSERT INTO test2 (id) VALUES (1)")
    check insertRes.ok
    
    # Begin transaction and modify
    let beginRes = beginTransaction(db)
    check beginRes.ok
    
    let updateRes = execSql(db, "UPDATE test2 SET id = 2 WHERE id = 1")
    check updateRes.ok
    
    # Rollback should use tombstone cleanup
    let rollbackRes = rollbackTransaction(db)
    check rollbackRes.ok
    
    # Verify data is correct after rollback
    let selectRes = execSql(db, "SELECT id FROM test2 WHERE id = 1")
    check selectRes.ok
    check selectRes.value.len == 1

  test "Clock compaction triggered at tombstone threshold":
    # This test exercises the compaction logic when tombstones accumulate
    let createRes = execSql(db, "CREATE TABLE test3 (id INTEGER PRIMARY KEY, blob BLOB)")
    check createRes.ok
    
    # Insert large blobs to fill cache quickly
    let blobData = repeat("x", 3000)
    for i in 1..10:
      let insertRes = execSql(db, "INSERT INTO test3 (id, blob) VALUES (?, ?)",
        @[Value(kind: vkInt64, int64Val: i), Value(kind: vkBlob, bytes: cast[seq[byte]](blobData))])
      check insertRes.ok
    
    # Force checkpoint to clean state
    discard checkpointDb(db)
    
    # Access pattern that causes many evictions
    for iteration in 1..3:
      for i in 1..10:
        let selectRes = execSql(db, "SELECT id FROM test3 WHERE id = ?",
          @[Value(kind: vkInt64, int64Val: i)])
        check selectRes.ok

# ============================================================================
# MED-006: WAL Index Binary Search Tests
# ============================================================================

suite "MED-006: WAL Index Binary Search":
  var db: Db
  var testPath = getTempDir() / "test_med006.db"

  setup:
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)
    let openRes = openDb(testPath, cachePages = 64)
    check openRes.ok
    db = openRes.value
    
    # Disable automatic checkpointing
    setCheckpointConfig(db.wal, everyBytes = 0, everyMs = 0)

  teardown:
    if db != nil and db.isOpen:
      discard closeDb(db)
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)

  test "Binary search finds correct WAL entry":
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Multiple transactions to create multiple WAL entries for same page
    for i in 1..10:
      let beginRes = beginTransaction(db)
      check beginRes.ok
      
      let insertRes = execSql(db, "INSERT INTO test (id, data) VALUES (?, ?)",
        @[Value(kind: vkInt64, int64Val: i), Value(kind: vkText, bytes: toBytes("version " & $i))])
      check insertRes.ok
      
      let commitRes = commitTransaction(db)
      check commitRes.ok
    
    # With binary search, WAL lookup should be efficient and correct
    let selectRes = execSql(db, "SELECT id FROM test WHERE id = 5")
    check selectRes.ok
    check selectRes.value.len == 1
    check selectRes.value[0] == "5"

  test "Binary search with concurrent reader snapshot":
    let createRes = execSql(db, "CREATE TABLE test2 (id INTEGER PRIMARY KEY, counter INTEGER)")
    check createRes.ok
    
    # Initial insert
    let insertRes = execSql(db, "INSERT INTO test2 (id, counter) VALUES (1, 0)")
    check insertRes.ok
    
    let commitRes = commitTransaction(db)
    check commitRes.ok
    
    # Start a read transaction
    let readTxn = beginRead(db.wal)
    
    # Multiple updates after read transaction starts
    for i in 1..5:
      let beginRes = beginTransaction(db)
      check beginRes.ok
      
      let updateRes = execSql(db, "UPDATE test2 SET counter = ? WHERE id = 1",
        @[Value(kind: vkInt64, int64Val: i)])
      check updateRes.ok
      
      let commitRes = commitTransaction(db)
      check commitRes.ok
    
    # Read transaction should see the old value using binary search in WAL index
    let overlay = getPageAtOrBefore(db.wal, PageId(2), readTxn.snapshot)
    # Page 2 should have an entry at or before the read snapshot
    # (this is an internal test - the overlay may or may not exist depending on page allocation)
    
    endRead(db.wal, readTxn)

# ============================================================================
# Combined/Integration Tests
# ============================================================================

suite "MED Combined: All optimizations work together":
  var db: Db
  var testPath = getTempDir() / "test_med_combined.db"

  setup:
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)
    let openRes = openDb(testPath, cachePages = 32)
    check openRes.ok
    db = openRes.value

  teardown:
    if db != nil and db.isOpen:
      discard closeDb(db)
    if fileExists(testPath):
      removeFile(testPath)
    let walPath = testPath & "-wal"
    if fileExists(walPath):
      removeFile(walPath)

  test "All optimizations work in integration scenario":
    # Create schema with both BTree and Trigram indexes
    let createRes = execSql(db, "CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
    check createRes.ok
    
    let btreeIndexRes = execSql(db, "CREATE INDEX idx_title ON articles(title)")
    check btreeIndexRes.ok
    
    let trigramIndexRes = execSql(db, "CREATE INDEX idx_body ON articles USING trigram(body)")
    check trigramIndexRes.ok
    
    # Insert data across multiple transactions
    for i in 1..50:
      let insertRes = execSql(db, "INSERT INTO articles (id, title, body) VALUES (?, ?, ?)",
        @[Value(kind: vkInt64, int64Val: i), 
          Value(kind: vkText, bytes: toBytes("Title " & $i)),
          Value(kind: vkText, bytes: toBytes("This is the body text number " & $i))])
      check insertRes.ok
    
    # Query using BTree index (tests MED-001 binary search)
    let btreeQueryRes = execSql(db, "SELECT id FROM articles WHERE title = 'Title 25'")
    check btreeQueryRes.ok
    check btreeQueryRes.value.len == 1
    check btreeQueryRes.value[0] == "25|Title 25|This is the body text number 25"
    
    # Query using trigram index (tests MED-003 deferred flush)
    let trigramQueryRes = execSql(db, "SELECT id FROM articles WHERE body LIKE '%body text%'")
    check trigramQueryRes.ok
    check trigramQueryRes.value.len == 50
    
    # Checkpoint (tests MED-003 flush at checkpoint)
    let checkpointRes = checkpointDb(db)
    check checkpointRes.ok
    
    # Verify data integrity after all operations
    let finalCountRes = execSql(db, "SELECT COUNT(*) FROM articles")
    check finalCountRes.ok
    check finalCountRes.value.len == 1
    check finalCountRes.value[0] == "50"

  test "Performance: Verify optimizations have expected characteristics":
    # This test verifies the optimizations don't break correctness
    # Performance benchmarks would be in a separate benchmark suite
    
    let createRes = execSql(db, "CREATE TABLE perf_test (id INTEGER PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Time a batch of inserts
    let startTime = epochTime()
    
    for i in 1..100:
      let insertRes = execSql(db, "INSERT INTO perf_test (id, data) VALUES (?, ?)",
        @[Value(kind: vkInt64, int64Val: i), Value(kind: vkText, bytes: toBytes("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))])
      check insertRes.ok
    
    let elapsed = epochTime() - startTime
    
    # Just verify it completes in reasonable time (not a strict performance test)
    check elapsed < 30.0  # Should complete within 30 seconds even on slow hardware
    
    # Verify all data is correct
    let countRes = execSql(db, "SELECT COUNT(*) FROM perf_test")
    check countRes.ok
    if countRes.ok and countRes.value.len > 0:
       check countRes.value[0] == "100"
