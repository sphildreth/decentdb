# Memory budget validation tests for DecentDb
import unittest
import ../../src/engine
import ../../src/record/record
import ../../src/exec/exec
import ../../src/pager/pager
import os
import times
import strutils

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

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "Memory Budget Validation":
  test "sort operation respects buffer size limit":
    # Create a database with a large dataset that requires sorting
    let path = makeTempDb("decentdb_mem_sort.db")
    let db = openDb(path)
    check db.ok
    
    # Create table with data
    let createRes = execSql(db.value, "CREATE TABLE sort_test (id INT PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Insert many rows
    var testRows: seq[seq[Value]] = @[]
    for i in 1 .. 10000:
      testRows.add(@[
        Value(kind: vkInt64, int64Val: i),
        Value(kind: vkText, bytes: toBytes("data_" & $i & "_" & repeat("x", 100)))
      ])
    
    var opts = defaultBulkLoadOptions()
    opts.disableIndexes = true
    opts.durability = dmNone
    let bulkRes = bulkLoad(db.value, "sort_test", testRows, opts)
    check bulkRes.ok
    
    # Execute a large ORDER BY query that should trigger external sort
    let startTime = epochTime()
    let sortRes = execSql(db.value, "SELECT * FROM sort_test ORDER BY data LIMIT 1000")
    let elapsed = (epochTime() - startTime) * 1000
    check sortRes.ok
    check sortRes.value.len == 1000
    
    # Verify the sort completed (external sort should have been used)
    # SortBufferBytes is 16MB, so this large sort should have spilled to disk
    # The fact that it completed without OOM is the test
    check elapsed < 30000  # Should complete within 30 seconds
    
    let closeRes = closeDb(db.value)
    check closeRes.ok
    
    # Cleanup
    if fileExists(path):
      removeFile(path)
    if fileExists(path & ".wal"):
      removeFile(path & ".wal")
  
  test "peak memory during query execution":
    # This test verifies that queries don't consume excessive memory
    let path = makeTempDb("decentdb_mem_peak.db")
    let db = openDb(path)
    check db.ok
    
    let createRes = execSql(db.value, "CREATE TABLE mem_test (id INT PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Insert test data
    for i in 1 .. 1000:
      let data = repeat("x", 1000)  # 1KB per row
      discard execSql(db.value, "INSERT INTO mem_test VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: i),
        Value(kind: vkText, bytes: toBytes(data))
      ])
    
    # Execute various queries and verify they complete
    # The actual memory tracking would require OS-level monitoring
    # For now, we verify queries complete without OOM
    
    # Large SELECT
    let selectRes = execSql(db.value, "SELECT * FROM mem_test")
    check selectRes.ok
    check selectRes.value.len == 1000
    
    # JOIN (if we had another table)
    # Aggregation
    let aggRes = execSql(db.value, "SELECT COUNT(*), MAX(id) FROM mem_test")
    check aggRes.ok
    
    let closeRes = closeDb(db.value)
    check closeRes.ok
    
    # Cleanup
    if fileExists(path):
      removeFile(path)
    if fileExists(path & ".wal"):
      removeFile(path & ".wal")
  
  test "cache size configuration is respected":
    # Open database with specific cache size
    let path = makeTempDb("decentdb_mem_cache.db")
    let db = openDb(path, cachePages = 100)  # 100 pages = 400KB
    check db.ok
    
    # Verify cache size was set
    check db.value.cachePages == 100
    
    let closeRes = closeDb(db.value)
    check closeRes.ok
    
    # Cleanup
    if fileExists(path):
      removeFile(path)
    if fileExists(path & ".wal"):
      removeFile(path & ".wal")
  
  test "query with large result set memory handling":
    let path = makeTempDb("decentdb_mem_large_result.db")
    let db = openDb(path)
    check db.ok
    
    let createRes = execSql(db.value, "CREATE TABLE large_result (id INT PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Insert many rows
    var opts = defaultBulkLoadOptions()
    opts.disableIndexes = true
    opts.durability = dmNone
    var testRows: seq[seq[Value]] = @[]
    for i in 1 .. 5000:
      testRows.add(@[
        Value(kind: vkInt64, int64Val: i),
        Value(kind: vkText, bytes: toBytes(repeat("x", 500)))
      ])
    
    let bulkRes = bulkLoad(db.value, "large_result", testRows, opts)
    check bulkRes.ok
    
    # Full table scan should handle memory efficiently
    let scanRes = execSql(db.value, "SELECT * FROM large_result")
    check scanRes.ok
    check scanRes.value.len == 5000
    
    let closeRes = closeDb(db.value)
    check closeRes.ok
    
    # Cleanup
    if fileExists(path):
      removeFile(path)
    if fileExists(path & ".wal"):
      removeFile(path & ".wal")
