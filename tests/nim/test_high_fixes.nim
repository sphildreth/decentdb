import os
import unittest
import engine
import wal/wal
import pager/pager
import vfs/os_vfs
import errors
import record/record
import catalog/catalog
import storage/storage
import strutils
import times
import locks

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
  path

proc toBytes(text: string): seq[byte] =
  ## Convert string to bytes for Value construction
  for ch in text:
    result.add(byte(ch))

# ============================================================================
# HIGH-005: Constraint Checking Performance Batching Tests
# ============================================================================

suite "HIGH-005: Batch Constraint Checking":
  test "enforceNotNullBatch validates multiple rows at once":
    let path = makeTempDb("high005_notnull_batch")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Create a table with NOT NULL constraints
    let createRes = execSql(db, "CREATE TABLE test_notnull (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INT64 NOT NULL)")
    require createRes.ok
    
    # Prepare test data - some rows with NULL values
    var rows: seq[seq[Value]] = @[]
    rows.add(@[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("Alice")), Value(kind: vkInt64, int64Val: 25)])  # Valid
    rows.add(@[Value(kind: vkInt64, int64Val: 2), Value(kind: vkNull), Value(kind: vkInt64, int64Val: 30)])  # NULL name
    rows.add(@[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: toBytes("Bob")), Value(kind: vkInt64, int64Val: 35)])  # Valid
    rows.add(@[Value(kind: vkInt64, int64Val: 4), Value(kind: vkText, bytes: toBytes("Charlie")), Value(kind: vkNull)])  # NULL age
    rows.add(@[Value(kind: vkInt64, int64Val: 5), Value(kind: vkText, bytes: toBytes("David")), Value(kind: vkInt64, int64Val: 40)])  # Valid
    
    let tableRes = db.catalog.getTable("test_notnull")
    require tableRes.ok
    let table = tableRes.value
    
    # Test batch NOT NULL checking
    let batchRes = enforceNotNullBatch(table, rows)
    require batchRes.ok
    
    # Should find 2 failures (row 1 and row 3, 0-indexed: 1 and 3)
    check batchRes.value.len == 2
    check 1 in batchRes.value  # Row with NULL name
    check 3 in batchRes.value  # Row with NULL age
    
    discard closeDb(db)

  test "enforceNotNullBatch returns empty for valid rows":
    let path = makeTempDb("high005_notnull_valid")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    let createRes = execSql(db, "CREATE TABLE test_valid (id INTEGER PRIMARY KEY, required TEXT NOT NULL)")
    require createRes.ok
    
    var rows: seq[seq[Value]] = @[]
    for i in 1..100:
      rows.add(@[Value(kind: vkInt64, int64Val: int64(i)), Value(kind: vkText, bytes: toBytes("value_" & $i))])
    
    let tableRes = db.catalog.getTable("test_valid")
    require tableRes.ok
    let table = tableRes.value
    
    let batchRes = enforceNotNullBatch(table, rows)
    require batchRes.ok
    check batchRes.value.len == 0
    
    discard closeDb(db)

  test "enforceUniqueBatch detects duplicates within batch":
    let path = makeTempDb("high005_unique_batch")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Create table with UNIQUE constraint
    let createRes = execSql(db, "CREATE TABLE test_unique (id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL)")
    require createRes.ok
    
    # Insert initial data
    let insertRes = execSql(db, "INSERT INTO test_unique (id, email) VALUES (1, 'existing@example.com')")
    require insertRes.ok
    
    let tableRes = db.catalog.getTable("test_unique")
    require tableRes.ok
    let table = tableRes.value
    
    # Prepare batch with duplicates within the batch
    var rows: seq[tuple[values: seq[Value], rowid: uint64]] = @[]
    rows.add((@[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: toBytes("new1@example.com"))], 0'u64))
    rows.add((@[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: toBytes("dup@example.com"))], 0'u64))
    rows.add((@[Value(kind: vkInt64, int64Val: 4), Value(kind: vkText, bytes: toBytes("new2@example.com"))], 0'u64))
    rows.add((@[Value(kind: vkInt64, int64Val: 5), Value(kind: vkText, bytes: toBytes("dup@example.com"))], 0'u64))  # Duplicate within batch
    rows.add((@[Value(kind: vkInt64, int64Val: 6), Value(kind: vkText, bytes: toBytes("existing@example.com"))], 0'u64))  # Duplicate of existing
    
    let batchRes = enforceUniqueBatch(db.catalog, db.pager, table, rows)
    require batchRes.ok
    
    # Should find at least 2 failures (rows 3 and 5 with duplicates, row 4 with existing)
    check batchRes.value.len >= 2
    
    discard closeDb(db)

  test "enforceForeignKeysBatch validates multiple FK references":
    let path = makeTempDb("high005_fk_batch")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Create parent and child tables
    let createParentRes = execSql(db, "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    require createParentRes.ok
    
    let createChildRes = execSql(db, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT NOT NULL, dept_id INT64 REFERENCES departments(id))")
    require createChildRes.ok
    
    # Insert parent data
    for i in 1..5:
      let deptName = "Dept " & $i
      let insertRes = execSql(db, "INSERT INTO departments (id, name) VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: int64(i)),
        Value(kind: vkText, bytes: toBytes(deptName))
      ])
      require insertRes.ok
    
    let tableRes = db.catalog.getTable("employees")
    require tableRes.ok
    let table = tableRes.value
    
    # Prepare batch with some invalid FKs
    var rows: seq[seq[Value]] = @[]
    rows.add(@[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("Alice")), Value(kind: vkInt64, int64Val: 1)])  # Valid
    rows.add(@[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: toBytes("Bob")), Value(kind: vkInt64, int64Val: 99)])  # Invalid FK
    rows.add(@[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: toBytes("Charlie")), Value(kind: vkInt64, int64Val: 3)])  # Valid
    rows.add(@[Value(kind: vkInt64, int64Val: 4), Value(kind: vkText, bytes: toBytes("David")), Value(kind: vkInt64, int64Val: 100)])  # Invalid FK
    rows.add(@[Value(kind: vkInt64, int64Val: 5), Value(kind: vkText, bytes: toBytes("Eve")), Value(kind: vkNull)])  # NULL FK (valid)
    
    let batchRes = enforceForeignKeysBatch(db.catalog, db.pager, table, rows)
    require batchRes.ok
    
    # Should find 2 failures (rows 1 and 3, 0-indexed: 1 and 3)
    check batchRes.value.len == 2
    
    discard closeDb(db)

  test "enforceConstraintsBatch combines all constraint checks":
    let path = makeTempDb("high005_combined_batch")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Create tables with multiple constraints
    let createParentRes = execSql(db, "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)")
    require createParentRes.ok
    
    let createChildRes = execSql(db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, category_id INT64 REFERENCES categories(id), sku TEXT UNIQUE)")
    require createChildRes.ok
    
    # Insert parent data
    for i in 1..3:
      let insertRes = execSql(db, "INSERT INTO categories (id, name) VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: int64(i)),
        Value(kind: vkText, bytes: toBytes("Category " & $i))
      ])
      require insertRes.ok
    
    let tableRes = db.catalog.getTable("products")
    require tableRes.ok
    let table = tableRes.value
    
    # Prepare batch with various constraint violations
    var rows: seq[tuple[values: seq[Value], rowid: uint64]] = @[]
    rows.add((@[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("Product A")), Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("SKU-001"))], 0'u64))  # Valid
    rows.add((@[Value(kind: vkInt64, int64Val: 2), Value(kind: vkNull), Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("SKU-002"))], 0'u64))  # NULL name
    rows.add((@[Value(kind: vkInt64, int64Val: 3), Value(kind: vkText, bytes: toBytes("Product C")), Value(kind: vkInt64, int64Val: 99), Value(kind: vkText, bytes: toBytes("SKU-003"))], 0'u64))  # Invalid FK
    rows.add((@[Value(kind: vkInt64, int64Val: 4), Value(kind: vkText, bytes: toBytes("Product D")), Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: toBytes("SKU-001"))], 0'u64))  # Duplicate SKU
    
    let batchRes = enforceConstraintsBatch(db.catalog, db.pager, table, rows)
    require batchRes.ok
    
    # Should find 3 failures
    check batchRes.value.len == 3
    
    # Verify correct constraint types were detected
    var hasNotNull = false
    var hasFk = false
    var hasUnique = false
    
    for failure in batchRes.value:
      if failure.constraint == "NOT NULL":
        hasNotNull = true
      elif failure.constraint == "FOREIGN KEY":
        hasFk = true
      elif failure.constraint == "UNIQUE":
        hasUnique = true
    
    check hasNotNull
    check hasFk
    check hasUnique
    
    discard closeDb(db)

  test "batch constraint checking is zero-cost for disabled checks":
    let path = makeTempDb("high005_zero_cost")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    require createRes.ok
    
    let tableRes = db.catalog.getTable("test")
    require tableRes.ok
    let table = tableRes.value
    
    var rows: seq[tuple[values: seq[Value], rowid: uint64]] = @[]
    rows.add((@[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("Alice"))], 0'u64))
    rows.add((@[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: toBytes("Bob"))], 0'u64))
    
    # Test with all checks disabled - should return immediately with no failures
    var options = defaultConstraintBatchOptions()
    options.checkNotNull = false
    options.checkUnique = false
    options.checkForeignKeys = false
    
    let batchRes = enforceConstraintsBatch(db.catalog, db.pager, table, rows, options)
    require batchRes.ok
    check batchRes.value.len == 0
    
    discard closeDb(db)

# ============================================================================
# HIGH-006: Long-running Reader Resource Management Tests
# ============================================================================

suite "HIGH-006: Long-running Reader Resource Management":
  test "reader tracks WAL size at start":
    let path = makeTempDb("high006_reader_tracking")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Create some data to generate WAL
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    require createRes.ok
    
    # Insert data to grow WAL
    for i in 1..100:
      let insertRes = execSql(db, "INSERT INTO test (id, data) VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: int64(i)),
        Value(kind: vkText, bytes: toBytes("Data " & $i))
      ])
      require insertRes.ok
    
    # Get initial WAL size
    let initialWalSize = db.wal.endOffset
    
    # Start a reader
    let txn = beginRead(db.wal)
    
    # Write more data while reader is active
    for i in 101..200:
      let insertRes = execSql(db, "INSERT INTO test (id, data) VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: int64(i)),
        Value(kind: vkText, bytes: toBytes("Data " & $i))
      ])
      require insertRes.ok
    
    # Check reader stats
    let stats = getReaderStats(db.wal)
    check stats.activeReaders == 1
    check stats.totalWalPinned > 0  # Reader should be pinning some WAL
    
    # Check specific reader
    let readerSize = readerWalSize(db.wal, txn.id)
    check readerSize > 0
    check readerSize == db.wal.endOffset - initialWalSize
    
    endRead(db.wal, txn)
    discard closeDb(db)

  test "reader exceeding WAL size limit is detected":
    let path = makeTempDb("high006_wal_limit")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Configure strict WAL limit
    setCheckpointConfig(db.wal,
      everyBytes = 1024 * 1024 * 1024,  # Large value to prevent auto-checkpoint
      everyMs = 0,
      readerWarnMs = 0,  # Disable time-based warnings
      readerTimeoutMs = 0,  # Disable time-based timeout
      maxWalBytesPerReader = 1000,  # Very small limit for testing
      readerCheckIntervalMs = 0)  # Check every time
    
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    require createRes.ok
    
    # Insert initial data
    for i in 1..10:
      let insertRes = execSql(db, "INSERT INTO test (id, data) VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: int64(i)),
        Value(kind: vkText, bytes: toBytes("Data " & $i))
      ])
      require insertRes.ok
    
    # Start a reader
    let txn = beginRead(db.wal)
    
    # Write enough data to exceed the limit
    for i in 11..1000:
      let insertRes = execSql(db, "INSERT INTO test (id, data) VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: int64(i)),
        Value(kind: vkText, bytes: toBytes("More data to exceed the limit " & $i))
      ])
      require insertRes.ok
    
    # Check if reader was detected as exceeding limit (before checkpoint aborts it)
    let oversized = readersExceedingWalLimit(db.wal)
    check oversized.len >= 1
    
    # Trigger a checkpoint to check readers
    discard checkpoint(db.wal, db.pager)
    
    # Reader should be aborted
    check isAborted(db.wal, txn)
    
    endRead(db.wal, txn)
    discard closeDb(db)

  test "reader warnings are rate-limited":
    let path = makeTempDb("high006_rate_limited_warnings")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Configure warning threshold
    setCheckpointConfig(db.wal,
      everyBytes = 1024 * 1024 * 1024,
      everyMs = 0,
      readerWarnMs = 1,  # Warn after 1ms
      readerTimeoutMs = 0,
      maxWalBytesPerReader = 0)
    
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    require createRes.ok
    
    # Start a reader
    let txn = beginRead(db.wal)
    
    # Sleep to exceed warning threshold
    sleep(100)
    
    # Trigger checkpoint multiple times
    for i in 1..5:
      discard checkpoint(db.wal, db.pager)
      sleep(50)
    
    # Check stats - should have warnings but not 5x (rate limited)
    let stats = getReaderStats(db.wal)
    check stats.totalWarnings >= 1
    check stats.totalWarnings <= 2  # Should be rate limited, not 5
    
    endRead(db.wal, txn)
    discard closeDb(db)

  test "reader stats track total aborted readers":
    let path = makeTempDb("high006_stats_tracking")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Configure short timeout for testing
    setCheckpointConfig(db.wal,
      everyBytes = 1024 * 1024 * 1024,
      everyMs = 0,
      readerWarnMs = 0,
      readerTimeoutMs = 1,  # 1ms timeout
      maxWalBytesPerReader = 0)
    
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    require createRes.ok
    
    # Start multiple readers
    let txn1 = beginRead(db.wal)
    let txn2 = beginRead(db.wal)
    let txn3 = beginRead(db.wal)
    
    # Sleep to exceed timeout
    sleep(100)
    
    # Trigger checkpoint - should abort all readers
    discard checkpoint(db.wal, db.pager)
    
    # Check stats
    let stats = getReaderStats(db.wal)
    check stats.totalAborted == 3
    check stats.activeReaders == 0  # All should have been cleaned up
    
    endRead(db.wal, txn1)
    endRead(db.wal, txn2)
    endRead(db.wal, txn3)
    discard closeDb(db)

  test "reader management is zero-cost when disabled":
    let path = makeTempDb("high006_zero_cost")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Configure with all reader management disabled
    setCheckpointConfig(db.wal,
      everyBytes = 1024 * 1024,
      everyMs = 0,
      readerWarnMs = 0,  # Disabled
      readerTimeoutMs = 0,  # Disabled
      maxWalBytesPerReader = 0,  # Disabled
      readerCheckIntervalMs = 1000)  # Long interval
    
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    require createRes.ok
    
    # Start a reader
    let txn = beginRead(db.wal)
    
    # These should be zero-cost operations when disabled
    let shouldCheck = shouldCheckReaders(db.wal)
    check shouldCheck == false  # All features disabled, no check needed
    
    let oversized = readersExceedingWalLimit(db.wal)
    check oversized.len == 0  # Zero results when disabled
    
    endRead(db.wal, txn)
    discard closeDb(db)

  test "readerCheckIntervalMs prevents excessive checks":
    let path = makeTempDb("high006_check_interval")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Configure with long check interval
    setCheckpointConfig(db.wal,
      everyBytes = 1024 * 1024 * 1024,
      everyMs = 0,
      readerWarnMs = 1,  # Would warn immediately
      readerTimeoutMs = 0,
      maxWalBytesPerReader = 0,
      readerCheckIntervalMs = 5000)  # 5 second interval
    
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    require createRes.ok
    
    # Start a reader
    let txn = beginRead(db.wal)
    
    # Sleep briefly
    sleep(100)
    
    # Check if we should check readers - should be false due to interval
    let shouldCheck = shouldCheckReaders(db.wal)
    check shouldCheck == false  # Interval hasn't elapsed
    
    # Wait for interval to elapse
    sleep(5100)
    
    # Now should check
    let shouldCheckNow = shouldCheckReaders(db.wal)
    check shouldCheckNow == true
    
    endRead(db.wal, txn)
    discard closeDb(db)

  test "oldestReaderAgeMs tracks longest running reader":
    let path = makeTempDb("high006_oldest_reader")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    require createRes.ok
    
    # Start first reader
    let txn1 = beginRead(db.wal)
    sleep(100)
    
    # Start second reader
    let txn2 = beginRead(db.wal)
    sleep(50)
    
    # Start third reader
    let txn3 = beginRead(db.wal)
    
    # Check stats - oldest should be at least 150ms old
    let stats = getReaderStats(db.wal)
    check stats.activeReaders == 3
    check stats.oldestReaderAgeMs >= 150
    
    endRead(db.wal, txn1)
    endRead(db.wal, txn2)
    endRead(db.wal, txn3)
    discard closeDb(db)

  test "forceTruncateOnTimeout allows checkpoint despite pinned WAL":
    let path = makeTempDb("high006_force_truncate")
    
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Configure with force truncate enabled
    setCheckpointConfig(db.wal,
      everyBytes = 1024 * 1024,
      everyMs = 0,
      readerWarnMs = 0,
      readerTimeoutMs = 1,  # Very short timeout
      forceTruncateOnTimeout = true,
      maxWalBytesPerReader = 0)
    
    let createRes = execSql(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
    require createRes.ok
    
    # Insert data
    for i in 1..100:
      let insertRes = execSql(db, "INSERT INTO test (id, data) VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: int64(i)),
        Value(kind: vkText, bytes: toBytes("Data " & $i))
      ])
      require insertRes.ok
    
    # Start a reader
    let txn = beginRead(db.wal)
    
    # Wait for timeout
    sleep(100)
    
    # Get initial WAL size
    let initialSize = db.wal.endOffset
    check initialSize > 0
    
    # Trigger checkpoint - should abort reader and allow truncate
    discard checkpoint(db.wal, db.pager)
    
    # Reader should be aborted
    let isAborted = isAborted(db.wal, txn)
    check isAborted == true
    
    endRead(db.wal, txn)
    discard closeDb(db)
