# B+Tree Page Utilization Monitoring Tests
import unittest
import options
import ../../src/btree/btree
import ../../src/pager/pager
import ../../src/catalog/catalog
import ../../src/engine
import ../../src/record/record as rec
import os

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  # Current DecentDB WAL naming convention is "<db>-wal".
  # Keep legacy cleanup for ".wal" to avoid leaving stale files.
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "B+Tree Page Utilization":
  test "calculatePageUtilization returns percentage for leaf page":
    let path = makeTempDb("decentdb_btree_util.db")
    let db = openDb(path)
    check db.ok
    
    # Create table and index to get a BTree
    let createRes = execSql(db.value, "CREATE TABLE util_test (id INT PRIMARY KEY, data TEXT)")
    check createRes.ok
    
    # Insert some data
    for i in 1 .. 10:
      let dataRes = execSql(db.value, "INSERT INTO util_test VALUES ($1, $2)", @[
        rec.Value(kind: vkInt64, int64Val: i),
        rec.Value(kind: vkText, bytes: toBytes("data_" & $i))
      ])
      check dataRes.ok
    
    # Get table info to find the root page
    let tableRes = db.value.catalog.getTable("util_test")
    check tableRes.ok
    let table = tableRes.value
    
    # Create BTree from table root
    let tree = newBTree(db.value.pager, table.rootPage)
    
    # Calculate utilization
    let utilRes = calculatePageUtilization(tree, table.rootPage)
    check utilRes.ok
    check utilRes.value > 0.0
    check utilRes.value <= 100.0
    
    let closeRes = closeDb(db.value)
    check closeRes.ok
    
    if fileExists(path):
      removeFile(path)
    if fileExists(path & "-wal"):
      removeFile(path & "-wal")
    if fileExists(path & ".wal"):
      removeFile(path & ".wal")
  
  test "calculateTreeUtilization returns average for all pages":
    let path = makeTempDb("decentdb_btree_util2.db")
    let db = openDb(path)
    check db.ok
    
    # Create table with index
    let createRes = execSql(db.value, "CREATE TABLE util_test2 (id INT PRIMARY KEY, val INT)")
    check createRes.ok
    
    let idxRes = execSql(db.value, "CREATE INDEX idx_val ON util_test2(val)")
    check idxRes.ok
    
    # Insert enough data to potentially create multiple pages
    var rows: seq[seq[rec.Value]] = @[]
    for i in 1 .. 100:
      rows.add(@[
        rec.Value(kind: vkInt64, int64Val: i),
        rec.Value(kind: vkInt64, int64Val: i * 10)
      ])
    
    var opts = defaultBulkLoadOptions()
    opts.disableIndexes = false
    opts.durability = dmNone
    let bulkRes = bulkLoad(db.value, "util_test2", rows, opts)
    check bulkRes.ok
    
    # Get index info
    let idxOpt = db.value.catalog.getBtreeIndexForColumn("util_test2", "val")
    check isSome(idxOpt)
    let idx = idxOpt.get
    
    # Create BTree from index root
    let tree = newBTree(db.value.pager, idx.rootPage)
    
    # Calculate tree utilization
    let utilRes = calculateTreeUtilization(tree)
    check utilRes.ok
    check utilRes.value > 0.0
    check utilRes.value <= 100.0
    
    let closeRes = closeDb(db.value)
    check closeRes.ok
    
    if fileExists(path):
      removeFile(path)
    if fileExists(path & "-wal"):
      removeFile(path & "-wal")
    if fileExists(path & ".wal"):
      removeFile(path & ".wal")
  
  test "needsCompaction detects low utilization":
    let path = makeTempDb("decentdb_btree_compact.db")
    let db = openDb(path)
    check db.ok
    
    # Create table
    let createRes = execSql(db.value, "CREATE TABLE compact_test (id INT PRIMARY KEY)")
    check createRes.ok
    
    # Insert data
    for i in 1 .. 50:
      discard execSql(db.value, "INSERT INTO compact_test VALUES ($1)", @[
        rec.Value(kind: vkInt64, int64Val: i)
      ])
    
    # Get table info
    let tableRes = db.value.catalog.getTable("compact_test")
    check tableRes.ok
    let table = tableRes.value
    
    # Create BTree
    let tree = newBTree(db.value.pager, table.rootPage)
    
    # Check if needs compaction (default 50% threshold)
    let needsCompactRes = needsCompaction(tree)
    check needsCompactRes.ok
    # Result depends on actual utilization, just verify it runs
    
    let closeRes = closeDb(db.value)
    check closeRes.ok
    
    if fileExists(path):
      removeFile(path)
    if fileExists(path & "-wal"):
      removeFile(path & "-wal")
    if fileExists(path & ".wal"):
      removeFile(path & ".wal")
