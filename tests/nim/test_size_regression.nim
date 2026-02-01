import unittest
import os
import engine
import record/record

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  if fileExists(path):
    removeFile(path)
  path

proc getFileSize(path: string): int64 =
  try:
    return getFileInfo(path).size
  except:
    return 0

suite "Size Regression":
  test "compact ints vs large ints":
    # Dataset A: Small integers (1..1000)
    let pathA = makeTempDb("decentdb_size_small.db")
    let dbA = openDb(pathA).value
    check execSql(dbA, "CREATE TABLE t (id INT)").ok
    check execSql(dbA, "BEGIN").ok
    for i in 1..1000:
      discard execSql(dbA, "INSERT INTO t (id) VALUES ($1)", @[Value(kind: vkInt64, int64Val: int64(i))])
    check execSql(dbA, "COMMIT").ok
    # Force flush/checkpoint (simple way: close db)
    discard closeDb(dbA)
    let sizeA = getFileSize(pathA)

    # Dataset B: Large integers (near int64.high)
    let pathB = makeTempDb("decentdb_size_large.db")
    let dbB = openDb(pathB).value
    check execSql(dbB, "CREATE TABLE t (id INT)").ok
    check execSql(dbB, "BEGIN").ok
    for i in 1..1000:
      discard execSql(dbB, "INSERT INTO t (id) VALUES ($1)", @[Value(kind: vkInt64, int64Val: int64.high - int64(i))])
    check execSql(dbB, "COMMIT").ok
    discard closeDb(dbB)
    let sizeB = getFileSize(pathB)

    echo "Size A (small ints): ", sizeA
    echo "Size B (large ints): ", sizeB

    # In old format, sizes would be identical (or very close).
    # In new format, A should be smaller.
    # Note: Page size is 4KB. 
    # A: 1000 rows * (16 header + 3 payload) = 19000 bytes ~ 5 pages.
    # B: 1000 rows * (16 header + 12 payload) = 28000 bytes ~ 7 pages.
    
    check sizeA < sizeB
    check sizeA > 0
    check sizeB > 0

    # Ensure difference is significant (at least 1 page difference)
    check sizeB - sizeA >= 4096

  test "mixed dataset size sanity":
    # 1000 rows of (id small, data text)
    let path = makeTempDb("decentdb_size_sanity.db")
    let db = openDb(path).value
    check execSql(db, "CREATE TABLE t (id INT, data TEXT)").ok
    check execSql(db, "BEGIN").ok
    for i in 1..1000:
      discard execSql(db, "INSERT INTO t VALUES ($1, 'test')", @[Value(kind: vkInt64, int64Val: int64(i))])
    check execSql(db, "COMMIT").ok
    discard closeDb(db)
    
    let size = getFileSize(path)
    # 1000 rows * (16 header + 3 id + 6 data + 1 rec header) = 26000 bytes.
    # + Metadata pages (header, catalog, root).
    # Should be around 32KB - 40KB (8-10 pages).
    # If it were old format: 17000 payload + 16000 header = 33000 -> similar page count but denser.
    # Wait, 33KB is 8.05 pages -> 9 pages.
    # 26KB is 6.3 pages -> 7 pages.
    
    echo "Size mixed: ", size
    # Loose upper bound check to catch massive regressions
    check size <= 65536 # 64KB
