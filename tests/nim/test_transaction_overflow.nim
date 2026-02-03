import unittest
import os
import strutils
import engine
import exec/exec
import sql/sql
import vfs/os_vfs

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path): removeFile(path)
  if fileExists(path & "-wal"): removeFile(path & "-wal")
  if fileExists(path & ".wal"): removeFile(path & ".wal")
  path

suite "Transaction Overflow":
  test "large transaction with small cache succeeds via WAL flushing":
    let path = makeTempDb("test_transaction_overflow.db")
    
    # Open with very small cache (5 pages)
    # 1 page for header/root
    # 1 page for catalog
    # Leaves very little room.
    let res = openDb(path, cachePages = 5)
    require res.ok
    let db = res.value
    
    # Create table (autocommits)
    require execSql(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY, data TEXT)").ok
    
    # Start transaction
    require execSql(db, "BEGIN").ok
    
    # Insert 20 records. Each 2KB. 
    # 20 * 2KB = 40KB. 
    # Page size 4KB -> ~10 pages of data.
    # Cache size 5.
    # This MUST force eviction of dirty pages.
    # Without the fix, this would fail with "No evictable page in cache".
    
    let payload = repeat("x", 2000)
    
    for i in 1..20:
      let sql = "INSERT INTO foo (id, data) VALUES (" & $i & ", '" & payload & "')"
      let execRes = execSql(db, sql)
      if not execRes.ok:
        echo "Failed at insert ", i, ": ", execRes.err.message
        fail()
    
    # Verify we can still read back data inside the transaction
    # This verifies the "overlay" logic (reading from flushed pages) works
    let readRes = execSql(db, "SELECT count(*) FROM foo")
    check readRes.ok
    check readRes.value[0] == "20"
    
    # Commit
    let commitRes = execSql(db, "COMMIT")
    check commitRes.ok
    
    # Verify data after commit
    let finalRes = execSql(db, "SELECT count(*) FROM foo")
    check finalRes.ok
    check finalRes.value[0] == "20"
    
    discard closeDb(db)
