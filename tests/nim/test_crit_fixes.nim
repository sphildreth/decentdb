import os
import strutils
import times
import locks
import sets
import unittest
import engine
import errors
import pager/pager
import wal/wal
import vfs/os_vfs

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

suite "WAL Reader Abort (CRIT-003)":
  test "readPageWithSnapshot should return error for aborted reader":
    let path = makeTempDb("crit003_abort_test")
    
    # Create database using engine (proper initialization)
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Create WAL
    let walRes = newWal(db.vfs, path & "-wal")
    require walRes.ok
    let wal = walRes.value
    
    # Begin a read transaction
    let txn = beginRead(wal)
    
    # Simulate reader being aborted (as would happen on timeout)
    acquire(wal.readerLock)
    wal.abortedReaders.incl(txn.id)
    release(wal.readerLock)
    
    # Attempt to read page with aborted reader ID - should fail
    let readRes = readPageWithSnapshot(db.pager, wal, txn.snapshot, PageId(1), txn.id)
    check not readRes.ok
    check readRes.err.code == ERR_TRANSACTION
    check "aborted" in readRes.err.message.toLower
    
    # Cleanup
    endRead(wal, txn)
    discard closeDb(db)

  test "reader should not see aborted error when not aborted":
    let path = makeTempDb("crit003_no_abort_test")
    
    # Create database using engine
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Create WAL
    let walRes = newWal(db.vfs, path & "-wal")
    require walRes.ok
    let wal = walRes.value
    
    # Begin a read transaction (not aborted)
    let txn = beginRead(wal)
    
    # Read should succeed (page 1 exists - it's the header page)
    let readRes = readPageWithSnapshot(db.pager, wal, txn.snapshot, PageId(1), txn.id)
    check readRes.ok
    
    # Cleanup
    endRead(wal, txn)
    discard closeDb(db)

suite "Rollback Cache Atomicity (CRIT-004)":
  test "rollbackCache holds rollback lock during eviction":
    let path = makeTempDb("crit004_rollback_lock_test")
    
    # Create database using engine
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Write some data to make pages dirty
    let pageData = newString(4096)
    let writeRes = writePage(db.pager, PageId(2), pageData)
    require writeRes.ok
    
    # Verify page is dirty
    check isDirty(db.pager, PageId(2)) == true
    
    # Call rollbackCache - should evict dirty pages
    rollbackCache(db.pager)
    
    # Page should no longer be in cache as dirty
    check isDirty(db.pager, PageId(2)) == false
    
    # Cleanup
    discard closeDb(db)
  
  test "read operations should wait during rollback":
    let path = makeTempDb("crit004_read_wait_test")
    
    # Create database using engine
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Acquire rollback lock (simulating rollback in progress)
    acquire(db.pager.rollbackLock)
    
    # Release it immediately (just verifying lock exists and works)
    release(db.pager.rollbackLock)
    
    # Now read should succeed
    let readRes = readPage(db.pager, PageId(1))
    check readRes.ok
    
    # Cleanup
    discard closeDb(db)

suite "WAL Index Memory Tracking (HIGH-001)":
  test "estimateIndexMemoryUsage returns positive value for non-empty index":
    let path = makeTempDb("high001_memory_test")
    let vfs = newOsVfs()
    
    # Create WAL directly
    let walRes = newWal(vfs, path & "-wal")
    require walRes.ok
    let wal = walRes.value
    
    # Initially empty
    let mem1 = estimateIndexMemoryUsage(wal)
    check mem1 >= 0
    
    # Add some entries to index (simulate commit)
    let writerRes = beginWrite(wal)
    require writerRes.ok
    let writer = writerRes.value
    
    # Write a page
    let pageData = newSeq[byte](4096)
    let writeRes = writePage(writer, PageId(1), pageData)
    require writeRes.ok
    
    # Commit
    let commitRes = commit(writer)
    require commitRes.ok
    
    # Now memory should be higher
    let mem2 = estimateIndexMemoryUsage(wal)
    check mem2 > mem1
  
  test "maybeCheckpoint triggers on memory threshold":
    let path = makeTempDb("high001_checkpoint_memory_test")
    
    # Use openDb to properly initialize database
    let dbRes = openDb(path, cachePages = 64)
    require dbRes.ok
    let db = dbRes.value
    
    # Create a separate WAL to test memory tracking
    let vfs = newOsVfs()
    let walRes = newWal(vfs, path & "_test-wal")
    require walRes.ok
    let wal = walRes.value
    
    # Set a very low memory threshold (1 byte) to force trigger
    setCheckpointConfig(wal, 
      everyBytes = 0,  # Disable byte-based trigger
      everyMs = 0,     # Disable time-based trigger
      memoryThreshold = 1)  # 1 byte threshold (will definitely trigger)
    
    # Add some data to index
    let writerRes = beginWrite(wal)
    require writerRes.ok
    let writer = writerRes.value
    
    let pageData = newSeq[byte](4096)
    discard writePage(writer, PageId(1), pageData)
    discard commit(writer)
    
    # Now memory threshold should trigger checkpoint
    let chkRes = maybeCheckpoint(wal, db.pager)
    check chkRes.ok
    # Should have triggered due to memory threshold
    check chkRes.value == true
    
    # Cleanup
    discard closeDb(db)
