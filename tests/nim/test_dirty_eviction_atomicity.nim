import unittest, os
import strutils
import locks

import pager/pager
import pager/db_header
import vfs/os_vfs

suite "Dirty Eviction Atomicity":
  setup:
    let dbPath = "test_dirty_eviction.db"
    if dirExists(dbPath): removeDir(dbPath)
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")

  teardown:
    let dbPath = "test_dirty_eviction.db"
    if dirExists(dbPath): removeDir(dbPath)
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")

  test "dirty eviction should not write to db file before commit":
    # Use internal APIs to force cache pressure while a transaction is active.
    # The invariant under test is: dirty pages created inside an uncommitted
    # transaction must never be written into the main DB file.
    let vfs = newOsVfs()
    let fileRes = vfs.open("test_dirty_eviction.db", fmReadWrite, true)
    require fileRes.ok
    let file = fileRes.value
    defer: discard vfs.close(file)
    
    # Initialize header
    var header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    require writeHeader(vfs, file, header).ok
    require vfs.truncate(file, int64(DefaultPageSize)).ok
    require vfs.fsync(file).ok
    
    # Create pager with a tiny cache.
    let pagerRes = newPager(vfs, file, 2)
    require pagerRes.ok
    let pager = pagerRes.value
    defer: discard closePager(pager)

    beginTxnPageTracking(pager)
    
    # Allocate + dirty two pages. With cache capacity=2, both pages will remain
    # resident (dirty + unpinned).
    var pageIds: seq[PageId] = @[]
    for _ in 0 ..< 2:
      let allocRes = pager.allocatePage()
      check allocRes.ok
      pageIds.add(allocRes.value)

    for pid in pageIds:
      let pinRes = pinPage(pager, pid)
      check pinRes.ok
      let entry = pinRes.value

      let marker = "UNCOMMITTED:" & $pid
      acquire(entry.lock)
      for i in 0 ..< marker.len:
        entry.data[i] = marker[i]
      release(entry.lock)
      discard unpinPage(pager, entry, dirty = true)

      
    # Now attempt to load a third page. This forces eviction.
    # The pager should refuse to evict dirty pages while inTransaction.
    let thirdRes = pager.allocatePage()
    check thirdRes.ok
    let thirdId = thirdRes.value
    let thirdPin = pinPage(pager, thirdId)
    check(not thirdPin.ok)
    
    let fSize = getFileSize("test_dirty_eviction.db")

    var foundDirtyInFile = false
    for pid in pageIds:
      let marker = "UNCOMMITTED:" & $pid
      let offset = int64(pid - 1) * int64(DefaultPageSize)
      if offset < fSize:
        var buf = newString(DefaultPageSize)
        let readRes = vfs.readStr(file, offset, buf)
        if readRes.ok:
          if marker in buf:
            echo "FOUND DIRTY DATA IN FILE for page ", pid
            foundDirtyInFile = true
            
    if foundDirtyInFile:
      echo "FAILURE: Uncommitted dirty pages flushed to DB file!"
      fail()
    else:
      echo "Success: No dirty data found in DB file."

