import os
import options
import unittest
import atomics
import wal/wal
import pager/pager
import vfs/os_vfs
import errors
import pager/db_header

suite "WAL checkpoint race condition tests":
  setup:
    let testDir = getTempDir() / "decentdb_checkpoint_race_test"
    createDir(testDir)
    let dbPath = testDir / "test.db"
    let walPath = dbPath & "-wal"
    
    # Clean up any existing files
    if fileExists(dbPath):
      removeFile(dbPath)
    if fileExists(walPath):
      removeFile(walPath)

  teardown:
    let testDir = getTempDir() / "decentdb_checkpoint_race_test"
    let dbPath = testDir / "test.db"
    let walPath = dbPath & "-wal"
    if fileExists(dbPath):
      removeFile(dbPath)
    if fileExists(walPath):
      removeFile(walPath)
    removeDir(testDir)

  test "checkpoint should not lose commits during I/O phase":
    # Create database and WAL
    let vfs = newOsVfs()
    
    # Create and initialize database file
    let fileRes = vfs.open(dbPath, fmReadWrite, true)
    check fileRes.ok
    let dbFile = fileRes.value
    
    # Write initial header and pad to page size
    let header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    let headerBytes = encodeHeader(header)
    var page1 = newString(DefaultPageSize)
    copyMem(addr page1[0], unsafeAddr headerBytes[0], headerBytes.len)
    let writeRes = vfs.writeStr(dbFile, 0, page1)
    check writeRes.ok
    
    let pagerRes = newPager(vfs, dbFile, cachePages = 100)
    check pagerRes.ok
    let pager = pagerRes.value
    
    let walRes = newWal(vfs, walPath)
    check walRes.ok
    let wal = walRes.value
    
    # Allocate pages through pager first
    let page2Res = allocatePage(pager)
    check page2Res.ok
    let page2Id = page2Res.value
    
    let page3Res = allocatePage(pager)
    check page3Res.ok
    let page3Id = page3Res.value
    
    # Commit some initial data
    var writer1 = beginWrite(wal)
    check writer1.ok
    
    var data1 = newSeq[byte](DefaultPageSize)
    data1[0] = 0xA1
    data1[1] = 0xB2
    let write1 = writePage(writer1.value, page2Id, data1)
    check write1.ok
    
    let commit1 = commit(writer1.value)
    check commit1.ok
    
    # Simulate concurrent write scenario
    var writer2 = beginWrite(wal)
    check writer2.ok
    
    var data2 = newSeq[byte](DefaultPageSize)
    data2[0] = 0xC3
    data2[1] = 0xD4
    let write2 = writePage(writer2.value, page3Id, data2)
    check write2.ok
    
    let commit2 = commit(writer2.value)
    check commit2.ok
    
    # Verify both commits are tracked
    let page2Overlay = wal.getPageAtOrBefore(page2Id, commit2.value)
    check page2Overlay.isSome
    
    let page3Overlay = wal.getPageAtOrBefore(page3Id, commit2.value)
    check page3Overlay.isSome
    
    # Run checkpoint
    let chkRes = checkpoint(wal, pager)
    check chkRes.ok
    
    # After checkpoint, the committed pages must be present in the main DB file.
    # (The WAL may have been truncated, so do not require WAL overlays to exist.)
    let page2FileRes = readPageDirect(pager, page2Id)
    check page2FileRes.ok
    check uint8(page2FileRes.value[0]) == 0xA1'u8
    check uint8(page2FileRes.value[1]) == 0xB2'u8

    let page3FileRes = readPageDirect(pager, page3Id)
    check page3FileRes.ok
    check uint8(page3FileRes.value[0]) == 0xC3'u8
    check uint8(page3FileRes.value[1]) == 0xD4'u8

  test "checkpoint preserves newer commits when truncation is unsafe":
    let vfs = newOsVfs()
    
    let fileRes = vfs.open(dbPath, fmReadWrite, true)
    check fileRes.ok
    let dbFile = fileRes.value
    
    let header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    let headerBytes = encodeHeader(header)
    var page1 = newString(DefaultPageSize)
    copyMem(addr page1[0], unsafeAddr headerBytes[0], headerBytes.len)
    let writeRes = vfs.writeStr(dbFile, 0, page1)
    check writeRes.ok
    
    let pagerRes = newPager(vfs, dbFile, cachePages = 100)
    check pagerRes.ok
    let pager = pagerRes.value
    
    let walRes = newWal(vfs, walPath)
    check walRes.ok
    let wal = walRes.value
    
    # Allocate pages first
    var pageIds: seq[PageId] = @[]
    for i in 0..4:
      let res = allocatePage(pager)
      check res.ok
      pageIds.add(res.value)
    
    # Commit first batch of pages
    var writer = beginWrite(wal)
    check writer.ok
    
    for i in 0..3:
      let data = newSeq[byte](DefaultPageSize)
      let wp = writePage(writer.value, pageIds[i], data)
      check wp.ok
    
    let commit1 = commit(writer.value)
    check commit1.ok
    
    # Record state before checkpoint
    let lsnBefore = commit1.value

    # Pin a reader snapshot at lsnBefore so truncation is unsafe.
    let reader = beginRead(wal)
    check reader.snapshot == lsnBefore
    
    # Simulate: new commit happens (simulating concurrent activity)
    var writer2 = beginWrite(wal)
    check writer2.ok
    
    let newData = newSeq[byte](DefaultPageSize)
    let wp2 = writePage(writer2.value, pageIds[4], newData)
    check wp2.ok
    
    let commit2 = commit(writer2.value)
    check commit2.ok
    check commit2.value > lsnBefore
    
    # Complete checkpoint
    let chkRes = checkpoint(wal, pager)
    check chkRes.ok
    
    # Verify checkpoint result: should only checkpoint up to the pinned reader snapshot.
    check chkRes.value <= lsnBefore

    # The newer commit must still be accessible via WAL overlay (cannot truncate).
    let page6Overlay = wal.getPageAtOrBefore(pageIds[4], commit2.value)
    check page6Overlay.isSome

    endRead(wal, reader)

  test "checkpoint with no new commits can safely truncate":
    let vfs = newOsVfs()
    
    let fileRes = vfs.open(dbPath, fmReadWrite, true)
    check fileRes.ok
    let dbFile = fileRes.value
    
    let header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    let headerBytes = encodeHeader(header)
    var page1 = newString(DefaultPageSize)
    copyMem(addr page1[0], unsafeAddr headerBytes[0], headerBytes.len)
    let writeRes = vfs.writeStr(dbFile, 0, page1)
    check writeRes.ok
    
    let pagerRes = newPager(vfs, dbFile, cachePages = 100)
    check pagerRes.ok
    let pager = pagerRes.value
    
    let walRes = newWal(vfs, walPath)
    check walRes.ok
    let wal = walRes.value
    
    # Allocate a page
    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    
    # Commit some data
    var writer = beginWrite(wal)
    check writer.ok
    
    let data = newSeq[byte](DefaultPageSize)
    let wp = writePage(writer.value, pageId, data)
    check wp.ok
    
    let commitRes = commit(writer.value)
    check commitRes.ok
    
    # Record WAL size before checkpoint
    let offsetBefore = wal.endOffset
    check offsetBefore > 0
    
    # Run checkpoint with no concurrent activity
    let chkRes = checkpoint(wal, pager)
    check chkRes.ok
    
    # WAL should be truncated to header-only (logical end reset to 0) because no new commits occurred
    check wal.walEnd.load(moAcquire) == 0
    check wal.endOffset == WalHeaderSize
