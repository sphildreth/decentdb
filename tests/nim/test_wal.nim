import unittest
import os
import times
import locks
import tables
import engine
import pager/pager
import wal/wal

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

proc bytesToPageString(data: seq[byte]): string =
  if data.len == 0:
    return ""
  result = newString(data.len)
  copyMem(addr result[0], unsafeAddr data[0], data.len)

suite "WAL":
  test "committed visible, uncommitted not visible":
    let path = makeTempDb("decentdb_wal_visibility.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    discard recover(wal)

    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    var data = newSeq[byte](pager.pageSize)
    for i in 0 ..< data.len:
      data[i] = 7
    let dataStr = bytesToPageString(data)
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, data).ok
    let snapBefore = wal.beginRead()
    let readBefore = readPageWithSnapshot(pager, wal, snapBefore.snapshot, pageId)
    check readBefore.ok
    check readBefore.value != dataStr
    wal.endRead(snapBefore)
    check commit(writer).ok
    let snapAfter = wal.beginRead()
    let readAfter = readPageWithSnapshot(pager, wal, snapAfter.snapshot, pageId)
    check readAfter.ok
    check readAfter.value == dataStr
    wal.endRead(snapAfter)
    discard closePager(pager)
    discard closeDb(db)

  test "torn write ignored on recovery":
    let path = makeTempDb("decentdb_wal_torn.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    wal.setFailpoint("wal_write_frame", WalFailpoint(kind: wfPartial, partialBytes: 8))
    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    var data = newSeq[byte](pager.pageSize)
    for i in 0 ..< data.len:
      data[i] = 9
    let dataStr = bytesToPageString(data)
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, data).ok
    let commitRes = commit(writer)
    check not commitRes.ok
    discard recover(wal)
    let snap = wal.beginRead()
    let readRes = readPageWithSnapshot(pager, wal, snap.snapshot, pageId)
    check readRes.ok
    check readRes.value != dataStr
    wal.endRead(snap)
    discard closePager(pager)
    discard closeDb(db)

  test "snapshot isolation for readers":
    let path = makeTempDb("decentdb_wal_snapshot.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    discard recover(wal)
    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    var initialBytes = newSeq[byte](pager.pageSize)
    for i in 0 ..< initialBytes.len:
      initialBytes[i] = 1
    let initial = bytesToPageString(initialBytes)
    check writePage(pager, pageId, initial).ok
    discard flushAll(pager)
    let snapOld = wal.beginRead()
    var updatedBytes = newSeq[byte](pager.pageSize)
    for i in 0 ..< updatedBytes.len:
      updatedBytes[i] = 2
    let updated = bytesToPageString(updatedBytes)
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, updatedBytes).ok
    check commit(writer).ok
    let readOld = readPageWithSnapshot(pager, wal, snapOld.snapshot, pageId)
    check readOld.ok
    check readOld.value == initial
    wal.endRead(snapOld)
    let snapNew = wal.beginRead()
    let readNew = readPageWithSnapshot(pager, wal, snapNew.snapshot, pageId)
    check readNew.ok
    check readNew.value == updated
    wal.endRead(snapNew)
    discard closePager(pager)
    discard closeDb(db)

  test "checkpoint truncates WAL when no readers":
    let path = makeTempDb("decentdb_wal_checkpoint.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    var data = newSeq[byte](pager.pageSize)
    for i in 0 ..< data.len:
      data[i] = 3
    let dataStr = bytesToPageString(data)
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, data).ok
    check commit(writer).ok
    let ckRes = checkpoint(wal, pager)
    check ckRes.ok
    let info = getFileInfo(path & ".wal")
    check info.size == 0
    let readRes = readPage(pager, pageId)
    check readRes.ok
    check readRes.value == dataStr
    discard closePager(pager)
    discard closeDb(db)

  test "checkpoint skips truncation with active readers":
    let path = makeTempDb("decentdb_wal_checkpoint_readers.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    var data = newSeq[byte](pager.pageSize)
    for i in 0 ..< data.len:
      data[i] = 4
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, data).ok
    let snap = wal.beginRead()
    check commit(writer).ok
    let ckRes = checkpoint(wal, pager)
    check ckRes.ok
    let info = getFileInfo(path & ".wal")
    check info.size > 0
    # Reader snapshot should still see the pre-update page image even after checkpoint.
    var zerosBytes = newSeq[byte](pager.pageSize)
    let zeros = bytesToPageString(zerosBytes)
    let readOld = readPageWithSnapshot(pager, wal, snap.snapshot, pageId)
    check readOld.ok
    check readOld.value == zeros
    wal.endRead(snap)
    discard closePager(pager)
    discard closeDb(db)

  test "checkpoint uses the best page version <= reader snapshot":
    let path = makeTempDb("decentdb_wal_checkpoint_best_version.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value

    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value

    var v1Bytes = newSeq[byte](pager.pageSize)
    for i in 0 ..< v1Bytes.len:
      v1Bytes[i] = 11
    let v1 = bytesToPageString(v1Bytes)

    var v2Bytes = newSeq[byte](pager.pageSize)
    for i in 0 ..< v2Bytes.len:
      v2Bytes[i] = 22
    let v2 = bytesToPageString(v2Bytes)

    # Commit v1.
    let w1Res = beginWrite(wal)
    check w1Res.ok
    let w1 = w1Res.value
    check w1.writePage(pageId, v1Bytes).ok
    check commit(w1).ok

    # Reader starts after v1 is committed.
    let snap = wal.beginRead()

    # Commit v2 (newer than reader snapshot).
    let w2Res = beginWrite(wal)
    check w2Res.ok
    let w2 = w2Res.value
    check w2.writePage(pageId, v2Bytes).ok
    check commit(w2).ok

    let ckRes = checkpoint(wal, pager)
    check ckRes.ok

    # The database file should be advanced to v1, not v2, while the reader is active.
    let base = readPageDirect(pager, pageId)
    check base.ok
    check base.value == v1
    check base.value != v2

    # The reader should still see v1 at its snapshot.
    let readSnap = readPageWithSnapshot(pager, wal, snap.snapshot, pageId)
    check readSnap.ok
    check readSnap.value == v1
    wal.endRead(snap)

    discard closePager(pager)
    discard closeDb(db)

  test "checkpoint failpoint returns error":
    let path = makeTempDb("decentdb_wal_checkpoint_fail.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    var data = newSeq[byte](pager.pageSize)
    for i in 0 ..< data.len:
      data[i] = 5
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, data).ok
    check commit(writer).ok
    wal.setFailpoint("checkpoint_write_page", WalFailpoint(kind: wfError))
    let ckRes = checkpoint(wal, pager)
    check not ckRes.ok
    check pager.header.lastCheckpointLsn == 0
    discard closePager(pager)
    discard closeDb(db)

  test "checkpoint fsync failpoint returns error":
    let path = makeTempDb("decentdb_wal_checkpoint_fsync_fail.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    var data = newSeq[byte](pager.pageSize)
    for i in 0 ..< data.len:
      data[i] = 8
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, data).ok
    check commit(writer).ok
    wal.setFailpoint("checkpoint_fsync", WalFailpoint(kind: wfError))
    let ckRes = checkpoint(wal, pager)
    check not ckRes.ok
    discard closePager(pager)
    discard closeDb(db)

  test "checkpoint warns on long-running readers":
    let path = makeTempDb("decentdb_wal_checkpoint_warn.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    wal.setCheckpointConfig(0, 0, readerWarnMs = 1)
    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    var data = newSeq[byte](pager.pageSize)
    for i in 0 ..< data.len:
      data[i] = 6
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, data).ok
    let snap = wal.beginRead()
    acquire(wal.readerLock)
    if wal.readers.hasKey(snap.id):
      var info = wal.readers[snap.id]
      info.started = epochTime() - 1.0
      wal.readers[snap.id] = info
    release(wal.readerLock)
    check commit(writer).ok
    let ckRes = checkpoint(wal, pager)
    check ckRes.ok
    let warnings = wal.takeWarnings()
    check warnings.len > 0
    wal.endRead(snap)
    discard closePager(pager)
    discard closeDb(db)

  test "checkpoint can force truncate on timeout":
    let path = makeTempDb("decentdb_wal_checkpoint_force.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    wal.setCheckpointConfig(0, 0, readerWarnMs = 0, readerTimeoutMs = 1, forceTruncateOnTimeout = true)
    let pageRes = allocatePage(pager)
    check pageRes.ok
    let pageId = pageRes.value
    var data = newSeq[byte](pager.pageSize)
    for i in 0 ..< data.len:
      data[i] = 7
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, data).ok
    let snap = wal.beginRead()
    acquire(wal.readerLock)
    if wal.readers.hasKey(snap.id):
      var info = wal.readers[snap.id]
      info.started = epochTime() - 1.0
      wal.readers[snap.id] = info
    release(wal.readerLock)
    check commit(writer).ok
    let ckRes = checkpoint(wal, pager)
    check ckRes.ok
    let info = getFileInfo(path & ".wal")
    check info.size == 0
    check wal.isAborted(snap)
    wal.endRead(snap)
    discard closePager(pager)
    discard closeDb(db)

  test "long-running reader does not prevent WAL growth bounds":
    # This test validates that WAL truncation happens despite long-running readers
    # when timeouts are enforced, preventing unbounded WAL growth.
    let path = makeTempDb("decentdb_wal_bounded_growth.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 16)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value
    # Set aggressive timeout: 10ms
    wal.setCheckpointConfig(0, 0, readerWarnMs = 0, readerTimeoutMs = 10, forceTruncateOnTimeout = true)
    
    # Start a long-running reader
    let writerRes1 = beginWrite(wal)
    check writerRes1.ok
    let writer1 = writerRes1.value
    let page1 = allocatePage(pager)
    check page1.ok
    var data1 = newSeq[byte](pager.pageSize)
    for i in 0 ..< data1.len:
      data1[i] = 1
    check writer1.writePage(page1.value, data1).ok
    check commit(writer1).ok
    
    let longReader = wal.beginRead()
    # Backdoor: manually set the reader's start time to be old
    acquire(wal.readerLock)
    if wal.readers.hasKey(longReader.id):
      var info = wal.readers[longReader.id]
      info.started = epochTime() - 1.0
      wal.readers[longReader.id] = info
    release(wal.readerLock)
    
    # Perform multiple write + checkpoint cycles
    for cycle in 0 ..< 5:
      let writerRes = beginWrite(wal)
      check writerRes.ok
      let writer = writerRes.value
      let pageRes = allocatePage(pager)
      check pageRes.ok
      var data = newSeq[byte](pager.pageSize)
      for i in 0 ..< data.len:
        data[i] = byte((cycle + 2) mod 256)
      check writer.writePage(pageRes.value, data).ok
      check commit(writer).ok
      
      # Checkpoint should abort the long reader and truncate
      let ckRes = checkpoint(wal, pager)
      check ckRes.ok
    
    # Verify the long reader was aborted
    check wal.isAborted(longReader)
    
    # Verify WAL was truncated (size should be 0 after the last checkpoint)
    let finalInfo = getFileInfo(path & ".wal")
    check finalInfo.size == 0
    
    wal.endRead(longReader)
    discard closePager(pager)
    discard closeDb(db)
