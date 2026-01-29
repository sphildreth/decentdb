import unittest
import os
import times
import tables
import engine
import pager/pager
import wal/wal

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

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
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, data).ok
    let snapBefore = wal.beginRead()
    let readBefore = readPageWithSnapshot(pager, wal, snapBefore.snapshot, pageId)
    check readBefore.ok
    check readBefore.value != data
    wal.endRead(snapBefore)
    check commit(writer).ok
    let snapAfter = wal.beginRead()
    let readAfter = readPageWithSnapshot(pager, wal, snapAfter.snapshot, pageId)
    check readAfter.ok
    check readAfter.value == data
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
    check readRes.value != data
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
    var initial = newSeq[byte](pager.pageSize)
    for i in 0 ..< initial.len:
      initial[i] = 1
    check writePage(pager, pageId, initial).ok
    discard flushAll(pager)
    let snapOld = wal.beginRead()
    var updated = newSeq[byte](pager.pageSize)
    for i in 0 ..< updated.len:
      updated[i] = 2
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageId, updated).ok
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
    check readRes.value == data
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
    wal.readers[snap.id] = (snapshot: snap.snapshot, started: epochTime() - 1.0)
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
    wal.readers[snap.id] = (snapshot: snap.snapshot, started: epochTime() - 1.0)
    check commit(writer).ok
    let ckRes = checkpoint(wal, pager)
    check ckRes.ok
    let info = getFileInfo(path & ".wal")
    check info.size == 0
    check wal.isAborted(snap)
    wal.endRead(snap)
    discard closePager(pager)
    discard closeDb(db)
