import unittest
import os

import engine
import pager/pager
import pager/db_header
import wal/wal
import vfs/types
import options

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

suite "WAL Extra":
  test "commit failpoint releases writer":
    let path = makeTempDb("decentdb_wal_failpoint.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value

    wal.setFailpoint("wal_fsync", WalFailpoint(kind: wfError))
    let pageRes = allocatePage(pager)
    check pageRes.ok
    var data = newSeq[byte](pager.pageSize)
    data[0] = 1
    let writerRes = beginWrite(wal)
    check writerRes.ok
    let writer = writerRes.value
    check writer.writePage(pageRes.value, data).ok
    let commitRes = commit(writer)
    check not commitRes.ok

    wal.clearFailpoints()
    let writer2Res = beginWrite(wal)
    check writer2Res.ok
    let writer2 = writer2Res.value
    check writer2.writePage(pageRes.value, data).ok
    check commit(writer2).ok

    discard closePager(pager)
    discard closeDb(db)

  test "recover stops on invalid frame type":
    let path = makeTempDb("decentdb_wal_bad_frame_type.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let walRes = newWal(db.vfs, path & ".wal")
    check walRes.ok
    let wal = walRes.value

    # Write a malformed frame directly.
    let payload = @[byte(1), byte(2), byte(3)]
    var buf = newSeq[byte](1 + 4 + payload.len + 8)
    buf[0] = 0xFF'u8
    writeU32LE(buf, 1, 1'u32)
    for i, b in payload:
      buf[5 + i] = b
    # Trailer left as zeros (checksum reserved)
    discard db.vfs.write(wal.file, int64(0), buf)

    check recover(wal).ok
    let snap = wal.beginRead()
    check isNone(wal.getPageAtOrBefore(PageId(1), snap.snapshot))
    wal.endRead(snap)

    discard closeDb(db)
