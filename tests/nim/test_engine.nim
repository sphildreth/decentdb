import unittest
import os

import engine
import pager/db_header
import vfs/os_vfs

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Engine":
  test "openDb rejects short header":
    let path = makeTempDb("decentdb_engine_short.db")
    var f: File
    check open(f, path, fmWrite)
    let data = newSeq[byte](HeaderSize div 2)
    discard f.writeBuffer(data[0].addr, data.len)
    close(f)
    let res = openDb(path)
    check not res.ok

  test "openDb rejects wrong format version":
    let path = makeTempDb("decentdb_engine_bad_version.db")
    let vfs = newOsVfs()
    let fileRes = vfs.open(path, fmReadWrite, true)
    check fileRes.ok
    let file = fileRes.value
    var header = DbHeader(
      formatVersion: FormatVersion + 1,
      pageSize: DefaultPageSize,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    check writeHeader(vfs, file, header).ok
    discard vfs.close(file)
    let res = openDb(path)
    check not res.ok

  test "openDb rejects wrong page size":
    let path = makeTempDb("decentdb_engine_bad_pagesize.db")
    let vfs = newOsVfs()
    let fileRes = vfs.open(path, fmReadWrite, true)
    check fileRes.ok
    let file = fileRes.value
    var header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize + 4096,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    check writeHeader(vfs, file, header).ok
    discard vfs.close(file)
    let res = openDb(path)
    check not res.ok

  test "openDb initializes new file":
    let path = makeTempDb("decentdb_engine_new.db")
    let res = openDb(path)
    check res.ok
    let db = res.value
    check db.isOpen
    check db.pageSize == DefaultPageSize
    discard closeDb(db)
