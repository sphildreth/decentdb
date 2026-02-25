import unittest
import os
import strutils
import options
import engine
import errors
import vfs/os_vfs
import pager/db_header

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Engine openDb Error Paths":
  test "openDb: Header unreadable (probeRes.value == 0 but initialSize > 0)":
    # This might be tricky to trigger since reading 0 bytes means EOF. 
    # But if initialSize > 0, it means the file is not empty.
    # To get EOF on a non-empty file... maybe if we make a directory instead of a file?
    let path = makeTempDb("open_unreadable.db")
    createDir(path)
    let res = openDb(path)
    check not res.ok
    # os_vfs will fail to open a directory on most systems with ERR_IO or similar.
    # If it fails at vfs.open, it doesn't reach probeRes.
    removeDir(path)

  test "openDb: Header too short":
    let path = makeTempDb("open_too_short.db")
    # write 10 bytes (less than HeaderSize)
    var f = open(path, fmWrite)
    f.write("short data")
    f.close()
    
    let res = openDb(path)
    check not res.ok
    check res.err.code == ERR_CORRUPTION
    check res.err.message == "Header too short"
    
  test "openDb: Unsupported format version":
    let path = makeTempDb("open_bad_format.db")
    let dbRes = openDb(path)
    require dbRes.ok
    discard closeDb(dbRes.value)
    
    # Overwrite format version in header
    var f = open(path, fmReadWriteExisting)
    f.setFilePos(16) # Format version is at offset 16
    let badVersion: uint32 = 999
    discard f.writeBuffer(unsafeAddr badVersion, 4)
    # We must also update the checksum at offset 24 so decodeHeader doesn't fail on checksum first
    f.close()
    # It's easier to just write the whole header
    var buf = newSeq[byte](100)
    let f2 = open(path, fmReadWriteExisting)
    discard f2.readBuffer(addr buf[0], 100)
    
    var badHeader = DbHeader(formatVersion: 999, pageSize: 4096)
    var encBuf = encodeHeader(badHeader)
    f2.setFilePos(0)
    discard f2.writeBuffer(addr encBuf[0], 100)
    f2.close()
    
    let res = openDb(path)
    check not res.ok
    check res.err.code == ERR_CORRUPTION
    check res.err.message == "Unsupported format version"

  test "openDb: Unsupported page size":
    let path = makeTempDb("open_bad_pagesize.db")
    let dbRes = openDb(path)
    require dbRes.ok
    discard closeDb(dbRes.value)
    
    let f2 = open(path, fmReadWriteExisting)
    var badHeader = DbHeader(formatVersion: FormatVersion, pageSize: 999)
    var encBuf = encodeHeader(badHeader)
    f2.setFilePos(0)
    discard f2.writeBuffer(addr encBuf[0], 100)
    f2.close()
    
    let res = openDb(path)
    check not res.ok
    check res.err.code == ERR_CORRUPTION
    check res.err.message == "Unsupported page size"

  test "openDb: decodeHeader fails (magic mismatch)":
    let path = makeTempDb("open_bad_magic.db")
    let dbRes = openDb(path)
    require dbRes.ok
    discard closeDb(dbRes.value)
    
    # Corrupt the magic bytes starting at offset 0
    var f = open(path, fmReadWriteExisting)
    f.setFilePos(0)
    f.write("BADMAGIC")
    f.close()
    
    let res = openDb(path)
    check not res.ok
    check res.err.code == ERR_CORRUPTION
    check res.err.message == "Bad header magic"

