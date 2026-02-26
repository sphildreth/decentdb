import unittest, engine, os, strutils, record/record
import pager/db_header

suite "openDb corrupt files":
  test "file too short":
    let path = getTempDir() / "corrupt_short.ddb"
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    writeFile(path, "BADDA")  # only 5 bytes, < 128
    let r = openDb(path)
    echo "short file: ok=", r.ok, " err=", if not r.ok: r.err.message else: ""
    check not r.ok

  test "bad magic bytes":
    let path = getTempDir() / "corrupt_magic.ddb"
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    var buf = newString(128)
    for i in 0..<128: buf[i] = 'X'
    writeFile(path, buf)
    let r = openDb(path)
    echo "bad magic: ok=", r.ok, " err=", if not r.ok: r.err.message else: ""
    check not r.ok

  test "openDb nonexistent path error handling":
    let r = openDb("/nonexistent_directory_12345/foo.ddb")
    echo "nonexistent: ok=", r.ok, " err=", if not r.ok: r.err.message else: ""
    check not r.ok

  test "wrong format version":
    # Create a valid header then patch the version using encodeHeader to maintain checksum
    let path = getTempDir() / "corrupt_version.ddb"
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    # Create valid db first
    let dbRes = openDb(path)
    check dbRes.ok
    discard closeDb(dbRes.value)
    # Write a header with bad format version 999 (encodeHeader computes correct checksum)
    var badHeader = DbHeader(formatVersion: 999'u32, pageSize: 4096'u32)
    let encBuf = encodeHeader(badHeader)
    var f = open(path, fmReadWriteExisting)
    f.setFilePos(0)
    discard f.writeBuffer(unsafeAddr encBuf[0], encBuf.len)
    f.close()
    let r2 = openDb(path)
    check not r2.ok
    check "format version" in r2.err.message.toLowerAscii() or "unsupported" in r2.err.message.toLowerAscii()
