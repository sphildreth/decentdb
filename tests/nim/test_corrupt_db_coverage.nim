import unittest
import os
import strutils
import engine
import record/record

## Tests openDb error paths triggered by corrupt/malformed database files.
## These cover error handling branches in openDb (engine.nim) including
## the ORC cleanup paths when openDb fails partway through.

suite "Corrupt DB file handling":

  test "openDb with file shorter than header":
    let path = getTempDir() / "test_corrupt_short.ddb"
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    writeFile(path, "SHORT")  # 5 bytes < 128 (HeaderSize)
    let r = openDb(path)
    check not r.ok
    check "too short" in r.err.message.toLowerAscii() or "Header" in r.err.message

  test "openDb with bad magic bytes":
    let path = getTempDir() / "test_corrupt_magic.ddb"
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    var buf = newString(128)
    for i in 0..<128: buf[i] = 'X'
    writeFile(path, buf)
    let r = openDb(path)
    check not r.ok
    check "magic" in r.err.message.toLowerAscii() or "Magic" in r.err.message

  test "openDb with header checksum mismatch":
    let path = getTempDir() / "test_corrupt_cksum.ddb"
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    # Create a valid DB first
    let dbRes = openDb(path)
    check dbRes.ok
    discard closeDb(dbRes.value)
    # Patch a byte inside the header to corrupt the checksum
    var contents = readFile(path)
    if contents.len >= 128:
      # Flip byte 30 (inside schemaCookie area, after checksum at 24-27)
      contents[30] = char(contents[30].byte xor 0xFF)
      writeFile(path, contents)
    let r2 = openDb(path)
    check not r2.ok
    check "checksum" in r2.err.message.toLowerAscii() or "Header" in r2.err.message

  test "openDb with nonexistent directory path":
    let r = openDb("/nonexistent_dir_xyz123/foo.ddb")
    check not r.ok

  test "openDb with 64-byte header (too short)":
    let path = getTempDir() / "test_corrupt_64.ddb"
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    var buf = newString(64)
    for i in 0..<8: buf[i] = "DECENTDB"[i]  # partial magic
    for i in 8..<64: buf[i] = '\0'
    writeFile(path, buf)
    let r = openDb(path)
    check not r.ok

  test "openDb with exactly 128 bytes but bad magic":
    let path = getTempDir() / "test_corrupt_128bad.ddb"
    defer:
      if fileExists(path): removeFile(path)
      if fileExists(path & "-wal"): removeFile(path & "-wal")
    var buf = newString(128)
    for i in 0..<128: buf[i] = '\0'
    # Write "BADMAGIC" instead of "DECENTDB"
    for i, c in "BADMAGIC":
      buf[i] = c
    writeFile(path, buf)
    let r = openDb(path)
    check not r.ok
