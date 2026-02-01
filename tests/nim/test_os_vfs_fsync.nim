import os
import strutils
import unittest
import vfs/os_vfs
import vfs/types
import errors

suite "OS VFS fsync tests":
  setup:
    let testDir = getTempDir() / "decentdb_fsync_test"
    createDir(testDir)
    let testFile = testDir / "test.db"
    # Clean up any existing test file
    if fileExists(testFile):
      removeFile(testFile)

  teardown:
    let testDir = getTempDir() / "decentdb_fsync_test"
    let testFile = testDir / "test.db"
    if fileExists(testFile):
      removeFile(testFile)
    removeDir(testDir)

  test "fsync should successfully sync data to disk":
    let vfs = newOsVfs()
    let openRes = vfs.open(testFile, fmReadWrite, true)
    check openRes.ok
    
    let file = openRes.value
    
    # Write some data
    let data = "Test data for fsync"
    let writeRes = vfs.writeStr(file, 0, data)
    check writeRes.ok
    check writeRes.value == data.len
    
    # Call fsync - this should succeed
    let syncRes = vfs.fsync(file)
    check syncRes.ok
    
    # Close file
    let closeRes = vfs.close(file)
    check closeRes.ok
    
    # Verify file exists and has data
    check fileExists(testFile)
    let content = readFile(testFile)
    check content == data

  test "fsync on empty file should succeed":
    let vfs = newOsVfs()
    let openRes = vfs.open(testFile, fmReadWrite, true)
    check openRes.ok
    
    let file = openRes.value
    
    # fsync on empty file should succeed
    let syncRes = vfs.fsync(file)
    check syncRes.ok
    
    # Close file
    let closeRes = vfs.close(file)
    check closeRes.ok

  test "fsync after multiple writes should persist all data":
    let vfs = newOsVfs()
    let openRes = vfs.open(testFile, fmReadWrite, true)
    check openRes.ok
    
    let file = openRes.value
    
    # Write data in chunks
    var offset = 0
    var expected = ""
    for i in 0..9:
      let chunk = "Chunk " & $i & " data "
      let writeRes = vfs.writeStr(file, int64(offset), chunk)
      check writeRes.ok
      offset += chunk.len
      expected.add(chunk)
    
    # Sync all writes
    let syncRes = vfs.fsync(file)
    check syncRes.ok
    
    # Close file
    let closeRes = vfs.close(file)
    check closeRes.ok
    
    # Verify all data persisted
    let content = readFile(testFile)
    check content == expected

  test "write should auto-flush buffers":
    let vfs = newOsVfs()
    let openRes = vfs.open(testFile, fmReadWrite, true)
    check openRes.ok
    
    let file = openRes.value
    
    # Write data (write method should auto-flush)
    let data = "Auto-flush test data"
    let writeRes = vfs.writeStr(file, 0, data)
    check writeRes.ok
    
    # Close file (should persist everything)
    let closeRes = vfs.close(file)
    check closeRes.ok
    
    # Verify data persisted without explicit fsync
    let content = readFile(testFile)
    check content == data

  test "close should work without fsync":
    let vfs = newOsVfs()
    let openRes = vfs.open(testFile, fmReadWrite, true)
    check openRes.ok
    
    let file = openRes.value
    
    # Write and close without explicit fsync
    let data = "Data without explicit fsync"
    let writeRes = vfs.writeStr(file, 0, data)
    check writeRes.ok
    
    let closeRes = vfs.close(file)
    check closeRes.ok
    
    # Verify data is there
    let content = readFile(testFile)
    check content == data
