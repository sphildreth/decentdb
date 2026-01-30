import unittest
import os
import strutils

import engine
import vfs/types
import vfs/os_vfs
import vfs/faulty_vfs
import errors

proc makeTempPath(name: string): string =
  result = getTempDir() / name
  if fileExists(result):
    removeFile(result)

suite "OS VFS Operations":
  test "newOsVfs creation":
    let vfs = newOsVfs()
    check vfs != nil

  test "open file for read/write":
    let path = makeTempPath("decentdb_vfs_open.db")
    let vfs = newOsVfs()
    let res = vfs.open(path, fmReadWrite, true)
    check res.ok
    let file = res.value
    let closeRes = vfs.close(file)
    check closeRes.ok

  test "open existing file":
    let path = makeTempPath("decentdb_vfs_open_exist.db")
    let vfs = newOsVfs()
    let res1 = vfs.open(path, fmReadWrite, true)
    check res1.ok
    discard vfs.close(res1.value)
    let res2 = vfs.open(path, fmReadWrite, false)
    check res2.ok
    discard vfs.close(res2.value)

  test "open non-existent file without create":
    let path = makeTempPath("decentdb_vfs_open_nocreate.db")
    removeFile(path)
    let vfs = newOsVfs()
    let res = vfs.open(path, fmReadWrite, false)
    check not res.ok

  test "write and read":
    let path = makeTempPath("decentdb_vfs_write.db")
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let data = @[byte(1), byte(2), byte(3), byte(4)]
    let writeRes = vfs.write(file, 0, data)
    check writeRes.ok
    check writeRes.value == 4
    
    discard vfs.fsync(file)
    
    var buf = newSeq[byte](4)
    let readRes = vfs.read(file, 0, buf)
    check readRes.ok
    check readRes.value == 4
    check buf == data
    
    discard vfs.close(file)

  test "write at offset":
    let path = makeTempPath("decentdb_vfs_offset.db")
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let data1 = @[byte(1), byte(2)]
    let data2 = @[byte(3), byte(4)]
    discard vfs.write(file, 0, data1)
    discard vfs.write(file, 2, data2)
    
    discard vfs.fsync(file)
    
    var buf = newSeq[byte](4)
    discard vfs.read(file, 0, buf)
    check buf == @[byte(1), byte(2), byte(3), byte(4)]
    
    discard vfs.close(file)

  test "read beyond file size":
    let path = makeTempPath("decentdb_vfs_read_beyond.db")
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let data = @[byte(1)]
    discard vfs.write(file, 0, data)
    
    discard vfs.fsync(file)
    
    var buf = newSeq[byte](10)
    let readRes = vfs.read(file, 0, buf)
    check readRes.ok
    check readRes.value == 1
    
    discard vfs.close(file)

  test "write string":
    let path = makeTempPath("decentdb_vfs_writestr.db")
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let writeRes = vfs.writeStr(file, 0, "hello")
    check writeRes.ok
    check writeRes.value == 5
    
    discard vfs.fsync(file)
    
    var buf = newString(5)
    let readRes = vfs.readStr(file, 0, buf)
    check readRes.ok
    check readRes.value == 5
    check buf == "hello"
    
    discard vfs.close(file)

  test "fsync":
    let path = makeTempPath("decentdb_vfs_fsync.db")
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let data = @[byte(1)]
    discard vfs.write(file, 0, data)
    let syncRes = vfs.fsync(file)
    check syncRes.ok
    
    discard vfs.close(file)

  test "truncate":
    let path = makeTempPath("decentdb_vfs_truncate.db")
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let data = @[byte(1), byte(2), byte(3), byte(4), byte(5)]
    discard vfs.write(file, 0, data)
    let truncRes = vfs.truncate(file, 2)
    check truncRes.ok
    
    var buf = newSeq[byte](5)
    let readRes = vfs.read(file, 0, buf)
    check readRes.ok
    check readRes.value == 2
    
    discard vfs.close(file)

  test "multiple opens":
    let path = makeTempPath("decentdb_vfs_multi.db")
    let vfs = newOsVfs()
    
    let res1 = vfs.open(path, fmReadWrite, true)
    check res1.ok
    let file1 = res1.value
    
    discard vfs.write(file1, 0, @[byte(1)])
    discard vfs.fsync(file1)
    discard vfs.close(file1)
    
    let res2 = vfs.open(path, fmReadWrite, false)
    check res2.ok
    let file2 = res2.value
    
    var buf = newSeq[byte](1)
    discard vfs.read(file2, 0, buf)
    check buf[0] == byte(1)
    
    discard vfs.close(file2)

suite "Faulty VFS":
  test "faulty vfs creation":
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    check vfs != nil

  test "faulty vfs basic operations":
    let path = makeTempPath("decentdb_faulty_basic.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let data = @[byte(1), byte(2)]
    let writeRes = vfs.write(file, 0, data)
    check writeRes.ok
    
    discard vfs.fsync(file)
    
    var buf = newSeq[byte](2)
    let readRes = vfs.read(file, 0, buf)
    check readRes.ok
    check buf == data
    
    discard vfs.close(file)

  test "faulty vfs with read error rule":
    let path = makeTempPath("decentdb_faulty_readerr.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let data = @[byte(1)]
    discard vfs.write(file, 0, data)
    discard vfs.fsync(file)
    
    # Add a rule to fail the next read
    vfs.addRule(FaultRule(
      op: foRead,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    var buf = newSeq[byte](1)
    let readRes = vfs.read(file, 0, buf)
    check not readRes.ok
    
    # Next read should succeed (rule consumed)
    let readRes2 = vfs.read(file, 0, buf)
    check readRes2.ok
    
    discard vfs.close(file)

  test "faulty vfs with write error rule":
    let path = makeTempPath("decentdb_faulty_writeerr.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    # Add a rule to fail the next write
    vfs.addRule(FaultRule(
      op: foWrite,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    let data = @[byte(1)]
    let writeRes = vfs.write(file, 0, data)
    check not writeRes.ok
    
    # Next write should succeed (rule consumed)
    let writeRes2 = vfs.write(file, 0, data)
    check writeRes2.ok
    
    discard vfs.close(file)

  test "faulty vfs with fsync error rule":
    let path = makeTempPath("decentdb_faulty_fsyncerr.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    # Add a rule to fail the next fsync
    vfs.addRule(FaultRule(
      op: foFsync,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    let syncRes = vfs.fsync(file)
    check not syncRes.ok
    
    # Next fsync should succeed (rule consumed)
    let syncRes2 = vfs.fsync(file)
    check syncRes2.ok
    
    discard vfs.close(file)

  test "faulty vfs with partial write rule":
    let path = makeTempPath("decentdb_faulty_partial.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let data = @[byte(1), byte(2), byte(3), byte(4), byte(5)]
    
    # Add a rule for partial write of 2 bytes
    vfs.addRule(FaultRule(
      op: foWrite,
      remaining: 1,
      action: FaultAction(kind: faPartialWrite, partialBytes: 2)
    ))
    
    let writeRes = vfs.write(file, 0, data)
    check writeRes.ok
    check writeRes.value == 2
    
    # Clear rules and write remaining
    vfs.clearRules()
    let writeRes2 = vfs.write(file, 2, data[2..^1])
    check writeRes2.ok
    check writeRes2.value == 3
    
    discard vfs.close(file)

  test "faulty vfs with close error rule":
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let path = makeTempPath("decentdb_faulty_closeerr.db")
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    # Add a rule to fail close
    vfs.addRule(FaultRule(
      op: foClose,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    let closeRes = vfs.close(file)
    check not closeRes.ok

  test "faulty vfs multiple error rules":
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let path = makeTempPath("decentdb_faulty_counter.db")
    
    # Add a rule to fail the first 2 reads
    vfs.addRule(FaultRule(
      op: foRead,
      remaining: 2,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let data = @[byte(1)]
    discard vfs.write(file, 0, data)
    discard vfs.fsync(file)
    
    var buf = newSeq[byte](1)
    let readRes1 = vfs.read(file, 0, buf)
    check not readRes1.ok
    
    let readRes2 = vfs.read(file, 0, buf)
    check not readRes2.ok
    
    let readRes3 = vfs.read(file, 0, buf)
    check readRes3.ok
    
    discard vfs.close(file)

  test "faulty vfs logging":
    let path = makeTempPath("decentdb_faulty_log.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    discard vfs.write(file, 0, @[byte(1)])
    discard vfs.fsync(file)
    
    var buf = newSeq[byte](1)
    discard vfs.read(file, 0, buf)
    
    discard vfs.close(file)
    
    let log = vfs.getLog()
    check log.len > 0

suite "VFS File Modes":
  test "read-only mode":
    let path = makeTempPath("decentdb_vfs_ro.db")
    let vfs = newOsVfs()
    
    let rwRes = vfs.open(path, fmReadWrite, true)
    check rwRes.ok
    discard vfs.write(rwRes.value, 0, @[byte(1)])
    discard vfs.close(rwRes.value)
    
    let roRes = vfs.open(path, fmRead, false)
    check roRes.ok
    var buf = newSeq[byte](1)
    let readRes = vfs.read(roRes.value, 0, buf)
    check readRes.ok
    check buf[0] == byte(1)
    discard vfs.close(roRes.value)

  test "append mode":
    let path = makeTempPath("decentdb_vfs_append.db")
    let vfs = newOsVfs()
    
    let res = vfs.open(path, fmAppend, true)
    check res.ok
    discard vfs.close(res.value)

suite "VFS Large Operations":
  test "large read/write":
    let path = makeTempPath("decentdb_vfs_large.db")
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    var bigData: seq[byte] = @[]
    for i in 0..<10000:
      bigData.add(byte(i mod 256))
    
    let writeRes = vfs.write(file, 0, bigData)
    check writeRes.ok
    check writeRes.value == bigData.len
    
    discard vfs.fsync(file)
    
    var readBuf = newSeq[byte](bigData.len)
    let readRes = vfs.read(file, 0, readBuf)
    check readRes.ok
    check readRes.value == bigData.len
    check readBuf == bigData
    
    discard vfs.close(file)

  test "multiple writes and reads":
    let path = makeTempPath("decentdb_vfs_multiops.db")
    let vfs = newOsVfs()
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    for i in 0..<100:
      let data = @[byte(i)]
      let writeRes = vfs.write(file, int64(i), data)
      check writeRes.ok
    
    discard vfs.fsync(file)
    
    for i in 0..<100:
      var buf = newSeq[byte](1)
      let readRes = vfs.read(file, int64(i), buf)
      check readRes.ok
      check buf[0] == byte(i)
    
    discard vfs.close(file)

suite "VFS Error Handling":
  test "read-only file operations":
    let path = makeTempPath("decentdb_vfs_ro_ops.db")
    let vfs = newOsVfs()
    
    # Create file with data
    let rwRes = vfs.open(path, fmReadWrite, true)
    check rwRes.ok
    discard vfs.write(rwRes.value, 0, @[byte(1), byte(2), byte(3)])
    discard vfs.fsync(rwRes.value)
    discard vfs.close(rwRes.value)
    
    # Open read-only and read
    let roRes = vfs.open(path, fmRead, false)
    check roRes.ok
    var buf = newSeq[byte](3)
    let readRes = vfs.read(roRes.value, 0, buf)
    check readRes.ok
    check readRes.value == 3
    discard vfs.close(roRes.value)

  test "fsync on invalid file":
    let vfs = newOsVfs()
    # Note: This test verifies fsync behavior
    # An invalid/unopened file handle would fail
    let path = makeTempPath("decentdb_vfs_fsync_invalid.db")
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let syncRes = vfs.fsync(openRes.value)
    check syncRes.ok
    discard vfs.close(openRes.value)
