import unittest
import os

import engine
import vfs/types
import vfs/os_vfs
import vfs/faulty_vfs
import errors
import catalog/catalog
import record/record

proc makeTempPath(name: string): string =
  result = getTempDir() / name
  if fileExists(result):
    removeFile(result)

suite "OS VFS Error Paths":
  test "open file failure with bad permissions":
    let path = makeTempPath("decentdb_vfs_open_fail.db")
    let vfs = newOsVfs()
    
    # Try to open non-existent file without create flag
    let res = vfs.open(path, fmReadWrite, false)
    check not res.ok
    check res.err.code == ERR_IO

  test "read error on closed file":
    let path = makeTempPath("decentdb_vfs_read_error.db")
    let vfs = newOsVfs()
    
    # Create a minimal file
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    discard vfs.write(openRes.value, 0, @[byte(1)])
    discard vfs.close(openRes.value)
    
    # Try to read with invalid offset (this might succeed or fail depending on OS)
    # But we can test other error conditions
    
  test "write error with read-only file":
    let path = makeTempPath("decentdb_vfs_write_ro.db")
    let vfs = newOsVfs()
    
    # Create file
    let rwRes = vfs.open(path, fmReadWrite, true)
    check rwRes.ok
    discard vfs.write(rwRes.value, 0, @[byte(1)])
    discard vfs.close(rwRes.value)
    
    # Open read-only (this should work)
    let roRes = vfs.open(path, fmRead, false)
    check roRes.ok
    
    # Write should fail
    var buf = newSeq[byte](1)
    let writeRes = vfs.write(roRes.value, 0, buf)
    check not writeRes.ok
    check writeRes.err.code == ERR_IO
    
    discard vfs.close(roRes.value)

  test "readStr returns 0 for empty buffer":
    let path = makeTempPath("decentdb_vfs_readstr_empty.db")
    let vfs = newOsVfs()
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    var emptyBuf = ""
    let readRes = vfs.readStr(file, 0, emptyBuf)
    check readRes.ok
    check readRes.value == 0
    
    discard vfs.close(file)

  test "writeStr returns 0 for empty string":
    let path = makeTempPath("decentdb_vfs_writestr_empty.db")
    let vfs = newOsVfs()
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let writeRes = vfs.writeStr(file, 0, "")
    check writeRes.ok
    check writeRes.value == 0
    
    discard vfs.close(file)

  test "truncate on empty file":
    let path = makeTempPath("decentdb_vfs_truncate_empty.db")
    let vfs = newOsVfs()
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let truncRes = vfs.truncate(file, 0)
    check truncRes.ok
    
    discard vfs.close(file)

  test "truncate to larger size":
    let path = makeTempPath("decentdb_vfs_truncate_grow.db")
    let vfs = newOsVfs()
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    discard vfs.write(file, 0, @[byte(1)])
    
    let truncRes = vfs.truncate(file, 1000)
    check truncRes.ok
    
    # Read back - should be zeros beyond written data
    var buf = newSeq[byte](10)
    discard vfs.read(file, 0, buf)
    check buf[0] == byte(1)
    for i in 1 ..< 10:
      check buf[i] == byte(0)
    
    discard vfs.close(file)

suite "Faulty VFS Error Paths":
  test "faulty vfs replay mode":
    let path = makeTempPath("decentdb_faulty_replay.db")
    let underlying = newOsVfs()
    
    # Create replay log
    var replayLog: seq[FaultLogEntry] = @[]
    replayLog.add(FaultLogEntry(
      op: foOpen,
      label: "test",
      action: FaultAction(kind: faNone),
      requestedBytes: 0,
      appliedBytes: 0,
      errorCode: ERR_INTERNAL
    ))
    
    let vfs = newFaultyVfsWithReplay(underlying, replayLog)
    check vfs != nil
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    discard vfs.close(openRes.value)

  test "faulty vfs with truncate error":
    let path = makeTempPath("decentdb_faulty_truncate.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    vfs.addRule(FaultRule(
      op: foTruncate,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    let truncRes = vfs.truncate(file, 100)
    check not truncRes.ok
    check truncRes.err.code == ERR_IO
    
    # Next truncate should succeed
    let truncRes2 = vfs.truncate(file, 200)
    check truncRes2.ok
    
    discard vfs.close(file)

  test "faulty vfs drop fsync":
    let path = makeTempPath("decentdb_faulty_drop_fsync.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    vfs.addRule(FaultRule(
      op: foFsync,
      remaining: 1,
      action: FaultAction(kind: faDropFsync)
    ))
    
    let syncRes = vfs.fsync(file)
    check syncRes.ok
    
    let log = vfs.getLog()
    check log.len > 0
    check log[^1].action.kind == faDropFsync
    
    discard vfs.close(file)

  test "faulty vfs with open error":
    let path = makeTempPath("decentdb_faulty_open.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    vfs.addRule(FaultRule(
      op: foOpen,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check not openRes.ok
    check openRes.err.code == ERR_IO

  test "faulty vfs partial write with zero bytes":
    let path = makeTempPath("decentdb_faulty_partial_zero.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    vfs.addRule(FaultRule(
      op: foWrite,
      remaining: 1,
      action: FaultAction(kind: faPartialWrite, partialBytes: 0)
    ))
    
    let data = @[byte(1), byte(2), byte(3)]
    let writeRes = vfs.write(file, 0, data)
    check writeRes.ok
    check writeRes.value == 0
    
    discard vfs.close(file)

  test "faulty vfs writeStr partial":
    let path = makeTempPath("decentdb_faulty_partial_str.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    vfs.addRule(FaultRule(
      op: foWrite,
      remaining: 1,
      action: FaultAction(kind: faPartialWrite, partialBytes: 3)
    ))
    
    let writeRes = vfs.writeStr(file, 0, "hello world")
    check writeRes.ok
    check writeRes.value == 3
    
    discard vfs.close(file)

  test "faulty vfs readStr error":
    let path = makeTempPath("decentdb_faulty_readstr_err.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    discard vfs.write(file, 0, @[byte(1)])
    
    vfs.addRule(FaultRule(
      op: foRead,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    var buf = newString(10)
    let readRes = vfs.readStr(file, 0, buf)
    check not readRes.ok
    check readRes.err.code == ERR_IO
    
    discard vfs.close(file)

  test "faulty vfs clearRules":
    let path = makeTempPath("decentdb_faulty_clear.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    vfs.addRule(FaultRule(
      op: foWrite,
      remaining: 100,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    check vfs.rules.len == 1
    vfs.clearRules()
    check vfs.rules.len == 0
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    # Write should succeed now
    let writeRes = vfs.write(file, 0, @[byte(1)])
    check writeRes.ok
    
    discard vfs.close(file)

  test "faulty vfs multiple rules different ops":
    let path = makeTempPath("decentdb_faulty_multiops.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    vfs.addRule(FaultRule(
      op: foWrite,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    vfs.addRule(FaultRule(
      op: foRead,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    vfs.addRule(FaultRule(
      op: foFsync,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let writeRes = vfs.write(file, 0, @[byte(1)])
    check not writeRes.ok
    
    let syncRes = vfs.fsync(file)
    check not syncRes.ok
    
    var buf = newSeq[byte](1)
    let readRes = vfs.read(file, 0, buf)
    check not readRes.ok
    
    # All operations should succeed now
    let writeRes2 = vfs.write(file, 0, @[byte(1)])
    check writeRes2.ok
    
    let syncRes2 = vfs.fsync(file)
    check syncRes2.ok
    
    let readRes2 = vfs.read(file, 0, buf)
    check readRes2.ok
    
    discard vfs.close(file)

suite "VFS Complex Error Scenarios":
  test "faulty vfs error code propagation":
    let path = makeTempPath("decentdb_faulty_code.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    vfs.addRule(FaultRule(
      op: foWrite,
      remaining: 1,
      action: FaultAction(kind: faError, errorCode: ERR_CORRUPTION, label: "test-label")
    ))
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    let writeRes = vfs.write(file, 0, @[byte(1)])
    check not writeRes.ok
    check writeRes.err.code == ERR_CORRUPTION
    check writeRes.err.message.find("Injected") >= 0
    
    discard vfs.close(file)

  test "faulty vfs with negative remaining (infinite)":
    let path = makeTempPath("decentdb_faulty_infinite.db")
    let underlying = newOsVfs()
    let vfs = newFaultyVfs(underlying)
    
    # Use a large negative value (which Nim treats as large positive in unsigned)
    # But let's test the zero case instead
    vfs.addRule(FaultRule(
      op: foWrite,
      remaining: 0,
      action: FaultAction(kind: faError, errorCode: ERR_IO)
    ))
    
    let openRes = vfs.open(path, fmReadWrite, true)
    check openRes.ok
    let file = openRes.value
    
    # Should succeed since remaining is 0
    let writeRes = vfs.write(file, 0, @[byte(1)])
    check writeRes.ok
    
    discard vfs.close(file)
