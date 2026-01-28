import unittest
import os
import sequtils
import vfs/vfs
import errors

suite "Faulty VFS":
  test "partial write is deterministic and replayable":
    let tempPath = getTempDir() / "decentdb_faulty_vfs_partial.bin"
    if fileExists(tempPath):
      removeFile(tempPath)
    let base = newOsVfs()
    let faulty = newFaultyVfs(base)
    faulty.addRule(FaultRule(label: "partial-write", op: foWrite, remaining: 1,
      action: FaultAction(kind: faPartialWrite, partialBytes: 4, errorCode: ERR_IO, label: "partial-write")))
    let openRes = faulty.open(tempPath, fmReadWrite, true)
    check openRes.ok
    let handle = openRes.value
    let payload = @[byte 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    let writeRes = faulty.write(handle, 0, payload)
    check writeRes.ok
    check writeRes.value == 4
    discard faulty.close(handle)
    let originalLog = faulty.getLog()

    let replayVfs = newFaultyVfsWithReplay(base, originalLog)
    let openReplay = replayVfs.open(tempPath & ".replay", fmReadWrite, true)
    check openReplay.ok
    let replayHandle = openReplay.value
    let replayWrite = replayVfs.write(replayHandle, 0, payload)
    check replayWrite.ok
    check replayWrite.value == 4
    discard replayVfs.close(replayHandle)
    let replayLog = replayVfs.getLog()
    check replayLog == originalLog

  test "dropped fsync is recorded and non-fatal":
    let tempPath = getTempDir() / "decentdb_faulty_vfs_fsync.bin"
    if fileExists(tempPath):
      removeFile(tempPath)
    let base = newOsVfs()
    let faulty = newFaultyVfs(base)
    faulty.addRule(FaultRule(label: "drop-fsync", op: foFsync, remaining: 1,
      action: FaultAction(kind: faDropFsync, partialBytes: 0, errorCode: ERR_IO, label: "drop-fsync")))
    let openRes = faulty.open(tempPath, fmReadWrite, true)
    check openRes.ok
    let handle = openRes.value
    let fsyncRes = faulty.fsync(handle)
    check fsyncRes.ok
    discard faulty.close(handle)
    let log = faulty.getLog()
    check log.anyIt(it.action.kind == faDropFsync)

  test "failpoint error is injected":
    let tempPath = getTempDir() / "decentdb_faulty_vfs_error.bin"
    if fileExists(tempPath):
      removeFile(tempPath)
    let base = newOsVfs()
    let faulty = newFaultyVfs(base)
    faulty.addRule(FaultRule(label: "fail-read", op: foRead, remaining: 1,
      action: FaultAction(kind: faError, partialBytes: 0, errorCode: ERR_IO, label: "fail-read")))
    let openRes = faulty.open(tempPath, fmReadWrite, true)
    check openRes.ok
    let handle = openRes.value
    var buffer = newSeq[byte](8)
    let readRes = faulty.read(handle, 0, buffer)
    check not readRes.ok
    check readRes.err.code == ERR_IO
    discard faulty.close(handle)
