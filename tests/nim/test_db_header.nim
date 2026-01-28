import unittest
import os
import vfs/os_vfs
import pager/db_header
import errors
import engine

suite "DB Header":
  test "header encode/decode roundtrip":
    let header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 123,
      rootCatalog: 10,
      rootFreelist: 20,
      freelistHead: 30,
      freelistCount: 40,
      lastCheckpointLsn: 50
    )
    let buf = encodeHeader(header)
    let decoded = decodeHeader(buf)
    check decoded.ok
    check decoded.value.schemaCookie == 123
    check decoded.value.rootCatalog == 10
    check decoded.value.freelistCount == 40

  test "corrupt checksum fails open":
    let tempPath = getTempDir() / "decentdb_header_corrupt.db"
    if fileExists(tempPath):
      removeFile(tempPath)
    let vfs = newOsVfs()
    let openRes = vfs.open(tempPath, fmReadWrite, true)
    check openRes.ok
    let header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    let writeRes = writeHeader(vfs, openRes.value, header)
    check writeRes.ok
    var corrupt = encodeHeader(header)
    corrupt[32] = byte(corrupt[32] xor 0xFF)
    let corruptWrite = vfs.write(openRes.value, 0, corrupt)
    check corruptWrite.ok
    check corruptWrite.value == HeaderSize
    let fsyncRes = vfs.fsync(openRes.value)
    check fsyncRes.ok
    var verify = newSeq[byte](HeaderSize)
    let verifyRead = vfs.read(openRes.value, 0, verify)
    check verifyRead.ok
    check verifyRead.value == HeaderSize
    let decoded = decodeHeader(verify)
    check not decoded.ok
    check decoded.err.code == ERR_CORRUPTION
    discard vfs.close(openRes.value)
    let info = getFileInfo(tempPath)
    check info.size >= HeaderSize.int64
    let raw = readFile(tempPath)
    check raw.len >= HeaderSize

    let reopenVfs = newOsVfs()
    let reopenFile = reopenVfs.open(tempPath, fmReadWrite, false)
    check reopenFile.ok
    var reopenBuf = newSeq[byte](HeaderSize)
    let reopenRead = reopenVfs.read(reopenFile.value, 0, reopenBuf)
    check reopenRead.ok
    check reopenRead.value == HeaderSize
    let reopenDecoded = decodeHeader(reopenBuf)
    check not reopenDecoded.ok
    check reopenDecoded.err.code == ERR_CORRUPTION
    discard reopenVfs.close(reopenFile.value)

    let reopen = openDb(tempPath)
    check not reopen.ok
    check reopen.err.code == ERR_CORRUPTION
