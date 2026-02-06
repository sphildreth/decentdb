import unittest
import os
import vfs/os_vfs
import pager/db_header
import errors
import strutils

suite "DB Header Extended":
  proc crc32cSlow(data: openArray[byte]): uint32 =
    const Poly = 0x82F63B78'u32
    var table: array[256, uint32]
    for i in 0 .. 255:
      var crc = uint32(i)
      for _ in 0 .. 7:
        if (crc and 1) == 1:
          crc = (crc shr 1) xor Poly
        else:
          crc = crc shr 1
      table[i] = crc
    var current = 0xFFFFFFFF'u32
    for b in data:
      let idx = (current xor uint32(b)) and 0xFF'u32
      current = (current shr 8) xor table[int(idx)]
    current xor 0xFFFFFFFF'u32

  proc crc32cSlow(data: string): uint32 =
    const Poly = 0x82F63B78'u32
    var table: array[256, uint32]
    for i in 0 .. 255:
      var crc = uint32(i)
      for _ in 0 .. 7:
        if (crc and 1) == 1:
          crc = (crc shr 1) xor Poly
        else:
          crc = crc shr 1
      table[i] = crc
    var current = 0xFFFFFFFF'u32
    for ch in data:
      let b = uint32(byte(ch))
      let idx = (current xor b) and 0xFF'u32
      current = (current shr 8) xor table[int(idx)]
    current xor 0xFFFFFFFF'u32
  test "writeU32LE with byte array":
    var buf: array[4, byte]
    writeU32LE(buf, 0, 0x12345678'u32)
    check buf[0] == 0x78'u8
    check buf[1] == 0x56'u8
    check buf[2] == 0x34'u8
    check buf[3] == 0x12'u8

  test "writeU32LE with string":
    var buf = newString(4)
    writeU32LE(buf, 0, 0x12345678'u32)
    check byte(buf[0]) == 0x78'u8
    check byte(buf[1]) == 0x56'u8
    check byte(buf[2]) == 0x34'u8
    check byte(buf[3]) == 0x12'u8

  test "writeU64LE with byte array":
    var buf: array[8, byte]
    writeU64LE(buf, 0, 0x123456789ABCDEF0'u64)
    check buf[0] == 0xF0'u8
    check buf[1] == 0xDE'u8
    check buf[2] == 0xBC'u8
    check buf[3] == 0x9A'u8
    check buf[4] == 0x78'u8
    check buf[5] == 0x56'u8
    check buf[6] == 0x34'u8
    check buf[7] == 0x12'u8

  test "writeU64LE with string":
    var buf = newString(8)
    writeU64LE(buf, 0, 0x123456789ABCDEF0'u64)
    check byte(buf[0]) == 0xF0'u8
    check byte(buf[7]) == 0x12'u8

  test "readU32LE from byte array":
    let buf = [0x78'u8, 0x56, 0x34, 0x12]
    check readU32LE(buf, 0) == 0x12345678'u32

  test "readU32LE from string":
    let buf = "\x78\x56\x34\x12"
    check readU32LE(buf, 0) == 0x12345678'u32

  test "readU64LE from byte array":
    let buf = [0xF0'u8, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12]
    check readU64LE(buf, 0) == 0x123456789ABCDEF0'u64

  test "readU64LE from string":
    let buf = "\xF0\xDE\xBC\x9A\x78\x56\x34\x12"
    check readU64LE(buf, 0) == 0x123456789ABCDEF0'u64

  test "crc32c produces consistent results":
    let data1 = "hello world"
    let data2 = "hello world"
    check crc32c(data1) == crc32c(data2)

  test "crc32c with different inputs produces different outputs":
    let data1 = "hello"
    let data2 = "world"
    check crc32c(data1) != crc32c(data2)

  test "crc32c with byte array":
    let data = @[byte('h'), byte('i')]
    let result = crc32c(data)
    check result != 0'u32

  test "crc32c with empty string":
    let result = crc32c("")
    let expected = 0xFFFFFFFF'u32 xor 0xFFFFFFFF'u32
    check result == expected  # ~0 ^ ~0 = 0

  test "crc32c fast matches slow":
    let data = "The quick brown fox jumps over the lazy dog"
    check crc32c(data) == crc32cSlow(data)
    let bytes = @[byte('A'), byte('B'), byte('C'), byte('D'), byte('E'), byte('F'), byte('G'), byte('H'), byte('I')]
    check crc32c(bytes) == crc32cSlow(bytes)

  test "headerMagicOk with valid magic":
    var buf = newSeq[byte](128)
    for i, ch in MagicPadded:
      buf[i] = byte(ch)
    check headerMagicOk(buf) == true

  test "headerMagicOk with invalid magic":
    var buf = newSeq[byte](128)
    buf[0] = 0xFF'u8
    check headerMagicOk(buf) == false

  test "headerMagicOk with short buffer":
    var buf = newSeq[byte](10)
    check headerMagicOk(buf) == false

  test "decodeHeaderUnsafe extracts correct values":
    let header = DbHeader(
      formatVersion: 2'u32,
      pageSize: 4096'u32,
      schemaCookie: 100'u32,
      rootCatalog: 1'u32,
      rootFreelist: 2'u32,
      freelistHead: 3'u32,
      freelistCount: 4'u32,
      lastCheckpointLsn: 123456789'u64
    )
    let buf = encodeHeader(header)
    let decoded = decodeHeaderUnsafe(buf)
    check decoded.ok
    check decoded.value.formatVersion == 2'u32
    check decoded.value.pageSize == 4096'u32
    check decoded.value.schemaCookie == 100'u32
    check decoded.value.rootCatalog == 1'u32
    check decoded.value.rootFreelist == 2'u32
    check decoded.value.freelistHead == 3'u32
    check decoded.value.freelistCount == 4'u32
    check decoded.value.lastCheckpointLsn == 123456789'u64

  test "decodeHeaderUnsafe with short buffer":
    var buf = newSeq[byte](50)
    let decoded = decodeHeaderUnsafe(buf)
    check not decoded.ok
    check decoded.err.code == ERR_CORRUPTION

  test "decodeHeader with invalid magic":
    var buf = newSeq[byte](128)
    # Fill with zeros - invalid magic
    let decoded = decodeHeader(buf)
    check not decoded.ok
    check decoded.err.code == ERR_CORRUPTION
    check decoded.err.message.contains("magic")

  test "decodeHeader with wrong checksum":
    var buf = newSeq[byte](128)
    # Set valid magic
    for i, ch in MagicPadded:
      buf[i] = byte(ch)
    # Set some values but don't update checksum
    writeU32LE(buf, 16, 2'u32)  # format version
    writeU32LE(buf, 20, 4096'u32)  # page size
    let decoded = decodeHeader(buf)
    check not decoded.ok
    check decoded.err.code == ERR_CORRUPTION
    check decoded.err.message.contains("checksum")

  test "header constants":
    check HeaderSize == 128
    check MagicBytes == "DECENTDB"
    check MagicPadded.len == 16
    check FormatVersion == 8'u32
    check DefaultPageSize == 4096'u32

  test "readHeader from empty file fails":
    let tempPath = getTempDir() / "decentdb_empty_header.ddb"
    if fileExists(tempPath):
      removeFile(tempPath)
    let vfs = newOsVfs()
    let openRes = vfs.open(tempPath, fmReadWrite, true)
    check openRes.ok
    # Don't write anything
    discard vfs.close(openRes.value)
    
    # Reopen and try to read header
    let reopenRes = vfs.open(tempPath, fmReadWrite, false)
    check reopenRes.ok
    let readRes = readHeader(vfs, reopenRes.value)
    check not readRes.ok
    check readRes.err.code == ERR_CORRUPTION
    discard vfs.close(reopenRes.value)

  test "writeHeader writes correct size":
    let tempPath = getTempDir() / "decentdb_write_header.ddb"
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
    
    # Check file size
    let info = getFileInfo(tempPath)
    check info.size >= HeaderSize
    
    discard vfs.close(openRes.value)

  test "header checksum computation changes with content":
    let header1 = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 1,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    let header2 = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 2,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    let buf1 = encodeHeader(header1)
    let buf2 = encodeHeader(header2)
    let checksum1 = headerChecksumExpected(buf1)
    let checksum2 = headerChecksumExpected(buf2)
    check checksum1 != checksum2
