import unittest
import os
import ../../src/record/record
import ../../src/storage/storage
import ../../src/pager/pager
import ../../src/pager/db_header
import ../../src/vfs/os_vfs
import ../../src/errors

const TEST_DB = "test_compression.db"

proc cleanup() =
  if fileExists(TEST_DB):
    removeFile(TEST_DB)
  if fileExists(TEST_DB & "-wal"):
    removeFile(TEST_DB & "-wal")
  if fileExists(TEST_DB & "-shm"):
    removeFile(TEST_DB & "-shm")

suite "Value Compression":
  
  setup:
    cleanup()

  teardown:
    cleanup()

  test "compressValue logic":
    # Small string - no compression
    let small = Value(kind: vkText, bytes: @[byte('a'), byte('b'), byte('c')])
    let resSmall = compressValue(small)
    check resSmall.kind == vkText
    check resSmall.bytes == small.bytes

    # Large compressible string
    # "aaaaa..." x 200 ( > 128 bytes)
    var largeStr = newString(200)
    for i in 0 ..< 200: largeStr[i] = 'a'
    
    var largeBytes = newSeq[byte](200)
    for i in 0 ..< 200: largeBytes[i] = byte('a')
    
    let large = Value(kind: vkText, bytes: largeBytes)
    let resLarge = compressValue(large)
    
    # Should compress well
    check resLarge.kind == vkTextCompressed
    check resLarge.bytes.len < large.bytes.len
    check resLarge.bytes.len < int(200.0 * 0.9) # Savings check

    # Verify decompress
    # We can't access private decompressData directly but encode/decodeValue uses it.
    let encoded = encodeValue(resLarge)
    var offset = 0
    let decodedRes = decodeValue(encoded, offset)
    check decodedRes.ok
    let decoded = decodedRes.value
    check decoded.kind == vkText
    check decoded.bytes == largeBytes

  test "compressValue uncompressible":
    # Large random string
    var randomBytes = newSeq[byte](200)
    var seed: uint32 = 12345
    for i in 0 ..< 200:
      seed = seed * 1664525 + 1013904223
      randomBytes[i] = byte(seed and 0xFF)
      
    let randomVal = Value(kind: vkBlob, bytes: randomBytes)
    let resRandom = compressValue(randomVal)
    
    # Should likely NOT compress (remain vkBlob)
    check resRandom.kind == vkBlob
    check resRandom.bytes.len == 200

  test "Integration with Storage (normalizeValues + Overflow)":
    let vfs = newOsVfs()
    let fileRes = vfs.open(TEST_DB, fmReadWrite, true)
    require fileRes.ok
    let file = fileRes.value
    
    # Initialize DB header
    var header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    
    # Write full page to ensure alignment
    var pageBuf = newSeq[byte](DefaultPageSize)
    let headerBytes = encodeHeader(header)
    for i in 0 ..< headerBytes.len:
      pageBuf[i] = headerBytes[i]
      
    let writeRes = vfs.write(file, 0, pageBuf)
    check writeRes.ok

    let pagerRes = newPager(vfs, file)
    if not pagerRes.ok:
      echo "Pager Init Failed: ", pagerRes.err.message
    require pagerRes.ok
    let pager = pagerRes.value
    
    # Case 1: Compressed Inline
    # "aaaa..." x 200. Fits in page (4096), but > 128.
    var largeBytes = newSeq[byte](200)
    for i in 0 ..< 200: largeBytes[i] = byte('a')
    let val1 = Value(kind: vkText, bytes: largeBytes)
    
    let normRes1 = normalizeValues(pager, @[val1])
    check normRes1.ok
    check normRes1.value[0].kind == vkTextCompressed
    
    # Verify decode works
    let encoded1 = encodeRecord(normRes1.value)
    let decodedRes1 = decodeRecord(encoded1)
    check decodedRes1.ok
    # decodeRecord decompresses automatically
    check decodedRes1.value[0].kind == vkText
    check decodedRes1.value[0].bytes == largeBytes

    # Case 2: Compressed Overflow
    # Use ASCII numbers which have entropy but compress somewhat.
    # "0 1 2 3 ..."
    var hugeString = ""
    for i in 0 .. 5000:
      hugeString.add($i & " ")
      
    var hugeBytes = newSeq[byte](hugeString.len)
    for i in 0 ..< hugeString.len: hugeBytes[i] = byte(hugeString[i])
    
    # 20k+ bytes. Compressed should be ~8k-10k (assuming 2-3x compression).
    # 8k > 4k (page size).
    
    let val3 = Value(kind: vkText, bytes: hugeBytes)
    let normRes3 = normalizeValues(pager, @[val3])
    check normRes3.ok
    
    let storedVal = normRes3.value[0]
    
    if storedVal.kind != vkTextCompressedOverflow:
      echo "Failed Overflow Check. Kind: ", storedVal.kind, " Bytes Len: ", storedVal.bytes.len
      
    check storedVal.kind == vkTextCompressedOverflow
    check storedVal.overflowPage > 0
    
    # Decode with overflow
    let decodedRes3 = decodeRecordWithOverflow(pager, encodeRecord(normRes3.value))
    check decodedRes3.ok
    check decodedRes3.value[0].kind == vkText
    check decodedRes3.value[0].bytes == hugeBytes

    discard closePager(pager)
