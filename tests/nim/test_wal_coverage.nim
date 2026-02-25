import unittest
import os
import options
import strutils
import locks
import sets

import wal/wal
import vfs/types
import vfs/mem_vfs
import pager/pager
import pager/db_header
import errors

suite "WAL Coverage":
  setup:
    let vfs = newMemVfs()
    let walRes = newWal(vfs, "test.wal")
    require walRes.ok
    let wal = walRes.value

  test "writePageDirect and writePageZeroCopy":
    let writerRes = beginWrite(wal)
    require writerRes.ok
    let writer = writerRes.value
    
    let data1 = "hello world"
    let res1 = writePageDirect(writer, 1, data1)
    check res1.ok
    
    let data2 = "zero copy data"
    let res2 = writePageZeroCopy(writer, 2, unsafeAddr data2[0], data2.len)
    check res2.ok
    
    let commitRes = commit(writer)
    check commitRes.ok

  test "rollback":
    let writerRes = beginWrite(wal)
    require writerRes.ok
    let writer = writerRes.value
    
    let data = "rollback data"
    discard writePageDirect(writer, 1, data)
    
    let rollbackRes = rollback(writer)
    check rollbackRes.ok
    
    # Verify data is not there
    let readTxn = beginRead(wal)
    let pageRes = getPageAtOrBefore(wal, 1, readTxn.snapshot)
    check pageRes.isNone
    endRead(wal, readTxn)

  test "reader stats and limits":
    setCheckpointConfig(wal, 1024, 1000, 100, 1000, false, 1024, 1024, 100, 1000)
    
    let readTxn1 = beginRead(wal)
    let readTxn2 = beginRead(wal)
    
    check readerCount(wal) == 2
    
    let stats = getReaderStats(wal)
    check stats.activeReaders == 2
    
    os.sleep(250) # Sleep longer to ensure epochTime crosses the threshold
    
    let overThreshold = readersOverThreshold(wal, 100)
    check overThreshold.len == 2
    check readerCount(wal) == 2
    
    let exceedingLimit = readersExceedingWalLimit(wal)
    check exceedingLimit.len == 0 # No bytes pinned yet
    
    endRead(wal, readTxn1)
    endRead(wal, readTxn2)
    check readerCount(wal) == 0

  test "readFramePayload":
    let writerRes = beginWrite(wal)
    require writerRes.ok
    let writer = writerRes.value
    
    let data = newSeq[byte](4096)
    discard writePage(writer, 100, data)
    let commitRes = commit(writer)
    check commitRes.ok
    
    # We need to know the offset to test readFramePayload directly,
    # but we can test getPageAtOrBefore which uses it.
    let readTxn = beginRead(wal)
    let pageRes = getPageAtOrBefore(wal, 100, readTxn.snapshot)
    check pageRes.isSome
    endRead(wal, readTxn)

  test "flushPage":
    let writerRes = beginWrite(wal)
    require writerRes.ok
    let writer = writerRes.value
    
    let data = newSeq[byte](4096)
    let res = flushPage(writer, 1, data)
    check res.ok
    
    let commitRes = commit(writer)
    check commitRes.ok

  test "WAL error paths and failpoints":
    # 1. readFramePayload invalid frame errors
    let writerRes = beginWrite(wal)
    require writerRes.ok
    let writer = writerRes.value
    discard writePageDirect(writer, 1, "test data")
    let commitRes = commit(writer)
    check commitRes.ok
    
    # Read from an offset that is out of bounds or not a frame
    let invalidPayload = readFramePayload(wal, 9999999)
    check invalidPayload.isNone
    
    # 2. readPageWithSnapshot aborted reader timeouts
    let readTxn = beginRead(wal)
    # Manually abort the reader
    acquire(wal.readerLock)
    wal.abortedReaders.incl(readTxn.id)
    release(wal.readerLock)
    
    let fileRes = vfs.open("test.db", fmReadWrite, true)
    require fileRes.ok
    
    # Write a valid DB header so newPager doesn't fail
    let header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 1,
      rootCatalog: 2,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    let headerBuf = encodeHeader(header)
    discard vfs.write(fileRes.value, 0, headerBuf)
    discard vfs.truncate(fileRes.value, 4096)
    
    let pagerRes = newPager(vfs, fileRes.value)
    if not pagerRes.ok:
      echo "Pager error: ", pagerRes.err.message
    require pagerRes.ok
    let pager = pagerRes.value
    let readRes = readPageWithSnapshot(pager, wal, readTxn.snapshot, 1, readTxn.id)
    check not readRes.ok
    check readRes.err.code == ERR_TRANSACTION
    
    endRead(wal, readTxn)
    discard vfs.close(fileRes.value)
    discard vfs.removeFile("test.db")
    
    # 3. writePageDirect and commit inactive transaction checks
    let writerRes2 = beginWrite(wal)
    require writerRes2.ok
    let writer2 = writerRes2.value
    discard rollback(writer2)
    
    let writeRes = writePageDirect(writer2, 2, "inactive")
    check not writeRes.ok
    check writeRes.err.code == ERR_TRANSACTION
    
    let commitRes2 = commit(writer2)
    check not commitRes2.ok
    check commitRes2.err.code == ERR_TRANSACTION
    
    # 4. flushPage append errors
    let writerRes3 = beginWrite(wal)
    require writerRes3.ok
    let writer3 = writerRes3.value
    
    setFailpoint(wal, "wal_write_frame", WalFailpoint(kind: wfError))
    let flushRes = flushPage(writer3, 3, newSeq[byte](10))
    check not flushRes.ok
    check flushRes.err.code == ERR_IO
    clearFailpoints(wal)
    discard rollback(writer3)
    
    # 5. commit failpoints (wal_write_frame), partial writes, and fsync errors
    let writerRes4 = beginWrite(wal)
    require writerRes4.ok
    let writer4 = writerRes4.value
    discard writePageDirect(writer4, 4, "failpoint data")
    
    setFailpoint(wal, "wal_write_frame", WalFailpoint(kind: wfError))
    let commitRes4 = commit(writer4)
    check not commitRes4.ok
    check commitRes4.err.code == ERR_IO
    clearFailpoints(wal)
    
    # Partial write
    let writerRes5 = beginWrite(wal)
    require writerRes5.ok
    let writer5 = writerRes5.value
    discard writePageDirect(writer5, 5, "partial write data")
    
    setFailpoint(wal, "wal_write_frame", WalFailpoint(kind: wfPartial, partialBytes: 10))
    let commitRes5 = commit(writer5)
    check not commitRes5.ok
    check commitRes5.err.code == ERR_IO
    clearFailpoints(wal)
    
    # fsync error
    let writerRes6 = beginWrite(wal)
    require writerRes6.ok
    let writer6 = writerRes6.value
    discard writePageDirect(writer6, 6, "fsync error data")
    
    setFailpoint(wal, "wal_fsync", WalFailpoint(kind: wfError))
    let commitRes6 = commit(writer6)
    check not commitRes6.ok
    check commitRes6.err.code == ERR_IO
    clearFailpoints(wal)
    
    # writeWalHeader error
    let writerRes7 = beginWrite(wal)
    require writerRes7.ok
    let writer7 = writerRes7.value
    discard writePageDirect(writer7, 7, "header error data")
    
    # Force mmap to fail so it falls back to vfs.write
    wal.mmapEnabled = false
    wal.mmapPtr = nil
    
    # We can't easily inject a failpoint into writeWalHeader because it doesn't have one.
    # But we can test the partial write path of appendFrame.
    discard rollback(writer7)

  test "isAborted":
    var readTxn = beginRead(wal)
    check not isAborted(wal, readTxn)
    
    # Test fallback path by setting aborted flag to nil
    let oldFlag = readTxn.aborted
    readTxn.aborted = nil
    check not isAborted(wal, readTxn)
    
    acquire(wal.readerLock)
    wal.abortedReaders.incl(readTxn.id)
    release(wal.readerLock)
    check isAborted(wal, readTxn)
    
    readTxn.aborted = oldFlag
    endRead(wal, readTxn)

  test "estimateIndexMemoryUsage":
    let mem = estimateIndexMemoryUsage(wal)
    check mem >= 0

