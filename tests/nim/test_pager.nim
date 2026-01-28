import unittest
import os
import engine
import pager/pager
import pager/db_header
import errors


# Use existing makeTempDb or define it if not imported
proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

suite "Pager":
  test "page roundtrip read/write":
    let path = makeTempDb("decentdb_pager_roundtrip.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let pageIdRes = allocatePage(pager)
    check pageIdRes.ok
    let pageId = pageIdRes.value
    var data = newSeq[byte](pager.pageSize)
    for i in 0 ..< data.len:
      data[i] = byte(i mod 251)
    let writeRes = writePage(pager, pageId, data)
    check writeRes.ok
    let readRes = readPage(pager, pageId)
    check readRes.ok
    check readRes.value == data
    discard closePager(pager)
    discard closeDb(db)

  test "cache eviction flushes dirty pages":
    let path = makeTempDb("decentdb_pager_evict.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let p1Res = allocatePage(pager)
    let p2Res = allocatePage(pager)
    let p3Res = allocatePage(pager)
    check p1Res.ok
    check p2Res.ok
    check p3Res.ok
    let p1 = p1Res.value
    let p2 = p2Res.value
    let p3 = p3Res.value
    discard p2
    var d1 = newSeq[byte](pager.pageSize)
    var d2 = newSeq[byte](pager.pageSize)
    var d3 = newSeq[byte](pager.pageSize)
    for i in 0 ..< pager.pageSize:
      d1[i] = 1
      d2[i] = 2
      d3[i] = 3
    check writePage(pager, p1, d1).ok
    check writePage(pager, p2, d2).ok
    check writePage(pager, p3, d3).ok
    let r1 = readPage(pager, p1)
    let r2 = readPage(pager, p2)
    let r3 = readPage(pager, p3)
    check r1.ok
    check r2.ok
    check r3.ok
    check r1.value == d1
    check r2.value == d2
    check r3.value == d3
    discard closePager(pager)
    discard closeDb(db)

  test "freelist allocate/free reuses pages":
    let path = makeTempDb("decentdb_pager_freelist.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    let p1Res = allocatePage(pager)
    let p2Res = allocatePage(pager)
    check p1Res.ok
    check p2Res.ok
    let p1 = p1Res.value
    let p2 = p2Res.value
    discard p2
    check freePage(pager, p1).ok
    let headerRes = readHeader(db.vfs, db.file)
    check headerRes.ok
    check headerRes.value.freelistCount == 1
    let p3Res = allocatePage(pager)
    check p3Res.ok
    let p3 = p3Res.value
    check p3 == p1
    let headerRes2 = readHeader(db.vfs, db.file)
    check headerRes2.ok
    check headerRes2.value.freelistCount == 0
    discard closePager(pager)
    discard closeDb(db)

  test "eviction fails if all pages pinned":
    let path = makeTempDb("decentdb_pager_pinned.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2)
    check pagerRes.ok
    let pager = pagerRes.value
    
    # Alloc two pages (will be pinned implicitly during alloc, but we need to verify if they stay pinned)
    let p1 = allocatePage(pager).value
    let p2 = allocatePage(pager).value
    
    # Manually pin them so they stay in cache and are pinned
    let pin1 = pinPage(pager, p1)
    check pin1.ok
    let pin2 = pinPage(pager, p2)
    check pin2.ok
    
    # Allocate a 3rd page (this bypasses cache if append is needed, which is true here)
    let p3Res = allocatePage(pager)
    check p3Res.ok
    let p3 = p3Res.value

    # Now try to bring p3 into cache. This should fail because p1 and p2 are pinned and take up all slots.
    let pin3 = pinPage(pager, p3)
    check not pin3.ok
    check pin3.err.code == ERR_INTERNAL # "No evictable page in cache"
    
    # Unpin one
    check unpinPage(pager, pin1.value).ok
    
    # Now it should work
    let pin3Retry = pinPage(pager, p3)
    check pin3Retry.ok
    
    discard closePager(pager)
    discard closeDb(db)

  test "freelist expansion to multiple pages":
    let path = makeTempDb("decentdb_pager_freelist_large.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    # Use larger cache to avoid eviction noise during mass alloc/free
    let pagerRes = newPager(db.vfs, db.file, cachePages = 2000) 
    check pagerRes.ok
    let pager = pagerRes.value
    
    let cap = (pager.pageSize - 8) div 4
    let limit = cap + 5 # enough to spill to second page
    
    var pages: seq[PageId] = @[]
    for i in 0 ..< limit:
      let p = allocatePage(pager).value
      pages.add(p)
      
    # Free them all in order
    for p in pages:
      check freePage(pager, p).ok
      
    # Verify freelist count in header
    let header = readHeader(db.vfs, db.file).value
    check header.freelistCount == uint32(limit)
    
    # Verify we can re-allocate them (LIFO order usually)
    for i in 0 ..< limit:
      let pRes = allocatePage(pager)
      check pRes.ok
      check pRes.value > 0
      
    discard closePager(pager)
    discard closeDb(db)

  test "detect corruption on open":
    let path = makeTempDb("decentdb_pager_corrupt.db")
    # multiple steps to create a bad file:
    # 1. Open valid DB
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file)
    check pagerRes.ok
    let validPager = pagerRes.value
    discard allocatePage(validPager)
    discard closePager(validPager)
    discard closeDb(db)
    
    # 2. Corrupt the file size (append 1 byte)
    let f = open(path, fmReadWrite)
    f.setFilePos(0, fspEnd)
    f.write(byte(0))
    f.close()
    
    # 3. Try to open
    let dbRes2 = openDb(path)
    check not dbRes2.ok
    check dbRes2.err.code == ERR_CORRUPTION
