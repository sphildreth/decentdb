import unittest
import os
import strutils
import engine
import pager/pager

proc makeTempDb(name: string): string =
  let normalizedName = if name.endsWith(".db"): name[0 .. ^4] & ".ddb" else: name
  let path = getTempDir() / normalizedName
  if fileExists(path): removeFile(path)
  path

suite "Pager Eviction":
  test "mark-and-compact eviction works under churn":
    let path = makeTempDb("decentdb_pager_stress.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    # Small cache to force frequent eviction
    let pagerRes = newPager(db.vfs, db.file, cachePages = 10)
    check pagerRes.ok
    let pager = pagerRes.value
    
    # We will allocate 100 pages, much more than cache size (10)
    # This forces eviction of 90% of pages
    var pageIds: seq[PageId] = @[]
    for i in 0 ..< 100:
      let pRes = allocatePage(pager)
      check pRes.ok
      pageIds.add(pRes.value)
      
      # Write something to make it dirty (forces flush on evict)
      var data = newString(pager.pageSize)
      data[0] = char(byte(i mod 255))
      check writePage(pager, pRes.value, data).ok
      
      # Read back some older page to mess with clock bits (approx every 10 allocs)
      if i > 10 and (i mod 10 == 0):
        let oldIdx = i - 10
        let oldPage = pageIds[oldIdx]
        # This might fail if it was evicted and we don't want to reload from disk in this test?
        # Actually readPage will reload from disk if evicted. 
        # We just want to ensure it works and doesn't crash.
        let r = readPage(pager, oldPage)
        check r.ok
        check r.value[0] == char(byte(oldIdx mod 255))

    # Verification:
    # 1. No crash
    # 2. Can read back all pages correctly (proves flush worked on eviction)
    for i in 0 ..< 100:
      let p = pageIds[i]
      let r = readPage(pager, p)
      check r.ok
      check r.value[0] == char(byte(i mod 255))
      
    discard closePager(pager)
    discard closeDb(db)
