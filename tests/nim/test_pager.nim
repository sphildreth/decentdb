import unittest
import os
import engine
import pager/pager
import pager/db_header

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
