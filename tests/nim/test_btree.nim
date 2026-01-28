import unittest
import os
import engine
import pager/pager
import btree/btree
import pager/db_header
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

proc buildLeaf(pageSize: int, keys: seq[uint64], values: seq[seq[byte]], nextLeaf: uint32): seq[byte] =
  var buf = newSeq[byte](pageSize)
  buf[0] = PageTypeLeaf
  buf[1] = 0
  writeU32LE(buf, 4, nextLeaf)
  let count = uint16(keys.len)
  buf[2] = byte(count and 0xFF)
  buf[3] = byte((count shr 8) and 0xFF)
  var offset = 8
  for i in 0 ..< keys.len:
    writeU64LE(buf, offset, keys[i])
    writeU32LE(buf, offset + 8, uint32(values[i].len))
    writeU32LE(buf, offset + 12, 0)
    offset += 16
    for b in values[i]:
      buf[offset] = b
      offset.inc
  buf

proc buildInternal(pageSize: int, keys: seq[uint64], children: seq[uint32], rightChild: uint32): seq[byte] =
  var buf = newSeq[byte](pageSize)
  buf[0] = PageTypeInternal
  buf[1] = 0
  writeU32LE(buf, 4, rightChild)
  let count = uint16(keys.len)
  buf[2] = byte(count and 0xFF)
  buf[3] = byte((count shr 8) and 0xFF)
  var offset = 8
  for i in 0 ..< keys.len:
    writeU64LE(buf, offset, keys[i])
    writeU32LE(buf, offset + 8, children[i])
    offset += 12
  buf

suite "BTree":
  test "lookup and cursor ordering":
    let path = makeTempDb("decentdb_btree_read.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let leaf1Res = allocatePage(pager)
    let leaf2Res = allocatePage(pager)
    let rootRes = allocatePage(pager)
    check leaf1Res.ok
    check leaf2Res.ok
    check rootRes.ok
    let leaf1 = leaf1Res.value
    let leaf2 = leaf2Res.value
    let root = rootRes.value
    let leaf1Buf = buildLeaf(pager.pageSize, @[1'u64, 2'u64], @[@[byte(1)], @[byte(2)]], uint32(leaf2))
    let leaf2Buf = buildLeaf(pager.pageSize, @[3'u64, 4'u64], @[@[byte(3)], @[byte(4)]], 0'u32)
    let rootBuf = buildInternal(pager.pageSize, @[3'u64], @[uint32(leaf1)], uint32(leaf2))
    check writePage(pager, leaf1, leaf1Buf).ok
    check writePage(pager, leaf2, leaf2Buf).ok
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    let findRes = find(tree, 4)
    check findRes.ok
    check findRes.value[0] == 4'u64
    let cursorRes = openCursor(tree)
    check cursorRes.ok
    let cursor = cursorRes.value
    var seen: seq[uint64] = @[]
    while true:
      let nextRes = cursorNext(cursor)
      if not nextRes.ok:
        break
      seen.add(nextRes.value[0])
    check seen == @[1'u64, 2'u64, 3'u64, 4'u64]
    discard closePager(pager)
    discard closeDb(db)

  test "insert split update delete":
    let path = makeTempDb("decentdb_btree_write.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 8)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newSeq[byte](pager.pageSize)
    rootBuf[0] = PageTypeLeaf
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    for i in 1 .. 200:
      let payload = @[byte(i mod 256)]
      check insert(tree, uint64(i), payload).ok
    let findRes = find(tree, 150)
    check findRes.ok
    check findRes.value[0] == 150'u64
    check findRes.value[1].len == 1
    check update(tree, 150, @[byte(9)]).ok
    let updated = find(tree, 150)
    check updated.ok
    check updated.value[1][0] == byte(9)
    check delete(tree, 150).ok
    let deleted = find(tree, 150)
    check deleted.ok == false
    var seen: seq[uint64] = @[]
    let cursorRes = openCursor(tree)
    check cursorRes.ok
    let cursor = cursorRes.value
    while true:
      let nextRes = cursorNext(cursor)
      if not nextRes.ok:
        break
      if nextRes.value[1].len == 0 and nextRes.value[2] == 0'u32:
        continue
      seen.add(nextRes.value[0])
    check seen.len == 199
    discard closePager(pager)
    discard closeDb(db)

  test "delete affecting root":
    let path = makeTempDb("decentdb_btree_root_delete.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newSeq[byte](pager.pageSize)
    rootBuf[0] = PageTypeLeaf
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    # Insert 1 item
    check insert(tree, 1, @[byte(1)]).ok
    
    # Verify we can find it
    check find(tree, 1).ok
    
    # Delete it
    check delete(tree, 1).ok
    
    # Verify we can't find it
    check not find(tree, 1).ok
    
    # Iterating empty tree
    let cursorRes = openCursor(tree)
    check cursorRes.ok
    let cursor = cursorRes.value
    let nextRes = cursorNext(cursor)
    if nextRes.ok:
      echo "Cursor found unexpected item: ", nextRes.value
    check not nextRes.ok
    
    discard closePager(pager)
    discard closeDb(db)
  
  test "iterate empty tree":
    let path = makeTempDb("decentdb_btree_empty.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newSeq[byte](pager.pageSize)
    rootBuf[0] = PageTypeLeaf
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    let cursorRes = openCursor(tree)
    check cursorRes.ok
    check not cursorNext(cursorRes.value).ok
    
    discard closePager(pager)
    discard closeDb(db)
