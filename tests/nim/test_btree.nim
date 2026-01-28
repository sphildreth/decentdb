import unittest
import os
import engine
import pager/pager
import btree/btree
import pager/db_header

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
    let leaf1Buf = buildLeaf(pager.pageSize, @[1'u64, 2'u64], @[newSeq[byte](0), newSeq[byte](0)], uint32(leaf2))
    let leaf2Buf = buildLeaf(pager.pageSize, @[3'u64, 4'u64], @[newSeq[byte](0), newSeq[byte](0)], 0'u32)
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
