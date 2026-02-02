import unittest
import os

import engine
import pager/pager
import btree/btree
import pager/db_header
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "BTree Error Paths":
  test "find in empty tree":
    let path = makeTempDb("decentdb_btree_find_empty.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    let findRes = find(tree, 1)
    check not findRes.ok
    
    discard closePager(pager)
    discard closeDb(db)

  test "update non-existent key":
    let path = makeTempDb("decentdb_btree_update_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    let updateRes = update(tree, 999, @[byte(1)])
    check not updateRes.ok
    
    discard closePager(pager)
    discard closeDb(db)

  test "delete non-existent key":
    let path = makeTempDb("decentdb_btree_delete_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    let deleteRes = delete(tree, 999)
    check not deleteRes.ok
    
    discard closePager(pager)
    discard closeDb(db)

  test "deleteKeyValue with non-existent key":
    let path = makeTempDb("decentdb_btree_deletekv_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    let deleteRes = deleteKeyValue(tree, 999, @[byte(1)])
    check deleteRes.ok
    check deleteRes.value == false
    
    discard closePager(pager)
    discard closeDb(db)

  test "openCursorAt with non-existent key":
    let path = makeTempDb("decentdb_btree_cursorat.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    let cursorRes = openCursorAt(tree, 999)
    check cursorRes.ok
    let cursor = cursorRes.value
    let nextRes = cursorNext(cursor)
    check not nextRes.ok
    
    discard closePager(pager)
    discard closeDb(db)

  test "openCursorAt then iterate all keys":
    let path = makeTempDb("decentdb_btree_cursorat_iterate.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    check insert(tree, 10, @[byte(10)]).ok
    check insert(tree, 20, @[byte(20)]).ok
    check insert(tree, 30, @[byte(30)]).ok
    
    let cursorRes = openCursorAt(tree, 15)
    check cursorRes.ok
    let cursor = cursorRes.value
    
    var seen: seq[uint64] = @[]
    while true:
      let nextRes = cursorNext(cursor)
      if not nextRes.ok:
        break
      seen.add(nextRes.value[0])
    
    check seen == @[20'u64, 30'u64]
    
    discard closePager(pager)
    discard closeDb(db)

  test "insert duplicate key":
    let path = makeTempDb("decentdb_btree_dupkey.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    # First insert
    check insert(tree, 1, @[byte(1)]).ok
    let findRes1 = find(tree, 1)
    check findRes1.ok
    check findRes1.value[1] == @[byte(1)]
    
    # Insert with same key - BTree behavior depends on implementation
    # It may add as duplicate or replace
    let insertRes2 = insert(tree, 1, @[byte(2)])
    check insertRes2.ok
    
    # The behavior depends on BTree implementation
    # Either it adds duplicate or replaces
    let findRes2 = find(tree, 1)
    check findRes2.ok
    
    discard closePager(pager)
    discard closeDb(db)

  test "bulkBuildFromSorted with empty input":
    let path = makeTempDb("decentdb_btree_bulk_empty.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    let entries: seq[(uint64, seq[byte])] = @[]
    let buildRes = bulkBuildFromSorted(tree, entries)
    check buildRes.ok
    
    discard closePager(pager)
    discard closeDb(db)

  test "leaf split handles clustered large inline values (no leaf overflow)":
    let path = makeTempDb("decentdb_btree_leaf_split_by_size.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)

    # Many small values, then two large values adjacent, then more small values.
    # With naive count-based splitting, this can place both large values into the
    # same leaf half and trigger ERR_IO "Leaf overflow" even though a valid split
    # exists.
    for k in 1'u64 .. 60'u64:
      check insert(tree, k, @[byte(k and 0xFF)]).ok

    let big = newSeq[byte](3000)
    check insert(tree, 61, big).ok
    check insert(tree, 62, big).ok

    for k in 63'u64 .. 120'u64:
      check insert(tree, k, @[byte(k and 0xFF)]).ok

    discard closePager(pager)
    discard closeDb(db)

  test "bulkBuildFromSorted with single entry":
    let path = makeTempDb("decentdb_btree_bulk_single.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    let entries = @[(1'u64, @[byte(1)])]
    let buildRes = bulkBuildFromSorted(tree, entries)
    check buildRes.ok
    
    let findRes = find(tree, 1)
    check findRes.ok
    check findRes.value[1] == @[byte(1)]
    
    discard closePager(pager)
    discard closeDb(db)

  test "bulkBuildFromSorted with many entries":
    let path = makeTempDb("decentdb_btree_bulk_many.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 16)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    var entries: seq[(uint64, seq[byte])] = @[]
    for i in 1 .. 100:
      entries.add((uint64(i), @[byte(i mod 256)]))
    
    let buildRes = bulkBuildFromSorted(tree, entries)
    check buildRes.ok
    
    # Verify some entries
    check find(tree, 1).ok
    check find(tree, 50).ok
    check find(tree, 100).ok
    check not find(tree, 101).ok
    
    discard closePager(pager)
    discard closeDb(db)

  test "deleteKeyValue with matching value":
    let path = makeTempDb("decentdb_btree_deletekv_match.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    check insert(tree, 1, @[byte(1)]).ok
    check insert(tree, 1, @[byte(2)]).ok
    
    let deleteRes = deleteKeyValue(tree, 1, @[byte(1)])
    check deleteRes.ok
    check deleteRes.value == true
    
    # One value should remain
    let findRes = find(tree, 1)
    check findRes.ok
    check findRes.value[1] == @[byte(2)]
    
    discard closePager(pager)
    discard closeDb(db)

  test "cursor on tree with deleted entries":
    let path = makeTempDb("decentdb_btree_cursor_deleted.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file, cachePages = 4)
    check pagerRes.ok
    let pager = pagerRes.value
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    writeU32LE(rootBuf, 4, 0)
    check writePage(pager, root, rootBuf).ok
    let tree = newBTree(pager, root)
    
    check insert(tree, 1, @[byte(1)]).ok
    check insert(tree, 2, @[byte(2)]).ok
    check insert(tree, 3, @[byte(3)]).ok
    
    check delete(tree, 2).ok
    
    let cursorRes = openCursor(tree)
    check cursorRes.ok
    let cursor = cursorRes.value
    
    var seen: seq[uint64] = @[]
    while true:
      let nextRes = cursorNext(cursor)
      if not nextRes.ok:
        break
      if nextRes.value[1].len > 0 or nextRes.value[2] != 0:
        seen.add(nextRes.value[0])
    
    check seen == @[1'u64, 3'u64]
    
    discard closePager(pager)
    discard closeDb(db)
