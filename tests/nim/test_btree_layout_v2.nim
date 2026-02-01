import unittest
import os
import engine
import pager/pager
import btree/btree
import record/record
import pager/db_header
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

suite "BTree Layout V2":
  test "compact leaf headers allow more rows":
    let path = makeTempDb("decentdb_btree_v2_compact.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let pagerRes = newPager(db.vfs, db.file)
    check pagerRes.ok
    let pager = pagerRes.value
    
    let rootRes = allocatePage(pager)
    check rootRes.ok
    let root = rootRes.value
    
    # Initialize root as leaf
    var rootBuf = newString(pager.pageSize)
    rootBuf[0] = char(PageTypeLeaf)
    check writePage(pager, root, rootBuf).ok
    
    let tree = newBTree(pager, root)
    
    # Insert minimal records: key=small, value=1 byte
    # Old format overhead: 16 bytes header + 1 byte payload = 17 bytes per row.
    # Max rows in 4096 bytes (approx): (4096 - 8) / 17 = ~240.
    # New format overhead: 
    #   Key < 128: 1 byte
    #   Control < 128 (len 1): 1 byte
    #   Payload: 1 byte
    #   Total: 3 bytes per row.
    # Max rows: (4096 - 8) / 3 = ~1362.
    
    # We insert 500 rows. 
    # In V3, this would split (500 > 240).
    # In V4, this should fit in one page (500 < 1362).
    
    for i in 0 ..< 500:
       check insert(tree, uint64(i), @[byte(1)]).ok
       
    # Check root page type.
    # If it fit in one page, root is still leaf.
    # If it split, root is internal.
    
    let pageRes = readPageRo(pager, tree.root)
    check pageRes.ok
    let page = pageRes.value
    check byte(page[0]) == PageTypeLeaf 
    
    # Verify utilization is reasonable (should be around 500 * 3 / 4096 = ~36%)
    let utilRes = calculatePageUtilization(tree, tree.root)
    check utilRes.ok
    echo "Utilization: ", utilRes.value
    check utilRes.value > 30.0
    check utilRes.value < 47.0 # 500 * 3.75 approx = 1880 bytes. 1880/4096 = 45.9%
    
    # Verify we can read them back
    let cursorRes = openCursor(tree)
    check cursorRes.ok
    let cursor = cursorRes.value
    var count = 0
    while true:
      let nextRes = cursorNext(cursor)
      if not nextRes.ok: break
      count.inc
    check count == 500
    
    discard closePager(pager)
    discard closeDb(db)
