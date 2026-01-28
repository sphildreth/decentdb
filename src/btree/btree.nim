import ../errors
import ../pager/pager
import ../pager/db_header

const
  PageTypeInternal* = 1'u8
  PageTypeLeaf* = 2'u8

type BTree* = ref object
  pager*: Pager
  root*: PageId

type BTreeCursor* = ref object
  tree*: BTree
  leaf*: PageId
  index*: int
  keys*: seq[uint64]
  values*: seq[seq[byte]]
  overflows*: seq[uint32]
  nextLeaf*: PageId

proc readU16LE(buf: openArray[byte], offset: int): uint16 =
  uint16(buf[offset]) or (uint16(buf[offset + 1]) shl 8)

proc readLeafCells(page: openArray[byte]): Result[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)] =
  if page.len < 8:
    return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](ERR_CORRUPTION, "Page too small")
  if page[0] != PageTypeLeaf:
    return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](ERR_CORRUPTION, "Not a leaf page")
  let count = int(readU16LE(page, 2))
  let nextLeaf = PageId(readU32LE(page, 4))
  var keys: seq[uint64] = @[]
  var values: seq[seq[byte]] = @[]
  var overflows: seq[uint32] = @[]
  var offset = 8
  for _ in 0 ..< count:
    if offset + 16 > page.len:
      return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](ERR_CORRUPTION, "Leaf cell out of bounds")
    let key = readU64LE(page, offset)
    let valueLen = int(readU32LE(page, offset + 8))
    let overflow = readU32LE(page, offset + 12)
    offset += 16
    if offset + valueLen > page.len:
      return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](ERR_CORRUPTION, "Leaf value out of bounds")
    var payload = newSeq[byte](valueLen)
    if valueLen > 0:
      copyMem(addr payload[0], unsafeAddr page[offset], valueLen)
    offset += valueLen
    keys.add(key)
    values.add(payload)
    overflows.add(overflow)
  ok((keys, values, overflows, nextLeaf))

proc readInternalCells(page: openArray[byte]): Result[(seq[uint64], seq[uint32], uint32)] =
  if page.len < 8:
    return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Page too small")
  if page[0] != PageTypeInternal:
    return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Not an internal page")
  let count = int(readU16LE(page, 2))
  let rightChild = readU32LE(page, 4)
  var keys: seq[uint64] = @[]
  var children: seq[uint32] = @[]
  var offset = 8
  for _ in 0 ..< count:
    if offset + 12 > page.len:
      return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Internal cell out of bounds")
    let key = readU64LE(page, offset)
    let child = readU32LE(page, offset + 8)
    offset += 12
    keys.add(key)
    children.add(child)
  ok((keys, children, rightChild))

proc newBTree*(pager: Pager, root: PageId): BTree =
  BTree(pager: pager, root: root)

proc findLeaf(tree: BTree, key: uint64): Result[PageId] =
  var current = tree.root
  while true:
    let pageRes = readPage(tree.pager, current)
    if not pageRes.ok:
      return err[PageId](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    let page = pageRes.value
    let pageType = page[0]
    if pageType == PageTypeLeaf:
      return ok(current)
    let internalRes = readInternalCells(page)
    if not internalRes.ok:
      return err[PageId](internalRes.err.code, internalRes.err.message, internalRes.err.context)
    let (keys, children, rightChild) = internalRes.value
    var chosen = rightChild
    for i in 0 ..< keys.len:
      if key < keys[i]:
        chosen = children[i]
        break
    current = PageId(chosen)

proc find*(tree: BTree, key: uint64): Result[(uint64, seq[byte], uint32)] =
  let leafRes = findLeaf(tree, key)
  if not leafRes.ok:
    return err[(uint64, seq[byte], uint32)](leafRes.err.code, leafRes.err.message, leafRes.err.context)
  let leafId = leafRes.value
  let pageRes = readPage(tree.pager, leafId)
  if not pageRes.ok:
    return err[(uint64, seq[byte], uint32)](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  let parsed = readLeafCells(pageRes.value)
  if not parsed.ok:
    return err[(uint64, seq[byte], uint32)](parsed.err.code, parsed.err.message, parsed.err.context)
  let (keys, values, overflows, _) = parsed.value
  for i in 0 ..< keys.len:
    if keys[i] == key:
      return ok((keys[i], values[i], overflows[i]))
  err[(uint64, seq[byte], uint32)](ERR_IO, "Key not found")

proc openCursor*(tree: BTree): Result[BTreeCursor] =
  var current = tree.root
  while true:
    let pageRes = readPage(tree.pager, current)
    if not pageRes.ok:
      return err[BTreeCursor](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    let page = pageRes.value
    let pageType = page[0]
    if pageType == PageTypeLeaf:
      let parsed = readLeafCells(page)
      if not parsed.ok:
        return err[BTreeCursor](parsed.err.code, parsed.err.message, parsed.err.context)
      let (keys, values, overflows, nextLeaf) = parsed.value
      let cursor = BTreeCursor(tree: tree, leaf: current, index: 0, keys: keys, values: values, overflows: overflows, nextLeaf: nextLeaf)
      return ok(cursor)
    let internalRes = readInternalCells(page)
    if not internalRes.ok:
      return err[BTreeCursor](internalRes.err.code, internalRes.err.message, internalRes.err.context)
    let (_, children, _) = internalRes.value
    if children.len == 0:
      return err[BTreeCursor](ERR_CORRUPTION, "Empty internal page")
    current = PageId(children[0])

proc cursorNext*(cursor: BTreeCursor): Result[(uint64, seq[byte], uint32)] =
  if cursor.leaf == 0:
    return err[(uint64, seq[byte], uint32)](ERR_IO, "Cursor exhausted")
  if cursor.index < cursor.keys.len:
    let i = cursor.index
    cursor.index.inc
    return ok((cursor.keys[i], cursor.values[i], cursor.overflows[i]))
  if cursor.nextLeaf == 0:
    cursor.leaf = 0
    return err[(uint64, seq[byte], uint32)](ERR_IO, "Cursor exhausted")
  let pageRes = readPage(cursor.tree.pager, cursor.nextLeaf)
  if not pageRes.ok:
    return err[(uint64, seq[byte], uint32)](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  let parsed = readLeafCells(pageRes.value)
  if not parsed.ok:
    return err[(uint64, seq[byte], uint32)](parsed.err.code, parsed.err.message, parsed.err.context)
  let (keys, values, overflows, nextLeaf) = parsed.value
  cursor.leaf = cursor.nextLeaf
  cursor.keys = keys
  cursor.values = values
  cursor.overflows = overflows
  cursor.nextLeaf = nextLeaf
  cursor.index = 0
  cursorNext(cursor)
