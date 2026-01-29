import options
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

proc readU16LE(buf: string, offset: int): uint16 =
  uint16(byte(buf[offset])) or (uint16(byte(buf[offset + 1])) shl 8)

proc readLeafCells(page: string): Result[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)] =
  if page.len < 8:
    return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](ERR_CORRUPTION, "Page too small")
  if byte(page[0]) != PageTypeLeaf:
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

proc readInternalCells(page: string): Result[(seq[uint64], seq[uint32], uint32)] =
  if page.len < 8:
    return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Page too small")
  if byte(page[0]) != PageTypeInternal:
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
    var pageType: byte = 0
    var keys: seq[uint64] = @[]
    var children: seq[uint32] = @[]
    var rightChild: uint32 = 0
    let pageRes = tree.pager.withPageRo(current, proc(page: string): Result[Void] =
      pageType = byte(page[0])
      if pageType == PageTypeLeaf:
        return okVoid()
      let internalRes = readInternalCells(page)
      if not internalRes.ok:
        return err[Void](internalRes.err.code, internalRes.err.message, internalRes.err.context)
      (keys, children, rightChild) = internalRes.value
      okVoid()
    )
    if not pageRes.ok:
      return err[PageId](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    if pageType == PageTypeLeaf:
      return ok(current)
    var chosen = rightChild
    for i in 0 ..< keys.len:
      if key < keys[i]:
        chosen = children[i]
        break
    current = PageId(chosen)

proc findLeafLeftmost(tree: BTree, key: uint64): Result[PageId] =
  var current = tree.root
  while true:
    var pageType: byte = 0
    var keys: seq[uint64] = @[]
    var children: seq[uint32] = @[]
    var rightChild: uint32 = 0
    let pageRes = tree.pager.withPageRo(current, proc(page: string): Result[Void] =
      pageType = byte(page[0])
      if pageType == PageTypeLeaf:
        return okVoid()
      let internalRes = readInternalCells(page)
      if not internalRes.ok:
        return err[Void](internalRes.err.code, internalRes.err.message, internalRes.err.context)
      (keys, children, rightChild) = internalRes.value
      okVoid()
    )
    if not pageRes.ok:
      return err[PageId](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    if pageType == PageTypeLeaf:
      return ok(current)
    var chosen = rightChild
    for i in 0 ..< keys.len:
      if key <= keys[i]:
        chosen = children[i]
        break
    current = PageId(chosen)

proc lowerBound(keys: seq[uint64], key: uint64): int =
  var lo = 0
  var hi = keys.len
  while lo < hi:
    let mid = (lo + hi) shr 1
    if keys[mid] < key:
      lo = mid + 1
    else:
      hi = mid
  lo

proc find*(tree: BTree, key: uint64): Result[(uint64, seq[byte], uint32)] =
  let leafRes = findLeaf(tree, key)
  if not leafRes.ok:
    return err[(uint64, seq[byte], uint32)](leafRes.err.code, leafRes.err.message, leafRes.err.context)
  let leafId = leafRes.value
  var keys: seq[uint64] = @[]
  var values: seq[seq[byte]] = @[]
  var overflows: seq[uint32] = @[]
  let pageRes = tree.pager.withPageRo(leafId, proc(page: string): Result[Void] =
    let parsed = readLeafCells(page)
    if not parsed.ok:
      return err[Void](parsed.err.code, parsed.err.message, parsed.err.context)
    (keys, values, overflows, _) = parsed.value
    okVoid()
  )
  if not pageRes.ok:
    return err[(uint64, seq[byte], uint32)](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  for i in 0 ..< keys.len:
    if keys[i] == key:
      if values[i].len == 0 and overflows[i] == 0'u32:
        return err[(uint64, seq[byte], uint32)](ERR_IO, "Key not found")
      return ok((keys[i], values[i], overflows[i]))
  err[(uint64, seq[byte], uint32)](ERR_IO, "Key not found")

proc openCursor*(tree: BTree): Result[BTreeCursor] =
  var current = tree.root
  while true:
    var pageType: byte = 0
    var keys: seq[uint64] = @[]
    var values: seq[seq[byte]] = @[]
    var overflows: seq[uint32] = @[]
    var nextLeaf: PageId = 0
    var children: seq[uint32] = @[]
    let pageRes = tree.pager.withPageRo(current, proc(page: string): Result[Void] =
      pageType = byte(page[0])
      if pageType == PageTypeLeaf:
        let parsed = readLeafCells(page)
        if not parsed.ok:
          return err[Void](parsed.err.code, parsed.err.message, parsed.err.context)
        (keys, values, overflows, nextLeaf) = parsed.value
        return okVoid()
      let internalRes = readInternalCells(page)
      if not internalRes.ok:
        return err[Void](internalRes.err.code, internalRes.err.message, internalRes.err.context)
      (_, children, _) = internalRes.value
      okVoid()
    )
    if not pageRes.ok:
      return err[BTreeCursor](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    if pageType == PageTypeLeaf:
      let cursor = BTreeCursor(tree: tree, leaf: current, index: 0, keys: keys, values: values, overflows: overflows, nextLeaf: nextLeaf)
      return ok(cursor)
    if children.len == 0:
      return err[BTreeCursor](ERR_CORRUPTION, "Empty internal page")
    current = PageId(children[0])

proc openCursorAt*(tree: BTree, startKey: uint64): Result[BTreeCursor] =
  let leafRes = findLeafLeftmost(tree, startKey)
  if not leafRes.ok:
    return err[BTreeCursor](leafRes.err.code, leafRes.err.message, leafRes.err.context)
  let leafId = leafRes.value
  var keys: seq[uint64] = @[]
  var values: seq[seq[byte]] = @[]
  var overflows: seq[uint32] = @[]
  var nextLeaf: PageId = 0
  let pageRes = tree.pager.withPageRo(leafId, proc(page: string): Result[Void] =
    let parsed = readLeafCells(page)
    if not parsed.ok:
      return err[Void](parsed.err.code, parsed.err.message, parsed.err.context)
    (keys, values, overflows, nextLeaf) = parsed.value
    okVoid()
  )
  if not pageRes.ok:
    return err[BTreeCursor](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  let idx = lowerBound(keys, startKey)
  let cursor = BTreeCursor(tree: tree, leaf: leafId, index: idx, keys: keys, values: values, overflows: overflows, nextLeaf: nextLeaf)
  ok(cursor)

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
  var keys: seq[uint64] = @[]
  var values: seq[seq[byte]] = @[]
  var overflows: seq[uint32] = @[]
  var nextLeaf: PageId = 0
  let pageRes = cursor.tree.pager.withPageRo(cursor.nextLeaf, proc(page: string): Result[Void] =
    let parsed = readLeafCells(page)
    if not parsed.ok:
      return err[Void](parsed.err.code, parsed.err.message, parsed.err.context)
    (keys, values, overflows, nextLeaf) = parsed.value
    okVoid()
  )
  if not pageRes.ok:
    return err[(uint64, seq[byte], uint32)](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  cursor.leaf = cursor.nextLeaf
  cursor.keys = keys
  cursor.values = values
  cursor.overflows = overflows
  cursor.nextLeaf = nextLeaf
  cursor.index = 0
  cursorNext(cursor)

proc encodeLeaf(keys: seq[uint64], values: seq[seq[byte]], overflows: seq[uint32], nextLeaf: PageId, pageSize: int): Result[string] =
  if keys.len != values.len or keys.len != overflows.len:
    return err[string](ERR_INTERNAL, "Leaf encode length mismatch")
  var buf = newString(pageSize)
  buf[0] = char(PageTypeLeaf)
  buf[1] = '\0'
  let count = uint16(keys.len)
  buf[2] = char(byte(count and 0xFF))
  buf[3] = char(byte((count shr 8) and 0xFF))
  writeU32LE(buf, 4, uint32(nextLeaf))
  var offset = 8
  for i in 0 ..< keys.len:
    let valueLen = values[i].len
    if offset + 16 + valueLen > pageSize:
      return err[string](ERR_IO, "Leaf overflow")
    writeU64LE(buf, offset, keys[i])
    writeU32LE(buf, offset + 8, uint32(valueLen))
    writeU32LE(buf, offset + 12, overflows[i])
    offset += 16
    if valueLen > 0:
      copyMem(addr buf[offset], unsafeAddr values[i][0], valueLen)
      offset += valueLen
  ok(buf)

proc encodeInternal(keys: seq[uint64], children: seq[uint32], rightChild: uint32, pageSize: int): Result[string] =
  if children.len != keys.len:
    return err[string](ERR_INTERNAL, "Internal encode length mismatch")
  var buf = newString(pageSize)
  buf[0] = char(PageTypeInternal)
  buf[1] = '\0'
  let count = uint16(keys.len)
  buf[2] = char(byte(count and 0xFF))
  buf[3] = char(byte((count shr 8) and 0xFF))
  writeU32LE(buf, 4, rightChild)
  var offset = 8
  for i in 0 ..< keys.len:
    if offset + 12 > pageSize:
      return err[string](ERR_IO, "Internal overflow")
    writeU64LE(buf, offset, keys[i])
    writeU32LE(buf, offset + 8, children[i])
    offset += 12
  ok(buf)

proc bulkBuildFromSorted*(tree: BTree, entries: seq[(uint64, seq[byte])]): Result[PageId] =
  ## Build a B+Tree from entries sorted by (key, value).
  ##
  ## This is intended for bulk index builds where keys are already sorted, to
  ## avoid O(N log N) insertion work and reduce split churn.
  ##
  ## Note: This builds new pages and may change the tree root page id.
  let pageSize = tree.pager.pageSize
  if entries.len == 0:
    let bufRes = encodeLeaf(@[], @[], @[], 0, pageSize)
    if not bufRes.ok:
      return err[PageId](bufRes.err.code, bufRes.err.message, bufRes.err.context)
    let writeRes = writePage(tree.pager, tree.root, bufRes.value)
    if not writeRes.ok:
      return err[PageId](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    return ok(tree.root)

  type ChildInfo = tuple[id: PageId, firstKey: uint64]

  var leaves: seq[ChildInfo] = @[]
  var currentLeaf = tree.root
  var currentKeys: seq[uint64] = @[]
  var currentVals: seq[seq[byte]] = @[]
  var currentOv: seq[uint32] = @[]

  proc flushLeaf(nextLeaf: PageId): Result[Void] =
    let bufRes = encodeLeaf(currentKeys, currentVals, currentOv, nextLeaf, pageSize)
    if not bufRes.ok:
      return err[Void](bufRes.err.code, bufRes.err.message, bufRes.err.context)
    let writeRes = writePage(tree.pager, currentLeaf, bufRes.value)
    if not writeRes.ok:
      return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    okVoid()

  for i, entry in entries:
    if currentKeys.len == 0:
      leaves.add((id: currentLeaf, firstKey: entry[0]))
    currentKeys.add(entry[0])
    currentVals.add(entry[1])
    currentOv.add(0'u32)
    let tryRes = encodeLeaf(currentKeys, currentVals, currentOv, 0, pageSize)
    if tryRes.ok:
      continue
    # Current leaf overflowed; move the last entry into a new leaf.
    currentKeys.setLen(currentKeys.len - 1)
    currentVals.setLen(currentVals.len - 1)
    currentOv.setLen(currentOv.len - 1)
    let newLeafRes = allocatePage(tree.pager)
    if not newLeafRes.ok:
      return err[PageId](newLeafRes.err.code, newLeafRes.err.message, newLeafRes.err.context)
    let newLeaf = newLeafRes.value
    let flushRes = flushLeaf(newLeaf)
    if not flushRes.ok:
      return err[PageId](flushRes.err.code, flushRes.err.message, flushRes.err.context)
    currentLeaf = newLeaf
    currentKeys = @[entry[0]]
    currentVals = @[entry[1]]
    currentOv = @[0'u32]
    leaves.add((id: currentLeaf, firstKey: entry[0]))

  let flushRes = flushLeaf(0)
  if not flushRes.ok:
    return err[PageId](flushRes.err.code, flushRes.err.message, flushRes.err.context)

  if leaves.len == 1:
    tree.root = leaves[0].id
    return ok(tree.root)

  proc maxInternalKeys(): int =
    (pageSize - 8) div 12

  var level = leaves
  while level.len > 1:
    var nextLevel: seq[ChildInfo] = @[]
    let maxKeys = maxInternalKeys()
    let maxChildren = maxKeys + 1
    var idx = 0
    while idx < level.len:
      let endIdx = min(idx + maxChildren, level.len)
      let group = level[idx ..< endIdx]
      if group.len == 1:
        # Degenerate parent: just bubble up the child (should only happen when
        # the final root is a single page).
        nextLevel.add(group[0])
        idx = endIdx
        continue
      let pageRes = allocatePage(tree.pager)
      if not pageRes.ok:
        return err[PageId](pageRes.err.code, pageRes.err.message, pageRes.err.context)
      let parentId = pageRes.value
      var keys: seq[uint64] = @[]
      var children: seq[uint32] = @[]
      for i in 0 ..< group.len - 1:
        children.add(uint32(group[i].id))
        keys.add(group[i + 1].firstKey)
      let rightChild = uint32(group[^1].id)
      let bufRes = encodeInternal(keys, children, rightChild, pageSize)
      if not bufRes.ok:
        return err[PageId](bufRes.err.code, bufRes.err.message, bufRes.err.context)
      let writeRes = writePage(tree.pager, parentId, bufRes.value)
      if not writeRes.ok:
        return err[PageId](writeRes.err.code, writeRes.err.message, writeRes.err.context)
      nextLevel.add((id: parentId, firstKey: group[0].firstKey))
      idx = endIdx
    level = nextLevel

  tree.root = level[0].id
  ok(tree.root)

type SplitResult = object
  promoted: uint64
  newPage: PageId

proc insertRecursive(tree: BTree, pageId: PageId, key: uint64, value: seq[byte]): Result[Option[SplitResult]] =
  let pageRes = readPageRo(tree.pager, pageId)
  if not pageRes.ok:
    return err[Option[SplitResult]](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  let page = pageRes.value
  if byte(page[0]) == PageTypeLeaf:
    let parsed = readLeafCells(page)
    if not parsed.ok:
      return err[Option[SplitResult]](parsed.err.code, parsed.err.message, parsed.err.context)
    var (keys, values, overflows, nextLeaf) = parsed.value
    var inserted = false
    for i in 0 ..< keys.len:
      if key < keys[i]:
        keys.insert(key, i)
        values.insert(value, i)
        overflows.insert(0'u32, i)
        inserted = true
        break
    if not inserted:
      keys.add(key)
      values.add(value)
      overflows.add(0)
    let encodeRes = encodeLeaf(keys, values, overflows, nextLeaf, tree.pager.pageSize)
    if encodeRes.ok:
      discard writePage(tree.pager, pageId, encodeRes.value)
      return ok(none(SplitResult))
    let mid = keys.len div 2
    let leftKeys = keys[0 ..< mid]
    let leftVals = values[0 ..< mid]
    let leftOv = overflows[0 ..< mid]
    let rightKeys = keys[mid ..< keys.len]
    let rightVals = values[mid ..< values.len]
    let rightOv = overflows[mid ..< overflows.len]
    let newRes = allocatePage(tree.pager)
    if not newRes.ok:
      return err[Option[SplitResult]](newRes.err.code, newRes.err.message, newRes.err.context)
    let newPage = newRes.value
    let rightBufRes = encodeLeaf(rightKeys, rightVals, rightOv, nextLeaf, tree.pager.pageSize)
    if not rightBufRes.ok:
      return err[Option[SplitResult]](rightBufRes.err.code, rightBufRes.err.message, rightBufRes.err.context)
    discard writePage(tree.pager, newPage, rightBufRes.value)
    let leftBufRes = encodeLeaf(leftKeys, leftVals, leftOv, newPage, tree.pager.pageSize)
    if not leftBufRes.ok:
      return err[Option[SplitResult]](leftBufRes.err.code, leftBufRes.err.message, leftBufRes.err.context)
    discard writePage(tree.pager, pageId, leftBufRes.value)
    return ok(some(SplitResult(promoted: rightKeys[0], newPage: newPage)))
  let internalRes = readInternalCells(page)
  if not internalRes.ok:
    return err[Option[SplitResult]](internalRes.err.code, internalRes.err.message, internalRes.err.context)
  var (keys, children, rightChild) = internalRes.value
  var childIndex = keys.len
  for i in 0 ..< keys.len:
    if key < keys[i]:
      childIndex = i
      break
  let childPage = if childIndex == keys.len: PageId(rightChild) else: PageId(children[childIndex])
  let splitRes = insertRecursive(tree, childPage, key, value)
  if not splitRes.ok:
    return err[Option[SplitResult]](splitRes.err.code, splitRes.err.message, splitRes.err.context)
  if splitRes.value.isNone:
    return ok(none(SplitResult))
  let split = splitRes.value.get
  if childIndex == keys.len:
    keys.add(split.promoted)
    children.add(uint32(childPage))
    rightChild = uint32(split.newPage)
  else:
    keys.insert(split.promoted, childIndex)
    children.insert(uint32(childPage), childIndex)
    children[childIndex + 1] = uint32(split.newPage)
  let encodeRes = encodeInternal(keys, children, rightChild, tree.pager.pageSize)
  if encodeRes.ok:
    discard writePage(tree.pager, pageId, encodeRes.value)
    return ok(none(SplitResult))
  let mid = keys.len div 2
  let promoted = keys[mid]
  let leftKeys = keys[0 ..< mid]
  let rightKeys = keys[mid + 1 ..< keys.len]
  let leftChildren = children[0 ..< mid]
  let rightChildren = children[mid + 1 ..< children.len]
  let leftRightChild = if mid < children.len: children[mid] else: rightChild
  let rightRightChild = rightChild
  let newRes = allocatePage(tree.pager)
  if not newRes.ok:
    return err[Option[SplitResult]](newRes.err.code, newRes.err.message, newRes.err.context)
  let newPage = newRes.value
  let leftBufRes = encodeInternal(leftKeys, leftChildren, leftRightChild, tree.pager.pageSize)
  let rightBufRes = encodeInternal(rightKeys, rightChildren, rightRightChild, tree.pager.pageSize)
  if not leftBufRes.ok:
    return err[Option[SplitResult]](leftBufRes.err.code, leftBufRes.err.message, leftBufRes.err.context)
  if not rightBufRes.ok:
    return err[Option[SplitResult]](rightBufRes.err.code, rightBufRes.err.message, rightBufRes.err.context)
  discard writePage(tree.pager, pageId, leftBufRes.value)
  discard writePage(tree.pager, newPage, rightBufRes.value)
  ok(some(SplitResult(promoted: promoted, newPage: newPage)))

proc insert*(tree: BTree, key: uint64, value: seq[byte]): Result[Void] =
  let splitRes = insertRecursive(tree, tree.root, key, value)
  if not splitRes.ok:
    return err[Void](splitRes.err.code, splitRes.err.message, splitRes.err.context)
  if splitRes.value.isSome:
    let split = splitRes.value.get
    let newRootRes = allocatePage(tree.pager)
    if not newRootRes.ok:
      return err[Void](newRootRes.err.code, newRootRes.err.message, newRootRes.err.context)
    let newRoot = newRootRes.value
    let keys = @[split.promoted]
    let children = @[uint32(tree.root)]
    let rightChild = uint32(split.newPage)
    let bufRes = encodeInternal(keys, children, rightChild, tree.pager.pageSize)
    if not bufRes.ok:
      return err[Void](bufRes.err.code, bufRes.err.message, bufRes.err.context)
    discard writePage(tree.pager, newRoot, bufRes.value)
    tree.root = newRoot
  okVoid()

proc update*(tree: BTree, key: uint64, value: seq[byte]): Result[Void] =
  let leafRes = findLeaf(tree, key)
  if not leafRes.ok:
    return err[Void](leafRes.err.code, leafRes.err.message, leafRes.err.context)
  let pageId = leafRes.value
  let pageRes = readPageRo(tree.pager, pageId)
  if not pageRes.ok:
    return err[Void](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  let parsed = readLeafCells(pageRes.value)
  if not parsed.ok:
    return err[Void](parsed.err.code, parsed.err.message, parsed.err.context)
  var (keys, values, overflows, nextLeaf) = parsed.value
  for i in 0 ..< keys.len:
    if keys[i] == key:
      values[i] = value
      overflows[i] = 0
      let encodeRes = encodeLeaf(keys, values, overflows, nextLeaf, tree.pager.pageSize)
      if not encodeRes.ok:
        return err[Void](encodeRes.err.code, encodeRes.err.message, encodeRes.err.context)
      discard writePage(tree.pager, pageId, encodeRes.value)
      return okVoid()
  err[Void](ERR_IO, "Key not found")

proc delete*(tree: BTree, key: uint64): Result[Void] =
  let leafRes = findLeaf(tree, key)
  if not leafRes.ok:
    return err[Void](leafRes.err.code, leafRes.err.message, leafRes.err.context)
  let pageId = leafRes.value
  let pageRes = readPageRo(tree.pager, pageId)
  if not pageRes.ok:
    return err[Void](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  let parsed = readLeafCells(pageRes.value)
  if not parsed.ok:
    return err[Void](parsed.err.code, parsed.err.message, parsed.err.context)
  var (keys, values, overflows, nextLeaf) = parsed.value
  for i in 0 ..< keys.len:
    if keys[i] == key:
      keys.delete(i)
      values.delete(i)
      overflows.delete(i)
      let encodeRes = encodeLeaf(keys, values, overflows, nextLeaf, tree.pager.pageSize)
      if not encodeRes.ok:
        return err[Void](encodeRes.err.code, encodeRes.err.message, encodeRes.err.context)
      discard writePage(tree.pager, pageId, encodeRes.value)
      return okVoid()
  err[Void](ERR_IO, "Key not found")

proc deleteKeyValue*(tree: BTree, key: uint64, value: seq[byte]): Result[bool] =
  let leafRes = findLeafLeftmost(tree, key)
  if not leafRes.ok:
    return err[bool](leafRes.err.code, leafRes.err.message, leafRes.err.context)
  var current = leafRes.value
  while current != 0:
    let pageRes = readPageRo(tree.pager, current)
    if not pageRes.ok:
      return err[bool](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    let parsed = readLeafCells(pageRes.value)
    if not parsed.ok:
      return err[bool](parsed.err.code, parsed.err.message, parsed.err.context)
    var (keys, values, overflows, nextLeaf) = parsed.value
    for i in 0 ..< keys.len:
      if keys[i] < key:
        continue
      if keys[i] > key:
        return ok(false)
      if values[i] == value and overflows[i] == 0'u32:
        keys.delete(i)
        values.delete(i)
        overflows.delete(i)
        let encodeRes = encodeLeaf(keys, values, overflows, nextLeaf, tree.pager.pageSize)
        if not encodeRes.ok:
          return err[bool](encodeRes.err.code, encodeRes.err.message, encodeRes.err.context)
        discard writePage(tree.pager, current, encodeRes.value)
        return ok(true)
    current = nextLeaf
  ok(false)
