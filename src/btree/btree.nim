import options
import algorithm
import ../errors
import ../pager/pager
import ../pager/db_header
import ../record/record
import sets

const
  PageTypeInternal* = 1'u8
  PageTypeLeaf* = 2'u8
  MaxLeafInlineValueBytes = 512

type
  InternalNodeIndex = ref object of RootRef
    keys: seq[uint64]
    children: seq[uint32]
    rightChild: uint32

  LeafNodeIndex = ref object of RootRef
    keys: seq[uint64]
    valueOffsets: seq[int]
    valueLens: seq[int]
    overflows: seq[uint32]

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

type BTreeCursorView* = ref object
  ## Cursor that avoids allocating/copying leaf payloads.
  ## Values are accessed as (page string, offset, len) views when inline.
  tree*: BTree
  leaf*: PageId
  index*: int
  keys*: seq[uint64]
  valueOffsets*: seq[int]
  valueLens*: seq[int]
  overflows*: seq[uint32]
  nextLeaf*: PageId
  leafPage*: string

type BTreeCursorStream* = ref object
  ## Streaming cursor for full scans.
  ## Decodes leaf cells on-the-fly without allocating per-leaf arrays.
  tree*: BTree
  leaf*: PageId
  nextLeaf*: PageId
  leafPage*: string
  offset*: int
  remaining*: int

type LeafValue = object
  inline: seq[byte]
  overflow: uint32

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

  var data = newSeq[byte](page.len)
  if page.len > 0:
    copyMem(addr data[0], unsafeAddr page[0], page.len)

  for _ in 0 ..< count:
    if offset >= page.len:
      return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](ERR_CORRUPTION, "Leaf cell out of bounds")

    let keyRes = decodeVarint(data, offset)
    if not keyRes.ok:
      return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](keyRes.err.code, keyRes.err.message, keyRes.err.context)
    let key = keyRes.value

    let ctrlRes = decodeVarint(data, offset)
    if not ctrlRes.ok:
      return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](ctrlRes.err.code, ctrlRes.err.message, ctrlRes.err.context)
    let control = ctrlRes.value

    let isOverflow = (control and 1) != 0
    let val = uint32(control shr 1)

    var valueLen = 0
    var overflow = 0'u32

    if isOverflow:
      overflow = val
      valueLen = 0
    else:
      valueLen = int(val)
      overflow = 0

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

proc readLeafCellsView(page: string): Result[(seq[uint64], seq[int], seq[int], seq[uint32], PageId)] =
  if page.len < 8:
    return err[(seq[uint64], seq[int], seq[int], seq[uint32], PageId)](ERR_CORRUPTION, "Page too small")
  if byte(page[0]) != PageTypeLeaf:
    return err[(seq[uint64], seq[int], seq[int], seq[uint32], PageId)](ERR_CORRUPTION, "Not a leaf page")
  let count = int(readU16LE(page, 2))
  let nextLeaf = PageId(readU32LE(page, 4))
  var keys = newSeq[uint64](count)
  var valueOffsets = newSeq[int](count)
  var valueLens = newSeq[int](count)
  var overflows = newSeq[uint32](count)
  var offset = 8

  for i in 0 ..< count:
    if offset >= page.len:
      return err[(seq[uint64], seq[int], seq[int], seq[uint32], PageId)](ERR_CORRUPTION, "Leaf cell out of bounds")

    let keyRes = decodeVarint(page, offset)
    if not keyRes.ok:
      return err[(seq[uint64], seq[int], seq[int], seq[uint32], PageId)](keyRes.err.code, keyRes.err.message, keyRes.err.context)
    keys[i] = keyRes.value

    let ctrlRes = decodeVarint(page, offset)
    if not ctrlRes.ok:
      return err[(seq[uint64], seq[int], seq[int], seq[uint32], PageId)](ctrlRes.err.code, ctrlRes.err.message, ctrlRes.err.context)
    let control = ctrlRes.value

    let isOverflow = (control and 1) != 0
    let val = uint32(control shr 1)

    if isOverflow:
      overflows[i] = val
      valueOffsets[i] = 0
      valueLens[i] = 0
    else:
      let valueLen = int(val)
      if offset + valueLen > page.len:
        return err[(seq[uint64], seq[int], seq[int], seq[uint32], PageId)](ERR_CORRUPTION, "Leaf value out of bounds")
      overflows[i] = 0'u32
      valueOffsets[i] = offset
      valueLens[i] = valueLen
      offset += valueLen

  ok((keys, valueOffsets, valueLens, overflows, nextLeaf))

proc readInternalCells(page: string): Result[(seq[uint64], seq[uint32], uint32)] =
  if page.len < 8:
    return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Page too small")
  if byte(page[0]) != PageTypeInternal:
    # stderr.writeLine("FAIL: Not an internal page (type=" & $int(byte(page[0])) & ") ...")
    return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Not an internal page (type=" & $int(byte(page[0])) & ")")
  let count = int(readU16LE(page, 2))
  let rightChild = readU32LE(page, 4)
  var keys: seq[uint64] = @[]
  var children: seq[uint32] = @[]
  var offset = 8

  var data = newSeq[byte](page.len)
  if page.len > 0:
    copyMem(addr data[0], unsafeAddr page[0], page.len)

  for _ in 0 ..< count:
    if offset >= page.len:
      return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Internal cell out of bounds")
    
    let keyRes = decodeVarint(data, offset)
    if not keyRes.ok:
      return err[(seq[uint64], seq[uint32], uint32)](keyRes.err.code, keyRes.err.message, keyRes.err.context)
    let key = keyRes.value

    let childRes = decodeVarint(data, offset)
    if not childRes.ok:
      return err[(seq[uint64], seq[uint32], uint32)](childRes.err.code, childRes.err.message, childRes.err.context)
    let child = uint32(childRes.value)

    keys.add(key)
    children.add(child)
  ok((keys, children, rightChild))

proc freeBTreePagesExceptRoot*(pager: Pager, root: PageId): Result[Void] =
  ## Free all pages in the B+Tree rooted at `root`, except the root page.
  ##
  ## This is used by index rebuild to avoid leaking unreachable pages.
  ## It also frees any overflow chains referenced by leaf cells.
  if root == 0:
    return okVoid()

  var stack: seq[PageId] = @[root]
  var visited = initHashSet[PageId]()
  visited.incl(root)

  var pagesToFree: seq[PageId] = @[]

  while stack.len > 0:
    let current = stack[^1]
    stack.setLen(stack.len - 1)

    var pageType: byte = 0
    var children: seq[uint32] = @[]
    var rightChild: uint32 = 0
    var overflows: seq[uint32] = @[]

    let pageRes = pager.withPageRo(current, proc(page: string): Result[Void] =
      pageType = byte(page[0])
      if pageType == PageTypeInternal:
        let internalRes = readInternalCells(page)
        if not internalRes.ok:
          return err[Void](internalRes.err.code, internalRes.err.message, internalRes.err.context)
        (_, children, rightChild) = internalRes.value
        return okVoid()
      if pageType == PageTypeLeaf:
        let leafRes = readLeafCells(page)
        if not leafRes.ok:
          return err[Void](leafRes.err.code, leafRes.err.message, leafRes.err.context)
        (_, _, overflows, _) = leafRes.value
        return okVoid()
      err[Void](ERR_CORRUPTION, "Unknown BTree page type", "page_id=" & $current)
    )
    if not pageRes.ok:
      return err[Void](pageRes.err.code, pageRes.err.message, pageRes.err.context)

    if pageType == PageTypeInternal:
      for child in children:
        let childId = PageId(child)
        if childId != 0 and not visited.contains(childId):
          visited.incl(childId)
          stack.add(childId)
          pagesToFree.add(childId)
      let rightId = PageId(rightChild)
      if rightId != 0 and not visited.contains(rightId):
        visited.incl(rightId)
        stack.add(rightId)
        pagesToFree.add(rightId)
    else:
      for ov in overflows:
        if ov != 0'u32:
          let freeOvRes = freeOverflowChain(pager, PageId(ov))
          if not freeOvRes.ok:
            return err[Void](freeOvRes.err.code, freeOvRes.err.message, freeOvRes.err.context)

  for pageId in pagesToFree:
    let freeRes = freePage(pager, pageId)
    if not freeRes.ok:
      return err[Void](freeRes.err.code, freeRes.err.message, freeRes.err.context)

  okVoid()

proc newBTree*(pager: Pager, root: PageId): BTree =
  BTree(pager: pager, root: root)

proc getOrBuildInternalIndex(page: string, entry: CacheEntry): Result[InternalNodeIndex] =
  if entry.aux != nil:
    if entry.aux of InternalNodeIndex:
       return ok(InternalNodeIndex(entry.aux))
    entry.aux = nil

  if page.len < 8:
    return err[InternalNodeIndex](ERR_CORRUPTION, "Page too small")
  
  let count = int(readU16LE(page, 2))
  let rightChild = readU32LE(page, 4)
  var keys = newSeq[uint64](count)
  var children = newSeq[uint32](count)
  var offset = 8
  
  for i in 0 ..< count:
    if offset >= page.len:
      return err[InternalNodeIndex](ERR_CORRUPTION, "Internal cell out of bounds")
    
    var k: uint64
    if not decodeVarintFast(page, offset, k):
      return err[InternalNodeIndex](ERR_CORRUPTION, "Invalid key varint")
    keys[i] = k
    
    var c: uint64
    if not decodeVarintFast(page, offset, c):
      return err[InternalNodeIndex](ERR_CORRUPTION, "Invalid child varint")
    children[i] = uint32(c)
    
  let idx = InternalNodeIndex(keys: keys, children: children, rightChild: rightChild)
  entry.aux = idx
  ok(idx)

proc getOrBuildLeafIndex(page: string, entry: CacheEntry): Result[LeafNodeIndex] =
  if entry.aux != nil:
    if entry.aux of LeafNodeIndex:
       return ok(LeafNodeIndex(entry.aux))
    entry.aux = nil

  if page.len < 8:
    return err[LeafNodeIndex](ERR_CORRUPTION, "Page too small")
    
  let count = int(readU16LE(page, 2))
  var keys = newSeq[uint64](count)
  var valueOffsets = newSeq[int](count)
  var valueLens = newSeq[int](count)
  var overflows = newSeq[uint32](count)
  var offset = 8
  
  for i in 0 ..< count:
    if offset >= page.len:
      return err[LeafNodeIndex](ERR_CORRUPTION, "Leaf cell out of bounds")
    
    var k: uint64
    if not decodeVarintFast(page, offset, k):
      return err[LeafNodeIndex](ERR_CORRUPTION, "Invalid key varint")
    keys[i] = k
    
    var ctrl: uint64
    if not decodeVarintFast(page, offset, ctrl):
      return err[LeafNodeIndex](ERR_CORRUPTION, "Invalid control varint")
      
    let isOverflow = (ctrl and 1) != 0
    let val = uint32(ctrl shr 1)
    
    if isOverflow:
      overflows[i] = val
      valueOffsets[i] = 0
      valueLens[i] = 0
    else:
      overflows[i] = 0
      valueOffsets[i] = offset
      valueLens[i] = int(val)
      offset += int(val)
      
  let idx = LeafNodeIndex(keys: keys, valueOffsets: valueOffsets, valueLens: valueLens, overflows: overflows)
  entry.aux = idx
  ok(idx)

proc findChildPage(keys: seq[uint64], children: seq[uint32], rightChild: uint32, key: uint64): uint32 =
  ## Binary search to find the appropriate child page for a key.
  ## Finds the first key > search key and returns corresponding children[index].
  ## Returns rightChild if key >= all keys.
  var lo = 0
  var hi = keys.len
  while lo < hi:
    let mid = (lo + hi) shr 1
    if keys[mid] <= key:
      lo = mid + 1
    else:
      hi = mid
  if lo < keys.len:
    return children[lo]
  return rightChild

proc findChildInPage(page: string, searchKey: uint64): Result[uint32] =
  ## Linear scan of internal page cells to find child page.
  ## Avoids allocating sequences for keys/children.
  if page.len < 8:
    return err[uint32](ERR_CORRUPTION, "Page too small")
  
  let count = int(readU16LE(page, 2))
  let rightChild = readU32LE(page, 4)
  var offset = 8
  
  for _ in 0 ..< count:
    if offset >= page.len:
      return err[uint32](ERR_CORRUPTION, "Internal cell out of bounds")
    
    let keyRes = decodeVarint(page, offset)
    if not keyRes.ok:
      return err[uint32](keyRes.err.code, keyRes.err.message, keyRes.err.context)
    let key = keyRes.value
    
    let childRes = decodeVarint(page, offset)
    if not childRes.ok:
      return err[uint32](childRes.err.code, childRes.err.message, childRes.err.context)
    let child = uint32(childRes.value)
    
    if key > searchKey:
      return ok(child)
      
  ok(rightChild)

proc findLeaf(tree: BTree, key: uint64): Result[PageId] =
  ## Navigate from root to leaf, avoiding seq allocations on internal pages.
  var current = tree.root
  while true:
    var nextPage: PageId = 0
    var isLeaf = false
    
    let handleRes = tree.pager.acquirePageRo(current)
    if not handleRes.ok:
      return err[PageId](handleRes.err.code, handleRes.err.message, handleRes.err.context)
    var handle = handleRes.value
    
    
    try:
      let page = handle.getPage
      if page.len < 8:
        return err[PageId](ERR_CORRUPTION, "Page too small")
      
      let pType = byte(page[0])
      if pType == PageTypeLeaf:
        isLeaf = true
      elif pType != PageTypeInternal:
        return err[PageId](ERR_CORRUPTION, "Invalid page type " & $pType)
      
      # Read internal page header
      let count = int(readU16LE(page, 2))
      let rightChild = readU32LE(page, 4)
      
      if isLeaf:
        # We found the leaf, loop will terminate
        discard
      elif count == 0:
        nextPage = PageId(rightChild)
      else:
        # Optimizaation: Check for cached index
        if handle.entry != nil:
           let idxRes = getOrBuildInternalIndex(page, handle.entry)
           if not idxRes.ok:
             return err[PageId](idxRes.err.code, idxRes.err.message, idxRes.err.context)
           let idx = idxRes.value
           nextPage = PageId(findChildPage(idx.keys, idx.children, idx.rightChild, key))
        else:
           # Fallback to linear scan
           let childRes = findChildInPage(page, key)
           if not childRes.ok:
              return err[PageId](childRes.err.code, childRes.err.message, childRes.err.context)
           nextPage = PageId(childRes.value)
    finally:
      handle.release()

    if isLeaf:
      return ok(current)
    current = nextPage

proc findChildPageLeftmost(keys: seq[uint64], children: seq[uint32], rightChild: uint32, key: uint64): uint32 =
  ## Binary search to find the appropriate child page for a key (leftmost variant).
  ## Finds the first key >= search key and returns corresponding children[index].
  ## Returns rightChild if key > all keys.
  var lo = 0
  var hi = keys.len
  while lo < hi:
    let mid = (lo + hi) shr 1
    if keys[mid] < key:
      lo = mid + 1
    else:
      hi = mid
  if lo < keys.len:
    return children[lo]
  return rightChild

proc findLeafLeftmost(tree: BTree, key: uint64): Result[PageId] =
  ## Navigate from root to leaf for leftmost variant, avoiding seq allocations.
  var current = tree.root
  while true:
    var nextPage: PageId = 0
    var isLeaf = false
    let pageRes = tree.pager.withPageRo(current, proc(page: string): Result[Void] =
      if page.len < 8:
        return err[Void](ERR_CORRUPTION, "Page too small")
      let pageType = byte(page[0])
      if pageType == PageTypeLeaf:
        isLeaf = true
        return okVoid()
      if pageType != PageTypeInternal:
        return err[Void](ERR_CORRUPTION, "Not an internal page (type=" & $int(pageType) & ")")
      
      let count = int(readU16LE(page, 2))
      let rightChild = readU32LE(page, 4)
      
      if count == 0:
        nextPage = PageId(rightChild)
        return okVoid()
      
      # Decode all keys inline
      var offset = 8
      var keyVals: array[256, uint64]
      var childVals: array[256, uint32]
      
      for i in 0 ..< count:
        # Decode key varint
        var k: uint64
        if not decodeVarintFast(page, offset, k):
          return err[Void](ERR_CORRUPTION, "Invalid key varint")
        
        if i < 256:
          keyVals[i] = k
        
        # Decode child varint
        var c: uint64
        if not decodeVarintFast(page, offset, c):
          return err[Void](ERR_CORRUPTION, "Invalid child varint")
        
        if i < 256:
          childVals[i] = uint32(c)
      
      # Binary search: find first key >= search key (leftmost variant)
      var lo = 0
      var hi = count
      while lo < hi:
        let mid = (lo + hi) shr 1
        if mid < 256 and keyVals[mid] < key:
          lo = mid + 1
        else:
          hi = mid
      
      if lo < count and lo < 256:
        nextPage = PageId(childVals[lo])
      else:
        nextPage = PageId(rightChild)
      okVoid()
    )
    if not pageRes.ok:
      return err[PageId](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    if isLeaf:
      return ok(current)
    current = nextPage

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

proc varintLen(value: uint64): int {.inline.} =
  var v = value
  result = 1
  while v >= 0x80'u64:
    v = v shr 7
    result.inc

proc leafEntryEncodedLen(key: uint64, valueLen: int, overflow: uint32): int {.inline.} =
  ## Number of bytes this entry contributes to a leaf page encoding (excluding
  ## the 8-byte page header).
  var control: uint64 = 0
  if overflow != 0'u32:
    control = (uint64(overflow) shl 1) or 1
  else:
    control = (uint64(valueLen) shl 1)
  varintLen(key) + varintLen(control) + valueLen

proc chooseLeafSplitPoint(keys: seq[uint64], values: seq[seq[byte]], overflows: seq[uint32], pageSize: int): Result[int] =
  ## Choose a split point for a leaf such that both halves fit in `pageSize`.
  ##
  ## This is required for variable-sized values: splitting by count can fail
  ## when large inline values cluster and end up on the same side.
  if keys.len != values.len or keys.len != overflows.len:
    return err[int](ERR_INTERNAL, "Leaf split length mismatch")
  if keys.len < 2:
    return err[int](ERR_IO, "Leaf overflow")

  let maxPayload = pageSize - 8
  var entrySizes: seq[int] = @[]
  entrySizes.setLen(keys.len)
  var total = 0
  for i in 0 ..< keys.len:
    let s = leafEntryEncodedLen(keys[i], values[i].len, overflows[i])
    if s > maxPayload:
      # Single cell can't fit even in an empty leaf.
      return err[int](ERR_IO, "Leaf overflow")
    entrySizes[i] = s
    total += s

  # Prefix sums for O(1) left/right size checks.
  var prefix: seq[int] = @[]
  prefix.setLen(keys.len + 1)
  prefix[0] = 0
  for i in 0 ..< keys.len:
    prefix[i + 1] = prefix[i] + entrySizes[i]

  var minSplitAt = -1
  for splitAt in 1 ..< keys.len:
    let r = 8 + (prefix[keys.len] - prefix[splitAt])
    if r <= pageSize:
      minSplitAt = splitAt
      break
  var maxSplitAt = -1
  for splitAt in 1 ..< keys.len:
    let l = 8 + prefix[splitAt]
    if l <= pageSize:
      maxSplitAt = splitAt

  if minSplitAt < 0 or maxSplitAt < 0 or minSplitAt > maxSplitAt:
    return err[int](ERR_IO, "Leaf overflow")

  var splitAt = keys.len div 2
  if splitAt < minSplitAt: splitAt = minSplitAt
  if splitAt > maxSplitAt: splitAt = maxSplitAt
  ok(splitAt)

proc maxInlineValue(tree: BTree): int =
  max(0, min(MaxLeafInlineValueBytes, tree.pager.pageSize - 24))

proc materializeValue(tree: BTree, inline: seq[byte], overflow: uint32): Result[seq[byte]] =
  if overflow == 0'u32:
    return ok(inline)
  let overflowRes = readOverflowChainAll(tree.pager, PageId(overflow))
  if not overflowRes.ok:
    return err[seq[byte]](overflowRes.err.code, overflowRes.err.message, overflowRes.err.context)
  if inline.len == 0:
    return ok(overflowRes.value)
  var merged = newSeq[byte](inline.len + overflowRes.value.len)
  if inline.len > 0:
    copyMem(addr merged[0], unsafeAddr inline[0], inline.len)
  if overflowRes.value.len > 0:
    copyMem(addr merged[inline.len], unsafeAddr overflowRes.value[0], overflowRes.value.len)
  ok(merged)

proc prepareLeafValue(tree: BTree, value: seq[byte]): Result[LeafValue] =
  let maxInline = maxInlineValue(tree)
  if value.len <= maxInline:
    return ok(LeafValue(inline: value, overflow: 0'u32))
  let ovRes = writeOverflowChain(tree.pager, value)
  if not ovRes.ok:
    return err[LeafValue](ovRes.err.code, ovRes.err.message, ovRes.err.context)
  ok(LeafValue(inline: @[], overflow: uint32(ovRes.value)))

proc find*(tree: BTree, key: uint64): Result[(uint64, seq[byte], uint32)] =
  let leafRes = findLeaf(tree, key)
  if not leafRes.ok:
    return err[(uint64, seq[byte], uint32)](leafRes.err.code, leafRes.err.message, leafRes.err.context)
  let leafId = leafRes.value
  
  # Optimized path: scan leaf directly without materializing all cells
  var foundKey: uint64 = 0
  var foundValue: seq[byte] = @[]
  var foundOverflow: uint32 = 0
  var found = false
  
  let pageRes = tree.pager.withPageRo(leafId, proc(page: string): Result[Void] =
    if page.len < 8:
      return err[Void](ERR_CORRUPTION, "Page too small")
    if byte(page[0]) != PageTypeLeaf:
      return err[Void](ERR_CORRUPTION, "Not a leaf page")
    let count = int(readU16LE(page, 2))
    var offset = 8
    
    for _ in 0 ..< count:
      if offset >= page.len:
        return err[Void](ERR_CORRUPTION, "Leaf cell out of bounds")
      
      # Decode key varint inline
      var k: uint64
      if not decodeVarintFast(page, offset, k):
        return err[Void](ERR_CORRUPTION, "Invalid key varint")
      
      # Decode control varint inline
      var ctrl: uint64
      if not decodeVarintFast(page, offset, ctrl):
        return err[Void](ERR_CORRUPTION, "Invalid control varint")
      
      let isOverflow = (ctrl and 1) != 0
      let val = uint32(ctrl shr 1)
      var valueLen = 0
      var overflow = 0'u32
      
      if isOverflow:
        overflow = val
      else:
        valueLen = int(val)
      
      if k == key:
        # Found the key - copy only this value
        if valueLen == 0 and overflow == 0'u32:
          return err[Void](ERR_IO, "Key not found")
        foundKey = k
        foundOverflow = overflow
        if valueLen > 0:
          if offset + valueLen > page.len:
            return err[Void](ERR_CORRUPTION, "Leaf value out of bounds")
          foundValue = newSeq[byte](valueLen)
          copyMem(addr foundValue[0], unsafeAddr page[offset], valueLen)
        found = true
        return okVoid()
      
      # Skip this value's bytes
      offset += valueLen

      if k > key:
        # Since keys are sorted, if we passed the key, it doesn't exist.
        return okVoid()
    
    okVoid()
  )
  
  if not pageRes.ok:
    return err[(uint64, seq[byte], uint32)](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  
  if not found:
    return err[(uint64, seq[byte], uint32)](ERR_IO, "Key not found")
  
  let valueRes = materializeValue(tree, foundValue, foundOverflow)
  if not valueRes.ok:
    return err[(uint64, seq[byte], uint32)](valueRes.err.code, valueRes.err.message, valueRes.err.context)
  ok((foundKey, valueRes.value, foundOverflow))

proc containsKey*(tree: BTree, key: uint64): Result[bool] =
  ## Return true if `key` exists in the tree.
  ##
  ## Optimized to scan leaf directly without materializing all cells.
  let leafRes = findLeaf(tree, key)
  if not leafRes.ok:
    return err[bool](leafRes.err.code, leafRes.err.message, leafRes.err.context)
  let leafId = leafRes.value
  
  let handleRes = tree.pager.acquirePageRo(leafId)
  if not handleRes.ok:
    return err[bool](handleRes.err.code, handleRes.err.message, handleRes.err.context)
  var handle = handleRes.value
  defer: handle.release()
  
  if handle.entry != nil:
     let idxRes = getOrBuildLeafIndex(handle.getPage, handle.entry)
     if not idxRes.ok:
       return err[bool](idxRes.err.code, idxRes.err.message, idxRes.err.context)
     let idx = idxRes.value
     let i = lowerBound(idx.keys, key)
     if i < idx.keys.len and idx.keys[i] == key:
        if idx.valueLens[i] == 0 and idx.overflows[i] == 0:
           return ok(false)
        return ok(true)
     return ok(false)

  let page = handle.getPage
  if page.len < 8:
    return err[bool](ERR_CORRUPTION, "Page too small")
  if byte(page[0]) != PageTypeLeaf:
    return err[bool](ERR_CORRUPTION, "Not a leaf page")
  let count = int(readU16LE(page, 2))
  var offset = 8
  
  for _ in 0 ..< count:
    if offset >= page.len:
      return err[bool](ERR_CORRUPTION, "Leaf cell out of bounds")
    
    var k: uint64
    if not decodeVarintFast(page, offset, k):
      return err[bool](ERR_CORRUPTION, "Invalid key varint")
    
    var ctrl: uint64
    if not decodeVarintFast(page, offset, ctrl):
      return err[bool](ERR_CORRUPTION, "Invalid control varint")
    
    let isOverflow = (ctrl and 1) != 0
    let val = uint32(ctrl shr 1)
    var valueLen = 0
    var overflow = 0'u32
    
    if isOverflow:
      overflow = val
    else:
      valueLen = int(val)
    
    if k == key:
      if valueLen == 0 and overflow == 0'u32:
        return ok(false)
      return ok(true)
    
    offset += valueLen
    if k > key:
      return ok(false)
  ok(false)

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

proc openCursorView*(tree: BTree): Result[BTreeCursorView] =
  var current = tree.root
  while true:
    var pageType: byte = 0
    var keys: seq[uint64] = @[]
    var valueOffsets: seq[int] = @[]
    var valueLens: seq[int] = @[]
    var overflows: seq[uint32] = @[]
    var nextLeaf: PageId = 0
    var leafPage: string = ""
    var children: seq[uint32] = @[]
    let pageRes = tree.pager.withPageRo(current, proc(page: string): Result[Void] =
      pageType = byte(page[0])
      if pageType == PageTypeLeaf:
        leafPage = page
        let parsed = readLeafCellsView(page)
        if not parsed.ok:
          return err[Void](parsed.err.code, parsed.err.message, parsed.err.context)
        (keys, valueOffsets, valueLens, overflows, nextLeaf) = parsed.value
        return okVoid()
      let internalRes = readInternalCells(page)
      if not internalRes.ok:
        return err[Void](internalRes.err.code, internalRes.err.message, internalRes.err.context)
      (_, children, _) = internalRes.value
      okVoid()
    )
    if not pageRes.ok:
      return err[BTreeCursorView](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    if pageType == PageTypeLeaf:
      let cursor = BTreeCursorView(
        tree: tree,
        leaf: current,
        index: 0,
        keys: keys,
        valueOffsets: valueOffsets,
        valueLens: valueLens,
        overflows: overflows,
        nextLeaf: nextLeaf,
        leafPage: leafPage
      )
      return ok(cursor)
    if children.len == 0:
      return err[BTreeCursorView](ERR_CORRUPTION, "Empty internal page")
    current = PageId(children[0])

proc cursorNextView*(cursor: BTreeCursorView): Result[(uint64, string, int, int, uint32)] =
  ## Returns (key, leafPage, valueOffset, valueLen, leafOverflowRoot)
  if cursor.leaf == 0:
    return err[(uint64, string, int, int, uint32)](ERR_IO, "Cursor exhausted")
  if cursor.index < cursor.keys.len:
    let i = cursor.index
    cursor.index.inc
    return ok((cursor.keys[i], cursor.leafPage, cursor.valueOffsets[i], cursor.valueLens[i], cursor.overflows[i]))
  if cursor.nextLeaf == 0:
    cursor.leaf = 0
    return err[(uint64, string, int, int, uint32)](ERR_IO, "Cursor exhausted")

  var keys: seq[uint64] = @[]
  var valueOffsets: seq[int] = @[]
  var valueLens: seq[int] = @[]
  var overflows: seq[uint32] = @[]
  var nextLeaf: PageId = 0
  var leafPage: string = ""
  let pageRes = cursor.tree.pager.withPageRo(cursor.nextLeaf, proc(page: string): Result[Void] =
    leafPage = page
    let parsed = readLeafCellsView(page)
    if not parsed.ok:
      return err[Void](parsed.err.code, parsed.err.message, parsed.err.context)
    (keys, valueOffsets, valueLens, overflows, nextLeaf) = parsed.value
    okVoid()
  )
  if not pageRes.ok:
    return err[(uint64, string, int, int, uint32)](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  cursor.leaf = cursor.nextLeaf
  cursor.keys = keys
  cursor.valueOffsets = valueOffsets
  cursor.valueLens = valueLens
  cursor.overflows = overflows
  cursor.nextLeaf = nextLeaf
  cursor.leafPage = leafPage
  cursor.index = 0
  cursorNextView(cursor)

proc openCursorStream*(tree: BTree): Result[BTreeCursorStream] =
  var current = tree.root
  while true:
    var pageType: byte = 0
    var children: seq[uint32] = @[]
    var leafPage: string = ""
    var count = 0
    var nextLeaf: PageId = 0
    let pageRes = tree.pager.withPageRo(current, proc(page: string): Result[Void] =
      if page.len < 8:
        return err[Void](ERR_CORRUPTION, "Page too small")
      pageType = byte(page[0])
      if pageType == PageTypeLeaf:
        leafPage = page
        count = int(readU16LE(page, 2))
        nextLeaf = PageId(readU32LE(page, 4))
        return okVoid()
      let internalRes = readInternalCells(page)
      if not internalRes.ok:
        return err[Void](internalRes.err.code, internalRes.err.message, internalRes.err.context)
      (_, children, _) = internalRes.value
      okVoid()
    )
    if not pageRes.ok:
      return err[BTreeCursorStream](pageRes.err.code, pageRes.err.message, pageRes.err.context)
    if pageType == PageTypeLeaf:
      return ok(BTreeCursorStream(
        tree: tree,
        leaf: current,
        nextLeaf: nextLeaf,
        leafPage: leafPage,
        offset: 8,
        remaining: count
      ))
    if children.len == 0:
      return err[BTreeCursorStream](ERR_CORRUPTION, "Empty internal page")
    current = PageId(children[0])

proc cursorNextStream*(cursor: BTreeCursorStream): Result[(uint64, string, int, int, uint32)] =
  ## Returns (key, leafPage, valueOffset, valueLen, leafOverflowRoot)
  if cursor.leaf == 0:
    return err[(uint64, string, int, int, uint32)](ERR_IO, "Cursor exhausted")

  if cursor.remaining <= 0:
    if cursor.nextLeaf == 0:
      cursor.leaf = 0
      return err[(uint64, string, int, int, uint32)](ERR_IO, "Cursor exhausted")

    var leafPage: string = ""
    var count = 0
    var nextLeaf: PageId = 0
    let pageRes = cursor.tree.pager.withPageRo(cursor.nextLeaf, proc(page: string): Result[Void] =
      if page.len < 8:
        return err[Void](ERR_CORRUPTION, "Page too small")
      if byte(page[0]) != PageTypeLeaf:
        return err[Void](ERR_CORRUPTION, "Not a leaf page")
      leafPage = page
      count = int(readU16LE(page, 2))
      nextLeaf = PageId(readU32LE(page, 4))
      okVoid()
    )
    if not pageRes.ok:
      return err[(uint64, string, int, int, uint32)](pageRes.err.code, pageRes.err.message, pageRes.err.context)

    cursor.leaf = cursor.nextLeaf
    cursor.nextLeaf = nextLeaf
    cursor.leafPage = leafPage
    cursor.offset = 8
    cursor.remaining = count
    return cursorNextStream(cursor)

  if cursor.offset >= cursor.leafPage.len:
    return err[(uint64, string, int, int, uint32)](ERR_CORRUPTION, "Leaf cell out of bounds")

  let keyRes = decodeVarint(cursor.leafPage, cursor.offset)
  if not keyRes.ok:
    return err[(uint64, string, int, int, uint32)](keyRes.err.code, keyRes.err.message, keyRes.err.context)
  let key = keyRes.value

  let ctrlRes = decodeVarint(cursor.leafPage, cursor.offset)
  if not ctrlRes.ok:
    return err[(uint64, string, int, int, uint32)](ctrlRes.err.code, ctrlRes.err.message, ctrlRes.err.context)
  let control = ctrlRes.value

  let isOverflow = (control and 1) != 0
  let val = uint32(control shr 1)

  var valueOffset = 0
  var valueLen = 0
  var overflow = 0'u32
  if isOverflow:
    overflow = val
    valueOffset = 0
    valueLen = 0
  else:
    valueLen = int(val)
    overflow = 0'u32
    if cursor.offset + valueLen > cursor.leafPage.len:
      return err[(uint64, string, int, int, uint32)](ERR_CORRUPTION, "Leaf value out of bounds")
    valueOffset = cursor.offset
    cursor.offset += valueLen

  cursor.remaining.dec
  ok((key, cursor.leafPage, valueOffset, valueLen, overflow))

proc cursorNext*(cursor: BTreeCursor): Result[(uint64, seq[byte], uint32)] =
  if cursor.leaf == 0:
    return err[(uint64, seq[byte], uint32)](ERR_IO, "Cursor exhausted")
  if cursor.index < cursor.keys.len:
    let i = cursor.index
    cursor.index.inc
    let valueRes = materializeValue(cursor.tree, cursor.values[i], cursor.overflows[i])
    if not valueRes.ok:
      return err[(uint64, seq[byte], uint32)](valueRes.err.code, valueRes.err.message, valueRes.err.context)
    return ok((cursor.keys[i], valueRes.value, cursor.overflows[i]))
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
    let keyBytes = encodeVarint(keys[i])
    var control: uint64 = 0
    let valueLen = values[i].len
    if overflows[i] != 0:
      control = (uint64(overflows[i]) shl 1) or 1
    else:
      control = (uint64(valueLen) shl 1)
    let ctrlBytes = encodeVarint(control)

    if offset + keyBytes.len + ctrlBytes.len + valueLen > pageSize:
      return err[string](ERR_IO, "Leaf overflow")
    
    for b in keyBytes:
      buf[offset] = char(b)
      offset.inc
    for b in ctrlBytes:
      buf[offset] = char(b)
      offset.inc
      
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
    let keyBytes = encodeVarint(keys[i])
    let childBytes = encodeVarint(uint64(children[i]))

    if offset + keyBytes.len + childBytes.len > pageSize:
      return err[string](ERR_IO, "Internal overflow")
    
    for b in keyBytes:
      buf[offset] = char(b)
      offset.inc
    for b in childBytes:
      buf[offset] = char(b)
      offset.inc
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
    let leafValRes = prepareLeafValue(tree, entry[1])
    if not leafValRes.ok:
      return err[PageId](leafValRes.err.code, leafValRes.err.message, leafValRes.err.context)
    let leafVal = leafValRes.value
    currentKeys.add(entry[0])
    currentVals.add(leafVal.inline)
    currentOv.add(leafVal.overflow)
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
    currentVals = @[leafVal.inline]
    currentOv = @[leafVal.overflow]
    leaves.add((id: currentLeaf, firstKey: entry[0]))

  let flushRes = flushLeaf(0)
  if not flushRes.ok:
    return err[PageId](flushRes.err.code, flushRes.err.message, flushRes.err.context)

  if leaves.len == 1:
    tree.root = leaves[0].id
    return ok(tree.root)

  proc maxInternalKeys(): int =
    (pageSize - 8) div 15

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

proc scanLeafLastKey(page: string): Result[(uint64, int, int)] =
  ## Returns (lastKey, usedBytes, count)
  if page.len < 8:
    return err[(uint64, int, int)](ERR_CORRUPTION, "Page too small")
  if byte(page[0]) != PageTypeLeaf:
    return err[(uint64, int, int)](ERR_CORRUPTION, "Not a leaf page")
  let count = int(readU16LE(page, 2))
  var offset = 8
  var lastKey: uint64 = 0

  if count == 0:
    return ok((0'u64, 8, 0))

  for i in 0 ..< count:
    if offset >= page.len:
      return err[(uint64, int, int)](ERR_CORRUPTION, "Leaf cell out of bounds")

    var val: uint64
    if not decodeVarintFast(page, offset, val):
       return err[(uint64, int, int)](ERR_CORRUPTION, "Invalid varint")
    lastKey = val

    var ctrl: uint64
    if not decodeVarintFast(page, offset, ctrl):
       return err[(uint64, int, int)](ERR_CORRUPTION, "Invalid varint")
    
    let isOverflow = (ctrl and 1) != 0
    let v = uint32(ctrl shr 1)
    if not isOverflow:
       offset += int(v)
  
  ok((lastKey, offset, count))

proc insertRecursive(tree: BTree, pageId: PageId, key: uint64, value: seq[byte], checkUnique: bool): Result[Option[SplitResult]] =
  let pageRes = readPageRo(tree.pager, pageId)
  if not pageRes.ok:
    return err[Option[SplitResult]](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  let page = pageRes.value
  if byte(page[0]) == PageTypeLeaf:
    let prepRes = prepareLeafValue(tree, value)
    if not prepRes.ok:
      return err[Option[SplitResult]](prepRes.err.code, prepRes.err.message, prepRes.err.context)
    let leafVal = prepRes.value.inline
    let leafOv = prepRes.value.overflow

    # Optimization: Fast path for append (sequential inserts)
    let scanRes = scanLeafLastKey(page)
    if scanRes.ok:
      let (lastKey, usedBytes, count) = scanRes.value
      if (count == 0 or key > lastKey):
         # stderr.writeLine("Fast append hit")
         let keyBytes = encodeVarint(key)
         var ctrlBytes: seq[byte]
         var valLen = 0
         if leafOv != 0:
            ctrlBytes = encodeVarint((uint64(leafOv) shl 1) or 1)
         else:
            valLen = leafVal.len
            ctrlBytes = encodeVarint(uint64(valLen) shl 1)
         
         if usedBytes + keyBytes.len + ctrlBytes.len + valLen <= tree.pager.pageSize:
             var newPage = page 
             let newCount = uint16(count + 1)
             newPage[2] = char(byte(newCount and 0xFF))
             newPage[3] = char(byte((newCount shr 8) and 0xFF))
             
             var writeOffset = usedBytes
             for b in keyBytes:
               newPage[writeOffset] = char(b)
               writeOffset.inc
             for b in ctrlBytes:
               newPage[writeOffset] = char(b)
               writeOffset.inc
             if valLen > 0:
               copyMem(addr newPage[writeOffset], unsafeAddr leafVal[0], valLen)
             
             discard writePage(tree.pager, pageId, newPage)
             return ok(none(SplitResult))

    let parsed = readLeafCells(page)
    if not parsed.ok:
      return err[Option[SplitResult]](parsed.err.code, parsed.err.message, parsed.err.context)
    var (keys, values, overflows, nextLeaf) = parsed.value
    var inserted = false
    for i in 0 ..< keys.len:
      if checkUnique and keys[i] == key:
        return err[Option[SplitResult]](ERR_CONSTRAINT, "Unique constraint violation", $key)
      if key < keys[i]:
        keys.insert(key, i)
        values.insert(leafVal, i)
        overflows.insert(leafOv, i)
        inserted = true
        break
    if not inserted:
      keys.add(key)
      values.add(leafVal)
      overflows.add(leafOv)
    let encodeRes = encodeLeaf(keys, values, overflows, nextLeaf, tree.pager.pageSize)
    if encodeRes.ok:
      discard writePage(tree.pager, pageId, encodeRes.value)
      return ok(none(SplitResult))

    let splitRes = chooseLeafSplitPoint(keys, values, overflows, tree.pager.pageSize)
    if not splitRes.ok:
      return err[Option[SplitResult]](splitRes.err.code, splitRes.err.message, splitRes.err.context)
    let mid = splitRes.value
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
  # Use binary search to find child page - O(log n) instead of O(n)
  # Finds first key > search key (same logic as findChildPage)
  var lo = 0
  var hi = keys.len
  while lo < hi:
    let mid = (lo + hi) shr 1
    if keys[mid] <= key:
      lo = mid + 1
    else:
      hi = mid
  let childIndex = lo
  let childPage = if childIndex < keys.len: PageId(children[childIndex]) else: PageId(rightChild)
  let splitRes = insertRecursive(tree, childPage, key, value, checkUnique)
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

proc insert*(tree: BTree, key: uint64, value: seq[byte], checkUnique: bool = false): Result[Void] =
  let splitRes = insertRecursive(tree, tree.root, key, value, checkUnique)
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
      let oldOv = overflows[i]
      let prepRes = prepareLeafValue(tree, value)
      if not prepRes.ok:
        return err[Void](prepRes.err.code, prepRes.err.message, prepRes.err.context)
      var leafVal = prepRes.value.inline
      var leafOv = prepRes.value.overflow
      values[i] = leafVal
      overflows[i] = leafOv
      var encodeRes = encodeLeaf(keys, values, overflows, nextLeaf, tree.pager.pageSize)
      if not encodeRes.ok and leafOv == 0'u32 and leafVal.len > 0:
        let ovRes = writeOverflowChain(tree.pager, leafVal)
        if not ovRes.ok:
          return err[Void](ovRes.err.code, ovRes.err.message, ovRes.err.context)
        values[i] = @[]
        overflows[i] = uint32(ovRes.value)
        encodeRes = encodeLeaf(keys, values, overflows, nextLeaf, tree.pager.pageSize)
      if not encodeRes.ok:
        return err[Void](encodeRes.err.code, encodeRes.err.message, encodeRes.err.context)
      discard writePage(tree.pager, pageId, encodeRes.value)
      if oldOv != 0'u32:
        let freeRes = freeOverflowChain(tree.pager, PageId(oldOv))
        if not freeRes.ok:
          return err[Void](freeRes.err.code, freeRes.err.message, freeRes.err.context)
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
      if overflows[i] != 0'u32:
        let freeRes = freeOverflowChain(tree.pager, PageId(overflows[i]))
        if not freeRes.ok:
          return err[Void](freeRes.err.code, freeRes.err.message, freeRes.err.context)
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
      let valueRes = materializeValue(tree, values[i], overflows[i])
      if not valueRes.ok:
        return err[bool](valueRes.err.code, valueRes.err.message, valueRes.err.context)
      if valueRes.value == value:
        if overflows[i] != 0'u32:
          let freeRes = freeOverflowChain(tree.pager, PageId(overflows[i]))
          if not freeRes.ok:
            return err[bool](freeRes.err.code, freeRes.err.message, freeRes.err.context)
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

# Page utilization monitoring for B+Tree space management (SPEC section 17.2)
proc calculatePageUtilization*(tree: BTree, pageId: PageId): Result[float] =
  ## Calculate utilization percentage for a single page
  ## Returns 0.0-100.0 representing percentage of page used
  let pageRes = readPageRo(tree.pager, pageId)
  if not pageRes.ok:
    return err[float](pageRes.err.code, pageRes.err.message, pageRes.err.context)
  
  let page = pageRes.value
  if page.len == 0:
    return ok(0.0)
  
  let pageType = byte(page[0])
  var usedBytes = 8  # Header: type(1) + padding(1) + count(2) + next/right(4)
  
  if pageType == PageTypeLeaf:
    let parsed = readLeafCells(page)
    if not parsed.ok:
      return err[float](parsed.err.code, parsed.err.message, parsed.err.context)
    let (keys, values, overflows, _) = parsed.value
    # Variable length headers
    for i in 0 ..< keys.len:
      let keyLen = encodeVarint(keys[i]).len
      var control: uint64 = 0
      if overflows[i] != 0:
        control = (uint64(overflows[i]) shl 1) or 1
      else:
        control = (uint64(values[i].len) shl 1)
      let ctrlLen = encodeVarint(control).len
      usedBytes += keyLen + ctrlLen + values[i].len
  elif pageType == PageTypeInternal:
    let parsed = readInternalCells(page)
    if not parsed.ok:
      return err[float](parsed.err.code, parsed.err.message, parsed.err.context)
    let (keys, children, _) = parsed.value
    # Variable length headers
    for i in 0 ..< keys.len:
      let keyLen = encodeVarint(keys[i]).len
      let childLen = encodeVarint(uint64(children[i])).len
      usedBytes += keyLen + childLen
  
  let utilization = (float(usedBytes) / float(page.len)) * 100.0
  ok(utilization)

proc calculateTreeUtilization*(tree: BTree): Result[float] =
  ## Calculate average utilization for entire B+Tree
  ## Traverses all pages and returns average utilization percentage
  var totalUtilization = 0.0
  var pageCount = 0
  var visitedPages: seq[PageId] = @[]
  var pagesToVisit: seq[PageId] = @[tree.root]
  
  while pagesToVisit.len > 0:
    let pageId = pagesToVisit.pop()
    if pageId in visitedPages:
      continue
    visitedPages.add(pageId)
    
    let utilRes = calculatePageUtilization(tree, pageId)
    if utilRes.ok:
      totalUtilization += utilRes.value
      pageCount += 1
    
    # Read page to find child pages
    let pageRes = readPageRo(tree.pager, pageId)
    if pageRes.ok:
      let page = pageRes.value
      if page.len > 0 and byte(page[0]) == PageTypeInternal:
        let internalRes = readInternalCells(page)
        if internalRes.ok:
          let (_, children, rightChild) = internalRes.value
          for child in children:
            if child != 0 and PageId(child) notin visitedPages:
              pagesToVisit.add(PageId(child))
          if rightChild != 0 and PageId(rightChild) notin visitedPages:
            pagesToVisit.add(PageId(rightChild))
  
  if pageCount == 0:
    return ok(0.0)
  ok(totalUtilization / float(pageCount))

proc needsCompaction*(tree: BTree, threshold: float = 50.0): Result[bool] =
  ## Check if B+Tree needs compaction based on utilization threshold
  ## Default threshold is 50% per SPEC section 17.2
  let utilRes = calculateTreeUtilization(tree)
  if not utilRes.ok:
    return err[bool](utilRes.err.code, utilRes.err.message, utilRes.err.context)
  ok(utilRes.value < threshold)
