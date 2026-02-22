import options
import tables
import locks
import ../errors
import ../pager/pager
import ../pager/db_header
import ../record/record
import sets

when defined(bench_breakdown):
  import std/monotimes
  import times
  import ../utils/bench_breakdown

const
  PageTypeInternal* = 1'u8
  PageTypeLeaf* = 2'u8
  MaxLeafInlineValueBytes = 512
  PageFlagDeltaKeys* = 0x01'u8  # byte[1] flag: keys are delta-encoded

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

type AppendCacheEntry = object
  pager: Pager
  leaf: PageId
  lastKey: uint64
  usedBytes: int
  count: int
  deltaEncoded: bool
  pinnedEntry: CacheEntry

var gAppendCache {.threadvar.}: Table[PageId, AppendCacheEntry]
# Last-accessed cache entry pointer for O(1) lookup when root doesn't change.
var gLastCacheRoot {.threadvar.}: PageId
var gLastCachePtr {.threadvar.}: ptr AppendCacheEntry

proc evictPagerFromAppendCache*(pager: Pager) =
  ## Remove all append-cache entries belonging to a specific pager.
  ## Call this when closing a database to prevent leaked Pager refs.
  if gAppendCache.len == 0:
    return
  var toDelete: seq[PageId]
  for root, entry in gAppendCache:
    if entry.pager == pager:
      toDelete.add(root)
  for root in toDelete:
    gAppendCache.del(root)
  gLastCachePtr = nil

proc encodeVarintToBuf*(v: uint64, buf: var array[10, byte]): int {.inline.} =
  ## Encode a varint into a stack-allocated buffer, returning the number of bytes written.
  var x = v
  var i = 0
  while x >= 0x80:
    buf[i] = byte(x and 0x7F) or 0x80
    x = x shr 7
    inc i
  buf[i] = byte(x)
  i + 1

proc invalidateAppendCache(tree: BTree) {.inline.} =
  if gAppendCache.len == 0:
    return
  if gAppendCache.hasKey(tree.root):
    let old = gAppendCache[tree.root]
    if old.pager != tree.pager:
      return
    if old.pinnedEntry != nil:
      discard unpinPage(tree.pager, old.pinnedEntry)
    gAppendCache.del(tree.root)
    if gLastCacheRoot == tree.root:
      gLastCachePtr = nil

proc updateAppendCache(tree: BTree, leaf: PageId, lastKey: uint64, usedBytes: int, count: int, deltaEncoded: bool, pinnedEntry: CacheEntry = nil) {.inline.} =
  if gAppendCache.len == 0:
    gAppendCache = initTable[PageId, AppendCacheEntry]()

  var keepPinned = pinnedEntry
  gAppendCache.withValue(tree.root, old):
    if old.pager != tree.pager:
      if old.pinnedEntry != nil:
        discard unpinPage(old.pager, old.pinnedEntry)
      old.pager = tree.pager
      old.leaf = leaf
      old.lastKey = lastKey
      old.usedBytes = usedBytes
      old.count = count
      old.deltaEncoded = deltaEncoded
      old.pinnedEntry = keepPinned
      return
    if keepPinned == nil and old.leaf == leaf:
      keepPinned = old.pinnedEntry
    elif old.pinnedEntry != nil and old.pinnedEntry != keepPinned:
      discard unpinPage(tree.pager, old.pinnedEntry)
    # Update in-place
    old.leaf = leaf
    old.lastKey = lastKey
    old.usedBytes = usedBytes
    old.count = count
    old.deltaEncoded = deltaEncoded
    old.pinnedEntry = keepPinned
    return
  do:
    gAppendCache[tree.root] = AppendCacheEntry(
      pager: tree.pager,
      leaf: leaf,
      lastKey: lastKey,
      usedBytes: usedBytes,
      count: count,
      deltaEncoded: deltaEncoded,
      pinnedEntry: keepPinned
    )
    # Table may have rehashed — update cached pointer
    gLastCachePtr = nil

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
  deltaEncoded*: bool
  prevKey*: uint64

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
  let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
  let count = int(readU16LE(page, 2))
  let nextLeaf = PageId(readU32LE(page, 4))
  var keys: seq[uint64] = @[]
  var values: seq[seq[byte]] = @[]
  var overflows: seq[uint32] = @[]
  var offset = 8

  var data = newSeq[byte](page.len)
  if page.len > 0:
    copyMem(addr data[0], unsafeAddr page[0], page.len)

  var prevKey: uint64 = 0
  for _ in 0 ..< count:
    if offset >= page.len:
      return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](ERR_CORRUPTION, "Leaf cell out of bounds")

    let keyRes = decodeVarint(data, offset)
    if not keyRes.ok:
      return err[(seq[uint64], seq[seq[byte]], seq[uint32], PageId)](keyRes.err.code, keyRes.err.message, keyRes.err.context)
    let key = if deltaEncoded: prevKey + keyRes.value else: keyRes.value
    prevKey = key

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
  let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
  let count = int(readU16LE(page, 2))
  let nextLeaf = PageId(readU32LE(page, 4))
  var keys = newSeq[uint64](count)
  var valueOffsets = newSeq[int](count)
  var valueLens = newSeq[int](count)
  var overflows = newSeq[uint32](count)
  var offset = 8

  var prevKey: uint64 = 0
  for i in 0 ..< count:
    if offset >= page.len:
      return err[(seq[uint64], seq[int], seq[int], seq[uint32], PageId)](ERR_CORRUPTION, "Leaf cell out of bounds")

    let keyRes = decodeVarint(page, offset)
    if not keyRes.ok:
      return err[(seq[uint64], seq[int], seq[int], seq[uint32], PageId)](keyRes.err.code, keyRes.err.message, keyRes.err.context)
    let key = if deltaEncoded: prevKey + keyRes.value else: keyRes.value
    prevKey = key
    keys[i] = key

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
    return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Not an internal page (type=" & $int(byte(page[0])) & ")")
  let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
  let count = int(readU16LE(page, 2))
  let rightChild = readU32LE(page, 4)
  var keys = newSeqOfCap[uint64](count)
  var children = newSeqOfCap[uint32](count)
  var offset = 8

  var prevKey: uint64 = 0
  for _ in 0 ..< count:
    if offset >= page.len:
      return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Internal cell out of bounds")

    var keyVal: uint64
    if not decodeVarintFast(page, offset, keyVal):
      return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Failed to decode key varint")
    let key = if deltaEncoded: prevKey + keyVal else: keyVal
    prevKey = key

    var childVal: uint64
    if not decodeVarintFast(page, offset, childVal):
      return err[(seq[uint64], seq[uint32], uint32)](ERR_CORRUPTION, "Failed to decode child varint")

    keys.add(key)
    children.add(uint32(childVal))
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
  
  let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
  let count = int(readU16LE(page, 2))
  let rightChild = readU32LE(page, 4)
  var keys = newSeq[uint64](count)
  var children = newSeq[uint32](count)
  var offset = 8
  
  var prevKey: uint64 = 0
  for i in 0 ..< count:
    if offset >= page.len:
      return err[InternalNodeIndex](ERR_CORRUPTION, "Internal cell out of bounds")
    
    var k: uint64
    if not decodeVarintFast(page, offset, k):
      return err[InternalNodeIndex](ERR_CORRUPTION, "Invalid key varint")
    if deltaEncoded:
      k = prevKey + k
      prevKey = k
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
    
  let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
  let count = int(readU16LE(page, 2))
  var keys = newSeq[uint64](count)
  var valueOffsets = newSeq[int](count)
  var valueLens = newSeq[int](count)
  var overflows = newSeq[uint32](count)
  var offset = 8
  
  var prevKey: uint64 = 0
  for i in 0 ..< count:
    if offset >= page.len:
      return err[LeafNodeIndex](ERR_CORRUPTION, "Leaf cell out of bounds")
    
    var k: uint64
    if not decodeVarintFast(page, offset, k):
      return err[LeafNodeIndex](ERR_CORRUPTION, "Invalid key varint")
    if deltaEncoded:
      k = prevKey + k
      prevKey = k
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
  
  let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
  let count = int(readU16LE(page, 2))
  let rightChild = readU32LE(page, 4)
  var offset = 8
  
  var prevKey: uint64 = 0
  for _ in 0 ..< count:
    if offset >= page.len:
      return err[uint32](ERR_CORRUPTION, "Internal cell out of bounds")
    
    let keyRes = decodeVarint(page, offset)
    if not keyRes.ok:
      return err[uint32](keyRes.err.code, keyRes.err.message, keyRes.err.context)
    let key = if deltaEncoded: prevKey + keyRes.value else: keyRes.value
    prevKey = key
    
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
  when defined(btree_trace):
    let doTrace = key == uint64(16137)
    if doTrace:
      echo "  findLeaf key=", key, " root=", current
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
      when defined(btree_trace):
        if doTrace and not isLeaf:
          echo "    findLeaf: page=", current, " type=internal count=", count, " -> nextPage=", nextPage
    finally:
      handle.release()

    when defined(btree_trace):
      if doTrace and isLeaf:
        echo "    findLeaf: page=", current, " type=leaf"

    if isLeaf:
      return ok(current)
    current = nextPage

# proc findChildPageLeftmost(keys: seq[uint64], children: seq[uint32], rightChild: uint32, key: uint64): uint32 =
#   ## Binary search to find the appropriate child page for a key (leftmost variant).
#   ## Finds the first key >= search key and returns corresponding children[index].
#   ## Returns rightChild if key > all keys.
#   var lo = 0
#   var hi = keys.len
#   while lo < hi:
#     let mid = (lo + hi) shr 1
#     if keys[mid] < key:
#       lo = mid + 1
#     else:
#       hi = mid
#   if lo < keys.len:
#     return children[lo]
#   return rightChild

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
      let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
      var keyVals: array[256, uint64]
      var childVals: array[256, uint32]
      
      var prevKey: uint64 = 0
      for i in 0 ..< count:
        # Decode key varint
        var k: uint64
        if not decodeVarintFast(page, offset, k):
          return err[Void](ERR_CORRUPTION, "Invalid key varint")
        if deltaEncoded:
          k = prevKey + k
          prevKey = k
        
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

  # Bias split toward keeping the left page fuller. This improves average page
  # utilization from ~50% to ~67% at the cost of slightly more frequent splits
  # on the right side.
  var splitAt = keys.len * 2 div 3
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
  
  when defined(btree_trace):
    if key == uint64(16137):
      echo "  find: leafId=", leafId
  
  # Optimized path: scan leaf directly without materializing all cells
  var foundKey: uint64 = 0
  var foundValue: seq[byte] = @[]
  var foundOverflow: uint32 = 0
  var found = false
  
  let pageRes = tree.pager.withPageRo(leafId, proc(page: string): Result[Void] =
    when defined(btree_trace):
      if key == uint64(16137):
        let pgCount = int(readU16LE(page, 2))
        echo "  find.body: page.len=", page.len, " count=", pgCount, " delta=", (byte(page[1]) == PageFlagDeltaKeys)
    if page.len < 8:
      return err[Void](ERR_CORRUPTION, "Page too small")
    if byte(page[0]) != PageTypeLeaf:
      return err[Void](ERR_CORRUPTION, "Not a leaf page")
    let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
    let count = int(readU16LE(page, 2))
    var offset = 8
    
    var prevKey: uint64 = 0
    for _ in 0 ..< count:
      if offset >= page.len:
        return err[Void](ERR_CORRUPTION, "Leaf cell out of bounds")
      
      # Decode key varint inline
      var k: uint64
      if not decodeVarintFast(page, offset, k):
        return err[Void](ERR_CORRUPTION, "Invalid key varint")
      if deltaEncoded:
        k = prevKey + k
        prevKey = k
      
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
  let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
  let count = int(readU16LE(page, 2))
  var offset = 8
  
  var prevKey: uint64 = 0
  for _ in 0 ..< count:
    if offset >= page.len:
      return err[bool](ERR_CORRUPTION, "Leaf cell out of bounds")
    
    var k: uint64
    if not decodeVarintFast(page, offset, k):
      return err[bool](ERR_CORRUPTION, "Invalid key varint")
    if deltaEncoded:
      k = prevKey + k
      prevKey = k
    
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
        remaining: count,
        deltaEncoded: byte(leafPage[1]) == PageFlagDeltaKeys,
        prevKey: 0
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
    cursor.deltaEncoded = byte(leafPage[1]) == PageFlagDeltaKeys
    cursor.prevKey = 0
    return cursorNextStream(cursor)

  if cursor.offset >= cursor.leafPage.len:
    return err[(uint64, string, int, int, uint32)](ERR_CORRUPTION, "Leaf cell out of bounds")

  let keyRes = decodeVarint(cursor.leafPage, cursor.offset)
  if not keyRes.ok:
    return err[(uint64, string, int, int, uint32)](keyRes.err.code, keyRes.err.message, keyRes.err.context)
  let key = if cursor.deltaEncoded: cursor.prevKey + keyRes.value else: keyRes.value
  cursor.prevKey = key

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
  buf[1] = char(PageFlagDeltaKeys)
  let count = uint16(keys.len)
  buf[2] = char(byte(count and 0xFF))
  buf[3] = char(byte((count shr 8) and 0xFF))
  writeU32LE(buf, 4, uint32(nextLeaf))
  var offset = 8
  var prevKey: uint64 = 0
  for i in 0 ..< keys.len:
    let delta = keys[i] - prevKey
    let keyBytes = encodeVarint(delta)
    prevKey = keys[i]
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
  buf[1] = char(PageFlagDeltaKeys)
  let count = uint16(keys.len)
  buf[2] = char(byte(count and 0xFF))
  buf[3] = char(byte((count shr 8) and 0xFF))
  writeU32LE(buf, 4, rightChild)
  var offset = 8
  var prevKey: uint64 = 0
  var vbuf: array[10, byte]
  for i in 0 ..< keys.len:
    let delta = keys[i] - prevKey
    prevKey = keys[i]
    let keyLen = encodeVarintToBuf(delta, vbuf)
    if offset + keyLen > pageSize:
      return err[string](ERR_IO, "Internal overflow")
    for j in 0 ..< keyLen:
      buf[offset] = char(vbuf[j])
      offset.inc
    let childLen = encodeVarintToBuf(uint64(children[i]), vbuf)
    if offset + childLen > pageSize:
      return err[string](ERR_IO, "Internal overflow")
    for j in 0 ..< childLen:
      buf[offset] = char(vbuf[j])
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

proc tryAppendToCachedRightmostLeaf(tree: BTree, key: uint64, value: openArray[byte]): Result[bool] =
  ## Fast path for sequential inserts.
  ## If we have an append-cache entry for the rightmost leaf and the new key is
  ## greater than the cached lastKey, attempt an in-place append directly on
  ## that leaf (no traversal through internal nodes).
  ##
  ## Returns ok(true) if append succeeded and no further work is needed.
  when defined(btree_no_append_cache):
    return ok(false)
  ## Returns ok(false) to fall back to the general insert path.
  if gAppendCache.len == 0:
    return ok(false)
  var cachedPtr: ptr AppendCacheEntry = nil
  # Fast path: reuse last-looked-up pointer if root matches (avoids hash lookup)
  if gLastCachePtr != nil and gLastCacheRoot == tree.root:
    cachedPtr = gLastCachePtr
  else:
    gAppendCache.withValue(tree.root, v):
      cachedPtr = addr v[]
    if cachedPtr != nil:
      gLastCacheRoot = tree.root
      gLastCachePtr = cachedPtr
  if cachedPtr == nil:
    return ok(false)

  if cachedPtr.pager != tree.pager or cachedPtr.leaf == 0 or key <= cachedPtr.lastKey:
    return ok(false)

  # Inline prepareLeafValue: avoid Result + seq copy for the common case.
  let maxInline = maxInlineValue(tree)
  var leafOv: uint32 = 0
  if value.len > maxInline:
    let ovRes = writeOverflowChain(tree.pager, value)
    if not ovRes.ok:
      return err[bool](ovRes.err.code, ovRes.err.message, ovRes.err.context)
    leafOv = uint32(ovRes.value)
  # For inline values, use `value` directly (borrowed, no copy).

  var entry = cachedPtr.pinnedEntry
  var pinnedNow = false
  if entry == nil:
    let pinRes = pinPage(tree.pager, cachedPtr.leaf)
    if not pinRes.ok:
      return ok(false)
    entry = pinRes.value
    pinnedNow = true

  acquire(entry.lock)
  var shouldUnpin = false
  var succeeded = false
  var newWriteOffset = cachedPtr.usedBytes
  var newCount = cachedPtr.count
  var isDelta = cachedPtr.deltaEncoded
  try:
    if entry.data.len < 8 or byte(entry.data[0]) != PageTypeLeaf:
      invalidateAppendCache(tree)
      shouldUnpin = pinnedNow
      return ok(false)

    if PageId(readU32LE(entry.data, 4)) != 0:
      invalidateAppendCache(tree)
      shouldUnpin = pinnedNow
      return ok(false)

    let pageCount = int(readU16LE(entry.data, 2))
    isDelta = byte(entry.data[1]) == PageFlagDeltaKeys
    if pageCount != cachedPtr.count or isDelta != cachedPtr.deltaEncoded:
      invalidateAppendCache(tree)
      shouldUnpin = pinnedNow
      return ok(false)

    let encodedKey = if isDelta: key - cachedPtr.lastKey else: key
    var keyBuf: array[10, byte]
    let keyLen = encodeVarintToBuf(encodedKey, keyBuf)
    var ctrlBuf: array[10, byte]
    var valLen = 0
    let ctrlLen =
      if leafOv != 0:
        encodeVarintToBuf((uint64(leafOv) shl 1) or 1'u64, ctrlBuf)
      else:
        valLen = value.len
        encodeVarintToBuf(uint64(valLen) shl 1, ctrlBuf)

    let usedBytes = cachedPtr.usedBytes
    let count = cachedPtr.count
    if usedBytes < 8 or count < 0 or usedBytes + keyLen + ctrlLen + valLen > tree.pager.pageSize:
      shouldUnpin = pinnedNow
      return ok(false)

    # Inline upgradeToRw: skip PageHandle creation and function call overhead.
    # The page is already dirty after the first write (common case).
    if not entry.dirty:
      entry.data = cloneString(entry.data)
      entry.dirty = true
      entry.aux = nil
      tree.pager.txnDirtyCount.inc
      tree.pager.txnDirtyPages.add(entry.id)
    tree.pager.txnLastDirtyId = entry.id

    when defined(btree_trace):
      if key == uint64(16137):
        echo "  tryAppend: entry.id=", entry.id, " cachedPtr.leaf=", cachedPtr.leaf
        echo "  tryAppend: entry.dirty=", entry.dirty, " entry.pinCount=", entry.pinCount
        echo "  tryAppend: usedBytes=", usedBytes, " count=", count, " encodedKey=", encodedKey
        echo "  tryAppend: entry.data.len=", entry.data.len, " pageCount_on_page=", int(readU16LE(entry.data, 2))

    newCount = count + 1
    let newCountU16 = uint16(newCount)
    entry.data[2] = char(byte(newCountU16 and 0xFF))
    entry.data[3] = char(byte((newCountU16 shr 8) and 0xFF))

    var writeOffset = usedBytes
    copyMem(addr entry.data[writeOffset], addr keyBuf[0], keyLen)
    writeOffset += keyLen
    copyMem(addr entry.data[writeOffset], addr ctrlBuf[0], ctrlLen)
    writeOffset += ctrlLen
    if valLen > 0:
      copyMem(addr entry.data[writeOffset], unsafeAddr value[0], valLen)
      writeOffset += valLen

    newWriteOffset = writeOffset
    succeeded = true
    when defined(btree_trace):
      if key == uint64(16137):
        echo "  tryAppend: WRITTEN count_on_page=", int(readU16LE(entry.data, 2)), " writeOffset=", writeOffset
  finally:
    release(entry.lock)
    if shouldUnpin:
      discard unpinPage(tree.pager, entry)

  if succeeded:
    # Update cache entry in-place via pointer (skip hash lookup in updateAppendCache)
    cachedPtr.lastKey = key
    cachedPtr.usedBytes = newWriteOffset
    cachedPtr.count = newCount
    cachedPtr.deltaEncoded = isDelta
    if cachedPtr.pinnedEntry != entry:
      if cachedPtr.pinnedEntry != nil and cachedPtr.pinnedEntry != entry:
        discard unpinPage(tree.pager, cachedPtr.pinnedEntry)
      cachedPtr.pinnedEntry = entry
    return ok(true)

  if pinnedNow:
    updateAppendCache(tree, cachedPtr.leaf, cachedPtr.lastKey, cachedPtr.usedBytes, cachedPtr.count, cachedPtr.deltaEncoded, pinnedEntry = nil)
  ok(false)

proc scanLeafLastKey(page: string): Result[(uint64, int, int)] =
  ## Returns (lastKey, usedBytes, count)
  if page.len < 8:
    return err[(uint64, int, int)](ERR_CORRUPTION, "Page too small")
  if byte(page[0]) != PageTypeLeaf:
    return err[(uint64, int, int)](ERR_CORRUPTION, "Not a leaf page")
  let deltaEncoded = byte(page[1]) == PageFlagDeltaKeys
  let count = int(readU16LE(page, 2))
  var offset = 8
  var lastKey: uint64 = 0

  if count == 0:
    return ok((0'u64, 8, 0))

  var prevKey: uint64 = 0
  for i in 0 ..< count:
    if offset >= page.len:
      return err[(uint64, int, int)](ERR_CORRUPTION, "Leaf cell out of bounds")

    var val: uint64
    if not decodeVarintFast(page, offset, val):
       return err[(uint64, int, int)](ERR_CORRUPTION, "Invalid varint")
    if deltaEncoded:
      val = prevKey + val
      prevKey = val
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
  when defined(btree_trace):
    const traceKey = 16137
    if key == uint64(16137):
      echo "  insertRecursive key=", key, " pageId=", pageId
  var haveLeafValue = false
  var leafVal: seq[byte] = @[]
  var leafOv: uint32 = 0'u32

  # If this is the cached rightmost leaf, try a pinned-entry append before we
  # acquire any handle (avoids per-insert pin/unpin on sequential inserts).
  if gAppendCache.len > 0 and gAppendCache.hasKey(tree.root):
    let cached = gAppendCache[tree.root]
    when defined(btree_trace):
      if key == uint64(16137):
        echo "  cache1 check: cached.leaf=", cached.leaf, " pageId=", pageId, " cached.lastKey=", cached.lastKey, " match=", (cached.pager == tree.pager and cached.leaf == pageId and key > cached.lastKey)
    if cached.pager == tree.pager and cached.leaf == pageId and key > cached.lastKey:
      let prepRes = prepareLeafValue(tree, value)
      if not prepRes.ok:
        return err[Option[SplitResult]](prepRes.err.code, prepRes.err.message, prepRes.err.context)
      leafVal = prepRes.value.inline
      leafOv = prepRes.value.overflow
      haveLeafValue = true

      var entry = cached.pinnedEntry
      var pinnedNow = false
      if entry == nil:
        let pin2 = pinPage(tree.pager, pageId)
        if pin2.ok:
          entry = pin2.value
          pinnedNow = true

      if entry != nil:
        acquire(entry.lock)
        defer: release(entry.lock)

        let pageCount = int(readU16LE(entry.data, 2))
        let isDelta = byte(entry.data[1]) == PageFlagDeltaKeys
        if pageCount != cached.count or isDelta != cached.deltaEncoded:
          if pinnedNow:
            discard unpinPage(tree.pager, entry)
          invalidateAppendCache(tree)
        else:

          let encodedKey = if isDelta: key - cached.lastKey else: key
          var keyBuf: array[10, byte]
          let keyLen = encodeVarintToBuf(encodedKey, keyBuf)
          var ctrlBuf: array[10, byte]
          var valLen = 0
          let ctrlLen =
            if leafOv != 0:
              encodeVarintToBuf((uint64(leafOv) shl 1) or 1'u64, ctrlBuf)
            else:
              valLen = leafVal.len
              encodeVarintToBuf(uint64(valLen) shl 1, ctrlBuf)

          let usedBytes = cached.usedBytes
          let count = cached.count
          if usedBytes >= 8 and count >= 0 and usedBytes + keyLen + ctrlLen + valLen <= tree.pager.pageSize:
            var tmp = PageHandle(pager: tree.pager, entry: entry, pinned: true)
            let upRes = upgradeToRw(tmp)
            if upRes.ok:
              when defined(btree_trace):
                if key == uint64(16137):
                  echo "  cache1 APPEND OK: usedBytes=", usedBytes, " count=", count
              let newCount = uint16(count + 1)
              entry.data[2] = char(byte(newCount and 0xFF))
              entry.data[3] = char(byte((newCount shr 8) and 0xFF))

              var writeOffset = usedBytes
              for i in 0 ..< keyLen:
                entry.data[writeOffset] = char(keyBuf[i])
                inc writeOffset
              for i in 0 ..< ctrlLen:
                entry.data[writeOffset] = char(ctrlBuf[i])
                inc writeOffset
              if valLen > 0:
                copyMem(addr entry.data[writeOffset], unsafeAddr leafVal[0], valLen)
                writeOffset += valLen

              updateAppendCache(tree, pageId, key, writeOffset, count + 1, isDelta, pinnedEntry = entry)
              return ok(none(SplitResult))

          # Append not possible; if we pinned a new entry for this attempt, do not keep it pinned.
          if pinnedNow:
            discard unpinPage(tree.pager, entry)
            updateAppendCache(tree, cached.leaf, cached.lastKey, cached.usedBytes, cached.count, cached.deltaEncoded, pinnedEntry = nil)

  let hRes = acquirePageRo(tree.pager, pageId)
  if not hRes.ok:
    return err[Option[SplitResult]](hRes.err.code, hRes.err.message, hRes.err.context)
  var h = hRes.value
  defer: release(h)

  let page =
    if h.entry != nil:
      h.entry.data
    else:
      h.data

  if byte(page[0]) == PageTypeLeaf:
    proc encodeVarintToBuf(v: uint64, buf: var array[10, byte]): int {.inline.} =
      var x = v
      var i = 0
      while true:
        var b = byte(x and 0x7F'u64)
        x = x shr 7
        if x != 0:
          b = b or 0x80'u8
        buf[i] = b
        inc i
        if x == 0:
          break
      i

    if not haveLeafValue:
      let prepRes = prepareLeafValue(tree, value)
      if not prepRes.ok:
        return err[Option[SplitResult]](prepRes.err.code, prepRes.err.message, prepRes.err.context)
      leafVal = prepRes.value.inline
      leafOv = prepRes.value.overflow
      haveLeafValue = true

    # Optimization: Fast path for append (sequential inserts).
    # Avoid O(cells) scanning of the leaf by caching the last-key and used-bytes
    # for the rightmost leaf per tree root.
    if gAppendCache.len > 0 and gAppendCache.hasKey(tree.root):
      let cached = gAppendCache[tree.root]
      if cached.pager == tree.pager and cached.leaf == pageId and key > cached.lastKey:
        when defined(btree_trace):
          if key == uint64(16137):
            echo "  cache2 match: cached.leaf=", cached.leaf, " nextLeaf=", PageId(readU32LE(page, 4))
        let nextLeaf = PageId(readU32LE(page, 4))
        let pageCount = int(readU16LE(page, 2))
        if pageCount != cached.count:
          invalidateAppendCache(tree)
        else:
          let isDelta = byte(page[1]) == PageFlagDeltaKeys
          if isDelta == cached.deltaEncoded:
            let encodedKey = if isDelta: key - cached.lastKey else: key
            var keyBuf: array[10, byte]
            let keyLen = encodeVarintToBuf(encodedKey, keyBuf)
            var ctrlBuf: array[10, byte]
            var valLen = 0
            let ctrlLen =
              if leafOv != 0:
                encodeVarintToBuf((uint64(leafOv) shl 1) or 1'u64, ctrlBuf)
              else:
                valLen = leafVal.len
                encodeVarintToBuf(uint64(valLen) shl 1, ctrlBuf)

            let usedBytes = cached.usedBytes
            let count = cached.count
            if usedBytes >= 8 and count >= 0 and usedBytes + keyLen + ctrlLen + valLen <= tree.pager.pageSize:
              when defined(btree_trace):
                if key == uint64(16137):
                  echo "  cache2 APPEND: usedBytes=", usedBytes, " count=", count
              if h.entry == nil or not h.pinned:
                return err[Option[SplitResult]](ERR_INTERNAL, "Leaf page handle not cached", "page_id=" & $pageId)
              let upRes = upgradeToRw(h)
              if not upRes.ok:
                return err[Option[SplitResult]](upRes.err.code, upRes.err.message, upRes.err.context)

              let newCount = uint16(count + 1)
              h.entry.data[2] = char(byte(newCount and 0xFF))
              h.entry.data[3] = char(byte((newCount shr 8) and 0xFF))

              var writeOffset = usedBytes
              for i in 0 ..< keyLen:
                h.entry.data[writeOffset] = char(keyBuf[i])
                inc writeOffset
              for i in 0 ..< ctrlLen:
                h.entry.data[writeOffset] = char(ctrlBuf[i])
                inc writeOffset
              if valLen > 0:
                copyMem(addr h.entry.data[writeOffset], unsafeAddr leafVal[0], valLen)
                writeOffset += valLen

              updateAppendCache(tree, pageId, key, writeOffset, count + 1, isDelta)
              return ok(none(SplitResult))
            elif nextLeaf == 0 and usedBytes >= 8 and count >= 0:
              when defined(btree_trace):
                if key == uint64(16137):
                  echo "  cache2 FAST SPLIT: usedBytes=", usedBytes, " count=", count
              # Rightmost leaf split on append: avoid decoding/re-encoding.
              # Old page remains intact; we create a new right leaf with only the new key/value,
              # and set old.nextLeaf -> newPage.
              if h.entry == nil or not h.pinned:
                return err[Option[SplitResult]](ERR_INTERNAL, "Leaf page handle not cached", "page_id=" & $pageId)
              let upRes = upgradeToRw(h)
              if not upRes.ok:
                return err[Option[SplitResult]](upRes.err.code, upRes.err.message, upRes.err.context)

              let newRes = allocatePage(tree.pager)
              if not newRes.ok:
                return err[Option[SplitResult]](newRes.err.code, newRes.err.message, newRes.err.context)
              let newPage = newRes.value

              # For the new leaf, the first key must be stored as an absolute
              # value (not relative to the old page's last key).
              var newKeyBuf: array[10, byte]
              let newKeyLen = encodeVarintToBuf(key, newKeyBuf)

              var buf = newString(tree.pager.pageSize)
              buf[0] = char(PageTypeLeaf)
              buf[1] = if isDelta: char(PageFlagDeltaKeys) else: char(0)
              let newCountU16 = 1'u16
              buf[2] = char(byte(newCountU16 and 0xFF))
              buf[3] = char(byte((newCountU16 shr 8) and 0xFF))
              writeU32LE(buf, 4, 0)
              var writeOffset = 8
              for i in 0 ..< newKeyLen:
                buf[writeOffset] = char(newKeyBuf[i])
                inc writeOffset
              for i in 0 ..< ctrlLen:
                buf[writeOffset] = char(ctrlBuf[i])
                inc writeOffset
              if valLen > 0:
                copyMem(addr buf[writeOffset], unsafeAddr leafVal[0], valLen)
                writeOffset += valLen

              let writeRes = writeNewPage(tree.pager, newPage, buf)
              if not writeRes.ok:
                return err[Option[SplitResult]](writeRes.err.code, writeRes.err.message, writeRes.err.context)

              # Link old -> new
              writeU32LE(h.entry.data, 4, uint32(newPage))

              # New leaf becomes the rightmost append target.
              updateAppendCache(tree, newPage, key, writeOffset, 1, isDelta)
              when defined(btree_debug):
                echo "FAST SPLIT: key=", key, " oldPage=", pageId, " newPage=", newPage, " isDelta=", isDelta
              return ok(some(SplitResult(promoted: key, newPage: newPage)))

    # Fallback: scan the leaf to find the last key and append position.
    let scanRes = scanLeafLastKey(page)
    when defined(btree_trace):
      if key == uint64(16137):
        if scanRes.ok:
          let (lk, ub, ct) = scanRes.value
          echo "  scanLeaf: lastKey=", lk, " usedBytes=", ub, " count=", ct
        else:
          echo "  scanLeaf: FAILED"
    if scanRes.ok:
      let (lastKey, usedBytes, count) = scanRes.value
      if (count == 0 or key > lastKey):
        let isDelta = byte(page[1]) == PageFlagDeltaKeys
        # Delta-encode the key relative to the last key when using delta format
        let encodedKey = if isDelta: key - lastKey else: key
        var keyBuf: array[10, byte]
        let keyLen = encodeVarintToBuf(encodedKey, keyBuf)
        var ctrlBuf: array[10, byte]
        var valLen = 0
        let ctrlLen =
          if leafOv != 0:
            encodeVarintToBuf((uint64(leafOv) shl 1) or 1'u64, ctrlBuf)
          else:
            valLen = leafVal.len
            encodeVarintToBuf(uint64(valLen) shl 1, ctrlBuf)

        if usedBytes + keyLen + ctrlLen + valLen <= tree.pager.pageSize:
          if h.entry == nil or not h.pinned:
            return err[Option[SplitResult]](ERR_INTERNAL, "Leaf page handle not cached", "page_id=" & $pageId)
          let upRes = upgradeToRw(h)
          if not upRes.ok:
            return err[Option[SplitResult]](upRes.err.code, upRes.err.message, upRes.err.context)

          let newCount = uint16(count + 1)
          h.entry.data[2] = char(byte(newCount and 0xFF))
          h.entry.data[3] = char(byte((newCount shr 8) and 0xFF))

          var writeOffset = usedBytes
          for i in 0 ..< keyLen:
            h.entry.data[writeOffset] = char(keyBuf[i])
            inc writeOffset
          for i in 0 ..< ctrlLen:
            h.entry.data[writeOffset] = char(ctrlBuf[i])
            inc writeOffset
          if valLen > 0:
            copyMem(addr h.entry.data[writeOffset], unsafeAddr leafVal[0], valLen)
            writeOffset += valLen

          updateAppendCache(tree, pageId, key, writeOffset, count + 1, isDelta)
          return ok(none(SplitResult))

    # Not an append. Release the pinned handle before doing the slow path, which
    # may call writePage() on this same page and would deadlock if we still hold
    # entry.lock.
    release(h)

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
    invalidateAppendCache(tree)
    let encodeRes = encodeLeaf(keys, values, overflows, nextLeaf, tree.pager.pageSize)
    if encodeRes.ok:
      discard writePage(tree.pager, pageId, encodeRes.value)
      # Re-encoding invalidates the append cache. If this was an append to the
      # rightmost leaf, re-establish the cache for the new page image.
      if key == keys[^1] and nextLeaf == 0:
        let scan2 = scanLeafLastKey(encodeRes.value)
        if scan2.ok:
          let (lastKey2, usedBytes2, count2) = scan2.value
          updateAppendCache(tree, pageId, lastKey2, usedBytes2, count2, byte(encodeRes.value[1]) == PageFlagDeltaKeys)
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
    discard writeNewPage(tree.pager, newPage, rightBufRes.value)
    let leftBufRes = encodeLeaf(leftKeys, leftVals, leftOv, newPage, tree.pager.pageSize)
    if not leftBufRes.ok:
      return err[Option[SplitResult]](leftBufRes.err.code, leftBufRes.err.message, leftBufRes.err.context)
    discard writePage(tree.pager, pageId, leftBufRes.value)
    # After a leaf split, sequential inserts will continue on the rightmost leaf.
    invalidateAppendCache(tree)
    if nextLeaf == 0:
      let scan3 = scanLeafLastKey(rightBufRes.value)
      if scan3.ok:
        let (lastKey3, usedBytes3, count3) = scan3.value
        updateAppendCache(tree, newPage, lastKey3, usedBytes3, count3, byte(rightBufRes.value[1]) == PageFlagDeltaKeys)
    return ok(some(SplitResult(promoted: rightKeys[0], newPage: newPage)))

  # Internal page: release the handle before descending.
  release(h)

  # Fast path for rightmost append: if key > last key in internal node,
  # descend to rightChild directly and try in-place append on split.
  let internalCount = int(readU16LE(page, 2))
  let internalRightChild = readU32LE(page, 4)

  if internalCount > 0 and byte(page[1]) == PageFlagDeltaKeys:
    # Scan forward to find last key and end offset for potential in-place append
    var scanOff = 8
    var prevK: uint64 = 0
    var scanOk = true
    for _ in 0 ..< internalCount:
      var kv, cv: uint64
      if not decodeVarintFast(page, scanOff, kv) or not decodeVarintFast(page, scanOff, cv):
        scanOk = false
        break
      prevK += kv
    if scanOk and key > prevK:
      # Key goes to rightChild — descend directly without full decode.
      let childPage = PageId(internalRightChild)
      let splitRes = insertRecursive(tree, childPage, key, value, checkUnique)
      if not splitRes.ok:
        return err[Option[SplitResult]](splitRes.err.code, splitRes.err.message, splitRes.err.context)
      if splitRes.value.isNone:
        return ok(none(SplitResult))
      let split = splitRes.value.get
      # Try in-place append: add promoted key + old rightChild at scanOff,
      # update rightChild to split.newPage, increment count.
      let delta = split.promoted - prevK
      var kbuf: array[10, byte]
      let klen = encodeVarintToBuf(delta, kbuf)
      var cbuf: array[10, byte]
      let clen = encodeVarintToBuf(uint64(childPage), cbuf)
      if scanOff + klen + clen <= tree.pager.pageSize:
        let hRes = acquirePageRw(tree.pager, pageId)
        if hRes.ok:
          var hw = hRes.value
          for j in 0 ..< klen:
            hw.entry.data[scanOff + j] = char(kbuf[j])
          for j in 0 ..< clen:
            hw.entry.data[scanOff + klen + j] = char(cbuf[j])
          let newCount = uint16(internalCount + 1)
          hw.entry.data[2] = char(byte(newCount and 0xFF))
          hw.entry.data[3] = char(byte((newCount shr 8) and 0xFF))
          writeU32LE(hw.entry.data, 4, uint32(split.newPage))
          release(hw)
          when defined(btree_debug):
            echo "INTERNAL FAST APPEND: promoted=", split.promoted, " page=", pageId, " count=", newCount
          return ok(none(SplitResult))
      # Doesn't fit — internal node needs to split. Read current page, add the
      # new child, then fall through to general internal-split logic below.
      let curPage = readPageRo(tree.pager, pageId)
      if not curPage.ok:
        return err[Option[SplitResult]](curPage.err.code, curPage.err.message, curPage.err.context)
      let intRes2 = readInternalCells(curPage.value)
      if not intRes2.ok:
        return err[Option[SplitResult]](intRes2.err.code, intRes2.err.message, intRes2.err.context)
      var (keys, children, rightChild) = intRes2.value
      keys.add(split.promoted)
      children.add(uint32(childPage))
      rightChild = uint32(split.newPage)
      var mid = keys.len * 2 div 3
      block validateSplit3:
        let lk = keys[0 ..< mid]
        let lc = children[0 ..< mid]
        let lrc = if mid < children.len: children[mid] else: rightChild
        let tryL = encodeInternal(lk, lc, lrc, tree.pager.pageSize)
        if tryL.ok:
          break validateSplit3
        mid = keys.len div 2
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
      discard writeNewPage(tree.pager, newPage, rightBufRes.value)
      when defined(btree_debug):
        echo "INTERNAL SPLIT (fast path): promoted=", promoted, " page=", pageId, " newPage=", newPage
      return ok(some(SplitResult(promoted: promoted, newPage: newPage)))

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
  when defined(btree_trace):
    if key == uint64(16137):
      echo "  internal descent: pageId=", pageId, " childIndex=", childIndex, " childPage=", childPage, " keys.len=", keys.len, " rightChild=", rightChild
      if childIndex > 0:
        echo "    keys[", childIndex-1, "]=", keys[childIndex-1]
      if childIndex < keys.len:
        echo "    keys[", childIndex, "]=", keys[childIndex]
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
    when defined(btree_debug):
      echo "INTERNAL ADD: promoted=", split.promoted, " page=", pageId, " keys=", keys.len, " childIndex=", childIndex
    return ok(none(SplitResult))
  var mid = keys.len * 2 div 3
  # Validate both halves fit; fall back to even split if needed
  block validateSplit:
    let leftKeys2 = keys[0 ..< mid]
    let leftChildren2 = children[0 ..< mid]
    let leftRC = if mid < children.len: children[mid] else: rightChild
    let tryLeft = encodeInternal(leftKeys2, leftChildren2, leftRC, tree.pager.pageSize)
    if not tryLeft.ok:
      mid = keys.len div 2
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
  discard writeNewPage(tree.pager, newPage, rightBufRes.value)
  when defined(btree_debug):
    echo "INTERNAL SPLIT: promoted=", promoted, " page=", pageId, " newPage=", newPage, " keys=", keys.len
  ok(some(SplitResult(promoted: promoted, newPage: newPage)))

proc insert*(tree: BTree, key: uint64, value: seq[byte], checkUnique: bool = false): Result[Void] =
  when defined(bench_breakdown):
    let t0 = getMonoTime()
    defer:
      if result.ok:
        addBtreeTotalNs(int64(inNanoseconds(getMonoTime() - t0)))

  when defined(btree_trace):
    const traceKey = 16137
    if key == uint64(16137):
      echo "TRACE INSERT key=", key, " root=", tree.root
      if gAppendCache.len > 0 and gAppendCache.hasKey(tree.root):
        let c = gAppendCache[tree.root]
        echo "  cache: leaf=", c.leaf, " lastKey=", c.lastKey, " usedBytes=", c.usedBytes, " count=", c.count, " delta=", c.deltaEncoded, " pinned=", (c.pinnedEntry != nil)

  let fastRes = tryAppendToCachedRightmostLeaf(tree, key, value)
  if not fastRes.ok:
    return err[Void](fastRes.err.code, fastRes.err.message, fastRes.err.context)
  if fastRes.value:
    when defined(btree_trace):
      if key == uint64(16137):
        echo "  -> fast append succeeded"
    return okVoid()

  let oldRoot = tree.root
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
    discard writeNewPage(tree.pager, newRoot, bufRes.value)
    when defined(btree_debug):
      echo "ROOT SPLIT: promoted=", split.promoted, " oldRoot=", oldRoot, " newRoot=", newRoot, " newPage=", split.newPage
    tree.root = newRoot
    # Preserve append cache across a root split.
    if gAppendCache.len > 0 and gAppendCache.hasKey(oldRoot):
      gAppendCache[newRoot] = gAppendCache[oldRoot]
      gAppendCache.del(oldRoot)
  okVoid()

proc update*(tree: BTree, key: uint64, value: seq[byte]): Result[Void] =
  invalidateAppendCache(tree)
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
  invalidateAppendCache(tree)
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

proc findMaxKey*(tree: BTree): Result[uint64] =
  ## Finds the maximum key in the B-Tree. Returns 0 if tree is empty.
  var current = tree.root
  while true:
    var nextPage: PageId = 0
    var isLeaf = false
    var maxKey: uint64 = 0
    
    let pageRes = tree.pager.withPageRo(current, proc(page: string): Result[Void] =
      if page.len < 1:
        return err[Void](ERR_CORRUPTION, "Page empty")
      let pageType = byte(page[0])
      if pageType == PageTypeLeaf:
        isLeaf = true
        let scanRes = scanLeafLastKey(page)
        if not scanRes.ok:
           return err[Void](scanRes.err.code, scanRes.err.message, scanRes.err.context)
        maxKey = scanRes.value[0]
        return okVoid()
      
      if pageType != PageTypeInternal:
         return err[Void](ERR_CORRUPTION, "Invalid page type " & $pageType)

      if page.len < 8:
         return err[Void](ERR_CORRUPTION, "Internal page too small")

      # Internal Page: right child is at offset 4
      nextPage = PageId(readU32LE(page, 4))
      okVoid()
    )
    if not pageRes.ok:
      return err[uint64](pageRes.err.code, pageRes.err.message, pageRes.err.context)
      
    if isLeaf:
      return ok(maxKey)
    current = nextPage
