import options
import tables
import algorithm
import strutils
import ../errors
import ../record/record
import ../pager/pager
import ../pager/db_header
import ../btree/btree
import ../catalog/catalog
import ../search/search
import ../sql/sql
import sets

when defined(bench_breakdown):
  import std/monotimes
  import times
  import ../utils/bench_breakdown

type StoredRow* = object
  rowid*: uint64
  values*: seq[Value]

proc initTableRoot*(pager: Pager): Result[PageId] =
  let rootRes = allocatePage(pager)
  if not rootRes.ok:
    return err[PageId](rootRes.err.code, rootRes.err.message, rootRes.err.context)
  let root = rootRes.value
  var buf = newString(pager.pageSize)
  buf[0] = char(PageTypeLeaf)
  buf[1] = char(PageFlagDeltaKeys)
  writeU32LE(buf, 4, 0)
  let writeRes = writePage(pager, root, buf)
  if not writeRes.ok:
    return err[PageId](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  ok(root)

proc encodeRowId(rowid: uint64): seq[byte] =
  var buf = newSeq[byte](8)
  writeU64LE(buf, 0, rowid)
  buf

proc sortPreservingPrefix*(data: openArray[byte]): uint64 =
  ## Encode up to the first 8 bytes of `data` as a big-endian uint64.
  ## This preserves lexicographic sort order for the prefix.
  var r: uint64 = 0
  let n = min(data.len, 8)
  for i in 0 ..< n:
    r = r or (uint64(data[i]) shl (56 - i * 8))
  r

proc indexKeyFromValue*(value: Value): uint64 =
  case value.kind
  of vkInt64:
    cast[uint64](value.int64Val)
  of vkBool:
    if value.boolVal: 1'u64 else: 0'u64
  of vkFloat64:
    cast[uint64](value.float64Val)
  of vkText, vkBlob:
    sortPreservingPrefix(value.bytes)
  of vkTextOverflow, vkBlobOverflow:
    0'u64
  else:
    0'u64

proc isTextBlobIndex*(table: TableMeta, idx: IndexMeta): bool =
  ## Returns true if this is a single-column index on a TEXT or BLOB column.
  if idx.columns.len != 1:
    return false
  for col in table.columns:
    if col.name == idx.columns[0]:
      return col.kind in {ctText, ctBlob}
  false

proc encodeIndexEntry*(rowid: uint64, value: Value, isTextBlob: bool): seq[byte] =
  ## Encode an index entry. For TEXT/BLOB indexes, embeds the value bytes
  ## alongside the rowid so post-verification reads are unnecessary.
  if isTextBlob and value.kind in {vkText, vkBlob}:
    let vlen = value.bytes.len
    var buf = encodeVarint(uint64(vlen))
    if vlen > 0:
      let oldLen = buf.len
      buf.setLen(oldLen + vlen)
      copyMem(addr buf[oldLen], unsafeAddr value.bytes[0], vlen)
    let oldLen = buf.len
    buf.setLen(oldLen + 8)
    writeU64LE(buf, oldLen, rowid)
    buf
  else:
    encodeRowId(rowid)

proc decodeIndexEntry*(data: openArray[byte], isTextBlob: bool): Result[tuple[rowid: uint64, valueBytes: seq[byte]]] =
  ## Decode an index entry. For TEXT/BLOB indexes, extracts the embedded value.
  if isTextBlob:
    var offset = 0
    let vlenRes = decodeVarint(data, offset)
    if not vlenRes.ok:
      return err[tuple[rowid: uint64, valueBytes: seq[byte]]](vlenRes.err.code, vlenRes.err.message, vlenRes.err.context)
    let vlen = int(vlenRes.value)
    if offset + vlen + 8 > data.len:
      return err[tuple[rowid: uint64, valueBytes: seq[byte]]](ERR_CORRUPTION, "Index entry too short for TEXT/BLOB")
    var vb = newSeq[byte](vlen)
    if vlen > 0:
      copyMem(addr vb[0], unsafeAddr data[offset], vlen)
    let rowid = readU64LE(data, offset + vlen)
    ok((rowid: rowid, valueBytes: vb))
  else:
    if data.len < 8:
      return err[tuple[rowid: uint64, valueBytes: seq[byte]]](ERR_CORRUPTION, "Index rowid payload too short")
    ok((rowid: readU64LE(data, 0), valueBytes: newSeq[byte]()))

proc compositeIndexKey*(values: seq[Value], columnIndices: seq[int]): uint64 =
  ## Hash multiple column values into a single uint64 key for composite indexes.
  if columnIndices.len == 1:
    return indexKeyFromValue(values[columnIndices[0]])
  var buf: seq[byte] = @[]
  for ci in columnIndices:
    let k = indexKeyFromValue(values[ci])
    buf.add(byte(k and 0xFF))
    buf.add(byte((k shr 8) and 0xFF))
    buf.add(byte((k shr 16) and 0xFF))
    buf.add(byte((k shr 24) and 0xFF))
    buf.add(byte((k shr 32) and 0xFF))
    buf.add(byte((k shr 40) and 0xFF))
    buf.add(byte((k shr 48) and 0xFF))
    buf.add(byte((k shr 56) and 0xFF))
  uint64(crc32c(buf))

proc indexColumnIndices*(table: TableMeta, idx: IndexMeta): seq[int] =
  ## Map index column names to column indices in the table.
  result = @[]
  for colName in idx.columns:
    if colName.startsWith(IndexExpressionPrefix):
      continue
    for i, col in table.columns:
      if col.name == colName:
        result.add(i)
        break

proc isExpressionIndexToken(token: string): bool =
  token.startsWith(IndexExpressionPrefix)

proc expressionIndexSql(token: string): string =
  if token.len <= IndexExpressionPrefix.len:
    return ""
  token[IndexExpressionPrefix.len .. ^1]

proc valueToIndexText(value: Value): string =
  case value.kind
  of vkNull:
    "NULL"
  of vkBool:
    if value.boolVal: "true" else: "false"
  of vkInt64:
    $value.int64Val
  of vkFloat64:
    $value.float64Val
  of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
    var s = ""
    for b in value.bytes:
      s.add(char(b))
    s
  else:
    ""

proc castExpressionValue(value: Value, targetKind: ColumnType): Result[Value] =
  if value.kind == vkNull:
    return ok(Value(kind: vkNull))

  case targetKind
  of ctInt64:
    case value.kind
    of vkInt64:
      ok(value)
    of vkFloat64:
      ok(Value(kind: vkInt64, int64Val: int64(value.float64Val)))
    of vkBool:
      ok(Value(kind: vkInt64, int64Val: (if value.boolVal: 1 else: 0)))
    of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
      let s = valueToIndexText(value).strip()
      try:
        ok(Value(kind: vkInt64, int64Val: int64(parseBiggestInt(s))))
      except ValueError:
        err[Value](ERR_SQL, "Invalid expression index cast value", s)
    else:
      err[Value](ERR_SQL, "Unsupported expression index cast source")
  of ctFloat64:
    case value.kind
    of vkFloat64:
      ok(value)
    of vkInt64:
      ok(Value(kind: vkFloat64, float64Val: float64(value.int64Val)))
    of vkBool:
      ok(Value(kind: vkFloat64, float64Val: (if value.boolVal: 1.0 else: 0.0)))
    of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
      let s = valueToIndexText(value).strip()
      try:
        ok(Value(kind: vkFloat64, float64Val: parseFloat(s)))
      except ValueError:
        err[Value](ERR_SQL, "Invalid expression index cast value", s)
    else:
      err[Value](ERR_SQL, "Unsupported expression index cast source")
  of ctText:
    var bytes: seq[byte] = @[]
    for ch in valueToIndexText(value):
      bytes.add(byte(ch))
    ok(Value(kind: vkText, bytes: bytes))
  of ctBool:
    case value.kind
    of vkBool:
      ok(value)
    of vkInt64:
      ok(Value(kind: vkBool, boolVal: value.int64Val != 0))
    of vkFloat64:
      ok(Value(kind: vkBool, boolVal: value.float64Val != 0.0))
    of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
      let s = valueToIndexText(value).strip().toLowerAscii()
      if s in ["true", "t", "1"]:
        return ok(Value(kind: vkBool, boolVal: true))
      if s in ["false", "f", "0"]:
        return ok(Value(kind: vkBool, boolVal: false))
      err[Value](ERR_SQL, "Invalid expression index cast value", s)
    else:
      err[Value](ERR_SQL, "Unsupported expression index cast source")
  else:
    err[Value](ERR_SQL, "Unsupported expression index cast target")

proc evalExpressionIndexExpr(table: TableMeta, values: seq[Value], expr: Expr): Result[Value] =
  if expr == nil:
    return err[Value](ERR_SQL, "Expression index expression is missing")
  case expr.kind
  of ekColumn:
    for i, col in table.columns:
      if col.name == expr.name:
        if i < values.len:
          return ok(values[i])
        return err[Value](ERR_CORRUPTION, "Row column count does not match table metadata", table.name)
    err[Value](ERR_SQL, "Expression index column not found", expr.name)
  of ekFunc:
    let fn = expr.funcName.toUpperAscii()
    if fn in ["LOWER", "UPPER", "TRIM", "LENGTH"]:
      if expr.args.len != 1:
        return err[Value](ERR_SQL, "Expression index function arity mismatch", fn)
      let argRes = evalExpressionIndexExpr(table, values, expr.args[0])
      if not argRes.ok:
        return argRes
      if argRes.value.kind == vkNull:
        return ok(Value(kind: vkNull))
      let textVal = valueToIndexText(argRes.value)
      if fn == "LENGTH":
        return ok(Value(kind: vkInt64, int64Val: int64(textVal.len)))
      let mapped =
        if fn == "LOWER":
          textVal.toLowerAscii()
        elif fn == "UPPER":
          textVal.toUpperAscii()
        else:
          textVal.strip()
      var outBytes: seq[byte] = @[]
      for ch in mapped:
        outBytes.add(byte(ch))
      return ok(Value(kind: vkText, bytes: outBytes))
    if fn == "CAST":
      if expr.args.len != 2 or expr.args[1] == nil or expr.args[1].kind != ekLiteral or expr.args[1].value.kind != svString:
        return err[Value](ERR_SQL, "Expression index CAST requires explicit target type")
      let argRes = evalExpressionIndexExpr(table, values, expr.args[0])
      if not argRes.ok:
        return argRes
      let targetRes = parseColumnType(expr.args[1].value.strVal)
      if not targetRes.ok:
        return err[Value](targetRes.err.code, targetRes.err.message, targetRes.err.context)
      if targetRes.value.kind notin {ctInt64, ctFloat64, ctText, ctBool}:
        return err[Value](ERR_SQL, "Expression index CAST target not supported in 0.x", expr.args[1].value.strVal)
      return castExpressionValue(argRes.value, targetRes.value.kind)
    err[Value](ERR_SQL, "Unsupported expression index function", fn)
  else:
    err[Value](ERR_SQL, "Unsupported expression index expression")

proc indexKeyForRow(table: TableMeta, idx: IndexMeta, values: seq[Value]): Result[uint64] =
  if idx.columns.len == 1 and isExpressionIndexToken(idx.columns[0]):
    let exprSql = expressionIndexSql(idx.columns[0])
    let parseRes = parseSql("SELECT " & exprSql & " FROM " & table.name)
    if not parseRes.ok:
      return err[uint64](parseRes.err.code, "Invalid expression index expression: " & parseRes.err.message, parseRes.err.context)
    if parseRes.value.statements.len != 1 or parseRes.value.statements[0].kind != skSelect:
      return err[uint64](ERR_SQL, "Invalid expression index expression", exprSql)
    let selectStmt = parseRes.value.statements[0]
    if selectStmt.selectItems.len != 1 or selectStmt.selectItems[0].isStar or selectStmt.selectItems[0].expr == nil:
      return err[uint64](ERR_SQL, "Expression index requires a single expression", exprSql)
    let evalRes = evalExpressionIndexExpr(table, values, selectStmt.selectItems[0].expr)
    if not evalRes.ok:
      return err[uint64](evalRes.err.code, evalRes.err.message, evalRes.err.context)
    return ok(indexKeyFromValue(evalRes.value))

  let colIndices = indexColumnIndices(table, idx)
  if colIndices.len != idx.columns.len or colIndices.len == 0:
    return err[uint64](ERR_SQL, "Invalid index column mapping", idx.name)
  ok(compositeIndexKey(values, colIndices))

proc shouldIncludeInIndex(table: TableMeta, idx: IndexMeta, values: seq[Value]): bool =
  ## v0 partial-index support: only `<indexed_column> IS NOT NULL`.
  if idx.predicateSql.len == 0:
    return true
  if idx.columns.len != 1:
    return false
  var ci = -1
  for i, col in table.columns:
    if col.name == idx.columns[0]:
      ci = i
      break
  if ci < 0 or ci >= values.len:
    return false
  values[ci].kind != vkNull

proc valueText(value: Value): string =
  var s = ""
  for b in value.bytes:
    s.add(char(b))
  s

proc uniqueTrigrams(text: string): seq[uint32] =
  let grams = trigrams(text)
  var seen = initHashSet[uint32]()
  for g in grams:
    if g notin seen:
      seen.incl(g)
      result.add(g)

const
  PostingsChunkThreshold = 400  # bytes; keep below B+Tree inline limit (512)

proc trigramChunkKey(trigram: uint32, chunkId: uint16): uint64 =
  (uint64(trigram) shl 16) or uint64(chunkId)

proc loadPostingsChunked(tree: BTree, trigram: uint32): Result[seq[byte]] =
  ## Load all postings chunks for a trigram and concatenate.
  var result_bytes: seq[byte] = @[]
  var chunkId: uint16 = 0
  while true:
    let key = trigramChunkKey(trigram, chunkId)
    let findRes = find(tree, key)
    if not findRes.ok:
      if findRes.err.code == ERR_IO:
        break
      return err[seq[byte]](findRes.err.code, findRes.err.message, findRes.err.context)
    result_bytes.add(findRes.value[1])
    chunkId.inc
    if chunkId == 0: break  # overflow protection
  ok(result_bytes)

proc loadPostings(tree: BTree, trigram: uint32): Result[seq[byte]] =
  # Try new chunked format first (chunk 0)
  let key0 = trigramChunkKey(trigram, 0)
  let find0 = find(tree, key0)
  if find0.ok:
    return loadPostingsChunked(tree, trigram)
  if find0.err.code != ERR_IO:
    return err[seq[byte]](find0.err.code, find0.err.message, find0.err.context)
  # Fall back to legacy single-key format
  let key = uint64(trigram)
  let findRes = find(tree, key)
  if findRes.ok:
    return ok(findRes.value[1])
  if findRes.err.code == ERR_IO:
    return ok(newSeq[byte]())
  err[seq[byte]](findRes.err.code, findRes.err.message, findRes.err.context)

proc deleteAllChunks(tree: BTree, trigram: uint32): Result[Void] =
  ## Delete all postings chunks for a trigram.
  var chunkId: uint16 = 0
  while true:
    let key = trigramChunkKey(trigram, chunkId)
    let delRes = delete(tree, key)
    if not delRes.ok:
      if delRes.err.code == ERR_IO:
        break
      return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
    chunkId.inc
    if chunkId == 0: break
  # Also delete legacy key if present
  let legacyKey = uint64(trigram)
  let legacyDel = delete(tree, legacyKey)
  if not legacyDel.ok and legacyDel.err.code != ERR_IO:
    return err[Void](legacyDel.err.code, legacyDel.err.message, legacyDel.err.context)
  okVoid()

proc storePostings(tree: BTree, trigram: uint32, data: seq[byte]): Result[Void] =
  if data.len == 0:
    return deleteAllChunks(tree, trigram)
  # Delete legacy key if present
  discard delete(tree, uint64(trigram))
  # Split data into chunks
  var offset = 0
  var chunkId: uint16 = 0
  while offset < data.len:
    let chunkEnd = min(offset + PostingsChunkThreshold, data.len)
    # Ensure we don't split in the middle of a varint: extend to next varint boundary
    var end_pos = chunkEnd
    if end_pos < data.len:
      # Find a varint boundary: scan forward until we hit a byte < 0x80 (varint terminator)
      while end_pos < data.len and end_pos > offset:
        if byte(data[end_pos - 1]) < 0x80:
          break
        end_pos.inc
      if end_pos >= data.len:
        end_pos = data.len
    let chunk = data[offset ..< end_pos]
    let key = trigramChunkKey(trigram, chunkId)
    let findRes = find(tree, key)
    if findRes.ok:
      let updateRes = update(tree, key, chunk)
      if not updateRes.ok:
        return err[Void](updateRes.err.code, updateRes.err.message, updateRes.err.context)
    else:
      let insertRes = insert(tree, key, chunk)
      if not insertRes.ok:
        return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
    offset = end_pos
    chunkId.inc
  # Delete any old chunks beyond the new count
  while true:
    let key = trigramChunkKey(trigram, chunkId)
    let delRes = delete(tree, key)
    if not delRes.ok:
      break
    chunkId.inc
    if chunkId == 0: break
  okVoid()

proc getTrigramPostings*(pager: Pager, index: IndexMeta, trigram: uint32): Result[seq[uint64]] =
  let idxTree = newBTree(pager, index.rootPage)
  let bytesRes = loadPostings(idxTree, trigram)
  if not bytesRes.ok:
    return err[seq[uint64]](bytesRes.err.code, bytesRes.err.message, bytesRes.err.context)
  if bytesRes.value.len == 0:
    return ok(newSeq[uint64]())
  let decoded = decodePostings(bytesRes.value)
  if not decoded.ok:
    return err[seq[uint64]](decoded.err.code, decoded.err.message, decoded.err.context)
  ok(decoded.value)

proc syncIndexRoot(catalog: Catalog, indexName: string, tree: BTree): Result[Void]

proc setToSortedSeq(setVal: HashSet[uint64]): seq[uint64] =
  for v in setVal.items:
    result.add(v)
  result.sort()

proc applyPostingDeltas(base: seq[uint64], adds: HashSet[uint64], removes: HashSet[uint64]): seq[uint64] =
  let addList = setToSortedSeq(adds)
  let removeList = setToSortedSeq(removes)
  var filtered: seq[uint64] = @[]
  var i = 0
  var j = 0
  while i < base.len:
    let v = base[i]
    while j < removeList.len and removeList[j] < v:
      j.inc
    if j < removeList.len and removeList[j] == v:
      i.inc
      continue
    filtered.add(v)
    i.inc
  var merged: seq[uint64] = @[]
  i = 0
  j = 0
  while i < filtered.len or j < addList.len:
    if j >= addList.len:
      merged.add(filtered[i])
      i.inc
    elif i >= filtered.len:
      merged.add(addList[j])
      j.inc
    else:
      let a = filtered[i]
      let b = addList[j]
      if a == b:
        merged.add(a)
        i.inc
        j.inc
      elif a < b:
        merged.add(a)
        i.inc
      else:
        merged.add(b)
        j.inc
  merged

proc getTrigramPostingsWithDeltas*(pager: Pager, catalog: Catalog, index: IndexMeta, trigram: uint32): Result[seq[uint64]] =
  let baseRes = getTrigramPostings(pager, index, trigram)
  if not baseRes.ok:
    return err[seq[uint64]](baseRes.err.code, baseRes.err.message, baseRes.err.context)
  let deltaOpt = catalog.trigramDelta(index.name, trigram)
  if deltaOpt.isNone:
    return ok(baseRes.value)
  let delta = deltaOpt.get
  ok(applyPostingDeltas(baseRes.value, delta.adds, delta.removes))

proc getTrigramPostingsWithDeltasUpTo*(pager: Pager, catalog: Catalog, index: IndexMeta, trigram: uint32, limit: int): Result[tuple[ids: seq[uint64], truncated: bool]] =
  ## Like getTrigramPostingsWithDeltas, but avoids unbounded allocations.
  ##
  ## If the postings list (after applying deltas) would exceed `limit`, returns
  ## truncated=true and does not guarantee ids contains all matches.
  let idxTree = newBTree(pager, index.rootPage)
  let bytesRes = loadPostings(idxTree, trigram)
  if not bytesRes.ok:
    return err[tuple[ids: seq[uint64], truncated: bool]](bytesRes.err.code, bytesRes.err.message, bytesRes.err.context)
  var baseIds: seq[uint64] = @[]
  if bytesRes.value.len > 0:
    let baseRes = decodePostingsUpTo(bytesRes.value, limit)
    if not baseRes.ok:
      return err[tuple[ids: seq[uint64], truncated: bool]](baseRes.err.code, baseRes.err.message, baseRes.err.context)
    if baseRes.value.truncated:
      return ok((ids: baseRes.value.ids, truncated: true))
    baseIds = baseRes.value.ids

  let deltaOpt = catalog.trigramDelta(index.name, trigram)
  if deltaOpt.isNone:
    return ok((ids: baseIds, truncated: false))

  let delta = deltaOpt.get
  let merged = applyPostingDeltas(baseIds, delta.adds, delta.removes)
  if limit > 0 and merged.len > limit:
    return ok((ids: merged[0 ..< limit], truncated: true))
  ok((ids: merged, truncated: false))

proc updateTrigramIndex(pager: Pager, catalog: Catalog, index: IndexMeta, rowid: uint64, oldValue: Value, newValue: Value): Result[Void] =
  let oldText = if oldValue.kind == vkText: valueText(oldValue) else: ""
  let newText = if newValue.kind == vkText: valueText(newValue) else: ""
  let oldTrigrams = uniqueTrigrams(oldText)
  let newTrigrams = uniqueTrigrams(newText)
  var oldSet = initHashSet[uint32]()
  for g in oldTrigrams: oldSet.incl(g)
  var newSet = initHashSet[uint32]()
  for g in newTrigrams: newSet.incl(g)
  for g in oldSet:
    if g notin newSet:
      catalog.trigramBufferRemove(index.name, g, rowid)
  for g in newSet:
    if g notin oldSet:
      catalog.trigramBufferAdd(index.name, g, rowid)
  okVoid()

proc flushTrigramDeltas*(pager: Pager, catalog: Catalog, clear: bool = true): Result[Void] =
  let all = catalog.allTrigramDeltas()
  if all.len == 0:
    return okVoid()
  var byIndex: Table[string, seq[(uint32, TrigramDelta)]] = initTable[string, seq[(uint32, TrigramDelta)]]()
  for entry in all:
    let indexName = entry[0][0]
    let trigram = entry[0][1]
    if not byIndex.hasKey(indexName):
      byIndex[indexName] = @[]
    byIndex[indexName].add((trigram, entry[1]))

  for indexName, ops in byIndex.mpairs:
    let idxOpt = catalog.getIndexByName(indexName)
    if isNone(idxOpt):
      continue
    let idx = idxOpt.get
    if idx.kind != ikTrigram:
      continue
    let idxTree = newBTree(pager, idx.rootPage)
    for op in ops:
      let trigram = op[0]
      let delta = op[1]
      let postingsRes = loadPostings(idxTree, trigram)
      if not postingsRes.ok:
        return err[Void](postingsRes.err.code, postingsRes.err.message, postingsRes.err.context)
      var baseIds: seq[uint64] = @[]
      if postingsRes.value.len > 0:
        let decoded = decodePostings(postingsRes.value)
        if not decoded.ok:
          return err[Void](decoded.err.code, decoded.err.message, decoded.err.context)
        baseIds = decoded.value
      let merged = applyPostingDeltas(baseIds, delta.adds, delta.removes)
      let updated = encodePostingsSorted(merged)
      let storeRes = storePostings(idxTree, trigram, updated)
      if not storeRes.ok:
        return storeRes
    let syncRes = syncIndexRoot(catalog, indexName, idxTree)
    if not syncRes.ok:
      return syncRes

  if clear:
    catalog.clearTrigramDeltas()
  okVoid()

proc syncIndexRoot(catalog: Catalog, indexName: string, tree: BTree): Result[Void] =
  if not catalog.indexes.hasKey(indexName):
    return okVoid()
  var meta = catalog.indexes[indexName]
  if meta.rootPage != tree.root:
    meta.rootPage = tree.root
    let saveRes = saveIndexMeta(catalog, meta)
    if not saveRes.ok:
      return err[Void](saveRes.err.code, saveRes.err.message, saveRes.err.context)
  okVoid()

proc normalizeValues*(pager: Pager, values: seq[Value]): Result[seq[Value]] =
  # Fast path: check if any value could possibly need normalization.
  # Values need normalization only if they are TEXT/BLOB with len > 128 (compression threshold).
  let overflowThreshold = pager.pageSize - 128
  var needsWork = false
  for value in values:
    if value.kind in {vkText, vkBlob} and value.bytes.len > 128:
      needsWork = true
      break
  if not needsWork:
    return ok(values)

  var resultValues: seq[Value] = @[]
  var changed = false
  for i, value in values:
    var processedValue = value
    if value.kind in {vkText, vkBlob}:
      processedValue = compressValue(value)

    if processedValue.kind in {vkText, vkBlob, vkTextCompressed, vkBlobCompressed} and processedValue.bytes.len > overflowThreshold:
      let pageRes = writeOverflowChain(pager, processedValue.bytes)
      if not pageRes.ok:
        return err[seq[Value]](pageRes.err.code, pageRes.err.message, pageRes.err.context)

      let overflowKind = case processedValue.kind
        of vkText: vkTextOverflow
        of vkBlob: vkBlobOverflow
        of vkTextCompressed: vkTextCompressedOverflow
        of vkBlobCompressed: vkBlobCompressedOverflow
        else: vkTextOverflow

      if not changed:
        changed = true
        resultValues = newSeq[Value](values.len)
        for j in 0 ..< i:
          resultValues[j] = values[j]
      resultValues[i] = Value(kind: overflowKind, overflowPage: pageRes.value, overflowLen: uint32(processedValue.bytes.len))
    else:
      if not changed and processedValue.kind != value.kind:
        changed = true
        resultValues = newSeq[Value](values.len)
        for j in 0 ..< i:
          resultValues[j] = values[j]
      if changed:
        resultValues[i] = processedValue
  if changed:
    ok(resultValues)
  else:
    ok(values)

proc readRowAt*(pager: Pager, table: TableMeta, rowid: uint64): Result[StoredRow] =
  let tree = newBTree(pager, table.rootPage)
  let findRes = find(tree, rowid)
  if not findRes.ok:
    return err[StoredRow](findRes.err.code, findRes.err.message, findRes.err.context)
  let payload = findRes.value[1]
  let decodeRes = decodeRecordWithOverflow(pager, payload)
  if not decodeRes.ok:
    return err[StoredRow](decodeRes.err.code, decodeRes.err.message, decodeRes.err.context)
  
  ok(StoredRow(rowid: rowid, values: decodeRes.value))

proc scanTable*(pager: Pager, table: TableMeta): Result[seq[StoredRow]] =
  let tree = newBTree(pager, table.rootPage)
  let cursorRes = openCursor(tree)
  if not cursorRes.ok:
    return err[seq[StoredRow]](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  let cursor = cursorRes.value
  var rows: seq[StoredRow] = @[]
  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      break
    let valueBytes = nextRes.value[1]
    let overflow = nextRes.value[2]
    if valueBytes.len == 0 and overflow == 0'u32:
      continue
    let decoded = decodeRecordWithOverflow(pager, valueBytes)
    if not decoded.ok:
      return err[seq[StoredRow]](decoded.err.code, decoded.err.message, decoded.err.context)
    rows.add(StoredRow(rowid: nextRes.value[0], values: decoded.value))
  ok(rows)

proc scanTableEach*(pager: Pager, table: TableMeta, body: proc(row: StoredRow): Result[Void]): Result[Void] =
  ## Iterate all rows in a table without materializing them into memory.
  let tree = newBTree(pager, table.rootPage)
  let cursorRes = openCursor(tree)
  if not cursorRes.ok:
    return err[Void](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  let cursor = cursorRes.value
  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      break
    let valueBytes = nextRes.value[1]
    let overflow = nextRes.value[2]
    if valueBytes.len == 0 and overflow == 0'u32:
      continue
    let decoded = decodeRecordWithOverflow(pager, valueBytes)
    if not decoded.ok:
      return err[Void](decoded.err.code, decoded.err.message, decoded.err.context)
    let cbRes = body(StoredRow(rowid: nextRes.value[0], values: decoded.value))
    if not cbRes.ok:
      return cbRes
  okVoid()

proc insertRowInternal(pager: Pager, catalog: Catalog, tableName: string, values: seq[Value], updateIndexes: bool, int64PkIdx: int = -2): Result[uint64] =
  when defined(bench_breakdown):
    let storStart = getMonoTime()
    var nsNormalize: int64 = 0
    var nsEncode: int64 = 0
    var nsBtree: int64 = 0
    var nsMeta: int64 = 0
    defer:
      if result.ok:
        let storTotal = int64(inNanoseconds(getMonoTime() - storStart))
        addStorageTotalNs(storTotal)
        addStorageNormalizeNs(nsNormalize)
        addStorageEncodeRecordNs(nsEncode)
        addStorageBtreeInsertNs(nsBtree)
        addStorageTableMetaNs(nsMeta)

  # Use pointer into catalog hash table to avoid copying TableMeta.
  # Safe because we hold the writer lock and don't rehash catalog.tables.
  let tablePtr = catalog.getTablePtr(tableName)
  if tablePtr == nil:
    return err[uint64](ERR_SQL, "Table not found", tableName)
  if values.len != tablePtr.columns.len:
    return err[uint64](ERR_SQL, "Column count mismatch", tableName)
  let origRootPage = tablePtr.rootPage
  
  var rowid: uint64 = 0
  var isExplicitRowId = false
  
  # INT64 PK optimization: use precomputed hint when available
  if int64PkIdx >= 0:
    # Fast path: single INT64 PK at known column index
    if values[int64PkIdx].kind == vkInt64:
      rowid = cast[uint64](values[int64PkIdx].int64Val)
      isExplicitRowId = true
  elif int64PkIdx == -2:
    # No hint: detect PK columns dynamically
    var pkCount = 0
    for col in tablePtr.columns:
      if col.primaryKey:
        inc pkCount
    if pkCount <= 1:
      for i, col in tablePtr.columns:
        if col.primaryKey and col.kind == ctInt64:
          if values[i].kind == vkInt64:
            rowid = cast[uint64](values[i].int64Val)
            isExplicitRowId = true
          break
  # int64PkIdx == -1: no single INT64 PK, skip detection
  
  if not isExplicitRowId:
    rowid = if tablePtr.nextRowId == 0: 1'u64 else: tablePtr.nextRowId

  # Back-fill auto-assigned rowid into PK column so it's stored with the row.
  # Avoid copying the values seq when no backfill is needed (explicit rowid case).
  var storedValues: seq[Value]
  if isExplicitRowId:
    storedValues = values  # Nim ARC: move if last use, shallow copy otherwise
  else:
    storedValues = values
    if int64PkIdx >= 0:
      storedValues[int64PkIdx] = Value(kind: vkInt64, int64Val: cast[int64](rowid))
    elif int64PkIdx == -2:
      var pkCount = 0
      for col in tablePtr.columns:
        if col.primaryKey:
          inc pkCount
      if pkCount <= 1:
        for i, col in tablePtr.columns:
          if col.primaryKey and col.kind == ctInt64:
            storedValues[i] = Value(kind: vkInt64, int64Val: cast[int64](rowid))
            break

  var indexKeys: seq[(IndexMeta, uint64, Value)]
  var trigramValues: seq[(IndexMeta, Value)]
  if updateIndexes:
    for _, idx in catalog.indexes:
      if idx.table != tableName:
        continue
      if not shouldIncludeInIndex(tablePtr[], idx, storedValues):
        continue
      if idx.kind == ikBtree:
        let keyRes = indexKeyForRow(tablePtr[], idx, storedValues)
        if not keyRes.ok:
          return err[uint64](keyRes.err.code, keyRes.err.message, keyRes.err.context)
        # Identify the value for TEXT/BLOB single-column indexes
        var idxValue = Value(kind: vkNull)
        if isTextBlobIndex(tablePtr[], idx) and idx.columns.len == 1:
          for i, col in tablePtr.columns:
            if col.name == idx.columns[0]:
              idxValue = storedValues[i]
              break
        indexKeys.add((idx, keyRes.value, idxValue))
      else:
        # Trigram indexes are always single-column
        if idx.columns.len == 1:
          var valueIndex = -1
          for i, col in tablePtr.columns:
            if col.name == idx.columns[0]:
              valueIndex = i
              break
          if valueIndex >= 0:
            trigramValues.add((idx, storedValues[valueIndex]))
  var normalizedRes: Result[seq[Value]]
  when defined(bench_breakdown):
    let t0 = getMonoTime()
    normalizedRes = normalizeValues(pager, storedValues)
    nsNormalize = int64(inNanoseconds(getMonoTime() - t0))
  else:
    normalizedRes = normalizeValues(pager, storedValues)
  if not normalizedRes.ok:
    return err[uint64](normalizedRes.err.code, normalizedRes.err.message, normalizedRes.err.context)
  var record: seq[byte]
  when defined(bench_breakdown):
    let t1 = getMonoTime()
    record = encodeRecord(normalizedRes.value)
    nsEncode = int64(inNanoseconds(getMonoTime() - t1))
  else:
    record = encodeRecord(normalizedRes.value)

  let tree = newBTree(pager, tablePtr.rootPage)
  var insertRes: Result[Void]
  when defined(bench_breakdown):
    let t2 = getMonoTime()
    insertRes = insert(tree, rowid, record, checkUnique = true)
    nsBtree = int64(inNanoseconds(getMonoTime() - t2))
  else:
    insertRes = insert(tree, rowid, record, checkUnique = true)
  if not insertRes.ok:
    return err[uint64](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  if updateIndexes:
    for entry in indexKeys:
      let idxTree = newBTree(pager, entry[0].rootPage)
      let textBlob = isTextBlobIndex(tablePtr[], entry[0])
      let idxInsert = insert(idxTree, entry[1], encodeIndexEntry(rowid, entry[2], textBlob))
      if not idxInsert.ok:
        return err[uint64](idxInsert.err.code, idxInsert.err.message, idxInsert.err.context)
      let syncRes = syncIndexRoot(catalog, entry[0].name, idxTree)
      if not syncRes.ok:
        return err[uint64](syncRes.err.code, syncRes.err.message, syncRes.err.context)
    for entry in trigramValues:
      if entry[1].kind != vkText:
        continue
      let grams = uniqueTrigrams(valueText(entry[1]))
      for g in grams:
        catalog.trigramBufferAdd(entry[0].name, g, rowid)
   
  when defined(bench_breakdown):
    let t3 = getMonoTime()
    if isExplicitRowId:
      if rowid >= tablePtr.nextRowId:
        tablePtr.nextRowId = rowid + 1
    else:
      tablePtr.nextRowId = rowid + 1

    tablePtr.rootPage = tree.root

    if tablePtr.rootPage != origRootPage:
      let saveRes = saveTable(catalog, pager, tablePtr[])
      if not saveRes.ok:
        return err[uint64](saveRes.err.code, saveRes.err.message, saveRes.err.context)
    nsMeta = int64(inNanoseconds(getMonoTime() - t3))
    return ok(rowid)
  else:
    if isExplicitRowId:
      if rowid >= tablePtr.nextRowId:
        tablePtr.nextRowId = rowid + 1
    else:
      tablePtr.nextRowId = rowid + 1

    tablePtr.rootPage = tree.root

    if tablePtr.rootPage != origRootPage:
      let saveRes = saveTable(catalog, pager, tablePtr[])
      if not saveRes.ok:
        return err[uint64](saveRes.err.code, saveRes.err.message, saveRes.err.context)

    ok(rowid)

proc insertRow*(pager: Pager, catalog: Catalog, tableName: string, values: seq[Value]): Result[uint64] =
  var hasIndexes = false
  for _, idx in catalog.indexes:
    if idx.table == tableName:
      hasIndexes = true
      break
  insertRowInternal(pager, catalog, tableName, values, hasIndexes)

proc insertRowWithHint*(pager: Pager, catalog: Catalog, tableName: string, values: seq[Value], hasIndexes: bool, int64PkIdx: int = -2): Result[uint64] =
  ## Insert with precomputed hints. int64PkIdx: -2 = unknown, -1 = no single INT64 PK, >= 0 = column index
  insertRowInternal(pager, catalog, tableName, values, hasIndexes, int64PkIdx)

proc insertRowNoIndexes*(pager: Pager, catalog: Catalog, tableName: string, values: seq[Value]): Result[uint64] =
  insertRowInternal(pager, catalog, tableName, values, false)

proc deleteRow*(pager: Pager, catalog: Catalog, tableName: string, rowid: uint64): Result[Void]

proc updateRow*(pager: Pager, catalog: Catalog, tableName: string, rowid: uint64, values: seq[Value]): Result[Void] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[Void](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  if values.len != table.columns.len:
    return err[Void](ERR_SQL, "Column count mismatch", tableName)

  var newRowId = rowid
  var pkChanged = false
  for i, col in table.columns:
    if col.primaryKey and col.kind == ctInt64:
      if values[i].kind == vkInt64:
        let val = cast[uint64](values[i].int64Val)
        if val != rowid:
          newRowId = val
          pkChanged = true
      break

  if pkChanged:
    let checkRes = readRowAt(pager, table, newRowId)
    if checkRes.ok:
      return err[Void](ERR_SQL, "Unique constraint failed: Primary Key conflict", $newRowId)
    let delRes = deleteRow(pager, catalog, tableName, rowid)
    if not delRes.ok: return delRes
    let insRes = insertRow(pager, catalog, tableName, values)
    if not insRes.ok: return err[Void](insRes.err.code, insRes.err.message, insRes.err.context)
    return okVoid()

  var hasIndexes = false
  for _, idx in catalog.indexes:
    if idx.table == tableName:
      hasIndexes = true
      break

  if hasIndexes:
    let oldRes = readRowAt(pager, table, rowid)
    if not oldRes.ok:
      return err[Void](oldRes.err.code, oldRes.err.message, oldRes.err.context)
    for _, idx in catalog.indexes:
      if idx.table != tableName:
        continue
      let oldIncluded = shouldIncludeInIndex(table, idx, oldRes.value.values)
      let newIncluded = shouldIncludeInIndex(table, idx, values)
      if idx.kind == ikBtree:
        if oldIncluded or newIncluded:
          let idxTree = newBTree(pager, idx.rootPage)
          let textBlob = isTextBlobIndex(table, idx)
          if oldIncluded:
            let oldKeyRes = indexKeyForRow(table, idx, oldRes.value.values)
            if not oldKeyRes.ok:
              return err[Void](oldKeyRes.err.code, oldKeyRes.err.message, oldKeyRes.err.context)
            var oldIdxValue = Value(kind: vkNull)
            if textBlob and idx.columns.len == 1:
              for i, col in table.columns:
                if col.name == idx.columns[0]:
                  oldIdxValue = oldRes.value.values[i]
                  break
            let delRes = deleteKeyValue(idxTree, oldKeyRes.value, encodeIndexEntry(rowid, oldIdxValue, textBlob))
            if not delRes.ok:
              return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
          if newIncluded:
            let newKeyRes = indexKeyForRow(table, idx, values)
            if not newKeyRes.ok:
              return err[Void](newKeyRes.err.code, newKeyRes.err.message, newKeyRes.err.context)
            var newIdxValue = Value(kind: vkNull)
            if textBlob and idx.columns.len == 1:
              for i, col in table.columns:
                if col.name == idx.columns[0]:
                  newIdxValue = values[i]
                  break
            let idxInsert = insert(idxTree, newKeyRes.value, encodeIndexEntry(rowid, newIdxValue, textBlob))
            if not idxInsert.ok:
              return err[Void](idxInsert.err.code, idxInsert.err.message, idxInsert.err.context)
          let syncRes = syncIndexRoot(catalog, idx.name, idxTree)
          if not syncRes.ok:
            return syncRes
      else:
        if idx.columns.len == 1:
          var valueIndex = -1
          for i, col in table.columns:
            if col.name == idx.columns[0]:
              valueIndex = i
              break
          if valueIndex >= 0:
            if oldIncluded and newIncluded:
              let updateRes = updateTrigramIndex(pager, catalog, idx, rowid, oldRes.value.values[valueIndex], values[valueIndex])
              if not updateRes.ok:
                return updateRes
            elif oldIncluded and not newIncluded:
              let updateRes = updateTrigramIndex(pager, catalog, idx, rowid, oldRes.value.values[valueIndex], Value(kind: vkNull))
              if not updateRes.ok:
                return updateRes
            elif (not oldIncluded) and newIncluded:
              let updateRes = updateTrigramIndex(pager, catalog, idx, rowid, Value(kind: vkNull), values[valueIndex])
              if not updateRes.ok:
                return updateRes
  let normalizedRes = normalizeValues(pager, values)
  if not normalizedRes.ok:
    return err[Void](normalizedRes.err.code, normalizedRes.err.message, normalizedRes.err.context)
  let tree = newBTree(pager, table.rootPage)
  let updateRes = update(tree, rowid, encodeRecord(normalizedRes.value))
  if not updateRes.ok:
    return err[Void](updateRes.err.code, updateRes.err.message, updateRes.err.context)
  okVoid()

proc deleteRow*(pager: Pager, catalog: Catalog, tableName: string, rowid: uint64): Result[Void] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[Void](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  let oldRes = readRowAt(pager, table, rowid)
  if not oldRes.ok:
    return err[Void](oldRes.err.code, oldRes.err.message, oldRes.err.context)
  for _, idx in catalog.indexes:
    if idx.table != tableName:
      continue
    if not shouldIncludeInIndex(table, idx, oldRes.value.values):
      continue
    if idx.kind == ikBtree:
      let oldKeyRes = indexKeyForRow(table, idx, oldRes.value.values)
      if not oldKeyRes.ok:
        return err[Void](oldKeyRes.err.code, oldKeyRes.err.message, oldKeyRes.err.context)
      let idxTree = newBTree(pager, idx.rootPage)
      let textBlob = isTextBlobIndex(table, idx)
      var oldIdxValue = Value(kind: vkNull)
      if textBlob and idx.columns.len == 1:
        for i, col in table.columns:
          if col.name == idx.columns[0]:
            oldIdxValue = oldRes.value.values[i]
            break
      let delRes = deleteKeyValue(idxTree, oldKeyRes.value, encodeIndexEntry(rowid, oldIdxValue, textBlob))
      if not delRes.ok:
        return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
      let syncRes = syncIndexRoot(catalog, idx.name, idxTree)
      if not syncRes.ok:
        return syncRes
    else:
      if idx.columns.len == 1:
        var valueIndex = -1
        for i, col in table.columns:
          if col.name == idx.columns[0]:
            valueIndex = i
            break
        if valueIndex >= 0:
          if oldRes.value.values[valueIndex].kind == vkText:
            let grams = uniqueTrigrams(valueText(oldRes.value.values[valueIndex]))
            for g in grams:
              catalog.trigramBufferRemove(idx.name, g, rowid)
  let tree = newBTree(pager, table.rootPage)
  let delRes = delete(tree, rowid)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  okVoid()

proc buildIndexForColumn*(pager: Pager, catalog: Catalog, tableName: string, columnName: string, indexRoot: PageId, predicateSql: string = ""): Result[PageId] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[PageId](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  var columnIndex = -1
  for i, col in table.columns:
    if col.name == columnName:
      columnIndex = i
      break
  if columnIndex < 0:
    return err[PageId](ERR_SQL, "Column not found", columnName)
  let rowsRes = scanTable(pager, table)
  if not rowsRes.ok:
    return err[PageId](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
  let idxMeta = IndexMeta(table: tableName, columns: @[columnName], predicateSql: predicateSql)
  let textBlob = table.columns[columnIndex].kind in {ctText, ctBlob}
  var triples: seq[(uint64, uint64, Value)] = @[]
  for row in rowsRes.value:
    if not shouldIncludeInIndex(table, idxMeta, row.values):
      continue
    triples.add((indexKeyFromValue(row.values[columnIndex]), row.rowid, row.values[columnIndex]))
  triples.sort(proc(a, b: (uint64, uint64, Value)): int =
    let c = cmp(a[0], b[0])
    if c != 0: c else: cmp(a[1], b[1])
  )
  var entries: seq[(uint64, seq[byte])] = @[]
  entries.setLen(triples.len)
  for i, triple in triples:
    entries[i] = (triple[0], encodeIndexEntry(triple[1], triple[2], textBlob))
  let idxTree = newBTree(pager, indexRoot)
  let buildRes = bulkBuildFromSorted(idxTree, entries)
  if not buildRes.ok:
    return err[PageId](buildRes.err.code, buildRes.err.message, buildRes.err.context)
  ok(buildRes.value)

proc buildIndexForColumns*(pager: Pager, catalog: Catalog, tableName: string, columnNames: seq[string], indexRoot: PageId, predicateSql: string = ""): Result[PageId] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[PageId](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  var colIndices: seq[int] = @[]
  for colName in columnNames:
    var found = false
    for i, col in table.columns:
      if col.name == colName:
        colIndices.add(i)
        found = true
        break
    if not found:
      return err[PageId](ERR_SQL, "Column not found", colName)
  let rowsRes = scanTable(pager, table)
  if not rowsRes.ok:
    return err[PageId](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
  let idxMeta = IndexMeta(table: tableName, columns: columnNames, predicateSql: predicateSql)
  var pairs: seq[(uint64, uint64)] = @[]
  for row in rowsRes.value:
    if not shouldIncludeInIndex(table, idxMeta, row.values):
      continue
    pairs.add((compositeIndexKey(row.values, colIndices), row.rowid))
  pairs.sort(proc(a, b: (uint64, uint64)): int =
    let c = cmp(a[0], b[0])
    if c != 0: c else: cmp(a[1], b[1])
  )
  var entries: seq[(uint64, seq[byte])] = @[]
  entries.setLen(pairs.len)
  for i, pair in pairs:
    entries[i] = (pair[0], encodeRowId(pair[1]))
  let idxTree = newBTree(pager, indexRoot)
  let buildRes = bulkBuildFromSorted(idxTree, entries)
  if not buildRes.ok:
    return err[PageId](buildRes.err.code, buildRes.err.message, buildRes.err.context)
  ok(buildRes.value)

proc buildIndexForExpression*(pager: Pager, catalog: Catalog, tableName: string, expressionToken: string, indexRoot: PageId): Result[PageId] =
  if not isExpressionIndexToken(expressionToken):
    return err[PageId](ERR_SQL, "Expression index token is invalid", expressionToken)
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[PageId](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  let rowsRes = scanTable(pager, table)
  if not rowsRes.ok:
    return err[PageId](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)

  let idxMeta = IndexMeta(table: tableName, columns: @[expressionToken])
  var pairs: seq[(uint64, uint64)] = @[]
  for row in rowsRes.value:
    let keyRes = indexKeyForRow(table, idxMeta, row.values)
    if not keyRes.ok:
      return err[PageId](keyRes.err.code, keyRes.err.message, keyRes.err.context)
    pairs.add((keyRes.value, row.rowid))
  pairs.sort(proc(a, b: (uint64, uint64)): int =
    let c = cmp(a[0], b[0])
    if c != 0: c else: cmp(a[1], b[1])
  )

  var entries: seq[(uint64, seq[byte])] = @[]
  entries.setLen(pairs.len)
  for i, pair in pairs:
    entries[i] = (pair[0], encodeRowId(pair[1]))
  let idxTree = newBTree(pager, indexRoot)
  let buildRes = bulkBuildFromSorted(idxTree, entries)
  if not buildRes.ok:
    return err[PageId](buildRes.err.code, buildRes.err.message, buildRes.err.context)
  ok(buildRes.value)

proc buildTrigramIndexForColumn*(pager: Pager, catalog: Catalog, tableName: string, columnName: string, indexRoot: PageId): Result[PageId] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[PageId](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  var columnIndex = -1
  for i, col in table.columns:
    if col.name == columnName:
      columnIndex = i
      break
  if columnIndex < 0:
    return err[PageId](ERR_SQL, "Column not found", columnName)

  # Bulk-build trigram postings in-memory and write once.
  # This is dramatically faster than repeatedly reading/decoding/writing postings
  # for each trigram across all rows.
  var postingsByTrigram: Table[uint32, seq[uint64]] = initTable[uint32, seq[uint64]]()
  let scanRes = scanTableEach(pager, table, proc(row: StoredRow): Result[Void] =
    if row.values[columnIndex].kind != vkText:
      return okVoid()
    let grams = uniqueTrigrams(valueText(row.values[columnIndex]))
    for g in grams:
      if not postingsByTrigram.hasKey(g):
        postingsByTrigram[g] = @[]
      postingsByTrigram[g].add(row.rowid)
    okVoid()
  )
  if not scanRes.ok:
    return err[PageId](scanRes.err.code, scanRes.err.message, scanRes.err.context)

  var entries: seq[(uint64, seq[byte])] = @[]
  var i = 0
  for trigram, ids in postingsByTrigram:
    # scanTableEach yields rowids in ascending order (table btree key order),
    # so ids are already sorted and unique per trigram.
    let encoded = encodePostingsSorted(ids)
    # Split into chunks for the chunked postings format
    var offset = 0
    var chunkId: uint16 = 0
    while offset < encoded.len:
      var end_pos = min(offset + PostingsChunkThreshold, encoded.len)
      if end_pos < encoded.len:
        while end_pos < encoded.len and end_pos > offset:
          if byte(encoded[end_pos - 1]) < 0x80:
            break
          end_pos.inc
        if end_pos >= encoded.len:
          end_pos = encoded.len
      entries.add((trigramChunkKey(trigram, chunkId), encoded[offset ..< end_pos]))
      offset = end_pos
      chunkId.inc
    i.inc
  entries.sort(proc(a, b: (uint64, seq[byte])): int = cmp(a[0], b[0]))

  let idxTree = newBTree(pager, indexRoot)
  let buildRes = bulkBuildFromSorted(idxTree, entries)
  if not buildRes.ok:
    return err[PageId](buildRes.err.code, buildRes.err.message, buildRes.err.context)
  ok(buildRes.value)

proc resetIndexRoot(pager: Pager, root: PageId): Result[Void] =
  var buf = newString(pager.pageSize)
  buf[0] = char(PageTypeLeaf)
  buf[1] = char(PageFlagDeltaKeys)
  buf[2] = '\0'
  buf[3] = '\0'
  writeU32LE(buf, 4, 0'u32)
  let writeRes = writePage(pager, root, buf)
  if not writeRes.ok:
    return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  okVoid()

proc rebuildIndex*(pager: Pager, catalog: Catalog, index: IndexMeta): Result[Void] =
  let freeRes = freeBTreePagesExceptRoot(pager, index.rootPage)
  if not freeRes.ok:
    return err[Void](freeRes.err.code, freeRes.err.message, freeRes.err.context)
  let resetRes = resetIndexRoot(pager, index.rootPage)
  if not resetRes.ok:
    return err[Void](resetRes.err.code, resetRes.err.message, resetRes.err.context)
  var newRoot = index.rootPage
  if index.kind == ikTrigram:
    let buildRes = buildTrigramIndexForColumn(pager, catalog, index.table, index.columns[0], index.rootPage)
    if not buildRes.ok:
      return err[Void](buildRes.err.code, buildRes.err.message, buildRes.err.context)
    newRoot = buildRes.value
  else:
    if index.columns.len == 1 and isExpressionIndexToken(index.columns[0]):
      let buildRes = buildIndexForExpression(pager, catalog, index.table, index.columns[0], index.rootPage)
      if not buildRes.ok:
        return err[Void](buildRes.err.code, buildRes.err.message, buildRes.err.context)
      newRoot = buildRes.value
    elif index.columns.len == 1:
      let buildRes = buildIndexForColumn(pager, catalog, index.table, index.columns[0], index.rootPage, index.predicateSql)
      if not buildRes.ok:
        return err[Void](buildRes.err.code, buildRes.err.message, buildRes.err.context)
      newRoot = buildRes.value
    else:
      let buildRes = buildIndexForColumns(pager, catalog, index.table, index.columns, index.rootPage, index.predicateSql)
      if not buildRes.ok:
        return err[Void](buildRes.err.code, buildRes.err.message, buildRes.err.context)
      newRoot = buildRes.value
  if newRoot != index.rootPage:
    var meta = index
    meta.rootPage = newRoot
    let saveRes = catalog.saveIndexMeta(meta)
    if not saveRes.ok:
      return err[Void](saveRes.err.code, saveRes.err.message, saveRes.err.context)
  okVoid()

proc indexSeek*(pager: Pager, catalog: Catalog, tableName: string, column: string, value: Value): Result[seq[uint64]] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[seq[uint64]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  var idxOpt: Option[IndexMeta] = none(IndexMeta)
  if isExpressionIndexToken(column):
    for _, idx in catalog.indexes:
      if idx.table == tableName and idx.kind == ikBtree and idx.columns.len == 1 and idx.columns[0] == column:
        idxOpt = some(idx)
        break
  else:
    idxOpt = catalog.getBtreeIndexForColumn(tableName, column)
  if isNone(idxOpt):
    return err[seq[uint64]](ERR_SQL, "Index not found", tableName & "." & column)
  let idx = idxOpt.get
  let textBlob = isTextBlobIndex(table, idx)
  let idxTree = newBTree(pager, idx.rootPage)
  let needle = indexKeyFromValue(value)
  let cursorRes = openCursorAt(idxTree, needle)
  if not cursorRes.ok:
    return err[seq[uint64]](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  let cursor = cursorRes.value
  var matches: seq[uint64] = @[]
  let needleBytes = if textBlob and value.kind in {vkText, vkBlob}: value.bytes else: @[]
  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      if nextRes.err.code == ERR_IO and nextRes.err.message == "Cursor exhausted":
        break
      return err[seq[uint64]](nextRes.err.code, nextRes.err.message, nextRes.err.context)
    if nextRes.value[0] < needle:
      continue
    if nextRes.value[0] > needle:
      break
    if nextRes.value[0] == needle:
      let entryRes = decodeIndexEntry(nextRes.value[1], textBlob)
      if entryRes.ok:
        if textBlob:
          if entryRes.value.valueBytes == needleBytes:
            matches.add(entryRes.value.rowid)
        else:
          matches.add(entryRes.value.rowid)
  ok(matches)

proc indexHasAnyKey*(pager: Pager, index: IndexMeta, key: uint64): Result[bool] =
  let idxTree = newBTree(pager, index.rootPage)
  let cursorRes = openCursorAt(idxTree, key)
  if not cursorRes.ok:
    return err[bool](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  let cursor = cursorRes.value
  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      break
    if nextRes.value[0] < key:
      continue
    if nextRes.value[0] > key:
      break
    if nextRes.value[0] == key:
      return ok(true)
  ok(false)

proc indexHasOtherRowid*(pager: Pager, index: IndexMeta, key: uint64, rowid: uint64, isTextBlob: bool = false, valueBytes: seq[byte] = @[]): Result[bool] =
  let idxTree = newBTree(pager, index.rootPage)
  let cursorRes = openCursorAt(idxTree, key)
  if not cursorRes.ok:
    return err[bool](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  let cursor = cursorRes.value
  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      break
    if nextRes.value[0] < key:
      continue
    if nextRes.value[0] > key:
      break
    if nextRes.value[0] == key:
      let entryRes = decodeIndexEntry(nextRes.value[1], isTextBlob)
      if entryRes.ok:
        if isTextBlob:
          if entryRes.value.valueBytes == valueBytes and entryRes.value.rowid != rowid:
            return ok(true)
        else:
          if entryRes.value.rowid != rowid:
            return ok(true)
  ok(false)

# ALTER TABLE implementation
proc columnFromColumnDef(colDef: ColumnDef): Result[Column] =
  let typeRes = parseColumnType(colDef.typeName)
  if not typeRes.ok:
    return err[Column](typeRes.err.code, typeRes.err.message, typeRes.err.context)
  let spec = typeRes.value
  ok(Column(
    name: colDef.name,
    kind: spec.kind,
    notNull: colDef.notNull,
    unique: colDef.unique,
    primaryKey: colDef.primaryKey,
    refTable: colDef.refTable,
    refColumn: colDef.refColumn,
    refOnDelete: colDef.refOnDelete,
    refOnUpdate: colDef.refOnUpdate,
    decPrecision: spec.decPrecision,
    decScale: spec.decScale
  ))

proc createNullValue(kind: ColumnType): Value =
  Value(kind: vkNull)

proc stringToTextValue(text: string): Value =
  var bytes: seq[byte] = @[]
  for ch in text:
    bytes.add(byte(ch))
  Value(kind: vkText, bytes: bytes)

proc valueToStringForAlter(value: Value): string =
  case value.kind
  of vkNull:
    "NULL"
  of vkBool:
    if value.boolVal: "true" else: "false"
  of vkInt64:
    $value.int64Val
  of vkFloat64:
    $value.float64Val
  of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
    var s = ""
    for b in value.bytes:
      s.add(char(b))
    s
  else:
    ""

proc convertValueForAlter(value: Value, targetKind: ColumnType): Result[Value] =
  if value.kind == vkNull:
    return ok(Value(kind: vkNull))

  case targetKind
  of ctInt64:
    case value.kind
    of vkInt64:
      return ok(value)
    of vkFloat64:
      return ok(Value(kind: vkInt64, int64Val: int64(value.float64Val)))
    of vkBool:
      return ok(Value(kind: vkInt64, int64Val: (if value.boolVal: 1 else: 0)))
    of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
      let s = valueToStringForAlter(value).strip()
      try:
        let parsed = parseBiggestInt(s)
        return ok(Value(kind: vkInt64, int64Val: int64(parsed)))
      except ValueError:
        return err[Value](ERR_SQL, "Invalid type conversion text-to-int value", s)
    else:
      return err[Value](ERR_SQL, "Unsupported type conversion source type")
  of ctFloat64:
    case value.kind
    of vkFloat64:
      return ok(value)
    of vkInt64:
      return ok(Value(kind: vkFloat64, float64Val: float64(value.int64Val)))
    of vkBool:
      return ok(Value(kind: vkFloat64, float64Val: (if value.boolVal: 1.0 else: 0.0)))
    of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
      let s = valueToStringForAlter(value).strip()
      try:
        return ok(Value(kind: vkFloat64, float64Val: parseFloat(s)))
      except ValueError:
        return err[Value](ERR_SQL, "Invalid type conversion text-to-float value", s)
    else:
      return err[Value](ERR_SQL, "Unsupported type conversion source type")
  of ctText:
    return ok(stringToTextValue(valueToStringForAlter(value)))
  of ctBool:
    case value.kind
    of vkBool:
      return ok(value)
    of vkInt64:
      return ok(Value(kind: vkBool, boolVal: value.int64Val != 0))
    of vkFloat64:
      return ok(Value(kind: vkBool, boolVal: value.float64Val != 0.0))
    of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
      let s = valueToStringForAlter(value).strip().toLowerAscii()
      if s in ["true", "t", "1"]:
        return ok(Value(kind: vkBool, boolVal: true))
      if s in ["false", "f", "0"]:
        return ok(Value(kind: vkBool, boolVal: false))
      return err[Value](ERR_SQL, "Invalid type conversion text-to-bool value", s)
    else:
      return err[Value](ERR_SQL, "Unsupported type conversion source type")
  else:
    return err[Value](ERR_SQL, "Unsupported type conversion target type", columnTypeToText(targetKind))

proc alterColumnTypeInTable(pager: Pager, catalog: Catalog, table: var TableMeta, columnName: string, newTypeName: string): Result[Void] =
  let originalTableMeta = table
  var columnIndex = -1
  for i, col in table.columns:
    if col.name == columnName:
      columnIndex = i
      break
  if columnIndex < 0:
    return err[Void](ERR_SQL, "Column not found", columnName)

  let targetTypeRes = parseColumnType(newTypeName)
  if not targetTypeRes.ok:
    return err[Void](targetTypeRes.err.code, targetTypeRes.err.message, targetTypeRes.err.context)
  let targetSpec = targetTypeRes.value

  if targetSpec.kind notin {ctInt64, ctFloat64, ctText, ctBool}:
    return err[Void](ERR_SQL, "ALTER COLUMN TYPE target not supported in 0.x", newTypeName)
  if table.columns[columnIndex].kind notin {ctInt64, ctFloat64, ctText, ctBool}:
    return err[Void](ERR_SQL, "ALTER COLUMN TYPE source not supported in 0.x", columnName)

  if table.columns[columnIndex].kind == targetSpec.kind and
     table.columns[columnIndex].decPrecision == targetSpec.decPrecision and
     table.columns[columnIndex].decScale == targetSpec.decScale:
    return okVoid()

  let oldTree = newBTree(pager, table.rootPage)
  let cursorRes = openCursor(oldTree)
  if not cursorRes.ok:
    return err[Void](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)

  let newRootRes = initTableRoot(pager)
  if not newRootRes.ok:
    return err[Void](newRootRes.err.code, newRootRes.err.message, newRootRes.err.context)

  let newRoot = newRootRes.value
  var newTree = newBTree(pager, newRoot)
  let cursor = cursorRes.value

  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      break

    let rowid = nextRes.value[0]
    let valueBytes = nextRes.value[1]
    let overflow = nextRes.value[2]
    if valueBytes.len == 0 and overflow == 0'u32:
      continue

    let decoded = decodeRecordWithOverflow(pager, valueBytes)
    if not decoded.ok:
      return err[Void](decoded.err.code, decoded.err.message, decoded.err.context)
    if columnIndex >= decoded.value.len:
      return err[Void](ERR_CORRUPTION, "Row column count does not match table metadata", table.name)

    var newValues = decoded.value
    let convertedRes = convertValueForAlter(decoded.value[columnIndex], targetSpec.kind)
    if not convertedRes.ok:
      return err[Void](convertedRes.err.code, convertedRes.err.message, "rowid=" & $rowid & ",column=" & columnName)
    newValues[columnIndex] = convertedRes.value

    let normalizedRes = normalizeValues(pager, newValues)
    if not normalizedRes.ok:
      return err[Void](normalizedRes.err.code, normalizedRes.err.message, normalizedRes.err.context)
    let record = encodeRecord(normalizedRes.value)
    let insertRes = insert(newTree, rowid, record)
    if not insertRes.ok:
      return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)

  table.columns[columnIndex].kind = targetSpec.kind
  table.columns[columnIndex].decPrecision = targetSpec.decPrecision
  table.columns[columnIndex].decScale = targetSpec.decScale
  table.rootPage = newRoot

  # Rebuild indexes against the rewritten table contents and updated column type.
  updateTableMeta(catalog, table)
  for _, idx in catalog.indexes:
    if idx.table == table.name:
      let rebuildRes = rebuildIndex(pager, catalog, idx)
      if not rebuildRes.ok:
        updateTableMeta(catalog, originalTableMeta)
        return rebuildRes
  okVoid()

proc dropColumnFromTable(pager: Pager, catalog: Catalog, table: var TableMeta, columnName: string): Result[Void] =
  var columnIndex = -1
  for i, col in table.columns:
    if col.name == columnName:
      columnIndex = i
      break
  
  if columnIndex < 0:
    return err[Void](ERR_SQL, "Column not found", columnName)
  
  var newColumns: seq[Column] = @[]
  for i, col in table.columns:
    if i != columnIndex:
      newColumns.add(col)
  
  var indexesToDrop: seq[string] = @[]
  for idxName, idx in catalog.indexes:
    if idx.table == table.name and idx.columns.len == 1 and idx.columns[0] == columnName:
      indexesToDrop.add(idxName)
  
  for idxName in indexesToDrop:
    let dropIdxRes = catalog.dropIndex(idxName)
    if not dropIdxRes.ok:
      return dropIdxRes
  
  let oldTree = newBTree(pager, table.rootPage)
  let cursorRes = openCursor(oldTree)
  if not cursorRes.ok:
    return err[Void](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  
  let newRootRes = initTableRoot(pager)
  if not newRootRes.ok:
    return err[Void](newRootRes.err.code, newRootRes.err.message, newRootRes.err.context)
  
  let newRoot = newRootRes.value
  var newTree = newBTree(pager, newRoot)
  let cursor = cursorRes.value
  
  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      break
    
    let rowid = nextRes.value[0]
    let valueBytes = nextRes.value[1]
    let overflow = nextRes.value[2]
    
    if valueBytes.len == 0 and overflow == 0'u32:
      continue
    
    let decoded = decodeRecordWithOverflow(pager, valueBytes)
    if not decoded.ok:
      return err[Void](decoded.err.code, decoded.err.message, decoded.err.context)
    
    var newValues: seq[Value] = @[]
    for i, val in decoded.value:
      if i != columnIndex:
        newValues.add(val)
    
    let normalizedRes = normalizeValues(pager, newValues)
    if not normalizedRes.ok:
      return err[Void](normalizedRes.err.code, normalizedRes.err.message, normalizedRes.err.context)
    
    let record = encodeRecord(normalizedRes.value)
    let insertRes = insert(newTree, rowid, record)
    if not insertRes.ok:
      return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  
  table.columns = newColumns
  table.rootPage = newRoot
  
  for idxName, idx in catalog.indexes:
    if idx.table == table.name:
      let rebuildRes = rebuildIndex(pager, catalog, idx)
      if not rebuildRes.ok:
        return rebuildRes
  
  okVoid()

proc addColumnToTable(pager: Pager, catalog: Catalog, table: var TableMeta, colDef: ColumnDef): Result[Void] =
  for col in table.columns:
    if col.name == colDef.name:
      return err[Void](ERR_SQL, "Column already exists", colDef.name)
  
  let colRes = columnFromColumnDef(colDef)
  if not colRes.ok:
    return err[Void](colRes.err.code, colRes.err.message, colRes.err.context)
  
  let newColumn = colRes.value
  let nullValue = createNullValue(newColumn.kind)
  
  let oldTree = newBTree(pager, table.rootPage)
  let cursorRes = openCursor(oldTree)
  if not cursorRes.ok:
    return err[Void](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  
  let newRootRes = initTableRoot(pager)
  if not newRootRes.ok:
    return err[Void](newRootRes.err.code, newRootRes.err.message, newRootRes.err.context)
  
  let newRoot = newRootRes.value
  var newTree = newBTree(pager, newRoot)
  let cursor = cursorRes.value
  
  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      break
    
    let rowid = nextRes.value[0]
    let valueBytes = nextRes.value[1]
    let overflow = nextRes.value[2]
    
    if valueBytes.len == 0 and overflow == 0'u32:
      continue
    
    let decoded = decodeRecordWithOverflow(pager, valueBytes)
    if not decoded.ok:
      return err[Void](decoded.err.code, decoded.err.message, decoded.err.context)
    
    var newValues = decoded.value
    newValues.add(nullValue)
    
    let normalizedRes = normalizeValues(pager, newValues)
    if not normalizedRes.ok:
      return err[Void](normalizedRes.err.code, normalizedRes.err.message, normalizedRes.err.context)
    
    let record = encodeRecord(normalizedRes.value)
    let insertRes = insert(newTree, rowid, record)
    if not insertRes.ok:
      return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  
  table.columns.add(newColumn)
  table.rootPage = newRoot
  
  for idxName, idx in catalog.indexes:
    if idx.table == table.name:
      let rebuildRes = rebuildIndex(pager, catalog, idx)
      if not rebuildRes.ok:
        return rebuildRes
  
  okVoid()

proc renameColumnInTable(pager: Pager, catalog: Catalog, table: var TableMeta, oldName: string, newName: string): Result[Void] =
  var oldIndex = -1
  for i, col in table.columns:
    if col.name == oldName:
      oldIndex = i
    if col.name == newName:
      return err[Void](ERR_SQL, "Column already exists", newName)
  if oldIndex < 0:
    return err[Void](ERR_SQL, "Column not found", oldName)

  table.columns[oldIndex].name = newName
  for col in table.columns.mitems:
    if col.refTable == table.name and col.refColumn == oldName:
      col.refColumn = newName

  var updatedIndexes: seq[IndexMeta] = @[]
  for _, idx in catalog.indexes:
    if idx.table != table.name:
      continue
    var changed = false
    var meta = idx
    for idxCol in meta.columns.mitems:
      if idxCol == oldName:
        idxCol = newName
        changed = true
    if meta.predicateSql.len > 0:
      let oldPredBare = oldName & " IS NOT NULL"
      let newPredBare = newName & " IS NOT NULL"
      let oldPredParen = "(" & oldName & " IS NOT NULL)"
      let newPredParen = "(" & newName & " IS NOT NULL)"
      if meta.predicateSql == oldPredBare:
        meta.predicateSql = newPredBare
        changed = true
      elif meta.predicateSql == oldPredParen:
        meta.predicateSql = newPredParen
        changed = true
    if changed:
      updatedIndexes.add(meta)

  for idxMeta in updatedIndexes:
    let saveIdxRes = saveIndexMeta(catalog, idxMeta)
    if not saveIdxRes.ok:
      return saveIdxRes

  var tableNames: seq[string] = @[]
  for tableName, _ in catalog.tables:
    if tableName != table.name:
      tableNames.add(tableName)
  for tableName in tableNames:
    var other = catalog.tables[tableName]
    var changed = false
    for col in other.columns.mitems:
      if col.refTable == table.name and col.refColumn == oldName:
        col.refColumn = newName
        changed = true
    if changed:
      let saveRes = saveTable(catalog, pager, other)
      if not saveRes.ok:
        return saveRes
  okVoid()

proc alterTable*(pager: Pager, catalog: Catalog, tableName: string, actions: seq[AlterTableAction]): Result[Void] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[Void](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  
  var table = tableRes.value
  if table.checks.len > 0:
    return err[Void](ERR_SQL, "ALTER TABLE on tables with CHECK constraints is not supported in 0.x", tableName)
  
  for action in actions:
    case action.kind
    of ataAddColumn:
      let addRes = addColumnToTable(pager, catalog, table, action.columnDef)
      if not addRes.ok:
        return addRes
    
    of ataDropColumn:
      let dropRes = dropColumnFromTable(pager, catalog, table, action.columnName)
      if not dropRes.ok:
        return dropRes

    of ataRenameColumn:
      let renameRes = renameColumnInTable(pager, catalog, table, action.columnName, action.newColumnName)
      if not renameRes.ok:
        return renameRes

    of ataAlterColumn:
      case action.alterColumnAction
      of acaSetType:
        let alterRes = alterColumnTypeInTable(pager, catalog, table, action.columnName, action.alterColumnNewType)
        if not alterRes.ok:
          return alterRes
      else:
        return err[Void](ERR_INTERNAL, "ALTER TABLE ALTER COLUMN action not yet supported", $action.alterColumnAction)
    
    else:
      return err[Void](ERR_INTERNAL, "ALTER TABLE action not yet supported", $action.kind)
  
  let saveRes = catalog.saveTable(pager, table)
  if not saveRes.ok:
    return saveRes
  
  pager.header.schemaCookie.inc
  let writeRes = writeHeader(pager.vfs, pager.file, pager.header)
  if not writeRes.ok:
    return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  
  okVoid()
