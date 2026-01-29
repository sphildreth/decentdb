import options
import tables
import algorithm
import ../errors
import ../record/record
import ../pager/pager
import ../pager/db_header
import ../btree/btree
import ../catalog/catalog
import ../search/search
import sets

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
  writeU32LE(buf, 4, 0)
  let writeRes = writePage(pager, root, buf)
  if not writeRes.ok:
    return err[PageId](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  ok(root)

proc encodeRowId(rowid: uint64): seq[byte] =
  var buf = newSeq[byte](8)
  writeU64LE(buf, 0, rowid)
  buf

proc decodeRowId(data: seq[byte]): Result[uint64] =
  if data.len < 8:
    return err[uint64](ERR_CORRUPTION, "Index rowid payload too short")
  ok(readU64LE(data, 0))

proc indexKeyFromValue*(value: Value): uint64 =
  case value.kind
  of vkInt64:
    cast[uint64](value.int64Val)
  of vkBool:
    if value.boolVal: 1'u64 else: 0'u64
  of vkFloat64:
    cast[uint64](value.float64Val)
  of vkText, vkBlob:
    uint64(crc32c(value.bytes))
  of vkTextOverflow, vkBlobOverflow:
    0'u64
  else:
    0'u64

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

proc loadPostings(tree: BTree, trigram: uint32): Result[seq[byte]] =
  let key = uint64(trigram)
  let findRes = find(tree, key)
  if findRes.ok:
    return ok(findRes.value[1])
  if findRes.err.code == ERR_IO:
    return ok(newSeq[byte]())
  err[seq[byte]](findRes.err.code, findRes.err.message, findRes.err.context)

proc storePostings(tree: BTree, trigram: uint32, data: seq[byte]): Result[Void] =
  let key = uint64(trigram)
  if data.len == 0:
    let delRes = delete(tree, key)
    if delRes.ok or delRes.err.code == ERR_IO:
      return okVoid()
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  let findRes = find(tree, key)
  if findRes.ok:
    let updateRes = update(tree, key, data)
    if not updateRes.ok:
      return err[Void](updateRes.err.code, updateRes.err.message, updateRes.err.context)
    return okVoid()
  let insertRes = insert(tree, key, data)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
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

proc flushTrigramDeltas*(pager: Pager, catalog: Catalog): Result[Void] =
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

  catalog.clearTrigramDeltas()
  okVoid()

proc syncIndexRoot(catalog: Catalog, indexName: string, tree: BTree): Result[Void] =
  if not catalog.indexes.hasKey(indexName):
    return okVoid()
  var meta = catalog.indexes[indexName]
  if meta.rootPage != tree.root:
    meta.rootPage = tree.root
    let saveRes = catalog.saveIndexMeta(meta)
    if not saveRes.ok:
      return err[Void](saveRes.err.code, saveRes.err.message, saveRes.err.context)
  okVoid()

proc normalizeValues*(pager: Pager, values: seq[Value]): Result[seq[Value]] =
  var resultValues: seq[Value] = @[]
  for value in values:
    if value.kind in {vkText, vkBlob} and value.bytes.len > (pager.pageSize - 128):
      let pageRes = writeOverflowChain(pager, value.bytes)
      if not pageRes.ok:
        return err[seq[Value]](pageRes.err.code, pageRes.err.message, pageRes.err.context)
      let overflowKind = if value.kind == vkText: vkTextOverflow else: vkBlobOverflow
      resultValues.add(Value(kind: overflowKind, overflowPage: pageRes.value, overflowLen: uint32(value.bytes.len)))
    else:
      resultValues.add(value)
  ok(resultValues)

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

proc insertRowInternal(pager: Pager, catalog: Catalog, tableName: string, values: seq[Value], updateIndexes: bool): Result[uint64] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[uint64](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  var table = tableRes.value
  if values.len != table.columns.len:
    return err[uint64](ERR_SQL, "Column count mismatch", tableName)
  let rowid = if table.nextRowId == 0: 1'u64 else: table.nextRowId
  var indexKeys: seq[(IndexMeta, uint64)] = @[]
  var trigramValues: seq[(IndexMeta, Value)] = @[]
  if updateIndexes:
    for _, idx in catalog.indexes:
      if idx.table != tableName:
        continue
      var valueIndex = -1
      for i, col in table.columns:
        if col.name == idx.column:
          valueIndex = i
          break
      if valueIndex >= 0:
        if idx.kind == ikBtree:
          indexKeys.add((idx, indexKeyFromValue(values[valueIndex])))
        else:
          trigramValues.add((idx, values[valueIndex]))
  let normalizedRes = normalizeValues(pager, values)
  if not normalizedRes.ok:
    return err[uint64](normalizedRes.err.code, normalizedRes.err.message, normalizedRes.err.context)
  let record = encodeRecord(normalizedRes.value)
  let tree = newBTree(pager, table.rootPage)
  let insertRes = insert(tree, rowid, record)
  if not insertRes.ok:
    return err[uint64](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  if updateIndexes:
    for entry in indexKeys:
      let idxTree = newBTree(pager, entry[0].rootPage)
      let idxInsert = insert(idxTree, entry[1], encodeRowId(rowid))
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
  table.nextRowId = rowid + 1
  discard catalog.saveTable(pager, table)
  ok(rowid)

proc insertRow*(pager: Pager, catalog: Catalog, tableName: string, values: seq[Value]): Result[uint64] =
  insertRowInternal(pager, catalog, tableName, values, true)

proc insertRowNoIndexes*(pager: Pager, catalog: Catalog, tableName: string, values: seq[Value]): Result[uint64] =
  insertRowInternal(pager, catalog, tableName, values, false)

proc updateRow*(pager: Pager, catalog: Catalog, tableName: string, rowid: uint64, values: seq[Value]): Result[Void] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[Void](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  if values.len != table.columns.len:
    return err[Void](ERR_SQL, "Column count mismatch", tableName)
  let oldRes = readRowAt(pager, table, rowid)
  if not oldRes.ok:
    return err[Void](oldRes.err.code, oldRes.err.message, oldRes.err.context)
  for _, idx in catalog.indexes:
    if idx.table != tableName:
      continue
    var valueIndex = -1
    for i, col in table.columns:
      if col.name == idx.column:
        valueIndex = i
        break
    if valueIndex >= 0:
      if idx.kind == ikBtree:
        let idxTree = newBTree(pager, idx.rootPage)
        let oldKey = indexKeyFromValue(oldRes.value.values[valueIndex])
        let delRes = deleteKeyValue(idxTree, oldKey, encodeRowId(rowid))
        if not delRes.ok:
          return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
        let newKey = indexKeyFromValue(values[valueIndex])
        let idxInsert = insert(idxTree, newKey, encodeRowId(rowid))
        if not idxInsert.ok:
          return err[Void](idxInsert.err.code, idxInsert.err.message, idxInsert.err.context)
        let syncRes = syncIndexRoot(catalog, idx.name, idxTree)
        if not syncRes.ok:
          return syncRes
      else:
        let updateRes = updateTrigramIndex(pager, catalog, idx, rowid, oldRes.value.values[valueIndex], values[valueIndex])
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
    var valueIndex = -1
    for i, col in table.columns:
      if col.name == idx.column:
        valueIndex = i
        break
    if valueIndex >= 0:
      if idx.kind == ikBtree:
        let idxTree = newBTree(pager, idx.rootPage)
        let oldKey = indexKeyFromValue(oldRes.value.values[valueIndex])
        let delRes = deleteKeyValue(idxTree, oldKey, encodeRowId(rowid))
        if not delRes.ok:
          return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
        let syncRes = syncIndexRoot(catalog, idx.name, idxTree)
        if not syncRes.ok:
          return syncRes
      else:
        if oldRes.value.values[valueIndex].kind == vkText:
          let grams = uniqueTrigrams(valueText(oldRes.value.values[valueIndex]))
          for g in grams:
            catalog.trigramBufferRemove(idx.name, g, rowid)
  let tree = newBTree(pager, table.rootPage)
  let delRes = delete(tree, rowid)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  okVoid()

proc buildIndexForColumn*(pager: Pager, catalog: Catalog, tableName: string, columnName: string, indexRoot: PageId): Result[PageId] =
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
  var pairs: seq[(uint64, uint64)] = @[]
  pairs.setLen(rowsRes.value.len)
  for i, row in rowsRes.value:
    pairs[i] = (indexKeyFromValue(row.values[columnIndex]), row.rowid)
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
  let rowsRes = scanTable(pager, table)
  if not rowsRes.ok:
    return err[PageId](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
  let idxTree = newBTree(pager, indexRoot)
  for row in rowsRes.value:
    if row.values[columnIndex].kind != vkText:
      continue
    let grams = uniqueTrigrams(valueText(row.values[columnIndex]))
    for g in grams:
      let postingsRes = loadPostings(idxTree, g)
      if not postingsRes.ok:
        return err[PageId](postingsRes.err.code, postingsRes.err.message, postingsRes.err.context)
      let updatedRes = addRowid(postingsRes.value, row.rowid)
      if not updatedRes.ok:
        return err[PageId](updatedRes.err.code, updatedRes.err.message, updatedRes.err.context)
      let storeRes = storePostings(idxTree, g, updatedRes.value)
      if not storeRes.ok:
        return err[PageId](storeRes.err.code, storeRes.err.message, storeRes.err.context)
  ok(idxTree.root)

proc resetIndexRoot(pager: Pager, root: PageId): Result[Void] =
  var buf = newString(pager.pageSize)
  buf[0] = char(PageTypeLeaf)
  buf[2] = '\0'
  buf[3] = '\0'
  writeU32LE(buf, 4, 0'u32)
  writePage(pager, root, buf)

proc rebuildIndex*(pager: Pager, catalog: Catalog, index: IndexMeta): Result[Void] =
  let resetRes = resetIndexRoot(pager, index.rootPage)
  if not resetRes.ok:
    return err[Void](resetRes.err.code, resetRes.err.message, resetRes.err.context)
  var newRoot = index.rootPage
  if index.kind == ikTrigram:
    let buildRes = buildTrigramIndexForColumn(pager, catalog, index.table, index.column, index.rootPage)
    if not buildRes.ok:
      return err[Void](buildRes.err.code, buildRes.err.message, buildRes.err.context)
    newRoot = buildRes.value
  else:
    let buildRes = buildIndexForColumn(pager, catalog, index.table, index.column, index.rootPage)
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
  let indexOpt = catalog.getBtreeIndexForColumn(tableName, column)
  if isNone(indexOpt):
    return err[seq[uint64]](ERR_SQL, "Index not found", tableName & "." & column)
  let idx = indexOpt.get
  let idxTree = newBTree(pager, idx.rootPage)
  let needle = indexKeyFromValue(value)
  let cursorRes = openCursorAt(idxTree, needle)
  if not cursorRes.ok:
    return err[seq[uint64]](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  let cursor = cursorRes.value
  var matches: seq[uint64] = @[]
  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      break
    if nextRes.value[0] < needle:
      continue
    if nextRes.value[0] > needle:
      break
    if nextRes.value[0] == needle:
      let rowidRes = decodeRowId(nextRes.value[1])
      if rowidRes.ok:
        matches.add(rowidRes.value)
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

proc indexHasOtherRowid*(pager: Pager, index: IndexMeta, key: uint64, rowid: uint64): Result[bool] =
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
      let rowidRes = decodeRowId(nextRes.value[1])
      if rowidRes.ok and rowidRes.value != rowid:
        return ok(true)
  ok(false)
