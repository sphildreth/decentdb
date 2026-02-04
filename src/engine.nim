import os
import strutils
import tables
import options
import algorithm
import atomics
import sets
import locks
import sequtils
import times
import ./errors
import ./vfs/types
import ./vfs/os_vfs
import ./pager/db_header
import ./pager/pager
import ./catalog/catalog
import ./sql/sql
import ./sql/binder
import ./planner/planner
import ./planner/explain
import ./exec/exec
import ./record/record
import ./storage/storage
import ./wal/wal

type Db* = ref object
  path*: string
  vfs*: Vfs
  file*: VfsFile
  isOpen*: bool
  formatVersion*: uint32
  pageSize*: uint32
  schemaCookie*: uint32
  pager*: Pager
  catalog*: Catalog
  wal*: Wal                          # WAL instance for checkpoint control
  activeWriter*: WalWriter           # Current transaction writer (nil if no active tx)
  walOverlayEnabled*: bool           # Disable WAL overlay after non-WAL writes
  cachePages*: int                   # Cache size for diagnostics
  sqlCache*: Table[string, tuple[schemaCookie: uint32, statements: seq[Statement], plans: seq[Plan]]]
  sqlCacheOrder*: seq[string]

type DurabilityMode* = enum
  dmFull
  dmDeferred
  dmNone

type BulkLoadOptions* = object
  batchSize*: int
  syncInterval*: int
  disableIndexes*: bool
  checkpointOnComplete*: bool
  durability*: DurabilityMode

proc defaultBulkLoadOptions*(): BulkLoadOptions =
  BulkLoadOptions(batchSize: 10000, syncInterval: 10, disableIndexes: true, checkpointOnComplete: true, durability: dmDeferred)

proc openDb*(path: string, cachePages: int = 1024): Result[Db] =
  ## Open a database file with configurable cache size
  ## cachePages: Number of 4KB pages to cache (default 1024 = 4MB)
  let vfs = newOsVfs()
  let res = vfs.open(path, fmReadWrite, true)
  if not res.ok:
    return err[Db](res.err.code, res.err.message, res.err.context)
  let file = res.value
  let info = getFileInfo(path)
  var probe = newSeq[byte](HeaderSize)
  let probeRes = vfs.read(file, 0, probe)
  if not probeRes.ok:
    discard vfs.close(file)
    return err[Db](probeRes.err.code, probeRes.err.message, probeRes.err.context)
  if probeRes.value == 0:
    if info.size > 0:
      discard vfs.close(file)
      return err[Db](ERR_CORRUPTION, "Header unreadable", "page_id=1")
    let header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    let writeRes = writeHeader(vfs, file, header)
    if not writeRes.ok:
      discard vfs.close(file)
      return err[Db](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    let padSize = int(DefaultPageSize) - HeaderSize
    if padSize > 0:
      var pad = newSeq[byte](padSize)
      let padRes = vfs.write(file, int64(HeaderSize), pad)
      if not padRes.ok:
        discard vfs.close(file)
        return err[Db](padRes.err.code, padRes.err.message, padRes.err.context)
      if padRes.value < padSize:
        discard vfs.close(file)
        return err[Db](ERR_IO, "Short write on header padding", "page_id=1")
      let syncRes = vfs.fsync(file)
      if not syncRes.ok:
        discard vfs.close(file)
        return err[Db](syncRes.err.code, syncRes.err.message, syncRes.err.context)
  elif probeRes.value < HeaderSize:
    discard vfs.close(file)
    return err[Db](ERR_CORRUPTION, "Header too short", "page_id=1")
  else:
    let readRes = decodeHeader(probe)
    if not readRes.ok:
      discard vfs.close(file)
      return err[Db](readRes.err.code, readRes.err.message, readRes.err.context)
    if readRes.value.formatVersion != FormatVersion:
      discard vfs.close(file)
      return err[Db](ERR_CORRUPTION, "Unsupported format version", "page_id=1")
    if readRes.value.pageSize != DefaultPageSize:
      discard vfs.close(file)
      return err[Db](ERR_CORRUPTION, "Unsupported page size", "page_id=1")
  let headerRes = readHeader(vfs, file)
  if not headerRes.ok:
    discard vfs.close(file)
    return err[Db](headerRes.err.code, headerRes.err.message, headerRes.err.context)
  let header = headerRes.value
  
  # Create pager with configurable cache size
  let pagerRes = newPager(vfs, file, cachePages = cachePages)
  if not pagerRes.ok:
    discard vfs.close(file)
    return err[Db](pagerRes.err.code, pagerRes.err.message, pagerRes.err.context)
  let pager = pagerRes.value

  # Initialize WAL
  let walPath = path & "-wal"
  let walRes = newWal(vfs, walPath)
  if not walRes.ok:
    discard closePager(pager)
    discard vfs.close(file)
    return err[Db](walRes.err.code, walRes.err.message, walRes.err.context)
  let wal = walRes.value
  
  # Recover WAL on open
  let recoverRes = recover(wal)
  if not recoverRes.ok:
    discard closePager(pager)
    discard vfs.close(file)
    return err[Db](recoverRes.err.code, recoverRes.err.message, recoverRes.err.context)

  # Create Db instance early so we can refer to it in callbacks
  # Use a ref to allow safe capture in closure
  var dbRef = Db(
    path: path,
    vfs: vfs,
    file: file,
    isOpen: true,
    formatVersion: header.formatVersion,
    pageSize: header.pageSize,
    schemaCookie: header.schemaCookie,
    pager: pager,
    catalog: Catalog(), # Placeholder, replaced after initCatalog
    wal: wal,
    activeWriter: nil,
    walOverlayEnabled: true,
    cachePages: cachePages,
    sqlCache: initTable[string, tuple[schemaCookie: uint32, statements: seq[Statement], plans: seq[Plan]]](),
    sqlCacheOrder: @[]
  )

  # Set up permanent WAL overlay to ensure late-commits are visible to all reads,
  # even after cache evictions.
  pager.setPageOverlay(0, proc(pageId: PageId): Option[string] =
    # Dirty pages in cache should ALWAYS take precedence over WAL.
    # This handles both uncommitted changes (overlaySnapshot == 0) and
    # uncommitted writes from dmNone bulk loads (overlaySnapshot > 0).
    if pager.isDirty(pageId):
      return none(string)
    
    # If page was written to disk directly (bypassing WAL), we must read from disk.
    acquire(pager.overlayLock)
    let isOverridden = pageId in pager.overriddenPages
    release(pager.overlayLock)
    if isOverridden:
      return none(string)
    
    # Check if page was flushed to WAL during current active transaction
    if dbRef.activeWriter != nil:
      let flushed = getFlushedPage(dbRef.activeWriter, pageId)
      if flushed.isSome:
        let payloadOpt = readFramePayload(wal, flushed.get.offset)
        if payloadOpt.isSome:
          return payloadOpt
    
    let snap = if pager.overlaySnapshot == 0: wal.walEnd.load(moAcquire) else: pager.overlaySnapshot
    let pageOpt = wal.getPageAtOrBefore(pageId, snap)
    if pageOpt.isNone:
      return none(string)
    let payload = pageOpt.get
    var s = newString(payload.len)
    if payload.len > 0: copyMem(addr s[0], unsafeAddr payload[0], payload.len)
    some(s)
  )

  # Reload header and catalog after recovery to see latest committed state
  let page1Res = readPage(pager, PageId(1))
  if page1Res.ok:
    let hRes = decodeHeader(page1Res.value)
    if hRes.ok:
      pager.header = hRes.value
    else:
      stderr.writeLine("Warning: Failed to decode recovered header: " & hRes.err.message)
  let txn = beginRead(wal)
  pager.overlaySnapshot = txn.snapshot
  let catalogRes = initCatalog(pager)
  pager.overlaySnapshot = 0
  endRead(wal, txn)

  if not catalogRes.ok:
    discard closePager(pager)
    discard vfs.close(file)
    return err[Db](catalogRes.err.code, catalogRes.err.message, catalogRes.err.context)
  
  dbRef.catalog = catalogRes.value

  # Commit any bootstrap changes (e.g. newly created catalog root)
  let dirtyBootstrap = snapshotDirtyPages(pager)
  if dirtyBootstrap.len > 0:
    let writerRes = beginWrite(wal)
    if writerRes.ok:
      let writer = writerRes.value
      var pageIds: seq[PageId] = @[]
      for entry in dirtyBootstrap:
        var bytes = newSeq[byte](entry[1].len)
        if entry[1].len > 0: copyMem(addr bytes[0], unsafeAddr entry[1][0], entry[1].len)
        discard writePage(writer, entry[0], bytes)
        pageIds.add(entry[0])
      let commitRes = commit(writer)
      if commitRes.ok:
        markPagesCommitted(pager, pageIds, commitRes.value)
      else:
        discard rollback(writer)
  # Default checkpoint + long-reader protection (can be overridden via CLI/config).
  # - Checkpoint when WAL grows large (bytes-based trigger)
  # - Checkpoint when WAL index memory exceeds threshold (memory-based trigger)
  # - Warn/abort long-running readers to prevent indefinite WAL pinning
  # - HIGH-006: Limit WAL bytes per reader to prevent unbounded growth
  setCheckpointConfig(wal,
    everyBytes = 64 * 1024 * 1024,
    everyMs = 0,
    readerWarnMs = 60 * 1000,
    readerTimeoutMs = 300 * 1000,
    forceTruncateOnTimeout = true,
    memoryThreshold = 256 * 1024 * 1024,  # 256MB memory limit for WAL index
    maxWalBytesPerReader = 256 * 1024 * 1024,  # HIGH-006: 256MB per reader limit
    readerCheckIntervalMs = 5000)  # HIGH-006: Check readers every 5 seconds
  
  ok(dbRef)

proc schemaBump(db: Db): Result[Void] =
  db.schemaCookie.inc
  db.pager.header.schemaCookie = db.schemaCookie
  db.sqlCache.clear()
  db.sqlCacheOrder = @[]
  
  var pageData = newString(db.pager.pageSize)
  let headerBytes = encodeHeader(db.pager.header)
  copyMem(addr pageData[0], unsafeAddr headerBytes[0], HeaderSize)
  
  let writeRes = writePage(db.pager, PageId(1), pageData)
  if not writeRes.ok:
    return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  okVoid()

proc valueFromSql(value: Value): Value =
  value

proc typeCheckValue(expected: ColumnType, value: Value): Result[Void] =
  case expected
  of ctInt64:
    if value.kind in {vkInt64, vkNull}: return okVoid()
  of ctBool:
    if value.kind in {vkBool, vkNull}: return okVoid()
  of ctFloat64:
    if value.kind in {vkFloat64, vkInt64, vkNull}: return okVoid()
  of ctText:
    if value.kind in {vkText, vkNull}: return okVoid()
  of ctBlob:
    if value.kind in {vkBlob, vkNull}: return okVoid()
  err[Void](ERR_SQL, "Type mismatch")

proc valuesEqual(a: Value, b: Value): bool =
  if a.kind == vkNull and b.kind == vkNull:
    return true
  if a.kind != b.kind:
    return false
  case a.kind
  of vkInt64:
    a.int64Val == b.int64Val
  of vkBool:
    a.boolVal == b.boolVal
  of vkFloat64:
    a.float64Val == b.float64Val
  of vkText, vkBlob:
    a.bytes == b.bytes
  else:
    false

proc bytesToString(bytes: seq[byte]): string =
  result = newString(bytes.len)
  for i, b in bytes:
    result[i] = char(b)

proc valueBytesKey(value: Value): string =
  ## Stable key for exact equality of TEXT/BLOB values.
  case value.kind
  of vkText, vkBlob:
    bytesToString(value.bytes)
  else:
    ""

proc indexHasAnyMatchingValue(
  catalog: Catalog,
  pager: Pager,
  table: TableMeta,
  columnName: string,
  columnIndex: int,
  value: Value,
  excludeRowid: uint64 = 0
): Result[bool] =
  ## Safety net for hashed index keys (TEXT/BLOB):
  ## look up candidate rowids by hash key, then verify exact value bytes.
  let rowIdsRes = indexSeek(pager, catalog, table.name, columnName, value)
  if not rowIdsRes.ok:
    return err[bool](rowIdsRes.err.code, rowIdsRes.err.message, rowIdsRes.err.context)
  for rid in rowIdsRes.value:
    if excludeRowid != 0 and rid == excludeRowid:
      continue
    let rowRes = readRowAt(pager, table, rid)
    if rowRes.ok:
      if valuesEqual(rowRes.value.values[columnIndex], value):
        return ok(true)
    else:
      if rowRes.err.code != ERR_IO:
        return err[bool](rowRes.err.code, rowRes.err.message, rowRes.err.context)
  ok(false)

proc enforceNotNull(table: TableMeta, values: seq[Value]): Result[Void] =
  for i, col in table.columns:
    if col.notNull and values[i].kind == vkNull:
      return err[Void](ERR_CONSTRAINT, "NOT NULL constraint failed", table.name & "." & col.name)
  okVoid()

proc enforceUnique(catalog: Catalog, pager: Pager, table: TableMeta, values: seq[Value], rowid: uint64 = 0): Result[Void] =
  for i, col in table.columns:
    if col.unique or col.primaryKey:
      if values[i].kind == vkNull:
        continue
      if col.primaryKey and col.kind == ctInt64:
        if values[i].kind == vkInt64:
          let targetId = cast[uint64](values[i].int64Val)
          let rowRes = readRowAt(pager, table, targetId)
          if rowRes.ok:
            if rowid == 0 or rowid != targetId:
              return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & col.name)
          else:
            if rowRes.err.code != ERR_IO:
              return err[Void](rowRes.err.code, rowRes.err.message, rowRes.err.context)
          continue
      let idxOpt = catalog.getBtreeIndexForColumn(table.name, col.name)
      if isNone(idxOpt):
        return err[Void](ERR_INTERNAL, "Missing UNIQUE index", table.name & "." & col.name)
      if col.kind in {ctText, ctBlob} and values[i].kind in {vkText, vkBlob}:
        let anyRes = indexHasAnyMatchingValue(
          catalog, pager, table, col.name, i, values[i], excludeRowid = rowid
        )
        if not anyRes.ok:
          return err[Void](anyRes.err.code, anyRes.err.message, anyRes.err.context)
        if anyRes.value:
          return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & col.name)
      else:
        let key = indexKeyFromValue(values[i])
        if rowid == 0:
          let anyRes = indexHasAnyKey(pager, idxOpt.get, key)
          if not anyRes.ok:
            return err[Void](anyRes.err.code, anyRes.err.message, anyRes.err.context)
          if anyRes.value:
            return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & col.name)
        else:
          let otherRes = indexHasOtherRowid(pager, idxOpt.get, key, rowid)
          if not otherRes.ok:
            return err[Void](otherRes.err.code, otherRes.err.message, otherRes.err.context)
          if otherRes.value:
            return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & col.name)
  okVoid()

proc enforceForeignKeys(catalog: Catalog, pager: Pager, table: TableMeta, values: seq[Value]): Result[Void] =
  for i, col in table.columns:
    if col.refTable.len == 0 or col.refColumn.len == 0:
      continue
    if values[i].kind == vkNull:
      continue
    let idxOpt = catalog.getBtreeIndexForColumn(col.refTable, col.refColumn)
    if isNone(idxOpt):
      # Check if it is an optimized INT64 PRIMARY KEY
      let parentRes = catalog.getTable(col.refTable)
      if parentRes.ok:
        var isInt64Pk = false
        for pCol in parentRes.value.columns:
          if pCol.name == col.refColumn and pCol.primaryKey and pCol.kind == ctInt64:
            isInt64Pk = true
            break
        
        if isInt64Pk and values[i].kind == vkInt64:
           let targetId = cast[uint64](values[i].int64Val)
           let rowRes = readRowAt(pager, parentRes.value, targetId)
           if rowRes.ok:
             continue # Found
           elif rowRes.err.code == ERR_IO: # ERR_IO usually means not found in readRowAt from find()
             return err[Void](ERR_CONSTRAINT, "FOREIGN KEY constraint failed", table.name & "." & col.name)
           else:
             return err[Void](rowRes.err.code, rowRes.err.message, rowRes.err.context)

      return err[Void](ERR_INTERNAL, "Missing FK parent index", col.refTable & "." & col.refColumn)
    let parentRes = catalog.getTable(col.refTable)
    if not parentRes.ok:
      return err[Void](parentRes.err.code, parentRes.err.message, parentRes.err.context)
    var parentColIdx = -1
    for pIdx, pCol in parentRes.value.columns:
      if pCol.name == col.refColumn:
        parentColIdx = pIdx
        break
    if parentColIdx < 0:
      return err[Void](ERR_INTERNAL, "Missing FK parent column", col.refTable & "." & col.refColumn)
    if parentRes.value.columns[parentColIdx].kind in {ctText, ctBlob} and values[i].kind in {vkText, vkBlob}:
      let existsRes = indexHasAnyMatchingValue(
        catalog,
        pager,
        parentRes.value,
        col.refColumn,
        parentColIdx,
        values[i]
      )
      if not existsRes.ok:
        return err[Void](existsRes.err.code, existsRes.err.message, existsRes.err.context)
      if not existsRes.value:
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY constraint failed", table.name & "." & col.name)
    else:
      let key = indexKeyFromValue(values[i])
      let anyRes = indexHasAnyKey(pager, idxOpt.get, key)
      if not anyRes.ok:
        return err[Void](anyRes.err.code, anyRes.err.message, anyRes.err.context)
      if not anyRes.value:
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY constraint failed", table.name & "." & col.name)
  okVoid()

# ============================================================================
# HIGH-005: Batched Constraint Checking for Bulk Operations
# ============================================================================

type ConstraintBatchOptions* = object
  ## Options for batch constraint checking
  checkNotNull*: bool
  checkUnique*: bool
  checkForeignKeys*: bool
  skipInt64PkOptimization*: bool  # When true, always check via index even for INT64 PK

proc defaultConstraintBatchOptions*(): ConstraintBatchOptions =
  ## Return default options (all checks enabled)
  ConstraintBatchOptions(
    checkNotNull: true,
    checkUnique: true,
    checkForeignKeys: true,
    skipInt64PkOptimization: false
  )

proc enforceNotNullBatch*(table: TableMeta, rows: seq[seq[Value]]): Result[seq[int]] =
  ## Batch NOT NULL constraint checking for multiple rows.
  ## Returns the indices of rows that failed the check, or empty seq if all passed.
  ## This is more efficient than calling enforceNotNull for each row individually
  ## because it amortizes the loop overhead across multiple rows.
  var failedIndices: seq[int] = @[]
  
  for rowIdx, values in rows:
    if values.len != table.columns.len:
      return err[seq[int]](ERR_SQL, "Column count mismatch", $rowIdx)
    
    for i, col in table.columns:
      if col.notNull and values[i].kind == vkNull:
        failedIndices.add(rowIdx)
        break  # Only record once per row
  
  ok(failedIndices)

proc hasFailure(failures: seq[tuple[rowIdx: int, colName: string]], rowIdx: int, colName: string): bool =
  for f in failures:
    if f.rowIdx == rowIdx and f.colName == colName:
      return true
  false

proc enforceUniqueBatch*(
  catalog: Catalog, 
  pager: Pager, 
  table: TableMeta, 
  rows: seq[tuple[values: seq[Value], rowid: uint64]],
  options: ConstraintBatchOptions = defaultConstraintBatchOptions()
): Result[seq[tuple[rowIdx: int, colName: string]]] =
  ## Batch UNIQUE constraint checking for multiple rows.
  ## Returns the (row index, column name) pairs that failed the check.
  ## Uses bulk index lookups to reduce the number of index traversals.
  ## 
  ## This is HIGH-005: Constraint checking performance batching.
  ## 
  ## Optimization: Groups checks by column and uses sorted key ranges
  ## to minimize index cursor operations.
  
  var failures: seq[tuple[rowIdx: int, colName: string]] = @[]
  
  # Collect unique columns that need checking
  var uniqueCols: seq[tuple[colIdx: int, colName: string, isInt64Pk: bool, idxOpt: Option[IndexMeta]]] = @[]
  
  for i, col in table.columns:
    if col.unique or col.primaryKey:
      let isInt64Pk = col.primaryKey and col.kind == ctInt64
      var idxOpt: Option[IndexMeta] = none(IndexMeta)
      if not isInt64Pk or options.skipInt64PkOptimization:
        idxOpt = catalog.getBtreeIndexForColumn(table.name, col.name)
      uniqueCols.add((colIdx: i, colName: col.name, isInt64Pk: isInt64Pk, idxOpt: idxOpt))
  
  if uniqueCols.len == 0:
    return ok(failures)
  
  # For each unique column, collect all keys to check
  for colInfo in uniqueCols:
    if table.columns[colInfo.colIdx].kind in {ctText, ctBlob}:
      # TEXT/BLOB btree indexes are currently hash-keyed (CRC32C). To avoid
      # hash-collision false positives, verify exact values by reading rows.
      if isNone(colInfo.idxOpt):
        return err[seq[tuple[rowIdx: int, colName: string]]](
          ERR_INTERNAL, "Missing UNIQUE index", table.name & "." & colInfo.colName
        )

      var byValue: Table[string, tuple[value: Value, rowid: uint64, rowIdxs: seq[int]]] =
        initTable[string, tuple[value: Value, rowid: uint64, rowIdxs: seq[int]]]()

      for rowIdx, rowData in rows:
        let values = rowData.values
        if values[colInfo.colIdx].kind == vkNull:
          continue
        if values[colInfo.colIdx].kind notin {vkText, vkBlob}:
          continue

        let k = valueBytesKey(values[colInfo.colIdx])
        if byValue.hasKey(k):
          failures.add((rowIdx: rowIdx, colName: colInfo.colName))
          byValue[k].rowIdxs.add(rowIdx)
        else:
          byValue[k] = (value: values[colInfo.colIdx], rowid: rowData.rowid, rowIdxs: @[rowIdx])

      for k, info in byValue.pairs:
        let existsRes = indexHasAnyMatchingValue(
          catalog,
          pager,
          table,
          colInfo.colName,
          colInfo.colIdx,
          info.value,
          excludeRowid = info.rowid
        )
        if not existsRes.ok:
          return err[seq[tuple[rowIdx: int, colName: string]]](
            existsRes.err.code, existsRes.err.message, existsRes.err.context
          )
        if existsRes.value:
          for ridx in info.rowIdxs:
            let colName = colInfo.colName
            if not hasFailure(failures, ridx, colName):
              failures.add((rowIdx: ridx, colName: colName))

      continue

    var keysToCheck: seq[tuple[rowIdx: int, key: uint64, rowid: uint64]] = @[]
    
    # Collect all non-null keys for this column from all rows
    for rowIdx, rowData in rows:
      let values = rowData.values
      if values[colInfo.colIdx].kind == vkNull:
        continue
      
      # For INT64 PK optimization, check via direct row lookup
      if colInfo.isInt64Pk and not options.skipInt64PkOptimization:
        if values[colInfo.colIdx].kind == vkInt64:
          let targetId = cast[uint64](values[colInfo.colIdx].int64Val)
          let rowRes = readRowAt(pager, table, targetId)
          if rowRes.ok:
            if rowData.rowid == 0 or rowData.rowid != targetId:
              failures.add((rowIdx: rowIdx, colName: colInfo.colName))
          elif rowRes.err.code != ERR_IO:
            return err[seq[tuple[rowIdx: int, colName: string]]](
              rowRes.err.code, rowRes.err.message, rowRes.err.context
            )
        continue
      
      # For index-based unique columns
      if isNone(colInfo.idxOpt):
        return err[seq[tuple[rowIdx: int, colName: string]]](
          ERR_INTERNAL, "Missing UNIQUE index", table.name & "." & colInfo.colName
        )
      
      let key = indexKeyFromValue(values[colInfo.colIdx])
      keysToCheck.add((rowIdx: rowIdx, key: key, rowid: rowData.rowid))
    
    # Sort keys to enable range scan efficiency
    if keysToCheck.len > 1:
      keysToCheck.sort(proc(a, b: tuple[rowIdx: int, key: uint64, rowid: uint64]): int =
        if a.key < b.key: return -1
        if a.key > b.key: return 1
        return 0
      )
    
    # Check for duplicates within the batch first (more efficient)
    if keysToCheck.len > 1:
      for i in 1 ..< keysToCheck.len:
        if keysToCheck[i].key == keysToCheck[i-1].key:
          # Two rows in this batch have the same key - violation
          let colName = colInfo.colName
          if not failures.any(proc(f: tuple[rowIdx: int, colName: string]): bool =
            f.rowIdx == keysToCheck[i].rowIdx and f.colName == colName):
            failures.add((rowIdx: keysToCheck[i].rowIdx, colName: colInfo.colName))
    
    # Now check against existing database via index
    var prevKey: Option[uint64] = none(uint64)
    for keyInfo in keysToCheck:
      # Skip if we already found this key in the batch
      if prevKey.isSome and prevKey.get == keyInfo.key:
        continue
      prevKey = some(keyInfo.key)
      
      let idx = colInfo.idxOpt.get
      if keyInfo.rowid == 0:
        # Insert case: any existing key is a violation
        let anyRes = indexHasAnyKey(pager, idx, keyInfo.key)
        if not anyRes.ok:
          return err[seq[tuple[rowIdx: int, colName: string]]](
            anyRes.err.code, anyRes.err.message, anyRes.err.context
          )
        if anyRes.value:
          let colName = colInfo.colName
          let keyInfoRowIdx = keyInfo.rowIdx
          if not failures.any(proc(f: tuple[rowIdx: int, colName: string]): bool =
            f.rowIdx == keyInfoRowIdx and f.colName == colName):
            failures.add((rowIdx: keyInfoRowIdx, colName: colName))
      else:
        # Update case: only other rowids with this key are violations
        let otherRes = indexHasOtherRowid(pager, idx, keyInfo.key, keyInfo.rowid)
        if not otherRes.ok:
          return err[seq[tuple[rowIdx: int, colName: string]]](
            otherRes.err.code, otherRes.err.message, otherRes.err.context
          )
        if otherRes.value:
          let colName = colInfo.colName
          let keyInfoRowIdx = keyInfo.rowIdx
          if not failures.any(proc(f: tuple[rowIdx: int, colName: string]): bool =
            f.rowIdx == keyInfoRowIdx and f.colName == colName):
            failures.add((rowIdx: keyInfoRowIdx, colName: colName))
  
  ok(failures)

proc enforceForeignKeysBatch*(
  catalog: Catalog, 
  pager: Pager, 
  table: TableMeta, 
  rows: seq[seq[Value]],
  options: ConstraintBatchOptions = defaultConstraintBatchOptions()
): Result[seq[tuple[rowIdx: int, colName: string]]] =
  ## Batch FOREIGN KEY constraint checking for multiple rows.
  ## Returns the (row index, column name) pairs that failed the check.
  ## 
  ## This is HIGH-005: Constraint checking performance batching.
  ## 
  ## Optimization: Groups FK checks by referenced table/column and uses
  ## bulk index lookups to minimize the number of index traversals.
  
  var failures: seq[tuple[rowIdx: int, colName: string]] = @[]
  
  # Collect FK columns grouped by referenced table.column for efficiency
  type FkRef = tuple[refTable: string, refColumn: string]
  var fkGroups: Table[FkRef, seq[tuple[rowIdx: int, colIdx: int, colName: string]]] = initTable[FkRef, seq[tuple[rowIdx: int, colIdx: int, colName: string]]]()
  var int64PkCols: seq[tuple[rowIdx: int, colIdx: int, colName: string, targetId: uint64, parentTable: TableMeta]] = @[]
  
  # First pass: group all FK checks by their referenced table/column
  for rowIdx, values in rows:
    if values.len != table.columns.len:
      return err[seq[tuple[rowIdx: int, colName: string]]](ERR_SQL, "Column count mismatch", $rowIdx)
    
    for i, col in table.columns:
      if col.refTable.len == 0 or col.refColumn.len == 0:
        continue
      if values[i].kind == vkNull:
        continue
      
      let refKey: FkRef = (refTable: col.refTable, refColumn: col.refColumn)
      if not fkGroups.hasKey(refKey):
        fkGroups[refKey] = @[]
      fkGroups[refKey].add((rowIdx: rowIdx, colIdx: i, colName: col.name))
  
  # Process each FK group
  for refKey, refs in fkGroups.pairs:
    let idxOpt = catalog.getBtreeIndexForColumn(refKey.refTable, refKey.refColumn)

    let parentRes = catalog.getTable(refKey.refTable)
    if not parentRes.ok:
      return err[seq[tuple[rowIdx: int, colName: string]]](
        parentRes.err.code, parentRes.err.message, parentRes.err.context
      )
    let parentTable = parentRes.value
    var parentColIdx = -1
    for i, col in parentTable.columns:
      if col.name == refKey.refColumn:
        parentColIdx = i
        break
    if parentColIdx < 0:
      return err[seq[tuple[rowIdx: int, colName: string]]](
        ERR_INTERNAL, "Missing FK parent column", refKey.refTable & "." & refKey.refColumn
      )
    
    if isNone(idxOpt):
      # Check if it's an optimized INT64 PRIMARY KEY
      var isInt64Pk = false
      for pCol in parentTable.columns:
        if pCol.name == refKey.refColumn and pCol.primaryKey and pCol.kind == ctInt64:
          isInt64Pk = true
          break
      
      if isInt64Pk and not options.skipInt64PkOptimization:
        # Collect INT64 PK references for batch lookup
        for fkRef in refs:
          let values = rows[fkRef.rowIdx]
          if values[fkRef.colIdx].kind == vkInt64:
            let targetId = cast[uint64](values[fkRef.colIdx].int64Val)
            int64PkCols.add((
              rowIdx: fkRef.rowIdx, 
              colIdx: fkRef.colIdx, 
              colName: fkRef.colName,
              targetId: targetId,
              parentTable: parentTable
            ))
        continue
      
      return err[seq[tuple[rowIdx: int, colName: string]]](
        ERR_INTERNAL, "Missing FK parent index", refKey.refTable & "." & refKey.refColumn
      )

    if parentTable.columns[parentColIdx].kind in {ctText, ctBlob}:
      # Hash-keyed TEXT/BLOB parent index: verify exact values.
      var byVal: Table[string, tuple[value: Value, refs: seq[tuple[rowIdx: int, colName: string]]]] =
        initTable[string, tuple[value: Value, refs: seq[tuple[rowIdx: int, colName: string]]]]()

      for fkRef in refs:
        let values = rows[fkRef.rowIdx]
        if values[fkRef.colIdx].kind notin {vkText, vkBlob}:
          continue
        let k = valueBytesKey(values[fkRef.colIdx])
        if not byVal.hasKey(k):
          byVal[k] = (value: values[fkRef.colIdx], refs: @[])
        byVal[k].refs.add((rowIdx: fkRef.rowIdx, colName: fkRef.colName))

      for k, info in byVal.pairs:
        let existsRes = indexHasAnyMatchingValue(
          catalog,
          pager,
          parentTable,
          refKey.refColumn,
          parentColIdx,
          info.value
        )
        if not existsRes.ok:
          return err[seq[tuple[rowIdx: int, colName: string]]](
            existsRes.err.code, existsRes.err.message, existsRes.err.context
          )
        if not existsRes.value:
          for keyRef in info.refs:
            let keyRefRowIdx = keyRef.rowIdx
            let keyRefColName = keyRef.colName
            if not hasFailure(failures, keyRefRowIdx, keyRefColName):
              failures.add((rowIdx: keyRefRowIdx, colName: keyRefColName))
      continue
    
    # Collect all unique keys to check for this index
    var uniqueKeys: seq[uint64] = @[]
    var keyToRefs: Table[uint64, seq[tuple[rowIdx: int, colName: string]]] = initTable[uint64, seq[tuple[rowIdx: int, colName: string]]]()
    
    for fkRef in refs:
      let values = rows[fkRef.rowIdx]
      let key = indexKeyFromValue(values[fkRef.colIdx])
      
      if not keyToRefs.hasKey(key):
        keyToRefs[key] = @[]
        uniqueKeys.add(key)
      keyToRefs[key].add((rowIdx: fkRef.rowIdx, colName: fkRef.colName))
    
    # Sort keys for efficient range scanning
    uniqueKeys.sort()
    
    # Batch check all unique keys against the index
    for key in uniqueKeys:
      let anyRes = indexHasAnyKey(pager, idxOpt.get, key)
      if not anyRes.ok:
        return err[seq[tuple[rowIdx: int, colName: string]]](
          anyRes.err.code, anyRes.err.message, anyRes.err.context
        )
      
      if not anyRes.value:
        # Key doesn't exist in parent - all references to this key fail
        for keyRef in keyToRefs[key]:
          let keyRefRowIdx = keyRef.rowIdx
          let keyRefColName = keyRef.colName
          if not failures.any(proc(f: tuple[rowIdx: int, colName: string]): bool =
            f.rowIdx == keyRefRowIdx and f.colName == keyRefColName):
            failures.add((rowIdx: keyRefRowIdx, colName: keyRefColName))
  
  # Process INT64 PK references in batch
  for pkRef in int64PkCols:
    let rowRes = readRowAt(pager, pkRef.parentTable, pkRef.targetId)
    if not rowRes.ok:
      if rowRes.err.code == ERR_IO:
        # Not found - FK violation
        let pkRefRowIdx = pkRef.rowIdx
        let pkRefColName = pkRef.colName
        if not failures.any(proc(f: tuple[rowIdx: int, colName: string]): bool =
          f.rowIdx == pkRefRowIdx and f.colName == pkRefColName):
          failures.add((rowIdx: pkRefRowIdx, colName: pkRefColName))
      else:
        return err[seq[tuple[rowIdx: int, colName: string]]](
          rowRes.err.code, rowRes.err.message, rowRes.err.context
        )
  
  ok(failures)

proc enforceConstraintsBatch*(
  catalog: Catalog,
  pager: Pager,
  table: TableMeta,
  rows: seq[tuple[values: seq[Value], rowid: uint64]],
  options: ConstraintBatchOptions = defaultConstraintBatchOptions()
): Result[seq[tuple[rowIdx: int, constraint: string, details: string]]] =
  ## Comprehensive batch constraint checking combining NOT NULL, UNIQUE, and FK checks.
  ## Returns detailed error information for each failure.
  ## 
  ## This is the main entry point for HIGH-005 batch constraint checking.
  ## Use this for bulk insert/update operations to reduce index lookup overhead.
  
  var allFailures: seq[tuple[rowIdx: int, constraint: string, details: string]] = @[]
  
  if options.checkNotNull:
    # Extract just the values for NOT NULL check
    var allValues: seq[seq[Value]] = @[]
    for row in rows:
      allValues.add(row.values)
    
    let notNullRes = enforceNotNullBatch(table, allValues)
    if not notNullRes.ok:
      return err[seq[tuple[rowIdx: int, constraint: string, details: string]]](
        notNullRes.err.code, notNullRes.err.message, notNullRes.err.context
      )
    
    for rowIdx in notNullRes.value:
      # Find the first NOT NULL column that failed
      for i, col in table.columns:
        if col.notNull and allValues[rowIdx][i].kind == vkNull:
          allFailures.add((
            rowIdx: rowIdx, 
            constraint: "NOT NULL", 
            details: table.name & "." & col.name
          ))
          break
  
  if options.checkUnique:
    let uniqueRes = enforceUniqueBatch(catalog, pager, table, rows, options)
    if not uniqueRes.ok:
      return err[seq[tuple[rowIdx: int, constraint: string, details: string]]](
        uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context
      )
    
    for failure in uniqueRes.value:
      let failureRowIdx = failure.rowIdx
      let failureColName = failure.colName
      if not allFailures.any(proc(f: tuple[rowIdx: int, constraint: string, details: string]): bool =
        f.rowIdx == failureRowIdx and f.constraint == "UNIQUE"):
        allFailures.add((
          rowIdx: failureRowIdx, 
          constraint: "UNIQUE", 
          details: failureColName
        ))
  
  if options.checkForeignKeys:
    var allValues: seq[seq[Value]] = @[]
    for row in rows:
      allValues.add(row.values)
    
    let fkRes = enforceForeignKeysBatch(catalog, pager, table, allValues, options)
    if not fkRes.ok:
      return err[seq[tuple[rowIdx: int, constraint: string, details: string]]](
        fkRes.err.code, fkRes.err.message, fkRes.err.context
      )
    
    for failure in fkRes.value:
      let failureRowIdx = failure.rowIdx
      let failureColName = failure.colName
      if not allFailures.any(proc(f: tuple[rowIdx: int, constraint: string, details: string]): bool =
        f.rowIdx == failureRowIdx and f.constraint == "FOREIGN KEY"):
        allFailures.add((
          rowIdx: failureRowIdx, 
          constraint: "FOREIGN KEY", 
          details: failureColName
        ))
  
  ok(allFailures)

proc referencingChildren(catalog: Catalog, table: string, column: string): seq[(string, string)] =
  for _, meta in catalog.tables:
    for col in meta.columns:
      if col.refTable == table and col.refColumn == column:
        result.add((meta.name, col.name))

proc enforceRestrictOnParent(catalog: Catalog, pager: Pager, table: TableMeta, oldValues: seq[Value], newValues: seq[Value]): Result[Void] =
  for i, col in table.columns:
    let children = referencingChildren(catalog, table.name, col.name)
    if children.len == 0:
      continue
    let oldVal = oldValues[i]
    let newVal = newValues[i]
    if valuesEqual(oldVal, newVal):
      continue
    if oldVal.kind == vkNull:
      continue
    for child in children:
      let idxOpt = catalog.getBtreeIndexForColumn(child[0], child[1])
      if isNone(idxOpt):
        # Check if the referencing column is an optimized INT64 PRIMARY KEY.
        let childTableRes = catalog.getTable(child[0])
        if childTableRes.ok:
          var isInt64Pk = false
          for cCol in childTableRes.value.columns:
            if cCol.name == child[1] and cCol.primaryKey and cCol.kind == ctInt64:
              isInt64Pk = true
              break
          if isInt64Pk:
            if oldVal.kind == vkInt64:
              let targetId = cast[uint64](oldVal.int64Val)
              let rowRes = readRowAt(pager, childTableRes.value, targetId)
              if rowRes.ok:
                return err[Void](ERR_CONSTRAINT, "FOREIGN KEY RESTRICT violation", table.name & "." & col.name)
              elif rowRes.err.code != ERR_IO:
                return err[Void](rowRes.err.code, rowRes.err.message, rowRes.err.context)
              continue

        return err[Void](ERR_INTERNAL, "Missing FK child index", child[0] & "." & child[1])
      let key = indexKeyFromValue(oldVal)
      let anyRes = indexHasAnyKey(pager, idxOpt.get, key)
      if not anyRes.ok:
        return err[Void](anyRes.err.code, anyRes.err.message, anyRes.err.context)
      if anyRes.value:
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY RESTRICT violation", table.name & "." & col.name)
  okVoid()

proc enforceRestrictOnDelete(catalog: Catalog, pager: Pager, table: TableMeta, oldValues: seq[Value]): Result[Void] =
  for i, col in table.columns:
    let children = referencingChildren(catalog, table.name, col.name)
    if children.len == 0:
      continue
    let oldVal = oldValues[i]
    if oldVal.kind == vkNull:
      continue
    for child in children:
      let idxOpt = catalog.getBtreeIndexForColumn(child[0], child[1])
      if isNone(idxOpt):
        # Check if the referencing column is an optimized INT64 PRIMARY KEY
        let childTableRes = catalog.getTable(child[0])
        if childTableRes.ok:
           var isInt64Pk = false
           for cCol in childTableRes.value.columns:
             if cCol.name == child[1] and cCol.primaryKey and cCol.kind == ctInt64:
               isInt64Pk = true
               break
           if isInt64Pk:
              if oldVal.kind == vkInt64:
                let targetId = cast[uint64](oldVal.int64Val)
                let rowRes = readRowAt(pager, childTableRes.value, targetId)
                if rowRes.ok:
                   return err[Void](ERR_CONSTRAINT, "FOREIGN KEY RESTRICT violation", table.name & "." & col.name)
                elif rowRes.err.code != ERR_IO:
                   return err[Void](rowRes.err.code, rowRes.err.message, rowRes.err.context)
                continue

        return err[Void](ERR_INTERNAL, "Missing FK child index", child[0] & "." & child[1])
      let key = indexKeyFromValue(oldVal)
      let anyRes = indexHasAnyKey(pager, idxOpt.get, key)
      if not anyRes.ok:
        return err[Void](anyRes.err.code, anyRes.err.message, anyRes.err.context)
      if anyRes.value:
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY RESTRICT violation", table.name & "." & col.name)
  okVoid()

proc evalInsertValues(stmt: Statement, params: seq[Value]): Result[seq[Value]] =
  var values: seq[Value] = @[]
  for expr in stmt.insertValues:
    let res = evalExpr(Row(), expr, params)
    if not res.ok:
      return err[seq[Value]](res.err.code, res.err.message, res.err.context)
    values.add(valueFromSql(res.value))
  ok(values)

# Forward declarations for transaction control (defined later)
proc beginTransaction*(db: Db): Result[Void]
proc commitTransaction*(db: Db): Result[Void]
proc rollbackTransaction*(db: Db): Result[Void]

proc execSql*(db: Db, sqlText: string, params: seq[Value]): Result[seq[string]] =
  if not db.isOpen:
    return err[seq[string]](ERR_INTERNAL, "Database not open")
  var output: seq[string] = @[]
  const SqlCacheMaxEntries = 128

  proc touchSqlCache(key: string) =
    # Fast path: if key is already at the end (most recent), skip
    if db.sqlCacheOrder.len > 0 and db.sqlCacheOrder[^1] == key:
      return
    var idx = -1
    for i, existing in db.sqlCacheOrder:
      if existing == key:
        idx = i
        break
    if idx >= 0:
      db.sqlCacheOrder.delete(idx)
    db.sqlCacheOrder.add(key)

  proc rememberSqlCache(key: string, statements: seq[Statement], plans: seq[Plan]) =
    db.sqlCache[key] = (schemaCookie: db.schemaCookie, statements: statements, plans: plans)
    touchSqlCache(key)
    while db.sqlCacheOrder.len > SqlCacheMaxEntries:
      let evictKey = db.sqlCacheOrder[0]
      db.sqlCacheOrder.delete(0)
      if db.sqlCache.hasKey(evictKey):
        db.sqlCache.del(evictKey)

  var boundStatements: seq[Statement] = @[]
  var cachedPlans: seq[Plan] = @[]
  if db.sqlCache.hasKey(sqlText):
    let cached = db.sqlCache[sqlText]
    if cached.schemaCookie == db.schemaCookie:
      boundStatements = cached.statements
      cachedPlans = cached.plans
      touchSqlCache(sqlText)

  if boundStatements.len == 0:
    let parseRes = parseSql(sqlText)
    if not parseRes.ok:
      return err[seq[string]](parseRes.err.code, parseRes.err.message, parseRes.err.context)
    for stmt in parseRes.value.statements:
      let bindRes = bindStatement(db.catalog, stmt)
      if not bindRes.ok:
        return err[seq[string]](bindRes.err.code, bindRes.err.message, bindRes.err.context)
      boundStatements.add(bindRes.value)
    for bound in boundStatements:
      if bound.kind == skSelect:
        let planRes = plan(db.catalog, bound)
        if not planRes.ok:
          return err[seq[string]](planRes.err.code, planRes.err.message, planRes.err.context)
        cachedPlans.add(planRes.value)
      elif bound.kind == skExplain:
        # For EXPLAIN, we plan the inner statement if it is a SELECT.
        # If it's not a SELECT, we'll error at execution time, so we store nil here.
        if bound.explainInner.kind == skSelect:
          let planRes = plan(db.catalog, bound.explainInner)
          if not planRes.ok:
            return err[seq[string]](planRes.err.code, planRes.err.message, planRes.err.context)
          cachedPlans.add(planRes.value)
        else:
          cachedPlans.add(nil)
      else:
        cachedPlans.add(nil)
    rememberSqlCache(sqlText, boundStatements, cachedPlans)
  proc findMatchingRowids(tableName: string, whereExpr: Expr): Result[seq[uint64]] =
    # Use the planner to execute a general WHERE clause, leveraging indexes when possible.
    # This converts the WHERE clause into a SELECT-like plan and executes it to find rowids.
    if whereExpr == nil:
      # No WHERE clause means all rows; scan the full table
      let tableRes = db.catalog.getTable(tableName)
      if not tableRes.ok:
        return err[seq[uint64]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let rowsRes = scanTable(db.pager, tableRes.value)
      if not rowsRes.ok:
        return err[seq[uint64]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
      var rowids: seq[uint64] = @[]
      for row in rowsRes.value:
        rowids.add(row.rowid)
      return ok(rowids)
    
    # Get table metadata to build proper select items
    let tableRes = db.catalog.getTable(tableName)
    if not tableRes.ok:
      return err[seq[uint64]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value
    
    # Create SELECT items for all columns (needed for proper row construction)
    var selectItems: seq[SelectItem] = @[]
    for col in table.columns:
      selectItems.add(SelectItem(expr: Expr(kind: ekColumn, name: col.name, table: tableName)))
    
    # Create a synthetic SELECT statement that the planner can optimize
    let selectStmt = Statement(
      kind: skSelect,
      fromTable: tableName,
      fromAlias: "",
      selectItems: selectItems,
      whereExpr: whereExpr,
      joins: @[],
      groupBy: @[],
      havingExpr: nil,
      orderBy: @[],
      limit: -1,
      offset: -1
    )
    
    # Plan and execute to get matching rows
    let planRes = plan(db.catalog, selectStmt)
    if not planRes.ok:
      return err[seq[uint64]](planRes.err.code, planRes.err.message, planRes.err.context)
    
    let rowsRes = execPlan(db.pager, db.catalog, planRes.value, params)
    if not rowsRes.ok:
      return err[seq[uint64]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
    
    var rowids: seq[uint64] = @[]
    for row in rowsRes.value:
      rowids.add(row.rowid)
    ok(rowids)

  proc runSelect(bound: Statement, cachedPlan: Plan): Result[seq[string]] =
    let usePlan =
      if cachedPlan != nil:
        cachedPlan
      else:
        let planRes = plan(db.catalog, bound)
        if not planRes.ok:
          return err[seq[string]](planRes.err.code, planRes.err.message, planRes.err.context)
        planRes.value
    if db.wal == nil or db.activeWriter != nil or not db.walOverlayEnabled:
      let rowsRes = execPlan(db.pager, db.catalog, usePlan, params)
      if not rowsRes.ok:
        return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
      for row in rowsRes.value:
        var parts: seq[string] = @[]
        for value in row.values:
          parts.add(valueToString(value))
        output.add(parts.join("|"))
      return ok(output)
    let txn = beginRead(db.wal)
    db.pager.overlaySnapshot = txn.snapshot
    db.pager.setReadGuard(proc(): Result[Void] =
      if db.wal.isAborted(txn):
        return err[Void](ERR_TRANSACTION, "Read transaction aborted (timeout)")
      okVoid()
    )
    defer:
      db.pager.overlaySnapshot = 0
      db.pager.clearReadGuard()
      endRead(db.wal, txn)
    let rowsRes = execPlan(db.pager, db.catalog, usePlan, params)
    if not rowsRes.ok:
      return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
    for row in rowsRes.value:
      var parts: seq[string] = @[]
      for value in row.values:
        parts.add(valueToString(value))
      output.add(parts.join("|"))
    ok(output)
  for i, bound in boundStatements:
    let isWrite = bound.kind in {skCreateTable, skDropTable, skAlterTable, skCreateIndex, skDropIndex, skInsert, skUpdate, skDelete}
    # stderr.writeLine("execSql: i=" & $i & " kind=" & $bound.kind & " isWrite=" & $isWrite)
    var autoCommit = false
    if isWrite and db.activeWriter == nil and db.wal != nil:
      let beginRes = beginTransaction(db)
      if not beginRes.ok:
        return err[seq[string]](beginRes.err.code, beginRes.err.message, beginRes.err.context)
      autoCommit = true
    # stderr.writeLine("execSql: autoCommit=" & $autoCommit)
    var committed = false
    defer:
      if autoCommit and not committed:
        discard rollbackTransaction(db)
    case bound.kind
    of skCreateTable:
      let rootRes = initTableRoot(db.pager)
      if not rootRes.ok:
        return err[seq[string]](rootRes.err.code, rootRes.err.message, rootRes.err.context)
      var columns: seq[Column] = @[]
      for col in bound.columns:
        let typeRes = parseColumnType(col.typeName)
        if not typeRes.ok:
          return err[seq[string]](typeRes.err.code, typeRes.err.message, typeRes.err.context)
        columns.add(Column(name: col.name, kind: typeRes.value, notNull: col.notNull, unique: col.unique, primaryKey: col.primaryKey, refTable: col.refTable, refColumn: col.refColumn))
      let meta = TableMeta(name: bound.createTableName, rootPage: rootRes.value, nextRowId: 1, columns: columns)
      let saveRes = db.catalog.saveTable(db.pager, meta)
      if not saveRes.ok:
        return err[seq[string]](saveRes.err.code, saveRes.err.message, saveRes.err.context)
      for col in columns:
        if col.primaryKey or col.unique:
          if col.primaryKey and col.kind == ctInt64:
            continue
          let idxName = if col.primaryKey: "pk_" & meta.name & "_" & col.name & "_idx" else: "uniq_" & meta.name & "_" & col.name & "_idx"
          if isNone(db.catalog.getIndexForColumn(meta.name, col.name, catalog.ikBtree, requireUnique = true)):
            let idxRootRes = initTableRoot(db.pager)
            if not idxRootRes.ok:
              return err[seq[string]](idxRootRes.err.code, idxRootRes.err.message, idxRootRes.err.context)
            let buildRes = buildIndexForColumn(db.pager, db.catalog, meta.name, col.name, idxRootRes.value)
            if not buildRes.ok:
              return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
            let idxMeta = IndexMeta(name: idxName, table: meta.name, column: col.name, rootPage: buildRes.value, kind: catalog.ikBtree, unique: true)
            let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
            if not idxSaveRes.ok:
              return err[seq[string]](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
        if col.refTable.len > 0 and col.refColumn.len > 0:
          if isNone(db.catalog.getBtreeIndexForColumn(meta.name, col.name)):
            let idxName = "fk_" & meta.name & "_" & col.name & "_idx"
            let idxRootRes = initTableRoot(db.pager)
            if not idxRootRes.ok:
              return err[seq[string]](idxRootRes.err.code, idxRootRes.err.message, idxRootRes.err.context)
            let buildRes = buildIndexForColumn(db.pager, db.catalog, meta.name, col.name, idxRootRes.value)
            if not buildRes.ok:
              return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
            let idxMeta = IndexMeta(name: idxName, table: meta.name, column: col.name, rootPage: buildRes.value, kind: catalog.ikBtree, unique: false)
            let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
            if not idxSaveRes.ok:
              return err[seq[string]](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skDropTable:
      var toDrop: seq[string] = @[]
      for name, idx in db.catalog.indexes:
        if idx.table == bound.dropTableName:
          toDrop.add(name)
      for idxName in toDrop:
        discard db.catalog.dropIndex(idxName)
      let dropRes = db.catalog.dropTable(bound.dropTableName)
      if not dropRes.ok:
        return err[seq[string]](dropRes.err.code, dropRes.err.message, dropRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skAlterTable:
      let alterRes = alterTable(db.pager, db.catalog, bound.alterTableName, bound.alterActions)
      if not alterRes.ok:
        return err[seq[string]](alterRes.err.code, alterRes.err.message, alterRes.err.context)
    of skCreateIndex:
      let indexRootRes = initTableRoot(db.pager)
      if not indexRootRes.ok:
        return err[seq[string]](indexRootRes.err.code, indexRootRes.err.message, indexRootRes.err.context)
      if bound.unique:
        let tableRes = db.catalog.getTable(bound.indexTableName)
        if not tableRes.ok:
          return err[seq[string]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
        let table = tableRes.value
        var colIndex = -1
        for i, col in table.columns:
          if col.name == bound.columnName:
            colIndex = i
            break
        if colIndex < 0:
          return err[seq[string]](ERR_SQL, "Column not found", bound.columnName)
        let rowsRes = scanTable(db.pager, table)
        if not rowsRes.ok:
          return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
        let colKind = table.columns[colIndex].kind
        var seen: Table[uint64, bool] = initTable[uint64, bool]()
        var seenVals: Table[uint64, seq[string]] = initTable[uint64, seq[string]]()
        for row in rowsRes.value:
          if row.values[colIndex].kind == vkNull:
            continue
          let key = indexKeyFromValue(row.values[colIndex])
          if colKind in {ctText, ctBlob} and row.values[colIndex].kind in {vkText, vkBlob}:
            let vKey = valueBytesKey(row.values[colIndex])
            if not seenVals.hasKey(key):
              seenVals[key] = @[vKey]
            else:
              if vKey in seenVals[key]:
                return err[seq[string]](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnName)
              seenVals[key].add(vKey)
          else:
            if seen.hasKey(key):
              return err[seq[string]](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnName)
            seen[key] = true
      var finalRoot = indexRootRes.value
      if bound.indexKind == sql.ikTrigram:
        let buildRes = buildTrigramIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnName, indexRootRes.value)
        if not buildRes.ok:
          return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
        finalRoot = buildRes.value
      else:
        let buildRes = buildIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnName, indexRootRes.value)
        if not buildRes.ok:
          return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
        finalRoot = buildRes.value
      let idxKind = if bound.indexKind == sql.ikTrigram: catalog.ikTrigram else: catalog.ikBtree
      let idxMeta = IndexMeta(name: bound.indexName, table: bound.indexTableName, column: bound.columnName, rootPage: finalRoot, kind: idxKind, unique: bound.unique)
      let saveRes = db.catalog.createIndexMeta(idxMeta)
      if not saveRes.ok:
        return err[seq[string]](saveRes.err.code, saveRes.err.message, saveRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skDropIndex:
      let dropRes = db.catalog.dropIndex(bound.dropIndexName)
      if not dropRes.ok:
        return err[seq[string]](dropRes.err.code, dropRes.err.message, dropRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skInsert:
      let tableRes = db.catalog.getTable(bound.insertTable)
      if not tableRes.ok:
        return err[seq[string]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let valuesRes = evalInsertValues(bound, params)
      if not valuesRes.ok:
        return err[seq[string]](valuesRes.err.code, valuesRes.err.message, valuesRes.err.context)
      let values = valuesRes.value
      for i, col in tableRes.value.columns:
        let typeRes = typeCheckValue(col.kind, values[i])
        if not typeRes.ok:
          return err[seq[string]](typeRes.err.code, typeRes.err.message, col.name)
      let notNullRes = enforceNotNull(tableRes.value, values)
      if not notNullRes.ok:
        return err[seq[string]](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
      let uniqueRes = enforceUnique(db.catalog, db.pager, tableRes.value, values)
      if not uniqueRes.ok:
        return err[seq[string]](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
      let fkRes = enforceForeignKeys(db.catalog, db.pager, tableRes.value, values)
      if not fkRes.ok:
        return err[seq[string]](fkRes.err.code, fkRes.err.message, fkRes.err.context)
      let insertRes = insertRow(db.pager, db.catalog, bound.insertTable, values)
      if not insertRes.ok:
        return err[seq[string]](insertRes.err.code, insertRes.err.message, insertRes.err.context)
    of skUpdate:
      let tableRes = db.catalog.getTable(bound.updateTable)
      if not tableRes.ok:
        return err[seq[string]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let table = tableRes.value
      var updates: seq[(uint64, seq[Value], seq[Value])] = @[]
      var cols: seq[string] = @[]
      for col in table.columns:
        cols.add(bound.updateTable & "." & col.name)

      # Use planner to find matching rowids (leveraging indexes when possible)
      let rowidsRes = findMatchingRowids(bound.updateTable, bound.updateWhere)
      if not rowidsRes.ok:
        return err[seq[string]](rowidsRes.err.code, rowidsRes.err.message, rowidsRes.err.context)

      for rowid in rowidsRes.value:
        let storedRes = readRowAt(db.pager, table, rowid)
        if not storedRes.ok:
          continue
        let stored = storedRes.value
        let row = Row(rowid: stored.rowid, columns: cols, values: stored.values)
        var newValues = stored.values
        for colName, expr in bound.assignments:
          var idx = -1
          for i, col in table.columns:
            if col.name == colName:
              idx = i
              break
          if idx >= 0:
            let evalRes = evalExpr(row, expr, params)
            if not evalRes.ok:
              return err[seq[string]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
            let typeRes = typeCheckValue(table.columns[idx].kind, evalRes.value)
            if not typeRes.ok:
              return err[seq[string]](typeRes.err.code, typeRes.err.message, colName)
            newValues[idx] = evalRes.value
        updates.add((stored.rowid, stored.values, newValues))
      for entry in updates:
        let notNullRes = enforceNotNull(table, entry[2])
        if not notNullRes.ok:
          return err[seq[string]](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
        let uniqueRes = enforceUnique(db.catalog, db.pager, table, entry[2], entry[0])
        if not uniqueRes.ok:
          return err[seq[string]](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
        let fkRes = enforceForeignKeys(db.catalog, db.pager, table, entry[2])
        if not fkRes.ok:
          return err[seq[string]](fkRes.err.code, fkRes.err.message, fkRes.err.context)
        let restrictRes = enforceRestrictOnParent(db.catalog, db.pager, table, entry[1], entry[2])
        if not restrictRes.ok:
          return err[seq[string]](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)
      for entry in updates:
        let upRes = updateRow(db.pager, db.catalog, bound.updateTable, entry[0], entry[2])
        if not upRes.ok:
          return err[seq[string]](upRes.err.code, upRes.err.message, upRes.err.context)
    of skDelete:
      let tableRes = db.catalog.getTable(bound.deleteTable)
      if not tableRes.ok:
        return err[seq[string]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let table = tableRes.value
      var deletions: seq[StoredRow] = @[]

      # Use planner to find matching rowids (leveraging indexes when possible)
      let rowidsRes = findMatchingRowids(bound.deleteTable, bound.deleteWhere)
      if not rowidsRes.ok:
        return err[seq[string]](rowidsRes.err.code, rowidsRes.err.message, rowidsRes.err.context)

      for rowid in rowidsRes.value:
        let storedRes = readRowAt(db.pager, table, rowid)
        if not storedRes.ok:
          continue
        let stored = storedRes.value
        deletions.add(stored)
      for row in deletions:
        let restrictRes = enforceRestrictOnDelete(db.catalog, db.pager, table, row.values)
        if not restrictRes.ok:
          return err[seq[string]](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)
      for row in deletions:
        let delRes = deleteRow(db.pager, db.catalog, bound.deleteTable, row.rowid)
        if not delRes.ok:
          return err[seq[string]](delRes.err.code, delRes.err.message, delRes.err.context)
    of skExplain:
      if bound.explainInner.kind != skSelect:
        return err[seq[string]](ERR_SQL, "EXPLAIN currently supports SELECT only")
      var p = if i < cachedPlans.len: cachedPlans[i] else: nil
      if p == nil:
        let planRes = plan(db.catalog, bound.explainInner)
        if not planRes.ok:
          return err[seq[string]](planRes.err.code, planRes.err.message, planRes.err.context)
        p = planRes.value
      let lines = explainPlanLines(db.catalog, p)
      for line in lines:
        output.add(line)
    of skSelect:
      let selectRes = runSelect(bound, if i < cachedPlans.len: cachedPlans[i] else: nil)
      if not selectRes.ok:
        if autoCommit:
          discard rollbackTransaction(db)
        return selectRes
    of skBegin:
      let beginRes = beginTransaction(db)
      if not beginRes.ok:
        return err[seq[string]](beginRes.err.code, beginRes.err.message, beginRes.err.context)
    of skCommit:
      let commitRes = commitTransaction(db)
      if not commitRes.ok:
        return err[seq[string]](commitRes.err.code, commitRes.err.message, commitRes.err.context)
    of skRollback:
      let rollbackRes = rollbackTransaction(db)
      if not rollbackRes.ok:
        return err[seq[string]](rollbackRes.err.code, rollbackRes.err.message, rollbackRes.err.context)
    if autoCommit:
      let commitRes = commitTransaction(db)
      if not commitRes.ok:
        return err[seq[string]](commitRes.err.code, commitRes.err.message, commitRes.err.context)
      committed = true
  ok(output)

proc findMatchingRowidsPrepared(db: Db, tableName: string, whereExpr: Expr, params: seq[Value]): Result[seq[uint64]] =
  ## Match the execSql() helper, but usable from prepared APIs.
  ## Uses the planner so indexes can be leveraged.
  if whereExpr == nil:
    let tableRes = db.catalog.getTable(tableName)
    if not tableRes.ok:
      return err[seq[uint64]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let rowsRes = scanTable(db.pager, tableRes.value)
    if not rowsRes.ok:
      return err[seq[uint64]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
    var rowids: seq[uint64] = @[]
    for row in rowsRes.value:
      rowids.add(row.rowid)
    return ok(rowids)

  let tableRes = db.catalog.getTable(tableName)
  if not tableRes.ok:
    return err[seq[uint64]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  var selectItems: seq[SelectItem] = @[]
  for col in table.columns:
    selectItems.add(SelectItem(expr: Expr(kind: ekColumn, name: col.name, table: tableName)))
  let selectStmt = Statement(
    kind: skSelect,
    fromTable: tableName,
    fromAlias: "",
    selectItems: selectItems,
    whereExpr: whereExpr,
    joins: @[],
    groupBy: @[],
    havingExpr: nil,
    orderBy: @[],
    limit: -1,
    offset: -1
  )
  let planRes = plan(db.catalog, selectStmt)
  if not planRes.ok:
    return err[seq[uint64]](planRes.err.code, planRes.err.message, planRes.err.context)
  let rowsRes = execPlan(db.pager, db.catalog, planRes.value, params)
  if not rowsRes.ok:
    return err[seq[uint64]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
  var rowids: seq[uint64] = @[]
  for row in rowsRes.value:
    rowids.add(row.rowid)
  ok(rowids)

proc execPreparedNonSelect*(db: Db, bound: Statement, params: seq[Value]): Result[int64] =
  ## Execute a single already-bound non-SELECT statement and return rows affected.
  ## Intended for the native C ABI / Go driver.
  if not db.isOpen:
    return err[int64](ERR_INTERNAL, "Database not open")
  if bound.kind == skSelect:
    return err[int64](ERR_INTERNAL, "execPreparedNonSelect called with SELECT")

  let isWrite = bound.kind in {skCreateTable, skDropTable, skAlterTable, skCreateIndex, skDropIndex, skInsert, skUpdate, skDelete}
  var autoCommit = false
  if isWrite and db.activeWriter == nil and db.wal != nil:
    let beginRes = beginTransaction(db)
    if not beginRes.ok:
      return err[int64](beginRes.err.code, beginRes.err.message, beginRes.err.context)
    autoCommit = true
  var committed = false
  defer:
    if autoCommit and not committed:
      discard rollbackTransaction(db)

  var affected: int64 = 0
  case bound.kind
  of skCreateTable:
    let rootRes = initTableRoot(db.pager)
    if not rootRes.ok:
      return err[int64](rootRes.err.code, rootRes.err.message, rootRes.err.context)
    var columns: seq[Column] = @[]
    for col in bound.columns:
      let typeRes = parseColumnType(col.typeName)
      if not typeRes.ok:
        return err[int64](typeRes.err.code, typeRes.err.message, typeRes.err.context)
      columns.add(Column(name: col.name, kind: typeRes.value, notNull: col.notNull, unique: col.unique, primaryKey: col.primaryKey, refTable: col.refTable, refColumn: col.refColumn))
    let meta = TableMeta(name: bound.createTableName, rootPage: rootRes.value, nextRowId: 1, columns: columns)
    let saveRes = db.catalog.saveTable(db.pager, meta)
    if not saveRes.ok:
      return err[int64](saveRes.err.code, saveRes.err.message, saveRes.err.context)
    for col in columns:
      if col.primaryKey and col.kind == ctInt64: continue
      if col.primaryKey or col.unique:
        let idxName = if col.primaryKey: "pk_" & meta.name & "_" & col.name & "_idx" else: "uniq_" & meta.name & "_" & col.name & "_idx"
        if isNone(db.catalog.getIndexByName(idxName)):
          let idxRootRes = initTableRoot(db.pager)
          if not idxRootRes.ok:
            return err[int64](idxRootRes.err.code, idxRootRes.err.message, idxRootRes.err.context)
          let buildRes = buildIndexForColumn(db.pager, db.catalog, meta.name, col.name, idxRootRes.value)
          if not buildRes.ok:
            return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
          let idxMeta = IndexMeta(name: idxName, table: meta.name, column: col.name, rootPage: buildRes.value, kind: catalog.ikBtree, unique: true)
          let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
          if not idxSaveRes.ok:
            return err[int64](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
      if col.refTable.len > 0 and col.refColumn.len > 0:
        if isNone(db.catalog.getBtreeIndexForColumn(meta.name, col.name)):
          let idxName = "fk_" & meta.name & "_" & col.name & "_idx"
          let idxRootRes = initTableRoot(db.pager)
          if not idxRootRes.ok:
            return err[int64](idxRootRes.err.code, idxRootRes.err.message, idxRootRes.err.context)
          let buildRes = buildIndexForColumn(db.pager, db.catalog, meta.name, col.name, idxRootRes.value)
          if not buildRes.ok:
            return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
          let idxMeta = IndexMeta(name: idxName, table: meta.name, column: col.name, rootPage: buildRes.value, kind: catalog.ikBtree, unique: false)
          let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
          if not idxSaveRes.ok:
            return err[int64](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skDropTable:
    var toDrop: seq[string] = @[]
    for name, idx in db.catalog.indexes:
      if idx.table == bound.dropTableName:
        toDrop.add(name)
    for idxName in toDrop:
      discard db.catalog.dropIndex(idxName)
    let dropRes = db.catalog.dropTable(bound.dropTableName)
    if not dropRes.ok:
      return err[int64](dropRes.err.code, dropRes.err.message, dropRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skAlterTable:
    let alterRes = alterTable(db.pager, db.catalog, bound.alterTableName, bound.alterActions)
    if not alterRes.ok:
      return err[int64](alterRes.err.code, alterRes.err.message, alterRes.err.context)
    affected = 0

  of skCreateIndex:
    let indexRootRes = initTableRoot(db.pager)
    if not indexRootRes.ok:
      return err[int64](indexRootRes.err.code, indexRootRes.err.message, indexRootRes.err.context)
    if bound.unique:
      let tableRes = db.catalog.getTable(bound.indexTableName)
      if not tableRes.ok:
        return err[int64](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let table = tableRes.value
      var colIndex = -1
      for i, col in table.columns:
        if col.name == bound.columnName:
          colIndex = i
          break
      if colIndex < 0:
        return err[int64](ERR_SQL, "Column not found", bound.columnName)
      let rowsRes = scanTable(db.pager, table)
      if not rowsRes.ok:
        return err[int64](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
      let colKind = table.columns[colIndex].kind
      var seen: Table[uint64, bool] = initTable[uint64, bool]()
      var seenVals: Table[uint64, seq[string]] = initTable[uint64, seq[string]]()
      for row in rowsRes.value:
        if row.values[colIndex].kind == vkNull:
          continue
        let key = indexKeyFromValue(row.values[colIndex])
        if colKind in {ctText, ctBlob} and row.values[colIndex].kind in {vkText, vkBlob}:
          let vKey = valueBytesKey(row.values[colIndex])
          if not seenVals.hasKey(key):
            seenVals[key] = @[vKey]
          else:
            if vKey in seenVals[key]:
              return err[int64](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnName)
            seenVals[key].add(vKey)
        else:
          if seen.hasKey(key):
            return err[int64](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnName)
          seen[key] = true
    var finalRoot = indexRootRes.value
    if bound.indexKind == sql.ikTrigram:
      let buildRes = buildTrigramIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnName, indexRootRes.value)
      if not buildRes.ok:
        return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
      finalRoot = buildRes.value
    else:
      let buildRes = buildIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnName, indexRootRes.value)
      if not buildRes.ok:
        return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
      finalRoot = buildRes.value
    let idxKind = if bound.indexKind == sql.ikTrigram: catalog.ikTrigram else: catalog.ikBtree
    let idxMeta = IndexMeta(name: bound.indexName, table: bound.indexTableName, column: bound.columnName, rootPage: finalRoot, kind: idxKind, unique: bound.unique)
    let saveRes = db.catalog.createIndexMeta(idxMeta)
    if not saveRes.ok:
      return err[int64](saveRes.err.code, saveRes.err.message, saveRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skDropIndex:
    let dropRes = db.catalog.dropIndex(bound.dropIndexName)
    if not dropRes.ok:
      return err[int64](dropRes.err.code, dropRes.err.message, dropRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skInsert:
    let tableRes = db.catalog.getTable(bound.insertTable)
    if not tableRes.ok:
      return err[int64](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let valuesRes = evalInsertValues(bound, params)
    if not valuesRes.ok:
      return err[int64](valuesRes.err.code, valuesRes.err.message, valuesRes.err.context)
    let values = valuesRes.value
    for i, col in tableRes.value.columns:
      let typeRes = typeCheckValue(col.kind, values[i])
      if not typeRes.ok:
        return err[int64](typeRes.err.code, typeRes.err.message, col.name)
    let notNullRes = enforceNotNull(tableRes.value, values)
    if not notNullRes.ok:
      return err[int64](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
    let uniqueRes = enforceUnique(db.catalog, db.pager, tableRes.value, values)
    if not uniqueRes.ok:
      return err[int64](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
    let fkRes = enforceForeignKeys(db.catalog, db.pager, tableRes.value, values)
    if not fkRes.ok:
      return err[int64](fkRes.err.code, fkRes.err.message, fkRes.err.context)
    let insertRes = insertRow(db.pager, db.catalog, bound.insertTable, values)
    if not insertRes.ok:
      return err[int64](insertRes.err.code, insertRes.err.message, insertRes.err.context)
    affected = 1

  of skUpdate:
    let tableRes = db.catalog.getTable(bound.updateTable)
    if not tableRes.ok:
      return err[int64](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value
    var updates: seq[(uint64, seq[Value], seq[Value])] = @[]
    var cols: seq[string] = @[]
    for col in table.columns:
      cols.add(bound.updateTable & "." & col.name)

    let rowidsRes = findMatchingRowidsPrepared(db, bound.updateTable, bound.updateWhere, params)
    if not rowidsRes.ok:
      return err[int64](rowidsRes.err.code, rowidsRes.err.message, rowidsRes.err.context)

    for rowid in rowidsRes.value:
      let storedRes = readRowAt(db.pager, table, rowid)
      if not storedRes.ok:
        continue
      let stored = storedRes.value
      let row = Row(rowid: stored.rowid, columns: cols, values: stored.values)
      var newValues = stored.values
      for colName, expr in bound.assignments:
        var idx = -1
        for i, col in table.columns:
          if col.name == colName:
            idx = i
            break
        if idx >= 0:
          let evalRes = evalExpr(row, expr, params)
          if not evalRes.ok:
            return err[int64](evalRes.err.code, evalRes.err.message, evalRes.err.context)
          let typeRes = typeCheckValue(table.columns[idx].kind, evalRes.value)
          if not typeRes.ok:
            return err[int64](typeRes.err.code, typeRes.err.message, colName)
          newValues[idx] = evalRes.value
      updates.add((stored.rowid, stored.values, newValues))

    for entry in updates:
      let notNullRes = enforceNotNull(table, entry[2])
      if not notNullRes.ok:
        return err[int64](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
      let uniqueRes = enforceUnique(db.catalog, db.pager, table, entry[2], entry[0])
      if not uniqueRes.ok:
        return err[int64](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
      let fkRes = enforceForeignKeys(db.catalog, db.pager, table, entry[2])
      if not fkRes.ok:
        return err[int64](fkRes.err.code, fkRes.err.message, fkRes.err.context)
      let restrictRes = enforceRestrictOnParent(db.catalog, db.pager, table, entry[1], entry[2])
      if not restrictRes.ok:
        return err[int64](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)
    for entry in updates:
      let upRes = updateRow(db.pager, db.catalog, bound.updateTable, entry[0], entry[2])
      if not upRes.ok:
        return err[int64](upRes.err.code, upRes.err.message, upRes.err.context)

    affected = int64(updates.len)

  of skDelete:
    let tableRes = db.catalog.getTable(bound.deleteTable)
    if not tableRes.ok:
      return err[int64](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value
    var deletions: seq[StoredRow] = @[]

    let rowidsRes = findMatchingRowidsPrepared(db, bound.deleteTable, bound.deleteWhere, params)
    if not rowidsRes.ok:
      return err[int64](rowidsRes.err.code, rowidsRes.err.message, rowidsRes.err.context)

    for rowid in rowidsRes.value:
      let storedRes = readRowAt(db.pager, table, rowid)
      if not storedRes.ok:
        continue
      deletions.add(storedRes.value)
    for row in deletions:
      let restrictRes = enforceRestrictOnDelete(db.catalog, db.pager, table, row.values)
      if not restrictRes.ok:
        return err[int64](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)
    for row in deletions:
      let delRes = deleteRow(db.pager, db.catalog, bound.deleteTable, row.rowid)
      if not delRes.ok:
        return err[int64](delRes.err.code, delRes.err.message, delRes.err.context)

    affected = int64(deletions.len)

  of skBegin:
    let beginRes = beginTransaction(db)
    if not beginRes.ok:
      return err[int64](beginRes.err.code, beginRes.err.message, beginRes.err.context)
    affected = 0

  of skCommit:
    let commitRes = commitTransaction(db)
    if not commitRes.ok:
      return err[int64](commitRes.err.code, commitRes.err.message, commitRes.err.context)
    affected = 0

  of skRollback:
    let rollbackRes = rollbackTransaction(db)
    if not rollbackRes.ok:
      return err[int64](rollbackRes.err.code, rollbackRes.err.message, rollbackRes.err.context)
    affected = 0

  else:
    affected = 0

  if autoCommit:
    let commitRes = commitTransaction(db)
    if not commitRes.ok:
      return err[int64](commitRes.err.code, commitRes.err.message, commitRes.err.context)
    committed = true
  ok(affected)

proc execSql*(db: Db, sqlText: string): Result[seq[string]] =
  execSql(db, sqlText, @[])

proc execSqlNoRows*(db: Db, sqlText: string, params: seq[Value]): Result[int64] =
  ## Execute a single SELECT statement and discard all result rows.
  ## Returns the number of rows produced by the query.
  ##
  ## This is intended for benchmarks/diagnostics where formatting/serializing rows
  ## would dominate, but you still want to exercise query execution.
  if not db.isOpen:
    return err[int64](ERR_INTERNAL, "Database not open")
  const SqlCacheMaxEntries = 128

  proc touchSqlCache(key: string) =
    # Fast path: if key is already at the end (most recent), skip
    if db.sqlCacheOrder.len > 0 and db.sqlCacheOrder[^1] == key:
      return
    var idx = -1
    for i, existing in db.sqlCacheOrder:
      if existing == key:
        idx = i
        break
    if idx >= 0:
      db.sqlCacheOrder.delete(idx)
    db.sqlCacheOrder.add(key)

  proc rememberSqlCache(key: string, statements: seq[Statement], plans: seq[Plan]) =
    db.sqlCache[key] = (schemaCookie: db.schemaCookie, statements: statements, plans: plans)
    touchSqlCache(key)
    while db.sqlCacheOrder.len > SqlCacheMaxEntries:
      let evictKey = db.sqlCacheOrder[0]
      db.sqlCacheOrder.delete(0)
      if db.sqlCache.hasKey(evictKey):
        db.sqlCache.del(evictKey)

  var boundStatements: seq[Statement] = @[]
  var cachedPlans: seq[Plan] = @[]
  if db.sqlCache.hasKey(sqlText):
    let cached = db.sqlCache[sqlText]
    if cached.schemaCookie == db.schemaCookie:
      boundStatements = cached.statements
      cachedPlans = cached.plans
      touchSqlCache(sqlText)

  if boundStatements.len == 0:
    let parseRes = parseSql(sqlText)
    if not parseRes.ok:
      return err[int64](parseRes.err.code, parseRes.err.message, parseRes.err.context)
    if parseRes.value.statements.len != 1:
      return err[int64](ERR_SQL, "execSqlNoRows expects a single SELECT statement")
    let stmt = parseRes.value.statements[0]
    let bindRes = bindStatement(db.catalog, stmt)
    if not bindRes.ok:
      return err[int64](bindRes.err.code, bindRes.err.message, bindRes.err.context)
    boundStatements = @[bindRes.value]
    if boundStatements[0].kind != skSelect:
      return err[int64](ERR_SQL, "execSqlNoRows expects a SELECT statement")
    let planRes = plan(db.catalog, boundStatements[0])
    if not planRes.ok:
      return err[int64](planRes.err.code, planRes.err.message, planRes.err.context)
    cachedPlans = @[planRes.value]
    rememberSqlCache(sqlText, boundStatements, cachedPlans)

  if boundStatements.len != 1 or boundStatements[0].kind != skSelect:
    return err[int64](ERR_SQL, "execSqlNoRows expects a single SELECT statement")
  if cachedPlans.len != 1 or cachedPlans[0] == nil:
    return err[int64](ERR_INTERNAL, "Missing cached plan for SELECT")

  # Fast path: for simple index-seek plans, count rows without decoding full rows.
  let fastCountRes = tryCountNoRowsFast(db.pager, db.catalog, cachedPlans[0], params)
  if not fastCountRes.ok:
    return err[int64](fastCountRes.err.code, fastCountRes.err.message, fastCountRes.err.context)
  if fastCountRes.value.isSome:
    return ok(fastCountRes.value.get)

  let cursorRes = openRowCursor(db.pager, db.catalog, cachedPlans[0], params)
  if not cursorRes.ok:
    return err[int64](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  let cursor = cursorRes.value

  var rowCount: int64 = 0
  while true:
    let nextRes = rowCursorNext(cursor)
    if not nextRes.ok:
      return err[int64](nextRes.err.code, nextRes.err.message, nextRes.err.context)
    if nextRes.value.isNone:
      break
    rowCount.inc
  ok(rowCount)

proc bulkLoad*(db: Db, tableName: string, rows: seq[seq[Value]], options: BulkLoadOptions = defaultBulkLoadOptions(), wal: Wal = nil): Result[Void] =
  let tableRes = db.catalog.getTable(tableName)
  if not tableRes.ok:
    return err[Void](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  if options.durability == dmNone and db.wal != nil:
    db.walOverlayEnabled = false
  let useWal = wal != nil and options.durability != dmNone
  let table = tableRes.value
  let batchSize = if options.batchSize <= 0: 1 else: options.batchSize
  let syncInterval = if options.syncInterval <= 0: 1 else: options.syncInterval

  var uniqueCols: seq[int] = @[]
  for i, col in table.columns:
    if col.unique or col.primaryKey:
      uniqueCols.add(i)
  var seenFiles: seq[string] = newSeq[string](uniqueCols.len)
  var tempFiles: seq[string] = @[]
  defer:
    for path in tempFiles:
      if path.len > 0 and fileExists(path):
        removeFile(path)

  proc writeSortedKeys(path: string, keys: seq[uint64]): Result[Void] =
    var f: File
    if not open(f, path, fmWrite):
      return err[Void](ERR_IO, "Failed to open temp key file", path)
    defer: close(f)
    var buf = newSeq[byte](8)
    for k in keys:
      writeU64LE(buf, 0, k)
      discard f.writeBuffer(addr buf[0], 8)
    okVoid()

  proc mergeSeenFile(existingPath: string, batchKeys: seq[uint64], outPath: string, context: string): Result[Void] =
    var outFile: File
    if not open(outFile, outPath, fmWrite):
      return err[Void](ERR_IO, "Failed to open temp key file", outPath)
    defer: close(outFile)
    var inFile: File
    var hasIn = false
    if existingPath.len > 0 and fileExists(existingPath):
      if not open(inFile, existingPath, fmRead):
        return err[Void](ERR_IO, "Failed to open temp key file", existingPath)
      hasIn = true
    defer:
      if hasIn:
        close(inFile)
    var inBuf = newSeq[byte](8)
    var outBuf = newSeq[byte](8)
    var inKey: uint64 = 0
    var inOk = false
    proc readNextIn(): bool =
      if not hasIn:
        return false
      let n = inFile.readBuffer(addr inBuf[0], 8)
      if n != 8:
        return false
      inKey = readU64LE(inBuf, 0)
      true
    inOk = readNextIn()
    var batchIdx = 0
    var prev: uint64 = 0
    var hasPrev = false
    while inOk or batchIdx < batchKeys.len:
      var nextKey: uint64 = 0
      if inOk and batchIdx < batchKeys.len:
        if inKey <= batchKeys[batchIdx]:
          nextKey = inKey
          inOk = readNextIn()
        else:
          nextKey = batchKeys[batchIdx]
          batchIdx.inc
      elif inOk:
        nextKey = inKey
        inOk = readNextIn()
      else:
        nextKey = batchKeys[batchIdx]
        batchIdx.inc
      if hasPrev and nextKey == prev:
        return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", context)
      hasPrev = true
      prev = nextKey
      writeU64LE(outBuf, 0, nextKey)
      discard outFile.writeBuffer(addr outBuf[0], 8)
    okVoid()

  var batchCount = 0
  var pendingInBatch = 0
  var batchRows: seq[seq[Value]] = @[]

  proc commitDirtyToWal(): Result[Void] =
    if not useWal:
      return okVoid()
    let dirtyPages = snapshotDirtyPages(db.pager)
    if dirtyPages.len == 0:
      return okVoid()
    let writerRes = beginWrite(wal)
    if not writerRes.ok:
      return err[Void](writerRes.err.code, writerRes.err.message, writerRes.err.context)
    let writer = writerRes.value
    var pageIds: seq[PageId] = @[]
    for entry in dirtyPages:
      var bytes = newSeq[byte](entry[1].len)
      if entry[1].len > 0:
        copyMem(addr bytes[0], unsafeAddr entry[1][0], entry[1].len)
      let writeRes = writePage(writer, entry[0], bytes)
      if not writeRes.ok:
        discard rollback(writer)
        return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
      pageIds.add(entry[0])
    let commitRes = commit(writer)
    if not commitRes.ok:
      return err[Void](commitRes.err.code, commitRes.err.message, commitRes.err.context)
    if pageIds.len > 0:
      markPagesCommitted(db.pager, pageIds, commitRes.value)
    okVoid()

  proc processBatch(): Result[Void] =
    if batchRows.len == 0:
      return okVoid()
    # Validate + uniqueness precheck for the batch (before inserting anything).
    var perColKeys: seq[seq[uint64]] = newSeq[seq[uint64]](uniqueCols.len)
    for values in batchRows:
      if values.len != table.columns.len:
        return err[Void](ERR_SQL, "Column count mismatch", tableName)
      for i, col in table.columns:
        let typeRes = typeCheckValue(col.kind, values[i])
        if not typeRes.ok:
          return err[Void](typeRes.err.code, typeRes.err.message, col.name)
      let notNullRes = enforceNotNull(table, values)
      if not notNullRes.ok:
        return err[Void](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
      let fkRes = enforceForeignKeys(db.catalog, db.pager, table, values)
      if not fkRes.ok:
        return err[Void](fkRes.err.code, fkRes.err.message, fkRes.err.context)
      for idx, colIndex in uniqueCols:
        if values[colIndex].kind == vkNull:
          continue
        let key = indexKeyFromValue(values[colIndex])
        perColKeys[idx].add(key)
    var nextSeen: seq[string] = newSeq[string](uniqueCols.len)
    var nextTemp: seq[string] = @[]
    defer:
      for path in nextTemp:
        if path.len > 0 and fileExists(path):
          removeFile(path)
    for idx, colIndex in uniqueCols:
      if perColKeys[idx].len == 0:
        continue
      perColKeys[idx].sort()
      for i in 1 ..< perColKeys[idx].len:
        if perColKeys[idx][i] == perColKeys[idx][i - 1]:
          return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & table.columns[colIndex].name)
      # Check against existing table via its unique index if present.
      let idxOpt = db.catalog.getBtreeIndexForColumn(table.name, table.columns[colIndex].name)
      if isSome(idxOpt):
        var prev: Option[uint64] = none(uint64)
        for k in perColKeys[idx]:
          if prev.isSome and prev.get == k:
            continue
          prev = some(k)
          let existsRes = indexHasAnyKey(db.pager, idxOpt.get, k)
          if not existsRes.ok:
            return err[Void](existsRes.err.code, existsRes.err.message, existsRes.err.context)
          if existsRes.value:
            return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & table.columns[colIndex].name)
      let outPath = getTempDir() / ("decentdb_bulk_seen_" & table.name & "_" & table.columns[colIndex].name & "_" & $batchCount & ".bin")
      nextTemp.add(outPath)
      if seenFiles[idx].len == 0:
        let wRes = writeSortedKeys(outPath, perColKeys[idx])
        if not wRes.ok:
          return wRes
      else:
        let mRes = mergeSeenFile(seenFiles[idx], perColKeys[idx], outPath, table.name & "." & table.columns[colIndex].name)
        if not mRes.ok:
          return mRes
      nextSeen[idx] = outPath
    # Insert rows
    for values in batchRows:
      let insertRes = if options.disableIndexes:
        insertRowNoIndexes(db.pager, db.catalog, tableName, values)
      else:
        insertRow(db.pager, db.catalog, tableName, values)
      if not insertRes.ok:
        return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
    # Commit seenFiles swap
    for i in 0 ..< uniqueCols.len:
      if nextSeen[i].len == 0:
        continue
      if seenFiles[i].len > 0 and fileExists(seenFiles[i]):
        removeFile(seenFiles[i])
      seenFiles[i] = nextSeen[i]
      tempFiles.add(seenFiles[i])
    nextTemp = @[]
    batchRows = @[]
    pendingInBatch = 0
    batchCount.inc
    if useWal:
      if options.durability == dmFull:
        let commitRes = commitDirtyToWal()
        if not commitRes.ok:
          return commitRes
      elif options.durability == dmDeferred:
        if batchCount mod syncInterval == 0:
          let commitRes = commitDirtyToWal()
          if not commitRes.ok:
            return commitRes
    else:
      if options.durability == dmFull:
        let flushRes = flushAll(db.pager)
        if not flushRes.ok:
          return err[Void](flushRes.err.code, flushRes.err.message, flushRes.err.context)
      elif options.durability == dmDeferred:
        if batchCount mod syncInterval == 0:
          let flushRes = flushAll(db.pager)
          if not flushRes.ok:
            return err[Void](flushRes.err.code, flushRes.err.message, flushRes.err.context)
    okVoid()

  for values in rows:
    batchRows.add(values)
    pendingInBatch.inc
    if pendingInBatch >= batchSize:
      let pRes = processBatch()
      if not pRes.ok:
        return pRes
  let pRes = processBatch()
  if not pRes.ok:
    return pRes

  if options.disableIndexes:
    for _, idx in db.catalog.indexes:
      if idx.table == tableName:
        let rebuildRes = rebuildIndex(db.pager, db.catalog, idx)
        if not rebuildRes.ok:
          return rebuildRes

  if useWal:
    let commitRes = commitDirtyToWal()
    if not commitRes.ok:
      return commitRes
  else:
    if options.durability in {dmFull, dmDeferred}:
      let flushRes = flushAll(db.pager)
      if not flushRes.ok:
        return err[Void](flushRes.err.code, flushRes.err.message, flushRes.err.context)

  if options.checkpointOnComplete and useWal:
    let ckRes = checkpoint(wal, db.pager)
    if not ckRes.ok:
      return err[Void](ckRes.err.code, ckRes.err.message, ckRes.err.context)
  okVoid()

proc closeDb*(db: Db): Result[Void] =
  if not db.isOpen:
    return okVoid()
  
  # Flush any pending trigram deltas before closing (ensure clean shutdown durability)
  # We ignore errors here to prioritize closing, but log them in a real system
  if db.catalog.trigramDeltas.len > 0:
    discard flushTrigramDeltas(db.pager, db.catalog)

  # Commit any uncommitted dirty pages to WAL before closing
  if db.wal != nil and db.activeWriter == nil:
    let dirtyPages = snapshotDirtyPages(db.pager)
    if dirtyPages.len > 0:
      let writerRes = beginWrite(db.wal)
      if writerRes.ok:
        let writer = writerRes.value
        var pageIds: seq[PageId] = @[]
        for entry in dirtyPages:
          var bytes = newSeq[byte](entry[1].len)
          if entry[1].len > 0: copyMem(addr bytes[0], unsafeAddr entry[1][0], entry[1].len)
          discard writePage(writer, entry[0], bytes)
          pageIds.add(entry[0])
        let commitRes = commit(writer)
        if commitRes.ok:
          markPagesCommitted(db.pager, pageIds, commitRes.value)
        else:
          discard rollback(writer)
  
  # Close WAL file if present
  if db.wal != nil:
    let walCloseRes = db.vfs.close(db.wal.file)
    if not walCloseRes.ok:
      return walCloseRes
  
  let pagerRes = closePager(db.pager)
  if not pagerRes.ok:
    return pagerRes
  let res = db.vfs.close(db.file)
  if not res.ok:
    return res
  db.isOpen = false
  okVoid()

# ============================================================================
# Transaction Control
# ============================================================================

proc beginTransaction*(db: Db): Result[Void] =
  ## Begin an explicit transaction
  if not db.isOpen:
    return err[Void](ERR_INTERNAL, "Database not open")
  if db.activeWriter != nil:
    return err[Void](ERR_TRANSACTION, "Transaction already active")
  
  let writerRes = beginWrite(db.wal)
  if not writerRes.ok:
    return err[Void](writerRes.err.code, writerRes.err.message, writerRes.err.context)
  
  db.activeWriter = writerRes.value
  
  # Begin tracking page allocations for this transaction (HIGH-003)
  beginTxnPageTracking(db.pager)
  
  # Install flush handler to allow evicting dirty pages during large transactions
  db.pager.flushHandler = proc(pageId: PageId, data: string): Result[Void] =
    # Convert page data (string) to WAL payload (seq[byte])
    var payload = newSeq[byte](data.len)
    if data.len > 0:
      copyMem(addr payload[0], unsafeAddr data[0], data.len)
      
    db.activeWriter.flushPage(pageId, payload)

  okVoid()

proc commitTransaction*(db: Db): Result[Void] =
  ## Commit the active transaction
  ## Note: Trigram deltas are NOT flushed on commit - they are flushed 
  ## during checkpoint for better performance (MED-003)
  if not db.isOpen:
    return err[Void](ERR_INTERNAL, "Database not open")
  if db.activeWriter == nil:
    return err[Void](ERR_TRANSACTION, "No active transaction")

  # MED-003: Trigram deltas are deferred to checkpoint for better performance
  # This means trigram indexes may be temporarily out of sync after crash
  # until a checkpoint completes. The B+Tree data is always durable.

  let dirtyPages = snapshotDirtyPages(db.pager)
  var pageIds: seq[PageId] = @[]

  for entry in dirtyPages:
    var bytes = newSeq[byte](entry[1].len)
    if entry[1].len > 0:
      copyMem(addr bytes[0], unsafeAddr entry[1][0], entry[1].len)
    let writeRes = writePage(db.activeWriter, entry[0], bytes)
    if not writeRes.ok:

      discard rollback(db.activeWriter)
      db.activeWriter = nil
      db.pager.flushHandler = nil
      clearCache(db.pager)
      return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    pageIds.add(entry[0])

  let commitRes = commit(db.activeWriter)
  if not commitRes.ok:
    db.activeWriter = nil
    db.pager.flushHandler = nil
    clearCache(db.pager)
    return err[Void](commitRes.err.code, commitRes.err.message, commitRes.err.context)

  if pageIds.len > 0:
    markPagesCommitted(db.pager, pageIds, commitRes.value)

  # MED-003: Check if checkpoint will be triggered and flush trigram deltas first
  # This ensures trigram indexes are consistent with the checkpointed data
  var willCheckpoint = false
  if db.wal.checkpointEveryBytes > 0 and db.wal.endOffset >= db.wal.checkpointEveryBytes:
    willCheckpoint = true
  if db.wal.checkpointEveryMs > 0:
    let elapsedMs = int64((epochTime() - db.wal.lastCheckpointAt) * 1000)
    if elapsedMs >= db.wal.checkpointEveryMs:
      willCheckpoint = true
  if db.wal.checkpointMemoryThreshold > 0:
    let memUsage = estimateIndexMemoryUsage(db.wal)
    if memUsage >= db.wal.checkpointMemoryThreshold:
      willCheckpoint = true
  
  if willCheckpoint:
    let trigramFlushRes = flushTrigramDeltas(db.pager, db.catalog)
    if not trigramFlushRes.ok:
      db.activeWriter = nil
      db.pager.flushHandler = nil
      return err[Void](trigramFlushRes.err.code, trigramFlushRes.err.message, trigramFlushRes.err.context)
  
  let chkRes = maybeCheckpoint(db.wal, db.pager)
  if not chkRes.ok:
    db.activeWriter = nil
    db.pager.flushHandler = nil
    return err[Void](chkRes.err.code, chkRes.err.message, chkRes.err.context)

  db.activeWriter = nil
  db.pager.flushHandler = nil
  
  # End page allocation tracking (pages are now permanent) (HIGH-003)
  endTxnPageTracking(db.pager)
  
  okVoid()

proc rollbackTransaction*(db: Db): Result[Void] =
  ## Rollback the active transaction atomically.
  ## 
  ## This ensures that dirty pages are evicted from cache atomically with
  ## the WAL rollback, preventing other threads from seeing partial dirty state.
  ## Also returns any pages allocated during this transaction to the freelist (HIGH-003).
  if not db.isOpen:
    return err[Void](ERR_INTERNAL, "Database not open")
  if db.activeWriter == nil:
    return err[Void](ERR_TRANSACTION, "No active transaction")
  
  # Hold rollback lock only across the atomic rollback window:
  # WAL rollback + cache eviction + page allocation rollback.
  # Do not hold it while reading pages (readPage/withPageRo acquire it internally).
  acquire(db.pager.rollbackLock)

  let dirtyPages = snapshotDirtyPages(db.pager)
  let rollbackRes = rollback(db.activeWriter)
  db.activeWriter = nil
  db.pager.flushHandler = nil
  if not rollbackRes.ok:
    release(db.pager.rollbackLock)
    return err[Void](rollbackRes.err.code, rollbackRes.err.message, rollbackRes.err.context)

  # Evict dirty pages immediately while holding rollback lock
  if dirtyPages.len > 0:
    rollbackCacheLocked(db.pager)

  # Return allocated pages to freelist (HIGH-003)
  let freeRes = rollbackTxnPageAllocations(db.pager)
  if not freeRes.ok:
    # Log error but continue - partial cleanup is better than none
    stderr.writeLine("Warning: failed to return some allocated pages to freelist during rollback")

  db.catalog.clearTrigramDeltas()

  release(db.pager.rollbackLock)

  # Reload header and catalog to revert any in-memory changes
  let page1Res = readPage(db.pager, PageId(1))
  if page1Res.ok:
    let hRes = decodeHeader(page1Res.value)
    if hRes.ok:
      db.pager.header = hRes.value

  let txn = beginRead(db.wal)
  db.pager.overlaySnapshot = txn.snapshot
  let reloadRes = initCatalog(db.pager)
  db.pager.overlaySnapshot = 0
  endRead(db.wal, txn)

  if reloadRes.ok:
    db.catalog = reloadRes.value

  okVoid()

proc checkpointDb*(db: Db): Result[uint64] =
  ## Force a checkpoint of the WAL.
  ## Also flushes any pending trigram index deltas (MED-003).
  if not db.isOpen:
    return err[uint64](ERR_INTERNAL, "Database not open")
  
  # MED-003: Flush trigram deltas during checkpoint rather than on every commit
  # This amortizes the cost across multiple transactions while ensuring
  # trigram indexes are eventually consistent with the B+Tree data.
  
  # Ensure no active transaction (checkpoint must be standalone)
  if db.activeWriter != nil:
    return err[uint64](ERR_TRANSACTION, "Cannot checkpoint during active transaction")

  # MED-003: Only start a transaction to flush trigram deltas if there are any.
  # This avoids unnecessary WAL writes and potential interference with bulk operations (like VACUUM).
  if db.catalog.trigramDeltas.len > 0:
    let beginRes = beginTransaction(db)
    if not beginRes.ok:
      return err[uint64](beginRes.err.code, beginRes.err.message, beginRes.err.context)

    # Don't clear deltas yet - wait for successful commit
    let trigramFlushRes = flushTrigramDeltas(db.pager, db.catalog, clear = false)
    if not trigramFlushRes.ok:
      discard rollbackTransaction(db)
      return err[uint64](trigramFlushRes.err.code, trigramFlushRes.err.message, trigramFlushRes.err.context)
    
    let commitRes = commitTransaction(db)
    if not commitRes.ok:
      return err[uint64](commitRes.err.code, commitRes.err.message, commitRes.err.context)
    
    # Now safe to clear memory deltas
    db.catalog.clearTrigramDeltas()
  
  checkpoint(db.wal, db.pager)
