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
import std/monotimes
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
import ./btree/btree
import ./wal/wal

when defined(bench_breakdown):
  import ./utils/bench_breakdown

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

type UpdateWriteProfile* = object
  hasUpdateTriggers*: bool
  hasCheckConstraints*: bool
  hasNotNullOnUpdatedCols*: bool
  hasUniqueOnUpdatedCols*: bool
  hasForeignKeysOnUpdatedCols*: bool
  isParentOfForeignKeys*: bool
  updatesIndexedColumns*: bool
  isSimpleUpdate*: bool

type InsertWriteProfile* = object
  valid*: bool                     ## true when profile has been computed
  hasChecks*: bool                 ## table has CHECK constraints
  hasForeignKeys*: bool            ## any column has FK references
  hasNonPkUniqueColumns*: bool     ## UNIQUE columns besides INT64 PK exist
  hasCompositeUniqueIndexes*: bool ## multi-column unique indexes exist
  hasSecondaryIndexes*: bool       ## table has secondary indexes
  isView*: bool                    ## target is a view (needs INSTEAD triggers)
  hasInsertTriggers*: bool         ## table has AFTER INSERT triggers
  hasNotNullConstraints*: bool     ## true if any non-PK column has NOT NULL (needs enforceNotNull)
  singleInt64PkIdx*: int          ## column index of single INT64 PK (-1 if none/composite)
  allParamIndices*: seq[int]       ## param indices when all insert values are direct params (empty = slow path)
  isIdentityParamMapping*: bool    ## true when allParamIndices = [0,1,...,n-1] (can use params directly)
  hasTextBlobColumns*: bool        ## true if any column is TEXT/BLOB (might need normalization)

type Prepared* = ref object
  db*: Db
  sql*: string
  schemaCookie*: uint32
  statements*: seq[Statement]
  plans*: seq[Plan]
  updateProfiles*: seq[UpdateWriteProfile]
  insertProfiles*: seq[InsertWriteProfile]
  # Fast-path flag: single INSERT, identity params, no constraints/triggers/RETURNING
  isFastInsert*: bool
  fastInsertProfile*: InsertWriteProfile
  fastInsertBound*: int  ## index into statements for the INSERT
  fastInsertTablePtr*: ptr TableMeta  ## cached table pointer for fast path

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
  let walPath = path & "-wal"
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
    # Fresh DB: remove any stale WAL from prior runs
    if fileExists(walPath):
      removeFile(walPath)
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
  let walRes = newWal(vfs, walPath, header.pageSize)
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
    readerCheckIntervalMs = 5000,  # HIGH-006: Check readers every 5 seconds
    checkpointCheckInterval = 64)  # Only evaluate time/memory every 64 commits
  
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

proc viewDependencies(stmt: Statement): seq[string] =
  if stmt == nil:
    return
  var seen = initHashSet[string]()
  if stmt.kind == skSelect:
    var cteNames = initHashSet[string]()
    for cteName in stmt.cteNames:
      cteNames.incl(cteName.toLowerAscii())
    for query in stmt.cteQueries:
      if query != nil and query.kind == skSelect:
        for dep in viewDependencies(query):
          seen.incl(dep)
    if stmt.setOpLeft != nil and stmt.setOpLeft.kind == skSelect:
      for dep in viewDependencies(stmt.setOpLeft):
        seen.incl(dep)
    if stmt.setOpRight != nil and stmt.setOpRight.kind == skSelect:
      for dep in viewDependencies(stmt.setOpRight):
        seen.incl(dep)
    if stmt.fromTable.len > 0 and stmt.fromTable.toLowerAscii() notin cteNames:
      seen.incl(stmt.fromTable.toLowerAscii())
    for join in stmt.joins:
      if join.table.len > 0 and join.table.toLowerAscii() notin cteNames:
        seen.incl(join.table.toLowerAscii())
  for dep in seen:
    result.add(dep)

proc valueFromSql(value: Value): Value =
  value

proc typeCheckValue(col: Column, value: Value): Result[Value] =
  case col.kind
  of ctInt64:
    if value.kind == vkNull: return ok(value)
    if value.kind == vkInt64: return ok(value)
    return err[Value](ERR_SQL, "Type mismatch: expected INT64")
  of ctBool:
    if value.kind == vkNull: return ok(value)
    if value.kind == vkBool: return ok(value)
    return err[Value](ERR_SQL, "Type mismatch: expected BOOL")
  of ctFloat64:
    if value.kind == vkNull: return ok(value)
    if value.kind == vkFloat64: return ok(value)
    if value.kind == vkInt64: return ok(Value(kind: vkFloat64, float64Val: float64(value.int64Val)))
    return err[Value](ERR_SQL, "Type mismatch: expected FLOAT64")
  of ctText:
    if value.kind == vkNull: return ok(value)
    if value.kind == vkText: return ok(value)
    return err[Value](ERR_SQL, "Type mismatch: expected TEXT")
  of ctBlob:
    if value.kind == vkNull: return ok(value)
    if value.kind == vkBlob: return ok(value)
    return err[Value](ERR_SQL, "Type mismatch: expected BLOB")
  of ctDecimal:
    if value.kind == vkNull: return ok(value)
    if value.kind == vkDecimal:
       if value.decimalScale != col.decScale:
          let res = scaleDecimal(value.int64Val, value.decimalScale, col.decScale)
          if not res.ok: return err[Value](res.err.code, res.err.message)
          let newVal = Value(kind: vkDecimal, int64Val: res.value, decimalScale: col.decScale)
          if ($abs(newVal.int64Val)).len > int(col.decPrecision):
             return err[Value](ERR_CONSTRAINT, "Precision overflow for DECIMAL")
          return ok(newVal)
       if ($abs(value.int64Val)).len > int(col.decPrecision):
          return err[Value](ERR_CONSTRAINT, "Precision overflow for DECIMAL")
       return ok(value)
    if value.kind == vkInt64:
       let res = scaleDecimal(value.int64Val, 0, col.decScale)
       if not res.ok: return err[Value](res.err.code, res.err.message)
       let newVal = Value(kind: vkDecimal, int64Val: res.value, decimalScale: col.decScale)
       if ($abs(newVal.int64Val)).len > int(col.decPrecision):
          return err[Value](ERR_CONSTRAINT, "Precision overflow for DECIMAL")
       return ok(newVal)
    return err[Value](ERR_SQL, "Type mismatch: expected DECIMAL")
  of ctUuid:
    if value.kind == vkNull: return ok(value)
    if value.kind == vkBlob:
       if value.bytes.len == 16: return ok(value)
       return err[Value](ERR_CONSTRAINT, "UUID must be 16 bytes")
    return err[Value](ERR_SQL, "Type mismatch: expected UUID")

proc typeCheckFast(col: Column, value: Value): bool {.inline.} =
  ## Returns true if value trivially matches the column type (no conversion needed).
  if value.kind == vkNull:
    return true
  case col.kind
  of ctInt64: value.kind == vkInt64
  of ctBool: value.kind == vkBool
  of ctFloat64: value.kind == vkFloat64
  of ctText: value.kind == vkText
  of ctBlob: value.kind == vkBlob
  of ctDecimal: value.kind == vkDecimal and value.decimalScale == col.decScale
  of ctUuid: value.kind == vkBlob and value.bytes.len == 16

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

proc enforceNotNull(table: TableMeta, values: seq[Value], skipAutoIncrementPk: bool = false): Result[Void] =
  for i, col in table.columns:
    if col.notNull and values[i].kind == vkNull:
      if skipAutoIncrementPk and col.primaryKey and col.kind == ctInt64:
        continue
      return err[Void](ERR_CONSTRAINT, "NOT NULL constraint failed", table.name & "." & col.name)
  okVoid()

proc enforceChecks(table: TableMeta, values: seq[Value]): Result[Void] =
  if table.checks.len == 0:
    return okVoid()
  if values.len != table.columns.len:
    return err[Void](ERR_SQL, "Column count mismatch for CHECK evaluation", table.name)

  var rowCols: seq[string] = @[]
  for col in table.columns:
    rowCols.add(table.name & "." & col.name)
  let row = makeRow(rowCols, values)

  for checkDef in table.checks:
    let exprRes = parseStandaloneExpr(checkDef.exprSql)
    if not exprRes.ok:
      return err[Void](ERR_CORRUPTION, "Invalid CHECK expression in catalog", table.name)
    let evalRes = evalExpr(row, exprRes.value, @[])
    if not evalRes.ok:
      return err[Void](evalRes.err.code, evalRes.err.message, evalRes.err.context)
    if evalRes.value.kind == vkNull:
      continue
    if evalRes.value.kind != vkBool:
      return err[Void](ERR_SQL, "CHECK expression must evaluate to BOOL", table.name)
    if not evalRes.value.boolVal:
      let context =
        if checkDef.name.len > 0:
          table.name & "." & checkDef.name
        else:
          table.name
      return err[Void](ERR_CONSTRAINT, "CHECK constraint failed", context)
  okVoid()

proc enforceUnique(catalog: Catalog, pager: Pager, table: TableMeta, values: seq[Value], rowid: uint64 = 0): Result[Void] =
  # Count PK columns to detect composite PKs
  var pkCount = 0
  for col in table.columns:
    if col.primaryKey:
      inc pkCount
  for i, col in table.columns:
    if col.unique or (col.primaryKey and pkCount == 1):
      if values[i].kind == vkNull:
        continue
      if col.primaryKey and col.kind == ctInt64:
        # Optimization: checked by btree.insert
        continue
      let idxOpt = catalog.getBtreeIndexForColumn(table.name, col.name)
      if isNone(idxOpt):
        return err[Void](ERR_INTERNAL, "Missing UNIQUE index", table.name & "." & col.name)
      let isTextBlob = col.kind in {ctText, ctBlob} and values[i].kind in {vkText, vkBlob}
      let key = indexKeyFromValue(values[i])
      if rowid == 0:
        if isTextBlob:
          let otherRes = indexHasOtherRowid(pager, idxOpt.get, key, 0'u64, isTextBlob = true, valueBytes = values[i].bytes)
          if not otherRes.ok:
            return err[Void](otherRes.err.code, otherRes.err.message, otherRes.err.context)
          if otherRes.value:
            return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & col.name)
        else:
          let anyRes = indexHasAnyKey(pager, idxOpt.get, key)
          if not anyRes.ok:
            return err[Void](anyRes.err.code, anyRes.err.message, anyRes.err.context)
          if anyRes.value:
            return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & col.name)
      else:
        let otherRes = indexHasOtherRowid(pager, idxOpt.get, key, rowid, isTextBlob = isTextBlob, valueBytes = if isTextBlob: values[i].bytes else: @[])
        if not otherRes.ok:
          return err[Void](otherRes.err.code, otherRes.err.message, otherRes.err.context)
        if otherRes.value:
          return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & col.name)
  # Check composite unique indexes (including composite PKs)
  for _, idx in catalog.indexes:
    if idx.table != table.name or idx.columns.len < 2 or not idx.unique:
      continue
    let colIndices = indexColumnIndices(table, idx)
    if colIndices.len != idx.columns.len:
      continue
    # Skip if any composite column value is NULL
    var hasNull = false
    for ci in colIndices:
      if values[ci].kind == vkNull:
        hasNull = true
        break
    if hasNull:
      continue
    let key = compositeIndexKey(values, colIndices)
    # Look up candidate rowids by hash key, then verify exact column values
    let idxTree = newBTree(pager, idx.rootPage)
    let cursorRes = openCursorAt(idxTree, key)
    if not cursorRes.ok:
      return err[Void](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
    let cursor = cursorRes.value
    while true:
      let nextRes = cursorNext(cursor)
      if not nextRes.ok:
        if nextRes.err.code == ERR_IO and nextRes.err.message == "Cursor exhausted":
          break
        return err[Void](nextRes.err.code, nextRes.err.message, nextRes.err.context)
      if nextRes.value[0] < key:
        continue
      if nextRes.value[0] > key:
        break
      if nextRes.value[1].len < 8:
        continue
      let candidateRowid = readU64LE(nextRes.value[1], 0)
      if rowid != 0 and candidateRowid == rowid:
        continue
      let rowRes = readRowAt(pager, table, candidateRowid)
      if not rowRes.ok:
        if rowRes.err.code != ERR_IO:
          return err[Void](rowRes.err.code, rowRes.err.message, rowRes.err.context)
        continue
      var allMatch = true
      for ci in colIndices:
        if not valuesEqual(rowRes.value.values[ci], values[ci]):
          allMatch = false
          break
      if allMatch:
        return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & idx.columns.join(","))
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
      let rowIdsRes = indexSeek(pager, catalog, col.refTable, col.refColumn, values[i])
      if not rowIdsRes.ok:
        return err[Void](rowIdsRes.err.code, rowIdsRes.err.message, rowIdsRes.err.context)
      if rowIdsRes.value.len == 0:
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY constraint failed", table.name & "." & col.name)
    else:
      let key = indexKeyFromValue(values[i])
      let anyRes = indexHasAnyKey(pager, idxOpt.get, key)
      if not anyRes.ok:
        return err[Void](anyRes.err.code, anyRes.err.message, anyRes.err.context)
      if not anyRes.value:
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY constraint failed", table.name & "." & col.name)
  okVoid()

proc isUniqueConflictError(errVal: DbError): bool =
  if errVal.code != ERR_CONSTRAINT:
    return false
  errVal.message == "UNIQUE constraint failed" or errVal.message == "Unique constraint violation"

proc hasInt64PkConflict(pager: Pager, table: TableMeta, values: seq[Value]): Result[bool] =
  for i, col in table.columns:
    if col.primaryKey and col.kind == ctInt64 and values[i].kind == vkInt64:
      let rowid = cast[uint64](values[i].int64Val)
      let rowRes = readRowAt(pager, table, rowid)
      if rowRes.ok:
        return ok(true)
      if rowRes.err.code == ERR_IO:
        return ok(false)
      return err[bool](rowRes.err.code, rowRes.err.message, rowRes.err.context)
  ok(false)

proc findConflictRowidOnTarget(
  catalog: Catalog,
  pager: Pager,
  table: TableMeta,
  values: seq[Value],
  targetCols: seq[string]
): Result[Option[uint64]] =
  if targetCols.len == 0:
    return ok(none(uint64))

  var colIndices: seq[int] = @[]
  for colName in targetCols:
    var idx = -1
    for i, col in table.columns:
      if col.name == colName:
        idx = i
        break
    if idx < 0:
      return err[Option[uint64]](ERR_SQL, "Unknown ON CONFLICT target column", colName)
    colIndices.add(idx)

  for ci in colIndices:
    if values[ci].kind == vkNull:
      return ok(none(uint64))

  if colIndices.len == 1:
    let ci = colIndices[0]
    let col = table.columns[ci]
    if col.primaryKey and col.kind == ctInt64 and values[ci].kind == vkInt64:
      let rowid = cast[uint64](values[ci].int64Val)
      let rowRes = readRowAt(pager, table, rowid)
      if rowRes.ok:
        return ok(some(rowid))
      if rowRes.err.code == ERR_IO:
        return ok(none(uint64))
      return err[Option[uint64]](rowRes.err.code, rowRes.err.message, rowRes.err.context)

    let idxOpt = catalog.getBtreeIndexForColumn(table.name, col.name)
    if isNone(idxOpt):
      return err[Option[uint64]](ERR_INTERNAL, "Missing UNIQUE index", table.name & "." & col.name)

    let rowIdsRes = indexSeek(pager, catalog, table.name, col.name, values[ci])
    if not rowIdsRes.ok:
      return err[Option[uint64]](rowIdsRes.err.code, rowIdsRes.err.message, rowIdsRes.err.context)
    for rid in rowIdsRes.value:
      let rowRes = readRowAt(pager, table, rid)
      if not rowRes.ok:
        if rowRes.err.code == ERR_IO:
          continue
        return err[Option[uint64]](rowRes.err.code, rowRes.err.message, rowRes.err.context)
      if valuesEqual(rowRes.value.values[ci], values[ci]):
        return ok(some(rid))
    return ok(none(uint64))

  var matchedIndex: Option[IndexMeta] = none(IndexMeta)
  for _, idx in catalog.indexes:
    if idx.table == table.name and idx.unique and idx.columns == targetCols:
      matchedIndex = some(idx)
      break
  if isNone(matchedIndex):
    return err[Option[uint64]](ERR_INTERNAL, "Missing UNIQUE index", table.name & "." & targetCols.join(","))

  let idxMeta = matchedIndex.get
  let key = compositeIndexKey(values, colIndices)
  let idxTree = newBTree(pager, idxMeta.rootPage)
  let cursorRes = openCursorAt(idxTree, key)
  if not cursorRes.ok:
    return err[Option[uint64]](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
  let cursor = cursorRes.value
  while true:
    let nextRes = cursorNext(cursor)
    if not nextRes.ok:
      if nextRes.err.code == ERR_IO and nextRes.err.message == "Cursor exhausted":
        break
      return err[Option[uint64]](nextRes.err.code, nextRes.err.message, nextRes.err.context)
    if nextRes.value[0] < key:
      continue
    if nextRes.value[0] > key:
      break
    if nextRes.value[1].len < 8:
      continue
    let candidateRowid = readU64LE(nextRes.value[1], 0)
    let rowRes = readRowAt(pager, table, candidateRowid)
    if not rowRes.ok:
      if rowRes.err.code != ERR_IO:
        return err[Option[uint64]](rowRes.err.code, rowRes.err.message, rowRes.err.context)
      continue
    var allMatch = true
    for ci in colIndices:
      if not valuesEqual(rowRes.value.values[ci], values[ci]):
        allMatch = false
        break
    if allMatch:
      return ok(some(candidateRowid))
  ok(none(uint64))

proc hasUniqueConflictOnTarget(
  catalog: Catalog,
  pager: Pager,
  table: TableMeta,
  values: seq[Value],
  targetCols: seq[string]
): Result[bool] =
  let conflictRowRes = findConflictRowidOnTarget(catalog, pager, table, values, targetCols)
  if not conflictRowRes.ok:
    return err[bool](conflictRowRes.err.code, conflictRowRes.err.message, conflictRowRes.err.context)
  ok(conflictRowRes.value.isSome)

proc hasAnyUniqueConflict(catalog: Catalog, pager: Pager, table: TableMeta, values: seq[Value]): Result[bool] =
  let uniqueRes = enforceUnique(catalog, pager, table, values)
  if not uniqueRes.ok:
    if isUniqueConflictError(uniqueRes.err):
      return ok(true)
    return err[bool](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
  let pkConflictRes = hasInt64PkConflict(pager, table, values)
  if not pkConflictRes.ok:
    return err[bool](pkConflictRes.err.code, pkConflictRes.err.message, pkConflictRes.err.context)
  ok(pkConflictRes.value)

# ============================================================================
# HIGH-005: Batched Constraint Checking for Bulk Operations
# ============================================================================

type ConstraintBatchOptions* = object
  ## Options for batch constraint checking
  checkNotNull*: bool
  checkChecks*: bool
  checkUnique*: bool
  checkForeignKeys*: bool
  skipInt64PkOptimization*: bool  # When true, always check via index even for INT64 PK

proc defaultConstraintBatchOptions*(): ConstraintBatchOptions =
  ## Return default options (all checks enabled)
  ConstraintBatchOptions(
    checkNotNull: true,
    checkChecks: true,
    checkUnique: true,
    checkForeignKeys: true,
    skipInt64PkOptimization: false
  )

proc enforceNotNullBatch*(table: TableMeta, rows: seq[seq[Value]], skipAutoIncrementPk: bool = false): Result[seq[int]] =
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
        if skipAutoIncrementPk and col.primaryKey and col.kind == ctInt64:
          continue
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
      # TEXT/BLOB indexes now embed value bytes. Use indexHasOtherRowid
      # with embedded value comparison instead of post-verification reads.
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
        let key = indexKeyFromValue(info.value)
        let existsRes = indexHasOtherRowid(
          pager,
          colInfo.idxOpt.get,
          key,
          info.rowid,
          isTextBlob = true,
          valueBytes = info.value.bytes
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
        let rowIdsRes = indexSeek(pager, catalog, parentTable.name, refKey.refColumn, info.value)
        if not rowIdsRes.ok:
          return err[seq[tuple[rowIdx: int, colName: string]]](
            rowIdsRes.err.code, rowIdsRes.err.message, rowIdsRes.err.context
          )
        if rowIdsRes.value.len == 0:
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
    
    let notNullRes = enforceNotNullBatch(table, allValues, skipAutoIncrementPk = true)
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

  if options.checkChecks:
    for rowIdx, row in rows:
      let checkRes = enforceChecks(table, row.values)
      if not checkRes.ok:
        if checkRes.err.code == ERR_CONSTRAINT:
          allFailures.add((
            rowIdx: rowIdx,
            constraint: "CHECK",
            details: checkRes.err.context
          ))
        else:
          return err[seq[tuple[rowIdx: int, constraint: string, details: string]]](
            checkRes.err.code, checkRes.err.message, checkRes.err.context
          )
  
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

proc columnIndexInTable(table: TableMeta, columnName: string): int =
  for i, col in table.columns:
    if col.name == columnName:
      return i
  -1

proc findReferencingRows(catalog: Catalog, pager: Pager, childTable: TableMeta, childColumn: string, parentValue: Value): Result[seq[StoredRow]] =
  var matches: seq[StoredRow] = @[]
  if parentValue.kind == vkNull:
    return ok(matches)

  let childColIdx = columnIndexInTable(childTable, childColumn)
  if childColIdx < 0:
    return err[seq[StoredRow]](ERR_INTERNAL, "Missing FK child column", childTable.name & "." & childColumn)

  let idxOpt = catalog.getBtreeIndexForColumn(childTable.name, childColumn)
  if isNone(idxOpt):
    var isInt64Pk = false
    for cCol in childTable.columns:
      if cCol.name == childColumn and cCol.primaryKey and cCol.kind == ctInt64:
        isInt64Pk = true
        break
    if isInt64Pk and parentValue.kind == vkInt64:
      let targetId = cast[uint64](parentValue.int64Val)
      let rowRes = readRowAt(pager, childTable, targetId)
      if rowRes.ok:
        if valuesEqual(rowRes.value.values[childColIdx], parentValue):
          matches.add(rowRes.value)
      elif rowRes.err.code != ERR_IO:
        return err[seq[StoredRow]](rowRes.err.code, rowRes.err.message, rowRes.err.context)
      return ok(matches)
    return err[seq[StoredRow]](ERR_INTERNAL, "Missing FK child index", childTable.name & "." & childColumn)

  let rowIdsRes = indexSeek(pager, catalog, childTable.name, childColumn, parentValue)
  if not rowIdsRes.ok:
    return err[seq[StoredRow]](rowIdsRes.err.code, rowIdsRes.err.message, rowIdsRes.err.context)
  for rowid in rowIdsRes.value:
    let rowRes = readRowAt(pager, childTable, rowid)
    if not rowRes.ok:
      if rowRes.err.code == ERR_IO:
        continue
      return err[seq[StoredRow]](rowRes.err.code, rowRes.err.message, rowRes.err.context)
    if valuesEqual(rowRes.value.values[childColIdx], parentValue):
      matches.add(rowRes.value)
  ok(matches)

proc enforceRestrictOnParent(catalog: Catalog, pager: Pager, table: TableMeta, oldValues: seq[Value], newValues: seq[Value]): Result[Void] =
  for i, col in table.columns:
    let children = catalog.referencingChildren(table.name, col.name)
    if children.len == 0:
      continue
    let oldVal = oldValues[i]
    let newVal = newValues[i]
    if valuesEqual(oldVal, newVal):
      continue
    if oldVal.kind == vkNull:
      continue
    for child in children:
      let childTableRes = catalog.getTable(child.tableName)
      if not childTableRes.ok:
        return err[Void](childTableRes.err.code, childTableRes.err.message, childTableRes.err.context)
      let childTable = childTableRes.value
      let refsRes = findReferencingRows(catalog, pager, childTable, child.columnName, oldVal)
      if not refsRes.ok:
        return err[Void](refsRes.err.code, refsRes.err.message, refsRes.err.context)
      let refs = refsRes.value
      if refs.len == 0:
        continue

      case child.onUpdate
      of "NO ACTION", "RESTRICT":
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY RESTRICT violation", table.name & "." & col.name)
      of "CASCADE", "SET NULL":
        let childColIdx = columnIndexInTable(childTable, child.columnName)
        if childColIdx < 0:
          return err[Void](ERR_INTERNAL, "Missing FK child column", child.tableName & "." & child.columnName)
        for row in refs:
          let existsRes = readRowAt(pager, childTable, row.rowid)
          if not existsRes.ok:
            if existsRes.err.code == ERR_IO:
              continue
            return err[Void](existsRes.err.code, existsRes.err.message, existsRes.err.context)
          let current = existsRes.value
          var newValues = current.values
          if child.onUpdate == "CASCADE":
            newValues[childColIdx] = newVal
          else:
            newValues[childColIdx] = Value(kind: vkNull)

          let notNullRes = enforceNotNull(childTable, newValues)
          if not notNullRes.ok:
            return err[Void](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
          let checkRes = enforceChecks(childTable, newValues)
          if not checkRes.ok:
            return err[Void](checkRes.err.code, checkRes.err.message, checkRes.err.context)
          let uniqueRes = enforceUnique(catalog, pager, childTable, newValues, current.rowid)
          if not uniqueRes.ok:
            return err[Void](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
          let cascadeRes = enforceRestrictOnParent(catalog, pager, childTable, current.values, newValues)
          if not cascadeRes.ok:
            return err[Void](cascadeRes.err.code, cascadeRes.err.message, cascadeRes.err.context)
          let upRes = updateRow(pager, catalog, child.tableName, current.rowid, newValues)
          if not upRes.ok:
            return err[Void](upRes.err.code, upRes.err.message, upRes.err.context)
      else:
        return err[Void](ERR_SQL, "Unsupported ON UPDATE action", child.onUpdate)
  okVoid()

proc enforceRestrictOnDelete(catalog: Catalog, pager: Pager, table: TableMeta, oldValues: seq[Value]): Result[Void] =
  for i, col in table.columns:
    let children = catalog.referencingChildren(table.name, col.name)
    if children.len == 0:
      continue
    let oldVal = oldValues[i]
    if oldVal.kind == vkNull:
      continue
    for child in children:
      let childTableRes = catalog.getTable(child.tableName)
      if not childTableRes.ok:
        return err[Void](childTableRes.err.code, childTableRes.err.message, childTableRes.err.context)
      let childTable = childTableRes.value
      let refsRes = findReferencingRows(catalog, pager, childTable, child.columnName, oldVal)
      if not refsRes.ok:
        return err[Void](refsRes.err.code, refsRes.err.message, refsRes.err.context)
      let refs = refsRes.value
      if refs.len == 0:
        continue

      case child.onDelete
      of "NO ACTION", "RESTRICT":
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY RESTRICT violation", table.name & "." & col.name)
      of "CASCADE":
        for row in refs:
          let cascadeRes = enforceRestrictOnDelete(catalog, pager, childTable, row.values)
          if not cascadeRes.ok:
            return err[Void](cascadeRes.err.code, cascadeRes.err.message, cascadeRes.err.context)
        for row in refs:
          let existsRes = readRowAt(pager, childTable, row.rowid)
          if not existsRes.ok:
            if existsRes.err.code == ERR_IO:
              continue
            return err[Void](existsRes.err.code, existsRes.err.message, existsRes.err.context)
          let delRes = deleteRow(pager, catalog, child.tableName, row.rowid)
          if not delRes.ok:
            return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
      of "SET NULL":
        let childColIdx = columnIndexInTable(childTable, child.columnName)
        if childColIdx < 0:
          return err[Void](ERR_INTERNAL, "Missing FK child column", child.tableName & "." & child.columnName)
        for row in refs:
          let existsRes = readRowAt(pager, childTable, row.rowid)
          if not existsRes.ok:
            if existsRes.err.code == ERR_IO:
              continue
            return err[Void](existsRes.err.code, existsRes.err.message, existsRes.err.context)
          let current = existsRes.value
          var newValues = current.values
          newValues[childColIdx] = Value(kind: vkNull)
          let notNullRes = enforceNotNull(childTable, newValues)
          if not notNullRes.ok:
            return err[Void](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
          let checkRes = enforceChecks(childTable, newValues)
          if not checkRes.ok:
            return err[Void](checkRes.err.code, checkRes.err.message, checkRes.err.context)
          let uniqueRes = enforceUnique(catalog, pager, childTable, newValues, current.rowid)
          if not uniqueRes.ok:
            return err[Void](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
          let fkRes = enforceForeignKeys(catalog, pager, childTable, newValues)
          if not fkRes.ok:
            return err[Void](fkRes.err.code, fkRes.err.message, fkRes.err.context)
          let restrictRes = enforceRestrictOnParent(catalog, pager, childTable, current.values, newValues)
          if not restrictRes.ok:
            return err[Void](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)
          let upRes = updateRow(pager, catalog, child.tableName, current.rowid, newValues)
          if not upRes.ok:
            return err[Void](upRes.err.code, upRes.err.message, upRes.err.context)
      else:
        return err[Void](ERR_SQL, "Unsupported ON DELETE action", child.onDelete)
  okVoid()

proc evalInsertValues(stmt: Statement, params: seq[Value]): Result[seq[Value]] =
  # Hot-path: INSERT ... VALUES ($1, $2, ...) is extremely common.
  # Avoid evalExpr/valueFromSql overhead when all values are parameters.
  if stmt.insertValues.len > 0:
    var allParams = true
    var fastValues = newSeq[Value](stmt.insertValues.len)
    for i, expr in stmt.insertValues:
      if expr.kind != ekParam:
        allParams = false
        break
      let paramIdx = expr.index - 1
      if paramIdx < 0 or paramIdx >= params.len:
        return err[seq[Value]](ERR_SQL, "Parameter index out of bounds", $expr.index)
      fastValues[i] = params[paramIdx]
    if allParams:
      return ok(fastValues)

  var values: seq[Value] = @[]
  for expr in stmt.insertValues:
    let res = evalExpr(Row(), expr, params)
    if not res.ok:
      return err[seq[Value]](res.err.code, res.err.message, res.err.context)
    values.add(valueFromSql(res.value))
  ok(values)

proc evalInsertExprs(exprs: seq[Expr], params: seq[Value]): Result[seq[Value]] {.used.} =
  var values: seq[Value] = @[]
  for expr in exprs:
    let res = evalExpr(Row(), expr, params)
    if not res.ok:
      return err[seq[Value]](res.err.code, res.err.message, res.err.context)
    values.add(valueFromSql(res.value))
  ok(values)

type InsertExecResult = object
  affected: bool
  row: Option[Row]

type MultiInsertResult = object
  totalAffected: int64
  rows: seq[Row]

proc buildInsertResultRow(tableName: string, table: TableMeta, rowid: uint64, values: seq[Value]): Row =
  var cols: seq[string] = @[]
  for col in table.columns:
    cols.add(tableName & "." & col.name)
  Row(rowid: rowid, columns: cols, values: values)

# Forward declarations for transaction control (defined later)
proc beginTransaction*(db: Db): Result[Void]
proc commitTransaction*(db: Db): Result[Void]
proc rollbackTransaction*(db: Db): Result[Void]

# Thread-local reusable values seq to avoid per-insert allocation
var gInsertValues {.threadvar.}: seq[Value]

proc execInsertStatement(db: Db, bound: Statement, params: seq[Value], wantRow: bool, profile: InsertWriteProfile = InsertWriteProfile()): Result[InsertExecResult] =
  when defined(bench_breakdown):
    let engStart = getMonoTime()
    var nsEvalValues: int64 = 0
    var nsTypeCheck: int64 = 0
    var nsConstraints: int64 = 0
    var nsStorage: int64 = 0
    defer:
      if result.ok:
        let engTotal = int64(inNanoseconds(getMonoTime() - engStart))
        addEngineTotalNs(engTotal)
        addEngineEvalValuesNs(nsEvalValues)
        addEngineTypeCheckNs(nsTypeCheck)
        addEngineConstraintsNs(nsConstraints)
        addEngineStorageCallNs(nsStorage)

  let tablePtr = db.catalog.getTablePtr(bound.insertTable)
  if tablePtr == nil:
    return err[InsertExecResult](ERR_SQL, "Table not found", bound.insertTable)
  template table: untyped = tablePtr[]

  var useParamsDirect = false
  when defined(bench_breakdown):
    let t0 = getMonoTime()
    if profile.valid and profile.isIdentityParamMapping and params.len == profile.allParamIndices.len:
      useParamsDirect = true
    elif profile.valid and profile.allParamIndices.len > 0:
      let numVals = profile.allParamIndices.len
      if gInsertValues.len != numVals:
        gInsertValues.setLen(numVals)
      for i, paramIdx in profile.allParamIndices:
        gInsertValues[i] = params[paramIdx]
    else:
      let valuesRes = evalInsertValues(bound, params)
      if not valuesRes.ok:
        return err[InsertExecResult](valuesRes.err.code, valuesRes.err.message, valuesRes.err.context)
      gInsertValues = valuesRes.value
    nsEvalValues = int64(inNanoseconds(getMonoTime() - t0))
  else:
    if profile.valid and profile.isIdentityParamMapping and params.len == profile.allParamIndices.len:
      useParamsDirect = true
    elif profile.valid and profile.allParamIndices.len > 0:
      let numVals = profile.allParamIndices.len
      if gInsertValues.len != numVals:
        gInsertValues.setLen(numVals)
      for i, paramIdx in profile.allParamIndices:
        gInsertValues[i] = params[paramIdx]
    else:
      let valuesRes = evalInsertValues(bound, params)
      if not valuesRes.ok:
        return err[InsertExecResult](valuesRes.err.code, valuesRes.err.message, valuesRes.err.context)
      gInsertValues = valuesRes.value
  # Use pointer to avoid per-access branch in the template.
  var valuesPtr: ptr seq[Value]
  if useParamsDirect:
    valuesPtr = unsafeAddr params
  else:
    valuesPtr = addr gInsertValues
  template values: untyped = valuesPtr[]

  when defined(bench_breakdown):
    let t1 = getMonoTime()
    for i, col in table.columns:
      if not typeCheckFast(col, values[i]):
        let typeRes = typeCheckValue(col, values[i])
        if not typeRes.ok:
          return err[InsertExecResult](typeRes.err.code, typeRes.err.message, col.name)
        values[i] = typeRes.value
    nsTypeCheck = int64(inNanoseconds(getMonoTime() - t1))
  else:
    for i, col in table.columns:
      if not typeCheckFast(col, values[i]):
        let typeRes = typeCheckValue(col, values[i])
        if not typeRes.ok:
          return err[InsertExecResult](typeRes.err.code, typeRes.err.message, col.name)
        values[i] = typeRes.value

  when defined(bench_breakdown):
    let t2 = getMonoTime()
    if not profile.valid or profile.hasNotNullConstraints:
      let notNullRes = enforceNotNull(table, values, skipAutoIncrementPk = true)
      if not notNullRes.ok:
        return err[InsertExecResult](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
    if not profile.valid or profile.hasChecks:
      let checkRes = enforceChecks(table, values)
      if not checkRes.ok:
        return err[InsertExecResult](checkRes.err.code, checkRes.err.message, checkRes.err.context)
    nsConstraints += int64(inNanoseconds(getMonoTime() - t2))
  else:
    if not profile.valid or profile.hasNotNullConstraints:
      let notNullRes = enforceNotNull(table, values, skipAutoIncrementPk = true)
      if not notNullRes.ok:
        return err[InsertExecResult](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
    if not profile.valid or profile.hasChecks:
      let checkRes = enforceChecks(table, values)
      if not checkRes.ok:
        return err[InsertExecResult](checkRes.err.code, checkRes.err.message, checkRes.err.context)

  case bound.insertConflictAction
  of icaDoNothing:
    if bound.insertConflictTargetCols.len > 0:
      let targetConflictRes = hasUniqueConflictOnTarget(
        db.catalog,
        db.pager,
        table,
        values,
        bound.insertConflictTargetCols
      )
      if not targetConflictRes.ok:
        return err[InsertExecResult](targetConflictRes.err.code, targetConflictRes.err.message, targetConflictRes.err.context)
      if targetConflictRes.value:
        return ok(InsertExecResult(affected: false, row: none(Row)))
    else:
      let anyConflictRes = hasAnyUniqueConflict(db.catalog, db.pager, table, values)
      if not anyConflictRes.ok:
        return err[InsertExecResult](anyConflictRes.err.code, anyConflictRes.err.message, anyConflictRes.err.context)
      if anyConflictRes.value:
        return ok(InsertExecResult(affected: false, row: none(Row)))
  of icaDoUpdate:
    let conflictRowRes = findConflictRowidOnTarget(
      db.catalog,
      db.pager,
      table,
      values,
      bound.insertConflictTargetCols
    )
    if not conflictRowRes.ok:
      return err[InsertExecResult](conflictRowRes.err.code, conflictRowRes.err.message, conflictRowRes.err.context)
    if conflictRowRes.value.isSome:
      let conflictRowid = conflictRowRes.value.get
      let storedRes = readRowAt(db.pager, table, conflictRowid)
      if not storedRes.ok:
        return err[InsertExecResult](storedRes.err.code, storedRes.err.message, storedRes.err.context)
      let stored = storedRes.value

      var evalCols: seq[string] = @[]
      var evalValues: seq[Value] = @[]
      for i, col in table.columns:
        evalCols.add(bound.insertTable & "." & col.name)
        evalValues.add(stored.values[i])
      for i, col in table.columns:
        evalCols.add("excluded." & col.name)
        evalValues.add(values[i])
      let evalRow = Row(rowid: stored.rowid, columns: evalCols, values: evalValues)

      if bound.insertConflictUpdateWhere != nil:
        let whereRes = evalExpr(evalRow, bound.insertConflictUpdateWhere, params)
        if not whereRes.ok:
          return err[InsertExecResult](whereRes.err.code, whereRes.err.message, whereRes.err.context)
        if not valueToBool(whereRes.value):
          return ok(InsertExecResult(affected: false, row: none(Row)))

      var newValues = stored.values
      for colName, expr in bound.insertConflictUpdateAssignments:
        var idx = -1
        for i, col in table.columns:
          if col.name == colName:
            idx = i
            break
        if idx < 0:
          return err[InsertExecResult](ERR_SQL, "Unknown column", colName)
        let evalRes = evalExpr(evalRow, expr, params)
        if not evalRes.ok:
          return err[InsertExecResult](evalRes.err.code, evalRes.err.message, evalRes.err.context)
        let typeRes = typeCheckValue(table.columns[idx], evalRes.value)
        if not typeRes.ok:
          return err[InsertExecResult](typeRes.err.code, typeRes.err.message, colName)
        newValues[idx] = typeRes.value

      let newNotNullRes = enforceNotNull(table, newValues)
      if not newNotNullRes.ok:
        return err[InsertExecResult](newNotNullRes.err.code, newNotNullRes.err.message, newNotNullRes.err.context)
      let newCheckRes = enforceChecks(table, newValues)
      if not newCheckRes.ok:
        return err[InsertExecResult](newCheckRes.err.code, newCheckRes.err.message, newCheckRes.err.context)

      let uniqueUpdateRes = enforceUnique(db.catalog, db.pager, table, newValues, stored.rowid)
      if not uniqueUpdateRes.ok:
        return err[InsertExecResult](uniqueUpdateRes.err.code, uniqueUpdateRes.err.message, uniqueUpdateRes.err.context)

      let fkUpdateRes = enforceForeignKeys(db.catalog, db.pager, table, newValues)
      if not fkUpdateRes.ok:
        return err[InsertExecResult](fkUpdateRes.err.code, fkUpdateRes.err.message, fkUpdateRes.err.context)

      let restrictRes = enforceRestrictOnParent(db.catalog, db.pager, table, stored.values, newValues)
      if not restrictRes.ok:
        return err[InsertExecResult](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)

      let updateRes = updateRow(db.pager, db.catalog, bound.insertTable, stored.rowid, newValues)
      if not updateRes.ok:
        return err[InsertExecResult](updateRes.err.code, updateRes.err.message, updateRes.err.context)
      return ok(InsertExecResult(
        affected: true,
        row: if wantRow: some(buildInsertResultRow(bound.insertTable, table, stored.rowid, newValues)) else: none(Row)
      ))
  of icaNone:
    discard

  when defined(bench_breakdown):
    let t3 = getMonoTime()
    if not profile.valid or profile.hasNonPkUniqueColumns or profile.hasCompositeUniqueIndexes:
      let uniqueRes = enforceUnique(db.catalog, db.pager, table, values)
      if not uniqueRes.ok:
        return err[InsertExecResult](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)

    if not profile.valid or profile.hasForeignKeys:
      let fkRes = enforceForeignKeys(db.catalog, db.pager, table, values)
      if not fkRes.ok:
        return err[InsertExecResult](fkRes.err.code, fkRes.err.message, fkRes.err.context)
    nsConstraints += int64(inNanoseconds(getMonoTime() - t3))

    let t4 = getMonoTime()
    let insertRes = if profile.valid:
        insertRowWithPtr(db.pager, db.catalog, bound.insertTable, values, profile.hasSecondaryIndexes, profile.singleInt64PkIdx, tablePtr)
      else:
        insertRow(db.pager, db.catalog, bound.insertTable, values)
    nsStorage = int64(inNanoseconds(getMonoTime() - t4))
    if not insertRes.ok:
      if bound.insertConflictAction == icaDoNothing and bound.insertConflictTargetCols.len == 0 and isUniqueConflictError(insertRes.err):
        return ok(InsertExecResult(affected: false, row: none(Row)))
      return err[InsertExecResult](insertRes.err.code, insertRes.err.message, insertRes.err.context)

    if wantRow:
      # Back-fill auto-increment PK value so RETURNING sees the assigned id
      for i, col in table.columns:
        if col.primaryKey and col.kind == ctInt64 and values[i].kind == vkNull:
          values[i] = Value(kind: vkInt64, int64Val: cast[int64](insertRes.value))

      return ok(InsertExecResult(
        affected: true,
        row: some(buildInsertResultRow(bound.insertTable, table, insertRes.value, values))
      ))
    else:
      return ok(InsertExecResult(affected: true, row: none(Row)))
  else:
    if not profile.valid or profile.hasNonPkUniqueColumns or profile.hasCompositeUniqueIndexes:
      let uniqueRes = enforceUnique(db.catalog, db.pager, table, values)
      if not uniqueRes.ok:
        return err[InsertExecResult](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)

    if not profile.valid or profile.hasForeignKeys:
      let fkRes = enforceForeignKeys(db.catalog, db.pager, table, values)
      if not fkRes.ok:
        return err[InsertExecResult](fkRes.err.code, fkRes.err.message, fkRes.err.context)

    let insertRes = if profile.valid:
        insertRowWithPtr(db.pager, db.catalog, bound.insertTable, values, profile.hasSecondaryIndexes, profile.singleInt64PkIdx, tablePtr)
      else:
        insertRow(db.pager, db.catalog, bound.insertTable, values)
  if not insertRes.ok:
    if bound.insertConflictAction == icaDoNothing and bound.insertConflictTargetCols.len == 0 and isUniqueConflictError(insertRes.err):
      return ok(InsertExecResult(affected: false, row: none(Row)))
    return err[InsertExecResult](insertRes.err.code, insertRes.err.message, insertRes.err.context)

  if wantRow:
    # Back-fill auto-increment PK value so RETURNING sees the assigned id
    for i, col in table.columns:
      if col.primaryKey and col.kind == ctInt64 and values[i].kind == vkNull:
        values[i] = Value(kind: vkInt64, int64Val: cast[int64](insertRes.value))

    return ok(InsertExecResult(
      affected: true,
      row: some(buildInsertResultRow(bound.insertTable, table, insertRes.value, values))
    ))
  ok(InsertExecResult(affected: true, row: none(Row)))

proc execInsertRowExprs(db: Db, bound: Statement, exprs: seq[Expr], params: seq[Value], wantRow: bool, profile: InsertWriteProfile = InsertWriteProfile()): Result[InsertExecResult] =
  ## Execute INSERT for a single extra row (multi-row INSERT VALUES).
  ## Builds a temporary statement with the given exprs as insertValues.
  var rowBound = bound
  rowBound.insertValues = exprs
  rowBound.insertValueRows = @[]
  execInsertStatement(db, rowBound, params, wantRow, profile)

proc execAllInsertRows(db: Db, bound: Statement, params: seq[Value], wantRows: bool, profile: InsertWriteProfile = InsertWriteProfile()): Result[MultiInsertResult] =
  ## Execute all rows of a multi-row INSERT, returning total affected and result rows.
  var multi = MultiInsertResult(totalAffected: 0, rows: @[])
  # First row (from insertValues)
  let firstRes = execInsertStatement(db, bound, params, wantRows, profile)
  if not firstRes.ok:
    return err[MultiInsertResult](firstRes.err.code, firstRes.err.message, firstRes.err.context)
  if firstRes.value.affected:
    multi.totalAffected.inc
  if wantRows and firstRes.value.row.isSome:
    multi.rows.add(firstRes.value.row.get)
  # Extra rows (from insertValueRows)
  for extraExprs in bound.insertValueRows:
    let extraRes = execInsertRowExprs(db, bound, extraExprs, params, wantRows, profile)
    if not extraRes.ok:
      return err[MultiInsertResult](extraRes.err.code, extraRes.err.message, extraRes.err.context)
    if extraRes.value.affected:
      multi.totalAffected.inc
    if wantRows and extraRes.value.row.isSome:
      multi.rows.add(extraRes.value.row.get)
  ok(multi)

proc buildUpdateWriteProfile(catalog: Catalog, table: TableMeta, assignments: Table[string, Expr]): UpdateWriteProfile =
  var updatedCols = initHashSet[string]()
  for colName, _ in assignments:
    updatedCols.incl(colName)

  var hasNotNullOnUpdated = false
  var hasUniqueOnUpdated = false
  var hasForeignKeysOnUpdated = false
  for col in table.columns:
    if col.name notin updatedCols:
      continue
    if col.notNull:
      hasNotNullOnUpdated = true
    if col.unique or col.primaryKey:
      hasUniqueOnUpdated = true
    if col.refTable.len > 0 and col.refColumn.len > 0:
      hasForeignKeysOnUpdated = true

  var updatesIndexedColumns = false
  for _, idx in catalog.indexes:
    if idx.table != table.name:
      continue
    for idxCol in idx.columns:
      if idxCol.startsWith(IndexExpressionPrefix):
        updatesIndexedColumns = true
        break
      if idxCol in updatedCols:
        updatesIndexedColumns = true
        break
    if updatesIndexedColumns:
      break

  result.hasUpdateTriggers = catalog.hasTriggersForTable(table.name, TriggerEventUpdateMask)
  result.hasCheckConstraints = table.checks.len > 0
  result.hasNotNullOnUpdatedCols = hasNotNullOnUpdated
  result.hasUniqueOnUpdatedCols = hasUniqueOnUpdated
  result.hasForeignKeysOnUpdatedCols = hasForeignKeysOnUpdated
  result.isParentOfForeignKeys = catalog.hasReferencingChildren(table.name)
  result.updatesIndexedColumns = updatesIndexedColumns
  result.isSimpleUpdate =
    not result.hasUpdateTriggers and
    not result.hasCheckConstraints and
    not result.hasNotNullOnUpdatedCols and
    not result.hasUniqueOnUpdatedCols and
    not result.hasForeignKeysOnUpdatedCols and
    not result.isParentOfForeignKeys and
    not result.updatesIndexedColumns

proc buildInsertWriteProfile(catalog: Catalog, tableName: string): InsertWriteProfile =
  result.valid = true
  result.singleInt64PkIdx = -1
  result.isView = catalog.hasViewName(tableName)
  if result.isView:
    return
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return
  let table = tableRes.value
  result.hasChecks = table.checks.len > 0
  result.hasInsertTriggers = catalog.hasTriggersForTable(tableName, TriggerEventInsertMask)
  # Check for FK refs and non-PK unique columns
  var pkCount = 0
  for col in table.columns:
    if col.primaryKey:
      inc pkCount
  for i, col in table.columns:
    if col.refTable.len > 0 and col.refColumn.len > 0:
      result.hasForeignKeys = true
    if col.unique and not col.primaryKey:
      result.hasNonPkUniqueColumns = true
    if col.primaryKey and pkCount == 1 and col.kind != ctInt64:
      # Non-INT64 PK: btree won't enforce uniqueness, need enforceUnique
      result.hasNonPkUniqueColumns = true
    if col.primaryKey and pkCount > 1:
      # Composite PK: handled by composite unique index check
      result.hasNonPkUniqueColumns = true
    if col.primaryKey and pkCount == 1 and col.kind == ctInt64:
      result.singleInt64PkIdx = i
      # INT64 PK uniqueness is enforced by btree.insert(checkUnique=true)
    if col.notNull and not col.primaryKey:
      result.hasNotNullConstraints = true
    if col.kind in {ctText, ctBlob}:
      result.hasTextBlobColumns = true
  # Check for composite unique indexes
  for _, idx in catalog.indexes:
    if idx.table != tableName:
      continue
    result.hasSecondaryIndexes = true
    if idx.columns.len >= 2 and idx.unique:
      result.hasCompositeUniqueIndexes = true

proc prepare*(db: Db, sqlText: string): Result[Prepared] =
  if not db.isOpen:
    return err[Prepared](ERR_INTERNAL, "Database not open")

  let parseRes = parseSql(sqlText)
  if not parseRes.ok:
    return err[Prepared](parseRes.err.code, parseRes.err.message, parseRes.err.context)

  var boundStatements: seq[Statement] = @[]
  var plans: seq[Plan] = @[]
  var updateProfiles: seq[UpdateWriteProfile] = @[]
  var insertProfiles: seq[InsertWriteProfile] = @[]

  for stmt in parseRes.value.statements:
    let bindRes = bindStatement(db.catalog, stmt)
    if not bindRes.ok:
      return err[Prepared](bindRes.err.code, bindRes.err.message, bindRes.err.context)
    let bound = bindRes.value
    boundStatements.add(bound)
    updateProfiles.add(UpdateWriteProfile())
    if bound.kind == skInsert:
      var profile = buildInsertWriteProfile(db.catalog, bound.insertTable)
      # Precompute param index mapping for fast eval
      if bound.insertValues.len > 0:
        var indices = newSeq[int](bound.insertValues.len)
        var allParams = true
        for i, expr in bound.insertValues:
          if expr.kind != ekParam or expr.index < 1:
            allParams = false
            break
          indices[i] = expr.index - 1
        if allParams:
          profile.allParamIndices = indices
          # Check if indices are a straight identity mapping [0,1,...,n-1]
          var isIdentity = true
          for i, idx in indices:
            if idx != i:
              isIdentity = false
              break
          profile.isIdentityParamMapping = isIdentity
      insertProfiles.add(profile)
    else:
      insertProfiles.add(InsertWriteProfile())

    if bound.kind == skSelect:
      let planRes = plan(db.catalog, bound)
      if not planRes.ok:
        return err[Prepared](planRes.err.code, planRes.err.message, planRes.err.context)
      plans.add(planRes.value)
    elif bound.kind == skUpdate:
      let tableRes = db.catalog.getTable(bound.updateTable)
      if not tableRes.ok:
        return err[Prepared](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let table = tableRes.value
      updateProfiles[^1] = buildUpdateWriteProfile(db.catalog, table, bound.assignments)
      var selectItems: seq[SelectItem] = @[]
      for col in table.columns:
        selectItems.add(SelectItem(expr: Expr(kind: ekColumn, name: col.name, table: bound.updateTable)))
      
      let sel = Statement(
        kind: skSelect,
        fromTable: bound.updateTable,
        fromAlias: "",
        selectItems: selectItems,
        whereExpr: bound.updateWhere,
        joins: @[],
        groupBy: @[],
        havingExpr: nil,
        orderBy: @[],
        limit: -1,
        offset: -1
      )
      let planRes = plan(db.catalog, sel)
      if not planRes.ok:
        return err[Prepared](planRes.err.code, planRes.err.message, planRes.err.context)
      plans.add(planRes.value)
    elif bound.kind == skDelete:
      let tableRes = db.catalog.getTable(bound.deleteTable)
      if not tableRes.ok:
        return err[Prepared](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let table = tableRes.value
      var selectItems: seq[SelectItem] = @[]
      for col in table.columns:
        selectItems.add(SelectItem(expr: Expr(kind: ekColumn, name: col.name, table: bound.deleteTable)))
      
      let sel = Statement(
        kind: skSelect,
        fromTable: bound.deleteTable,
        fromAlias: "",
        selectItems: selectItems,
        whereExpr: bound.deleteWhere,
        joins: @[],
        groupBy: @[],
        havingExpr: nil,
        orderBy: @[],
        limit: -1,
        offset: -1
      )
      let planRes = plan(db.catalog, sel)
      if not planRes.ok:
        return err[Prepared](planRes.err.code, planRes.err.message, planRes.err.context)
      plans.add(planRes.value)
    elif bound.kind == skExplain and bound.explainInner.kind == skSelect:
      let planRes = plan(db.catalog, bound.explainInner)
      if not planRes.ok:
        return err[Prepared](planRes.err.code, planRes.err.message, planRes.err.context)
      plans.add(planRes.value)
    else:
      plans.add(nil)

  var prepared = Prepared(
    db: db,
    sql: sqlText,
    schemaCookie: db.schemaCookie,
    statements: boundStatements,
    plans: plans,
    updateProfiles: updateProfiles,
    insertProfiles: insertProfiles
  )
  # Detect fast-path: single INSERT statement with identity params, no constraints/triggers/RETURNING
  if boundStatements.len == 1 and boundStatements[0].kind == skInsert:
    let prof = insertProfiles[0]
    let bound = boundStatements[0]
    if prof.valid and prof.isIdentityParamMapping and
       not prof.hasInsertTriggers and not prof.isView and
       not prof.hasNonPkUniqueColumns and not prof.hasCompositeUniqueIndexes and
       not prof.hasForeignKeys and not prof.hasNotNullConstraints and not prof.hasChecks and
       bound.insertReturning.len == 0 and bound.insertValueRows.len == 0 and
       bound.insertConflictAction == icaNone:
      prepared.isFastInsert = true
      prepared.fastInsertProfile = prof
      prepared.fastInsertBound = 0
      prepared.fastInsertTablePtr = db.catalog.getTablePtr(bound.insertTable)
  ok(prepared)

proc execSql*(db: Db, sqlText: string, params: seq[Value]): Result[seq[string]]

# Forward declaration
proc execPreparedNonSelect*(db: Db, bound: Statement, params: seq[Value], plan: Plan = nil, updateProfile: UpdateWriteProfile = UpdateWriteProfile()): Result[int64]

const MaxTriggerExecutionDepth = 16
var gTriggerExecutionDepth {.threadvar.}: int

proc runTriggerActions(db: Db, triggers: seq[TriggerMeta], affectedRows: int64): Result[Void] =
  if affectedRows <= 0 or triggers.len == 0:
    return okVoid()
  if gTriggerExecutionDepth >= MaxTriggerExecutionDepth:
    return err[Void](ERR_SQL, "Trigger recursion depth exceeded", "max_depth=" & $MaxTriggerExecutionDepth)
  gTriggerExecutionDepth.inc
  defer:
    gTriggerExecutionDepth.dec
  for _ in 0 ..< int(affectedRows):
    for trigger in triggers:
      let execRes = execSql(db, trigger.actionSql, @[])
      if not execRes.ok:
        return err[Void](execRes.err.code, "Trigger action failed: " & execRes.err.message, trigger.name)
  okVoid()

proc executeAfterTriggers(db: Db, tableName: string, eventMask: int, affectedRows: int64): Result[Void] =
  if not db.catalog.hasTriggersForTable(tableName, eventMask):
    return okVoid()
  var afterTriggers: seq[TriggerMeta] = @[]
  for trigger in db.catalog.listTriggersForTable(tableName, eventMask):
    if (trigger.eventsMask and TriggerTimingInsteadMask) == 0:
      afterTriggers.add(trigger)
  runTriggerActions(db, afterTriggers, affectedRows)

proc executeInsteadTriggers(db: Db, objectName: string, eventMask: int, affectedRows: int64): Result[Void] =
  if not db.catalog.hasTriggersForTable(objectName, eventMask):
    return okVoid()
  var insteadTriggers: seq[TriggerMeta] = @[]
  for trigger in db.catalog.listTriggersForTable(objectName, eventMask):
    if (trigger.eventsMask and TriggerTimingInsteadMask) != 0:
      insteadTriggers.add(trigger)
  runTriggerActions(db, insteadTriggers, affectedRows)

proc countViewRows(db: Db, viewName: string, whereExpr: Expr, params: seq[Value]): Result[int64] =
  var countSql = "SELECT COUNT(*) FROM " & viewName
  if whereExpr != nil:
    countSql.add(" WHERE " & exprToCanonicalSql(whereExpr))
  let rowsRes = execSql(db, countSql, params)
  if not rowsRes.ok:
    return err[int64](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
  if rowsRes.value.len == 0:
    return ok(0'i64)
  let countText = rowsRes.value[0].strip()
  try:
    ok(parseBiggestInt(countText).int64)
  except ValueError:
    err[int64](ERR_INTERNAL, "Invalid COUNT(*) result", countText)

proc execPrepared*(prepared: Prepared, params: seq[Value]): Result[seq[string]] =
  if not prepared.db.isOpen:
    return err[seq[string]](ERR_INTERNAL, "Database not open")
  
  if prepared.schemaCookie != prepared.db.schemaCookie:
    # Schema changed, need to re-prepare
    # For now, just error out or re-prepare implicitly?
    # Simpler to error out and let user handle it, but for benchmarks we assume stable schema.
    # Let's try to re-prepare internally.
    let reRes = prepare(prepared.db, prepared.sql)
    if not reRes.ok:
      return err[seq[string]](reRes.err.code, "Schema changed and re-prepare failed: " & reRes.err.message, reRes.err.context)
    let newPrep = reRes.value
    prepared.schemaCookie = newPrep.schemaCookie
    prepared.statements = newPrep.statements
    prepared.plans = newPrep.plans
    prepared.updateProfiles = newPrep.updateProfiles
    prepared.insertProfiles = newPrep.insertProfiles
    prepared.isFastInsert = newPrep.isFastInsert
    prepared.fastInsertProfile = newPrep.fastInsertProfile
    prepared.fastInsertBound = newPrep.fastInsertBound
    prepared.fastInsertTablePtr = newPrep.fastInsertTablePtr

  let db = prepared.db

  # Fast path: single INSERT with identity params, no constraints/triggers/RETURNING.
  # Bypasses execInsertStatement to eliminate Result[InsertExecResult] wrapping.
  if prepared.isFastInsert:
    let profile = prepared.fastInsertProfile
    let tablePtr = prepared.fastInsertTablePtr
    if tablePtr != nil and params.len == profile.allParamIndices.len:
      # Type check (required for correctness)
      for i, col in tablePtr.columns:
        if not typeCheckFast(col, params[i]):
          let typeRes = typeCheckValue(col, params[i])
          if not typeRes.ok:
            return err[seq[string]](typeRes.err.code, typeRes.err.message, col.name)
      # Direct storage insert — skips execInsertStatement overhead
      let insertRes = insertRowDirect(db.pager, db.catalog,
                                        params, profile.singleInt64PkIdx, tablePtr)
      if not insertRes.ok:
        return err[seq[string]](insertRes.err.code, insertRes.err.message, insertRes.err.context)
      var r: Result[seq[string]]
      r.ok = true
      return r

  var output: seq[string] = @[]

  for i, bound in prepared.statements:
    let plan = prepared.plans[i]
    case bound.kind
    of skBegin:
      let res = beginTransaction(db)
      if not res.ok: return err[seq[string]](res.err.code, res.err.message, res.err.context)
    of skCommit:
      let res = commitTransaction(db)
      if not res.ok: return err[seq[string]](res.err.code, res.err.message, res.err.context)
    of skRollback:
      let res = rollbackTransaction(db)
      if not res.ok: return err[seq[string]](res.err.code, res.err.message, res.err.context)
    of skExplain:
      if bound.explainAnalyze:
        let t0 = getMonoTime()
        let rowsRes = execPlan(db.pager, db.catalog, plan, params)
        let t1 = getMonoTime()
        if not rowsRes.ok:
          return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
        let metrics = PlanMetrics(
          actualRows: rowsRes.value.len,
          actualTimeMs: (t1 - t0).inNanoseconds.float64 / 1_000_000.0
        )
        let lines = explainAnalyzePlanLines(db.catalog, plan, metrics)
        for line in lines:
          output.add(line)
      else:
        let explainStr = explainPlanLines(db.catalog, plan).join("\n")
        output.add(explainStr)
    of skSelect:
      let cursorRes = openRowCursor(db.pager, db.catalog, plan, params)
      if not cursorRes.ok:
        return err[seq[string]](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
      let cursor = cursorRes.value
      var rows: seq[string] = @[]
      # Limit output for safety
      var limit = 1000
      while true:
        let nextRes = rowCursorNext(cursor)
        if not nextRes.ok:
          return err[seq[string]](nextRes.err.code, nextRes.err.message, nextRes.err.context)
        if nextRes.value.isNone:
          break
        let row = nextRes.value.get
        var parts: seq[string] = @[]
        for v in row.values:
          parts.add($v)
        rows.add(parts.join("|"))
        limit.dec
        if limit == 0:
          break
      output.add(rows.join("\n"))
    of skInsert:
      let insertProfile =
        if i < prepared.insertProfiles.len:
          prepared.insertProfiles[i]
        else:
          InsertWriteProfile()
      if insertProfile.valid and insertProfile.isView:
        let rowCount = int64(1 + bound.insertValueRows.len)
        let insteadRes = executeInsteadTriggers(db, bound.insertTable, TriggerEventInsertMask, if bound.insertValues.len > 0: rowCount else: 0)
        if not insteadRes.ok:
          return err[seq[string]](insteadRes.err.code, insteadRes.err.message, insteadRes.err.context)
      elif not insertProfile.valid and db.catalog.hasViewName(bound.insertTable):
        let rowCount = int64(1 + bound.insertValueRows.len)
        let insteadRes = executeInsteadTriggers(db, bound.insertTable, TriggerEventInsertMask, if bound.insertValues.len > 0: rowCount else: 0)
        if not insteadRes.ok:
          return err[seq[string]](insteadRes.err.code, insteadRes.err.message, insteadRes.err.context)
      else:
        # Fast path: single-row insert, no RETURNING, no triggers → skip MultiInsertResult allocation
        if insertProfile.valid and not insertProfile.hasInsertTriggers and bound.insertReturning.len == 0 and bound.insertValueRows.len == 0:
          let singleRes = execInsertStatement(db, bound, params, wantRow = false, profile = insertProfile)
          if not singleRes.ok:
            return err[seq[string]](singleRes.err.code, singleRes.err.message, singleRes.err.context)
        else:
          let multiRes = execAllInsertRows(db, bound, params, wantRows = bound.insertReturning.len > 0, profile = insertProfile)
          if not multiRes.ok:
            return err[seq[string]](multiRes.err.code, multiRes.err.message, multiRes.err.context)
          if not insertProfile.valid or insertProfile.hasInsertTriggers:
            let triggerRes = executeAfterTriggers(db, bound.insertTable, TriggerEventInsertMask, multiRes.value.totalAffected)
            if not triggerRes.ok:
              return err[seq[string]](triggerRes.err.code, triggerRes.err.message, triggerRes.err.context)
          if bound.insertReturning.len > 0 and multiRes.value.rows.len > 0:
            let projectedRes = projectRows(multiRes.value.rows, bound.insertReturning, params)
            if not projectedRes.ok:
              return err[seq[string]](projectedRes.err.code, projectedRes.err.message, projectedRes.err.context)
            for row in projectedRes.value:
              var parts: seq[string] = @[]
              for v in row.values:
                parts.add($v)
              output.add(parts.join("|"))
    of skUpdate:
      let updateProfile =
        if i < prepared.updateProfiles.len:
          prepared.updateProfiles[i]
        else:
          UpdateWriteProfile()
      let execRes = execPreparedNonSelect(db, bound, params, plan, updateProfile)
      if not execRes.ok:
        return err[seq[string]](execRes.err.code, execRes.err.message, execRes.err.context)
    of skDelete:
       let execRes = execPreparedNonSelect(db, bound, params, plan)
       if not execRes.ok:
         return err[seq[string]](execRes.err.code, execRes.err.message, execRes.err.context)
    of skCreateTable, skDropTable, skCreateIndex, skDropIndex, skCreateTrigger, skDropTrigger, skCreateView, skDropView, skAlterView, skAlterTable:
       # DDL invalidates prepared statements anyway
       return execSql(db, prepared.sql, params)

  ok(output)

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
    let t0 = getMonoTime()
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
    let t1 = getMonoTime()
    if sqlText.contains("count(*)"):
      stderr.writeLine("Plan: " & $((t1 - t0).inNanoseconds.float / 1000.0) & "us")
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
      let t2 = getMonoTime()
      let rowsRes = execPlan(db.pager, db.catalog, usePlan, params)
      let t3 = getMonoTime()
      if sqlText.contains("count(*)"):
        stderr.writeLine("Exec(NoWAL): " & $((t3 - t2).inNanoseconds.float / 1000.0) & "us")
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
    let t2 = getMonoTime()
    let rowsRes = execPlan(db.pager, db.catalog, usePlan, params)
    let t3 = getMonoTime()
    if sqlText.contains("count(*)"):
      stderr.writeLine("Exec(WAL): " & $((t3 - t2).inNanoseconds.float / 1000.0) & "us")
    if not rowsRes.ok:
      return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
    for row in rowsRes.value:
      var parts: seq[string] = @[]
      for value in row.values:
        parts.add(valueToString(value))
      output.add(parts.join("|"))
    ok(output)
  for i, bound in boundStatements:
    let isWrite = bound.kind in {skCreateTable, skDropTable, skAlterTable, skCreateIndex, skDropIndex, skCreateTrigger, skDropTrigger, skCreateView, skDropView, skAlterView, skInsert, skUpdate, skDelete}
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
        let spec = typeRes.value
        columns.add(Column(
          name: col.name,
          kind: spec.kind,
          notNull: col.notNull,
          unique: col.unique,
          primaryKey: col.primaryKey,
          refTable: col.refTable,
          refColumn: col.refColumn,
          refOnDelete: col.refOnDelete,
          refOnUpdate: col.refOnUpdate,
          decPrecision: spec.decPrecision,
          decScale: spec.decScale
        ))
      var checks: seq[catalog.CheckConstraint] = @[]
      for checkDef in bound.createChecks:
        checks.add(catalog.CheckConstraint(name: checkDef.name, exprSql: exprToCanonicalSql(checkDef.expr)))
      let meta = TableMeta(name: bound.createTableName, rootPage: rootRes.value, nextRowId: 1, columns: columns, checks: checks)
      let saveRes = db.catalog.saveTable(db.pager, meta)
      if not saveRes.ok:
        return err[seq[string]](saveRes.err.code, saveRes.err.message, saveRes.err.context)
      # Collect PK columns for composite PK detection
      var pkCols: seq[string] = @[]
      for col in columns:
        if col.primaryKey:
          pkCols.add(col.name)
      if pkCols.len > 1:
        # Composite PK: single-INT64 optimization doesn't apply; create composite unique index
        let idxName = "pk_" & meta.name & "_" & pkCols.join("_") & "_idx"
        let idxRootRes = initTableRoot(db.pager)
        if not idxRootRes.ok:
          return err[seq[string]](idxRootRes.err.code, idxRootRes.err.message, idxRootRes.err.context)
        let buildRes = buildIndexForColumns(db.pager, db.catalog, meta.name, pkCols, idxRootRes.value)
        if not buildRes.ok:
          return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
        let idxMeta = IndexMeta(name: idxName, table: meta.name, columns: pkCols, rootPage: buildRes.value, kind: catalog.ikBtree, unique: true)
        let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
        if not idxSaveRes.ok:
          return err[seq[string]](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
      for col in columns:
        if col.primaryKey or col.unique:
          if col.primaryKey and pkCols.len > 1:
            continue  # handled by composite index above
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
            let idxMeta = IndexMeta(name: idxName, table: meta.name, columns: @[col.name], rootPage: buildRes.value, kind: catalog.ikBtree, unique: true)
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
            let idxMeta = IndexMeta(name: idxName, table: meta.name, columns: @[col.name], rootPage: buildRes.value, kind: catalog.ikBtree, unique: false)
            let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
            if not idxSaveRes.ok:
              return err[seq[string]](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skDropTable:
      if bound.dropTableIfExists and bound.dropTableName notin db.catalog.tables:
        discard  # IF EXISTS: silently skip
      else:
        let dependentViews = db.catalog.listDependentViews(bound.dropTableName)
        if dependentViews.len > 0:
          return err[seq[string]](ERR_SQL, "Cannot drop table with dependent views", bound.dropTableName)
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
      let predicateSql = if bound.indexPredicate != nil: exprToCanonicalSql(bound.indexPredicate) else: ""
      if bound.unique:
        let tableRes = db.catalog.getTable(bound.indexTableName)
        if not tableRes.ok:
          return err[seq[string]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
        let table = tableRes.value
        var colIndices: seq[int] = @[]
        for colName in bound.columnNames:
          var found = false
          for i, col in table.columns:
            if col.name == colName:
              colIndices.add(i)
              found = true
              break
          if not found:
            return err[seq[string]](ERR_SQL, "Column not found", colName)
        let rowsRes = scanTable(db.pager, table)
        if not rowsRes.ok:
          return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
        if colIndices.len == 1:
          let colKind = table.columns[colIndices[0]].kind
          var seen: Table[uint64, bool] = initTable[uint64, bool]()
          var seenVals: Table[uint64, seq[string]] = initTable[uint64, seq[string]]()
          for row in rowsRes.value:
            if row.values[colIndices[0]].kind == vkNull:
              continue
            let key = indexKeyFromValue(row.values[colIndices[0]])
            if colKind in {ctText, ctBlob} and row.values[colIndices[0]].kind in {vkText, vkBlob}:
              let vKey = valueBytesKey(row.values[colIndices[0]])
              if not seenVals.hasKey(key):
                seenVals[key] = @[vKey]
              else:
                if vKey in seenVals[key]:
                  return err[seq[string]](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnNames[0])
                seenVals[key].add(vKey)
            else:
              if seen.hasKey(key):
                return err[seq[string]](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnNames[0])
              seen[key] = true
        else:
          # Composite uniqueness check: hash all column values, verify via row comparison
          var seen: Table[uint64, seq[seq[Value]]] = initTable[uint64, seq[seq[Value]]]()
          for row in rowsRes.value:
            var hasNull = false
            var vals: seq[Value] = @[]
            for ci in colIndices:
              if row.values[ci].kind == vkNull:
                hasNull = true
                break
              vals.add(row.values[ci])
            if hasNull:
              continue
            let key = compositeIndexKey(row.values, colIndices)
            if seen.hasKey(key):
              for existing in seen[key]:
                var allMatch = true
                for j in 0..<vals.len:
                  if not valuesEqual(existing[j], vals[j]):
                    allMatch = false
                    break
                if allMatch:
                  return err[seq[string]](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnNames.join(","))
              seen[key].add(vals)
            else:
              seen[key] = @[vals]
      var finalRoot = indexRootRes.value
      if bound.indexKind == sql.ikTrigram:
        let buildRes = buildTrigramIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnNames[0], indexRootRes.value)
        if not buildRes.ok:
          return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
        finalRoot = buildRes.value
      else:
        if bound.columnNames.len == 1 and bound.columnNames[0].startsWith(IndexExpressionPrefix):
          let buildRes = buildIndexForExpression(db.pager, db.catalog, bound.indexTableName, bound.columnNames[0], indexRootRes.value)
          if not buildRes.ok:
            return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
          finalRoot = buildRes.value
        elif bound.columnNames.len == 1:
          let buildRes = buildIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnNames[0], indexRootRes.value, predicateSql)
          if not buildRes.ok:
            return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
          finalRoot = buildRes.value
        else:
          let buildRes = buildIndexForColumns(db.pager, db.catalog, bound.indexTableName, bound.columnNames, indexRootRes.value, predicateSql)
          if not buildRes.ok:
            return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
          finalRoot = buildRes.value
      let idxKind = if bound.indexKind == sql.ikTrigram: catalog.ikTrigram else: catalog.ikBtree
      let idxMeta = IndexMeta(
        name: bound.indexName,
        table: bound.indexTableName,
        columns: bound.columnNames,
        rootPage: finalRoot,
        kind: idxKind,
        unique: bound.unique,
        predicateSql: predicateSql
      )
      let saveRes = db.catalog.createIndexMeta(idxMeta)
      if not saveRes.ok:
        return err[seq[string]](saveRes.err.code, saveRes.err.message, saveRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skCreateTrigger:
      let triggerMeta = TriggerMeta(
        name: bound.triggerName,
        table: bound.triggerTableName,
        eventsMask: bound.triggerEventsMask,
        actionSql: bound.triggerActionSql
      )
      let createRes = db.catalog.createTriggerMeta(triggerMeta)
      if not createRes.ok:
        return err[seq[string]](createRes.err.code, createRes.err.message, createRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skDropTrigger:
      if not db.catalog.hasTrigger(bound.dropTriggerTableName, bound.dropTriggerName):
        if not bound.dropTriggerIfExists:
          return err[seq[string]](ERR_SQL, "Trigger not found", bound.dropTriggerTableName & "." & bound.dropTriggerName)
      else:
        let dropRes = db.catalog.dropTrigger(bound.dropTriggerTableName, bound.dropTriggerName)
        if not dropRes.ok:
          return err[seq[string]](dropRes.err.code, dropRes.err.message, dropRes.err.context)
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
    of skCreateView:
      let dependencies = viewDependencies(bound.createViewQuery)
      let viewMeta = ViewMeta(
        name: bound.createViewName,
        sqlText: bound.createViewSqlText,
        columnNames: bound.createViewColumns,
        dependencies: dependencies
      )
      if bound.createViewIfNotExists and db.catalog.hasViewName(bound.createViewName):
        discard
      elif bound.createViewOrReplace and db.catalog.hasViewName(bound.createViewName):
        let saveRes = db.catalog.saveViewMeta(viewMeta)
        if not saveRes.ok:
          return err[seq[string]](saveRes.err.code, saveRes.err.message, saveRes.err.context)
      else:
        let createRes = db.catalog.createViewMeta(viewMeta)
        if not createRes.ok:
          return err[seq[string]](createRes.err.code, createRes.err.message, createRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skDropView:
      if not db.catalog.hasViewName(bound.dropViewName):
        if bound.dropViewIfExists:
          discard
        else:
          return err[seq[string]](ERR_SQL, "View not found", bound.dropViewName)
      else:
        let dependentViews = db.catalog.listDependentViews(bound.dropViewName)
        if dependentViews.len > 0:
          return err[seq[string]](ERR_SQL, "Cannot drop view with dependent views", bound.dropViewName)
        let dropRes = db.catalog.dropView(bound.dropViewName)
        if not dropRes.ok:
          return err[seq[string]](dropRes.err.code, dropRes.err.message, dropRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skAlterView:
      let dependentViews = db.catalog.listDependentViews(bound.alterViewName)
      if dependentViews.len > 0:
        return err[seq[string]](ERR_SQL, "Cannot rename view with dependent views", bound.alterViewName)
      if db.catalog.hasTableOrViewName(bound.alterViewNewName):
        return err[seq[string]](ERR_SQL, "Target name already exists", bound.alterViewNewName)
      let renameRes = db.catalog.renameView(bound.alterViewName, bound.alterViewNewName)
      if not renameRes.ok:
        return err[seq[string]](renameRes.err.code, renameRes.err.message, renameRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skInsert:
      if db.catalog.hasViewName(bound.insertTable):
        let rowCount = int64(1 + bound.insertValueRows.len)
        let insteadRes = executeInsteadTriggers(db, bound.insertTable, TriggerEventInsertMask, if bound.insertValues.len > 0: rowCount else: 0)
        if not insteadRes.ok:
          return err[seq[string]](insteadRes.err.code, insteadRes.err.message, insteadRes.err.context)
      else:
        let multiRes = execAllInsertRows(db, bound, params, wantRows = bound.insertReturning.len > 0)
        if not multiRes.ok:
          return err[seq[string]](multiRes.err.code, multiRes.err.message, multiRes.err.context)
        let triggerRes = executeAfterTriggers(db, bound.insertTable, TriggerEventInsertMask, multiRes.value.totalAffected)
        if not triggerRes.ok:
          return err[seq[string]](triggerRes.err.code, triggerRes.err.message, triggerRes.err.context)
        if bound.insertReturning.len > 0 and multiRes.value.rows.len > 0:
          let projectedRes = projectRows(multiRes.value.rows, bound.insertReturning, params)
          if not projectedRes.ok:
            return err[seq[string]](projectedRes.err.code, projectedRes.err.message, projectedRes.err.context)
          for row in projectedRes.value:
            var parts: seq[string] = @[]
            for value in row.values:
              parts.add(valueToString(value))
            output.add(parts.join("|"))
    of skUpdate:
      if db.catalog.hasViewName(bound.updateTable):
        let countRes = countViewRows(db, bound.updateTable, bound.updateWhere, params)
        if not countRes.ok:
          return err[seq[string]](countRes.err.code, countRes.err.message, countRes.err.context)
        let insteadRes = executeInsteadTriggers(db, bound.updateTable, TriggerEventUpdateMask, countRes.value)
        if not insteadRes.ok:
          return err[seq[string]](insteadRes.err.code, insteadRes.err.message, insteadRes.err.context)
      else:
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
              let typeRes = typeCheckValue(table.columns[idx], evalRes.value)
              if not typeRes.ok:
                return err[seq[string]](typeRes.err.code, typeRes.err.message, colName)
              newValues[idx] = typeRes.value
          updates.add((stored.rowid, stored.values, newValues))
        for entry in updates:
          let notNullRes = enforceNotNull(table, entry[2])
          if not notNullRes.ok:
            return err[seq[string]](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
          let checkRes = enforceChecks(table, entry[2])
          if not checkRes.ok:
            return err[seq[string]](checkRes.err.code, checkRes.err.message, checkRes.err.context)
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
        let triggerRes = executeAfterTriggers(db, bound.updateTable, TriggerEventUpdateMask, int64(updates.len))
        if not triggerRes.ok:
          return err[seq[string]](triggerRes.err.code, triggerRes.err.message, triggerRes.err.context)
    of skDelete:
      if db.catalog.hasViewName(bound.deleteTable):
        let countRes = countViewRows(db, bound.deleteTable, bound.deleteWhere, params)
        if not countRes.ok:
          return err[seq[string]](countRes.err.code, countRes.err.message, countRes.err.context)
        let insteadRes = executeInsteadTriggers(db, bound.deleteTable, TriggerEventDeleteMask, countRes.value)
        if not insteadRes.ok:
          return err[seq[string]](insteadRes.err.code, insteadRes.err.message, insteadRes.err.context)
      else:
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
        let triggerRes = executeAfterTriggers(db, bound.deleteTable, TriggerEventDeleteMask, int64(deletions.len))
        if not triggerRes.ok:
          return err[seq[string]](triggerRes.err.code, triggerRes.err.message, triggerRes.err.context)
    of skExplain:
      if bound.explainInner.kind != skSelect:
        return err[seq[string]](ERR_SQL, "EXPLAIN currently supports SELECT only")
      var p = if i < cachedPlans.len: cachedPlans[i] else: nil
      if p == nil:
        let planRes = plan(db.catalog, bound.explainInner)
        if not planRes.ok:
          return err[seq[string]](planRes.err.code, planRes.err.message, planRes.err.context)
        p = planRes.value
      if bound.explainAnalyze:
        # Execute the query and measure actual metrics
        let t0 = getMonoTime()
        let rowsRes = execPlan(db.pager, db.catalog, p, params)
        let t1 = getMonoTime()
        if not rowsRes.ok:
          return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
        let metrics = PlanMetrics(
          actualRows: rowsRes.value.len,
          actualTimeMs: (t1 - t0).inNanoseconds.float64 / 1_000_000.0
        )
        let lines = explainAnalyzePlanLines(db.catalog, p, metrics)
        for line in lines:
          output.add(line)
      else:
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

proc isPkColumnExpr(expr: Expr, pkName: string, tableName: string): bool =
  expr != nil and expr.kind == ekColumn and expr.name == pkName and (expr.table.len == 0 or expr.table == tableName)

proc tryFastPkUpdate(db: Db, bound: Statement, params: seq[Value]): Result[Option[int64]] =
  if bound.updateWhere == nil:
    return ok(none(int64))
  let tableRes = db.catalog.getTable(bound.updateTable)
  if not tableRes.ok:
    return err[Option[int64]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  var pkName = ""
  for col in table.columns:
    if col.primaryKey and col.kind == ctInt64:
      pkName = col.name
      break
  if pkName.len == 0:
    return ok(none(int64))
  let whereExpr = bound.updateWhere
  if whereExpr.kind != ekBinary or whereExpr.op != "=":
    return ok(none(int64))
  var valueExpr: Expr = nil
  if isPkColumnExpr(whereExpr.left, pkName, bound.updateTable):
    valueExpr = whereExpr.right
  elif isPkColumnExpr(whereExpr.right, pkName, bound.updateTable):
    valueExpr = whereExpr.left
  else:
    return ok(none(int64))
  var rowidVal: uint64 = 0
  case valueExpr.kind
  of ekLiteral:
    if valueExpr.value.kind != svInt:
      return ok(none(int64))
    rowidVal = cast[uint64](valueExpr.value.intVal)
  of ekParam:
    if valueExpr.index <= 0 or valueExpr.index > params.len:
      return err[Option[int64]](ERR_SQL, "Missing parameter", $valueExpr.index)
    let paramVal = params[valueExpr.index - 1]
    if paramVal.kind != vkInt64:
      return ok(none(int64))
    rowidVal = cast[uint64](paramVal.int64Val)
  else:
    return ok(none(int64))
  let storedRes = readRowAt(db.pager, table, rowidVal)
  if not storedRes.ok:
    return ok(some(0'i64))
  var cols: seq[string] = @[]
  for col in table.columns:
    cols.add(bound.updateTable & "." & col.name)
  let row = Row(rowid: storedRes.value.rowid, columns: cols, values: storedRes.value.values)
  var newValues = row.values
  for colName, expr in bound.assignments:
    var idx = -1
    for i, col in table.columns:
      if col.name == colName:
        idx = i
        break
    if idx >= 0:
      let evalRes = evalExpr(row, expr, params)
      if not evalRes.ok:
        return err[Option[int64]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
      let typeRes = typeCheckValue(table.columns[idx], evalRes.value)
      if not typeRes.ok:
        return err[Option[int64]](typeRes.err.code, typeRes.err.message, colName)
      newValues[idx] = typeRes.value
  let notNullRes = enforceNotNull(table, newValues)
  if not notNullRes.ok:
    return err[Option[int64]](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
  let checkRes = enforceChecks(table, newValues)
  if not checkRes.ok:
    return err[Option[int64]](checkRes.err.code, checkRes.err.message, checkRes.err.context)
  let uniqueRes = enforceUnique(db.catalog, db.pager, table, newValues, row.rowid)
  if not uniqueRes.ok:
    return err[Option[int64]](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
  let fkRes = enforceForeignKeys(db.catalog, db.pager, table, newValues)
  if not fkRes.ok:
    return err[Option[int64]](fkRes.err.code, fkRes.err.message, fkRes.err.context)
  let restrictRes = enforceRestrictOnParent(db.catalog, db.pager, table, row.values, newValues)
  if not restrictRes.ok:
    return err[Option[int64]](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)
  let upRes = updateRow(db.pager, db.catalog, bound.updateTable, row.rowid, newValues)
  if not upRes.ok:
    return err[Option[int64]](upRes.err.code, upRes.err.message, upRes.err.context)
  ok(some(1'i64))

proc execPreparedNonSelect*(db: Db, bound: Statement, params: seq[Value], plan: Plan = nil, updateProfile: UpdateWriteProfile = UpdateWriteProfile()): Result[int64] =
  ## Execute a single already-bound non-SELECT statement and return rows affected.
  ## Intended for the native C ABI / Go driver.
  if not db.isOpen:
    return err[int64](ERR_INTERNAL, "Database not open")
  if bound.kind == skSelect:
    return err[int64](ERR_INTERNAL, "execPreparedNonSelect called with SELECT")

  let isWrite = bound.kind in {skCreateTable, skDropTable, skAlterTable, skCreateIndex, skDropIndex, skCreateTrigger, skDropTrigger, skCreateView, skDropView, skAlterView, skInsert, skUpdate, skDelete}
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
      let spec = typeRes.value
      columns.add(Column(
        name: col.name,
        kind: spec.kind,
        notNull: col.notNull,
        unique: col.unique,
        primaryKey: col.primaryKey,
        refTable: col.refTable,
        refColumn: col.refColumn,
        refOnDelete: col.refOnDelete,
        refOnUpdate: col.refOnUpdate,
        decPrecision: spec.decPrecision,
        decScale: spec.decScale
      ))
    var checks: seq[catalog.CheckConstraint] = @[]
    for checkDef in bound.createChecks:
      checks.add(catalog.CheckConstraint(name: checkDef.name, exprSql: exprToCanonicalSql(checkDef.expr)))
    let meta = TableMeta(name: bound.createTableName, rootPage: rootRes.value, nextRowId: 1, columns: columns, checks: checks)
    let saveRes = db.catalog.saveTable(db.pager, meta)
    if not saveRes.ok:
      return err[int64](saveRes.err.code, saveRes.err.message, saveRes.err.context)
    var pkCols: seq[string] = @[]
    for col in columns:
      if col.primaryKey:
        pkCols.add(col.name)
    if pkCols.len > 1:
      let idxName = "pk_" & meta.name & "_" & pkCols.join("_") & "_idx"
      let idxRootRes = initTableRoot(db.pager)
      if not idxRootRes.ok:
        return err[int64](idxRootRes.err.code, idxRootRes.err.message, idxRootRes.err.context)
      let buildRes = buildIndexForColumns(db.pager, db.catalog, meta.name, pkCols, idxRootRes.value)
      if not buildRes.ok:
        return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
      let idxMeta = IndexMeta(name: idxName, table: meta.name, columns: pkCols, rootPage: buildRes.value, kind: catalog.ikBtree, unique: true)
      let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
      if not idxSaveRes.ok:
        return err[int64](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
    for col in columns:
      if col.primaryKey or col.unique:
        if col.primaryKey and pkCols.len > 1:
          continue
        if col.primaryKey and col.kind == ctInt64:
          continue
        let idxName = if col.primaryKey: "pk_" & meta.name & "_" & col.name & "_idx" else: "uniq_" & meta.name & "_" & col.name & "_idx"
        if isNone(db.catalog.getIndexByName(idxName)):
          let idxRootRes = initTableRoot(db.pager)
          if not idxRootRes.ok:
            return err[int64](idxRootRes.err.code, idxRootRes.err.message, idxRootRes.err.context)
          let buildRes = buildIndexForColumn(db.pager, db.catalog, meta.name, col.name, idxRootRes.value)
          if not buildRes.ok:
            return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
          let idxMeta = IndexMeta(name: idxName, table: meta.name, columns: @[col.name], rootPage: buildRes.value, kind: catalog.ikBtree, unique: true)
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
          let idxMeta = IndexMeta(name: idxName, table: meta.name, columns: @[col.name], rootPage: buildRes.value, kind: catalog.ikBtree, unique: false)
          let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
          if not idxSaveRes.ok:
            return err[int64](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skDropTable:
    if bound.dropTableIfExists and bound.dropTableName notin db.catalog.tables:
      discard  # IF EXISTS: silently skip
    else:
      let dependentViews = db.catalog.listDependentViews(bound.dropTableName)
      if dependentViews.len > 0:
        return err[int64](ERR_SQL, "Cannot drop table with dependent views", bound.dropTableName)
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
    let predicateSql = if bound.indexPredicate != nil: exprToCanonicalSql(bound.indexPredicate) else: ""
    if bound.unique:
      let tableRes = db.catalog.getTable(bound.indexTableName)
      if not tableRes.ok:
        return err[int64](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let table = tableRes.value
      var colIndices: seq[int] = @[]
      for colName in bound.columnNames:
        var found = false
        for i, col in table.columns:
          if col.name == colName:
            colIndices.add(i)
            found = true
            break
        if not found:
          return err[int64](ERR_SQL, "Column not found", colName)
      let rowsRes = scanTable(db.pager, table)
      if not rowsRes.ok:
        return err[int64](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
      if colIndices.len == 1:
        let colKind = table.columns[colIndices[0]].kind
        var seen: Table[uint64, bool] = initTable[uint64, bool]()
        var seenVals: Table[uint64, seq[string]] = initTable[uint64, seq[string]]()
        for row in rowsRes.value:
          if row.values[colIndices[0]].kind == vkNull:
            continue
          let key = indexKeyFromValue(row.values[colIndices[0]])
          if colKind in {ctText, ctBlob} and row.values[colIndices[0]].kind in {vkText, vkBlob}:
            let vKey = valueBytesKey(row.values[colIndices[0]])
            if not seenVals.hasKey(key):
              seenVals[key] = @[vKey]
            else:
              if vKey in seenVals[key]:
                return err[int64](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnNames[0])
              seenVals[key].add(vKey)
          else:
            if seen.hasKey(key):
              return err[int64](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnNames[0])
            seen[key] = true
      else:
        var seen: Table[uint64, seq[seq[Value]]] = initTable[uint64, seq[seq[Value]]]()
        for row in rowsRes.value:
          var hasNull = false
          var vals: seq[Value] = @[]
          for ci in colIndices:
            if row.values[ci].kind == vkNull:
              hasNull = true
              break
            vals.add(row.values[ci])
          if hasNull:
            continue
          let key = compositeIndexKey(row.values, colIndices)
          if seen.hasKey(key):
            for existing in seen[key]:
              var allMatch = true
              for j in 0..<vals.len:
                if not valuesEqual(existing[j], vals[j]):
                  allMatch = false
                  break
              if allMatch:
                return err[int64](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnNames.join(","))
            seen[key].add(vals)
          else:
            seen[key] = @[vals]
    var finalRoot = indexRootRes.value
    if bound.indexKind == sql.ikTrigram:
      let buildRes = buildTrigramIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnNames[0], indexRootRes.value)
      if not buildRes.ok:
        return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
      finalRoot = buildRes.value
    else:
      if bound.columnNames.len == 1 and bound.columnNames[0].startsWith(IndexExpressionPrefix):
        let buildRes = buildIndexForExpression(db.pager, db.catalog, bound.indexTableName, bound.columnNames[0], indexRootRes.value)
        if not buildRes.ok:
          return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
        finalRoot = buildRes.value
      elif bound.columnNames.len == 1:
        let buildRes = buildIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnNames[0], indexRootRes.value, predicateSql)
        if not buildRes.ok:
          return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
        finalRoot = buildRes.value
      else:
        let buildRes = buildIndexForColumns(db.pager, db.catalog, bound.indexTableName, bound.columnNames, indexRootRes.value, predicateSql)
        if not buildRes.ok:
          return err[int64](buildRes.err.code, buildRes.err.message, buildRes.err.context)
        finalRoot = buildRes.value
    let idxKind = if bound.indexKind == sql.ikTrigram: catalog.ikTrigram else: catalog.ikBtree
    let idxMeta = IndexMeta(
      name: bound.indexName,
      table: bound.indexTableName,
      columns: bound.columnNames,
      rootPage: finalRoot,
      kind: idxKind,
      unique: bound.unique,
      predicateSql: predicateSql
    )
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

  of skCreateTrigger:
    let triggerMeta = TriggerMeta(
      name: bound.triggerName,
      table: bound.triggerTableName,
      eventsMask: bound.triggerEventsMask,
      actionSql: bound.triggerActionSql
    )
    let createRes = db.catalog.createTriggerMeta(triggerMeta)
    if not createRes.ok:
      return err[int64](createRes.err.code, createRes.err.message, createRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skDropTrigger:
    if not db.catalog.hasTrigger(bound.dropTriggerTableName, bound.dropTriggerName):
      if not bound.dropTriggerIfExists:
        return err[int64](ERR_SQL, "Trigger not found", bound.dropTriggerTableName & "." & bound.dropTriggerName)
    else:
      let dropRes = db.catalog.dropTrigger(bound.dropTriggerTableName, bound.dropTriggerName)
      if not dropRes.ok:
        return err[int64](dropRes.err.code, dropRes.err.message, dropRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skCreateView:
    let dependencies = viewDependencies(bound.createViewQuery)
    let viewMeta = ViewMeta(
      name: bound.createViewName,
      sqlText: bound.createViewSqlText,
      columnNames: bound.createViewColumns,
      dependencies: dependencies
    )
    if bound.createViewIfNotExists and db.catalog.hasViewName(bound.createViewName):
      discard
    elif bound.createViewOrReplace and db.catalog.hasViewName(bound.createViewName):
      let saveRes = db.catalog.saveViewMeta(viewMeta)
      if not saveRes.ok:
        return err[int64](saveRes.err.code, saveRes.err.message, saveRes.err.context)
    else:
      let createRes = db.catalog.createViewMeta(viewMeta)
      if not createRes.ok:
        return err[int64](createRes.err.code, createRes.err.message, createRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skDropView:
    if not db.catalog.hasViewName(bound.dropViewName):
      if not bound.dropViewIfExists:
        return err[int64](ERR_SQL, "View not found", bound.dropViewName)
    else:
      let dependentViews = db.catalog.listDependentViews(bound.dropViewName)
      if dependentViews.len > 0:
        return err[int64](ERR_SQL, "Cannot drop view with dependent views", bound.dropViewName)
      let dropRes = db.catalog.dropView(bound.dropViewName)
      if not dropRes.ok:
        return err[int64](dropRes.err.code, dropRes.err.message, dropRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skAlterView:
    let dependentViews = db.catalog.listDependentViews(bound.alterViewName)
    if dependentViews.len > 0:
      return err[int64](ERR_SQL, "Cannot rename view with dependent views", bound.alterViewName)
    if db.catalog.hasTableOrViewName(bound.alterViewNewName):
      return err[int64](ERR_SQL, "Target name already exists", bound.alterViewNewName)
    let renameRes = db.catalog.renameView(bound.alterViewName, bound.alterViewNewName)
    if not renameRes.ok:
      return err[int64](renameRes.err.code, renameRes.err.message, renameRes.err.context)
    let bumpRes = schemaBump(db)
    if not bumpRes.ok:
      return err[int64](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    affected = 0

  of skInsert:
    if bound.insertReturning.len > 0:
      return err[int64](ERR_SQL, "INSERT RETURNING is not supported by non-select execution API")
    if db.catalog.hasViewName(bound.insertTable):
      affected = if bound.insertValues.len > 0: int64(1 + bound.insertValueRows.len) else: 0
      let insteadRes = executeInsteadTriggers(db, bound.insertTable, TriggerEventInsertMask, affected)
      if not insteadRes.ok:
        return err[int64](insteadRes.err.code, insteadRes.err.message, insteadRes.err.context)
    else:
      let multiRes = execAllInsertRows(db, bound, params, wantRows = false)
      if not multiRes.ok:
        return err[int64](multiRes.err.code, multiRes.err.message, multiRes.err.context)
      affected = multiRes.value.totalAffected
      let triggerRes = executeAfterTriggers(db, bound.insertTable, TriggerEventInsertMask, affected)
      if not triggerRes.ok:
        return err[int64](triggerRes.err.code, triggerRes.err.message, triggerRes.err.context)

  of skUpdate:
    if db.catalog.hasViewName(bound.updateTable):
      let countRes = countViewRows(db, bound.updateTable, bound.updateWhere, params)
      if not countRes.ok:
        return err[int64](countRes.err.code, countRes.err.message, countRes.err.context)
      affected = countRes.value
      let insteadRes = executeInsteadTriggers(db, bound.updateTable, TriggerEventUpdateMask, affected)
      if not insteadRes.ok:
        return err[int64](insteadRes.err.code, insteadRes.err.message, insteadRes.err.context)
    else:
      let fastRes = tryFastPkUpdate(db, bound, params)
      if not fastRes.ok:
        return err[int64](fastRes.err.code, fastRes.err.message, fastRes.err.context)
      if fastRes.value.isSome:
        affected = fastRes.value.get
      else:
        let tableRes = db.catalog.getTable(bound.updateTable)
        if not tableRes.ok:
          return err[int64](tableRes.err.code, tableRes.err.message, tableRes.err.context)
        let table = tableRes.value
        var updates: seq[(uint64, seq[Value], seq[Value])] = @[]
        var cols: seq[string] = @[]
        for col in table.columns:
          cols.add(bound.updateTable & "." & col.name)

        var assignmentIndices: seq[(int, string, Expr)] = @[]
        for colName, expr in bound.assignments:
          var idx = -1
          for i, col in table.columns:
            if col.name == colName:
              idx = i
              break
          if idx >= 0:
            assignmentIndices.add((idx, colName, expr))

        var rows: seq[Row] = @[]
        if plan != nil:
          let rowsRes = execPlan(db.pager, db.catalog, plan, params)
          if not rowsRes.ok:
            return err[int64](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
          rows = rowsRes.value
        else:
          let rowidsRes = findMatchingRowidsPrepared(db, bound.updateTable, bound.updateWhere, params)
          if not rowidsRes.ok:
            return err[int64](rowidsRes.err.code, rowidsRes.err.message, rowidsRes.err.context)
          for rowid in rowidsRes.value:
            let storedRes = readRowAt(db.pager, table, rowid)
            if not storedRes.ok: continue
            rows.add(Row(rowid: storedRes.value.rowid, columns: cols, values: storedRes.value.values))

        for row in rows:
          var newValues = row.values
          for assignment in assignmentIndices:
            let (idx, colName, expr) = assignment
            let evalRes = evalExpr(row, expr, params)
            if not evalRes.ok:
              return err[int64](evalRes.err.code, evalRes.err.message, evalRes.err.context)
            let typeRes = typeCheckValue(table.columns[idx], evalRes.value)
            if not typeRes.ok:
              return err[int64](typeRes.err.code, typeRes.err.message, colName)
            newValues[idx] = typeRes.value
          updates.add((row.rowid, row.values, newValues))

        if not updateProfile.isSimpleUpdate:
          for entry in updates:
            let notNullRes = enforceNotNull(table, entry[2])
            if not notNullRes.ok:
              return err[int64](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
            let checkRes = enforceChecks(table, entry[2])
            if not checkRes.ok:
              return err[int64](checkRes.err.code, checkRes.err.message, checkRes.err.context)
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
      let triggerRes = executeAfterTriggers(db, bound.updateTable, TriggerEventUpdateMask, affected)
      if not triggerRes.ok:
        return err[int64](triggerRes.err.code, triggerRes.err.message, triggerRes.err.context)

  of skDelete:
    if db.catalog.hasViewName(bound.deleteTable):
      let countRes = countViewRows(db, bound.deleteTable, bound.deleteWhere, params)
      if not countRes.ok:
        return err[int64](countRes.err.code, countRes.err.message, countRes.err.context)
      affected = countRes.value
      let insteadRes = executeInsteadTriggers(db, bound.deleteTable, TriggerEventDeleteMask, affected)
      if not insteadRes.ok:
        return err[int64](insteadRes.err.code, insteadRes.err.message, insteadRes.err.context)
    else:
      let tableRes = db.catalog.getTable(bound.deleteTable)
      if not tableRes.ok:
        return err[int64](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let table = tableRes.value
      var deletions: seq[Row] = @[]
      if plan != nil:
        let rowsRes = execPlan(db.pager, db.catalog, plan, params)
        if not rowsRes.ok:
          return err[int64](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
        deletions = rowsRes.value
      else:
        let rowidsRes = findMatchingRowidsPrepared(db, bound.deleteTable, bound.deleteWhere, params)
        if not rowidsRes.ok:
          return err[int64](rowidsRes.err.code, rowidsRes.err.message, rowidsRes.err.context)
        for rowid in rowidsRes.value:
          let storedRes = readRowAt(db.pager, table, rowid)
          if not storedRes.ok: continue
          deletions.add(Row(rowid: storedRes.value.rowid, columns: @[], values: storedRes.value.values))

      for row in deletions:
        let restrictRes = enforceRestrictOnDelete(db.catalog, db.pager, table, row.values)
        if not restrictRes.ok:
          return err[int64](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)
      for row in deletions:
        let delRes = deleteRow(db.pager, db.catalog, bound.deleteTable, row.rowid)
        if not delRes.ok:
          return err[int64](delRes.err.code, delRes.err.message, delRes.err.context)

      affected = int64(deletions.len)
      let triggerRes = executeAfterTriggers(db, bound.deleteTable, TriggerEventDeleteMask, affected)
      if not triggerRes.ok:
        return err[int64](triggerRes.err.code, triggerRes.err.message, triggerRes.err.context)

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

proc execSqlRows*(db: Db, sqlText: string, params: seq[Value]): Result[seq[Row]] =
  ## Execute a single SELECT statement and return raw Rows.
  ## This avoids the overhead of converting all values to strings.
  if not db.isOpen:
    return err[seq[Row]](ERR_INTERNAL, "Database not open")
  
  const SqlCacheMaxEntries = 128

  proc touchSqlCache(key: string) =
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
      when defined(decentdbDebugLogging):
        echo "DEBUG Parse Error: ", parseRes.err.message
      return err[seq[Row]](parseRes.err.code, parseRes.err.message, parseRes.err.context)
    if parseRes.value.statements.len != 1:
      return err[seq[Row]](ERR_SQL, "execSqlRows expects a single SELECT statement")
    let stmt = parseRes.value.statements[0]
    let bindRes = bindStatement(db.catalog, stmt)
    if not bindRes.ok:
      when defined(decentdbDebugLogging):
        echo "DEBUG Bind Error: ", bindRes.err.message
      return err[seq[Row]](bindRes.err.code, bindRes.err.message, bindRes.err.context)
    boundStatements = @[bindRes.value]
    if boundStatements[0].kind != skSelect:
      return err[seq[Row]](ERR_SQL, "execSqlRows expects a SELECT statement")
    let planRes = plan(db.catalog, boundStatements[0])
    if not planRes.ok:
      when defined(decentdbDebugLogging):
        echo "DEBUG Plan Error: ", planRes.err.message
      return err[seq[Row]](planRes.err.code, planRes.err.message, planRes.err.context)
    cachedPlans = @[planRes.value]
    rememberSqlCache(sqlText, boundStatements, cachedPlans)

  if boundStatements.len != 1 or boundStatements[0].kind != skSelect:
    return err[seq[Row]](ERR_SQL, "execSqlRows expects a single SELECT statement")
  if cachedPlans.len != 1 or cachedPlans[0] == nil:
    return err[seq[Row]](ERR_INTERNAL, "Missing cached plan for SELECT")

  let usePlan = cachedPlans[0]

  if db.wal == nil or db.activeWriter != nil or not db.walOverlayEnabled:
    return execPlan(db.pager, db.catalog, usePlan, params)
  
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
    
  return execPlan(db.pager, db.catalog, usePlan, params)

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
    for values in batchRows.mitems:
      if values.len != table.columns.len:
        return err[Void](ERR_SQL, "Column count mismatch", tableName)
      for i, col in table.columns:
        let typeRes = typeCheckValue(col, values[i])
        if not typeRes.ok:
          return err[Void](typeRes.err.code, typeRes.err.message, col.name)
        values[i] = typeRes.value
      let notNullRes = enforceNotNull(table, values, skipAutoIncrementPk = true)
      if not notNullRes.ok:
        return err[Void](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
      let checkRes = enforceChecks(table, values)
      if not checkRes.ok:
        return err[Void](checkRes.err.code, checkRes.err.message, checkRes.err.context)
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
    unmapWalIfMapped(db.wal)
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
  
  # Install flush handler to allow evicting dirty pages during large transactions.
  # Capture writer (not db) to avoid ref cycles under ARC.
  let writer = db.activeWriter
  db.pager.flushHandler = proc(pageId: PageId, data: string): Result[Void] =
    var payload = newSeq[byte](data.len)
    if data.len > 0:
      copyMem(addr payload[0], unsafeAddr data[0], data.len)
    writer.flushPage(pageId, payload)

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

  # Fast path: single dirty page avoids snapshotDirtyPages scan + allocation
  let dirtyCount = db.pager.txnDirtyCount
  when defined(pager_trace):
    echo "commitTransaction: dirtyCount=", dirtyCount, " lastDirtyId=", db.pager.txnLastDirtyId
  if dirtyCount == 1:
    let dirtyId = db.pager.txnLastDirtyId
    let (pageId, pageData) = snapshotSingleDirtyPage(db.pager, dirtyId)
    let hasDirtyPage = pageData.len > 0
    if hasDirtyPage:
      let writeRes = writePageDirect(db.activeWriter, pageId, pageData)
      if not writeRes.ok:
        discard rollback(db.activeWriter)
        db.activeWriter = nil
        db.pager.flushHandler = nil
        clearCache(db.pager)
        return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)

    let commitRes = commit(db.activeWriter)
    if not commitRes.ok:
      db.activeWriter = nil
      db.pager.flushHandler = nil
      clearCache(db.pager)
      return err[Void](commitRes.err.code, commitRes.err.message, commitRes.err.context)

    if hasDirtyPage:
      markPageCommitted(db.pager, dirtyId, commitRes.value)
  else:
    # Multi-page path: zero-copy scan passes data pointers to WAL writer
    let dirtyRefs = collectDirtyPageRefs(db.pager)
    var pageIds = newSeqOfCap[PageId](dirtyRefs.len)

    for entry in dirtyRefs:
      pageIds.add(entry[0])
      let writeRes = writePageZeroCopy(db.activeWriter, entry[0], entry[1], entry[2])
      if not writeRes.ok:
        discard rollback(db.activeWriter)
        db.activeWriter = nil
        db.pager.flushHandler = nil
        clearCache(db.pager)
        return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)

    let commitRes = commit(db.activeWriter)
    if not commitRes.ok:
      db.activeWriter = nil
      db.pager.flushHandler = nil
      clearCache(db.pager)
      return err[Void](commitRes.err.code, commitRes.err.message, commitRes.err.context)

    if pageIds.len > 0:
      markPagesCommitted(db.pager, pageIds, commitRes.value)

  # MED-003: Check if checkpoint will be triggered and flush trigram deltas first.
  # Use byte threshold as a fast gate; maybeCheckpoint handles time/memory checks
  # with its own lazy evaluation interval.
  if db.wal.checkpointEveryBytes > 0 and db.wal.endOffset >= db.wal.checkpointEveryBytes:
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
