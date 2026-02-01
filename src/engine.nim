import os
import strutils
import tables
import options
import algorithm
import atomics
import sets
import locks
import ./errors
import ./vfs/types
import ./vfs/os_vfs
import ./pager/db_header
import ./pager/pager
import ./catalog/catalog
import ./sql/sql
import ./sql/binder
import ./planner/planner
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
  # - Warn/abort long-running readers to prevent indefinite WAL pinning
  setCheckpointConfig(wal,
    everyBytes = 64 * 1024 * 1024,
    everyMs = 0,
    readerWarnMs = 60 * 1000,
    readerTimeoutMs = 300 * 1000,
    forceTruncateOnTimeout = true)
  
  ok(Db(
    path: path,
    vfs: vfs,
    file: file,
    isOpen: true,
    formatVersion: header.formatVersion,
    pageSize: header.pageSize,
    schemaCookie: header.schemaCookie,
    pager: pager,
    catalog: catalogRes.value,
    wal: wal,
    activeWriter: nil,
    walOverlayEnabled: true,
    cachePages: cachePages,
    sqlCache: initTable[string, tuple[schemaCookie: uint32, statements: seq[Statement], plans: seq[Plan]]](),
    sqlCacheOrder: @[]
  ))

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
    let key = indexKeyFromValue(values[i])
    let anyRes = indexHasAnyKey(pager, idxOpt.get, key)
    if not anyRes.ok:
      return err[Void](anyRes.err.code, anyRes.err.message, anyRes.err.context)
    if not anyRes.value:
      return err[Void](ERR_CONSTRAINT, "FOREIGN KEY constraint failed", table.name & "." & col.name)
  okVoid()

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
        var seen = initTable[uint64, bool]()
        for row in rowsRes.value:
          if row.values[colIndex].kind == vkNull:
            continue
          let key = indexKeyFromValue(row.values[colIndex])
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
      var seen = initTable[uint64, bool]()
      for row in rowsRes.value:
        if row.values[colIndex].kind == vkNull:
          continue
        let key = indexKeyFromValue(row.values[colIndex])
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
  db.catalog.clearTrigramDeltas()
  okVoid()

proc commitTransaction*(db: Db): Result[Void] =
  ## Commit the active transaction
  if not db.isOpen:
    return err[Void](ERR_INTERNAL, "Database not open")
  if db.activeWriter == nil:
    return err[Void](ERR_TRANSACTION, "No active transaction")

  let trigramFlushRes = flushTrigramDeltas(db.pager, db.catalog)
  if not trigramFlushRes.ok:
    discard rollback(db.activeWriter)
    db.activeWriter = nil
    clearCache(db.pager)
    return err[Void](trigramFlushRes.err.code, trigramFlushRes.err.message, trigramFlushRes.err.context)

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
      clearCache(db.pager)
      return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    pageIds.add(entry[0])

  let commitRes = commit(db.activeWriter)
  if not commitRes.ok:
    db.activeWriter = nil
    clearCache(db.pager)
    return err[Void](commitRes.err.code, commitRes.err.message, commitRes.err.context)

  if pageIds.len > 0:
    markPagesCommitted(db.pager, pageIds, commitRes.value)

  let chkRes = maybeCheckpoint(db.wal, db.pager)
  if not chkRes.ok:
    db.activeWriter = nil
    return err[Void](chkRes.err.code, chkRes.err.message, chkRes.err.context)

  db.activeWriter = nil
  okVoid()

proc rollbackTransaction*(db: Db): Result[Void] =
  ## Rollback the active transaction
  if not db.isOpen:
    return err[Void](ERR_INTERNAL, "Database not open")
  if db.activeWriter == nil:
    return err[Void](ERR_TRANSACTION, "No active transaction")
  let dirtyPages = snapshotDirtyPages(db.pager)
  let rollbackRes = rollback(db.activeWriter)
  db.activeWriter = nil
  if not rollbackRes.ok:
    return err[Void](rollbackRes.err.code, rollbackRes.err.message, rollbackRes.err.context)
  db.catalog.clearTrigramDeltas()
  if dirtyPages.len > 0:
    rollbackCache(db.pager)
  
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
  ##  Force a checkpoint of the WAL
  if not db.isOpen:
    return err[uint64](ERR_INTERNAL, "Database not open")
  
  checkpoint(db.wal, db.pager)
