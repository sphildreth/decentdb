import strutils
import tables
import options
import json
import algorithm
import catalog/catalog
import errors
import record/record
import pager/db_header
import pager/pager
import sql/sql
import sql/binder
import planner/planner
import exec/exec
import engine
import wal/wal

type
  DbHandle = ref object
    db: Db
    lastErrorCode: int
    lastErrorMessage: string
    lastErrorMessageC: cstring

  DecentdbValueView = object
    kind: cint
    isNull: cint
    int64Val: int64
    float64Val: float64
    bytes: ptr uint8
    bytesLen: cint
    decimalScale: cint

  StmtHandle = ref object
    db: DbHandle
    sql: string
    statement: Statement
    plan: Plan
    params: seq[Value]
    columnNames: seq[string]
    explainLines: seq[string]
    explainPos: int
    affectedRows: int64
    cursor: RowCursor
    hasRow: bool
    currentValues: seq[Value]
    isDone: bool
    readTxnActive: bool
    readTxn: ReadTxn
    rowView: seq[DecentdbValueView]
    returningRows: seq[seq[Value]]
    returningPos: int

var globalLastErrorCode {.threadvar.}: int
var globalLastErrorMessage {.threadvar.}: string
var globalLastErrorMessageC {.threadvar.}: cstring

# Forward declarations (this module is compiled single-pass).
proc setGlobalError(code: ErrorCode, msg: string)
proc clearGlobalError()

proc setError(h: DbHandle, code: ErrorCode, msg: string)
proc clearError(h: DbHandle)

proc toApiCode(code: ErrorCode): int {.inline.} =
  ## C API error codes reserve 0 for OK; internal ErrorCode starts at 0.
  int(code) + 1

proc allocSharedCString(s: string, outLen: ptr cint): cstring =
  ## Allocate a NUL-terminated string via shared allocator so FFI callers
  ## can free it with `decentdb_free`.
  if outLen != nil:
    outLen[] = cint(s.len)
  let buf = cast[ptr UncheckedArray[char]](allocShared0(s.len + 1))
  if s.len > 0:
    copyMem(addr buf[0], unsafeAddr s[0], s.len)
  buf[s.len] = '\0'
  return cast[cstring](buf)

proc decentdb_free*(p: pointer) {.exportc, cdecl, dynlib.} =
  ## Free memory returned by DecentDB C API functions that allocate.
  if p != nil:
    deallocShared(p)

proc decentdb_list_tables_json*(p: pointer, out_len: ptr cint): cstring {.exportc, cdecl, dynlib.} =
  ## Returns a JSON array of table names, e.g. ["users","items"].
  ## Caller must free returned pointer with `decentdb_free`.
  if p == nil:
    setGlobalError(ERR_INTERNAL, "NULL db handle")
    return nil
  let dbh = cast[DbHandle](p)
  dbh.clearError()

  var names: seq[string] = @[]
  for name in dbh.db.catalog.tables.keys:
    names.add(name)
  names.sort(system.cmp)

  var arr = newJArray()
  for name in names:
    arr.add(%name)
  let payload = $arr
  clearGlobalError()
  return allocSharedCString(payload, out_len)

proc decentdb_get_table_columns_json*(p: pointer, table_utf8: cstring, out_len: ptr cint): cstring {.exportc, cdecl, dynlib.} =
  ## Returns a JSON array of column metadata objects for a given table.
  ## Caller must free returned pointer with `decentdb_free`.
  if p == nil:
    setGlobalError(ERR_INTERNAL, "NULL db handle")
    return nil
  if table_utf8 == nil:
    let dbh = cast[DbHandle](p)
    dbh.setError(ERR_INTERNAL, "NULL table name")
    return nil

  let dbh = cast[DbHandle](p)
  dbh.clearError()
  let tableName = $table_utf8
  if not dbh.db.catalog.tables.hasKey(tableName):
    dbh.setError(ERR_SQL, "Table not found: " & tableName)
    return nil

  let t = dbh.db.catalog.tables[tableName]
  var arr = newJArray()
  for col in t.columns:
    var obj = newJObject()
    obj["name"] = %col.name
    obj["type"] = %columnTypeToText(col.kind)
    obj["not_null"] = %col.notNull
    obj["unique"] = %col.unique
    obj["primary_key"] = %col.primaryKey
    if col.refTable.len > 0:
      obj["ref_table"] = %col.refTable
    if col.refColumn.len > 0:
      obj["ref_column"] = %col.refColumn
    if col.refTable.len > 0 and col.refColumn.len > 0:
      obj["ref_on_delete"] = %(if col.refOnDelete.len > 0: col.refOnDelete else: "NO ACTION")
      obj["ref_on_update"] = %(if col.refOnUpdate.len > 0: col.refOnUpdate else: "NO ACTION")
    arr.add(obj)

  let payload = $arr
  clearGlobalError()
  return allocSharedCString(payload, out_len)

proc decentdb_list_indexes_json*(p: pointer, out_len: ptr cint): cstring {.exportc, cdecl, dynlib.} =
  ## Returns a JSON array of index metadata objects.
  ## Caller must free returned pointer with `decentdb_free`.
  if p == nil:
    setGlobalError(ERR_INTERNAL, "NULL db handle")
    return nil
  let dbh = cast[DbHandle](p)
  dbh.clearError()

  var arr = newJArray()
  for name, idx in dbh.db.catalog.indexes:
    var obj = newJObject()
    obj["name"] = %idx.name
    obj["table"] = %idx.table
    var cols = newJArray()
    for c in idx.columns:
      cols.add(%c)
    obj["columns"] = cols
    obj["unique"] = %idx.unique
    obj["kind"] = %(if idx.kind == ikBtree: "btree" else: "trigram")
    arr.add(obj)

  let payload = $arr
  clearGlobalError()
  return allocSharedCString(payload, out_len)

proc setGlobalError(code: ErrorCode, msg: string) =
  globalLastErrorCode = toApiCode(code)
  globalLastErrorMessage = msg
  if globalLastErrorMessageC != nil:
    deallocShared(globalLastErrorMessageC)
    globalLastErrorMessageC = nil
  globalLastErrorMessageC = allocSharedCString(msg, nil)

proc clearGlobalError() =
  globalLastErrorCode = 0
  globalLastErrorMessage = ""
  if globalLastErrorMessageC != nil:
    deallocShared(globalLastErrorMessageC)
    globalLastErrorMessageC = nil

proc setError(h: DbHandle, code: ErrorCode, msg: string) =
  h.lastErrorCode = toApiCode(code)
  h.lastErrorMessage = msg
  if h.lastErrorMessageC != nil:
    deallocShared(h.lastErrorMessageC)
    h.lastErrorMessageC = nil
  h.lastErrorMessageC = allocSharedCString(msg, nil)
  setGlobalError(code, msg)

proc clearError(h: DbHandle) =
  h.lastErrorCode = 0
  h.lastErrorMessage = ""
  if h.lastErrorMessageC != nil:
    deallocShared(h.lastErrorMessageC)
    h.lastErrorMessageC = nil
  clearGlobalError()

proc parseCachePages(options: string): int =
  ## Parse an URL query string and extract cache size as pages.
  ## Accepts `cache_pages` or `cache_size`.
  result = 1024
  if options.len == 0:
    return

  proc tryParseCachePagesValue(raw: string): Option[int] =
    let s = raw.strip()
    if s.len == 0:
      return none(int)

    let lower = s.toLowerAscii()
    let pageSize = int(DefaultPageSize)

    if lower.endsWith("mb"):
      let numPart = lower[0 ..< lower.len - 2].strip()
      try:
        let mb = parseFloat(numPart)
        if mb <= 0:
          return none(int)
        let bytes = int64(mb * 1024.0 * 1024.0)
        let pages = int((bytes + int64(pageSize) - 1) div int64(pageSize))
        if pages > 0:
          return some(pages)
      except ValueError:
        discard
      return none(int)

    try:
      let v = parseInt(lower)
      if v > 0:
        return some(v)
    except ValueError:
      discard
    return none(int)

  for part in options.split('&'):
    if part.len == 0:
      continue
    let kv = part.split('=', 1)
    if kv.len != 2:
      continue
    let key = kv[0].toLowerAscii()

    if key == "cache_mb":
      # Convenience: cache_mb=<int> means MB of cache using DefaultPageSize.
      let vOpt = tryParseCachePagesValue(kv[1] & "MB")
      if vOpt.isSome:
        result = vOpt.get
      continue

    if key != "cache_pages" and key != "cache_size":
      continue

    let vOpt = tryParseCachePagesValue(kv[1])
    if vOpt.isSome:
      result = vOpt.get

proc decentdb_open*(path: cstring, options: cstring): pointer {.exportc, cdecl, dynlib.} =
  let cachePages = parseCachePages(if options == nil: "" else: $options)
  let res = openDb($path, cachePages = cachePages)

  if not res.ok:
    setGlobalError(res.err.code, res.err.message)
    return nil

  clearGlobalError()
  let handle = DbHandle(db: res.value, lastErrorCode: 0, lastErrorMessage: "", lastErrorMessageC: nil)
  GC_ref(handle)
  return cast[pointer](handle)

proc decentdb_close*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil: return 0
  let handle = cast[DbHandle](p)
  handle.clearError()
  if handle.db != nil:
    discard closeDb(handle.db)

  GC_unref(handle)
  return 0

proc decentdb_checkpoint*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil: return -1
  let handle = cast[DbHandle](p)
  handle.clearError()
  let res = checkpointDb(handle.db)
  if not res.ok:
    handle.setError(res.err.code, res.err.message)
    return -1
  return 0

proc decentdb_last_error_code*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil:
    return cint(globalLastErrorCode)
  let handle = cast[DbHandle](p)
  return cint(handle.lastErrorCode)

proc decentdb_last_error_message*(p: pointer): cstring {.exportc, cdecl, dynlib.} =
  if p == nil:
    return globalLastErrorMessageC
  let handle = cast[DbHandle](p)
  return handle.lastErrorMessageC

proc findMaxParam(stmt: Statement): int =
  var maxIdx = 0
  proc walk(e: Expr) =
    if e == nil: return
    case e.kind
    of ekParam: maxIdx = max(maxIdx, e.index)
    of ekBinary: walk(e.left); walk(e.right)
    of ekUnary: walk(e.expr)
    of ekFunc: (for a in e.args: walk(a))
    of ekInList: walk(e.inExpr); (for a in e.inList: walk(a))
    else: discard
  
  case stmt.kind
  of skSelect:
    for query in stmt.cteQueries:
      if query != nil:
        maxIdx = max(maxIdx, findMaxParam(query))
    if stmt.setOpLeft != nil:
      maxIdx = max(maxIdx, findMaxParam(stmt.setOpLeft))
    if stmt.setOpRight != nil:
      maxIdx = max(maxIdx, findMaxParam(stmt.setOpRight))
    for item in stmt.selectItems: walk(item.expr)
    walk(stmt.whereExpr)
    for j in stmt.joins: walk(j.onExpr)
    for g in stmt.groupBy: walk(g)
    walk(stmt.havingExpr)
    for o in stmt.orderBy: walk(o.expr)
    if stmt.limitParam > 0:
      maxIdx = max(maxIdx, stmt.limitParam)
    if stmt.offsetParam > 0:
      maxIdx = max(maxIdx, stmt.offsetParam)
  of skExplain:
    return findMaxParam(stmt.explainInner)
  of skInsert:
    for v in stmt.insertValues: walk(v)
    for row in stmt.insertValueRows:
      for v in row: walk(v)
    for item in stmt.insertReturning:
      if not item.isStar:
        walk(item.expr)
  of skUpdate:
    for _, v in stmt.assignments: walk(v)
    walk(stmt.updateWhere)
  of skDelete:
    walk(stmt.deleteWhere)
  else: discard
  return maxIdx

proc valueTextFromString(s: string): Value =
  var bytes = newSeq[byte](s.len)
  if s.len > 0:
    copyMem(addr bytes[0], unsafeAddr s[0], s.len)
  Value(kind: vkText, bytes: bytes)

proc decentdb_prepare*(p: pointer, sql_text: cstring, out_stmt: ptr pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil or out_stmt == nil: return -1
  let db_handle = cast[DbHandle](p)
  db_handle.clearError()


  let parseRes = parseSql($sql_text)
  if not parseRes.ok:
    db_handle.setError(parseRes.err.code, parseRes.err.message)
    return cint(toApiCode(parseRes.err.code))
  
  if parseRes.value.statements.len == 0:
    db_handle.setError(ERR_SQL, "No SQL statement found")
    return cint(toApiCode(ERR_SQL))
  
  let stmt = parseRes.value.statements[0]
  let bindRes = bindStatement(db_handle.db.catalog, stmt)
  if not bindRes.ok:
    db_handle.setError(bindRes.err.code, "Bind failure: " & bindRes.err.message)
    return cint(toApiCode(bindRes.err.code))
  
  let bound = bindRes.value
  var plan: Plan = nil
  var colNames: seq[string] = @[]
  var explainLines: seq[string] = @[]
  var explainPos: int = 0

  if bound.kind == skSelect:
    let planRes = planner.plan(db_handle.db.catalog, bound)
    if not planRes.ok:
      db_handle.setError(planRes.err.code, planRes.err.message)
      return cint(toApiCode(planRes.err.code))
    plan = planRes.value
    
    if bound.selectItems.len == 1 and bound.selectItems[0].isStar:
      let tableRes = db_handle.db.catalog.getTable(bound.fromTable)
      if tableRes.ok:
        for col in tableRes.value.columns:
          colNames.add(col.name)
    else:
      for item in bound.selectItems:
        var name = if item.alias.len > 0: item.alias else: ""
        if name.len == 0 and item.expr != nil and item.expr.kind == ekColumn:
          name = item.expr.name
        if name.len == 0:
          name = "column" & $colNames.len
        colNames.add(name)

  if bound.kind == skExplain:
    if bound.explainInner.kind != skSelect:
      db_handle.setError(ERR_SQL, "EXPLAIN currently supports SELECT only")
      return cint(toApiCode(ERR_SQL))
    colNames = @["query_plan"]
    explainPos = 0

  let maxParam = findMaxParam(bound)
  var params = newSeq[Value](maxParam)
  for i in 0 ..< maxParam: params[i] = Value(kind: vkNull)

  if bound.kind == skExplain:
    # Reuse the canonical EXPLAIN execution path in the engine to produce
    # deterministic plan lines (still does not execute the inner SELECT).
    let explainRes = engine.execSql(db_handle.db, $sql_text, params)
    if not explainRes.ok:
      db_handle.setError(explainRes.err.code, explainRes.err.message)
      return cint(toApiCode(explainRes.err.code))
    explainLines = explainRes.value

  if bound.kind == skInsert and bound.insertReturning.len > 0:
    # Build column names from RETURNING clause
    for item in bound.insertReturning:
      if item.isStar:
        let tableRes = db_handle.db.catalog.getTable(bound.insertTable)
        if tableRes.ok:
          for col in tableRes.value.columns:
            colNames.add(col.name)
      else:
        var name = if item.alias.len > 0: item.alias else: ""
        if name.len == 0 and item.expr != nil and item.expr.kind == ekColumn:
          name = item.expr.name
        if name.len == 0:
          name = "column" & $colNames.len
        colNames.add(name)

  let stmt_handle = StmtHandle(
    db: db_handle,
    sql: $sql_text,
    statement: bound,
    plan: plan,
    params: params,
    columnNames: colNames,
    explainLines: explainLines,
    explainPos: explainPos,
    affectedRows: 0,
    cursor: nil,
    hasRow: false,
    currentValues: @[],
    isDone: false,
    readTxnActive: false,
    rowView: @[],
    returningRows: @[],
    returningPos: 0
  )

  GC_ref(stmt_handle)
  out_stmt[] = cast[pointer](stmt_handle)
  return 0

proc decentdb_finalize*(p: pointer) {.exportc, cdecl, dynlib.} =
  if p == nil: return
  let handle = cast[StmtHandle](p)
  if handle.readTxnActive and handle.db != nil and handle.db.db != nil and handle.db.db.wal != nil:
    endRead(handle.db.db.wal, handle.readTxn)
    handle.readTxnActive = false
  handle.cursor = nil
  GC_unref(handle)

proc decentdb_reset*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil: return -1
  let h = cast[StmtHandle](p)
  if h.readTxnActive and h.db != nil and h.db.db != nil and h.db.db.wal != nil:
    endRead(h.db.db.wal, h.readTxn)
    h.readTxnActive = false
  h.cursor = nil
  h.hasRow = false
  h.currentValues = @[]
  h.affectedRows = 0
  h.isDone = false
  h.explainPos = 0
  h.returningRows = @[]
  h.returningPos = 0
  return 0

proc decentdb_clear_bindings*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil: return -1
  let h = cast[StmtHandle](p)
  for i in 0 ..< h.params.len:
    h.params[i] = Value(kind: vkNull)
  return 0

proc bindIndex0(h: StmtHandle, index1: cint): int =
  let idx = int(index1) - 1
  if index1 <= 0 or idx < 0 or idx >= h.params.len:
    h.db.setError(ERR_SQL, "Bind index out of bounds: " & $index1)
    return -1
  idx

proc decentdb_bind_null*(p: pointer, col: cint): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  let idx = bindIndex0(h, col)
  if idx < 0: return -1
  h.params[idx] = Value(kind: vkNull)
  return 0

proc decentdb_bind_int64*(p: pointer, col: cint, val: int64): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  let idx = bindIndex0(h, col)
  if idx < 0: return -1
  h.params[idx] = Value(kind: vkInt64, int64Val: val)
  return 0

proc decentdb_bind_bool*(p: pointer, col: cint, val: cint): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  let idx = bindIndex0(h, col)
  if idx < 0: return -1
  h.params[idx] = Value(kind: vkBool, boolVal: val != 0)
  return 0

proc decentdb_bind_float64*(p: pointer, col: cint, val: float64): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  let idx = bindIndex0(h, col)
  if idx < 0: return -1
  h.params[idx] = Value(kind: vkFloat64, float64Val: val)
  return 0

proc decentdb_bind_text*(p: pointer, col: cint, utf8: cstring, byte_len: cint): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  let idx = bindIndex0(h, col)
  if idx < 0: return -1
  var bytes = newSeq[byte](byte_len)
  if byte_len > 0: copyMem(addr bytes[0], utf8, byte_len)
  h.params[idx] = Value(kind: vkText, bytes: bytes)
  return 0

proc decentdb_bind_blob*(p: pointer, col: cint, data: ptr uint8, byte_len: cint): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  let idx = bindIndex0(h, col)
  if idx < 0: return -1
  var bytes = newSeq[byte](byte_len)
  if byte_len > 0: copyMem(addr bytes[0], data, byte_len)
  h.params[idx] = Value(kind: vkBlob, bytes: bytes)
  return 0

proc decentdb_bind_decimal*(p: pointer, col: cint, int_val: int64, scale: cint): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  let idx = bindIndex0(h, col)
  if idx < 0: return -1
  if scale < 0 or scale > 18:
    h.db.setError(ERR_SQL, "Invalid decimal scale")
    return -1
  h.params[idx] = Value(kind: vkDecimal, int64Val: int_val, decimalScale: uint8(scale))
  return 0

proc decentdb_step*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil: return -1
  let h = cast[StmtHandle](p)
  
  if h.isDone:
    return 0

  if h.statement.kind == skExplain:
    if h.explainLines.len == 0:
      h.db.setError(ERR_INTERNAL, "EXPLAIN returned 0 plan lines")
      return -1
    if h.explainPos >= h.explainLines.len:
      h.isDone = true
      h.hasRow = false
      h.currentValues = @[]
      return 0
    h.currentValues = @[valueTextFromString(h.explainLines[h.explainPos])]
    h.explainPos.inc
    h.hasRow = true
    return 1

  if h.statement.kind == skInsert and h.statement.insertReturning.len > 0:
    # INSERT RETURNING: execute via engine.execSql on first step, cache rows
    if h.returningRows.len == 0 and h.returningPos == 0:
      let execRes = engine.execSql(h.db.db, h.sql, h.params)
      if not execRes.ok:
        h.db.setError(execRes.err.code, execRes.err.message)
        return -1
      # Parse pipe-delimited result rows into Value sequences
      for line in execRes.value:
        var rowVals: seq[Value] = @[]
        for part in line.split('|'):
          if part == "NULL":
            rowVals.add(Value(kind: vkNull))
          else:
            try:
              let intVal = parseBiggestInt(part)
              rowVals.add(Value(kind: vkInt64, int64Val: int64(intVal)))
            except ValueError:
              try:
                let floatVal = parseFloat(part)
                rowVals.add(Value(kind: vkFloat64, float64Val: floatVal))
              except ValueError:
                rowVals.add(valueTextFromString(part))
        h.returningRows.add(rowVals)
    if h.returningPos >= h.returningRows.len:
      h.isDone = true
      h.hasRow = false
      h.currentValues = @[]
      return 0
    h.currentValues = h.returningRows[h.returningPos]
    h.returningPos.inc
    h.hasRow = true
    return 1

  if h.statement.kind != skSelect:
    let res = execPreparedNonSelect(h.db.db, h.statement, h.params)
    if not res.ok:
      h.db.setError(res.err.code, res.err.message)
      return -1
    h.affectedRows = res.value
    h.isDone = true
    return 0

  let db = h.db.db
  let useWalOverlay = db.wal != nil and db.activeWriter == nil and db.walOverlayEnabled
  if useWalOverlay and not h.readTxnActive:
    h.readTxn = beginRead(db.wal)
    h.readTxnActive = true

  proc withSnapshot(body: proc(): cint): cint =
    if useWalOverlay and h.readTxnActive:
      db.pager.overlaySnapshot = h.readTxn.snapshot
      db.pager.setReadGuard(proc(): Result[Void] =
        if db.wal.isAborted(h.readTxn):
          return err[Void](ERR_TRANSACTION, "Read transaction aborted")
        okVoid()
      )
      defer:
        db.pager.overlaySnapshot = 0
        db.pager.clearReadGuard()
    body()

  let stepRes = withSnapshot(proc(): cint =
    if h.cursor == nil:
      let curRes = openRowCursor(db.pager, db.catalog, h.plan, h.params)
      if not curRes.ok:
        h.db.setError(curRes.err.code, curRes.err.message)
        return -1
      h.cursor = curRes.value

    let nextRes = rowCursorNext(h.cursor)
    if not nextRes.ok:
      h.db.setError(nextRes.err.code, nextRes.err.message)
      return -1
    if nextRes.value.isNone:
      h.isDone = true
      h.hasRow = false
      h.currentValues = @[]
      if h.readTxnActive and db.wal != nil:
        endRead(db.wal, h.readTxn)
        h.readTxnActive = false
      return 0
    let row = nextRes.value.get
    h.currentValues = row.values
    h.hasRow = true
    return 1
  )
  stepRes

proc decentdb_column_count*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  return cint(h.columnNames.len)

proc decentdb_column_name*(p: pointer, col: cint): cstring {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if col < 0 or col >= cint(h.columnNames.len): return ""
  return cstring(h.columnNames[col])

proc decentdb_column_type*(p: pointer, col: cint): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len):
    return 0
  return cint(h.currentValues[col].kind)

proc decentdb_column_is_null*(p: pointer, col: cint): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len):
    return 1
  return if h.currentValues[col].kind == vkNull: 1 else: 0

proc decentdb_column_int64*(p: pointer, col: cint): int64 {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len): return 0
  let val = h.currentValues[col]
  if val.kind == vkInt64: return val.int64Val
  if val.kind == vkBool: return if val.boolVal: 1 else: 0
  if val.kind == vkFloat64: return int64(val.float64Val)
  if val.kind == vkDecimal:
    # Truncate
    var v = val.int64Val
    for _ in 1 .. int(val.decimalScale): v = v div 10
    return v
  return 0

proc decentdb_column_float64*(p: pointer, col: cint): float64 {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len): return 0
  let val = h.currentValues[col]
  if val.kind == vkFloat64: return val.float64Val
  if val.kind == vkInt64: return float64(val.int64Val)
  if val.kind == vkBool: return if val.boolVal: 1.0 else: 0.0
  if val.kind == vkDecimal:
    # Best effort conversion to float
    var f = float64(val.int64Val)
    var divS = 1.0
    for _ in 1 .. int(val.decimalScale): divS *= 10.0
    return f / divS
  return 0

proc decentdb_column_decimal_scale*(p: pointer, col: cint): cint {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len): return 0
  let val = h.currentValues[col]
  if val.kind == vkDecimal: return cint(val.decimalScale)
  return 0

proc decentdb_column_decimal_unscaled*(p: pointer, col: cint): int64 {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len): return 0
  let val = h.currentValues[col]
  if val.kind == vkDecimal: return val.int64Val
  if val.kind == vkInt64: return val.int64Val
  return 0

proc decentdb_column_text*(p: pointer, col: cint, out_len: ptr cint): cstring {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len): return nil
  let v = h.currentValues[col]
  if v.kind in {vkText, vkBlob}:
    if out_len != nil: out_len[] = cint(v.bytes.len)
    if v.bytes.len == 0: return ""
    # IMPORTANT: return a pointer into statement-owned storage.
    return cast[cstring](unsafeAddr h.currentValues[col].bytes[0])
  return nil

proc decentdb_column_blob*(p: pointer, col: cint, out_len: ptr cint): ptr uint8 {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len): return nil
  let v = h.currentValues[col]
  if v.kind in {vkText, vkBlob}:
    if out_len != nil: out_len[] = cint(v.bytes.len)
    if v.bytes.len == 0: return nil
    return cast[ptr uint8](unsafeAddr h.currentValues[col].bytes[0])
  return nil

proc decentdb_row_view*(p: pointer, out_values: ptr ptr DecentdbValueView, out_count: ptr cint): cint {.exportc, cdecl, dynlib.} =
  if p == nil: return -1
  let h = cast[StmtHandle](p)
  if not h.hasRow:
    if out_values != nil: out_values[] = nil
    if out_count != nil: out_count[] = 0
    return 0
  let n = h.currentValues.len
  if h.rowView.len != n:
    h.rowView = newSeq[DecentdbValueView](n)
  for i in 0 ..< n:
    let v = h.currentValues[i]
    var view = DecentdbValueView(kind: cint(v.kind), isNull: 0, int64Val: 0, float64Val: 0, bytes: nil, bytesLen: 0, decimalScale: 0)
    if v.kind == vkNull:
      view.isNull = 1
    elif v.kind == vkInt64:
      view.int64Val = v.int64Val
    elif v.kind == vkBool:
      view.int64Val = if v.boolVal: 1 else: 0
    elif v.kind == vkFloat64:
      view.float64Val = v.float64Val
    elif v.kind == vkDecimal:
      view.int64Val = v.int64Val
      view.decimalScale = cint(v.decimalScale)
    elif v.kind in {vkText, vkBlob}:
      view.bytesLen = cint(v.bytes.len)
      if v.bytes.len > 0:
        view.bytes = cast[ptr uint8](unsafeAddr h.currentValues[i].bytes[0])
    h.rowView[i] = view
  if out_values != nil:
    if n > 0:
      out_values[] = addr h.rowView[0]
    else:
      out_values[] = nil
  if out_count != nil:
    out_count[] = cint(n)
  return 0

proc decentdb_step_with_params_row_view*(
  p: pointer,
  in_params: ptr DecentdbValueView,
  in_count: cint,
  out_values: ptr ptr DecentdbValueView,
  out_count: ptr cint,
  out_has_row: ptr cint
): cint {.exportc, cdecl, dynlib.} =
  ## Convenience API for FFI consumers (notably Python/ctypes):
  ## - reset statement
  ## - clear bindings
  ## - bind all params from a `decentdb_value_view[]`
  ## - step once
  ## - if a row is available, populate row_view and return it
  ##
  ## Returns 0 on success, -1 on error.
  if p == nil: return -1
  let h = cast[StmtHandle](p)

  discard decentdb_reset(p)
  discard decentdb_clear_bindings(p)

  let n = int(in_count)
  if n != h.params.len:
    h.db.setError(ERR_SQL, "Incorrect parameter count: expected " & $h.params.len & " got " & $n)
    return -1

  if n > 0 and in_params == nil:
    h.db.setError(ERR_INTERNAL, "NULL in_params with non-zero in_count")
    return -1

  if n > 0:
    let arr = cast[ptr UncheckedArray[DecentdbValueView]](in_params)
    for i in 0 ..< n:
      let v = arr[i]
      let kindInt = int(v.kind)
      if v.isNull != 0 or kindInt == int(vkNull):
        h.params[i] = Value(kind: vkNull)
      elif kindInt == int(vkInt64):
        h.params[i] = Value(kind: vkInt64, int64Val: v.int64Val)
      elif kindInt == int(vkBool):
        h.params[i] = Value(kind: vkBool, boolVal: v.int64Val != 0)
      elif kindInt == int(vkFloat64):
        h.params[i] = Value(kind: vkFloat64, float64Val: v.float64Val)
      elif kindInt == int(vkDecimal):
        h.params[i] = Value(kind: vkDecimal, int64Val: v.int64Val, decimalScale: uint8(v.decimalScale))
      elif kindInt == int(vkText) or kindInt == int(vkBlob):
        let byteLen = int(v.bytesLen)
        if byteLen < 0:
          h.db.setError(ERR_SQL, "Negative bytesLen")
          return -1
        if byteLen > 0 and v.bytes == nil:
          h.db.setError(ERR_SQL, "NULL bytes with non-zero bytesLen")
          return -1
        var bytes = newSeq[byte](byteLen)
        if byteLen > 0:
          copyMem(addr bytes[0], v.bytes, byteLen)
        if kindInt == int(vkText):
          h.params[i] = Value(kind: vkText, bytes: bytes)
        else:
          h.params[i] = Value(kind: vkBlob, bytes: bytes)
      else:
        h.db.setError(ERR_SQL, "Unsupported parameter kind: " & $kindInt)
        return -1

  let stepRes = decentdb_step(p)
  if out_has_row != nil:
    out_has_row[] = if stepRes == 1: 1 else: 0
  if stepRes == -1:
    return -1
  if stepRes == 0:
    if out_values != nil: out_values[] = nil
    if out_count != nil: out_count[] = 0
    return 0

  # Row available: populate row view (borrowed until next step/reset/finalize).
  discard decentdb_row_view(p, out_values, out_count)
  return 0

proc decentdb_rows_affected*(p: pointer): int64 {.exportc, cdecl, dynlib.} =
  if p == nil: return 0
  let h = cast[StmtHandle](p)
  return h.affectedRows
