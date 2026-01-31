import strutils
import tables
import options
import catalog/catalog
import errors
import record/record
import pager/pager
import vfs/os_vfs
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

  DecentdbValueView = object
    kind: cint
    isNull: cint
    int64Val: int64
    float64Val: float64
    bytes: ptr uint8
    bytesLen: cint

  StmtHandle = ref object
    db: DbHandle
    sql: string
    statement: Statement
    plan: Plan
    params: seq[Value]
    columnNames: seq[string]
    affectedRows: int64
    cursor: RowCursor
    hasRow: bool
    currentValues: seq[Value]
    isDone: bool
    readTxnActive: bool
    readTxn: ReadTxn
    rowView: seq[DecentdbValueView]

proc setError(h: DbHandle, code: ErrorCode, msg: string) =
  h.lastErrorCode = int(code)
  h.lastErrorMessage = msg

proc clearError(h: DbHandle) =
  h.lastErrorCode = 0
  h.lastErrorMessage = ""

proc parseCachePages(options: string): int =
  ## Parse an URL query string and extract cache size as pages.
  ## Accepts `cache_pages` or `cache_size`.
  result = 1024
  if options.len == 0:
    return
  for part in options.split('&'):
    if part.len == 0:
      continue
    let kv = part.split('=', 1)
    if kv.len != 2:
      continue
    let key = kv[0].toLowerAscii()
    if key != "cache_pages" and key != "cache_size":
      continue
    try:
      let v = parseInt(kv[1])
      if v > 0:
        result = v
    except ValueError:
      discard

proc decentdb_open*(path: cstring, options: cstring): pointer {.exportc, cdecl, dynlib.} =
  let cachePages = parseCachePages(if options == nil: "" else: $options)
  let res = openDb($path, cachePages = cachePages)
  let handle = DbHandle()

  if res.ok:
    handle.db = res.value
  else:
    handle.lastErrorCode = int(res.err.code)
    handle.lastErrorMessage = res.err.message
  
  GC_ref(handle)
  return cast[pointer](handle)

proc decentdb_close*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil: return 0
  let handle = cast[DbHandle](p)
  if handle.db != nil:
    discard closeDb(handle.db)

  GC_unref(handle)
  return 0

proc decentdb_last_error_code*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil: return 0
  let handle = cast[DbHandle](p)
  return cint(handle.lastErrorCode)

proc decentdb_last_error_message*(p: pointer): cstring {.exportc, cdecl, dynlib.} =
  if p == nil: return ""
  let handle = cast[DbHandle](p)
  return cstring(handle.lastErrorMessage)

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
    for item in stmt.selectItems: walk(item.expr)
    walk(stmt.whereExpr)
    for j in stmt.joins: walk(j.onExpr)
    for g in stmt.groupBy: walk(g)
    walk(stmt.havingExpr)
    for o in stmt.orderBy: walk(o.expr)
  of skInsert:
    for v in stmt.insertValues: walk(v)
  of skUpdate:
    for _, v in stmt.assignments: walk(v)
    walk(stmt.updateWhere)
  of skDelete:
    walk(stmt.deleteWhere)
  else: discard
  return maxIdx

proc decentdb_prepare*(p: pointer, sql_text: cstring, out_stmt: ptr pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil or out_stmt == nil: return -1
  let db_handle = cast[DbHandle](p)
  db_handle.clearError()


  let parseRes = parseSql($sql_text)
  if not parseRes.ok:
    db_handle.setError(parseRes.err.code, parseRes.err.message)
    return cint(parseRes.err.code)
  
  if parseRes.value.statements.len == 0:
    db_handle.setError(ERR_SQL, "No SQL statement found")
    return cint(ERR_SQL)
  
  let stmt = parseRes.value.statements[0]
  let bindRes = bindStatement(db_handle.db.catalog, stmt)
  if not bindRes.ok:
    db_handle.setError(bindRes.err.code, "Bind failure: " & bindRes.err.message)
    return cint(bindRes.err.code)
  
  let bound = bindRes.value
  var plan: Plan = nil
  var colNames: seq[string] = @[]

  if bound.kind == skSelect:
    let planRes = planner.plan(db_handle.db.catalog, bound)
    if not planRes.ok:
      db_handle.setError(planRes.err.code, planRes.err.message)
      return cint(planRes.err.code)
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

  let maxParam = findMaxParam(bound)
  var params = newSeq[Value](maxParam)
  for i in 0 ..< maxParam: params[i] = Value(kind: vkNull)

  let stmt_handle = StmtHandle(
    db: db_handle,
    sql: $sql_text,
    statement: bound,
    plan: plan,
    params: params,
    columnNames: colNames,
    affectedRows: 0,
    cursor: nil,
    hasRow: false,
    currentValues: @[],
    isDone: false,
    readTxnActive: false,
    rowView: @[]
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

proc decentdb_step*(p: pointer): cint {.exportc, cdecl, dynlib.} =
  if p == nil: return -1
  let h = cast[StmtHandle](p)
  
  if h.isDone:
    return 0

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
  return 0

proc decentdb_column_float64*(p: pointer, col: cint): float64 {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len): return 0
  let val = h.currentValues[col]
  if val.kind == vkFloat64: return val.float64Val
  if val.kind == vkInt64: return float64(val.int64Val)
  if val.kind == vkBool: return if val.boolVal: 1.0 else: 0.0
  return 0

proc decentdb_column_text*(p: pointer, col: cint, out_len: ptr cint): cstring {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len): return nil
  let val = h.currentValues[col]
  if val.kind in {vkText, vkBlob}:
    if out_len != nil: out_len[] = cint(val.bytes.len)
    if val.bytes.len == 0: return ""
    return cast[cstring](unsafeAddr val.bytes[0])
  return nil

proc decentdb_column_blob*(p: pointer, col: cint, out_len: ptr cint): ptr uint8 {.exportc, cdecl, dynlib.} =
  let h = cast[StmtHandle](p)
  if not h.hasRow or col < 0 or col >= cint(h.currentValues.len): return nil
  let val = h.currentValues[col]
  if val.kind in {vkText, vkBlob}:
    if out_len != nil: out_len[] = cint(val.bytes.len)
    if val.bytes.len == 0: return nil
    return cast[ptr uint8](unsafeAddr val.bytes[0])
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
    var view = DecentdbValueView(kind: cint(v.kind), isNull: 0, int64Val: 0, float64Val: 0, bytes: nil, bytesLen: 0)
    if v.kind == vkNull:
      view.isNull = 1
    elif v.kind == vkInt64:
      view.int64Val = v.int64Val
    elif v.kind == vkBool:
      view.int64Val = if v.boolVal: 1 else: 0
    elif v.kind == vkFloat64:
      view.float64Val = v.float64Val
    elif v.kind in {vkText, vkBlob}:
      view.bytesLen = cint(v.bytes.len)
      if v.bytes.len > 0:
        view.bytes = cast[ptr uint8](unsafeAddr v.bytes[0])
    h.rowView[i] = view
  if out_values != nil:
    if n > 0:
      out_values[] = addr h.rowView[0]
    else:
      out_values[] = nil
  if out_count != nil:
    out_count[] = cint(n)
  return 0

proc decentdb_rows_affected*(p: pointer): int64 {.exportc, cdecl, dynlib.} =
  if p == nil: return 0
  let h = cast[StmtHandle](p)
  return h.affectedRows
