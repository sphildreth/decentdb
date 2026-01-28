import os
import options
import strutils
import tables
import algorithm
import ../errors
import ../sql/sql
import ../catalog/catalog
import ../record/record
import ../pager/pager
import ../pager/db_header
import ../storage/storage
import ../planner/planner
import ../search/search

type Row* = object
  rowid*: uint64
  columns*: seq[string]
  values*: seq[Value]

proc valueToString*(value: Value): string =
  case value.kind
  of vkNull: "NULL"
  of vkBool:
    if value.boolVal:
      "true"
    else:
      "false"
  of vkInt64: $value.int64Val
  of vkFloat64: $value.float64Val
  of vkText, vkBlob:
    var s = ""
    for b in value.bytes:
      s.add(char(b))
    s
  else:
    ""

proc likeMatch*(text: string, pattern: string, caseInsensitive: bool): bool =
  var t = text
  var p = pattern
  if caseInsensitive:
    t = t.toUpperAscii()
    p = p.toUpperAscii()
  var i = 0
  var j = 0
  var star = -1
  var match = 0
  while i < t.len:
    if j < p.len and (p[j] == '_' or p[j] == t[i]):
      i.inc
      j.inc
    elif j < p.len and p[j] == '%':
      star = j
      j.inc
      match = i
    elif star != -1:
      j = star + 1
      match.inc
      i = match
    else:
      return false
  while j < p.len and p[j] == '%':
    j.inc
  j == p.len

proc makeRow(columns: seq[string], values: seq[Value], rowid: uint64 = 0): Row =
  Row(rowid: rowid, columns: columns, values: values)

const SortBufferBytes = 16 * 1024 * 1024
const SortMaxRuns = 64

proc varintLen(value: uint64): int =
  var v = value
  result = 1
  while v >= 0x80'u64:
    v = v shr 7
    result.inc

proc estimateRowBytes(row: Row): int =
  result = varintLen(uint64(row.values.len))
  for value in row.values:
    result.inc
    var payloadLen = 0
    case value.kind
    of vkNull:
      payloadLen = 0
    of vkBool:
      payloadLen = 1
    of vkInt64, vkFloat64:
      payloadLen = 8
    of vkText, vkBlob:
      payloadLen = value.bytes.len
    of vkTextOverflow, vkBlobOverflow:
      payloadLen = 8
    result += varintLen(uint64(payloadLen)) + payloadLen

proc columnIndex(row: Row, table: string, name: string): Result[int] =
  if table.len > 0:
    let key = table & "." & name
    for i, col in row.columns:
      if col == key:
        return ok(i)
    return err[int](ERR_SQL, "Unknown column", key)
  var matches: seq[int] = @[]
  for i, col in row.columns:
    if col == name or col.endsWith("." & name):
      matches.add(i)
  if matches.len == 1:
    return ok(matches[0])
  if matches.len == 0:
    return err[int](ERR_SQL, "Unknown column", name)
  err[int](ERR_SQL, "Ambiguous column", name)

proc evalLiteral(value: SqlValue): Value =
  case value.kind
  of svNull: Value(kind: vkNull)
  of svBool: Value(kind: vkBool, boolVal: value.boolVal)
  of svInt: Value(kind: vkInt64, int64Val: value.intVal)
  of svFloat: Value(kind: vkFloat64, float64Val: value.floatVal)
  of svString:
    var bytes: seq[byte] = @[]
    for ch in value.strVal:
      bytes.add(byte(ch))
    Value(kind: vkText, bytes: bytes)
  of svParam: Value(kind: vkNull)

proc valueToBool*(value: Value): bool =
  case value.kind
  of vkBool: value.boolVal
  of vkInt64: value.int64Val != 0
  of vkFloat64: value.float64Val != 0.0
  of vkText, vkBlob: value.bytes.len > 0
  else: false

proc compareValues*(a: Value, b: Value): int =
  if a.kind == vkText or a.kind == vkBlob:
    let aStr = valueToString(a)
    let bStr = valueToString(b)
    return cmp(aStr, bStr)
  if a.kind == vkFloat64 or b.kind == vkFloat64:
    let af = if a.kind == vkFloat64: a.float64Val else: float(a.int64Val)
    let bf = if b.kind == vkFloat64: b.float64Val else: float(b.int64Val)
    return cmp(af, bf)
  if a.kind == vkInt64 and b.kind == vkInt64:
    return cmp(a.int64Val, b.int64Val)
  if a.kind == vkBool and b.kind == vkBool:
    return cmp(a.boolVal, b.boolVal)
  0

proc evalExpr*(row: Row, expr: Expr, params: seq[Value]): Result[Value] =
  if expr == nil:
    return ok(Value(kind: vkNull))
  case expr.kind
  of ekLiteral:
    return ok(evalLiteral(expr.value))
  of ekParam:
    if expr.index <= 0 or expr.index > params.len:
      return err[Value](ERR_SQL, "Missing parameter", $expr.index)
    return ok(params[expr.index - 1])
  of ekColumn:
    let idxRes = columnIndex(row, expr.table, expr.name)
    if not idxRes.ok:
      return err[Value](idxRes.err.code, idxRes.err.message, idxRes.err.context)
    return ok(row.values[idxRes.value])
  of ekUnary:
    let innerRes = evalExpr(row, expr.expr, params)
    if not innerRes.ok:
      return err[Value](innerRes.err.code, innerRes.err.message, innerRes.err.context)
    if expr.unOp == "NOT":
      return ok(Value(kind: vkBool, boolVal: not valueToBool(innerRes.value)))
    return innerRes
  of ekBinary:
    let leftRes = evalExpr(row, expr.left, params)
    if not leftRes.ok:
      return err[Value](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    let rightRes = evalExpr(row, expr.right, params)
    if not rightRes.ok:
      return err[Value](rightRes.err.code, rightRes.err.message, rightRes.err.context)
    case expr.op
    of "AND":
      return ok(Value(kind: vkBool, boolVal: valueToBool(leftRes.value) and valueToBool(rightRes.value)))
    of "OR":
      return ok(Value(kind: vkBool, boolVal: valueToBool(leftRes.value) or valueToBool(rightRes.value)))
    of "=":
      return ok(Value(kind: vkBool, boolVal: compareValues(leftRes.value, rightRes.value) == 0))
    of "!=":
      return ok(Value(kind: vkBool, boolVal: compareValues(leftRes.value, rightRes.value) != 0))
    of "<":
      return ok(Value(kind: vkBool, boolVal: compareValues(leftRes.value, rightRes.value) < 0))
    of "<=":
      return ok(Value(kind: vkBool, boolVal: compareValues(leftRes.value, rightRes.value) <= 0))
    of ">":
      return ok(Value(kind: vkBool, boolVal: compareValues(leftRes.value, rightRes.value) > 0))
    of ">=":
      return ok(Value(kind: vkBool, boolVal: compareValues(leftRes.value, rightRes.value) >= 0))
    of "LIKE", "ILIKE":
      let leftStr = valueToString(leftRes.value)
      let rightStr = valueToString(rightRes.value)
      return ok(Value(kind: vkBool, boolVal: likeMatch(leftStr, rightStr, true)))
    else:
      return err[Value](ERR_SQL, "Unsupported operator", expr.op)
  of ekFunc:
    return err[Value](ERR_SQL, "Aggregate functions evaluated elsewhere")

proc tableScanRows(pager: Pager, catalog: Catalog, tableName: string, alias: string): Result[seq[Row]] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[seq[Row]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  let rowsRes = scanTable(pager, table)
  if not rowsRes.ok:
    return err[seq[Row]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
  var rows: seq[Row] = @[]
  let prefix = if alias.len > 0: alias else: tableName
  var cols: seq[string] = @[]
  for col in table.columns:
    cols.add(prefix & "." & col.name)
  for stored in rowsRes.value:
    rows.add(makeRow(cols, stored.values, stored.rowid))
  ok(rows)

proc indexSeekRows(pager: Pager, catalog: Catalog, tableName: string, alias: string, column: string, value: Value): Result[seq[Row]] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[seq[Row]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  let rowIdsRes = indexSeek(pager, catalog, tableName, column, value)
  if not rowIdsRes.ok:
    return err[seq[Row]](rowIdsRes.err.code, rowIdsRes.err.message, rowIdsRes.err.context)
  var rows: seq[Row] = @[]
  let prefix = if alias.len > 0: alias else: tableName
  var cols: seq[string] = @[]
  for col in table.columns:
    cols.add(prefix & "." & col.name)
  var valueIndex = -1
  for i, col in table.columns:
    if col.name == column:
      valueIndex = i
      break
  for rowid in rowIdsRes.value:
    let readRes = readRowAt(pager, table, rowid)
    if not readRes.ok:
      continue
    if valueIndex >= 0:
      if compareValues(readRes.value.values[valueIndex], value) != 0:
        continue
    rows.add(makeRow(cols, readRes.value.values, rowid))
  ok(rows)

proc trigramSeekRows(pager: Pager, catalog: Catalog, tableName: string, alias: string, column: string, pattern: string, caseInsensitive: bool): Result[seq[Row]] =
  let tableRes = catalog.getTable(tableName)
  if not tableRes.ok:
    return err[seq[Row]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  let indexOpt = catalog.getTrigramIndexForColumn(tableName, column)
  if isNone(indexOpt):
    return err[seq[Row]](ERR_SQL, "Trigram index not found", tableName & "." & column)
  let idx = indexOpt.get
  var columnIndex = -1
  for i, col in table.columns:
    if col.name == column:
      columnIndex = i
      break
  if columnIndex < 0:
    return err[seq[Row]](ERR_SQL, "Column not found", column)
  var stripped = ""
  for ch in pattern:
    if ch != '%' and ch != '_':
      stripped.add(ch)
  let normalized = canonicalize(stripped)
  if normalized.len < 3:
    let rowsRes = tableScanRows(pager, catalog, tableName, alias)
    if not rowsRes.ok:
      return err[seq[Row]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
    var filtered: seq[Row] = @[]
    for row in rowsRes.value:
      let text = valueToString(row.values[columnIndex])
      if likeMatch(text, pattern, caseInsensitive):
        filtered.add(row)
    return ok(filtered)
  let grams = trigrams(normalized)
  if grams.len == 0:
    return ok(newSeq[Row]())
  var postingsLists: seq[seq[uint64]] = @[]
  var rarestCount = -1
  for g in grams:
    let postRes = getTrigramPostings(pager, idx, g)
    if not postRes.ok:
      return err[seq[Row]](postRes.err.code, postRes.err.message, postRes.err.context)
    let list = postRes.value
    if list.len == 0:
      return ok(newSeq[Row]())
    postingsLists.add(list)
    if rarestCount < 0 or list.len < rarestCount:
      rarestCount = list.len
  let threshold = DefaultPostingsThreshold
  if normalized.len <= 5 and rarestCount >= threshold:
    let rowsRes = tableScanRows(pager, catalog, tableName, alias)
    if not rowsRes.ok:
      return err[seq[Row]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
    var filtered: seq[Row] = @[]
    for row in rowsRes.value:
      let text = valueToString(row.values[columnIndex])
      if likeMatch(text, pattern, caseInsensitive):
        filtered.add(row)
    return ok(filtered)
  var candidates = intersectPostings(postingsLists)
  if normalized.len > 5 and rarestCount >= threshold and candidates.len > threshold:
    candidates.setLen(threshold)
  var rows: seq[Row] = @[]
  let prefix = if alias.len > 0: alias else: tableName
  var cols: seq[string] = @[]
  for col in table.columns:
    cols.add(prefix & "." & col.name)
  for rowid in candidates:
    let readRes = readRowAt(pager, table, rowid)
    if not readRes.ok:
      continue
    let text = valueToString(readRes.value.values[columnIndex])
    if not likeMatch(text, pattern, caseInsensitive):
      continue
    rows.add(makeRow(cols, readRes.value.values, rowid))
  ok(rows)

proc applyFilter(rows: seq[Row], expr: Expr, params: seq[Value]): Result[seq[Row]] =
  if expr == nil:
    return ok(rows)
  var resultRows: seq[Row] = @[]
  for row in rows:
    let evalRes = evalExpr(row, expr, params)
    if not evalRes.ok:
      return err[seq[Row]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
    if valueToBool(evalRes.value):
      resultRows.add(row)
  ok(resultRows)

proc projectRows(rows: seq[Row], items: seq[SelectItem], params: seq[Value]): Result[seq[Row]] =
  if items.len == 0:
    return ok(rows)
  if items.len == 1 and items[0].isStar:
    return ok(rows)
  var resultRows: seq[Row] = @[]
  for row in rows:
    var cols: seq[string] = @[]
    var vals: seq[Value] = @[]
    for item in items:
      if item.isStar:
        for i, col in row.columns:
          cols.add(col)
          vals.add(row.values[i])
      else:
        let evalRes = evalExpr(row, item.expr, params)
        if not evalRes.ok:
          return err[seq[Row]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
        let name = if item.alias.len > 0: item.alias else: "expr"
        cols.add(name)
        vals.add(evalRes.value)
    resultRows.add(makeRow(cols, vals))
  ok(resultRows)

type AggState = object
  count: int64
  sum: float64
  min: Value
  max: Value
  initialized: bool

proc aggregateRows(rows: seq[Row], items: seq[SelectItem], groupBy: seq[Expr], having: Expr, params: seq[Value]): Result[seq[Row]] =
  var groups = initTable[string, AggState]()
  var groupRows = initTable[string, Row]()
  for row in rows:
    var keyParts: seq[string] = @[]
    var keyValues: seq[Value] = @[]
    for expr in groupBy:
      let evalRes = evalExpr(row, expr, params)
      if not evalRes.ok:
        return err[seq[Row]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
      keyValues.add(evalRes.value)
      keyParts.add(valueToString(evalRes.value))
    let key = keyParts.join("|")
    if not groups.hasKey(key):
      groups[key] = AggState()
      groupRows[key] = row
    var state = groups[key]
    state.count.inc
    for item in items:
      if item.expr != nil and item.expr.kind == ekFunc:
        let funcName = item.expr.funcName
        if funcName == "COUNT":
          discard
        else:
          let arg = if item.expr.args.len > 0: item.expr.args[0] else: nil
          if arg != nil:
            let evalRes = evalExpr(row, arg, params)
            if not evalRes.ok:
              return err[seq[Row]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
            let val = evalRes.value
            if funcName == "SUM" or funcName == "AVG":
              let addVal = if val.kind == vkFloat64: val.float64Val else: float(val.int64Val)
              state.sum += addVal
            if funcName == "MIN":
              if not state.initialized or compareValues(val, state.min) < 0:
                state.min = val
            if funcName == "MAX":
              if not state.initialized or compareValues(val, state.max) > 0:
                state.max = val
            state.initialized = true
    groups[key] = state
  var resultRows: seq[Row] = @[]
  for key, state in groups:
    var cols: seq[string] = @[]
    var vals: seq[Value] = @[]
    for item in items:
      if item.expr != nil and item.expr.kind == ekFunc:
        let funcName = item.expr.funcName
        if funcName == "COUNT":
          cols.add("count")
          vals.add(Value(kind: vkInt64, int64Val: state.count))
        elif funcName == "SUM":
          cols.add("sum")
          vals.add(Value(kind: vkFloat64, float64Val: state.sum))
        elif funcName == "AVG":
          cols.add("avg")
          let avg = if state.count == 0: 0.0 else: state.sum / float(state.count)
          vals.add(Value(kind: vkFloat64, float64Val: avg))
        elif funcName == "MIN":
          cols.add("min")
          vals.add(state.min)
        elif funcName == "MAX":
          cols.add("max")
          vals.add(state.max)
      else:
        let evalRes = evalExpr(groupRows.getOrDefault(key, Row()), item.expr, params)
        if not evalRes.ok:
          return err[seq[Row]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
        cols.add("expr")
        vals.add(evalRes.value)
    let row = makeRow(cols, vals)
    if having != nil:
      let havingRes = evalExpr(row, having, params)
      if not havingRes.ok:
        return err[seq[Row]](havingRes.err.code, havingRes.err.message, havingRes.err.context)
      if not valueToBool(havingRes.value):
        continue
    resultRows.add(row)
  ok(resultRows)

proc writeRowChunk(path: string, rows: seq[Row]) =
  var f: File
  if not open(f, path, fmWrite):
    return
  var lenBuf = newSeq[byte](4)
  for row in rows:
    let data = encodeRecord(row.values)
    writeU32LE(lenBuf, 0, uint32(data.len))
    discard f.writeBuffer(lenBuf[0].addr, lenBuf.len)
    if data.len > 0:
      discard f.writeBuffer(data[0].addr, data.len)
  close(f)

proc readRowChunk(path: string, columns: seq[string]): seq[Row] =
  var f: File
  if not open(f, path, fmRead):
    return @[]
  var lenBuf = newSeq[byte](4)
  while true:
    let readLen = f.readBuffer(lenBuf[0].addr, 4)
    if readLen < 4:
      break
    let length = int(readU32LE(lenBuf, 0))
    var data = newSeq[byte](length)
    if length > 0:
      let readData = f.readBuffer(data[0].addr, length)
      if readData < length:
        break
    let decoded = decodeRecord(data)
    if decoded.ok:
      result.add(makeRow(columns, decoded.value))
  close(f)

proc sortRows(rows: seq[Row], orderBy: seq[OrderItem], params: seq[Value]): Result[seq[Row]] =
  proc cmpRows(a, b: Row): int =
    for item in orderBy:
      let av = evalExpr(a, item.expr, params)
      let bv = evalExpr(b, item.expr, params)
      if not av.ok or not bv.ok:
        return 0
      let c = compareValues(av.value, bv.value)
      if c != 0:
        return if item.asc: c else: -c
    0
  if rows.len <= 1:
    var sorted = rows
    sorted.sort(proc(x, y: Row): int = cmpRows(x, y))
    return ok(sorted)
  var tempFiles: seq[string] = @[]
  var chunk: seq[Row] = @[]
  var chunkBytes = 0
  for row in rows:
    let rowBytes = estimateRowBytes(row)
    if chunk.len > 0 and chunkBytes + rowBytes > SortBufferBytes:
      chunk.sort(proc(x, y: Row): int = cmpRows(x, y))
      let path = getTempDir() / ("decentdb_sort_" & $tempFiles.len & ".tmp")
      writeRowChunk(path, chunk)
      tempFiles.add(path)
      chunk = @[]
      chunkBytes = 0
      if tempFiles.len > SortMaxRuns:
        return err[seq[Row]](ERR_SQL, "Sort exceeded spill run limit", "max_runs=" & $SortMaxRuns)
    chunk.add(row)
    chunkBytes += rowBytes
  if tempFiles.len == 0:
    var sorted = rows
    sorted.sort(proc(x, y: Row): int = cmpRows(x, y))
    return ok(sorted)
  if chunk.len > 0:
    chunk.sort(proc(x, y: Row): int = cmpRows(x, y))
    let path = getTempDir() / ("decentdb_sort_" & $tempFiles.len & ".tmp")
    writeRowChunk(path, chunk)
    tempFiles.add(path)
  var readers: seq[seq[Row]] = @[]
  var indices: seq[int] = @[]
  let columns = if rows.len > 0: rows[0].columns else: @[]
  for path in tempFiles:
    let chunk = readRowChunk(path, columns)
    readers.add(chunk)
    indices.add(0)
  var resultRows: seq[Row] = @[]
  while true:
    var bestIdx = -1
    var bestRow: Row
    for i, chunk in readers:
      if indices[i] >= chunk.len:
        continue
      let candidate = chunk[indices[i]]
      if bestIdx < 0:
        bestIdx = i
        bestRow = candidate
      else:
        if cmpRows(candidate, bestRow) < 0:
          bestIdx = i
          bestRow = candidate
    if bestIdx < 0:
      break
    resultRows.add(bestRow)
    indices[bestIdx].inc
  for path in tempFiles:
    if fileExists(path):
      removeFile(path)
  ok(resultRows)

proc applyLimit(rows: seq[Row], limit: int, offset: int): seq[Row] =
  var start = if offset >= 0: offset else: 0
  var endIndex = rows.len
  if limit >= 0:
    endIndex = min(start + limit, rows.len)
  if start >= rows.len:
    return @[]
  rows[start ..< endIndex]

proc execPlan*(pager: Pager, catalog: Catalog, plan: Plan, params: seq[Value]): Result[seq[Row]] =
  case plan.kind
  of pkTableScan:
    return tableScanRows(pager, catalog, plan.table, plan.alias)
  of pkIndexSeek:
    let valueRes = evalExpr(Row(), plan.valueExpr, params)
    if not valueRes.ok:
      return err[seq[Row]](valueRes.err.code, valueRes.err.message, valueRes.err.context)
    return indexSeekRows(pager, catalog, plan.table, plan.alias, plan.column, valueRes.value)
  of pkTrigramSeek:
    let patternRes = evalExpr(Row(), plan.likeExpr, params)
    if not patternRes.ok:
      return err[seq[Row]](patternRes.err.code, patternRes.err.message, patternRes.err.context)
    let pattern = valueToString(patternRes.value)
    return trigramSeekRows(pager, catalog, plan.table, plan.alias, plan.column, pattern, plan.likeInsensitive)
  of pkFilter:
    let inputRes = execPlan(pager, catalog, plan.left, params)
    if not inputRes.ok:
      return err[seq[Row]](inputRes.err.code, inputRes.err.message, inputRes.err.context)
    return applyFilter(inputRes.value, plan.predicate, params)
  of pkProject:
    let inputRes = execPlan(pager, catalog, plan.left, params)
    if not inputRes.ok:
      return err[seq[Row]](inputRes.err.code, inputRes.err.message, inputRes.err.context)
    return projectRows(inputRes.value, plan.projections, params)
  of pkAggregate:
    let inputRes = execPlan(pager, catalog, plan.left, params)
    if not inputRes.ok:
      return err[seq[Row]](inputRes.err.code, inputRes.err.message, inputRes.err.context)
    return aggregateRows(inputRes.value, plan.projections, plan.groupBy, plan.having, params)
  of pkJoin:
    let leftRes = execPlan(pager, catalog, plan.left, params)
    if not leftRes.ok:
      return err[seq[Row]](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    var resultRows: seq[Row] = @[]
    var rightColumns: seq[string] = @[]
    if plan.right.table.len > 0:
      let tableRes = catalog.getTable(plan.right.table)
      if tableRes.ok:
        let prefix = if plan.right.alias.len > 0: plan.right.alias else: plan.right.table
        for col in tableRes.value.columns:
          rightColumns.add(prefix & "." & col.name)
    for lrow in leftRes.value:
      var matched = false
      var rightRows: seq[Row] = @[]
      if plan.right.kind == pkIndexSeek:
        let valueRes = evalExpr(lrow, plan.right.valueExpr, params)
        if not valueRes.ok:
          return err[seq[Row]](valueRes.err.code, valueRes.err.message, valueRes.err.context)
        let idxRes = indexSeekRows(pager, catalog, plan.right.table, plan.right.alias, plan.right.column, valueRes.value)
        if not idxRes.ok:
          return err[seq[Row]](idxRes.err.code, idxRes.err.message, idxRes.err.context)
        rightRows = idxRes.value
      else:
        let rightRes = execPlan(pager, catalog, plan.right, params)
        if not rightRes.ok:
          return err[seq[Row]](rightRes.err.code, rightRes.err.message, rightRes.err.context)
        rightRows = rightRes.value
      if rightColumns.len == 0 and rightRows.len > 0:
        rightColumns = rightRows[0].columns
      for rrow in rightRows:
        var merged = Row(columns: lrow.columns & rrow.columns, values: lrow.values & rrow.values)
        let predRes = evalExpr(merged, plan.joinOn, params)
        if not predRes.ok:
          return err[seq[Row]](predRes.err.code, predRes.err.message, predRes.err.context)
        if valueToBool(predRes.value):
          matched = true
          resultRows.add(merged)
      if plan.joinType == jtLeft and not matched:
        var nullVals: seq[Value] = @[]
        for _ in rightColumns:
          nullVals.add(Value(kind: vkNull))
        let merged = Row(columns: lrow.columns & rightColumns, values: lrow.values & nullVals)
        resultRows.add(merged)
    ok(resultRows)
  of pkSort:
    let inputRes = execPlan(pager, catalog, plan.left, params)
    if not inputRes.ok:
      return err[seq[Row]](inputRes.err.code, inputRes.err.message, inputRes.err.context)
    return sortRows(inputRes.value, plan.orderBy, params)
  of pkLimit:
    let inputRes = execPlan(pager, catalog, plan.left, params)
    if not inputRes.ok:
      return err[seq[Row]](inputRes.err.code, inputRes.err.message, inputRes.err.context)
    return ok(applyLimit(inputRes.value, plan.limit, plan.offset))
  of pkStatement:
    return ok(newSeq[Row]())
