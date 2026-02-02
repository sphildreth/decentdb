import os
import options
import strutils
import tables
import algorithm
import atomics
import sets
import ../errors
import ../sql/sql
import ../catalog/catalog
import ../record/record
import ../pager/pager
import ../pager/db_header
import ../storage/storage
import ../planner/planner
import ../search/search
import ../btree/btree

type Row* = object
  rowid*: uint64
  columns*: seq[string]
  values*: seq[Value]

type RowCursor* = ref object
  ## Forward-only row iterator for common SELECT paths.
  ##
  ## - For complex plans (join/sort/aggregate), this may materialize internally.
  columns*: seq[string]
  nextFn: proc(): Result[Option[Row]] {.closure.}

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
    let n = value.bytes.len
    var s = newString(n)
    if n > 0:
      copyMem(addr s[0], unsafeAddr value.bytes[0], n)
    s
  else:
    ""

proc likeMatch*(text: string, pattern: string, caseInsensitive: bool): bool =
  template norm(ch: char): char =
    (if caseInsensitive: toUpperAscii(ch) else: ch)

  # Fast paths for common patterns.
  # These avoid the general wildcard matcher for the important cases:
  # - '%needle%'
  # - 'prefix%'
  # - '%suffix'
  # Only enabled when there are no '_' wildcards.
  if pattern.find('_') < 0:
    if not caseInsensitive:
      # '%needle%'
      if pattern.len >= 2 and pattern[0] == '%' and pattern[^1] == '%':
        let inner = pattern[1 .. ^2]
        if inner.len == 0:
          return true
        if inner.find('%') < 0:
          return text.find(inner) >= 0

      # 'prefix%'
      if pattern.len >= 1 and pattern[^1] == '%' and pattern.find('%') == pattern.len - 1:
        let prefix = pattern[0 .. ^2]
        if prefix.len == 0:
          return true
        return text.startsWith(prefix)

      # '%suffix'
      if pattern.len >= 1 and pattern[0] == '%' and pattern.rfind('%') == 0:
        let suffix = pattern[1 .. ^1]
        if suffix.len == 0:
          return true
        return text.endsWith(suffix)
  var i = 0
  var j = 0
  var star = -1
  var match = 0
  while i < text.len:
    if j < pattern.len and (pattern[j] == '_' or norm(pattern[j]) == norm(text[i])):
      i.inc
      j.inc
    elif j < pattern.len and pattern[j] == '%':
      star = j
      j.inc
      match = i
    elif star != -1:
      j = star + 1
      match.inc
      i = match
    else:
      return false
  while j < pattern.len and pattern[j] == '%':
    j.inc
  j == pattern.len

const MaxLikePatternLen* = 4096
const MaxLikeWildcards* = 128

proc likeMatchChecked*(text: string, pattern: string, caseInsensitive: bool): Result[bool] =
  ## Guardrails to prevent pathological LIKE inputs from monopolizing CPU/memory.
  if pattern.len > MaxLikePatternLen:
    return err[bool](ERR_SQL, "LIKE pattern too long", "len=" & $pattern.len)
  # Count wildcards to prevent excessive backtracking
  var wildcardCount = 0
  for ch in pattern:
    if ch == '%':
      wildcardCount.inc
  if wildcardCount > MaxLikeWildcards:
    return err[bool](ERR_SQL, "LIKE pattern has too many wildcards", "count=" & $wildcardCount)
  ok(likeMatch(text, pattern, caseInsensitive))

proc makeRow*(columns: seq[string], values: seq[Value], rowid: uint64 = 0): Row =
  Row(rowid: rowid, columns: columns, values: values)

proc rowCursorColumns*(cursor: RowCursor): seq[string] =
  if cursor == nil: return @[]
  cursor.columns

proc rowCursorNext*(cursor: RowCursor): Result[Option[Row]] =
  if cursor == nil:
    return ok(none(Row))
  cursor.nextFn()

# Forward declaration (used by openRowCursor materialization fallback)
proc execPlan*(pager: Pager, catalog: Catalog, plan: Plan, params: seq[Value]): Result[seq[Row]]

# Forward declarations used by streaming cursor wrappers
proc evalExpr*(row: Row, expr: Expr, params: seq[Value]): Result[Value]
proc valueToBool*(value: Value): bool

proc openRowCursor*(pager: Pager, catalog: Catalog, plan: Plan, params: seq[Value]): Result[RowCursor] =
  ## Build a forward-only cursor for a query plan.
  ##
  ## Streaming is implemented for these plan shapes:
  ## - Table scan
  ## - Index seek (equality)
  ## - Filter
  ## - Project
  ## - Limit/Offset (without requiring full materialization)
  ##
  ## For other plan kinds, we fall back to materializing via execPlan.
  proc resolveLimitOffset(): Result[(int, int)] =
    var limit = plan.limit
    var offset = plan.offset

    if plan.limitParam > 0:
      let i = plan.limitParam - 1
      if i < 0 or i >= params.len:
        return err[(int, int)](ERR_SQL, "LIMIT parameter index out of bounds")
      let v = params[i]
      if v.kind != vkInt64:
        return err[(int, int)](ERR_SQL, "LIMIT parameter must be INT64")
      if v.int64Val < 0:
        return err[(int, int)](ERR_SQL, "LIMIT parameter must be non-negative")
      if v.int64Val > int64(high(int)):
        return err[(int, int)](ERR_SQL, "LIMIT parameter too large")
      limit = int(v.int64Val)

    if plan.offsetParam > 0:
      let i = plan.offsetParam - 1
      if i < 0 or i >= params.len:
        return err[(int, int)](ERR_SQL, "OFFSET parameter index out of bounds")
      let v = params[i]
      if v.kind != vkInt64:
        return err[(int, int)](ERR_SQL, "OFFSET parameter must be INT64")
      if v.int64Val < 0:
        return err[(int, int)](ERR_SQL, "OFFSET parameter must be non-negative")
      if v.int64Val > int64(high(int)):
        return err[(int, int)](ERR_SQL, "OFFSET parameter too large")
      offset = int(v.int64Val)

    ok((limit, offset))

  proc materialize(): Result[RowCursor] =
    let rowsRes = execPlan(pager, catalog, plan, params)
    if not rowsRes.ok:
      return err[RowCursor](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
    let rows = rowsRes.value
    let cols = if rows.len > 0: rows[0].columns else: @[]
    var i = 0
    let c = RowCursor(
      columns: cols,
      nextFn: proc(): Result[Option[Row]] =
        if i >= rows.len:
          return ok(none(Row))
        let r = rows[i]
        i.inc
        ok(some(r))
    )
    ok(c)

  case plan.kind
  of pkTableScan:
    let tableRes = catalog.getTable(plan.table)
    if not tableRes.ok:
      return err[RowCursor](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value
    let tree = newBTree(pager, table.rootPage)
    let cursorRes = openCursor(tree)
    if not cursorRes.ok:
      return err[RowCursor](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
    let btCursor = cursorRes.value
    let prefix = if plan.alias.len > 0: plan.alias else: plan.table
    var cols: seq[string] = @[]
    for col in table.columns:
      cols.add(prefix & "." & col.name)
    let c = RowCursor(
      columns: cols,
      nextFn: proc(): Result[Option[Row]] =
        while true:
          let nextRes = cursorNext(btCursor)
          if not nextRes.ok:
            return ok(none(Row))
          let (rowid, valueBytes, overflow) = nextRes.value
          if valueBytes.len == 0 and overflow == 0'u32:
            continue
          let decoded = decodeRecordWithOverflow(pager, valueBytes)
          if not decoded.ok:
            return err[Option[Row]](decoded.err.code, decoded.err.message, decoded.err.context)
          return ok(some(makeRow(cols, decoded.value, rowid)))
    )
    ok(c)

  of pkRowidSeek:
    let valueRes = evalExpr(Row(), plan.valueExpr, params)
    if not valueRes.ok:
      return err[RowCursor](valueRes.err.code, valueRes.err.message, valueRes.err.context)
    if valueRes.value.kind != vkInt64:
      return err[RowCursor](ERR_SQL, "Rowid seek expects INT64")
    let tableRes = catalog.getTable(plan.table)
    if not tableRes.ok:
      return err[RowCursor](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value
    let prefix = if plan.alias.len > 0: plan.alias else: plan.table
    var cols: seq[string] = @[]
    for col in table.columns:
      cols.add(prefix & "." & col.name)
    let targetRowId = cast[uint64](valueRes.value.int64Val)
    var done = false
    let c = RowCursor(
      columns: cols,
      nextFn: proc(): Result[Option[Row]] =
        if done:
          return ok(none(Row))
        done = true
        let readRes = readRowAt(pager, table, targetRowId)
        if not readRes.ok:
          # Not found -> empty result set.
          if readRes.err.code == ERR_IO and readRes.err.message == "Key not found":
            return ok(none(Row))
          return err[Option[Row]](readRes.err.code, readRes.err.message, readRes.err.context)
        ok(some(makeRow(cols, readRes.value.values, targetRowId)))
    )
    ok(c)

  of pkIndexSeek:
    let valueRes = evalExpr(Row(), plan.valueExpr, params)
    if not valueRes.ok:
      return err[RowCursor](valueRes.err.code, valueRes.err.message, valueRes.err.context)
    let tableRes = catalog.getTable(plan.table)
    if not tableRes.ok:
      return err[RowCursor](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value
    let indexOpt = catalog.getBtreeIndexForColumn(plan.table, plan.column)
    if isNone(indexOpt):
      return err[RowCursor](ERR_SQL, "Index not found", plan.table & "." & plan.column)
    let idx = indexOpt.get
    let needle = indexKeyFromValue(valueRes.value)
    let idxTree = newBTree(pager, idx.rootPage)
    let idxCursorRes = openCursorAt(idxTree, needle)
    if not idxCursorRes.ok:
      return err[RowCursor](idxCursorRes.err.code, idxCursorRes.err.message, idxCursorRes.err.context)
    let idxCursor = idxCursorRes.value
    let prefix = if plan.alias.len > 0: plan.alias else: plan.table
    var cols: seq[string] = @[]
    for col in table.columns:
      cols.add(prefix & "." & col.name)
    proc decodeRowIdBytes(data: seq[byte]): Result[uint64] =
      if data.len < 8:
        return err[uint64](ERR_CORRUPTION, "Index rowid payload too short")
      ok(readU64LE(data, 0))
    let c = RowCursor(
      columns: cols,
      nextFn: proc(): Result[Option[Row]] =
        while true:
          let nextRes = cursorNext(idxCursor)
          if not nextRes.ok:
            return ok(none(Row))
          if nextRes.value[0] < needle:
            continue
          if nextRes.value[0] > needle:
            return ok(none(Row))
          let rowidRes = decodeRowIdBytes(nextRes.value[1])
          if not rowidRes.ok:
            # Skip corrupt rowid payloads.
            continue
          let readRes = readRowAt(pager, table, rowidRes.value)
          if not readRes.ok:
            continue
          return ok(some(makeRow(cols, readRes.value.values, rowidRes.value)))
    )
    ok(c)

  of pkFilter:
    let childRes = openRowCursor(pager, catalog, plan.left, params)
    if not childRes.ok:
      return err[RowCursor](childRes.err.code, childRes.err.message, childRes.err.context)
    let child = childRes.value
    let c = RowCursor(
      columns: child.columns,
      nextFn: proc(): Result[Option[Row]] =
        while true:
          let rRes = rowCursorNext(child)
          if not rRes.ok:
            return err[Option[Row]](rRes.err.code, rRes.err.message, rRes.err.context)
          if rRes.value.isNone:
            return ok(none(Row))
          let r = rRes.value.get
          if plan.predicate == nil:
            return ok(some(r))
          let evalRes = evalExpr(r, plan.predicate, params)
          if not evalRes.ok:
            return err[Option[Row]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
          if valueToBool(evalRes.value):
            return ok(some(r))
    )
    ok(c)

  of pkProject:
    let childRes = openRowCursor(pager, catalog, plan.left, params)
    if not childRes.ok:
      return err[RowCursor](childRes.err.code, childRes.err.message, childRes.err.context)
    let child = childRes.value
    let items = plan.projections
    if items.len == 0 or (items.len == 1 and items[0].isStar):
      return ok(child)
    var outCols: seq[string] = @[]
    for item in items:
      if item.isStar:
        for c in child.columns:
          outCols.add(c)
      else:
        var name = if item.alias.len > 0: item.alias else: ""
        if name.len == 0 and item.expr != nil and item.expr.kind == ekColumn:
          name = item.expr.name
        if name.len == 0:
          name = "expr"
        outCols.add(name)
    let c = RowCursor(
      columns: outCols,
      nextFn: proc(): Result[Option[Row]] =
        let rRes = rowCursorNext(child)
        if not rRes.ok:
          return err[Option[Row]](rRes.err.code, rRes.err.message, rRes.err.context)
        if rRes.value.isNone:
          return ok(none(Row))
        let r = rRes.value.get
        var vals: seq[Value] = @[]
        for item in items:
          if item.isStar:
            for v in r.values:
              vals.add(v)
          else:
            let evalRes = evalExpr(r, item.expr, params)
            if not evalRes.ok:
              return err[Option[Row]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
            vals.add(evalRes.value)
        ok(some(makeRow(outCols, vals, r.rowid)))
    )
    ok(c)

  of pkLimit:
    let childPlan =
      if plan.left != nil and plan.left.kind == pkSort:
        # Sorting requires materialization; preserve existing optimized path.
        nil
      else:
        plan.left
    if childPlan == nil:
      return materialize()
    let childRes = openRowCursor(pager, catalog, childPlan, params)
    if not childRes.ok:
      return err[RowCursor](childRes.err.code, childRes.err.message, childRes.err.context)
    let child = childRes.value
    let loRes = resolveLimitOffset()
    if not loRes.ok:
      return err[RowCursor](loRes.err.code, loRes.err.message, loRes.err.context)
    let offset = if loRes.value[1] >= 0: loRes.value[1] else: 0
    let limit = loRes.value[0]
    var skipped = 0
    var produced = 0
    let c = RowCursor(
      columns: child.columns,
      nextFn: proc(): Result[Option[Row]] =
        if limit >= 0 and produced >= limit:
          return ok(none(Row))
        while skipped < offset:
          let rRes = rowCursorNext(child)
          if not rRes.ok:
            return err[Option[Row]](rRes.err.code, rRes.err.message, rRes.err.context)
          if rRes.value.isNone:
            return ok(none(Row))
          skipped.inc
        let rRes = rowCursorNext(child)
        if not rRes.ok:
          return err[Option[Row]](rRes.err.code, rRes.err.message, rRes.err.context)
        if rRes.value.isNone:
          return ok(none(Row))
        produced.inc
        ok(rRes.value)
    )
    ok(c)

  of pkTrigramSeek, pkUnionDistinct, pkJoin, pkSort, pkAggregate, pkStatement:
    materialize()

proc tryCountNoRowsFast*(pager: Pager, catalog: Catalog, plan: Plan, params: seq[Value]): Result[Option[int64]] =
  ## Attempt to compute the number of rows produced by `plan` without
  ## materializing full `Row` objects.
  ##
  ## Returns:
  ## - ok(some(count)) when handled
  ## - ok(none(int64)) when the plan is not supported by the fast path
  ## - err(...) on real execution errors
  if plan == nil:
    return ok(none(int64))

  type LikeMode = enum
    lmGeneric
    lmContains
    lmPrefix
    lmSuffix

  proc parseLikePattern(pattern: string, caseInsensitive: bool): (LikeMode, string) =
    ## Extract a simple LIKE pattern into a fast match mode when possible.
    ## Returns (mode, needleText). For lmGeneric, needleText is unused.
    if pattern.find('_') >= 0:
      return (lmGeneric, "")

    # For now, only optimize the case-sensitive common forms.
    if not caseInsensitive:
      # '%needle%'
      if pattern.len >= 2 and pattern[0] == '%' and pattern[^1] == '%':
        let inner = pattern[1 .. ^2]
        if inner.len == 0:
          return (lmContains, "")
        if inner.find('%') < 0:
          return (lmContains, inner)

      # 'prefix%'
      if pattern.len >= 1 and pattern[^1] == '%' and pattern.find('%') == pattern.len - 1:
        let prefix = pattern[0 .. ^2]
        return (lmPrefix, prefix)

      # '%suffix'
      if pattern.len >= 1 and pattern[0] == '%' and pattern.rfind('%') == 0:
        let suffix = pattern[1 .. ^1]
        return (lmSuffix, suffix)

    (lmGeneric, "")

  proc extractSimpleLike(expr: Expr, table: string, columnOut: var string, patternOut: var Expr, insensitiveOut: var bool): bool =
    ## Return true for a single-column LIKE/ILIKE predicate.
    if expr == nil or expr.kind != ekBinary or not (expr.op in ["LIKE", "ILIKE"]):
      return false
    insensitiveOut = expr.op == "ILIKE"
    let left = expr.left
    let right = expr.right
    if left != nil and left.kind == ekColumn and (left.table.len == 0 or left.table == table):
      columnOut = left.name
      patternOut = right
      return true
    if right != nil and right.kind == ekColumn and (right.table.len == 0 or right.table == table):
      columnOut = right.name
      patternOut = left
      return true
    false

  proc bytesEqAt(hay: openArray[byte], hayStart: int, needle: openArray[byte]): bool {.inline.} =
    if needle.len == 0:
      return true
    if hayStart < 0 or hayStart + needle.len > hay.len:
      return false
    for i in 0 ..< needle.len:
      if hay[hayStart + i] != needle[i]:
        return false
    true

  type BmhSkipTable = array[256, int]

  proc initBmhSkipTable(needle: openArray[byte], skip: var BmhSkipTable) {.inline.} =
    ## Boyer–Moore–Horspool skip table for byte strings.
    let m = needle.len
    for i in 0 .. 255:
      skip[i] = m
    if m <= 1:
      return
    for i in 0 ..< (m - 1):
      skip[int(needle[i])] = (m - 1) - i

  proc bytesFindInBmh(hay: openArray[byte], hayStart: int, hayLen: int, needle: openArray[byte], skip: ptr BmhSkipTable): int =
    if needle.len == 0:
      return 0
    if hayLen < 0 or needle.len > hayLen:
      return -1
    if needle.len == 1:
      let b0 = needle[0]
      for i in hayStart ..< (hayStart + hayLen):
        if hay[i] == b0:
          return i - hayStart
      return -1
    let m = needle.len
    let lastNeedle = needle[m - 1]
    let endPos = hayStart + hayLen - m
    var i = hayStart
    while i <= endPos:
      let c = hay[i + m - 1]
      if c == lastNeedle and bytesEqAt(hay, i, needle):
        return i - hayStart
      i += skip[][int(c)]
    -1

  proc bytesFindIn(hay: openArray[byte], hayStart: int, hayLen: int, needle: openArray[byte]): int =
    if needle.len == 0:
      return 0
    if hayLen < 0 or needle.len > hayLen:
      return -1
    let last = hayStart + hayLen - needle.len
    for i in hayStart .. last:
      if hay[i] == needle[0] and bytesEqAt(hay, i, needle):
        return i - hayStart
    -1

  proc bytesStartsWithIn(hay: openArray[byte], hayStart: int, hayLen: int, needle: openArray[byte]): bool {.inline.} =
    if needle.len == 0:
      return true
    if needle.len > hayLen:
      return false
    bytesEqAt(hay, hayStart, needle)

  proc bytesEndsWithIn(hay: openArray[byte], hayStart: int, hayLen: int, needle: openArray[byte]): bool {.inline.} =
    if needle.len == 0:
      return true
    if needle.len > hayLen:
      return false
    bytesEqAt(hay, hayStart + hayLen - needle.len, needle)

  proc matchLikeInRecord(recordData: openArray[byte], colIndex: int, mode: LikeMode, needleBytes: seq[byte], pattern: string, caseInsensitive: bool, containsBmh: ptr BmhSkipTable): Result[bool] =
    ## Fast LIKE matcher for a single TEXT column inside a record.
    ## Falls back to full decode for uncommon encodings.
    var offset = 0
    let countRes = decodeVarint(recordData, offset)
    if not countRes.ok:
      return err[bool](countRes.err.code, countRes.err.message, countRes.err.context)
    let fieldCount = int(countRes.value)
    if colIndex < 0 or colIndex >= fieldCount:
      return ok(false)

    for idx in 0 ..< fieldCount:
      if offset >= recordData.len:
        return err[bool](ERR_CORRUPTION, "Unexpected end of record")
      let kindValue = int(recordData[offset])
      if kindValue < 0 or kindValue > ord(high(ValueKind)):
        return err[bool](ERR_CORRUPTION, "Unknown value kind")
      let kind = ValueKind(kindValue)
      offset.inc

      let lenRes = decodeVarint(recordData, offset)
      if not lenRes.ok:
        return err[bool](lenRes.err.code, lenRes.err.message, lenRes.err.context)
      let length = int(lenRes.value)
      if offset + length > recordData.len:
        return err[bool](ERR_CORRUPTION, "Record field length out of bounds")

      if idx == colIndex:
        case kind
        of vkNull:
          return ok(false)
        of vkText:
          if mode == lmGeneric or caseInsensitive:
            var s = newString(length)
            if length > 0:
              copyMem(addr s[0], unsafeAddr recordData[offset], length)
            let likeRes = likeMatchChecked(s, pattern, caseInsensitive)
            if not likeRes.ok:
              return err[bool](likeRes.err.code, likeRes.err.message, likeRes.err.context)
            return ok(likeRes.value)

          if mode == lmContains:
            if needleBytes.len == 0:
              return ok(true)
            if containsBmh != nil:
              return ok(bytesFindInBmh(recordData, offset, length, needleBytes, containsBmh) >= 0)
            return ok(bytesFindIn(recordData, offset, length, needleBytes) >= 0)
          if mode == lmPrefix:
            return ok(bytesStartsWithIn(recordData, offset, length, needleBytes))
          if mode == lmSuffix:
            return ok(bytesEndsWithIn(recordData, offset, length, needleBytes))
          return ok(false)
        else:
          # Overflow/compressed/non-text: decode fully and reuse generic matcher.
          let decoded = decodeRecordWithOverflow(pager, recordData)
          if not decoded.ok:
            return err[bool](decoded.err.code, decoded.err.message, decoded.err.context)
          if colIndex >= decoded.value.len:
            return ok(false)
          let v = decoded.value[colIndex]
          if v.kind != vkText:
            return ok(false)
          let s = valueToString(v)
          let likeRes = likeMatchChecked(s, pattern, caseInsensitive)
          if not likeRes.ok:
            return err[bool](likeRes.err.code, likeRes.err.message, likeRes.err.context)
          return ok(likeRes.value)

      offset += length
    ok(false)

  proc bytesEqAtStr(hay: string, hayStart: int, needle: openArray[byte]): bool {.inline.} =
    if needle.len == 0:
      return true
    if hayStart < 0 or hayStart + needle.len > hay.len:
      return false
    for i in 0 ..< needle.len:
      if byte(hay[hayStart + i]) != needle[i]:
        return false
    true

  proc bytesFindInBmhStr(hay: string, hayStart: int, hayLen: int, needle: openArray[byte], skip: ptr BmhSkipTable): int =
    if needle.len == 0:
      return 0
    if hayLen < 0 or needle.len > hayLen:
      return -1
    if needle.len == 1:
      let b0 = needle[0]
      for i in hayStart ..< (hayStart + hayLen):
        if byte(hay[i]) == b0:
          return i - hayStart
      return -1
    let m = needle.len
    let lastNeedle = needle[m - 1]
    let endPos = hayStart + hayLen - m
    var i = hayStart
    while i <= endPos:
      let c = byte(hay[i + m - 1])
      if c == lastNeedle and bytesEqAtStr(hay, i, needle):
        return i - hayStart
      i += skip[][int(c)]
    -1

  proc bytesStartsWithInStr(hay: string, hayStart: int, hayLen: int, needle: openArray[byte]): bool {.inline.} =
    if needle.len == 0:
      return true
    if needle.len > hayLen:
      return false
    bytesEqAtStr(hay, hayStart, needle)

  proc bytesEndsWithInStr(hay: string, hayStart: int, hayLen: int, needle: openArray[byte]): bool {.inline.} =
    if needle.len == 0:
      return true
    if needle.len > hayLen:
      return false
    bytesEqAtStr(hay, hayStart + hayLen - needle.len, needle)

  proc decodeVarintBoundedStr(data: string, offset: var int, endPos: int): Result[uint64] =
    ## Like decodeVarint, but refuses to read beyond endPos.
    var shift = 0
    var value: uint64 = 0
    while offset < endPos:
      let b = byte(data[offset])
      offset.inc
      value = value or (uint64(b and 0x7F) shl shift)
      if (b and 0x80) == 0:
        return ok(value)
      shift += 7
      if shift > 63:
        return err[uint64](ERR_CORRUPTION, "Varint overflow")
    err[uint64](ERR_CORRUPTION, "Unexpected end of varint")

  proc matchLikeInRecordStr(recordPage: string, recordStart: int, recordLen: int, colIndex: int, mode: LikeMode, needleBytes: seq[byte], pattern: string, caseInsensitive: bool, containsBmh: ptr BmhSkipTable): Result[bool] =
    ## Like matchLikeInRecord, but treats the record as a view into a page string.
    ## Avoids allocating/copying the record payload for the common vkText + simple LIKE cases.
    var offset = recordStart
    let endPos = recordStart + recordLen
    if offset < 0 or endPos < 0 or endPos > recordPage.len:
      return err[bool](ERR_CORRUPTION, "Record view out of bounds")

    let countRes = decodeVarintBoundedStr(recordPage, offset, endPos)
    if not countRes.ok:
      return err[bool](countRes.err.code, countRes.err.message, countRes.err.context)
    let fieldCount = int(countRes.value)
    if colIndex < 0 or colIndex >= fieldCount:
      return ok(false)

    for idx in 0 ..< fieldCount:
      if offset >= endPos:
        return err[bool](ERR_CORRUPTION, "Unexpected end of record")
      let kindValue = int(byte(recordPage[offset]))
      if kindValue < 0 or kindValue > ord(high(ValueKind)):
        return err[bool](ERR_CORRUPTION, "Unknown value kind")
      let kind = ValueKind(kindValue)
      offset.inc

      let lenRes = decodeVarintBoundedStr(recordPage, offset, endPos)
      if not lenRes.ok:
        return err[bool](lenRes.err.code, lenRes.err.message, lenRes.err.context)
      let length = int(lenRes.value)
      if offset + length > endPos:
        return err[bool](ERR_CORRUPTION, "Record field length out of bounds")

      if idx == colIndex:
        case kind
        of vkNull:
          return ok(false)
        of vkText:
          if mode == lmGeneric or caseInsensitive:
            var s = newString(length)
            if length > 0:
              copyMem(addr s[0], unsafeAddr recordPage[offset], length)
            let likeRes = likeMatchChecked(s, pattern, caseInsensitive)
            if not likeRes.ok:
              return err[bool](likeRes.err.code, likeRes.err.message, likeRes.err.context)
            return ok(likeRes.value)

          if mode == lmContains:
            if needleBytes.len == 0:
              return ok(true)
            if containsBmh != nil:
              return ok(bytesFindInBmhStr(recordPage, offset, length, needleBytes, containsBmh) >= 0)
            # Fallback for 1-byte needles.
            if needleBytes.len == 1:
              let b0 = needleBytes[0]
              for i in offset ..< (offset + length):
                if byte(recordPage[i]) == b0:
                  return ok(true)
              return ok(false)
            return ok(false)
          if mode == lmPrefix:
            return ok(bytesStartsWithInStr(recordPage, offset, length, needleBytes))
          if mode == lmSuffix:
            return ok(bytesEndsWithInStr(recordPage, offset, length, needleBytes))
          return ok(false)
        else:
          # Decode fully for non-text or special encodings.
          var recordBytes = newSeq[byte](recordLen)
          if recordLen > 0:
            copyMem(addr recordBytes[0], unsafeAddr recordPage[recordStart], recordLen)
          let decoded = decodeRecordWithOverflow(pager, recordBytes)
          if not decoded.ok:
            return err[bool](decoded.err.code, decoded.err.message, decoded.err.context)
          if colIndex >= decoded.value.len:
            return ok(false)
          let v = decoded.value[colIndex]
          if v.kind != vkText:
            return ok(false)
          let s = valueToString(v)
          let likeRes = likeMatchChecked(s, pattern, caseInsensitive)
          if not likeRes.ok:
            return err[bool](likeRes.err.code, likeRes.err.message, likeRes.err.context)
          return ok(likeRes.value)

      offset += length
    ok(false)

  proc countLikeTableScan(table: TableMeta, colIndex: int, patternStr: string, caseInsensitive: bool): Result[int64] =
    ## Count matches for a single-column LIKE by scanning the base table.
    let (mode, needleText) = parseLikePattern(patternStr, caseInsensitive)
    var needleBytes: seq[byte] = @[]
    if mode != lmGeneric:
      needleBytes = newSeq[byte](needleText.len)
      for i, ch in needleText:
        needleBytes[i] = byte(ch)

    var containsBmh: BmhSkipTable
    var containsBmhPtr: ptr BmhSkipTable = nil
    if mode == lmContains and needleBytes.len >= 2:
      initBmhSkipTable(needleBytes, containsBmh)
      containsBmhPtr = addr containsBmh
    let tree = newBTree(pager, table.rootPage)
    let cursorRes = openCursorStream(tree)
    if not cursorRes.ok:
      return err[int64](cursorRes.err.code, cursorRes.err.message, cursorRes.err.context)
    let btCursor = cursorRes.value
    var count: int64 = 0
    while true:
      let nextRes = cursorNextStream(btCursor)
      if not nextRes.ok:
        break
      let (_, leafPage, valueOffset, valueLen, leafOverflow) = nextRes.value
      if valueLen == 0 and leafOverflow == 0'u32:
        continue

      if leafOverflow != 0'u32:
        # Rare: row payload itself is in an overflow chain.
        let rowBytesRes = readOverflowChainAll(pager, PageId(leafOverflow))
        if not rowBytesRes.ok:
          return err[int64](rowBytesRes.err.code, rowBytesRes.err.message, rowBytesRes.err.context)
        let mRes = matchLikeInRecord(rowBytesRes.value, colIndex, mode, needleBytes, patternStr, caseInsensitive, containsBmhPtr)
        if not mRes.ok:
          return err[int64](mRes.err.code, mRes.err.message, mRes.err.context)
        if mRes.value:
          count.inc
      else:
        let mRes = matchLikeInRecordStr(leafPage, valueOffset, valueLen, colIndex, mode, needleBytes, patternStr, caseInsensitive, containsBmhPtr)
        if not mRes.ok:
          return err[int64](mRes.err.code, mRes.err.message, mRes.err.context)
        if mRes.value:
          count.inc
    ok(count)

  case plan.kind
  of pkProject:
    # Projection does not affect row count.
    return tryCountNoRowsFast(pager, catalog, plan.left, params)

  of pkLimit:
    let innerRes = tryCountNoRowsFast(pager, catalog, plan.left, params)
    if not innerRes.ok:
      return err[Option[int64]](innerRes.err.code, innerRes.err.message, innerRes.err.context)
    if innerRes.value.isNone:
      return ok(none(int64))
    var count = innerRes.value.get
    var limit = plan.limit
    var offset = plan.offset
    if plan.limitParam > 0:
      let idx = plan.limitParam - 1
      if idx < 0 or idx >= params.len:
        return err[Option[int64]](ERR_SQL, "LIMIT parameter index out of bounds")
      let v = params[idx]
      if v.kind != vkInt64:
        return err[Option[int64]](ERR_SQL, "LIMIT parameter must be INT64")
      if v.int64Val < 0:
        return err[Option[int64]](ERR_SQL, "LIMIT parameter must be non-negative")
      if v.int64Val > int64(high(int)):
        return err[Option[int64]](ERR_SQL, "LIMIT parameter too large")
      limit = int(v.int64Val)
    if plan.offsetParam > 0:
      let idx = plan.offsetParam - 1
      if idx < 0 or idx >= params.len:
        return err[Option[int64]](ERR_SQL, "OFFSET parameter index out of bounds")
      let v = params[idx]
      if v.kind != vkInt64:
        return err[Option[int64]](ERR_SQL, "OFFSET parameter must be INT64")
      if v.int64Val < 0:
        return err[Option[int64]](ERR_SQL, "OFFSET parameter must be non-negative")
      if v.int64Val > int64(high(int)):
        return err[Option[int64]](ERR_SQL, "OFFSET parameter too large")
      offset = int(v.int64Val)
    if offset > 0:
      if count <= int64(offset):
        count = 0
      else:
        count -= int64(offset)
    if limit >= 0 and count > int64(limit):
      count = int64(limit)
    return ok(some(count))

  of pkIndexSeek:
    let valueRes = evalExpr(Row(), plan.valueExpr, params)
    if not valueRes.ok:
      return err[Option[int64]](valueRes.err.code, valueRes.err.message, valueRes.err.context)
    # Only safe for non-hashed key types.
    if valueRes.value.kind notin {vkInt64, vkBool, vkFloat64}:
      return ok(none(int64))
    let indexOpt = catalog.getBtreeIndexForColumn(plan.table, plan.column)
    if isNone(indexOpt):
      return err[Option[int64]](ERR_SQL, "Index not found", plan.table & "." & plan.column)
    let idx = indexOpt.get
    let needle = indexKeyFromValue(valueRes.value)
    let idxTree = newBTree(pager, idx.rootPage)
    let idxCursorRes = openCursorAt(idxTree, needle)
    if not idxCursorRes.ok:
      return err[Option[int64]](idxCursorRes.err.code, idxCursorRes.err.message, idxCursorRes.err.context)
    let idxCursor = idxCursorRes.value
    var count: int64 = 0
    while true:
      let nextRes = cursorNext(idxCursor)
      if not nextRes.ok:
        break
      let key = nextRes.value[0]
      if key < needle:
        continue
      if key > needle:
        break
      count.inc
    return ok(some(count))

  of pkRowidSeek:
    let valueRes = evalExpr(Row(), plan.valueExpr, params)
    if not valueRes.ok:
      return err[Option[int64]](valueRes.err.code, valueRes.err.message, valueRes.err.context)
    if valueRes.value.kind != vkInt64:
      return err[Option[int64]](ERR_SQL, "Rowid seek expects INT64")
    let tableRes = catalog.getTable(plan.table)
    if not tableRes.ok:
      return err[Option[int64]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value
    let tree = newBTree(pager, table.rootPage)
    let existsRes = containsKey(tree, cast[uint64](valueRes.value.int64Val))
    if not existsRes.ok:
      return err[Option[int64]](existsRes.err.code, existsRes.err.message, existsRes.err.context)
    return ok(some(if existsRes.value: 1'i64 else: 0'i64))

  of pkTrigramSeek:
    let patternRes = evalExpr(Row(), plan.likeExpr, params)
    if not patternRes.ok:
      return err[Option[int64]](patternRes.err.code, patternRes.err.message, patternRes.err.context)
    let patternStr = valueToString(patternRes.value)

    let tableRes = catalog.getTable(plan.table)
    if not tableRes.ok:
      return err[Option[int64]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value

    let indexOpt = catalog.getTrigramIndexForColumn(plan.table, plan.column)
    if isNone(indexOpt):
      return err[Option[int64]](ERR_SQL, "Trigram index not found", plan.table & "." & plan.column)
    let idx = indexOpt.get

    var colIndex = -1
    for i, col in table.columns:
      if col.name == plan.column:
        colIndex = i
        break
    if colIndex < 0:
      return err[Option[int64]](ERR_SQL, "Column not found", plan.column)

    # Mirror trigramSeekRows heuristics.
    var stripped = ""
    for ch in patternStr:
      if ch != '%' and ch != '_':
        stripped.add(ch)
    let normalized = canonicalize(stripped)
    if normalized.len < 3:
      let countRes = countLikeTableScan(table, colIndex, patternStr, plan.likeInsensitive)
      if not countRes.ok:
        return err[Option[int64]](countRes.err.code, countRes.err.message, countRes.err.context)
      return ok(some(countRes.value))

    let grams = trigrams(normalized)
    if grams.len == 0:
      return ok(some(0'i64))

    let threshold = DefaultPostingsThreshold
    var postingsLists: seq[seq[uint64]] = @[]
    var rarestCount = -1
    var anyTruncated = false
    for g in grams:
      let postRes = getTrigramPostingsWithDeltasUpTo(pager, catalog, idx, g, threshold)
      if not postRes.ok:
        return err[Option[int64]](postRes.err.code, postRes.err.message, postRes.err.context)
      if postRes.value.truncated:
        anyTruncated = true
        break
      let list = postRes.value.ids
      if list.len == 0:
        return ok(some(0'i64))
      postingsLists.add(list)
      if rarestCount < 0 or list.len < rarestCount:
        rarestCount = list.len

    if anyTruncated:
      let countRes = countLikeTableScan(table, colIndex, patternStr, plan.likeInsensitive)
      if not countRes.ok:
        return err[Option[int64]](countRes.err.code, countRes.err.message, countRes.err.context)
      return ok(some(countRes.value))
    if normalized.len <= 5 and rarestCount >= threshold:
      let countRes = countLikeTableScan(table, colIndex, patternStr, plan.likeInsensitive)
      if not countRes.ok:
        return err[Option[int64]](countRes.err.code, countRes.err.message, countRes.err.context)
      return ok(some(countRes.value))

    var candidates = intersectPostings(postingsLists)
    if normalized.len > 5 and rarestCount >= threshold and candidates.len > threshold:
      candidates.setLen(threshold)

    var count: int64 = 0
    for rowid in candidates:
      let readRes = readRowAt(pager, table, rowid)
      if not readRes.ok:
        continue
      if colIndex >= readRes.value.values.len:
        continue
      let text = valueToString(readRes.value.values[colIndex])
      let likeRes = likeMatchChecked(text, patternStr, plan.likeInsensitive)
      if not likeRes.ok:
        return err[Option[int64]](likeRes.err.code, likeRes.err.message, likeRes.err.context)
      if likeRes.value:
        count.inc
    return ok(some(count))

  of pkFilter:
    if plan.predicate == nil:
      return tryCountNoRowsFast(pager, catalog, plan.left, params)
    if plan.left == nil or plan.left.kind != pkTableScan:
      return ok(none(int64))

    let tableRes = catalog.getTable(plan.left.table)
    if not tableRes.ok:
      return err[Option[int64]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value

    var colName = ""
    var patExpr: Expr = nil
    var insensitive = false
    if not extractSimpleLike(plan.predicate, plan.left.table, colName, patExpr, insensitive):
      return ok(none(int64))

    var colIndex = -1
    for i, col in table.columns:
      if col.name == colName:
        colIndex = i
        break
    if colIndex < 0:
      return ok(none(int64))

    let patValRes = evalExpr(Row(), patExpr, params)
    if not patValRes.ok:
      return err[Option[int64]](patValRes.err.code, patValRes.err.message, patValRes.err.context)
    if patValRes.value.kind != vkText:
      return err[Option[int64]](ERR_SQL, "LIKE pattern must be TEXT")
    let patternStr = valueToString(patValRes.value)
    let countRes = countLikeTableScan(table, colIndex, patternStr, insensitive)
    if not countRes.ok:
      return err[Option[int64]](countRes.err.code, countRes.err.message, countRes.err.context)
    return ok(some(countRes.value))

  else:
    return ok(none(int64))

const SortBufferBytes = 16 * 1024 * 1024
const SortMaxOpenRuns = 64
var sortTempId: Atomic[uint64]

proc applyLimit*(rows: seq[Row], limit: int, offset: int): seq[Row]

proc varintLen*(value: uint64): int =
  var v = value
  result = 1
  while v >= 0x80'u64:
    v = v shr 7
    result.inc

proc estimateRowBytes*(row: Row): int =
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
    of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
      payloadLen = value.bytes.len
    of vkTextOverflow, vkBlobOverflow, vkTextCompressedOverflow, vkBlobCompressedOverflow:
      payloadLen = 8
    result += varintLen(uint64(payloadLen)) + payloadLen

proc columnIndex*(row: Row, table: string, name: string): Result[int] =
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
  of vkText, vkBlob, vkTextCompressed, vkBlobCompressed: value.bytes.len > 0
  else: false

proc compareValues*(a: Value, b: Value): int =
  if a.kind != b.kind:
    return cmp(a.kind, b.kind)
  case a.kind
  of vkNull: 0
  of vkBool: cmp(a.boolVal, b.boolVal)
  of vkInt64: cmp(a.int64Val, b.int64Val)
  of vkFloat64: cmp(a.float64Val, b.float64Val)
  of vkText, vkBlob, vkTextCompressed, vkBlobCompressed:
    var lenA = a.bytes.len
    var lenB = b.bytes.len
    var minLen = min(lenA, lenB)
    if minLen > 0:
      let c = cmpMem(unsafeAddr a.bytes[0], unsafeAddr b.bytes[0], minLen)
      if c != 0: return c
    return cmp(lenA, lenB)
  else:
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
    if expr.table.len == 0:
      let lower = expr.name.toLowerAscii()
      if lower == "true": return ok(Value(kind: vkBool, boolVal: true))
      if lower == "false": return ok(Value(kind: vkBool, boolVal: false))
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
    of "+", "-", "*", "/":
      if leftRes.value.kind == vkNull or rightRes.value.kind == vkNull:
        return ok(Value(kind: vkNull))
      if leftRes.value.kind notin {vkInt64, vkFloat64} or rightRes.value.kind notin {vkInt64, vkFloat64}:
        return err[Value](ERR_SQL, "Numeric operator on non-numeric", expr.op)
      if leftRes.value.kind == vkFloat64 or rightRes.value.kind == vkFloat64:
        let l = if leftRes.value.kind == vkFloat64: leftRes.value.float64Val else: float64(leftRes.value.int64Val)
        let r = if rightRes.value.kind == vkFloat64: rightRes.value.float64Val else: float64(rightRes.value.int64Val)
        case expr.op
        of "+":
          return ok(Value(kind: vkFloat64, float64Val: l + r))
        of "-":
          return ok(Value(kind: vkFloat64, float64Val: l - r))
        of "*":
          return ok(Value(kind: vkFloat64, float64Val: l * r))
        of "/":
          if r == 0.0:
            return err[Value](ERR_SQL, "Division by zero")
          return ok(Value(kind: vkFloat64, float64Val: l / r))
        else:
          return err[Value](ERR_SQL, "Unsupported operator", expr.op)
      else:
        let l = leftRes.value.int64Val
        let r = rightRes.value.int64Val
        case expr.op
        of "+":
          return ok(Value(kind: vkInt64, int64Val: l + r))
        of "-":
          return ok(Value(kind: vkInt64, int64Val: l - r))
        of "*":
          return ok(Value(kind: vkInt64, int64Val: l * r))
        of "/":
          if r == 0:
            return err[Value](ERR_SQL, "Division by zero")
          return ok(Value(kind: vkInt64, int64Val: l div r))
        else:
          return err[Value](ERR_SQL, "Unsupported operator", expr.op)
    of "LIKE", "ILIKE":
      let leftStr = valueToString(leftRes.value)
      let rightStr = valueToString(rightRes.value)
      let likeRes = likeMatchChecked(leftStr, rightStr, expr.op == "ILIKE")
      if not likeRes.ok:
        return err[Value](likeRes.err.code, likeRes.err.message, likeRes.err.context)
      return ok(Value(kind: vkBool, boolVal: likeRes.value))
    of "IS":
       if rightRes.value.kind == vkNull:
         return ok(Value(kind: vkBool, boolVal: leftRes.value.kind == vkNull))
       return ok(Value(kind: vkBool, boolVal: compareValues(leftRes.value, rightRes.value) == 0))
    of "IS NOT":
       if rightRes.value.kind == vkNull:
         return ok(Value(kind: vkBool, boolVal: leftRes.value.kind != vkNull))
       return ok(Value(kind: vkBool, boolVal: compareValues(leftRes.value, rightRes.value) != 0))
    else:
      return err[Value](ERR_SQL, "Unsupported operator", expr.op)
  of ekFunc:
    return err[Value](ERR_SQL, "Aggregate functions evaluated elsewhere")
  of ekInList:
    # Evaluate the expression being tested
    let exprRes = evalExpr(row, expr.inExpr, params)
    if not exprRes.ok:
      return err[Value](exprRes.err.code, exprRes.err.message, exprRes.err.context)
    
    # If the expression is NULL, result is NULL (3-valued logic)
    if exprRes.value.kind == vkNull:
      return ok(Value(kind: vkNull))
    
    # Check if value matches any item in the IN list
    for item in expr.inList:
      let itemRes = evalExpr(row, item, params)
      if not itemRes.ok:
        return err[Value](itemRes.err.code, itemRes.err.message, itemRes.err.context)
      
      # Compare values
      if compareValues(exprRes.value, itemRes.value) == 0:
        return ok(Value(kind: vkBool, boolVal: true))
    
    # No match found
    return ok(Value(kind: vkBool, boolVal: false))

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
      let likeRes = likeMatchChecked(text, pattern, caseInsensitive)
      if not likeRes.ok:
        return err[seq[Row]](likeRes.err.code, likeRes.err.message, likeRes.err.context)
      if likeRes.value:
        filtered.add(row)
    return ok(filtered)
  let grams = trigrams(normalized)
  if grams.len == 0:
    return ok(newSeq[Row]())
  let threshold = DefaultPostingsThreshold
  var postingsLists: seq[seq[uint64]] = @[]
  var rarestCount = -1
  var anyTruncated = false
  for g in grams:
    let postRes = getTrigramPostingsWithDeltasUpTo(pager, catalog, idx, g, threshold)
    if not postRes.ok:
      return err[seq[Row]](postRes.err.code, postRes.err.message, postRes.err.context)
    if postRes.value.truncated:
      anyTruncated = true
      break
    let list = postRes.value.ids
    if list.len == 0:
      return ok(newSeq[Row]())
    postingsLists.add(list)
    if rarestCount < 0 or list.len < rarestCount:
      rarestCount = list.len

  if anyTruncated:
    let rowsRes = tableScanRows(pager, catalog, tableName, alias)
    if not rowsRes.ok:
      return err[seq[Row]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
    var filtered: seq[Row] = @[]
    for row in rowsRes.value:
      let text = valueToString(row.values[columnIndex])
      let likeRes = likeMatchChecked(text, pattern, caseInsensitive)
      if not likeRes.ok:
        return err[seq[Row]](likeRes.err.code, likeRes.err.message, likeRes.err.context)
      if likeRes.value:
        filtered.add(row)
    return ok(filtered)
  if normalized.len <= 5 and rarestCount >= threshold:
    let rowsRes = tableScanRows(pager, catalog, tableName, alias)
    if not rowsRes.ok:
      return err[seq[Row]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
    var filtered: seq[Row] = @[]
    for row in rowsRes.value:
      let text = valueToString(row.values[columnIndex])
      let likeRes = likeMatchChecked(text, pattern, caseInsensitive)
      if not likeRes.ok:
        return err[seq[Row]](likeRes.err.code, likeRes.err.message, likeRes.err.context)
      if likeRes.value:
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
    let likeRes = likeMatchChecked(text, pattern, caseInsensitive)
    if not likeRes.ok:
      return err[seq[Row]](likeRes.err.code, likeRes.err.message, likeRes.err.context)
    if not likeRes.value:
      continue
    rows.add(makeRow(cols, readRes.value.values, rowid))
  ok(rows)

proc applyFilter*(rows: seq[Row], expr: Expr, params: seq[Value]): Result[seq[Row]] =
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

proc projectRows*(rows: seq[Row], items: seq[SelectItem], params: seq[Value]): Result[seq[Row]] =
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
        var name = if item.alias.len > 0: item.alias else: ""
        if name.len == 0 and item.expr.kind == ekColumn:
          name = item.expr.name
        if name.len == 0:
          name = "expr"
        cols.add(name)
        vals.add(evalRes.value)
    resultRows.add(makeRow(cols, vals, row.rowid))
  ok(resultRows)

type AggState = object
  count: int64
  sum: float64
  min: Value
  max: Value
  initialized: bool

proc aggregateRows*(rows: seq[Row], items: seq[SelectItem], groupBy: seq[Expr], having: Expr, params: seq[Value]): Result[seq[Row]] =
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
  
  if rows.len == 0 and groupBy.len == 0:
    # Scalar aggregate on empty set
    groups[""] = AggState()
    groupRows[""] = Row(columns: @[], values: @[])

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

proc writeRowChunk*(path: string, rows: seq[Row]) =
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

type ChunkReader* = ref object
  file: File
  columns: seq[string]
  peeked: Option[Row]
  finished: bool

proc openChunkReader*(path: string, columns: seq[string]): ChunkReader =
  var f: File
  if not open(f, path, fmRead):
    return ChunkReader(finished: true)
  result = ChunkReader(file: f, columns: columns, finished: false)
  
  # Read first row
  var lenBuf = newSeq[byte](4)
  let readLen = result.file.readBuffer(lenBuf[0].addr, 4)
  if readLen < 4:
    result.finished = true
    close(result.file)
    return

  let length = int(readU32LE(lenBuf, 0))
  var data = newSeq[byte](length)
  if length > 0:
    let readData = result.file.readBuffer(data[0].addr, length)
    if readData < length:
      result.finished = true
      close(result.file)
      return

  let decoded = decodeRecord(data)
  if decoded.ok:
    result.peeked = some(makeRow(columns, decoded.value))
  else:
    result.finished = true
    close(result.file)

proc next*(reader: ChunkReader): Option[Row] =
  if reader.finished:
    return none(Row)
  
  result = reader.peeked
  
  # Advance to next
  var lenBuf = newSeq[byte](4)
  let readLen = reader.file.readBuffer(lenBuf[0].addr, 4)
  if readLen < 4:
    reader.finished = true
    close(reader.file)
    reader.peeked = none(Row)
    return

  let length = int(readU32LE(lenBuf, 0))
  var data = newSeq[byte](length)
  if length > 0:
    let readData = reader.file.readBuffer(data[0].addr, length)
    if readData < length:
      reader.finished = true
      close(reader.file)
      reader.peeked = none(Row)
      return

  let decoded = decodeRecord(data)
  if decoded.ok:
    reader.peeked = some(makeRow(reader.columns, decoded.value))
  else:
    reader.finished = true
    close(reader.file)
    reader.peeked = none(Row)

proc close*(reader: ChunkReader) =
  if not reader.finished:
    close(reader.file)
    reader.finished = true

proc removeTempFiles(paths: seq[string]) =
  for path in paths:
    if fileExists(path):
      try:
        removeFile(path)
      except:
        discard

proc writeRowToFile(f: File, row: Row, lenBuf: var seq[byte]) =
  let data = encodeRecord(row.values)
  writeU32LE(lenBuf, 0, uint32(data.len))
  discard f.writeBuffer(lenBuf[0].addr, lenBuf.len)
  if data.len > 0:
    discard f.writeBuffer(data[0].addr, data.len)

proc mergeRunsToRows(runPaths: seq[string], columns: seq[string], cmpRows: proc(a, b: Row): int, maxOutputRows: int, skipRows: int): Result[seq[Row]] =
  var readers: seq[ChunkReader] = @[]
  for path in runPaths:
    readers.add(openChunkReader(path, columns))

  defer:
    for reader in readers:
      reader.close()

  var skipped = 0
  var produced = 0
  var resultRows: seq[Row] = @[]

  while true:
    var bestIdx = -1
    var bestRow: Row

    for i, reader in readers:
      if reader.peeked.isNone:
        continue
      let candidate = reader.peeked.get
      if bestIdx < 0 or cmpRows(candidate, bestRow) < 0:
        bestIdx = i
        bestRow = candidate

    if bestIdx < 0:
      break

    discard next(readers[bestIdx])

    if skipped < skipRows:
      skipped.inc
      continue

    resultRows.add(bestRow)
    produced.inc
    if maxOutputRows >= 0 and produced >= maxOutputRows:
      break

  ok(resultRows)

proc mergeRunsToFile(runPaths: seq[string], outPath: string, columns: seq[string], cmpRows: proc(a, b: Row): int): Result[Void] =
  var outFile: File
  if not open(outFile, outPath, fmWrite):
    return err[Void](ERR_IO, "Failed to open sort merge output", outPath)
  defer:
    close(outFile)

  var readers: seq[ChunkReader] = @[]
  for path in runPaths:
    readers.add(openChunkReader(path, columns))
  defer:
    for reader in readers:
      reader.close()

  var lenBuf = newSeq[byte](4)
  while true:
    var bestIdx = -1
    var bestRow: Row

    for i, reader in readers:
      if reader.peeked.isNone:
        continue
      let candidate = reader.peeked.get
      if bestIdx < 0 or cmpRows(candidate, bestRow) < 0:
        bestIdx = i
        bestRow = candidate

    if bestIdx < 0:
      break

    discard next(readers[bestIdx])
    writeRowToFile(outFile, bestRow, lenBuf)

  okVoid()

proc sortRowsWithConfig*(rows: seq[Row], orderBy: seq[OrderItem], params: seq[Value], limit: int = -1, offset: int = 0, bufferBytes: int = SortBufferBytes, maxOpenRuns: int = SortMaxOpenRuns, tempPrefix: string = "decentdb_sort_"): Result[seq[Row]] =
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

  let skipRows = max(0, offset)
  let maxOutputRows =
    if limit < 0:
      -1
    else:
      limit

  if rows.len <= 1:
    var sorted = rows
    sorted.sort(proc(x, y: Row): int = cmpRows(x, y))
    return ok(applyLimit(sorted, limit, offset))

  let invocationId = sortTempId.fetchAdd(1'u64)
  let invPrefix = tempPrefix & $getCurrentProcessId() & "_" & $invocationId & "_"

  var tempFiles: seq[string] = @[]
  var chunk: seq[Row] = @[]
  var chunkBytes = 0
  for row in rows:
    let rowBytes = estimateRowBytes(row)
    if chunk.len > 0 and chunkBytes + rowBytes > bufferBytes:
      chunk.sort(proc(x, y: Row): int = cmpRows(x, y))
      let path = getTempDir() / (invPrefix & $tempFiles.len & ".tmp")
      writeRowChunk(path, chunk)
      tempFiles.add(path)
      chunk = @[]
      chunkBytes = 0
    chunk.add(row)
    chunkBytes += rowBytes

  if tempFiles.len == 0:
    var sorted = rows
    sorted.sort(proc(x, y: Row): int = cmpRows(x, y))
    return ok(applyLimit(sorted, limit, offset))

  if chunk.len > 0:
    chunk.sort(proc(x, y: Row): int = cmpRows(x, y))
    let path = getTempDir() / (invPrefix & $tempFiles.len & ".tmp")
    writeRowChunk(path, chunk)
    tempFiles.add(path)

  let columns = if rows.len > 0: rows[0].columns else: @[]

  var allTempFiles = tempFiles
  var runs = tempFiles
  var pass = 0

  defer:
    removeTempFiles(allTempFiles)

  while runs.len > maxOpenRuns:
    var nextRuns: seq[string] = @[]
    var idx = 0
    while idx < runs.len:
      let endIdx = min(idx + maxOpenRuns, runs.len)
      let group = runs[idx ..< endIdx]
      let outPath = getTempDir() / (invPrefix & "merge_" & $pass & "_" & $nextRuns.len & ".tmp")
      let mergeRes = mergeRunsToFile(group, outPath, columns, cmpRows)
      if not mergeRes.ok:
        return err[seq[Row]](mergeRes.err.code, mergeRes.err.message, mergeRes.err.context)
      nextRuns.add(outPath)
      allTempFiles.add(outPath)
      idx = endIdx
    runs = nextRuns
    pass.inc

  mergeRunsToRows(runs, columns, cmpRows, maxOutputRows, skipRows)

proc sortRows*(rows: seq[Row], orderBy: seq[OrderItem], params: seq[Value]): Result[seq[Row]] =
  sortRowsWithConfig(rows, orderBy, params)

proc applyLimit*(rows: seq[Row], limit: int, offset: int): seq[Row] =
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
  of pkRowidSeek:
    let valueRes = evalExpr(Row(), plan.valueExpr, params)
    if not valueRes.ok:
      return err[seq[Row]](valueRes.err.code, valueRes.err.message, valueRes.err.context)
    if valueRes.value.kind != vkInt64:
      return err[seq[Row]](ERR_SQL, "Rowid seek expects INT64")
    let tableRes = catalog.getTable(plan.table)
    if not tableRes.ok:
      return err[seq[Row]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    let table = tableRes.value
    let prefix = if plan.alias.len > 0: plan.alias else: plan.table
    var cols: seq[string] = @[]
    for col in table.columns:
      cols.add(prefix & "." & col.name)
    let targetRowId = cast[uint64](valueRes.value.int64Val)
    let readRes = readRowAt(pager, table, targetRowId)
    if not readRes.ok:
      if readRes.err.code == ERR_IO and readRes.err.message == "Key not found":
        return ok(newSeq[Row]())
      return err[seq[Row]](readRes.err.code, readRes.err.message, readRes.err.context)
    return ok(@[makeRow(cols, readRes.value.values, targetRowId)])
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
  of pkUnionDistinct:
    let leftRes = execPlan(pager, catalog, plan.left, params)
    if not leftRes.ok:
      return err[seq[Row]](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    let rightRes = execPlan(pager, catalog, plan.right, params)
    if not rightRes.ok:
      return err[seq[Row]](rightRes.err.code, rightRes.err.message, rightRes.err.context)

    let leftRows = leftRes.value
    let rightRows = rightRes.value
    if leftRows.len > 0 and rightRows.len > 0 and leftRows[0].columns != rightRows[0].columns:
      return err[seq[Row]](ERR_SQL, "UNION requires matching column sets")

    var seen = initHashSet[uint64]()
    var outRows: seq[Row] = @[]
    for r in leftRows:
      if not seen.contains(r.rowid):
        seen.incl(r.rowid)
        outRows.add(r)
    for r in rightRows:
      if not seen.contains(r.rowid):
        seen.incl(r.rowid)
        outRows.add(r)
    return ok(outRows)
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
    # Only cache right side if it's not an index seek AND we have a reasonable number of left rows
    # This prevents memory blowup when joining large tables
    const MaxLeftRowsForCache = 100
    let canCacheRight = plan.right.kind != pkIndexSeek and leftRes.value.len <= MaxLeftRowsForCache
    var cachedRight: seq[Row] = @[]
    if canCacheRight:
      let rightRes = execPlan(pager, catalog, plan.right, params)
      if not rightRes.ok:
        return err[seq[Row]](rightRes.err.code, rightRes.err.message, rightRes.err.context)
      cachedRight = rightRes.value
    if plan.right.table.len > 0:
      let tableRes = catalog.getTable(plan.right.table)
      if tableRes.ok:
        let prefix = if plan.right.alias.len > 0: plan.right.alias else: plan.right.table
        for col in tableRes.value.columns:
          rightColumns.add(prefix & "." & col.name)
    proc joinPredicateIsCoveredByIndexSeek(): bool =
      if plan.right.kind != pkIndexSeek:
        return false
      if plan.joinOn == nil or plan.joinOn.kind != ekBinary or plan.joinOn.op != "=":
        return false
      let left = plan.joinOn.left
      let right = plan.joinOn.right
      if left == nil or right == nil:
        return false
      proc isRightCol(expr: Expr): bool =
        if expr.kind != ekColumn:
          return false
        if expr.name != plan.right.column:
          return false
        if plan.right.alias.len > 0 and expr.table == plan.right.alias:
          return true
        if expr.table.len == 0 or expr.table == plan.right.table:
          return true
        false
      (isRightCol(left) and right.kind == ekColumn) or (isRightCol(right) and left.kind == ekColumn)
    let skipJoinPredicate = joinPredicateIsCoveredByIndexSeek()
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
        if not canCacheRight:
          # Fallback: execute right plan for each left row (Nested Loop Join)
          # This is necessary because we couldn't cache the right side (e.g. too many left rows)
          # and it's not an index seek.
          let rightRes = execPlan(pager, catalog, plan.right, params)
          if not rightRes.ok:
            return err[seq[Row]](rightRes.err.code, rightRes.err.message, rightRes.err.context)
          rightRows = rightRes.value
        else:
          rightRows = cachedRight
      if rightColumns.len == 0 and rightRows.len > 0:
        rightColumns = rightRows[0].columns
      for rrow in rightRows:
        var merged = Row(columns: lrow.columns & rrow.columns, values: lrow.values & rrow.values)
        if skipJoinPredicate:
          matched = true
          resultRows.add(merged)
        else:
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
    proc resolveLimitOffset(): Result[(int, int)] =
      var limit = plan.limit
      var offset = plan.offset

      if plan.limitParam > 0:
        let i = plan.limitParam - 1
        if i < 0 or i >= params.len:
          return err[(int, int)](ERR_SQL, "LIMIT parameter index out of bounds")
        let v = params[i]
        if v.kind != vkInt64:
          return err[(int, int)](ERR_SQL, "LIMIT parameter must be INT64")
        if v.int64Val < 0:
          return err[(int, int)](ERR_SQL, "LIMIT parameter must be non-negative")
        if v.int64Val > int64(high(int)):
          return err[(int, int)](ERR_SQL, "LIMIT parameter too large")
        limit = int(v.int64Val)

      if plan.offsetParam > 0:
        let i = plan.offsetParam - 1
        if i < 0 or i >= params.len:
          return err[(int, int)](ERR_SQL, "OFFSET parameter index out of bounds")
        let v = params[i]
        if v.kind != vkInt64:
          return err[(int, int)](ERR_SQL, "OFFSET parameter must be INT64")
        if v.int64Val < 0:
          return err[(int, int)](ERR_SQL, "OFFSET parameter must be non-negative")
        if v.int64Val > int64(high(int)):
          return err[(int, int)](ERR_SQL, "OFFSET parameter too large")
        offset = int(v.int64Val)

      ok((limit, offset))

    let loRes = resolveLimitOffset()
    if not loRes.ok:
      return err[seq[Row]](loRes.err.code, loRes.err.message, loRes.err.context)
    let limit = loRes.value[0]
    let offset = loRes.value[1]

    if plan.left.kind == pkSort:
      let inputRes = execPlan(pager, catalog, plan.left.left, params)
      if not inputRes.ok:
        return err[seq[Row]](inputRes.err.code, inputRes.err.message, inputRes.err.context)
      return sortRowsWithConfig(inputRes.value, plan.left.orderBy, params, limit, offset)

    let inputRes = execPlan(pager, catalog, plan.left, params)
    if not inputRes.ok:
      return err[seq[Row]](inputRes.err.code, inputRes.err.message, inputRes.err.context)
    return ok(applyLimit(inputRes.value, limit, offset))
  of pkStatement:
    return ok(newSeq[Row]())
