import options
import tables
import sets
import sequtils
import strutils
import ../errors
import ../catalog/catalog
import ./sql

const MaxViewExpansionDepth* = 16
const MaxExpandedAstNodes* = 10_000

proc bindStatement*(catalog: Catalog, stmt: Statement): Result[Statement]
  # stderr.writeLine("DEBUG: bindStatement kind=", stmt.kind)
  
proc normalizedName(name: string): string =
  name.toLowerAscii()

proc nullExpr(): Expr =
  Expr(kind: ekLiteral, value: SqlValue(kind: svNull))

proc literalType(expr: Expr): Option[ColumnType] =
  if expr == nil or expr.kind != ekLiteral:
    return none(ColumnType)
  case expr.value.kind
  of svNull:
    return none(ColumnType)
  of svInt:
    return some(ctInt64)
  of svBool:
    return some(ctBool)
  of svFloat:
    return some(ctFloat64)
  of svString:
    return some(ctText)
  of svParam:
    return none(ColumnType)

proc checkLiteralType(expr: Expr, expected: ColumnType, columnName: string): Result[Void] =
  let lit = literalType(expr)
  if isSome(lit) and lit.get != expected:
    return err[Void](ERR_SQL, "Type mismatch for column", columnName)
  okVoid()

proc cloneExpr(expr: Expr): Expr

proc cloneExpr(expr: Expr): Expr =
  if expr == nil:
    return nil
  case expr.kind
  of ekLiteral:
    Expr(kind: ekLiteral, value: expr.value)
  of ekColumn:
    Expr(kind: ekColumn, table: expr.table, name: expr.name)
  of ekBinary:
    Expr(kind: ekBinary, op: expr.op, left: cloneExpr(expr.left), right: cloneExpr(expr.right))
  of ekUnary:
    Expr(kind: ekUnary, unOp: expr.unOp, expr: cloneExpr(expr.expr))
  of ekFunc:
    var args: seq[Expr] = @[]
    for arg in expr.args:
      args.add(cloneExpr(arg))
    Expr(kind: ekFunc, funcName: expr.funcName, args: args, isStar: expr.isStar)
  of ekParam:
    Expr(kind: ekParam, index: expr.index)
  of ekInList:
    var listExprs: seq[Expr] = @[]
    for item in expr.inList:
      listExprs.add(cloneExpr(item))
    Expr(kind: ekInList, inExpr: cloneExpr(expr.inExpr), inList: listExprs)
  of ekWindowRowNumber:
    var partitions: seq[Expr] = @[]
    for part in expr.windowPartitions:
      partitions.add(cloneExpr(part))
    var orderExprs: seq[Expr] = @[]
    for o in expr.windowOrderExprs:
      orderExprs.add(cloneExpr(o))
    Expr(
      kind: ekWindowRowNumber,
      windowPartitions: partitions,
      windowOrderExprs: orderExprs,
      windowOrderAsc: expr.windowOrderAsc
    )

proc qualifyInsertConflictExpr(expr: Expr, tableName: string): Expr =
  if expr == nil:
    return nil
  case expr.kind
  of ekLiteral:
    Expr(kind: ekLiteral, value: expr.value)
  of ekColumn:
    if expr.table.len == 0:
      Expr(kind: ekColumn, table: tableName, name: expr.name)
    else:
      Expr(kind: ekColumn, table: expr.table, name: expr.name)
  of ekBinary:
    Expr(
      kind: ekBinary,
      op: expr.op,
      left: qualifyInsertConflictExpr(expr.left, tableName),
      right: qualifyInsertConflictExpr(expr.right, tableName)
    )
  of ekUnary:
    Expr(kind: ekUnary, unOp: expr.unOp, expr: qualifyInsertConflictExpr(expr.expr, tableName))
  of ekFunc:
    var args: seq[Expr] = @[]
    for arg in expr.args:
      args.add(qualifyInsertConflictExpr(arg, tableName))
    Expr(kind: ekFunc, funcName: expr.funcName, args: args, isStar: expr.isStar)
  of ekParam:
    Expr(kind: ekParam, index: expr.index)
  of ekInList:
    var listExprs: seq[Expr] = @[]
    for item in expr.inList:
      listExprs.add(qualifyInsertConflictExpr(item, tableName))
    Expr(kind: ekInList, inExpr: qualifyInsertConflictExpr(expr.inExpr, tableName), inList: listExprs)
  of ekWindowRowNumber:
    var partitions: seq[Expr] = @[]
    for part in expr.windowPartitions:
      partitions.add(qualifyInsertConflictExpr(part, tableName))
    var orderExprs: seq[Expr] = @[]
    for o in expr.windowOrderExprs:
      orderExprs.add(qualifyInsertConflictExpr(o, tableName))
    Expr(
      kind: ekWindowRowNumber,
      windowPartitions: partitions,
      windowOrderExprs: orderExprs,
      windowOrderAsc: expr.windowOrderAsc
    )

proc cloneSelectItem(item: SelectItem): SelectItem =
  SelectItem(expr: cloneExpr(item.expr), alias: item.alias, isStar: item.isStar)

proc cloneJoin(join: JoinClause): JoinClause =
  JoinClause(joinType: join.joinType, table: join.table, alias: join.alias, onExpr: cloneExpr(join.onExpr))

proc cloneOrderItem(item: OrderItem): OrderItem =
  OrderItem(expr: cloneExpr(item.expr), asc: item.asc)

proc cloneSelectStatement(stmt: Statement): Statement

proc cloneSelectStatement(stmt: Statement): Statement =
  var cteQueries: seq[Statement] = @[]
  for query in stmt.cteQueries:
    if query != nil and query.kind == skSelect:
      cteQueries.add(cloneSelectStatement(query))
    else:
      cteQueries.add(query)
  var setLeft: Statement = nil
  if stmt.setOpLeft != nil and stmt.setOpLeft.kind == skSelect:
    setLeft = cloneSelectStatement(stmt.setOpLeft)
  else:
    setLeft = stmt.setOpLeft
  var setRight: Statement = nil
  if stmt.setOpRight != nil and stmt.setOpRight.kind == skSelect:
    setRight = cloneSelectStatement(stmt.setOpRight)
  else:
    setRight = stmt.setOpRight
  var selectItems: seq[SelectItem] = @[]
  for item in stmt.selectItems:
    selectItems.add(cloneSelectItem(item))
  var joins: seq[JoinClause] = @[]
  for join in stmt.joins:
    joins.add(cloneJoin(join))
  var groupBy: seq[Expr] = @[]
  for expr in stmt.groupBy:
    groupBy.add(cloneExpr(expr))
  var orderBy: seq[OrderItem] = @[]
  for item in stmt.orderBy:
    orderBy.add(cloneOrderItem(item))
  Statement(
    kind: skSelect,
    cteNames: stmt.cteNames,
    cteColumns: stmt.cteColumns,
    cteQueries: cteQueries,
    setOpKind: stmt.setOpKind,
    setOpLeft: setLeft,
    setOpRight: setRight,
    selectItems: selectItems,
    fromTable: stmt.fromTable,
    fromAlias: stmt.fromAlias,
    joins: joins,
    whereExpr: cloneExpr(stmt.whereExpr),
    groupBy: groupBy,
    havingExpr: cloneExpr(stmt.havingExpr),
    orderBy: orderBy,
    limit: stmt.limit,
    limitParam: stmt.limitParam,
    offset: stmt.offset,
    offsetParam: stmt.offsetParam
  )

proc countExprNodes(expr: Expr): int

proc countExprNodes(expr: Expr): int =
  if expr == nil:
    return 0
  result = 1
  case expr.kind
  of ekBinary:
    result += countExprNodes(expr.left)
    result += countExprNodes(expr.right)
  of ekUnary:
    result += countExprNodes(expr.expr)
  of ekFunc:
    for arg in expr.args:
      result += countExprNodes(arg)
  of ekInList:
    result += countExprNodes(expr.inExpr)
    for item in expr.inList:
      result += countExprNodes(item)
  of ekWindowRowNumber:
    for part in expr.windowPartitions:
      result += countExprNodes(part)
    for o in expr.windowOrderExprs:
      result += countExprNodes(o)
  else:
    discard

proc countSelectNodes(stmt: Statement): int =
  var nodes = 1
  for query in stmt.cteQueries:
    nodes += 1
    if query != nil and query.kind == skSelect:
      nodes += countSelectNodes(query)
  if stmt.setOpLeft != nil and stmt.setOpLeft.kind == skSelect:
    nodes += countSelectNodes(stmt.setOpLeft)
  if stmt.setOpRight != nil and stmt.setOpRight.kind == skSelect:
    nodes += countSelectNodes(stmt.setOpRight)
  nodes += stmt.selectItems.len
  for item in stmt.selectItems:
    if not item.isStar:
      nodes += countExprNodes(item.expr)
  nodes += stmt.joins.len
  for join in stmt.joins:
    nodes += countExprNodes(join.onExpr)
  nodes += countExprNodes(stmt.whereExpr)
  for expr in stmt.groupBy:
    nodes += countExprNodes(expr)
  nodes += countExprNodes(stmt.havingExpr)
  for item in stmt.orderBy:
    nodes += countExprNodes(item.expr)
  nodes

proc andExpr(left: Expr, right: Expr): Expr =
  if left == nil:
    return right
  if right == nil:
    return left
  Expr(kind: ekBinary, op: "AND", left: left, right: right)

proc hasParamsInExpr(expr: Expr): bool =
  if expr == nil:
    return false
  case expr.kind
  of ekParam:
    true
  of ekBinary:
    hasParamsInExpr(expr.left) or hasParamsInExpr(expr.right)
  of ekUnary:
    hasParamsInExpr(expr.expr)
  of ekFunc:
    for arg in expr.args:
      if hasParamsInExpr(arg):
        return true
    false
  of ekInList:
    if hasParamsInExpr(expr.inExpr):
      return true
    for item in expr.inList:
      if hasParamsInExpr(item):
        return true
    false
  of ekWindowRowNumber:
    for part in expr.windowPartitions:
      if hasParamsInExpr(part):
        return true
    for o in expr.windowOrderExprs:
      if hasParamsInExpr(o):
        return true
    false
  of ekLiteral:
    expr.value.kind == svParam
  else:
    false

proc hasWindowInExpr(expr: Expr): bool =
  if expr == nil:
    return false
  case expr.kind
  of ekWindowRowNumber:
    return true
  of ekBinary:
    return hasWindowInExpr(expr.left) or hasWindowInExpr(expr.right)
  of ekUnary:
    return hasWindowInExpr(expr.expr)
  of ekFunc:
    for arg in expr.args:
      if hasWindowInExpr(arg):
        return true
    return false
  of ekInList:
    if hasWindowInExpr(expr.inExpr):
      return true
    for item in expr.inList:
      if hasWindowInExpr(item):
        return true
    return false
  else:
    return false

proc hasParamsInSelect(stmt: Statement): bool =
  for query in stmt.cteQueries:
    if query != nil and query.kind == skSelect and hasParamsInSelect(query):
      return true
  if stmt.setOpLeft != nil and stmt.setOpLeft.kind == skSelect and hasParamsInSelect(stmt.setOpLeft):
    return true
  if stmt.setOpRight != nil and stmt.setOpRight.kind == skSelect and hasParamsInSelect(stmt.setOpRight):
    return true
  for item in stmt.selectItems:
    if not item.isStar and hasParamsInExpr(item.expr):
      return true
  if hasParamsInExpr(stmt.whereExpr):
    return true
  for join in stmt.joins:
    if hasParamsInExpr(join.onExpr):
      return true
  for expr in stmt.groupBy:
    if hasParamsInExpr(expr):
      return true
  if hasParamsInExpr(stmt.havingExpr):
    return true
  for item in stmt.orderBy:
    if hasParamsInExpr(item.expr):
      return true
  stmt.limitParam > 0 or stmt.offsetParam > 0

proc hasParamsInDmlStatement(stmt: Statement): bool =
  case stmt.kind
  of skInsert:
    for expr in stmt.insertValues:
      if hasParamsInExpr(expr):
        return true
    if hasParamsInExpr(stmt.insertConflictUpdateWhere):
      return true
    for _, expr in stmt.insertConflictUpdateAssignments:
      if hasParamsInExpr(expr):
        return true
    for item in stmt.insertReturning:
      if not item.isStar and hasParamsInExpr(item.expr):
        return true
    false
  of skUpdate:
    if hasParamsInExpr(stmt.updateWhere):
      return true
    for _, expr in stmt.assignments:
      if hasParamsInExpr(expr):
        return true
    false
  of skDelete:
    hasParamsInExpr(stmt.deleteWhere)
  else:
    false

proc buildTableMap(catalog: Catalog, fromTable: string, fromAlias: string, joins: seq[JoinClause]): Result[Table[string, TableMeta]] =
  var map = initTable[string, TableMeta]()
  if fromTable.len > 0:
    let baseRes = catalog.getTable(fromTable)
    if not baseRes.ok:
      return err[Table[string, TableMeta]](baseRes.err.code, baseRes.err.message, baseRes.err.context)
    map[fromTable] = baseRes.value
    if fromAlias.len > 0:
      map[fromAlias] = baseRes.value
  for join in joins:
    let tableRes = catalog.getTable(join.table)
    if not tableRes.ok:
      return err[Table[string, TableMeta]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
    map[join.table] = tableRes.value
    if join.alias.len > 0:
      map[join.alias] = tableRes.value
  ok(map)

proc resolveColumn(map: Table[string, TableMeta], table: string, name: string): Result[ColumnType] =
  if table.len > 0:
    if not map.hasKey(table):
      return err[ColumnType](ERR_SQL, "Unknown table", table)
    let meta = map[table]
    for col in meta.columns:
      if col.name == name:
        return ok(col.kind)
    return err[ColumnType](ERR_SQL, "Unknown column", table & "." & name)
  var found: seq[ColumnType] = @[]
  for _, meta in map:
    for col in meta.columns:
      if col.name == name:
        found.add(col.kind)
  if found.len == 0:
    return err[ColumnType](ERR_SQL, "Unknown column", name)
  if found.len > 1:
    return err[ColumnType](ERR_SQL, "Ambiguous column", name)
  ok(found[0])

proc bindExpr(map: Table[string, TableMeta], expr: Expr): Result[Void] =
  if expr == nil:
    return okVoid()
  case expr.kind
  of ekColumn:
    let res = resolveColumn(map, expr.table, expr.name)
    if not res.ok:
      return err[Void](res.err.code, res.err.message, res.err.context)
    okVoid()
  of ekBinary:
    let leftRes = bindExpr(map, expr.left)
    if not leftRes.ok:
      return leftRes
    let rightRes = bindExpr(map, expr.right)
    if not rightRes.ok:
      return rightRes
    okVoid()
  of ekUnary:
    bindExpr(map, expr.expr)
  of ekFunc:
    for arg in expr.args:
      let res = bindExpr(map, arg)
      if not res.ok:
        return res
    okVoid()
  of ekInList:
    let exprRes = bindExpr(map, expr.inExpr)
    if not exprRes.ok:
      return exprRes
    for item in expr.inList:
      let itemRes = bindExpr(map, item)
      if not itemRes.ok:
        return itemRes
    okVoid()
  of ekWindowRowNumber:
    for part in expr.windowPartitions:
      let partRes = bindExpr(map, part)
      if not partRes.ok:
        return partRes
    if expr.windowOrderExprs.len == 0:
      return err[Void](ERR_SQL, "ROW_NUMBER window requires ORDER BY in 0.x")
    if expr.windowOrderAsc.len != expr.windowOrderExprs.len:
      return err[Void](ERR_SQL, "ROW_NUMBER window ORDER BY metadata mismatch")
    for o in expr.windowOrderExprs:
      let orderRes = bindExpr(map, o)
      if not orderRes.ok:
        return orderRes
    okVoid()
  else:
    okVoid()

proc validateCheckExpr(expr: Expr): Result[Void] =
  if expr == nil:
    return err[Void](ERR_SQL, "CHECK expression cannot be empty")
  case expr.kind
  of ekParam:
    return err[Void](ERR_SQL, "CHECK expression cannot use parameters")
  of ekUnary:
    return validateCheckExpr(expr.expr)
  of ekBinary:
    let leftRes = validateCheckExpr(expr.left)
    if not leftRes.ok:
      return leftRes
    return validateCheckExpr(expr.right)
  of ekFunc:
    let fn = expr.funcName.toUpperAscii()
    if fn in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
      return err[Void](ERR_SQL, "CHECK expression cannot use aggregate functions", expr.funcName)
    if fn == "EXISTS":
      return err[Void](ERR_SQL, "CHECK expression cannot use EXISTS in 0.x")
    let allowed = ["CASE", "CAST", "COALESCE", "NULLIF", "LENGTH", "LOWER", "UPPER", "TRIM", "LIKE_ESCAPE"]
    if fn notin allowed:
      return err[Void](ERR_SQL, "Unsupported function in CHECK expression", expr.funcName)
    for arg in expr.args:
      let argRes = validateCheckExpr(arg)
      if not argRes.ok:
        return argRes
    return okVoid()
  of ekInList:
    let lhsRes = validateCheckExpr(expr.inExpr)
    if not lhsRes.ok:
      return lhsRes
    for item in expr.inList:
      let itemRes = validateCheckExpr(item)
      if not itemRes.ok:
        return itemRes
    return okVoid()
  of ekWindowRowNumber:
    return err[Void](ERR_SQL, "CHECK expression cannot use window functions")
  else:
    return okVoid()

proc sourceName(sourceTable: string, sourceAlias: string): string =
  if sourceAlias.len > 0: sourceAlias else: sourceTable

proc directSelectDependencies(stmt: Statement): seq[string] =
  var cteNames = initHashSet[string]()
  for name in stmt.cteNames:
    cteNames.incl(normalizedName(name))
  var seen = initHashSet[string]()
  if stmt.fromTable.len > 0 and normalizedName(stmt.fromTable) notin cteNames:
    seen.incl(normalizedName(stmt.fromTable))
  for join in stmt.joins:
    if normalizedName(join.table) notin cteNames:
      seen.incl(normalizedName(join.table))
  for query in stmt.cteQueries:
    if query != nil and query.kind == skSelect:
      for dep in directSelectDependencies(query):
        seen.incl(dep)
  if stmt.setOpLeft != nil and stmt.setOpLeft.kind == skSelect:
    for dep in directSelectDependencies(stmt.setOpLeft):
      seen.incl(dep)
  if stmt.setOpRight != nil and stmt.setOpRight.kind == skSelect:
    for dep in directSelectDependencies(stmt.setOpRight):
      seen.incl(dep)
  for name in seen:
    result.add(name)

proc collectTransitiveDependents(catalog: Catalog, objectName: string): seq[string] =
  let root = normalizedName(objectName)
  var visited = initHashSet[string]()
  var queue = catalog.listDependentViews(root)
  var i = 0
  while i < queue.len:
    let name = normalizedName(queue[i])
    i.inc
    if name in visited:
      continue
    visited.incl(name)
    result.add(name)
    for dep in catalog.listDependentViews(name):
      let normalized = normalizedName(dep)
      if normalized notin visited:
        queue.add(normalized)

proc viewDependsOn(catalog: Catalog, viewName: string, targetName: string, visiting: var HashSet[string]): bool =
  let current = normalizedName(viewName)
  let target = normalizedName(targetName)
  if current == target:
    return true
  if current in visiting:
    return false
  visiting.incl(current)
  let viewRes = catalog.getView(current)
  if not viewRes.ok:
    return false
  for dep in viewRes.value.dependencies:
    let depName = normalizedName(dep)
    if depName == target:
      return true
    if catalog.hasViewName(depName) and viewDependsOn(catalog, depName, target, visiting):
      return true
  false

proc parseViewSelect(view: ViewMeta): Result[Statement] =
  let parseRes = parseSql(view.sqlText)
  if not parseRes.ok:
    return err[Statement](parseRes.err.code, parseRes.err.message, view.name)
  if parseRes.value.statements.len != 1:
    return err[Statement](ERR_SQL, "View definition must contain exactly one SELECT", view.name)
  let stmt = parseRes.value.statements[0]
  if stmt.kind != skSelect:
    return err[Statement](ERR_SQL, "View definition must be SELECT", view.name)
  ok(stmt)

proc ensureExpandableViewShape(viewName: string, stmt: Statement): Result[Void] =
  if stmt.groupBy.len > 0 or stmt.havingExpr != nil:
    return err[Void](ERR_SQL, "Views with GROUP BY/HAVING are not supported in 0.x", viewName)
  if stmt.orderBy.len > 0:
    return err[Void](ERR_SQL, "Views with ORDER BY are not supported in 0.x", viewName)
  if stmt.limit >= 0 or stmt.limitParam > 0 or stmt.offset >= 0 or stmt.offsetParam > 0:
    return err[Void](ERR_SQL, "Views with LIMIT/OFFSET are not supported in 0.x", viewName)
  okVoid()

proc buildViewOutputExprs(catalog: Catalog, stmt: Statement): Result[seq[Expr]] =
  if stmt.setOpKind != sokNone:
    # For SetOps (UNION, INTERSECT, EXCEPT), the output columns are determined by the left-hand side.
    # The parser/binder ensures that right-hand side matches in count/type.
    return buildViewOutputExprs(catalog, stmt.setOpLeft)

  var outputExprs: seq[Expr] = @[]
  for item in stmt.selectItems:
    if item.isStar:
      if stmt.fromTable.len == 0:
        return err[seq[Expr]](ERR_SQL, "SELECT * requires FROM clause")
      let baseRes = catalog.getTable(stmt.fromTable)
      if not baseRes.ok:
        return err[seq[Expr]](baseRes.err.code, baseRes.err.message, baseRes.err.context)
      let basePrefix = sourceName(stmt.fromTable, stmt.fromAlias)
      # echo "DEBUG: buildViewOutputExprs basePrefix=", basePrefix, " table=", stmt.fromTable
      for col in baseRes.value.columns:
        outputExprs.add(Expr(kind: ekColumn, table: basePrefix, name: col.name))
      for join in stmt.joins:
        let tableRes = catalog.getTable(join.table)
        if not tableRes.ok:
          return err[seq[Expr]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
        let joinPrefix = sourceName(join.table, join.alias)
        # echo "DEBUG: buildViewOutputExprs joinPrefix=", joinPrefix, " table=", join.table
        for col in tableRes.value.columns:
          outputExprs.add(Expr(kind: ekColumn, table: joinPrefix, name: col.name))
    else:
      outputExprs.add(cloneExpr(item.expr))
  ok(outputExprs)

proc resolveViewOutputColumnNames(
  catalog: Catalog,
  viewName: string,
  explicitColumns: seq[string],
  expandedStmt: Statement
): Result[seq[string]] =
  let outputExprsRes = buildViewOutputExprs(catalog, expandedStmt)
  if not outputExprsRes.ok:
    return err[seq[string]](outputExprsRes.err.code, outputExprsRes.err.message, outputExprsRes.err.context)
  let outputExprs = outputExprsRes.value
  var names: seq[string] = @[]

  if explicitColumns.len > 0:
    if explicitColumns.len != outputExprs.len:
      let ctx = "view_name=" & viewName & ",expected_count=" & $explicitColumns.len & ",actual_count=" & $outputExprs.len
      return err[seq[string]](ERR_SQL, "View column count mismatch", ctx)
    names = explicitColumns
  else:
    names = newSeq[string](outputExprs.len)
    var sourceStmt = expandedStmt
    while sourceStmt.setOpKind != sokNone and sourceStmt.setOpLeft != nil:
      sourceStmt = sourceStmt.setOpLeft

    for i, expr in outputExprs:
      var name = ""
      if i < sourceStmt.selectItems.len:
        let item = sourceStmt.selectItems[i]
        if not item.isStar and item.alias.len > 0:
          name = item.alias
      if name.len == 0 and expr.kind == ekColumn:
        name = expr.name
      if name.len == 0:
        name = "col" & $(i + 1)
      names[i] = name

  var seen = initHashSet[string]()
  for name in names:
    let key = normalizedName(name)
    if key in seen:
      let ctx = "view_name=" & viewName & ",column_name=" & name
      return err[seq[string]](ERR_SQL, "Duplicate view output column", ctx)
    seen.incl(key)
  ok(names)

proc viewColumnExprMap(catalog: Catalog, expandedViewStmt: Statement, viewMeta: ViewMeta): Result[Table[string, Expr]] =
  let outputExprsRes = buildViewOutputExprs(catalog, expandedViewStmt)
  if not outputExprsRes.ok:
    return err[Table[string, Expr]](outputExprsRes.err.code, outputExprsRes.err.message, outputExprsRes.err.context)
  let outputExprs = outputExprsRes.value
  if outputExprs.len != viewMeta.columnNames.len:
    let ctx = "view_name=" & viewMeta.name & ",expected_count=" & $viewMeta.columnNames.len & ",actual_count=" & $outputExprs.len
    return err[Table[string, Expr]](ERR_SQL, "Stored view column shape mismatch", ctx)
  var map = initTable[string, Expr]()
  for i, name in viewMeta.columnNames:
    map[normalizedName(name)] = cloneExpr(outputExprs[i])
  ok(map)

proc rewriteExprForViewRef(
  expr: Expr,
  refTable: string,
  refAlias: string,
  columnMap: Table[string, Expr],
  allowUnqualified: bool
): Result[Expr] =
  if expr == nil:
    return ok(Expr(nil))
  case expr.kind
  of ekColumn:
    let tableMatches =
      (expr.table.len > 0) and
      (expr.table == refTable or (refAlias.len > 0 and expr.table == refAlias))
    let unqualifiedMatch = expr.table.len == 0 and allowUnqualified
    if tableMatches or unqualifiedMatch:
      let key = normalizedName(expr.name)
      if not columnMap.hasKey(key):
        return err[Expr](ERR_SQL, "Unknown view column", refTable & "." & expr.name)
      return ok(cloneExpr(columnMap[key]))
    return ok(cloneExpr(expr))
  of ekBinary:
    let leftRes = rewriteExprForViewRef(expr.left, refTable, refAlias, columnMap, allowUnqualified)
    if not leftRes.ok:
      return err[Expr](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    let rightRes = rewriteExprForViewRef(expr.right, refTable, refAlias, columnMap, allowUnqualified)
    if not rightRes.ok:
      return err[Expr](rightRes.err.code, rightRes.err.message, rightRes.err.context)
    ok(Expr(kind: ekBinary, op: expr.op, left: leftRes.value, right: rightRes.value))
  of ekUnary:
    let innerRes = rewriteExprForViewRef(expr.expr, refTable, refAlias, columnMap, allowUnqualified)
    if not innerRes.ok:
      return err[Expr](innerRes.err.code, innerRes.err.message, innerRes.err.context)
    ok(Expr(kind: ekUnary, unOp: expr.unOp, expr: innerRes.value))
  of ekFunc:
    var args: seq[Expr] = @[]
    for arg in expr.args:
      let argRes = rewriteExprForViewRef(arg, refTable, refAlias, columnMap, allowUnqualified)
      if not argRes.ok:
        return err[Expr](argRes.err.code, argRes.err.message, argRes.err.context)
      args.add(argRes.value)
    ok(Expr(kind: ekFunc, funcName: expr.funcName, args: args, isStar: expr.isStar))
  of ekInList:
    let inExprRes = rewriteExprForViewRef(expr.inExpr, refTable, refAlias, columnMap, allowUnqualified)
    if not inExprRes.ok:
      return err[Expr](inExprRes.err.code, inExprRes.err.message, inExprRes.err.context)
    var listExprs: seq[Expr] = @[]
    for item in expr.inList:
      let itemRes = rewriteExprForViewRef(item, refTable, refAlias, columnMap, allowUnqualified)
      if not itemRes.ok:
        return err[Expr](itemRes.err.code, itemRes.err.message, itemRes.err.context)
      listExprs.add(itemRes.value)
    ok(Expr(kind: ekInList, inExpr: inExprRes.value, inList: listExprs))
  of ekWindowRowNumber:
    var partitions: seq[Expr] = @[]
    for part in expr.windowPartitions:
      let partRes = rewriteExprForViewRef(part, refTable, refAlias, columnMap, allowUnqualified)
      if not partRes.ok:
        return err[Expr](partRes.err.code, partRes.err.message, partRes.err.context)
      partitions.add(partRes.value)
    var orderExprs: seq[Expr] = @[]
    for o in expr.windowOrderExprs:
      let orderRes = rewriteExprForViewRef(o, refTable, refAlias, columnMap, allowUnqualified)
      if not orderRes.ok:
        return err[Expr](orderRes.err.code, orderRes.err.message, orderRes.err.context)
      orderExprs.add(orderRes.value)
    ok(Expr(
      kind: ekWindowRowNumber,
      windowPartitions: partitions,
      windowOrderExprs: orderExprs,
      windowOrderAsc: expr.windowOrderAsc
    ))
  else:
    ok(cloneExpr(expr))

proc selectHasStar(stmt: Statement): bool =
  for item in stmt.selectItems:
    if item.isStar:
      return true
  false

proc expandSelectViews(
  catalog: Catalog,
  stmt: Statement,
  depth: int,
  stack: seq[string]
): Result[Statement]

type ExpandedCte = object
  query: Statement
  columnNames: seq[string]

proc outputExprMapForColumns(
  catalog: Catalog,
  expandedStmt: Statement,
  columnNames: seq[string],
  sourceName: string
): Result[Table[string, Expr]] =
  let outputExprsRes = buildViewOutputExprs(catalog, expandedStmt)
  if not outputExprsRes.ok:
    return err[Table[string, Expr]](outputExprsRes.err.code, outputExprsRes.err.message, outputExprsRes.err.context)
  let outputExprs = outputExprsRes.value
  if outputExprs.len != columnNames.len:
    let ctx = "cte_name=" & sourceName & ",expected_count=" & $columnNames.len & ",actual_count=" & $outputExprs.len
    return err[Table[string, Expr]](ERR_SQL, "CTE output column shape mismatch", ctx)
  var map = initTable[string, Expr]()
  for i, name in columnNames:
    map[normalizedName(name)] = cloneExpr(outputExprs[i])
  ok(map)

proc rewriteAllExpressions(
  target: var Statement,
  refTable: string,
  refAlias: string,
  columnMap: Table[string, Expr],
  allowUnqualified: bool,
  skipSelectItems: bool = false
): Result[Void] =
    var rewrittenItems: seq[SelectItem] = @[]
    if not skipSelectItems:
      for item in target.selectItems:
        if item.isStar:
          rewrittenItems.add(item)
          continue
        let exprRes = rewriteExprForViewRef(item.expr, refTable, refAlias, columnMap, allowUnqualified)
        if not exprRes.ok:
          return err[Void](exprRes.err.code, exprRes.err.message, exprRes.err.context)
        rewrittenItems.add(SelectItem(expr: exprRes.value, alias: item.alias, isStar: false))
      target.selectItems = rewrittenItems

    let whereRes = rewriteExprForViewRef(target.whereExpr, refTable, refAlias, columnMap, allowUnqualified)
    if not whereRes.ok:
      return err[Void](whereRes.err.code, whereRes.err.message, whereRes.err.context)
    target.whereExpr = whereRes.value

    var rewrittenJoins: seq[JoinClause] = @[]
    for join in target.joins:
      let onRes = rewriteExprForViewRef(join.onExpr, refTable, refAlias, columnMap, allowUnqualified)
      if not onRes.ok:
        return err[Void](onRes.err.code, onRes.err.message, onRes.err.context)
      rewrittenJoins.add(JoinClause(joinType: join.joinType, table: join.table, alias: join.alias, onExpr: onRes.value))
    target.joins = rewrittenJoins

    var rewrittenGroupBy: seq[Expr] = @[]
    for expr in target.groupBy:
      let exprRes = rewriteExprForViewRef(expr, refTable, refAlias, columnMap, allowUnqualified)
      if not exprRes.ok:
        return err[Void](exprRes.err.code, exprRes.err.message, exprRes.err.context)
      rewrittenGroupBy.add(exprRes.value)
    target.groupBy = rewrittenGroupBy

    let havingRes = rewriteExprForViewRef(target.havingExpr, refTable, refAlias, columnMap, allowUnqualified)
    if not havingRes.ok:
      return err[Void](havingRes.err.code, havingRes.err.message, havingRes.err.context)
    target.havingExpr = havingRes.value

    var rewrittenOrder: seq[OrderItem] = @[]
    for item in target.orderBy:
      let exprRes = rewriteExprForViewRef(item.expr, refTable, refAlias, columnMap, allowUnqualified)
      if not exprRes.ok:
        return err[Void](exprRes.err.code, exprRes.err.message, exprRes.err.context)
      rewrittenOrder.add(OrderItem(expr: exprRes.value, asc: item.asc))
    target.orderBy = rewrittenOrder
    okVoid()

proc pushDownQuery(
  catalog: Catalog,
  outer: Statement,
  inner: Statement,
  refTable: string,
  refAlias: string,
  columns: seq[string],
  allowUnqualified: bool,
  skipSelectItemsRewrite: bool = false
): Result[Statement] =
  if inner.setOpKind != sokNone:
    let leftRes = pushDownQuery(catalog, outer, inner.setOpLeft, refTable, refAlias, columns, allowUnqualified, skipSelectItemsRewrite)
    if not leftRes.ok: return leftRes
    let rightRes = pushDownQuery(catalog, outer, inner.setOpRight, refTable, refAlias, columns, allowUnqualified, skipSelectItemsRewrite)
    if not rightRes.ok: return rightRes
    return ok(Statement(
      kind: skSelect,
      setOpKind: inner.setOpKind,
      setOpLeft: leftRes.value,
      setOpRight: rightRes.value,
      orderBy: @[],
      limit: -1,
      limitParam: 0,
      offset: -1,
      offsetParam: 0
    ))
  else:
    let mapRes = outputExprMapForColumns(catalog, inner, columns, refTable)
    if not mapRes.ok:
       return err[Statement](mapRes.err.code, mapRes.err.message, mapRes.err.context)
    let columnMap = mapRes.value

    var res = cloneSelectStatement(outer)
    res.orderBy = @[]
    res.limit = -1
    res.limitParam = 0
    res.offset = -1
    res.offsetParam = 0
    
    let rewriteRes = rewriteAllExpressions(res, refTable, refAlias, columnMap, allowUnqualified, skipSelectItemsRewrite)
    if not rewriteRes.ok:
      return err[Statement](rewriteRes.err.code, rewriteRes.err.message, rewriteRes.err.context)
    
    let innerClone = cloneSelectStatement(inner)
    res.fromTable = innerClone.fromTable
    res.fromAlias = innerClone.fromAlias
    res.joins = innerClone.joins & res.joins
    res.whereExpr = andExpr(cloneExpr(innerClone.whereExpr), res.whereExpr)
    
    ok(res)

proc expandSelectCteRefs(
  catalog: Catalog,
  stmt: Statement,
  ctes: Table[string, ExpandedCte]
): Result[Statement] =
  var expanded = cloneSelectStatement(stmt)
  expanded.cteNames = @[]
  expanded.cteColumns = @[]
  expanded.cteQueries = @[]

  if expanded.fromTable.len > 0 and ctes.hasKey(normalizedName(expanded.fromTable)):
    let onlySource = expanded.joins.len == 0
    let singleSourceStar = selectHasStar(expanded)
    if singleSourceStar and not onlySource:
      return err[Statement](ERR_SQL, "SELECT * with joined CTEs is not supported in 0.x")

    let refTable = expanded.fromTable
    let refAlias = expanded.fromAlias
    let cteDef = ctes[normalizedName(refTable)]

    var skipRewrite = false
    if singleSourceStar and cteDef.query.setOpKind == sokNone:
      # Optimization for single source star on simple query: replace star with explicit columns
      let columnMapRes = outputExprMapForColumns(catalog, cteDef.query, cteDef.columnNames, refTable)
      if not columnMapRes.ok:
        return err[Statement](columnMapRes.err.code, columnMapRes.err.message, columnMapRes.err.context)
      let columnMap = columnMapRes.value
      var items: seq[SelectItem] = @[]
      for item in expanded.selectItems:
        if item.isStar:
          for colName in cteDef.columnNames:
            let key = normalizedName(colName)
            if columnMap.hasKey(key):
              items.add(SelectItem(expr: cloneExpr(columnMap[key]), alias: colName, isStar: false))
        else:
          let exprRes = rewriteExprForViewRef(item.expr, refTable, refAlias, columnMap, onlySource)
          if not exprRes.ok:
             return err[Statement](exprRes.err.code, exprRes.err.message, exprRes.err.context)
          items.add(SelectItem(expr: exprRes.value, alias: item.alias, isStar: false))
      expanded.selectItems = items
      skipRewrite = true

    let originalOrderBy = expanded.orderBy
    let originalLimit = expanded.limit
    let originalLimitParam = expanded.limitParam
    let originalOffset = expanded.offset
    let originalOffsetParam = expanded.offsetParam
    
    let pushedRes = pushDownQuery(catalog, expanded, cteDef.query, refTable, refAlias, cteDef.columnNames, onlySource, skipRewrite)
    if not pushedRes.ok:
      return err[Statement](pushedRes.err.code, pushedRes.err.message, pushedRes.err.context)
    expanded = pushedRes.value
    
    if expanded.setOpKind != sokNone:
        # If it became a SetOp, restore the outer query's ordering/limit
        expanded.orderBy = originalOrderBy
        expanded.limit = originalLimit
        expanded.limitParam = originalLimitParam
        expanded.offset = originalOffset
        expanded.offsetParam = originalOffsetParam
    else:
        # If it is still a Select, restore ordering/limit
        expanded.orderBy = originalOrderBy
        expanded.limit = originalLimit
        expanded.limitParam = originalLimitParam
        expanded.offset = originalOffset
        expanded.offsetParam = originalOffsetParam

  var joinIdx = 0
  while joinIdx < expanded.joins.len:
    if ctes.hasKey(normalizedName(expanded.joins[joinIdx].table)):
      if selectHasStar(expanded):
        return err[Statement](ERR_SQL, "SELECT * with joined CTEs is not supported in 0.x")

      let refTable = expanded.joins[joinIdx].table
      let refAlias = expanded.joins[joinIdx].alias
      let cteDef = ctes[normalizedName(refTable)]
      
      if cteDef.query.setOpKind != sokNone:
         return err[Statement](ERR_SQL, "Joining with SetOp CTE not supported in 0.x")

      let columnMapRes = outputExprMapForColumns(catalog, cteDef.query, cteDef.columnNames, refTable)
      if not columnMapRes.ok:
        return err[Statement](columnMapRes.err.code, columnMapRes.err.message, columnMapRes.err.context)
      let columnMap = columnMapRes.value

      let rewriteRes = rewriteAllExpressions(expanded, refTable, refAlias, columnMap, false)
      if not rewriteRes.ok:
        return err[Statement](rewriteRes.err.code, rewriteRes.err.message, rewriteRes.err.context)

      let cteQuery = cloneSelectStatement(cteDef.query)
      var baseJoin = expanded.joins[joinIdx]
      baseJoin.table = cteQuery.fromTable
      baseJoin.alias = cteQuery.fromAlias
      baseJoin.onExpr = andExpr(baseJoin.onExpr, cloneExpr(cteQuery.whereExpr))

      var newJoins: seq[JoinClause] = @[]
      for i, join in expanded.joins:
        if i == joinIdx:
          newJoins.add(baseJoin)
          for extra in cteQuery.joins:
            newJoins.add(extra)
        else:
          newJoins.add(join)
      expanded.joins = newJoins
    joinIdx.inc


  let nodeCount = countSelectNodes(expanded)
  if nodeCount > MaxExpandedAstNodes:
    return err[Statement](ERR_SQL, "Expanded AST node budget exceeded", "max_nodes=" & $MaxExpandedAstNodes)
  ok(expanded)

proc expandSelectCtes(
  catalog: Catalog,
  stmt: Statement,
  depth: int,
  stack: seq[string]
): Result[Statement] =
  if depth > MaxViewExpansionDepth:
    return err[Statement](ERR_SQL, "CTE expansion depth exceeded", "max_depth=" & $MaxViewExpansionDepth)

  var expandedCtes = initTable[string, ExpandedCte]()
  for i, cteName in stmt.cteNames:
    if i >= stmt.cteQueries.len:
      return err[Statement](ERR_SQL, "CTE query must be SELECT", cteName)
    let cteQuery = stmt.cteQueries[i]
    if cteQuery == nil or cteQuery.kind != skSelect:
      return err[Statement](ERR_SQL, "CTE query must be SELECT", cteName)
    let key = normalizedName(cteName)
    if expandedCtes.hasKey(key):
      return err[Statement](ERR_SQL, "Duplicate CTE name", cteName)
    for existing in stack:
      if normalizedName(existing) == key:
        return err[Statement](ERR_SQL, "Circular CTE reference", cteName)

    let nestedRes = expandSelectCtes(catalog, cteQuery, depth + 1, stack & @[cteName])
    if not nestedRes.ok:
      return err[Statement](nestedRes.err.code, nestedRes.err.message, nestedRes.err.context)
    let inlineRes = expandSelectCteRefs(catalog, nestedRes.value, expandedCtes)
    if not inlineRes.ok:
      return err[Statement](inlineRes.err.code, inlineRes.err.message, inlineRes.err.context)
    let viewRes = expandSelectViews(catalog, inlineRes.value, depth + 1, stack & @[cteName])
    if not viewRes.ok:
      return err[Statement](viewRes.err.code, viewRes.err.message, viewRes.err.context)
    let shapeRes = ensureExpandableViewShape(cteName, viewRes.value)
    if not shapeRes.ok:
      return err[Statement](shapeRes.err.code, shapeRes.err.message, shapeRes.err.context)
    let cols = if i < stmt.cteColumns.len: stmt.cteColumns[i] else: @[]
    let colRes = resolveViewOutputColumnNames(catalog, cteName, cols, viewRes.value)
    if not colRes.ok:
      return err[Statement](colRes.err.code, colRes.err.message, colRes.err.context)

    expandedCtes[key] = ExpandedCte(query: viewRes.value, columnNames: colRes.value)
  var outer = cloneSelectStatement(stmt)
  outer.cteNames = @[]
  outer.cteColumns = @[]
  outer.cteQueries = @[]
  let outerInlineRes = expandSelectCteRefs(catalog, outer, expandedCtes)
  if not outerInlineRes.ok:
    return err[Statement](outerInlineRes.err.code, outerInlineRes.err.message, outerInlineRes.err.context)
  ok(outerInlineRes.value)

proc expandSelectSources(
  catalog: Catalog,
  stmt: Statement,
  depth: int,
  stack: seq[string]
): Result[Statement] =
  let cteRes = expandSelectCtes(catalog, stmt, depth, stack)
  if not cteRes.ok:
    return err[Statement](cteRes.err.code, cteRes.err.message, cteRes.err.context)
  let viewRes = expandSelectViews(catalog, cteRes.value, depth, stack)
  if not viewRes.ok:
    return err[Statement](viewRes.err.code, viewRes.err.message, viewRes.err.context)
  ok(viewRes.value)

proc expandSelectViews(
  catalog: Catalog,
  stmt: Statement,
  depth: int,
  stack: seq[string]
): Result[Statement] =
  if depth > MaxViewExpansionDepth:
    return err[Statement](ERR_SQL, "View expansion depth exceeded", "max_depth=" & $MaxViewExpansionDepth)

  var expanded = cloneSelectStatement(stmt)

  proc expandSingleSource(refTable: string, refAlias: string, allowUnqualified: bool): Result[(ViewMeta, Statement)] =
    let viewRes = catalog.getView(refTable)
    if not viewRes.ok:
      return err[(ViewMeta, Statement)](viewRes.err.code, viewRes.err.message, viewRes.err.context)
    let normalized = normalizedName(refTable)
    for existing in stack:
      if normalizedName(existing) == normalized:
        return err[(ViewMeta, Statement)](ERR_SQL, "Circular view reference", refTable)
    let parsedRes = parseViewSelect(viewRes.value)
    if not parsedRes.ok:
      return err[(ViewMeta, Statement)](parsedRes.err.code, parsedRes.err.message, parsedRes.err.context)
    let shapeRes = ensureExpandableViewShape(viewRes.value.name, parsedRes.value)
    if not shapeRes.ok:
      return err[(ViewMeta, Statement)](shapeRes.err.code, shapeRes.err.message, shapeRes.err.context)
    let nextRes = expandSelectSources(catalog, parsedRes.value, depth + 1, stack & @[refTable])
    if not nextRes.ok:
      return err[(ViewMeta, Statement)](nextRes.err.code, nextRes.err.message, nextRes.err.context)
    ok((viewRes.value, nextRes.value))

  if expanded.fromTable.len > 0 and catalog.hasViewName(expanded.fromTable):
    let onlySource = expanded.joins.len == 0
    let singleSourceStar = selectHasStar(expanded)
    if singleSourceStar and not onlySource:
      return err[Statement](ERR_SQL, "SELECT * with joined views is not supported in 0.x")

    let sourceRes = expandSingleSource(expanded.fromTable, expanded.fromAlias, onlySource)
    if not sourceRes.ok:
      return err[Statement](sourceRes.err.code, sourceRes.err.message, sourceRes.err.context)

    let viewMeta = sourceRes.value[0]
    let viewExpanded = sourceRes.value[1]
    let refTable = expanded.fromTable
    let refAlias = expanded.fromAlias

    var skipRewrite = false
    if singleSourceStar and viewExpanded.setOpKind == sokNone:
      # Optimization: expand star using the view's output columns (from Left if SetOp? No, only if None for now)
      # Actually, for SetOp, we can also expand star if we trust viewMeta.columnNames
      # But let's stick to safe path.
      let columnMapRes = outputExprMapForColumns(catalog, viewExpanded, viewMeta.columnNames, refTable)
      if not columnMapRes.ok:
         return err[Statement](columnMapRes.err.code, columnMapRes.err.message, columnMapRes.err.context)
      let columnMap = columnMapRes.value
      
      var items: seq[SelectItem] = @[]
      for item in expanded.selectItems:
        if item.isStar:
          for colName in viewMeta.columnNames:
            let key = normalizedName(colName)
            if columnMap.hasKey(key):
              items.add(SelectItem(expr: cloneExpr(columnMap[key]), alias: colName, isStar: false))
        else:
          let exprRes = rewriteExprForViewRef(item.expr, refTable, refAlias, columnMap, onlySource)
          if not exprRes.ok:
             return err[Statement](exprRes.err.code, exprRes.err.message, exprRes.err.context)
          items.add(SelectItem(expr: exprRes.value, alias: item.alias, isStar: false))
      expanded.selectItems = items
      skipRewrite = true

    let originalOrderBy = expanded.orderBy
    let originalLimit = expanded.limit
    let originalLimitParam = expanded.limitParam
    let originalOffset = expanded.offset
    let originalOffsetParam = expanded.offsetParam
    
    let pushedRes = pushDownQuery(catalog, expanded, viewExpanded, refTable, refAlias, viewMeta.columnNames, onlySource, skipRewrite)
    if not pushedRes.ok:
      return err[Statement](pushedRes.err.code, pushedRes.err.message, pushedRes.err.context)
    expanded = pushedRes.value
    
    if expanded.setOpKind != sokNone:
        expanded.orderBy = originalOrderBy
        expanded.limit = originalLimit
        expanded.limitParam = originalLimitParam
        expanded.offset = originalOffset
        expanded.offsetParam = originalOffsetParam
    else:
        expanded.orderBy = originalOrderBy
        expanded.limit = originalLimit
        expanded.limitParam = originalLimitParam
        expanded.offset = originalOffset
        expanded.offsetParam = originalOffsetParam

  var joinIdx = 0
  while joinIdx < expanded.joins.len:
    if catalog.hasViewName(expanded.joins[joinIdx].table):
      if selectHasStar(expanded):
        return err[Statement](ERR_SQL, "SELECT * with joined views is not supported in 0.x")

      let refTable = expanded.joins[joinIdx].table
      let refAlias = expanded.joins[joinIdx].alias
      let sourceRes = expandSingleSource(refTable, refAlias, false)
      if not sourceRes.ok:
        return err[Statement](sourceRes.err.code, sourceRes.err.message, sourceRes.err.context)
      let viewExpanded = sourceRes.value[1]
      let viewMeta = sourceRes.value[0]

      if viewExpanded.setOpKind != sokNone:
          return err[Statement](ERR_SQL, "Joining with SetOp view not supported in 0.x")

      let columnMapRes = outputExprMapForColumns(catalog, viewExpanded, viewMeta.columnNames, refTable)
      if not columnMapRes.ok:
         return err[Statement](columnMapRes.err.code, columnMapRes.err.message, columnMapRes.err.context)
      let columnMap = columnMapRes.value

      let rewriteRes = rewriteAllExpressions(expanded, refTable, refAlias, columnMap, false)
      if not rewriteRes.ok:
        return err[Statement](rewriteRes.err.code, rewriteRes.err.message, rewriteRes.err.context)

      let viewQuery = cloneSelectStatement(viewExpanded)
      var baseJoin = expanded.joins[joinIdx]
      baseJoin.table = viewQuery.fromTable
      baseJoin.alias = viewQuery.fromAlias
      baseJoin.onExpr = andExpr(baseJoin.onExpr, cloneExpr(viewQuery.whereExpr))

      var newJoins: seq[JoinClause] = @[]
      for i, join in expanded.joins:
        if i == joinIdx:
          newJoins.add(baseJoin)
          for extra in viewQuery.joins:
            newJoins.add(extra)
        else:
          newJoins.add(join)
      expanded.joins = newJoins
    joinIdx.inc

  let nodeCount = countSelectNodes(expanded)
  if nodeCount > MaxExpandedAstNodes:
    return err[Statement](ERR_SQL, "Expanded AST node budget exceeded", "max_nodes=" & $MaxExpandedAstNodes)

  ok(expanded)

proc bindSelect(catalog: Catalog, stmt: Statement): Result[Statement] =
  if stmt.setOpKind != sokNone:
    if stmt.cteNames.len > 0:
      return err[Statement](ERR_SQL, "WITH with set operations is not supported in 0.x")
    if stmt.setOpKind notin {sokUnionAll, sokUnion, sokIntersect, sokExcept}:
      return err[Statement](ERR_SQL, "Set operation not supported in 0.x")
    if stmt.setOpLeft == nil or stmt.setOpRight == nil:
      return err[Statement](ERR_SQL, "Set operation requires both sides")
    let leftRes = bindStatement(catalog, stmt.setOpLeft)
    if not leftRes.ok:
      return err[Statement](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    let rightRes = bindStatement(catalog, stmt.setOpRight)
    if not rightRes.ok:
      return err[Statement](rightRes.err.code, rightRes.err.message, rightRes.err.context)
    if leftRes.value.kind != skSelect or rightRes.value.kind != skSelect:
      return err[Statement](ERR_SQL, "Set operation requires SELECT operands")
    return ok(Statement(
      kind: skSelect,
      cteNames: @[],
      cteColumns: @[],
      cteQueries: @[],
      setOpKind: stmt.setOpKind,
      setOpLeft: leftRes.value,
      setOpRight: rightRes.value,
      selectItems: @[],
      fromTable: "",
      fromAlias: "",
      joins: @[],
      whereExpr: nil,
      groupBy: @[],
      havingExpr: nil,
      orderBy: stmt.orderBy,
      limit: stmt.limit,
      limitParam: stmt.limitParam,
      offset: stmt.offset,
      offsetParam: stmt.offsetParam
    ))

  let expandedRes = expandSelectSources(catalog, stmt, 0, @[])
  if not expandedRes.ok:
    return err[Statement](expandedRes.err.code, expandedRes.err.message, expandedRes.err.context)
  let expanded = expandedRes.value

  # If expansion resulted in a SetOp (e.g. from UNION View/CTE), we need to bind that.
  if expanded.setOpKind != sokNone:
    return bindSelect(catalog, expanded)

  let mapRes = buildTableMap(catalog, expanded.fromTable, expanded.fromAlias, expanded.joins)
  if not mapRes.ok:
    return err[Statement](mapRes.err.code, mapRes.err.message, mapRes.err.context)
  let map = mapRes.value
  for item in expanded.selectItems:
    if item.isStar:
      continue
    if hasWindowInExpr(item.expr):
      if item.expr.kind != ekWindowRowNumber:
        return err[Statement](ERR_SQL, "Only top-level ROW_NUMBER window expressions are supported in 0.x")
    let res = bindExpr(map, item.expr)
    if not res.ok:
      return err[Statement](res.err.code, res.err.message, res.err.context)
  if hasWindowInExpr(expanded.whereExpr):
    return err[Statement](ERR_SQL, "Window functions are not allowed in WHERE in 0.x")
  let whereRes = bindExpr(map, expanded.whereExpr)
  if not whereRes.ok:
    return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)
  for join in expanded.joins:
    if hasWindowInExpr(join.onExpr):
      return err[Statement](ERR_SQL, "Window functions are not allowed in JOIN predicates in 0.x")
    let onRes = bindExpr(map, join.onExpr)
    if not onRes.ok:
      return err[Statement](onRes.err.code, onRes.err.message, onRes.err.context)
  for expr in expanded.groupBy:
    if hasWindowInExpr(expr):
      return err[Statement](ERR_SQL, "Window functions are not allowed in GROUP BY in 0.x")
    let res = bindExpr(map, expr)
    if not res.ok:
      return err[Statement](res.err.code, res.err.message, res.err.context)
  if hasWindowInExpr(expanded.havingExpr):
    return err[Statement](ERR_SQL, "Window functions are not allowed in HAVING in 0.x")
  let havingRes = bindExpr(map, expanded.havingExpr)
  if not havingRes.ok:
    return err[Statement](havingRes.err.code, havingRes.err.message, havingRes.err.context)
  for item in expanded.orderBy:
    if hasWindowInExpr(item.expr):
      return err[Statement](ERR_SQL, "Window functions are not allowed in ORDER BY in 0.x")
    let res = bindExpr(map, item.expr)
    if not res.ok:
      return err[Statement](res.err.code, res.err.message, res.err.context)
  ok(expanded)

proc bindInsert(catalog: Catalog, stmt: Statement): Result[Statement] =
  if catalog.hasViewName(stmt.insertTable):
    if stmt.insertConflictAction != icaNone:
      return err[Statement](ERR_SQL, "INSERT ... ON CONFLICT is not supported for views in 0.x", stmt.insertTable)
    if stmt.insertReturning.len > 0:
      return err[Statement](ERR_SQL, "INSERT ... RETURNING is not supported for views in 0.x", stmt.insertTable)
    let triggers = catalog.listTriggersForTable(stmt.insertTable, TriggerEventInsertMask)
    var hasInstead = false
    for trigger in triggers:
      if (trigger.eventsMask and TriggerTimingInsteadMask) != 0:
        hasInstead = true
        break
    if not hasInstead:
      return err[Statement](ERR_SQL, "View is read-only", stmt.insertTable)
    let viewRes = catalog.getView(stmt.insertTable)
    if not viewRes.ok:
      return err[Statement](viewRes.err.code, viewRes.err.message, viewRes.err.context)
    let targetCols = if stmt.insertColumns.len == 0: viewRes.value.columnNames else: stmt.insertColumns
    if stmt.insertValues.len != targetCols.len:
      return err[Statement](ERR_SQL, "Column count mismatch", stmt.insertTable)
    var colSet = initHashSet[string]()
    for col in viewRes.value.columnNames:
      colSet.incl(col)
    for colName in targetCols:
      if not colSet.contains(colName):
        return err[Statement](ERR_SQL, "Unknown column", colName)
    return ok(stmt)
  let tableRes = catalog.getTable(stmt.insertTable)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  let targetCols = if stmt.insertColumns.len == 0:
    table.columns.mapIt(it.name)
  else:
    stmt.insertColumns
  if stmt.insertValues.len != targetCols.len:
    return err[Statement](ERR_SQL, "Column count mismatch", stmt.insertTable)
  var colIndex = initTable[string, int]()
  for i, col in table.columns:
    colIndex[col.name] = i
  var ordered: seq[Expr] = newSeq[Expr](table.columns.len)
  for i in 0 ..< ordered.len:
    ordered[i] = nullExpr()
  for i, colName in targetCols:
    if not colIndex.hasKey(colName):
      return err[Statement](ERR_SQL, "Unknown column", colName)
    let idx = colIndex[colName]
    let expr = stmt.insertValues[i]
    let typeRes = checkLiteralType(expr, table.columns[idx].kind, colName)
    if not typeRes.ok:
      return err[Statement](typeRes.err.code, typeRes.err.message, typeRes.err.context)
    ordered[idx] = expr

  var conflictTargetCols = stmt.insertConflictTargetCols
  var conflictTargetConstraint = stmt.insertConflictTargetConstraint
  var conflictAssignments = initTable[string, Expr]()
  var conflictWhere: Expr = nil
  var returningItems: seq[SelectItem] = @[]
  if stmt.insertConflictAction != icaNone:
    if conflictTargetCols.len > 0 and conflictTargetConstraint.len > 0:
      return err[Statement](ERR_SQL, "ON CONFLICT cannot specify both column target and constraint target")

    if conflictTargetConstraint.len > 0:
      if not catalog.indexes.hasKey(conflictTargetConstraint):
        return err[Statement](ERR_SQL, "Unknown ON CONFLICT constraint", conflictTargetConstraint)
      let idx = catalog.indexes[conflictTargetConstraint]
      if idx.table != stmt.insertTable:
        return err[Statement](ERR_SQL, "ON CONFLICT constraint belongs to different table", conflictTargetConstraint)
      if not idx.unique:
        return err[Statement](ERR_SQL, "ON CONFLICT constraint must be unique", conflictTargetConstraint)
      conflictTargetCols = idx.columns

    if conflictTargetCols.len > 0:
      var colSet = initHashSet[string]()
      for col in table.columns:
        colSet.incl(col.name)
      for name in conflictTargetCols:
        if not colSet.contains(name):
          return err[Statement](ERR_SQL, "Unknown ON CONFLICT target column", name)

      var matchedUniqueTarget = false
      if conflictTargetCols.len == 1:
        for col in table.columns:
          if col.name == conflictTargetCols[0] and (col.primaryKey or col.unique):
            matchedUniqueTarget = true
            break
      if not matchedUniqueTarget:
        for _, idx in catalog.indexes:
          if idx.table == stmt.insertTable and idx.unique and idx.columns == conflictTargetCols:
            matchedUniqueTarget = true
            break
      if not matchedUniqueTarget:
        return err[Statement](
          ERR_SQL,
          "ON CONFLICT target does not match a unique constraint",
          stmt.insertTable & "." & conflictTargetCols.join(",")
        )

  if stmt.insertConflictAction == icaDoUpdate:
    if conflictTargetCols.len == 0:
      return err[Statement](ERR_SQL, "ON CONFLICT DO UPDATE requires conflict target")
    if stmt.insertConflictUpdateAssignments.len == 0:
      return err[Statement](ERR_SQL, "ON CONFLICT DO UPDATE requires at least one assignment")

    let mapRes = buildTableMap(catalog, stmt.insertTable, "", @[])
    if not mapRes.ok:
      return err[Statement](mapRes.err.code, mapRes.err.message, mapRes.err.context)
    var exprMap = mapRes.value
    exprMap["excluded"] = table

    for colName, expr in stmt.insertConflictUpdateAssignments:
      var colIdx = -1
      for i, col in table.columns:
        if col.name == colName:
          colIdx = i
          break
      if colIdx < 0:
        return err[Statement](ERR_SQL, "Unknown column", colName)
      let qualifiedExpr = qualifyInsertConflictExpr(expr, stmt.insertTable)
      let bindRes = bindExpr(exprMap, qualifiedExpr)
      if not bindRes.ok:
        return err[Statement](bindRes.err.code, bindRes.err.message, bindRes.err.context)
      let typeRes = checkLiteralType(qualifiedExpr, table.columns[colIdx].kind, colName)
      if not typeRes.ok:
        return err[Statement](typeRes.err.code, typeRes.err.message, typeRes.err.context)
      conflictAssignments[colName] = qualifiedExpr

    conflictWhere = qualifyInsertConflictExpr(stmt.insertConflictUpdateWhere, stmt.insertTable)
    let whereRes = bindExpr(exprMap, conflictWhere)
    if not whereRes.ok:
      return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)

  let returningMapRes = buildTableMap(catalog, stmt.insertTable, "", @[])
  if not returningMapRes.ok:
    return err[Statement](returningMapRes.err.code, returningMapRes.err.message, returningMapRes.err.context)
  for item in stmt.insertReturning:
    if item.isStar:
      returningItems.add(item)
      continue
    let bindRes = bindExpr(returningMapRes.value, item.expr)
    if not bindRes.ok:
      return err[Statement](bindRes.err.code, bindRes.err.message, bindRes.err.context)
    returningItems.add(item)

  ok(Statement(
    kind: skInsert,
    insertTable: stmt.insertTable,
    insertColumns: @[],
    insertValues: ordered,
    insertConflictAction: stmt.insertConflictAction,
    insertConflictTargetCols: conflictTargetCols,
    insertConflictTargetConstraint: conflictTargetConstraint,
    insertConflictUpdateAssignments: conflictAssignments,
    insertConflictUpdateWhere: conflictWhere,
    insertReturning: returningItems
  ))

proc bindUpdate(catalog: Catalog, stmt: Statement): Result[Statement] =
  if catalog.hasViewName(stmt.updateTable):
    let triggers = catalog.listTriggersForTable(stmt.updateTable, TriggerEventUpdateMask)
    var hasInstead = false
    for trigger in triggers:
      if (trigger.eventsMask and TriggerTimingInsteadMask) != 0:
        hasInstead = true
        break
    if not hasInstead:
      return err[Statement](ERR_SQL, "View is read-only", stmt.updateTable)
    let viewRes = catalog.getView(stmt.updateTable)
    if not viewRes.ok:
      return err[Statement](viewRes.err.code, viewRes.err.message, viewRes.err.context)
    var colSet = initHashSet[string]()
    for col in viewRes.value.columnNames:
      colSet.incl(col)
    for colName, _ in stmt.assignments:
      if not colSet.contains(colName):
        return err[Statement](ERR_SQL, "Unknown column", colName)
    return ok(stmt)
  let tableRes = catalog.getTable(stmt.updateTable)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  for colName, expr in stmt.assignments:
    var found = false
    for col in table.columns:
      if col.name == colName:
        found = true
        let typeRes = checkLiteralType(expr, col.kind, colName)
        if not typeRes.ok:
          return err[Statement](typeRes.err.code, typeRes.err.message, typeRes.err.context)
        break
    if not found:
      return err[Statement](ERR_SQL, "Unknown column", colName)
  let mapRes = buildTableMap(catalog, stmt.updateTable, "", @[])
  if not mapRes.ok:
    return err[Statement](mapRes.err.code, mapRes.err.message, mapRes.err.context)
  let whereRes = bindExpr(mapRes.value, stmt.updateWhere)
  if not whereRes.ok:
    return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)
  ok(stmt)

proc bindDelete(catalog: Catalog, stmt: Statement): Result[Statement] =
  if catalog.hasViewName(stmt.deleteTable):
    let triggers = catalog.listTriggersForTable(stmt.deleteTable, TriggerEventDeleteMask)
    var hasInstead = false
    for trigger in triggers:
      if (trigger.eventsMask and TriggerTimingInsteadMask) != 0:
        hasInstead = true
        break
    if not hasInstead:
      return err[Statement](ERR_SQL, "View is read-only", stmt.deleteTable)
    return ok(stmt)
  let mapRes = buildTableMap(catalog, stmt.deleteTable, "", @[])
  if not mapRes.ok:
    return err[Statement](mapRes.err.code, mapRes.err.message, mapRes.err.context)
  let whereRes = bindExpr(mapRes.value, stmt.deleteWhere)
  if not whereRes.ok:
    return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)
  ok(stmt)

proc bindCreateTable(catalog: Catalog, stmt: Statement): Result[Statement] =
  if catalog.hasTableOrViewName(stmt.createTableName):
    return err[Statement](ERR_SQL, "Object already exists", stmt.createTableName)

  var primaryCount = 0
  var tableColumns: seq[Column] = @[]
  for col in stmt.columns:
    let typeRes = parseColumnType(col.typeName)
    if not typeRes.ok:
      return err[Statement](typeRes.err.code, typeRes.err.message, col.typeName)
    let spec = typeRes.value
    tableColumns.add(Column(
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
    # ColumnDef typeName is preserved for catalog storage; actual Column.kind is computed later.
    if col.primaryKey:
      primaryCount.inc
    if col.refTable.len > 0 and col.refColumn.len > 0:
      let onDelete = if col.refOnDelete.len > 0: col.refOnDelete else: "NO ACTION"
      let onUpdate = if col.refOnUpdate.len > 0: col.refOnUpdate else: "NO ACTION"
      if onDelete == "SET NULL" and col.notNull:
        return err[Statement](ERR_SQL, "ON DELETE SET NULL requires nullable child column", col.name)
      if onUpdate == "SET NULL" and col.notNull:
        return err[Statement](ERR_SQL, "ON UPDATE SET NULL requires nullable child column", col.name)
      let parentRes = catalog.getTable(col.refTable)
      if not parentRes.ok:
        return err[Statement](parentRes.err.code, parentRes.err.message, col.refTable)
      var parentHasColumn = false
      for parentCol in parentRes.value.columns:
        if parentCol.name == col.refColumn:
          parentHasColumn = true
          break
      if not parentHasColumn:
        return err[Statement](ERR_SQL, "Referenced column not found", col.refTable & "." & col.refColumn)
      let parentIdx = catalog.getIndexForColumn(col.refTable, col.refColumn, ikBtree, requireUnique = true)
      var isInt64Pk = false
      for parentCol in parentRes.value.columns:
        if parentCol.name == col.refColumn and parentCol.primaryKey and parentCol.kind == ctInt64:
          isInt64Pk = true
          break

      if not isInt64Pk:
        if isNone(parentIdx):
          return err[Statement](ERR_SQL, "Referenced column must be indexed uniquely", col.refTable & "." & col.refColumn)
  if primaryCount > 1:
    for col in stmt.columns:
      if col.primaryKey and not col.notNull:
        return err[Statement](ERR_SQL, "Composite primary key column must be NOT NULL", col.name)

  if stmt.createChecks.len > 0:
    var map = initTable[string, TableMeta]()
    map[stmt.createTableName] = TableMeta(
      name: stmt.createTableName,
      columns: tableColumns
    )
    for checkDef in stmt.createChecks:
      let validateRes = validateCheckExpr(checkDef.expr)
      if not validateRes.ok:
        return err[Statement](validateRes.err.code, validateRes.err.message, validateRes.err.context)
      let bindRes = bindExpr(map, checkDef.expr)
      if not bindRes.ok:
        return err[Statement](bindRes.err.code, bindRes.err.message, bindRes.err.context)
  ok(stmt)

proc bindCreateIndex(catalog: Catalog, stmt: Statement): Result[Statement] =
  let tableRes = catalog.getTable(stmt.indexTableName)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  if stmt.columnNames.len == 0:
    return err[Statement](ERR_SQL, "CREATE INDEX requires at least one column")
  var hasExpression = false
  for colName in stmt.columnNames:
    if colName.startsWith(IndexExpressionPrefix):
      hasExpression = true
      break

  if hasExpression:
    if stmt.columnNames.len != 1:
      return err[Statement](ERR_SQL, "Expression indexes support only a single expression in 0.x")
    if stmt.indexKind != sql.ikBtree:
      return err[Statement](ERR_SQL, "Expression indexes are supported only for BTREE in 0.x")
    if stmt.unique:
      return err[Statement](ERR_SQL, "UNIQUE expression indexes are not supported in 0.x")
    if stmt.indexPredicate != nil:
      return err[Statement](ERR_SQL, "Partial expression indexes are not supported in 0.x")

    let exprSql = stmt.columnNames[0][IndexExpressionPrefix.len .. ^1]
    let parseRes = parseSql("SELECT " & exprSql & " FROM " & stmt.indexTableName)
    if not parseRes.ok:
      return err[Statement](parseRes.err.code, "Invalid expression index expression: " & parseRes.err.message, parseRes.err.context)
    if parseRes.value.statements.len != 1 or parseRes.value.statements[0].kind != skSelect:
      return err[Statement](ERR_SQL, "Invalid expression index expression", exprSql)
    let parsedSelect = parseRes.value.statements[0]
    if parsedSelect.selectItems.len != 1 or parsedSelect.selectItems[0].isStar:
      return err[Statement](ERR_SQL, "Expression index requires exactly one expression", exprSql)
    let expr = parsedSelect.selectItems[0].expr
    if expr == nil:
      return err[Statement](ERR_SQL, "Expression index expression is required")

    let mapRes = buildTableMap(catalog, stmt.indexTableName, "", @[])
    if not mapRes.ok:
      return err[Statement](mapRes.err.code, mapRes.err.message, mapRes.err.context)
    let exprBind = bindExpr(mapRes.value, expr)
    if not exprBind.ok:
      return err[Statement](exprBind.err.code, exprBind.err.message, exprBind.err.context)

    var supported = false
    case expr.kind
    of ekColumn:
      supported = true
    of ekFunc:
      let fn = expr.funcName.toUpperAscii()
      if fn in ["LOWER", "UPPER", "TRIM", "LENGTH"]:
        supported = expr.args.len == 1 and expr.args[0] != nil and expr.args[0].kind == ekColumn
      elif fn == "CAST":
        if expr.args.len == 2 and expr.args[0] != nil and expr.args[0].kind == ekColumn and
            expr.args[1] != nil and expr.args[1].kind == ekLiteral and expr.args[1].value.kind == svString:
          let castTypeRes = parseColumnType(expr.args[1].value.strVal)
          supported = castTypeRes.ok and castTypeRes.value.kind in {ctInt64, ctFloat64, ctText, ctBool}
    else:
      discard
    if not supported:
      return err[Statement](ERR_SQL, "Expression index supports only `column`, `LOWER/UPPER/TRIM/LENGTH(column)`, or `CAST(column AS type)` in 0.x")
    return ok(stmt)

  for colName in stmt.columnNames:
    var found = false
    for col in tableRes.value.columns:
      if col.name == colName:
        found = true
        if stmt.indexKind == sql.ikTrigram and col.kind != ctText:
          return err[Statement](ERR_SQL, "Trigram index only supported on TEXT columns", colName)
        break
    if not found:
      return err[Statement](ERR_SQL, "Unknown column", colName)
  if stmt.indexKind == sql.ikTrigram and stmt.columnNames.len > 1:
    return err[Statement](ERR_SQL, "Trigram index on multiple columns not supported")
  if stmt.indexKind == sql.ikTrigram and stmt.unique:
    return err[Statement](ERR_SQL, "Trigram index cannot be UNIQUE", stmt.columnNames[0])
  if stmt.indexPredicate != nil:
    if stmt.indexKind != sql.ikBtree:
      return err[Statement](ERR_SQL, "Partial indexes are supported only for BTREE in 0.x")
    if stmt.columnNames.len != 1:
      return err[Statement](ERR_SQL, "Partial indexes support only single-column BTREE indexes in 0.x")
    if stmt.unique:
      return err[Statement](ERR_SQL, "UNIQUE partial indexes are not supported in 0.x")
    var map = initTable[string, TableMeta]()
    map[stmt.indexTableName] = tableRes.value
    let predBind = bindExpr(map, stmt.indexPredicate)
    if not predBind.ok:
      return err[Statement](predBind.err.code, predBind.err.message, predBind.err.context)
    let p = stmt.indexPredicate
    let supported =
      p.kind == ekBinary and p.op == "IS NOT" and
      p.left != nil and p.left.kind == ekColumn and
      (p.left.table.len == 0 or p.left.table == stmt.indexTableName) and
      p.left.name == stmt.columnNames[0] and
      p.right != nil and p.right.kind == ekLiteral and p.right.value.kind == svNull
    if not supported:
      return err[Statement](ERR_SQL, "Partial index predicate must be `<indexed_column> IS NOT NULL` in 0.x")
  ok(stmt)

proc validateDependentViewsForReplacement(catalog: Catalog, candidate: ViewMeta): Result[Void] =
  let direct = catalog.listDependentViews(candidate.name)
  if direct.len == 0:
    return okVoid()

  let backupViews = catalog.views
  let backupDependents = catalog.dependentViews

  catalog.views[candidate.name] = candidate
  rebuildDependentViewsIndex(catalog)

  let dependents = collectTransitiveDependents(catalog, candidate.name)
  for depName in dependents:
    let depRes = catalog.getView(depName)
    if not depRes.ok:
      catalog.views = backupViews
      catalog.dependentViews = backupDependents
      return err[Void](depRes.err.code, depRes.err.message, depRes.err.context)
    let parseRes = parseViewSelect(depRes.value)
    if not parseRes.ok:
      catalog.views = backupViews
      catalog.dependentViews = backupDependents
      return err[Void](parseRes.err.code, parseRes.err.message, parseRes.err.context)
    let expandRes = expandSelectSources(catalog, parseRes.value, 0, @[depName])
    if not expandRes.ok:
      catalog.views = backupViews
      catalog.dependentViews = backupDependents
      return err[Void](expandRes.err.code, "Dependent view revalidation failed: " & expandRes.err.message, depName)
    let colRes = resolveViewOutputColumnNames(catalog, depName, depRes.value.columnNames, expandRes.value)
    if not colRes.ok:
      catalog.views = backupViews
      catalog.dependentViews = backupDependents
      return err[Void](colRes.err.code, "Dependent view revalidation failed: " & colRes.err.message, colRes.err.context)

  catalog.views = backupViews
  catalog.dependentViews = backupDependents
  okVoid()

proc bindCreateView(catalog: Catalog, stmt: Statement): Result[Statement] =
  if stmt.createViewIfNotExists and stmt.createViewOrReplace:
    return err[Statement](ERR_SQL, "CREATE OR REPLACE VIEW cannot include IF NOT EXISTS", stmt.createViewName)

  let viewName = normalizedName(stmt.createViewName)

  if catalog.hasTableName(viewName):
    return err[Statement](ERR_SQL, "Object name collides with existing table", stmt.createViewName)

  let exists = catalog.hasViewName(viewName)
  if exists and not stmt.createViewOrReplace:
    if stmt.createViewIfNotExists:
      return ok(stmt)
    return err[Statement](ERR_SQL, "View already exists", stmt.createViewName)

  if stmt.createViewQuery == nil or stmt.createViewQuery.kind != skSelect:
    return err[Statement](ERR_SQL, "CREATE VIEW requires SELECT definition", stmt.createViewName)

  if hasParamsInSelect(stmt.createViewQuery):
    return err[Statement](ERR_SQL, "View definition cannot contain parameters", stmt.createViewName)

  let deps = directSelectDependencies(stmt.createViewQuery)
  for dep in deps:
    if not catalog.hasTableName(dep) and not catalog.hasViewName(dep):
      return err[Statement](ERR_SQL, "Referenced object not found", dep)
    if dep == viewName:
      return err[Statement](ERR_SQL, "Circular view reference", stmt.createViewName)
    if catalog.hasViewName(dep):
      var visiting = initHashSet[string]()
      if viewDependsOn(catalog, dep, viewName, visiting):
        return err[Statement](ERR_SQL, "Circular view reference", stmt.createViewName)

  let expandedRes = expandSelectSources(catalog, stmt.createViewQuery, 0, @[viewName])
  if not expandedRes.ok:
    return err[Statement](expandedRes.err.code, expandedRes.err.message, expandedRes.err.context)

  let colRes = resolveViewOutputColumnNames(catalog, viewName, stmt.createViewColumns, expandedRes.value)
  if not colRes.ok:
    return err[Statement](colRes.err.code, colRes.err.message, colRes.err.context)

  let bound = Statement(
    kind: skCreateView,
    createViewName: viewName,
    createViewIfNotExists: stmt.createViewIfNotExists,
    createViewOrReplace: stmt.createViewOrReplace,
    createViewColumns: colRes.value,
    createViewSqlText: stmt.createViewSqlText,
    createViewQuery: cloneSelectStatement(stmt.createViewQuery)
  )

  if stmt.createViewOrReplace and exists:
    let candidate = ViewMeta(
      name: viewName,
      sqlText: bound.createViewSqlText,
      columnNames: bound.createViewColumns,
      dependencies: deps
    )
    let revalidateRes = validateDependentViewsForReplacement(catalog, candidate)
    if not revalidateRes.ok:
      return err[Statement](revalidateRes.err.code, revalidateRes.err.message, revalidateRes.err.context)

  ok(bound)

proc bindDropTable(catalog: Catalog, stmt: Statement): Result[Statement] =
  let deps = catalog.listDependentViews(stmt.dropTableName)
  if deps.len > 0:
    return err[Statement](ERR_SQL, "Cannot drop table with dependent views", stmt.dropTableName)
  ok(stmt)

proc bindDropView(catalog: Catalog, stmt: Statement): Result[Statement] =
  if not catalog.hasViewName(stmt.dropViewName):
    if stmt.dropViewIfExists:
      return ok(stmt)
    return err[Statement](ERR_SQL, "View not found", stmt.dropViewName)
  let deps = catalog.listDependentViews(stmt.dropViewName)
  if deps.len > 0:
    return err[Statement](ERR_SQL, "Cannot drop view with dependent views", stmt.dropViewName)
  ok(stmt)

proc bindAlterView(catalog: Catalog, stmt: Statement): Result[Statement] =
  if not catalog.hasViewName(stmt.alterViewName):
    return err[Statement](ERR_SQL, "View not found", stmt.alterViewName)
  if catalog.hasTableOrViewName(stmt.alterViewNewName):
    return err[Statement](ERR_SQL, "Target name already exists", stmt.alterViewNewName)
  let deps = catalog.listDependentViews(stmt.alterViewName)
  if deps.len > 0:
    return err[Statement](ERR_SQL, "Cannot rename view with dependent views", stmt.alterViewName)
  ok(stmt)

proc bindAlterTable(catalog: Catalog, stmt: Statement): Result[Statement] =
  let tableRes = catalog.getTable(stmt.alterTableName)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let table = tableRes.value
  for _, idx in catalog.indexes:
    if idx.table == stmt.alterTableName and idx.columns.len == 1 and idx.columns[0].startsWith(IndexExpressionPrefix):
      return err[Statement](ERR_SQL, "ALTER TABLE on tables with expression indexes is not supported in 0.x", stmt.alterTableName)
  if table.checks.len > 0:
    return err[Statement](ERR_SQL, "ALTER TABLE on tables with CHECK constraints is not supported in 0.x", stmt.alterTableName)
  let dependentViews = catalog.listDependentViews(stmt.alterTableName)
  for action in stmt.alterActions:
    case action.kind
    of ataAddColumn:
      for col in table.columns:
        if col.name == action.columnDef.name:
          return err[Statement](ERR_SQL, "Column already exists", action.columnDef.name)
      let validTypes = ["INT", "INT64", "INTEGER", "TEXT", "BLOB", "BOOL", "BOOLEAN", "FLOAT", "FLOAT64", "REAL"]
      var typeValid = false
      let typeUpper = action.columnDef.typeName.toUpperAscii()
      for validType in validTypes:
        if typeUpper == validType:
          typeValid = true
          break
      if not typeValid:
        return err[Statement](ERR_SQL, "Unsupported column type for ALTER TABLE", action.columnDef.typeName)
    of ataDropColumn:
      var found = false
      for col in table.columns:
        if col.name == action.columnName:
          found = true
          if col.primaryKey:
            return err[Statement](ERR_SQL, "Cannot drop PRIMARY KEY column", action.columnName)
          break
      if not found:
        return err[Statement](ERR_SQL, "Column does not exist", action.columnName)
    of ataRenameColumn:
      if dependentViews.len > 0:
        return err[Statement](ERR_SQL, "Cannot rename column on table with dependent views", stmt.alterTableName)
      var oldFound = false
      var newFound = false
      for col in table.columns:
        if col.name == action.columnName:
          oldFound = true
        if col.name == action.newColumnName:
          newFound = true
      if not oldFound:
        return err[Statement](ERR_SQL, "Column does not exist", action.columnName)
      if newFound:
        return err[Statement](ERR_SQL, "Column already exists", action.newColumnName)
    of ataAlterColumn:
      var found = false
      var foundCol = Column()
      for col in table.columns:
        if col.name == action.columnName:
          found = true
          foundCol = col
          break
      if not found:
        return err[Statement](ERR_SQL, "Column does not exist", action.columnName)
      if action.alterColumnAction == acaSetType:
        if foundCol.primaryKey:
          return err[Statement](ERR_SQL, "Cannot alter type of PRIMARY KEY column", action.columnName)
        if foundCol.refTable.len > 0 and foundCol.refColumn.len > 0:
          return err[Statement](ERR_SQL, "Cannot alter type of FOREIGN KEY child column", action.columnName)
        for otherTableName, otherTable in catalog.tables:
          for otherCol in otherTable.columns:
            if otherCol.refTable == stmt.alterTableName and otherCol.refColumn == action.columnName:
              return err[Statement](ERR_SQL, "Cannot alter type of column referenced by FOREIGN KEY", stmt.alterTableName & "." & action.columnName)
        let targetTypeRes = parseColumnType(action.alterColumnNewType)
        if not targetTypeRes.ok:
          return err[Statement](targetTypeRes.err.code, targetTypeRes.err.message, targetTypeRes.err.context)
        if targetTypeRes.value.kind notin {ctInt64, ctFloat64, ctText, ctBool}:
          return err[Statement](ERR_SQL, "ALTER COLUMN TYPE target not supported in 0.x", action.alterColumnNewType)
        if foundCol.kind notin {ctInt64, ctFloat64, ctText, ctBool}:
          return err[Statement](ERR_SQL, "ALTER COLUMN TYPE source not supported in 0.x", action.columnName)
    else:
      return err[Statement](ERR_SQL, "Unsupported ALTER TABLE action")
  ok(stmt)

proc bindCreateTrigger(catalog: Catalog, stmt: Statement): Result[Statement] =
  let isInstead = (stmt.triggerEventsMask and TriggerTimingInsteadMask) != 0
  if isInstead:
    let viewRes = catalog.getView(stmt.triggerTableName)
    if not viewRes.ok:
      return err[Statement](viewRes.err.code, viewRes.err.message, viewRes.err.context)
  else:
    let tableRes = catalog.getTable(stmt.triggerTableName)
    if not tableRes.ok:
      if catalog.hasViewName(stmt.triggerTableName):
        return err[Statement](ERR_SQL, "AFTER triggers require a base table target", stmt.triggerTableName)
      return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)

  if stmt.triggerName.len == 0:
    return err[Statement](ERR_SQL, "Trigger name is required")
  if not stmt.triggerForEachRow:
    return err[Statement](ERR_SQL, "FOR EACH STATEMENT triggers are not supported in 0.x")
  if (stmt.triggerEventsMask and (TriggerEventInsertMask or TriggerEventUpdateMask or TriggerEventDeleteMask)) == 0:
    return err[Statement](ERR_SQL, "Trigger must include INSERT, UPDATE, or DELETE event")

  let fn = stmt.triggerFunctionName.toLowerAscii()
  if fn != "decentdb_exec_sql":
    return err[Statement](ERR_SQL, "Only decentdb_exec_sql trigger action function is supported in 0.x", stmt.triggerFunctionName)
  if stmt.triggerActionSql.len == 0:
    return err[Statement](ERR_SQL, "Trigger action SQL is required")
  if catalog.hasTrigger(stmt.triggerTableName, stmt.triggerName):
    return err[Statement](ERR_SQL, "Trigger already exists", stmt.triggerTableName & "." & stmt.triggerName)

  let parseRes = parseSql(stmt.triggerActionSql)
  if not parseRes.ok:
    return err[Statement](parseRes.err.code, "Invalid trigger action SQL: " & parseRes.err.message, parseRes.err.context)
  if parseRes.value.statements.len != 1:
    return err[Statement](ERR_SQL, "Trigger action must contain exactly one statement")
  let actionStmt = parseRes.value.statements[0]
  if actionStmt.kind notin {skInsert, skUpdate, skDelete}:
    return err[Statement](ERR_SQL, "Trigger action must be INSERT, UPDATE, or DELETE in 0.x")
  if actionStmt.kind == skInsert and actionStmt.insertReturning.len > 0:
    return err[Statement](ERR_SQL, "Trigger action INSERT ... RETURNING is not supported in 0.x")
  if hasParamsInDmlStatement(actionStmt):
    return err[Statement](ERR_SQL, "Trigger action cannot use parameters in 0.x")

  let boundAction = bindStatement(catalog, actionStmt)
  if not boundAction.ok:
    return err[Statement](boundAction.err.code, "Invalid trigger action SQL: " & boundAction.err.message, boundAction.err.context)

  ok(stmt)

proc bindDropTrigger(catalog: Catalog, stmt: Statement): Result[Statement] =
  if stmt.dropTriggerName.len == 0 or stmt.dropTriggerTableName.len == 0:
    return err[Statement](ERR_SQL, "DROP TRIGGER requires trigger name and table")
  let hasTable = catalog.hasTableName(stmt.dropTriggerTableName)
  let hasView = catalog.hasViewName(stmt.dropTriggerTableName)
  if not hasTable and not hasView:
    return err[Statement](ERR_SQL, "Object not found", stmt.dropTriggerTableName)
  if not catalog.hasTrigger(stmt.dropTriggerTableName, stmt.dropTriggerName):
    if stmt.dropTriggerIfExists:
      return ok(stmt)
    return err[Statement](ERR_SQL, "Trigger not found", stmt.dropTriggerTableName & "." & stmt.dropTriggerName)
  ok(stmt)

proc bindStatement*(catalog: Catalog, stmt: Statement): Result[Statement] =
  case stmt.kind
  of skSelect:
    bindSelect(catalog, stmt)
  of skInsert:
    bindInsert(catalog, stmt)
  of skUpdate:
    bindUpdate(catalog, stmt)
  of skDelete:
    bindDelete(catalog, stmt)
  of skCreateTable:
    bindCreateTable(catalog, stmt)
  of skCreateIndex:
    bindCreateIndex(catalog, stmt)
  of skCreateTrigger:
    bindCreateTrigger(catalog, stmt)
  of skCreateView:
    bindCreateView(catalog, stmt)
  of skDropTable:
    bindDropTable(catalog, stmt)
  of skDropIndex:
    ok(stmt)
  of skDropTrigger:
    bindDropTrigger(catalog, stmt)
  of skDropView:
    bindDropView(catalog, stmt)
  of skAlterView:
    bindAlterView(catalog, stmt)
  of skAlterTable:
    bindAlterTable(catalog, stmt)
  of skBegin, skCommit, skRollback:
    ok(stmt)
  of skExplain:
    let innerRes = bindStatement(catalog, stmt.explainInner)
    if not innerRes.ok:
      return err[Statement](innerRes.err.code, innerRes.err.message, innerRes.err.context)
    ok(Statement(kind: skExplain, explainInner: innerRes.value, explainHasOptions: stmt.explainHasOptions))
