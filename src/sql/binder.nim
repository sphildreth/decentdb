import options
import tables
import sequtils
import ../errors
import ../catalog/catalog
import ./sql

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
    return okVoid()
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
    # Bind the expression being tested
    let exprRes = bindExpr(map, expr.inExpr)
    if not exprRes.ok:
      return exprRes
    # Bind all values in the IN list
    for item in expr.inList:
      let itemRes = bindExpr(map, item)
      if not itemRes.ok:
        return itemRes
    okVoid()
  else:
    okVoid()

proc bindSelect(catalog: Catalog, stmt: Statement): Result[Statement] =
  let mapRes = buildTableMap(catalog, stmt.fromTable, stmt.fromAlias, stmt.joins)
  if not mapRes.ok:
    return err[Statement](mapRes.err.code, mapRes.err.message, mapRes.err.context)
  let map = mapRes.value
  for item in stmt.selectItems:
    if item.isStar:
      continue
    let res = bindExpr(map, item.expr)
    if not res.ok:
      return err[Statement](res.err.code, res.err.message, res.err.context)
  let whereRes = bindExpr(map, stmt.whereExpr)
  if not whereRes.ok:
    return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)
  for join in stmt.joins:
    let onRes = bindExpr(map, join.onExpr)
    if not onRes.ok:
      return err[Statement](onRes.err.code, onRes.err.message, onRes.err.context)
  for expr in stmt.groupBy:
    let res = bindExpr(map, expr)
    if not res.ok:
      return err[Statement](res.err.code, res.err.message, res.err.context)
  let havingRes = bindExpr(map, stmt.havingExpr)
  if not havingRes.ok:
    return err[Statement](havingRes.err.code, havingRes.err.message, havingRes.err.context)
  for item in stmt.orderBy:
    let res = bindExpr(map, item.expr)
    if not res.ok:
      return err[Statement](res.err.code, res.err.message, res.err.context)
  ok(stmt)

proc bindInsert(catalog: Catalog, stmt: Statement): Result[Statement] =
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
  ok(Statement(kind: skInsert, insertTable: stmt.insertTable, insertColumns: @[], insertValues: ordered))

proc bindUpdate(catalog: Catalog, stmt: Statement): Result[Statement] =
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
  let mapRes = buildTableMap(catalog, stmt.deleteTable, "", @[])
  if not mapRes.ok:
    return err[Statement](mapRes.err.code, mapRes.err.message, mapRes.err.context)
  let whereRes = bindExpr(mapRes.value, stmt.deleteWhere)
  if not whereRes.ok:
    return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)
  ok(stmt)

proc bindCreateTable(catalog: Catalog, stmt: Statement): Result[Statement] =
  var primaryCount = 0
  for col in stmt.columns:
    let typeRes = parseColumnType(col.typeName)
    if not typeRes.ok:
      return err[Statement](typeRes.err.code, typeRes.err.message, col.typeName)
    if col.primaryKey:
      primaryCount.inc
    if col.refTable.len > 0 and col.refColumn.len > 0:
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
      let parentIdx = catalog.getBtreeIndexForColumn(col.refTable, col.refColumn)
      if isNone(parentIdx) or not parentIdx.get.unique:
        return err[Statement](ERR_SQL, "Referenced column must be indexed uniquely", col.refTable & "." & col.refColumn)
  if primaryCount > 1:
    return err[Statement](ERR_SQL, "Multiple primary keys not supported")
  ok(stmt)

proc bindCreateIndex(catalog: Catalog, stmt: Statement): Result[Statement] =
  let tableRes = catalog.getTable(stmt.indexTableName)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  var found = false
  var columnType = ctInt64
  for col in tableRes.value.columns:
    if col.name == stmt.columnName:
      found = true
      columnType = col.kind
  if not found:
    return err[Statement](ERR_SQL, "Unknown column", stmt.columnName)
  if stmt.indexKind == sql.ikTrigram and columnType != ctText:
    return err[Statement](ERR_SQL, "Trigram index only supported on TEXT columns", stmt.columnName)
  if stmt.indexKind == sql.ikTrigram and stmt.unique:
    return err[Statement](ERR_SQL, "Trigram index cannot be UNIQUE", stmt.columnName)
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
  of skDropTable, skDropIndex, skBegin, skCommit, skRollback:
    ok(stmt)
