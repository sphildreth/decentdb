import options
import ../errors
import ../sql/sql
import ../catalog/catalog

type PlanKind* = enum
  pkStatement
  pkTableScan
  pkIndexSeek
  pkTrigramSeek
  pkFilter
  pkProject
  pkJoin
  pkSort
  pkAggregate
  pkLimit

type Plan* = ref object
  kind*: PlanKind
  stmt*: Statement
  table*: string
  alias*: string
  column*: string
  valueExpr*: Expr
  likeExpr*: Expr
  likeInsensitive*: bool
  predicate*: Expr
  projections*: seq[SelectItem]
  joinType*: JoinType
  joinOn*: Expr
  left*: Plan
  right*: Plan
  orderBy*: seq[OrderItem]
  limit*: int
  offset*: int
  groupBy*: seq[Expr]
  having*: Expr

proc isSimpleEquality(expr: Expr, table: string, columnOut: var string, valueOut: var Expr): bool =
  if expr == nil or expr.kind != ekBinary or expr.op != "=":
    return false
  let left = expr.left
  let right = expr.right
  if left.kind == ekColumn and (left.table.len == 0 or left.table == table):
    columnOut = left.name
    valueOut = right
    return true
  if right.kind == ekColumn and (right.table.len == 0 or right.table == table):
    columnOut = right.name
    valueOut = left
    return true
  false

proc isTrigramLike(expr: Expr, table: string, columnOut: var string, patternOut: var Expr, insensitive: var bool): bool =
  if expr == nil or expr.kind != ekBinary or not (expr.op in ["LIKE", "ILIKE"]):
    return false
  let left = expr.left
  let right = expr.right
  insensitive = expr.op == "ILIKE"
  if left.kind == ekColumn and (left.table.len == 0 or left.table == table):
    columnOut = left.name
    patternOut = right
    return true
  if right.kind == ekColumn and (right.table.len == 0 or right.table == table):
    columnOut = right.name
    patternOut = left
    return true
  false

proc planSelect(catalog: Catalog, stmt: Statement): Plan =
  proc hasAggregate(items: seq[SelectItem]): bool =
    for item in items:
      if item.expr != nil and item.expr.kind == ekFunc:
        return true
    false
  var base: Plan = nil
  var idxColumn = ""
  var idxValue: Expr = nil
  var likeColumn = ""
  var likePattern: Expr = nil
  var ignoreInsensitive = false
  if isTrigramLike(stmt.whereExpr, stmt.fromTable, likeColumn, likePattern, ignoreInsensitive):
    let idxOpt = catalog.getTrigramIndexForColumn(stmt.fromTable, likeColumn)
    if isSome(idxOpt):
      base = Plan(kind: pkTrigramSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: likeColumn, likeExpr: likePattern, likeInsensitive: true)
  if isSimpleEquality(stmt.whereExpr, stmt.fromTable, idxColumn, idxValue):
    let idxOpt = catalog.getBtreeIndexForColumn(stmt.fromTable, idxColumn)
    if isSome(idxOpt):
      base = Plan(kind: pkIndexSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idxColumn, valueExpr: idxValue)
  if base == nil:
    base = Plan(kind: pkTableScan, table: stmt.fromTable, alias: stmt.fromAlias)
  if stmt.whereExpr != nil and base.kind notin {pkIndexSeek, pkTrigramSeek}:
    base = Plan(kind: pkFilter, predicate: stmt.whereExpr, left: base)
  for join in stmt.joins:
    var rightPlan: Plan = nil
    var joinIdxCol = ""
    var joinIdxVal: Expr = nil
    if isSimpleEquality(join.onExpr, join.table, joinIdxCol, joinIdxVal):
      let idxOpt = catalog.getBtreeIndexForColumn(join.table, joinIdxCol)
      if isSome(idxOpt):
        rightPlan = Plan(kind: pkIndexSeek, table: join.table, alias: join.alias, column: joinIdxCol, valueExpr: joinIdxVal)
    if rightPlan == nil:
      rightPlan = Plan(kind: pkTableScan, table: join.table, alias: join.alias)
    base = Plan(kind: pkJoin, joinType: join.joinType, joinOn: join.onExpr, left: base, right: rightPlan)
  if stmt.groupBy.len > 0 or hasAggregate(stmt.selectItems):
    base = Plan(kind: pkAggregate, groupBy: stmt.groupBy, having: stmt.havingExpr, projections: stmt.selectItems, left: base)
  else:
    base = Plan(kind: pkProject, projections: stmt.selectItems, left: base)
  if stmt.orderBy.len > 0:
    base = Plan(kind: pkSort, orderBy: stmt.orderBy, left: base)
  if stmt.limit >= 0 or stmt.offset >= 0:
    base = Plan(kind: pkLimit, limit: stmt.limit, offset: stmt.offset, left: base)
  base

proc plan*(catalog: Catalog, stmt: Statement): Result[Plan] =
  if stmt.kind == skSelect:
    return ok(planSelect(catalog, stmt))
  ok(Plan(kind: pkStatement, stmt: stmt))
