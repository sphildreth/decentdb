import ../errors
import ../sql/sql
import ../catalog/catalog

type PlanKind* = enum
  pkStatement
  pkTableScan
  pkIndexSeek
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

proc planSelect(catalog: Catalog, stmt: Statement): Plan =
  var base: Plan = nil
  var idxColumn = ""
  var idxValue: Expr = nil
  if isSimpleEquality(stmt.whereExpr, stmt.fromTable, idxColumn, idxValue):
    let idxOpt = catalog.getIndexForColumn(stmt.fromTable, idxColumn)
    if idxOpt.isSome:
      base = Plan(kind: pkIndexSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idxColumn, valueExpr: idxValue)
  if base == nil:
    base = Plan(kind: pkTableScan, table: stmt.fromTable, alias: stmt.fromAlias)
  if stmt.whereExpr != nil and base.kind != pkIndexSeek:
    base = Plan(kind: pkFilter, predicate: stmt.whereExpr, left: base)
  for join in stmt.joins:
    var rightPlan: Plan = nil
    var joinIdxCol = ""
    var joinIdxVal: Expr = nil
    if isSimpleEquality(join.onExpr, join.table, joinIdxCol, joinIdxVal):
      let idxOpt = catalog.getIndexForColumn(join.table, joinIdxCol)
      if idxOpt.isSome:
        rightPlan = Plan(kind: pkIndexSeek, table: join.table, alias: join.alias, column: joinIdxCol, valueExpr: joinIdxVal)
    if rightPlan == nil:
      rightPlan = Plan(kind: pkTableScan, table: join.table, alias: join.alias)
    base = Plan(kind: pkJoin, joinType: join.joinType, joinOn: join.onExpr, left: base, right: rightPlan)
  if stmt.groupBy.len > 0:
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
