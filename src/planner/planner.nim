import options
import ../errors
import ../sql/sql
import ../catalog/catalog
import sets
import strutils

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
  limitParam*: int
  offset*: int
  offsetParam*: int
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

proc isSimpleEqualityFor(expr: Expr, table: string, alias: string, columnOut: var string, valueOut: var Expr): bool =
  if isSimpleEquality(expr, table, columnOut, valueOut):
    return true
  if alias.len > 0 and isSimpleEquality(expr, alias, columnOut, valueOut):
    return true
  false

proc isTrigramLikeFor(expr: Expr, table: string, alias: string, columnOut: var string, patternOut: var Expr, insensitive: var bool): bool =
  if isTrigramLike(expr, table, columnOut, patternOut, insensitive):
    return true
  if alias.len > 0 and isTrigramLike(expr, alias, columnOut, patternOut, insensitive):
    return true
  false

proc splitAnd(expr: Expr): seq[Expr] =
  if expr == nil:
    return @[]
  if expr.kind == ekBinary and expr.op.toUpperAscii() == "AND":
    result.add(splitAnd(expr.left))
    result.add(splitAnd(expr.right))
  else:
    result.add(expr)

proc referencedTables(expr: Expr, tablesOut: var HashSet[string]) =
  if expr == nil:
    return
  case expr.kind
  of ekColumn:
    if expr.table.len > 0:
      tablesOut.incl(expr.table)
  of ekBinary:
    referencedTables(expr.left, tablesOut)
    referencedTables(expr.right, tablesOut)
  of ekUnary:
    referencedTables(expr.expr, tablesOut)
  of ekFunc:
    for a in expr.args:
      referencedTables(a, tablesOut)
  of ekInList:
    referencedTables(expr.inExpr, tablesOut)
    for item in expr.inList:
      referencedTables(item, tablesOut)
  else:
    discard

proc refs(expr: Expr): HashSet[string] =
  result = initHashSet[string]()
  referencedTables(expr, result)

proc planSelect(catalog: Catalog, stmt: Statement): Plan =
  proc hasAggregate(items: seq[SelectItem]): bool =
    for item in items:
      if item.expr != nil and item.expr.kind == ekFunc:
        return true
    false
  var conjuncts = splitAnd(stmt.whereExpr)
  var base: Plan = nil

  # Choose the best access path for the FROM table from any conjunct.
  var accessConjunctIdx = -1
  for i, c in conjuncts:
    var idxColumn = ""
    var idxValue: Expr = nil
    if isSimpleEqualityFor(c, stmt.fromTable, stmt.fromAlias, idxColumn, idxValue):
      let idxOpt = catalog.getBtreeIndexForColumn(stmt.fromTable, idxColumn)
      if isSome(idxOpt):
        base = Plan(kind: pkIndexSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idxColumn, valueExpr: idxValue)
        accessConjunctIdx = i
        break
  if base == nil:
    for i, c in conjuncts:
      var likeColumn = ""
      var likePattern: Expr = nil
      var ignoreInsensitive = false
      if isTrigramLikeFor(c, stmt.fromTable, stmt.fromAlias, likeColumn, likePattern, ignoreInsensitive):
        let idxOpt = catalog.getTrigramIndexForColumn(stmt.fromTable, likeColumn)
        if isSome(idxOpt):
          base = Plan(kind: pkTrigramSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: likeColumn, likeExpr: likePattern, likeInsensitive: ignoreInsensitive)
          accessConjunctIdx = i
          break
  if accessConjunctIdx >= 0:
    conjuncts.delete(accessConjunctIdx)
  if base == nil:
    base = Plan(kind: pkTableScan, table: stmt.fromTable, alias: stmt.fromAlias)

  # Predicate pushdown: apply conjuncts as early as possible (once referenced
  # tables are available), rather than applying the full WHERE before joins.
  var available = initHashSet[string]()
  if stmt.fromAlias.len > 0:
    available.incl(stmt.fromAlias)
  available.incl(stmt.fromTable)

  proc applyEligible() =
    var remaining: seq[Expr] = @[]
    for c in conjuncts:
      let r = refs(c)
      if r.len == 0:
        # Unqualified columns are treated conservatively; only apply at the end
        # once all joins have been introduced.
        remaining.add(c)
      elif r <= available:
        base = Plan(kind: pkFilter, predicate: c, left: base)
      else:
        remaining.add(c)
    conjuncts = remaining
  applyEligible()
  for join in stmt.joins:
    var rightPlan: Plan = nil
    var joinIdxCol = ""
    var joinIdxVal: Expr = nil
    if isSimpleEqualityFor(join.onExpr, join.table, join.alias, joinIdxCol, joinIdxVal):
      let idxOpt = catalog.getBtreeIndexForColumn(join.table, joinIdxCol)
      if isSome(idxOpt):
        rightPlan = Plan(kind: pkIndexSeek, table: join.table, alias: join.alias, column: joinIdxCol, valueExpr: joinIdxVal)
    if rightPlan == nil:
      rightPlan = Plan(kind: pkTableScan, table: join.table, alias: join.alias)
    base = Plan(kind: pkJoin, joinType: join.joinType, joinOn: join.onExpr, left: base, right: rightPlan)
    if join.alias.len > 0:
      available.incl(join.alias)
    available.incl(join.table)
    applyEligible()

  # Apply any remaining conjuncts after all joins are present.
  for c in conjuncts:
    base = Plan(kind: pkFilter, predicate: c, left: base)
  if stmt.groupBy.len > 0 or hasAggregate(stmt.selectItems):
    base = Plan(kind: pkAggregate, groupBy: stmt.groupBy, having: stmt.havingExpr, projections: stmt.selectItems, left: base)
  else:
    base = Plan(kind: pkProject, projections: stmt.selectItems, left: base)
  if stmt.orderBy.len > 0:
    base = Plan(kind: pkSort, orderBy: stmt.orderBy, left: base)
  if stmt.limit >= 0 or stmt.limitParam > 0 or stmt.offset >= 0 or stmt.offsetParam > 0:
    base = Plan(kind: pkLimit, limit: stmt.limit, limitParam: stmt.limitParam, offset: stmt.offset, offsetParam: stmt.offsetParam, left: base)
  base

proc plan*(catalog: Catalog, stmt: Statement): Result[Plan] =
  if stmt.kind == skSelect:
    return ok(planSelect(catalog, stmt))
  ok(Plan(kind: pkStatement, stmt: stmt))
