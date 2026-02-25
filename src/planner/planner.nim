import options
import math
import algorithm
import ../errors
import ../sql/sql
import ../catalog/catalog
import ../record/record
import sets
import strutils
import tables

type PlanKind* = enum
  pkStatement
  pkOneRow
  pkTableScan
  pkRowidSeek
  pkIndexSeek
  pkTrigramSeek
  pkSubqueryScan
  pkLiteralRows
  pkUnionDistinct
  pkSetUnionDistinct
  pkSetIntersect
  pkSetIntersectAll
  pkSetExcept
  pkSetExceptAll
  pkAppend
  pkFilter
  pkProject
  pkJoin
  pkSort
  pkAggregate
  pkLimit
  pkTvfScan

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
  subPlan*: Plan
  orderBy*: seq[OrderItem]
  limit*: int
  limitParam*: int
  offset*: int
  offsetParam*: int
  groupBy*: seq[Expr]
  having*: Expr
  rows*: seq[seq[(string, Value)]]  # for pkLiteralRows: pre-materialized rows
  tvfFunc*: string                   # for pkTvfScan: function name
  tvfArgs*: seq[Expr]                # for pkTvfScan: argument expressions
  estRows*: int64                    # estimated output cardinality (0 = unknown)
  estCost*: float64                  # estimated relative cost (0.0 = unknown)

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

proc normalizeIndexExprSql(sqlText: string, table: string, alias: string): string =
  var normalized = sqlText
  if table.len > 0:
    normalized = normalized.replace(table & ".", "")
  if alias.len > 0:
    normalized = normalized.replace(alias & ".", "")
  normalized

proc splitAnd(expr: Expr): seq[Expr] =
  if expr == nil:
    return @[]
  if expr.kind == ekBinary and expr.op.toUpperAscii() == "AND":
    result.add(splitAnd(expr.left))
    result.add(splitAnd(expr.right))
  else:
    result.add(expr)

proc splitOr(expr: Expr): seq[Expr] =
  if expr == nil:
    return @[]
  if expr.kind == ekBinary and expr.op.toUpperAscii() == "OR":
    result.add(splitOr(expr.left))
    result.add(splitOr(expr.right))
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
  of ekWindowRowNumber:
    for part in expr.windowPartitions:
      referencedTables(part, tablesOut)
    for o in expr.windowOrderExprs:
      referencedTables(o, tablesOut)
    for a in expr.windowArgs:
      referencedTables(a, tablesOut)
  else:
    discard

proc refs(expr: Expr): HashSet[string] =
  result = initHashSet[string]()
  referencedTables(expr, result)

proc seekCheaperThanScan(catalog: Catalog, tableName: string, idxName: string): bool =
  ## Return true if an index seek is estimated cheaper than a full table scan.
  ## When stats are absent, default to seeking (preserves prior heuristic).
  let tableStatsOpt = catalog.getTableStats(tableName)
  if tableStatsOpt.isNone:
    return true
  let tableRows = max(1, tableStatsOpt.get.rowCount)
  let scanCost = float64(max(1, (tableRows + 99) div 100))
  let idxStatsOpt = catalog.getIndexStats(idxName)
  let sel =
    if idxStatsOpt.isSome and idxStatsOpt.get.distinctKeyCount > 0:
      1.0 / float64(idxStatsOpt.get.distinctKeyCount)
    else:
      0.10  # heuristic
  let seekRows = float64(tableRows) * sel
  let seekCost = 1.0 + ln(float64(max(1, tableRows))) / ln(2.0) + seekRows / 100.0
  seekCost < scanCost

proc makeJoinPlan(joinType: JoinType, joinOn: Expr, left, right: Plan): Plan =
  ## Construct a join plan, rewriting RIGHT JOIN as LEFT JOIN with swapped operands.
  ## For FULL OUTER JOIN, disable index seek since we need all right rows.
  if joinType == jtRight:
    Plan(kind: pkJoin, joinType: jtLeft, joinOn: joinOn, left: right, right: left)
  elif joinType == jtFull and right.kind == pkIndexSeek:
    let fallback = Plan(kind: pkTableScan, table: right.table, alias: right.alias)
    Plan(kind: pkJoin, joinType: jtFull, joinOn: joinOn, left: left, right: fallback)
  else:
    Plan(kind: pkJoin, joinType: joinType, joinOn: joinOn, left: left, right: right)

proc planSelect(catalog: Catalog, stmt: Statement, materializedCtes: Table[string, seq[seq[(string, Value)]]] = initTable[string, seq[seq[(string, Value)]]]()): Plan =
  if stmt.setOpKind == sokUnionAll:
    let leftPlan = planSelect(catalog, stmt.setOpLeft, materializedCtes)
    let rightPlan = planSelect(catalog, stmt.setOpRight, materializedCtes)
    return Plan(kind: pkAppend, left: leftPlan, right: rightPlan)
  if stmt.setOpKind == sokUnion:
    let leftPlan = planSelect(catalog, stmt.setOpLeft, materializedCtes)
    let rightPlan = planSelect(catalog, stmt.setOpRight, materializedCtes)
    return Plan(kind: pkSetUnionDistinct, left: leftPlan, right: rightPlan)
  if stmt.setOpKind == sokIntersect:
    let leftPlan = planSelect(catalog, stmt.setOpLeft, materializedCtes)
    let rightPlan = planSelect(catalog, stmt.setOpRight, materializedCtes)
    return Plan(kind: pkSetIntersect, left: leftPlan, right: rightPlan)
  if stmt.setOpKind == sokIntersectAll:
    let leftPlan = planSelect(catalog, stmt.setOpLeft, materializedCtes)
    let rightPlan = planSelect(catalog, stmt.setOpRight, materializedCtes)
    return Plan(kind: pkSetIntersectAll, left: leftPlan, right: rightPlan)
  if stmt.setOpKind == sokExcept:
    let leftPlan = planSelect(catalog, stmt.setOpLeft, materializedCtes)
    let rightPlan = planSelect(catalog, stmt.setOpRight, materializedCtes)
    return Plan(kind: pkSetExcept, left: leftPlan, right: rightPlan)
  if stmt.setOpKind == sokExceptAll:
    let leftPlan = planSelect(catalog, stmt.setOpLeft, materializedCtes)
    let rightPlan = planSelect(catalog, stmt.setOpRight, materializedCtes)
    return Plan(kind: pkSetExceptAll, left: leftPlan, right: rightPlan)

  proc exprHasAggregate(expr: Expr): bool =
    if expr == nil: return false
    case expr.kind
    of ekFunc:
      if expr.funcName.toUpperAscii() in ["COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "STRING_AGG", "TOTAL"]:
        return true
      for arg in expr.args:
        if exprHasAggregate(arg): return true
    of ekBinary:
      return exprHasAggregate(expr.left) or exprHasAggregate(expr.right)
    of ekUnary:
      return exprHasAggregate(expr.expr)
    of ekInList:
      if exprHasAggregate(expr.inExpr): return true
      for item in expr.inList:
        if exprHasAggregate(item): return true
    else: discard
    false
  proc hasAggregate(items: seq[SelectItem]): bool =
    for item in items:
      if exprHasAggregate(item.expr):
        return true
    false
  var conjuncts = splitAnd(stmt.whereExpr)
  var base: Plan = nil

  let tableRes = catalog.getTable(stmt.fromTable)
  # Planning should only run on bound statements, but keep a safe fallback.
  let tableMeta = if tableRes.ok: tableRes.value else: TableMeta()

  # When the FROM source is a subquery, recursively plan it and use as base.
  if stmt.fromSubquery != nil:
    let innerPlan = planSelect(catalog, stmt.fromSubquery, materializedCtes)
    base = Plan(kind: pkSubqueryScan, subPlan: innerPlan, table: stmt.fromTable, alias: stmt.fromAlias)
    # Apply WHERE filters.
    for c in conjuncts:
      base = Plan(kind: pkFilter, predicate: c, left: base)
    conjuncts = @[]
    # Build join plans (handling subquery join sources too).
    var available = initHashSet[string]()
    if stmt.fromAlias.len > 0:
      available.incl(stmt.fromAlias)
    available.incl(stmt.fromTable)
    for i, join in stmt.joins:
      var rightPlan: Plan = nil
      if i < stmt.joinSubqueries.len and stmt.joinSubqueries[i] != nil:
        let joinInner = planSelect(catalog, stmt.joinSubqueries[i], materializedCtes)
        rightPlan = Plan(kind: pkSubqueryScan, subPlan: joinInner, table: join.table, alias: join.alias)
      else:
        rightPlan = Plan(kind: pkTableScan, table: join.table, alias: join.alias)
      base = makeJoinPlan(join.joinType, join.onExpr, base, rightPlan)
      if join.alias.len > 0:
        available.incl(join.alias)
      available.incl(join.table)
    if stmt.groupBy.len > 0 or hasAggregate(stmt.selectItems):
      base = Plan(kind: pkAggregate, groupBy: stmt.groupBy, having: stmt.havingExpr, projections: stmt.selectItems, left: base)
    else:
      base = Plan(kind: pkProject, projections: stmt.selectItems, left: base)
    if stmt.orderBy.len > 0:
      base = Plan(kind: pkSort, orderBy: stmt.orderBy, left: base)
    if stmt.limit >= 0 or stmt.limitParam > 0 or stmt.offset >= 0 or stmt.offsetParam > 0:
      base = Plan(kind: pkLimit, limit: stmt.limit, limitParam: stmt.limitParam, offset: stmt.offset, offsetParam: stmt.offsetParam, left: base)
    return base

  # When the FROM source is a table-valued function.
  if stmt.fromTvfFunc.len > 0:
    base = Plan(kind: pkTvfScan, table: stmt.fromTable, alias: stmt.fromAlias, tvfFunc: stmt.fromTvfFunc, tvfArgs: stmt.fromTvfArgs)
    for c in conjuncts:
      base = Plan(kind: pkFilter, predicate: c, left: base)
    conjuncts = @[]
    if stmt.groupBy.len > 0 or hasAggregate(stmt.selectItems):
      base = Plan(kind: pkAggregate, groupBy: stmt.groupBy, having: stmt.havingExpr, projections: stmt.selectItems, left: base)
    else:
      base = Plan(kind: pkProject, projections: stmt.selectItems, left: base)
    if stmt.orderBy.len > 0:
      base = Plan(kind: pkSort, orderBy: stmt.orderBy, left: base)
    if stmt.limit >= 0 or stmt.limitParam > 0 or stmt.offset >= 0 or stmt.offsetParam > 0:
      base = Plan(kind: pkLimit, limit: stmt.limit, limitParam: stmt.limitParam, offset: stmt.offset, offsetParam: stmt.offsetParam, left: base)
    return base

  # When the FROM source is a materialized recursive CTE, use pre-computed rows.
  let cteKey = stmt.fromTable.toLowerAscii()
  if materializedCtes.hasKey(cteKey):
    base = Plan(kind: pkLiteralRows, table: stmt.fromTable, alias: stmt.fromAlias, rows: materializedCtes[cteKey])
    for c in conjuncts:
      base = Plan(kind: pkFilter, predicate: c, left: base)
    conjuncts = @[]
    var available = initHashSet[string]()
    if stmt.fromAlias.len > 0:
      available.incl(stmt.fromAlias)
    available.incl(stmt.fromTable)
    for i, join in stmt.joins:
      var rightPlan: Plan = nil
      if i < stmt.joinSubqueries.len and stmt.joinSubqueries[i] != nil:
        let joinInner = planSelect(catalog, stmt.joinSubqueries[i], materializedCtes)
        rightPlan = Plan(kind: pkSubqueryScan, subPlan: joinInner, table: join.table, alias: join.alias)
      else:
        rightPlan = Plan(kind: pkTableScan, table: join.table, alias: join.alias)
      base = makeJoinPlan(join.joinType, join.onExpr, base, rightPlan)
      if join.alias.len > 0:
        available.incl(join.alias)
      available.incl(join.table)
    if stmt.groupBy.len > 0 or hasAggregate(stmt.selectItems):
      base = Plan(kind: pkAggregate, groupBy: stmt.groupBy, having: stmt.havingExpr, projections: stmt.selectItems, left: base)
    else:
      base = Plan(kind: pkProject, projections: stmt.selectItems, left: base)
    if stmt.orderBy.len > 0:
      base = Plan(kind: pkSort, orderBy: stmt.orderBy, left: base)
    if stmt.limit >= 0 or stmt.limitParam > 0 or stmt.offset >= 0 or stmt.offsetParam > 0:
      base = Plan(kind: pkLimit, limit: stmt.limit, limitParam: stmt.limitParam, offset: stmt.offset, offsetParam: stmt.offsetParam, left: base)
    return base

  proc isRowidPkColumn(colName: string): bool =
    if not tableRes.ok:
      return false
    # A column is the rowid only when it is the SOLE INT64 primary key.
    # Composite PKs use a hash-based unique index; individual columns are
    # NOT rowid aliases even when they are INTEGER PRIMARY KEY.
    var pkCount = 0
    for col in tableMeta.columns:
      if col.primaryKey:
        inc pkCount
    if pkCount != 1:
      return false
    for col in tableMeta.columns:
      if col.name.toLowerAscii() == colName.toLowerAscii() and col.primaryKey and col.kind == ctInt64:
        return true
    false

  proc planSeekFromConjuncts(ds: seq[Expr]): Option[Plan] =
    var partBase: Plan = nil
    var accessIdx = -1

    for i, c in ds:
      var idxColumn = ""
      var idxValue: Expr = nil
      if isSimpleEqualityFor(c, stmt.fromTable, stmt.fromAlias, idxColumn, idxValue):
        if isRowidPkColumn(idxColumn):
          partBase = Plan(kind: pkRowidSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idxColumn, valueExpr: idxValue)
          accessIdx = i
          break
        let idxOpt = catalog.getBtreeIndexForColumn(stmt.fromTable, idxColumn)
        if isSome(idxOpt):
          if seekCheaperThanScan(catalog, stmt.fromTable, idxOpt.get.name):
            partBase = Plan(kind: pkIndexSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idxColumn, valueExpr: idxValue)
            accessIdx = i
            break
      if c != nil and c.kind == ekBinary and c.op == "=":
        let leftSql = normalizeIndexExprSql(exprToCanonicalSql(c.left), stmt.fromTable, stmt.fromAlias)
        let rightSql = normalizeIndexExprSql(exprToCanonicalSql(c.right), stmt.fromTable, stmt.fromAlias)
        for _, idx in catalog.indexes:
          if idx.table != stmt.fromTable or idx.kind != ikBtree or idx.columns.len != 1:
            continue
          if not idx.columns[0].startsWith(IndexExpressionPrefix):
            continue
          let idxExprSql = normalizeIndexExprSql(idx.columns[0][IndexExpressionPrefix.len .. ^1], stmt.fromTable, stmt.fromAlias)
          if leftSql == idxExprSql and refs(c.right).len == 0:
            partBase = Plan(kind: pkIndexSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idx.columns[0], valueExpr: c.right)
            accessIdx = i
            break
          if rightSql == idxExprSql and refs(c.left).len == 0:
            partBase = Plan(kind: pkIndexSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idx.columns[0], valueExpr: c.left)
            accessIdx = i
            break
        if partBase != nil:
          break

    if partBase == nil:
      for i, c in ds:
        var likeColumn = ""
        var likePattern: Expr = nil
        var ignoreInsensitive = false
        if isTrigramLikeFor(c, stmt.fromTable, stmt.fromAlias, likeColumn, likePattern, ignoreInsensitive):
          let idxOpt = catalog.getTrigramIndexForColumn(stmt.fromTable, likeColumn)
          if isSome(idxOpt):
            partBase = Plan(kind: pkTrigramSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: likeColumn, likeExpr: likePattern, likeInsensitive: ignoreInsensitive)
            accessIdx = i
            break

    if partBase == nil:
      return none(Plan)

    var rem = ds
    if accessIdx >= 0:
      rem.delete(accessIdx)
    for c in rem:
      partBase = Plan(kind: pkFilter, predicate: c, left: partBase)
    some(partBase)

  proc planDisjunct(d: Expr): Option[Plan] =
    let ds = splitAnd(d)

    var orIdx = -1
    for i, c in ds:
      if c != nil and c.kind == ekBinary and c.op.toUpperAscii() == "OR":
        orIdx = i
        break

    if orIdx < 0:
      return planSeekFromConjuncts(ds)

    let orExpr = ds[orIdx]
    let arms = splitOr(orExpr)
    if arms.len < 2:
      return none(Plan)

    var otherConj: seq[Expr] = @[]
    for i, c in ds:
      if i != orIdx:
        otherConj.add(c)

    var armPlans: seq[Plan] = @[]
    for arm in arms:
      var armConj = otherConj
      armConj.add(splitAnd(arm))
      let p = planSeekFromConjuncts(armConj)
      if p.isNone:
        return none(Plan)
      armPlans.add(p.get)

    var unionBase = Plan(kind: pkUnionDistinct, left: armPlans[0], right: armPlans[1])
    for i in 2 ..< armPlans.len:
      unionBase = Plan(kind: pkUnionDistinct, left: unionBase, right: armPlans[i])
    some(unionBase)

  proc planOrExpr(orExpr: Expr): Option[Plan] =
    if orExpr == nil or orExpr.kind != ekBinary or orExpr.op.toUpperAscii() != "OR":
      return none(Plan)
    let disjuncts = splitOr(orExpr)
    if disjuncts.len < 2:
      return none(Plan)
    var parts: seq[Plan] = @[]
    for d in disjuncts:
      let p = planDisjunct(d)
      if p.isNone:
        return none(Plan)
      parts.add(p.get)
    var unionBase = Plan(kind: pkUnionDistinct, left: parts[0], right: parts[1])
    for i in 2 ..< parts.len:
      unionBase = Plan(kind: pkUnionDistinct, left: unionBase, right: parts[i])
    some(unionBase)

  # OR-planning: if the WHERE is a top-level OR of disjuncts, and every disjunct
  # can use an indexable access path, plan it as a UNION DISTINCT of seeks.
  # This preserves semantics but avoids a full table scan for OR-heavy predicates.
  if stmt.joins.len == 0 and stmt.whereExpr != nil:
    let disjuncts = splitOr(stmt.whereExpr)
    if disjuncts.len > 1:
      var parts: seq[Plan] = @[]
      var allSeek = true
      for d in disjuncts:
        let p = planDisjunct(d)
        if p.isNone:
          allSeek = false
          break
        parts.add(p.get)

      if allSeek and parts.len >= 2:
        var unionBase = Plan(kind: pkUnionDistinct, left: parts[0], right: parts[1])
        for i in 2 ..< parts.len:
          unionBase = Plan(kind: pkUnionDistinct, left: unionBase, right: parts[i])

        var base = unionBase
        if stmt.groupBy.len > 0 or hasAggregate(stmt.selectItems):
          base = Plan(kind: pkAggregate, groupBy: stmt.groupBy, having: stmt.havingExpr, projections: stmt.selectItems, left: base)
        else:
          base = Plan(kind: pkProject, projections: stmt.selectItems, left: base)
        if stmt.orderBy.len > 0:
          base = Plan(kind: pkSort, orderBy: stmt.orderBy, left: base)
        if stmt.limit >= 0 or stmt.limitParam > 0 or stmt.offset >= 0 or stmt.offsetParam > 0:
          base = Plan(kind: pkLimit, limit: stmt.limit, limitParam: stmt.limitParam, offset: stmt.offset, offsetParam: stmt.offsetParam, left: base)
        return base

  # Choose the best access path for the FROM table from any conjunct.
  var accessConjunctIdx = -1
  for i, c in conjuncts:
    var idxColumn = ""
    var idxValue: Expr = nil
    if isSimpleEqualityFor(c, stmt.fromTable, stmt.fromAlias, idxColumn, idxValue):
      # INT64 PRIMARY KEY is stored as the table rowid (no secondary index).
      if isRowidPkColumn(idxColumn):
        base = Plan(kind: pkRowidSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idxColumn, valueExpr: idxValue)
        accessConjunctIdx = i
        break
      let idxOpt = catalog.getBtreeIndexForColumn(stmt.fromTable, idxColumn)
      if isSome(idxOpt):
        if seekCheaperThanScan(catalog, stmt.fromTable, idxOpt.get.name):
          base = Plan(kind: pkIndexSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idxColumn, valueExpr: idxValue)
          accessConjunctIdx = i
          break
    if c != nil and c.kind == ekBinary and c.op == "=":
      let leftSql = normalizeIndexExprSql(exprToCanonicalSql(c.left), stmt.fromTable, stmt.fromAlias)
      let rightSql = normalizeIndexExprSql(exprToCanonicalSql(c.right), stmt.fromTable, stmt.fromAlias)
      for _, idx in catalog.indexes:
        if idx.table != stmt.fromTable or idx.kind != ikBtree or idx.columns.len != 1:
          continue
        if not idx.columns[0].startsWith(IndexExpressionPrefix):
          continue
        let idxExprSql = normalizeIndexExprSql(idx.columns[0][IndexExpressionPrefix.len .. ^1], stmt.fromTable, stmt.fromAlias)
        if leftSql == idxExprSql and refs(c.right).len == 0:
          base = Plan(kind: pkIndexSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idx.columns[0], valueExpr: c.right)
          accessConjunctIdx = i
          break
        if rightSql == idxExprSql and refs(c.left).len == 0:
          base = Plan(kind: pkIndexSeek, table: stmt.fromTable, alias: stmt.fromAlias, column: idx.columns[0], valueExpr: c.left)
          accessConjunctIdx = i
          break
      if base != nil:
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
  if base == nil:
    for i, c in conjuncts:
      let p = planOrExpr(c)
      if p.isSome:
        base = p.get
        accessConjunctIdx = i
        break
  if accessConjunctIdx >= 0:
    conjuncts.delete(accessConjunctIdx)
  if base == nil:
    if stmt.fromTable.len == 0:
      base = Plan(kind: pkOneRow)
    else:
      base = Plan(kind: pkTableScan, table: stmt.fromTable, alias: stmt.fromAlias)

  # Predicate pushdown: apply conjuncts as early as possible (once referenced
  # tables are available), rather than applying the full WHERE before joins.
  var available = initHashSet[string]()
  if stmt.fromAlias.len > 0:
    available.incl(stmt.fromAlias)
  available.incl(stmt.fromTable)

  # Join reordering: for queries with only inner joins and all base table sources,
  # choose a left-deep join order by dynamic programming over join permutations.
  #
  # IMPORTANT: we only consider join orders where each JOIN's ON predicate references
  # tables that are already available at that point (plus the join table itself).
  # This preserves semantics for ON predicates that reference previously-joined tables.
  # LEFT/FULL/RIGHT joins are never reordered to preserve NULL semantics.
  # Restricted to ≤ 6 tables total (FROM + joins) to bound the permutation search.
  const joinReorderMaxTables = 6
  var effectiveJoins = stmt.joins  # may be replaced with reordered version
  var effectiveJoinSubqueries = stmt.joinSubqueries
  block joinReorderBlock:
    if stmt.joins.len == 0 or stmt.joins.len + 1 > joinReorderMaxTables:
      break joinReorderBlock
    # Only reorder if all joins are inner joins with no subquery sources.
    var allInner = true
    for j in stmt.joins:
      if j.joinType != jtInner:
        allInner = false
        break
    if not allInner:
      break joinReorderBlock
    for j in stmt.joins:
      if j.isNatural:
        break joinReorderBlock
    var hasSubquery = false
    for k in 0 ..< stmt.joins.len:
      if k < stmt.joinSubqueries.len and stmt.joinSubqueries[k] != nil:
        hasSubquery = true
        break
    if hasSubquery:
      break joinReorderBlock
    # Also skip if any join source is a recursive CTE materialized table.
    var hasCte = false
    for j in stmt.joins:
      if materializedCtes.hasKey(j.table.toLowerAscii()):
        hasCte = true
        break
    if hasCte:
      break joinReorderBlock

    proc exprHasUnqualifiedColumn(expr: Expr): bool =
      if expr == nil:
        return false
      case expr.kind
      of ekColumn:
        return expr.table.len == 0
      of ekBinary:
        return exprHasUnqualifiedColumn(expr.left) or exprHasUnqualifiedColumn(expr.right)
      of ekUnary:
        return exprHasUnqualifiedColumn(expr.expr)
      of ekFunc:
        for a in expr.args:
          if exprHasUnqualifiedColumn(a):
            return true
        return false
      of ekInList:
        if exprHasUnqualifiedColumn(expr.inExpr):
          return true
        for item in expr.inList:
          if exprHasUnqualifiedColumn(item):
            return true
        return false
      of ekWindowRowNumber:
        for part in expr.windowPartitions:
          if exprHasUnqualifiedColumn(part):
            return true
        for o in expr.windowOrderExprs:
          if exprHasUnqualifiedColumn(o):
            return true
        for a in expr.windowArgs:
          if exprHasUnqualifiedColumn(a):
            return true
        return false
      else:
        return false

    # Be conservative: skip reordering if any ON predicate uses unqualified columns.
    for j in stmt.joins:
      if exprHasUnqualifiedColumn(j.onExpr):
        break joinReorderBlock

    proc tableEst(tname: string): int64 =
      let st = catalog.getTableStats(tname)
      if st.isSome: max(1, st.get.rowCount) else: 1000'i64

    const dpRowsPerPage = 100
    const dpJoinSel = 0.10

    proc scanCost(rows: int64): float64 =
      float64(max(1, (rows + dpRowsPerPage - 1) div dpRowsPerPage))

    proc seekProbeCost(tableRows: int64): float64 =
      1.0 + ln(float64(max(1, tableRows))) / ln(2.0)

    let fromKey = if stmt.fromAlias.len > 0: stmt.fromAlias else: stmt.fromTable
    let n = stmt.joins.len

    # Precompute ON dependencies for each join clause.
    var deps: seq[HashSet[string]] = newSeq[HashSet[string]](n)
    for i, j in stmt.joins:
      deps[i] = refs(j.onExpr)
      # ON clauses may refer to the join table via either table name or alias.
      if j.table.len > 0:
        deps[i].excl(j.table)
      if j.alias.len > 0:
        deps[i].excl(j.alias)

    # DP over subsets of joins: dpCost[mask] is the cheapest estimated cost so far.
    let fullMask = (1 shl n) - 1
    var dpCost = newSeq[float64](1 shl n)
    var dpRows = newSeq[int64](1 shl n)
    var prevMask = newSeq[int](1 shl n)
    var chosenJoinIdx = newSeq[int](1 shl n)
    for m in 0 .. fullMask:
      dpCost[m] = Inf
      prevMask[m] = -1
      chosenJoinIdx[m] = -1
      dpRows[m] = 0
    dpCost[0] = 0.0
    dpRows[0] = tableEst(stmt.fromTable)

    proc buildAvailable(mask: int): HashSet[string] =
      result = initHashSet[string]()
      if stmt.fromTable.len > 0:
        result.incl(stmt.fromTable)
      if stmt.fromAlias.len > 0:
        result.incl(stmt.fromAlias)
      result.incl(fromKey)
      for i in 0 ..< n:
        if (mask and (1 shl i)) != 0:
          let j = stmt.joins[i]
          if j.table.len > 0:
            result.incl(j.table)
          if j.alias.len > 0:
            result.incl(j.alias)

    proc joinProbeCost(join: JoinClause, avail: HashSet[string]): float64 =
      ## Estimate per-outer-row cost of probing the right side.
      let trows = tableEst(join.table)
      var col = ""
      var valExpr: Expr = nil
      if join.onExpr != nil and isSimpleEqualityFor(join.onExpr, join.table, join.alias, col, valExpr):
        # Only treat as a correlated seek if the value expression depends only
        # on already-available tables.
        let r = refs(valExpr)
        if (r <= avail) and (not r.contains(join.table)) and (join.alias.len == 0 or not r.contains(join.alias)):
          let idxOpt = catalog.getBtreeIndexForColumn(join.table, col)
          if idxOpt.isSome and seekCheaperThanScan(catalog, join.table, idxOpt.get.name):
            return seekProbeCost(trows)
      scanCost(trows)

    for mask in 0 .. fullMask:
      if dpCost[mask] == Inf:
        continue
      let avail = buildAvailable(mask)
      for i in 0 ..< n:
        if (mask and (1 shl i)) != 0:
          continue
        if not (deps[i] <= avail):
          continue
        let j = stmt.joins[i]
        let probe = joinProbeCost(j, avail)
        let outerRows = max(1'i64, dpRows[mask])
        let newCost = dpCost[mask] + float64(outerRows) * probe
        let rightRows = tableEst(j.table)
        let newRowsF = min(float64(high(int64)), float64(outerRows) * float64(rightRows) * dpJoinSel)
        let newRows = max(1'i64, int64(newRowsF))
        let newMask = mask or (1 shl i)
        if newCost < dpCost[newMask]:
          dpCost[newMask] = newCost
          dpRows[newMask] = newRows
          prevMask[newMask] = mask
          chosenJoinIdx[newMask] = i

    if dpCost[fullMask] == Inf:
      break joinReorderBlock

    # Reconstruct best join order.
    var orderIdxs: seq[int] = @[]
    var m = fullMask
    while m != 0:
      let jIdx = chosenJoinIdx[m]
      if jIdx < 0:
        break
      orderIdxs.add(jIdx)
      m = prevMask[m]
    orderIdxs.reverse()
    if orderIdxs.len != n:
      break joinReorderBlock

    var reordered: seq[JoinClause] = @[]
    var reorderedSubqueries: seq[Statement] = @[]
    for jIdx in orderIdxs:
      reordered.add(stmt.joins[jIdx])
      if jIdx < stmt.joinSubqueries.len:
        reorderedSubqueries.add(stmt.joinSubqueries[jIdx])
      else:
        reorderedSubqueries.add(nil)
    effectiveJoins = reordered
    effectiveJoinSubqueries = reorderedSubqueries

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
  for i, join in effectiveJoins:
    var rightPlan: Plan = nil
    if i < effectiveJoinSubqueries.len and effectiveJoinSubqueries[i] != nil:
      let joinInner = planSelect(catalog, effectiveJoinSubqueries[i], materializedCtes)
      rightPlan = Plan(kind: pkSubqueryScan, subPlan: joinInner, table: join.table, alias: join.alias)
    else:
      # Check if the join table is a materialized recursive CTE.
      let joinCteKey = join.table.toLowerAscii()
      if materializedCtes.hasKey(joinCteKey):
        rightPlan = Plan(kind: pkLiteralRows, table: join.table, alias: join.alias, rows: materializedCtes[joinCteKey])
      else:
        var joinIdxCol = ""
        var joinIdxVal: Expr = nil
        if isSimpleEqualityFor(join.onExpr, join.table, join.alias, joinIdxCol, joinIdxVal):
          let idxOpt = catalog.getBtreeIndexForColumn(join.table, joinIdxCol)
          if isSome(idxOpt):
            rightPlan = Plan(kind: pkIndexSeek, table: join.table, alias: join.alias, column: joinIdxCol, valueExpr: joinIdxVal)
        if rightPlan == nil:
          rightPlan = Plan(kind: pkTableScan, table: join.table, alias: join.alias)
    base = makeJoinPlan(join.joinType, join.onExpr, base, rightPlan)
    if join.alias.len > 0:
      available.incl(join.alias)
    available.incl(join.table)
    applyEligible()

  # Apply any remaining conjuncts after all joins are present.
  for c in conjuncts:
    base = Plan(kind: pkFilter, predicate: c, left: base)
  if stmt.groupBy.len > 0 or hasAggregate(stmt.selectItems):
    base = Plan(kind: pkAggregate, groupBy: stmt.groupBy, having: stmt.havingExpr, projections: stmt.selectItems, left: base)
    if stmt.orderBy.len > 0:
      # Rewrite ORDER BY aggregate expressions to column references so the
      # sort evaluates against materialized aggregate output columns instead
      # of trying to re-evaluate the aggregate function (which would fail).
      var rewrittenOrder: seq[OrderItem] = @[]
      for item in stmt.orderBy:
        if exprHasAggregate(item.expr):
          var colName = ""
          for si in stmt.selectItems:
            if exprToCanonicalSql(si.expr) == exprToCanonicalSql(item.expr):
              if si.alias.len > 0:
                colName = si.alias
              elif si.expr != nil and si.expr.kind == ekFunc:
                colName = si.expr.funcName.toLowerAscii()
              break
          if colName.len > 0:
            rewrittenOrder.add(OrderItem(
              expr: Expr(kind: ekColumn, name: colName, table: ""),
              asc: item.asc))
          else:
            rewrittenOrder.add(item)
        else:
          rewrittenOrder.add(item)
      base = Plan(kind: pkSort, orderBy: rewrittenOrder, left: base)
  else:
    # Place Sort before Project so that ORDER BY sees the original column
    # names (not aliases) and expensive projections (e.g. scalar subqueries)
    # run only on the sorted (and possibly limited) result set.
    if stmt.orderBy.len > 0:
      base = Plan(kind: pkSort, orderBy: stmt.orderBy, left: base)
    base = Plan(kind: pkProject, projections: stmt.selectItems, left: base)
  if stmt.limit >= 0 or stmt.limitParam > 0 or stmt.offset >= 0 or stmt.offsetParam > 0:
    base = Plan(kind: pkLimit, limit: stmt.limit, limitParam: stmt.limitParam, offset: stmt.offset, offsetParam: stmt.offsetParam, left: base)
  base

proc plan*(catalog: Catalog, stmt: Statement): Result[Plan] =
  if stmt.kind == skSelect:
    return ok(planSelect(catalog, stmt))
  ok(Plan(kind: pkStatement, stmt: stmt))

proc plan*(catalog: Catalog, stmt: Statement, materializedCtes: Table[string, seq[seq[(string, Value)]]]): Result[Plan] =
  if stmt.kind == skSelect:
    return ok(planSelect(catalog, stmt, materializedCtes))
  ok(Plan(kind: pkStatement, stmt: stmt))

# ---------------------------------------------------------------------------
# Cost annotation pass
# ---------------------------------------------------------------------------
# Selectivity heuristics (when stats not available or predicate unanalyzed)
const selEqualityHeuristic = 0.10
const selRangeHeuristic = 0.30
const selLikeHeuristic = 0.05
const rowsPerPage = 100
const heuristicRowCount: int64 = 1000

proc estimateSelectivity(catalog: Catalog, table: string, predicate: Expr): float64 =
  ## Return a selectivity estimate for a predicate on `table`.
  if predicate == nil:
    return 1.0
  case predicate.kind
  of ekBinary:
    let op = predicate.op.toUpperAscii()
    if op == "AND":
      let l = estimateSelectivity(catalog, table, predicate.left)
      let r = estimateSelectivity(catalog, table, predicate.right)
      return l * r
    if op == "OR":
      let l = estimateSelectivity(catalog, table, predicate.left)
      let r = estimateSelectivity(catalog, table, predicate.right)
      return l + r - l * r
    if op == "=":
      # Try to find an index for selectivity via distinct key count.
      var colName = ""
      var valExpr: Expr = nil
      if predicate.left.kind == ekColumn:
        colName = predicate.left.name
      elif predicate.right.kind == ekColumn:
        colName = predicate.right.name
      if colName.len > 0:
        let idxOpt = catalog.getBtreeIndexForColumn(table, colName)
        if idxOpt.isSome:
          let statsOpt = catalog.getIndexStats(idxOpt.get.name)
          if statsOpt.isSome and statsOpt.get.distinctKeyCount > 0:
            return 1.0 / float64(statsOpt.get.distinctKeyCount)
      return selEqualityHeuristic
    if op in [">", "<", ">=", "<="]:
      return selRangeHeuristic
    if op in ["LIKE", "ILIKE"]:
      return selLikeHeuristic
    return 1.0
  else:
    return 1.0

proc annotatePlan*(catalog: Catalog, p: Plan): void =
  ## Fill in estRows and estCost fields for each plan node bottom-up.
  if p == nil:
    return
  # Recurse into children first.
  annotatePlan(catalog, p.left)
  annotatePlan(catalog, p.right)
  annotatePlan(catalog, p.subPlan)

  case p.kind
  of pkOneRow:
    p.estRows = 1
    p.estCost = 1.0
  of pkTableScan:
    let statsOpt = catalog.getTableStats(p.table)
    p.estRows = if statsOpt.isSome: max(1, statsOpt.get.rowCount) else: heuristicRowCount
    p.estCost = float64(max(1, (p.estRows + rowsPerPage - 1) div rowsPerPage))
  of pkRowidSeek:
    p.estRows = 1
    let tableRows = block:
      let st = catalog.getTableStats(p.table)
      if st.isSome: max(1, st.get.rowCount) else: heuristicRowCount
    p.estCost = 1.0 + float64(max(1, tableRows)).ln() / float64(2).ln()
  of pkIndexSeek:
    let tableRowsOpt = catalog.getTableStats(p.table)
    let tableRows = if tableRowsOpt.isSome: max(1, tableRowsOpt.get.rowCount) else: heuristicRowCount
    let sel = block:
      let col = p.column
      let idxOpt = catalog.getBtreeIndexForColumn(p.table, col)
      if idxOpt.isSome:
        let statsOpt = catalog.getIndexStats(idxOpt.get.name)
        if statsOpt.isSome and statsOpt.get.distinctKeyCount > 0:
          1.0 / float64(statsOpt.get.distinctKeyCount)
        else:
          selEqualityHeuristic
      else:
        selEqualityHeuristic
    p.estRows = max(1, int64(float64(tableRows) * sel))
    p.estCost = 1.0 + float64(max(1, tableRows)).ln() / float64(2).ln()
  of pkTrigramSeek:
    p.estRows = 10  # conservative estimate for trigram seek
    p.estCost = 10.0
  of pkFilter:
    let inputRows = if p.left != nil: p.left.estRows else: heuristicRowCount
    let inputCost = if p.left != nil: p.left.estCost else: 0.0
    let sel = estimateSelectivity(catalog, p.left.table, p.predicate)
    p.estRows = max(1, int64(float64(inputRows) * sel))
    p.estCost = inputCost
  of pkProject:
    p.estRows = if p.left != nil: p.left.estRows else: 1
    p.estCost = if p.left != nil: p.left.estCost else: 0.0
  of pkJoin:
    let leftRows = if p.left != nil: p.left.estRows else: heuristicRowCount
    let rightRows = if p.right != nil: p.right.estRows else: heuristicRowCount
    let leftCost = if p.left != nil: p.left.estCost else: 0.0
    let rightCost = if p.right != nil: p.right.estCost else: 0.0
    p.estRows = max(1, int64(float64(leftRows) * float64(rightRows) * selEqualityHeuristic))
    p.estCost = leftCost + float64(leftRows) * rightCost
  of pkSort:
    let inputRows = if p.left != nil: p.left.estRows else: 1
    let inputCost = if p.left != nil: p.left.estCost else: 0.0
    p.estRows = inputRows
    let sortFactor = float64(max(inputRows, 2)).ln() / float64(2).ln() / 100.0
    p.estCost = inputCost + float64(inputRows) * sortFactor
  of pkAggregate:
    p.estRows = max(1, if p.left != nil: p.left.estRows div 10 else: 1)
    p.estCost = if p.left != nil: p.left.estCost else: 0.0
  of pkLimit:
    let inputRows = if p.left != nil: p.left.estRows else: heuristicRowCount
    let inputCost = if p.left != nil: p.left.estCost else: 0.0
    let limitRows = if p.limit >= 0: int64(p.limit) else: inputRows
    p.estRows = min(limitRows, inputRows)
    p.estCost = if inputRows > 0: inputCost * float64(p.estRows) / float64(inputRows) else: inputCost
  of pkSubqueryScan:
    p.estRows = if p.subPlan != nil: p.subPlan.estRows else: heuristicRowCount
    p.estCost = if p.subPlan != nil: p.subPlan.estCost else: 0.0
  of pkLiteralRows:
    p.estRows = int64(p.rows.len)
    p.estCost = float64(p.rows.len) / float64(rowsPerPage)
  of pkUnionDistinct, pkSetUnionDistinct:
    let l = if p.left != nil: p.left.estRows else: 0
    let r = if p.right != nil: p.right.estRows else: 0
    p.estRows = l + r
    p.estCost = (if p.left != nil: p.left.estCost else: 0.0) + (if p.right != nil: p.right.estCost else: 0.0)
  of pkAppend:
    let l = if p.left != nil: p.left.estRows else: 0
    let r = if p.right != nil: p.right.estRows else: 0
    p.estRows = l + r
    p.estCost = (if p.left != nil: p.left.estCost else: 0.0) + (if p.right != nil: p.right.estCost else: 0.0)
  of pkSetIntersect, pkSetIntersectAll:
    let l = if p.left != nil: p.left.estRows else: 0
    p.estRows = max(1, l div 2)
    p.estCost = (if p.left != nil: p.left.estCost else: 0.0) + (if p.right != nil: p.right.estCost else: 0.0)
  of pkSetExcept, pkSetExceptAll:
    let l = if p.left != nil: p.left.estRows else: 0
    p.estRows = max(1, l div 2)
    p.estCost = (if p.left != nil: p.left.estCost else: 0.0) + (if p.right != nil: p.right.estCost else: 0.0)
  of pkTvfScan:
    p.estRows = heuristicRowCount
    p.estCost = float64(heuristicRowCount div rowsPerPage)
  of pkStatement:
    p.estRows = 0
    p.estCost = 0.0

proc planWithStats*(catalog: Catalog, stmt: Statement): Result[Plan] =
  ## Plan a statement and annotate the tree with cost estimates.
  let res = plan(catalog, stmt)
  if res.ok:
    annotatePlan(catalog, res.value)
  res

proc planWithStats*(catalog: Catalog, stmt: Statement, materializedCtes: Table[string, seq[seq[(string, Value)]]]): Result[Plan] =
  let res = plan(catalog, stmt, materializedCtes)
  if res.ok:
    annotatePlan(catalog, res.value)
  res
