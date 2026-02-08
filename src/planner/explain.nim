import strutils, sequtils, options, tables
import ../sql/sql
import ../catalog/catalog
import ./planner

type
  PlanMetrics* = object
    actualRows*: int
    actualTimeMs*: float64

proc renderExpr*(expr: Expr): string

proc renderSelectItem*(item: SelectItem): string =
  if item.isStar:
    result = "*"
  else:
    result = renderExpr(item.expr)
  if item.alias.len > 0:
    result.add(" AS " & item.alias)

proc renderOrderItem*(item: OrderItem): string =
  result = renderExpr(item.expr)
  if item.asc:
    result.add(" ASC")
  else:
    result.add(" DESC")

proc renderExpr*(expr: Expr): string =
  if expr == nil:
    return "<nil>"
  case expr.kind
  of ekLiteral:
    case expr.value.kind
    of svNull: "NULL"
    of svInt: $expr.value.intVal
    of svFloat: $expr.value.floatVal
    of svBool: $expr.value.boolVal
    of svString: "'" & expr.value.strVal.replace("'", "''") & "'"
    of svParam: "$" & $expr.value.paramIndex
  of ekColumn:
    if expr.table.len > 0:
      expr.table & "." & expr.name
    else:
      expr.name
  of ekBinary:
    "(" & renderExpr(expr.left) & " " & expr.op & " " & renderExpr(expr.right) & ")"
  of ekUnary:
    "(" & expr.unOp & " " & renderExpr(expr.expr) & ")"
  of ekFunc:
    var s = expr.funcName & "("
    if expr.isStar:
      s.add("*")
    else:
      for i, arg in expr.args:
        if i > 0: s.add(", ")
        s.add(renderExpr(arg))
    s.add(")")
    s
  of ekParam:
    "$" & $expr.index
  of ekInList:
    var s = "(" & renderExpr(expr.inExpr) & " IN ("
    for i, item in expr.inList:
      if i > 0: s.add(", ")
      s.add(renderExpr(item))
    s.add("))")
    s
  of ekWindowRowNumber:
    var s = "ROW_NUMBER() OVER ("
    if expr.windowPartitions.len > 0:
      s.add("PARTITION BY ")
      for i, p in expr.windowPartitions:
        if i > 0: s.add(", ")
        s.add(renderExpr(p))
      if expr.windowOrderExprs.len > 0:
        s.add(" ")
    if expr.windowOrderExprs.len > 0:
      s.add("ORDER BY ")
      for i, o in expr.windowOrderExprs:
        if i > 0: s.add(", ")
        s.add(renderExpr(o))
        let asc = if i < expr.windowOrderAsc.len: expr.windowOrderAsc[i] else: true
        s.add(if asc: " ASC" else: " DESC")
    s.add(")")
    s

proc explainPlanLines*(catalog: Catalog, plan: Plan): seq[string] =
  var lines: seq[string] = @[]
  
  proc traverse(p: Plan, depth: int) =
    if p == nil: return
    let indent = repeat("  ", depth)
    var line = indent
    
    case p.kind
    of pkOneRow:
      line.add("OneRow")
      lines.add(line)
    of pkTableScan:
      line.add("TableScan(table=" & p.table & " alias=" & p.alias & ")")
      lines.add(line)
    of pkRowidSeek:
      line.add("RowidSeek(table=" & p.table & " alias=" & p.alias & " column=" & p.column & " value=" & renderExpr(p.valueExpr) & ")")
      lines.add(line)
    of pkIndexSeek:
      var idxName = "?"
      if p.column.startsWith(IndexExpressionPrefix):
        for _, idx in catalog.indexes:
          if idx.table == p.table and idx.kind == ikBtree and idx.columns.len == 1 and idx.columns[0] == p.column:
            idxName = idx.name
            break
      else:
        let idxOpt = catalog.getBtreeIndexForColumn(p.table, p.column)
        if idxOpt.isSome:
          idxName = idxOpt.get.name
      let colDisplay =
        if p.column.startsWith(IndexExpressionPrefix):
          p.column[IndexExpressionPrefix.len .. ^1]
        else:
          p.column
      line.add("IndexSeek(table=" & p.table & " column=" & colDisplay & " value=" & renderExpr(p.valueExpr) & " index=" & idxName & ")")
      lines.add(line)
    of pkTrigramSeek:
      let idxOpt = catalog.getTrigramIndexForColumn(p.table, p.column)
      let idxName = if idxOpt.isSome: idxOpt.get.name else: "?"
      line.add("TrigramSeek(table=" & p.table & " column=" & p.column & " pattern=" & renderExpr(p.likeExpr) & " insensitive=" & $p.likeInsensitive & " index=" & idxName & ")")
      lines.add(line)
    of pkUnionDistinct:
      line.add("UnionDistinct")
      lines.add(line)
      traverse(p.left, depth + 1)
      traverse(p.right, depth + 1)
    of pkAppend:
      line.add("Append")
      lines.add(line)
      traverse(p.left, depth + 1)
      traverse(p.right, depth + 1)
    of pkSetUnionDistinct:
      line.add("SetUnionDistinct")
      lines.add(line)
      traverse(p.left, depth + 1)
      traverse(p.right, depth + 1)
    of pkSetIntersect:
      line.add("SetIntersect")
      lines.add(line)
      traverse(p.left, depth + 1)
      traverse(p.right, depth + 1)
    of pkSetExcept:
      line.add("SetExcept")
      lines.add(line)
      traverse(p.left, depth + 1)
      traverse(p.right, depth + 1)
    of pkFilter:
      line.add("Filter(predicate=" & renderExpr(p.predicate) & ")")
      lines.add(line)
      traverse(p.left, depth + 1)
    of pkProject:
      let proj = p.projections.mapIt(renderSelectItem(it)).join(", ")
      line.add("Project(projections=" & proj & ")")
      lines.add(line)
      traverse(p.left, depth + 1)
    of pkJoin:
      let jType = if p.joinType == jtInner: "INNER" else: "LEFT"
      line.add("Join(type=" & jType & " on=" & renderExpr(p.joinOn) & ")")
      lines.add(line)
      traverse(p.left, depth + 1)
      traverse(p.right, depth + 1)
    of pkSort:
      let orders = p.orderBy.mapIt(renderOrderItem(it)).join(", ")
      line.add("Sort(orderBy=" & orders & ")")
      lines.add(line)
      traverse(p.left, depth + 1)
    of pkAggregate:
      let gb = p.groupBy.mapIt(renderExpr(it)).join(", ")
      let hav = renderExpr(p.having)
      let projs = p.projections.mapIt(renderSelectItem(it)).join(", ")
      line.add("Aggregate(groupBy=" & gb & " having=" & hav & " projections=" & projs & ")")
      lines.add(line)
      traverse(p.left, depth + 1)
    of pkLimit:
      var l = "limit=" & (if p.limitParam > 0: "$" & $p.limitParam else: $p.limit)
      var o = "offset=" & (if p.offsetParam > 0: "$" & $p.offsetParam else: $p.offset)
      line.add("Limit(" & l & " " & o & ")")
      lines.add(line)
      traverse(p.left, depth + 1)
    of pkStatement:
      line.add("Statement(kind=" & $p.stmt.kind & ")")
      lines.add(line)

  traverse(plan, 0)
  result = lines

proc explainAnalyzePlanLines*(catalog: Catalog, plan: Plan, metrics: PlanMetrics): seq[string] =
  ## Render EXPLAIN ANALYZE output: plan lines followed by actual execution metrics.
  result = explainPlanLines(catalog, plan)
  result.add("---")
  result.add("Actual Rows: " & $metrics.actualRows)
  result.add("Actual Time: " & formatFloat(metrics.actualTimeMs, ffDecimal, 3) & " ms")
