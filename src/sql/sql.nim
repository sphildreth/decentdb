import tables
import json
import strutils
import ../errors
import ./pg_query_ffi

type SqlValueKind* = enum
  svNull
  svInt
  svFloat
  svString
  svBool
  svParam

type SqlValue* = object
  kind*: SqlValueKind
  intVal*: int64
  floatVal*: float64
  strVal*: string
  boolVal*: bool
  paramIndex*: int

type ExprKind* = enum
  ekLiteral
  ekColumn
  ekBinary
  ekUnary
  ekFunc
  ekParam
  ekInList

type Expr* = ref object
  case kind*: ExprKind
  of ekLiteral:
    value*: SqlValue
  of ekColumn:
    table*: string
    name*: string
  of ekBinary:
    op*: string
    left*: Expr
    right*: Expr
  of ekUnary:
    unOp*: string
    expr*: Expr
  of ekFunc:
    funcName*: string
    args*: seq[Expr]
    isStar*: bool
  of ekParam:
    index*: int
  of ekInList:
    inExpr*: Expr
    inList*: seq[Expr]

type ColumnDef* = object
  name*: string
  typeName*: string
  notNull*: bool
  unique*: bool
  primaryKey*: bool
  refTable*: string
  refColumn*: string

type SqlIndexKind* = enum
  ikBtree
  ikTrigram

type OrderItem* = object
  expr*: Expr
  asc*: bool

type JoinType* = enum
  jtInner
  jtLeft

type JoinClause* = object
  joinType*: JoinType
  table*: string
  alias*: string
  onExpr*: Expr

type SelectItem* = object
  expr*: Expr
  alias*: string
  isStar*: bool

type AlterTableActionKind* = enum
  ataAddColumn
  ataDropColumn
  ataRenameColumn
  ataRenameTable
  ataAlterColumn

type AlterColumnAction* = enum
  acaSetType
  acaSetDefault
  acaDropDefault
  acaSetNotNull
  acaDropNotNull

type InsertConflictAction* = enum
  icaNone
  icaDoNothing
  icaDoUpdate

type AlterTableAction* = object
  kind*: AlterTableActionKind
  columnDef*: ColumnDef        # For ADD COLUMN
  columnName*: string          # For DROP/RENAME COLUMN
  newColumnName*: string       # For RENAME COLUMN
  newTableName*: string        # For RENAME TABLE
  alterColumnAction*: AlterColumnAction  # For ALTER COLUMN
  alterColumnNewType*: string  # For ALTER COLUMN SET TYPE
  alterColumnDefault*: Expr    # For ALTER COLUMN SET DEFAULT

type StatementKind* = enum
  skCreateTable
  skCreateIndex
  skDropTable
  skDropIndex
  skCreateView
  skDropView
  skAlterView
  skAlterTable
  skInsert
  skSelect
  skUpdate
  skDelete
  skBegin
  skCommit
  skRollback
  skExplain

type Statement* = ref object
  case kind*: StatementKind
  of skExplain:
    explainInner*: Statement
    explainHasOptions*: bool
  of skCreateTable:
    createTableName*: string
    columns*: seq[ColumnDef]
  of skCreateIndex:
    indexName*: string
    indexTableName*: string
    columnNames*: seq[string]
    indexKind*: SqlIndexKind
    unique*: bool
  of skDropTable:
    dropTableName*: string
  of skDropIndex:
    dropIndexName*: string
  of skCreateView:
    createViewName*: string
    createViewIfNotExists*: bool
    createViewOrReplace*: bool
    createViewColumns*: seq[string]
    createViewSqlText*: string
    createViewQuery*: Statement
  of skDropView:
    dropViewName*: string
    dropViewIfExists*: bool
  of skAlterView:
    alterViewName*: string
    alterViewNewName*: string
  of skAlterTable:
    alterTableName*: string
    alterActions*: seq[AlterTableAction]
  of skInsert:
    insertTable*: string
    insertColumns*: seq[string]
    insertValues*: seq[Expr]
    insertConflictAction*: InsertConflictAction
    insertConflictTargetCols*: seq[string]
    insertConflictTargetConstraint*: string
    insertConflictUpdateAssignments*: Table[string, Expr]
    insertConflictUpdateWhere*: Expr
  of skSelect:
    selectItems*: seq[SelectItem]
    fromTable*: string
    fromAlias*: string
    joins*: seq[JoinClause]
    whereExpr*: Expr
    groupBy*: seq[Expr]
    havingExpr*: Expr
    orderBy*: seq[OrderItem]
    limit*: int
    limitParam*: int
    offset*: int
    offsetParam*: int
  of skUpdate:
    updateTable*: string
    assignments*: Table[string, Expr]
    updateWhere*: Expr
  of skDelete:
    deleteTable*: string
    deleteWhere*: Expr
  of skBegin:
    discard
  of skCommit:
    discard
  of skRollback:
    discard

type SqlAst* = ref object
  statements*: seq[Statement]

proc nodeHas*(node: JsonNode, key: string): bool =
  node.kind == JObject and node.hasKey(key)

proc nodeGet*(node: JsonNode, key: string): JsonNode =
  if nodeHas(node, key): node[key] else: newJNull()

proc nodeString*(node: JsonNode): string =
  if node.kind == JString:
    return node.str
  if nodeHas(node, "String"):
    if node["String"].kind == JString:
      return node["String"].str
    if nodeHas(node["String"], "str"):
      return node["String"]["str"].str
    if nodeHas(node["String"], "sval"):
      return node["String"]["sval"].str
  if nodeHas(node, "str"):
    return node["str"].str
  if nodeHas(node, "sval"):
    return node["sval"].str
  ""

proc nodeStringOr*(node: JsonNode, key: string, fallback: string): string =
  if nodeHas(node, key):
    return node[key].getStr
  fallback

proc parseExprNode(node: JsonNode): Result[Expr]
proc parseStatementNode(node: JsonNode): Result[Statement]
proc selectToCanonicalSql(stmt: Statement): string

proc parseAConst*(node: JsonNode): Result[Expr] =
  if nodeHas(node, "isnull") and node["isnull"].getBool:
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svNull)))
  if nodeHas(node, "ival"):
    let intNode = node["ival"]
    if nodeHas(intNode, "ival"):
      return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: int64(intNode["ival"].getInt))))
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: int64(intNode.getInt))))
  if nodeHas(node, "sval"):
    let strNode = node["sval"]
    if nodeHas(strNode, "sval"):
      return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: strNode["sval"].getStr)))
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: strNode.getStr)))
  if nodeHas(node, "fval"):
    let floatNode = node["fval"]
    let strVal = if nodeHas(floatNode, "fval"): floatNode["fval"].getStr else: floatNode.getStr
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svFloat, floatVal: parseFloat(strVal))))
  let valNode = nodeGet(node, "val")
  if nodeHas(valNode, "Integer"):
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: int64(valNode["Integer"]["ival"].getInt))))
  if nodeHas(valNode, "Float"):
    let strVal = valNode["Float"]["str"].getStr
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svFloat, floatVal: parseFloat(strVal))))
  if nodeHas(valNode, "String"):
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: valNode["String"]["str"].getStr)))
  if nodeHas(valNode, "Boolean"):
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: valNode["Boolean"]["boolval"].getBool)))
  if nodeHas(valNode, "Null"):
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svNull)))
  if nodeHas(node, "Null"):
    return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svNull)))
  if nodeHas(node, "boolval"):
    let boolNode = node["boolval"]
    if nodeHas(boolNode, "boolval"):
      return ok(Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: boolNode["boolval"].getBool)))
  err[Expr](ERR_SQL, "Unsupported A_Const")

proc parseColumnRef(node: JsonNode): Result[Expr] =
  let fields = nodeGet(node, "fields")
  if fields.kind != JArray or fields.len == 0:
    return err[Expr](ERR_SQL, "ColumnRef missing fields")
  if fields.len == 1 and nodeHas(fields[0], "A_Star"):
    return ok(Expr(kind: ekColumn, table: "", name: "*"))
  if fields.len == 1:
    return ok(Expr(kind: ekColumn, table: "", name: nodeString(fields[0])))
  let tableName = nodeString(fields[0])
  let colName = nodeString(fields[1])
  ok(Expr(kind: ekColumn, table: tableName, name: colName))

proc parseFuncCall(node: JsonNode): Result[Expr] =
  let nameParts = nodeGet(node, "funcname")
  var funcName = ""
  if nameParts.kind == JArray and nameParts.len > 0:
    funcName = nodeString(nameParts[^1]).toUpperAscii()
  if funcName == "BTRIM":
    funcName = "TRIM"
  let argsNode = nodeGet(node, "args")
  var args: seq[Expr] = @[]
  if argsNode.kind == JArray:
    for arg in argsNode:
      let argRes = parseExprNode(arg)
      if not argRes.ok:
        return err[Expr](argRes.err.code, argRes.err.message, argRes.err.context)
      args.add(argRes.value)
  let isStar = nodeHas(node, "agg_star") and node["agg_star"].getBool
  ok(Expr(kind: ekFunc, funcName: funcName, args: args, isStar: isStar))

proc parseListItems(node: JsonNode): seq[JsonNode] =
  if node.kind == JArray:
    for item in node:
      result.add(item)
    return
  if nodeHas(node, "List") and nodeHas(node["List"], "items"):
    let items = node["List"]["items"]
    if items.kind == JArray:
      for item in items:
        result.add(item)
      return

proc parseBoolExpr(node: JsonNode): Result[Expr] =
  let op = nodeGet(node, "boolop").getStr
  let args = nodeGet(node, "args")
  if args.kind != JArray or args.len == 0:
    return err[Expr](ERR_SQL, "BoolExpr missing args")
  if op == "NOT_EXPR":
    let inner = parseExprNode(args[0])
    if not inner.ok:
      return err[Expr](inner.err.code, inner.err.message, inner.err.context)
    return ok(Expr(kind: ekUnary, unOp: "NOT", expr: inner.value))
  var currentRes = parseExprNode(args[0])
  if not currentRes.ok:
    return err[Expr](currentRes.err.code, currentRes.err.message, currentRes.err.context)
  var current = currentRes.value
  for i in 1 ..< args.len:
    let nextRes = parseExprNode(args[i])
    if not nextRes.ok:
      return err[Expr](nextRes.err.code, nextRes.err.message, nextRes.err.context)
    let opStr = if op == "AND_EXPR": "AND" else: "OR"
    current = Expr(kind: ekBinary, op: opStr, left: current, right: nextRes.value)
  ok(current)

proc parseAExpr(node: JsonNode): Result[Expr] =
  let nameArr = nodeGet(node, "name")
  var op = ""
  if nameArr.kind == JArray and nameArr.len > 0:
    op = nodeString(nameArr[0])
  if op == "~~":
    op = "LIKE"
  elif op == "~~*":
    op = "ILIKE"
  
  let kindNode = nodeGet(node, "kind")
  var kindName = ""
  if kindNode.kind == JString:
    kindName = kindNode.getStr
  elif kindNode.kind == JInt:
    # Older libpg_query builds expose integer enum tags.
    if kindNode.getInt == 10:
      kindName = "AEXPR_IN"

  if kindName == "AEXPR_NULLIF":
    let leftRes = parseExprNode(nodeGet(node, "lexpr"))
    if not leftRes.ok:
      return err[Expr](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    let rightRes = parseExprNode(nodeGet(node, "rexpr"))
    if not rightRes.ok:
      return err[Expr](rightRes.err.code, rightRes.err.message, rightRes.err.context)
    return ok(Expr(kind: ekFunc, funcName: "NULLIF", args: @[leftRes.value, rightRes.value], isStar: false))

  if kindName == "AEXPR_BETWEEN" or kindName == "AEXPR_NOT_BETWEEN":
    let leftRes = parseExprNode(nodeGet(node, "lexpr"))
    if not leftRes.ok:
      return err[Expr](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    let items = parseListItems(nodeGet(node, "rexpr"))
    if items.len != 2:
      return err[Expr](ERR_SQL, "BETWEEN requires lower and upper bounds")
    let lowRes = parseExprNode(items[0])
    if not lowRes.ok:
      return err[Expr](lowRes.err.code, lowRes.err.message, lowRes.err.context)
    let highRes = parseExprNode(items[1])
    if not highRes.ok:
      return err[Expr](highRes.err.code, highRes.err.message, highRes.err.context)
    let geExpr = Expr(kind: ekBinary, op: ">=", left: leftRes.value, right: lowRes.value)
    let leExpr = Expr(kind: ekBinary, op: "<=", left: leftRes.value, right: highRes.value)
    let betweenExpr = Expr(kind: ekBinary, op: "AND", left: geExpr, right: leExpr)
    if kindName == "AEXPR_NOT_BETWEEN":
      return ok(Expr(kind: ekUnary, unOp: "NOT", expr: betweenExpr))
    return ok(betweenExpr)

  # Check for IN expression
  if kindName == "AEXPR_IN":
    # This is an IN expression
    let leftRes = parseExprNode(nodeGet(node, "lexpr"))
    if not leftRes.ok:
      return err[Expr](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    
    # Parse the list of values from rexpr
    var inList: seq[Expr] = @[]
    let rexpr = nodeGet(node, "rexpr")
    
    proc addFromList(listNode: JsonNode): Result[Void] =
      if listNode.kind == JArray:
        for item in listNode:
          if nodeHas(item, "List"):
            if nodeHas(item["List"], "items"):
              let subRes = addFromList(item["List"]["items"])
              if not subRes.ok: return subRes
          else:
            let itemRes = parseExprNode(item)
            if not itemRes.ok:
              return err[Void](itemRes.err.code, itemRes.err.message, itemRes.err.context)
            inList.add(itemRes.value)
      okVoid()

    if rexpr.kind == JArray:
      let res = addFromList(rexpr)
      if not res.ok: return err[Expr](res.err.code, res.err.message, res.err.context)
    elif nodeHas(rexpr, "List") and nodeHas(rexpr["List"], "items"):
      let res = addFromList(rexpr["List"]["items"])
      if not res.ok: return err[Expr](res.err.code, res.err.message, res.err.context)
    
    return ok(Expr(kind: ekInList, inExpr: leftRes.value, inList: inList))
  
  # Regular binary expression
  let leftRes = parseExprNode(nodeGet(node, "lexpr"))
  if not leftRes.ok:
    return err[Expr](leftRes.err.code, leftRes.err.message, leftRes.err.context)
  let rightRes = parseExprNode(nodeGet(node, "rexpr"))
  if not rightRes.ok:
    return err[Expr](rightRes.err.code, rightRes.err.message, rightRes.err.context)
  ok(Expr(kind: ekBinary, op: op, left: leftRes.value, right: rightRes.value))

proc parseParamRef(node: JsonNode): Result[Expr] =
  let number = nodeGet(node, "number").getInt
  ok(Expr(kind: ekParam, index: number))

proc parseCoalesceExpr(node: JsonNode): Result[Expr] =
  var args: seq[Expr] = @[]
  let argsNode = nodeGet(node, "args")
  if argsNode.kind == JArray:
    for arg in argsNode:
      let argRes = parseExprNode(arg)
      if not argRes.ok:
        return err[Expr](argRes.err.code, argRes.err.message, argRes.err.context)
      args.add(argRes.value)
  ok(Expr(kind: ekFunc, funcName: "COALESCE", args: args, isStar: false))

proc parseTypeCast(node: JsonNode): Result[Expr] =
  let argRes = parseExprNode(nodeGet(node, "arg"))
  if not argRes.ok:
    return err[Expr](argRes.err.code, argRes.err.message, argRes.err.context)
  let typeNodeRaw = nodeGet(node, "typeName")

  proc extractTypeNameWithMods(typeNode: JsonNode): string =
    var tn = typeNode
    if nodeHas(tn, "TypeName"):
      tn = tn["TypeName"]
    if tn.kind != JObject:
      return ""
    let names = nodeGet(tn, "names")
    if names.kind != JArray or names.len == 0:
      return ""
    var base = nodeString(names[^1]).toUpperAscii()
    let typmods = nodeGet(tn, "typmods")
    if typmods.kind != JArray or typmods.len == 0:
      return base
    var mods: seq[string] = @[]
    for m in typmods:
      var n = m
      if nodeHas(n, "A_Const"):
        n = n["A_Const"]
      if nodeHas(n, "val"):
        n = n["val"]
      if nodeHas(n, "Integer"):
        n = n["Integer"]
      if nodeHas(n, "ival"):
        mods.add($n["ival"].getInt)
      elif n.kind == JInt:
        mods.add($n.getInt)
    if mods.len == 0:
      return base
    base & "(" & mods.join(",") & ")"

  let resolvedType = extractTypeNameWithMods(typeNodeRaw)
  if resolvedType.len == 0:
    return err[Expr](ERR_SQL, "CAST requires target type")
  ok(Expr(
    kind: ekFunc,
    funcName: "CAST",
    args: @[
      argRes.value,
      Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: resolvedType))
    ],
    isStar: false
  ))

proc parseCaseExpr(node: JsonNode): Result[Expr] =
  var caseArgs: seq[Expr] = @[]
  var simpleCaseArg: Expr = nil
  if nodeHas(node, "arg"):
    let argRes = parseExprNode(node["arg"])
    if not argRes.ok:
      return err[Expr](argRes.err.code, argRes.err.message, argRes.err.context)
    simpleCaseArg = argRes.value

  let whenNodes = nodeGet(node, "args")
  if whenNodes.kind == JArray:
    for whenNode in whenNodes:
      if not nodeHas(whenNode, "CaseWhen"):
        continue
      let cw = whenNode["CaseWhen"]
      let condRawRes = parseExprNode(nodeGet(cw, "expr"))
      if not condRawRes.ok:
        return err[Expr](condRawRes.err.code, condRawRes.err.message, condRawRes.err.context)
      var condExpr = condRawRes.value
      if simpleCaseArg != nil:
        condExpr = Expr(kind: ekBinary, op: "=", left: simpleCaseArg, right: condExpr)
      let resultRes = parseExprNode(nodeGet(cw, "result"))
      if not resultRes.ok:
        return err[Expr](resultRes.err.code, resultRes.err.message, resultRes.err.context)
      caseArgs.add(condExpr)
      caseArgs.add(resultRes.value)

  var elseExpr = Expr(kind: ekLiteral, value: SqlValue(kind: svNull))
  if nodeHas(node, "defresult"):
    let elseRes = parseExprNode(node["defresult"])
    if not elseRes.ok:
      return err[Expr](elseRes.err.code, elseRes.err.message, elseRes.err.context)
    elseExpr = elseRes.value
  caseArgs.add(elseExpr)
  ok(Expr(kind: ekFunc, funcName: "CASE", args: caseArgs, isStar: false))

proc parseSubLink(node: JsonNode): Result[Expr] =
  let linkType = nodeStringOr(node, "subLinkType", "")
  if linkType != "EXISTS_SUBLINK":
    return err[Expr](ERR_SQL, "Only EXISTS subqueries are supported in 0.x")
  let subselectNode = nodeGet(node, "subselect")
  let stmtRes = parseStatementNode(subselectNode)
  if not stmtRes.ok:
    return err[Expr](stmtRes.err.code, stmtRes.err.message, stmtRes.err.context)
  if stmtRes.value.kind != skSelect:
    return err[Expr](ERR_SQL, "EXISTS requires SELECT subquery")
  let sqlText = selectToCanonicalSql(stmtRes.value)
  ok(Expr(kind: ekFunc, funcName: "EXISTS", args: @[Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: sqlText))], isStar: false))

proc parseNullTest(node: JsonNode): Result[Expr] =
  let argRes = parseExprNode(node["arg"])
  if not argRes.ok:
    return err[Expr](argRes.err.code, argRes.err.message, argRes.err.context)
  let typeStr = nodeString(node["nulltesttype"])
  var op = "IS"
  if typeStr == "IS_NOT_NULL":
    op = "IS NOT"
  ok(Expr(kind: ekBinary, op: op, left: argRes.value, right: Expr(kind: ekLiteral, value: SqlValue(kind: svNull))))

proc parseExprNode(node: JsonNode): Result[Expr] =
  if node.kind == JObject:
    if nodeHas(node, "A_Const"):
      return parseAConst(node["A_Const"])
    if nodeHas(node, "ColumnRef"):
      return parseColumnRef(node["ColumnRef"])
    if nodeHas(node, "FuncCall"):
      return parseFuncCall(node["FuncCall"])
    if nodeHas(node, "BoolExpr"):
      return parseBoolExpr(node["BoolExpr"])
    if nodeHas(node, "A_Expr"):
      return parseAExpr(node["A_Expr"])
    if nodeHas(node, "CoalesceExpr"):
      return parseCoalesceExpr(node["CoalesceExpr"])
    if nodeHas(node, "CaseExpr"):
      return parseCaseExpr(node["CaseExpr"])
    if nodeHas(node, "SubLink"):
      return parseSubLink(node["SubLink"])
    if nodeHas(node, "ParamRef"):
      return parseParamRef(node["ParamRef"])
    if nodeHas(node, "TypeCast"):
      return parseTypeCast(node["TypeCast"])
    if nodeHas(node, "NullTest"):
      return parseNullTest(node["NullTest"])
  err[Expr](ERR_SQL, "Unsupported expression node: " & $node)

proc unwrapRangeVar(node: JsonNode): JsonNode =
  if nodeHas(node, "RangeVar"):
    return node["RangeVar"]
  node

proc parseRangeVar(node: JsonNode): Result[(string, string)] =
  let rangeNode = unwrapRangeVar(node)
  let relname = nodeGet(rangeNode, "relname").getStr
  var alias = ""
  if nodeHas(rangeNode, "alias"):
    var aliasNode = rangeNode["alias"]
    if nodeHas(aliasNode, "Alias"):
      aliasNode = aliasNode["Alias"]
    if nodeHas(aliasNode, "aliasname"):
      alias = aliasNode["aliasname"].getStr
  ok((relname, alias))

proc parseFromItem(node: JsonNode, baseTable: var string, baseAlias: var string, joins: var seq[JoinClause]): Result[Void] =
  if nodeHas(node, "RangeVar") or nodeHas(node, "relname"):
    let rvRes = parseRangeVar(node)
    if not rvRes.ok:
      return err[Void](rvRes.err.code, rvRes.err.message, rvRes.err.context)
    if baseTable.len == 0:
      baseTable = rvRes.value[0]
      baseAlias = rvRes.value[1]
    else:
      joins.add(JoinClause(joinType: jtInner, table: rvRes.value[0], alias: rvRes.value[1], onExpr: nil))
    return okVoid()
  if nodeHas(node, "JoinExpr"):
    let join = node["JoinExpr"]
    let leftRes = parseFromItem(join["larg"], baseTable, baseAlias, joins)
    if not leftRes.ok:
      return err[Void](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    var rightTable = ""
    var rightAlias = ""
    if nodeHas(join, "rarg"):
      let rarg = join["rarg"]
      let rvRes = parseRangeVar(rarg)
      if rvRes.ok:
        rightTable = rvRes.value[0]
        rightAlias = rvRes.value[1]
    let joinKindStr = if nodeHas(join, "jointype"): join["jointype"].getStr else: ""
    let joinKind = if joinKindStr == "JOIN_LEFT": jtLeft else: jtInner
    var onExpr: Expr = nil
    if nodeHas(join, "quals"):
      let onRes = parseExprNode(join["quals"])
      if onRes.ok:
        onExpr = onRes.value
    if rightTable.len > 0:
      joins.add(JoinClause(joinType: joinKind, table: rightTable, alias: rightAlias, onExpr: onExpr))
    return okVoid()
  err[Void](ERR_SQL, "Unsupported FROM item")

proc parseSelectStmt(node: JsonNode): Result[Statement] =
  var items: seq[SelectItem] = @[]
  let targets = nodeGet(node, "targetList")
  if targets.kind == JArray:
    for target in targets:
      let rt = target["ResTarget"]
      if nodeHas(rt, "val"):
        let valNode = rt["val"]
        if nodeHas(valNode, "ColumnRef"):
          let colRes = parseColumnRef(valNode["ColumnRef"])
          if colRes.ok and colRes.value.name == "*":
            items.add(SelectItem(isStar: true))
            continue
        let exprRes = parseExprNode(valNode)
        if not exprRes.ok:
          return err[Statement](exprRes.err.code, exprRes.err.message, exprRes.err.context)
        let alias = if nodeHas(rt, "name"): rt["name"].getStr else: ""
        items.add(SelectItem(expr: exprRes.value, alias: alias, isStar: false))
  var fromTable = ""
  var fromAlias = ""
  var joins: seq[JoinClause] = @[]
  let fromClause = nodeGet(node, "fromClause")
  if fromClause.kind == JArray and fromClause.len > 0:
    for item in fromClause:
      let res = parseFromItem(item, fromTable, fromAlias, joins)
      if not res.ok:
        return err[Statement](res.err.code, res.err.message, res.err.context)
  var whereExpr: Expr = nil
  if nodeHas(node, "whereClause"):
    let whereRes = parseExprNode(node["whereClause"])
    if not whereRes.ok:
      return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)
    whereExpr = whereRes.value
  var groupBy: seq[Expr] = @[]
  let groupClause = nodeGet(node, "groupClause")
  if groupClause.kind == JArray:
    for g in groupClause:
      let gRes = parseExprNode(g)
      if not gRes.ok:
        return err[Statement](gRes.err.code, gRes.err.message, gRes.err.context)
      groupBy.add(gRes.value)
  var havingExpr: Expr = nil
  if nodeHas(node, "havingClause"):
    let hRes = parseExprNode(node["havingClause"])
    if not hRes.ok:
      return err[Statement](hRes.err.code, hRes.err.message, hRes.err.context)
    havingExpr = hRes.value
  var orderBy: seq[OrderItem] = @[]
  let sortClause = nodeGet(node, "sortClause")
  if sortClause.kind == JArray:
    for s in sortClause:
      let sortBy = s["SortBy"]
      let exprRes = parseExprNode(sortBy["node"])
      if not exprRes.ok:
        return err[Statement](exprRes.err.code, exprRes.err.message, exprRes.err.context)
      let dir = nodeStringOr(sortBy, "sortby_dir", "")
      let asc = dir != "SORTBY_DESC" and dir != "2"
      orderBy.add(OrderItem(expr: exprRes.value, asc: asc))
  var limit = -1
  var limitParam = 0
  var offset = -1
  var offsetParam = 0
  if nodeHas(node, "limitCount"):
    let limitRes = parseExprNode(node["limitCount"])
    if not limitRes.ok:
      return err[Statement](limitRes.err.code, limitRes.err.message, limitRes.err.context)
    if limitRes.value.kind == ekLiteral and limitRes.value.value.kind == svInt:
      limit = int(limitRes.value.value.intVal)
    elif limitRes.value.kind == ekParam and limitRes.value.index > 0:
      limitParam = limitRes.value.index
    else:
      return err[Statement](ERR_SQL, "LIMIT must be an integer literal or $N parameter")
  if nodeHas(node, "limitOffset"):
    let offsetRes = parseExprNode(node["limitOffset"])
    if not offsetRes.ok:
      return err[Statement](offsetRes.err.code, offsetRes.err.message, offsetRes.err.context)
    if offsetRes.value.kind == ekLiteral and offsetRes.value.value.kind == svInt:
      offset = int(offsetRes.value.value.intVal)
    elif offsetRes.value.kind == ekParam and offsetRes.value.index > 0:
      offsetParam = offsetRes.value.index
    else:
      return err[Statement](ERR_SQL, "OFFSET must be an integer literal or $N parameter")
  ok(Statement(kind: skSelect, selectItems: items, fromTable: fromTable, fromAlias: fromAlias, joins: joins, whereExpr: whereExpr, groupBy: groupBy, havingExpr: havingExpr, orderBy: orderBy, limit: limit, limitParam: limitParam, offset: offset, offsetParam: offsetParam))

proc parseInsertStmt(node: JsonNode): Result[Statement] =
  let rel = unwrapRangeVar(node["relation"])
  let tableRes = parseRangeVar(rel)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  var cols: seq[string] = @[]
  let colsNode = nodeGet(node, "cols")
  if colsNode.kind == JArray:
    for colNode in colsNode:
      let resTarget = colNode["ResTarget"]
      cols.add(resTarget["name"].getStr)
  let selectNode = nodeGet(node, "selectStmt")
  var values: seq[Expr] = @[]
  if nodeHas(selectNode, "SelectStmt"):
    let selectStmt = selectNode["SelectStmt"]
    let valuesLists = nodeGet(selectStmt, "valuesLists")
    if valuesLists.kind == JArray and valuesLists.len > 0:
      let first = valuesLists[0]
      var itemsNode = first
      if nodeHas(first, "List"):
        itemsNode = nodeGet(first["List"], "items")
      if itemsNode.kind == JArray:
        for v in itemsNode:
          let exprRes = parseExprNode(v)
          if not exprRes.ok:
            return err[Statement](exprRes.err.code, exprRes.err.message, exprRes.err.context)
          values.add(exprRes.value)

  var conflictAction = icaNone
  var conflictTargetCols: seq[string] = @[]
  var conflictTargetConstraint = ""
  var conflictAssignments = initTable[string, Expr]()
  var conflictWhere: Expr = nil
  if nodeHas(node, "onConflictClause"):
    let conflict = node["onConflictClause"]
    let action = nodeGet(conflict, "action").getStr
    case action
    of "ONCONFLICT_NOTHING":
      conflictAction = icaDoNothing
    of "ONCONFLICT_UPDATE":
      conflictAction = icaDoUpdate
    else:
      return err[Statement](ERR_SQL, "Unsupported ON CONFLICT action", action)

    let infer = nodeGet(conflict, "infer")
    if infer.kind == JObject:
      if nodeHas(infer, "conname"):
        conflictTargetConstraint = nodeGet(infer, "conname").getStr
      let elems = nodeGet(infer, "indexElems")
      if elems.kind == JArray:
        for entry in elems:
          if not nodeHas(entry, "IndexElem"):
            return err[Statement](ERR_SQL, "Unsupported ON CONFLICT target element")
          let idxElem = entry["IndexElem"]
          let colName = nodeGet(idxElem, "name").getStr
          if colName.len == 0:
            return err[Statement](ERR_SQL, "ON CONFLICT target expressions are not supported")
          conflictTargetCols.add(colName)

    if conflictAction == icaDoUpdate:
      let targetList = nodeGet(conflict, "targetList")
      if targetList.kind == JArray:
        for entry in targetList:
          let resTarget = nodeGet(entry, "ResTarget")
          let name = nodeGet(resTarget, "name").getStr
          if name.len == 0:
            return err[Statement](ERR_SQL, "ON CONFLICT DO UPDATE requires assignment target column")
          let exprRes = parseExprNode(nodeGet(resTarget, "val"))
          if not exprRes.ok:
            return err[Statement](exprRes.err.code, exprRes.err.message, exprRes.err.context)
          conflictAssignments[name] = exprRes.value
      if nodeHas(conflict, "whereClause"):
        let whereRes = parseExprNode(nodeGet(conflict, "whereClause"))
        if not whereRes.ok:
          return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)
        conflictWhere = whereRes.value

  if nodeHas(node, "returningList"):
    let returningNode = nodeGet(node, "returningList")
    if returningNode.kind == JArray and returningNode.len > 0:
      return err[Statement](ERR_SQL, "INSERT RETURNING is not supported")

  ok(Statement(
    kind: skInsert,
    insertTable: tableRes.value[0],
    insertColumns: cols,
    insertValues: values,
    insertConflictAction: conflictAction,
    insertConflictTargetCols: conflictTargetCols,
    insertConflictTargetConstraint: conflictTargetConstraint,
    insertConflictUpdateAssignments: conflictAssignments,
    insertConflictUpdateWhere: conflictWhere
  ))

proc parseUpdateStmt(node: JsonNode): Result[Statement] =
  let rel = unwrapRangeVar(node["relation"])
  let tableRes = parseRangeVar(rel)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  var assigns = initTable[string, Expr]()
  let targetList = nodeGet(node, "targetList")
  if targetList.kind == JArray:
    for entry in targetList:
      let resTarget = entry["ResTarget"]
      let name = resTarget["name"].getStr
      let exprRes = parseExprNode(resTarget["val"])
      if not exprRes.ok:
        return err[Statement](exprRes.err.code, exprRes.err.message, exprRes.err.context)
      assigns[name] = exprRes.value
  var whereExpr: Expr = nil
  if nodeHas(node, "whereClause"):
    let whereRes = parseExprNode(node["whereClause"])
    if not whereRes.ok:
      return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)
    whereExpr = whereRes.value
  ok(Statement(kind: skUpdate, updateTable: tableRes.value[0], assignments: assigns, updateWhere: whereExpr))

proc parseDeleteStmt(node: JsonNode): Result[Statement] =
  let rel = unwrapRangeVar(node["relation"])
  let tableRes = parseRangeVar(rel)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  var whereExpr: Expr = nil
  if nodeHas(node, "whereClause"):
    let whereRes = parseExprNode(node["whereClause"])
    if not whereRes.ok:
      return err[Statement](whereRes.err.code, whereRes.err.message, whereRes.err.context)
    whereExpr = whereRes.value
  ok(Statement(kind: skDelete, deleteTable: tableRes.value[0], deleteWhere: whereExpr))

proc quoteSqlString(text: string): string =
  result = "'"
  for ch in text:
    if ch == '\'':
      result.add("''")
    else:
      result.add(ch)
  result.add("'")

proc exprToCanonicalSql(expr: Expr): string

proc exprToCanonicalSql(expr: Expr): string =
  if expr == nil:
    return "NULL"
  case expr.kind
  of ekLiteral:
    case expr.value.kind
    of svNull:
      "NULL"
    of svInt:
      $expr.value.intVal
    of svFloat:
      $expr.value.floatVal
    of svString:
      quoteSqlString(expr.value.strVal)
    of svBool:
      if expr.value.boolVal: "TRUE" else: "FALSE"
    of svParam:
      "$" & $expr.value.paramIndex
  of ekColumn:
    if expr.table.len > 0:
      expr.table & "." & expr.name
    else:
      expr.name
  of ekBinary:
    "(" & exprToCanonicalSql(expr.left) & " " & expr.op & " " & exprToCanonicalSql(expr.right) & ")"
  of ekUnary:
    "(" & expr.unOp & " " & exprToCanonicalSql(expr.expr) & ")"
  of ekFunc:
    if expr.funcName == "CAST" and expr.args.len == 2 and expr.args[1].kind == ekLiteral and expr.args[1].value.kind == svString:
      "CAST(" & exprToCanonicalSql(expr.args[0]) & " AS " & expr.args[1].value.strVal & ")"
    elif expr.funcName == "CASE":
      var text = "CASE"
      var i = 0
      while i + 1 < expr.args.len - 1:
        text.add(" WHEN " & exprToCanonicalSql(expr.args[i]) & " THEN " & exprToCanonicalSql(expr.args[i + 1]))
        i += 2
      if expr.args.len > 0:
        text.add(" ELSE " & exprToCanonicalSql(expr.args[^1]))
      text.add(" END")
      text
    elif expr.funcName == "EXISTS" and expr.args.len == 1 and expr.args[0].kind == ekLiteral and expr.args[0].value.kind == svString:
      "EXISTS (" & expr.args[0].value.strVal & ")"
    else:
      let argsText =
        if expr.isStar:
          "*"
        else:
          block:
            var args: seq[string] = @[]
            for arg in expr.args:
              args.add(exprToCanonicalSql(arg))
            args.join(", ")
      expr.funcName & "(" & argsText & ")"
  of ekParam:
    "$" & $expr.index
  of ekInList:
    var parts: seq[string] = @[]
    for item in expr.inList:
      parts.add(exprToCanonicalSql(item))
    "(" & exprToCanonicalSql(expr.inExpr) & " IN (" & parts.join(", ") & "))"

proc selectToCanonicalSql(stmt: Statement): string =
  var parts: seq[string] = @[]
  var selectItems: seq[string] = @[]
  for item in stmt.selectItems:
    if item.isStar:
      selectItems.add("*")
      continue
    var entry = exprToCanonicalSql(item.expr)
    if item.alias.len > 0:
      entry.add(" AS " & item.alias)
    selectItems.add(entry)
  if selectItems.len == 0:
    selectItems.add("*")
  parts.add("SELECT " & selectItems.join(", "))

  if stmt.fromTable.len > 0:
    var fromPart = "FROM " & stmt.fromTable
    if stmt.fromAlias.len > 0:
      fromPart.add(" " & stmt.fromAlias)
    parts.add(fromPart)

  for join in stmt.joins:
    let joinKeyword = if join.joinType == jtLeft: "LEFT JOIN " else: "INNER JOIN "
    var joinPart = joinKeyword & join.table
    if join.alias.len > 0:
      joinPart.add(" " & join.alias)
    if join.onExpr != nil:
      joinPart.add(" ON " & exprToCanonicalSql(join.onExpr))
    parts.add(joinPart)

  if stmt.whereExpr != nil:
    parts.add("WHERE " & exprToCanonicalSql(stmt.whereExpr))

  if stmt.groupBy.len > 0:
    var groupParts: seq[string] = @[]
    for expr in stmt.groupBy:
      groupParts.add(exprToCanonicalSql(expr))
    parts.add("GROUP BY " & groupParts.join(", "))

  if stmt.havingExpr != nil:
    parts.add("HAVING " & exprToCanonicalSql(stmt.havingExpr))

  if stmt.orderBy.len > 0:
    var orderParts: seq[string] = @[]
    for item in stmt.orderBy:
      var entry = exprToCanonicalSql(item.expr)
      if not item.asc:
        entry.add(" DESC")
      orderParts.add(entry)
    parts.add("ORDER BY " & orderParts.join(", "))

  if stmt.limitParam > 0:
    parts.add("LIMIT $" & $stmt.limitParam)
  elif stmt.limit >= 0:
    parts.add("LIMIT " & $stmt.limit)

  if stmt.offsetParam > 0:
    parts.add("OFFSET $" & $stmt.offsetParam)
  elif stmt.offset >= 0:
    parts.add("OFFSET " & $stmt.offset)

  parts.join(" ")
# ... Rest of file is identical, assume parseCreateStmt etc don't need changes as they don't have expression parsing flaws in context
# Actually, parseCreateStmt etc. are fine.
# I will output the whole file content to be safe.

proc parseCreateStmt(node: JsonNode): Result[Statement] =
  let rel = unwrapRangeVar(node["relation"])
  let tableRes = parseRangeVar(rel)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  var columns: seq[ColumnDef] = @[]
  var tableConstraints: seq[JsonNode] = @[]
  let elts = nodeGet(node, "tableElts")
  if elts.kind == JArray:
    for entry in elts:
      if nodeHas(entry, "ColumnDef"):
        let col = entry["ColumnDef"]
        let name = col["colname"].getStr
        var typeName = ""
        if nodeHas(col, "typeName"):
          var typeNode = col["typeName"]
          if nodeHas(typeNode, "TypeName"):
            typeNode = typeNode["TypeName"]
          let typeNames = nodeGet(typeNode, "names")
          if typeNames.kind == JArray and typeNames.len > 0:
            typeName = nodeString(typeNames[^1])
            let typmods = nodeGet(typeNode, "typmods")
            if typmods.kind == JArray and typmods.len > 0:
              var mods: seq[string] = @[]
              for m in typmods:
                var n = m
                if nodeHas(n, "A_Const"):
                  n = n["A_Const"]
                if nodeHas(n, "val"):
                  n = n["val"]
                if nodeHas(n, "Integer"):
                  n = n["Integer"]
                if nodeHas(n, "ival"):
                  mods.add($n["ival"].getInt)
                elif n.kind == JInt:
                  mods.add($n.getInt)
              if mods.len > 0:
                typeName.add("(" & mods.join(",") & ")")
        var def = ColumnDef(name: name, typeName: typeName)
        let constraints = nodeGet(col, "constraints")
        if constraints.kind == JArray:
          for cons in constraints:
            if nodeHas(cons, "Constraint"):
              let constraint = cons["Constraint"]
              let contype = nodeGet(constraint, "contype").getStr
              case contype
              of "CONSTR_NOTNULL":
                def.notNull = true
              of "CONSTR_UNIQUE":
                def.unique = true
              of "CONSTR_PRIMARY":
                def.primaryKey = true
                def.unique = true
                def.notNull = true
              of "CONSTR_FOREIGN":
                if nodeHas(constraint, "pktable"):
                  let pkRes = parseRangeVar(constraint["pktable"])
                  if pkRes.ok:
                    def.refTable = pkRes.value[0]
                let attrs = nodeGet(constraint, "pk_attrs")
                if attrs.kind == JArray and attrs.len > 0:
                  def.refColumn = nodeString(attrs[0])
              else:
                discard
        columns.add(def)
      elif nodeHas(entry, "Constraint"):
        tableConstraints.add(entry["Constraint"])
  for constraint in tableConstraints:
    let contype = nodeGet(constraint, "contype").getStr
    if contype in ["CONSTR_UNIQUE", "CONSTR_PRIMARY"]:
      let keys = nodeGet(constraint, "keys")
      if keys.kind != JArray or keys.len == 0:
        return err[Statement](ERR_SQL, "Constraint must reference at least one column")
      for keyNode in keys:
        let colName = nodeString(keyNode)
        var found = false
        for i, col in columns:
          if col.name == colName:
            found = true
            columns[i].unique = keys.len == 1  # only mark unique for single-column
            if contype == "CONSTR_PRIMARY":
              columns[i].primaryKey = true
              columns[i].notNull = true
            break
        if not found:
          return err[Statement](ERR_SQL, "Constraint refers to unknown column", colName)
    elif contype == "CONSTR_FOREIGN":
      return err[Statement](ERR_SQL, "Table-level foreign keys not supported")
  ok(Statement(kind: skCreateTable, createTableName: tableRes.value[0], columns: columns))

proc parseIndexStmt(node: JsonNode): Result[Statement] =
  let idxName = nodeGet(node, "idxname").getStr
  let rel = unwrapRangeVar(node["relation"])
  let tableRes = parseRangeVar(rel)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  var columnNames: seq[string] = @[]
  let params = nodeGet(node, "indexParams")
  if params.kind == JArray:
    for p in params:
      if nodeHas(p, "IndexElem"):
        let param = p["IndexElem"]
        if nodeHas(param, "name"):
          columnNames.add(param["name"].getStr)
  var kind = ikBtree
  let methodName = nodeGet(node, "accessMethod").getStr
  if methodName.len > 0:
    let methodLower = methodName.toLowerAscii()
    if methodLower == "trigram":
      kind = ikTrigram
  let unique = nodeHas(node, "unique") and node["unique"].getBool
  ok(Statement(kind: skCreateIndex, indexName: idxName, indexTableName: tableRes.value[0], columnNames: columnNames, indexKind: kind, unique: unique))

proc parseViewStmt(node: JsonNode): Result[Statement] =
  let viewRes = parseRangeVar(nodeGet(node, "view"))
  if not viewRes.ok:
    return err[Statement](viewRes.err.code, viewRes.err.message, viewRes.err.context)
  let viewName = viewRes.value[0]

  let persistence = nodeStringOr(nodeGet(node, "view"), "relpersistence", "p")
  if persistence != "p":
    return err[Statement](ERR_SQL, "TEMP/TEMPORARY VIEW not supported")

  let checkOption = nodeStringOr(node, "withCheckOption", "NO_CHECK_OPTION")
  if checkOption != "NO_CHECK_OPTION":
    return err[Statement](ERR_SQL, "WITH CHECK OPTION not supported")

  var aliases: seq[string] = @[]
  let aliasNode = nodeGet(node, "aliases")
  if aliasNode.kind == JArray:
    for alias in aliasNode:
      let name = nodeString(alias)
      if name.len == 0:
        return err[Statement](ERR_SQL, "Invalid view column name")
      aliases.add(name)

  let queryRes = parseStatementNode(nodeGet(node, "query"))
  if not queryRes.ok:
    return err[Statement](queryRes.err.code, queryRes.err.message, queryRes.err.context)
  if queryRes.value.kind != skSelect:
    return err[Statement](ERR_SQL, "CREATE VIEW requires SELECT definition", viewName)

  let replace = nodeHas(node, "replace") and node["replace"].getBool
  ok(Statement(
    kind: skCreateView,
    createViewName: viewName,
    createViewIfNotExists: false,
    createViewOrReplace: replace,
    createViewColumns: aliases,
    createViewSqlText: selectToCanonicalSql(queryRes.value),
    createViewQuery: queryRes.value
  ))

proc parseDropStmt(node: JsonNode): Result[Statement] =
  let removeType = nodeGet(node, "removeType").getStr
  let missingOk = nodeHas(node, "missing_ok") and node["missing_ok"].getBool
  let objects = nodeGet(node, "objects")
  var name = ""
  if objects.kind == JArray and objects.len > 0:
    let obj = objects[0]
    if nodeHas(obj, "List"):
      let items = nodeGet(obj["List"], "items")
      if items.kind == JArray and items.len > 0:
        name = nodeString(items[^1])
    elif obj.kind == JArray and obj.len > 0:
      name = nodeString(obj[^1])
    else:
      name = nodeString(obj)
  if removeType == "OBJECT_INDEX":
    return ok(Statement(kind: skDropIndex, dropIndexName: name))
  if removeType == "OBJECT_VIEW":
    return ok(Statement(kind: skDropView, dropViewName: name, dropViewIfExists: missingOk))
  if removeType == "OBJECT_TABLE":
    return ok(Statement(kind: skDropTable, dropTableName: name))
  err[Statement](ERR_SQL, "Unsupported DROP object type", removeType)

proc parseRenameStmt(node: JsonNode): Result[Statement] =
  let renameType = nodeGet(node, "renameType").getStr
  if renameType != "OBJECT_VIEW":
    return err[Statement](ERR_SQL, "Unsupported RENAME statement", renameType)
  let viewRes = parseRangeVar(nodeGet(node, "relation"))
  if not viewRes.ok:
    return err[Statement](viewRes.err.code, viewRes.err.message, viewRes.err.context)
  let newName = nodeGet(node, "newname").getStr
  if newName.len == 0:
    return err[Statement](ERR_SQL, "ALTER VIEW RENAME requires new name")
  ok(Statement(kind: skAlterView, alterViewName: viewRes.value[0], alterViewNewName: newName))

proc parseTransactionStmt(node: JsonNode): Result[Statement] =
  let kindStr = nodeString(node["kind"])
  var kind = skBegin
  if kindStr == "TRANS_STMT_COMMIT":
    kind = skCommit
  elif kindStr == "TRANS_STMT_ROLLBACK":
    kind = skRollback
  elif kindStr == "TRANS_STMT_START" or kindStr == "TRANS_STMT_BEGIN":
    kind = skBegin
  else:
    return err[Statement](ERR_SQL, "Unsupported transaction statement", kindStr)
  ok(Statement(kind: kind))

proc parseColumnDef(node: JsonNode): Result[ColumnDef] =
  let colName = nodeGet(node, "colname").getStr
  let typeNode = nodeGet(node, "typeName")
  var typeName = ""
  if typeNode.kind == JObject:
    let names = nodeGet(typeNode, "names")
    if names.kind == JArray and names.len > 0:
      typeName = nodeString(names[^1])
      let typmods = nodeGet(typeNode, "typmods")
      if typmods.kind == JArray and typmods.len > 0:
        var mods: seq[string] = @[]
        for m in typmods:
          var n = m
          if nodeHas(n, "A_Const"):
            n = n["A_Const"]
          if nodeHas(n, "val"):
            n = n["val"]
          if nodeHas(n, "Integer"):
            n = n["Integer"]
          if nodeHas(n, "ival"):
            mods.add($n["ival"].getInt)
          elif n.kind == JInt:
            mods.add($n.getInt)
        if mods.len > 0:
          typeName.add("(" & mods.join(",") & ")")
  let notNull = nodeHas(node, "is_not_null") and node["is_not_null"].getBool
  let constraints = nodeGet(node, "constraints")
  var isPrimaryKey = false
  var isUnique = false
  if constraints.kind == JArray:
    for c in constraints:
      if nodeHas(c, "Constraint"):
        let contype = nodeGet(c["Constraint"], "contype").getStr
        if contype == "CONSTR_PRIMARY":
          isPrimaryKey = true
        elif contype == "CONSTR_UNIQUE":
          isUnique = true
  ok(ColumnDef(name: colName, typeName: typeName.toUpperAscii(), notNull: notNull, unique: isUnique, primaryKey: isPrimaryKey))

proc parseAlterTableStmt(node: JsonNode): Result[Statement] =
  let rel = unwrapRangeVar(node["relation"])
  let tableRes = parseRangeVar(rel)
  if not tableRes.ok:
    return err[Statement](tableRes.err.code, tableRes.err.message, tableRes.err.context)
  let tableName = tableRes.value[0]
  let cmdNode = nodeGet(node, "cmds")
  if cmdNode.kind != JArray or cmdNode.len == 0:
    return err[Statement](ERR_SQL, "ALTER TABLE requires at least one command")
  var actions: seq[AlterTableAction] = @[]
  for cmd in cmdNode:
    if not nodeHas(cmd, "AlterTableCmd"):
      continue
    let alterCmd = cmd["AlterTableCmd"]
    let subtype = nodeGet(alterCmd, "subtype").getStr
    var action: AlterTableAction
    case subtype
    of "AT_AddColumn":
      if nodeHas(alterCmd, "def"):
        let defNode = alterCmd["def"]
        if nodeHas(defNode, "ColumnDef"):
          let colRes = parseColumnDef(defNode["ColumnDef"])
          if not colRes.ok:
            return err[Statement](colRes.err.code, colRes.err.message, colRes.err.context)
          action = AlterTableAction(kind: ataAddColumn, columnDef: colRes.value)
        else:
          return err[Statement](ERR_SQL, "ADD COLUMN requires column definition")
      else:
        return err[Statement](ERR_SQL, "ADD COLUMN requires column definition")
    of "AT_DropColumn":
      let colName = nodeGet(alterCmd, "name").getStr
      action = AlterTableAction(kind: ataDropColumn, columnName: colName)
    of "AT_ColumnDefault":
      let colName = nodeGet(alterCmd, "name").getStr
      if nodeHas(alterCmd, "def"):
        let defExpr = parseExprNode(alterCmd["def"])
        if not defExpr.ok:
          return err[Statement](defExpr.err.code, defExpr.err.message, defExpr.err.context)
        action = AlterTableAction(kind: ataAlterColumn, columnName: colName, alterColumnAction: acaSetDefault, alterColumnDefault: defExpr.value)
      else:
        action = AlterTableAction(kind: ataAlterColumn, columnName: colName, alterColumnAction: acaDropDefault)
    else:
      return err[Statement](ERR_SQL, "Unsupported ALTER TABLE operation", subtype)
    actions.add(action)
  ok(Statement(kind: skAlterTable, alterTableName: tableName, alterActions: actions))

proc parseExplainStmt(node: JsonNode): Result[Statement] =
  if nodeHas(node, "options"):
    let opts = node["options"]
    if opts.kind == JArray and opts.len > 0:
      return err[Statement](ERR_SQL, "EXPLAIN options not supported")

  let queryNode = nodeGet(node, "query")
  # nodeGet returns JNull if missing, or the node.
  # But we want to ensure it's a valid statement node.
  # PG query structure for ExplainStmt has "query" field.
  
  let innerRes = parseStatementNode(queryNode)
  if not innerRes.ok:
    return err[Statement](innerRes.err.code, innerRes.err.message, innerRes.err.context)
  
  ok(Statement(kind: skExplain, explainInner: innerRes.value, explainHasOptions: false))

proc parseStatementNode(node: JsonNode): Result[Statement] =
  if nodeHas(node, "ExplainStmt"):
    return parseExplainStmt(node["ExplainStmt"])
  if nodeHas(node, "SelectStmt"):
    return parseSelectStmt(node["SelectStmt"])
  if nodeHas(node, "InsertStmt"):
    return parseInsertStmt(node["InsertStmt"])
  if nodeHas(node, "UpdateStmt"):
    return parseUpdateStmt(node["UpdateStmt"])
  if nodeHas(node, "DeleteStmt"):
    return parseDeleteStmt(node["DeleteStmt"])
  if nodeHas(node, "CreateStmt"):
    return parseCreateStmt(node["CreateStmt"])
  if nodeHas(node, "IndexStmt"):
    return parseIndexStmt(node["IndexStmt"])
  if nodeHas(node, "ViewStmt"):
    return parseViewStmt(node["ViewStmt"])
  if nodeHas(node, "DropStmt"):
    return parseDropStmt(node["DropStmt"])
  if nodeHas(node, "RenameStmt"):
    return parseRenameStmt(node["RenameStmt"])
  if nodeHas(node, "AlterTableStmt"):
    return parseAlterTableStmt(node["AlterTableStmt"])
  if nodeHas(node, "TransactionStmt"):
    return parseTransactionStmt(node["TransactionStmt"])
  err[Statement](ERR_SQL, "Unsupported statement node")

proc parseSql*(sql: string): Result[SqlAst] =
  # Fast-path parser for a tiny subset of SELECT statements.
  #
  # This avoids the cost of pg_query + JSON parsing on common CLI benchmark queries,
  # while still falling back to the full parser for everything else.
  proc tryParseFastSelect(sqlText: string): SqlAst =
    var i = 0
    proc skipWs() =
      while i < sqlText.len and sqlText[i].isSpaceAscii:
        i.inc

    proc consumeChar(ch: char): bool =
      skipWs()
      if i < sqlText.len and sqlText[i] == ch:
        i.inc
        return true
      false

    proc consumeKeyword(word: string): bool =
      skipWs()
      let start = i
      if start + word.len > sqlText.len:
        return false
      # Case-insensitive match.
      for k in 0 ..< word.len:
        if toUpperAscii(sqlText[start + k]) != toUpperAscii(word[k]):
          return false
      let endPos = start + word.len
      # Require a boundary (whitespace/punct/end) after keyword.
      if endPos < sqlText.len:
        let c = sqlText[endPos]
        if c.isAlphaNumeric or c == '_':
          return false
      i = endPos
      true

    proc parseIdent(): string =
      skipWs()
      if i >= sqlText.len:
        return ""
      # Unquoted identifier: [A-Za-z_][A-Za-z0-9_]*
      let c0 = sqlText[i]
      if not (c0.isAlphaAscii or c0 == '_'):
        return ""
      let start = i
      i.inc
      while i < sqlText.len:
        let c = sqlText[i]
        if c.isAlphaNumeric or c == '_':
          i.inc
        else:
          break
      toLowerAscii(sqlText[start ..< i])

    proc parseInt64Literal(outVal: var int64): bool =
      skipWs()
      if i >= sqlText.len:
        return false
      var sign = 1'i64
      if sqlText[i] == '-':
        sign = -1
        i.inc
      elif sqlText[i] == '+':
        i.inc
      if i >= sqlText.len or not sqlText[i].isDigit:
        return false
      var v: int64 = 0
      while i < sqlText.len and sqlText[i].isDigit:
        let d = int64(ord(sqlText[i]) - ord('0'))
        # Avoid overflow; fall back to full parser if suspicious.
        if v > (high(int64) - d) div 10:
          return false
        v = v * 10 + d
        i.inc
      outVal = v * sign
      true

    proc parseSqlStringLiteral(outVal: var string): bool =
      skipWs()
      if i >= sqlText.len or sqlText[i] != '\'':
        return false
      i.inc
      outVal = ""
      while i < sqlText.len:
        let c = sqlText[i]
        if c == '\'':
          # '' inside string -> single quote
          if i + 1 < sqlText.len and sqlText[i + 1] == '\'':
            outVal.add('\'')
            i += 2
            continue
          i.inc
          return true
        outVal.add(c)
        i.inc
      false

    # SELECT * FROM <table> WHERE <col> (=|LIKE|ILIKE) (<int>|'pattern') [;]
    if not consumeKeyword("SELECT"):
      return nil
    if not consumeChar('*'):
      return nil
    if not consumeKeyword("FROM"):
      return nil
    let tableName = parseIdent()
    if tableName.len == 0:
      return nil
    if not consumeKeyword("WHERE"):
      return nil
    let colName = parseIdent()
    if colName.len == 0:
      return nil

    var op = ""
    skipWs()
    if consumeChar('='):
      op = "="
    else:
      if consumeKeyword("ILIKE"):
        op = "ILIKE"
      elif consumeKeyword("LIKE"):
        op = "LIKE"
      else:
        return nil

    var rhs: Expr = nil
    if op == "=":
      var v: int64 = 0
      if not parseInt64Literal(v):
        return nil
      rhs = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: v))
    else:
      var s = ""
      if not parseSqlStringLiteral(s):
        return nil
      rhs = Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: s))

    let lhs = Expr(kind: ekColumn, table: "", name: colName)
    let whereExpr = Expr(kind: ekBinary, op: op, left: lhs, right: rhs)

    # Optional trailing semicolon + whitespace
    discard consumeChar(';')
    skipWs()
    if i != sqlText.len:
      return nil

    let stmt = Statement(
      kind: skSelect,
      selectItems: @[SelectItem(expr: nil, alias: "", isStar: true)],
      fromTable: tableName,
      fromAlias: "",
      joins: @[],
      whereExpr: whereExpr,
      groupBy: @[],
      havingExpr: nil,
      orderBy: @[],
      limit: -1,
      limitParam: 0,
      offset: -1,
      offsetParam: 0
    )
    SqlAst(statements: @[stmt])

  proc rewriteCreateViewIfNotExists(sqlText: string): string =
    var i = 0

    proc skipWs() =
      while i < sqlText.len and sqlText[i].isSpaceAscii:
        i.inc

    proc consumeKeyword(word: string): bool =
      skipWs()
      let start = i
      if start + word.len > sqlText.len:
        return false
      for k in 0 ..< word.len:
        if toUpperAscii(sqlText[start + k]) != toUpperAscii(word[k]):
          return false
      let endPos = start + word.len
      if endPos < sqlText.len:
        let c = sqlText[endPos]
        if c.isAlphaNumeric or c == '_':
          return false
      i = endPos
      true

    skipWs()
    if not consumeKeyword("CREATE"):
      return ""
    if not consumeKeyword("VIEW"):
      return ""

    let ifStart = i
    if not consumeKeyword("IF"):
      return ""
    if not consumeKeyword("NOT"):
      return ""
    if not consumeKeyword("EXISTS"):
      return ""

    sqlText[0 ..< ifStart] & " " & sqlText[i .. ^1]

  var isExplain = false
  let rewrittenCreateView = rewriteCreateViewIfNotExists(sql)
  if rewrittenCreateView.len > 0:
    let rewrittenRes = parseSql(rewrittenCreateView)
    if not rewrittenRes.ok:
      return rewrittenRes
    if rewrittenRes.value.statements.len != 1 or rewrittenRes.value.statements[0].kind != skCreateView:
      return err[SqlAst](ERR_SQL, "Invalid CREATE VIEW IF NOT EXISTS statement")
    rewrittenRes.value.statements[0].createViewIfNotExists = true
    return rewrittenRes

  if sql.len >= 7:
    # Check for case-insensitive "EXPLAIN" prefix (ignoring leading whitespace is handled by tryParseFastSelect but here we want to avoid it)
    # Actually, tryParseFastSelect handles whitespace. 
    # But checking strictly for "EXPLAIN" prefix might miss "  EXPLAIN".
    # However, tryParseFastSelect is very strict. It expects SELECT.
    # So if it starts with EXPLAIN, tryParseFastSelect will likely fail anyway or return nil.
    # The requirement is: "Ensure EXPLAIN never goes through the fast-path."
    # "Before calling tryParseFastSelect(sql), check if the input starts with EXPLAIN (case-insensitive, leading whitespace allowed). If so, skip fast-path and go directly to pg_query."
    
    var i = 0
    while i < sql.len and sql[i].isSpaceAscii: i.inc
    if i + 7 <= sql.len:
      var match = true
      let target = "EXPLAIN"
      for k in 0 ..< 7:
        if sql[i+k].toUpperAscii != target[k]:
          match = false
          break
      if match:
        isExplain = true

  if not isExplain:
    let fastAst = tryParseFastSelect(sql)
    if fastAst != nil:
      return ok(fastAst)

  when not defined(libpg_query):
    return err[SqlAst](ERR_INTERNAL, "libpg_query required", "build with -d:libpg_query and link libpg_query")
  let parseResult = pg_query_parse(sql.cstring)
  defer: pg_query_free_parse_result(parseResult)
  if parseResult.error.message != nil:
    return err[SqlAst](ERR_SQL, $parseResult.error.message)
  if parseResult.parse_tree == nil:
    return err[SqlAst](ERR_SQL, "Empty parse tree")
  let jsonText = $parseResult.parse_tree
  let root = parseJson(jsonText)
  let stmts = nodeGet(root, "stmts")
  var statements: seq[Statement] = @[]
  if stmts.kind == JArray:
    for entry in stmts:
      let stmtNode = entry["stmt"]
      let stmtRes = parseStatementNode(stmtNode)
      if not stmtRes.ok:
        return err[SqlAst](stmtRes.err.code, stmtRes.err.message, stmtRes.err.context)
      statements.add(stmtRes.value)
  ok(SqlAst(statements: statements))
