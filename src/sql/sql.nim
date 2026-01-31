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
  skAlterTable
  skInsert
  skSelect
  skUpdate
  skDelete
  skBegin
  skCommit
  skRollback

type Statement* = ref object
  case kind*: StatementKind
  of skCreateTable:
    createTableName*: string
    columns*: seq[ColumnDef]
  of skCreateIndex:
    indexName*: string
    indexTableName*: string
    columnName*: string
    indexKind*: SqlIndexKind
    unique*: bool
  of skDropTable:
    dropTableName*: string
  of skDropIndex:
    dropIndexName*: string
  of skAlterTable:
    alterTableName*: string
    alterActions*: seq[AlterTableAction]
  of skInsert:
    insertTable*: string
    insertColumns*: seq[string]
    insertValues*: seq[Expr]
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
  
  # Check for IN expression (kind = 10 for AEXPR_IN in PostgreSQL)
  let kindNode = nodeGet(node, "kind")
  if kindNode.kind == JInt and kindNode.getInt == 10:
    # This is an IN expression
    let leftRes = parseExprNode(nodeGet(node, "lexpr"))
    if not leftRes.ok:
      return err[Expr](leftRes.err.code, leftRes.err.message, leftRes.err.context)
    
    # Parse the list of values from rexpr
    var inList: seq[Expr] = @[]
    let rexpr = nodeGet(node, "rexpr")
    if rexpr.kind == JArray:
      for item in rexpr:
        let itemRes = parseExprNode(item)
        if not itemRes.ok:
          return err[Expr](itemRes.err.code, itemRes.err.message, itemRes.err.context)
        inList.add(itemRes.value)
    
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

proc parseTypeCast(node: JsonNode): Result[Expr] =
  let argRes = parseExprNode(nodeGet(node, "arg"))
  if not argRes.ok:
    return err[Expr](argRes.err.code, argRes.err.message, argRes.err.context)
  ok(argRes.value)

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
    if nodeHas(node, "ParamRef"):
      return parseParamRef(node["ParamRef"])
    if nodeHas(node, "TypeCast"):
      return parseTypeCast(node["TypeCast"])
    if nodeHas(node, "NullTest"):
      return parseNullTest(node["NullTest"])
  err[Expr](ERR_SQL, "Unsupported expression node")

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
  ok(Statement(kind: skInsert, insertTable: tableRes.value[0], insertColumns: cols, insertValues: values))

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
      if keys.kind != JArray or keys.len != 1:
        return err[Statement](ERR_SQL, "Only single-column constraints supported")
      let colName = nodeString(keys[0])
      var found = false
      for i, col in columns:
        if col.name == colName:
          found = true
          columns[i].unique = true
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
  var columnName = ""
  let params = nodeGet(node, "indexParams")
  if params.kind == JArray and params.len > 0:
    let param = params[0]["IndexElem"]
    if nodeHas(param, "name"):
      columnName = param["name"].getStr
  var kind = ikBtree
  let methodName = nodeGet(node, "accessMethod").getStr
  if methodName.len > 0:
    let methodLower = methodName.toLowerAscii()
    if methodLower == "trigram":
      kind = ikTrigram
  let unique = nodeHas(node, "unique") and node["unique"].getBool
  ok(Statement(kind: skCreateIndex, indexName: idxName, indexTableName: tableRes.value[0], columnName: columnName, indexKind: kind, unique: unique))

proc parseDropStmt(node: JsonNode): Result[Statement] =
  let removeType = nodeGet(node, "removeType").getStr
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
  ok(Statement(kind: skDropTable, dropTableName: name))

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
  let notNull = nodeHas(node, "is_not_null") and node["is_not_null"].getBool
  let isNull = nodeHas(node, "is_null") and node["is_null"].getBool
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

proc parseStatementNode(node: JsonNode): Result[Statement] =
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
  if nodeHas(node, "DropStmt"):
    return parseDropStmt(node["DropStmt"])
  if nodeHas(node, "AlterTableStmt"):
    return parseAlterTableStmt(node["AlterTableStmt"])
  if nodeHas(node, "TransactionStmt"):
    return parseTransactionStmt(node["TransactionStmt"])
  err[Statement](ERR_SQL, "Unsupported statement node")

proc parseSql*(sql: string): Result[SqlAst] =
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
