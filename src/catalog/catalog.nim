import options
import tables
import sets
import algorithm
import strutils
import json
import ../errors
import ../sql/sql
import ../record/record
import ../pager/pager
import ../pager/db_header
import ../btree/btree

type ColumnType* = enum
  ctInt64
  ctBool
  ctFloat64
  ctText
  ctBlob
  ctDecimal
  ctUuid
  ctDateTime

type ColumnTypeSpec* = object
  kind*: ColumnType
  decPrecision*: uint8
  decScale*: uint8

type IndexKind* = enum
  ikBtree
  ikTrigram

type Column* = object
  name*: string
  kind*: ColumnType
  notNull*: bool
  unique*: bool
  primaryKey*: bool
  refTable*: string
  refColumn*: string
  refOnDelete*: string
  refOnUpdate*: string
  decPrecision*: uint8
  decScale*: uint8
  defaultExpr*: string  ## SQL text of the DEFAULT expression (empty = no default)
  generatedExpr*: string  ## SQL text of GENERATED ALWAYS AS expr STORED (empty = not generated)

type CheckConstraint* = object
  name*: string
  exprSql*: string

type TableMeta* = object
  name*: string
  rootPage*: PageId
  nextRowId*: uint64
  columns*: seq[Column]
  checks*: seq[CheckConstraint]
  temporary*: bool

type ViewMeta* = object
  name*: string
  sqlText*: string
  columnNames*: seq[string]
  dependencies*: seq[string]
  temporary*: bool

type TriggerMeta* = object
  name*: string
  table*: string
  eventsMask*: int
  actionSql*: string
  temporary*: bool

type ReferencingChildFk* = object
  tableName*: string
  columnName*: string
  onDelete*: string
  onUpdate*: string

type IndexMeta* = object
  name*: string
  table*: string
  columns*: seq[string]
  rootPage*: PageId
  kind*: IndexKind
  unique*: bool
  predicateSql*: string
  temporary*: bool

type TrigramDelta* = ref object
  adds*: HashSet[uint64]
  removes*: HashSet[uint64]

type TableStats* = object
  rowCount*: int64

type IndexStats* = object
  entryCount*: int64
  distinctKeyCount*: int64

type Catalog* = ref object
  tables*: Table[string, TableMeta]
  indexes*: Table[string, IndexMeta]
  views*: Table[string, ViewMeta]
  triggers*: Table[string, TriggerMeta]
  dependentViews*: Table[string, HashSet[string]]
  reverseFkRefs*: Table[(string, string), seq[ReferencingChildFk]]
  tablesWithReferencingChildren*: HashSet[string]
  triggerEventMaskByTable*: Table[string, int]
  catalogTree*: BTree
  trigramDeltas*: Table[(string, uint32), TrigramDelta]
  tableStats*: Table[string, TableStats]
  indexStats*: Table[string, IndexStats]
  rowCountDeltas*: Table[string, int64]

type CatalogRecordKind = enum
  crTable
  crIndex
  crView
  crTrigger
  crTableStats
  crIndexStats

type CatalogRecord = object
  kind: CatalogRecordKind
  table: TableMeta
  index: IndexMeta
  view: ViewMeta
  trigger: TriggerMeta
  tableStats: TableStats
  tableStatsName: string
  indexStats: IndexStats
  indexStatsName: string

proc parseColumnType*(text: string): Result[ColumnTypeSpec] =
  let raw = text.strip()
  if raw.len == 0:
    return err[ColumnTypeSpec](ERR_SQL, "Unsupported column type", text)

  var baseType = raw
  var mods = ""
  let parenPos = raw.find('(')
  if parenPos >= 0:
    baseType = raw[0..<parenPos].strip()
    mods = raw[parenPos..^1].strip()

  let baseUpper = baseType.toUpperAscii()
  case baseUpper
  of "INT", "INTEGER", "INT64", "BIGINT", "INT4", "INT8":
    ok(ColumnTypeSpec(kind: ctInt64))
  of "BOOL", "BOOLEAN":
    ok(ColumnTypeSpec(kind: ctBool))
  of "FLOAT", "FLOAT64", "DOUBLE", "FLOAT8", "FLOAT4", "REAL":
    ok(ColumnTypeSpec(kind: ctFloat64))
  of "TEXT", "VARCHAR", "CHARACTER VARYING":
    ok(ColumnTypeSpec(kind: ctText))
  of "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE", "DATETIME":
    ok(ColumnTypeSpec(kind: ctDateTime))
  of "BLOB":
    ok(ColumnTypeSpec(kind: ctBlob))
  of "UUID":
    ok(ColumnTypeSpec(kind: ctUuid))
  of "DECIMAL", "NUMERIC":
    if mods.len == 0:
      return err[ColumnTypeSpec](ERR_SQL, "DECIMAL/NUMERIC requires (p,s)", text)
    if not (mods.startsWith("(") and mods.endsWith(")")):
      return err[ColumnTypeSpec](ERR_SQL, "Invalid DECIMAL/NUMERIC modifiers", text)
    let inner = mods[1 ..< mods.len-1]
    let parts = inner.split(",")
    if parts.len != 2:
      return err[ColumnTypeSpec](ERR_SQL, "DECIMAL/NUMERIC requires (p,s)", text)
    let pStr = parts[0].strip()
    let sStr = parts[1].strip()
    var pInt: int
    var sInt: int
    try:
      pInt = parseInt(pStr)
      sInt = parseInt(sStr)
    except ValueError:
      return err[ColumnTypeSpec](ERR_SQL, "Invalid DECIMAL/NUMERIC (p,s)", text)
    if pInt <= 0 or pInt > 18:
      return err[ColumnTypeSpec](ERR_SQL, "DECIMAL precision must be 1..18", text)
    if sInt < 0 or sInt > pInt:
      return err[ColumnTypeSpec](ERR_SQL, "DECIMAL scale must be 0..p", text)
    ok(ColumnTypeSpec(kind: ctDecimal, decPrecision: uint8(pInt), decScale: uint8(sInt)))
  else:
    err[ColumnTypeSpec](ERR_SQL, "Unsupported column type", text)

proc columnTypeToText*(kind: ColumnType): string =
  case kind
  of ctInt64: "INT64"
  of ctBool: "BOOL"
  of ctFloat64: "FLOAT64"
  of ctText: "TEXT"
  of ctBlob: "BLOB"
  of ctDecimal: "DECIMAL"
  of ctUuid: "UUID"
  of ctDateTime: "TIMESTAMP"

proc normalizeFkAction(action: string): string =
  let upper = action.strip().toUpperAscii()
  if upper.len == 0:
    return "NO ACTION"
  if upper in ["NO ACTION", "RESTRICT", "CASCADE", "SET NULL"]:
    return upper
  "NO ACTION"

proc fkActionToCode(action: string): string =
  case normalizeFkAction(action)
  of "NO ACTION": "a"
  of "RESTRICT": "r"
  of "CASCADE": "c"
  of "SET NULL": "n"
  else: "a"

proc fkActionFromCode(code: string): string =
  case code.toLowerAscii()
  of "", "a": "NO ACTION"
  of "r": "RESTRICT"
  of "c": "CASCADE"
  of "n": "SET NULL"
  else: "NO ACTION"

proc quoteSqlStringLiteral(text: string): string =
  "'" & text.replace("'", "''") & "'"

proc columnTypeSql*(col: Column): string =
  if col.kind == ctDecimal and col.decPrecision > 0:
    return "DECIMAL(" & $col.decPrecision & "," & $col.decScale & ")"
  columnTypeToText(col.kind)

proc quotedIdentList(names: openArray[string]): string =
  var parts: seq[string] = @[]
  for name in names:
    parts.add(quoteIdent(name))
  parts.join(", ")

proc indexKeySql(name: string): string =
  if name.startsWith(IndexExpressionPrefix):
    return name[IndexExpressionPrefix.len .. ^1]
  quoteIdent(name)

proc tableCheckSql(checkDef: CheckConstraint): string =
  if checkDef.name.len > 0:
    return "CONSTRAINT " & quoteIdent(checkDef.name) & " CHECK " & checkDef.exprSql
  "CHECK " & checkDef.exprSql

proc tableColumnSql(col: Column, inlinePrimaryKey: bool): string =
  var parts: seq[string] = @[quoteIdent(col.name), columnTypeSql(col)]
  if col.notNull:
    parts.add("NOT NULL")
  if col.unique:
    parts.add("UNIQUE")
  if inlinePrimaryKey:
    parts.add("PRIMARY KEY")
  if col.defaultExpr.len > 0:
    parts.add("DEFAULT " & col.defaultExpr)
  if col.generatedExpr.len > 0:
    parts.add("GENERATED ALWAYS AS (" & col.generatedExpr & ") STORED")
  if col.refTable.len > 0 and col.refColumn.len > 0:
    parts.add("REFERENCES " & quoteIdent(col.refTable) & "(" & quoteIdent(col.refColumn) & ")")
    parts.add("ON DELETE " & normalizeFkAction(col.refOnDelete))
    parts.add("ON UPDATE " & normalizeFkAction(col.refOnUpdate))
  parts.join(" ")

proc tableDdl*(table: TableMeta): string =
  var parts: seq[string] = @[]
  var pkCols: seq[string] = @[]
  for col in table.columns:
    if col.primaryKey:
      pkCols.add(col.name)
  let inlinePk = pkCols.len == 1
  for col in table.columns:
    parts.add(tableColumnSql(col, inlinePk and col.primaryKey))
  if pkCols.len > 1:
    parts.add("PRIMARY KEY (" & quotedIdentList(pkCols) & ")")
  for checkDef in table.checks:
    parts.add(tableCheckSql(checkDef))
  let createPrefix = if table.temporary: "CREATE TEMP TABLE " else: "CREATE TABLE "
  createPrefix & quoteIdent(table.name) & " (" & parts.join(", ") & ")"

proc viewDdl*(view: ViewMeta): string =
  let createPrefix = if view.temporary: "CREATE TEMP VIEW " else: "CREATE VIEW "
  createPrefix & quoteIdent(view.name) & " AS " & view.sqlText

proc indexDdl*(index: IndexMeta): string =
  var columnsSql: seq[string] = @[]
  for colName in index.columns:
    columnsSql.add(indexKeySql(colName))
  let createPrefix = if index.unique: "CREATE UNIQUE INDEX " else: "CREATE INDEX "
  var sqlText = createPrefix & quoteIdent(index.name) & " ON " & quoteIdent(index.table)
  if index.kind == ikTrigram:
    sqlText.add(" USING trigram")
  sqlText.add(" (" & columnsSql.join(", ") & ")")
  if index.predicateSql.len > 0:
    sqlText.add(" WHERE " & index.predicateSql)
  sqlText

proc triggerTimingName*(eventsMask: int): string =
  if (eventsMask and TriggerTimingInsteadMask) != 0:
    return "instead_of"
  "after"

proc triggerTimingSql(eventsMask: int): string =
  if (eventsMask and TriggerTimingInsteadMask) != 0:
    return "INSTEAD OF"
  "AFTER"

proc triggerEventNames*(eventsMask: int): seq[string] =
  if (eventsMask and TriggerEventInsertMask) != 0:
    result.add("insert")
  if (eventsMask and TriggerEventUpdateMask) != 0:
    result.add("update")
  if (eventsMask and TriggerEventDeleteMask) != 0:
    result.add("delete")
  if (eventsMask and TriggerEventTruncateMask) != 0:
    result.add("truncate")

proc triggerEventSql(eventsMask: int): string =
  var parts: seq[string] = @[]
  for name in triggerEventNames(eventsMask):
    parts.add(name.toUpperAscii())
  parts.join(" OR ")

proc triggerTargetKindName*(eventsMask: int): string =
  if (eventsMask and TriggerTimingInsteadMask) != 0:
    return "view"
  "table"

proc triggerDdl*(trigger: TriggerMeta): string =
  var sqlText = "CREATE TRIGGER " & quoteIdent(trigger.name) & " " &
    triggerTimingSql(trigger.eventsMask) & " " & triggerEventSql(trigger.eventsMask) &
    " ON " & quoteIdent(trigger.table) &
    " FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql(" &
    quoteSqlStringLiteral(trigger.actionSql) & ")"
  sqlText

proc encodeColumns(columns: seq[Column]): seq[byte] =
  var parts: seq[string] = @[]
  for col in columns:
    var flags: seq[string] = @[]
    if col.notNull:
      flags.add("nn")
    if col.unique:
      flags.add("unique")
    if col.primaryKey:
      flags.add("pk")
    if col.refTable.len > 0 and col.refColumn.len > 0:
      flags.add("ref=" & col.refTable & "." & col.refColumn)
      flags.add("refdel=" & fkActionToCode(col.refOnDelete))
      flags.add("refupd=" & fkActionToCode(col.refOnUpdate))
    if col.defaultExpr.len > 0:
      # Percent-encode special delimiters to avoid conflicts with column encoding format
      var encoded = col.defaultExpr.replace("%", "%25").replace(";", "%3B").replace(":", "%3A").replace(",", "%2C")
      flags.add("default=" & encoded)
    if col.generatedExpr.len > 0:
      var encoded = col.generatedExpr.replace("%", "%25").replace(";", "%3B").replace(":", "%3A").replace(",", "%2C")
      flags.add("gen=" & encoded)
    let flagPart = if flags.len > 0: ":" & flags.join(",") else: ""
    let typeText =
      if col.kind == ctDecimal:
        "DECIMAL(" & $col.decPrecision & "," & $col.decScale & ")"
      else:
        columnTypeToText(col.kind)
    parts.add(col.name & ":" & typeText & flagPart)
  let joined = parts.join(";")
  var bytes: seq[byte] = @[]
  for ch in joined:
    bytes.add(byte(ch))
  bytes

proc decodeColumns(bytes: seq[byte]): seq[Column] =
  var s = ""
  for b in bytes:
    s.add(char(b))
  if s.len == 0:
    return @[]
  let parts = s.split(";")
  for part in parts:
    let pieces = part.split(":")
    if pieces.len >= 2:
      let typeRes = parseColumnType(pieces[1])
      if typeRes.ok:
        var col = Column(name: pieces[0], kind: typeRes.value.kind)
        if col.kind == ctDecimal:
          col.decPrecision = typeRes.value.decPrecision
          col.decScale = typeRes.value.decScale
        if pieces.len >= 3:
          let flags = pieces[2].split(",")
          for flag in flags:
            case flag
            of "nn":
              col.notNull = true
            of "unique":
              col.unique = true
            of "pk":
              col.primaryKey = true
            else:
              if flag.startsWith("ref="):
                let target = flag[4 .. ^1]
                let parts = target.split(".")
                if parts.len == 2:
                  col.refTable = parts[0]
                  col.refColumn = parts[1]
              elif flag.startsWith("refdel="):
                col.refOnDelete = fkActionFromCode(flag[7 .. ^1])
              elif flag.startsWith("refupd="):
                col.refOnUpdate = fkActionFromCode(flag[7 .. ^1])
              elif flag.startsWith("default="):
                var encoded = flag[8 .. ^1]
                col.defaultExpr = encoded.replace("%2C", ",").replace("%3A", ":").replace("%3B", ";").replace("%25", "%")
              elif flag.startsWith("gen="):
                var encoded = flag[4 .. ^1]
                col.generatedExpr = encoded.replace("%2C", ",").replace("%3A", ":").replace("%3B", ";").replace("%25", "%")
        if col.refTable.len > 0 and col.refColumn.len > 0:
          if col.refOnDelete.len == 0:
            col.refOnDelete = "NO ACTION"
          if col.refOnUpdate.len == 0:
            col.refOnUpdate = "NO ACTION"
        result.add(col)

proc encodeChecks(checks: seq[CheckConstraint]): seq[byte] =
  var arr = newJArray()
  for checkDef in checks:
    var node = newJObject()
    node["name"] = %checkDef.name
    node["expr"] = %checkDef.exprSql
    arr.add(node)
  let payload = $arr
  result = newSeq[byte](payload.len)
  for i, ch in payload:
    result[i] = byte(ch)

proc decodeChecks(bytes: seq[byte]): Result[seq[CheckConstraint]] =
  if bytes.len == 0:
    return ok(newSeq[CheckConstraint]())
  var checks: seq[CheckConstraint] = @[]
  var payload = newString(bytes.len)
  for i, b in bytes:
    payload[i] = char(b)
  try:
    let node = parseJson(payload)
    if node.kind != JArray:
      return err[seq[CheckConstraint]](ERR_CORRUPTION, "Invalid CHECK metadata format")
    for item in node.items:
      if item.kind != JObject:
        return err[seq[CheckConstraint]](ERR_CORRUPTION, "Invalid CHECK metadata entry")
      let exprSql = if item.hasKey("expr"): item["expr"].getStr else: ""
      if exprSql.len == 0:
        return err[seq[CheckConstraint]](ERR_CORRUPTION, "CHECK metadata missing expression")
      let name = if item.hasKey("name"): item["name"].getStr else: ""
      checks.add(CheckConstraint(name: name, exprSql: exprSql))
  except CatchableError:
    return err[seq[CheckConstraint]](ERR_CORRUPTION, "Invalid CHECK metadata JSON")
  ok(checks)

proc stringToBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

proc bytesToString(bytes: seq[byte]): string =
  for b in bytes:
    result.add(char(b))

proc makeTableRecord(name: string, rootPage: PageId, nextRowId: uint64, columns: seq[Column], checks: seq[CheckConstraint]): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("table")),
    Value(kind: vkText, bytes: stringToBytes(name)),
    Value(kind: vkInt64, int64Val: int64(rootPage)),
    Value(kind: vkInt64, int64Val: int64(nextRowId)),
    Value(kind: vkText, bytes: encodeColumns(columns)),
    Value(kind: vkText, bytes: encodeChecks(checks))
  ]
  encodeRecord(values)

proc makeIndexRecord(name: string, table: string, columns: seq[string], rootPage: PageId, kind: IndexKind, unique: bool, predicateSql: string): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("index")),
    Value(kind: vkText, bytes: stringToBytes(name)),
    Value(kind: vkText, bytes: stringToBytes(table)),
    Value(kind: vkText, bytes: stringToBytes(columns.join(";"))),
    Value(kind: vkInt64, int64Val: int64(rootPage)),
    Value(kind: vkText, bytes: stringToBytes(if kind == ikTrigram: "trigram" else: "btree")),
    Value(kind: vkInt64, int64Val: int64(if unique: 1 else: 0)),
    Value(kind: vkText, bytes: stringToBytes(predicateSql))
  ]
  encodeRecord(values)

proc makeViewRecord(name: string, sqlText: string, columnNames: seq[string], dependencies: seq[string]): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("view")),
    Value(kind: vkText, bytes: stringToBytes(name)),
    Value(kind: vkText, bytes: stringToBytes(sqlText)),
    Value(kind: vkText, bytes: stringToBytes(columnNames.join(";"))),
    Value(kind: vkText, bytes: stringToBytes(dependencies.join(";")))
  ]
  encodeRecord(values)

proc makeTriggerRecord(name: string, tableName: string, eventsMask: int, actionSql: string): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("trigger")),
    Value(kind: vkText, bytes: stringToBytes(name)),
    Value(kind: vkText, bytes: stringToBytes(tableName)),
    Value(kind: vkInt64, int64Val: int64(eventsMask)),
    Value(kind: vkText, bytes: stringToBytes(actionSql))
  ]
  encodeRecord(values)

proc makeTableStatsRecord(normName: string, rowCount: int64): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("stats:table")),
    Value(kind: vkText, bytes: stringToBytes(normName)),
    Value(kind: vkInt64, int64Val: rowCount)
  ]
  encodeRecord(values)

proc makeIndexStatsRecord(normName: string, entryCount: int64, distinctKeyCount: int64): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("stats:index")),
    Value(kind: vkText, bytes: stringToBytes(normName)),
    Value(kind: vkInt64, int64Val: entryCount),
    Value(kind: vkInt64, int64Val: distinctKeyCount)
  ]
  encodeRecord(values)

proc parseCatalogRecord(data: seq[byte]): Result[CatalogRecord] =
  let decoded = decodeRecord(data)
  if not decoded.ok:
    return err[CatalogRecord](decoded.err.code, decoded.err.message, decoded.err.context)
  let values = decoded.value
  if values.len < 1:
    return err[CatalogRecord](ERR_CORRUPTION, "Empty catalog record")
  # Detect old compact format (4 values: name, rootPage, nextRowId, columns).
  # Only treat as compact if the first field is not a known type discriminator.
  if values.len == 4 and values[0].kind == vkText:
    let firstStr = bytesToString(values[0].bytes).toLowerAscii()
    if firstStr notin ["table", "index", "view", "trigger", "stats:table", "stats:index"]:
      let name = bytesToString(values[0].bytes)
      let rootPage = PageId(values[1].int64Val)
      let nextRowId = uint64(values[2].int64Val)
      let columns = decodeColumns(values[3].bytes)
      return ok(CatalogRecord(kind: crTable, table: TableMeta(name: name, rootPage: rootPage, nextRowId: nextRowId, columns: columns, checks: @[])))
  if values.len < 2:
    return err[CatalogRecord](ERR_CORRUPTION, "Catalog record too short")
  let recordType = bytesToString(values[0].bytes).toLowerAscii()
  if recordType == "table":
    let name = bytesToString(values[1].bytes)
    let rootPage = PageId(values[2].int64Val)
    let nextRowId = uint64(values[3].int64Val)
    let columns = decodeColumns(values[4].bytes)
    let checksRes =
      if values.len >= 6:
        decodeChecks(values[5].bytes)
      else:
        ok(newSeq[CheckConstraint]())
    if not checksRes.ok:
      return err[CatalogRecord](checksRes.err.code, checksRes.err.message, checksRes.err.context)
    let checks = checksRes.value
    return ok(CatalogRecord(kind: crTable, table: TableMeta(name: name, rootPage: rootPage, nextRowId: nextRowId, columns: columns, checks: checks)))
  if recordType == "index":
    if values.len < 5:
      return err[CatalogRecord](ERR_CORRUPTION, "Index catalog record too short")
    let name = bytesToString(values[1].bytes)
    let tableName = bytesToString(values[2].bytes)
    let columnStr = bytesToString(values[3].bytes)
    let columns = if columnStr.len > 0: columnStr.split(";") else: @[]
    let rootPage = PageId(values[4].int64Val)
    var kind = ikBtree
    var unique = false
    var predicateSql = ""
    if values.len >= 6:
      let kindText = bytesToString(values[5].bytes).toLowerAscii()
      if kindText == "trigram":
        kind = ikTrigram
    if values.len >= 7:
      unique = values[6].int64Val != 0
    if values.len >= 8:
      predicateSql = bytesToString(values[7].bytes)
    return ok(CatalogRecord(
      kind: crIndex,
      index: IndexMeta(
        name: name,
        table: tableName,
        columns: columns,
        rootPage: rootPage,
        kind: kind,
        unique: unique,
        predicateSql: predicateSql
      )
    ))
  if recordType == "view":
    if values.len < 5:
      return err[CatalogRecord](ERR_CORRUPTION, "View catalog record too short")
    let name = bytesToString(values[1].bytes)
    let sqlText = bytesToString(values[2].bytes)
    let columnStr = bytesToString(values[3].bytes)
    let depStr = bytesToString(values[4].bytes)
    let columnNames = if columnStr.len > 0: columnStr.split(";") else: @[]
    let dependencies = if depStr.len > 0: depStr.split(";") else: @[]
    return ok(CatalogRecord(kind: crView, view: ViewMeta(name: name, sqlText: sqlText, columnNames: columnNames, dependencies: dependencies)))
  if recordType == "trigger":
    if values.len < 5:
      return err[CatalogRecord](ERR_CORRUPTION, "Trigger catalog record too short")
    let name = bytesToString(values[1].bytes)
    let tableName = bytesToString(values[2].bytes)
    let eventsMask = int(values[3].int64Val)
    let actionSql = bytesToString(values[4].bytes)
    return ok(CatalogRecord(kind: crTrigger, trigger: TriggerMeta(name: name, table: tableName, eventsMask: eventsMask, actionSql: actionSql)))
  if recordType == "stats:table":
    if values.len < 3:
      return err[CatalogRecord](ERR_CORRUPTION, "Table stats record too short")
    let name = bytesToString(values[1].bytes)
    let rowCount = values[2].int64Val
    return ok(CatalogRecord(kind: crTableStats, tableStats: TableStats(rowCount: rowCount), tableStatsName: name))
  if recordType == "stats:index":
    if values.len < 4:
      return err[CatalogRecord](ERR_CORRUPTION, "Index stats record too short")
    let name = bytesToString(values[1].bytes)
    let entryCount = values[2].int64Val
    let distinctKeyCount = values[3].int64Val
    return ok(CatalogRecord(kind: crIndexStats, indexStats: IndexStats(entryCount: entryCount, distinctKeyCount: distinctKeyCount), indexStatsName: name))
  # Unknown record types are silently skipped (forward compatibility).
  err[CatalogRecord](ERR_CORRUPTION, "Unknown catalog record type", recordType)

proc normalizedObjectName(name: string): string =
  name.toLowerAscii()

proc triggerMetaKey(tableName: string, triggerName: string): string =
  normalizedObjectName(tableName) & ":" & normalizedObjectName(triggerName)

proc rebuildReverseFkCache(catalog: Catalog) =
  catalog.reverseFkRefs = initTable[(string, string), seq[ReferencingChildFk]]()
  catalog.tablesWithReferencingChildren = initHashSet[string]()
  for _, meta in catalog.tables:
    for col in meta.columns:
      if col.refTable.len == 0 or col.refColumn.len == 0:
        continue
      let key = (normalizedObjectName(col.refTable), normalizedObjectName(col.refColumn))
      if not catalog.reverseFkRefs.hasKey(key):
        catalog.reverseFkRefs[key] = @[]
      catalog.reverseFkRefs[key].add(ReferencingChildFk(
        tableName: meta.name,
        columnName: col.name,
        onDelete: normalizeFkAction(col.refOnDelete),
        onUpdate: normalizeFkAction(col.refOnUpdate)
      ))
      catalog.tablesWithReferencingChildren.incl(normalizedObjectName(col.refTable))
  for key, refs in mpairs(catalog.reverseFkRefs):
    refs.sort(proc(a, b: ReferencingChildFk): int =
      let tableCmp = cmp(a.tableName, b.tableName)
      if tableCmp != 0: tableCmp else: cmp(a.columnName, b.columnName)
    )

proc rebuildTriggerCache(catalog: Catalog) =
  catalog.triggerEventMaskByTable = initTable[string, int]()
  for _, trigger in catalog.triggers:
    let key = normalizedObjectName(trigger.table)
    let existing = if catalog.triggerEventMaskByTable.hasKey(key): catalog.triggerEventMaskByTable[key] else: 0
    catalog.triggerEventMaskByTable[key] = existing or trigger.eventsMask

proc rebuildDependentViewsIndex*(catalog: Catalog) =
  catalog.dependentViews = initTable[string, HashSet[string]]()
  for _, view in catalog.views:
    let dependentName = normalizedObjectName(view.name)
    for dep in view.dependencies:
      let key = normalizedObjectName(dep)
      if not catalog.dependentViews.hasKey(key):
        catalog.dependentViews[key] = initHashSet[string]()
      catalog.dependentViews[key].incl(dependentName)

proc initCatalog*(pager: Pager): Result[Catalog] =
  if pager.header.rootCatalog == 0:
    let rootRes = allocatePage(pager)
    if not rootRes.ok:
      return err[Catalog](rootRes.err.code, rootRes.err.message, rootRes.err.context)
    let rootPage = rootRes.value
    var buf = newString(pager.pageSize)
    buf[0] = char(PageTypeLeaf)
    writeU32LE(buf, 4, 0)
    let writeRes = writePage(pager, rootPage, buf)
    if not writeRes.ok:
      return err[Catalog](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    pager.header.rootCatalog = uint32(rootPage)
    discard writeHeader(pager.vfs, pager.file, pager.header)
  let tree = newBTree(pager, PageId(pager.header.rootCatalog))
  let catalog = Catalog(
    tables: initTable[string, TableMeta](),
    indexes: initTable[string, IndexMeta](),
    views: initTable[string, ViewMeta](),
    triggers: initTable[string, TriggerMeta](),
    dependentViews: initTable[string, HashSet[string]](),
    reverseFkRefs: initTable[(string, string), seq[ReferencingChildFk]](),
    tablesWithReferencingChildren: initHashSet[string](),
    triggerEventMaskByTable: initTable[string, int](),
    catalogTree: tree,
    trigramDeltas: initTable[(string, uint32), TrigramDelta](),
    tableStats: initTable[string, TableStats](),
    indexStats: initTable[string, IndexStats](),
    rowCountDeltas: initTable[string, int64]()
  )
  let cursorRes = openCursor(tree)
  if cursorRes.ok:
    let cursor = cursorRes.value
    while true:
      let nextRes = cursorNext(cursor)
      if not nextRes.ok:
        break
      let data = nextRes.value[1]
      let recordRes = parseCatalogRecord(data)
      if recordRes.ok:
        let record = recordRes.value
        case record.kind
        of crTable:
          var table = record.table
          let tableTree = newBTree(pager, table.rootPage)
          let maxKeyRes = findMaxKey(tableTree)
          if maxKeyRes.ok:
            if table.nextRowId <= maxKeyRes.value:
               table.nextRowId = maxKeyRes.value + 1
          catalog.tables[normalizedObjectName(record.table.name)] = table
        of crIndex:
          catalog.indexes[normalizedObjectName(record.index.name)] = record.index
        of crView:
          catalog.views[normalizedObjectName(record.view.name)] = record.view
        of crTrigger:
          catalog.triggers[triggerMetaKey(record.trigger.table, record.trigger.name)] = record.trigger
        of crTableStats:
          catalog.tableStats[record.tableStatsName] = record.tableStats
        of crIndexStats:
          catalog.indexStats[record.indexStatsName] = record.indexStats
  rebuildDependentViewsIndex(catalog)
  rebuildReverseFkCache(catalog)
  rebuildTriggerCache(catalog)
  ok(catalog)

proc trigramBufferAdd*(catalog: Catalog, indexName: string, trigram: uint32, rowid: uint64) =
  let key = (indexName, trigram)
  if not catalog.trigramDeltas.hasKey(key):
    catalog.trigramDeltas[key] = TrigramDelta(adds: initHashSet[uint64](), removes: initHashSet[uint64]())
  let delta = catalog.trigramDeltas[key]
  delta.removes.excl(rowid)
  delta.adds.incl(rowid)

proc trigramBufferRemove*(catalog: Catalog, indexName: string, trigram: uint32, rowid: uint64) =
  let key = (indexName, trigram)
  if not catalog.trigramDeltas.hasKey(key):
    catalog.trigramDeltas[key] = TrigramDelta(adds: initHashSet[uint64](), removes: initHashSet[uint64]())
  let delta = catalog.trigramDeltas[key]
  delta.adds.excl(rowid)
  delta.removes.incl(rowid)

proc trigramDelta*(catalog: Catalog, indexName: string, trigram: uint32): Option[TrigramDelta] =
  let key = (indexName, trigram)
  if catalog.trigramDeltas.hasKey(key):
    return some(catalog.trigramDeltas[key])
  none(TrigramDelta)

proc clearTrigramDeltas*(catalog: Catalog) =
  catalog.trigramDeltas.clear()

proc allTrigramDeltas*(catalog: Catalog): seq[((string, uint32), TrigramDelta)] =
  for k, v in catalog.trigramDeltas.pairs:
    result.add((k, v))

proc updateTableMeta*(catalog: Catalog, table: TableMeta) =
  ## Updates the in-memory metadata for a table without persisting to disk.
  ## Use with caution: changes will be lost on crash if not followed by saveTable eventually.
  catalog.tables[normalizedObjectName(table.name)] = table

proc updateTableMetaFast*(catalog: Catalog, tableName: string, nextRowId: uint64, rootPage: PageId) {.inline.} =
  ## Updates only nextRowId and rootPage in the in-memory table metadata.
  ## Avoids copying the entire TableMeta struct when only these fields change.
  catalog.tables.withValue(normalizedObjectName(tableName), entry):
    entry.nextRowId = nextRowId
    entry.rootPage = rootPage

proc saveTable*(catalog: Catalog, pager: Pager, table: TableMeta): Result[Void] =
  var storedTable = table
  storedTable.temporary = false
  var rebuildFk = true
  if catalog.tables.hasKey(normalizedObjectName(table.name)):
    rebuildFk = catalog.tables[normalizedObjectName(table.name)].columns != storedTable.columns
  catalog.tables[normalizedObjectName(storedTable.name)] = storedTable
  if rebuildFk:
    rebuildReverseFkCache(catalog)
  let key = uint64(crc32c(stringToBytes("table:" & storedTable.name)))
  let record = makeTableRecord(storedTable.name, storedTable.rootPage, storedTable.nextRowId, storedTable.columns, storedTable.checks)
  
  let updateRes = update(catalog.catalogTree, key, record)
  if updateRes.ok:
    return okVoid()
  
  if updateRes.err.message != "Key not found":
    return err[Void](updateRes.err.code, updateRes.err.message, updateRes.err.context)

  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  
  # Ensure catalog root page update is propagated to the pager header
  if catalog.catalogTree.root != pager.header.rootCatalog:
    pager.header.rootCatalog = catalog.catalogTree.root
  
  okVoid()

proc registerTempTable*(catalog: Catalog, table: TableMeta) =
  ## Add a table to the in-memory catalog only (not persisted).
  var tempTable = table
  tempTable.temporary = true
  catalog.tables[normalizedObjectName(tempTable.name)] = tempTable
  rebuildReverseFkCache(catalog)

proc getTable*(catalog: Catalog, name: string): Result[TableMeta] =
  if not catalog.tables.hasKey(normalizedObjectName(name)):
    return err[TableMeta](ERR_SQL, "Table not found", name)
  ok(catalog.tables[normalizedObjectName(name)])

proc getTablePtr*(catalog: Catalog, name: string): ptr TableMeta =
  ## Returns a mutable pointer into the catalog table map. Caller must not
  ## hold this across operations that could rehash catalog.tables.
  catalog.tables.withValue(normalizedObjectName(name), v):
    return addr v[]
  return nil

proc createIndexMeta*(catalog: Catalog, index: IndexMeta): Result[Void] =
  var storedIndex = index
  storedIndex.temporary = false
  catalog.indexes[normalizedObjectName(storedIndex.name)] = storedIndex
  let key = uint64(crc32c(stringToBytes("index:" & storedIndex.name)))
  let record = makeIndexRecord(storedIndex.name, storedIndex.table, storedIndex.columns, storedIndex.rootPage, storedIndex.kind, storedIndex.unique, storedIndex.predicateSql)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    catalog.indexes.del(normalizedObjectName(storedIndex.name))
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root

  okVoid()

proc saveIndexMeta*(catalog: Catalog, index: IndexMeta): Result[Void] =
  var storedIndex = index
  storedIndex.temporary = false
  catalog.indexes[normalizedObjectName(storedIndex.name)] = storedIndex
  let key = uint64(crc32c(stringToBytes("index:" & storedIndex.name)))
  discard delete(catalog.catalogTree, key)
  let record = makeIndexRecord(storedIndex.name, storedIndex.table, storedIndex.columns, storedIndex.rootPage, storedIndex.kind, storedIndex.unique, storedIndex.predicateSql)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root

  okVoid()

proc registerTempIndex*(catalog: Catalog, index: IndexMeta) =
  ## Add an index to the in-memory catalog only (not persisted).
  var tempIndex = index
  tempIndex.temporary = true
  catalog.indexes[normalizedObjectName(tempIndex.name)] = tempIndex

proc createViewMeta*(catalog: Catalog, view: ViewMeta): Result[Void] =
  var storedView = view
  storedView.temporary = false
  let normName = normalizedObjectName(storedView.name)
  if catalog.views.hasKey(normName):
    return err[Void](ERR_SQL, "View already exists", storedView.name)
  catalog.views[normName] = storedView
  rebuildDependentViewsIndex(catalog)
  let key = uint64(crc32c(stringToBytes("view:" & storedView.name)))
  let record = makeViewRecord(storedView.name, storedView.sqlText, storedView.columnNames, storedView.dependencies)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    catalog.views.del(normName)
    rebuildDependentViewsIndex(catalog)
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc registerTempView*(catalog: Catalog, view: ViewMeta) =
  ## Add a view to the in-memory catalog only (not persisted).
  var tempView = view
  tempView.temporary = true
  catalog.views[normalizedObjectName(tempView.name)] = tempView
  rebuildDependentViewsIndex(catalog)

proc saveViewMeta*(catalog: Catalog, view: ViewMeta): Result[Void] =
  var storedView = view
  storedView.temporary = false
  catalog.views[normalizedObjectName(storedView.name)] = storedView
  rebuildDependentViewsIndex(catalog)
  let key = uint64(crc32c(stringToBytes("view:" & storedView.name)))
  let record = makeViewRecord(storedView.name, storedView.sqlText, storedView.columnNames, storedView.dependencies)
  let updateRes = update(catalog.catalogTree, key, record)
  if updateRes.ok:
    if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
      catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
    return okVoid()
  if updateRes.err.message != "Key not found":
    return err[Void](updateRes.err.code, updateRes.err.message, updateRes.err.context)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc getView*(catalog: Catalog, name: string): Result[ViewMeta] =
  let normName = normalizedObjectName(name)
  if not catalog.views.hasKey(normName):
    return err[ViewMeta](ERR_SQL, "View not found", name)
  ok(catalog.views[normName])

proc createTriggerMeta*(catalog: Catalog, trigger: TriggerMeta): Result[Void]
proc registerTempTrigger*(catalog: Catalog, trigger: TriggerMeta)
proc dropTrigger*(catalog: Catalog, tableName: string, triggerName: string): Result[Void]

proc dropView*(catalog: Catalog, name: string): Result[Void] =
  let normName = normalizedObjectName(name)
  if not catalog.views.hasKey(normName):
    return err[Void](ERR_SQL, "View not found", name)
  let view = catalog.views[normName]
  let originalName = view.name
  let isTemporary = view.temporary
  var triggerNames: seq[string] = @[]
  for _, trigger in catalog.triggers:
    if normalizedObjectName(trigger.table) == normName:
      triggerNames.add(trigger.name)
  for triggerName in triggerNames:
    let dropTrigRes = dropTrigger(catalog, name, triggerName)
    if not dropTrigRes.ok:
      return dropTrigRes
  catalog.views.del(normName)
  rebuildDependentViewsIndex(catalog)
  if isTemporary:
    return okVoid()
  let key = uint64(crc32c(stringToBytes("view:" & originalName)))
  let delRes = delete(catalog.catalogTree, key)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc renameView*(catalog: Catalog, oldName: string, newName: string): Result[Void] =
  let normOldName = normalizedObjectName(oldName)
  let normNewName = normalizedObjectName(newName)
  if not catalog.views.hasKey(normOldName):
    return err[Void](ERR_SQL, "View not found", oldName)
  if catalog.views.hasKey(normNewName):
    return err[Void](ERR_SQL, "View already exists", newName)
  var view = catalog.views[normOldName]
  if not view.temporary:
    let oldKey = uint64(crc32c(stringToBytes("view:" & view.name)))
    let delRes = delete(catalog.catalogTree, oldKey)
    if not delRes.ok:
      return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  catalog.views.del(normOldName)
  view.name = newName
  catalog.views[normNewName] = view
  var triggerMetas: seq[TriggerMeta] = @[]
  for _, trigger in catalog.triggers:
    if normalizedObjectName(trigger.table) == normalizedObjectName(oldName):
      triggerMetas.add(trigger)
  for trigger in triggerMetas:
    let dropTrigRes = dropTrigger(catalog, oldName, trigger.name)
    if not dropTrigRes.ok:
      return dropTrigRes
  for trigger in triggerMetas:
    var renamed = trigger
    renamed.table = newName
    if renamed.temporary:
      registerTempTrigger(catalog, renamed)
    else:
      let createTrigRes = createTriggerMeta(catalog, renamed)
      if not createTrigRes.ok:
        return createTrigRes
  rebuildDependentViewsIndex(catalog)
  if view.temporary:
    return okVoid()
  let newKey = uint64(crc32c(stringToBytes("view:" & newName)))
  let record = makeViewRecord(view.name, view.sqlText, view.columnNames, view.dependencies)
  let insertRes = insert(catalog.catalogTree, newKey, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc createTriggerMeta*(catalog: Catalog, trigger: TriggerMeta): Result[Void] =
  var storedTrigger = trigger
  storedTrigger.temporary = false
  let keyName = triggerMetaKey(storedTrigger.table, storedTrigger.name)
  if catalog.triggers.hasKey(keyName):
    return err[Void](ERR_SQL, "Trigger already exists", storedTrigger.table & "." & storedTrigger.name)
  catalog.triggers[keyName] = storedTrigger
  let key = uint64(crc32c(stringToBytes("trigger:" & storedTrigger.table & ":" & storedTrigger.name)))
  let record = makeTriggerRecord(storedTrigger.name, storedTrigger.table, storedTrigger.eventsMask, storedTrigger.actionSql)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    catalog.triggers.del(keyName)
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  rebuildTriggerCache(catalog)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc registerTempTrigger*(catalog: Catalog, trigger: TriggerMeta) =
  ## Add a trigger to the in-memory catalog only (not persisted).
  var tempTrigger = trigger
  tempTrigger.temporary = true
  catalog.triggers[triggerMetaKey(tempTrigger.table, tempTrigger.name)] = tempTrigger
  rebuildTriggerCache(catalog)

proc dropTrigger*(catalog: Catalog, tableName: string, triggerName: string): Result[Void] =
  let keyName = triggerMetaKey(tableName, triggerName)
  if not catalog.triggers.hasKey(keyName):
    return err[Void](ERR_SQL, "Trigger not found", tableName & "." & triggerName)
  let trigger = catalog.triggers[keyName]
  catalog.triggers.del(keyName)
  if trigger.temporary:
    rebuildTriggerCache(catalog)
    return okVoid()
  let key = uint64(crc32c(stringToBytes("trigger:" & tableName & ":" & triggerName)))
  let delRes = delete(catalog.catalogTree, key)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  rebuildTriggerCache(catalog)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc referencingChildren*(catalog: Catalog, tableName: string, columnName: string): seq[ReferencingChildFk] =
  let key = (normalizedObjectName(tableName), normalizedObjectName(columnName))
  if catalog.reverseFkRefs.hasKey(key):
    return catalog.reverseFkRefs[key]
  @[]

proc hasReferencingChildren*(catalog: Catalog, tableName: string): bool =
  normalizedObjectName(tableName) in catalog.tablesWithReferencingChildren

proc hasTriggersForTable*(catalog: Catalog, tableName: string, eventMask: int = 0): bool =
  let key = normalizedObjectName(tableName)
  if not catalog.triggerEventMaskByTable.hasKey(key):
    return false
  if eventMask == 0:
    return true
  (catalog.triggerEventMaskByTable[key] and eventMask) != 0

proc hasTrigger*(catalog: Catalog, tableName: string, triggerName: string): bool =
  catalog.triggers.hasKey(triggerMetaKey(tableName, triggerName))

proc getTrigger*(catalog: Catalog, tableName: string, triggerName: string): Result[TriggerMeta] =
  let keyName = triggerMetaKey(tableName, triggerName)
  if not catalog.triggers.hasKey(keyName):
    return err[TriggerMeta](ERR_SQL, "Trigger not found", tableName & "." & triggerName)
  ok(catalog.triggers[keyName])

proc listTriggersForTable*(catalog: Catalog, tableName: string, eventMask: int = 0): seq[TriggerMeta] =
  if not catalog.hasTriggersForTable(tableName, eventMask):
    return @[]
  let tableKey = normalizedObjectName(tableName)
  for _, trigger in catalog.triggers:
    if normalizedObjectName(trigger.table) != tableKey:
      continue
    if eventMask != 0 and (trigger.eventsMask and eventMask) == 0:
      continue
    result.add(trigger)
  result.sort(proc(a, b: TriggerMeta): int = cmp(a.name, b.name))

proc listTriggers*(catalog: Catalog): seq[TriggerMeta] =
  for _, trigger in catalog.triggers:
    result.add(trigger)
  result.sort(proc(a, b: TriggerMeta): int =
    let tableCmp = cmp(a.table, b.table)
    if tableCmp != 0: tableCmp else: cmp(a.name, b.name)
  )

proc listDependentViews*(catalog: Catalog, objectName: string): seq[string] =
  let key = normalizedObjectName(objectName)
  if not catalog.dependentViews.hasKey(key):
    return @[]
  for name in catalog.dependentViews[key]:
    result.add(name)
  result.sort()

proc dropTable*(catalog: Catalog, name: string): Result[Void] =
  let normName = normalizedObjectName(name)
  if not catalog.tables.hasKey(normName):
    return err[Void](ERR_SQL, "Table not found", name)
  let table = catalog.tables[normName]
  let originalName = table.name
  let isTemporary = table.temporary
  var triggerNames: seq[string] = @[]
  for _, trigger in catalog.triggers:
    if normalizedObjectName(trigger.table) == normName:
      triggerNames.add(trigger.name)
  for triggerName in triggerNames:
    let dropTrigRes = dropTrigger(catalog, name, triggerName)
    if not dropTrigRes.ok:
      return dropTrigRes
  catalog.tables.del(normName)
  rebuildReverseFkCache(catalog)
  if isTemporary:
    return okVoid()
  let key = uint64(crc32c(stringToBytes("table:" & originalName)))
  let delRes = delete(catalog.catalogTree, key)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root

  okVoid()

proc dropIndex*(catalog: Catalog, name: string): Result[Void] =
  let normName = normalizedObjectName(name)
  if not catalog.indexes.hasKey(normName):
    return err[Void](ERR_SQL, "Index not found", name)
  let index = catalog.indexes[normName]
  let originalName = index.name
  catalog.indexes.del(normName)
  if index.temporary:
    return okVoid()
  let key = uint64(crc32c(stringToBytes("index:" & originalName)))
  let delRes = delete(catalog.catalogTree, key)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root

  okVoid()

proc getBtreeIndexForColumn*(catalog: Catalog, table: string, column: string): Option[IndexMeta] =
  let normTable = normalizedObjectName(table)
  for _, idx in catalog.indexes:
    if normalizedObjectName(idx.table) == normTable and idx.columns.len == 1 and normalizedObjectName(idx.columns[0]) == normalizedObjectName(column) and idx.kind == ikBtree and idx.predicateSql.len == 0:
      return some(idx)
  none(IndexMeta)

proc getIndexForColumn*(catalog: Catalog, table: string, column: string, kind: IndexKind, requireUnique: bool = false): Option[IndexMeta] =
  ## Returns any single-column index that semantically satisfies the requested signature.
  ## If requireUnique is true, only unique indexes satisfy.
  ## Partial indexes (with predicateSql) are excluded — they may not cover all rows.
  let normTable = normalizedObjectName(table)
  let normColumn = normalizedObjectName(column)
  for _, idx in catalog.indexes:
    if normalizedObjectName(idx.table) != normTable or idx.columns.len != 1 or normalizedObjectName(idx.columns[0]) != normColumn or idx.kind != kind:
      continue
    if requireUnique and not idx.unique:
      continue
    if idx.predicateSql.len > 0:
      continue
    return some(idx)
  none(IndexMeta)

proc getTrigramIndexForColumn*(catalog: Catalog, table: string, column: string): Option[IndexMeta] =
  let normTable = normalizedObjectName(table)
  for _, idx in catalog.indexes:
    if normalizedObjectName(idx.table) == normTable and idx.columns.len == 1 and normalizedObjectName(idx.columns[0]) == normalizedObjectName(column) and idx.kind == ikTrigram:
      return some(idx)
  none(IndexMeta)

proc getIndexByName*(catalog: Catalog, name: string): Option[IndexMeta] =
  let normName = normalizedObjectName(name)
  if catalog.indexes.hasKey(normName):
    return some(catalog.indexes[normName])
  none(IndexMeta)

proc hasTableName*(catalog: Catalog, name: string): bool =
  catalog.tables.hasKey(normalizedObjectName(name))

proc hasViewName*(catalog: Catalog, name: string): bool =
  catalog.views.hasKey(normalizedObjectName(name))

proc hasTableOrViewName*(catalog: Catalog, name: string): bool =
  catalog.tables.hasKey(normalizedObjectName(name)) or catalog.views.hasKey(normalizedObjectName(name))

# ---------------------------------------------------------------------------
# Statistics: table and index stats persistence
# ---------------------------------------------------------------------------

proc getTableStats*(catalog: Catalog, tableName: string): Option[TableStats] =
  let normName = normalizedObjectName(tableName)
  if catalog.tableStats.hasKey(normName):
    return some(catalog.tableStats[normName])
  none(TableStats)

proc getIndexStats*(catalog: Catalog, indexName: string): Option[IndexStats] =
  let normName = normalizedObjectName(indexName)
  if catalog.indexStats.hasKey(normName):
    return some(catalog.indexStats[normName])
  none(IndexStats)

proc saveTableStats*(catalog: Catalog, tableName: string, stats: TableStats): Result[Void] =
  let normName = normalizedObjectName(tableName)
  catalog.tableStats[normName] = stats
  let key = uint64(crc32c(stringToBytes("stats:table:" & normName)))
  let record = makeTableStatsRecord(normName, stats.rowCount)
  # Try update first; insert if not found.
  let updateRes = update(catalog.catalogTree, key, record)
  if updateRes.ok:
    if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
      catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
    return okVoid()
  if updateRes.err.message != "Key not found":
    return err[Void](updateRes.err.code, updateRes.err.message, updateRes.err.context)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc saveIndexStats*(catalog: Catalog, indexName: string, stats: IndexStats): Result[Void] =
  let normName = normalizedObjectName(indexName)
  catalog.indexStats[normName] = stats
  let key = uint64(crc32c(stringToBytes("stats:index:" & normName)))
  let record = makeIndexStatsRecord(normName, stats.entryCount, stats.distinctKeyCount)
  let updateRes = update(catalog.catalogTree, key, record)
  if updateRes.ok:
    if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
      catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
    return okVoid()
  if updateRes.err.message != "Key not found":
    return err[Void](updateRes.err.code, updateRes.err.message, updateRes.err.context)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc applyRowCountDeltas*(catalog: Catalog): Result[Void] =
  ## Apply pending incremental row-count deltas to persisted table stats.
  ## Called at commit time; clears deltas afterward.
  for normName, delta in catalog.rowCountDeltas:
    if delta == 0:
      continue
    if catalog.tableStats.hasKey(normName):
      var stats = catalog.tableStats[normName]
      stats.rowCount = max(0, stats.rowCount + delta)
      let saveRes = saveTableStats(catalog, normName, stats)
      if not saveRes.ok:
        return saveRes
  catalog.rowCountDeltas.clear()
  okVoid()

proc discardRowCountDeltas*(catalog: Catalog) =
  ## Discard pending incremental row-count deltas on rollback.
  catalog.rowCountDeltas.clear()

proc addRowCountDelta*(catalog: Catalog, tableName: string, delta: int64) =
  ## Record a row-count change for commit-time application.
  let normName = normalizedObjectName(tableName)
  let current = if catalog.rowCountDeltas.hasKey(normName): catalog.rowCountDeltas[normName] else: 0'i64
  catalog.rowCountDeltas[normName] = current + delta
