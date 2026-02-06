import options
import tables
import sets
import algorithm
import strutils
import ../errors
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
  decPrecision*: uint8
  decScale*: uint8

type TableMeta* = object
  name*: string
  rootPage*: PageId
  nextRowId*: uint64
  columns*: seq[Column]

type ViewMeta* = object
  name*: string
  sqlText*: string
  columnNames*: seq[string]
  dependencies*: seq[string]

type IndexMeta* = object
  name*: string
  table*: string
  columns*: seq[string]
  rootPage*: PageId
  kind*: IndexKind
  unique*: bool

type TrigramDelta* = ref object
  adds*: HashSet[uint64]
  removes*: HashSet[uint64]

type Catalog* = ref object
  tables*: Table[string, TableMeta]
  indexes*: Table[string, IndexMeta]
  views*: Table[string, ViewMeta]
  dependentViews*: Table[string, HashSet[string]]
  catalogTree*: BTree
  trigramDeltas*: Table[(string, uint32), TrigramDelta]

type CatalogRecordKind = enum
  crTable
  crIndex
  crView

type CatalogRecord = object
  kind: CatalogRecordKind
  table: TableMeta
  index: IndexMeta
  view: ViewMeta

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
        result.add(col)

proc stringToBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

proc bytesToString(bytes: seq[byte]): string =
  for b in bytes:
    result.add(char(b))

proc makeTableRecord(name: string, rootPage: PageId, nextRowId: uint64, columns: seq[Column]): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("table")),
    Value(kind: vkText, bytes: stringToBytes(name)),
    Value(kind: vkInt64, int64Val: int64(rootPage)),
    Value(kind: vkInt64, int64Val: int64(nextRowId)),
    Value(kind: vkText, bytes: encodeColumns(columns))
  ]
  encodeRecord(values)

proc makeIndexRecord(name: string, table: string, columns: seq[string], rootPage: PageId, kind: IndexKind, unique: bool): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("index")),
    Value(kind: vkText, bytes: stringToBytes(name)),
    Value(kind: vkText, bytes: stringToBytes(table)),
    Value(kind: vkText, bytes: stringToBytes(columns.join(";"))),
    Value(kind: vkInt64, int64Val: int64(rootPage)),
    Value(kind: vkText, bytes: stringToBytes(if kind == ikTrigram: "trigram" else: "btree")),
    Value(kind: vkInt64, int64Val: int64(if unique: 1 else: 0))
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

proc parseCatalogRecord(data: seq[byte]): Result[CatalogRecord] =
  let decoded = decodeRecord(data)
  if not decoded.ok:
    return err[CatalogRecord](decoded.err.code, decoded.err.message, decoded.err.context)
  let values = decoded.value
  if values.len < 4:
    return err[CatalogRecord](ERR_CORRUPTION, "Catalog record too short")
  if values.len == 4:
    let name = bytesToString(values[0].bytes)
    let rootPage = PageId(values[1].int64Val)
    let nextRowId = uint64(values[2].int64Val)
    let columns = decodeColumns(values[3].bytes)
    return ok(CatalogRecord(kind: crTable, table: TableMeta(name: name, rootPage: rootPage, nextRowId: nextRowId, columns: columns)))
  let recordType = bytesToString(values[0].bytes).toLowerAscii()
  if recordType == "table":
    let name = bytesToString(values[1].bytes)
    let rootPage = PageId(values[2].int64Val)
    let nextRowId = uint64(values[3].int64Val)
    let columns = decodeColumns(values[4].bytes)
    return ok(CatalogRecord(kind: crTable, table: TableMeta(name: name, rootPage: rootPage, nextRowId: nextRowId, columns: columns)))
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
    if values.len >= 6:
      let kindText = bytesToString(values[5].bytes).toLowerAscii()
      if kindText == "trigram":
        kind = ikTrigram
    if values.len >= 7:
      unique = values[6].int64Val != 0
    return ok(CatalogRecord(kind: crIndex, index: IndexMeta(name: name, table: tableName, columns: columns, rootPage: rootPage, kind: kind, unique: unique)))
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
  err[CatalogRecord](ERR_CORRUPTION, "Unknown catalog record type", recordType)

proc normalizedObjectName(name: string): string =
  name.toLowerAscii()

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
    dependentViews: initTable[string, HashSet[string]](),
    catalogTree: tree,
    trigramDeltas: initTable[(string, uint32), TrigramDelta]()
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
          catalog.tables[record.table.name] = table
        of crIndex:
          catalog.indexes[record.index.name] = record.index
        of crView:
          catalog.views[record.view.name] = record.view
  rebuildDependentViewsIndex(catalog)
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
  catalog.tables[table.name] = table

proc saveTable*(catalog: Catalog, pager: Pager, table: TableMeta): Result[Void] =
  catalog.tables[table.name] = table
  let key = uint64(crc32c(stringToBytes("table:" & table.name)))
  let record = makeTableRecord(table.name, table.rootPage, table.nextRowId, table.columns)
  
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

proc getTable*(catalog: Catalog, name: string): Result[TableMeta] =
  if not catalog.tables.hasKey(name):
    return err[TableMeta](ERR_SQL, "Table not found", name)
  ok(catalog.tables[name])

proc createIndexMeta*(catalog: Catalog, index: IndexMeta): Result[Void] =
  catalog.indexes[index.name] = index
  let key = uint64(crc32c(stringToBytes("index:" & index.name)))
  let record = makeIndexRecord(index.name, index.table, index.columns, index.rootPage, index.kind, index.unique)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root

  okVoid()

proc saveIndexMeta*(catalog: Catalog, index: IndexMeta): Result[Void] =
  catalog.indexes[index.name] = index
  let key = uint64(crc32c(stringToBytes("index:" & index.name)))
  discard delete(catalog.catalogTree, key)
  let record = makeIndexRecord(index.name, index.table, index.columns, index.rootPage, index.kind, index.unique)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root

  okVoid()

proc createViewMeta*(catalog: Catalog, view: ViewMeta): Result[Void] =
  if catalog.views.hasKey(view.name):
    return err[Void](ERR_SQL, "View already exists", view.name)
  catalog.views[view.name] = view
  rebuildDependentViewsIndex(catalog)
  let key = uint64(crc32c(stringToBytes("view:" & view.name)))
  let record = makeViewRecord(view.name, view.sqlText, view.columnNames, view.dependencies)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    catalog.views.del(view.name)
    rebuildDependentViewsIndex(catalog)
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc saveViewMeta*(catalog: Catalog, view: ViewMeta): Result[Void] =
  catalog.views[view.name] = view
  rebuildDependentViewsIndex(catalog)
  let key = uint64(crc32c(stringToBytes("view:" & view.name)))
  let record = makeViewRecord(view.name, view.sqlText, view.columnNames, view.dependencies)
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
  if not catalog.views.hasKey(name):
    return err[ViewMeta](ERR_SQL, "View not found", name)
  ok(catalog.views[name])

proc dropView*(catalog: Catalog, name: string): Result[Void] =
  if not catalog.views.hasKey(name):
    return err[Void](ERR_SQL, "View not found", name)
  catalog.views.del(name)
  rebuildDependentViewsIndex(catalog)
  let key = uint64(crc32c(stringToBytes("view:" & name)))
  let delRes = delete(catalog.catalogTree, key)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc renameView*(catalog: Catalog, oldName: string, newName: string): Result[Void] =
  if not catalog.views.hasKey(oldName):
    return err[Void](ERR_SQL, "View not found", oldName)
  if catalog.views.hasKey(newName):
    return err[Void](ERR_SQL, "View already exists", newName)
  var view = catalog.views[oldName]
  let oldKey = uint64(crc32c(stringToBytes("view:" & oldName)))
  let delRes = delete(catalog.catalogTree, oldKey)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  catalog.views.del(oldName)
  view.name = newName
  catalog.views[newName] = view
  rebuildDependentViewsIndex(catalog)
  let newKey = uint64(crc32c(stringToBytes("view:" & newName)))
  let record = makeViewRecord(view.name, view.sqlText, view.columnNames, view.dependencies)
  let insertRes = insert(catalog.catalogTree, newKey, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root
  okVoid()

proc listDependentViews*(catalog: Catalog, objectName: string): seq[string] =
  let key = normalizedObjectName(objectName)
  if not catalog.dependentViews.hasKey(key):
    return @[]
  for name in catalog.dependentViews[key]:
    result.add(name)
  result.sort()

proc dropTable*(catalog: Catalog, name: string): Result[Void] =
  if not catalog.tables.hasKey(name):
    return err[Void](ERR_SQL, "Table not found", name)
  catalog.tables.del(name)
  let key = uint64(crc32c(stringToBytes("table:" & name)))
  let delRes = delete(catalog.catalogTree, key)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root

  okVoid()

proc dropIndex*(catalog: Catalog, name: string): Result[Void] =
  if not catalog.indexes.hasKey(name):
    return err[Void](ERR_SQL, "Index not found", name)
  catalog.indexes.del(name)
  let key = uint64(crc32c(stringToBytes("index:" & name)))
  let delRes = delete(catalog.catalogTree, key)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  
  if catalog.catalogTree.root != catalog.catalogTree.pager.header.rootCatalog:
    catalog.catalogTree.pager.header.rootCatalog = catalog.catalogTree.root

  okVoid()

proc getBtreeIndexForColumn*(catalog: Catalog, table: string, column: string): Option[IndexMeta] =
  for _, idx in catalog.indexes:
    if idx.table == table and idx.columns.len == 1 and idx.columns[0] == column and idx.kind == ikBtree:
      return some(idx)
  none(IndexMeta)

proc getIndexForColumn*(catalog: Catalog, table: string, column: string, kind: IndexKind, requireUnique: bool = false): Option[IndexMeta] =
  ## Returns any single-column index that semantically satisfies the requested signature.
  ## If requireUnique is true, only unique indexes satisfy.
  for _, idx in catalog.indexes:
    if idx.table != table or idx.columns.len != 1 or idx.columns[0] != column or idx.kind != kind:
      continue
    if requireUnique and not idx.unique:
      continue
    return some(idx)
  none(IndexMeta)

proc getTrigramIndexForColumn*(catalog: Catalog, table: string, column: string): Option[IndexMeta] =
  for _, idx in catalog.indexes:
    if idx.table == table and idx.columns.len == 1 and idx.columns[0] == column and idx.kind == ikTrigram:
      return some(idx)
  none(IndexMeta)

proc getIndexByName*(catalog: Catalog, name: string): Option[IndexMeta] =
  if catalog.indexes.hasKey(name):
    return some(catalog.indexes[name])
  none(IndexMeta)

proc hasTableName*(catalog: Catalog, name: string): bool =
  catalog.tables.hasKey(name)

proc hasViewName*(catalog: Catalog, name: string): bool =
  catalog.views.hasKey(name)

proc hasTableOrViewName*(catalog: Catalog, name: string): bool =
  catalog.tables.hasKey(name) or catalog.views.hasKey(name)
