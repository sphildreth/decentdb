import options
import tables
import sets
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

type TableMeta* = object
  name*: string
  rootPage*: PageId
  nextRowId*: uint64
  columns*: seq[Column]

type IndexMeta* = object
  name*: string
  table*: string
  column*: string
  rootPage*: PageId
  kind*: IndexKind
  unique*: bool

type TrigramDelta* = ref object
  adds*: HashSet[uint64]
  removes*: HashSet[uint64]

type Catalog* = ref object
  tables*: Table[string, TableMeta]
  indexes*: Table[string, IndexMeta]
  catalogTree*: BTree
  trigramDeltas*: Table[(string, uint32), TrigramDelta]

type CatalogRecordKind = enum
  crTable
  crIndex

type CatalogRecord = object
  kind: CatalogRecordKind
  table: TableMeta
  index: IndexMeta

proc parseColumnType*(text: string): Result[ColumnType] =
  case text.toUpperAscii()
  of "INT", "INT64", "BIGINT", "INT4", "INT8":
    ok(ctInt64)
  of "BOOL", "BOOLEAN":
    ok(ctBool)
  of "FLOAT", "FLOAT64", "DOUBLE", "FLOAT8", "FLOAT4", "REAL":
    ok(ctFloat64)
  of "TEXT":
    ok(ctText)
  of "BLOB":
    ok(ctBlob)
  else:
    err[ColumnType](ERR_SQL, "Unsupported column type", text)

proc columnTypeToText*(kind: ColumnType): string =
  case kind
  of ctInt64: "INT64"
  of ctBool: "BOOL"
  of ctFloat64: "FLOAT64"
  of ctText: "TEXT"
  of ctBlob: "BLOB"

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
    parts.add(col.name & ":" & columnTypeToText(col.kind) & flagPart)
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
        var col = Column(name: pieces[0], kind: typeRes.value)
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

proc makeIndexRecord(name: string, table: string, column: string, rootPage: PageId, kind: IndexKind, unique: bool): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("index")),
    Value(kind: vkText, bytes: stringToBytes(name)),
    Value(kind: vkText, bytes: stringToBytes(table)),
    Value(kind: vkText, bytes: stringToBytes(column)),
    Value(kind: vkInt64, int64Val: int64(rootPage)),
    Value(kind: vkText, bytes: stringToBytes(if kind == ikTrigram: "trigram" else: "btree")),
    Value(kind: vkInt64, int64Val: int64(if unique: 1 else: 0))
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
    let columnName = bytesToString(values[3].bytes)
    let rootPage = PageId(values[4].int64Val)
    var kind = ikBtree
    var unique = false
    if values.len >= 6:
      let kindText = bytesToString(values[5].bytes).toLowerAscii()
      if kindText == "trigram":
        kind = ikTrigram
    if values.len >= 7:
      unique = values[6].int64Val != 0
    return ok(CatalogRecord(kind: crIndex, index: IndexMeta(name: name, table: tableName, column: columnName, rootPage: rootPage, kind: kind, unique: unique)))
  err[CatalogRecord](ERR_CORRUPTION, "Unknown catalog record type", recordType)

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
          catalog.tables[record.table.name] = record.table
        of crIndex:
          catalog.indexes[record.index.name] = record.index
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

proc saveTable*(catalog: Catalog, pager: Pager, table: TableMeta): Result[Void] =
  catalog.tables[table.name] = table
  let key = uint64(crc32c(stringToBytes("table:" & table.name)))
  let record = makeTableRecord(table.name, table.rootPage, table.nextRowId, table.columns)
  discard delete(catalog.catalogTree, key)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  okVoid()

proc getTable*(catalog: Catalog, name: string): Result[TableMeta] =
  if not catalog.tables.hasKey(name):
    return err[TableMeta](ERR_SQL, "Table not found", name)
  ok(catalog.tables[name])

proc createIndexMeta*(catalog: Catalog, index: IndexMeta): Result[Void] =
  catalog.indexes[index.name] = index
  let key = uint64(crc32c(stringToBytes("index:" & index.name)))
  let record = makeIndexRecord(index.name, index.table, index.column, index.rootPage, index.kind, index.unique)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  okVoid()

proc saveIndexMeta*(catalog: Catalog, index: IndexMeta): Result[Void] =
  catalog.indexes[index.name] = index
  let key = uint64(crc32c(stringToBytes("index:" & index.name)))
  discard delete(catalog.catalogTree, key)
  let record = makeIndexRecord(index.name, index.table, index.column, index.rootPage, index.kind, index.unique)
  let insertRes = insert(catalog.catalogTree, key, record)
  if not insertRes.ok:
    return err[Void](insertRes.err.code, insertRes.err.message, insertRes.err.context)
  okVoid()

proc dropTable*(catalog: Catalog, name: string): Result[Void] =
  if not catalog.tables.hasKey(name):
    return err[Void](ERR_SQL, "Table not found", name)
  catalog.tables.del(name)
  let key = uint64(crc32c(stringToBytes("table:" & name)))
  let delRes = delete(catalog.catalogTree, key)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  okVoid()

proc dropIndex*(catalog: Catalog, name: string): Result[Void] =
  if not catalog.indexes.hasKey(name):
    return err[Void](ERR_SQL, "Index not found", name)
  catalog.indexes.del(name)
  let key = uint64(crc32c(stringToBytes("index:" & name)))
  let delRes = delete(catalog.catalogTree, key)
  if not delRes.ok:
    return err[Void](delRes.err.code, delRes.err.message, delRes.err.context)
  okVoid()

proc getBtreeIndexForColumn*(catalog: Catalog, table: string, column: string): Option[IndexMeta] =
  for _, idx in catalog.indexes:
    if idx.table == table and idx.column == column and idx.kind == ikBtree:
      return some(idx)
  none(IndexMeta)

proc getTrigramIndexForColumn*(catalog: Catalog, table: string, column: string): Option[IndexMeta] =
  for _, idx in catalog.indexes:
    if idx.table == table and idx.column == column and idx.kind == ikTrigram:
      return some(idx)
  none(IndexMeta)

proc getIndexByName*(catalog: Catalog, name: string): Option[IndexMeta] =
  if catalog.indexes.hasKey(name):
    return some(catalog.indexes[name])
  none(IndexMeta)
