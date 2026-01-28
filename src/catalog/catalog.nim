import options
import tables
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

type Column* = object
  name*: string
  kind*: ColumnType

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

type Catalog* = ref object
  tables*: Table[string, TableMeta]
  indexes*: Table[string, IndexMeta]
  catalogTree*: BTree

type CatalogRecordKind = enum
  crTable
  crIndex

type CatalogRecord = object
  kind: CatalogRecordKind
  table: TableMeta
  index: IndexMeta

proc parseColumnType*(text: string): Result[ColumnType] =
  case text.toUpperAscii()
  of "INT", "INT64", "BIGINT":
    ok(ctInt64)
  of "BOOL", "BOOLEAN":
    ok(ctBool)
  of "FLOAT", "FLOAT64", "DOUBLE":
    ok(ctFloat64)
  of "TEXT":
    ok(ctText)
  of "BLOB":
    ok(ctBlob)
  else:
    err[ColumnType](ERR_SQL, "Unsupported column type", text)

proc columnTypeToText(kind: ColumnType): string =
  case kind
  of ctInt64: "INT64"
  of ctBool: "BOOL"
  of ctFloat64: "FLOAT64"
  of ctText: "TEXT"
  of ctBlob: "BLOB"

proc encodeColumns(columns: seq[Column]): seq[byte] =
  var parts: seq[string] = @[]
  for col in columns:
    parts.add(col.name & ":" & columnTypeToText(col.kind))
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
    if pieces.len == 2:
      let typeRes = parseColumnType(pieces[1])
      if typeRes.ok:
        result.add(Column(name: pieces[0], kind: typeRes.value))

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

proc makeIndexRecord(name: string, table: string, column: string, rootPage: PageId): seq[byte] =
  let values = @[
    Value(kind: vkText, bytes: stringToBytes("index")),
    Value(kind: vkText, bytes: stringToBytes(name)),
    Value(kind: vkText, bytes: stringToBytes(table)),
    Value(kind: vkText, bytes: stringToBytes(column)),
    Value(kind: vkInt64, int64Val: int64(rootPage))
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
    return ok(CatalogRecord(kind: crIndex, index: IndexMeta(name: name, table: tableName, column: columnName, rootPage: rootPage)))
  err[CatalogRecord](ERR_CORRUPTION, "Unknown catalog record type", recordType)

proc initCatalog*(pager: Pager): Result[Catalog] =
  if pager.header.rootCatalog == 0:
    let rootRes = allocatePage(pager)
    if not rootRes.ok:
      return err[Catalog](rootRes.err.code, rootRes.err.message, rootRes.err.context)
    let rootPage = rootRes.value
    var buf = newSeq[byte](pager.pageSize)
    buf[0] = PageTypeLeaf
    writeU32LE(buf, 4, 0)
    let writeRes = writePage(pager, rootPage, buf)
    if not writeRes.ok:
      return err[Catalog](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    pager.header.rootCatalog = uint32(rootPage)
    discard writeHeader(pager.vfs, pager.file, pager.header)
  let tree = newBTree(pager, PageId(pager.header.rootCatalog))
  let catalog = Catalog(tables: initTable[string, TableMeta](), indexes: initTable[string, IndexMeta](), catalogTree: tree)
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

proc saveTable*(catalog: Catalog, pager: Pager, table: TableMeta): Result[Void] =
  catalog.tables[table.name] = table
  let key = uint64(crc32c(stringToBytes("table:" & table.name)))
  let record = makeTableRecord(table.name, table.rootPage, table.nextRowId, table.columns)
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
  let record = makeIndexRecord(index.name, index.table, index.column, index.rootPage)
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

proc getIndexForColumn*(catalog: Catalog, table: string, column: string): Option[IndexMeta] =
  for _, idx in catalog.indexes:
    if idx.table == table and idx.column == column:
      return some(idx)
  none(IndexMeta)
