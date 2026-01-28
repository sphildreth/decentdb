import os
import strutils
import tables
import options
import ./errors
import ./vfs/types
import ./vfs/os_vfs
import ./pager/db_header
import ./pager/pager
import ./catalog/catalog
import ./sql/sql
import ./sql/binder
import ./planner/planner
import ./exec/exec
import ./record/record
import ./storage/storage

type Db* = ref object
  path*: string
  vfs*: Vfs
  file*: VfsFile
  isOpen*: bool
  formatVersion*: uint32
  pageSize*: uint32
  schemaCookie*: uint32
  pager*: Pager
  catalog*: Catalog

proc openDb*(path: string): Result[Db] =
  let vfs = newOsVfs()
  let res = vfs.open(path, fmReadWrite, true)
  if not res.ok:
    return err[Db](res.err.code, res.err.message, res.err.context)
  let file = res.value
  let info = getFileInfo(path)
  var probe = newSeq[byte](HeaderSize)
  let probeRes = vfs.read(file, 0, probe)
  if not probeRes.ok:
    discard vfs.close(file)
    return err[Db](probeRes.err.code, probeRes.err.message, probeRes.err.context)
  if probeRes.value == 0:
    if info.size > 0:
      discard vfs.close(file)
      return err[Db](ERR_CORRUPTION, "Header unreadable", "page_id=1")
    let header = DbHeader(
      formatVersion: FormatVersion,
      pageSize: DefaultPageSize,
      schemaCookie: 0,
      rootCatalog: 0,
      rootFreelist: 0,
      freelistHead: 0,
      freelistCount: 0,
      lastCheckpointLsn: 0
    )
    let writeRes = writeHeader(vfs, file, header)
    if not writeRes.ok:
      discard vfs.close(file)
      return err[Db](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    let padSize = int(DefaultPageSize) - HeaderSize
    if padSize > 0:
      var pad = newSeq[byte](padSize)
      let padRes = vfs.write(file, int64(HeaderSize), pad)
      if not padRes.ok:
        discard vfs.close(file)
        return err[Db](padRes.err.code, padRes.err.message, padRes.err.context)
      if padRes.value < padSize:
        discard vfs.close(file)
        return err[Db](ERR_IO, "Short write on header padding", "page_id=1")
      let syncRes = vfs.fsync(file)
      if not syncRes.ok:
        discard vfs.close(file)
        return err[Db](syncRes.err.code, syncRes.err.message, syncRes.err.context)
  elif probeRes.value < HeaderSize:
    discard vfs.close(file)
    return err[Db](ERR_CORRUPTION, "Header too short", "page_id=1")
  else:
    let readRes = decodeHeader(probe)
    if not readRes.ok:
      discard vfs.close(file)
      return err[Db](readRes.err.code, readRes.err.message, readRes.err.context)
    if readRes.value.formatVersion != FormatVersion:
      discard vfs.close(file)
      return err[Db](ERR_CORRUPTION, "Unsupported format version", "page_id=1")
    if readRes.value.pageSize != DefaultPageSize:
      discard vfs.close(file)
      return err[Db](ERR_CORRUPTION, "Unsupported page size", "page_id=1")
  let headerRes = readHeader(vfs, file)
  if not headerRes.ok:
    discard vfs.close(file)
    return err[Db](headerRes.err.code, headerRes.err.message, headerRes.err.context)
  let header = headerRes.value
  let pagerRes = newPager(vfs, file, cachePages = 64)
  if not pagerRes.ok:
    discard vfs.close(file)
    return err[Db](pagerRes.err.code, pagerRes.err.message, pagerRes.err.context)
  let pager = pagerRes.value
  let catalogRes = initCatalog(pager)
  if not catalogRes.ok:
    discard closePager(pager)
    discard vfs.close(file)
    return err[Db](catalogRes.err.code, catalogRes.err.message, catalogRes.err.context)
  ok(Db(
    path: path,
    vfs: vfs,
    file: file,
    isOpen: true,
    formatVersion: header.formatVersion,
    pageSize: header.pageSize,
    schemaCookie: header.schemaCookie,
    pager: pager,
    catalog: catalogRes.value
  ))

proc schemaBump(db: Db): Result[Void] =
  db.schemaCookie.inc
  db.pager.header.schemaCookie = db.schemaCookie
  let writeRes = writeHeader(db.vfs, db.file, db.pager.header)
  if not writeRes.ok:
    return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
  okVoid()

proc valueFromSql(value: Value): Value =
  value

proc typeCheckValue(expected: ColumnType, value: Value): Result[Void] =
  case expected
  of ctInt64:
    if value.kind in {vkInt64, vkNull}: return okVoid()
  of ctBool:
    if value.kind in {vkBool, vkNull}: return okVoid()
  of ctFloat64:
    if value.kind in {vkFloat64, vkInt64, vkNull}: return okVoid()
  of ctText:
    if value.kind in {vkText, vkNull}: return okVoid()
  of ctBlob:
    if value.kind in {vkBlob, vkNull}: return okVoid()
  err[Void](ERR_SQL, "Type mismatch")

proc valuesEqual(a: Value, b: Value): bool =
  if a.kind == vkNull and b.kind == vkNull:
    return true
  if a.kind != b.kind:
    return false
  case a.kind
  of vkInt64:
    a.int64Val == b.int64Val
  of vkBool:
    a.boolVal == b.boolVal
  of vkFloat64:
    a.float64Val == b.float64Val
  of vkText, vkBlob:
    a.bytes == b.bytes
  else:
    false

proc enforceNotNull(table: TableMeta, values: seq[Value]): Result[Void] =
  for i, col in table.columns:
    if col.notNull and values[i].kind == vkNull:
      return err[Void](ERR_CONSTRAINT, "NOT NULL constraint failed", table.name & "." & col.name)
  okVoid()

proc enforceUnique(catalog: Catalog, pager: Pager, table: TableMeta, values: seq[Value], rowid: uint64 = 0): Result[Void] =
  for i, col in table.columns:
    if col.unique or col.primaryKey:
      if values[i].kind == vkNull:
        continue
      let matchesRes = indexSeek(pager, catalog, table.name, col.name, values[i])
      if not matchesRes.ok:
        return err[Void](matchesRes.err.code, matchesRes.err.message, matchesRes.err.context)
      for existing in matchesRes.value:
        if rowid == 0 or existing != rowid:
          return err[Void](ERR_CONSTRAINT, "UNIQUE constraint failed", table.name & "." & col.name)
  okVoid()

proc enforceForeignKeys(catalog: Catalog, pager: Pager, table: TableMeta, values: seq[Value]): Result[Void] =
  for i, col in table.columns:
    if col.refTable.len == 0 or col.refColumn.len == 0:
      continue
    if values[i].kind == vkNull:
      continue
    let matchesRes = indexSeek(pager, catalog, col.refTable, col.refColumn, values[i])
    if not matchesRes.ok:
      return err[Void](matchesRes.err.code, matchesRes.err.message, matchesRes.err.context)
    if matchesRes.value.len == 0:
      return err[Void](ERR_CONSTRAINT, "FOREIGN KEY constraint failed", table.name & "." & col.name)
  okVoid()

proc referencingChildren(catalog: Catalog, table: string, column: string): seq[(string, string)] =
  for _, meta in catalog.tables:
    for col in meta.columns:
      if col.refTable == table and col.refColumn == column:
        result.add((meta.name, col.name))

proc enforceRestrictOnParent(catalog: Catalog, pager: Pager, table: TableMeta, oldValues: seq[Value], newValues: seq[Value]): Result[Void] =
  for i, col in table.columns:
    let children = referencingChildren(catalog, table.name, col.name)
    if children.len == 0:
      continue
    let oldVal = oldValues[i]
    let newVal = newValues[i]
    if valuesEqual(oldVal, newVal):
      continue
    if oldVal.kind == vkNull:
      continue
    for child in children:
      let matchesRes = indexSeek(pager, catalog, child[0], child[1], oldVal)
      if not matchesRes.ok:
        return err[Void](matchesRes.err.code, matchesRes.err.message, matchesRes.err.context)
      if matchesRes.value.len > 0:
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY RESTRICT violation", table.name & "." & col.name)
  okVoid()

proc enforceRestrictOnDelete(catalog: Catalog, pager: Pager, table: TableMeta, oldValues: seq[Value]): Result[Void] =
  for i, col in table.columns:
    let children = referencingChildren(catalog, table.name, col.name)
    if children.len == 0:
      continue
    let oldVal = oldValues[i]
    if oldVal.kind == vkNull:
      continue
    for child in children:
      let matchesRes = indexSeek(pager, catalog, child[0], child[1], oldVal)
      if not matchesRes.ok:
        return err[Void](matchesRes.err.code, matchesRes.err.message, matchesRes.err.context)
      if matchesRes.value.len > 0:
        return err[Void](ERR_CONSTRAINT, "FOREIGN KEY RESTRICT violation", table.name & "." & col.name)
  okVoid()

proc evalInsertValues(stmt: Statement, params: seq[Value]): Result[seq[Value]] =
  var values: seq[Value] = @[]
  for expr in stmt.insertValues:
    let res = evalExpr(Row(), expr, params)
    if not res.ok:
      return err[seq[Value]](res.err.code, res.err.message, res.err.context)
    values.add(valueFromSql(res.value))
  ok(values)

proc execSql*(db: Db, sqlText: string, params: seq[Value]): Result[seq[string]] =
  if not db.isOpen:
    return err[seq[string]](ERR_INTERNAL, "Database not open")
  let parseRes = parseSql(sqlText)
  if not parseRes.ok:
    return err[seq[string]](parseRes.err.code, parseRes.err.message, parseRes.err.context)
  var output: seq[string] = @[]
  for stmt in parseRes.value.statements:
    let bindRes = bindStatement(db.catalog, stmt)
    if not bindRes.ok:
      return err[seq[string]](bindRes.err.code, bindRes.err.message, bindRes.err.context)
    let bound = bindRes.value
    case bound.kind
    of skCreateTable:
      let rootRes = initTableRoot(db.pager)
      if not rootRes.ok:
        return err[seq[string]](rootRes.err.code, rootRes.err.message, rootRes.err.context)
      var columns: seq[Column] = @[]
      for col in bound.columns:
        let typeRes = parseColumnType(col.typeName)
        if not typeRes.ok:
          return err[seq[string]](typeRes.err.code, typeRes.err.message, typeRes.err.context)
        columns.add(Column(name: col.name, kind: typeRes.value, notNull: col.notNull, unique: col.unique, primaryKey: col.primaryKey, refTable: col.refTable, refColumn: col.refColumn))
      let meta = TableMeta(name: bound.createTableName, rootPage: rootRes.value, nextRowId: 1, columns: columns)
      let saveRes = db.catalog.saveTable(db.pager, meta)
      if not saveRes.ok:
        return err[seq[string]](saveRes.err.code, saveRes.err.message, saveRes.err.context)
      for col in columns:
        if col.primaryKey or col.unique:
          let idxName = if col.primaryKey: "pk_" & meta.name & "_" & col.name & "_idx" else: "uniq_" & meta.name & "_" & col.name & "_idx"
          if isNone(db.catalog.getIndexByName(idxName)):
            let idxRootRes = initTableRoot(db.pager)
            if not idxRootRes.ok:
              return err[seq[string]](idxRootRes.err.code, idxRootRes.err.message, idxRootRes.err.context)
            let buildRes = buildIndexForColumn(db.pager, db.catalog, meta.name, col.name, idxRootRes.value)
            if not buildRes.ok:
              return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
            let idxMeta = IndexMeta(name: idxName, table: meta.name, column: col.name, rootPage: idxRootRes.value, kind: catalog.ikBtree, unique: true)
            let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
            if not idxSaveRes.ok:
              return err[seq[string]](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
        if col.refTable.len > 0 and col.refColumn.len > 0:
          if isNone(db.catalog.getBtreeIndexForColumn(meta.name, col.name)):
            let idxName = "fk_" & meta.name & "_" & col.name & "_idx"
            let idxRootRes = initTableRoot(db.pager)
            if not idxRootRes.ok:
              return err[seq[string]](idxRootRes.err.code, idxRootRes.err.message, idxRootRes.err.context)
            let buildRes = buildIndexForColumn(db.pager, db.catalog, meta.name, col.name, idxRootRes.value)
            if not buildRes.ok:
              return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
            let idxMeta = IndexMeta(name: idxName, table: meta.name, column: col.name, rootPage: idxRootRes.value, kind: catalog.ikBtree, unique: false)
            let idxSaveRes = db.catalog.createIndexMeta(idxMeta)
            if not idxSaveRes.ok:
              return err[seq[string]](idxSaveRes.err.code, idxSaveRes.err.message, idxSaveRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skDropTable:
      var toDrop: seq[string] = @[]
      for name, idx in db.catalog.indexes:
        if idx.table == bound.dropTableName:
          toDrop.add(name)
      for idxName in toDrop:
        discard db.catalog.dropIndex(idxName)
      let dropRes = db.catalog.dropTable(bound.dropTableName)
      if not dropRes.ok:
        return err[seq[string]](dropRes.err.code, dropRes.err.message, dropRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skCreateIndex:
      let indexRootRes = initTableRoot(db.pager)
      if not indexRootRes.ok:
        return err[seq[string]](indexRootRes.err.code, indexRootRes.err.message, indexRootRes.err.context)
      if bound.unique:
        let tableRes = db.catalog.getTable(bound.indexTableName)
        if not tableRes.ok:
          return err[seq[string]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
        let table = tableRes.value
        var colIndex = -1
        for i, col in table.columns:
          if col.name == bound.columnName:
            colIndex = i
            break
        if colIndex < 0:
          return err[seq[string]](ERR_SQL, "Column not found", bound.columnName)
        let rowsRes = scanTable(db.pager, table)
        if not rowsRes.ok:
          return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
        var seen = initTable[uint64, bool]()
        for row in rowsRes.value:
          if row.values[colIndex].kind == vkNull:
            continue
          let key = indexKeyFromValue(row.values[colIndex])
          if seen.hasKey(key):
            return err[seq[string]](ERR_CONSTRAINT, "UNIQUE index creation failed", bound.columnName)
          seen[key] = true
      if bound.indexKind == sql.ikTrigram:
        let buildRes = buildTrigramIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnName, indexRootRes.value)
        if not buildRes.ok:
          return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
      else:
        let buildRes = buildIndexForColumn(db.pager, db.catalog, bound.indexTableName, bound.columnName, indexRootRes.value)
        if not buildRes.ok:
          return err[seq[string]](buildRes.err.code, buildRes.err.message, buildRes.err.context)
      let idxKind = if bound.indexKind == sql.ikTrigram: catalog.ikTrigram else: catalog.ikBtree
      let idxMeta = IndexMeta(name: bound.indexName, table: bound.indexTableName, column: bound.columnName, rootPage: indexRootRes.value, kind: idxKind, unique: bound.unique)
      let saveRes = db.catalog.createIndexMeta(idxMeta)
      if not saveRes.ok:
        return err[seq[string]](saveRes.err.code, saveRes.err.message, saveRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skDropIndex:
      let dropRes = db.catalog.dropIndex(bound.dropIndexName)
      if not dropRes.ok:
        return err[seq[string]](dropRes.err.code, dropRes.err.message, dropRes.err.context)
      let bumpRes = schemaBump(db)
      if not bumpRes.ok:
        return err[seq[string]](bumpRes.err.code, bumpRes.err.message, bumpRes.err.context)
    of skInsert:
      let tableRes = db.catalog.getTable(bound.insertTable)
      if not tableRes.ok:
        return err[seq[string]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let valuesRes = evalInsertValues(bound, params)
      if not valuesRes.ok:
        return err[seq[string]](valuesRes.err.code, valuesRes.err.message, valuesRes.err.context)
      let values = valuesRes.value
      for i, col in tableRes.value.columns:
        let typeRes = typeCheckValue(col.kind, values[i])
        if not typeRes.ok:
          return err[seq[string]](typeRes.err.code, typeRes.err.message, col.name)
      let notNullRes = enforceNotNull(tableRes.value, values)
      if not notNullRes.ok:
        return err[seq[string]](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
      let uniqueRes = enforceUnique(db.catalog, db.pager, tableRes.value, values)
      if not uniqueRes.ok:
        return err[seq[string]](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
      let fkRes = enforceForeignKeys(db.catalog, db.pager, tableRes.value, values)
      if not fkRes.ok:
        return err[seq[string]](fkRes.err.code, fkRes.err.message, fkRes.err.context)
      let insertRes = insertRow(db.pager, db.catalog, bound.insertTable, values)
      if not insertRes.ok:
        return err[seq[string]](insertRes.err.code, insertRes.err.message, insertRes.err.context)
    of skUpdate:
      let tableRes = db.catalog.getTable(bound.updateTable)
      if not tableRes.ok:
        return err[seq[string]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let table = tableRes.value
      let rowsRes = scanTable(db.pager, table)
      if not rowsRes.ok:
        return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
      var updates: seq[(uint64, seq[Value], seq[Value])] = @[]
      for stored in rowsRes.value:
        var cols: seq[string] = @[]
        for col in table.columns:
          cols.add(bound.updateTable & "." & col.name)
        let row = Row(rowid: stored.rowid, columns: cols, values: stored.values)
        let whereRes = evalExpr(row, bound.updateWhere, params)
        if not whereRes.ok:
          return err[seq[string]](whereRes.err.code, whereRes.err.message, whereRes.err.context)
        if bound.updateWhere != nil and not valueToBool(whereRes.value):
          continue
        var newValues = stored.values
        for colName, expr in bound.assignments:
          var idx = -1
          for i, col in table.columns:
            if col.name == colName:
              idx = i
              break
          if idx >= 0:
            let evalRes = evalExpr(row, expr, params)
            if not evalRes.ok:
              return err[seq[string]](evalRes.err.code, evalRes.err.message, evalRes.err.context)
            let typeRes = typeCheckValue(table.columns[idx].kind, evalRes.value)
            if not typeRes.ok:
              return err[seq[string]](typeRes.err.code, typeRes.err.message, colName)
            newValues[idx] = evalRes.value
        updates.add((stored.rowid, stored.values, newValues))
      for entry in updates:
        let notNullRes = enforceNotNull(table, entry[2])
        if not notNullRes.ok:
          return err[seq[string]](notNullRes.err.code, notNullRes.err.message, notNullRes.err.context)
        let uniqueRes = enforceUnique(db.catalog, db.pager, table, entry[2], entry[0])
        if not uniqueRes.ok:
          return err[seq[string]](uniqueRes.err.code, uniqueRes.err.message, uniqueRes.err.context)
        let fkRes = enforceForeignKeys(db.catalog, db.pager, table, entry[2])
        if not fkRes.ok:
          return err[seq[string]](fkRes.err.code, fkRes.err.message, fkRes.err.context)
        let restrictRes = enforceRestrictOnParent(db.catalog, db.pager, table, entry[1], entry[2])
        if not restrictRes.ok:
          return err[seq[string]](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)
      for entry in updates:
        let upRes = updateRow(db.pager, db.catalog, bound.updateTable, entry[0], entry[2])
        if not upRes.ok:
          return err[seq[string]](upRes.err.code, upRes.err.message, upRes.err.context)
    of skDelete:
      let tableRes = db.catalog.getTable(bound.deleteTable)
      if not tableRes.ok:
        return err[seq[string]](tableRes.err.code, tableRes.err.message, tableRes.err.context)
      let table = tableRes.value
      let rowsRes = scanTable(db.pager, table)
      if not rowsRes.ok:
        return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
      var deletions: seq[StoredRow] = @[]
      for stored in rowsRes.value:
        var cols: seq[string] = @[]
        for col in table.columns:
          cols.add(bound.deleteTable & "." & col.name)
        let row = Row(rowid: stored.rowid, columns: cols, values: stored.values)
        let whereRes = evalExpr(row, bound.deleteWhere, params)
        if not whereRes.ok:
          return err[seq[string]](whereRes.err.code, whereRes.err.message, whereRes.err.context)
        if bound.deleteWhere != nil and not valueToBool(whereRes.value):
          continue
        deletions.add(stored)
      for row in deletions:
        let restrictRes = enforceRestrictOnDelete(db.catalog, db.pager, table, row.values)
        if not restrictRes.ok:
          return err[seq[string]](restrictRes.err.code, restrictRes.err.message, restrictRes.err.context)
      for row in deletions:
        let delRes = deleteRow(db.pager, db.catalog, bound.deleteTable, row.rowid)
        if not delRes.ok:
          return err[seq[string]](delRes.err.code, delRes.err.message, delRes.err.context)
    of skSelect:
      let planRes = plan(db.catalog, bound)
      if not planRes.ok:
        return err[seq[string]](planRes.err.code, planRes.err.message, planRes.err.context)
      let rowsRes = execPlan(db.pager, db.catalog, planRes.value, params)
      if not rowsRes.ok:
        return err[seq[string]](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
      for row in rowsRes.value:
        var parts: seq[string] = @[]
        for value in row.values:
          parts.add(valueToString(value))
        output.add(parts.join("|"))
  ok(output)

proc execSql*(db: Db, sqlText: string): Result[seq[string]] =
  execSql(db, sqlText, @[])

proc closeDb*(db: Db): Result[Void] =
  if not db.isOpen:
    return okVoid()
  let pagerRes = closePager(db.pager)
  if not pagerRes.ok:
    return pagerRes
  let res = db.vfs.close(db.file)
  if not res.ok:
    return res
  db.isOpen = false
  okVoid()
