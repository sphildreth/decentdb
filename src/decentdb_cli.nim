import json
import os
import strutils
import tables
import parsecsv
import streams
import times
import atomics
import ./engine
import ./errors
import ./catalog/catalog
import ./pager/pager
import ./pager/db_header
import ./record/record
import ./storage/storage
import ./wal/wal
import ./vfs/os_vfs

const Version = "0.0.1"

# Helper to convert JSON to Result for consistent output format
proc resultJson(ok: bool, err: DbError = DbError(), rows: seq[string] = @[]): JsonNode =
  let errorNode = if ok: newJNull() else: errorToJson(err)
  %*{
    "ok": ok,
    "error": errorNode,
    "rows": rows
  }

proc loadConfig(): Table[string, string] =
  var cfg = initTable[string, string]()
  let path = expandTilde("~/.decentdb/config")
  if not fileExists(path):
    return cfg
  for line in lines(path):
    let trimmed = line.strip()
    if trimmed.len == 0 or trimmed.startsWith("#"):
      continue
    let eqPos = trimmed.find('=')
    if eqPos <= 0:
      continue
    let key = trimmed[0 ..< eqPos].strip()
    let value = trimmed[eqPos + 1 .. ^1].strip()
    if key.len > 0:
      cfg[key] = value
  cfg

proc applyDbConfig(db: string, cfg: Table[string, string]): string =
  if db.len > 0:
    return db
  if cfg.hasKey("db"):
    return cfg["db"]
  ""

proc resolveDbPath(db: string): string =
  let cfg = loadConfig()
  applyDbConfig(db, cfg)

proc parseConfigInt(cfg: Table[string, string], key: string): int =
  if not cfg.hasKey(key):
    return 0
  try:
    parseInt(cfg[key])
  except:
    0

proc formatRows(rows: seq[string], format: string): string =
  let normalized = format.strip().toLowerAscii()
  if normalized == "csv":
    var lines: seq[string] = @[]
    for row in rows:
      lines.add(row.replace("|", ","))
    return lines.join("\n")
  if normalized == "table":
    var lines: seq[string] = @[]
    for row in rows:
      lines.add(row.replace("|", " | "))
    return lines.join("\n")
  ""

proc emitRows(rows: seq[string], format: string) =
  let normalized = format.strip().toLowerAscii()
  if normalized == "json" or normalized.len == 0:
    echo resultJson(true, rows = rows)
    return
  let rendered = formatRows(rows, normalized)
  if rendered.len == 0:
    if rows.len == 0:
      return
    echo resultJson(false, DbError(code: ERR_IO, message: "Unsupported output format", context: format))
    return
  echo rendered

proc formatDbInfo(database: Db): seq[string] =
  @[
    "Format version: " & $database.formatVersion,
    "Page size: " & $database.pageSize & " bytes",
    "Cache pages: " & $database.cachePages & " (" & $(database.cachePages * 4096 div 1024) & "KB)",
    "Schema cookie: " & $database.schemaCookie,
    "WAL LSN: " & $database.wal.walEnd.load(moAcquire),
    "Active readers: " & $readerCount(database.wal)
  ]

proc formatStats(database: Db): seq[string] =
  let cache = database.pager.cache
  @[
    "Page size: " & $database.pager.pageSize & " bytes",
    "Page count: " & $database.pager.pageCount,
    "Cache capacity: " & $cache.capacity & " pages",
    "Cache loaded: " & $cache.pages.len & " pages",
    "WAL LSN: " & $database.wal.walEnd.load(moAcquire),
    "Active readers: " & $readerCount(database.wal)
  ]

proc parseDurability(mode: string): Result[DurabilityMode] =
  let normalized = mode.strip().toLowerAscii()
  case normalized
  of "full":
    ok(dmFull)
  of "deferred":
    ok(dmDeferred)
  of "none":
    ok(dmNone)
  else:
    err[DurabilityMode](ERR_SQL, "Invalid durability mode", mode)

proc parseParamValue(raw: string): Result[Value] =
  let trimmed = raw.strip()
  if trimmed.len == 0:
    return ok(Value(kind: vkNull))
  let lower = trimmed.toLowerAscii()
  if lower == "null":
    return ok(Value(kind: vkNull))
  let sep = trimmed.find(':')
  if sep > 0:
    let prefix = trimmed[0 ..< sep].toLowerAscii()
    let value = trimmed[sep + 1 .. ^1]
    case prefix
    of "int", "int64":
      try:
        return ok(Value(kind: vkInt64, int64Val: parseBiggestInt(value)))
      except:
        return err[Value](ERR_SQL, "Invalid int parameter", raw)
    of "float", "float64":
      try:
        return ok(Value(kind: vkFloat64, float64Val: parseFloat(value)))
      except:
        return err[Value](ERR_SQL, "Invalid float parameter", raw)
    of "bool":
      return ok(Value(kind: vkBool, boolVal: value.toLowerAscii() in ["true", "1", "yes"]))
    of "text", "string":
      var bytes: seq[byte] = @[]
      for ch in value:
        bytes.add(byte(ch))
      return ok(Value(kind: vkText, bytes: bytes))
    of "blob":
      var bytes: seq[byte] = @[]
      let v = value.strip()
      if v.startsWith("0x") and (v.len mod 2 == 0):
        var i = 2
        while i < v.len:
          try:
            let byteVal = parseHexInt(v[i .. i + 1])
            bytes.add(byte(byteVal))
          except:
            return err[Value](ERR_SQL, "Invalid blob hex parameter", raw)
          i += 2
      else:
        for ch in v:
          bytes.add(byte(ch))
      return ok(Value(kind: vkBlob, bytes: bytes))
    else:
      return err[Value](ERR_SQL, "Unknown parameter type", raw)
  var bytes: seq[byte] = @[]
  for ch in trimmed:
    bytes.add(byte(ch))
  ok(Value(kind: vkText, bytes: bytes))

proc parseWalFailpointSpec(spec: string): Result[(string, WalFailpoint)] =
  let trimmed = spec.strip()
  if trimmed.len == 0:
    return err[(string, WalFailpoint)](ERR_IO, "Empty WAL failpoint spec")
  let parts = trimmed.split(":")
  let label = parts[0].strip()
  if label.len == 0:
    return err[(string, WalFailpoint)](ERR_IO, "Missing WAL failpoint label", spec)
  var kind = wfError
  var partialBytes = 0
  var remaining = 0
  if parts.len >= 2:
    let kindText = parts[1].strip().toLowerAscii()
    case kindText
    of "none":
      kind = wfNone
    of "error":
      kind = wfError
    of "partial":
      kind = wfPartial
    else:
      return err[(string, WalFailpoint)](ERR_IO, "Unknown WAL failpoint kind", spec)
  if kind == wfPartial:
    if parts.len < 3:
      return err[(string, WalFailpoint)](ERR_IO, "Partial failpoint requires byte count", spec)
    try:
      partialBytes = parseInt(parts[2].strip())
    except:
      return err[(string, WalFailpoint)](ERR_IO, "Invalid partial byte count", spec)
  if parts.len >= 4:
    try:
      remaining = parseInt(parts[3].strip())
    except:
      return err[(string, WalFailpoint)](ERR_IO, "Invalid failpoint remaining count", spec)
  ok((label, WalFailpoint(kind: kind, partialBytes: partialBytes, remaining: remaining)))

proc csvCellToValue(cellValue: string, col: Column): Value =
  if cellValue.len == 0:
    return Value(kind: vkNull)
  case col.kind
  of ctInt64:
    try:
      Value(kind: vkInt64, int64Val: parseBiggestInt(cellValue))
    except:
      Value(kind: vkNull)
  of ctBool:
    Value(kind: vkBool, boolVal: cellValue.toLowerAscii() in ["true", "1", "yes"])
  of ctFloat64:
    try:
      Value(kind: vkFloat64, float64Val: parseFloat(cellValue))
    except:
      Value(kind: vkNull)
  of ctText:
    var bytes: seq[byte] = @[]
    for ch in cellValue:
      bytes.add(byte(ch))
    Value(kind: vkText, bytes: bytes)
  of ctBlob:
    var bytes: seq[byte] = @[]
    for ch in cellValue:
      bytes.add(byte(ch))
    Value(kind: vkBlob, bytes: bytes)

proc readCsvRows(tableMeta: TableMeta, csvFile: string): Result[seq[seq[Value]]] =
  var parser: CsvParser
  try:
    var s = newFileStream(csvFile, fmRead)
    if s == nil:
      return err[seq[seq[Value]]](ERR_IO, "Cannot open CSV file", csvFile)
    parser.open(s, csvFile)
    parser.readHeaderRow()

    var rows: seq[seq[Value]] = @[]
    while parser.readRow():
      if parser.headers.len != tableMeta.columns.len:
        parser.close()
        return err[seq[seq[Value]]](ERR_IO, "CSV column count mismatch", $parser.headers.len & " vs " & $tableMeta.columns.len)
      var values: seq[Value] = @[]
      for i, col in tableMeta.columns:
        values.add(csvCellToValue(parser.row[i], col))
      rows.add(values)
    parser.close()
    ok(rows)
  except:
    let msg = getCurrentExceptionMsg()
    err[seq[seq[Value]]](ERR_IO, "CSV parsing error", msg)

proc jsonToValue(node: JsonNode, col: Column): Value =
  if node.isNil or node.kind == JNull:
    return Value(kind: vkNull)
  case col.kind
  of ctInt64:
    if node.kind == JInt:
      return Value(kind: vkInt64, int64Val: node.getBiggestInt())
    if node.kind == JFloat:
      return Value(kind: vkInt64, int64Val: int64(node.getFloat()))
    if node.kind == JString:
      try:
        return Value(kind: vkInt64, int64Val: parseBiggestInt(node.getStr()))
      except:
        return Value(kind: vkNull)
    Value(kind: vkNull)
  of ctBool:
    if node.kind == JBool:
      return Value(kind: vkBool, boolVal: node.getBool())
    if node.kind == JString:
      return Value(kind: vkBool, boolVal: node.getStr().toLowerAscii() in ["true", "1", "yes"])
    Value(kind: vkNull)
  of ctFloat64:
    if node.kind == JFloat:
      return Value(kind: vkFloat64, float64Val: node.getFloat())
    if node.kind == JInt:
      return Value(kind: vkFloat64, float64Val: float64(node.getBiggestInt()))
    if node.kind == JString:
      try:
        return Value(kind: vkFloat64, float64Val: parseFloat(node.getStr()))
      except:
        return Value(kind: vkNull)
    Value(kind: vkNull)
  of ctText:
    if node.kind == JString:
      var bytes: seq[byte] = @[]
      for ch in node.getStr():
        bytes.add(byte(ch))
      return Value(kind: vkText, bytes: bytes)
    Value(kind: vkNull)
  of ctBlob:
    if node.kind == JString:
      let value = node.getStr()
      var bytes: seq[byte] = @[]
      if value.startsWith("0x") and value.len mod 2 == 0:
        var i = 2
        while i < value.len:
          try:
            let byteVal = parseHexInt(value[i .. i + 1])
            bytes.add(byte(byteVal))
          except:
            bytes = @[]
            break
          i += 2
      else:
        for ch in value:
          bytes.add(byte(ch))
      return Value(kind: vkBlob, bytes: bytes)
    Value(kind: vkNull)

proc readJsonRows(tableMeta: TableMeta, jsonFile: string): Result[seq[seq[Value]]] =
  try:
    let content = readFile(jsonFile)
    let root = parseJson(content)
    if root.kind != JArray:
      return err[seq[seq[Value]]](ERR_IO, "JSON input must be an array", jsonFile)
    var rows: seq[seq[Value]] = @[]
    for rowNode in root.items:
      var values: seq[Value] = @[]
      if rowNode.kind == JArray:
        if rowNode.len != tableMeta.columns.len:
          return err[seq[seq[Value]]](ERR_IO, "JSON column count mismatch", $rowNode.len & " vs " & $tableMeta.columns.len)
        for i, col in tableMeta.columns:
          values.add(jsonToValue(rowNode[i], col))
      elif rowNode.kind == JObject:
        for col in tableMeta.columns:
          if rowNode.hasKey(col.name):
            values.add(jsonToValue(rowNode[col.name], col))
          else:
            values.add(Value(kind: vkNull))
      else:
        return err[seq[seq[Value]]](ERR_IO, "JSON rows must be arrays or objects", jsonFile)
      rows.add(values)
    ok(rows)
  except:
    let msg = getCurrentExceptionMsg()
    err[seq[seq[Value]]](ERR_IO, "JSON parsing error", msg)

# ============================================================================
# Main SQL Execution Command
# ============================================================================

proc cliMain*(db: string = "", sql: string = "", openClose: bool = false, timing: bool = false,
              cachePages: int = 64, cacheMb: int = 0, checkpoint: bool = false,
              readerCount: bool = false, longReaders: int = 0, dbInfo: bool = false,
              warnings: bool = false, verbose: bool = false,
              checkpointBytes: int = 0, checkpointMs: int = 0,
              format: string = "json", params: seq[string] = @[],
              walFailpoints: seq[string] = @[], clearWalFailpoints: bool = false): int =
  ## DecentDb CLI v0.0.1 - ACID-first embedded relational database
  ## 
  ## Execute SQL statements against a DecentDb database file.
  ## All output is JSON formatted for programmatic use.
  
  let startTime = if timing: epochTime() else: 0.0
  
  let cfg = loadConfig()
  let dbPath = applyDbConfig(db, cfg)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  # Calculate cache size (cacheMb takes precedence if specified)
  let configCacheMb = parseConfigInt(cfg, "cacheMb")
  let configCachePages = parseConfigInt(cfg, "cachePages")
  let resolvedCacheMb = if cacheMb > 0: cacheMb else: configCacheMb
  let resolvedCachePages = if cachePages != 64 or resolvedCacheMb > 0: cachePages else: configCachePages
  let actualCachePages = if resolvedCacheMb > 0:
    (resolvedCacheMb * 1024 * 1024) div 4096  # Convert MB to 4KB pages
  else:
    if resolvedCachePages > 0: resolvedCachePages else: cachePages

  let openRes = openDb(dbPath, cachePages = actualCachePages)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value

  if clearWalFailpoints:
    clearFailpoints(database.wal)
  for spec in walFailpoints:
    let fpRes = parseWalFailpointSpec(spec)
    if not fpRes.ok:
      discard closeDb(database)
      echo resultJson(false, fpRes.err)
      return 1
    database.wal.setFailpoint(fpRes.value[0], fpRes.value[1])
  
  # Configure auto-checkpoint policies if specified
  if checkpointBytes > 0 or checkpointMs > 0:
    setCheckpointConfig(database.wal, int64(checkpointBytes), int64(checkpointMs))
  
  # Handle reader count diagnostic
  if readerCount:
    let count = readerCount(database.wal)
    discard closeDb(database)
    echo resultJson(true, rows = @["Active readers: " & $count])
    return 0
  
  # Handle long-running readers diagnostic
  if longReaders > 0:
    let longRunning = readersOverThreshold(database.wal, int64(longReaders))
    var info: seq[string] = @[]
    info.add("Threshold: " & $longReaders & "ms")
    for reader in longRunning:
      info.add("Snapshot " & $reader.snapshot & " age: " & $reader.ageMs & "ms")
    discard closeDb(database)
    echo resultJson(true, rows = info)
    return 0
  
  # Handle database info request
  if dbInfo:
    let info = formatDbInfo(database)
    discard closeDb(database)
    echo resultJson(true, rows = info)
    return 0
  
  # Handle checkpoint request
  if checkpoint:
    let ckRes = checkpointDb(database)
    if not ckRes.ok:
      discard closeDb(database)
      echo resultJson(false, ckRes.err)
      return 1
    
    var ckRows: seq[string] = @["Checkpoint completed at LSN " & $ckRes.value]
    
    # Add warnings if present
    if warnings or verbose:
      let walWarnings = takeWarnings(database.wal)
      for warn in walWarnings:
        ckRows.add("WARNING: " & warn)
    
    discard closeDb(database)
    echo resultJson(true, rows = ckRows)
    return 0
  
  if not openClose and sql.len > 0:
    var parsedParams: seq[Value] = @[]
    for param in params:
      let valueRes = parseParamValue(param)
      if not valueRes.ok:
        discard closeDb(database)
        echo resultJson(false, valueRes.err)
        return 1
      parsedParams.add(valueRes.value)
    let queryStart = if timing: epochTime() else: 0.0
    let execRes = execSql(database, sql, parsedParams)
    let queryEnd = if timing: epochTime() else: 0.0
    
    if not execRes.ok:
      discard closeDb(database)
      echo resultJson(false, execRes.err)
      return 1
    
    let rows = execRes.value
    
    # Collect warnings if requested
    var walWarnings: seq[string] = @[]
    if warnings or verbose:
      walWarnings = takeWarnings(database.wal)
    
    discard closeDb(database)
    
    # Add timing info and warnings to JSON output if requested
    if timing or warnings or verbose:
      var resultPayload = resultJson(true, rows = rows)
      
      if timing:
        let totalTime = (epochTime() - startTime) * 1000.0  # Convert to ms
        let queryTime = (queryEnd - queryStart) * 1000.0
        let timingInfo = %*{
          "total_ms": totalTime,
          "query_ms": queryTime,
          "cache_pages": actualCachePages,
          "cache_mb": (actualCachePages * 4096) div (1024 * 1024)
        }
        resultPayload["timing"] = timingInfo
      
      if (warnings or verbose) and walWarnings.len > 0:
        resultPayload["warnings"] = %walWarnings
      
      if verbose:
        let verboseInfo = %*{
          "wal_lsn": database.wal.walEnd.load(moAcquire),
          "active_readers": readerCount(database.wal),
          "cache_pages": actualCachePages
        }
        resultPayload["verbose"] = verboseInfo
      
      if format.strip().toLowerAscii() == "json":
        echo resultPayload
      else:
        echo resultJson(false, DbError(code: ERR_IO, message: "Non-JSON format not supported with timing/warnings/verbose"))
    else:
      emitRows(rows, format)
    return 0

  discard closeDb(database)
  emitRows(@[], format)
  return 0

# ============================================================================
# Schema Introspection Commands
# ============================================================================

proc schemaListTables*(db: string = ""): int =
  ## List all tables in the database
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  var tables: seq[string] = @[]
  
  for tableName, _ in database.catalog.tables:
    tables.add(tableName)
  
  discard closeDb(database)
  echo resultJson(true, rows = tables)
  return 0

proc schemaDescribe*(table: string, db: string = ""): int =
  ## Show table structure (columns, types, constraints)
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  
  if table.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing table name argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  
  if not database.catalog.tables.hasKey(table):
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_SQL, message: "Table not found", context: table))
    return 1
  
  let tableMeta = database.catalog.tables[table]
  var output: seq[string] = @[]
  output.add("Column|Type|NotNull|PrimaryKey|Unique|RefTable|RefColumn")
  
  for col in tableMeta.columns:
    let colType = case col.kind
      of ctInt64: "INT64"
      of ctBool: "BOOL"
      of ctFloat64: "FLOAT64"
      of ctText: "TEXT"
      of ctBlob: "BLOB"
    
    let notNull = if col.notNull: "YES" else: "NO"
    let primaryKey = if col.primaryKey: "YES" else: "NO"
    let unique = if col.unique: "YES" else: "NO"
    let refTable = col.refTable
    let refColumn = col.refColumn
    
    output.add("$1|$2|$3|$4|$5|$6|$7" % [col.name, colType, notNull, primaryKey, unique, refTable, refColumn])
  
  discard closeDb(database)
  echo resultJson(true, rows = output)
  return 0

proc schemaListIndexes*(db: string = "", table: string = ""): int =
  ## List all indexes, optionally filtered by table
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  var output: seq[string] = @[]
  output.add("Index|Table|Column|Type|Unique")
  
  for indexName, indexMeta in database.catalog.indexes:
    if table.len > 0 and indexMeta.table != table:
      continue
    
    let indexType = case indexMeta.kind
      of ikBtree: "btree"
      of ikTrigram: "trigram"
    
    let unique = if indexMeta.unique: "YES" else: "NO"
    output.add("$1|$2|$3|$4|$5" % [indexName, indexMeta.table, indexMeta.column, indexType, unique])
  
  discard closeDb(database)
  echo resultJson(true, rows = output)
  return 0

# ============================================================================
# Index Maintenance Commands
# ============================================================================

proc cmdRebuildIndex*(index: string = "", db: string = ""): int =
  ## Rebuild an index from scratch
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  
  if index.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --index argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  
  if not database.catalog.indexes.hasKey(index):
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_SQL, message: "Index not found", context: index))
    return 1
  
  let indexMeta = database.catalog.indexes[index]
  let rebuildRes = storage.rebuildIndex(database.pager, database.catalog, indexMeta)
  
  if not rebuildRes.ok:
    discard closeDb(database)
    echo resultJson(false, rebuildRes.err)
    return 1
  
  discard closeDb(database)
  echo resultJson(true, rows = @["Index '" & index & "' rebuilt successfully"])
  return 0

proc cmdVerifyIndex*(index: string = "", db: string = ""): int =
  ## Verify index integrity (basic check)
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  
  if index.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --index argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  
  if not database.catalog.indexes.hasKey(index):
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_SQL, message: "Index not found", context: index))
    return 1
  
  let indexMeta = database.catalog.indexes[index]
  
  # Basic verification - check that index exists in catalog
  # More sophisticated verification would scan index structure
  var info: seq[string] = @[]
  info.add("Index: " & index)
  info.add("Table: " & indexMeta.table)
  info.add("Column: " & indexMeta.column)
  info.add("Type: " & $indexMeta.kind)
  info.add("Root page: " & $indexMeta.rootPage)
  info.add("Status: OK")
  
  discard closeDb(database)
  echo resultJson(true, rows = info)
  return 0

# ============================================================================
# Import/Export Commands
# ============================================================================

proc importData*(table: string, input: string, db: string = "", batchSize: int = 10000, format: string = "csv"): int =
  ## Import data from CSV or JSON into a table
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  
  if table.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing table name"))
    return 1
  
  if input.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing input file path"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  
  if not database.catalog.tables.hasKey(table):
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_SQL, message: "Table not found", context: table))
    return 1
  
  let tableMeta = database.catalog.tables[table]
  
  let formatNormalized = format.strip().toLowerAscii()
  let rowsRes = if formatNormalized == "csv":
    readCsvRows(tableMeta, input)
  elif formatNormalized == "json":
    readJsonRows(tableMeta, input)
  else:
    err[seq[seq[Value]]](ERR_IO, "Unsupported import format", format)
  if not rowsRes.ok:
    discard closeDb(database)
    echo resultJson(false, rowsRes.err)
    return 1

  var rowCount = 0
  var batch: seq[seq[Value]] = @[]
  for row in rowsRes.value:
    batch.add(row)
    rowCount.inc
    if batch.len >= batchSize:
      for insertRowValues in batch:
        let insertRes = insertRow(database.pager, database.catalog, table, insertRowValues)
        if not insertRes.ok:
          discard closeDb(database)
          echo resultJson(false, insertRes.err)
          return 1
      batch = @[]

  if batch.len > 0:
    for insertRowValues in batch:
      let insertRes = insertRow(database.pager, database.catalog, table, insertRowValues)
      if not insertRes.ok:
        discard closeDb(database)
        echo resultJson(false, insertRes.err)
        return 1

  discard closeDb(database)
  echo resultJson(true, rows = @["Imported " & $rowCount & " rows"])
  return 0

proc exportData*(table: string, output: string, db: string = "", format: string = "csv"): int =
  ## Export table data to CSV or JSON file
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  
  if table.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing table name"))
    return 1
  
  if output.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing output file path"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  
  if not database.catalog.tables.hasKey(table):
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_SQL, message: "Table not found", context: table))
    return 1
  
  let tableMeta = database.catalog.tables[table]
  let formatNormalized = format.strip().toLowerAscii()
  if formatNormalized notin ["csv", "json"]:
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_IO, message: "Unsupported export format", context: format))
    return 1

  try:
    # Read all rows
    let rowsRes = scanTable(database.pager, tableMeta)
    if not rowsRes.ok:
      discard closeDb(database)
      echo resultJson(false, rowsRes.err)
      return 1

    if formatNormalized == "csv":
      var f = open(output, fmWrite)
      var headers: seq[string] = @[]
      for col in tableMeta.columns:
        headers.add(col.name)
      f.writeLine(headers.join(","))

      var rowCount = 0
      for row in rowsRes.value:
        var fields: seq[string] = @[]
        for value in row.values:
          let fieldValue = case value.kind
          of vkNull:
            ""
          of vkInt64:
            $value.int64Val
          of vkBool:
            if value.boolVal: "true" else: "false"
          of vkFloat64:
            $value.float64Val
          of vkText, vkBlob:
            var s = ""
            for b in value.bytes:
              s.add(char(b))
            s
          else:
            ""

          let escaped = if "," in fieldValue or "\"" in fieldValue:
            "\"" & fieldValue.replace("\"", "\"\"") & "\""
          else:
            fieldValue
          fields.add(escaped)
        f.writeLine(fields.join(","))
        rowCount.inc
      f.close()
      discard closeDb(database)
      echo resultJson(true, rows = @["Exported " & $rowCount & " rows to " & output & " (csv)"])
      return 0

    var f = open(output, fmWrite)
    var jsonRows: JsonNode = newJArray()
    for row in rowsRes.value:
      var obj = newJObject()
      for i, value in row.values:
        let colName = tableMeta.columns[i].name
        case value.kind
        of vkNull:
          obj[colName] = newJNull()
        of vkInt64:
          obj[colName] = %value.int64Val
        of vkBool:
          obj[colName] = %value.boolVal
        of vkFloat64:
          obj[colName] = %value.float64Val
        of vkText:
          var s = ""
          for b in value.bytes:
            s.add(char(b))
          obj[colName] = %s
        of vkBlob:
          var hexStr = ""
          for b in value.bytes:
            hexStr.add(toHex(int(b), 2))
          obj[colName] = %("0x" & hexStr)
        else:
          obj[colName] = newJNull()
      jsonRows.add(obj)
    f.writeLine($jsonRows)
    f.close()
    discard closeDb(database)
    echo resultJson(true, rows = @["Exported " & $rowsRes.value.len & " rows to " & output & " (json)"])
    return 0
  except:
    let msg = getCurrentExceptionMsg()
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_IO, message: "File write error", context: msg))
    return 1

proc dumpSql*(db: string = "", output: string = ""): int =
  ## Dump entire database as SQL statements
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  var sqlStatements: seq[string] = @[]
  
  # Generate CREATE TABLE statements
  for tableName, tableMeta in database.catalog.tables:
    var createStmt = "CREATE TABLE " & tableName & " (\n"
    var columnDefs: seq[string] = @[]
    
    for col in tableMeta.columns:
      var colDef = "  " & col.name & " "
      
      let typeName = case col.kind
        of ctInt64: "INT64"
        of ctBool: "BOOL"
        of ctFloat64: "FLOAT64"
        of ctText: "TEXT"
        of ctBlob: "BLOB"
      
      colDef &= typeName
      
      if col.primaryKey:
        colDef &= " PRIMARY KEY"
      if col.unique and not col.primaryKey:
        colDef &= " UNIQUE"
      if col.notNull and not col.primaryKey:
        colDef &= " NOT NULL"
      if col.refTable.len > 0 and col.refColumn.len > 0:
        colDef &= " REFERENCES " & col.refTable & "(" & col.refColumn & ")"
      
      columnDefs.add(colDef)
    
    createStmt &= columnDefs.join(",\n") & "\n);"
    sqlStatements.add(createStmt)
    
    # Generate INSERT statements
    let rowsRes = scanTable(database.pager, tableMeta)
    if rowsRes.ok:
      for row in rowsRes.value:
        var insertStmt = "INSERT INTO " & tableName & " VALUES ("
        var values: seq[string] = @[]
        
        for value in row.values:
          let valueStr = case value.kind
          of vkNull:
            "NULL"
          of vkInt64:
            $value.int64Val
          of vkBool:
            if value.boolVal: "true" else: "false"
          of vkFloat64:
            $value.float64Val
          of vkText:
            var s = ""
            for b in value.bytes:
              s.add(char(b))
            "'" & s.replace("'", "''") & "'"
          of vkBlob:
            var hexStr = ""
            for b in value.bytes:
              hexStr.add(toHex(int(b), 2))
            "X'" & hexStr & "'"
          else:
            "NULL"
          
          values.add(valueStr)
        
        insertStmt &= values.join(", ") & ");"
        sqlStatements.add(insertStmt)
  
  discard closeDb(database)
  
  # Output to file or stdout
  if output.len > 0:
    try:
      var f = open(output, fmWrite)
      for stmt in sqlStatements:
        f.writeLine(stmt)
      f.close()
      echo resultJson(true, rows = @["Dumped " & $sqlStatements.len & " statements to " & output])
      return 0
    except:
      let msg = getCurrentExceptionMsg()
      echo resultJson(false, DbError(code: ERR_IO, message: "File write error", context: msg))
      return 1
  else:
    echo resultJson(true, rows = sqlStatements)
    return 0

# ============================================================================
# Header Forensics Commands
# ============================================================================

proc dumpHeader*(db: string = ""): int =
  ## Dump raw database header fields and checksum status
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let vfs = newOsVfs()
  let openRes = vfs.open(dbPath, fmReadWrite, false)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1
  let file = openRes.value

  var buf = newSeq[byte](HeaderSize)
  let readRes = vfs.read(file, 0, buf)
  discard vfs.close(file)
  if not readRes.ok:
    echo resultJson(false, readRes.err)
    return 1
  if readRes.value < HeaderSize:
    echo resultJson(false, DbError(code: ERR_CORRUPTION, message: "Header too short", context: "page_id=1"))
    return 1

  let magicOk = headerMagicOk(buf)
  let checksumExpected = headerChecksumExpected(buf)
  let checksumActual = headerChecksumActual(buf)
  let checksumOk = checksumExpected == checksumActual
  let headerRes = decodeHeaderUnsafe(buf)
  if not headerRes.ok:
    echo resultJson(false, headerRes.err)
    return 1

  let header = headerRes.value
  let rows = @[
    "Magic OK: " & $magicOk,
    "Checksum OK: " & $checksumOk,
    "Checksum expected: " & $checksumExpected,
    "Checksum actual: " & $checksumActual,
    "Format version: " & $header.formatVersion,
    "Page size: " & $header.pageSize,
    "Schema cookie: " & $header.schemaCookie,
    "Root catalog: " & $header.rootCatalog,
    "Root freelist: " & $header.rootFreelist,
    "Freelist head: " & $header.freelistHead,
    "Freelist count: " & $header.freelistCount,
    "Last checkpoint LSN: " & $header.lastCheckpointLsn
  ]
  echo resultJson(true, rows = rows)
  return 0

proc verifyHeader*(db: string = ""): int =
  ## Verify database header magic and checksum
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let vfs = newOsVfs()
  let openRes = vfs.open(dbPath, fmReadWrite, false)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1
  let file = openRes.value

  var buf = newSeq[byte](HeaderSize)
  let readRes = vfs.read(file, 0, buf)
  discard vfs.close(file)
  if not readRes.ok:
    echo resultJson(false, readRes.err)
    return 1
  if readRes.value < HeaderSize:
    echo resultJson(false, DbError(code: ERR_CORRUPTION, message: "Header too short", context: "page_id=1"))
    return 1

  let magicOk = headerMagicOk(buf)
  let checksumOk = headerChecksumExpected(buf) == headerChecksumActual(buf)
  if not magicOk:
    echo resultJson(false, DbError(code: ERR_CORRUPTION, message: "Bad header magic", context: "page_id=1"))
    return 1
  if not checksumOk:
    echo resultJson(false, DbError(code: ERR_CORRUPTION, message: "Header checksum mismatch", context: "page_id=1"))
    return 1
  echo resultJson(true, rows = @["Header OK"])
  return 0

# ============================================================================
# Bulk Load Command
# ============================================================================

proc bulkLoadCsv*(table: string, input: string, db: string = "", batchSize: int = 10000,
                  syncInterval: int = 10, durability: string = "deferred",
                  disableIndexes: bool = true, noCheckpoint: bool = false): int =
  ## Bulk load data from CSV using optimized ingestion
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  if table.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing table name"))
    return 1
  if input.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing input file path"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  if not database.catalog.tables.hasKey(table):
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_SQL, message: "Table not found", context: table))
    return 1

  let durRes = parseDurability(durability)
  if not durRes.ok:
    discard closeDb(database)
    echo resultJson(false, durRes.err)
    return 1

  let rowsRes = readCsvRows(database.catalog.tables[table], input)
  if not rowsRes.ok:
    discard closeDb(database)
    echo resultJson(false, rowsRes.err)
    return 1

  let options = BulkLoadOptions(
    batchSize: batchSize,
    syncInterval: syncInterval,
    disableIndexes: disableIndexes,
    checkpointOnComplete: not noCheckpoint,
    durability: durRes.value
  )
  let loadRes = bulkLoad(database, table, rowsRes.value, options, database.wal)
  if not loadRes.ok:
    discard closeDb(database)
    echo resultJson(false, loadRes.err)
    return 1

  discard closeDb(database)
  echo resultJson(true, rows = @["Bulk loaded " & $rowsRes.value.len & " rows"])
  return 0

# ============================================================================
# Maintenance & Diagnostics Commands
# ============================================================================

proc checkpointCmd*(db: string = "", warnings: bool = false, verbose: bool = false): int =
  ## Force a WAL checkpoint and exit
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  let ckRes = checkpointDb(database)
  if not ckRes.ok:
    discard closeDb(database)
    echo resultJson(false, ckRes.err)
    return 1

  var ckRows: seq[string] = @["Checkpoint completed at LSN " & $ckRes.value]
  if warnings or verbose:
    let walWarnings = takeWarnings(database.wal)
    for warn in walWarnings:
      ckRows.add("WARNING: " & warn)

  discard closeDb(database)
  echo resultJson(true, rows = ckRows)
  return 0

proc infoCmd*(db: string = ""): int =
  ## Display database information (format, size, cache, LSN)
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  let info = formatDbInfo(database)
  discard closeDb(database)
  echo resultJson(true, rows = info)
  return 0

proc statsCmd*(db: string = ""): int =
  ## Show basic engine statistics
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  let stats = formatStats(database)
  discard closeDb(database)
  echo resultJson(true, rows = stats)
  return 0

# ============================================================================
# REPL & Completion Commands
# ============================================================================

proc repl*(db: string = "", format: string = "table"): int =
  ## Interactive REPL mode
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  echo "DecentDb REPL (.exit to quit)"
  while true:
    stdout.write("> ")
    stdout.flushFile()
    var line: string
    if not stdin.readLine(line):
      break
    let trimmed = line.strip()
    if trimmed.len == 0:
      continue
    if trimmed == ".exit" or trimmed == ".quit":
      break
    if trimmed == ".help":
      echo "Enter SQL statements. Use .exit to quit."
      continue
    let execRes = execSql(database, trimmed, @[])
    if not execRes.ok:
      echo resultJson(false, execRes.err)
      continue
    emitRows(execRes.value, format)

  discard closeDb(database)
  return 0

proc completion*(shell: string = "bash"): int =
  ## Emit basic shell completion script
  let normalized = shell.strip().toLowerAscii()
  let commands = "exec list-tables describe list-indexes rebuild-index verify-index import export dump bulk-load checkpoint stats info dump-header verify-header repl completion"
  if normalized == "zsh":
    echo "#compdef decentdb"
    echo "_decentdb() {"
    echo "  local -a cmds"
    echo "  cmds=(" & commands & ")"
    echo "  _describe 'command' cmds"
    echo "}"
    echo "compdef _decentdb decentdb"
    return 0
  if normalized == "bash":
    echo "_decentdb() {"
    echo "  local cur=\"${COMP_WORDS[COMP_CWORD]}\""
    echo "  local cmds=\"" & commands & "\""
    echo "  COMPREPLY=( $(compgen -W \"$cmds\" -- \"$cur\") )"
    echo "}"
    echo "complete -F _decentdb decentdb"
    return 0
  echo resultJson(false, DbError(code: ERR_IO, message: "Unsupported shell for completion", context: shell))
  return 1

# ============================================================================
# Main Entry Point - Backward Compatible Mode
# ============================================================================
