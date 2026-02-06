import json
import options
import os
import strutils
import tables
import parsecsv
import streams
import times
import std/monotimes
import math
import atomics
import algorithm
import ./engine
import ./errors
import ./catalog/catalog
import ./pager/pager
import ./pager/db_header
import ./record/record
import ./storage/storage
import ./wal/wal
import ./vfs/os_vfs

# Helper to convert JSON to Result for consistent output format
proc resultJson(ok: bool, err: DbError = DbError(), rows: seq[string] = @[]): JsonNode =
  let errorNode = if ok: newJNull() else: errorToJson(err)
  %*{
    "ok": ok,
    "error": errorNode,
    "rows": rows
  }

proc roundMs*(ms: float64): float64 {.inline.} =
  ## Round milliseconds to 4 decimal places for stable, readable CLI output.
  round(ms * 10_000.0) / 10_000.0

type HeartbeatCtx = object
  stop: ptr Atomic[bool]
  startedAt: MonoTime
  everyMs: int
  label: string

proc heartbeatThread(ctx: ptr HeartbeatCtx) {.thread.} =
  ## Periodically prints a heartbeat to stderr while a long-running operation
  ## is in progress. Intended for CLI usability; output always goes to stderr
  ## so stdout remains machine-parseable (e.g. JSON).
  if ctx.isNil:
    return
  if ctx.everyMs <= 0:
    return
  while not ctx.stop[].load(moAcquire):
    sleep(ctx.everyMs)
    if ctx.stop[].load(moAcquire):
      break
    let now = getMonoTime()
    let elapsedNs = inNanoseconds(now - ctx.startedAt)
    let elapsedMs = roundMs(float64(elapsedNs) / 1_000_000.0)
    stderr.writeLine("Still running (" & ctx.label & ") elapsed_ms=" & $elapsedMs)
    flushFile(stderr)

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

proc formatSchemaSummary(database: Db): seq[string] =
  var tableNames: seq[string] = @[]
  for name, _ in database.catalog.tables:
    tableNames.add(name)
  tableNames.sort()

  var indexNames: seq[string] = @[]
  for name, _ in database.catalog.indexes:
    indexNames.add(name)
  indexNames.sort()

  var lines: seq[string] = @[]
  lines.add("Schema summary")
  lines.add("Tables: " & $tableNames.len)
  for tableName in tableNames:
    let t = database.catalog.tables[tableName]
    lines.add("Table: " & tableName)
    for col in t.columns:
      var colLine = "  " & col.name & " " & columnTypeToText(col.kind)
      if col.notNull:
        colLine &= " NOT NULL"
      if col.unique:
        colLine &= " UNIQUE"
      if col.primaryKey:
        colLine &= " PRIMARY KEY"
      if col.refTable.len > 0:
        colLine &= " REFERENCES " & col.refTable & "(" & col.refColumn & ")"
      lines.add(colLine)

  lines.add("Indexes: " & $indexNames.len)
  for indexName in indexNames:
    let idx = database.catalog.indexes[indexName]
    var idxLine = "Index: " & indexName & " ON " & idx.table & "(" & idx.columns.join(", ") & ")"
    if idx.unique:
      idxLine &= " UNIQUE"
    idxLine &= " " & (if idx.kind == ikBtree: "BTREE" else: "TRIGRAM")
    lines.add(idxLine)
  lines

proc collectInfoRows*(database: Db, schema_summary: bool): seq[string] =
  ## Used by `decentdb info` (and unit tests).
  var rows = formatDbInfo(database)
  if schema_summary:
    rows.add("")
    rows.add(formatSchemaSummary(database))
  rows

proc formatStats(database: Db): seq[string] =
  let cache = database.pager.cache
  @[
    "Page size: " & $database.pager.pageSize & " bytes",
    "Page count: " & $database.pager.pageCount,
    "Cache capacity: " & $cache.capacity & " pages",
    "Cache loaded: " & $cacheLoadedCount(cache) & " pages",
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
  of ctDecimal:
    try:
      Value(kind: vkInt64, int64Val: parseBiggestInt(cellValue))
    except:
      Value(kind: vkNull)
  of ctUuid:
    var bytes: seq[byte] = @[]
    for ch in cellValue:
      bytes.add(byte(ch))
    Value(kind: vkText, bytes: bytes)

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
  of ctDecimal:
    if node.kind == JInt:
      return Value(kind: vkInt64, int64Val: node.getBiggestInt())
    if node.kind == JString:
      try:
        return Value(kind: vkInt64, int64Val: parseBiggestInt(node.getStr()))
      except:
        return Value(kind: vkNull)
    Value(kind: vkNull)
  of ctUuid:
    if node.kind == JString:
      var bytes: seq[byte] = @[]
      for ch in node.getStr():
        bytes.add(byte(ch))
      return Value(kind: vkText, bytes: bytes)
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
              cachePages: int = 1024, cacheMb: int = 0, checkpoint: bool = false,
              readerCount: bool = false, longReaders: int = 0, dbInfo: bool = false,
              warnings: bool = false, verbose: bool = false,
              checkpointBytes: int = 0, checkpointMs: int = 0,
              readerWarnMs: int = 0, readerTimeoutMs: int = 0, forceTruncateOnTimeout: bool = false,
              heartbeatMs: int = 0,
              format: string = "json", noRows: bool = false, params: seq[string] = @[],
              walFailpoints: seq[string] = @[], clearWalFailpoints: bool = false): int =
  ## Execute SQL statements against a DecentDb database file.
  ## Output can be rendered as json/csv/table depending on --format.
  ## (Some diagnostic modes like timing/warnings/verbose currently require json.)

  let cmdStart = getMonoTime()
  
  let cfg = loadConfig()
  let dbPath = applyDbConfig(db, cfg)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  # Calculate cache size (cacheMb takes precedence if specified)
  let configCacheMb = parseConfigInt(cfg, "cacheMb")
  let configCachePages = parseConfigInt(cfg, "cachePages")
  let resolvedCacheMb = if cacheMb > 0: cacheMb else: configCacheMb
  let resolvedCachePages = if cachePages != 1024 or resolvedCacheMb > 0: cachePages else: configCachePages
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
  if checkpointBytes > 0 or checkpointMs > 0 or readerWarnMs > 0 or readerTimeoutMs > 0 or forceTruncateOnTimeout:
    setCheckpointConfig(database.wal, int64(checkpointBytes), int64(checkpointMs),
      readerWarnMs = int64(readerWarnMs),
      readerTimeoutMs = int64(readerTimeoutMs),
      forceTruncateOnTimeout = forceTruncateOnTimeout)
  
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
      info.add("Reader " & $reader.id & " snapshot " & $reader.snapshot & " age: " & $reader.ageMs & "ms")
    discard closeDb(database)
    echo resultJson(true, rows = info)
    return 0
  
  # Handle database info request
  if dbInfo:
    let info = formatDbInfo(database)
    discard closeDb(database)
    echo resultJson(true, rows = info)
    return 0
  
  # Handle checkpoint-only request (no SQL to execute)
  if checkpoint and sql.len == 0:
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
    let execStart = getMonoTime()
    var parsedParams: seq[Value] = @[]
    for param in params:
      let valueRes = parseParamValue(param)
      if not valueRes.ok:
        discard closeDb(database)
        let execEnd = getMonoTime()
        let elapsedNs = inNanoseconds(execEnd - execStart)
        let elapsedMs = roundMs(float64(elapsedNs) / 1_000_000.0)
        var payload = resultJson(false, valueRes.err)
        if format.strip().toLowerAscii() == "json":
          payload["elapsed_ms"] = %elapsedMs
        echo payload
        return 1
      parsedParams.add(valueRes.value)

    let queryStart = getMonoTime()
    let normalizedFormat = format.strip().toLowerAscii()
    if noRows and normalizedFormat != "json":
      discard closeDb(database)
      echo resultJson(false, DbError(code: ERR_IO, message: "--noRows requires --format=json"))
      return 1

    var rows: seq[string] = @[]
    var rowsReturned: int64 = 0

    var execOk = true
    var execErr = DbError()

    var hbStop: Atomic[bool]
    var hbCtx = HeartbeatCtx()
    var hbThread: Thread[ptr HeartbeatCtx]
    var hbStarted = false
    if heartbeatMs > 0:
      hbStop.store(false, moRelaxed)
      hbCtx.stop = addr hbStop
      hbCtx.startedAt = queryStart
      hbCtx.everyMs = heartbeatMs
      hbCtx.label = "sql"
      createThread(hbThread, heartbeatThread, addr hbCtx)
      hbStarted = true

    try:
      if noRows:
        let res = execSqlNoRows(database, sql, parsedParams)
        if not res.ok:
          execOk = false
          execErr = res.err
        else:
          rowsReturned = res.value
      else:
        let res = execSql(database, sql, parsedParams)
        if not res.ok:
          execOk = false
          execErr = res.err
        else:
          rows = res.value
    finally:
      if hbStarted:
        hbStop.store(true, moRelease)
        joinThread(hbThread)

    let queryEnd = getMonoTime()
    let execEnd = getMonoTime()
    let elapsedNs = inNanoseconds(execEnd - execStart)

    let queryNs = inNanoseconds(queryEnd - queryStart)
    let elapsedMs = roundMs(float64(elapsedNs) / 1_000_000.0)
    let queryMs = roundMs(float64(queryNs) / 1_000_000.0)
    
    if not execOk:
      discard closeDb(database)
      var payload = resultJson(false, execErr)
      if normalizedFormat == "json":
        payload["elapsed_ms"] = %elapsedMs
      echo payload
      return 1
    
    # Collect warnings if requested
    var walWarnings: seq[string] = @[]
    if warnings or verbose:
      walWarnings = takeWarnings(database.wal)

    # Handle checkpoint after SQL execution if requested
    var checkpointLsn: uint64 = 0
    var checkpointErr: DbError
    var checkpointOk = true
    if checkpoint:
      let ckRes = checkpointDb(database)
      if not ckRes.ok:
        checkpointOk = false
        checkpointErr = ckRes.err
      else:
        checkpointLsn = ckRes.value
        # Add checkpoint warnings to existing warnings
        if warnings or verbose:
          let ckWarnings = takeWarnings(database.wal)
          for warn in ckWarnings:
            walWarnings.add("CHECKPOINT: " & warn)

    discard closeDb(database)

    # Always include timing for exec in JSON output.
    if normalizedFormat == "json":
      var resultPayload = resultJson(true, rows = rows)
      resultPayload["elapsed_ms"] = %elapsedMs
      if noRows:
        resultPayload["rows_returned"] = %rowsReturned

      # Preserve existing detailed timing output behind --timing.
      if timing:
        let cmdEnd = getMonoTime()
        let totalNs = inNanoseconds(cmdEnd - cmdStart)
        let totalMs = roundMs(float64(totalNs) / 1_000_000.0)
        let timingInfo = %*{
          "total_ms": float64(totalMs),
          "query_ms": float64(queryMs),
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

      # Include checkpoint info if checkpoint was requested
      if checkpoint:
        if checkpointOk:
          resultPayload["checkpoint_lsn"] = %checkpointLsn
        else:
          resultPayload["checkpoint_error"] = %checkpointErr.message

      echo resultPayload
    else:
      # Keep existing behavior for non-JSON formats.
      # (Timing is still available via JSON output.)
      if timing or warnings or verbose:
        echo resultJson(false, DbError(code: ERR_IO, message: "Non-JSON format not supported with timing/warnings/verbose"))
      else:
        emitRows(rows, format)
        # Print checkpoint info for non-JSON output if requested
        if checkpoint:
          if checkpointOk:
            echo "Checkpoint completed at LSN " & $checkpointLsn
          else:
            echo "Checkpoint failed: " & checkpointErr.message
    # Return error if checkpoint failed
    if checkpoint and not checkpointOk:
      return 1
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
      of ctDecimal: "DECIMAL"
      of ctUuid: "UUID"
    
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
    output.add("$1|$2|$3|$4|$5" % [indexName, indexMeta.table, indexMeta.columns.join(","), indexType, unique])
  
  discard closeDb(database)
  echo resultJson(true, rows = output)
  return 0

proc rebuildAllIndexes*(database: Db, table: string = ""): Result[seq[string]] =
  var indexNames: seq[string] = @[]
  for indexName, indexMeta in database.catalog.indexes:
    if table.len > 0 and indexMeta.table != table:
      continue
    indexNames.add(indexName)
  indexNames.sort()

  var rows: seq[string] = @[]
  for indexName in indexNames:
    let indexMeta = database.catalog.indexes[indexName]
    let rebuildRes = storage.rebuildIndex(database.pager, database.catalog, indexMeta)
    if not rebuildRes.ok:
      return err[seq[string]](rebuildRes.err.code, rebuildRes.err.message, rebuildRes.err.context)
    rows.add("Index '" & indexName & "' rebuilt successfully")
  ok(rows)

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

proc cmdRebuildIndexes*(db: string = "", table: string = ""): int =
  ## Rebuild all indexes in the database
  let dbPath = resolveDbPath(db)
  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  let rebuildRes = rebuildAllIndexes(database, table)
  if not rebuildRes.ok:
    discard closeDb(database)
    echo resultJson(false, rebuildRes.err)
    return 1

  discard closeDb(database)
  echo resultJson(true, rows = rebuildRes.value)
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
  info.add("Columns: " & indexMeta.columns.join(", "))
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
        of ctDecimal: "DECIMAL"
        of ctUuid: "UUID"
      
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

proc infoCmd*(db: string = "", schema_summary: bool = false): int =
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
  let info = collectInfoRows(database, schema_summary)
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

proc vacuumCmd*(db: string = "", output: string = "", overwrite: bool = false, cachePages: int = 1024, cacheMb: int = 0): int =
  ## Rewrite the database into a new file to reclaim space (VACUUM).
  let srcPath = resolveDbPath(db)
  if srcPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  if output.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --output argument"))
    return 1
  if expandTilde(output) == expandTilde(srcPath):
    echo resultJson(false, DbError(code: ERR_IO, message: "--output must be different from --db"))
    return 1

  let resolvedCacheMb = cacheMb
  let actualCachePages = if resolvedCacheMb > 0:
    (resolvedCacheMb * 1024 * 1024) div 4096
  else:
    cachePages

  let outPath = expandTilde(output)
  if fileExists(outPath) or fileExists(outPath & "-wal"):
    if not overwrite:
      echo resultJson(false, DbError(code: ERR_IO, message: "Output file exists (use --overwrite)", context: outPath))
      return 1
    if fileExists(outPath & "-wal"):
      try: removeFile(outPath & "-wal")
      except: discard
    if fileExists(outPath):
      try: removeFile(outPath)
      except: discard

  let srcRes = openDb(srcPath, cachePages = actualCachePages)
  if not srcRes.ok:
    echo resultJson(false, srcRes.err)
    return 1
  let srcDb = srcRes.value

  let dstRes = openDb(outPath, cachePages = actualCachePages)
  if not dstRes.ok:
    discard closeDb(srcDb)
    echo resultJson(false, dstRes.err)
    return 1
  let dstDb = dstRes.value

  # Install a flush handler to ensure evicted dirty pages go to WAL, not DB file.
  # IMPORTANT: Do not fsync/commit per evicted page (too slow under small caches).
  # Instead, append evicted pages into dstDb.activeWriter and commit in batches.
  if dstDb.wal != nil:
    let writerRes = beginWrite(dstDb.wal)
    if not writerRes.ok:
      discard closeDb(srcDb)
      discard closeDb(dstDb)
      echo resultJson(false, writerRes.err)
      return 1
    dstDb.activeWriter = writerRes.value

  dstDb.pager.flushHandler = proc(pageId: PageId, data: string): Result[Void] =
    if dstDb.wal == nil:
      return okVoid()
    if dstDb.activeWriter == nil:
      return err[Void](ERR_TRANSACTION, "VACUUM flush requires active WAL writer")

    var bytes = newSeq[byte](data.len)
    if data.len > 0:
      copyMem(addr bytes[0], unsafeAddr data[0], data.len)

    let writeRes = writePage(dstDb.activeWriter, pageId, bytes)
    if not writeRes.ok:
      return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
    okVoid()

  if dstDb.catalog.tables.len != 0 or dstDb.catalog.indexes.len != 0:
    discard closeDb(srcDb)
    discard closeDb(dstDb)
    echo resultJson(false, DbError(code: ERR_IO, message: "Output database is not empty", context: outPath))
    return 1

  proc commitDirtyToWal(db: Db): Result[Void] =
    ## Commit dirty pages to WAL in a single batch.
    ## Also rotates dstDb.activeWriter so the flush handler can keep appending
    ## pages without forcing per-page fsync.
    if db.wal == nil:
      return okVoid()
    if db.activeWriter == nil:
      return err[Void](ERR_TRANSACTION, "VACUUM requires active WAL writer")

    let dirtyPages = snapshotDirtyPages(db.pager)
    var pageIds: seq[PageId] = @[]
    for entry in dirtyPages:
      var bytes = newSeq[byte](entry[1].len)
      if entry[1].len > 0:
        copyMem(addr bytes[0], unsafeAddr entry[1][0], entry[1].len)
      let writeRes = writePage(db.activeWriter, entry[0], bytes)
      if not writeRes.ok:
        discard rollback(db.activeWriter)
        db.activeWriter = nil
        clearCache(db.pager)
        return err[Void](writeRes.err.code, writeRes.err.message, writeRes.err.context)
      pageIds.add(entry[0])

    let commitRes = commit(db.activeWriter)
    if not commitRes.ok:
      db.activeWriter = nil
      clearCache(db.pager)
      return err[Void](commitRes.err.code, commitRes.err.message, commitRes.err.context)

    if pageIds.len > 0:
      markPagesCommitted(db.pager, pageIds, commitRes.value)

    # Start a new writer so the flush handler can continue enqueueing pages.
    let nextWriterRes = beginWrite(db.wal)
    if not nextWriterRes.ok:
      db.activeWriter = nil
      return err[Void](nextWriterRes.err.code, nextWriterRes.err.message, nextWriterRes.err.context)
    db.activeWriter = nextWriterRes.value
    okVoid()

  # Toposort tables by inline FK dependencies.
  var deps = initTable[string, seq[string]]()
  for tableName, tableMeta in srcDb.catalog.tables:
    var refs: seq[string] = @[]
    for col in tableMeta.columns:
      if col.refTable.len > 0:
        refs.add(col.refTable)
    deps[tableName] = refs

  var ordered: seq[string] = @[]
  var perm = initTable[string, bool]()
  var temp = initTable[string, bool]()

  proc visit(name: string): Result[Void] =
    if perm.getOrDefault(name, false):
      return okVoid()
    if temp.getOrDefault(name, false):
      return err[Void](ERR_SQL, "Cycle in foreign key dependencies", name)
    temp[name] = true
    for dep in deps.getOrDefault(name, @[]):
      if deps.hasKey(dep):
        let vRes = visit(dep)
        if not vRes.ok:
          return vRes
    temp[name] = false
    perm[name] = true
    ordered.add(name)
    okVoid()

  for tableName, _ in srcDb.catalog.tables:
    let vRes = visit(tableName)
    if not vRes.ok:
      discard closeDb(srcDb)
      discard closeDb(dstDb)
      echo resultJson(false, vRes.err)
      return 1

  # Create tables (with constraints) in dependency order.
  for tableName in ordered:
    let tableMeta = srcDb.catalog.tables[tableName]
    var createStmt = "CREATE TABLE " & tableName & " (\n"
    var columnDefs: seq[string] = @[]
    for col in tableMeta.columns:
      var colDef = "  " & col.name & " " & columnTypeToText(col.kind)
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

    let createRes = execSql(dstDb, createStmt)
    if not createRes.ok:
      discard closeDb(srcDb)
      discard closeDb(dstDb)
      echo resultJson(false, createRes.err)
      return 1

  # Copy data with index updates disabled, then rebuild constraint-created indexes.
  var totalRows = 0'i64
  let commitEvery = max(64, actualCachePages div 4)
  var sinceCommit = 0
  for tableName in ordered:
    let dstTableName = tableName
    let tableMeta = srcDb.catalog.tables[tableName]
    let scanRes = scanTableEach(srcDb.pager, tableMeta, proc(row: StoredRow): Result[Void] =
      let insRes = insertRowNoIndexes(dstDb.pager, dstDb.catalog, dstTableName, row.values)
      if not insRes.ok:
        return err[Void](insRes.err.code, insRes.err.message, insRes.err.context)
      totalRows.inc
      sinceCommit.inc
      if sinceCommit >= commitEvery:
        let cRes = commitDirtyToWal(dstDb)
        if not cRes.ok:
          return cRes
        sinceCommit = 0
      okVoid()
    )
    if not scanRes.ok:
      discard closeDb(srcDb)
      discard closeDb(dstDb)
      echo resultJson(false, scanRes.err)
      return 1

    # Commit at table boundaries to keep the cache from spilling dirty pages.
    let cRes = commitDirtyToWal(dstDb)
    if not cRes.ok:
      discard closeDb(srcDb)
      discard closeDb(dstDb)
      echo resultJson(false, cRes.err)
      return 1
    sinceCommit = 0

  for _, idx in dstDb.catalog.indexes:
    let rebuildRes = storage.rebuildIndex(dstDb.pager, dstDb.catalog, idx)
    if not rebuildRes.ok:
      discard closeDb(srcDb)
      discard closeDb(dstDb)
      echo resultJson(false, rebuildRes.err)
      return 1

  # Recreate any additional indexes present in the source but not auto-created by constraints.
  var createdIndexes = 0
  for idxName, idx in srcDb.catalog.indexes:
    if dstDb.catalog.indexes.hasKey(idxName):
      continue

    # Semantic dedupe: if destination already has an equivalent index under a different
    # name, do not recreate it.
    if idx.columns.len == 1 and isSome(dstDb.catalog.getIndexForColumn(idx.table, idx.columns[0], idx.kind, requireUnique = idx.unique)):
      continue

    var stmt = "CREATE "
    if idx.unique:
      stmt &= "UNIQUE "
    stmt &= "INDEX " & idx.name & " ON " & idx.table
    if idx.kind == ikTrigram:
      stmt &= " USING trigram "
    stmt &= "(" & idx.columns.join(", ") & ")"
    let idxRes = execSql(dstDb, stmt)
    if not idxRes.ok:
      discard closeDb(srcDb)
      discard closeDb(dstDb)
      echo resultJson(false, idxRes.err)
      return 1
    createdIndexes.inc

  # Ensure any remaining dirty pages (table/index/catalog) are committed before checkpoint.
  let cRes = commitDirtyToWal(dstDb)
  if not cRes.ok:
    discard closeDb(srcDb)
    discard closeDb(dstDb)
    echo resultJson(false, cRes.err)
    return 1

  # Finish the final writer batch before checkpoint/close.
  if dstDb.wal != nil and dstDb.activeWriter != nil:
    let finalCommitRes = commit(dstDb.activeWriter)
    dstDb.activeWriter = nil
    if not finalCommitRes.ok:
      discard closeDb(srcDb)
      discard closeDb(dstDb)
      echo resultJson(false, finalCommitRes.err)
      return 1
  dstDb.pager.flushHandler = nil

  # Checkpoint destination to truncate WAL and persist pages.
  let ckptRes = checkpointDb(dstDb)
  if not ckptRes.ok:
    echo "Checkpoint failed: ", ckptRes.err.message, " ", ckptRes.err.context

  let srcPages = srcDb.pager.pageCount
  let dstPages = dstDb.pager.pageCount
  discard closeDb(srcDb)
  discard closeDb(dstDb)

  echo resultJson(true, rows = @[
    "Vacuum complete",
    "Rows copied: " & $totalRows,
    "Extra indexes created: " & $createdIndexes,
    "Source pages: " & $srcPages,
    "Destination pages: " & $dstPages
  ])
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
  let commands = "exec list-tables describe list-indexes rebuild-index rebuild-indexes verify-index import export dump bulk-load checkpoint stats info vacuum dump-header verify-header repl completion"
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
