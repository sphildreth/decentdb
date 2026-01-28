import json
import strutils
import tables
import parsecsv
import streams
import times
import ./engine
import ./errors
import ./catalog/catalog
import ./pager/pager
import ./record/record
import ./storage/storage

const Version = "0.0.1"

# Helper to convert JSON to Result for consistent output format
proc resultJson(ok: bool, err: DbError = DbError(), rows: seq[string] = @[]): JsonNode =
  let errorNode = if ok: newJNull() else: errorToJson(err)
  %*{
    "ok": ok,
    "error": errorNode,
    "rows": rows
  }

# ============================================================================
# Main SQL Execution Command
# ============================================================================

proc cliMain(db: string = "", sql: string = "", openClose: bool = false, timing: bool = false): int =
  ## DecentDb CLI v0.0.1 - ACID-first embedded relational database
  ## 
  ## Execute SQL statements against a DecentDb database file.
  ## All output is JSON formatted for programmatic use.
  
  let startTime = if timing: epochTime() else: 0.0
  
  if db.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(db)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  
  if not openClose and sql.len > 0:
    let queryStart = if timing: epochTime() else: 0.0
    let execRes = execSql(database, sql)
    let queryEnd = if timing: epochTime() else: 0.0
    
    if not execRes.ok:
      discard closeDb(database)
      echo resultJson(false, execRes.err)
      return 1
    
    let rows = execRes.value
    discard closeDb(database)
    
    # Add timing info to JSON output if requested
    if timing:
      let totalTime = (epochTime() - startTime) * 1000.0  # Convert to ms
      let queryTime = (queryEnd - queryStart) * 1000.0
      let timingInfo = %*{
        "total_ms": totalTime,
        "query_ms": queryTime
      }
      var result = resultJson(true, rows = rows)
      result["timing"] = timingInfo
      echo result
    else:
      echo resultJson(true, rows = rows)
    return 0

  discard closeDb(database)
  echo resultJson(true)
  return 0

# ============================================================================
# Schema Introspection Commands
# ============================================================================

proc schemaListTables(db: string = ""): int =
  ## List all tables in the database
  if db.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(db)
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

proc schemaDescribe(table: string, db: string = ""): int =
  ## Show table structure (columns, types, constraints)
  if db.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  
  if table.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing table name argument"))
    return 1

  let openRes = openDb(db)
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

proc schemaListIndexes(db: string = "", table: string = ""): int =
  ## List all indexes, optionally filtered by table
  if db.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(db)
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
# Import/Export Commands
# ============================================================================

proc importCsv(table: string, csvFile: string, db: string = "", batchSize: int = 10000): int =
  ## Import data from CSV file into a table
  if db.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  
  if table.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing table name"))
    return 1
  
  if csvFile.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing CSV file path"))
    return 1

  let openRes = openDb(db)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  
  if not database.catalog.tables.hasKey(table):
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_SQL, message: "Table not found", context: table))
    return 1
  
  let tableMeta = database.catalog.tables[table]
  
  var parser: CsvParser
  try:
    var s = newFileStream(csvFile, fmRead)
    if s == nil:
      discard closeDb(database)
      echo resultJson(false, DbError(code: ERR_IO, message: "Cannot open CSV file", context: csvFile))
      return 1
    
    parser.open(s, csvFile)
    parser.readHeaderRow()
    
    var rowCount = 0
    var batch: seq[seq[Value]] = @[]
    
    while parser.readRow():
      if parser.headers.len != tableMeta.columns.len:
        parser.close()
        discard closeDb(database)
        echo resultJson(false, DbError(code: ERR_IO, message: "CSV column count mismatch", context: $parser.headers.len & " vs " & $tableMeta.columns.len))
        return 1
      
      var values: seq[Value] = @[]
      for i, col in tableMeta.columns:
        let cellValue = parser.row[i]
        
        # Parse value according to column type
        let value = if cellValue.len == 0:
          Value(kind: vkNull)
        else:
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
            # For now, treat blob as text encoding
            var bytes: seq[byte] = @[]
            for ch in cellValue:
              bytes.add(byte(ch))
            Value(kind: vkBlob, bytes: bytes)
        
        values.add(value)
      
      batch.add(values)
      rowCount.inc
      
      # Insert batch when it reaches batchSize
      if batch.len >= batchSize:
        for row in batch:
          let insertRes = insertRow(database.pager, database.catalog, table, row)
          if not insertRes.ok:
            parser.close()
            discard closeDb(database)
            echo resultJson(false, insertRes.err)
            return 1
        batch = @[]
    
    # Insert remaining batch
    if batch.len > 0:
      for row in batch:
        let insertRes = insertRow(database.pager, database.catalog, table, row)
        if not insertRes.ok:
          parser.close()
          discard closeDb(database)
          echo resultJson(false, insertRes.err)
          return 1
    
    parser.close()
    discard closeDb(database)
    echo resultJson(true, rows = @["Imported " & $rowCount & " rows"])
    return 0
    
  except:
    let msg = getCurrentExceptionMsg()
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_IO, message: "CSV parsing error", context: msg))
    return 1

proc exportCsv(table: string, csvFile: string, db: string = ""): int =
  ## Export table data to CSV file
  if db.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1
  
  if table.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing table name"))
    return 1
  
  if csvFile.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing CSV file path"))
    return 1

  let openRes = openDb(db)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    return 1

  let database = openRes.value
  
  if not database.catalog.tables.hasKey(table):
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_SQL, message: "Table not found", context: table))
    return 1
  
  let tableMeta = database.catalog.tables[table]
  
  try:
    var f = open(csvFile, fmWrite)
    
    # Write header
    var headers: seq[string] = @[]
    for col in tableMeta.columns:
      headers.add(col.name)
    f.writeLine(headers.join(","))
    
    # Read all rows and export
    let rowsRes = scanTable(database.pager, tableMeta)
    if not rowsRes.ok:
      f.close()
      discard closeDb(database)
      echo resultJson(false, rowsRes.err)
      return 1
    
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
        
        # Escape commas and quotes in CSV
        let escaped = if "," in fieldValue or "\"" in fieldValue:
          "\"" & fieldValue.replace("\"", "\"\"") & "\""
        else:
          fieldValue
        
        fields.add(escaped)
      
      f.writeLine(fields.join(","))
      rowCount.inc
    
    f.close()
    discard closeDb(database)
    echo resultJson(true, rows = @["Exported " & $rowCount & " rows"])
    return 0
    
  except:
    let msg = getCurrentExceptionMsg()
    discard closeDb(database)
    echo resultJson(false, DbError(code: ERR_IO, message: "File write error", context: msg))
    return 1

proc dumpSql(db: string = "", output: string = ""): int =
  ## Dump entire database as SQL statements
  if db.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    return 1

  let openRes = openDb(db)
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
# Main Entry Point - Backward Compatible Mode
# ============================================================================

when isMainModule:
  import cligen
  
  # For now, use simple dispatch for backward compatibility with test harness
  # The subcommand functions (schemaListTables, etc.) remain available for
  # future migration to dispatchMulti when test harness is updated
  
  dispatch cliMain,
    help = {
      "db": "Path to database file (required)",
      "sql": "SQL statement to execute",
      "openClose": "Open and close database without executing SQL (testing mode)",
      "timing": "Show query execution timing in milliseconds"
    },
    short = {
      "db": 'd',
      "sql": 's',
      "timing": 't'
    }
