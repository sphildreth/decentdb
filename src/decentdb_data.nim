## DecentDb Data Tool - Import/Export utilities

import json
import strutils
import tables
import parsecsv
import streams
import ./engine
import ./errors
import ./catalog/catalog
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

when isMainModule:
  import cligen
  
  dispatchMulti(
    ["multi", doc="DecentDb Data Tool v" & Version],
    [importCsv,
     cmdName = "import",
     help = {
       "table": "Table name to import into",
       "csvFile": "CSV file path",
       "db": "Path to database file (required)",
       "batchSize": "Number of rows per batch (default: 10000)"
     },
     short = {
       "db": 'd',
       "table": 't'
     }],
    [exportCsv,
     cmdName = "export",
     help = {
       "table": "Table name to export",
       "csvFile": "Output CSV file path",
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd',
       "table": 't'
     }],
    [dumpSql,
     cmdName = "dump",
     help = {
       "db": "Path to database file (required)",
       "output": "Output SQL file path (optional, defaults to stdout)"
     },
     short = {
       "db": 'd',
       "output": 'o'
     }]
  )
