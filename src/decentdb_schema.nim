## DecentDb Schema Tool - Utility for schema introspection and data import/export

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
# Schema Introspection Commands
# ============================================================================

proc listTables(db: string = ""): int =
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

proc describe(table: string, db: string = ""): int =
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

proc listIndexes(db: string = "", table: string = ""): int =
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

when isMainModule:
  import cligen
  
  dispatchMulti(
    ["multi", doc="DecentDb Schema Tool v" & Version],
    [listTables,
     cmdName = "list-tables",
     help = {
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd'
     }],
    [describe,
     cmdName = "describe",
     help = {
       "table": "Table name to describe",
       "db": "Path to database file (required)"
     },
     short = {
       "db": 'd',
       "table": 't'
     }],
    [listIndexes,
     cmdName = "list-indexes",
     help = {
       "db": "Path to database file (required)",
       "table": "Optional table name to filter indexes"
     },
     short = {
       "db": 'd',
       "table": 't'
     }]
  )
