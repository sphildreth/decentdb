# Nim API Reference

DecentDB's native Nim API for embedded applications.

## Opening a Database

```nim
import decentdb/engine

# Open with default cache (1024 pages = 4MB)
let db = openDb("myapp.ddb")
if not db.ok:
  echo "Failed to open: ", db.err.message
  quit(1)

# Open with custom cache size
let db2 = openDb("myapp.ddb", cachePages = 4096)  # 16MB cache
```

## Executing SQL

### Basic Queries

```nim
import decentdb/engine
import decentdb/record/record

let db = openDb("myapp.ddb").value

# Execute DDL
let createRes = execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
if not createRes.ok:
  echo "Error: ", createRes.err.message

# Insert data
let insertRes = execSql(db, "INSERT INTO users VALUES (1, 'Alice')")
if insertRes.ok:
  echo "Inserted, rows affected: ", insertRes.value.len
```

### With Parameters

```nim
# Use positional parameters $1, $2, etc.
let params = @[
  Value(kind: vkInt64, int64Val: 2),
  Value(kind: vkText, bytes: toBytes("Bob"))
]

let res = execSql(db, "INSERT INTO users VALUES ($1, $2)", params)
```

### Query Results

```nim
# SELECT returns rows as strings
let selectRes = execSql(db, "SELECT * FROM users")
if selectRes.ok:
  for row in selectRes.value:
    echo row  # "1|Alice"
```

## Transactions

### Manual Transaction Control

```nim
# Begin transaction
let beginRes = execSql(db, "BEGIN")
if not beginRes.ok:
  echo "Failed to begin transaction"

# Execute operations
let res1 = execSql(db, "INSERT INTO users VALUES (3, 'Carol')")
let res2 = execSql(db, "INSERT INTO users VALUES (4, 'Dave')")

# Commit or rollback
if res1.ok and res2.ok:
  discard execSql(db, "COMMIT")
  echo "Transaction committed"
else:
  discard execSql(db, "ROLLBACK")
  echo "Transaction rolled back"
```

## Bulk Loading

For high-performance data import:

```nim
import decentdb/engine

let db = openDb("myapp.ddb").value

# Create table
discard execSql(db, "CREATE TABLE bulk_data (id INT PRIMARY KEY, data TEXT)")

# Prepare data
var rows: seq[seq[Value]] = @[]
for i in 1 .. 10000:
  rows.add(@[
    Value(kind: vkInt64, int64Val: i),
    Value(kind: vkText, bytes: toBytes("data_" & $i))
  ])

# Configure bulk load
var opts = defaultBulkLoadOptions()
opts.disableIndexes = true        # Faster: rebuild indexes after
opts.durability = dmDeferred      # Batch fsync operations
opts.batchSize = 10000
opts.checkpointOnComplete = true  # Checkpoint when done

# Load data
let bulkRes = bulkLoad(db, "bulk_data", rows, opts)
if bulkRes.ok:
  echo "Loaded ", rows.len, " rows"
else:
  echo "Bulk load failed: ", bulkRes.err.message
```

## Working with Values

### Creating Values

```nim
import decentdb/record/record

# NULL
let nullVal = Value(kind: vkNull)

# Integer
let intVal = Value(kind: vkInt64, int64Val: 42)

# Float
let floatVal = Value(kind: vkFloat64, float64Val: 3.14)

# Boolean
let boolVal = Value(kind: vkBool, boolVal: true)

# Text
proc toBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i, c in s:
    result[i] = byte(c)

let textVal = Value(kind: vkText, bytes: toBytes("Hello"))

# Blob
let blobVal = Value(kind: vkBlob, bytes: toBytes("\x00\x01\x02"))
```

### Converting Values to Strings

```nim
proc valueToString(v: Value): string =
  case v.kind
  of vkNull: "NULL"
  of vkInt64: $v.int64Val
  of vkFloat64: $v.float64Val
  of vkBool: if v.boolVal: "true" else: "false"
  of vkText, vkBlob:
    result = ""
    for b in v.bytes:
      result.add(char(b))
```

## Error Handling

All operations return a `Result[T]` type:

```nim
type Result[T] = object
  ok: bool
  value: T          # Only valid if ok == true
  err: DbError      # Only valid if ok == false

type DbError = object
  code: ErrorCode
  message: string
  context: string
```

### Checking Results

```nim
let res = execSql(db, "SELECT * FROM nonexistent")
if not res.ok:
  case res.err.code
  of ERR_SQL:
    echo "SQL error: ", res.err.message
  of ERR_IO:
    echo "IO error: ", res.err.message
  of ERR_CORRUPTION:
    echo "Database corruption detected!"
  else:
    echo "Error: ", res.err.message
```

## Database Information

### Database Properties

```nim
let db = openDb("myapp.ddb").value

echo "Path: ", db.path
echo "Page size: ", db.pageSize, " bytes"
echo "Format version: ", db.formatVersion
echo "Cache pages: ", db.cachePages
echo "Schema cookie: ", db.schemaCookie
```

### Getting Table Information

```nim
let tableRes = db.catalog.getTable("users")
if tableRes.ok:
  let table = tableRes.value
  echo "Table: ", table.name
  echo "Root page: ", table.rootPage
  for col in table.columns:
    echo "  Column: ", col.name, " (", $col.kind, ")"
```

## Closing the Database

Always close the database when done:

```nim
let closeRes = closeDb(db)
if not closeRes.ok:
  echo "Error closing: ", closeRes.err.message
```

## Complete Example

```nim
import decentdb/engine
import decentdb/record/record
import os

proc main() =
  let dbPath = getTempDir() / "example.ddb"
  
  # Open database
  let dbRes = openDb(dbPath)
  if not dbRes.ok:
    echo "Failed to open: ", dbRes.err.message
    return
  let db = dbRes.value
  
  # Create table
  let createRes = execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
  if not createRes.ok:
    echo "Create failed: ", createRes.err.message
    discard closeDb(db)
    return
  
  # Insert data
  let names = ["Alice", "Bob", "Carol"]
  for i, name in names:
    let params = @[
      Value(kind: vkInt64, int64Val: int64(i + 1)),
      Value(kind: vkText, bytes: toBytes(name))
    ]
    let insertRes = execSql(db, "INSERT INTO users VALUES ($1, $2)", params)
    if not insertRes.ok:
      echo "Insert failed: ", insertRes.err.message
  
  # Query data
  let selectRes = execSql(db, "SELECT * FROM users ORDER BY id")
  if selectRes.ok:
    echo "Users:"
    for row in selectRes.value:
      echo "  ", row
  
  # Cleanup
  discard closeDb(db)
  removeFile(dbPath)

main()
```

## Advanced Topics

### Direct Pager Access

For low-level operations:

```nim
import decentdb/pager/pager

# Read a page directly
let pageRes = db.pager.readPage(1)
if pageRes.ok:
  let page = pageRes.value
  # Work with raw page bytes
```

### Custom VFS

Implement your own virtual file system:

```nim
import decentdb/vfs/types

type MyVfs = ref object of Vfs
  # Custom implementation

method open*(vfs: MyVfs, path: string, mode: FileMode, create: bool): Result[VfsFile] =
  # Custom open implementation
  ...
```

See the [VFS module](../architecture/storage.md) for details.

## Thread Safety

- **Single writer**: Only one thread can write at a time
- **Multiple readers**: Many threads can read concurrently
- Use proper synchronization in your application

## Performance Tips

1. Reuse database connections
2. Use bulk load for large imports
3. Prepare/cache frequently used queries
4. Use appropriate cache size
5. Create indexes for frequent queries

## API Stability

DecentDB is currently pre-1.0 (starting at 0.0.1). Until 1.0.0, APIs may change, including breaking changes.
Once 1.0.0 is released, we will follow Semantic Versioning for API compatibility.
