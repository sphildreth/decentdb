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

### In-Memory Databases

Use `:memory:` to create an ephemeral, isolated in-memory database — ideal for caching, testing, and temporary workloads:

```nim
let db = openDb(":memory:").value

# Full SQL support (DDL, DML, indexes, transactions)
discard execSql(db, "CREATE TABLE cache (key TEXT PRIMARY KEY, val TEXT)")
discard execSql(db, "INSERT INTO cache (key, val) VALUES ('k1', 'hello')")

# Data is lost when the database is closed
discard closeDb(db)
```

Each call to `openDb(":memory:")` creates a completely independent database instance. Detection is case-insensitive (`:memory:`, `:MEMORY:`, `:Memory:` all work).

### SaveAs (Export to Disk)

Export any open database — including `:memory:` — to a new on-disk file:

```nim
let db = openDb(":memory:").value
discard execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
discard execSql(db, "INSERT INTO items (id, name) VALUES (1, 'widget')")

# Save snapshot to disk
let res = saveAs(db, "/tmp/backup.ddb")
if not res.ok:
  echo "SaveAs failed: ", res.err.message

discard closeDb(db)
```

`saveAs` performs a full WAL checkpoint, then streams pages to the destination file atomically (temp file + rename + fsync). The destination must not already exist.

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

# Insert data (id auto-assigned when omitted)
let insertRes = execSql(db, "INSERT INTO users (name) VALUES ('Alice')")
if insertRes.ok:
  echo "Inserted, rows affected: ", insertRes.value.len

# Insert with explicit id
let insertRes2 = execSql(db, "INSERT INTO users VALUES (10, 'Bob')")
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

### INSERT RETURNING

Use `INSERT ... RETURNING` to get auto-assigned values back:

```nim
let res = execSql(db, "INSERT INTO users (name) VALUES ('Alice') RETURNING id, name")
if res.ok:
  echo res.value[0]  # "1|Alice"
```

### Upsert (ON CONFLICT)

```nim
# Insert or update on conflict
discard execSql(db, """
  INSERT INTO users (id, name) VALUES (1, 'Alice')
  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
""")

# Insert or ignore
discard execSql(db, """
  INSERT INTO users (id, name) VALUES (1, 'Alice')
  ON CONFLICT DO NOTHING
""")
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

# TIMESTAMP (native datetime): int64 microseconds since Unix epoch UTC
let tsVal = Value(kind: vkDateTime, int64Val: 1735689600'i64 * 1_000_000)  # 2025-01-01T00:00:00 (UTC)

# DECIMAL (scaled integer)
let decVal = Value(kind: vkDecimal, int64Val: 12345, decimalScale: 2)  # 123.45
```

### Converting Values to Strings

```nim
proc valueToString(v: Value): string =
  case v.kind
  of vkNull: "NULL"
  of vkInt64, vkDateTime: $v.int64Val
  of vkFloat64: $v.float64Val
  of vkBool: if v.boolVal: "true" else: "false"
  of vkDecimal:
    # Value = int64Val * 10^-decimalScale
    $v.int64Val & "e-" & $v.decimalScale
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

DecentDB is on the stable `1.x` line and follows Semantic Versioning for public API compatibility.
Patch releases may include fixes and internal performance work; minor releases may add backward-compatible features.
