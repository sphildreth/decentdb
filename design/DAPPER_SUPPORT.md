# Dapper Support Requirements for DecentDB 1.0.0

## Overview

Enable C# applications to perform high-performance CRUD operations and LINQ-style queries against DecentDB database files through Dapper, without requiring a server process or decorative attributes on POCOs.

## Goals

- C# apps can query DecentDB files directly (embedded mode)
- Dapper integration works out-of-the-box
- LINQ-style queries with Skip/Take pagination
- Convention-based mapping (zero configuration)
- **Performance-first SELECT operations** - Query execution overhead < 1ms for typical operations

## Performance Targets

All SELECT operations must meet these performance criteria:

| Query Type | Target | Max Acceptable |
|------------|--------|----------------|
| Single record by ID | 0.5ms | 2ms |
| Simple list (no pagination) | 1ms + 0.1ms/row | 5ms + 0.5ms/row |
| Filtered list (1-2 conditions) | 2ms + 0.1ms/row | 10ms + 0.5ms/row |
| Paginated query (Skip/Take) | 3ms + 0.1ms/row | 15ms + 0.5ms/row |
| Sorted + Paginated | 4ms + 0.1ms/row | 20ms + 0.5ms/row |
| Count with filter | 2ms | 10ms |
| Text search (trigram index) | 5ms + 0.2ms/row | 50ms + 1ms/row |

**Critical**: Query execution overhead (C# layer) must add < 1ms to native DecentDB query time.

## Non-Goals

- No DecentDB server process (embedded only)
- No Entity Framework Core provider (future consideration)
- No change tracking (Dapper-style stateless operations)
- No required attributes (convention-first, attributes optional)

## Optional Attributes

While conventions handle 90% of cases, optional attributes available for edge cases:

| Attribute | Use Case | When Needed |
|-----------|----------|-------------|
| `[Table("custom_name")]` | Non-conventional table names | When table name doesn't match pluralized class name |
| `[Column("custom_column")]` | Non-conventional column names | Legacy schemas, reserved keywords, naming conflicts |
| `[PrimaryKey]` | Non-Id primary keys | When PK property is not named `Id` |
| `[NotNull]` / `[Nullable]` | Override nullability | Value types that should be NULL, refs that cannot be NULL |
| `[Index]` | Add non-foreign-key indexes | Frequently queried columns, search optimization |
| `[Ignore]` | Exclude properties from mapping | Computed properties, temporary fields |

**Philosophy**: Conventions first, attributes only when conventions fail. Zero attributes should work for greenfield development.

### Attribute Examples

```csharp
// Convention-based (zero attributes)
public class Artist
{
    public int Id { get; set; }           // → column "id", PK
    public string Name { get; set; }      // → column "name"
    public DateTime CreatedAt { get; set; } // → column "createdat"
}

// When you need attributes
[Table("tbl_artist")]  // Legacy database with different naming
public class Artist
{
    [PrimaryKey]  // Property not named "Id"
    [Column("artist_id")]
    public int ArtistId { get; set; }
    
    [Column("display_name")]
    public string Name { get; set; }
    
    [Index]  // Add index for frequent queries
    public string Genre { get; set; }
    
    [Ignore]  // Not stored in database
    public string TempData { get; set; }
}

---

## Architecture

```
C# Application
    ├── Dapper (optional, works via ADO.NET)
    └── DecentDb.MicroOrm (LINQ + conventions)
            ↓
    DecentDb.AdoNet (ADO.NET provider)
            ↓
    DecentDb.Native (P/Invoke to Nim DLL)
            ↓
    DecentDB (Nim engine, direct file I/O)
```

---

## Phase 1: Native C API (Nim)

### Requirements

Expose a C-compatible API from the Nim DecentDB engine for P/Invoke:

```c
// Database lifecycle
db_handle* decentdb_open(const char* path, const char* options);
int decentdb_close(db_handle* db);
const char* decentdb_last_error(db_handle* db);

// Query execution
decentdb_result* decentdb_query(db_handle* db, const char* sql, decentdb_param* params, int param_count);
int decentdb_execute(db_handle* db, const char* sql, decentdb_param* params, int param_count, int64_t* rows_affected);

// Result handling
int decentdb_result_row_count(decentdb_result* result);
int decentdb_result_column_count(decentdb_result* result);
const char* decentdb_result_column_name(decentdb_result* result, int col);
decentdb_value* decentdb_result_get(decentdb_result* result, int row, int col);
void decentdb_result_free(decentdb_result* result);

// Type accessors
int decentdb_value_type(decentdb_value* val);
int64_t decentdb_value_int64(decentdb_value* val);
double decentdb_value_float64(decentdb_value* val);
const char* decentdb_value_text(decentdb_value* val);
const uint8_t* decentdb_value_blob(decentdb_value* val, int* size);
```

### Tasks

1. **Create C API wrapper module** (`src/c_api.nim`)
   - Wrap existing Nim API in C-compatible functions
   - Handle memory management (caller frees results)
   - Error handling via last_error string
   - Thread-safety (single writer, multiple readers)

2. **Export Nim functions with C calling convention**
   - Compile to shared library (`.so`/`.dylib`/`.dll`)
   - Generate header file for C# P/Invoke
   - Handle platform differences

3. **Memory management design**
   - Query results: C# owns memory, must call `decentdb_result_free`
   - Strings: return const char* (Nim manages, copied in C#)
   - Blobs: return pointer + size (copied in C#)

---

## Phase 2: ADO.NET Provider

### Requirements

Implement standard ADO.NET interfaces for Dapper compatibility:

```csharp
public class DecentDbConnection : DbConnection
public class DecentDbCommand : DbCommand  
public class DecentDbParameter : DbParameter
public class DecentDbDataReader : DbDataReader
public class DecentDbTransaction : DbTransaction
```

### Tasks

1. **DecentDbConnection**
   - Connection string parsing: `Data Source=/path/to.db;Cache Size=1024`
   - Open/Close with P/Invoke to `decentdb_open`/`decentdb_close`
   - Connection pooling (Dapper manages this)
   - Async support (Begin/End pattern or async/await)

2. **DecentDbCommand**
   - SQL execution via `decentdb_query`/`decentdb_execute`
   - Parameter collection (`@name` or `$1` syntax)
   - Command timeout support
   - Async execution

3. **DecentDbParameter**
   - Type mapping: C# types ↔ DecentDB types
   - Support for: int, long, string, double, bool, byte[], DateTime

4. **DecentDbDataReader**
   - Forward-only, read-only cursor
   - Type-safe accessors (GetInt32, GetString, etc.)
   - Column name resolution
   - IDisposable cleanup

5. **DecentDbTransaction**
   - BEGIN/COMMIT/ROLLBACK support
   - Savepoints (optional for 1.0.0)
   - Isolation level (Snapshot)

---

## Phase 3: Convention-Based Micro-ORM

### Requirements

Provide LINQ-style query building with semantic defaults:

```csharp
// Zero configuration - conventions determine mapping
public class Artist {        // → table "artists"
    public int Id { get; set; }      // → column "id", primary key
    public string Name { get; set; } // → column "name"
}

// Usage
using var db = new DecentDbContext("/path/to.db");

// Get single record
var artist = await db.Artists.GetAsync(1);

// Query with LINQ
var artists = await db.Artists
    .Where(a => a.Name.StartsWith("A"))  // → WHERE name LIKE 'A%'
    .OrderBy(a => a.Name)
    .Skip(10)                            // → OFFSET 10
    .Take(20)                            // → LIMIT 20
    .ToListAsync();

// CRUD operations
await db.Artists.InsertAsync(artist);
await db.Artists.UpdateAsync(artist);
await db.Artists.DeleteAsync(artist);
```

### Conventions (Semantic Defaults)

| Convention | Rule |
|------------|------|
| Table name | Pluralized, lowercase class name (`Artist` → `artists`) |
| Column name | C# PascalCase → lowercase column (`Name` → `name`) |
| Primary key | Property named `Id` (maps to column `id`) |
| Foreign key | Property named `{NavigationProperty}Id` (maps to `{navigationproperty}_id`) |
| Nullable | Reference types nullable, value types not null |
| Type mapping | Standard C# types to DecentDB types |

**Foreign Key Example:**
```csharp
public class Album
{
    public int Id { get; set; }           // → column "id"
    public string Title { get; set; }     // → column "title"
    public int ArtistId { get; set; }     // → column "artist_id", FK to artists
}
```

### Tasks

1. **Expression tree parser**
   - Parse `Expression<Func<T, bool>>` predicates
   - Support: ==, !=, <, >, <=, >=, &&, ||
   - Support: string.Contains, StartsWith, EndsWith → LIKE
   - Support: int/long range queries
   - Generate parameterized SQL (prevent injection)

2. **Query builder**
   - Fluent API: Where, OrderBy, OrderByDescending, ThenBy, ThenByDescending, Skip, Take
   - Method translation:
     - `Where(predicate)` → WHERE clause with parameterized values
     - `OrderBy(key)` → ORDER BY ASC
     - `OrderByDescending(key)` → ORDER BY DESC
     - `ThenBy(key)` → comma-separated ORDER BY (secondary sort)
     - `ThenByDescending(key)` → comma-separated ORDER BY DESC
     - `Skip(n)` → OFFSET n
     - `Take(n)` → LIMIT n
     - `First()` → LIMIT 1
     - `Count()` → SELECT COUNT(*)
   - Query composition (chaining multiple operations)
    - **Pagination Pattern**: `.OrderBy(x => x.Id).Skip(100).Take(20)` → `ORDER BY id LIMIT 20 OFFSET 100`

3. **Convention engine**
   - Type → table name mapping (pluralization rules)
   - Property → column name mapping
    - Primary key detection (property named `Id`)
   - Foreign key relationships (navigation properties)
   - Type conversion (C# DateTime ↔ DecentDB INT64)

4. **DbSet<T> implementation**
   - `IQueryable<T>` implementation
   - CRUD methods: GetAsync, InsertAsync, UpdateAsync, DeleteAsync
   - Bulk operations (InsertMany, DeleteMany)
   - Async throughout

5. **DecentDbContext**
   - Database file path configuration
   - DbSet<T> property discovery
   - Connection management (open/close per operation)
   - Transaction support (BeginTransaction)

---

## Phase 4: Type Mapping

### C# to DecentDB Type Matrix

| C# Type | DecentDB Type | Storage Size | Notes |
|---------|---------------|--------------|-------|
| `short` / `Int16` | INT64 | 8 bytes | 16-bit value promoted to INT64 |
| `int` / `Int32` | INT64 | 8 bytes | 32-bit value promoted to INT64 |
| `long` / `Int64` | INT64 | 8 bytes | Native 64-bit signed |
| `string` | VARCHAR(255) | Variable (UTF-8) | Full Unicode support, default 255 chars |
| `string` + `[Text]` | TEXT | Variable (UTF-8) | Unbounded text (explicit opt-in) |
| `double` | FLOAT64 | 8 bytes | IEEE 754 double precision |
| `float` | FLOAT64 | 8 bytes | Promoted to double precision |
| `bool` | BOOL | 1 byte | 0 = false, 1 = true |
| `byte[]` | BLOB | Variable | Binary data, max 1GB per value |
| `Guid` | BLOB | 16 bytes | Fixed-size binary, PK-safe |
| `DateTime` | INT64 | 8 bytes | Unix epoch milliseconds (UTC) |
| `DateTimeOffset` | INT64 | 8 bytes | Unix epoch milliseconds (UTC) |
| `DateOnly` | INT64 | 8 bytes | Days since Unix epoch (midnight UTC) |
| `TimeSpan` | INT64 | 8 bytes | Ticks (100-nanosecond units) |
| `TimeOnly` | INT64 | 8 bytes | Ticks since midnight |
| `enum` | INT64 | 8 bytes | Underlying value as integer |
| `decimal` | TEXT | Variable | String representation (precision preserved) |
| `char` | TEXT | 1-4 bytes | Single Unicode character |

### Type Mapping Details

#### 1. **Integer Types (short, int, long)**
All promoted to INT64 for simplicity:
- No overflow issues with 16-bit or 32-bit values
- Single code path for all integer operations
- Slight storage overhead (8 bytes vs 2/4 bytes) acceptable for embedded use

```csharp
public class Product
{
    public short CategoryId { get; set; }  // Stored as INT64
    public int Quantity { get; set; }        // Stored as INT64
    public long TotalSales { get; set; }    // Stored as INT64
}
```

#### 2. **Guid as BLOB (Primary Key Performance)**
**Performance characteristics:**
- **Storage**: 16 bytes (vs 8 bytes for INT64) - 2x size
- **Index performance**: Identical to INT64 for lookups (fixed-size binary comparison)
- **Insertion**: Slightly slower than sequential INT64 (random page access)
- **Use case**: Perfect for distributed systems, natural keys, UUID requirements

**Recommendation**: Use `id` (INT64 auto-increment) for performance, `Guid` only when needed:
```csharp
// High performance (sequential inserts, clustered index)
public class Order
{
    public long Id { get; set; }  // INT64, auto-increment recommended
}

// Distributed/Microservices (when ID must be generated client-side)
public class Event
{
    public Guid Id { get; set; }  // BLOB(16), client-generated
}
```

#### 3. **String Length Constraints (VARCHAR Support)**
**HARD REQUIREMENT**: DecentDB must support `VARCHAR(n)` in DDL with length validation at both language binding and engine layers.

**Architecture: Hybrid Storage with Dual Validation**
```
SQL Layer:     VARCHAR(n) → Parser/Binder
                  ↓
Catalog:       TEXT storage + max_length metadata
                  ↓
Validation:    C# AND Nim (defense in depth)
```

**SQL Syntax Support:**
```sql
-- Standard SQL VARCHAR (RECOMMENDED for most cases)
CREATE TABLE users (
    id INT PRIMARY KEY,
    username VARCHAR(100) NOT NULL,  -- Max 100 characters
    email VARCHAR(255),
    bio TEXT                          -- Unbounded (explicit choice)
);

-- TEXT remains valid for unbounded strings
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title VARCHAR(200),
    content TEXT  -- No length limit
);
```

**C# Mapping:**
```csharp
public class User
{
    public int Id { get; set; }
    
    // Convention: string without attribute → VARCHAR(255) default
    public string Username { get; set; }
    
    // Explicit length override
    [MaxLength(100)]
    public string Code { get; set; }
    
    // Unbounded text (must opt-in)
    [Text]
    public string Description { get; set; }
}

// Generates: 
// CREATE TABLE users (
//     id INT PRIMARY KEY,
//     username VARCHAR(255),
//     code VARCHAR(100),
//     description TEXT
// )
```

**Dual Validation Strategy (Defense in Depth):**

| Layer | Responsibility | Behavior on Violation |
|-------|---------------|---------------------|
| **C#** | Pre-flight validation | `ArgumentException` before SQL sent |
| **Nim Binder** | SQL parameter validation | `ERR_CONSTRAINT` with message |
| **Nim Storage** | Last-resort check | Reject with constraint violation |

**Why Both Layers:**
- **C# validation**: Fast feedback during development, saves round-trip
- **Nim validation**: Enforced for all clients (Node.js, Kotlin, Go, raw SQL)
- **Security**: Prevents bypass via direct SQL injection attempts

**Storage Implementation:**
```nim
# Catalog stores max_length alongside column metadata
type ColumnMeta = object
  name: string
  type_name: string        # "TEXT" or "VARCHAR"
  max_length: Option[int]  # Some(n) for VARCHAR, None for TEXT
  nullable: bool
```

**Validation Points:**
1. **Parse Time**: `VARCHAR(n)` syntax validated (n > 0, n <= max_allowed)
2. **Bind Time**: String literals checked against column constraints
3. **Insert/Update Time**: All string values validated before storage
4. **C# Client**: Pre-validates before sending to minimize errors

**Migration Path:**
```sql
-- Can convert between VARCHAR and TEXT
ALTER TABLE users ALTER COLUMN username TYPE TEXT;
ALTER TABLE users ALTER COLUMN bio TYPE VARCHAR(500);
```

**Error Messages:**
```
C#: ArgumentException: "Value exceeds maximum length of 100 characters"
Nim: ERR_CONSTRAINT: "String value exceeds column 'username' maximum length (100)"
```

**Performance Notes:**
- Validation cost: ~1-2 microseconds per string
- Tables with 20+ VARCHAR columns may exceed 0.5ms target for single-record lookups
- Storage: Same as TEXT (variable length), no padding
- Index: Prefix index recommended for VARCHAR columns > 100 chars

#### 4. **Time Types (TimeSpan, TimeOnly)**
Stored as INT64 ticks for precision:
- **TimeSpan**: Total ticks (can exceed 24 hours for durations)
- **TimeOnly**: Ticks since midnight (0 to 863,999,999,999)

```csharp
public class Schedule
{
    public int Id { get; set; }
    public TimeOnly StartTime { get; set; }     // → INT64 (ticks since midnight)
    public TimeSpan Duration { get; set; }       // → INT64 (total ticks)
}

// Query with time comparisons
var meetings = db.Schedules
    .Where(s => s.start_time > new TimeOnly(9, 0))  // After 9 AM
    .ToList();
```

#### 5. **DateOnly (.NET 6+)**
Date without time component - useful for birth dates, anniversaries:
```csharp
public class Person
{
    public int Id { get; set; }
    public string Name { get; set; }
    public DateOnly BirthDate { get; set; }  // → INT64 (days since epoch)
}

// Range queries work naturally
var adults = db.People
    .Where(p => p.birth_date < DateOnly.FromDateTime(DateTime.Now.AddYears(-18)))
    .ToList();
```

#### 6. **UTF-8 and Unicode Support**
- **TEXT column**: Full UTF-8 encoding (1-4 bytes per character)
- **Supports**: ASCII, Latin-1, CJK, Emoji, all Unicode planes
- **Length behavior**: Length measured in **characters**, not bytes
- **Comparison**: UTF-8 binary collation (fast, culture-insensitive)

```csharp
// All valid in TEXT columns
public class Message
{
    public int Id { get; set; }
    public string English { get; set; }      // ASCII
    public string Japanese { get; set; }     // 日本語 (3 bytes per char)
    public string Emoji { get; set; }        // 🎉🚀 (4 bytes per char)
}
```

**Important**: String length validation counts characters, not bytes. A 100-character string with emoji uses ~400 bytes storage but passes `[MaxLength(100)]` validation.

### Tasks

1. **Type converter system**
   - ITypeConverter interface for custom mappings
   - Built-in converters for all standard types
   - Nullable type support (int? → may be NULL)

2. **DateTime handling**
   - Unix epoch milliseconds for all date/time types
   - Timezone handling (store UTC only)
   - Conversion helpers for DateOnly, TimeOnly, TimeSpan

3. **VARCHAR(n) Support (HARD REQUIREMENT)**
   - **Parser**: Add `VARCHAR(n)` syntax support in SQL parser
   - **Binder**: Validate length parameter (n > 0, reasonable max)
   - **Catalog**: Add `max_length` column to catalog schema
   - **Storage**: Store as TEXT internally with length metadata
   - **Validation Layer**: 
     - Nim: Validate string lengths in INSERT/UPDATE binder
     - C#: Pre-validate before SQL execution
   - **Error Handling**: Standard constraint violation error codes
   - **Migration**: Support ALTER TABLE to change VARCHAR ↔ TEXT
   - **CLI**: Update `describe` command to show VARCHAR(n) instead of TEXT
   - **Tests**: Unit tests for all length violations, edge cases (empty, max, max+1)

4. **Unicode and Encoding**
   - UTF-8 validation on all TEXT/VARCHAR inputs
   - Character count vs byte count distinction for MaxLength
   - Proper handling of surrogate pairs and combining characters

---

## Error Handling Strategy

### Requirements

Consistent error reporting across all layers with clear mapping between native DecentDB errors and C# exceptions.

### Error Code Mapping

| DecentDB Error Code | C# Exception Type | Description |
|---------------------|-------------------|-------------|
| `ERR_CONSTRAINT` | `ConstraintViolationException` | Constraint violation (e.g., VARCHAR length, foreign key) |
| `ERR_LOCK_TIMEOUT` | `DatabaseLockedException` | Timeout waiting for database lock |
| `ERR_IO_ERROR` | `IOException` | File system error |
| `ERR_PARSE_ERROR` | `SqlSyntaxErrorException` | Invalid SQL syntax |
| `ERR_BIND_ERROR` | `ArgumentException` | Parameter binding error |
| `ERR_FULL` | `DatabaseFullException` | Disk space exhausted |
| `ERR_CORRUPT` | `DatabaseCorruptedException` | Database file corruption detected |

### Error Propagation Chain

```
Native Layer (Nim):
  - Return error codes via `decentdb_last_error()`
  - Detailed error messages in UTF-8

P/Invoke Layer (C#):
  - Check return codes from native calls
  - Marshal error messages from `decentdb_last_error()`
  - Map native error codes to C# exceptions

ADO.NET Layer:
  - Translate exceptions to standard ADO.NET exception types
  - Preserve original error details in exception data
  - Follow ADO.NET exception hierarchy

Micro-ORM Layer:
  - Wrap lower-level exceptions with context
  - Include SQL statement and parameters in exception details
  - Maintain exception chaining for debugging
```

### Constraint Violation Handling

**VARCHAR(n) Length Validation:**
```csharp
// C# layer pre-validation
public void SetValue(DbParameter param, string value)
{
    if (param.MaxLength > 0 && value.Length > param.MaxLength)
    {
        throw new ArgumentException(
            $"Value exceeds maximum length of {param.MaxLength} characters. Actual length: {value.Length}");
    }

    // Pass to native layer for secondary validation
    SetNativeValue(param, value);
}
```

**Native layer validation:**
```nim
# Secondary validation in Nim
proc validate_string_length(col_meta: ColumnMeta, value: string): bool =
  if col_meta.max_length.isSome():
    if value.len > col_meta.max_length.get():
      setError(ERR_CONSTRAINT, fmt"String value exceeds column '{col_meta.name}' maximum length ({col_meta.max_length.get()})")
      return false
  return true
```

### Exception Context Preservation

All exceptions should preserve context for debugging:

```csharp
public class DecentDbException : DataException
{
    public string SqlStatement { get; }
    public Dictionary<string, object> Parameters { get; }
    public DateTimeOffset Timestamp { get; }
    public string NativeErrorMessage { get; }

    public DecentDbException(string message, string sql, Dictionary<string, object> parameters)
        : base(message)
    {
        SqlStatement = sql;
        Parameters = parameters ?? new Dictionary<string, object>();
        Timestamp = DateTimeOffset.UtcNow;
    }
}
```

### Connection-Level Error Handling

Handle database-level errors that affect connection state:

```csharp
public class DecentDbConnection : DbConnection
{
    protected override void Open()
    {
        try
        {
            _handle = decentdb_open(_connectionString, _options);
            if (_handle == null)
            {
                var errorMsg = decentdb_last_error(null);
                throw new InvalidOperationException($"Failed to open database: {errorMsg}");
            }
        }
        catch (Exception ex)
        {
            throw new CannotOpenDatabaseException($"Unable to connect to database at {_dataSource}", ex);
        }
    }
}
```

### Transaction Error Recovery

Handle transaction rollback and recovery scenarios:

```csharp
public async Task<T> InTransactionAsync<T>(Func<Task<T>> operation)
{
    using var tx = BeginTransaction();
    try
    {
        var result = await operation();
        await tx.CommitAsync();
        return result;
    }
    catch (Exception ex)
    {
        try
        {
            await tx.RollbackAsync();
        }
        catch (Exception rollbackEx)
        {
            // Log rollback failure but throw original exception
            _logger?.LogWarning(rollbackEx, "Transaction rollback failed after operation error");
        }

        throw new TransactionOperationException("Transaction failed and was rolled back", ex);
    }
}
```

### Async Error Handling

Ensure proper exception propagation in async operations:

```csharp
public async Task<List<T>> QueryAsync<T>(string sql, params DbParameter[] parameters)
{
    using var cmd = CreateCommand(sql, parameters);
    try
    {
        return await cmd.ToListAsync<T>();
    }
    catch (Exception ex)
    {
        // Wrap with context about the operation
        throw new QueryExecutionException($"Error executing query: {sql}", sql, parameters, ex);
    }
}
```

---

## Phase 5: Performance Optimization

### Requirements

**SELECT operations are critical path - every millisecond matters.**

Target performance for common operations:

| Operation | Target | P95 Max |
|-----------|--------|---------|
| Single record by ID | 0.5ms | 2ms |
| Simple list (no pagination) | 1ms + 0.1ms/row | 5ms + 0.5ms/row |
| Filtered query (WHERE) | 2ms + 0.1ms/row | 10ms + 0.5ms/row |
| Paginated (Skip/Take) | 3ms + 0.1ms/row | 15ms + 0.5ms/row |
| Sorted + Paginated | 4ms + 0.1ms/row | 20ms + 0.5ms/row |
| Count with filter | 2ms | 10ms |
| Text search (trigram) | 5ms + 0.2ms/row | 50ms + 1ms/row |
| Insert single row | < 2ms | 5ms |
| Bulk insert (1000 rows) | < 100ms | 200ms |
| Connection open | < 1ms | 3ms |

**Overhead Budget**: C# layer must add < 1ms to native DecentDB execution time.

### SELECT Performance Optimization Tasks

#### 1. **Query Compilation Caching** (Critical)
- Cache `Expression<Func<T,bool>>` → SQL string compilation
- Cache keyed by expression tree hash
- Invalidate only when expression changes
- Pre-compile common query patterns at startup

```csharp
// Cache compiled queries
var cacheKey = ComputeHash(expression);
if (_queryCache.TryGetValue(cacheKey, out var compiled))
    return compiled;
    
var sql = CompileExpression(expression);
_queryCache[cacheKey] = sql;
return sql;
```

#### 2. **Fast Materialization** (Critical)
- Use `Expression.Compile()` to create delegates for object creation
- Generate IL or use compiled expressions instead of reflection
- Cache property setters per type
- Avoid boxing for value types

```csharp
// Compile once: Func<IDataReader, T>
var factory = Expression.Lambda<Func<IDataReader, T>>(
    Expression.MemberInit(
        Expression.New(typeof(T)),
        properties.Select(p => Expression.Bind(
            p.Property,
            Expression.Call(reader, getValueMethod, Expression.Constant(p.Ordinal))
        ))
    ),
    reader
).Compile();
```

#### 3. **Efficient P/Invoke for Results** (Critical)
- Use struct-based result marshaling instead of pointer chasing
- Batch row retrieval (fetch N rows at once from native)
- Minimize managed/native boundary crossings
- Use blittable types exclusively

```csharp
// Instead of row-by-row P/Invoke
[DllImport("decentdb")]
static unsafe extern int decentdb_fetch_batch(
    void* result,
    RowData* buffer,
    int bufferSize,
    out int fetched);

struct RowData
{
    public long Id;
    public fixed byte Name[256];  // Inline string buffer
    public long CreatedAt;
}
```

#### 3.5. **Memory Management for P/Invoke Layer** (Critical)
- Explicit resource ownership: C# layer owns all allocated memory from native calls
- Deterministic cleanup with `IDisposable` pattern for result sets
- Safe handle pattern for native resources (database handles, prepared statements)
- Pinning strategy for large data transfers (avoid excessive GC pressure)
- Buffer reuse patterns to minimize allocations during result processing

**Safe Handle Implementation:**
```csharp
public class DecentDbHandle : SafeHandle
{
    public DecentDbHandle() : base(IntPtr.Zero, true) { }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            decentdb_close(handle);
            handle = IntPtr.Zero;
            return true;
        }
        return false;
    }
}
```

**Result Set Memory Management:**
```csharp
public class DecentDbResultSet : IDisposable
{
    private IntPtr _resultPtr;
    private bool _disposed = false;

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed && _resultPtr != IntPtr.Zero)
        {
            decentdb_result_free(_resultPtr);
            _resultPtr = IntPtr.Zero;
            _disposed = true;
        }
    }

    ~DecentDbResultSet()
    {
        Dispose(false);
    }
}
```

**String and Blob Handling:**
- Copy strings from native to managed memory immediately after P/Invoke call
- Free native string memory before returning to caller
- Use `Marshal.PtrToStringUTF8` for efficient UTF-8 string conversion
- For blobs, copy to managed byte arrays to avoid pinning issues

#### 4. **Connection Pooling with Reader Semantics**
- Pool connections separately for readers vs writers
- Keep reader connections "warm" with cached handles
- Automatic handle reuse for consecutive queries
- Thread-local storage for connection affinity

#### 5. **Streaming for Large Result Sets**
- `IAsyncEnumerable<T>` support for large queries
- Don't buffer entire result set in memory
- Yield rows as they're read from native layer
- Configurable fetch size (default 100 rows)

```csharp
public async IAsyncEnumerable<T> StreamAsync(
    Expression<Func<T, bool>> predicate,
    [EnumeratorCancellation] CancellationToken ct = default)
{
    using var cmd = CreateCommand(predicate);
    using var reader = await cmd.ExecuteReaderAsync(ct);
    
    while (await reader.ReadAsync(ct))
    {
        yield return _materializer(reader);
    }
}
```

#### 6. **Smart Pagination**
- Use keyset pagination (WHERE id > @lastId) when possible
- Avoid OFFSET for large datasets (O(n) scan)
- Cache total count for frequent paginated queries
- Support cursor-based pagination for infinite scroll

```csharp
// Bad: OFFSET 100000 (scans 100k rows)
SELECT * FROM artists ORDER BY id LIMIT 20 OFFSET 100000

// Good: Keyset pagination (O(log n))
SELECT * FROM artists WHERE id > @lastId ORDER BY id LIMIT 20
```

#### 7. **Projection Optimization**
- `Select()` should generate column-specific queries
- Avoid `SELECT *` when only subset needed
- Support anonymous type projection
- Skip materialization for aggregate-only queries

```csharp
// Only fetch columns we need
var names = db.Artists.Select(a => a.Name).ToList();
// → SELECT name FROM artists (not SELECT *)

// Anonymous projection
var dtos = db.Artists.Select(a => new { a.Name, a.Genre }).ToList();
// → SELECT name, genre FROM artists
```

#### 8. **Index Awareness**
- Validate filter columns have indexes at query time (debug mode)
- Warn if query performs full table scan
- Provide `WithIndexHint()` method for query optimization
- Analyze query plan (if DecentDB exposes it)

#### 9. **Async I/O Optimization**
- True async/await throughout (no sync-over-async)
- Use `ValueTask` for hot paths to reduce allocations
- Cancelable operations with `CancellationToken`
- I/O completion ports on Windows, epoll/kqueue on Linux/macOS

#### 10. **Result Set Caching** (Optional)
- Cache immutable query results (e.g., lookup tables)
- Time-based expiration
- Manual cache invalidation for write operations
- Cache key based on SQL + parameters hash

### CRUD Performance (Secondary Priority)

1. **Bulk Operations**
   - Batch inserts (single transaction)
   - Prepared statement reuse
   - Parameter batching

2. **Update/Delete Optimization**
   - Where-expression updates without fetching
   - Direct SQL generation for simple cases

3. **P/Invoke Batching**
   - Minimize round-trips to native layer
   - Batch parameter binding

---

## Phase 6: Testing Strategy

### Tasks

1. **Unit tests**
   - Convention mapping tests
   - Expression tree parser tests
   - Type converter tests
   - SQL generation tests

2. **Integration tests**
   - Round-trip tests (insert → query → verify)
   - CRUD operation tests
   - Pagination tests (Skip/Take)
   - Transaction tests
   - Performance benchmarks

3. **Dapper compatibility tests**
   - Verify Dapper.Query works
   - Verify Dapper.Execute works
   - Parameter handling tests

4. **Cross-platform tests**
   - Windows (.NET 10)
   - Linux (.NET 10)
   - macOS (.NET 10)

### Cross-Platform Considerations

**Native Library Distribution:**
- Platform-specific native libraries packaged in NuGet `runtimes/` folder
- RID (Runtime Identifier) specific builds for x64 and ARM64 architectures
- Automatic selection of appropriate native library at runtime
- Fallback mechanism if specific platform variant is unavailable

**File System Differences:**
- Case sensitivity: Linux/macOS file systems are case-sensitive, Windows is not
- Path separators: Use `System.IO.Path.DirectorySeparatorChar` consistently
- File locking: Different behaviors across platforms (advisory vs mandatory locks)
- Permissions: Linux/macOS require appropriate file permissions for database files

**Character Encoding:**
- UTF-8 encoding used consistently across all platforms
- Native libraries must handle UTF-8 paths correctly on all platforms
- String marshaling uses UTF-8 for all text data exchange

**Performance Variations:**
- Different default page sizes may be optimal per platform
- Memory management differs between platforms (GC behavior)
- I/O patterns vary (WAL file handling, checkpoint behavior)
- Threading models differ (Windows fibers vs Linux pthreads)

**Build Configuration:**
- Separate build pipelines for each target platform
- Static linking preferred to avoid runtime dependencies
- Compiler flags optimized per platform (SIMD instructions, etc.)
- Size optimization for embedded scenarios

**Platform-Specific Optimizations:**
- Windows: Use of Windows APIs for file I/O and synchronization
- Linux: epoll for async I/O, futex for synchronization
- macOS: kqueue for async I/O, native synchronization primitives

### Testing Strategy Enhancements

**Performance Regression Testing:**
- Automated performance benchmarks integrated into CI pipeline
- Baseline performance measurements for all critical operations
- Alerting when performance degrades beyond acceptable thresholds
- Separate benchmarks for cold vs warm cache scenarios

**Compatibility Testing:**
- Test against multiple .NET versions (.NET 10, future versions)
- Verify compatibility with different Dapper versions
- Test with various third-party ADO.NET diagnostic tools
- Validate behavior with popular logging frameworks (Serilog, NLog, etc.)

**Real-World Scenario Testing:**
- Simulated application workloads with mixed read/write patterns
- Stress testing with concurrent operations
- Memory leak detection over extended periods
- File corruption and recovery scenario testing

**Integration Test Coverage:**
- End-to-end tests using real-world domain models
- Validation of complex LINQ queries with joins and aggregations
- Test backup/restore operations with active connections
- Verify behavior during system resource constraints (memory, disk space)

---

## Implementation Order

### Sprint 0: Core Engine (Prerequisite)
1. **VARCHAR(n) Implementation (HARD REQUIREMENT)**
   - Parser support for `VARCHAR(n)` syntax
   - Catalog schema extension (add `max_length` column)
   - Nim binder validation for string lengths
   - Error handling for constraint violations
   - CLI describe command updates
   - Comprehensive unit tests

**Critical Dependency Note**: VARCHAR(n) implementation in the core DecentDB engine is a prerequisite for the entire Dapper integration effort. This feature must be completed before any C# layer development begins, as it requires changes to the SQL parser, binder, and catalog schema. The Nim engine must support this feature before the C API can expose it, and before the ADO.NET provider can utilize it. Teams should coordinate to ensure the core engine implementation is completed and tested before proceeding to Sprint 1.

**Dependency Chain**:
```
Core Engine (VARCHAR support) → C API → ADO.NET Provider → Micro-ORM → Dapper Compatibility
```

Any delays in the core engine implementation will cascade to all subsequent phases. Consider parallelizing work where possible by having C# team begin ADO.NET implementation against mock interfaces while core engine team completes VARCHAR implementation.

### Sprint 1: Foundation
1. C API wrapper in Nim
2. Native library build (Windows/Linux/macOS)
3. Basic P/Invoke layer in C#

### Sprint 2: ADO.NET
1. DecentDbConnection implementation
2. DecentDbCommand and Parameters
3. DecentDbDataReader
4. Dapper integration tests

### Sprint 3: Micro-ORM
1. Convention engine
2. Expression tree parser (basic predicates)
3. DbSet<T> with Where/GetAsync
4. CRUD operations

### Sprint 4: LINQ Features
1. OrderBy/ThenBy
2. Skip/Take pagination
3. First/Single/Count aggregates
4. Query composition

### Sprint 5: Polish
1. Type converters and DateTime handling
2. Performance optimization
3. Comprehensive testing
4. Documentation and samples

---

## Architecture Decision Records (ADRs)

**All significant implementation decisions must be documented via ADRs.**

### When to Create an ADR

Create an ADR in `design/adr/` for any of the following:

- **Protocol changes**: C API design decisions, P/Invoke strategy
- **Type mapping**: How C# types map to DecentDB types (e.g., DateTime → INT64)
- **Performance trade-offs**: Query caching strategy, connection pooling approach
- **Breaking changes**: Any change that affects backward compatibility
- **Security decisions**: Validation strategies, encoding choices
- **Cross-language concerns**: How conventions work across C#, Node.js, Kotlin, Go

### ADR Format

Use the template at `design/adr/0000-template.md`:

```markdown
# ADR-00XX: Title

**Status**: Proposed / Accepted / Deprecated
**Date**: YYYY-MM-DD

## Context
What is the issue we're deciding?

## Decision
What did we decide?

## Consequences
What are the trade-offs? What else was considered?
```

### Required ADRs for This Project

The following decisions must have ADRs before implementation:

1. **ADR-00XX: C API Design** - Native library interface (P/Invoke vs C++/CLI)
2. **ADR-00XX: Type System** - C# to DecentDB type mappings
3. **ADR-00XX: Connection Pooling** - Single writer enforcement strategy
4. **ADR-00XX: Query Compilation** - Expression tree caching approach
5. **ADR-00XX: VARCHAR Implementation** - Length validation architecture
6. **ADR-00XX: String Encoding** - UTF-8 handling and validation
7. **ADR-00XX: NuGet Packaging** - Native library distribution strategy
8. **ADR-00XX: SQL Observability** - Event-based logging with zero-cost when disabled
9. **ADR-00XX: Connection String Design** - Parameter parsing, validation, and default behavior

**No implementation without documentation.** Each major feature sprint must have corresponding ADRs created before coding begins.

## SQL Logging and Observability

### Requirements

**Zero-cost when disabled.** Full observability when enabled.

### Connection String Configuration

```
Data Source=/path/to.db;Logging=1;LogLevel=Debug
```

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `Logging` | `0` or `1` | `0` | Enable/disable SQL logging globally |
| `LogLevel` | `Verbose`, `Debug`, `Info`, `Warning`, `Error` | `Debug` | Minimum log level for SQL statements |

**Performance guarantee:** When `Logging=0`, overhead must be < 5ns per query (null check only).

### Events (Zero-Cost Pattern)

```csharp
public class SqlExecutingEventArgs : EventArgs
{
    public string Sql { get; }
    public Dictionary<string, object> Parameters { get; }
    public DateTimeOffset Timestamp { get; }
}

public class SqlExecutedEventArgs : SqlExecutingEventArgs
{
    public TimeSpan Duration { get; }
    public int RowsAffected { get; }
    public Exception Exception { get; }  // Null if success
}

public class DecentDbContext
{
    // Events fire only when Logging=1 or when handlers attached
    public event EventHandler<SqlExecutingEventArgs> SqlExecuting;
    public event EventHandler<SqlExecutedEventArgs> SqlExecuted;
    
    // Optional ILogger for structured logging
    public ILogger Logger { get; set; }
}
```

### Usage Examples

**Development - Debug Logging:**
```csharp
var connectionString = "Data Source=my.db;Logging=1;LogLevel=Debug";
using var db = new DecentDbContext(connectionString);
db.Logger = loggerFactory.CreateLogger<DecentDbContext>();
// Logs: "Executing SQL: SELECT * FROM artists WHERE id = @p0"
```

**Production - Metrics Only:**
```csharp
var connectionString = "Data Source=my.db;Logging=1;LogLevel=Warning";
using var db = new DecentDbContext(connectionString);
db.SqlExecuted += (s, e) => metrics.RecordQueryTime(e.Duration, e.Sql);
```

**Production - Maximum Performance:**
```csharp
var connectionString = "Data Source=my.db;Logging=0";
using var db = new DecentDbContext(connectionString);
// Zero observability overhead
```

**ADR Required:** ADR-00XX: SQL Observability Strategy (events vs logging, performance guarantees)

---

## Connection String Parameters

### Overview

Connection strings configure database behavior at open time. All parameters have sensible defaults.

```
Data Source=/path/to.db;Cache Size=1024;Pooling=true;Command Timeout=30;Logging=0
```

### Tier 1: Essential Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `Data Source` | File path | **Required** | Path to DecentDB file. Created if doesn't exist. |
| `Cache Size` | Pages or MB | `1024` | Page cache size. Use `1024` (pages) or `64MB` format. |
| `Pooling` | `true` / `false` | `true` | Enable connection pooling for performance. |
| `Command Timeout` | Seconds | `30` | Default timeout for SQL commands. 0 = infinite. |
| `Logging` | `0` / `1` | `0` | Enable SQL logging. `0` = zero overhead. |
| `LogLevel` | `Verbose` / `Debug` / `Info` / `Warning` / `Error` | `Debug` | Minimum level for SQL log output. |

### Tier 2: Performance Tuning

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `Max Pool Size` | Integer | `10` | Maximum connections in pool. |
| `Checkpoint Threshold` | MB | `10` | Auto-checkpoint WAL when reaching this size. |
| `Checkpoint Timeout` | Seconds | `30` | Max time to wait for readers before forcing checkpoint. |
| `Busy Timeout` | Milliseconds | `5000` | How long to wait for database lock before timeout error. |

### Usage Examples

**Development (verbose debugging):**
```csharp
var connStr = "Data Source=dev.db;" +
              "Cache Size=256;" +              // Small cache
              "Logging=1;" +                   // Enable SQL logging
              "LogLevel=Verbose;" +            // All SQL logged
              "Pooling=false";                 // Disable pooling for simplicity
using var db = new DecentDbContext(connStr);
```

**Production (maximum performance):**
```csharp
var connStr = "Data Source=/data/prod.db;" +
              "Cache Size=4096;" +             // 16MB cache
              "Checkpoint Threshold=50MB;" +   // Less frequent checkpoints
              "Pooling=true;" +                // Connection reuse
              "Max Pool Size=20;" +            // Higher concurrency
              "Logging=0;" +                   // Zero logging overhead
              "Busy Timeout=10000";            // 10 second lock timeout
using var db = new DecentDbContext(connStr);
```

**Read-heavy analytics:**
```csharp
var connStr = "Data Source=prod.db;" +
              "Cache Size=8192;" +             // Large cache for hot data
              "Checkpoint Threshold=100MB;" +  // Infrequent checkpoints
              "Command Timeout=300;" +         // 5 minute timeout
              "Logging=0";
using var db = new DecentDbContext(connStr);
```

### Cache Size Guidelines

| Database Size | Cache Size | Pages | Memory |
|---------------|------------|-------|--------|
| < 100 MB | Default | 1024 | 4 MB |
| 100 MB - 1 GB | Medium | 4096 | 16 MB |
| > 1 GB | Large | 16384 | 64 MB |
| Analytics | XL | 65536 | 256 MB |

**Cache Size Units:**
- `Cache Size=1024` → 1024 pages (4KB each = 4MB)
- `Cache Size=64MB` → Automatically converted to pages

### Checkpointing Behavior

**When checkpoints occur:**
1. Manual: `PRAGMA checkpoint;` or CLI `decentdb checkpoint`
2. Auto-size: When WAL reaches `Checkpoint Threshold`
3. Auto-close: When connection closes (if `Checkpoint On Close=true`)

**Configuration guidance:**
- **Low write volume**: `Checkpoint Threshold=5MB` (frequent small checkpoints)
- **High write volume**: `Checkpoint Threshold=50MB` (fewer large checkpoints)
- **Read-heavy**: `Checkpoint Threshold=100MB` (minimal checkpoint overhead)

### Connection Pooling

**When to disable pooling:**
- Single-threaded applications
- Short-lived processes (CLI tools)
- Memory-constrained environments

**Pool behavior:**
- Readers: Multiple concurrent connections allowed
- Writers: Single writer with queue (DecentDB constraint)
- Idle timeout: 5 minutes (non-configurable in v1.0.0)

### Transaction Isolation Semantics

**Isolation Level Support:**
The specific isolation levels supported by DecentDB depend on the underlying engine's concurrency control mechanisms. Based on the document's mention of single-writer constraint and WAL (Write-Ahead Logging) behavior, DecentDB likely implements a form of snapshot isolation for readers.

| Isolation Level | DecentDB Behavior | Notes |
|-----------------|-------------------|-------|
| `ReadUncommitted` | Not supported | DecentDB prevents dirty reads by design |
| `ReadCommitted` | Default behavior | Readers see committed data only |
| `RepeatableRead` | Limited support | Consistency depends on engine implementation |
| `Serializable` | Serialized writes | Single writer constraint ensures serializability |
| `Snapshot` | Recommended | Best for read-heavy workloads |

**Implementation Details:**
- Single writer constraint ensures serialized write operations
- Multiple concurrent readers are supported (subject to engine limitations)
- Writers may block if another writer holds the write lock
- Reader transactions should not block write operations

**Connection String Configuration:**
```
Data Source=/path/to.db;IsolationLevel=Serializable
```

**Usage:**
```csharp
using var conn = new DecentDbConnection(connectionString);
conn.Open();

using var tx = conn.BeginTransaction(IsolationLevel.Serializable);
// Transaction behavior depends on DecentDB engine implementation
var data1 = await cmd1.ExecuteScalarAsync();
var data2 = await cmd2.ExecuteScalarAsync();
await tx.CommitAsync();
```

**Important Considerations:**
- Due to single-writer constraint, high-concurrency write scenarios may experience contention
- Read operations are generally non-blocking but behavior depends on engine implementation
- For embedded scenarios, the isolation characteristics are determined by DecentDB's core architecture
- Specific ADR required: ADR-00XX: Transaction Isolation Strategy (to document actual DecentDB behavior)

### Error Handling

**Invalid parameters:**
- `ArgumentException` on context creation
- Clear error message: "Invalid Cache Size 'abc'. Expected integer pages or MB format."

**Runtime configuration changes:**
Some parameters can be changed after opening:
```csharp
db.SetCacheSize(2048);           // Dynamically adjust cache
db.SetCommandTimeout(60);        // Change default timeout
// Checkpoint settings require reconnect
```

**ADR Required:** ADR-00XX: Connection String Design (parameter parsing, validation, defaults)

---

## API Surface (Public)

```csharp
// Core ADO.NET
namespace DecentDb.AdoNet
{
    public class DecentDbConnection : DbConnection { }
    public class DecentDbCommand : DbCommand { }
    public class DecentDbParameter : DbParameter { }
    public class DecentDbDataReader : DbDataReader { }
    public class DecentDbTransaction : DbTransaction { }
}

// Micro-ORM
namespace DecentDb.Orm
{
    public class DecentDbContext : IDisposable
    {
        public DecentDbContext(string dataSource);
        public DbSet<T> Set<T>() where T : class;
        public Task<int> SaveChangesAsync();
        public IDbTransaction BeginTransaction();
    }

    public class DbSet<T> : IQueryable<T> where T : class
    {
        // Query
        // id parameter: int, long, or Guid (matches entity's key type)
        public Task<T> GetAsync(object id);
        public Task<T> FirstAsync(Expression<Func<T, bool>> predicate);
        public Task<T> FirstOrDefaultAsync(Expression<Func<T, bool>> predicate);
        public Task<int> CountAsync(Expression<Func<T, bool>> predicate);
        public Task<List<T>> ToListAsync();
        
        // CRUD
        public Task InsertAsync(T entity);
        public Task UpdateAsync(T entity);
        public Task DeleteAsync(T entity);
        public Task InsertManyAsync(IEnumerable<T> entities);
        public Task DeleteManyAsync(Expression<Func<T, bool>> predicate);
    }

    // Extension methods for IQueryable<T>
    public static class DecentDbQueryableExtensions
    {
        public static IQueryable<T> Where<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate);
        public static IOrderedQueryable<T> OrderBy<T, TKey>(this IQueryable<T> source, Expression<Func<T, TKey>> keySelector);
        public static IOrderedQueryable<T> OrderByDescending<T, TKey>(this IQueryable<T> source, Expression<Func<T, TKey>> keySelector);
        public static IOrderedQueryable<T> ThenBy<T, TKey>(this IOrderedQueryable<T> source, Expression<Func<T, TKey>> keySelector);
        public static IQueryable<T> Skip<T>(this IQueryable<T> source, int count);
        public static IQueryable<T> Take<T>(this IQueryable<T> source, int count);
    }
}
```

---

## NuGet Package Distribution

### Package Structure

```
DecentDb.NET
├── lib/
│   └── net10.0/
│       ├── DecentDb.AdoNet.dll
│       ├── DecentDb.Orm.dll
│       └── DecentDb.Native.dll
├── runtimes/
│   ├── win-x64/
│   │   └── native/
│   │       └── decentdb.dll
│   ├── linux-x64/
│   │   └── native/
│   │       └── libdecentdb.so
│   ├── osx-x64/
│   │   └── native/
│   │       └── libdecentdb.dylib
│   └── osx-arm64/
│       └── native/
│           └── libdecentdb.dylib
└── DecentDb.NET.targets (MSBuild props for native libs)
```

### Installation

```bash
# Install from NuGet
dotnet add package DecentDb.NET

# Or via PackageReference
<PackageReference Include="DecentDb.NET" Version="1.0.0" />
```

### Runtime Native Library Resolution

**Automatic (MSBuild targets):**
- Native libraries copied to output directory on build
- Platform-specific subdirectory (`runtimes/{rid}/native/`)
- Runtime handles loading via `DllImport` resolution

**Manual (optional):**
```csharp
// If native lib is in custom location
DecentDbNative.SetLibraryPath("/usr/local/lib/libdecentdb.so");
```

### Build Process

1. **Compile Nim to native libraries**
   ```bash
   # Windows
   nim c -d:release --app:lib --out:decentdb.dll src/c_api.nim
   
   # Linux
   nim c -d:release --app:lib --out:libdecentdb.so src/c_api.nim
   
   # macOS
   nim c -d:release --app:lib --out:libdecentdb.dylib src/c_api.nim
   ```

2. **Build C# projects**
   ```bash
   dotnet build -c Release
   dotnet pack -c Release
   ```

3. **CI/CD Pipeline**
   - Build native libraries on Windows, Linux, macOS runners
   - Collect all platform binaries
   - Pack into single multi-platform NuGet package
   - Publish to NuGet.org

---

## Success Criteria

### Functionality
1. ✅ Can open DecentDB file from C# and execute SQL via Dapper
2. ✅ Can use LINQ syntax with OrderBy/ThenBy and Skip/Take for pagination
3. ✅ Zero configuration required (convention-based)
4. ✅ All CRUD operations work with async/await
5. ✅ No attribute decoration required on POCOs
6. ✅ Cross-platform support (Windows, Linux, macOS)
7. ✅ VARCHAR(n) DDL support with length validation at engine layer
8. ✅ MaxLength attribute enforces constraints in C# layer

### Performance (Critical)
9. ✅ Single record query: < 2ms (P95)
10. ✅ Filtered list query: < 10ms + 0.5ms/row (P95)
11. ✅ Paginated + Sorted query: < 20ms + 0.5ms/row (P95)
12. ✅ C# layer overhead: < 1ms over native DecentDB execution
13. ✅ Query compilation cached (no re-parsing on identical queries)
14. ✅ Materialization uses compiled expressions (no reflection per row)

---

## Open Questions - Recommendations

### 1. Pluralization: Simple "s" Suffix vs Full Inflection

**Recommendation: Simple "s" suffix with escape hatch**

**Analysis:**
- **Simple "s" (95% accurate)**: Artist → artists, Song → songs, Album → albums
- **Full inflection library**: Person → people, Category → categories, but adds dependency complexity

**Decision:**
```csharp
// Default: Simple "s" suffix (zero dependencies)
public class Artist { }  // → table "artists"
public class Song { }    // → table "songs"

// Edge cases: Use [Table] attribute (explicit override)
[Table("people")]
public class Person { }  // Override default "persons"

[Table("categories")]
public class Category { }  // Override default "categorys"
```

**Rationale:**
- Avoids external dependency (Humanizer or similar ~100KB+ dependency)
- 95% of use cases work with simple "s"
- Remaining 5% handled by explicit attribute (clearer intent)
- Performance: No inflection computation at runtime
- Convention over configuration: Simple rule that's easy to remember

**Implementation:**
```csharp
public static string ToTableName(Type type)
{
    var name = type.Name.ToLowerInvariant();
    return name.EndsWith("s") ? name : name + "s";
}
```

---

### 2. DateTime: Unix Epoch vs ISO 8601 Strings

**Recommendation: Unix epoch milliseconds (INT64) with UTC enforcement**

**Analysis:**

| Approach | Storage | Comparison | Indexing | Human Readable | Precision |
|----------|---------|------------|----------|----------------|-----------|
| Unix epoch (ms) | 8 bytes | O(1) numeric | Excellent | No | Millisecond |
| ISO 8601 | Variable (20-30 bytes) | String sort | Good | Yes | Microsecond |

**Decision:**
```csharp
// Storage: Unix epoch milliseconds (INT64)
// C#: DateTime.UtcNow → long milliseconds
// DecentDB: INT64

// Automatic UTC conversion
public class Event
{
    public long Id { get; set; }
    
    // Stored as INT64 milliseconds since Unix epoch
    public DateTime CreatedAt { get; set; }  // Always UTC
}

// Range queries work perfectly (numeric comparison)
var recent = db.Events
    .Where(e => e.CreatedAt > DateTime.UtcNow.AddDays(-7))
    .OrderBy(e => e.CreatedAt)
    .ToList();
```

**Rationale:**
- **Performance**: INT64 comparison 10x faster than string parsing
- **Storage**: 8 bytes vs 24+ bytes for ISO string
- **Indexing**: B+Tree handles numeric ranges optimally
- **Sorting**: Native numeric sort (no collation needed)
- **Timezone safety**: Force UTC at boundary, prevent local time errors
- **Range queries**: WHERE created_at > 1704067200000 (efficient)

**Type Converter:**
```csharp
public class DateTimeConverter : ITypeConverter<DateTime, long>
{
    public long ConvertTo(DateTime value) 
        => new DateTimeOffset(value.ToUniversalTime()).ToUnixTimeMilliseconds();
    
    public DateTime ConvertFrom(long value) 
        => DateTimeOffset.FromUnixTimeMilliseconds(value).UtcDateTime;
}
```

**Trade-off:** Human readability in raw SQL queries is lost, but this is acceptable for an embedded database where direct SQL inspection is rare.

---

### 3. Connection Pooling: Custom vs Dapper

**Recommendation: Custom lightweight pool with writer/reader separation**

**Analysis:**

Dapper doesn't provide connection pooling - it relies on ADO.NET provider's pooling or creates new connections per query. For DecentDB (embedded, single writer), we need specialized handling.

**Decision:**

```csharp
// Custom pool optimized for DecentDB's single-writer constraint
public class DecentDbConnectionPool
{
    // Separate pools for readers and writers
    private readonly ConcurrentBag<DecentDbConnection> _readerPool;
    private readonly SemaphoreSlim _writerLock;  // Max 1 writer
    
    public async Task<DecentDbConnection> GetReaderAsync()
    {
        // Reuse existing reader connection
        if (_readerPool.TryTake(out var conn))
            return conn;
        
        // Create new reader (snapshot isolation)
        return await CreateConnectionAsync(readOnly: true);
    }
    
    public async Task<IDisposable> AcquireWriterAsync()
    {
        await _writerLock.WaitAsync();
        return new WriterLease(CreateConnectionAsync(readOnly: false), _writerLock);
    }
}

// Usage
public class DecentDbContext
{
    public async Task<List<T>> QueryAsync<T>(Expression<Func<T, bool>> predicate)
    {
        using var conn = await _pool.GetReaderAsync();
        // Execute query...
    }
    
    public async Task InsertAsync<T>(T entity)
    {
        using var writer = await _pool.AcquireWriterAsync();
        // Execute insert...
    }
}
```

**Rationale:**
- **DecentDB constraint**: Single writer requires explicit coordination
- **Performance**: Reuse native handles, avoid open/close overhead (~1ms)
- **Snapshot isolation**: Readers get consistent view via saved LSN
- **Resource management**: Controlled connection lifecycle
- **Scalability**: Many concurrent readers, one writer queue

**Pool Configuration:**
```csharp
public class PoolOptions
{
    public int MaxReaderConnections { get; set; } = 10;
    public int MaxWriterQueueDepth { get; set; } = 100;
    public TimeSpan ConnectionIdleTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public bool AutoCheckpoint { get; set; } = true;
}
```

**Alternative:** If keeping it simple for v1.0.0, use "no pooling" (create/close per operation) with connection string parameter `Pooling=false`. This works fine for low-concurrency scenarios (<100 ops/sec).

---

### 4. Query Caching: Cache Granularity

**Recommendation: Two-tier caching (Expression hash → SQL, and SQL → Execution plan)**

**Analysis:**

LINQ query compilation is expensive (Expression tree parsing + SQL generation). We need aggressive caching.

**Decision:**

```csharp
public class QueryCache
{
    // Tier 1: Expression tree → SQL string (expensive parsing)
    private readonly ConcurrentDictionary<int, CachedQuery> _expressionCache;
    
    // Tier 2: SQL string → Prepared statement (DB side)
    // Managed by DecentDB Nim layer
    
    public async Task<QueryResult<T>> ExecuteAsync<T>(
        Expression<Func<T, bool>> predicate,
        QueryContext context)
    {
        var hash = ComputeHash(predicate);
        
        if (!_expressionCache.TryGetValue(hash, out var cached))
        {
            // Compile once, cache forever
            var sql = CompileExpression(predicate);
            var parameterExtractor = CreateParameterExtractor(predicate);
            
            cached = new CachedQuery(sql, parameterExtractor);
            _expressionCache[hash] = cached;
        }
        
        // Reuse compiled SQL with new parameter values
        var parameters = cached.ExtractParameters(context);
        return await ExecuteSqlAsync(cached.Sql, parameters);
    }
}

// Cache key: Expression tree structure (not parameter values)
private int ComputeHash<T>(Expression<Func<T, bool>> expression)
{
    // Hash: Member names, operators, method calls
    // NOT: Constant values (parameters)
    var visitor = new ExpressionHashVisitor();
    visitor.Visit(expression);
    return visitor.GetHashCode();
}
```

**Cache Levels:**

| Level | Key | Value | Lifetime | Memory |
|-------|-----|-------|----------|--------|
| L1 | Expression hash (int) | SQL string + parameter extractor | App lifetime | ~1KB/query |
| L2 | SQL string | Prepared statement handle | Connection lifetime | ~4KB/query |

**Cache Invalidation:**
```csharp
// Never invalidate L1 (expressions are immutable)
// L2 auto-invalidated on connection close

// Optional: LRU eviction for L1 if memory pressure
public class LruQueryCache
{
    private readonly int _maxEntries = 1000;
    private readonly ConcurrentDictionary<int, LinkedListNode<CachedQuery>> _cache;
    private readonly LinkedList<CachedQuery> _lruList;
    
    public void Add(int hash, CachedQuery query)
    {
        if (_cache.Count >= _maxEntries)
        {
            // Evict oldest
            var oldest = _lruList.Last;
            _cache.TryRemove(ComputeHash(oldest.Value), out _);
            _lruList.RemoveLast();
        }
        
        var node = _lruList.AddFirst(query);
        _cache[hash] = node;
    }
}
```

**Rationale:**
- **Granularity**: Per-expression-type (not per-query-instance)
- **Performance**: Same LINQ query with different parameters = cache hit
- **Memory**: Bounded cache with LRU eviction
- **Thread safety**: ConcurrentDictionary for L1 (read-heavy)
- **Prepared statements**: DecentDB Nim layer handles SQL → execution plan

**Benchmark targets:**
- Cache hit: < 0.1ms (parameter extraction only)
- Cache miss: 1-5ms (full expression compilation)
- Hit ratio target: > 95% for typical app workloads

---

## Dependencies

- .NET 10 (current version, no .NET Framework support)
- Dapper 2.0+ (optional, supported)
- No Entity Framework Core dependency
- Nim runtime (statically linked into native DLL)

---

## Risks

1. **P/Invoke Performance**: Marshaling overhead may impact performance (mitigate with batching)
2. **Expression Tree Complexity**: Full LINQ support is complex (mitigate with phased delivery)
3. **Memory Management**: Nim/C# boundary requires careful cleanup (mitigate with IDisposable patterns)
4. **Thread Safety**: DecentDB single writer constraint must be enforced (mitigate with connection pooling)

---

## Future Considerations (Post-1.0.0)

- Entity Framework Core provider
- Code-first migrations
- Change tracking (Unit of Work pattern)
- Lazy loading
- Include/eager loading
- Raw SQL with LINQ composition
- Stored procedure support
