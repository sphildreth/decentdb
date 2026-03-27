# .NET bindings

DecentDB ships several in-tree .NET packages under `bindings/dotnet/`.

## Package surfaces

The current .NET source tree includes:

- `bindings/dotnet/src/DecentDB.Native` — native library loading, P/Invoke
  layer over the stable `ddb_*` C ABI, and high-level `DecentDB` / `PreparedStatement`
  classes
- `bindings/dotnet/src/DecentDB.AdoNet` — ADO.NET provider
- `bindings/dotnet/src/DecentDB.MicroOrm` — LINQ-style Micro ORM
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore` — EF Core provider
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore.Design` — design-time
  services
- `bindings/dotnet/src/DecentDB.EntityFrameworkCore.NodaTime` — NodaTime
  integration

Tests and benchmarks live alongside the packages under `bindings/dotnet/tests/`
and `bindings/dotnet/benchmarks/`.

## C ABI coverage

The .NET binding declares and exposes **all 50 C ABI functions** defined in
`include/decentdb.h`. Performance-critical paths (batch execution, fused
bind+step, re-execute, zero-copy row views) are exposed through
`DecentDBNativeUnsafe` and wrapped by the `PreparedStatement` class.

## Use via NuGet

For normal application development, prefer the published NuGet packages. You do
not need to build DecentDB from source or download native binaries separately
just to consume the .NET provider surface in your project.

```bash
dotnet add package DecentDB.AdoNet
dotnet add package DecentDB.MicroOrm
dotnet add package DecentDB.EntityFrameworkCore
```

## Opening a database

```csharp
using DecentDB.Native;

// Open or create (default)
using var db = new DecentDB("/path/to/data.ddb");

// Create only — throws DecentDBException if file exists
using var db = new DecentDB("/path/to/data.ddb", DbOpenMode.Create);

// Open only — throws DecentDBException if file doesn't exist
using var db = new DecentDB("/path/to/data.ddb", DbOpenMode.Open);
```

### ADO.NET connection

```csharp
using DecentDB.AdoNet;

using var conn = new DecentDBConnection("Data Source=/path/to/data.ddb");
conn.Open();
// ... use conn ...
conn.Close();
```

## Version introspection

```csharp
uint abi = DecentDB.AbiVersion();       // e.g. 1
string ver = DecentDB.EngineVersion();  // e.g. "2.0.0"

// Via ADO.NET
uint abi = DecentDBConnection.AbiVersion();
string ver = DecentDBConnection.EngineVersion();
```

## Prepared statements (Native API)

```csharp
using var stmt = db.Prepare("INSERT INTO users (name, email) VALUES ($1, $2)");
stmt.BindText(1, "Alice").BindText(2, "alice@example.com");
stmt.StepRowsAffected();

// Point read
using var point = db.Prepare("SELECT id, name FROM users WHERE id = $1");
point.BindInt64(1, 42);
if (point.Step() == 1)
{
    long id = point.GetInt64(0);
    string name = point.GetText(1);
}
```

## Re-execute fast path (V2)

Re-execute combines reset + bind + execute + affected rows into a single native
call, eliminating 4 P/Invoke crossings:

```csharp
using var update = db.Prepare("UPDATE counters SET val = val + 1 WHERE id = $1");
update.BindInt64(1, 1).StepRowsAffected(); // initial execute

// Re-execute with different parameter — single native call
long affected = update.RebindInt64Execute(2);
```

Variants:
- `RebindInt64Execute(long value)` — rebind first parameter as int64
- `RebindTextInt64Execute(byte[] utf8, long intValue)` — rebind two params
- `RebindInt64TextExecute(long intValue, byte[] utf8)` — rebind two params

## Batch execution (V2)

```csharp
using var stmt = db.Prepare("INSERT INTO items (id) VALUES ($1)");
long[] ids = [1, 2, 3, 4, 5];
long affected = stmt.ExecuteBatchInt64(ids);
```

## Transactions

```csharp
// Using methods
db.BeginTransaction();
// ... execute statements ...
db.CommitTransaction();

// Check transaction state (queries engine directly)
bool inTxn = db.InTransaction; // true during transaction

// Using ADO.NET
using var txn = conn.BeginTransaction();
// ... execute commands ...
txn.Commit();
```

## Schema introspection (V2)

```csharp
// Tables
string tablesJson = db.ListTablesJson();              // ["users", "orders"]
string colsJson = db.GetTableColumnsJson("users");     // [{"name":"id",...}, ...]
string ddl = db.GetTableDdl("users");                  // CREATE TABLE ...
string indexesJson = db.ListIndexesJson();

// Views
string viewsJson = db.ListViewsJson();                 // ["v_active_users"]
string viewDdl = db.GetViewDdl("v_active_users");      // CREATE VIEW ...

// Triggers
string triggersJson = db.ListTriggersJson();

// Via ADO.NET
string ddl = conn.GetTableDdl("users");
string views = conn.ListViewsJson();
string triggers = conn.ListTriggersJson();
bool inTxn = conn.InTransaction;
```

## Native types

The binding supports all DecentDB native types:

| C# Type | DecentDB Type | Bind | Read |
|---------|--------------|------|------|
| `long` | INT64 | `BindInt64()` | `GetInt64()` |
| `double` | FLOAT64 | `BindFloat64()` | `GetFloat64()` |
| `bool` | BOOL | `BindBool()` | `GetBool()` |
| `string` | TEXT | `BindText()` | `GetText()` |
| `decimal` | DECIMAL | `BindDecimal()` | `GetDecimal()` |
| `byte[]` | BLOB | `BindBlob()` | `GetBlob()` |
| `Guid` | UUID | `BindGuid()` | `GetGuid()` |
| `DateTime` | TIMESTAMP | `BindDatetime(micros)` | `GetTimestampMicros()` / `GetValueObject()` |

## Maintenance

```csharp
// Checkpoint (flush WAL)
conn.Checkpoint();

// Online backup
conn.SaveAs("/path/to/backup.ddb");
```

## Build the native library

From the repository root:

```bash
cargo build -p decentdb
```

## Run the in-tree .NET test suite

```bash
cd bindings/dotnet
dotnet test DecentDB.NET.sln -v minimal
```

## Run the V2 benchmarks

```bash
cd bindings/dotnet
DECENTDB_NATIVE_LIB_PATH=../../target/release/libdecentdb.so \
  dotnet run -c Release --project benchmarks/DecentDB.BenchmarksV2/
```

The BenchmarksV2 project demonstrates all V2 features: version API, connection
modes, schema introspection, transaction state, native types (DECIMAL, UUID,
TIMESTAMP), insert throughput vs SQLite, point reads, re-execute fast paths,
full table scans, and maintenance operations.
