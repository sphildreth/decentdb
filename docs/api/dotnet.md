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

# Optional: design-time services for `dotnet ef`
dotnet add package DecentDB.EntityFrameworkCore.Design
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

### Entity Framework Core

The EF Core provider follows the standard provider pattern and is configured via
`DbContextOptionsBuilder.UseDecentDB(...)`.

For most apps, the simplest setup is to build the connection string with
`DecentDBConnectionStringBuilder` and pass that string into EF Core:

```csharp
using DecentDB.AdoNet;
using Microsoft.EntityFrameworkCore;

var csb = new DecentDBConnectionStringBuilder
{
    DataSource = "/path/to/shop.ddb",
    CommandTimeout = 120,
    Logging = false,
};

var options = new DbContextOptionsBuilder<ShopContext>()
    .UseDecentDB(csb.ConnectionString)
    .Options;

await using var db = new ShopContext(options);
await db.Database.EnsureCreatedAsync();
```

Example `DbContext`:

```csharp
using Microsoft.EntityFrameworkCore;

public sealed class ShopContext(DbContextOptions<ShopContext> options)
    : DbContext(options)
{
    public DbSet<Product> Products => Set<Product>();
    public DbSet<Cart> Carts => Set<Cart>();
}

public sealed class Product
{
    public long Id { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

public sealed class Cart
{
    public long Id { get; set; }
    public string CustomerEmail { get; set; } = string.Empty;
}
```

If you prefer dependency injection, register DecentDB the same way you would any
other EF Core provider:

```csharp
using DecentDB.AdoNet;
using Microsoft.EntityFrameworkCore;

var csb = new DecentDBConnectionStringBuilder
{
    DataSource = "/path/to/shop.ddb",
};

builder.Services.AddDbContext<ShopContext>(options =>
    options.UseDecentDB(csb.ConnectionString));
```

You can also pass an existing `DbConnection` instead of a connection string:

```csharp
using DecentDB.AdoNet;
using Microsoft.EntityFrameworkCore;

using var connection = new DecentDBConnection("Data Source=/path/to/shop.ddb");

var options = new DbContextOptionsBuilder<ShopContext>()
    .UseDecentDB(connection, contextOwnsConnection: false)
    .Options;
```

#### Connection string builder

`DecentDBConnectionStringBuilder` exposes the same connection string surface used
by the ADO.NET and EF Core providers:

```csharp
var csb = new DecentDBConnectionStringBuilder
{
    DataSource = "/path/to/shop.ddb",
    CacheSize = "268435456",   // optional native cache size
    Logging = true,            // optional SQL logging
    LogLevel = "Info",         // optional log level
    CommandTimeout = 120,      // default command timeout in seconds
};

string connectionString = csb.ConnectionString;
```

The EF Core provider also accepts the builder directly:

```csharp
var options = new DbContextOptionsBuilder<MyDbContext>()
    .UseDecentDB(csb)
    .Options;
```

#### Design-time services and migrations

If you use `dotnet ef` for design-time services, install
`DecentDB.EntityFrameworkCore.Design` alongside the runtime provider:

```bash
dotnet add package DecentDB.EntityFrameworkCore
dotnet add package DecentDB.EntityFrameworkCore.Design
```

Then use the usual EF Core workflow:

```bash
dotnet ef migrations add InitialCreate
dotnet ef database update
```

The in-tree EF Core provider tests cover runtime migration application plus the
provider SQL generation paths for table rename, column rename, column type
change, and index drop operations.

#### EF Core provider coverage highlights

The current in-tree provider validation covers:

- server-side translation for representative `Union`, `Concat`, `Intersect`, and
  `Except` query shapes
- provider-specific window functions exposed via `EF.Functions`
- `ExecuteUpdateAsync()` and `ExecuteDeleteAsync()` rowcount and persistence
- `AsAsyncEnumerable()` over composed queries
- optimistic concurrency conflicts surfaced as `DbUpdateConcurrencyException`
- database execution failures surfaced as `DbUpdateException` with inner
  `DecentDB.Native.DecentDBException`
- builder-driven provider configuration via `UseDecentDB(DecentDBConnectionStringBuilder)`
- performance-sanity coverage for `AsNoTracking`, split-query includes, keyset
  pagination, async streaming, and bulk mutation rowcount behavior

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

Savepoints are currently unsupported in the EF Core relational transaction
surface. `SupportsSavepoints` is `false`, and savepoint APIs intentionally throw
`NotSupportedException`.

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

// File-backed vacuum/compaction helper
await DecentDBMaintenance.VacuumAtomicAsync("/path/to/shop.ddb");
```

## Performance sanity guidance

The in-tree `DecentDb.ShowCase` sample includes a `PERFORMANCE PATTERNS`
section, but it should be read as a sanity-check aid rather than a benchmark
suite. The current showcase and tests intentionally focus on:

- projection vs tracked reads
- `AsNoTracking()` for read-mostly paths
- `AsSplitQuery()` over included relationship graphs
- keyset-style paging
- async materialization vs `AsAsyncEnumerable()` result ordering
- bulk update/delete rowcount sanity

These checks are meant to catch obviously pathological provider behavior and to
teach reasonable defaults for embedded workloads. They are not claims of
cross-provider performance parity.

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
