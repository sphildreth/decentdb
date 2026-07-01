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

The .NET binding declares the C ABI functions used by the native, ADO.NET, EF
Core, and MicroORM surfaces. Performance-critical paths (batch execution, fused
bind+step, re-execute, zero-copy row views) are exposed through
`DecentDBNativeUnsafe` and wrapped by the `PreparedStatement` class.

ADO.NET connection strings pass native cross-process coordination and
write-queue options through to the C ABI. Coordination keys are
`Process Coordination` (`auto`, `required`, or `single_process_unsafe`) and
`Process Coordination Timeout Ms`. Write-queue keys are `Write Queue Enabled`,
`Write Queue Capacity`, `Write Queue Default Timeout Ms`,
`Write Queue Strict Group Commit`, `Write Queue Max Batch`, and
`Write Queue Max Group Delay Us`. The `DecentDB.Native.DecentDB` class also
accepts native option strings, exposes `ExecuteQueued(sql)` for self-contained
queued SQL, and exposes
`WriteQueueMetrics()` for native queue counters. ADO.NET prepared statements
remain on the direct prepared path until the C ABI adds queued
prepared-statement execution.

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

var csb = new DecentDBConnectionStringBuilder
{
    DataSource = "/path/to/data.ddb",
    ProcessCoordination = "required",
    ProcessCoordinationTimeoutMs = 30000,
};

using var conn = new DecentDBConnection(csb.ConnectionString);
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
    PerformanceProfile = "embedded_fast", // optional durable profile for hot embedded apps
    CacheSize = "64MB",        // optional native cache size
    RetainPagedRowSourcesAfterCommit = true,
    PagedRowStorage = false,
    WalAutoCheckpoint = "0",
    Logging = true,            // optional SQL logging
    LogLevel = "Info",         // optional log level
    CommandTimeout = 120,      // default command timeout in seconds
};

string connectionString = csb.ConnectionString;
```

For a single-process embedded application with a hot working set, start with
`PerformanceProfile = "embedded_fast"` instead of rediscovering individual
storage knobs. It preserves durable WAL sync while increasing the cache,
retaining hot row sources across commits, using the lower-overhead row-source
layout for repeated writes, and disabling size-triggered auto-checkpoints. You
can still override any individual option, for example `CacheSize = "64MB"`.
Use `ProcessCoordination = "single_process_unsafe"` only when one OS process will
open the database file.

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
uint abi = DecentDB.AbiVersion();       // e.g. 4
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

// Stable tooling metadata
string metadataJson = db.GetToolingMetadataJson();
string contractJson = db.DescribeQueryJson("SELECT id FROM users WHERE id = $1");

// Via ADO.NET
string ddl = conn.GetTableDdl("users");
string views = conn.ListViewsJson();
string triggers = conn.ListTriggersJson();
string metadata = conn.GetToolingMetadataJson();
string contract = conn.DescribeQueryJson("SELECT id FROM users WHERE id = $1");
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
| `DecentDBEnumValue` | ENUM | string labels in column context | `GetValueObject()` |
| `string` | IPADDR / CIDR / MACADDR | `BindText()` in column context | `GetText()` / `GetValueObject()` |
| `DateOnly` | DATE | integer day count or text in column context | `GetValueObject()` |
| `TimeOnly` | TIME | integer microseconds or text in column context | `GetValueObject()` |
| `DateTimeOffset` | TIMESTAMPTZ | UTC microseconds or text in column context | `GetValueObject()` |
| `DecentDBIntervalValue` / `TimeSpan` | INTERVAL | integer microseconds or text in column context | `GetValueObject()` |

The low-level native value object path returns semantic values without requiring
applications to parse display strings. `ENUM` values expose stable type and
label ids; catalog metadata carries the human-readable label mapping.

## Maintenance

```csharp
// Checkpoint committed WAL frames into the database file
conn.Checkpoint();

// Online backup
conn.SaveAs("/path/to/backup.ddb");

// Binding-native file maintenance helpers
await DecentDBMaintenance.CheckpointAsync("/path/to/shop.ddb");
await DecentDBMaintenance.CompactAsync("/path/to/shop.ddb", "/path/to/shop.compact.ddb");
await DecentDBMaintenance.VacuumAsync("/path/to/shop.ddb", createBackup: true);
```

## Sync SDK

The .NET binding exposes an engine-local sync surface through
`DecentDB.Native` and `DecentDB.AdoNet`. Use it to initialize replicas, manage
peers and scopes, inspect doctor output, and exchange batches without shelling
out to the CLI.

- Sync quickstart sample: `bindings/dotnet/examples/sync-quickstart.md`

`DecentDBConnection.Sync` returns `DecentDB.Native.DecentDBSyncClient`. The
same client is also exposed from `DecentDB.Native.DecentDB.Sync`.

### Core surface

`DecentDBSyncClient` includes:

- `ExecuteRawJson` / `ExecuteRawJsonAsync`
- `GetStatus` / `GetStatusAsync`
- `InitializeReplica` / `InitializeReplicaAsync`
- `SetEnabled` / `SetEnabledAsync`
- `GetPendingChanges` / `GetPendingChangesAsync`
- `ExportBatch` / `ExportBatchAsync`
- `ImportBatch` / `ImportBatchAsync`
- `AddPeer` / `AddPeerAsync`
- `RemovePeer` / `RemovePeerAsync`
- `ListPeers` / `ListPeersAsync`
- `CreateScope` / `CreateScopeAsync`
- `DropScope` / `DropScopeAsync`
- `ListScopes` / `ListScopesAsync`
- `BindPeerScope` / `BindPeerScopeAsync`
- `UnbindPeerScope` / `UnbindPeerScopeAsync`
- `ListPeerScopeBindings` / `ListPeerScopeBindingsAsync`
- `ListSessions` / `ListSessionsAsync`
- `ListConflicts` / `ListConflictsAsync`
- `GetConflict` / `GetConflictAsync`
- `ResolveConflict` / `ResolveConflictAsync`
- `ReopenConflict` / `ReopenConflictAsync`
- `GetConflictPolicy` / `GetConflictPolicyAsync`
- `SetConflictPolicy` / `SetConflictPolicyAsync`
- `GetDoctorReport` / `GetDoctorReportAsync`
- `GetRetentionReport` / `GetRetentionReportAsync`
- `GetPeerLag` / `GetPeerLagAsync`
- `Prune` / `PruneAsync`
- `CreateChangeset` / `CreateChangesetAsync`
- `InspectChangeset` / `InspectChangesetAsync`
- `ApplyChangeset` / `ApplyChangesetAsync`
- `InvertChangeset` / `InvertChangesetAsync`

Returned models include:

- `SyncStatus`
- `SyncJournalRecord`
- `SyncChangeBatch`
- `SyncImportSummary`
- `SyncPeer`
- `SyncScope`
- `SyncPeerScopeBinding`
- `SyncSession`
- `SyncConflict`
- `SyncConflictPolicyConfig`
- `SyncOperationalDoctorReport`
- `SyncRetentionReport`
- `SyncPeerLag`
- `SyncPruneSummary`

### Example usage

```csharp
using System.Collections.Generic;
using DecentDB.AdoNet;
using DecentDB.Native;

await using var connection = new DecentDBConnection("Data Source=/tmp/app.ddb");
await connection.OpenAsync();

await connection.Sync.InitializeReplicaAsync("node-a");

await connection.Sync.AddPeerAsync(new SyncPeer
{
    Name = "central",
    Endpoint = "http://127.0.0.1:43123",
    TokenEnv = "DECENTDB_SYNC_TOKEN"
});

await connection.Sync.CreateScopeAsync(new SyncScope
{
    Name = "tenant_42",
    IncludeTables = new List<string> { "accounts", "orders" },
    RowFilter = "tenant_id = 42"
});

await connection.Sync.BindPeerScopeAsync("central", "tenant_42");

var batch = await connection.Sync.ExportBatchAsync(since: 0, limit: 100);
var summary = await connection.Sync.ImportBatchAsync(batch);
var doctor = await connection.Sync.GetDoctorReportAsync();
var retention = await connection.Sync.GetRetentionReportAsync();
var conflicts = await connection.Sync.ListConflictsAsync();
var raw = await connection.Sync.ExecuteRawJsonAsync("{\"op\":\"status\"}");
```

Changeset helpers accept and return `JsonElement` so .NET applications can use
the stable JSON envelope while typed models evolve:

```csharp
using System.Text.Json;

var options = JsonSerializer.Deserialize<JsonElement>(
    "{\"source\":{\"kind\":\"checkpoint\",\"peer\":\"relay\",\"since_sequence\":0}}");
var changeset = await connection.Sync.CreateChangesetAsync(options);
var inspection = await connection.Sync.InspectChangesetAsync(changeset);
var applyResult = await connection.Sync.ApplyChangesetAsync(changeset);
```

## Performance guidance

### Embedded profile

Use an explicit profile before comparing DecentDB to a tuned SQLite connection.
For a durable .NET application, start with:

```csharp
var csb = new DecentDBConnectionStringBuilder
{
    DataSource = "/path/to/app.ddb",
    PerformanceProfile = "embedded_fast",
    CacheSize = "64MB",
};
```

`embedded_fast` keeps durable WAL sync enabled, raises the cache, keeps hot row
sources across commits, uses the lower-overhead row-source layout for repeated
access, and disables size-triggered auto-checkpoints. Use
`ProcessCoordination = "single_process_unsafe"` only when one OS process can
open the database file.

When comparing to SQLite `PRAGMA synchronous = NORMAL`, make the durability
tradeoff explicit:

```csharp
var csb = new DecentDBConnectionStringBuilder
{
    DataSource = "/path/to/app.ddb",
    PerformanceProfile = "embedded_fast",
    CacheSize = "64MB",
    WalAutoCheckpoint = "0",
    ProcessCoordination = "single_process_unsafe",
};

var connectionString = csb.ConnectionString
    + ";wal_sync_mode=async_commit:10"
    + ";plan_cache_max_bytes=2097152";
```

`async_commit` can acknowledge recent commits before the covering fsync. Use it
only when that bounded post-crash durability window is acceptable, and run
`connection.Checkpoint()` at controlled application or benchmark boundaries.

### ADO.NET hot loops

For repeated inserts, updates, and point reads, keep one `DbCommand`, create its
parameters once, call `Prepare()`, and mutate parameter values inside the loop.
Creating a new command and parameter objects for every row can dominate a small
statement benchmark even though the provider also has connection-level statement
and plan caches.

```csharp
using System.Data;
using DecentDB.AdoNet;

using var connection = new DecentDBConnection(connectionString);
connection.Open();

using var transaction = connection.BeginTransaction();
using var command = connection.CreateCommand();
command.Transaction = transaction;
command.CommandText = """
    INSERT INTO events (id, category, amount)
    VALUES (@id, @category, @amount)
    """;

var id = command.CreateParameter();
id.ParameterName = "@id";
id.DbType = DbType.Int64;
command.Parameters.Add(id);

var category = command.CreateParameter();
category.ParameterName = "@category";
category.DbType = DbType.String;
command.Parameters.Add(category);

var amount = command.CreateParameter();
amount.ParameterName = "@amount";
amount.DbType = DbType.Double;
command.Parameters.Add(amount);

command.Prepare();

foreach (var row in rows)
{
    id.Value = row.Id;
    category.Value = row.Category;
    amount.Value = row.Amount;
    command.ExecuteNonQuery();
}

transaction.Commit();
```

ADO.NET accepts normal named parameters such as `@id`; the provider rewrites them
to DecentDB's native positional parameters internally. Prefer provider
parameters over ad-hoc SQL string replacement.

#### Typed batch inserts

For import tools and benchmark loops where every row has the same primitive
shape, `DecentDBConnection.ExecutePreparedBatchTyped(...)` exposes the native
typed batch path without manually managing a native statement. The API uses
DecentDB positional parameters (`$1`, `$2`, ...) and a NUL-terminated ASCII
signature:

- `i` for INT64 values
- `b` for BOOLEAN values, supplied as `0` for false and non-zero for true in the
  INT64 array
- `f` for FLOAT64 values
- `t` for UTF-8 TEXT byte arrays

The value arrays are flat and row-major for each type. For signature `itfb`,
each row contributes two INT64 slots (`i` and `b`), one FLOAT64 slot, and one
TEXT byte array:

```csharp
using System.Text;
using DecentDB.AdoNet;

using var tx = connection.BeginTransaction();

long affected = connection.ExecutePreparedBatchTyped(
    """
    INSERT INTO events (id, category, amount, active)
    VALUES ($1, $2, $3, $4)
    """,
    Encoding.ASCII.GetBytes("itfb\0"),
    rowCount: 3,
    i64Values: new long[] { 1, 1, 2, 0, 3, 1 },
    f64Values: new double[] { 10.5, 20.0, 30.25 },
    textValues: new[]
    {
        Encoding.UTF8.GetBytes("alpha"),
        Encoding.UTF8.GetBytes("beta"),
        Encoding.UTF8.GetBytes("gamma"),
    });

tx.Commit();
```

Use this only for hot homogeneous batches. It bypasses normal `DbParameter`
objects, so the caller owns UTF-8 encoding, array sizing, boolean encoding, and
matching the signature to the SQL parameter order.

#### Runnable mini benchmark shape

This console-program-sized example shows the expected ADO.NET benchmark shape:
one prepared insert command, one prepared point-read command, `ExplainQuery`, and
an explicit checkpoint boundary.

```bash
dotnet new console -n DecentDbMiniBench
cd DecentDbMiniBench
dotnet add package DecentDB.AdoNet --prerelease
```

Replace `Program.cs` with:

```csharp
using System.Data;
using System.Diagnostics;
using DecentDB.AdoNet;

const int RowCount = 50_000;
const int ReadCount = 100_000;
var path = Path.Combine(Path.GetTempPath(), "decentdb-mini-bench.ddb");

DecentDBConnection.DeleteDatabaseFiles(path);

var csb = new DecentDBConnectionStringBuilder
{
    DataSource = path,
    PerformanceProfile = "embedded_fast",
    CacheSize = "64MB",
    ProcessCoordination = "single_process_unsafe",
    WalAutoCheckpoint = "0",
};

using var connection = new DecentDBConnection(csb.ConnectionString);
connection.Open();

using (var schema = connection.CreateCommand())
{
    schema.CommandText = """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            amount FLOAT64 NOT NULL
        );
        CREATE INDEX events_category_idx ON events(category);
        """;
    schema.ExecuteNonQuery();
}

using var insertTx = connection.BeginTransaction();
using var insert = connection.CreateCommand();
insert.Transaction = insertTx;
insert.CommandText = """
    INSERT INTO events (id, category, amount)
    VALUES (@id, @category, @amount)
    """;

var insertId = insert.CreateParameter();
insertId.ParameterName = "@id";
insertId.DbType = DbType.Int64;
insert.Parameters.Add(insertId);

var insertCategory = insert.CreateParameter();
insertCategory.ParameterName = "@category";
insertCategory.DbType = DbType.String;
insert.Parameters.Add(insertCategory);

var insertAmount = insert.CreateParameter();
insertAmount.ParameterName = "@amount";
insertAmount.DbType = DbType.Double;
insert.Parameters.Add(insertAmount);

insert.Prepare();

var sw = Stopwatch.StartNew();
for (var i = 1; i <= RowCount; i++)
{
    insertId.Value = i;
    insertCategory.Value = "cat-" + (i % 20);
    insertAmount.Value = i * 1.25;
    insert.ExecuteNonQuery();
}

insertTx.Commit();
connection.Checkpoint();
Console.WriteLine($"insert+checkpoint: {sw.Elapsed}");

using var read = connection.CreateCommand();
read.CommandText = "SELECT amount FROM events WHERE id = @id";
var readId = read.CreateParameter();
readId.ParameterName = "@id";
readId.DbType = DbType.Int64;
read.Parameters.Add(readId);
read.Prepare();

var checksum = 0.0;
sw.Restart();
for (var i = 0; i < ReadCount; i++)
{
    readId.Value = (i % RowCount) + 1;
    checksum += Convert.ToDouble(read.ExecuteScalar());
}

Console.WriteLine($"point reads: {sw.Elapsed}; checksum={checksum:0.00}");

var plan = connection.ExplainQuery(
    "SELECT amount FROM events WHERE id = @id",
    analyze: true);
Console.WriteLine(plan.Text);
```

Run it in Release mode:

```bash
dotnet run -c Release
```

For the lowest-overhead microbenchmarks or import tools, use the native
`DecentDB.Native.DecentDB` surface directly. Native prepared statements are
reusable, but each repeated execution must reset the cursor and clear old
bindings unless you use a fused `Rebind*Execute` or `ExecuteBatch*` helper:

```csharp
using var db = new DecentDB.Native.DecentDB("/path/to/app.ddb");
using var stmt = db.Prepare("UPDATE counters SET value = value + 1 WHERE id = $1");

stmt.BindInt64(1, 1).StepRowsAffected();

foreach (var id in ids)
{
    stmt.RebindInt64Execute(id);
}
```

Native fused and batch helpers are useful for import tools and microbenchmarks,
but they are a lower-level surface than ADO.NET. They still require explicit
statement lifetime management, correct reset/binding behavior, and benchmark
coverage that matches the application's transaction and durability settings.

### Query diagnostics

Use `ExplainQuery` before attributing a slow query to binding overhead:

```csharp
var plan = connection.ExplainQuery(
    "SELECT id, email FROM users WHERE id = @id",
    analyze: true);

Console.WriteLine(plan.Text);
```

`EXPLAIN` and `EXPLAIN ANALYZE` currently support `SELECT` queries. For UPDATE
or DELETE performance, first verify that an equivalent SELECT predicate uses the
expected index, then benchmark the mutation separately.

### Benchmarking against SQLite from .NET

For useful DecentDB-vs-SQLite measurements:

- run Release builds, warm up the JIT, repeat each case, and alternate engine
  order between runs
- use the same transaction boundaries and checkpoint both engines at the same
  logical boundaries
- keep durability modes honest; do not compare DecentDB's durable default
  against SQLite `synchronous = NORMAL` or `OFF` without labeling the result as
  a relaxed-durability comparison
- reuse prepared `DbCommand` instances for both providers in hot loops
- do not allocate a new command and new parameter objects for every row in an
  insert, update, delete, or point-read loop
- time materialized-summary maintenance if the measured query reads a summary
  table instead of doing the original aggregate
- verify that search patterns return comparable rows; no-result LIKE queries
  mostly measure planning and binding overhead
- add indexes that match the tested predicate and ordering, for example
  `(paid, total)` for `WHERE paid = FALSE AND total < @max` or `due_at DESC`
  for `ORDER BY due_at DESC LIMIT 1000`
- compare query plans before drawing planner conclusions

The in-tree `DecentDb.ShowCase` sample includes a `PERFORMANCE PATTERNS`
section, but it is a sanity-check aid rather than a benchmark suite. The
`bindings/dotnet/benchmarks` projects are better starting points for measuring
provider overhead, native fast paths, point reads, scans, and SQLite comparison
cases. The canonical CRM comparison benchmark lives at
[`bindings/dotnet/benchmarks/DecentDB.CrmComparison/`](../../bindings/dotnet/benchmarks/DecentDB.CrmComparison/).

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
