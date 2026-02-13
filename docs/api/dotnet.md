# .NET (C#) Bindings

DecentDB ships .NET bindings (ADO.NET + Micro-ORM) for embedded use.

For most .NET applications, choose the package based on API level:
- [`DecentDB.MicroOrm`](https://www.nuget.org/packages/DecentDB.MicroOrm/) for LINQ-style Micro-ORM usage.
- [`DecentDB.AdoNet`](https://www.nuget.org/packages/DecentDB.AdoNet/) for direct ADO.NET usage and EF provider dependencies.


### .NET NuGet packages

- `DecentDB.MicroOrm`: use this for the LINQ-style Micro-ORM experience (includes ADO.NET + native runtime assets).
- `DecentDB.AdoNet`: use this for direct ADO.NET access (and for EF Core provider dependencies).
- Current packaged native RID assets: `linux-x64`, `osx-x64`, `win-x64`.

```bash
dotnet add package DecentDB.MicroOrm
dotnet add package DecentDB.AdoNet
# If you want pre-release builds:
dotnet add package DecentDB.MicroOrm --prerelease
dotnet add package DecentDB.AdoNet --prerelease
```

Notes:

- Both packages target `.NET 10` (`net10.0`).
- `DecentDB.MicroOrm` remains a one-package install for Micro-ORM users.
- `DecentDB.AdoNet` carries `DecentDB.Native` and native runtime assets for ADO.NET/EF-provider use.
- Ships native assets under `runtimes/{rid}/native/` for: `linux-x64`, `osx-x64`, `win-x64`.

## EF Core provider status

DecentDB now publishes EF Core packages:

- `DecentDB.EntityFrameworkCore` (runtime provider)
- `DecentDB.EntityFrameworkCore.Design` (design-time tooling)
- `DecentDB.EntityFrameworkCore.NodaTime` (optional NodaTime mappings)

Use the package that matches your scenario:

- Prefer `DecentDB.MicroOrm` for lightweight LINQ-style access without EF infrastructure.
- Prefer `DecentDB.AdoNet` for direct SQL/command control.
- Prefer `DecentDB.EntityFrameworkCore` when you need DbContext, change tracking, and migrations tooling.

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

services.AddDbContextFactory<AppDbContext>(options =>
    options.UseDecentDb("Data Source=./app.ddb"));

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
}
```

### EF Core migrations (design-time)

Install packages:

```bash
dotnet add package DecentDB.EntityFrameworkCore
dotnet add package DecentDB.EntityFrameworkCore.Design
```

Create/apply migrations:

```bash
dotnet ef migrations add InitialCreate
dotnet ef database update
```

For `IDesignTimeDbContextFactory<TContext>` projects, keep `UseDecentDb("Data Source=...")` in the factory so `dotnet ef` can create the context.

### EF Core provider type mappings

EF Core provider type mappings with the existing ADO.NET/MicroOrm conventions:

| CLR type | EF store type | DecentDB storage representation |
|---|---|---|
| `bool` | `BOOLEAN` | native boolean |
| `byte`, `short`, `int`, `long` | `INTEGER` | signed integer |
| `float`, `double` | `REAL` | floating-point |
| `decimal` | `DECIMAL` | native DECIMAL (unscaled + scale; see ADR-0091) |
| `Guid` | `UUID` | 16-byte blob UUID (see ADR-0091) |
| `string` | `TEXT` | UTF-8 text |
| `byte[]` | `BLOB` | binary blob |
| `DateTime` | `INTEGER` | Unix epoch milliseconds (UTC) |
| `DateTimeOffset` | `INTEGER` | Unix epoch milliseconds (UTC) |
| `DateOnly` | `INTEGER` | day offset from Unix epoch day |
| `TimeOnly` | `INTEGER` | ticks since midnight |
| `TimeSpan` | `INTEGER` | ticks |

### EF Core query translation scope

Current supported query translation subset:

- Basic `Where`, `OrderBy`, `Skip`, and `Take` translation.
- Paging uses `LIMIT/OFFSET`.
- Null/bool relational semantics are emitted by EF relational SQL generation (`IS NULL`, boolean predicates).
- String operators: `Contains`, `StartsWith`, `EndsWith` translate to `LIKE` patterns for literal string arguments, with wildcard escaping for literal `%` and `_`.

Current guardrails:

- `IN (...)` lists are capped at **1000 values**; larger lists fail fast with a provider error.
- Correlated subqueries and some set-operation/generated-subquery shapes are currently unsupported by the engine SQL subset and are tracked under [issue #20](https://github.com/sphildreth/decentdb/issues/20).
- Provider conformance skip list is maintained in `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/ConformanceSkipList.md`.

### EF Core NodaTime extension

```bash
dotnet add package DecentDB.EntityFrameworkCore.NodaTime
```

```csharp
options.UseDecentDb("Data Source=./app.ddb", decent => decent.UseNodaTime());
```

Supported NodaTime types in the extension package:

- `Instant` -> `INTEGER` (Unix epoch milliseconds)
- `LocalDate` -> `INTEGER` (day offset from Unix epoch day)
- `LocalDateTime` -> `INTEGER` (UTC epoch milliseconds via UTC zone conversion)

## Assemblies

The NuGet package includes these assemblies:

| Assembly | Description |
|---------|-------------|
| `DecentDB.Native` | Low-level P/Invoke wrapper over the DecentDB C API |
| `DecentDB.AdoNet` | ADO.NET provider (`DbConnection`, `DbCommand`, `DbDataReader`) |
| `DecentDB.MicroOrm` | Micro-ORM with `DbSet<T>`, `DecentDBContext`, LINQ-style queries |

## Build the native library (from source)

If you need a RID not shipped by the NuGet package or you're working in this repo:

```bash
nimble build_lib
```

This produces `build/libc_api.so` (Linux), `build/libc_api.dylib` (macOS), or `build/decentdb.dll` (Windows).

## ADO.NET Usage

### Connection

```csharp
using DecentDB.AdoNet;

using var conn = new DecentDBConnection("Data Source=./sample.ddb");
conn.Open();
```

### Connection String Builder

```csharp
var csb = new DecentDBConnectionStringBuilder
{
    DataSource = "./sample.ddb",
    CacheSize = 256
};
using var conn = new DecentDBConnection(csb.ConnectionString);
conn.Open();
```

### DbProviderFactory

```csharp
var factory = DecentDBFactory.Instance;
using var conn = factory.CreateConnection();
conn.ConnectionString = "Data Source=./sample.ddb";
conn.Open();
```

### Commands and Parameters

```csharp
using var cmd = conn.CreateCommand();

// DDL
cmd.CommandText = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)";
cmd.ExecuteNonQuery();

// INSERT with auto-increment (omit id column)
cmd.CommandText = "INSERT INTO users (name, email) VALUES (@name, @email)";
cmd.Parameters.Add(new DecentDBParameter("@name", "Alice"));
cmd.Parameters.Add(new DecentDBParameter("@email", "alice@example.com"));
cmd.ExecuteNonQuery();  // id auto-assigned
```

### Reading Results

```csharp
cmd.CommandText = "SELECT id, name, email FROM users WHERE name = @name";
cmd.Parameters.Clear();
cmd.Parameters.Add(new DecentDBParameter("@name", "Alice"));

using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    long id = reader.GetInt64(0);
    string name = reader.GetString(1);
    string email = reader.IsDBNull(2) ? null : reader.GetString(2);
}
```

### Transactions

```csharp
using var tx = conn.BeginTransaction();
try
{
    using var cmd = conn.CreateCommand();
    cmd.Transaction = tx;
    cmd.CommandText = "INSERT INTO users (name) VALUES (@name)";
    cmd.Parameters.Add(new DecentDBParameter("@name", "Bob"));
    cmd.ExecuteNonQuery();
    tx.Commit();
}
catch
{
    tx.Rollback();
    throw;
}
```

### Schema Introspection

```csharp
// List all tables
DataTable tables = conn.GetSchema("Tables");

// List columns for a specific table
DataTable columns = conn.GetSchema("Columns", new[] { "users" });

// List all indexes
DataTable indexes = conn.GetSchema("Indexes");

// JSON variants (lighter weight)
string tablesJson = conn.ListTablesJson();
string columnsJson = conn.GetTableColumnsJson("users");
string indexesJson = conn.ListIndexesJson();
```

## MicroOrm Usage

### Define entities and context

```csharp
public class User
{
    public long Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
}

public class AppContext : DecentDBContext
{
    public DbSet<User> Users { get; }

    public AppContext(DecentDBConnection conn) : base(conn)
    {
        Users = Set<User>();
    }
}
```

### CRUD operations

```csharp
using var conn = new DecentDBConnection("Data Source=./app.ddb");
conn.Open();
var ctx = new AppContext(conn);

// Create table (MicroOrm is convention-based; use raw DDL for schema)
await ctx.ExecuteNonQueryAsync(
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)");

// INSERT (id is auto-assigned via RETURNING)
var user = new User { Name = "Alice", Email = "alice@example.com" };
await ctx.Users.InsertAsync(user);
Console.WriteLine(user.Id);  // prints auto-assigned ID

// SELECT
var users = await ctx.Users.ToListAsync();
var alice = (await ctx.Users.Where(u => u.Name == "Alice").ToListAsync()).First();

// UPDATE
alice.Email = "newalice@example.com";
await ctx.Users.UpdateAsync(alice);

// DELETE
await ctx.Users.DeleteAsync(alice);
```

### Upsert

```csharp
// INSERT or UPDATE on conflict
var user = new User { Id = 1, Name = "Alice", Email = "alice@example.com" };
await ctx.Users.UpsertAsync(user, "id");

// INSERT or ignore on conflict
await ctx.Users.InsertOrIgnoreAsync(user, "id");
```

### Projection

```csharp
// Select only specific columns into an anonymous type
var names = await ctx.Users.SelectAsync(u => new { u.Name, u.Email });
```

### Raw SQL

```csharp
// Execute non-query
await ctx.ExecuteNonQueryAsync("DELETE FROM users WHERE name = $1", "Alice");

// Scalar value
var count = await ctx.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM users");

// Query with mapping
var results = await ctx.QueryAsync<User>("SELECT * FROM users WHERE id > $1", 5);
```

## Parameter Style

The engine uses Postgres-style positional parameters (`$1`, `$2`, ...). The ADO.NET provider automatically rewrites common .NET styles:

- Named: `@id`, `@name` → `$1`, `$2` (in order of first appearance)
- Indexed: `@p0`, `@p1` → `$1`, `$2`
- Positional: `?` → `$1`, `$2`, ...

You can also write `$N` directly.

## Examples

- Dapper example: `examples/dotnet/dapper-basic/`
- Micro-ORM + LINQ example: `examples/dotnet/microorm-linq/`
