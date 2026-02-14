# .NET (C#) Bindings

DecentDB ships .NET bindings at multiple API levels:

- [`DecentDB.EntityFrameworkCore`](https://www.nuget.org/packages/DecentDB.EntityFrameworkCore/) for full EF Core DbContext, change tracking, migrations, and LINQ-to-SQL.
- [`DecentDB.MicroOrm`](https://www.nuget.org/packages/DecentDB.MicroOrm/) for lightweight LINQ-style Micro-ORM usage.
- [`DecentDB.AdoNet`](https://www.nuget.org/packages/DecentDB.AdoNet/) for direct ADO.NET usage.


### .NET NuGet packages

- `DecentDB.EntityFrameworkCore`: use this for EF Core DbContext, LINQ queries, change tracking, and migrations.
- `DecentDB.EntityFrameworkCore.Design`: add this alongside the above for `dotnet ef` design-time commands.
- `DecentDB.EntityFrameworkCore.NodaTime`: optional extension for NodaTime type mappings (`Instant`, `LocalDate`, `LocalDateTime`).
- `DecentDB.MicroOrm`: use this for the LINQ-style Micro-ORM experience (includes ADO.NET + native runtime assets).
- `DecentDB.AdoNet`: use this for direct ADO.NET access.
- Current packaged native RID assets: `linux-x64`, `osx-x64`, `win-x64`.

```bash
# EF Core (recommended for most applications)
dotnet add package DecentDB.EntityFrameworkCore
dotnet add package DecentDB.EntityFrameworkCore.Design  # for dotnet ef commands
dotnet add package DecentDB.EntityFrameworkCore.NodaTime # optional: NodaTime support

# Micro-ORM (lightweight alternative)
dotnet add package DecentDB.MicroOrm

# ADO.NET (low-level)
dotnet add package DecentDB.AdoNet
```

Notes:

- All packages target `.NET 10` (`net10.0`).
- `DecentDB.EntityFrameworkCore` depends on `DecentDB.AdoNet` (which includes native runtime assets).
- `DecentDB.MicroOrm` remains a one-package install for Micro-ORM users.
- Ships native assets under `runtimes/{rid}/native/` for: `linux-x64`, `osx-x64`, `win-x64`.

## EF Core Provider

DecentDB includes a full EF Core provider:

- `DecentDB.EntityFrameworkCore` — runtime provider (query pipeline, SaveChanges, schema creation)
- `DecentDB.EntityFrameworkCore.Design` — design-time tooling (`dotnet ef migrations`, `dotnet ef dbcontext scaffold`)
- `DecentDB.EntityFrameworkCore.NodaTime` — optional NodaTime type mappings

Use the package that matches your scenario:

- Prefer `DecentDB.EntityFrameworkCore` for full DbContext, change tracking, LINQ queries, and migrations.
- Prefer `DecentDB.MicroOrm` for lightweight LINQ-style access without EF infrastructure.
- Prefer `DecentDB.AdoNet` for direct SQL/command control.

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

services.AddDbContextFactory<AppDbContext>(options =>
    options.UseDecentDB("Data Source=./app.ddb"));

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

For `IDesignTimeDbContextFactory<TContext>` projects, keep `UseDecentDB("Data Source=...")` in the factory so `dotnet ef` can create the context.

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

Supported LINQ-to-SQL translation:

**Core query operators:**

- `Where`, `OrderBy`/`OrderByDescending`, `ThenBy`/`ThenByDescending`
- `Skip`, `Take` (translated to `LIMIT`/`OFFSET`)
- `Select` projections (anonymous types, DTOs)
- `Distinct`
- `GroupBy` with aggregate projections
- `Include`/`ThenInclude` (eager loading), `AsSplitQuery`
- Filtered `Include` (e.g., `Include(a => a.Tracks.Where(...))`)
- `Any`, `All`
- `Count`, `Sum`, `Min`, `Max`, `Average`
- `FirstOrDefault`, `SingleOrDefault`
- Subqueries in FROM clauses
- `EXISTS` and scalar subqueries in SELECT lists
- `FromSqlRaw` for raw SQL pass-through

**String method translation:**

| C# method | SQL |
|---|---|
| `string.Contains(value)` | `LIKE '%' \|\| @p \|\| '%'` |
| `string.StartsWith(value)` | `LIKE @p \|\| '%'` |
| `string.EndsWith(value)` | `LIKE '%' \|\| @p` |
| `string.ToUpper()` | `UPPER(column)` |
| `string.ToLower()` | `LOWER(column)` |
| `string.Trim()` | `TRIM(column)` |
| `string.TrimStart()` | `LTRIM(column)` |
| `string.TrimEnd()` | `RTRIM(column)` |
| `string.Substring(start)` | `SUBSTRING(column, start+1)` |
| `string.Substring(start, len)` | `SUBSTRING(column, start+1, len)` |
| `string.Replace(old, new)` | `REPLACE(column, old, new)` |
| `string.Length` | `LENGTH(column)` |

**Math method translation:**

| C# method | SQL |
|---|---|
| `Math.Abs(x)` | `ABS(x)` |
| `Math.Round(x)` / `Math.Round(x, d)` | `ROUND(x)` / `ROUND(x, d)` |
| `Math.Ceiling(x)` | `CEIL(x)` |
| `Math.Floor(x)` | `FLOOR(x)` |
| `Math.Max(a, b)` | `CASE WHEN a > b THEN a ELSE b END` |
| `Math.Min(a, b)` | `CASE WHEN a < b THEN a ELSE b END` |

**Conditional expressions:**

- Ternary (`a ? b : c`) translates to `CASE WHEN ... THEN ... ELSE ... END`
- Null-coalescing (`a ?? b`) translates to `COALESCE(a, b)`

Current guardrails:

- `IN (...)` lists are capped at **1000 values**; larger lists fail fast with a provider error.
- Provider conformance skip list is maintained in `bindings/dotnet/tests/DecentDB.EntityFrameworkCore.Tests/ConformanceSkipList.md`.

### EF Core NodaTime extension

```bash
dotnet add package DecentDB.EntityFrameworkCore.NodaTime
```

```csharp
options.UseDecentDB("Data Source=./app.ddb", decent => decent.UseNodaTime());
```

Supported NodaTime types in the extension package:

- `Instant` -> `INTEGER` (Unix epoch milliseconds)
- `LocalDate` -> `INTEGER` (day offset from Unix epoch day)
- `LocalDateTime` -> `INTEGER` (UTC epoch milliseconds via UTC zone conversion)

## Assemblies

The NuGet packages include these assemblies:

| Assembly | Description |
|---------|-------------|
| `DecentDB.Native` | Low-level P/Invoke wrapper over the DecentDB C API |
| `DecentDB.AdoNet` | ADO.NET provider (`DbConnection`, `DbCommand`, `DbDataReader`) |
| `DecentDB.MicroOrm` | Micro-ORM with `DbSet<T>`, `DecentDBContext`, LINQ-style queries |
| `DecentDB.EntityFrameworkCore` | EF Core runtime provider (query pipeline, SaveChanges, migrations) |
| `DecentDB.EntityFrameworkCore.Design` | EF Core design-time tooling (`dotnet ef` commands) |
| `DecentDB.EntityFrameworkCore.NodaTime` | Optional NodaTime type mappings for EF Core |

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

- EF Core + NodaTime demo: `examples/dotnet/entityframework/` — comprehensive showcase with 67 benchmarked operations covering CRUD, pagination, Include/ThenInclude, GroupBy, DISTINCT, projections, CASE WHEN, string/math operations, Any/Min/Max, FromSqlRaw, AsSplitQuery, filtered Include, and NodaTime (`Instant`, `LocalDate`, `DateTime` coexistence).
- Dapper example: `examples/dotnet/dapper-basic/`
- Micro-ORM + LINQ example: `examples/dotnet/microorm-linq/`
