# .NET (C#) Bindings

DecentDB’s .NET bindings live in-repo under `bindings/dotnet/` and are intended for embedded use.

## Packages (in this repo)

- `DecentDB.Native`: low-level native wrapper over the DecentDB C API
- `DecentDB.AdoNet`: ADO.NET provider (`DbConnection`/`DbCommand`)
- `DecentDB.MicroOrm`: small LINQ-style micro-ORM on top of the ADO.NET layer

## Build the native library

The managed bindings call into the DecentDB C API. Build the shared library from the repo root:

```bash
nimble build_lib
```

This produces a platform-specific shared library under `build/` (for example `build/libc_api.so` on Linux).

## ADO.NET usage

```csharp
using DecentDB.AdoNet;

using var conn = new DecentDBConnection("Data Source=./sample.ddb");
conn.Open();

using var cmd = conn.CreateCommand();
cmd.CommandText = "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT)";
cmd.ExecuteNonQuery();

cmd.CommandText = "INSERT INTO users (name) VALUES (@name)";

var pName = cmd.CreateParameter();
pName.ParameterName = "@name";
pName.Value = "Alice";
cmd.Parameters.Add(pName);

cmd.ExecuteNonQuery();  // id is auto-assigned
```

## Parameter style

The engine requires Postgres-style positional parameters (`$1`, `$2`, ...). The ADO.NET provider rewrites common .NET styles to `$N`:

- Named parameters like `@id`, `@name`
- `@p0`, `@p1`, ... (common in generated SQL)

You can also write `$N` directly.

## Examples

- Dapper example: `examples/dotnet/dapper-basic/`
- Micro-ORM + LINQ example: `examples/dotnet/microorm-linq/`
