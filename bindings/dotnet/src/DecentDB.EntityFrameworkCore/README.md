# DecentDB.EntityFrameworkCore

Entity Framework Core provider for DecentDB, targeting `.NET 10` (`net10.0`).

This package depends on `DecentDB.AdoNet` and `DecentDB.MicroOrm`.

## Install

```bash
dotnet add package DecentDB.EntityFrameworkCore --prerelease
```

## Performance tips

For applications that create many short-lived `DbContext` instances (e.g., CLI tools, test suites), the first context construction can incur a ~1–2 second model-building cost. To avoid this, pre-build the model once and reuse it:

```csharp
using DecentDB.EntityFrameworkCore;

// Pre-build the model at application startup
var model = DecentDBModelBuilder.BuildModel<MyDbContext>();

// For each context instance
var options = new DbContextOptionsBuilder<MyDbContext>()
    .UseDecentDB("Data Source=/tmp/app.ddb", model)
    .Options;

using var ctx = new MyDbContext(options);
// Use ctx...
```

The `DecentDBModelBuilder.BuildModel<TContext>()` method caches the model per context type. Subsequent calls return the cached instance immediately.

### Compiled queries for tight loops

For hot-path queries executed thousands of times (e.g., in a request loop), use `EF.CompileQuery` to pre-compile the LINQ expression tree and eliminate per-call translation overhead:

```csharp
using Microsoft.EntityFrameworkCore;

// Define once at application startup
private static readonly Func<MyDbContext, int, Artist?> _artistById =
    EF.CompileQuery((MyDbContext ctx, int id) =>
        ctx.Artists.FirstOrDefault(a => a.Id == id));

// Use in hot path — zero LINQ translation cost
var artist = _artistById(context, 42);
```

This reduces per-call latency from ~1–5 ms (LINQ translation + SQL generation) to sub-millisecond execution, matching the raw engine performance.

See the [top-level .NET bindings README](../../README.md) for the feature-parity matrix and connection-string documentation.

Repository: https://github.com/sphildreth/decentdb
