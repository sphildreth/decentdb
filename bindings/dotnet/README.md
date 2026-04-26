# DecentDB for .NET

This directory contains the official .NET bindings for DecentDB:

| Package | Description |
|---------|-------------|
| `DecentDB.AdoNet` | ADO.NET provider (`DbConnection`, `DbCommand`, `DbDataReader`). Use for direct SQL execution. |
| `DecentDB.MicroOrm` | Lightweight micro-ORM with LINQ-style queries and `DbSet<T>`. Use for simple CRUD without EF Core overhead. |
| `DecentDB.EntityFrameworkCore` | Full EF Core provider. Use when you need change tracking, migrations, and the full EF Core ecosystem. |

## Choosing a binding

- **AdoNet** — Best for maximum performance and full SQL control. Ideal for migration scripts, bulk operations, and scenarios where you write SQL directly.
- **MicroOrm** — Best for simple CRUD with minimal overhead. Ideal for embedded apps that want type-safe queries without the complexity of EF Core.
- **EF Core** — Best for complex domain models, change tracking, and migrations. Ideal for applications already invested in the EF Core ecosystem.

## Feature parity matrix

| Feature | AdoNet | MicroOrm | EF Core |
|---------|--------|----------|---------|
| Single-row INSERT | ✅ | ✅ | ✅ |
| Bulk INSERT (multi-row VALUES) | ✅ (multi-statement) | ✅ (256-row chunks) | ✅ (256-row coalescing) |
| RETURNING (single row) | ✅ | ✅ | ✅ |
| RETURNING (bulk) | ✅ | ✅ (`InsertManyReturningAsync`) | ❌ |
| Async transactions | ✅ | ✅ | ✅ |
| IDbBatch | ✅ | ❌ | ✅ |
| LINQ where/order/skip/take | ❌ | ✅ | ✅ |
| LINQ aggregates | ❌ | ✅ (basic) | ✅ |
| Streaming reads | ✅ (`DbDataReader`) | ❌ | ✅ |
| Connection pooling | ❌ (MicroOrm interprets `Pooling`) | ✅ | ❌ |
| View querying (keyless DTO) | ✅ | ✅ (`QueryRawAsync<T>`) | ✅ (`DbQuery` / keyless entity) |
| Diagnostic events (`SqlExecuting`/`SqlExecuted`) | ✅ | ❌ | ✅ (EF Core logging) |
| `GetSchema("Indexes")` with `IS_PRIMARY_KEY` | ✅ | N/A | N/A |
| `DeleteDatabaseFiles` helper | ✅ | N/A | N/A |
| Model pre-building cache | N/A | N/A | ✅ (`DecentDBModelBuilder`) |
| Compiled query support | N/A | N/A | ✅ (`EF.CompileQuery`) |
| Correlated aggregate rewrite | N/A | N/A | ✅ (infrastructure; rewrite deferred) |

## Connection strings

All three bindings accept bare paths (e.g., `"/tmp/mydb.ddb"`) and full connection strings. The canonical form is:

```
Data Source=/path/to/db.ddb;Pooling=true;Cache Size=64MB;Logging=false;Command Timeout=30
```

Supported keys:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Data Source` | string | *required* | Path to the database file. |
| `Cache Size` | string | engine default | Cache size: integer (pages) or with unit (`64MB`). |
| `Logging` | bool | `false` | Fire `SqlExecuting`/`SqlExecuted` events. |
| `LogLevel` | enum | `Debug` | Minimum log severity. |
| `Command Timeout` | int | `30` | Command timeout in seconds. |
| `Pooling` | bool | `true` | Consumed by MicroOrm only; ADO.NET ignores this key. |

The `DecentDBConnection.DeleteDatabaseFiles(path)` helper deletes the database file and all sidecar files (`.wal`, `-wal`, `-shm`) safely.

## POCO portability with EF Core

When sharing POCOs between EF Core and MicroOrm, note that MicroOrm auto-skips properties that are not natively bindable (reference types, collections, complex types). To include such a property in MicroOrm, use `[Column]`:

```csharp
public class Album
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public int ArtistId { get; set; }

    // EF Core treats this as a navigation; MicroOrm ignores it by default.
    public Artist? Artist { get; set; }

    // Opt in for MicroOrm if needed:
    // [Column("artist_blob")] public Artist? ArtistBlob { get; set; }
}
```

Bindable types (auto-included by MicroOrm): all primitives, `string`, `Guid`, `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan`, `byte[]`, `enum`, and `Nullable<T>` of any of these.

## Per-database vs per-binding views

Views are per-database (`CREATE VIEW` persists in the file). Apps that share a file across bindings only need to issue the DDL once. Apps that use separate files per binding must re-create each view per file.

## Performance characteristics

Measured on engine 2.3.1, scale full (50K artists, 500K albums, ~2.75M songs):

| binding | total | seed_artists r/s | seed_albums r/s | seed_songs r/s | peak heap |
|---------|------:|-----------------:|----------------:|---------------:|----------:|
| AdoNet | 20s | 381 896 | 501 652 | 528 336 | 324 MB |
| MicroOrm | 130s | 119 051 | 72 551 | 25 026 | 327 MB |
| EF Core (refactored) | 27s | 353 499 | 334 420 | 250 474 | 324 MB |

Numbers as of 2026-04-26, engine 2.3.1. Post-N1/N2 improvements.

## Package READMEs

- [AdoNet](src/DecentDB.AdoNet/README.md)
- [MicroOrm](src/DecentDB.MicroOrm/README.md)
- [EF Core](src/DecentDB.EntityFrameworkCore/README.md)
