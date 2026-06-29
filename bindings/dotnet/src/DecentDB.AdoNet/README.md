# DecentDB.AdoNet

ADO.NET provider for DecentDB, targeting `.NET 10` (`net10.0`).

This package provides:

- `DecentDBConnection` / `DecentDBCommand` / `DecentDBDataReader`
- `DecentDBConnectionStringBuilder`
- `DecentDBMaintenance` for checkpoint, WAL status, compact, and vacuum helpers

## Install

```bash
dotnet add package DecentDB.AdoNet --prerelease
```

## Connection string

The connection string accepts the following keys:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Data Source` | string | *required* | Path to the database file (e.g., `/tmp/mydb.ddb`). |
| `Performance Profile` | string | engine default | Named native profile: `default`, `low_memory`, `balanced`, `embedded_fast`, or `tuned_durable`. Explicit low-level options override profile values. |
| `Cache Size` | string | engine default | SQLite-style cache size: integer (pages) or with unit (`64MB`, `1GB`). |
| `Retain Paged Row Sources After Commit` | bool | engine default | Keep paged row sources resident after commits on this handle for hot read/write workloads. |
| `Paged Row Storage` | bool | engine default | Enable the paged row storage format; `embedded_fast` sets this to `False` for cheaper repeated small writes. |
| `Persistent PK Index` | bool | engine default | Enable the persistent primary-key locator index. Benchmark before enabling globally because it adds write-time and file-size overhead. |
| `WAL Auto Checkpoint` | int | engine default | WAL auto-checkpoint page threshold; `embedded_fast` sets this to `0` so bulk loads are not interrupted mid-flight. |
| `Process Coordination` | string | `auto` | Cross-process WAL coordination mode: `auto`, `required`, or `single_process_unsafe`. |
| `Process Coordination Timeout Ms` | int | `30000` | Bounded wait for cross-process coordination locks. |
| `Logging` | bool | `false` | When `true`, fires `SqlExecuting` and `SqlExecuted` events on the connection. |
| `LogLevel` | enum (`Debug`/`Info`/`Warn`/`Error`) | `Debug` | Minimum severity for log events when `Logging=true`. |
| `Command Timeout` | int | `30` | Command execution timeout in seconds. |
| `Pooling` | bool | `true` | Currently consumed by `DecentDB.MicroOrm` only; controls whether a single open connection is reused across operations. ADO.NET ignores this key. |

Bare paths (e.g., `"/tmp/mydb.ddb"`) are also accepted by `DecentDBConnection`'s constructor and are automatically prefixed with `Data Source=`.

For single-process embedded applications with a hot working set, start with:

```csharp
var csb = new DecentDBConnectionStringBuilder
{
    DataSource = "/path/to/app.ddb",
    PerformanceProfile = "embedded_fast",
    CacheSize = "64MB",
    ProcessCoordination = "single_process_unsafe", // only for one-process apps
};
```

When reusing native prepared statements directly, reset and clear bindings before
each repeated execution unless you use a `Rebind*Execute` or `ExecuteBatch*`
helper:

```csharp
stmt.Reset().ClearBindings().BindInt64(1, id).StepRowsAffected();
```

## Cleanup helper

Use `DecentDBConnection.DeleteDatabaseFiles(path)` to safely delete the database file and all sidecar files (`.wal`, `-wal`, `-shm`, `.coord`) in the correct order. This prevents stale WAL or coordination artifacts when recreating databases.

## Maintenance helpers

Use `DecentDBMaintenance` for file-path based maintenance through the .NET
binding:

```csharp
var before = DecentDBMaintenance.GetWalStatus(path);
var checkpoint = await DecentDBMaintenance.CheckpointAsync(path);
var compact = await DecentDBMaintenance.CompactAsync(path, compactedPath);
var vacuum = await DecentDBMaintenance.VacuumAsync(path, createBackup: true);
```

`VacuumAsync(...)` uses the .NET binding directly by checkpointing, saving a
compact temporary copy, and replacing the original file. `VacuumAtomicAsync(...)`
remains available for legacy executable-backed offline vacuum flows.

## Query diagnostics

Use `ExplainQuery` on an open `DecentDBConnection` to capture `EXPLAIN` or
`EXPLAIN ANALYZE` output without writing command boilerplate:

```csharp
var plan = connection.ExplainQuery(
    "SELECT * FROM artists WHERE musicbrainz_id_raw = $1",
    analyze: true);

Console.WriteLine(plan.Text);
```

## Notes

- The native engine library is shipped as a NuGet runtime native asset under `runtimes/{rid}/native/`.
- Supported RIDs in this pre-release: `linux-x64`, `osx-x64`, `win-x64`.

See the [top-level .NET bindings README](../../README.md) for the feature-parity matrix.

Repository: https://github.com/sphildreth/decentdb
