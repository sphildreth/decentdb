# DecentDB.AdoNet

ADO.NET provider for DecentDB, targeting `.NET 10` (`net10.0`).

This package provides:

- `DecentDBConnection` / `DecentDBCommand` / `DecentDBDataReader`
- `DecentDBConnectionStringBuilder`

## Install

```bash
dotnet add package DecentDB.AdoNet --prerelease
```

## Connection string

The connection string accepts the following keys:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Data Source` | string | *required* | Path to the database file (e.g., `/tmp/mydb.ddb`). |
| `Cache Size` | string | engine default | SQLite-style cache size: integer (pages) or with unit (`64MB`, `1GB`). |
| `Logging` | bool | `false` | When `true`, fires `SqlExecuting` and `SqlExecuted` events on the connection. |
| `LogLevel` | enum (`Debug`/`Info`/`Warn`/`Error`) | `Debug` | Minimum severity for log events when `Logging=true`. |
| `Command Timeout` | int | `30` | Command execution timeout in seconds. |
| `Pooling` | bool | `true` | Currently consumed by `DecentDB.MicroOrm` only; controls whether a single open connection is reused across operations. ADO.NET ignores this key. |

Bare paths (e.g., `"/tmp/mydb.ddb"`) are also accepted by `DecentDBConnection`'s constructor and are automatically prefixed with `Data Source=`.

## Cleanup helper

Use `DecentDBConnection.DeleteDatabaseFiles(path)` to safely delete the database file and all sidecar files (`.wal`, `-wal`, `-shm`) in the correct order. This prevents stale WAL issues when recreating databases.

## Notes

- The native engine library is shipped as a NuGet runtime native asset under `runtimes/{rid}/native/`.
- Supported RIDs in this pre-release: `linux-x64`, `osx-x64`, `win-x64`.

See the [top-level .NET bindings README](../../README.md) for the feature-parity matrix.

Repository: https://github.com/sphildreth/decentdb
