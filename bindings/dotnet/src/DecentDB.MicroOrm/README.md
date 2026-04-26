# DecentDB.MicroOrm

Micro-ORM for DecentDB, targeting `.NET 10` (`net10.0`).

This package is intended for embedded use and includes:

- `DecentDB.MicroOrm` (LINQ-style query surface)
- `DecentDB.AdoNet` (ADO.NET provider)
- `DecentDB.Native` (P/Invoke layer)

## Install

```bash
dotnet add package DecentDB.MicroOrm --prerelease
```

## Conventions

- Properties are mapped by name to columns. Use `[Column("name")]` to override.
- One property must be marked `[PrimaryKey]` or named `Id` (case-sensitive).
- For read-only DTOs without a primary key, use `ctx.QueryRawAsync<T>(sql)`.

### POCO portability with EF Core

When sharing POCOs between EF Core and MicroOrm, MicroOrm auto-skips properties that are not natively bindable (reference types like navigation properties, collections, complex types). This means a POCO with `public Artist? Artist { get; set; }` works in both bindings — EF Core treats it as a navigation, MicroOrm ignores it. To force MicroOrm to include such a property, use `[Column]`.

See the [top-level .NET bindings README](../../README.md#poco-portability-with-ef-core) for the full list of bindable types.

## Notes

- The native engine library is shipped as a NuGet runtime native asset under `runtimes/{rid}/native/`.
- Supported RIDs in this pre-release: `linux-x64`, `osx-x64`, `win-x64`.
- If you only need direct ADO.NET usage (or EF provider dependencies), install `DecentDB.AdoNet` instead.

See the [top-level .NET bindings README](../../README.md) for the feature-parity matrix and connection-string documentation.

Repository: https://github.com/sphildreth/decentdb
