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

## Notes

- The native engine library is shipped as a NuGet runtime native asset under `runtimes/{rid}/native/`.
- Supported RIDs in this pre-release: `linux-x64`, `osx-x64`, `win-x64`.
- If you only need direct ADO.NET usage (or EF provider dependencies), install `DecentDB.AdoNet` instead.

Repository: https://github.com/sphildreth/decentdb
