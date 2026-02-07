# Decent.MicroOrm

Micro-ORM for DecentDB, targeting `.NET 10` (`net10.0`).

This package is intended for embedded use and includes:

- `DecentDb.MicroOrm` (LINQ-style query surface)
- `DecentDb.AdoNet` (ADO.NET provider)
- `DecentDb.Native` (P/Invoke layer)

## Install

```bash
dotnet add package Decent.MicroOrm --prerelease
```

## Notes

- The native engine library is shipped as a NuGet runtime native asset under `runtimes/{rid}/native/`.
- Supported RIDs in this pre-release: `linux-x64`, `osx-x64`, `win-x64`.

Repository: https://github.com/sphildreth/decentdb
