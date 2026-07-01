# DecentDB ADO.NET Microbenchmarks

BenchmarkDotNet coverage for phase-2 ADO.NET hot paths in `DecentDB.AdoNet`, with the same benchmark methods run against DecentDB and SQLite.

## Run

```bash
dotnet run --configuration Release --project bindings/dotnet/benchmarks/DecentDB.AdoNetMicrobenchmarks/DecentDB.AdoNetMicrobenchmarks.csproj
```

BenchmarkDotNet artifacts are written under `.tmp/adonet-microbenchmarks/artifacts`.
Temporary database files are written under `.tmp/adonet-microbenchmarks/databases`
and removed during benchmark cleanup.

## Benchmarks

- `PreparedOneRowInsert`
- `PreparedPointReadScalar`
- `PreparedOneRowUpdate`
- `ReaderCreationDisposal`
- `ExecuteNonQuerySync`
- `ExecuteNonQueryAsync`

Each benchmark reuses stable prepared command and parameter objects. The project enables BenchmarkDotNet `MemoryDiagnoser` so allocation counts are captured for both providers.
