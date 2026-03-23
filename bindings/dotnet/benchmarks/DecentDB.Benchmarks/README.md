# DecentDB .NET benchmarks

This is a lightweight, dependency-free benchmark harness for the ADO.NET provider and the Micro-ORM.

## Run

From repo root:

- `dotnet run -c Release --project bindings/dotnet/benchmarks/DecentDB.Benchmarks/DecentDB.Benchmarks.csproj`

Notes:
- Timings are sensitive to machine load and CPU frequency scaling.
- Results are best used for relative comparisons (regressions) rather than absolute guarantees.
