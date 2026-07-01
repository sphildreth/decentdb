# DecentDB .NET benchmarks

This is a lightweight, dependency-free benchmark harness for the ADO.NET provider and the Micro-ORM.

Additional benchmark suites:

- `DecentDB.CrmComparison`: canonical CRM-style DecentDB vs SQLite comparison with JSON artifacts, matrix runner, validation, and optional allocation telemetry.
- `DecentDB.AdoNetMicrobenchmarks`: BenchmarkDotNet ADO.NET hot-path microbenchmarks with memory diagnostics for prepared inserts, point reads, updates, reader creation, and sync vs async wrappers.

## Run

From repo root:

- `dotnet run -c Release --project bindings/dotnet/benchmarks/DecentDB.Benchmarks/DecentDB.Benchmarks.csproj`

To make the point-read comparison more like the native tuned configuration, benchmarks now run DecentDB with a tuned durable profile by default: `Cache Size=64MB`, `Retain Paged Row Sources After Commit=True`, `Paged Row Storage=False`, and `WAL Auto Checkpoint=0`.

- `dotnet run -c Release --project bindings/dotnet/benchmarks/DecentDB.Benchmarks/DecentDB.Benchmarks.csproj -- --decentdb-cache-size 64MB`

You can override this with:

- `--decentdb-cache-size <n|size>`
- examples: `16384` (pages), `128MB`, `1GB`

Notes:
- The benchmark table declares `id` as the primary key for both DecentDB and SQLite; the point-read workload samples unique lookups by that key.
- Dapper insert throughput uses the same parameterized multi-row insert shape for both engines. The default batch size is 128 rows, which avoids measuring Dapper's per-row enumerable adapter as the dominant cost.
- EF Core inserts use the same large `SaveChanges` batch for both providers so the benchmark measures provider/database write throughput instead of frequent change-tracker flushes.
- DecentDB MicroOrm is included in the overall ranking, but it is not paired against raw SQLite ADO.NET because that is not a like-for-like provider comparison.
- Timings are sensitive to machine load and CPU frequency scaling.
- Results are best used for relative comparisons (regressions) rather than absolute guarantees.
- Default extension guidance: DecentDB databases use `.ddb`; SQLite databases use `.db`.
