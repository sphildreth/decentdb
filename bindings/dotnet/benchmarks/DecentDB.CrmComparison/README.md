# DecentDB CRM Comparison Benchmark

This benchmark compares DecentDB ADO.NET against `Microsoft.Data.Sqlite` on a
CRM-style embedded workload. It is intended as a repeatable local engineering
harness, not a product-wide performance claim.

## Durability And Schema Scope

The default mode is a relaxed local comparison: DecentDB uses `async_commit:10`
with `embedded_fast`, and SQLite uses WAL with `synchronous=NORMAL`. These are
not full-durability default settings.

The schemas are equivalent for the measured workload, but not byte-for-byte
identical. Each engine gets the closest supported form for partial/covering
indexes and text search, and the harness avoids indexes that no measured scenario
uses.

## Build

```bash
 dotnet build bindings/dotnet/benchmarks/DecentDB.CrmComparison/DecentDB.CrmComparison.csproj -c Release
```

## Fast Smoke Run

```bash
dotnet run --project bindings/dotnet/benchmarks/DecentDB.CrmComparison/DecentDB.CrmComparison.csproj -c Release -- \
  --size Tiny \
  --warmup-iterations 1 \
  --iterations 2 \
  --seed 42 \
  --out-dir .tmp/crm-comparison \
  --engines all \
  --no-decentdb-native-hot-paths \
  --json .tmp/crm-comparison/tiny-results.json
```

## Larger Local Run

```bash
dotnet run --project bindings/dotnet/benchmarks/DecentDB.CrmComparison/DecentDB.CrmComparison.csproj -c Release -- \
  --size Small \
  --warmup-iterations 1 \
  --iterations 5 \
  --seed 42 \
  --out-dir .tmp/crm-comparison \
  --engines all \
  --no-decentdb-native-hot-paths \
  --json .tmp/crm-comparison/small-results.json
```

You can also run one mode through the helper:

```bash
bash bindings/dotnet/benchmarks/DecentDB.CrmComparison/run-crm-benchmark.sh .tmp/crm-comparison Small relaxed 5 1 0 42 all
```

Argument order for the helper script is:

1. output directory
2. size (`Tiny|Small|Medium|Large|Jumbo`)
3. durability (`relaxed|durable`)
4. measured iterations
5. warmup iterations
6. native mode (`1` to enable, `0` to disable)
7. seed
8. engines (`all`, `decentdb`, `sqlite`)
9. collect allocations (`1` to enable, `0` to disable; default: `0`)

## Matrix Runner

Run all canonical comparison modes through one wrapper:

```bash
bash bindings/dotnet/benchmarks/DecentDB.CrmComparison/run-crm-benchmark-matrix.sh --size Small --iterations 3 --warmup 1 --run-id ci-smoke
```

Enable allocation telemetry for an entire matrix run with:

```bash
bash bindings/dotnet/benchmarks/DecentDB.CrmComparison/run-crm-benchmark-matrix.sh --size Small --iterations 3 --collect-allocations
```

The matrix runner executes:

1. DecentDB ADO.NET vs SQLite, relaxed
2. DecentDB ADO.NET vs SQLite, durable
3. SQLite ADO.NET only, relaxed
4. SQLite ADO.NET only, durable
5. Optional DecentDB native-only modes when `--native-on` is set (relaxed and durable)

Each mode writes `validation.json` beside `results.json`, and the matrix run writes a
top-level `matrix-summary.json` for lightweight trend capture.

## Options

- `--size Tiny|Small|Medium|Large|Jumbo`: workload scale.
- `--warmup-iterations <n>`: runs discarded before measured iterations.
- `--iterations <n>`: measured iterations written to JSON and summaries.
- `--seed <n>`: deterministic data and query sample seed.
- `--out-dir <path>`: artifact root. Use `.tmp/` for local runs.
- `--run-id <id>`: stable matrix directory name, useful for CI artifact capture (defaults to timestamp).
- `--json <path>`: machine-readable measured results.
- `--no-alternate-order`: always run DecentDB before SQLite.
- `--durability <relaxed|durable>`: relaxed (default) or durable settings.
- `--decentdb-native-hot-paths`: experimental native `DecentDB.Native` path for
  point reads, update, window, and delete workloads.
- `--no-decentdb-native-hot-paths`: force ADO.NET-only execution. This is the
  default because native path behavior is still being tuned for full-suite stability.
- `--decentdb-relaxed`: explicit relaxed profile.
- `--decentdb-durable`: explicit durable profile.
- `--engines <all|decentdb|sqlite>`: limit engine execution for harness separation.
- `--collect-allocations`: enable managed allocation telemetry by scenario using
  `GC.GetAllocatedBytesForCurrentThread()`.
- `--no-collect-allocations`: force allocation telemetry off (default).

## Logs

Each benchmark run writes a plain-text log file at:
`<out-dir>/<timestamp>/benchmark.log`.
Artifacts now include both JSON and shell-captured command logs, which keeps CI and
manual review scripts parsing the same canonical command contract.

## Regression Guard

Use `compare-crm-benchmark.py` to compare measured summaries and validate per-mode outputs:

```bash
python bindings/dotnet/benchmarks/DecentDB.CrmComparison/compare-crm-benchmark.py \
  --baseline .tmp/crm-comparison/baseline.json \
  --current .tmp/crm-comparison/current.json \
  --max-regression 0.10 \
  --max-allocation-regression 0.10 \
  --check-decentdb-win
```

For local smoke checks without a baseline file, validate output completeness with:

```bash
python bindings/dotnet/benchmarks/DecentDB.CrmComparison/compare-crm-benchmark.py \
  --current .tmp/crm-comparison/tiny-results.json \
  --require-complete \
  --expected-engines DecentDB,SQLite
```

Matrix mode runs now write `validation.json` beside each `results.json` with the
mode-level coverage and regression payload.

The guardrail fails if:

- a scenario/engine pair is missing from current output;
- any mean duration regresses beyond the configured ratio; or
- any collected mean allocation count regresses beyond the configured ratio; or
- DecentDB fails the lead policy when `--check-decentdb-win` is set.

## Scenario Split (Phase 10)

Scenario 07 is split to keep benchmark truthfulness:

- `07a. Raw Joined Aggregate` — full relational group-by and sum path.
- `07b. Build Revenue Summary` — materialized summary maintenance.
- `07c. Read Revenue Summary` — reads precomputed summary rows.

The split prevents the aggregate read from accidentally bypassing executor-path
coverage.

## Artifacts

Each run creates a timestamped directory under `--out-dir`. Warmup directories
are retained but omitted from JSON results. Measured iteration directories are
named `iteration-NNN`, with engine subdirectories prefixed by run order, such as
`01-decentdb` and `02-sqlite`.

Each engine directory contains:

- the database files used for that engine run;
- `explain/*.txt` files for the key SELECT scenarios.

The JSON manifest records the workload seed, warmup/measured iteration counts,
engine order per measured scenario result, runtime/platform details, DecentDB
engine/ABI details, SQLite provider/native versions, raw scenario timings,
optional allocation telemetry, and grouped summary statistics.
