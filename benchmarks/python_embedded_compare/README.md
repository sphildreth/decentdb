# Python Embedded Database Comparison Framework

A fair, reproducible benchmark harness for comparing DecentDB against other embedded databases using Python as the orchestrator.

## Overview

This framework benchmarks embedded database engines across multiple runtimes:
- **Phase 1 (Required):** DecentDB, SQLite, DuckDB
- **Phase 2 (JDBC):** H2, Apache Derby, HSQLDB
- **Phase 3 (Optional):** Firebird, LiteDB

It now also includes a flat-table indexed-read workload that mirrors the Python
binding benchmark shape more closely than the OLTP-style canonical suite.

## Fairness Contract

This framework treats fairness as a non-negotiable product requirement:

- **Same schema and indexes** across all engines, or closest equivalent with differences documented
- **Same generated dataset** - deterministic seed-based generation with stable row ordering
- **Same transaction policy** per scenario (autocommit, batched, explicit transaction)
- **Same durability semantics** - results are labeled with durability mode
- **Same warmup policy** where applicable
- **Same measurement methodology** across all engines

Each run produces a manifest recording:
- Engine version and runtime version
- Dataset seed and generator version
- Transaction mode and durability settings
- Environment details (CPU, OS, filesystem)

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and edit configuration
cp config/database_configs.example.yaml config/database_configs.yaml

# Run benchmarks for Phase 1 engines
python comparison_runner.py --engines sqlite,duckdb,decentdb --scenario workload_a

# Run with specific durability mode
python comparison_runner.py --engines sqlite --durability durable --scenario workload_a

# Run an operation sweep and export docs-referenceable charts
python comparison_runner.py --engines sqlite,duckdb,decentdb --workload workload_a --ops-list 10000,100000,1000000

# Run a sweep and refresh docs/user-guide/benchmarks.md summary sections
python comparison_runner.py --engines sqlite,duckdb,decentdb --workload workload_a --ops-list 10,50,100,250,500 --update-benchmarks-doc

# Run the flat-table binding-parity workload using orders_n as the row count
python comparison_runner.py --engines sqlite,decentdb --workload workload_c --customers 1 --orders 1000000 --events 1 --ops 1000
```

Single `--ops` runs produce per-benchmark comparison charts for one operation count.
Use `--ops-list` when you want the line charts described in the benchmark plan,
with operation count on the x-axis and one series per engine.
Add `--update-benchmarks-doc` to regenerate marked summary sections in
`docs/user-guide/benchmarks.md` from the exported `chart_data.json` files under
`docs/assets/benchmarks/python-embedded-compare/`.

## Directory Structure

```
python_embedded_compare/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── comparison_runner.py         # Main CLI entry point
├── config/
│   └── database_configs.example.yaml  # Example configuration
├── drivers/
│   ├── base_driver.py          # Abstract base class
│   ├── sqlite_driver.py        # SQLite implementation
│   ├── duckdb_driver.py        # DuckDB implementation
│   ├── decentdb_driver.py      # DecentDB implementation
│   └── jdbc_driver.py          # Base JDBC driver for H2/Derby/HSQLDB
├── scenarios/
│   └── canonical_workloads.py  # OLTP-ish Orders & Web Analytics workloads
├── utils/
│   ├── dataset_generator.py    # Deterministic seeded data generator
│   ├── performance_timer.py    # High-precision timing
│   └── manifest.py             # Run manifest generation
├── out/
│   └── .gitkeep                # Output directory marker
└── helpers/
    └── litedb/                 # LiteDB subprocess harness (Phase 3)
```

## Workloads

### Workload A: OLTP-ish Orders

Schema:
- `customers` (customer_id PK, email, created_at)
- `orders` (order_id PK, customer_id FK, created_at, status, total_cents)

Operations:
- Point lookup by customer_id
- Range scan by customer_id + time window
- Join orders with customers
- Aggregate by status within time window
- Update order status
- Delete orders

### Workload B: Web Analytics Events

Schema:
- `events` (event_id PK, user_id, ts, path, referrer, bytes)

Operations:
- Recent events by time range
- Per-user rollup (COUNT, SUM) within time window

### Workload C: Binding-Parity Flat Table

Schema:
- `bench` (id PK, val, f)

Operations:
- Random indexed point lookups by `id`
- Full table scans

Notes:
- This workload is intended to mirror the simpler shape from
    [bindings/python/benchmarks/bench_fetch.py](/home/steven/source/decentdb/bindings/python/benchmarks/bench_fetch.py)
    more closely than the OLTP-style workloads.
- `orders_n` controls the row count for this workload; `customers_n` and `events_n`
    are ignored after dataset generation.

## Transaction Modes

- **autocommit**: Commit after each statement
- **batched**: Commit every N statements (configurable)
- **explicit**: Single large transaction for bulk operations

## Durability Modes

- **durable**: Strongest durability (fsync on commit)
- **relaxed**: Deferred durability for throughput exploration

SQLite specifically uses multiple explicitly-named variants:
- `sqlite_wal_full`: WAL mode + synchronous=FULL
- `sqlite_wal_normal`: WAL mode + synchronous=NORMAL

## Output

The runner produces:
- `out/results_<engine>_<benchmark>.json`: Per-benchmark raw results
- `out/manifest.json`: Run metadata and fairness contract
- `out/engine_status.json`: Per-engine run status and skip or failure reasons
- `out/results_merged.json`: Combined results for aggregation
- `out/charts/latency-comparison-overview.*`: Multi-panel comparison chart for a single `--ops` run
- `out/charts/latency-overview.*`: Multi-panel line chart for an `--ops-list` sweep
- `out/charts/<benchmark>-latency.*`: Per-benchmark bar chart for single-op runs or line chart for sweeps
- `docs/assets/benchmarks/python-embedded-compare/<workload>/*.svg`: Docs-referenceable chart exports grouped by workload

When `--ops-list` is provided, each operation count is written under `out/ops_<count>/`
and the combined chart assets are still written to `out/charts/` and
`docs/assets/benchmarks/python-embedded-compare/<workload>/`.

## Integration with Native Benchmarks

This Python framework complements the native Rust benchmarks in `crates/decentdb/benches/embedded_compare.rs`:

| Tier | Engines | Use Case |
|------|---------|----------|
| Native (Rust) | DecentDB, SQLite, DuckDB | Raw engine performance, no FFI overhead |
| Python (This) | + H2, Derby, HSQLDB, LiteDB | Cross-runtime comparison |

Results can be merged using `scripts/aggregate_benchmarks.py`.

## CI Integration

- **PRs**: Smoke validation (SQLite + DecentDB subset)
- **Nightly**: Full benchmark run
- **workflow_dispatch**: Manual full run

See `.github/workflows/` for automation details.

## Engine Support Status

| Engine | Status | Notes |
|--------|--------|-------|
| SQLite | Supported | Multiple variants (WAL full/normal) |
| DuckDB | Supported | Single-threaded for fair comparison |
| DecentDB | Supported | Native Rust embedded engine |
| H2 | Supported (JDBC) | Requires Java 17+ |
| Derby | Supported (JDBC) | Requires Java 17+ |
| HSQLDB | Supported (JDBC) | Requires Java 17+ |
| Firebird | Partial | Requires native library |
| LiteDB | Partial | Requires .NET runtime |

## Development

Run tests:
```bash
pytest tests/ -v
```

Run smoke test:
```bash
python comparison_runner.py --engines sqlite --scenario workload_a --ops 1000
```
