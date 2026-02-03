# PRD — DecentDB
**Product:** DecentDB (embedded, single-file relational DB)  
**Status:** Draft  
**Primary goals:** (1) Durable ACID writes, (2) fast reads

## 1. Problem statement
Developers want an embedded relational database with:
- A single-file store
- Strong durability guarantees (ACID)
- Fast point reads and common relational joins
- A pragmatic SQL subset (PostgreSQL-like syntax where possible)
- A clear story for “show me it’s fast enough” via benchmarks

DecentDB is intentionally scoped: it is not trying to be a full PostgreSQL or SQLite clone.

## 2. Target use cases
- Local app databases (desktop/CLI tools)
- Catalog/search-style datasets (artist/album/track scale and beyond)
- Embedded metadata stores for services
- Offline-first/local-first prototypes where a single file is desirable

## 3. Non-goals (MVP)
- Multi-process locking / shared-memory concurrency
- PostgreSQL wire protocol compatibility
- Full SQL: triggers, views, partial indexes, advanced planner, extensions
- Distributed / replication features

## 4. MVP requirements
### 4.1 Durability & transactions (Priority #1)
- WAL-based durability.
- Default commit mode is **safe**: commit must be durable after return.
- Recovery must be correct after process kill during writes.

### 4.2 Read performance (Priority #2)
- Single writer, multiple concurrent reader threads within a single process.
- Readers must observe a consistent snapshot.

### 4.3 SQL & relational features
- Tables with primary keys.
- Foreign keys with enforcement (MVP: RESTRICT/NO ACTION).
- Secondary indexes.
- SELECT with JOIN (INNER/LEFT), WHERE, ORDER BY, LIMIT/OFFSET, parameters.
- `LIKE '%pattern%'` support; optimization via trigram index is permitted but optional for MVP.

### 4.4 API surface (MVP internal)
- Native library API sufficient to:
  - open/close database
  - prepare/execute statements
  - bind parameters
  - iterate rows

## 5. Benchmarking & README performance chart (must-have)
The repository must provide an **at-a-glance bar chart** in the root README comparing DecentDB against common embedded engines.

### 5.1 Benchmark approach
Two benchmark layers are allowed; **only one is required for MVP**:

**MVP (required): Raw-engine benchmark runner**
- A native benchmark runner (Nim) measuring engine performance directly (no Python/ORM in hot path).
- Engines: SQLite (C API), DuckDB (C API), DecentDB (native/C ABI).
- Runner outputs *raw* JSON lines per run into `benchmarks/raw/` (or configured output path).

**Optional (non-MVP): Ecosystem-experience benchmarks**
- Python/SQLAlchemy “real usage” benches may exist but must be labeled clearly and not used for the README chart baseline.

### 5.2 README chart data file (required)
Agents must implement production of:
- `data/bench_summary.json`

This file is the single input used by `scripts/make_readme_chart.py` to render `assets/decentdb-benchmarks.svg`.

### 5.3 bench_summary.json generation plan (required)
Agents must implement an **aggregator** script that:
- Reads raw JSON benchmark outputs from `benchmarks/raw/*.jsonl` (or `*.json`).
- Computes the selected chart metrics (see §5.4).
- Writes `data/bench_summary.json` in the expected format.
- Ensures the output is deterministic given identical inputs.

Required output path:
- `data/bench_summary.json` (repo root)

Aggregator location:
- `scripts/aggregate_benchmarks.py`

### 5.4 Metrics to include in README chart (MVP)
At minimum, compute these 5 metrics (normalized by the chart script):
- `read_p95_ms` — point read latency p95 (lower better)
- `join_p95_ms` — join query latency p95 (lower better)
- `commit_p95_ms` — durable commit latency p95 (lower better)
- `insert_rows_per_sec` — insert throughput (higher better)
- `db_size_mb` — database size on disk after load (lower better)

### 5.5 Truth-in-benchmarking requirements
- The README must link to benchmark methodology and raw result files.
- The chart must be regenerated via scripts (not hand-edited).
- Include machine metadata (CPU, OS, filesystem) in `bench_summary.json`.

## 6. Acceptance criteria (MVP)
- DecentDB passes unit tests and crash/recovery tests for WAL semantics.
- Benchmark runner can execute at least 3 workloads (point read, join, commit latency) across SQLite/DuckDB/DecentDB.
- Aggregator generates `data/bench_summary.json`.
- `scripts/make_readme_chart.py` produces an SVG chart committed to `assets/`.
- Root README includes the SVG chart and regeneration instructions.
