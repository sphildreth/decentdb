# DecentDB Benchmarking Guide

## Overview

This document serves as the canonical guide for how DecentDB measures, reports, and publishes its performance metrics against other embedded database engines. Benchmarking is a core tenet of the DecentDB project; our goal is not just to build a database, but to quantitatively prove its performance profile (fast point reads, durable ACID writes) against established industry baselines.

Because the embedded database ecosystem spans multiple languages and runtimes (C/C++, Rust, Java, .NET), obtaining fair, "apples-to-apples" measurements is challenging. To address this, our benchmarking strategy is split into two distinct tiers:

1. **Step 1: The Native Tier (Rust-Based)**
2. **Step 2: The Polyglot Tier (Python-Based Framework)**

This guide explains the architecture of both tiers, how to run them, and how their results are aggregated into the final visualizations found in our README.

---

## Step 1: The Native Tier (Rust-Based)

### Rationale

When comparing DecentDB (written in Rust) to engines like SQLite (C) and DuckDB (C++), it is critical to measure raw engine performance without the overhead of Foreign Function Interfaces (FFI), inter-process communication, or garbage collection.

To achieve this, we maintain a native Rust benchmarking harness that compiles SQLite (via `rusqlite`), DuckDB (via `duckdb` crate), and DecentDB directly into a single binary.

### Architecture

The native tier lives entirely within `crates/decentdb/benches/embedded_compare.rs`.

It defines a `DatabaseBenchmarker` trait that each engine implements. This trait enforces a standard lifecycle:
1. `setup()`: Creates a fresh database in an isolated temporary directory.
2. `insert_batch()`: Measures raw bulk insert throughput.
3. `random_reads()`: Measures p95 latency of point `SELECT` queries.
4. `durable_commits()`: Measures p95 latency of individual synchronous commits (testing WAL/fsync).
5. `teardown()`: Safely closes connections and measures the final database size on disk.

Latency collection detail:
- Per-operation latencies are captured in **nanoseconds** in the harness and then reported as milliseconds (`*_p95_ms`) in JSON/output. This allows sub-microsecond differences (for example `0.0010 ms` vs `0.0001 ms`) to be visible when the platform timer supports it.

### Execution

To run the native benchmarks, use standard Cargo tooling from the project root:

```bash
cd crates/decentdb
cargo bench --bench embedded_compare
```

**Output:**
The runner executes the suite and serializes the results into `decentdb/data/bench_summary.json`. This JSON file acts as the bridge between the Rust runner and our charting scripts.

---

## Step 2: The Polyglot Tier (Python-Based Framework)

### Rationale

While the Native Tier is perfect for C/C++/Rust engines, it cannot fairly benchmark managed databases like H2 (Java), Apache Derby (Java), HSQLDB (Java), or LiteDB (.NET). Attempting to embed a JVM or CLR inside a Rust micro-benchmark loop introduces massive startup and crossing overheads that invalidate the results.

To include these systems, we implement a robust Python-based testing framework. Python acts as the orchestrator, utilizing appropriate bridges (`jaydebeapi` for JDBC, subprocess execution for .NET) to interface with these databases.

*(Note: This framework is thoroughly detailed in `design/COMPARISON_BENCHMARK_PLAN.md`)*

### Framework Architecture

The framework is organized under `decentdb/benchmarks/python_embedded_compare/` and follows this structure:

```text
python_embedded_compare/
├── config/
│   └── database_configs.yaml      # Connection strings and driver paths
├── drivers/
│   ├── base_driver.py             # Abstract base class
│   ├── sqlite_driver.py           # Uses built-in sqlite3
│   ├── jdbc_driver.py             # Wrapper for H2, Derby, HSQLDB via JayDeBeApi
│   └── litedb_driver.py           # Subprocess wrapper for .NET harness
├── scenarios/
│   └── canonical_workloads.py     # OLTP-ish Orders & Web Analytics scenarios
├── utils/
│   ├── dataset_generator.py       # Deterministic seeded data generator
│   └── performance_timer.py       # High-precision ns/us timing
└── comparison_runner.py           # Main CLI entrypoint
```

### The Fairness Contract

To ensure mature and robust comparisons across drastically different runtimes, the Python framework enforces strict rules:

1. **Deterministic Datasets:** `utils/dataset_generator.py` uses a fixed `dataset_seed` to generate identical records in the same order for every database.
2. **Explicit Commit Boundaries:** Drivers must run with auto-commit disabled. Benchmarks explicitly call `commit()` to measure batched vs. individual durable commits accurately.
3. **Durability Modes:** Benchmarks are run twice:
   - *Durable Mode:* `fsync` is strictly enforced (e.g., SQLite `synchronous=FULL`).
   - *Relaxed Mode:* `fsync` is deferred (e.g., SQLite `synchronous=NORMAL`).
4. **Environment Pinning:** The framework outputs a `manifest.json` recording OS, CPU governor, file system type, and specific driver versions to ensure reproducibility.

### Managing JDBC / JVM Overheads

A major challenge in benchmarking Java engines via Python is the overhead of the Python-to-Java bridge (like `JayDeBeApi` or `JPype1`). To ensure robustness:
- The framework utilizes "warm-up" iterations (e.g., discarding the first 10% of queries) to allow the JVM's Just-In-Time (JIT) compiler to optimize the execution paths before we begin recording latencies.
- If bridge overhead proves too high for accurate microsecond measurements, the framework supports a "delegate" mode, where a tiny, pre-compiled Java JAR executes the benchmark loop internally and simply reports the JSON results back to the Python orchestrator via stdout.

### Execution

```bash
cd benchmarks/python_embedded_compare
pip install -r requirements.txt
python comparison_runner.py --scenario workload_a --durability durable
```

**Output:**
This runner produces an enriched JSON structure in `benchmarks/python_embedded_compare/out/results_merged.json`. 

---

## Step 3: Aggregation and Visualization

Both the Native Tier and the Polyglot Tier ultimately produce structured JSON data.

The Python script `scripts/make_readme_chart.py` is responsible for consuming `data/bench_summary.json` (and optionally merging data from the Polyglot Tier) to generate the SVG charts.

```bash
python scripts/make_readme_chart.py
```

This generates `assets/decentdb-benchmarks.svg`, which is displayed prominently in the repository README, ensuring users always see an up-to-date, scientifically sound representation of DecentDB's performance landscape.
