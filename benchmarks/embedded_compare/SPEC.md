# SPEC — DecentDB Benchmarking for README Chart
**Status:** Draft

This spec defines the file formats and scripts needed to produce the README benchmark chart.

## 1. Raw benchmark output format (benchmarks/raw)
The raw runner must write one JSON object per measured operation batch.

### 1.1 File naming
- `benchmarks/raw/<engine>__<dataset>__<benchmark>__<runid>.jsonl`
- JSON Lines (one object per line) is preferred.

### 1.2 Required fields per record
```json
{
  "timestamp_utc": "2026-01-28T12:34:56Z",
  "engine": "SQLite|DuckDB|DecentDB",
  "engine_version": "string",
  "dataset": "small|large|custom",
  "benchmark": "point_read|join|commit_latency|insert|range_scan|like",
  "durability": "safe|fast",
  "threads": 1,
  "iterations": 10000,
  "metrics": {
    "latencies_us": [12, 15, 14, "... optional large array ..."],
    "p50_us": 12,
    "p95_us": 22,
    "p99_us": 30,
    "ops_per_sec": 123456.0,
    "rows_processed": 10000,
    "checksum_u64": 1234567890
  },
  "artifacts": {
    "db_path": "path or name",
    "db_size_bytes": 3502305280,
    "wal_size_bytes": 123456789
  },
  "environment": {
    "os": "Linux",
    "kernel": "string",
    "cpu": "string",
    "ram_gb": 64,
    "filesystem": "ext4",
    "notes": "optional"
  }
}
```

Notes:
- `latencies_us` may be omitted if the runner already computes p50/p95/p99.
- If omitted, `p50_us/p95_us/p99_us` are required for latency benchmarks.

## 2. Aggregated chart input format (data/bench_summary.json)
This file is consumed by `scripts/make_readme_chart.py`.

### 2.1 Required structure
```json
{
  "metadata": {
    "run_id": "string",
    "machine": "string",
    "notes": "string",
    "units": {
      "read_p95_ms": "ms (lower is better)",
      "join_p95_ms": "ms (lower is better)",
      "commit_p95_ms": "ms (lower is better)",
      "insert_rows_per_sec": "rows/sec (higher is better)",
      "db_size_mb": "MB (lower is better)"
    }
  },
  "engines": {
    "DecentDB": { "read_p95_ms": 1.8, "join_p95_ms": 4.9, "commit_p95_ms": 2.7, "insert_rows_per_sec": 120000, "db_size_mb": 620 },
    "SQLite":   { "read_p95_ms": 1.5, "join_p95_ms": 4.2, "commit_p95_ms": 2.2, "insert_rows_per_sec": 135000, "db_size_mb": 590 }
  }
}
```

### 2.2 Field rules
- All numeric values must be floats or ints; no strings like "1.2ms".
- Missing metrics are allowed (set to `null`), but README chart will omit them for that engine.
- `SQLite` must be present (used as normalization baseline by default in the chart generator script).

## 3. Aggregator script (scripts/aggregate_benchmarks.py)
### 3.1 Inputs
- Reads all files under `benchmarks/raw/` ending in `.jsonl` or `.json`.

### 3.2 Processing rules
For each engine:
- Compute:
  - `read_p95_ms`: from benchmark `point_read` p95
  - `join_p95_ms`: from benchmark `join` p95
  - `commit_p95_ms`: from benchmark `commit_latency` p95 (durability=safe)
  - `insert_rows_per_sec`: from benchmark `insert` ops/sec (or rows/sec) (durability=safe if applicable)
  - `db_size_mb`: from `artifacts.db_size_bytes` after dataset load

If multiple runs exist:
- Choose the **median** of p95 values across runs (robust to outliers).
- Document selection strategy in script comments.

### 3.3 Output
- Writes `data/bench_summary.json` (overwrite).
- Must include:
  - `metadata.run_id` (can be timestamp-based)
  - `metadata.machine` (hostname + CPU summary is fine)
  - `metadata.notes` (brief methodology pointer)

## 4. Chart generation script
The chart generator is provided:
- `scripts/make_readme_chart.py`
It expects `data/bench_summary.json` and outputs SVG/PNG in `assets/`.

## 5. README integration
Root README must include:
- The SVG image link: `assets/decentdb-benchmarks.svg`
- A short explanation of normalization
- Regeneration commands:
  - run raw benchmarks
  - aggregate
  - generate chart
