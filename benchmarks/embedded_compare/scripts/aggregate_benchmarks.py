#!/usr/bin/env python3
"""Aggregate raw benchmark outputs into data/bench_summary.json.

This script is REQUIRED by PRD/SPEC.

Inputs:
  - benchmarks/raw/**/*.jsonl or *.json
Output:
  - data/bench_summary.json

Rules:
  - For each engine and metric, compute median-of-runs for p95 values.
  - Convert units: microseconds -> milliseconds, bytes -> MB.
  - Baseline engine SQLite must be present.
"""

import argparse
import json
import math
import os
import socket
from collections import defaultdict
from pathlib import Path
from statistics import median
from datetime import datetime, timezone


def iter_records(paths):
    for p in paths:
        if p.suffix == ".json":
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, list):
                for record in data:
                    yield record
            else:
                yield data
        elif p.suffix == ".jsonl":
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    yield json.loads(line)


def get_machine_info():
    hostname = socket.gethostname()
    cpu_info = "unknown"
    try:
        with open("/proc/cpuinfo", "r") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu_info = line.split(":", 1)[1].strip()
                    break
    except (IOError, OSError):
        pass
    return f"{hostname} ({cpu_info})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="benchmarks/embedded_compare/raw", help="Folder containing raw benchmark outputs")
    ap.add_argument("--output", default="benchmarks/embedded_compare/data/bench_summary.json", help="Output JSON path")
    args = ap.parse_args()

    inp = Path(args.input)
    outp = Path(args.output)

    files = [p for p in inp.rglob("*") if p.suffix in (".json", ".jsonl")]
    if not files:
        raise SystemExit(f"No .json/.jsonl files found under {inp}")

    records = list(iter_records(files))

    if not records:
        raise SystemExit("No valid benchmark records found")

    data = defaultdict(lambda: defaultdict(list))

    for record in records:
        engine = record.get("engine")
        benchmark = record.get("benchmark")
        metrics = record.get("metrics", {})
        artifacts = record.get("artifacts", {})

        if not engine or not benchmark:
            continue

        p95_us = metrics.get("p95_us")
        if p95_us is not None:
            data[(engine, benchmark)]["p95_us"].append(float(p95_us))

        if benchmark == "insert":
            ops_per_sec = metrics.get("ops_per_sec")
            if ops_per_sec is not None:
                data[(engine, "insert")]["ops_per_sec"].append(float(ops_per_sec))
            db_size_bytes = artifacts.get("db_size_bytes")
            if db_size_bytes is not None:
                data[(engine, "insert")]["db_size_bytes"].append(float(db_size_bytes))

    engines = {}

    for (engine, benchmark), values_dict in data.items():
        if engine not in engines:
            engines[engine] = {}

        if benchmark == "point_read":
            p95_us_values = values_dict.get("p95_us", [])
            if p95_us_values:
                engines[engine]["read_p95_ms"] = median(p95_us_values) / 1000.0
        elif benchmark == "join":
            p95_us_values = values_dict.get("p95_us", [])
            if p95_us_values:
                engines[engine]["join_p95_ms"] = median(p95_us_values) / 1000.0
        elif benchmark == "commit_latency":
            p95_us_values = values_dict.get("p95_us", [])
            if p95_us_values:
                engines[engine]["commit_p95_ms"] = median(p95_us_values) / 1000.0
        elif benchmark == "insert":
            ops_per_sec_values = values_dict.get("ops_per_sec", [])
            if ops_per_sec_values:
                engines[engine]["insert_rows_per_sec"] = median(ops_per_sec_values)
            db_size_bytes_values = values_dict.get("db_size_bytes", [])
            if db_size_bytes_values:
                engines[engine]["db_size_mb"] = median(db_size_bytes_values) / (1024.0 * 1024.0)

    if "SQLite" not in engines:
        raise SystemExit("Baseline engine 'SQLite' not found in benchmark data")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    machine = get_machine_info()

    result = {
        "metadata": {
            "run_id": run_id,
            "machine": machine,
            "notes": "Generated from raw benchmark outputs in benchmarks/raw/",
            "units": {
                "read_p95_ms": "ms (lower is better)",
                "join_p95_ms": "ms (lower is better)",
                "commit_p95_ms": "ms (lower is better)",
                "insert_rows_per_sec": "rows/sec (higher is better)",
                "db_size_mb": "MB (lower is better)"
            }
        },
        "engines": engines
    }

    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)

    print(f"Wrote aggregated results to: {outp}")
    print(f"  Engines: {', '.join(sorted(engines.keys()))}")
    print(f"  Run ID: {run_id}")


if __name__ == "__main__":
    main()
