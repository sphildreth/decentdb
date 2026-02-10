#!/usr/bin/env python3
"""Aggregate raw benchmark outputs into data/bench_summary.json.

This script is REQUIRED by PRD/SPEC.

Inputs:
    - benchmarks/embedded_compare/raw/sample/**/*.jsonl or *.json (default)
    - pass --input to aggregate a different raw folder
Output:
  - data/bench_summary.json

Rules:
  - For each engine and metric, compute median-of-runs for p95 values.
  - Convert units: nanoseconds/microseconds -> milliseconds, bytes -> MB.
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


def _safe_float(x):
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _pick_nearest(records, n_ops_target):
    """Pick record with n_ops closest to target."""
    best = None
    best_dist = None
    for r in records:
        n = r.get("n_ops")
        if n is None:
            continue
        dist = abs(int(n) - int(n_ops_target))
        if best is None or dist < best_dist:
            best = r
            best_dist = dist
    return best


def merge_python_embedded_compare_results(engines, py_results_path):
    """Merge additional engines from benchmarks/python_embedded_compare.

    This is intentionally best-effort and only fills metrics we can derive.
    Missing metrics remain absent (or null), and the chart generator will
    omit those bars.
    """
    p = Path(py_results_path)
    if not p.exists():
        return False

    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return False

    results = doc.get("results")
    if not isinstance(results, list):
        return False

    # Map python engine names -> embedded_compare display names.
    name_map = {
        "H2(JDBC)": "H2",
        "Derby(JDBC)": "Apache Derby",
        "HSQLDB(JDBC)": "HSQLDB",
        "LiteDB": "LiteDB",
        "Firebird": "Firebird",
    }

    # Group python rows by (engine, bench)
    grouped = defaultdict(list)
    for row in results:
        eng = row.get("engine")
        bench = row.get("bench")
        if not eng or not bench:
            continue
        grouped[(eng, bench)].append(row)

    # We align to embedded_compare's point_read iterations=100000.
    target_n_ops = 100_000

    for (py_engine, py_bench), rows in grouped.items():
        out_name = name_map.get(py_engine)
        if out_name is None:
            # Ignore SQLite variants / DecentDB / DuckDB from python run to
            # avoid conflicting with native embedded_compare results.
            continue

        if out_name in ("SQLite", "DecentDB", "DuckDB"):
            continue

        if out_name not in engines:
            engines[out_name] = {}

        chosen = _pick_nearest(rows, target_n_ops)
        if chosen is None:
            continue

        if py_bench == "point_select":
            p95_us_per_op = _safe_float(chosen.get("p95_us_per_op"))
            if p95_us_per_op is not None and p95_us_per_op != 0:
                engines[out_name]["read_p95_ms"] = p95_us_per_op / 1000.0

        elif py_bench == "insert_txn":
            # Convert p50 us/op into rows/sec (higher is better).
            p50_us_per_op = _safe_float(chosen.get("p50_us_per_op"))
            if p50_us_per_op is not None and p50_us_per_op != 0:
                engines[out_name]["insert_rows_per_sec"] = 1_000_000.0 / p50_us_per_op

    return True


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
    ap.add_argument(
        "--input",
        default="benchmarks/embedded_compare/raw/sample",
        help="Folder containing raw benchmark outputs",
    )
    ap.add_argument(
        "--output",
        default="benchmarks/embedded_compare/data/bench_summary.json",
        help="Output JSON path",
    )
    ap.add_argument(
        "--python-embedded-compare-results",
        default="benchmarks/python_embedded_compare/out/results_merged.json",
        help="Optional results file from python_embedded_compare to merge extra engines",
    )
    args = ap.parse_args()

    inp = Path(args.input)
    outp = Path(args.output)

    files = [p for p in inp.rglob("*") if p.suffix in (".json", ".jsonl")]
    if not files:
        raise SystemExit(f"No .json/.jsonl files found under {inp}")

    records = list(iter_records(files))

    if not records:
        raise SystemExit("No valid benchmark records found")

    # Enforce that we are aggregating a single durability profile.
    # Mixing safe/default (or other) profiles makes the summary misleading.
    all_durabilities = sorted({r.get("durability") for r in records if r.get("durability")})
    if len(all_durabilities) > 1:
        raise SystemExit(
            "Mixed durability profiles found in input: "
            + ", ".join(all_durabilities)
            + ". Re-run with a clean output directory or aggregate a single run folder."
        )
    durability_profile = all_durabilities[0] if all_durabilities else "unknown"

    data = defaultdict(lambda: defaultdict(list))

    for record in records:
        engine = record.get("engine")
        benchmark = record.get("benchmark")
        metrics = record.get("metrics", {})
        artifacts = record.get("artifacts", {})

        if not engine or not benchmark:
            continue

        # Prefer nanosecond percentiles when available to avoid microsecond
        # quantization for very fast operations.
        p95_ns = metrics.get("p95_ns")
        if p95_ns is not None:
            data[(engine, benchmark)]["p95_ns"].append(float(p95_ns))

        p95_us = metrics.get("p95_us")
        if p95_us is not None:
            data[(engine, benchmark)]["p95_us"].append(float(p95_us))

        if benchmark == "insert":
            ops_per_sec = metrics.get("ops_per_sec")
            if ops_per_sec is not None:
                data[(engine, "insert")]["ops_per_sec"].append(float(ops_per_sec))

    engines = {}

    for (engine, benchmark), values_dict in data.items():
        if engine not in engines:
            engines[engine] = {}

        if benchmark == "point_read":
            p95_ns_values = values_dict.get("p95_ns", [])
            p95_us_values = values_dict.get("p95_us", [])
            if p95_ns_values:
                engines[engine]["read_p95_ms"] = median(p95_ns_values) / 1_000_000.0
            elif p95_us_values:
                engines[engine]["read_p95_ms"] = median(p95_us_values) / 1000.0
        elif benchmark == "join":
            p95_ns_values = values_dict.get("p95_ns", [])
            p95_us_values = values_dict.get("p95_us", [])
            if p95_ns_values:
                engines[engine]["join_p95_ms"] = median(p95_ns_values) / 1_000_000.0
            elif p95_us_values:
                engines[engine]["join_p95_ms"] = median(p95_us_values) / 1000.0
        elif benchmark == "commit_latency":
            p95_ns_values = values_dict.get("p95_ns", [])
            p95_us_values = values_dict.get("p95_us", [])
            if p95_ns_values:
                engines[engine]["commit_p95_ms"] = median(p95_ns_values) / 1_000_000.0
            elif p95_us_values:
                engines[engine]["commit_p95_ms"] = median(p95_us_values) / 1000.0
        elif benchmark == "insert":
            ops_per_sec_values = values_dict.get("ops_per_sec", [])
            if ops_per_sec_values:
                engines[engine]["insert_rows_per_sec"] = median(ops_per_sec_values)

    if "SQLite" not in engines:
        raise SystemExit("Baseline engine 'SQLite' not found in benchmark data")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    machine = get_machine_info()

    result = {
        "metadata": {
            "run_id": run_id,
            "machine": machine,
            "durability_profile": durability_profile,
            "notes": f"Generated from raw benchmark outputs in {inp}",
            "units": {
                "read_p95_ms": "ms (lower is better)",
                "join_p95_ms": "ms (lower is better)",
                "commit_p95_ms": "ms (lower is better)",
                "insert_rows_per_sec": "rows/sec (higher is better)",
            },
        },
        "engines": engines,
    }

    merged = merge_python_embedded_compare_results(
        result["engines"],
        args.python_embedded_compare_results,
    )
    if merged:
        result["metadata"]["notes"] += f"; merged extra engines from {args.python_embedded_compare_results}"

    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)

    print(f"Wrote aggregated results to: {outp}")
    print(f"  Engines: {', '.join(sorted(engines.keys()))}")
    print(f"  Run ID: {run_id}")


if __name__ == "__main__":
    main()
