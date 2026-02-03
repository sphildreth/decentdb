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

NOTE: This is an initial stub. Implement per SPEC.md §3.
"""

import argparse, json, math, os
from pathlib import Path
from statistics import median

def iter_records(paths):
    for p in paths:
        if p.suffix == ".json":
            yield json.loads(p.read_text(encoding="utf-8"))
        elif p.suffix == ".jsonl":
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="benchmarks/raw", help="Folder containing raw benchmark outputs")
    ap.add_argument("--output", default="data/bench_summary.json", help="Output JSON path")
    args = ap.parse_args()

    inp = Path(args.input)
    outp = Path(args.output)

    files = [p for p in inp.rglob("*") if p.suffix in (".json", ".jsonl")]
    if not files:
        raise SystemExit(f"No .json/.jsonl files found under {inp}")

    # TODO: Implement per SPEC.md
    # For now, just copy example if present
    example = Path("data/bench_summary.example.json")
    if example.exists():
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
        return

    raise SystemExit("Implement aggregation per SPEC.md")

if __name__ == "__main__":
    main()
