#!/usr/bin/env python3
"""Merge README benchmark inputs into data/bench_summary.json.

This script treats the native Rust benchmark summary as authoritative for the
engines measured directly by `cargo bench -p decentdb --bench embedded_compare`
and then optionally layers in additional engines from
`benchmarks/python_embedded_compare/out/results_merged.json`.
"""

import argparse
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path


def _safe_float(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _canonical_engine_name(engine):
    return str(engine).strip().lower()


def _engine_exists(engines, engine_name):
    target = _canonical_engine_name(engine_name)
    return any(_canonical_engine_name(existing) == target for existing in engines)


def _pick_nearest(records, target_operations):
    best = None
    best_distance = None
    for record in records:
        operations = record.get("operations", record.get("n_ops"))
        if operations is None:
            continue
        distance = abs(int(operations) - int(target_operations))
        if best is None or distance < best_distance:
            best = record
            best_distance = distance
    return best


def load_native_summary(path):
    if not path.exists():
        raise SystemExit(
            f"Native benchmark summary not found: {path}\n"
            "Run `cargo bench -p decentdb --bench embedded_compare` first."
        )

    with path.open("r", encoding="utf-8") as handle:
        document = json.load(handle)

    engines = document.get("engines")
    if not isinstance(engines, dict) or not engines:
        raise SystemExit(f"Invalid native summary at {path}: missing `engines` object")

    metadata = document.get("metadata")
    if metadata is None:
        metadata = {}
    if not isinstance(metadata, dict):
        raise SystemExit(f"Invalid native summary at {path}: `metadata` must be an object")

    if not _engine_exists(engines, "sqlite"):
        raise SystemExit(
            f"Baseline engine `sqlite` not found in native summary {path}"
        )

    return {
        "engines": deepcopy(engines),
        "metadata": deepcopy(metadata),
    }


def merge_python_embedded_compare_results(summary, py_results_path, target_operations):
    path = Path(py_results_path)
    if not path.exists():
        return {
            "merged": False,
            "reason": f"results file not found: {path}",
            "engines": [],
        }

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "merged": False,
            "reason": f"could not parse JSON ({exc})",
            "engines": [],
        }

    results = document.get("results")
    if not isinstance(results, list):
        return {
            "merged": False,
            "reason": "missing `results` list",
            "engines": [],
        }

    name_map = {
        "H2(JDBC)": "H2",
        "Derby(JDBC)": "Apache Derby",
        "HSQLDB(JDBC)": "HSQLDB",
        "LiteDB": "LiteDB",
        "Firebird": "Firebird",
    }

    grouped = {}
    for row in results:
        engine = row.get("engine")
        benchmark = row.get("benchmark", row.get("bench"))
        if not engine or not benchmark:
            continue
        grouped.setdefault((engine, benchmark), []).append(row)

    merged_engines = []
    for (python_engine, benchmark), rows in grouped.items():
        output_name = name_map.get(python_engine)
        if output_name is None:
            continue

        if _engine_exists(summary["engines"], output_name):
            continue

        chosen = _pick_nearest(rows, target_operations)
        if chosen is None:
            continue

        latency_ms = chosen.get("latency_ms", {})
        if not isinstance(latency_ms, dict):
            latency_ms = {}

        engine_metrics = summary["engines"].setdefault(output_name, {})
        updated = False

        if benchmark == "point_select":
            p95_ms = _safe_float(latency_ms.get("p95_ms"))
            if p95_ms is None:
                p95_us_per_op = _safe_float(chosen.get("p95_us_per_op"))
                if p95_us_per_op is not None:
                    p95_ms = p95_us_per_op / 1000.0
            if p95_ms is not None and p95_ms != 0:
                engine_metrics["read_p95_ms"] = p95_ms
                updated = True

        elif benchmark in ("insert", "insert_txn"):
            throughput = _safe_float(chosen.get("throughput_ops_sec"))
            if throughput is None:
                p50_ms = _safe_float(latency_ms.get("p50_ms"))
                if p50_ms is not None and p50_ms != 0:
                    throughput = 1000.0 / p50_ms
                else:
                    p50_us_per_op = _safe_float(chosen.get("p50_us_per_op"))
                    if p50_us_per_op is not None and p50_us_per_op != 0:
                        throughput = 1_000_000.0 / p50_us_per_op
            if throughput is not None and throughput != 0:
                engine_metrics["insert_rows_per_sec"] = throughput
                updated = True

        if updated and output_name not in merged_engines:
            merged_engines.append(output_name)
        elif not updated and not engine_metrics:
            summary["engines"].pop(output_name, None)

    if not merged_engines:
        return {
            "merged": False,
            "reason": "no additional engine metrics could be derived",
            "engines": [],
        }

    note = f"merged extra engines from {path}"
    existing_note = summary["metadata"].get("notes")
    summary["metadata"]["notes"] = (
        f"{existing_note}; {note}" if existing_note else note
    )
    summary["metadata"]["aggregated_at"] = datetime.now(timezone.utc).isoformat()
    summary["metadata"]["python_merge_target_operations"] = target_operations

    return {
        "merged": True,
        "reason": "",
        "engines": merged_engines,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Merge native and Python benchmark summaries for README charts"
    )
    parser.add_argument(
        "--native-summary",
        default="data/bench_summary.json",
        help="Native benchmark summary generated by cargo bench",
    )
    parser.add_argument(
        "--python-embedded-compare-results",
        default="benchmarks/python_embedded_compare/out/results_merged.json",
        help="Optional results file from the Python embedded comparison harness",
    )
    parser.add_argument(
        "--output",
        default="data/bench_summary.json",
        help="Output path for the merged benchmark summary",
    )
    parser.add_argument(
        "--target-operations",
        type=int,
        default=100_000,
        help="Operation count to select from the Python embedded comparison results",
    )
    args = parser.parse_args()

    native_summary_path = Path(args.native_summary)
    output_path = Path(args.output)

    summary = load_native_summary(native_summary_path)
    merge_result = merge_python_embedded_compare_results(
        summary,
        args.python_embedded_compare_results,
        args.target_operations,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(f"Wrote benchmark summary to: {output_path}")
    print(f"  Engines: {', '.join(summary['engines'].keys())}")
    if merge_result["merged"]:
        print(
            "  Merged Python comparison engines: "
            + ", ".join(merge_result["engines"])
        )
    else:
        print(f"  Python comparison merge skipped: {merge_result['reason']}")


if __name__ == "__main__":
    main()
