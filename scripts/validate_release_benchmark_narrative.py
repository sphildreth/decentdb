#!/usr/bin/env python3
"""Validate that release benchmark assets tell one coherent performance story."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


METRICS = [
    ("read_p95_ms", "Point read p95", "lower"),
    ("join_p95_ms", "Join p95", "lower"),
    ("range_scan_p95_ms", "Range scan p95", "lower"),
    ("aggregate_p95_ms", "Aggregate p95", "lower"),
    ("concurrent_read_p95_ms", "Concurrent read p95", "lower"),
    ("commit_p95_ms", "Commit p95", "lower"),
    ("insert_rows_per_sec", "Insert throughput", "higher"),
]

REQUIRED_ENGINES = {
    "decentdb_balanced_durable",
    "decentdb_low_memory_durable",
    "decentdb_tuned_durable",
    "duckdb_engine_default",
    "sqlite",
}


@dataclass(frozen=True)
class RustBaselineRun:
    path: Path
    scale: str
    profile: str
    started_unix: int
    engine_version: str
    total_seconds: float


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"{path} is missing or invalid JSON: {error}") from error


def as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def speedup(base: float, value: float, direction: str) -> float | None:
    if base <= 0.0 or value <= 0.0:
        return None
    if direction == "higher":
        return value / base
    return base / value


def rust_baseline_runs(directory: Path) -> list[RustBaselineRun]:
    if not directory.exists():
        return []

    runs: list[RustBaselineRun] = []
    for path in sorted(directory.glob("*.json")):
        document = load_json(path)
        steps = document.get("steps", [])
        if not isinstance(steps, list):
            continue
        total = 0.0
        for step in steps:
            if isinstance(step, dict):
                total += float(step.get("duration_seconds") or 0.0)
        runs.append(
            RustBaselineRun(
                path=path,
                scale=str(document.get("scale_name", "")),
                profile=str(document.get("benchmark_profile", "default")),
                started_unix=int(document.get("started_unix", 0)),
                engine_version=str(document.get("engine_version", "")),
                total_seconds=total,
            )
        )
    return runs


def latest_rust_baseline_run(
    directory: Path,
    *,
    scale: str,
    profile: str,
) -> RustBaselineRun | None:
    matches = [
        run
        for run in rust_baseline_runs(directory)
        if run.scale == scale and run.profile == profile
    ]
    if not matches:
        return None
    return max(matches, key=lambda run: (run.started_unix, run.path.name))


def validate_summary(
    summary_path: Path,
    *,
    min_tuned_sqlite_wins: int,
) -> tuple[list[str], list[str]]:
    issues: list[str] = []
    report: list[str] = []

    summary = load_json(summary_path)
    engines = summary.get("engines", {})
    metadata = summary.get("metadata", {})
    if not isinstance(engines, dict):
        return ["benchmark summary does not contain an engines object"], report
    if not isinstance(metadata, dict):
        metadata = {}

    missing = sorted(REQUIRED_ENGINES - set(engines))
    if missing:
        issues.append(
            "benchmark summary is missing release engines: " + ", ".join(missing)
        )
        return issues, report

    process_coordination = str(metadata.get("decentdb_process_coordination", ""))
    if process_coordination != "single_process_unsafe":
        issues.append(
            "release README benchmark summary must mark DecentDB rows as "
            "process_coordination=single_process_unsafe; got "
            f"{process_coordination!r}"
        )

    for key in (
        "decentdb_profile_balanced",
        "decentdb_profile_default",
        "decentdb_profile_low_memory",
        "decentdb_profile_tuned",
    ):
        value = str(metadata.get(key, ""))
        if "process_coordination=single_process_unsafe" not in value:
            issues.append(
                f"metadata[{key!r}] must include "
                "process_coordination=single_process_unsafe"
            )

    sqlite = engines["sqlite"]
    tuned = engines["decentdb_tuned_durable"]
    if not isinstance(sqlite, dict) or not isinstance(tuned, dict):
        issues.append("sqlite and decentdb_tuned_durable metrics must be objects")
        return issues, report

    wins = 0
    metric_lines: list[str] = []
    for key, label, direction in METRICS:
        base = as_float(sqlite.get(key))
        value = as_float(tuned.get(key))
        if base is None or value is None:
            metric_lines.append(f"- {label}: missing")
            continue
        ratio = speedup(base, value, direction)
        if ratio is None:
            metric_lines.append(f"- {label}: invalid base={base} value={value}")
            continue
        if ratio > 1.0:
            wins += 1
        metric_lines.append(f"- {label}: {ratio:.3f}x vs SQLite")

    report.append("README chart release summary")
    report.append(
        "- benchmark family: cross-engine single-process embedded comparison"
    )
    report.append(f"- DecentDB process coordination: {process_coordination or 'missing'}")
    report.append(
        f"- DecentDB tuned durable wins: {wins}/{len(METRICS)} "
        f"(minimum {min_tuned_sqlite_wins})"
    )
    report.extend(metric_lines)

    if wins < min_tuned_sqlite_wins:
        issues.append(
            "decentdb_tuned_durable wins only "
            f"{wins}/{len(METRICS)} README chart metrics vs SQLite; "
            f"minimum is {min_tuned_sqlite_wins}. Do not publish a release "
            "chart that communicates a severe tuned-profile degradation "
            "without accepting and documenting that regression."
        )

    return issues, report


def rust_baseline_report(
    history_dir: Path,
    current_dir: Path,
    *,
    scale: str,
    profile: str,
    max_current_vs_latest: float,
) -> tuple[list[str], list[str]]:
    issues: list[str] = []
    report: list[str] = []

    history = latest_rust_baseline_run(history_dir, scale=scale, profile=profile)
    current = latest_rust_baseline_run(current_dir, scale=scale, profile=profile)
    report.append("")
    report.append("Rust raw-engine baseline cross-check")
    report.append(f"- scale/profile: {scale}/{profile}")
    if history is None:
        report.append(f"- historical latest: missing under {history_dir}")
        return issues, report
    report.append(
        f"- historical latest: {history.path.name} "
        f"v{history.engine_version} total={history.total_seconds:.3f}s"
    )
    if current is None:
        report.append(f"- current run: missing under {current_dir}")
        return issues, report

    ratio = (
        current.total_seconds / history.total_seconds
        if history.total_seconds > 0.0
        else 0.0
    )
    report.append(
        f"- current run: {current.path.name} "
        f"v{current.engine_version} total={current.total_seconds:.3f}s"
    )
    report.append(f"- current/latest total ratio: {ratio:.3f}x")
    if ratio > max_current_vs_latest:
        issues.append(
            "rust-baseline current total runtime is "
            f"{ratio:.3f}x latest historical for {scale}/{profile}; "
            f"maximum allowed is {max_current_vs_latest:.3f}x"
        )

    return issues, report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate release-facing benchmark narrative"
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("data/bench_summary.json"),
        help="Merged README benchmark summary",
    )
    parser.add_argument(
        "--min-tuned-sqlite-wins",
        type=int,
        default=4,
        help="Minimum README chart metrics where tuned DecentDB must beat SQLite",
    )
    parser.add_argument(
        "--rust-baseline-history-dir",
        type=Path,
        default=Path("benchmarks/rust-baseline/results"),
    )
    parser.add_argument(
        "--rust-baseline-current-dir",
        type=Path,
        default=Path(".tmp/rust-baseline-current"),
    )
    parser.add_argument("--rust-baseline-scale", default="full")
    parser.add_argument("--rust-baseline-profile", default="default")
    parser.add_argument(
        "--max-rust-baseline-current-vs-latest",
        type=float,
        default=1.25,
        help="Fail if current rust-baseline total exceeds latest historical by this ratio",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="Optional path to write the validation report",
    )
    args = parser.parse_args()

    issues, report = validate_summary(
        args.summary,
        min_tuned_sqlite_wins=args.min_tuned_sqlite_wins,
    )
    baseline_issues, baseline_report = rust_baseline_report(
        args.rust_baseline_history_dir,
        args.rust_baseline_current_dir,
        scale=args.rust_baseline_scale,
        profile=args.rust_baseline_profile,
        max_current_vs_latest=args.max_rust_baseline_current_vs_latest,
    )
    issues.extend(baseline_issues)
    report.extend(baseline_report)

    output = "\n".join(report) + "\n"
    print(output, end="")
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output, encoding="utf-8")

    if issues:
        print("Benchmark narrative validation failed:", file=sys.stderr)
        for issue in issues:
            print(f"- {issue}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
