#!/usr/bin/env python3
"""Compare rust-baseline benchmark history with optional current runs.

The historical JSON files in benchmarks/rust-baseline/results are useful for
spotting performance step changes. This helper keeps ad hoc current runs under
.tmp/ comparable without modifying the historical result directory.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_HISTORY_DIR = Path("benchmarks/rust-baseline/results")
DEFAULT_CURRENT_DIR = Path(".tmp/rust-baseline-current")

STEP_ORDER = [
    "connect_open",
    "schema_create",
    "seed_artists",
    "seed_albums",
    "seed_songs",
    "query_count_songs",
    "query_aggregate_durations",
    "query_artist_by_id",
    "query_top10_artists_by_songs",
    "query_top10_albums_by_songs",
    "query_view_first_1000",
    "query_songs_for_artist_via_view",
]


@dataclass(frozen=True)
class Run:
    path: Path
    scale: str
    profile: str
    started_unix: int
    engine_version: str
    total_seconds: float
    peak_rss_bytes: int
    database_size_bytes: int
    wal_size_bytes: int
    steps: dict[str, dict[str, Any]]

    @property
    def label(self) -> str:
        stamp = datetime.fromtimestamp(self.started_unix, timezone.utc)
        return stamp.strftime("%Y-%m-%d %H:%M")


def load_runs(directory: Path) -> list[Run]:
    runs: list[Run] = []
    if not directory.exists():
        return runs

    for path in sorted(directory.glob("*.json")):
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
        steps = {
            str(step["name"]): step
            for step in document.get("steps", [])
            if isinstance(step, dict) and "name" in step
        }
        total_seconds = sum(
            float(step.get("duration_seconds", 0.0))
            for step in steps.values()
        )
        runs.append(
            Run(
                path=path,
                scale=str(document.get("scale_name", "")),
                profile=str(document.get("benchmark_profile", "default")),
                started_unix=int(document.get("started_unix", 0)),
                engine_version=str(document.get("engine_version", "")),
                total_seconds=total_seconds,
                peak_rss_bytes=int(document.get("peak_rss_bytes", 0)),
                database_size_bytes=int(document.get("database_size_bytes", 0)),
                wal_size_bytes=int(document.get("wal_size_bytes", 0)),
                steps=steps,
            )
        )
    return runs


def step_duration(run: Run, name: str) -> float | None:
    step = run.steps.get(name)
    if step is None:
        return None
    value = step.get("duration_seconds")
    return None if value is None else float(value)


def step_throughput(run: Run, name: str) -> float | None:
    step = run.steps.get(name)
    if step is None:
        return None
    value = step.get("records_per_second")
    return None if value is None else float(value)


def format_bytes(value: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    number = float(value)
    unit = units[0]
    for unit in units:
        if number < 1024.0 or unit == units[-1]:
            break
        number /= 1024.0
    return f"{number:.1f}{unit}"


def format_seconds(value: float | None) -> str:
    if value is None:
        return "-"
    if value < 0.001:
        return f"{value * 1_000_000:.0f}us"
    if value < 1.0:
        return f"{value * 1000:.1f}ms"
    return f"{value:.3f}s"


def format_ratio(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}x"


def group_by_scale(runs: list[Run]) -> dict[str, list[Run]]:
    grouped: dict[str, list[Run]] = {}
    for run in runs:
        grouped.setdefault(run.scale, []).append(run)
    for scale_runs in grouped.values():
        scale_runs.sort(key=lambda run: (run.started_unix, run.path.name))
    return dict(sorted(grouped.items()))


def first_regression(
    runs: list[Run],
    step_name: str,
    threshold: float,
) -> tuple[Run, Run, float] | None:
    best: Run | None = None
    best_duration: float | None = None
    for run in runs:
        duration = step_duration(run, step_name)
        if duration is None or duration <= 0.0:
            continue
        if best_duration is not None and duration / best_duration >= threshold:
            return best, run, duration / best_duration
        if best_duration is None or duration < best_duration:
            best = run
            best_duration = duration
    return None


def print_run_summary(title: str, run: Run) -> None:
    print(
        f"{title}: {run.path.name} {run.label} "
        f"v{run.engine_version} profile={run.profile} total={run.total_seconds:.2f}s "
        f"rss={format_bytes(run.peak_rss_bytes)} "
        f"db={format_bytes(run.database_size_bytes)} "
        f"wal={format_bytes(run.wal_size_bytes)}"
    )


def print_scale_report(
    scale: str,
    history: list[Run],
    current: list[Run],
    threshold: float,
) -> None:
    print(f"\n## {scale}")
    if not history:
        print("No historical runs.")
        return

    best = min(history, key=lambda run: run.total_seconds)
    latest = max(history, key=lambda run: (run.started_unix, run.path.name))
    print_run_summary("Best historical", best)
    print_run_summary("Latest historical", latest)

    if current:
        current_latest = max(current, key=lambda run: (run.started_unix, run.path.name))
        print_run_summary("Current", current_latest)
        print(
            "Current total ratio: "
            f"{current_latest.total_seconds / best.total_seconds:.2f}x best, "
            f"{current_latest.total_seconds / latest.total_seconds:.2f}x latest"
        )

    print("\nStep comparisons:")
    print("step                               best      latest    current   cur/best cur/latest")
    for step_name in STEP_ORDER:
        best_value = step_duration(best, step_name)
        latest_value = step_duration(latest, step_name)
        current_value = step_duration(current[-1], step_name) if current else None
        current_best = (
            current_value / best_value
            if current_value is not None and best_value not in (None, 0.0)
            else None
        )
        current_latest = (
            current_value / latest_value
            if current_value is not None and latest_value not in (None, 0.0)
            else None
        )
        print(
            f"{step_name:<34} "
            f"{format_seconds(best_value):>8} "
            f"{format_seconds(latest_value):>9} "
            f"{format_seconds(current_value):>9} "
            f"{format_ratio(current_best):>8} "
            f"{format_ratio(current_latest):>10}"
        )

    print("\nFirst regressions by step:")
    for step_name in STEP_ORDER:
        regression = first_regression(history, step_name, threshold)
        if regression is None:
            continue
        baseline, regressed, ratio = regression
        print(
            f"- {step_name}: {ratio:.2f}x at {regressed.path.name} "
            f"versus {baseline.path.name}"
        )

    print("\nSeed throughput:")
    for step_name in ("seed_artists", "seed_albums", "seed_songs"):
        best_rps = step_throughput(best, step_name)
        latest_rps = step_throughput(latest, step_name)
        current_rps = step_throughput(current[-1], step_name) if current else None
        print(
            f"- {step_name}: best={best_rps or 0:.0f} r/s "
            f"latest={latest_rps or 0:.0f} r/s "
            f"current={current_rps or 0:.0f} r/s"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare rust-baseline historical and current JSON results"
    )
    parser.add_argument(
        "--history-dir",
        type=Path,
        default=DEFAULT_HISTORY_DIR,
        help="Directory containing checked-in historical rust-baseline JSON files",
    )
    parser.add_argument(
        "--current-dir",
        type=Path,
        default=DEFAULT_CURRENT_DIR,
        help="Directory containing current ad hoc rust-baseline JSON files",
    )
    parser.add_argument(
        "--scale",
        action="append",
        help="Scale to report. May be repeated. Defaults to every scale found.",
    )
    parser.add_argument(
        "--regression-threshold",
        type=float,
        default=1.5,
        help="Ratio used to report first large historical regression by step",
    )
    args = parser.parse_args()

    history_by_scale = group_by_scale(load_runs(args.history_dir))
    current_by_scale = group_by_scale(load_runs(args.current_dir))
    scales = args.scale or sorted(set(history_by_scale) | set(current_by_scale))

    print(f"History: {args.history_dir}")
    print(f"Current: {args.current_dir}")
    print(f"Regression threshold: {args.regression_threshold:.2f}x")
    for scale in scales:
        print_scale_report(
            scale,
            history_by_scale.get(scale, []),
            current_by_scale.get(scale, []),
            args.regression_threshold,
        )


if __name__ == "__main__":
    main()
