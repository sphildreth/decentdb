#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two DecentDB CRM benchmark JSON outputs")
    parser.add_argument("--baseline", required=True, help="Baseline CRM benchmark JSON")
    parser.add_argument("--current", required=True, help="Current CRM benchmark JSON")
    parser.add_argument(
        "--max-regression",
        type=float,
        default=0.10,
        help="Max allowed regression ratio on MeanMs (default: 0.10 = 10%%)",
    )
    parser.add_argument(
        "--check-decentdb-win",
        action="store_true",
        help="Fail if any scenario has DecentDB slower than SQLite by margin",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional JSON report path",
    )
    return parser.parse_args()


def load_summaries(path: Path) -> Dict[Tuple[str, str], float]:
    payload = json.loads(path.read_text())
    rows = (
        payload.get("summary")
        or payload.get("summaries")
        or []
    )
    return {(row["Scenario"], row["Engine"]): float(row["MeanMs"]) for row in rows}


def main() -> int:
    args = parse_args()
    baseline = load_summaries(Path(args.baseline))
    current = load_summaries(Path(args.current))

    threshold = 1.0 + args.max_regression
    failures = []
    missing = []

    for key, baseline_mean in baseline.items():
        if key not in current:
            missing.append(key)
            continue

        current_mean = current[key]
        if baseline_mean <= 0:
            continue

        ratio = current_mean / baseline_mean
        if ratio > threshold:
            failures.append((key, baseline_mean, current_mean, ratio))

    decentdb_not_leading = []
    if args.check_decentdb_win:
        for (scenario, engine), decent_mean in current.items():
            if engine != "DecentDB":
                continue

            sqlite_key = (scenario, "SQLite")
            if sqlite_key not in current:
                continue

            sqlite_mean = current[sqlite_key]
            if sqlite_mean <= 0 or decent_mean <= 0:
                continue

            ratio = sqlite_mean / decent_mean
            if ratio < 1.10:
                decentdb_not_leading.append((scenario, decent_mean, sqlite_mean, ratio))

    if missing:
        print("Missing scenarios in current:")
        for scenario, engine in sorted(missing):
            print(f" - {scenario} / {engine}")

    if failures:
        print("Regression detected:")
        for (scenario, engine), baseline_mean, current_mean, ratio in sorted(failures):
            pct = (ratio - 1.0) * 100
            print(
                f" - {scenario} / {engine}: baseline {baseline_mean:.3f}ms -> "
                f"current {current_mean:.3f}ms (+{pct:.1f}%)"
            )

    if decentdb_not_leading:
        print("DecentDB/SQLite lead target missed:")
        for scenario, decent_mean, sqlite_mean, ratio in sorted(decentdb_not_leading):
            print(
                f" - {scenario}: DecentDB={decent_mean:.3f}ms, SQLite={sqlite_mean:.3f}ms, "
                f"SQLite/DecentDB={ratio:.2f}x"
            )

    report = {
        "baseline": args.baseline,
        "current": args.current,
        "max_regression": args.max_regression,
        "regressions": [
            {
                "scenario": k[0],
                "engine": k[1],
                "baseline_mean_ms": baseline_mean,
                "current_mean_ms": current_mean,
                "ratio": ratio,
            }
            for k, baseline_mean, current_mean, ratio in failures
        ],
        "decentdb_not_leading": [
            {
                "scenario": scenario,
                "decent_ms": decent_mean,
                "sqlite_ms": sqlite_mean,
                "sqlite_over_decent": ratio,
            }
            for scenario, decent_mean, sqlite_mean, ratio in decentdb_not_leading
        ],
        "missing_in_current": [{"scenario": scenario, "engine": engine} for scenario, engine in sorted(missing)],
    }

    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2))

    failed = bool(failures or missing)
    if args.check_decentdb_win:
        failed = failed or bool(decentdb_not_leading)

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
