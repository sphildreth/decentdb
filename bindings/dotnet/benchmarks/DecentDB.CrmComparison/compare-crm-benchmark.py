#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


EXPECTED_SCENARIOS = (
    "01. Bulk Insert Companies",
    "02. Bulk Insert Users",
    "03. Bulk Insert Addresses",
    "04. Bulk Insert Invoices",
    "05. Bulk Insert Invoice Items",
    "06. Point Reads (PK lookup)",
    "07a. Raw Joined Aggregate",
    "07b. Build Revenue Summary",
    "07c. Read Revenue Summary",
    "08. Substring Search (LIKE %pattern%)",
    "09. Update Invoices Paid",
    "10. Complex Window/Analytic Query",
    "11. View Query (Unpaid Invoices)",
    "12. Delete Cascade Test",
)


@dataclass(frozen=True)
class SummaryRow:
    mean_ms: float
    mean_allocated_bytes: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare two DecentDB CRM benchmark JSON outputs"
    )
    parser.add_argument(
        "--baseline",
        default="",
        help="Optional baseline CRM benchmark JSON",
    )
    parser.add_argument(
        "--current",
        required=True,
        help="Current CRM benchmark JSON",
    )
    parser.add_argument(
        "--max-regression",
        type=float,
        default=0.10,
        help="Max allowed regression ratio on MeanMs (default: 0.10 = 10%%)",
    )
    parser.add_argument(
        "--max-allocation-regression",
        type=float,
        default=None,
        help=(
            "Optional max allowed regression ratio on MeanAllocatedBytes "
            "(for example: 0.10 = 10%%). Disabled when omitted."
        ),
    )
    parser.add_argument(
        "--max-mean-ms",
        type=float,
        default=None,
        help="Optional hard upper bound for scenario mean in current results",
    )
    parser.add_argument(
        "--expected-scenarios",
        default=",".join(EXPECTED_SCENARIOS),
        help="Comma-separated expected scenarios (default: canonical CRM suite)",
    )
    parser.add_argument(
        "--expected-engines",
        default="DecentDB,SQLite",
        help="Comma-separated expected engines for scenario completeness checks",
    )
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="Require expected scenario/engine pairs in baseline/current rows",
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


def parse_csv(value: str) -> Tuple[str, ...]:
    items = tuple(item.strip() for item in value.split(",") if item.strip())
    if not items:
        raise argparse.ArgumentTypeError("expected non-empty comma-separated value")
    return items


def load_summaries(path: Path) -> Dict[Tuple[str, str], SummaryRow]:
    payload = json.loads(path.read_text())
    rows = (
        payload.get("Summary")
        or payload.get("Summaries")
        or payload.get("summary")
        or payload.get("summaries")
        or []
    )
    if not isinstance(rows, list):
        raise ValueError(f"{path}: summary field is not a list")

    output: Dict[Tuple[str, str], SummaryRow] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(f"{path}: summary row is not an object")
        scenario = row.get("Scenario")
        engine = row.get("Engine")
        mean_ms = row.get("MeanMs")
        mean_allocated_bytes = row.get("MeanAllocatedBytes")
        if scenario is None or engine is None or mean_ms is None:
            raise ValueError(
                f"{path}: summary row is missing Scenario/Engine/MeanMs"
            )
        key = (str(scenario), str(engine))
        if key in output:
            raise ValueError(f"{path}: duplicate summary row for {scenario} / {engine}")
        mean = float(mean_ms)
        if mean <= 0:
            raise ValueError(f"{path}: {scenario} / {engine} has non-positive mean duration")
        allocated = None
        if mean_allocated_bytes is not None:
            allocated = float(mean_allocated_bytes)
            if allocated < 0:
                raise ValueError(
                    f"{path}: {scenario} / {engine} has negative mean allocated bytes"
                )
        output[key] = SummaryRow(mean_ms=mean, mean_allocated_bytes=allocated)
    return output


def expected_pairs(
    scenarios: Sequence[str],
    engines: Sequence[str],
) -> Iterable[Tuple[str, str]]:
    for scenario in scenarios:
        for engine in engines:
            yield (scenario, engine)


def build_regression_report(
    baseline: str | None,
    current: str,
    max_regression: float,
    max_allocation_regression: float | None,
    require_complete: bool,
    expected_scenarios: Sequence[str],
    expected_engines: Sequence[str],
    failures: Sequence[Tuple[Tuple[str, str], float, float, float]],
    allocation_failures: Sequence[Tuple[Tuple[str, str], float, float, float | None]],
    missing_baseline: Sequence[Tuple[str, str]],
    missing_current: Sequence[Tuple[str, str]],
    max_mean_failures: Sequence[Tuple[Tuple[str, str], float, float]],
    decentdb_not_leading: Sequence[Tuple[str, float, float, float]],
) -> str:
    return json.dumps(
        {
            "baseline": baseline,
            "current": current,
            "max_regression": max_regression,
            "max_allocation_regression": max_allocation_regression,
            "require_complete": require_complete,
            "required_scenarios": list(expected_scenarios),
            "required_engines": list(expected_engines),
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
            "allocation_regressions": [
                {
                    "scenario": k[0],
                    "engine": k[1],
                    "baseline_mean_allocated_bytes": baseline_allocated,
                    "current_mean_allocated_bytes": current_allocated,
                    "ratio": ratio,
                }
                for k, baseline_allocated, current_allocated, ratio in allocation_failures
            ],
            "missing_in_baseline": [
                {"scenario": scenario, "engine": engine}
                for scenario, engine in sorted(missing_baseline)
            ],
            "missing_in_current": [
                {"scenario": scenario, "engine": engine}
                for scenario, engine in sorted(missing_current)
            ],
            "max_mean_regressions": [
                {
                    "scenario": k[0],
                    "engine": k[1],
                    "current_mean_ms": current_mean,
                    "max_mean_ms": max_mean_ms,
                }
                for k, current_mean, max_mean_ms in max_mean_failures
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
        },
        indent=2,
    )


def main() -> int:
    args = parse_args()
    current = load_summaries(Path(args.current))
    baseline = load_summaries(Path(args.baseline)) if args.baseline else None

    expected_scenarios = parse_csv(args.expected_scenarios)
    expected_engines = parse_csv(args.expected_engines)

    threshold = 1.0 + args.max_regression
    failures: List[Tuple[Tuple[str, str], float, float, float]] = []
    allocation_failures: List[Tuple[Tuple[str, str], float, float, float | None]] = []
    max_mean_failures: List[Tuple[Tuple[str, str], float, float]] = []
    missing_baseline: List[Tuple[str, str]] = []
    missing_current: List[Tuple[str, str]] = []

    if args.require_complete and baseline is not None:
        for pair in expected_pairs(expected_scenarios, expected_engines):
            if pair not in baseline:
                missing_baseline.append(pair)
    if args.require_complete:
        for pair in expected_pairs(expected_scenarios, expected_engines):
            if pair not in current:
                missing_current.append(pair)

    if args.max_mean_ms is not None:
        for pair, summary in current.items():
            if summary.mean_ms > args.max_mean_ms:
                max_mean_failures.append((pair, summary.mean_ms, args.max_mean_ms))

    if baseline is not None:
        for pair, baseline_summary in baseline.items():
            if args.require_complete and pair in missing_baseline:
                continue
            if pair not in current:
                if pair not in missing_current:
                    missing_current.append(pair)
                continue

            baseline_mean = baseline_summary.mean_ms
            current_mean = current[pair].mean_ms
            if baseline_mean <= 0:
                continue
            ratio = current_mean / baseline_mean
            if ratio > threshold:
                failures.append((pair, baseline_mean, current_mean, ratio))

    if baseline is not None and args.max_allocation_regression is not None:
        allocation_threshold = 1.0 + args.max_allocation_regression
        for pair, baseline_summary in baseline.items():
            if args.require_complete and pair in missing_baseline:
                continue
            if pair not in current:
                continue

            baseline_allocated = baseline_summary.mean_allocated_bytes
            current_allocated = current[pair].mean_allocated_bytes
            if baseline_allocated is None or current_allocated is None:
                continue
            if baseline_allocated == 0:
                if current_allocated > 0:
                    allocation_failures.append(
                        (pair, baseline_allocated, current_allocated, None)
                    )
                continue

            ratio = current_allocated / baseline_allocated
            if ratio > allocation_threshold:
                allocation_failures.append(
                    (pair, baseline_allocated, current_allocated, ratio)
                )

    decentdb_not_leading: List[Tuple[str, float, float, float]] = []
    if args.check_decentdb_win:
        for (scenario, engine), summary in current.items():
            if engine != "DecentDB":
                continue
            sqlite_key = (scenario, "SQLite")
            if sqlite_key not in current:
                continue
            decent_mean = summary.mean_ms
            sqlite_mean = current[sqlite_key].mean_ms
            if sqlite_mean <= 0 or decent_mean <= 0:
                continue
            ratio = sqlite_mean / decent_mean
            if ratio < 1.10:
                decentdb_not_leading.append((scenario, decent_mean, sqlite_mean, ratio))

    if baseline is None:
        if args.require_complete:
            print("Current summary coverage:")
            if missing_current:
                print("Missing scenario/engine pairs in current:")
                for scenario, engine in sorted(missing_current):
                    print(f" - {scenario} / {engine}")
            else:
                print(f" - all {len(expected_scenarios)} scenarios present for {', '.join(expected_engines)}")
        else:
            print("Current summary present:")
            print(f" - {len(current)} scenario/engine rows")
    else:
        if missing_baseline:
            print("Missing scenarios in baseline:")
            for scenario, engine in sorted(missing_baseline):
                print(f" - {scenario} / {engine}")
        if missing_current:
            print("Missing scenarios in current:")
            for scenario, engine in sorted(missing_current):
                print(f" - {scenario} / {engine}")

    if failures:
        print("Regression detected:")
        for (scenario, engine), baseline_mean, current_mean, ratio in sorted(failures):
            pct = (ratio - 1.0) * 100
            print(
                f" - {scenario} / {engine}: baseline {baseline_mean:.3f}ms -> "
                f"current {current_mean:.3f}ms (+{pct:.1f}%)"
            )

    if allocation_failures:
        print("Allocation regression detected:")
        for (scenario, engine), baseline_allocated, current_allocated, ratio in sorted(
            allocation_failures
        ):
            if ratio is None:
                print(
                    f" - {scenario} / {engine}: baseline {baseline_allocated:.0f}B -> "
                    f"current {current_allocated:.0f}B"
                )
            else:
                pct = (ratio - 1.0) * 100
                print(
                    f" - {scenario} / {engine}: baseline {baseline_allocated:.0f}B -> "
                    f"current {current_allocated:.0f}B (+{pct:.1f}%)"
                )

    if max_mean_failures:
        print("Max-mean limits exceeded:")
        for (scenario, engine), current_mean, max_mean_ms in sorted(max_mean_failures):
            print(f" - {scenario} / {engine}: {current_mean:.3f}ms > {max_mean_ms:.3f}ms")

    if decentdb_not_leading:
        print("DecentDB/SQLite lead target missed:")
        for scenario, decent_mean, sqlite_mean, ratio in sorted(decentdb_not_leading):
            print(
                f" - {scenario}: DecentDB={decent_mean:.3f}ms, SQLite={sqlite_mean:.3f}ms, "
                f"SQLite/DecentDB={ratio:.2f}x"
            )

    if args.output:
        Path(args.output).write_text(
            build_regression_report(
                args.baseline or None,
                args.current,
                args.max_regression,
                args.max_allocation_regression,
                args.require_complete,
                expected_scenarios,
                expected_engines,
                failures,
                allocation_failures,
                missing_baseline,
                missing_current,
                max_mean_failures,
                decentdb_not_leading,
            )
        )

    failed = bool(
        failures
        or allocation_failures
        or missing_current
        or missing_baseline
        or max_mean_failures
    )
    if args.check_decentdb_win:
        failed = failed or bool(decentdb_not_leading)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
