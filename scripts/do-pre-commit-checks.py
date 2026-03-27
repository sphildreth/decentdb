#!/usr/bin/env python3
"""Run pre-commit checks with simple, readable output."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_ROOT = REPO_ROOT / ".tmp" / "pre-commit-checks"
BENCHMARK_BASELINE_PATH = LOG_ROOT / "benchmark-comparison-baselines.json"
COMPARISON_HEADER_RE = re.compile(r"^===\s*(.+?)\s*===\s*$")


@dataclass(frozen=True)
class Check:
    key: str
    title: str
    cwd: Path
    command: str
    env: dict[str, str]
    modes: tuple[str, ...] = ("paranoid",)
    benchmark_comparison: bool = False


@dataclass
class CheckState:
    check: Check
    status: str = "pending"
    elapsed_seconds: float = 0.0
    return_code: int | None = None
    log_path: Path | None = None
    benchmark_drift: str | None = None


# ANSI colors
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
DIM = "\033[2m"
RESET = "\033[0m"
BOLD = "\033[1m"


def format_duration(seconds: float) -> str:
    total = int(seconds)
    minutes, secs = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def tail_lines(path: Path, max_lines: int = 80) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-max_lines:])


def extract_comparison_block(log_text: str) -> str | None:
    lines = log_text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.strip().startswith("=== Comparison"):
            start = i
            break
    if start is None:
        return None
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if lines[i].strip().startswith("===") and "Comparison" not in lines[i]:
            end = i
            break
    return "\n".join(lines[start:end])


def parse_benchmark_comparisons(text: str) -> dict[str, dict[str, list[str]]]:
    sections: dict[str, dict[str, list[str]]] = {}
    current_label: str | None = None
    current_side: str | None = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        header_match = COMPARISON_HEADER_RE.match(line)
        if header_match:
            label = header_match.group(1).strip()
            if label.startswith("Comparison (") and label.endswith(")"):
                label = label[len("Comparison (") : -1]
            if "DecentDB vs SQLite" in label:
                current_label = label
                current_side = None
                sections.setdefault(current_label, {"decentdb": [], "sqlite": []})
            else:
                current_label = None
                current_side = None
            continue

        if current_label is None:
            continue
        if line == "DecentDB better at:":
            current_side = "decentdb"
            continue
        if line == "SQLite better at:":
            current_side = "sqlite"
            continue
        if not line.startswith("- ") or current_side is None:
            continue

        item = line[2:].strip()
        if item and item.lower() != "none":
            metric_name = item.split(":", 1)[0].strip()
            sections[current_label][current_side].append(metric_name)

    return sections


def load_benchmark_baselines() -> dict[str, dict[str, dict[str, list[str]]]]:
    if not BENCHMARK_BASELINE_PATH.exists():
        return {}
    try:
        return json.loads(BENCHMARK_BASELINE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_benchmark_baselines(
    baselines: dict[str, dict[str, dict[str, list[str]]]],
) -> None:
    BENCHMARK_BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    BENCHMARK_BASELINE_PATH.write_text(
        json.dumps(baselines, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def compute_benchmark_drift(
    current: dict[str, dict[str, list[str]]],
    baselines: dict[str, dict[str, dict[str, list[str]]]],
    check_key: str,
) -> str | None:
    if not current:
        return None

    previous = baselines.get(check_key, {})
    results = []
    for label in sorted(current):
        current_decent = set(current[label]["decentdb"])
        previous_decent = set(previous.get(label, {}).get("decentdb", []))

        if label not in previous:
            results.append("baseline")
            continue

        gained = current_decent - previous_decent
        lost = previous_decent - current_decent

        if lost:
            results.append("regressed")
        elif gained:
            results.append("improved")
        else:
            results.append("same")

    if "regressed" in results:
        return "REGRESSED"
    if "improved" in results:
        return "IMPROVED"
    if all(r == "baseline" for r in results):
        return "BASELINE"
    return "STABLE"


def build_checks() -> list[Check]:
    native_lib = REPO_ROOT / "target" / "release" / "libdecentdb.so"
    python_env = {
        "DECENTDB_NATIVE_LIB": str(native_lib),
        "PYTHONPATH": ".",
    }
    python_exec = shlex.quote(sys.executable)

    return [
        # ── Stage 1: Static analysis (no build artifacts needed) ──
        Check(
            key="rust-format",
            title="Rust fmt check",
            cwd=REPO_ROOT,
            command="cargo fmt --check",
            env={},
            modes=("fast", "paranoid"),
        ),
        Check(
            key="rust-clippy",
            title="Rust clippy",
            cwd=REPO_ROOT,
            command="cargo clippy -p decentdb --all-targets --all-features -- -D warnings",
            env={},
            modes=("fast", "paranoid"),
        ),
        # ── Stage 2: Build the release native lib (needed by all binding checks) ──
        Check(
            key="rust-release-build",
            title="Rust release build",
            cwd=REPO_ROOT,
            command="cargo build --release -p decentdb",
            env={},
            modes=("fast", "paranoid"),
        ),
        # ── Stage 3: Fast regression tests (Rust + binding smoke) ──
        Check(
            key="rust-regressions",
            title="Rust regression tests",
            cwd=REPO_ROOT,
            command=(
                "cargo test -p decentdb --test sql_dml_tests --quiet && "
                "cargo test -p decentdb --test sql_expressions_tests --quiet"
            ),
            env={},
            modes=("fast", "paranoid"),
        ),
        Check(
            key="python-fastdecode-smoke",
            title="Python fastdecode smoke",
            cwd=REPO_ROOT / "bindings" / "python",
            command=f"{python_exec} -m pytest -q tests/test_basic.py -k 'fastdecode_imports_with_native_lib_env'",
            env=python_env,
            modes=("fast", "paranoid"),
        ),
        Check(
            key="python-regressions",
            title="Python regression smoke",
            cwd=REPO_ROOT / "bindings" / "python",
            command=(
                f"{python_exec} -m pytest -q tests/test_comprehensive.py "
                "-k 'cursor_rowcount_after_operations or cursor_execute_two_float_range_scan'"
            ),
            env=python_env,
            modes=("fast", "paranoid"),
        ),
        # ── Stage 4: Full test suites (paranoid only) ──
        Check(
            key="rust-core",
            title="Rust core tests",
            cwd=REPO_ROOT,
            command="cargo test -p decentdb",
            env={},
            modes=("paranoid",),
        ),
        Check(
            key="dotnet-stack",
            title=".NET clean/build/test",
            cwd=REPO_ROOT / "bindings" / "dotnet",
            command="dotnet clean && dotnet build && dotnet test",
            env={},
            modes=("paranoid",),
        ),
        Check(
            key="python-smoke",
            title="Python broader smoke tests",
            cwd=REPO_ROOT / "bindings" / "python",
            command=f"{python_exec} -m pytest -q tests/test_basic.py tests/test_comprehensive.py",
            env=python_env,
            modes=("paranoid",),
        ),
        # ── Stage 5: Benchmarks (paranoid only) ──
        Check(
            key="python-benchmark",
            title="Python complex benchmark",
            cwd=REPO_ROOT / "bindings" / "python",
            command=(
                f"{python_exec} benchmarks/bench_complex.py "
                "--users 1000 --items 50 --orders 100"
            ),
            env=python_env,
            modes=("paranoid",),
            benchmark_comparison=True,
        ),
        Check(
            key="dotnet-benchmark",
            title=".NET benchmark harness",
            cwd=REPO_ROOT
            / "bindings"
            / "dotnet"
            / "benchmarks"
            / "DecentDB.Benchmarks",
            command=(
                "dotnet clean && "
                "dotnet run -c Release --project DecentDB.Benchmarks.csproj -- "
                "--count 50000 --point-reads 2000 --fetchmany-batch 1000"
            ),
            env={},
            modes=("paranoid",),
            benchmark_comparison=True,
        ),
    ]


def run_check(
    state: CheckState,
    index: int,
    total: int,
    log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    state.log_path = log_root / f"{state.check.key}.log"
    state.status = "running"
    start = time.perf_counter()

    label = f"[{index}/{total}] {state.check.title}"
    print(f"{CYAN}RUNNING{RESET} {label}")

    env = os.environ.copy()
    env.update(state.check.env)

    with state.log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"$ (cd {state.check.cwd} && {state.check.command})\n\n")
        log_file.flush()
        process = subprocess.Popen(
            ["bash", "-lc", state.check.command],
            cwd=state.check.cwd,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        process.wait()

    state.elapsed_seconds = time.perf_counter() - start
    state.return_code = process.returncode
    state.status = "passed" if state.return_code == 0 else "failed"

    duration = format_duration(state.elapsed_seconds)
    if state.status == "passed":
        print(f"{GREEN}PASS{RESET}   {label} {DIM}({duration}){RESET}")
    else:
        print(f"{RED}FAIL{RESET}   {label} {DIM}({duration}){RESET}")

    if state.return_code == 0 and state.check.benchmark_comparison:
        log_text = state.log_path.read_text(encoding="utf-8", errors="replace")
        comparison = extract_comparison_block(log_text)
        if comparison:
            print()
            print(comparison)
            print()
        current_sections = parse_benchmark_comparisons(log_text)
        state.benchmark_drift = compute_benchmark_drift(
            current_sections, benchmark_baselines, state.check.key
        )
        if current_sections:
            benchmark_baselines[state.check.key] = current_sections
            save_benchmark_baselines(benchmark_baselines)

    if state.return_code != 0:
        output = tail_lines(state.log_path, max_lines=40)
        if output:
            print(f"\n{RED}--- {state.check.key} failed output (tail) ---{RESET}")
            print(output)
            print()


def select_checks(
    all_checks: Sequence[Check], selected_keys: Sequence[str], mode: str
) -> list[Check]:
    if not selected_keys:
        return [check for check in all_checks if mode in check.modes]

    selected_set = set(selected_keys)
    unknown = sorted(selected_set - {check.key for check in all_checks})
    if unknown:
        raise SystemExit(f"Unknown check key(s): {', '.join(unknown)}")
    return [check for check in all_checks if check.key in selected_set]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run pre-commit checks.")
    parser.add_argument(
        "--mode",
        choices=("fast", "paranoid"),
        default="paranoid",
        help="Choose a faster smoke pass or the full paranoia suite.",
    )
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        metavar="CHECK",
        help="Run only the named check key (repeatable).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available check keys and exit.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately after the first failing check.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    all_checks = build_checks()

    if args.list:
        for check in all_checks:
            modes = ",".join(check.modes)
            suffix = " [benchmark]" if check.benchmark_comparison else ""
            print(f"{check.key:24} {modes:14} {check.title}{suffix}")
        return 0

    checks = select_checks(all_checks, args.only, args.mode)
    run_log_root = LOG_ROOT / time.strftime("%Y%m%d-%H%M%S")
    states = [CheckState(check=check) for check in checks]
    started_at = time.perf_counter()
    benchmark_baselines = load_benchmark_baselines()

    print(
        f"\n{BOLD}Pre-commit checks ({len(checks)} checks, {args.mode} mode){RESET}\n"
    )

    failures: list[CheckState] = []
    try:
        for index, state in enumerate(states, start=1):
            run_check(state, index, len(checks), run_log_root, benchmark_baselines)
            if state.status == "failed":
                failures.append(state)
                if args.fail_fast:
                    break
    except KeyboardInterrupt:
        print(f"\n{RED}Interrupted.{RESET}")
        return 130

    total_elapsed = format_duration(time.perf_counter() - started_at)

    print(f"\n{'─' * 60}")
    if failures:
        print(
            f"{RED}{BOLD}FAILED{RESET} in {total_elapsed}  ({len(failures)}/{len(checks)} failed)"
        )
        for f in failures:
            log_hint = f" → {f.log_path}" if f.log_path else ""
            print(f"  {RED}✗{RESET} {f.check.key}{log_hint}")
        print(f"\nLogs: {run_log_root}")
        return 1

    print(f"{GREEN}{BOLD}PASSED{RESET} in {total_elapsed}")
    for state in states:
        drift = ""
        if state.benchmark_drift:
            color = {
                "REGRESSED": RED,
                "IMPROVED": GREEN,
                "STABLE": "",
                "BASELINE": YELLOW,
            }.get(state.benchmark_drift, "")
            drift = f"  [{color}{state.benchmark_drift}{RESET}]"
        print(
            f"  {GREEN}✓{RESET} {state.check.key} {format_duration(state.elapsed_seconds)}{drift}"
        )
    print(f"\nLogs: {run_log_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
