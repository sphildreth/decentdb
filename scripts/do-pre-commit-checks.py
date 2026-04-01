#!/usr/bin/env python3
"""Run pre-commit checks with simple, readable output.

Checks are organized into stages.  Within each stage, independent checks
run in parallel using a thread pool.  Stages execute sequentially so that
build artifacts are available before test stages begin.

Checks that invoke ``cargo`` are serialized within their stage via a
shared lock (cargo holds an exclusive file-lock on the target directory,
so true parallelism would just queue them anyway).

Binding checks that require a specific toolchain (Go, Java, Node, Dart)
are skipped gracefully when the toolchain is not installed.

When the ``rich`` library is available the output uses a live-updating
table with spinners and a progress bar.  Otherwise it falls back to
plain ANSI text.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table
    from rich.text import Text

    HAS_RICH = True
except ImportError:
    HAS_RICH = False


REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_ROOT = REPO_ROOT / ".tmp" / "pre-commit-checks"
BENCHMARK_BASELINE_PATH = LOG_ROOT / "benchmark-comparison-baselines.json"
COMPARISON_HEADER_RE = re.compile(r"^===\s*(.+?)\s*===\s*$")

# Serializes cargo invocations (they share a target-directory file lock).
_cargo_lock = threading.Lock()
# Serializes terminal output so parallel checks don't interleave lines.
_print_lock = threading.Lock()


@dataclass(frozen=True)
class Check:
    key: str
    title: str
    cwd: Path
    command: str
    env: dict[str, str]
    stage: int = 0
    modes: tuple[str, ...] = ("paranoid",)
    benchmark_comparison: bool = False
    cargo_bound: bool = False
    requires: str | None = None


@dataclass
class CheckState:
    check: Check
    status: str = "pending"
    elapsed_seconds: float = 0.0
    return_code: int | None = None
    log_path: Path | None = None
    benchmark_drift: str | None = None


# ANSI colors (fallback renderer only)
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
    release_dir = REPO_ROOT / "target" / "release"
    include_dir = REPO_ROOT / "include"
    python_env = {
        "DECENTDB_NATIVE_LIB": str(native_lib),
        "PYTHONPATH": ".",
    }
    python_exec = shlex.quote(sys.executable)

    return [
        # ── Stage 0: Static analysis (parallel, fast) ──
        Check(
            key="rust-format",
            title="Rust fmt check",
            cwd=REPO_ROOT,
            command="cargo fmt --check",
            env={},
            stage=0,
            modes=("fast", "paranoid"),
        ),
        Check(
            key="rust-clippy",
            title="Rust clippy (workspace)",
            cwd=REPO_ROOT,
            command="cargo clippy --workspace --all-targets --all-features -- -D warnings",
            env={},
            stage=0,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        # ── Stage 1: Clean + rebuild (guarantees no stale artifacts) ──
        Check(
            key="rust-clean",
            title="Rust clean (both profiles)",
            cwd=REPO_ROOT,
            command="cargo clean --release -p decentdb && cargo clean -p decentdb",
            env={},
            stage=1,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        Check(
            key="rust-release-build",
            title="Rust release build",
            cwd=REPO_ROOT,
            command="cargo build --release -p decentdb -p decentdb-cli",
            env={},
            stage=2,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        # ── Stage 3: Fast regression tests (parallel) ──
        Check(
            key="rust-regressions",
            title="Rust regression tests",
            cwd=REPO_ROOT,
            command=(
                "cargo test -p decentdb --test sql_dml_tests --quiet && "
                "cargo test -p decentdb --test sql_expressions_tests --quiet"
            ),
            env={},
            stage=3,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        Check(
            key="python-fastdecode-smoke",
            title="Python fastdecode smoke",
            cwd=REPO_ROOT / "bindings" / "python",
            command=f"{python_exec} -m pytest -q tests/test_basic.py -k 'fastdecode_imports_with_native_lib_env'",
            env=python_env,
            stage=3,
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
            stage=3,
            modes=("fast", "paranoid"),
        ),
        # ── Stage 4: Full test suites (parallel, cargo-bound ones serialized) ──
        Check(
            key="rust-core",
            title="Rust core tests",
            cwd=REPO_ROOT,
            command="cargo test -p decentdb",
            env={},
            stage=4,
            cargo_bound=True,
        ),
        Check(
            key="rust-cli",
            title="Rust CLI tests",
            cwd=REPO_ROOT,
            command="cargo test -p decentdb-cli",
            env={},
            stage=4,
            cargo_bound=True,
        ),
        Check(
            key="rust-migrate",
            title="Rust migrate tests",
            cwd=REPO_ROOT,
            command="cargo test -p decentdb-migrate",
            env={},
            stage=4,
            cargo_bound=True,
        ),
        Check(
            key="python-smoke",
            title="Python full test suite",
            cwd=REPO_ROOT / "bindings" / "python",
            command=f"{python_exec} -m pytest -q tests/",
            env=python_env,
            stage=4,
        ),
        Check(
            key="python-ffi",
            title="Python FFI smoke",
            cwd=REPO_ROOT,
            command=f"{python_exec} tests/bindings/python/test_ffi.py",
            env={"DECENTDB_NATIVE_LIB": str(native_lib)},
            stage=4,
        ),
        Check(
            key="dotnet-stack",
            title=".NET clean/build/test",
            cwd=REPO_ROOT / "bindings" / "dotnet",
            command="dotnet clean -c Release && dotnet build -c Release && dotnet test -c Release",
            env={},
            stage=4,
        ),
        Check(
            key="go-tests",
            title="Go binding tests",
            cwd=REPO_ROOT / "bindings" / "go" / "decentdb-go",
            command="go test -count=1 ./...",
            env={},
            stage=4,
            requires="go",
        ),
        Check(
            key="java-tests",
            title="Java binding tests",
            cwd=REPO_ROOT / "bindings" / "java",
            command=f"./gradlew test -PnativeLibDir={release_dir}",
            env={},
            stage=4,
            requires="java",
        ),
        Check(
            key="node-tests",
            title="Node.js binding tests",
            cwd=REPO_ROOT / "bindings" / "node" / "decentdb",
            command="npm test",
            env={"DECENTDB_NATIVE_LIB_PATH": str(native_lib)},
            stage=4,
            requires="node",
        ),
        Check(
            key="c-smoke",
            title="C binding smoke test",
            cwd=REPO_ROOT,
            command=(
                f"cc -I{include_dir} tests/bindings/c/smoke.c "
                f"-L{release_dir} -Wl,-rpath,{release_dir} -ldecentdb "
                f"-o target/bindings-c-smoke && "
                f"target/bindings-c-smoke"
            ),
            env={},
            stage=4,
            requires="cc",
        ),
        Check(
            key="dart-tests",
            title="Dart binding tests",
            cwd=REPO_ROOT / "bindings" / "dart" / "dart",
            command="dart test",
            env={"DECENTDB_NATIVE_LIB": str(native_lib)},
            stage=4,
            requires="dart",
        ),
        # ── Stage 5: Benchmarks (parallel) ──
        Check(
            key="python-benchmark",
            title="Python complex benchmark",
            cwd=REPO_ROOT / "bindings" / "python",
            command=(
                f"{python_exec} benchmarks/bench_complex.py "
                "--users 1000 --items 50 --orders 100"
            ),
            env=python_env,
            stage=5,
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
            stage=5,
            benchmark_comparison=True,
        ),
    ]


def run_check(
    state: CheckState,
    log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    state.log_path = log_root / f"{state.check.key}.log"
    state.status = "running"
    start = time.perf_counter()

    env = os.environ.copy()
    env.update(state.check.env)

    lock = _cargo_lock if state.check.cargo_bound else contextlib.nullcontext()
    with lock:
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

    if state.return_code == 0 and state.check.benchmark_comparison:
        log_text = state.log_path.read_text(encoding="utf-8", errors="replace")
        current_sections = parse_benchmark_comparisons(log_text)
        state.benchmark_drift = compute_benchmark_drift(
            current_sections, benchmark_baselines, state.check.key
        )
        if current_sections:
            benchmark_baselines[state.check.key] = current_sections
            save_benchmark_baselines(benchmark_baselines)


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
        help="Stop immediately after the first failing stage.",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Disable parallel execution (run all checks sequentially).",
    )
    return parser.parse_args()


def _run_stage(
    stage_states: list[tuple[int, CheckState]],
    log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
    sequential: bool,
) -> list[CheckState]:
    """Run all checks in a stage and return any that failed."""
    failures: list[CheckState] = []

    if sequential or len(stage_states) == 1:
        for _idx, state in stage_states:
            run_check(state, log_root, benchmark_baselines)
            if state.status == "failed":
                failures.append(state)
    else:
        with ThreadPoolExecutor(max_workers=len(stage_states)) as pool:
            futures = {}
            for _idx, state in stage_states:
                fut = pool.submit(
                    run_check, state, log_root, benchmark_baselines,
                )
                futures[fut] = state
            for fut in as_completed(futures):
                fut.result()
                state = futures[fut]
                if state.status == "failed":
                    failures.append(state)

    return failures


def _check_skip_reason(check: Check) -> str | None:
    """Return a human-readable skip reason, or None if the check can run."""
    if not check.requires:
        return None
    if not shutil.which(check.requires):
        return f"{check.requires} not found"
    # Gradle 8.x doesn't support JDK 25+; detect and skip gracefully.
    if check.key == "java-tests":
        try:
            ver_out = subprocess.check_output(
                ["java", "-version"], stderr=subprocess.STDOUT, text=True,
            )
            match = re.search(r'"(\d+)', ver_out)
            if match and int(match.group(1)) >= 25:
                props = check.cwd / "gradle" / "wrapper" / "gradle-wrapper.properties"
                if props.exists():
                    text = props.read_text()
                    gv_match = re.search(r"gradle-(\d+)\.", text)
                    if gv_match and int(gv_match.group(1)) < 9:
                        return (
                            f"Gradle {gv_match.group(1)}.x incompatible with JDK {match.group(1)} "
                            "(needs Gradle 9+)"
                        )
        except (subprocess.CalledProcessError, OSError, ValueError):
            pass
    return None


def main() -> int:
    args = parse_args()
    all_checks = build_checks()

    if args.list:
        if HAS_RICH:
            return _list_checks_rich(all_checks)
        return _list_checks_plain(all_checks)

    checks = select_checks(all_checks, args.only, args.mode)

    # Detect missing toolchains and partition into active vs skipped.
    active_checks: list[Check] = []
    skipped: list[tuple[Check, str]] = []
    for check in checks:
        skip_reason = _check_skip_reason(check)
        if skip_reason:
            skipped.append((check, skip_reason))
        else:
            active_checks.append(check)

    run_log_root = LOG_ROOT / time.strftime("%Y%m%d-%H%M%S")
    states = [CheckState(check=check) for check in active_checks]
    started_at = time.perf_counter()
    benchmark_baselines = load_benchmark_baselines()

    mode_label = args.mode
    if args.sequential:
        mode_label += ", sequential"

    # Group states by stage.
    stages: dict[int, list[tuple[int, CheckState]]] = {}
    for idx, state in enumerate(states, start=1):
        stages.setdefault(state.check.stage, []).append((idx, state))

    if HAS_RICH:
        return _run_rich(
            states, stages, skipped, run_log_root, benchmark_baselines,
            mode_label, args.sequential, started_at, args.fail_fast,
        )
    return _run_plain(
        states, stages, skipped, run_log_root, benchmark_baselines,
        mode_label, args.sequential, started_at, args.fail_fast,
    )


# ─── Rich renderer ──────────────────────────────────────────────────────────


_STATUS_STYLE = {
    "pending": ("dim", "⋯"),
    "running": ("cyan bold", "⟳"),
    "passed": ("green", "✓"),
    "failed": ("red bold", "✗"),
    "skipped": ("yellow", "○"),
}


def _build_live_table(
    states: list[CheckState],
    skipped: list[tuple[Check, str]],
) -> Table:
    table = Table(
        show_header=True,
        header_style="bold",
        expand=True,
        border_style="dim",
        pad_edge=False,
    )
    table.add_column("", width=3, justify="center", no_wrap=True)
    table.add_column("Check", ratio=3, no_wrap=True)
    table.add_column("Stage", width=5, justify="center")
    table.add_column("Status", width=10, justify="center")
    table.add_column("Time", width=8, justify="right")
    table.add_column("Info", ratio=2)

    for state in states:
        style, icon = _STATUS_STYLE.get(state.status, ("", "?"))
        status_text = Text(state.status.upper(), style=style)
        icon_text = Text(icon, style=style)
        elapsed = format_duration(state.elapsed_seconds) if state.status in ("passed", "failed") else ""
        info = ""
        if state.benchmark_drift:
            drift_style = {
                "REGRESSED": "red bold",
                "IMPROVED": "green",
                "STABLE": "dim",
                "BASELINE": "yellow",
            }.get(state.benchmark_drift, "")
            info = f"[{drift_style}]{state.benchmark_drift}[/]"
        table.add_row(icon_text, state.check.title, str(state.check.stage), status_text, elapsed, info)

    for chk, reason in skipped:
        table.add_row(
            Text("○", style="yellow"),
            Text(chk.title, style="dim"),
            str(chk.stage),
            Text("SKIP", style="yellow"),
            "",
            Text(reason, style="dim italic"),
        )

    return table


def _run_rich(
    states: list[CheckState],
    stages: dict[int, list[tuple[int, CheckState]]],
    skipped: list[tuple[Check, str]],
    run_log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
    mode_label: str,
    sequential: bool,
    started_at: float,
    fail_fast: bool,
) -> int:
    console = Console()
    total = len(states)
    completed = 0

    progress = Progress(
        SpinnerColumn("dots"),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )
    task_id = progress.add_task(
        f"Pre-commit ({mode_label})", total=total,
    )

    def make_display() -> Group:
        return Group(
            _build_live_table(states, skipped),
            progress,
        )

    failures: list[CheckState] = []
    try:
        with Live(make_display(), console=console, refresh_per_second=8, transient=True) as live:
            for stage_num in sorted(stages):
                stage_list = stages[stage_num]

                if sequential or len(stage_list) == 1:
                    for _idx, state in stage_list:
                        run_check(state, run_log_root, benchmark_baselines)
                        completed += 1
                        progress.update(task_id, completed=completed)
                        live.update(make_display())
                        if state.status == "failed":
                            failures.append(state)
                else:
                    with ThreadPoolExecutor(max_workers=len(stage_list)) as pool:
                        futures = {}
                        for _idx, state in stage_list:
                            fut = pool.submit(run_check, state, run_log_root, benchmark_baselines)
                            futures[fut] = state

                        for fut in as_completed(futures):
                            fut.result()
                            st = futures[fut]
                            completed += 1
                            progress.update(task_id, completed=completed)
                            live.update(make_display())
                            if st.status == "failed":
                                failures.append(st)

                if failures and fail_fast:
                    break

    except KeyboardInterrupt:
        console.print("\n[red bold]Interrupted.[/]")
        return 130

    total_elapsed = format_duration(time.perf_counter() - started_at)
    console.print()

    # Final summary table
    final_table = _build_live_table(states, skipped)
    console.print(final_table)
    console.print()

    if failures:
        console.print(
            Panel(
                f"[red bold]FAILED[/] in {total_elapsed}  "
                f"({len(failures)}/{total} failed)",
                border_style="red",
                expand=False,
            )
        )
        for f in failures:
            output = tail_lines(f.log_path, max_lines=40) if f.log_path else ""
            if output:
                console.print(
                    Panel(
                        output,
                        title=f"[red]{f.check.key}[/] failure log",
                        border_style="red",
                        expand=True,
                    )
                )
        console.print(f"[dim]Logs: {run_log_root}[/]")
        return 1

    console.print(
        Panel(
            f"[green bold]PASSED[/] in {total_elapsed}  "
            f"({total} passed"
            + (f", {len(skipped)} skipped" if skipped else "")
            + ")",
            border_style="green",
            expand=False,
        )
    )
    console.print(f"[dim]Logs: {run_log_root}[/]")
    return 0


def _list_checks_rich(all_checks: list[Check]) -> int:
    console = Console()
    table = Table(title="Available Checks", show_lines=False, border_style="dim")
    table.add_column("Key", style="cyan", no_wrap=True)
    table.add_column("Stage", justify="center")
    table.add_column("Modes")
    table.add_column("Title")
    table.add_column("Flags", style="dim")

    for check in all_checks:
        modes = ",".join(check.modes)
        flags = []
        if check.cargo_bound:
            flags.append("cargo")
        if check.requires:
            skip = _check_skip_reason(check)
            if skip:
                flags.append(f"[yellow]skip: {skip}[/]")
            else:
                flags.append(f"requires:{check.requires}")
        if check.benchmark_comparison:
            flags.append("benchmark")
        table.add_row(check.key, str(check.stage), modes, check.title, ", ".join(flags))

    console.print(table)
    return 0


# ─── Plain ANSI fallback renderer ───────────────────────────────────────────


def _run_plain(
    states: list[CheckState],
    stages: dict[int, list[tuple[int, CheckState]]],
    skipped: list[tuple[Check, str]],
    run_log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
    mode_label: str,
    sequential: bool,
    started_at: float,
    fail_fast: bool,
) -> int:
    total = len(states)
    print(
        f"\n{BOLD}Pre-commit checks ({total} checks, {mode_label}){RESET}"
    )
    if skipped:
        for chk, reason in skipped:
            print(f"  {YELLOW}SKIP{RESET} {chk.title} ({reason})")
    print()

    failures: list[CheckState] = []
    try:
        for stage_num in sorted(stages):
            stage_failures = _run_stage(
                stages[stage_num], run_log_root,
                benchmark_baselines, sequential,
            )
            failures.extend(stage_failures)
            if failures and fail_fast:
                break
    except KeyboardInterrupt:
        print(f"\n{RED}Interrupted.{RESET}")
        return 130

    # Print status lines for each state.
    for state in states:
        duration = format_duration(state.elapsed_seconds)
        label = state.check.title
        if state.status == "passed":
            print(f"{GREEN}PASS{RESET}   {label} {DIM}({duration}){RESET}")
        elif state.status == "failed":
            print(f"{RED}FAIL{RESET}   {label} {DIM}({duration}){RESET}")

    total_elapsed = format_duration(time.perf_counter() - started_at)

    print(f"\n{'─' * 60}")
    if failures:
        print(
            f"{RED}{BOLD}FAILED{RESET} in {total_elapsed}  ({len(failures)}/{total} failed)"
        )
        for f in failures:
            log_hint = f" → {f.log_path}" if f.log_path else ""
            print(f"  {RED}✗{RESET} {f.check.key}{log_hint}")
            output = tail_lines(f.log_path, max_lines=40) if f.log_path else ""
            if output:
                print(f"\n{RED}--- {f.check.key} failed output (tail) ---{RESET}")
                print(output)
                print()
        if skipped:
            for chk, reason in skipped:
                print(f"  {YELLOW}○{RESET} {chk.key} (skipped, {reason})")
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
    if skipped:
        for chk, reason in skipped:
            print(f"  {YELLOW}○{RESET} {chk.key} (skipped, {reason})")
    print(f"\nLogs: {run_log_root}")
    return 0


def _list_checks_plain(all_checks: list[Check]) -> int:
    prev_stage = -1
    for check in all_checks:
        if check.stage != prev_stage:
            if prev_stage >= 0:
                print()
            print(f"  {BOLD}Stage {check.stage}{RESET}")
            prev_stage = check.stage
        modes = ",".join(check.modes)
        flags = []
        if check.cargo_bound:
            flags.append("cargo")
        if check.requires:
            flags.append(f"requires:{check.requires}")
        if check.benchmark_comparison:
            flags.append("benchmark")
        suffix = f"  [{', '.join(flags)}]" if flags else ""
        print(f"    {check.key:24} {modes:14} {check.title}{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
