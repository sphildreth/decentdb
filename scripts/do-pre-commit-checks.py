#!/usr/bin/env python3
"""Run Steven's pre-commit paranoia checks with visible progress."""

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

try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table

    HAVE_RICH = True
except ImportError:
    HAVE_RICH = False


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
    benchmark_summary: BenchmarkSummary | None = None


@dataclass
class BenchmarkSectionSummary:
    label: str
    status: str
    gained: list[str] = field(default_factory=list)
    lost: list[str] = field(default_factory=list)
    moved_to_sqlite: list[str] = field(default_factory=list)


@dataclass
class BenchmarkSummary:
    check_key: str
    overall_status: str
    sections: list[BenchmarkSectionSummary] = field(default_factory=list)


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


def overall_benchmark_status(sections: Sequence[BenchmarkSectionSummary]) -> str:
    statuses = {section.status for section in sections}
    if not statuses:
        return "SAME"
    if "LOSE" in statuses:
        return "LOSE"
    if "WIN" in statuses:
        return "WIN"
    if statuses == {"BASELINE"}:
        return "BASELINE"
    if "BASELINE" in statuses and len(statuses) > 1:
        return "WIN"
    return "SAME"


def normalize_comparison_label(raw_label: str) -> str:
    raw_label = raw_label.strip()
    if raw_label.startswith("Comparison (") and raw_label.endswith(")"):
        return raw_label[len("Comparison (") : -1]
    return raw_label


def parse_metric_name(line: str) -> str | None:
    item = line[2:].strip()
    if not item or item.lower() == "none":
        return None
    return item.split(":", 1)[0].strip()


def parse_benchmark_comparisons(text: str) -> dict[str, dict[str, list[str]]]:
    sections: dict[str, dict[str, list[str]]] = {}
    current_label: str | None = None
    current_side: str | None = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        header_match = COMPARISON_HEADER_RE.match(line)
        if header_match:
            normalized_label = normalize_comparison_label(header_match.group(1))
            if "DecentDB vs SQLite" in normalized_label:
                current_label = normalized_label
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

        metric_name = parse_metric_name(line)
        if metric_name is not None:
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


def compare_benchmark_sections(
    check_key: str,
    current: dict[str, dict[str, list[str]]],
    baselines: dict[str, dict[str, dict[str, list[str]]]],
) -> BenchmarkSummary | None:
    if not current:
        return None

    previous = baselines.get(check_key, {})
    section_summaries: list[BenchmarkSectionSummary] = []
    for label in sorted(current):
        current_decent = set(current[label]["decentdb"])
        current_sqlite = set(current[label]["sqlite"])
        previous_decent = set(previous.get(label, {}).get("decentdb", []))

        if label not in previous:
            section_summaries.append(
                BenchmarkSectionSummary(label=label, status="BASELINE")
            )
            continue

        gained = sorted(current_decent - previous_decent)
        lost = sorted(previous_decent - current_decent)
        moved_to_sqlite = sorted(set(lost) & current_sqlite)

        if lost:
            status = "LOSE"
        elif gained:
            status = "WIN"
        else:
            status = "SAME"

        section_summaries.append(
            BenchmarkSectionSummary(
                label=label,
                status=status,
                gained=gained,
                lost=lost,
                moved_to_sqlite=moved_to_sqlite,
            )
        )

    overall_status = overall_benchmark_status(section_summaries)
    return BenchmarkSummary(
        check_key=check_key,
        overall_status=overall_status,
        sections=section_summaries,
    )


def render_benchmark_summary(summary: BenchmarkSummary) -> Panel:
    table = Table(expand=True)
    table.add_column("Section", style="bold")
    table.add_column("Result", justify="center")
    table.add_column("New DecentDB wins", overflow="fold")
    table.add_column("Lost DecentDB wins", overflow="fold")

    styles = {
        "WIN": "[bold green]WIN[/bold green]",
        "SAME": "[bold cyan]SAME[/bold cyan]",
        "LOSE": "[bold red]LOSE[/bold red]",
        "BASELINE": "[bold yellow]BASELINE[/bold yellow]",
    }
    for section in summary.sections:
        lost_detail = ", ".join(section.lost) if section.lost else "-"
        if section.moved_to_sqlite:
            lost_detail = f"{lost_detail} [dim](to SQLite: {', '.join(section.moved_to_sqlite)})[/dim]"
        table.add_row(
            section.label,
            styles.get(section.status, section.status),
            ", ".join(section.gained) if section.gained else "-",
            lost_detail,
        )

    border_style = {
        "WIN": "green",
        "SAME": "cyan",
        "LOSE": "red",
        "BASELINE": "yellow",
    }.get(summary.overall_status, "cyan")
    return Panel(
        table,
        title=f"{summary.check_key} benchmark drift: {summary.overall_status}",
        border_style=border_style,
    )


def build_checks() -> list[Check]:
    native_lib = REPO_ROOT / "target" / "release" / "libdecentdb.so"
    python_env = {
        "DECENTDB_NATIVE_LIB": str(native_lib),
        "PYTHONPATH": ".",
    }
    python_exec = shlex.quote(sys.executable)

    return [
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
        Check(
            key="rust-core",
            title="Rust release build + core tests",
            cwd=REPO_ROOT,
            command="cargo build --release && cargo test -p decentdb",
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


def render_dashboard(
    states: Sequence[CheckState], started_at: float, log_root: Path
) -> Group:
    completed = sum(1 for state in states if state.status in {"passed", "failed"})
    total = len(states)
    running = next((state for state in states if state.status == "running"), None)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        expand=True,
    )
    description = (
        f"[bold cyan]Running:[/bold cyan] {running.check.title}"
        if running is not None
        else "[bold cyan]Waiting for checks[/bold cyan]"
    )
    progress.add_task(description, total=total, completed=completed)

    table = Table(expand=True)
    table.add_column("#", justify="right", style="bold")
    table.add_column("Check", overflow="fold")
    table.add_column("Status", justify="center")
    table.add_column("Elapsed", justify="right")
    table.add_column("Workdir", overflow="fold", style="dim")

    status_styles = {
        "pending": "[dim]pending[/dim]",
        "running": "[bold yellow]RUNNING[/bold yellow]",
        "passed": "[bold green]PASS[/bold green]",
        "failed": "[bold red]FAIL[/bold red]",
    }
    for index, state in enumerate(states, start=1):
        table.add_row(
            str(index),
            f"{state.check.title}\n[dim]{state.check.key}[/dim]",
            status_styles[state.status],
            format_duration(state.elapsed_seconds),
            str(state.check.cwd.relative_to(REPO_ROOT)),
        )

    footer = Panel(
        f"[bold]Logs:[/bold] {log_root}\n"
        f"[bold]Total elapsed:[/bold] {format_duration(time.perf_counter() - started_at)}",
        title="Pre-commit paranoia suite",
        border_style="cyan",
    )
    return Group(progress, table, footer)


def print_plain_status(
    console: Console, states: Sequence[CheckState], started_at: float, log_root: Path
) -> None:
    console.print("=" * 80)
    console.print(
        f"Pre-commit paranoia suite | total elapsed {format_duration(time.perf_counter() - started_at)}"
    )
    for state in states:
        console.print(
            f"{state.check.key:18} {state.status.upper():8} "
            f"{format_duration(state.elapsed_seconds):>8}  {state.check.title}"
        )
    console.print(f"Logs: {log_root}")


def run_check(
    console: Console,
    live: Live | None,
    states: list[CheckState],
    state: CheckState,
    show_output: bool,
    suite_started_at: float,
    log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    state.log_path = log_root / f"{state.check.key}.log"
    state.status = "running"
    start = time.perf_counter()

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

        while True:
            return_code = process.poll()
            state.elapsed_seconds = time.perf_counter() - start
            if live is not None:
                live.update(render_dashboard(states, suite_started_at, log_root))
            if return_code is not None:
                state.return_code = return_code
                break
            time.sleep(0.15)

    state.elapsed_seconds = time.perf_counter() - start
    state.status = "passed" if state.return_code == 0 else "failed"
    if live is not None:
        live.update(render_dashboard(states, suite_started_at, log_root))
    if state.return_code == 0 and state.check.benchmark_comparison:
        log_text = state.log_path.read_text(encoding="utf-8", errors="replace")
        current_sections = parse_benchmark_comparisons(log_text)
        state.benchmark_summary = compare_benchmark_sections(
            state.check.key, current_sections, benchmark_baselines
        )
        if current_sections:
            benchmark_baselines[state.check.key] = current_sections
            save_benchmark_baselines(benchmark_baselines)
        if state.benchmark_summary is not None:
            console.print(render_benchmark_summary(state.benchmark_summary))

    if state.return_code != 0 or show_output:
        output = tail_lines(state.log_path, max_lines=120)
        title = f"{state.check.key} output"
        style = "red" if state.return_code else "green"
        console.print(
            Panel(output or "(no output captured)", title=title, border_style=style)
        )

    state.elapsed_seconds = time.perf_counter() - start
    if live is not None:
        live.update(render_dashboard(states, suite_started_at, log_root))


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
    parser = argparse.ArgumentParser(
        description="Run Steven's pre-commit paranoia checks."
    )
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
        "--show-output",
        action="store_true",
        help="Print the captured output for successful checks too.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately after the first failing check.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not HAVE_RICH:
        print(
            "This script requires the 'rich' package in the active Python environment.",
            file=sys.stderr,
        )
        return 2

    all_checks = build_checks()
    if args.list:
        for check in all_checks:
            modes = ",".join(check.modes)
            suffix = " [benchmark-drift]" if check.benchmark_comparison else ""
            print(f"{check.key:24} {modes:14} {check.title}{suffix}")
        return 0

    checks = select_checks(all_checks, args.only, args.mode)
    run_log_root = LOG_ROOT / time.strftime("%Y%m%d-%H%M%S")
    run_log_root.mkdir(parents=True, exist_ok=True)
    console = Console()
    states = [CheckState(check=check) for check in checks]
    started_at = time.perf_counter()
    benchmark_baselines = load_benchmark_baselines()

    use_live = HAVE_RICH and console.is_terminal

    if use_live:
        live_cm = Live(
            render_dashboard(states, started_at, run_log_root),
            console=console,
            refresh_per_second=6,
        )
    else:
        live_cm = None

    failures: list[CheckState] = []
    try:
        if live_cm is not None:
            with live_cm as live:
                for state in states:
                    run_check(
                        console,
                        live,
                        states,
                        state,
                        args.show_output,
                        started_at,
                        run_log_root,
                        benchmark_baselines,
                    )
                    if state.status == "failed":
                        failures.append(state)
                        if args.fail_fast:
                            break
        else:
            for state in states:
                print_plain_status(console, states, started_at, run_log_root)
                run_check(
                    console,
                    None,
                    states,
                    state,
                    args.show_output,
                    started_at,
                    run_log_root,
                    benchmark_baselines,
                )
                if state.status == "failed":
                    failures.append(state)
                    if args.fail_fast:
                        break
            print_plain_status(console, states, started_at, run_log_root)
    except KeyboardInterrupt:
        console.print("\n[bold red]Interrupted.[/bold red]")
        return 130

    total_elapsed = format_duration(time.perf_counter() - started_at)
    if failures:
        summary = Table(expand=True)
        summary.add_column("Failed check", style="bold red")
        summary.add_column("Exit", justify="right")
        summary.add_column("Log", overflow="fold")
        for failure in failures:
            summary.add_row(
                failure.check.key,
                str(failure.return_code),
                str(failure.log_path) if failure.log_path is not None else "",
            )
        console.print(
            Panel(summary, title=f"FAIL after {total_elapsed}", border_style="red")
        )
        return 1

    summary = Table(expand=True)
    summary.add_column("Check", style="bold green")
    summary.add_column("Elapsed", justify="right")
    summary.add_column("Benchmark drift", justify="center")
    for state in states:
        drift = (
            state.benchmark_summary.overall_status
            if state.benchmark_summary is not None
            else "-"
        )
        summary.add_row(state.check.key, format_duration(state.elapsed_seconds), drift)
    console.print(
        Panel(summary, title=f"PASS in {total_elapsed}", border_style="green")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
