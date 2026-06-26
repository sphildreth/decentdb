#!/usr/bin/env python3
"""Run DecentDB vs SQLite validation benchmarks with a Rich summary.

The runner is intentionally opinionated around the benchmark loop used for the
current DecentDB performance work:

- build the DecentDB release native library
- rebuild the Python fast-decode extension when it is missing or stale
- run reduced Showdown repetitions plus broader validation workloads
- parse the benchmark's "DecentDB better at" and "SQLite better at" sections
- show a concise pass/gap summary and keep raw logs under .tmp/
"""

from __future__ import annotations

import argparse
import dataclasses
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import sysconfig
import time
from typing import Iterable

try:
    from rich import box
    from rich.console import Console
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
except ImportError as exc:  # pragma: no cover - exercised only on missing deps.
    print(
        "This helper requires the Python 'rich' package. "
        "Install the Python binding dependencies, then rerun.",
        file=sys.stderr,
    )
    raise SystemExit(2) from exc


REPO_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = REPO_ROOT / "bindings/python/benchmarks/bench_complex.py"
FASTDECODE_C = REPO_ROOT / "bindings/python/decentdb/_fastdecode.c"
DECENTDB_HEADER = REPO_ROOT / "include/decentdb.h"


@dataclasses.dataclass
class CommandResult:
    label: str
    command: list[str]
    log_path: Path | None
    returncode: int
    duration_s: float
    skipped_reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclasses.dataclass
class Comparison:
    name: str
    decentdb_better: list[str] = dataclasses.field(default_factory=list)
    sqlite_better: list[str] = dataclasses.field(default_factory=list)
    ties: list[str] = dataclasses.field(default_factory=list)
    skipped: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class BenchmarkResult:
    label: str
    command_result: CommandResult
    comparisons: list[Comparison]

    @property
    def sqlite_win_count(self) -> int:
        return sum(len(comp.sqlite_better) for comp in self.comparisons)

    @property
    def decentdb_win_count(self) -> int:
        return sum(len(comp.decentdb_better) for comp in self.comparisons)

    @property
    def tie_count(self) -> int:
        return sum(len(comp.ties) for comp in self.comparisons)

    @property
    def skipped_count(self) -> int:
        return sum(len(comp.skipped) for comp in self.comparisons)


@dataclasses.dataclass(frozen=True)
class SqliteGap:
    run: str
    section: str
    detail: str
    category: str
    ratio: float
    material: bool


@dataclasses.dataclass(frozen=True)
class BenchmarkSpec:
    label: str
    args: tuple[str, ...]
    notes: str = ""


def shlex_join(command: Iterable[str]) -> str:
    return subprocess.list2cmdline(list(command))


def rel(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def default_output_dir() -> Path:
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return REPO_ROOT / ".tmp/perf-validate" / stamp


def python_env() -> dict[str, str]:
    env = os.environ.copy()
    binding_path = str(REPO_ROOT / "bindings/python")
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        binding_path if not existing else binding_path + os.pathsep + existing
    )

    lib_path = resolve_release_native_library()
    if lib_path is not None:
        env["DECENTDB_NATIVE_LIB"] = str(lib_path)
    return env


def resolve_release_native_library() -> Path | None:
    if platform.system() == "Darwin":
        names = ["libdecentdb.dylib", "libc_api.dylib"]
    elif platform.system() == "Windows":
        names = ["decentdb.dll", "c_api.dll"]
    else:
        names = ["libdecentdb.so", "libc_api.so"]
    for name in names:
        candidate = REPO_ROOT / "target/release" / name
        if candidate.exists():
            return candidate
    return None


def run_command(
    *,
    console: Console,
    label: str,
    command: list[str],
    log_path: Path,
    env: dict[str, str] | None = None,
    echo: bool = False,
) -> CommandResult:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"$ {shlex_join(command)}\n\n")
        log_file.flush()

        with console.status(f"[bold cyan]{label}[/]") as status:
            process = subprocess.Popen(
                command,
                cwd=REPO_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="replace",
                bufsize=1,
            )
            assert process.stdout is not None
            for line in process.stdout:
                log_file.write(line)
                if echo:
                    console.print(line.rstrip())
                else:
                    stripped = line.strip()
                    if stripped:
                        status.update(f"[bold cyan]{label}[/] {stripped[:120]}")
            returncode = process.wait()

    duration_s = time.perf_counter() - started
    return CommandResult(
        label=label,
        command=command,
        log_path=log_path,
        returncode=returncode,
        duration_s=duration_s,
    )


def skipped_result(label: str, command: list[str], reason: str) -> CommandResult:
    return CommandResult(
        label=label,
        command=command,
        log_path=None,
        returncode=0,
        duration_s=0.0,
        skipped_reason=reason,
    )


def fastdecode_output_path() -> Path:
    suffix = sysconfig.get_config_var("EXT_SUFFIX") or ".so"
    return FASTDECODE_C.with_name("_fastdecode" + suffix)


def fastdecode_needs_rebuild(output_path: Path) -> bool:
    if not FASTDECODE_C.exists():
        return False
    if not output_path.exists():
        return True
    output_mtime = output_path.stat().st_mtime
    inputs = [FASTDECODE_C]
    if DECENTDB_HEADER.exists():
        inputs.append(DECENTDB_HEADER)
    return any(path.stat().st_mtime > output_mtime for path in inputs)


def fastdecode_compile_command(cc: str, output_path: Path) -> list[str]:
    include = sysconfig.get_path("include")
    platinclude = sysconfig.get_path("platinclude")
    command = [cc, "-O3", "-shared", "-fPIC"]
    if include:
        command.append(f"-I{include}")
    if platinclude and platinclude != include:
        command.append(f"-I{platinclude}")
    command.extend(
        [
            f"-I{REPO_ROOT / 'include'}",
            str(FASTDECODE_C),
            "-o",
            str(output_path),
        ]
    )
    return command


def maybe_rebuild_fastdecode(
    *,
    console: Console,
    output_dir: Path,
    mode: str,
    cc: str,
) -> CommandResult:
    output_path = fastdecode_output_path()
    if mode == "skip":
        return skipped_result(
            "fastdecode extension",
            [],
            "skipped by --fastdecode=skip",
        )
    if not FASTDECODE_C.exists():
        return skipped_result(
            "fastdecode extension",
            [],
            f"{rel(FASTDECODE_C)} does not exist",
        )
    if mode == "auto" and not fastdecode_needs_rebuild(output_path):
        return skipped_result(
            "fastdecode extension",
            [str(output_path)],
            "up to date",
        )
    if shutil.which(cc) is None:
        return CommandResult(
            label="fastdecode extension",
            command=[cc],
            log_path=None,
            returncode=127,
            duration_s=0.0,
            skipped_reason=f"C compiler '{cc}' was not found",
        )

    command = fastdecode_compile_command(cc, output_path)
    return run_command(
        console=console,
        label="fastdecode extension",
        command=command,
        log_path=output_dir / "preflight_fastdecode.log",
        env=python_env(),
    )


COMPARISON_RE = re.compile(
    r"^===\s+(?:(?P<name>.*?)\s+)?Comparison \(DecentDB vs SQLite\)\s+===$"
)


def parse_comparisons(log_path: Path) -> list[Comparison]:
    comparisons: list[Comparison] = []
    current: Comparison | None = None
    section: str | None = None

    for raw_line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        match = COMPARISON_RE.match(line)
        if match:
            name = (match.group("name") or "Complex").strip()
            current = Comparison(name=name)
            comparisons.append(current)
            section = None
            continue

        if current is None:
            continue
        if line == "DecentDB better at:":
            section = "decentdb_better"
            continue
        if line == "SQLite better at:":
            section = "sqlite_better"
            continue
        if line == "Ties:":
            section = "ties"
            continue
        if line == "Skipped/unsupported:":
            section = "skipped"
            continue
        if line.startswith("==="):
            section = None
            continue

        if section and line.startswith("- "):
            item = line[2:].strip()
            if item and item != "none":
                getattr(current, section).append(item)

    return comparisons


def reduced_showdown_args(prefix: Path) -> tuple[str, ...]:
    return (
        "--workload",
        "showdown",
        "--showdown-movies",
        "700",
        "--showdown-people-mult",
        "1",
        "--showdown-reviews-per-movie",
        "2",
        "--showdown-point-reads",
        "100",
        "--db-prefix",
        str(prefix),
    )


def build_benchmark_specs(args: argparse.Namespace, output_dir: Path) -> list[BenchmarkSpec]:
    specs: list[BenchmarkSpec] = []

    for index in range(1, args.reduced_runs + 1):
        specs.append(
            BenchmarkSpec(
                label=f"Reduced Showdown #{index}",
                args=reduced_showdown_args(output_dir / f"reduced_{index}"),
                notes="Fast regression loop used during optimization.",
            )
        )

    if args.profile in ("standard", "full"):
        specs.append(
            BenchmarkSpec(
                label="Showdown smoke",
                args=(
                    "--workload",
                    "showdown",
                    "--db-prefix",
                    str(output_dir / "showdown_smoke"),
                ),
                notes="Default Showdown smoke workload.",
            )
        )

    if args.profile == "full":
        specs.extend(
            [
                BenchmarkSpec(
                    label="Showdown GLM52 scale",
                    args=(
                        "--workload",
                        "showdown",
                        "--showdown-scale",
                        "glm52",
                        "--db-prefix",
                        str(output_dir / "showdown_glm52"),
                    ),
                    notes="Large Showdown workload matching the second .NET harness.",
                ),
                BenchmarkSpec(
                    label="MovieDB scratch scale",
                    args=(
                        "--workload",
                        "movie",
                        "--movie-scale",
                        "scratch",
                        "--db-prefix",
                        str(output_dir / "movie_scratch"),
                    ),
                    notes="Large MovieDB workload matching the first .NET harness.",
                ),
            ]
        )

    return specs


def benchmark_command(
    spec: BenchmarkSpec,
    args: argparse.Namespace,
) -> list[str]:
    command = [sys.executable, str(BENCHMARK), *spec.args]
    if args.keep_db:
        command.append("--keep-db")
    if args.sqlite_profile:
        command.extend(["--sqlite-profile", args.sqlite_profile])
    if args.sqlite_cache_mb is not None:
        command.extend(["--sqlite-cache-mb", str(args.sqlite_cache_mb)])
    if args.decentdb_options is not None:
        command.extend(["--decentdb-options", args.decentdb_options])
    return command


def render_plan(console: Console, args: argparse.Namespace, output_dir: Path) -> None:
    profile_text = Text()
    profile_text.append("Profile: ", style="bold")
    profile_text.append(args.profile)
    profile_text.append("\nOutput: ", style="bold")
    profile_text.append(rel(output_dir))
    profile_text.append("\nStrict: ", style="bold")
    profile_text.append("fail on SQLite wins" if args.strict else "report only")
    profile_text.append("\nDatabases: ", style="bold")
    profile_text.append("kept" if args.keep_db else "cleaned by benchmark")
    console.print(
        Panel(
            profile_text,
            title="DecentDB Benchmark Runner",
            border_style="cyan",
            box=box.ROUNDED,
        )
    )


def render_preflight(console: Console, results: list[CommandResult]) -> None:
    table = Table(title="Preflight", box=box.SIMPLE_HEAVY)
    table.add_column("Step", style="bold")
    table.add_column("Status")
    table.add_column("Time", justify="right")
    table.add_column("Log / Note")

    for result in results:
        if result.skipped_reason:
            status = "[yellow]skipped[/]"
            note = result.skipped_reason
        elif result.ok:
            status = "[green]ok[/]"
            note = rel(result.log_path)
        else:
            status = "[red]failed[/]"
            note = rel(result.log_path) or result.skipped_reason or ""
        table.add_row(result.label, status, f"{result.duration_s:.1f}s", note)
    console.print(table)


def render_benchmark_summary(console: Console, results: list[BenchmarkResult]) -> None:
    table = Table(title="Benchmark Summary", box=box.SIMPLE_HEAVY)
    table.add_column("Benchmark", style="bold")
    table.add_column("Status")
    table.add_column("DDB Better", justify="right")
    table.add_column("SQLite Better", justify="right")
    table.add_column("Ties", justify="right")
    table.add_column("Skipped", justify="right")
    table.add_column("Time", justify="right")
    table.add_column("Log")

    for result in results:
        if not result.command_result.ok:
            status = "[red]failed[/]"
        elif result.sqlite_win_count:
            status = "[red]gaps[/]"
        elif result.skipped_count:
            status = "[yellow]ok with skips[/]"
        else:
            status = "[green]ok[/]"
        table.add_row(
            result.label,
            status,
            str(result.decentdb_win_count),
            str(result.sqlite_win_count),
            str(result.tie_count),
            str(result.skipped_count),
            f"{result.command_result.duration_s:.1f}s",
            rel(result.command_result.log_path),
        )
    console.print(table)


def render_detail_table(
    console: Console,
    title: str,
    style: str,
    results: list[BenchmarkResult],
    attr: str,
    max_rows: int,
) -> None:
    rows: list[tuple[str, str, str]] = []
    for result in results:
        for comparison in result.comparisons:
            for item in getattr(comparison, attr):
                rows.append((result.label, comparison.name, item))

    if not rows:
        console.print(Panel("none", title=title, border_style=style, box=box.ROUNDED))
        return

    table = Table(title=title, box=box.SIMPLE_HEAVY, border_style=style)
    table.add_column("Run", style="bold", no_wrap=True)
    table.add_column("Section", no_wrap=True)
    table.add_column("Metric")
    for run, section, metric in rows[:max_rows]:
        table.add_row(run, section, metric)
    if len(rows) > max_rows:
        table.caption = f"Showing {max_rows} of {len(rows)} rows. Increase --max-details for more."
    console.print(table)


GAP_DETAIL_RE = re.compile(
    r"^(?P<name>.*?): "
    r"(?P<winner>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)(?P<unit>[^0-9()]+?) "
    r"vs "
    r"(?P<loser>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)(?P=unit) "
    r"\((?P<ratio>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)x "
    r"(?P<direction>higher|faster/lower)\)$"
)


def _metric_category(name: str) -> str:
    lower = name.lower()
    if "equivalence" in lower or "mismatch" in lower:
        return "equivalence_mismatch/other"
    if "final file size" in lower or "file size" in lower:
        return "file_size"
    if "checkpoint" in lower:
        return "checkpoint"
    if "analyze" in lower:
        return "analyze_stats"
    if "bulk load" in lower or "insert throughput" in lower:
        return "bulk_load"
    if "search" in lower or "fulltext" in lower or "substring like" in lower:
        return "search"
    if "index build" in lower:
        return "index_build"
    if "point lookup" in lower or "point read" in lower:
        return "point_read"
    if any(
        token in lower
        for token in (
            "insert ",
            " update ",
            "upsert",
            " delete ",
            " returning",
            "bulk update",
            "bulk delete",
        )
    ):
        return "dml"
    if any(
        token in lower
        for token in (
            "join",
            "aggregate",
            "scan",
            "range",
            "pagination",
            "cte",
            "union",
            "ranking",
            "rolling avg",
            "yearly counts",
            "top by",
            "top-rated",
            "busiest",
            "watchlist query",
            "window",
        )
    ):
        return "query_join_aggregate"
    return "equivalence_mismatch/other"


def _is_time_unit(unit: str) -> bool:
    return unit.strip() in {"s", "ms", "us", "ns"}


def _unit_to_seconds(unit: str, value: float) -> float:
    normalized = unit.strip()
    if normalized == "s":
        return value
    if normalized == "ms":
        return value / 1000.0
    if normalized == "us":
        return value / 1_000_000.0
    if normalized == "ns":
        return value / 1_000_000_000.0
    return value


def _parse_sqlite_gap(detail: str) -> tuple[str, str, float, float, float] | None:
    match = GAP_DETAIL_RE.match(detail)
    if not match:
        return None
    name = match.group("name")
    unit = match.group("unit")
    winner = float(match.group("winner"))
    loser = float(match.group("loser"))
    ratio = float(match.group("ratio"))
    return name, unit, winner, loser, ratio


def _collect_sqlite_gaps(results: list[BenchmarkResult]) -> list[SqliteGap]:
    gaps: list[SqliteGap] = []
    for result in results:
        for comparison in result.comparisons:
            for detail in comparison.sqlite_better:
                parsed = _parse_sqlite_gap(detail)
                if parsed is None:
                    continue
                name, unit, winner, loser, ratio = parsed
                material = ratio >= 1.25
                if material and _is_time_unit(unit):
                    winner_seconds = _unit_to_seconds(unit, winner)
                    loser_seconds = _unit_to_seconds(unit, loser)
                    material = abs(winner_seconds - loser_seconds) >= 0.00025
                gaps.append(
                    SqliteGap(
                        run=result.label,
                        section=comparison.name,
                        detail=detail,
                        category=_metric_category(name),
                        ratio=ratio,
                        material=material,
                    )
                )
    return gaps


def _group_sqlite_gaps(gaps: list[SqliteGap]) -> tuple[dict[str, list[SqliteGap]], list[str]]:
    grouped: dict[str, list[SqliteGap]] = {}
    for gap in gaps:
        grouped.setdefault(gap.category, []).append(gap)

    order = [
        "bulk_load",
        "index_build",
        "analyze_stats",
        "checkpoint",
        "point_read",
        "query_join_aggregate",
        "dml",
        "search",
        "file_size",
        "equivalence_mismatch/other",
    ]

    def sort_key(category: str) -> tuple[int, int, str]:
        items = grouped[category]
        return (-sum(1 for gap in items if gap.material), -len(items), category)

    categories = [category for category in order if category in grouped]
    categories.extend(sorted(set(grouped) - set(categories), key=sort_key))
    return grouped, categories


def render_sqlite_gap_groups(console: Console, results: list[BenchmarkResult]) -> None:
    gaps = _collect_sqlite_gaps(results)
    if not gaps:
        return

    grouped, categories = _group_sqlite_gaps(gaps)

    summary = Table(
        title="SQLite Win Groups",
        box=box.SIMPLE_HEAVY,
    )
    summary.add_column("Category", style="bold")
    summary.add_column("Wins", justify="right")
    summary.add_column("Material", justify="right")
    summary.add_column("Material gaps")

    for category in categories:
        items = grouped[category]
        material_items = [gap for gap in items if gap.material]
        examples = ", ".join(
            f"{gap.detail.split(': ', 1)[0]} ({gap.ratio:.2f}x)"
            for gap in material_items[:2]
        )
        summary.add_row(
            category,
            str(len(items)),
            str(len(material_items)),
            examples or "-",
        )

    console.print(
        Panel(
            "Material gaps use a 1.25x ratio threshold; second-based timings also require an absolute delta of at least 0.00025s.",
            title="SQLite Gap Policy",
            border_style="red",
            box=box.ROUNDED,
        )
    )
    console.print(summary)


def _self_check_sqlite_gap_helpers() -> None:
    parsed = _parse_sqlite_gap(
        "Bulk load: 0.50s vs 0.40s (1.25x faster/lower)"
    )
    expected = ("Bulk load", "s", 0.50, 0.40, 1.25)
    if parsed != expected:
        raise AssertionError(f"unexpected parse result: {parsed!r}")

    result = BenchmarkResult(
        label="Smoke Run",
        command_result=CommandResult(
            label="Smoke Run",
            command=["benchmark"],
            log_path=None,
            returncode=0,
            duration_s=0.0,
        ),
        comparisons=[
            Comparison(
                name="Primary",
                sqlite_better=[
                    "Bulk load: 0.50s vs 0.40s (1.25x faster/lower)",
                    "Point read: 0.000100s vs 0.000090s (1.25x faster/lower)",
                    "Movie genres 3-table join: 0.010s vs 0.008s (1.25x faster/lower)",
                    "Final file size: 1200 bytes vs 800 bytes (1.50x higher)",
                ],
            )
        ],
    )

    gaps = _collect_sqlite_gaps([result])
    grouped, categories = _group_sqlite_gaps(gaps)
    expected_categories = [
        "bulk_load",
        "point_read",
        "query_join_aggregate",
        "file_size",
    ]
    if categories != expected_categories:
        raise AssertionError(f"unexpected category order: {categories!r}")
    if not grouped["bulk_load"][0].material:
        raise AssertionError("bulk_load gap should be material")
    if grouped["point_read"][0].material:
        raise AssertionError("point_read gap should stay below the absolute delta threshold")
    if not grouped["query_join_aggregate"][0].material:
        raise AssertionError("query_join_aggregate gap should be material")
    if not grouped["file_size"][0].material:
        raise AssertionError("file_size gap should be material")


def render_final(
    console: Console,
    benchmark_results: list[BenchmarkResult],
    strict: bool,
) -> int:
    failed = [result for result in benchmark_results if not result.command_result.ok]
    sqlite_wins = sum(result.sqlite_win_count for result in benchmark_results)
    if failed:
        console.print(
            Panel(
                f"{len(failed)} benchmark command(s) failed. Check the logs above.",
                title="Result",
                border_style="red",
                box=box.ROUNDED,
            )
        )
        return 1
    if sqlite_wins:
        border = "red" if strict else "yellow"
        message = (
            f"SQLite is still better in {sqlite_wins} measured area(s). "
            "Use the red details above as the next optimization task list."
        )
        if strict:
            message += " Strict mode returns a failing exit code."
        console.print(Panel(message, title="Result", border_style=border, box=box.ROUNDED))
        return 3 if strict else 0

    console.print(
        Panel(
            "DecentDB is at parity with or faster than SQLite in every parsed benchmark comparison.",
            title="Result",
            border_style="green",
            box=box.ROUNDED,
        )
    )
    return 0


def add_preflight_commands(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    commands: list[tuple[str, list[str]]] = []
    if not args.skip_preflight:
        commands.append(("cargo fmt", ["cargo", "fmt", "--check"]))
        commands.append(("cargo check", ["cargo", "check", "-p", "decentdb"]))
    if not args.skip_rust_build:
        commands.append(("cargo release build", ["cargo", "build", "-p", "decentdb", "--release"]))
    commands.append(
        (
            "python py_compile",
            [
                sys.executable,
                "-m",
                "py_compile",
                "bindings/python/decentdb/__init__.py",
                "bindings/python/benchmarks/bench_complex.py",
            ],
        )
    )
    return commands


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run DecentDB vs SQLite validation benchmarks and render a Rich summary."
        )
    )
    parser.add_argument(
        "--profile",
        choices=["quick", "standard", "full"],
        default="full",
        help=(
            "quick: reduced Showdown repetitions only; standard: quick plus "
            "Showdown smoke; full: standard plus comparable-profile GLM52 and "
            "MovieDB scratch validations (default: full)"
        ),
    )
    parser.add_argument(
        "--reduced-runs",
        type=int,
        default=3,
        help="Number of reduced Showdown repetitions (default: 3)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for logs and generated database prefixes (default: .tmp/perf-validate/<timestamp>)",
    )
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="Pass --keep-db to bench_complex.py so generated DB files remain after each run.",
    )
    parser.add_argument(
        "--strict",
        dest="strict",
        action="store_true",
        default=True,
        help=(
            "Return a failing exit code if any parsed metric still favors SQLite "
            "(default)."
        ),
    )
    parser.add_argument(
        "--report-only",
        dest="strict",
        action="store_false",
        help="Print remaining SQLite wins but return success if commands complete.",
    )
    parser.add_argument(
        "--max-details",
        type=int,
        default=200,
        help="Maximum rows in each detailed Rich table (default: 200)",
    )
    parser.add_argument(
        "--fastdecode",
        choices=["auto", "force", "skip"],
        default="auto",
        help="Rebuild Python _fastdecode extension if stale, always, or never (default: auto)",
    )
    parser.add_argument(
        "--cc",
        default=os.environ.get("CC", "gcc"),
        help="C compiler for _fastdecode.c (default: $CC or gcc)",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip cargo fmt and cargo check. The release build still runs unless --skip-rust-build is set.",
    )
    parser.add_argument(
        "--skip-rust-build",
        action="store_true",
        help="Skip cargo build -p decentdb --release.",
    )
    parser.add_argument(
        "--sqlite-profile",
        choices=["wal_normal", "wal_full", "delete_full"],
        default=None,
        help="Override the benchmark's SQLite profile.",
    )
    parser.add_argument(
        "--sqlite-cache-mb",
        type=int,
        default=None,
        help="Override SQLite cache size in MiB.",
    )
    parser.add_argument(
        "--decentdb-options",
        default=None,
        help="Override DecentDB options for benchmark runs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned commands without running them.",
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="Run the SQLite gap parsing/grouping smoke check and exit.",
    )
    parser.add_argument(
        "--echo",
        action="store_true",
        help="Echo subprocess output to the terminal as well as logs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.reduced_runs < 1:
        raise SystemExit("--reduced-runs must be at least 1")
    if args.max_details < 1:
        raise SystemExit("--max-details must be at least 1")

    console = Console()
    if args.self_check:
        _self_check_sqlite_gap_helpers()
        console.print("SQLite gap helper self-check passed.")
        return 0

    output_dir = (args.output_dir or default_output_dir()).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    render_plan(console, args, output_dir)

    preflight_commands = add_preflight_commands(args)
    specs = build_benchmark_specs(args, output_dir)

    if args.dry_run:
        console.print(Rule("Preflight Commands"))
        for label, command in preflight_commands:
            console.print(f"[bold]{label}[/]: {shlex_join(command)}")
        console.print(
            f"[bold]fastdecode extension[/]: mode={args.fastdecode}, cc={args.cc}"
        )
        console.print(Rule("Benchmark Commands"))
        for spec in specs:
            console.print(f"[bold]{spec.label}[/]: {shlex_join(benchmark_command(spec, args))}")
        return 0

    preflight_results: list[CommandResult] = []
    for index, (label, command) in enumerate(preflight_commands, start=1):
        result = run_command(
            console=console,
            label=label,
            command=command,
            log_path=output_dir / f"preflight_{index}_{label.replace(' ', '_')}.log",
            env=python_env(),
            echo=args.echo,
        )
        preflight_results.append(result)
        if not result.ok:
            render_preflight(console, preflight_results)
            console.print(
                Panel(
                    f"Preflight failed at {label}. See {rel(result.log_path)}.",
                    title="Stopped",
                    border_style="red",
                    box=box.ROUNDED,
                )
            )
            return 1

    fastdecode_result = maybe_rebuild_fastdecode(
        console=console,
        output_dir=output_dir,
        mode=args.fastdecode,
        cc=args.cc,
    )
    preflight_results.append(fastdecode_result)
    render_preflight(console, preflight_results)
    if not fastdecode_result.ok:
        console.print(
            Panel(
                fastdecode_result.skipped_reason
                or f"Fastdecode build failed. See {rel(fastdecode_result.log_path)}.",
                title="Stopped",
                border_style="red",
                box=box.ROUNDED,
            )
        )
        return 1

    benchmark_results: list[BenchmarkResult] = []
    for index, spec in enumerate(specs, start=1):
        console.print(Rule(spec.label))
        command = benchmark_command(spec, args)
        result = run_command(
            console=console,
            label=spec.label,
            command=command,
            log_path=output_dir / f"benchmark_{index}_{spec.label.lower().replace(' ', '_').replace('#', '')}.log",
            env=python_env(),
            echo=args.echo,
        )
        comparisons = parse_comparisons(result.log_path) if result.log_path else []
        benchmark_result = BenchmarkResult(
            label=spec.label,
            command_result=result,
            comparisons=comparisons,
        )
        benchmark_results.append(benchmark_result)
        if not result.ok:
            break

    render_benchmark_summary(console, benchmark_results)
    render_detail_table(
        console,
        "DecentDB Better",
        "green",
        benchmark_results,
        "decentdb_better",
        args.max_details,
    )
    render_detail_table(
        console,
        "SQLite Better / Remaining Gaps",
        "red",
        benchmark_results,
        "sqlite_better",
        args.max_details,
    )
    render_sqlite_gap_groups(console, benchmark_results)
    render_detail_table(
        console,
        "Ties",
        "yellow",
        benchmark_results,
        "ties",
        args.max_details,
    )
    skipped_total = sum(result.skipped_count for result in benchmark_results)
    if skipped_total:
        render_detail_table(
            console,
            "Skipped / Unsupported",
            "magenta",
            benchmark_results,
            "skipped",
            args.max_details,
        )

    return render_final(console, benchmark_results, args.strict)


if __name__ == "__main__":
    raise SystemExit(main())
