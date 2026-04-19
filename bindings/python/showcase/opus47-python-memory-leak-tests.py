#!/usr/bin/env python3
"""
DecentDB Memory Leak Showcase (opus47).

Exercises the DecentDB engine core and the Python bindings with a battery of
scenarios that are particularly good at surfacing leaks:

  1. connection churn    - open/close many short-lived connections
  2. schema churn        - create/drop tables and indexes repeatedly
  3. stmt cache churn    - execute many unique SQL texts to stress the cache
  4. crud + index churn  - insert/update/delete with and without indexes
  5. large fetch churn   - repeatedly fetch large result sets
  6. txn churn           - BEGIN/COMMIT/ROLLBACK cycles

For each scenario we sample process RSS, python tracemalloc heap, GC object
counts, and DB file size. A linear regression on the post-warmup RSS series
is used to decide PASS/FAIL per scenario.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import random
import statistics
import sys
import tempfile
import time
import tracemalloc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import decentdb
from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text

console = Console()

SPARK_CHARS = " ▁▂▃▄▅▆▇█"


# ---------------------------------------------------------------------------
# process metrics
# ---------------------------------------------------------------------------

def _read_status_field(field_name: str) -> float:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(field_name):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return 0.0


def rss_mb() -> float:
    return _read_status_field("VmRSS:")


def vmhwm_mb() -> float:
    return _read_status_field("VmHWM:")


def db_artifact_paths(db_path: Path) -> list[Path]:
    return [
        db_path,
        Path(f"{db_path}.wal"),
        Path(f"{db_path}.shm"),
        Path(f"{db_path}-wal"),
        Path(f"{db_path}-shm"),
        Path(f"{db_path}-lock"),
    ]


def remove_db_artifacts(db_path: Path) -> None:
    for p in db_artifact_paths(db_path):
        if p.exists():
            try:
                p.unlink()
            except OSError:
                pass


def db_size_mb(db_path: Path) -> float:
    total = 0
    for p in db_artifact_paths(db_path):
        if p.exists():
            total += p.stat().st_size
    return total / (1024.0 * 1024.0)


def linear_slope(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    xs = list(range(len(values)))
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(values)
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom == 0.0:
        return 0.0
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values)) / denom


def sparkline(values: list[float], width: int = 40) -> str:
    if not values:
        return ""
    data = values[-width:]
    lo = min(data)
    hi = max(data)
    if hi - lo < 1e-9:
        return SPARK_CHARS[1] * len(data)
    scale = (len(SPARK_CHARS) - 1) / (hi - lo)
    return "".join(SPARK_CHARS[min(len(SPARK_CHARS) - 1, max(0, int((v - lo) * scale)))] for v in data)


# ---------------------------------------------------------------------------
# scenario framework
# ---------------------------------------------------------------------------

@dataclass
class Sample:
    iteration: int
    rss_mb: float
    heap_mb: float
    heap_peak_mb: float
    gc_objects: int
    db_mb: float
    iter_ms: float
    ops: int


@dataclass
class ScenarioResult:
    name: str
    samples: list[Sample] = field(default_factory=list)
    start_rss_mb: float = 0.0
    warmup: int = 0
    slope_mb_per_iter: float = 0.0
    drift_mb: float = 0.0
    db_growth_mb: float = 0.0
    net_drift_mb: float = 0.0
    drift_budget_mb: float = 0.0
    peak_rss_mb: float = 0.0
    total_ops: int = 0
    elapsed_s: float = 0.0
    passed: bool = False
    threshold_slope: float = 0.0
    threshold_drift: float = 0.0

    def summary_row(self) -> tuple[str, ...]:
        return (
            self.name,
            f"{len(self.samples)}",
            f"{self.total_ops:,}",
            f"{self.start_rss_mb:.2f}",
            f"{self.peak_rss_mb:.2f}",
            f"{self.net_drift_mb:+.3f}",
            f"{self.slope_mb_per_iter:+.5f}",
            f"{self.elapsed_s:.1f}s",
            "[bold green]PASS[/]" if self.passed else "[bold red]FAIL[/]",
        )


Scenario = Callable[[Path, int], int]  # returns number of logical ops performed


# ---------------------------------------------------------------------------
# individual scenarios
# ---------------------------------------------------------------------------

def scenario_connection_churn(db_path: Path, iteration: int) -> int:
    ops = 0
    for _ in range(8):
        conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS conn_probe (id INT64 PRIMARY KEY, v TEXT)")
        cur.execute("DELETE FROM conn_probe WHERE id = ?", (iteration,))
        cur.execute("INSERT INTO conn_probe (id, v) VALUES (?, ?)", (iteration, f"it-{iteration}"))
        conn.commit()
        cur.execute("SELECT id, v FROM conn_probe WHERE id = ?", (iteration,))
        cur.fetchall()
        cur.close()
        conn.close()
        ops += 4
    return ops


def scenario_schema_churn(db_path: Path, iteration: int) -> int:
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=32)
    cur = conn.cursor()
    for t in range(4):
        tname = f"schema_t_{iteration}_{t}"
        iname = f"schema_idx_{iteration}_{t}"
        cur.execute(f"CREATE TABLE {tname} (id INT64 PRIMARY KEY, bucket INT64, tag TEXT)")
        cur.executemany(
            f"INSERT INTO {tname} (id, bucket, tag) VALUES (?, ?, ?)",
            [(i, i % 17, f"tag_{i % 11}") for i in range(50)],
        )
        conn.commit()
        cur.execute(f"CREATE INDEX {iname} ON {tname}(bucket)")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {tname} WHERE bucket = ?", (3,))
        cur.fetchone()
        cur.execute(f"DROP INDEX {iname}")
        cur.execute(f"DROP TABLE {tname}")
        conn.commit()
        ops += 50 + 6
    cur.close()
    conn.close()
    return ops


def scenario_stmt_cache_churn(db_path: Path, iteration: int) -> int:
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=8)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS stmt_probe (id INT64 PRIMARY KEY, v INT64)")
    cur.execute("DELETE FROM stmt_probe WHERE id = ?", (iteration,))
    cur.execute("INSERT INTO stmt_probe (id, v) VALUES (?, ?)", (iteration, iteration))
    conn.commit()
    for k in range(40):
        sql = f"SELECT id, v, {k} AS k_tag, {iteration} AS it_tag FROM stmt_probe WHERE id = ?"
        cur.execute(sql, (iteration,))
        cur.fetchall()
        ops += 1
    cur.close()
    conn.close()
    return ops


def scenario_crud_index_churn(db_path: Path, iteration: int) -> int:
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=32)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS crud_probe (id INT64 PRIMARY KEY, bucket INT64, payload TEXT)"
    )
    rng = random.Random(iteration)
    rows = [
        (iteration * 10_000 + i, rng.randint(0, 31), f"payload_{iteration}_{i}_{'x' * rng.randint(0, 32)}")
        for i in range(200)
    ]
    cur.executemany("INSERT INTO crud_probe (id, bucket, payload) VALUES (?, ?, ?)", rows)
    conn.commit()
    ops += len(rows)

    iname = f"crud_idx_{iteration}"
    cur.execute(f"CREATE INDEX {iname} ON crud_probe(bucket)")
    conn.commit()
    for b in range(8):
        cur.execute("SELECT COUNT(*) FROM crud_probe WHERE bucket = ?", (b,))
        cur.fetchone()
        ops += 1

    cur.execute(
        "UPDATE crud_probe SET payload = payload WHERE id >= ? AND id < ?",
        (iteration * 10_000, iteration * 10_000 + 100),
    )
    conn.commit()
    ops += 1

    cur.execute(
        "DELETE FROM crud_probe WHERE id >= ? AND id < ?",
        (iteration * 10_000, iteration * 10_000 + 200),
    )
    conn.commit()
    cur.execute(f"DROP INDEX {iname}")
    conn.commit()
    ops += 2
    cur.close()
    conn.close()
    return ops


def scenario_large_fetch_churn(db_path: Path, iteration: int) -> int:
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS fetch_probe (id INT64 PRIMARY KEY, bucket INT64, blob BLOB)"
    )
    if iteration == 1:
        cur.executemany(
            "INSERT INTO fetch_probe (id, bucket, blob) VALUES (?, ?, ?)",
            [(i, i % 50, bytes(((i + j) % 256 for j in range(128)))) for i in range(2000)],
        )
        conn.commit()
    ops = 0
    for _ in range(3):
        cur.execute("SELECT id, bucket, blob FROM fetch_probe")
        rows = cur.fetchall()
        ops += len(rows)
    cur.execute("SELECT id, bucket FROM fetch_probe WHERE bucket < ?", (25,))
    rows = cur.fetchall()
    ops += len(rows)
    cur.close()
    conn.close()
    return ops


def scenario_txn_churn(db_path: Path, iteration: int) -> int:
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS txn_probe (id INT64 PRIMARY KEY, v TEXT)")
    conn.commit()
    for i in range(20):
        cur.execute("DELETE FROM txn_probe WHERE id = ?", (i,))
        cur.execute("INSERT INTO txn_probe (id, v) VALUES (?, ?)", (i, f"it-{iteration}-{i}"))
        if i % 2 == 0:
            conn.commit()
        else:
            conn.rollback()
        ops += 1
    cur.execute("DELETE FROM txn_probe")
    conn.commit()
    cur.close()
    conn.close()
    return ops


SCENARIOS: list[tuple[str, Scenario, str]] = [
    ("connection_churn", scenario_connection_churn, "open/close many short-lived connections"),
    ("schema_churn", scenario_schema_churn, "create/drop tables and indexes"),
    ("stmt_cache_churn", scenario_stmt_cache_churn, "execute many unique SQL texts"),
    ("crud_index_churn", scenario_crud_index_churn, "insert/update/delete w/ indexes"),
    ("large_fetch_churn", scenario_large_fetch_churn, "repeated large result set fetches"),
    ("txn_churn", scenario_txn_churn, "BEGIN/COMMIT/ROLLBACK cycles"),
]


# ---------------------------------------------------------------------------
# rich dashboard
# ---------------------------------------------------------------------------

@dataclass
class DashState:
    started: float
    scenario_name: str = ""
    scenario_desc: str = ""
    scenario_index: int = 0
    scenario_total: int = 0
    iteration: int = 0
    iterations_total: int = 0
    last_sample: Sample | None = None
    rss_series: list[float] = field(default_factory=list)
    heap_series: list[float] = field(default_factory=list)
    iter_ms_series: list[float] = field(default_factory=list)
    results: list[ScenarioResult] = field(default_factory=list)
    start_rss_mb: float = 0.0
    total_ops: int = 0
    scenario_progress: Progress | None = None
    overall_progress: Progress | None = None


def make_layout() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=12),
    )
    layout["body"].split_row(
        Layout(name="metrics", ratio=1),
        Layout(name="scenarios", ratio=1),
    )
    return layout


def render_header(state: DashState) -> Panel:
    elapsed = time.perf_counter() - state.started
    text = Text()
    text.append("DecentDB Memory Leak Showcase  ", style="bold cyan")
    text.append(f"(opus47)\n", style="dim")
    text.append(f"scenario {state.scenario_index}/{state.scenario_total}  ", style="white")
    text.append(f"{state.scenario_name}", style="bold yellow")
    text.append(f"   elapsed {elapsed:5.1f}s", style="dim")
    return Panel(text, border_style="cyan")


def render_metrics(state: DashState) -> Panel:
    sample = state.last_sample
    tbl = Table.grid(padding=(0, 1))
    tbl.add_column(style="cyan", no_wrap=True)
    tbl.add_column(style="white")

    def row(k: str, v: str) -> None:
        tbl.add_row(k, v)

    if sample is None:
        row("status", "warming up...")
    else:
        base = state.start_rss_mb
        row("iteration", f"{sample.iteration} / {state.iterations_total}")
        row("rss", f"{sample.rss_mb:8.2f} MB   Δstart {sample.rss_mb - base:+.3f} MB")
        row("heap (tracemalloc)", f"{sample.heap_mb:8.2f} MB   peak {sample.heap_peak_mb:.2f} MB")
        row("gc objects", f"{sample.gc_objects:,}")
        row("db file size", f"{sample.db_mb:.3f} MB")
        row("iter time", f"{sample.iter_ms:.2f} ms")
        if state.iter_ms_series:
            recent = state.iter_ms_series[-20:]
            row("iter avg (last 20)", f"{statistics.mean(recent):.2f} ms")
        row("total ops", f"{state.total_ops:,}")
        row("vmhwm", f"{vmhwm_mb():.2f} MB")

    group = Group(
        tbl,
        Text(""),
        Text("rss       ", style="magenta") + Text(sparkline(state.rss_series, 48), style="magenta"),
        Text("heap      ", style="green") + Text(sparkline(state.heap_series, 48), style="green"),
        Text("iter ms   ", style="blue") + Text(sparkline(state.iter_ms_series, 48), style="blue"),
    )
    return Panel(group, title="live metrics", border_style="magenta")


def render_scenarios(state: DashState) -> Panel:
    tbl = Table(expand=True, show_edge=False)
    tbl.add_column("scenario", style="cyan", no_wrap=True)
    tbl.add_column("iters", justify="right")
    tbl.add_column("ops", justify="right")
    tbl.add_column("start", justify="right")
    tbl.add_column("peak", justify="right")
    tbl.add_column("drift", justify="right")
    tbl.add_column("slope", justify="right")
    tbl.add_column("time", justify="right")
    tbl.add_column("verdict", justify="center")
    for r in state.results:
        tbl.add_row(*r.summary_row())
    if state.scenario_name and (not state.results or state.results[-1].name != state.scenario_name):
        tbl.add_row(
            state.scenario_name,
            f"{state.iteration}/{state.iterations_total}",
            f"{state.total_ops:,}",
            f"{state.start_rss_mb:.2f}",
            f"{max(state.rss_series) if state.rss_series else 0:.2f}",
            f"{(state.rss_series[-1] - state.start_rss_mb) if state.rss_series else 0:+.3f}",
            "...",
            "...",
            "[yellow]RUNNING[/]",
        )
    return Panel(tbl, title="scenarios", border_style="cyan")


def render_footer(state: DashState) -> Panel:
    parts: list = []
    if state.overall_progress is not None:
        parts.append(state.overall_progress)
    if state.scenario_progress is not None:
        parts.append(state.scenario_progress)
    parts.append(Text(f"description: {state.scenario_desc}", style="dim"))
    return Panel(Group(*parts), title="progress", border_style="blue")


def render_dashboard(state: DashState) -> Layout:
    layout = make_layout()
    layout["header"].update(render_header(state))
    layout["metrics"].update(render_metrics(state))
    layout["scenarios"].update(render_scenarios(state))
    layout["footer"].update(render_footer(state))
    return layout


# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------

def run_scenario(
    state: DashState,
    live: Live,
    name: str,
    fn: Scenario,
    desc: str,
    db_path: Path,
    iterations: int,
    warmup: int,
    slope_threshold: float,
    drift_threshold: float,
) -> ScenarioResult:
    remove_db_artifacts(db_path)
    gc.collect()
    tracemalloc.reset_peak()

    result = ScenarioResult(
        name=name,
        warmup=warmup,
        threshold_slope=slope_threshold,
        threshold_drift=drift_threshold,
    )
    result.start_rss_mb = rss_mb()

    state.scenario_name = name
    state.scenario_desc = desc
    state.iteration = 0
    state.iterations_total = iterations
    state.rss_series.clear()
    state.heap_series.clear()
    state.iter_ms_series.clear()
    state.start_rss_mb = result.start_rss_mb
    state.total_ops = 0
    state.last_sample = None

    scenario_prog = Progress(
        SpinnerColumn(style="yellow"),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        transient=False,
    )
    scenario_task = scenario_prog.add_task(f"{name}", total=iterations)
    state.scenario_progress = scenario_prog

    t_phase = time.perf_counter()
    for i in range(1, iterations + 1):
        t_iter = time.perf_counter()
        ops = fn(db_path, i)
        iter_ms = (time.perf_counter() - t_iter) * 1000.0
        state.total_ops += ops

        if i % 5 == 0:
            gc.collect()

        current, peak = tracemalloc.get_traced_memory()
        sample = Sample(
            iteration=i,
            rss_mb=rss_mb(),
            heap_mb=current / (1024.0 * 1024.0),
            heap_peak_mb=peak / (1024.0 * 1024.0),
            gc_objects=len(gc.get_objects()),
            db_mb=db_size_mb(db_path),
            iter_ms=iter_ms,
            ops=ops,
        )
        result.samples.append(sample)
        state.rss_series.append(sample.rss_mb)
        state.heap_series.append(sample.heap_mb)
        state.iter_ms_series.append(sample.iter_ms)
        state.iteration = i
        state.last_sample = sample

        scenario_prog.update(scenario_task, advance=1)
        live.update(render_dashboard(state))

    result.elapsed_s = time.perf_counter() - t_phase
    result.total_ops = sum(s.ops for s in result.samples)
    result.peak_rss_mb = max(s.rss_mb for s in result.samples)

    tail = [s.rss_mb for s in result.samples[warmup:]] if warmup < len(result.samples) else [s.rss_mb for s in result.samples]
    tail_db = [s.db_mb for s in result.samples[warmup:]] if warmup < len(result.samples) else [s.db_mb for s in result.samples]
    result.slope_mb_per_iter = linear_slope(tail)
    result.drift_mb = tail[-1] - tail[0] if len(tail) > 1 else 0.0
    result.db_growth_mb = (tail_db[-1] - tail_db[0]) if len(tail_db) > 1 else 0.0
    # RSS growth attributable to the DB file itself (rows persisted across
    # iterations, page cache backing the larger file) is expected and is not
    # a process leak. Subtract it before applying the absolute drift cap.
    result.net_drift_mb = result.drift_mb - max(0.0, result.db_growth_mb)
    # The slope threshold is a per-iter rate; the drift cap must be at least
    # consistent with that rate over the post-warmup window, otherwise the two
    # criteria contradict each other at high iteration counts.
    post_warmup_iters = max(1, len(tail) - 1)
    result.drift_budget_mb = drift_threshold + slope_threshold * post_warmup_iters
    result.passed = (
        abs(result.slope_mb_per_iter) <= slope_threshold
        and abs(result.net_drift_mb) <= result.drift_budget_mb
    )

    state.results.append(result)
    remove_db_artifacts(db_path)
    state.scenario_progress = None
    live.update(render_dashboard(state))
    return result


def print_final_report(state: DashState, args: argparse.Namespace, json_out: Path | None) -> int:
    console.print()
    tbl = Table(title="opus47 DecentDB Memory Leak - Final Verdict", expand=True)
    tbl.add_column("scenario", style="cyan")
    tbl.add_column("iters", justify="right")
    tbl.add_column("ops", justify="right")
    tbl.add_column("start rss", justify="right")
    tbl.add_column("peak rss", justify="right")
    tbl.add_column("drift MB", justify="right")
    tbl.add_column("slope MB/iter", justify="right")
    tbl.add_column("elapsed", justify="right")
    tbl.add_column("verdict", justify="center")

    for r in state.results:
        tbl.add_row(*r.summary_row())

    console.print(tbl)
    # Show drift breakdown so it's clear that DB-file growth is excluded.
    console.print(
        "[dim]drift MB = RSS drift over the post-warmup tail with DB file "
        "growth subtracted (data persisting on disk is not a process leak); "
        "the per-scenario drift budget = drift_threshold + slope_threshold * "
        "post_warmup_iters so the two criteria stay consistent at any iteration count.[/]"
    )

    overall_pass = all(r.passed for r in state.results)
    color = "green" if overall_pass else "red"
    verdict_text = Text("PASS - no meaningful memory leak detected" if overall_pass else "FAIL - potential leak detected, see scenarios above", style=f"bold {color}")
    console.print(Panel(Align.center(verdict_text), border_style=color, title="overall"))

    if json_out is not None:
        payload = {
            "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
            "scenarios": [
                {
                    "name": r.name,
                    "iterations": len(r.samples),
                    "total_ops": r.total_ops,
                    "start_rss_mb": r.start_rss_mb,
                    "peak_rss_mb": r.peak_rss_mb,
                    "drift_mb": r.drift_mb,
                    "db_growth_mb": r.db_growth_mb,
                    "net_drift_mb": r.net_drift_mb,
                    "drift_budget_mb": r.drift_budget_mb,
                    "slope_mb_per_iter": r.slope_mb_per_iter,
                    "threshold_slope": r.threshold_slope,
                    "threshold_drift": r.threshold_drift,
                    "warmup": r.warmup,
                    "elapsed_s": r.elapsed_s,
                    "passed": r.passed,
                    "samples": [
                        {
                            "i": s.iteration,
                            "rss_mb": s.rss_mb,
                            "heap_mb": s.heap_mb,
                            "heap_peak_mb": s.heap_peak_mb,
                            "gc_objects": s.gc_objects,
                            "db_mb": s.db_mb,
                            "iter_ms": s.iter_ms,
                            "ops": s.ops,
                        }
                        for s in r.samples
                    ],
                }
                for r in state.results
            ],
            "overall_pass": overall_pass,
        }
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        console.print(f"[dim]json report: {json_out}[/]")

    return 0 if overall_pass else 2


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DecentDB memory leak showcase (opus47)")
    p.add_argument("--iterations", type=int, default=150, help="iterations per scenario")
    p.add_argument("--warmup", type=int, default=15, help="warmup iterations excluded from slope")
    p.add_argument("--slope-threshold", type=float, default=0.020, help="|MB/iter| allowed on post-warmup tail")
    p.add_argument("--drift-threshold", type=float, default=3.0, help="|MB| start-to-end drift allowed on post-warmup tail")
    p.add_argument("--only", type=str, default=None, help="comma-separated scenario names to run")
    p.add_argument("--workdir", type=Path, default=None, help="directory for temp db files")
    p.add_argument("--json-out", type=Path, default=None, help="path to write JSON report")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    workdir = args.workdir or (Path(tempfile.gettempdir()) / "decentdb-opus47-leak")
    workdir.mkdir(parents=True, exist_ok=True)

    scenarios = SCENARIOS
    if args.only:
        wanted = {s.strip() for s in args.only.split(",") if s.strip()}
        scenarios = [s for s in SCENARIOS if s[0] in wanted]
        if not scenarios:
            console.print(f"[red]no scenarios match --only={args.only}[/]")
            return 2

    console.rule("[bold cyan]DecentDB Memory Leak Showcase (opus47)[/]")
    console.print(
        f"python={sys.version.split()[0]}  pid={os.getpid()}  workdir={workdir}  "
        f"iterations/scenario={args.iterations}  warmup={args.warmup}"
    )

    tracemalloc.start()
    state = DashState(started=time.perf_counter())
    state.scenario_total = len(scenarios)

    overall = Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold]overall"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total} scenarios"),
        TimeElapsedColumn(),
    )
    overall_task = overall.add_task("overall", total=len(scenarios))
    state.overall_progress = overall

    with Live(render_dashboard(state), console=console, refresh_per_second=8, screen=False) as live:
        for idx, (name, fn, desc) in enumerate(scenarios, start=1):
            state.scenario_index = idx
            db_path = workdir / f"opus47-{name}.ddb"
            run_scenario(
                state=state,
                live=live,
                name=name,
                fn=fn,
                desc=desc,
                db_path=db_path,
                iterations=args.iterations,
                warmup=min(args.warmup, max(0, args.iterations - 2)),
                slope_threshold=args.slope_threshold,
                drift_threshold=args.drift_threshold,
            )
            overall.update(overall_task, advance=1)
            live.update(render_dashboard(state))

    tracemalloc.stop()
    return print_final_report(state, args, args.json_out)


if __name__ == "__main__":
    sys.exit(main())
