#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gc
import json
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from dataclasses import dataclass
from pathlib import Path

import decentdb
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

console = Console()


@dataclass
class IterationStats:
    iteration: int
    rss_mb: float
    rss_delta_mb: float
    py_heap_mb: float
    py_heap_peak_mb: float
    iter_ms: float
    rows_per_sec: float
    wal_mb: float


def rss_mb() -> float:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as status_file:
            for line in status_file:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    return int(parts[1]) / 1024.0
    except OSError:
        pass
    return 0.0


def vmhwm_mb() -> float:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as status_file:
            for line in status_file:
                if line.startswith("VmHWM:"):
                    parts = line.split()
                    return int(parts[1]) / 1024.0
    except OSError:
        pass
    return 0.0


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
    for path in db_artifact_paths(db_path):
        if path.exists():
            path.unlink()


def copy_db_artifacts(src_db: Path, dst_db: Path) -> None:
    remove_db_artifacts(dst_db)
    for src in db_artifact_paths(src_db):
        if src.exists():
            suffix = str(src).removeprefix(str(src_db))
            dst = Path(f"{dst_db}{suffix}")
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


def total_artifact_size_mb(db_path: Path) -> float:
    total_bytes = 0
    for path in db_artifact_paths(db_path):
        if path.exists():
            total_bytes += path.stat().st_size
    return total_bytes / (1024.0 * 1024.0)


def linear_slope(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    xs = list(range(len(values)))
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(values)
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom == 0.0:
        return 0.0
    numer = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    return numer / denom


def build_engine_template(db_path: Path, rows: int, payload_bytes: int) -> None:
    remove_db_artifacts(db_path)
    conn = decentdb.connect(str(db_path), mode="create")
    cur = conn.cursor()
    cur.execute("CREATE TABLE leak_engine (id INT64 PRIMARY KEY, category TEXT, payload BLOB)")
    cur.execute("BEGIN")
    for idx in range(1, rows + 1):
        category = f"group_{idx % 25}"
        payload = bytes(((idx + n) % 256 for n in range(payload_bytes)))
        cur.execute(
            "INSERT INTO leak_engine (id, category, payload) VALUES ($1, $2, $3)",
            (idx, category, payload),
        )
        if idx % 500 == 0:
            cur.execute("COMMIT")
            cur.execute("BEGIN")
    conn.commit()
    cur.execute("CREATE INDEX idx_leak_engine_category ON leak_engine(category)")
    conn.commit()
    conn.close()


def engine_worker(db_path: Path) -> int:
    payload: dict[str, float | int] = {"rss_before_open_mb": rss_mb()}
    open_started = time.perf_counter()
    conn = decentdb.connect(str(db_path), mode="open", stmt_cache_size=32)
    payload["open_ms"] = round((time.perf_counter() - open_started) * 1000.0, 3)
    payload["rss_after_open_mb"] = rss_mb()
    query_started = time.perf_counter()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM leak_engine")
    payload["rows"] = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM leak_engine WHERE category = ?", ("group_7",))
    payload["indexed_count"] = int(cur.fetchone()[0])
    payload["query_ms"] = round((time.perf_counter() - query_started) * 1000.0, 3)
    conn.close()
    payload["rss_after_close_mb"] = rss_mb()
    payload["hwm_mb"] = vmhwm_mb()
    print(json.dumps(payload))
    return 0


def run_engine_fresh_process_phase(script_path: Path, work_dir: Path, runs: int, rows: int, payload_bytes: int) -> dict:
    console.print("\n[bold cyan]Phase 1/2: Engine fresh-process leak test[/bold cyan]")
    template_db = work_dir / "engine-template.ddb"
    build_engine_template(template_db, rows=rows, payload_bytes=payload_bytes)
    before_open_series: list[float] = []
    close_series: list[float] = []
    hwm_series: list[float] = []

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )
    with progress:
        task_id = progress.add_task("Running subprocess open/query/close checks", total=runs)
        for run in range(1, runs + 1):
            run_db = work_dir / f"engine-run-{run}.ddb"
            copy_db_artifacts(template_db, run_db)
            proc = subprocess.run(
                [sys.executable, str(script_path), "--mode", "engine-worker", "--db-path", str(run_db)],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"engine worker failed run={run}: {proc.stderr or proc.stdout}")
            result = json.loads(proc.stdout.strip().splitlines()[-1])
            before_open_series.append(float(result["rss_before_open_mb"]))
            close_series.append(float(result["rss_after_close_mb"]))
            hwm_series.append(float(result["hwm_mb"]))
            progress.update(task_id, advance=1, description=f"Run {run}/{runs} rss_before={result['rss_before_open_mb']:.2f} MB")
            remove_db_artifacts(run_db)

    remove_db_artifacts(template_db)

    return {
        "runs": runs,
        "rss_before_open_mb": before_open_series,
        "rss_after_close_mb": close_series,
        "hwm_mb": hwm_series,
        "rss_before_open_slope_mb_per_run": linear_slope(before_open_series),
        "rss_before_open_drift_mb": before_open_series[-1] - before_open_series[0] if len(before_open_series) > 1 else 0.0,
    }


def run_binding_iteration(db_path: Path, iteration: int, rows_per_step: int) -> tuple[float, float]:
    table_name = "leak_cycle"
    index_name = "idx_leak_cycle_category"

    started = time.perf_counter()
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=0)
    cur = conn.cursor()

    cur.execute(f"DROP TABLE {table_name}") if table_name in conn.list_tables() else None
    cur.execute(f"CREATE TABLE {table_name} (id INT64 PRIMARY KEY, category TEXT, payload TEXT)")

    insert_data_1 = [(idx, f"cat_{idx % 10}", f"payload_{iteration}_{idx}") for idx in range(rows_per_step)]
    cur.executemany(f"INSERT INTO {table_name} (id, category, payload) VALUES (?, ?, ?)", insert_data_1)
    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE category = ?", ("cat_5",))
    cur.fetchone()

    cur.execute(f"CREATE INDEX {index_name} ON {table_name}(category)")
    conn.commit()
    cur.close()
    cur = conn.cursor()

    base = rows_per_step
    insert_data_2 = [
        (base + idx, f"cat_{(base + idx) % 10}", f"payload_indexed_{iteration}_{idx}")
        for idx in range(rows_per_step)
    ]
    cur.executemany(f"INSERT INTO {table_name} (id, category, payload) VALUES (?, ?, ?)", insert_data_2)
    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE category = ?", ("cat_3",))
    cur.fetchone()

    cur.execute(f"DELETE FROM {table_name} WHERE id < ?", (rows_per_step // 2,))
    conn.commit()

    cur.execute(f"DROP INDEX {index_name}")
    conn.commit()

    cur.execute(f"DROP TABLE {table_name}")
    conn.commit()

    wal_mb = total_artifact_size_mb(db_path)
    conn.close()
    gc.collect()

    iter_ms = (time.perf_counter() - started) * 1000.0
    return iter_ms, wal_mb


def build_live_table(history: list[IterationStats], start_mem_mb: float, phase_started: float, rows_per_step: int) -> Panel:
    table = Table(title="Python Binding Leak Stress (In-Process)", expand=True)
    table.add_column("Iteration", justify="right", style="cyan")
    table.add_column("RSS MB", justify="right", style="magenta")
    table.add_column("ΔRSS MB", justify="right", style="yellow")
    table.add_column("Py Heap MB", justify="right", style="green")
    table.add_column("Iter ms", justify="right", style="blue")
    table.add_column("Rows/s", justify="right", style="bright_cyan")
    table.add_column("DB Artifacts MB", justify="right", style="bright_magenta")

    for item in history[-15:]:
        table.add_row(
            str(item.iteration),
            f"{item.rss_mb:.2f}",
            f"{item.rss_delta_mb:+.2f}",
            f"{item.py_heap_mb:.2f}/{item.py_heap_peak_mb:.2f}",
            f"{item.iter_ms:.2f}",
            f"{item.rows_per_sec:.0f}",
            f"{item.wal_mb:.2f}",
        )

    elapsed = time.perf_counter() - phase_started
    recent_ms = [h.iter_ms for h in history[-25:]]
    avg_ms = statistics.mean(recent_ms) if recent_ms else 0.0
    header = (
        f"elapsed={elapsed:.1f}s | start_rss={start_mem_mb:.2f} MB | "
        f"recent_avg={avg_ms:.2f} ms | logical_rows_per_iter={rows_per_step * 2}"
    )
    return Panel(table, title=header)


def run_binding_inprocess_phase(db_path: Path, iterations: int, rows_per_step: int, warmup: int) -> dict:
    console.print("\n[bold cyan]Phase 2/2: Python binding in-process leak test[/bold cyan]")
    remove_db_artifacts(db_path)
    history: list[IterationStats] = []
    start_mem_mb = rss_mb()
    phase_started = time.perf_counter()
    tracemalloc.start()

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )

    with Live(console=console, refresh_per_second=6) as live:
        with progress:
            task_id = progress.add_task("Running connection/table/index CRUD cycles", total=iterations)
            live.update(Panel(progress, title="Starting..."))
            for iteration in range(1, iterations + 1):
                iter_ms, wal_mb = run_binding_iteration(db_path=db_path, iteration=iteration, rows_per_step=rows_per_step)
                current_rss_mb = rss_mb()
                current_heap_bytes, peak_heap_bytes = tracemalloc.get_traced_memory()
                rows_total = rows_per_step * 2
                rows_per_sec = rows_total / (iter_ms / 1000.0) if iter_ms > 0 else 0.0

                history.append(
                    IterationStats(
                        iteration=iteration,
                        rss_mb=current_rss_mb,
                        rss_delta_mb=current_rss_mb - start_mem_mb,
                        py_heap_mb=current_heap_bytes / (1024.0 * 1024.0),
                        py_heap_peak_mb=peak_heap_bytes / (1024.0 * 1024.0),
                        iter_ms=iter_ms,
                        rows_per_sec=rows_per_sec,
                        wal_mb=wal_mb,
                    )
                )

                progress.update(task_id, advance=1)
                live.update(build_live_table(history, start_mem_mb, phase_started, rows_per_step))

    tracemalloc.stop()

    rss_series = [h.rss_mb for h in history]
    tail = rss_series[warmup:] if warmup < len(rss_series) else rss_series
    iter_ms_series = [h.iter_ms for h in history]

    result = {
        "iterations": iterations,
        "warmup": warmup,
        "start_rss_mb": start_mem_mb,
        "final_rss_mb": rss_series[-1] if rss_series else start_mem_mb,
        "peak_rss_mb": max(rss_series) if rss_series else start_mem_mb,
        "tail_drift_mb": tail[-1] - tail[0] if len(tail) > 1 else 0.0,
        "tail_slope_mb_per_iter": linear_slope(tail) if len(tail) > 1 else 0.0,
        "iter_ms_avg": statistics.mean(iter_ms_series) if iter_ms_series else 0.0,
        "iter_ms_p95": statistics.quantiles(iter_ms_series, n=20)[-1] if len(iter_ms_series) >= 20 else (max(iter_ms_series) if iter_ms_series else 0.0),
    }
    remove_db_artifacts(db_path)
    return result


def print_summary(engine_result: dict, binding_result: dict, args: argparse.Namespace) -> int:
    engine_pass = (
        abs(engine_result["rss_before_open_slope_mb_per_run"]) <= args.engine_slope_threshold_mb_per_run
        and abs(engine_result["rss_before_open_drift_mb"]) <= args.engine_drift_threshold_mb
    )
    binding_pass = (
        abs(binding_result["tail_slope_mb_per_iter"]) <= args.binding_slope_threshold_mb_per_iter
        and abs(binding_result["tail_drift_mb"]) <= args.binding_drift_threshold_mb
    )
    overall_pass = engine_pass and binding_pass

    summary = Table(title="DecentDB Memory Leak Verdict", expand=True)
    summary.add_column("Check", style="cyan")
    summary.add_column("Metric", style="magenta")
    summary.add_column("Threshold", style="yellow")
    summary.add_column("Status", style="green")

    summary.add_row(
        "Engine fresh-process",
        f"slope={engine_result['rss_before_open_slope_mb_per_run']:+.4f} MB/run, drift={engine_result['rss_before_open_drift_mb']:+.3f} MB",
        f"|slope|<={args.engine_slope_threshold_mb_per_run:.4f}, |drift|<={args.engine_drift_threshold_mb:.3f}",
        "PASS" if engine_pass else "FAIL",
    )
    summary.add_row(
        "Python binding in-process",
        f"slope={binding_result['tail_slope_mb_per_iter']:+.4f} MB/iter, drift={binding_result['tail_drift_mb']:+.3f} MB",
        f"|slope|<={args.binding_slope_threshold_mb_per_iter:.4f}, |drift|<={args.binding_drift_threshold_mb:.3f}",
        "PASS" if binding_pass else "FAIL",
    )

    details = {
        "engine": engine_result,
        "binding": binding_result,
        "thresholds": {
            "engine_slope_threshold_mb_per_run": args.engine_slope_threshold_mb_per_run,
            "engine_drift_threshold_mb": args.engine_drift_threshold_mb,
            "binding_slope_threshold_mb_per_iter": args.binding_slope_threshold_mb_per_iter,
            "binding_drift_threshold_mb": args.binding_drift_threshold_mb,
        },
        "verdict": {
            "engine_pass": engine_pass,
            "binding_pass": binding_pass,
            "overall_pass": overall_pass,
        },
    }

    console.print()
    console.print(summary)
    console.print(Panel("PASS" if overall_pass else "FAIL", title="Overall", border_style="green" if overall_pass else "red"))
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(details, indent=2), encoding="utf-8")
        console.print(f"\n[bold]JSON report:[/bold] {args.json_out}")

    return 0 if overall_pass else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DecentDB engine + Python binding memory leak showcase")
    parser.add_argument("--mode", choices=["full", "engine-worker"], default="full")
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--engine-runs", type=int, default=30)
    parser.add_argument("--engine-template-rows", type=int, default=5000)
    parser.add_argument("--engine-payload-bytes", type=int, default=512)
    parser.add_argument("--binding-iterations", type=int, default=300)
    parser.add_argument("--binding-rows-per-step", type=int, default=250)
    parser.add_argument("--binding-warmup", type=int, default=20)
    parser.add_argument("--engine-slope-threshold-mb-per-run", type=float, default=0.020)
    parser.add_argument("--engine-drift-threshold-mb", type=float, default=2.0)
    parser.add_argument("--binding-slope-threshold-mb-per-iter", type=float, default=0.015)
    parser.add_argument("--binding-drift-threshold-mb", type=float, default=4.0)
    parser.add_argument("--json-out", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.mode == "engine-worker":
        if args.db_path is None:
            raise SystemExit("--db-path is required for --mode engine-worker")
        return engine_worker(args.db_path)

    started = time.perf_counter()
    tmp_root = Path(tempfile.gettempdir()) / "decentdb-memory-leak-showcase"
    tmp_root.mkdir(parents=True, exist_ok=True)

    engine_result = run_engine_fresh_process_phase(
        script_path=Path(__file__).resolve(),
        work_dir=tmp_root,
        runs=args.engine_runs,
        rows=args.engine_template_rows,
        payload_bytes=args.engine_payload_bytes,
    )

    binding_db_path = tmp_root / "binding-stress.ddb"
    binding_result = run_binding_inprocess_phase(
        db_path=binding_db_path,
        iterations=args.binding_iterations,
        rows_per_step=args.binding_rows_per_step,
        warmup=args.binding_warmup,
    )

    console.print(f"\n[bold]Elapsed:[/bold] {time.perf_counter() - started:.2f}s")
    return print_summary(engine_result, binding_result, args)


if __name__ == "__main__":
    sys.exit(main())
