#!/usr/bin/env python3
"""
DecentDB memory leak showcase (GPT-5.4).

This script does two things:

1. Fresh-process engine cycles:
   - create a non-trivial seed database once
   - copy it for each run
   - spawn a fresh Python process
   - open/query/index/checkpoint/drop/close
   - compare post-close RSS across runs

   This isolates the native engine + C ABI lifetime from long-lived Python
   process accumulation.

2. In-process Python binding cycles:
   - keep one Python process alive
   - repeatedly open/close connections
   - create/drop tables and indexes
   - insert/query/update/delete rows
   - introspect schema and storage state
   - checkpoint and close
   - track RSS + Python heap over time

The result is a clear PASS/FAIL verdict for both surfaces.
"""

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
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import decentdb
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

console = Console()
MB = 1024.0 * 1024.0
SPARK_CHARS = " ▁▂▃▄▅▆▇█"


@dataclass
class CycleStats:
    elapsed_ms: float
    ops: int
    rows_written: int
    rows_read: int
    seed_count: int
    indexed_count: int
    artifact_mb: float
    wal_mb: float
    table_count: int
    index_count: int
    wal_versions: int | None
    active_readers: int | None
    wal_file_size_mb: float | None


@dataclass
class EngineRunSample:
    run: int
    rss_before_open_mb: float
    rss_after_open_mb: float
    rss_after_cycle_mb: float
    rss_after_close_mb: float
    hwm_mb: float
    open_ms: float
    cycle_ms: float
    ops: int
    rows_written: int
    rows_read: int
    artifact_mb: float
    wal_mb: float
    wal_versions: int | None
    active_readers: int | None


@dataclass
class BindingSample:
    iteration: int
    rss_mb: float
    rss_delta_mb: float
    hwm_mb: float
    py_heap_mb: float
    py_heap_peak_mb: float
    iter_ms: float
    ops_per_sec: float
    rows_per_sec: float
    artifact_mb: float
    wal_mb: float
    wal_versions: int | None
    active_readers: int | None
    status: str


def read_status_mb(field: str) -> float:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as status_file:
            for line in status_file:
                if line.startswith(field):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return 0.0


def rss_mb() -> float:
    return read_status_mb("VmRSS:")


def vmhwm_mb() -> float:
    return read_status_mb("VmHWM:")


def db_artifact_paths(db_path: Path) -> list[Path]:
    return [
        db_path,
        Path(f"{db_path}.wal"),
        Path(f"{db_path}.shm"),
        Path(f"{db_path}-wal"),
        Path(f"{db_path}-shm"),
        Path(f"{db_path}-lock"),
    ]


def wal_artifact_paths(db_path: Path) -> list[Path]:
    return [Path(f"{db_path}.wal"), Path(f"{db_path}-wal")]


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


def artifact_size_mb(paths: list[Path]) -> float:
    total = 0
    for path in paths:
        if path.exists():
            total += path.stat().st_size
    return total / MB


def total_artifact_size_mb(db_path: Path) -> float:
    return artifact_size_mb(db_artifact_paths(db_path))


def wal_size_mb(db_path: Path) -> float:
    return artifact_size_mb(wal_artifact_paths(db_path))


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


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    pos = (len(ordered) - 1) * (pct / 100.0)
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return ordered[lo] + (ordered[hi] - ordered[lo]) * frac


def sparkline(values: list[float], width: int = 36) -> str:
    if not values:
        return ""
    data = values[-width:]
    lo = min(data)
    hi = max(data)
    if hi - lo < 1e-9:
        return SPARK_CHARS[1] * len(data)
    scale = (len(SPARK_CHARS) - 1) / (hi - lo)
    return "".join(
        SPARK_CHARS[min(len(SPARK_CHARS) - 1, max(0, int((value - lo) * scale)))]
        for value in data
    )


def storage_state(conn: decentdb.Connection) -> dict[str, Any] | None:
    inspector = getattr(conn, "inspect_storage_state", None)
    if inspector is None:
        return None
    try:
        payload = inspector()
    except decentdb.NotSupportedError:
        return None
    return payload if isinstance(payload, dict) else None


def normalize_storage_metrics(payload: dict[str, Any] | None) -> tuple[int | None, int | None, float | None]:
    if payload is None:
        return None, None, None
    wal_versions = payload.get("wal_versions")
    active_readers = payload.get("active_readers")
    wal_file_size = payload.get("wal_file_size")
    return (
        int(wal_versions) if wal_versions is not None else None,
        int(active_readers) if active_readers is not None else None,
        (float(wal_file_size) / MB) if wal_file_size is not None else None,
    )


def index_names(conn: decentdb.Connection) -> set[str]:
    return {entry["name"] for entry in conn.list_indexes()}


def cleanup_cycle_objects(
    conn: decentdb.Connection,
    cur: decentdb.Cursor,
    table_name: str,
    index_name: str,
) -> None:
    existing_indexes = index_names(conn)
    if index_name in existing_indexes:
        conn.begin_transaction()
        cur.execute(f"DROP INDEX {index_name}")
        conn.commit()

    existing_tables = set(conn.list_tables())
    if table_name in existing_tables:
        conn.begin_transaction()
        cur.execute(f"DROP TABLE {table_name}")
        conn.commit()


def build_seed_database(db_path: Path, seed_rows: int, payload_bytes: int) -> None:
    remove_db_artifacts(db_path)
    conn = decentdb.connect(str(db_path), mode="create", stmt_cache_size=64)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE seed_accounts (
            id INTEGER PRIMARY KEY,
            segment INTEGER,
            email TEXT NOT NULL,
            note TEXT,
            payload TEXT
        )
        """
    )
    conn.commit()

    conn.begin_transaction()
    rows = [
        (
            row_id,
            row_id % 16,
            f"user_{row_id}@example.com",
            f"seed-note-{row_id % 31}",
            f"{row_id:06d}-" + ("x" * payload_bytes),
        )
        for row_id in range(1, seed_rows + 1)
    ]
    cur.executemany(
        "INSERT INTO seed_accounts (id, segment, email, note, payload) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()

    conn.begin_transaction()
    cur.execute("CREATE INDEX idx_seed_accounts_segment ON seed_accounts(segment)")
    cur.execute("CREATE INDEX idx_seed_accounts_email ON seed_accounts(email)")
    conn.commit()
    conn.checkpoint()
    cur.close()
    conn.close()


def read_probe(db_path: Path, table_name: str, bucket: int, stmt_cache_size: int) -> int:
    conn = decentdb.connect(str(db_path), mode="open", stmt_cache_size=stmt_cache_size)
    cur = conn.cursor()
    cur.execute(
        f"SELECT id, bucket, note FROM {table_name} WHERE bucket = ? ORDER BY id",
        (bucket,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return len(rows)


def run_workload_cycle(
    db_path: Path,
    cycle_id: int,
    rows_per_batch: int,
    do_checkpoint: bool,
) -> CycleStats:
    table_name = "leak_lab"
    index_name = "idx_leak_lab_bucket"
    started = time.perf_counter()

    writer = decentdb.connect(
        str(db_path),
        mode="open_or_create",
        stmt_cache_size=64 if cycle_id % 2 == 0 else 0,
    )
    cur = writer.cursor()
    cleanup_cycle_objects(writer, cur, table_name, index_name)

    ops = 0
    rows_written = 0
    rows_read = 0

    cur.execute("SELECT COUNT(*) FROM seed_accounts WHERE segment = ?", (cycle_id % 16,))
    seed_count = int(cur.fetchone()[0])
    ops += 1
    rows_read += 1

    for salt in range(6):
        cur.execute("SELECT COUNT(*) FROM seed_accounts WHERE segment = ?", ((cycle_id + salt) % 16,))
        cur.fetchone()
        ops += 1
        rows_read += 1

    writer.begin_transaction()
    cur.execute(
        f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            bucket INTEGER,
            note TEXT
        )
        """
    )
    writer.commit()
    ops += 1

    writer.begin_transaction()
    batch_one = [
        (
            cycle_id * 1_000_000 + row_id,
            (cycle_id * 10_000 + row_id) % 50_000,
            row_id % 12,
            f"phase-a-{cycle_id}-{row_id}",
        )
        for row_id in range(rows_per_batch)
    ]
    cur.executemany(
        f"INSERT INTO {table_name} (id, customer_id, bucket, note) VALUES (?, ?, ?, ?)",
        batch_one,
    )
    writer.commit()
    ops += len(batch_one)
    rows_written += len(batch_one)

    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE bucket = ?", (3,))
    pre_index_count = int(cur.fetchone()[0])
    ops += 1
    rows_read += 1

    writer.begin_transaction()
    cur.execute(f"CREATE INDEX {index_name} ON {table_name}(bucket)")
    writer.commit()
    ops += 1

    writer.begin_transaction()
    batch_two = [
        (
            cycle_id * 1_000_000 + rows_per_batch + row_id,
            (cycle_id * 20_000 + row_id) % 50_000,
            (row_id + 5) % 12,
            f"phase-b-{cycle_id}-{row_id}",
        )
        for row_id in range(rows_per_batch)
    ]
    cur.executemany(
        f"INSERT INTO {table_name} (id, customer_id, bucket, note) VALUES (?, ?, ?, ?)",
        batch_two,
    )
    writer.commit()
    ops += len(batch_two)
    rows_written += len(batch_two)

    writer.begin_transaction()
    cur.execute(f"UPDATE {table_name} SET note = ? WHERE bucket = ?", (f"indexed-{cycle_id}", 7))
    writer.commit()
    ops += 1

    cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE bucket = ?", (7,))
    indexed_count = int(cur.fetchone()[0])
    ops += 1
    rows_read += 1

    writer.begin_transaction()
    for rollback_id in range(4):
        cur.execute(
            f"INSERT INTO {table_name} (id, customer_id, bucket, note) VALUES (?, ?, ?, ?)",
            (
                cycle_id * 1_000_000 + 500_000 + rollback_id,
                rollback_id,
                99,
                "rollback-probe",
            ),
        )
    writer.rollback()
    ops += 4

    rows_read += read_probe(db_path, table_name, 7, stmt_cache_size=16)
    rows_read += read_probe(db_path, table_name, 3, stmt_cache_size=0)
    ops += 2

    table_count = len(writer.list_tables())
    index_count = len(writer.list_indexes())
    ddl = writer.get_table_ddl(table_name)
    if not ddl:
        raise RuntimeError(f"expected DDL for {table_name}")
    ops += 3

    writer.begin_transaction()
    cur.execute(f"DELETE FROM {table_name} WHERE id < ?", (cycle_id * 1_000_000 + rows_per_batch // 2,))
    writer.commit()
    ops += 1

    state_payload = storage_state(writer)
    if do_checkpoint:
        writer.checkpoint()
        ops += 1
        state_payload = storage_state(writer)

    writer.begin_transaction()
    cur.execute(f"DROP INDEX {index_name}")
    cur.execute(f"DROP TABLE {table_name}")
    writer.commit()
    ops += 2

    wal_versions, active_readers, wal_file_size_mb = normalize_storage_metrics(state_payload)
    artifact_mb = total_artifact_size_mb(db_path)
    wal_mb_value = wal_size_mb(db_path)

    cur.close()
    writer.close()

    return CycleStats(
        elapsed_ms=(time.perf_counter() - started) * 1000.0,
        ops=ops,
        rows_written=rows_written,
        rows_read=rows_read,
        seed_count=seed_count,
        indexed_count=indexed_count + pre_index_count,
        artifact_mb=artifact_mb,
        wal_mb=wal_mb_value,
        table_count=table_count,
        index_count=index_count,
        wal_versions=wal_versions,
        active_readers=active_readers,
        wal_file_size_mb=wal_file_size_mb,
    )


def engine_worker(db_path: Path, rows_per_batch: int) -> int:
    rss_before = rss_mb()
    open_started = time.perf_counter()
    probe_conn = decentdb.connect(str(db_path), mode="open", stmt_cache_size=32)
    rss_after_open = rss_mb()
    probe_conn.close()
    open_ms = (time.perf_counter() - open_started) * 1000.0

    cycle = run_workload_cycle(
        db_path=db_path,
        cycle_id=1,
        rows_per_batch=rows_per_batch,
        do_checkpoint=True,
    )
    rss_after_cycle = rss_mb()
    gc.collect()
    rss_after_close = rss_mb()

    payload = {
        "rss_before_open_mb": rss_before,
        "rss_after_open_mb": rss_after_open,
        "rss_after_cycle_mb": rss_after_cycle,
        "rss_after_close_mb": rss_after_close,
        "hwm_mb": vmhwm_mb(),
        "open_ms": open_ms,
        "cycle": asdict(cycle),
    }
    print(json.dumps(payload))
    return 0


def build_engine_dashboard(
    samples: list[EngineRunSample],
    progress: Progress,
    phase_started: float,
) -> Group:
    current = samples[-1] if samples else None
    rss_series = [sample.rss_after_close_mb for sample in samples]

    summary = Table.grid(expand=True)
    summary.add_column(justify="left")
    summary.add_column(justify="left")
    summary.add_column(justify="left")
    summary.add_row(
        f"[bold]Runs[/bold] {len(samples)}",
        f"[bold]Elapsed[/bold] {time.perf_counter() - phase_started:.1f}s",
        f"[bold]RSS trend[/bold] {sparkline(rss_series)}",
    )
    if current is not None:
        summary.add_row(
            f"[bold]Current close RSS[/bold] {current.rss_after_close_mb:.2f} MB",
            f"[bold]Current HWM[/bold] {current.hwm_mb:.2f} MB",
            f"[bold]Current ops[/bold] {current.ops}",
        )

    table = Table(title="Fresh-process native lifecycle", expand=True)
    table.add_column("Run", justify="right", style="cyan")
    table.add_column("Close RSS MB", justify="right", style="magenta")
    table.add_column("Cycle ms", justify="right", style="green")
    table.add_column("Open ms", justify="right", style="blue")
    table.add_column("Ops", justify="right", style="bright_cyan")
    table.add_column("Rows R/W", justify="right", style="yellow")
    table.add_column("Artifacts MB", justify="right", style="bright_magenta")

    for sample in samples[-12:]:
        table.add_row(
            str(sample.run),
            f"{sample.rss_after_close_mb:.2f}",
            f"{sample.cycle_ms:.1f}",
            f"{sample.open_ms:.1f}",
            str(sample.ops),
            f"{sample.rows_read}/{sample.rows_written}",
            f"{sample.artifact_mb:.2f}",
        )

    return Group(Panel(summary, border_style="bright_blue"), table, progress)


def run_engine_phase(
    script_path: Path,
    work_dir: Path,
    runs: int,
    seed_rows: int,
    seed_payload_bytes: int,
    rows_per_batch: int,
) -> dict[str, Any]:
    console.print("\n[bold cyan]Phase 1/2: Fresh-process native lifecycle[/bold cyan]")

    template_db = work_dir / "engine-template.ddb"
    build_seed_database(template_db, seed_rows=seed_rows, payload_bytes=seed_payload_bytes)
    samples: list[EngineRunSample] = []
    phase_started = time.perf_counter()

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )

    with Live(console=console, refresh_per_second=8) as live:
        with progress:
            task_id = progress.add_task("Running fresh-process engine cycles", total=runs)
            live.update(build_engine_dashboard(samples, progress, phase_started))
            for run in range(1, runs + 1):
                run_db = work_dir / f"engine-run-{run}.ddb"
                copy_db_artifacts(template_db, run_db)
                proc = subprocess.run(
                    [
                        sys.executable,
                        str(script_path),
                        "--mode",
                        "engine-worker",
                        "--db-path",
                        str(run_db),
                        "--rows-per-batch",
                        str(rows_per_batch),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if proc.returncode != 0:
                    raise RuntimeError(proc.stderr or proc.stdout or f"engine worker failed on run {run}")
                result = json.loads(proc.stdout.strip().splitlines()[-1])
                cycle = result["cycle"]
                samples.append(
                    EngineRunSample(
                        run=run,
                        rss_before_open_mb=float(result["rss_before_open_mb"]),
                        rss_after_open_mb=float(result["rss_after_open_mb"]),
                        rss_after_cycle_mb=float(result["rss_after_cycle_mb"]),
                        rss_after_close_mb=float(result["rss_after_close_mb"]),
                        hwm_mb=float(result["hwm_mb"]),
                        open_ms=float(result["open_ms"]),
                        cycle_ms=float(cycle["elapsed_ms"]),
                        ops=int(cycle["ops"]),
                        rows_written=int(cycle["rows_written"]),
                        rows_read=int(cycle["rows_read"]),
                        artifact_mb=float(cycle["artifact_mb"]),
                        wal_mb=float(cycle["wal_mb"]),
                        wal_versions=cycle["wal_versions"],
                        active_readers=cycle["active_readers"],
                    )
                )
                progress.update(task_id, advance=1)
                live.update(build_engine_dashboard(samples, progress, phase_started))
                remove_db_artifacts(run_db)

    rss_series = [sample.rss_after_close_mb for sample in samples]
    open_series = [sample.rss_after_open_mb for sample in samples]
    cycle_series = [sample.cycle_ms for sample in samples]
    artifact_series = [sample.artifact_mb for sample in samples]

    remove_db_artifacts(template_db)
    return {
        "runs": runs,
        "rss_close_start_mb": rss_series[0] if rss_series else 0.0,
        "rss_close_end_mb": rss_series[-1] if rss_series else 0.0,
        "rss_close_peak_mb": max(rss_series) if rss_series else 0.0,
        "rss_close_drift_mb": rss_series[-1] - rss_series[0] if len(rss_series) > 1 else 0.0,
        "rss_close_slope_mb_per_run": linear_slope(rss_series),
        "rss_open_avg_mb": statistics.mean(open_series) if open_series else 0.0,
        "cycle_ms_avg": statistics.mean(cycle_series) if cycle_series else 0.0,
        "cycle_ms_p95": percentile(cycle_series, 95),
        "artifact_peak_mb": max(artifact_series) if artifact_series else 0.0,
        "samples": [asdict(sample) for sample in samples],
    }


def build_binding_dashboard(
    samples: list[BindingSample],
    progress: Progress,
    phase_started: float,
    current_status: str,
) -> Group:
    current = samples[-1] if samples else None
    rss_series = [sample.rss_mb for sample in samples]

    summary = Table.grid(expand=True)
    summary.add_column(justify="left")
    summary.add_column(justify="left")
    summary.add_column(justify="left")
    summary.add_row(
        f"[bold]Iterations[/bold] {len(samples)}",
        f"[bold]Elapsed[/bold] {time.perf_counter() - phase_started:.1f}s",
        f"[bold]Stage[/bold] {current_status}",
    )
    summary.add_row(
        f"[bold]RSS trend[/bold] {sparkline(rss_series)}",
        f"[bold]Current RSS[/bold] {current.rss_mb:.2f} MB" if current else "[bold]Current RSS[/bold] n/a",
        f"[bold]Current heap[/bold] {current.py_heap_mb:.2f}/{current.py_heap_peak_mb:.2f} MB"
        if current
        else "[bold]Current heap[/bold] n/a",
    )

    table = Table(title="In-process binding churn", expand=True)
    table.add_column("Iter", justify="right", style="cyan")
    table.add_column("RSS MB", justify="right", style="magenta")
    table.add_column("ΔRSS MB", justify="right", style="yellow")
    table.add_column("Heap MB", justify="right", style="green")
    table.add_column("ms", justify="right", style="blue")
    table.add_column("ops/s", justify="right", style="bright_cyan")
    table.add_column("rows/s", justify="right", style="bright_green")
    table.add_column("Artifacts MB", justify="right", style="bright_magenta")
    table.add_column("Status", style="white")

    for sample in samples[-12:]:
        table.add_row(
            str(sample.iteration),
            f"{sample.rss_mb:.2f}",
            f"{sample.rss_delta_mb:+.2f}",
            f"{sample.py_heap_mb:.2f}/{sample.py_heap_peak_mb:.2f}",
            f"{sample.iter_ms:.1f}",
            f"{sample.ops_per_sec:.0f}",
            f"{sample.rows_per_sec:.0f}",
            f"{sample.artifact_mb:.2f}",
            sample.status,
        )

    return Group(Panel(summary, border_style="bright_blue"), table, progress)


def run_binding_phase(
    db_path: Path,
    iterations: int,
    seed_rows: int,
    seed_payload_bytes: int,
    rows_per_batch: int,
    warmup: int,
    checkpoint_interval: int,
) -> dict[str, Any]:
    console.print("\n[bold cyan]Phase 2/2: In-process Python binding churn[/bold cyan]")
    build_seed_database(db_path, seed_rows=seed_rows, payload_bytes=seed_payload_bytes)

    samples: list[BindingSample] = []
    start_rss = rss_mb()
    phase_started = time.perf_counter()
    tracemalloc.start()
    current_status = "starting"

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )

    with Live(console=console, refresh_per_second=8) as live:
        with progress:
            task_id = progress.add_task("Running in-process connection/schema/data churn", total=iterations)
            live.update(build_binding_dashboard(samples, progress, phase_started, current_status))
            for iteration in range(1, iterations + 1):
                cycle = run_workload_cycle(
                    db_path=db_path,
                    cycle_id=iteration,
                    rows_per_batch=rows_per_batch,
                    do_checkpoint=(iteration % checkpoint_interval == 0),
                )
                gc.collect()
                current_heap, peak_heap = tracemalloc.get_traced_memory()
                current_rss = rss_mb()
                current_status = (
                    f"seed={cycle.seed_count} idx={cycle.indexed_count} "
                    f"walv={cycle.wal_versions if cycle.wal_versions is not None else '-'}"
                )
                rows_total = cycle.rows_written + cycle.rows_read
                samples.append(
                    BindingSample(
                        iteration=iteration,
                        rss_mb=current_rss,
                        rss_delta_mb=current_rss - start_rss,
                        hwm_mb=vmhwm_mb(),
                        py_heap_mb=current_heap / MB,
                        py_heap_peak_mb=peak_heap / MB,
                        iter_ms=cycle.elapsed_ms,
                        ops_per_sec=cycle.ops / (cycle.elapsed_ms / 1000.0) if cycle.elapsed_ms > 0 else 0.0,
                        rows_per_sec=rows_total / (cycle.elapsed_ms / 1000.0) if cycle.elapsed_ms > 0 else 0.0,
                        artifact_mb=cycle.artifact_mb,
                        wal_mb=cycle.wal_mb,
                        wal_versions=cycle.wal_versions,
                        active_readers=cycle.active_readers,
                        status=current_status,
                    )
                )
                progress.update(task_id, advance=1)
                live.update(build_binding_dashboard(samples, progress, phase_started, current_status))

    tracemalloc.stop()
    rss_series = [sample.rss_mb for sample in samples]
    iter_series = [sample.iter_ms for sample in samples]
    artifact_series = [sample.artifact_mb for sample in samples]
    tail = rss_series[warmup:] if warmup < len(rss_series) else rss_series

    return {
        "iterations": iterations,
        "warmup": warmup,
        "start_rss_mb": start_rss,
        "final_rss_mb": rss_series[-1] if rss_series else start_rss,
        "peak_rss_mb": max(rss_series) if rss_series else start_rss,
        "tail_drift_mb": tail[-1] - tail[0] if len(tail) > 1 else 0.0,
        "tail_slope_mb_per_iter": linear_slope(tail),
        "iter_ms_avg": statistics.mean(iter_series) if iter_series else 0.0,
        "iter_ms_p95": percentile(iter_series, 95),
        "artifact_peak_mb": max(artifact_series) if artifact_series else 0.0,
        "samples": [asdict(sample) for sample in samples],
    }


def print_summary(
    engine_result: dict[str, Any],
    binding_result: dict[str, Any],
    args: argparse.Namespace,
) -> int:
    engine_pass = (
        engine_result["rss_close_slope_mb_per_run"] <= args.engine_slope_threshold_mb_per_run
        and engine_result["rss_close_drift_mb"] <= args.engine_drift_threshold_mb
    )
    binding_pass = (
        binding_result["tail_slope_mb_per_iter"] <= args.binding_slope_threshold_mb_per_iter
        and binding_result["tail_drift_mb"] <= args.binding_drift_threshold_mb
    )
    overall_pass = engine_pass and binding_pass

    summary = Table(title="DecentDB Memory Leak Verdict", expand=True)
    summary.add_column("Surface", style="cyan")
    summary.add_column("Observed", style="magenta")
    summary.add_column("Threshold", style="yellow")
    summary.add_column("Status", style="green")

    summary.add_row(
        "Fresh-process native lifecycle",
        (
            f"slope={engine_result['rss_close_slope_mb_per_run']:+.4f} MB/run, "
            f"drift={engine_result['rss_close_drift_mb']:+.3f} MB, "
            f"p95={engine_result['cycle_ms_p95']:.1f} ms"
        ),
        (
            f"slope<={args.engine_slope_threshold_mb_per_run:.4f}, "
            f"drift<={args.engine_drift_threshold_mb:.3f}"
        ),
        "PASS" if engine_pass else "FAIL",
    )
    summary.add_row(
        "In-process Python binding churn",
        (
            f"slope={binding_result['tail_slope_mb_per_iter']:+.4f} MB/iter, "
            f"drift={binding_result['tail_drift_mb']:+.3f} MB, "
            f"p95={binding_result['iter_ms_p95']:.1f} ms"
        ),
        (
            f"slope<={args.binding_slope_threshold_mb_per_iter:.4f}, "
            f"drift<={args.binding_drift_threshold_mb:.3f}"
        ),
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
    console.print(
        Panel(
            "PASS" if overall_pass else "FAIL",
            title="Overall",
            border_style="green" if overall_pass else "red",
        )
    )

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(details, indent=2), encoding="utf-8")
        console.print(f"\n[bold]JSON report:[/bold] {args.json_out}")

    return 0 if overall_pass else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DecentDB engine + Python binding memory leak showcase")
    parser.add_argument("--mode", choices=["full", "engine-worker"], default="full")
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--engine-runs", type=int, default=18)
    parser.add_argument("--binding-iterations", type=int, default=120)
    parser.add_argument("--seed-rows", type=int, default=2500)
    parser.add_argument("--seed-payload-bytes", type=int, default=192)
    parser.add_argument("--rows-per-batch", type=int, default=160)
    parser.add_argument("--binding-warmup", type=int, default=15)
    parser.add_argument("--checkpoint-interval", type=int, default=8)
    parser.add_argument("--engine-slope-threshold-mb-per-run", type=float, default=0.030)
    parser.add_argument("--engine-drift-threshold-mb", type=float, default=2.0)
    parser.add_argument("--binding-slope-threshold-mb-per-iter", type=float, default=0.020)
    parser.add_argument("--binding-drift-threshold-mb", type=float, default=4.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.mode == "engine-worker":
        if args.db_path is None:
            raise SystemExit("--db-path is required for --mode engine-worker")
        return engine_worker(args.db_path, rows_per_batch=args.rows_per_batch)

    started = time.perf_counter()
    work_dir = args.work_dir or (Path(tempfile.gettempdir()) / "decentdb-gpt54-memory-leak-tests")
    work_dir.mkdir(parents=True, exist_ok=True)

    console.print(
        Panel(
            (
                "[bold]DecentDB leak showcase[/bold]\n"
                "Phase 1 isolates fresh-process native lifecycle churn.\n"
                "Phase 2 keeps one Python interpreter alive to expose binding leaks."
            ),
            border_style="bright_blue",
        )
    )

    engine_result = run_engine_phase(
        script_path=Path(__file__).resolve(),
        work_dir=work_dir,
        runs=args.engine_runs,
        seed_rows=args.seed_rows,
        seed_payload_bytes=args.seed_payload_bytes,
        rows_per_batch=args.rows_per_batch,
    )

    binding_db = work_dir / "binding-stress.ddb"
    binding_result = run_binding_phase(
        db_path=binding_db,
        iterations=args.binding_iterations,
        seed_rows=args.seed_rows,
        seed_payload_bytes=args.seed_payload_bytes,
        rows_per_batch=args.rows_per_batch,
        warmup=args.binding_warmup,
        checkpoint_interval=args.checkpoint_interval,
    )

    elapsed_s = time.perf_counter() - started
    console.print(f"\n[bold]Elapsed:[/bold] {elapsed_s:.2f}s")

    try:
        return print_summary(engine_result, binding_result, args)
    finally:
        if not args.keep_db:
            shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
