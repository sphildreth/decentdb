#!/usr/bin/env python3
"""
DecentDB Memory Leak Verification Suite (GLM-5.1)

Proves (or disproves) that the DecentDB engine core and Python bindings
have no memory leaks across a broad spectrum of database operations.

Phases:
  1. Connection lifecycle       – open / close under load
  2. Table + index CRUD         – create, populate, index, query, drop
  3. Mixed-type stress          – all SQL types + batch inserts + transactions
  4. Large result set churn     – fetchmany / fetchall over big sets
  5. Schema introspection loop  – list_tables, list_indexes, get_table_ddl …
  6. In-process binding stress  – high-frequency connect/exec/close cycling

Each phase is run for *iterations* rounds.  RSS and Python heap are sampled
every round.  After warmup a linear regression on RSS slope is computed; a
slope above the threshold flags a leak.
"""

from __future__ import annotations

import argparse
import gc
import os
import shutil
import statistics
import sys
import tempfile
import time
import tracemalloc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

import decentdb
from rich.console import Console
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
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

console = Console()


@dataclass
class Sample:
    iteration: int
    rss_mb: float
    rss_delta_mb: float
    heap_mb: float
    heap_peak_mb: float
    iter_ms: float
    ops_per_sec: float
    phase_label: str


@dataclass
class PhaseResult:
    label: str
    iterations: int
    start_rss_mb: float
    final_rss_mb: float
    peak_rss_mb: float
    tail_drift_mb: float
    tail_slope_mb_per_iter: float
    avg_iter_ms: float
    p95_iter_ms: float
    total_ops: int
    verdict: str = ""


def rss_mb() -> float:
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return 0.0


def vmhwm_mb() -> float:
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return 0.0


def db_artifact_paths(db_path: str | Path) -> list[Path]:
    p = str(db_path)
    return [Path(p + s) for s in ("", ".wal", ".shm", "-wal", "-shm", "-lock")]


def remove_db_artifacts(db_path: str | Path) -> None:
    for p in db_artifact_paths(db_path):
        try:
            p.unlink()
        except OSError:
            pass


def total_artifact_size_mb(db_path: str | Path) -> float:
    return sum(
        p.stat().st_size for p in db_artifact_paths(db_path) if p.exists()
    ) / (1024.0 * 1024.0)


def linear_slope(values: Sequence[float]) -> float:
    if len(values) < 4:
        return 0.0
    n = len(values)
    x_mean = (n - 1) / 2.0
    y_mean = statistics.mean(values)
    denom = sum((i - x_mean) ** 2 for i in range(n))
    if denom == 0.0:
        return 0.0
    numer = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    return numer / denom


def percentiles(data: Sequence[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(s):
        return s[-1]
    return s[f] + (k - f) * (s[c] - s[f])


def finalize_phase(
    label: str,
    history: list[Sample],
    start_rss: float,
    total_ops: int,
    warmup: int,
) -> PhaseResult:
    rss_series = [s.rss_mb for s in history]
    ms_series = [s.iter_ms for s in history]
    tail = rss_series[warmup:] if warmup < len(rss_series) else rss_series
    return PhaseResult(
        label=label,
        iterations=len(history),
        start_rss_mb=start_rss,
        final_rss_mb=rss_series[-1] if rss_series else start_rss,
        peak_rss_mb=max(rss_series) if rss_series else start_rss,
        tail_drift_mb=tail[-1] - tail[0] if len(tail) > 1 else 0.0,
        tail_slope_mb_per_iter=linear_slope(tail),
        avg_iter_ms=statistics.mean(ms_series) if ms_series else 0.0,
        p95_iter_ms=percentiles(ms_series, 95),
        total_ops=total_ops,
    )


def build_dashboard(
    history: list[Sample],
    start_rss: float,
    phase_label: str,
    phase_started: float,
    total_phases: int,
    current_phase_idx: int,
) -> Panel:
    tbl = Table(
        title=f"Phase {current_phase_idx}/{total_phases}: {phase_label}",
        expand=True,
        show_lines=False,
    )
    tbl.add_column("Iter", justify="right", style="cyan", width=6)
    tbl.add_column("RSS MB", justify="right", style="magenta", width=10)
    tbl.add_column("\u0394RSS MB", justify="right", style="yellow", width=10)
    tbl.add_column("Heap MB", justify="right", style="green", width=16)
    tbl.add_column("ms/iter", justify="right", style="blue", width=9)
    tbl.add_column("ops/s", justify="right", style="bright_cyan", width=10)

    for s in history[-12:]:
        tbl.add_row(
            str(s.iteration),
            f"{s.rss_mb:.2f}",
            f"{s.rss_delta_mb:+.2f}",
            f"{s.heap_mb:.2f}/{s.heap_peak_mb:.2f}",
            f"{s.iter_ms:.1f}",
            f"{s.ops_per_sec:.0f}",
        )

    elapsed = time.perf_counter() - phase_started
    recent = [s.iter_ms for s in history[-20:]]
    avg = statistics.mean(recent) if recent else 0.0
    hdr = (
        f"elapsed={elapsed:.1f}s | start_rss={start_rss:.2f} MB | "
        f"recent_avg={avg:.1f} ms"
    )
    return Panel(tbl, title=hdr, border_style="bright_blue")


def sample_now(
    iteration: int,
    start_rss: float,
    iter_ms: float,
    ops_in_iter: int,
    phase_label: str,
) -> Sample:
    cur_rss = rss_mb()
    cur_heap, peak_heap = tracemalloc.get_traced_memory()
    return Sample(
        iteration=iteration,
        rss_mb=cur_rss,
        rss_delta_mb=cur_rss - start_rss,
        heap_mb=cur_heap / (1024 * 1024),
        heap_peak_mb=peak_heap / (1024 * 1024),
        iter_ms=iter_ms,
        ops_per_sec=ops_in_iter / (iter_ms / 1000.0) if iter_ms > 0 else 0.0,
        phase_label=phase_label,
    )


PHASE_CONNECTION = "Connection Lifecycle"
PHASE_TABLE_INDEX = "Table + Index CRUD"
PHASE_MIXED_TYPES = "Mixed-Type Stress"
PHASE_LARGE_RS = "Large Result Set Churn"
PHASE_INTROSPECTION = "Schema Introspection"
PHASE_BINDING = "Binding Stress"

ALL_PHASES = [
    PHASE_CONNECTION,
    PHASE_TABLE_INDEX,
    PHASE_MIXED_TYPES,
    PHASE_LARGE_RS,
    PHASE_INTROSPECTION,
    PHASE_BINDING,
]

OPS_PER_PHASE: dict[str, int] = {}


def run_phase_connection_lifecycle(
    db_path: str, iterations: int
) -> tuple[list[Sample], float, int]:
    remove_db_artifacts(db_path)
    start_rss = rss_mb()
    history: list[Sample] = []
    total_ops = 0

    seed_conn = decentdb.connect(db_path, mode="create")
    seed_conn.execute("CREATE TABLE conn_test (id INT64 PRIMARY KEY, val TEXT)")
    seed_conn.execute("INSERT INTO conn_test VALUES (1, 'seed')")
    seed_conn.commit()
    seed_conn.close()

    ops_per = 4
    OPS_PER_PHASE[PHASE_CONNECTION] = ops_per

    for i in range(1, iterations + 1):
        t0 = time.perf_counter()
        conn = decentdb.connect(db_path, mode="open", stmt_cache_size=0)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM conn_test")
        cur.fetchone()
        cur.execute("INSERT INTO conn_test VALUES (?, ?)", (i + 1000, f"v_{i}"))
        conn.commit()
        cur.execute("SELECT * FROM conn_test ORDER BY id")
        cur.fetchall()
        cur.close()
        conn.close()
        gc.collect()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        total_ops += ops_per
        history.append(sample_now(i, start_rss, elapsed_ms, ops_per, PHASE_CONNECTION))

    remove_db_artifacts(db_path)
    return history, start_rss, total_ops


def run_phase_table_index_crud(
    db_path: str, iterations: int, rows_per_iter: int
) -> tuple[list[Sample], float, int]:
    remove_db_artifacts(db_path)
    start_rss = rss_mb()
    history: list[Sample] = []
    total_ops = 0

    ops_per = rows_per_iter * 2 + 8
    OPS_PER_PHASE[PHASE_TABLE_INDEX] = ops_per

    for i in range(1, iterations + 1):
        t0 = time.perf_counter()
        conn = decentdb.connect(db_path, mode="open_or_create", stmt_cache_size=0)
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS tbl_crud")
        cur.execute(
            "CREATE TABLE tbl_crud (id INT64 PRIMARY KEY, name TEXT, score FLOAT64)"
        )

        batch = [(j, f"user_{i}_{j}", j * 1.5) for j in range(rows_per_iter)]
        cur.executemany(
            "INSERT INTO tbl_crud (id, name, score) VALUES (?, ?, ?)", batch
        )
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM tbl_crud WHERE score > ?", (50.0,))
        cur.fetchone()

        cur.execute("CREATE INDEX idx_crud_name ON tbl_crud(name)")
        conn.commit()
        cur.close()
        cur = conn.cursor()

        batch2 = [
            (rows_per_iter + j, f"idx_user_{i}_{j}", j * 2.5)
            for j in range(rows_per_iter)
        ]
        cur.executemany(
            "INSERT INTO tbl_crud (id, name, score) VALUES (?, ?, ?)", batch2
        )
        conn.commit()

        cur.execute(
            "SELECT id, name FROM tbl_crud WHERE name = ?", (f"idx_user_{i}_7",)
        )
        cur.fetchall()

        cur.execute("DELETE FROM tbl_crud WHERE id < ?", (rows_per_iter // 2,))
        conn.commit()

        cur.execute("DROP INDEX idx_crud_name")
        conn.commit()

        cur.execute("UPDATE tbl_crud SET score = score + 1.0")
        conn.commit()

        cur.execute("DROP TABLE tbl_crud")
        conn.commit()

        cur.close()
        conn.close()
        gc.collect()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        total_ops += ops_per
        history.append(sample_now(i, start_rss, elapsed_ms, ops_per, PHASE_TABLE_INDEX))

    remove_db_artifacts(db_path)
    return history, start_rss, total_ops


def run_phase_mixed_types(
    db_path: str, iterations: int
) -> tuple[list[Sample], float, int]:
    import decimal
    import datetime

    remove_db_artifacts(db_path)
    start_rss = rss_mb()
    history: list[Sample] = []
    total_ops = 0

    ops_per = 14
    OPS_PER_PHASE[PHASE_MIXED_TYPES] = ops_per

    for i in range(1, iterations + 1):
        t0 = time.perf_counter()
        conn = decentdb.connect(db_path, mode="open_or_create", stmt_cache_size=0)
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS tbl_mixed")
        cur.execute(
            "CREATE TABLE tbl_mixed ("
            "  id INT64 PRIMARY KEY, "
            "  label TEXT, "
            "  score FLOAT64, "
            "  flag BOOL, "
            "  amount DECIMAL(12,2), "
            "  data BLOB, "
            "  created TIMESTAMP"
            ")"
        )

        cur.execute("BEGIN")
        for j in range(5):
            cur.execute(
                "INSERT INTO tbl_mixed (id, label, score, flag, amount, data, created) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                (
                    j + i * 1000,
                    f"text_{i}_{j}",
                    3.14 * j,
                    j % 2 == 0,
                    decimal.Decimal(f"{j * 99.99:.2f}"),
                    bytes(range(j, j + 64)),
                    datetime.datetime(2026, 1, 1, 0, j, 0),
                ),
            )
        conn.commit()

        cur.execute("SELECT * FROM tbl_mixed WHERE flag = ?", (True,))
        cur.fetchall()

        cur.execute(
            "UPDATE tbl_mixed SET score = score * 2.0 WHERE id > ?", (i * 1000,)
        )
        conn.commit()

        cur.execute("SELECT AVG(score), SUM(amount) FROM tbl_mixed")
        cur.fetchone()

        cur.execute("DROP TABLE tbl_mixed")
        conn.commit()

        cur.close()
        conn.close()
        gc.collect()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        total_ops += ops_per
        history.append(sample_now(i, start_rss, elapsed_ms, ops_per, PHASE_MIXED_TYPES))

    remove_db_artifacts(db_path)
    return history, start_rss, total_ops


def run_phase_large_result_sets(
    db_path: str, iterations: int, rows_per_iter: int
) -> tuple[list[Sample], float, int]:
    remove_db_artifacts(db_path)
    start_rss = rss_mb()
    history: list[Sample] = []
    total_ops = 0

    ops_per = rows_per_iter * 2 + 4
    OPS_PER_PHASE[PHASE_LARGE_RS] = ops_per

    conn = decentdb.connect(db_path, mode="create", stmt_cache_size=0)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE tbl_large (id INT64 PRIMARY KEY, payload TEXT, weight FLOAT64)"
    )
    cur.execute("CREATE INDEX idx_large_weight ON tbl_large(weight)")
    conn.commit()
    cur.close()
    conn.close()

    for i in range(1, iterations + 1):
        t0 = time.perf_counter()
        conn = decentdb.connect(db_path, mode="open", stmt_cache_size=0)
        cur = conn.cursor()

        cur.execute("BEGIN")
        batch = [
            (i * rows_per_iter + j, f"row_{i}_{j}" * 4, float(j) * 0.37)
            for j in range(rows_per_iter)
        ]
        cur.executemany(
            "INSERT INTO tbl_large (id, payload, weight) VALUES (?, ?, ?)", batch
        )
        conn.commit()

        cur.execute("SELECT * FROM tbl_large WHERE weight > ?", (100.0,))
        rows = cur.fetchmany(500)
        while rows:
            rows = cur.fetchmany(500)

        cur.execute("SELECT COUNT(*), AVG(weight) FROM tbl_large")
        cur.fetchone()

        cur.execute("DELETE FROM tbl_large WHERE id < ?", ((i - 3) * rows_per_iter if i > 3 else 0))
        conn.commit()

        cur.close()
        conn.close()
        gc.collect()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        total_ops += ops_per
        history.append(sample_now(i, start_rss, elapsed_ms, ops_per, PHASE_LARGE_RS))

    remove_db_artifacts(db_path)
    return history, start_rss, total_ops


def run_phase_introspection(
    db_path: str, iterations: int
) -> tuple[list[Sample], float, int]:
    remove_db_artifacts(db_path)
    start_rss = rss_mb()
    history: list[Sample] = []
    total_ops = 0

    conn = decentdb.connect(db_path, mode="create", stmt_cache_size=64)
    for t in range(5):
        conn.execute(
            f"CREATE TABLE intro_t{t} (id INT64 PRIMARY KEY, val TEXT, weight FLOAT64)"
        )
        conn.execute(f"CREATE INDEX idx_intro_t{t}_val ON intro_t{t}(val)")
    conn.commit()
    conn.close()

    ops_per = 22
    OPS_PER_PHASE[PHASE_INTROSPECTION] = ops_per

    for i in range(1, iterations + 1):
        t0 = time.perf_counter()
        conn = decentdb.connect(db_path, mode="open", stmt_cache_size=64)

        tables = conn.list_tables()
        for t in tables:
            cols = conn.get_table_columns(t)
            _ = conn.get_table_ddl(t)
        indexes = conn.list_indexes()
        state = conn.inspect_storage_state()

        conn.checkpoint()

        conn.close()
        gc.collect()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        total_ops += ops_per
        history.append(
            sample_now(i, start_rss, elapsed_ms, ops_per, PHASE_INTROSPECTION)
        )

    remove_db_artifacts(db_path)
    return history, start_rss, total_ops


def run_phase_binding_stress(
    db_path: str, iterations: int, rows_per_iter: int
) -> tuple[list[Sample], float, int]:
    remove_db_artifacts(db_path)
    start_rss = rss_mb()
    history: list[Sample] = []
    total_ops = 0

    ops_per = rows_per_iter * 2 + 12
    OPS_PER_PHASE[PHASE_BINDING] = ops_per

    for i in range(1, iterations + 1):
        t0 = time.perf_counter()

        with decentdb.connect(db_path, mode="open_or_create", stmt_cache_size=0) as conn:
            cur = conn.cursor()

            cur.execute("DROP TABLE IF EXISTS bind_stress")
            cur.execute(
                "CREATE TABLE bind_stress "
                "(id INT64 PRIMARY KEY, category TEXT, payload TEXT)"
            )

            batch = [
                (j, f"cat_{j % 20}", f"payload_{i}_{j}" * 3)
                for j in range(rows_per_iter)
            ]
            cur.executemany(
                "INSERT INTO bind_stress (id, category, payload) VALUES (?, ?, ?)",
                batch,
            )
            conn.commit()

            cur.execute(
                "CREATE INDEX idx_bind_cat ON bind_stress(category)"
            )
            conn.commit()
            cur.close()
            cur = conn.cursor()

            batch2 = [
                (rows_per_iter + j, f"cat_{j % 20}", f"more_{i}_{j}" * 3)
                for j in range(rows_per_iter)
            ]
            cur.executemany(
                "INSERT INTO bind_stress (id, category, payload) VALUES (?, ?, ?)",
                batch2,
            )
            conn.commit()

            cur.execute(
                "SELECT COUNT(*) FROM bind_stress WHERE category = ?", ("cat_7",)
            )
            cur.fetchone()

            cur.execute(
                "DELETE FROM bind_stress WHERE id < ?", (rows_per_iter // 4,)
            )
            conn.commit()

            cur.execute("DROP INDEX idx_bind_cat")
            conn.commit()

            cur.execute("DROP TABLE bind_stress")
            conn.commit()

            cur.close()

        gc.collect()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        total_ops += ops_per
        history.append(sample_now(i, start_rss, elapsed_ms, ops_per, PHASE_BINDING))

    remove_db_artifacts(db_path)
    return history, start_rss, total_ops


def print_header() -> None:
    console.print()
    console.rule("[bold bright_white]DecentDB Memory Leak Verification Suite[/bold bright_white]")
    console.print(
        f"  Engine  : [cyan]{decentdb.engine_version()}[/cyan]   "
        f"ABI : [cyan]{decentdb.abi_version()}[/cyan]   "
        f"Python : [cyan]{sys.version.split()[0]}[/cyan]"
    )
    console.print(
        f"  RSS at start : [magenta]{rss_mb():.2f} MB[/magenta]   "
        f"HWM : [magenta]{vmhwm_mb():.2f} MB[/magenta]"
    )
    console.print()


def print_verdict(results: list[PhaseResult], args: argparse.Namespace) -> int:
    console.print()
    console.rule("[bold bright_white]Verdict[/bold bright_white]")

    tbl = Table(title="Phase Results", expand=True, show_lines=True)
    tbl.add_column("Phase", style="cyan", min_width=26)
    tbl.add_column("Iters", justify="right", style="white")
    tbl.add_column("Total Ops", justify="right", style="white")
    tbl.add_column("Avg ms", justify="right", style="blue")
    tbl.add_column("P95 ms", justify="right", style="blue")
    tbl.add_column("Slope MB/iter", justify="right", style="yellow")
    tbl.add_column("Drift MB", justify="right", style="yellow")
    tbl.add_column("Status", style="bold")

    all_pass = True
    for r in results:
        ok = abs(r.tail_slope_mb_per_iter) <= args.slope_threshold and abs(
            r.tail_drift_mb
        ) <= args.drift_threshold
        if not ok:
            all_pass = False
        r.verdict = "PASS" if ok else "FAIL"
        status_style = "[bold green]PASS[/bold green]" if ok else "[bold red]FAIL[/bold red]"
        tbl.add_row(
            r.label,
            str(r.iterations),
            f"{r.total_ops:,}",
            f"{r.avg_iter_ms:.1f}",
            f"{r.p95_iter_ms:.1f}",
            f"{r.tail_slope_mb_per_iter:+.5f}",
            f"{r.tail_drift_mb:+.3f}",
            status_style,
        )

    console.print(tbl)

    slope_note = (
        f"Slope threshold: |slope| <= {args.slope_threshold} MB/iter   "
        f"Drift threshold: |drift| <= {args.drift_threshold} MB"
    )
    console.print(f"  [dim]{slope_note}[/dim]")

    panel_text = Text()
    if all_pass:
        panel_text.append("PASS", style="bold green")
        panel_text.append(" — No evidence of memory leaks across any phase.")
        border = "green"
    else:
        failed = [r.label for r in results if r.verdict == "FAIL"]
        panel_text.append("FAIL", style="bold red")
        panel_text.append(f" — Potential leak detected in: {', '.join(failed)}")
        border = "red"

    console.print()
    console.print(Panel(panel_text, title="Overall Verdict", border_style=border))
    return 0 if all_pass else 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description="DecentDB engine + Python binding memory leak verification"
    )
    parser.add_argument(
        "--iterations", type=int, default=150, help="Iterations per phase"
    )
    parser.add_argument(
        "--rows", type=int, default=200, help="Rows per insert batch"
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=20,
        help="Warmup iterations to skip in slope calculation",
    )
    parser.add_argument(
        "--slope-threshold",
        type=float,
        default=0.010,
        help="Max acceptable RSS slope (MB/iter)",
    )
    parser.add_argument(
        "--drift-threshold",
        type=float,
        default=4.0,
        help="Max acceptable RSS drift (MB) after warmup",
    )
    parser.add_argument("--db-path", type=str, default=None)
    args = parser.parse_args()

    print_header()

    tmp_dir = Path(tempfile.gettempdir()) / "decentdb-glm51-leak-test"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    db_base = args.db_path or str(tmp_dir / "test.ddb")

    tracemalloc.start()
    global_start = time.perf_counter()

    phase_runners = [
        (
            PHASE_CONNECTION,
            lambda: run_phase_connection_lifecycle(
                str(tmp_dir / "conn.ddb"), args.iterations
            ),
        ),
        (
            PHASE_TABLE_INDEX,
            lambda: run_phase_table_index_crud(
                str(tmp_dir / "table_index.ddb"), args.iterations, args.rows
            ),
        ),
        (
            PHASE_MIXED_TYPES,
            lambda: run_phase_mixed_types(
                str(tmp_dir / "mixed.ddb"), args.iterations
            ),
        ),
        (
            PHASE_LARGE_RS,
            lambda: run_phase_large_result_sets(
                str(tmp_dir / "large.ddb"), args.iterations, args.rows
            ),
        ),
        (
            PHASE_INTROSPECTION,
            lambda: run_phase_introspection(
                str(tmp_dir / "intro.ddb"), args.iterations
            ),
        ),
        (
            PHASE_BINDING,
            lambda: run_phase_binding_stress(
                str(tmp_dir / "binding.ddb"), args.iterations, args.rows
            ),
        ),
    ]

    results: list[PhaseResult] = []

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=True,
    )

    with Live(console=console, refresh_per_second=8, transient=True) as live:
        with progress:
            overall_task = progress.add_task(
                "All phases", total=len(phase_runners)
            )

            for phase_idx, (phase_label, runner) in enumerate(phase_runners, 1):
                phase_started = time.perf_counter()
                phase_start_rss = rss_mb()

                phase_task = progress.add_task(
                    f"[cyan]{phase_label}[/cyan]", total=args.iterations
                )

                history: list[Sample] = []
                start_rss = 0.0
                total_ops = 0

                samples_raw: list[Sample] = []

                _i = 0

                original_runner = runner

                for i in range(1, args.iterations + 1):
                    t0 = time.perf_counter()
                    if phase_label == PHASE_CONNECTION:
                        hist, sr, ops = run_phase_connection_lifecycle_single(
                            str(tmp_dir / "conn.ddb"), i, args.iterations
                        )
                    elif phase_label == PHASE_TABLE_INDEX:
                        hist, sr, ops = run_phase_table_index_single(
                            str(tmp_dir / "table_index.ddb"), i, args.rows
                        )
                    elif phase_label == PHASE_MIXED_TYPES:
                        hist, sr, ops = run_phase_mixed_types_single(
                            str(tmp_dir / "mixed.ddb"), i
                        )
                    elif phase_label == PHASE_LARGE_RS:
                        hist, sr, ops = run_phase_large_rs_single(
                            str(tmp_dir / "large.ddb"), i, args.rows
                        )
                    elif phase_label == PHASE_INTROSPECTION:
                        hist, sr, ops = run_phase_introspection_single(
                            str(tmp_dir / "intro.ddb"), i
                        )
                    else:
                        hist, sr, ops = run_phase_binding_single(
                            str(tmp_dir / "binding.ddb"), i, args.rows
                        )
                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    if i == 1:
                        start_rss = sr
                    total_ops += ops
                    samples_raw.append(hist)
                    progress.update(phase_task, advance=1)
                    if i % 3 == 0 or i == args.iterations:
                        live.update(
                            build_dashboard(
                                samples_raw,
                                start_rss,
                                phase_label,
                                phase_started,
                                len(phase_runners),
                                phase_idx,
                            )
                        )

                result = finalize_phase(
                    phase_label, samples_raw, start_rss, total_ops, args.warmup
                )
                results.append(result)

                progress.remove_task(phase_task)
                progress.update(overall_task, advance=1)

                cleanup_db = str(
                    tmp_dir
                    / f"{phase_label.split()[0].lower()}.ddb"
                )
                remove_db_artifacts(cleanup_db)

    tracemalloc.stop()

    elapsed_total = time.perf_counter() - global_start

    console.print()
    console.print(
        f"  [bold]Total time:[/bold] {elapsed_total:.1f}s   "
        f"[bold]Final RSS:[/bold] {rss_mb():.2f} MB   "
        f"[bold]HWM:[/bold] {vmhwm_mb():.2f} MB"
    )

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return print_verdict(results, args)


def run_phase_connection_lifecycle_single(
    db_path: str, iteration: int, total_iters: int
) -> tuple[Sample, float, int]:
    if iteration == 1:
        remove_db_artifacts(db_path)
        c = decentdb.connect(db_path, mode="create")
        c.execute("CREATE TABLE conn_test (id INT64 PRIMARY KEY, val TEXT)")
        c.execute("INSERT INTO conn_test VALUES (1, 'seed')")
        c.commit()
        c.close()

    ops = 4
    t0 = time.perf_counter()
    start_rss = rss_mb()

    conn = decentdb.connect(db_path, mode="open", stmt_cache_size=0)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM conn_test")
    cur.fetchone()
    cur.execute("INSERT INTO conn_test VALUES (?, ?)", (iteration + 10000, f"v_{iteration}"))
    conn.commit()
    cur.execute("SELECT * FROM conn_test ORDER BY id")
    cur.fetchall()
    cur.close()
    conn.close()
    gc.collect()

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return sample_now(iteration, start_rss, elapsed_ms, ops, PHASE_CONNECTION), start_rss, ops


def run_phase_table_index_single(
    db_path: str, iteration: int, rows: int
) -> tuple[Sample, float, int]:
    ops = rows * 2 + 8
    t0 = time.perf_counter()
    start_rss = rss_mb()

    conn = decentdb.connect(db_path, mode="open_or_create", stmt_cache_size=0)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS tbl_crud")
    cur.execute("CREATE TABLE tbl_crud (id INT64 PRIMARY KEY, name TEXT, score FLOAT64)")
    batch = [(j, f"user_{iteration}_{j}", j * 1.5) for j in range(rows)]
    cur.executemany("INSERT INTO tbl_crud (id, name, score) VALUES (?, ?, ?)", batch)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM tbl_crud WHERE score > ?", (50.0,))
    cur.fetchone()
    cur.execute("CREATE INDEX idx_crud_name ON tbl_crud(name)")
    conn.commit()
    cur.close()
    cur = conn.cursor()
    batch2 = [(rows + j, f"idx_{iteration}_{j}", j * 2.5) for j in range(rows)]
    cur.executemany("INSERT INTO tbl_crud (id, name, score) VALUES (?, ?, ?)", batch2)
    conn.commit()
    cur.execute("SELECT id, name FROM tbl_crud WHERE name = ?", (f"idx_{iteration}_7",))
    cur.fetchall()
    cur.execute("DELETE FROM tbl_crud WHERE id < ?", (rows // 2,))
    conn.commit()
    cur.execute("DROP INDEX idx_crud_name")
    conn.commit()
    cur.execute("UPDATE tbl_crud SET score = score + 1.0")
    conn.commit()
    cur.execute("DROP TABLE tbl_crud")
    conn.commit()
    cur.close()
    conn.close()
    gc.collect()

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return sample_now(iteration, start_rss, elapsed_ms, ops, PHASE_TABLE_INDEX), start_rss, ops


def run_phase_mixed_types_single(
    db_path: str, iteration: int
) -> tuple[Sample, float, int]:
    import decimal
    import datetime

    ops = 14
    t0 = time.perf_counter()
    start_rss = rss_mb()

    conn = decentdb.connect(db_path, mode="open_or_create", stmt_cache_size=0)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS tbl_mixed")
    cur.execute(
        "CREATE TABLE tbl_mixed ("
        "id INT64 PRIMARY KEY, label TEXT, score FLOAT64, flag BOOL, "
        "amount DECIMAL(12,2), data BLOB, created TIMESTAMP)"
    )
    cur.execute("BEGIN")
    for j in range(5):
        cur.execute(
            "INSERT INTO tbl_mixed (id, label, score, flag, amount, data, created) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            (
                j + iteration * 1000,
                f"text_{iteration}_{j}",
                3.14 * j,
                j % 2 == 0,
                decimal.Decimal(f"{j * 99.99:.2f}"),
                bytes(range(j, j + 64)),
                datetime.datetime(2026, 1, 1, 0, j, 0),
            ),
        )
    conn.commit()
    cur.execute("SELECT * FROM tbl_mixed WHERE flag = ?", (True,))
    cur.fetchall()
    cur.execute("UPDATE tbl_mixed SET score = score * 2.0 WHERE id > ?", (iteration * 1000,))
    conn.commit()
    cur.execute("SELECT AVG(score), SUM(amount) FROM tbl_mixed")
    cur.fetchone()
    cur.execute("DROP TABLE tbl_mixed")
    conn.commit()
    cur.close()
    conn.close()
    gc.collect()

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return sample_now(iteration, start_rss, elapsed_ms, ops, PHASE_MIXED_TYPES), start_rss, ops


def run_phase_large_rs_single(
    db_path: str, iteration: int, rows: int
) -> tuple[Sample, float, int]:
    ops = rows * 2 + 4
    t0 = time.perf_counter()
    start_rss = rss_mb()

    if iteration == 1:
        remove_db_artifacts(db_path)
        c = decentdb.connect(db_path, mode="create")
        c.execute("CREATE TABLE tbl_large (id INT64 PRIMARY KEY, payload TEXT, weight FLOAT64)")
        c.execute("CREATE INDEX idx_large_weight ON tbl_large(weight)")
        c.commit()
        c.close()

    conn = decentdb.connect(db_path, mode="open", stmt_cache_size=0)
    cur = conn.cursor()
    cur.execute("BEGIN")
    batch = [(iteration * rows + j, f"row_{iteration}_{j}" * 4, float(j) * 0.37) for j in range(rows)]
    cur.executemany("INSERT INTO tbl_large (id, payload, weight) VALUES (?, ?, ?)", batch)
    conn.commit()
    cur.execute("SELECT * FROM tbl_large WHERE weight > ?", (100.0,))
    chunk = cur.fetchmany(500)
    while chunk:
        chunk = cur.fetchmany(500)
    cur.execute("SELECT COUNT(*), AVG(weight) FROM tbl_large")
    cur.fetchone()
    cutoff = (iteration - 3) * rows if iteration > 3 else 0
    cur.execute("DELETE FROM tbl_large WHERE id < ?", (cutoff,))
    conn.commit()
    cur.close()
    conn.close()
    gc.collect()

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return sample_now(iteration, start_rss, elapsed_ms, ops, PHASE_LARGE_RS), start_rss, ops


def run_phase_introspection_single(
    db_path: str, iteration: int
) -> tuple[Sample, float, int]:
    if iteration == 1:
        remove_db_artifacts(db_path)
        c = decentdb.connect(db_path, mode="create")
        for t in range(5):
            c.execute(f"CREATE TABLE intro_t{t} (id INT64 PRIMARY KEY, val TEXT, weight FLOAT64)")
            c.execute(f"CREATE INDEX idx_intro_t{t}_val ON intro_t{t}(val)")
        c.commit()
        c.close()

    ops = 22
    t0 = time.perf_counter()
    start_rss = rss_mb()

    conn = decentdb.connect(db_path, mode="open", stmt_cache_size=64)
    tables = conn.list_tables()
    for t in tables:
        conn.get_table_columns(t)
        conn.get_table_ddl(t)
    conn.list_indexes()
    conn.inspect_storage_state()
    conn.checkpoint()
    conn.close()
    gc.collect()

    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return sample_now(iteration, start_rss, elapsed_ms, ops, PHASE_INTROSPECTION), start_rss, ops


def run_phase_binding_single(
    db_path: str, iteration: int, rows: int
) -> tuple[Sample, float, int]:
    ops = rows * 2 + 12
    t0 = time.perf_counter()
    start_rss = rss_mb()

    with decentdb.connect(db_path, mode="open_or_create", stmt_cache_size=0) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS bind_stress")
        cur.execute("CREATE TABLE bind_stress (id INT64 PRIMARY KEY, category TEXT, payload TEXT)")
        batch = [(j, f"cat_{j % 20}", f"payload_{iteration}_{j}" * 3) for j in range(rows)]
        cur.executemany("INSERT INTO bind_stress (id, category, payload) VALUES (?, ?, ?)", batch)
        conn.commit()
        cur.execute("CREATE INDEX idx_bind_cat ON bind_stress(category)")
        conn.commit()
        cur.close()
        cur = conn.cursor()
        batch2 = [(rows + j, f"cat_{j % 20}", f"more_{iteration}_{j}" * 3) for j in range(rows)]
        cur.executemany("INSERT INTO bind_stress (id, category, payload) VALUES (?, ?, ?)", batch2)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM bind_stress WHERE category = ?", ("cat_7",))
        cur.fetchone()
        cur.execute("DELETE FROM bind_stress WHERE id < ?", (rows // 4,))
        conn.commit()
        cur.execute("DROP INDEX idx_bind_cat")
        conn.commit()
        cur.execute("DROP TABLE bind_stress")
        conn.commit()
        cur.close()

    gc.collect()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return sample_now(iteration, start_rss, elapsed_ms, ops, PHASE_BINDING), start_rss, ops


if __name__ == "__main__":
    sys.exit(main())
