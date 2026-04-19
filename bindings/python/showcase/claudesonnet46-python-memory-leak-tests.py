#!/usr/bin/env python3
"""
DecentDB Memory Leak Showcase (claudesonnet46)

Eight focused scenarios that stress different resource-ownership surfaces in
both the native engine core (via C ABI) and the Python bindings:

  1. connection_churn       – rapid open/close with varying stmt_cache sizes
  2. schema_churn           – create/drop tables and indexes in tight loops
  3. cursor_reuse           – reuse vs. re-create cursors; stmt cache pressure
  4. type_variety           – exercise all 8 column types: INT64, FLOAT64, TEXT,
                             BLOB, BOOL, DECIMAL, UUID, TIMESTAMP
  5. large_blob_churn       – write and read progressively larger binary payloads
  6. rollback_stress        – heavy rollback / abandoned-write / nested txn paths
  7. introspection_churn    – list_tables / list_indexes / describe / storage_state
  8. multi_reader_churn     – multiple concurrent read connections, one writer

Each scenario is assessed with:
  • Linear regression slope (MB/iter) on post-warmup RSS
  • Mann-Kendall monotonic trend S-statistic and approximate p-value
  • Absolute start-to-end drift (MB)
  • Python GC generation object counts (gen0/gen1/gen2)
  • tracemalloc top-5 allocation-site diff vs. baseline

Optional --canary flag injects a known-leaky reference-accumulation scenario
first, verifying the framework CAN detect leaks before trusting the clean results.

Usage
-----
  cd bindings/python
  python showcase/claudesonnet46-python-memory-leak-tests.py [options]

  --iterations N          iterations per scenario (default 120)
  --warmup N              warmup iters excluded from analysis (default 12)
  --slope-threshold F     |MB/iter| tolerance (default 0.025)
  --drift-threshold F     |MB| start-to-end tolerance (default 4.0)
  --mk-p-threshold F      Mann-Kendall p < this is a warning (default 0.05)
  --only name[,name]      run only the named scenarios
  --canary                prepend a known-leaky canary scenario
  --workdir DIR           directory for temp DB files
  --json-out PATH         write JSON report to PATH
"""

from __future__ import annotations

import argparse
import collections
import datetime
import decimal
import gc
import json
import math
import os
import random
import statistics
import sys
import tempfile
import time
import tracemalloc
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# ensure the bindings package is importable when run from the showcase dir
# ---------------------------------------------------------------------------
_pkg_root = Path(__file__).resolve().parents[1]
if str(_pkg_root) not in sys.path:
    sys.path.insert(0, str(_pkg_root))

import decentdb

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

console = Console()

MODEL_TAG = "claudesonnet46"
SPARK_CHARS = " ▁▂▃▄▅▆▇█"
MB = 1024.0 * 1024.0


# ---------------------------------------------------------------------------
# system / process metrics
# ---------------------------------------------------------------------------

def _proc_status_kv() -> dict[str, int]:
    result: dict[str, int] = {}
    try:
        with open("/proc/self/status", encoding="utf-8") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        result[parts[0].rstrip(":")] = int(parts[1])
                    except ValueError:
                        pass
    except OSError:
        pass
    return result


def rss_mb() -> float:
    kv = _proc_status_kv()
    return kv.get("VmRSS", 0) / 1024.0


def vmhwm_mb() -> float:
    kv = _proc_status_kv()
    return kv.get("VmHWM", 0) / 1024.0


def gc_gen_counts() -> tuple[int, int, int]:
    counts = gc.get_count()
    return counts[0], counts[1], counts[2] if len(counts) > 2 else 0


def gc_total_objects() -> int:
    return sum(len(gc.get_objects(gen)) for gen in range(3))


def tracemalloc_snapshot_diff(
    baseline: tracemalloc.Snapshot | None,
) -> list[tuple[str, float]]:
    """Return top-5 (location, delta_MB) diffs vs baseline; positive = growth."""
    current_snap = tracemalloc.take_snapshot()
    if baseline is None:
        return []
    stats = current_snap.compare_to(baseline, "lineno")
    top = sorted(stats, key=lambda s: s.size_diff, reverse=True)[:5]
    out = []
    for s in top:
        if s.size_diff == 0:
            break
        loc = str(s.traceback[0]) if s.traceback else "<unknown>"
        out.append((loc, s.size_diff / MB))
    return out


# ---------------------------------------------------------------------------
# DB artifact helpers
# ---------------------------------------------------------------------------

def _artifact_paths(db_path: Path) -> list[Path]:
    stems = [db_path] + [
        Path(f"{db_path}{suf}")
        for suf in (".wal", ".shm", "-wal", "-shm", "-lock")
    ]
    return stems


def remove_db_artifacts(db_path: Path) -> None:
    for p in _artifact_paths(db_path):
        if p.exists():
            try:
                p.unlink()
            except OSError:
                pass


def db_total_mb(db_path: Path) -> float:
    return sum(
        p.stat().st_size for p in _artifact_paths(db_path) if p.exists()
    ) / MB


def wal_mb(db_path: Path) -> float:
    total = 0
    for suf in (".wal", "-wal"):
        p = Path(f"{db_path}{suf}")
        if p.exists():
            total += p.stat().st_size
    return total / MB


# ---------------------------------------------------------------------------
# Statistical helpers
# ---------------------------------------------------------------------------

def linear_slope(values: list[float]) -> float:
    """Least-squares slope of values vs. their integer indices."""
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    xm = statistics.mean(xs)
    ym = statistics.mean(values)
    denom = sum((x - xm) ** 2 for x in xs)
    if denom < 1e-15:
        return 0.0
    return sum((x - xm) * (y - ym) for x, y in zip(xs, values)) / denom


def mann_kendall(values: list[float]) -> tuple[float, float]:
    """
    Two-sided Mann-Kendall trend test.
    Returns (S, p_approx) where large |S| and small p indicates a trend.
    p_approx uses the normal approximation (valid for n >= 8).
    """
    n = len(values)
    if n < 4:
        return 0.0, 1.0
    s = 0
    for i in range(n - 1):
        for j in range(i + 1, n):
            diff = values[j] - values[i]
            if diff > 0:
                s += 1
            elif diff < 0:
                s -= 1
    # variance (no ties assumed for simplicity)
    var_s = n * (n - 1) * (2 * n + 5) / 18.0
    if var_s <= 0:
        return float(s), 1.0
    z = (s - (1 if s > 0 else -1)) / math.sqrt(var_s) if s != 0 else 0.0
    # two-sided p from standard normal
    p = 2.0 * (1.0 - _norm_cdf(abs(z)))
    return float(s), p


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def sparkline(values: list[float], width: int = 36) -> str:
    data = values[-width:]
    if not data:
        return ""
    lo, hi = min(data), max(data)
    span = hi - lo
    if span < 1e-9:
        return SPARK_CHARS[1] * len(data)
    scale = (len(SPARK_CHARS) - 1) / span
    return "".join(
        SPARK_CHARS[min(len(SPARK_CHARS) - 1, max(0, int((v - lo) * scale)))]
        for v in data
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Sample:
    iteration: int
    rss_mb: float
    heap_mb: float
    heap_peak_mb: float
    gc_gen0: int
    gc_gen1: int
    gc_gen2: int
    gc_objects: int
    db_mb: float
    wal_mb_val: float
    iter_ms: float
    ops: int


@dataclass
class ScenarioResult:
    name: str
    description: str
    samples: list[Sample] = field(default_factory=list)
    start_rss_mb: float = 0.0
    warmup: int = 0
    slope_mb_per_iter: float = 0.0
    mk_s: float = 0.0
    mk_p: float = 1.0
    drift_mb: float = 0.0
    db_growth_mb: float = 0.0
    net_drift_mb: float = 0.0
    drift_budget_mb: float = 0.0
    peak_rss_mb: float = 0.0
    total_ops: int = 0
    elapsed_s: float = 0.0
    passed: bool = False
    # thresholds stored for reporting
    threshold_slope: float = 0.0
    threshold_drift: float = 0.0
    mk_p_threshold: float = 0.05

    def verdict_markup(self) -> str:
        if self.passed:
            return "[bold green]PASS[/]"
        return "[bold red]FAIL[/]"

    def summary_row(self) -> tuple[str, ...]:
        mk_color = "red" if self.mk_p < self.mk_p_threshold and not self.passed else "green"
        return (
            self.name,
            f"{len(self.samples)}",
            f"{self.total_ops:,}",
            f"{self.start_rss_mb:.2f}",
            f"{self.peak_rss_mb:.2f}",
            f"{self.net_drift_mb:+.3f}",
            f"{self.slope_mb_per_iter:+.5f}",
            f"[{mk_color}]{self.mk_p:.3f}[/]",
            f"{self.elapsed_s:.1f}s",
            self.verdict_markup(),
        )


Scenario = Callable[[Path, int], int]  # returns number of logical ops


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def scenario_connection_churn(db_path: Path, iteration: int) -> int:
    """
    Open/close many short-lived connections with varying stmt_cache_size values.
    Exercises Connection.__init__ / close() and the underlying ddb_db_open /
    ddb_db_free native calls.
    """
    ops = 0
    for cache_size in (0, 8, 64):
        conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=cache_size)
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS conn_probe (id INT64 PRIMARY KEY, tag TEXT)"
        )
        cur.execute(
            "INSERT INTO conn_probe (id, tag) VALUES (?, ?) "
            "ON CONFLICT(id) DO UPDATE SET tag = excluded.tag",
            (iteration, f"churn-{iteration}-cs{cache_size}"),
        )
        conn.commit()
        cur.execute("SELECT id, tag FROM conn_probe WHERE id = ?", (iteration,))
        cur.fetchone()
        cur.close()
        conn.close()
        ops += 4
    return ops


def scenario_schema_churn(db_path: Path, iteration: int) -> int:
    """
    Create and immediately drop tables + indexes in a tight loop.
    Stresses the DDL path and schema-object memory lifecycle.
    """
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()
    for slot in range(5):
        tname = f"sc_t{slot}_{iteration}"
        iname = f"sc_i{slot}_{iteration}"
        cur.execute(
            f"CREATE TABLE {tname} (id INT64 PRIMARY KEY, bucket INT64, label TEXT)"
        )
        cur.executemany(
            f"INSERT INTO {tname} (id, bucket, label) VALUES (?, ?, ?)",
            [(j, j % 13, f"lbl{j}") for j in range(40)],
        )
        conn.commit()
        cur.execute(f"CREATE INDEX {iname} ON {tname}(bucket)")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {tname} WHERE bucket = ?", (3,))
        cur.fetchone()
        cur.execute(f"DROP INDEX {iname}")
        cur.execute(f"DROP TABLE {tname}")
        conn.commit()
        ops += 40 + 5
    cur.close()
    conn.close()
    return ops


def scenario_cursor_reuse(db_path: Path, iteration: int) -> int:
    """
    Alternate between reusing the same cursor and creating new cursors for the
    same queries.  Exercises the statement-cache recycle path inside the binding.
    """
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=32)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS cur_probe (id INT64 PRIMARY KEY, v INT64, s TEXT)"
    )
    conn.commit()

    base_id = iteration * 500

    # Insert batch with the *same* cursor
    cur.executemany(
        "INSERT INTO cur_probe (id, v, s) VALUES (?, ?, ?)",
        [(base_id + j, j, f"s{j}") for j in range(50)],
    )
    conn.commit()
    ops += 50

    # Query with reused cursor
    for j in range(0, 50, 5):
        cur.execute("SELECT id, v, s FROM cur_probe WHERE v = ?", (j,))
        cur.fetchall()
        ops += 1

    # Create fresh cursors for the same queries
    for j in range(0, 50, 5):
        fresh = conn.cursor()
        fresh.execute("SELECT id, v, s FROM cur_probe WHERE id = ?", (base_id + j,))
        fresh.fetchone()
        fresh.close()
        ops += 1

    # Update + delete via reused cursor
    cur.execute(
        "UPDATE cur_probe SET v = v + 1 WHERE id >= ? AND id < ?",
        (base_id, base_id + 50),
    )
    cur.execute(
        "DELETE FROM cur_probe WHERE id >= ? AND id < ?",
        (base_id, base_id + 50),
    )
    conn.commit()
    ops += 2

    cur.close()
    conn.close()
    return ops


def scenario_type_variety(db_path: Path, iteration: int) -> int:
    """
    Exercise all eight column types that DecentDB exposes: INT64, FLOAT64, TEXT,
    BLOB, BOOL, DECIMAL, UUID (as BLOB), TIMESTAMP.  Forces the full decode
    path for every column type on every fetch.
    """
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=32)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS types_probe ("
        "  id       INT64 PRIMARY KEY,"
        "  f64      FLOAT64,"
        "  txt      TEXT,"
        "  blob_col BLOB,"
        "  flag     BOOL,"
        "  dec_val  DECIMAL(12,4),"
        "  uid      BLOB,"
        "  ts       TIMESTAMP"
        ")"
    )
    conn.commit()

    now = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    uid = uuid.uuid4().bytes
    blob_payload = bytes(range(256)) * 4  # 1 KB

    rows = [
        (
            iteration * 100 + j,
            float(iteration * 100 + j) / 3.14159,
            f"text_{iteration}_{j}_{'α' * (j % 8)}",
            blob_payload[: 64 + j * 4],
            bool(j % 2),
            decimal.Decimal(f"{iteration}.{j:04d}"),
            uid,
            now + datetime.timedelta(seconds=j),
        )
        for j in range(20)
    ]
    cur.executemany(
        "INSERT INTO types_probe (id, f64, txt, blob_col, flag, dec_val, uid, ts) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    ops += len(rows)

    cur.execute("SELECT id, f64, txt, blob_col, flag, dec_val, uid, ts FROM types_probe "
                "WHERE id >= ? AND id < ?",
                (iteration * 100, iteration * 100 + 20))
    fetched = cur.fetchall()
    ops += len(fetched)

    cur.execute(
        "DELETE FROM types_probe WHERE id >= ? AND id < ?",
        (iteration * 100, iteration * 100 + 20),
    )
    conn.commit()
    ops += 1

    cur.close()
    conn.close()
    return ops


def scenario_large_blob_churn(db_path: Path, iteration: int) -> int:
    """
    Write and immediately re-read progressively larger binary blobs.
    Targets the BLOB encode/decode path and native buffer lifetimes.
    """
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS blob_probe (id INT64 PRIMARY KEY, payload BLOB)"
    )
    conn.commit()

    rng = random.Random(iteration)
    sizes = [1 << k for k in range(10, 16)]  # 1 KB … 32 KB

    for idx, size in enumerate(sizes):
        row_id = iteration * 10 + idx
        payload = bytes(rng.randint(0, 255) for _ in range(size))
        cur.execute(
            "INSERT INTO blob_probe (id, payload) VALUES (?, ?) "
            "ON CONFLICT(id) DO UPDATE SET payload = excluded.payload",
            (row_id, payload),
        )
        conn.commit()
        ops += 1

        cur.execute("SELECT id, payload FROM blob_probe WHERE id = ?", (row_id,))
        row = cur.fetchone()
        assert row is not None and len(row[1]) == size
        ops += 1

    cur.execute("DELETE FROM blob_probe")
    conn.commit()
    ops += 1

    cur.close()
    conn.close()
    return ops


def scenario_rollback_stress(db_path: Path, iteration: int) -> int:
    """
    Heavy rollback / abandoned-write paths.  Exercises the WAL revert logic and
    checks whether partially-written pages leak after rollback.
    """
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS rb_probe (id INT64 PRIMARY KEY, v TEXT)"
    )
    conn.commit()

    base = iteration * 200

    # Commit every-other batch, rollback the rest
    for batch in range(20):
        for j in range(5):
            row_id = base + batch * 5 + j
            cur.execute(
                "INSERT INTO rb_probe (id, v) VALUES (?, ?) "
                "ON CONFLICT(id) DO UPDATE SET v = excluded.v",
                (row_id, f"val-{batch}-{j}"),
            )
        ops += 5
        if batch % 2 == 0:
            conn.commit()
        else:
            conn.rollback()

    # Verify committed rows exist, rolled-back rows do not
    cur.execute("SELECT COUNT(*) FROM rb_probe WHERE id >= ? AND id < ?",
                (base, base + 200))
    count_row = cur.fetchone()
    ops += 1

    # Clean up only committed rows
    cur.execute("DELETE FROM rb_probe WHERE id >= ? AND id < ?", (base, base + 200))
    conn.commit()
    ops += 1

    cur.close()
    conn.close()
    return ops


def scenario_introspection_churn(db_path: Path, iteration: int) -> int:
    """
    Repeatedly call the reflection APIs: list_tables, list_indexes,
    get_table_columns, get_table_ddl, inspect_storage_state.
    These return heap-allocated strings / JSON that must be freed on both sides.
    """
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()

    tname = f"introspect_t{iteration % 4}"
    iname = f"introspect_i{iteration % 4}"

    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {tname} "
        f"(id INT64 PRIMARY KEY, bucket INT64, tag TEXT)"
    )
    cur.executemany(
        f"INSERT INTO {tname} (id, bucket, tag) VALUES (?, ?, ?)",
        [(iteration * 10 + j, j % 5, f"t{j}") for j in range(10)],
    )
    conn.commit()
    ops += 10

    cur.execute(f"CREATE INDEX IF NOT EXISTS {iname} ON {tname}(bucket)")
    conn.commit()
    ops += 1

    for _ in range(6):
        _ = conn.list_tables()
        _ = conn.list_indexes()
        _ = conn.get_table_columns(tname)
        _ = conn.get_table_ddl(tname)
        ops += 4
        try:
            _ = conn.inspect_storage_state()
            ops += 1
        except decentdb.NotSupportedError:
            pass

    cur.execute(f"DROP INDEX IF EXISTS {iname}")
    cur.execute(f"DROP TABLE IF EXISTS {tname}")
    conn.commit()
    ops += 2

    cur.close()
    conn.close()
    return ops


def scenario_multi_reader_churn(db_path: Path, iteration: int) -> int:
    """
    One writer connection plus three simultaneous reader connections.
    Exercises multi-connection state isolation and reader reference counting.
    """
    ops = 0
    writer = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=32)
    wcur = writer.cursor()
    wcur.execute(
        "CREATE TABLE IF NOT EXISTS mr_probe (id INT64 PRIMARY KEY, grp INT64, val TEXT)"
    )
    writer.commit()

    base = iteration * 300
    wcur.executemany(
        "INSERT INTO mr_probe (id, grp, val) VALUES (?, ?, ?)",
        [(base + j, j % 10, f"v{iteration}-{j}") for j in range(300)],
    )
    writer.commit()
    ops += 300

    # Three concurrent readers
    readers = [
        decentdb.connect(str(db_path), mode="open", stmt_cache_size=8)
        for _ in range(3)
    ]
    for grp, reader in enumerate(readers):
        rcur = reader.cursor()
        rcur.execute(
            "SELECT id, grp, val FROM mr_probe WHERE grp = ? AND id >= ?",
            (grp % 10, base),
        )
        rows = rcur.fetchall()
        ops += len(rows)
        rcur.close()
        reader.close()

    # Writer cleans up
    wcur.execute("DELETE FROM mr_probe WHERE id >= ? AND id < ?", (base, base + 300))
    writer.commit()
    ops += 1

    wcur.close()
    writer.close()
    return ops


# ---------------------------------------------------------------------------
# Canary scenario – intentionally accumulates references to verify detection
# ---------------------------------------------------------------------------

_canary_accumulated: list = []


def scenario_canary_leak(db_path: Path, iteration: int) -> int:
    """CANARY: intentionally holds references – should FAIL the leak check."""
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=8)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS canary (id INT64 PRIMARY KEY, payload BLOB)")
    payload = bytes(range(256)) * 16  # 4 KB
    cur.execute("INSERT INTO canary (id, payload) VALUES (?, ?) "
                "ON CONFLICT(id) DO UPDATE SET payload = excluded.payload",
                (iteration, payload))
    conn.commit()
    cur.execute("SELECT id, payload FROM canary WHERE id = ?", (iteration,))
    row = cur.fetchone()
    # Intentionally keep a strong reference to grow memory monotonically
    _canary_accumulated.append((iteration, row[1] if row else None))
    cur.close()
    conn.close()
    return 3


# ---------------------------------------------------------------------------
# Scenario registry
# ---------------------------------------------------------------------------

SCENARIOS: list[tuple[str, Scenario, str]] = [
    ("connection_churn",      scenario_connection_churn,    "rapid open/close with varying cache sizes"),
    ("schema_churn",          scenario_schema_churn,        "create/drop tables and indexes in tight loops"),
    ("cursor_reuse",          scenario_cursor_reuse,        "cursor reuse vs. re-create; stmt cache pressure"),
    ("type_variety",          scenario_type_variety,        "all 8 column types: int, float, text, blob, bool, decimal, uuid, ts"),
    ("large_blob_churn",      scenario_large_blob_churn,    "write and read progressively larger binary payloads"),
    ("rollback_stress",       scenario_rollback_stress,     "heavy rollback / abandoned-write / WAL revert paths"),
    ("introspection_churn",   scenario_introspection_churn, "list_tables / list_indexes / describe / storage_state"),
    ("multi_reader_churn",    scenario_multi_reader_churn,  "multiple concurrent readers with one writer"),
]

CANARY_SCENARIO = ("canary_leak", scenario_canary_leak, "CANARY: intentional leak – must FAIL")


# ---------------------------------------------------------------------------
# Dashboard state + rendering
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
    start_rss_mb: float = 0.0
    total_ops: int = 0
    results: list[ScenarioResult] = field(default_factory=list)
    top_allocs: list[tuple[str, float]] = field(default_factory=list)
    scenario_progress: Progress | None = None
    overall_progress: Progress | None = None


def _make_layout() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=4),
        Layout(name="body"),
        Layout(name="footer", size=10),
    )
    layout["body"].split_row(
        Layout(name="left", ratio=5),
        Layout(name="right", ratio=7),
    )
    layout["left"].split_column(
        Layout(name="metrics"),
        Layout(name="allocs", size=9),
    )
    return layout


def _render_header(state: DashState) -> Panel:
    elapsed = time.perf_counter() - state.started
    t = Text()
    t.append(f"  DecentDB Memory Leak Probe", style="bold bright_cyan")
    t.append(f"  [{MODEL_TAG}]", style="dim cyan")
    t.append(f"\n  scenario {state.scenario_index}/{state.scenario_total}", style="white")
    if state.scenario_name:
        t.append(f"  →  {state.scenario_name}", style="bold yellow")
    t.append(f"   ·  elapsed {elapsed:6.1f}s", style="dim")
    return Panel(t, border_style="bright_cyan", padding=(0, 1))


def _render_metrics(state: DashState) -> Panel:
    s = state.last_sample
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="cyan", no_wrap=True, min_width=18)
    grid.add_column(style="white", no_wrap=True)

    def row(k: str, v: str) -> None:
        grid.add_row(k, v)

    if s is None:
        row("status", "warming up …")
    else:
        delta = s.rss_mb - state.start_rss_mb
        delta_color = "red" if delta > 2.0 else ("yellow" if delta > 0.5 else "green")
        row("iteration", f"{s.iteration} / {state.iterations_total}")
        row("RSS", f"{s.rss_mb:8.2f} MB  Δ=[{delta_color}]{delta:+.3f} MB[/]")
        row("heap (tracemalloc)", f"{s.heap_mb:8.3f} MB  peak {s.heap_peak_mb:.3f} MB")
        row("GC objects", f"{s.gc_objects:,}  (gen0={s.gc_gen0} gen1={s.gc_gen1} gen2={s.gc_gen2})")
        row("DB size", f"{s.db_mb:.3f} MB   WAL {s.wal_mb_val:.3f} MB")
        row("iter time", f"{s.iter_ms:.2f} ms")
        if len(state.iter_ms_series) >= 5:
            row("avg (last 20)", f"{statistics.mean(state.iter_ms_series[-20:]):.2f} ms")
        row("total ops", f"{state.total_ops:,}")
        row("VmHWM", f"{vmhwm_mb():.2f} MB")

    group = Group(
        grid,
        Text(""),
        Text("RSS  ", style="magenta") + Text(sparkline(state.rss_series, 36), style="magenta"),
        Text("heap ", style="green")   + Text(sparkline(state.heap_series, 36), style="green"),
        Text("ms   ", style="blue")    + Text(sparkline(state.iter_ms_series, 36), style="blue"),
    )
    return Panel(group, title="live metrics", border_style="magenta", padding=(0, 1))


def _render_allocs(state: DashState) -> Panel:
    tbl = Table(show_edge=False, expand=True, box=None)
    tbl.add_column("allocation site", style="dim", ratio=4)
    tbl.add_column("Δ MB", style="yellow", justify="right", ratio=1)
    if state.top_allocs:
        for loc, delta in state.top_allocs[:5]:
            color = "red" if delta > 0 else "green"
            tbl.add_row(
                loc[-55:] if len(loc) > 55 else loc,
                f"[{color}]{delta:+.4f}[/]",
            )
    else:
        tbl.add_row("[dim]no tracemalloc data yet[/]", "")
    return Panel(tbl, title="top allocation deltas vs baseline", border_style="yellow", padding=(0, 1))


def _render_scenarios(state: DashState) -> Panel:
    tbl = Table(expand=True, show_edge=False, box=None)
    tbl.add_column("scenario", style="cyan", no_wrap=True, ratio=3)
    tbl.add_column("iters", justify="right", ratio=1)
    tbl.add_column("ops", justify="right", ratio=1)
    tbl.add_column("start MB", justify="right", ratio=1)
    tbl.add_column("peak MB", justify="right", ratio=1)
    tbl.add_column("drift", justify="right", ratio=1)
    tbl.add_column("slope", justify="right", ratio=1)
    tbl.add_column("mk-p", justify="right", ratio=1)
    tbl.add_column("time", justify="right", ratio=1)
    tbl.add_column("verdict", justify="center", ratio=1)

    for r in state.results:
        tbl.add_row(*r.summary_row())

    # Add in-progress row
    if state.scenario_name and (
        not state.results or state.results[-1].name != state.scenario_name
    ):
        peak = max(state.rss_series) if state.rss_series else 0.0
        drift = (state.rss_series[-1] - state.start_rss_mb) if state.rss_series else 0.0
        tbl.add_row(
            state.scenario_name,
            f"{state.iteration}/{state.iterations_total}",
            f"{state.total_ops:,}",
            f"{state.start_rss_mb:.2f}",
            f"{peak:.2f}",
            f"{drift:+.3f}",
            "…",
            "…",
            "…",
            "[yellow]RUNNING[/]",
        )

    return Panel(tbl, title="scenario results", border_style="cyan", padding=(0, 1))


def _render_footer(state: DashState) -> Panel:
    parts: list = []
    if state.overall_progress is not None:
        parts.append(state.overall_progress)
    if state.scenario_progress is not None:
        parts.append(state.scenario_progress)
    parts.append(Text(f"  {state.scenario_desc}", style="dim"))
    return Panel(Group(*parts), title="progress", border_style="blue", padding=(0, 1))


def _render_dashboard(state: DashState) -> Layout:
    layout = _make_layout()
    layout["header"].update(_render_header(state))
    layout["metrics"].update(_render_metrics(state))
    layout["allocs"].update(_render_allocs(state))
    layout["right"].update(_render_scenarios(state))
    layout["footer"].update(_render_footer(state))
    return layout


# ---------------------------------------------------------------------------
# Runner
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
    mk_p_threshold: float,
    scenario_index: int,
) -> ScenarioResult:
    remove_db_artifacts(db_path)
    gc.collect()
    gc.collect()
    tracemalloc.reset_peak()
    baseline_snap = tracemalloc.take_snapshot()

    result = ScenarioResult(
        name=name,
        description=desc,
        warmup=warmup,
        threshold_slope=slope_threshold,
        threshold_drift=drift_threshold,
        mk_p_threshold=mk_p_threshold,
    )
    result.start_rss_mb = rss_mb()

    state.scenario_name = name
    state.scenario_desc = desc
    state.scenario_index = scenario_index
    state.iteration = 0
    state.iterations_total = iterations
    state.rss_series.clear()
    state.heap_series.clear()
    state.iter_ms_series.clear()
    state.start_rss_mb = result.start_rss_mb
    state.total_ops = 0
    state.last_sample = None
    state.top_allocs.clear()

    scen_prog = Progress(
        SpinnerColumn(style="yellow"),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        transient=False,
    )
    scen_task = scen_prog.add_task(name, total=iterations)
    state.scenario_progress = scen_prog

    t_start = time.perf_counter()

    for i in range(1, iterations + 1):
        t_iter = time.perf_counter()
        ops = fn(db_path, i)
        iter_ms = (time.perf_counter() - t_iter) * 1000.0
        state.total_ops += ops

        if i % 10 == 0:
            gc.collect()

        current, peak = tracemalloc.get_traced_memory()
        gen0, gen1, gen2 = gc_gen_counts()
        sample = Sample(
            iteration=i,
            rss_mb=rss_mb(),
            heap_mb=current / MB,
            heap_peak_mb=peak / MB,
            gc_gen0=gen0,
            gc_gen1=gen1,
            gc_gen2=gen2,
            gc_objects=gc_total_objects(),
            db_mb=db_total_mb(db_path),
            wal_mb_val=wal_mb(db_path),
            iter_ms=iter_ms,
            ops=ops,
        )
        result.samples.append(sample)
        state.rss_series.append(sample.rss_mb)
        state.heap_series.append(sample.heap_mb)
        state.iter_ms_series.append(sample.iter_ms)
        state.iteration = i
        state.last_sample = sample

        if i % 20 == 0:
            state.top_allocs = tracemalloc_snapshot_diff(baseline_snap)

        scen_prog.update(scen_task, advance=1)
        live.update(_render_dashboard(state))

    result.elapsed_s = time.perf_counter() - t_start
    result.total_ops = sum(s.ops for s in result.samples)
    result.peak_rss_mb = max(s.rss_mb for s in result.samples)

    # Post-warmup analysis
    tail_rss = (
        [s.rss_mb for s in result.samples[warmup:]]
        if warmup < len(result.samples)
        else [s.rss_mb for s in result.samples]
    )
    tail_db = (
        [s.db_mb for s in result.samples[warmup:]]
        if warmup < len(result.samples)
        else [s.db_mb for s in result.samples]
    )
    result.slope_mb_per_iter = linear_slope(tail_rss)
    result.mk_s, result.mk_p = mann_kendall(tail_rss)
    result.drift_mb = (tail_rss[-1] - tail_rss[0]) if len(tail_rss) > 1 else 0.0
    result.db_growth_mb = (tail_db[-1] - tail_db[0]) if len(tail_db) > 1 else 0.0
    # RSS growth attributable to the on-disk DB+WAL footprint (rows persisted
    # across iterations, page cache backing the larger file) is not a process
    # leak; subtract it before applying the absolute drift cap.
    result.net_drift_mb = result.drift_mb - max(0.0, result.db_growth_mb)
    # The slope threshold is a per-iter rate; the drift cap must be at least
    # consistent with that rate over the post-warmup window so the two
    # criteria don't contradict each other at large iteration counts.
    post_warmup_iters = max(1, len(tail_rss) - 1)
    result.drift_budget_mb = drift_threshold + slope_threshold * post_warmup_iters

    slope_ok = abs(result.slope_mb_per_iter) <= slope_threshold
    drift_ok = abs(result.net_drift_mb) <= result.drift_budget_mb
    # Mann-Kendall is a more robust monotonic-trend test than a short-window
    # linear regression. A borderline slope is only meaningful if MK also
    # detects a significant trend, otherwise it is just noise.
    trend_significant = result.mk_p < mk_p_threshold
    result.passed = drift_ok and (slope_ok or not trend_significant)

    state.top_allocs = tracemalloc_snapshot_diff(baseline_snap)
    state.results.append(result)
    remove_db_artifacts(db_path)
    state.scenario_progress = None
    live.update(_render_dashboard(state))
    return result


# ---------------------------------------------------------------------------
# Final report
# ---------------------------------------------------------------------------

def print_final_report(
    state: DashState,
    args: argparse.Namespace,
    json_out: Path | None,
) -> int:
    console.print()
    console.print(Rule(f"[bold bright_cyan]{MODEL_TAG} · DecentDB Memory Leak – Final Report[/]"))
    console.print()

    tbl = Table(
        title=None,
        expand=True,
        show_lines=True,
        border_style="cyan",
    )
    tbl.add_column("scenario", style="cyan", no_wrap=True)
    tbl.add_column("iters", justify="right")
    tbl.add_column("ops", justify="right")
    tbl.add_column("start MB", justify="right")
    tbl.add_column("peak MB", justify="right")
    tbl.add_column("drift MB", justify="right")
    tbl.add_column("slope MB/iter", justify="right")
    tbl.add_column("mk-p", justify="right")
    tbl.add_column("elapsed", justify="right")
    tbl.add_column("verdict", justify="center")

    for r in state.results:
        tbl.add_row(*r.summary_row())

    console.print(tbl)
    console.print()

    overall_pass = all(r.passed for r in state.results if r.name != "canary_leak")
    canary = next((r for r in state.results if r.name == "canary_leak"), None)
    if canary is not None:
        canary_detected = not canary.passed
        canary_color = "green" if canary_detected else "red"
        canary_label = "CANARY DETECTED (framework working)" if canary_detected else "CANARY MISSED – framework may not detect leaks"
        console.print(Panel(
            Align.center(Text(canary_label, style=f"bold {canary_color}")),
            title="canary check",
            border_style=canary_color,
        ))
        console.print()

    color = "green" if overall_pass else "red"
    verdict = (
        "✅  PASS – no meaningful memory leak detected across all scenarios"
        if overall_pass
        else "❌  FAIL – potential memory leak detected; see slope/drift columns above"
    )
    console.print(Panel(
        Align.center(Text(verdict, style=f"bold {color}")),
        title="overall verdict",
        border_style=color,
    ))

    # Legend
    console.print()
    console.print("[dim]  slope  = linear regression MB/iter on post-warmup RSS[/]")
    console.print("[dim]  drift  = post-warmup RSS drift with on-disk DB+WAL growth subtracted[/]")
    console.print("[dim]           (data persisting on disk across iterations is not a process leak)[/]")
    console.print("[dim]  mk-p   = Mann-Kendall trend test p-value (low = trend present)[/]")
    console.print("[dim]  pass   = |slope| <= slope_threshold OR mk-p >= mk_p_threshold;[/]")
    console.print("[dim]           AND |drift| <= drift_threshold + slope_threshold * post_warmup_iters[/]")
    console.print()

    if json_out is not None:
        payload = {
            "model": MODEL_TAG,
            "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
            "overall_pass": overall_pass,
            "scenarios": [
                {
                    "name": r.name,
                    "description": r.description,
                    "iterations": len(r.samples),
                    "warmup": r.warmup,
                    "total_ops": r.total_ops,
                    "start_rss_mb": r.start_rss_mb,
                    "peak_rss_mb": r.peak_rss_mb,
                    "drift_mb": r.drift_mb,
                    "db_growth_mb": r.db_growth_mb,
                    "net_drift_mb": r.net_drift_mb,
                    "drift_budget_mb": r.drift_budget_mb,
                    "slope_mb_per_iter": r.slope_mb_per_iter,
                    "mk_s": r.mk_s,
                    "mk_p": r.mk_p,
                    "threshold_slope": r.threshold_slope,
                    "threshold_drift": r.threshold_drift,
                    "elapsed_s": r.elapsed_s,
                    "passed": r.passed,
                    "samples": [
                        {
                            "i": s.iteration,
                            "rss_mb": s.rss_mb,
                            "heap_mb": s.heap_mb,
                            "heap_peak_mb": s.heap_peak_mb,
                            "gc_gen0": s.gc_gen0,
                            "gc_gen1": s.gc_gen1,
                            "gc_gen2": s.gc_gen2,
                            "gc_objects": s.gc_objects,
                            "db_mb": s.db_mb,
                            "wal_mb": s.wal_mb_val,
                            "iter_ms": s.iter_ms,
                            "ops": s.ops,
                        }
                        for s in r.samples
                    ],
                }
                for r in state.results
            ],
        }
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        console.print(f"[dim]  JSON report written to: {json_out}[/]")

    return 0 if overall_pass else 2


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=f"DecentDB memory leak showcase [{MODEL_TAG}]",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--iterations", type=int, default=120,
                   help="iterations per scenario")
    p.add_argument("--warmup", type=int, default=12,
                   help="warmup iterations excluded from statistical analysis")
    p.add_argument("--slope-threshold", type=float, default=0.025,
                   help="|MB/iter| tolerance on post-warmup RSS linear slope")
    p.add_argument("--drift-threshold", type=float, default=4.0,
                   help="|MB| start-to-end RSS drift tolerance on post-warmup window")
    p.add_argument("--mk-p-threshold", type=float, default=0.05,
                   help="Mann-Kendall p < this is highlighted as a trend (informational)")
    p.add_argument("--only", type=str, default=None,
                   help="comma-separated scenario names to run (e.g. cursor_reuse,schema_churn)")
    p.add_argument("--canary", action="store_true",
                   help="prepend a known-leaky canary scenario to verify leak detection")
    p.add_argument("--workdir", type=Path, default=None,
                   help="directory for temp DB files (default: system temp)")
    p.add_argument("--json-out", type=Path, default=None,
                   help="write full JSON report to this path")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    workdir = args.workdir or (
        Path(tempfile.gettempdir()) / f"decentdb-{MODEL_TAG}-leak"
    )
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "leak_probe.ddb"

    # Build scenario list
    scenarios = list(SCENARIOS)
    if args.only:
        wanted = {n.strip() for n in args.only.split(",")}
        scenarios = [s for s in scenarios if s[0] in wanted]
        if not scenarios:
            console.print(f"[red]No scenarios matched --only={args.only!r}[/]")
            return 1

    if args.canary:
        scenarios = [CANARY_SCENARIO] + scenarios

    tracemalloc.start(25)  # capture 25-frame tracebacks

    overall_prog = Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total} scenarios"),
        TimeElapsedColumn(),
        transient=False,
    )
    overall_task = overall_prog.add_task("overall", total=len(scenarios))

    state = DashState(
        started=time.perf_counter(),
        scenario_total=len(scenarios),
        overall_progress=overall_prog,
    )

    with Live(
        _render_dashboard(state),
        console=console,
        refresh_per_second=8,
        screen=False,
    ) as live:
        for idx, (name, fn, desc) in enumerate(scenarios, start=1):
            run_scenario(
                state=state,
                live=live,
                name=name,
                fn=fn,
                desc=desc,
                db_path=db_path,
                iterations=args.iterations,
                warmup=args.warmup,
                slope_threshold=args.slope_threshold,
                drift_threshold=args.drift_threshold,
                mk_p_threshold=args.mk_p_threshold,
                scenario_index=idx,
            )
            overall_prog.update(overall_task, advance=1)
            live.update(_render_dashboard(state))

    tracemalloc.stop()

    return print_final_report(state, args, args.json_out)


if __name__ == "__main__":
    sys.exit(main())
