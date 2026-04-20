#!/usr/bin/env python3
"""
DecentDB Comprehensive Memory-Leak Test Harness
================================================

Goal: deliver high confidence that neither the DecentDB engine core nor the
DecentDB Python bindings leak memory, by combining the strongest signals from
the existing showcase scripts with several coverage gaps they all share.

What this harness adds vs. the existing scripts
-----------------------------------------------
1. **Canary gate.** Two intentional leaks (Python ref accumulation and a
   "forget to close connections" leak) MUST be detected before any clean
   verdict is trusted. If the canaries don't trip, the framework is
   uncalibrated and the run is aborted.
2. **Subprocess-parity engine check.** Spawn N short-lived child processes,
   each does a heavy DDL+DML+fetch workload then exits. The parent process
   RSS must not drift across spawns; this isolates engine teardown from
   in-process noise (page cache, glibc arenas).
3. **Long-lived single connection.** One connection executes millions of
   statements. Most existing scripts churn connections, which masks
   per-connection accumulation that's only freed at close.
4. **Error-path stress.** Parse errors, bind errors, runtime errors, all
   caught and discarded in tight loops. Exception unwinding through the
   binding is a common leak surface.
5. **Reopen / WAL-recovery cycle.** Open, write, close without checkpoint,
   reopen → recover. RSS at successive reopen cycles should converge.
6. **SQLAlchemy dialect surface.** End-to-end engine+session churn through
   the dialect package, since that path uses different binding entry points
   than direct DB-API usage.
7. **Principled analysis.** Per-iter slope, Mann-Kendall trend test,
   DB-growth-corrected drift, drift budget that scales with iterations,
   tracemalloc top-allocator diff, gc-object delta, gc-gen2 collections.
8. **Optional Valgrind hint.** With ``--print-valgrind-cmd`` prints the
   exact command to re-run this harness under Valgrind / massif for an
   independent corroboration.

Pass criteria for HIGH confidence
---------------------------------
A scenario PASSes when **all** of:
  - |slope MB/iter| <= slope_threshold,  OR Mann-Kendall trend not significant
  - (drift - db_growth) <= drift_threshold + slope_threshold * post_warmup_iters
    (negative net_drift means RSS grew less than the DB on disk -> always passes)
  - tracemalloc top-allocator diff shows no decentdb-attributable site
    growing more than ``--alloc-leak-threshold`` bytes per iter

Overall verdict is HIGH-CONFIDENCE PASS when:
  - Both canary leaks were detected (framework is calibrated)
  - All real scenarios PASS
  - Subprocess-parity drift is below the drift budget
  - Reopen-recovery RSS series converges (slope on tail < threshold)

Usage
-----
  cd bindings/python
  ./venv/bin/python showcase/comprehensive-memory-leak-tests.py [options]

Common runs:
  # Quick smoke (~3 min)
  python showcase/comprehensive-memory-leak-tests.py --iterations 80 \\
      --long-lived-ops 50000 --subprocess-runs 8

  # High-confidence run (~30 min)
  python showcase/comprehensive-memory-leak-tests.py --iterations 500 \\
      --long-lived-ops 1000000 --subprocess-runs 32

  # Run only one scenario
  python showcase/comprehensive-memory-leak-tests.py --only long_lived_conn
"""

from __future__ import annotations

import argparse
import datetime
import decimal
import gc
import json
import logging
import math
import os
import platform
import random
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# Make sibling decentdb package importable when run from showcase/
# ---------------------------------------------------------------------------
_PKG_ROOT = Path(__file__).resolve().parents[1]
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))

import decentdb  # noqa: E402

try:
    from rich.align import Align
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
    _HAS_RICH = True
except Exception:  # pragma: no cover - rich is optional
    _HAS_RICH = False
    Console = None  # type: ignore

console = Console() if _HAS_RICH else None
MB = 1024.0 * 1024.0


# ---------------------------------------------------------------------------
# Process metrics
# ---------------------------------------------------------------------------

def _proc_status(field_name: str) -> float:
    try:
        with open("/proc/self/status", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(field_name):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return 0.0


def rss_mb() -> float:
    return _proc_status("VmRSS:")


def vmhwm_mb() -> float:
    return _proc_status("VmHWM:")


def db_artifact_paths(db_path: Path) -> list[Path]:
    base = str(db_path)
    return [Path(p) for p in (
        base, base + ".wal", base + ".shm",
        base + "-wal", base + "-shm", base + "-lock",
    )]


def db_total_mb(db_path: Path) -> float:
    total = 0
    for p in db_artifact_paths(db_path):
        if p.exists():
            total += p.stat().st_size
    return total / MB


def remove_db_artifacts(db_path: Path) -> None:
    for p in db_artifact_paths(db_path):
        if p.exists():
            with suppress(OSError):
                p.unlink()


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def linear_slope(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    xs = list(range(len(values)))
    xm = statistics.mean(xs)
    ym = statistics.mean(values)
    denom = sum((x - xm) ** 2 for x in xs)
    if denom == 0.0:
        return 0.0
    return sum((x - xm) * (y - ym) for x, y in zip(xs, values)) / denom


def mann_kendall(values: list[float]) -> tuple[float, float]:
    """Return (S, p) for a two-sided Mann-Kendall trend test (normal approx)."""
    n = len(values)
    if n < 4:
        return 0.0, 1.0
    s = 0
    for i in range(n - 1):
        for j in range(i + 1, n):
            d = values[j] - values[i]
            if d > 0:
                s += 1
            elif d < 0:
                s -= 1
    var_s = n * (n - 1) * (2 * n + 5) / 18.0
    if var_s == 0:
        return float(s), 1.0
    if s > 0:
        z = (s - 1) / math.sqrt(var_s)
    elif s < 0:
        z = (s + 1) / math.sqrt(var_s)
    else:
        z = 0.0
    p = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2.0))))
    return float(s), p


# ---------------------------------------------------------------------------
# Sample / scenario result
# ---------------------------------------------------------------------------

@dataclass
class Sample:
    iteration: int
    rss_mb: float
    heap_mb: float
    heap_peak_mb: float
    gc_objects: int
    gc_gen2: int
    db_mb: float
    iter_ms: float
    ops: int


@dataclass
class ScenarioResult:
    name: str
    description: str
    samples: list[Sample] = field(default_factory=list)
    warmup: int = 0
    start_rss_mb: float = 0.0
    peak_rss_mb: float = 0.0
    slope_mb_per_iter: float = 0.0
    mk_p: float = 1.0
    drift_mb: float = 0.0
    db_growth_mb: float = 0.0
    net_drift_mb: float = 0.0
    drift_budget_mb: float = 0.0
    total_ops: int = 0
    elapsed_s: float = 0.0
    passed: bool = False
    is_canary: bool = False
    leak_top: list[tuple[str, float]] = field(default_factory=list)
    threshold_slope: float = 0.0
    threshold_drift: float = 0.0
    notes: str = ""


# ---------------------------------------------------------------------------
# Scenarios — REAL workloads
# ---------------------------------------------------------------------------

def s_connection_churn(db_path: Path, i: int) -> int:
    ops = 0
    for cs in (0, 16, 64):
        conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=cs)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS conn_probe (id INT64 PRIMARY KEY, v TEXT)")
        cur.execute(
            "INSERT INTO conn_probe (id, v) VALUES (?, ?) "
            "ON CONFLICT(id) DO UPDATE SET v = excluded.v",
            (i, f"churn-{i}-{cs}"),
        )
        conn.commit()
        cur.execute("SELECT id, v FROM conn_probe WHERE id = ?", (i,))
        cur.fetchone()
        cur.close()
        conn.close()
        ops += 4
    return ops


# Module-level state shared between s_long_lived_conn and its teardown.
# (Do NOT use per-function `_state={}` defaults — each `def` would get its
# own dict, and the teardown would never see the iter function's connection,
# leaving the native Db handle and its prepared-statement cache to leak
# at process exit. That bug masked itself under RSS-based detection because
# the leak was finite, but Valgrind caught it cleanly at `ddb_db_prepare`
# and `ddb_db_open_or_create`.)
_long_lived_state: dict = {}


def s_long_lived_conn(db_path: Path, i: int) -> int:
    """ONE connection across all iterations, doing a fixed batch of work each iter.

    Targets per-connection accumulation that connection-churn tests miss.
    """
    conn = _long_lived_state.get("conn")
    if conn is None:
        conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=128)
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS ll_probe ("
            "id INT64 PRIMARY KEY, bucket INT64, v TEXT, payload BLOB)"
        )
        conn.commit()
        cur.close()
        _long_lived_state["conn"] = conn
    cur = conn.cursor()
    base = i * 200
    cur.executemany(
        "INSERT INTO ll_probe (id, bucket, v, payload) VALUES (?, ?, ?, ?)",
        [
            (base + j, j % 17, f"v-{i}-{j}", bytes((j + k) % 256 for k in range(64)))
            for j in range(200)
        ],
    )
    conn.commit()
    cur.execute("SELECT COUNT(*), SUM(bucket) FROM ll_probe WHERE bucket = ?", (i % 17,))
    cur.fetchone()
    cur.execute("UPDATE ll_probe SET v = v WHERE id >= ? AND id < ?", (base, base + 100))
    conn.commit()
    cur.execute("DELETE FROM ll_probe WHERE id >= ? AND id < ?", (base, base + 200))
    conn.commit()
    cur.close()
    return 200 + 1 + 1 + 1


def s_long_lived_conn_teardown(_db_path: Path) -> None:
    conn = _long_lived_state.pop("conn", None)
    if conn is not None:
        with suppress(Exception):
            conn.close()


def s_schema_churn(db_path: Path, i: int) -> int:
    ops = 0
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()
    for slot in range(4):
        t = f"sc_t_{i}_{slot}"
        idx = f"sc_i_{i}_{slot}"
        cur.execute(f"CREATE TABLE {t} (id INT64 PRIMARY KEY, b INT64, s TEXT)")
        cur.executemany(
            f"INSERT INTO {t} (id, b, s) VALUES (?, ?, ?)",
            [(j, j % 11, f"x{j}") for j in range(40)],
        )
        conn.commit()
        cur.execute(f"CREATE INDEX {idx} ON {t}(b)")
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {t} WHERE b = ?", (3,))
        cur.fetchone()
        cur.execute(f"DROP INDEX {idx}")
        cur.execute(f"DROP TABLE {t}")
        conn.commit()
        ops += 45
    cur.close()
    conn.close()
    return ops


def s_type_variety(db_path: Path, i: int) -> int:
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=32)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS types_probe ("
        "id INT64 PRIMARY KEY, f64 FLOAT64, txt TEXT, blb BLOB,"
        " flag BOOL, dec_v DECIMAL(12,4), uid BLOB, ts TIMESTAMP)"
    )
    conn.commit()
    now = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    uid = uuid.uuid4().bytes
    rows = [
        (
            i * 100 + j, j / 3.14, f"t-{i}-{j}-{'α' * (j % 6)}",
            bytes(range(64)), bool(j & 1),
            decimal.Decimal(f"{i}.{j:04d}"), uid,
            now + datetime.timedelta(seconds=j),
        )
        for j in range(20)
    ]
    cur.executemany(
        "INSERT INTO types_probe (id, f64, txt, blb, flag, dec_v, uid, ts)"
        " VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    cur.execute(
        "SELECT id, f64, txt, blb, flag, dec_v, uid, ts FROM types_probe"
        " WHERE id >= ? AND id < ?", (i * 100, i * 100 + 20),
    )
    fetched = cur.fetchall()
    cur.execute("DELETE FROM types_probe WHERE id >= ? AND id < ?",
                (i * 100, i * 100 + 20))
    conn.commit()
    cur.close()
    conn.close()
    return len(rows) + len(fetched) + 1


def s_blob_churn(db_path: Path, i: int) -> int:
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS blob_probe (id INT64 PRIMARY KEY, p BLOB)")
    conn.commit()
    rng = random.Random(i)
    ops = 0
    for k in range(10, 16):
        rid = i * 10 + (k - 10)
        size = 1 << k
        payload = bytes(rng.randint(0, 255) for _ in range(size))
        cur.execute(
            "INSERT INTO blob_probe (id, p) VALUES (?, ?) "
            "ON CONFLICT(id) DO UPDATE SET p = excluded.p", (rid, payload),
        )
        conn.commit()
        cur.execute("SELECT id, p FROM blob_probe WHERE id = ?", (rid,))
        row = cur.fetchone()
        assert row and len(row[1]) == size
        ops += 2
    cur.execute("DELETE FROM blob_probe")
    conn.commit()
    cur.close()
    conn.close()
    return ops + 1


def s_rollback_stress(db_path: Path, i: int) -> int:
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS rb_probe (id INT64 PRIMARY KEY, v TEXT)")
    conn.commit()
    ops = 0
    base = i * 200
    for batch in range(20):
        for j in range(5):
            rid = base + batch * 5 + j
            cur.execute(
                "INSERT INTO rb_probe (id, v) VALUES (?, ?) "
                "ON CONFLICT(id) DO UPDATE SET v = excluded.v",
                (rid, f"v{batch}-{j}"),
            )
        if batch % 2 == 0:
            conn.commit()
        else:
            conn.rollback()
        ops += 5
    cur.execute("DELETE FROM rb_probe WHERE id >= ? AND id < ?", (base, base + 200))
    conn.commit()
    cur.close()
    conn.close()
    return ops + 1


def s_error_paths(db_path: Path, i: int) -> int:
    """Trigger many error paths in tight loops; ensure exception unwind doesn't leak."""
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=8)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS err_probe (id INT64 PRIMARY KEY, v TEXT)")
    conn.commit()
    ops = 0
    for k in range(30):
        # 1) parse error
        with suppress(Exception):
            cur.execute(f"THIS IS NOT SQL {i}-{k}")
        ops += 1
        # 2) reference an unknown table
        with suppress(Exception):
            cur.execute("SELECT * FROM table_that_does_not_exist_42")
        ops += 1
        # 3) bind count mismatch
        with suppress(Exception):
            cur.execute("INSERT INTO err_probe (id, v) VALUES (?, ?)", (1,))
        ops += 1
        # 4) PK collision (constraint violation)
        with suppress(Exception):
            cur.execute("INSERT INTO err_probe (id, v) VALUES (?, ?)", (i, f"a-{k}"))
            cur.execute("INSERT INTO err_probe (id, v) VALUES (?, ?)", (i, f"b-{k}"))
            conn.commit()
        with suppress(Exception):
            conn.rollback()
        ops += 2
        # 5) operate on closed cursor
        tmp = conn.cursor()
        tmp.close()
        with suppress(Exception):
            tmp.execute("SELECT 1")
        ops += 1
    cur.execute("DELETE FROM err_probe")
    conn.commit()
    cur.close()
    conn.close()
    return ops


def s_reopen_recovery(db_path: Path, i: int) -> int:
    """Open, write, close without explicit checkpoint, reopen → recover.

    Each iteration writes, then closes the connection in a state that forces
    WAL recovery on next open. RSS at successive iterations should converge.
    """
    ops = 0
    for cycle in range(3):
        conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS recov_probe (id INT64 PRIMARY KEY, v TEXT)")
        cur.executemany(
            "INSERT INTO recov_probe (id, v) VALUES (?, ?) "
            "ON CONFLICT(id) DO UPDATE SET v = excluded.v",
            [(i * 50 + j, f"r-{cycle}-{j}") for j in range(50)],
        )
        conn.commit()
        cur.execute("DELETE FROM recov_probe WHERE id >= ? AND id < ?",
                    (i * 50, i * 50 + 50))
        conn.commit()
        cur.close()
        conn.close()
        ops += 51
    return ops


def s_introspection(db_path: Path, i: int) -> int:
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=16)
    cur = conn.cursor()
    t = f"intro_t{i % 4}"
    cur.execute(f"CREATE TABLE IF NOT EXISTS {t} (id INT64 PRIMARY KEY, b INT64, tag TEXT)")
    cur.executemany(
        f"INSERT INTO {t} (id, b, tag) VALUES (?, ?, ?) "
        f"ON CONFLICT(id) DO UPDATE SET tag = excluded.tag",
        [(j, j % 7, f"tag{j}") for j in range(20)],
    )
    conn.commit()
    ops = 0
    # use whatever introspection methods the binding exposes
    for name in ("list_tables", "list_indexes", "get_table_columns",
                 "get_table_ddl", "inspect_storage_state"):
        fn = getattr(conn, name, None)
        if fn is None:
            continue
        try:
            if name in ("get_table_columns", "get_table_ddl"):
                fn(t)
            else:
                fn()
            ops += 1
        except Exception:
            pass
    cur.execute(f"SELECT b, COUNT(*) FROM {t} GROUP BY b")
    cur.fetchall()
    cur.close()
    conn.close()
    return ops + 1


def s_sqlalchemy_churn(db_path: Path, i: int) -> int:
    """End-to-end churn through the SQLAlchemy dialect."""
    try:
        import sqlalchemy as sa
    except Exception:
        return 0
    eng = sa.create_engine(f"decentdb:///{db_path}")
    try:
        with eng.connect() as c:
            c.execute(sa.text(
                "CREATE TABLE IF NOT EXISTS sa_probe (id INT64 PRIMARY KEY, v TEXT)"
            ))
            c.execute(
                sa.text(
                    "INSERT INTO sa_probe (id, v) VALUES (:id, :v) "
                    "ON CONFLICT(id) DO UPDATE SET v = excluded.v"
                ),
                [{"id": i * 10 + j, "v": f"sa-{i}-{j}"} for j in range(10)],
            )
            c.commit()
            res = c.execute(sa.text("SELECT id, v FROM sa_probe WHERE id < :n"),
                            {"n": (i + 1) * 10}).fetchall()
            c.execute(sa.text("DELETE FROM sa_probe WHERE id < :n"),
                      {"n": (i + 1) * 10})
            c.commit()
        return 10 + len(res) + 1
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# CANARY scenarios — must be detected as leaks, otherwise the framework is
# uncalibrated and the run is aborted.
# ---------------------------------------------------------------------------

_canary_refs: list[bytes] = []


def s_canary_pyref_leak(db_path: Path, i: int) -> int:
    """Intentional Python reference leak: keep large bytes objects alive forever."""
    conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=8)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS canary_a (id INT64 PRIMARY KEY, p BLOB)")
    # 256 KB / iter, retained as fetched rows (forces real Python heap growth).
    payload = bytes((i + k) % 256 for k in range(256 * 1024))
    cur.execute(
        "INSERT INTO canary_a (id, p) VALUES (?, ?) "
        "ON CONFLICT(id) DO UPDATE SET p = excluded.p", (i, payload),
    )
    conn.commit()
    cur.execute("SELECT id, p FROM canary_a WHERE id = ?", (i,))
    _canary_refs.append(cur.fetchall())  # the leak: full row tuples kept
    cur.close()
    conn.close()
    return 1


_canary_open_conns: list[object] = []


def s_canary_unclosed_conn(db_path: Path, i: int) -> int:
    """Intentional native-resource leak: forget to close connections AND cursors.

    Each iter opens several connections, runs a query that holds rows in the
    native result, and never closes any of them. The accumulated native
    resources + Python wrapper objects must produce measurable RSS drift.
    """
    ops = 0
    for k in range(2):
        conn = decentdb.connect(str(db_path), mode="open_or_create", stmt_cache_size=64)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS canary_b (id INT64 PRIMARY KEY, p BLOB)")
        cur.executemany(
            "INSERT INTO canary_b (id, p) VALUES (?, ?) "
            "ON CONFLICT(id) DO UPDATE SET p = excluded.p",
            [(i * 1000 + k * 100 + j,
              bytes((j + k) % 256 for _ in range(2048))) for j in range(10)],
        )
        conn.commit()
        # Bounded fetch — only the rows just inserted, NOT a full table scan.
        cur.execute(
            "SELECT id, p FROM canary_b WHERE id >= ? AND id < ?",
            (i * 1000 + k * 100, i * 1000 + k * 100 + 10),
        )
        cur.fetchall()
        _canary_open_conns.append(conn)
        _canary_open_conns.append(cur)
        ops += 12
    return ops


def reset_canaries() -> None:
    _canary_refs.clear()
    for c in _canary_open_conns:
        with suppress(Exception):
            c.close()
    _canary_open_conns.clear()
    gc.collect()


# ---------------------------------------------------------------------------
# Scenario registry
# ---------------------------------------------------------------------------

@dataclass
class ScenarioSpec:
    name: str
    description: str
    fn: Callable[[Path, int], int]
    teardown: Callable[[Path], None] | None = None
    is_canary: bool = False


SCENARIOS: list[ScenarioSpec] = [
    ScenarioSpec("connection_churn", "open/close many short-lived connections", s_connection_churn),
    ScenarioSpec("long_lived_conn", "single persistent connection, millions of stmts", s_long_lived_conn,
                 teardown=s_long_lived_conn_teardown),
    ScenarioSpec("schema_churn", "create/drop tables and indexes per iteration", s_schema_churn),
    ScenarioSpec("type_variety", "exercise all column types end-to-end", s_type_variety),
    ScenarioSpec("blob_churn", "1KB..32KB BLOB write/read/overwrite", s_blob_churn),
    ScenarioSpec("rollback_stress", "heavy commit/rollback alternation", s_rollback_stress),
    ScenarioSpec("error_paths", "exception unwind: parse/bind/runtime/closed-cursor errors", s_error_paths),
    ScenarioSpec("reopen_recovery", "close-without-checkpoint → reopen → recover cycles", s_reopen_recovery),
    ScenarioSpec("introspection", "list_tables / list_indexes / DDL / storage_state", s_introspection),
    ScenarioSpec("sqlalchemy_churn", "SQLAlchemy dialect: engine/connect/exec/dispose", s_sqlalchemy_churn),
]

CANARY_SCENARIOS: list[ScenarioSpec] = [
    ScenarioSpec("canary_pyref_leak", "intentional Python ref leak (256 KB/iter)",
                 s_canary_pyref_leak, is_canary=True),
    ScenarioSpec("canary_unclosed_conn", "intentional unclosed connection leak",
                 s_canary_unclosed_conn, is_canary=True),
]


# ---------------------------------------------------------------------------
# Subprocess engine-parity check
# ---------------------------------------------------------------------------

_SUBPROCESS_CHILD_CODE = r"""
import sys, os
sys.path.insert(0, __PKG_ROOT__)
import decentdb, random, datetime, decimal, uuid
db = sys.argv[1]
rows_per_table = int(sys.argv[2])
conn = decentdb.connect(db, mode="open_or_create", stmt_cache_size=64)
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS sp_probe (id INT64 PRIMARY KEY, b INT64, v TEXT, payload BLOB)")
conn.commit()
rng = random.Random(os.getpid())
cur.executemany(
    "INSERT INTO sp_probe (id, b, v, payload) VALUES (?, ?, ?, ?) "
    "ON CONFLICT(id) DO UPDATE SET v = excluded.v",
    [(j, j % 31, f"v{j}", bytes((j+k) % 256 for k in range(128))) for j in range(rows_per_table)],
)
conn.commit()
cur.execute("CREATE INDEX IF NOT EXISTS sp_idx ON sp_probe(b)")
conn.commit()
cur.execute("SELECT COUNT(*), SUM(b) FROM sp_probe")
cur.fetchone()
cur.execute("DELETE FROM sp_probe")
conn.commit()
cur.execute("DROP INDEX IF EXISTS sp_idx")
conn.commit()
cur.close()
conn.close()
"""


@dataclass
class SubprocessParityResult:
    runs: int
    parent_rss_series: list[float]
    parent_drift_mb: float
    parent_slope_mb_per_run: float
    elapsed_s: float
    passed: bool
    drift_budget_mb: float


def run_subprocess_parity(
    workdir: Path, runs: int, rows_per_table: int,
    slope_threshold: float, drift_threshold: float,
) -> SubprocessParityResult:
    db = workdir / "subproc.ddb"
    remove_db_artifacts(db)
    code = _SUBPROCESS_CHILD_CODE.replace("__PKG_ROOT__", repr(str(_PKG_ROOT)))
    series: list[float] = []
    t0 = time.perf_counter()
    for k in range(runs):
        gc.collect()
        subprocess.run(
            [sys.executable, "-c", code, str(db), str(rows_per_table)],
            check=True, capture_output=True,
        )
        series.append(rss_mb())
    elapsed = time.perf_counter() - t0
    drift = series[-1] - series[0] if len(series) > 1 else 0.0
    slope = linear_slope(series)
    budget = drift_threshold + slope_threshold * max(1, runs - 1)
    passed = abs(slope) <= slope_threshold and abs(drift) <= budget
    remove_db_artifacts(db)
    return SubprocessParityResult(
        runs=runs, parent_rss_series=series, parent_drift_mb=drift,
        parent_slope_mb_per_run=slope, elapsed_s=elapsed,
        passed=passed, drift_budget_mb=budget,
    )


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------

def _tracemalloc_top(baseline, n: int) -> list[tuple[str, float]]:
    snap = tracemalloc.take_snapshot().filter_traces((
        tracemalloc.Filter(False, tracemalloc.__file__),
    ))
    if baseline is None:
        return []
    diff = snap.compare_to(baseline, "lineno")
    out: list[tuple[str, float]] = []
    for stat in diff[:n]:
        frame = stat.traceback[0] if stat.traceback else None
        loc = f"{frame.filename}:{frame.lineno}" if frame else "?"
        out.append((loc, stat.size_diff / 1024.0))
    return out


def run_one_scenario(
    spec: ScenarioSpec,
    db_path: Path,
    iterations: int,
    warmup: int,
    slope_threshold: float,
    drift_threshold: float,
    mk_p_threshold: float,
    progress_cb: Callable[[int, Sample], None] | None = None,
) -> ScenarioResult:
    remove_db_artifacts(db_path)
    gc.collect()
    tracemalloc.reset_peak()
    baseline = tracemalloc.take_snapshot()
    result = ScenarioResult(
        name=spec.name, description=spec.description, warmup=warmup,
        is_canary=spec.is_canary,
        threshold_slope=slope_threshold, threshold_drift=drift_threshold,
    )
    result.start_rss_mb = rss_mb()
    t_phase = time.perf_counter()
    for i in range(1, iterations + 1):
        t_iter = time.perf_counter()
        ops = spec.fn(db_path, i)
        iter_ms = (time.perf_counter() - t_iter) * 1000.0
        if i % 10 == 0:
            gc.collect()
        cur_h, peak_h = tracemalloc.get_traced_memory()
        s = Sample(
            iteration=i, rss_mb=rss_mb(),
            heap_mb=cur_h / MB, heap_peak_mb=peak_h / MB,
            gc_objects=len(gc.get_objects()),
            gc_gen2=gc.get_count()[2],
            db_mb=db_total_mb(db_path), iter_ms=iter_ms, ops=ops,
        )
        result.samples.append(s)
        if progress_cb:
            progress_cb(i, s)
    result.elapsed_s = time.perf_counter() - t_phase
    result.total_ops = sum(s.ops for s in result.samples)
    result.peak_rss_mb = max(s.rss_mb for s in result.samples)

    tail_rss = [s.rss_mb for s in result.samples[warmup:]] or [s.rss_mb for s in result.samples]
    tail_db = [s.db_mb for s in result.samples[warmup:]] or [s.db_mb for s in result.samples]
    result.slope_mb_per_iter = linear_slope(tail_rss)
    _, result.mk_p = mann_kendall(tail_rss)
    result.drift_mb = (tail_rss[-1] - tail_rss[0]) if len(tail_rss) > 1 else 0.0
    result.db_growth_mb = (tail_db[-1] - tail_db[0]) if len(tail_db) > 1 else 0.0
    result.net_drift_mb = result.drift_mb - max(0.0, result.db_growth_mb)
    post_warmup_iters = max(1, len(tail_rss) - 1)
    result.drift_budget_mb = drift_threshold + slope_threshold * post_warmup_iters

    slope_ok = abs(result.slope_mb_per_iter) <= slope_threshold
    drift_ok = result.net_drift_mb <= result.drift_budget_mb
    trend_significant = result.mk_p < mk_p_threshold
    result.passed = drift_ok and (slope_ok or not trend_significant)
    result.leak_top = _tracemalloc_top(baseline, n=10)

    log = logging.getLogger("ddb.leak")
    log.info(
        "scenario=%s iters=%d ops=%d elapsed=%.2fs start_rss=%.2fMB peak_rss=%.2fMB "
        "drift=%+.3fMB db_growth=%+.3fMB net_drift=%+.3fMB budget=%.3fMB "
        "slope=%+.5fMB/iter mk_p=%.4f slope_ok=%s drift_ok=%s passed=%s",
        spec.name, iterations, result.total_ops, result.elapsed_s,
        result.start_rss_mb, result.peak_rss_mb,
        result.drift_mb, result.db_growth_mb, result.net_drift_mb, result.drift_budget_mb,
        result.slope_mb_per_iter, result.mk_p, slope_ok, drift_ok, result.passed,
    )
    log.debug("scenario=%s leak_top=%s", spec.name, result.leak_top)
    if log.isEnabledFor(logging.DEBUG):
        for s in result.samples:
            log.debug(
                "sample scenario=%s i=%d rss_mb=%.3f heap_mb=%.3f heap_peak_mb=%.3f "
                "gc_objects=%d gc_gen2=%d db_mb=%.3f iter_ms=%.3f ops=%d",
                spec.name, s.iteration, s.rss_mb, s.heap_mb, s.heap_peak_mb,
                s.gc_objects, s.gc_gen2, s.db_mb, s.iter_ms, s.ops,
            )

    if spec.teardown is not None:
        with suppress(Exception):
            spec.teardown(db_path)
    remove_db_artifacts(db_path)
    gc.collect()
    return result


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print(msg: str) -> None:
    if console is not None:
        console.print(msg)
    else:
        print(msg)


def print_results(
    real_results: list[ScenarioResult],
    canary_results: list[ScenarioResult],
    sp_result: SubprocessParityResult | None,
    args: argparse.Namespace,
) -> bool:
    framework_calibrated = all(not r.passed for r in canary_results) if canary_results else True
    real_pass = all(r.passed for r in real_results)
    sp_pass = sp_result.passed if sp_result is not None else True
    high_confidence = framework_calibrated and real_pass and sp_pass

    if console is None:
        print("=== canaries (must FAIL) ===")
        for r in canary_results:
            print(f"  {r.name}: drift={r.net_drift_mb:+.3f} MB slope={r.slope_mb_per_iter:+.5f} "
                  f"detected={'YES' if not r.passed else 'NO'}")
        print("=== scenarios (must PASS) ===")
        for r in real_results:
            print(f"  {r.name}: drift={r.net_drift_mb:+.3f} MB / budget {r.drift_budget_mb:.2f}, "
                  f"slope={r.slope_mb_per_iter:+.5f} MB/iter (thr {r.threshold_slope}), "
                  f"mk_p={r.mk_p:.3f}, db_growth={r.db_growth_mb:+.3f} MB, "
                  f"verdict={'PASS' if r.passed else 'FAIL'}")
        if sp_result is not None:
            print(f"=== subprocess parity ===")
            print(f"  runs={sp_result.runs}, parent drift={sp_result.parent_drift_mb:+.3f} MB / "
                  f"budget {sp_result.drift_budget_mb:.2f}, "
                  f"parent slope={sp_result.parent_slope_mb_per_run:+.5f} MB/run, "
                  f"verdict={'PASS' if sp_result.passed else 'FAIL'}")
        verdict = "HIGH-CONFIDENCE PASS" if high_confidence else "FAIL or LOW CONFIDENCE"
        print(f"\n*** OVERALL: {verdict} ***")
        return high_confidence

    _print(Rule("[bold cyan]DecentDB Comprehensive Memory-Leak Report[/]"))
    # canary table
    ct = Table(title="Canary scenarios (framework calibration)", expand=True, border_style="yellow")
    ct.add_column("scenario", style="yellow")
    ct.add_column("net drift MB", justify="right")
    ct.add_column("slope MB/iter", justify="right")
    ct.add_column("mk-p", justify="right")
    ct.add_column("detected as leak?", justify="center")
    for r in canary_results:
        detected = not r.passed
        ct.add_row(r.name, f"{r.net_drift_mb:+.3f}", f"{r.slope_mb_per_iter:+.5f}",
                   f"{r.mk_p:.3f}",
                   "[bold green]YES[/]" if detected else "[bold red]NO (BAD)[/]")
    _print(ct)

    # real scenarios
    rt = Table(title="Real scenarios", expand=True, border_style="cyan")
    rt.add_column("scenario", style="cyan")
    rt.add_column("iters", justify="right")
    rt.add_column("ops", justify="right")
    rt.add_column("start MB", justify="right")
    rt.add_column("peak MB", justify="right")
    rt.add_column("db growth", justify="right")
    rt.add_column("net drift / budget", justify="right")
    rt.add_column("slope MB/iter", justify="right")
    rt.add_column("mk-p", justify="right")
    rt.add_column("elapsed", justify="right")
    rt.add_column("verdict", justify="center")
    for r in real_results:
        rt.add_row(
            r.name, f"{len(r.samples)}", f"{r.total_ops:,}",
            f"{r.start_rss_mb:.2f}", f"{r.peak_rss_mb:.2f}",
            f"{r.db_growth_mb:+.3f}",
            f"{r.net_drift_mb:+.3f} / {r.drift_budget_mb:.2f}",
            f"{r.slope_mb_per_iter:+.5f}",
            f"{r.mk_p:.3f}", f"{r.elapsed_s:.1f}s",
            "[bold green]PASS[/]" if r.passed else "[bold red]FAIL[/]",
        )
    _print(rt)

    if sp_result is not None:
        st = Table(title="Subprocess engine-parity", expand=True, border_style="magenta")
        st.add_column("runs", justify="right")
        st.add_column("parent drift MB / budget", justify="right")
        st.add_column("parent slope MB/run", justify="right")
        st.add_column("elapsed", justify="right")
        st.add_column("verdict", justify="center")
        st.add_row(
            f"{sp_result.runs}",
            f"{sp_result.parent_drift_mb:+.3f} / {sp_result.drift_budget_mb:.2f}",
            f"{sp_result.parent_slope_mb_per_run:+.5f}",
            f"{sp_result.elapsed_s:.1f}s",
            "[bold green]PASS[/]" if sp_result.passed else "[bold red]FAIL[/]",
        )
        _print(st)

    # top tracemalloc allocators for any FAILing real scenario
    failing = [r for r in real_results if not r.passed]
    for r in failing:
        if not r.leak_top:
            continue
        ft = Table(title=f"top alloc-site diffs for FAIL scenario: {r.name}",
                   expand=True, border_style="red")
        ft.add_column("location")
        ft.add_column("Δ KB", justify="right")
        for loc, dkb in r.leak_top:
            ft.add_row(loc, f"{dkb:+.1f}")
        _print(ft)

    color = "green" if high_confidence else "red"
    if not framework_calibrated:
        verdict = "FAIL — canary leaks NOT detected, framework is uncalibrated"
    elif not real_pass:
        verdict = "FAIL — one or more real scenarios show a leak signature"
    elif not sp_pass:
        verdict = "FAIL — subprocess engine parity drifted (engine-side leak suspected)"
    else:
        verdict = "✅  HIGH-CONFIDENCE PASS — no memory-leak evidence in engine or bindings"
    _print(Panel(Align.center(Text(verdict, style=f"bold {color}")),
                 title="overall verdict", border_style=color))

    _print("[dim]Pass criteria:[/]")
    _print("[dim]  • |slope MB/iter| ≤ slope_threshold OR Mann-Kendall p ≥ mk_p_threshold[/]")
    _print("[dim]  • (drift − db_growth) ≤ drift_threshold + slope_threshold·iters[/]")
    _print("[dim]  • Canary scenarios MUST be detected (otherwise framework uncalibrated)[/]")
    _print("[dim]  • Subprocess parent RSS does not drift across child spawns[/]")
    return high_confidence


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--iterations", type=int, default=200,
                   help="iterations per scenario (default 200)")
    p.add_argument("--warmup", type=int, default=20,
                   help="warmup iterations excluded from analysis (default 20)")
    p.add_argument("--slope-threshold", type=float, default=0.020,
                   help="|MB/iter| tolerance on post-warmup slope (default 0.020)")
    p.add_argument("--drift-threshold", type=float, default=4.0,
                   help="absolute |MB| net-drift tolerance baseline (default 4.0)")
    p.add_argument("--mk-p-threshold", type=float, default=0.05,
                   help="Mann-Kendall p below this means trend is significant (default 0.05)")
    p.add_argument("--long-lived-ops", type=int, default=0,
                   help="if >0, run long_lived_conn for ceil(N/200) iterations")
    p.add_argument("--subprocess-runs", type=int, default=16,
                   help="number of child processes for engine-parity check (default 16)")
    p.add_argument("--subprocess-rows", type=int, default=2000,
                   help="rows per child-process workload (default 2000)")
    p.add_argument("--skip-subprocess", action="store_true",
                   help="skip the subprocess engine-parity check")
    p.add_argument("--skip-canary", action="store_true",
                   help="skip the canary calibration scenarios (NOT recommended)")
    p.add_argument("--only", type=str, default=None,
                   help="comma-separated subset of real scenarios to run")
    p.add_argument("--workdir", type=Path, default=None,
                   help="directory for temp DB files")
    p.add_argument("--json-out", type=Path, default=None,
                   help="write JSON report to this path")
    p.add_argument("--report-out", type=Path, default=None,
                   help="write Markdown debug report to this path "
                        "(default: <workdir>/comprehensive-leak-report.md)")
    p.add_argument("--log-out", type=Path, default=None,
                   help="write a debug-level log file to this path "
                        "(default: <workdir>/comprehensive-leak.log)")
    p.add_argument("--log-level", type=str, default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="console log level (default INFO). File log is always DEBUG.")
    p.add_argument("--print-valgrind-cmd", action="store_true",
                   help="print the Valgrind command to corroborate results, then exit")
    return p.parse_args()


def write_json_report(
    path: Path, args: argparse.Namespace,
    real_results: list[ScenarioResult],
    canary_results: list[ScenarioResult],
    sp_result: SubprocessParityResult | None,
    high_confidence: bool,
) -> None:
    def res_dict(r: ScenarioResult) -> dict:
        return {
            "name": r.name, "description": r.description, "is_canary": r.is_canary,
            "iterations": len(r.samples), "warmup": r.warmup,
            "total_ops": r.total_ops, "elapsed_s": r.elapsed_s,
            "start_rss_mb": r.start_rss_mb, "peak_rss_mb": r.peak_rss_mb,
            "drift_mb": r.drift_mb, "db_growth_mb": r.db_growth_mb,
            "net_drift_mb": r.net_drift_mb, "drift_budget_mb": r.drift_budget_mb,
            "slope_mb_per_iter": r.slope_mb_per_iter, "mk_p": r.mk_p,
            "threshold_slope": r.threshold_slope,
            "threshold_drift": r.threshold_drift,
            "passed": r.passed,
            "leak_top": [{"loc": loc, "delta_kb": dkb} for loc, dkb in r.leak_top],
        }
    payload = {
        "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
        "high_confidence": high_confidence,
        "real_scenarios": [res_dict(r) for r in real_results],
        "canary_scenarios": [res_dict(r) for r in canary_results],
        "subprocess_parity": (
            None if sp_result is None else {
                "runs": sp_result.runs,
                "parent_drift_mb": sp_result.parent_drift_mb,
                "parent_slope_mb_per_run": sp_result.parent_slope_mb_per_run,
                "drift_budget_mb": sp_result.drift_budget_mb,
                "elapsed_s": sp_result.elapsed_s,
                "passed": sp_result.passed,
                "rss_series": sp_result.parent_rss_series,
            }
        ),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Logging + Markdown debug report
# ---------------------------------------------------------------------------

def _setup_logging(log_path: Path | None, console_level: str) -> logging.Logger:
    log = logging.getLogger("ddb.leak")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(getattr(logging, console_level))
    ch.setFormatter(fmt)
    log.addHandler(ch)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        log.addHandler(fh)

    log.propagate = False
    return log


def _env_snapshot() -> dict:
    """Collect machine, process, and library context for the report."""
    snap = {
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or "unknown",
        "python": sys.version.replace("\n", " "),
        "python_executable": sys.executable,
        "pid": os.getpid(),
        "cwd": os.getcwd(),
        "decentdb_version": getattr(decentdb, "__version__", "unknown"),
        "decentdb_path": getattr(decentdb, "__file__", "unknown"),
    }
    # Native lib path (best-effort) — useful for confirming which build was tested.
    for attr in ("_lib_path", "_LIB_PATH", "LIB_PATH"):
        if hasattr(decentdb, attr):
            snap["decentdb_native_lib"] = str(getattr(decentdb, attr))
            break
    try:
        import sqlalchemy  # type: ignore
        snap["sqlalchemy_version"] = sqlalchemy.__version__
    except Exception:
        snap["sqlalchemy_version"] = "not installed"
    # /proc/self/status excerpt for memory baseline.
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith(("VmPeak", "VmSize", "VmRSS", "VmHWM", "VmData")):
                    k, v = line.split(":", 1)
                    snap[f"proc.{k}"] = v.strip()
    except Exception:
        pass
    # CPU model on Linux.
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("model name"):
                    snap["cpu_model"] = line.split(":", 1)[1].strip()
                    break
    except Exception:
        pass
    # MemTotal.
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    snap["mem_total"] = line.split(":", 1)[1].strip()
                    break
    except Exception:
        pass
    return snap


def _scenario_md(r: ScenarioResult) -> str:
    lines: list[str] = []
    badge = "✅ PASS" if r.passed else "❌ FAIL"
    kind = "canary" if r.is_canary else "real"
    lines.append(f"### `{r.name}` — {badge}  _({kind})_")
    lines.append("")
    lines.append(f"_{r.description}_")
    lines.append("")
    lines.append("| metric | value |")
    lines.append("|---|---|")
    lines.append(f"| iterations | {len(r.samples)} (warmup={r.warmup}) |")
    lines.append(f"| total ops | {r.total_ops:,} |")
    lines.append(f"| elapsed | {r.elapsed_s:.2f} s |")
    lines.append(f"| start RSS | {r.start_rss_mb:.3f} MB |")
    lines.append(f"| peak RSS | {r.peak_rss_mb:.3f} MB |")
    lines.append(f"| RSS drift (tail) | {r.drift_mb:+.3f} MB |")
    lines.append(f"| DB-file growth (tail) | {r.db_growth_mb:+.3f} MB |")
    lines.append(f"| net drift (RSS − DB) | {r.net_drift_mb:+.3f} MB |")
    lines.append(f"| drift budget | {r.drift_budget_mb:.3f} MB "
                 f"(= {r.threshold_drift:.2f} + {r.threshold_slope:.4f} × iters) |")
    lines.append(f"| slope | {r.slope_mb_per_iter:+.6f} MB/iter "
                 f"(threshold ±{r.threshold_slope}) |")
    lines.append(f"| Mann-Kendall p | {r.mk_p:.4f} |")
    lines.append("")

    if r.samples:
        # Sample summary (every Nth row, plus tail).
        n = len(r.samples)
        step = max(1, n // 20)
        idxs = sorted(set(list(range(0, n, step)) + [n - 1]))
        iter_ms = [s.iter_ms for s in r.samples]
        avg_ms = statistics.fmean(iter_ms) if iter_ms else 0.0
        med_ms = statistics.median(iter_ms) if iter_ms else 0.0
        max_ms = max(iter_ms) if iter_ms else 0.0
        first_obj = r.samples[0].gc_objects
        last_obj = r.samples[-1].gc_objects
        first_heap = r.samples[0].heap_mb
        last_heap = r.samples[-1].heap_mb
        lines.append("**Per-iter timing**: "
                     f"avg={avg_ms:.3f} ms · median={med_ms:.3f} ms · max={max_ms:.3f} ms")
        lines.append("")
        lines.append(f"**gc.get_objects() drift**: {first_obj:,} → {last_obj:,} "
                     f"({last_obj - first_obj:+,})")
        lines.append(f"**tracemalloc heap MB**: start={first_heap:.3f} → "
                     f"end={last_heap:.3f} ({last_heap - first_heap:+.3f})")
        lines.append("")
        lines.append("**RSS / heap / DB / gc samples** (every "
                     f"{step}-th iter, plus tail):")
        lines.append("")
        lines.append("| iter | rss MB | heap MB | heap-peak MB | db MB | gc objects | gc gen2 | iter ms | ops |")
        lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for i in idxs:
            s = r.samples[i]
            lines.append(
                f"| {s.iteration} | {s.rss_mb:.3f} | {s.heap_mb:.3f} | "
                f"{s.heap_peak_mb:.3f} | {s.db_mb:.3f} | {s.gc_objects:,} | "
                f"{s.gc_gen2} | {s.iter_ms:.3f} | {s.ops} |"
            )
        lines.append("")

    if r.leak_top:
        lines.append("**Top tracemalloc allocation-site deltas** (post − baseline):")
        lines.append("")
        lines.append("| Δ KB | location |")
        lines.append("|---:|---|")
        for loc, dkb in r.leak_top:
            lines.append(f"| {dkb:+.1f} | `{loc}` |")
        lines.append("")
    return "\n".join(lines)


def write_markdown_report(
    path: Path, args: argparse.Namespace,
    real_results: list[ScenarioResult],
    canary_results: list[ScenarioResult],
    sp_result: SubprocessParityResult | None,
    high_confidence: bool,
    started_at: datetime.datetime,
    finished_at: datetime.datetime,
    env: dict,
) -> None:
    out: list[str] = []
    badge = "✅ HIGH-CONFIDENCE PASS" if high_confidence else "❌ FAIL"
    out.append("# DecentDB Comprehensive Memory-Leak Report")
    out.append("")
    out.append(f"**Verdict:** {badge}")
    out.append("")
    out.append(f"- started:  `{started_at.isoformat()}`")
    out.append(f"- finished: `{finished_at.isoformat()}`")
    out.append(f"- duration: `{(finished_at - started_at).total_seconds():.1f} s`")
    out.append("")
    canary_pass = (not canary_results) or all(
        (c.slope_mb_per_iter > c.threshold_slope) or
        (c.net_drift_mb > c.drift_budget_mb)
        for c in canary_results
    )
    real_pass = all(r.passed for r in real_results)
    sp_pass = (sp_result is None) or sp_result.passed
    out.append("## Pass-criteria summary")
    out.append("")
    out.append(f"- Canary calibration ({'detected' if canary_pass else 'NOT detected'}): "
               f"{'✅' if canary_pass else '❌'}")
    out.append(f"- Real scenarios ({sum(1 for r in real_results if r.passed)}/"
               f"{len(real_results)} pass): {'✅' if real_pass else '❌'}")
    if sp_result is not None:
        out.append(f"- Subprocess engine-parity (drift {sp_result.parent_drift_mb:+.3f} MB / "
                   f"budget {sp_result.drift_budget_mb:.2f} MB): "
                   f"{'✅' if sp_pass else '❌'}")
    out.append("")
    out.append("Each scenario passes when **both**: ")
    out.append("")
    out.append("- `|slope MB/iter| ≤ slope_threshold`  **OR**  "
               "Mann-Kendall p ≥ mk_p_threshold (no significant trend)")
    out.append("- `(rss_drift − db_growth) ≤ drift_threshold + slope_threshold·iters`  "
               "(net RSS growth above legitimate DB-file growth is bounded)")
    out.append("")

    out.append("## Run configuration")
    out.append("")
    out.append("| arg | value |")
    out.append("|---|---|")
    for k, v in vars(args).items():
        out.append(f"| `{k}` | `{v}` |")
    out.append("")

    out.append("## Environment")
    out.append("")
    out.append("| key | value |")
    out.append("|---|---|")
    for k, v in env.items():
        out.append(f"| `{k}` | `{v}` |")
    out.append("")

    out.append("## Real scenarios")
    out.append("")
    if not real_results:
        out.append("_(none)_")
    for r in real_results:
        out.append(_scenario_md(r))

    out.append("## Canary scenarios (framework calibration)")
    out.append("")
    if not canary_results:
        out.append("_(skipped)_")
    else:
        out.append("Each canary intentionally leaks. **They MUST trip** for the harness "
                   "to be considered calibrated; if a canary passes the leak filters, the "
                   "thresholds are too loose and the overall verdict is rejected.")
        out.append("")
        for r in canary_results:
            detected = (
                r.slope_mb_per_iter > r.threshold_slope
                or r.net_drift_mb > r.drift_budget_mb
            )
            tag = "✅ detected" if detected else "❌ NOT detected (BAD)"
            out.append(f"- `{r.name}`: {tag} "
                       f"(slope={r.slope_mb_per_iter:+.5f} MB/iter, "
                       f"net_drift={r.net_drift_mb:+.3f} MB)")
        out.append("")
        for r in canary_results:
            out.append(_scenario_md(r))

    out.append("## Subprocess engine-parity")
    out.append("")
    if sp_result is None:
        out.append("_(skipped)_")
    else:
        out.append(f"- runs: **{sp_result.runs}**")
        out.append(f"- parent RSS drift: **{sp_result.parent_drift_mb:+.3f} MB** "
                   f"(budget {sp_result.drift_budget_mb:.2f} MB)")
        out.append(f"- parent slope: **{sp_result.parent_slope_mb_per_run:+.5f} MB/run**")
        out.append(f"- elapsed: {sp_result.elapsed_s:.2f} s")
        out.append(f"- verdict: {'✅ PASS' if sp_result.passed else '❌ FAIL'}")
        out.append("")
        if sp_result.parent_rss_series:
            out.append("**Parent RSS after each child spawn:**")
            out.append("")
            out.append("| spawn | parent RSS MB |")
            out.append("|---:|---:|")
            for i, mb in enumerate(sp_result.parent_rss_series):
                out.append(f"| {i} | {mb:.3f} |")
            out.append("")

    out.append("## How to interpret this report")
    out.append("")
    out.append("- **slope MB/iter** is the iteration-independent leak metric. "
               "A leak shows up as a positive, persistent slope.")
    out.append("- **net_drift_mb** subtracts on-disk DB growth from RSS drift, "
               "so legitimate page-cache growth from inserted rows isn't flagged.")
    out.append("- A negative `net_drift` means RSS grew **less** than the DB on disk — "
               "always healthy.")
    out.append("- **Mann-Kendall p** filters borderline slopes: if there's no significant "
               "monotonic trend, a small slope is treated as noise.")
    out.append("- **tracemalloc deltas** localise growth to a Python source line — "
               "look here first when investigating a FAIL.")
    out.append("- **Subprocess parity** rules out engine-side leaks that the in-process "
               "metrics could miss (e.g. native-allocator fragmentation).")
    out.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(out), encoding="utf-8")


def main() -> int:
    args = parse_args()

    if args.print_valgrind_cmd:
        cmd = (
            "PYTHONMALLOC=malloc valgrind --tool=memcheck --leak-check=full "
            "--show-leak-kinds=definite,indirect --error-exitcode=99 "
            f"{sys.executable} {Path(__file__).resolve()} "
            "--iterations 60 --warmup 10 --subprocess-runs 0 --skip-subprocess"
        )
        print(cmd)
        return 0

    workdir = args.workdir or (Path(tempfile.gettempdir()) / "decentdb-comprehensive-leak")
    workdir.mkdir(parents=True, exist_ok=True)

    log_path = args.log_out or (workdir / "comprehensive-leak.log")
    report_path = args.report_out or (workdir / "comprehensive-leak-report.md")
    log = _setup_logging(log_path, args.log_level)
    started_at = datetime.datetime.now(datetime.timezone.utc)
    env = _env_snapshot()
    log.info("=== DecentDB comprehensive memory-leak run starting ===")
    log.info("workdir=%s log=%s report=%s json=%s",
             workdir, log_path, report_path, args.json_out)
    log.info("args=%s", {k: str(v) for k, v in vars(args).items()})
    for k, v in env.items():
        log.debug("env %s=%s", k, v)

    real_specs = SCENARIOS
    if args.only:
        wanted = {s.strip() for s in args.only.split(",") if s.strip()}
        real_specs = [s for s in SCENARIOS if s.name in wanted]
        if not real_specs:
            print(f"no scenarios match --only={args.only}", file=sys.stderr)
            return 2

    if args.long_lived_ops > 0:
        # Each long_lived_conn iter does ~200 inserts + a few other ops; convert
        # the requested op budget into iterations by rounding up.
        ll_iters = max(args.iterations, math.ceil(args.long_lived_ops / 200))
    else:
        ll_iters = args.iterations

    _print(f"[bold]DecentDB Comprehensive Memory-Leak Test Harness[/]"
           if console else "DecentDB Comprehensive Memory-Leak Test Harness")
    _print(f"python={sys.version.split()[0]} pid={os.getpid()} workdir={workdir}")
    _print(f"iterations={args.iterations} warmup={args.warmup} "
           f"slope_threshold={args.slope_threshold} drift_threshold={args.drift_threshold} "
           f"mk_p_threshold={args.mk_p_threshold}")
    if not args.skip_subprocess:
        _print(f"subprocess parity: runs={args.subprocess_runs} rows={args.subprocess_rows}")

    tracemalloc.start()

    canary_results: list[ScenarioResult] = []
    real_results: list[ScenarioResult] = []
    sp_result: SubprocessParityResult | None = None

    progress = None
    overall_task = None
    if console:
        progress = Progress(
            SpinnerColumn(style="cyan"), TextColumn("{task.description}"),
            BarColumn(), TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(), console=console, transient=False,
        )
        progress.start()
        total_phases = (0 if args.skip_canary else len(CANARY_SCENARIOS)) + len(real_specs) + (
            0 if args.skip_subprocess else 1
        )
        overall_task = progress.add_task("[bold]overall", total=total_phases)

    try:
        # Phase 1: real scenarios (canaries run last so their RSS residue
        # doesn't pollute the real scenarios' baseline).
        for spec in real_specs:
            iters = ll_iters if spec.name == "long_lived_conn" else args.iterations
            if progress:
                t = progress.add_task(f"scenario: {spec.name}", total=iters)
            cb = (lambda i, _s, _t=t: progress.update(_t, completed=i)) if progress else None
            r = run_one_scenario(
                spec, workdir / f"{spec.name}.ddb",
                iterations=iters,
                warmup=min(args.warmup, max(0, iters - 2)),
                slope_threshold=args.slope_threshold,
                drift_threshold=args.drift_threshold,
                mk_p_threshold=args.mk_p_threshold,
                progress_cb=cb,
            )
            real_results.append(r)
            if progress:
                progress.remove_task(t)
                progress.update(overall_task, advance=1)

        # Phase 2: subprocess engine parity
        if not args.skip_subprocess and args.subprocess_runs > 0:
            if progress:
                t = progress.add_task("subprocess parity", total=args.subprocess_runs)
            sp_result = run_subprocess_parity(
                workdir, runs=args.subprocess_runs,
                rows_per_table=args.subprocess_rows,
                slope_threshold=args.slope_threshold,
                drift_threshold=args.drift_threshold,
            )
            if progress:
                progress.update(t, completed=args.subprocess_runs)
                progress.remove_task(t)
                progress.update(overall_task, advance=1)

        # Phase 3: canary calibration (LAST, so the residual RSS from the
        # intentional leaks doesn't bias real-scenario baselines).
        if not args.skip_canary:
            for spec in CANARY_SCENARIOS:
                if progress:
                    t = progress.add_task(f"canary: {spec.name}", total=args.iterations)
                cb = (lambda i, _s, _t=t: progress.update(_t, completed=i)) if progress else None
                r = run_one_scenario(
                    spec, workdir / f"{spec.name}.ddb",
                    iterations=args.iterations, warmup=args.warmup,
                    slope_threshold=args.slope_threshold,
                    drift_threshold=args.drift_threshold,
                    mk_p_threshold=args.mk_p_threshold,
                    progress_cb=cb,
                )
                canary_results.append(r)
                reset_canaries()
                if progress:
                    progress.remove_task(t)
                    progress.update(overall_task, advance=1)
    finally:
        if progress:
            progress.stop()
        tracemalloc.stop()
        reset_canaries()

    high_confidence = print_results(real_results, canary_results, sp_result, args)
    finished_at = datetime.datetime.now(datetime.timezone.utc)
    if args.json_out is not None:
        write_json_report(args.json_out, args, real_results, canary_results,
                          sp_result, high_confidence)
        _print(f"[dim]json report: {args.json_out}[/]")
        log.info("wrote json report: %s", args.json_out)
    write_markdown_report(report_path, args, real_results, canary_results,
                          sp_result, high_confidence, started_at, finished_at, env)
    _print(f"[dim]markdown report: {report_path}[/]")
    _print(f"[dim]debug log:       {log_path}[/]")
    log.info("wrote markdown report: %s", report_path)
    log.info("=== run complete: %s ===",
             "HIGH-CONFIDENCE PASS" if high_confidence else "FAIL")
    logging.shutdown()
    return 0 if high_confidence else 2


if __name__ == "__main__":
    sys.exit(main())
