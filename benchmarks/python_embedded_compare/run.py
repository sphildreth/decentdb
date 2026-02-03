#!/usr/bin/env python3

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _now_ns() -> int:
    return time.perf_counter_ns()


def _pctl(sorted_values: Sequence[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if pct <= 0:
        return float(sorted_values[0])
    if pct >= 100:
        return float(sorted_values[-1])
    idx = int(round((pct / 100.0) * (len(sorted_values) - 1)))
    idx = max(0, min(len(sorted_values) - 1, idx))
    return float(sorted_values[idx])


def _read_text_best_effort(path: str, max_bytes: int = 64 * 1024) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            data = f.read(max_bytes)
        return data.decode("utf-8", errors="replace")
    except Exception:
        return None


def _git_rev() -> Optional[str]:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd="/repo", stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except Exception:
        return None


def _pkg_version(module_name: str) -> Optional[str]:
    try:
        mod = __import__(module_name)
        return getattr(mod, "__version__", None)
    except Exception:
        return None


def _cpu_model_name() -> Optional[str]:
    txt = _read_text_best_effort("/proc/cpuinfo")
    if not txt:
        return None
    for line in txt.splitlines():
        if line.lower().startswith("model name"):
            parts = line.split(":", 1)
            if len(parts) == 2:
                return parts[1].strip()
    return None


@dataclasses.dataclass(frozen=True)
class Sample:
    elapsed_ns: int


@dataclasses.dataclass
class Metric:
    unit: str
    samples: List[Sample]

    def summarize_us_per_op(self, n_ops: int) -> Dict[str, float]:
        us = [s.elapsed_ns / n_ops / 1_000.0 for s in self.samples]
        us_sorted = sorted(us)
        return {
            "p50_us_per_op": _pctl(us_sorted, 50),
            "p95_us_per_op": _pctl(us_sorted, 95),
            "mean_us_per_op": statistics.fmean(us_sorted) if us_sorted else 0.0,
            "min_us_per_op": float(us_sorted[0]) if us_sorted else 0.0,
            "max_us_per_op": float(us_sorted[-1]) if us_sorted else 0.0,
        }


class BenchError(RuntimeError):
    pass


class DbEngine:
    name: str

    def open(self, db_path: str):
        raise NotImplementedError

    def close(self, conn) -> None:
        try:
            conn.close()
        except Exception:
            pass

    def setup_schema(self, conn) -> None:
        cur = conn.cursor()
        cur.execute("CREATE TABLE kv (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")

    def begin(self, conn) -> None:
        conn.cursor().execute("BEGIN")

    def commit(self, conn) -> None:
        conn.cursor().execute("COMMIT")

    def seed(self, conn, rows: int) -> None:
        cur = conn.cursor()
        self.begin(conn)
        for i in range(1, rows + 1):
            cur.execute("INSERT INTO kv (id, v) VALUES (?, ?)", (i, i))
        self.commit(conn)

    def point_select(self, conn, ids: Sequence[int]) -> None:
        cur = conn.cursor()
        for idv in ids:
            cur.execute("SELECT v FROM kv WHERE id = ?", (idv,))
            row = cur.fetchone()
            if row is None:
                raise BenchError("missing row")
            _ = row[0]

    def insert_txn(self, conn, start_id: int, n_ops: int) -> None:
        cur = conn.cursor()
        self.begin(conn)
        for i in range(n_ops):
            idv = start_id + i
            cur.execute("INSERT INTO kv (id, v) VALUES (?, ?)", (idv, idv))
        self.commit(conn)


class DecentDbEngine(DbEngine):
    def __init__(self, name: str = "DecentDB"):
        self.name = name

    def open(self, db_path: str):
        import decentdb

        # Keep stmt cache on; SQLite has statement caching too.
        return decentdb.connect(db_path)

    def setup_schema(self, conn) -> None:
        # DecentDB has a fast path for `INT64 PRIMARY KEY` (rowid-optimized).
        # Using `INTEGER PRIMARY KEY` here would parse, but can miss that
        # optimization depending on dialect/type mapping.
        cur = conn.cursor()
        cur.execute("CREATE TABLE kv (id INT64 PRIMARY KEY, v INT64 NOT NULL)")


class SQLiteEngine(DbEngine):
    def __init__(self, variant: str):
        self.variant = variant
        self.name = f"SQLite({variant})"

    def open(self, db_path: str):
        import sqlite3

        conn = sqlite3.connect(db_path, isolation_level=None)
        # Fairness knobs: explicitly set FK enforcement and the chosen variant.
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        if self.variant == "wal_full":
            conn.execute("PRAGMA synchronous=FULL")
        elif self.variant == "wal_normal":
            conn.execute("PRAGMA synchronous=NORMAL")
        else:
            raise BenchError(f"unknown sqlite variant: {self.variant}")
        return conn


class DuckDbEngine(DbEngine):
    def __init__(self):
        self.name = "DuckDB"

    def open(self, db_path: str):
        import duckdb

        return duckdb.connect(db_path)


class JdbcEngine(DbEngine):
    def __init__(
        self,
        name: str,
        driver_class: str,
        jdbc_url: str,
        jar_path: str,
    ):
        self.name = name
        self._driver_class = driver_class
        self._jdbc_url = jdbc_url
        self._jar_path = jar_path

    def open(self, db_path: str):
        import jaydebeapi

        if not os.path.exists(self._jar_path):
            raise BenchError(f"missing jar: {self._jar_path}")

        url = self._jdbc_url.format(path=db_path)

        conn = jaydebeapi.connect(
            self._driver_class,
            url,
            [],
            self._jar_path,
        )

        # Best-effort: disable auto-commit so BEGIN/COMMIT is meaningful.
        try:
            conn.jconn.setAutoCommit(False)
        except Exception:
            pass

        return conn

    def setup_schema(self, conn) -> None:
        # Derby/HSQLDB can error if table exists; use a fresh db path per run.
        super().setup_schema(conn)


def _lcg_ids(n: int, modulo: int, seed: int = 0xC0FFEE) -> List[int]:
    # Deterministic pseudo-random ids in [1..modulo]
    a = 1664525
    c = 1013904223
    m = 2**32
    x = seed & 0xFFFFFFFF
    out: List[int] = []
    for _ in range(n):
        x = (a * x + c) % m
        out.append((x % modulo) + 1)
    return out


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _rm_tree(path: str) -> None:
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        return


def _manifest(extra: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": {
            "python": sys.version,
            "os": platform.platform(),
            "uname": " ".join(platform.uname()),
            "cpu_model": _cpu_model_name(),
        },
        "repo": {
            "git_rev": _git_rev(),
        },
        "deps": {
            "duckdb": _pkg_version("duckdb"),
            "jaydebeapi": _pkg_version("jaydebeapi"),
            "jpype": _pkg_version("jpype"),
            "matplotlib": _pkg_version("matplotlib"),
        },
        **extra,
    }


def _run_one(
    engine: DbEngine,
    bench: str,
    n_ops: int,
    iterations: int,
    warmup: int,
    db_dir: str,
) -> Dict[str, Any]:
    # Fresh DB per (engine, bench, n_ops) so write amplification doesn’t accumulate.
    safe_name = re.sub(r"[^a-zA-Z0-9_-]+", "_", engine.name)
    db_path = os.path.join(db_dir, f"bench_{safe_name}_{bench}_{n_ops}.db")

    # Remove common sidecar files.
    for p in [db_path, db_path + "-wal", db_path + "-shm", db_path + ".mv.db", db_path + ".trace.db", db_path + ".lck"]:
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    conn = engine.open(db_path)
    try:
        engine.setup_schema(conn)

        # Seed table for read benchmark.
        if bench == "point_select":
            seed_rows = max(n_ops, 100_000)
            engine.seed(conn, seed_rows)
            ids = _lcg_ids(n_ops, modulo=seed_rows)

            def action() -> None:
                engine.point_select(conn, ids)

        elif bench == "insert_txn":
            # Inserts dominate file growth; start IDs at 1.
            def action() -> None:
                engine.insert_txn(conn, start_id=1, n_ops=n_ops)

        else:
            raise BenchError(f"unknown bench: {bench}")

        # Warmup
        for _ in range(warmup):
            action()

            # Reset DB state between warmup iterations for insert bench.
            if bench == "insert_txn":
                engine.close(conn)
                # Start over
                for p in [db_path, db_path + "-wal", db_path + "-shm"]:
                    try:
                        os.remove(p)
                    except FileNotFoundError:
                        pass
                conn = engine.open(db_path)
                engine.setup_schema(conn)

        samples: List[Sample] = []
        for _ in range(iterations):
            t0 = _now_ns()
            action()
            t1 = _now_ns()
            samples.append(Sample(elapsed_ns=t1 - t0))

            if bench == "insert_txn":
                engine.close(conn)
                for p in [db_path, db_path + "-wal", db_path + "-shm"]:
                    try:
                        os.remove(p)
                    except FileNotFoundError:
                        pass
                conn = engine.open(db_path)
                engine.setup_schema(conn)

        metric = Metric(unit="us/op", samples=samples)
        summary = metric.summarize_us_per_op(n_ops)

        return {
            "engine": engine.name,
            "bench": bench,
            "n_ops": n_ops,
            "unit": "us/op",
            "samples_elapsed_ns": [s.elapsed_ns for s in samples],
            **summary,
        }
    finally:
        engine.close(conn)


def _plot(results: List[Dict[str, Any]], out_path: str) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        raise BenchError(
            "Plotting requires matplotlib. Install it or run without --plot. "
            f"Original error: {e}"
        )

    benches = sorted({r["bench"] for r in results})

    # One figure per bench, stacked vertically.
    fig, axes = plt.subplots(len(benches), 1, figsize=(10, 4 * max(1, len(benches))))
    if len(benches) == 1:
        axes = [axes]

    for ax, bench in zip(axes, benches):
        bench_rows = [r for r in results if r["bench"] == bench]
        engines = sorted({r["engine"] for r in bench_rows})
        for eng in engines:
            rows = sorted([r for r in bench_rows if r["engine"] == eng], key=lambda x: int(x["n_ops"]))
            xs = [int(r["n_ops"]) for r in rows]
            ys = [float(r["p50_us_per_op"]) for r in rows]
            ax.plot(xs, ys, marker="o", label=eng)

        ax.set_xscale("log")
        ax.set_xlabel("Operation count")
        ax.set_ylabel("p50 µs/op")
        ax.set_title(bench)
        ax.grid(True, which="both", linestyle=":", linewidth=0.7)
        ax.legend(loc="best")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db-dir", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--plot", default=None)
    p.add_argument("--op-counts", default="10000,100000,1000000")
    p.add_argument("--iterations", type=int, default=7)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--benches", default="point_select,insert_txn")
    p.add_argument("--engines", default="all")

    args = p.parse_args(argv)

    _ensure_dir(args.db_dir)
    _ensure_dir(os.path.dirname(args.out) or ".")

    op_counts = [int(x.strip()) for x in args.op_counts.split(",") if x.strip()]
    benches = [x.strip() for x in args.benches.split(",") if x.strip()]

    engines: List[DbEngine] = []

    # Native/Python engines
    engines.append(DecentDbEngine())
    engines.append(SQLiteEngine("wal_normal"))
    engines.append(SQLiteEngine("wal_full"))

    try:
        import duckdb  # noqa: F401

        engines.append(DuckDbEngine())
    except Exception:
        pass

    # JDBC engines (optional; included if jars + deps exist)
    try:
        import jaydebeapi  # noqa: F401

        h2_jar = os.environ.get("JDBC_H2_JAR", "/opt/jdbc/h2.jar")
        hsqldb_jar = os.environ.get("JDBC_HSQLDB_JAR", "/opt/jdbc/hsqldb.jar")
        derby_jar = os.environ.get("JDBC_DERBY_JAR", "/opt/jdbc/derby.jar")

        engines.extend(
            [
                JdbcEngine(
                    name="H2(JDBC)",
                    driver_class="org.h2.Driver",
                    jdbc_url="jdbc:h2:file:{path};MODE=PostgreSQL;DATABASE_TO_UPPER=false;AUTO_SERVER=FALSE",
                    jar_path=h2_jar,
                ),
                JdbcEngine(
                    name="Derby(JDBC)",
                    driver_class="org.apache.derby.jdbc.EmbeddedDriver",
                    jdbc_url="jdbc:derby:{path};create=true",
                    jar_path=derby_jar,
                ),
                JdbcEngine(
                    name="HSQLDB(JDBC)",
                    driver_class="org.hsqldb.jdbc.JDBCDriver",
                    jdbc_url="jdbc:hsqldb:file:{path};shutdown=true",
                    jar_path=hsqldb_jar,
                ),
            ]
        )
    except Exception:
        pass

    if args.engines != "all":
        wanted = {x.strip() for x in args.engines.split(",") if x.strip()}
        engines = [e for e in engines if e.name in wanted]

    run_manifest = _manifest(
        {
            "bench": {
                "op_counts": op_counts,
                "benches": benches,
                "iterations": args.iterations,
                "warmup": args.warmup,
                "db_dir": args.db_dir,
            }
        }
    )

    results: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for engine in engines:
        for bench in benches:
            for n_ops in op_counts:
                try:
                    row = _run_one(engine, bench, n_ops, args.iterations, args.warmup, args.db_dir)
                    results.append(row)
                    print(f"OK {engine.name} {bench} n={n_ops} p50={row['p50_us_per_op']:.3f} us/op")
                except Exception as e:
                    skipped.append({"engine": engine.name, "bench": bench, "n_ops": n_ops, "error": str(e)})
                    print(f"SKIP {engine.name} {bench} n={n_ops}: {e}")

    payload = {
        "manifest": run_manifest,
        "results": results,
        "skipped": skipped,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)

    if args.plot:
        try:
            _plot(results, args.plot)
        except BenchError as e:
            print(f"WARN plot skipped: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
