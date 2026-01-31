#!/usr/bin/env python3
import argparse
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import sqlite3
except Exception:  # pragma: no cover
    sqlite3 = None  # type: ignore

SQLITE_TIMER_RE = re.compile(
    r"Run Time:\s+real\s+([0-9.]+)(?:\s+user\s+([0-9.]+)\s+sys\s+([0-9.]+))?"
)


@dataclass(frozen=True)
class Summary:
    n: int
    min_ms: float
    mean_ms: float
    median_ms: float
    p95_ms: float
    max_ms: float

def run_cmd(cmd: List[str], input_text: Optional[str] = None, timeout: int = 30) -> Tuple[int, str, str]:
    p = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    return p.returncode, p.stdout, p.stderr

def percentile(values: List[float], p: float) -> float:
    xs = sorted(values)
    if not xs:
        raise ValueError("No values")
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return xs[int(k)]
    return xs[f] * (c - k) + xs[c] * (k - f)

def summarize(label: str, ms: List[float]) -> None:
    if not ms:
        print(f"\n== {label} ==\n(no samples)")
        return
    out = compute_summary(ms)
    print(f"\n== {label} ==")
    print(f"n      : {out.n}")
    print(f"min    : {out.min_ms:.4f} ms")
    print(f"mean   : {out.mean_ms:.4f} ms")
    print(f"median : {out.median_ms:.4f} ms")
    print(f"p95    : {out.p95_ms:.4f} ms")
    print(f"max    : {out.max_ms:.4f} ms")


def compute_summary(ms: List[float]) -> Summary:
    if not ms:
        return Summary(n=0, min_ms=0.0, mean_ms=0.0, median_ms=0.0, p95_ms=0.0, max_ms=0.0)
    return Summary(
        n=len(ms),
        min_ms=min(ms),
        mean_ms=statistics.fmean(ms),
        median_ms=statistics.median(ms),
        p95_ms=percentile(ms, 95),
        max_ms=max(ms),
    )

def sqlite_cli_time_ms(sqlite_db: str, query: str, sqlite_path: str, timeout: int, debug: bool) -> float:
    # Important: many sqlite3 builds only emit .timer output in interactive mode.
    # Also: .output /dev/null suppresses .timer output on some builds, so we avoid it.
    script = f""".timer on
.prompt '' ''
{query}
"""
    rc, out, err = run_cmd([sqlite_path, "-interactive", sqlite_db], input_text=script, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"sqlite3 exited {rc}\nstdout:\n{out}\nstderr:\n{err}")

    # Some sqlite3 builds print the timer to stdout, others to stderr.
    combined = (out or "") + "\n" + (err or "")
    m = SQLITE_TIMER_RE.search(combined)
    if not m:
        if debug:
            raise RuntimeError(
                "Could not find sqlite timer output. Here is raw output:\n"
                f"--- stdout ---\n{out}\n--- stderr ---\n{err}\n"
            )
        raise RuntimeError("Could not find sqlite timer output (use --debug for raw stdout/stderr).")

    real_s = float(m.group(1))
    return real_s * 1000.0

def decentdb_time_ms(decentdb_path: str, ddb_path: str, query: str, timeout: int, debug: bool) -> float:
    rc, out, err = run_cmd([decentdb_path, "exec", "-d", ddb_path, "-s", query], timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"decentdb exited {rc}\nstdout:\n{out}\nstderr:\n{err}")

    s = out.strip()
    try:
        payload = json.loads(s)
        return float(payload["elapsed_ms"])
    except Exception:
        m = re.search(r'"elapsed_ms"\s*:\s*([0-9.]+)', s)
        if not m:
            if debug:
                raise RuntimeError(
                    "Could not parse elapsed_ms from DecentDB output. Raw:\n"
                    f"--- stdout ---\n{out}\n--- stderr ---\n{err}\n"
                )
            raise RuntimeError("Could not parse elapsed_ms (use --debug for raw stdout/stderr).")
        return float(m.group(1))


def maybe_import_decentdb() -> Any:
    try:
        import decentdb  # type: ignore

        return decentdb
    except Exception:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        candidate = os.path.join(repo_root, "bindings", "python")
        if candidate not in sys.path:
            sys.path.insert(0, candidate)
        import decentdb  # type: ignore

        return decentdb


def sqlite_python_time_ms(sqlite_db: str, query: str, *, fetch: str) -> float:
    if sqlite3 is None:
        raise RuntimeError("sqlite3 stdlib module unavailable")
    conn = sqlite3.connect(f"file:{sqlite_db}?mode=ro", uri=True)
    try:
        cur = conn.cursor()
        start = time.perf_counter()
        cur.execute(query)
        if fetch == "one":
            cur.fetchone()
        elif fetch == "all":
            cur.fetchall()
        end = time.perf_counter()
        return (end - start) * 1000.0
    finally:
        conn.close()


def decentdb_python_time_ms(ddb_path: str, query: str, *, fetch: str) -> float:
    decentdb = maybe_import_decentdb()
    conn = decentdb.connect(ddb_path)
    try:
        cur = conn.cursor()
        start = time.perf_counter()
        cur.execute(query)
        if fetch == "one":
            cur.fetchone()
        elif fetch == "all":
            cur.fetchall()
        end = time.perf_counter()
        return (end - start) * 1000.0
    finally:
        conn.close()


def bench_python(
    *,
    sqlite_db: str,
    ddb_path: str,
    query: str,
    iterations: int,
    warmup: int,
    fetch: str,
    open_per_iter: bool,
) -> Tuple[List[float], List[float]]:
    if fetch not in ("none", "one", "all"):
        raise ValueError("fetch must be none|one|all")
    if sqlite3 is None:
        raise RuntimeError("sqlite3 stdlib module unavailable")

    sqlite_ms: List[float] = []
    decent_ms: List[float] = []

    print(f"python mode: fetch={fetch} open_per_iter={open_per_iter}")

    if open_per_iter:
        if warmup > 0:
            print(f"\nWarmup: {warmup} runs each (not measured)…")
            for _ in range(warmup):
                _ = sqlite_python_time_ms(sqlite_db, query, fetch=fetch)
            for _ in range(warmup):
                _ = decentdb_python_time_ms(ddb_path, query, fetch=fetch)

        print(f"\nMeasured iterations: {iterations}")
        for i in range(1, iterations + 1):
            sqlite_ms.append(sqlite_python_time_ms(sqlite_db, query, fetch=fetch))
            decent_ms.append(decentdb_python_time_ms(ddb_path, query, fetch=fetch))
            if i % max(1, (iterations // 10)) == 0:
                print(f"  progress: {i}/{iterations}")

        return sqlite_ms, decent_ms

    # Open once and reuse connections/cursors.
    decentdb = maybe_import_decentdb()
    sqlite_conn = sqlite3.connect(f"file:{sqlite_db}?mode=ro", uri=True)
    decent_conn = decentdb.connect(ddb_path)
    try:
        sqlite_cur = sqlite_conn.cursor()
        decent_cur = decent_conn.cursor()

        if warmup > 0:
            print(f"\nWarmup: {warmup} runs each (not measured)…")
            for _ in range(warmup):
                sqlite_cur.execute(query)
                if fetch == "one":
                    sqlite_cur.fetchone()
                elif fetch == "all":
                    sqlite_cur.fetchall()
            for _ in range(warmup):
                decent_cur.execute(query)
                if fetch == "one":
                    decent_cur.fetchone()
                elif fetch == "all":
                    decent_cur.fetchall()

        print(f"\nMeasured iterations: {iterations}")
        for i in range(1, iterations + 1):
            start = time.perf_counter()
            sqlite_cur.execute(query)
            if fetch == "one":
                sqlite_cur.fetchone()
            elif fetch == "all":
                sqlite_cur.fetchall()
            end = time.perf_counter()
            sqlite_ms.append((end - start) * 1000.0)

            start = time.perf_counter()
            decent_cur.execute(query)
            if fetch == "one":
                decent_cur.fetchone()
            elif fetch == "all":
                decent_cur.fetchall()
            end = time.perf_counter()
            decent_ms.append((end - start) * 1000.0)

            if i % max(1, (iterations // 10)) == 0:
                print(f"  progress: {i}/{iterations}")

        return sqlite_ms, decent_ms
    finally:
        sqlite_conn.close()
        decent_conn.close()


def write_json_report(
    out_path: str,
    benches: Sequence[Tuple[str, List[float]]],
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    doc: Dict[str, Any] = {
        "generated_at": time.time(),
        "benchmarks": [],
    }
    if extra:
        doc.update(extra)
    for name, samples in benches:
        s = compute_summary(samples)
        doc["benchmarks"].append(
            {
                "name": name,
                "iterations": s.n,
                "p50_ms": s.median_ms,
                "p95_ms": s.p95_ms,
                "min_ms": s.min_ms,
                "mean_ms": s.mean_ms,
                "max_ms": s.max_ms,
            }
        )
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)

def main() -> int:
    ap = argparse.ArgumentParser(description="Benchmark SQLite CLI (.timer) vs DecentDB exec elapsed_ms.")
    ap.add_argument("--sqlite-db", required=True)
    ap.add_argument("--ddb", required=True)
    ap.add_argument("--query", required=True)
    ap.add_argument("--mode", choices=["cli", "python"], default="cli")
    ap.add_argument("--fetch", choices=["none", "one", "all"], default="none")
    ap.add_argument(
        "--python-open-per-iter",
        action="store_true",
        help="In python mode, include connect/close in each iteration",
    )
    ap.add_argument("--iterations", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=10)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--sqlite-path", default="sqlite3")
    ap.add_argument("--decentdb-path", default="./decentdb")
    ap.add_argument("--sleep-ms", type=float, default=0.0)
    ap.add_argument("--debug", action="store_true", help="Show raw stdout/stderr on parse failures")
    ap.add_argument("--out-json", default=None, help="Write JSON report (p50/p95/etc)")
    args = ap.parse_args()

    # Always print something immediately so you know it started.
    print("bench_sqlite_vs_decentdb: starting…")
    print(f"sqlite3 : {args.sqlite_path}")
    print(f"db      : {args.sqlite_db}")
    print(f"decentdb: {args.decentdb_path}")
    print(f"ddb     : {args.ddb}")
    print(f"query   : {args.query}")
    print(f"warmup  : {args.warmup}")
    print(f"iters   : {args.iterations}")

    query = args.query.strip()
    if not query.endswith(";"):
        query += ";"

    sqlite_ms: List[float]
    decent_ms: List[float]

    # Quick probe so failures are obvious right away.
    print("\nProbe (1 run each)…")
    if args.mode == "cli":
        s0 = sqlite_cli_time_ms(args.sqlite_db, query, args.sqlite_path, args.timeout, args.debug)
        d0 = decentdb_time_ms(args.decentdb_path, args.ddb, query, args.timeout, args.debug)
        print(f"SQLite  : {s0:.4f} ms")
        print(f"DecentDB: {d0:.4f} ms")
    else:
        s0 = sqlite_python_time_ms(args.sqlite_db, query, fetch=args.fetch)
        d0 = decentdb_python_time_ms(args.ddb, query, fetch=args.fetch)
        print(f"SQLite  : {s0:.4f} ms")
        print(f"DecentDB: {d0:.4f} ms")

    if args.mode == "cli" and args.fetch != "none":
        print("warning: --fetch is ignored in --mode=cli", file=sys.stderr)

    if args.mode == "cli":
        # Warmup
        if args.warmup > 0:
            print(f"\nWarmup: {args.warmup} runs each (not measured)…")
            for _ in range(args.warmup):
                sqlite_cli_time_ms(args.sqlite_db, query, args.sqlite_path, args.timeout, args.debug)
            for _ in range(args.warmup):
                decentdb_time_ms(args.decentdb_path, args.ddb, query, args.timeout, args.debug)

        sqlite_ms = []
        decent_ms = []

        print(f"\nMeasured iterations: {args.iterations}")
        for i in range(1, args.iterations + 1):
            sqlite_ms.append(sqlite_cli_time_ms(args.sqlite_db, query, args.sqlite_path, args.timeout, args.debug))
            decent_ms.append(decentdb_time_ms(args.decentdb_path, args.ddb, query, args.timeout, args.debug))

            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

            if i % max(1, (args.iterations // 10)) == 0:
                print(f"  progress: {i}/{args.iterations}")

        summarize("SQLite CLI (.timer real)", sqlite_ms)
        summarize("DecentDB (elapsed_ms)", decent_ms)
    else:
        sqlite_ms, decent_ms = bench_python(
            sqlite_db=args.sqlite_db,
            ddb_path=args.ddb,
            query=query,
            iterations=args.iterations,
            warmup=args.warmup,
            fetch=args.fetch,
            open_per_iter=args.python_open_per_iter,
        )
        summarize(f"SQLite Python (fetch={args.fetch})", sqlite_ms)
        summarize(f"DecentDB Python (fetch={args.fetch})", decent_ms)

    s_med = statistics.median(sqlite_ms)
    d_med = statistics.median(decent_ms)
    ratio = (d_med / s_med) if s_med > 0 else float("inf")
    print("\n== Median comparison ==")
    print(f"SQLite median  : {s_med:.4f} ms")
    print(f"DecentDB median: {d_med:.4f} ms")
    print(f"Ratio (DecentDB / SQLite): {ratio:.3f}x")

    if args.out_json:
        extra: Dict[str, Any] = {
            "mode": args.mode,
            "query": query,
        }
        if args.mode == "python":
            extra["fetch"] = args.fetch
            extra["python_open_per_iter"] = bool(args.python_open_per_iter)
        write_json_report(
            args.out_json,
            benches=[
                (f"sqlite_{args.mode}", sqlite_ms),
                (f"decentdb_{args.mode}", decent_ms),
            ],
            extra=extra,
        )
        print(f"\nWrote JSON report: {args.out_json}")

    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as e:
        # Ensure you always see *something* if it fails early.
        print(f"\nERROR: {e}", file=sys.stderr)
        raise
