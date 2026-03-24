"""
Apples-to-apples Python benchmark for DecentDB vs sqlite3.

Workloads are identical for both engines:
- Insert loop in one explicit transaction (parameterized single-row INSERT)
- Full table scan via fetchall()
- Full table scan via fetchmany(batch_size)
- Seeded random point lookups by indexed id (p50/p95 latency)

Usage:
    python benchmarks/bench_fetch.py
    python benchmarks/bench_fetch.py --engine decentdb
    python benchmarks/bench_fetch.py --count 1000000 --point-reads 20000
"""

import argparse
import os
import random
import sqlite3
import time

import decentdb
from decentdb.native import load_library as load_decentdb_library

def remove_if_exists(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def cleanup_db_files(base_path):
    # Handle both DecentDB and SQLite sidecars.
    remove_if_exists(base_path)
    remove_if_exists(base_path + ".wal")
    remove_if_exists(base_path + "-wal")
    remove_if_exists(base_path + "-shm")


def row_iter(count):
    for i in range(count):
        yield (i, f"value_{i}", float(i))


def percentile_sorted(sorted_values, pct):
    if not sorted_values:
        return 0.0
    idx = int(round((pct / 100.0) * (len(sorted_values) - 1)))
    idx = min(max(idx, 0), len(sorted_values) - 1)
    return sorted_values[idx]


def build_point_read_ids(row_count, point_reads, point_seed):
    rng = random.Random(point_seed)
    if point_reads <= row_count:
        return rng.sample(range(row_count), point_reads)
    ids = [rng.randrange(row_count) for _ in range(point_reads)]
    return ids


def to_ms(ns):
    return ns / 1_000_000.0


def setup_decentdb(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)")
    cur.execute("CREATE INDEX bench_id_idx ON bench(id)")
    return conn


def setup_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=FULL")
    cur.execute("PRAGMA wal_autocheckpoint=0")
    cur.execute("CREATE TABLE bench (id INTEGER, val TEXT, f REAL)")
    cur.execute("CREATE INDEX bench_id_idx ON bench(id)")
    return conn


def run_engine_benchmark(
    engine_name,
    db_path,
    count,
    fetchmany_batch,
    point_reads,
    point_seed,
    keep_db,
):
    cleanup_db_files(db_path)
    print(f"\n=== {engine_name} ===")
    print("Setting up data...")

    if engine_name == "decentdb":
        lib = load_decentdb_library()
        lib_path = getattr(lib, "_name", "<unknown>")
        print(f"DecentDB native library: {lib_path}")
        conn = setup_decentdb(db_path)
    elif engine_name == "sqlite":
        conn = setup_sqlite(db_path)
    else:
        raise ValueError(f"Unknown engine: {engine_name}")

    insert_cur = conn.cursor()
    started = time.perf_counter()
    insert_cur.execute("BEGIN")
    try:
        insert_cur.executemany("INSERT INTO bench VALUES (?, ?, ?)", row_iter(count))
        insert_cur.execute("COMMIT")
    except Exception:
        insert_cur.execute("ROLLBACK")
        raise
    insert_s = time.perf_counter() - started
    insert_rps = count / insert_s
    print(f"Insert {count} rows: {insert_s:.4f}s ({insert_rps:,.2f} rows/sec)")

    fetchall_cur = conn.cursor()
    started = time.perf_counter()
    fetchall_cur.execute("SELECT id, val, f FROM bench")
    rows = fetchall_cur.fetchall()
    fetchall_s = time.perf_counter() - started
    if len(rows) != count:
        raise AssertionError(f"Expected {count} rows from fetchall, got {len(rows)}")
    print(f"Fetchall {count} rows: {fetchall_s:.4f}s")

    fetchmany_cur = conn.cursor()
    started = time.perf_counter()
    fetchmany_cur.execute("SELECT id, val, f FROM bench")
    total = 0
    while True:
        batch = fetchmany_cur.fetchmany(fetchmany_batch)
        if not batch:
            break
        total += len(batch)
    fetchmany_s = time.perf_counter() - started
    if total != count:
        raise AssertionError(f"Expected {count} rows from fetchmany, got {total}")
    print(f"Fetchmany({fetchmany_batch}) {count} rows: {fetchmany_s:.4f}s")

    point_cur = conn.cursor()
    point_sql = "SELECT id, val, f FROM bench WHERE id = ?"
    point_ids = build_point_read_ids(count, point_reads, point_seed)
    warmup_id = point_ids[len(point_ids) // 2]
    point_cur.execute(point_sql, (warmup_id,))
    if point_cur.fetchone() is None:
        raise AssertionError("Warmup point read missed expected row")

    latencies_ns = []
    for lookup_id in point_ids:
        started_ns = time.perf_counter_ns()
        point_cur.execute(point_sql, (lookup_id,))
        row = point_cur.fetchone()
        elapsed_ns = time.perf_counter_ns() - started_ns
        if row is None:
            raise AssertionError(f"Point read missed id={lookup_id}")
        latencies_ns.append(elapsed_ns)
    latencies_ns.sort()
    p50_ms = to_ms(percentile_sorted(latencies_ns, 50))
    p95_ms = to_ms(percentile_sorted(latencies_ns, 95))
    print(
        f"Random point reads by id ({point_reads}, seed={point_seed}): "
        f"p50={p50_ms:.6f}ms p95={p95_ms:.6f}ms"
    )

    if engine_name == "sqlite":
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    if not keep_db:
        cleanup_db_files(db_path)

    return {
        "insert_s": insert_s,
        "insert_rps": insert_rps,
        "fetchall_s": fetchall_s,
        "fetchmany_s": fetchmany_s,
        "point_p50_ms": p50_ms,
        "point_p95_ms": p95_ms,
    }


def print_comparison(results):
    if "decentdb" not in results or "sqlite" not in results:
        return

    d = results["decentdb"]
    s = results["sqlite"]

    metrics = [
        {
            "name": "Insert throughput",
            "decent": d["insert_rps"],
            "sqlite": s["insert_rps"],
            "unit": " rows/s",
            "higher_is_better": True,
            "fmt": ".2f",
        },
        {
            "name": "Fetchall time",
            "decent": d["fetchall_s"],
            "sqlite": s["fetchall_s"],
            "unit": "s",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Fetchmany time",
            "decent": d["fetchmany_s"],
            "sqlite": s["fetchmany_s"],
            "unit": "s",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Point read p50",
            "decent": d["point_p50_ms"],
            "sqlite": s["point_p50_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Point read p95",
            "decent": d["point_p95_ms"],
            "sqlite": s["point_p95_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
    ]

    decent_better = []
    sqlite_better = []
    ties = []

    for metric in metrics:
        name = metric["name"]
        decent = metric["decent"]
        sqlite = metric["sqlite"]
        unit = metric["unit"]
        fmt = metric["fmt"]
        higher_is_better = metric["higher_is_better"]

        if decent == sqlite:
            ties.append(f"{name}: tie ({decent:{fmt}}{unit})")
            continue

        if higher_is_better:
            decent_wins = decent > sqlite
            winner_val = decent if decent_wins else sqlite
            loser_val = sqlite if decent_wins else decent
            ratio = winner_val / loser_val if loser_val else float("inf")
            detail = (
                f"{name}: {winner_val:{fmt}}{unit} vs {loser_val:{fmt}}{unit} "
                f"({ratio:.3f}x higher)"
            )
        else:
            decent_wins = decent < sqlite
            winner_val = decent if decent_wins else sqlite
            loser_val = sqlite if decent_wins else decent
            ratio = loser_val / winner_val if winner_val else float("inf")
            detail = (
                f"{name}: {winner_val:{fmt}}{unit} vs {loser_val:{fmt}}{unit} "
                f"({ratio:.3f}x faster/lower)"
            )

        if decent_wins:
            decent_better.append(detail)
        else:
            sqlite_better.append(detail)

    print("\n=== Comparison (DecentDB vs SQLite) ===")
    print("DecentDB better at:")
    if decent_better:
        for line in decent_better:
            print(f"- {line}")
    else:
        print("- none")

    print("SQLite better at:")
    if sqlite_better:
        for line in sqlite_better:
            print(f"- {line}")
    else:
        print("- none")

    if ties:
        print("Ties:")
        for line in ties:
            print(f"- {line}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fair Python benchmark: DecentDB bindings vs sqlite3"
    )
    parser.add_argument(
        "--engine",
        choices=["all", "decentdb", "sqlite"],
        default="all",
        help="Engine to run (default: all)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1_000_000,
        help="Number of rows to insert/fetch (default: 1000000)",
    )
    parser.add_argument(
        "--fetchmany-batch",
        type=int,
        default=1000,
        help="Batch size for fetchmany benchmark (default: 1000)",
    )
    parser.add_argument(
        "--point-reads",
        type=int,
        default=10_000,
        help="Number of random indexed point lookups by id (default: 10000)",
    )
    parser.add_argument(
        "--point-seed",
        type=int,
        default=1337,
        help="RNG seed for random point-id sampling (default: 1337)",
    )
    parser.add_argument(
        "--db-prefix",
        default="bench_fetch",
        help="Database file prefix (default: bench_fetch)",
    )
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="Keep generated database files after benchmark run",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    engines = (
        ["decentdb", "sqlite"]
        if args.engine == "all"
        else [args.engine]
    )
    results = {}

    for engine in engines:
        suffix = "ddb" if engine == "decentdb" else "db"
        path = f"{args.db_prefix}_{engine}.{suffix}"
        results[engine] = run_engine_benchmark(
            engine_name=engine,
            db_path=path,
            count=args.count,
            fetchmany_batch=args.fetchmany_batch,
            point_reads=args.point_reads,
            point_seed=args.point_seed,
            keep_db=args.keep_db,
        )

    print_comparison(results)


if __name__ == "__main__":
    main()
