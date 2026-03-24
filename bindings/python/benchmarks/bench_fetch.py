"""
Benchmark script for comparing fetch performance in DecentDB Python bindings.

This script measures the time to retrieve 1M rows using:
- fetchall(): retrieves all rows in a single call
- fetchmany(batch_size): retrieves rows in batches (default 1000)

Usage:
    python bench_fetch.py
"""

import argparse
import os
import time

import decentdb


def remove_if_exists(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def row_iter(count):
    for i in range(count):
        yield (i, f"value_{i}", float(i))


def run_benchmark(db_path, count, fetchmany_batch):
    """
    Run fetch performance benchmarks.

    Creates a temporary database, inserts rows in one explicit transaction, then
    measures fetchall() and fetchmany(batch_size). Cleans database files up.
    """
    wal_path = f"{db_path}.wal"
    remove_if_exists(db_path)
    remove_if_exists(wal_path)

    print("Setting up data...")
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)")
    start_time = time.perf_counter()
    cur.execute("BEGIN")
    try:
        cur.executemany("INSERT INTO bench VALUES (?, ?, ?)", row_iter(count))
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise
    end_time = time.perf_counter()
    print(f"Insert {count} rows: {end_time - start_time:.4f}s")

    cur = conn.cursor()
    print("Benchmarking fetchall...")
    start_time = time.perf_counter()
    cur.execute("SELECT * FROM bench")
    rows = cur.fetchall()
    end_time = time.perf_counter()
    print(f"Fetchall {count} rows: {end_time - start_time:.4f}s")
    assert len(rows) == count

    cur = conn.cursor()
    print(f"Benchmarking fetchmany({fetchmany_batch})...")
    start_time = time.perf_counter()
    cur.execute("SELECT * FROM bench")
    total = 0
    while True:
        batch = cur.fetchmany(fetchmany_batch)
        if not batch:
            break
        total += len(batch)
    end_time = time.perf_counter()
    print(f"Fetchmany({fetchmany_batch}) {count} rows: {end_time - start_time:.4f}s")
    assert total == count

    conn.close()
    remove_if_exists(db_path)
    remove_if_exists(wal_path)


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark DecentDB Python fetch performance")
    parser.add_argument(
        "--count",
        type=int,
        default=1_000_000,
        help="Number of rows to insert and fetch (default: 1000000)",
    )
    parser.add_argument(
        "--fetchmany-batch",
        type=int,
        default=1000,
        help="Batch size for fetchmany benchmark (default: 1000)",
    )
    parser.add_argument(
        "--db-path",
        default="bench_fetch.ddb",
        help="Temporary benchmark database path (default: bench_fetch.ddb)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_benchmark(
        db_path=args.db_path,
        count=args.count,
        fetchmany_batch=args.fetchmany_batch,
    )
