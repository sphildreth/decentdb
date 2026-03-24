"""
Benchmark script for comparing fetch performance in DecentDB Python bindings.

This script measures the time to retrieve 1M rows using:
- fetchall(): retrieves all rows in a single call
- fetchmany(batch_size): retrieves rows in batches (default 1000)

Usage:
    python bench_fetch.py
"""

import decentdb
import time
import os
import sys


def run_benchmark():
    """
    Run fetch performance benchmarks.

    Creates a temporary database, inserts 1M rows, then measures
    the time to fetch all rows using fetchall() and fetchmany().
    The database is cleaned up after the benchmark completes.
    """
    db_path = "bench_fetch.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = decentdb.connect(db_path)
    cur = conn.cursor()

    # Setup: create table and insert test data
    print("Setting up data...")
    cur.execute("CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)")

    # Prepare 1M rows for insertion
    # Note: Using executemany could be more efficient for batch inserts
    count = 1000000
    data = [(i, f"value_{i}", float(i)) for i in range(count)]

    # Insert rows within a transaction for durability
    start_time = time.perf_counter()
    cur.execute("BEGIN")
    for row in data:
        cur.execute("INSERT INTO bench VALUES (?, ?, ?)", row)
    cur.execute("COMMIT")
    end_time = time.perf_counter()
    print(f"Insert {count} rows: {end_time - start_time:.4f}s")

    # Benchmark 1: fetchall() - retrieve all rows at once
    # Reconnect to ensure a clean cursor state
    conn.close()
    conn = decentdb.connect(db_path)
    cur = conn.cursor()

    print("Benchmarking fetchall...")
    start_time = time.perf_counter()
    cur.execute("SELECT * FROM bench")
    rows = cur.fetchall()
    end_time = time.perf_counter()

    print(f"Fetchall {count} rows: {end_time - start_time:.4f}s")
    assert len(rows) == count

    # Benchmark 2: fetchmany(batch_size) - retrieve rows in batches
    # This approach can reduce memory pressure for large result sets
    conn.close()
    conn = decentdb.connect(db_path)
    cur = conn.cursor()

    print("Benchmarking fetchmany(1000)...")
    start_time = time.perf_counter()
    cur.execute("SELECT * FROM bench")
    total = 0
    while True:
        batch = cur.fetchmany(1000)
        if not batch:
            break
        total += len(batch)
    end_time = time.perf_counter()

    print(f"Fetchmany(1000) {count} rows: {end_time - start_time:.4f}s")
    assert total == count

    # Cleanup: close connection and remove temporary database
    conn.close()
    if os.path.exists(db_path):
        os.remove(db_path)


if __name__ == "__main__":
    run_benchmark()