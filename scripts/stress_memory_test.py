#!/usr/bin/env python3
"""
Long-running stress test for memory leak detection.

This script runs continuous allocation/deallocation cycles to detect
slow memory leaks that may not appear in short test runs.

Usage:
    python scripts/stress_memory_test.py --duration 1800 --db-path /tmp/stress.ddb
"""

import argparse
import gc
import os
import random
import string
import sys
import threading
import time
from pathlib import Path

try:
    import psutil
except ImportError:
    print("psutil not installed. Install with: pip install psutil")
    sys.exit(1)

try:
    import decentdb
except ImportError:
    print("decentdb not installed. Install with: pip install -e bindings/python")
    sys.exit(1)


def get_rss_mb() -> float:
    """Get current RSS in MB."""
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)


def generate_random_string(min_len: int, max_len: int) -> str:
    """Generate a random string of random length."""
    length = random.randint(min_len, max_len)
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def stress_test(db_path: str, duration_seconds: int, report_interval: int = 30):
    """
    Run stress test for the specified duration.
    
    Args:
        db_path: Path to the database file
        duration_seconds: How long to run the test
        report_interval: How often to report RSS (seconds)
    """
    # Remove existing database
    db_file = Path(db_path)
    if db_file.exists():
        db_file.unlink()
    wal_file = Path(f"{db_path}.wal")
    if wal_file.exists():
        wal_file.unlink()

    print(f"Starting stress test for {duration_seconds} seconds")
    print(f"Database path: {db_path}")
    print(f"Report interval: {report_interval}s")
    print("-" * 60)

    # Initialize database with schema
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE stress_test (
            id INTEGER PRIMARY KEY,
            data TEXT,
            blob_data BLOB,
            created_at INTEGER
        )
    """)
    conn.commit()
    conn.close()

    # Force GC and get baseline
    gc.collect()
    gc.collect()
    baseline_rss = get_rss_mb()
    print(f"Baseline RSS: {baseline_rss:.2f} MB")

    # Statistics
    stats = {
        "inserts": 0,
        "selects": 0,
        "updates": 0,
        "deletes": 0,
        "open_close_cycles": 0,
        "errors": [],
    }

    start_time = time.time()
    last_report = start_time
    max_rss = baseline_rss
    min_rss = baseline_rss

    def log_error(msg: str):
        stats["errors"].append((time.time() - start_time, msg))
        print(f"ERROR: {msg}")

    iteration = 0
    while time.time() - start_time < duration_seconds:
        iteration += 1
        elapsed = time.time() - start_time

        try:
            # Pattern 1: Open/close cycles with queries
            if iteration % 5 == 0:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM stress_test")
                cur.fetchone()
                conn.close()
                stats["open_close_cycles"] += 1

            # Pattern 2: Insert with varying payload sizes
            if iteration % 3 == 0:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                row_id = random.randint(1, 10_000_000)
                data = generate_random_string(10, 1000)
                blob_data = os.urandom(random.randint(100, 5000))
                cur.execute(
                    "INSERT OR REPLACE INTO stress_test VALUES (?, ?, ?, ?)",
                    (row_id, data, blob_data, int(time.time() * 1000))
                )
                conn.commit()
                conn.close()
                stats["inserts"] += 1

            # Pattern 3: Select with various patterns
            if iteration % 4 == 0:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                cur.execute("SELECT * FROM stress_test ORDER BY id DESC LIMIT 100")
                cur.fetchall()
                conn.close()
                stats["selects"] += 1

            # Pattern 4: Update operations
            if iteration % 7 == 0:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                cur.execute("SELECT id FROM stress_test ORDER BY RANDOM() LIMIT 1")
                row = cur.fetchone()
                if row:
                    new_data = generate_random_string(50, 500)
                    cur.execute(
                        "UPDATE stress_test SET data = ? WHERE id = ?",
                        (new_data, row[0])
                    )
                    conn.commit()
                    stats["updates"] += 1
                conn.close()

            # Pattern 5: Delete old records (keep table bounded)
            if iteration % 100 == 0:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM stress_test")
                count = cur.fetchone()[0]
                if count > 10000:
                    cur.execute(
                        "DELETE FROM stress_test WHERE id IN "
                        "(SELECT id FROM stress_test ORDER BY id LIMIT 5000)"
                    )
                    conn.commit()
                    stats["deletes"] += 1
                conn.close()

            # Pattern 6: Concurrent connections (simulated)
            if iteration % 20 == 0:
                connections = []
                for _ in range(5):
                    conn = decentdb.connect(db_path)
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM stress_test")
                    cur.fetchone()
                    connections.append(conn)
                for conn in connections:
                    conn.close()

            # Periodic GC
            if iteration % 50 == 0:
                gc.collect()

        except Exception as e:
            log_error(f"Iteration {iteration}: {e}")

        # Report RSS periodically
        if time.time() - last_report >= report_interval:
            gc.collect()
            current_rss = get_rss_mb()
            max_rss = max(max_rss, current_rss)
            min_rss = min(min_rss, current_rss)
            growth = current_rss - baseline_rss

            print(
                f"[{elapsed:6.1f}s] RSS: {current_rss:7.2f} MB "
                f"(growth: {growth:+7.2f} MB, max: {max_rss:.2f}, min: {min_rss:.2f}) "
                f"| ops: ins={stats['inserts']} sel={stats['selects']} "
                f"upd={stats['updates']} del={stats['deletes']} "
                f"cycles={stats['open_close_cycles']}"
            )

            last_report = time.time()

    # Final report
    gc.collect()
    gc.collect()
    final_rss = get_rss_mb()
    total_growth = final_rss - baseline_rss

    print("-" * 60)
    print("STRESS TEST COMPLETE")
    print(f"Duration: {duration_seconds}s")
    print(f"Baseline RSS: {baseline_rss:.2f} MB")
    print(f"Final RSS: {final_rss:.2f} MB")
    print(f"Total growth: {total_growth:+.2f} MB")
    print(f"Max RSS: {max_rss:.2f} MB")
    print(f"Min RSS: {min_rss:.2f} MB")
    print()
    print("Operations:")
    print(f"  Inserts: {stats['inserts']}")
    print(f"  Selects: {stats['selects']}")
    print(f"  Updates: {stats['updates']}")
    print(f"  Deletes: {stats['deletes']}")
    print(f"  Open/Close cycles: {stats['open_close_cycles']}")
    print()

    if stats["errors"]:
        print(f"Errors encountered: {len(stats['errors'])}")
        for ts, msg in stats["errors"][:10]:
            print(f"  [{ts:.1f}s] {msg}")
    else:
        print("No errors encountered")

    # Determine pass/fail
    # Allow 50 MB growth over 30 minutes (generous threshold)
    threshold_mb = 50.0
    if total_growth > threshold_mb:
        print(f"\nFAIL: RSS growth ({total_growth:.2f} MB) exceeds threshold ({threshold_mb} MB)")
        return 1
    else:
        print(f"\nPASS: RSS growth ({total_growth:.2f} MB) within threshold ({threshold_mb} MB)")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Long-running stress test for memory leak detection"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=1800,
        help="Duration in seconds (default: 1800 = 30 minutes)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="/tmp/decentdb_stress.ddb",
        help="Path to database file",
    )
    parser.add_argument(
        "--report-interval",
        type=int,
        default=30,
        help="Report interval in seconds (default: 30)",
    )

    args = parser.parse_args()

    sys.exit(stress_test(args.db_path, args.duration, args.report_interval))


if __name__ == "__main__":
    main()
