#!/usr/bin/env python3
"""
Resource leak detection tests for DecentDB.

Verifies that resources (file handles, memory, temp files) are properly
released after operations complete.
"""

import argparse
import os
import psutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


class DecentDBAdapter:
    """Adapter for running SQL against DecentDB via CLI."""

    def __init__(self, engine_path: str):
        self.engine_path = engine_path

    def execute(self, db_path: str, sql: str) -> tuple[bool, str]:
        """Execute SQL and return (success, error)."""
        cmd = [self.engine_path, "exec", "--db", db_path, "--sql", sql]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)

        try:
            import json

            result = json.loads(proc.stdout.strip() or "{}")
            if not result.get("ok", False):
                return False, str(result.get("error", {}))
            return True, ""
        except:
            return False, proc.stdout


def count_open_files(pid: int, db_path: str) -> int:
    """Count open file handles for a specific database."""
    try:
        process = psutil.Process(pid)
        count = 0
        for fd in process.open_files():
            if db_path in fd.path:
                count += 1
        return count
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return -1


def test_file_handle_leak(engine: DecentDBAdapter, db_path: str) -> tuple[bool, str]:
    """
    Test: Repeated open/close should not leak file handles.
    """
    # Create initial database
    ok, err = engine.execute(db_path, "CREATE TABLE test (id INT PRIMARY KEY)")
    if not ok:
        return False, f"Failed to create table: {err}"

    # Get baseline file handles
    pid = os.getpid()
    baseline = count_open_files(pid, db_path)

    # Open and close database multiple times
    for i in range(20):
        ok, err = engine.execute(db_path, f"INSERT INTO test VALUES ({i})")
        if not ok:
            return False, f"Failed on iteration {i}: {err}"

    # Check file handles
    final = count_open_files(pid, db_path)

    if final > baseline + 2:  # Allow small variance
        return False, f"File handle leak: baseline={baseline}, final={final}"

    return True, f"File handles stable: {baseline} -> {final}"


def test_sort_temp_cleanup(engine: DecentDBAdapter, temp_dir: str) -> tuple[bool, str]:
    """
    Test: Sort spill files should be cleaned up after query completes.
    """
    db_path = os.path.join(temp_dir, "sort_test.ddb")

    # Create table with data
    ok, err = engine.execute(
        db_path, "CREATE TABLE test (id INT PRIMARY KEY, data TEXT)"
    )
    if not ok:
        return False, f"Failed to create table: {err}"

    # Insert many rows to force external sort
    for i in range(1000):
        data = "x" * 100  # 100 bytes per row
        ok, err = engine.execute(db_path, f"INSERT INTO test VALUES ({i}, '{data}')")
        if not ok:
            return False, f"Failed to insert row {i}: {err}"

    # Check temp directory before sort
    temp_files_before = set(os.listdir(temp_dir))

    # Execute query that requires sorting large dataset
    ok, err = engine.execute(db_path, "SELECT * FROM test ORDER BY data DESC, id DESC")
    if not ok:
        return False, f"Sort query failed: {err}"

    # Give a moment for cleanup
    time.sleep(0.1)

    # Check temp directory after sort
    temp_files_after = set(os.listdir(temp_dir))
    new_files = temp_files_after - temp_files_before

    # Filter out the database files
    spill_files = [f for f in new_files if not f.endswith((".ddb", ".db", ".wal"))]

    if spill_files:
        return False, f"Temp files not cleaned up: {spill_files}"

    return True, "All temp files cleaned up after sort"


def test_memory_stability(engine: DecentDBAdapter, db_path: str) -> tuple[bool, str]:
    """
    Test: Memory usage should stabilize after repeated operations.
    """
    import gc

    # Create table
    ok, err = engine.execute(
        db_path, "CREATE TABLE test (id INT PRIMARY KEY, data TEXT)"
    )
    if not ok:
        return False, f"Failed to create table: {err}"

    # Get baseline memory
    gc.collect()
    process = psutil.Process(os.getpid())
    baseline_mem = process.memory_info().rss / 1024 / 1024  # MB

    # Perform many operations
    for i in range(100):
        data = "x" * 1000
        ok, err = engine.execute(db_path, f"INSERT INTO test VALUES ({i}, '{data}')")
        if not ok:
            return False, f"Failed on iteration {i}: {err}"

    # Cleanup and check memory
    gc.collect()
    final_mem = process.memory_info().rss / 1024 / 1024  # MB

    # Allow for growth but not unbounded
    growth = final_mem - baseline_mem
    if growth > 100:  # 100MB threshold
        return (
            False,
            f"Excessive memory growth: {baseline_mem:.1f}MB -> {final_mem:.1f}MB (+{growth:.1f}MB)",
        )

    return (
        True,
        f"Memory stable: {baseline_mem:.1f}MB -> {final_mem:.1f}MB (+{growth:.1f}MB)",
    )


def test_wal_growth_managed(engine: DecentDBAdapter, temp_dir: str) -> tuple[bool, str]:
    """
    Test: WAL size should not grow unbounded with normal operations.
    """
    db_path = os.path.join(temp_dir, "wal_growth_test.ddb")
    wal_path = db_path + ".wal"

    # Create table
    ok, err = engine.execute(
        db_path, "CREATE TABLE test (id INT PRIMARY KEY, data TEXT)"
    )
    if not ok:
        return False, f"Failed to create table: {err}"

    # Track WAL size
    max_wal_size = 0

    # Perform operations
    for i in range(100):
        data = "x" * 500
        ok, err = engine.execute(db_path, f"INSERT INTO test VALUES ({i}, '{data}')")
        if not ok:
            return False, f"Failed on iteration {i}: {err}"

        if os.path.exists(wal_path):
            size = os.path.getsize(wal_path)
            max_wal_size = max(max_wal_size, size)

    # WAL should be managed (checkpointed) and not grow indefinitely
    # For 100 small inserts, WAL should be under 10MB
    if max_wal_size > 10 * 1024 * 1024:
        return False, f"WAL grew too large: {max_wal_size / 1024 / 1024:.1f}MB"

    return True, f"WAL size managed: max {max_wal_size / 1024:.1f}KB"


def main() -> int:
    parser = argparse.ArgumentParser(description="Resource leak detection tests")
    parser.add_argument(
        "--engine", required=True, help="Path to decentdb CLI executable"
    )
    parser.add_argument(
        "--test", choices=["files", "sort", "memory", "wal", "all"], default="all"
    )
    args = parser.parse_args()

    engine = DecentDBAdapter(args.engine)

    tests = []
    if args.test in ("files", "all"):
        tests.append(("File Handle Leak", test_file_handle_leak))
    if args.test in ("sort", "all"):
        tests.append(("Sort Temp Cleanup", test_sort_temp_cleanup))
    if args.test in ("memory", "all"):
        tests.append(("Memory Stability", test_memory_stability))
    if args.test in ("wal", "all"):
        tests.append(("WAL Growth Managed", test_wal_growth_managed))

    print(f"Running {len(tests)} resource leak tests")
    print(f"Engine: {args.engine}")

    passed = 0
    failed = 0

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "leak_test.ddb")

        for test_name, test_func in tests:
            try:
                if test_name == "Sort Temp Cleanup":
                    ok, msg = test_func(engine, temp_dir)
                elif test_name == "WAL Growth Managed":
                    ok, msg = test_func(engine, temp_dir)
                else:
                    ok, msg = test_func(engine, db_path)

                if ok:
                    print(f"  [OK] {test_name}: {msg}")
                    passed += 1
                else:
                    print(f"  [FAIL] {test_name}: {msg}")
                    failed += 1
            except Exception as e:
                print(f"  [ERROR] {test_name}: {e}")
                failed += 1

    print(f"\n{'=' * 50}")
    print(f"Resource leak test results: {passed} passed, {failed} failed")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
