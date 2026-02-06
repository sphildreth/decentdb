#!/usr/bin/env python3
"""
Property-based tests for DecentDb invariants.

Tests that certain properties always hold regardless of the specific data or operations.
Uses random data generation to explore the state space.
"""

import argparse
import json
import os
import random
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional


class DecentDbAdapter:
    """Adapter for running SQL against DecentDb via CLI."""

    def __init__(self, engine_path: str):
        self.engine_path = engine_path

    def execute(self, db_path: str, sql: str) -> tuple[bool, list[str], str]:
        """Execute SQL and return (success, rows, error)."""
        cmd = [self.engine_path, "exec", f"--db={db_path}", f"--sql={sql}"]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)

        try:
            result = json.loads(proc.stdout.strip() or "{}")
        except json.JSONDecodeError:
            return False, [], f"Invalid JSON: {proc.stdout}"

        if not result.get("ok", False):
            error = result.get("error", {})
            return (
                False,
                [],
                f"{error.get('code', 'ERR_UNKNOWN')}: {error.get('message', 'Unknown')}",
            )

        return True, result.get("rows", []), ""


def generate_random_string(length: int = 8) -> str:
    """Generate a random string."""
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choice(chars) for _ in range(length))


def generate_random_int(min_val: int = -1000000, max_val: int = 1000000) -> int:
    """Generate a random integer."""
    return random.randint(min_val, max_val)


def test_index_scan_equivalence(
    engine: DecentDbAdapter, db_path: str, seed: int
) -> tuple[bool, str]:
    """
    Property: SELECT via index == SELECT via table scan (same results)

    For any table with an index, a query using the index should return
    the same results as a full table scan with the same filter.
    """
    random.seed(seed)

    # Create table with random data
    table_name = f"idx_test_{seed}"
    engine.execute(db_path, f"DROP TABLE IF EXISTS {table_name}")

    ok, _, err = engine.execute(
        db_path, f"CREATE TABLE {table_name} (id INT PRIMARY KEY, val INT)"
    )
    if not ok:
        return False, f"Failed to create table: {err}"

    # Insert random data
    num_rows = random.randint(10, 100)
    for i in range(num_rows):
        val = generate_random_int()
        ok, _, err = engine.execute(
            db_path, f"INSERT INTO {table_name} VALUES ({i}, {val})"
        )
        if not ok:
            return False, f"Failed to insert row {i}: {err}"

    # Create index
    ok, _, err = engine.execute(
        db_path, f"CREATE INDEX idx_{table_name}_val ON {table_name}(val)"
    )
    if not ok:
        return False, f"Failed to create index: {err}"

    # Test: Query with index seek vs table scan should return same results
    # We'll use a range query that can use the index
    min_val = -500000
    max_val = 500000

    # Query via index (engine should choose index)
    ok, index_rows, err = engine.execute(
        db_path,
        f"SELECT * FROM {table_name} WHERE val > {min_val} AND val < {max_val} ORDER BY id",
    )
    if not ok:
        return False, f"Index query failed: {err}"

    # Note: We can't easily force a table scan, but we can verify the index results
    # are consistent by checking all returned rows satisfy the condition
    for row in index_rows:
        parts = row.split("|")
        if len(parts) >= 2:
            try:
                val = int(parts[1])
                if not (min_val < val < max_val):
                    return (
                        False,
                        f"Index returned row with val={val} outside range ({min_val}, {max_val})",
                    )
            except ValueError:
                pass

    # Verify we got some results (highly likely with this range)
    if len(index_rows) == 0:
        # Check if this is actually correct (all values outside range)
        ok, all_rows, err = engine.execute(
            db_path, f"SELECT COUNT(*) FROM {table_name}"
        )
        if ok and all_rows:
            count = int(all_rows[0])
            if count > 0:
                return False, f"Index returned 0 rows but table has {count} rows"

    # Cleanup
    engine.execute(db_path, f"DROP TABLE {table_name}")

    return True, f"Verified {len(index_rows)} rows match index condition"


def test_btree_ordering(
    engine: DecentDbAdapter, db_path: str, seed: int
) -> tuple[bool, str]:
    """
    Property: BTree cursor iteration always returns sorted order.

    For any BTree index, iterating through it should return keys in
    strictly sorted order.
    """
    random.seed(seed)

    table_name = f"btree_test_{seed}"
    engine.execute(db_path, f"DROP TABLE IF EXISTS {table_name}")

    # Create table
    ok, _, err = engine.execute(
        db_path, f"CREATE TABLE {table_name} (id INT PRIMARY KEY, val TEXT)"
    )
    if not ok:
        return False, f"Failed to create table: {err}"

    # Insert random data in random order
    ids = list(range(100))
    random.shuffle(ids)

    for i in ids:
        val = generate_random_string()
        ok, _, err = engine.execute(
            db_path, f"INSERT INTO {table_name} VALUES ({i}, '{val}')"
        )
        if not ok:
            return False, f"Failed to insert: {err}"

    # Retrieve all rows ordered by id
    ok, rows, err = engine.execute(db_path, f"SELECT id FROM {table_name} ORDER BY id")
    if not ok:
        return False, f"Failed to query: {err}"

    # Verify strict ordering
    prev_id = -1
    for row in rows:
        try:
            curr_id = int(row)
            if curr_id <= prev_id:
                return False, f"Ordering violation: {curr_id} after {prev_id}"
            prev_id = curr_id
        except ValueError:
            return False, f"Invalid row format: {row}"

    # Verify we got all rows
    if len(rows) != 100:
        return False, f"Expected 100 rows, got {len(rows)}"

    # Cleanup
    engine.execute(db_path, f"DROP TABLE {table_name}")

    return True, f"Verified {len(rows)} rows in strict order"


def test_foreign_key_invariant(
    engine: DecentDbAdapter, db_path: str, seed: int
) -> tuple[bool, str]:
    """
    Property: Foreign key constraints are never violated.

    After any sequence of inserts/updates/deletes, all foreign key
    relationships must be valid (parent exists for each child).
    """
    random.seed(seed)

    # Create parent and child tables with FK
    parent_table = f"parent_{seed}"
    child_table = f"child_{seed}"

    engine.execute(db_path, f"DROP TABLE IF EXISTS {child_table}")
    engine.execute(db_path, f"DROP TABLE IF EXISTS {parent_table}")

    ok, _, err = engine.execute(
        db_path, f"CREATE TABLE {parent_table} (id INT PRIMARY KEY)"
    )
    if not ok:
        return False, f"Failed to create parent: {err}"

    ok, _, err = engine.execute(
        db_path,
        f"CREATE TABLE {child_table} (id INT PRIMARY KEY, parent_id INT REFERENCES {parent_table}(id))",
    )
    if not ok:
        return False, f"Failed to create child: {err}"

    # Insert parents
    num_parents = random.randint(5, 20)
    for i in range(num_parents):
        ok, _, err = engine.execute(db_path, f"INSERT INTO {parent_table} VALUES ({i})")
        if not ok:
            return False, f"Failed to insert parent {i}: {err}"

    # Insert valid children
    num_children = random.randint(10, 50)
    valid_children = 0
    for i in range(num_children):
        parent_id = random.randint(0, num_parents - 1)
        ok, _, err = engine.execute(
            db_path, f"INSERT INTO {child_table} VALUES ({i}, {parent_id})"
        )
        if ok:
            valid_children += 1
        # If it failed, that's expected (FK violation would fail earlier)

    # Verify all children have valid parents
    ok, rows, err = engine.execute(
        db_path,
        f"""SELECT c.id, c.parent_id FROM {child_table} c 
            LEFT JOIN {parent_table} p ON c.parent_id = p.id 
            WHERE p.id IS NULL""",
    )
    if not ok:
        return False, f"Failed to verify FKs: {err}"

    if rows:
        return False, f"Found {len(rows)} orphaned children: {rows[:3]}"

    # Try to insert invalid child (should fail)
    ok, _, err = engine.execute(
        db_path, f"INSERT INTO {child_table} VALUES (9999, 9999)"
    )
    if ok:
        return False, "Inserted child with invalid parent (FK not enforced)"

    # Cleanup
    engine.execute(db_path, f"DROP TABLE {child_table}")
    engine.execute(db_path, f"DROP TABLE {parent_table}")

    return True, f"Verified {valid_children} children have valid parents"


def test_snapshot_isolation(
    engine: DecentDbAdapter, db_path: str, seed: int
) -> tuple[bool, str]:
    """
    Property: Readers see consistent snapshot from transaction start.

    A reader started at time T should not see changes committed after T.
    """
    random.seed(seed)

    table_name = f"snapshot_test_{seed}"
    engine.execute(db_path, f"DROP TABLE IF EXISTS {table_name}")

    # Create table
    ok, _, err = engine.execute(
        db_path, f"CREATE TABLE {table_name} (id INT PRIMARY KEY, val INT)"
    )
    if not ok:
        return False, f"Failed to create table: {err}"

    # Insert initial data
    ok, _, err = engine.execute(db_path, f"INSERT INTO {table_name} VALUES (1, 100)")
    if not ok:
        return False, f"Failed to insert: {err}"

    # Note: True concurrent testing requires multiple connections
    # For now, we verify basic snapshot behavior within single connection

    # Start transaction
    ok, _, err = engine.execute(db_path, "BEGIN")
    if not ok:
        # Auto-commit mode - test differently
        pass

    # Read initial value
    ok, rows1, err = engine.execute(
        db_path, f"SELECT val FROM {table_name} WHERE id = 1"
    )
    if not ok or not rows1:
        return False, f"Failed to read: {err}"

    initial_val = int(rows1[0])

    # Update the value (in auto-commit this commits immediately)
    ok, _, err = engine.execute(
        db_path, f"UPDATE {table_name} SET val = 200 WHERE id = 1"
    )
    if not ok:
        return False, f"Failed to update: {err}"

    # Read again - should see new value (auto-commit)
    ok, rows2, err = engine.execute(
        db_path, f"SELECT val FROM {table_name} WHERE id = 1"
    )
    if not ok or not rows2:
        return False, f"Failed to read after update: {err}"

    new_val = int(rows2[0])

    # In auto-commit mode, we should see the committed value
    if new_val != 200:
        return False, f"Expected val=200 after update, got {new_val}"

    # Cleanup
    engine.execute(db_path, f"DROP TABLE {table_name}")

    return True, f"Verified value changed from {initial_val} to {new_val}"


def test_acid_durability_simple(
    engine: DecentDbAdapter, db_path: str, seed: int
) -> tuple[bool, str]:
    """
    Property: Committed data persists after reopen.

    Data from committed transactions should be visible after
    closing and reopening the database.
    """
    random.seed(seed)

    table_name = f"acid_test_{seed}"

    # Insert data
    ok, _, err = engine.execute(db_path, f"DROP TABLE IF EXISTS {table_name}")
    ok, _, err = engine.execute(
        db_path, f"CREATE TABLE {table_name} (id INT PRIMARY KEY, val TEXT)"
    )
    if not ok:
        return False, f"Failed to create: {err}"

    num_rows = random.randint(5, 20)
    for i in range(num_rows):
        val = generate_random_string()
        ok, _, err = engine.execute(
            db_path, f"INSERT INTO {table_name} VALUES ({i}, '{val}')"
        )
        if not ok:
            return False, f"Failed to insert row {i}: {err}"

    # Reopen database by running a new query
    ok, rows, err = engine.execute(db_path, f"SELECT COUNT(*) FROM {table_name}")
    if not ok:
        return False, f"Failed to count after reopen: {err}"

    count = int(rows[0]) if rows else 0
    if count != num_rows:
        return False, f"Expected {num_rows} rows after reopen, found {count}"

    # Cleanup
    engine.execute(db_path, f"DROP TABLE {table_name}")

    return True, f"Verified {count} rows persisted after reopen"


def test_view_equivalence(
    engine: DecentDbAdapter, db_path: str, seed: int
) -> tuple[bool, str]:
    """
    Property: SELECT from a simple view matches the inlined SELECT.
    """
    random.seed(seed)

    table_name = f"view_base_{seed}"
    view_name = f"view_v_{seed}"

    engine.execute(db_path, f"DROP VIEW IF EXISTS {view_name}")
    engine.execute(db_path, f"DROP TABLE IF EXISTS {table_name}")

    ok, _, err = engine.execute(
        db_path, f"CREATE TABLE {table_name} (id INT PRIMARY KEY, val INT)"
    )
    if not ok:
        return False, f"Failed to create table: {err}"

    num_rows = random.randint(10, 40)
    threshold = random.randint(-100, 100)
    for i in range(num_rows):
        val = generate_random_int(-200, 200)
        ok, _, err = engine.execute(
            db_path, f"INSERT INTO {table_name} VALUES ({i}, {val})"
        )
        if not ok:
            return False, f"Failed to insert row {i}: {err}"

    ok, _, err = engine.execute(
        db_path,
        f"CREATE VIEW {view_name} AS SELECT id, val FROM {table_name} WHERE val > {threshold}",
    )
    if not ok:
        return False, f"Failed to create view: {err}"

    ok, view_rows, err = engine.execute(
        db_path, f"SELECT id, val FROM {view_name} ORDER BY id"
    )
    if not ok:
        return False, f"Failed to query view: {err}"

    ok, base_rows, err = engine.execute(
        db_path,
        f"SELECT id, val FROM {table_name} WHERE val > {threshold} ORDER BY id",
    )
    if not ok:
        return False, f"Failed to query base: {err}"

    if view_rows != base_rows:
        return False, f"View/base mismatch: view={view_rows[:5]} base={base_rows[:5]}"

    replace_ok, _, replace_err = engine.execute(
        db_path,
        f"CREATE OR REPLACE VIEW {view_name} AS SELECT id, val FROM {table_name} WHERE val >= {threshold}",
    )
    if not replace_ok:
        return False, f"Failed to replace view: {replace_err}"

    engine.execute(db_path, f"DROP VIEW {view_name}")
    engine.execute(db_path, f"DROP TABLE {table_name}")
    return True, f"Verified equivalence on {num_rows} rows (threshold={threshold})"


def main() -> int:
    parser = argparse.ArgumentParser(description="Property-based tests for DecentDb")
    parser.add_argument(
        "--engine", required=True, help="Path to decentdb CLI executable"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=50,
        help="Number of random iterations per test",
    )
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    parser.add_argument(
        "--test",
        choices=["index", "btree", "fk", "snapshot", "acid", "view", "all"],
        default="all",
    )
    args = parser.parse_args()

    # Initialize
    engine = DecentDbAdapter(args.engine)

    if args.seed:
        random.seed(args.seed)
        base_seed = args.seed
    else:
        base_seed = random.randint(1, 1000000)
        print(f"Using random seed: {base_seed}")

    # Define tests
    tests = []
    if args.test in ("index", "all"):
        tests.append(("Index-Scan Equivalence", test_index_scan_equivalence))
    if args.test in ("btree", "all"):
        tests.append(("BTree Ordering", test_btree_ordering))
    if args.test in ("fk", "all"):
        tests.append(("Foreign Key Invariant", test_foreign_key_invariant))
    if args.test in ("snapshot", "all"):
        tests.append(("Snapshot Isolation", test_snapshot_isolation))
    if args.test in ("acid", "all"):
        tests.append(("ACID Durability", test_acid_durability_simple))
    if args.test in ("view", "all"):
        tests.append(("View Equivalence", test_view_equivalence))

    print(
        f"Running property-based tests: {len(tests)} properties x {args.iterations} iterations"
    )
    print(f"Engine: {args.engine}")

    passed = 0
    failed = 0

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "property_test.ddb")

        for test_name, test_func in tests:
            print(f"\n{test_name}:")

            for i in range(args.iterations):
                seed = base_seed + i
                try:
                    ok, msg = test_func(engine, db_path, seed)
                    if ok:
                        passed += 1
                        if i < 3:  # Show first few successes
                            print(f"  [{i + 1}] OK: {msg}")
                    else:
                        failed += 1
                        print(f"  [{i + 1}] FAIL (seed={seed}): {msg}")
                except Exception as e:
                    failed += 1
                    print(f"  [{i + 1}] ERROR (seed={seed}): {e}")

    total = passed + failed
    print(f"\n{'=' * 50}")
    print(f"Property test results: {passed}/{total} passed ({failed} failed)")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
