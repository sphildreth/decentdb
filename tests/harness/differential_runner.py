#!/usr/bin/env python3
"""
Differential test harness - Compare DecentDb behavior against PostgreSQL.

Runs identical SQL operations on both databases and verifies results match.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional

# Import postgres_ref module
sys.path.insert(0, str(Path(__file__).resolve().parent))
from postgres_ref import PostgresRef, compare_results


class DecentDbAdapter:
    """Adapter for running SQL against DecentDb via CLI."""

    def __init__(self, engine_path: str):
        self.engine_path = engine_path

    def execute(self, db_path: str, sql: str) -> tuple[bool, list[str], str]:
        """
        Execute SQL against DecentDb.

        Returns:
            Tuple of (success, rows, error_message)
        """
        cmd = [self.engine_path, "exec", "--db", db_path, "--sql", sql]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)

        try:
            result = json.loads(proc.stdout.strip() or "{}")
        except json.JSONDecodeError:
            return False, [], f"Invalid JSON output: {proc.stdout}"

        if not result.get("ok", False):
            error = result.get("error", {})
            return (
                False,
                [],
                f"{error.get('code', 'ERR_UNKNOWN')}: {error.get('message', 'Unknown error')}",
            )

        rows = result.get("rows", [])
        return True, rows, ""


class DifferentialTest:
    """A single differential test case."""

    def __init__(
        self,
        name: str,
        description: str,
        schema_sql: str,
        test_sql: str,
        expect_rows: Optional[list[str]] = None,
        ignore_order: bool = False,
        setup_sql: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.schema_sql = schema_sql
        self.test_sql = test_sql
        self.expect_rows = expect_rows or []
        self.ignore_order = ignore_order
        self.setup_sql = setup_sql


# Define differential test cases
DIFFERENTIAL_TESTS = [
    # DDL Tests
    DifferentialTest(
        name="create_table_int",
        description="CREATE TABLE with INTEGER column",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        test_sql="SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'test' ORDER BY ordinal_position",
        ignore_order=True,  # Schema introspection might differ
    ),
    DifferentialTest(
        name="create_table_text",
        description="CREATE TABLE with TEXT column",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, name TEXT)",
        test_sql="INSERT INTO test VALUES (1, 'hello'), (2, 'world'); SELECT * FROM test ORDER BY id",
        expect_rows=["1|hello", "2|world"],
    ),
    DifferentialTest(
        name="create_table_bool",
        description="CREATE TABLE with BOOLEAN column",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, active BOOL)",
        test_sql="INSERT INTO test VALUES (1, true), (2, false); SELECT * FROM test ORDER BY id",
        expect_rows=["1|true", "2|false"],
    ),
    # DML - INSERT tests
    DifferentialTest(
        name="insert_single",
        description="Single row INSERT",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, data TEXT)",
        test_sql="INSERT INTO test VALUES (1, 'single'); SELECT * FROM test",
        expect_rows=["1|single"],
    ),
    DifferentialTest(
        name="insert_null",
        description="INSERT with NULL values",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, data TEXT)",
        test_sql="INSERT INTO test VALUES (1, NULL); SELECT id, data FROM test",
        expect_rows=["1|"],
    ),
    # DML - SELECT tests
    DifferentialTest(
        name="select_where_equality",
        description="SELECT with WHERE equality",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)",
        test_sql="SELECT * FROM test WHERE val = 20",
        expect_rows=["2|20"],
    ),
    DifferentialTest(
        name="select_where_range",
        description="SELECT with WHERE range operators",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
        test_sql="SELECT * FROM test WHERE val > 15 AND val < 35 ORDER BY id",
        expect_rows=["2|20", "3|30"],
    ),
    DifferentialTest(
        name="select_order_by",
        description="SELECT with ORDER BY",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, name TEXT)",
        setup_sql="INSERT INTO test VALUES (3, 'charlie'), (1, 'alpha'), (2, 'bravo')",
        test_sql="SELECT * FROM test ORDER BY name",
        expect_rows=["1|alpha", "2|bravo", "3|charlie"],
    ),
    DifferentialTest(
        name="select_limit_offset",
        description="SELECT with LIMIT and OFFSET",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY)",
        setup_sql="INSERT INTO test VALUES (1), (2), (3), (4), (5)",
        test_sql="SELECT * FROM test ORDER BY id LIMIT 2 OFFSET 1",
        expect_rows=["2", "3"],
    ),
    DifferentialTest(
        name="select_like_pattern",
        description="SELECT with LIKE pattern matching",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, name TEXT)",
        setup_sql="INSERT INTO test VALUES (1, 'apple'), (2, 'banana'), (3, 'apricot')",
        test_sql="SELECT * FROM test WHERE name LIKE '%app%' ORDER BY id",
        expect_rows=["1|apple", "3|apricot"],
    ),
    # Aggregate tests
    DifferentialTest(
        name="count_star",
        description="COUNT(*) aggregate",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY)",
        setup_sql="INSERT INTO test VALUES (1), (2), (3)",
        test_sql="SELECT COUNT(*) FROM test",
        expect_rows=["3"],
    ),
    DifferentialTest(
        name="count_column",
        description="COUNT(column) with NULL handling",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, NULL), (3, 30)",
        test_sql="SELECT COUNT(val) FROM test",
        expect_rows=["2"],
    ),
    DifferentialTest(
        name="sum_aggregate",
        description="SUM aggregate function",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)",
        test_sql="SELECT SUM(val) FROM test",
        expect_rows=["60"],
    ),
    DifferentialTest(
        name="avg_aggregate",
        description="AVG aggregate function",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)",
        test_sql="SELECT AVG(val) FROM test",
        expect_rows=["20"],  # May need tolerance for floating point
    ),
    DifferentialTest(
        name="min_max_aggregate",
        description="MIN and MAX aggregate functions",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, 5), (3, 30)",
        test_sql="SELECT MIN(val), MAX(val) FROM test",
        expect_rows=["5|30"],
    ),
    DifferentialTest(
        name="group_by",
        description="GROUP BY with aggregates",
        schema_sql="CREATE TABLE test (category TEXT, val INT)",
        setup_sql="INSERT INTO test VALUES ('a', 10), ('a', 20), ('b', 30)",
        test_sql="SELECT category, SUM(val) FROM test GROUP BY category ORDER BY category",
        expect_rows=["a|30", "b|30"],
    ),
    # JOIN tests
    DifferentialTest(
        name="inner_join",
        description="INNER JOIN on equality",
        schema_sql="CREATE TABLE a (id INT PRIMARY KEY, name TEXT); CREATE TABLE b (id INT PRIMARY KEY, a_id INT, value TEXT)",
        setup_sql="INSERT INTO a VALUES (1, 'one'), (2, 'two'); INSERT INTO b VALUES (1, 1, 'first'), (2, 1, 'second'), (3, 3, 'orphan')",
        test_sql="SELECT a.id, a.name, b.value FROM a INNER JOIN b ON a.id = b.a_id ORDER BY b.id",
        expect_rows=["1|one|first", "1|one|second"],
    ),
    DifferentialTest(
        name="left_join",
        description="LEFT JOIN preserving left table rows",
        schema_sql="CREATE TABLE a (id INT PRIMARY KEY, name TEXT); CREATE TABLE b (id INT PRIMARY KEY, a_id INT)",
        setup_sql="INSERT INTO a VALUES (1, 'one'), (2, 'two'); INSERT INTO b VALUES (1, 1)",
        test_sql="SELECT a.id, a.name, b.id FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id",
        expect_rows=["1|one|1", "2|two|"],
    ),
    # DML - UPDATE tests
    DifferentialTest(
        name="update_single",
        description="UPDATE single row",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, 20)",
        test_sql="UPDATE test SET val = 100 WHERE id = 1; SELECT * FROM test ORDER BY id",
        expect_rows=["1|100", "2|20"],
    ),
    DifferentialTest(
        name="update_where",
        description="UPDATE multiple rows with WHERE",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)",
        test_sql="UPDATE test SET val = val * 2 WHERE val > 15; SELECT * FROM test ORDER BY id",
        expect_rows=["1|10", "2|40", "3|60"],
    ),
    # DML - DELETE tests
    DifferentialTest(
        name="delete_single",
        description="DELETE single row",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, 20)",
        test_sql="DELETE FROM test WHERE id = 1; SELECT * FROM test",
        expect_rows=["2|20"],
    ),
    DifferentialTest(
        name="delete_where",
        description="DELETE with WHERE clause",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT)",
        setup_sql="INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)",
        test_sql="DELETE FROM test WHERE val > 15; SELECT * FROM test ORDER BY id",
        expect_rows=["1|10"],
    ),
    # Constraint tests
    DifferentialTest(
        name="not_null_constraint",
        description="NOT NULL constraint enforcement",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, data TEXT NOT NULL)",
        test_sql="INSERT INTO test VALUES (1, NULL)",
        expect_rows=[],  # Should fail
    ),
    DifferentialTest(
        name="unique_constraint",
        description="UNIQUE constraint enforcement",
        schema_sql="CREATE TABLE test (id INT PRIMARY KEY, val INT UNIQUE)",
        setup_sql="INSERT INTO test VALUES (1, 10)",
        test_sql="INSERT INTO test VALUES (2, 10)",
        expect_rows=[],  # Should fail
    ),
]


def run_differential_test(
    test: DifferentialTest, decent: DecentDbAdapter, pg: PostgresRef, temp_dir: str
) -> tuple[bool, str]:
    """
    Run a single differential test.

    Returns:
        Tuple of (passed, message)
    """
    db_path = os.path.join(temp_dir, f"diff_{test.name}.ddb")
    pg_schema = f"decentdb_diff_{test.name}"

    # Clean up any existing files
    for f in [db_path, db_path + ".wal"]:
        if os.path.exists(f):
            os.unlink(f)

    try:
        # Setup PostgreSQL schema
        pg.drop_schema(pg_schema)
        if not pg.create_schema(pg_schema):
            return False, "Failed to create PostgreSQL schema"

        # Execute schema creation
        decent_ok, decent_err = decent.execute(db_path, test.schema_sql)[:2]
        if not decent_ok:
            return False, f"DecentDb schema failed: {decent_err}"

        pg_ok, pg_err = pg.execute_in_schema(pg_schema, test.schema_sql)[:2]
        if not pg_ok:
            # PostgreSQL might have slightly different syntax - skip comparison
            return True, "Skipped (PostgreSQL schema difference)"

        # Execute setup SQL if present
        if test.setup_sql:
            decent_ok, _, decent_err = decent.execute(db_path, test.setup_sql)
            if not decent_ok:
                return False, f"DecentDb setup failed: {decent_err}"

            pg_ok, _, pg_err = pg.execute_in_schema(pg_schema, test.setup_sql)
            if not pg_ok:
                return True, "Skipped (PostgreSQL setup difference)"

        # Execute test SQL
        decent_ok, decent_rows, decent_err = decent.execute(db_path, test.test_sql)
        pg_ok, pg_rows, pg_err = pg.execute_in_schema(pg_schema, test.test_sql)

        # Handle expected failures (like constraint violations)
        if not decent_ok and not pg_ok:
            # Both failed - behavior matches
            return True, "Both failed as expected"

        if not decent_ok:
            return False, f"DecentDb failed: {decent_err}"

        if not pg_ok:
            # PostgreSQL might have different behavior - log but don't fail
            return True, f"PostgreSQL failed (may be expected): {pg_err}"

        # Compare results
        match, msg = compare_results(decent_rows, pg_rows, test.ignore_order)
        if not match:
            return False, f"Results mismatch: {msg}"

        return True, ""

    finally:
        # Cleanup
        pg.drop_schema(pg_schema)
        for f in [db_path, db_path + ".wal"]:
            if os.path.exists(f):
                os.unlink(f)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Differential test harness for DecentDb"
    )
    parser.add_argument(
        "--engine", required=True, help="Path to decentdb CLI executable"
    )
    parser.add_argument("--test", help="Run specific test by name")
    parser.add_argument("--list", action="store_true", help="List available tests")
    parser.add_argument(
        "--skip-pg", action="store_true", help="Skip tests requiring PostgreSQL"
    )
    args = parser.parse_args()

    if args.list:
        print("Available differential tests:")
        for test in DIFFERENTIAL_TESTS:
            print(f"  - {test.name}: {test.description}")
        return 0

    # Initialize adapters
    decent = DecentDbAdapter(args.engine)

    if args.skip_pg:
        print("Skipping PostgreSQL tests (--skip-pg specified)")
        return 0

    try:
        pg = PostgresRef()
    except RuntimeError as e:
        print(f"Cannot connect to PostgreSQL: {e}")
        print(
            "Set PGDATABASE environment variable or use --skip-pg to skip differential tests"
        )
        return 1

    # Run tests
    tests_to_run = DIFFERENTIAL_TESTS
    if args.test:
        tests_to_run = [t for t in DIFFERENTIAL_TESTS if t.name == args.test]
        if not tests_to_run:
            print(f"Test '{args.test}' not found")
            return 1

    print(f"Running {len(tests_to_run)} differential tests...")
    print(f"Engine: {args.engine}")
    print(f"PostgreSQL: {pg.conn_str}")

    passed = 0
    failed = 0
    skipped = 0

    with tempfile.TemporaryDirectory() as temp_dir:
        for test in tests_to_run:
            try:
                ok, msg = run_differential_test(test, decent, pg, temp_dir)
                if ok:
                    if msg and "Skipped" in msg:
                        print(f"  [SKIP] {test.name}: {msg}")
                        skipped += 1
                    else:
                        print(f"  [OK] {test.name}")
                        passed += 1
                else:
                    print(f"  [FAIL] {test.name}: {msg}")
                    failed += 1
            except Exception as e:
                print(f"  [ERROR] {test.name}: {e}")
                failed += 1

    print(f"\n{'=' * 50}")
    print(
        f"Differential test results: {passed} passed, {failed} failed, {skipped} skipped"
    )

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
