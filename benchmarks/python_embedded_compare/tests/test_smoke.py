"""Smoke tests for benchmark framework.

These tests exercise the benchmark framework with a small dataset
to ensure everything works end-to-end.
"""

import os
import tempfile

import pytest

from drivers.sqlite_driver import SQLiteDriver

try:
    from drivers.duckdb_driver import DuckDBDriver

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    DuckDBDriver = None

from scenarios.canonical_workloads import get_workload, WORKLOADS

from utils.dataset_generator import DatasetGenerator, GeneratorConfig


class TestSmokeSQLite:
    """Smoke test for SQLite driver."""

    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield os.path.join(tmpdir, "test.db")

    def test_sqlite_connection(self, temp_db_path):
        """SQLite driver should connect successfully."""
        driver = SQLiteDriver({"database_path": temp_db_path})

        assert driver.connect()

        driver.disconnect()

    def test_sqlite_schema_creation(self, temp_db_path):
        """SQLite driver should create schema."""
        driver = SQLiteDriver({"database_path": temp_db_path})
        driver.connect()

        workload_a = get_workload("workload_a")
        driver.create_schema(workload_a.get_schema_sql())

        # Verify tables exist from workload A
        result = driver.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        table_names = [r["name"] for r in result]

        assert "customers" in table_names
        assert "orders" in table_names

        # Also test workload B (events)
        workload_b = get_workload("workload_b")
        driver.create_schema(workload_b.get_schema_sql())

        result = driver.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        table_names = [r["name"] for r in result]

        assert "events" in table_names

        driver.disconnect()

    def test_sqlite_data_insertion(self, temp_db_path):
        """SQLite driver should insert data."""
        driver = SQLiteDriver({"database_path": temp_db_path})
        driver.connect()

        # Create table
        driver.execute_update("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

        # Insert data
        driver.begin_transaction()
        driver.execute_update("INSERT INTO test (id, value) VALUES (?, ?)", (1, "test"))
        driver.commit()

        # Verify
        result = driver.execute_query("SELECT * FROM test")

        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["value"] == "test"

        driver.disconnect()


class TestSmokeDuckDB:
    """Smoke test for DuckDB driver."""

    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield os.path.join(tmpdir, "test.duckdb")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
    def test_duckdb_connection(self, temp_db_path):
        """DuckDB driver should connect successfully."""
        driver = DuckDBDriver({"database_path": temp_db_path})

        assert driver.connect()

        driver.disconnect()

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
    def test_duckdb_schema_creation(self, temp_db_path):
        """DuckDB driver should create schema."""
        driver = DuckDBDriver({"database_path": temp_db_path})
        driver.connect()

        workload = get_workload("workload_a")
        driver.create_schema(workload.get_schema_sql())

        # Verify tables exist
        result = driver.execute_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        )
        table_names = [r["table_name"].lower() for r in result]

        assert "customers" in table_names
        assert "orders" in table_names

        driver.disconnect()


class TestWorkloads:
    """Test workload implementations."""

    def test_workload_a_schema(self):
        """Workload A should have valid schema."""
        workload = get_workload("workload_a")

        schema = workload.get_schema_sql()

        assert "customers" in schema
        assert "orders" in schema
        assert "customer_id" in schema
        assert "order_id" in schema

    def test_workload_b_schema(self):
        """Workload B should have valid schema."""
        workload = get_workload("workload_b")

        schema = workload.get_schema_sql()

        assert "events" in schema
        assert "event_id" in schema
        assert "user_id" in schema

    def test_workload_registry(self):
        """Workload registry should have expected workloads."""
        assert "workload_a" in WORKLOADS
        assert "workload_b" in WORKLOADS


class TestDatasetGenerator:
    """Smoke test for dataset generator."""

    def test_generator_produces_data(self):
        """Generator should produce data."""
        config = GeneratorConfig(
            seed=42,
            customers_n=10,
            orders_n=20,
            events_n=20,
        )

        gen = DatasetGenerator(config)
        customers, orders, events = gen.generate()

        assert len(customers) == 10
        assert len(orders) == 20
        assert len(events) == 20

    def test_generator_referential_integrity(self):
        """Generator should maintain referential integrity."""
        config = GeneratorConfig(
            seed=42,
            customers_n=100,
            orders_n=500,
            events_n=500,
        )

        gen = DatasetGenerator(config)
        customers, orders, events = gen.generate()

        customer_ids = {c.customer_id for c in customers}

        for order in orders:
            assert order.customer_id in customer_ids
