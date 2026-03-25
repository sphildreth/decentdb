"""Smoke tests for benchmark framework.

These tests exercise the benchmark framework with a small dataset
to ensure everything works end-to-end.
"""

import os
import tempfile
from pathlib import Path

import pytest

from drivers.jdbc_driver import JDBCDriver
from drivers.sqlite_driver import SQLiteDriver
from drivers.firebird_driver import FirebirdDriver
from utils.charting import _decentdb_rank_summary, _engine_sort_key

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

    def test_workload_c_schema(self):
        """Workload C should expose the flat benchmark table."""
        workload = get_workload("workload_c")

        schema = workload.get_schema_sql()

        assert "bench" in schema
        assert "bench_id_idx" in schema
        assert "val" in schema

    def test_workload_registry(self):
        """Workload registry should have expected workloads."""
        assert "workload_a" in WORKLOADS
        assert "workload_b" in WORKLOADS
        assert "workload_c" in WORKLOADS


class TestJdbcDriverConfig:
    """Configuration-only tests for JDBC URL handling."""

    def test_h2_mem_url_uses_unique_db_path(self):
        """Configured H2 mem URLs should still isolate runs by db path."""
        driver = JDBCDriver(
            {
                "engine": "h2",
                "database_path": "/tmp/run-123/h2.db",
                "jdbc_url": "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
            }
        )

        assert driver.jdbc_url.startswith("jdbc:h2:mem:")
        assert "test;DB_CLOSE_DELAY=-1" not in driver.jdbc_url
        assert "DB_CLOSE_DELAY=-1" in driver.jdbc_url
        assert "run_123" in driver.jdbc_url

    def test_firebird_driver_adds_native_support_jars(self):
        """Firebird driver should include bundled Jaybird native support jars."""
        driver = FirebirdDriver(
            {
                "database_path": "/tmp/firebird/test.fdb",
                "jdbc_url": "jdbc:firebirdsql:embedded:{db_path}",
                "firebird_lib_dir": "/tmp/firebird/lib",
            }
        )

        assert any(path.endswith("jaybird-native-6.0.4.jar") for path in driver.jar_paths)
        assert any(path.endswith("jna-jpms-5.18.1.jar") for path in driver.jar_paths)
        assert driver.connection_properties["nativeLibraryPath"] == "/tmp/firebird"
        assert driver.jvm_properties["jna.library.path"] == "/tmp/firebird"
        assert driver.connection_properties["user"] == "SYSDBA"
        assert driver.connection_properties["password"] == "masterkey"

    def test_firebird_driver_creates_shim_for_versioned_fbclient(self, tmp_path):
        """Firebird driver should expose versioned fbclient as libfbclient.so."""
        versioned_lib = tmp_path / "libfbclient.so.2"
        versioned_lib.write_text("", encoding="utf-8")

        driver = FirebirdDriver(
            {
                "database_path": "/tmp/firebird/test.fdb",
                "jdbc_url": "jdbc:firebirdsql:embedded:{db_path}",
                "firebird_lib_dir": str(tmp_path),
            }
        )

        shim_dir = Path(driver.connection_properties["nativeLibraryPath"])
        assert (shim_dir / "libfbclient.so").exists()


class TestChartingConfig:
    """Configuration-only tests for benchmark chart readability helpers."""

    def test_decentdb_series_sorts_first(self):
        """DecentDB should lead legends and style maps consistently."""
        engines = ["DuckDB", "DecentDB", "SQLite_wal_full"]

        assert sorted(engines, key=_engine_sort_key)[0] == "DecentDB"

    def test_rank_summary_reports_decentdb_as_leader(self):
        """Rank summary should call out when DecentDB is winning."""
        rows = [
            {"engine": "DuckDB", "operations": 500, "mean_latency_us": 12.0},
            {"engine": "DecentDB", "operations": 500, "mean_latency_us": 4.0},
            {"engine": "SQLite_wal_full", "operations": 500, "mean_latency_us": 8.0},
        ]

        summary = _decentdb_rank_summary(rows, 500)

        assert summary == "DecentDB: 1st of 3 at 500 ops (2.0x faster than #2)"

    def test_rank_summary_reports_decentdb_as_loser(self):
        """Rank summary should call out when DecentDB trails the leader."""
        rows = [
            {"engine": "DuckDB", "operations": 500, "mean_latency_us": 4.0},
            {"engine": "DecentDB", "operations": 500, "mean_latency_us": 12.0},
            {"engine": "SQLite_wal_full", "operations": 500, "mean_latency_us": 8.0},
        ]

        summary = _decentdb_rank_summary(rows, 500)

        assert summary == "DecentDB: 3rd of 3 at 500 ops (3.0x slower than #1)"

    def test_rank_summary_handles_zero_latency_leader(self):
        """Rank summary should stay readable when the leader rounds to zero latency."""
        rows = [
            {"engine": "DecentDB", "operations": 500, "mean_latency_us": 0.0},
            {"engine": "DuckDB", "operations": 500, "mean_latency_us": 2.0},
            {"engine": "SQLite_wal_full", "operations": 500, "mean_latency_us": 3.0},
        ]

        summary = _decentdb_rank_summary(rows, 500)

        assert summary == "DecentDB: 1st of 3 at 500 ops (next best is 2.0 us)"


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
