"""Abstract base class for database benchmark drivers.

This module defines the interface that all database drivers must implement
to ensure fair and consistent benchmarking across different engines.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class EngineMetadata:
    """Metadata about the database engine."""

    name: str
    version: str
    runtime_version: str
    config_notes: str = ""


class DatabaseDriver(ABC):
    """Abstract base class for database benchmark drivers.

    All drivers must implement this interface to ensure fair comparison.
    Drivers must NOT auto-commit inside execute_update - transaction
    boundaries are controlled by the benchmark scenario.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize driver with configuration.

        Args:
            config: Driver-specific configuration from database_configs.yaml
        """
        self.config = config
        self.connection = None
        self._engine_metadata: Optional[EngineMetadata] = None

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the engine name for reporting."""
        pass

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the database.

        Returns:
            True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    def disconnect(self):
        """Close the database connection and cleanup resources."""
        pass

    @abstractmethod
    def create_schema(self, schema_sql: str):
        """Create database schema.

        Args:
            schema_sql: SQL DDL statements to create tables and indexes
        """
        pass

    @abstractmethod
    def drop_table(self, table_name: str):
        """Drop a table if it exists.

        Args:
            table_name: Name of the table to drop
        """
        pass

    @abstractmethod
    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        """Execute a SELECT query and return results.

        Args:
            sql: SQL query string
            params: Optional query parameters

        Returns:
            List of dictionaries representing rows
        """
        pass

    @abstractmethod
    def execute_update(self, sql: str, params: Optional[Tuple] = None) -> int:
        """Execute an INSERT/UPDATE/DELETE statement.

        IMPORTANT: This must NOT call commit() implicitly.
        Transaction boundaries are controlled by the benchmark scenario.

        Args:
            sql: SQL statement to execute
            params: Optional statement parameters

        Returns:
            Number of affected rows
        """
        pass

    @abstractmethod
    def execute_many(self, sql: str, params_list: List[Tuple]) -> int:
        """Execute a statement with multiple parameter sets.

        Useful for batch inserts.

        Args:
            sql: SQL statement template
            params_list: List of parameter tuples

        Returns:
            Total number of affected rows
        """
        pass

    @abstractmethod
    def begin_transaction(self):
        """Begin an explicit transaction.

        Must be called before a series of operations that should
        be committed together.
        """
        pass

    @abstractmethod
    def commit(self):
        """Commit the current transaction."""
        pass

    @abstractmethod
    def rollback(self):
        """Rollback the current transaction."""
        pass

    @abstractmethod
    def prepare_statement(self, sql: str):
        """Prepare a statement for repeated execution.

        Args:
            sql: SQL statement to prepare

        Returns:
            Prepared statement handle (driver-specific)
        """
        pass

    @abstractmethod
    def execute_prepared(self, handle: Any, params: Optional[Tuple] = None) -> Any:
        """Execute a prepared statement.

        Args:
            handle: Prepared statement handle from prepare_statement
            params: Optional parameters

        Returns:
            Driver-specific result (for fetch in queries)
        """
        pass

    @abstractmethod
    def get_engine_metadata(self) -> EngineMetadata:
        """Get engine and runtime version information.

        Returns:
            EngineMetadata with version information
        """
        pass

    @abstractmethod
    def set_durability_mode(self, mode: str):
        """Configure durability settings.

        Args:
            mode: 'durable' or 'relaxed'
        """
        pass

    @abstractmethod
    def get_storage_size(self) -> int:
        """Get the current database storage size in bytes.

        Returns:
            Total size of database files (including WAL if present)
        """
        pass

    def get_config_notes(self) -> str:
        """Get notes about configuration used for this engine.

        Returns:
            String describing any engine-specific configuration
        """
        return ""

    def validate_connection(self) -> bool:
        """Validate that the connection is alive.

        Returns:
            True if connection is valid
        """
        return self.connection is not None


class BenchmarkMetrics:
    """Container for benchmark metrics."""

    def __init__(self):
        self.latencies: List[float] = []
        self.operations: int = 0
        self.errors: int = 0

    def add_latency(self, latency_ms: float):
        """Record a latency measurement in milliseconds."""
        self.latencies.append(latency_ms)

    def get_percentiles(self) -> Dict[str, float]:
        """Calculate latency percentiles.

        Returns:
            Dictionary with p50, p95, p99 latency in ms
        """
        if not self.latencies:
            return {"p50_ms": 0, "p95_ms": 0, "p99_ms": 0}

        sorted_latencies = sorted(self.latencies)
        n = len(sorted_latencies)

        return {
            "p50_ms": sorted_latencies[int(n * 0.50)],
            "p95_ms": sorted_latencies[int(n * 0.95)],
            "p99_ms": sorted_latencies[int(n * 0.99)],
        }

    def get_throughput(self, duration_sec: float) -> float:
        """Calculate operations per second.

        Args:
            duration_sec: Total duration in seconds

        Returns:
            Operations per second
        """
        if duration_sec <= 0:
            return 0
        return self.operations / duration_sec
