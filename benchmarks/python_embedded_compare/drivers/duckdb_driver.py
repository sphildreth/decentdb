"""DuckDB driver for benchmark comparisons.

This driver implements fair benchmarking for DuckDB with explicit
transaction control. Note: DuckDB is primarily analytical, so we
use single-threaded mode for fair OLTP comparison.
"""

import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from drivers.base_driver import BenchmarkMetrics, DatabaseDriver, EngineMetadata


class DuckDBDriver(DatabaseDriver):
    """DuckDB database driver for benchmarking."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.db_path = config.get("database_path", "duckdb.db")
        self._cursor = None
        self._prepared_stmts: Dict[str, duckdb.Statement] = {}

    @property
    def name(self) -> str:
        return "DuckDB"

    def connect(self) -> bool:
        try:
            self.connection = duckdb.connect(self.db_path, read_only=False)
            self._cursor = self.connection.cursor()

            # Use single thread for fair OLTP comparison
            self.connection.execute("SET threads = 1")

            return True
        except Exception as e:
            print(f"Failed to connect to DuckDB: {e}", file=sys.stderr)
            return False

    def disconnect(self):
        if self.connection:
            try:
                self.connection.execute("CHECKPOINT")
            except:
                pass
            self.connection.close()
            self.connection = None
            self._cursor = None
            self._prepared_stmts.clear()

    def create_schema(self, schema_sql: str):
        # DuckDB supports multiple statements in execute_batch
        statements = schema_sql.split(";")
        for statement in statements:
            statement = statement.strip()
            if statement:
                self.connection.execute(statement)

    def drop_table(self, table_name: str):
        self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")

    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        cursor = self.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        # Get column names from description
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        # Fetch all results
        result = cursor.fetchall()

        return [dict(zip(columns, row)) for row in result]

    def execute_update(self, sql: str, params: Optional[Tuple] = None) -> int:
        if params:
            self.connection.execute(sql, params)
        else:
            self.connection.execute(sql)
        # DuckDB doesn't return rowcount directly, estimate
        return 1

    def execute_many(self, sql: str, params_list: List[Tuple]) -> int:
        self.connection.executemany(sql, params_list)
        return len(params_list)

    def begin_transaction(self):
        self.connection.execute("BEGIN TRANSACTION")

    def commit(self):
        self.connection.execute("COMMIT")

    def rollback(self):
        self.connection.execute("ROLLBACK")

    def prepare_statement(self, sql: str) -> duckdb.Stmt:
        stmt = self.connection.prepare(sql)
        self._prepared_stmts[sql] = stmt
        return stmt

    def execute_prepared(
        self, handle: duckdb.Stmt, params: Optional[Tuple] = None
    ) -> Any:
        if params:
            result = handle.execute(params)
        else:
            result = handle.execute()
        return result.fetchall()

    def get_engine_metadata(self) -> EngineMetadata:
        version = duckdb.__version__

        return EngineMetadata(
            name="DuckDB",
            version=version,
            runtime_version=sys.version,
            config_notes=self._get_config_notes(),
        )

    def _get_config_notes(self) -> str:
        """Get current configuration notes."""
        notes = []

        # Get thread count
        result = self.connection.execute("SELECT current_setting('threads')").fetchone()
        threads = result[0] if result else "unknown"
        notes.append(f"threads={threads}")

        return ";".join(notes)

    def set_durability_mode(self, mode: str):
        # DuckDB's durability is not as configurable as SQLite
        # We document this and use default settings
        pass

    def get_storage_size(self) -> int:
        """Get database file size."""
        total = 0

        if os.path.exists(self.db_path):
            total += os.path.getsize(self.db_path)

            # WAL file
            wal_path = f"{self.db_path}.wal"
            if os.path.exists(wal_path):
                total += os.path.getsize(wal_path)

        return total

    def get_config_notes(self) -> str:
        return self._get_config_notes()
