"""DecentDB driver for benchmark comparisons.

This driver aligns with the existing Python binding benchmarks by reusing
statement cursors and leaning on the binding's internal statement cache.
"""

import os
import sys
from typing import Any, Dict, List, Optional, Tuple

from drivers.base_driver import BenchmarkMetrics, DatabaseDriver, EngineMetadata

try:
    import decentdb

    DECENTDB_AVAILABLE = True
except ImportError:
    DECENTDB_AVAILABLE = False


class DecentDBDriver(DatabaseDriver):
    """DecentDB database driver for benchmarking."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.db_path = config.get("database_path", "decentdb.db")
        self._statement_cursors: Dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "DecentDB"

    def connect(self) -> bool:
        if not DECENTDB_AVAILABLE:
            print("DecentDB Python bindings not available", file=sys.stderr)
            return False

        try:
            # Remove existing database if it exists for clean benchmark
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
            wal_path = f"{self.db_path}.wal"
            if os.path.exists(wal_path):
                os.remove(wal_path)

            # Connect using DB-API
            self.connection = decentdb.connect(
                self.db_path,
                stmt_cache_size=int(self.config.get("stmt_cache_size", 512)),
            )

            return True
        except Exception as e:
            print(f"Failed to connect to DecentDB: {e}", file=sys.stderr)
            return False

    def disconnect(self):
        if self.connection:
            try:
                for cursor in self._statement_cursors.values():
                    try:
                        cursor.close()
                    except Exception:
                        pass
                self._statement_cursors.clear()
                self.connection.close()
            except Exception:
                pass
            self.connection = None

    def _get_cursor(self, sql: str):
        cursor = self._statement_cursors.get(sql)
        if cursor is None:
            cursor = self.connection.cursor()
            self._statement_cursors[sql] = cursor
        return cursor

    def create_schema(self, schema_sql: str):
        statements = schema_sql.split(";")
        for statement in statements:
            statement = statement.strip()
            if statement:
                self.connection.execute(statement)

    def drop_table(self, table_name: str):
        self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")

    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        cursor = self._get_cursor(sql)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        rows = cursor.fetchall()

        # Get column names from description
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        return []

    def execute_update(self, sql: str, params: Optional[Tuple] = None) -> int:
        cursor = self._get_cursor(sql)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        return cursor.rowcount

    def execute_many(self, sql: str, params_list: List[Tuple]) -> int:
        cursor = self._get_cursor(sql)
        cursor.executemany(sql, params_list)
        return cursor.rowcount

    def begin_transaction(self):
        # DecentDB is in autocommit mode by default
        # Use explicit BEGIN
        self.connection.execute("BEGIN")

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def prepare_statement(self, sql: str):
        return sql, self._get_cursor(sql)

    def execute_prepared(self, handle, params: Optional[Tuple] = None) -> Any:
        sql, cursor = handle
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        if cursor.description:
            return cursor.fetchall()
        return cursor.rowcount

    def get_engine_metadata(self) -> EngineMetadata:
        version = "unknown"
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT decentdb_version()")
            result = cursor.fetchone()
            if result:
                version = str(result[0])
            cursor.close()
        except Exception:
            version = getattr(decentdb, "__version__", "unknown")

        return EngineMetadata(
            name="DecentDB",
            version=version,
            runtime_version=sys.version,
            config_notes=self._get_config_notes(),
        )

    def _get_config_notes(self) -> str:
        """Get current configuration notes."""
        stmt_cache_size = int(self.config.get("stmt_cache_size", 512))
        return f"stmt_cache_size={stmt_cache_size}"

    def set_durability_mode(self, mode: str):
        # DecentDB uses WAL with fsync by default
        # This is a placeholder for future configuration
        pass

    def get_storage_size(self) -> int:
        """Get database file size including WAL."""
        total = 0

        if os.path.exists(self.db_path):
            total += os.path.getsize(self.db_path)

        wal_path = f"{self.db_path}.wal"
        if os.path.exists(wal_path):
            total += os.path.getsize(wal_path)

        return total

    def get_config_notes(self) -> str:
        return self._get_config_notes()
