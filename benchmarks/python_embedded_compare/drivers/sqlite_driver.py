"""SQLite driver for benchmark comparisons.

This driver implements fair benchmarking for SQLite with explicit
transaction control and configurable durability modes.
"""

import os
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Tuple

from drivers.base_driver import BenchmarkMetrics, DatabaseDriver, EngineMetadata


class SQLiteDriver(DatabaseDriver):
    """SQLite database driver for benchmarking."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.db_path = config.get("database_path", ":memory:")
        self.variant = config.get("variant", "wal_full")  # wal_full, wal_normal
        self._statement_cursors: Dict[str, sqlite3.Cursor] = {}

    @property
    def name(self) -> str:
        return f"SQLite_{self.variant}"

    def connect(self) -> bool:
        try:
            # Use isolation_level=None for explicit transaction control
            # This prevents sqlite3 from auto-committing
            self.connection = sqlite3.connect(
                self.db_path, isolation_level=None, timeout=30.0
            )
            self.connection.row_factory = sqlite3.Row
            self._cursor = self.connection.cursor()

            # Set up PRAGMA based on variant
            self._setup_pragmas()

            return True
        except Exception as e:
            print(f"Failed to connect to SQLite: {e}", file=sys.stderr)
            return False

    def _setup_pragmas(self):
        """Configure SQLite PRAGMA settings based on variant."""
        if self.variant == "wal_full":
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.execute("PRAGMA wal_autocheckpoint=0")
        elif self.variant == "wal_normal":
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=NORMAL")
        else:
            # Default to DELETE mode with FULL sync
            self.connection.execute("PRAGMA journal_mode=DELETE")
            self.connection.execute("PRAGMA synchronous=FULL")

        # Common settings
        self.connection.execute("PRAGMA cache_size=-64000")  # 64MB cache
        self.connection.execute("PRAGMA temp_store=MEMORY")

    def disconnect(self):
        if self.connection:
            try:
                self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except:
                pass
            self.connection.close()
            self.connection = None
            for cursor in self._statement_cursors.values():
                try:
                    cursor.close()
                except Exception:
                    pass
            self._statement_cursors.clear()

    def _get_cursor(self, sql: str) -> sqlite3.Cursor:
        cursor = self._statement_cursors.get(sql)
        if cursor is None:
            cursor = self.connection.cursor()
            self._statement_cursors[sql] = cursor
        return cursor

    def create_schema(self, schema_sql: str):
        for statement in schema_sql.split(";"):
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
        return [dict(row) for row in rows]

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
        self.connection.execute("BEGIN")

    def commit(self):
        self.connection.execute("COMMIT")

    def rollback(self):
        self.connection.execute("ROLLBACK")

    def prepare_statement(self, sql: str) -> sqlite3.Cursor:
        return sql, self._get_cursor(sql)

    def execute_prepared(
        self, handle: sqlite3.Cursor, params: Optional[Tuple] = None
    ) -> Any:
        sql, cursor = handle
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor.fetchall() if cursor.description else cursor.rowcount

    def get_engine_metadata(self) -> EngineMetadata:
        cursor = self.connection.cursor()
        cursor.execute("SELECT sqlite_version()")
        version = cursor.fetchone()[0]

        return EngineMetadata(
            name="SQLite",
            version=version,
            runtime_version=sys.version,
            config_notes=self._get_config_notes(),
        )

    def _get_config_notes(self) -> str:
        """Get current configuration notes."""
        notes = []

        # Get journal mode
        cursor = self.connection.cursor()
        cursor.execute("PRAGMA journal_mode")
        journal = cursor.fetchone()[0]
        notes.append(f"journal_mode={journal}")

        # Get synchronous
        cursor.execute("PRAGMA synchronous")
        sync = cursor.fetchone()[0]
        sync_name = {0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA"}.get(sync, str(sync))
        notes.append(f"synchronous={sync_name}")

        return f"variant={self.variant};{' '.join(notes)}"

    def set_durability_mode(self, mode: str):
        if mode == "durable":
            self.connection.execute("PRAGMA synchronous=FULL")
        elif mode == "relaxed":
            self.connection.execute("PRAGMA synchronous=NORMAL")

    def get_storage_size(self) -> int:
        """Get database file size including WAL."""
        total = 0

        # Main database file
        if self.db_path != ":memory:" and os.path.exists(self.db_path):
            total += os.path.getsize(self.db_path)

            # WAL file
            wal_path = f"{self.db_path}-wal"
            if os.path.exists(wal_path):
                total += os.path.getsize(wal_path)

            # SHM file
            shm_path = f"{self.db_path}-shm"
            if os.path.exists(shm_path):
                total += os.path.getsize(shm_path)

        return total

    def get_config_notes(self) -> str:
        return self._get_config_notes()
