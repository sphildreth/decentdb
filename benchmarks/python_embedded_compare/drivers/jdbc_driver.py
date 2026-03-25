"""JDBC driver for Java-based embedded databases (H2, Derby, HSQLDB).

This driver uses JayDeBeApi to connect to Java databases via JDBC.
Note: JVM-based engines will have Python-to-Java bridge overhead.
"""

import os
import sys
from typing import Any, Dict, List, Optional, Tuple

from drivers.base_driver import BenchmarkMetrics, DatabaseDriver, EngineMetadata

# Try to import JayDeBeApi
try:
    import jaydebeapi

    JAYDEBEAPI_AVAILABLE = True
except ImportError:
    JAYDEBEAPI_AVAILABLE = False

try:
    import jpype

    JPYPE_AVAILABLE = True
except ImportError:
    JPYPE_AVAILABLE = False


class JDBCDriver(DatabaseDriver):
    """Generic JDBC driver for Java-based embedded databases."""

    ENGINE_CONFIGS = {
        "h2": {
            "name": "H2",
            "jdbc_prefix": "jdbc:h2:",
            "driver_class": "org.h2.Driver",
            "jar_path": "h2.jar",
            "jdbc_suffix": ";DB_CLOSE_DELAY=-1",
        },
        "derby": {
            "name": "Apache Derby",
            "jdbc_prefix": "jdbc:derby:",
            "driver_class": "org.apache.derby.iapi.jdbc.AutoloadedDriver",
            "jar_path": "derby.jar",
            "jdbc_suffix": ";create=true",
        },
        "hsqldb": {
            "name": "HSQLDB",
            "jdbc_prefix": "jdbc:hsqldb:",
            "driver_class": "org.hsqldb.jdbc.JDBCDriver",
            "jar_path": "hsqldb.jar",
            "jdbc_suffix": ";hsqldb.write_delay=false",
        },
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.engine = config.get("engine", "h2").lower()
        self.db_path = config.get("database_path", "mem:test")
        self.jdbc_url = config.get("jdbc_url", "")
        self.driver_class = config.get("driver_class", "")
        self.jar_paths = config.get("jar_paths", [])
        self._prepared_stmts: Dict[str, Any] = {}

        # Get engine-specific config
        if self.engine in self.ENGINE_CONFIGS:
            eng_cfg = self.ENGINE_CONFIGS[self.engine]
            if not self.jdbc_url:
                suffix = eng_cfg.get("jdbc_suffix", "")
                self.jdbc_url = f"{eng_cfg['jdbc_prefix']}{self.db_path}{suffix}"
            else:
                self.jdbc_url = self._resolve_jdbc_url(self.jdbc_url)
            if not self.driver_class:
                self.driver_class = eng_cfg["driver_class"]
            if not self.jar_paths and eng_cfg.get("jar_path"):
                self.jar_paths = [eng_cfg["jar_path"]]

    def _resolve_jdbc_url(self, jdbc_url: str) -> str:
        if "{db_path}" in jdbc_url:
            return jdbc_url.format(db_path=self.db_path)

        if self.engine == "h2" and jdbc_url.startswith("jdbc:h2:mem:"):
            prefix, separator, suffix = jdbc_url.partition(";")
            unique_name = "".join(
                ch if ch.isalnum() else "_" for ch in os.path.abspath(self.db_path)
            )
            resolved = f"jdbc:h2:mem:{unique_name}"
            if separator:
                resolved = f"{resolved};{suffix}"
            return resolved

        return jdbc_url

    def _adapt_sql(self, sql: str) -> str:
        adapted = sql

        if self.engine in {"derby", "hsqldb"}:
            adapted = adapted.replace(" TEXT ", " VARCHAR(255) ")
            adapted = adapted.replace(" TEXT,", " VARCHAR(255),")
            adapted = adapted.replace(" TEXT\n", " VARCHAR(255)\n")
            adapted = adapted.replace(" referrer TEXT", " referrer VARCHAR(255)")

        if self.engine == "derby":
            adapted = adapted.replace(" ORDER BY created_at LIMIT ?", " ORDER BY created_at FETCH FIRST ? ROWS ONLY")
            adapted = adapted.replace(" ORDER BY o.created_at LIMIT ?", " ORDER BY o.created_at FETCH FIRST ? ROWS ONLY")

        return adapted

    @property
    def name(self) -> str:
        return f"{self.ENGINE_CONFIGS.get(self.engine, {}).get('name', 'JDBC')}(JDBC)"

    def connect(self) -> bool:
        if not JAYDEBEAPI_AVAILABLE:
            print(
                "JayDeBeApi not available. Install: pip install JayDeBeApi JPype1",
                file=sys.stderr,
            )
            return False

        try:
            # Build classpath from jar_paths
            classpath = ":".join(self.jar_paths) if self.jar_paths else None

            if JPYPE_AVAILABLE and self.jar_paths:
                for jar_path in self.jar_paths:
                    jpype.addClassPath(jar_path)

            self.connection = jaydebeapi.connect(
                self.driver_class,
                self.jdbc_url,
                [],  # No auth for embedded
                classpath,
            )
            self.connection.jconn.setAutoCommit(False)

            return True
        except Exception as e:
            print(f"Failed to connect to {self.engine} via JDBC: {e}", file=sys.stderr)
            return False

    def disconnect(self):
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
            self.connection = None

    def create_schema(self, schema_sql: str):
        cursor = self.connection.cursor()
        statements = self._adapt_sql(schema_sql).split(";")
        for statement in statements:
            statement = statement.strip()
            if statement:
                cursor.execute(statement)
        cursor.close()

    def drop_table(self, table_name: str):
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()

    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        sql = self._adapt_sql(sql)
        cursor = self.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        rows = cursor.fetchall()
        cursor.close()

        return [dict(zip(columns, row)) for row in rows]

    def execute_update(self, sql: str, params: Optional[Tuple] = None) -> int:
        sql = self._adapt_sql(sql)
        cursor = self.connection.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        rowcount = cursor.rowcount
        cursor.close()
        return rowcount

    def execute_many(self, sql: str, params_list: List[Tuple]) -> int:
        sql = self._adapt_sql(sql)
        cursor = self.connection.cursor()
        cursor.executemany(sql, params_list)
        rowcount = cursor.rowcount * len(params_list)
        cursor.close()
        return rowcount

    def begin_transaction(self):
        # Auto-commit is already off, but we explicitly begin
        pass

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def prepare_statement(self, sql: str):
        sql = self._adapt_sql(sql)
        cursor = self.connection.cursor()
        cursor.prepare(sql)
        self._prepared_stmts[sql] = cursor
        return cursor

    def execute_prepared(self, handle, params: Optional[Tuple] = None) -> Any:
        if params:
            handle.execute(params)
        else:
            handle.execute()

        if handle.description:
            return handle.fetchall()
        return handle.rowcount

    def get_engine_metadata(self) -> EngineMetadata:
        # Try to get version from the engine
        version = "unknown"
        try:
            cursor = self.connection.cursor()
            if self.engine == "h2":
                cursor.execute("SELECT H2VERSION()")
                result = cursor.fetchone()
                if result:
                    version = str(result[0])
            elif self.engine == "derby":
                cursor.execute(
                    "SELECT SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('DataDictionaryVersion')"
                )
                result = cursor.fetchone()
                if result:
                    version = str(result[0])
            elif self.engine == "hsqldb":
                cursor.execute("SELECT AVG(ID) FROM INFORMATION_SCHEMA.SYSTEM_TABLES")
                version = "HSQLDB"
            cursor.close()
        except:
            pass

        return EngineMetadata(
            name=self.ENGINE_CONFIGS.get(self.engine, {}).get("name", "JDBC"),
            version=version,
            runtime_version=f"JDBC (JayDeBeApi)",
            config_notes=self._get_config_notes(),
        )

    def _get_config_notes(self) -> str:
        """Get current configuration notes."""
        return f"engine={self.engine};jdbc_url={self.jdbc_url}"

    def set_durability_mode(self, mode: str):
        cursor = self.connection.cursor()

        if self.engine == "h2":
            # H2 v2 uses different settings
            # For durable: use LOG=0 and MVCC
            # For relaxed: use default settings
            pass  # H2 defaults are already reasonable for benchmarks
        elif self.engine == "derby":
            if mode == "durable":
                try:
                    cursor.execute(
                        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.syncMethod', 'full')"
                    )
                except:
                    pass  # May not be supported
        elif self.engine == "hsqldb":
            if mode == "durable":
                cursor.execute("SET DATABASE DEFAULT TABLE TYPE CACHED")
                cursor.execute("SET DATABASE TRANSACTION CONTROL LOCKS")

        cursor.close()

    def get_storage_size(self) -> int:
        """Get database storage size."""
        total = 0

        # For file-based databases, calculate size
        if self.db_path and not self.db_path.startswith("mem:"):
            base_path = self.db_path.split(";")[0]
            if os.path.exists(base_path):
                total += os.path.getsize(base_path)

            # Look for related files
            for ext in [".data", ".log", ".tmp", ".lck", ".lob"]:
                alt_path = base_path + ext
                if os.path.exists(alt_path):
                    total += os.path.getsize(alt_path)

        return total

    def get_config_notes(self) -> str:
        return self._get_config_notes()
