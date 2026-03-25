"""Firebird driver for benchmark comparisons.

This driver interfaces with Firebird embedded using Jaybird JDBC driver.
Firebird is accessed via JDBC like H2/Derby/HSQLDB.
"""

from typing import Any, Dict, List, Optional, Tuple

from drivers.jdbc_driver import JDBCDriver


class FirebirdDriver(JDBCDriver):
    """Firebird embedded database driver for benchmarking."""

    ENGINE_CONFIGS = {
        "firebird": {
            "name": "Firebird",
            "jdbc_prefix": "jdbc:firebirdsql:",
            "driver_class": "org.firebirdsql.jdbc.FBDriver",
        },
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.engine = "firebird"

        # Build JDBC URL for embedded Firebird
        db_path = config.get("database_path", "firebird.db")
        if not db_path.startswith("embedded:"):
            db_path = f"embedded:{db_path}"

        self.jdbc_url = config.get("jdbc_url", f"jdbc:firebirdsql:{db_path}")
        self.driver_class = config.get("driver_class", "org.firebirdsql.jdbc.FBDriver")
        self.jar_paths = config.get("jar_paths", [])

    @property
    def name(self) -> str:
        return "Firebird"

    def set_durability_mode(self, mode: str):
        # Firebird durability is managed differently
        # This is a placeholder for future implementation
        pass
