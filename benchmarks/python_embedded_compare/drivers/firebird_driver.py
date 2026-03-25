"""Firebird driver for benchmark comparisons.

This driver interfaces with Firebird embedded using Jaybird JDBC driver.
Firebird is accessed via JDBC like H2/Derby/HSQLDB.
"""

import os
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

        jdbc_url = config.get("jdbc_url", f"jdbc:firebirdsql:{db_path}")
        if "{db_path}" in jdbc_url:
            jdbc_url = jdbc_url.format(db_path=db_path)
        self.jdbc_url = jdbc_url
        self.driver_class = config.get("driver_class", "org.firebirdsql.jdbc.FBDriver")
        self.jar_paths = config.get("jar_paths", [])
        self.firebird_home = config.get("firebird_home", "")
        self.firebird_conf = config.get("firebird_conf", "")
        self.firebird_lib_dir = config.get("firebird_lib_dir", "")

    @property
    def name(self) -> str:
        return "Firebird"

    def connect(self) -> bool:
        if self.firebird_home:
            os.environ["FIREBIRD"] = self.firebird_home
        if self.firebird_conf:
            os.environ["FIREBIRD_CONF"] = self.firebird_conf
        if self.firebird_lib_dir:
            existing = os.environ.get("LD_LIBRARY_PATH", "")
            paths = [self.firebird_lib_dir]
            if existing:
                paths.append(existing)
            os.environ["LD_LIBRARY_PATH"] = ":".join(paths)
        return super().connect()

    def set_durability_mode(self, mode: str):
        # Firebird durability is managed differently
        # This is a placeholder for future implementation
        pass
