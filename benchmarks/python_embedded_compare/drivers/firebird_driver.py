"""Firebird driver for benchmark comparisons.

This driver interfaces with Firebird embedded using Jaybird JDBC driver.
Firebird is accessed via JDBC like H2/Derby/HSQLDB.
"""

import os
import tempfile
from pathlib import Path
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
        self.connection_properties = dict(config.get("connection_properties", {}))
        self.jvm_properties = dict(config.get("jvm_properties", {}))

        vendor_dir = Path(__file__).resolve().parents[1] / "vendor"
        for jar_name in ("jaybird-native-6.0.4.jar", "jna-jpms-5.18.1.jar"):
            jar_path = str(vendor_dir / jar_name)
            if os.path.exists(jar_path) and jar_path not in self.jar_paths:
                self.jar_paths.append(jar_path)

        native_library_path = self._prepare_native_library_path(
            self.firebird_lib_dir or self.firebird_home
        )
        if native_library_path and "nativeLibraryPath" not in self.connection_properties:
            self.connection_properties["nativeLibraryPath"] = native_library_path
        if native_library_path and "jna.library.path" not in self.jvm_properties:
            self.jvm_properties["jna.library.path"] = native_library_path
        self.connection_properties.setdefault("user", config.get("user", "SYSDBA"))
        self.connection_properties.setdefault("password", config.get("password", "masterkey"))
        self.connection_properties.setdefault("createDatabaseIfNotExist", "true")

    def _prepare_native_library_path(self, native_library_path: str) -> str:
        if not native_library_path:
            return native_library_path

        lib_dir = Path(native_library_path)
        if not lib_dir.is_dir():
            lib_dir = lib_dir.parent

        libfbclient = lib_dir / "libfbclient.so"
        if libfbclient.exists():
            return str(lib_dir)

        candidates = sorted(lib_dir.glob("libfbclient.so.*"))
        if not candidates:
            return str(lib_dir)

        shim_dir = Path(tempfile.gettempdir()) / "decentdb-firebird-lib"
        shim_dir.mkdir(parents=True, exist_ok=True)
        shim_target = shim_dir / "libfbclient.so"
        if shim_target.exists() or shim_target.is_symlink():
            shim_target.unlink()
        shim_target.symlink_to(candidates[0])
        return str(shim_dir)

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
