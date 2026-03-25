"""LiteDB driver for benchmark comparisons via subprocess harness.

LiteDB is a .NET embedded document database. We communicate with it via a
small C# harness that executes benchmarks and returns JSON results.
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from drivers.base_driver import BenchmarkMetrics, DatabaseDriver, EngineMetadata


class LiteDBDriver(DatabaseDriver):
    """LiteDB driver using subprocess harness."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.db_path = config.get("database_path", "litedb.db")
        self.harness_path = config.get("harness_path", "")
        self._process = None

    @property
    def name(self) -> str:
        return "LiteDB"

    def connect(self) -> bool:
        # For LiteDB, we just verify .NET is available
        # Actual "connection" happens per-operation via subprocess
        try:
            result = subprocess.run(
                ["dotnet", "--version"], capture_output=True, text=True, timeout=5
            )
            if result.returncode != 0:
                print("dotnet not available", file=sys.stderr)
                return False
            return True
        except Exception as e:
            print(f"Failed to check dotnet: {e}", file=sys.stderr)
            return False

    def disconnect(self):
        # No persistent connection
        pass

    def create_schema(self, schema_sql: str):
        # LiteDB is schemaless - we create collections via the harness
        pass

    def drop_table(self, table_name: str):
        # LiteDB collections - handled by harness
        pass

    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict]:
        # Not used - LiteDB uses document API, handled by harness
        return []

    def execute_update(self, sql: str, params: Optional[Tuple] = None) -> int:
        # Not used - handled by harness
        return 0

    def execute_many(self, sql: str, params_list: List[Tuple]) -> int:
        # Not used - handled by harness
        return 0

    def begin_transaction(self):
        # Not applicable for LiteDB subprocess mode
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def prepare_statement(self, sql: str):
        return sql

    def execute_prepared(self, handle, params: Optional[Tuple] = None) -> Any:
        return []

    def get_engine_metadata(self) -> EngineMetadata:
        try:
            result = subprocess.run(
                ["dotnet", "--version"], capture_output=True, text=True, timeout=5
            )
            version = result.stdout.strip() if result.returncode == 0 else "unknown"
        except:
            version = "unknown"

        return EngineMetadata(
            name="LiteDB",
            version="document_db",
            runtime_version=version,
            config_notes="subprocess_harness",
        )

    def set_durability_mode(self, mode: str):
        pass

    def get_storage_size(self) -> int:
        if os.path.exists(self.db_path):
            return os.path.getsize(self.db_path)
        return 0

    def get_config_notes(self) -> str:
        return "document_store"
