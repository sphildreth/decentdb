"""
PostgreSQL reference adapter for differential testing.

Provides a unified interface to run SQL against PostgreSQL and compare
with DecentDB results.
"""

import os
import subprocess
from typing import Optional


class PostgresRef:
    """Adapter for running SQL against PostgreSQL reference database."""

    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize PostgreSQL connection.

        Args:
            connection_string: PostgreSQL connection string. If None, uses
                PGDATABASE environment variable or defaults to 'postgres'.
        """
        self.conn_str = connection_string or os.environ.get("PGDATABASE", "postgres")
        self.psql = self._find_psql()
        if not self.psql:
            raise RuntimeError("psql not found in PATH")

    def _find_psql(self) -> Optional[str]:
        """Find psql executable."""
        import shutil

        return shutil.which("psql")

    def execute(self, sql: str) -> tuple[bool, list[str], str]:
        """
        Execute SQL against PostgreSQL.

        Args:
            sql: SQL statement to execute

        Returns:
            Tuple of (success, rows, error_message)
            - success: True if query succeeded
            - rows: List of result rows (as strings)
            - error_message: Error message if failed, empty string if succeeded
        """
        cmd = [
            self.psql,
            "-X",  # No psqlrc
            "-q",  # Quiet
            "-t",  # Tuples only
            "-A",  # Unaligned output
            "-v",
            "ON_ERROR_STOP=1",  # Stop on error
            "-c",
            sql,
        ]

        # Add connection info if specified
        if self.conn_str and not os.environ.get("PGDATABASE"):
            # Assume conn_str is a database name for simple case
            cmd.extend(["-d", self.conn_str])

        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if proc.returncode != 0:
            return False, [], proc.stderr.strip()

        # Parse output - rows are newline-separated
        rows = [
            line.strip() for line in proc.stdout.strip().splitlines() if line.strip()
        ]
        return True, rows, ""

    def create_schema(self, schema_name: str) -> bool:
        """Create a new schema for testing."""
        sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        success, _, error = self.execute(sql)
        return success

    def drop_schema(self, schema_name: str) -> bool:
        """Drop a test schema."""
        sql = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"
        success, _, error = self.execute(sql)
        return success

    def execute_in_schema(
        self, schema_name: str, sql: str
    ) -> tuple[bool, list[str], str]:
        """Execute SQL within a specific schema context."""
        # Prepend schema to table names or set search_path
        set_schema = f"SET search_path TO {schema_name}, public"
        full_sql = f"{set_schema}; {sql}"
        return self.execute(full_sql)


def normalize_row(row: str) -> str:
    """Normalize a row string for comparison."""
    # Handle different NULL representations
    if row == "NULL" or row == "":
        return "NULL"
    return row


def compare_results(
    decent_rows: list[str], postgres_rows: list[str], ignore_order: bool = False
) -> tuple[bool, str]:
    """
    Compare DecentDB and PostgreSQL results.

    Args:
        decent_rows: Results from DecentDB
        postgres_rows: Results from PostgreSQL
        ignore_order: If True, compare as sets; if False, compare as ordered lists

    Returns:
        Tuple of (match, message)
    """
    # Normalize rows
    decent_normalized = [normalize_row(r) for r in decent_rows]
    postgres_normalized = [normalize_row(r) for r in postgres_rows]

    if ignore_order:
        decent_set = set(decent_normalized)
        postgres_set = set(postgres_normalized)
        if decent_set == postgres_set:
            return True, ""
        missing_in_decent = postgres_set - decent_set
        extra_in_decent = decent_set - postgres_set
        msg = f"Row mismatch (ignoring order). Missing: {missing_in_decent}, Extra: {extra_in_decent}"
        return False, msg
    else:
        if decent_normalized == postgres_normalized:
            return True, ""

        # Find first mismatch
        for i, (d, p) in enumerate(zip(decent_normalized, postgres_normalized)):
            if d != p:
                return False, f"Row {i} differs: DecentDB='{d}' vs PostgreSQL='{p}'"

        # Length mismatch
        if len(decent_normalized) < len(postgres_normalized):
            return (
                False,
                f"DecentDB has {len(decent_normalized)} rows, PostgreSQL has {len(postgres_normalized)}",
            )
        elif len(decent_normalized) > len(postgres_normalized):
            return (
                False,
                f"DecentDB has {len(decent_normalized)} rows, PostgreSQL has {len(postgres_normalized)}",
            )

        return True, ""
