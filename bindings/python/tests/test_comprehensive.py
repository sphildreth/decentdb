"""
Comprehensive DB-API 2.0 compliance and feature tests for DecentDB Python bindings.
This file aims for complete coverage of DB-API specification and common use cases.
"""

import pytest
import decentdb
import datetime
import decimal
import uuid
import os


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_comprehensive.ddb")


class TestDBAPIGlobals:
    """Test DB-API 2.0 global attributes."""

    def test_apilevel(self):
        """Verify DB-API level is 2.0."""
        assert decentdb.apilevel == "2.0"

    def test_threadsafety(self):
        """Verify thread safety level is 1 (connections not shared)."""
        assert decentdb.threadsafety == 1

    def test_paramstyle(self):
        """Verify parameter style is qmark."""
        assert decentdb.paramstyle == "qmark"

    def test_exception_hierarchy(self):
        """Verify exception hierarchy is correct."""
        # Base exceptions
        assert issubclass(decentdb.Error, Exception)
        assert issubclass(decentdb.Warning, Exception)

        # Database errors
        assert issubclass(decentdb.DatabaseError, decentdb.Error)
        assert issubclass(decentdb.InterfaceError, decentdb.Error)
        assert issubclass(decentdb.InternalError, decentdb.DatabaseError)
        assert issubclass(decentdb.OperationalError, decentdb.DatabaseError)
        assert issubclass(decentdb.ProgrammingError, decentdb.DatabaseError)
        assert issubclass(decentdb.IntegrityError, decentdb.DatabaseError)
        assert issubclass(decentdb.DataError, decentdb.DatabaseError)
        assert issubclass(decentdb.NotSupportedError, decentdb.DatabaseError)


class TestConnectionBasicsComprehensive:
    """Test Connection object basics."""

    def test_connection_in_memory(self):
        """Test connecting to in-memory database."""
        conn = decentdb.connect(":memory:")
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
        conn.close()

    def test_connection_with_options(self):
        """Test connecting with options string."""
        conn = decentdb.connect(":memory:", options="")
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
        conn.close()

    def test_connection_stmt_cache_disabled(self):
        """Test connecting with statement cache disabled (size=0)."""
        conn = decentdb.connect(":memory:", stmt_cache_size=0)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
        conn.close()

    def test_connection_stmt_cache_small(self, db_path):
        """Test connecting with small statement cache."""
        conn = decentdb.connect(db_path, stmt_cache_size=1)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

        # Execute same query twice
        cur.execute("SELECT * FROM foo")
        cur.fetchone()
        cur.execute("SELECT * FROM foo")
        cur.fetchone()

        # With cache size 1, queries might be recycled - just verify no errors
        conn.close()

    def test_connection_closed_via_internal_attribute(self, db_path):
        """Test closed connection detection via internal attribute."""
        conn = decentdb.connect(db_path)
        assert conn._closed is False
        conn.close()
        assert conn._closed is True

    def test_connection_cursor_returns_cursor(self):
        """Test that cursor() returns a Cursor object."""
        conn = decentdb.connect(":memory:")
        cur = conn.cursor()
        assert isinstance(cur, decentdb.Cursor)
        conn.close()


class TestCursorAttributesComprehensive:
    """Test Cursor object attributes (DB-API 2.0)."""

    def test_cursor_description_after_select(self, db_path):
        """Test cursor.description after SELECT."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, name TEXT, amount FLOAT64)")
        cur.execute("INSERT INTO foo VALUES (1, 'test', 1.5)")
        conn.commit()

        cur.execute("SELECT id, name, amount FROM foo")
        desc = cur.description

        assert desc is not None
        assert len(desc) == 3
        assert desc[0][0] == "id"
        assert desc[1][0] == "name"
        assert desc[2][0] == "amount"
        conn.close()

    def test_cursor_description_after_dml(self, db_path):
        """Test cursor.description after INSERT/UPDATE/DELETE."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")

        assert cur.description is None
        conn.close()

    def test_cursor_rowcount_after_operations(self, db_path):
        """Test cursor.rowcount after various operations."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")

        # After INSERT
        cur.execute("INSERT INTO foo VALUES (1)")
        cur.execute("INSERT INTO foo VALUES (2)")
        assert cur.rowcount == 1

        # After UPDATE
        cur.execute("UPDATE foo SET id = 3 WHERE id = 1")
        assert cur.rowcount == 1

        # After DELETE
        cur.execute("DELETE FROM foo WHERE id = 3")
        assert cur.rowcount == 1

        # After SELECT (should be -1 per DB-API)
        cur.execute("SELECT * FROM foo")
        assert cur.rowcount == -1

        conn.close()

    def test_cursor_execute_two_float_range_scan(self, db_path):
        """Two-float execute paths should fall back cleanly when prefetch declines."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE items (id INT64, name TEXT, price FLOAT64)")
        cur.execute("INSERT INTO items VALUES (1, 'a', 1.0)")
        cur.execute("INSERT INTO items VALUES (2, 'b', 1.5)")
        cur.execute("INSERT INTO items VALUES (3, 'c', 3.0)")
        conn.commit()

        cur.execute(
            "SELECT id, name, price FROM items WHERE price >= ? AND price < ? ORDER BY price LIMIT 100",
            (1.0, 2.0),
        )

        assert cur.fetchall() == [(1, "a", 1.0), (2, "b", 1.5)]
        assert cur.rowcount == -1
        conn.close()

    def test_cursor_arraysize_default(self, db_path):
        """Test cursor.arraysize default value."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        assert cur.arraysize == 1  # DB-API default
        conn.close()


class TestSQLFeaturesComprehensive:
    """Test SQL features."""

    def test_inner_join(self, db_path):
        """Test INNER JOIN."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE a (id INT64, name TEXT)")
        cur.execute("CREATE TABLE b (aid INT64, value TEXT)")
        cur.execute("INSERT INTO a VALUES (1, 'alice')")
        cur.execute("INSERT INTO a VALUES (2, 'bob')")
        cur.execute("INSERT INTO b VALUES (1, 'x')")
        cur.execute("INSERT INTO b VALUES (2, 'y')")
        conn.commit()

        cur.execute(
            "SELECT a.name, b.value FROM a INNER JOIN b ON a.id = b.aid ORDER BY a.name"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == ("alice", "x")
        assert rows[1] == ("bob", "y")
        conn.close()

    def test_left_join(self, db_path):
        """Test LEFT JOIN."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE a (id INT64, name TEXT)")
        cur.execute("CREATE TABLE b (aid INT64, value TEXT)")
        cur.execute("INSERT INTO a VALUES (1, 'alice')")
        cur.execute("INSERT INTO a VALUES (2, 'bob')")
        cur.execute("INSERT INTO b VALUES (1, 'x')")
        conn.commit()

        cur.execute(
            "SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.aid ORDER BY a.name"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0] == ("alice", "x")
        assert rows[1] == ("bob", None)
        conn.close()

    def test_case_expression(self, db_path):
        """Test CASE expression."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val INT64)")
        cur.execute("INSERT INTO foo VALUES (1, 10)")
        cur.execute("INSERT INTO foo VALUES (2, 20)")
        cur.execute("INSERT INTO foo VALUES (3, 30)")
        conn.commit()

        cur.execute("""
            SELECT id, 
                CASE 
                    WHEN val < 20 THEN 'small'
                    WHEN val < 30 THEN 'medium'
                    ELSE 'large'
                END as size
            FROM foo ORDER BY id
        """)
        rows = cur.fetchall()
        assert rows[0] == (1, "small")
        assert rows[1] == (2, "medium")
        assert rows[2] == (3, "large")
        conn.close()

    def test_nullif(self, db_path):
        """Test NULLIF function."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (a INT64, b INT64)")
        cur.execute("INSERT INTO foo VALUES (10, 10)")
        cur.execute("INSERT INTO foo VALUES (10, 20)")
        conn.commit()

        cur.execute("SELECT NULLIF(a, b) FROM foo ORDER BY b")
        rows = cur.fetchall()
        assert rows[0][0] is None  # 10 == 10 -> NULL
        assert rows[1][0] == 10  # 10 != 20 -> 10
        conn.close()

    def test_between(self, db_path):
        """Test BETWEEN operator."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        for i in [1, 5, 10, 15, 20]:
            cur.execute("INSERT INTO foo VALUES (?)", (i,))
        conn.commit()

        cur.execute("SELECT id FROM foo WHERE id BETWEEN 5 AND 15 ORDER BY id")
        rows = cur.fetchall()
        assert rows == [(5,), (10,), (15,)]

        # NOT BETWEEN
        cur.execute("SELECT id FROM foo WHERE id NOT BETWEEN 5 AND 15 ORDER BY id")
        rows = cur.fetchall()
        assert rows == [(1,), (20,)]
        conn.close()

    def test_having(self, db_path):
        """Test HAVING clause."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (category TEXT, value INT64)")
        cur.execute("INSERT INTO foo VALUES ('a', 10)")
        cur.execute("INSERT INTO foo VALUES ('a', 20)")
        cur.execute("INSERT INTO foo VALUES ('b', 5)")
        conn.commit()

        cur.execute(
            "SELECT category, SUM(value) as total FROM foo GROUP BY category HAVING SUM(value) > 15 ORDER BY category"
        )
        rows = cur.fetchall()
        assert rows == [("a", 30)]
        conn.close()

    def test_limit_with_expression(self, db_path):
        """Test LIMIT with expression/parameter."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        for i in range(10):
            cur.execute("INSERT INTO foo VALUES (?)", (i,))
        conn.commit()

        cur.execute("SELECT id FROM foo ORDER BY id LIMIT ?", (3,))
        rows = cur.fetchall()
        assert rows == [(0,), (1,), (2,)]
        conn.close()


class TestConstraintViolations:
    """Test constraint violation handling."""

    def test_primary_key_violation(self, db_path):
        """Test PRIMARY KEY constraint violation."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64 PRIMARY KEY)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

        with pytest.raises(decentdb.IntegrityError):
            cur.execute("INSERT INTO foo VALUES (1)")

        conn.close()

    def test_not_null_violation(self, db_path):
        """Test NOT NULL constraint violation."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64 NOT NULL)")

        with pytest.raises(decentdb.IntegrityError):
            cur.execute("INSERT INTO foo VALUES (NULL)")

        conn.close()

    def test_unique_violation(self, db_path):
        """Test UNIQUE constraint violation."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64 UNIQUE)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

        with pytest.raises(decentdb.IntegrityError):
            cur.execute("INSERT INTO foo VALUES (1)")

        conn.close()

    def test_foreign_key_restrict(self, db_path):
        """Test FOREIGN KEY RESTRICT action."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE parent (id INT64 PRIMARY KEY)")
        cur.execute(
            "CREATE TABLE child (id INT64, parent_id INT64 REFERENCES parent(id) ON DELETE RESTRICT)"
        )
        cur.execute("INSERT INTO parent VALUES (1)")
        conn.commit()

        with pytest.raises(decentdb.IntegrityError):
            cur.execute("INSERT INTO child VALUES (1, 2)")  # Parent 2 doesn't exist

        conn.close()


class TestErrorHandlingComprehensive:
    """Test error handling edge cases."""

    def test_syntax_error(self, db_path):
        """Test SQL syntax error."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()

        with pytest.raises(decentdb.ProgrammingError):
            cur.execute("SELEC * FROM foo")

        conn.close()

    def test_error_message_includes_params(self, db_path):
        """Test that error message includes parameters for debugging."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")

        try:
            cur.execute("SELECT * FROM foo WHERE id = ?", (1, "extra_param"))
            pytest.fail("Should have raised an error")
        except decentdb.ProgrammingError as e:
            msg = str(e)
            # Should include parameter info
            assert "1" in msg or "extra" in msg.lower() or "params" in msg.lower()

        conn.close()

    def test_transaction_error_rollback(self, db_path):
        """Test that error in transaction can be rolled back."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")

        # Start explicit transaction
        cur.execute("BEGIN")

        # This will fail due to PK
        cur.execute("INSERT INTO foo VALUES (1)")

        # Should be able to rollback
        cur.execute("ROLLBACK")

        # Verify only 1 row exists
        cur.execute("SELECT COUNT(*) FROM foo")
        assert cur.fetchone()[0] == 1

        conn.close()


class TestTypeCoverageComprehensive:
    """Test all supported types."""

    def test_int64_bounds(self, db_path):
        """Test INT64 min/max bounds."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (val INT64)")
        cur.execute("INSERT INTO foo VALUES (?)", (9223372036854775807,))
        cur.execute("INSERT INTO foo VALUES (?)", (-9223372036854775808,))
        conn.commit()

        cur.execute("SELECT val FROM foo ORDER BY val")
        rows = cur.fetchall()
        assert rows[0][0] == -9223372036854775808
        assert rows[1][0] == 9223372036854775807
        conn.close()

    def test_numeric_precision(self, db_path):
        """Test DECIMAL with various precisions."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()

        cur.execute("CREATE TABLE foo (val DECIMAL(10, 2))")

        # Insert various values
        values = [
            decimal.Decimal("12345.67"),
            decimal.Decimal("-12345.67"),
            decimal.Decimal("0.01"),
        ]
        for v in values:
            cur.execute("INSERT INTO foo VALUES (?)", (v,))
        conn.commit()

        cur.execute("SELECT val FROM foo ORDER BY val")
        rows = cur.fetchall()

        assert len(rows) == 3

        conn.close()

    def test_basic_text(self, db_path):
        """Test TEXT with basic content."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (val TEXT)")

        test_values = [
            "",
            "Hello World",
            "Newlines\n\r\t",
            "Quotes '\"",
        ]

        for v in test_values:
            cur.execute("INSERT INTO foo VALUES (?)", (v,))
        conn.commit()

        cur.execute("SELECT val FROM foo ORDER BY length(val)")
        rows = cur.fetchall()

        assert rows[0][0] == ""

        conn.close()

    def test_blob_types(self, db_path):
        """Test BLOB with various content."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (val BLOB)")

        test_values = [
            b"",
            b"\x00\x01\x02",
            b"Hello Binary",
        ]

        for v in test_values:
            cur.execute("INSERT INTO foo VALUES (?)", (v,))
        conn.commit()

        cur.execute("SELECT val FROM foo ORDER BY length(val)")
        rows = cur.fetchall()

        assert rows[0][0] == b""

        conn.close()


class TestMultipleCursors:
    """Test multiple cursors on same connection."""

    def test_multiple_cursors_same_connection(self, db_path):
        """Test multiple cursors on same connection."""
        conn = decentdb.connect(db_path)
        cur1 = conn.cursor()
        cur2 = conn.cursor()

        cur1.execute("CREATE TABLE foo (id INT64)")
        cur1.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

        # Both cursors should see the same data
        cur1.execute("SELECT COUNT(*) FROM foo")
        cur2.execute("SELECT COUNT(*) FROM foo")

        assert cur1.fetchone()[0] == 1
        assert cur2.fetchone()[0] == 1

        conn.close()


class TestConnectionPoolingComprehensive:
    """Test connection management."""

    def test_many_connections(self, tmp_path):
        """Test creating many connections."""
        db_path = str(tmp_path / "many_conn.ddb")

        # Create first connection with schema
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()
        conn.close()

        # Open many connections sequentially
        for i in range(20):
            conn = decentdb.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM foo")
            assert cur.fetchone()[0] == 1
            conn.close()

    def test_connection_context_manager(self, tmp_path):
        """Test connection as context manager."""
        db_path = str(tmp_path / "context.ddb")

        with decentdb.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE foo (id INT64)")
            cur.execute("INSERT INTO foo VALUES (1)")
            # No explicit commit needed - context manager handles it

        # Reopen and verify
        with decentdb.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM foo")
            assert cur.fetchone()[0] == 1


class TestAPIStabilityComprehensive:
    """Test API stability and backward compatibility."""

    def test_module_has_all_exports(self):
        """Test that expected items are exported from module."""
        expected = [
            "connect",
            "apilevel",
            "threadsafety",
            "paramstyle",
            "Error",
            "Warning",
            "DatabaseError",
            "IntegrityError",
            "ProgrammingError",
            "OperationalError",
            "DataError",
            "InternalError",
            "InterfaceError",
            "NotSupportedError",
        ]

        for name in expected:
            assert hasattr(decentdb, name), f"Missing export: {name}"

    def test_connection_has_required_methods(self, db_path):
        """Test Connection has all required DB-API methods."""
        conn = decentdb.connect(db_path)

        required = ["close", "commit", "cursor", "rollback", "execute"]
        for method in required:
            assert hasattr(conn, method), f"Connection missing method: {method}"

        conn.close()

    def test_cursor_has_required_methods(self, db_path):
        """Test Cursor has all required DB-API methods."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()

        required = [
            "close",
            "execute",
            "executemany",
            "fetchone",
            "fetchmany",
            "fetchall",
        ]
        for method in required:
            assert hasattr(cur, method), f"Cursor missing method: {method}"

        conn.close()
