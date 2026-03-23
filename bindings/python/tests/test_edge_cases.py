import pytest
import decentdb
import datetime
import decimal
import uuid


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_edge_cases.ddb")


class TestCursorBasics:
    """Basic cursor operations and DB-API compliance."""

    def test_cursor_iteration(self, db_path):
        """Test that cursor is iterable (for row in cursor)."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
        for i in range(5):
            cur.execute("INSERT INTO foo VALUES (?, ?)", (i, f"v{i}"))
        conn.commit()

        cur.execute("SELECT id, val FROM foo ORDER BY id")
        rows = []
        for row in cur:
            rows.append(row)

        assert len(rows) == 5
        assert rows[0] == (0, "v0")
        assert rows[4] == (4, "v4")
        conn.close()

    def test_rowcount_insert(self, db_path):
        """Test rowcount after INSERT."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        cur.execute("INSERT INTO foo VALUES (2)")
        cur.execute("INSERT INTO foo VALUES (3)")
        assert cur.rowcount == 1
        conn.commit()
        conn.close()

    def test_rowcount_update(self, db_path):
        """Test rowcount after UPDATE."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
        cur.execute("INSERT INTO foo VALUES (1, 'a')")
        cur.execute("INSERT INTO foo VALUES (2, 'b')")
        conn.commit()

        cur.execute("UPDATE foo SET val = 'x' WHERE id <= 1")
        assert cur.rowcount == 1
        conn.close()

    def test_rowcount_delete(self, db_path):
        """Test rowcount after DELETE."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        cur.execute("INSERT INTO foo VALUES (2)")
        cur.execute("INSERT INTO foo VALUES (3)")
        conn.commit()

        cur.execute("DELETE FROM foo WHERE id = 2")
        assert cur.rowcount == 1
        conn.close()

    def test_rowcount_select(self, db_path):
        """Test rowcount after SELECT (should be -1 per DB-API)."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

        cur.execute("SELECT * FROM foo")
        assert cur.rowcount == -1
        conn.close()

    def test_description_columns(self, db_path):
        """Test that cursor.description is populated after SELECT."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, name TEXT, amount FLOAT64)")
        cur.execute("INSERT INTO foo VALUES (1, 'test', 1.5)")
        conn.commit()

        cur.execute("SELECT id, name, amount FROM foo")
        assert cur.description is not None
        assert len(cur.description) == 3
        assert cur.description[0][0] == "id"
        assert cur.description[1][0] == "name"
        assert cur.description[2][0] == "amount"
        conn.close()

    def test_description_after_dml(self, db_path):
        """Test that cursor.description is None after INSERT/UPDATE/DELETE."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        assert cur.description is None
        conn.close()

    def test_executemany(self, db_path):
        """Test executemany for batch execution."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val TEXT)")

        params = [(i, f"v{i}") for i in range(10)]
        cur.executemany("INSERT INTO foo VALUES (?, ?)", params)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM foo")
        assert cur.fetchone()[0] == 10
        conn.close()

    def test_executemany_empty(self, db_path):
        """Test executemany with empty sequence."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.executemany("INSERT INTO foo VALUES (?)", [])
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM foo")
        assert cur.fetchone()[0] == 0
        conn.close()


class TestTransactions:
    """Transaction handling tests."""

    def test_explicit_rollback(self, db_path):
        """Test that ROLLBACK statement requires an active transaction.

        DecentDB requires explicit BEGIN for transactions.
        """
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

        cur.execute("INSERT INTO foo VALUES (2)")
        with pytest.raises(decentdb.OperationalError) as exc:
            cur.execute("ROLLBACK")
        assert "No active transaction" in str(exc.value)
        conn.close()

    def test_auto_commit_behavior(self, db_path):
        """Test that operations auto-commit by default."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.close()

        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM foo")
        count = cur.fetchone()[0]
        conn.close()
        assert count == 1

    def test_multiple_commits(self, db_path):
        """Test multiple commits don't cause issues."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        for i in range(5):
            cur.execute("INSERT INTO foo VALUES (?)", (i,))
            conn.commit()

        cur.execute("SELECT COUNT(*) FROM foo")
        assert cur.fetchone()[0] == 5
        conn.close()


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_result_set(self, db_path):
        """Test SELECT with no matching rows."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

        cur.execute("SELECT * FROM foo WHERE id = 999")
        row = cur.fetchone()
        assert row is None
        conn.close()

    def test_empty_table(self, db_path):
        """Test operations on empty table."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")

        cur.execute("SELECT * FROM foo")
        assert cur.fetchall() == []

        cur.execute("DELETE FROM foo")
        assert cur.rowcount == 0

        cur.execute("UPDATE foo SET id = 1")
        assert cur.rowcount == 0
        conn.close()

    def test_duplicate_column_names(self, db_path):
        """Test SELECT with duplicate column names."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, id2 INT64)")
        cur.execute("INSERT INTO foo VALUES (1, 2)")
        conn.commit()

        cur.execute("SELECT id, id AS id2 FROM foo")
        row = cur.fetchone()
        assert row == (1, 1)
        conn.close()

    def test_limit_offset(self, db_path):
        """Test LIMIT and OFFSET."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        for i in range(10):
            cur.execute("INSERT INTO foo VALUES (?)", (i,))
        conn.commit()

        cur.execute("SELECT id FROM foo ORDER BY id LIMIT 3")
        rows = cur.fetchall()
        assert rows == [(0,), (1,), (2,)]

        cur.execute("SELECT id FROM foo ORDER BY id LIMIT 3 OFFSET 5")
        rows = cur.fetchall()
        assert rows == [(5,), (6,), (7,)]
        conn.close()

    def test_limit_zero(self, db_path):
        """Test LIMIT 0."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

        cur.execute("SELECT id FROM foo LIMIT 0")
        rows = cur.fetchall()
        assert rows == []
        conn.close()

    def test_order_by(self, db_path):
        """Test ORDER BY."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
        cur.execute("INSERT INTO foo VALUES (3, 'c')")
        cur.execute("INSERT INTO foo VALUES (1, 'a')")
        cur.execute("INSERT INTO foo VALUES (2, 'b')")
        conn.commit()

        cur.execute("SELECT val FROM foo ORDER BY id")
        rows = cur.fetchall()
        assert rows == [("a",), ("b",), ("c",)]

        cur.execute("SELECT val FROM foo ORDER BY id DESC")
        rows = cur.fetchall()
        assert rows == [("c",), ("b",), ("a",)]
        conn.close()

    def test_null_in_where(self, db_path):
        """Test IS NULL and IS NOT NULL."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
        cur.execute("INSERT INTO foo VALUES (1, 'a')")
        cur.execute("INSERT INTO foo VALUES (2, NULL)")
        conn.commit()

        cur.execute("SELECT id FROM foo WHERE val IS NULL")
        assert cur.fetchone() == (2,)

        cur.execute("SELECT id FROM foo WHERE val IS NOT NULL")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (1,)
        conn.close()

    def test_string_edge_cases(self, db_path):
        """Test empty and long strings."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val TEXT)")

        cur.execute("INSERT INTO foo VALUES (1, '')")
        cur.execute("INSERT INTO foo VALUES (2, 'hello')")
        long_str = "x" * 10000
        cur.execute("INSERT INTO foo VALUES (3, ?)", (long_str,))
        conn.commit()

        cur.execute("SELECT val FROM foo ORDER BY id")
        rows = cur.fetchall()
        assert rows[0][0] == ""
        assert rows[1][0] == "hello"
        assert rows[2][0] == long_str
        conn.close()

    def test_special_characters(self, db_path):
        """Test special characters in strings."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (val TEXT)")
        cur.execute("INSERT INTO foo VALUES (?)", ("hello world",))
        cur.execute("INSERT INTO foo VALUES (?)", ("line1\nline2",))
        cur.execute("INSERT INTO foo VALUES (?)", ("tab\there",))
        cur.execute("INSERT INTO foo VALUES (?)", ("quote'here",))
        cur.execute("INSERT INTO foo VALUES (?)", ('double"quote',))
        conn.commit()

        cur.execute("SELECT val FROM foo ORDER BY val")
        rows = cur.fetchall()
        assert len(rows) == 5
        conn.close()

    def test_double_close_cursor(self, db_path):
        """Test calling cursor.close() twice."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.close()
        cur.close()
        conn.close()

    def test_double_close_connection(self, db_path):
        """Test calling connection.close() twice."""
        conn = decentdb.connect(db_path)
        conn.close()
        conn.close()

    def test_cursor_after_connection_close(self, db_path):
        """Test using cursor after connection is closed."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        conn.close()

        with pytest.raises(decentdb.ProgrammingError):
            cur.execute("SELECT 1")

    def test_aggregates(self, db_path):
        """Test aggregate functions."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val INT64)")
        cur.execute("INSERT INTO foo VALUES (1, 10)")
        cur.execute("INSERT INTO foo VALUES (2, 20)")
        cur.execute("INSERT INTO foo VALUES (3, 30)")
        conn.commit()

        cur.execute("SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM foo")
        row = cur.fetchone()
        assert row == (3, 60, 20.0, 10, 30)
        conn.close()

    def test_group_by(self, db_path):
        """Test GROUP BY."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (category TEXT, value INT64)")
        cur.execute("INSERT INTO foo VALUES ('a', 10)")
        cur.execute("INSERT INTO foo VALUES ('a', 20)")
        cur.execute("INSERT INTO foo VALUES ('b', 5)")
        conn.commit()

        cur.execute(
            "SELECT category, SUM(value) FROM foo GROUP BY category ORDER BY category"
        )
        rows = cur.fetchall()
        assert rows == [("a", 30), ("b", 5)]
        conn.close()

    def test_subquery(self, db_path):
        """Test subqueries."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val INT64)")
        cur.execute("INSERT INTO foo VALUES (1, 10)")
        cur.execute("INSERT INTO foo VALUES (2, 20)")
        cur.execute("INSERT INTO foo VALUES (3, 30)")
        conn.commit()

        cur.execute("SELECT * FROM foo WHERE id IN (SELECT id FROM foo WHERE val > 15)")
        rows = cur.fetchall()
        assert len(rows) == 2
        conn.close()


class TestDateTime:
    """DateTime handling tests."""

    def test_datetime_roundtrip(self, db_path):
        """Test datetime storage and retrieval."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (dt DATETIME)")

        dt = datetime.datetime(2024, 1, 15, 10, 30, 45)
        cur.execute("INSERT INTO foo VALUES (?)", (dt,))
        conn.commit()

        cur.execute("SELECT dt FROM foo")
        row = cur.fetchone()
        assert row[0].year == 2024
        assert row[0].month == 1
        assert row[0].day == 15
        assert row[0].hour == 10
        assert row[0].minute == 30
        assert row[0].second == 45
        conn.close()

    def test_date_roundtrip(self, db_path):
        """Test date storage and retrieval.

        Note: DATE columns are returned as datetime objects in this binding.
        """
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (d DATE)")

        d = datetime.date(2024, 3, 20)
        cur.execute("INSERT INTO foo VALUES (?)", (d,))
        conn.commit()

        cur.execute("SELECT d FROM foo")
        row = cur.fetchone()
        assert row[0].year == 2024
        assert row[0].month == 3
        assert row[0].day == 20
        conn.close()

    def test_datetime_with_timezone(self, db_path):
        """Test datetime with timezone."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (dt DATETIME)")

        dt_utc = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        cur.execute("INSERT INTO foo VALUES (?)", (dt_utc,))
        conn.commit()

        cur.execute("SELECT dt FROM foo")
        row = cur.fetchone()
        assert row[0].year == 2024
        conn.close()


class TestIntegerEdgeCases:
    """Integer edge case tests."""

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

    def test_float_nan(self, db_path):
        """Test FLOAT64 NaN handling."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (val FLOAT64)")
        cur.execute("INSERT INTO foo VALUES (?)", (float("nan"),))
        conn.commit()

        cur.execute("SELECT val FROM foo")
        row = cur.fetchone()
        assert row[0] != row[0]
        conn.close()

    def test_float_inf(self, db_path):
        """Test FLOAT64 infinity."""
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (val FLOAT64)")
        cur.execute("INSERT INTO foo VALUES (?)", (float("inf"),))
        cur.execute("INSERT INTO foo VALUES (?)", (float("-inf"),))
        conn.commit()

        cur.execute("SELECT val FROM foo ORDER BY val")
        rows = cur.fetchall()
        assert rows[0][0] == float("-inf")
        assert rows[1][0] == float("inf")
        conn.close()

class TestAPIEdgeCases:
    """DB-API 2.0 and assorted edge case tests."""

    def test_fetch_before_execute(self, db_path):
        import decentdb
        import pytest
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        with pytest.raises(decentdb.ProgrammingError, match="No statement"):
            cur.fetchone()
        with pytest.raises(decentdb.ProgrammingError, match="No statement"):
            cur.fetchall()
        with pytest.raises(decentdb.ProgrammingError, match="No statement"):
            cur.fetchmany()
        conn.close()

    def test_fetch_after_dml_returns_none(self, db_path):
        import decentdb
        import pytest
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()
        
        assert cur.fetchone() is None
        assert cur.fetchall() == []
        assert cur.fetchmany() == []
        conn.close()

    def test_cursor_closed_operations(self, db_path):
        import decentdb
        import pytest
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.close()
        with pytest.raises(decentdb.ProgrammingError, match="closed"):
            cur.execute("SELECT 1")
        with pytest.raises(decentdb.ProgrammingError, match="closed"):
            cur.fetchone()
        with pytest.raises(decentdb.ProgrammingError, match="closed"):
            cur.fetchmany()
        with pytest.raises(decentdb.ProgrammingError, match="closed"):
            cur.fetchall()
        
        # Double close is safe (already tested elsewhere, but just explicitly mention it here)
        cur.close()
        conn.close()

    def test_connection_closed_operations(self, db_path):
        import decentdb
        import pytest
        conn = decentdb.connect(db_path)
        conn.close()
        with pytest.raises(decentdb.ProgrammingError, match="closed"):
            conn.cursor()
        with pytest.raises(decentdb.ProgrammingError, match="closed"):
            conn.commit()
        with pytest.raises(decentdb.ProgrammingError, match="closed"):
            conn.rollback()

    def test_fetchmany_edge_cases(self, db_path):
        import decentdb
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        for i in range(3):
            cur.execute("INSERT INTO foo VALUES (?)", (i,))
        
        cur.execute("SELECT id FROM foo")
        # <= 0 should behave gracefully (return empty list)
        assert cur.fetchmany(0) == []
        assert cur.fetchmany(-1) == []
        
        # size larger than remaining returns remaining
        res = cur.fetchmany(10)
        assert len(res) == 3
        
        # Exhausted cursor
        assert cur.fetchmany() == []
        conn.close()

    def test_executemany_mixed_parameters(self, db_path):
        import decentdb
        import pytest
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (a INT64, b INT64)")
        # Passing wrong sequence types/lengths shouldn't crash ungracefully
        with pytest.raises(Exception):
            cur.executemany("INSERT INTO foo VALUES (?, ?)", [(1, 2), (1,)])
        conn.close()

