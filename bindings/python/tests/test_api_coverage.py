"""Tests for Python binding API methods that currently lack coverage.

This file tests public API methods that have ZERO or minimal test coverage:
- cursor.executemany()
- cursor.fetchmany()
- cursor.__iter__() / cursor.__next__()
- connection.rollback()
- connection.checkpoint()
- connection.list_indexes()
- evict_shared_wal()
- DB-API 2.0 constructors: DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary
"""
import datetime
import gc
import pytest
import decentdb


class TestCursorExecutemany:
    """Tests for cursor.executemany()."""

    def test_executemany_bulk_insert(self, tmp_path):
        """Bulk inserts (100+ rows) via executemany."""
        db_path = str(tmp_path / "executemany_bulk.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        
        # Bulk insert 150 rows
        params = [(i, f"value_{i}") for i in range(150)]
        cur.executemany("INSERT INTO t VALUES (?, ?)", params)
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM t")
        count = cur.fetchone()[0]
        assert count == 150, f"Expected 150 rows, got {count}"
        
        conn.close()

    def test_executemany_mixed_types(self, tmp_path):
        """executemany with mixed types (int, float, text, blob)."""
        db_path = str(tmp_path / "executemany_types.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE t (id INTEGER, fval REAL, sval TEXT, bval BLOB)"
        )
        
        params = [
            (1, 1.5, "hello", b"binary"),
            (2, 2.5, "world", b"data"),
            (3, 3.5, "test", b"bytes"),
        ]
        cur.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", params)
        conn.commit()
        
        cur.execute("SELECT * FROM t ORDER BY id")
        rows = cur.fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, 1.5, "hello", b"binary")
        assert rows[1] == (2, 2.5, "world", b"data")
        assert rows[2] == (3, 3.5, "test", b"bytes")
        
        conn.close()

    def test_executemany_empty_params(self, tmp_path):
        """executemany with empty params_seq should be a no-op."""
        db_path = str(tmp_path / "executemany_empty.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        
        # Empty params should not crash
        cur.executemany("INSERT INTO t VALUES (?)", [])
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM t")
        count = cur.fetchone()[0]
        assert count == 0, f"Expected 0 rows, got {count}"
        
        conn.close()

    def test_executemany_single_row(self, tmp_path):
        """executemany with single-row params_seq."""
        db_path = str(tmp_path / "executemany_single.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        
        cur.executemany("INSERT INTO t VALUES (?)", [(42,)])
        conn.commit()
        
        cur.execute("SELECT * FROM t")
        row = cur.fetchone()
        assert row == (42,)
        
        conn.close()


class TestCursorFetchmany:
    """Tests for cursor.fetchmany()."""

    def test_fetchmany_default_arraysize(self, tmp_path):
        """fetchmany uses default arraysize (1)."""
        db_path = str(tmp_path / "fetchmany_default.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        for i in range(5):
            cur.execute("INSERT INTO t VALUES (?)", (i,))
        conn.commit()
        
        assert cur.arraysize == 1
        
        cur.execute("SELECT * FROM t ORDER BY id")
        rows = cur.fetchmany()
        assert len(rows) == 1
        assert rows[0] == (0,)
        
        conn.close()

    def test_fetchmany_custom_size(self, tmp_path):
        """fetchmany with custom size parameter."""
        db_path = str(tmp_path / "fetchmany_custom.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        for i in range(10):
            cur.execute("INSERT INTO t VALUES (?)", (i,))
        conn.commit()
        
        cur.execute("SELECT * FROM t ORDER BY id")
        
        rows = cur.fetchmany(3)
        assert len(rows) == 3
        assert rows == [(0,), (1,), (2,)]
        
        rows = cur.fetchmany(4)
        assert len(rows) == 4
        assert rows == [(3,), (4,), (5,), (6,)]
        
        rows = cur.fetchmany(10)
        assert len(rows) == 3  # Only 3 remaining
        assert rows == [(7,), (8,), (9,)]
        
        conn.close()

    def test_fetchmany_size_greater_than_remaining(self, tmp_path):
        """fetchmany with size > remaining rows returns only remaining."""
        db_path = str(tmp_path / "fetchmany_great.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        for i in range(3):
            cur.execute("INSERT INTO t VALUES (?)", (i,))
        conn.commit()
        
        cur.execute("SELECT * FROM t ORDER BY id")
        
        rows = cur.fetchmany(100)  # Request more than exists
        assert len(rows) == 3
        assert rows == [(0,), (1,), (2,)]
        
        conn.close()

    def test_fetchmany_size_zero(self, tmp_path):
        """fetchmany with size=0 returns empty list."""
        db_path = str(tmp_path / "fetchmany_zero.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        cur.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        
        cur.execute("SELECT * FROM t")
        rows = cur.fetchmany(0)
        assert rows == []
        
        conn.close()

    def test_fetchmany_after_exhausted_cursor(self, tmp_path):
        """fetchmany after cursor is exhausted returns empty list."""
        db_path = str(tmp_path / "fetchmany_exhausted.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        cur.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        
        cur.execute("SELECT * FROM t")
        rows = cur.fetchmany(10)  # Exhaust cursor
        assert len(rows) == 1
        
        # More fetchmany should return empty
        rows = cur.fetchmany(10)
        assert rows == []
        
        conn.close()


class TestCursorIteration:
    """Tests for cursor.__iter__() and cursor.__next__()."""

    def test_cursor_iteration_for_loop(self, tmp_path):
        """for row in cursor: iteration works correctly."""
        db_path = str(tmp_path / "cursor_iter.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        for i in range(5):
            cur.execute("INSERT INTO t VALUES (?, ?)", (i, f"val_{i}"))
        conn.commit()
        
        cur.execute("SELECT * FROM t ORDER BY id")
        rows = []
        for row in cur:
            rows.append(row)
        
        assert len(rows) == 5
        assert rows == [
            (0, "val_0"),
            (1, "val_1"),
            (2, "val_2"),
            (3, "val_3"),
            (4, "val_4"),
        ]
        
        conn.close()

    def test_cursor_iteration_stopiteration(self, tmp_path):
        """StopIteration raised at end of iteration."""
        db_path = str(tmp_path / "cursor_stopiter.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        cur.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        
        cur.execute("SELECT * FROM t")
        
        # First next should work
        row = next(cur)
        assert row == (1,)
        
        # Second next should raise StopIteration
        with pytest.raises(StopIteration):
            next(cur)
        
        conn.close()

    def test_cursor_iteration_after_close(self, tmp_path):
        """Iteration after cursor close raises ProgrammingError."""
        db_path = str(tmp_path / "cursor_close_iter.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        cur.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        
        cur.execute("SELECT * FROM t")
        cur.close()
        
        with pytest.raises(decentdb.ProgrammingError):
            next(cur)
        
        conn.close()


class TestConnectionRollback:
    """Tests for connection.rollback().
    
    Note: DecentDB auto-commits each statement by default. Rollback only works
    within explicit transactions (BEGIN...COMMIT/ROLLBACK blocks).
    """

    def test_rollback_after_insert_in_transaction(self, tmp_path):
        """Rollback after INSERT within transaction makes row not visible."""
        db_path = str(tmp_path / "rollback_insert.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        conn.commit()
        
        # Start explicit transaction
        cur.execute("BEGIN")
        cur.execute("INSERT INTO t VALUES (1)")
        conn.rollback()
        
        cur.execute("SELECT COUNT(*) FROM t")
        count = cur.fetchone()[0]
        assert count == 0, f"Expected 0 rows after rollback, got {count}"
        
        conn.close()

    def test_rollback_after_update_in_transaction(self, tmp_path):
        """Rollback after UPDATE within transaction preserves old value."""
        db_path = str(tmp_path / "rollback_update.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        cur.execute("INSERT INTO t VALUES (1, 'original')")
        conn.commit()
        
        # Start explicit transaction
        cur.execute("BEGIN")
        cur.execute("UPDATE t SET val = 'modified' WHERE id = 1")
        conn.rollback()
        
        cur.execute("SELECT val FROM t WHERE id = 1")
        row = cur.fetchone()
        assert row[0] == "original", f"Expected 'original', got '{row[0]}'"
        
        conn.close()

    def test_rollback_after_delete_in_transaction(self, tmp_path):
        """Rollback after DELETE within transaction leaves row still exists."""
        db_path = str(tmp_path / "rollback_delete.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        cur.execute("INSERT INTO t VALUES (1, 'keep_me')")
        conn.commit()
        
        # Start explicit transaction
        cur.execute("BEGIN")
        cur.execute("DELETE FROM t WHERE id = 1")
        conn.rollback()
        
        cur.execute("SELECT COUNT(*) FROM t")
        count = cur.fetchone()[0]
        assert count == 1, f"Expected 1 row after rollback, got {count}"
        
        cur.execute("SELECT val FROM t WHERE id = 1")
        row = cur.fetchone()
        assert row[0] == "keep_me"
        
        conn.close()

    def test_rollback_with_no_pending_changes(self, tmp_path):
        """Rollback with no pending changes is a no-op."""
        db_path = str(tmp_path / "rollback_nop.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        
        # Rollback without any changes
        conn.rollback()
        
        # Should not crash
        cur.execute("SELECT COUNT(*) FROM t")
        count = cur.fetchone()[0]
        assert count == 0
        
        conn.close()

    def test_rollback_after_commit(self, tmp_path):
        """Rollback after commit is a no-op (nothing to rollback)."""
        db_path = str(tmp_path / "rollback_after_commit.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        cur.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        
        # Rollback after commit should be no-op
        conn.rollback()
        
        # Data should still be there (from commit)
        cur.execute("SELECT COUNT(*) FROM t")
        count = cur.fetchone()[0]
        assert count == 1
        
        conn.close()


class TestConnectionCheckpoint:
    """Tests for connection.checkpoint()."""

    def test_checkpoint_reduces_wal_size(self, tmp_path):
        """Checkpoint reduces WAL file size."""
        import os
        db_path = str(tmp_path / "checkpoint_size.ddb")
        wal_path = db_path + ".wal"
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        
        # Many writes to grow WAL
        for i in range(500):
            cur.execute("INSERT INTO t VALUES (?, ?)", (i, "x" * 100))
            conn.commit()
        
        wal_size_before = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
        
        # Checkpoint
        conn.checkpoint()
        
        wal_size_after = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
        
        # WAL should be smaller (or empty) after checkpoint
        assert wal_size_after <= wal_size_before, (
            f"WAL should shrink: before={wal_size_before}, after={wal_size_after}"
        )
        
        conn.close()

    def test_checkpoint_after_many_commits(self, tmp_path):
        """Checkpoint works correctly after many commits."""
        db_path = str(tmp_path / "checkpoint_many.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        
        for i in range(1000):
            cur.execute("INSERT INTO t VALUES (?)", (i,))
            conn.commit()
        
        # Checkpoint should work without error
        conn.checkpoint()
        
        cur.execute("SELECT COUNT(*) FROM t")
        count = cur.fetchone()[0]
        assert count == 1000
        
        conn.close()

    def test_checkpoint_with_active_readers(self, tmp_path):
        """Checkpoint with active readers should not crash."""
        db_path = str(tmp_path / "checkpoint_readers.ddb")
        
        # Seed data
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        for i in range(100):
            cur.execute("INSERT INTO t VALUES (?)", (i,))
        conn.commit()
        conn.close()
        
        # Open reader
        conn_reader = decentdb.connect(db_path)
        cur_reader = conn_reader.cursor()
        cur_reader.execute("SELECT * FROM t")
        _ = cur_reader.fetchall()
        
        # Open writer and checkpoint
        conn_writer = decentdb.connect(db_path)
        cur_writer = conn_writer.cursor()
        
        for i in range(100, 200):
            cur_writer.execute("INSERT INTO t VALUES (?)", (i,))
        conn_writer.commit()
        
        # Checkpoint while reader connection is open
        conn_writer.checkpoint()
        
        # Reader should still work
        cur_reader.execute("SELECT COUNT(*) FROM t")
        count = cur_reader.fetchone()[0]
        assert count == 200
        
        conn_reader.close()
        conn_writer.close()


class TestConnectionListIndexes:
    """Tests for connection.list_indexes()."""

    def test_list_indexes_returns_created_indexes(self, tmp_path):
        """list_indexes returns created indexes."""
        db_path = str(tmp_path / "list_indexes.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        cur.execute("CREATE INDEX idx_val ON t(val)")
        conn.commit()
        
        indexes = conn.list_indexes()
        
        assert len(indexes) >= 1
        index_names = [idx.get("name", "") for idx in indexes]
        assert "idx_val" in index_names
        
        conn.close()

    def test_list_indexes_empty_when_none(self, tmp_path):
        """list_indexes returns empty list when no indexes exist."""
        db_path = str(tmp_path / "list_indexes_empty.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        conn.commit()
        
        indexes = conn.list_indexes()
        
        # May have internal indexes, but no user indexes
        user_indexes = [
            idx for idx in indexes
            if not idx.get("name", "").startswith("sqlite_")
        ]
        assert len(user_indexes) == 0
        
        conn.close()

    def test_list_indexes_multi_column(self, tmp_path):
        """list_indexes handles multi-column indexes."""
        db_path = str(tmp_path / "list_indexes_multi.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
        cur.execute("CREATE INDEX idx_multi ON t(a, b, c)")
        conn.commit()
        
        indexes = conn.list_indexes()
        
        index_names = [idx.get("name", "") for idx in indexes]
        assert "idx_multi" in index_names
        
        conn.close()


class TestEvictSharedWal:
    """Tests for evict_shared_wal()."""

    def test_evict_existing_path(self, tmp_path):
        """Evict existing path should work."""
        db_path = str(tmp_path / "evict.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        conn.commit()
        conn.close()
        
        # Should not crash
        decentdb.evict_shared_wal(db_path)
        
        # New connection should work
        conn2 = decentdb.connect(db_path)
        cur2 = conn2.cursor()
        cur2.execute("SELECT * FROM t")
        conn2.close()

    def test_evict_nonexistent_path_no_crash(self, tmp_path):
        """Evict non-existent path should not crash.
        
        Regression: expandFilename() calls C realpath() which returns NULL
        for non-existent files, causing an unhandled Nim OSError.
        """
        db_path = str(tmp_path / "nonexistent.ddb")
        
        # Ensure files don't exist
        for f in tmp_path.iterdir():
            if f.name.startswith("nonexistent"):
                f.unlink()
        
        # Evict on non-existent path — must not crash
        decentdb.evict_shared_wal(db_path)
        
        # Verify we can still open a new DB (runtime not corrupted)
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        cur.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        conn.close()


class TestDBAPI2Constructors:
    """Tests for DB-API 2.0 constructor functions."""

    def test_date_from_ticks(self):
        """DateFromTicks returns a date object."""
        import calendar
        # Use a known date and compute ticks in local time
        d = decentdb.DateFromTicks(0)  # Unix epoch
        assert isinstance(d, datetime.date)
        # Epoch is 1970-01-01 UTC, but fromtimestamp uses local time
        # Just verify it's a valid date
        assert d.year >= 1969 and d.year <= 1971  # Allow timezone offset

    def test_time_from_ticks(self):
        """TimeFromTicks returns a time object."""
        t = decentdb.TimeFromTicks(0)
        assert isinstance(t, datetime.time)
        # Simplified implementation returns midnight
        assert t.hour == 0
        assert t.minute == 0
        assert t.second == 0

    def test_timestamp_from_ticks(self):
        """TimestampFromTicks returns a datetime object."""
        ts = decentdb.TimestampFromTicks(0)  # Unix epoch
        assert isinstance(ts, datetime.datetime)
        # Epoch is 1970-01-01 UTC, but fromtimestamp uses local time
        # Just verify it's a valid datetime near epoch
        assert ts.year >= 1969 and ts.year <= 1971  # Allow timezone offset

    def test_binary(self):
        """Binary returns bytes."""
        b = decentdb.Binary(b"hello")
        assert isinstance(b, bytes)
        assert b == b"hello"
        
        # String input requires encoding
        b2 = decentdb.Binary("hello".encode("utf-8"))
        assert isinstance(b2, bytes)
        assert b2 == b"hello"
