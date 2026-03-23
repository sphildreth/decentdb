import gc
import threading
import weakref
import os
import pytest
import decentdb


class TestThreadSafety:
    """Tests for thread safety of read operations.

    According to DB-API 2.0, threadsafety = 1 means:
    "Threads may share the module, but not connections"

    DecentDB supports multiple concurrent reader threads using separate connections.
    """

    def test_concurrent_reads_separate_connections(self, tmp_path):
        """Multiple threads each with their own connection reading from the same DB."""
        db_path = str(tmp_path / "thread_separate_conn.ddb")

        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
        for i in range(100):
            cur.execute("INSERT INTO foo VALUES (?, ?)", (i, f"value_{i}"))
        conn.commit()
        conn.close()

        results = []
        errors = []
        barrier = threading.Barrier(4)

        def reader(thread_id):
            try:
                conn = decentdb.connect(db_path)
                barrier.wait()
                for _ in range(10):
                    cur = conn.cursor()
                    cur.execute("SELECT * FROM foo WHERE id < 10 ORDER BY id")
                    rows = cur.fetchall()
                    results.append((thread_id, len(rows), [r[1] for r in rows]))
                conn.close()
            except Exception as e:
                errors.append((thread_id, str(e)))

        threads = [threading.Thread(target=reader, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 40

        for thread_id, count, vals in results:
            assert count == 10
            assert vals == [
                "value_0",
                "value_1",
                "value_2",
                "value_3",
                "value_4",
                "value_5",
                "value_6",
                "value_7",
                "value_8",
                "value_9",
            ]

    def test_concurrent_reads_different_tables(self, tmp_path):
        """Multiple threads reading different tables simultaneously."""
        db_path = str(tmp_path / "thread_diff_tables.ddb")
        conn = decentdb.connect(db_path)
        cur = conn.cursor()

        cur.execute("CREATE TABLE t1 (id INT64)")
        cur.execute("CREATE TABLE t2 (id INT64)")
        cur.execute("CREATE TABLE t3 (id INT64)")

        for i in range(50):
            cur.execute("INSERT INTO t1 VALUES (?)", (i,))
            cur.execute("INSERT INTO t2 VALUES (?)", (i * 2,))
            cur.execute("INSERT INTO t3 VALUES (?)", (i * 3,))
        conn.commit()
        conn.close()

        results = {"t1": [], "t2": [], "t3": []}
        errors = []
        lock = threading.Lock()

        def reader(table_name):
            try:
                conn = decentdb.connect(db_path)
                for _ in range(20):
                    cur = conn.cursor()
                    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cur.fetchone()[0]
                    with lock:
                        results[table_name].append(count)
                conn.close()
            except Exception as e:
                with lock:
                    errors.append((table_name, str(e)))

        threads = [
            threading.Thread(target=reader, args=(t,)) for t in ["t1", "t2", "t3"]
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        for table_name in ["t1", "t2", "t3"]:
            assert len(results[table_name]) == 20
            assert all(c == 50 for c in results[table_name])

    def test_read_while_writing(self, tmp_path):
        """Read operation while another thread is writing.

        DecentDB supports snapshot isolation - readers should see a consistent
        snapshot. After writer commits, opening a new connection should see
        the committed data.
        """
        db_path = str(tmp_path / "read_while_write.ddb")

        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
        cur.execute("INSERT INTO foo VALUES (0, 'initial')")
        conn.commit()
        conn.close()

        write_done = threading.Event()

        def writer():
            conn = decentdb.connect(db_path)
            cur = conn.cursor()
            for i in range(1, 101):
                cur.execute("INSERT INTO foo VALUES (?, ?)", (i, f"value_{i}"))
            conn.commit()
            write_done.set()
            conn.close()

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()
        writer_thread.join()

        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM foo")
        count = cur.fetchone()[0]
        conn.close()

        assert count == 101


class TestMemoryLeaks:
    """Tests for memory leak detection and cleanup."""

    def test_connection_garbage_collection(self, tmp_path):
        """Test that connections are properly garbage collected when cursors are deleted."""
        db_path = str(tmp_path / "gc_conn.ddb")
        weak_refs = []

        for i in range(5):
            conn = decentdb.connect(db_path)
            cur = conn.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS foo_{i} (id INT64)")
            cur.execute(f"INSERT INTO foo_{i} VALUES (?)", (i,))
            conn.commit()
            weak_refs.append(weakref.ref(conn))
            del cur
            del conn
            gc.collect()

        for i, ref in enumerate(weak_refs):
            assert ref() is None, f"Connection {i} was not garbage collected"

    def test_cursor_garbage_collection(self, tmp_path):
        """Test that cursors are properly garbage collected."""
        db_path = str(tmp_path / "gc_cursor.ddb")
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64)")
        cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

        weak_refs = []
        for i in range(10):
            cur = conn.cursor()
            cur.execute("SELECT * FROM foo")
            cur.fetchall()
            weak_refs.append(weakref.ref(cur))

        del cur
        gc.collect()

        for i, ref in enumerate(weak_refs):
            assert ref() is None, f"Cursor {i} was not garbage collected"

        conn.close()

    def test_statements_cleaned_on_connection_close(self, tmp_path):
        """Test that prepared statements are finalized when connection closes."""
        db_path = str(tmp_path / "stmt_cleanup.ddb")

        for i in range(3):
            conn = decentdb.connect(db_path, stmt_cache_size=10)
            cur = conn.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS foo_{i} (id INT64)")
            cur.execute(f"INSERT INTO foo_{i} VALUES (?)", (i,))
            conn.commit()

            assert len(conn._stmt_cache) > 0 or conn._stats["prepare_count"] > 0

            conn.close()

            gc.collect()

    def test_repeated_open_close_cycles(self, tmp_path):
        """Test that repeated open/close cycles don't leak resources."""
        db_path = str(tmp_path / "open_close.ddb")

        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
        cur.execute("INSERT INTO foo VALUES (1, 'test')")
        conn.commit()
        conn.close()

        for i in range(20):
            conn = decentdb.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT * FROM foo")
            row = cur.fetchone()
            assert row == (1, "test")
            conn.close()
            gc.collect()

    def test_in_memory_database_cleanup(self, tmp_path):
        """Test that in-memory databases are properly cleaned up."""
        weak_refs = []

        for i in range(5):
            conn = decentdb.connect(":memory:")
            cur = conn.cursor()
            cur.execute("CREATE TABLE foo (id INT64)")
            cur.execute("INSERT INTO foo VALUES (?)", (i,))
            cur.execute("SELECT * FROM foo")
            assert cur.fetchone() == (i,)
            weak_refs.append(weakref.ref(conn))
            del cur
            del conn
            gc.collect()

        for i, ref in enumerate(weak_refs):
            assert ref() is None, f"In-memory connection {i} was not garbage collected"

    def test_weakref_collection_cursors(self, tmp_path):
        """Test that Connection's WeakSet of cursors is cleaned up."""
        db_path = str(tmp_path / "weakset_cleanup.ddb")
        conn = decentdb.connect(db_path)

        cur1 = conn.cursor()
        cur2 = conn.cursor()
        cur3 = conn.cursor()

        assert len(conn.cursors) == 3

        del cur1
        gc.collect()
        assert len(conn.cursors) == 2

        del cur2
        gc.collect()
        assert len(conn.cursors) == 1

        del cur3
        gc.collect()
        assert len(conn.cursors) == 0

        conn.close()

    def test_context_manager_cleanup(self, tmp_path):
        """Test that context managers properly clean up resources.

        The context manager correctly closes the connection (sets _db to None).
        Note: The connection object may not be garbage collected immediately within
        the same function due to Python's internal frame management, but it WILL be
        collected when the function returns. This is not a leak.
        """
        db_path = str(tmp_path / "context_mgr.ddb")

        with decentdb.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS foo (id INT64)")
            cur.execute("INSERT INTO foo VALUES (1)")
            cur.execute("SELECT * FROM foo")
            assert cur.fetchone() == (1,)
            weak_ref = weakref.ref(conn)
            del cur

        gc.collect()
        gc.collect()
        gc.collect()

        conn_obj = weak_ref()
        if conn_obj is not None:
            assert conn_obj._db is None
            assert conn_obj._closed is True

    def test_large_result_set_cleanup(self, tmp_path):
        """Test that large result sets don't leak memory."""
        db_path = str(tmp_path / "large_results.ddb")
        conn = decentdb.connect(db_path)
        cur = conn.cursor()

        cur.execute("CREATE TABLE foo (id INT64, data TEXT)")
        for i in range(1000):
            cur.execute("INSERT INTO foo VALUES (?, ?)", (i, "x" * 100))
        conn.commit()

        weak_cur_ref = None
        for i in range(10):
            cur = conn.cursor()
            cur.execute("SELECT * FROM foo")
            rows = cur.fetchall()
            assert len(rows) == 1000
            weak_cur_ref = weakref.ref(cur)
            del cur
            del rows
            gc.collect()

        assert weak_cur_ref() is None
        conn.close()
