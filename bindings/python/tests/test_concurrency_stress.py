"""Stress tests for concurrent database access patterns.

These reproduce real-world usage: multiple threads reading while one writes,
concurrent schema operations, long-running readers during checkpoints.

These tests would have caught the cross-pager page allocation bug where
Connection B couldn't free pages allocated by Connection A.
"""
import gc
import os
import threading
import time
import pytest
import decentdb


class TestWriterReaderInterleave:
    """Tests for concurrent writer/reader patterns."""

    def test_writer_reader_interleave(self, tmp_path):
        """1 writer thread doing continuous INSERT+COMMIT, 3 reader threads
        doing continuous SELECT. Run for 2 seconds. No crashes, no stale reads.
        """
        db_path = str(tmp_path / "writer_reader.ddb")
        
        # Seed schema
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        conn.commit()
        conn.close()
        
        stop_flag = threading.Event()
        write_count = [0]
        read_counts = [0, 0, 0]
        errors = []
        max_seen_ids = [0, 0, 0]
        
        def writer():
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                i = 1
                while not stop_flag.is_set():
                    cur.execute(
                        "INSERT INTO t VALUES (?, ?)",
                        (i, f"row_{i}")
                    )
                    conn.commit()
                    write_count[0] = i
                    i += 1
                conn.close()
            except Exception as e:
                errors.append(("writer", str(e)))
        
        def reader(reader_id):
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                while not stop_flag.is_set():
                    cur.execute("SELECT MAX(id) FROM t")
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        max_id = row[0]
                        if max_id > max_seen_ids[reader_id]:
                            max_seen_ids[reader_id] = max_id
                    read_counts[reader_id] += 1
                conn.close()
            except Exception as e:
                errors.append((f"reader{reader_id}", str(e)))
        
        # Start writer and readers
        writer_thread = threading.Thread(target=writer)
        reader_threads = [
            threading.Thread(target=reader, args=(i,))
            for i in range(3)
        ]
        
        writer_thread.start()
        for t in reader_threads:
            t.start()
        
        # Run for 2 seconds
        time.sleep(2)
        stop_flag.set()
        
        writer_thread.join(timeout=2)
        for t in reader_threads:
            t.join(timeout=2)
        
        assert len(errors) == 0, f"Errors during test: {errors}"
        assert write_count[0] > 0, "Writer should have written some rows"
        assert all(c > 0 for c in read_counts), "All readers should have read"
        
        # Verify final count
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM t")
        final_count = cur.fetchone()[0]
        conn.close()
        
        assert final_count == write_count[0], (
            f"Final count {final_count} should match writes {write_count[0]}"
        )

    def test_concurrent_table_operations(self, tmp_path):
        """Thread A creates table, inserts data. Thread B opens AFTER A commits
        and can query the table. Exercises shared WAL visibility for DDL.
        
        Note: A reader opened BEFORE a CREATE TABLE won't see the new table
        because schema is cached at open time. This test verifies that a reader
        opening AFTER the DDL commit can see the new table.
        """
        db_path = str(tmp_path / "concurrent_ddl.ddb")
        
        # Setup: create initial schema
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE initial (id INTEGER)")
        cur.execute("INSERT INTO initial VALUES (1)")
        conn.commit()
        conn.close()
        
        errors = []
        table_created = threading.Event()
        
        def writer():
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                cur.execute("CREATE TABLE new_table (id INTEGER, val TEXT)")
                for i in range(100):
                    cur.execute(
                        "INSERT INTO new_table VALUES (?, ?)",
                        (i, f"val_{i}")
                    )
                conn.commit()
                table_created.set()
                conn.close()
            except Exception as e:
                errors.append(("writer", str(e)))
        
        writer_thread = threading.Thread(target=writer)
        writer_thread.start()
        table_created.wait(timeout=10)
        writer_thread.join(timeout=10)
        
        assert len(errors) == 0, f"Writer errors: {errors}"
        
        # Open reader AFTER writer commits - should see the new table
        conn_reader = decentdb.connect(db_path)
        cur_reader = conn_reader.cursor()
        cur_reader.execute("SELECT COUNT(*) FROM new_table")
        count = cur_reader.fetchone()[0]
        assert count == 100, f"Reader should see 100 rows in new_table, got {count}"
        
        conn_reader.close()

    def test_reader_during_checkpoint(self, tmp_path):
        """Reader holds long SELECT open. Writer commits many rows then calls
        checkpoint(). Reader should still get consistent results.
        """
        db_path = str(tmp_path / "checkpoint_reader.ddb")
        
        # Seed schema and initial data
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        for i in range(100):
            cur.execute("INSERT INTO t VALUES (?, ?)", (i, f"initial_{i}"))
        conn.commit()
        conn.close()
        
        errors = []
        reader_result = {}
        reader_ready = threading.Event()
        checkpoint_done = threading.Event()
        
        def reader():
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                cur.execute("SELECT * FROM t ORDER BY id")
                rows_before = cur.fetchall()
                reader_result["rows_before"] = len(rows_before)
                
                # Signal writer that reader has captured initial snapshot
                reader_ready.set()
                
                # Wait for checkpoint to complete
                checkpoint_done.wait(timeout=10)
                
                # Reader should still be able to query
                cur.execute("SELECT COUNT(*) FROM t")
                count_after = cur.fetchone()[0]
                reader_result["count_after"] = count_after
                
                conn.close()
            except Exception as e:
                errors.append(("reader", str(e)))
                reader_ready.set()  # Unblock writer on error
        
        def writer():
            try:
                # Wait for reader to capture initial snapshot
                reader_ready.wait(timeout=10)
                
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                for i in range(100, 500):
                    cur.execute(
                        "INSERT INTO t VALUES (?, ?)",
                        (i, f"added_{i}")
                    )
                    if i % 100 == 0:
                        conn.commit()
                conn.commit()
                
                conn.checkpoint()
                checkpoint_done.set()
                conn.close()
            except Exception as e:
                errors.append(("writer", str(e)))
                checkpoint_done.set()  # Unblock reader on error
        
        reader_thread = threading.Thread(target=reader)
        writer_thread = threading.Thread(target=writer)
        
        reader_thread.start()
        writer_thread.start()
        
        reader_thread.join(timeout=15)
        writer_thread.join(timeout=15)
        
        assert len(errors) == 0, f"Errors: {errors}"
        assert reader_result.get("rows_before") == 100
        assert reader_result.get("count_after") == 500

    def test_page_allocation_cross_connection(self, tmp_path):
        """REGRESSION: Connection A allocates many pages (large INSERT).
        Connection B (opened after A's commit) can DELETE rows from those pages.
        
        The pager's effectivePageCount must account for pages allocated by other
        connections via the shared WAL maxPageCount atomic.
        
        Note: This test verifies that Connection B can operate on pages that were
        allocated by Connection A. The original bug was that B's pager rejected
        freePage() for page IDs beyond B's local pageCount.
        """
        db_path = str(tmp_path / "cross_pager_pages.ddb")
        
        # Connection A: create schema and seed data
        conn_a = decentdb.connect(db_path)
        cur_a = conn_a.cursor()
        cur_a.execute("CREATE TABLE t (id INTEGER, payload TEXT)")
        for i in range(50):
            cur_a.execute(
                "INSERT INTO t VALUES (?, ?)",
                (i, "seed_" + "x" * 200)
            )
        conn_a.commit()
        
        # Connection A inserts MORE data, allocating NEW pages
        for i in range(50, 200):
            cur_a.execute(
                "INSERT INTO t VALUES (?, ?)",
                (i, "new_" + "y" * 200)
            )
        conn_a.commit()
        
        # Connection B opens AFTER A's commits
        conn_b = decentdb.connect(db_path)
        cur_b = conn_b.cursor()
        
        # Connection B should be able to DELETE rows that live on pages
        # allocated by A
        cur_b.execute("DELETE FROM t WHERE id >= 50")
        conn_b.commit()
        
        # Verify the delete worked
        cur_b.execute("SELECT COUNT(*) FROM t")
        count = cur_b.fetchone()[0]
        assert count == 50, f"Expected 50 rows after delete, got {count}"
        
        conn_a.close()
        conn_b.close()

    def test_rapid_open_close_under_concurrent_writes(self, tmp_path):
        """While one thread writes continuously, another thread rapidly opens
        and closes connections (100 cycles). No crashes, no corruption.
        
        Note: Due to snapshot isolation, readers may not see all writes.
        This test verifies no crashes or corruption under connection churn.
        """
        db_path = str(tmp_path / "rapid_open_close.ddb")
        
        # Seed schema
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.commit()
        conn.close()
        
        errors = []
        open_close_count = [0]
        write_count = [0]
        stop_flag = threading.Event()
        write_lock = threading.Lock()
        
        def writer():
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                i = 1
                while not stop_flag.is_set():
                    with write_lock:
                        cur.execute(
                            "INSERT INTO t VALUES (?, ?)",
                            (i, f"row_{i}_" + "data" * 50)
                        )
                        conn.commit()
                        write_count[0] = i
                        i += 1
                conn.close()
            except Exception as e:
                errors.append(("writer", str(e)))
        
        def connection_churn():
            try:
                while not stop_flag.is_set():
                    conn = decentdb.connect(db_path)
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM t")
                    cur.fetchone()
                    conn.close()
                    open_close_count[0] += 1
            except Exception as e:
                errors.append(("churn", str(e)))
        
        writer_thread = threading.Thread(target=writer)
        churn_thread = threading.Thread(target=connection_churn)
        
        writer_thread.start()
        churn_thread.start()
        
        # Run for 2 seconds (reduced from 3 for speed)
        time.sleep(2)
        stop_flag.set()
        
        writer_thread.join(timeout=2)
        churn_thread.join(timeout=2)
        
        assert len(errors) == 0, f"Errors: {errors}"
        assert open_close_count[0] >= 5, (
            f"Should have completed several open/close cycles, got {open_close_count[0]}"
        )
        assert write_count[0] > 0, "Writer should have written"
        
        # Verify database is still accessible (count may vary due to timing)
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM t")
        count = cur.fetchone()[0]
        conn.close()
        
        # Count should be positive and reasonable
        assert count > 0, "Database should have data"
        assert count <= write_count[0], f"Count {count} should not exceed writes {write_count[0]}"


class TestConcurrentSchemaOperations:
    """Tests for concurrent DDL operations."""

    def test_concurrent_creates_different_tables(self, tmp_path):
        """Multiple threads each create their own table, serialized via lock.

        Each thread opens its own connection, creates a table with rows,
        commits, and closes within the serialization lock. This verifies
        that tables created by separate connections are all visible.
        """
        db_path = str(tmp_path / "concurrent_create.ddb")

        errors = []
        tables_created = []
        results_lock = threading.Lock()
        create_lock = threading.Lock()

        def create_table(thread_id):
            try:
                table_name = f"table_{thread_id}"
                with create_lock:
                    conn = decentdb.connect(db_path)
                    cur = conn.cursor()
                    cur.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, val TEXT)")
                    for i in range(50):
                        cur.execute(
                            f"INSERT INTO {table_name} VALUES (?, ?)",
                            (i, f"val_{i}")
                        )
                    conn.commit()
                    conn.close()

                with results_lock:
                    tables_created.append(table_name)
            except Exception as e:
                with results_lock:
                    errors.append((thread_id, str(e)))

        threads = [
            threading.Thread(target=create_table, args=(i,))
            for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors: {errors}"
        assert len(tables_created) == 5

        conn = decentdb.connect(db_path)
        tables = conn.list_tables()
        conn.close()

        if tables and isinstance(tables[0], dict):
            table_names = [t["name"] for t in tables]
        else:
            table_names = tables

        for i in range(5):
            assert f"table_{i}" in table_names, f"table_{i} should exist"

    def test_create_index_while_reading(self, tmp_path):
        """Thread A creates index while Thread B reads. No crashes."""
        db_path = str(tmp_path / "index_while_read.ddb")
        
        # Seed data
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        for i in range(500):
            cur.execute("INSERT INTO t VALUES (?, ?)", (i, f"val_{i}"))
        conn.commit()
        conn.close()
        
        errors = []
        index_created = threading.Event()
        reader_ready = threading.Event()
        read_count = [0]
        
        def reader():
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                while not index_created.is_set():
                    cur.execute("SELECT * FROM t WHERE id > ?", (250,))
                    cur.fetchall()
                    read_count[0] += 1
                    reader_ready.set()
                conn.close()
            except Exception as e:
                errors.append(("reader", str(e)))
        
        def index_creator():
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                cur.execute("CREATE INDEX idx_val ON t(val)")
                conn.commit()
                index_created.set()
                conn.close()
            except Exception as e:
                errors.append(("indexer", str(e)))
        
        reader_thread = threading.Thread(target=reader)
        indexer_thread = threading.Thread(target=index_creator)
        
        reader_thread.start()
        if not reader_ready.wait(timeout=5):
            index_created.set()
            reader_thread.join(timeout=10)
            assert False, f"Reader did not complete an initial read; errors: {errors}"
        indexer_thread.start()
        
        reader_thread.join(timeout=10)
        indexer_thread.join(timeout=10)
        
        assert len(errors) == 0, f"Errors: {errors}"
        assert read_count[0] > 0, "Reader should have read"
        
        # Verify index exists
        conn = decentdb.connect(db_path)
        indexes = conn.list_indexes()
        conn.close()
        
        index_names = [idx.get("name", "") for idx in indexes]
        assert "idx_val" in index_names, "Index should exist"
