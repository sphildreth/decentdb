"""Test cross-connection visibility: writes on one connection should be
visible from another connection to the same database WITHOUT reconnecting.

This is the root cause of the SPF5000 memory leak: because connections don't
share WAL state, the application must close and reopen connections to see
other connections' commits, causing unbounded glibc arena growth.
"""
import threading
import decentdb


class TestCrossConnectionVisibility:
    """Cross-connection visibility tests."""

    def test_conn_b_sees_conn_a_commit(self, tmp_path):
        """Connection B should see data committed by Connection A without
        reconnecting. This is the fundamental cross-connection visibility
        guarantee that databases like SQLite provide via shared WAL state."""
        db_path = str(tmp_path / "cross_vis.ddb")

        conn_a = decentdb.connect(db_path)
        cur_a = conn_a.cursor()
        cur_a.execute("CREATE TABLE t (id INT64, val TEXT)")
        cur_a.execute("INSERT INTO t VALUES (1, 'initial')")
        conn_a.commit()

        # Open connection B AFTER A's commit
        conn_b = decentdb.connect(db_path)
        cur_b = conn_b.cursor()
        cur_b.execute("SELECT val FROM t WHERE id = 1")
        row = cur_b.fetchone()
        assert row is not None and row[0] == "initial", (
            f"B should see A's initial commit, got {row}"
        )

        # Now A inserts more data and commits
        cur_a.execute("INSERT INTO t VALUES (2, 'second')")
        conn_a.commit()

        # B should see the new row WITHOUT closing/reopening
        cur_b.execute("SELECT val FROM t WHERE id = 2")
        row = cur_b.fetchone()
        assert row is not None, "B must see A's second commit without reconnecting"
        assert row[0] == "second", f"Expected 'second', got {row[0]}"

        conn_a.close()
        conn_b.close()

    def test_conn_b_sees_conn_a_update(self, tmp_path):
        """Connection B should see UPDATEs committed by Connection A."""
        db_path = str(tmp_path / "cross_vis_update.ddb")

        conn_a = decentdb.connect(db_path)
        cur_a = conn_a.cursor()
        cur_a.execute("CREATE TABLE t (id INT64, val TEXT)")
        cur_a.execute("INSERT INTO t VALUES (1, 'original')")
        conn_a.commit()

        conn_b = decentdb.connect(db_path)
        cur_b = conn_b.cursor()

        # Verify initial state
        cur_b.execute("SELECT val FROM t WHERE id = 1")
        assert cur_b.fetchone()[0] == "original"

        # A updates and commits
        cur_a.execute("UPDATE t SET val = 'modified' WHERE id = 1")
        conn_a.commit()

        # B should see the update without reconnecting
        cur_b.execute("SELECT val FROM t WHERE id = 1")
        row = cur_b.fetchone()
        assert row is not None, "B must see A's row after update"
        assert row[0] == "modified", (
            f"B should see updated value 'modified', got '{row[0]}'"
        )

        conn_a.close()
        conn_b.close()

    def test_conn_b_sees_conn_a_delete(self, tmp_path):
        """Connection B should see DELETEs committed by Connection A."""
        db_path = str(tmp_path / "cross_vis_delete.ddb")

        conn_a = decentdb.connect(db_path)
        cur_a = conn_a.cursor()
        cur_a.execute("CREATE TABLE t (id INT64, val TEXT)")
        cur_a.execute("INSERT INTO t VALUES (1, 'to_delete')")
        cur_a.execute("INSERT INTO t VALUES (2, 'keep')")
        conn_a.commit()

        conn_b = decentdb.connect(db_path)
        cur_b = conn_b.cursor()

        cur_b.execute("SELECT COUNT(*) FROM t")
        assert cur_b.fetchone()[0] == 2

        # A deletes a row and commits
        cur_a.execute("DELETE FROM t WHERE id = 1")
        conn_a.commit()

        # B should see the deletion without reconnecting
        cur_b.execute("SELECT COUNT(*) FROM t")
        count = cur_b.fetchone()[0]
        assert count == 1, f"B should see 1 row after A's delete, got {count}"

        conn_a.close()
        conn_b.close()

    def test_threaded_cross_connection_visibility(self, tmp_path):
        """Writer thread commits, reader thread on a pre-existing connection
        sees the new data without reconnecting."""
        db_path = str(tmp_path / "cross_vis_threaded.ddb")

        # Setup
        conn_setup = decentdb.connect(db_path)
        cur = conn_setup.cursor()
        cur.execute("CREATE TABLE t (id INT64, val TEXT)")
        cur.execute("INSERT INTO t VALUES (1, 'initial')")
        conn_setup.commit()
        conn_setup.close()

        write_done = threading.Event()
        reader_result = {}

        def writer():
            conn_w = decentdb.connect(db_path)
            cur_w = conn_w.cursor()
            cur_w.execute("INSERT INTO t VALUES (2, 'from_writer')")
            conn_w.commit()
            write_done.set()
            conn_w.close()

        # Open reader connection BEFORE writer starts
        conn_r = decentdb.connect(db_path)
        cur_r = conn_r.cursor()

        # Verify initial state
        cur_r.execute("SELECT COUNT(*) FROM t")
        assert cur_r.fetchone()[0] == 1

        # Writer commits on separate thread
        t = threading.Thread(target=writer)
        t.start()
        write_done.wait(timeout=10)
        t.join()

        # Reader should see writer's commit without reconnecting
        cur_r.execute("SELECT COUNT(*) FROM t")
        count = cur_r.fetchone()[0]
        reader_result["count"] = count

        assert count == 2, (
            f"Reader should see writer's commit (count=2), got {count}"
        )

        conn_r.close()

    def test_multiple_commits_visible(self, tmp_path):
        """Multiple sequential commits on A should all be visible to B."""
        db_path = str(tmp_path / "cross_vis_multi.ddb")

        conn_a = decentdb.connect(db_path)
        cur_a = conn_a.cursor()
        cur_a.execute("CREATE TABLE t (id INT64)")
        conn_a.commit()

        conn_b = decentdb.connect(db_path)
        cur_b = conn_b.cursor()

        for i in range(10):
            cur_a.execute("INSERT INTO t VALUES (?)", (i,))
            conn_a.commit()

            cur_b.execute("SELECT COUNT(*) FROM t")
            count = cur_b.fetchone()[0]
            assert count == i + 1, (
                f"After commit {i}, B should see {i+1} rows, got {count}"
            )

        conn_a.close()
        conn_b.close()

    def test_conn_b_can_free_pages_allocated_by_conn_a(self, tmp_path):
        """When Connection A allocates new pages (extending the database) and
        commits, Connection B must be able to free those pages via DELETE.

        This tests the shared maxPageCount atomic: without it, B's pager
        rejects freePage() for page IDs beyond B's local pageCount, causing
        'Cannot free page id' errors."""
        db_path = str(tmp_path / "cross_vis_pagecount.ddb")

        # Connection A: create schema and seed enough data to establish a
        # baseline file size / page count
        conn_a = decentdb.connect(db_path)
        cur_a = conn_a.cursor()
        cur_a.execute("CREATE TABLE t (id INT64, payload TEXT)")
        for i in range(50):
            cur_a.execute(
                "INSERT INTO t VALUES (?, ?)",
                (i, "x" * 200),
            )
        conn_a.commit()

        # Connection B opens — its pager.pageCount is set from the file size
        conn_b = decentdb.connect(db_path)
        cur_b = conn_b.cursor()

        # Connection A inserts MORE data, allocating NEW pages beyond B's
        # initial pageCount
        for i in range(50, 150):
            cur_a.execute(
                "INSERT INTO t VALUES (?, ?)",
                (i, "y" * 200),
            )
        conn_a.commit()

        # Connection B should be able to DELETE rows that live on pages
        # allocated by A after B opened.  This triggers freePage() on those
        # higher-numbered pages.
        cur_b.execute("DELETE FROM t WHERE id >= 50")
        conn_b.commit()

        # Verify the delete worked
        cur_b.execute("SELECT COUNT(*) FROM t")
        count = cur_b.fetchone()[0]
        assert count == 50, (
            f"Expected 50 rows after deleting id>=50, got {count}"
        )

        conn_a.close()
        conn_b.close()

    def test_threaded_page_allocation_and_free(self, tmp_path):
        """Writer thread allocates pages, reader thread deletes data from
        those pages — exercises the shared pageCount across threads."""
        db_path = str(tmp_path / "cross_vis_threaded_pagecount.ddb")

        conn_setup = decentdb.connect(db_path)
        cur = conn_setup.cursor()
        cur.execute("CREATE TABLE t (id INT64, payload TEXT)")
        for i in range(30):
            cur.execute("INSERT INTO t VALUES (?, ?)", (i, "seed" * 50))
        conn_setup.commit()
        conn_setup.close()

        write_done = threading.Event()

        def writer():
            conn_w = decentdb.connect(db_path)
            cur_w = conn_w.cursor()
            # Allocate many new pages
            for i in range(30, 130):
                cur_w.execute(
                    "INSERT INTO t VALUES (?, ?)",
                    (i, "writer" * 50),
                )
            conn_w.commit()
            write_done.set()
            conn_w.close()

        # Open reader BEFORE writer starts
        conn_r = decentdb.connect(db_path)
        cur_r = conn_r.cursor()

        # Writer allocates new pages on a separate thread
        t = threading.Thread(target=writer)
        t.start()
        write_done.wait(timeout=30)
        t.join()

        # Reader deletes rows on pages allocated by the writer
        cur_r.execute("DELETE FROM t WHERE id >= 30")
        conn_r.commit()

        cur_r.execute("SELECT COUNT(*) FROM t")
        count = cur_r.fetchone()[0]
        assert count == 30, (
            f"Expected 30 rows after deleting writer data, got {count}"
        )

        conn_r.close()
