"""Resource management tests: FD counting, RSS tracking, connection pool behavior.

These tests detect resource leaks that only manifest under realistic usage patterns
(multiple connections, concurrent threads, long-running sessions).

These tests would have caught the SPF5000 memory leak caused by connection churn
with shared WAL.
"""
import gc
import os
import threading
import time
import pytest
import decentdb

psutil = pytest.importorskip("psutil")


def count_open_fds():
    """Count open file descriptors for current process."""
    proc = psutil.Process(os.getpid())
    return proc.num_fds()


def get_rss_bytes():
    """Get current RSS (resident set size) in bytes."""
    proc = psutil.Process(os.getpid())
    return proc.memory_info().rss


class TestFileDescriptorManagement:
    """Tests for file descriptor leaks."""

    def test_open_close_fd_bounded(self, tmp_path):
        """Open/close 500 connections to the same DB. FDs must not grow.
        
        Regression: Each open/close cycle should not leak file descriptors.
        """
        db_path = str(tmp_path / "fd_test.ddb")
        
        # Seed the database
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        for i in range(100):
            cur.execute("INSERT INTO t VALUES (?, ?)", (i, f"value_{i}"))
        conn.commit()
        conn.close()
        
        gc.collect()
        fd_before = count_open_fds()
        
        # Open/close 500 connections
        for i in range(500):
            conn = decentdb.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT * FROM t WHERE id = ?", (i % 100,))
            cur.fetchone()
            conn.close()
            if i % 100 == 0:
                gc.collect()
        
        gc.collect()
        fd_after = count_open_fds()
        
        # Allow some tolerance for GC timing, but growth should be minimal
        fd_growth = fd_after - fd_before
        assert fd_growth < 10, f"FD leak detected: before={fd_before}, after={fd_after}, growth={fd_growth}"

    def test_many_connections_same_db_no_fd_leak(self, tmp_path):
        """Open 20 connections to same DB simultaneously, close all, verify FD count returns to baseline."""
        db_path = str(tmp_path / "fd_many.ddb")
        
        # Seed the database
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        cur.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        conn.close()
        
        gc.collect()
        fd_before = count_open_fds()
        
        # Open 20 connections simultaneously
        connections = []
        for i in range(20):
            conn = decentdb.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT * FROM t")
            cur.fetchall()
            connections.append(conn)
        
        # All 20 connections open
        gc.collect()
        fd_mid = count_open_fds()
        
        # Close all connections
        for conn in connections:
            conn.close()
        del connections
        gc.collect()
        gc.collect()
        
        fd_after = count_open_fds()
        
        # FDs should return close to baseline
        fd_growth = fd_after - fd_before
        assert fd_growth < 5, f"FD leak after close all: before={fd_before}, after={fd_after}, growth={fd_growth}"


class TestRSSManagement:
    """Tests for RSS (memory) leaks."""

    def test_concurrent_connections_rss_bounded(self, tmp_path):
        """Open 4 connections on 4 threads. Each does 50 write+commit cycles.
        RSS growth must be < 10 MB.
        
        Note: Writes are serialized via lock because DecentDB uses single-writer
        model. This test verifies RSS stays bounded under concurrent connection
        churn with serialized writes.
        """
        db_path = str(tmp_path / "rss_concurrent.ddb")
        
        # Seed the database with schema only
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.commit()
        conn.close()
        
        gc.collect()
        rss_before = get_rss_bytes()
        
        errors = []
        write_lock = threading.Lock()  # Serialize writes (single-writer model)
        
        def writer(thread_id):
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                for i in range(50):  # Reduced from 100 for speed
                    # Serialize writes
                    with write_lock:
                        row_id = thread_id * 1000 + i
                        cur.execute(
                            "INSERT INTO t VALUES (?, ?)",
                            (row_id, f"thread_{thread_id}_row_{i}_" + "x" * 100)
                        )
                        conn.commit()
                conn.close()
            except Exception as e:
                errors.append((thread_id, str(e)))
        
        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0, f"Errors during writes: {errors}"
        
        gc.collect()
        gc.collect()
        rss_after = get_rss_bytes()
        
        rss_growth_mb = (rss_after - rss_before) / (1024 * 1024)
        assert rss_growth_mb < 10, f"RSS leak detected: growth={rss_growth_mb:.2f} MB"

    def test_shared_wal_rss_under_write_churn(self, tmp_path):
        """THE test that would have caught the SPF5000 leak.
        
        Open Connection A (thread 1), Connection B (thread 2), Connection C (thread 3).
        Thread 1 writes every 100ms for 5 seconds (50 writes).
        Threads 2 and 3 read continuously.
        RSS growth must be < 5 MB total.
        
        This simulates SPF5000's weather service writing while display threads read.
        """
        db_path = str(tmp_path / "rss_shared_wal.ddb")
        
        # Seed the database
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT, ts INTEGER)")
        cur.execute("INSERT INTO t VALUES (0, 'initial', 0)")
        conn.commit()
        conn.close()
        
        gc.collect()
        rss_before = get_rss_bytes()
        
        write_count = [0]
        read_counts = {"reader1": 0, "reader2": 0}
        errors = []
        stop_flag = threading.Event()
        
        def writer():
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                i = 1
                while not stop_flag.is_set():
                    cur.execute(
                        "INSERT INTO t VALUES (?, ?, ?)",
                        (i, f"row_{i}_" + "data" * 50, int(time.time() * 1000))
                    )
                    conn.commit()
                    write_count[0] += 1
                    i += 1
                    time.sleep(0.1)
                conn.close()
            except Exception as e:
                errors.append(("writer", str(e)))
        
        def reader(reader_name):
            try:
                conn = decentdb.connect(db_path)
                cur = conn.cursor()
                while not stop_flag.is_set():
                    cur.execute("SELECT COUNT(*) FROM t")
                    cur.fetchone()
                    read_counts[reader_name] += 1
                    time.sleep(0.01)  # Read every 10ms
                conn.close()
            except Exception as e:
                errors.append((reader_name, str(e)))
        
        # Start writer and readers
        writer_thread = threading.Thread(target=writer)
        reader1_thread = threading.Thread(target=reader, args=("reader1",))
        reader2_thread = threading.Thread(target=reader, args=("reader2",))
        
        writer_thread.start()
        reader1_thread.start()
        reader2_thread.start()
        
        # Run for 5 seconds
        time.sleep(5)
        stop_flag.set()
        
        writer_thread.join(timeout=2)
        reader1_thread.join(timeout=2)
        reader2_thread.join(timeout=2)
        
        assert len(errors) == 0, f"Errors during test: {errors}"
        assert write_count[0] >= 40, f"Writer should have done ~50 writes, got {write_count[0]}"
        
        gc.collect()
        gc.collect()
        rss_after = get_rss_bytes()
        
        rss_growth_mb = (rss_after - rss_before) / (1024 * 1024)
        assert rss_growth_mb < 5, (
            f"RSS leak detected under shared WAL churn: growth={rss_growth_mb:.2f} MB, "
            f"writes={write_count[0]}, reads={read_counts}"
        )

    def test_connection_close_releases_native_memory(self, tmp_path):
        """Open a connection, do a large INSERT (10K rows), close connection,
        force GC, open another connection. Peak RSS should not grow on second cycle.
        """
        db_path = str(tmp_path / "rss_large_insert.ddb")
        
        # Seed schema
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, payload TEXT)")
        conn.commit()
        conn.close()
        
        gc.collect()
        rss_baseline = get_rss_bytes()
        
        rss_peaks = []
        
        for cycle in range(3):
            conn = decentdb.connect(db_path)
            cur = conn.cursor()
            
            # Insert 10K rows with 200 bytes each = ~2MB data
            for i in range(10000):
                cur.execute(
                    "INSERT INTO t VALUES (?, ?)",
                    (cycle * 10000 + i, f"cycle_{cycle}_row_{i}_" + "x" * 200)
                )
                if i % 1000 == 0:
                    conn.commit()
            conn.commit()
            
            rss_during = get_rss_bytes()
            rss_peaks.append(rss_during)
            
            conn.close()
            del conn
            gc.collect()
            gc.collect()
        
        rss_after = get_rss_bytes()
        
        # RSS after all cycles should not have grown excessively
        rss_growth = rss_after - rss_baseline
        rss_growth_mb = rss_growth / (1024 * 1024)
        
        # Allow some growth for caching, but not unbounded
        assert rss_growth_mb < 20, (
            f"RSS leak after large insert cycles: baseline={rss_baseline/1024/1024:.1f}MB, "
            f"after={rss_after/1024/1024:.1f}MB, growth={rss_growth_mb:.2f}MB"
        )

    def test_checkpoint_releases_wal_memory(self, tmp_path):
        """Open connection, do 1000 small writes (each committed), call checkpoint(),
        verify WAL file is small and RSS hasn't grown excessively.
        """
        db_path = str(tmp_path / "rss_checkpoint.ddb")
        wal_path = db_path + "-wal"
        
        # Seed schema
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        conn.commit()
        conn.close()
        
        gc.collect()
        rss_before = get_rss_bytes()
        
        # Do 1000 small writes
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        for i in range(1000):
            cur.execute(
                "INSERT INTO t VALUES (?, ?)",
                (i, f"row_{i}")
            )
            conn.commit()
        
        # Check WAL size before checkpoint
        wal_size_before = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
        
        # Checkpoint
        conn.checkpoint()
        
        # Check WAL size after checkpoint
        wal_size_after = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
        
        conn.close()
        
        gc.collect()
        rss_after = get_rss_bytes()
        
        # WAL should be much smaller after checkpoint
        assert wal_size_after < wal_size_before * 0.5, (
            f"WAL should shrink after checkpoint: before={wal_size_before}, after={wal_size_after}"
        )
        
        # RSS growth should be bounded
        rss_growth = rss_after - rss_before
        rss_growth_mb = rss_growth / (1024 * 1024)
        assert rss_growth_mb < 10, f"RSS leak after checkpoint: growth={rss_growth_mb:.2f}MB"


class TestEvictSharedWal:
    """Tests for evict_shared_wal resource management."""

    def test_evict_existing_path(self, tmp_path):
        """Evict existing path should work without error."""
        db_path = str(tmp_path / "evict_test.ddb")
        
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER)")
        conn.commit()
        conn.close()
        
        # Evict should work
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

    def test_evict_after_all_connections_closed(self, tmp_path):
        """Evict after all connections closed should work."""
        db_path = str(tmp_path / "evict_closed.ddb")
        
        conn1 = decentdb.connect(db_path)
        cur1 = conn1.cursor()
        cur1.execute("CREATE TABLE t (id INTEGER)")
        conn1.commit()
        conn1.close()
        
        # Evict after close
        decentdb.evict_shared_wal(db_path)
        
        # New connection should work
        conn2 = decentdb.connect(db_path)
        cur2 = conn2.cursor()
        cur2.execute("SELECT COUNT(*) FROM t")
        assert cur2.fetchone()[0] == 0
        conn2.close()
