"""Regression test for repeated open/close memory leak.

The WAL overlay closure in engine.nim captured the Db ref, creating a
reference cycle (Db -> Pager -> closure -> Db) that ARC could not collect.
closeDb now breaks this cycle by clearing the overlay before closing.

This test verifies that repeated open/close cycles release native resources
by checking RSS stays bounded.
"""
import os
import resource
import pytest
import decentdb


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "leak_test.ddb")
    conn = decentdb.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'hello')")
    conn.commit()
    conn.close()
    return path


def test_repeated_open_close_bounded_rss(db_path):
    """RSS must not grow unboundedly across repeated open/close cycles."""
    iterations = 500
    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # KB on Linux

    for _ in range(iterations):
        conn = decentdb.connect(db_path)
        cur = conn.execute("SELECT * FROM t WHERE id = 1")
        cur.fetchall()
        conn.commit()
        conn.close()

    rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    growth_mb = (rss_after - rss_before) / 1024.0

    # Before fix: ~55 MB per 500 iterations.  After fix: < 1 MB.
    # Use a generous 10 MB threshold to avoid CI flakiness.
    assert growth_mb < 10.0, (
        f"RSS grew by {growth_mb:.1f} MB over {iterations} open/close cycles "
        f"(before={rss_before/1024:.1f} MB, after={rss_after/1024:.1f} MB)"
    )
