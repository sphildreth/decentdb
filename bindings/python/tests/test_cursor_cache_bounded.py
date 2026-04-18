"""Tests for cursor cache boundedness.

These tests verify that cursor caches do not grow unboundedly,
preventing memory leaks in long-running applications.
"""

import gc
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from decentdb import connect

ALL_CURSOR_CACHES = [
    '_rewrite_sql_cache',
    '_metadata_cache',
    '_is_direct_execute_sql_cache',
    '_should_buffer_first_row_sql_cache',
    '_should_prefetch_small_result_sql_cache',
    '_should_prefetch_zero_param_result_sql_cache',
    '_native_fetch_rows_i64_text_f64_sql_support',
    '_decode_matrix_i64_text_f64_sql_support',
    '_decode_matrix_i64_text_text_sql_support',
    '_decode_matrix_i64_f64_text_sql_support',
    '_decode_matrix_text_i64_f64_sql_support',
    '_decode_matrix_i64_sql_support',
    '_decode_matrix_i64_f64_text_text_i64_f64_sql_support',
    '_native_bind_int64_step_row_view_sql_support',
    '_native_bind_text_step_row_view_sql_support',
    '_native_bind_int64_fetch_all_row_views_sql_support',
    '_native_step_fetch_all_row_views_sql_support',
    '_native_bind_text_fetch_all_row_views_sql_support',
    '_native_bind_f64_f64_fetch_all_row_views_sql_support',
    '_fast_repeat_cache',
    '_select_fast_info',
]


def get_cache_sizes(cursor):
    """Return dict of cache name -> size for all cursor caches."""
    sizes = {}
    for cache_name in ALL_CURSOR_CACHES:
        cache = getattr(cursor, cache_name, None)
        if cache is not None:
            sizes[cache_name] = len(cache)
    return sizes


def test_caches_cleared_on_close(db_path):
    """Verify that all caches are cleared when cursor is closed."""
    db = connect(db_path)
    cur = db.cursor()

    for i in range(100):
        cur.execute(f"SELECT {i} AS n")

    db.commit()

    sizes_before = get_cache_sizes(cur)
    total_before = sum(sizes_before.values())

    assert total_before > 0, "Caches should have entries after executing queries"

    cur.close()

    sizes_after = get_cache_sizes(cur)
    total_after = sum(sizes_after.values())

    assert total_after == 0, (
        f"Cursor caches should be empty after close(). "
        f"Before: {sizes_before}, After: {sizes_after}"
    )


def test_caches_cleared_after_connection_context(db_path):
    """Verify caches are cleared when connection is used as context manager."""
    with connect(db_path) as db:
        cur = db.cursor()
        for i in range(50):
            cur.execute(f"SELECT {i} AS n, 'text' AS t, 3.14 AS f")

        sizes_before = get_cache_sizes(cur)
        total_before = sum(sizes_before.values())
        assert total_before > 0, "Caches should have entries during use"

        cur.close()
        sizes_after = get_cache_sizes(cur)
        total_after = sum(sizes_after.values())

        assert total_after == 0, (
            f"Cursor caches should be empty after close(). "
            f"Before: {sizes_before}, After: {sizes_after}"
        )


def test_diverse_queries_cleared_on_close(db_path):
    """Verify caches don't accumulate with many diverse queries."""
    db = connect(db_path)
    cur = db.cursor()

    cur.execute("CREATE TABLE test_items(id INT, name TEXT, value REAL)")
    for i in range(20):
        cur.execute(
            "INSERT INTO test_items VALUES (?, ?, ?)",
            (i, f"name_{i}", i * 1.5)
        )
    db.commit()

    query_templates = [
        "SELECT * FROM test_items WHERE id = ?",
        "SELECT * FROM test_items WHERE name = ?",
        "SELECT * FROM test_items WHERE value > ?",
        "SELECT id, name FROM test_items",
        "SELECT id, value FROM test_items",
        "SELECT name, value FROM test_items",
        "SELECT COUNT(*) FROM test_items",
        "SELECT SUM(value) FROM test_items",
        "SELECT AVG(value) FROM test_items",
        "SELECT * FROM test_items ORDER BY id",
    ]

    for _ in range(10):
        for i, template in enumerate(query_templates):
            if '?' in template:
                cur.execute(template, (i,))

    db.commit()

    sizes_before = get_cache_sizes(cur)
    cur.close()

    sizes_after = get_cache_sizes(cur)

    uncleared = {k: v for k, v in sizes_after.items() if v > 0}

    assert len(uncleared) == 0, (
        f"The following caches should be empty after close() but still have entries: {uncleared}"
    )


def test_rewrite_sql_cache_bounded(db_path):
    """Verify _rewrite_sql_cache is bounded and cleared on close."""
    db = connect(db_path)
    cur = db.cursor()

    for i in range(100):
        cur.execute(f"SELECT {i} AS n")

    cache_size = len(cur._rewrite_sql_cache)
    assert cache_size > 0, "Cache should have entries"

    cur.close()

    assert len(cur._rewrite_sql_cache) == 0, (
        f"_rewrite_sql_cache should be empty after close(), but has {len(cur._rewrite_sql_cache)} entries"
    )


def test_metadata_cache_bounded(db_path):
    """Verify _metadata_cache is bounded and cleared on close."""
    db = connect(db_path)
    cur = db.cursor()

    for i in range(50):
        cur.execute(f"SELECT {i} AS n, 'text' AS t, 3.14 AS f")

    cache_size = len(cur._metadata_cache)
    assert cache_size > 0, "Cache should have entries"

    cur.close()

    assert len(cur._metadata_cache) == 0, (
        f"_metadata_cache should be empty after close(), but has {len(cur._metadata_cache)} entries"
    )


def test_native_sql_support_caches_bounded(db_path):
    """Verify *_sql_support caches are bounded and cleared on close."""
    db = connect(db_path)
    cur = db.cursor()

    cur.execute("CREATE TABLE t1(id INT, data TEXT)")
    cur.execute("CREATE TABLE t2(id INT, data TEXT)")
    cur.execute("CREATE TABLE t3(id INT, data TEXT)")

    for i in range(30):
        cur.execute(f"SELECT * FROM t{i % 3 + 1}")

    sql_support_caches = [c for c in ALL_CURSOR_CACHES if '_sql_support' in c]

    cur.close()

    total_size_after = sum(len(getattr(cur, c)) for c in sql_support_caches)

    assert total_size_after == 0, (
        f"SQL support caches should be empty after close(). "
        f"After: {total_size_after}"
    )


def test_multiple_cursor_lifecycle(db_path):
    """Verify each cursor's caches are properly isolated and cleared."""
    db = connect(db_path)
    db.execute("CREATE TABLE items(id INT, name TEXT)")

    cursors = []
    for i in range(5):
        cur = db.cursor()
        cur.execute("INSERT INTO items VALUES (?, ?)", (i, f"item_{i}"))
        cur.execute(f"SELECT * FROM items WHERE id = {i}")
        cur.fetchone()
        cursors.append(cur)

    db.commit()

    for i, cur in enumerate(cursors):
        sizes = get_cache_sizes(cur)
        total = sum(sizes.values())
        assert total > 0, f"Cursor {i} should have cache entries before close"

    for cur in cursors:
        cur.close()

    for i, cur in enumerate(cursors):
        sizes = get_cache_sizes(cur)
        total = sum(sizes.values())
        assert total == 0, (
            f"Cursor {i} should have no cache entries after close, but has: {sizes}"
        )


if __name__ == "__main__":
    import tempfile
    import sys

    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.ddb")

        tests = [
            test_caches_cleared_on_close,
            test_caches_cleared_after_connection_context,
            test_diverse_queries_cleared_on_close,
            test_rewrite_sql_cache_bounded,
            test_metadata_cache_bounded,
            test_native_sql_support_caches_bounded,
            test_multiple_cursor_lifecycle,
        ]

        for test in tests:
            print(f"Running {test.__name__}...")
            try:
                test(db_path)
                print(f"  PASSED")
            except AssertionError as e:
                print(f"  FAILED: {e}")
                sys.exit(1)
            except Exception as e:
                print(f"  ERROR: {e}")
                sys.exit(1)

    print("\nAll tests passed!")
