import gc
import weakref

import pytest
import decentdb

def test_statement_cache_reuse(tmp_path):
    db_path = str(tmp_path / "cache_test.ddb")
    conn = decentdb.connect(db_path, stmt_cache_size=10)
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE foo (id INT64)")
    cur.execute("INSERT INTO foo VALUES (1)")
    conn.commit()
    
    # Initial stats
    # CREATE, INSERT, COMMIT (exec "COMMIT" also prepares?)
    # Note: COMMIT is executed via self.execute("COMMIT") in conn.commit()
    # So we expect prepare_count to be at least 3
    initial_prepares = conn._stats['prepare_count']
    assert initial_prepares > 0
    
    # Execute same statement multiple times
    sql = "SELECT * FROM foo WHERE id = ?"
    
    # 1. First execution - should prepare
    cur.execute(sql, (1,))
    cur.fetchone()
    
    prepares_after_1 = conn._stats['prepare_count']
    assert prepares_after_1 == initial_prepares + 1
    
    # 2. Second execution - should reuse same stmt object (held by cursor optimization)
    # The cursor logic I wrote checks `if self._last_sql == sql: reset`
    cur.execute(sql, (1,))
    cur.fetchone()
    
    prepares_after_2 = conn._stats['prepare_count']
    assert prepares_after_2 == prepares_after_1, "Should reuse existing statement on cursor"
    
    # 3. Use a DIFFERENT statement, then go back to the first one
    cur.execute("SELECT * FROM foo") # This recycles the previous one into cache
    cur.fetchone()
    
    prepares_after_interim = conn._stats['prepare_count']
    assert prepares_after_interim == prepares_after_2 + 1
    
    # 4. Go back to first SQL. Should hit cache.
    cur.execute(sql, (1,))
    cur.fetchone()
    
    prepares_after_return = conn._stats['prepare_count']
    assert prepares_after_return == prepares_after_interim, "Should hit cache"
    assert conn._stats['cache_hit'] > 0
    
    conn.close()

def test_cache_eviction(tmp_path):
    db_path = str(tmp_path / "eviction_test.ddb")
    conn = decentdb.connect(db_path, stmt_cache_size=2) # Small cache
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE foo (id INT64)")
    
    # Fill cache with 2 statements
    cur.execute("SELECT 1")
    cur.execute("SELECT 2") 
    # "SELECT 1" is recycled to cache when "SELECT 2" runs
    # Cache: ["SELECT 1"]
    
    cur.execute("SELECT 3")
    # "SELECT 2" recycled. Cache: ["SELECT 1", "SELECT 2"]
    
    cur.execute("SELECT 4")
    # "SELECT 3" recycled. Cache: ["SELECT 2", "SELECT 3"] (SELECT 1 evicted)
    
    # Now execute SELECT 1 again. Should be a miss.
    before = conn._stats['prepare_count']
    cur.execute("SELECT 1")
    after = conn._stats['prepare_count']
    
    assert after == before + 1, "Should be a cache miss (evicted)"
    
    conn.close()


def test_connection_execute_does_not_retain_discarded_cursors(tmp_path):
    db_path = str(tmp_path / "execute_cursor_cleanup.ddb")
    conn = decentdb.connect(db_path, stmt_cache_size=10)

    weak_cursor = None
    for i in range(5):
        cur = conn.execute("SELECT 1")
        assert cur.fetchone() == (1,)
        if i == 0:
            weak_cursor = weakref.ref(cur)
        del cur
        gc.collect()

    assert weak_cursor is not None
    assert weak_cursor() is None
    assert len(conn.cursors) == 0
    assert conn._stats["prepare_count"] == 1
    assert conn._stats["cache_hit"] == 4
    assert len(conn._stmt_cache) == 1

    conn.close()


def test_repeated_execute_fetchall_does_not_duplicate_buffered_first_row(tmp_path):
    db_path = str(tmp_path / "buffered_first_row.ddb")
    conn = decentdb.connect(db_path, stmt_cache_size=10)
    cur = conn.cursor()

    cur.execute("CREATE TABLE foo (id INT64 PRIMARY KEY, name TEXT, email TEXT)")
    cur.execute(
        "INSERT INTO foo VALUES (?, ?, ?)",
        (1, "alice", "alice@example.com"),
    )
    conn.commit()

    sql = "SELECT id, name, email FROM foo WHERE id = ?"
    for _ in range(3):
        cur.execute(sql, (1,))
        assert cur.fetchall() == [(1, "alice", "alice@example.com")]

    conn.close()


def test_prefetched_rows_are_consumed_without_duplication(tmp_path):
    db_path = str(tmp_path / "prefetched_rows.ddb")
    conn = decentdb.connect(db_path, stmt_cache_size=10)
    cur = conn.cursor()

    cur.execute("CREATE TABLE foo (id INT64 PRIMARY KEY, bucket INT64)")
    cur.executemany(
        "INSERT INTO foo VALUES (?, ?)",
        [
            (1, 10),
            (2, 10),
            (3, 10),
        ],
    )
    conn.commit()

    cur.execute(
        "SELECT id FROM foo WHERE bucket = ? ORDER BY id LIMIT 10",
        (10,),
    )
    assert cur.fetchone() == (1,)
    assert cur.fetchmany(2) == [(2,), (3,)]
    assert cur.fetchone() is None
    assert cur.fetchall() == []

    conn.close()


def test_prefetched_rows_support_zero_param_and_two_float_queries(tmp_path):
    db_path = str(tmp_path / "prefetched_shapes.ddb")
    conn = decentdb.connect(db_path, stmt_cache_size=10)
    cur = conn.cursor()

    cur.execute(
        "CREATE TABLE items (id INT64 PRIMARY KEY, price FLOAT64)"
    )
    cur.executemany(
        "INSERT INTO items VALUES (?, ?)",
        [
            (1, 5.0),
            (2, 7.5),
            (3, 11.0),
        ],
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM items")
    assert cur.fetchall() == [(3,)]

    cur.execute(
        "SELECT id FROM items WHERE price >= ? AND price < ? ORDER BY price LIMIT 10",
        (5.0, 10.0),
    )
    assert cur.fetchall() == [(1,), (2,)]

    conn.close()
