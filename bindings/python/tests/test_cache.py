import pytest
import decentdb

def test_statement_cache_reuse(tmp_path):
    db_path = str(tmp_path / "cache_test.db")
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
    db_path = str(tmp_path / "eviction_test.db")
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
