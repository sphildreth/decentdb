import os
import gc
import pytest
import decentdb

psutil = pytest.importorskip("psutil")

def test_engine_open_close_memory_leak(tmp_path):
    db_path = str(tmp_path / "test_py_leak.ddb")
    process = psutil.Process(os.getpid())
    
    db = decentdb.connect(db_path)
    cur = db.cursor()
    cur.execute("CREATE TABLE leak_test(id int, data text)")
    for i in range(1000):
        # 1000 rows with 1KB text each ~ 1MB data
        cur.execute("INSERT INTO leak_test VALUES (?, ?)", (i, "a" * 1000))
    db.commit()
    db.close()
        
    gc.collect()
    mem_before = process.memory_info().rss
    
    for _ in range(500):
        db = decentdb.connect(db_path)
        cur = db.cursor()
        cur.execute("SELECT * FROM leak_test")
        cur.fetchall()
        db.close()
        
    gc.collect()
    mem_after = process.memory_info().rss
    diff = mem_after - mem_before
    
    print(f"Memory before: {mem_before}, after: {mem_after}, diff: {diff}")
    assert diff < 10000000, f"Memory leak detected! Diff: {diff} bytes"
