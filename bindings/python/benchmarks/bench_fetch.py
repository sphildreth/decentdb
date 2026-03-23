import decentdb
import time
import os
import sys

def run_benchmark():
    db_path = "bench_fetch.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    
    print("Setting up data...")
    cur.execute("CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)")
    
    # Insert 100k rows
    # Batch insert? executemany
    count = 100000
    data = [(i, f"value_{i}", float(i)) for i in range(count)]
    
    start_time = time.perf_counter()
    cur.execute("BEGIN")
    for row in data:
        cur.execute("INSERT INTO bench VALUES (?, ?, ?)", row)
    cur.execute("COMMIT")
    end_time = time.perf_counter()
    print(f"Insert {count} rows: {end_time - start_time:.4f}s")
    
    # Benchmark fetchall
    conn.close()
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    
    print("Benchmarking fetchall...")
    start_time = time.perf_counter()
    cur.execute("SELECT * FROM bench")
    rows = cur.fetchall()
    end_time = time.perf_counter()
    
    print(f"Fetchall {count} rows: {end_time - start_time:.4f}s")
    assert len(rows) == count
    
    # Benchmark fetchmany(1000)
    conn.close()
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    
    print("Benchmarking fetchmany(1000)...")
    start_time = time.perf_counter()
    cur.execute("SELECT * FROM bench")
    total = 0
    while True:
        batch = cur.fetchmany(1000)
        if not batch:
            break
        total += len(batch)
    end_time = time.perf_counter()
    
    print(f"Fetchmany(1000) {count} rows: {end_time - start_time:.4f}s")
    assert total == count
    
    conn.close()
    if os.path.exists(db_path):
        os.remove(db_path)

if __name__ == "__main__":
    run_benchmark()