#!/usr/bin/env python3
import sys
import os
import tempfile
from pathlib import Path
import subprocess

# Add the bindings directory to Python path to import decentdb
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import decentdb
    print("Successfully imported decentdb")
except ImportError as e:
    print(f"Failed to import decentdb: {e}")
    sys.exit(1)

# Test with a simple operation
temp_dir = tempfile.mkdtemp(prefix="simple_test_")
db_path = os.path.join(temp_dir, "test.ddb")

print(f"Testing with database: {db_path}")

# Initialize database using CLI
try:
    result = subprocess.run([
        '/home/steven/source/decentdb/target/release/decentdb',
        'exec',
        '--db', db_path,
        '--sql', 'CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, value TEXT);'
    ], check=True, capture_output=True, text=True)
    print(f"CLI init stdout: {result.stdout}")
    print(f"CLI init stderr: {result.stderr}")
except subprocess.CalledProcessError as e:
    print(f"CLI init failed: {e}")
    print(f"stdout: {e.stdout}")
    print(f"stderr: {e.stderr}")

# Now try with Python bindings
try:
    db = decentdb.connect(db_path)
    print("Connected successfully")
    
    cursor = db.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
    print("Insert executed")
    
    db.commit()
    print("Committed")
    
    cursor = db.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    print(f"Selected {len(rows)} rows: {rows}")
    
    db.close()
    print("Closed successfully")
    
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# Cleanup
import shutil
shutil.rmtree(temp_dir)
print(f"Cleaned up {temp_dir}")