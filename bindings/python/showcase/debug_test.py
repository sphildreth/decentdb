#!/usr/bin/env python3
import sys
import os
import tempfile
from pathlib import Path

# Add the bindings directory to Python path to import decentdb
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import decentdb
    print("Successfully imported decentdb")
except ImportError as e:
    print(f"Failed to import decentdb: {e}")
    sys.exit(1)

# Test basic functionality
with tempfile.NamedTemporaryFile(suffix='.ddb', delete=False) as tmp:
    db_path = tmp.name

print(f"Testing with database: {db_path}")

try:
    # Open database connection
    db = decentdb.connect(db_path)
    print("Successfully connected to database")
    
    # Create table
    db.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, value TEXT)")
    print("Successfully created table")
    
    # Insert a record
    db.execute("INSERT INTO test (value) VALUES (?)", ("test_value",))
    print("Successfully inserted record")
    
    # Query the record
    cursor = db.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    print(f"Query returned {len(rows)} rows: {rows}")
    
    db.close()
    print("Successfully closed database")
    
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
finally:
    # Clean up
    try:
        os.unlink(db_path)
    except:
        pass