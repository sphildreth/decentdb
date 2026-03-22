# Python Bindings

The Python bindings live under `bindings/python/` and provide:

1. `decentdb`: a DB-API 2.0 driver
2. `decentdb_sqlalchemy`: a SQLAlchemy 2.x dialect

## Install (editable, from this repo)

```bash
python -m pip install -e bindings/python
```

## Build / locate the native library

The Python bindings load the DecentDB C API via `ctypes`.

```bash
nimble build_lib
```

The loader finds `build/libc_api.so` automatically when running from the repo. To force an explicit path:

```bash
export DECENTDB_NATIVE_LIB=$PWD/build/libc_api.so
```

## DB-API 2.0 Usage

```python
import decentdb

conn = decentdb.connect("/path/to/database.ddb")

# DDL
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)")
conn.commit()

# INSERT with auto-increment (omit id column)
conn.execute("INSERT INTO users (name, email) VALUES ($1, $2)", ["Alice", "alice@example.com"])
conn.execute("INSERT INTO users (name, email) VALUES ($1, $2)", ["Bob", "bob@example.com"])
conn.commit()

# SELECT
cur = conn.execute("SELECT id, name, email FROM users ORDER BY id")
for row in cur.fetchall():
    print(row)  # (1, 'Alice', 'alice@example.com')

# UPDATE
conn.execute("UPDATE users SET email = $1 WHERE name = $2", ["newalice@example.com", "Alice"])
conn.commit()

# DELETE
conn.execute("DELETE FROM users WHERE name = $1", ["Bob"])
conn.commit()

conn.close()
```

### Transactions

```python
conn = decentdb.connect("/path/to/database.ddb")

try:
    conn.execute("INSERT INTO users (name) VALUES ($1)", ["Carol"])
    conn.commit()
except Exception:
    conn.rollback()
    raise
```

Or use the context manager:

```python
with decentdb.connect("/path/to/database.ddb") as conn:
    conn.execute("INSERT INTO users (name) VALUES ($1)", ["Carol"])
    # auto-commits on exit, auto-rollbacks on exception
```

### TIMESTAMP / datetime

The DB-API driver binds `datetime.datetime` to `TIMESTAMP` as **microseconds since Unix epoch (UTC)**. Naive datetimes are treated as UTC.

```python
import datetime

conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, occurred_at TIMESTAMP NOT NULL)")
now = datetime.datetime.now(datetime.timezone.utc)
conn.execute("INSERT INTO events (occurred_at) VALUES ($1)", [now])
conn.commit()

(occurred_at,) = conn.execute("SELECT occurred_at FROM events").fetchone()
print(occurred_at)  # timezone-aware datetime (UTC)
```

### Schema Introspection

```python
# List all tables
tables = conn.list_tables()  # ["users", "orders"]

# Get column metadata for a table
cols = conn.get_table_columns("users")
for col in cols:
    print(f"  {col['name']} {col['type']} pk={col['primary_key']} notnull={col['not_null']}")

# List all indexes
indexes = conn.list_indexes()
for idx in indexes:
    print(f"  {idx['name']} on {idx['table']} ({idx['columns']}) unique={idx['unique']}")
```

### Checkpoint

```python
conn.checkpoint()  # flush WAL to main database file
```

### Cross-Connection Visibility

Multiple connections to the same on-disk database automatically share WAL state.
Commits on one connection are immediately visible to reads on another — no
reconnection required:

```python
conn_a = decentdb.connect("app.ddb")
conn_b = decentdb.connect("app.ddb")

conn_a.execute("INSERT INTO t VALUES (1, 'hello')")
conn_a.commit()

# conn_b sees the row immediately
row = conn_b.execute("SELECT * FROM t WHERE id = 1").fetchone()
```

### WAL Eviction

Before replacing or deleting a database file on disk (e.g. restoring a backup),
evict its shared WAL entry so the next `connect()` starts fresh:

```python
decentdb.evict_shared_wal("/path/to/app.ddb")
# Safe to replace the file now; next connect() creates a new WAL
```

### In-Memory Databases

Use `:memory:` for an ephemeral in-memory database (case-insensitive):

```python
conn = decentdb.connect(":memory:")
conn.execute("CREATE TABLE cache (key TEXT PRIMARY KEY, val TEXT)")
conn.execute("INSERT INTO cache (key, val) VALUES ($1, $2)", ["k1", "hello"])
conn.commit()
# Data is lost when the connection is closed
```

### SaveAs (Export to Disk)

Export any open database — including `:memory:` — to a new on-disk file:

```python
conn = decentdb.connect(":memory:")
conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
conn.execute("INSERT INTO items (id, name) VALUES ($1, $2)", [1, "widget"])
conn.commit()

conn.save_as("/tmp/snapshot.ddb")
conn.close()
```

`save_as` performs a full checkpoint, then copies all pages atomically. The destination must not already exist.

## Data Types

DecentDB supports the following column types:

| Python Type | DecentDB Type | Notes |
|-------------|---------------|-------|
| `int` | `INT64` | 64-bit signed integer |
| `float` | `FLOAT64` | 64-bit floating point |
| `decimal.Decimal` | `DECIMAL(p,s)` | Fixed-point precision |
| `str` | `TEXT` | UTF-8 encoded string |
| `bytes` | `BLOB` | Binary data |
| `bool` | `BOOL` | True/False |
| `uuid.UUID` | `UUID` | UUID v4 |
| `datetime.datetime` | `DATETIME` / `TIMESTAMP` | UTC timezone-aware |
| `None` | NULL | SQL NULL |

See `examples/python/example_types.py` for detailed examples of each type.

### DECIMAL

```python
import decimal

cur.execute("CREATE TABLE financials (id INT, amount DECIMAL(18, 2))")
cur.execute("INSERT INTO financials VALUES (?, ?)", (1, decimal.Decimal("12345.67")))
result = cur.execute("SELECT amount FROM financials").fetchone()
# Returns decimal.Decimal
```

### UUID

```python
import uuid

cur.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)")
cur.execute("INSERT INTO items VALUES (?, ?)", (uuid.uuid4(), "Widget"))
result = cur.execute("SELECT id FROM items").fetchone()
# Returns bytes - convert to UUID manually:
item_uuid = uuid.UUID(bytes=result[0])
```

### BLOB

```python
cur.execute("CREATE TABLE files (id INT, data BLOB)")
cur.execute("INSERT INTO files VALUES (?, ?)", (1, b"\x00\x01\x02"))
result = cur.execute("SELECT data FROM files").fetchone()
# Returns bytes
```

## Bulk Operations

### executemany

Use `cursor.executemany()` for efficient batch inserts:

```python
users = [
    ("Alice", "alice@example.com", 30),
    ("Bob", "bob@example.com", 25),
]
cur.executemany("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", users)
conn.commit()
```

### fetchmany

For large result sets, use `fetchmany()` to stream rows:

```python
cur.execute("SELECT * FROM large_table")
while True:
    batch = cur.fetchmany(1000)
    if not batch:
        break
    process(batch)
```

See `examples/python/example_bulk.py` for performance benchmarks.

## SQLAlchemy Usage

```python
from sqlalchemy import create_engine, text

engine = create_engine("decentdb+pysql:////path/to/database.ddb")

with engine.connect() as conn:
    conn.execute(text("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"))
    conn.execute(text("INSERT INTO users (name) VALUES ('Alice')"))  # id auto-assigned
    conn.commit()

    rows = conn.execute(text("SELECT * FROM users")).all()
    print(rows)
```

## Concurrency Model

DecentDB is optimized for a single process with **one writer** and **many concurrent readers** under snapshot isolation. The Python binding is DB-API threadsafety level 1: **use separate connections per thread**.

```python
# Correct: each thread has its own connection
def worker():
    conn = decentdb.connect("/path/to/db.ddb")  # one per thread
    # ... do work
    conn.close()

# Wrong: sharing a connection across threads is not safe
```

See `examples/python/example_threading.py` for a complete multi-threaded read example.

### FastAPI

DecentDB works well with FastAPI. Since DecentDB is a sync library, use a **ThreadPoolExecutor** to avoid blocking the event loop:

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Depends
import decentdb

app = FastAPI()
db_pool = ThreadPoolExecutor(max_workers=8)
DB_PATH = "/path/to/database.ddb"

async def run_db(func, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(db_pool, lambda: func(*args))

def get_items(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM items")
    return cur.fetchall()

@app.get("/items")
async def list_items():
    conn = decentdb.connect(DB_PATH)
    try:
        return await run_db(get_items, conn)
    finally:
        conn.close()
```

See these complete examples:
- `examples/python/example_fastapi.py` - Basic FastAPI CRUD with dependency injection
- `examples/python/example_fastapi_async.py` - Async endpoints with ThreadPoolExecutor
- `examples/python/example_fastapi_sqlalchemy.py` - FastAPI + SQLAlchemy ORM

## Import Tools

### SQLite Import

```bash
decentdb-sqlite-import source.db output.ddb [--overwrite] [--no-progress]
```

### PostgreSQL Import

```bash
decentdb-pgbak-import dump.sql.gz output.ddb [--overwrite] [--no-progress] [--preserve-case]
```

See the [Import Tools Guide](../development/import-tools.md) for detailed documentation.
