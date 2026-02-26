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
conn.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT)")
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
conn.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
conn.execute("INSERT INTO items (id, name) VALUES ($1, $2)", [1, "widget"])
conn.commit()

conn.save_as("/tmp/snapshot.ddb")
conn.close()
```

`save_as` performs a full checkpoint, then copies all pages atomically. The destination must not already exist.

## SQLAlchemy Usage

```python
from sqlalchemy import create_engine, text

engine = create_engine("decentdb+pysql:////path/to/database.ddb")

with engine.connect() as conn:
    conn.execute(text("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT)"))
    conn.execute(text("INSERT INTO users (name) VALUES ('Alice')"))  # id auto-assigned
    conn.commit()

    rows = conn.execute(text("SELECT * FROM users")).all()
    print(rows)
```

## Concurrency Model

DecentDB is optimized for a single process with **one writer** and **many concurrent readers** under snapshot isolation.

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
