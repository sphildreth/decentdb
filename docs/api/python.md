# Python bindings

DecentDB ships in-tree Python bindings under `bindings/python/`.

## Package surfaces

The Python tree currently includes:

- `decentdb` — a DB-API 2.0 driver
- `decentdb_sqlalchemy` — a SQLAlchemy 2.x dialect
- import tools exposed as `decentdb-sqlite-import` and
  `decentdb-pgbak-import`

The source of truth for the packaged Python surface lives in:

```text
bindings/python/decentdb/
bindings/python/decentdb_sqlalchemy/
bindings/python/tests/
```

## C ABI coverage

The Python binding declares and exposes **all 50 C ABI functions** defined in
`include/decentdb.h`. Performance-critical paths (batch execution, fused
bind+step, re-execute, zero-copy row views) are accelerated by the
`_fastdecode.c` C extension when available, falling back to ctypes otherwise.

## Use the packaged Python binding

For application development, prefer the packaged `decentdb` Python binding
instead of calling the raw FFI validation script directly.

If you are consuming a published release, install `decentdb` from your package
index. From a source checkout, the equivalent is:

```bash
python3 -m pip install -e bindings/python
```

The Python package still needs the DecentDB shared library at runtime. The
easiest ways to satisfy that are:

- use a DecentDB release bundle that includes the native library
- or build it locally with `cargo build -p decentdb`

## Connecting to a database

```python
import decentdb

# Open or create (default)
conn = decentdb.connect("/path/to/data.ddb")

# Create only — raises DatabaseError if file exists
conn = decentdb.connect("/path/to/data.ddb", mode="create")

# Open only — raises DatabaseError if file doesn't exist
conn = decentdb.connect("/path/to/data.ddb", mode="open")

# Or use Connection directly
conn = decentdb.Connection("/path/to/data.ddb", mode="open_or_create", stmt_cache_size=256)
```

## Executing queries

```python
cur = conn.cursor()

# DDL
cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
conn.commit()

# Insert with parameters
cur.execute("INSERT INTO users (name) VALUES ($1)", ["Alice"])

# Select
cur.execute("SELECT id, name FROM users WHERE id = $1", [1])
row = cur.fetchone()  # (1, "Alice")
rows = cur.fetchall() # [(1, "Alice"), ...]

# Many inserts
cur.executemany(
    "INSERT INTO users (name) VALUES ($1)",
    [["Bob"], ["Carol"], ["Dave"]]
)
conn.commit()
```

## Transactions

```python
# Using methods
conn.begin_transaction()
cur.execute("UPDATE users SET name = $1 WHERE id = $2", ["Bob", 1])
conn.commit()

# Using context manager
with conn:
    cur.execute("UPDATE users SET name = $1 WHERE id = $2", ["Carol", 2])

# Check transaction state (queries the engine)
if conn.in_transaction:
    conn.rollback()
```

## Schema introspection

```python
# Tables
tables = conn.list_tables()               # ["users", "orders"]
cols = conn.get_table_columns("users")    # [{"name": "id", "type": "INT64", ...}, ...]
ddl = conn.get_table_ddl("users")         # "CREATE TABLE users (id INTEGER PRIMARY KEY, ...)"

# Indexes
indexes = conn.list_indexes()             # [{"name": "idx_users_name", ...}, ...]

# Views
views = conn.list_views()                 # ["v_active_users", ...]
view_ddl = conn.get_view_ddl("v_active_users")

# Triggers
triggers = conn.list_triggers()           # [...]
```

## Version introspection

```python
abi = decentdb.abi_version()        # e.g. 1
ver = decentdb.engine_version()     # e.g. "2.0.0"
```

## Maintenance

```python
conn.checkpoint()                      # WAL checkpoint
conn.save_as("/path/to/backup.ddb")   # Online backup
decentdb.evict_shared_wal("/path/to/data.ddb")  # Evict shared WAL
```

## SQLAlchemy

Register the dialect and use standard SQLAlchemy 2.x:

```python
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Uuid, DateTime
from sqlalchemy.orm import Session, declarative_base, select

engine = create_engine("decentdb:///path/to/data.ddb")
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    balance = Column(Numeric(10, 2))  # Maps to DECIMAL
    created = Column(DateTime)         # Maps to TIMESTAMP (microsecond precision)
    token = Column(Uuid)               # Maps to UUID

Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(User(id=1, name="Alice"))
    session.commit()

with Session(engine) as session:
    user = session.execute(select(User)).scalar_one()
```

### Type mappings

| SQLAlchemy type | DecentDB type | Notes |
|----------------|---------------|-------|
| `Integer`/`BigInteger` | `INT64` | |
| `Float` | `FLOAT64` | |
| `Boolean` | `BOOL` | |
| `String`/`Text` | `TEXT` | |
| `Numeric` | `DECIMAL` | Preserves precision/scale |
| `LargeBinary` | `BLOB` | |
| `Date` | `TIMESTAMP` | Stored as microsecond epoch |
| `DateTime` | `TIMESTAMP` | Microsecond precision, UTC |
| `Time` | `TIMESTAMP` | Stored as microsecond-of-day |
| `Uuid` | `UUID` | Native 128-bit UUID |

## Work on the package locally

```bash
python3 -m pip install -e bindings/python
pytest -q bindings/python/tests
```

## Run the C ABI validation suite

The repository also keeps a direct native validation path under
`tests/bindings/python/test_ffi.py`.

```bash
cargo build -p decentdb
python3 tests/bindings/python/test_ffi.py
```

Override the native library path if needed:

```bash
DECENTDB_NATIVE_LIB=/path/to/libdecentdb.so python3 tests/bindings/python/test_ffi.py
```

See `bindings/python/README.md` for higher-level usage examples with DB-API and
SQLAlchemy.
