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

The Python binding declares the C ABI functions used by the packaged DB-API,
metadata, maintenance, and fast-path surfaces. Performance-critical paths
(batch execution, fused bind+step, re-execute, zero-copy row views) are
accelerated by the `_fastdecode.c` C extension when available, falling back to
ctypes otherwise.

The DB-API layer also declares the C ABI write-queue entry points. Queue
configuration can be passed through `connect(...)`, and queued execution is
available as `Connection.execute_queued(...)` or `Cursor.execute_queued(...)`.

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

# Pass native open options when you need profiles or low-level knobs
conn = decentdb.connect(
    "/path/to/data.ddb",
    options="profile=embedded_fast;cache_size=64MB",
)
```

### Cross-Process WAL Coordination

Local on-disk databases use cross-process WAL coordination by default when the
native VFS supports file locks. Pass explicit options when a Python application,
CLI helper, or background worker must fail instead of running without the
coordination sidecar:

```python
conn = decentdb.connect(
    "/path/to/data.ddb",
    process_coordination="required",
    process_coordination_timeout_ms=30_000,
)

coordination = conn.execute("SELECT * FROM sys.process_coordination").fetchone()
```

Supported modes are `"auto"`, `"required"`, and `"single_process_unsafe"`.

### Write Queue

```python
conn = decentdb.connect(
    "/path/to/data.ddb",
    write_queue_enabled=True,
    write_queue_capacity=128,
    write_queue_default_timeout_ms=1000,
    write_queue_group_commit=True,
)

conn.execute_queued(
    "INSERT INTO events (id, name) VALUES (?, ?)",
    (1, "queued"),
)

metrics = conn.write_queue_metrics()
```

Queued writes preserve DecentDB's one-writer model and use strict group commit
without weakening default durable acknowledgement semantics.
The Python keyword `write_queue_group_commit` maps to the native
`write_queue_strict_group_commit` option.

### Reactive Subscriptions

Python exposes reactive watch handles over the C ABI JSON event stream:

```python
conn = decentdb.connect("/tmp/app.ddb")
conn.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")

watch = conn.watch_query("SELECT id, name FROM items ORDER BY id")
initial = watch.next(timeout_ms=1000)      # {"type": "initial", ...}

conn.execute("INSERT INTO items VALUES (?, ?)", (1, "Ada"))
event = watch.next(timeout_ms=1000)        # {"type": "invalidate", ...}

watch.close()
```

Available helpers are `watch_table(...)`, `watch_range(...)`,
`watch_query(...)`, and `change_stream(...)`. `Watch.next(...)` returns `None`
on timeout; `Watch.next_json(...)` returns the raw event JSON.

## Executing queries

```python
cur = conn.cursor()

# DDL
cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
conn.commit()

# Insert with parameters
cur.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])

# Select
cur.execute("SELECT id, name FROM users WHERE id = ?", [1])
row = cur.fetchone()  # (1, "Alice")
rows = cur.fetchall() # [(1, "Alice"), ...]

# Many inserts
cur.executemany(
    "INSERT INTO users (name) VALUES (?)",
    [["Bob"], ["Carol"], ["Dave"]]
)
conn.commit()
```

For sqlite3-style convenience, `Connection.execute(...)` creates and returns a
fresh cursor on each call. Keeping multiple returned cursors alive at once is
supported; later `Connection.execute(...)` calls do not invalidate earlier
result objects.

## Transactions

```python
# Using methods
conn.begin_transaction()
cur.execute("UPDATE users SET name = ? WHERE id = ?", ["Bob", 1])
conn.commit()

# Using context manager
with conn:
    cur.execute("UPDATE users SET name = ? WHERE id = ?", ["Carol", 2])

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

# Stable tooling metadata
metadata = conn.get_tooling_metadata()
contract = conn.describe_query_contract(
    "SELECT id, name FROM users WHERE id = $1"
)
```

## Version introspection

```python
abi = decentdb.abi_version()        # e.g. 4
ver = decentdb.engine_version()     # e.g. "2.0.0"
```

## Native Result Types

The DB-API layer decodes semantic column values from the C ABI into Python
domain objects:

| DecentDB type | Python result value |
|---------------|---------------------|
| `ENUM` | `decentdb.EnumValue(type_id, label_id)` |
| `IPADDR` / `INET` | `ipaddress.IPv4Address` or `ipaddress.IPv6Address` |
| `CIDR` | `ipaddress.IPv4Network` or `ipaddress.IPv6Network` |
| `DATE` | `datetime.date` |
| `TIME` | `datetime.time` |
| `TIMESTAMPTZ` | timezone-aware `datetime.datetime` in UTC |
| `INTERVAL` | `decentdb.IntervalValue(months, days, micros)` |
| `MACADDR` / `MACADDR8` | canonical lowercase `str` |

String parameters can be used for semantic columns when the target column type
is known from SQL, such as inserting `'2001:db8::/32'` into `CIDR` or `'paid'`
into an inline enum column.

## Maintenance

```python
conn.checkpoint()                      # Fold committed WAL frames into the database file
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
| `Date` | `DATE` | Stored as day count |
| `DateTime` | `TIMESTAMP` | Microsecond precision |
| `DateTime(timezone=True)` | `TIMESTAMPTZ` | Normalized to UTC |
| `Time` | `TIME` | Stored as microsecond-of-day |
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
