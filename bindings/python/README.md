# DecentDB Python Bindings

This package provides:
1. `decentdb`: A DB-API 2.0 compliant driver for DecentDB. Like `sqlite3`,
   `Connection.execute(...)` returns a new cursor for each call.
2. `decentdb_sqlalchemy`: A SQLAlchemy 2.x dialect.

## Usage

```python
import sqlalchemy
from sqlalchemy import create_engine

# Use the decentdb dialect
engine = create_engine("decentdb+pysql:////path/to/database.ddb")

with engine.connect() as conn:
    conn.execute(sqlalchemy.text("CREATE TABLE IF NOT EXISTS users (id INT, name TEXT)"))
    conn.execute(sqlalchemy.text("INSERT INTO users VALUES (1, 'Alice')"))
    conn.commit()

    result = conn.execute(sqlalchemy.text("SELECT * FROM users"))
    for row in result:
        print(row)
```

## Semantic result values

The DB-API driver decodes semantic native types directly:

- `ENUM` -> `decentdb.EnumValue(type_id, label_id)`
- `IPADDR` / `INET` -> `ipaddress` address objects
- `CIDR` -> `ipaddress` network objects
- `DATE`, `TIME`, `TIMESTAMPTZ` -> `datetime.date`, `datetime.time`, and
  timezone-aware UTC `datetime.datetime`
- `INTERVAL` -> `decentdb.IntervalValue(months, days, micros)`
- `MACADDR` / `MACADDR8` -> canonical lowercase `str`

SQLAlchemy `Date`, `Time`, and `DateTime(timezone=True)` now compile to the
native `DATE`, `TIME`, and `TIMESTAMPTZ` column types.

## Concurrency Model

DecentDB operates as an embedded database with the following concurrency model:
- **Single Writer**: Only one connection can write to the database at a time.
- **Multiple Readers**: Multiple connections can read simultaneously (Snapshot Isolation).
- **Process Model**: Currently optimized for single-process usage. Multi-process sharing is not guaranteed safe yet.

**Recommendation**: Ensure your application architecture enforces a single-writer pattern (e.g. via a dedicated writer thread or queue).

## Bounded Write Queue (DDB v3)

Python now exposes write-queue execution through both low-level C bindings and the DB-API path.

```python
from decentdb import connect

with connect(
    "queue_demo.ddb",
    write_queue_enabled=True,
    write_queue_capacity=128,
    write_queue_default_timeout_ms=500,
    write_queue_group_commit=True,
) as con:
    con.execute("CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, payload TEXT)")
    con.execute_queued(
        "INSERT INTO events(id, payload) VALUES (?, ?)",
        (1, "queued"),
        timeout_ms=250,
    )
    metrics = con.write_queue_metrics()
    print(metrics["admitted"], metrics["committed"])
```

`write_queue_default_timeout_ms` can be omitted to use the engine default; pass
`DDB_WRITE_QUEUE_TIMEOUT_DEFAULT` to leave a single `execute_queued` call at the native
default.

- `write_queue_enabled`
  Enables queued writer mode for the connection.
- `write_queue_capacity`
  Maximum in-flight queued write entries.
- `write_queue_group_commit`
  Enables queue grouping for durable batching behavior.
- `write_queue_max_batch`
  Maximum statements per commit group.
- `write_queue_max_group_delay_us`
  Maximum delay before a partial batch is forced to commit.
- `write_queue_default_timeout_ms`
  Default timeout applied by direct queued API calls when no explicit timeout is passed.

## Benchmarks

To run the fetch benchmark:
```bash
python benchmarks/bench_fetch.py
```

## SQLite Import

Convert an existing SQLite database file into a DecentDB database file:

```bash
decentdb-sqlite-import /path/to/input.sqlite /path/to/output.decentdb
```

By default, identifiers are normalized to lowercase so you can query without quoting (Postgres-style).

To preserve original SQLite casing (requires quoting identifiers in SQL):

```bash
decentdb-sqlite-import --preserve-case /path/to/input.sqlite /path/to/output.decentdb
```

To overwrite an existing destination:

```bash
decentdb-sqlite-import --overwrite /path/to/input.sqlite /path/to/output.decentdb
```

Write a machine-readable conversion report:

```bash
decentdb-sqlite-import /path/to/input.sqlite /path/to/output.decentdb --report-json report.json
```

Or to stdout:

```bash
decentdb-sqlite-import /path/to/input.sqlite /path/to/output.decentdb --report-json -
```

## Statement Cache Statistics

Connections expose `stmt_cache_stats` as a read-only dictionary with:

- `hits`
- `misses`
- `size`
- `capacity`

Low hit rates usually mean SQL strings are being built with embedded literals
instead of parameters. Prefer parameterized queries so prepared statements can
be reused.

On `Connection.close()`, DecentDB emits `decentdb.PerformanceWarning` when the
statement cache sees at least 100 lookups and the hit rate stays below 50%.
