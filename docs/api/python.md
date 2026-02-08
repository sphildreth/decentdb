# Python Bindings

The Python bindings live under `bindings/python/` and provide:

1. `decentdb`: a DB-API 2.0 driver
2. `decentdb_sqlalchemy`: a SQLAlchemy 2.x dialect

## Install (editable, from this repo)

From the repo root:

```bash
python -m pip install -e bindings/python
```

## Build / locate the native library

The Python bindings load the DecentDB C API via `ctypes`.

Build the shared library from the repo root:

```bash
nimble build_lib
```

The loader will try to find common build artifacts (like `build/libc_api.so`) automatically when running from the repo.

If needed, you can force an explicit path:

```bash
export DECENTDB_NATIVE_LIB=$PWD/build/libc_api.so
```

## SQLAlchemy usage

```python
import sqlalchemy
from sqlalchemy import create_engine

engine = create_engine("decentdb+pysql:////path/to/database.ddb")

with engine.connect() as conn:
    conn.execute(sqlalchemy.text("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT)"))
    conn.execute(sqlalchemy.text("INSERT INTO users (name) VALUES ('Alice')"))  # id auto-assigned
    conn.commit()

    rows = conn.execute(sqlalchemy.text("SELECT * FROM users")).all()
    print(rows)
```

## Concurrency model

DecentDB is currently optimized for a single process with **one writer** and **many concurrent readers** under snapshot isolation.

## Import Tools

The Python bindings include two command-line tools for importing data from other databases:

### SQLite Import (`decentdb-sqlite-import`)

Convert SQLite databases to DecentDB format.

```bash
decentdb-sqlite-import source.db output.ddb [--overwrite] [--no-progress]
```

### PostgreSQL Import (`decentdb-pgbak-import`)

Import PostgreSQL dump files (plain SQL or gzipped) into DecentDB.

```bash
# Basic usage
decentdb-pgbak-import dump.sql.gz output.ddb

# With all options
decentdb-pgbak-import dump.sql output.ddb \
  --overwrite \
  --no-progress \
  --preserve-case \
  --report-json conversion.json \
  --commit-every 10000
```

See the [Import Tools Guide](../development/import-tools.md) for detailed documentation.
