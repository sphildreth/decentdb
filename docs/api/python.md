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
    conn.execute(sqlalchemy.text("CREATE TABLE IF NOT EXISTS users (id INT, name TEXT)"))
    conn.execute(sqlalchemy.text("INSERT INTO users VALUES (1, 'Alice')"))
    conn.commit()

    rows = conn.execute(sqlalchemy.text("SELECT * FROM users")).all()
    print(rows)
```

## Concurrency model

DecentDB is currently optimized for a single process with **one writer** and **many concurrent readers** under snapshot isolation.
