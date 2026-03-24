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
