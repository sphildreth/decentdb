# Python Validation

Phase 4 currently ships the Python validation suite over the stable C ABI rather than a packaged DB-API driver.

The validation lives in:

```text
tests/bindings/python/test_ffi.py
```

It covers:
- open / close
- execute
- positional parameter binding
- result retrieval
- error retrieval
- explicit transaction control
- `save_as`

## Run locally

Build the Rust cdylib first:

```bash
cargo build -p decentdb
python3 tests/bindings/python/test_ffi.py
```

Override the native library path if needed:

```bash
DECENTDB_NATIVE_LIB=/path/to/libdecentdb.so python3 tests/bindings/python/test_ffi.py
```
