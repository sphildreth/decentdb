# Node.js bindings

This folder contains the Node.js ecosystem integrations for DecentDB.

Packages:
- `bindings/node/decentdb`: N-API native addon + minimal JS wrapper
- `bindings/node/knex-decentdb`: Knex client/dialect for DecentDB

These bindings are designed to sit on top of DecentDB's stable native C ABI
(`include/decentdb.h`).

## Status

The N-API native addon and JS wrapper are production-ready (26 tests covering all data types, error handling, schema introspection, transactions, async iteration, and statement lifecycle).

## Build native library (local)

From the repository root:

```bash
cargo build -p decentdb
```

This produces:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

Then point the Node addon at it with `DECENTDB_NATIVE_LIB_PATH` (see package README).
