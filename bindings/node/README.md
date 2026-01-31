# Node.js bindings

This folder contains the Node.js ecosystem integrations for DecentDB.

Packages:
- `bindings/node/decentdb`: N-API native addon + minimal JS wrapper
- `bindings/node/knex-decentdb`: Knex client/dialect for DecentDB

These bindings are designed to sit on top of DecentDB’s stable native C ABI (`src/c_api.nim`).

## Status

Scaffolded: build/test wiring + initial API surface. The addon currently expects a DecentDB native library built from `src/c_api.nim` to be available at runtime.

## Build native library (local)

From repo root:

- Linux: `nim c -d:release --app:lib --out:libdecentdb.so src/c_api.nim`
- macOS: `nim c -d:release --app:lib --out:libdecentdb.dylib src/c_api.nim`
- Windows: `nim c -d:release --app:lib --out:decentdb.dll src/c_api.nim`

Then point the Node addon at it with `DECENTDB_NATIVE_LIB_PATH` (see package README).
