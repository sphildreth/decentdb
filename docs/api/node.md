# Node.js Bindings

DecentDB’s Node.js integrations live under `bindings/node/`:

- `bindings/node/decentdb`: N-API native addon (`decentdb-native`) + minimal JS wrapper
- `bindings/node/knex-decentdb`: Knex client/dialect (`knex-decentdb`) with automatic placeholder rewriting

## Build the native library

The Node addon loads the DecentDB C API dynamically at runtime.

From the repo root:

```bash
nimble build_lib
```

On Linux this produces `build/libc_api.so`.

## Using `decentdb-native`

Build the addon:

```bash
cd bindings/node/decentdb
npm install
npm run build
```

Point it at the native library (Linux example):

```bash
export DECENTDB_NATIVE_LIB_PATH=$PWD/../../../build/libc_api.so
node --test
```

## Parameter style

DecentDB requires Postgres-style positional parameters (`$1`, `$2`, ...).

- `decentdb-native` rejects unquoted `?` placeholders.
- `knex-decentdb` automatically rewrites Knex’s `?` placeholders to `$N` (and respects strings/comments).
