# Node.js bindings

DecentDB ships two in-tree Node packages under `bindings/node/`.

## Package surfaces

- `bindings/node/decentdb` — the `decentdb-native` N-API addon plus a thin
  JavaScript `Database` / `Statement` wrapper
- `bindings/node/knex-decentdb` — a Knex client/dialect that rewrites `?`
  placeholders to DecentDB's `$N` parameter style

See the package READMEs under `bindings/node/` for API details and examples.

## Use the packaged Node modules

For application development, prefer the packaged Node layers
(`decentdb-native` and `knex-decentdb`) instead of the raw smoke addon under
`tests/bindings/node/`.

Depending on your environment, that usually means consuming the package through
your normal package-manager flow or from the in-repo package directories under
`bindings/node/`.

The current Node packages still expect the DecentDB shared library to be present
at runtime and referenced via `DECENTDB_NATIVE_LIB_PATH`. The easiest way to
provide that library is either:

- a DecentDB release bundle
- or a local `cargo build -p decentdb`

## Build the native library

From the repository root:

```bash
cargo build -p decentdb
```

This produces the shared library loaded by the Node packages:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

## Run the package tests

Set `DECENTDB_NATIVE_LIB_PATH` to the absolute path of the shared library for
your platform, then run the package suites from the source tree:

```bash
cd bindings/node/decentdb
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm test

cd ../knex-decentdb
DECENTDB_NATIVE_LIB_PATH=/absolute/path/to/target/debug/libdecentdb.so npm test
```

## Run the C ABI smoke validation

The repository also keeps a tiny N-API smoke path under `tests/bindings/node/`:

```bash
cargo build -p decentdb
bash tests/bindings/node/build.sh
node tests/bindings/node/smoke.js
```
