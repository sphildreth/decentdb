# Browser binding smoke

This directory contains browser integration coverage for `@decentdb/web` in a real
browser (Playwright) and OPFS-backed owner runtime.

## Automated OPFS smoke (S9 coverage)

From `bindings/web`:

```bash
cd /home/steven/src/github/decentdb/bindings/web
npm ci
npm run build
```

Build the DecentDB wasm artifact for `worker.js`/`decentdb_wasm.js`, then run:

```bash
cd /home/steven/src/github/decentdb
cargo build -p decentdb --target wasm32-unknown-unknown --release
wasm-bindgen target/wasm32-unknown-unknown/release/decentdb.wasm \
  --target web \
  --out-dir bindings/web/dist \
  --out-name decentdb_wasm
cd bindings/web
```

```bash
npm run browser:smoke
```

`npm run browser:smoke` exercises:

- capability probes and stable unsupported-runtime errors
- Dedicated Worker owner routing through BroadcastChannel and Web Locks
- multi-tab owner sharing and recovery after the original owner tab closes
- create/open flow
- query
- reopen with `open`
- binary and JSON result transports
- export
- checkpoint
- persist helper
- import into a second database handle
- browser runtime/storage/sync diagnostics

## Transport benchmark (S7 coverage)

From `bindings/web`, after building the package and wasm artifact:

```bash
npm run browser:bench
```

The benchmark exercises binary and JSON result transports against the same
large-result shape and reports query time, row count, and WASM memory samples.

If your environment lacks browser binaries, install Chromium once:

```bash
npm run browser:install
```

If you only need the manual page for local inspection:

1. Serve the repository root (so `bindings/web/dist` and `tests/bindings/web` are both addressable).
2. Open `tests/bindings/web/smoke.html` in an OPFS-capable browser.
