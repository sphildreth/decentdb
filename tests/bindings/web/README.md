# Browser binding smoke

This directory contains browser integration coverage for `@decentdb/web` in a real
browser (Playwright) and OPFS-backed worker runtime.

## Automated OPFS smoke (S9 coverage)

From `bindings/web`:

```bash
cd /home/steven/src/github/decentdb/bindings/web
npm install
npm run build
```

Build the DecentDB wasm artifact for `worker.js`/`decentdb_wasm.js` with your
existing wasm workflow, then run:

```bash
npm run browser:smoke
```

`npm run browser:smoke` exercises:

- create/open flow
- query
- reopen with `open`
- export
- checkpoint
- persist helper
- import into a second database handle

If your environment lacks browser binaries, install Chromium once:

```bash
npm run browser:install
```

If you only need the manual page for local inspection:

1. Serve the repository root (so `bindings/web/dist` and `tests/bindings/web` are both addressable).
2. Open `tests/bindings/web/smoke.html` in an OPFS-capable browser.
