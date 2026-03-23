# Node Smoke Coverage

The Node smoke test is a tiny N-API addon that calls the DecentDB C ABI directly.

Files:

```text
tests/bindings/node/smoke.c
tests/bindings/node/build.sh
tests/bindings/node/smoke.js
```

It proves:
- library load
- database open
- one write
- one read
- one error path

## Run locally

```bash
cargo build -p decentdb
bash tests/bindings/node/build.sh
node tests/bindings/node/smoke.js
```
