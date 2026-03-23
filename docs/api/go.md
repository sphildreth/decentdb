# Go Smoke Coverage

The Go release smoke test lives in:

```text
tests/bindings/go/smoke.go
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
go run ./tests/bindings/go/smoke.go
```
