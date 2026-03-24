# Go Binding

This directory contains the in-tree Go `database/sql` driver package at
`bindings/go/decentdb-go`.

## Benchmark

From the repository root:

```bash
cargo build -p decentdb --release
cd bindings/go/decentdb-go
go run ./benchmarks/bench_fetch/main.go --count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix go_bench_fetch
```

Supported options:

- `--engine=all|decentdb|sqlite`
- `--count`
- `--point-reads`
- `--fetchmany-batch`
- `--point-seed`
- `--db-prefix` (DecentDB writes `.ddb`, SQLite writes `.db`)
- `--keep-db`
