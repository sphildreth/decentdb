# decentdb-go

Go `database/sql` driver for DecentDB.

## Validation

Run the Go binding test suite, including the strict cgo pointer checker:

```bash
go test ./...
GOEXPERIMENT=cgocheck2 go test ./...
```

## Benchmark

Run the fair DecentDB vs SQLite benchmark:

```bash
cargo build -p decentdb --release
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
