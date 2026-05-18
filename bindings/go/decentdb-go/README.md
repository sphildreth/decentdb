# decentdb-go

Go `database/sql` driver for DecentDB.

## Type notes

Semantic result values decode as Go-native shapes:

- `ENUM` -> `EnumValue{TypeID, LabelID}`
- `IPADDR`, `CIDR`, `MACADDR` -> canonical `string`
- `DATE`, `TIMESTAMPTZ` -> UTC `time.Time`
- `TIME` -> `time.Duration`
- `INTERVAL` -> `IntervalValue{Months, Days, Micros}`

## Validation

Run the Go binding test suite, including the strict cgo pointer checker:

```bash
cargo build -p decentdb --release
go test ./...
GOEXPERIMENT=cgocheck2 go test ./...
```

The cgo linker searches the release target first when it exists, so rebuild the
release native library before running the full suite after C ABI changes.

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
