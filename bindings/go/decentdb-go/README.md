# decentdb-go

Go `database/sql` driver for DecentDB.

## Type notes

Semantic result values decode as Go-native shapes:

- `ENUM` -> `EnumValue{TypeID, LabelID}`
- `IPADDR`, `CIDR`, `MACADDR` -> canonical `string`
- `DATE`, `TIMESTAMPTZ` -> UTC `time.Time`
- `TIME` -> `time.Duration`
- `INTERVAL` -> `IntervalValue{Months, Days, Micros}`

## Write queue support

The driver supports the bounded write queue via DSN options:

- `write_queue_enabled`
- `write_queue_capacity`
- `write_queue_default_timeout_ms`
- `write_queue_group_commit`
- `write_queue_max_batch`
- `write_queue_max_group_delay_us`

Queue options are passed as query parameters in the driver DSN:

```go
db, err := sql.Open("decentdb", "file:demo.ddb?write_queue_enabled=true&write_queue_capacity=128&write_queue_default_timeout_ms=1000")
if err != nil {
    log.Fatal(err)
}
```

With a queue-enabled connection, write SQL (for example `INSERT`, `UPDATE`, `CREATE`) is routed through
`ddb_db_execute_queued` automatically via `ExecContext`/`Exec`.

For explicit queued execution, the direct helper is available on the custom binding type:

```go
dbDirect, err := decentdb.OpenDirect("demo.ddb")
if err != nil { ... }
rows, err := dbDirect.ExecQueued(context.Background(), "INSERT INTO users(name) VALUES ($1)", "Alice")
if err != nil { ... }
_ = rows
```

Queue counters can be read through:

```go
metrics, err := dbDirect.WriteQueueMetrics()
```

If write queue mode is not configured, these helpers return `ErrQueueClosed`.

where `dbDirect` is the custom binding type returned by `OpenDirect(...)` or a direct queue-enabled handle.

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
