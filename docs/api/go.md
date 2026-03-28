# Go driver

DecentDB ships an in-tree Go package under `bindings/go/decentdb-go/`.

## C ABI coverage

The Go binding exposes 28 of 50 C ABI functions directly through cgo.
Performance-critical fused `step_row_view` is implemented (reduces cgo crossings
per row from 2 to 1). Batch, re-execute, and fused bind+step operations remain
as future optimizations.

## Package surface

The Go package:

- registers the `decentdb` driver with Go's `database/sql`
- accepts plain file paths, `file:/...` DSNs, and `:memory:`
- supports DSN mode parameter: `?mode=create|open|open_or_create`
- exposes a direct `OpenDirect()` path for DecentDB-specific helpers

### Type support

| Go Type | DecentDB Type | Notes |
|---------|--------------|-------|
| `int64` | INT64 | Also accepts `int` |
| `float64` | FLOAT64 | |
| `bool` | BOOL | |
| `string` | TEXT | |
| `[]byte` | BLOB | Also reads UUID |
| `time.Time` | TIMESTAMP | Microsecond precision |
| `Decimal{Unscaled, Scale}` | DECIMAL | Explicit decimal type |

## Use the Go driver from an application

```bash
go get github.com/sphildreth/decentdb-go
```

## Minimal `database/sql` usage

```go
import (
    "database/sql"
    _ "github.com/sphildreth/decentdb-go"
)

db, err := sql.Open("decentdb", "file:/tmp/app.ddb")
```

### DSN modes

```go
// Open or create (default)
db, err := sql.Open("decentdb", "file:/tmp/app.ddb")

// Create only — fails if file exists
db, err := sql.Open("decentdb", "file:/tmp/app.ddb?mode=create")

// Open only — fails if file doesn't exist
db, err := sql.Open("decentdb", "file:/tmp/app.ddb?mode=open")
```

## Version introspection

```go
abi := decentdb.AbiVersion()       // e.g. 1
ver := decentdb.EngineVersion()    // e.g. "2.0.0"
```

## Direct API access

The `DB` type provides DecentDB-specific operations beyond `database/sql`:

```go
import "github.com/sphildreth/decentdb-go"

db, err := decentdb.OpenDirect("/tmp/app.ddb")
if err != nil { log.Fatal(err) }
defer db.Close()

// Schema introspection
tables, _ := db.ListTables()
columns, _ := db.GetTableColumns("users")
indexes, _ := db.ListIndexes()
ddl, _ := db.GetTableDdl("users")
views, _ := db.ListViews()
viewDdl, _ := db.GetViewDdl("v_active_users")
triggers, _ := db.ListTriggers()

// Transaction state
if db.InTransaction() {
    // engine has an active transaction
}

// Maintenance
db.Checkpoint()
db.SaveAs("/tmp/backup.ddb")
```

## Full example

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/sphildreth/decentdb-go"
    decentdb "github.com/sphildreth/decentdb-go"
)

func main() {
    // Open database
    db, err := sql.Open("decentdb", "file:example.ddb")
    if err != nil { log.Fatal(err) }
    defer db.Close()

    // Create table
    _, err = db.Exec(`CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
    )`)
    if err != nil { log.Fatal(err) }

    // Insert with transaction
    tx, _ := db.Begin()
    _, err = tx.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com")
    if err != nil { tx.Rollback(); log.Fatal(err) }
    tx.Commit()

    // Query
    rows, _ := db.Query("SELECT id, name FROM users WHERE id = $1", 1)
    defer rows.Close()
    for rows.Next() {
        var id int64
        var name string
        rows.Scan(&id, &name)
        fmt.Printf("id=%d name=%s\n", id, name)
    }

    // Direct API
    direct, _ := decentdb.OpenDirect("example.ddb")
    defer direct.Close()
    tables, _ := direct.ListTables()
    fmt.Println("Tables:", tables)
    ddl, _ := direct.GetTableDdl("users")
    fmt.Println("DDL:", ddl)
    fmt.Println("InTransaction:", direct.InTransaction())
}
```

## Build the native library

From the repository root:

```bash
cargo build -p decentdb --release
```

## Run tests

```bash
cd bindings/go/decentdb-go
go test -v ./...
```

## Run benchmarks

```bash
cd bindings/go/decentdb-go
go run ./benchmarks/bench_fetch/main.go --count 100000 --point-reads 5000 --engine=all
```

Benchmark results with 10K rows (DecentDB vs SQLite):

| Metric | DecentDB | SQLite | Ratio |
|--------|----------|--------|-------|
| Insert throughput | 398K rows/s | 177K rows/s | 2.2x |
| Fetchall | 8.4ms | 9.5ms | 1.1x |
| Fetchmany | 7.9ms | 9.7ms | 1.2x |
| Point read p50 | 0.004ms | 0.014ms | 3.2x |
| Point read p95 | 0.006ms | 0.018ms | 2.8x |

## Thread safety

The DecentDB engine supports one writer and multiple concurrent readers per process.
Go's `database/sql` manages its own connection pool, and each `*sql.DB` is safe for
concurrent use. The `OpenDirect()` `*DB` type is **not** safe for concurrent use —
create one per goroutine if needed.
