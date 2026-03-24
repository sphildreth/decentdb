# Go driver

DecentDB ships an in-tree Go package under `bindings/go/decentdb-go/`.

## Package surface

The Go package:

- registers the `decentdb` driver with Go's `database/sql`
- accepts plain file paths, `file:/...` DSNs, and `:memory:`
- exposes a direct `OpenDirect()` path for DecentDB-specific helpers such as
  `Checkpoint()`, `SaveAs()`, `ListTables()`, `GetTableColumns()`, and
  `ListIndexes()`

Minimal `database/sql` usage:

```go
import (
    "database/sql"
    _ "github.com/sphildreth/decentdb-go"
)

db, err := sql.Open("decentdb", "file:/tmp/app.ddb")
```

## Use the Go driver from an application

For application development, prefer depending on the Go driver package through
normal Go modules instead of using the smoke program directly.

```bash
go get github.com/sphildreth/decentdb-go
```

The Go package still relies on the DecentDB shared library. The easiest ways to
provide that library are:

- a DecentDB release bundle
- or a local `cargo build -p decentdb`

## Build the native library

From the repository root:

```bash
cargo build -p decentdb
```

## Work on the package locally

```bash
cd bindings/go/decentdb-go
go test ./...
```

## Run the C ABI smoke validation

The repository also keeps a narrow release smoke program under
`tests/bindings/go/smoke.go`.

```bash
cargo build -p decentdb
go run ./tests/bindings/go/smoke.go
```
