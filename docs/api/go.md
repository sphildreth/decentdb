# Go Bindings

DecentDB provides a Go `database/sql` driver under `bindings/go/decentdb-go/`.

## Status

This driver is intended for embedded usage and currently links against the DecentDB C API via CGO.

## Build the native library

From the repo root:

```bash
nimble build_lib
```

On Linux this produces `build/libc_api.so`.

## Usage

Register the driver (blank import), then use `database/sql` as usual:

```go
package main

import (
  "database/sql"
  _ "github.com/sphildreth/decentdb-go"
)

func main() {
  db, err := sql.Open("decentdb", "file:/tmp/sample.ddb")
  if err != nil {
    panic(err)
  }
  defer db.Close()

  if _, err := db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"); err != nil {
    panic(err)
  }

  // DecentDB uses Postgres-style placeholders ($1..$N)
  // id is auto-assigned when omitted from the column list
  if _, err := db.Exec("INSERT INTO users (name) VALUES ($1)", "Alice"); err != nil {
    panic(err)
  }
}
```

## DSN format

The driver accepts either:

- A file URL like `file:/path/to.db.ddb?opt=value`
- A raw path like `/path/to.db.ddb`

## Parameter style guardrails

To avoid silent misbinding, the driver rejects common unsupported placeholder styles (such as `?` and `@name`). Use `$1..$N`.
