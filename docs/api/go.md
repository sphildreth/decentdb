# Go Bindings

DecentDB provides a Go `database/sql` driver and a direct API under `bindings/go/decentdb-go/`.

## Build the native library

```bash
nimble build_lib
```

On Linux this produces `build/libc_api.so`.

## database/sql Driver

Register the driver (blank import), then use `database/sql` as usual:

```go
package main

import (
  "database/sql"
  "fmt"
  _ "github.com/sphildreth/decentdb-go"
)

func main() {
  db, err := sql.Open("decentdb", "file:/tmp/sample.ddb")
  if err != nil {
    panic(err)
  }
  defer db.Close()

  db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT)")

  // Auto-increment: omit id column
  db.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com")
  db.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@example.com")

  // Query
  rows, _ := db.Query("SELECT id, name, email FROM users ORDER BY id")
  defer rows.Close()
  for rows.Next() {
    var id int64
    var name, email string
    rows.Scan(&id, &name, &email)
    fmt.Printf("id=%d name=%s email=%s\n", id, name, email)
  }
}
```

### Transactions

```go
tx, _ := db.Begin()
tx.Exec("INSERT INTO users (name) VALUES ($1)", "Carol")
tx.Commit()  // or tx.Rollback()
```

### Decimal Type

The driver provides a `Decimal` type for fixed-precision values:

```go
import decentdb "github.com/sphildreth/decentdb-go"

// Insert a decimal value
db.Exec("INSERT INTO accounts (balance) VALUES ($1)", decentdb.Decimal{Unscaled: 12345, Scale: 2})  // 123.45

// Read a decimal value
var d decentdb.Decimal
row := db.QueryRow("SELECT balance FROM accounts WHERE id = $1", 1)
row.Scan(&d)
fmt.Printf("%.2f\n", float64(d.Unscaled) / math.Pow10(int(d.Scale)))
```

## Direct API (OpenDirect)

For DecentDB-specific features beyond `database/sql`, use `OpenDirect`:

```go
import decentdb "github.com/sphildreth/decentdb-go"

db, _ := decentdb.OpenDirect("/tmp/sample.ddb")
defer db.Close()

db.Exec("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
db.Exec("INSERT INTO items (name) VALUES ($1)", "Widget")
```

### Checkpoint

```go
db.Checkpoint()  // flush WAL to main database file
```

### Schema Introspection

```go
// List all tables
tables, _ := db.ListTables()
fmt.Println(tables)  // ["items", "users"]

// Get column metadata
cols, _ := db.GetTableColumns("users")
for _, c := range cols {
    fmt.Printf("  %s %s pk=%v notnull=%v\n", c.Name, c.Type, c.PrimaryKey, c.NotNull)
}

// List all indexes
indexes, _ := db.ListIndexes()
for _, idx := range indexes {
    fmt.Printf("  %s on %s (%v) unique=%v\n", idx.Name, idx.Table, idx.Columns, idx.Unique)
}
```

## DSN Format

The driver accepts either:

- A file URL: `file:/path/to.ddb?opt=value`
- A raw path: `/path/to.ddb`

## Parameter Style

DecentDB uses Postgres-style positional placeholders (`$1`, `$2`, ...). The driver rejects `?` and `@name` with a clear error message to prevent silent misbinding.
