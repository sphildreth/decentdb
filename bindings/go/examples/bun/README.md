# Bun ORM with DecentDB

This executable example uses [Bun](https://github.com/uptrace/bun), its standard
PostgreSQL dialect, and DecentDB's native Go `database/sql` driver:

```go
sqldb, err := sql.Open("decentdb", dsn)
// Handle err and configure the pool before use.
db := bun.NewDB(sqldb, pgdialect.New())
```

The example pins Bun and `pgdialect` to the same version, v1.3.0. Its local
module replacement uses the driver from this checkout. Compatibility is
implemented in DecentDB's SQL engine and shared Go driver. The application uses
ordinary Bun model tags and APIs, with all PostgreSQL dialect features enabled.

## Run

You need Rust, Go 1.25.5 or newer, cgo enabled, and a C compiler. Build the native
library from this checkout, then run from the example directory:

```bash
# From the repository root:
cargo build -p decentdb --release --locked
cd bindings/go/examples/bun
go run .
```

The default database is `.tmp/bun-orm/demo.ddb` under the repository root.
The example upserts its seed data and can be run repeatedly. After closing and
reopening the database, it verifies the committed invoice and relationship.
Use a dedicated demo database; the example creates and changes `bun_*` tables.

```bash
go run . --db :memory:
go run . --db /tmp/bun-demo.ddb --verbose
go test -count=1 ./...
go vet ./...
```

`--verbose` prints the SQL Bun generates. On Linux/macOS the in-tree driver
links the repository's native library automatically, preferring `target/release`
over `target/debug`. Rebuild the release library after engine changes.

## What it demonstrates

- Model-generated tables, foreign keys, unique constraints, and B-tree indexes.
- `pk,autoincrement` IDs, server defaults, and `INSERT/UPDATE/DELETE RETURNING`.
- CRUD, bulk insertion, conflict upserts, and model scans.
- Belongs-to and has-many relations with native foreign-key enforcement.
- Transaction commit and rollback, plus file persistence across reopen.
- Exact decimal arithmetic, indexed decimal filters, ordering, and pagination.
- CTE reports with exact `SUM`, `AVG`, and `MAX` results.
- JSON values and extraction, byte arrays, nullable fields, and timestamps with
  microsecond precision.
- Bun soft deletion, queries for deleted rows, and permanent deletion.
- Bun migrations with apply, rollback, and migration locking.

For example, `19.99 + 0.01` returns the exact decimal `20`. The unpaid invoices
of `0.01` and `0.1` report a total of `0.11` and an average of `0.055`.
`decentdb.Decimal` implements the standard `sql.Scanner` and `driver.Valuer`
interfaces and preserves values without a float conversion:

```go
Amount decentdb.Decimal `bun:",notnull,type:NUMERIC"`
```

## Native SQL contracts

DecentDB accepts Bun's `BIGSERIAL` declaration for a single integer primary key
and uses its durable native ID assignment. PostgreSQL sequence objects are a
separate API. `DEFAULT` resolves the column default, including automatic IDs.

JSON/JSONB columns use validated JSON text and the native JSON functions; JSONB
does not introduce PostgreSQL binary JSON storage. Bun's BYTEA hex literals
decode to native BLOB values. NUMERIC retains DecentDB's signed 64-bit coefficient
and scale limits; decimal averages use at least six fractional digits and round
ties away from zero. An explicit FLOAT64 operand promotes a numeric aggregate
to FLOAT64; `TOTAL` always returns FLOAT64.

This example validates the APIs listed above. PostgreSQL dialect selection does
not make DecentDB a PostgreSQL server: APIs that require PostgreSQL catalogs or
SQL features outside DecentDB's [SQL reference](../../../../docs/user-guide/sql-reference.md)
still follow the native engine's supported SQL surface. The migration example
uses Bun's registered migration callbacks.
