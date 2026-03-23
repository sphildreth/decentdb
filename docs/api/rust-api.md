# Rust API Reference

DecentDB's native Rust API lives in the `decentdb` crate.

## Opening a database

```rust
use decentdb::{Db, DbConfig};

let db = Db::create("app.ddb", DbConfig::default())?;
let reopened = Db::open("app.ddb", DbConfig::default())?;
# Ok::<(), decentdb::DbError>(())
```

`Db::create` initializes a new database file. `Db::open` validates the header, opens the pager/WAL, and loads the latest SQL runtime state from the WAL-backed catalog root.

`":memory:"` uses the in-memory VFS:

```rust
use decentdb::{Db, DbConfig};

let mem = Db::open(":memory:", DbConfig::default())?;
mem.execute("CREATE TABLE cache (id INT64 PRIMARY KEY, value TEXT)")?;
# Ok::<(), decentdb::DbError>(())
```

## Executing SQL

### Single statements

```rust
use decentdb::{Db, DbConfig};

let db = Db::open(":memory:", DbConfig::default())?;
db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)")?;
db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")?;

let rows = db.execute("SELECT id, name FROM users ORDER BY id")?;
assert_eq!(rows.columns(), &["id", "name"]);
assert_eq!(rows.rows().len(), 1);
# Ok::<(), decentdb::DbError>(())
```

### Parameters

Use PostgreSQL-style `$1`, `$2`, ... positional parameters:

```rust
use decentdb::{Db, DbConfig, Value};

let db = Db::open(":memory:", DbConfig::default())?;
db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")?;
db.execute_with_params(
    "INSERT INTO users (id, name) VALUES ($1, $2)",
    &[Value::Int64(1), Value::Text("Ada".to_string())],
)?;
# Ok::<(), decentdb::DbError>(())
```

### Multi-statement batches

`Db::execute_batch` and `Db::execute_batch_with_params` run semicolon-delimited statements one at a time, persisting each successful mutating statement before continuing to the next statement in the batch.

## Results

`Db::execute*` returns `QueryResult`.

```rust
use decentdb::{Db, DbConfig, Value};

let db = Db::open(":memory:", DbConfig::default())?;
db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")?;
db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")?;

let result = db.execute("SELECT id, name FROM users")?;
assert_eq!(result.affected_rows(), 1);
assert_eq!(
    result.rows()[0].values(),
    &[Value::Int64(1), Value::Text("Ada".to_string())]
);
# Ok::<(), decentdb::DbError>(())
```

For `EXPLAIN`, `QueryResult::explain_lines()` returns the rendered plan rows.

## Bulk load and maintenance

```rust
use decentdb::{BulkLoadOptions, Db, DbConfig, Value};

let db = Db::open(":memory:", DbConfig::default())?;
db.execute("CREATE TABLE bulk_data (id INT64 PRIMARY KEY, value TEXT)")?;

db.bulk_load_rows(
    "bulk_data",
    &["id", "value"],
    &[
        vec![Value::Int64(1), Value::Text("a".to_string())],
        vec![Value::Int64(2), Value::Text("b".to_string())],
    ],
    BulkLoadOptions::default(),
)?;

db.rebuild_indexes()?;
db.checkpoint()?;
# Ok::<(), decentdb::DbError>(())
```

Available maintenance helpers:

- `Db::checkpoint()`
- `Db::rebuild_index(name)`
- `Db::rebuild_indexes()`
- `Db::schema_cookie()`

## Error handling

All fallible operations return `decentdb::Result<T>`, which is an alias for `std::result::Result<T, DbError>`.

`DbError` carries stable numeric categories through `DbError::numeric_code()` and specific variants for:

- I/O failures
- corruption
- constraint violations
- transaction errors
- SQL errors
- internal engine errors
- panic boundaries
