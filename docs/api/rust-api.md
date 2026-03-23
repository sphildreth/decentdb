# Rust API Reference

The native Rust API lives in the `decentdb` crate.

## Opening a database

```rust
use decentdb::{Db, DbConfig};

let db = Db::create("app.ddb", DbConfig::default())?;
let reopened = Db::open("app.ddb", DbConfig::default())?;
let auto = Db::open_or_create("auto.ddb", DbConfig::default())?;
# Ok::<(), decentdb::DbError>(())
```

Use `:memory:` for the in-memory VFS:

```rust
use decentdb::{Db, DbConfig};

let db = Db::open(":memory:", DbConfig::default())?;
db.execute("CREATE TABLE cache (id INT64 PRIMARY KEY, value TEXT)")?;
# Ok::<(), decentdb::DbError>(())
```

## Executing SQL

```rust
use decentdb::{Db, DbConfig, Value};

let db = Db::open(":memory:", DbConfig::default())?;
db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")?;
db.execute_with_params(
    "INSERT INTO users (id, name) VALUES ($1, $2)",
    &[Value::Int64(1), Value::Text("Ada".to_string())],
)?;

let result = db.execute("SELECT id, name FROM users ORDER BY id")?;
assert_eq!(result.columns(), &["id", "name"]);
assert_eq!(result.rows().len(), 1);
# Ok::<(), decentdb::DbError>(())
```

`Db::execute_batch` and `Db::execute_batch_with_params` accept semicolon-delimited batches.

## Explicit transactions

The Rust engine now supports explicit handle-local SQL transactions:

```rust
use decentdb::{Db, DbConfig};

let db = Db::open(":memory:", DbConfig::default())?;
db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")?;

db.begin_transaction()?;
db.execute("INSERT INTO items (id, name) VALUES (1, 'Ada')")?;
db.commit_transaction()?;
# Ok::<(), decentdb::DbError>(())
```

Available helpers:
- `Db::begin_transaction()`
- `Db::commit_transaction()`
- `Db::rollback_transaction()`
- `Db::in_transaction()`

## Metadata and maintenance

The crate exposes structured inspection helpers for the CLI and higher-level bindings:

- `Db::storage_info()`
- `Db::header_info()`
- `Db::list_tables()`
- `Db::describe_table(name)`
- `Db::list_indexes()`
- `Db::list_views()`
- `Db::list_triggers()`
- `Db::verify_index(name)`
- `Db::dump_sql()`

Maintenance helpers:

- `Db::checkpoint()`
- `Db::bulk_load_rows(...)`
- `Db::rebuild_index(name)`
- `Db::rebuild_indexes()`
- `Db::save_as(path)`
- `decentdb::evict_shared_wal(path)`

## Results

`Db::execute*` returns `QueryResult`.

```rust
use decentdb::{Db, DbConfig, Value};

let db = Db::open(":memory:", DbConfig::default())?;
db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")?;
db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")?;

let result = db.execute("SELECT id, name FROM users")?;
assert_eq!(result.rows()[0].values(), &[
    Value::Int64(1),
    Value::Text("Ada".to_string()),
]);
# Ok::<(), decentdb::DbError>(())
```

For `EXPLAIN`, `QueryResult::explain_lines()` returns the rendered plan rows.

## Error handling

All fallible operations return `decentdb::Result<T>`, which aliases `std::result::Result<T, DbError>`.

`DbError` exposes stable numeric categories through `DbError::numeric_code()` for:

- I/O failures
- corruption
- constraint violations
- transaction errors
- SQL errors
- internal engine errors
- panic boundaries
