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

## Branching and time travel

The Rust API exposes the same branch workflow used by the CLI:

- `Db::snapshot_create`, `snapshot_list`, `snapshot_delete`
- `Db::execute_batch_at_snapshot`, `execute_batch_at_snapshot_lsn`
- `Db::branch_create`, `branch_list`, `branch_rename`, `branch_delete`
- `Db::execute_batch_on_branch`
- `Db::branch_commit`, `branch_log`
- `Db::branch_diff`
- `Db::branch_restore`
- `Db::branch_merge`

Report types such as `BranchDiffReport`, `BranchRestoreReport`, and
`BranchMergeReport` are serializable for tooling.

## Reactive subscriptions

The Rust API exposes in-process reactive watches:

- `Db::watch_table(TableWatchOptions)`
- `Db::watch_range(RangeWatchOptions)`
- `Db::watch_query(sql, params, QueryWatchOptions)`
- `Db::change_stream(ChangeStreamOptions)`
- `Db::reactive_metrics()`
- `Db::reactive_subscriptions()`

Query watches deliver an initial `QueryResult`, then invalidation events for
committed changes to dependent tables. Table, range, and change-stream watches
deliver initial snapshot metadata followed by committed events with LSN
boundaries. Watch queues are bounded; a slow consumer receives `WatchEvent::Lagged`
and should resynchronize from a fresh query.

```rust
use std::time::Duration;

use decentdb::{Db, DbConfig, TableWatchOptions, WatchEvent};

let db = Db::open(":memory:", DbConfig::default())?;
db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")?;

let watch = db.watch_table(TableWatchOptions {
    tables: vec!["users".to_string()],
    queue_capacity: None,
})?;

assert!(matches!(
    watch.recv_timeout(Duration::from_secs(1))?,
    Some(WatchEvent::Initial(_))
));

db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")?;

if let Some(WatchEvent::Invalidate(event)) =
    watch.recv_timeout(Duration::from_secs(1))?
{
    assert_eq!(event.tables, vec!["users"]);
}
# Ok::<(), decentdb::DbError>(())
```

## Lua extensions

The Rust API exposes the Lua extension package lifecycle through
`Db::extensions()` and open-time trust through `DbConfig`.

```rust
use decentdb::{
    Db, DbConfig, ExtensionTrustAnchor, ExtensionValidationOptions,
};

let report = decentdb::validate_extension_package(
    "./text_tools",
    ExtensionValidationOptions::unsigned_development(),
)?;
let hash = report.content_hash.expect("validated package hash");

let mut config = DbConfig::default();
config.extension_trust_anchors.push(ExtensionTrustAnchor::new(
    "text_tools",
    hash,
));

let db = Db::open_or_create("app.ddb", config)?;
db.extensions().install_with_options(
    "./text_tools",
    ExtensionValidationOptions::unsigned_development(),
)?;
db.extensions().enable("text_tools")?;

let result = db.execute("SELECT slugify('Hello, DecentDB')")?;
assert_eq!(result.rows().len(), 1);
# Ok::<(), decentdb::DbError>(())
```

Lifecycle helpers include `validate_package`, `install`, `install_with_options`,
`enable`, `disable`, `purge`, `list`, `show`, `dependencies`, and
`rebuild_dependents`.

Use `DbConfig::extension_unsigned_development_mode` only for local package
authoring. Production connections should pass exact `ExtensionTrustAnchor`
entries for the package content hashes they allow to execute.

## Metadata and maintenance

The crate exposes structured inspection helpers for the CLI and higher-level bindings:

- `Db::storage_info()`
- `Db::header_info()`
- `Db::list_tables()`
- `Db::describe_table(name)`
- `Db::list_indexes()`
- `Db::list_views()`
- `Db::list_triggers()`
- `Db::get_schema_snapshot()`
- `Db::get_tooling_metadata()`
- `Db::describe_query_contract(sql)`
- `Db::verify_index(name)`
- `Db::dump_sql()`

`get_tooling_metadata()` returns the versioned schema contract used by external
tooling, including a deterministic schema fingerprint and native type metadata.
`describe_query_contract(sql)` parses and analyzes a statement without executing
it, returning parameter and result-column metadata plus diagnostics for unknown
inference.

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

## Related Native Surfaces

- [C/C++ ABI](c-cpp.md) documents the stable `include/decentdb.h` surface for
  C callers and C++ callers that want to consume DecentDB through C linkage.
