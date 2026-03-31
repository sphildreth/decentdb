# DecentDB Dart Binding

The in-tree Dart package wraps the stable Rust `ddb_*` C ABI with a small,
idiomatic Dart API for desktop and CLI applications.

## What is covered today

- `Database.open()` / `Database.create()` / `Database.openExisting()` /
  `Database.memory()` / `Database.close()`
- `Database.inTransaction` query helper backed by `ddb_db_in_transaction`
- `Database.evictSharedWal(path)` maintenance helper for shared WAL cache cleanup
- `Database` `Finalizer` – the native handle is released by the GC if `close()`
  is never called
- One-shot `execute()`, `executeWithParams()`, and `query()`
- Native prepared statements (`ddb_stmt_t`) backing every `Statement` object –
  SQL is compiled once and the query plan is reused across executions
- `Statement.step()` and `Statement.nextPage()` stream rows from the native cursor
  without a Dart-side full-result backing store
- Fast paths for high-throughput workloads:
  `executeBatchInt64`, `executeBatchI64TextF64`, `executeBatchTyped`,
  `rebindInt64Execute`, `rebindTextInt64Execute`, `rebindInt64TextExecute`
- Efficient row decoding: a single `DdbValue` allocation is reused for every
  cell in a result set; a shared `Map<String, int>` index is built once per
  result and shared across all rows for O(1) named-column access via `row['col']`
- Typed bind methods call `ddb_stmt_bind_*` directly:
  `bindNull`, `bindInt64`, `bindBool`, `bindFloat64`, `bindText`, `bindBlob`,
  `bindDecimal`, `bindDateTime`
- `Statement.reset()` / `clearBindings()` / `dispose()` map to native
  `ddb_stmt_reset` / `ddb_stmt_clear_bindings` / `ddb_stmt_free`
- Transaction helpers: `begin()`, `commit()`, `rollback()`, `transaction()`
- Maintenance helpers: `checkpoint()` and `saveAs()`
- Schema metadata via `Schema.listTables()`, `describeTable()`, `listIndexes()`,
  `listViews()`, `getTableDdl()`, `getViewDdl()`, and `listTriggers()`
- Rich schema metadata via `Schema.getSchemaSnapshot()` with typed Dart models
  (`SchemaSnapshot`, `SchemaTableInfo`, `SchemaColumnInfo`, `SchemaViewInfo`,
  `SchemaIndexInfo`, `SchemaTriggerInfo`, `SchemaCheckConstraintInfo`)
- `ErrorCode.fromCode` throws `StateError` on unrecognised codes
- `sqlite3` moved to `dev_dependencies` (only used by the benchmark)

## Build the native library

From the repository root:

```bash
cargo build -p decentdb
```

The Rust `cdylib` is emitted to:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

You can also use the helper script:

```bash
bindings/dart/scripts/build_native.sh
```

## Run the Dart package tests

```bash
bindings/dart/scripts/run_tests.sh
```

That script builds the shared library, runs `dart pub get`, and executes the
package suite in `bindings/dart/dart/test/decentdb_test.dart`.

## Run the Dart benchmark

From the repository root:

```bash
cargo build -p decentdb --release
cd bindings/dart/dart
dart pub get
DECENTDB_NATIVE_LIB=../../../target/release/libdecentdb.so dart run benchmarks/bench_fetch.dart --count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix dart_bench_fetch
```

Benchmark CLI options:

- `--engine <all|decentdb|sqlite>`
- `--count <n>`
- `--point-reads <n>`
- `--fetchmany-batch <n>`
- `--point-seed <n>`
- `--db-prefix <prefix>` (DecentDB writes `.ddb`, SQLite writes `.db`)
- `--keep-db`

## Quick start

```dart
import 'package:decentdb/decentdb.dart';

void main() {
  final db = Database.open(
    'mydata.ddb',
    libraryPath: '/absolute/path/to/libdecentdb.so',
  );

  db.execute('CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)');
  db.execute("INSERT INTO users VALUES (1, 'Alice')");

  final rows = db.query('SELECT id, name FROM users ORDER BY id');
  for (final row in rows) {
    print("${row['id']}: ${row['name']}");
  }

  db.close();
}
```

## Parameter binding and paging

```dart
final insert = db.prepare(r'INSERT INTO users VALUES ($1, $2)');
insert.bindAll([2, 'Bob']);
insert.execute();
insert.dispose();

final select = db.prepare('SELECT id, name FROM users ORDER BY id');
while (true) {
  final page = select.nextPage(100);
  for (final row in page.rows) {
    print(row['name']);
  }
  if (page.isLast) break;
}
select.dispose();
```

Supported Dart bind values in the tested wrapper path are:

- `null`
- `int`
- `bool`
- `double`
- `String`
- `Uint8List`
- `DateTime`
- `DecimalValue`

Whether a value can be stored in a specific table column still depends on the
current engine SQL type surface. The examples and tests in this directory stick
to the currently validated SQL types: `INT64`, `FLOAT64`, `BOOL`, and `TEXT`.

## Schema metadata

```dart
final tables = db.schema.listTables();
final users = db.schema.describeTable('users');
final ddl = db.schema.getTableDdl('users');
final indexes = db.schema.listIndexes();
final views = db.schema.listViewsInfo();
final triggers = db.schema.listTriggers();
```

For full metadata fidelity (checks, FKs, generated columns, canonical DDL, temp
objects), use:

```dart
final snapshot = db.schema.getSchemaSnapshot();
for (final table in snapshot.tables) {
  print('${table.name} temp=${table.temporary} rows=${table.rowCount}');
  for (final check in table.checks) {
    print('  check: ${check.name ?? '<unnamed>'} => ${check.expressionSql}');
  }
}
```

## Flutter desktop notes

Bundle the Rust shared library with your application and pass the resolved path
into `Database.open(..., libraryPath: ...)`. See
`bindings/dart/examples/flutter_desktop/main.dart` for a minimal reference.

## Current limitations

- `Database.open(options: ...)` is not exposed by the current stable `ddb_*`
  ABI; the Dart wrapper rejects non-empty `options` values.
- The package uses the stable C ABI from `include/decentdb.h`; the reference
  header under `bindings/dart/native/decentdb.h` includes that file so the two
  surfaces stay in sync.
- Two C ABI fused bind+step helpers are not wrapped yet:
  `ddb_stmt_bind_int64_step_row_view` and
  `ddb_stmt_bind_int64_step_i64_text_f64`.
