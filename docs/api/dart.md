# Dart binding

DecentDB ships two Dart-facing layers:

- `bindings/dart/dart/` — the packaged `decentdb` Dart API
- `tests/bindings/dart/` — the smoke-validation package used in CI and repository validation

## Native library requirement

Build the shared library from the repository root:

```bash
cargo build -p decentdb
```

The Dart package loads the built shared library from `DECENTDB_NATIVE_LIB` or an explicit `libraryPath`:

- Linux: `target/debug/libdecentdb.so`
- macOS: `target/debug/libdecentdb.dylib`
- Windows: `target/debug/decentdb.dll`

## Quick start

```dart
import 'package:decentdb/decentdb.dart';

void main() {
  final db = Database.open(
    'app.ddb',
    libraryPath: '/absolute/path/to/libdecentdb.so',
  );

  db.execute('CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)');

  final insert = db.prepare(r'INSERT INTO users VALUES ($1, $2)');
  insert.bindAll([1, 'Ada']);
  insert.execute();
  insert.dispose();

  final rows = db.query('SELECT id, name FROM users ORDER BY id');
  print(rows.single['name']);

  db.close();
}
```

## Open modes and lifecycle

The Dart wrapper now exposes distinct open modes backed by the stable C ABI:

- `Database.open(path, ...)` — open-or-create
- `Database.create(path, ...)` — create-only
- `Database.openExisting(path, ...)` — open-only
- `Database.memory(...)`
- `Database.close()`
- `Database.inTransaction`

The package also installs a Dart `Finalizer` so a leaked `Database` handle is still released if the object is garbage-collected, but callers should still close explicitly.

## Statement API

`Statement` is now backed by native prepared statements (`ddb_stmt_t`), not by re-sending the SQL text for every execution.

Available operations include:

- `bindNull`, `bindInt64`, `bindBool`, `bindFloat64`, `bindText`, `bindBlob`, `bindDecimal`, `bindDateTime`
- `bindAll([...])`
- `reset()` and `clearBindings()`
- `execute()`
- `query()`
- `step()` / `readRow()`
- `nextPage(pageSize)`
- `dispose()`

Supported high-level bind values in `bindAll(...)`:

- `null`
- `int`
- `bool`
- `double`
- `String`
- `Uint8List`
- `DateTime`
- `DecimalValue`

Rows use an O(1) column-name index map, so `row['column_name']` no longer performs a linear scan.

## Schema helpers

The packaged wrapper exposes:

- `db.schema.listTables()` / `listTablesInfo()`
- `db.schema.describeTable(name)` / `getTableColumns(name)`
- `db.schema.getTableDdl(name)`
- `db.schema.listIndexes()`
- `db.schema.listViews()` / `listViewsInfo()`
- `db.schema.getViewDdl(name)`
- `db.schema.listTriggers()`

## Validation commands

Package suite:

```bash
bindings/dart/scripts/run_tests.sh
```

Manual package validation:

```bash
cargo build -p decentdb
cd bindings/dart/dart
dart analyze lib/ test/ benchmarks/
DECENTDB_NATIVE_LIB=../../../target/debug/libdecentdb.so dart test --reporter expanded
```

Smoke path:

```bash
cargo build -p decentdb
cd tests/bindings/dart
dart pub get
dart run smoke.dart
```

Console example:

```bash
cd bindings/dart/examples/console
dart pub get
DECENTDB_NATIVE_LIB=../../../../target/debug/libdecentdb.so dart run main.dart
```

Benchmark:

```bash
cd bindings/dart/dart
dart pub get
DECENTDB_NATIVE_LIB=../../../target/debug/libdecentdb.so dart run benchmarks/bench_fetch.dart --count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix dart_bench_fetch
```

## Current limitations

- the stable C ABI still does not expose open-with-config, so non-empty `options` strings are rejected
- batch execution APIs (`ddb_stmt_execute_batch_*`) are not wrapped yet
- fused bind+step, row-view, fetch-row-view, and re-execute fast paths are not wrapped yet
- `ddb_evict_shared_wal` is not exposed yet
- the example under `bindings/dart/examples/flutter_desktop/` is still a desktop-oriented reference rather than a real Flutter SDK app
- DecentDB remains a one-writer / many-readers engine; keep that concurrency model in mind when sharing database handles across isolates or threads
