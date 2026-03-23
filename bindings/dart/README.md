# DecentDB Dart Binding

The in-tree Dart package wraps the stable Rust `ddb_*` C ABI with a small,
idiomatic Dart API for desktop and CLI applications.

## What is covered today

- `Database.open()` / `Database.memory()` / `Database.close()`
- One-shot `execute()`, `executeWithParams()`, and `query()`
- A Dart-side `Statement` convenience wrapper for parameter binding, reuse, and
  paging
- Transaction helpers: `begin()`, `commit()`, `rollback()`, `transaction()`
- Maintenance helpers: `checkpoint()` and `saveAs()`
- Schema metadata via `Schema.listTables()`, `describeTable()`, `listIndexes()`,
  `listViews()`, `getTableDdl()`, `getViewDdl()`, and `listTriggers()`

The high-level `Statement` API is implemented in Dart on top of the native
`ddb_db_execute` result handle API. It does not depend on a separate native
prepared-statement ABI.

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

## Flutter desktop notes

Bundle the Rust shared library with your application and pass the resolved path
into `Database.open(..., libraryPath: ...)`. See
`bindings/dart/examples/flutter_desktop/main.dart` for a minimal reference.

## Current limitations

- `Database.open(options: ...)` is not exposed by the current stable `ddb_*`
  ABI, so the Dart wrapper rejects non-empty `options` values.
- The package uses the stable C ABI from `include/decentdb.h`; the reference
  header under `bindings/dart/native/decentdb.h` simply includes that file so
  the two surfaces stay in sync.
