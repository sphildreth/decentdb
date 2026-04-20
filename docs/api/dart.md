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

For Flutter/Dart desktop packaging, GitHub Releases also publish small
platform-native archives that contain just the FFI library:

- `decentdb-dart-native-<tag>-Linux-x64.tar.gz`
- `decentdb-dart-native-<tag>-Linux-arm64.tar.gz`
- `decentdb-dart-native-<tag>-macOS-arm64.tar.gz`
- `decentdb-dart-native-<tag>-Windows-x64.zip`

Each archive extracts to the platform-native library file
(`libdecentdb.so`, `libdecentdb.dylib`, or `decentdb.dll`) so desktop apps can
bundle it directly.

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

### Streaming and pagination

`step()` and `nextPage()` stream from native row-view buffers without materializing the full result set in Dart:

```dart
final stmt = db.prepare('SELECT id, name FROM users ORDER BY id');

while (stmt.step()) {
  final row = stmt.readRow();
  print(row['name']);
}

stmt.dispose();
```

```dart
final stmt = db.prepare('SELECT id, name FROM users ORDER BY id');

while (true) {
  final page = stmt.nextPage(128);
  for (final row in page.rows) {
    print(row['name']);
  }
  if (page.isLast) break;
}

stmt.dispose();
```

`query()` still returns all rows but internally chunks at 256 rows via the streaming path. `nextPage()` invalidates any row from a prior `step()` call, and vice versa. Binding, resetting, or clearing bindings also invalidates streaming state.

### Batch execution

Batch helpers execute many rows in a single FFI call, which is significantly faster than per-row bind/execute loops:

```dart
final stmt = db.prepare(r'INSERT INTO users VALUES ($1, $2)');
db.transaction(() {
  stmt.executeBatchTyped('it', [
    [1, 'Alice'],
    [2, 'Bob'],
    [3, 'Charlie'],
  ]);
});
stmt.dispose();
```

Available batch methods:

- `executeBatchInt64(List<int> values)` — one-column INT64 batch
- `executeBatchI64TextF64(List<(int, String, double)> rows)` — `(INT64, TEXT, FLOAT64)` triple batch
- `executeBatchTyped(String signature, List<List<Object?>> rows)` — mixed-type batch using an `i`/`t`/`f` signature string

### Re-execute helpers

Re-execute helpers combine reset, bind, and execute into a single FFI call for hot DML loops:

```dart
final stmt = db.prepare(r'UPDATE counters SET val = $1 WHERE id = 1');
stmt.rebindInt64Execute(42);
stmt.dispose();
```

Available re-execute methods:

- `rebindInt64Execute(int value)` — reset, bind INT64 at position 1, execute
- `rebindTextInt64Execute(String text, int value)` — reset, bind `(TEXT, INT64)`, execute
- `rebindInt64TextExecute(int value, String text)` — reset, bind `(INT64, TEXT)`, execute

### Fused bind+step helpers

For extremely hot query paths, fused helpers combine binding and stepping into a single FFI boundary crossing:

```dart
final stmt = db.prepare('SELECT id, name, score FROM t WHERE id = $1');

// Single-row lookup returning a primitive tuple
final result = stmt.bindInt64StepI64TextF64(1, 42); 
if (result != null) {
  print('Name: ${result.$2}');
}

stmt.dispose();
```

Available fused methods:

- `bindInt64Step(int index, int value)` — bind INT64 and stream one row view (returns `true` if row available, use `readRow()`)
- `bindInt64StepI64TextF64(int index, int value)` — bind INT64 and return a strongly-typed `(int, String, double)?` tuple directly

## Schema helpers

The packaged wrapper exposes:

- `db.schema.listTables()` / `listTablesInfo()`
- `db.schema.describeTable(name)` / `getTableColumns(name)`
- `db.schema.getTableDdl(name)`
- `db.schema.listIndexes()`
- `db.schema.listViews()` / `listViewsInfo()`
- `db.schema.getViewDdl(name)`
- `db.schema.listTriggers()`

### Rich schema snapshot

`getSchemaSnapshot()` returns the complete schema in one call with rich typed metadata:

```dart
final snapshot = db.schema.getSchemaSnapshot();
print('v${snapshot.snapshotVersion}, cookie=${snapshot.schemaCookie}');

for (final table in snapshot.tables) {
  print('Table ${table.name} (temp=${table.temporary}, rows=${table.rowCount})');
  print('  DDL: ${table.ddl}');

  for (final fk in table.foreignKeys) {
    print('  FK: ${fk.columns} -> ${fk.referencedTable}(${fk.referencedColumns})');
  }

  for (final column in table.columns) {
    if (column.generatedSql != null) {
      print('  Generated: ${column.name} = ${column.generatedSql} (${column.generatedStored ? "STORED" : "VIRTUAL"})');
    }
    for (final check in column.checks) {
      print('  Check: ${check.name ?? "<unnamed>"}: ${check.expressionSql}');
    }
  }
}
```

The snapshot model includes:

- `SchemaSnapshot` — top-level container with `tables`, `views`, `indexes`, `triggers`
- `SchemaTableInfo` — DDL, row count, primary key columns, foreign keys, check constraints, generated columns
- `SchemaViewInfo` — DDL, SQL text, column names, dependencies
- `SchemaIndexInfo` — DDL, kind, uniqueness, partial-index predicate, include columns
- `SchemaTriggerInfo` — DDL, target kind, timing, events, event mask, for-each-row flag
- `SchemaCheckConstraintInfo` — optional name and expression SQL

All collections are deterministically ordered by name.

## WAL maintenance

```dart
Database.evictSharedWal(
  '/path/to/database.ddb',
  libraryPath: '/path/to/libdecentdb.so',
);
```

Evicts the shared WAL cache entry for an on-disk database. Call only after all handles for that path are closed.

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

## Notes
- the stable C ABI still does not expose open-with-config, so non-empty `options` strings are rejected
- the example under `bindings/dart/examples/flutter_desktop/` is still a desktop-oriented reference rather than a real Flutter SDK app
- DecentDB remains a one-writer / many-readers engine; keep that concurrency model in mind when sharing database handles across isolates or threads
