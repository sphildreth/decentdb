# Dart/Flutter Bindings

DecentDB provides Dart FFI bindings for Flutter desktop apps under `bindings/dart/`.

## Build the native library

```bash
cargo build_lib
```

On Linux this produces `build/libc_api.so`, on macOS `build/libc_api.dylib`, on Windows `build/c_api.dll`.

## Installation

Add the `decentdb` package to your `pubspec.yaml`:

```yaml
dependencies:
  decentdb:
    path: <path-to-repo>/bindings/dart/dart
```

## Quick Start

```dart
import 'package:decentdb/decentdb.dart';

void main() {
  final db = Database.open('mydata.ddb', libraryPath: 'path/to/libc_api.so');

  db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
  db.execute("INSERT INTO users VALUES (1, 'Alice')");

  final rows = db.query('SELECT * FROM users');
  for (final row in rows) {
    print('${row["id"]}: ${row["name"]}');
  }

  db.close();
}
```

## Prepared Statements

```dart
final stmt = db.prepare(r'INSERT INTO users VALUES ($1, $2)');
stmt.bindInt64(1, 42);
stmt.bindText(2, 'Bob');
stmt.execute();

// Reuse the statement
stmt.reset();
stmt.clearBindings();
stmt.bindAll([43, 'Charlie']);
stmt.execute();
stmt.dispose();
```

## Cursor Paging

Stream large results page by page without loading everything into memory:

```dart
final stmt = db.prepare('SELECT * FROM large_table ORDER BY id');
while (true) {
  final page = stmt.nextPage(100);  // 100 rows per page
  for (final row in page.rows) {
    print(row['name']);
  }
  if (page.isLast) break;
}
stmt.dispose();
```

## Transactions

```dart
// Manual
db.begin();
db.execute("INSERT INTO users VALUES (1, 'Alice')");
db.commit();

// Automatic (rolls back on exception)
db.transaction(() {
  db.execute("INSERT INTO users VALUES (2, 'Bob')");
  db.execute("INSERT INTO users VALUES (3, 'Charlie')");
});
```

## Schema Introspection

```dart
// Tables
List<String> tables = db.schema.listTables();
List<TableInfo> tableInfos = db.schema.listTablesInfo();
String? tableDdl = db.schema.getTableDdl('users');

// Column metadata
List<ColumnInfo> cols = db.schema.getTableColumns('users');
for (final col in cols) {
  print('${col.name} ${col.type} notNull=${col.notNull} pk=${col.primaryKey}');
  print('  default=${col.defaultExpr} generated=${col.generatedExpr}');
  if (col.refTable != null) {
    print('  FK -> ${col.refTable}.${col.refColumn}');
  }
}

for (final table in tableInfos) {
  print('${table.name} temp=${table.temporary}');
  print(table.ddl);
  for (final check in table.checks) {
    print('  CHECK ${check.name}: ${check.exprSql}');
  }
}

// Indexes
List<IndexInfo> indexes = db.schema.listIndexes();

// Views
List<String> views = db.schema.listViews();
List<ViewInfo> viewInfos = db.schema.listViewsInfo();
String? viewSql = db.schema.getViewDdl('my_view'); // canonical SELECT body

// Triggers
List<TriggerInfo> triggers = db.schema.listTriggers();
for (final trigger in triggers) {
  print('${trigger.name} ${trigger.timing} ${trigger.events.join("|")} '
      'on ${trigger.targetKind} ${trigger.targetName}');
  print(trigger.ddl);
}
```

`ColumnInfo` includes default expressions, generated-column metadata, and FK action details. `TableInfo`, `ViewInfo`, and `TriggerInfo` expose canonical DDL plus temporary-object metadata.

## Supported Types

| Dart Type    | Bind Method        | DecentDB Type |
|-------------|-------------------|---------------|
| `null`      | `bindNull()`      | NULL          |
| `int`       | `bindInt64()`     | INTEGER       |
| `bool`      | `bindBool()`      | BOOLEAN       |
| `double`    | `bindFloat64()`   | FLOAT         |
| `String`    | `bindText()`      | TEXT          |
| `Uint8List` | `bindBlob()`      | BLOB          |
| `DateTime`  | `bindDateTime()`  | TIMESTAMP     |
| decimal     | `bindDecimal()`   | DECIMAL       |

## EXPLAIN

```dart
final rows = db.query('EXPLAIN SELECT * FROM users WHERE id = 1');
for (final row in rows) {
  print(row['query_plan']);
}
```

## Error Handling

All errors are thrown as `DecentDbException` with a structured error code and message:

```dart
try {
  db.execute('INVALID SQL');
} on DecentDbException catch (e) {
  print('Error ${e.code}: ${e.message}');
  // e.code is an ErrorCode enum: io, corruption, constraint, transaction, sql, internal
}
```

## Flutter Desktop Integration

### Bundling the native library

**Linux** — copy `libc_api.so` to `linux/libs/` and add to `CMakeLists.txt`:

```cmake
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/libs/libc_api.so"
        DESTINATION "${INSTALL_BUNDLE_LIB_DIR}"
        COMPONENT Runtime)
```

**macOS** — copy `libc_api.dylib` to `macos/libs/` and add to Xcode "Copy Bundle Resources".

**Windows** — copy `c_api.dll` to `windows/libs/` and add to `CMakeLists.txt`:

```cmake
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/libs/c_api.dll"
        DESTINATION "${INSTALL_BUNDLE_LIB_DIR}"
        COMPONENT Runtime)
```

### Resolving the library path at runtime

```dart
import 'dart:io' show Platform;
import 'package:path/path.dart' as p;

String resolveLibPath() {
  final exeDir = p.dirname(Platform.resolvedExecutable);
  if (Platform.isLinux) return p.join(exeDir, 'lib', 'libc_api.so');
  if (Platform.isMacOS) return p.join(exeDir, '..', 'Frameworks', 'libc_api.dylib');
  if (Platform.isWindows) return p.join(exeDir, 'c_api.dll');
  throw UnsupportedError('Unsupported platform');
}
```

## Threading & Isolates

DecentDB uses a **single-writer, multiple-reader** model:

- All write operations must be serialized on one isolate
- Read queries (SELECT) can run concurrently via separate statement handles
- Each `Statement` handle must be used from one isolate only

For Flutter apps, run database operations in a dedicated isolate:

```dart
void dbWorker(SendPort sendPort) {
  final db = Database.open('app.ddb', libraryPath: libPath);
  final port = ReceivePort();
  sendPort.send(port.sendPort);

  port.listen((message) {
    try {
      final result = db.query(message['sql'], message['params'] ?? []);
      message['replyPort'].send({'rows': result.map((r) => r.values).toList()});
    } on DecentDbException catch (e) {
      message['replyPort'].send({'error': e.toString()});
    }
  });
}
```

## Running Tests

```bash
cargo build_lib
bindings/dart/scripts/run_tests.sh
```

## Source Code

The binding source is at [`bindings/dart/`](https://github.com/nicholasgasior/decentdb/tree/main/bindings/dart). See the [README](https://github.com/nicholasgasior/decentdb/blob/main/bindings/dart/README.md) for full details.
