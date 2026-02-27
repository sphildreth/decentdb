# DecentDB Dart/Flutter Binding

Dart FFI bindings for [DecentDB](../../README.md) — an embedded ACID database engine.

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

## Building the Native Library

The Dart package requires the DecentDB shared library (`libc_api.so` / `libc_api.dylib` / `c_api.dll`).

### Prerequisites

- [Nim](https://nim-lang.org/) >= 1.6.0
- C compiler (gcc/clang/MSVC)

### Linux

```bash
cd <repo-root>
nimble build_lib
# Output: build/libc_api.so
```

### macOS

```bash
cd <repo-root>
nimble build_lib
# Output: build/libc_api.dylib
```

### Windows

```bash
cd <repo-root>
nimble build_lib
# Output: build/c_api.dll
```

Or use the convenience script:

```bash
bindings/dart/scripts/build_native.sh
```

## Package Structure

```
bindings/dart/
├── native/
│   └── decentdb.h           # C ABI header (reference)
├── dart/
│   ├── lib/
│   │   ├── decentdb.dart     # Library barrel export
│   │   └── src/
│   │       ├── database.dart       # High-level Database class
│   │       ├── statement.dart      # Prepared statements + cursor
│   │       ├── schema.dart         # Schema introspection
│   │       ├── types.dart          # Type definitions
│   │       ├── errors.dart         # Error types
│   │       └── native_bindings.dart # Low-level FFI bindings
│   ├── test/
│   │   └── decentdb_test.dart
│   └── pubspec.yaml
├── examples/
│   ├── console/main.dart
│   └── flutter_desktop/main.dart
├── scripts/
│   ├── build_native.sh
│   └── run_tests.sh
└── README.md
```

## API Reference

### Database

```dart
// Open / create
final db = Database.open('path.ddb', libraryPath: 'libc_api.so');
final memDb = Database.memory(libraryPath: 'libc_api.so');

// Execute DDL/DML (returns affected rows)
db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
int affected = db.execute("INSERT INTO t VALUES (1, 'Alice')");

// Execute with parameters
db.executeWithParams(r'UPDATE t SET name = $1 WHERE id = $2', ['Bob', 1]);

// Query (returns all rows)
List<Row> rows = db.query('SELECT * FROM t');
List<Row> rows = db.query(r'SELECT * FROM t WHERE id = $1', [1]);

// Transactions
db.begin();
db.execute("INSERT INTO t VALUES (2, 'Charlie')");
db.commit();
// or: db.rollback();

// Transaction helper (auto commit/rollback)
db.transaction(() {
  db.execute("INSERT INTO t VALUES (3, 'Diana')");
  db.execute("INSERT INTO t VALUES (4, 'Eve')");
});

// Maintenance
db.checkpoint();
db.saveAs('backup.ddb');

// Close
db.close();
```

### Prepared Statements

```dart
final stmt = db.prepare(r'INSERT INTO t VALUES ($1, $2)');

// Typed binding (1-based index)
stmt.bindInt64(1, 42);
stmt.bindText(2, 'hello');
stmt.execute();

// Reuse
stmt.reset();
stmt.clearBindings();
stmt.bindAll([43, 'world']);
stmt.execute();

// Query with cursor paging
final select = db.prepare('SELECT * FROM t ORDER BY id');
while (true) {
  final page = select.nextPage(100); // 100 rows per page
  for (final row in page.rows) {
    print(row['name']);
  }
  if (page.isLast) break;
}

// Always dispose when done
stmt.dispose();
select.dispose();
```

### Supported Bind Types

| Dart Type    | Bind Method      | DecentDB Type |
|-------------|-----------------|---------------|
| `null`      | `bindNull()`    | NULL          |
| `int`       | `bindInt64()`   | INTEGER       |
| `bool`      | `bindBool()`    | BOOLEAN       |
| `double`    | `bindFloat64()` | FLOAT         |
| `String`    | `bindText()`    | TEXT          |
| `Uint8List` | `bindBlob()`    | BLOB          |
| `DateTime`  | `bindDateTime()`| TIMESTAMP     |
| decimal     | `bindDecimal()` | DECIMAL       |

### Schema Introspection

```dart
// Tables
List<String> tables = db.schema.listTables();
List<ColumnInfo> cols = db.schema.getTableColumns('users');

// Indexes
List<IndexInfo> indexes = db.schema.listIndexes();

// Views
List<String> views = db.schema.listViews();
String? ddl = db.schema.getViewDdl('my_view');

// Column metadata
for (final col in cols) {
  print('${col.name} ${col.type} notNull=${col.notNull} pk=${col.primaryKey}');
  if (col.refTable != null) {
    print('  FK -> ${col.refTable}.${col.refColumn}');
  }
}
```

### EXPLAIN

```dart
final rows = db.query('EXPLAIN SELECT * FROM users WHERE id = 1');
for (final row in rows) {
  print(row['query_plan']);
}
```

### Error Handling

```dart
try {
  db.execute('INVALID SQL');
} on DecentDbException catch (e) {
  print('Error ${e.code}: ${e.message}');
}
```

## Flutter Desktop Integration

### 1. Add the Dart package

In your Flutter app's `pubspec.yaml`:

```yaml
dependencies:
  decentdb:
    path: <path-to-repo>/bindings/dart/dart
```

### 2. Bundle the native library

#### Linux

Copy `libc_api.so` to `linux/libs/` and update `linux/CMakeLists.txt`:

```cmake
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/libs/libc_api.so"
        DESTINATION "${INSTALL_BUNDLE_LIB_DIR}"
        COMPONENT Runtime)
```

#### macOS

Copy `libc_api.dylib` to `macos/libs/` and add to your Xcode project's
"Copy Bundle Resources" build phase. Set `@rpath` appropriately.

#### Windows

Copy `c_api.dll` to `windows/libs/` and update `windows/CMakeLists.txt`:

```cmake
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/libs/c_api.dll"
        DESTINATION "${INSTALL_BUNDLE_LIB_DIR}"
        COMPONENT Runtime)
```

### 3. Load the library

```dart
import 'dart:io' show Platform;
import 'package:path/path.dart' as p;

String resolveLibPath() {
  // In bundled Flutter apps, the library is next to the executable
  final exeDir = p.dirname(Platform.resolvedExecutable);
  if (Platform.isLinux) return p.join(exeDir, 'lib', 'libc_api.so');
  if (Platform.isMacOS) return p.join(exeDir, '..', 'Frameworks', 'libc_api.dylib');
  if (Platform.isWindows) return p.join(exeDir, 'c_api.dll');
  throw UnsupportedError('Unsupported platform');
}

final db = Database.open('data.ddb', libraryPath: resolveLibPath());
```

## Threading & Isolates

DecentDB uses a **single-writer, multiple-reader** concurrency model:

- **One writer at a time**: All write operations (INSERT/UPDATE/DELETE/DDL/BEGIN/COMMIT/ROLLBACK) must be serialized. Open one `Database` handle for writes.
- **Concurrent readers**: SELECT queries via separate `Statement` handles can run concurrently with the writer.
- **Statement handles are NOT thread-safe**: Each `Statement` must be used from one isolate only.

### Recommended Flutter Pattern

```dart
// Dedicated database isolate
void _dbWorker(SendPort sendPort) {
  final db = Database.open('app.ddb', libraryPath: libPath);
  final port = ReceivePort();
  sendPort.send(port.sendPort);

  port.listen((message) {
    // Process SQL commands, send results back
    try {
      final result = db.query(message['sql'], message['params'] ?? []);
      message['replyPort'].send({'rows': result.map((r) => r.values).toList()});
    } on DecentDbException catch (e) {
      message['replyPort'].send({'error': e.toString()});
    }
  });
}
```

### Cancellation

DecentDB does not currently support mid-query interruption. To cancel a
long-running query:

1. Call `stmt.dispose()` from a different isolate (this finalizes the native
   statement handle and releases the read transaction).
2. The original isolate's next `step()` call will throw.

This is best-effort; the engine may complete the current page of work before
the cancellation takes effect.

## ABI Versioning

The Dart package checks the native library's ABI version at load time via
`decentdb_abi_version()`. If the version doesn't match, the package throws
immediately with a clear error message.

**Current ABI version: 1**

The ABI version is bumped when:
- Function signatures change
- Struct layouts change
- Semantic contracts change (e.g., error code meanings)

The ABI version is NOT bumped for:
- Adding new functions (backward compatible)
- Bug fixes in existing functions

## Running Tests

```bash
# Build native library first
nimble build_lib

# Run Dart tests
bindings/dart/scripts/run_tests.sh

# Or manually:
cd bindings/dart/dart
DECENTDB_NATIVE_LIB=../../../build/libc_api.so dart pub get
DECENTDB_NATIVE_LIB=../../../build/libc_api.so dart test
```

## License

Apache 2.0 — same as DecentDB. See [LICENSE](../../LICENSE).
