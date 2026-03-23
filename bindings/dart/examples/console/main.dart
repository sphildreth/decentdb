/// DecentDB Dart console example.
///
/// Demonstrates: open, create table, insert with prepared statements,
/// cursor paging, schema introspection.
///
/// Run:
///   cd bindings/dart/dart
///   DECENTDB_NATIVE_LIB=../../../build/libc_api.so dart run ../examples/console/main.dart
import 'dart:io';

// Adjust import path for running outside the package
import 'package:decentdb/decentdb.dart';

String findNativeLib() {
  final env = Platform.environment['DECENTDB_NATIVE_LIB'];
  if (env != null && env.isNotEmpty) return env;
  var dir = Directory.current;
  for (var i = 0; i < 10; i++) {
    for (final name in ['libc_api.so', 'libc_api.dylib', 'c_api.dll']) {
      final f = File('${dir.path}/build/$name');
      if (f.existsSync()) return f.path;
    }
    dir = dir.parent;
  }
  throw StateError('Cannot find native library. Set DECENTDB_NATIVE_LIB.');
}

void main() {
  final libPath = findNativeLib();
  print('Using native library: $libPath');

  // Open in-memory database
  final db = Database.open(':memory:', libraryPath: libPath);
  print('Engine version: ${db.engineVersion}');

  // --- Create schema ---
  db.execute('''
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE,
      score FLOAT,
      active BOOLEAN
    )
  ''');
  db.execute('CREATE INDEX idx_users_name ON users (name)');
  print('\nCreated table "users" with index "idx_users_name".');

  // --- Insert rows with prepared statement ---
  final insertStmt =
      db.prepare(r'INSERT INTO users VALUES ($1, $2, $3, $4, $5)');

  final users = [
    [1, 'Alice', 'alice@example.com', 95.5, true],
    [2, 'Bob', 'bob@example.com', 87.3, true],
    [3, 'Charlie', 'charlie@example.com', 72.1, false],
    [4, 'Diana', 'diana@example.com', 91.8, true],
    [5, 'Eve', 'eve@example.com', 68.4, false],
    [6, 'Frank', 'frank@example.com', 83.0, true],
    [7, 'Grace', 'grace@example.com', 96.2, true],
    [8, 'Hank', 'hank@example.com', 74.5, false],
    [9, 'Ivy', 'ivy@example.com', 89.1, true],
    [10, 'Jack', 'jack@example.com', 77.7, true],
  ];

  db.transaction(() {
    for (final user in users) {
      insertStmt.reset();
      insertStmt.clearBindings();
      insertStmt.bindAll(user);
      insertStmt.execute();
    }
  });
  insertStmt.dispose();
  print('Inserted ${users.length} users.');

  // --- Query with cursor paging ---
  print('\n--- Paged query results (page size = 3) ---');
  final selectStmt = db.prepare('SELECT id, name, score FROM users ORDER BY score DESC');
  var pageNum = 0;

  while (true) {
    final page = selectStmt.nextPage(3);
    pageNum++;
    print('\nPage $pageNum (${page.rows.length} rows):');
    for (final row in page.rows) {
      print('  id=${row["id"]}, name=${row["name"]}, score=${row["score"]}');
    }
    if (page.isLast) break;
  }
  selectStmt.dispose();

  // --- Schema introspection ---
  print('\n--- Schema introspection ---');

  print('\nTables: ${db.schema.listTables()}');

  print('\nColumns of "users":');
  for (final col in db.schema.getTableColumns('users')) {
    print('  $col');
  }

  print('\nIndexes:');
  for (final idx in db.schema.listIndexes()) {
    print('  $idx');
  }

  // --- Create a view and inspect ---
  db.execute('CREATE VIEW active_users AS SELECT id, name, score FROM users WHERE active = true');
  print('\nViews: ${db.schema.listViews()}');

  final ddl = db.schema.getViewDdl('active_users');
  print('View DDL: $ddl');

  // --- EXPLAIN ---
  print('\n--- Query plan ---');
  final planRows = db.query('EXPLAIN SELECT * FROM users WHERE id = 1');
  for (final row in planRows) {
    print('  ${row["query_plan"]}');
  }

  db.close();
  print('\nDone.');
}
