/// DecentDB Dart console example.
///
/// Run from the repository root:
///   cargo build -p decentdb
///   cd bindings/dart/dart
///   DECENTDB_NATIVE_LIB=../../../target/debug/libdecentdb.so dart run ../examples/console/main.dart
import 'dart:io';

import 'package:decentdb/decentdb.dart';

String findNativeLib() {
  final env = Platform.environment['DECENTDB_NATIVE_LIB'];
  if (env != null && env.isNotEmpty) return env;
  var dir = Directory.current;
  for (var i = 0; i < 10; i++) {
    for (final name in [
      'target/debug/libdecentdb.so',
      'target/debug/libdecentdb.dylib',
      'target/debug/decentdb.dll',
    ]) {
      final file = File('${dir.path}/$name');
      if (file.existsSync()) return file.path;
    }
    dir = dir.parent;
  }
  throw StateError('Cannot find native library. Set DECENTDB_NATIVE_LIB.');
}

void main() {
  final libPath = findNativeLib();
  final db = Database.open(':memory:', libraryPath: libPath);

  print('Using native library: $libPath');
  print('Engine version: ${db.engineVersion}');

  db.execute('''
    CREATE TABLE users (
      id INT64 PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE,
      score FLOAT64,
      active BOOL
    )
  ''');
  db.execute('CREATE INDEX idx_users_name ON users (name)');

  final insert = db.prepare(r'INSERT INTO users VALUES ($1, $2, $3, $4, $5)');
  final users = [
    [1, 'Alice', 'alice@example.com', 95.5, true],
    [2, 'Bob', 'bob@example.com', 87.3, true],
    [3, 'Charlie', 'charlie@example.com', 72.1, false],
    [4, 'Diana', 'diana@example.com', 91.8, true],
  ];
  try {
    db.transaction(() {
      for (final user in users) {
        insert.reset();
        insert.clearBindings();
        insert.bindAll(user);
        insert.execute();
      }
    });
  } finally {
    insert.dispose();
  }

  print('\nPaged query results:');
  final select = db.prepare(
    'SELECT id, name, score FROM users ORDER BY score DESC',
  );
  try {
    while (true) {
      final page = select.nextPage(2);
      for (final row in page.rows) {
        print("  id=${row['id']} name=${row['name']} score=${row['score']}");
      }
      if (page.isLast) break;
      print('  -- next page --');
    }
  } finally {
    select.dispose();
  }

  print('\nSchema introspection:');
  final table = db.schema.describeTable('users');
  print('  tables: ${db.schema.listTables()}');
  print("  users DDL: ${db.schema.getTableDdl('users')}");
  for (final column in table.columns) {
    print(
      '  column ${column.name}: ${column.type} nullable=${column.nullable}',
    );
  }

  db.execute(
    'CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = true',
  );
  print('  views: ${db.schema.listViews()}');
  print("  active_users DDL: ${db.schema.getViewDdl('active_users')}");

  db.close();
}
