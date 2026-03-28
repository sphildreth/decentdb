/// Minimal desktop example for DecentDB.
///
/// Setup:
/// 1. Build the native library with `cargo build -p decentdb`.
/// 2. Bundle `libdecentdb.so`, `libdecentdb.dylib`, or `decentdb.dll` with your app.
/// 3. Point `Database.open(..., libraryPath: ...)` at the bundled shared library.
import 'dart:io';

import 'package:decentdb/decentdb.dart';

void main() {
  final libPath = _findNativeLib();
  final db = Database.open(':memory:', libraryPath: libPath);

  db.execute(
    'CREATE TABLE tasks (id INT64 PRIMARY KEY, title TEXT, done BOOL)',
  );
  db.execute("INSERT INTO tasks VALUES (1, 'Build Gridlock UI', false)");
  db.execute("INSERT INTO tasks VALUES (2, 'Integrate DecentDB', true)");
  db.execute("INSERT INTO tasks VALUES (3, 'Ship v1.0', false)");

  final page = db
      .prepare('SELECT id, title, done FROM tasks ORDER BY id')
      .nextPage(10);
  print('Tasks:');
  for (final row in page.rows) {
    final done = row['done'] == true ? '✓' : '○';
    print("  $done ${row['title']}");
  }

  db.close();
}

String _findNativeLib() {
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
