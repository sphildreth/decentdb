/// Minimal Flutter desktop example for DecentDB.
///
/// This is a reference implementation showing how to integrate DecentDB
/// into a Flutter desktop app. Copy and adapt for your project.
///
/// ## Setup
///
/// 1. Build the native library: `nimble build_lib`
/// 2. Place the library in your Flutter app's platform-specific location:
///    - Linux: `linux/libs/libc_api.so`
///    - macOS: `macos/libs/libc_api.dylib`
///    - Windows: `windows/libs/c_api.dll`
/// 3. Add the `decentdb` package to your `pubspec.yaml`
/// 4. Configure your platform runner to bundle the library (see README.md)
///
/// ## Architecture
///
/// For production apps, run database operations in a separate isolate:
///
/// ```dart
/// // In main isolate:
/// final receivePort = ReceivePort();
/// await Isolate.spawn(_dbWorker, receivePort.sendPort);
///
/// // In worker isolate:
/// void _dbWorker(SendPort sendPort) {
///   final db = Database.open('myapp.ddb', libraryPath: libPath);
///   // Process commands from main isolate...
/// }
/// ```
library;

import 'dart:io';

import 'package:decentdb/decentdb.dart';

/// Example: open a database, query, and display results.
///
/// In a real Flutter app, this would be called from a widget's initState
/// or a provider/bloc/cubit.
void main() {
  // In Flutter, you'd resolve the library path relative to the app bundle:
  //   final libPath = path.join(appDir, 'libs', NativeBindings.defaultLibraryName());
  final libPath = _findNativeLib();
  print('Native library: $libPath');

  final db = Database.open(':memory:', libraryPath: libPath);
  print('DecentDB ${db.engineVersion}');

  // Create sample data
  db.execute('CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT, done BOOLEAN)');
  db.execute("INSERT INTO tasks VALUES (1, 'Build Gridlock UI', false)");
  db.execute("INSERT INTO tasks VALUES (2, 'Integrate DecentDB', true)");
  db.execute("INSERT INTO tasks VALUES (3, 'Ship v1.0', false)");

  // Query first page
  final stmt = db.prepare('SELECT id, title, done FROM tasks ORDER BY id');
  final page = stmt.nextPage(10);

  print('\nTasks:');
  for (final row in page.rows) {
    final done = row['done'] == true ? '✓' : '○';
    print('  $done ${row["title"]}');
  }
  print('(${page.rows.length} rows, last page: ${page.isLast})');

  stmt.dispose();
  db.close();
}

String _findNativeLib() {
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
