import 'dart:ffi';
import 'dart:io';

import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

String findNativeLib() {
  final envPath = Platform.environment['DECENTDB_NATIVE_LIB'];
  if (envPath != null && envPath.isNotEmpty) {
    return envPath;
  }

  var dir = Directory.current;
  for (var i = 0; i < 10; i++) {
    for (final candidateName in [
      'target/debug/libdecentdb.so',
      'target/debug/libdecentdb.dylib',
      'target/debug/decentdb.dll',
    ]) {
      final candidate = File('${dir.path}/$candidateName');
      if (candidate.existsSync()) {
        return candidate.path;
      }
    }
    dir = dir.parent;
  }

  throw StateError(
    'Cannot find DecentDB native library. '
    'Set DECENTDB_NATIVE_LIB or run from the repo root after `cargo build -p decentdb`.',
  );
}

typedef _MallocTrimNative = Int32 Function(IntPtr);
typedef _MallocTrimDart = int Function(int);

int rssBytes() => ProcessInfo.currentRss;

bool trimProcessHeap() {
  if (!Platform.isLinux) {
    return false;
  }

  try {
    final libc = DynamicLibrary.open('libc.so.6');
    final mallocTrim =
        libc.lookupFunction<_MallocTrimNative, _MallocTrimDart>('malloc_trim');
    return mallocTrim(0) != 0;
  } on Object {
    return false;
  }
}

void main() {
  late String libPath;

  setUpAll(() {
    libPath = findNativeLib();
  });

  test(
    'repeated open/query/close keeps RSS bounded',
    () {
      final tempDir = Directory.systemTemp.createTempSync(
        'decentdb_dart_memory_leak_',
      );
      final dbPath = '${tempDir.path}/memory_leak.ddb';

      try {
        final seed = Database.open(dbPath, libraryPath: libPath);
        seed.execute('CREATE TABLE leak_test (id INT64 PRIMARY KEY, data TEXT)');
        seed.transaction(() {
          final insert =
              seed.prepare(r'INSERT INTO leak_test VALUES ($1, $2)');
          try {
            for (var i = 0; i < 1000; i++) {
              insert.reset();
              insert.clearBindings();
              insert.bindInt64(1, i);
              insert.bindText(2, 'a' * 1000);
              insert.execute();
            }
          } finally {
            insert.dispose();
          }
        });
        seed.close();

        trimProcessHeap();
        final before = rssBytes();

        for (var i = 0; i < 500; i++) {
          final db = Database.open(dbPath, libraryPath: libPath);
          final row =
              db.query('SELECT COUNT(*) AS cnt FROM leak_test').single;
          expect(row['cnt'], 1000);
          db.close();

          if (i % 50 == 0) {
            trimProcessHeap();
          }
        }

        trimProcessHeap();
        final after = rssBytes();
        final diff = after - before;

        expect(
          diff,
          lessThan(10 * 1024 * 1024),
          reason:
              'RSS should stay bounded in long-running Dart processes '
              '(before=$before after=$after diff=$diff)',
        );
      } finally {
        tempDir.deleteSync(recursive: true);
      }
    },
    skip: Platform.isLinux ? false : 'RSS leak regression is Linux-specific',
  );
}
