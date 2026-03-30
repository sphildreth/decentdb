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

void main() {
  late Database db;
  late String libPath;

  setUpAll(() {
    libPath = findNativeLib();
  });

  setUp(() {
    db = Database.open(':memory:', libraryPath: libPath);
    db.execute(
      'CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT, active BOOL)',
    );
  });

  tearDown(() {
    db.close();
  });

  void seed(int count) {
    db.transaction(() {
      for (var i = 1; i <= count; i++) {
        db.execute("INSERT INTO items VALUES ($i, 'item_$i', ${i % 2 == 0})");
      }
    });
  }

  test('nextPage paginates across large result sets', () {
    seed(600);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      var seen = 0;
      while (true) {
        final page = stmt.nextPage(128);
        seen += page.rows.length;
        if (page.isLast) {
          break;
        }
      }
      expect(seen, 600);
    } finally {
      stmt.dispose();
    }
  });

  test('mixed step then nextPage does not duplicate rows', () {
    seed(7);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 1);
      final page = stmt.nextPage(3);
      expect(page.rows.map((row) => row['id']).toList(), [2, 3, 4]);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 5);
    } finally {
      stmt.dispose();
    }
  });

  test('mixed nextPage then step continues from page end', () {
    seed(6);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      final page = stmt.nextPage(4);
      expect(page.rows.map((row) => row['id']).toList(), [1, 2, 3, 4]);
      expect(page.isLast, isFalse);

      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 5);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 6);
      expect(stmt.step(), isFalse);
    } finally {
      stmt.dispose();
    }
  });

  test('readRow validity and invalidation rules are enforced', () {
    seed(3);
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      expect(() => stmt.readRow(), throwsStateError);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 1);
      stmt.nextPage(1);
      expect(() => stmt.readRow(), throwsStateError);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 3);
    } finally {
      stmt.dispose();
    }
  });

  test('reset/rebind/clearBindings invalidate streaming state', () {
    seed(4);
    final stmt =
        db.prepare(r'SELECT id FROM items WHERE active = $1 ORDER BY id');
    try {
      stmt.bindBool(1, true);
      expect(stmt.step(), isTrue);
      expect(stmt.readRow()['id'], 2);

      stmt.reset();
      expect(() => stmt.readRow(), throwsStateError);

      stmt.clearBindings();
      stmt.bindBool(1, false);
      final page = stmt.nextPage(10);
      expect(page.rows.map((row) => row['id']).toList(), [1, 3]);
      expect(() => stmt.readRow(), throwsStateError);
    } finally {
      stmt.dispose();
    }
  });

  test('empty result sets are handled cleanly', () {
    final stmt = db.prepare('SELECT id FROM items ORDER BY id');
    try {
      expect(stmt.step(), isFalse);
      final page = stmt.nextPage(10);
      expect(page.rows, isEmpty);
      expect(page.isLast, isTrue);
    } finally {
      stmt.dispose();
    }
  });
}
