import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

void main() {
  late String libPath;

  setUpAll(() {
    libPath = findNativeLib();
  });

  group('stmt cache disabled (capacity == 0)', () {
    late Database db;

    setUp(() {
      db = Database.open(':memory:', libraryPath: libPath, stmtCacheCapacity: 0);
      db.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
    });

    tearDown(() => db.close());

    test('prepare() returns independent objects for the same SQL', () {
      final s1 = db.prepare('SELECT id FROM t');
      final s2 = db.prepare('SELECT id FROM t');
      try {
        expect(identical(s1, s2), isFalse);
      } finally {
        s1.dispose();
        s2.dispose();
      }
    });

    test('stmtCacheStats reports 0 hits, size, and capacity', () {
      db.prepare('SELECT id FROM t').dispose();
      final stats = db.stmtCacheStats;
      expect(stats['hits'], 0);
      expect(stats['misses'], greaterThan(0));
      expect(stats['size'], 0);
      expect(stats['capacity'], 0);
    });
  });

  group('stmt cache enabled (capacity > 0)', () {
    late Database db;

    setUp(() {
      db = Database.open(':memory:', libraryPath: libPath, stmtCacheCapacity: 4);
      db.execute('CREATE TABLE t (id INT64 PRIMARY KEY)');
    });

    tearDown(() => db.close());

    test('prepare() returns the cached handle on second call', () {
      final s1 = db.prepare('SELECT id FROM t');
      // return it so the cache can serve it again
      final s2 = db.prepare('SELECT id FROM t');
      expect(identical(s1, s2), isTrue);
      // s2 is the same object; do not double-dispose via cache
      db.clearStmtCache();
    });

    test('stmtCacheStats tracks hits and misses', () {
      db.prepare('SELECT id FROM t'); // miss
      db.prepare('SELECT id FROM t'); // hit
      db.prepare('SELECT id FROM t'); // hit
      final stats = db.stmtCacheStats;
      expect(stats['misses'], 1);
      expect(stats['hits'], 2);
      expect(stats['size'], 1);
      expect(stats['capacity'], 4);
    });

    test('cache evicts LRU entry when over capacity', () {
      // Fill cache beyond capacity 4.
      for (var i = 0; i < 5; i++) {
        db.execute('CREATE TABLE t$i (id INT64 PRIMARY KEY)');
        db.prepare('SELECT id FROM t$i');
      }
      final stats = db.stmtCacheStats;
      expect(stats['size'], lessThanOrEqualTo(4));
    });

    test('clearStmtCache() drains all entries', () {
      db.prepare('SELECT id FROM t');
      expect(db.stmtCacheStats['size'], greaterThan(0));
      db.clearStmtCache();
      expect(db.stmtCacheStats['size'], 0);
      expect(db.stmtCacheStats['hits'], 0);
      expect(db.stmtCacheStats['misses'], 0);
    });

    test('onPerformanceWarning fires once when hit rate is < 50%', () {
      final warnings = <PerformanceWarning>[];
      db.onPerformanceWarning = warnings.add;

      // 100 unique SQLs → all misses → hit rate == 0%, fires at 100.
      for (var i = 0; i < 100; i++) {
        db.execute('CREATE TABLE pw$i (id INT64 PRIMARY KEY)');
        db.prepare('SELECT id FROM pw$i');
      }

      expect(warnings, hasLength(1));
      expect(warnings.first.message, contains('stmt cache'));

      // Subsequent call must NOT fire again.
      db.execute('CREATE TABLE pw_extra (id INT64 PRIMARY KEY)');
      db.prepare('SELECT id FROM pw_extra');
      expect(warnings, hasLength(1));
    });
  });
}
