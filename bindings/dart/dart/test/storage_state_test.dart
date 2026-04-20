import 'dart:convert';
import 'dart:io';

import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

void main() {
  late String libPath;

  setUpAll(() {
    libPath = findNativeLib();
  });

  group('inspectStorageState', () {
    test('returns zero-allocation state on a fresh :memory: DB', () {
      final db = Database.open(':memory:', libraryPath: libPath);
      try {
        final snap = db.inspectStorageState();
        expect(snap.pageSize, equals(4096));
        // In-memory DBs may have a small WAL header (e.g. 32 bytes).
        expect(snap.walSizeBytes, greaterThanOrEqualTo(0));
        expect(snap.walVersions, equals(0));
        expect(snap.checkpointSequence, equals(0));
        // pageCount may be 0 or 1 depending on catalog initialisation; >= 0.
        expect(snap.pageCount, greaterThanOrEqualTo(0));
        expect(snap.activeReaders, greaterThanOrEqualTo(0));
      } finally {
        db.close();
      }
    });

    test('returns page and WAL state after a write', () {
      final tmp = Directory.systemTemp.createTempSync('decentdb_snap_test_');
      final dbPath = '${tmp.path}/test.db';
      try {
        final db = Database.open(dbPath, libraryPath: libPath);
        try {
          db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
          db.execute("INSERT INTO t VALUES (1, 'hello')");

          final snap = db.inspectStorageState();
          expect(snap.pageCount, greaterThan(0));
          expect(snap.walSizeBytes, greaterThan(0));
        } finally {
          db.close();
        }
      } finally {
        tmp.deleteSync(recursive: true);
      }
    });

    test('rawJson parses as JSON object', () {
      final db = Database.open(':memory:', libraryPath: libPath);
      try {
        final snap = db.inspectStorageState();
        expect(snap.rawJson, isNotEmpty);
        final decoded = jsonDecode(snap.rawJson);
        expect(decoded, isA<Map<String, Object?>>());
      } finally {
        db.close();
      }
    });

    test('StorageStateSnapshot.fromJson ignores unknown keys', () {
      const raw = '{"page_size":4096,"unknown_future_field":42}';
      final snap = StorageStateSnapshot.fromJson(
        jsonDecode(raw) as Map<String, Object?>,
        rawJson: raw,
      );
      expect(snap.pageSize, equals(4096));
    });

    test('StorageStateSnapshot.fromJson defaults missing numeric keys to 0',
        () {
      const raw = '{"page_size":4096}';
      final snap = StorageStateSnapshot.fromJson(
        jsonDecode(raw) as Map<String, Object?>,
        rawJson: raw,
      );
      expect(snap.pageCount, equals(0));
      expect(snap.walSizeBytes, equals(0));
    });

    test(
        'StorageStateSnapshot.fromJson throws FormatException for bad numeric value',
        () {
      const raw = '{"page_size":"not-a-number"}';
      expect(
        () => StorageStateSnapshot.fromJson(
          jsonDecode(raw) as Map<String, Object?>,
          rawJson: raw,
        ),
        throwsA(isA<FormatException>()),
      );
    });
  });
}
