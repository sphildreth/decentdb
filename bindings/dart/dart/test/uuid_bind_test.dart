import 'dart:typed_data';

import 'package:decentdb/decentdb.dart';
import 'package:test/test.dart';

import 'test_utils.dart';

void main() {
  late Database db;
  late String libPath;

  setUpAll(() {
    libPath = findNativeLib();
  });

  setUp(() {
    db = Database.open(':memory:', libraryPath: libPath);
  });

  tearDown(() {
    db.close();
  });

  group('UuidValue bind and round-trip', () {
    test('binds UuidValue into a UUID column and round-trips exact bytes', () {
      db.execute('CREATE TABLE t (id UUID PRIMARY KEY, v UUID)');

      final id1 =
          UuidValue(Uint8List.fromList(List.generate(16, (i) => i + 1)));
      final v1 =
          UuidValue(Uint8List.fromList(List.generate(16, (i) => 16 - i)));

      final insert = db.prepare('INSERT INTO t (id, v) VALUES (\$1, \$2)');
      try {
        insert.bind(1, id1);
        insert.bind(2, v1);
        insert.execute();
      } finally {
        insert.dispose();
      }

      final select = db.prepare('SELECT id, v FROM t');
      try {
        final rows = select.query();
        expect(rows.length, equals(1));
        final row = rows[0];
        // Decode path returns Uint8List for UUID columns; compare byte-for-byte.
        expect(row.values[0], isA<Uint8List>());
        expect(row.values[1], isA<Uint8List>());
        expect(row.values[0], equals(id1.bytes));
        expect(row.values[1], equals(v1.bytes));
      } finally {
        select.dispose();
      }
    });

    test('parses and formats canonical text', () {
      const canonical = '550e8400-e29b-41d4-a716-446655440000';
      final uuid = UuidValue.parse(canonical);
      expect(uuid.toText(), equals(canonical));
    });

    test('parse normalises uppercase hex to lowercase', () {
      final lower = UuidValue.parse('550e8400-e29b-41d4-a716-446655440000');
      final upper = UuidValue.parse('550E8400-E29B-41D4-A716-446655440000');
      expect(upper.toText(), equals(lower.toText()));
      expect(upper, equals(lower));
    });

    test('rejects malformed UUID text — FormatException', () {
      // Wrong length
      expect(
        () => UuidValue.parse('not-a-uuid'),
        throwsA(isA<FormatException>()),
      );
      // Missing dashes
      expect(
        () => UuidValue.parse('550e8400e29b41d4a716446655440000'),
        throwsA(isA<FormatException>()),
      );
      // Dash in wrong position
      expect(
        () => UuidValue.parse('550e840-0e29b-41d4-a716-446655440000'),
        throwsA(isA<FormatException>()),
      );
      // Invalid hex character
      expect(
        () => UuidValue.parse('xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'),
        throwsA(isA<FormatException>()),
      );
    });

    test('rejects UuidValue with wrong byte length — ArgumentError', () {
      expect(
        () => UuidValue(Uint8List(15)),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => UuidValue(Uint8List(17)),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => UuidValue(Uint8List(0)),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('bare Uint8List still binds as BLOB', () {
      db.execute('CREATE TABLE blobs (id INT64 PRIMARY KEY, v BLOB)');

      final blobBytes = Uint8List.fromList(List.generate(16, (i) => i + 10));

      final insert =
          db.prepare('INSERT INTO blobs (id, v) VALUES (\$1, \$2)');
      try {
        insert.bind(1, 1);
        insert.bind(2, blobBytes); // bare Uint8List → must go to bindBlob
        insert.execute();
      } finally {
        insert.dispose();
      }

      final select = db.prepare('SELECT v FROM blobs WHERE id = \$1');
      try {
        select.bind(1, 1);
        final rows = select.query();
        expect(rows.length, equals(1));
        // Must come back as Uint8List (BLOB), not UuidValue.
        final v = rows[0].values[0];
        expect(v, isA<Uint8List>());
        expect(v, isNot(isA<UuidValue>()));
        expect(v, equals(blobBytes));
      } finally {
        select.dispose();
      }
    });
  });
}
