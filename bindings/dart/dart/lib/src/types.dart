import 'dart:typed_data';

/// DecentDB error codes.
enum ErrorCode {
  ok(0),
  io(1),
  corruption(2),
  constraint(3),
  transaction(4),
  sql(5),
  internal(6),
  panic(7),
  unsupportedFormatVersion(8);

  const ErrorCode(this.code);

  final int code;

  static ErrorCode fromCode(int code) {
    for (final value in values) {
      if (value.code == code) return value;
    }
    throw StateError('Unknown DecentDB error code: $code');
  }
}

class DecimalValue {
  const DecimalValue(this.scaled, this.scale);

  final int scaled;
  final int scale;

  @override
  bool operator ==(Object other) =>
      other is DecimalValue && other.scaled == scaled && other.scale == scale;

  @override
  int get hashCode => Object.hash(scaled, scale);

  @override
  String toString() => 'DecimalValue($scaled, scale: $scale)';
}

/// A canonical 16-byte UUID value.
///
/// Bytes are stored in native order (hex byte 0 first, matching the engine's
/// `Value::Uuid([u8; 16])` representation). Use [UuidValue.parse] to construct
/// from canonical hyphenated text ("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx").
///
/// Only the 36-character hyphenated form is accepted by [parse]; compact
/// 32-character text is rejected with [FormatException].
final class UuidValue {
  /// The raw 16 bytes of the UUID in canonical byte order.
  ///
  /// A defensive copy is made on construction so mutations to the original
  /// [Uint8List] do not affect this value.
  final Uint8List bytes;

  /// Constructs a [UuidValue] from [bytes].
  ///
  /// [bytes] must have exactly 16 elements; throws [ArgumentError] otherwise.
  /// A copy of [bytes] is stored internally.
  UuidValue(Uint8List bytes) : bytes = Uint8List.fromList(bytes) {
    if (bytes.length != 16) {
      throw ArgumentError(
          'UuidValue requires exactly 16 bytes, got ${bytes.length}');
    }
  }

  /// Parses a canonical 36-character hyphenated UUID string.
  ///
  /// Accepts both lower- and uppercase hex digits. Throws [FormatException]
  /// if [text] is not exactly 36 characters in the form
  /// "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
  factory UuidValue.parse(String text) {
    if (text.length != 36 ||
        text[8] != '-' ||
        text[13] != '-' ||
        text[18] != '-' ||
        text[23] != '-') {
      throw FormatException(
          'Invalid UUID: expected xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, '
          'got "$text"');
    }
    final result = Uint8List(16);
    var byteIdx = 0;
    var i = 0;
    while (i < 36) {
      if (text[i] == '-') {
        i++;
        continue;
      }
      result[byteIdx++] =
          (_hexNibble(text, i) << 4) | _hexNibble(text, i + 1);
      i += 2;
    }
    // Use internal constructor to avoid a second copy.
    return UuidValue._trusted(result);
  }

  // Private constructor used by parse — skips the defensive copy since the
  // bytes were just freshly allocated inside parse.
  UuidValue._trusted(this.bytes);

  static int _hexNibble(String s, int index) {
    final v = s.codeUnitAt(index);
    if (v >= 0x30 && v <= 0x39) return v - 0x30; // 0-9
    if (v >= 0x61 && v <= 0x66) return v - 0x57; // a-f
    if (v >= 0x41 && v <= 0x46) return v - 0x37; // A-F
    throw FormatException(
        'Invalid hex character "${s[index]}" in UUID "$s"');
  }

  /// Returns the canonical lowercase hyphenated text representation.
  String toText() {
    final buf = StringBuffer();
    for (var i = 0; i < 16; i++) {
      if (i == 4 || i == 6 || i == 8 || i == 10) buf.write('-');
      buf.write(bytes[i].toRadixString(16).padLeft(2, '0'));
    }
    return buf.toString();
  }

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    if (other is! UuidValue) return false;
    for (var i = 0; i < 16; i++) {
      if (bytes[i] != other.bytes[i]) return false;
    }
    return true;
  }

  @override
  int get hashCode => Object.hashAll(bytes);

  @override
  String toString() => toText();
}

class ForeignKeyInfo {
  const ForeignKeyInfo({
    this.name,
    required this.columns,
    required this.referencedTable,
    required this.referencedColumns,
    required this.onDelete,
    required this.onUpdate,
  });

  final String? name;
  final List<String> columns;
  final String referencedTable;
  final List<String> referencedColumns;
  final String onDelete;
  final String onUpdate;

  factory ForeignKeyInfo.fromJson(Map<String, dynamic> json) {
    return ForeignKeyInfo(
      name: json['name'] as String?,
      columns: (json['columns'] as List? ?? const []).cast<String>(),
      referencedTable: json['referenced_table'] as String,
      referencedColumns:
          (json['referenced_columns'] as List? ?? const []).cast<String>(),
      onDelete: json['on_delete'] as String,
      onUpdate: json['on_update'] as String,
    );
  }
}

class ColumnInfo {
  const ColumnInfo({
    required this.name,
    required this.type,
    required this.nullable,
    this.defaultSql,
    required this.primaryKey,
    required this.unique,
    required this.autoIncrement,
    required this.checks,
    this.foreignKey,
  });

  final String name;
  final String type;
  final bool nullable;
  final String? defaultSql;
  final bool primaryKey;
  final bool unique;
  final bool autoIncrement;
  final List<String> checks;
  final ForeignKeyInfo? foreignKey;

  factory ColumnInfo.fromJson(Map<String, dynamic> json) {
    return ColumnInfo(
      name: json['name'] as String,
      type: json['column_type'] as String,
      nullable: json['nullable'] as bool? ?? true,
      defaultSql: json['default_sql'] as String?,
      primaryKey: json['primary_key'] as bool? ?? false,
      unique: json['unique'] as bool? ?? false,
      autoIncrement: json['auto_increment'] as bool? ?? false,
      checks: (json['checks'] as List? ?? const []).cast<String>(),
      foreignKey: json['foreign_key'] == null
          ? null
          : ForeignKeyInfo.fromJson(
              json['foreign_key'] as Map<String, dynamic>),
    );
  }
}

class TableInfo {
  const TableInfo({
    required this.name,
    required this.columns,
    required this.checks,
    required this.foreignKeys,
    required this.primaryKeyColumns,
    required this.rowCount,
  });

  final String name;
  final List<ColumnInfo> columns;
  final List<String> checks;
  final List<ForeignKeyInfo> foreignKeys;
  final List<String> primaryKeyColumns;
  final int rowCount;

  factory TableInfo.fromJson(Map<String, dynamic> json) {
    return TableInfo(
      name: json['name'] as String,
      columns: (json['columns'] as List? ?? const [])
          .map((value) => ColumnInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
      checks: (json['checks'] as List? ?? const []).cast<String>(),
      foreignKeys: (json['foreign_keys'] as List? ?? const [])
          .map(
              (value) => ForeignKeyInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
      primaryKeyColumns:
          (json['primary_key_columns'] as List? ?? const []).cast<String>(),
      rowCount: json['row_count'] as int? ?? 0,
    );
  }
}

class IndexInfo {
  const IndexInfo({
    required this.name,
    required this.tableName,
    required this.kind,
    required this.unique,
    required this.columns,
    this.predicateSql,
    required this.fresh,
  });

  final String name;
  final String tableName;
  final String kind;
  final bool unique;
  final List<String> columns;
  final String? predicateSql;
  final bool fresh;

  factory IndexInfo.fromJson(Map<String, dynamic> json) {
    return IndexInfo(
      name: json['name'] as String,
      tableName: json['table_name'] as String,
      kind: json['kind'] as String,
      unique: json['unique'] as bool? ?? false,
      columns: (json['columns'] as List? ?? const []).cast<String>(),
      predicateSql: json['predicate_sql'] as String?,
      fresh: json['fresh'] as bool? ?? false,
    );
  }
}

class ViewInfo {
  const ViewInfo({
    required this.name,
    required this.sqlText,
    required this.columnNames,
    required this.dependencies,
  });

  final String name;
  final String sqlText;
  final List<String> columnNames;
  final List<String> dependencies;

  factory ViewInfo.fromJson(Map<String, dynamic> json) {
    return ViewInfo(
      name: json['name'] as String,
      sqlText: json['sql_text'] as String,
      columnNames: (json['column_names'] as List? ?? const []).cast<String>(),
      dependencies: (json['dependencies'] as List? ?? const []).cast<String>(),
    );
  }
}

class TriggerInfo {
  const TriggerInfo({
    required this.name,
    required this.targetName,
    required this.kind,
    required this.event,
    required this.onView,
    required this.actionSql,
  });

  final String name;
  final String targetName;
  final String kind;
  final String event;
  final bool onView;
  final String actionSql;

  factory TriggerInfo.fromJson(Map<String, dynamic> json) {
    return TriggerInfo(
      name: json['name'] as String,
      targetName: json['target_name'] as String,
      kind: json['kind'] as String,
      event: json['event'] as String,
      onView: json['on_view'] as bool? ?? false,
      actionSql: json['action_sql'] as String,
    );
  }
}

/// A snapshot of the database engine's storage and WAL state.
///
/// Obtained via [Database.inspectStorageState]. The [rawJson] field preserves
/// the full JSON text for forward compatibility; all named fields are parsed
/// from it.
class StorageStateSnapshot {
  const StorageStateSnapshot({
    required this.rawJson,
    required this.path,
    required this.pageSize,
    required this.pageCount,
    required this.schemaCookie,
    required this.walEndLsn,
    required this.walSizeBytes,
    required this.walPath,
    required this.checkpointSequence,
    required this.activeReaders,
    required this.walVersions,
    required this.warningCount,
    required this.sharedWal,
    required this.cacheEntries,
    required this.cacheCapacity,
  });

  /// The raw JSON text returned by the engine. Non-empty; round-trips through
  /// `jsonDecode` without error.
  final String rawJson;

  /// The database file path (or `":memory:"` for in-memory databases).
  final String path;

  /// Page size in bytes (e.g. 4096).
  final int pageSize;

  /// Number of committed pages currently tracked by the pager.
  final int pageCount;

  /// Schema cookie — incremented on every DDL change.
  final int schemaCookie;

  /// LSN of the latest WAL record (end-of-WAL position).
  final int walEndLsn;

  /// Current WAL file size in bytes. Zero for in-memory databases.
  final int walSizeBytes;

  /// Path to the WAL file.
  final String walPath;

  /// LSN of the last successful checkpoint.
  final int checkpointSequence;

  /// Number of active concurrent readers holding WAL snapshots.
  final int activeReaders;

  /// Number of distinct WAL page versions retained for active readers.
  final int walVersions;

  /// Number of health warnings emitted by the engine.
  final int warningCount;

  /// Whether this database uses a shared (inter-process) WAL.
  final bool sharedWal;

  /// Number of entries currently in the page cache (0 if not reported).
  final int cacheEntries;

  /// Page cache capacity in entries (0 if not reported).
  final int cacheCapacity;

  /// Construct from a decoded JSON map.
  ///
  /// Behaviour:
  /// - Unknown keys are silently ignored.
  /// - Missing numeric keys default to `0`.
  /// - A numeric-typed key that is present but holds a non-numeric value
  ///   throws [FormatException].
  factory StorageStateSnapshot.fromJson(
    Map<String, Object?> json, {
    required String rawJson,
  }) {
    return StorageStateSnapshot(
      rawJson: rawJson,
      path: _stringField(json, 'path'),
      pageSize: _numericField(json, 'page_size'),
      pageCount: _numericField(json, 'page_count'),
      schemaCookie: _numericField(json, 'schema_cookie'),
      walEndLsn: _numericField(json, 'wal_end_lsn'),
      walSizeBytes: _numericField(json, 'wal_file_size'),
      walPath: _stringField(json, 'wal_path'),
      checkpointSequence: _numericField(json, 'last_checkpoint_lsn'),
      activeReaders: _numericField(json, 'active_readers'),
      walVersions: _numericField(json, 'wal_versions'),
      warningCount: _numericField(json, 'warning_count'),
      sharedWal: _boolField(json, 'shared_wal'),
      cacheEntries: _numericField(json, 'cache_entries'),
      cacheCapacity: _numericField(json, 'cache_capacity'),
    );
  }

  static int _numericField(Map<String, Object?> json, String key) {
    final v = json[key];
    if (v == null) return 0;
    if (v is int) return v;
    if (v is double) return v.toInt();
    throw FormatException(
        'Expected numeric value for key "$key", got ${v.runtimeType}: $v');
  }

  static bool _boolField(Map<String, Object?> json, String key) {
    final v = json[key];
    if (v == null) return false;
    if (v is bool) return v;
    throw FormatException(
        'Expected boolean value for key "$key", got ${v.runtimeType}: $v');
  }

  static String _stringField(Map<String, Object?> json, String key) {
    final v = json[key];
    if (v == null) return '';
    if (v is String) return v;
    return v.toString();
  }
}
