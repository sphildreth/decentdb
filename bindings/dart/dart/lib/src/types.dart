/// DecentDB logical value types returned by the Dart binding.
///
/// `decentdb_column_type()` exposes raw Nim `ValueKind` ordinals. `fromCode()`
/// normalizes storage-specific variants (for example overflow/compressed
/// text/blob kinds and compact bool/int encodings) to these logical types.
enum ColumnType {
  /// NULL value.
  vkNull(0),

  /// 64-bit signed integer.
  vkInt64(1),

  /// Boolean.
  vkBool(2),

  /// 64-bit IEEE 754 float.
  vkFloat64(3),

  /// UTF-8 text.
  vkText(4),

  /// Binary blob.
  vkBlob(5),

  /// Fixed-point decimal (unscaled int64 + scale).
  vkDecimal(12),

  /// Timestamp as microseconds since Unix epoch UTC.
  vkDateTime(17);

  final int code;
  const ColumnType(this.code);

  static ColumnType fromCode(int code) {
    switch (code) {
      case 0:
        return vkNull;
      case 1:
      case 15:
      case 16:
        return vkInt64;
      case 2:
      case 13:
      case 14:
        return vkBool;
      case 3:
        return vkFloat64;
      case 4:
      case 6:
      case 8:
      case 10:
        return vkText;
      case 5:
      case 7:
      case 9:
      case 11:
        return vkBlob;
      case 12:
        return vkDecimal;
      case 17:
        return vkDateTime;
      default:
        throw ArgumentError.value(
          code,
          'code',
          'Unknown DecentDB column type code',
        );
    }
  }
}

/// DecentDB error codes (internal ErrorCode + 1).
///
/// 0 = OK (no error).
enum ErrorCode {
  ok(0),
  io(1),
  corruption(2),
  constraint(3),
  transaction(4),
  sql(5),
  internal(6);

  final int code;
  const ErrorCode(this.code);

  static ErrorCode fromCode(int code) {
    for (final e in values) {
      if (e.code == code) return e;
    }
    return internal;
  }
}

/// Metadata for a table column, as returned by schema introspection.
class ColumnInfo {
  final String name;
  final String type;
  final bool notNull;
  final bool unique;
  final bool primaryKey;
  final String? refTable;
  final String? refColumn;
  final String? refOnDelete;
  final String? refOnUpdate;

  const ColumnInfo({
    required this.name,
    required this.type,
    required this.notNull,
    required this.unique,
    required this.primaryKey,
    this.refTable,
    this.refColumn,
    this.refOnDelete,
    this.refOnUpdate,
  });

  factory ColumnInfo.fromJson(Map<String, dynamic> json) {
    return ColumnInfo(
      name: json['name'] as String,
      type: json['type'] as String,
      notNull: json['not_null'] as bool? ?? false,
      unique: json['unique'] as bool? ?? false,
      primaryKey: json['primary_key'] as bool? ?? false,
      refTable: json['ref_table'] as String?,
      refColumn: json['ref_column'] as String?,
      refOnDelete: json['ref_on_delete'] as String?,
      refOnUpdate: json['ref_on_update'] as String?,
    );
  }

  @override
  String toString() =>
      'ColumnInfo($name $type${notNull ? " NOT NULL" : ""}'
      '${primaryKey ? " PK" : ""}${unique ? " UNIQUE" : ""})';
}

/// Metadata for an index.
class IndexInfo {
  final String name;
  final String table;
  final List<String> columns;
  final bool unique;
  final String kind;

  const IndexInfo({
    required this.name,
    required this.table,
    required this.columns,
    required this.unique,
    required this.kind,
  });

  factory IndexInfo.fromJson(Map<String, dynamic> json) {
    return IndexInfo(
      name: json['name'] as String,
      table: json['table'] as String,
      columns: (json['columns'] as List).cast<String>(),
      unique: json['unique'] as bool? ?? false,
      kind: json['kind'] as String? ?? 'btree',
    );
  }

  @override
  String toString() =>
      'IndexInfo($name on $table(${columns.join(", ")}) $kind'
      '${unique ? " UNIQUE" : ""})';
}
