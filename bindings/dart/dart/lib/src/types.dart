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
