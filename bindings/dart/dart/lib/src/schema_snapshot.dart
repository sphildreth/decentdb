import 'types.dart';

class SchemaCheckConstraintInfo {
  const SchemaCheckConstraintInfo({
    this.name,
    required this.expressionSql,
  });

  final String? name;
  final String expressionSql;

  factory SchemaCheckConstraintInfo.fromJson(Map<String, dynamic> json) {
    return SchemaCheckConstraintInfo(
      name: json['name'] as String?,
      expressionSql: json['expression_sql'] as String,
    );
  }
}

class SchemaColumnInfo {
  const SchemaColumnInfo({
    required this.name,
    required this.type,
    required this.nullable,
    this.defaultSql,
    required this.primaryKey,
    required this.unique,
    required this.autoIncrement,
    this.generatedSql,
    required this.generatedStored,
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
  final String? generatedSql;
  final bool generatedStored;
  final List<SchemaCheckConstraintInfo> checks;
  final ForeignKeyInfo? foreignKey;

  factory SchemaColumnInfo.fromJson(Map<String, dynamic> json) {
    return SchemaColumnInfo(
      name: json['name'] as String,
      type: json['column_type'] as String,
      nullable: json['nullable'] as bool? ?? true,
      defaultSql: json['default_sql'] as String?,
      primaryKey: json['primary_key'] as bool? ?? false,
      unique: json['unique'] as bool? ?? false,
      autoIncrement: json['auto_increment'] as bool? ?? false,
      generatedSql: json['generated_sql'] as String?,
      generatedStored: json['generated_stored'] as bool? ?? false,
      checks: (json['checks'] as List? ?? const [])
          .map((value) =>
              SchemaCheckConstraintInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
      foreignKey: json['foreign_key'] == null
          ? null
          : ForeignKeyInfo.fromJson(
              json['foreign_key'] as Map<String, dynamic>),
    );
  }
}

class SchemaTableInfo {
  const SchemaTableInfo({
    required this.name,
    required this.temporary,
    required this.ddl,
    required this.rowCount,
    required this.primaryKeyColumns,
    required this.checks,
    required this.foreignKeys,
    required this.columns,
  });

  final String name;
  final bool temporary;
  final String ddl;
  final int rowCount;
  final List<String> primaryKeyColumns;
  final List<SchemaCheckConstraintInfo> checks;
  final List<ForeignKeyInfo> foreignKeys;
  final List<SchemaColumnInfo> columns;

  factory SchemaTableInfo.fromJson(Map<String, dynamic> json) {
    return SchemaTableInfo(
      name: json['name'] as String,
      temporary: json['temporary'] as bool? ?? false,
      ddl: json['ddl'] as String,
      rowCount: json['row_count'] as int? ?? 0,
      primaryKeyColumns:
          (json['primary_key_columns'] as List? ?? const []).cast<String>(),
      checks: (json['checks'] as List? ?? const [])
          .map((value) =>
              SchemaCheckConstraintInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
      foreignKeys: (json['foreign_keys'] as List? ?? const [])
          .map(
              (value) => ForeignKeyInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
      columns: (json['columns'] as List? ?? const [])
          .map((value) =>
              SchemaColumnInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
    );
  }
}

class SchemaViewInfo {
  const SchemaViewInfo({
    required this.name,
    required this.temporary,
    required this.sqlText,
    required this.columnNames,
    required this.dependencies,
    required this.ddl,
  });

  final String name;
  final bool temporary;
  final String sqlText;
  final List<String> columnNames;
  final List<String> dependencies;
  final String ddl;

  factory SchemaViewInfo.fromJson(Map<String, dynamic> json) {
    return SchemaViewInfo(
      name: json['name'] as String,
      temporary: json['temporary'] as bool? ?? false,
      sqlText: json['sql_text'] as String,
      columnNames: (json['column_names'] as List? ?? const []).cast<String>(),
      dependencies: (json['dependencies'] as List? ?? const []).cast<String>(),
      ddl: json['ddl'] as String,
    );
  }
}

class SchemaIndexInfo {
  const SchemaIndexInfo({
    required this.name,
    required this.tableName,
    required this.kind,
    required this.unique,
    required this.columns,
    required this.includeColumns,
    this.predicateSql,
    required this.fresh,
    required this.temporary,
    required this.ddl,
  });

  final String name;
  final String tableName;
  final String kind;
  final bool unique;
  final List<String> columns;
  final List<String> includeColumns;
  final String? predicateSql;
  final bool fresh;
  final bool temporary;
  final String ddl;

  factory SchemaIndexInfo.fromJson(Map<String, dynamic> json) {
    return SchemaIndexInfo(
      name: json['name'] as String,
      tableName: json['table_name'] as String,
      kind: json['kind'] as String,
      unique: json['unique'] as bool? ?? false,
      columns: (json['columns'] as List? ?? const []).cast<String>(),
      includeColumns:
          (json['include_columns'] as List? ?? const []).cast<String>(),
      predicateSql: json['predicate_sql'] as String?,
      fresh: json['fresh'] as bool? ?? false,
      temporary: json['temporary'] as bool? ?? false,
      ddl: json['ddl'] as String,
    );
  }
}

class SchemaTriggerInfo {
  const SchemaTriggerInfo({
    required this.name,
    required this.targetName,
    required this.targetKind,
    required this.timing,
    required this.events,
    required this.eventsMask,
    required this.forEachRow,
    required this.temporary,
    required this.actionSql,
    required this.ddl,
  });

  final String name;
  final String targetName;
  final String targetKind;
  final String timing;
  final List<String> events;
  final int eventsMask;
  final bool forEachRow;
  final bool temporary;
  final String actionSql;
  final String ddl;

  factory SchemaTriggerInfo.fromJson(Map<String, dynamic> json) {
    return SchemaTriggerInfo(
      name: json['name'] as String,
      targetName: json['target_name'] as String,
      targetKind: json['target_kind'] as String,
      timing: json['timing'] as String,
      events: (json['events'] as List? ?? const []).cast<String>(),
      eventsMask: json['events_mask'] as int? ?? 0,
      forEachRow: json['for_each_row'] as bool? ?? false,
      temporary: json['temporary'] as bool? ?? false,
      actionSql: json['action_sql'] as String,
      ddl: json['ddl'] as String,
    );
  }
}

class SchemaSnapshot {
  const SchemaSnapshot({
    required this.snapshotVersion,
    required this.schemaCookie,
    required this.tables,
    required this.views,
    required this.indexes,
    required this.triggers,
  });

  final int snapshotVersion;
  final int schemaCookie;
  final List<SchemaTableInfo> tables;
  final List<SchemaViewInfo> views;
  final List<SchemaIndexInfo> indexes;
  final List<SchemaTriggerInfo> triggers;

  factory SchemaSnapshot.fromJson(Map<String, dynamic> json) {
    return SchemaSnapshot(
      snapshotVersion: json['snapshot_version'] as int? ?? 0,
      schemaCookie: json['schema_cookie'] as int? ?? 0,
      tables: (json['tables'] as List? ?? const [])
          .map((value) =>
              SchemaTableInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
      views: (json['views'] as List? ?? const [])
          .map(
              (value) => SchemaViewInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
      indexes: (json['indexes'] as List? ?? const [])
          .map((value) =>
              SchemaIndexInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
      triggers: (json['triggers'] as List? ?? const [])
          .map((value) =>
              SchemaTriggerInfo.fromJson(value as Map<String, dynamic>))
          .toList(),
    );
  }
}
