use super::*;

pub(super) fn table_info(table: &TableSchema, row_count: usize) -> TableInfo {
    TableInfo {
        name: table.name.clone(),
        temporary: table.temporary,
        columns: table.columns.iter().map(column_info).collect(),
        checks: table
            .checks
            .iter()
            .map(|check| check.expression_sql.clone())
            .collect(),
        foreign_keys: table.foreign_keys.iter().map(foreign_key_info).collect(),
        primary_key_columns: table.primary_key_columns.clone(),
        row_count,
    }
}

pub(super) fn column_info(column: &ColumnSchema) -> ColumnInfo {
    ColumnInfo {
        name: column.name.clone(),
        column_type: column.column_type.as_str().to_string(),
        nullable: column.nullable,
        default_sql: column.default_sql.clone(),
        primary_key: column.primary_key,
        unique: column.unique,
        auto_increment: column.auto_increment,
        checks: column
            .checks
            .iter()
            .map(|check| check.expression_sql.clone())
            .collect(),
        foreign_key: column.foreign_key.as_ref().map(foreign_key_info),
    }
}

pub(super) fn foreign_key_info(foreign_key: &ForeignKeyConstraint) -> ForeignKeyInfo {
    ForeignKeyInfo {
        name: foreign_key.name.clone(),
        columns: foreign_key.columns.clone(),
        referenced_table: foreign_key.referenced_table.clone(),
        referenced_columns: foreign_key.referenced_columns.clone(),
        on_delete: foreign_key_action_name(foreign_key.on_delete).to_string(),
        on_update: foreign_key_action_name(foreign_key.on_update).to_string(),
    }
}

pub(super) fn index_info(index: &IndexSchema) -> IndexInfo {
    IndexInfo {
        name: index.name.clone(),
        table_name: index.table_name.clone(),
        kind: match index.kind {
            IndexKind::Btree => "btree",
            IndexKind::Trigram => "trigram",
            IndexKind::Spatial => "spatial",
            IndexKind::FullText => "fulltext",
        }
        .to_string(),
        unique: index.unique,
        columns: index.columns.iter().map(index_column_name).collect(),
        include_columns: index.include_columns.clone(),
        predicate_sql: index.predicate_sql.clone(),
        full_text_options_json: full_text_options_json(index),
        fresh: index.fresh,
    }
}

pub(super) fn view_info(view: &ViewSchema) -> ViewInfo {
    ViewInfo {
        name: view.name.clone(),
        temporary: view.temporary,
        sql_text: view.sql_text.clone(),
        column_names: view.column_names.clone(),
        dependencies: view.dependencies.clone(),
    }
}

pub(super) fn trigger_info(trigger: &TriggerSchema) -> TriggerInfo {
    TriggerInfo {
        name: trigger.name.clone(),
        target_name: trigger.target_name.clone(),
        kind: trigger_kind_name(trigger.kind).to_string(),
        event: trigger_event_name(trigger.event).to_string(),
        on_view: trigger.on_view,
        action_sql: trigger.action_sql.clone(),
    }
}

pub(super) fn schema_snapshot(db: &Db, runtime: &EngineRuntime) -> Result<SchemaSnapshot> {
    let mut tables = Vec::with_capacity(runtime.catalog.tables.len() + runtime.temp_tables.len());
    for table in runtime.catalog.tables.values() {
        if crate::sync::is_internal_table_name(&table.name) {
            continue;
        }
        tables.push(schema_table_info(
            table,
            db.runtime_table_row_count(runtime, &table.name, None)?,
        ));
    }
    for table in runtime.temp_tables.values() {
        tables.push(schema_table_info(
            table,
            db.runtime_table_row_count(runtime, &table.name, None)?,
        ));
    }
    tables.sort_by(|left, right| left.name.cmp(&right.name));

    let mut views = runtime
        .catalog
        .views
        .values()
        .map(schema_view_info)
        .collect::<Vec<_>>();
    views.extend(runtime.temp_views.values().map(schema_view_info));
    views.sort_by(|left, right| left.name.cmp(&right.name));

    let mut indexes = runtime
        .catalog
        .indexes
        .values()
        .map(schema_index_info)
        .collect::<Vec<_>>();
    indexes.sort_by(|left, right| left.name.cmp(&right.name));

    let mut triggers = runtime
        .catalog
        .triggers
        .values()
        .map(schema_trigger_info)
        .collect::<Vec<_>>();
    triggers.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(SchemaSnapshot {
        snapshot_version: 1,
        schema_cookie: runtime.catalog.schema_cookie,
        tables,
        views,
        indexes,
        triggers,
    })
}

pub(super) fn schema_table_info(table: &TableSchema, row_count: usize) -> SchemaTableInfo {
    SchemaTableInfo {
        name: table.name.clone(),
        temporary: table.temporary,
        ddl: render_create_table(table),
        row_count,
        primary_key_columns: table.primary_key_columns.clone(),
        checks: table.checks.iter().map(check_constraint_info).collect(),
        foreign_keys: table.foreign_keys.iter().map(foreign_key_info).collect(),
        columns: table.columns.iter().map(schema_column_info).collect(),
    }
}

pub(super) fn schema_column_info(column: &ColumnSchema) -> SchemaColumnInfo {
    SchemaColumnInfo {
        name: column.name.clone(),
        column_type: column.column_type.as_str().to_string(),
        nullable: column.nullable,
        default_sql: column.default_sql.clone(),
        primary_key: column.primary_key,
        unique: column.unique,
        auto_increment: column.auto_increment,
        generated_sql: column.generated_sql.clone(),
        generated_stored: column.generated_stored,
        checks: column.checks.iter().map(check_constraint_info).collect(),
        foreign_key: column.foreign_key.as_ref().map(foreign_key_info),
    }
}

pub(super) fn check_constraint_info(check: &CheckConstraint) -> CheckConstraintInfo {
    CheckConstraintInfo {
        name: check.name.clone(),
        expression_sql: check.expression_sql.clone(),
    }
}

pub(super) fn schema_view_info(view: &ViewSchema) -> SchemaViewInfo {
    SchemaViewInfo {
        name: view.name.clone(),
        temporary: view.temporary,
        sql_text: view.sql_text.clone(),
        column_names: view.column_names.clone(),
        dependencies: view.dependencies.clone(),
        ddl: render_create_view(view),
    }
}

pub(super) fn schema_index_info(index: &IndexSchema) -> SchemaIndexInfo {
    SchemaIndexInfo {
        name: index.name.clone(),
        table_name: index.table_name.clone(),
        kind: match index.kind {
            IndexKind::Btree => "btree",
            IndexKind::Trigram => "trigram",
            IndexKind::Spatial => "spatial",
            IndexKind::FullText => "fulltext",
        }
        .to_string(),
        unique: index.unique,
        columns: index.columns.iter().map(index_column_name).collect(),
        include_columns: index.include_columns.clone(),
        predicate_sql: index.predicate_sql.clone(),
        full_text_options_json: full_text_options_json(index),
        fresh: index.fresh,
        temporary: false,
        ddl: render_create_index(index),
    }
}

pub(super) fn full_text_options_json(index: &IndexSchema) -> Option<String> {
    index
        .full_text
        .as_ref()
        .and_then(|config| String::from_utf8(config.to_json().ok()?).ok())
}

pub(super) fn schema_trigger_info(trigger: &TriggerSchema) -> SchemaTriggerInfo {
    let event = trigger_event_name(trigger.event).to_ascii_lowercase();
    SchemaTriggerInfo {
        name: trigger.name.clone(),
        target_name: trigger.target_name.clone(),
        target_kind: if trigger.on_view {
            "view".to_string()
        } else {
            "table".to_string()
        },
        timing: match trigger.kind {
            TriggerKind::After => "after".to_string(),
            TriggerKind::InsteadOf => "instead_of".to_string(),
        },
        events: vec![event],
        events_mask: trigger_event_mask(trigger.event),
        for_each_row: true,
        temporary: false,
        action_sql: trigger.action_sql.clone(),
        ddl: render_create_trigger(trigger),
    }
}

pub(super) fn trigger_event_mask(event: TriggerEvent) -> u32 {
    match event {
        TriggerEvent::Insert => 1,
        TriggerEvent::Update => 2,
        TriggerEvent::Delete => 4,
    }
}

pub(super) fn index_column_name(column: &IndexColumn) -> String {
    if let Some(name) = &column.column_name {
        name.clone()
    } else if let Some(expression) = &column.expression_sql {
        expression.clone()
    } else {
        "<expr>".to_string()
    }
}

pub(super) fn foreign_key_action_name(action: ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::NoAction => "NO ACTION",
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::Cascade => "CASCADE",
        ForeignKeyAction::SetNull => "SET NULL",
    }
}

pub(super) fn trigger_kind_name(kind: TriggerKind) -> &'static str {
    match kind {
        TriggerKind::After => "AFTER",
        TriggerKind::InsteadOf => "INSTEAD OF",
    }
}

pub(super) fn trigger_event_name(event: TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update => "UPDATE",
        TriggerEvent::Delete => "DELETE",
    }
}

pub(super) fn runtime_index_entry_count(index: &RuntimeIndex) -> usize {
    match index {
        RuntimeIndex::Btree { keys, .. } => keys.total_row_id_count(),
        RuntimeIndex::Trigram { index } => index.entry_count(),
        RuntimeIndex::Spatial { index } => index.len(),
        RuntimeIndex::FullText { index } => index.entry_count(),
    }
}
