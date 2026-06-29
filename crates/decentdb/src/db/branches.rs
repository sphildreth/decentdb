use super::*;

pub(super) struct MergePlan {
    source: String,
    target: String,
    base_head_id: String,
    table_count: usize,
    pub(super) changes: Vec<MergeChangePlan>,
    pub(super) conflicts: Vec<crate::branch::BranchMergeConflict>,
}

impl MergePlan {
    pub(super) fn into_report(self, dry_run: bool) -> crate::branch::BranchMergeReport {
        crate::branch::BranchMergeReport {
            source: self.source,
            target: self.target,
            dry_run,
            clean: self.conflicts.is_empty(),
            base_head_id: self.base_head_id,
            table_count: self.table_count,
            applied_change_count: if dry_run || !self.conflicts.is_empty() {
                0
            } else {
                self.changes.len()
            },
            conflict_count: self.conflicts.len(),
            applied: if dry_run || !self.conflicts.is_empty() {
                Vec::new()
            } else {
                self.changes
                    .into_iter()
                    .map(|change| change.change)
                    .collect()
            },
            conflicts: self.conflicts,
        }
    }
}

pub(super) struct MergeChangePlan {
    change: crate::branch::BranchMergeChange,
    pub(super) sql: String,
}

pub(super) struct MergeTableInputs<'a> {
    base_db: &'a Db,
    source_db: &'a Db,
    target_db: &'a Db,
    base_table: &'a TableInfo,
    source_table: &'a TableInfo,
    target_table: &'a TableInfo,
}

pub(super) fn build_merge_plan(
    source_ref: &str,
    target_ref: &str,
    base_head_id: &str,
    base_db: &Db,
    source_db: &Db,
    target_db: &Db,
) -> Result<MergePlan> {
    let base_tables = table_info_map(base_db)?;
    let source_tables = table_info_map(source_db)?;
    let target_tables = table_info_map(target_db)?;
    let mut table_names = BTreeSet::new();
    table_names.extend(base_tables.keys().cloned());
    table_names.extend(source_tables.keys().cloned());
    table_names.extend(target_tables.keys().cloned());

    let mut changes = Vec::new();
    let mut conflicts = Vec::new();
    for table_name in &table_names {
        let Some(base_table) = base_tables.get(table_name) else {
            if source_tables.get(table_name) != target_tables.get(table_name) {
                conflicts.push(merge_conflict(
                    table_name,
                    Vec::new(),
                    "schema_change",
                    "merge does not support tables created after the branch base",
                ));
            }
            continue;
        };
        let Some(source_table) = source_tables.get(table_name) else {
            conflicts.push(merge_conflict(
                table_name,
                Vec::new(),
                "schema_change",
                "merge does not support dropped source tables",
            ));
            continue;
        };
        let Some(target_table) = target_tables.get(table_name) else {
            conflicts.push(merge_conflict(
                table_name,
                Vec::new(),
                "schema_change",
                "merge does not support dropped target tables",
            ));
            continue;
        };
        if !merge_table_schema_equal(base_table, source_table)
            || !merge_table_schema_equal(base_table, target_table)
        {
            conflicts.push(merge_conflict(
                table_name,
                Vec::new(),
                "schema_change",
                "merge supports identical table schemas only",
            ));
            continue;
        }
        if base_table.primary_key_columns.is_empty() {
            conflicts.push(merge_conflict(
                table_name,
                Vec::new(),
                "missing_primary_key",
                "merge requires primary-key tables",
            ));
            continue;
        }
        merge_table_rows(
            MergeTableInputs {
                base_db,
                source_db,
                target_db,
                base_table,
                source_table,
                target_table,
            },
            &mut changes,
            &mut conflicts,
        )?;
    }

    Ok(MergePlan {
        source: source_ref.to_string(),
        target: target_ref.to_string(),
        base_head_id: base_head_id.to_string(),
        table_count: table_names.len(),
        changes,
        conflicts,
    })
}

pub(super) fn table_info_map(db: &Db) -> Result<BTreeMap<String, TableInfo>> {
    Ok(db
        .list_tables()?
        .into_iter()
        .map(|table| (table.name.clone(), table))
        .collect())
}

pub(super) fn merge_table_rows(
    inputs: MergeTableInputs<'_>,
    changes: &mut Vec<MergeChangePlan>,
    conflicts: &mut Vec<crate::branch::BranchMergeConflict>,
) -> Result<()> {
    let base_rows = diff_table_rows(inputs.base_db, inputs.base_table)?;
    let source_rows = diff_table_rows(inputs.source_db, inputs.source_table)?;
    let target_rows = diff_table_rows(inputs.target_db, inputs.target_table)?;
    let mut row_keys = BTreeSet::new();
    row_keys.extend(base_rows.keys().cloned());
    row_keys.extend(source_rows.keys().cloned());
    row_keys.extend(target_rows.keys().cloned());

    for primary_key in row_keys {
        let base = base_rows.get(&primary_key);
        let source = source_rows.get(&primary_key);
        let target = target_rows.get(&primary_key);
        let source_changed = source != base;
        if !source_changed {
            continue;
        }
        let target_changed = target != base;
        if target_changed {
            if target == source {
                continue;
            }
            conflicts.push(merge_conflict(
                &inputs.base_table.name,
                primary_key,
                merge_conflict_type(base, source, target),
                "source and target changed the same primary-key row differently",
            ));
            continue;
        }
        let Some(change) =
            merge_change_for_source_delta(inputs.base_table, &primary_key, base, source)?
        else {
            continue;
        };
        changes.push(change);
    }
    Ok(())
}

pub(super) fn merge_change_for_source_delta(
    table: &TableInfo,
    primary_key: &[String],
    base: Option<&Vec<String>>,
    source: Option<&Vec<String>>,
) -> Result<Option<MergeChangePlan>> {
    match (base, source) {
        (None, Some(after)) => Ok(Some(MergeChangePlan {
            change: crate::branch::BranchMergeChange {
                table: table.name.clone(),
                primary_key: primary_key.to_vec(),
                operation: crate::branch::BranchMergeOperation::Insert,
            },
            sql: merge_insert_sql(table, after),
        })),
        (Some(_), None) => Ok(Some(MergeChangePlan {
            change: crate::branch::BranchMergeChange {
                table: table.name.clone(),
                primary_key: primary_key.to_vec(),
                operation: crate::branch::BranchMergeOperation::Delete,
            },
            sql: merge_delete_sql(table, primary_key)?,
        })),
        (Some(_), Some(after)) => {
            let Some(sql) = merge_update_sql(table, primary_key, after)? else {
                return Ok(None);
            };
            Ok(Some(MergeChangePlan {
                change: crate::branch::BranchMergeChange {
                    table: table.name.clone(),
                    primary_key: primary_key.to_vec(),
                    operation: crate::branch::BranchMergeOperation::Update,
                },
                sql,
            }))
        }
        (None, None) => Ok(None),
    }
}

pub(super) fn merge_insert_sql(table: &TableInfo, after: &[String]) -> String {
    let columns = table
        .columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({columns}) VALUES ({});",
        sql_identifier(&table.name),
        after.join(", ")
    )
}

pub(super) fn merge_delete_sql(table: &TableInfo, primary_key: &[String]) -> Result<String> {
    Ok(format!(
        "DELETE FROM {} WHERE {};",
        sql_identifier(&table.name),
        merge_where_clause(table, primary_key)?
    ))
}

pub(super) fn merge_update_sql(
    table: &TableInfo,
    primary_key: &[String],
    after: &[String],
) -> Result<Option<String>> {
    let assignments = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            !table
                .primary_key_columns
                .iter()
                .any(|primary_key| identifiers_equal(primary_key, &column.name))
        })
        .map(|(index, column)| format!("{} = {}", sql_identifier(&column.name), after[index]))
        .collect::<Vec<_>>();
    if assignments.is_empty() {
        return Ok(None);
    }
    Ok(Some(format!(
        "UPDATE {} SET {} WHERE {};",
        sql_identifier(&table.name),
        assignments.join(", "),
        merge_where_clause(table, primary_key)?
    )))
}

pub(super) fn merge_where_clause(table: &TableInfo, primary_key: &[String]) -> Result<String> {
    if primary_key.len() != table.primary_key_columns.len() {
        return Err(DbError::internal("merge primary-key arity mismatch"));
    }
    Ok(table
        .primary_key_columns
        .iter()
        .zip(primary_key.iter())
        .map(|(column, value)| format!("{} = {value}", sql_identifier(column)))
        .collect::<Vec<_>>()
        .join(" AND "))
}

pub(super) fn merge_conflict_type(
    base: Option<&Vec<String>>,
    source: Option<&Vec<String>>,
    target: Option<&Vec<String>>,
) -> &'static str {
    match (base, source, target) {
        (None, Some(_), Some(_)) => "duplicate_insert",
        (Some(_), None, Some(_)) => "delete_update",
        (Some(_), Some(_), None) => "update_delete",
        (Some(_), Some(_), Some(_)) => "update_update",
        _ => "row_conflict",
    }
}

pub(super) fn merge_conflict(
    table: &str,
    primary_key: Vec<String>,
    conflict_type: &str,
    message: &str,
) -> crate::branch::BranchMergeConflict {
    crate::branch::BranchMergeConflict {
        table: table.to_string(),
        primary_key,
        conflict_type: conflict_type.to_string(),
        message: message.to_string(),
    }
}

pub(super) fn merge_table_schema_equal(left: &TableInfo, right: &TableInfo) -> bool {
    left.columns == right.columns
        && left.checks == right.checks
        && left.foreign_keys == right.foreign_keys
        && left.primary_key_columns == right.primary_key_columns
}

pub(super) fn diff_materialized_refs(
    left_ref: &str,
    right_ref: &str,
    left_db: &Db,
    right_db: &Db,
) -> Result<crate::branch::BranchDiffReport> {
    let left_tables = left_db
        .list_tables()?
        .into_iter()
        .map(|table| (table.name.clone(), table))
        .collect::<BTreeMap<_, _>>();
    let right_tables = right_db
        .list_tables()?
        .into_iter()
        .map(|table| (table.name.clone(), table))
        .collect::<BTreeMap<_, _>>();
    let mut table_names = BTreeSet::new();
    table_names.extend(left_tables.keys().cloned());
    table_names.extend(right_tables.keys().cloned());

    let mut tables = Vec::new();
    for table_name in table_names {
        let table_diff = match (left_tables.get(&table_name), right_tables.get(&table_name)) {
            (None, Some(right_table)) => {
                let (added, message) = diff_rows_for_added_table(right_db, right_table)?;
                crate::branch::BranchTableDiff {
                    table: table_name,
                    status: crate::branch::BranchTableDiffStatus::Added,
                    schema_changed: true,
                    added,
                    updated: Vec::new(),
                    deleted: Vec::new(),
                    message,
                }
            }
            (Some(left_table), None) => {
                let (deleted, message) = diff_rows_for_removed_table(left_db, left_table)?;
                crate::branch::BranchTableDiff {
                    table: table_name,
                    status: crate::branch::BranchTableDiffStatus::Removed,
                    schema_changed: true,
                    added: Vec::new(),
                    updated: Vec::new(),
                    deleted,
                    message,
                }
            }
            (Some(left_table), Some(right_table)) => {
                diff_existing_table(left_db, right_db, left_table, right_table)?
            }
            (None, None) => continue,
        };
        tables.push(table_diff);
    }

    let added_row_count = tables.iter().map(|table| table.added.len()).sum();
    let updated_row_count = tables.iter().map(|table| table.updated.len()).sum();
    let deleted_row_count = tables.iter().map(|table| table.deleted.len()).sum();
    let changed_table_count = tables
        .iter()
        .filter(|table| table.status != crate::branch::BranchTableDiffStatus::Unchanged)
        .count();

    Ok(crate::branch::BranchDiffReport {
        left_ref: left_ref.to_string(),
        right_ref: right_ref.to_string(),
        table_count: tables.len(),
        changed_table_count,
        added_row_count,
        updated_row_count,
        deleted_row_count,
        tables,
    })
}

pub(super) fn diff_existing_table(
    left_db: &Db,
    right_db: &Db,
    left_table: &TableInfo,
    right_table: &TableInfo,
) -> Result<crate::branch::BranchTableDiff> {
    let schema_changed = left_table.columns != right_table.columns
        || left_table.checks != right_table.checks
        || left_table.foreign_keys != right_table.foreign_keys
        || left_table.primary_key_columns != right_table.primary_key_columns;
    if left_table.primary_key_columns.is_empty() || right_table.primary_key_columns.is_empty() {
        let changed = schema_changed || left_table.row_count != right_table.row_count;
        return Ok(crate::branch::BranchTableDiff {
            table: left_table.name.clone(),
            status: if changed {
                crate::branch::BranchTableDiffStatus::Unsupported
            } else {
                crate::branch::BranchTableDiffStatus::Unchanged
            },
            schema_changed,
            added: Vec::new(),
            updated: Vec::new(),
            deleted: Vec::new(),
            message: Some(
                "row diff requires a primary key; table-level metadata was compared only"
                    .to_string(),
            ),
        });
    }
    if left_table.primary_key_columns != right_table.primary_key_columns {
        return Ok(crate::branch::BranchTableDiff {
            table: left_table.name.clone(),
            status: crate::branch::BranchTableDiffStatus::Unsupported,
            schema_changed,
            added: Vec::new(),
            updated: Vec::new(),
            deleted: Vec::new(),
            message: Some("primary-key columns differ; row diff is not supported".to_string()),
        });
    }

    let left_rows = diff_table_rows(left_db, left_table)?;
    let right_rows = diff_table_rows(right_db, right_table)?;
    let mut keys = BTreeSet::new();
    keys.extend(left_rows.keys().cloned());
    keys.extend(right_rows.keys().cloned());

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut deleted = Vec::new();
    for key in keys {
        match (left_rows.get(&key), right_rows.get(&key)) {
            (None, Some(after)) => added.push(crate::branch::BranchRowDiff {
                primary_key: key,
                before: None,
                after: Some(after.clone()),
            }),
            (Some(before), None) => deleted.push(crate::branch::BranchRowDiff {
                primary_key: key,
                before: Some(before.clone()),
                after: None,
            }),
            (Some(before), Some(after)) if before != after => {
                updated.push(crate::branch::BranchRowDiff {
                    primary_key: key,
                    before: Some(before.clone()),
                    after: Some(after.clone()),
                });
            }
            _ => {}
        }
    }

    let changed = schema_changed || !added.is_empty() || !updated.is_empty() || !deleted.is_empty();
    Ok(crate::branch::BranchTableDiff {
        table: left_table.name.clone(),
        status: if changed {
            crate::branch::BranchTableDiffStatus::Changed
        } else {
            crate::branch::BranchTableDiffStatus::Unchanged
        },
        schema_changed,
        added,
        updated,
        deleted,
        message: None,
    })
}

pub(super) fn diff_rows_for_added_table(
    db: &Db,
    table: &TableInfo,
) -> Result<(Vec<crate::branch::BranchRowDiff>, Option<String>)> {
    if table.primary_key_columns.is_empty() {
        return Ok((
            Vec::new(),
            Some("row diff requires a primary key; table is reported as added".to_string()),
        ));
    }
    let rows = diff_table_rows(db, table)?
        .into_iter()
        .map(|(primary_key, after)| crate::branch::BranchRowDiff {
            primary_key,
            before: None,
            after: Some(after),
        })
        .collect();
    Ok((rows, None))
}

pub(super) fn diff_rows_for_removed_table(
    db: &Db,
    table: &TableInfo,
) -> Result<(Vec<crate::branch::BranchRowDiff>, Option<String>)> {
    if table.primary_key_columns.is_empty() {
        return Ok((
            Vec::new(),
            Some("row diff requires a primary key; table is reported as removed".to_string()),
        ));
    }
    let rows = diff_table_rows(db, table)?
        .into_iter()
        .map(|(primary_key, before)| crate::branch::BranchRowDiff {
            primary_key,
            before: Some(before),
            after: None,
        })
        .collect();
    Ok((rows, None))
}

pub(super) fn diff_table_rows(
    db: &Db,
    table: &TableInfo,
) -> Result<BTreeMap<Vec<String>, Vec<String>>> {
    let columns = table
        .columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let order_by = table
        .primary_key_columns
        .iter()
        .map(|column| sql_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {columns} FROM {} ORDER BY {order_by}",
        sql_identifier(&table.name)
    );
    let result = db.execute(&sql)?;
    let primary_key_indexes = table
        .primary_key_columns
        .iter()
        .map(|primary_key| {
            table
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, primary_key))
                .ok_or_else(|| {
                    DbError::corruption(format!(
                        "primary-key column '{primary_key}' missing from table '{}'",
                        table.name
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut rows = BTreeMap::new();
    for row in result.rows() {
        let values = row
            .values()
            .iter()
            .map(diff_value_string)
            .collect::<Vec<_>>();
        let primary_key = primary_key_indexes
            .iter()
            .map(|index| values[*index].clone())
            .collect::<Vec<_>>();
        rows.insert(primary_key, values);
    }
    Ok(rows)
}

pub(super) fn diff_value_string(value: &Value) -> String {
    render_value_sql(value)
}

pub(super) fn render_runtime_dump(
    db: &Db,
    runtime: &mut EngineRuntime,
    snapshot_lsn: Option<u64>,
) -> Result<String> {
    let mut lines = Vec::new();

    for table in runtime.catalog.tables.values() {
        lines.push(render_create_table(table));
    }
    let table_names = runtime.catalog.tables.keys().cloned().collect::<Vec<_>>();
    for table_name in table_names {
        db.ensure_inspection_table_row_source(runtime, &table_name, snapshot_lsn)?;
        let table = runtime
            .catalog
            .table(&table_name)
            .cloned()
            .ok_or_else(|| DbError::internal(format!("unknown table {table_name}")))?;
        let row_source = runtime.table_row_source(&table.name).ok_or_else(|| {
            DbError::internal(format!("table row source for {} is missing", table.name))
        })?;
        for row in row_source.rows() {
            lines.push(render_insert(&table, row?.values()));
        }
        db.redefer_inspection_table_row_source(runtime, &table_name, snapshot_lsn);
    }
    for view in runtime.catalog.views.values() {
        lines.push(render_create_view(view));
    }
    for table in runtime.temp_tables.values() {
        lines.push(render_create_table(table));
    }
    for (table_name, table_data) in runtime.temp_table_data.iter() {
        if let Some(table) = runtime.temp_tables.get(table_name) {
            for row in table_data.visible_rows() {
                lines.push(render_insert(table, &row.values));
            }
        }
    }
    for view in runtime.temp_views.values() {
        lines.push(render_create_view(view));
    }
    for index in runtime.catalog.indexes.values() {
        if runtime
            .catalog
            .table(&index.table_name)
            .is_some_and(|table| is_auto_table_index(table, index))
        {
            continue;
        }
        lines.push(render_create_index(index));
    }
    for trigger in runtime.catalog.triggers.values() {
        lines.push(render_create_trigger(trigger));
    }

    Ok(lines.join("\n"))
}

pub(super) fn render_create_table(table: &TableSchema) -> String {
    let mut definitions = Vec::new();
    for column in &table.columns {
        let mut definition = format!(
            "{} {}",
            sql_identifier(&column.name),
            render_column_type(column)
        );
        if !column.nullable {
            definition.push_str(" NOT NULL");
        }
        if column.primary_key {
            definition.push_str(" PRIMARY KEY");
        }
        if column.unique {
            definition.push_str(" UNIQUE");
        }
        if let Some(generated_sql) = &column.generated_sql {
            definition.push_str(" GENERATED ALWAYS AS (");
            definition.push_str(generated_sql);
            if column.generated_stored {
                definition.push_str(") STORED");
            } else {
                definition.push_str(") VIRTUAL");
            }
        } else if let Some(default_sql) = &column.default_sql {
            definition.push_str(" DEFAULT ");
            definition.push_str(default_sql);
        }
        for check in &column.checks {
            definition.push_str(" CHECK (");
            definition.push_str(&check.expression_sql);
            definition.push(')');
        }
        if let Some(foreign_key) = &column.foreign_key {
            definition.push(' ');
            definition.push_str(&render_foreign_key(foreign_key));
        }
        definitions.push(definition);
    }

    if table.primary_key_columns.len() > 1 {
        definitions.push(format!(
            "PRIMARY KEY ({})",
            table
                .primary_key_columns
                .iter()
                .map(|name| sql_identifier(name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    for foreign_key in &table.foreign_keys {
        definitions.push(render_foreign_key(foreign_key));
    }
    for check in &table.checks {
        definitions.push(format!("CHECK ({})", check.expression_sql));
    }

    format!(
        "CREATE {}TABLE {} ({});",
        if table.temporary { "TEMP " } else { "" },
        sql_identifier(&table.name),
        definitions.join(", ")
    )
}

pub(super) fn render_foreign_key(foreign_key: &ForeignKeyConstraint) -> String {
    let mut sql = String::new();
    if let Some(name) = &foreign_key.name {
        sql.push_str("CONSTRAINT ");
        sql.push_str(&sql_identifier(name));
        sql.push(' ');
    }
    sql.push_str("FOREIGN KEY (");
    sql.push_str(
        &foreign_key
            .columns
            .iter()
            .map(|name| sql_identifier(name))
            .collect::<Vec<_>>()
            .join(", "),
    );
    sql.push_str(") REFERENCES ");
    sql.push_str(&sql_identifier(&foreign_key.referenced_table));
    sql.push_str(" (");
    sql.push_str(
        &foreign_key
            .referenced_columns
            .iter()
            .map(|name| sql_identifier(name))
            .collect::<Vec<_>>()
            .join(", "),
    );
    sql.push(')');
    if foreign_key.on_delete != ForeignKeyAction::NoAction {
        sql.push_str(" ON DELETE ");
        sql.push_str(foreign_key_action_name(foreign_key.on_delete));
    }
    if foreign_key.on_update != ForeignKeyAction::NoAction {
        sql.push_str(" ON UPDATE ");
        sql.push_str(foreign_key_action_name(foreign_key.on_update));
    }
    sql
}

pub(super) fn render_insert(table: &TableSchema, values: &[Value]) -> String {
    let columns = table
        .columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let values = values
        .iter()
        .zip(table.columns.iter())
        .map(|(value, column)| render_column_value_sql(column, value))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({columns}) VALUES ({values});",
        sql_identifier(&table.name)
    )
}

pub(super) fn render_column_type(column: &crate::catalog::ColumnSchema) -> String {
    if column.column_type == crate::catalog::ColumnType::Enum {
        if let Some(enum_type) = &column.enum_type {
            let labels = enum_type
                .labels
                .iter()
                .map(|label| sql_string_literal(&label.label))
                .collect::<Vec<_>>()
                .join(", ");
            return format!("ENUM({labels})");
        }
    }
    column.column_type.as_str().to_string()
}

pub(super) fn render_column_value_sql(
    column: &crate::catalog::ColumnSchema,
    value: &Value,
) -> String {
    if let (
        crate::catalog::ColumnType::Enum,
        Some(enum_type),
        Value::Enum {
            enum_type_id,
            label_id,
        },
    ) = (column.column_type, &column.enum_type, value)
    {
        if *enum_type_id == enum_type.type_id {
            if let Some(label) = enum_type.label_for_id(*label_id) {
                return sql_string_literal(label);
            }
        }
    }
    render_value_sql(value)
}

pub(super) fn render_create_view(view: &ViewSchema) -> String {
    let columns = if view.column_names.is_empty() {
        String::new()
    } else {
        format!(
            " ({})",
            view.column_names
                .iter()
                .map(|name| sql_identifier(name))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "CREATE {}VIEW {}{columns} AS {};",
        if view.temporary { "TEMP " } else { "" },
        sql_identifier(&view.name),
        view.sql_text
    )
}

pub(super) fn render_create_index(index: &IndexSchema) -> String {
    let unique = if index.unique { "UNIQUE " } else { "" };
    let using = match index.kind {
        IndexKind::Btree => String::new(),
        IndexKind::Trigram => " USING trigram".to_string(),
        IndexKind::Spatial => " USING spatial".to_string(),
        IndexKind::FullText => " USING fulltext".to_string(),
    };
    let full_text_options = render_full_text_options(index.full_text.as_ref());
    let columns = index
        .columns
        .iter()
        .map(index_column_name)
        .collect::<Vec<_>>()
        .join(", ");
    let include = if index.include_columns.is_empty() {
        String::new()
    } else {
        format!(
            " INCLUDE ({})",
            index
                .include_columns
                .iter()
                .map(|column| sql_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let predicate = index
        .predicate_sql
        .as_ref()
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    format!(
        "CREATE {unique}INDEX {} ON {}{using} ({columns}){full_text_options}{include}{predicate};",
        sql_identifier(&index.name),
        sql_identifier(&index.table_name)
    )
}

pub(super) fn render_full_text_options(config: Option<&AnalyzerConfig>) -> String {
    let Some(config) = config else {
        return String::new();
    };
    let tokenizer = match config.tokenizer {
        AnalyzerTokenization::Unicode => "unicode",
    };
    let language = match config.language {
        AnalyzerLanguage::Simple => "simple",
        AnalyzerLanguage::English => "english",
    };
    let stopwords = match &config.stopwords {
        AnalyzerStopwords::None => "none".to_string(),
        AnalyzerStopwords::Builtin => "builtin".to_string(),
        AnalyzerStopwords::Custom(words) => words.join(","),
    };
    let stemming = match config.stemming {
        AnalyzerStemmer::None => "none",
        AnalyzerStemmer::English => "english",
    };
    let diacritics = match config.diacritics {
        AnalyzerDiacritics::Preserve => "preserve",
        AnalyzerDiacritics::Remove => "remove",
    };
    let prefix = if config.prefix.enabled {
        config
            .prefix
            .lengths
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(",")
    } else {
        "none".to_string()
    };
    format!(
        " WITH (tokenizer = {}, language = {}, stopwords = {}, stemming = {}, case_folded = {}, diacritics = {}, prefix = {})",
        sql_string_literal(tokenizer),
        sql_string_literal(language),
        sql_string_literal(&stopwords),
        sql_string_literal(stemming),
        if config.case_folded { "TRUE" } else { "FALSE" },
        sql_string_literal(diacritics),
        sql_string_literal(&prefix),
    )
}

pub(super) fn is_auto_table_index(table: &TableSchema, index: &IndexSchema) -> bool {
    let Some(column_names) = index_column_names(index) else {
        return false;
    };
    if !index.include_columns.is_empty() || index.predicate_sql.is_some() {
        return false;
    }
    if !identifiers_equal(&index.table_name, &table.name) || index.kind != IndexKind::Btree {
        return false;
    }
    if !table.primary_key_columns.is_empty()
        && index.unique
        && identifier_lists_equal(&column_names, &table.primary_key_columns)
        && identifiers_equal(
            &index.name,
            &dump_auto_index_name("pk", &table.name, &table.primary_key_columns),
        )
    {
        return true;
    }
    if column_names.len() == 1
        && index.unique
        && table.columns.iter().any(|column| {
            column.unique
                && !column.primary_key
                && identifiers_equal(&column.name, &column_names[0])
                && identifiers_equal(
                    &index.name,
                    &dump_auto_index_name("uq", &table.name, &column_names),
                )
        })
    {
        return true;
    }
    table.foreign_keys.iter().any(|foreign_key| {
        !index.unique
            && identifier_lists_equal(&column_names, &foreign_key.columns)
            && identifiers_equal(
                &index.name,
                &format!(
                    "{}_idx",
                    dump_auto_index_name("fk", &table.name, &foreign_key.columns)
                ),
            )
    })
}

pub(super) fn index_column_names(index: &IndexSchema) -> Option<Vec<String>> {
    index
        .columns
        .iter()
        .map(|column| column.column_name.clone())
        .collect()
}

pub(super) fn identifier_lists_equal(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| identifiers_equal(left, right))
}

pub(super) fn dump_auto_index_name(prefix: &str, table_name: &str, columns: &[String]) -> String {
    format!("{prefix}_{}_{}", table_name, columns.join("_"))
}

pub(super) fn render_create_trigger(trigger: &TriggerSchema) -> String {
    format!(
        "CREATE TRIGGER {} {} {} ON {} FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql({});",
        sql_identifier(&trigger.name),
        trigger_kind_name(trigger.kind),
        trigger_event_name(trigger.event),
        sql_identifier(&trigger.target_name),
        render_value_sql(&Value::Text(trigger.action_sql.clone()))
    )
}

pub(super) fn render_value_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => {
            if value.is_finite() {
                value.to_string()
            } else {
                "NULL".to_string()
            }
        }
        Value::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Text(value) => sql_string_literal(value),
        Value::Blob(value) => format!("X'{}'", hex_encode(value)),
        Value::Geometry(value) | Value::Geography(value) => format!("X'{}'", hex_encode(value)),
        Value::Decimal { scaled, scale } => decimal_sql_text(*scaled, *scale),
        Value::Uuid(value) => format!("X'{}'", hex_encode(value)),
        Value::TimestampMicros(value) => value.to_string(),
        Value::Enum {
            enum_type_id,
            label_id,
        } => format!("'{enum_type_id}:{label_id}'"),
        Value::IpAddr { family, addr } => match format_ip_addr(*family, addr) {
            Ok(value) => format!("'{value}'"),
            Err(_) => "NULL".to_string(),
        },
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => match format_cidr(*family, *prefix_len, network) {
            Ok(value) => format!("'{value}'"),
            Err(_) => "NULL".to_string(),
        },
        Value::MacAddr { len, bytes } => match format_mac_addr(*len, bytes) {
            Ok(value) => format!("'{value}'"),
            Err(_) => "NULL".to_string(),
        },
        Value::DateDays(days) => format!("'{}'", format_date_days(*days)),
        Value::TimeMicros(micros) => match format_time_micros(*micros) {
            Ok(value) => format!("'{value}'"),
            Err(_) => "NULL".to_string(),
        },
        Value::TimestampTzMicros(micros) => {
            format!("'{}'", format_timestamp_tz_micros(*micros))
        }
        Value::Interval {
            months,
            days,
            micros,
        } => format!("'{}'", format_interval(*months, *days, *micros)),
    }
}

pub(super) fn render_branch_parameter_value_sql(value: &Value) -> Result<String> {
    match value {
        Value::Decimal { scaled, scale } => Ok(format!(
            "CAST({} AS DECIMAL)",
            sql_string_literal(&decimal_sql_text(*scaled, *scale))
        )),
        Value::Uuid(value) => Ok(format!(
            "UUID_PARSE({})",
            sql_string_literal(&hex_encode(value))
        )),
        Value::TimestampMicros(value) => Ok(format!(
            "CAST({} AS TIMESTAMP)",
            sql_string_literal(&format_timestamp_tz_micros(*value))
        )),
        Value::TimestampTzMicros(value) => Ok(format!(
            "CAST({} AS TIMESTAMP WITH TIME ZONE)",
            sql_string_literal(&format_timestamp_tz_micros(*value))
        )),
        Value::DateDays(days) => Ok(format!(
            "CAST({} AS DATE)",
            sql_string_literal(&format_date_days(*days))
        )),
        Value::TimeMicros(micros) => Ok(format!(
            "CAST({} AS TIME)",
            sql_string_literal(&format_time_micros(*micros)?)
        )),
        Value::Interval {
            months,
            days,
            micros,
        } => Ok(format!(
            "CAST({} AS INTERVAL)",
            sql_string_literal(&format_interval(*months, *days, *micros))
        )),
        Value::IpAddr { family, addr } => Ok(format!(
            "CAST({} AS IPADDR)",
            sql_string_literal(&format_ip_addr(*family, addr)?)
        )),
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => Ok(format!(
            "CAST({} AS CIDR)",
            sql_string_literal(&format_cidr(*family, *prefix_len, network)?)
        )),
        Value::MacAddr { len, bytes } => Ok(format!(
            "CAST({} AS MACADDR)",
            sql_string_literal(&format_mac_addr(*len, bytes)?)
        )),
        Value::Geometry(value) => Ok(format!("ST_GeomFromWKB(X'{}')", hex_encode(value))),
        Value::Geography(value) => Ok(format!("ST_GeogFromWKB(X'{}')", hex_encode(value))),
        _ => Ok(render_value_sql(value)),
    }
}

pub(super) fn decimal_sql_text(scaled: i64, scale: u8) -> String {
    if scale == 0 {
        scaled.to_string()
    } else {
        let negative = scaled < 0;
        let digits = scaled.unsigned_abs().to_string();
        let scale = usize::from(scale);
        let padded = if digits.len() <= scale {
            format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
        } else {
            digits
        };
        let split = padded.len() - scale;
        let mut decimal = format!("{}.{}", &padded[..split], &padded[split..]);
        if negative {
            decimal.insert(0, '-');
        }
        decimal
    }
}

pub(super) fn sql_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
