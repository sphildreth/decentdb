//! Constraint enforcement helpers.

use crate::catalog::{identifiers_equal, ColumnSchema, IndexKind, IndexSchema, TableSchema};
use crate::error::{DbError, Result};
use crate::record::row::Row;
use crate::record::value::Value;
use crate::sql::ast::ConflictTarget;
use crate::sql::parser::parse_expression_sql;

use super::{
    compare_values, generated_columns_are_stored, row_satisfies_index_predicate, table_row_dataset,
    EngineRuntime, RuntimeBtreeKey, RuntimeIndex, StoredRow, TableRowRef,
};

impl EngineRuntime {
    pub(super) fn coerce_row_values(
        &self,
        table: &TableSchema,
        values: Vec<Value>,
    ) -> Result<Vec<Value>> {
        if values.len() != table.columns.len() {
            return Err(DbError::sql(format!(
                "table {} expects {} values but received {}",
                table.name,
                table.columns.len(),
                values.len()
            )));
        }
        table
            .columns
            .iter()
            .zip(values)
            .map(|(column, value)| coerce_value(column, value))
            .collect()
    }

    pub(super) fn default_value_for_column(
        &self,
        column: &ColumnSchema,
        params: &[Value],
    ) -> Result<Value> {
        let Some(default_sql) = &column.default_sql else {
            return Ok(Value::Null);
        };
        let expr = parse_expression_sql(default_sql)?;
        self.eval_expr(
            &expr,
            &super::row::Dataset::empty(),
            &[],
            params,
            &std::collections::BTreeMap::new(),
            None,
        )
    }

    pub(super) fn validate_row(
        &self,
        table_name: &str,
        row: &[Value],
        existing_row_id: Option<i64>,
        params: &[Value],
    ) -> Result<()> {
        self.validate_row_inner(table_name, row, existing_row_id, params, true)
    }

    /// Validates a row without checking foreign key constraints. Used during
    /// FK CASCADE / SET NULL actions where the parent row is being
    /// concurrently mutated and FK consistency is maintained by the caller.
    pub(super) fn validate_row_skip_fk(
        &self,
        table_name: &str,
        row: &[Value],
        existing_row_id: Option<i64>,
        params: &[Value],
    ) -> Result<()> {
        self.validate_row_inner(table_name, row, existing_row_id, params, false)
    }

    fn validate_row_inner(
        &self,
        table_name: &str,
        row: &[Value],
        existing_row_id: Option<i64>,
        params: &[Value],
        check_foreign_keys: bool,
    ) -> Result<()> {
        let table = self
            .table_schema(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
        let row_materialized = if generated_columns_are_stored(table) {
            std::borrow::Cow::Borrowed(row)
        } else {
            let mut materialized = row.to_vec();
            self.apply_virtual_generated_columns(table, &mut materialized)?;
            std::borrow::Cow::Owned(materialized)
        };
        let row_for_eval = row_materialized.as_ref();

        for (column, value) in table.columns.iter().zip(row_for_eval) {
            if !column.nullable && matches!(value, Value::Null) {
                return Err(DbError::constraint(format!(
                    "column {}.{} may not be NULL",
                    table.name, column.name
                )));
            }
            for check in &column.checks {
                self.assert_check(table, row_for_eval, &check.expression_sql, params)?;
            }
        }
        for check in &table.checks {
            self.assert_check(table, row_for_eval, &check.expression_sql, params)?;
        }

        for index in unique_indexes_for_table(self, table_name) {
            if !row_satisfies_index_predicate(self, index, table, row_for_eval)? {
                continue;
            }
            let candidate = index_values(self, index, table, row_for_eval)?;
            if candidate.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            if let Some(row_ids) = unique_index_row_ids(self, index, table, row_for_eval)? {
                if row_ids
                    .into_iter()
                    .any(|row_id| Some(row_id) != existing_row_id)
                {
                    return Err(DbError::constraint(format!(
                        "unique constraint {} on {} was violated",
                        index.name, table.name
                    )));
                }
                continue;
            }
            let Some(row_source) = self.visible_table_row_source(table_name) else {
                continue;
            };
            for existing in row_source.rows() {
                let existing = materialize_constraint_row(self, table, existing?)?;
                if Some(existing.row_id) == existing_row_id {
                    continue;
                }
                if !row_satisfies_index_predicate(self, index, table, &existing.values)? {
                    continue;
                }
                let existing_values = index_values(self, index, table, &existing.values)?;
                if existing_values
                    .iter()
                    .any(|value| matches!(value, Value::Null))
                {
                    continue;
                }
                if values_equal(&candidate, &existing_values)? {
                    return Err(DbError::constraint(format!(
                        "unique constraint {} on {} was violated",
                        index.name, table.name
                    )));
                }
            }
        }

        if !check_foreign_keys {
            return Ok(());
        }

        for foreign_key in &table.foreign_keys {
            let child_values = foreign_key
                .columns
                .iter()
                .map(|column_name| lookup_column_value(table, row_for_eval, column_name))
                .collect::<Result<Vec<_>>>()?;
            if child_values
                .iter()
                .any(|value| matches!(value, Value::Null))
            {
                continue;
            }
            let parent = self
                .catalog
                .tables
                .get(&foreign_key.referenced_table)
                .ok_or_else(|| {
                    DbError::constraint(format!(
                        "foreign key references unknown table {}",
                        foreign_key.referenced_table
                    ))
                })?;
            let referenced_columns = if foreign_key.referenced_columns.is_empty() {
                parent.primary_key_columns.clone()
            } else {
                foreign_key.referenced_columns.clone()
            };
            if referenced_columns.is_empty() {
                return Err(DbError::constraint(format!(
                    "foreign key {} must reference a primary or explicit parent key",
                    foreign_key
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("{}_fk", table.name))
                )));
            }
            if let Some(exists) = parent_exists_via_single_or_composite_index(
                self,
                parent,
                &foreign_key.referenced_table,
                &referenced_columns,
                &child_values,
            )? {
                if exists {
                    continue;
                }
                return Err(DbError::constraint(format!(
                    "foreign key on {} references missing parent row in {}",
                    table.name, foreign_key.referenced_table
                )));
            }
            let Some(parent_rows) = self.visible_table_row_source(&foreign_key.referenced_table)
            else {
                return Err(DbError::constraint(format!(
                    "foreign key parent table {} has no row store",
                    foreign_key.referenced_table
                )));
            };
            let mut exists = false;
            for parent_row in parent_rows.rows() {
                let parent_row = materialize_constraint_row(self, parent, parent_row?)?;
                let is_match = referenced_columns.iter().zip(&child_values).all(
                    |(column_name, child_value)| {
                        lookup_column_value(parent, &parent_row.values, column_name)
                            .and_then(|parent_value| compare_values(parent_value, child_value))
                            .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal)
                    },
                );
                if is_match {
                    exists = true;
                    break;
                }
            }
            if !exists {
                return Err(DbError::constraint(format!(
                    "foreign key on {} references missing parent row in {}",
                    table.name, foreign_key.referenced_table
                )));
            }
        }

        Ok(())
    }

    pub(super) fn find_conflicting_row(
        &self,
        table_name: &str,
        row: &[Value],
        target: &ConflictTarget,
    ) -> Result<Option<StoredRow>> {
        let table = self
            .table_schema(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
        let indexes = indexes_for_conflict_target(self, table_name, target)?;
        if indexes.is_empty() {
            return Ok(None);
        }
        let Some(table_row_source) = self.table_row_source(table_name) else {
            return Ok(None);
        };
        for index in indexes {
            if !row_satisfies_index_predicate(self, index, table, row)? {
                continue;
            }
            let candidate = index_values(self, index, table, row)?;
            if candidate.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            if let Some(row_ids) = unique_index_row_ids(self, index, table, row)? {
                for row_id in row_ids {
                    if let Some(existing) = table_row_source.row_by_id(row_id)? {
                        return Ok(Some(StoredRow {
                            row_id: existing.row_id(),
                            values: existing.values().to_vec(),
                        }));
                    }
                }
                continue;
            }
            for existing in table_row_source.rows() {
                let existing = existing?;
                let existing_values = existing.values();
                if row_satisfies_index_predicate(self, index, table, existing_values)? {
                    let existing_index_values = index_values(self, index, table, existing_values)?;
                    if values_equal(&candidate, &existing_index_values)? {
                        return Ok(Some(StoredRow {
                            row_id: existing.row_id(),
                            values: existing_values.to_vec(),
                        }));
                    }
                }
            }
        }
        Ok(None)
    }

    fn assert_check(
        &self,
        table: &TableSchema,
        row: &[Value],
        sql: &str,
        params: &[Value],
    ) -> Result<()> {
        let expr = parse_expression_sql(sql)?;
        let dataset = table_row_dataset(table, row, &table.name);
        match self.eval_expr(
            &expr,
            &dataset,
            row,
            params,
            &std::collections::BTreeMap::new(),
            None,
        )? {
            Value::Bool(false) => Err(DbError::constraint(format!(
                "CHECK constraint failed on table {}",
                table.name
            ))),
            _ => Ok(()),
        }
    }
}

pub(super) fn auto_index_name(prefix: &str, table_name: &str, columns: &[String]) -> String {
    format!("{prefix}_{}_{}", table_name, columns.join("_"))
}

fn coerce_value(column: &ColumnSchema, value: Value) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    super::cast_value(value, column.column_type)
}

fn unique_indexes_for_table<'a>(
    runtime: &'a EngineRuntime,
    table_name: &str,
) -> Vec<&'a IndexSchema> {
    if runtime.visible_table_is_temporary(table_name) {
        return runtime
            .temp_indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, table_name) && index.unique)
            .collect();
    }
    runtime
        .catalog
        .indexes
        .values()
        .filter(|index| identifiers_equal(&index.table_name, table_name) && index.unique)
        .collect()
}

fn unique_index_row_ids(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row: &[Value],
) -> Result<Option<Vec<i64>>> {
    if !index.fresh || index.kind != IndexKind::Btree {
        return Ok(None);
    }
    let Some(RuntimeIndex::Btree { keys }) = runtime.index(&index.name) else {
        return Ok(None);
    };
    let Some(key) = super::compute_index_key(runtime, index, table, row)? else {
        return Ok(Some(Vec::new()));
    };
    Ok(Some(keys.row_ids_for_key(&key)))
}

fn indexes_for_conflict_target<'a>(
    runtime: &'a EngineRuntime,
    table_name: &str,
    target: &ConflictTarget,
) -> Result<Vec<&'a IndexSchema>> {
    let indexes = unique_indexes_for_table(runtime, table_name);
    match target {
        ConflictTarget::Any => Ok(indexes),
        ConflictTarget::Columns(columns) => {
            let matches = indexes
                .into_iter()
                .filter(|index| {
                    index.columns.len() == columns.len()
                        && index
                            .columns
                            .iter()
                            .zip(columns)
                            .all(|(candidate, target)| {
                                candidate
                                    .column_name
                                    .as_deref()
                                    .is_some_and(|name| identifiers_equal(name, target))
                                    && candidate.expression_sql.is_none()
                            })
                })
                .collect::<Vec<_>>();
            if matches.is_empty() {
                return Err(DbError::constraint(format!(
                    "no unique index matches ON CONFLICT ({})",
                    columns.join(", ")
                )));
            }
            Ok(matches)
        }
        ConflictTarget::Constraint(name) => {
            let matches = indexes
                .into_iter()
                .filter(|index| index.name == *name)
                .collect::<Vec<_>>();
            if matches.is_empty() {
                return Err(DbError::constraint(format!(
                    "no unique constraint or index named {name}"
                )));
            }
            Ok(matches)
        }
    }
}

fn index_values(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row: &[Value],
) -> Result<Vec<Value>> {
    let dataset = table_row_dataset(table, row, &table.name);
    index
        .columns
        .iter()
        .map(|column| {
            if let Some(column_name) = &column.column_name {
                lookup_column_value(table, row, column_name).cloned()
            } else if let Some(sql) = &column.expression_sql {
                let expr = parse_expression_sql(sql)?;
                runtime.eval_expr(
                    &expr,
                    &dataset,
                    row,
                    &[],
                    &std::collections::BTreeMap::new(),
                    None,
                )
            } else {
                Err(DbError::constraint("index key definition is empty"))
            }
        })
        .collect()
}

fn lookup_column_value<'a>(
    table: &TableSchema,
    row: &'a [Value],
    column_name: &str,
) -> Result<&'a Value> {
    let index = table
        .columns
        .iter()
        .position(|column| identifiers_equal(&column.name, column_name))
        .ok_or_else(|| DbError::constraint(format!("unknown column {}", column_name)))?;
    row.get(index)
        .ok_or_else(|| DbError::internal("row is shorter than table schema"))
}

fn values_equal(left: &[Value], right: &[Value]) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        if compare_values(left, right)? != std::cmp::Ordering::Equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn parent_exists_via_single_or_composite_index(
    runtime: &EngineRuntime,
    _parent: &TableSchema,
    parent_table_name: &str,
    referenced_columns: &[String],
    child_values: &[&Value],
) -> Result<Option<bool>> {
    if referenced_columns.len() != child_values.len() || referenced_columns.is_empty() {
        return Ok(None);
    }
    let Some(index) = unique_indexes_for_table(runtime, parent_table_name)
        .into_iter()
        .find(|index| {
            index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == referenced_columns.len()
                && index.columns.iter().zip(referenced_columns).all(
                    |(index_column, referenced_column)| {
                        index_column.expression_sql.is_none()
                            && index_column
                                .column_name
                                .as_ref()
                                .is_some_and(|name| identifiers_equal(name, referenced_column))
                    },
                )
        })
    else {
        return Ok(None);
    };
    let Some(RuntimeIndex::Btree { keys }) = runtime.index(&index.name) else {
        return Ok(None);
    };
    let matched_row_ids = if child_values.len() == 1 {
        keys.row_ids_for_value(child_values[0])?
    } else {
        keys.row_ids_for_key(&RuntimeBtreeKey::Encoded(
            Row::new(child_values.iter().map(|value| (*value).clone()).collect()).encode()?,
        ))
    };
    if matched_row_ids.is_empty() {
        return Ok(Some(false));
    }
    let Some(parent_rows) = runtime.visible_table_row_source(parent_table_name) else {
        return Ok(Some(false));
    };
    for row_id in matched_row_ids {
        if parent_rows.row_by_id(row_id)?.is_some() {
            return Ok(Some(true));
        }
    }
    Ok(Some(false))
}

fn materialize_constraint_row(
    runtime: &EngineRuntime,
    table: &TableSchema,
    row: TableRowRef<'_>,
) -> Result<StoredRow> {
    let mut values = row.values().to_vec();
    if !generated_columns_are_stored(table) {
        runtime.apply_virtual_generated_columns(table, &mut values)?;
    }
    Ok(StoredRow {
        row_id: row.row_id(),
        values,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::{ColumnSchema, ForeignKeyAction, ForeignKeyConstraint, IndexColumn};

    fn paged_row_source(rows: Vec<StoredRow>) -> super::super::TableRowSource {
        let payload = super::super::encode_table_payload(&crate::exec::TableData { rows })
            .expect("encode paged test payload");
        let manifest = super::super::TablePageManifest::from_payload(Arc::new(payload))
            .expect("build paged test manifest");
        super::super::TableRowSource::Paged(Arc::new(manifest))
    }

    fn int_column(name: &str, primary_key: bool, nullable: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            column_type: crate::catalog::ColumnType::Int64,
            nullable,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key,
            unique: false,
            auto_increment: false,
            checks: vec![],
            foreign_key: None,
        }
    }

    #[test]
    fn validate_row_unique_fallback_scans_paged_row_source() {
        let mut runtime = EngineRuntime::empty(1);
        let table = TableSchema {
            name: "items".to_string(),
            temporary: false,
            columns: vec![
                int_column("id", true, false),
                int_column("code", false, false),
            ],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 2,
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(table.name.clone(), table.clone());
        runtime.catalog_mut().indexes.insert(
            "items_code_unique".to_string(),
            IndexSchema {
                name: "items_code_unique".to_string(),
                table_name: "items".to_string(),
                kind: IndexKind::Btree,
                unique: true,
                columns: vec![IndexColumn {
                    column_name: Some("code".to_string()),
                    expression_sql: None,
                }],
                include_columns: vec![],
                predicate_sql: None,
                fresh: false,
            },
        );
        runtime.tables_mut().insert(
            "items".to_string(),
            paged_row_source(vec![StoredRow {
                row_id: 1,
                values: vec![Value::Int64(1), Value::Int64(7)],
            }]),
        );

        let err = runtime
            .validate_row("items", &[Value::Int64(2), Value::Int64(7)], None, &[])
            .expect_err("duplicate code should violate unique constraint");
        assert!(err.to_string().contains("unique constraint"));
        assert!(matches!(
            runtime.table_row_source("items"),
            Some(super::super::TableRowSource::Paged(_))
        ));
    }

    #[test]
    fn validate_row_foreign_key_fallback_scans_paged_parent_source() {
        let mut runtime = EngineRuntime::empty(1);
        let parent = TableSchema {
            name: "parent".to_string(),
            temporary: false,
            columns: vec![
                int_column("id", true, false),
                int_column("code", false, false),
            ],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 2,
            pk_index_root: None,
        };
        let child = TableSchema {
            name: "child".to_string(),
            temporary: false,
            columns: vec![
                int_column("id", true, false),
                int_column("parent_code", false, false),
            ],
            checks: vec![],
            foreign_keys: vec![ForeignKeyConstraint {
                name: None,
                columns: vec!["parent_code".to_string()],
                referenced_table: "parent".to_string(),
                referenced_columns: vec!["code".to_string()],
                on_delete: ForeignKeyAction::Restrict,
                on_update: ForeignKeyAction::Restrict,
            }],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(parent.name.clone(), parent.clone());
        runtime
            .catalog_mut()
            .tables
            .insert(child.name.clone(), child.clone());
        runtime.tables_mut().insert(
            "parent".to_string(),
            paged_row_source(vec![StoredRow {
                row_id: 1,
                values: vec![Value::Int64(1), Value::Int64(42)],
            }]),
        );

        runtime
            .validate_row("child", &[Value::Int64(1), Value::Int64(42)], None, &[])
            .expect("matching parent row should satisfy foreign key");
        assert!(matches!(
            runtime.table_row_source("parent"),
            Some(super::super::TableRowSource::Paged(_))
        ));
    }

    #[test]
    fn validate_row_foreign_key_composite_index_checks_paged_parent_source() {
        let mut runtime = EngineRuntime::empty(1);
        let parent = TableSchema {
            name: "parent".to_string(),
            temporary: false,
            columns: vec![int_column("a", false, false), int_column("b", false, false)],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["a".to_string(), "b".to_string()],
            next_row_id: 2,
            pk_index_root: None,
        };
        let child = TableSchema {
            name: "child".to_string(),
            temporary: false,
            columns: vec![
                int_column("id", true, false),
                int_column("parent_a", false, false),
                int_column("parent_b", false, false),
            ],
            checks: vec![],
            foreign_keys: vec![ForeignKeyConstraint {
                name: None,
                columns: vec!["parent_a".to_string(), "parent_b".to_string()],
                referenced_table: "parent".to_string(),
                referenced_columns: vec!["a".to_string(), "b".to_string()],
                on_delete: ForeignKeyAction::Restrict,
                on_update: ForeignKeyAction::Restrict,
            }],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(parent.name.clone(), parent.clone());
        runtime
            .catalog_mut()
            .tables
            .insert(child.name.clone(), child.clone());
        runtime.catalog_mut().indexes.insert(
            "parent_ab_unique".to_string(),
            IndexSchema {
                name: "parent_ab_unique".to_string(),
                table_name: "parent".to_string(),
                kind: IndexKind::Btree,
                unique: true,
                columns: vec![
                    IndexColumn {
                        column_name: Some("a".to_string()),
                        expression_sql: None,
                    },
                    IndexColumn {
                        column_name: Some("b".to_string()),
                        expression_sql: None,
                    },
                ],
                include_columns: vec![],
                predicate_sql: None,
                fresh: true,
            },
        );
        let mut entries = std::collections::BTreeMap::new();
        entries.insert(
            crate::record::row::Row::new(vec![Value::Int64(42), Value::Int64(7)])
                .encode()
                .expect("encode composite parent key"),
            1,
        );
        runtime.indexes_mut().insert(
            "parent_ab_unique".to_string(),
            Arc::new(RuntimeIndex::Btree {
                keys: super::super::RuntimeBtreeKeys::UniqueEncoded(entries),
            }),
        );
        runtime.tables_mut().insert(
            "parent".to_string(),
            paged_row_source(vec![StoredRow {
                row_id: 1,
                values: vec![Value::Int64(42), Value::Int64(7)],
            }]),
        );

        runtime
            .validate_row(
                "child",
                &[Value::Int64(1), Value::Int64(42), Value::Int64(7)],
                None,
                &[],
            )
            .expect("matching composite parent row should satisfy foreign key");
        assert!(matches!(
            runtime.table_row_source("parent"),
            Some(super::super::TableRowSource::Paged(_))
        ));
    }
}
