//! Constraint enforcement helpers.

use crate::catalog::{ColumnSchema, IndexKind, IndexSchema, TableSchema};
use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::sql::ast::ConflictTarget;
use crate::sql::parser::parse_expression_sql;

use super::{compare_values, table_row_dataset, EngineRuntime, RuntimeIndex, StoredRow};

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
        let table = self
            .catalog
            .tables
            .get(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;

        for (column, value) in table.columns.iter().zip(row) {
            if !column.nullable && matches!(value, Value::Null) {
                return Err(DbError::constraint(format!(
                    "column {}.{} may not be NULL",
                    table.name, column.name
                )));
            }
            for check in &column.checks {
                self.assert_check(table, row, &check.expression_sql, params)?;
            }
        }
        for check in &table.checks {
            self.assert_check(table, row, &check.expression_sql, params)?;
        }

        for index in unique_indexes_for_table(self, table_name) {
            let candidate = index_values(self, index, table, row)?;
            if candidate.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            if let Some(row_ids) = unique_index_row_ids(self, index, table, row)? {
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
            let rows = self
                .tables
                .get(table_name)
                .map(|data| data.rows.as_slice())
                .unwrap_or(&[]);
            for existing in rows {
                if Some(existing.row_id) == existing_row_id {
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

        for foreign_key in &table.foreign_keys {
            let child_values = foreign_key
                .columns
                .iter()
                .map(|column_name| lookup_column_value(table, row, column_name))
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
            let Some(parent_rows) = self.tables.get(&foreign_key.referenced_table) else {
                return Err(DbError::constraint(format!(
                    "foreign key parent table {} has no row store",
                    foreign_key.referenced_table
                )));
            };
            let exists = parent_rows.rows.iter().any(|parent_row| {
                referenced_columns
                    .iter()
                    .zip(&child_values)
                    .all(|(column_name, child_value)| {
                        lookup_column_value(parent, &parent_row.values, column_name)
                            .and_then(|parent_value| compare_values(parent_value, child_value))
                            .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal)
                    })
            });
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
            .catalog
            .tables
            .get(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
        let indexes = indexes_for_conflict_target(self, table_name, target)?;
        if indexes.is_empty() {
            return Ok(None);
        }
        let Some(table_data) = self.tables.get(table_name) else {
            return Ok(None);
        };
        for index in indexes {
            let candidate = index_values(self, index, table, row)?;
            if candidate.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            if let Some(row_ids) = unique_index_row_ids(self, index, table, row)? {
                if let Some(existing) = table_data
                    .rows
                    .iter()
                    .find(|existing| row_ids.contains(&existing.row_id))
                {
                    return Ok(Some(existing.clone()));
                }
                continue;
            }
            if let Some(existing) = table_data.rows.iter().find(|existing| {
                index_values(self, index, table, &existing.values)
                    .and_then(|existing_values| values_equal(&candidate, &existing_values))
                    .unwrap_or(false)
            }) {
                return Ok(Some(existing.clone()));
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
    runtime
        .catalog
        .indexes
        .values()
        .filter(|index| index.table_name == table_name && index.unique)
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
    let Some(RuntimeIndex::Btree { keys }) = runtime.indexes.get(&index.name) else {
        return Ok(None);
    };
    let probe = StoredRow {
        row_id: 0,
        values: row.to_vec(),
    };
    let Some(key) = super::compute_index_key(runtime, index, table, &probe)? else {
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
                                candidate.column_name.as_deref() == Some(target.as_str())
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
        .position(|column| column.name == column_name)
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
