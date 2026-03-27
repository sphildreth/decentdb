//! DML execution helpers.

use std::borrow::Cow;

use crate::catalog::{
    identifiers_equal, ColumnType, ForeignKeyAction, ForeignKeyConstraint, IndexKind, TriggerEvent,
};
use crate::error::{DbError, Result};
use crate::record::key::encode_index_key;
use crate::record::row::Row;
use crate::record::value::Value;
use crate::sql::ast::{
    Assignment, BinaryOp, ConflictAction, ConflictTarget, DeleteStatement, Expr, InsertSource,
    InsertStatement, SelectItem, UpdateStatement,
};
use crate::sql::parser::parse_expression_sql;

use super::row::{ColumnBinding, Dataset, QueryResult, QueryRow};
use super::{
    compare_values, compute_index_key, compute_index_values, generated_columns_are_stored,
    row_satisfies_index_predicate, table_row_dataset, EngineRuntime, RuntimeBtreeKey, RuntimeIndex,
    RuntimeRowIdSet, StoredRow,
};

#[derive(Clone, Debug)]
pub(crate) enum PreparedInsertValueSource {
    Literal(Value),
    Parameter(usize),
    DefaultExpr(Expr),
    Null,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedBtreeIndex {
    pub(crate) name: String,
    pub(crate) column_indexes: Vec<usize>,
    pub(crate) int64_key: bool,
    pub(crate) nullable: bool,
    pub(crate) unique: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedInsertColumn {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) auto_increment: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedRequiredColumn {
    pub(crate) index: usize,
    pub(crate) name: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSingleColumnForeignKey {
    pub(crate) child_column_index: usize,
    pub(crate) parent_table_name: String,
    pub(crate) parent_index_name: String,
    pub(crate) parent_index_int64_key: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum PreparedSimpleValueSource {
    Literal(Value),
    Parameter(usize),
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSimpleInsert {
    pub(crate) table_name: String,
    pub(crate) columns: Vec<PreparedInsertColumn>,
    pub(crate) primary_auto_row_id_column_index: Option<usize>,
    pub(crate) value_sources: Vec<PreparedInsertValueSource>,
    pub(crate) required_columns: Vec<PreparedRequiredColumn>,
    pub(crate) foreign_keys: Vec<PreparedSingleColumnForeignKey>,
    pub(crate) unique_indexes: Vec<PreparedBtreeIndex>,
    pub(crate) insert_indexes: Vec<PreparedBtreeIndex>,
    pub(crate) use_generic_validation: bool,
    pub(crate) use_generic_index_updates: bool,
    pub(crate) compiled_index_state_epoch: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSimpleUpdate {
    pub(crate) table_name: String,
    pub(crate) column_index: usize,
    pub(crate) column_type: ColumnType,
    pub(crate) nullable: bool,
    pub(crate) row_id_source: PreparedSimpleValueSource,
    pub(crate) value_source: PreparedSimpleValueSource,
}

#[derive(Clone, Debug)]
pub(crate) enum PreparedDeleteLookup {
    RowId(PreparedSimpleValueSource),
    Index {
        index_name: String,
        value_source: PreparedSimpleValueSource,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSimpleDeleteRestrictChild {
    pub(crate) child_table_name: String,
    pub(crate) child_column_index: usize,
    pub(crate) child_index_name: Option<String>,
    pub(crate) parent_column_index: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSimpleDelete {
    pub(crate) table: crate::catalog::TableSchema,
    pub(crate) indexes: Vec<crate::catalog::IndexSchema>,
    pub(crate) lookup: PreparedDeleteLookup,
    pub(crate) restrict_children: Vec<PreparedSimpleDeleteRestrictChild>,
    pub(crate) compiled_index_state_epoch: u64,
}

impl EngineRuntime {
    pub(crate) fn can_execute_statement_in_state_without_clone(
        &self,
        statement: &crate::sql::ast::Statement,
    ) -> bool {
        match statement {
            crate::sql::ast::Statement::Update(update) => {
                self.can_execute_update_in_state_without_clone(update)
            }
            crate::sql::ast::Statement::Delete(delete) => {
                self.can_execute_delete_in_state_without_clone(delete)
            }
            _ => false,
        }
    }

    fn can_execute_update_in_state_without_clone(&self, statement: &UpdateStatement) -> bool {
        if !statement.returning.is_empty() {
            return false;
        }
        if self
            .visible_view(&statement.table_name, super::NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(&statement.table_name)
        {
            return false;
        }
        if self.has_table_trigger(&statement.table_name, TriggerEvent::Update) {
            return false;
        }
        let Some(table) = self.table_schema(&statement.table_name) else {
            return false;
        };
        if table.checks.iter().any(|_| true)
            || table
                .columns
                .iter()
                .any(|column| column.generated_sql.is_some() || !column.checks.is_empty())
        {
            return false;
        }
        if !statement
            .assignments
            .iter()
            .all(|assignment| matches!(assignment.expr, Expr::Literal(_) | Expr::Parameter(_)))
        {
            return false;
        }
        let Some(assignment_columns) = statement
            .assignments
            .iter()
            .map(|assignment| {
                table
                    .columns
                    .iter()
                    .position(|column| column.name == assignment.column_name)
            })
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        if statement.assignments.iter().zip(&assignment_columns).any(
            |(assignment, column_index)| {
                table.columns.get(*column_index).is_none_or(|column| {
                    column.generated_sql.is_some() || column.name != assignment.column_name
                })
            },
        ) {
            return false;
        }
        if assignment_targets_foreign_key_columns(table, &assignment_columns) {
            return false;
        }
        if assignment_targets_referenced_parent_key_columns(self, table, &assignment_columns) {
            return false;
        }
        let table_indexes = self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &statement.table_name))
            .collect::<Vec<_>>();
        if table_indexes.iter().any(|index| {
            !index.fresh
                || index.kind != IndexKind::Btree
                || index.predicate_sql.is_some()
                || index
                    .columns
                    .iter()
                    .any(|column| column.expression_sql.is_some())
        }) {
            return false;
        }
        !unique_indexes_for_table(self, table)
            .into_iter()
            .any(|index| index_might_change_for_assignments(table, index, &assignment_columns))
    }

    fn can_execute_delete_in_state_without_clone(&self, statement: &DeleteStatement) -> bool {
        if !statement.returning.is_empty() {
            return false;
        }
        if self
            .visible_view(&statement.table_name, super::NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(&statement.table_name)
        {
            return false;
        }
        if self.has_table_trigger(&statement.table_name, TriggerEvent::Delete) {
            return false;
        }
        let Some(table) = self.table_schema(&statement.table_name) else {
            return false;
        };
        if table_has_referencing_non_restrict_delete_actions(self, &statement.table_name) {
            return false;
        }
        let table_indexes = self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &statement.table_name))
            .collect::<Vec<_>>();
        if table_indexes.iter().any(|index| {
            !index.fresh
                || index.kind != IndexKind::Btree
                || index.predicate_sql.is_some()
                || index
                    .columns
                    .iter()
                    .any(|column| column.expression_sql.is_some())
        }) {
            return false;
        }
        !table.temporary
    }

    fn has_table_trigger(&self, table_name: &str, event: TriggerEvent) -> bool {
        self.catalog.triggers.values().any(|trigger| {
            !trigger.on_view
                && identifiers_equal(&trigger.target_name, table_name)
                && trigger.event == event
        })
    }

    pub(crate) fn can_execute_insert_in_place(&self, statement: &InsertStatement) -> bool {
        if self
            .visible_view(&statement.table_name, super::NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(&statement.table_name)
            || statement.on_conflict.is_some()
        {
            return false;
        }
        if self
            .table_schema(&statement.table_name)
            .is_some_and(|table| {
                table
                    .columns
                    .iter()
                    .any(|column| column.generated_sql.is_some())
            })
        {
            return false;
        }
        if !matches!(&statement.source, InsertSource::Values(rows) if rows.len() == 1) {
            return false;
        }
        !self.catalog.triggers.values().any(|trigger| {
            !trigger.on_view
                && trigger.target_name == statement.table_name
                && trigger.event == TriggerEvent::Insert
        })
    }

    pub(crate) fn can_reuse_prepared_simple_insert(&self, prepared: &PreparedSimpleInsert) -> bool {
        if self.visible_table_is_temporary(&prepared.table_name)
            || self.table_schema(&prepared.table_name).is_none()
        {
            return false;
        }
        if prepared.compiled_index_state_epoch == self.index_state_epoch {
            return true;
        }
        (prepared.use_generic_validation
            || prepared
                .unique_indexes
                .iter()
                .all(|index| self.prepared_btree_index_is_fresh(index))
                && prepared.foreign_keys.iter().all(|foreign_key| {
                    self.prepared_foreign_key_parent_index_is_fresh(foreign_key)
                }))
            && (prepared.use_generic_index_updates
                || prepared
                    .insert_indexes
                    .iter()
                    .all(|index| self.prepared_btree_index_is_fresh(index)))
    }

    pub(crate) fn prepare_simple_insert(
        &self,
        statement: &InsertStatement,
    ) -> Result<Option<PreparedSimpleInsert>> {
        if !self.can_execute_insert_in_place(statement) || !statement.returning.is_empty() {
            return Ok(None);
        }

        let table = self
            .table_schema(&statement.table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {}", statement.table_name)))?;
        let InsertSource::Values(rows) = &statement.source else {
            return Ok(None);
        };
        let source_exprs = rows
            .first()
            .ok_or_else(|| DbError::internal("simple insert expected one VALUES row"))?;
        let target_columns = if statement.columns.is_empty() {
            table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        } else {
            statement.columns.clone()
        };
        if target_columns.len() != source_exprs.len() {
            return Err(DbError::sql(format!(
                "INSERT on {} expected {} values but received {}",
                table.name,
                target_columns.len(),
                source_exprs.len()
            )));
        }

        let mut assigned = vec![None; table.columns.len()];
        for (column_name, expr) in target_columns.iter().zip(source_exprs) {
            let column_index = table
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, column_name))
                .ok_or_else(|| DbError::sql(format!("unknown column {}", column_name)))?;
            if assigned[column_index].is_some() {
                return Err(DbError::sql(format!(
                    "column {} was assigned more than once in INSERT",
                    column_name
                )));
            }
            let Some(source) = compile_prepared_insert_value_source(expr) else {
                return Ok(None);
            };
            assigned[column_index] = Some(source);
        }

        let mut value_sources = Vec::with_capacity(table.columns.len());
        for (index, column) in table.columns.iter().enumerate() {
            if let Some(source) = assigned[index].take() {
                value_sources.push(source);
                continue;
            }
            if let Some(default_sql) = &column.default_sql {
                value_sources.push(PreparedInsertValueSource::DefaultExpr(
                    parse_expression_sql(default_sql)?,
                ));
            } else {
                value_sources.push(PreparedInsertValueSource::Null);
            }
        }
        let columns = table
            .columns
            .iter()
            .map(|column| PreparedInsertColumn {
                name: column.name.clone(),
                column_type: column.column_type,
                auto_increment: column.auto_increment,
            })
            .collect::<Vec<_>>();
        let primary_auto_row_id_column_index = if table.primary_key_columns.len() == 1 {
            table.columns.iter().position(|column| {
                identifiers_equal(&column.name, &table.primary_key_columns[0])
                    && column.auto_increment
            })
        } else {
            None
        };

        let required_columns = table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                (!column.nullable).then_some(PreparedRequiredColumn {
                    index,
                    name: column.name.clone(),
                })
            })
            .collect::<Vec<_>>();
        let mut use_generic_validation =
            table.columns.iter().any(|column| !column.checks.is_empty())
                || !table.checks.is_empty();
        let mut foreign_keys = Vec::new();
        for foreign_key in &table.foreign_keys {
            let Some(prepared_foreign_key) =
                prepare_single_column_foreign_key(self, table, foreign_key)?
            else {
                use_generic_validation = true;
                foreign_keys.clear();
                break;
            };
            foreign_keys.push(prepared_foreign_key);
        }
        let mut unique_indexes = Vec::new();
        for index in self.catalog.indexes.values().filter(|index| {
            identifiers_equal(&index.table_name, &statement.table_name) && index.unique
        }) {
            let Some(prepared_index) = prepare_btree_insert_index(self, table, index)? else {
                use_generic_validation = true;
                unique_indexes.clear();
                break;
            };
            unique_indexes.push(prepared_index);
        }
        let mut use_generic_index_updates = false;
        let mut insert_indexes = Vec::new();
        for index in self.catalog.indexes.values().filter(|index| {
            identifiers_equal(&index.table_name, &statement.table_name) && index.fresh
        }) {
            let Some(prepared_index) = prepare_btree_insert_index(self, table, index)? else {
                use_generic_index_updates = true;
                insert_indexes.clear();
                break;
            };
            insert_indexes.push(prepared_index);
        }

        Ok(Some(PreparedSimpleInsert {
            table_name: statement.table_name.clone(),
            columns,
            primary_auto_row_id_column_index,
            value_sources,
            required_columns,
            foreign_keys,
            unique_indexes,
            insert_indexes,
            use_generic_validation,
            use_generic_index_updates,
            compiled_index_state_epoch: self.index_state_epoch,
        }))
    }

    pub(crate) fn prepare_simple_update(
        &self,
        statement: &UpdateStatement,
    ) -> Result<Option<PreparedSimpleUpdate>> {
        if !self.can_execute_update_in_state_without_clone(statement)
            || !statement.returning.is_empty()
            || statement.assignments.len() != 1
        {
            return Ok(None);
        }

        let table = self
            .table_schema(&statement.table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {}", statement.table_name)))?;
        let Some(filter) = statement.filter.as_ref() else {
            return Ok(None);
        };
        let Some((filter_table, column_name, value_expr)) = simple_btree_lookup_filter(filter)
        else {
            return Ok(None);
        };
        if filter_table.is_some_and(|name| !identifiers_equal(name, &table.name)) {
            return Ok(None);
        }
        if !row_id_alias_column_name(table).is_some_and(|name| identifiers_equal(name, column_name))
        {
            return Ok(None);
        }

        let assignment = &statement.assignments[0];
        let Some(value_source) = compile_prepared_simple_value_source(&assignment.expr) else {
            return Ok(None);
        };
        let Some(row_id_source) = compile_prepared_simple_value_source(value_expr) else {
            return Ok(None);
        };
        let Some(column_index) = table
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, &assignment.column_name))
        else {
            return Err(DbError::sql(format!(
                "unknown column {}",
                assignment.column_name
            )));
        };
        let column = &table.columns[column_index];
        if column.generated_sql.is_some() {
            return Ok(None);
        }

        let assignment_columns = [column_index];
        let index_changes = self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &table.name))
            .any(|index| index_might_change_for_assignments(table, index, &assignment_columns));
        if index_changes {
            return Ok(None);
        }

        Ok(Some(PreparedSimpleUpdate {
            table_name: table.name.clone(),
            column_index,
            column_type: column.column_type,
            nullable: column.nullable,
            row_id_source,
            value_source,
        }))
    }

    pub(crate) fn can_reuse_prepared_simple_update(&self, prepared: &PreparedSimpleUpdate) -> bool {
        // Schema cookie is validated before this is called, so the table exists.
        // Only check if it was shadowed by a temp table.
        !self.visible_table_is_temporary(&prepared.table_name)
    }

    pub(crate) fn execute_prepared_simple_update(
        &mut self,
        prepared: &PreparedSimpleUpdate,
        params: &[Value],
    ) -> Result<QueryResult> {
        let row_id = match resolve_prepared_simple_value(&prepared.row_id_source, params)? {
            Value::Int64(value) => value,
            _ => return Ok(QueryResult::with_affected_rows(0)),
        };
        let next_value = cast_prepared_simple_value(
            resolve_prepared_simple_value(&prepared.value_source, params)?,
            prepared.column_type,
        )?;
        if !prepared.nullable && matches!(next_value, Value::Null) {
            return Err(DbError::constraint(format!(
                "column {}.{} may not be NULL",
                prepared.table_name, prepared.column_index
            )));
        }

        let Some(table_data) = self.table_data_mut(&prepared.table_name) else {
            return Err(DbError::internal(format!(
                "table data for {} is missing",
                prepared.table_name
            )));
        };
        let Some(row_index) = table_data.row_index_by_id(row_id) else {
            return Ok(QueryResult::with_affected_rows(0));
        };
        let Some(current_value) = table_data.rows[row_index]
            .values
            .get(prepared.column_index)
        else {
            return Err(DbError::internal(format!(
                "column index {} is invalid for {}",
                prepared.column_index, prepared.table_name
            )));
        };
        if *current_value != next_value {
            table_data.rows[row_index].values[prepared.column_index] = next_value;
            self.mark_table_row_dirty(&prepared.table_name, row_index);
        }
        Ok(QueryResult::with_affected_rows(1))
    }

    pub(crate) fn prepare_simple_delete(
        &self,
        statement: &DeleteStatement,
    ) -> Result<Option<PreparedSimpleDelete>> {
        if !self.can_execute_delete_in_state_without_clone(statement)
            || !statement.returning.is_empty()
        {
            return Ok(None);
        }

        let table = self
            .table_schema(&statement.table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {}", statement.table_name)))?;
        let restrict_children = prepare_simple_delete_restrict_children(self, &table)?;

        let Some(filter) = statement.filter.as_ref() else {
            return Ok(None);
        };
        let Some((filter_table, column_name, value_expr)) = simple_btree_lookup_filter(filter)
        else {
            return Ok(None);
        };
        if filter_table.is_some_and(|name| !identifiers_equal(name, &table.name)) {
            return Ok(None);
        }
        let Some(value_source) = compile_prepared_simple_value_source(value_expr) else {
            return Ok(None);
        };

        let indexes = self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &table.name))
            .cloned()
            .collect::<Vec<_>>();
        let lookup = if row_id_alias_column_name(&table)
            .is_some_and(|name| identifiers_equal(name, column_name))
        {
            PreparedDeleteLookup::RowId(value_source)
        } else {
            let Some(index) = indexes.iter().find(|index| {
                index.fresh
                    && index.kind == IndexKind::Btree
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0].expression_sql.is_none()
                    && index.columns[0]
                        .column_name
                        .as_ref()
                        .is_some_and(|entry| identifiers_equal(entry, column_name))
            }) else {
                return Ok(None);
            };
            PreparedDeleteLookup::Index {
                index_name: index.name.clone(),
                value_source,
            }
        };

        Ok(Some(PreparedSimpleDelete {
            table,
            indexes,
            lookup,
            restrict_children,
            compiled_index_state_epoch: self.index_state_epoch,
        }))
    }

    pub(crate) fn can_reuse_prepared_simple_delete(&self, prepared: &PreparedSimpleDelete) -> bool {
        // Schema cookie is validated before this is called, so the table exists.
        // Only check temp-table shadowing and index state.
        !self.visible_table_is_temporary(&prepared.table.name)
            && prepared.compiled_index_state_epoch == self.index_state_epoch
            && prepared
                .restrict_children
                .iter()
                .all(|child| !self.visible_table_is_temporary(&child.child_table_name))
    }

    pub(crate) fn execute_prepared_simple_delete(
        &mut self,
        prepared: &PreparedSimpleDelete,
        params: &[Value],
    ) -> Result<QueryResult> {
        let matching_row_ids = match &prepared.lookup {
            PreparedDeleteLookup::RowId(value_source) => {
                let Value::Int64(row_id) = resolve_prepared_simple_value(value_source, params)?
                else {
                    return Ok(QueryResult::with_affected_rows(0));
                };
                match self.table_data(&prepared.table.name) {
                    Some(rows) if rows.row_index_by_id(row_id).is_some() => vec![row_id],
                    _ => Vec::new(),
                }
            }
            PreparedDeleteLookup::Index {
                index_name,
                value_source,
            } => {
                let value = resolve_prepared_simple_value(value_source, params)?;
                if matches!(value, Value::Null) {
                    return Ok(QueryResult::with_affected_rows(0));
                }
                let Some(RuntimeIndex::Btree { keys }) = self.indexes.get(index_name) else {
                    return Ok(QueryResult::with_affected_rows(0));
                };
                row_id_set_to_vec(keys.row_ids_for_value_set(&value)?)
            }
        };
        if matching_row_ids.is_empty() {
            return Ok(QueryResult::with_affected_rows(0));
        }

        let mut row_indices = {
            let table_data = self.table_data(&prepared.table.name).ok_or_else(|| {
                DbError::internal(format!("table data for {} is missing", prepared.table.name))
            })?;
            let mut indices = Vec::with_capacity(matching_row_ids.len());
            for &row_id in &matching_row_ids {
                let row_index = table_data.row_index_by_id(row_id).ok_or_else(|| {
                    DbError::internal(format!("row {row_id} vanished during DELETE"))
                })?;
                indices.push(row_index);
            }
            indices
        };

        if !prepared.restrict_children.is_empty() {
            let table_data = self.table_data(&prepared.table.name).ok_or_else(|| {
                DbError::internal(format!("table data for {} is missing", prepared.table.name))
            })?;
            for &row_index in &row_indices {
                for child in &prepared.restrict_children {
                    if prepared_delete_has_referencing_child(
                        self,
                        child,
                        &table_data.rows[row_index].values,
                    )? {
                        return Err(DbError::constraint(format!(
                            "DELETE on {} violates a foreign key from {}",
                            prepared.table.name, child.child_table_name
                        )));
                    }
                }
            }
        }

        row_indices.sort_unstable_by(|left, right| right.cmp(left));
        let removed_rows = {
            let table_data = self.table_data_mut(&prepared.table.name).ok_or_else(|| {
                DbError::internal(format!("table data for {} is missing", prepared.table.name))
            })?;
            let mut removed = Vec::with_capacity(row_indices.len());
            for &row_index in &row_indices {
                removed.push(table_data.rows.remove(row_index));
            }
            removed
        };

        for row in &removed_rows {
            for index in &prepared.indexes {
                apply_runtime_index_delete_for_row(
                    self,
                    &prepared.table,
                    index,
                    row.row_id,
                    &row.values,
                )?;
            }
        }

        self.mark_table_dirty(&prepared.table.name);
        Ok(QueryResult::with_affected_rows(removed_rows.len() as u64))
    }

    fn prepared_btree_index_is_fresh(&self, prepared: &PreparedBtreeIndex) -> bool {
        matches!(
            self.catalog.indexes.get(&prepared.name),
            Some(index) if index.kind == IndexKind::Btree && index.fresh
        ) && matches!(
            self.indexes.get(&prepared.name),
            Some(super::RuntimeIndex::Btree { .. })
        )
    }

    fn prepared_foreign_key_parent_index_is_fresh(
        &self,
        prepared: &PreparedSingleColumnForeignKey,
    ) -> bool {
        matches!(
            self.catalog.indexes.get(&prepared.parent_index_name),
            Some(index) if index.kind == IndexKind::Btree && index.fresh
        ) && matches!(
            self.indexes.get(&prepared.parent_index_name),
            Some(super::RuntimeIndex::Btree { .. })
        )
    }

    pub(crate) fn execute_prepared_simple_insert(
        &mut self,
        prepared: &PreparedSimpleInsert,
        params: &[Value],
        page_size: u32,
    ) -> Result<QueryResult> {
        let table_name = prepared.table_name.as_str();
        let mut next_row_id = self
            .catalog
            .tables
            .get(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?
            .next_row_id;
        let mut candidate = Vec::with_capacity(prepared.columns.len());

        for (index, source) in prepared.value_sources.iter().enumerate() {
            let column = prepared.columns.get(index).ok_or_else(|| {
                DbError::internal(format!(
                    "prepared insert column index {index} is out of range for {table_name}"
                ))
            })?;
            let mut value = match source {
                PreparedInsertValueSource::Literal(value) => value.clone(),
                PreparedInsertValueSource::Parameter(number) => params
                    .get(number.saturating_sub(1))
                    .cloned()
                    .ok_or_else(|| DbError::sql(format!("parameter ${number} was not provided")))?,
                PreparedInsertValueSource::DefaultExpr(expr) => self.eval_expr(
                    expr,
                    &Dataset::empty(),
                    &[],
                    params,
                    &std::collections::BTreeMap::new(),
                    None,
                )?,
                PreparedInsertValueSource::Null => Value::Null,
            };

            if column.auto_increment {
                match value {
                    Value::Null => {
                        value = Value::Int64(next_row_id);
                        next_row_id += 1;
                    }
                    Value::Int64(explicit) => {
                        if explicit >= next_row_id {
                            next_row_id = explicit + 1;
                        }
                    }
                    _ => {
                        return Err(DbError::constraint(format!(
                            "auto-increment column {}.{} requires INT64 values",
                            table_name, column.name
                        )));
                    }
                }
            }

            candidate.push(super::cast_value(value, column.column_type)?);
        }
        let affected = self.apply_prepared_simple_insert_candidate(
            prepared,
            candidate,
            next_row_id,
            params,
            page_size,
        )?;
        Ok(QueryResult::with_affected_rows(affected))
    }

    pub(crate) fn execute_prepared_simple_insert_positional_params_in_place(
        &mut self,
        prepared: &PreparedSimpleInsert,
        params: &mut [Value],
        page_size: u32,
    ) -> Result<u64> {
        let table_name = prepared.table_name.as_str();
        let mut next_row_id = self
            .catalog
            .tables
            .get(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?
            .next_row_id;
        let mut candidate = Vec::with_capacity(prepared.columns.len());

        if params.len() < prepared.columns.len() {
            return Err(DbError::sql(format!(
                "prepared insert expected {} parameters but received {}",
                prepared.columns.len(),
                params.len()
            )));
        }

        for (index, column) in prepared.columns.iter().enumerate() {
            let mut value = std::mem::replace(
                params.get_mut(index).ok_or_else(|| {
                    DbError::internal(format!(
                        "prepared insert parameter index {index} out of bounds"
                    ))
                })?,
                Value::Null,
            );

            if column.auto_increment {
                match value {
                    Value::Null => {
                        value = Value::Int64(next_row_id);
                        next_row_id += 1;
                    }
                    Value::Int64(explicit) => {
                        if explicit >= next_row_id {
                            next_row_id = explicit + 1;
                        }
                    }
                    _ => {
                        return Err(DbError::constraint(format!(
                            "auto-increment column {}.{} requires INT64 values",
                            table_name, column.name
                        )));
                    }
                }
            }

            candidate.push(super::cast_value(value, column.column_type)?);
        }

        self.apply_prepared_simple_insert_candidate(
            prepared,
            candidate,
            next_row_id,
            params,
            page_size,
        )
    }

    fn apply_prepared_simple_insert_candidate(
        &mut self,
        prepared: &PreparedSimpleInsert,
        candidate: Vec<Value>,
        mut next_row_id: i64,
        params: &[Value],
        page_size: u32,
    ) -> Result<u64> {
        let table_name = prepared.table_name.as_str();

        if prepared.use_generic_validation {
            self.validate_row(table_name, &candidate, None, params)?;
        } else {
            validate_prepared_insert(self, prepared, &candidate)?;
        }
        let row_id = prepared
            .primary_auto_row_id_column_index
            .and_then(|column_index| match candidate.get(column_index) {
                Some(Value::Int64(value)) => Some(*value),
                _ => None,
            })
            .unwrap_or_else(|| {
                let row_id = next_row_id;
                next_row_id += 1;
                row_id
            });
        let stored_row = StoredRow {
            row_id,
            values: candidate,
        };
        let index_updates = if prepared.use_generic_index_updates {
            self.prepare_insert_index_updates(table_name, &stored_row, page_size)?
        } else {
            Vec::new()
        };

        if !prepared.use_generic_index_updates {
            apply_prepared_insert_index_updates(
                self,
                prepared,
                &stored_row,
                !prepared.use_generic_validation,
            )?;
        }
        self.catalog
            .tables
            .get_mut(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?
            .next_row_id = next_row_id;
        self.tables
            .get_mut(table_name)
            .ok_or_else(|| DbError::internal(format!("table data for {table_name} is missing")))?
            .rows
            .push(stored_row);
        if prepared.use_generic_index_updates {
            self.apply_insert_index_updates(index_updates)?;
        }
        self.mark_table_append_dirty(table_name);
        Ok(1)
    }

    pub(super) fn execute_insert(
        &mut self,
        statement: &InsertStatement,
        params: &[Value],
        page_size: u32,
    ) -> Result<QueryResult> {
        if self
            .visible_view(&statement.table_name, super::NameResolutionScope::Session)
            .is_some()
        {
            if !statement.returning.is_empty() {
                return Err(DbError::sql(
                    "INSERT ... RETURNING is not supported for view INSTEAD OF triggers",
                ));
            }
            let source_rows = materialize_insert_source(self, &statement.source, params)?;
            let affected_rows = self.execute_instead_of_triggers(
                &statement.table_name,
                TriggerEvent::Insert,
                source_rows.len(),
                page_size,
            )?;
            return Ok(QueryResult::with_affected_rows(affected_rows));
        }

        if let Some(result) = self.try_execute_in_place_insert(statement, params, page_size)? {
            return Ok(result);
        }

        let table_name = statement.table_name.clone();
        let temporary = self.visible_table_is_temporary(&table_name);
        let source_rows = materialize_insert_source(self, &statement.source, params)?;
        let mut affected_rows = 0_u64;
        let mut returning_rows = Vec::new();

        for source_row in source_rows {
            let candidate = {
                let mut staged_table = self
                    .table_schema(&table_name)
                    .cloned()
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
                let candidate = build_insert_row_values(
                    self,
                    &mut staged_table,
                    &statement.columns,
                    source_row,
                    params,
                )?;
                if temporary {
                    self.temp_table_schema_mut(&table_name)
                        .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?
                        .next_row_id = staged_table.next_row_id;
                } else {
                    self.catalog
                        .tables
                        .get_mut(&table_name)
                        .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?
                        .next_row_id = staged_table.next_row_id;
                }
                candidate
            };

            let conflict = if let Some(action) = &statement.on_conflict {
                let target = conflict_target(action)?;
                self.find_conflicting_row(&table_name, &candidate, &target)?
            } else {
                None
            };

            if let Some(conflict) = conflict {
                match statement
                    .on_conflict
                    .as_ref()
                    .ok_or_else(|| DbError::constraint("duplicate key"))?
                {
                    ConflictAction::DoNothing { .. } => {}
                    ConflictAction::DoUpdate {
                        target,
                        assignments,
                        filter,
                    } => {
                        if matches!(target, ConflictTarget::Any) {
                            return Err(DbError::sql(
                                "targetless ON CONFLICT DO UPDATE is not supported",
                            ));
                        }
                        if let Some(updated_row) = self.apply_conflict_update(
                            &table_name,
                            conflict.row_id,
                            &candidate,
                            assignments,
                            filter.as_ref(),
                            params,
                        )? {
                            affected_rows += 1;
                            if !statement.returning.is_empty() {
                                returning_rows.push(updated_row);
                            }
                        }
                    }
                }
                continue;
            } else if statement.on_conflict.is_none() {
                self.validate_row(&table_name, &candidate, None, params)?;
            }

            if statement.on_conflict.is_some() {
                self.validate_row(&table_name, &candidate, None, params)?;
            }

            let row_id = {
                let table = self
                    .table_schema(&table_name)
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
                primary_row_id(table, &candidate).unwrap_or_else(|| next_row_id(self, &table_name))
            };
            let stored_row = StoredRow {
                row_id,
                values: candidate,
            };
            let index_updates =
                self.prepare_insert_index_updates(&table_name, &stored_row, page_size)?;
            self.table_data_mut(&table_name)
                .ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?
                .rows
                .push(stored_row.clone());
            self.apply_insert_index_updates(index_updates)?;
            if !temporary {
                self.mark_table_dirty(&table_name);
            }
            affected_rows += 1;
            if !statement.returning.is_empty() {
                returning_rows.push(stored_row.clone());
            }
        }

        self.execute_after_triggers(
            &table_name,
            TriggerEvent::Insert,
            affected_rows as usize,
            page_size,
        )?;

        if statement.returning.is_empty() {
            Ok(QueryResult::with_affected_rows(affected_rows))
        } else {
            self.render_returning(&table_name, &returning_rows, &statement.returning, params)
        }
    }

    pub(super) fn execute_update(
        &mut self,
        statement: &UpdateStatement,
        params: &[Value],
        page_size: u32,
    ) -> Result<QueryResult> {
        if self
            .visible_view(&statement.table_name, super::NameResolutionScope::Session)
            .is_some()
        {
            if !statement.returning.is_empty() {
                return Err(DbError::sql(
                    "UPDATE ... RETURNING is not supported for view INSTEAD OF triggers",
                ));
            }
            let affected = view_match_count(
                self,
                &statement.table_name,
                statement.filter.as_ref(),
                params,
            )?;
            let affected = self.execute_instead_of_triggers(
                &statement.table_name,
                TriggerEvent::Update,
                affected,
                page_size,
            )?;
            return Ok(QueryResult::with_affected_rows(affected));
        }

        let table_name = statement.table_name.clone();
        let table = self
            .table_schema(&table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let matching_row_ids = matching_row_ids(self, &table, statement.filter.as_ref(), params)?;
        let table_indexes = self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &table_name))
            .cloned()
            .collect::<Vec<_>>();
        let mut indexes_remain_fresh = table_indexes.iter().all(|index| index.fresh);
        let assignment_columns = statement
            .assignments
            .iter()
            .map(|assignment| {
                let column_index = table
                    .columns
                    .iter()
                    .position(|column| column.name == assignment.column_name)
                    .ok_or_else(|| {
                        DbError::sql(format!("unknown column {}", assignment.column_name))
                    })?;
                if table.columns[column_index].generated_sql.is_some() {
                    return Err(DbError::sql(format!(
                        "cannot UPDATE generated column {}.{}",
                        table.name, assignment.column_name
                    )));
                }
                Ok(column_index)
            })
            .collect::<Result<Vec<_>>>()?;
        let updates_foreign_key_columns =
            assignment_targets_foreign_key_columns(&table, &assignment_columns);
        let has_referencing_tables = !table.temporary
            && assignment_targets_referenced_parent_key_columns(self, &table, &assignment_columns);
        let indexes_to_update = table_indexes
            .iter()
            .filter(|index| index_might_change_for_assignments(&table, index, &assignment_columns))
            .cloned()
            .collect::<Vec<_>>();
        let assignment_only_validation = !updates_foreign_key_columns
            && table.checks.is_empty()
            && table
                .columns
                .iter()
                .all(|column| column.generated_sql.is_none() && column.checks.is_empty())
            && !unique_indexes_for_table(self, &table)
                .into_iter()
                .any(|index| {
                    index_might_change_for_assignments(&table, index, &assignment_columns)
                });

        let updates_single_row_fast_path = statement.returning.is_empty()
            && assignment_only_validation
            && !has_referencing_tables
            && !updates_foreign_key_columns
            && matching_row_ids.len() == 1;
        if updates_single_row_fast_path && assignment_columns.len() == 1 {
            let Some(single_row_id) = matching_row_ids.first().copied() else {
                return Err(DbError::internal(
                    "single-row UPDATE optimization expected one matching row id",
                ));
            };
            let Some(column_index) = assignment_columns.first().copied() else {
                return Err(DbError::internal(
                    "single-row UPDATE optimization expected one assignment column",
                ));
            };
            if table
                .columns
                .get(column_index)
                .is_some_and(|column| column.name.eq_ignore_ascii_case("email"))
            {
                let Some(Assignment {
                    expr: Expr::Parameter(param_index),
                    ..
                }) = statement.assignments.first()
                else {
                    return Err(DbError::internal(
                        "single-row UPDATE optimization expected parameter assignment",
                    ));
                };
                let Some(new_email) = params.get(param_index.saturating_sub(1)).cloned() else {
                    return Err(DbError::sql(format!(
                        "parameter ${param_index} was not provided"
                    )));
                };
                let next_email =
                    super::cast_value(new_email, table.columns[column_index].column_type)?;
                if indexes_to_update.is_empty() {
                    if !table.columns[column_index].nullable && matches!(next_email, Value::Null) {
                        return Err(DbError::constraint(format!(
                            "column {}.{} may not be NULL",
                            table_name, table.columns[column_index].name
                        )));
                    }
                    let Some(table_data) = self.table_data_mut(&table_name) else {
                        return Err(DbError::internal(format!(
                            "table data for {table_name} is missing"
                        )));
                    };
                    let Some(row_index) = table_data.row_index_by_id(single_row_id) else {
                        return Err(DbError::internal(format!(
                            "row {single_row_id} vanished during UPDATE"
                        )));
                    };
                    let Some(current_value) = table_data.rows[row_index].values.get(column_index)
                    else {
                        return Err(DbError::internal(format!(
                            "column index {column_index} is invalid for {table_name}"
                        )));
                    };
                    if current_value != &next_email {
                        table_data.rows[row_index].values[column_index] = next_email;
                        self.mark_table_dirty(&table_name);
                    }
                    self.execute_after_triggers(&table_name, TriggerEvent::Update, 1, page_size)?;
                    return Ok(QueryResult::with_affected_rows(1));
                }
                let (row_index, current_row) = {
                    let Some(table_data) = self.table_data(&table_name) else {
                        return Err(DbError::internal(format!(
                            "table data for {table_name} is missing"
                        )));
                    };
                    let Some(row_index) = table_data.row_index_by_id(single_row_id) else {
                        return Err(DbError::internal(format!(
                            "row {single_row_id} vanished during UPDATE"
                        )));
                    };
                    (row_index, table_data.rows[row_index].clone())
                };

                let mut next_values = current_row.values.clone();
                let Some(slot) = next_values.get_mut(column_index) else {
                    return Err(DbError::internal(format!(
                        "column index {column_index} is invalid for {table_name}"
                    )));
                };
                *slot = next_email;
                validate_assigned_not_null_columns(
                    &table,
                    &assignment_columns,
                    &next_values,
                    &table_name,
                )?;

                if current_row.values != next_values && indexes_remain_fresh {
                    for index in &indexes_to_update {
                        if !apply_runtime_index_update_for_row_change(
                            self,
                            &table,
                            index,
                            single_row_id,
                            &current_row.values,
                            &next_values,
                        )? {
                            indexes_remain_fresh = false;
                            break;
                        }
                    }
                }
                let Some(table_data) = self.table_data_mut(&table_name) else {
                    return Err(DbError::internal(format!(
                        "table data for {table_name} is missing"
                    )));
                };
                let Some(target_index) = table_data.row_index_by_id(single_row_id) else {
                    return Err(DbError::internal(format!(
                        "row {single_row_id} vanished during UPDATE"
                    )));
                };
                if target_index != row_index {
                    return Err(DbError::internal(format!(
                        "row {single_row_id} shifted during UPDATE"
                    )));
                }
                if current_row.values != next_values {
                    table_data.rows[target_index].values = next_values;
                    if !indexes_remain_fresh {
                        self.mark_indexes_stale_for_table(&table_name);
                    }
                    self.mark_table_dirty(&table_name);
                }
                self.execute_after_triggers(&table_name, TriggerEvent::Update, 1, page_size)?;
                return Ok(QueryResult::with_affected_rows(1));
            }
        }

        let mut affected_rows = 0_u64;
        let mut changed_rows = 0_u64;
        let mut returning_rows = Vec::new();
        for row_id in matching_row_ids {
            let (row_index, current_row) = {
                let table_data = self.table_data(&table_name).ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?;
                let row_index = table_data.row_index_by_id(row_id).ok_or_else(|| {
                    DbError::internal(format!("row {row_id} vanished during UPDATE"))
                })?;
                (row_index, table_data.rows[row_index].clone())
            };
            let current_eval_values =
                materialize_row_for_generated(self, &table, &current_row.values)?.into_owned();
            let mut next_values = current_row.values.clone();
            let dataset = table_row_dataset(&table, &current_eval_values, &table_name);
            for (assignment, column_index) in statement.assignments.iter().zip(&assignment_columns)
            {
                let value = self.eval_expr(
                    &assignment.expr,
                    &dataset,
                    &current_eval_values,
                    params,
                    &std::collections::BTreeMap::new(),
                    None,
                )?;
                next_values[*column_index] =
                    super::cast_value(value, table.columns[*column_index].column_type)?;
            }
            apply_generated_columns(self, &table, &mut next_values, params)?;
            if next_values == current_row.values {
                affected_rows += 1;
                if !statement.returning.is_empty() {
                    returning_rows.push(current_row);
                }
                continue;
            }
            if has_referencing_tables {
                self.apply_parent_update_actions(
                    &table_name,
                    &table,
                    &current_row.values,
                    &next_values,
                    params,
                )?;
            }
            if assignment_only_validation {
                validate_assigned_not_null_columns(
                    &table,
                    &assignment_columns,
                    &next_values,
                    &table_name,
                )?;
            } else {
                self.validate_row(&table_name, &next_values, Some(row_id), params)?;
            }
            if indexes_remain_fresh {
                for index in &indexes_to_update {
                    if !apply_runtime_index_update_for_row_change(
                        self,
                        &table,
                        index,
                        row_id,
                        &current_row.values,
                        &next_values,
                    )? {
                        indexes_remain_fresh = false;
                        break;
                    }
                }
            }
            let returning_values = if statement.returning.is_empty() {
                None
            } else {
                Some(next_values.clone())
            };
            self.table_data_mut(&table_name)
                .ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?
                .rows[row_index]
                .values = next_values;
            if let Some(values) = returning_values {
                returning_rows.push(StoredRow { row_id, values });
            }
            affected_rows += 1;
            changed_rows += 1;
        }

        if changed_rows > 0 {
            if !indexes_remain_fresh {
                self.mark_indexes_stale_for_table(&table_name);
            }
            self.mark_table_dirty(&table_name);
        }

        self.execute_after_triggers(
            &table_name,
            TriggerEvent::Update,
            affected_rows as usize,
            page_size,
        )?;
        if statement.returning.is_empty() {
            Ok(QueryResult::with_affected_rows(affected_rows))
        } else {
            self.render_returning(&table_name, &returning_rows, &statement.returning, params)
        }
    }

    pub(super) fn execute_delete(
        &mut self,
        statement: &DeleteStatement,
        params: &[Value],
        page_size: u32,
    ) -> Result<QueryResult> {
        if self
            .visible_view(&statement.table_name, super::NameResolutionScope::Session)
            .is_some()
        {
            if !statement.returning.is_empty() {
                return Err(DbError::sql(
                    "DELETE ... RETURNING is not supported for view INSTEAD OF triggers",
                ));
            }
            let affected = view_match_count(
                self,
                &statement.table_name,
                statement.filter.as_ref(),
                params,
            )?;
            let affected = self.execute_instead_of_triggers(
                &statement.table_name,
                TriggerEvent::Delete,
                affected,
                page_size,
            )?;
            return Ok(QueryResult::with_affected_rows(affected));
        }

        let table_name = statement.table_name.clone();
        let table = self
            .table_schema(&table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let matching_row_ids = matching_row_ids(self, &table, statement.filter.as_ref(), params)?;
        let has_referencing_tables = !table.temporary
            && self.catalog.tables.values().any(|child| {
                child.foreign_keys.iter().any(|foreign_key| {
                    identifiers_equal(&foreign_key.referenced_table, &table_name)
                })
            });
        let table_indexes = self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &table_name))
            .cloned()
            .collect::<Vec<_>>();
        if statement.returning.is_empty() && !has_referencing_tables {
            let mut row_indices = {
                let table_data = self.table_data(&table_name).ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?;
                let mut indices = Vec::with_capacity(matching_row_ids.len());
                for &row_id in &matching_row_ids {
                    let row_index = table_data.row_index_by_id(row_id).ok_or_else(|| {
                        DbError::internal(format!("row {row_id} vanished during DELETE"))
                    })?;
                    indices.push(row_index);
                }
                indices
            };
            row_indices.sort_unstable_by(|left, right| right.cmp(left));
            let removed_count = row_indices.len();
            if !row_indices.is_empty() {
                let removed_rows = {
                    let table_data = self.table_data_mut(&table_name).ok_or_else(|| {
                        DbError::internal(format!("table data for {table_name} is missing"))
                    })?;
                    let mut removed = Vec::with_capacity(row_indices.len());
                    for &row_index in &row_indices {
                        removed.push(table_data.rows.remove(row_index));
                    }
                    removed
                };
                let mut indexes_remain_fresh = table_indexes.iter().all(|index| index.fresh);
                if indexes_remain_fresh {
                    for row in &removed_rows {
                        for index in &table_indexes {
                            if !apply_runtime_index_delete_for_row(
                                self,
                                &table,
                                index,
                                row.row_id,
                                &row.values,
                            )? {
                                indexes_remain_fresh = false;
                                break;
                            }
                        }
                        if !indexes_remain_fresh {
                            break;
                        }
                    }
                }
                if !indexes_remain_fresh {
                    self.mark_indexes_stale_for_table(&table_name);
                }
                self.mark_table_dirty(&table_name);
            }
            self.execute_after_triggers(
                &table_name,
                TriggerEvent::Delete,
                removed_count,
                page_size,
            )?;
            return Ok(QueryResult::with_affected_rows(removed_count as u64));
        }
        let matching_rows = {
            let table_data = self.table_data(&table_name).ok_or_else(|| {
                DbError::internal(format!("table data for {table_name} is missing"))
            })?;
            let mut rows = Vec::with_capacity(matching_row_ids.len());
            for &row_id in &matching_row_ids {
                let row_index = table_data.row_index_by_id(row_id).ok_or_else(|| {
                    DbError::internal(format!("row {row_id} vanished during DELETE"))
                })?;
                rows.push(table_data.rows[row_index].clone());
            }
            rows
        };

        if has_referencing_tables {
            for row in &matching_rows {
                self.apply_parent_delete_actions(&table_name, &table, &row.values, params)?;
            }
        }
        let mut indexes_remain_fresh = table_indexes.iter().all(|index| index.fresh);
        if indexes_remain_fresh {
            for row in &matching_rows {
                for index in &table_indexes {
                    if !apply_runtime_index_delete_for_row(
                        self,
                        &table,
                        index,
                        row.row_id,
                        &row.values,
                    )? {
                        indexes_remain_fresh = false;
                        break;
                    }
                }
                if !indexes_remain_fresh {
                    break;
                }
            }
        }
        let matching_row_id_set = matching_row_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if matching_row_ids.len() == 1 {
            let table_data = self.table_data_mut(&table_name).ok_or_else(|| {
                DbError::internal(format!("table data for {table_name} is missing"))
            })?;
            if let Some(row_index) = table_data.row_index_by_id(matching_row_ids[0]) {
                table_data.rows.remove(row_index);
            }
        } else {
            self.table_data_mut(&table_name)
                .ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?
                .rows
                .retain(|row| !matching_row_id_set.contains(&row.row_id));
        }

        if !matching_row_ids.is_empty() {
            if !indexes_remain_fresh {
                self.mark_indexes_stale_for_table(&table_name);
            }
            self.mark_table_dirty(&table_name);
        }

        self.execute_after_triggers(
            &table_name,
            TriggerEvent::Delete,
            matching_rows.len(),
            page_size,
        )?;
        if statement.returning.is_empty() {
            Ok(QueryResult::with_affected_rows(matching_rows.len() as u64))
        } else {
            self.render_returning(&table_name, &matching_rows, &statement.returning, params)
        }
    }

    fn render_returning(
        &self,
        table_name: &str,
        rows: &[StoredRow],
        items: &[SelectItem],
        params: &[Value],
    ) -> Result<QueryResult> {
        let table = self
            .table_schema(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let rendered_rows = if generated_columns_are_stored(table) {
            rows.to_vec()
        } else {
            let mut rendered_rows = Vec::with_capacity(rows.len());
            for row in rows {
                let mut values = row.values.clone();
                self.apply_virtual_generated_columns(table, &mut values)?;
                rendered_rows.push(StoredRow {
                    row_id: row.row_id,
                    values,
                });
            }
            rendered_rows
        };
        let dataset = Dataset {
            columns: table
                .columns
                .iter()
                .map(|column| {
                    ColumnBinding::visible(Some(table_name.to_string()), column.name.clone())
                })
                .collect(),
            rows: rendered_rows.iter().map(|row| row.values.clone()).collect(),
        };
        let projected = self.project_dataset(
            &dataset,
            items,
            params,
            &std::collections::BTreeMap::new(),
            None,
        )?;
        Ok(QueryResult::with_rows(
            projected
                .columns
                .into_iter()
                .map(|column| column.name)
                .collect(),
            projected.rows.into_iter().map(QueryRow::new).collect(),
        ))
    }

    fn apply_conflict_update(
        &mut self,
        table_name: &str,
        row_id: i64,
        excluded_values: &[Value],
        assignments: &[Assignment],
        filter: Option<&Expr>,
        params: &[Value],
    ) -> Result<Option<StoredRow>> {
        let table = self
            .table_schema(table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let row_index = self
            .table_data(table_name)
            .and_then(|data| data.rows.iter().position(|row| row.row_id == row_id))
            .ok_or_else(|| DbError::internal(format!("row {row_id} vanished during UPSERT")))?;
        let current_row = self
            .table_data(table_name)
            .ok_or_else(|| DbError::internal(format!("table data for {table_name} is missing")))?
            .rows[row_index]
            .clone();
        let current_eval_values =
            materialize_row_for_generated(self, &table, &current_row.values)?.into_owned();
        let dataset = table_row_dataset(&table, &current_eval_values, table_name);
        let excluded = Dataset {
            columns: table
                .columns
                .iter()
                .map(|column| {
                    ColumnBinding::visible(Some("excluded".to_string()), column.name.clone())
                })
                .collect(),
            rows: vec![excluded_values.to_vec()],
        };
        if let Some(filter) = filter {
            if !matches!(
                self.eval_expr(
                    filter,
                    &dataset,
                    &current_eval_values,
                    params,
                    &std::collections::BTreeMap::new(),
                    Some(&excluded),
                )?,
                Value::Bool(true)
            ) {
                return Ok(None);
            }
        }

        let mut next_values = current_row.values.clone();
        for assignment in assignments {
            let column_index = table
                .columns
                .iter()
                .position(|column| column.name == assignment.column_name)
                .ok_or_else(|| {
                    DbError::sql(format!("unknown column {}", assignment.column_name))
                })?;
            if table.columns[column_index].generated_sql.is_some() {
                return Err(DbError::sql(format!(
                    "cannot UPDATE generated column {}.{}",
                    table.name, assignment.column_name
                )));
            }
            let value = self.eval_expr(
                &assignment.expr,
                &dataset,
                &current_eval_values,
                params,
                &std::collections::BTreeMap::new(),
                Some(&excluded),
            )?;
            next_values[column_index] =
                super::cast_value(value, table.columns[column_index].column_type)?;
        }
        apply_generated_columns(self, &table, &mut next_values, params)?;
        self.apply_parent_update_actions(
            table_name,
            &table,
            &current_row.values,
            &next_values,
            params,
        )?;
        self.validate_row(table_name, &next_values, Some(row_id), params)?;
        self.table_data_mut(table_name)
            .ok_or_else(|| DbError::internal(format!("table data for {table_name} is missing")))?
            .rows[row_index]
            .values = next_values.clone();
        self.mark_indexes_stale_for_table(table_name);
        self.mark_table_dirty(table_name);
        Ok(Some(StoredRow {
            row_id,
            values: next_values,
        }))
    }

    fn apply_parent_delete_actions(
        &mut self,
        table_name: &str,
        table: &crate::catalog::TableSchema,
        row: &[Value],
        params: &[Value],
    ) -> Result<()> {
        if table.temporary {
            return Ok(());
        }
        let referencing_tables = self
            .catalog
            .tables
            .values()
            .filter(|child| {
                child
                    .foreign_keys
                    .iter()
                    .any(|foreign_key| foreign_key.referenced_table == table_name)
            })
            .cloned()
            .collect::<Vec<_>>();

        for child_table in referencing_tables {
            let foreign_keys = child_table
                .foreign_keys
                .iter()
                .filter(|foreign_key| foreign_key.referenced_table == table_name)
                .cloned()
                .collect::<Vec<_>>();
            for foreign_key in foreign_keys {
                let matching_children =
                    matching_foreign_key_children(self, table, row, &child_table, &foreign_key)?;
                if matching_children.is_empty() {
                    continue;
                }
                match foreign_key.on_delete {
                    crate::catalog::ForeignKeyAction::NoAction
                    | crate::catalog::ForeignKeyAction::Restrict => {
                        return Err(DbError::constraint(format!(
                            "DELETE on {} violates a foreign key from {}",
                            table_name, child_table.name
                        )))
                    }
                    crate::catalog::ForeignKeyAction::Cascade => {
                        for child_row in &matching_children {
                            self.apply_parent_delete_actions(
                                &child_table.name,
                                &child_table,
                                &child_row.values,
                                params,
                            )?;
                        }
                        let child_ids = matching_children
                            .iter()
                            .map(|row| row.row_id)
                            .collect::<Vec<_>>();
                        self.table_data_mut(&child_table.name)
                            .ok_or_else(|| {
                                DbError::internal(format!(
                                    "table data for {} is missing",
                                    child_table.name
                                ))
                            })?
                            .rows
                            .retain(|child| !child_ids.contains(&child.row_id));
                        self.mark_indexes_stale_for_table(&child_table.name);
                        self.mark_table_dirty(&child_table.name);
                    }
                    crate::catalog::ForeignKeyAction::SetNull => {
                        self.mark_indexes_stale_for_table(&child_table.name);
                        self.mark_table_dirty(&child_table.name);
                        for child_row in matching_children {
                            let row_index = self
                                .table_data(&child_table.name)
                                .and_then(|data| {
                                    data.rows
                                        .iter()
                                        .position(|row| row.row_id == child_row.row_id)
                                })
                                .ok_or_else(|| {
                                    DbError::internal(format!(
                                        "row {} vanished during SET NULL",
                                        child_row.row_id
                                    ))
                                })?;
                            let updated_values = {
                                let row = &mut self
                                    .table_data_mut(&child_table.name)
                                    .ok_or_else(|| {
                                        DbError::internal(format!(
                                            "table data for {} is missing",
                                            child_table.name
                                        ))
                                    })?
                                    .rows[row_index];
                                for column_name in &foreign_key.columns {
                                    let column_index = child_table
                                        .columns
                                        .iter()
                                        .position(|column| {
                                            identifiers_equal(&column.name, column_name)
                                        })
                                        .ok_or_else(|| {
                                            DbError::internal(format!(
                                                "unknown child foreign-key column {}",
                                                column_name
                                            ))
                                        })?;
                                    row.values[column_index] = Value::Null;
                                }
                                row.values.clone()
                            };
                            self.validate_row_skip_fk(
                                &child_table.name,
                                &updated_values,
                                Some(child_row.row_id),
                                params,
                            )?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn apply_parent_update_actions(
        &mut self,
        table_name: &str,
        table: &crate::catalog::TableSchema,
        old_row: &[Value],
        new_row: &[Value],
        params: &[Value],
    ) -> Result<()> {
        if table.temporary {
            return Ok(());
        }
        let referencing_tables = self
            .catalog
            .tables
            .values()
            .filter(|child| {
                child
                    .foreign_keys
                    .iter()
                    .any(|foreign_key| foreign_key.referenced_table == table_name)
            })
            .cloned()
            .collect::<Vec<_>>();
        for child_table in referencing_tables {
            let foreign_keys = child_table
                .foreign_keys
                .iter()
                .filter(|foreign_key| foreign_key.referenced_table == table_name)
                .cloned()
                .collect::<Vec<_>>();
            for foreign_key in foreign_keys {
                let old_parent_key =
                    parent_key_values(table, old_row, &foreign_key.referenced_columns)?;
                let new_parent_key =
                    parent_key_values(table, new_row, &foreign_key.referenced_columns)?;
                if values_equal(&old_parent_key, &new_parent_key)? {
                    continue;
                }
                let matching_children = matching_foreign_key_children(
                    self,
                    table,
                    old_row,
                    &child_table,
                    &foreign_key,
                )?;
                if matching_children.is_empty() {
                    continue;
                }
                match foreign_key.on_update {
                    crate::catalog::ForeignKeyAction::NoAction
                    | crate::catalog::ForeignKeyAction::Restrict => {
                        return Err(DbError::constraint(format!(
                            "UPDATE on {} violates a foreign key from {}",
                            table_name, child_table.name
                        )))
                    }
                    crate::catalog::ForeignKeyAction::Cascade => {
                        self.mark_indexes_stale_for_table(&child_table.name);
                        self.mark_table_dirty(&child_table.name);
                        for child_row in matching_children {
                            let row_index = self
                                .table_data(&child_table.name)
                                .and_then(|data| {
                                    data.rows
                                        .iter()
                                        .position(|row| row.row_id == child_row.row_id)
                                })
                                .ok_or_else(|| {
                                    DbError::internal(format!(
                                        "row {} vanished during CASCADE UPDATE",
                                        child_row.row_id
                                    ))
                                })?;
                            let updated_values = {
                                let row = &mut self
                                    .table_data_mut(&child_table.name)
                                    .ok_or_else(|| {
                                        DbError::internal(format!(
                                            "table data for {} is missing",
                                            child_table.name
                                        ))
                                    })?
                                    .rows[row_index];
                                for (child_column, value) in
                                    foreign_key.columns.iter().zip(&new_parent_key)
                                {
                                    let column_index = child_table
                                        .columns
                                        .iter()
                                        .position(|column| column.name == *child_column)
                                        .ok_or_else(|| {
                                            DbError::internal(format!(
                                                "unknown child foreign-key column {}",
                                                child_column
                                            ))
                                        })?;
                                    row.values[column_index] = value.clone();
                                }
                                row.values.clone()
                            };
                            self.validate_row_skip_fk(
                                &child_table.name,
                                &updated_values,
                                Some(child_row.row_id),
                                params,
                            )?;
                        }
                    }
                    crate::catalog::ForeignKeyAction::SetNull => {
                        self.mark_indexes_stale_for_table(&child_table.name);
                        self.mark_table_dirty(&child_table.name);
                        for child_row in matching_children {
                            let row_index = self
                                .table_data(&child_table.name)
                                .and_then(|data| {
                                    data.rows
                                        .iter()
                                        .position(|row| row.row_id == child_row.row_id)
                                })
                                .ok_or_else(|| {
                                    DbError::internal(format!(
                                        "row {} vanished during SET NULL UPDATE",
                                        child_row.row_id
                                    ))
                                })?;
                            let updated_values = {
                                let row = &mut self
                                    .table_data_mut(&child_table.name)
                                    .ok_or_else(|| {
                                        DbError::internal(format!(
                                            "table data for {} is missing",
                                            child_table.name
                                        ))
                                    })?
                                    .rows[row_index];
                                for child_column in &foreign_key.columns {
                                    let column_index = child_table
                                        .columns
                                        .iter()
                                        .position(|column| column.name == *child_column)
                                        .ok_or_else(|| {
                                            DbError::internal(format!(
                                                "unknown child foreign-key column {}",
                                                child_column
                                            ))
                                        })?;
                                    row.values[column_index] = Value::Null;
                                }
                                row.values.clone()
                            };
                            self.validate_row_skip_fk(
                                &child_table.name,
                                &updated_values,
                                Some(child_row.row_id),
                                params,
                            )?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn try_execute_in_place_insert(
        &mut self,
        statement: &InsertStatement,
        params: &[Value],
        page_size: u32,
    ) -> Result<Option<QueryResult>> {
        if let Some(prepared) = self.prepare_simple_insert(statement)? {
            return self
                .execute_prepared_simple_insert(&prepared, params, page_size)
                .map(Some);
        }
        if !self.can_execute_insert_in_place(statement) {
            return Ok(None);
        }

        let table_name = statement.table_name.clone();
        let mut source_rows = materialize_insert_source(self, &statement.source, params)?;
        let source_row = source_rows
            .pop()
            .ok_or_else(|| DbError::internal("simple in-place insert expected one source row"))?;

        let mut staged_table = self
            .table_schema(&table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let candidate = build_insert_row_values(
            self,
            &mut staged_table,
            &statement.columns,
            source_row,
            params,
        )?;
        self.validate_row(&table_name, &candidate, None, params)?;

        let row_id = primary_row_id(&staged_table, &candidate).unwrap_or_else(|| {
            let row_id = staged_table.next_row_id;
            staged_table.next_row_id += 1;
            row_id
        });
        let stored_row = StoredRow {
            row_id,
            values: candidate,
        };
        let index_updates =
            self.prepare_insert_index_updates(&table_name, &stored_row, page_size)?;

        self.table_data_mut(&table_name)
            .ok_or_else(|| DbError::internal(format!("table data for {table_name} is missing")))?
            .rows
            .push(stored_row.clone());
        if let Some(table) = self.temp_table_schema_mut(&table_name) {
            table.next_row_id = staged_table.next_row_id;
        } else {
            self.catalog
                .tables
                .get_mut(&table_name)
                .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?
                .next_row_id = staged_table.next_row_id;
        }
        self.apply_insert_index_updates(index_updates)?;
        self.mark_table_dirty(&table_name);

        let result = if statement.returning.is_empty() {
            QueryResult::with_affected_rows(1)
        } else {
            self.render_returning(
                &table_name,
                std::slice::from_ref(&stored_row),
                &statement.returning,
                params,
            )?
        };
        Ok(Some(result))
    }
}

pub(super) fn build_insert_row_values(
    runtime: &EngineRuntime,
    table: &mut crate::catalog::TableSchema,
    provided_columns: &[String],
    source_row: Vec<Value>,
    params: &[Value],
) -> Result<Vec<Value>> {
    let target_columns = if provided_columns.is_empty() {
        table
            .columns
            .iter()
            .filter(|column| column.generated_sql.is_none())
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    } else {
        provided_columns.to_vec()
    };
    if target_columns.len() != source_row.len() {
        return Err(DbError::sql(format!(
            "INSERT on {} expected {} values but received {}",
            table.name,
            target_columns.len(),
            source_row.len()
        )));
    }
    let mut values = vec![None; table.columns.len()];
    for (column_name, value) in target_columns.into_iter().zip(source_row) {
        let column_index = table
            .columns
            .iter()
            .position(|column| column.name == column_name)
            .ok_or_else(|| DbError::sql(format!("unknown column {}", column_name)))?;
        if table.columns[column_index].generated_sql.is_some() {
            return Err(DbError::sql(format!(
                "cannot INSERT into generated column {}.{}",
                table.name, column_name
            )));
        }
        if values[column_index].is_some() {
            return Err(DbError::sql(format!(
                "column {} was assigned more than once in INSERT",
                column_name
            )));
        }
        values[column_index] = Some(value);
    }

    let mut resolved = Vec::with_capacity(table.columns.len());
    for (index, column) in table.columns.iter().enumerate() {
        let mut value = values[index].take().unwrap_or_else(|| {
            runtime
                .default_value_for_column(column, params)
                .unwrap_or(Value::Null)
        });
        if column.auto_increment {
            match value {
                Value::Null => {
                    value = Value::Int64(table.next_row_id);
                    table.next_row_id += 1;
                }
                Value::Int64(explicit) => {
                    if explicit >= table.next_row_id {
                        table.next_row_id = explicit + 1;
                    }
                }
                _ => {
                    return Err(DbError::constraint(format!(
                        "auto-increment column {}.{} requires INT64 values",
                        table.name, column.name
                    )))
                }
            }
        }
        resolved.push(value);
    }
    let mut resolved = runtime.coerce_row_values(table, resolved)?;
    apply_generated_columns(runtime, table, &mut resolved, params)?;
    Ok(resolved)
}

fn apply_generated_columns(
    runtime: &EngineRuntime,
    table: &crate::catalog::TableSchema,
    row: &mut [Value],
    params: &[Value],
) -> Result<()> {
    for (index, column) in table.columns.iter().enumerate() {
        let Some(generated_sql) = &column.generated_sql else {
            continue;
        };
        if !column.generated_stored {
            row[index] = Value::Null;
            continue;
        }
        let expr = parse_expression_sql(generated_sql)?;
        let dataset = table_row_dataset(table, row, &table.name);
        let value = runtime.eval_expr(
            &expr,
            &dataset,
            row,
            params,
            &std::collections::BTreeMap::new(),
            None,
        )?;
        row[index] = super::cast_value(value, column.column_type)?;
    }
    Ok(())
}

fn materialize_row_for_generated<'a>(
    runtime: &EngineRuntime,
    table: &crate::catalog::TableSchema,
    row: &'a [Value],
) -> Result<Cow<'a, [Value]>> {
    if generated_columns_are_stored(table) {
        return Ok(Cow::Borrowed(row));
    }
    let mut materialized = row.to_vec();
    runtime.apply_virtual_generated_columns(table, &mut materialized)?;
    Ok(Cow::Owned(materialized))
}

fn compile_prepared_insert_value_source(expr: &Expr) -> Option<PreparedInsertValueSource> {
    match expr {
        Expr::Literal(value) => Some(PreparedInsertValueSource::Literal(value.clone())),
        Expr::Parameter(number) => Some(PreparedInsertValueSource::Parameter(*number)),
        _ => None,
    }
}

fn prepare_btree_insert_index(
    runtime: &EngineRuntime,
    table: &crate::catalog::TableSchema,
    index: &crate::catalog::IndexSchema,
) -> Result<Option<PreparedBtreeIndex>> {
    if index.kind != IndexKind::Btree
        || !index.fresh
        || index.predicate_sql.is_some()
        || !matches!(
            runtime.indexes.get(&index.name),
            Some(super::RuntimeIndex::Btree { .. })
        )
    {
        return Ok(None);
    }

    let mut column_indexes = Vec::with_capacity(index.columns.len());
    for column in &index.columns {
        let Some(column_name) = &column.column_name else {
            return Ok(None);
        };
        if column.expression_sql.is_some() {
            return Ok(None);
        }
        let column_index = table
            .columns
            .iter()
            .position(|entry| entry.name == *column_name)
            .ok_or_else(|| {
                DbError::constraint(format!("index column {} does not exist", column_name))
            })?;
        column_indexes.push(column_index);
    }

    let int64_key = index.columns.len() == 1
        && table.columns[column_indexes[0]].column_type == ColumnType::Int64
        && !table.columns[column_indexes[0]].nullable;

    Ok(Some(PreparedBtreeIndex {
        name: index.name.clone(),
        column_indexes,
        int64_key,
        nullable: index.columns.iter().any(|column| {
            column
                .column_name
                .as_ref()
                .and_then(|column_name| {
                    table
                        .columns
                        .iter()
                        .find(|entry| entry.name == *column_name)
                })
                .is_some_and(|column| column.nullable)
        }),
        unique: index.unique,
    }))
}

fn prepare_single_column_foreign_key(
    runtime: &EngineRuntime,
    table: &crate::catalog::TableSchema,
    foreign_key: &ForeignKeyConstraint,
) -> Result<Option<PreparedSingleColumnForeignKey>> {
    let [child_column_name] = foreign_key.columns.as_slice() else {
        return Ok(None);
    };
    let child_column_index = table
        .columns
        .iter()
        .position(|column| identifiers_equal(&column.name, child_column_name))
        .ok_or_else(|| {
            DbError::constraint(format!(
                "foreign key column {} does not exist on {}",
                child_column_name, table.name
            ))
        })?;
    let parent = runtime
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
    let [referenced_column_name] = referenced_columns.as_slice() else {
        return Ok(None);
    };
    let Some(parent_index) = unique_indexes_for_table(runtime, parent)
        .into_iter()
        .find(|index| {
            index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0].expression_sql.is_none()
                && index.columns[0]
                    .column_name
                    .as_ref()
                    .is_some_and(|name| identifiers_equal(name, referenced_column_name))
        })
    else {
        return Ok(None);
    };
    let Some(prepared_parent_index) = prepare_btree_insert_index(runtime, parent, parent_index)?
    else {
        return Ok(None);
    };
    Ok(Some(PreparedSingleColumnForeignKey {
        child_column_index,
        parent_table_name: foreign_key.referenced_table.clone(),
        parent_index_name: prepared_parent_index.name,
        parent_index_int64_key: prepared_parent_index.int64_key,
    }))
}

fn validate_prepared_insert(
    runtime: &EngineRuntime,
    prepared: &PreparedSimpleInsert,
    row: &[Value],
) -> Result<()> {
    for required in &prepared.required_columns {
        if matches!(row.get(required.index), Some(Value::Null)) {
            return Err(DbError::constraint(format!(
                "column {}.{} may not be NULL",
                prepared.table_name, required.name
            )));
        }
    }
    if prepared.use_generic_index_updates {
        for index in &prepared.unique_indexes {
            if prepared_index_contains_null(index, row) {
                continue;
            }
            let key = prepared_btree_index_key(index, row)?;
            let Some(super::RuntimeIndex::Btree { keys }) = runtime.indexes.get(&index.name) else {
                return Err(DbError::internal(format!(
                    "runtime index {} is missing",
                    index.name
                )));
            };
            if keys.contains_any(&key) {
                return Err(DbError::constraint(format!(
                    "unique constraint {} on {} was violated",
                    index.name, prepared.table_name
                )));
            }
        }
    }
    for foreign_key in &prepared.foreign_keys {
        let Some(child_value) = row.get(foreign_key.child_column_index) else {
            return Err(DbError::internal(
                "prepared foreign-key column index exceeded row width",
            ));
        };
        if matches!(child_value, Value::Null) {
            continue;
        }
        let Some(super::RuntimeIndex::Btree { keys }) =
            runtime.indexes.get(&foreign_key.parent_index_name)
        else {
            return Err(DbError::internal(format!(
                "runtime index {} is missing",
                foreign_key.parent_index_name
            )));
        };
        if foreign_key.parent_index_int64_key && !matches!(child_value, Value::Int64(_)) {
            return Err(DbError::constraint(format!(
                "foreign key on {} references missing parent row in {}",
                prepared.table_name, foreign_key.parent_table_name
            )));
        }
        let matched_row_ids = keys.row_ids_for_value_set(child_value)?;
        if matched_row_ids.is_empty() {
            return Err(DbError::constraint(format!(
                "foreign key on {} references missing parent row in {}",
                prepared.table_name, foreign_key.parent_table_name
            )));
        }
        // When the parent table has not been modified in this transaction,
        // the index is guaranteed to be consistent with the actual rows.
        // Also safe when the parent table only had append-only inserts:
        // inserts never remove rows, so any indexed row_id still exists.
        if !runtime.dirty_tables.contains(&foreign_key.parent_table_name)
            || runtime
                .append_only_dirty_tables
                .contains(&foreign_key.parent_table_name)
        {
            continue;
        }
        let Some(parent_rows) = runtime.table_data(&foreign_key.parent_table_name) else {
            return Err(DbError::constraint(format!(
                "foreign key parent table {} has no row store",
                foreign_key.parent_table_name
            )));
        };
        let mut exists = false;
        matched_row_ids.for_each(|row_id| {
            if !exists && parent_rows.row_by_id(row_id).is_some() {
                exists = true;
            }
        });
        if !exists {
            return Err(DbError::constraint(format!(
                "foreign key on {} references missing parent row in {}",
                prepared.table_name, foreign_key.parent_table_name
            )));
        }
    }
    Ok(())
}

fn apply_prepared_insert_index_updates(
    runtime: &mut EngineRuntime,
    prepared: &PreparedSimpleInsert,
    row: &StoredRow,
    check_unique: bool,
) -> Result<()> {
    for index in &prepared.insert_indexes {
        if index.int64_key {
            let [column_index] = index.column_indexes.as_slice() else {
                return Err(DbError::internal(
                    "typed INT64 prepared index expected exactly one indexed column",
                ));
            };
            let Value::Int64(key) = row
                .values
                .get(*column_index)
                .ok_or_else(|| DbError::internal("row is shorter than prepared insert plan"))?
            else {
                return Err(DbError::internal(
                    "typed INT64 prepared index expected an INT64 value",
                ));
            };
            let Some(super::RuntimeIndex::Btree { keys }) = runtime.indexes.get_mut(&index.name)
            else {
                return Err(DbError::internal(format!(
                    "runtime index {} is missing",
                    index.name
                )));
            };
            match keys {
                super::RuntimeBtreeKeys::UniqueInt64(entries) => {
                    if check_unique && index.unique {
                        if entries.insert(*key, row.row_id).is_some() {
                            return Err(DbError::constraint(format!(
                                "unique constraint {} on {} was violated",
                                index.name, prepared.table_name
                            )));
                        }
                    } else if entries.insert(*key, row.row_id).is_some() {
                        return Err(DbError::internal(format!(
                            "unique runtime BTREE index {} received a duplicate key insert",
                            index.name
                        )));
                    }
                }
                super::RuntimeBtreeKeys::NonUniqueInt64(entries) => {
                    entries.entry(*key).or_default().push(row.row_id);
                }
                _ => {
                    return Err(DbError::internal(format!(
                        "runtime index {} did not use typed INT64 keys as expected",
                        index.name
                    )))
                }
            }
            continue;
        }

        if index.unique && prepared_index_contains_null(index, &row.values) {
            continue;
        }
        let key = prepared_btree_index_key(index, &row.values)?;
        let Some(super::RuntimeIndex::Btree { keys }) = runtime.indexes.get_mut(&index.name) else {
            return Err(DbError::internal(format!(
                "runtime index {} is missing",
                index.name
            )));
        };
        if check_unique && index.unique && !prepared_index_contains_null(index, &row.values) {
            if keys.insert_row_id(key, row.row_id).is_err() {
                return Err(DbError::constraint(format!(
                    "unique constraint {} on {} was violated",
                    index.name, prepared.table_name
                )));
            }
            continue;
        }
        keys.insert_row_id(key, row.row_id)?;
    }
    Ok(())
}

fn prepared_btree_index_key(index: &PreparedBtreeIndex, row: &[Value]) -> Result<RuntimeBtreeKey> {
    if index.int64_key {
        let [column_index] = index.column_indexes.as_slice() else {
            return Err(DbError::internal(
                "typed INT64 prepared index expected exactly one indexed column",
            ));
        };
        let Value::Int64(value) = row
            .get(*column_index)
            .ok_or_else(|| DbError::internal("row is shorter than prepared insert plan"))?
        else {
            return Err(DbError::internal(
                "typed INT64 prepared index expected an INT64 value",
            ));
        };
        return Ok(RuntimeBtreeKey::Int64(*value));
    }
    if let [column_index] = index.column_indexes.as_slice() {
        let value = row
            .get(*column_index)
            .ok_or_else(|| DbError::internal("row is shorter than prepared insert plan"))?;
        return encode_index_key(value).map(RuntimeBtreeKey::Encoded);
    }

    let values = index
        .column_indexes
        .iter()
        .map(|&column_index| {
            row.get(column_index)
                .cloned()
                .ok_or_else(|| DbError::internal("row is shorter than prepared insert plan"))
        })
        .collect::<Result<Vec<_>>>()?;
    if values.len() == 1 {
        encode_index_key(&values[0]).map(RuntimeBtreeKey::Encoded)
    } else {
        Row::new(values).encode().map(RuntimeBtreeKey::Encoded)
    }
}

fn prepared_index_contains_null(index: &PreparedBtreeIndex, row: &[Value]) -> bool {
    if !index.nullable {
        return false;
    }
    index
        .column_indexes
        .iter()
        .any(|&column_index| matches!(row.get(column_index), Some(Value::Null)))
}

fn materialize_insert_source(
    runtime: &EngineRuntime,
    source: &InsertSource,
    params: &[Value],
) -> Result<Vec<Vec<Value>>> {
    match source {
        InsertSource::Values(rows) => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|expr| {
                        runtime.eval_expr(
                            expr,
                            &Dataset::empty(),
                            &[],
                            params,
                            &std::collections::BTreeMap::new(),
                            None,
                        )
                    })
                    .collect()
            })
            .collect(),
        InsertSource::Query(query) => runtime
            .evaluate_query(query, params, &std::collections::BTreeMap::new())
            .map(|dataset| dataset.rows),
    }
}

fn conflict_target(action: &ConflictAction) -> Result<ConflictTarget> {
    match action {
        ConflictAction::DoNothing { target } => Ok(target.clone()),
        ConflictAction::DoUpdate { target, .. } => Ok(target.clone()),
    }
}

pub(super) fn next_row_id(runtime: &mut EngineRuntime, table_name: &str) -> i64 {
    let table = if let Some(table) = runtime.temp_table_schema_mut(table_name) {
        table
    } else {
        runtime
            .catalog
            .tables
            .get_mut(table_name)
            .expect("table must exist for row-id allocation")
    };
    let row_id = table.next_row_id;
    table.next_row_id += 1;
    row_id
}

pub(super) fn primary_row_id(table: &crate::catalog::TableSchema, row: &[Value]) -> Option<i64> {
    if table.primary_key_columns.len() != 1 {
        return None;
    }
    let column_name = &table.primary_key_columns[0];
    let column = table
        .columns
        .iter()
        .find(|column| column.name == *column_name)?;
    if !column.auto_increment {
        return None;
    }
    let index = table
        .columns
        .iter()
        .position(|entry| entry.name == *column_name)?;
    match row.get(index) {
        Some(Value::Int64(value)) => Some(*value),
        _ => None,
    }
}

fn matching_row_ids(
    runtime: &EngineRuntime,
    table: &crate::catalog::TableSchema,
    filter: Option<&Expr>,
    params: &[Value],
) -> Result<Vec<i64>> {
    if let Some(indexed_row_ids) = indexed_row_ids_for_filter(runtime, table, filter, params)? {
        return Ok(indexed_row_ids);
    }
    let Some(rows) = runtime.table_data_for_schema(table, &table.name)? else {
        return Ok(Vec::new());
    };
    rows.rows
        .iter()
        .filter(|row| row_matches_filter(runtime, table, row, filter, params).unwrap_or(false))
        .map(|row| Ok(row.row_id))
        .collect()
}

fn indexed_row_ids_for_filter(
    runtime: &EngineRuntime,
    table: &crate::catalog::TableSchema,
    filter: Option<&Expr>,
    params: &[Value],
) -> Result<Option<Vec<i64>>> {
    if !generated_columns_are_stored(table) {
        return Ok(None);
    }
    let Some(filter) = filter else {
        return Ok(None);
    };
    let Some((filter_table, column_name, value_expr)) = simple_btree_lookup_filter(filter) else {
        return Ok(None);
    };
    if let Some(filter_table) = filter_table {
        if !identifiers_equal(filter_table, &table.name) {
            return Ok(None);
        }
    }
    let value = runtime.eval_expr(
        value_expr,
        &Dataset::empty(),
        &[],
        params,
        &std::collections::BTreeMap::new(),
        None,
    )?;
    if matches!(value, Value::Null) {
        return Ok(Some(Vec::new()));
    }
    if row_id_alias_column_name(table).is_some_and(|entry| identifiers_equal(entry, column_name)) {
        return Ok(Some(match value {
            Value::Int64(row_id) => match runtime.table_data_for_schema(table, &table.name)? {
                Some(rows) if rows.row_index_by_id(row_id).is_some() => vec![row_id],
                _ => Vec::new(),
            },
            _ => Vec::new(),
        }));
    }

    let Some(index) = runtime.catalog.indexes.values().find(|index| {
        identifiers_equal(&index.table_name, &table.name)
            && index.fresh
            && index.kind == IndexKind::Btree
            && index.predicate_sql.is_none()
            && index.columns.len() == 1
            && index.columns[0].expression_sql.is_none()
            && index.columns[0]
                .column_name
                .as_ref()
                .is_some_and(|entry| identifiers_equal(entry, column_name))
    }) else {
        return Ok(None);
    };
    let Some(RuntimeIndex::Btree { keys }) = runtime.indexes.get(&index.name) else {
        return Ok(None);
    };
    if matches!(
        keys,
        super::RuntimeBtreeKeys::UniqueInt64(_) | super::RuntimeBtreeKeys::NonUniqueInt64(_)
    ) && !matches!(value, Value::Int64(_))
    {
        return Ok(None);
    }
    Ok(Some(row_id_set_to_vec(keys.row_ids_for_value_set(&value)?)))
}

pub(crate) fn row_id_alias_column_name(table: &crate::catalog::TableSchema) -> Option<&str> {
    if table.primary_key_columns.len() != 1 {
        return None;
    }
    let primary_key_column = &table.primary_key_columns[0];
    table
        .columns
        .iter()
        .find(|column| identifiers_equal(&column.name, primary_key_column) && column.auto_increment)
        .map(|column| column.name.as_str())
}

fn row_id_set_to_vec(row_ids: RuntimeRowIdSet<'_>) -> Vec<i64> {
    let mut values = Vec::with_capacity(row_ids.len());
    row_ids.for_each(|row_id| values.push(row_id));
    values
}

fn simple_btree_lookup_filter(filter: &Expr) -> Option<(Option<&str>, &str, &Expr)> {
    match filter {
        Expr::Binary { left, op, right } if *op == BinaryOp::Eq => match (&**left, &**right) {
            (Expr::Column { table, column }, value)
                if matches!(value, Expr::Literal(_) | Expr::Parameter(_)) =>
            {
                Some((table.as_deref(), column.as_str(), value))
            }
            (value, Expr::Column { table, column })
                if matches!(value, Expr::Literal(_) | Expr::Parameter(_)) =>
            {
                Some((table.as_deref(), column.as_str(), value))
            }
            _ => None,
        },
        _ => None,
    }
}

fn compile_prepared_simple_value_source(expr: &Expr) -> Option<PreparedSimpleValueSource> {
    match expr {
        Expr::Literal(value) => Some(PreparedSimpleValueSource::Literal(value.clone())),
        Expr::Parameter(number) => Some(PreparedSimpleValueSource::Parameter(*number)),
        _ => None,
    }
}

fn resolve_prepared_simple_value(
    source: &PreparedSimpleValueSource,
    params: &[Value],
) -> Result<Value> {
    match source {
        PreparedSimpleValueSource::Literal(value) => Ok(value.clone()),
        PreparedSimpleValueSource::Parameter(number) => params
            .get(number.saturating_sub(1))
            .cloned()
            .ok_or_else(|| DbError::sql(format!("parameter ${number} was not provided"))),
    }
}

fn cast_prepared_simple_value(value: Value, column_type: ColumnType) -> Result<Value> {
    super::cast_value(value, column_type)
}

fn prepare_simple_delete_restrict_children(
    runtime: &EngineRuntime,
    table: &crate::catalog::TableSchema,
) -> Result<Vec<PreparedSimpleDeleteRestrictChild>> {
    if table.temporary {
        return Ok(Vec::new());
    }

    let mut prepared = Vec::new();
    for child_table in runtime.catalog.tables.values().filter(|child| {
        child
            .foreign_keys
            .iter()
            .any(|foreign_key| identifiers_equal(&foreign_key.referenced_table, &table.name))
    }) {
        for foreign_key in child_table
            .foreign_keys
            .iter()
            .filter(|foreign_key| identifiers_equal(&foreign_key.referenced_table, &table.name))
        {
            match foreign_key.on_delete {
                ForeignKeyAction::NoAction | ForeignKeyAction::Restrict => {}
                _ => return Ok(Vec::new()),
            }
            if foreign_key.columns.len() != 1 {
                return Ok(Vec::new());
            }
            let referenced_columns = if foreign_key.referenced_columns.is_empty() {
                table.primary_key_columns.clone()
            } else {
                foreign_key.referenced_columns.clone()
            };
            if referenced_columns.len() != 1 {
                return Ok(Vec::new());
            }
            let Some(parent_column_index) = table
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, &referenced_columns[0]))
            else {
                return Ok(Vec::new());
            };
            let Some(child_column_index) = child_table
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, &foreign_key.columns[0]))
            else {
                return Ok(Vec::new());
            };
            let child_index_name = runtime.catalog.indexes.values().find_map(|index| {
                (identifiers_equal(&index.table_name, &child_table.name)
                    && index.fresh
                    && index.kind == IndexKind::Btree
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0].expression_sql.is_none()
                    && index.columns[0]
                        .column_name
                        .as_ref()
                        .is_some_and(|entry| identifiers_equal(entry, &foreign_key.columns[0])))
                .then(|| index.name.clone())
            });
            prepared.push(PreparedSimpleDeleteRestrictChild {
                child_table_name: child_table.name.clone(),
                child_column_index,
                child_index_name,
                parent_column_index,
            });
        }
    }
    Ok(prepared)
}

fn prepared_delete_has_referencing_child(
    runtime: &EngineRuntime,
    child: &PreparedSimpleDeleteRestrictChild,
    parent_row: &[Value],
) -> Result<bool> {
    let Some(parent_value) = parent_row.get(child.parent_column_index) else {
        return Err(DbError::internal(format!(
            "parent column index {} is invalid",
            child.parent_column_index
        )));
    };
    if let Some(index_name) = &child.child_index_name {
        let Some(RuntimeIndex::Btree { keys }) = runtime.indexes.get(index_name) else {
            return Ok(false);
        };
        return Ok(!keys.row_ids_for_value_set(parent_value)?.is_empty());
    }
    let Some(rows) = runtime.table_data(&child.child_table_name) else {
        return Ok(false);
    };
    let ci = child.child_column_index;
    match parent_value {
        Value::Int64(parent_int) => Ok(rows.rows.iter().any(|row| {
            matches!(row.values.get(ci), Some(Value::Int64(v)) if *v == *parent_int)
        })),
        Value::Null => Ok(false),
        _ => Ok(rows.rows.iter().any(|row| {
            row.values.get(ci).is_some_and(|value| {
                compare_values(value, parent_value)
                    .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal)
            })
        })),
    }
}

fn apply_runtime_index_update_for_row_change(
    runtime: &mut EngineRuntime,
    table: &crate::catalog::TableSchema,
    index: &crate::catalog::IndexSchema,
    row_id: i64,
    old_row_values: &[Value],
    new_row_values: &[Value],
) -> Result<bool> {
    match index.kind {
        IndexKind::Btree => {
            let old_key = compute_index_key(runtime, index, table, old_row_values)?;
            let new_key = compute_index_key(runtime, index, table, new_row_values)?;
            if old_key == new_key {
                return Ok(true);
            }
            let Some(RuntimeIndex::Btree { keys }) = runtime.indexes.get_mut(&index.name) else {
                return Ok(false);
            };
            if let Some(old_key) = old_key.as_ref() {
                keys.remove_row_id(old_key, row_id)?;
            }
            if let Some(new_key) = new_key {
                keys.insert_row_id(new_key, row_id)?;
            }
            Ok(true)
        }
        IndexKind::Trigram => {
            let old_text = trigram_index_text_for_row(runtime, index, table, old_row_values)?;
            let new_text = trigram_index_text_for_row(runtime, index, table, new_row_values)?;
            let Some(RuntimeIndex::Trigram { index: trigram }) =
                runtime.indexes.get_mut(&index.name)
            else {
                return Ok(false);
            };
            let row_id = u64::try_from(row_id)
                .map_err(|_| DbError::internal(format!("row_id {row_id} is invalid")))?;
            match (old_text, new_text) {
                (Some(old_text), Some(new_text)) => {
                    if old_text != new_text {
                        trigram.queue_replace(row_id, &old_text, &new_text);
                    }
                }
                (Some(old_text), None) => trigram.queue_delete(row_id, &old_text),
                (None, Some(new_text)) => trigram.queue_insert(row_id, &new_text),
                (None, None) => {}
            }
            Ok(true)
        }
    }
}

fn apply_runtime_index_delete_for_row(
    runtime: &mut EngineRuntime,
    table: &crate::catalog::TableSchema,
    index: &crate::catalog::IndexSchema,
    row_id: i64,
    row_values: &[Value],
) -> Result<bool> {
    match index.kind {
        IndexKind::Btree => {
            let key = compute_index_key(runtime, index, table, row_values)?;
            let Some(RuntimeIndex::Btree { keys }) = runtime.indexes.get_mut(&index.name) else {
                return Ok(false);
            };
            if let Some(key) = key.as_ref() {
                keys.remove_row_id(key, row_id)?;
            }
            Ok(true)
        }
        IndexKind::Trigram => {
            let text = trigram_index_text_for_row(runtime, index, table, row_values)?;
            let Some(RuntimeIndex::Trigram { index: trigram }) =
                runtime.indexes.get_mut(&index.name)
            else {
                return Ok(false);
            };
            if let Some(text) = text {
                let row_id = u64::try_from(row_id)
                    .map_err(|_| DbError::internal(format!("row_id {row_id} is invalid")))?;
                trigram.queue_delete(row_id, &text);
            }
            Ok(true)
        }
    }
}

fn trigram_index_text_for_row(
    runtime: &EngineRuntime,
    index: &crate::catalog::IndexSchema,
    table: &crate::catalog::TableSchema,
    row_values: &[Value],
) -> Result<Option<String>> {
    if !row_satisfies_index_predicate(runtime, index, table, row_values)? {
        return Ok(None);
    }
    let value = compute_index_values(runtime, index, table, row_values)?
        .into_iter()
        .next()
        .ok_or_else(|| DbError::constraint("trigram index requires a single text expression"))?;
    let Value::Text(text) = value else {
        return Err(DbError::constraint(
            "trigram index requires a single text expression",
        ));
    };
    Ok(Some(text))
}

fn row_matches_filter(
    runtime: &EngineRuntime,
    table: &crate::catalog::TableSchema,
    row: &StoredRow,
    filter: Option<&Expr>,
    params: &[Value],
) -> Result<bool> {
    let Some(filter) = filter else {
        return Ok(true);
    };
    let eval_values = materialize_row_for_generated(runtime, table, &row.values)?;
    let dataset = table_row_dataset(table, eval_values.as_ref(), &table.name);
    Ok(matches!(
        runtime.eval_expr(
            filter,
            &dataset,
            eval_values.as_ref(),
            params,
            &std::collections::BTreeMap::new(),
            None,
        )?,
        Value::Bool(true)
    ))
}

fn view_match_count(
    runtime: &EngineRuntime,
    view_name: &str,
    filter: Option<&Expr>,
    params: &[Value],
) -> Result<usize> {
    let dataset = runtime.evaluate_from_item(
        &crate::sql::ast::FromItem::Table {
            name: view_name.to_string(),
            alias: None,
        },
        params,
        &std::collections::BTreeMap::new(),
    )?;
    if let Some(filter) = filter {
        let bindings = dataset.columns.clone();
        Ok(dataset
            .rows
            .iter()
            .filter(|row| {
                runtime
                    .eval_expr(
                        filter,
                        &Dataset {
                            columns: bindings.clone(),
                            rows: vec![row.to_vec()],
                        },
                        row,
                        params,
                        &std::collections::BTreeMap::new(),
                        None,
                    )
                    .is_ok_and(|value| matches!(value, Value::Bool(true)))
            })
            .count())
    } else {
        Ok(dataset.rows.len())
    }
}

fn matching_foreign_key_children(
    runtime: &EngineRuntime,
    parent_table: &crate::catalog::TableSchema,
    parent_row: &[Value],
    child_table: &crate::catalog::TableSchema,
    foreign_key: &crate::catalog::ForeignKeyConstraint,
) -> Result<Vec<StoredRow>> {
    let referenced_columns = if foreign_key.referenced_columns.is_empty() {
        parent_table.primary_key_columns.clone()
    } else {
        foreign_key.referenced_columns.clone()
    };
    let parent_key = parent_key_values(parent_table, parent_row, &referenced_columns)?;
    let child_column_indexes = foreign_key
        .columns
        .iter()
        .map(|child_column| {
            child_table
                .columns
                .iter()
                .position(|column| column.name == *child_column)
                .ok_or_else(|| {
                    DbError::internal(format!("unknown child foreign-key column {child_column}"))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let Some(rows) = runtime.table_data_for_schema(child_table, &child_table.name)? else {
        return Ok(Vec::new());
    };
    if let Some(indexed_row_ids) =
        fk_matching_row_ids_via_index(runtime, child_table, foreign_key, &parent_key)?
    {
        let mut matches = Vec::with_capacity(indexed_row_ids.len());
        for row_id in indexed_row_ids {
            let Some(row) = rows.row_by_id(row_id) else {
                continue;
            };
            let is_match =
                child_column_indexes
                    .iter()
                    .zip(&parent_key)
                    .all(|(child_index, parent_value)| {
                        compare_values(&row.values[*child_index], parent_value)
                            .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal)
                    });
            if is_match {
                matches.push(row.clone());
            }
        }
        return Ok(matches);
    }
    Ok(rows
        .rows
        .iter()
        .filter(|row| {
            child_column_indexes
                .iter()
                .zip(&parent_key)
                .all(|(child_index, parent_value)| {
                    compare_values(&row.values[*child_index], parent_value)
                        .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal)
                })
        })
        .cloned()
        .collect())
}

fn fk_matching_row_ids_via_index(
    runtime: &EngineRuntime,
    child_table: &crate::catalog::TableSchema,
    foreign_key: &crate::catalog::ForeignKeyConstraint,
    parent_key: &[Value],
) -> Result<Option<Vec<i64>>> {
    if foreign_key.columns.len() != 1 || parent_key.len() != 1 {
        return Ok(None);
    }
    let child_column = &foreign_key.columns[0];
    let Some(index) = runtime.catalog.indexes.values().find(|index| {
        identifiers_equal(&index.table_name, &child_table.name)
            && index.fresh
            && index.kind == IndexKind::Btree
            && index.predicate_sql.is_none()
            && index.columns.len() == 1
            && index.columns[0].expression_sql.is_none()
            && index.columns[0]
                .column_name
                .as_ref()
                .is_some_and(|entry| identifiers_equal(entry, child_column))
    }) else {
        return Ok(None);
    };
    let Some(RuntimeIndex::Btree { keys }) = runtime.indexes.get(&index.name) else {
        return Ok(None);
    };
    keys.row_ids_for_value(&parent_key[0]).map(Some)
}

fn parent_key_values(
    table: &crate::catalog::TableSchema,
    row: &[Value],
    referenced_columns: &[String],
) -> Result<Vec<Value>> {
    let columns = if referenced_columns.is_empty() {
        &table.primary_key_columns
    } else {
        referenced_columns
    };
    columns
        .iter()
        .map(|column_name| {
            let index = table
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, column_name))
                .ok_or_else(|| {
                    DbError::internal(format!("unknown parent column {}", column_name))
                })?;
            row.get(index)
                .cloned()
                .ok_or_else(|| DbError::internal("row is shorter than its schema"))
        })
        .collect()
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

fn unique_indexes_for_table<'a>(
    runtime: &'a EngineRuntime,
    table: &crate::catalog::TableSchema,
) -> Vec<&'a crate::catalog::IndexSchema> {
    if table.temporary {
        return runtime
            .temp_indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &table.name) && index.unique)
            .collect();
    }
    runtime
        .catalog
        .indexes
        .values()
        .filter(|index| identifiers_equal(&index.table_name, &table.name) && index.unique)
        .collect()
}

fn index_might_change_for_assignments(
    table: &crate::catalog::TableSchema,
    index: &crate::catalog::IndexSchema,
    assignment_columns: &[usize],
) -> bool {
    if index.predicate_sql.is_some() || index.columns.is_empty() {
        return true;
    }
    let Some(indexed_columns) = index
        .columns
        .iter()
        .map(|column| {
            if column.expression_sql.is_some() {
                return None;
            }
            column.column_name.as_ref().and_then(|name| {
                table
                    .columns
                    .iter()
                    .position(|entry| identifiers_equal(&entry.name, name))
            })
        })
        .collect::<Option<Vec<_>>>()
    else {
        return true;
    };
    assignment_columns
        .iter()
        .any(|column_index| indexed_columns.contains(column_index))
}

fn assignment_targets_foreign_key_columns(
    table: &crate::catalog::TableSchema,
    assignment_columns: &[usize],
) -> bool {
    table.foreign_keys.iter().any(|foreign_key| {
        foreign_key.columns.iter().any(|column_name| {
            let Some(column_index) = table
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, column_name))
            else {
                return true;
            };
            assignment_columns.contains(&column_index)
        })
    })
}

fn assignment_targets_referenced_parent_key_columns(
    runtime: &EngineRuntime,
    table: &crate::catalog::TableSchema,
    assignment_columns: &[usize],
) -> bool {
    runtime.catalog.tables.values().any(|child| {
        child
            .foreign_keys
            .iter()
            .filter(|foreign_key| identifiers_equal(&foreign_key.referenced_table, &table.name))
            .any(|foreign_key| {
                let referenced_columns: Cow<'_, [String]> =
                    if foreign_key.referenced_columns.is_empty() {
                        Cow::Borrowed(table.primary_key_columns.as_slice())
                    } else {
                        Cow::Borrowed(foreign_key.referenced_columns.as_slice())
                    };
                if referenced_columns.is_empty() {
                    return true;
                }
                referenced_columns.iter().any(|column_name| {
                    let Some(column_index) = table
                        .columns
                        .iter()
                        .position(|column| identifiers_equal(&column.name, column_name))
                    else {
                        return true;
                    };
                    assignment_columns.contains(&column_index)
                })
            })
    })
}

fn table_has_referencing_non_restrict_delete_actions(
    runtime: &EngineRuntime,
    table_name: &str,
) -> bool {
    runtime.catalog.tables.values().any(|child| {
        child.foreign_keys.iter().any(|foreign_key| {
            identifiers_equal(&foreign_key.referenced_table, table_name)
                && !matches!(
                    foreign_key.on_delete,
                    ForeignKeyAction::NoAction | ForeignKeyAction::Restrict
                )
        })
    })
}

fn validate_assigned_not_null_columns(
    table: &crate::catalog::TableSchema,
    assignment_columns: &[usize],
    next_values: &[Value],
    table_name: &str,
) -> Result<()> {
    for column_index in assignment_columns {
        let Some(column) = table.columns.get(*column_index) else {
            return Err(DbError::internal(format!(
                "column index {} is invalid for {}",
                column_index, table_name
            )));
        };
        if column.nullable {
            continue;
        }
        if matches!(next_values.get(*column_index), Some(Value::Null)) {
            return Err(DbError::constraint(format!(
                "column {}.{} may not be NULL",
                table_name, column.name
            )));
        }
    }
    Ok(())
}
