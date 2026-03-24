//! DML execution helpers.

use crate::catalog::{identifiers_equal, ColumnType, IndexKind, TriggerEvent};
use crate::error::{DbError, Result};
use crate::record::key::encode_index_key;
use crate::record::row::Row;
use crate::record::value::Value;
use crate::sql::ast::{
    Assignment, ConflictAction, ConflictTarget, DeleteStatement, Expr, InsertSource,
    InsertStatement, SelectItem, UpdateStatement,
};
use crate::sql::parser::parse_expression_sql;

use super::row::{ColumnBinding, Dataset, QueryResult, QueryRow};
use super::{compare_values, table_row_dataset, EngineRuntime, RuntimeBtreeKey, StoredRow};

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
pub(crate) struct PreparedSimpleInsert {
    pub(crate) table_name: String,
    pub(crate) columns: Vec<PreparedInsertColumn>,
    pub(crate) primary_auto_row_id_column_index: Option<usize>,
    pub(crate) value_sources: Vec<PreparedInsertValueSource>,
    pub(crate) required_columns: Vec<PreparedRequiredColumn>,
    pub(crate) unique_indexes: Vec<PreparedBtreeIndex>,
    pub(crate) insert_indexes: Vec<PreparedBtreeIndex>,
    pub(crate) use_generic_validation: bool,
    pub(crate) use_generic_index_updates: bool,
}

impl EngineRuntime {
    pub(crate) fn can_execute_insert_in_place(&self, statement: &InsertStatement) -> bool {
        if self.catalog.view(&statement.table_name).is_some() || statement.on_conflict.is_some()
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
        self.catalog.table(&prepared.table_name).is_some()
            && (prepared.use_generic_validation
                || prepared
                    .unique_indexes
                    .iter()
                    .all(|index| self.prepared_btree_index_is_fresh(index)))
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
            .catalog
            .table(&statement.table_name)
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
                identifiers_equal(&column.name, &table.primary_key_columns[0]) && column.auto_increment
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
                || !table.checks.is_empty()
                || !table.foreign_keys.is_empty();
        let mut unique_indexes = Vec::new();
        for index in self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &statement.table_name) && index.unique)
        {
            let Some(prepared_index) = prepare_btree_insert_index(self, table, index)? else {
                use_generic_validation = true;
                unique_indexes.clear();
                break;
            };
            unique_indexes.push(prepared_index);
        }
        let mut use_generic_index_updates = false;
        let mut insert_indexes = Vec::new();
        for index in self
            .catalog
            .indexes
            .values()
            .filter(|index| identifiers_equal(&index.table_name, &statement.table_name) && index.fresh)
        {
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
            unique_indexes,
            insert_indexes,
            use_generic_validation,
            use_generic_index_updates,
        }))
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
        self.mark_table_dirty(table_name);
        Ok(QueryResult::with_affected_rows(1))
    }

    pub(super) fn execute_insert(
        &mut self,
        statement: &InsertStatement,
        params: &[Value],
        page_size: u32,
    ) -> Result<QueryResult> {
        if self.catalog.view(&statement.table_name).is_some() {
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
        let source_rows = materialize_insert_source(self, &statement.source, params)?;
        let mut affected_rows = 0_u64;
        let mut returning_rows = Vec::new();

        for source_row in source_rows {
            let candidate = {
                let mut staged_table = self
                    .catalog
                    .tables
                    .get(&table_name)
                    .cloned()
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
                let candidate = build_insert_row_values(
                    self,
                    &mut staged_table,
                    &statement.columns,
                    source_row,
                    params,
                )?;
                self.catalog
                    .tables
                    .get_mut(&table_name)
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?
                    .next_row_id = staged_table.next_row_id;
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
                    .catalog
                    .tables
                    .get(&table_name)
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
                primary_row_id(table, &candidate).unwrap_or_else(|| next_row_id(self, &table_name))
            };
            let stored_row = StoredRow {
                row_id,
                values: candidate,
            };
            let index_updates =
                self.prepare_insert_index_updates(&table_name, &stored_row, page_size)?;
            self.tables
                .get_mut(&table_name)
                .ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?
                .rows
                .push(stored_row.clone());
            self.apply_insert_index_updates(index_updates)?;
            self.mark_table_dirty(&table_name);
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
        if self.catalog.view(&statement.table_name).is_some() {
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
            .catalog
            .tables
            .get(&table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let matching_row_ids = matching_row_ids(
            self,
            &table,
            self.tables
                .get(&table_name)
                .ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?
                .rows
                .as_slice(),
            statement.filter.as_ref(),
            params,
        )?;

        let mut affected_rows = 0_u64;
        for row_id in matching_row_ids {
            let row_index = self
                .tables
                .get(&table_name)
                .and_then(|data| data.rows.iter().position(|row| row.row_id == row_id))
                .ok_or_else(|| DbError::internal(format!("row {row_id} vanished during UPDATE")))?;
            let current_row = self.tables[&table_name].rows[row_index].clone();
            let mut next_values = current_row.values.clone();
            let dataset = table_row_dataset(&table, &current_row.values, &table_name);
            for assignment in &statement.assignments {
                let column_index = table
                    .columns
                    .iter()
                    .position(|column| column.name == assignment.column_name)
                    .ok_or_else(|| {
                        DbError::sql(format!("unknown column {}", assignment.column_name))
                    })?;
                let value = self.eval_expr(
                    &assignment.expr,
                    &dataset,
                    &current_row.values,
                    params,
                    &std::collections::BTreeMap::new(),
                    None,
                )?;
                next_values[column_index] =
                    super::cast_value(value, table.columns[column_index].column_type)?;
            }
            self.apply_parent_update_actions(
                &table_name,
                &table,
                &current_row.values,
                &next_values,
                params,
            )?;
            self.validate_row(&table_name, &next_values, Some(row_id), params)?;
            self.tables
                .get_mut(&table_name)
                .ok_or_else(|| {
                    DbError::internal(format!("table data for {table_name} is missing"))
                })?
                .rows[row_index]
                .values = next_values;
            affected_rows += 1;
        }

        if affected_rows > 0 {
            self.mark_indexes_stale_for_table(&table_name);
            self.mark_table_dirty(&table_name);
        }

        self.execute_after_triggers(
            &table_name,
            TriggerEvent::Update,
            affected_rows as usize,
            page_size,
        )?;
        Ok(QueryResult::with_affected_rows(affected_rows))
    }

    pub(super) fn execute_delete(
        &mut self,
        statement: &DeleteStatement,
        params: &[Value],
        page_size: u32,
    ) -> Result<QueryResult> {
        if self.catalog.view(&statement.table_name).is_some() {
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
            .catalog
            .tables
            .get(&table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let rows = self
            .tables
            .get(&table_name)
            .ok_or_else(|| DbError::internal(format!("table data for {table_name} is missing")))?
            .rows
            .clone();
        let matching_rows = rows
            .into_iter()
            .filter(|row| {
                row_matches_filter(self, &table, row, statement.filter.as_ref(), params)
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        for row in &matching_rows {
            self.apply_parent_delete_actions(&table_name, &table, &row.values, params)?;
        }

        let matching_row_ids = matching_rows
            .iter()
            .map(|row| row.row_id)
            .collect::<Vec<_>>();
        self.tables
            .get_mut(&table_name)
            .ok_or_else(|| DbError::internal(format!("table data for {table_name} is missing")))?
            .rows
            .retain(|row| !matching_row_ids.contains(&row.row_id));

        if !matching_row_ids.is_empty() {
            self.mark_indexes_stale_for_table(&table_name);
            self.mark_table_dirty(&table_name);
        }

        self.execute_after_triggers(
            &table_name,
            TriggerEvent::Delete,
            matching_rows.len(),
            page_size,
        )?;
        Ok(QueryResult::with_affected_rows(matching_rows.len() as u64))
    }

    fn render_returning(
        &self,
        table_name: &str,
        rows: &[StoredRow],
        items: &[SelectItem],
        params: &[Value],
    ) -> Result<QueryResult> {
        let table = self
            .catalog
            .tables
            .get(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let dataset = Dataset {
            columns: table
                .columns
                .iter()
                .map(|column| ColumnBinding {
                    table: Some(table_name.to_string()),
                    name: column.name.clone(),
                })
                .collect(),
            rows: rows.iter().map(|row| row.values.clone()).collect(),
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
            .catalog
            .tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let row_index = self
            .tables
            .get(table_name)
            .and_then(|data| data.rows.iter().position(|row| row.row_id == row_id))
            .ok_or_else(|| DbError::internal(format!("row {row_id} vanished during UPSERT")))?;
        let current_row = self.tables[table_name].rows[row_index].clone();
        let dataset = table_row_dataset(&table, &current_row.values, table_name);
        let excluded = Dataset {
            columns: table
                .columns
                .iter()
                .map(|column| ColumnBinding {
                    table: Some("excluded".to_string()),
                    name: column.name.clone(),
                })
                .collect(),
            rows: vec![excluded_values.to_vec()],
        };
        if let Some(filter) = filter {
            if !matches!(
                self.eval_expr(
                    filter,
                    &dataset,
                    &current_row.values,
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
            let value = self.eval_expr(
                &assignment.expr,
                &dataset,
                &current_row.values,
                params,
                &std::collections::BTreeMap::new(),
                Some(&excluded),
            )?;
            next_values[column_index] =
                super::cast_value(value, table.columns[column_index].column_type)?;
        }
        self.apply_parent_update_actions(
            table_name,
            &table,
            &current_row.values,
            &next_values,
            params,
        )?;
        self.validate_row(table_name, &next_values, Some(row_id), params)?;
        self.tables
            .get_mut(table_name)
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
                        self.tables
                            .get_mut(&child_table.name)
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
                                .tables
                                .get(&child_table.name)
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
                                    .tables
                                    .get_mut(&child_table.name)
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
                                        .position(|column| identifiers_equal(&column.name, column_name))
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
                            self.validate_row(
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
                                .tables
                                .get(&child_table.name)
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
                                    .tables
                                    .get_mut(&child_table.name)
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
                            self.validate_row(
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
                                .tables
                                .get(&child_table.name)
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
                                    .tables
                                    .get_mut(&child_table.name)
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
                            self.validate_row(
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
            .catalog
            .tables
            .get(&table_name)
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

        self.catalog
            .tables
            .get_mut(&table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?
            .next_row_id = staged_table.next_row_id;
        self.tables
            .get_mut(&table_name)
            .ok_or_else(|| DbError::internal(format!("table data for {table_name} is missing")))?
            .rows
            .push(stored_row.clone());
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
    runtime.coerce_row_values(table, resolved)
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
    Ok(())
}

fn apply_prepared_insert_index_updates(
    runtime: &mut EngineRuntime,
    prepared: &PreparedSimpleInsert,
    row: &StoredRow,
    check_unique: bool,
) -> Result<()> {
    for index in &prepared.insert_indexes {
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
        if check_unique
            && index.unique
            && !prepared_index_contains_null(index, &row.values)
            && keys.contains_any(&key)
        {
            return Err(DbError::constraint(format!(
                "unique constraint {} on {} was violated",
                index.name, prepared.table_name
            )));
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
    let table = runtime
        .catalog
        .tables
        .get_mut(table_name)
        .expect("table must exist for row-id allocation");
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
    rows: &[StoredRow],
    filter: Option<&Expr>,
    params: &[Value],
) -> Result<Vec<i64>> {
    rows.iter()
        .filter(|row| row_matches_filter(runtime, table, row, filter, params).unwrap_or(false))
        .map(|row| Ok(row.row_id))
        .collect()
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
    let dataset = table_row_dataset(table, &row.values, &table.name);
    Ok(matches!(
        runtime.eval_expr(
            filter,
            &dataset,
            &row.values,
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
    let rows = runtime
        .tables
        .get(&child_table.name)
        .map(|data| data.rows.clone())
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter(|row| {
            foreign_key
                .columns
                .iter()
                .zip(&parent_key)
                .all(|(child_column, parent_value)| {
                    let child_index = child_table
                        .columns
                        .iter()
                        .position(|column| column.name == *child_column)
                        .expect("child FK column must exist");
                    compare_values(&row.values[child_index], parent_value)
                        .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal)
                })
        })
        .collect())
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
