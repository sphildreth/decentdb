//! DDL execution helpers.

use crate::catalog::{
    identifiers_equal, CheckConstraint, ColumnSchema, ForeignKeyAction, ForeignKeyConstraint,
    IndexColumn, IndexKind, IndexSchema, SchemaInfo, TableSchema,
};
use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::sql::ast::{
    AlterTableAction, ColumnDefinition, CreateIndexStatement, CreateTableStatement, Expr,
    ForeignKeyActionSpec, ForeignKeyDefinition, IndexExpression, TableConstraint,
};
use crate::sql::parser::parse_expression_sql;

use super::constraints::auto_index_name;
use super::{table_row_dataset, EngineRuntime, StoredRow, TableData, TableRowSource};
use std::sync::Arc;

impl EngineRuntime {
    pub(super) fn execute_create_schema(&mut self, name: &str, if_not_exists: bool) -> Result<()> {
        if self.catalog.schema(name).is_some() {
            if if_not_exists {
                return Ok(());
            }
            return Err(DbError::sql(format!("schema {} already exists", name)));
        }
        if self.catalog.contains_non_schema_object(name) {
            return Err(DbError::sql(format!("object {} already exists", name)));
        }
        self.catalog_mut().schemas.insert(
            name.to_string(),
            SchemaInfo {
                name: name.to_string(),
            },
        );
        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_create_table(&mut self, statement: &CreateTableStatement) -> Result<()> {
        if statement.temporary {
            if self.temp_relation_exists(&statement.table_name) {
                if statement.if_not_exists
                    && self.temp_table_schema(&statement.table_name).is_some()
                {
                    return Ok(());
                }
                return Err(DbError::sql(format!(
                    "object {} already exists",
                    statement.table_name
                )));
            }
        } else if self.catalog.contains_object(&statement.table_name) {
            if statement.if_not_exists && self.catalog.table(&statement.table_name).is_some() {
                return Ok(());
            }
            return Err(DbError::sql(format!(
                "object {} already exists",
                statement.table_name
            )));
        }

        let mut columns = statement
            .columns
            .iter()
            .map(column_schema_from_definition)
            .collect::<Result<Vec<_>>>()?;
        ensure_unique_column_names(&columns, &statement.table_name)?;

        let mut table_checks = Vec::new();
        let mut foreign_keys = Vec::new();
        let mut primary_key_columns = columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let mut secondary_unique_indexes = Vec::<(Option<String>, Vec<String>)>::new();

        for constraint in &statement.constraints {
            match constraint {
                TableConstraint::PrimaryKey { name: _, columns } => {
                    primary_key_columns = columns.clone();
                }
                TableConstraint::Unique { name, columns } => {
                    secondary_unique_indexes.push((name.clone(), columns.clone()));
                }
                TableConstraint::Check { name, expr } => {
                    table_checks.push(CheckConstraint {
                        name: name.clone(),
                        expression_sql: expr.to_sql(),
                    });
                }
                TableConstraint::ForeignKey(foreign_key) => {
                    foreign_keys.push(foreign_key_constraint_from_definition(foreign_key));
                }
            }
        }

        for column in &columns {
            if let Some(foreign_key) = &column.foreign_key {
                foreign_keys.push(foreign_key.clone());
            }
            if column.unique && !column.primary_key {
                secondary_unique_indexes.push((None, vec![column.name.clone()]));
            }
        }

        for primary_key_column in &primary_key_columns {
            let column = columns
                .iter_mut()
                .find(|column| identifiers_equal(&column.name, primary_key_column))
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "primary key column {} does not exist on {}",
                        primary_key_column, statement.table_name
                    ))
                })?;
            if column.generated_sql.is_some() {
                return Err(DbError::sql(format!(
                    "generated column {} may not be part of PRIMARY KEY",
                    column.name
                )));
            }
            column.primary_key = true;
            column.unique = true;
            column.nullable = false;
            if column.column_type == crate::catalog::ColumnType::Int64 {
                column.auto_increment = true;
            }
        }

        let table = TableSchema {
            name: statement.table_name.clone(),
            temporary: statement.temporary,
            columns,
            checks: table_checks,
            foreign_keys,
            primary_key_columns,
            next_row_id: 1,
            pk_index_root: None,
        };
        validate_generated_columns(self, &table)?;
        if table.temporary {
            let mut temp_indexes = Vec::new();
            if !table.foreign_keys.is_empty() {
                return Err(DbError::sql(
                    "foreign keys are not supported on temporary tables",
                ));
            }
            if !table.primary_key_columns.is_empty() {
                temp_indexes.push(IndexSchema {
                    name: auto_index_name("pk", &table.name, &table.primary_key_columns),
                    table_name: table.name.clone(),
                    kind: IndexKind::Btree,
                    unique: true,
                    columns: table
                        .primary_key_columns
                        .iter()
                        .map(|column_name| IndexColumn {
                            column_name: Some(column_name.clone()),
                            expression_sql: None,
                        })
                        .collect(),
                    include_columns: Vec::new(),
                    predicate_sql: None,
                    fresh: false,
                });
            }
            for (name, columns) in secondary_unique_indexes {
                temp_indexes.push(IndexSchema {
                    name: name.unwrap_or_else(|| auto_index_name("uq", &table.name, &columns)),
                    table_name: table.name.clone(),
                    kind: IndexKind::Btree,
                    unique: true,
                    columns: columns
                        .iter()
                        .map(|column_name| IndexColumn {
                            column_name: Some(column_name.clone()),
                            expression_sql: None,
                        })
                        .collect(),
                    include_columns: Vec::new(),
                    predicate_sql: None,
                    fresh: false,
                });
            }
            self.temp_tables_mut()
                .insert(statement.table_name.clone(), table);
            self.temp_table_data_map_mut()
                .insert(statement.table_name.clone(), Arc::new(TableData::default()));
            for index in temp_indexes {
                self.temp_indexes_mut().insert(index.name.clone(), index);
            }
            self.bump_temp_schema_cookie();
            return Ok(());
        }
        validate_foreign_keys(self, &table)?;

        self.catalog_mut()
            .tables
            .insert(statement.table_name.clone(), table.clone());
        self.tables_mut()
            .insert(statement.table_name.clone(), TableData::default().into());

        if !table.primary_key_columns.is_empty() {
            self.insert_index_schema(IndexSchema {
                name: auto_index_name("pk", &table.name, &table.primary_key_columns),
                table_name: table.name.clone(),
                kind: IndexKind::Btree,
                unique: true,
                columns: table
                    .primary_key_columns
                    .iter()
                    .map(|column_name| IndexColumn {
                        column_name: Some(column_name.clone()),
                        expression_sql: None,
                    })
                    .collect(),
                include_columns: Vec::new(),
                predicate_sql: None,
                fresh: true,
            })?;
        }

        for (name, columns) in secondary_unique_indexes {
            self.insert_index_schema(IndexSchema {
                name: name.unwrap_or_else(|| auto_index_name("uq", &table.name, &columns)),
                table_name: table.name.clone(),
                kind: IndexKind::Btree,
                unique: true,
                columns: columns
                    .iter()
                    .map(|column_name| IndexColumn {
                        column_name: Some(column_name.clone()),
                        expression_sql: None,
                    })
                    .collect(),
                include_columns: Vec::new(),
                predicate_sql: None,
                fresh: true,
            })?;
        }

        for foreign_key in &table.foreign_keys {
            let index_name = format!(
                "{}_idx",
                auto_index_name("fk", &table.name, &foreign_key.columns)
            );
            if !self.catalog.indexes.contains_key(&index_name) {
                self.insert_index_schema(IndexSchema {
                    name: index_name,
                    table_name: table.name.clone(),
                    kind: IndexKind::Btree,
                    unique: false,
                    columns: foreign_key
                        .columns
                        .iter()
                        .map(|column_name| IndexColumn {
                            column_name: Some(column_name.clone()),
                            expression_sql: None,
                        })
                        .collect(),
                    include_columns: Vec::new(),
                    predicate_sql: None,
                    fresh: true,
                })?;
            }
        }

        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_create_index(
        &mut self,
        statement: &CreateIndexStatement,
        _page_size: u32,
    ) -> Result<()> {
        if self.catalog.contains_object(&statement.index_name) {
            if statement.if_not_exists && self.catalog.indexes.contains_key(&statement.index_name) {
                return Ok(());
            }
            return Err(DbError::sql(format!(
                "object {} already exists",
                statement.index_name
            )));
        }
        if self.visible_table_is_temporary(&statement.table_name) {
            return Err(DbError::sql(format!(
                "cannot create indexes on temporary table {}",
                statement.table_name
            )));
        }
        if self
            .visible_view(&statement.table_name, super::NameResolutionScope::Session)
            .is_some()
        {
            return Err(DbError::sql(format!(
                "cannot create an index on view {}",
                statement.table_name
            )));
        }
        let table = self
            .table_schema(&statement.table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {}", statement.table_name)))?;

        let access_method = if statement.access_method.is_empty() {
            "btree".to_string()
        } else {
            statement.access_method.to_ascii_lowercase()
        };
        let kind = match access_method.as_str() {
            "btree" => IndexKind::Btree,
            "gin" | "trigram" => IndexKind::Trigram,
            other => {
                return Err(DbError::sql(format!(
                    "unsupported index access method {other}"
                )))
            }
        };

        let has_expression = statement
            .columns
            .iter()
            .any(|column| matches!(column, IndexExpression::Expr(_)));
        if kind == IndexKind::Trigram {
            if statement.unique {
                return Err(DbError::sql("trigram indexes cannot be UNIQUE"));
            }
            if statement.columns.len() != 1 || has_expression {
                return Err(DbError::sql(
                    "trigram indexes require a single plain column key",
                ));
            }
            if statement.predicate.is_some() {
                return Err(DbError::sql("partial trigram indexes are not supported"));
            }
            if !statement.include_columns.is_empty() {
                return Err(DbError::sql(
                    "trigram indexes do not support INCLUDE columns",
                ));
            }
        }
        if has_expression {
            if kind != IndexKind::Btree {
                return Err(DbError::sql("expression indexes must use BTREE"));
            }
            if statement.unique {
                return Err(DbError::sql("UNIQUE expression indexes are not supported"));
            }
            if statement.columns.len() != 1 {
                return Err(DbError::sql(
                    "expression indexes must define exactly one key expression",
                ));
            }
            if statement.predicate.is_some() {
                return Err(DbError::sql("partial expression indexes are not supported"));
            }
            if !statement.include_columns.is_empty() {
                return Err(DbError::sql(
                    "expression indexes do not support INCLUDE columns",
                ));
            }
        }
        if let Some(_predicate) = &statement.predicate {
            if kind != IndexKind::Btree || statement.columns.len() != 1 {
                return Err(DbError::sql(
                    "only single-column BTREE partial indexes are supported",
                ));
            }
            let _column_name = match &statement.columns[0] {
                IndexExpression::Column(column_name) => column_name,
                IndexExpression::Expr(_) => {
                    return Err(DbError::sql(
                        "partial indexes require a plain indexed column",
                    ))
                }
            };
        }

        for column in &statement.columns {
            if let IndexExpression::Column(column_name) = column {
                if !table
                    .columns
                    .iter()
                    .any(|column| identifiers_equal(&column.name, column_name))
                {
                    return Err(DbError::sql(format!(
                        "index column {} does not exist on {}",
                        column_name, table.name
                    )));
                }
            }
        }
        for (include_index, include_column) in statement.include_columns.iter().enumerate() {
            if !table
                .columns
                .iter()
                .any(|column| identifiers_equal(&column.name, include_column))
            {
                return Err(DbError::sql(format!(
                    "index INCLUDE column {} does not exist on {}",
                    include_column, table.name
                )));
            }
            if statement.columns.iter().any(|column| {
                matches!(
                    column,
                    IndexExpression::Column(column_name)
                        if identifiers_equal(column_name, include_column)
                )
            }) {
                return Err(DbError::sql(format!(
                    "index INCLUDE column {} duplicates key column",
                    include_column
                )));
            }
            if statement
                .include_columns
                .iter()
                .take(include_index)
                .any(|column| identifiers_equal(column, include_column))
            {
                return Err(DbError::sql(format!(
                    "index INCLUDE column {} is duplicated",
                    include_column
                )));
            }
        }

        self.insert_index_schema(IndexSchema {
            name: statement.index_name.clone(),
            table_name: statement.table_name.clone(),
            kind,
            unique: statement.unique,
            columns: statement
                .columns
                .iter()
                .map(|column| match column {
                    IndexExpression::Column(column_name) => IndexColumn {
                        column_name: Some(column_name.clone()),
                        expression_sql: None,
                    },
                    IndexExpression::Expr(expr) => IndexColumn {
                        column_name: None,
                        expression_sql: Some(expr.to_sql()),
                    },
                })
                .collect(),
            include_columns: statement.include_columns.clone(),
            predicate_sql: statement
                .predicate
                .as_ref()
                .map(|predicate| predicate.to_sql()),
            fresh: true,
        })?;

        // Drop any redundant auto FK index that covers the same column(s).
        if kind == IndexKind::Btree && !has_expression {
            let column_names: Vec<String> = statement
                .columns
                .iter()
                .filter_map(|c| {
                    if let IndexExpression::Column(n) = c {
                        Some(n.clone())
                    } else {
                        None
                    }
                })
                .collect();
            if !column_names.is_empty() {
                let fk_auto_name = format!(
                    "{}_idx",
                    auto_index_name("fk", &statement.table_name, &column_names)
                );
                if self.catalog.indexes.contains_key(&fk_auto_name) {
                    self.catalog_mut().indexes.remove(&fk_auto_name);
                    self.indexes_mut().remove(&fk_auto_name);
                }
            }
        }

        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_drop_table(
        &mut self,
        name: &str,
        if_exists: bool,
        _page_size: u32,
    ) -> Result<()> {
        if self.temp_view(name).is_some() {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::sql(format!("unknown table {name}")));
        }
        if let Some(table_name) = self.temp_table_schema(name).map(|table| table.name.clone()) {
            let dependent_views = super::views::dependent_views(self, &table_name, true);
            if !dependent_views.is_empty() {
                return Err(DbError::sql(format!(
                    "cannot drop table {} because views depend on it: {}",
                    table_name,
                    dependent_views.join(", ")
                )));
            }
            self.temp_tables_mut().remove(&table_name);
            self.temp_table_data_map_mut().remove(&table_name);
            self.temp_indexes_mut()
                .retain(|_, index| !identifiers_equal(&index.table_name, &table_name));
            self.bump_temp_schema_cookie();
            return Ok(());
        }
        let Some(table_name) = self.catalog.table(name).map(|table| table.name.clone()) else {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::sql(format!("unknown table {name}")));
        };
        let dependent_views = super::views::dependent_views(self, &table_name, false);
        if !dependent_views.is_empty() {
            return Err(DbError::sql(format!(
                "cannot drop table {} because views depend on it: {}",
                table_name,
                dependent_views.join(", ")
            )));
        }
        let referencing_tables = self
            .catalog
            .tables
            .values()
            .filter(|table| {
                table.foreign_keys.iter().any(|foreign_key| {
                    identifiers_equal(&foreign_key.referenced_table, &table_name)
                })
            })
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        if !referencing_tables.is_empty() {
            return Err(DbError::sql(format!(
                "cannot drop table {} because foreign keys still reference it from {}",
                table_name,
                referencing_tables.join(", ")
            )));
        }

        self.catalog_mut().tables.remove(&table_name);
        self.tables_mut().remove(&table_name);
        self.catalog_mut()
            .indexes
            .retain(|_, index| !identifiers_equal(&index.table_name, &table_name));
        self.catalog_mut().triggers.retain(|_, trigger| {
            !identifiers_equal(&trigger.target_name, &table_name) || trigger.on_view
        });
        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_drop_index(&mut self, name: &str, if_exists: bool) -> Result<()> {
        let Some(index) = self.catalog.indexes.get(name).cloned() else {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::sql(format!("unknown index {name}")));
        };
        if index.unique {
            return Err(DbError::sql(format!(
                "dropping unique index {} is not supported in DecentDB 1.0",
                name
            )));
        }
        self.catalog_mut().indexes.remove(name);
        self.indexes_mut().remove(name);
        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_truncate_table(
        &mut self,
        table_name: &str,
        restart_identity: bool,
        cascade: bool,
        page_size: u32,
    ) -> Result<()> {
        if self.temp_table_schema(table_name).is_some() {
            return Err(DbError::sql(
                "TRUNCATE TABLE is not supported for temporary tables",
            ));
        }
        if self.temp_view(table_name).is_some() {
            return Err(DbError::sql(format!("cannot truncate view {}", table_name)));
        }

        let table_name = self
            .catalog
            .table(table_name)
            .map(|table| table.name.clone())
            .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
        let mut targets = Vec::new();
        let mut visited = std::collections::BTreeSet::new();
        collect_truncate_targets(
            &self.catalog,
            &table_name,
            cascade,
            &mut visited,
            &mut targets,
        )?;

        for target in &targets {
            self.materialize_table_row_source(target)?;
            let entry = self.tables_mut().get_mut(target).ok_or_else(|| {
                DbError::internal(format!("table data for {} is missing", target))
            })?;
            let data = entry.resident_data_mut();
            data.rows.clear();

            if restart_identity {
                let table = self.catalog_mut().tables.get_mut(target).ok_or_else(|| {
                    DbError::internal(format!("table schema for {} is missing", target))
                })?;
                table.next_row_id = 1;
            }

            self.mark_table_dirty(target);
            self.mark_indexes_stale_for_table(target);
            self.catalog_mut()
                .table_stats
                .insert(target.clone(), super::TableStats { row_count: 0 });
        }

        self.rebuild_indexes(page_size)?;

        Ok(())
    }

    pub(super) fn execute_alter_table(
        &mut self,
        table_name: &str,
        actions: &[AlterTableAction],
        params: &[Value],
        _page_size: u32,
    ) -> Result<()> {
        if self.temp_table_schema(table_name).is_some() {
            return Err(DbError::sql(
                "ALTER TABLE is not supported for temporary tables",
            ));
        }
        if self.temp_view(table_name).is_some() && self.catalog.table(table_name).is_none() {
            return Err(DbError::sql(format!("unknown table {table_name}")));
        }
        if actions
            .iter()
            .any(|action| matches!(action, AlterTableAction::RenameTable { .. }))
        {
            if actions.len() != 1 {
                return Err(DbError::sql(
                    "ALTER TABLE RENAME TO cannot be combined with other ALTER TABLE actions",
                ));
            }
            let AlterTableAction::RenameTable { new_name } = &actions[0] else {
                return Err(DbError::internal(
                    "ALTER TABLE RENAME dispatch reached a non-rename action",
                ));
            };
            return self.execute_alter_table_rename(table_name, new_name);
        }
        if actions.iter().any(|action| {
            matches!(
                action,
                AlterTableAction::AddConstraint(_) | AlterTableAction::DropConstraint { .. }
            )
        }) {
            if actions.len() != 1 {
                return Err(DbError::sql(
                    "ALTER TABLE ADD/DROP CONSTRAINT cannot be combined with other ALTER TABLE actions",
                ));
            }
            return self.execute_alter_table_constraint(table_name, &actions[0], _page_size);
        }
        let mut table = self
            .catalog
            .tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
        if table
            .columns
            .iter()
            .any(|column| column.generated_sql.is_some())
        {
            return Err(DbError::sql(
                "ALTER TABLE is not supported for tables with generated columns",
            ));
        }
        if !table.checks.is_empty() || table.columns.iter().any(|column| !column.checks.is_empty())
        {
            return Err(DbError::sql(
                "ALTER TABLE is rejected on tables that define CHECK constraints",
            ));
        }
        if self.catalog.indexes.values().any(|index| {
            index.table_name == table_name
                && index
                    .columns
                    .iter()
                    .any(|column| column.expression_sql.is_some())
        }) {
            return Err(DbError::sql(
                "ALTER TABLE is rejected on tables that define expression indexes",
            ));
        }
        self.materialize_table_row_source(table_name)?;
        for action in actions {
            match action {
                AlterTableAction::AddColumn(definition) => {
                    if definition.generated.is_some() {
                        return Err(DbError::sql(
                            "ALTER TABLE ADD COLUMN does not support generated columns",
                        ));
                    }
                    if table
                        .columns
                        .iter()
                        .any(|column| column.name == definition.name)
                    {
                        return Err(DbError::sql(format!(
                            "column {} already exists on {}",
                            definition.name, table_name
                        )));
                    }
                    if definition.primary_key
                        || definition.unique
                        || definition.references.is_some()
                    {
                        return Err(DbError::sql(
                            "ALTER TABLE ADD COLUMN with PRIMARY KEY, UNIQUE, or REFERENCES is not supported",
                        ));
                    }
                    let column = column_schema_from_definition(definition)?;
                    let fill_value = if let Some(default) = &column.default_sql {
                        let expr = crate::sql::parser::parse_expression_sql(default)?;
                        self.eval_expr(
                            &expr,
                            &super::row::Dataset::empty(),
                            &[],
                            params,
                            &std::collections::BTreeMap::new(),
                            None,
                        )?
                    } else {
                        Value::Null
                    };
                    let fill_value = super::cast_value(fill_value, column.column_type)?;
                    let has_rows = !self
                        .tables
                        .get(table_name)
                        .ok_or_else(|| {
                            DbError::internal(format!("table data for {table_name} is missing"))
                        })?
                        .resident_data()
                        .rows
                        .is_empty();
                    if !column.nullable && matches!(fill_value, Value::Null) && has_rows {
                        return Err(DbError::constraint(format!(
                            "cannot add NOT NULL column {} without a non-NULL default",
                            column.name
                        )));
                    }
                    {
                        let entry = self.tables_mut().get_mut(table_name).ok_or_else(|| {
                            DbError::internal(format!("table data for {table_name} is missing"))
                        })?;
                        for row in &mut entry.resident_data_mut().rows {
                            row.values.push(fill_value.clone());
                        }
                    }
                    table.columns.push(column);
                }
                AlterTableAction::DropColumn { column_name } => {
                    if table
                        .primary_key_columns
                        .iter()
                        .any(|column| column == column_name)
                    {
                        return Err(DbError::sql(format!(
                            "cannot drop primary-key column {}",
                            column_name
                        )));
                    }
                    if table.foreign_keys.iter().any(|foreign_key| {
                        foreign_key
                            .columns
                            .iter()
                            .any(|column| column == column_name)
                    }) {
                        return Err(DbError::sql(format!(
                            "cannot drop foreign-key column {}",
                            column_name
                        )));
                    }
                    if self.catalog.indexes.values().any(|index| {
                        index.table_name == table_name
                            && (index
                                .columns
                                .iter()
                                .any(|column| column.column_name.as_deref() == Some(column_name))
                                || index.include_columns.iter().any(|name| name == column_name))
                    }) {
                        return Err(DbError::sql(format!(
                            "cannot drop indexed column {}",
                            column_name
                        )));
                    }
                    let index = table
                        .columns
                        .iter()
                        .position(|column| column.name == *column_name)
                        .ok_or_else(|| DbError::sql(format!("unknown column {column_name}")))?;
                    table.columns.remove(index);
                    {
                        let entry = self.tables_mut().get_mut(table_name).ok_or_else(|| {
                            DbError::internal(format!("table data for {table_name} is missing"))
                        })?;
                        for row in &mut entry.resident_data_mut().rows {
                            row.values.remove(index);
                        }
                    }
                }
                AlterTableAction::RenameColumn { old_name, new_name } => {
                    if !super::views::dependent_views(self, table_name, false).is_empty() {
                        return Err(DbError::sql(
                            "RENAME COLUMN is rejected when dependent views exist",
                        ));
                    }
                    if table.columns.iter().any(|column| column.name == *new_name) {
                        return Err(DbError::sql(format!(
                            "column {} already exists on {}",
                            new_name, table_name
                        )));
                    }
                    let column_index = table
                        .columns
                        .iter()
                        .position(|column| column.name == *old_name)
                        .ok_or_else(|| DbError::sql(format!("unknown column {old_name}")))?;
                    table.columns[column_index].name = new_name.clone();
                    rename_column_references(self, table_name, old_name, new_name);
                }
                AlterTableAction::AlterColumnType {
                    column_name,
                    new_type,
                } => {
                    let index = table
                        .columns
                        .iter()
                        .position(|column| column.name == *column_name)
                        .ok_or_else(|| DbError::sql(format!("unknown column {column_name}")))?;
                    if !matches!(
                        table.columns[index].column_type,
                        crate::catalog::ColumnType::Int64
                            | crate::catalog::ColumnType::Float64
                            | crate::catalog::ColumnType::Text
                            | crate::catalog::ColumnType::Bool
                    ) || !matches!(
                        new_type,
                        crate::catalog::ColumnType::Int64
                            | crate::catalog::ColumnType::Float64
                            | crate::catalog::ColumnType::Text
                            | crate::catalog::ColumnType::Bool
                    ) {
                        return Err(DbError::sql(
                            "ALTER COLUMN TYPE supports only INT64, FLOAT64, TEXT, and BOOL",
                        ));
                    }
                    if table.columns[index].primary_key {
                        return Err(DbError::sql(format!(
                            "cannot alter the type of primary-key column {}",
                            column_name
                        )));
                    }
                    if table.foreign_keys.iter().any(|foreign_key| {
                        foreign_key
                            .columns
                            .iter()
                            .any(|column| column == column_name)
                    }) {
                        return Err(DbError::sql(format!(
                            "cannot alter the type of foreign-key child column {}",
                            column_name
                        )));
                    }
                    if self.catalog.tables.values().any(|other| {
                        other.foreign_keys.iter().any(|foreign_key| {
                            foreign_key.referenced_table == table_name
                                && foreign_key
                                    .referenced_columns
                                    .iter()
                                    .any(|column| column == column_name)
                        })
                    }) {
                        return Err(DbError::sql(format!(
                            "cannot alter the type of referenced parent column {}",
                            column_name
                        )));
                    }
                    {
                        let entry = self.tables_mut().get_mut(table_name).ok_or_else(|| {
                            DbError::internal(format!("table data for {table_name} is missing"))
                        })?;
                        for row in &mut entry.resident_data_mut().rows {
                            row.values[index] =
                                super::cast_value(row.values[index].clone(), *new_type)?;
                        }
                    }
                    table.columns[index].column_type = *new_type;
                }
                AlterTableAction::RenameTable { .. } => {
                    return Err(DbError::internal(
                        "ALTER TABLE RENAME action should have been dispatched earlier",
                    ));
                }
                AlterTableAction::AddConstraint(_) | AlterTableAction::DropConstraint { .. } => {
                    return Err(DbError::internal(
                        "ALTER TABLE constraint action should have been dispatched earlier",
                    ));
                }
            }
        }

        self.catalog_mut()
            .tables
            .insert(table_name.to_string(), table);
        self.mark_table_dirty(table_name);
        self.bump_schema_cookie();
        Ok(())
    }

    fn materialize_table_row_source(&mut self, table_name: &str) -> Result<()> {
        if !matches!(
            self.table_row_source(table_name),
            Some(TableRowSource::Paged(_))
        ) {
            return Ok(());
        }
        let row_source = self
            .table_row_source(table_name)
            .ok_or_else(|| DbError::internal(format!("table data for {table_name} is missing")))?;
        let mut rows = Vec::with_capacity(row_source.row_count());
        for row in row_source.rows() {
            let row = row?;
            rows.push(StoredRow {
                row_id: row.row_id(),
                values: row.values().to_vec(),
            });
        }
        self.replace_table_row_source(table_name, TableData::from_rows(rows).into())
    }

    fn execute_alter_table_constraint(
        &mut self,
        table_name: &str,
        action: &AlterTableAction,
        page_size: u32,
    ) -> Result<()> {
        let table_name = self
            .catalog
            .table(table_name)
            .map(|table| table.name.clone())
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
        self.materialize_table_row_source(&table_name)?;
        let mut table = self
            .catalog
            .tables
            .get(&table_name)
            .cloned()
            .ok_or_else(|| {
                DbError::internal(format!("table schema for {} is missing", table_name))
            })?;
        match action {
            AlterTableAction::AddConstraint(TableConstraint::Check { name, expr }) => {
                if let Some(constraint_name) = name {
                    ensure_constraint_name_is_available(
                        self,
                        &table,
                        &table_name,
                        constraint_name,
                    )?;
                }
                let expression_sql = expr.to_sql();
                let candidate = CheckConstraint {
                    name: name.clone(),
                    expression_sql: expression_sql.clone(),
                };
                if table
                    .checks
                    .iter()
                    .any(|check| check.expression_sql == expression_sql)
                {
                    return Err(DbError::sql(format!(
                        "constraint expression already exists on {}",
                        table_name
                    )));
                }
                let probe_table = {
                    let mut probe = table.clone();
                    probe.checks.push(candidate.clone());
                    probe
                };
                for row in &self
                    .tables
                    .get(&table_name)
                    .ok_or_else(|| {
                        DbError::internal(format!("table data for {table_name} is missing"))
                    })?
                    .resident_data()
                    .rows
                {
                    let expr = parse_expression_sql(&candidate.expression_sql)?;
                    let dataset = table_row_dataset(&probe_table, &row.values, &probe_table.name);
                    if let Value::Bool(false) = self.eval_expr(
                        &expr,
                        &dataset,
                        &row.values,
                        &[],
                        &std::collections::BTreeMap::new(),
                        None,
                    )? {
                        return Err(DbError::constraint(format!(
                            "CHECK constraint failed on table {}",
                            table_name
                        )));
                    }
                }
                table.checks.push(candidate);
                self.catalog_mut().tables.insert(table_name.clone(), table);
                self.bump_schema_cookie();
                Ok(())
            }
            AlterTableAction::AddConstraint(TableConstraint::ForeignKey(definition)) => {
                if let Some(constraint_name) = &definition.name {
                    ensure_constraint_name_is_available(
                        self,
                        &table,
                        &table_name,
                        constraint_name,
                    )?;
                }
                let candidate = foreign_key_constraint_from_definition(definition);
                if table
                    .foreign_keys
                    .iter()
                    .any(|foreign_key| foreign_key == &candidate)
                {
                    return Err(DbError::sql(format!(
                        "constraint definition already exists on {}",
                        table_name
                    )));
                }
                table.foreign_keys.push(candidate.clone());
                validate_foreign_keys(self, &table)?;
                validate_existing_rows_with_staged_table(self, &table_name, &table)?;

                self.catalog_mut().tables.insert(table_name.clone(), table);
                let index_name = foreign_key_index_name(&table_name, &candidate.columns);
                if !self.catalog.indexes.contains_key(&index_name) {
                    self.insert_index_schema(IndexSchema {
                        name: index_name.clone(),
                        table_name: table_name.clone(),
                        kind: IndexKind::Btree,
                        unique: false,
                        columns: candidate
                            .columns
                            .iter()
                            .map(|column_name| IndexColumn {
                                column_name: Some(column_name.clone()),
                                expression_sql: None,
                            })
                            .collect(),
                        include_columns: Vec::new(),
                        predicate_sql: None,
                        fresh: false,
                    })?;
                    self.rebuild_index(&index_name, page_size)?;
                }
                self.bump_schema_cookie();
                Ok(())
            }
            AlterTableAction::AddConstraint(TableConstraint::Unique { name, columns }) => {
                if columns.is_empty() {
                    return Err(DbError::sql(
                        "UNIQUE constraints must include at least one column",
                    ));
                }
                for column_name in columns {
                    if !table
                        .columns
                        .iter()
                        .any(|column| identifiers_equal(&column.name, column_name))
                    {
                        return Err(DbError::sql(format!(
                            "unique constraint column {} does not exist on {}",
                            column_name, table_name
                        )));
                    }
                }
                if let Some(constraint_name) = name {
                    ensure_constraint_name_is_available(
                        self,
                        &table,
                        &table_name,
                        constraint_name,
                    )?;
                }
                let index_name = name
                    .clone()
                    .unwrap_or_else(|| auto_index_name("uq", &table_name, columns));
                let index = IndexSchema {
                    name: index_name.clone(),
                    table_name: table_name.clone(),
                    kind: IndexKind::Btree,
                    unique: true,
                    columns: columns
                        .iter()
                        .map(|column_name| IndexColumn {
                            column_name: Some(column_name.clone()),
                            expression_sql: None,
                        })
                        .collect(),
                    include_columns: Vec::new(),
                    predicate_sql: None,
                    fresh: false,
                };
                self.insert_index_schema(index)?;
                let validation = self
                    .tables
                    .get(&table_name)
                    .ok_or_else(|| {
                        DbError::internal(format!("table data for {table_name} is missing"))
                    })?
                    .resident_data()
                    .rows
                    .iter()
                    .try_for_each(|row| {
                        self.validate_row(&table_name, &row.values, Some(row.row_id), &[])
                    });
                if let Err(error) = validation {
                    self.catalog_mut().indexes.remove(&index_name);
                    return Err(error);
                }
                self.rebuild_index(&index_name, page_size)?;
                self.bump_schema_cookie();
                Ok(())
            }
            AlterTableAction::AddConstraint(other) => Err(DbError::sql(format!(
                "ALTER TABLE ADD CONSTRAINT does not support {other:?} in DecentDB 1.0"
            ))),
            AlterTableAction::DropConstraint { constraint_name } => {
                if let Some(index) = table
                    .checks
                    .iter()
                    .position(|check| check.name.as_deref() == Some(constraint_name.as_str()))
                {
                    table.checks.remove(index);
                    self.catalog_mut().tables.insert(table_name.clone(), table);
                    self.bump_schema_cookie();
                    return Ok(());
                }

                if let Some(index) = table.foreign_keys.iter().position(|foreign_key| {
                    foreign_key.name.as_deref() == Some(constraint_name.as_str())
                }) {
                    let foreign_key = table.foreign_keys.remove(index);
                    for column in &mut table.columns {
                        if column.foreign_key.as_ref().is_some_and(|candidate| {
                            candidate.name.as_deref() == Some(constraint_name.as_str())
                        }) {
                            column.foreign_key = None;
                        }
                    }
                    let index_name = foreign_key_index_name(&table_name, &foreign_key.columns);
                    let drop_index = !table
                        .foreign_keys
                        .iter()
                        .any(|candidate| candidate.columns == foreign_key.columns);
                    self.catalog_mut().tables.insert(table_name.clone(), table);
                    if drop_index {
                        self.catalog_mut().indexes.remove(&index_name);
                        self.indexes_mut().remove(&index_name);
                    }
                    self.bump_schema_cookie();
                    return Ok(());
                }

                if self.catalog.indexes.values().any(|index| {
                    identifiers_equal(&index.table_name, &table_name)
                        && index.unique
                        && identifiers_equal(&index.name, constraint_name)
                }) {
                    self.catalog_mut().indexes.remove(constraint_name);
                    self.indexes_mut().remove(constraint_name);
                    self.bump_schema_cookie();
                    return Ok(());
                }

                Err(DbError::sql(format!(
                    "unknown constraint {} on {}",
                    constraint_name, table_name
                )))
            }
            _ => Err(DbError::internal(
                "ALTER TABLE constraint dispatch reached a non-constraint action",
            )),
        }
    }

    fn execute_alter_table_rename(&mut self, table_name: &str, new_name: &str) -> Result<()> {
        if self.catalog.contains_object(new_name) {
            return Err(DbError::sql(format!("object {} already exists", new_name)));
        }
        let old_table_name = self
            .catalog
            .table(table_name)
            .map(|table| table.name.clone())
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
        let dependent_views = super::views::dependent_views(self, &old_table_name, false);
        if !dependent_views.is_empty() {
            return Err(DbError::sql(format!(
                "cannot rename table {} because dependent views exist: {}",
                old_table_name,
                dependent_views.join(", ")
            )));
        }
        let mut table = self
            .catalog_mut()
            .tables
            .remove(&old_table_name)
            .ok_or_else(|| {
                DbError::internal(format!("table schema for {} is missing", table_name))
            })?;
        table.name = new_name.to_string();
        self.catalog_mut()
            .tables
            .insert(new_name.to_string(), table);

        let data = self.tables_mut().remove(&old_table_name).ok_or_else(|| {
            DbError::internal(format!("table data for {} is missing", table_name))
        })?;
        self.tables_mut().insert(new_name.to_string(), data);

        if let Some(state) = self.persisted_tables_mut().remove(&old_table_name) {
            self.persisted_tables_mut()
                .insert(new_name.to_string(), state);
        }
        if let Some(stats) = self.catalog_mut().table_stats.remove(&old_table_name) {
            self.catalog_mut()
                .table_stats
                .insert(new_name.to_string(), stats);
        }
        if self.dirty_tables_mut().remove(&old_table_name) {
            self.dirty_tables_mut().insert(new_name.to_string());
        }
        if let Some(delta) = self.paged_mutations.remove(&old_table_name) {
            self.paged_mutations.insert(new_name.to_string(), delta);
        }

        rename_table_references(self, &old_table_name, new_name);
        self.bump_schema_cookie();
        Ok(())
    }

    fn insert_index_schema(&mut self, index: IndexSchema) -> Result<()> {
        if self.catalog.contains_object(&index.name) {
            return Err(DbError::sql(format!(
                "object {} already exists",
                index.name
            )));
        }
        self.catalog_mut().indexes.insert(index.name.clone(), index);
        Ok(())
    }

    pub(super) fn bump_schema_cookie(&mut self) {
        let next = self.catalog.schema_cookie.saturating_add(1);
        self.catalog_mut().schema_cookie = next;
    }
}

fn collect_truncate_targets(
    catalog: &crate::catalog::CatalogState,
    table_name: &str,
    cascade: bool,
    visited: &mut std::collections::BTreeSet<String>,
    ordered: &mut Vec<String>,
) -> Result<()> {
    let table_name = catalog
        .table(table_name)
        .map(|table| table.name.clone())
        .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
    if !visited.insert(table_name.clone()) {
        return Ok(());
    }
    let referencing_tables = catalog
        .tables
        .values()
        .filter(|child| {
            child
                .foreign_keys
                .iter()
                .any(|foreign_key| identifiers_equal(&foreign_key.referenced_table, &table_name))
        })
        .map(|child| child.name.clone())
        .collect::<Vec<_>>();
    if !cascade && !referencing_tables.is_empty() {
        return Err(DbError::sql(format!(
            "cannot truncate table {} because other tables reference it: {}",
            table_name,
            referencing_tables.join(", ")
        )));
    }
    for child in referencing_tables {
        collect_truncate_targets(catalog, &child, true, visited, ordered)?;
    }
    ordered.push(table_name);
    Ok(())
}

fn column_schema_from_definition(definition: &ColumnDefinition) -> Result<ColumnSchema> {
    if definition.generated.is_some() && definition.default.is_some() {
        return Err(DbError::sql(
            "generated columns may not also define DEFAULT",
        ));
    }
    if definition.generated.is_some() && definition.primary_key {
        return Err(DbError::sql("generated columns may not be PRIMARY KEY"));
    }
    Ok(ColumnSchema {
        name: definition.name.clone(),
        column_type: definition.column_type,
        nullable: definition.nullable && !definition.primary_key,
        default_sql: definition
            .generated
            .is_none()
            .then(|| definition.default.as_ref().map(|expr| expr.to_sql()))
            .flatten(),
        generated_sql: definition.generated.as_ref().map(|expr| expr.to_sql()),
        generated_stored: definition.generated_stored,
        primary_key: definition.primary_key,
        unique: definition.unique || definition.primary_key,
        auto_increment: definition.primary_key
            && definition.column_type == crate::catalog::ColumnType::Int64,
        checks: definition
            .checks
            .iter()
            .map(|expr| CheckConstraint {
                name: None,
                expression_sql: expr.to_sql(),
            })
            .collect(),
        foreign_key: definition
            .references
            .as_ref()
            .map(foreign_key_constraint_from_definition),
    })
}

fn foreign_key_constraint_from_definition(
    definition: &ForeignKeyDefinition,
) -> ForeignKeyConstraint {
    ForeignKeyConstraint {
        name: definition.name.clone(),
        columns: definition.columns.clone(),
        referenced_table: definition.referenced_table.clone(),
        referenced_columns: definition.referenced_columns.clone(),
        on_delete: map_fk_action(definition.on_delete),
        on_update: map_fk_action(definition.on_update),
    }
}

fn map_fk_action(action: ForeignKeyActionSpec) -> ForeignKeyAction {
    match action {
        ForeignKeyActionSpec::NoAction => ForeignKeyAction::NoAction,
        ForeignKeyActionSpec::Restrict => ForeignKeyAction::Restrict,
        ForeignKeyActionSpec::Cascade => ForeignKeyAction::Cascade,
        ForeignKeyActionSpec::SetNull => ForeignKeyAction::SetNull,
    }
}

fn foreign_key_index_name(table_name: &str, columns: &[String]) -> String {
    format!("{}_idx", auto_index_name("fk", table_name, columns))
}

fn ensure_constraint_name_is_available(
    runtime: &EngineRuntime,
    table: &TableSchema,
    table_name: &str,
    constraint_name: &str,
) -> Result<()> {
    if table
        .checks
        .iter()
        .any(|check| check.name.as_deref() == Some(constraint_name))
        || table
            .foreign_keys
            .iter()
            .any(|foreign_key| foreign_key.name.as_deref() == Some(constraint_name))
        || runtime.catalog.indexes.values().any(|index| {
            identifiers_equal(&index.table_name, table_name)
                && identifiers_equal(&index.name, constraint_name)
        })
    {
        return Err(DbError::sql(format!(
            "constraint {} already exists on {}",
            constraint_name, table_name
        )));
    }
    Ok(())
}

fn validate_existing_rows_with_staged_table(
    runtime: &mut EngineRuntime,
    table_name: &str,
    staged_table: &TableSchema,
) -> Result<()> {
    let original_table = runtime
        .catalog_mut()
        .tables
        .insert(table_name.to_string(), staged_table.clone());
    let rows = runtime
        .tables
        .get(table_name)
        .ok_or_else(|| DbError::internal(format!("table data for {table_name} is missing")))?
        .resident_data()
        .rows
        .clone();
    let validation = rows
        .iter()
        .try_for_each(|row| runtime.validate_row(table_name, &row.values, Some(row.row_id), &[]));
    if let Some(original_table) = original_table {
        runtime
            .catalog_mut()
            .tables
            .insert(table_name.to_string(), original_table);
    } else {
        runtime.catalog_mut().tables.remove(table_name);
    }
    validation
}

fn ensure_unique_column_names(columns: &[ColumnSchema], table_name: &str) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for column in columns {
        if !seen.insert(column.name.clone()) {
            return Err(DbError::sql(format!(
                "duplicate column {} in table {}",
                column.name, table_name
            )));
        }
    }
    Ok(())
}

fn validate_generated_columns(runtime: &EngineRuntime, table: &TableSchema) -> Result<()> {
    let row = vec![Value::Null; table.columns.len()];
    let dataset = table_row_dataset(table, &row, &table.name);
    for column in &table.columns {
        let Some(generated_sql) = &column.generated_sql else {
            continue;
        };
        let expr = parse_expression_sql(generated_sql)?;
        validate_generated_expr(&expr, table, &column.name)?;
        runtime.eval_expr(
            &expr,
            &dataset,
            &row,
            &[],
            &std::collections::BTreeMap::new(),
            None,
        )?;
    }
    Ok(())
}

fn validate_generated_expr(
    expr: &Expr,
    table: &TableSchema,
    generated_column_name: &str,
) -> Result<()> {
    match expr {
        Expr::Literal(_) => Ok(()),
        Expr::Column {
            table: qualifier,
            column,
        } => {
            if let Some(qualifier) = qualifier {
                if !identifiers_equal(qualifier, &table.name) {
                    return Err(DbError::sql(format!(
                        "generated column {} may only reference columns from {}",
                        generated_column_name, table.name
                    )));
                }
            }
            let referenced = table
                .columns
                .iter()
                .find(|candidate| identifiers_equal(&candidate.name, column))
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "generated column {} references unknown column {}",
                        generated_column_name, column
                    ))
                })?;
            if identifiers_equal(&referenced.name, generated_column_name) {
                return Err(DbError::sql(format!(
                    "generated column {} may not reference itself",
                    generated_column_name
                )));
            }
            if referenced.generated_sql.is_some() {
                return Err(DbError::sql(format!(
                    "generated column {} may not reference generated column {}",
                    generated_column_name, referenced.name
                )));
            }
            Ok(())
        }
        Expr::Parameter(_) => Err(DbError::sql(
            "generated columns may not use query parameters",
        )),
        Expr::Aggregate { .. } => Err(DbError::sql(
            "generated columns may not use aggregate expressions",
        )),
        Expr::RowNumber { .. } | Expr::WindowFunction { .. } => Err(DbError::sql(
            "generated columns may not use window functions",
        )),
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. } => {
            Err(DbError::sql("generated columns may not use subqueries"))
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } | Expr::Cast { expr, .. } => {
            validate_generated_expr(expr, table, generated_column_name)
        }
        Expr::Binary { left, right, .. } => {
            validate_generated_expr(left, table, generated_column_name)?;
            validate_generated_expr(right, table, generated_column_name)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_generated_expr(expr, table, generated_column_name)?;
            validate_generated_expr(low, table, generated_column_name)?;
            validate_generated_expr(high, table, generated_column_name)
        }
        Expr::InList { expr, items, .. } => {
            validate_generated_expr(expr, table, generated_column_name)?;
            for item in items {
                validate_generated_expr(item, table, generated_column_name)?;
            }
            Ok(())
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            validate_generated_expr(expr, table, generated_column_name)?;
            validate_generated_expr(pattern, table, generated_column_name)?;
            if let Some(escape) = escape {
                validate_generated_expr(escape, table, generated_column_name)?;
            }
            Ok(())
        }
        Expr::Function { args, .. } => {
            for arg in args {
                validate_generated_expr(arg, table, generated_column_name)?;
            }
            Ok(())
        }
        Expr::Row(items) => {
            for item in items {
                validate_generated_expr(item, table, generated_column_name)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_generated_expr(operand, table, generated_column_name)?;
            }
            for (condition, result) in branches {
                validate_generated_expr(condition, table, generated_column_name)?;
                validate_generated_expr(result, table, generated_column_name)?;
            }
            if let Some(else_expr) = else_expr {
                validate_generated_expr(else_expr, table, generated_column_name)?;
            }
            Ok(())
        }
    }
}

fn validate_foreign_keys(runtime: &EngineRuntime, table: &TableSchema) -> Result<()> {
    for foreign_key in &table.foreign_keys {
        if !foreign_key.columns.iter().all(|column_name| {
            table
                .columns
                .iter()
                .any(|column| identifiers_equal(&column.name, column_name))
        }) {
            return Err(DbError::sql(format!(
                "foreign key on {} references missing child columns",
                table.name
            )));
        }
        let parent = if identifiers_equal(&foreign_key.referenced_table, &table.name) {
            table
        } else {
            runtime
                .catalog
                .table(&foreign_key.referenced_table)
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "foreign key references unknown table {}",
                        foreign_key.referenced_table
                    ))
                })?
        };
        let referenced_columns = if foreign_key.referenced_columns.is_empty() {
            parent.primary_key_columns.clone()
        } else {
            foreign_key.referenced_columns.clone()
        };
        if referenced_columns.is_empty() {
            return Err(DbError::sql(format!(
                "foreign key parent {} does not define a referenced key",
                parent.name
            )));
        }
        let has_parent_key =
            identifier_lists_equal(&referenced_columns, &parent.primary_key_columns)
                || runtime.catalog.indexes.values().any(|index| {
                    identifiers_equal(&index.table_name, &parent.name)
                        && index.unique
                        && index.columns.len() == referenced_columns.len()
                        && index.columns.iter().zip(&referenced_columns).all(
                            |(candidate, target)| {
                                candidate
                                    .column_name
                                    .as_deref()
                                    .is_some_and(|name| identifiers_equal(name, target))
                                    && candidate.expression_sql.is_none()
                            },
                        )
                });
        if !has_parent_key {
            return Err(DbError::sql(format!(
                "foreign key parent {} must have an index on {}",
                parent.name,
                referenced_columns.join(", ")
            )));
        }
        if matches!(foreign_key.on_delete, ForeignKeyAction::SetNull)
            || matches!(foreign_key.on_update, ForeignKeyAction::SetNull)
        {
            for column_name in &foreign_key.columns {
                let column = table
                    .columns
                    .iter()
                    .find(|column| identifiers_equal(&column.name, column_name))
                    .ok_or_else(|| DbError::sql(format!("unknown column {}", column_name)))?;
                if !column.nullable {
                    return Err(DbError::sql(format!(
                        "SET NULL foreign key columns on {} must be nullable",
                        table.name
                    )));
                }
            }
        }
    }
    Ok(())
}

fn identifier_lists_equal(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| identifiers_equal(left, right))
}

fn rename_column_references(
    runtime: &mut EngineRuntime,
    table_name: &str,
    old_name: &str,
    new_name: &str,
) {
    if let Some(table) = runtime.catalog_mut().tables.get_mut(table_name) {
        for primary_key_column in &mut table.primary_key_columns {
            if primary_key_column == old_name {
                *primary_key_column = new_name.to_string();
            }
        }
        for foreign_key in &mut table.foreign_keys {
            for column_name in &mut foreign_key.columns {
                if column_name == old_name {
                    *column_name = new_name.to_string();
                }
            }
        }
    }

    for index in runtime.catalog_mut().indexes.values_mut() {
        if index.table_name == table_name {
            for column in &mut index.columns {
                if column.column_name.as_deref() == Some(old_name) {
                    column.column_name = Some(new_name.to_string());
                }
            }
            for include_column in &mut index.include_columns {
                if include_column == old_name {
                    *include_column = new_name.to_string();
                }
            }
            if let Some(predicate) = &mut index.predicate_sql {
                *predicate = predicate.replace(old_name, new_name);
            }
        }
    }

    for table in runtime.catalog_mut().tables.values_mut() {
        for foreign_key in &mut table.foreign_keys {
            if foreign_key.referenced_table == table_name {
                for column_name in &mut foreign_key.referenced_columns {
                    if column_name == old_name {
                        *column_name = new_name.to_string();
                    }
                }
            }
        }
    }
}

fn rename_table_references(runtime: &mut EngineRuntime, old_name: &str, new_name: &str) {
    for index in runtime.catalog_mut().indexes.values_mut() {
        if identifiers_equal(&index.table_name, old_name) {
            index.table_name = new_name.to_string();
        }
    }

    for trigger in runtime.catalog_mut().triggers.values_mut() {
        if !trigger.on_view && identifiers_equal(&trigger.target_name, old_name) {
            trigger.target_name = new_name.to_string();
        }
    }

    for table in runtime.catalog_mut().tables.values_mut() {
        for foreign_key in &mut table.foreign_keys {
            if identifiers_equal(&foreign_key.referenced_table, old_name) {
                foreign_key.referenced_table = new_name.to_string();
            }
        }
        for column in &mut table.columns {
            if let Some(foreign_key) = &mut column.foreign_key {
                if identifiers_equal(&foreign_key.referenced_table, old_name) {
                    foreign_key.referenced_table = new_name.to_string();
                }
            }
        }
    }
}
