//! DDL execution helpers.

use crate::catalog::{
    CheckConstraint, ColumnSchema, ForeignKeyAction, ForeignKeyConstraint, IndexColumn, IndexKind,
    IndexSchema, TableSchema,
};
use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::sql::ast::{
    AlterTableAction, ColumnDefinition, CreateIndexStatement, CreateTableStatement,
    ForeignKeyActionSpec, ForeignKeyDefinition, IndexExpression, TableConstraint,
};

use super::constraints::auto_index_name;
use super::{EngineRuntime, TableData};

impl EngineRuntime {
    pub(super) fn execute_create_table(&mut self, statement: &CreateTableStatement) -> Result<()> {
        if self.catalog.contains_object(&statement.table_name) {
            if statement.if_not_exists && self.catalog.tables.contains_key(&statement.table_name) {
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
                .find(|column| column.name == *primary_key_column)
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "primary key column {} does not exist on {}",
                        primary_key_column, statement.table_name
                    ))
                })?;
            column.primary_key = true;
            column.unique = true;
            column.nullable = false;
            if column.column_type == crate::catalog::ColumnType::Int64 {
                column.auto_increment = true;
            }
        }

        let table = TableSchema {
            name: statement.table_name.clone(),
            columns,
            checks: table_checks,
            foreign_keys,
            primary_key_columns,
            next_row_id: 1,
        };
        validate_foreign_keys(self, &table)?;

        self.catalog
            .tables
            .insert(statement.table_name.clone(), table.clone());
        self.tables
            .insert(statement.table_name.clone(), TableData::default());

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
        if self.catalog.views.contains_key(&statement.table_name) {
            return Err(DbError::sql(format!(
                "cannot create an index on view {}",
                statement.table_name
            )));
        }
        let table = self
            .catalog
            .tables
            .get(&statement.table_name)
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
        }
        if let Some(predicate) = &statement.predicate {
            if statement.unique || kind != IndexKind::Btree || statement.columns.len() != 1 {
                return Err(DbError::sql(
                    "only single-column non-unique BTREE partial indexes are supported",
                ));
            }
            let column_name = match &statement.columns[0] {
                IndexExpression::Column(column_name) => column_name,
                IndexExpression::Expr(_) => {
                    return Err(DbError::sql(
                        "partial indexes require a plain indexed column",
                    ))
                }
            };
            let expected = format!("{column_name} IS NOT NULL");
            if !predicate.to_sql().eq_ignore_ascii_case(&expected) {
                return Err(DbError::sql(
                    "partial index predicate must be <indexed_column> IS NOT NULL",
                ));
            }
        }

        for column in &statement.columns {
            if let IndexExpression::Column(column_name) = column {
                if !table
                    .columns
                    .iter()
                    .any(|column| column.name == *column_name)
                {
                    return Err(DbError::sql(format!(
                        "index column {} does not exist on {}",
                        column_name, table.name
                    )));
                }
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
            predicate_sql: statement
                .predicate
                .as_ref()
                .map(|predicate| predicate.to_sql()),
            fresh: true,
        })?;
        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_drop_table(
        &mut self,
        name: &str,
        if_exists: bool,
        _page_size: u32,
    ) -> Result<()> {
        if !self.catalog.tables.contains_key(name) {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::sql(format!("unknown table {name}")));
        }
        let dependent_views = super::views::dependent_views(self, name);
        if !dependent_views.is_empty() {
            return Err(DbError::sql(format!(
                "cannot drop table {} because views depend on it: {}",
                name,
                dependent_views.join(", ")
            )));
        }
        let referencing_tables = self
            .catalog
            .tables
            .values()
            .filter(|table| {
                table
                    .foreign_keys
                    .iter()
                    .any(|foreign_key| foreign_key.referenced_table == name)
            })
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        if !referencing_tables.is_empty() {
            return Err(DbError::sql(format!(
                "cannot drop table {} because foreign keys still reference it from {}",
                name,
                referencing_tables.join(", ")
            )));
        }

        self.catalog.tables.remove(name);
        self.tables.remove(name);
        self.catalog
            .indexes
            .retain(|_, index| index.table_name != name);
        self.catalog
            .triggers
            .retain(|_, trigger| trigger.target_name != name || trigger.on_view);
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
        self.catalog.indexes.remove(name);
        self.indexes.remove(name);
        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_alter_table(
        &mut self,
        table_name: &str,
        actions: &[AlterTableAction],
        params: &[Value],
        _page_size: u32,
    ) -> Result<()> {
        let mut table = self
            .catalog
            .tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
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
        for action in actions {
            match action {
                AlterTableAction::AddColumn(definition) => {
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
                        .rows
                        .is_empty();
                    if !column.nullable && matches!(fill_value, Value::Null) && has_rows {
                        return Err(DbError::constraint(format!(
                            "cannot add NOT NULL column {} without a non-NULL default",
                            column.name
                        )));
                    }
                    for row in &mut self
                        .tables
                        .get_mut(table_name)
                        .ok_or_else(|| {
                            DbError::internal(format!("table data for {table_name} is missing"))
                        })?
                        .rows
                    {
                        row.values.push(fill_value.clone());
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
                            && index
                                .columns
                                .iter()
                                .any(|column| column.column_name.as_deref() == Some(column_name))
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
                    for row in &mut self
                        .tables
                        .get_mut(table_name)
                        .ok_or_else(|| {
                            DbError::internal(format!("table data for {table_name} is missing"))
                        })?
                        .rows
                    {
                        row.values.remove(index);
                    }
                }
                AlterTableAction::RenameColumn { old_name, new_name } => {
                    if !super::views::dependent_views(self, table_name).is_empty() {
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
                    for row in &mut self
                        .tables
                        .get_mut(table_name)
                        .ok_or_else(|| {
                            DbError::internal(format!("table data for {table_name} is missing"))
                        })?
                        .rows
                    {
                        row.values[index] =
                            super::cast_value(row.values[index].clone(), *new_type)?;
                    }
                    table.columns[index].column_type = *new_type;
                }
            }
        }

        self.catalog.tables.insert(table_name.to_string(), table);
        self.mark_table_dirty(table_name);
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
        self.catalog.indexes.insert(index.name.clone(), index);
        Ok(())
    }

    pub(super) fn bump_schema_cookie(&mut self) {
        self.catalog.schema_cookie = self.catalog.schema_cookie.saturating_add(1);
    }
}

fn column_schema_from_definition(definition: &ColumnDefinition) -> Result<ColumnSchema> {
    Ok(ColumnSchema {
        name: definition.name.clone(),
        column_type: definition.column_type,
        nullable: definition.nullable && !definition.primary_key,
        default_sql: definition.default.as_ref().map(|expr| expr.to_sql()),
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

fn validate_foreign_keys(runtime: &EngineRuntime, table: &TableSchema) -> Result<()> {
    for foreign_key in &table.foreign_keys {
        if !foreign_key.columns.iter().all(|column_name| {
            table
                .columns
                .iter()
                .any(|column| column.name == *column_name)
        }) {
            return Err(DbError::sql(format!(
                "foreign key on {} references missing child columns",
                table.name
            )));
        }
        let parent = runtime
            .catalog
            .tables
            .get(&foreign_key.referenced_table)
            .ok_or_else(|| {
                DbError::sql(format!(
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
            return Err(DbError::sql(format!(
                "foreign key parent {} does not define a referenced key",
                parent.name
            )));
        }
        let has_parent_key =
            referenced_columns == parent.primary_key_columns
                || runtime.catalog.indexes.values().any(|index| {
                    index.table_name == parent.name
                        && index.unique
                        && index.columns.len() == referenced_columns.len()
                        && index.columns.iter().zip(&referenced_columns).all(
                            |(candidate, target)| {
                                candidate.column_name.as_deref() == Some(target.as_str())
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
                    .find(|column| column.name == *column_name)
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

fn rename_column_references(
    runtime: &mut EngineRuntime,
    table_name: &str,
    old_name: &str,
    new_name: &str,
) {
    if let Some(table) = runtime.catalog.tables.get_mut(table_name) {
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

    for index in runtime.catalog.indexes.values_mut() {
        if index.table_name == table_name {
            for column in &mut index.columns {
                if column.column_name.as_deref() == Some(old_name) {
                    column.column_name = Some(new_name.to_string());
                }
            }
            if let Some(predicate) = &mut index.predicate_sql {
                *predicate = predicate.replace(old_name, new_name);
            }
        }
    }

    for table in runtime.catalog.tables.values_mut() {
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
