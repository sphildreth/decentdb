//! View expansion helpers.

use std::collections::BTreeSet;

use crate::catalog::ViewSchema;
use crate::error::{DbError, Result};
use crate::sql::ast::{CreateViewStatement, FromItem, Query, QueryBody};

use super::EngineRuntime;

impl EngineRuntime {
    pub(super) fn execute_create_view(&mut self, statement: &CreateViewStatement) -> Result<()> {
        if statement.temporary {
            if self.temp_relation_exists(&statement.view_name)
                && (!statement.replace || self.temp_view(&statement.view_name).is_none())
            {
                return Err(DbError::sql(format!(
                    "object {} already exists",
                    statement.view_name
                )));
            }
        } else if self.catalog.contains_object(&statement.view_name)
            && (!statement.replace || !self.catalog.views.contains_key(&statement.view_name))
        {
            return Err(DbError::sql(format!(
                "object {} already exists",
                statement.view_name
            )));
        }

        let column_names = if statement.column_names.is_empty() {
            self.evaluate_query(&statement.query, &[], &std::collections::BTreeMap::new())?
                .columns
                .into_iter()
                .map(|binding| binding.name)
                .collect()
        } else {
            statement.column_names.clone()
        };
        let dependencies = collect_view_dependencies(&statement.query);
        if !statement.temporary
            && dependencies
                .iter()
                .any(|dependency| self.temp_relation_exists(dependency))
        {
            return Err(DbError::sql(
                "persistent views may not depend on temporary tables or views",
            ));
        }

        let view = ViewSchema {
            name: statement.view_name.clone(),
            temporary: statement.temporary,
            sql_text: statement.query.to_sql(),
            column_names,
            dependencies,
        };
        if statement.temporary {
            self.temp_views.insert(statement.view_name.clone(), view);
            self.bump_temp_schema_cookie();
        } else {
            self.catalog.views.insert(statement.view_name.clone(), view);
            self.bump_schema_cookie();
        }
        Ok(())
    }

    pub(super) fn execute_drop_view(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if self.temp_table_schema(name).is_some() {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::sql(format!("unknown view {name}")));
        }
        if let Some(view_name) = self.temp_view(name).map(|view| view.name.clone()) {
            let dependents = dependent_views(self, &view_name, true);
            if !dependents.is_empty() {
                return Err(DbError::sql(format!(
                    "cannot drop view {} because views depend on it: {}",
                    view_name,
                    dependents.join(", ")
                )));
            }
            self.temp_views.remove(&view_name);
            self.bump_temp_schema_cookie();
            return Ok(());
        }
        let Some(view_name) = self.catalog.view(name).map(|view| view.name.clone()) else {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::sql(format!("unknown view {name}")));
        };
        let dependents = dependent_views(self, &view_name, false);
        if !dependents.is_empty() {
            return Err(DbError::sql(format!(
                "cannot drop view {} because views depend on it: {}",
                view_name,
                dependents.join(", ")
            )));
        }
        self.catalog.views.remove(&view_name);
        self.catalog
            .triggers
            .retain(|_, trigger| !(trigger.target_name == view_name && trigger.on_view));
        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_alter_view_rename(
        &mut self,
        view_name: &str,
        new_name: &str,
    ) -> Result<()> {
        if self.temp_view(view_name).is_some() {
            return Err(DbError::sql(
                "ALTER VIEW RENAME is not supported for temporary views",
            ));
        }
        if self.temp_table_schema(view_name).is_some() && self.catalog.view(view_name).is_none() {
            return Err(DbError::sql(format!("unknown view {view_name}")));
        }
        if self.catalog.contains_object(new_name) {
            return Err(DbError::sql(format!("object {} already exists", new_name)));
        }
        let dependents = dependent_views(self, view_name, false);
        if !dependents.is_empty() {
            return Err(DbError::sql(format!(
                "cannot rename view {} because dependent views exist: {}",
                view_name,
                dependents.join(", ")
            )));
        }
        let mut view = self
            .catalog
            .views
            .remove(view_name)
            .ok_or_else(|| DbError::sql(format!("unknown view {view_name}")))?;
        view.name = new_name.to_string();
        self.catalog.views.insert(new_name.to_string(), view);
        for trigger in self.catalog.triggers.values_mut() {
            if trigger.on_view && trigger.target_name == view_name {
                trigger.target_name = new_name.to_string();
            }
        }
        self.bump_schema_cookie();
        Ok(())
    }
}

pub(super) fn dependent_views(
    runtime: &EngineRuntime,
    object_name: &str,
    temporary: bool,
) -> Vec<String> {
    let views = if temporary {
        runtime.temp_views.values().collect::<Vec<_>>()
    } else {
        runtime.catalog.views.values().collect::<Vec<_>>()
    };
    views
        .into_iter()
        .filter(|view| {
            view.dependencies
                .iter()
                .any(|dependency| dependency == object_name)
        })
        .map(|view| view.name.clone())
        .collect()
}

fn collect_view_dependencies(query: &Query) -> Vec<String> {
    let mut dependencies = BTreeSet::new();
    collect_body_dependencies(&query.body, &mut dependencies);
    dependencies.into_iter().collect()
}

fn collect_body_dependencies(body: &QueryBody, dependencies: &mut BTreeSet<String>) {
    match body {
        QueryBody::Select(select) => {
            for from in &select.from {
                collect_from_dependencies(from, dependencies);
            }
        }
        QueryBody::SetOperation { left, right, .. } => {
            collect_body_dependencies(left, dependencies);
            collect_body_dependencies(right, dependencies);
        }
    }
}

fn collect_from_dependencies(item: &FromItem, dependencies: &mut BTreeSet<String>) {
    match item {
        FromItem::Table { name, .. } => {
            dependencies.insert(name.clone());
        }
        FromItem::Function { .. } => {}
        FromItem::Subquery { query, .. } => {
            collect_body_dependencies(&query.body, dependencies);
        }
        FromItem::Join { left, right, .. } => {
            collect_from_dependencies(left, dependencies);
            collect_from_dependencies(right, dependencies);
        }
    }
}
