//! View expansion helpers.

use std::collections::BTreeSet;

use crate::catalog::ViewSchema;
use crate::error::{DbError, Result};
use crate::sql::ast::{CreateViewStatement, Expr, FromItem, Query, QueryBody, SelectItem};

use super::EngineRuntime;

/// Attempts to derive the output column names of [`query`] without executing
/// its body.
///
/// Returns `Some(names)` when the projection can be resolved purely from the
/// AST — i.e. no `*` / `tbl.*` wildcards anywhere in the relevant projection.
/// Returns `None` when execution-based resolution is required (e.g. the
/// outermost projection contains a wildcard, in which case the source
/// dataset's column metadata is needed to expand it).
///
/// This is the fast path for `CREATE VIEW v AS SELECT ...` when no explicit
/// column list is provided: most real views project a fixed list of named or
/// aliased expressions and never need to run the body just to learn the
/// column names. Resolving syntactically also avoids triggering side effects
/// that the SELECT would otherwise execute as a side effect of DDL.
fn try_resolve_query_column_names_syntactic(query: &Query) -> Option<Vec<String>> {
    resolve_body_columns_syntactic(&query.body)
}

fn resolve_body_columns_syntactic(body: &QueryBody) -> Option<Vec<String>> {
    match body {
        QueryBody::Select(select) => {
            let mut names = Vec::with_capacity(select.projection.len());
            for (index, item) in select.projection.iter().enumerate() {
                match item {
                    SelectItem::Expr { expr, alias } => {
                        names.push(
                            alias
                                .clone()
                                .unwrap_or_else(|| infer_expr_name_syntactic(expr, index + 1)),
                        );
                    }
                    // Wildcards require knowing the source dataset's columns,
                    // which we cannot derive without inspecting the catalog
                    // (and resolving JOIN/CTE scopes). Defer to the execution
                    // path in that case.
                    SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => return None,
                }
            }
            Some(names)
        }
        QueryBody::Values(rows) => {
            let first_row = rows.first()?;
            let mut names = Vec::with_capacity(first_row.len());
            for (index, expr) in first_row.iter().enumerate() {
                names.push(infer_expr_name_syntactic(expr, index + 1));
            }
            Some(names)
        }
        // For a set operation (UNION / INTERSECT / EXCEPT) the result column
        // names come from the LEFT operand per SQL semantics.
        QueryBody::SetOperation { left, .. } => resolve_body_columns_syntactic(left),
    }
}

/// Mirrors `crate::exec::infer_expr_name` so the fast path produces names
/// identical to the execution path for non-wildcard projections.
fn infer_expr_name_syntactic(expr: &Expr, ordinal: usize) -> String {
    match expr {
        Expr::Column { column, .. } => column.clone(),
        Expr::RowNumber { .. } => "row_number".to_string(),
        Expr::WindowFunction { name, .. } => name.clone(),
        _ => format!("col{ordinal}"),
    }
}

impl EngineRuntime {
    pub(super) fn execute_create_view(&mut self, statement: &CreateViewStatement) -> Result<()> {
        // Handle existence conflicts for temporary and persistent views.
        if statement.temporary {
            if let Some(_existing) = self.temp_view(&statement.view_name) {
                if statement.replace {
                    self.temp_views_mut().remove(&statement.view_name);
                } else if statement.if_not_exists {
                    return Ok(());
                } else {
                    return Err(DbError::sql(format!(
                        "object {} already exists",
                        statement.view_name
                    )));
                }
            }
        } else if self.catalog.contains_object(&statement.view_name) {
            if self.catalog.view(&statement.view_name).is_some() {
                if statement.replace {
                    self.catalog_mut().views.remove(&statement.view_name);
                } else if statement.if_not_exists {
                    return Ok(());
                } else {
                    return Err(DbError::sql(format!(
                        "object {} already exists",
                        statement.view_name
                    )));
                }
            } else {
                // Object exists but is not a view (e.g., a table)
                return Err(DbError::sql(format!(
                    "object {} already exists",
                    statement.view_name
                )));
            }
        }

        let column_names = if statement.column_names.is_empty() {
            // Fast path: resolve output column names purely from the AST when
            // the projection has no `*` / `tbl.*` wildcards. This avoids
            // executing the full SELECT body just to learn its schema (which
            // for view bodies that join large tables is the dominant cost of
            // CREATE VIEW). The execution path (next branch) is only taken
            // when wildcards force us to inspect the source dataset.
            if let Some(names) = try_resolve_query_column_names_syntactic(&statement.query) {
                names
            } else {
                self.evaluate_query(&statement.query, &[], &std::collections::BTreeMap::new())?
                    .columns
                    .into_iter()
                    .map(|binding| binding.name)
                    .collect()
            }
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

        // Validate that each dependency resolves to a known table or view.
        // The execution-fallback path below would surface the same error, but
        // when we take the syntactic fast path we must validate explicitly to
        // preserve the historical "CREATE VIEW fails on unknown source"
        // behavior. CTE names declared in the same query are excluded.
        let cte_names: std::collections::BTreeSet<&str> = statement
            .query
            .ctes
            .iter()
            .map(|cte| cte.name.as_str())
            .collect();
        for dependency in &dependencies {
            if cte_names.contains(dependency.as_str()) {
                continue;
            }
            let exists = self.catalog.table(dependency).is_some()
                || self.catalog.view(dependency).is_some()
                || self.temp_relation_exists(dependency);
            if !exists {
                return Err(DbError::sql(format!("unknown table or view {dependency}")));
            }
        }

        let view = ViewSchema {
            name: statement.view_name.clone(),
            temporary: statement.temporary,
            sql_text: statement.query.to_sql(),
            column_names,
            dependencies,
        };
        if statement.temporary {
            self.temp_views_mut()
                .insert(statement.view_name.clone(), view);
            self.bump_temp_schema_cookie();
        } else {
            self.catalog_mut()
                .views
                .insert(statement.view_name.clone(), view);
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
            self.temp_views_mut().remove(&view_name);
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
        self.catalog_mut().views.remove(&view_name);
        self.catalog_mut()
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
            .catalog_mut()
            .views
            .remove(view_name)
            .ok_or_else(|| DbError::sql(format!("unknown view {view_name}")))?;
        view.name = new_name.to_string();
        self.catalog_mut().views.insert(new_name.to_string(), view);
        for trigger in self.catalog_mut().triggers.values_mut() {
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
        QueryBody::Values(_) => {}
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
