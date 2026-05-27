use std::collections::BTreeSet;

use crate::catalog::identifiers_equal;
use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::sql::ast::{
    CommonTableExpr, Expr, FromItem, JoinConstraint, Query, QueryBody, Select, SelectItem,
};

use super::projection_has_aggregate_items;
use super::row::Dataset;

pub(crate) fn augment_dataset_with_outer_scope(
    mut dataset: Dataset,
    outer_dataset: &Dataset,
    outer_row: &[Value],
) -> Dataset {
    if outer_dataset.columns.is_empty() || outer_row.is_empty() {
        return dataset;
    }

    dataset
        .columns
        .extend(outer_dataset.columns.iter().cloned().map(|mut binding| {
            binding.hidden = true;
            binding
        }));
    for row in dataset.rows_mut() {
        row.extend_from_slice(outer_row);
    }
    dataset
}

pub(crate) fn query_references_outer_scope(query: &Query, outer_dataset: &Dataset) -> bool {
    let outer_tables = outer_dataset
        .columns
        .iter()
        .filter_map(|binding| binding.table.clone())
        .collect::<BTreeSet<_>>();
    if outer_tables.is_empty() {
        return false;
    }
    query_references_outer_tables(query, &outer_tables)
}

pub(crate) fn query_references_outer_tables(
    query: &Query,
    outer_tables: &BTreeSet<String>,
) -> bool {
    let local_tables = collect_query_table_names(query);
    query
        .ctes
        .iter()
        .any(|cte| query_references_outer_tables(&cte.query, outer_tables))
        || query_body_references_outer(&query.body, outer_tables, &local_tables)
        || query
            .order_by
            .iter()
            .any(|order| expr_references_outer(&order.expr, outer_tables, &local_tables))
        || query
            .limit
            .as_ref()
            .is_some_and(|expr| expr_references_outer(expr, outer_tables, &local_tables))
        || query
            .offset
            .as_ref()
            .is_some_and(|expr| expr_references_outer(expr, outer_tables, &local_tables))
}

pub(crate) fn query_body_references_outer(
    body: &QueryBody,
    outer_tables: &BTreeSet<String>,
    local_tables: &BTreeSet<String>,
) -> bool {
    match body {
        QueryBody::Select(select) => select_references_outer(select, outer_tables, local_tables),
        QueryBody::Values(rows) => rows
            .iter()
            .flatten()
            .any(|expr| expr_references_outer(expr, outer_tables, local_tables)),
        QueryBody::SetOperation { left, right, .. } => {
            query_body_references_outer(left, outer_tables, local_tables)
                || query_body_references_outer(right, outer_tables, local_tables)
        }
    }
}

pub(crate) fn select_references_outer(
    select: &Select,
    outer_tables: &BTreeSet<String>,
    local_tables: &BTreeSet<String>,
) -> bool {
    select
        .from
        .iter()
        .any(|item| from_item_references_outer(item, outer_tables, local_tables))
        || select.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => {
                expr_references_outer(expr, outer_tables, local_tables)
            }
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
        || select
            .filter
            .as_ref()
            .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
        || select
            .group_by
            .iter()
            .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
        || select
            .having
            .as_ref()
            .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
        || select
            .distinct_on
            .iter()
            .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
}

fn from_item_references_outer(
    item: &FromItem,
    outer_tables: &BTreeSet<String>,
    local_tables: &BTreeSet<String>,
) -> bool {
    match item {
        FromItem::Table { .. } => false,
        FromItem::Subquery { query, .. } => query_references_outer_tables(query, outer_tables),
        FromItem::Function { args, .. } => args
            .iter()
            .any(|arg| expr_references_outer(arg, outer_tables, local_tables)),
        FromItem::Join {
            left,
            right,
            constraint,
            ..
        } => {
            from_item_references_outer(left, outer_tables, local_tables)
                || from_item_references_outer(right, outer_tables, local_tables)
                || match constraint {
                    JoinConstraint::On(expr) => {
                        expr_references_outer(expr, outer_tables, local_tables)
                    }
                    JoinConstraint::Using(_) | JoinConstraint::Natural => false,
                }
        }
    }
}

pub(crate) fn expr_references_outer(
    expr: &Expr,
    outer_tables: &BTreeSet<String>,
    local_tables: &BTreeSet<String>,
) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => false,
        Expr::Column { table, .. } => table.as_ref().is_some_and(|table_name| {
            outer_tables
                .iter()
                .any(|outer_table| identifiers_equal(outer_table, table_name))
                && !local_tables
                    .iter()
                    .any(|local_table| identifiers_equal(local_table, table_name))
        }),
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => expr_references_outer(expr, outer_tables, local_tables),
        Expr::Binary { left, right, .. } => {
            expr_references_outer(left, outer_tables, local_tables)
                || expr_references_outer(right, outer_tables, local_tables)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || expr_references_outer(low, outer_tables, local_tables)
                || expr_references_outer(high, outer_tables, local_tables)
        }
        Expr::Row(items) => items
            .iter()
            .any(|item| expr_references_outer(item, outer_tables, local_tables)),
        Expr::InList { expr, items, .. } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || items
                    .iter()
                    .any(|item| expr_references_outer(item, outer_tables, local_tables))
        }
        Expr::InSubquery { expr, query, .. } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || query_references_outer_tables(query, outer_tables)
        }
        Expr::CompareSubquery { expr, query, .. } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || query_references_outer_tables(query, outer_tables)
        }
        Expr::ScalarSubquery(query) | Expr::Exists(query) => {
            query_references_outer_tables(query, outer_tables)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_references_outer(expr, outer_tables, local_tables)
                || expr_references_outer(pattern, outer_tables, local_tables)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
        }
        Expr::Function { args, .. } | Expr::Aggregate { args, .. } => args
            .iter()
            .any(|arg| expr_references_outer(arg, outer_tables, local_tables)),
        Expr::RowNumber {
            partition_by,
            order_by,
            ..
        } => {
            partition_by
                .iter()
                .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
                || order_by
                    .iter()
                    .any(|order| expr_references_outer(&order.expr, outer_tables, local_tables))
        }
        Expr::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
                || partition_by
                    .iter()
                    .any(|expr| expr_references_outer(expr, outer_tables, local_tables))
                || order_by
                    .iter()
                    .any(|order| expr_references_outer(&order.expr, outer_tables, local_tables))
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
                || branches.iter().any(|(condition, value)| {
                    expr_references_outer(condition, outer_tables, local_tables)
                        || expr_references_outer(value, outer_tables, local_tables)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr_references_outer(expr, outer_tables, local_tables))
        }
    }
}

pub(crate) fn collect_query_table_names(query: &Query) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    collect_query_body_table_names(&query.body, &mut names);
    names
}

pub(crate) fn collect_query_body_table_names(body: &QueryBody, names: &mut BTreeSet<String>) {
    match body {
        QueryBody::Select(select) => {
            for item in &select.from {
                collect_from_item_table_names(item, names);
            }
        }
        QueryBody::Values(_) => {}
        QueryBody::SetOperation { left, right, .. } => {
            collect_query_body_table_names(left, names);
            collect_query_body_table_names(right, names);
        }
    }
}

pub(crate) fn collect_from_item_table_names(item: &FromItem, names: &mut BTreeSet<String>) {
    match item {
        FromItem::Table { name, alias } => {
            names.insert(alias.clone().unwrap_or_else(|| name.clone()));
        }
        FromItem::Function { name, alias, .. } => {
            names.insert(alias.clone().unwrap_or_else(|| name.clone()));
        }
        FromItem::Subquery { alias, .. } => {
            names.insert(alias.clone());
        }
        FromItem::Join { left, right, .. } => {
            collect_from_item_table_names(left, names);
            collect_from_item_table_names(right, names);
        }
    }
}

pub(crate) fn prepare_cte_dataset(cte: &CommonTableExpr, mut dataset: Dataset) -> Result<Dataset> {
    if !cte.column_names.is_empty() {
        if cte.column_names.len() != dataset.columns.len() {
            return Err(DbError::sql(format!(
                "CTE {} expected {} columns but produced {}",
                cte.name,
                cte.column_names.len(),
                dataset.columns.len()
            )));
        }
        for (binding, name) in dataset.columns.iter_mut().zip(&cte.column_names) {
            binding.name = name.clone();
        }
    }
    for binding in &mut dataset.columns {
        binding.table = Some(cte.name.clone());
        binding.hidden = false;
    }
    Ok(dataset)
}

pub(crate) fn recursive_term_has_unsupported_features(body: &QueryBody) -> bool {
    match body {
        QueryBody::Select(select) => {
            select.distinct
                || !select.distinct_on.is_empty()
                || !select.group_by.is_empty()
                || select.having.is_some()
                || select
                    .projection
                    .iter()
                    .any(select_item_contains_window_or_subquery)
                || projection_has_aggregate_items(&select.projection)
                || select
                    .filter
                    .as_ref()
                    .is_some_and(expr_contains_recursive_unsupported_feature)
                || select
                    .group_by
                    .iter()
                    .any(expr_contains_recursive_unsupported_feature)
                || select
                    .having
                    .as_ref()
                    .is_some_and(expr_contains_recursive_unsupported_feature)
                || select
                    .distinct_on
                    .iter()
                    .any(expr_contains_recursive_unsupported_feature)
                || select.from.iter().any(from_item_contains_subquery)
        }
        QueryBody::Values(_) => true,
        QueryBody::SetOperation { .. } => true,
    }
}

pub(crate) fn select_item_contains_window_or_subquery(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => expr_contains_recursive_unsupported_feature(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }
}

pub(crate) fn expr_contains_recursive_unsupported_feature(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. }
        | Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => true,
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => expr_contains_recursive_unsupported_feature(expr),
        Expr::Binary { left, right, .. } => {
            expr_contains_recursive_unsupported_feature(left)
                || expr_contains_recursive_unsupported_feature(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_recursive_unsupported_feature(expr)
                || expr_contains_recursive_unsupported_feature(low)
                || expr_contains_recursive_unsupported_feature(high)
        }
        Expr::InList { expr, items, .. } => {
            expr_contains_recursive_unsupported_feature(expr)
                || items
                    .iter()
                    .any(expr_contains_recursive_unsupported_feature)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_recursive_unsupported_feature(expr)
                || expr_contains_recursive_unsupported_feature(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| expr_contains_recursive_unsupported_feature(expr))
        }
        Expr::Function { args, .. } => args.iter().any(expr_contains_recursive_unsupported_feature),
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_contains_recursive_unsupported_feature(expr))
                || branches.iter().any(|(condition, value)| {
                    expr_contains_recursive_unsupported_feature(condition)
                        || expr_contains_recursive_unsupported_feature(value)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr_contains_recursive_unsupported_feature(expr))
        }
        Expr::Row(items) => items
            .iter()
            .any(expr_contains_recursive_unsupported_feature),
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter(_) => false,
    }
}

pub(crate) fn from_item_contains_subquery(item: &FromItem) -> bool {
    match item {
        FromItem::Table { .. } => false,
        FromItem::Function { .. } => false,
        FromItem::Subquery { .. } => true,
        FromItem::Join { left, right, .. } => {
            from_item_contains_subquery(left) || from_item_contains_subquery(right)
        }
    }
}

pub(crate) fn from_item_is_lateral(item: &FromItem) -> bool {
    match item {
        FromItem::Subquery { lateral, .. } | FromItem::Function { lateral, .. } => *lateral,
        FromItem::Table { .. } | FromItem::Join { .. } => false,
    }
}

pub(crate) fn from_item_contains_lateral(item: &FromItem) -> bool {
    match item {
        FromItem::Subquery { lateral, .. } | FromItem::Function { lateral, .. } => *lateral,
        FromItem::Table { .. } => false,
        FromItem::Join { left, right, .. } => {
            from_item_contains_lateral(left) || from_item_contains_lateral(right)
        }
    }
}

pub(crate) fn validate_recursive_term(body: &QueryBody, cte_name: &str) -> Result<()> {
    if recursive_term_has_unsupported_features(body) {
        return Err(DbError::sql(format!(
            "recursive CTE {} recursive term only supports non-distinct SELECT statements without aggregates, window functions, or subqueries",
            cte_name
        )));
    }
    Ok(())
}

pub(crate) fn validate_recursive_ctes(query: &Query) -> Result<BTreeSet<String>> {
    let recursive_ctes = query
        .ctes
        .iter()
        .filter(|cte| query_table_reference_count(&cte.query, &cte.name) > 0)
        .map(|cte| cte.name.clone())
        .collect::<BTreeSet<_>>();
    if recursive_ctes.len() > 1 {
        return Err(DbError::sql(
            "WITH RECURSIVE supports only one self-referencing CTE per statement in DecentDB v0",
        ));
    }
    Ok(recursive_ctes)
}

pub(crate) fn query_table_reference_count(query: &Query, table_name: &str) -> usize {
    query
        .ctes
        .iter()
        .map(|cte| query_table_reference_count(&cte.query, table_name))
        .sum::<usize>()
        + query_body_table_reference_count(&query.body, table_name)
}

pub(crate) fn query_body_table_reference_count(body: &QueryBody, table_name: &str) -> usize {
    match body {
        QueryBody::Select(select) => select
            .from
            .iter()
            .map(|item| from_item_table_reference_count(item, table_name))
            .sum(),
        QueryBody::Values(_) => 0,
        QueryBody::SetOperation { left, right, .. } => {
            query_body_table_reference_count(left, table_name)
                + query_body_table_reference_count(right, table_name)
        }
    }
}

pub(crate) fn from_item_table_reference_count(item: &FromItem, table_name: &str) -> usize {
    match item {
        FromItem::Table { name, alias } => usize::from(
            identifiers_equal(name, table_name)
                || alias
                    .as_deref()
                    .is_some_and(|alias| identifiers_equal(alias, table_name)),
        ),
        FromItem::Function { name, alias, .. } => usize::from(
            identifiers_equal(name, table_name)
                || alias
                    .as_deref()
                    .is_some_and(|alias| identifiers_equal(alias, table_name)),
        ),
        FromItem::Subquery { query, .. } => query_table_reference_count(query, table_name),
        FromItem::Join { left, right, .. } => {
            from_item_table_reference_count(left, table_name)
                + from_item_table_reference_count(right, table_name)
        }
    }
}
