//! Logical planning and rule-based optimization.

pub(crate) mod logical;
pub(crate) mod physical;

use crate::catalog::{CatalogState, IndexKind};
use crate::error::Result;
use crate::sql::ast::{BinaryOp, Expr, FromItem, JoinKind, Query, QueryBody, Select, Statement};

use self::physical::PhysicalPlan;

pub(crate) fn plan_statement(
    statement: &Statement,
    catalog: &CatalogState,
) -> Result<PhysicalPlan> {
    match statement {
        Statement::Query(query) => plan_query(query, catalog),
        Statement::Explain(explain) => plan_statement(&explain.statement, catalog),
        _ => Ok(PhysicalPlan::Empty),
    }
}

pub(crate) fn plan_query(query: &Query, catalog: &CatalogState) -> Result<PhysicalPlan> {
    let mut plan = plan_query_body(&query.body, catalog)?;
    if !query.order_by.is_empty() {
        plan = PhysicalPlan::Sort {
            input: Box::new(plan),
            order_by: query.order_by.clone(),
        };
    }
    if query.limit.is_some() || query.offset.is_some() {
        plan = PhysicalPlan::Limit {
            input: Box::new(plan),
            limit: query.limit.clone(),
            offset: query.offset.clone(),
        };
    }
    Ok(plan)
}

fn plan_query_body(query: &QueryBody, catalog: &CatalogState) -> Result<PhysicalPlan> {
    match query {
        QueryBody::Select(select) => plan_select(select, catalog),
        QueryBody::SetOperation {
            op,
            all,
            left,
            right,
        } => Ok(PhysicalPlan::SetOp {
            op: *op,
            all: *all,
            left: Box::new(plan_query_body(left, catalog)?),
            right: Box::new(plan_query_body(right, catalog)?),
        }),
    }
}

fn plan_select(select: &Select, catalog: &CatalogState) -> Result<PhysicalPlan> {
    let mut plan = if select.from.is_empty() {
        PhysicalPlan::Empty
    } else {
        plan_from_item(&select.from[0], catalog)?
    };
    for item in select.from.iter().skip(1) {
        plan = PhysicalPlan::NestedLoopJoin {
            left: Box::new(plan),
            right: Box::new(plan_from_item(item, catalog)?),
            kind: JoinKind::Inner,
            on: Expr::Literal(crate::record::value::Value::Bool(true)),
        };
    }

    if let Some(filter) = &select.filter {
        if let Some(index_plan) = maybe_index_plan(select, filter, catalog) {
            plan = index_plan;
        } else {
            plan = PhysicalPlan::Filter {
                input: Box::new(plan),
                predicate: filter.clone(),
            };
        }
    }
    if !select.group_by.is_empty() || projection_has_aggregate(select) {
        plan = PhysicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: select.group_by.clone(),
            having: select.having.clone(),
        };
    }
    plan = PhysicalPlan::Project {
        input: Box::new(plan),
        items: select.projection.clone(),
    };
    Ok(plan)
}

fn plan_from_item(item: &FromItem, catalog: &CatalogState) -> Result<PhysicalPlan> {
    Ok(match item {
        FromItem::Table { name, .. } => PhysicalPlan::TableScan {
            table: if catalog.views.contains_key(name) {
                format!("view:{name}")
            } else {
                name.clone()
            },
        },
        FromItem::Subquery { query, .. } => plan_query(query, catalog)?,
        FromItem::Join {
            left,
            right,
            kind,
            on,
        } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(plan_from_item(left, catalog)?),
            right: Box::new(plan_from_item(right, catalog)?),
            kind: *kind,
            on: on.clone(),
        },
    })
}

fn maybe_index_plan(
    select: &Select,
    filter: &Expr,
    catalog: &CatalogState,
) -> Option<PhysicalPlan> {
    let FromItem::Table { name, .. } = select.from.first()? else {
        return None;
    };
    let (column_name, uses_like) = simple_indexable_filter(filter)?;
    let index = catalog.indexes.values().find(|index| {
        index.table_name == *name
            && index.columns.len() == 1
            && index.columns[0]
                .column_name
                .as_ref()
                .is_some_and(|indexed| indexed == column_name)
            && (uses_like && index.kind == IndexKind::Trigram
                || !uses_like && index.kind == IndexKind::Btree)
            && index.fresh
    })?;
    Some(if uses_like {
        PhysicalPlan::TrigramSearch {
            table: name.clone(),
            index: index.name.clone(),
            predicate: filter.clone(),
        }
    } else {
        PhysicalPlan::IndexSeek {
            table: name.clone(),
            index: index.name.clone(),
            predicate: filter.clone(),
        }
    })
}

fn simple_indexable_filter(filter: &Expr) -> Option<(&str, bool)> {
    match filter {
        Expr::Binary { left, op, right } => match (&**left, op, &**right) {
            (Expr::Column { column, .. }, BinaryOp::Eq, Expr::Literal(_)) => {
                Some((column.as_str(), false))
            }
            (Expr::Literal(_), BinaryOp::Eq, Expr::Column { column, .. }) => {
                Some((column.as_str(), false))
            }
            _ => None,
        },
        Expr::Like {
            expr: left,
            pattern: right,
            ..
        } => match (&**left, &**right) {
            (Expr::Column { column, .. }, Expr::Literal(crate::record::value::Value::Text(_))) => {
                Some((column.as_str(), true))
            }
            _ => None,
        },
        _ => None,
    }
}

fn projection_has_aggregate(select: &Select) -> bool {
    select.projection.iter().any(select_item_has_aggregate)
}

fn select_item_has_aggregate(item: &crate::sql::ast::SelectItem) -> bool {
    match item {
        crate::sql::ast::SelectItem::Expr { expr, .. } => expr_has_aggregate(expr),
        crate::sql::ast::SelectItem::Wildcard
        | crate::sql::ast::SelectItem::QualifiedWildcard(_) => false,
    }
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Unary { expr, .. } => expr_has_aggregate(expr),
        Expr::Binary { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::Between {
            expr, low, high, ..
        } => expr_has_aggregate(expr) || expr_has_aggregate(low) || expr_has_aggregate(high),
        Expr::InList { expr, items, .. } => {
            expr_has_aggregate(expr) || items.iter().any(expr_has_aggregate)
        }
        Expr::InSubquery { expr, .. } => expr_has_aggregate(expr),
        Expr::Exists(_) => false,
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_has_aggregate(expr)
                || expr_has_aggregate(pattern)
                || escape.as_deref().is_some_and(expr_has_aggregate)
        }
        Expr::IsNull { expr, .. } => expr_has_aggregate(expr),
        Expr::Function { args, .. } => args.iter().any(expr_has_aggregate),
        Expr::RowNumber { .. } => false,
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_has_aggregate)
                || branches
                    .iter()
                    .any(|(left, right)| expr_has_aggregate(left) || expr_has_aggregate(right))
                || else_expr.as_deref().is_some_and(expr_has_aggregate)
        }
        Expr::Cast { expr, .. } => expr_has_aggregate(expr),
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter(_) => false,
    }
}
