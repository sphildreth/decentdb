//! Logical planning and rule-based optimization.

pub(crate) mod logical;
pub(crate) mod physical;

use crate::catalog::{identifiers_equal, CatalogState, IndexKind};
use crate::error::Result;
use crate::sql::ast::{
    BinaryOp, Expr, FromItem, JoinConstraint, JoinKind, Query, QueryBody, Select, Statement,
};

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
        plan =
            maybe_spatial_knn_plan(&plan, query, catalog).unwrap_or_else(|| PhysicalPlan::Sort {
                input: Box::new(plan),
                order_by: query.order_by.clone(),
            });
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
        QueryBody::Values(_) => Ok(PhysicalPlan::Empty),
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
            constraint: JoinConstraint::On(Expr::Literal(crate::record::value::Value::Bool(true))),
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
            table: if catalog.view(name).is_some() {
                format!("view:{name}")
            } else {
                name.clone()
            },
        },
        FromItem::Function { name, alias, .. } => PhysicalPlan::TableScan {
            table: alias.clone().unwrap_or_else(|| format!("tvf:{name}")),
        },
        FromItem::Subquery { query, .. } => plan_query(query, catalog)?,
        FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } => {
            if let Some(plan) = maybe_spatial_join_plan(left, right, *kind, constraint, catalog)? {
                plan
            } else {
                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(plan_from_item(left, catalog)?),
                    right: Box::new(plan_from_item(right, catalog)?),
                    kind: *kind,
                    constraint: constraint.clone(),
                }
            }
        }
    })
}

fn maybe_spatial_join_plan(
    left: &FromItem,
    right: &FromItem,
    kind: JoinKind,
    constraint: &JoinConstraint,
    catalog: &CatalogState,
) -> Result<Option<PhysicalPlan>> {
    if kind != JoinKind::Inner {
        return Ok(None);
    }
    let JoinConstraint::On(on) = constraint else {
        return Ok(None);
    };
    let FromItem::Table {
        name: left_name,
        alias: left_alias,
    } = left
    else {
        return Ok(None);
    };
    let FromItem::Table {
        name: right_name,
        alias: right_alias,
    } = right
    else {
        return Ok(None);
    };
    let left_binding = TableBindingRef {
        name: left_name,
        alias: left_alias,
    };
    let right_binding = TableBindingRef {
        name: right_name,
        alias: right_alias,
    };
    let Some(predicate) = simple_spatial_join_predicate(on, left_binding, right_binding) else {
        return Ok(None);
    };
    for indexed_ref in [predicate.left, predicate.right] {
        let table = if matches_table_binding(left_binding, indexed_ref.table) {
            left_binding
        } else if matches_table_binding(right_binding, indexed_ref.table) {
            right_binding
        } else {
            continue;
        };
        let Some(index) = catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, table.name)
                && index.kind == IndexKind::Spatial
                && index.fresh
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0]
                    .column_name
                    .as_ref()
                    .is_some_and(|indexed| identifiers_equal(indexed, indexed_ref.column))
        }) else {
            continue;
        };
        return Ok(Some(PhysicalPlan::SpatialJoin {
            table: table.name.to_string(),
            index: index.name.clone(),
            predicate: on.clone(),
            input: Box::new(PhysicalPlan::NestedLoopJoin {
                left: Box::new(plan_from_item(left, catalog)?),
                right: Box::new(plan_from_item(right, catalog)?),
                kind,
                constraint: constraint.clone(),
            }),
        }));
    }
    Ok(None)
}

fn maybe_index_plan(
    select: &Select,
    filter: &Expr,
    catalog: &CatalogState,
) -> Option<PhysicalPlan> {
    let FromItem::Table { name, .. } = select.from.first()? else {
        return None;
    };
    let table = catalog.table(name)?;
    if let Some(column_name) = simple_spatial_indexable_filter(filter) {
        let index = catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, &table.name)
                && index.columns.len() == 1
                && index.predicate_sql.is_none()
                && index.columns[0]
                    .column_name
                    .as_ref()
                    .is_some_and(|indexed| identifiers_equal(indexed, column_name))
                && index.kind == IndexKind::Spatial
                && index.fresh
        })?;
        return Some(PhysicalPlan::SpatialFilter {
            table: table.name.clone(),
            index: index.name.clone(),
            predicate: filter.clone(),
        });
    }
    let (column_name, uses_like) = simple_indexable_filter(filter)?;
    let index = catalog.indexes.values().find(|index| {
        identifiers_equal(&index.table_name, &table.name)
            && index.columns.len() == 1
            && index.predicate_sql.is_none()
            && index.columns[0]
                .column_name
                .as_ref()
                .is_some_and(|indexed| indexed == column_name)
            && (uses_like && index.kind == IndexKind::Trigram
                || !uses_like && index.kind == IndexKind::Btree)
            && index.fresh
    })?;
    if !uses_like && !should_use_btree_index(table.name.as_str(), index.name.as_str(), catalog) {
        return None;
    }
    Some(if uses_like {
        PhysicalPlan::TrigramSearch {
            table: table.name.clone(),
            index: index.name.clone(),
            predicate: filter.clone(),
        }
    } else {
        PhysicalPlan::IndexSeek {
            table: table.name.clone(),
            index: index.name.clone(),
            predicate: filter.clone(),
        }
    })
}

fn should_use_btree_index(table_name: &str, index_name: &str, catalog: &CatalogState) -> bool {
    let Some(table_stats) = catalog.table_stats.get(table_name) else {
        return true;
    };
    let Some(index_stats) = catalog.index_stats.get(index_name) else {
        return true;
    };
    if table_stats.row_count <= 0
        || index_stats.entry_count <= 0
        || index_stats.distinct_key_count <= 0
    {
        return false;
    }
    let estimated_matches =
        (i128::from(index_stats.entry_count) + i128::from(index_stats.distinct_key_count) - 1)
            / i128::from(index_stats.distinct_key_count);
    estimated_matches * 4 < i128::from(table_stats.row_count)
}

fn maybe_spatial_knn_plan(
    input: &PhysicalPlan,
    query: &Query,
    catalog: &CatalogState,
) -> Option<PhysicalPlan> {
    let QueryBody::Select(select) = &query.body else {
        return None;
    };
    if select.from.len() != 1 || query.order_by.len() != 1 {
        return None;
    }
    let FromItem::Table { name, .. } = &select.from[0] else {
        return None;
    };
    let table = catalog.table(name)?;
    let order = &query.order_by[0];
    if order.descending {
        return None;
    }
    let column_name = simple_spatial_order_column(&order.expr)?;
    let index = catalog.indexes.values().find(|index| {
        identifiers_equal(&index.table_name, &table.name)
            && index.columns.len() == 1
            && index.predicate_sql.is_none()
            && index.columns[0]
                .column_name
                .as_ref()
                .is_some_and(|indexed| identifiers_equal(indexed, column_name))
            && index.kind == IndexKind::Spatial
            && index.fresh
    })?;
    Some(PhysicalPlan::SpatialKnn {
        table: table.name.clone(),
        index: index.name.clone(),
        order: order.expr.clone(),
        input: Box::new(input.clone()),
    })
}

#[derive(Clone, Copy)]
struct TableBindingRef<'a> {
    name: &'a str,
    alias: &'a Option<String>,
}

impl<'a> TableBindingRef<'a> {
    fn binding_name(self) -> &'a str {
        self.alias.as_deref().unwrap_or(self.name)
    }
}

#[derive(Clone, Copy)]
struct QualifiedColumnRef<'a> {
    table: Option<&'a str>,
    column: &'a str,
}

#[derive(Clone, Copy)]
struct SimpleSpatialJoinPredicate<'a> {
    left: QualifiedColumnRef<'a>,
    right: QualifiedColumnRef<'a>,
}

fn simple_spatial_join_predicate<'a>(
    expr: &'a Expr,
    left_binding: TableBindingRef<'a>,
    right_binding: TableBindingRef<'a>,
) -> Option<SimpleSpatialJoinPredicate<'a>> {
    let Expr::Function { name, args } = expr else {
        return None;
    };
    let lower = name.to_ascii_lowercase();
    let (left, right) = if lower == "st_dwithin" {
        let [left, right, radius] = args.as_slice() else {
            return None;
        };
        if expr_has_column_ref(radius) {
            return None;
        }
        (left, right)
    } else if matches!(
        lower.as_str(),
        "st_intersects" | "st_contains" | "st_within" | "st_equals"
    ) {
        let [left, right] = args.as_slice() else {
            return None;
        };
        (left, right)
    } else {
        return None;
    };
    let left_ref = qualified_column_ref_expr(left)?;
    let right_ref = qualified_column_ref_expr(right)?;
    let left_is_left = matches_table_binding(left_binding, left_ref.table);
    let left_is_right = matches_table_binding(right_binding, left_ref.table);
    let right_is_left = matches_table_binding(left_binding, right_ref.table);
    let right_is_right = matches_table_binding(right_binding, right_ref.table);
    if (left_is_left && right_is_right) || (left_is_right && right_is_left) {
        Some(SimpleSpatialJoinPredicate {
            left: left_ref,
            right: right_ref,
        })
    } else {
        None
    }
}

fn qualified_column_ref_expr(expr: &Expr) -> Option<QualifiedColumnRef<'_>> {
    let Expr::Column { table, column } = expr else {
        return None;
    };
    Some(QualifiedColumnRef {
        table: table.as_deref(),
        column,
    })
}

fn matches_table_binding(table: TableBindingRef<'_>, qualifier: Option<&str>) -> bool {
    qualifier.is_some_and(|qualifier| identifiers_equal(qualifier, table.binding_name()))
}

fn simple_spatial_order_column(expr: &Expr) -> Option<&str> {
    let Expr::Binary {
        left,
        op: BinaryOp::Distance,
        right,
    } = expr
    else {
        return None;
    };
    match (&**left, &**right) {
        (Expr::Column { column, .. }, value) if !expr_has_column_ref(value) => {
            Some(column.as_str())
        }
        (value, Expr::Column { column, .. }) if !expr_has_column_ref(value) => {
            Some(column.as_str())
        }
        _ => None,
    }
}

fn simple_spatial_indexable_filter(filter: &Expr) -> Option<&str> {
    match filter {
        Expr::Function { name, args } if name.eq_ignore_ascii_case("st_dwithin") => {
            let [left, right, _] = args.as_slice() else {
                return None;
            };
            simple_spatial_column_value_pair(left, right)
        }
        Expr::Function { name, args }
            if name.eq_ignore_ascii_case("st_intersects")
                || name.eq_ignore_ascii_case("st_contains")
                || name.eq_ignore_ascii_case("st_within")
                || name.eq_ignore_ascii_case("st_equals") =>
        {
            let [left, right] = args.as_slice() else {
                return None;
            };
            simple_spatial_column_value_pair(left, right)
        }
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            simple_spatial_indexable_filter(left).or_else(|| simple_spatial_indexable_filter(right))
        }
        _ => None,
    }
}

fn simple_spatial_column_value_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<&'a str> {
    match (left, right) {
        (Expr::Column { column, .. }, value) if !expr_has_column_ref(value) => {
            Some(column.as_str())
        }
        (value, Expr::Column { column, .. }) if !expr_has_column_ref(value) => {
            Some(column.as_str())
        }
        _ => None,
    }
}

fn expr_has_column_ref(expr: &Expr) -> bool {
    match expr {
        Expr::Column { .. } => true,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            expr_has_column_ref(expr)
        }
        Expr::Binary { left, right, .. } => expr_has_column_ref(left) || expr_has_column_ref(right),
        Expr::Between {
            expr, low, high, ..
        } => expr_has_column_ref(expr) || expr_has_column_ref(low) || expr_has_column_ref(high),
        Expr::InList { expr, items, .. } => {
            expr_has_column_ref(expr) || items.iter().any(expr_has_column_ref)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_has_column_ref(expr)
                || expr_has_column_ref(pattern)
                || escape.as_deref().is_some_and(expr_has_column_ref)
        }
        Expr::Function { args, .. } => args.iter().any(expr_has_column_ref),
        Expr::Aggregate { args, order_by, .. } => {
            args.iter().any(expr_has_column_ref)
                || order_by
                    .iter()
                    .any(|order| expr_has_column_ref(&order.expr))
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_has_column_ref)
                || branches.iter().any(|(condition, value)| {
                    expr_has_column_ref(condition) || expr_has_column_ref(value)
                })
                || else_expr.as_deref().is_some_and(expr_has_column_ref)
        }
        Expr::Row(items) => items.iter().any(expr_has_column_ref),
        Expr::InSubquery { expr, .. } | Expr::CompareSubquery { expr, .. } => {
            expr_has_column_ref(expr)
        }
        Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => false,
    }
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
        Expr::CompareSubquery { expr, .. } => expr_has_aggregate(expr),
        Expr::ScalarSubquery(_) | Expr::Exists(_) => false,
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
        Expr::RowNumber { .. } | Expr::WindowFunction { .. } => false,
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
        Expr::Row(items) => items.iter().any(expr_has_aggregate),
        Expr::Cast { expr, .. } => expr_has_aggregate(expr),
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::value::Value;
    use crate::sql::ast::{SelectItem, UnaryOp};

    fn col(name: &str) -> Expr {
        Expr::Column {
            table: None,
            column: name.to_string(),
        }
    }

    fn lit_int(v: i64) -> Expr {
        Expr::Literal(Value::Int64(v))
    }

    fn lit_text(s: &str) -> Expr {
        Expr::Literal(Value::Text(s.to_string()))
    }

    fn agg_count() -> Expr {
        Expr::Aggregate {
            name: "COUNT".to_string(),
            args: vec![],
            distinct: false,
            star: true,
            order_by: vec![],
            within_group: false,
        }
    }

    fn agg_sum(e: Expr) -> Expr {
        Expr::Aggregate {
            name: "SUM".to_string(),
            args: vec![e],
            distinct: false,
            star: false,
            order_by: vec![],
            within_group: false,
        }
    }

    // ── expr_has_aggregate ──────────────────────────────────────────

    #[test]
    fn aggregate_literal() {
        assert!(!expr_has_aggregate(&lit_int(42)));
    }

    #[test]
    fn aggregate_column() {
        assert!(!expr_has_aggregate(&col("x")));
    }

    #[test]
    fn aggregate_parameter() {
        assert!(!expr_has_aggregate(&Expr::Parameter(1)));
    }

    #[test]
    fn aggregate_direct() {
        assert!(expr_has_aggregate(&agg_count()));
    }

    #[test]
    fn aggregate_in_unary() {
        let expr = Expr::Unary {
            op: UnaryOp::Negate,
            expr: Box::new(agg_sum(col("x"))),
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_binary_left() {
        let expr = Expr::Binary {
            left: Box::new(agg_sum(col("x"))),
            op: BinaryOp::Add,
            right: Box::new(lit_int(1)),
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_binary_right() {
        let expr = Expr::Binary {
            left: Box::new(lit_int(1)),
            op: BinaryOp::Add,
            right: Box::new(agg_count()),
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_between() {
        let expr = Expr::Between {
            expr: Box::new(agg_sum(col("x"))),
            low: Box::new(lit_int(0)),
            high: Box::new(lit_int(100)),
            negated: false,
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_between_high() {
        let expr = Expr::Between {
            expr: Box::new(col("x")),
            low: Box::new(lit_int(0)),
            high: Box::new(agg_sum(col("y"))),
            negated: false,
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_in_list() {
        let expr = Expr::InList {
            expr: Box::new(col("x")),
            items: vec![lit_int(1), agg_count()],
            negated: false,
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_in_subquery() {
        let expr = Expr::InSubquery {
            expr: Box::new(agg_sum(col("x"))),
            query: Box::new(Query {
                ctes: vec![],
                recursive: false,
                body: QueryBody::Select(Select {
                    distinct: false,
                    distinct_on: vec![],
                    projection: vec![],
                    from: vec![],
                    filter: None,
                    group_by: vec![],
                    having: None,
                }),
                order_by: vec![],
                limit: None,
                offset: None,
            }),
            negated: false,
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_compare_subquery_left_expr() {
        let expr = Expr::CompareSubquery {
            expr: Box::new(agg_sum(col("x"))),
            op: BinaryOp::Gt,
            quantifier: crate::sql::ast::SubqueryQuantifier::Any,
            query: Box::new(Query {
                ctes: vec![],
                recursive: false,
                body: QueryBody::Select(Select {
                    distinct: false,
                    distinct_on: vec![],
                    projection: vec![],
                    from: vec![],
                    filter: None,
                    group_by: vec![],
                    having: None,
                }),
                order_by: vec![],
                limit: None,
                offset: None,
            }),
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn no_aggregate_scalar_subquery() {
        let q = Query {
            ctes: vec![],
            recursive: false,
            body: QueryBody::Select(Select {
                distinct: false,
                distinct_on: vec![],
                projection: vec![],
                from: vec![],
                filter: None,
                group_by: vec![],
                having: None,
            }),
            order_by: vec![],
            limit: None,
            offset: None,
        };
        assert!(!expr_has_aggregate(&Expr::ScalarSubquery(Box::new(
            q.clone()
        ))));
        assert!(!expr_has_aggregate(&Expr::Exists(Box::new(q))));
    }

    #[test]
    fn aggregate_in_like_pattern() {
        let expr = Expr::Like {
            expr: Box::new(col("x")),
            pattern: Box::new(agg_sum(col("y"))),
            escape: None,
            case_insensitive: false,
            negated: false,
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_like_escape() {
        let expr = Expr::Like {
            expr: Box::new(col("x")),
            pattern: Box::new(lit_text("%")),
            escape: Some(Box::new(agg_count())),
            case_insensitive: false,
            negated: false,
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_is_null() {
        let expr = Expr::IsNull {
            expr: Box::new(agg_sum(col("x"))),
            negated: false,
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_function_args() {
        let expr = Expr::Function {
            name: "upper".to_string(),
            args: vec![agg_sum(col("x"))],
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn no_aggregate_window_function() {
        let expr = Expr::WindowFunction {
            name: "LEAD".to_string(),
            args: vec![col("x")],
            partition_by: vec![],
            order_by: vec![],
            frame: None,
            distinct: false,
            star: false,
        };
        assert!(!expr_has_aggregate(&expr));
    }

    #[test]
    fn no_aggregate_row_number() {
        let expr = Expr::RowNumber {
            partition_by: vec![],
            order_by: vec![],
            frame: None,
        };
        assert!(!expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_case_operand() {
        let expr = Expr::Case {
            operand: Some(Box::new(agg_count())),
            branches: vec![(lit_int(1), lit_text("one"))],
            else_expr: None,
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_case_branch() {
        let expr = Expr::Case {
            operand: None,
            branches: vec![(col("x"), agg_sum(col("y")))],
            else_expr: None,
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_case_else() {
        let expr = Expr::Case {
            operand: None,
            branches: vec![(col("x"), lit_int(1))],
            else_expr: Some(Box::new(agg_count())),
        };
        assert!(expr_has_aggregate(&expr));
    }

    #[test]
    fn no_aggregate_case_without() {
        let expr = Expr::Case {
            operand: None,
            branches: vec![(col("x"), lit_int(1))],
            else_expr: Some(Box::new(lit_int(0))),
        };
        assert!(!expr_has_aggregate(&expr));
    }

    #[test]
    fn aggregate_in_cast() {
        let expr = Expr::Cast {
            expr: Box::new(agg_count()),
            target_type: crate::catalog::ColumnType::Int64,
        };
        assert!(expr_has_aggregate(&expr));
    }

    // ── simple_indexable_filter ──────────────────────────────────────

    #[test]
    fn indexable_eq_column_literal() {
        let filter = Expr::Binary {
            left: Box::new(col("id")),
            op: BinaryOp::Eq,
            right: Box::new(lit_int(42)),
        };
        let result = simple_indexable_filter(&filter);
        assert_eq!(result, Some(("id", false)));
    }

    #[test]
    fn indexable_eq_literal_column() {
        let filter = Expr::Binary {
            left: Box::new(lit_int(42)),
            op: BinaryOp::Eq,
            right: Box::new(col("id")),
        };
        let result = simple_indexable_filter(&filter);
        assert_eq!(result, Some(("id", false)));
    }

    #[test]
    fn indexable_like_column_text() {
        let filter = Expr::Like {
            expr: Box::new(col("name")),
            pattern: Box::new(lit_text("%hello%")),
            escape: None,
            case_insensitive: false,
            negated: false,
        };
        let result = simple_indexable_filter(&filter);
        assert_eq!(result, Some(("name", true)));
    }

    #[test]
    fn not_indexable_gt() {
        let filter = Expr::Binary {
            left: Box::new(col("id")),
            op: BinaryOp::Gt,
            right: Box::new(lit_int(42)),
        };
        assert!(simple_indexable_filter(&filter).is_none());
    }

    #[test]
    fn not_indexable_column_eq_column() {
        let filter = Expr::Binary {
            left: Box::new(col("a")),
            op: BinaryOp::Eq,
            right: Box::new(col("b")),
        };
        assert!(simple_indexable_filter(&filter).is_none());
    }

    #[test]
    fn not_indexable_like_non_text_pattern() {
        let filter = Expr::Like {
            expr: Box::new(col("name")),
            pattern: Box::new(col("pattern_col")),
            escape: None,
            case_insensitive: false,
            negated: false,
        };
        assert!(simple_indexable_filter(&filter).is_none());
    }

    #[test]
    fn not_indexable_is_null() {
        let filter = Expr::IsNull {
            expr: Box::new(col("x")),
            negated: false,
        };
        assert!(simple_indexable_filter(&filter).is_none());
    }

    // ── select_item_has_aggregate ───────────────────────────────────

    #[test]
    fn select_item_wildcard_no_aggregate() {
        assert!(!select_item_has_aggregate(&SelectItem::Wildcard));
    }

    #[test]
    fn select_item_qualified_wildcard_no_aggregate() {
        assert!(!select_item_has_aggregate(&SelectItem::QualifiedWildcard(
            "t".to_string()
        )));
    }

    #[test]
    fn select_item_expr_with_aggregate() {
        assert!(select_item_has_aggregate(&SelectItem::Expr {
            expr: agg_count(),
            alias: None,
        }));
    }

    #[test]
    fn select_item_expr_without_aggregate() {
        assert!(!select_item_has_aggregate(&SelectItem::Expr {
            expr: col("x"),
            alias: None,
        }));
    }

    // ── projection_has_aggregate ────────────────────────────────────

    #[test]
    fn projection_with_aggregate() {
        let select = Select {
            distinct: false,
            distinct_on: vec![],
            projection: vec![
                SelectItem::Expr {
                    expr: col("name"),
                    alias: None,
                },
                SelectItem::Expr {
                    expr: agg_count(),
                    alias: Some("cnt".to_string()),
                },
            ],
            from: vec![],
            filter: None,
            group_by: vec![],
            having: None,
        };
        assert!(projection_has_aggregate(&select));
    }

    #[test]
    fn projection_without_aggregate() {
        let select = Select {
            distinct: false,
            distinct_on: vec![],
            projection: vec![
                SelectItem::Expr {
                    expr: col("a"),
                    alias: None,
                },
                SelectItem::Wildcard,
            ],
            from: vec![],
            filter: None,
            group_by: vec![],
            having: None,
        };
        assert!(!projection_has_aggregate(&select));
    }

    // ── should_use_btree_index ──────────────────────────────────────

    #[test]
    fn btree_no_stats_defaults_yes() {
        let catalog = CatalogState::empty(0);
        assert!(should_use_btree_index("t", "idx", &catalog));
    }

    #[test]
    fn btree_no_index_stats_defaults_yes() {
        let mut catalog = CatalogState::empty(0);
        catalog.table_stats.insert(
            "t".to_string(),
            crate::catalog::TableStats { row_count: 1000 },
        );
        assert!(should_use_btree_index("t", "idx", &catalog));
    }

    #[test]
    fn btree_zero_row_count_skip() {
        let mut catalog = CatalogState::empty(0);
        catalog
            .table_stats
            .insert("t".to_string(), crate::catalog::TableStats { row_count: 0 });
        catalog.index_stats.insert(
            "idx".to_string(),
            crate::catalog::IndexStats {
                entry_count: 0,
                distinct_key_count: 0,
            },
        );
        assert!(!should_use_btree_index("t", "idx", &catalog));
    }

    #[test]
    fn btree_high_selectivity_use_index() {
        let mut catalog = CatalogState::empty(0);
        catalog.table_stats.insert(
            "t".to_string(),
            crate::catalog::TableStats { row_count: 10000 },
        );
        catalog.index_stats.insert(
            "idx".to_string(),
            crate::catalog::IndexStats {
                entry_count: 10000,
                distinct_key_count: 10000,
            },
        );
        // estimated_matches = 1, 1*4 < 10000 => true
        assert!(should_use_btree_index("t", "idx", &catalog));
    }

    #[test]
    fn btree_low_selectivity_skip_index() {
        let mut catalog = CatalogState::empty(0);
        catalog.table_stats.insert(
            "t".to_string(),
            crate::catalog::TableStats { row_count: 100 },
        );
        catalog.index_stats.insert(
            "idx".to_string(),
            crate::catalog::IndexStats {
                entry_count: 100,
                distinct_key_count: 2,
            },
        );
        // estimated_matches = 50, 50*4 = 200 >= 100 => false
        assert!(!should_use_btree_index("t", "idx", &catalog));
    }
}
