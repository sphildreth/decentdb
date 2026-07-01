//! Logical planning and rule-based optimization.

pub(crate) mod logical;
pub(crate) mod physical;

use crate::catalog::{identifiers_equal, CatalogState, IndexKind, TableSchema};
use crate::error::Result;
use crate::record::value::Value;
use crate::sql::ast::{
    BinaryOp, Expr, FromItem, JoinConstraint, JoinKind, Query, QueryBody, Select, SelectItem,
    Statement,
};
use crate::sql::parser::parse_sql_statement;

use self::physical::{PhysicalPlan, PlanEstimate};

const PLANNER_TABLE_ROWS_HEURISTIC: u64 = 1_000;
const PLANNER_ROWS_PER_PAGE: f64 = 100.0;
const PLANNER_EQ_SELECTIVITY_WITH_STATS: f64 = 0.10;
const PLANNER_EQ_SELECTIVITY_WITHOUT_STATS: f64 = 0.10;
const PLANNER_RANGE_SELECTIVITY: f64 = 0.30;
const PLANNER_LIKE_SELECTIVITY: f64 = 0.05;

pub(crate) fn plan_statement(
    statement: &Statement,
    catalog: &CatalogState,
) -> Result<PhysicalPlan> {
    match statement {
        Statement::Query(query) => plan_query(query, catalog),
        Statement::Explain(explain) => plan_statement(&explain.statement, catalog),
        _ => Ok(PhysicalPlan::Empty {
            estimate: PlanEstimate::ZERO,
        }),
    }
}

pub(crate) fn plan_query(query: &Query, catalog: &CatalogState) -> Result<PhysicalPlan> {
    let view_pushdown = simple_view_pushdown_flags(query, catalog);
    let mut plan = plan_query_body(&query.body, catalog)?;
    if !query.order_by.is_empty() {
        plan = maybe_spatial_knn_plan(&plan, query, catalog)
            .or_else(|| maybe_ordered_row_id_scan_plan(query, catalog))
            .unwrap_or_else(|| PhysicalPlan::Sort {
                input: Box::new(plan),
                order_by: query.order_by.clone(),
                estimate: PlanEstimate::ZERO,
            });
    }
    if query.limit.is_some() || query.offset.is_some() {
        plan = PhysicalPlan::Limit {
            input: Box::new(plan),
            limit: query.limit.clone(),
            offset: query.offset.clone(),
            estimate: PlanEstimate::ZERO,
        };
    }
    if view_pushdown != ViewPushdownFlags::default() {
        plan = mark_expanded_view_pushdown(plan, view_pushdown);
    }
    Ok(annotate_plan(plan, catalog))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ViewPushdownFlags {
    filter: bool,
    projection: bool,
    limit: bool,
}

fn simple_view_pushdown_flags(query: &Query, catalog: &CatalogState) -> ViewPushdownFlags {
    let QueryBody::Select(select) = &query.body else {
        return ViewPushdownFlags::default();
    };
    let [FromItem::Table { name, .. }] = select.from.as_slice() else {
        return ViewPushdownFlags::default();
    };
    if catalog.view(name).is_none() {
        return ViewPushdownFlags::default();
    }
    let filter = select.filter.is_some();
    ViewPushdownFlags {
        filter,
        projection: select_projection_can_push_into_view(&select.projection),
        limit: query.limit.is_some()
            && query.offset.is_none()
            && query.order_by.is_empty()
            && !filter,
    }
}

fn select_projection_can_push_into_view(projection: &[SelectItem]) -> bool {
    !projection.is_empty()
        && projection.iter().all(|item| match item {
            SelectItem::Expr { expr, .. } => !expr_has_aggregate(expr),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
}

fn mark_expanded_view_pushdown(plan: PhysicalPlan, flags: ViewPushdownFlags) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Project {
            input,
            items,
            estimate,
        } => PhysicalPlan::Project {
            input: Box::new(mark_expanded_view_pushdown(*input, flags)),
            items,
            estimate,
        },
        PhysicalPlan::Filter {
            input,
            predicate,
            estimate,
        } => PhysicalPlan::Filter {
            input: Box::new(mark_expanded_view_pushdown(*input, flags)),
            predicate,
            estimate,
        },
        PhysicalPlan::Sort {
            input,
            order_by,
            estimate,
        } => PhysicalPlan::Sort {
            input: Box::new(mark_expanded_view_pushdown(*input, flags)),
            order_by,
            estimate,
        },
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
            estimate,
        } => PhysicalPlan::Limit {
            input: Box::new(mark_expanded_view_pushdown(*input, flags)),
            limit,
            offset,
            estimate,
        },
        PhysicalPlan::ExpandedView {
            name,
            input,
            pushed_filter,
            pushed_projection,
            pushed_limit,
            estimate,
        } => PhysicalPlan::ExpandedView {
            name,
            input,
            pushed_filter: pushed_filter || flags.filter,
            pushed_projection: pushed_projection || flags.projection,
            pushed_limit: pushed_limit || flags.limit,
            estimate,
        },
        other => other,
    }
}

fn plan_query_body(query: &QueryBody, catalog: &CatalogState) -> Result<PhysicalPlan> {
    match query {
        QueryBody::Select(select) => plan_select(select, catalog),
        QueryBody::Values(_) => Ok(PhysicalPlan::Empty {
            estimate: PlanEstimate::ZERO,
        }),
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
            estimate: PlanEstimate::ZERO,
        }),
    }
}

fn plan_select(select: &Select, catalog: &CatalogState) -> Result<PhysicalPlan> {
    let mut plan = if select.from.is_empty() {
        PhysicalPlan::Empty {
            estimate: PlanEstimate::ZERO,
        }
    } else {
        plan_from_item(&select.from[0], catalog)?
    };
    for item in select.from.iter().skip(1) {
        plan = PhysicalPlan::NestedLoopJoin {
            left: Box::new(plan),
            right: Box::new(plan_from_item(item, catalog)?),
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(Expr::Literal(crate::record::value::Value::Bool(true))),
            estimate: PlanEstimate::ZERO,
        };
    }

    if let Some(filter) = &select.filter {
        if let Some(index_plan) = maybe_index_plan(select, filter, catalog) {
            plan = index_plan;
        } else {
            plan = PhysicalPlan::Filter {
                input: Box::new(plan),
                predicate: filter.clone(),
                estimate: PlanEstimate::ZERO,
            };
        }
    }
    if !select.group_by.is_empty() || projection_has_aggregate(select) {
        plan = PhysicalPlan::StreamingAggregate {
            input: Box::new(plan),
            group_by: select.group_by.clone(),
            having: select.having.clone(),
            estimate: PlanEstimate::ZERO,
        };
    }
    plan = PhysicalPlan::Project {
        input: Box::new(plan),
        items: select.projection.clone(),
        estimate: PlanEstimate::ZERO,
    };
    let plan = rewrite_join_order(plan, select, catalog)?;
    Ok(plan)
}

fn plan_from_item(item: &FromItem, catalog: &CatalogState) -> Result<PhysicalPlan> {
    Ok(match item {
        FromItem::Table { name, .. } => {
            if catalog.view(name).is_some() {
                if let Some(plan) = maybe_expand_view(name, catalog)? {
                    return Ok(plan);
                }
                PhysicalPlan::ViewScan {
                    name: name.clone(),
                    estimate: PlanEstimate::ZERO,
                }
            } else {
                PhysicalPlan::TableScan {
                    table: name.clone(),
                    estimate: PlanEstimate::ZERO,
                }
            }
        }
        FromItem::Function { name, alias, .. } => PhysicalPlan::TableScan {
            table: alias.clone().unwrap_or_else(|| format!("tvf:{name}")),
            estimate: PlanEstimate::ZERO,
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
            } else if let Some(plan) = maybe_join_plan(left, right, *kind, constraint, catalog) {
                plan
            } else {
                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(plan_from_item(left, catalog)?),
                    right: Box::new(plan_from_item(right, catalog)?),
                    kind: *kind,
                    constraint: constraint.clone(),
                    estimate: PlanEstimate::ZERO,
                }
            }
        }
    })
}

fn rewrite_join_order(
    original_plan: PhysicalPlan,
    select: &Select,
    catalog: &CatalogState,
) -> Result<PhysicalPlan> {
    if select.from.len() < 2
        || !select
            .from
            .iter()
            .all(|item| matches!(item, FromItem::Table { .. }))
    {
        return Ok(original_plan);
    }
    rewrite_join_order_by_stats(select.from.as_slice(), select.filter.as_ref(), catalog)
}

fn rewrite_join_order_by_stats(
    from_items: &[FromItem],
    filter: Option<&Expr>,
    catalog: &CatalogState,
) -> Result<PhysicalPlan> {
    let mut table_plans = Vec::with_capacity(from_items.len());
    for item in from_items {
        table_plans.push(plan_from_item(item, catalog)?);
    }
    let mut tables = Vec::with_capacity(table_plans.len());
    for (index, item) in from_items.iter().enumerate() {
        let (table, alias) = match item {
            FromItem::Table { name, alias } => (name.as_str(), alias.as_ref()),
            _ => {
                return Ok(PhysicalPlan::Empty {
                    estimate: PlanEstimate::ZERO,
                })
            }
        };
        let columns = catalog.table(table).map_or_else(Vec::new, |table| {
            table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        });
        let annotated = annotate_plan(table_plans[index].clone(), catalog);
        tables.push(JoinTableRelation {
            name: table.to_string(),
            alias: alias.cloned(),
            columns,
            plan: annotated,
        });
    }

    if tables.len() < 2 {
        return Ok(tables.into_iter().next().map_or_else(
            || PhysicalPlan::Empty {
                estimate: PlanEstimate::ZERO,
            },
            |table| table.plan,
        ));
    }

    let join_predicates = filter
        .filter(|filter| !filter_is_always_true(filter))
        .map(|filter| collect_join_predicates(tables.as_slice(), filter))
        .unwrap_or_default();

    if tables.len() <= 6 {
        build_left_deep_join_plan_dp(&tables, &join_predicates, catalog)
    } else {
        build_greedy_left_deep_join_plan(&tables, &join_predicates, catalog)
    }
}

#[derive(Clone, Debug)]
struct JoinTableRelation {
    name: String,
    alias: Option<String>,
    columns: Vec<String>,
    plan: PhysicalPlan,
}

impl JoinTableRelation {
    fn estimate_rows(&self) -> u64 {
        self.plan.estimate().rows.max(1)
    }
}

#[derive(Clone, Debug)]
struct JoinPredicate {
    left_table: usize,
    right_table: usize,
    left_column: String,
    right_column: String,
    expr: Expr,
}

fn collect_join_predicates(relations: &[JoinTableRelation], filter: &Expr) -> Vec<JoinPredicate> {
    let mut predicates = Vec::new();
    collect_join_predicates_recursive(relations, filter, &mut predicates);
    predicates
}

fn collect_join_predicates_recursive(
    relations: &[JoinTableRelation],
    expr: &Expr,
    out: &mut Vec<JoinPredicate>,
) {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_join_predicates_recursive(relations, left, out);
            collect_join_predicates_recursive(relations, right, out);
        }
        Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            let Some((left_table, left_column)) = relation_column_from_expr(left, relations) else {
                return;
            };
            let Some((right_table, right_column)) = relation_column_from_expr(right, relations)
            else {
                return;
            };
            if left_table == right_table {
                return;
            }
            out.push(JoinPredicate {
                left_table,
                right_table,
                left_column,
                right_column,
                expr: Expr::Binary {
                    left: left.clone(),
                    op: BinaryOp::Eq,
                    right: right.clone(),
                },
            });
        }
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            collect_join_predicates_recursive(relations, left, out);
            collect_join_predicates_recursive(relations, right, out);
        }
        _ => {}
    }
}

fn relation_column_from_expr(
    expr: &Expr,
    relations: &[JoinTableRelation],
) -> Option<(usize, String)> {
    let Expr::Column { table, column } = expr else {
        return None;
    };
    let with_qualifier = table.as_ref().and_then(|qualifier| {
        relations
            .iter()
            .position(|relation| {
                identifiers_equal(&relation.name, qualifier)
                    || relation
                        .alias
                        .as_deref()
                        .is_some_and(|alias| identifiers_equal(alias, qualifier))
            })
            .filter(|&index| relation_has_column(&relations[index], column))
    });
    if let Some(index) = with_qualifier {
        return Some((index, column.clone()));
    }
    let matches = relations
        .iter()
        .enumerate()
        .filter(|(_, relation)| relation_has_column(relation, column))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        Some((matches[0], column.clone()))
    } else {
        None
    }
}

fn relation_has_column(relation: &JoinTableRelation, column: &str) -> bool {
    relation
        .columns
        .iter()
        .any(|candidate| identifiers_equal(candidate, column))
}

fn filter_is_always_true(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Value::Bool(true)))
}

fn build_left_deep_join_plan_dp(
    relations: &[JoinTableRelation],
    join_predicates: &[JoinPredicate],
    catalog: &CatalogState,
) -> Result<PhysicalPlan> {
    let relation_count = relations.len();
    let total_states = 1_usize << relation_count;
    let mut best: Vec<Option<(PlanEstimate, PhysicalPlan, u64)>> = vec![None; total_states];
    for (index, relation) in relations.iter().enumerate() {
        let mask = 1_usize << index;
        let estimate = relation.plan.estimate();
        best[mask] = Some((estimate, relation.plan.clone(), estimate.rows));
    }

    for mask in 1..total_states {
        let Some((_, base_plan, base_rows)) = best[mask].as_ref().cloned() else {
            continue;
        };
        for next in 0..relation_count {
            let next_bit = 1_usize << next;
            if mask & next_bit != 0 {
                continue;
            }
            let Some(next_relation) = relations.get(next) else {
                continue;
            };
            let on = predicates_to_join_expr(mask, next, join_predicates);
            let right_index_name = on.as_ref().and_then(|_| {
                index_name_for_relation_join_predicates(
                    next,
                    &next_relation.name,
                    mask,
                    join_predicates,
                    catalog,
                )
            });
            let candidate = choose_join_plan(
                base_plan.clone(),
                next_relation.plan.clone(),
                on,
                catalog,
                None,
                right_index_name,
            );
            let candidate = annotate_plan(candidate, catalog);
            let next_mask = mask | next_bit;
            let candidate_cost = candidate.estimate().cost;
            let candidate_rows = candidate.estimate().rows.max(1).max(base_rows);
            let candidate = (
                PlanEstimate {
                    rows: candidate_rows,
                    cost: candidate_cost,
                },
                candidate,
                candidate_rows,
            );
            if best[next_mask]
                .as_ref()
                .is_none_or(|(existing, _, _)| existing.cost > candidate_cost)
            {
                best[next_mask] = Some(candidate);
            }
        }
    }
    if let Some((_, plan, _)) = best[total_states - 1].clone() {
        Ok(plan)
    } else {
        Ok(PhysicalPlan::Empty {
            estimate: PlanEstimate::ZERO,
        })
    }
}

fn predicates_to_join_expr(
    left_mask: usize,
    right: usize,
    predicates: &[JoinPredicate],
) -> Option<Expr> {
    let exprs: Vec<Expr> = predicates
        .iter()
        .filter(|predicate| {
            (predicate.left_table == right && mask_has_relation(left_mask, predicate.right_table))
                || (predicate.right_table == right
                    && mask_has_relation(left_mask, predicate.left_table))
        })
        .map(|predicate| predicate.expr.clone())
        .collect();
    merge_predicates_by_and(exprs)
}

fn build_greedy_left_deep_join_plan(
    relations: &[JoinTableRelation],
    join_predicates: &[JoinPredicate],
    catalog: &CatalogState,
) -> Result<PhysicalPlan> {
    if relations.is_empty() {
        return Ok(PhysicalPlan::Empty {
            estimate: PlanEstimate::ZERO,
        });
    }
    let mut sorted_indexes: Vec<usize> = (0..relations.len()).collect();
    sorted_indexes.sort_by_key(|index| relations[*index].estimate_rows());
    let mut used_mask = 0_usize;
    let first_index = sorted_indexes
        .first()
        .copied()
        .expect("at least one join relation");
    used_mask |= 1usize << first_index;
    let first_relation = &relations[first_index];
    let mut plan = first_relation.plan.clone();
    for index in sorted_indexes {
        let next_relation = &relations[index];
        let next_bit = 1_usize << index;
        if used_mask & next_bit != 0 {
            continue;
        }
        let on = predicates_to_join_expr(used_mask, index, join_predicates);
        let next_index_name = index_name_for_relation_join_predicates(
            index,
            &next_relation.name,
            used_mask,
            join_predicates,
            catalog,
        );
        let candidate = annotate_plan(
            choose_join_plan(
                plan,
                next_relation.plan.clone(),
                on,
                catalog,
                None,
                next_index_name,
            ),
            catalog,
        );
        plan = candidate;
        used_mask |= next_bit;
    }
    Ok(plan)
}

fn mask_has_relation(mask: usize, relation_index: usize) -> bool {
    (mask & (1_usize << relation_index)) != 0
}

fn choose_join_plan(
    left: PhysicalPlan,
    right: PhysicalPlan,
    on: Option<Expr>,
    catalog: &CatalogState,
    left_index_name: Option<String>,
    right_index_name: Option<String>,
) -> PhysicalPlan {
    let (Some(on), kind) = (on, JoinKind::Inner) else {
        return PhysicalPlan::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(Expr::Literal(Value::Bool(true))),
            estimate: PlanEstimate::ZERO,
        };
    };
    if is_simple_equi_join(&on) {
        let indexed_candidates = [
            right_index_name
                .as_ref()
                .map(|index| (index.clone(), false)),
            left_index_name.as_ref().map(|index| (index.clone(), true)),
        ];
        let mut best: Option<PhysicalPlan> = None;
        let mut best_cost = f64::INFINITY;
        for (index_name, swap_sides) in indexed_candidates.into_iter().flatten() {
            let candidate = if swap_sides {
                PhysicalPlan::IndexedJoin {
                    left: Box::new(right.clone()),
                    right: Box::new(left.clone()),
                    kind,
                    on: swap_join_on_expr(&on),
                    index: index_name,
                    estimate: PlanEstimate::ZERO,
                }
            } else {
                PhysicalPlan::IndexedJoin {
                    left: Box::new(left.clone()),
                    right: Box::new(right.clone()),
                    kind,
                    on: on.clone(),
                    index: index_name,
                    estimate: PlanEstimate::ZERO,
                }
            };
            let cost = estimate_join_plan_cost(&candidate, catalog);
            if cost < best_cost {
                best_cost = cost;
                best = Some(candidate);
            }
        }
        if let Some(best) = best {
            return best;
        }
        return PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            kind,
            on,
            estimate: PlanEstimate::ZERO,
        };
    }
    PhysicalPlan::NestedLoopJoin {
        left: Box::new(left),
        right: Box::new(right),
        kind,
        constraint: JoinConstraint::On(on),
        estimate: PlanEstimate::ZERO,
    }
}

fn estimate_join_plan_cost(plan: &PhysicalPlan, catalog: &CatalogState) -> f64 {
    match plan {
        PhysicalPlan::IndexedJoin {
            left,
            right,
            on: _,
            index,
            ..
        } => {
            let left_plan = annotate_plan((**left).clone(), catalog);
            let right_plan = annotate_plan((**right).clone(), catalog);
            let right_rows = right_plan.estimate().rows.max(1);
            let probe_cost = estimate_join_probe_cost(
                right_rows,
                index,
                catalog,
                &Expr::Literal(Value::Bool(true)),
            );
            left_plan.estimate().cost + (left_plan.estimate().rows as f64) * probe_cost
        }
        PhysicalPlan::HashJoin { left, right, .. } => {
            let left_plan = annotate_plan((**left).clone(), catalog);
            let right_plan = annotate_plan((**right).clone(), catalog);
            left_plan.estimate().cost
                + right_plan.estimate().cost
                + left_plan.estimate().rows as f64
        }
        _ => {
            let annotated = annotate_plan(plan.clone(), catalog);
            annotated.estimate().cost
        }
    }
}

fn maybe_join_plan(
    left: &FromItem,
    right: &FromItem,
    kind: JoinKind,
    constraint: &JoinConstraint,
    catalog: &CatalogState,
) -> Option<PhysicalPlan> {
    if kind != JoinKind::Inner {
        return None;
    }
    let JoinConstraint::On(on) = constraint else {
        return None;
    };
    let FromItem::Table {
        name: left_name,
        alias: left_alias,
    } = left
    else {
        return None;
    };
    let FromItem::Table {
        name: right_name,
        alias: right_alias,
    } = right
    else {
        return None;
    };
    let left_binding = TableBindingRef {
        name: left_name,
        alias: left_alias,
    };
    let right_binding = TableBindingRef {
        name: right_name,
        alias: right_alias,
    };
    let left_plan = plan_from_item(left, catalog).ok()?;
    let right_plan = plan_from_item(right, catalog).ok()?;
    let left_index = join_side_index_name(catalog, left_binding, on);
    let right_index = join_side_index_name(catalog, right_binding, on);
    Some(choose_join_plan(
        left_plan,
        right_plan,
        Some(on.clone()),
        catalog,
        left_index,
        right_index,
    ))
}

fn join_side_index_name(
    catalog: &CatalogState,
    binding: TableBindingRef<'_>,
    constraint: &Expr,
) -> Option<String> {
    for predicate in flattened_join_predicate_columns(constraint) {
        // `flattened_join_predicate_columns` only splits AND-chains, so each
        // remaining predicate is an individual equality (or other comparison).
        // Decompose the equality into its two column operands so a usable join
        // index on either side is detected.
        let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = predicate
        else {
            continue;
        };
        for operand in [left.as_ref(), right.as_ref()] {
            let Some(column_ref) = qualified_column_ref_expr(operand) else {
                continue;
            };
            if !matches_table_binding(binding, column_ref.table) {
                continue;
            }
            if let Some(index_name) = catalog.indexes.values().find_map(|index| {
                if !identifiers_equal(&index.table_name, binding.name)
                    || index.columns.len() != 1
                    || index.predicate_sql.is_some()
                    || !index.fresh
                    || index.kind != IndexKind::Btree
                {
                    return None;
                }
                match &index.columns[0].column_name {
                    Some(indexed_column)
                        if identifiers_equal(indexed_column, column_ref.column) =>
                    {
                        Some(index.name.clone())
                    }
                    _ => None,
                }
            }) {
                return Some(index_name);
            }
        }
    }
    None
}

fn index_name_for_table_column(
    catalog: &CatalogState,
    table_name: &str,
    column_name: &str,
) -> Option<String> {
    if column_name.is_empty() {
        return None;
    }
    catalog.indexes.values().find_map(|index| {
        if !identifiers_equal(&index.table_name, table_name)
            || index.columns.len() != 1
            || index.predicate_sql.is_some()
            || index.columns[0].column_name.as_deref().is_none()
            || !index.fresh
            || index.kind != IndexKind::Btree
        {
            return None;
        }
        let indexed_column = index.columns[0].column_name.as_ref()?;
        if identifiers_equal(indexed_column, column_name) {
            Some(index.name.clone())
        } else {
            None
        }
    })
}

fn index_name_for_relation_join_predicates(
    relation_index: usize,
    relation_name: &str,
    used_mask: usize,
    predicates: &[JoinPredicate],
    catalog: &CatalogState,
) -> Option<String> {
    for predicate in predicates {
        if predicate.left_table == relation_index
            && used_mask_has_any_relation(used_mask, predicate.right_table)
        {
            if let Some(index_name) =
                index_name_for_table_column(catalog, relation_name, &predicate.left_column)
            {
                return Some(index_name);
            }
        }
        if predicate.right_table == relation_index
            && used_mask_has_any_relation(used_mask, predicate.left_table)
        {
            if let Some(index_name) =
                index_name_for_table_column(catalog, relation_name, &predicate.right_column)
            {
                return Some(index_name);
            }
        }
    }
    None
}

fn used_mask_has_any_relation(mask: usize, relation: usize) -> bool {
    (mask & (1_usize << relation)) != 0
}

fn merge_predicates_by_and(predicates: Vec<Expr>) -> Option<Expr> {
    let mut merged = None;
    for predicate in predicates {
        match merged {
            None => merged = Some(predicate),
            Some(existing) => {
                merged = Some(Expr::Binary {
                    left: Box::new(existing),
                    op: BinaryOp::And,
                    right: Box::new(predicate),
                });
            }
        }
    }
    merged
}

fn swap_join_on_expr(expr: &Expr) -> Expr {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => Expr::Binary {
            left: Box::new(swap_join_on_expr(left)),
            op: BinaryOp::And,
            right: Box::new(swap_join_on_expr(right)),
        },
        Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } => Expr::Binary {
            left: right.clone(),
            op: BinaryOp::Eq,
            right: left.clone(),
        },
        _ => expr.clone(),
    }
}

fn is_simple_equi_join(expr: &Expr) -> bool {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            matches!(left.as_ref(), Expr::Column { .. })
                && matches!(right.as_ref(), Expr::Column { .. })
        }
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => is_simple_equi_join(left) && is_simple_equi_join(right),
        _ => false,
    }
}

fn flattened_join_predicate_columns(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let mut parts = flattened_join_predicate_columns(left);
            parts.extend(flattened_join_predicate_columns(right));
            parts
        }
        _ => vec![expr],
    }
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
            estimate: PlanEstimate::ZERO,
            input: Box::new(PhysicalPlan::NestedLoopJoin {
                left: Box::new(plan_from_item(left, catalog)?),
                right: Box::new(plan_from_item(right, catalog)?),
                kind,
                constraint: constraint.clone(),
                estimate: PlanEstimate::ZERO,
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
    if let Some(index) = best_matching_compound_prefix_index(filter, table, catalog) {
        if !index.unique
            && !should_use_btree_index(table.name.as_str(), index.name.as_str(), catalog)
        {
            return None;
        }
        return Some(
            if select_projection_is_covered_by_index(select, table, index) {
                PhysicalPlan::CoveringIndexSeek {
                    table: table.name.clone(),
                    index: index.name.clone(),
                    predicate: filter.clone(),
                    estimate: PlanEstimate::ZERO,
                }
            } else {
                PhysicalPlan::IndexSeek {
                    table: table.name.clone(),
                    index: index.name.clone(),
                    predicate: filter.clone(),
                    estimate: PlanEstimate::ZERO,
                }
            },
        );
    }
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
            estimate: PlanEstimate::ZERO,
        });
    }
    let (column_name, uses_like) = simple_indexable_filter(filter)?;
    if !uses_like {
        if let Some(row_id_alias) = planner_row_id_alias_column_name(table)
            .filter(|row_id_alias| identifiers_equal(row_id_alias, column_name))
        {
            return Some(PhysicalPlan::RowIdLookup {
                table: table.name.clone(),
                column: row_id_alias.to_string(),
                predicate: filter.clone(),
                estimate: PlanEstimate::ZERO,
            });
        }
    }
    let index = catalog.indexes.values().find(|index| {
        identifiers_equal(&index.table_name, &table.name)
            && index.columns.len() == 1
            && index.predicate_sql.is_none()
            && index.columns[0]
                .column_name
                .as_ref()
                .is_some_and(|indexed| identifiers_equal(indexed, column_name))
            && (uses_like && index.kind == IndexKind::Trigram
                || !uses_like && index.kind == IndexKind::Btree)
            && index.fresh
    })?;
    if !uses_like
        && !index.unique
        && !table
            .primary_key_columns
            .iter()
            .any(|pk| identifiers_equal(pk, column_name))
        && !should_use_btree_index(table.name.as_str(), index.name.as_str(), catalog)
    {
        return None;
    }
    Some(if uses_like {
        PhysicalPlan::TrigramSearch {
            table: table.name.clone(),
            index: index.name.clone(),
            predicate: filter.clone(),
            estimate: PlanEstimate::ZERO,
        }
    } else if select_projection_is_covered_by_index(select, table, index) {
        PhysicalPlan::CoveringIndexSeek {
            table: table.name.clone(),
            index: index.name.clone(),
            predicate: filter.clone(),
            estimate: PlanEstimate::ZERO,
        }
    } else {
        PhysicalPlan::IndexSeek {
            table: table.name.clone(),
            index: index.name.clone(),
            predicate: filter.clone(),
            estimate: PlanEstimate::ZERO,
        }
    })
}

fn best_matching_compound_prefix_index<'a>(
    filter: &Expr,
    table: &TableSchema,
    catalog: &'a CatalogState,
) -> Option<&'a crate::catalog::IndexSchema> {
    const MIN_PREFIX_COLUMNS: usize = 2;
    let mut best: Option<(&crate::catalog::IndexSchema, usize)> = None;
    for index in catalog.indexes.values() {
        if !identifiers_equal(&index.table_name, &table.name)
            || index.kind != IndexKind::Btree
            || index.predicate_sql.is_some()
            || !index.fresh
            || index.columns.len() < MIN_PREFIX_COLUMNS
        {
            continue;
        }
        let Some(matched_prefix_len) = compound_index_prefix_len(filter, index, MIN_PREFIX_COLUMNS)
        else {
            continue;
        };
        if best.is_none_or(|(_, current)| matched_prefix_len > current) {
            best = Some((index, matched_prefix_len));
        }
    }
    best.map(|(index, _)| index)
}

fn compound_index_prefix_len(
    filter: &Expr,
    index: &crate::catalog::IndexSchema,
    min_prefix_len: usize,
) -> Option<usize> {
    let mut indexable_columns: Vec<&str> = Vec::new();
    for filter_expr in flattened_and_predicates(filter) {
        let Some(column) = simple_indexable_equality_filter(filter_expr) else {
            continue;
        };
        if indexable_columns
            .iter()
            .any(|candidate| identifiers_equal(candidate, column))
        {
            continue;
        }
        indexable_columns.push(column);
    }
    let mut matched_prefix_len = 0;
    for indexed_column in index.columns.iter() {
        let Some(indexed) = indexed_column.column_name.as_deref() else {
            break;
        };
        if !indexable_columns
            .iter()
            .any(|filter_column| identifiers_equal(filter_column, indexed))
        {
            break;
        }
        matched_prefix_len += 1;
    }
    (matched_prefix_len >= min_prefix_len).then_some(matched_prefix_len)
}

fn flattened_and_predicates(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let mut predicates = flattened_and_predicates(left);
            predicates.extend(flattened_and_predicates(right));
            predicates
        }
        _ => vec![expr],
    }
}

fn select_projection_is_covered_by_index(
    select: &Select,
    table: &TableSchema,
    index: &crate::catalog::IndexSchema,
) -> bool {
    if index.kind != IndexKind::Btree
        || !index.fresh
        || index.predicate_sql.is_some()
        || index.include_columns.is_empty()
        || !generated_columns_are_stored_for_planner(table)
    {
        return false;
    }
    let Some(covered_columns) = planner_covering_index_columns(index, table) else {
        return false;
    };
    let Some(FromItem::Table { name, alias }) = select.from.first() else {
        return false;
    };
    let binding_name = alias.as_deref().unwrap_or(name);
    select.projection.iter().all(|item| match item {
        SelectItem::Expr { expr, .. } => {
            let Expr::Column {
                table: qualifier,
                column,
            } = expr
            else {
                return false;
            };
            if qualifier.as_deref().is_some_and(|qualifier| {
                !identifiers_equal(qualifier, &table.name)
                    && !identifiers_equal(qualifier, binding_name)
            }) {
                return false;
            }
            covered_columns
                .iter()
                .any(|covered| identifiers_equal(covered, column))
        }
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    })
}

fn planner_covering_index_columns(
    index: &crate::catalog::IndexSchema,
    table: &TableSchema,
) -> Option<Vec<String>> {
    let mut columns: Vec<String> = Vec::new();
    for column in index
        .columns
        .iter()
        .map(|column| column.column_name.as_deref())
        .chain(
            index
                .include_columns
                .iter()
                .map(|column| Some(column.as_str())),
        )
    {
        let column = column?;
        if !table
            .columns
            .iter()
            .any(|candidate| identifiers_equal(&candidate.name, column))
        {
            return None;
        }
        if !columns
            .iter()
            .any(|existing| identifiers_equal(existing, column))
        {
            columns.push(column.to_string());
        }
    }
    Some(columns)
}

fn generated_columns_are_stored_for_planner(table: &TableSchema) -> bool {
    table
        .columns
        .iter()
        .all(|column| column.generated_sql.is_none() || column.generated_stored)
}

fn planner_row_id_alias_column_name(table: &TableSchema) -> Option<&str> {
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
        estimate: PlanEstimate::ZERO,
        input: Box::new(input.clone()),
    })
}

fn maybe_ordered_row_id_scan_plan(query: &Query, catalog: &CatalogState) -> Option<PhysicalPlan> {
    if !query.ctes.is_empty()
        || query.limit.is_none()
        || query.order_by.len() != 1
        || query.order_by[0].descending
    {
        return None;
    }
    let QueryBody::Select(select) = &query.body else {
        return None;
    };
    if select.filter.is_some()
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select.distinct
        || projection_has_aggregate(select)
        || select.from.len() != 1
    {
        return None;
    }
    let FromItem::Table { name, alias } = &select.from[0] else {
        return None;
    };
    let table = catalog.table(name)?;
    let Expr::Column {
        table: order_table,
        column: order_column,
    } = &query.order_by[0].expr
    else {
        return None;
    };
    if let Some(qualifier) = order_table.as_deref() {
        if !identifiers_equal(qualifier, name)
            && !alias
                .as_deref()
                .is_some_and(|binding| identifiers_equal(qualifier, binding))
        {
            return None;
        }
    }
    let column = table
        .columns
        .iter()
        .find(|column| identifiers_equal(&column.name, order_column))?;
    if column.column_type != crate::catalog::ColumnType::Int64
        || !table
            .primary_key_columns
            .iter()
            .any(|primary_key| identifiers_equal(primary_key, order_column))
    {
        return None;
    }
    Some(PhysicalPlan::Project {
        input: Box::new(PhysicalPlan::OrderedRowIdScan {
            table: table.name.clone(),
            column: column.name.clone(),
            estimate: PlanEstimate::ZERO,
        }),
        items: select.projection.clone(),
        estimate: PlanEstimate::ZERO,
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
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => expr_has_column_ref(expr),
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
    simple_indexable_equality_filter(filter)
        .map(|column| (column, false))
        .or_else(|| match filter {
            Expr::Like {
                expr: left,
                pattern: right,
                escape,
                negated,
                ..
            } => match (&**left, &**right) {
                (
                    Expr::Column { column, .. },
                    Expr::Literal(crate::record::value::Value::Text(_)),
                ) if !*negated && escape.is_none() => Some((column.as_str(), true)),
                _ => None,
            },
            _ => None,
        })
}

fn simple_indexable_equality_filter(filter: &Expr) -> Option<&str> {
    match filter {
        Expr::Binary { left, op, right } => match (&**left, op, &**right) {
            (Expr::Column { column, .. }, BinaryOp::Eq, Expr::Literal(_))
            | (Expr::Column { column, .. }, BinaryOp::Eq, Expr::Parameter(_))
            | (Expr::Literal(_), BinaryOp::Eq, Expr::Column { column, .. })
            | (Expr::Parameter(_), BinaryOp::Eq, Expr::Column { column, .. }) => {
                Some(column.as_str())
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
        Expr::Unary { expr, .. } | Expr::Collate { expr, .. } => expr_has_aggregate(expr),
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

fn annotate_plan(plan: PhysicalPlan, catalog: &CatalogState) -> PhysicalPlan {
    match plan {
        PhysicalPlan::TableScan { table, .. } => {
            let rows = estimate_table_rows(catalog, &table);
            PhysicalPlan::TableScan {
                table,
                estimate: PlanEstimate {
                    rows,
                    cost: scan_cost_u64(rows),
                },
            }
        }
        PhysicalPlan::IndexSeek {
            table,
            index,
            predicate,
            ..
        } => {
            let table_rows = estimate_table_rows(catalog, &table);
            let rows = estimate_filter_rows(table_rows, &predicate, catalog);
            PhysicalPlan::IndexSeek {
                table,
                index,
                predicate,
                estimate: PlanEstimate {
                    rows,
                    cost: 1.0 + scan_cost_u64(rows),
                },
            }
        }
        PhysicalPlan::CoveringIndexSeek {
            table,
            index,
            predicate,
            ..
        } => {
            let table_rows = estimate_table_rows(catalog, &table);
            let rows = estimate_filter_rows(table_rows, &predicate, catalog);
            PhysicalPlan::CoveringIndexSeek {
                table,
                index,
                predicate,
                estimate: PlanEstimate {
                    rows,
                    cost: scan_cost_u64(rows),
                },
            }
        }
        PhysicalPlan::RowIdLookup {
            table,
            column,
            predicate,
            ..
        } => {
            let table_rows = estimate_table_rows(catalog, &table);
            PhysicalPlan::RowIdLookup {
                table,
                column,
                predicate,
                estimate: PlanEstimate {
                    rows: 1,
                    cost: 1.0 + (table_rows.max(1) as f64).log2(),
                },
            }
        }
        PhysicalPlan::OrderedRowIdScan { table, column, .. } => {
            let rows = estimate_table_rows(catalog, &table);
            PhysicalPlan::OrderedRowIdScan {
                table,
                column,
                estimate: PlanEstimate {
                    rows,
                    cost: scan_cost_u64(rows),
                },
            }
        }
        PhysicalPlan::TrigramSearch {
            table,
            index,
            predicate,
            ..
        } => {
            let rows =
                estimate_filter_rows(estimate_table_rows(catalog, &table), &predicate, catalog);
            PhysicalPlan::TrigramSearch {
                table,
                index,
                predicate,
                estimate: PlanEstimate { rows, cost: 10.0 },
            }
        }
        PhysicalPlan::SpatialFilter {
            table,
            index,
            predicate,
            ..
        } => {
            let rows =
                estimate_filter_rows(estimate_table_rows(catalog, &table), &predicate, catalog);
            PhysicalPlan::SpatialFilter {
                table,
                index,
                predicate,
                estimate: PlanEstimate {
                    rows,
                    cost: 1.0 + scan_cost_u64(rows),
                },
            }
        }
        PhysicalPlan::SpatialKnn {
            table,
            index,
            order,
            input,
            ..
        } => {
            let input = Box::new(annotate_plan(*input, catalog));
            let estimate = input.estimate();
            PhysicalPlan::SpatialKnn {
                table,
                index,
                order,
                input,
                estimate,
            }
        }
        PhysicalPlan::SpatialJoin {
            table,
            index,
            predicate,
            input,
            ..
        } => {
            let input = Box::new(annotate_plan(*input, catalog));
            let estimate = input.estimate();
            PhysicalPlan::SpatialJoin {
                table,
                index,
                predicate,
                input,
                estimate,
            }
        }
        PhysicalPlan::Filter {
            input, predicate, ..
        } => {
            let input = Box::new(annotate_plan(*input, catalog));
            let input_estimate = input.estimate();
            let rows = ((input_estimate.rows as f64) * estimate_selectivity(&predicate, catalog))
                .max(1.0) as u64;
            PhysicalPlan::Filter {
                input,
                predicate,
                estimate: PlanEstimate {
                    rows,
                    cost: input_estimate.cost,
                },
            }
        }
        PhysicalPlan::Project { input, items, .. } => {
            let input = Box::new(annotate_plan(*input, catalog));
            let estimate = input.estimate();
            PhysicalPlan::Project {
                input,
                items,
                estimate,
            }
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            kind,
            constraint,
            ..
        } => {
            let left = Box::new(annotate_plan(*left, catalog));
            let right = Box::new(annotate_plan(*right, catalog));
            let left_estimate = left.estimate();
            let right_estimate = right.estimate();
            let selectivity = if kind == JoinKind::Inner {
                estimate_join_selectivity(&constraint, catalog)
            } else {
                1.0
            };
            let rows = (left_estimate.rows as f64 * right_estimate.rows as f64 * selectivity)
                .max(1.0) as u64;
            let cost = left_estimate.cost + left_estimate.rows as f64 * right_estimate.cost;
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                kind,
                constraint,
                estimate: PlanEstimate { rows, cost },
            }
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            kind,
            on,
            ..
        } => {
            let left = Box::new(annotate_plan(*left, catalog));
            let right = Box::new(annotate_plan(*right, catalog));
            let left_estimate = left.estimate();
            let right_estimate = right.estimate();
            let rows = (left_estimate.rows as f64
                * right_estimate.rows as f64
                * estimate_join_selectivity_constraint(&on, catalog))
            .max(1.0) as u64;
            let cost = left_estimate.cost + right_estimate.cost + left_estimate.rows as f64;
            PhysicalPlan::HashJoin {
                left,
                right,
                kind,
                on,
                estimate: PlanEstimate { rows, cost },
            }
        }
        PhysicalPlan::IndexedJoin {
            left,
            right,
            kind,
            on,
            index,
            ..
        } => {
            let left = Box::new(annotate_plan(*left, catalog));
            let right = Box::new(annotate_plan(*right, catalog));
            let left_estimate = left.estimate();
            let right_estimate = right.estimate();
            let rows = (left_estimate.rows as f64
                * right_estimate.rows as f64
                * estimate_join_selectivity_constraint(&on, catalog))
            .max(1.0) as u64;
            let probe_cost =
                estimate_join_probe_cost(right_estimate.rows.max(1), &index, catalog, &on);
            let cost = left_estimate.cost + (left_estimate.rows as f64) * probe_cost;
            PhysicalPlan::IndexedJoin {
                left,
                right,
                kind,
                on,
                index,
                estimate: PlanEstimate { rows, cost },
            }
        }
        PhysicalPlan::StreamingAggregate {
            input,
            group_by,
            having,
            ..
        } => {
            let input = Box::new(annotate_plan(*input, catalog));
            let input_estimate = input.estimate();
            let rows = if group_by.is_empty() {
                1
            } else {
                input_estimate.rows.max(1)
            };
            let rows = rows.min(input_estimate.rows.max(1));
            PhysicalPlan::StreamingAggregate {
                input,
                group_by,
                having,
                estimate: PlanEstimate {
                    rows,
                    cost: input_estimate.cost,
                },
            }
        }
        PhysicalPlan::Sort {
            input, order_by, ..
        } => {
            let input = Box::new(annotate_plan(*input, catalog));
            let estimate = input.estimate();
            PhysicalPlan::Sort {
                input,
                order_by,
                estimate: PlanEstimate {
                    rows: estimate.rows,
                    cost: estimate.cost
                        + (estimate.rows as f64) * (estimate.rows.max(2) as f64).log2()
                            / PLANNER_ROWS_PER_PAGE,
                },
            }
        }
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
            ..
        } => {
            let input = Box::new(annotate_plan(*input, catalog));
            let input_estimate = input.estimate();
            let limit_rows = literal_usize_to_rows(limit.as_ref());
            let rows = if let Some(limit_rows) = limit_rows {
                input_estimate.rows.min(limit_rows)
            } else {
                input_estimate.rows
            };
            let offset_rows = literal_usize_to_rows(offset.as_ref());
            let rows = offset_rows.map_or(rows, |offset| rows.saturating_sub(offset));
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
                estimate: PlanEstimate {
                    rows,
                    cost: if input_estimate.rows == 0 {
                        0.0
                    } else {
                        (rows as f64) / (input_estimate.rows as f64) * input_estimate.cost
                    },
                },
            }
        }
        PhysicalPlan::SetOp {
            op,
            all,
            left,
            right,
            ..
        } => {
            let left = Box::new(annotate_plan(*left, catalog));
            let right = Box::new(annotate_plan(*right, catalog));
            let left_estimate = left.estimate();
            let right_estimate = right.estimate();
            let rows = left_estimate.rows.saturating_add(right_estimate.rows);
            PhysicalPlan::SetOp {
                op,
                all,
                left,
                right,
                estimate: PlanEstimate {
                    rows,
                    cost: left_estimate.cost + right_estimate.cost,
                },
            }
        }
        PhysicalPlan::ViewScan { name, .. } => {
            let rows = PLANNER_TABLE_ROWS_HEURISTIC;
            PhysicalPlan::ViewScan {
                name,
                estimate: PlanEstimate {
                    rows,
                    cost: scan_cost_u64(rows),
                },
            }
        }
        PhysicalPlan::ExpandedView {
            name,
            input,
            pushed_filter,
            pushed_projection,
            pushed_limit,
            ..
        } => {
            let input = Box::new(annotate_plan(*input, catalog));
            let estimate = input.estimate();
            PhysicalPlan::ExpandedView {
                name,
                input,
                pushed_filter,
                pushed_projection,
                pushed_limit,
                estimate,
            }
        }
        PhysicalPlan::Empty { .. } => PhysicalPlan::Empty {
            estimate: PlanEstimate::ZERO,
        },
    }
}

fn estimate_table_rows(catalog: &CatalogState, table: &str) -> u64 {
    catalog
        .table_stats
        .get(table)
        .map_or(PLANNER_TABLE_ROWS_HEURISTIC, |stats| {
            stats.row_count.max(0) as u64
        })
}

fn scan_cost_u64(rows: u64) -> f64 {
    (rows as f64 / PLANNER_ROWS_PER_PAGE).max(1.0)
}

fn estimate_filter_rows(row_count: u64, filter: &Expr, catalog: &CatalogState) -> u64 {
    ((row_count as f64) * estimate_selectivity(filter, catalog)).max(1.0) as u64
}

fn estimate_selectivity(expr: &Expr, catalog: &CatalogState) -> f64 {
    let _ = catalog;
    match expr {
        Expr::Binary { left, op, right } => match op {
            BinaryOp::And => {
                estimate_selectivity(left, catalog) * estimate_selectivity(right, catalog)
            }
            BinaryOp::Or => {
                let left = estimate_selectivity(left, catalog);
                let right = estimate_selectivity(right, catalog);
                (left + right - (left * right)).min(1.0)
            }
            BinaryOp::Eq => {
                let index_selectivity = estimate_eq_selectivity_with_expr(left, right);
                index_selectivity.clamp(PLANNER_EQ_SELECTIVITY_WITHOUT_STATS, 1.0)
            }
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                PLANNER_RANGE_SELECTIVITY
            }
            BinaryOp::NotEq => 1.0 - PLANNER_EQ_SELECTIVITY_WITHOUT_STATS,
            _ => 1.0,
        },
        Expr::Like {
            expr: _left,
            pattern: right,
            negated,
            ..
        } => {
            if !negated && matches!(right.as_ref(), Expr::Literal(Value::Text(_))) {
                PLANNER_LIKE_SELECTIVITY
            } else {
                1.0
            }
        }
        Expr::Between { .. } => PLANNER_RANGE_SELECTIVITY,
        Expr::InSubquery { .. } => PLANNER_RANGE_SELECTIVITY,
        Expr::CompareSubquery { .. } => PLANNER_RANGE_SELECTIVITY,
        Expr::Function { .. } => PLANNER_LIKE_SELECTIVITY,
        Expr::ScalarSubquery(_) | Expr::Exists(_) => 1.0,
        Expr::Collate { expr, .. } => estimate_selectivity(expr, catalog),
        Expr::Cast { expr, .. } => estimate_selectivity(expr, catalog),
        Expr::Unary { expr, .. } => estimate_selectivity(expr, catalog),
        Expr::IsNull { expr, .. } => estimate_selectivity(expr, catalog),
        Expr::InList { .. } => PLANNER_EQ_SELECTIVITY_WITHOUT_STATS,
        Expr::Case { .. } => PLANNER_RANGE_SELECTIVITY,
        Expr::Row(_) => PLANNER_RANGE_SELECTIVITY,
        Expr::WindowFunction { .. } => 1.0,
        Expr::Aggregate { .. } => 1.0,
        Expr::RowNumber { .. } => 1.0,
        Expr::Literal(_) | Expr::Parameter(_) | Expr::Column { .. } => 1.0,
    }
}

fn estimate_eq_selectivity_with_expr(left: &Expr, right: &Expr) -> f64 {
    let has_column = matches!(
        (left, right),
        (Expr::Column { .. }, Expr::Literal(_))
            | (Expr::Column { .. }, Expr::Parameter(_))
            | (Expr::Literal(_), Expr::Column { .. })
            | (Expr::Parameter(_), Expr::Column { .. })
    );
    if has_column {
        PLANNER_EQ_SELECTIVITY_WITHOUT_STATS
    } else {
        1.0
    }
}

fn estimate_join_selectivity(on_constraint: &JoinConstraint, catalog: &CatalogState) -> f64 {
    match on_constraint {
        JoinConstraint::On(expr) => estimate_join_selectivity_constraint(expr, catalog),
        JoinConstraint::Using(_) | JoinConstraint::Natural => 1.0,
    }
}

fn estimate_join_selectivity_constraint(expr: &Expr, _catalog: &CatalogState) -> f64 {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right: _right,
        } => {
            let left_is_col = matches!(left.as_ref(), Expr::Column { .. });
            if left_is_col {
                PLANNER_EQ_SELECTIVITY_WITH_STATS
            } else {
                1.0
            }
        }
        Expr::Binary { left, op, right } => match op {
            BinaryOp::And => {
                estimate_join_selectivity_constraint(left, _catalog)
                    * estimate_join_selectivity_constraint(right, _catalog)
            }
            BinaryOp::Or => {
                let left = estimate_join_selectivity_constraint(left, _catalog);
                let right = estimate_join_selectivity_constraint(right, _catalog);
                (left + right - left * right).min(1.0)
            }
            _ => 1.0,
        },
        _ => 1.0,
    }
}

fn estimate_join_probe_cost(
    right_rows: u64,
    _index: &str,
    _catalog: &CatalogState,
    _predicate: &Expr,
) -> f64 {
    (right_rows as f64).log2().max(1.0)
}

fn literal_usize_to_rows(expr: Option<&Expr>) -> Option<u64> {
    let expr = expr?;
    match expr {
        Expr::Literal(Value::Int64(value)) => u64::try_from(*value).ok(),
        Expr::Literal(Value::Decimal { scaled, scale: _ }) => u64::try_from(*scaled).ok(),
        _ => None,
    }
}
fn maybe_expand_view(name: &str, catalog: &CatalogState) -> Result<Option<PhysicalPlan>> {
    let Some(view) = catalog.view(name) else {
        return Ok(None);
    };
    let view_statement = parse_sql_statement(&view.sql_text)?;
    let Statement::Query(view_query) = view_statement else {
        return Ok(None);
    };
    if view_query.recursive
        || !view_query.ctes.is_empty()
        || !view_query.order_by.is_empty()
        || view_query.limit.is_some()
        || view_query.offset.is_some()
    {
        return Ok(None);
    }
    let QueryBody::Select(view_select) = &view_query.body else {
        return Ok(None);
    };
    if view_select.distinct
        || !view_select.distinct_on.is_empty()
        || !view_select.group_by.is_empty()
        || view_select.having.is_some()
        || projection_has_aggregate_items(&view_select.projection)
    {
        return Ok(None);
    }
    let inner_plan = plan_select(view_select, catalog)?;
    Ok(Some(PhysicalPlan::ExpandedView {
        name: view.name.clone(),
        input: Box::new(inner_plan),
        pushed_filter: view_select.filter.is_some(),
        pushed_projection: false,
        pushed_limit: false,
        estimate: PlanEstimate::ZERO,
    }))
}

fn projection_has_aggregate_items(items: &[SelectItem]) -> bool {
    items.iter().any(select_item_has_aggregate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnSchema, ColumnType, IndexColumn, IndexSchema, ViewSchema};
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

    fn test_column(name: &str, auto_increment: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            column_type: ColumnType::Int64,
            spatial_type: None,
            enum_type: None,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: auto_increment,
            unique: auto_increment,
            auto_increment,
            checks: Vec::new(),
            foreign_key: None,
        }
    }

    fn catalog_with_artist_table() -> CatalogState {
        let mut catalog = CatalogState::empty(0);
        catalog.tables.insert(
            "Artist".to_string(),
            TableSchema {
                name: "Artist".to_string(),
                temporary: false,
                columns: vec![
                    test_column("Id", true),
                    test_column("NameNormalized", false),
                ],
                checks: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_columns: vec!["Id".to_string()],
                next_row_id: 1,
                pk_index_root: None,
            },
        );
        catalog.indexes.insert(
            "IX_Artist_NameNormalized".to_string(),
            IndexSchema {
                name: "IX_Artist_NameNormalized".to_string(),
                table_name: "Artist".to_string(),
                kind: IndexKind::Btree,
                unique: false,
                columns: vec![IndexColumn {
                    column_name: Some("NameNormalized".to_string()),
                    expression_sql: None,
                }],
                include_columns: Vec::new(),
                predicate_sql: None,
                full_text: None,
                fresh: true,
            },
        );
        catalog
    }

    fn catalog_with_issue_table() -> CatalogState {
        let mut catalog = CatalogState::empty(0);
        catalog.tables.insert(
            "issues".to_string(),
            TableSchema {
                name: "issues".to_string(),
                temporary: false,
                columns: vec![
                    test_column("project_id", false),
                    test_column("status", false),
                    test_column("id", true),
                ],
                checks: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_columns: vec!["id".to_string()],
                next_row_id: 1,
                pk_index_root: None,
            },
        );
        catalog.indexes.insert(
            "idx_issues_project_status".to_string(),
            IndexSchema {
                name: "idx_issues_project_status".to_string(),
                table_name: "issues".to_string(),
                kind: IndexKind::Btree,
                unique: false,
                columns: vec![
                    IndexColumn {
                        column_name: Some("project_id".to_string()),
                        expression_sql: None,
                    },
                    IndexColumn {
                        column_name: Some("status".to_string()),
                        expression_sql: None,
                    },
                ],
                include_columns: Vec::new(),
                predicate_sql: None,
                full_text: None,
                fresh: true,
            },
        );
        catalog
    }

    fn catalog_with_artist_view() -> CatalogState {
        let mut catalog = catalog_with_artist_table();
        catalog.views.insert(
            "v_artist".to_string(),
            ViewSchema {
                name: "v_artist".to_string(),
                temporary: false,
                sql_text: "SELECT Id, NameNormalized FROM Artist".to_string(),
                column_names: vec!["Id".to_string(), "NameNormalized".to_string()],
                dependencies: vec!["Artist".to_string()],
            },
        );
        catalog
    }

    fn catalog_with_filtered_artist_view() -> CatalogState {
        let mut catalog = catalog_with_artist_table();
        catalog.views.insert(
            "v_filtered_artist".to_string(),
            ViewSchema {
                name: "v_filtered_artist".to_string(),
                temporary: false,
                sql_text:
                    "SELECT Id, NameNormalized FROM Artist WHERE NameNormalized = 'MOTLEYCRUE'"
                        .to_string(),
                column_names: vec!["Id".to_string(), "NameNormalized".to_string()],
                dependencies: vec!["Artist".to_string()],
            },
        );
        catalog
    }

    fn single_table_select(filter: Expr) -> Select {
        Select {
            distinct: false,
            distinct_on: vec![],
            projection: vec![SelectItem::Wildcard],
            from: vec![FromItem::Table {
                name: "Artist".to_string(),
                alias: None,
            }],
            filter: Some(filter),
            group_by: vec![],
            having: None,
        }
    }

    fn single_issues_select(filter: Expr) -> Select {
        Select {
            distinct: false,
            distinct_on: vec![],
            projection: vec![SelectItem::Wildcard],
            from: vec![FromItem::Table {
                name: "issues".to_string(),
                alias: None,
            }],
            filter: Some(filter),
            group_by: vec![],
            having: None,
        }
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

    #[test]
    fn explain_plan_renders_expanded_view_for_simple_view_query() {
        let catalog = catalog_with_artist_view();
        let statement = parse_sql_statement("SELECT Id FROM v_artist LIMIT 10").expect("parse");

        let lines = plan_statement(&statement, &catalog).expect("plan").render();

        assert!(
            lines
                .iter()
                .any(|line| line.contains(
                    "ExpandedView(name=v_artist, pushedFilter=false, pushedProjection=true, pushedLimit=true"
                )),
            "expected rendered plan to expose pushed projection and limit on ExpandedView, got: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("TableScan(table=artist")),
            "expected rendered expanded view plan to scan the base table, got: {lines:?}"
        );
    }

    #[test]
    fn explain_plan_marks_simple_view_filter_projection_pushdown() {
        let catalog = catalog_with_artist_view();
        let statement = parse_sql_statement(
            "SELECT Id FROM v_artist WHERE NameNormalized = 'MOTLEYCRUE' LIMIT 10",
        )
        .expect("parse");

        let lines = plan_statement(&statement, &catalog).expect("plan").render();

        assert!(
            lines
                .iter()
                .any(|line| line.contains(
                    "ExpandedView(name=v_artist, pushedFilter=true, pushedProjection=true, pushedLimit=false"
                )),
            "expected rendered plan to expose pushed filter/projection and avoid unsafe limit pushdown, got: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Filter((namenormalized = 'MOTLEYCRUE')")),
            "expected rendered plan to retain visible outer filter, got: {lines:?}"
        );
    }

    #[test]
    fn explain_plan_expands_filtered_view_for_ordered_limit_query() {
        let catalog = catalog_with_filtered_artist_view();
        let statement = parse_sql_statement("SELECT Id FROM v_filtered_artist ORDER BY Id LIMIT 5")
            .expect("parse");

        let lines = plan_statement(&statement, &catalog).expect("plan").render();

        assert!(
            lines.iter().any(|line| line.contains(
                "ExpandedView(name=v_filtered_artist, pushedFilter=true, pushedProjection=true, pushedLimit=false"
            )),
            "expected filtered view expansion with projection pushdown, got: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains("predicate=(namenormalized = 'MOTLEYCRUE')")),
            "expected expanded filtered view to retain the view predicate in an indexed or filtered path, got: {lines:?}"
        );
    }

    fn catalog_with_two_table_join(indexed: bool, with_stats: bool) -> CatalogState {
        let mut catalog = CatalogState::empty(0);
        catalog.tables.insert(
            "artists".to_string(),
            TableSchema {
                name: "artists".to_string(),
                temporary: false,
                columns: vec![test_column("id", true), test_column("name", false)],
                checks: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_columns: vec!["id".to_string()],
                next_row_id: 1,
                pk_index_root: None,
            },
        );
        catalog.tables.insert(
            "albums".to_string(),
            TableSchema {
                name: "albums".to_string(),
                temporary: false,
                columns: vec![test_column("id", true), test_column("artist_id", false)],
                checks: Vec::new(),
                foreign_keys: Vec::new(),
                primary_key_columns: vec!["id".to_string()],
                next_row_id: 1,
                pk_index_root: None,
            },
        );
        if indexed {
            catalog.indexes.insert(
                "idx_albums_artist".to_string(),
                IndexSchema {
                    name: "idx_albums_artist".to_string(),
                    table_name: "albums".to_string(),
                    kind: IndexKind::Btree,
                    unique: false,
                    columns: vec![IndexColumn {
                        column_name: Some("artist_id".to_string()),
                        expression_sql: None,
                    }],
                    include_columns: Vec::new(),
                    predicate_sql: None,
                    full_text: None,
                    fresh: true,
                },
            );
        }
        if with_stats {
            catalog.table_stats.insert(
                "artists".to_string(),
                crate::catalog::TableStats { row_count: 10_000 },
            );
            catalog.table_stats.insert(
                "albums".to_string(),
                crate::catalog::TableStats { row_count: 50_000 },
            );
            catalog.index_stats.insert(
                "idx_albums_artist".to_string(),
                crate::catalog::IndexStats {
                    entry_count: 50_000,
                    distinct_key_count: 10_000,
                },
            );
        }
        catalog
    }

    #[test]
    fn explain_plan_chooses_indexed_join_when_useful_index_exists() {
        let catalog = catalog_with_two_table_join(true, true);
        let statement = parse_sql_statement(
            "SELECT artists.name FROM artists JOIN albums ON albums.artist_id = artists.id",
        )
        .expect("parse");

        let lines = plan_statement(&statement, &catalog).expect("plan").render();
        let rendered = lines.join("\n");
        assert!(
            rendered.contains("IndexedJoin("),
            "expected an indexed join when a useful join index exists, got: {lines:?}"
        );
        assert!(
            rendered.contains("estRows=") && rendered.contains("estCost="),
            "expected estimated rows and cost on the join plan, got: {lines:?}"
        );
    }

    #[test]
    fn explain_plan_chooses_hash_join_without_useful_index() {
        let catalog = catalog_with_two_table_join(false, true);
        let statement = parse_sql_statement(
            "SELECT artists.name FROM artists JOIN albums ON albums.artist_id = artists.id",
        )
        .expect("parse");

        let lines = plan_statement(&statement, &catalog).expect("plan").render();
        let rendered = lines.join("\n");
        assert!(
            !rendered.contains("IndexedJoin("),
            "expected no indexed join when no useful index exists, got: {lines:?}"
        );
        assert!(
            rendered.contains("HashJoin("),
            "expected a hash join for an equi-join without a useful index, got: {lines:?}"
        );
    }

    #[test]
    fn explain_plan_exposes_estimates_on_operators() {
        let catalog = catalog_with_two_table_join(true, true);
        let statement =
            parse_sql_statement("SELECT name FROM artists WHERE id = 5").expect("parse");
        let lines = plan_statement(&statement, &catalog).expect("plan").render();
        let rendered = lines.join("\n");
        assert!(
            rendered.contains("estRows=") && rendered.contains("estCost="),
            "expected EXPLAIN to surface estRows and estCost, got: {lines:?}"
        );
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
    fn indexable_eq_column_parameter() {
        let filter = Expr::Binary {
            left: Box::new(col("id")),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Parameter(1)),
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
    fn not_indexable_negated_like() {
        let filter = Expr::Like {
            expr: Box::new(col("name")),
            pattern: Box::new(lit_text("%hello%")),
            escape: None,
            case_insensitive: false,
            negated: true,
        };
        assert!(simple_indexable_filter(&filter).is_none());
    }

    #[test]
    fn not_indexable_like_with_escape() {
        let filter = Expr::Like {
            expr: Box::new(col("name")),
            pattern: Box::new(lit_text("%hello/%")),
            escape: Some(Box::new(lit_text("/"))),
            case_insensitive: false,
            negated: false,
        };
        assert!(simple_indexable_filter(&filter).is_none());
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

    // ── maybe_index_plan ─────────────────────────────────────────────

    #[test]
    fn row_id_lookup_plan_uses_identifier_equality() {
        let catalog = catalog_with_artist_table();
        let select = single_table_select(Expr::Binary {
            left: Box::new(col("id")),
            op: BinaryOp::Eq,
            right: Box::new(lit_int(42)),
        });
        let plan = maybe_index_plan(&select, select.filter.as_ref().unwrap(), &catalog)
            .expect("row-id lookup plan");
        let lines = plan.render();
        assert!(
            lines
                .iter()
                .any(|line| line.contains("RowIdLookup(table=Artist, column=Id")),
            "expected RowIdLookup, got: {lines:?}"
        );
    }

    #[test]
    fn btree_index_plan_matches_column_case_insensitively() {
        let catalog = catalog_with_artist_table();
        let select = single_table_select(Expr::Binary {
            left: Box::new(col("namenormalized")),
            op: BinaryOp::Eq,
            right: Box::new(lit_text("MOTLEYCRUE")),
        });
        let plan = maybe_index_plan(&select, select.filter.as_ref().unwrap(), &catalog)
            .expect("btree index plan");
        let lines = plan.render();
        assert!(
            lines
                .iter()
                .any(|line| line.contains("IndexSeek(table=Artist, index=IX_Artist_NameNormalized")),
            "expected IndexSeek, got: {lines:?}"
        );
    }

    #[test]
    fn btree_index_plan_uses_compound_prefix_index_for_two_eq_predicates() {
        let catalog = catalog_with_issue_table();
        let select = single_issues_select(Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(col("project_id")),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Parameter(1)),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(col("status")),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Parameter(2)),
            }),
        });
        let plan = maybe_index_plan(&select, select.filter.as_ref().unwrap(), &catalog)
            .expect("compound index plan");
        let lines = plan.render();
        assert!(
            lines.iter().any(
                |line| line.contains("IndexSeek(table=issues, index=idx_issues_project_status")
            ),
            "expected IndexSeek, got: {lines:?}"
        );
    }

    #[test]
    fn btree_index_plan_uses_compound_prefix_index_for_reversed_and_predicates() {
        let catalog = catalog_with_issue_table();
        let select = single_issues_select(Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(col("status")),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Parameter(2)),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(col("project_id")),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Parameter(1)),
            }),
        });
        let plan = maybe_index_plan(&select, select.filter.as_ref().unwrap(), &catalog)
            .expect("compound index plan");
        let lines = plan.render();
        assert!(
            lines.iter().any(
                |line| line.contains("IndexSeek(table=issues, index=idx_issues_project_status")
            ),
            "expected IndexSeek, got: {lines:?}"
        );
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
