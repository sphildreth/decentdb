//! Physical plan nodes and EXPLAIN rendering helpers.

use crate::sql::ast::{Expr, JoinConstraint, JoinKind, OrderBy, SelectItem, SetOperation};

/// Simple cardinality/cost estimate captured by the optimizer and surfaced by
/// `EXPLAIN`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct PlanEstimate {
    pub(crate) rows: u64,
    pub(crate) cost: f64,
}

impl PlanEstimate {
    pub(crate) const ZERO: Self = Self { rows: 0, cost: 0.0 };
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PhysicalPlan {
    TableScan {
        table: String,
        estimate: PlanEstimate,
    },
    IndexSeek {
        table: String,
        index: String,
        predicate: Expr,
        estimate: PlanEstimate,
    },
    CoveringIndexSeek {
        table: String,
        index: String,
        predicate: Expr,
        estimate: PlanEstimate,
    },
    RowIdLookup {
        table: String,
        column: String,
        predicate: Expr,
        estimate: PlanEstimate,
    },
    OrderedRowIdScan {
        table: String,
        column: String,
        estimate: PlanEstimate,
    },
    TrigramSearch {
        table: String,
        index: String,
        predicate: Expr,
        estimate: PlanEstimate,
    },
    SpatialFilter {
        table: String,
        index: String,
        predicate: Expr,
        estimate: PlanEstimate,
    },
    SpatialKnn {
        table: String,
        index: String,
        order: Expr,
        input: Box<PhysicalPlan>,
        estimate: PlanEstimate,
    },
    SpatialJoin {
        table: String,
        index: String,
        predicate: Expr,
        input: Box<PhysicalPlan>,
        estimate: PlanEstimate,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
        estimate: PlanEstimate,
    },
    Project {
        input: Box<PhysicalPlan>,
        items: Vec<SelectItem>,
        estimate: PlanEstimate,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        kind: JoinKind,
        constraint: JoinConstraint,
        estimate: PlanEstimate,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        kind: JoinKind,
        on: Expr,
        estimate: PlanEstimate,
    },
    IndexedJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        kind: JoinKind,
        on: Expr,
        index: String,
        estimate: PlanEstimate,
    },
    StreamingAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        estimate: PlanEstimate,
    },
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<OrderBy>,
        estimate: PlanEstimate,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<Expr>,
        offset: Option<Expr>,
        estimate: PlanEstimate,
    },
    SetOp {
        op: SetOperation,
        all: bool,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        estimate: PlanEstimate,
    },
    ViewScan {
        name: String,
        estimate: PlanEstimate,
    },
    ExpandedView {
        name: String,
        input: Box<PhysicalPlan>,
        pushed_filter: bool,
        pushed_projection: bool,
        pushed_limit: bool,
        estimate: PlanEstimate,
    },
    Empty {
        estimate: PlanEstimate,
    },
}

impl PhysicalPlan {
    #[must_use]
    pub(crate) fn estimate(&self) -> PlanEstimate {
        match self {
            Self::TableScan { estimate, .. } => *estimate,
            Self::IndexSeek { estimate, .. } => *estimate,
            Self::CoveringIndexSeek { estimate, .. } => *estimate,
            Self::RowIdLookup { estimate, .. } => *estimate,
            Self::OrderedRowIdScan { estimate, .. } => *estimate,
            Self::TrigramSearch { estimate, .. } => *estimate,
            Self::SpatialFilter { estimate, .. } => *estimate,
            Self::SpatialKnn { estimate, .. } => *estimate,
            Self::SpatialJoin { estimate, .. } => *estimate,
            Self::Filter { estimate, .. } => *estimate,
            Self::Project { estimate, .. } => *estimate,
            Self::NestedLoopJoin { estimate, .. } => *estimate,
            Self::HashJoin { estimate, .. } => *estimate,
            Self::IndexedJoin { estimate, .. } => *estimate,
            Self::StreamingAggregate { estimate, .. } => *estimate,
            Self::Sort { estimate, .. } => *estimate,
            Self::Limit { estimate, .. } => *estimate,
            Self::SetOp { estimate, .. } => *estimate,
            Self::ViewScan { estimate, .. } => *estimate,
            Self::ExpandedView { estimate, .. } => *estimate,
            Self::Empty { estimate, .. } => *estimate,
        }
    }

    #[must_use]
    pub(crate) fn render(&self) -> Vec<String> {
        let mut lines = Vec::new();
        self.render_into(0, &mut lines);
        lines
    }

    fn render_into(&self, depth: usize, output: &mut Vec<String>) {
        let indent = "  ".repeat(depth);
        match self {
            Self::TableScan { table, estimate } => {
                output.push(format!(
                    "{indent}TableScan(table={table}, estRows={}, estCost={:.3})",
                    estimate.rows, estimate.cost
                ));
            }
            Self::IndexSeek {
                table,
                index,
                predicate,
                estimate,
            } => output.push(format!(
                "{indent}IndexSeek(table={table}, index={index}, predicate={}, estRows={}, estCost={:.3})",
                predicate.to_sql(),
                estimate.rows,
                estimate.cost
            )),
            Self::CoveringIndexSeek {
                table,
                index,
                predicate,
                estimate,
            } => output.push(format!(
                "{indent}CoveringIndexSeek(table={table}, index={index}, predicate={}, estRows={}, estCost={:.3})",
                predicate.to_sql(),
                estimate.rows,
                estimate.cost
            )),
            Self::RowIdLookup {
                table,
                column,
                predicate,
                estimate,
            } => output.push(format!(
                "{indent}RowIdLookup(table={table}, column={column}, predicate={}, estRows={}, estCost={:.3})",
                predicate.to_sql(),
                estimate.rows,
                estimate.cost
            )),
            Self::OrderedRowIdScan {
                table,
                column,
                estimate,
            } => output.push(format!(
                "{indent}OrderedRowIdScan(table={table}, column={column}, estRows={}, estCost={:.3})",
                estimate.rows, estimate.cost
            )),
            Self::TrigramSearch {
                table,
                index,
                predicate,
                estimate,
            } => output.push(format!(
                "{indent}TrigramSearch(table={table}, index={index}, predicate={}, estRows={}, estCost={:.3})",
                predicate.to_sql(),
                estimate.rows,
                estimate.cost
            )),
            Self::SpatialFilter {
                table,
                index,
                predicate,
                estimate,
            } => output.push(format!(
                "{indent}SpatialFilter(table={table}, index={index}, predicate={}, estRows={}, estCost={:.3})",
                predicate.to_sql(),
                estimate.rows,
                estimate.cost
            )),
            Self::SpatialKnn {
                table,
                index,
                order,
                input,
                estimate,
            } => {
                output.push(format!(
                    "{indent}SpatialKnn(table={table}, index={index}, order={}, estRows={}, estCost={:.3})",
                    order.to_sql(),
                    estimate.rows,
                    estimate.cost
                ));
                input.render_into(depth + 1, output);
            }
            Self::SpatialJoin {
                table,
                index,
                predicate,
                input,
                estimate,
            } => {
                output.push(format!(
                    "{indent}SpatialJoin(table={table}, index={index}, predicate={}, estRows={}, estCost={:.3})",
                    predicate.to_sql(),
                    estimate.rows,
                    estimate.cost
                ));
                input.render_into(depth + 1, output);
            }
            Self::Filter {
                input,
                predicate,
                estimate,
            } => {
                output.push(format!(
                    "{indent}Filter({}, estRows={}, estCost={:.3})",
                    predicate.to_sql(),
                    estimate.rows,
                    estimate.cost
                ));
                input.render_into(depth + 1, output);
            }
            Self::Project {
                input,
                items,
                estimate,
            } => {
                output.push(format!(
                    "{indent}Project({} , estRows={}, estCost={:.3})",
                    items
                        .iter()
                        .map(SelectItem::to_sql)
                        .collect::<Vec<_>>()
                        .join(", "),
                    estimate.rows,
                    estimate.cost
                ));
                input.render_into(depth + 1, output);
            }
            Self::NestedLoopJoin {
                left,
                right,
                kind,
                constraint,
                estimate,
            } => {
                let constraint_sql = constraint.to_sql();
                output.push(format!(
                    "{indent}NestedLoopJoin(kind={kind:?}, constraint={constraint_sql}, estRows={}, estCost={:.3})",
                    estimate.rows,
                    estimate.cost
                ));
                left.render_into(depth + 1, output);
                right.render_into(depth + 1, output);
            }
            Self::HashJoin {
                left,
                right,
                kind,
                on,
                estimate,
            } => {
                output.push(format!(
                    "{indent}HashJoin(kind={kind:?}, on={}, estRows={}, estCost={:.3})",
                    on.to_sql(),
                    estimate.rows,
                    estimate.cost
                ));
                left.render_into(depth + 1, output);
                right.render_into(depth + 1, output);
            }
            Self::IndexedJoin {
                left,
                right,
                kind,
                on,
                index,
                estimate,
            } => {
                output.push(format!(
                    "{indent}IndexedJoin(kind={kind:?}, index={index}, on={}, estRows={}, estCost={:.3})",
                    on.to_sql(),
                    estimate.rows,
                    estimate.cost
                ));
                left.render_into(depth + 1, output);
                right.render_into(depth + 1, output);
            }
            Self::StreamingAggregate {
                input,
                group_by,
                having,
                estimate,
            } => {
                let groups = if group_by.is_empty() {
                    "global".to_string()
                } else {
                    group_by
                        .iter()
                        .map(Expr::to_sql)
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let suffix = having.as_ref().map_or(String::new(), |having| {
                    format!(", having={}", having.to_sql())
                });
                output.push(format!(
                    "{indent}StreamingAggregate(group_by={groups}{suffix}, estRows={}, estCost={:.3})",
                    estimate.rows, estimate.cost
                ));
                input.render_into(depth + 1, output);
            }
            Self::Sort {
                input,
                order_by,
                estimate,
            } => {
                output.push(format!(
                    "{indent}Sort({}, estRows={}, estCost={:.3})",
                    order_by
                        .iter()
                        .map(OrderBy::to_sql)
                        .collect::<Vec<_>>()
                        .join(", "),
                    estimate.rows,
                    estimate.cost
                ));
                input.render_into(depth + 1, output);
            }
            Self::Limit {
                input,
                limit,
                offset,
                estimate,
            } => {
                let limit_sql = limit.as_ref().map_or_else(|| "none".to_string(), Expr::to_sql);
                let offset_sql = offset.as_ref().map_or_else(|| "none".to_string(), Expr::to_sql);
                output.push(format!(
                    "{indent}Limit(limit={limit_sql}, offset={offset_sql}, estRows={}, estCost={:.3})",
                    estimate.rows,
                    estimate.cost
                ));
                input.render_into(depth + 1, output);
            }
            Self::SetOp {
                op,
                all,
                left,
                right,
                estimate,
            } => {
                output.push(format!(
                    "{indent}SetOp({op:?}, all={all}, estRows={}, estCost={:.3})",
                    estimate.rows, estimate.cost
                ));
                left.render_into(depth + 1, output);
                right.render_into(depth + 1, output);
            }
            Self::ViewScan { name, estimate } => {
                output.push(format!(
                    "{indent}ViewScan(name={name}, estRows={}, estCost={:.3})",
                    estimate.rows, estimate.cost
                ));
            }
            Self::ExpandedView {
                name,
                input,
                pushed_filter,
                pushed_projection,
                pushed_limit,
                estimate,
            } => {
                output.push(format!(
                    "{indent}ExpandedView(name={name}, pushedFilter={pushed_filter}, pushedProjection={pushed_projection}, pushedLimit={pushed_limit}, estRows={}, estCost={:.3})",
                    estimate.rows,
                    estimate.cost
                ));
                input.render_into(depth + 1, output);
            }
            Self::Empty { estimate } => output.push(format!(
                "{indent}Empty(estRows={}, estCost={:.3})",
                estimate.rows, estimate.cost
            )),
        }
    }
}
