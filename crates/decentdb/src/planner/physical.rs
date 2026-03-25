//! Physical plan nodes and EXPLAIN rendering helpers.

use crate::sql::ast::{Expr, JoinConstraint, JoinKind, OrderBy, SelectItem, SetOperation};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PhysicalPlan {
    TableScan {
        table: String,
    },
    IndexSeek {
        table: String,
        index: String,
        predicate: Expr,
    },
    TrigramSearch {
        table: String,
        index: String,
        predicate: Expr,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan>,
        items: Vec<SelectItem>,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        kind: JoinKind,
        constraint: JoinConstraint,
    },
    Aggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<OrderBy>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    SetOp {
        op: SetOperation,
        all: bool,
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },
    Empty,
}

impl PhysicalPlan {
    #[must_use]
    pub(crate) fn render(&self) -> Vec<String> {
        let mut lines = Vec::new();
        self.render_into(0, &mut lines);
        lines
    }

    fn render_into(&self, depth: usize, output: &mut Vec<String>) {
        let indent = "  ".repeat(depth);
        match self {
            Self::TableScan { table } => output.push(format!("{indent}TableScan(table={table})")),
            Self::IndexSeek {
                table,
                index,
                predicate,
            } => output.push(format!(
                "{indent}IndexSeek(table={table}, index={index}, predicate={})",
                predicate.to_sql()
            )),
            Self::TrigramSearch {
                table,
                index,
                predicate,
            } => output.push(format!(
                "{indent}TrigramSearch(table={table}, index={index}, predicate={})",
                predicate.to_sql()
            )),
            Self::Filter { input, predicate } => {
                output.push(format!("{indent}Filter({})", predicate.to_sql()));
                input.render_into(depth + 1, output);
            }
            Self::Project { input, items } => {
                output.push(format!(
                    "{indent}Project({})",
                    items
                        .iter()
                        .map(SelectItem::to_sql)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                input.render_into(depth + 1, output);
            }
            Self::NestedLoopJoin {
                left,
                right,
                kind,
                constraint,
            } => {
                output.push(format!(
                    "{indent}NestedLoopJoin(kind={:?}, {})",
                    kind,
                    constraint.to_sql()
                ));
                left.render_into(depth + 1, output);
                right.render_into(depth + 1, output);
            }
            Self::Aggregate {
                input,
                group_by,
                having,
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
                output.push(format!("{indent}Aggregate(group_by={groups}{suffix})"));
                input.render_into(depth + 1, output);
            }
            Self::Sort { input, order_by } => {
                output.push(format!(
                    "{indent}Sort({})",
                    order_by
                        .iter()
                        .map(OrderBy::to_sql)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                input.render_into(depth + 1, output);
            }
            Self::Limit {
                input,
                limit,
                offset,
            } => {
                let limit_sql = limit
                    .as_ref()
                    .map_or_else(|| "none".to_string(), Expr::to_sql);
                let offset_sql = offset
                    .as_ref()
                    .map_or_else(|| "none".to_string(), Expr::to_sql);
                output.push(format!(
                    "{indent}Limit(limit={limit_sql}, offset={offset_sql})"
                ));
                input.render_into(depth + 1, output);
            }
            Self::SetOp {
                op,
                all,
                left,
                right,
            } => {
                output.push(format!("{indent}SetOp({op:?}, all={all})"));
                left.render_into(depth + 1, output);
                right.render_into(depth + 1, output);
            }
            Self::Empty => output.push(format!("{indent}Empty")),
        }
    }
}
