//! Logical plan nodes for the relational executor.

use crate::sql::ast::{Expr, JoinKind, OrderBy, Query, SelectItem, SetOperation};

#[allow(dead_code, clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LogicalPlan {
    TableScan {
        table: String,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        items: Vec<SelectItem>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        kind: JoinKind,
        on: Expr,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderBy>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    SetOp {
        op: SetOperation,
        all: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    Query(Query),
}
