//! Bind DML target aliases to the target table in the normalized SQL tree.

use super::ast::{Expr, FromItem, JoinConstraint, Query, QueryBody, Select, SelectItem};
use crate::catalog::identifiers_equal;

pub(super) struct TargetAlias<'a> {
    pub(super) alias: &'a str,
    pub(super) table: &'a str,
}

impl TargetAlias<'_> {
    pub(super) fn projection(&self, items: &mut [SelectItem]) {
        for item in items {
            match item {
                SelectItem::Expr { expr, .. } => self.expr(expr),
                SelectItem::QualifiedWildcard(table) if identifiers_equal(table, self.alias) => {
                    *table = self.table.to_string();
                }
                _ => {}
            }
        }
    }

    pub(super) fn expr(&self, expr: &mut Expr) {
        match expr {
            Expr::Column {
                table: Some(table), ..
            } if identifiers_equal(table, self.alias) => {
                *table = self.table.to_string();
            }
            Expr::Unary { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::IsNull { expr, .. } => self.expr(expr),
            Expr::Binary { left, right, .. } => {
                self.expr(left);
                self.expr(right);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr(expr);
                self.expr(low);
                self.expr(high);
            }
            Expr::InList { expr, items, .. } => {
                self.expr(expr);
                for item in items {
                    self.expr(item);
                }
            }
            Expr::InSubquery { expr, query, .. } | Expr::CompareSubquery { expr, query, .. } => {
                self.expr(expr);
                self.query(query);
            }
            Expr::ScalarSubquery(query) | Expr::Exists(query) => self.query(query),
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.expr(expr);
                self.expr(pattern);
                if let Some(escape) = escape {
                    self.expr(escape);
                }
            }
            Expr::Function { args, .. } | Expr::Row(args) => {
                for arg in args {
                    self.expr(arg);
                }
            }
            Expr::Aggregate { args, order_by, .. } => {
                for arg in args {
                    self.expr(arg);
                }
                for order in order_by {
                    self.expr(&mut order.expr);
                }
            }
            Expr::RowNumber {
                partition_by,
                order_by,
                frame,
            }
            | Expr::WindowFunction {
                partition_by,
                order_by,
                frame,
                ..
            } => {
                for expr in partition_by {
                    self.expr(expr);
                }
                for order in order_by {
                    self.expr(&mut order.expr);
                }
                if let Some(frame) = frame {
                    use super::ast::WindowFrameBound;
                    for bound in std::iter::once(&mut frame.start).chain(frame.end.as_mut()) {
                        if let WindowFrameBound::Preceding(expr)
                        | WindowFrameBound::Following(expr) = bound
                        {
                            self.expr(expr);
                        }
                    }
                }
                if let Expr::WindowFunction { args, .. } = expr {
                    for arg in args {
                        self.expr(arg);
                    }
                }
            }
            Expr::Case {
                operand,
                branches,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    self.expr(operand);
                }
                for (condition, result) in branches {
                    self.expr(condition);
                    self.expr(result);
                }
                if let Some(expr) = else_expr {
                    self.expr(expr);
                }
            }
            Expr::Default | Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter(_) => {}
        }
    }

    fn query(&self, query: &mut Query) {
        if let QueryBody::Select(select) = &mut query.body {
            if let Some(local_alias) = self.protect_target_binding(select) {
                let local_binding = TargetAlias {
                    alias: self.table,
                    table: &local_alias,
                };
                for order in &mut query.order_by {
                    local_binding.expr(&mut order.expr);
                }
            }
        }
        // Each subquery introduces its own table bindings. A local alias hides
        // the DML target alias; correlated references otherwise retain it.
        for cte in &mut query.ctes {
            self.query(&mut cte.query);
        }
        if !self.body_shadows(&query.body) {
            for order in &mut query.order_by {
                self.expr(&mut order.expr);
            }
            if let Some(limit) = &mut query.limit {
                self.expr(limit);
            }
            if let Some(offset) = &mut query.offset {
                self.expr(offset);
            }
        }
        self.body(&mut query.body);
    }

    fn body_shadows(&self, body: &QueryBody) -> bool {
        match body {
            QueryBody::Select(select) => select.from.iter().any(|item| self.binding_shadows(item)),
            QueryBody::Values(_) => false,
            QueryBody::SetOperation { left, right, .. } => {
                self.body_shadows(left) || self.body_shadows(right)
            }
        }
    }

    fn binding_shadows(&self, item: &FromItem) -> bool {
        match item {
            FromItem::Table { name, alias } | FromItem::Function { name, alias, .. } => {
                identifiers_equal(alias.as_deref().unwrap_or(name), self.alias)
            }
            FromItem::Subquery { alias, .. } => identifiers_equal(alias, self.alias),
            FromItem::Join { left, right, .. } => {
                self.binding_shadows(left) || self.binding_shadows(right)
            }
        }
    }

    fn body(&self, body: &mut QueryBody) {
        match body {
            QueryBody::Select(select) => {
                if select.from.iter().any(|item| self.binding_shadows(item)) {
                    return;
                }
                self.protect_target_binding(select);
                self.select_expressions(select);
            }
            QueryBody::Values(rows) => {
                for row in rows {
                    for expr in row {
                        self.expr(expr);
                    }
                }
            }
            QueryBody::SetOperation { left, right, .. } => {
                self.body(left);
                self.body(right);
            }
        }
    }

    fn select_expressions(&self, select: &mut Select) {
        self.projection(&mut select.projection);
        if let Some(filter) = &mut select.filter {
            self.expr(filter);
        }
        if let Some(having) = &mut select.having {
            self.expr(having);
        }
        for expr in select.group_by.iter_mut().chain(&mut select.distinct_on) {
            self.expr(expr);
        }
        for item in &mut select.from {
            self.resolve_from(item);
        }
    }

    /// Alpha-rename a local binding that would capture the canonical target
    /// qualifier. The relation name is unchanged; this only binds AST names.
    fn protect_target_binding(&self, select: &mut Select) -> Option<String> {
        if identifiers_equal(self.alias, self.table)
            || select.from.iter().any(|item| self.binding_shadows(item))
        {
            return None;
        }
        let local_binding = TargetAlias {
            alias: self.table,
            table: self.table,
        };
        if !select
            .from
            .iter()
            .any(|item| local_binding.binding_shadows(item))
        {
            return None;
        }
        // Include nested references when choosing a fresh local name, so the
        // rename cannot capture any existing qualifier. No SQL is rewritten.
        let names = select.to_sql().to_ascii_lowercase();
        let mut ordinal = 0;
        let fresh = loop {
            let candidate = format!("__ddb_dml_local_{ordinal}");
            if !names.contains(&candidate) && !identifiers_equal(&candidate, self.alias) {
                break candidate;
            }
            ordinal += 1;
        };
        let renamed = TargetAlias {
            alias: self.table,
            table: &fresh,
        };
        renamed.select_expressions(select);
        for item in &mut select.from {
            Self::rename_binding(item, self.table, &fresh);
        }
        Some(fresh)
    }

    fn rename_binding(item: &mut FromItem, old: &str, new: &str) {
        match item {
            FromItem::Table { name, alias } | FromItem::Function { name, alias, .. } => {
                if identifiers_equal(alias.as_deref().unwrap_or(name), old) {
                    *alias = Some(new.to_string());
                }
            }
            FromItem::Subquery { alias, .. } => {
                if identifiers_equal(alias, old) {
                    *alias = new.to_string();
                }
            }
            FromItem::Join { left, right, .. } => {
                Self::rename_binding(left, old, new);
                Self::rename_binding(right, old, new);
            }
        }
    }

    fn resolve_from(&self, item: &mut FromItem) {
        match item {
            FromItem::Subquery { query, .. } => self.query(query),
            FromItem::Function { args, .. } => {
                for arg in args {
                    self.expr(arg);
                }
            }
            FromItem::Join {
                left,
                right,
                constraint,
                ..
            } => {
                self.resolve_from(left);
                self.resolve_from(right);
                if let JoinConstraint::On(expr) = constraint {
                    self.expr(expr);
                }
            }
            FromItem::Table { .. } => {}
        }
    }
}
