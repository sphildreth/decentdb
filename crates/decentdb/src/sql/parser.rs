//! Parser wrapper that normalizes supported SQL into the engine AST.

use crate::error::{DbError, Result};

use super::ast::{Expr, SelectItem, Statement};
use super::normalize::normalize_statement_text;

pub(crate) fn parse_sql_statement(sql: &str) -> Result<Statement> {
    let mut statements = parse_sql_batch(sql)?;
    if statements.len() != 1 {
        return Err(DbError::sql(format!(
            "expected exactly one SQL statement, got {}",
            statements.len()
        )));
    }
    Ok(statements.remove(0))
}

pub(crate) fn parse_sql_batch(sql: &str) -> Result<Vec<Statement>> {
    let statements = libpg_query_sys::split_statements(sql)
        .map_err(|error| DbError::sql(error.message().to_string()))?;
    if statements.is_empty() {
        return Err(DbError::sql("no SQL statements found"));
    }

    statements
        .into_iter()
        .filter(|statement| !statement.trim().is_empty())
        .map(|statement| normalize_statement_text(&statement))
        .collect()
}

pub(crate) fn parse_expression_sql(sql: &str) -> Result<Expr> {
    match parse_sql_statement(&format!("SELECT {sql}"))? {
        Statement::Query(query) => match query.body {
            super::ast::QueryBody::Select(select) if select.projection.len() == 1 => {
                match select
                    .projection
                    .into_iter()
                    .next()
                    .expect("projection exists")
                {
                    SelectItem::Expr { expr, .. } => Ok(expr),
                    _ => Err(DbError::sql(
                        "expression parser expected a scalar SELECT item",
                    )),
                }
            }
            _ => Err(DbError::sql("expression parser expected a simple SELECT")),
        },
        _ => Err(DbError::sql(
            "expression parser expected a SELECT statement",
        )),
    }
}
