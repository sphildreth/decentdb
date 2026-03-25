//! Parser wrapper that normalizes supported SQL into the engine AST.

use std::borrow::Cow;

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
    let compat_sql = rewrite_legacy_trigger_body(sql);
    let statements = libpg_query_sys::split_statements(compat_sql.as_ref())
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

pub(crate) fn rewrite_legacy_trigger_body(sql: &str) -> Cow<'_, str> {
    let trimmed = sql.trim();
    let keywords = top_level_keywords(trimmed);
    if keywords.len() < 2 || keywords[0].2 != "CREATE" || keywords[1].2 != "TRIGGER" {
        return Cow::Borrowed(sql);
    }

    let Some((begin_start, begin_end, _)) =
        keywords.iter().find(|(_, _, keyword)| keyword == "BEGIN")
    else {
        return Cow::Borrowed(sql);
    };
    let Some((end_start, _, _)) = keywords
        .iter()
        .rev()
        .find(|(start, _, keyword)| *start > *begin_start && keyword == "END")
    else {
        return Cow::Borrowed(sql);
    };

    let prefix = trimmed[..*begin_start].trim_end();
    let mut body = trimmed[*begin_end..*end_start].trim();
    while let Some(stripped) = body.strip_suffix(';') {
        body = stripped.trim_end();
    }
    if body.is_empty() || has_top_level_semicolon(body) {
        return Cow::Borrowed(sql);
    }

    let body_keywords = top_level_keywords(body);
    let Some((select_start, select_end, keyword)) = body_keywords.first() else {
        return Cow::Borrowed(sql);
    };
    if *select_start != 0 || keyword != "SELECT" {
        return Cow::Borrowed(sql);
    }
    let action = body[*select_end..].trim();
    if action.is_empty() {
        return Cow::Borrowed(sql);
    }

    Cow::Owned(format!("{prefix} EXECUTE FUNCTION {action}"))
}

fn top_level_keywords(sql: &str) -> Vec<(usize, usize, String)> {
    let mut keywords = Vec::new();
    let mut chars = sql.char_indices().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some((index, ch)) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }
        if in_block_comment {
            if ch == '*' && matches!(chars.peek(), Some((_, '/'))) {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    chars.next();
                } else {
                    in_single = false;
                }
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                if matches!(chars.peek(), Some((_, '"'))) {
                    chars.next();
                } else {
                    in_double = false;
                }
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
            }
            '"' => {
                in_double = true;
            }
            '-' if matches!(chars.peek(), Some((_, '-'))) => {
                chars.next();
                in_line_comment = true;
            }
            '/' if matches!(chars.peek(), Some((_, '*'))) => {
                chars.next();
                in_block_comment = true;
            }
            _ if is_keyword_char(ch) => {
                let mut end = index + ch.len_utf8();
                let mut keyword = ch.to_ascii_uppercase().to_string();
                while let Some((next_index, next)) = chars.peek().copied() {
                    if !is_keyword_char(next) {
                        break;
                    }
                    chars.next();
                    end = next_index + next.len_utf8();
                    keyword.push(next.to_ascii_uppercase());
                }
                keywords.push((index, end, keyword));
            }
            _ => {}
        }
    }

    keywords
}

fn has_top_level_semicolon(sql: &str) -> bool {
    let mut chars = sql.char_indices().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some((_, ch)) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }
        if in_block_comment {
            if ch == '*' && matches!(chars.peek(), Some((_, '/'))) {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    chars.next();
                } else {
                    in_single = false;
                }
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                if matches!(chars.peek(), Some((_, '"'))) {
                    chars.next();
                } else {
                    in_double = false;
                }
            }
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '-' if matches!(chars.peek(), Some((_, '-'))) => {
                chars.next();
                in_line_comment = true;
            }
            '/' if matches!(chars.peek(), Some((_, '*'))) => {
                chars.next();
                in_block_comment = true;
            }
            ';' => return true,
            _ => {}
        }
    }

    false
}

fn is_keyword_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}
