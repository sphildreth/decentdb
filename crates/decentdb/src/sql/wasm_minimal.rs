//! Minimal wasm parser used to keep the initial browser target independent of
//! the native C-backed PostgreSQL parser.
//!
//! This parser is intentionally narrow. It exists to support the first browser
//! smoke path while the full wasm-compatible parser strategy is finalized.

use crate::catalog::{ColumnType, EnumTypeInfo, SpatialTypeInfo};
use crate::error::{DbError, Result};
use crate::record::value::Value;

use super::ast::{
    Assignment, BinaryOp, ColumnDefinition, CreateIndexStatement, CreateTableStatement,
    DeleteStatement, Expr, FromItem, IndexExpression, InsertSource, InsertStatement, OrderBy,
    Query, QueryBody, Select, SelectItem, Statement, UpdateStatement,
};

pub(crate) fn parse_sql_batch(sql: &str) -> Result<Vec<Statement>> {
    let statements = split_sql_batch(sql);
    if statements.is_empty() {
        return Err(DbError::sql("no SQL statements found"));
    }
    statements
        .iter()
        .map(|statement| parse_statement(statement.trim()))
        .collect()
}

fn parse_statement(sql: &str) -> Result<Statement> {
    let trimmed = sql.trim();
    if starts_with_keyword(trimmed, "CREATE TABLE") {
        parse_create_table(trimmed).map(Statement::CreateTable)
    } else if starts_with_keyword(trimmed, "CREATE INDEX")
        || starts_with_keyword(trimmed, "CREATE UNIQUE INDEX")
    {
        parse_create_index(trimmed).map(Statement::CreateIndex)
    } else if starts_with_keyword(trimmed, "INSERT INTO") {
        parse_insert(trimmed).map(Statement::Insert)
    } else if starts_with_keyword(trimmed, "UPDATE") {
        parse_update(trimmed).map(Statement::Update)
    } else if starts_with_keyword(trimmed, "DELETE FROM") {
        parse_delete(trimmed).map(Statement::Delete)
    } else if starts_with_keyword(trimmed, "SELECT") {
        parse_select(trimmed).map(Statement::Query)
    } else if starts_with_keyword(trimmed, "DROP TABLE") {
        parse_drop_table(trimmed)
    } else if starts_with_keyword(trimmed, "DROP INDEX") {
        parse_drop_index(trimmed)
    } else {
        Err(DbError::sql(format!(
            "unsupported SQL in initial wasm parser: {trimmed}"
        )))
    }
}

fn parse_create_table(sql: &str) -> Result<CreateTableStatement> {
    let after_prefix = consume_keyword(sql, "CREATE")?;
    let after_temp = if let Some(rest) = consume_keyword_optional(after_prefix, "TEMPORARY") {
        rest
    } else if let Some(rest) = consume_keyword_optional(after_prefix, "TEMP") {
        rest
    } else {
        after_prefix
    };
    let temporary = !std::ptr::eq(after_prefix, after_temp);
    let after_table = consume_keyword(after_temp, "TABLE")?;
    let (if_not_exists, rest) = if let Some(after_if_not_exists) =
        consume_keywords_optional(after_table, &["IF", "NOT", "EXISTS"])
    {
        (true, after_if_not_exists)
    } else {
        (false, after_table)
    };
    let open = rest
        .find('(')
        .ok_or_else(|| DbError::sql("CREATE TABLE requires a column list"))?;
    let table_name = clean_identifier(rest[..open].trim())?;
    let close = matching_final_paren(rest)?;
    let column_sql = &rest[open + 1..close];
    let columns = split_top_level(column_sql, ',')
        .into_iter()
        .map(|column| parse_column_definition(column.trim()))
        .collect::<Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Err(DbError::sql("CREATE TABLE requires at least one column"));
    }
    Ok(CreateTableStatement {
        table_name,
        temporary,
        if_not_exists,
        columns,
        constraints: Vec::new(),
    })
}

fn parse_column_definition(sql: &str) -> Result<ColumnDefinition> {
    let tokens = tokenize_words(sql);
    if tokens.len() < 2 {
        return Err(DbError::sql(format!("invalid column definition: {sql}")));
    }
    let name = clean_identifier(&tokens[0])?;
    let column_type = parse_column_type(&tokens[1])?;
    let mut nullable = true;
    let mut primary_key = false;
    let mut unique = false;
    let mut index = 2;
    while index < tokens.len() {
        if token_eq(&tokens[index], "PRIMARY")
            && tokens
                .get(index + 1)
                .is_some_and(|token| token_eq(token, "KEY"))
        {
            primary_key = true;
            nullable = false;
            index += 2;
        } else if token_eq(&tokens[index], "NOT")
            && tokens
                .get(index + 1)
                .is_some_and(|token| token_eq(token, "NULL"))
        {
            nullable = false;
            index += 2;
        } else if token_eq(&tokens[index], "UNIQUE") {
            unique = true;
            index += 1;
        } else if token_eq(&tokens[index], "NULL") {
            nullable = true;
            index += 1;
        } else {
            return Err(DbError::sql(format!(
                "unsupported column clause in initial wasm parser: {}",
                tokens[index]
            )));
        }
    }
    Ok(ColumnDefinition {
        name,
        column_type,
        spatial_type: None::<SpatialTypeInfo>,
        enum_type: None::<EnumTypeInfo>,
        nullable,
        default: None,
        generated: None,
        generated_stored: false,
        primary_key,
        unique,
        checks: Vec::new(),
        references: None,
    })
}

fn parse_drop_table(sql: &str) -> Result<Statement> {
    let after_table = consume_keywords(sql, &["DROP", "TABLE"])?;
    let (if_exists, rest) =
        if let Some(after_if_exists) = consume_keywords_optional(after_table, &["IF", "EXISTS"]) {
            (true, after_if_exists)
        } else {
            (false, after_table)
        };
    Ok(Statement::DropTable {
        name: clean_identifier(rest)?,
        if_exists,
    })
}

fn parse_create_index(sql: &str) -> Result<CreateIndexStatement> {
    let after_create = consume_keyword(sql, "CREATE")?;
    let (unique, after_unique) =
        if let Some(rest) = consume_keyword_optional(after_create, "UNIQUE") {
            (true, rest)
        } else {
            (false, after_create)
        };
    let after_index = consume_keyword(after_unique, "INDEX")?;
    let (if_not_exists, rest) = if let Some(after_if_not_exists) =
        consume_keywords_optional(after_index, &["IF", "NOT", "EXISTS"])
    {
        (true, after_if_not_exists)
    } else {
        (false, after_index)
    };
    let on_index =
        find_keyword(rest, "ON").ok_or_else(|| DbError::sql("CREATE INDEX requires ON table"))?;
    let index_name = clean_identifier(rest[..on_index].trim())?;
    let after_on = rest[on_index + "ON".len()..].trim();
    let open = after_on
        .find('(')
        .ok_or_else(|| DbError::sql("CREATE INDEX requires a column list"))?;
    let table_name = clean_identifier(after_on[..open].trim())?;
    let close = matching_final_paren(after_on)?;
    let columns = split_top_level(&after_on[open + 1..close], ',')
        .into_iter()
        .map(|column| clean_identifier(column.trim()).map(IndexExpression::Column))
        .collect::<Result<Vec<_>>>()?;
    Ok(CreateIndexStatement {
        index_name,
        table_name,
        unique,
        if_not_exists,
        access_method: "btree".to_string(),
        columns,
        include_columns: Vec::new(),
        predicate: None,
        options: Vec::new(),
    })
}

fn parse_drop_index(sql: &str) -> Result<Statement> {
    let after_index = consume_keywords(sql, &["DROP", "INDEX"])?;
    let (if_exists, rest) =
        if let Some(after_if_exists) = consume_keywords_optional(after_index, &["IF", "EXISTS"]) {
            (true, after_if_exists)
        } else {
            (false, after_index)
        };
    Ok(Statement::DropIndex {
        name: clean_identifier(rest)?,
        if_exists,
    })
}

fn parse_insert(sql: &str) -> Result<InsertStatement> {
    let rest = consume_keywords(sql, &["INSERT", "INTO"])?;
    let values_index = find_keyword(rest, "VALUES")
        .ok_or_else(|| DbError::sql("INSERT requires VALUES in initial wasm parser"))?;
    let target = rest[..values_index].trim();
    let values_sql = rest[values_index + "VALUES".len()..].trim();
    let (table_name, columns) = if let Some(open) = target.find('(') {
        let close = matching_final_paren(target)?;
        let name = clean_identifier(target[..open].trim())?;
        let columns = split_top_level(&target[open + 1..close], ',')
            .into_iter()
            .map(|column| clean_identifier(column.trim()))
            .collect::<Result<Vec<_>>>()?;
        (name, columns)
    } else {
        (clean_identifier(target)?, Vec::new())
    };
    let rows = parse_values_rows(values_sql)?;
    Ok(InsertStatement {
        table_name,
        columns,
        source: InsertSource::Values(rows),
        on_conflict: None,
        returning: Vec::new(),
    })
}

fn parse_delete(sql: &str) -> Result<DeleteStatement> {
    let rest = consume_keywords(sql, &["DELETE", "FROM"])?;
    let where_index = find_keyword(rest, "WHERE");
    let (table_sql, filter_sql) = match where_index {
        Some(index) => (
            rest[..index].trim(),
            Some(rest[index + "WHERE".len()..].trim()),
        ),
        None => (rest.trim(), None),
    };
    Ok(DeleteStatement {
        table_name: clean_identifier(table_sql)?,
        filter: filter_sql.map(parse_expr).transpose()?,
        returning: Vec::new(),
    })
}

fn parse_update(sql: &str) -> Result<UpdateStatement> {
    let rest = consume_keyword(sql, "UPDATE")?;
    let set_index = find_keyword(rest, "SET").ok_or_else(|| DbError::sql("UPDATE requires SET"))?;
    let table_name = clean_identifier(rest[..set_index].trim())?;
    let after_set = rest[set_index + "SET".len()..].trim();
    let where_index = find_keyword(after_set, "WHERE");
    let (assignments_sql, filter_sql) = match where_index {
        Some(index) => (
            after_set[..index].trim(),
            Some(after_set[index + "WHERE".len()..].trim()),
        ),
        None => (after_set, None),
    };
    let assignments = split_top_level(assignments_sql, ',')
        .into_iter()
        .map(|assignment| parse_assignment(&assignment))
        .collect::<Result<Vec<_>>>()?;
    if assignments.is_empty() {
        return Err(DbError::sql("UPDATE requires at least one assignment"));
    }
    Ok(UpdateStatement {
        table_name,
        assignments,
        filter: filter_sql.map(parse_expr).transpose()?,
        returning: Vec::new(),
    })
}

fn parse_assignment(sql: &str) -> Result<Assignment> {
    let index = find_operator(sql, "=").ok_or_else(|| DbError::sql("assignment requires ="))?;
    Ok(Assignment {
        column_name: clean_identifier(sql[..index].trim())?,
        expr: parse_expr(&sql[index + 1..])?,
    })
}

fn parse_values_rows(sql: &str) -> Result<Vec<Vec<Expr>>> {
    let mut rows = Vec::new();
    for row_sql in split_top_level(sql.trim(), ',') {
        let row_sql = row_sql.trim();
        if !(row_sql.starts_with('(') && row_sql.ends_with(')')) {
            return Err(DbError::sql("VALUES rows must be parenthesized"));
        }
        rows.push(
            split_top_level(&row_sql[1..row_sql.len() - 1], ',')
                .into_iter()
                .map(|expr| parse_expr(expr.trim()))
                .collect::<Result<Vec<_>>>()?,
        );
    }
    Ok(rows)
}

fn parse_select(sql: &str) -> Result<Query> {
    let rest = consume_keyword(sql, "SELECT")?;
    let offset_index = find_keyword(rest, "OFFSET");
    let (before_offset, offset) = match offset_index {
        Some(index) => (
            rest[..index].trim(),
            Some(parse_expr(rest[index + "OFFSET".len()..].trim())?),
        ),
        None => (rest, None),
    };
    let limit_index = find_keyword(before_offset, "LIMIT");
    let (before_limit, limit) = match limit_index {
        Some(index) => (
            before_offset[..index].trim(),
            Some(parse_expr(before_offset[index + "LIMIT".len()..].trim())?),
        ),
        None => (before_offset, None),
    };
    let order_index = find_keyword(before_limit, "ORDER").filter(|index| {
        consume_keyword_optional(&before_limit[index + "ORDER".len()..], "BY").is_some()
    });
    let (main_sql, order_by) = match order_index {
        Some(index) => (
            before_limit[..index].trim(),
            parse_order_by(consume_keyword(
                &before_limit[index + "ORDER".len()..],
                "BY",
            )?)?,
        ),
        None => (before_limit, Vec::new()),
    };
    let from_index = find_keyword(main_sql, "FROM");
    let (projection_sql, from_sql) = match from_index {
        Some(index) => (
            &main_sql[..index],
            Some(main_sql[index + "FROM".len()..].trim()),
        ),
        None => (main_sql, None),
    };
    let projection = split_top_level(projection_sql, ',')
        .into_iter()
        .map(|item| parse_select_item(item.trim()))
        .collect::<Result<Vec<_>>>()?;
    let (from, filter) = if let Some(from_sql) = from_sql {
        let where_index = find_keyword(from_sql, "WHERE");
        let (table_sql, filter_sql) = match where_index {
            Some(index) => (
                from_sql[..index].trim(),
                Some(from_sql[index + "WHERE".len()..].trim()),
            ),
            None => (from_sql.trim(), None),
        };
        let table_name = clean_identifier(table_sql)?;
        (
            vec![FromItem::Table {
                name: table_name,
                alias: None,
            }],
            filter_sql.map(parse_expr).transpose()?,
        )
    } else {
        (Vec::new(), None)
    };
    Ok(Query {
        recursive: false,
        ctes: Vec::new(),
        body: QueryBody::Select(Select {
            projection,
            from,
            filter,
            group_by: Vec::new(),
            having: None,
            distinct: false,
            distinct_on: Vec::new(),
        }),
        order_by,
        limit,
        offset,
    })
}

fn parse_order_by(sql: &str) -> Result<Vec<OrderBy>> {
    split_top_level(sql, ',')
        .into_iter()
        .map(|item| {
            let tokens = tokenize_words(&item);
            let (expr_sql, descending) = match tokens.last() {
                Some(last) if token_eq(last, "DESC") => {
                    (item[..item.len().saturating_sub(last.len())].trim(), true)
                }
                Some(last) if token_eq(last, "ASC") => {
                    (item[..item.len().saturating_sub(last.len())].trim(), false)
                }
                _ => (item.trim(), false),
            };
            Ok(OrderBy {
                expr: parse_expr(expr_sql)?,
                descending,
                collation: None,
            })
        })
        .collect()
}

fn parse_select_item(sql: &str) -> Result<SelectItem> {
    if sql == "*" {
        return Ok(SelectItem::Wildcard);
    }
    let (expr_sql, alias) = if let Some(index) = find_keyword(sql, "AS") {
        (
            &sql[..index],
            Some(clean_identifier(sql[index + "AS".len()..].trim())?),
        )
    } else {
        (sql, None)
    };
    Ok(SelectItem::Expr {
        expr: parse_expr(expr_sql.trim())?,
        alias,
    })
}

fn parse_expr(sql: &str) -> Result<Expr> {
    let trimmed = sql.trim();
    if let Some(index) = find_keyword(trimmed, "OR") {
        return Ok(Expr::Binary {
            left: Box::new(parse_expr(&trimmed[..index])?),
            op: BinaryOp::Or,
            right: Box::new(parse_expr(&trimmed[index + "OR".len()..])?),
        });
    }
    if let Some(index) = find_keyword(trimmed, "AND") {
        return Ok(Expr::Binary {
            left: Box::new(parse_expr(&trimmed[..index])?),
            op: BinaryOp::And,
            right: Box::new(parse_expr(&trimmed[index + "AND".len()..])?),
        });
    }
    if let Some(index) = find_operator(trimmed, "<>") {
        return Ok(Expr::Binary {
            left: Box::new(parse_expr(&trimmed[..index])?),
            op: BinaryOp::NotEq,
            right: Box::new(parse_expr(&trimmed[index + 2..])?),
        });
    }
    if let Some(index) = find_operator(trimmed, "!=") {
        return Ok(Expr::Binary {
            left: Box::new(parse_expr(&trimmed[..index])?),
            op: BinaryOp::NotEq,
            right: Box::new(parse_expr(&trimmed[index + 2..])?),
        });
    }
    if let Some(index) = find_operator(trimmed, "<=") {
        return Ok(Expr::Binary {
            left: Box::new(parse_expr(&trimmed[..index])?),
            op: BinaryOp::LtEq,
            right: Box::new(parse_expr(&trimmed[index + 2..])?),
        });
    }
    if let Some(index) = find_operator(trimmed, ">=") {
        return Ok(Expr::Binary {
            left: Box::new(parse_expr(&trimmed[..index])?),
            op: BinaryOp::GtEq,
            right: Box::new(parse_expr(&trimmed[index + 2..])?),
        });
    }
    if let Some(index) = find_operator(trimmed, "=") {
        return Ok(Expr::Binary {
            left: Box::new(parse_expr(&trimmed[..index])?),
            op: BinaryOp::Eq,
            right: Box::new(parse_expr(&trimmed[index + 1..])?),
        });
    }
    if let Some(index) = find_operator(trimmed, "<") {
        return Ok(Expr::Binary {
            left: Box::new(parse_expr(&trimmed[..index])?),
            op: BinaryOp::Lt,
            right: Box::new(parse_expr(&trimmed[index + 1..])?),
        });
    }
    if let Some(index) = find_operator(trimmed, ">") {
        return Ok(Expr::Binary {
            left: Box::new(parse_expr(&trimmed[..index])?),
            op: BinaryOp::Gt,
            right: Box::new(parse_expr(&trimmed[index + 1..])?),
        });
    }
    if let Some(rest) = trimmed.strip_prefix('$') {
        let index = rest
            .parse::<usize>()
            .map_err(|_| DbError::sql(format!("invalid parameter reference: {trimmed}")))?;
        if index == 0 {
            return Err(DbError::sql("parameter indexes are 1-based"));
        }
        return Ok(Expr::Parameter(index));
    }
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(Expr::Literal(Value::Null));
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return Ok(Expr::Literal(Value::Bool(true)));
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return Ok(Expr::Literal(Value::Bool(false)));
    }
    if let Some(text) = parse_string_literal(trimmed)? {
        return Ok(Expr::Literal(Value::Text(text)));
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return Ok(Expr::Literal(Value::Int64(value)));
    }
    if let Ok(value) = trimmed.parse::<f64>() {
        return Ok(Expr::Literal(Value::Float64(value)));
    }
    Ok(Expr::Column {
        table: None,
        column: clean_identifier(trimmed)?,
    })
}

fn parse_string_literal(sql: &str) -> Result<Option<String>> {
    if !sql.starts_with('\'') {
        return Ok(None);
    }
    if !sql.ends_with('\'') || sql.len() < 2 {
        return Err(DbError::sql("unterminated string literal"));
    }
    Ok(Some(sql[1..sql.len() - 1].replace("''", "'")))
}

fn parse_column_type(sql: &str) -> Result<ColumnType> {
    let normalized = sql.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "INT" | "INTEGER" | "INT64" | "BIGINT" => Ok(ColumnType::Int64),
        "REAL" | "DOUBLE" | "FLOAT" | "FLOAT64" => Ok(ColumnType::Float64),
        "TEXT" | "VARCHAR" | "CHAR" | "STRING" => Ok(ColumnType::Text),
        "BOOL" | "BOOLEAN" => Ok(ColumnType::Bool),
        "BLOB" | "BYTEA" => Ok(ColumnType::Blob),
        "DECIMAL" | "NUMERIC" => Ok(ColumnType::Decimal),
        "UUID" => Ok(ColumnType::Uuid),
        "TIMESTAMP" => Ok(ColumnType::Timestamp),
        other => Err(DbError::sql(format!(
            "unsupported column type in initial wasm parser: {other}"
        ))),
    }
}

fn split_sql_batch(sql: &str) -> Vec<String> {
    split_top_level(sql, ';')
        .into_iter()
        .map(|statement| statement.trim().to_string())
        .filter(|statement| !statement.is_empty())
        .collect()
}

fn split_top_level(sql: &str, delimiter: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0_i32;
    let mut in_single = false;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_single {
            current.push(ch);
            if ch == '\'' {
                if matches!(chars.peek(), Some('\'')) {
                    current.push(chars.next().expect("escaped quote"));
                } else {
                    in_single = false;
                }
            }
            continue;
        }
        match ch {
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            _ if ch == delimiter && depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

fn matching_final_paren(sql: &str) -> Result<usize> {
    let open = sql
        .find('(')
        .ok_or_else(|| DbError::sql("expected opening parenthesis"))?;
    let mut depth = 0_i32;
    let mut in_single = false;
    let mut last_close = None;
    let mut chars = sql.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if in_single {
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    let _ = chars.next();
                } else {
                    in_single = false;
                }
            }
            continue;
        }
        match ch {
            '\'' => in_single = true,
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    last_close = Some(index);
                }
            }
            _ => {}
        }
    }
    let close = last_close.ok_or_else(|| DbError::sql("expected closing parenthesis"))?;
    if close <= open {
        return Err(DbError::sql("invalid parenthesis range"));
    }
    if !sql[close + 1..].trim().is_empty() {
        return Err(DbError::sql(format!(
            "unsupported trailing SQL in initial wasm parser: {}",
            sql[close + 1..].trim()
        )));
    }
    Ok(close)
}

fn clean_identifier(identifier: &str) -> Result<String> {
    let trimmed = identifier.trim();
    if trimmed.is_empty() {
        return Err(DbError::sql("expected identifier"));
    }
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        return Ok(trimmed[1..trimmed.len() - 1].replace("\"\"", "\""));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
    {
        return Err(DbError::sql(format!("invalid identifier: {trimmed}")));
    }
    Ok(trimmed.to_string())
}

fn find_operator(sql: &str, operator: &str) -> Option<usize> {
    let mut depth = 0_i32;
    let mut in_single = false;
    let mut chars = sql.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if in_single {
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    let _ = chars.next();
                } else {
                    in_single = false;
                }
            }
            continue;
        }
        match ch {
            '\'' => in_single = true,
            '(' => depth += 1,
            ')' => depth -= 1,
            _ if depth == 0 && sql[index..].starts_with(operator) => return Some(index),
            _ => {}
        }
    }
    None
}

fn find_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let needle = keyword.as_bytes();
    let bytes = sql.as_bytes();
    let mut depth = 0_i32;
    let mut in_single = false;
    let mut index = 0;
    while index + needle.len() <= bytes.len() {
        let ch = bytes[index] as char;
        if in_single {
            if ch == '\'' {
                if index + 1 < bytes.len() && bytes[index + 1] as char == '\'' {
                    index += 2;
                    continue;
                }
                in_single = false;
            }
            index += 1;
            continue;
        }
        match ch {
            '\'' => {
                in_single = true;
                index += 1;
            }
            '(' => {
                depth += 1;
                index += 1;
            }
            ')' => {
                depth -= 1;
                index += 1;
            }
            _ if depth == 0
                && bytes[index..].len() >= needle.len()
                && sql[index..index + needle.len()].eq_ignore_ascii_case(keyword)
                && is_keyword_boundary(sql, index, keyword.len()) =>
            {
                return Some(index)
            }
            _ => index += 1,
        }
    }
    None
}

fn starts_with_keyword(sql: &str, keyword: &str) -> bool {
    let trimmed = sql.trim_start();
    trimmed.len() >= keyword.len()
        && trimmed[..keyword.len()].eq_ignore_ascii_case(keyword)
        && trimmed[keyword.len()..]
            .chars()
            .next()
            .is_none_or(|ch| ch.is_whitespace() || ch == '(')
}

fn consume_keyword<'a>(sql: &'a str, keyword: &str) -> Result<&'a str> {
    consume_keyword_optional(sql, keyword)
        .ok_or_else(|| DbError::sql(format!("expected keyword {keyword}")))
}

fn consume_keywords<'a>(mut sql: &'a str, keywords: &[&str]) -> Result<&'a str> {
    for keyword in keywords {
        sql = consume_keyword(sql, keyword)?;
    }
    Ok(sql)
}

fn consume_keyword_optional<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = sql.trim_start();
    if starts_with_keyword(trimmed, keyword) {
        Some(trimmed[keyword.len()..].trim_start())
    } else {
        None
    }
}

fn consume_keywords_optional<'a>(mut sql: &'a str, keywords: &[&str]) -> Option<&'a str> {
    for keyword in keywords {
        sql = consume_keyword_optional(sql, keyword)?;
    }
    Some(sql)
}

fn tokenize_words(sql: &str) -> Vec<String> {
    sql.split_whitespace()
        .map(|token| token.trim_matches(',').to_string())
        .filter(|token| !token.is_empty())
        .collect()
}

fn token_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn is_keyword_boundary(sql: &str, start: usize, len: usize) -> bool {
    let before_ok = start == 0
        || !sql[..start]
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    let after = start + len;
    let after_ok = after >= sql.len()
        || !sql[after..]
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    before_ok && after_ok
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_initial_browser_smoke_batch() {
        let statements = parse_sql_batch(
            "CREATE TABLE IF NOT EXISTS notes(id INT64 PRIMARY KEY, body TEXT);
             INSERT INTO notes(id, body) VALUES ($1, 'semi; colon'), (2, 'second');
             CREATE INDEX IF NOT EXISTS notes_body_idx ON notes(body);
             UPDATE notes SET body = 'updated' WHERE id = $1;
             SELECT id, body FROM notes WHERE id = $1 AND body <> 'x' ORDER BY id ASC LIMIT 10 OFFSET 0;
             DELETE FROM notes WHERE id = 2;
             DROP INDEX IF EXISTS notes_body_idx;
             DROP TABLE IF EXISTS notes;",
        )
        .expect("browser smoke SQL should parse");

        assert_eq!(statements.len(), 8);
        assert!(matches!(statements[0], Statement::CreateTable(_)));
        assert!(matches!(statements[1], Statement::Insert(_)));
        assert!(matches!(statements[2], Statement::CreateIndex(_)));
        assert!(matches!(statements[3], Statement::Update(_)));
        assert!(matches!(statements[4], Statement::Query(_)));
        assert!(matches!(statements[5], Statement::Delete(_)));
        assert!(matches!(statements[6], Statement::DropIndex { .. }));
        assert!(matches!(statements[7], Statement::DropTable { .. }));
    }

    #[test]
    fn parses_select_without_from_for_health_checks() {
        let statements = parse_sql_batch("SELECT 1 AS ok").expect("constant select should parse");

        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        let QueryBody::Select(select) = &query.body else {
            panic!("expected select body");
        };
        assert!(select.from.is_empty());
        assert_eq!(select.projection.len(), 1);
    }

    #[test]
    fn rejects_sql_outside_initial_browser_subset() {
        let error = parse_sql_batch("ALTER TABLE notes ADD COLUMN done BOOL")
            .expect_err("unsupported SQL must be rejected explicitly");

        assert!(error
            .to_string()
            .contains("unsupported SQL in initial wasm parser"));
    }
}
