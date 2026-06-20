use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum TransactionControl {
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum PragmaCommand {
    Query(PragmaTarget),
    Call {
        target: PragmaTarget,
        argument: Option<String>,
    },
    Set(PragmaTarget, PragmaValue),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PragmaTarget {
    pub(super) name: PragmaName,
    pub(super) schema: Option<PragmaSchema>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PragmaSchema {
    Main,
    Temp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum PragmaValue {
    Int(i64),
    Text(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PragmaName {
    PageSize,
    CacheSize,
    IntegrityCheck,
    DatabaseList,
    QuickCheck,
    ForeignKeys,
    JournalMode,
    Synchronous,
    WalCheckpoint,
    SchemaVersion,
    UserVersion,
    ApplicationId,
    Encoding,
    BusyTimeout,
    LockingMode,
    TempStore,
    TableInfo,
    TableXInfo,
    TableList,
    IndexList,
    IndexInfo,
    IndexXInfo,
    ForeignKeyList,
    FlushPlanCache,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SimpleCountSqlPlan<'a> {
    pub(super) table_name: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SimpleGroupedCountSqlPlan<'a> {
    pub(super) table_name: &'a str,
    pub(super) group_column: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SimpleRowIdProjectionSqlPlan<'a> {
    pub(super) table_name: &'a str,
    pub(super) projection_columns: Vec<&'a str>,
    pub(super) filter_column: &'a str,
    pub(super) param_index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SimpleRowIdRangeProjectionSqlPlan<'a> {
    pub(super) table_name: &'a str,
    pub(super) projection_columns: Vec<&'a str>,
    pub(super) filter_column: &'a str,
    pub(super) lower_bound: Option<PreparedSimpleRangeBoundParam>,
    pub(super) upper_bound: Option<PreparedSimpleRangeBoundParam>,
    pub(super) limit_param_index: usize,
}

pub(super) fn simple_single_statement_fast_path_sql(sql: &str) -> Option<&str> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return None;
    }
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    if trimmed.is_empty() || trimmed.contains(';') {
        return None;
    }
    Some(trimmed)
}

pub(super) fn parse_simple_count_star_sql(sql: &str) -> Option<SimpleCountSqlPlan<'_>> {
    const PREFIX: &str = "select count(*) from ";
    let trimmed = sql.trim();
    if !trimmed.is_ascii()
        || trimmed.len() <= PREFIX.len()
        || !trimmed[..PREFIX.len()].eq_ignore_ascii_case(PREFIX)
    {
        return None;
    }
    let table_name = trimmed[PREFIX.len()..].trim();
    if !is_simple_sql_identifier(table_name) {
        return None;
    }
    Some(SimpleCountSqlPlan { table_name })
}

pub(super) fn parse_simple_grouped_count_sql(sql: &str) -> Option<SimpleGroupedCountSqlPlan<'_>> {
    let trimmed = sql.trim();
    if !trimmed.is_ascii() {
        return None;
    }
    if trimmed.len() <= 7 || !trimmed[..7].eq_ignore_ascii_case("select ") {
        return None;
    }

    let from_index = find_ascii_case_insensitive(trimmed, " from ")?;
    let group_marker = " group by ";
    let group_index = find_ascii_case_insensitive(&trimmed[from_index + 6..], group_marker)
        .map(|index| from_index + 6 + index)?;
    let order_marker = " order by ";
    let order_index =
        find_ascii_case_insensitive(&trimmed[group_index + group_marker.len()..], order_marker)
            .map(|index| group_index + group_marker.len() + index)?;

    let projection_sql = trimmed[6..from_index].trim();
    let table_name = trimmed[from_index + 6..group_index].trim();
    let group_column = trimmed[group_index + group_marker.len()..order_index].trim();
    let order_sql = trimmed[order_index + order_marker.len()..].trim();
    let mut order_parts = order_sql.split_ascii_whitespace();
    let order_column = order_parts.next()?;
    let order_direction = order_parts.next();
    if order_parts.next().is_some()
        || order_direction.is_some_and(|direction| !direction.eq_ignore_ascii_case("asc"))
    {
        return None;
    }

    let mut projection_parts = projection_sql.split(',');
    let projection_column = projection_parts.next()?.trim();
    let count_expr = projection_parts.next()?.trim();
    if projection_parts.next().is_some()
        || !is_simple_sql_identifier(table_name)
        || !is_simple_sql_identifier(projection_column)
        || !is_simple_sql_identifier(group_column)
        || !is_simple_sql_identifier(order_column)
        || !identifiers_equal(projection_column, group_column)
        || !identifiers_equal(order_column, group_column)
        || !count_expr.eq_ignore_ascii_case("count(*)")
    {
        return None;
    }

    Some(SimpleGroupedCountSqlPlan {
        table_name,
        group_column,
    })
}

pub(super) fn parse_simple_row_id_projection_sql(
    sql: &str,
) -> Option<SimpleRowIdProjectionSqlPlan<'_>> {
    let trimmed = sql.trim();
    if !trimmed.is_ascii() {
        return None;
    }
    if trimmed.len() <= 7 || !trimmed[..7].eq_ignore_ascii_case("select ") {
        return None;
    }
    let from_index = find_ascii_case_insensitive(trimmed, " from ")?;
    let where_marker = " where ";
    let where_index = find_ascii_case_insensitive(&trimmed[from_index + 6..], where_marker)
        .map(|index| from_index + 6 + index)?;
    let filter_tail = &trimmed[where_index + where_marker.len()..];
    if contains_ascii_case_insensitive(filter_tail, " order ")
        || contains_ascii_case_insensitive(filter_tail, " limit ")
        || contains_ascii_case_insensitive(filter_tail, " group ")
    {
        return None;
    }

    let projection_sql = trimmed[6..from_index].trim();
    let table_name = trimmed[from_index + 6..where_index].trim();
    let filter_sql = trimmed[where_index + where_marker.len()..].trim();
    if projection_sql.is_empty() || !is_simple_sql_identifier(table_name) {
        return None;
    }
    let mut projection_columns = Vec::new();
    for column in projection_sql.split(',') {
        let column = column.trim();
        if !is_simple_sql_identifier(column) {
            return None;
        }
        projection_columns.push(column);
    }
    let (left, right) = filter_sql.split_once('=')?;
    let left = left.trim();
    let right = right.trim();
    let (filter_column, param_index) = if let Some(param_index) = parse_positional_param(right) {
        (left, param_index)
    } else if let Some(param_index) = parse_positional_param(left) {
        (right, param_index)
    } else {
        return None;
    };
    if !is_simple_sql_identifier(filter_column) {
        return None;
    }
    Some(SimpleRowIdProjectionSqlPlan {
        table_name,
        projection_columns,
        filter_column,
        param_index,
    })
}

pub(super) fn parse_simple_row_id_range_projection_sql(
    sql: &str,
) -> Option<SimpleRowIdRangeProjectionSqlPlan<'_>> {
    let trimmed = sql.trim();
    if !trimmed.is_ascii() {
        return None;
    }
    if trimmed.len() <= 7 || !trimmed[..7].eq_ignore_ascii_case("select ") {
        return None;
    }
    let from_index = find_ascii_case_insensitive(trimmed, " from ")?;
    let where_marker = " where ";
    let where_index = find_ascii_case_insensitive(&trimmed[from_index + 6..], where_marker)
        .map(|index| from_index + 6 + index)?;
    let projection_sql = trimmed[6..from_index].trim();
    let table_name = trimmed[from_index + 6..where_index].trim();
    if projection_sql.is_empty() || !is_simple_sql_identifier(table_name) {
        return None;
    }

    let filter_tail = trimmed[where_index + where_marker.len()..].trim();
    let limit_marker = " limit ";
    let limit_index = find_ascii_case_insensitive(filter_tail, limit_marker)?;
    let before_limit = filter_tail[..limit_index].trim();
    let limit_sql = filter_tail[limit_index + limit_marker.len()..].trim();
    if limit_sql.is_empty()
        || contains_ascii_case_insensitive(limit_sql, " offset ")
        || limit_sql.split_ascii_whitespace().count() != 1
    {
        return None;
    }
    let limit_param_index = parse_positional_param(limit_sql)?;

    let order_marker = " order by ";
    let (filter_sql, order_column) =
        if let Some(order_index) = find_ascii_case_insensitive(before_limit, order_marker) {
            let filter_sql = before_limit[..order_index].trim();
            let order_sql = before_limit[order_index + order_marker.len()..].trim();
            let mut order_parts = order_sql.split_ascii_whitespace();
            let order_column = order_parts.next()?;
            if !is_simple_sql_identifier(order_column) {
                return None;
            }
            match order_parts.next() {
                None => {}
                Some(direction) if direction.eq_ignore_ascii_case("asc") => {}
                _ => return None,
            }
            if order_parts.next().is_some() {
                return None;
            }
            (filter_sql, Some(order_column))
        } else {
            (before_limit, None)
        };
    if filter_sql.is_empty()
        || contains_ascii_case_insensitive(filter_sql, " group ")
        || contains_ascii_case_insensitive(filter_sql, " having ")
    {
        return None;
    }

    let mut projection_columns = Vec::new();
    for column in projection_sql.split(',') {
        let column = column.trim();
        if !is_simple_sql_identifier(column) {
            return None;
        }
        projection_columns.push(column);
    }

    let mut filter_column = None;
    let mut lower_bound = None;
    let mut upper_bound = None;
    let mut remaining = filter_sql.trim();
    loop {
        let (term, rest) = if let Some(and_index) = find_ascii_case_insensitive(remaining, " and ")
        {
            (
                remaining[..and_index].trim(),
                Some(remaining[and_index + 5..].trim()),
            )
        } else {
            (remaining.trim(), None)
        };
        let (term_column, bound_kind, param_index) = parse_simple_range_param_term(term)?;
        if let Some(existing_column) = filter_column {
            if !identifiers_equal(existing_column, term_column) {
                return None;
            }
        } else {
            filter_column = Some(term_column);
        }
        let bound = PreparedSimpleRangeBoundParam {
            inclusive: bound_kind.inclusive(),
            param_index,
        };
        match bound_kind {
            SimpleRangeParamBoundKind::Lower(_) => {
                if lower_bound.replace(bound).is_some() {
                    return None;
                }
            }
            SimpleRangeParamBoundKind::Upper(_) => {
                if upper_bound.replace(bound).is_some() {
                    return None;
                }
            }
        }
        let Some(rest) = rest else {
            break;
        };
        if rest.is_empty() {
            return None;
        }
        remaining = rest;
    }
    let filter_column = filter_column?;
    if lower_bound.is_none() && upper_bound.is_none() {
        return None;
    }
    if order_column.is_some_and(|column| !identifiers_equal(column, filter_column)) {
        return None;
    }

    Some(SimpleRowIdRangeProjectionSqlPlan {
        table_name,
        projection_columns,
        filter_column,
        lower_bound,
        upper_bound,
        limit_param_index,
    })
}

pub(super) type PreparedJoinColumnRef<'a> = (Option<&'a str>, &'a str);
pub(super) type PreparedJoinColumnEquality<'a> =
    (PreparedJoinColumnRef<'a>, PreparedJoinColumnRef<'a>);

pub(super) fn prepared_join_column_equality(
    constraint: &crate::sql::ast::JoinConstraint,
) -> Option<PreparedJoinColumnEquality<'_>> {
    let crate::sql::ast::JoinConstraint::On(expr) = constraint else {
        return None;
    };
    let crate::sql::ast::Expr::Binary {
        left,
        op: crate::sql::ast::BinaryOp::Eq,
        right,
    } = expr
    else {
        return None;
    };
    let crate::sql::ast::Expr::Column {
        table: left_table,
        column: left_column,
    } = left.as_ref()
    else {
        return None;
    };
    let crate::sql::ast::Expr::Column {
        table: right_table,
        column: right_column,
    } = right.as_ref()
    else {
        return None;
    };
    Some((
        (left_table.as_deref(), left_column.as_str()),
        (right_table.as_deref(), right_column.as_str()),
    ))
}

pub(super) fn prepared_join_filter_param(
    filter: &crate::sql::ast::Expr,
) -> Option<(Option<&str>, &str, usize)> {
    let crate::sql::ast::Expr::Binary {
        left,
        op: crate::sql::ast::BinaryOp::Eq,
        right,
    } = filter
    else {
        return None;
    };
    if let crate::sql::ast::Expr::Column { table, column } = left.as_ref() {
        let crate::sql::ast::Expr::Parameter(param_index) = right.as_ref() else {
            return None;
        };
        return Some((
            table.as_deref(),
            column.as_str(),
            param_index.checked_sub(1)?,
        ));
    }
    if let crate::sql::ast::Expr::Column { table, column } = right.as_ref() {
        let crate::sql::ast::Expr::Parameter(param_index) = left.as_ref() else {
            return None;
        };
        return Some((
            table.as_deref(),
            column.as_str(),
            param_index.checked_sub(1)?,
        ));
    }
    None
}

pub(super) fn prepared_join_column_side(
    table: Option<&str>,
    left_name: &str,
    left_alias: &Option<String>,
    right_name: &str,
    right_alias: &Option<String>,
) -> Option<SimpleJoinProjectionSide> {
    let table = table?;
    let matches_left = identifiers_equal(table, left_name)
        || left_alias
            .as_deref()
            .is_some_and(|alias| identifiers_equal(table, alias));
    let matches_right = identifiers_equal(table, right_name)
        || right_alias
            .as_deref()
            .is_some_and(|alias| identifiers_equal(table, alias));
    match (matches_left, matches_right) {
        (true, false) => Some(SimpleJoinProjectionSide::Left),
        (false, true) => Some(SimpleJoinProjectionSide::Right),
        _ => None,
    }
}

pub(super) fn push_prepared_join_projection_index(indexes: &mut Vec<usize>, index: usize) -> usize {
    if let Some(position) = indexes.iter().position(|candidate| *candidate == index) {
        position
    } else {
        indexes.push(index);
        indexes.len() - 1
    }
}

pub(super) fn prepared_table_generated_columns_are_stored(table: &TableSchema) -> bool {
    table
        .columns
        .iter()
        .all(|column| column.generated_sql.is_none() || column.generated_stored)
}

pub(super) fn prepared_scalar_count_star(expr: &crate::sql::ast::Expr) -> bool {
    let crate::sql::ast::Expr::Aggregate {
        name,
        args,
        distinct,
        star,
        order_by,
        within_group,
    } = expr
    else {
        return false;
    };
    name.eq_ignore_ascii_case("count")
        && args.is_empty()
        && !*distinct
        && *star
        && order_by.is_empty()
        && !*within_group
}

pub(super) fn prepared_scalar_sum_column<'a>(
    expr: &'a crate::sql::ast::Expr,
    table_name: &str,
    alias: &Option<String>,
) -> Option<&'a str> {
    let crate::sql::ast::Expr::Aggregate {
        name,
        args,
        distinct,
        star,
        order_by,
        within_group,
    } = expr
    else {
        return None;
    };
    if !name.eq_ignore_ascii_case("sum")
        || *distinct
        || *star
        || !order_by.is_empty()
        || *within_group
        || args.len() != 1
    {
        return None;
    }
    let crate::sql::ast::Expr::Column { table, column } = &args[0] else {
        return None;
    };
    if prepared_scalar_column_matches_table(table.as_deref(), table_name, alias) {
        Some(column.as_str())
    } else {
        None
    }
}

pub(super) fn prepared_scalar_filter_param(
    filter: &crate::sql::ast::Expr,
    table_name: &str,
    alias: &Option<String>,
) -> Option<usize> {
    let crate::sql::ast::Expr::Binary {
        left,
        op: crate::sql::ast::BinaryOp::Eq,
        right,
    } = filter
    else {
        return None;
    };
    if let crate::sql::ast::Expr::Column { table, .. } = left.as_ref() {
        if prepared_scalar_column_matches_table(table.as_deref(), table_name, alias) {
            let crate::sql::ast::Expr::Parameter(param_index) = right.as_ref() else {
                return None;
            };
            return param_index.checked_sub(1);
        }
    }
    if let crate::sql::ast::Expr::Column { table, .. } = right.as_ref() {
        if prepared_scalar_column_matches_table(table.as_deref(), table_name, alias) {
            let crate::sql::ast::Expr::Parameter(param_index) = left.as_ref() else {
                return None;
            };
            return param_index.checked_sub(1);
        }
    }
    None
}

pub(super) fn prepared_scalar_column_matches_table(
    table: Option<&str>,
    table_name: &str,
    alias: &Option<String>,
) -> bool {
    table.is_none_or(|qualifier| {
        identifiers_equal(qualifier, table_name)
            || alias
                .as_deref()
                .is_some_and(|alias| identifiers_equal(qualifier, alias))
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SimpleRangeParamBoundKind {
    Lower(bool),
    Upper(bool),
}

impl SimpleRangeParamBoundKind {
    pub(super) fn inclusive(self) -> bool {
        match self {
            Self::Lower(inclusive) | Self::Upper(inclusive) => inclusive,
        }
    }
}

pub(super) fn parse_simple_range_param_term(
    term: &str,
) -> Option<(&str, SimpleRangeParamBoundKind, usize)> {
    let (left, op, right) = split_simple_range_operator(term)?;
    if is_simple_sql_identifier(left) {
        let param_index = parse_positional_param(right)?;
        let bound = match op {
            SimpleRangeOperator::Gt => SimpleRangeParamBoundKind::Lower(false),
            SimpleRangeOperator::GtEq => SimpleRangeParamBoundKind::Lower(true),
            SimpleRangeOperator::Lt => SimpleRangeParamBoundKind::Upper(false),
            SimpleRangeOperator::LtEq => SimpleRangeParamBoundKind::Upper(true),
        };
        return Some((left, bound, param_index));
    }
    if is_simple_sql_identifier(right) {
        let param_index = parse_positional_param(left)?;
        let bound = match op {
            SimpleRangeOperator::Gt => SimpleRangeParamBoundKind::Upper(false),
            SimpleRangeOperator::GtEq => SimpleRangeParamBoundKind::Upper(true),
            SimpleRangeOperator::Lt => SimpleRangeParamBoundKind::Lower(false),
            SimpleRangeOperator::LtEq => SimpleRangeParamBoundKind::Lower(true),
        };
        return Some((right, bound, param_index));
    }
    None
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SimpleRangeOperator {
    Gt,
    GtEq,
    Lt,
    LtEq,
}

pub(super) fn split_simple_range_operator(term: &str) -> Option<(&str, SimpleRangeOperator, &str)> {
    for (token, op) in [
        (">=", SimpleRangeOperator::GtEq),
        ("<=", SimpleRangeOperator::LtEq),
        (">", SimpleRangeOperator::Gt),
        ("<", SimpleRangeOperator::Lt),
    ] {
        if let Some(index) = term.find(token) {
            let left = term[..index].trim();
            let right = term[index + token.len()..].trim();
            if left.is_empty() || right.is_empty() {
                return None;
            }
            return Some((left, op, right));
        }
    }
    None
}

pub(super) fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .position(|window| ascii_bytes_eq_ignore_ascii_case(window, needle.as_bytes()))
}

pub(super) fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    find_ascii_case_insensitive(haystack, needle).is_some()
}

pub(super) fn ascii_bytes_eq_ignore_ascii_case(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

pub(super) fn parse_positional_param(sql: &str) -> Option<usize> {
    let number = sql.strip_prefix('$')?;
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    number.parse::<usize>().ok()?.checked_sub(1)
}

pub(super) fn is_simple_sql_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

pub(super) fn parse_transaction_control(sql: &str) -> Option<TransactionControl> {
    let normalized = normalized_control_sql(sql);
    let upper = normalized.to_ascii_uppercase();

    match upper.as_str() {
        "BEGIN"
        | "BEGIN TRANSACTION"
        | "BEGIN DEFERRED"
        | "BEGIN DEFERRED TRANSACTION"
        | "BEGIN IMMEDIATE"
        | "BEGIN IMMEDIATE TRANSACTION"
        | "BEGIN EXCLUSIVE"
        | "BEGIN EXCLUSIVE TRANSACTION" => Some(TransactionControl::Begin),
        "COMMIT" | "END" | "END TRANSACTION" => Some(TransactionControl::Commit),
        "ROLLBACK" | "ROLLBACK TRANSACTION" => Some(TransactionControl::Rollback),
        _ => parse_savepoint_control(&normalized),
    }
}

pub(super) fn parse_pragma_command(sql: &str) -> Result<Option<PragmaCommand>> {
    let trimmed = sql.trim();
    let Some(_) = trimmed
        .get(..6)
        .filter(|prefix| prefix.eq_ignore_ascii_case("PRAGMA"))
    else {
        return Ok(None);
    };
    let body = trimmed[6..].trim();
    if body.is_empty() {
        return Err(DbError::sql("PRAGMA requires a name"));
    }
    let body = body.trim_end_matches(';').trim();
    if body.is_empty() {
        return Err(DbError::sql("PRAGMA requires a name"));
    }

    if let Some(open_paren) = body.find('(') {
        let close_paren = body
            .rfind(')')
            .ok_or_else(|| DbError::sql("PRAGMA call is missing closing ')'"))?;
        if close_paren <= open_paren {
            return Err(DbError::sql("PRAGMA call has invalid parentheses"));
        }
        let name = body[..open_paren].trim();
        let argument = body[open_paren + 1..close_paren].trim();
        let trailing = body[close_paren + 1..].trim();
        if !trailing.is_empty() {
            return Err(DbError::sql("PRAGMA call has unexpected trailing content"));
        }
        let name = parse_pragma_name(name)?;
        if argument.is_empty() && name.name != PragmaName::WalCheckpoint {
            return Err(DbError::sql("PRAGMA call requires an argument"));
        }
        let accepts_call = matches!(
            name.name,
            PragmaName::TableInfo
                | PragmaName::TableXInfo
                | PragmaName::IndexList
                | PragmaName::IndexInfo
                | PragmaName::IndexXInfo
                | PragmaName::ForeignKeyList
                | PragmaName::WalCheckpoint
        );
        if !accepts_call {
            return Err(DbError::sql(format!(
                "PRAGMA {} does not accept call syntax",
                pragma_name_sql(&name.name)
            )));
        }
        let argument = if argument.is_empty() {
            None
        } else {
            Some(parse_pragma_table_argument(argument)?)
        };
        return Ok(Some(PragmaCommand::Call {
            target: name,
            argument,
        }));
    }

    let (name, value) = if let Some(eq_index) = body.find('=') {
        let name = body[..eq_index].trim();
        let value = body[eq_index + 1..].trim();
        if name.is_empty() || value.is_empty() {
            return Err(DbError::sql("PRAGMA assignment requires a name and value"));
        }
        (name, Some(value))
    } else {
        (body, None)
    };
    let pragma_name = parse_pragma_name(name)?;
    let command = if let Some(value) = value {
        let value = parse_pragma_value(value)?;
        PragmaCommand::Set(pragma_name, value)
    } else {
        PragmaCommand::Query(pragma_name)
    };
    Ok(Some(command))
}

pub(super) fn parse_pragma_name(name: &str) -> Result<PragmaTarget> {
    let normalized = name.trim().to_ascii_lowercase();
    let (schema, pragma_name) = match normalized.split_once('.') {
        Some((schema, pragma_name)) => {
            let schema = match schema.trim() {
                "main" => Some(PragmaSchema::Main),
                "temp" => Some(PragmaSchema::Temp),
                _ => {
                    return Err(DbError::sql(format!(
                        "unsupported PRAGMA schema qualifier '{}'; supported qualifiers are main and temp",
                        schema
                    )));
                }
            };
            (schema, pragma_name.trim())
        }
        None => (None, normalized.as_str()),
    };
    if pragma_name.is_empty() {
        return Err(DbError::sql("PRAGMA requires a name"));
    }
    let name = match pragma_name {
        "page_size" => Ok(PragmaName::PageSize),
        "cache_size" => Ok(PragmaName::CacheSize),
        "integrity_check" => Ok(PragmaName::IntegrityCheck),
        "database_list" => Ok(PragmaName::DatabaseList),
        "quick_check" => Ok(PragmaName::QuickCheck),
        "foreign_keys" => Ok(PragmaName::ForeignKeys),
        "journal_mode" => Ok(PragmaName::JournalMode),
        "synchronous" => Ok(PragmaName::Synchronous),
        "wal_checkpoint" => Ok(PragmaName::WalCheckpoint),
        "schema_version" => Ok(PragmaName::SchemaVersion),
        "user_version" => Ok(PragmaName::UserVersion),
        "application_id" => Ok(PragmaName::ApplicationId),
        "encoding" => Ok(PragmaName::Encoding),
        "busy_timeout" => Ok(PragmaName::BusyTimeout),
        "locking_mode" => Ok(PragmaName::LockingMode),
        "temp_store" => Ok(PragmaName::TempStore),
        "table_info" => Ok(PragmaName::TableInfo),
        "table_xinfo" => Ok(PragmaName::TableXInfo),
        "table_list" => Ok(PragmaName::TableList),
        "index_list" => Ok(PragmaName::IndexList),
        "index_info" => Ok(PragmaName::IndexInfo),
        "index_xinfo" => Ok(PragmaName::IndexXInfo),
        "foreign_key_list" => Ok(PragmaName::ForeignKeyList),
        "flush_plan_cache" => Ok(PragmaName::FlushPlanCache),
        "auto_vacuum" => Err(DbError::sql(
            "PRAGMA auto_vacuum is not supported; not applicable to DecentDB storage/checkpointing",
        )),
        "cache_spill" => Err(DbError::sql(
            "PRAGMA cache_spill is not supported; no DecentDB page-cache spill policy",
        )),
        "case_sensitive_like" => Err(DbError::sql(
            "PRAGMA case_sensitive_like is not supported unless the SQL engine explicitly exposes it",
        )),
        "defer_foreign_keys" => Err(DbError::sql(
            "PRAGMA defer_foreign_keys is not supported; deferred constraints are advanced compatibility work",
        )),
        "ignore_check_constraints" => Err(DbError::sql(
            "PRAGMA ignore_check_constraints is not supported; constraints cannot be disabled",
        )),
        "mmap_size" => Err(DbError::sql(
            "PRAGMA mmap_size is not supported as a SQL runtime tuning knob",
        )),
        "optimize" => Err(DbError::sql(
            "PRAGMA optimize is not supported in this compatibility slice",
        )),
        "read_uncommitted" => Err(DbError::sql(
            "PRAGMA read_uncommitted is not supported; DecentDB does not expose dirty reads",
        )),
        "recursive_triggers" => Err(DbError::sql(
            "PRAGMA recursive_triggers is not supported without trigger recursion semantics",
        )),
        "secure_delete" => Err(DbError::sql(
            "PRAGMA secure_delete is not supported; no exact SQLite free-page overwrite mode",
        )),
        "trusted_schema" => Err(DbError::sql(
            "PRAGMA trusted_schema is not supported; DecentDB does not expose that extension model",
        )),
        _ => Err(DbError::sql(format!("unsupported PRAGMA {}", pragma_name))),
    }?;
    Ok(PragmaTarget { name, schema })
}

pub(super) fn parse_pragma_value(value: &str) -> Result<PragmaValue> {
    let value = value.trim();
    if value.is_empty() {
        return Err(DbError::sql("PRAGMA assignment requires a value"));
    }
    if value.starts_with('"') {
        if !value.ends_with('"') || value.len() < 2 {
            return Err(DbError::sql("PRAGMA assignment has invalid quoted value"));
        }
        return Ok(PragmaValue::Text(
            value[1..value.len() - 1].replace("\"\"", "\""),
        ));
    }
    if value.starts_with('\'') {
        if !value.ends_with('\'') || value.len() < 2 {
            return Err(DbError::sql("PRAGMA assignment has invalid quoted value"));
        }
        return Ok(PragmaValue::Text(
            value[1..value.len() - 1].replace("''", "'"),
        ));
    }
    if let Ok(value) = value.parse::<i64>() {
        return Ok(PragmaValue::Int(value));
    }
    Ok(PragmaValue::Text(value.to_string()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SynchronousRequest {
    Full,
    Normal,
    Off,
    Extra,
}

pub(super) fn pragma_synchronous_mode_value(mode: WalSyncMode) -> i64 {
    match mode {
        WalSyncMode::Full => 2,
        WalSyncMode::Normal | WalSyncMode::AsyncCommit { .. } => 1,
        WalSyncMode::TestingOnlyUnsafeNoSync => 0,
    }
}

pub(super) fn pragma_value_i64(value: &PragmaValue) -> Result<i64> {
    match value {
        PragmaValue::Int(value) => Ok(*value),
        PragmaValue::Text(value) => Err(DbError::sql(format!(
            "PRAGMA assignment expects a numeric value, got '{value}'"
        ))),
    }
}

pub(super) fn parse_pragma_bool_value(value: &PragmaValue, pragma: &str) -> Result<bool> {
    let normalized = match value {
        PragmaValue::Int(value) => value.to_string(),
        PragmaValue::Text(value) => value.clone(),
    };
    match normalized.to_ascii_uppercase().as_str() {
        "1" | "ON" | "TRUE" | "YES" => Ok(true),
        "0" | "OFF" | "FALSE" | "NO" => Ok(false),
        _ => Err(DbError::sql(format!(
            "{} accepts ON/TRUE/YES/1 or OFF/FALSE/NO/0",
            pragma
        ))),
    }
}

pub(super) fn parse_pragma_text_or_mode(value: &PragmaValue, pragma: &str) -> Result<String> {
    let value = match value {
        PragmaValue::Text(value) => value.clone(),
        PragmaValue::Int(value) => value.to_string(),
    };
    let normalized = value.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(DbError::sql(format!("{pragma} requires a mode/value",)));
    }
    Ok(normalized)
}

pub(super) fn parse_pragma_synchronous_request(
    value: &PragmaValue,
    pragma: &str,
) -> Result<SynchronousRequest> {
    match parse_pragma_text_or_mode(value, pragma)?.as_str() {
        "FULL" => Ok(SynchronousRequest::Full),
        "NORMAL" => Ok(SynchronousRequest::Normal),
        "OFF" => Ok(SynchronousRequest::Off),
        "EXTRA" | "3" => Ok(SynchronousRequest::Extra),
        "0" => Ok(SynchronousRequest::Off),
        "1" => Ok(SynchronousRequest::Normal),
        "2" => Ok(SynchronousRequest::Full),
        _ => Err(DbError::sql(
            "PRAGMA synchronous accepts OFF, NORMAL, FULL, EXTRA, 0, 1, 2, or 3",
        )),
    }
}

pub(super) fn pragma_required_argument(
    target: &PragmaTarget,
    argument: Option<String>,
) -> Result<String> {
    argument.ok_or_else(|| {
        DbError::sql(format!(
            "PRAGMA {} requires an argument",
            pragma_name_sql(&target.name)
        ))
    })
}

pub(super) fn pragma_schema_function_prefix(schema: Option<PragmaSchema>) -> &'static str {
    match schema {
        Some(PragmaSchema::Main) => "main.",
        Some(PragmaSchema::Temp) => "temp.",
        None => "",
    }
}

pub(super) fn pragma_name_sql(name: &PragmaName) -> &'static str {
    match name {
        PragmaName::PageSize => "page_size",
        PragmaName::CacheSize => "cache_size",
        PragmaName::IntegrityCheck => "integrity_check",
        PragmaName::DatabaseList => "database_list",
        PragmaName::QuickCheck => "quick_check",
        PragmaName::ForeignKeys => "foreign_keys",
        PragmaName::JournalMode => "journal_mode",
        PragmaName::Synchronous => "synchronous",
        PragmaName::WalCheckpoint => "wal_checkpoint",
        PragmaName::SchemaVersion => "schema_version",
        PragmaName::UserVersion => "user_version",
        PragmaName::ApplicationId => "application_id",
        PragmaName::Encoding => "encoding",
        PragmaName::BusyTimeout => "busy_timeout",
        PragmaName::LockingMode => "locking_mode",
        PragmaName::TempStore => "temp_store",
        PragmaName::TableInfo => "table_info",
        PragmaName::TableXInfo => "table_xinfo",
        PragmaName::TableList => "table_list",
        PragmaName::IndexList => "index_list",
        PragmaName::IndexInfo => "index_info",
        PragmaName::IndexXInfo => "index_xinfo",
        PragmaName::ForeignKeyList => "foreign_key_list",
        PragmaName::FlushPlanCache => "flush_plan_cache",
    }
}

pub(super) fn parse_pragma_table_argument(argument: &str) -> Result<String> {
    let trimmed = argument.trim();
    if trimmed.is_empty() {
        return Err(DbError::sql("PRAGMA table_info requires a table name"));
    }
    if trimmed.starts_with('\"') {
        if !trimmed.ends_with('\"') || trimmed.len() < 2 {
            return Err(DbError::sql(
                "PRAGMA table_info has invalid quoted table name",
            ));
        }
        let inner = &trimmed[1..trimmed.len() - 1];
        return Ok(inner.replace("\"\"", "\""));
    }
    if trimmed.starts_with('\'') {
        if !trimmed.ends_with('\'') || trimmed.len() < 2 {
            return Err(DbError::sql(
                "PRAGMA table_info has invalid quoted table name",
            ));
        }
        let inner = &trimmed[1..trimmed.len() - 1];
        return Ok(inner.replace("''", "'"));
    }
    Ok(trimmed.to_string())
}

pub(super) fn cache_size_pages(config: &DbConfig) -> i64 {
    let bytes = config.cache_size_mb.saturating_mul(1024 * 1024);
    let pages = (bytes / config.page_size as usize).max(1);
    i64::try_from(pages).unwrap_or(i64::MAX)
}

pub(super) fn normalized_control_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn parse_savepoint_control(sql: &str) -> Option<TransactionControl> {
    if let Some(name) = strip_control_prefix(sql, "SAVEPOINT ") {
        return Some(TransactionControl::Savepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "RELEASE SAVEPOINT ") {
        return Some(TransactionControl::ReleaseSavepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "RELEASE ") {
        return Some(TransactionControl::ReleaseSavepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "ROLLBACK TO SAVEPOINT ") {
        return Some(TransactionControl::RollbackToSavepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "ROLLBACK TRANSACTION TO SAVEPOINT ") {
        return Some(TransactionControl::RollbackToSavepoint(name.to_string()));
    }
    if let Some(name) = strip_control_prefix(sql, "ROLLBACK TO ") {
        return Some(TransactionControl::RollbackToSavepoint(name.to_string()));
    }
    None
}

pub(super) fn strip_control_prefix<'a>(sql: &'a str, prefix: &str) -> Option<&'a str> {
    if !sql.get(..prefix.len())?.eq_ignore_ascii_case(prefix) {
        return None;
    }
    let remainder = sql[prefix.len()..].trim();
    if remainder.is_empty() {
        None
    } else {
        Some(remainder)
    }
}

pub(super) fn canonical_savepoint_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

pub(super) fn split_sql_batch(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut statement_tokens = Vec::new();
    let mut trigger_body_depth = 0usize;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            current.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            current.push(ch);
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                current.push(chars.next().expect("comment terminator"));
                in_block_comment = false;
            }
            continue;
        }

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

        if in_double {
            current.push(ch);
            if ch == '"' {
                if matches!(chars.peek(), Some('"')) {
                    current.push(chars.next().expect("escaped quote"));
                } else {
                    in_double = false;
                }
            }
            continue;
        }

        match ch {
            _ if ch.is_ascii_alphanumeric() || ch == '_' => {
                current.push(ch);
                let mut token = ch.to_ascii_uppercase().to_string();
                while let Some(next) = chars.peek().copied() {
                    if !(next.is_ascii_alphanumeric() || next == '_') {
                        break;
                    }
                    let next = chars.next().expect("peeked token char");
                    current.push(next);
                    token.push(next.to_ascii_uppercase());
                }
                if statement_tokens.len() < 2 {
                    statement_tokens.push(token.clone());
                }
                if statement_tokens.as_slice() == ["CREATE", "TRIGGER"] {
                    if token == "BEGIN" {
                        trigger_body_depth += 1;
                    } else if token == "END" && trigger_body_depth > 0 {
                        trigger_body_depth -= 1;
                    }
                }
            }
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '"' => {
                in_double = true;
                current.push(ch);
            }
            '-' if matches!(chars.peek(), Some('-')) => {
                current.push(ch);
                current.push(chars.next().expect("line comment start"));
                in_line_comment = true;
            }
            '/' if matches!(chars.peek(), Some('*')) => {
                current.push(ch);
                current.push(chars.next().expect("block comment start"));
                in_block_comment = true;
            }
            ';' => {
                if trigger_body_depth > 0 {
                    current.push(ch);
                } else if !current.trim().is_empty() {
                    statements.push(rewrite_legacy_trigger_body(current.trim()).into_owned());
                    current.clear();
                    statement_tokens.clear();
                    trigger_body_depth = 0;
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        statements.push(rewrite_legacy_trigger_body(current.trim()).into_owned());
    }
    statements
}

pub(super) fn expand_sql_parameters_for_branch_log(sql: &str, params: &[Value]) -> Result<String> {
    let mut expanded = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            expanded.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            expanded.push(ch);
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                expanded.push(chars.next().expect("comment terminator"));
                in_block_comment = false;
            }
            continue;
        }

        if in_single {
            expanded.push(ch);
            if ch == '\'' {
                if matches!(chars.peek(), Some('\'')) {
                    expanded.push(chars.next().expect("escaped quote"));
                } else {
                    in_single = false;
                }
            }
            continue;
        }

        if in_double {
            expanded.push(ch);
            if ch == '"' {
                if matches!(chars.peek(), Some('"')) {
                    expanded.push(chars.next().expect("escaped quote"));
                } else {
                    in_double = false;
                }
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                expanded.push(ch);
            }
            '"' => {
                in_double = true;
                expanded.push(ch);
            }
            '-' if matches!(chars.peek(), Some('-')) => {
                expanded.push(ch);
                expanded.push(chars.next().expect("line comment start"));
                in_line_comment = true;
            }
            '/' if matches!(chars.peek(), Some('*')) => {
                expanded.push(ch);
                expanded.push(chars.next().expect("block comment start"));
                in_block_comment = true;
            }
            '$' if chars.peek().is_some_and(|next| next.is_ascii_digit()) => {
                let mut digits = String::new();
                while let Some(next) = chars.peek().copied() {
                    if !next.is_ascii_digit() {
                        break;
                    }
                    digits.push(chars.next().expect("peeked digit"));
                }
                let index = digits
                    .parse::<usize>()
                    .map_err(|_| DbError::sql(format!("invalid parameter reference: ${digits}")))?;
                if index == 0 {
                    return Err(DbError::sql("parameter indexes are 1-based"));
                }
                let value = params
                    .get(index - 1)
                    .ok_or_else(|| DbError::sql(format!("parameter ${index} was not provided")))?;
                expanded.push_str(&render_branch_parameter_value_sql(value)?);
            }
            _ => expanded.push(ch),
        }
    }

    Ok(expanded)
}

pub(super) fn prepared_statement_sql(sql: &str) -> Result<String> {
    let statements = split_sql_batch(sql)
        .into_iter()
        .map(|statement| statement.trim().to_string())
        .filter(|statement| !statement.is_empty())
        .collect::<Vec<_>>();
    if statements.len() != 1 {
        return Err(DbError::sql(format!(
            "expected exactly one SQL statement, got {}",
            statements.len()
        )));
    }
    let statement = statements
        .into_iter()
        .next()
        .ok_or_else(|| DbError::sql("expected exactly one SQL statement, got 0"))?;
    if parse_transaction_control(&statement).is_some() {
        return Err(DbError::sql(
            "prepared statements do not support transaction control",
        ));
    }
    reject_unsupported_collated_key_sql(&statement)?;
    Ok(statement)
}

pub(super) fn parameter_shape_for_prepared_sql(sql: &str) -> crate::plan_cache::ParameterShape {
    let bytes = sql.as_bytes();
    let mut index = 0_usize;
    let mut highest_dollar_param = 0_usize;
    let mut positional_params = 0_usize;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\'' {
                        if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                            index += 2;
                        } else {
                            index += 1;
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
            }
            b'"' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'"' {
                        if index + 1 < bytes.len() && bytes[index + 1] == b'"' {
                            index += 2;
                        } else {
                            index += 1;
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
            }
            b'-' if index + 1 < bytes.len() && bytes[index + 1] == b'-' => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if index + 1 < bytes.len() && bytes[index + 1] == b'*' => {
                index += 2;
                while index + 1 < bytes.len() {
                    if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            b'?' => {
                positional_params = positional_params.saturating_add(1);
                index += 1;
            }
            b'$' if index + 1 < bytes.len() && bytes[index + 1].is_ascii_digit() => {
                index += 1;
                let mut value = 0_usize;
                while index < bytes.len() && bytes[index].is_ascii_digit() {
                    value = value
                        .saturating_mul(10)
                        .saturating_add((bytes[index] - b'0') as usize);
                    index += 1;
                }
                highest_dollar_param = highest_dollar_param.max(value);
            }
            _ => index += 1,
        }
    }

    let arity = if highest_dollar_param > 0 && positional_params > 0 {
        highest_dollar_param.saturating_add(positional_params)
    } else {
        highest_dollar_param.max(positional_params)
    };
    crate::plan_cache::ParameterShape::unknown_with_arity(arity)
}

pub(super) fn reject_unsupported_collated_key_sql(sql: &str) -> Result<()> {
    let upper = sql.to_ascii_uppercase();
    if let Some(select_index) = upper.find("SELECT DISTINCT") {
        let after_distinct = select_index + "SELECT DISTINCT".len();
        let projection_end = upper[after_distinct..]
            .find(" FROM ")
            .map(|offset| after_distinct + offset)
            .unwrap_or(upper.len());
        if upper[after_distinct..projection_end].contains(" COLLATE ") {
            return Err(DbError::sql(
                "COLLATE in DISTINCT keys is not supported in this compatibility slice",
            ));
        }
    }
    if let Some(group_by_index) = upper.find(" GROUP BY ") {
        let group_start = group_by_index + " GROUP BY ".len();
        let group_end = [" HAVING ", " ORDER BY ", " LIMIT ", " OFFSET "]
            .iter()
            .filter_map(|marker| {
                upper[group_start..]
                    .find(marker)
                    .map(|offset| group_start + offset)
            })
            .min()
            .unwrap_or(upper.len());
        if upper[group_start..group_end].contains(" COLLATE ") {
            return Err(DbError::sql(
                "COLLATE in GROUP BY keys is not supported in this compatibility slice",
            ));
        }
    }
    Ok(())
}
