//! SQL type names for PG_TYPEOF, including declared types of NULL expressions.

use super::{ColumnType, Dataset};
use crate::record::value::Value;
use crate::sql::ast::{BinaryOp, Expr, UnaryOp};

pub(super) fn type_name(
    expr: &Expr,
    value: &Value,
    dataset: &Dataset,
    params: &[Value],
) -> &'static str {
    let column_type = value_type(value).or_else(|| declared_type(expr, dataset, params));
    match column_type {
        None => "unknown",
        Some(ColumnType::Int64) => "bigint",
        Some(ColumnType::Float64) => "double precision",
        Some(ColumnType::Text) => "text",
        Some(ColumnType::Bool) => "boolean",
        Some(ColumnType::Blob) => "bytea",
        Some(ColumnType::Decimal) => "numeric",
        Some(ColumnType::Uuid) => "uuid",
        Some(ColumnType::Timestamp) => "timestamp without time zone",
        Some(ColumnType::TimestampTz) => "timestamp with time zone",
        Some(ColumnType::Date) => "date",
        Some(ColumnType::Time) => "time without time zone",
        Some(ColumnType::Interval) => "interval",
        Some(ColumnType::Enum) => "enum",
        Some(ColumnType::IpAddr) => "inet",
        Some(ColumnType::Cidr) => "cidr",
        Some(ColumnType::MacAddr) => "macaddr",
        Some(ColumnType::Geometry) => "geometry",
        Some(ColumnType::Geography) => "geography",
    }
}

fn value_type(value: &Value) -> Option<ColumnType> {
    Some(match value {
        Value::Null => return None,
        Value::Int64(_) => ColumnType::Int64,
        Value::Float64(_) => ColumnType::Float64,
        Value::Text(_) => ColumnType::Text,
        Value::Bool(_) => ColumnType::Bool,
        Value::Blob(_) => ColumnType::Blob,
        Value::Decimal { .. } => ColumnType::Decimal,
        Value::Uuid(_) => ColumnType::Uuid,
        Value::TimestampMicros(_) => ColumnType::Timestamp,
        Value::TimestampTzMicros(_) => ColumnType::TimestampTz,
        Value::DateDays(_) => ColumnType::Date,
        Value::TimeMicros(_) => ColumnType::Time,
        Value::Interval { .. } => ColumnType::Interval,
        Value::Enum { .. } => ColumnType::Enum,
        Value::IpAddr { .. } => ColumnType::IpAddr,
        Value::Cidr { .. } => ColumnType::Cidr,
        Value::MacAddr { .. } => ColumnType::MacAddr,
        Value::Geometry(_) => ColumnType::Geometry,
        Value::Geography(_) => ColumnType::Geography,
    })
}

pub(super) fn declared_type(
    expr: &Expr,
    dataset: &Dataset,
    params: &[Value],
) -> Option<ColumnType> {
    match expr {
        Expr::Literal(value) => value_type(value),
        Expr::Cast { target_type, .. } => Some(*target_type),
        Expr::Parameter(number) => params.get(number.saturating_sub(1)).and_then(value_type),
        Expr::Column { table, column } => dataset
            .columns
            .iter()
            .find(|binding| {
                crate::catalog::identifiers_equal(&binding.name, column)
                    && table.as_deref().is_none_or(|table| {
                        binding
                            .table
                            .as_deref()
                            .is_some_and(|name| crate::catalog::identifiers_equal(name, table))
                    })
            })
            .and_then(|binding| binding.column_type),
        Expr::Unary {
            op: UnaryOp::Not, ..
        } => Some(ColumnType::Bool),
        Expr::Unary { expr, .. } | Expr::Collate { expr, .. } => {
            declared_type(expr, dataset, params)
        }
        Expr::Binary { left, op, right } => {
            if matches!(op, BinaryOp::Concat | BinaryOp::JsonExtractText) {
                return Some(ColumnType::Text);
            }
            if matches!(op, BinaryOp::Distance) {
                return Some(ColumnType::Float64);
            }
            if matches!(op, BinaryOp::JsonExtract) {
                return None;
            }
            if !matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
            ) {
                return Some(ColumnType::Bool);
            }
            let left = declared_type(left, dataset, params);
            let right = declared_type(right, dataset, params);
            if matches!(op, BinaryOp::Add | BinaryOp::Sub) {
                use ColumnType::{Date, Int64, Interval, Text, Timestamp, TimestampTz};
                match (op, left, right) {
                    (_, Some(kind @ (Timestamp | TimestampTz)), Some(Int64 | Interval | Text))
                    | (
                        BinaryOp::Add,
                        Some(Int64 | Interval | Text),
                        Some(kind @ (Timestamp | TimestampTz)),
                    ) => {
                        return Some(kind);
                    }
                    (_, Some(Date), Some(Interval))
                    | (BinaryOp::Add, Some(Interval), Some(Date))
                    | (_, Some(Text), Some(Int64 | Text))
                    | (BinaryOp::Add, Some(Int64), Some(Text)) => return Some(Timestamp),
                    _ => {}
                }
            }
            if left == Some(ColumnType::Float64) || right == Some(ColumnType::Float64) {
                Some(ColumnType::Float64)
            } else if left == Some(ColumnType::Decimal) || right == Some(ColumnType::Decimal) {
                Some(ColumnType::Decimal)
            } else {
                left.or(right)
            }
        }
        Expr::Function { name, args } => match name.strip_prefix("pg_catalog.").unwrap_or(name) {
            "pg_typeof" | "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "concat"
            | "concat_ws" | "substr" | "substring" | "version" => Some(ColumnType::Text),
            "to_timestamp" | "now" | "current_timestamp" => Some(ColumnType::TimestampTz),
            "interval" => Some(ColumnType::Interval),
            "json" | "jsonb" => Some(ColumnType::Text),
            "length" | "char_length" | "octet_length" => Some(ColumnType::Int64),
            "coalesce" | "nullif" | "greatest" | "least" | "abs" => args
                .iter()
                .find_map(|arg| declared_type(arg, dataset, params)),
            _ => None,
        },
        Expr::Aggregate { name, args, .. } => match name.as_str() {
            "count" => Some(ColumnType::Int64),
            "total" => Some(ColumnType::Float64),
            "avg" => Some(
                if args
                    .first()
                    .and_then(|arg| declared_type(arg, dataset, params))
                    == Some(ColumnType::Decimal)
                {
                    ColumnType::Decimal
                } else {
                    ColumnType::Float64
                },
            ),
            "min" | "max" | "sum" => args
                .first()
                .and_then(|arg| declared_type(arg, dataset, params)),
            _ => None,
        },
        Expr::Case {
            branches,
            else_expr,
            ..
        } => branches
            .iter()
            .find_map(|(_, result)| declared_type(result, dataset, params))
            .or_else(|| {
                else_expr
                    .as_deref()
                    .and_then(|expr| declared_type(expr, dataset, params))
            }),
        Expr::IsNull { .. }
        | Expr::Between { .. }
        | Expr::InList { .. }
        | Expr::Like { .. }
        | Expr::Exists { .. } => Some(ColumnType::Bool),
        _ => None,
    }
}
