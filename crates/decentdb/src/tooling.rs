//! Stable schema and query-contract metadata for external tooling.

use std::collections::BTreeMap;

use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::catalog::{
    identifiers_equal, ColumnSchema, ColumnType, SpatialDimensions, SpatialSubtype,
    SpatialTypeInfo, TableSchema,
};
use crate::error::{DbError, Result};
use crate::exec::{statement_is_read_only, EngineRuntime};
use crate::metadata::{
    QueryContract, QueryParameterInfo, QueryResultColumnInfo, SchemaIndexInfo, SchemaSnapshot,
    SchemaTableInfo, SchemaViewInfo, ToolingCapabilities, ToolingColumnTypeMetadata,
    ToolingMetadata, ToolingSpatialTypeInfo, ToolingTypeInfo,
};
use crate::sql::ast::{
    Assignment, BinaryOp, Expr, FromItem, InsertSource, Query, QueryBody, Select, SelectItem,
    Statement,
};
use crate::storage::DB_FORMAT_VERSION;

const TOOLING_METADATA_VERSION: u32 = 1;
const QUERY_CONTRACT_VERSION: u32 = 1;
const FINGERPRINT_ALGORITHM: &str = "sha256:decentdb-tooling-schema-v1";

#[derive(Clone, Debug)]
struct DescribedType {
    column_type: ColumnType,
    nullable: Option<bool>,
    source_table: Option<String>,
    source_column: Option<String>,
}

#[derive(Clone, Debug)]
struct ScopeColumn {
    table: Option<String>,
    name: String,
    described_type: DescribedType,
    hidden: bool,
}

#[derive(Clone, Debug, Default)]
struct QueryScope {
    columns: Vec<ScopeColumn>,
}

#[derive(Clone, Debug, Default)]
struct ParameterAccumulator {
    params: BTreeMap<usize, QueryParameterInfo>,
}

pub(crate) fn build_tooling_metadata(
    snapshot: &SchemaSnapshot,
    runtime: &EngineRuntime,
) -> Result<ToolingMetadata> {
    let column_type_metadata = tooling_column_type_metadata(runtime);
    let schema_fingerprint = schema_fingerprint(snapshot, &column_type_metadata)?;
    Ok(ToolingMetadata {
        metadata_version: TOOLING_METADATA_VERSION,
        engine_version: crate::version().to_string(),
        database_format_version: DB_FORMAT_VERSION,
        schema_cookie: runtime.catalog.schema_cookie,
        temp_schema_cookie: runtime.temp_schema_cookie,
        schema_fingerprint,
        schema_fingerprint_algorithm: FINGERPRINT_ALGORITHM.to_string(),
        schema: snapshot.clone(),
        column_type_metadata,
        capabilities: ToolingCapabilities {
            query_contract_version: QUERY_CONTRACT_VERSION,
            query_describe: true,
            deterministic_json: true,
        },
    })
}

pub(crate) fn describe_query_contract(
    sql: &str,
    statement: &Statement,
    runtime: &EngineRuntime,
    schema_fingerprint: &str,
) -> Result<QueryContract> {
    let mut diagnostics = Vec::new();
    let mut params = ParameterAccumulator::default();
    let result_columns =
        describe_statement_outputs(statement, runtime, &mut params, &mut diagnostics)?;
    collect_statement_parameters(statement, runtime, &mut params, &mut diagnostics)?;
    Ok(QueryContract {
        contract_version: QUERY_CONTRACT_VERSION,
        sql: sql.to_string(),
        statement_kind: statement_kind(statement).to_string(),
        read_only: statement_is_read_only(statement),
        schema_cookie: runtime.catalog.schema_cookie,
        temp_schema_cookie: runtime.temp_schema_cookie,
        schema_fingerprint: schema_fingerprint.to_string(),
        parameters: params.into_sorted(),
        result_columns,
        diagnostics,
    })
}

fn tooling_column_type_metadata(runtime: &EngineRuntime) -> Vec<ToolingColumnTypeMetadata> {
    let mut columns = Vec::new();
    for table in runtime.catalog.tables.values() {
        if crate::sync::is_internal_table_name(&table.name) {
            continue;
        }
        append_tooling_column_types(&mut columns, table);
    }
    for table in runtime.temp_tables.values() {
        append_tooling_column_types(&mut columns, table);
    }
    columns.sort_by(|left, right| {
        left.table_name
            .cmp(&right.table_name)
            .then_with(|| left.column_name.cmp(&right.column_name))
    });
    columns
}

fn append_tooling_column_types(out: &mut Vec<ToolingColumnTypeMetadata>, table: &TableSchema) {
    out.extend(
        table
            .columns
            .iter()
            .map(|column| ToolingColumnTypeMetadata {
                table_name: table.name.clone(),
                column_name: column.name.clone(),
                column_type: column.column_type.as_str().to_string(),
                type_info: tooling_type_info(column.column_type, column.spatial_type),
            }),
    );
}

fn tooling_type_info(column_type: ColumnType, spatial: Option<SpatialTypeInfo>) -> ToolingTypeInfo {
    ToolingTypeInfo {
        type_name: column_type.as_str().to_string(),
        value_kind: value_kind(column_type).to_string(),
        c_value_tag: c_value_tag(column_type),
        spatial: spatial.map(tooling_spatial_type_info),
    }
}

fn tooling_spatial_type_info(spatial: SpatialTypeInfo) -> ToolingSpatialTypeInfo {
    ToolingSpatialTypeInfo {
        subtype: spatial_subtype_name(spatial.subtype).to_string(),
        dimensions: spatial_dimensions_name(spatial.dimensions).to_string(),
        srid: spatial.srid,
    }
}

fn spatial_subtype_name(subtype: SpatialSubtype) -> &'static str {
    match subtype {
        SpatialSubtype::Any => "ANY",
        SpatialSubtype::Point => "POINT",
        SpatialSubtype::LineString => "LINESTRING",
        SpatialSubtype::Polygon => "POLYGON",
        SpatialSubtype::MultiPoint => "MULTIPOINT",
        SpatialSubtype::MultiLineString => "MULTILINESTRING",
        SpatialSubtype::MultiPolygon => "MULTIPOLYGON",
    }
}

fn spatial_dimensions_name(dimensions: SpatialDimensions) -> &'static str {
    match dimensions {
        SpatialDimensions::Any => "ANY",
        SpatialDimensions::Xy => "XY",
        SpatialDimensions::Xyz => "XYZ",
        SpatialDimensions::Xym => "XYM",
        SpatialDimensions::Xyzm => "XYZM",
    }
}

fn value_kind(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Int64 => "int64",
        ColumnType::Float64 => "float64",
        ColumnType::Text => "text",
        ColumnType::Bool => "bool",
        ColumnType::Blob => "blob",
        ColumnType::Decimal => "decimal",
        ColumnType::Uuid => "uuid",
        ColumnType::Timestamp => "timestamp_micros",
        ColumnType::Enum => "enum_id",
        ColumnType::IpAddr => "ipaddr",
        ColumnType::Cidr => "cidr",
        ColumnType::MacAddr => "macaddr",
        ColumnType::Date => "date_days",
        ColumnType::Time => "time_micros",
        ColumnType::TimestampTz => "timestamptz_micros",
        ColumnType::Interval => "interval",
        ColumnType::Geometry => "geometry_ewkb",
        ColumnType::Geography => "geography_ewkb",
    }
}

fn c_value_tag(column_type: ColumnType) -> u32 {
    match column_type {
        ColumnType::Int64 => 1,
        ColumnType::Float64 => 2,
        ColumnType::Bool => 3,
        ColumnType::Text => 4,
        ColumnType::Blob => 5,
        ColumnType::Decimal => 6,
        ColumnType::Uuid => 7,
        ColumnType::Timestamp => 8,
        ColumnType::Geometry => 9,
        ColumnType::Geography => 10,
        ColumnType::Enum => 11,
        ColumnType::IpAddr => 12,
        ColumnType::Cidr => 13,
        ColumnType::Date => 14,
        ColumnType::Time => 15,
        ColumnType::TimestampTz => 16,
        ColumnType::Interval => 17,
        ColumnType::MacAddr => 18,
    }
}

fn schema_fingerprint(
    snapshot: &SchemaSnapshot,
    column_type_metadata: &[ToolingColumnTypeMetadata],
) -> Result<String> {
    let payload = json!({
        "metadata_version": TOOLING_METADATA_VERSION,
        "database_format_version": DB_FORMAT_VERSION,
        "tables": snapshot.tables.iter().map(fingerprint_table).collect::<Vec<_>>(),
        "views": snapshot.views.iter().map(fingerprint_view).collect::<Vec<_>>(),
        "indexes": snapshot.indexes.iter().map(fingerprint_index).collect::<Vec<_>>(),
        "triggers": snapshot.triggers.iter().map(fingerprint_trigger).collect::<Vec<_>>(),
        "column_type_metadata": column_type_metadata,
    });
    let bytes = serde_json::to_vec(&payload).map_err(|error| {
        DbError::internal(format!("failed to serialize schema fingerprint: {error}"))
    })?;
    Ok(hex_lower(&Sha256::digest(&bytes)))
}

fn fingerprint_table(table: &SchemaTableInfo) -> JsonValue {
    json!({
        "name": table.name,
        "temporary": table.temporary,
        "ddl": table.ddl,
        "primary_key_columns": table.primary_key_columns,
        "checks": table.checks,
        "foreign_keys": table.foreign_keys,
        "columns": table.columns,
    })
}

fn fingerprint_view(view: &SchemaViewInfo) -> JsonValue {
    json!({
        "name": view.name,
        "temporary": view.temporary,
        "sql_text": view.sql_text,
        "column_names": view.column_names,
        "dependencies": view.dependencies,
        "ddl": view.ddl,
    })
}

fn fingerprint_index(index: &SchemaIndexInfo) -> JsonValue {
    json!({
        "name": index.name,
        "table_name": index.table_name,
        "kind": index.kind,
        "unique": index.unique,
        "columns": index.columns,
        "include_columns": index.include_columns,
        "predicate_sql": index.predicate_sql,
        "temporary": index.temporary,
        "ddl": index.ddl,
    })
}

fn fingerprint_trigger(trigger: &crate::metadata::SchemaTriggerInfo) -> JsonValue {
    json!({
        "name": trigger.name,
        "target_name": trigger.target_name,
        "target_kind": trigger.target_kind,
        "timing": trigger.timing,
        "events": trigger.events,
        "events_mask": trigger.events_mask,
        "for_each_row": trigger.for_each_row,
        "temporary": trigger.temporary,
        "action_sql": trigger.action_sql,
        "ddl": trigger.ddl,
    })
}

fn describe_statement_outputs(
    statement: &Statement,
    runtime: &EngineRuntime,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
) -> Result<Vec<QueryResultColumnInfo>> {
    match statement {
        Statement::Query(query) => describe_query_outputs(query, runtime, params, diagnostics),
        Statement::Explain(_) => Ok(vec![result_column(
            0,
            "plan".to_string(),
            Some(DescribedType::scalar(ColumnType::Text, false)),
            "expression",
            None,
        )]),
        Statement::Insert(insert) if !insert.returning.is_empty() => {
            let scope = table_scope(runtime, &insert.table_name, None)?;
            describe_select_items(&insert.returning, &scope, params, diagnostics)
        }
        Statement::Update(update) if !update.returning.is_empty() => {
            let scope = table_scope(runtime, &update.table_name, None)?;
            describe_select_items(&update.returning, &scope, params, diagnostics)
        }
        Statement::Delete(delete) if !delete.returning.is_empty() => {
            let scope = table_scope(runtime, &delete.table_name, None)?;
            describe_select_items(&delete.returning, &scope, params, diagnostics)
        }
        _ => Ok(Vec::new()),
    }
}

fn describe_query_outputs(
    query: &Query,
    runtime: &EngineRuntime,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
) -> Result<Vec<QueryResultColumnInfo>> {
    describe_query_body_outputs(&query.body, runtime, params, diagnostics)
}

fn describe_query_body_outputs(
    body: &QueryBody,
    runtime: &EngineRuntime,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
) -> Result<Vec<QueryResultColumnInfo>> {
    match body {
        QueryBody::Select(select) => {
            let scope = scope_for_select(select, runtime, params, diagnostics)?;
            if let Some(filter) = &select.filter {
                infer_params_from_expr(filter, &scope, params, diagnostics, None);
            }
            if let Some(having) = &select.having {
                infer_params_from_expr(having, &scope, params, diagnostics, None);
            }
            for expr in &select.group_by {
                infer_params_from_expr(expr, &scope, params, diagnostics, None);
            }
            describe_select_items(&select.projection, &scope, params, diagnostics)
        }
        QueryBody::Values(rows) => {
            let width = rows.first().map_or(0, Vec::len);
            for row in rows {
                for expr in row {
                    infer_params_from_expr(expr, &QueryScope::default(), params, diagnostics, None);
                }
            }
            Ok((0..width)
                .map(|index| {
                    let inferred = rows
                        .iter()
                        .filter_map(|row| row.get(index))
                        .find_map(|expr| {
                            infer_expr_type(expr, &QueryScope::default(), diagnostics)
                        });
                    result_column(
                        index,
                        format!("column{}", index + 1),
                        inferred,
                        "expression",
                        None,
                    )
                })
                .collect())
        }
        QueryBody::SetOperation { left, right, .. } => {
            let left_columns = describe_query_body_outputs(left, runtime, params, diagnostics)?;
            let _ = describe_query_body_outputs(right, runtime, params, diagnostics)?;
            Ok(left_columns)
        }
    }
}

fn scope_for_select(
    select: &Select,
    runtime: &EngineRuntime,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
) -> Result<QueryScope> {
    let mut scope = QueryScope::default();
    for item in &select.from {
        append_from_item_scope(&mut scope, item, runtime, params, diagnostics)?;
    }
    Ok(scope)
}

fn append_from_item_scope(
    scope: &mut QueryScope,
    item: &FromItem,
    runtime: &EngineRuntime,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
) -> Result<()> {
    match item {
        FromItem::Table { name, alias } => {
            scope
                .columns
                .extend(table_scope(runtime, name, alias.as_deref())?.columns);
        }
        FromItem::Subquery {
            query,
            alias,
            column_names,
            ..
        } => {
            let columns = describe_query_outputs(query, runtime, params, diagnostics)?;
            for (index, column) in columns.into_iter().enumerate() {
                let name = column_names
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| column.name.clone());
                scope.columns.push(ScopeColumn {
                    table: Some(alias.clone()),
                    name,
                    described_type: described_from_result_column(&column),
                    hidden: false,
                });
            }
        }
        FromItem::Function { name, alias, .. } => {
            let table = alias.clone().unwrap_or_else(|| name.clone());
            append_function_scope(scope, name, &table, diagnostics);
        }
        FromItem::Join {
            left,
            right,
            constraint,
            ..
        } => {
            append_from_item_scope(scope, left, runtime, params, diagnostics)?;
            append_from_item_scope(scope, right, runtime, params, diagnostics)?;
            match constraint {
                crate::sql::ast::JoinConstraint::On(expr) => {
                    infer_params_from_expr(expr, scope, params, diagnostics, None);
                }
                crate::sql::ast::JoinConstraint::Using(_)
                | crate::sql::ast::JoinConstraint::Natural => {}
            }
        }
    }
    Ok(())
}

fn append_function_scope(
    scope: &mut QueryScope,
    function_name: &str,
    table: &str,
    diagnostics: &mut Vec<String>,
) {
    match function_name.to_ascii_lowercase().as_str() {
        "json_each" => {
            for (name, column_type) in [
                ("key", ColumnType::Text),
                ("value", ColumnType::Text),
                ("type", ColumnType::Text),
                ("path", ColumnType::Text),
            ] {
                scope.columns.push(ScopeColumn {
                    table: Some(table.to_string()),
                    name: name.to_string(),
                    described_type: DescribedType::scalar(column_type, true),
                    hidden: false,
                });
            }
        }
        other => diagnostics.push(format!(
            "FROM function '{other}' has no stable query-contract column metadata"
        )),
    }
}

fn table_scope(
    runtime: &EngineRuntime,
    table_name: &str,
    alias: Option<&str>,
) -> Result<QueryScope> {
    let table = runtime
        .temp_tables
        .get(table_name)
        .or_else(|| runtime.catalog.table(table_name))
        .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
    let binding_name = alias.unwrap_or(&table.name).to_string();
    Ok(QueryScope {
        columns: table
            .columns
            .iter()
            .map(|column| ScopeColumn {
                table: Some(binding_name.clone()),
                name: column.name.clone(),
                described_type: described_from_column(&table.name, column),
                hidden: false,
            })
            .collect(),
    })
}

fn describe_select_items(
    items: &[SelectItem],
    scope: &QueryScope,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
) -> Result<Vec<QueryResultColumnInfo>> {
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Expr { expr, alias } => {
                infer_params_from_expr(expr, scope, params, diagnostics, None);
                let described = infer_expr_type(expr, scope, diagnostics);
                let name = alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(expr, out.len() + 1));
                out.push(result_column(
                    out.len(),
                    name,
                    described,
                    "expression",
                    Some(expr.to_sql()),
                ));
            }
            SelectItem::Wildcard => {
                for column in scope.columns.iter().filter(|column| !column.hidden) {
                    out.push(result_column(
                        out.len(),
                        column.name.clone(),
                        Some(column.described_type.clone()),
                        "catalog_column",
                        None,
                    ));
                }
            }
            SelectItem::QualifiedWildcard(table) => {
                for column in scope
                    .columns
                    .iter()
                    .filter(|column| column.table.as_deref() == Some(table.as_str()))
                {
                    out.push(result_column(
                        out.len(),
                        column.name.clone(),
                        Some(column.described_type.clone()),
                        "catalog_column",
                        None,
                    ));
                }
            }
        }
    }
    Ok(out)
}

fn collect_statement_parameters(
    statement: &Statement,
    runtime: &EngineRuntime,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
) -> Result<()> {
    match statement {
        Statement::Query(query) => {
            collect_query_parameters(query, runtime, params, diagnostics)?;
        }
        Statement::Insert(insert) => {
            let table = table_schema(runtime, &insert.table_name)?;
            let target_columns = insert_target_columns(table, &insert.columns)?;
            match &insert.source {
                InsertSource::Values(rows) => {
                    for row in rows {
                        for (index, expr) in row.iter().enumerate() {
                            let expected = target_columns
                                .get(index)
                                .map(|column| described_from_column(&table.name, column));
                            infer_params_from_expr(
                                expr,
                                &QueryScope::default(),
                                params,
                                diagnostics,
                                expected.as_ref(),
                            );
                        }
                    }
                }
                InsertSource::Query(query) => {
                    collect_query_parameters(query, runtime, params, diagnostics)?;
                }
            }
            if let Some(crate::sql::ast::ConflictAction::DoUpdate {
                assignments,
                filter,
                ..
            }) = &insert.on_conflict
            {
                collect_assignment_parameters(
                    table,
                    assignments,
                    params,
                    diagnostics,
                    Some("excluded"),
                )?;
                if let Some(filter) = filter {
                    let scope = table_scope(runtime, &insert.table_name, None)?;
                    infer_params_from_expr(filter, &scope, params, diagnostics, None);
                }
            }
        }
        Statement::Update(update) => {
            let table = table_schema(runtime, &update.table_name)?;
            collect_assignment_parameters(table, &update.assignments, params, diagnostics, None)?;
            if let Some(filter) = &update.filter {
                let scope = table_scope(runtime, &update.table_name, None)?;
                infer_params_from_expr(filter, &scope, params, diagnostics, None);
            }
        }
        Statement::Delete(delete) => {
            if let Some(filter) = &delete.filter {
                let scope = table_scope(runtime, &delete.table_name, None)?;
                infer_params_from_expr(filter, &scope, params, diagnostics, None);
            }
        }
        Statement::Explain(explain) => {
            collect_statement_parameters(&explain.statement, runtime, params, diagnostics)?;
        }
        _ => collect_params_in_statement(statement, params),
    }
    Ok(())
}

fn collect_query_parameters(
    query: &Query,
    runtime: &EngineRuntime,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
) -> Result<()> {
    let _ = describe_query_outputs(query, runtime, params, diagnostics)?;
    if let Some(limit) = &query.limit {
        infer_params_from_expr(
            limit,
            &QueryScope::default(),
            params,
            diagnostics,
            Some(&DescribedType::scalar(ColumnType::Int64, false)),
        );
    }
    if let Some(offset) = &query.offset {
        infer_params_from_expr(
            offset,
            &QueryScope::default(),
            params,
            diagnostics,
            Some(&DescribedType::scalar(ColumnType::Int64, false)),
        );
    }
    Ok(())
}

fn collect_assignment_parameters(
    table: &TableSchema,
    assignments: &[Assignment],
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
    pseudo_table: Option<&str>,
) -> Result<()> {
    let scope = QueryScope {
        columns: table
            .columns
            .iter()
            .map(|column| ScopeColumn {
                table: Some(pseudo_table.unwrap_or(&table.name).to_string()),
                name: column.name.clone(),
                described_type: described_from_column(&table.name, column),
                hidden: false,
            })
            .collect(),
    };
    for assignment in assignments {
        let Some(column) = table
            .columns
            .iter()
            .find(|column| identifiers_equal(&column.name, &assignment.column_name))
        else {
            return Err(DbError::sql(format!(
                "unknown column {} in table {}",
                assignment.column_name, table.name
            )));
        };
        let expected = described_from_column(&table.name, column);
        infer_params_from_expr(
            &assignment.expr,
            &scope,
            params,
            diagnostics,
            Some(&expected),
        );
    }
    Ok(())
}

fn infer_params_from_expr(
    expr: &Expr,
    scope: &QueryScope,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
    expected: Option<&DescribedType>,
) {
    match expr {
        Expr::Parameter(position) => params.observe(*position, expected.cloned()),
        Expr::Literal(_) | Expr::Column { .. } => {}
        Expr::Cast { expr, target_type } => {
            let cast_type = DescribedType::scalar(*target_type, true);
            infer_params_from_expr(expr, scope, params, diagnostics, Some(&cast_type));
        }
        Expr::Collate { expr, .. } => {
            infer_params_from_expr(expr, scope, params, diagnostics, expected)
        }
        Expr::Unary { expr, .. } => {
            infer_params_from_expr(expr, scope, params, diagnostics, expected)
        }
        Expr::Binary { left, op, right } => {
            let left_type = infer_expr_type(left, scope, diagnostics);
            let right_type = infer_expr_type(right, scope, diagnostics);
            let bool_type = DescribedType::scalar(ColumnType::Bool, false);
            match op {
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
                | BinaryOp::IsDistinctFrom
                | BinaryOp::IsNotDistinctFrom => {
                    infer_params_from_expr(left, scope, params, diagnostics, right_type.as_ref());
                    infer_params_from_expr(right, scope, params, diagnostics, left_type.as_ref());
                }
                BinaryOp::And | BinaryOp::Or => {
                    infer_params_from_expr(left, scope, params, diagnostics, Some(&bool_type));
                    infer_params_from_expr(right, scope, params, diagnostics, Some(&bool_type));
                }
                BinaryOp::Concat | BinaryOp::JsonExtract | BinaryOp::JsonExtractText => {
                    let text = DescribedType::scalar(ColumnType::Text, true);
                    infer_params_from_expr(left, scope, params, diagnostics, Some(&text));
                    infer_params_from_expr(right, scope, params, diagnostics, Some(&text));
                }
                BinaryOp::Distance => {
                    infer_params_from_expr(left, scope, params, diagnostics, right_type.as_ref());
                    infer_params_from_expr(right, scope, params, diagnostics, left_type.as_ref());
                }
                _ => {
                    infer_params_from_expr(left, scope, params, diagnostics, left_type.as_ref());
                    infer_params_from_expr(right, scope, params, diagnostics, right_type.as_ref());
                }
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let value_type = infer_expr_type(expr, scope, diagnostics);
            infer_params_from_expr(expr, scope, params, diagnostics, expected);
            infer_params_from_expr(low, scope, params, diagnostics, value_type.as_ref());
            infer_params_from_expr(high, scope, params, diagnostics, value_type.as_ref());
        }
        Expr::InList { expr, items, .. } => {
            let value_type = infer_expr_type(expr, scope, diagnostics);
            infer_params_from_expr(expr, scope, params, diagnostics, expected);
            for item in items {
                infer_params_from_expr(item, scope, params, diagnostics, value_type.as_ref());
            }
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            let text = DescribedType::scalar(ColumnType::Text, true);
            infer_params_from_expr(expr, scope, params, diagnostics, Some(&text));
            infer_params_from_expr(pattern, scope, params, diagnostics, Some(&text));
            if let Some(escape) = escape {
                infer_params_from_expr(escape, scope, params, diagnostics, Some(&text));
            }
        }
        Expr::IsNull { expr, .. } => infer_params_from_expr(expr, scope, params, diagnostics, None),
        Expr::Function { name, args } => {
            infer_function_params(name, args, scope, params, diagnostics)
        }
        Expr::Aggregate { args, .. } | Expr::WindowFunction { args, .. } => {
            for arg in args {
                infer_params_from_expr(arg, scope, params, diagnostics, None);
            }
        }
        Expr::RowNumber { .. } => {}
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                infer_params_from_expr(operand, scope, params, diagnostics, None);
            }
            for (condition, value) in branches {
                infer_params_from_expr(condition, scope, params, diagnostics, None);
                infer_params_from_expr(value, scope, params, diagnostics, expected);
            }
            if let Some(else_expr) = else_expr {
                infer_params_from_expr(else_expr, scope, params, diagnostics, expected);
            }
        }
        Expr::Row(items) => {
            for item in items {
                infer_params_from_expr(item, scope, params, diagnostics, None);
            }
        }
        Expr::InSubquery { expr, query, .. } | Expr::CompareSubquery { expr, query, .. } => {
            infer_params_from_expr(expr, scope, params, diagnostics, None);
            collect_params_in_query(query, params);
        }
        Expr::ScalarSubquery(query) | Expr::Exists(query) => {
            collect_params_in_query(query, params);
        }
    }
}

fn infer_function_params(
    name: &str,
    args: &[Expr],
    scope: &QueryScope,
    params: &mut ParameterAccumulator,
    diagnostics: &mut Vec<String>,
) {
    let normalized = name.to_ascii_lowercase();
    match normalized.as_str() {
        "st_dwithin" if args.len() == 3 => {
            let left = infer_expr_type(&args[0], scope, diagnostics);
            let right = infer_expr_type(&args[1], scope, diagnostics);
            infer_params_from_expr(&args[0], scope, params, diagnostics, right.as_ref());
            infer_params_from_expr(&args[1], scope, params, diagnostics, left.as_ref());
            let numeric = DescribedType::scalar(ColumnType::Float64, false);
            infer_params_from_expr(&args[2], scope, params, diagnostics, Some(&numeric));
        }
        "st_contains" | "st_within" | "st_intersects" | "st_equals" | "st_distance"
            if args.len() >= 2 =>
        {
            let left = infer_expr_type(&args[0], scope, diagnostics);
            let right = infer_expr_type(&args[1], scope, diagnostics);
            infer_params_from_expr(&args[0], scope, params, diagnostics, right.as_ref());
            infer_params_from_expr(&args[1], scope, params, diagnostics, left.as_ref());
            for arg in &args[2..] {
                infer_params_from_expr(arg, scope, params, diagnostics, None);
            }
        }
        _ => {
            for arg in args {
                infer_params_from_expr(arg, scope, params, diagnostics, None);
            }
        }
    }
}

fn infer_expr_type(
    expr: &Expr,
    scope: &QueryScope,
    diagnostics: &mut Vec<String>,
) -> Option<DescribedType> {
    match expr {
        Expr::Literal(value) => Some(DescribedType::scalar(
            value_column_type(value)?,
            value_nullable(value),
        )),
        Expr::Column { table, column } => scope.resolve(table.as_deref(), column),
        Expr::Parameter(_) => None,
        Expr::Cast { target_type, .. } => Some(DescribedType::scalar(*target_type, true)),
        Expr::Collate { expr, .. } => infer_expr_type(expr, scope, diagnostics),
        Expr::Unary { op, expr } => match op {
            crate::sql::ast::UnaryOp::Not => Some(DescribedType::scalar(ColumnType::Bool, false)),
            crate::sql::ast::UnaryOp::Negate => infer_expr_type(expr, scope, diagnostics),
        },
        Expr::Binary { left, op, right } => match op {
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::RegexMatch
            | BinaryOp::RegexMatchCaseInsensitive
            | BinaryOp::RegexNotMatch
            | BinaryOp::RegexNotMatchCaseInsensitive
            | BinaryOp::IsDistinctFrom
            | BinaryOp::IsNotDistinctFrom => Some(DescribedType::scalar(ColumnType::Bool, false)),
            BinaryOp::Concat | BinaryOp::JsonExtractText => {
                Some(DescribedType::scalar(ColumnType::Text, true))
            }
            BinaryOp::JsonExtract => Some(DescribedType::scalar(ColumnType::Text, true)),
            BinaryOp::Distance | BinaryOp::Div => {
                Some(DescribedType::scalar(ColumnType::Float64, true))
            }
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Mod => numeric_result_type(
                infer_expr_type(left, scope, diagnostics),
                infer_expr_type(right, scope, diagnostics),
            ),
        },
        Expr::Between { .. }
        | Expr::InList { .. }
        | Expr::Like { .. }
        | Expr::IsNull { .. }
        | Expr::Exists(_) => Some(DescribedType::scalar(ColumnType::Bool, false)),
        Expr::Function { name, args } => infer_function_type(name, args, scope, diagnostics),
        Expr::Aggregate { name, .. } | Expr::WindowFunction { name, .. } => {
            infer_aggregate_type(name)
        }
        Expr::RowNumber { .. } => Some(DescribedType::scalar(ColumnType::Int64, false)),
        Expr::Case {
            branches,
            else_expr,
            ..
        } => branches
            .iter()
            .filter_map(|(_, expr)| infer_expr_type(expr, scope, diagnostics))
            .next()
            .or_else(|| {
                else_expr
                    .as_ref()
                    .and_then(|expr| infer_expr_type(expr, scope, diagnostics))
            }),
        Expr::ScalarSubquery(query) => {
            diagnostics.push(format!(
                "scalar subquery '{}' has unknown result type in query-contract metadata",
                query.to_sql()
            ));
            None
        }
        Expr::Row(_) | Expr::InSubquery { .. } | Expr::CompareSubquery { .. } => None,
    }
}

fn infer_function_type(
    name: &str,
    args: &[Expr],
    scope: &QueryScope,
    diagnostics: &mut Vec<String>,
) -> Option<DescribedType> {
    let normalized = name.to_ascii_lowercase();
    match normalized.as_str() {
        "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "substr" | "substring" | "replace"
        | "printf" | "format" | "hex" | "sha256" | "md5" | "uuid" | "st_astext"
        | "st_asgeojson" | "st_geometrytype" => Some(DescribedType::scalar(ColumnType::Text, true)),
        "length" | "json_array_length" | "st_srid" => {
            Some(DescribedType::scalar(ColumnType::Int64, true))
        }
        "abs" | "round" | "ceil" | "ceiling" | "floor" => args
            .first()
            .and_then(|expr| infer_expr_type(expr, scope, diagnostics))
            .or_else(|| Some(DescribedType::scalar(ColumnType::Float64, true))),
        "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2" | "sqrt" | "pow" | "power"
        | "radians" | "degrees" | "st_distance" | "st_length" | "st_area" => {
            Some(DescribedType::scalar(ColumnType::Float64, true))
        }
        "coalesce" => args
            .iter()
            .filter_map(|expr| infer_expr_type(expr, scope, diagnostics))
            .next(),
        "st_dwithin" | "st_intersects" | "st_contains" | "st_within" | "st_equals"
        | "st_isvalid" => Some(DescribedType::scalar(ColumnType::Bool, true)),
        "st_asbinary" => Some(DescribedType::scalar(ColumnType::Blob, true)),
        "st_geogpoint" | "st_geogpointz" | "st_geogpointm" | "st_geogpointzm"
        | "st_geogfromwkb" | "st_geogfromtext" | "st_geogfromgeojson" => {
            Some(DescribedType::scalar(ColumnType::Geography, false))
        }
        "st_point" | "st_pointz" | "st_pointm" | "st_pointzm" | "st_geomfromwkb"
        | "st_geomfromtext" | "st_geomfromgeojson" => {
            Some(DescribedType::scalar(ColumnType::Geometry, false))
        }
        "st_setsrid" => args
            .first()
            .and_then(|expr| infer_expr_type(expr, scope, diagnostics)),
        _ => {
            diagnostics.push(format!(
                "function '{name}' has unknown result type in query-contract metadata"
            ));
            None
        }
    }
}

fn infer_aggregate_type(name: &str) -> Option<DescribedType> {
    match name.to_ascii_lowercase().as_str() {
        "count" | "row_number" | "rank" | "dense_rank" => {
            Some(DescribedType::scalar(ColumnType::Int64, false))
        }
        "sum" | "avg" | "min" | "max" | "median" | "percentile_cont" | "percentile_disc" => {
            Some(DescribedType::scalar(ColumnType::Float64, true))
        }
        "string_agg" => Some(DescribedType::scalar(ColumnType::Text, true)),
        _ => None,
    }
}

fn numeric_result_type(
    left: Option<DescribedType>,
    right: Option<DescribedType>,
) -> Option<DescribedType> {
    let left_type = left.as_ref().map(|value| value.column_type);
    let right_type = right.as_ref().map(|value| value.column_type);
    if matches!(left_type, Some(ColumnType::Float64))
        || matches!(right_type, Some(ColumnType::Float64))
    {
        Some(DescribedType::scalar(ColumnType::Float64, true))
    } else if matches!(left_type, Some(ColumnType::Decimal))
        || matches!(right_type, Some(ColumnType::Decimal))
    {
        Some(DescribedType::scalar(ColumnType::Decimal, true))
    } else if left_type.is_some() || right_type.is_some() {
        Some(DescribedType::scalar(ColumnType::Int64, true))
    } else {
        None
    }
}

fn value_column_type(value: &crate::record::value::Value) -> Option<ColumnType> {
    match value {
        crate::record::value::Value::Null => None,
        crate::record::value::Value::Int64(_) => Some(ColumnType::Int64),
        crate::record::value::Value::Float64(_) => Some(ColumnType::Float64),
        crate::record::value::Value::Text(_) => Some(ColumnType::Text),
        crate::record::value::Value::Bool(_) => Some(ColumnType::Bool),
        crate::record::value::Value::Blob(_) => Some(ColumnType::Blob),
        crate::record::value::Value::Decimal { .. } => Some(ColumnType::Decimal),
        crate::record::value::Value::Uuid(_) => Some(ColumnType::Uuid),
        crate::record::value::Value::TimestampMicros(_) => Some(ColumnType::Timestamp),
        crate::record::value::Value::Enum { .. } => Some(ColumnType::Enum),
        crate::record::value::Value::IpAddr { .. } => Some(ColumnType::IpAddr),
        crate::record::value::Value::Cidr { .. } => Some(ColumnType::Cidr),
        crate::record::value::Value::MacAddr { .. } => Some(ColumnType::MacAddr),
        crate::record::value::Value::DateDays(_) => Some(ColumnType::Date),
        crate::record::value::Value::TimeMicros(_) => Some(ColumnType::Time),
        crate::record::value::Value::TimestampTzMicros(_) => Some(ColumnType::TimestampTz),
        crate::record::value::Value::Interval { .. } => Some(ColumnType::Interval),
        crate::record::value::Value::Geometry(_) => Some(ColumnType::Geometry),
        crate::record::value::Value::Geography(_) => Some(ColumnType::Geography),
    }
}

fn value_nullable(value: &crate::record::value::Value) -> bool {
    matches!(value, crate::record::value::Value::Null)
}

fn described_from_column(table_name: &str, column: &ColumnSchema) -> DescribedType {
    DescribedType {
        column_type: column.column_type,
        nullable: Some(column.nullable),
        source_table: Some(table_name.to_string()),
        source_column: Some(column.name.clone()),
    }
}

fn described_from_result_column(column: &QueryResultColumnInfo) -> DescribedType {
    DescribedType {
        column_type: column
            .type_name
            .as_deref()
            .and_then(column_type_from_name)
            .unwrap_or(ColumnType::Text),
        nullable: column.nullable,
        source_table: column.source_table.clone(),
        source_column: column.source_column.clone(),
    }
}

fn column_type_from_name(name: &str) -> Option<ColumnType> {
    match name.to_ascii_uppercase().as_str() {
        "INT" | "INTEGER" | "INT64" => Some(ColumnType::Int64),
        "FLOAT" | "FLOAT64" | "REAL" | "DOUBLE" => Some(ColumnType::Float64),
        "TEXT" | "STRING" => Some(ColumnType::Text),
        "BOOL" | "BOOLEAN" => Some(ColumnType::Bool),
        "BLOB" => Some(ColumnType::Blob),
        "DECIMAL" => Some(ColumnType::Decimal),
        "UUID" => Some(ColumnType::Uuid),
        "TIMESTAMP" => Some(ColumnType::Timestamp),
        "ENUM" => Some(ColumnType::Enum),
        "IPADDR" | "INET" => Some(ColumnType::IpAddr),
        "CIDR" => Some(ColumnType::Cidr),
        "MACADDR" | "MACADDR8" => Some(ColumnType::MacAddr),
        "DATE" => Some(ColumnType::Date),
        "TIME" => Some(ColumnType::Time),
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Some(ColumnType::TimestampTz),
        "INTERVAL" => Some(ColumnType::Interval),
        "GEOMETRY" => Some(ColumnType::Geometry),
        "GEOGRAPHY" => Some(ColumnType::Geography),
        _ => None,
    }
}

fn table_schema<'a>(runtime: &'a EngineRuntime, table_name: &str) -> Result<&'a TableSchema> {
    runtime
        .temp_tables
        .get(table_name)
        .or_else(|| runtime.catalog.table(table_name))
        .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))
}

fn insert_target_columns<'a>(
    table: &'a TableSchema,
    names: &[String],
) -> Result<Vec<&'a ColumnSchema>> {
    if names.is_empty() {
        return Ok(table.columns.iter().collect());
    }
    names
        .iter()
        .map(|name| {
            table
                .columns
                .iter()
                .find(|column| identifiers_equal(&column.name, name))
                .ok_or_else(|| {
                    DbError::sql(format!("unknown column {name} in table {}", table.name))
                })
        })
        .collect()
}

fn result_column(
    ordinal: usize,
    name: String,
    described: Option<DescribedType>,
    source: &str,
    expression_sql: Option<String>,
) -> QueryResultColumnInfo {
    QueryResultColumnInfo {
        ordinal,
        name,
        type_name: described
            .as_ref()
            .map(|described| described.column_type.as_str().to_string()),
        nullable: described.as_ref().and_then(|described| described.nullable),
        source: source.to_string(),
        source_table: described
            .as_ref()
            .and_then(|described| described.source_table.clone()),
        source_column: described
            .as_ref()
            .and_then(|described| described.source_column.clone()),
        expression_sql,
        diagnostics: if described.is_some() {
            Vec::new()
        } else {
            vec!["type could not be inferred without execution".to_string()]
        },
    }
}

fn infer_expr_name(expr: &Expr, ordinal: usize) -> String {
    match expr {
        Expr::Column { column, .. } => column.clone(),
        Expr::Aggregate { name, .. } | Expr::Function { name, .. } => name.clone(),
        Expr::RowNumber { .. } => "row_number".to_string(),
        Expr::WindowFunction { name, .. } => name.clone(),
        _ => format!("col{ordinal}"),
    }
}

fn statement_kind(statement: &Statement) -> &'static str {
    match statement {
        Statement::Query(_) => "query",
        Statement::Explain(_) => "explain",
        Statement::Insert(_) => "insert",
        Statement::Update(_) => "update",
        Statement::Delete(_) => "delete",
        Statement::Analyze { .. } => "analyze",
        Statement::CreateTable(_) => "create_table",
        Statement::CreateTableAs(_) => "create_table_as",
        Statement::CreateSchema { .. } => "create_schema",
        Statement::CreateIndex(_) => "create_index",
        Statement::CreateView(_) => "create_view",
        Statement::CreateTrigger(_) => "create_trigger",
        Statement::DropTable { .. } => "drop_table",
        Statement::DropIndex { .. } => "drop_index",
        Statement::DropView { .. } => "drop_view",
        Statement::DropTrigger { .. } => "drop_trigger",
        Statement::AlterViewRename { .. } => "alter_view_rename",
        Statement::AlterTable { .. } => "alter_table",
        Statement::TruncateTable { .. } => "truncate_table",
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

impl DescribedType {
    fn scalar(column_type: ColumnType, nullable: bool) -> Self {
        Self {
            column_type,
            nullable: Some(nullable),
            source_table: None,
            source_column: None,
        }
    }
}

impl QueryScope {
    fn resolve(&self, table: Option<&str>, name: &str) -> Option<DescribedType> {
        self.columns
            .iter()
            .filter(|column| {
                identifiers_equal(&column.name, name)
                    && table.is_none_or(|table| {
                        column
                            .table
                            .as_deref()
                            .is_some_and(|column_table| identifiers_equal(column_table, table))
                    })
            })
            .map(|column| column.described_type.clone())
            .next()
    }
}

impl ParameterAccumulator {
    fn observe(&mut self, position: usize, described: Option<DescribedType>) {
        let entry = self
            .params
            .entry(position)
            .or_insert_with(|| QueryParameterInfo {
                position,
                name: format!("${position}"),
                type_name: None,
                nullable: None,
                source: "unknown".to_string(),
                source_table: None,
                source_column: None,
                diagnostics: vec!["parameter type could not be inferred from context".to_string()],
            });
        if let Some(described) = described {
            let type_name = described.column_type.as_str().to_string();
            if entry.type_name.as_deref() == Some(type_name.as_str()) {
                return;
            }
            if entry.type_name.is_some() {
                entry.diagnostics.push(format!(
                    "parameter has multiple inferred type contexts; keeping {}",
                    entry.type_name.as_deref().unwrap_or("unknown")
                ));
                return;
            }
            entry.type_name = Some(type_name);
            entry.nullable = described.nullable;
            entry.source = if described.source_column.is_some() {
                "catalog_column".to_string()
            } else {
                "expression_context".to_string()
            };
            entry.source_table = described.source_table;
            entry.source_column = described.source_column;
            entry.diagnostics.clear();
        }
    }

    fn into_sorted(self) -> Vec<QueryParameterInfo> {
        self.params.into_values().collect()
    }
}

fn collect_params_in_statement(statement: &Statement, params: &mut ParameterAccumulator) {
    match statement {
        Statement::Query(query) => collect_params_in_query(query, params),
        Statement::Explain(explain) => collect_params_in_statement(&explain.statement, params),
        Statement::Insert(insert) => {
            match &insert.source {
                InsertSource::Values(rows) => {
                    for row in rows {
                        for expr in row {
                            collect_params_in_expr(expr, params);
                        }
                    }
                }
                InsertSource::Query(query) => collect_params_in_query(query, params),
            }
            for item in &insert.returning {
                collect_params_in_select_item(item, params);
            }
        }
        Statement::Update(update) => {
            for assignment in &update.assignments {
                collect_params_in_expr(&assignment.expr, params);
            }
            if let Some(filter) = &update.filter {
                collect_params_in_expr(filter, params);
            }
            for item in &update.returning {
                collect_params_in_select_item(item, params);
            }
        }
        Statement::Delete(delete) => {
            if let Some(filter) = &delete.filter {
                collect_params_in_expr(filter, params);
            }
            for item in &delete.returning {
                collect_params_in_select_item(item, params);
            }
        }
        _ => {}
    }
}

fn collect_params_in_query(query: &Query, params: &mut ParameterAccumulator) {
    for cte in &query.ctes {
        collect_params_in_query(&cte.query, params);
    }
    collect_params_in_query_body(&query.body, params);
    for order in &query.order_by {
        collect_params_in_expr(&order.expr, params);
    }
    if let Some(limit) = &query.limit {
        collect_params_in_expr(limit, params);
    }
    if let Some(offset) = &query.offset {
        collect_params_in_expr(offset, params);
    }
}

fn collect_params_in_query_body(body: &QueryBody, params: &mut ParameterAccumulator) {
    match body {
        QueryBody::Select(select) => {
            for item in &select.projection {
                collect_params_in_select_item(item, params);
            }
            for from in &select.from {
                collect_params_in_from_item(from, params);
            }
            if let Some(filter) = &select.filter {
                collect_params_in_expr(filter, params);
            }
            for expr in &select.group_by {
                collect_params_in_expr(expr, params);
            }
            if let Some(having) = &select.having {
                collect_params_in_expr(having, params);
            }
        }
        QueryBody::Values(rows) => {
            for row in rows {
                for expr in row {
                    collect_params_in_expr(expr, params);
                }
            }
        }
        QueryBody::SetOperation { left, right, .. } => {
            collect_params_in_query_body(left, params);
            collect_params_in_query_body(right, params);
        }
    }
}

fn collect_params_in_select_item(item: &SelectItem, params: &mut ParameterAccumulator) {
    if let SelectItem::Expr { expr, .. } = item {
        collect_params_in_expr(expr, params);
    }
}

fn collect_params_in_from_item(item: &FromItem, params: &mut ParameterAccumulator) {
    match item {
        FromItem::Subquery { query, .. } => collect_params_in_query(query, params),
        FromItem::Function { args, .. } => {
            for arg in args {
                collect_params_in_expr(arg, params);
            }
        }
        FromItem::Join {
            left,
            right,
            constraint,
            ..
        } => {
            collect_params_in_from_item(left, params);
            collect_params_in_from_item(right, params);
            if let crate::sql::ast::JoinConstraint::On(expr) = constraint {
                collect_params_in_expr(expr, params);
            }
        }
        FromItem::Table { .. } => {}
    }
}

fn collect_params_in_expr(expr: &Expr, params: &mut ParameterAccumulator) {
    match expr {
        Expr::Parameter(position) => params.observe(*position, None),
        Expr::Literal(_) | Expr::Column { .. } | Expr::RowNumber { .. } => {}
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::Collate { expr, .. } => {
            collect_params_in_expr(expr, params)
        }
        Expr::Binary { left, right, .. } => {
            collect_params_in_expr(left, params);
            collect_params_in_expr(right, params);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_params_in_expr(expr, params);
            collect_params_in_expr(low, params);
            collect_params_in_expr(high, params);
        }
        Expr::InList { expr, items, .. } => {
            collect_params_in_expr(expr, params);
            for item in items {
                collect_params_in_expr(item, params);
            }
        }
        Expr::InSubquery { expr, query, .. } | Expr::CompareSubquery { expr, query, .. } => {
            collect_params_in_expr(expr, params);
            collect_params_in_query(query, params);
        }
        Expr::ScalarSubquery(query) | Expr::Exists(query) => collect_params_in_query(query, params),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_params_in_expr(expr, params);
            collect_params_in_expr(pattern, params);
            if let Some(escape) = escape {
                collect_params_in_expr(escape, params);
            }
        }
        Expr::IsNull { expr, .. } => collect_params_in_expr(expr, params),
        Expr::Function { args, .. }
        | Expr::Aggregate { args, .. }
        | Expr::WindowFunction { args, .. } => {
            for arg in args {
                collect_params_in_expr(arg, params);
            }
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_params_in_expr(operand, params);
            }
            for (condition, value) in branches {
                collect_params_in_expr(condition, params);
                collect_params_in_expr(value, params);
            }
            if let Some(else_expr) = else_expr {
                collect_params_in_expr(else_expr, params);
            }
        }
        Expr::Row(items) => {
            for item in items {
                collect_params_in_expr(item, params);
            }
        }
    }
}
