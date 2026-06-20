use super::*;

pub(super) fn infer_expr_name(expr: &Expr, ordinal: usize) -> String {
    match expr {
        Expr::Column { column, .. } => column.clone(),
        Expr::Collate { expr, .. } => infer_expr_name(expr, ordinal),
        Expr::RowNumber { .. } => "row_number".to_string(),
        Expr::WindowFunction { name, .. } => name.clone(),
        _ => format!("col{ordinal}"),
    }
}

pub(super) enum NumericAgg {
    Sum,
    Avg,
    Total,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum VarianceAgg {
    StddevSamp,
    StddevPop,
    VarSamp,
    VarPop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BoolAgg {
    And,
    Or,
}

pub(super) struct AggregateEvalContext<'a> {
    pub(super) runtime: &'a EngineRuntime,
    pub(super) dataset: &'a Dataset,
    pub(super) params: &'a [Value],
    pub(super) ctes: &'a BTreeMap<String, Dataset>,
}

impl AggregateEvalContext<'_> {
    fn eval_row(&self, row: &[Value], expr: &Expr) -> Result<Value> {
        self.runtime
            .eval_expr(expr, self.dataset, row, self.params, self.ctes, None)
    }
}

pub(super) fn aggregate_numeric(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    kind: NumericAgg,
    distinct: bool,
) -> Result<Value> {
    let mut total_int = 0_i64;
    let mut total_float = 0_f64;
    let mut saw_float = false;
    let mut count = 0_i64;

    if distinct {
        let mut vals = Vec::new();
        for row_index in row_indexes {
            let row = ctx
                .dataset
                .rows
                .get(*row_index)
                .map(Vec::as_slice)
                .ok_or_else(|| DbError::internal("group row index is invalid"))?;
            let val = ctx.eval_row(row, expr)?;
            if !matches!(val, Value::Null) {
                vals.push(val);
            }
        }
        vals.sort_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        vals.dedup_by(|a, b| {
            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal) == std::cmp::Ordering::Equal
        });
        for val in vals {
            match val {
                Value::Int64(value) => {
                    total_int += value;
                    total_float += value as f64;
                    count += 1;
                }
                Value::Float64(value) => {
                    total_float += value;
                    saw_float = true;
                    count += 1;
                }
                Value::Decimal { scaled, scale } => {
                    total_float += (scaled as f64) / 10_f64.powi(i32::from(scale));
                    saw_float = true;
                    count += 1;
                }
                other => {
                    return Err(DbError::sql(format!(
                        "numeric aggregate does not support {other:?}"
                    )))
                }
            }
        }
    } else {
        for row_index in row_indexes {
            let row = ctx
                .dataset
                .rows
                .get(*row_index)
                .map(Vec::as_slice)
                .ok_or_else(|| DbError::internal("group row index is invalid"))?;
            match ctx.eval_row(row, expr)? {
                Value::Null => {}
                Value::Int64(value) => {
                    total_int += value;
                    total_float += value as f64;
                    count += 1;
                }
                Value::Float64(value) => {
                    total_float += value;
                    saw_float = true;
                    count += 1;
                }
                Value::Decimal { scaled, scale } => {
                    total_float += (scaled as f64) / 10_f64.powi(i32::from(scale));
                    saw_float = true;
                    count += 1;
                }
                other => {
                    return Err(DbError::sql(format!(
                        "numeric aggregate does not support {other:?}"
                    )))
                }
            }
        }
    }
    if count == 0 {
        return Ok(match kind {
            NumericAgg::Total => Value::Float64(0.0),
            NumericAgg::Sum | NumericAgg::Avg => Value::Null,
        });
    }
    Ok(match kind {
        NumericAgg::Sum if saw_float => Value::Float64(total_float),
        NumericAgg::Sum => Value::Int64(total_int),
        NumericAgg::Avg => Value::Float64(total_float / count as f64),
        NumericAgg::Total => Value::Float64(total_float),
    })
}

pub(super) fn aggregate_variance(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    kind: VarianceAgg,
    distinct: bool,
) -> Result<Value> {
    let mut values = Vec::new();
    for row_index in row_indexes {
        let row = ctx
            .dataset
            .rows
            .get(*row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = ctx.eval_row(row, expr)?;
        if !matches!(value, Value::Null) {
            values.push(value);
        }
    }

    if distinct {
        values.sort_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        values.dedup_by(|a, b| {
            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal) == std::cmp::Ordering::Equal
        });
    }

    let mut count = 0_u64;
    let mut mean = 0.0_f64;
    let mut m2 = 0.0_f64;
    for value in values {
        let number = match value {
            Value::Int64(value) => value as f64,
            Value::Float64(value) => value,
            Value::Decimal { scaled, scale } => (scaled as f64) / 10_f64.powi(i32::from(scale)),
            other => {
                return Err(DbError::sql(format!(
                    "variance aggregate does not support {other:?}"
                )));
            }
        };
        count += 1;
        let delta = number - mean;
        mean += delta / (count as f64);
        let delta2 = number - mean;
        m2 += delta * delta2;
    }

    if count == 0 {
        return Ok(Value::Null);
    }

    let denominator = match kind {
        VarianceAgg::StddevPop | VarianceAgg::VarPop => count as f64,
        VarianceAgg::StddevSamp | VarianceAgg::VarSamp => {
            if count < 2 {
                return Ok(Value::Null);
            }
            (count - 1) as f64
        }
    };
    let variance = m2 / denominator;
    Ok(match kind {
        VarianceAgg::StddevSamp | VarianceAgg::StddevPop => Value::Float64(variance.sqrt()),
        VarianceAgg::VarSamp | VarianceAgg::VarPop => Value::Float64(variance),
    })
}

pub(super) fn aggregate_bool(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    kind: BoolAgg,
    distinct: bool,
) -> Result<Value> {
    let mut values = Vec::new();
    for row_index in row_indexes {
        let row = ctx
            .dataset
            .rows
            .get(*row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = ctx.eval_row(row, expr)?;
        if !matches!(value, Value::Null) {
            values.push(value);
        }
    }

    if distinct {
        values.sort_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
        values.dedup_by(|a, b| {
            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal) == std::cmp::Ordering::Equal
        });
    }

    let mut saw_non_null = false;
    let mut result = match kind {
        BoolAgg::And => true,
        BoolAgg::Or => false,
    };

    for value in values {
        let boolean = match value {
            Value::Bool(value) => value,
            other => {
                return Err(DbError::sql(format!(
                    "boolean aggregate does not support {other:?}"
                )))
            }
        };
        saw_non_null = true;
        match kind {
            BoolAgg::And => {
                result &= boolean;
                if !result {
                    break;
                }
            }
            BoolAgg::Or => {
                result |= boolean;
                if result {
                    break;
                }
            }
        }
    }

    if saw_non_null {
        Ok(Value::Bool(result))
    } else {
        Ok(Value::Null)
    }
}

pub(super) fn aggregate_extreme(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    row_indexes: &[usize],
    expr: &Expr,
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
    want_min: bool,
) -> Result<Value> {
    let mut current: Option<Value> = None;
    for row_index in row_indexes {
        let row = dataset
            .rows
            .get(*row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = runtime.eval_expr(expr, dataset, row, params, ctes, None)?;
        if matches!(value, Value::Null) {
            continue;
        }
        match &current {
            Some(existing) => {
                let ordering = compare_values(&value, existing)?;
                if (want_min && ordering == std::cmp::Ordering::Less)
                    || (!want_min && ordering == std::cmp::Ordering::Greater)
                {
                    current = Some(value);
                }
            }
            None => current = Some(value),
        }
    }
    Ok(current.unwrap_or(Value::Null))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_fulltext_match(
    runtime: &EngineRuntime,
    args: &[Expr],
    dataset: &Dataset,
    row: &[Value],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
    excluded: Option<&Dataset>,
) -> Result<Value> {
    if args.len() != 2 {
        return Err(DbError::sql("FULLTEXT_MATCH expects 2 arguments"));
    }
    let index_value = runtime.eval_expr(&args[0], dataset, row, params, ctes, excluded)?;
    let query_value = runtime.eval_expr(&args[1], dataset, row, params, ctes, excluded)?;
    let Some(index_name) = expect_text_arg("FULLTEXT_MATCH", "first", &index_value)? else {
        return Ok(Value::Null);
    };
    let Some(query_text) = expect_text_arg("FULLTEXT_MATCH", "second", &query_value)? else {
        return Ok(Value::Null);
    };
    let (index_schema, fulltext) = fulltext_runtime_index(runtime, index_name)?;
    let row_id = resolve_fulltext_row_id(dataset, row, index_schema)?;
    let row_id_u64 = u64::try_from(row_id).map_err(|_| {
        DbError::internal(format!(
            "fulltext index {} received negative row id {row_id}",
            index_schema.name
        ))
    })?;
    let matched = fulltext
        .matches_query(row_id_u64, query_text)
        .map_err(|error| DbError::sql(error.message))?;
    let mut context = runtime
        .fts_eval_context
        .lock()
        .map_err(|_| DbError::internal("fulltext evaluation context lock poisoned"))?;
    let key = (index_schema.name.clone(), row_id);
    if matched {
        let score = fulltext
            .score_query(row_id_u64, query_text)
            .map_err(|error| DbError::sql(error.message))?;
        context.scores.insert(key, score);
    } else {
        context.scores.remove(&key);
    }
    Ok(Value::Bool(matched))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_bm25(
    runtime: &EngineRuntime,
    args: &[Expr],
    dataset: &Dataset,
    row: &[Value],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
    excluded: Option<&Dataset>,
) -> Result<Value> {
    if args.len() != 1 {
        return Err(DbError::sql("BM25 expects 1 argument"));
    }
    let index_value = runtime.eval_expr(&args[0], dataset, row, params, ctes, excluded)?;
    let Some(index_name) = expect_text_arg("BM25", "first", &index_value)? else {
        return Ok(Value::Null);
    };
    let (index_schema, _) = fulltext_runtime_index(runtime, index_name)?;
    let Some(row_id_column_index) = fulltext_row_id_column_index(dataset, index_schema)? else {
        return Err(DbError::sql(format!(
            "{FTS_SEMANTIC_ERROR_PREFIX} bm25 requires fulltext_match in the same query block"
        )));
    };
    let row_id = match row.get(row_id_column_index) {
        Some(Value::Int64(row_id)) => *row_id,
        Some(other) => {
            return Err(DbError::internal(format!(
                "fulltext hidden row id has unexpected value {other:?}"
            )));
        }
        None => return Err(DbError::internal("row is shorter than fulltext bindings")),
    };
    let context = runtime
        .fts_eval_context
        .lock()
        .map_err(|_| DbError::internal("fulltext evaluation context lock poisoned"))?;
    context
        .scores
        .get(&(index_schema.name.clone(), row_id))
        .copied()
        .map(Value::Float64)
        .ok_or_else(|| {
            DbError::sql(format!(
                "{FTS_SEMANTIC_ERROR_PREFIX} bm25 requires fulltext_match in the same query block"
            ))
        })
}

pub(super) fn fulltext_runtime_index<'a>(
    runtime: &'a EngineRuntime,
    index_name: &str,
) -> Result<(&'a IndexSchema, &'a FullTextIndex)> {
    let index_schema = runtime.catalog.index(index_name).ok_or_else(|| {
        DbError::sql(format!(
            "{FTS_SEMANTIC_ERROR_PREFIX} unknown fulltext index {index_name}"
        ))
    })?;
    if index_schema.kind != IndexKind::FullText {
        return Err(DbError::sql(format!(
            "{FTS_SEMANTIC_ERROR_PREFIX} index {index_name} is not a fulltext index"
        )));
    }
    if !index_schema.fresh {
        return Err(DbError::sql(format!(
            "{FTS_SEMANTIC_ERROR_PREFIX} fulltext index {index_name} is stale"
        )));
    }
    let Some(RuntimeIndex::FullText { index }) = runtime.index(&index_schema.name) else {
        return Err(DbError::sql(format!(
            "{FTS_SEMANTIC_ERROR_PREFIX} runtime fulltext index {index_name} is missing"
        )));
    };
    Ok((index_schema, index))
}

pub(super) fn resolve_fulltext_row_id(
    dataset: &Dataset,
    row: &[Value],
    index_schema: &IndexSchema,
) -> Result<i64> {
    let column_index = fulltext_row_id_column_index(dataset, index_schema)?.ok_or_else(|| {
        DbError::sql(format!(
            "{FTS_SEMANTIC_ERROR_PREFIX} fulltext index {} table is not in query scope",
            index_schema.name
        ))
    })?;
    match row.get(column_index) {
        Some(Value::Int64(row_id)) => Ok(*row_id),
        Some(other) => Err(DbError::internal(format!(
            "fulltext hidden row id has unexpected value {other:?}"
        ))),
        None => Err(DbError::internal("row is shorter than its bindings")),
    }
}

pub(super) fn fulltext_row_id_column_index(
    dataset: &Dataset,
    index_schema: &IndexSchema,
) -> Result<Option<usize>> {
    let mut matched_index = None;
    for (column_index, binding) in dataset.columns.iter().enumerate() {
        if !binding.hidden || !identifiers_equal(&binding.name, FTS_HIDDEN_ROW_ID_COLUMN) {
            continue;
        }
        if !binding
            .source_table
            .as_deref()
            .is_some_and(|source| identifiers_equal(source, &index_schema.table_name))
        {
            continue;
        }
        if matched_index.replace(column_index).is_some() {
            return Err(DbError::sql(format!(
                "{FTS_SEMANTIC_ERROR_PREFIX} fulltext index {} matches more than one table instance in this query",
                index_schema.name
            )));
        }
    }
    Ok(matched_index)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn eval_function(
    runtime: &EngineRuntime,
    name: &str,
    args: &[Expr],
    dataset: &Dataset,
    row: &[Value],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
    excluded: Option<&Dataset>,
) -> Result<Value> {
    if name.eq_ignore_ascii_case("fulltext_match") {
        return eval_fulltext_match(runtime, args, dataset, row, params, ctes, excluded);
    }
    if name.eq_ignore_ascii_case("bm25") {
        return eval_bm25(runtime, args, dataset, row, params, ctes, excluded);
    }

    let values = args
        .iter()
        .map(|expr| runtime.eval_expr(expr, dataset, row, params, ctes, excluded))
        .collect::<Result<Vec<_>>>()?;
    match name {
        "current_database" | "current_schema" | "database" | "schema" => {
            if !values.is_empty() {
                return Err(DbError::sql(format!(
                    "{} expects 0 arguments",
                    name.to_ascii_uppercase()
                )));
            }
            Ok(Value::Text("main".to_string()))
        }
        "current_audit_context" => {
            if values.len() != 1 {
                return Err(DbError::sql("CURRENT_AUDIT_CONTEXT expects 1 argument"));
            }
            let Some(key) = expect_text_arg("CURRENT_AUDIT_CONTEXT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(runtime
                .audit_context
                .lock()
                .map_err(|_| DbError::internal("audit context lock poisoned"))?
                .get(key)
                .unwrap_or(Value::Null))
        }
        "current_tenant" => {
            if !values.is_empty() {
                return Err(DbError::sql("CURRENT_TENANT expects 0 arguments"));
            }
            Ok(runtime
                .audit_context
                .lock()
                .map_err(|_| DbError::internal("audit context lock poisoned"))?
                .tenant()
                .unwrap_or(Value::Null))
        }
        "current_actor" => {
            if !values.is_empty() {
                return Err(DbError::sql("CURRENT_ACTOR expects 0 arguments"));
            }
            Ok(runtime
                .audit_context
                .lock()
                .map_err(|_| DbError::internal("audit context lock poisoned"))?
                .actor()
                .unwrap_or(Value::Null))
        }
        "version" => {
            if !values.is_empty() {
                return Err(DbError::sql("VERSION expects 0 arguments"));
            }
            Ok(Value::Text(format!(
                "DecentDB {}",
                env!("CARGO_PKG_VERSION")
            )))
        }
        "sqlite_version" => Err(DbError::sql(
            "sqlite_version() is not supported; DecentDB is not SQLite",
        )),
        "pg_backend_pid" => Err(DbError::sql(
            "pg_backend_pid() is not supported; DecentDB is embedded and has no server backend PID",
        )),
        "coalesce" => Ok(values
            .into_iter()
            .find(|value| !matches!(value, Value::Null))
            .unwrap_or(Value::Null)),
        "nullif" => {
            if values.len() != 2 {
                return Err(DbError::sql("NULLIF expects two arguments"));
            }
            if compare_values(&values[0], &values[1])? == std::cmp::Ordering::Equal {
                Ok(Value::Null)
            } else {
                Ok(values[0].clone())
            }
        }
        "greatest" => eval_greatest_least(&values, true),
        "least" => eval_greatest_least(&values, false),
        "iif" => eval_iif(&values),
        "concat" => {
            let mut output = String::new();
            for value in &values {
                if matches!(value, Value::Null) {
                    continue;
                }
                output.push_str(&value_to_text(value)?);
            }
            Ok(Value::Text(output))
        }
        "concat_ws" => {
            if values.is_empty() {
                return Err(DbError::sql("CONCAT_WS expects at least 1 argument"));
            }
            let Some(separator) = expect_text_arg("CONCAT_WS", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let mut parts = Vec::new();
            for value in &values[1..] {
                if matches!(value, Value::Null) {
                    continue;
                }
                parts.push(value_to_text(value)?);
            }
            Ok(Value::Text(parts.join(separator)))
        }
        "lower" => unary_text_fn(values, |value| value.to_ascii_lowercase()),
        "upper" => unary_text_fn(values, |value| value.to_ascii_uppercase()),
        "trim" | "pg_catalog.btrim" => unary_text_fn(values, |value| value.trim().to_string()),
        "ltrim" | "pg_catalog.ltrim" => {
            unary_text_fn(values, |value| value.trim_start().to_string())
        }
        "rtrim" | "pg_catalog.rtrim" => unary_text_fn(values, |value| value.trim_end().to_string()),
        "position" | "pg_catalog.position" => {
            if values.len() != 2 {
                return Err(DbError::sql("POSITION expects 2 arguments"));
            }
            let Some(haystack) = expect_text_arg("POSITION", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(needle) = expect_text_arg("POSITION", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            if needle.is_empty() {
                return Ok(Value::Int64(1));
            }
            if let Some(idx) = haystack.find(needle) {
                let char_idx = haystack[..idx].chars().count();
                Ok(Value::Int64((char_idx + 1) as i64))
            } else {
                Ok(Value::Int64(0))
            }
        }
        "initcap" => {
            if values.len() != 1 {
                return Err(DbError::sql("INITCAP expects 1 argument"));
            }
            let Some(value) = expect_text_arg("INITCAP", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let mut output = String::with_capacity(value.len());
            let mut start_of_word = true;
            for ch in value.chars() {
                if ch.is_alphanumeric() {
                    if start_of_word {
                        for upper in ch.to_uppercase() {
                            output.push(upper);
                        }
                        start_of_word = false;
                    } else {
                        for lower in ch.to_lowercase() {
                            output.push(lower);
                        }
                    }
                } else {
                    start_of_word = true;
                    output.push(ch);
                }
            }
            Ok(Value::Text(output))
        }
        "ascii" => {
            if values.len() != 1 {
                return Err(DbError::sql("ASCII expects 1 argument"));
            }
            let Some(value) = expect_text_arg("ASCII", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Int64(
                value
                    .chars()
                    .next()
                    .map_or(0_i64, |ch| i64::from(ch as u32)),
            ))
        }
        "length" => {
            if values.len() != 1 {
                return Err(DbError::sql("LENGTH expects one argument"));
            }
            match &values[0] {
                Value::Text(value) => Ok(Value::Int64(value.chars().count() as i64)),
                Value::Blob(value) => Ok(Value::Int64(value.len() as i64)),
                Value::Null => Ok(Value::Null),
                other => Err(DbError::sql(format!(
                    "LENGTH expects text or blob, got {other:?}"
                ))),
            }
        }
        "substr" | "substring" => {
            if values.len() < 2 || values.len() > 3 {
                return Err(DbError::sql("SUBSTR expects 2 or 3 arguments"));
            }
            if matches!(values[0], Value::Null) || matches!(values[1], Value::Null) {
                return Ok(Value::Null);
            }
            if values.len() == 3 && matches!(values[2], Value::Null) {
                return Ok(Value::Null);
            }
            let s = match &values[0] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("SUBSTR expects text for first argument")),
            };
            let start = match &values[1] {
                Value::Int64(i) => *i,
                _ => return Err(DbError::sql("SUBSTR expects int for second argument")),
            };
            let length = if values.len() == 3 {
                match &values[2] {
                    Value::Int64(i) => Some(*i),
                    _ => return Err(DbError::sql("SUBSTR expects int for third argument")),
                }
            } else {
                None
            };

            let char_idx = if start > 0 { start - 1 } else { 0 } as usize;
            let chars = s.chars().skip(char_idx);
            if let Some(l) = length {
                let len = if l > 0 { l as usize } else { 0 };
                Ok(Value::Text(chars.take(len).collect()))
            } else {
                Ok(Value::Text(chars.collect()))
            }
        }
        "replace" => {
            if values.len() != 3 {
                return Err(DbError::sql("REPLACE expects 3 arguments"));
            }
            if matches!(values[0], Value::Null)
                || matches!(values[1], Value::Null)
                || matches!(values[2], Value::Null)
            {
                return Ok(Value::Null);
            }
            let s = match &values[0] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("REPLACE expects text for first argument")),
            };
            let target = match &values[1] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("REPLACE expects text for second argument")),
            };
            let replacement = match &values[2] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("REPLACE expects text for third argument")),
            };
            Ok(Value::Text(s.replace(target, replacement)))
        }
        "regexp_replace" => {
            if values.len() < 3 || values.len() > 4 {
                return Err(DbError::sql("REGEXP_REPLACE expects 3 or 4 arguments"));
            }
            let Some(input) = expect_text_arg("REGEXP_REPLACE", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(pattern) = expect_text_arg("REGEXP_REPLACE", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let Some(replacement) = expect_text_arg("REGEXP_REPLACE", "third", &values[2])? else {
                return Ok(Value::Null);
            };
            let flags = if let Some(flag_value) = values.get(3) {
                let Some(flags) = expect_text_arg("REGEXP_REPLACE", "fourth", flag_value)? else {
                    return Ok(Value::Null);
                };
                Some(flags)
            } else {
                None
            };
            Ok(Value::Text(eval_regexp_replace(
                input,
                pattern,
                replacement,
                flags,
            )?))
        }
        "split_part" => {
            if values.len() != 3 {
                return Err(DbError::sql("SPLIT_PART expects 3 arguments"));
            }
            let Some(value) = expect_text_arg("SPLIT_PART", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(delimiter) = expect_text_arg("SPLIT_PART", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let Some(index) = expect_int_arg("SPLIT_PART", "third", &values[2])? else {
                return Ok(Value::Null);
            };
            if index <= 0 {
                return Err(DbError::sql("SPLIT_PART index must be greater than 0"));
            }
            if delimiter.is_empty() {
                if index == 1 {
                    return Ok(Value::Text(value.to_string()));
                }
                return Ok(Value::Text(String::new()));
            }
            let index = usize::try_from(index - 1)
                .map_err(|_| DbError::sql("SPLIT_PART index is out of range"))?;
            Ok(Value::Text(
                value
                    .split(delimiter)
                    .nth(index)
                    .unwrap_or_default()
                    .to_string(),
            ))
        }
        "string_to_array" => {
            if values.len() != 2 {
                return Err(DbError::sql("STRING_TO_ARRAY expects 2 arguments"));
            }
            let Some(value) = expect_text_arg("STRING_TO_ARRAY", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(delimiter) = expect_text_arg("STRING_TO_ARRAY", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let array = if delimiter.is_empty() {
                JsonValue::Array(vec![JsonValue::String(value.to_string())])
            } else {
                JsonValue::Array(
                    value
                        .split(delimiter)
                        .map(|part| JsonValue::String(part.to_string()))
                        .collect::<Vec<_>>(),
                )
            };
            Ok(Value::Text(array.render_json()))
        }
        "quote_ident" => {
            if values.len() != 1 {
                return Err(DbError::sql("QUOTE_IDENT expects 1 argument"));
            }
            let Some(identifier) = expect_text_arg("QUOTE_IDENT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(format!(
                "\"{}\"",
                identifier.replace('"', "\"\"")
            )))
        }
        "quote_literal" => {
            if values.len() != 1 {
                return Err(DbError::sql("QUOTE_LITERAL expects 1 argument"));
            }
            let Some(literal) = expect_text_arg("QUOTE_LITERAL", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(format!("'{}'", literal.replace('\'', "''"))))
        }
        "md5" => {
            if values.len() != 1 {
                return Err(DbError::sql("MD5 expects 1 argument"));
            }
            let Some(value) = expect_text_arg("MD5", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(format!("{:x}", md5::compute(value.as_bytes()))))
        }
        "sha256" => {
            if values.len() != 1 {
                return Err(DbError::sql("SHA256 expects 1 argument"));
            }
            let Some(value) = expect_text_arg("SHA256", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let digest = <sha2::Sha256 as sha2::Digest>::digest(value.as_bytes());
            Ok(Value::Text(hex_encode_lower(&digest)))
        }
        "instr" => {
            if values.len() != 2 {
                return Err(DbError::sql("INSTR expects 2 arguments"));
            }
            if matches!(values[0], Value::Null) || matches!(values[1], Value::Null) {
                return Ok(Value::Null);
            }
            let s = match &values[0] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("INSTR expects text for first argument")),
            };
            let target = match &values[1] {
                Value::Text(s) => s,
                _ => return Err(DbError::sql("INSTR expects text for second argument")),
            };
            match s.find(target) {
                Some(idx) => {
                    let char_idx = s[..idx].chars().count();
                    Ok(Value::Int64((char_idx + 1) as i64))
                }
                None => Ok(Value::Int64(0)),
            }
        }
        "left" => {
            if values.len() != 2 {
                return Err(DbError::sql("LEFT expects 2 arguments"));
            }
            let Some(value) = expect_text_arg("LEFT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(length) = expect_int_arg("LEFT", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(take_left_chars(
                value,
                non_negative_usize(length, "LEFT", "second")?,
            )))
        }
        "right" => {
            if values.len() != 2 {
                return Err(DbError::sql("RIGHT expects 2 arguments"));
            }
            let Some(value) = expect_text_arg("RIGHT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(length) = expect_int_arg("RIGHT", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(take_right_chars(
                value,
                non_negative_usize(length, "RIGHT", "second")?,
            )))
        }
        "lpad" => {
            if values.len() != 3 {
                return Err(DbError::sql("LPAD expects 3 arguments"));
            }
            let Some(value) = expect_text_arg("LPAD", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(target_len) = expect_int_arg("LPAD", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let Some(pad) = expect_text_arg("LPAD", "third", &values[2])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(pad_left(value, target_len, pad)))
        }
        "rpad" => {
            if values.len() != 3 {
                return Err(DbError::sql("RPAD expects 3 arguments"));
            }
            let Some(value) = expect_text_arg("RPAD", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(target_len) = expect_int_arg("RPAD", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let Some(pad) = expect_text_arg("RPAD", "third", &values[2])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(pad_right(value, target_len, pad)))
        }
        "repeat" => {
            if values.len() != 2 {
                return Err(DbError::sql("REPEAT expects 2 arguments"));
            }
            let Some(value) = expect_text_arg("REPEAT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(count) = expect_int_arg("REPEAT", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let count = non_negative_usize(count, "REPEAT", "second")?;
            Ok(Value::Text(value.repeat(count)))
        }
        "reverse" => {
            if values.len() != 1 {
                return Err(DbError::sql("REVERSE expects 1 argument"));
            }
            let Some(value) = expect_text_arg("REVERSE", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Text(value.chars().rev().collect()))
        }
        "chr" | "char" => {
            if values.len() != 1 {
                return Err(DbError::sql("CHR expects 1 argument"));
            }
            let Some(codepoint) = expect_int_arg("CHR", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let codepoint = u32::try_from(codepoint)
                .map_err(|_| DbError::sql("CHR code point must be between 0 and 1114111"))?;
            let ch = char::from_u32(codepoint)
                .ok_or_else(|| DbError::sql("CHR code point must be between 0 and 1114111"))?;
            Ok(Value::Text(ch.to_string()))
        }
        "hex" => {
            if values.len() != 1 {
                return Err(DbError::sql("HEX expects 1 argument"));
            }
            match &values[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(value) => Ok(Value::Text(hex_encode_upper(value.as_bytes()))),
                Value::Blob(value) => Ok(Value::Text(hex_encode_upper(value))),
                Value::Uuid(value) => Ok(Value::Text(hex_encode_upper(value))),
                other => Err(DbError::sql(format!(
                    "HEX expects text, BLOB, or UUID, got {other:?}"
                ))),
            }
        }
        "abs" => {
            if values.len() != 1 {
                return Err(DbError::sql("ABS expects 1 argument"));
            }
            match expect_numeric_arg("ABS", "first", &values[0])? {
                None => Ok(Value::Null),
                Some(NumericValue::Int64(value)) => value
                    .checked_abs()
                    .map(Value::Int64)
                    .ok_or_else(|| DbError::sql("ABS overflow for INT64 input")),
                Some(NumericValue::Float64(value)) => Ok(Value::Float64(value.abs())),
                Some(NumericValue::Decimal { scaled, scale }) => scaled
                    .checked_abs()
                    .map(|scaled| Value::Decimal { scaled, scale })
                    .ok_or_else(|| DbError::sql("ABS overflow for DECIMAL input")),
            }
        }
        "ceil" | "ceiling" => {
            if values.len() != 1 {
                return Err(DbError::sql("CEIL expects 1 argument"));
            }
            match expect_numeric_arg("CEIL", "first", &values[0])? {
                None => Ok(Value::Null),
                Some(NumericValue::Int64(value)) => Ok(Value::Int64(value)),
                Some(value) => Ok(Value::Float64(value.as_f64().ceil())),
            }
        }
        "floor" => {
            if values.len() != 1 {
                return Err(DbError::sql("FLOOR expects 1 argument"));
            }
            match expect_numeric_arg("FLOOR", "first", &values[0])? {
                None => Ok(Value::Null),
                Some(NumericValue::Int64(value)) => Ok(Value::Int64(value)),
                Some(value) => Ok(Value::Float64(value.as_f64().floor())),
            }
        }
        "round" => {
            if values.is_empty() || values.len() > 2 {
                return Err(DbError::sql("ROUND expects 1 or 2 arguments"));
            }
            let Some(number) = expect_numeric_arg("ROUND", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let scale = if let Some(value) = values.get(1) {
                let Some(scale) = expect_int_arg("ROUND", "second", value)? else {
                    return Ok(Value::Null);
                };
                i32::try_from(scale).map_err(|_| DbError::sql("ROUND precision is out of range"))?
            } else {
                0
            };
            let factor = 10_f64.powi(scale);
            Ok(Value::Float64((number.as_f64() * factor).round() / factor))
        }
        "sqrt" => {
            if values.len() != 1 {
                return Err(DbError::sql("SQRT expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("SQRT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let number = number.as_f64();
            if number < 0.0 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(number.sqrt()))
        }
        "power" | "pow" => {
            if values.len() != 2 {
                return Err(DbError::sql("POWER expects 2 arguments"));
            }
            let Some(base) = expect_numeric_arg("POWER", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(exponent) = expect_numeric_arg("POWER", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(base.as_f64().powf(exponent.as_f64())))
        }
        "mod" => {
            if values.len() != 2 {
                return Err(DbError::sql("MOD expects 2 arguments"));
            }
            let Some(left) = expect_numeric_arg("MOD", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(right) = expect_numeric_arg("MOD", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            match (left, right) {
                (_, NumericValue::Int64(0)) => Ok(Value::Null),
                (_, NumericValue::Float64(0.0)) => Ok(Value::Null),
                (_, NumericValue::Decimal { scaled: 0, .. }) => Ok(Value::Null),
                (NumericValue::Int64(left), NumericValue::Int64(right)) => {
                    Ok(Value::Int64(left % right))
                }
                (left, right) => Ok(Value::Float64(left.as_f64() % right.as_f64())),
            }
        }
        "sign" => {
            if values.len() != 1 {
                return Err(DbError::sql("SIGN expects 1 argument"));
            }
            match expect_numeric_arg("SIGN", "first", &values[0])? {
                None => Ok(Value::Null),
                Some(NumericValue::Int64(value)) => Ok(Value::Int64(value.signum())),
                Some(NumericValue::Float64(value)) => Ok(Value::Int64(if value > 0.0 {
                    1
                } else if value < 0.0 {
                    -1
                } else {
                    0
                })),
                Some(NumericValue::Decimal { scaled, .. }) => Ok(Value::Int64(scaled.signum())),
            }
        }
        "ln" => {
            if values.len() != 1 {
                return Err(DbError::sql("LN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("LN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let number = number.as_f64();
            if number <= 0.0 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(number.ln()))
        }
        "log" => {
            if values.is_empty() || values.len() > 2 {
                return Err(DbError::sql("LOG expects 1 or 2 arguments"));
            }
            if values.len() == 1 {
                let Some(number) = expect_numeric_arg("LOG", "first", &values[0])? else {
                    return Ok(Value::Null);
                };
                let number = number.as_f64();
                if number <= 0.0 {
                    return Ok(Value::Null);
                }
                return Ok(Value::Float64(number.log10()));
            }
            let Some(base) = expect_numeric_arg("LOG", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(number) = expect_numeric_arg("LOG", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            let base = base.as_f64();
            let number = number.as_f64();
            if base <= 0.0 || base == 1.0 || number <= 0.0 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(number.log(base)))
        }
        "exp" => {
            if values.len() != 1 {
                return Err(DbError::sql("EXP expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("EXP", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().exp()))
        }
        "sin" => {
            if values.len() != 1 {
                return Err(DbError::sql("SIN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("SIN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().sin()))
        }
        "cos" => {
            if values.len() != 1 {
                return Err(DbError::sql("COS expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("COS", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().cos()))
        }
        "tan" => {
            if values.len() != 1 {
                return Err(DbError::sql("TAN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("TAN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let radians = number.as_f64();
            if radians.cos().abs() < 1e-12 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(radians.tan()))
        }
        "asin" => {
            if values.len() != 1 {
                return Err(DbError::sql("ASIN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("ASIN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let value = number.as_f64();
            if !(-1.0..=1.0).contains(&value) {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(value.asin()))
        }
        "acos" => {
            if values.len() != 1 {
                return Err(DbError::sql("ACOS expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("ACOS", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let value = number.as_f64();
            if !(-1.0..=1.0).contains(&value) {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(value.acos()))
        }
        "atan" => {
            if values.len() != 1 {
                return Err(DbError::sql("ATAN expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("ATAN", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().atan()))
        }
        "atan2" => {
            if values.len() != 2 {
                return Err(DbError::sql("ATAN2 expects 2 arguments"));
            }
            let Some(y) = expect_numeric_arg("ATAN2", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let Some(x) = expect_numeric_arg("ATAN2", "second", &values[1])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(y.as_f64().atan2(x.as_f64())))
        }
        "pi" => {
            if !values.is_empty() {
                return Err(DbError::sql("PI expects 0 arguments"));
            }
            Ok(Value::Float64(std::f64::consts::PI))
        }
        "degrees" => {
            if values.len() != 1 {
                return Err(DbError::sql("DEGREES expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("DEGREES", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().to_degrees()))
        }
        "radians" => {
            if values.len() != 1 {
                return Err(DbError::sql("RADIANS expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("RADIANS", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            Ok(Value::Float64(number.as_f64().to_radians()))
        }
        "cot" => {
            if values.len() != 1 {
                return Err(DbError::sql("COT expects 1 argument"));
            }
            let Some(number) = expect_numeric_arg("COT", "first", &values[0])? else {
                return Ok(Value::Null);
            };
            let tan = number.as_f64().tan();
            if tan.abs() < 1e-12 {
                return Ok(Value::Null);
            }
            Ok(Value::Float64(1.0 / tan))
        }
        "random" => {
            if !values.is_empty() {
                return Err(DbError::sql("RANDOM expects 0 arguments"));
            }
            Ok(Value::Float64(next_random_f64()))
        }
        "now" | "current_timestamp" | "localtimestamp" => eval_current_timestamp(values),
        "current_date" => eval_current_date(values),
        "current_time" | "localtime" => eval_current_time(values),
        "date_trunc" => eval_date_trunc(values),
        "date_part" | "pg_catalog.date_part" => eval_date_part(values),
        "date_diff" => eval_date_diff(values),
        "last_day" => eval_last_day(values),
        "next_day" => eval_next_day(values),
        "make_date" => eval_make_date(values),
        "make_timestamp" => eval_make_timestamp(values),
        "to_timestamp" => eval_to_timestamp(values),
        "interval" => eval_interval(values),
        "age" => eval_age(values),
        "date" => eval_date(values),
        "datetime" => eval_datetime(values),
        "strftime" => eval_strftime(values),
        "extract" | "pg_catalog.extract" => eval_extract(values),
        "gen_random_uuid" => eval_gen_random_uuid(values),
        "uuid_parse" => eval_uuid_parse(values),
        "uuid_to_string" => eval_uuid_to_string(values),
        "json_array" | "pg_catalog.json_array" => eval_json_array(values),
        "json_array_length" => eval_json_array_length(values),
        "json_extract" => eval_json_extract(values),
        "json_object" | "pg_catalog.json_object" => eval_json_object(values),
        "json_type" | "pg_catalog.json_type" => eval_json_type(values),
        "json_valid" | "pg_catalog.json_valid" => eval_json_valid(values),
        "st_point" | "st_makepoint" => eval_spatial_point_constructor(&values, false, false),
        "st_pointz" => eval_spatial_point_constructor(&values, false, true),
        "st_pointm" => eval_spatial_point_m_constructor(&values, false),
        "st_pointzm" => eval_spatial_point_zm_constructor(&values, false),
        "st_geogpoint" => eval_spatial_point_constructor(&values, true, false),
        "st_geogpointz" => eval_spatial_point_constructor(&values, true, true),
        "st_geogpointm" => eval_spatial_point_m_constructor(&values, true),
        "st_geogpointzm" => eval_spatial_point_zm_constructor(&values, true),
        "st_asbinary" => eval_spatial_as_binary(&values),
        "st_astext" => eval_spatial_as_text(&values),
        "st_asgeojson" => eval_spatial_as_geojson(&values),
        "st_srid" => eval_spatial_srid(&values),
        "st_geometrytype" => eval_spatial_geometry_type(&values),
        "st_x" => eval_spatial_point_accessor(&values, SpatialAccessor::X),
        "st_y" => eval_spatial_point_accessor(&values, SpatialAccessor::Y),
        "st_z" => eval_spatial_point_accessor(&values, SpatialAccessor::Z),
        "st_m" => eval_spatial_point_accessor(&values, SpatialAccessor::M),
        "st_setsrid" => eval_spatial_set_srid(&values),
        "st_isvalid" => eval_spatial_is_valid(&values),
        "st_distance" => eval_spatial_distance(&values),
        "st_dwithin" => eval_spatial_dwithin(&values),
        "st_intersects" => eval_spatial_predicate(&values, SpatialPredicate::Intersects),
        "st_contains" => eval_spatial_predicate(&values, SpatialPredicate::Contains),
        "st_within" => eval_spatial_predicate(&values, SpatialPredicate::Within),
        "st_equals" => eval_spatial_predicate(&values, SpatialPredicate::Equals),
        "st_length" => eval_spatial_length(&values),
        "st_area" => eval_spatial_area(&values),
        "st_geomfromwkb" => eval_spatial_from_wkb(&values, false),
        "st_geogfromwkb" => eval_spatial_from_wkb(&values, true),
        "st_geomfromtext" => eval_spatial_from_text(&values, false),
        "st_geogfromtext" => eval_spatial_from_text(&values, true),
        "st_geomfromgeojson" => eval_spatial_from_geojson(&values, false),
        "st_geogfromgeojson" => eval_spatial_from_geojson(&values, true),
        other => {
            if let Some(value) =
                crate::extensions::invoke_scalar_from_runtime(runtime, other, &values)?
            {
                return Ok(value);
            }
            Err(DbError::sql(format!("unsupported scalar function {other}")))
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum SpatialAccessor {
    X,
    Y,
    Z,
    M,
}

#[derive(Clone, Copy)]
pub(super) enum SpatialPredicate {
    Intersects,
    Contains,
    Within,
    Equals,
}

pub(super) fn spatial_error(error: SpatialError) -> DbError {
    DbError::sql(error.to_string())
}

pub(super) fn eval_spatial_point_constructor(
    values: &[Value],
    geography: bool,
    with_z: bool,
) -> Result<Value> {
    let expected = if with_z { 3 } else { 2 };
    if values.len() != expected {
        return Err(DbError::sql(format!(
            "{} expects {expected} arguments",
            if geography {
                "ST_GeogPoint"
            } else {
                "ST_Point"
            }
        )));
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let x = numeric_value_as_f64("spatial point", "first", &values[0])?;
    let y = numeric_value_as_f64("spatial point", "second", &values[1])?;
    let z = if with_z {
        Some(numeric_value_as_f64("spatial point", "third", &values[2])?)
    } else {
        None
    };
    let dimensions = if with_z {
        CoordinateDimensions::Xyz
    } else {
        CoordinateDimensions::Xy
    };
    spatial_point_value(geography, x, y, z, None, dimensions)
}

pub(super) fn eval_spatial_point_m_constructor(values: &[Value], geography: bool) -> Result<Value> {
    if values.len() != 3 {
        return Err(DbError::sql(if geography {
            "ST_GeogPointM expects 3 arguments"
        } else {
            "ST_PointM expects 3 arguments"
        }));
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let x = numeric_value_as_f64("spatial point", "first", &values[0])?;
    let y = numeric_value_as_f64("spatial point", "second", &values[1])?;
    let m = numeric_value_as_f64("spatial point", "third", &values[2])?;
    spatial_point_value(geography, x, y, None, Some(m), CoordinateDimensions::Xym)
}

pub(super) fn eval_spatial_point_zm_constructor(
    values: &[Value],
    geography: bool,
) -> Result<Value> {
    if values.len() != 4 {
        return Err(DbError::sql(if geography {
            "ST_GeogPointZM expects 4 arguments"
        } else {
            "ST_PointZM expects 4 arguments"
        }));
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let x = numeric_value_as_f64("spatial point", "first", &values[0])?;
    let y = numeric_value_as_f64("spatial point", "second", &values[1])?;
    let z = numeric_value_as_f64("spatial point", "third", &values[2])?;
    let m = numeric_value_as_f64("spatial point", "fourth", &values[3])?;
    spatial_point_value(
        geography,
        x,
        y,
        Some(z),
        Some(m),
        CoordinateDimensions::Xyzm,
    )
}

pub(super) fn spatial_point_value(
    geography: bool,
    x: f64,
    y: f64,
    z: Option<f64>,
    m: Option<f64>,
    dimensions: CoordinateDimensions,
) -> Result<Value> {
    let position = Position::for_dimensions(x, y, z, m, dimensions).map_err(spatial_error)?;
    let value = SpatialValue::new(
        if geography { 4326 } else { 0 },
        dimensions,
        SpatialGeometry::Point(position),
    )
    .map_err(spatial_error)?;
    validate_spatial_value(&value, geography)?;
    let bytes = crate::spatial::ewkb::to_ewkb(&value);
    Ok(if geography {
        Value::Geography(bytes)
    } else {
        Value::Geometry(bytes)
    })
}

pub(super) fn eval_spatial_from_wkb(values: &[Value], geography: bool) -> Result<Value> {
    let valid_arity = if geography {
        values.len() == 1
    } else {
        values.len() == 1 || values.len() == 2
    };
    if !valid_arity {
        return Err(DbError::sql(if geography {
            "ST_GeogFromWKB expects 1 argument"
        } else {
            "ST_GeomFromWKB expects 1 or 2 arguments"
        }));
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let bytes = expect_blob_arg(
        if geography {
            "ST_GeogFromWKB"
        } else {
            "ST_GeomFromWKB"
        },
        "first",
        &values[0],
    )?;
    let default_srid = if geography {
        4326
    } else if values.len() == 2 {
        u32::try_from(expect_int_arg("ST_GeomFromWKB", "second", &values[1])?.unwrap_or(0))
            .map_err(|_| DbError::sql("ST_GeomFromWKB SRID must be non-negative"))?
    } else {
        0
    };
    let mut spatial = crate::spatial::ewkb::from_wkb_with_default_srid(bytes, default_srid)
        .map_err(spatial_error)?;
    if !geography && values.len() == 2 && spatial.srid != default_srid {
        return Err(DbError::sql(
            "ST_GeomFromWKB explicit SRID does not match EWKB SRID",
        ));
    }
    if geography {
        spatial.srid = 4326;
    }
    validate_spatial_value(&spatial, geography)?;
    let bytes = crate::spatial::ewkb::to_ewkb(&spatial);
    Ok(if geography {
        Value::Geography(bytes)
    } else {
        Value::Geometry(bytes)
    })
}

pub(super) fn eval_spatial_from_text(values: &[Value], geography: bool) -> Result<Value> {
    let valid_arity = if geography {
        values.len() == 1
    } else {
        values.len() == 1 || values.len() == 2
    };
    if !valid_arity {
        return Err(DbError::sql(if geography {
            "ST_GeogFromText expects 1 argument"
        } else {
            "ST_GeomFromText expects 1 or 2 arguments"
        }));
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let Some(text) = expect_text_arg(
        if geography {
            "ST_GeogFromText"
        } else {
            "ST_GeomFromText"
        },
        "first",
        &values[0],
    )?
    else {
        return Ok(Value::Null);
    };
    let default_srid = if geography {
        4326
    } else if values.len() == 2 {
        u32::try_from(expect_int_arg("ST_GeomFromText", "second", &values[1])?.unwrap_or(0))
            .map_err(|_| DbError::sql("ST_GeomFromText SRID must be non-negative"))?
    } else {
        0
    };
    let mut spatial = crate::spatial::wkt::from_wkt(text, default_srid).map_err(spatial_error)?;
    if !geography && values.len() == 2 && spatial.srid != default_srid {
        return Err(DbError::sql(
            "ST_GeomFromText explicit SRID does not match EWKT SRID",
        ));
    }
    if geography {
        spatial.srid = 4326;
    }
    validate_spatial_value(&spatial, geography)?;
    let bytes = crate::spatial::ewkb::to_ewkb(&spatial);
    Ok(if geography {
        Value::Geography(bytes)
    } else {
        Value::Geometry(bytes)
    })
}

pub(super) fn eval_spatial_from_geojson(values: &[Value], geography: bool) -> Result<Value> {
    let valid_arity = if geography {
        values.len() == 1
    } else {
        values.len() == 1 || values.len() == 2
    };
    if !valid_arity {
        return Err(DbError::sql(if geography {
            "ST_GeogFromGeoJSON expects 1 argument"
        } else {
            "ST_GeomFromGeoJSON expects 1 or 2 arguments"
        }));
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let Some(text) = expect_text_arg(
        if geography {
            "ST_GeogFromGeoJSON"
        } else {
            "ST_GeomFromGeoJSON"
        },
        "first",
        &values[0],
    )?
    else {
        return Ok(Value::Null);
    };
    let srid = if geography {
        4326
    } else if values.len() == 2 {
        u32::try_from(expect_int_arg("ST_GeomFromGeoJSON", "second", &values[1])?.unwrap_or(0))
            .map_err(|_| DbError::sql("ST_GeomFromGeoJSON SRID must be non-negative"))?
    } else {
        0
    };
    let spatial = crate::spatial::geojson::from_geojson(text, srid).map_err(spatial_error)?;
    validate_spatial_value(&spatial, geography)?;
    let bytes = crate::spatial::ewkb::to_ewkb(&spatial);
    Ok(if geography {
        Value::Geography(bytes)
    } else {
        Value::Geometry(bytes)
    })
}

pub(super) fn eval_spatial_as_binary(values: &[Value]) -> Result<Value> {
    expect_arity("ST_AsBinary", values, 1)?;
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Geometry(bytes) | Value::Geography(bytes) => Ok(Value::Blob(bytes.clone())),
        other => Err(DbError::sql(format!(
            "ST_AsBinary expects spatial input, got {other:?}"
        ))),
    }
}

pub(super) fn eval_spatial_as_text(values: &[Value]) -> Result<Value> {
    expect_arity("ST_AsText", values, 1)?;
    let Some((_, spatial)) = spatial_value_from_db(&values[0])? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(crate::spatial::wkt::to_wkt(&spatial)))
}

pub(super) fn eval_spatial_as_geojson(values: &[Value]) -> Result<Value> {
    expect_arity("ST_AsGeoJSON", values, 1)?;
    let Some((_, spatial)) = spatial_value_from_db(&values[0])? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(
        crate::spatial::geojson::to_geojson(&spatial).map_err(spatial_error)?,
    ))
}

pub(super) fn eval_spatial_srid(values: &[Value]) -> Result<Value> {
    expect_arity("ST_SRID", values, 1)?;
    let Some((_, spatial)) = spatial_value_from_db(&values[0])? else {
        return Ok(Value::Null);
    };
    Ok(Value::Int64(i64::from(spatial.srid)))
}

pub(super) fn eval_spatial_geometry_type(values: &[Value]) -> Result<Value> {
    expect_arity("ST_GeometryType", values, 1)?;
    let Some((_, spatial)) = spatial_value_from_db(&values[0])? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(spatial_kind_name(spatial.kind()).to_string()))
}

pub(super) fn eval_spatial_point_accessor(
    values: &[Value],
    accessor: SpatialAccessor,
) -> Result<Value> {
    expect_arity("spatial coordinate accessor", values, 1)?;
    let Some((_, spatial)) = spatial_value_from_db(&values[0])? else {
        return Ok(Value::Null);
    };
    let SpatialGeometry::Point(point) = spatial.geometry else {
        return Err(DbError::sql(
            "spatial coordinate accessors require POINT input",
        ));
    };
    match accessor {
        SpatialAccessor::X => Ok(Value::Float64(point.x)),
        SpatialAccessor::Y => Ok(Value::Float64(point.y)),
        SpatialAccessor::Z => Ok(point.z.map(Value::Float64).unwrap_or(Value::Null)),
        SpatialAccessor::M => Ok(point.m.map(Value::Float64).unwrap_or(Value::Null)),
    }
}

pub(super) fn eval_spatial_set_srid(values: &[Value]) -> Result<Value> {
    expect_arity("ST_SetSRID", values, 2)?;
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let srid = u32::try_from(expect_int_arg("ST_SetSRID", "second", &values[1])?.unwrap_or(0))
        .map_err(|_| DbError::sql("ST_SetSRID SRID must be non-negative"))?;
    let Some((geography, mut spatial)) = spatial_value_from_db(&values[0])? else {
        return Ok(Value::Null);
    };
    if geography && srid != 4326 {
        return Err(DbError::sql(
            "GEOGRAPHY supports only SRID 4326 in DecentDB 1.0",
        ));
    }
    spatial.srid = srid;
    validate_spatial_value(&spatial, geography)?;
    let bytes = crate::spatial::ewkb::to_ewkb(&spatial);
    Ok(if geography {
        Value::Geography(bytes)
    } else {
        Value::Geometry(bytes)
    })
}

pub(super) fn eval_spatial_is_valid(values: &[Value]) -> Result<Value> {
    expect_arity("ST_IsValid", values, 1)?;
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Geometry(bytes) => Ok(Value::Bool(
            crate::spatial::ewkb::from_ewkb(bytes)
                .and_then(|value| validate_spatial_value_spatial_error(&value, false))
                .is_ok(),
        )),
        Value::Geography(bytes) => Ok(Value::Bool(
            crate::spatial::ewkb::from_ewkb(bytes)
                .and_then(|value| validate_spatial_value_spatial_error(&value, true))
                .is_ok(),
        )),
        other => Err(DbError::sql(format!(
            "ST_IsValid expects spatial input, got {other:?}"
        ))),
    }
}

pub(super) fn eval_spatial_distance(values: &[Value]) -> Result<Value> {
    expect_arity("ST_Distance", values, 2)?;
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    Ok(Value::Float64(spatial_distance_values(
        &values[0], &values[1],
    )?))
}

pub(super) fn eval_spatial_dwithin(values: &[Value]) -> Result<Value> {
    expect_arity("ST_DWithin", values, 3)?;
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let distance = numeric_value_as_f64("ST_DWithin", "third", &values[2])?;
    if distance < 0.0 {
        return Ok(Value::Bool(false));
    }
    Ok(Value::Bool(
        spatial_distance_values(&values[0], &values[1])? <= distance,
    ))
}

pub(super) fn eval_spatial_predicate(
    values: &[Value],
    predicate: SpatialPredicate,
) -> Result<Value> {
    expect_arity("spatial predicate", values, 2)?;
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let (left_geography, left) = spatial_value_from_db_required(&values[0])?;
    let (right_geography, right) = spatial_value_from_db_required(&values[1])?;
    ensure_spatial_compatible(left_geography, &left, right_geography, &right)?;
    let result = match predicate {
        SpatialPredicate::Intersects => {
            crate::spatial::predicate::intersects(&left.geometry, &right.geometry)
        }
        SpatialPredicate::Contains => {
            crate::spatial::predicate::contains(&left.geometry, &right.geometry)
        }
        SpatialPredicate::Within => {
            crate::spatial::predicate::within(&left.geometry, &right.geometry)
        }
        SpatialPredicate::Equals => {
            crate::spatial::predicate::equals(&left.geometry, &right.geometry)
        }
    }
    .map_err(spatial_error)?;
    Ok(Value::Bool(result))
}

pub(super) fn eval_spatial_length(values: &[Value]) -> Result<Value> {
    expect_arity("ST_Length", values, 1)?;
    let Some((geography, spatial)) = spatial_value_from_db(&values[0])? else {
        return Ok(Value::Null);
    };
    if geography {
        return Err(DbError::sql("ST_Length for GEOGRAPHY is not supported"));
    }
    Ok(Value::Float64(geometry_length(&spatial.geometry)?))
}

pub(super) fn eval_spatial_area(values: &[Value]) -> Result<Value> {
    expect_arity("ST_Area", values, 1)?;
    let Some((geography, spatial)) = spatial_value_from_db(&values[0])? else {
        return Ok(Value::Null);
    };
    let area = if geography {
        geography_area_approx_meters(&spatial.geometry)?
    } else {
        geometry_area(&spatial.geometry)?
    };
    Ok(Value::Float64(area))
}

pub(super) fn spatial_distance_values(left: &Value, right: &Value) -> Result<f64> {
    let (left_geography, left) = spatial_value_from_db_required(left)?;
    let (right_geography, right) = spatial_value_from_db_required(right)?;
    ensure_spatial_compatible(left_geography, &left, right_geography, &right)?;
    if left_geography {
        let (SpatialGeometry::Point(left_point), SpatialGeometry::Point(right_point)) =
            (&left.geometry, &right.geometry)
        else {
            return Err(DbError::sql(
                "ST_Distance for GEOGRAPHY supports POINT inputs in this release",
            ));
        };
        Ok(crate::spatial::distance::geography_point_distance_meters(
            *left_point,
            *right_point,
        ))
    } else {
        crate::spatial::distance::planar_geometry_distance(&left.geometry, &right.geometry)
            .map_err(spatial_error)
    }
}

pub(super) fn spatial_value_from_db_required(value: &Value) -> Result<(bool, SpatialValue)> {
    spatial_value_from_db(value)?.ok_or_else(|| DbError::sql("spatial input must not be NULL"))
}

pub(super) fn spatial_value_from_db(value: &Value) -> Result<Option<(bool, SpatialValue)>> {
    match value {
        Value::Null => Ok(None),
        Value::Geometry(bytes) => Ok(Some((
            false,
            crate::spatial::ewkb::from_ewkb(bytes).map_err(spatial_error)?,
        ))),
        Value::Geography(bytes) => Ok(Some((
            true,
            crate::spatial::ewkb::from_ewkb(bytes).map_err(spatial_error)?,
        ))),
        other => Err(DbError::sql(format!(
            "expected spatial value, got {other:?}"
        ))),
    }
}

pub(super) fn ensure_spatial_compatible(
    left_geography: bool,
    left: &SpatialValue,
    right_geography: bool,
    right: &SpatialValue,
) -> Result<()> {
    if left_geography != right_geography {
        return Err(DbError::sql(
            "spatial predicates require matching GEOMETRY/GEOGRAPHY families",
        ));
    }
    if left.srid != right.srid {
        return Err(DbError::sql("spatial predicates require matching SRIDs"));
    }
    Ok(())
}

pub(super) fn normalize_geometry_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let value =
        crate::spatial::ewkb::from_wkb_with_default_srid(bytes, 0).map_err(spatial_error)?;
    validate_spatial_value(&value, false)?;
    Ok(crate::spatial::ewkb::to_ewkb(&value))
}

pub(super) fn normalize_geography_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut value =
        crate::spatial::ewkb::from_wkb_with_default_srid(bytes, 4326).map_err(spatial_error)?;
    value.srid = 4326;
    validate_spatial_value(&value, true)?;
    Ok(crate::spatial::ewkb::to_ewkb(&value))
}

pub(super) fn validate_spatial_value(value: &SpatialValue, geography: bool) -> Result<()> {
    validate_spatial_value_spatial_error(value, geography).map_err(spatial_error)
}

pub(super) fn validate_spatial_value_spatial_error(
    value: &SpatialValue,
    geography: bool,
) -> std::result::Result<(), SpatialError> {
    if geography && value.srid != 4326 {
        return Err(SpatialError::InvalidInput(
            "GEOGRAPHY supports only SRID 4326 in DecentDB 1.0".to_string(),
        ));
    }
    if !geography && value.srid > i32::MAX as u32 {
        return Err(SpatialError::InvalidInput(
            "GEOMETRY SRID exceeds supported range".to_string(),
        ));
    }
    for position in value.geometry.all_positions() {
        if !position.x.is_finite()
            || !position.y.is_finite()
            || position.z.is_some_and(|z| !z.is_finite())
            || position.m.is_some_and(|m| !m.is_finite())
        {
            return Err(SpatialError::InvalidInput(
                "spatial coordinates must be finite".to_string(),
            ));
        }
        if geography
            && !((-180.0..=180.0).contains(&position.x) && (-90.0..=90.0).contains(&position.y))
        {
            return Err(SpatialError::InvalidInput(
                "GEOGRAPHY coordinates must be lon/lat in valid WGS84 ranges".to_string(),
            ));
        }
    }
    validate_shape_minimums(&value.geometry)
}

pub(super) fn validate_shape_minimums(
    geometry: &SpatialGeometry,
) -> std::result::Result<(), SpatialError> {
    match geometry {
        SpatialGeometry::Point(_) => Ok(()),
        SpatialGeometry::LineString(line) => {
            if line.len() < 2 {
                Err(SpatialError::InvalidInput(
                    "LINESTRING requires at least two points".to_string(),
                ))
            } else {
                Ok(())
            }
        }
        SpatialGeometry::Polygon(polygon) => validate_polygon_minimums(polygon),
        SpatialGeometry::MultiPoint(points) => {
            if points.is_empty() {
                Err(SpatialError::InvalidInput(
                    "MULTIPOINT requires at least one point".to_string(),
                ))
            } else {
                Ok(())
            }
        }
        SpatialGeometry::MultiLineString(lines) => {
            for line in lines {
                if line.len() < 2 {
                    return Err(SpatialError::InvalidInput(
                        "MULTILINESTRING members require at least two points".to_string(),
                    ));
                }
            }
            Ok(())
        }
        SpatialGeometry::MultiPolygon(polygons) => {
            for polygon in polygons {
                validate_polygon_minimums(polygon)?;
            }
            Ok(())
        }
    }
}

pub(super) fn validate_polygon_minimums(
    polygon: &[Vec<Position>],
) -> std::result::Result<(), SpatialError> {
    if polygon.is_empty() {
        return Err(SpatialError::InvalidInput(
            "POLYGON requires at least one ring".to_string(),
        ));
    }
    for ring in polygon {
        if ring.len() < 4 {
            return Err(SpatialError::InvalidInput(
                "POLYGON rings require at least four points".to_string(),
            ));
        }
        if !ring
            .first()
            .zip(ring.last())
            .is_some_and(|(first, last)| first.xy_equals(*last))
        {
            return Err(SpatialError::InvalidInput(
                "POLYGON rings must be closed".to_string(),
            ));
        }
    }
    Ok(())
}

pub(super) fn expect_arity(function_name: &str, values: &[Value], expected: usize) -> Result<()> {
    if values.len() != expected {
        return Err(DbError::sql(format!(
            "{function_name} expects {expected} argument{}",
            if expected == 1 { "" } else { "s" }
        )));
    }
    Ok(())
}

pub(super) fn expect_blob_arg<'a>(
    function_name: &str,
    ordinal: &str,
    value: &'a Value,
) -> Result<&'a [u8]> {
    match value {
        Value::Blob(value) => Ok(value),
        other => Err(DbError::sql(format!(
            "{function_name} expects BLOB for {ordinal} argument, got {other:?}"
        ))),
    }
}

pub(super) fn numeric_value_as_f64(
    function_name: &str,
    ordinal: &str,
    value: &Value,
) -> Result<f64> {
    let Some(number) = expect_numeric_arg(function_name, ordinal, value)? else {
        return Err(DbError::sql(format!(
            "{function_name} {ordinal} argument must not be NULL"
        )));
    };
    let value = number.as_f64();
    if !value.is_finite() {
        return Err(DbError::sql(format!(
            "{function_name} {ordinal} argument must be finite"
        )));
    }
    Ok(value)
}

pub(super) fn spatial_kind_name(kind: SpatialKind) -> &'static str {
    match kind {
        SpatialKind::Point => "POINT",
        SpatialKind::LineString => "LINESTRING",
        SpatialKind::Polygon => "POLYGON",
        SpatialKind::MultiPoint => "MULTIPOINT",
        SpatialKind::MultiLineString => "MULTILINESTRING",
        SpatialKind::MultiPolygon => "MULTIPOLYGON",
    }
}

pub(super) fn geometry_length(geometry: &SpatialGeometry) -> Result<f64> {
    match geometry {
        SpatialGeometry::LineString(line) => Ok(line_length(line)),
        SpatialGeometry::MultiLineString(lines) => {
            Ok(lines.iter().map(|line| line_length(line)).sum())
        }
        _ => Err(DbError::sql(
            "ST_Length supports LINESTRING geometry inputs",
        )),
    }
}

pub(super) fn line_length(line: &[Position]) -> f64 {
    line.windows(2)
        .map(|segment| crate::spatial::distance::point_distance_xy(segment[0], segment[1]))
        .sum()
}

pub(super) fn geometry_area(geometry: &SpatialGeometry) -> Result<f64> {
    match geometry {
        SpatialGeometry::Polygon(polygon) => Ok(polygon_area(polygon).abs()),
        SpatialGeometry::MultiPolygon(polygons) => Ok(polygons
            .iter()
            .map(|polygon| polygon_area(polygon).abs())
            .sum()),
        _ => Err(DbError::sql("ST_Area supports POLYGON geometry inputs")),
    }
}

pub(super) fn polygon_area(polygon: &[Vec<Position>]) -> f64 {
    let Some(shell) = polygon.first() else {
        return 0.0;
    };
    let shell_area = ring_area(shell).abs();
    let holes = polygon
        .iter()
        .skip(1)
        .map(|ring| ring_area(ring).abs())
        .sum::<f64>();
    shell_area - holes
}

pub(super) fn ring_area(ring: &[Position]) -> f64 {
    if ring.len() < 3 {
        return 0.0;
    }
    ring.windows(2)
        .map(|segment| segment[0].x * segment[1].y - segment[1].x * segment[0].y)
        .sum::<f64>()
        * 0.5
}

pub(super) fn geography_area_approx_meters(geometry: &SpatialGeometry) -> Result<f64> {
    const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;
    let scale_polygon = |polygon: &[Vec<Position>]| -> f64 {
        let mean_lat = polygon
            .first()
            .map(|ring| ring.iter().map(|p| p.y).sum::<f64>() / ring.len().max(1) as f64)
            .unwrap_or(0.0)
            .to_radians();
        let meters_per_degree_lat = crate::spatial::distance::EARTH_RADIUS_METERS * DEG_TO_RAD;
        let meters_per_degree_lon = meters_per_degree_lat * mean_lat.cos().abs().max(1e-12);
        let scaled = polygon
            .iter()
            .map(|ring| {
                ring.iter()
                    .map(|p| Position::xy(p.x * meters_per_degree_lon, p.y * meters_per_degree_lat))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        polygon_area(&scaled).abs()
    };
    match geometry {
        SpatialGeometry::Polygon(polygon) => Ok(scale_polygon(polygon)),
        SpatialGeometry::MultiPolygon(polygons) => {
            Ok(polygons.iter().map(|polygon| scale_polygon(polygon)).sum())
        }
        _ => Err(DbError::sql("ST_Area supports POLYGON geography inputs")),
    }
}

pub(super) fn eval_greatest_least(values: &[Value], want_greatest: bool) -> Result<Value> {
    if values.is_empty() {
        return Err(DbError::sql(if want_greatest {
            "GREATEST expects at least 1 argument"
        } else {
            "LEAST expects at least 1 argument"
        }));
    }
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let mut best = values[0].clone();
    for value in &values[1..] {
        let ordering = compare_values(value, &best)?;
        if (want_greatest && ordering == std::cmp::Ordering::Greater)
            || (!want_greatest && ordering == std::cmp::Ordering::Less)
        {
            best = value.clone();
        }
    }
    Ok(best)
}

pub(super) fn eval_iif(values: &[Value]) -> Result<Value> {
    if values.len() != 3 {
        return Err(DbError::sql("IIF expects 3 arguments"));
    }
    Ok(if matches!(truthy(&values[0]), Some(true)) {
        values[1].clone()
    } else {
        values[2].clone()
    })
}

#[derive(Clone, Copy, Debug)]
pub(super) enum NumericValue {
    Int64(i64),
    Float64(f64),
    Decimal { scaled: i64, scale: u8 },
}

impl NumericValue {
    fn as_f64(self) -> f64 {
        match self {
            Self::Int64(value) => value as f64,
            Self::Float64(value) => value,
            Self::Decimal { scaled, scale } => (scaled as f64) / 10_f64.powi(i32::from(scale)),
        }
    }
}

pub(super) fn unary_text_fn(values: Vec<Value>, f: impl FnOnce(String) -> String) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("function expects one argument"));
    }
    match values.into_iter().next() {
        Some(Value::Text(value)) => Ok(Value::Text(f(value))),
        Some(Value::Null) => Ok(Value::Null),
        Some(other) => Err(DbError::sql(format!(
            "function expects text, got {other:?}"
        ))),
        None => Err(DbError::sql("function expects one argument")),
    }
}

pub(super) fn expect_text_arg<'a>(
    function_name: &str,
    ordinal: &str,
    value: &'a Value,
) -> Result<Option<&'a str>> {
    match value {
        Value::Text(value) => Ok(Some(value)),
        Value::Null => Ok(None),
        other => Err(DbError::sql(format!(
            "{function_name} expects text for {ordinal} argument, got {other:?}"
        ))),
    }
}

pub(super) fn expect_int_arg(
    function_name: &str,
    ordinal: &str,
    value: &Value,
) -> Result<Option<i64>> {
    match value {
        Value::Int64(value) => Ok(Some(*value)),
        Value::Null => Ok(None),
        other => Err(DbError::sql(format!(
            "{function_name} expects int for {ordinal} argument, got {other:?}"
        ))),
    }
}

pub(super) fn expect_numeric_arg(
    function_name: &str,
    ordinal: &str,
    value: &Value,
) -> Result<Option<NumericValue>> {
    match value {
        Value::Int64(value) => Ok(Some(NumericValue::Int64(*value))),
        Value::Float64(value) => Ok(Some(NumericValue::Float64(*value))),
        Value::Decimal { scaled, scale } => Ok(Some(NumericValue::Decimal {
            scaled: *scaled,
            scale: *scale,
        })),
        Value::Null => Ok(None),
        other => Err(DbError::sql(format!(
            "{function_name} expects numeric input for {ordinal} argument, got {other:?}"
        ))),
    }
}

pub(super) fn non_negative_usize(value: i64, function_name: &str, ordinal: &str) -> Result<usize> {
    if value < 0 {
        return Ok(0);
    }
    usize::try_from(value).map_err(|_| {
        DbError::sql(format!(
            "{function_name} {ordinal} argument is out of range"
        ))
    })
}

pub(super) fn take_left_chars(value: &str, len: usize) -> String {
    value.chars().take(len).collect()
}

pub(super) fn take_right_chars(value: &str, len: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    let start = chars.len().saturating_sub(len);
    chars[start..].iter().copied().collect()
}

pub(super) fn pad_left(value: &str, target_len: i64, pad: &str) -> String {
    let target_len = target_len.max(0) as usize;
    let current_len = value.chars().count();
    if target_len <= current_len {
        return take_left_chars(value, target_len);
    }
    if pad.is_empty() {
        return value.to_string();
    }
    let padding = repeat_to_char_len(pad, target_len - current_len);
    format!("{padding}{value}")
}

pub(super) fn pad_right(value: &str, target_len: i64, pad: &str) -> String {
    let target_len = target_len.max(0) as usize;
    let current_len = value.chars().count();
    if target_len <= current_len {
        return take_left_chars(value, target_len);
    }
    if pad.is_empty() {
        return value.to_string();
    }
    let padding = repeat_to_char_len(pad, target_len - current_len);
    format!("{value}{padding}")
}

pub(super) fn repeat_to_char_len(pattern: &str, len: usize) -> String {
    if len == 0 || pattern.is_empty() {
        return String::new();
    }
    let mut output = String::new();
    while output.chars().count() < len {
        output.push_str(pattern);
    }
    output.chars().take(len).collect()
}

pub(super) fn hex_encode_upper(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;

        let _ = write!(output, "{byte:02X}");
    }
    output
}

pub(super) fn hex_encode_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;

        let _ = write!(output, "{byte:02x}");
    }
    output
}

pub(super) fn eval_regexp_replace(
    input: &str,
    pattern: &str,
    replacement: &str,
    flags: Option<&str>,
) -> Result<String> {
    let mut case_insensitive = false;
    let mut global = false;
    if let Some(flags) = flags {
        for flag in flags.chars() {
            match flag {
                'i' | 'I' => case_insensitive = true,
                'g' | 'G' => global = true,
                _ => {
                    return Err(DbError::sql(format!(
                        "REGEXP_REPLACE flag {flag} is not supported"
                    )))
                }
            }
        }
    }
    let mut builder = regex::RegexBuilder::new(pattern);
    builder.case_insensitive(case_insensitive);
    let regex = builder
        .build()
        .map_err(|error| DbError::sql(format!("invalid regular expression: {error}")))?;
    if global {
        Ok(regex.replace_all(input, replacement).to_string())
    } else {
        Ok(regex.replace(input, replacement).to_string())
    }
}

pub(super) fn next_random_u64() -> u64 {
    let mut observed = RANDOM_STATE.load(Ordering::Relaxed);
    loop {
        let current = if observed == 0 {
            random_seed()
        } else {
            observed
        };
        let next = splitmix64(current);
        match RANDOM_STATE.compare_exchange_weak(
            observed,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return next,
            Err(actual) => observed = actual,
        }
    }
}

pub(super) fn next_random_f64() -> f64 {
    let value = next_random_u64();
    ((value >> 11) as f64) / ((1_u64 << 53) as f64)
}

pub(super) fn random_seed() -> u64 {
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos() as u64,
        Err(_) => 0xDDB5_EED5_A17C_E55D,
    };
    nanos ^ 0x9E37_79B9_7F4A_7C15
}

pub(super) fn splitmix64(state: u64) -> u64 {
    let mut value = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

pub(super) fn eval_current_timestamp(values: Vec<Value>) -> Result<Value> {
    if !values.is_empty() {
        return Err(DbError::sql("CURRENT_TIMESTAMP expects 0 arguments"));
    }
    Ok(Value::TimestampTzMicros(current_utc_timestamp_micros()?))
}

pub(super) fn eval_current_date(values: Vec<Value>) -> Result<Value> {
    if !values.is_empty() {
        return Err(DbError::sql("CURRENT_DATE expects 0 arguments"));
    }
    Ok(Value::DateDays(parse_date_days(&format_date(
        current_utc_datetime(),
    ))?))
}

pub(super) fn eval_current_time(values: Vec<Value>) -> Result<Value> {
    if !values.is_empty() {
        return Err(DbError::sql("CURRENT_TIME expects 0 arguments"));
    }
    Ok(Value::TimeMicros(parse_time_micros(&format_time(
        current_utc_datetime(),
    ))?))
}

pub(super) fn eval_date(values: Vec<Value>) -> Result<Value> {
    let Some(datetime) = resolve_datetime_arguments("DATE", &values, true)? else {
        return Ok(Value::Null);
    };
    Ok(Value::DateDays(parse_date_days(&format_date(datetime))?))
}

pub(super) fn eval_datetime(values: Vec<Value>) -> Result<Value> {
    let Some(datetime) = resolve_datetime_arguments("DATETIME", &values, true)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(format_datetime(datetime)))
}

pub(super) fn eval_strftime(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() {
        return Err(DbError::sql("STRFTIME expects at least 1 argument"));
    }
    let Some(format) = expect_text_arg("STRFTIME", "first", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(datetime) = resolve_datetime_arguments("STRFTIME", &values[1..], true)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(datetime.format(format).to_string()))
}

pub(super) fn eval_extract(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("EXTRACT expects 2 arguments"));
    }
    let Some(field) = expect_text_arg("EXTRACT", "first", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(datetime) = datetime_from_value("EXTRACT", &values[1])? else {
        return Ok(Value::Null);
    };
    match field.to_ascii_uppercase().as_str() {
        "YEAR" => Ok(Value::Int64(i64::from(datetime.year()))),
        "MONTH" => Ok(Value::Int64(i64::from(datetime.month()))),
        "DAY" => Ok(Value::Int64(i64::from(datetime.day()))),
        "HOUR" => Ok(Value::Int64(i64::from(datetime.hour()))),
        "MINUTE" => Ok(Value::Int64(i64::from(datetime.minute()))),
        "SECOND" => Ok(Value::Int64(i64::from(datetime.second()))),
        "DOW" => Ok(Value::Int64(i64::from(
            datetime.weekday().num_days_from_sunday(),
        ))),
        "DOY" => Ok(Value::Int64(i64::from(datetime.ordinal()))),
        "EPOCH" => Ok(Value::Float64(
            (datetime.timestamp_micros() as f64) / 1_000_000.0,
        )),
        other => Err(DbError::sql(format!(
            "EXTRACT field {other} is not supported"
        ))),
    }
}

pub(super) fn eval_date_trunc(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("DATE_TRUNC expects 2 arguments"));
    }
    let Some(precision) = expect_text_arg("DATE_TRUNC", "first", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(datetime) = datetime_from_value("DATE_TRUNC", &values[1])? else {
        return Ok(Value::Null);
    };
    let truncated = truncate_datetime(datetime, precision)?;
    Ok(Value::TimestampMicros(truncated.timestamp_micros()))
}

pub(super) fn eval_date_part(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("DATE_PART expects 2 arguments"));
    }
    eval_extract(values)
}

pub(super) fn eval_date_diff(values: Vec<Value>) -> Result<Value> {
    if values.len() != 3 {
        return Err(DbError::sql("DATE_DIFF expects 3 arguments"));
    }
    let Some(part) = expect_text_arg("DATE_DIFF", "first", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(start) = datetime_from_value("DATE_DIFF", &values[1])? else {
        return Ok(Value::Null);
    };
    let Some(end) = datetime_from_value("DATE_DIFF", &values[2])? else {
        return Ok(Value::Null);
    };
    let diff = date_diff_part(part, start, end)?;
    Ok(Value::Int64(diff))
}

pub(super) fn eval_last_day(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("LAST_DAY expects 1 argument"));
    }
    let Some(datetime) = datetime_from_value("LAST_DAY", &values[0])? else {
        return Ok(Value::Null);
    };
    let first_of_month = datetime
        .date_naive()
        .with_day(1)
        .ok_or_else(|| DbError::sql("LAST_DAY date is out of range"))?;
    let next_month = first_of_month
        .checked_add_months(Months::new(1))
        .ok_or_else(|| DbError::sql("LAST_DAY date is out of range"))?;
    let last_day = next_month
        .pred_opt()
        .ok_or_else(|| DbError::sql("LAST_DAY date is out of range"))?;
    Ok(Value::Text(last_day.format("%Y-%m-%d").to_string()))
}

pub(super) fn eval_next_day(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("NEXT_DAY expects 2 arguments"));
    }
    let Some(datetime) = datetime_from_value("NEXT_DAY", &values[0])? else {
        return Ok(Value::Null);
    };
    let Some(weekday_text) = expect_text_arg("NEXT_DAY", "second", &values[1])? else {
        return Ok(Value::Null);
    };
    let target = parse_weekday_name(weekday_text)?;
    let mut days_ahead = i64::from(target.num_days_from_monday())
        - i64::from(datetime.weekday().num_days_from_monday());
    if days_ahead <= 0 {
        days_ahead += 7;
    }
    let next = datetime
        .checked_add_signed(ChronoDuration::days(days_ahead))
        .ok_or_else(|| DbError::sql("NEXT_DAY result overflowed supported range"))?;
    Ok(Value::Text(
        next.date_naive().format("%Y-%m-%d").to_string(),
    ))
}

pub(super) fn eval_make_date(values: Vec<Value>) -> Result<Value> {
    if values.len() != 3 {
        return Err(DbError::sql("MAKE_DATE expects 3 arguments"));
    }
    let year = expect_int_arg("MAKE_DATE", "first", &values[0])?;
    let month = expect_int_arg("MAKE_DATE", "second", &values[1])?;
    let day = expect_int_arg("MAKE_DATE", "third", &values[2])?;
    let (Some(year), Some(month), Some(day)) = (year, month, day) else {
        return Ok(Value::Null);
    };
    let year = i32::try_from(year).map_err(|_| DbError::sql("MAKE_DATE year is out of range"))?;
    let month =
        u32::try_from(month).map_err(|_| DbError::sql("MAKE_DATE month is out of range"))?;
    let day = u32::try_from(day).map_err(|_| DbError::sql("MAKE_DATE day is out of range"))?;
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| DbError::sql("MAKE_DATE arguments are not a valid date"))?;
    Ok(Value::Text(date.format("%Y-%m-%d").to_string()))
}

pub(super) fn eval_make_timestamp(values: Vec<Value>) -> Result<Value> {
    if values.len() != 6 {
        return Err(DbError::sql("MAKE_TIMESTAMP expects 6 arguments"));
    }
    let year = expect_int_arg("MAKE_TIMESTAMP", "first", &values[0])?;
    let month = expect_int_arg("MAKE_TIMESTAMP", "second", &values[1])?;
    let day = expect_int_arg("MAKE_TIMESTAMP", "third", &values[2])?;
    let hour = expect_int_arg("MAKE_TIMESTAMP", "fourth", &values[3])?;
    let minute = expect_int_arg("MAKE_TIMESTAMP", "fifth", &values[4])?;
    let second = expect_numeric_arg("MAKE_TIMESTAMP", "sixth", &values[5])?;
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) =
        (year, month, day, hour, minute, second)
    else {
        return Ok(Value::Null);
    };
    let year =
        i32::try_from(year).map_err(|_| DbError::sql("MAKE_TIMESTAMP year is out of range"))?;
    let month =
        u32::try_from(month).map_err(|_| DbError::sql("MAKE_TIMESTAMP month is out of range"))?;
    let day = u32::try_from(day).map_err(|_| DbError::sql("MAKE_TIMESTAMP day is out of range"))?;
    let hour =
        u32::try_from(hour).map_err(|_| DbError::sql("MAKE_TIMESTAMP hour is out of range"))?;
    let minute =
        u32::try_from(minute).map_err(|_| DbError::sql("MAKE_TIMESTAMP minute is out of range"))?;
    let second_f64 = second.as_f64();
    if !(0.0..60.0).contains(&second_f64) {
        return Err(DbError::sql(
            "MAKE_TIMESTAMP seconds must be between 0 (inclusive) and 60 (exclusive)",
        ));
    }
    let second_whole = second_f64.floor() as u32;
    let micros = ((second_f64 - f64::from(second_whole)) * 1_000_000.0).round() as u32;
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| DbError::sql("MAKE_TIMESTAMP arguments are not a valid date"))?;
    let datetime = date
        .and_hms_micro_opt(hour, minute, second_whole, micros.min(999_999))
        .ok_or_else(|| DbError::sql("MAKE_TIMESTAMP arguments are not a valid timestamp"))?;
    Ok(Value::TimestampMicros(
        DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc).timestamp_micros(),
    ))
}

pub(super) fn eval_to_timestamp(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() || values.len() > 2 {
        return Err(DbError::sql("TO_TIMESTAMP expects 1 or 2 arguments"));
    }
    match (&values[0], values.get(1)) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Int64(epoch), None) => Ok(Value::TimestampMicros(
            epoch
                .checked_mul(1_000_000)
                .ok_or_else(|| DbError::sql("TO_TIMESTAMP epoch is out of range"))?,
        )),
        (Value::Float64(epoch), None) => Ok(Value::TimestampMicros((epoch * 1_000_000.0) as i64)),
        (Value::Text(text), None) => Ok(Value::TimestampMicros(
            parse_datetime_text("TO_TIMESTAMP", text)?.timestamp_micros(),
        )),
        (Value::Text(text), Some(Value::Text(format))) => Ok(Value::TimestampMicros(
            parse_to_timestamp_with_format(text, format)?.timestamp_micros(),
        )),
        (_, Some(Value::Null)) => Ok(Value::Null),
        (other, None) => Err(DbError::sql(format!(
            "TO_TIMESTAMP expects numeric epoch or text input, got {other:?}"
        ))),
        (_, Some(other)) => Err(DbError::sql(format!(
            "TO_TIMESTAMP format argument must be text, got {other:?}"
        ))),
    }
}

pub(super) fn eval_interval(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("INTERVAL expects 1 argument"));
    }
    let raw = match &values[0] {
        Value::Null => return Ok(Value::Null),
        Value::Text(raw) => raw.as_str(),
        other => {
            return Err(DbError::sql(format!(
                "INTERVAL expects text literal input, got {other:?}"
            )))
        }
    };
    let (months, days, micros) = parse_interval(raw)?;
    Ok(Value::Interval {
        months,
        days,
        micros,
    })
}

pub(super) fn eval_age(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() || values.len() > 2 {
        return Err(DbError::sql("AGE expects 1 or 2 arguments"));
    }
    let first = match datetime_from_value("AGE", &values[0])? {
        Some(value) => value,
        None => return Ok(Value::Null),
    };
    let second = if let Some(second) = values.get(1) {
        match datetime_from_value("AGE", second)? {
            Some(value) => value,
            None => return Ok(Value::Null),
        }
    } else {
        current_utc_datetime()
    };
    let delta = first.timestamp_micros() - second.timestamp_micros();
    Ok(Value::Text(format_age_interval(delta)))
}

pub(super) fn truncate_datetime(datetime: DateTime<Utc>, precision: &str) -> Result<DateTime<Utc>> {
    let lower = precision.to_ascii_lowercase();
    let date = datetime.date_naive();
    let time = datetime.time();
    let truncated = match lower.as_str() {
        "microsecond" | "microseconds" => datetime.naive_utc(),
        "millisecond" | "milliseconds" => datetime
            .naive_utc()
            .with_nanosecond((time.nanosecond() / 1_000_000) * 1_000_000)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "second" | "seconds" => date
            .and_hms_opt(time.hour(), time.minute(), time.second())
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "minute" | "minutes" => date
            .and_hms_opt(time.hour(), time.minute(), 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "hour" | "hours" => date
            .and_hms_opt(time.hour(), 0, 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "day" | "days" => date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "week" | "weeks" => {
            let start = date
                .checked_sub_signed(ChronoDuration::days(i64::from(
                    date.weekday().num_days_from_monday(),
                )))
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?;
            start
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        "month" | "months" => date
            .with_day(1)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "quarter" | "quarters" => {
            let month = ((date.month() - 1) / 3) * 3 + 1;
            NaiveDate::from_ymd_opt(date.year(), month, 1)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        "year" | "years" => NaiveDate::from_ymd_opt(date.year(), 1, 1)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?,
        "decade" | "decades" => {
            let year = date.year().div_euclid(10) * 10;
            NaiveDate::from_ymd_opt(year, 1, 1)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        "century" | "centuries" => {
            let year = ((date.year() - 1).div_euclid(100) * 100) + 1;
            NaiveDate::from_ymd_opt(year, 1, 1)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        "millennium" | "millennia" => {
            let year = ((date.year() - 1).div_euclid(1000) * 1000) + 1;
            NaiveDate::from_ymd_opt(year, 1, 1)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid date"))?
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| DbError::sql("DATE_TRUNC produced an invalid timestamp"))?
        }
        other => {
            return Err(DbError::sql(format!(
                "DATE_TRUNC precision {other} is not supported"
            )))
        }
    };
    Ok(DateTime::from_naive_utc_and_offset(truncated, Utc))
}

pub(super) fn date_diff_part(part: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<i64> {
    let part = part.to_ascii_lowercase();
    let micros = end.timestamp_micros() - start.timestamp_micros();
    let result = match part.as_str() {
        "microsecond" | "microseconds" => micros,
        "millisecond" | "milliseconds" => micros.div_euclid(1_000),
        "second" | "seconds" => micros.div_euclid(1_000_000),
        "minute" | "minutes" => micros.div_euclid(60 * 1_000_000),
        "hour" | "hours" => micros.div_euclid(60 * 60 * 1_000_000),
        "day" | "days" => micros.div_euclid(24 * 60 * 60 * 1_000_000),
        "week" | "weeks" => micros.div_euclid(7 * 24 * 60 * 60 * 1_000_000),
        "month" | "months" => {
            let start_date = start.date_naive();
            let end_date = end.date_naive();
            let mut months = i64::from(end_date.year() - start_date.year()) * 12
                + i64::from(end_date.month())
                - i64::from(start_date.month());
            if end_date.day() < start_date.day() {
                months -= 1;
            }
            months
        }
        "year" | "years" => {
            let start_date = start.date_naive();
            let end_date = end.date_naive();
            let mut years = i64::from(end_date.year() - start_date.year());
            if (end_date.month(), end_date.day()) < (start_date.month(), start_date.day()) {
                years -= 1;
            }
            years
        }
        other => {
            return Err(DbError::sql(format!(
                "DATE_DIFF part {other} is not supported"
            )))
        }
    };
    Ok(result)
}

pub(super) fn parse_weekday_name(value: &str) -> Result<chrono::Weekday> {
    match value.trim().to_ascii_lowercase().as_str() {
        "monday" | "mon" => Ok(chrono::Weekday::Mon),
        "tuesday" | "tue" | "tues" => Ok(chrono::Weekday::Tue),
        "wednesday" | "wed" => Ok(chrono::Weekday::Wed),
        "thursday" | "thu" | "thurs" => Ok(chrono::Weekday::Thu),
        "friday" | "fri" => Ok(chrono::Weekday::Fri),
        "saturday" | "sat" => Ok(chrono::Weekday::Sat),
        "sunday" | "sun" => Ok(chrono::Weekday::Sun),
        other => Err(DbError::sql(format!(
            "NEXT_DAY weekday {other} is not supported"
        ))),
    }
}

pub(super) fn parse_to_timestamp_with_format(text: &str, format: &str) -> Result<DateTime<Utc>> {
    let mapped = match format {
        "YYYY-MM-DD HH24:MI:SS" => "%Y-%m-%d %H:%M:%S",
        "YYYY-MM-DD" => "%Y-%m-%d",
        "DD/MM/YYYY" => "%d/%m/%Y",
        _ => {
            return Err(DbError::sql(
                "TO_TIMESTAMP format is not supported by DecentDB yet",
            ))
        }
    };
    if mapped.contains("%H") {
        let naive = NaiveDateTime::parse_from_str(text, mapped)
            .map_err(|_| DbError::sql("TO_TIMESTAMP input does not match format"))?;
        Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
    } else {
        let date = NaiveDate::parse_from_str(text, mapped)
            .map_err(|_| DbError::sql("TO_TIMESTAMP input does not match format"))?;
        let naive = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("TO_TIMESTAMP input is out of range"))?;
        Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
    }
}

pub(super) fn format_age_interval(delta_micros: i64) -> String {
    let negative = delta_micros < 0;
    let mut remainder = delta_micros.unsigned_abs();
    let day_micros = 24_u64 * 60 * 60 * 1_000_000;
    let hour_micros = 60_u64 * 60 * 1_000_000;
    let minute_micros = 60_u64 * 1_000_000;
    let second_micros = 1_000_000_u64;
    let days = remainder / day_micros;
    remainder %= day_micros;
    let hours = remainder / hour_micros;
    remainder %= hour_micros;
    let minutes = remainder / minute_micros;
    remainder %= minute_micros;
    let seconds = remainder / second_micros;
    let micros = remainder % second_micros;
    let sign = if negative { "-" } else { "" };
    if micros == 0 {
        format!("{sign}{days} days {hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{days} days {hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
    }
}

pub(super) fn eval_gen_random_uuid(values: Vec<Value>) -> Result<Value> {
    if !values.is_empty() {
        return Err(DbError::sql("GEN_RANDOM_UUID expects 0 arguments"));
    }
    let mut value = [0u8; 16];
    value[..8].copy_from_slice(&next_random_u64().to_be_bytes());
    value[8..].copy_from_slice(&next_random_u64().to_be_bytes());
    value[6] = (value[6] & 0x0f) | 0x40;
    value[8] = (value[8] & 0x3f) | 0x80;
    Ok(Value::Uuid(value))
}

pub(super) fn eval_uuid_parse(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("UUID_PARSE expects 1 argument"));
    }
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(value) => Ok(Value::Uuid(parse_uuid_text(value)?)),
        other => Err(DbError::sql(format!(
            "UUID_PARSE expects text input, got {other:?}"
        ))),
    }
}

pub(super) fn eval_uuid_to_string(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("UUID_TO_STRING expects 1 argument"));
    }
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Uuid(value) => Ok(Value::Text(value_to_text(&Value::Uuid(*value))?)),
        other => Err(DbError::sql(format!(
            "UUID_TO_STRING expects UUID input, got {other:?}"
        ))),
    }
}

pub(super) fn current_utc_datetime() -> DateTime<Utc> {
    Utc::now()
}

pub(super) fn current_utc_timestamp_micros() -> Result<i64> {
    let now = current_utc_datetime();
    let seconds = now.timestamp();
    let micros = i64::from(now.timestamp_subsec_micros());
    seconds
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(micros))
        .ok_or_else(|| DbError::sql("CURRENT_TIMESTAMP overflowed the supported range"))
}

pub(super) fn resolve_datetime_arguments(
    function_name: &str,
    values: &[Value],
    default_now: bool,
) -> Result<Option<DateTime<Utc>>> {
    if values.is_empty() {
        return if default_now {
            Ok(Some(current_utc_datetime()))
        } else {
            Err(DbError::sql(format!(
                "{function_name} expects a date/time argument"
            )))
        };
    }
    let Some(mut datetime) = datetime_from_value(function_name, &values[0])? else {
        return Ok(None);
    };
    for modifier in &values[1..] {
        let Some(modifier) = expect_text_arg(function_name, "modifier", modifier)? else {
            return Ok(None);
        };
        datetime = apply_datetime_modifier(function_name, datetime, modifier)?;
    }
    Ok(Some(datetime))
}

pub(super) fn datetime_from_value(
    function_name: &str,
    value: &Value,
) -> Result<Option<DateTime<Utc>>> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => parse_datetime_text(function_name, value).map(Some),
        Value::TimestampMicros(value) | Value::TimestampTzMicros(value) => {
            datetime_from_epoch_micros(function_name, *value).map(Some)
        }
        Value::DateDays(days) => {
            let micros = i64::from(*days)
                .checked_mul(EXEC_MICROS_PER_DAY)
                .ok_or_else(|| DbError::sql(format!("{function_name} date is out of range")))?;
            datetime_from_epoch_micros(function_name, micros).map(Some)
        }
        Value::TimeMicros(micros) => datetime_from_epoch_micros(function_name, *micros).map(Some),
        other => Err(DbError::sql(format!(
            "{function_name} expects text or date/time input, got {other:?}"
        ))),
    }
}

pub(super) fn datetime_from_epoch_micros(
    function_name: &str,
    micros: i64,
) -> Result<DateTime<Utc>> {
    Utc.timestamp_micros(micros)
        .single()
        .ok_or_else(|| DbError::sql(format!("{function_name} timestamp is out of range")))
}

pub(super) fn parse_datetime_text(function_name: &str, value: &str) -> Result<DateTime<Utc>> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("now") {
        return Ok(current_utc_datetime());
    }
    if let Ok(value) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(value.with_timezone(&Utc));
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(value) = NaiveDateTime::parse_from_str(trimmed, format) {
            return Ok(DateTime::from_naive_utc_and_offset(value, Utc));
        }
    }
    if let Ok(value) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let value = value
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| DbError::sql("date value is out of range"))?;
        return Ok(DateTime::from_naive_utc_and_offset(value, Utc));
    }
    Err(DbError::sql(format!(
        "{function_name} expects ISO-like date/time text or 'now'"
    )))
}

pub(super) fn apply_datetime_modifier(
    function_name: &str,
    datetime: DateTime<Utc>,
    modifier: &str,
) -> Result<DateTime<Utc>> {
    let modifier = modifier.trim();
    if modifier.is_empty() {
        return Err(DbError::sql(format!(
            "{function_name} date/time modifier must not be empty"
        )));
    }
    let (sign, body) = if let Some(rest) = modifier.strip_prefix('+') {
        (1_i64, rest)
    } else if let Some(rest) = modifier.strip_prefix('-') {
        (-1_i64, rest)
    } else {
        (1_i64, modifier)
    };
    let mut parts = body.split_whitespace();
    let amount = parts
        .next()
        .ok_or_else(|| DbError::sql(format!("{function_name} date/time modifier is invalid")))?
        .parse::<i64>()
        .map_err(|_| DbError::sql(format!("{function_name} date/time modifier is invalid")))?;
    let amount = amount
        .checked_mul(sign)
        .ok_or_else(|| DbError::sql(format!("{function_name} date/time modifier overflowed")))?;
    let unit = parts
        .next()
        .ok_or_else(|| DbError::sql(format!("{function_name} date/time modifier is invalid")))?;
    if parts.next().is_some() {
        return Err(DbError::sql(format!(
            "{function_name} date/time modifier is invalid"
        )));
    }
    match unit.to_ascii_lowercase().trim_end_matches('s') {
        "year" => shift_datetime_by_months(
            datetime,
            amount.checked_mul(12).ok_or_else(|| {
                DbError::sql(format!("{function_name} date/time modifier overflowed"))
            })?,
        ),
        "month" => shift_datetime_by_months(datetime, amount),
        "day" => shift_datetime_by_duration(function_name, datetime, ChronoDuration::days(amount)),
        "hour" => {
            shift_datetime_by_duration(function_name, datetime, ChronoDuration::hours(amount))
        }
        "minute" => {
            shift_datetime_by_duration(function_name, datetime, ChronoDuration::minutes(amount))
        }
        "second" => {
            shift_datetime_by_duration(function_name, datetime, ChronoDuration::seconds(amount))
        }
        other => Err(DbError::sql(format!(
            "{function_name} does not support date/time modifier unit {other}"
        ))),
    }
}

pub(super) fn shift_datetime_by_duration(
    function_name: &str,
    datetime: DateTime<Utc>,
    duration: ChronoDuration,
) -> Result<DateTime<Utc>> {
    datetime.checked_add_signed(duration).ok_or_else(|| {
        DbError::sql(format!(
            "{function_name} date/time modifier overflowed the supported range"
        ))
    })
}

pub(super) fn shift_datetime_by_months(
    datetime: DateTime<Utc>,
    months: i64,
) -> Result<DateTime<Utc>> {
    if months == 0 {
        return Ok(datetime);
    }
    let magnitude = months
        .checked_abs()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| DbError::sql("date/time month modifier is out of range"))?;
    let naive = if months > 0 {
        datetime
            .naive_utc()
            .checked_add_months(Months::new(magnitude))
    } else {
        datetime
            .naive_utc()
            .checked_sub_months(Months::new(magnitude))
    };
    naive
        .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
        .ok_or_else(|| DbError::sql("date/time month modifier overflowed the supported range"))
}

pub(super) fn format_date(datetime: DateTime<Utc>) -> String {
    datetime.format("%Y-%m-%d").to_string()
}

pub(super) fn format_time(datetime: DateTime<Utc>) -> String {
    datetime.format("%H:%M:%S").to_string()
}

pub(super) fn format_datetime(datetime: DateTime<Utc>) -> String {
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub(super) fn parse_uuid_text(value: &str) -> Result<[u8; 16]> {
    let compact = if value.len() == 36 {
        if value.as_bytes().get(8) != Some(&b'-')
            || value.as_bytes().get(13) != Some(&b'-')
            || value.as_bytes().get(18) != Some(&b'-')
            || value.as_bytes().get(23) != Some(&b'-')
        {
            return Err(DbError::sql("UUID_PARSE expects canonical UUID text"));
        }
        value.replace('-', "")
    } else if value.len() == 32 {
        value.to_string()
    } else {
        return Err(DbError::sql("UUID_PARSE expects canonical UUID text"));
    };
    if compact.len() != 32 {
        return Err(DbError::sql("UUID_PARSE expects canonical UUID text"));
    }
    let mut uuid = [0u8; 16];
    for (index, chunk) in compact.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk)
            .map_err(|_| DbError::sql("UUID_PARSE expects canonical UUID text"))?;
        uuid[index] = u8::from_str_radix(text, 16)
            .map_err(|_| DbError::sql("UUID_PARSE expects canonical UUID text"))?;
    }
    Ok(uuid)
}

pub(super) fn eval_json_binary_operator(
    left: &Value,
    right: &Value,
    text_mode: bool,
) -> Result<Value> {
    let Some(target) = json_operator_target(left, right)? else {
        return Ok(Value::Null);
    };
    if text_mode {
        match target {
            JsonValue::Null => Ok(Value::Null),
            JsonValue::String(value) => Ok(Value::Text(value)),
            JsonValue::Number(value) => Ok(Value::Text(value)),
            JsonValue::Bool(value) => Ok(Value::Text(if value {
                "true".to_string()
            } else {
                "false".to_string()
            })),
            other => Ok(Value::Text(other.render_json())),
        }
    } else {
        Ok(Value::Text(target.render_json()))
    }
}

pub(super) fn json_operator_target(left: &Value, right: &Value) -> Result<Option<JsonValue>> {
    let json = match left {
        Value::Null => return Ok(None),
        Value::Text(value) => parse_json(value)?,
        other => {
            return Err(DbError::sql(format!(
                "JSON operators expect text JSON input, got {other:?}"
            )))
        }
    };
    match right {
        Value::Null => Ok(None),
        Value::Text(path) if path.starts_with('$') => {
            let path = parse_json_path(path)?;
            Ok(json.lookup(&path).cloned())
        }
        Value::Text(key) => match &json {
            JsonValue::Object(object) => Ok(object.get(key).cloned()),
            JsonValue::Array(array) => {
                let Ok(index) = key.parse::<usize>() else {
                    return Ok(None);
                };
                Ok(array.get(index).cloned())
            }
            _ => Ok(None),
        },
        Value::Int64(index) => {
            let Ok(index) = usize::try_from(*index) else {
                return Ok(None);
            };
            match &json {
                JsonValue::Array(array) => Ok(array.get(index).cloned()),
                _ => Ok(None),
            }
        }
        other => Err(DbError::sql(format!(
            "JSON operators expect text keys or integer indexes, got {other:?}"
        ))),
    }
}

pub(super) fn expand_json_each_rows(value: &Value) -> Result<Vec<Vec<Value>>> {
    let Some(json) = json_table_input("json_each", value)? else {
        return Ok(Vec::new());
    };
    match json {
        JsonValue::Object(object) => object
            .into_iter()
            .map(|(key, value)| json_table_row(Value::Text(key), value, None))
            .collect(),
        JsonValue::Array(array) => array
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                json_table_row(
                    Value::Int64(i64::try_from(index).unwrap_or(i64::MAX)),
                    value,
                    None,
                )
            })
            .collect(),
        other => Ok(vec![json_table_row(Value::Null, other, None)?]),
    }
}

pub(super) fn expand_json_tree_rows(value: &Value) -> Result<Vec<Vec<Value>>> {
    let Some(json) = json_table_input("json_tree", value)? else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    append_json_tree_rows(Value::Null, json, "$".to_string(), &mut rows)?;
    Ok(rows)
}

pub(super) fn json_table_input(function_name: &str, value: &Value) -> Result<Option<JsonValue>> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => parse_json(value).map(Some),
        other => Err(DbError::sql(format!(
            "{function_name} expects text JSON input, got {other:?}"
        ))),
    }
}

pub(super) fn generate_series_rows(values: &[Value]) -> Result<Vec<Value>> {
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Vec::new());
    }
    match values {
        [Value::Int64(start), Value::Int64(stop)] => {
            generate_int_series_rows(*start, *stop, 1)
        }
        [Value::Int64(start), Value::Int64(stop), Value::Int64(step)] => {
            generate_int_series_rows(*start, *stop, *step)
        }
        [
            Value::TimestampMicros(start),
            Value::TimestampMicros(stop),
            Value::Interval {
                months,
                days,
                micros,
            },
        ] => generate_timestamp_series_rows(*start, *stop, *months, *days, *micros),
        [
            Value::DateDays(start),
            Value::DateDays(stop),
            Value::Interval {
                months,
                days,
                micros,
            },
        ] => generate_date_series_rows(*start, *stop, *months, *days, *micros),
        [Value::TimestampMicros(_), Value::TimestampMicros(_)]
        | [Value::DateDays(_), Value::DateDays(_)] => Err(DbError::sql(
            "temporal generate_series requires an INTERVAL step argument",
        )),
        _ => Err(DbError::sql(
            "generate_series supports INT64 start/stop[/step], TIMESTAMP start/stop/INTERVAL step, and DATE start/stop/INTERVAL step",
        )),
    }
}

pub(super) fn generate_int_series_rows(start: i64, stop: i64, step: i64) -> Result<Vec<Value>> {
    if step == 0 {
        return Err(DbError::sql("generate_series step cannot be zero"));
    }
    let ascending = step > 0;
    if (ascending && start > stop) || (!ascending && start < stop) {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let mut current = start;
    loop {
        push_generate_series_value(&mut rows, Value::Int64(current))?;
        match current.checked_add(step) {
            Some(next) if (ascending && next <= stop) || (!ascending && next >= stop) => {
                current = next;
            }
            Some(_) => break,
            None => break,
        }
    }
    Ok(rows)
}

pub(super) fn generate_timestamp_series_rows(
    start: i64,
    stop: i64,
    months: i32,
    days: i32,
    micros: i64,
) -> Result<Vec<Value>> {
    let first_next = apply_interval_to_timestamp_micros(start, months, days, micros, true)?;
    let direction = first_next.cmp(&start);
    if direction == std::cmp::Ordering::Equal {
        return Err(DbError::sql("generate_series step cannot be zero"));
    }
    if (direction == std::cmp::Ordering::Greater && start > stop)
        || (direction == std::cmp::Ordering::Less && start < stop)
    {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    let mut current = start;
    loop {
        push_generate_series_value(&mut rows, Value::TimestampMicros(current))?;
        let next = apply_interval_to_timestamp_micros(current, months, days, micros, true)?;
        if next == current {
            return Err(DbError::sql("generate_series step cannot be zero"));
        }
        let in_range = if direction == std::cmp::Ordering::Greater {
            next <= stop
        } else {
            next >= stop
        };
        if !in_range {
            break;
        }
        current = next;
    }
    Ok(rows)
}

pub(super) fn generate_date_series_rows(
    start: i32,
    stop: i32,
    months: i32,
    days: i32,
    micros: i64,
) -> Result<Vec<Value>> {
    if months != 0 || micros != 0 {
        return Err(DbError::sql(
            "DATE generate_series currently requires an INTERVAL with whole days only",
        ));
    }
    if days == 0 {
        return Err(DbError::sql("generate_series step cannot be zero"));
    }
    let ascending = days > 0;
    if (ascending && start > stop) || (!ascending && start < stop) {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let mut current = start;
    loop {
        push_generate_series_value(&mut rows, Value::DateDays(current))?;
        match current.checked_add(days) {
            Some(next) if (ascending && next <= stop) || (!ascending && next >= stop) => {
                current = next;
            }
            Some(_) => break,
            None => break,
        }
    }
    Ok(rows)
}

pub(super) fn push_generate_series_value(rows: &mut Vec<Value>, value: Value) -> Result<()> {
    if rows.len() >= GENERATE_SERIES_MAX_ROWS {
        return Err(DbError::sql(format!(
            "generate_series produced more than {GENERATE_SERIES_MAX_ROWS} rows"
        )));
    }
    rows.push(value);
    Ok(())
}

pub(super) fn one_text_arg(function_name: &str, values: Vec<Value>) -> Result<String> {
    if values.len() != 1 {
        return Err(DbError::sql(format!("{function_name} expects 1 argument")));
    }
    match values.into_iter().next().unwrap_or(Value::Null) {
        Value::Text(value) => Ok(value),
        Value::Null => Ok(String::new()),
        other => Err(DbError::sql(format!(
            "{function_name} expects a text argument, got {other:?}"
        ))),
    }
}

pub(super) fn visible_columns(table_name: &str, names: &[&str]) -> Vec<ColumnBinding> {
    names
        .iter()
        .map(|name| ColumnBinding::visible(Some(table_name.to_string()), (*name).to_string()))
        .collect()
}

pub(super) fn compat_catalog_object_is_visible(name: &str) -> bool {
    !name.starts_with("__decentdb_")
}

pub(super) fn pragma_table_info_dataset(
    table_name: String,
    columns: &[ColumnSchema],
    extended: bool,
) -> Dataset {
    let mut rows = Vec::new();
    for (cid, column) in columns.iter().enumerate() {
        let mut row = vec![
            Value::Int64(i64::try_from(cid).unwrap_or(i64::MAX)),
            Value::Text(column.name.clone()),
            Value::Text(column.column_type.as_str().to_string()),
            Value::Int64(if column.nullable { 0 } else { 1 }),
            column.default_sql.clone().map_or(Value::Null, Value::Text),
            Value::Int64(if column.primary_key { 1 } else { 0 }),
        ];
        if extended {
            let hidden = if column.generated_sql.is_none() {
                0
            } else if column.generated_stored {
                3
            } else {
                2
            };
            row.push(Value::Int64(hidden));
        }
        rows.push(row);
    }
    let names = if extended {
        vec![
            "cid",
            "name",
            "type",
            "notnull",
            "dflt_value",
            "pk",
            "hidden",
        ]
    } else {
        vec!["cid", "name", "type", "notnull", "dflt_value", "pk"]
    };
    Dataset::with_rows(visible_columns(&table_name, &names), rows)
}

pub(super) fn table_list_row(
    schema: &str,
    name: &str,
    kind: &str,
    column_count: usize,
) -> Vec<Value> {
    vec![
        Value::Text(schema.to_string()),
        Value::Text(name.to_string()),
        Value::Text(kind.to_string()),
        Value::Int64(i64::try_from(column_count).unwrap_or(i64::MAX)),
        Value::Int64(0),
        Value::Int64(0),
    ]
}

pub(super) fn index_info_rows(index: &IndexSchema, extended: bool) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for (seqno, column) in index.columns.iter().enumerate() {
        let cid = if column.column_name.is_some() {
            seqno as i64
        } else {
            -2
        };
        let name = column
            .column_name
            .clone()
            .or_else(|| column.expression_sql.clone())
            .map_or(Value::Null, Value::Text);
        let mut row = vec![
            Value::Int64(i64::try_from(seqno).unwrap_or(i64::MAX)),
            Value::Int64(cid),
            name,
        ];
        if extended {
            row.extend([
                Value::Int64(0),
                Value::Text("BINARY".to_string()),
                Value::Int64(1),
            ]);
        }
        rows.push(row);
    }
    if extended {
        let base = rows.len();
        for (offset, include) in index.include_columns.iter().enumerate() {
            rows.push(vec![
                Value::Int64(i64::try_from(base + offset).unwrap_or(i64::MAX)),
                Value::Int64(-1),
                Value::Text(include.clone()),
                Value::Int64(0),
                Value::Text("BINARY".to_string()),
                Value::Int64(0),
            ]);
        }
    }
    rows
}

pub(super) fn foreign_key_rows(table: &TableSchema) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for (id, foreign_key) in table.foreign_keys.iter().enumerate() {
        for (seq, (from, to)) in foreign_key
            .columns
            .iter()
            .zip(foreign_key.referenced_columns.iter())
            .enumerate()
        {
            rows.push(vec![
                Value::Int64(i64::try_from(id).unwrap_or(i64::MAX)),
                Value::Int64(i64::try_from(seq).unwrap_or(i64::MAX)),
                Value::Text(foreign_key.referenced_table.clone()),
                Value::Text(from.clone()),
                Value::Text(to.clone()),
                Value::Text(foreign_key_action_sqlite_name(foreign_key.on_update)),
                Value::Text(foreign_key_action_sqlite_name(foreign_key.on_delete)),
                Value::Text("NONE".to_string()),
            ]);
        }
    }
    rows
}

pub(super) fn foreign_key_action_sqlite_name(action: ForeignKeyAction) -> String {
    match action {
        ForeignKeyAction::NoAction => "NO ACTION",
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::Cascade => "CASCADE",
        ForeignKeyAction::SetNull => "SET NULL",
    }
    .to_string()
}

pub(super) fn sqlite_schema_row(
    kind: &str,
    name: &str,
    tbl_name: &str,
    sql: Option<String>,
) -> Vec<Value> {
    vec![
        Value::Text(kind.to_string()),
        Value::Text(name.to_string()),
        Value::Text(tbl_name.to_string()),
        Value::Int64(0),
        sql.map_or(Value::Null, Value::Text),
    ]
}

pub(super) fn information_schema_schemata_row(schema: &str) -> Vec<Value> {
    vec![
        Value::Text("main".to_string()),
        Value::Text(schema.to_string()),
        Value::Text("decentdb".to_string()),
        Value::Text("main".to_string()),
        Value::Text(schema.to_string()),
        Value::Text("UTF-8".to_string()),
    ]
}

pub(super) fn information_schema_table_row(
    schema: &str,
    name: &str,
    table_type: &str,
) -> Vec<Value> {
    vec![
        Value::Text("main".to_string()),
        Value::Text(schema.to_string()),
        Value::Text(name.to_string()),
        Value::Text(table_type.to_string()),
    ]
}

pub(super) fn information_schema_column_rows(
    schema: &str,
    table_name: &str,
    columns: &[ColumnSchema],
) -> Vec<Vec<Value>> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            vec![
                Value::Text("main".to_string()),
                Value::Text(schema.to_string()),
                Value::Text(table_name.to_string()),
                Value::Text(column.name.clone()),
                Value::Int64(i64::try_from(index + 1).unwrap_or(i64::MAX)),
                column.default_sql.clone().map_or(Value::Null, Value::Text),
                Value::Text(if column.nullable { "YES" } else { "NO" }.to_string()),
                Value::Text(column.column_type.as_str().to_string()),
            ]
        })
        .collect()
}

pub(super) fn render_compat_create_table(table: &TableSchema) -> String {
    let columns = table
        .columns
        .iter()
        .map(|column| {
            format!(
                "{} {}",
                sql_identifier_exec(&column.name),
                column.column_type.as_str()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE {}TABLE {} ({});",
        if table.temporary { "TEMP " } else { "" },
        sql_identifier_exec(&table.name),
        columns
    )
}

pub(super) fn render_compat_create_view(view: &ViewSchema) -> String {
    format!(
        "CREATE {}VIEW {} AS {};",
        if view.temporary { "TEMP " } else { "" },
        sql_identifier_exec(&view.name),
        view.sql_text
    )
}

pub(super) fn render_compat_create_index(index: &IndexSchema) -> String {
    let columns = index
        .columns
        .iter()
        .map(|column| {
            column
                .column_name
                .as_deref()
                .map(sql_identifier_exec)
                .or_else(|| column.expression_sql.clone())
                .unwrap_or_else(|| "\"expr\"".to_string())
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE {}INDEX {} ON {} ({});",
        if index.unique { "UNIQUE " } else { "" },
        sql_identifier_exec(&index.name),
        sql_identifier_exec(&index.table_name),
        columns
    )
}

pub(super) fn render_compat_create_trigger(trigger: &crate::catalog::TriggerSchema) -> String {
    format!(
        "CREATE TRIGGER {} {} {} ON {} FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql({});",
        sql_identifier_exec(&trigger.name),
        trigger_kind_name_exec(trigger.kind),
        trigger_event_name_exec(trigger.event),
        sql_identifier_exec(&trigger.target_name),
        sql_string_literal_exec(&trigger.action_sql)
    )
}

pub(super) fn trigger_kind_name_exec(kind: TriggerKind) -> &'static str {
    match kind {
        TriggerKind::After => "AFTER",
        TriggerKind::InsteadOf => "INSTEAD OF",
    }
}

pub(super) fn trigger_event_name_exec(event: TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update => "UPDATE",
        TriggerEvent::Delete => "DELETE",
    }
}

pub(super) fn sql_identifier_exec(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) fn sql_string_literal_exec(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) fn append_json_tree_rows(
    key: Value,
    value: JsonValue,
    path: String,
    rows: &mut Vec<Vec<Value>>,
) -> Result<()> {
    rows.push(json_table_row(key, value.clone(), Some(path.clone()))?);
    match value {
        JsonValue::Object(object) => {
            for (child_key, child_value) in object {
                append_json_tree_rows(
                    Value::Text(child_key.clone()),
                    child_value,
                    format!("{path}.{child_key}"),
                    rows,
                )?;
            }
        }
        JsonValue::Array(array) => {
            for (index, child_value) in array.into_iter().enumerate() {
                append_json_tree_rows(
                    Value::Int64(i64::try_from(index).unwrap_or(i64::MAX)),
                    child_value,
                    format!("{path}[{index}]"),
                    rows,
                )?;
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::String(_) | JsonValue::Number(_) => {}
    }
    Ok(())
}

pub(super) fn json_table_row(
    key: Value,
    value: JsonValue,
    path: Option<String>,
) -> Result<Vec<Value>> {
    let mut row = vec![
        key,
        json_value_to_value(value.clone())?,
        Value::Text(json_type_name(&value).to_string()),
    ];
    if let Some(path) = path {
        row.push(Value::Text(path));
    }
    Ok(row)
}

pub(super) fn eval_json_array(values: Vec<Value>) -> Result<Value> {
    let items = values
        .iter()
        .map(json_value_from_value)
        .collect::<Result<Vec<_>>>()?;
    Ok(Value::Text(JsonValue::Array(items).render_json()))
}

pub(super) fn eval_json_object(values: Vec<Value>) -> Result<Value> {
    if !values.len().is_multiple_of(2) {
        return Err(DbError::sql(
            "json_object expects an even number of arguments",
        ));
    }
    let mut object = BTreeMap::new();
    for pair in values.chunks_exact(2) {
        let key = match &pair[0] {
            Value::Text(value) => value.clone(),
            Value::Null => return Err(DbError::sql("json_object keys cannot be NULL")),
            other => {
                return Err(DbError::sql(format!(
                    "json_object keys must be text, got {other:?}"
                )))
            }
        };
        object.insert(key, json_value_from_value(&pair[1])?);
    }
    Ok(Value::Text(JsonValue::Object(object).render_json()))
}

pub(super) fn eval_json_type(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() || values.len() > 2 {
        return Err(DbError::sql("json_type expects 1 or 2 arguments"));
    }
    let Some(target) = json_target_value(&values)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(json_type_name(&target).to_string()))
}

pub(super) fn eval_json_valid(values: Vec<Value>) -> Result<Value> {
    if values.len() != 1 {
        return Err(DbError::sql("json_valid expects 1 argument"));
    }
    match &values[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(value) => Ok(Value::Bool(parse_json(value).is_ok())),
        other => Err(DbError::sql(format!(
            "json_valid expects text input, got {other:?}"
        ))),
    }
}

pub(super) fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(true) => "true",
        JsonValue::Bool(false) => "false",
        JsonValue::String(_) => "text",
        JsonValue::Number(number) => {
            if number.contains('.') {
                "real"
            } else {
                "integer"
            }
        }
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

pub(super) fn json_value_from_value(value: &Value) -> Result<JsonValue> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Int64(value) => Ok(JsonValue::Number(value.to_string())),
        Value::Float64(value) => Ok(JsonValue::Number(value.to_string())),
        Value::Bool(value) => Ok(JsonValue::Bool(*value)),
        Value::Text(value) => Ok(JsonValue::String(value.clone())),
        Value::Blob(_) | Value::Geometry(_) | Value::Geography(_) => {
            Err(DbError::sql("cannot encode binary value as JSON"))
        }
        Value::Decimal { scaled, scale } => {
            Ok(JsonValue::Number(decimal_to_string(*scaled, *scale)))
        }
        Value::Uuid(value) => Ok(JsonValue::String(value_to_text(&Value::Uuid(*value))?)),
        Value::TimestampMicros(value) => Ok(JsonValue::Number(value.to_string())),
        Value::Enum {
            enum_type_id,
            label_id,
        } => Ok(JsonValue::String(format!("{enum_type_id}:{label_id}"))),
        Value::IpAddr { family, addr } => Ok(JsonValue::String(format_ip_addr(*family, addr)?)),
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => Ok(JsonValue::String(format_cidr(
            *family,
            *prefix_len,
            network,
        )?)),
        Value::MacAddr { len, bytes } => Ok(JsonValue::String(format_mac_addr(*len, bytes)?)),
        Value::DateDays(days) => Ok(JsonValue::String(format_date_days(*days))),
        Value::TimeMicros(micros) => Ok(JsonValue::String(format_time_micros(*micros)?)),
        Value::TimestampTzMicros(micros) => {
            Ok(JsonValue::String(format_timestamp_tz_micros(*micros)))
        }
        Value::Interval {
            months,
            days,
            micros,
        } => Ok(JsonValue::String(format_interval(*months, *days, *micros))),
    }
}

pub(super) fn sort_aggregate_row_indexes(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    row_indexes: &[usize],
    order_by: &[crate::sql::ast::OrderBy],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Vec<usize>> {
    if order_by.is_empty() {
        return Ok(row_indexes.to_vec());
    }
    let mut keyed = row_indexes
        .iter()
        .map(|row_index| {
            let row = dataset
                .rows
                .get(*row_index)
                .map(Vec::as_slice)
                .ok_or_else(|| DbError::internal("group row index is invalid"))?;
            let keys = order_by
                .iter()
                .map(|order| runtime.eval_expr(&order.expr, dataset, row, params, ctes, None))
                .collect::<Result<Vec<_>>>()?;
            Ok((*row_index, keys))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut sort_error = None;
    keyed.sort_by(|(left_index, left_keys), (right_index, right_keys)| {
        for (order, (left, right)) in order_by.iter().zip(left_keys.iter().zip(right_keys.iter())) {
            let ordering = match compare_values_with_runtime_collation(
                Some(runtime),
                left,
                right,
                order.collation.clone(),
            ) {
                Ok(ordering) => ordering,
                Err(error) => {
                    if sort_error.is_none() {
                        sort_error = Some(error);
                    }
                    std::cmp::Ordering::Equal
                }
            };
            if ordering != std::cmp::Ordering::Equal {
                return if order.descending {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        left_index.cmp(right_index)
    });
    if let Some(error) = sort_error {
        return Err(error);
    }
    Ok(keyed.into_iter().map(|(row_index, _)| row_index).collect())
}

pub(super) fn values_equal(left: &Value, right: &Value) -> Result<bool> {
    Ok(compare_values(left, right)? == std::cmp::Ordering::Equal)
}

pub(super) fn value_to_numeric_f64(value: Value, fn_name: &str) -> Result<Option<f64>> {
    match value {
        Value::Null => Ok(None),
        Value::Int64(value) => Ok(Some(value as f64)),
        Value::Float64(value) => Ok(Some(value)),
        Value::Decimal { scaled, scale } => {
            Ok(Some((scaled as f64) / 10_f64.powi(i32::from(scale))))
        }
        other => Err(DbError::sql(format!(
            "{fn_name} expects numeric values, got {other:?}"
        ))),
    }
}

pub(super) fn parse_percentile_fraction(value: Value, fn_name: &str) -> Result<f64> {
    let Some(fraction) = value_to_numeric_f64(value, fn_name)? else {
        return Err(DbError::sql(format!("{fn_name} fraction cannot be NULL")));
    };
    if !(0.0..=1.0).contains(&fraction) {
        return Err(DbError::sql(format!(
            "{fn_name} fraction must be between 0 and 1"
        )));
    }
    Ok(fraction)
}

pub(super) fn aggregate_array_agg(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    distinct: bool,
    order_by: &[crate::sql::ast::OrderBy],
) -> Result<Value> {
    let ordered_indexes = sort_aggregate_row_indexes(
        ctx.runtime,
        ctx.dataset,
        row_indexes,
        order_by,
        ctx.params,
        ctx.ctes,
    )?;
    let mut values = Vec::new();
    let mut seen_values = Vec::<Value>::new();
    for row_index in ordered_indexes {
        let row = ctx
            .dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = ctx.eval_row(row, expr)?;
        if distinct {
            if seen_values
                .iter()
                .map(|seen| values_equal(seen, &value))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .any(std::convert::identity)
            {
                continue;
            }
            seen_values.push(value.clone());
        }
        values.push(json_value_from_value(&value)?);
    }
    Ok(Value::Text(JsonValue::Array(values).render_json()))
}

pub(super) fn aggregate_median(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    expr: &Expr,
    distinct: bool,
) -> Result<Value> {
    let mut values = Vec::new();
    for row_index in row_indexes {
        let row = ctx
            .dataset
            .rows
            .get(*row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        if let Some(number) = value_to_numeric_f64(ctx.eval_row(row, expr)?, "MEDIAN")? {
            values.push(number);
        }
    }
    if distinct {
        values.sort_by(|a, b| a.total_cmp(b));
        values.dedup_by(|a, b| a.total_cmp(b) == std::cmp::Ordering::Equal);
    } else {
        values.sort_by(|a, b| a.total_cmp(b));
    }
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        Ok(Value::Float64(values[mid]))
    } else {
        Ok(Value::Float64((values[mid - 1] + values[mid]) / 2.0))
    }
}

pub(super) fn aggregate_percentile_cont(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    row_indexes: &[usize],
    fraction_expr: &Expr,
    order_by: &[crate::sql::ast::OrderBy],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Value> {
    if order_by.len() != 1 {
        return Err(DbError::sql(
            "PERCENTILE_CONT requires exactly one ORDER BY expression",
        ));
    }
    let fraction = parse_percentile_fraction(
        runtime.eval_expr(fraction_expr, dataset, &[], params, ctes, None)?,
        "PERCENTILE_CONT",
    )?;
    let ordered_indexes =
        sort_aggregate_row_indexes(runtime, dataset, row_indexes, order_by, params, ctes)?;
    let mut values = Vec::new();
    for row_index in ordered_indexes {
        let row = dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let order_value = runtime.eval_expr(&order_by[0].expr, dataset, row, params, ctes, None)?;
        if let Some(number) = value_to_numeric_f64(order_value, "PERCENTILE_CONT")? {
            values.push(number);
        }
    }
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let max_index = (values.len() - 1) as f64;
    let position = fraction * max_index;
    let lower_index = position.floor() as usize;
    let upper_index = position.ceil() as usize;
    if lower_index == upper_index {
        Ok(Value::Float64(values[lower_index]))
    } else {
        let lower = values[lower_index];
        let upper = values[upper_index];
        let weight = position - (lower_index as f64);
        Ok(Value::Float64(lower + (upper - lower) * weight))
    }
}

pub(super) fn aggregate_percentile_disc(
    runtime: &EngineRuntime,
    dataset: &Dataset,
    row_indexes: &[usize],
    fraction_expr: &Expr,
    order_by: &[crate::sql::ast::OrderBy],
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Value> {
    if order_by.len() != 1 {
        return Err(DbError::sql(
            "PERCENTILE_DISC requires exactly one ORDER BY expression",
        ));
    }
    let fraction = parse_percentile_fraction(
        runtime.eval_expr(fraction_expr, dataset, &[], params, ctes, None)?,
        "PERCENTILE_DISC",
    )?;
    let ordered_indexes =
        sort_aggregate_row_indexes(runtime, dataset, row_indexes, order_by, params, ctes)?;
    let mut values = Vec::new();
    for row_index in ordered_indexes {
        let row = dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let order_value = runtime.eval_expr(&order_by[0].expr, dataset, row, params, ctes, None)?;
        if !matches!(order_value, Value::Null) {
            values.push(order_value);
        }
    }
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let threshold = ((values.len() as f64) * fraction).ceil() as usize;
    let index = threshold.saturating_sub(1).min(values.len() - 1);
    Ok(values[index].clone())
}

pub(super) fn decimal_to_string(scaled: i64, scale: u8) -> String {
    if scale == 0 {
        return scaled.to_string();
    }
    let negative = scaled < 0;
    let digits = scaled.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    let mut output = format!("{}.{}", &padded[..split], &padded[split..]);
    if negative {
        output.insert(0, '-');
    }
    output
}

pub(super) fn eval_like(
    left: Value,
    right: Value,
    escape: Option<Value>,
    case_insensitive: bool,
    negated: bool,
) -> Result<Value> {
    let escape = normalize_like_escape(escape)?;
    match (left, right) {
        (Value::Text(left), Value::Text(right)) => {
            let matches = like_match(&left, &right, case_insensitive, escape);
            Ok(Value::Bool(if negated { !matches } else { matches }))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        other => Err(DbError::sql(format!(
            "LIKE expects text values, got {other:?}"
        ))),
    }
}

pub(super) fn normalize_like_escape(escape: Option<Value>) -> Result<Option<char>> {
    match escape {
        None => Ok(None),
        Some(Value::Null) => Ok(None),
        Some(Value::Text(text)) => {
            let mut chars = text.chars();
            let Some(ch) = chars.next() else {
                return Ok(None);
            };
            if chars.next().is_some() {
                return Err(DbError::sql(
                    "LIKE ESCAPE expression must evaluate to a single character",
                ));
            }
            Ok(Some(ch))
        }
        Some(other) => Err(DbError::sql(format!(
            "LIKE ESCAPE expects text, got {other:?}"
        ))),
    }
}

pub(super) fn aggregate_group_concat(
    ctx: &AggregateEvalContext<'_>,
    row_indexes: &[usize],
    args: &[Expr],
    distinct: bool,
    order_by: &[crate::sql::ast::OrderBy],
    function_name: &str,
) -> Result<Value> {
    let function_name = function_name.to_ascii_uppercase();
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::sql(format!(
            "{function_name} expects 1 or 2 arguments"
        )));
    }
    let ordered_indexes = sort_aggregate_row_indexes(
        ctx.runtime,
        ctx.dataset,
        row_indexes,
        order_by,
        ctx.params,
        ctx.ctes,
    )?;
    let mut parts = Vec::new();
    let mut seen_values = Vec::<Value>::new();
    let mut separator = ",".to_string();
    for row_index in ordered_indexes {
        let row = ctx
            .dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
        let value = ctx.eval_row(row, &args[0])?;
        if matches!(value, Value::Null) {
            continue;
        }
        if distinct {
            if seen_values
                .iter()
                .map(|seen| values_equal(seen, &value))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .any(std::convert::identity)
            {
                continue;
            }
            seen_values.push(value.clone());
        }
        if let Some(separator_expr) = args.get(1) {
            separator = match ctx.eval_row(row, separator_expr)? {
                Value::Text(value) => value,
                Value::Null => String::new(),
                other => {
                    return Err(DbError::sql(format!(
                        "{function_name} separator must be text, got {other:?}"
                    )))
                }
            };
        }
        parts.push(value_to_text(&value)?);
    }
    if parts.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(Value::Text(parts.join(&separator)))
    }
}

pub(super) fn eval_json_array_length(values: Vec<Value>) -> Result<Value> {
    if values.is_empty() || values.len() > 2 {
        return Err(DbError::sql("json_array_length expects 1 or 2 arguments"));
    }
    let target = json_target_value(&values)?;
    match target {
        Some(JsonValue::Array(array)) => Ok(Value::Int64(array.len() as i64)),
        Some(_) => Ok(Value::Int64(0)),
        None => Ok(Value::Null),
    }
}

pub(super) fn eval_json_extract(values: Vec<Value>) -> Result<Value> {
    if values.len() != 2 {
        return Err(DbError::sql("json_extract expects 2 arguments"));
    }
    let Some(target) = json_target_value(&values)? else {
        return Ok(Value::Null);
    };
    json_value_to_value(target)
}

pub(super) fn json_target_value(values: &[Value]) -> Result<Option<JsonValue>> {
    let json = match values.first() {
        Some(Value::Null) | None => return Ok(None),
        Some(Value::Text(value)) => parse_json(value)?,
        Some(other) => {
            return Err(DbError::sql(format!(
                "JSON functions expect text input, got {other:?}"
            )))
        }
    };
    if let Some(path_value) = values.get(1) {
        let path = match path_value {
            Value::Null => return Ok(None),
            Value::Text(path) => parse_json_path(path)?,
            other => {
                return Err(DbError::sql(format!(
                    "JSON path must be text, got {other:?}"
                )))
            }
        };
        Ok(json.lookup(&path).cloned())
    } else {
        Ok(Some(json))
    }
}

pub(super) fn json_value_to_value(value: JsonValue) -> Result<Value> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(value) => Ok(Value::Bool(value)),
        JsonValue::String(value) => Ok(Value::Text(value)),
        JsonValue::Number(value) => {
            let (scaled, scale) = parse_decimal_text(&value)?;
            if scale == 0 {
                Ok(Value::Int64(scaled))
            } else {
                Ok(Value::Float64(
                    (scaled as f64) / 10_f64.powi(i32::from(scale)),
                ))
            }
        }
        JsonValue::Object(_) | JsonValue::Array(_) => Ok(Value::Text(value.render_json())),
    }
}

pub(super) fn value_to_text(value: &Value) -> Result<String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Int64(value) => Ok(value.to_string()),
        Value::Float64(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(if *value { "true" } else { "false" }.to_string()),
        Value::Text(value) => Ok(value.clone()),
        Value::Blob(_) => Err(DbError::sql("cannot stringify BLOB value")),
        Value::Decimal { scaled, scale } => {
            if *scale == 0 {
                Ok(scaled.to_string())
            } else {
                let negative = *scaled < 0;
                let digits = scaled.unsigned_abs().to_string();
                let scale = usize::from(*scale);
                if digits.len() <= scale {
                    let padded = format!("{digits:0>width$}", width = scale + 1);
                    let split = padded.len() - scale;
                    Ok(format!(
                        "{}{}.{}",
                        if negative { "-" } else { "" },
                        &padded[..split],
                        &padded[split..]
                    ))
                } else {
                    let split = digits.len() - scale;
                    Ok(format!(
                        "{}{}.{}",
                        if negative { "-" } else { "" },
                        &digits[..split],
                        &digits[split..]
                    ))
                }
            }
        }
        Value::Uuid(value) => Ok(format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
            value[8], value[9], value[10], value[11], value[12], value[13], value[14], value[15]
        )),
        Value::TimestampMicros(value) => Ok(value.to_string()),
        Value::Enum {
            enum_type_id,
            label_id,
        } => Ok(format!("{enum_type_id}:{label_id}")),
        Value::IpAddr { family, addr } => format_ip_addr(*family, addr),
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => format_cidr(*family, *prefix_len, network),
        Value::MacAddr { len, bytes } => format_mac_addr(*len, bytes),
        Value::DateDays(days) => Ok(format_date_days(*days)),
        Value::TimeMicros(micros) => format_time_micros(*micros),
        Value::TimestampTzMicros(micros) => Ok(format_timestamp_tz_micros(*micros)),
        Value::Interval {
            months,
            days,
            micros,
        } => Ok(format_interval(*months, *days, *micros)),
        Value::Geometry(_) | Value::Geography(_) => {
            Err(DbError::sql("use ST_AsText to render spatial values"))
        }
    }
}

pub(super) fn cast_value(value: Value, target_type: crate::catalog::ColumnType) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match target_type {
        crate::catalog::ColumnType::Int64 => match value {
            Value::Int64(value) => Ok(Value::Int64(value)),
            Value::Float64(value) => Ok(Value::Int64(value as i64)),
            Value::Decimal { scaled, scale } => Ok(Value::Int64(decimal_to_i64(scaled, scale))),
            Value::Bool(value) => Ok(Value::Int64(if value { 1 } else { 0 })),
            Value::Text(value) => value
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| DbError::sql("invalid INT64 cast")),
            other => Err(DbError::sql(format!("cannot cast {other:?} to INT64"))),
        },
        crate::catalog::ColumnType::Float64 => match value {
            Value::Int64(value) => Ok(Value::Float64(value as f64)),
            Value::Float64(value) => Ok(Value::Float64(value)),
            Value::Decimal { scaled, scale } => Ok(Value::Float64(decimal_to_f64(scaled, scale))),
            Value::Text(value) => value
                .parse::<f64>()
                .map(Value::Float64)
                .map_err(|_| DbError::sql("invalid FLOAT64 cast")),
            other => Err(DbError::sql(format!("cannot cast {other:?} to FLOAT64"))),
        },
        crate::catalog::ColumnType::Text => Ok(Value::Text(match value {
            Value::Text(value) => value,
            Value::Int64(value) => value.to_string(),
            Value::Float64(value) => value.to_string(),
            Value::Bool(value) => value.to_string(),
            Value::Enum {
                enum_type_id,
                label_id,
            } => format!("{enum_type_id}:{label_id}"),
            Value::IpAddr { family, addr } => format_ip_addr(family, &addr)?,
            Value::Cidr {
                family,
                prefix_len,
                network,
            } => format_cidr(family, prefix_len, &network)?,
            Value::MacAddr { len, bytes } => format_mac_addr(len, &bytes)?,
            Value::DateDays(days) => format_date_days(days),
            Value::TimeMicros(micros) => format_time_micros(micros)?,
            Value::TimestampTzMicros(micros) => format_timestamp_tz_micros(micros),
            Value::Interval {
                months,
                days,
                micros,
            } => format_interval(months, days, micros),
            other => return Err(DbError::sql(format!("cannot cast {other:?} to TEXT"))),
        })),
        crate::catalog::ColumnType::Bool => match value {
            Value::Bool(value) => Ok(Value::Bool(value)),
            Value::Text(value) => match value.to_ascii_lowercase().as_str() {
                "true" | "t" | "1" => Ok(Value::Bool(true)),
                "false" | "f" | "0" => Ok(Value::Bool(false)),
                _ => Err(DbError::sql("invalid BOOL cast")),
            },
            other => Err(DbError::sql(format!("cannot cast {other:?} to BOOL"))),
        },
        crate::catalog::ColumnType::Blob => match value {
            Value::Blob(value) => Ok(Value::Blob(value)),
            Value::Uuid(value) => Ok(Value::Blob(value.to_vec())),
            other => Err(DbError::sql(format!("cannot cast {other:?} to BLOB"))),
        },
        crate::catalog::ColumnType::Geometry => match value {
            Value::Geometry(value) => Ok(Value::Geometry(normalize_geometry_bytes(&value)?)),
            other => Err(DbError::sql(format!(
                "cannot cast {other:?} to GEOMETRY; use ST_GeomFromWKB or ST_GeomFromText"
            ))),
        },
        crate::catalog::ColumnType::Geography => match value {
            Value::Geography(value) => Ok(Value::Geography(normalize_geography_bytes(&value)?)),
            other => Err(DbError::sql(format!(
                "cannot cast {other:?} to GEOGRAPHY; use ST_GeogFromWKB or ST_GeogFromText"
            ))),
        },
        crate::catalog::ColumnType::Decimal => match value {
            Value::Decimal { scaled, scale } => Ok(Value::Decimal { scaled, scale }),
            Value::Int64(value) => Ok(Value::Decimal {
                scaled: value,
                scale: 0,
            }),
            Value::Float64(value) => {
                let (scaled, scale) = parse_decimal_text(&value.to_string())?;
                Ok(Value::Decimal { scaled, scale })
            }
            Value::Text(value) => {
                let (scaled, scale) = parse_decimal_text(&value)?;
                Ok(Value::Decimal { scaled, scale })
            }
            other => Err(DbError::sql(format!("cannot cast {other:?} to DECIMAL"))),
        },
        crate::catalog::ColumnType::Uuid => match value {
            Value::Uuid(value) => Ok(Value::Uuid(value)),
            Value::Blob(value) if value.len() == 16 => {
                let mut uuid = [0u8; 16];
                uuid.copy_from_slice(&value);
                Ok(Value::Uuid(uuid))
            }
            other => Err(DbError::sql(format!("cannot cast {other:?} to UUID"))),
        },
        crate::catalog::ColumnType::Timestamp => match value {
            Value::TimestampMicros(value) => Ok(Value::TimestampMicros(value)),
            Value::TimestampTzMicros(value) => Ok(Value::TimestampMicros(value)),
            Value::Int64(value) => Ok(Value::TimestampMicros(value)),
            Value::Text(value) => Ok(Value::TimestampMicros(
                parse_datetime_text("TIMESTAMP cast", &value)?.timestamp_micros(),
            )),
            other => Err(DbError::sql(format!("cannot cast {other:?} to TIMESTAMP"))),
        },
        crate::catalog::ColumnType::Enum => match value {
            Value::Enum {
                enum_type_id,
                label_id,
            } => Ok(Value::Enum {
                enum_type_id,
                label_id,
            }),
            other => Err(DbError::sql(format!(
                "cannot cast {other:?} to ENUM without column enum metadata"
            ))),
        },
        crate::catalog::ColumnType::IpAddr => match value {
            Value::IpAddr { family, addr } => Ok(Value::IpAddr { family, addr }),
            Value::Text(value) => {
                let (family, addr) = parse_ip_addr(&value)?;
                Ok(Value::IpAddr { family, addr })
            }
            other => Err(DbError::sql(format!("cannot cast {other:?} to IPADDR"))),
        },
        crate::catalog::ColumnType::Cidr => match value {
            Value::Cidr {
                family,
                prefix_len,
                network,
            } => Ok(Value::Cidr {
                family,
                prefix_len,
                network,
            }),
            Value::Text(value) => {
                let (family, prefix_len, network) = parse_cidr(&value)?;
                Ok(Value::Cidr {
                    family,
                    prefix_len,
                    network,
                })
            }
            other => Err(DbError::sql(format!("cannot cast {other:?} to CIDR"))),
        },
        crate::catalog::ColumnType::MacAddr => match value {
            Value::MacAddr { len, bytes } => Ok(Value::MacAddr { len, bytes }),
            Value::Text(value) => {
                let (len, bytes) = parse_mac_addr(&value)?;
                Ok(Value::MacAddr { len, bytes })
            }
            other => Err(DbError::sql(format!("cannot cast {other:?} to MACADDR"))),
        },
        crate::catalog::ColumnType::Date => match value {
            Value::DateDays(days) => Ok(Value::DateDays(days)),
            Value::Int64(value) => {
                let days = i32::try_from(value).map_err(|_| DbError::sql("invalid DATE cast"))?;
                Ok(Value::DateDays(days))
            }
            Value::Text(value) => Ok(Value::DateDays(parse_date_days(&value)?)),
            other => Err(DbError::sql(format!("cannot cast {other:?} to DATE"))),
        },
        crate::catalog::ColumnType::Time => match value {
            Value::TimeMicros(micros) => Ok(Value::TimeMicros(micros)),
            Value::Int64(value) => Ok(Value::TimeMicros(value)),
            Value::Text(value) => Ok(Value::TimeMicros(parse_time_micros(&value)?)),
            other => Err(DbError::sql(format!("cannot cast {other:?} to TIME"))),
        },
        crate::catalog::ColumnType::TimestampTz => match value {
            Value::TimestampTzMicros(value) => Ok(Value::TimestampTzMicros(value)),
            Value::TimestampMicros(value) | Value::Int64(value) => {
                Ok(Value::TimestampTzMicros(value))
            }
            Value::Text(value) => Ok(Value::TimestampTzMicros(parse_timestamp_tz_micros(&value)?)),
            other => Err(DbError::sql(format!(
                "cannot cast {other:?} to TIMESTAMPTZ"
            ))),
        },
        crate::catalog::ColumnType::Interval => match value {
            Value::Interval {
                months,
                days,
                micros,
            } => Ok(Value::Interval {
                months,
                days,
                micros,
            }),
            Value::Int64(value) => Ok(Value::Interval {
                months: 0,
                days: 0,
                micros: value,
            }),
            Value::Text(value) => {
                let (months, days, micros) = parse_interval(&value)?;
                Ok(Value::Interval {
                    months,
                    days,
                    micros,
                })
            }
            other => Err(DbError::sql(format!("cannot cast {other:?} to INTERVAL"))),
        },
    }
}

pub(super) fn decimal_to_f64(scaled: i64, scale: u8) -> f64 {
    (scaled as f64) / 10_f64.powi(i32::from(scale))
}

pub(super) fn decimal_to_i64(scaled: i64, scale: u8) -> i64 {
    if scale == 0 {
        return scaled;
    }
    let Some(divisor) = 10_i64.checked_pow(u32::from(scale)) else {
        return 0;
    };
    scaled / divisor
}

pub(super) fn infer_column_type_for_ctas(
    rows: &[Vec<Value>],
    column_index: usize,
) -> crate::catalog::ColumnType {
    for row in rows {
        let Some(value) = row.get(column_index) else {
            continue;
        };
        match value {
            Value::Null => continue,
            Value::Int64(_) => return crate::catalog::ColumnType::Int64,
            Value::Float64(_) => return crate::catalog::ColumnType::Float64,
            Value::Text(_) => return crate::catalog::ColumnType::Text,
            Value::Bool(_) => return crate::catalog::ColumnType::Bool,
            Value::Blob(_) => return crate::catalog::ColumnType::Blob,
            Value::Decimal { .. } => return crate::catalog::ColumnType::Decimal,
            Value::Uuid(_) => return crate::catalog::ColumnType::Uuid,
            Value::TimestampMicros(_) => return crate::catalog::ColumnType::Timestamp,
            Value::Enum { .. } => return crate::catalog::ColumnType::Enum,
            Value::IpAddr { .. } => return crate::catalog::ColumnType::IpAddr,
            Value::Cidr { .. } => return crate::catalog::ColumnType::Cidr,
            Value::MacAddr { .. } => return crate::catalog::ColumnType::MacAddr,
            Value::DateDays(_) => return crate::catalog::ColumnType::Date,
            Value::TimeMicros(_) => return crate::catalog::ColumnType::Time,
            Value::TimestampTzMicros(_) => return crate::catalog::ColumnType::TimestampTz,
            Value::Interval { .. } => return crate::catalog::ColumnType::Interval,
            Value::Geometry(_) => return crate::catalog::ColumnType::Geometry,
            Value::Geography(_) => return crate::catalog::ColumnType::Geography,
        }
    }
    crate::catalog::ColumnType::Text
}

pub(super) fn truthy(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Null => None,
        _ => None,
    }
}

pub(super) fn eval_binary(op: &BinaryOp, left: Value, right: Value) -> Result<Value> {
    match op {
        BinaryOp::And => Ok(match (truthy(&left), truthy(&right)) {
            (Some(false), _) | (_, Some(false)) => Value::Bool(false),
            (Some(true), Some(true)) => Value::Bool(true),
            _ => Value::Null,
        }),
        BinaryOp::Or => Ok(match (truthy(&left), truthy(&right)) {
            (Some(true), _) | (_, Some(true)) => Value::Bool(true),
            (Some(false), Some(false)) => Value::Bool(false),
            _ => Value::Null,
        }),
        BinaryOp::Concat => match (left, right) {
            (Value::Text(left), Value::Text(right)) => Ok(Value::Text(left + &right)),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            other => Err(DbError::sql(format!("cannot concatenate {other:?}"))),
        },
        BinaryOp::JsonExtract => eval_json_binary_operator(&left, &right, false),
        BinaryOp::JsonExtractText => eval_json_binary_operator(&left, &right, true),
        BinaryOp::Distance => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                Ok(Value::Null)
            } else {
                Ok(Value::Float64(spatial_distance_values(&left, &right)?))
            }
        }
        BinaryOp::RegexMatch => eval_regex(left, right, false, false),
        BinaryOp::RegexMatchCaseInsensitive => eval_regex(left, right, true, false),
        BinaryOp::RegexNotMatch => eval_regex(left, right, false, true),
        BinaryOp::RegexNotMatchCaseInsensitive => eval_regex(left, right, true, true),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            arithmetic(op, left, right)
        }
        BinaryOp::IsDistinctFrom => Ok(Value::Bool(
            compare_values(&left, &right)? != std::cmp::Ordering::Equal,
        )),
        BinaryOp::IsNotDistinctFrom => Ok(Value::Bool(
            compare_values(&left, &right)? == std::cmp::Ordering::Equal,
        )),
        _ => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                return Ok(Value::Null);
            }
            let ordering = compare_values(&left, &right)?;
            Ok(Value::Bool(match op {
                BinaryOp::Eq => ordering == std::cmp::Ordering::Equal,
                BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
                BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
                BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
                BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
                BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
                _ => {
                    return Err(DbError::internal(
                        "internal: invalid comparison operator state",
                    ))
                }
            }))
        }
    }
}

pub(super) fn compare_values_with_runtime_collation(
    runtime: Option<&EngineRuntime>,
    left: &Value,
    right: &Value,
    collation: Option<Collation>,
) -> Result<std::cmp::Ordering> {
    match (&collation, left, right) {
        (Some(Collation::Extension(name)), Value::Text(left), Value::Text(right)) => {
            let Some(runtime) = runtime else {
                return Err(DbError::sql(format!(
                    "extension collation {name} requires runtime-aware comparison"
                )));
            };
            crate::extensions::compare_with_collation_from_runtime(runtime, name, left, right)?
                .ok_or_else(|| DbError::sql(format!("unsupported collation {name}")))
        }
        _ => compare_values_with_collation(left, right, collation),
    }
}

pub(super) fn eval_binary_with_collation(
    runtime: Option<&EngineRuntime>,
    op: &BinaryOp,
    left: Value,
    right: Value,
    collation: Option<Collation>,
) -> Result<Value> {
    if collation.is_none() {
        return eval_binary(op, left, right);
    }
    match op {
        BinaryOp::IsDistinctFrom => Ok(Value::Bool(
            compare_values_with_runtime_collation(runtime, &left, &right, collation.clone())?
                != std::cmp::Ordering::Equal,
        )),
        BinaryOp::IsNotDistinctFrom => Ok(Value::Bool(
            compare_values_with_runtime_collation(runtime, &left, &right, collation.clone())?
                == std::cmp::Ordering::Equal,
        )),
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                return Ok(Value::Null);
            }
            let ordering =
                compare_values_with_runtime_collation(runtime, &left, &right, collation)?;
            Ok(Value::Bool(match op {
                BinaryOp::Eq => ordering == std::cmp::Ordering::Equal,
                BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
                BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
                BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
                BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
                BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
                _ => {
                    return Err(DbError::internal(
                        "internal: invalid runtime collation comparison operator",
                    ));
                }
            }))
        }
        _ => eval_binary(op, left, right),
    }
}

pub(super) fn eval_regex(
    left: Value,
    right: Value,
    case_insensitive: bool,
    negated: bool,
) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Text(left), Value::Text(pattern)) => {
            let mut builder = regex::RegexBuilder::new(&pattern);
            builder.case_insensitive(case_insensitive);
            let regex = builder
                .build()
                .map_err(|error| DbError::sql(format!("invalid regular expression: {error}")))?;
            let matched = regex.is_match(&left);
            Ok(Value::Bool(if negated { !matched } else { matched }))
        }
        other => Err(DbError::sql(format!(
            "regex operators expect text values, got {other:?}"
        ))),
    }
}

pub(super) fn arithmetic(op: &BinaryOp, left: Value, right: Value) -> Result<Value> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some(result) = timestamp_interval_arithmetic(op, &left, &right)? {
        return Ok(result);
    }
    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => {
            if matches!(op, BinaryOp::Div | BinaryOp::Mod) && right == 0 {
                return Ok(Value::Null);
            }
            Ok(match op {
                BinaryOp::Add => Value::Int64(left + right),
                BinaryOp::Sub => Value::Int64(left - right),
                BinaryOp::Mul => Value::Int64(left * right),
                BinaryOp::Div => Value::Int64(left / right),
                BinaryOp::Mod => Value::Int64(left % right),
                _ => {
                    return Err(DbError::internal(
                        "internal: invalid arithmetic operator for integer operands",
                    ));
                }
            })
        }
        (Value::Int64(left), Value::Float64(right)) => {
            arithmetic(op, Value::Float64(left as f64), Value::Float64(right))
        }
        (Value::Float64(left), Value::Int64(right)) => {
            arithmetic(op, Value::Float64(left), Value::Float64(right as f64))
        }
        (Value::Float64(left), Value::Float64(right)) => {
            if matches!(op, BinaryOp::Div | BinaryOp::Mod) && right == 0.0 {
                return Ok(Value::Null);
            }
            Ok(match op {
                BinaryOp::Add => Value::Float64(left + right),
                BinaryOp::Sub => Value::Float64(left - right),
                BinaryOp::Mul => Value::Float64(left * right),
                BinaryOp::Div => Value::Float64(left / right),
                BinaryOp::Mod => Value::Float64(left % right),
                _ => {
                    return Err(DbError::internal(
                        "internal: invalid arithmetic operator for floating-point operands",
                    ));
                }
            })
        }
        other => Err(DbError::sql(format!(
            "arithmetic is not defined for {other:?}"
        ))),
    }
}

pub(super) fn timestamp_interval_arithmetic(
    op: &BinaryOp,
    left: &Value,
    right: &Value,
) -> Result<Option<Value>> {
    match (op, left, right) {
        (BinaryOp::Add, Value::TimestampMicros(timestamp), Value::Int64(interval_micros)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                *timestamp,
                *interval_micros,
                true,
            )?)))
        }
        (BinaryOp::Sub, Value::TimestampMicros(timestamp), Value::Int64(interval_micros)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                *timestamp,
                *interval_micros,
                false,
            )?)))
        }
        (BinaryOp::Add, Value::Int64(interval_micros), Value::TimestampMicros(timestamp)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                *timestamp,
                *interval_micros,
                true,
            )?)))
        }
        (BinaryOp::Add, Value::Text(timestamp), Value::Int64(interval_micros)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                *interval_micros,
                true,
            )?)))
        }
        (BinaryOp::Sub, Value::Text(timestamp), Value::Int64(interval_micros)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                *interval_micros,
                false,
            )?)))
        }
        (BinaryOp::Add, Value::Int64(interval_micros), Value::Text(timestamp)) => {
            Ok(Some(Value::TimestampMicros(apply_interval_micros(
                parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                *interval_micros,
                true,
            )?)))
        }
        (
            BinaryOp::Add,
            Value::TimestampMicros(timestamp),
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Ok(Some(Value::TimestampMicros(
            apply_interval_to_timestamp_micros(*timestamp, *months, *days, *micros, true)?,
        ))),
        (
            BinaryOp::Sub,
            Value::TimestampMicros(timestamp),
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Ok(Some(Value::TimestampMicros(
            apply_interval_to_timestamp_micros(*timestamp, *months, *days, *micros, false)?,
        ))),
        (
            BinaryOp::Add,
            Value::Interval {
                months,
                days,
                micros,
            },
            Value::TimestampMicros(timestamp),
        ) => Ok(Some(Value::TimestampMicros(
            apply_interval_to_timestamp_micros(*timestamp, *months, *days, *micros, true)?,
        ))),
        (
            BinaryOp::Add,
            Value::TimestampTzMicros(timestamp),
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Ok(Some(Value::TimestampTzMicros(
            apply_interval_to_timestamp_micros(*timestamp, *months, *days, *micros, true)?,
        ))),
        (
            BinaryOp::Sub,
            Value::TimestampTzMicros(timestamp),
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Ok(Some(Value::TimestampTzMicros(
            apply_interval_to_timestamp_micros(*timestamp, *months, *days, *micros, false)?,
        ))),
        (
            BinaryOp::Add,
            Value::Interval {
                months,
                days,
                micros,
            },
            Value::TimestampTzMicros(timestamp),
        ) => Ok(Some(Value::TimestampTzMicros(
            apply_interval_to_timestamp_micros(*timestamp, *months, *days, *micros, true)?,
        ))),
        (
            BinaryOp::Add,
            Value::DateDays(days_since_epoch),
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Ok(Some(Value::TimestampMicros(
            apply_interval_to_timestamp_micros(
                date_days_to_micros(*days_since_epoch)?,
                *months,
                *days,
                *micros,
                true,
            )?,
        ))),
        (
            BinaryOp::Sub,
            Value::DateDays(days_since_epoch),
            Value::Interval {
                months,
                days,
                micros,
            },
        ) => Ok(Some(Value::TimestampMicros(
            apply_interval_to_timestamp_micros(
                date_days_to_micros(*days_since_epoch)?,
                *months,
                *days,
                *micros,
                false,
            )?,
        ))),
        (
            BinaryOp::Add,
            Value::Interval {
                months,
                days,
                micros,
            },
            Value::DateDays(days_since_epoch),
        ) => Ok(Some(Value::TimestampMicros(
            apply_interval_to_timestamp_micros(
                date_days_to_micros(*days_since_epoch)?,
                *months,
                *days,
                *micros,
                true,
            )?,
        ))),
        (BinaryOp::Add, Value::Text(timestamp), Value::Text(interval_text)) => {
            let (months, days, micros) = parse_interval(interval_text)?;
            Ok(Some(Value::TimestampMicros(
                apply_interval_to_timestamp_micros(
                    parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                    months,
                    days,
                    micros,
                    true,
                )?,
            )))
        }
        (BinaryOp::Sub, Value::Text(timestamp), Value::Text(interval_text)) => {
            let (months, days, micros) = parse_interval(interval_text)?;
            Ok(Some(Value::TimestampMicros(
                apply_interval_to_timestamp_micros(
                    parse_datetime_text("interval arithmetic", timestamp)?.timestamp_micros(),
                    months,
                    days,
                    micros,
                    false,
                )?,
            )))
        }
        (BinaryOp::Add, Value::TimestampMicros(timestamp), Value::Text(interval_text)) => {
            let (months, days, micros) = parse_interval(interval_text)?;
            Ok(Some(Value::TimestampMicros(
                apply_interval_to_timestamp_micros(*timestamp, months, days, micros, true)?,
            )))
        }
        (BinaryOp::Sub, Value::TimestampMicros(timestamp), Value::Text(interval_text)) => {
            let (months, days, micros) = parse_interval(interval_text)?;
            Ok(Some(Value::TimestampMicros(
                apply_interval_to_timestamp_micros(*timestamp, months, days, micros, false)?,
            )))
        }
        (BinaryOp::Add, Value::Text(interval_text), Value::TimestampMicros(timestamp)) => {
            let (months, days, micros) = parse_interval(interval_text)?;
            Ok(Some(Value::TimestampMicros(
                apply_interval_to_timestamp_micros(*timestamp, months, days, micros, true)?,
            )))
        }
        _ => Ok(None),
    }
}

pub(super) fn apply_interval_micros(
    timestamp_micros: i64,
    interval_micros: i64,
    add: bool,
) -> Result<i64> {
    if add {
        timestamp_micros
            .checked_add(interval_micros)
            .ok_or_else(|| DbError::sql("timestamp addition overflowed"))
    } else {
        timestamp_micros
            .checked_sub(interval_micros)
            .ok_or_else(|| DbError::sql("timestamp subtraction overflowed"))
    }
}

pub(super) fn apply_interval_to_timestamp_micros(
    timestamp_micros: i64,
    months: i32,
    days: i32,
    micros: i64,
    add: bool,
) -> Result<i64> {
    let mut datetime = datetime_from_epoch_micros("interval arithmetic", timestamp_micros)?;
    let month_delta = if add {
        i64::from(months)
    } else {
        -i64::from(months)
    };
    if month_delta != 0 {
        datetime = shift_datetime_by_months(datetime, month_delta)?;
    }
    let day_delta = if add {
        i64::from(days)
    } else {
        -i64::from(days)
    };
    if day_delta != 0 {
        datetime = shift_datetime_by_duration(
            "interval arithmetic",
            datetime,
            ChronoDuration::days(day_delta),
        )?;
    }
    let micros_delta = if add {
        micros
    } else {
        micros
            .checked_neg()
            .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?
    };
    if micros_delta != 0 {
        datetime = shift_datetime_by_duration(
            "interval arithmetic",
            datetime,
            ChronoDuration::microseconds(micros_delta),
        )?;
    }
    Ok(datetime.timestamp_micros())
}

pub(super) fn date_days_to_micros(days: i32) -> Result<i64> {
    i64::from(days)
        .checked_mul(EXEC_MICROS_PER_DAY)
        .ok_or_else(|| DbError::sql("DATE value is out of range"))
}

pub(super) fn expr_collation(expr: &Expr) -> Option<Collation> {
    match expr {
        Expr::Collate { collation, .. } => Some(collation.clone()),
        _ => None,
    }
}

pub(super) fn select_item_contains_collation(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => expr_contains_collation(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }
}

pub(super) fn select_item_contains_fulltext_function(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => expr_contains_fulltext_function(expr),
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    }
}

pub(super) fn expr_contains_fulltext_function(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args } => {
            name.eq_ignore_ascii_case("fulltext_match")
                || name.eq_ignore_ascii_case("bm25")
                || args.iter().any(expr_contains_fulltext_function)
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            expr_contains_fulltext_function(expr)
        }
        Expr::Collate { expr, .. } => expr_contains_fulltext_function(expr),
        Expr::Binary { left, right, .. } => {
            expr_contains_fulltext_function(left) || expr_contains_fulltext_function(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_fulltext_function(expr)
                || expr_contains_fulltext_function(low)
                || expr_contains_fulltext_function(high)
        }
        Expr::InList { expr, items, .. } => {
            expr_contains_fulltext_function(expr)
                || items.iter().any(expr_contains_fulltext_function)
        }
        Expr::InSubquery { expr, .. } | Expr::CompareSubquery { expr, .. } => {
            expr_contains_fulltext_function(expr)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_fulltext_function(expr)
                || expr_contains_fulltext_function(pattern)
                || escape
                    .as_deref()
                    .is_some_and(expr_contains_fulltext_function)
        }
        Expr::Aggregate { args, order_by, .. } => {
            args.iter().any(expr_contains_fulltext_function)
                || order_by
                    .iter()
                    .any(|order| expr_contains_fulltext_function(&order.expr))
        }
        Expr::RowNumber {
            partition_by,
            order_by,
            ..
        } => {
            partition_by.iter().any(expr_contains_fulltext_function)
                || order_by
                    .iter()
                    .any(|order| expr_contains_fulltext_function(&order.expr))
        }
        Expr::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(expr_contains_fulltext_function)
                || partition_by.iter().any(expr_contains_fulltext_function)
                || order_by
                    .iter()
                    .any(|order| expr_contains_fulltext_function(&order.expr))
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(expr_contains_fulltext_function)
                || branches.iter().any(|(when, then)| {
                    expr_contains_fulltext_function(when) || expr_contains_fulltext_function(then)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(expr_contains_fulltext_function)
        }
        Expr::Row(exprs) => exprs.iter().any(expr_contains_fulltext_function),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::Parameter(_)
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => false,
    }
}

pub(super) fn expr_contains_collation(expr: &Expr) -> bool {
    match expr {
        Expr::Collate { .. } => true,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            expr_contains_collation(expr)
        }
        Expr::Binary { left, right, .. } => {
            expr_contains_collation(left) || expr_contains_collation(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_collation(expr)
                || expr_contains_collation(low)
                || expr_contains_collation(high)
        }
        Expr::InList { expr, items, .. } => {
            expr_contains_collation(expr) || items.iter().any(expr_contains_collation)
        }
        Expr::InSubquery { expr, .. } | Expr::CompareSubquery { expr, .. } => {
            expr_contains_collation(expr)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_collation(expr)
                || expr_contains_collation(pattern)
                || escape.as_deref().is_some_and(expr_contains_collation)
        }
        Expr::Function { args, .. } => args.iter().any(expr_contains_collation),
        Expr::Aggregate { args, order_by, .. } => {
            args.iter().any(expr_contains_collation)
                || order_by.iter().any(order_by_contains_collation)
        }
        Expr::RowNumber {
            partition_by,
            order_by,
            ..
        } => {
            partition_by.iter().any(expr_contains_collation)
                || order_by.iter().any(order_by_contains_collation)
        }
        Expr::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(expr_contains_collation)
                || partition_by.iter().any(expr_contains_collation)
                || order_by.iter().any(order_by_contains_collation)
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_contains_collation)
                || branches.iter().any(|(when, then)| {
                    expr_contains_collation(when) || expr_contains_collation(then)
                })
                || else_expr.as_deref().is_some_and(expr_contains_collation)
        }
        Expr::Row(exprs) => exprs.iter().any(expr_contains_collation),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::Parameter(_)
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => false,
    }
}

pub(super) fn order_by_contains_collation(order_by: &OrderBy) -> bool {
    order_by.collation.is_some() || expr_contains_collation(&order_by.expr)
}

pub(super) fn compare_values_with_collation(
    left: &Value,
    right: &Value,
    collation: Option<Collation>,
) -> Result<std::cmp::Ordering> {
    match (collation, left, right) {
        (Some(Collation::Binary), Value::Text(left), Value::Text(right)) => Ok(left.cmp(right)),
        (Some(Collation::NoCase), Value::Text(left), Value::Text(right)) => {
            Ok(compare_ascii_nocase(left, right))
        }
        (Some(Collation::RTrim), Value::Text(left), Value::Text(right)) => {
            Ok(left.trim_end_matches(' ').cmp(right.trim_end_matches(' ')))
        }
        (Some(Collation::Extension(name)), Value::Text(_), Value::Text(_)) => {
            Err(DbError::sql(format!(
                "extension collation {name} requires runtime-aware comparison and is not supported in this execution path"
            )))
        }
        _ => compare_values(left, right),
    }
}

pub(super) fn compare_ascii_nocase(left: &str, right: &str) -> std::cmp::Ordering {
    left.bytes()
        .map(|byte| byte.to_ascii_lowercase())
        .cmp(right.bytes().map(|byte| byte.to_ascii_lowercase()))
}

pub(super) fn compare_values(left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if let Some(ordering) = compare_numeric_text_values(left, right) {
        return Ok(ordering);
    }
    match (left, right) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => Ok(Ordering::Less),
        (_, Value::Null) => Ok(Ordering::Greater),
        (Value::Int64(left), Value::Int64(right)) => Ok(left.cmp(right)),
        (Value::Float64(left), Value::Float64(right)) => Ok(left.total_cmp(right)),
        (Value::Int64(left), Value::Float64(right)) => Ok((*left as f64).total_cmp(right)),
        (Value::Float64(left), Value::Int64(right)) => Ok(left.total_cmp(&(*right as f64))),
        (
            Value::Decimal {
                scaled: left_scaled,
                scale: left_scale,
            },
            Value::Decimal {
                scaled: right_scaled,
                scale: right_scale,
            },
        ) => Ok(compare_decimal(
            *left_scaled,
            *left_scale,
            *right_scaled,
            *right_scale,
        )),
        (
            Value::Decimal {
                scaled: left_scaled,
                scale: left_scale,
            },
            Value::Float64(right),
        ) => {
            let left_f64 = decimal_to_f64(*left_scaled, *left_scale);
            Ok(left_f64.total_cmp(right))
        }
        (
            Value::Float64(left),
            Value::Decimal {
                scaled: right_scaled,
                scale: right_scale,
            },
        ) => {
            let right_f64 = decimal_to_f64(*right_scaled, *right_scale);
            Ok(left.total_cmp(&right_f64))
        }
        (
            Value::Decimal {
                scaled: left_scaled,
                scale: left_scale,
            },
            Value::Int64(right),
        ) => {
            let left_f64 = decimal_to_f64(*left_scaled, *left_scale);
            Ok(left_f64.total_cmp(&(*right as f64)))
        }
        (
            Value::Int64(left),
            Value::Decimal {
                scaled: right_scaled,
                scale: right_scale,
            },
        ) => {
            let right_f64 = decimal_to_f64(*right_scaled, *right_scale);
            Ok((*left as f64).total_cmp(&right_f64))
        }
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        (Value::Text(left), Value::Text(right)) => Ok(left.cmp(right)),
        (Value::Blob(left), Value::Blob(right)) => Ok(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Ok(left.cmp(right)),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => Ok(left.cmp(right)),
        (Value::TimestampTzMicros(left), Value::TimestampTzMicros(right)) => Ok(left.cmp(right)),
        (Value::DateDays(left), Value::DateDays(right)) => Ok(left.cmp(right)),
        (Value::TimeMicros(left), Value::TimeMicros(right)) => Ok(left.cmp(right)),
        (
            Value::Enum {
                enum_type_id: left_type,
                label_id: left_label,
            },
            Value::Enum {
                enum_type_id: right_type,
                label_id: right_label,
            },
        ) => Ok(left_type.cmp(right_type).then_with(|| left_label.cmp(right_label))),
        (
            Value::IpAddr {
                family: left_family,
                addr: left_addr,
            },
            Value::IpAddr {
                family: right_family,
                addr: right_addr,
            },
        ) => compare_ip_addr(*left_family, left_addr, *right_family, right_addr),
        (
            Value::Cidr {
                family: left_family,
                prefix_len: left_prefix,
                network: left_network,
            },
            Value::Cidr {
                family: right_family,
                prefix_len: right_prefix,
                network: right_network,
            },
        ) => compare_cidr(
            *left_family,
            *left_prefix,
            left_network,
            *right_family,
            *right_prefix,
            right_network,
        ),
        (
            Value::MacAddr {
                len: left_len,
                bytes: left_bytes,
            },
            Value::MacAddr {
                len: right_len,
                bytes: right_bytes,
            },
        ) => compare_mac_addr(*left_len, left_bytes, *right_len, right_bytes),
        (
            Value::Interval {
                months: left_months,
                days: left_days,
                micros: left_micros,
            },
            Value::Interval {
                months: right_months,
                days: right_days,
                micros: right_micros,
            },
        ) => Ok(compare_interval(
            *left_months,
            *left_days,
            *left_micros,
            *right_months,
            *right_days,
            *right_micros,
        )),
        (Value::Blob(left), Value::Uuid(right)) => Ok(left.as_slice().cmp(right.as_slice())),
        (Value::Uuid(left), Value::Blob(right)) => Ok(left.as_slice().cmp(right.as_slice())),
        (Value::Geometry(_), Value::Geometry(_)) | (Value::Geography(_), Value::Geography(_)) => {
            Err(DbError::sql(
                "spatial values do not have generic comparison semantics; use ST_Equals or spatial predicates",
            ))
        }
        _ => Err(DbError::sql(format!(
            "cannot compare values {left:?} and {right:?}"
        ))),
    }
}

pub(super) fn window_order_keys_equal(left: &[Value], right: &[Value]) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left_value, right_value) in left.iter().zip(right) {
        if compare_values(left_value, right_value)? != std::cmp::Ordering::Equal {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn compute_window_peer_bounds(
    order_keys: &[Vec<Value>],
) -> Result<(Vec<usize>, Vec<usize>)> {
    let mut starts = vec![0_usize; order_keys.len()];
    let mut ends = vec![0_usize; order_keys.len()];
    let mut ordinal = 0_usize;
    while ordinal < order_keys.len() {
        let mut peer_end = ordinal;
        while peer_end + 1 < order_keys.len()
            && window_order_keys_equal(&order_keys[ordinal], &order_keys[peer_end + 1])?
        {
            peer_end += 1;
        }
        for peer in ordinal..=peer_end {
            starts[peer] = ordinal;
            ends[peer] = peer_end;
        }
        ordinal = peer_end + 1;
    }
    Ok((starts, ends))
}

pub(super) fn normalize_window_frame_range(
    start: i64,
    end: i64,
    partition_len: usize,
) -> Result<Option<(usize, usize)>> {
    if partition_len == 0 {
        return Ok(None);
    }
    let len_i64 = i64::try_from(partition_len)
        .map_err(|_| DbError::internal("window partition is too large"))?;
    let start = start.clamp(0, len_i64);
    let end = end.clamp(-1, len_i64 - 1);
    if start > end {
        return Ok(None);
    }
    let start =
        usize::try_from(start).map_err(|_| DbError::internal("window frame start is invalid"))?;
    let end = usize::try_from(end).map_err(|_| DbError::internal("window frame end is invalid"))?;
    Ok(Some((start, end)))
}

pub(super) fn compare_numeric_text_values(
    left: &Value,
    right: &Value,
) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    fn parsed_numeric_text(value: &str) -> Option<f64> {
        let (scaled, scale) = parse_decimal_text(value).ok()?;
        Some((scaled as f64) / 10_f64.powi(i32::from(scale)))
    }

    match (left, right) {
        (Value::Int64(left), Value::Text(right)) => parsed_numeric_text(right)
            .map(|right| (*left as f64).total_cmp(&right))
            .or(Some(Ordering::Less)),
        (Value::Float64(left), Value::Text(right)) => parsed_numeric_text(right)
            .map(|right| left.total_cmp(&right))
            .or(Some(Ordering::Less)),
        (Value::Text(left), Value::Int64(right)) => parsed_numeric_text(left)
            .map(|left| left.total_cmp(&(*right as f64)))
            .or(Some(Ordering::Greater)),
        (Value::Text(left), Value::Float64(right)) => parsed_numeric_text(left)
            .map(|left| left.total_cmp(right))
            .or(Some(Ordering::Greater)),
        _ => None,
    }
}

pub(super) fn like_match(
    input: &str,
    pattern: &str,
    case_insensitive: bool,
    escape: Option<char>,
) -> bool {
    if let Some(result) = like_match_simple_pattern(input, pattern, case_insensitive, escape) {
        return result;
    }
    let input = if case_insensitive {
        input.to_ascii_uppercase()
    } else {
        input.to_string()
    };
    let pattern = if case_insensitive {
        pattern.to_ascii_uppercase()
    } else {
        pattern.to_string()
    };
    let input = input.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    like_match_chars(&input, &pattern, escape)
}

pub(super) fn like_match_simple_pattern(
    input: &str,
    pattern: &str,
    case_insensitive: bool,
    escape: Option<char>,
) -> Option<bool> {
    if escape.is_some() || pattern.as_bytes().contains(&b'_') {
        return None;
    }
    if !pattern.is_empty() && pattern.as_bytes().iter().all(|byte| *byte == b'%') {
        return Some(true);
    }

    let literal = pattern.trim_matches('%');
    if literal.as_bytes().contains(&b'%') {
        return None;
    }

    let starts_with_wildcard = pattern.starts_with('%');
    let ends_with_wildcard = pattern.ends_with('%');
    Some(match (starts_with_wildcard, ends_with_wildcard) {
        (true, true) => like_contains_literal(input, literal, case_insensitive),
        (true, false) => like_ends_with_literal(input, literal, case_insensitive),
        (false, true) => like_starts_with_literal(input, literal, case_insensitive),
        (false, false) => like_equals_literal(input, literal, case_insensitive),
    })
}

pub(super) fn like_equals_literal(input: &str, literal: &str, case_insensitive: bool) -> bool {
    if case_insensitive {
        input.eq_ignore_ascii_case(literal)
    } else {
        input == literal
    }
}

pub(super) fn like_starts_with_literal(input: &str, literal: &str, case_insensitive: bool) -> bool {
    if case_insensitive {
        input
            .as_bytes()
            .get(..literal.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(literal.as_bytes()))
    } else {
        input.starts_with(literal)
    }
}

pub(super) fn like_ends_with_literal(input: &str, literal: &str, case_insensitive: bool) -> bool {
    if case_insensitive {
        input
            .as_bytes()
            .get(input.len().saturating_sub(literal.len())..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(literal.as_bytes()))
    } else {
        input.ends_with(literal)
    }
}

pub(super) fn like_contains_literal(input: &str, literal: &str, case_insensitive: bool) -> bool {
    if !case_insensitive {
        return input.contains(literal);
    }
    if literal.is_empty() {
        return true;
    }
    input
        .as_bytes()
        .windows(literal.len())
        .any(|candidate| candidate.eq_ignore_ascii_case(literal.as_bytes()))
}

pub(super) fn like_match_chars(input: &[char], pattern: &[char], escape: Option<char>) -> bool {
    if pattern.is_empty() {
        return input.is_empty();
    }
    let current = pattern[0];
    if Some(current) == escape {
        return match pattern.get(1) {
            Some(literal) => {
                !input.is_empty()
                    && input[0] == *literal
                    && like_match_chars(&input[1..], &pattern[2..], escape)
            }
            None => {
                !input.is_empty()
                    && input[0] == current
                    && like_match_chars(&input[1..], &pattern[1..], escape)
            }
        };
    }
    match current {
        '%' => (0..=input.len())
            .any(|offset| like_match_chars(&input[offset..], &pattern[1..], escape)),
        '_' => !input.is_empty() && like_match_chars(&input[1..], &pattern[1..], escape),
        literal => {
            !input.is_empty()
                && input[0] == literal
                && like_match_chars(&input[1..], &pattern[1..], escape)
        }
    }
}

pub(super) fn row_identity(row: &[Value]) -> Result<Vec<u8>> {
    Row::new(row.to_vec()).encode()
}

pub(super) fn deduplicate_rows_stable(rows: Vec<Vec<Value>>) -> Result<Vec<Vec<Value>>> {
    let mut seen = BTreeSet::new();
    let mut distinct_rows = Vec::new();
    for row in rows {
        if seen.insert(row_identity(&row)?) {
            distinct_rows.push(row);
        }
    }
    Ok(distinct_rows)
}

pub(super) fn count_row_identities(rows: &[Vec<Value>]) -> Result<HashMap<Vec<u8>, usize>> {
    let mut counts = HashMap::new();
    for row in rows {
        *counts.entry(row_identity(row)?).or_insert(0) += 1;
    }
    Ok(counts)
}

pub(super) fn consume_row_identity_count(
    counts: &mut HashMap<Vec<u8>, usize>,
    identity: &[u8],
) -> bool {
    if let Some(remaining) = counts.get_mut(identity) {
        if *remaining > 0 {
            *remaining -= 1;
            return true;
        }
    }
    false
}

pub(super) fn deduplicate_rows(rows: Vec<Vec<Value>>) -> Result<Vec<Vec<Value>>> {
    let mut seen = BTreeMap::<Vec<u8>, Vec<Value>>::new();
    for row in rows {
        seen.entry(row_identity(&row)?).or_insert(row);
    }
    Ok(seen.into_values().collect())
}
