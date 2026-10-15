//! Thematic extraction (mechanical split; no behavior change).

use super::*;

pub(crate) fn simple_window_column_positions(
    dataset: &Dataset,
    expressions: &[Expr],
) -> Result<Option<Vec<usize>>> {
    let mut positions = Vec::with_capacity(expressions.len());
    for expr in expressions {
        let Expr::Column { table, column } = expr else {
            return Ok(None);
        };
        positions.push(resolve_dataset_column_position(
            dataset,
            table.as_deref(),
            column,
        )?);
    }
    Ok(Some(positions))
}

pub(crate) fn simple_window_order_column_positions(
    dataset: &Dataset,
    order_by: &[OrderBy],
) -> Result<Option<Vec<usize>>> {
    let mut positions = Vec::with_capacity(order_by.len());
    for order in order_by {
        if order.collation.is_some() {
            return Ok(None);
        }
        let Expr::Column { table, column } = &order.expr else {
            return Ok(None);
        };
        positions.push(resolve_dataset_column_position(
            dataset,
            table.as_deref(),
            column,
        )?);
    }
    Ok(Some(positions))
}

pub(crate) fn simple_stored_column_eq_literal_predicate(
    table: &TableSchema,
    row_values: &[Value],
    expr: &Expr,
) -> Result<Option<bool>> {
    if !generated_columns_are_stored(table) {
        return Ok(None);
    }
    let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = expr
    else {
        return Ok(None);
    };
    let Some((table_qualifier, column_name, literal_value)) =
        simple_column_literal_eq(left, right).or_else(|| simple_column_literal_eq(right, left))
    else {
        return Ok(None);
    };
    if table_qualifier.is_some_and(|qualifier| !identifiers_equal(qualifier, &table.name)) {
        return Ok(None);
    }
    let Some(position) = column_position(table, column_name) else {
        return Ok(None);
    };
    let Some(column) = table.columns.get(position) else {
        return Ok(None);
    };
    let Some(row_value) = row_values.get(position) else {
        return Ok(None);
    };
    if matches!(row_value, Value::Null) || matches!(literal_value, Value::Null) {
        return Ok(Some(false));
    }
    let literal_value = constraints::coerce_column_value(column, literal_value.clone())?;
    Ok(Some(
        compare_values(row_value, &literal_value)? == std::cmp::Ordering::Equal,
    ))
}

pub(crate) fn simple_column_literal_eq<'a>(
    left: &'a Expr,
    right: &'a Expr,
) -> Option<(Option<&'a str>, &'a str, &'a Value)> {
    let Expr::Column { table, column } = left else {
        return None;
    };
    let Expr::Literal(value) = right else {
        return None;
    };
    Some((table.as_deref(), column.as_str(), value))
}

pub(crate) fn simple_btree_lookup(filter: &Expr) -> Option<(Option<&str>, &str, &Expr)> {
    match filter {
        Expr::Binary { left, op, right } if *op == BinaryOp::Eq => match (&**left, &**right) {
            (Expr::Column { table, column }, value) if simple_btree_lookup_value_expr(value) => {
                Some((table.as_deref(), column.as_str(), value))
            }
            (value, Expr::Column { table, column }) if simple_btree_lookup_value_expr(value) => {
                Some((table.as_deref(), column.as_str(), value))
            }
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn simple_btree_lookup_value_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => true,
        Expr::Cast { expr, .. } => simple_btree_lookup_value_expr(expr),
        _ => false,
    }
}

pub(crate) fn simple_btree_lookup_terms(filter: &Expr) -> Option<Vec<(Option<&str>, &str, &Expr)>> {
    fn collect<'a>(
        expr: &'a Expr,
        terms: &mut Vec<(Option<&'a str>, &'a str, &'a Expr)>,
    ) -> Option<()> {
        match expr {
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => {
                collect(left, terms)?;
                collect(right, terms)?;
                Some(())
            }
            _ => {
                let term = simple_btree_lookup(expr)?;
                if terms
                    .iter()
                    .any(|(_, column, _)| identifiers_equal(column, term.1))
                {
                    return None;
                }
                terms.push(term);
                Some(())
            }
        }
    }

    let mut terms = Vec::new();
    collect(filter, &mut terms)?;
    (!terms.is_empty()).then_some(terms)
}

pub(crate) fn simple_join_projection_eval_bindings(
    left_table_name: &str,
    left_alias: &Option<String>,
    left_schema: &TableSchema,
    right_table_name: &str,
    right_alias: &Option<String>,
    right_schema: &TableSchema,
) -> Vec<ColumnBinding> {
    let left_binding = left_alias.as_deref().unwrap_or(left_table_name);
    let right_binding = right_alias.as_deref().unwrap_or(right_table_name);
    let mut bindings = Vec::with_capacity(left_schema.columns.len() + right_schema.columns.len());
    bindings.extend(left_schema.columns.iter().map(|column| {
        ColumnBinding::visible_source(
            Some(left_binding.to_string()),
            Some(left_table_name.to_string()),
            column.name.clone(),
        )
        .with_type(column.column_type)
    }));
    bindings.extend(right_schema.columns.iter().map(|column| {
        ColumnBinding::visible_source(
            Some(right_binding.to_string()),
            Some(right_table_name.to_string()),
            column.name.clone(),
        )
        .with_type(column.column_type)
    }));
    bindings
}

pub(crate) fn simple_grouped_projection_bindings(
    group_count: usize,
    aggregate_count: usize,
) -> Vec<ColumnBinding> {
    let mut bindings = Vec::with_capacity(group_count + aggregate_count);
    bindings.extend(
        (0..group_count).map(|index| {
            ColumnBinding::visible(None, format!("__grouped_projection_group_{index}"))
        }),
    );
    bindings
        .extend((0..aggregate_count).map(|index| {
            ColumnBinding::visible(None, format!("__grouped_projection_agg_{index}"))
        }));
    bindings
}

pub(crate) fn simple_grouped_projection_group_index(
    expr: &Expr,
    group_exprs: &[Expr],
    table_binding: TableBindingRef<'_>,
) -> Option<usize> {
    let mut matched = None;
    for (index, group_expr) in group_exprs.iter().enumerate() {
        if !grouped_projection_expr_matches_group_expr(expr, group_expr, table_binding) {
            continue;
        }
        if matched.replace(index).is_some() {
            return None;
        }
    }
    matched
}

pub(crate) fn rewrite_simple_grouped_output_expr(
    expr: &Expr,
    group_exprs: &[Expr],
    table_name: &str,
    binding_name: &str,
    table_binding: TableBindingRef<'_>,
    synthetic_names: &[String],
    aggregate_bindings: &[SimpleGroupedNumericAggregateBinding],
) -> Option<Expr> {
    let group_count = group_exprs.len();
    if let Some(index) = simple_grouped_projection_group_index(expr, group_exprs, table_binding) {
        return Some(Expr::Column {
            table: None,
            column: synthetic_names[index].clone(),
        });
    }

    match expr {
        Expr::Default => None,
        Expr::Literal(_) | Expr::Parameter(_) => Some(expr.clone()),
        Expr::Unary { op, expr } => Some(Expr::Unary {
            op: *op,
            expr: Box::new(rewrite_simple_grouped_output_expr(
                expr,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
        }),
        Expr::Binary { left, op, right } => Some(Expr::Binary {
            left: Box::new(rewrite_simple_grouped_output_expr(
                left,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            op: *op,
            right: Box::new(rewrite_simple_grouped_output_expr(
                right,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
        }),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Some(Expr::Between {
            expr: Box::new(rewrite_simple_grouped_output_expr(
                expr,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            low: Box::new(rewrite_simple_grouped_output_expr(
                low,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            high: Box::new(rewrite_simple_grouped_output_expr(
                high,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            negated: *negated,
        }),
        Expr::InList {
            expr,
            items,
            negated,
        } => Some(Expr::InList {
            expr: Box::new(rewrite_simple_grouped_output_expr(
                expr,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            items: items
                .iter()
                .map(|item| {
                    rewrite_simple_grouped_output_expr(
                        item,
                        group_exprs,
                        table_name,
                        binding_name,
                        table_binding,
                        synthetic_names,
                        aggregate_bindings,
                    )
                })
                .collect::<Option<Vec<_>>>()?,
            negated: *negated,
        }),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Some(Expr::Like {
            expr: Box::new(rewrite_simple_grouped_output_expr(
                expr,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            pattern: Box::new(rewrite_simple_grouped_output_expr(
                pattern,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            escape: match escape.as_ref() {
                Some(expr) => Some(Box::new(rewrite_simple_grouped_output_expr(
                    expr,
                    group_exprs,
                    table_name,
                    binding_name,
                    table_binding,
                    synthetic_names,
                    aggregate_bindings,
                )?)),
                None => None,
            },
            case_insensitive: *case_insensitive,
            negated: *negated,
        }),
        Expr::IsNull { expr, negated } => Some(Expr::IsNull {
            expr: Box::new(rewrite_simple_grouped_output_expr(
                expr,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            negated: *negated,
        }),
        Expr::Collate { expr, collation } => Some(Expr::Collate {
            expr: Box::new(rewrite_simple_grouped_output_expr(
                expr,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            collation: collation.clone(),
        }),
        // This optimization substitutes aggregates with untyped synthetic
        // columns. Keep the original expressions for SQL type inspection.
        Expr::Function { name, .. }
            if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") =>
        {
            None
        }
        Expr::Function { name, args } => Some(Expr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| {
                    rewrite_simple_grouped_output_expr(
                        arg,
                        group_exprs,
                        table_name,
                        binding_name,
                        table_binding,
                        synthetic_names,
                        aggregate_bindings,
                    )
                })
                .collect::<Option<Vec<_>>>()?,
        }),
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => Some(Expr::Case {
            operand: match operand.as_ref() {
                Some(expr) => Some(Box::new(rewrite_simple_grouped_output_expr(
                    expr,
                    group_exprs,
                    table_name,
                    binding_name,
                    table_binding,
                    synthetic_names,
                    aggregate_bindings,
                )?)),
                None => None,
            },
            branches: branches
                .iter()
                .map(|(condition, value)| {
                    Some((
                        rewrite_simple_grouped_output_expr(
                            condition,
                            group_exprs,
                            table_name,
                            binding_name,
                            table_binding,
                            synthetic_names,
                            aggregate_bindings,
                        )?,
                        rewrite_simple_grouped_output_expr(
                            value,
                            group_exprs,
                            table_name,
                            binding_name,
                            table_binding,
                            synthetic_names,
                            aggregate_bindings,
                        )?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?,
            else_expr: match else_expr.as_ref() {
                Some(expr) => Some(Box::new(rewrite_simple_grouped_output_expr(
                    expr,
                    group_exprs,
                    table_name,
                    binding_name,
                    table_binding,
                    synthetic_names,
                    aggregate_bindings,
                )?)),
                None => None,
            },
        }),
        Expr::Row(items) => Some(Expr::Row(
            items
                .iter()
                .map(|item| {
                    rewrite_simple_grouped_output_expr(
                        item,
                        group_exprs,
                        table_name,
                        binding_name,
                        table_binding,
                        synthetic_names,
                        aggregate_bindings,
                    )
                })
                .collect::<Option<Vec<_>>>()?,
        )),
        Expr::Cast { expr, target_type } => Some(Expr::Cast {
            expr: Box::new(rewrite_simple_grouped_output_expr(
                expr,
                group_exprs,
                table_name,
                binding_name,
                table_binding,
                synthetic_names,
                aggregate_bindings,
            )?),
            target_type: *target_type,
        }),
        Expr::Aggregate { .. } => {
            let index = aggregate_bindings.iter().position(|binding| {
                matching_simple_grouped_aggregate_binding(
                    expr,
                    table_name,
                    binding_name,
                    std::slice::from_ref(binding),
                )
                .is_some()
            })?;
            Some(Expr::Column {
                table: None,
                column: synthetic_names[group_count + index].clone(),
            })
        }
        Expr::Column { .. }
        | Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => None,
    }
}

pub(crate) fn simple_residual_matches(
    candidate: &Value,
    plan: &SimpleResidualPlan,
) -> Result<bool> {
    // SQL three-valued logic: any comparison with a NULL operand yields
    // NULL (unknown), which a WHERE treats as false. Mirror the generic
    // executor's NULL short-circuit (eval_binary, expressions.rs) so the
    // residual fast path never includes rows that the generic path would
    // exclude for `col <> v`, `col < v`, `col <= v` on a NULL candidate.
    if matches!(candidate, Value::Null) || matches!(plan.value, Value::Null) {
        return Ok(false);
    }
    // Incompatible-type comparisons return Err from compare_values. The
    // generic executor may coerce some of these; rather than aborting the
    // query on the fast path, treat the term as not satisfied so the row is
    // excluded consistently with a WHERE that cannot match.
    let ordering = if let Some(ordering) = simple_fast_compare_values(candidate, &plan.value) {
        ordering
    } else {
        let Ok(ordering) = compare_values(candidate, &plan.value) else {
            return Ok(false);
        };
        ordering
    };
    let truthy = match plan.op {
        BinaryOp::Eq => ordering == std::cmp::Ordering::Equal,
        BinaryOp::NotEq => ordering != std::cmp::Ordering::Equal,
        BinaryOp::Gt => ordering == std::cmp::Ordering::Greater,
        BinaryOp::GtEq => ordering != std::cmp::Ordering::Less,
        BinaryOp::Lt => ordering == std::cmp::Ordering::Less,
        BinaryOp::LtEq => ordering != std::cmp::Ordering::Greater,
        _ => false,
    };
    Ok(truthy)
}

pub(crate) fn simple_fast_compare_values(
    left: &Value,
    right: &Value,
) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => Some(left.cmp(right)),
        (Value::Float64(left), Value::Float64(right)) => Some(left.total_cmp(right)),
        (Value::DateDays(left), Value::DateDays(right)) => Some(left.cmp(right)),
        (
            Value::TimestampMicros(left) | Value::TimestampTzMicros(left),
            Value::TimestampMicros(right) | Value::TimestampTzMicros(right),
        ) => Some(left.cmp(right)),
        (Value::TimeMicros(left), Value::TimeMicros(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

pub(crate) fn simple_residual_matches_all(
    values: &[Value],
    residual_plans: &[SimpleResidualPlan],
) -> Result<bool> {
    if residual_plans.is_empty() {
        return Ok(true);
    }
    for plan in residual_plans {
        let Some(candidate) = values.get(plan.column_index) else {
            return Ok(false);
        };
        if !simple_residual_matches(candidate, plan)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn simple_range_projection_filter(
    filter: &Expr,
) -> Option<SimpleRangeProjectionFilter<'_>> {
    let mut state = SimpleRangeFilterState::default();
    collect_simple_range_projection_terms(filter, &mut state)?;
    Some(SimpleRangeProjectionFilter {
        table: state.table,
        column: state.column?,
        lower: state.lower,
        upper: state.upper,
        residual: state.residual,
    })
}

pub(crate) fn simple_contains_like_projection_filter(
    filter: &Expr,
) -> Option<(Option<&str>, &str, &str)> {
    let Expr::Like {
        expr,
        pattern,
        escape: None,
        case_insensitive: false,
        negated: false,
    } = filter
    else {
        return None;
    };
    let Expr::Column { table, column } = expr.as_ref() else {
        return None;
    };
    let Expr::Literal(Value::Text(pattern)) = pattern.as_ref() else {
        return None;
    };
    let literal = simple_contains_like_literal(pattern)?;
    Some((table.as_deref(), column.as_str(), literal))
}

pub(crate) fn simple_contains_like_literal(pattern: &str) -> Option<&str> {
    let literal = pattern.strip_prefix('%')?.strip_suffix('%')?;
    if literal.is_empty() || literal.bytes().any(|byte| matches!(byte, b'%' | b'_')) {
        return None;
    }
    Some(literal)
}

pub(crate) fn collect_simple_range_projection_terms<'a>(
    filter: &'a Expr,
    state: &mut SimpleRangeFilterState<'a>,
) -> Option<()> {
    match filter {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_simple_range_projection_terms(left, state)?;
            collect_simple_range_projection_terms(right, state)?;
            Some(())
        }
        Expr::Binary { left, op, right } => {
            let bound = simple_range_projection_bound(left, *op, right)
                .or_else(|| simple_range_projection_bound(right, reverse_binary_op(*op)?, left));
            if let Some((table, column, bound_kind, value_expr)) = bound {
                // Determine whether this term is on the same column as the
                // range column we are building. If it is on a different
                // column, do not bail; fall through to the residual handling
                // below so conjunctive filters like
                // `rating BETWEEN 7.5 AND 9.0 AND runtime_minutes > 120` can
                // still use the filtered projection fast path with a residual
                // predicate instead of falling back to the generic executor.
                let same_as_range_column = state
                    .column
                    .is_some_and(|existing| identifiers_equal(existing, column));
                if same_as_range_column {
                    if let Some(existing_table) = state.table {
                        if Some(existing_table) != table {
                            return None;
                        }
                    } else {
                        state.table = table;
                    }
                    match bound_kind {
                        SimpleRangeBoundKind::Lower(inclusive) => {
                            if state.lower.is_some() {
                                return None;
                            }
                            state.lower = Some(SimpleRangeBound {
                                inclusive,
                                value_expr,
                            });
                        }
                        SimpleRangeBoundKind::Upper(inclusive) => {
                            if state.upper.is_some() {
                                return None;
                            }
                            state.upper = Some(SimpleRangeBound {
                                inclusive,
                                value_expr,
                            });
                        }
                    }
                    return Some(());
                }
                if state.column.is_some() {
                    // A range column is already chosen and this term is on a
                    // different column; treat it as a residual below.
                } else {
                    // No range column chosen yet and this term is a range
                    // bound; claim it as the range column.
                    if let Some(existing_table) = state.table {
                        if Some(existing_table) != table {
                            return None;
                        }
                    } else {
                        state.table = table;
                    }
                    state.column = Some(column);
                    match bound_kind {
                        SimpleRangeBoundKind::Lower(inclusive) => {
                            if state.lower.is_some() {
                                return None;
                            }
                            state.lower = Some(SimpleRangeBound {
                                inclusive,
                                value_expr,
                            });
                        }
                        SimpleRangeBoundKind::Upper(inclusive) => {
                            if state.upper.is_some() {
                                return None;
                            }
                            state.upper = Some(SimpleRangeBound {
                                inclusive,
                                value_expr,
                            });
                        }
                    }
                    return Some(());
                }
            }
            // Not a range bound on the range column. Try to capture it as a
            // simple residual column-vs-literal/param comparison on a
            // different column so the filtered projection fast path can
            // still apply the range prefilter and evaluate the residual
            // inline, avoiding the generic executor for conjunctive filters
            // like `rating BETWEEN 7.5 AND 9.0 AND runtime_minutes > 120`.
            let (res_table, res_column, res_op, res_value) =
                simple_residual_projection_bound(left, *op, right).or_else(|| {
                    simple_residual_projection_bound(right, reverse_binary_op(*op)?, left)
                })?;
            if let Some(existing_table) = state.table {
                if Some(existing_table) != res_table && res_table.is_some() {
                    return None;
                }
            }
            if state
                .column
                .is_some_and(|existing| identifiers_equal(existing, res_column))
            {
                // Residual on the same column as the range would duplicate a
                // bound we already captured; bail to keep semantics simple.
                return None;
            }
            if state
                .residual
                .iter()
                .any(|existing| identifiers_equal(existing.column, res_column))
            {
                // At most one residual term per column to avoid interaction
                // edge cases (e.g. two predicates on the same column).
                return None;
            }
            state.residual.push(SimpleResidualFilterTerm {
                table: res_table,
                column: res_column,
                op: res_op,
                value_expr: res_value,
            });
            Some(())
        }
        _ => None,
    }
}

pub(crate) fn simple_range_projection_bound<'a>(
    left: &'a Expr,
    op: BinaryOp,
    right: &'a Expr,
) -> Option<(Option<&'a str>, &'a str, SimpleRangeBoundKind, &'a Expr)> {
    let Expr::Column { table, column } = left else {
        return None;
    };
    if !simple_bound_value_expr_is_constant(right) {
        return None;
    }
    let bound_kind = match op {
        BinaryOp::Gt => SimpleRangeBoundKind::Lower(false),
        BinaryOp::GtEq => SimpleRangeBoundKind::Lower(true),
        BinaryOp::Lt => SimpleRangeBoundKind::Upper(false),
        BinaryOp::LtEq => SimpleRangeBoundKind::Upper(true),
        _ => return None,
    };
    Some((table.as_deref(), column.as_str(), bound_kind, right))
}

pub(crate) fn simple_residual_projection_bound<'a>(
    left: &'a Expr,
    op: BinaryOp,
    right: &'a Expr,
) -> Option<(Option<&'a str>, &'a str, BinaryOp, &'a Expr)> {
    let Expr::Column { table, column } = left else {
        return None;
    };
    if !simple_bound_value_expr_is_constant(right) {
        return None;
    }
    if !matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
    ) {
        return None;
    }
    Some((table.as_deref(), column.as_str(), op, right))
}

/// A range/residual bound value is "constant" if it can be evaluated once
/// without row context: a literal, a parameter, or a cast of a literal or
/// parameter (e.g. `CAST('2010-01-01' AS DATE)`, which is how the parser
/// represents typed date literals).
pub(crate) fn simple_bound_value_expr_is_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => true,
        Expr::Cast { expr, .. } => simple_bound_value_expr_is_constant(expr),
        _ => false,
    }
}

pub(crate) fn simple_int64_constant_expr_value(
    expr: &Expr,
    params: &[Value],
) -> Result<Option<i64>> {
    match expr {
        Expr::Literal(Value::Int64(value)) => Ok(Some(*value)),
        Expr::Parameter(index) => {
            let Some(value) = index.checked_sub(1).and_then(|index| params.get(index)) else {
                return Err(DbError::sql(format!("missing parameter ${index}")));
            };
            match value {
                Value::Int64(value) => Ok(Some(*value)),
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

pub(crate) fn simple_range_bounds_match_column_type(
    column_type: ColumnType,
    lower_bound: Option<&SimpleRangeBoundValue>,
    upper_bound: Option<&SimpleRangeBoundValue>,
) -> bool {
    lower_bound.is_none_or(|bound| simple_value_matches_column_type(column_type, &bound.value))
        && upper_bound
            .is_none_or(|bound| simple_value_matches_column_type(column_type, &bound.value))
}

pub(crate) fn simple_value_matches_column_type(column_type: ColumnType, value: &Value) -> bool {
    matches!(
        (column_type, value),
        (ColumnType::Int64, Value::Int64(_))
            | (ColumnType::Float64, Value::Float64(_))
            | (ColumnType::Text, Value::Text(_))
            | (ColumnType::Bool, Value::Bool(_))
            | (ColumnType::Blob, Value::Blob(_))
            | (ColumnType::Decimal, Value::Decimal { .. })
            | (ColumnType::Uuid, Value::Uuid(_))
            | (ColumnType::Timestamp, Value::TimestampMicros(_))
            | (ColumnType::Enum, Value::Enum { .. })
            | (ColumnType::IpAddr, Value::IpAddr { .. })
            | (ColumnType::Cidr, Value::Cidr { .. })
            | (ColumnType::MacAddr, Value::MacAddr { .. })
            | (ColumnType::Date, Value::DateDays(_))
            | (ColumnType::Time, Value::TimeMicros(_))
            | (ColumnType::TimestampTz, Value::TimestampTzMicros(_))
            | (ColumnType::Interval, Value::Interval { .. })
            | (ColumnType::Geometry, Value::Geometry(_))
            | (ColumnType::Geography, Value::Geography(_))
    )
}

pub(crate) fn timestamp_key_types_match(left: ColumnType, right: ColumnType) -> bool {
    !matches!(
        (left, right),
        (ColumnType::Timestamp, ColumnType::TimestampTz)
            | (ColumnType::TimestampTz, ColumnType::Timestamp)
    )
}

pub(crate) fn coerce_lookup_literal(
    table: &TableSchema,
    column: &str,
    expr: &Expr,
    value: Value,
) -> Result<Value> {
    if !matches!(expr, Expr::Literal(Value::Text(_))) {
        return Ok(value);
    }
    let index = schema_column_index(table, column)
        .ok_or_else(|| DbError::sql(format!("unknown column {column}")))?;
    let column = &table.columns[index];
    match column.column_type {
        ColumnType::Decimal => {
            // ADR 0099 orders invalid numeric text after numeric values.
            let original = value.clone();
            constraints::coerce_column_value(column, value).or(Ok(original))
        }
        ColumnType::Uuid | ColumnType::Date | ColumnType::Timestamp | ColumnType::TimestampTz => {
            constraints::coerce_column_value(column, value)
        }
        _ => Ok(value),
    }
}

pub(crate) fn timestamp_lookup_matches_schema(
    table: &TableSchema,
    column: &str,
    value: &Value,
) -> bool {
    let Some(index) = schema_column_index(table, column) else {
        return false;
    };
    let value_type = match value {
        Value::TimestampMicros(_) => ColumnType::Timestamp,
        Value::TimestampTzMicros(_) => ColumnType::TimestampTz,
        _ => return true,
    };
    timestamp_key_types_match(table.columns[index].column_type, value_type)
}

pub(crate) fn simple_range_bound_matches(
    candidate: &Value,
    lower_bound: Option<&SimpleRangeBoundValue>,
    upper_bound: Option<&SimpleRangeBoundValue>,
) -> Result<bool> {
    if let Some(lower_bound) = lower_bound {
        let ordering = simple_fast_compare_values(candidate, &lower_bound.value)
            .map(Ok)
            .unwrap_or_else(|| compare_values(candidate, &lower_bound.value))?;
        let lower_matches = if lower_bound.inclusive {
            ordering != std::cmp::Ordering::Less
        } else {
            ordering == std::cmp::Ordering::Greater
        };
        if !lower_matches {
            return Ok(false);
        }
    }
    if let Some(upper_bound) = upper_bound {
        let ordering = simple_fast_compare_values(candidate, &upper_bound.value)
            .map(Ok)
            .unwrap_or_else(|| compare_values(candidate, &upper_bound.value))?;
        let upper_matches = if upper_bound.inclusive {
            ordering != std::cmp::Ordering::Greater
        } else {
            ordering == std::cmp::Ordering::Less
        };
        if !upper_matches {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn simple_int64_range_start(bound: Option<&SimpleRangeBoundValue>) -> Option<i64> {
    let Some(bound) = bound else {
        return Some(i64::MIN);
    };
    let Value::Int64(value) = &bound.value else {
        return None;
    };
    let value = *value;
    if bound.inclusive {
        Some(value)
    } else {
        value.checked_add(1)
    }
}

pub(crate) fn simple_int64_range_end_exclusive(
    bound: Option<&SimpleRangeBoundValue>,
) -> Option<i64> {
    let Some(bound) = bound else {
        return Some(i64::MAX);
    };
    let Value::Int64(value) = &bound.value else {
        return None;
    };
    let value = *value;
    if bound.inclusive {
        value.checked_add(1)
    } else {
        Some(value)
    }
}

pub(crate) fn simple_join_equality(
    on: &Expr,
) -> Option<(QualifiedColumnRef<'_>, QualifiedColumnRef<'_>)> {
    let Expr::Binary { left, op, right } = on else {
        return None;
    };
    if *op != BinaryOp::Eq {
        return None;
    }
    let (
        Expr::Column {
            table: left_table,
            column: left_column,
        },
        Expr::Column {
            table: right_table,
            column: right_column,
        },
    ) = (&**left, &**right)
    else {
        return None;
    };
    Some((
        QualifiedColumnRef {
            table: left_table.as_deref(),
            column: left_column,
        },
        QualifiedColumnRef {
            table: right_table.as_deref(),
            column: right_column,
        },
    ))
}

pub(crate) fn simple_join_equalities(
    on: &Expr,
) -> Option<Vec<(QualifiedColumnRef<'_>, QualifiedColumnRef<'_>)>> {
    fn collect<'a>(
        expr: &'a Expr,
        equalities: &mut Vec<(QualifiedColumnRef<'a>, QualifiedColumnRef<'a>)>,
    ) -> Option<()> {
        match expr {
            Expr::Binary { left, op, right } if *op == BinaryOp::And => {
                collect(left, equalities)?;
                collect(right, equalities)?;
                Some(())
            }
            _ => {
                equalities.push(simple_join_equality(expr)?);
                Some(())
            }
        }
    }

    let mut equalities = Vec::new();
    collect(on, &mut equalities)?;
    if equalities.is_empty() {
        return None;
    }
    Some(equalities)
}

pub(crate) fn simple_indexed_join_constraint_equalities<'a>(
    constraint: &'a JoinConstraint,
    left: TableBindingRef<'a>,
    right: TableBindingRef<'a>,
    left_schema: &'a TableSchema,
    right_schema: &'a TableSchema,
) -> Option<Vec<(QualifiedColumnRef<'a>, QualifiedColumnRef<'a>)>> {
    let equalities = match constraint {
        JoinConstraint::On(on) => simple_join_equalities(on),
        JoinConstraint::Using(columns) if !columns.is_empty() => {
            let mut equalities = Vec::with_capacity(columns.len());
            for column in columns {
                equalities.push((
                    QualifiedColumnRef {
                        table: Some(left.binding_name()),
                        column: column.as_str(),
                    },
                    QualifiedColumnRef {
                        table: Some(right.binding_name()),
                        column: column.as_str(),
                    },
                ));
            }
            Some(equalities)
        }
        JoinConstraint::Using(_) => None,
        JoinConstraint::Natural => {
            let common_columns = simple_indexed_join_natural_columns(left_schema, right_schema);
            if common_columns.is_empty() {
                return None;
            }
            let mut equalities = Vec::with_capacity(common_columns.len());
            for column in common_columns {
                equalities.push((
                    QualifiedColumnRef {
                        table: Some(left.binding_name()),
                        column,
                    },
                    QualifiedColumnRef {
                        table: Some(right.binding_name()),
                        column,
                    },
                ));
            }
            Some(equalities)
        }
    }?;
    let (left_columns, right_columns) = orient_join_equalities(&equalities, left, right)?;
    for (left_column, right_column) in left_columns.iter().zip(&right_columns) {
        let left_index = schema_column_index(left_schema, left_column)?;
        let right_index = schema_column_index(right_schema, right_column)?;
        if !timestamp_key_types_match(
            left_schema.columns[left_index].column_type,
            right_schema.columns[right_index].column_type,
        ) {
            return None;
        }
    }
    Some(equalities)
}

pub(crate) fn simple_indexed_join_using_columns(
    constraint: &JoinConstraint,
    left_schema: &TableSchema,
    right_schema: &TableSchema,
) -> Vec<String> {
    match constraint {
        JoinConstraint::Using(columns) => columns.clone(),
        JoinConstraint::Natural => simple_indexed_join_natural_columns(left_schema, right_schema)
            .into_iter()
            .map(str::to_string)
            .collect(),
        JoinConstraint::On(_) => Vec::new(),
    }
}

pub(crate) fn simple_indexed_join_natural_columns<'a>(
    left_schema: &'a TableSchema,
    right_schema: &'a TableSchema,
) -> Vec<&'a str> {
    left_schema
        .columns
        .iter()
        .filter(|left_column| {
            right_schema
                .columns
                .iter()
                .any(|right_column| identifiers_equal(&left_column.name, &right_column.name))
        })
        .map(|column| column.name.as_str())
        .collect()
}

pub(crate) fn project_simple_projection_row(
    stored_row: &StoredRow,
    projection_indexes: &[usize],
) -> QueryRow {
    project_simple_projection_values(&stored_row.values, projection_indexes)
}

pub(crate) fn project_simple_projection_value_vec(
    values: &[Value],
    projection_indexes: &[usize],
) -> Vec<Value> {
    let mut projected = Vec::with_capacity(projection_indexes.len());
    for index in projection_indexes {
        projected.push(values[*index].clone());
    }
    projected
}

pub(crate) fn project_simple_projection_values(
    values: &[Value],
    projection_indexes: &[usize],
) -> QueryRow {
    let mut projected = SmallVec::<[Value; 4]>::with_capacity(projection_indexes.len());
    for index in projection_indexes {
        projected.push(values[*index].clone());
    }
    QueryRow::from_small_values(projected)
}

pub(crate) fn simple_expression_projection_plan<'a>(
    table_schema: &'a TableSchema,
    table_name: &str,
    binding_name: &str,
    projection: &'a [SelectItem],
) -> Option<SimpleExpressionProjectionPlan<'a>> {
    let mut sources = Vec::new();
    let mut column_names = Vec::new();
    for (item_index, item) in projection.iter().enumerate() {
        match item {
            SelectItem::Expr { expr, alias } => {
                if let Expr::Column { table, column } = expr {
                    let column_index = simple_expression_projection_column_index(
                        table_schema,
                        table_name,
                        binding_name,
                        table.as_deref(),
                        column,
                    )?;
                    sources.push(SimpleExpressionProjectionSource::Column(column_index));
                    column_names.push(alias.clone().unwrap_or_else(|| column.clone()));
                } else {
                    sources.push(SimpleExpressionProjectionSource::Expr(expr));
                    column_names.push(
                        alias
                            .clone()
                            .unwrap_or_else(|| infer_expr_name(expr, item_index + 1)),
                    );
                }
            }
            SelectItem::Wildcard => {
                for (column_index, column) in table_schema.columns.iter().enumerate() {
                    sources.push(SimpleExpressionProjectionSource::Column(column_index));
                    column_names.push(column.name.clone());
                }
            }
            SelectItem::QualifiedWildcard(qualified_name) => {
                if !identifiers_equal(qualified_name, table_name)
                    && !identifiers_equal(qualified_name, binding_name)
                {
                    return None;
                }
                for (column_index, column) in table_schema.columns.iter().enumerate() {
                    sources.push(SimpleExpressionProjectionSource::Column(column_index));
                    column_names.push(column.name.clone());
                }
            }
        }
    }
    Some(SimpleExpressionProjectionPlan {
        sources,
        column_names,
    })
}

pub(crate) fn simple_expression_projection_column_index(
    table_schema: &TableSchema,
    table_name: &str,
    binding_name: &str,
    qualifier: Option<&str>,
    column_name: &str,
) -> Option<usize> {
    if let Some(qualifier) = qualifier {
        if !identifiers_equal(qualifier, table_name) && !identifiers_equal(qualifier, binding_name)
        {
            return None;
        }
    }
    table_schema
        .columns
        .iter()
        .position(|candidate| identifiers_equal(&candidate.name, column_name))
}

pub(crate) fn simple_grouped_having_bindings(column_count: usize) -> Vec<ColumnBinding> {
    (0..column_count)
        .map(|index| ColumnBinding::visible(None, format!("__grouped_having_col_{index}")))
        .collect()
}

pub(crate) fn simple_grouped_having_column_name(
    select: &Select,
    table_name: &str,
    binding_name: &str,
    column_names: &[String],
    synthetic_names: &[String],
    table: Option<&str>,
    column: &str,
) -> Option<String> {
    if let Some(table) = table {
        if !identifiers_equal(table, table_name) && !identifiers_equal(table, binding_name) {
            return None;
        }
        return unique_grouped_having_group_index(select, column)
            .map(|index| synthetic_names[index].clone());
    }

    let alias_index = unique_grouped_having_column_index(column_names, column);
    let group_index = unique_grouped_having_group_index(select, column);
    match (alias_index, group_index) {
        (Some(alias_index), None) => Some(synthetic_names[alias_index].clone()),
        (None, Some(group_index)) => Some(synthetic_names[group_index].clone()),
        (Some(alias_index), Some(group_index)) if alias_index == group_index => {
            Some(synthetic_names[alias_index].clone())
        }
        _ => None,
    }
}

pub(crate) fn analyze_simple_grouped_numeric_aggregate_binding(
    expr: &Expr,
    projection_index: usize,
    table_name: &str,
    binding_name: &str,
    table_binding: TableBindingRef<'_>,
    table_schema: &TableSchema,
) -> Option<SimpleGroupedNumericAggregateBinding> {
    let Expr::Aggregate {
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
    if !order_by.is_empty() || *within_group {
        return None;
    }
    if name.eq_ignore_ascii_case("count") {
        if *distinct && *star {
            return None;
        }
        if args.is_empty() && *star {
            return Some(SimpleGroupedNumericAggregateBinding {
                kind: SimpleGroupedNumericAggregateKind::CountRows,
                projection_index,
                source_column_name: None,
                source_column_index: None,
                source_expr: None,
            });
        }
        if *star || args.len() != 1 || !expr_references_only_binding(&args[0], table_binding) {
            return None;
        }
        if let Expr::Column { table, column } = &args[0] {
            if let Some(table) = table.as_deref() {
                if !identifiers_equal(table, table_name) && !identifiers_equal(table, binding_name)
                {
                    return None;
                }
            }
            let column_index = table_schema
                .columns
                .iter()
                .position(|candidate| identifiers_equal(&candidate.name, column))?;
            return Some(SimpleGroupedNumericAggregateBinding {
                kind: if *distinct {
                    SimpleGroupedNumericAggregateKind::CountDistinct
                } else {
                    SimpleGroupedNumericAggregateKind::CountNonNull
                },
                projection_index,
                source_column_name: Some(column.clone()),
                source_column_index: Some(column_index),
                source_expr: None,
            });
        }
        return Some(SimpleGroupedNumericAggregateBinding {
            kind: if *distinct {
                SimpleGroupedNumericAggregateKind::CountDistinct
            } else {
                SimpleGroupedNumericAggregateKind::CountNonNull
            },
            projection_index,
            source_column_name: None,
            source_column_index: None,
            source_expr: Some(args[0].clone()),
        });
    }

    let kind = if name.eq_ignore_ascii_case("sum") {
        if *distinct {
            SimpleGroupedNumericAggregateKind::SumDistinct
        } else {
            SimpleGroupedNumericAggregateKind::Sum
        }
    } else if name.eq_ignore_ascii_case("avg") {
        if *distinct {
            SimpleGroupedNumericAggregateKind::AvgDistinct
        } else {
            SimpleGroupedNumericAggregateKind::Avg
        }
    } else if name.eq_ignore_ascii_case("total") {
        if *distinct {
            SimpleGroupedNumericAggregateKind::TotalDistinct
        } else {
            SimpleGroupedNumericAggregateKind::Total
        }
    } else if name.eq_ignore_ascii_case("stddev") || name.eq_ignore_ascii_case("stddev_samp") {
        if *distinct {
            SimpleGroupedNumericAggregateKind::StddevSampDistinct
        } else {
            SimpleGroupedNumericAggregateKind::StddevSamp
        }
    } else if name.eq_ignore_ascii_case("stddev_pop") {
        if *distinct {
            SimpleGroupedNumericAggregateKind::StddevPopDistinct
        } else {
            SimpleGroupedNumericAggregateKind::StddevPop
        }
    } else if name.eq_ignore_ascii_case("variance") || name.eq_ignore_ascii_case("var_samp") {
        if *distinct {
            SimpleGroupedNumericAggregateKind::VarSampDistinct
        } else {
            SimpleGroupedNumericAggregateKind::VarSamp
        }
    } else if name.eq_ignore_ascii_case("var_pop") {
        if *distinct {
            SimpleGroupedNumericAggregateKind::VarPopDistinct
        } else {
            SimpleGroupedNumericAggregateKind::VarPop
        }
    } else if name.eq_ignore_ascii_case("bool_and") {
        if *distinct {
            SimpleGroupedNumericAggregateKind::BoolAndDistinct
        } else {
            SimpleGroupedNumericAggregateKind::BoolAnd
        }
    } else if name.eq_ignore_ascii_case("bool_or") {
        if *distinct {
            SimpleGroupedNumericAggregateKind::BoolOrDistinct
        } else {
            SimpleGroupedNumericAggregateKind::BoolOr
        }
    } else if name.eq_ignore_ascii_case("min") {
        if *distinct {
            return None;
        }
        SimpleGroupedNumericAggregateKind::Min
    } else if name.eq_ignore_ascii_case("max") {
        if *distinct {
            return None;
        }
        SimpleGroupedNumericAggregateKind::Max
    } else {
        return None;
    };
    if *star || args.len() != 1 {
        return None;
    }
    if !expr_references_only_binding(&args[0], table_binding) {
        return None;
    }

    if matches!(
        kind,
        SimpleGroupedNumericAggregateKind::Min | SimpleGroupedNumericAggregateKind::Max
    ) {
        return Some(SimpleGroupedNumericAggregateBinding {
            kind,
            projection_index,
            source_column_name: None,
            source_column_index: None,
            source_expr: Some(args[0].clone()),
        });
    }

    if let Expr::Column { table, column } = &args[0] {
        if let Some(table) = table.as_deref() {
            if !identifiers_equal(table, table_name) && !identifiers_equal(table, binding_name) {
                return None;
            }
        }
        let column_index = table_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, column))?;
        Some(SimpleGroupedNumericAggregateBinding {
            kind,
            projection_index,
            source_column_name: Some(column.clone()),
            source_column_index: Some(column_index),
            source_expr: None,
        })
    } else {
        Some(SimpleGroupedNumericAggregateBinding {
            kind,
            projection_index,
            source_column_name: None,
            source_column_index: None,
            source_expr: Some(args[0].clone()),
        })
    }
}

pub(crate) fn simple_aggregate_source_column_index(
    aggregate: &SimpleGroupedNumericAggregateBinding,
    table_schema: &TableSchema,
) -> Option<usize> {
    if let Some(column_index) = aggregate.source_column_index {
        return Some(column_index);
    }
    let Some(Expr::Column { column, .. }) = aggregate.source_expr.as_ref() else {
        return None;
    };
    schema_column_index(table_schema, column)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn collect_simple_grouped_numeric_projection_aggregates(
    expr: &Expr,
    table_name: &str,
    binding_name: &str,
    table_binding: TableBindingRef<'_>,
    table_schema: &TableSchema,
    aggregate_bindings: &mut Vec<SimpleGroupedNumericAggregateBinding>,
    saw_supported_aggregate: &mut bool,
) -> Option<()> {
    match expr {
        Expr::Default => None,
        Expr::Literal(_) | Expr::Parameter(_) => Some(()),
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => collect_simple_grouped_numeric_projection_aggregates(
            expr,
            table_name,
            binding_name,
            table_binding,
            table_schema,
            aggregate_bindings,
            saw_supported_aggregate,
        ),
        Expr::Binary { left, right, .. } => {
            collect_simple_grouped_numeric_projection_aggregates(
                left,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            collect_simple_grouped_numeric_projection_aggregates(
                right,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_simple_grouped_numeric_projection_aggregates(
                expr,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            collect_simple_grouped_numeric_projection_aggregates(
                low,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            collect_simple_grouped_numeric_projection_aggregates(
                high,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )
        }
        Expr::InList { expr, items, .. } => {
            collect_simple_grouped_numeric_projection_aggregates(
                expr,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            for item in items {
                collect_simple_grouped_numeric_projection_aggregates(
                    item,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            Some(())
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_simple_grouped_numeric_projection_aggregates(
                expr,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            collect_simple_grouped_numeric_projection_aggregates(
                pattern,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            if let Some(escape) = escape {
                collect_simple_grouped_numeric_projection_aggregates(
                    escape,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            Some(())
        }
        Expr::Function { args, .. } | Expr::Row(args) => {
            for arg in args {
                collect_simple_grouped_numeric_projection_aggregates(
                    arg,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            Some(())
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_simple_grouped_numeric_projection_aggregates(
                    operand,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            for (condition, value) in branches {
                collect_simple_grouped_numeric_projection_aggregates(
                    condition,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
                collect_simple_grouped_numeric_projection_aggregates(
                    value,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            if let Some(else_expr) = else_expr {
                collect_simple_grouped_numeric_projection_aggregates(
                    else_expr,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            Some(())
        }
        Expr::Aggregate { .. } => {
            let binding = analyze_simple_grouped_numeric_aggregate_binding(
                expr,
                usize::MAX,
                table_name,
                binding_name,
                table_binding,
                table_schema,
            )?;
            if !matches!(binding.kind, SimpleGroupedNumericAggregateKind::CountRows) {
                *saw_supported_aggregate = true;
            }
            if !aggregate_bindings.iter().any(|existing| {
                existing.kind == binding.kind
                    && existing.source_column_name == binding.source_column_name
                    && existing.source_expr == binding.source_expr
            }) {
                aggregate_bindings.push(binding);
            }
            Some(())
        }
        Expr::Column { .. }
        | Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn collect_simple_grouped_numeric_having_aggregates(
    expr: &Expr,
    table_name: &str,
    binding_name: &str,
    table_binding: TableBindingRef<'_>,
    table_schema: &TableSchema,
    aggregate_bindings: &mut Vec<SimpleGroupedNumericAggregateBinding>,
    saw_supported_aggregate: &mut bool,
) -> Option<()> {
    match expr {
        Expr::Default => None,
        Expr::Literal(_) | Expr::Parameter(_) | Expr::Column { .. } => Some(()),
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Collate { expr, .. } => collect_simple_grouped_numeric_having_aggregates(
            expr,
            table_name,
            binding_name,
            table_binding,
            table_schema,
            aggregate_bindings,
            saw_supported_aggregate,
        ),
        Expr::Binary { left, right, .. } => {
            collect_simple_grouped_numeric_having_aggregates(
                left,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            collect_simple_grouped_numeric_having_aggregates(
                right,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_simple_grouped_numeric_having_aggregates(
                expr,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            collect_simple_grouped_numeric_having_aggregates(
                low,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            collect_simple_grouped_numeric_having_aggregates(
                high,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )
        }
        Expr::InList { expr, items, .. } => {
            collect_simple_grouped_numeric_having_aggregates(
                expr,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            for item in items {
                collect_simple_grouped_numeric_having_aggregates(
                    item,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            Some(())
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_simple_grouped_numeric_having_aggregates(
                expr,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            collect_simple_grouped_numeric_having_aggregates(
                pattern,
                table_name,
                binding_name,
                table_binding,
                table_schema,
                aggregate_bindings,
                saw_supported_aggregate,
            )?;
            if let Some(escape) = escape {
                collect_simple_grouped_numeric_having_aggregates(
                    escape,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            Some(())
        }
        Expr::Function { args, .. } | Expr::Row(args) => {
            for arg in args {
                collect_simple_grouped_numeric_having_aggregates(
                    arg,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            Some(())
        }
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_simple_grouped_numeric_having_aggregates(
                    operand,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            for (condition, value) in branches {
                collect_simple_grouped_numeric_having_aggregates(
                    condition,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
                collect_simple_grouped_numeric_having_aggregates(
                    value,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            if let Some(else_expr) = else_expr {
                collect_simple_grouped_numeric_having_aggregates(
                    else_expr,
                    table_name,
                    binding_name,
                    table_binding,
                    table_schema,
                    aggregate_bindings,
                    saw_supported_aggregate,
                )?;
            }
            Some(())
        }
        Expr::Aggregate { .. } => {
            let binding = analyze_simple_grouped_numeric_aggregate_binding(
                expr,
                usize::MAX,
                table_name,
                binding_name,
                table_binding,
                table_schema,
            )?;
            if !matches!(binding.kind, SimpleGroupedNumericAggregateKind::CountRows) {
                *saw_supported_aggregate = true;
            }
            if !aggregate_bindings.iter().any(|existing| {
                existing.kind == binding.kind
                    && existing.source_column_name == binding.source_column_name
                    && existing.source_expr == binding.source_expr
            }) {
                aggregate_bindings.push(binding);
            }
            Some(())
        }
        Expr::RowNumber { .. }
        | Expr::WindowFunction { .. }
        | Expr::InSubquery { .. }
        | Expr::CompareSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_) => None,
    }
}

pub(crate) fn simple_select_item_column_index(
    dataset: &Dataset,
    table: Option<&str>,
    column: &str,
) -> Option<usize> {
    let matches = dataset
        .columns
        .iter()
        .enumerate()
        .filter(|(_, binding)| {
            if !identifiers_equal(&binding.name, column) {
                return false;
            }
            match table {
                Some(table_name) => binding
                    .table
                    .as_deref()
                    .is_some_and(|binding_table| identifiers_equal(binding_table, table_name)),
                None => !binding.hidden,
            }
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Some(*index),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn simple_join_projection_plan(
    items: &[SelectItem],
    eval_dataset: &Dataset,
    left_table_name: &str,
    left_alias: &Option<String>,
    left_schema: &TableSchema,
    right_table_name: &str,
    right_alias: &Option<String>,
    right_schema: &TableSchema,
    using_join_columns: &[String],
) -> Option<(Vec<SimpleJoinProjectionSource>, Vec<String>)> {
    let left_binding = left_alias.as_deref().unwrap_or(left_table_name);
    let right_binding = right_alias.as_deref().unwrap_or(right_table_name);
    let mut projection_plan = Vec::new();
    let mut column_names = Vec::new();

    for (index, item) in items.iter().enumerate() {
        match item {
            SelectItem::Wildcard => {
                if !using_join_columns.is_empty() {
                    for using_column in using_join_columns {
                        projection_plan.push(SimpleJoinProjectionSource::Expr(
                            merged_single_column_using_expr(
                                left_binding,
                                right_binding,
                                using_column,
                            ),
                        ));
                        column_names.push(using_column.to_string());
                    }
                    projection_plan.extend(
                        left_schema
                            .columns
                            .iter()
                            .enumerate()
                            .filter(|(_, column)| {
                                !using_join_columns.iter().any(|using_column| {
                                    identifiers_equal(&column.name, using_column)
                                })
                            })
                            .map(|(column_index, _)| {
                                SimpleJoinProjectionSource::Left(column_index)
                            }),
                    );
                    column_names.extend(
                        left_schema
                            .columns
                            .iter()
                            .filter(|column| {
                                !using_join_columns.iter().any(|using_column| {
                                    identifiers_equal(&column.name, using_column)
                                })
                            })
                            .map(|column| column.name.clone()),
                    );
                    projection_plan.extend(
                        right_schema
                            .columns
                            .iter()
                            .enumerate()
                            .filter(|(_, column)| {
                                !using_join_columns.iter().any(|using_column| {
                                    identifiers_equal(&column.name, using_column)
                                })
                            })
                            .map(|(column_index, _)| {
                                SimpleJoinProjectionSource::Right(column_index)
                            }),
                    );
                    column_names.extend(
                        right_schema
                            .columns
                            .iter()
                            .filter(|column| {
                                !using_join_columns.iter().any(|using_column| {
                                    identifiers_equal(&column.name, using_column)
                                })
                            })
                            .map(|column| column.name.clone()),
                    );
                } else {
                    projection_plan.extend(
                        left_schema
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(column_index, _)| {
                                SimpleJoinProjectionSource::Left(column_index)
                            }),
                    );
                    column_names
                        .extend(left_schema.columns.iter().map(|column| column.name.clone()));
                    projection_plan.extend(
                        right_schema
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(column_index, _)| {
                                SimpleJoinProjectionSource::Right(column_index)
                            }),
                    );
                    column_names.extend(
                        right_schema
                            .columns
                            .iter()
                            .map(|column| column.name.clone()),
                    );
                }
            }
            SelectItem::QualifiedWildcard(table_name) => {
                if identifiers_equal(table_name, left_table_name)
                    || identifiers_equal(table_name, left_binding)
                {
                    projection_plan.extend(
                        left_schema
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(column_index, _)| {
                                SimpleJoinProjectionSource::Left(column_index)
                            }),
                    );
                    column_names
                        .extend(left_schema.columns.iter().map(|column| column.name.clone()));
                } else if identifiers_equal(table_name, right_table_name)
                    || identifiers_equal(table_name, right_binding)
                {
                    projection_plan.extend(
                        right_schema
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(column_index, _)| {
                                SimpleJoinProjectionSource::Right(column_index)
                            }),
                    );
                    column_names.extend(
                        right_schema
                            .columns
                            .iter()
                            .map(|column| column.name.clone()),
                    );
                } else {
                    return None;
                }
            }
            SelectItem::Expr { expr, alias } => {
                let source = if let Expr::Column { table, column } = expr {
                    let left_index = left_schema
                        .columns
                        .iter()
                        .position(|candidate| identifiers_equal(&candidate.name, column));
                    let right_index = right_schema
                        .columns
                        .iter()
                        .position(|candidate| identifiers_equal(&candidate.name, column));
                    match table.as_deref() {
                        Some(table_name)
                            if identifiers_equal(table_name, left_table_name)
                                || identifiers_equal(table_name, left_binding) =>
                        {
                            Some(SimpleJoinProjectionSource::Left(left_index?))
                        }
                        Some(table_name)
                            if identifiers_equal(table_name, right_table_name)
                                || identifiers_equal(table_name, right_binding) =>
                        {
                            Some(SimpleJoinProjectionSource::Right(right_index?))
                        }
                        Some(_) => None,
                        None => match (left_index, right_index) {
                            (Some(left_index), None) => {
                                Some(SimpleJoinProjectionSource::Left(left_index))
                            }
                            (None, Some(right_index)) => {
                                Some(SimpleJoinProjectionSource::Right(right_index))
                            }
                            (Some(_), Some(_))
                                if using_join_columns.iter().any(|using_column| {
                                    identifiers_equal(column, using_column)
                                }) =>
                            {
                                Some(SimpleJoinProjectionSource::Expr(
                                    merged_single_column_using_expr(
                                        left_binding,
                                        right_binding,
                                        column,
                                    ),
                                ))
                            }
                            _ => None,
                        },
                    }
                } else if expr_resolves_against_dataset(expr, eval_dataset) {
                    Some(SimpleJoinProjectionSource::Expr(expr.clone()))
                } else {
                    None
                }?;
                projection_plan.push(source);
                column_names.push(
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                );
            }
        }
    }

    Some((projection_plan, column_names))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn simple_join_projection_order_by_plan(
    query: &Query,
    projection_items: &[SelectItem],
    projection_plan: &[SimpleJoinProjectionSource],
    column_names: &[String],
    left_table_name: &str,
    left_alias: &Option<String>,
    left_schema: &TableSchema,
    right_table_name: &str,
    right_alias: &Option<String>,
    right_schema: &TableSchema,
) -> Result<Option<Vec<SimpleOrderByPlan>>> {
    if query.order_by.is_empty() {
        return Ok(None);
    }
    let left_binding = TableBindingRef {
        name: left_table_name,
        alias: left_alias,
    };
    let right_binding = TableBindingRef {
        name: right_table_name,
        alias: right_alias,
    };
    let direct_order = query
        .order_by
        .iter()
        .map(|entry| {
            let Expr::Column {
                table: order_table,
                column: order_column,
            } = &entry.expr
            else {
                return None;
            };

            let mut projection_index = None;
            for (index, source) in projection_plan.iter().enumerate() {
                let source_matches = match source {
                    SimpleJoinProjectionSource::Left(column_index) => {
                        identifiers_equal(&left_schema.columns[*column_index].name, order_column)
                            && order_table.as_deref().is_none_or(|qualifier| {
                                matches_table_binding(left_binding, Some(qualifier))
                            })
                    }
                    SimpleJoinProjectionSource::Right(column_index) => {
                        identifiers_equal(&right_schema.columns[*column_index].name, order_column)
                            && order_table.as_deref().is_none_or(|qualifier| {
                                matches_table_binding(right_binding, Some(qualifier))
                            })
                    }
                    SimpleJoinProjectionSource::Expr(_) => false,
                };
                let alias_matches = order_table.is_none()
                    && column_names
                        .get(index)
                        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(order_column));
                if !source_matches && !alias_matches {
                    continue;
                }
                if projection_index.replace(index).is_some() {
                    return None;
                }
            }

            projection_index.map(|projection_index| SimpleOrderByPlan {
                projection_index,
                descending: entry.descending,
                collation: entry.collation.clone(),
            })
        })
        .collect::<Option<Vec<_>>>();
    if direct_order.is_some() {
        return Ok(direct_order);
    }

    Ok(projection_order_by_plan(&query.order_by, projection_items))
}

pub(crate) fn simple_join_eval_row(
    left_values: Option<&[Value]>,
    left_width: usize,
    right_values: Option<&[Value]>,
    right_width: usize,
) -> Vec<Value> {
    let mut values = Vec::with_capacity(left_width + right_width);
    match left_values {
        Some(left_values) => values.extend(left_values.iter().cloned()),
        None => values.extend((0..left_width).map(|_| Value::Null)),
    }
    match right_values {
        Some(right_values) => values.extend(right_values.iter().cloned()),
        None => values.extend((0..right_width).map(|_| Value::Null)),
    }
    values
}

pub(crate) fn simple_join_key_from_indexes(
    row: &[Value],
    indexes: &[usize],
) -> Result<Option<Vec<u8>>> {
    let join_values = indexes
        .iter()
        .map(|index| {
            row.get(*index)
                .ok_or_else(|| DbError::internal("join row is shorter than table schema"))
        })
        .collect::<Result<Vec<_>>>()?;
    if join_values
        .iter()
        .any(|join_value| matches!(join_value, Value::Null))
    {
        return Ok(None);
    }
    Row::new(join_values.into_iter().map(equality_key_value).collect())
        .encode()
        .map(Some)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn simple_join_filter_matches(
    runtime: &EngineRuntime,
    filter: Option<&Expr>,
    eval_dataset: &Dataset,
    left_values: Option<&[Value]>,
    left_width: usize,
    right_values: Option<&[Value]>,
    right_width: usize,
    params: &[Value],
) -> Result<bool> {
    let Some(filter) = filter else {
        return Ok(true);
    };
    let joined_values = simple_join_eval_row(left_values, left_width, right_values, right_width);
    Ok(matches!(
        runtime.eval_expr(
            filter,
            eval_dataset,
            &joined_values,
            params,
            &BTreeMap::new(),
            None,
        )?,
        Value::Bool(true)
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn project_simple_join_row(
    runtime: &EngineRuntime,
    projection_plan: &[SimpleJoinProjectionSource],
    eval_dataset: &Dataset,
    left_values: Option<&[Value]>,
    left_width: usize,
    right_values: Option<&[Value]>,
    right_width: usize,
    params: &[Value],
) -> Result<Vec<Value>> {
    let mut projected = Vec::with_capacity(projection_plan.len());
    let mut joined_values = None::<Vec<Value>>;
    for slot in projection_plan {
        match slot {
            SimpleJoinProjectionSource::Left(index) => {
                let value = left_values
                    .and_then(|row| row.get(*index))
                    .cloned()
                    .unwrap_or(Value::Null);
                projected.push(value);
            }
            SimpleJoinProjectionSource::Right(index) => {
                let value = right_values
                    .and_then(|row| row.get(*index))
                    .cloned()
                    .unwrap_or(Value::Null);
                projected.push(value);
            }
            SimpleJoinProjectionSource::Expr(expr) => {
                let joined_values = joined_values.get_or_insert_with(|| {
                    simple_join_eval_row(left_values, left_width, right_values, right_width)
                });
                projected.push(runtime.eval_expr(
                    expr,
                    eval_dataset,
                    joined_values,
                    params,
                    &BTreeMap::new(),
                    None,
                )?);
            }
        }
    }
    Ok(projected)
}

pub(crate) fn simple_trigram_lookup(filter: &Expr) -> Option<SimpleTrigramLookup<'_>> {
    match filter {
        Expr::Like {
            expr,
            pattern,
            escape,
            negated,
            ..
        } if !negated && escape.is_none() => match (&**expr, &**pattern) {
            (Expr::Column { table, column }, pattern @ (Expr::Literal(_) | Expr::Parameter(_))) => {
                Some(SimpleTrigramLookup {
                    table_qualifier: table.as_deref(),
                    column_name: column.as_str(),
                    pattern_expr: pattern,
                    has_additional_filter: false,
                })
            }
            _ => None,
        },
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            if let Some(mut lookup) = simple_trigram_lookup(left) {
                lookup.has_additional_filter = true;
                Some(lookup)
            } else {
                simple_trigram_lookup(right).map(|mut lookup| {
                    lookup.has_additional_filter = true;
                    lookup
                })
            }
        }
        _ => None,
    }
}

pub(crate) fn simple_fulltext_lookup(filter: &Expr) -> Option<SimpleFullTextLookup<'_>> {
    match filter {
        Expr::Function { name, args }
            if name.eq_ignore_ascii_case("fulltext_match") && args.len() == 2 =>
        {
            Some(SimpleFullTextLookup {
                index_name_expr: &args[0],
                query_expr: &args[1],
            })
        }
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => simple_fulltext_lookup(left).or_else(|| simple_fulltext_lookup(right)),
        _ => None,
    }
}

pub(crate) fn simple_spatial_join_predicate<'a>(
    expr: &'a Expr,
    left_binding: TableBindingRef<'a>,
    right_binding: TableBindingRef<'a>,
) -> Option<SimpleSpatialJoinPredicate<'a>> {
    let Expr::Function { name, args } = expr else {
        return None;
    };
    let lower = name.to_ascii_lowercase();
    let (left, right, radius_expr) = if lower == "st_dwithin" {
        let [left, right, radius] = args.as_slice() else {
            return None;
        };
        if expr_has_column_ref(radius) {
            return None;
        }
        (left, right, Some(radius))
    } else if matches!(
        lower.as_str(),
        "st_intersects" | "st_contains" | "st_within" | "st_equals"
    ) {
        let [left, right] = args.as_slice() else {
            return None;
        };
        (left, right, None)
    } else {
        return None;
    };
    let left_ref = qualified_column_ref_expr(left)?;
    let right_ref = qualified_column_ref_expr(right)?;
    let left_is_left = matches_table_binding(left_binding, left_ref.table);
    let left_is_right = matches_table_binding(right_binding, left_ref.table);
    let right_is_left = matches_table_binding(left_binding, right_ref.table);
    let right_is_right = matches_table_binding(right_binding, right_ref.table);
    if (left_is_left && right_is_right) || (left_is_right && right_is_left) {
        Some(SimpleSpatialJoinPredicate {
            left: left_ref,
            right: right_ref,
            radius_expr,
        })
    } else {
        None
    }
}

pub(crate) fn simple_spatial_lookup(filter: &Expr) -> Option<SimpleSpatialLookup<'_>> {
    match filter {
        Expr::Function { name, args } if name.eq_ignore_ascii_case("st_dwithin") => {
            let [left, right, radius] = args.as_slice() else {
                return None;
            };
            simple_spatial_column_value_pair(left, right).map(
                |(table_qualifier, column_name, value_expr)| SimpleSpatialLookup {
                    table_qualifier,
                    column_name,
                    value_expr,
                    radius_expr: Some(radius),
                },
            )
        }
        Expr::Function { name, args }
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "st_intersects" | "st_contains" | "st_within" | "st_equals"
            ) =>
        {
            let [left, right] = args.as_slice() else {
                return None;
            };
            simple_spatial_column_value_pair(left, right).map(
                |(table_qualifier, column_name, value_expr)| SimpleSpatialLookup {
                    table_qualifier,
                    column_name,
                    value_expr,
                    radius_expr: None,
                },
            )
        }
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => simple_spatial_lookup(left).or_else(|| simple_spatial_lookup(right)),
        _ => None,
    }
}

pub(crate) fn simple_spatial_column_value_pair<'a>(
    left: &'a Expr,
    right: &'a Expr,
) -> Option<(Option<&'a str>, &'a str, &'a Expr)> {
    match (left, right) {
        (Expr::Column { table, column }, value) if !expr_has_column_ref(value) => {
            Some((table.as_deref(), column.as_str(), value))
        }
        (value, Expr::Column { table, column }) if !expr_has_column_ref(value) => {
            Some((table.as_deref(), column.as_str(), value))
        }
        _ => None,
    }
}

impl EngineRuntime {
    fn analyze_simple_count_query<'a>(
        &'a self,
        query: &'a Query,
    ) -> Result<Option<SimpleCountQueryPlan<'a>>> {
        if !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
            || select.projection.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table {
            name,
            alias: table_alias,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }
        let Some(table) = self.table_schema(name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table) {
            return Ok(None);
        }

        let SelectItem::Expr { expr, alias } = &select.projection[0] else {
            return Ok(None);
        };
        let Expr::Aggregate {
            name: aggregate_name,
            args,
            distinct,
            star,
            order_by,
            within_group,
        } = expr
        else {
            return Ok(None);
        };
        if !aggregate_name.eq_ignore_ascii_case("count")
            || !args.is_empty()
            || *distinct
            || !*star
            || !order_by.is_empty()
            || *within_group
        {
            return Ok(None);
        }

        Ok(Some(SimpleCountQueryPlan {
            table_name: name,
            table_ref: table_alias.as_deref().unwrap_or(name),
            filter: select.filter.as_ref(),
            column_name: alias.clone().unwrap_or_else(|| infer_expr_name(expr, 1)),
        }))
    }
    pub(crate) fn try_execute_simple_count_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_count_query(query)? else {
            return Ok(None);
        };

        let row_count = if let Some(filter) = plan.filter {
            let table = self
                .table_schema(plan.table_name)
                .ok_or_else(|| DbError::sql(format!("unknown table {}", plan.table_name)))?;
            dml::matching_row_ids(
                self,
                plan.table_name,
                plan.table_ref,
                table,
                Some(filter),
                params,
            )?
            .len()
        } else {
            self.visible_table_row_source(plan.table_name).map_or_else(
                || {
                    self.table_data(plan.table_name)
                        .map_or(0, TableData::row_count)
                },
                |source| source.row_count(),
            )
        };
        let row_count = i64::try_from(row_count).map_err(|_| {
            DbError::sql(format!(
                "table {} exceeds COUNT(*) row-count limits",
                plan.table_name
            ))
        })?;
        Ok(Some(QueryResult::with_rows(
            vec![plan.column_name],
            vec![QueryRow::new(vec![Value::Int64(row_count)])],
        )))
    }
    fn analyze_simple_min_max_query<'a>(
        &'a self,
        query: &'a Query,
    ) -> Result<Option<SimpleMinMaxQueryPlan<'a>>> {
        if !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
            || select.projection.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }
        let Some(table) = self.table_schema(name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table) {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr,
            alias: output_alias,
        } = &select.projection[0]
        else {
            return Ok(None);
        };
        let Expr::Aggregate {
            name: aggregate_name,
            args,
            distinct,
            star,
            order_by,
            within_group,
        } = expr
        else {
            return Ok(None);
        };
        if *distinct || *star || !order_by.is_empty() || *within_group || args.len() != 1 {
            return Ok(None);
        }
        let is_max = if aggregate_name.eq_ignore_ascii_case("max") {
            true
        } else if aggregate_name.eq_ignore_ascii_case("min") {
            false
        } else {
            return Ok(None);
        };

        let binding_name = alias.as_deref().unwrap_or(name);
        let Expr::Column {
            table: aggregate_table,
            column: aggregate_column,
        } = &args[0]
        else {
            return Ok(None);
        };
        if let Some(aggregate_table) = aggregate_table.as_deref() {
            if !identifiers_equal(aggregate_table, name)
                && !identifiers_equal(aggregate_table, binding_name)
            {
                return Ok(None);
            }
        }
        let Some(column_index) = table
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, aggregate_column))
        else {
            return Ok(None);
        };

        Ok(Some(SimpleMinMaxQueryPlan {
            table_name: name,
            column_index,
            is_max,
            column_name: output_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(expr, 1)),
        }))
    }
    pub(crate) fn try_execute_simple_min_max_query(
        &self,
        query: &Query,
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_min_max_query(query)? else {
            return Ok(None);
        };
        let Some(row_source) = self.visible_table_row_source(plan.table_name) else {
            return Ok(None);
        };
        Ok(Some(
            self.simple_min_max_result_from_source(row_source, &plan)?,
        ))
    }
    fn simple_min_max_result_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        plan: &SimpleMinMaxQueryPlan<'_>,
    ) -> Result<QueryResult> {
        let mut best = Value::Null;
        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            update_simple_min_max_value(
                &mut best,
                stored_row.values()[plan.column_index].clone(),
                plan.is_max,
            )?;
        }
        Ok(QueryResult::with_rows(
            vec![plan.column_name.clone()],
            vec![QueryRow::new(vec![best])],
        ))
    }
    fn simple_min_max_result_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        plan: &SimpleMinMaxQueryPlan<'_>,
    ) -> Result<QueryResult> {
        let mut best = Value::Null;
        visit_persisted_table_rows(store, state, |_, values| {
            update_simple_min_max_value(&mut best, values[plan.column_index].clone(), plan.is_max)
        })?;
        Ok(QueryResult::with_rows(
            vec![plan.column_name.clone()],
            vec![QueryRow::new(vec![best])],
        ))
    }
    pub(crate) fn try_execute_simple_deferred_count_query(
        &self,
        query: &Query,
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_count_query(query)? else {
            return Ok(None);
        };
        if plan.filter.is_some() {
            return Ok(None);
        }
        if self.visible_table_is_temporary(plan.table_name)
            || self.visible_table_row_source(plan.table_name).is_some()
            || !self.has_deferred_tables()
        {
            return Ok(None);
        }
        if !self
            .deferred_table_names()
            .any(|candidate| identifiers_equal(candidate, plan.table_name))
        {
            return Ok(None);
        }
        let Some(state) = self.persisted_table_state(plan.table_name) else {
            return Ok(None);
        };
        let row_count = if state.pointer.is_table_paged_manifest()
            && state.pointer.head_page_id != 0
            && state.pointer.logical_len != 0
        {
            let store = SnapshotPageStore {
                pager,
                wal,
                snapshot_lsn,
            };
            read_persisted_table_row_count(&store, state)
                .ok()
                .and_then(|count| i64::try_from(count).ok())
                .unwrap_or(0)
        } else {
            self.catalog
                .table_stats
                .iter()
                .find(|(name, _)| identifiers_equal(name, plan.table_name))
                .map(|(_, stats)| stats.row_count)
                .or_else(|| {
                    if state.row_count == 0
                        && state.pointer.head_page_id != 0
                        && state.pointer.logical_len != 0
                    {
                        let store = SnapshotPageStore {
                            pager,
                            wal,
                            snapshot_lsn,
                        };
                        read_persisted_table_row_count(&store, state)
                            .ok()
                            .and_then(|count| i64::try_from(count).ok())
                    } else {
                        i64::try_from(state.row_count).ok()
                    }
                })
                .unwrap_or(0)
        };
        Ok(Some(QueryResult::with_rows(
            vec![plan.column_name],
            vec![QueryRow::new(vec![Value::Int64(row_count)])],
        )))
    }
    pub(crate) fn try_execute_simple_deferred_min_max_query(
        &self,
        query: &Query,
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_min_max_query(query)? else {
            return Ok(None);
        };
        if self.visible_table_is_temporary(plan.table_name)
            || self.visible_table_row_source(plan.table_name).is_some()
            || !self.has_deferred_tables()
        {
            return Ok(None);
        }
        if !self
            .deferred_table_names()
            .any(|candidate| identifiers_equal(candidate, plan.table_name))
        {
            return Ok(None);
        }
        let Some(state) = self.persisted_table_state(plan.table_name) else {
            return Ok(None);
        };
        if !state.pointer.is_table_paged_manifest() {
            return Ok(None);
        }
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        Ok(Some(self.simple_min_max_result_from_persisted_state(
            &store, state, &plan,
        )?))
    }
    pub(crate) fn try_execute_simple_grouped_count_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_grouped_count_query(query, params)? else {
            return Ok(None);
        };
        let Some(source) = self.visible_table_row_source(plan.table_name) else {
            return Ok(None);
        };
        Ok(Some(self.simple_grouped_count_result_from_source(
            source, &plan, params,
        )?))
    }
    pub(crate) fn analyze_simple_grouped_count_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<SimpleGroupedCountPlan<'a>>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
            || select.group_by.is_empty()
            || select.projection.len() != select.group_by.len() + 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let binding_name = alias.as_deref().unwrap_or(name);
        let table_binding = TableBindingRef { name, alias };
        for group_expr in &select.group_by {
            if expr_contains_recursive_unsupported_feature(group_expr)
                || !expr_references_only_binding(group_expr, table_binding)
            {
                return Ok(None);
            }
        }
        let group_eval_bindings = table_schema
            .columns
            .iter()
            .map(|column| {
                ColumnBinding::visible(Some(binding_name.to_string()), column.name.clone())
                    .with_type(column.column_type)
            })
            .collect::<Vec<_>>();

        let filter_expr = match select.filter.as_ref() {
            Some(filter)
                if !expr_contains_recursive_unsupported_feature(filter)
                    && expr_references_only_binding(filter, table_binding) =>
            {
                Some(filter.clone())
            }
            Some(_) => return Ok(None),
            None => None,
        };

        let mut column_names = Vec::with_capacity(select.projection.len());
        let mut group_projection_needs_rewrite = false;
        for (projection_item, group_expr) in select
            .projection
            .iter()
            .take(select.group_by.len())
            .zip(&select.group_by)
        {
            let SelectItem::Expr {
                expr: projection_group_expr,
                alias: projection_group_alias,
            } = projection_item
            else {
                return Ok(None);
            };
            if !grouped_projection_expr_matches_group_expr(
                projection_group_expr,
                group_expr,
                table_binding,
            ) {
                group_projection_needs_rewrite = true;
            }
            column_names.push(
                projection_group_alias.clone().unwrap_or_else(|| {
                    infer_expr_name(projection_group_expr, column_names.len() + 1)
                }),
            );
        }

        let SelectItem::Expr {
            expr: count_expr,
            alias: count_alias,
        } = &select.projection[select.group_by.len()]
        else {
            return Ok(None);
        };
        let count_projection_index = select.group_by.len();
        let count_binding = SimpleGroupedNumericAggregateBinding {
            kind: SimpleGroupedNumericAggregateKind::CountRows,
            projection_index: count_projection_index,
            source_column_name: None,
            source_column_index: None,
            source_expr: None,
        };
        let direct_count = matches!(
            count_expr,
            Expr::Aggregate {
                name,
                args,
                distinct,
                star,
                order_by,
                within_group,
            } if name.eq_ignore_ascii_case("count")
                && args.is_empty()
                && !*distinct
                && *star
                && order_by.is_empty()
                && !*within_group
        );

        column_names.push(
            count_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(count_expr, select.group_by.len() + 1)),
        );
        let needs_projection_rewrite = group_projection_needs_rewrite || !direct_count;
        let (projection_exprs, raw_projection_bindings, having, having_bindings, order_by) =
            if needs_projection_rewrite {
                let raw_projection_bindings =
                    simple_grouped_projection_bindings(select.group_by.len(), 1);
                let raw_names = raw_projection_bindings
                    .iter()
                    .map(|binding| binding.name.clone())
                    .collect::<Vec<_>>();
                let projection_exprs = select
                    .projection
                    .iter()
                    .map(|item| {
                        let SelectItem::Expr { expr, .. } = item else {
                            return None;
                        };
                        rewrite_simple_grouped_output_expr(
                            expr,
                            select.group_by.as_slice(),
                            name,
                            binding_name,
                            table_binding,
                            &raw_names,
                            std::slice::from_ref(&count_binding),
                        )
                    })
                    .collect::<Option<Vec<_>>>();
                let Some(projection_exprs) = projection_exprs else {
                    return Ok(None);
                };
                let having = match select.having.as_ref() {
                    Some(having) => rewrite_simple_grouped_output_expr(
                        having,
                        select.group_by.as_slice(),
                        name,
                        binding_name,
                        table_binding,
                        &raw_names,
                        std::slice::from_ref(&count_binding),
                    ),
                    None => None,
                };
                if select.having.is_some() && having.is_none() {
                    return Ok(None);
                }
                let order_by = projection_order_by_plan(&query.order_by, &select.projection);
                if !query.order_by.is_empty() && order_by.is_none() {
                    return Ok(None);
                }
                (
                    Some(projection_exprs),
                    Some(raw_projection_bindings.clone()),
                    having,
                    raw_projection_bindings,
                    order_by,
                )
            } else {
                let having_bindings = simple_grouped_having_bindings(column_names.len());
                let having_names = having_bindings
                    .iter()
                    .map(|binding| binding.name.clone())
                    .collect::<Vec<_>>();
                let having = match select.having.as_ref() {
                    Some(having) => self.rewrite_simple_grouped_having_expr(
                        having,
                        select,
                        name,
                        binding_name,
                        &column_names,
                        &having_names,
                        count_projection_index,
                        None,
                    )?,
                    None => None,
                };
                if select.having.is_some() && having.is_none() {
                    return Ok(None);
                }
                let order_by = self.simple_grouped_order_by_plan(
                    query,
                    select,
                    name,
                    binding_name,
                    &column_names,
                )?;
                if !query.order_by.is_empty() && order_by.is_none() {
                    return Ok(None);
                }
                (None, None, having, having_bindings, order_by)
            };
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        Ok(Some(SimpleGroupedCountPlan {
            table_name: name,
            group_exprs: &select.group_by,
            group_eval_bindings,
            filter_expr,
            column_names,
            projection_exprs,
            raw_projection_bindings,
            having,
            having_bindings,
            order_by,
            limit,
            offset,
        }))
    }
    fn simple_grouped_count_result_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        plan: &SimpleGroupedCountPlan<'_>,
        params: &[Value],
    ) -> Result<QueryResult> {
        if let Some(result) =
            self.try_simple_grouped_count_result_from_runtime_index(Some(row_source), plan, params)?
        {
            return Ok(result);
        }

        let mut groups = Vec::<SimpleGroupedCountAggregate>::new();
        let mut group_positions = BTreeMap::<Vec<u8>, usize>::new();
        let group_dataset = Dataset::with_rows(plan.group_eval_bindings.clone(), Vec::new());
        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            if let Some(filter_expr) = plan.filter_expr.as_ref() {
                if !matches!(
                    self.eval_expr(
                        filter_expr,
                        &group_dataset,
                        stored_row.values(),
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }

            let group_values = evaluate_simple_grouped_values(
                self,
                plan.group_exprs,
                &group_dataset,
                stored_row.values(),
                params,
            )?;
            let group_key = row_identity(&group_values)?;
            let group_index = if let Some(group_index) = group_positions.get(&group_key).copied() {
                group_index
            } else {
                groups.push(SimpleGroupedCountAggregate::new(group_values));
                let group_index = groups.len() - 1;
                group_positions.insert(group_key, group_index);
                group_index
            };
            groups[group_index].count += 1;
        }

        render_simple_grouped_count_groups(self, groups, plan, params)
    }
    pub(crate) fn try_simple_grouped_count_result_from_runtime_index(
        &self,
        row_source: Option<VisibleTableRowSource<'_>>,
        plan: &SimpleGroupedCountPlan<'_>,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if plan.group_exprs.len() != 1
            || plan.filter_expr.is_some()
            || plan.projection_exprs.is_some()
            || plan.having.is_some()
        {
            return Ok(None);
        }
        let Expr::Column {
            table: group_table,
            column: group_column,
        } = &plan.group_exprs[0]
        else {
            return Ok(None);
        };
        if group_table
            .as_deref()
            .is_some_and(|table| !identifiers_equal(table, plan.table_name))
        {
            return Ok(None);
        }
        let Some(table_schema) = self.table_schema(plan.table_name) else {
            return Ok(None);
        };
        let Some(group_column_index) = schema_column_index(table_schema, group_column) else {
            return Ok(None);
        };
        let Some(index) = self.single_column_btree_index(plan.table_name, group_column) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
            return Ok(None);
        };

        let mut groups = Vec::new();
        match keys {
            RuntimeBtreeKeys::UniqueInt64(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_id) in entries.iter() {
                    if deleted.contains(&row_id) {
                        continue;
                    }
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![Value::Int64(key)],
                        count: 1,
                    });
                }
            }
            RuntimeBtreeKeys::NonUniqueInt64(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_ids) in entries.iter() {
                    let count = row_ids
                        .iter()
                        .filter(|row_id| !deleted.contains(row_id))
                        .count();
                    if count == 0 {
                        continue;
                    }
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![Value::Int64(key)],
                        count: i64::try_from(count).map_err(|_| {
                            DbError::constraint(
                                "grouped COUNT index bucket exceeds INT64 row-count limits",
                            )
                        })?,
                    });
                }
            }
            RuntimeBtreeKeys::UniqueUuid(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_id) in entries.iter() {
                    if deleted.contains(row_id) {
                        continue;
                    }
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![Value::Uuid(*key)],
                        count: 1,
                    });
                }
            }
            RuntimeBtreeKeys::NonUniqueUuid(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_ids) in entries.iter() {
                    let count = row_ids
                        .iter()
                        .filter(|row_id| !deleted.contains(row_id))
                        .count();
                    if count == 0 {
                        continue;
                    }
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![Value::Uuid(*key)],
                        count: i64::try_from(count).map_err(|_| {
                            DbError::constraint(
                                "grouped COUNT index bucket exceeds INT64 row-count limits",
                            )
                        })?,
                    });
                }
            }
            RuntimeBtreeKeys::UniqueEncoded(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_id) in entries.iter() {
                    if deleted.contains(row_id) {
                        continue;
                    }
                    let Some(values) = Self::runtime_index_key_values_to_group_values(
                        key,
                        Some(*row_id),
                        row_source,
                        &[group_column_index],
                    )?
                    else {
                        continue;
                    };
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: values,
                        count: 1,
                    });
                }
            }
            RuntimeBtreeKeys::NonUniqueEncoded(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_ids) in entries.iter() {
                    let count = row_ids
                        .iter()
                        .filter(|row_id| !deleted.contains(row_id))
                        .count();
                    if count == 0 {
                        continue;
                    }
                    let Some(row_id) = row_ids
                        .iter()
                        .copied()
                        .find(|row_id| !deleted.contains(row_id))
                    else {
                        continue;
                    };
                    let Some(values) = Self::runtime_index_key_values_to_group_values(
                        key,
                        Some(row_id),
                        row_source,
                        &[group_column_index],
                    )?
                    else {
                        continue;
                    };
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: values,
                        count: i64::try_from(count).map_err(|_| {
                            DbError::constraint(
                                "grouped COUNT index bucket exceeds INT64 row-count limits",
                            )
                        })?,
                    });
                }
            }
        }

        Ok(Some(render_simple_grouped_count_groups(
            self, groups, plan, params,
        )?))
    }
    pub(crate) fn try_execute_simple_grouped_count_sql_from_runtime_index(
        &self,
        table_name: &str,
        group_column: &str,
    ) -> Result<Option<QueryResult>> {
        if self.security_rules_active()?
            || self
                .visible_view(table_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(table_name)
        {
            return Ok(None);
        }
        let Some(table_schema) = self.table_schema(table_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some(group_column_index) = schema_column_index(table_schema, group_column) else {
            return Ok(None);
        };
        let Some(index) = self.single_column_btree_index(table_name, group_column) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
            return Ok(None);
        };

        let mut groups = Vec::new();
        match keys {
            RuntimeBtreeKeys::UniqueInt64(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_id) in entries.iter() {
                    if deleted.contains(&row_id) {
                        continue;
                    }
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![Value::Int64(key)],
                        count: 1,
                    });
                }
            }
            RuntimeBtreeKeys::NonUniqueInt64(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_ids) in entries.iter() {
                    let count = row_ids
                        .iter()
                        .filter(|row_id| !deleted.contains(row_id))
                        .count();
                    if count == 0 {
                        continue;
                    }
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![Value::Int64(key)],
                        count: i64::try_from(count).map_err(|_| {
                            DbError::constraint(
                                "grouped COUNT index bucket exceeds INT64 row-count limits",
                            )
                        })?,
                    });
                }
            }
            RuntimeBtreeKeys::UniqueUuid(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_id) in entries.iter() {
                    if deleted.contains(row_id) {
                        continue;
                    }
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![Value::Uuid(*key)],
                        count: 1,
                    });
                }
            }
            RuntimeBtreeKeys::NonUniqueUuid(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_ids) in entries.iter() {
                    let count = row_ids
                        .iter()
                        .filter(|row_id| !deleted.contains(row_id))
                        .count();
                    if count == 0 {
                        continue;
                    }
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![Value::Uuid(*key)],
                        count: i64::try_from(count).map_err(|_| {
                            DbError::constraint(
                                "grouped COUNT index bucket exceeds INT64 row-count limits",
                            )
                        })?,
                    });
                }
            }
            RuntimeBtreeKeys::UniqueEncoded(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_id) in entries.iter() {
                    if deleted.contains(row_id) {
                        continue;
                    }
                    let Some(value) = Self::decode_runtime_index_group_key(key) else {
                        return Ok(None);
                    };
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![value],
                        count: 1,
                    });
                }
            }
            RuntimeBtreeKeys::NonUniqueEncoded(entries, deleted) => {
                groups.reserve(entries.len());
                for (key, row_ids) in entries.iter() {
                    let count = row_ids
                        .iter()
                        .filter(|row_id| !deleted.contains(row_id))
                        .count();
                    if count == 0 {
                        continue;
                    }
                    let Some(value) = Self::decode_runtime_index_group_key(key) else {
                        return Ok(None);
                    };
                    groups.push(SimpleGroupedCountAggregate {
                        group_values: vec![value],
                        count: i64::try_from(count).map_err(|_| {
                            DbError::constraint(
                                "grouped COUNT index bucket exceeds INT64 row-count limits",
                            )
                        })?,
                    });
                }
            }
        }

        let mut rows = groups
            .into_iter()
            .map(SimpleGroupedCountAggregate::into_row)
            .collect::<Vec<_>>();
        sort_query_rows_by_projection_order(
            Some(self),
            &mut rows,
            &[SimpleOrderByPlan {
                projection_index: 0,
                descending: false,
                collation: None,
            }],
        )?;
        Ok(Some(QueryResult::with_rows(
            vec![
                table_schema.columns[group_column_index].name.clone(),
                "col2".to_string(),
            ],
            rows,
        )))
    }
    fn simple_grouped_count_result_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        plan: &SimpleGroupedCountPlan<'_>,
        params: &[Value],
    ) -> Result<QueryResult> {
        let mut groups = Vec::<SimpleGroupedCountAggregate>::new();
        let mut group_positions = BTreeMap::<Vec<u8>, usize>::new();
        let group_dataset = Dataset::with_rows(plan.group_eval_bindings.clone(), Vec::new());
        visit_persisted_table_rows(store, state, |_, values| {
            if let Some(filter_expr) = plan.filter_expr.as_ref() {
                if !matches!(
                    self.eval_expr(
                        filter_expr,
                        &group_dataset,
                        values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                    Value::Bool(true)
                ) {
                    return Ok(());
                }
            }

            let group_values = evaluate_simple_grouped_values(
                self,
                plan.group_exprs,
                &group_dataset,
                values,
                params,
            )?;
            let group_key = row_identity(&group_values)?;
            let group_index = if let Some(group_index) = group_positions.get(&group_key).copied() {
                group_index
            } else {
                groups.push(SimpleGroupedCountAggregate::new(group_values));
                let group_index = groups.len() - 1;
                group_positions.insert(group_key, group_index);
                group_index
            };
            groups[group_index].count += 1;
            Ok(())
        })?;

        render_simple_grouped_count_groups(self, groups, plan, params)
    }
    pub(crate) fn try_execute_simple_grouped_numeric_aggregate_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_grouped_numeric_aggregate_query(query, params)? else {
            return Ok(None);
        };
        let Some(source) = self.visible_table_row_source(plan.table_name) else {
            return Ok(None);
        };
        Ok(Some(
            self.simple_grouped_numeric_aggregate_result_from_source(source, &plan, params)?,
        ))
    }
    fn analyze_simple_grouped_numeric_aggregate_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<SimpleGroupedNumericAggregatePlan<'a>>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
            || select.projection.len() <= select.group_by.len()
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let binding_name = alias.as_deref().unwrap_or(name);
        let table_binding = TableBindingRef { name, alias };
        for group_expr in &select.group_by {
            if expr_contains_recursive_unsupported_feature(group_expr)
                || !expr_references_only_binding(group_expr, table_binding)
            {
                return Ok(None);
            }
        }
        let group_eval_bindings = table_schema
            .columns
            .iter()
            .map(|column| {
                ColumnBinding::visible(Some(binding_name.to_string()), column.name.clone())
                    .with_type(column.column_type)
            })
            .collect::<Vec<_>>();

        let filter_expr = match select.filter.as_ref() {
            Some(filter)
                if !expr_contains_recursive_unsupported_feature(filter)
                    && expr_references_only_binding(filter, table_binding) =>
            {
                Some(filter.clone())
            }
            Some(_) => return Ok(None),
            None => None,
        };

        let mut column_names = Vec::with_capacity(select.projection.len());
        let mut group_projection_needs_rewrite = false;
        for (projection_item, group_expr) in select
            .projection
            .iter()
            .take(select.group_by.len())
            .zip(&select.group_by)
        {
            let SelectItem::Expr {
                expr: projection_group_expr,
                alias: projection_group_alias,
            } = projection_item
            else {
                return Ok(None);
            };
            if !grouped_projection_expr_matches_group_expr(
                projection_group_expr,
                group_expr,
                table_binding,
            ) {
                group_projection_needs_rewrite = true;
            }
            column_names.push(
                projection_group_alias.clone().unwrap_or_else(|| {
                    infer_expr_name(projection_group_expr, column_names.len() + 1)
                }),
            );
        }

        let mut aggregate_bindings = Vec::new();
        let mut saw_supported_aggregate = false;
        let mut projection_exprs: Option<Vec<Expr>> = None;
        for (projection_index, projection_item) in select
            .projection
            .iter()
            .enumerate()
            .skip(select.group_by.len())
        {
            let SelectItem::Expr {
                expr: projection_expr,
                alias,
            } = projection_item
            else {
                return Ok(None);
            };
            let binding = if let Some(binding) = analyze_simple_grouped_numeric_aggregate_binding(
                projection_expr,
                projection_index,
                name,
                binding_name,
                table_binding,
                table_schema,
            ) {
                if !matches!(binding.kind, SimpleGroupedNumericAggregateKind::CountRows) {
                    saw_supported_aggregate = true;
                }
                Some(binding)
            } else {
                if collect_simple_grouped_numeric_projection_aggregates(
                    projection_expr,
                    name,
                    binding_name,
                    table_binding,
                    table_schema,
                    &mut aggregate_bindings,
                    &mut saw_supported_aggregate,
                )
                .is_none()
                {
                    return Ok(None);
                }
                projection_exprs.get_or_insert_with(Vec::new);
                None
            };
            column_names.push(
                alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(projection_expr, projection_index + 1)),
            );
            if let Some(binding) = binding {
                aggregate_bindings.push(binding);
            }
        }
        let mut having_requires_projection_rewrite = false;
        if let Some(having) = select.having.as_ref() {
            let aggregate_count_before = aggregate_bindings.len();
            if collect_simple_grouped_numeric_having_aggregates(
                having,
                name,
                binding_name,
                table_binding,
                table_schema,
                &mut aggregate_bindings,
                &mut saw_supported_aggregate,
            )
            .is_none()
            {
                return Ok(None);
            }
            having_requires_projection_rewrite = aggregate_bindings.len() != aggregate_count_before;
        }
        if aggregate_bindings.is_empty() {
            return Ok(None);
        }
        let (projection_exprs, raw_projection_bindings, having, having_bindings, order_by) =
            if group_projection_needs_rewrite
                || projection_exprs.is_some()
                || having_requires_projection_rewrite
            {
                let raw_projection_bindings = simple_grouped_projection_bindings(
                    select.group_by.len(),
                    aggregate_bindings.len(),
                );
                let raw_names = raw_projection_bindings
                    .iter()
                    .map(|binding| binding.name.clone())
                    .collect::<Vec<_>>();
                let mut rewritten_projection_exprs = Vec::with_capacity(select.projection.len());
                for projection_item in &select.projection {
                    let SelectItem::Expr { expr, .. } = projection_item else {
                        return Ok(None);
                    };
                    let Some(rewritten) = rewrite_simple_grouped_output_expr(
                        expr,
                        select.group_by.as_slice(),
                        name,
                        binding_name,
                        table_binding,
                        &raw_names,
                        &aggregate_bindings,
                    ) else {
                        return Ok(None);
                    };
                    rewritten_projection_exprs.push(rewritten);
                }
                let having = match select.having.as_ref() {
                    Some(having) => rewrite_simple_grouped_output_expr(
                        having,
                        select.group_by.as_slice(),
                        name,
                        binding_name,
                        table_binding,
                        &raw_names,
                        &aggregate_bindings,
                    ),
                    None => None,
                };
                if select.having.is_some() && having.is_none() {
                    return Ok(None);
                }
                let order_by = projection_order_by_plan(&query.order_by, &select.projection);
                if !query.order_by.is_empty() && order_by.is_none() {
                    return Ok(None);
                }
                (
                    Some(rewritten_projection_exprs),
                    Some(raw_projection_bindings.clone()),
                    having,
                    raw_projection_bindings,
                    order_by,
                )
            } else {
                let having_bindings = simple_grouped_having_bindings(column_names.len());
                let having_names = having_bindings
                    .iter()
                    .map(|binding| binding.name.clone())
                    .collect::<Vec<_>>();
                let having = match select.having.as_ref() {
                    Some(having) => self.rewrite_simple_grouped_having_expr_with_bindings(
                        having,
                        select,
                        name,
                        binding_name,
                        &column_names,
                        &having_names,
                        &aggregate_bindings,
                    )?,
                    None => None,
                };
                if select.having.is_some() && having.is_none() {
                    return Ok(None);
                }
                let order_by = self.simple_grouped_order_by_plan(
                    query,
                    select,
                    name,
                    binding_name,
                    &column_names,
                )?;
                if !query.order_by.is_empty() && order_by.is_none() {
                    return Ok(None);
                }
                (None, None, having, having_bindings, order_by)
            };
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        Ok(Some(SimpleGroupedNumericAggregatePlan {
            table_name: name,
            group_exprs: &select.group_by,
            group_eval_bindings,
            filter_expr,
            column_names,
            aggregate_bindings,
            projection_exprs,
            raw_projection_bindings,
            having,
            having_bindings,
            order_by,
            limit,
            offset,
        }))
    }
    fn simple_grouped_numeric_aggregate_result_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        plan: &SimpleGroupedNumericAggregatePlan<'_>,
        params: &[Value],
    ) -> Result<QueryResult> {
        if let Some(result) =
            self.try_simple_scalar_int64_numeric_aggregate_from_source(row_source, plan)?
        {
            return Ok(result);
        }
        if let Some(result) =
            self.try_simple_scalar_filtered_numeric_aggregate_from_source(row_source, plan, params)?
        {
            return Ok(result);
        }

        let mut groups = Vec::<SimpleGroupedNumericAggregate>::new();
        let mut group_positions = BTreeMap::<Vec<u8>, usize>::new();
        let group_dataset = Dataset::with_rows(plan.group_eval_bindings.clone(), Vec::new());
        if plan.group_exprs.is_empty() {
            groups.push(SimpleGroupedNumericAggregate::new(
                Vec::new(),
                plan.aggregate_bindings.len(),
            ));
            group_positions.insert(row_identity(&[])?, 0);
        }
        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            if let Some(filter_expr) = plan.filter_expr.as_ref() {
                if !matches!(
                    self.eval_expr(
                        filter_expr,
                        &group_dataset,
                        stored_row.values(),
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }

            let group_values = evaluate_simple_grouped_values(
                self,
                plan.group_exprs,
                &group_dataset,
                stored_row.values(),
                params,
            )?;
            let group_key = row_identity(&group_values)?;
            let group_index = if let Some(group_index) = group_positions.get(&group_key).copied() {
                group_index
            } else {
                groups.push(SimpleGroupedNumericAggregate::new(
                    group_values,
                    plan.aggregate_bindings.len(),
                ));
                let group_index = groups.len() - 1;
                group_positions.insert(group_key, group_index);
                group_index
            };
            groups[group_index].count += 1;
            for (aggregate_index, aggregate) in plan.aggregate_bindings.iter().enumerate() {
                match aggregate.kind {
                    SimpleGroupedNumericAggregateKind::CountNonNull => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            groups[group_index].count_non_null(
                                aggregate_index,
                                &stored_row.values()[source_column_index],
                            );
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                stored_row.values(),
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            groups[group_index].count_non_null(aggregate_index, &value);
                        }
                    }
                    SimpleGroupedNumericAggregateKind::CountDistinct => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            groups[group_index].count_distinct(
                                aggregate_index,
                                &stored_row.values()[source_column_index],
                            )?;
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                stored_row.values(),
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            groups[group_index].count_distinct(aggregate_index, &value)?;
                        }
                    }
                    SimpleGroupedNumericAggregateKind::Sum
                    | SimpleGroupedNumericAggregateKind::SumDistinct
                    | SimpleGroupedNumericAggregateKind::Avg
                    | SimpleGroupedNumericAggregateKind::AvgDistinct
                    | SimpleGroupedNumericAggregateKind::Total
                    | SimpleGroupedNumericAggregateKind::TotalDistinct => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            if matches!(
                                aggregate.kind,
                                SimpleGroupedNumericAggregateKind::SumDistinct
                                    | SimpleGroupedNumericAggregateKind::AvgDistinct
                                    | SimpleGroupedNumericAggregateKind::TotalDistinct
                            ) {
                                groups[group_index].add_numeric_distinct(
                                    aggregate_index,
                                    &stored_row.values()[source_column_index],
                                )?;
                            } else {
                                groups[group_index].add_numeric(
                                    aggregate_index,
                                    &stored_row.values()[source_column_index],
                                )?;
                            }
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                stored_row.values(),
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            if matches!(
                                aggregate.kind,
                                SimpleGroupedNumericAggregateKind::SumDistinct
                                    | SimpleGroupedNumericAggregateKind::AvgDistinct
                                    | SimpleGroupedNumericAggregateKind::TotalDistinct
                            ) {
                                groups[group_index]
                                    .add_numeric_distinct(aggregate_index, &value)?;
                            } else {
                                groups[group_index].add_numeric(aggregate_index, &value)?;
                            }
                        }
                    }
                    SimpleGroupedNumericAggregateKind::StddevSamp
                    | SimpleGroupedNumericAggregateKind::StddevSampDistinct
                    | SimpleGroupedNumericAggregateKind::StddevPop
                    | SimpleGroupedNumericAggregateKind::StddevPopDistinct
                    | SimpleGroupedNumericAggregateKind::VarSamp
                    | SimpleGroupedNumericAggregateKind::VarSampDistinct
                    | SimpleGroupedNumericAggregateKind::VarPop
                    | SimpleGroupedNumericAggregateKind::VarPopDistinct => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            if aggregate.kind.uses_distinct() {
                                groups[group_index].add_variance_distinct(
                                    aggregate_index,
                                    &stored_row.values()[source_column_index],
                                )?;
                            } else {
                                groups[group_index].add_variance(
                                    aggregate_index,
                                    &stored_row.values()[source_column_index],
                                )?;
                            }
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                stored_row.values(),
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            if aggregate.kind.uses_distinct() {
                                groups[group_index]
                                    .add_variance_distinct(aggregate_index, &value)?;
                            } else {
                                groups[group_index].add_variance(aggregate_index, &value)?;
                            }
                        }
                    }
                    SimpleGroupedNumericAggregateKind::BoolAnd
                    | SimpleGroupedNumericAggregateKind::BoolAndDistinct
                    | SimpleGroupedNumericAggregateKind::BoolOr
                    | SimpleGroupedNumericAggregateKind::BoolOrDistinct => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            if aggregate.kind.uses_distinct() {
                                groups[group_index].add_bool_distinct(
                                    aggregate_index,
                                    &stored_row.values()[source_column_index],
                                )?;
                            } else {
                                groups[group_index].add_bool(
                                    aggregate_index,
                                    &stored_row.values()[source_column_index],
                                )?;
                            }
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                stored_row.values(),
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            if aggregate.kind.uses_distinct() {
                                groups[group_index].add_bool_distinct(aggregate_index, &value)?;
                            } else {
                                groups[group_index].add_bool(aggregate_index, &value)?;
                            }
                        }
                    }
                    SimpleGroupedNumericAggregateKind::Min
                    | SimpleGroupedNumericAggregateKind::Max => {
                        let Some(source_expr) = aggregate.source_expr.as_ref() else {
                            continue;
                        };
                        let value = self.eval_expr(
                            source_expr,
                            &group_dataset,
                            stored_row.values(),
                            params,
                            &BTreeMap::new(),
                            None,
                        )?;
                        update_simple_min_max_value(
                            &mut groups[group_index].extreme_values[aggregate_index],
                            value,
                            matches!(aggregate.kind, SimpleGroupedNumericAggregateKind::Max),
                        )?;
                    }
                    SimpleGroupedNumericAggregateKind::CountRows => {}
                }
            }
        }

        render_simple_grouped_numeric_aggregate_groups(self, groups, plan, params)
    }
    fn try_simple_scalar_int64_numeric_aggregate_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        plan: &SimpleGroupedNumericAggregatePlan<'_>,
    ) -> Result<Option<QueryResult>> {
        let Some(column_index) = self.simple_scalar_int64_aggregate_column(plan) else {
            return Ok(None);
        };
        let mut stats = SimpleScalarInt64AggregateStats::default();
        row_source.visit_int64_column_values(column_index, |_, value| {
            stats.add(value);
            Ok(())
        })?;
        Ok(Some(QueryResult::with_rows(
            plan.column_names.clone(),
            vec![QueryRow::new(stats.into_values(&plan.aggregate_bindings))],
        )))
    }
    fn simple_scalar_int64_aggregate_column(
        &self,
        plan: &SimpleGroupedNumericAggregatePlan<'_>,
    ) -> Option<usize> {
        if !plan.group_exprs.is_empty()
            || plan.filter_expr.is_some()
            || plan.projection_exprs.is_some()
            || plan.having.is_some()
            || plan.order_by.is_some()
            || plan.limit.is_some()
            || plan.offset != 0
        {
            return None;
        }
        let table_schema = self.table_schema(plan.table_name)?;
        let mut scalar_column_index = None;
        for aggregate in &plan.aggregate_bindings {
            let column_index = match aggregate.kind {
                SimpleGroupedNumericAggregateKind::CountRows => continue,
                SimpleGroupedNumericAggregateKind::CountNonNull
                | SimpleGroupedNumericAggregateKind::Sum
                | SimpleGroupedNumericAggregateKind::Avg
                | SimpleGroupedNumericAggregateKind::Min
                | SimpleGroupedNumericAggregateKind::Max => {
                    simple_aggregate_source_column_index(aggregate, table_schema)?
                }
                _ => return None,
            };
            if table_schema.columns.get(column_index)?.column_type != ColumnType::Int64 {
                return None;
            }
            if scalar_column_index
                .replace(column_index)
                .is_some_and(|existing| existing != column_index)
            {
                return None;
            }
        }
        scalar_column_index
    }
    fn try_simple_scalar_filtered_numeric_aggregate_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        plan: &SimpleGroupedNumericAggregatePlan<'_>,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !plan.group_exprs.is_empty()
            || plan.projection_exprs.is_some()
            || plan.having.is_some()
            || plan.order_by.is_some()
            || plan.limit.is_some()
            || plan.offset != 0
        {
            return Ok(None);
        }
        let Some(filter_expr) = plan.filter_expr.as_ref() else {
            return Ok(None);
        };
        let Some((_filter_table, filter_column, filter_value_expr)) =
            simple_btree_lookup(filter_expr)
        else {
            return Ok(None);
        };
        let Some(table_schema) = self.table_schema(plan.table_name) else {
            return Ok(None);
        };
        let Some(filter_column_index) = schema_column_index(table_schema, filter_column) else {
            return Ok(None);
        };
        if plan.aggregate_bindings.iter().any(|aggregate| {
            !matches!(
                aggregate.kind,
                SimpleGroupedNumericAggregateKind::CountRows
                    | SimpleGroupedNumericAggregateKind::Sum
            ) || (matches!(aggregate.kind, SimpleGroupedNumericAggregateKind::Sum)
                && aggregate.source_column_index.is_none())
        }) {
            return Ok(None);
        }

        let filter_value = self.eval_expr(
            filter_value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        let mut row_count = 0_i64;
        let mut numeric_states =
            vec![SimpleGroupedNumericState::default(); plan.aggregate_bindings.len()];

        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            if compare_values(&stored_row.values()[filter_column_index], &filter_value)?
                != std::cmp::Ordering::Equal
            {
                continue;
            }
            row_count += 1;
            for (aggregate_index, aggregate) in plan.aggregate_bindings.iter().enumerate() {
                if matches!(aggregate.kind, SimpleGroupedNumericAggregateKind::Sum) {
                    let source_column_index = aggregate.source_column_index.ok_or_else(|| {
                        DbError::internal("simple scalar SUM missing source column")
                    })?;
                    numeric_states[aggregate_index]
                        .add(&stored_row.values()[source_column_index])?;
                }
            }
        }

        let values = plan
            .aggregate_bindings
            .iter()
            .enumerate()
            .map(|(aggregate_index, aggregate)| match aggregate.kind {
                SimpleGroupedNumericAggregateKind::CountRows => Ok(Value::Int64(row_count)),
                SimpleGroupedNumericAggregateKind::Sum => {
                    numeric_states[aggregate_index].value(aggregate.kind)
                }
                _ => Ok(Value::Null),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(QueryResult::with_rows(
            plan.column_names.clone(),
            vec![QueryRow::new(values)],
        )))
    }
    fn try_simple_scalar_filtered_numeric_aggregate_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        plan: &SimpleGroupedNumericAggregatePlan<'_>,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !plan.group_exprs.is_empty()
            || plan.projection_exprs.is_some()
            || plan.having.is_some()
            || plan.order_by.is_some()
            || plan.limit.is_some()
            || plan.offset != 0
        {
            return Ok(None);
        }
        let Some(filter_expr) = plan.filter_expr.as_ref() else {
            return Ok(None);
        };
        let Some((_filter_table, filter_column, filter_value_expr)) =
            simple_btree_lookup(filter_expr)
        else {
            return Ok(None);
        };
        let Some(table_schema) = self.table_schema(plan.table_name) else {
            return Ok(None);
        };
        let Some(filter_column_index) = schema_column_index(table_schema, filter_column) else {
            return Ok(None);
        };
        if plan.aggregate_bindings.iter().any(|aggregate| {
            !matches!(
                aggregate.kind,
                SimpleGroupedNumericAggregateKind::CountRows
                    | SimpleGroupedNumericAggregateKind::Sum
            ) || (matches!(aggregate.kind, SimpleGroupedNumericAggregateKind::Sum)
                && aggregate.source_column_index.is_none())
        }) {
            return Ok(None);
        }

        let filter_value = self.eval_expr(
            filter_value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        let mut row_count = 0_i64;
        let mut numeric_states =
            vec![SimpleGroupedNumericState::default(); plan.aggregate_bindings.len()];

        let mut projection_indexes = Vec::with_capacity(plan.aggregate_bindings.len() + 1);
        projection_indexes.push(filter_column_index);
        let filter_projection_index = 0;
        let mut aggregate_projection_indexes = Vec::with_capacity(plan.aggregate_bindings.len());
        for aggregate in &plan.aggregate_bindings {
            let Some(source_column_index) = aggregate.source_column_index else {
                aggregate_projection_indexes.push(None);
                continue;
            };
            let projection_index =
                push_projection_index(&mut projection_indexes, source_column_index);
            aggregate_projection_indexes.push(Some(projection_index));
        }

        visit_persisted_table_projected_values(store, state, &projection_indexes, |_, values| {
            if compare_values(&values[filter_projection_index], &filter_value)?
                != std::cmp::Ordering::Equal
            {
                return Ok(());
            }
            row_count += 1;
            for (aggregate_index, aggregate) in plan.aggregate_bindings.iter().enumerate() {
                if matches!(aggregate.kind, SimpleGroupedNumericAggregateKind::Sum) {
                    let source_projection_index = aggregate_projection_indexes[aggregate_index]
                        .ok_or_else(|| {
                            DbError::internal("simple scalar SUM missing source column")
                        })?;
                    numeric_states[aggregate_index].add(&values[source_projection_index])?;
                }
            }
            Ok(())
        })?;

        let values = plan
            .aggregate_bindings
            .iter()
            .enumerate()
            .map(|(aggregate_index, aggregate)| match aggregate.kind {
                SimpleGroupedNumericAggregateKind::CountRows => Ok(Value::Int64(row_count)),
                SimpleGroupedNumericAggregateKind::Sum => {
                    numeric_states[aggregate_index].value(aggregate.kind)
                }
                _ => Ok(Value::Null),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(QueryResult::with_rows(
            plan.column_names.clone(),
            vec![QueryRow::new(values)],
        )))
    }
    fn simple_grouped_numeric_aggregate_result_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        plan: &SimpleGroupedNumericAggregatePlan<'_>,
        params: &[Value],
    ) -> Result<QueryResult> {
        if let Some(result) =
            self.try_simple_scalar_int64_numeric_aggregate_from_persisted_state(store, state, plan)?
        {
            return Ok(result);
        }
        if let Some(result) = self
            .try_simple_scalar_filtered_numeric_aggregate_from_persisted_state(
                store, state, plan, params,
            )?
        {
            return Ok(result);
        }

        let mut groups = Vec::<SimpleGroupedNumericAggregate>::new();
        let mut group_positions = BTreeMap::<Vec<u8>, usize>::new();
        let group_dataset = Dataset::with_rows(plan.group_eval_bindings.clone(), Vec::new());
        if plan.group_exprs.is_empty() {
            groups.push(SimpleGroupedNumericAggregate::new(
                Vec::new(),
                plan.aggregate_bindings.len(),
            ));
            group_positions.insert(row_identity(&[])?, 0);
        }
        visit_persisted_table_rows(store, state, |_, values| {
            if let Some(filter_expr) = plan.filter_expr.as_ref() {
                if !matches!(
                    self.eval_expr(
                        filter_expr,
                        &group_dataset,
                        values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                    Value::Bool(true)
                ) {
                    return Ok(());
                }
            }

            let group_values = evaluate_simple_grouped_values(
                self,
                plan.group_exprs,
                &group_dataset,
                values,
                params,
            )?;
            let group_key = row_identity(&group_values)?;
            let group_index = if let Some(group_index) = group_positions.get(&group_key).copied() {
                group_index
            } else {
                groups.push(SimpleGroupedNumericAggregate::new(
                    group_values,
                    plan.aggregate_bindings.len(),
                ));
                let group_index = groups.len() - 1;
                group_positions.insert(group_key, group_index);
                group_index
            };
            groups[group_index].count += 1;
            for (aggregate_index, aggregate) in plan.aggregate_bindings.iter().enumerate() {
                match aggregate.kind {
                    SimpleGroupedNumericAggregateKind::CountNonNull => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            groups[group_index]
                                .count_non_null(aggregate_index, &values[source_column_index]);
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                values,
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            groups[group_index].count_non_null(aggregate_index, &value);
                        }
                    }
                    SimpleGroupedNumericAggregateKind::CountDistinct => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            groups[group_index]
                                .count_distinct(aggregate_index, &values[source_column_index])?;
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                values,
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            groups[group_index].count_distinct(aggregate_index, &value)?;
                        }
                    }
                    SimpleGroupedNumericAggregateKind::Sum
                    | SimpleGroupedNumericAggregateKind::SumDistinct
                    | SimpleGroupedNumericAggregateKind::Avg
                    | SimpleGroupedNumericAggregateKind::AvgDistinct
                    | SimpleGroupedNumericAggregateKind::Total
                    | SimpleGroupedNumericAggregateKind::TotalDistinct => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            if matches!(
                                aggregate.kind,
                                SimpleGroupedNumericAggregateKind::SumDistinct
                                    | SimpleGroupedNumericAggregateKind::AvgDistinct
                                    | SimpleGroupedNumericAggregateKind::TotalDistinct
                            ) {
                                groups[group_index].add_numeric_distinct(
                                    aggregate_index,
                                    &values[source_column_index],
                                )?;
                            } else {
                                groups[group_index]
                                    .add_numeric(aggregate_index, &values[source_column_index])?;
                            }
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                values,
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            if matches!(
                                aggregate.kind,
                                SimpleGroupedNumericAggregateKind::SumDistinct
                                    | SimpleGroupedNumericAggregateKind::AvgDistinct
                                    | SimpleGroupedNumericAggregateKind::TotalDistinct
                            ) {
                                groups[group_index]
                                    .add_numeric_distinct(aggregate_index, &value)?;
                            } else {
                                groups[group_index].add_numeric(aggregate_index, &value)?;
                            }
                        }
                    }
                    SimpleGroupedNumericAggregateKind::StddevSamp
                    | SimpleGroupedNumericAggregateKind::StddevSampDistinct
                    | SimpleGroupedNumericAggregateKind::StddevPop
                    | SimpleGroupedNumericAggregateKind::StddevPopDistinct
                    | SimpleGroupedNumericAggregateKind::VarSamp
                    | SimpleGroupedNumericAggregateKind::VarSampDistinct
                    | SimpleGroupedNumericAggregateKind::VarPop
                    | SimpleGroupedNumericAggregateKind::VarPopDistinct => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            if aggregate.kind.uses_distinct() {
                                groups[group_index].add_variance_distinct(
                                    aggregate_index,
                                    &values[source_column_index],
                                )?;
                            } else {
                                groups[group_index]
                                    .add_variance(aggregate_index, &values[source_column_index])?;
                            }
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                values,
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            if aggregate.kind.uses_distinct() {
                                groups[group_index]
                                    .add_variance_distinct(aggregate_index, &value)?;
                            } else {
                                groups[group_index].add_variance(aggregate_index, &value)?;
                            }
                        }
                    }
                    SimpleGroupedNumericAggregateKind::BoolAnd
                    | SimpleGroupedNumericAggregateKind::BoolAndDistinct
                    | SimpleGroupedNumericAggregateKind::BoolOr
                    | SimpleGroupedNumericAggregateKind::BoolOrDistinct => {
                        if let Some(source_column_index) = aggregate.source_column_index {
                            if aggregate.kind.uses_distinct() {
                                groups[group_index].add_bool_distinct(
                                    aggregate_index,
                                    &values[source_column_index],
                                )?;
                            } else {
                                groups[group_index]
                                    .add_bool(aggregate_index, &values[source_column_index])?;
                            }
                        } else if let Some(source_expr) = aggregate.source_expr.as_ref() {
                            let value = self.eval_expr(
                                source_expr,
                                &group_dataset,
                                values,
                                params,
                                &BTreeMap::new(),
                                None,
                            )?;
                            if aggregate.kind.uses_distinct() {
                                groups[group_index].add_bool_distinct(aggregate_index, &value)?;
                            } else {
                                groups[group_index].add_bool(aggregate_index, &value)?;
                            }
                        }
                    }
                    SimpleGroupedNumericAggregateKind::Min
                    | SimpleGroupedNumericAggregateKind::Max => {
                        let Some(source_expr) = aggregate.source_expr.as_ref() else {
                            continue;
                        };
                        let value = self.eval_expr(
                            source_expr,
                            &group_dataset,
                            values,
                            params,
                            &BTreeMap::new(),
                            None,
                        )?;
                        update_simple_min_max_value(
                            &mut groups[group_index].extreme_values[aggregate_index],
                            value,
                            matches!(aggregate.kind, SimpleGroupedNumericAggregateKind::Max),
                        )?;
                    }
                    SimpleGroupedNumericAggregateKind::CountRows => {}
                }
            }
            Ok(())
        })?;

        render_simple_grouped_numeric_aggregate_groups(self, groups, plan, params)
    }
    fn try_simple_scalar_int64_numeric_aggregate_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        plan: &SimpleGroupedNumericAggregatePlan<'_>,
    ) -> Result<Option<QueryResult>> {
        let Some(column_index) = self.simple_scalar_int64_aggregate_column(plan) else {
            return Ok(None);
        };
        let mut stats = SimpleScalarInt64AggregateStats::default();
        visit_persisted_table_int64_column(store, state, column_index, |_, value| {
            stats.add(value);
            Ok(())
        })?;
        Ok(Some(QueryResult::with_rows(
            plan.column_names.clone(),
            vec![QueryRow::new(stats.into_values(&plan.aggregate_bindings))],
        )))
    }
    fn try_execute_simple_deferred_paged_grouped_count_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_grouped_count_query(query, params)? else {
            return Ok(None);
        };
        if self.visible_table_is_temporary(plan.table_name)
            || self.visible_table_row_source(plan.table_name).is_some()
        {
            return Ok(None);
        }
        if let Some(result) =
            self.try_simple_grouped_count_result_from_runtime_index(None, &plan, params)?
        {
            return Ok(Some(result));
        }
        let Some(state) = self.persisted_table_state(plan.table_name) else {
            return Ok(None);
        };
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        Ok(Some(
            self.simple_grouped_count_result_from_persisted_state(&store, state, &plan, params)?,
        ))
    }
    pub(crate) fn try_execute_simple_deferred_paged_grouped_numeric_aggregate_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_grouped_numeric_aggregate_query(query, params)? else {
            return Ok(None);
        };
        if self.visible_table_is_temporary(plan.table_name)
            || self.visible_table_row_source(plan.table_name).is_some()
        {
            return Ok(None);
        }
        let Some(state) = self.persisted_table_state(plan.table_name) else {
            return Ok(None);
        };
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        Ok(Some(
            self.simple_grouped_numeric_aggregate_result_from_persisted_state(
                &store, state, &plan, params,
            )?,
        ))
    }
    pub(crate) fn try_execute_simple_view_projection_limit_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if query.recursive || !query.ctes.is_empty() {
            return Ok(None);
        }
        let Some(limit_expr) = query.limit.as_ref() else {
            return Ok(None);
        };
        let ctes = BTreeMap::new();
        let limit_value = match simple_int64_constant_expr_value(limit_expr, params)? {
            Some(value) => value,
            None => self.eval_constant_i64(limit_expr, params, &ctes)?,
        };
        let limit = usize::try_from(limit_value.max(0)).unwrap_or(usize::MAX);
        let offset = query
            .offset
            .as_ref()
            .map(|expr| {
                simple_int64_constant_expr_value(expr, params)?
                    .map(Ok)
                    .unwrap_or_else(|| self.eval_constant_i64(expr, params, &ctes))
            })
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || projection_has_aggregate_items(&select.projection)
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        let Some(view) = self.visible_view(name, NameResolutionScope::Session) else {
            return Ok(None);
        };
        let view_binding = alias.as_deref().unwrap_or(name.as_str());
        let view_query = self.cached_view_query(view)?;
        if view_query.recursive
            || !view_query.ctes.is_empty()
            || !view_query.order_by.is_empty()
            || view_query.limit.is_some()
            || view_query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(view_select) = &view_query.body else {
            return Ok(None);
        };
        if view_select.distinct
            || !view_select.distinct_on.is_empty()
            || !view_select.group_by.is_empty()
            || view_select.having.is_some()
            || projection_has_aggregate_items(&view_select.projection)
        {
            return Ok(None);
        }
        if !query.order_by.is_empty() {
            return if view.temporary {
                self.try_execute_ordered_view_projection_limit_select(
                    select,
                    view_select,
                    &view.name,
                    &view.column_names,
                    view_binding,
                    &query.order_by,
                    limit,
                    offset,
                    params,
                )
            } else {
                let persistent_runtime = self.persistent_resolution_runtime();
                persistent_runtime.try_execute_ordered_view_projection_limit_select(
                    select,
                    view_select,
                    &view.name,
                    &view.column_names,
                    view_binding,
                    &query.order_by,
                    limit,
                    offset,
                    params,
                )
            };
        }
        if view_select.filter.is_some() {
            return Ok(None);
        }

        let mut pushed_projection = Vec::with_capacity(select.projection.len());
        for (index, item) in select.projection.iter().enumerate() {
            let SelectItem::Expr { expr, alias } = item else {
                return Ok(None);
            };
            let Expr::Column { table, column } = expr else {
                return Ok(None);
            };
            if table
                .as_deref()
                .is_some_and(|qualifier| !identifiers_equal(qualifier, view_binding))
            {
                return Ok(None);
            }
            let Some(view_expr) =
                view_projection_expr_for_output_column(&view_select.projection, column)
            else {
                return Ok(None);
            };
            pushed_projection.push(SelectItem::Expr {
                expr: view_expr,
                alias: Some(
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                ),
            });
        }

        if view.temporary {
            self.try_execute_indexed_join_limit_projection_select(
                view_select,
                &pushed_projection,
                limit,
                offset,
            )
        } else {
            let persistent_runtime = self.persistent_resolution_runtime();
            persistent_runtime.try_execute_indexed_join_limit_projection_select(
                view_select,
                &pushed_projection,
                limit,
                offset,
            )
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn try_execute_ordered_view_projection_limit_select(
        &self,
        outer_select: &Select,
        view_select: &Select,
        view_name: &str,
        view_column_names: &[String],
        view_binding: &str,
        order_by: &[OrderBy],
        limit: usize,
        offset: usize,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if order_by.len() != 1 || order_by[0].collation.is_some() {
            return Ok(None);
        }
        let Some(pushed_projection) = pushed_view_projection_for_outer_projection(
            &outer_select.projection,
            view_select,
            view_name,
            view_binding,
            view_column_names,
        ) else {
            return Ok(None);
        };

        let mut join_select = view_select.clone();
        join_select.filter = None;
        let Some(plan) = self.analyze_indexed_join_limit_projection_select(
            &join_select,
            &pushed_projection,
            limit,
            offset,
        )?
        else {
            return Ok(None);
        };

        let Expr::Column {
            table: order_table,
            column: order_column,
        } = &order_by[0].expr
        else {
            return Ok(None);
        };
        if order_table.as_deref().is_some_and(|qualifier| {
            !identifiers_equal(qualifier, view_binding) && !identifiers_equal(qualifier, view_name)
        }) {
            return Ok(None);
        }
        let Some(order_expr) = view_projection_expr_for_output_column_with_names(
            &view_select.projection,
            view_column_names,
            order_column,
        ) else {
            return Ok(None);
        };
        let Some((order_table_index, order_column_index)) =
            indexed_join_limit_projection_column(&order_expr, &plan.tables, self)
        else {
            return Ok(None);
        };
        if order_table_index != 0 {
            return Ok(None);
        }

        let root_table = plan.tables[0];
        let root_binding = root_table.alias.as_deref().unwrap_or(root_table.name);
        let Some(root_schema) = self.table_schema(root_table.name) else {
            return Ok(None);
        };
        let Some(order_column_schema) = root_schema.columns.get(order_column_index) else {
            return Ok(None);
        };

        let root_filter_columns = if let Some(filter) = view_select.filter.as_ref() {
            let Some(root_columns) = indexed_join_table_eval_columns(&plan.tables[..1], self)
            else {
                return Ok(None);
            };
            let Some(join_columns) = indexed_join_table_eval_columns(&plan.tables, self) else {
                return Ok(None);
            };
            let root_dataset = Dataset::with_rows(root_columns.clone(), Vec::new());
            let join_dataset = Dataset::with_rows(join_columns, Vec::new());
            if !expr_resolves_against_dataset(filter, &root_dataset)
                || !expr_resolves_against_dataset(filter, &join_dataset)
            {
                return Ok(None);
            }
            Some(root_columns)
        } else {
            None
        };

        let Some(index) = self.ordered_view_root_btree_index(
            root_table.name,
            &order_column_schema.name,
            view_select.filter.as_ref(),
            root_binding,
        )?
        else {
            return Ok(None);
        };

        self.execute_ordered_indexed_join_limit_projection_plan(
            &plan,
            view_select.filter.as_ref(),
            root_filter_columns,
            &index.name,
            order_by[0].descending,
            params,
        )
        .map(Some)
    }
    pub(crate) fn try_execute_simple_indexed_join_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || projection_has_aggregate_items(&select.projection)
            || select.from.len() != 1
        {
            return Ok(None);
        }
        if !select.distinct_on.is_empty() {
            return Ok(None);
        }
        let FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        if !matches!(
            kind,
            JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Full
        ) {
            return Ok(None);
        }
        let is_left_outer = matches!(kind, JoinKind::Left | JoinKind::Full);
        let is_right_outer = matches!(kind, JoinKind::Right | JoinKind::Full);
        let (left_name, left_alias) = match &**left {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        let (right_name, right_alias) = match &**right {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        if self
            .visible_view(left_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(right_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(left_name)
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }

        let left_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        let left_schema = match self.table_schema(left_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let right_schema = match self.table_schema(right_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(left_schema) || !generated_columns_are_stored(right_schema)
        {
            return Ok(None);
        }
        let Some(join_equalities) = simple_indexed_join_constraint_equalities(
            constraint,
            left_binding,
            right_binding,
            left_schema,
            right_schema,
        ) else {
            return Ok(None);
        };
        let Some((left_join_columns, right_join_columns)) =
            orient_join_equalities(&join_equalities, left_binding, right_binding)
        else {
            return Ok(None);
        };
        let using_join_columns =
            simple_indexed_join_using_columns(constraint, left_schema, right_schema);

        let left_source = match self.visible_table_row_source(left_name) {
            Some(source) => source,
            None => return Ok(None),
        };
        let right_source = match self.visible_table_row_source(right_name) {
            Some(source) => source,
            None => return Ok(None),
        };
        let join_eval_bindings = simple_join_projection_eval_bindings(
            left_name,
            left_alias,
            left_schema,
            right_name,
            right_alias,
            right_schema,
        );
        let join_eval_dataset = Dataset::with_rows(join_eval_bindings, Vec::new());
        let default_source_is_left = if matches!(kind, JoinKind::Full) {
            true
        } else {
            !is_right_outer
        };
        let (source_is_left, source_filter, post_join_filter) = if let Some(filter) =
            select.filter.as_ref()
        {
            if let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) {
                if matches_table_binding(left_binding, filter_table) && !is_right_outer {
                    (true, Some((filter_column, value_expr)), None)
                } else if !is_left_outer && matches_table_binding(right_binding, filter_table) {
                    (false, Some((filter_column, value_expr)), None)
                } else if !expr_contains_recursive_unsupported_feature(filter)
                    && expr_resolves_against_dataset(filter, &join_eval_dataset)
                {
                    (default_source_is_left, None, Some(filter.clone()))
                } else {
                    return Ok(None);
                }
            } else if !expr_contains_recursive_unsupported_feature(filter)
                && expr_resolves_against_dataset(filter, &join_eval_dataset)
            {
                (default_source_is_left, None, Some(filter.clone()))
            } else {
                return Ok(None);
            }
        } else {
            (default_source_is_left, None, None)
        };
        let Some((projection_plan, column_names)) = simple_join_projection_plan(
            &select.projection,
            &join_eval_dataset,
            left_name,
            left_alias,
            left_schema,
            right_name,
            right_alias,
            right_schema,
            &using_join_columns,
        ) else {
            return Ok(None);
        };
        let order_by = simple_join_projection_order_by_plan(
            query,
            &select.projection,
            &projection_plan,
            &column_names,
            left_name,
            left_alias,
            left_schema,
            right_name,
            right_alias,
            right_schema,
        )?;

        let (
            source_table,
            source_schema,
            source_join_columns,
            source_source,
            probe_table,
            probe_schema,
            probe_join_columns,
            probe_source,
        ) = if source_is_left {
            (
                left_name,
                left_schema,
                left_join_columns,
                left_source,
                right_name,
                right_schema,
                right_join_columns,
                right_source,
            )
        } else {
            (
                right_name,
                right_schema,
                right_join_columns,
                right_source,
                left_name,
                left_schema,
                left_join_columns,
                left_source,
            )
        };
        let mut source_join_indexes = Vec::with_capacity(source_join_columns.len());
        for source_join_column in &source_join_columns {
            let source_join_index = source_schema
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, source_join_column))
                .ok_or_else(|| DbError::sql(format!("unknown column {source_join_column}")))?;
            source_join_indexes.push(source_join_index);
        }
        let source_row_ids = if let Some((filter_column, value_expr)) = source_filter {
            let filter_value = self.eval_expr(
                value_expr,
                &Dataset::empty(),
                &[],
                params,
                &BTreeMap::new(),
                None,
            )?;
            let filter_value =
                coerce_lookup_literal(source_schema, filter_column, value_expr, filter_value)?;
            if !timestamp_lookup_matches_schema(source_schema, filter_column, &filter_value) {
                return Ok(None);
            }
            if crate::exec::dml::row_id_alias_column_name(source_schema)
                .is_some_and(|name| identifiers_equal(name, filter_column))
            {
                Some(match filter_value {
                    Value::Int64(row_id) => RuntimeRowIdSet::Single(row_id),
                    _ => RuntimeRowIdSet::Empty,
                })
            } else {
                let Some(filter_index) = self.catalog.indexes.values().find(|index| {
                    identifiers_equal(&index.table_name, source_table)
                        && index.fresh
                        && index.kind == IndexKind::Btree
                        && index.predicate_sql.is_none()
                        && index.columns.len() == 1
                        && index.columns[0]
                            .column_name
                            .as_deref()
                            .is_some_and(|index_column| {
                                identifiers_equal(index_column, filter_column)
                            })
                        && index.columns[0].expression_sql.is_none()
                }) else {
                    return Ok(None);
                };
                let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&filter_index.name) else {
                    return Ok(None);
                };
                Some(keys.row_ids_for_value_set(&filter_value)?)
            }
        } else {
            None
        };

        let is_probe_rowid_alias = probe_join_columns.len() == 1
            && crate::exec::dml::row_id_alias_column_name(probe_schema)
                .is_some_and(|name| identifiers_equal(name, probe_join_columns[0]));
        let (probe_index, ordered_source_join_indexes) = if is_probe_rowid_alias {
            (None, source_join_indexes)
        } else {
            match self.catalog.indexes.values().find_map(|index| {
                if !identifiers_equal(&index.table_name, probe_table)
                    || !index.fresh
                    || index.kind != IndexKind::Btree
                    || index.predicate_sql.is_some()
                    || index.columns.len() != probe_join_columns.len()
                {
                    return None;
                }
                let mut ordered_source_join_indexes = Vec::with_capacity(index.columns.len());
                for index_column in &index.columns {
                    if index_column.expression_sql.is_some() {
                        return None;
                    }
                    let index_column_name = index_column.column_name.as_deref()?;
                    let join_position = probe_join_columns.iter().position(|join_column| {
                        identifiers_equal(join_column, index_column_name)
                    })?;
                    let probe_column_index = schema_column_index(probe_schema, index_column_name)?;
                    if !timestamp_key_types_match(
                        source_schema.columns[source_join_indexes[join_position]].column_type,
                        probe_schema.columns[probe_column_index].column_type,
                    ) {
                        return None;
                    }
                    ordered_source_join_indexes.push(source_join_indexes[join_position]);
                }
                Some((index, ordered_source_join_indexes))
            }) {
                Some((probe_index, ordered_source_join_indexes)) => {
                    (Some(probe_index), ordered_source_join_indexes)
                }
                None => (None, source_join_indexes),
            }
        };
        let keys = if let Some(index) = probe_index {
            let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
                return Ok(None);
            };
            Some(keys)
        } else {
            None
        };
        let probe_join_indexes = if probe_index.is_none() && !is_probe_rowid_alias {
            let mut probe_join_indexes = Vec::with_capacity(probe_join_columns.len());
            for probe_join_column in &probe_join_columns {
                let Some(probe_join_index) = probe_schema
                    .columns
                    .iter()
                    .position(|column| identifiers_equal(&column.name, probe_join_column))
                else {
                    return Ok(None);
                };
                probe_join_indexes.push(probe_join_index);
            }
            Some(probe_join_indexes)
        } else {
            None
        };

        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let early_stop_limit = if order_by.is_none() && !select.distinct {
            limit.map(|limit| limit.saturating_add(offset))
        } else {
            None
        };

        let use_probe_row_position_map = probe_source
            .row_count()
            .saturating_mul(source_source.row_count())
            > 8_192;
        let probe_row_positions = if use_probe_row_position_map {
            let mut positions = Int64Map::<usize>::default();
            for (position, row) in probe_source.rows().enumerate() {
                positions.insert(row?.row_id(), position);
            }
            Some(positions)
        } else {
            None
        };
        let probe_hash_rows = if let Some(probe_join_indexes) = probe_join_indexes.as_ref() {
            let mut hashed = SimpleJoinHashRows::new();
            for probe_row in probe_source.rows() {
                let probe_row = probe_row?;
                let Some(join_key) =
                    simple_join_key_from_indexes(probe_row.values(), probe_join_indexes)?
                else {
                    continue;
                };
                hashed
                    .entry(join_key)
                    .or_default()
                    .push((probe_row.row_id(), probe_row.values().to_vec()));
            }
            Some(hashed)
        } else {
            None
        };

        let mut rows = Vec::new();
        let mut stop = false;
        let left_width = left_schema.columns.len();
        let right_width = right_schema.columns.len();
        let mut matched_probe_row_ids =
            matches!(kind, JoinKind::Full).then(Int64Map::<()>::default);
        if let Some(source_row_ids) = source_row_ids {
            match source_row_ids {
                RuntimeRowIdSet::Empty => {}
                RuntimeRowIdSet::Single(source_row_id) => {
                    if let Some(source_row) = source_source.row_by_id(source_row_id)? {
                        let _ = self.process_simple_indexed_join_source_row(
                            *kind,
                            source_is_left,
                            source_row.values(),
                            &ordered_source_join_indexes,
                            probe_source,
                            probe_row_positions.as_ref(),
                            keys,
                            probe_hash_rows.as_ref(),
                            matched_probe_row_ids.as_mut(),
                            post_join_filter.as_ref(),
                            &projection_plan,
                            &join_eval_dataset,
                            left_width,
                            right_width,
                            params,
                            early_stop_limit,
                            &mut rows,
                        )?;
                    }
                }
                RuntimeRowIdSet::Contiguous { start, len } => {
                    for source_row_id in contiguous_row_ids(start, len) {
                        if stop {
                            break;
                        }
                        let Some(source_row) = source_source.row_by_id(source_row_id)? else {
                            continue;
                        };
                        stop = self.process_simple_indexed_join_source_row(
                            *kind,
                            source_is_left,
                            source_row.values(),
                            &ordered_source_join_indexes,
                            probe_source,
                            probe_row_positions.as_ref(),
                            keys,
                            probe_hash_rows.as_ref(),
                            matched_probe_row_ids.as_mut(),
                            post_join_filter.as_ref(),
                            &projection_plan,
                            &join_eval_dataset,
                            left_width,
                            right_width,
                            params,
                            early_stop_limit,
                            &mut rows,
                        )?;
                    }
                }
                RuntimeRowIdSet::Many(source_row_ids) => {
                    for source_row_id in source_row_ids {
                        if stop {
                            break;
                        }
                        let Some(source_row) = source_source.row_by_id(*source_row_id)? else {
                            continue;
                        };
                        stop = self.process_simple_indexed_join_source_row(
                            *kind,
                            source_is_left,
                            source_row.values(),
                            &ordered_source_join_indexes,
                            probe_source,
                            probe_row_positions.as_ref(),
                            keys,
                            probe_hash_rows.as_ref(),
                            matched_probe_row_ids.as_mut(),
                            post_join_filter.as_ref(),
                            &projection_plan,
                            &join_eval_dataset,
                            left_width,
                            right_width,
                            params,
                            early_stop_limit,
                            &mut rows,
                        )?;
                    }
                }
                RuntimeRowIdSet::Owned(source_row_ids) => {
                    for source_row_id in source_row_ids {
                        if stop {
                            break;
                        }
                        let Some(source_row) = source_source.row_by_id(source_row_id)? else {
                            continue;
                        };
                        stop = self.process_simple_indexed_join_source_row(
                            *kind,
                            source_is_left,
                            source_row.values(),
                            &ordered_source_join_indexes,
                            probe_source,
                            probe_row_positions.as_ref(),
                            keys,
                            probe_hash_rows.as_ref(),
                            matched_probe_row_ids.as_mut(),
                            post_join_filter.as_ref(),
                            &projection_plan,
                            &join_eval_dataset,
                            left_width,
                            right_width,
                            params,
                            early_stop_limit,
                            &mut rows,
                        )?;
                    }
                }
            }
        } else {
            for source_row in source_source.rows() {
                let source_row = source_row?;
                stop = self.process_simple_indexed_join_source_row(
                    *kind,
                    source_is_left,
                    source_row.values(),
                    &ordered_source_join_indexes,
                    probe_source,
                    probe_row_positions.as_ref(),
                    keys,
                    probe_hash_rows.as_ref(),
                    matched_probe_row_ids.as_mut(),
                    post_join_filter.as_ref(),
                    &projection_plan,
                    &join_eval_dataset,
                    left_width,
                    right_width,
                    params,
                    early_stop_limit,
                    &mut rows,
                )?;
                if stop {
                    break;
                }
            }
        }
        if matches!(kind, JoinKind::Full) && source_is_left && !stop {
            if let Some(matched_probe_row_ids) = matched_probe_row_ids.as_ref() {
                for probe_row in probe_source.rows() {
                    let probe_row = probe_row?;
                    if matched_probe_row_ids.contains_key(&probe_row.row_id()) {
                        continue;
                    }
                    if !simple_join_filter_matches(
                        self,
                        post_join_filter.as_ref(),
                        &join_eval_dataset,
                        None,
                        left_width,
                        Some(probe_row.values()),
                        right_width,
                        params,
                    )? {
                        continue;
                    }
                    rows.push(project_simple_join_row(
                        self,
                        &projection_plan,
                        &join_eval_dataset,
                        None,
                        left_width,
                        Some(probe_row.values()),
                        right_width,
                        params,
                    )?);
                    if early_stop_limit.is_some_and(|limit| rows.len() >= limit) {
                        break;
                    }
                }
            }
        }
        let mut rows = rows.into_iter().map(QueryRow::new).collect::<Vec<_>>();
        if select.distinct {
            rows = dedup_query_rows(rows)?;
        }
        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by.as_deref(),
            limit,
            offset,
        )?))
    }
    #[allow(clippy::too_many_arguments)]
    fn process_simple_indexed_join_source_row(
        &self,
        kind: JoinKind,
        source_is_left: bool,
        source_values: &[Value],
        source_join_indexes: &[usize],
        probe_source: VisibleTableRowSource<'_>,
        probe_row_positions: Option<&Int64Map<usize>>,
        probe_keys: Option<&RuntimeBtreeKeys>,
        probe_hash_rows: Option<&SimpleJoinHashRows>,
        mut matched_probe_row_ids: Option<&mut Int64Map<()>>,
        post_join_filter: Option<&Expr>,
        projection_plan: &[SimpleJoinProjectionSource],
        join_eval_dataset: &Dataset,
        left_width: usize,
        right_width: usize,
        params: &[Value],
        early_stop_limit: Option<usize>,
        rows: &mut Vec<Vec<Value>>,
    ) -> Result<bool> {
        let join_values = source_join_indexes
            .iter()
            .map(|source_join_index| {
                source_values
                    .get(*source_join_index)
                    .ok_or_else(|| DbError::internal("join row is shorter than table schema"))
            })
            .collect::<Result<Vec<_>>>()?;
        if join_values
            .iter()
            .any(|join_value| matches!(join_value, Value::Null))
        {
            if matches!(kind, JoinKind::Inner) {
                return Ok(false);
            }
            if !simple_join_filter_matches(
                self,
                post_join_filter,
                join_eval_dataset,
                if source_is_left {
                    Some(source_values)
                } else {
                    None
                },
                left_width,
                if source_is_left {
                    None
                } else {
                    Some(source_values)
                },
                right_width,
                params,
            )? {
                return Ok(false);
            }
            rows.push(project_simple_join_row(
                self,
                projection_plan,
                join_eval_dataset,
                if source_is_left {
                    Some(source_values)
                } else {
                    None
                },
                left_width,
                if source_is_left {
                    None
                } else {
                    Some(source_values)
                },
                right_width,
                params,
            )?);
            return Ok(early_stop_limit.is_some_and(|limit| rows.len() >= limit));
        }

        let join_key = Row::new(
            join_values
                .iter()
                .map(|value| equality_key_value(value))
                .collect(),
        )
        .encode()?;
        let probe_row_ids = if let Some(keys) = probe_keys {
            if join_values.len() == 1 {
                keys.row_ids_for_value_set(join_values[0])?
            } else {
                keys.row_id_set_for_key(&RuntimeBtreeKey::Encoded(RuntimeEncodedKey::from_vec(
                    Row::new(join_values.into_iter().cloned().collect()).encode()?,
                )))
            }
        } else if join_values.len() == 1 {
            let join_value = join_values[0];
            if let Value::Int64(val) = join_value {
                RuntimeRowIdSet::Single(*val)
            } else {
                RuntimeRowIdSet::Empty
            }
        } else {
            RuntimeRowIdSet::Empty
        };
        let rows_before = rows.len();
        let mut should_stop = false;
        let mut row_error = None;

        if let Some(probe_hash_rows) = probe_hash_rows {
            if let Some(matching_probe_rows) = probe_hash_rows.get(&join_key) {
                for (row_id, probe_row) in matching_probe_rows {
                    match simple_join_filter_matches(
                        self,
                        post_join_filter,
                        join_eval_dataset,
                        if source_is_left {
                            Some(source_values)
                        } else {
                            Some(probe_row.as_slice())
                        },
                        left_width,
                        if source_is_left {
                            Some(probe_row.as_slice())
                        } else {
                            Some(source_values)
                        },
                        right_width,
                        params,
                    ) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(error) => {
                            row_error = Some(error);
                            break;
                        }
                    }
                    match project_simple_join_row(
                        self,
                        projection_plan,
                        join_eval_dataset,
                        if source_is_left {
                            Some(source_values)
                        } else {
                            Some(probe_row.as_slice())
                        },
                        left_width,
                        if source_is_left {
                            Some(probe_row.as_slice())
                        } else {
                            Some(source_values)
                        },
                        right_width,
                        params,
                    ) {
                        Ok(projected) => {
                            if let Some(matched_probe_row_ids) = matched_probe_row_ids.as_mut() {
                                matched_probe_row_ids.insert(*row_id, ());
                            }
                            rows.push(projected);
                        }
                        Err(error) => {
                            row_error = Some(error);
                            break;
                        }
                    }
                    if early_stop_limit.is_some_and(|limit| rows.len() >= limit) {
                        should_stop = true;
                        break;
                    }
                }
            }
            if let Some(error) = row_error {
                return Err(error);
            }
            if !should_stop && !matches!(kind, JoinKind::Inner) && rows.len() == rows_before {
                if !simple_join_filter_matches(
                    self,
                    post_join_filter,
                    join_eval_dataset,
                    if source_is_left {
                        Some(source_values)
                    } else {
                        None
                    },
                    left_width,
                    if source_is_left {
                        None
                    } else {
                        Some(source_values)
                    },
                    right_width,
                    params,
                )? {
                    return Ok(false);
                }
                rows.push(project_simple_join_row(
                    self,
                    projection_plan,
                    join_eval_dataset,
                    if source_is_left {
                        Some(source_values)
                    } else {
                        None
                    },
                    left_width,
                    if source_is_left {
                        None
                    } else {
                        Some(source_values)
                    },
                    right_width,
                    params,
                )?);
                should_stop = early_stop_limit.is_some_and(|limit| rows.len() >= limit);
            }
            return Ok(should_stop);
        }

        probe_row_ids.for_each(|row_id| {
            if should_stop || row_error.is_some() {
                return;
            }
            let probe_row = if let Some(positions) = probe_row_positions {
                let Some(probe_position) = positions.get(&row_id).copied() else {
                    return;
                };
                match probe_source.row_at_position(probe_position) {
                    Ok(Some(probe_row)) => probe_row.values().to_vec(),
                    Ok(None) => return,
                    Err(error) => {
                        row_error = Some(error);
                        return;
                    }
                }
            } else {
                match probe_source.row_by_id(row_id) {
                    Ok(Some(probe_row)) => probe_row.values().to_vec(),
                    Ok(None) => return,
                    Err(error) => {
                        row_error = Some(error);
                        return;
                    }
                }
            };

            match simple_join_filter_matches(
                self,
                post_join_filter,
                join_eval_dataset,
                if source_is_left {
                    Some(source_values)
                } else {
                    Some(&probe_row)
                },
                left_width,
                if source_is_left {
                    Some(&probe_row)
                } else {
                    Some(source_values)
                },
                right_width,
                params,
            ) {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => {
                    row_error = Some(error);
                    return;
                }
            }
            match project_simple_join_row(
                self,
                projection_plan,
                join_eval_dataset,
                if source_is_left {
                    Some(source_values)
                } else {
                    Some(&probe_row)
                },
                left_width,
                if source_is_left {
                    Some(&probe_row)
                } else {
                    Some(source_values)
                },
                right_width,
                params,
            ) {
                Ok(projected) => {
                    if let Some(matched_probe_row_ids) = matched_probe_row_ids.as_mut() {
                        matched_probe_row_ids.insert(row_id, ());
                    }
                    rows.push(projected)
                }
                Err(error) => {
                    row_error = Some(error);
                    return;
                }
            }
            if early_stop_limit.is_some_and(|limit| rows.len() >= limit) {
                should_stop = true;
            }
        });
        if let Some(error) = row_error {
            return Err(error);
        }
        if !should_stop && !matches!(kind, JoinKind::Inner) && rows.len() == rows_before {
            if !simple_join_filter_matches(
                self,
                post_join_filter,
                join_eval_dataset,
                if source_is_left {
                    Some(source_values)
                } else {
                    None
                },
                left_width,
                if source_is_left {
                    None
                } else {
                    Some(source_values)
                },
                right_width,
                params,
            )? {
                return Ok(false);
            }
            rows.push(project_simple_join_row(
                self,
                projection_plan,
                join_eval_dataset,
                if source_is_left {
                    Some(source_values)
                } else {
                    None
                },
                left_width,
                if source_is_left {
                    None
                } else {
                    Some(source_values)
                },
                right_width,
                params,
            )?);
            should_stop = early_stop_limit.is_some_and(|limit| rows.len() >= limit);
        }
        Ok(should_stop)
    }
    pub(crate) fn single_column_btree_keys(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Option<&RuntimeBtreeKeys> {
        let index = self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, table_name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|index_column| identifiers_equal(index_column, column_name))
                && index.columns[0].expression_sql.is_none()
        })?;
        let RuntimeIndex::Btree { keys, .. } = self.index(&index.name)? else {
            return None;
        };
        Some(keys)
    }
    pub(crate) fn try_execute_simple_table_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let order_by = self.simple_projection_order_by_plan(
            query,
            table_schema,
            name,
            alias.as_deref().unwrap_or(name),
            &projection_indexes,
        )?;
        let row_id_order = if query.order_by.len() == 1 {
            if let Expr::Column {
                table: order_table,
                column: order_column,
            } = &query.order_by[0].expr
            {
                if order_table.as_deref().is_some_and(|qualifier| {
                    !matches_table_binding(TableBindingRef { name, alias }, Some(qualifier))
                }) {
                    None
                } else if let Some(filter_column_index) =
                    schema_column_index(table_schema, order_column)
                {
                    if table_schema
                        .primary_key_columns
                        .iter()
                        .any(|column| identifiers_equal(column, order_column))
                        && table_schema.columns[filter_column_index].column_type
                            == crate::catalog::ColumnType::Int64
                    {
                        Some((order_column.as_str(), query.order_by[0].descending))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if !query.order_by.is_empty() && order_by.is_none() && row_id_order.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let _row_source = self.visible_table_row_source(name);
        let Some(row_source) = _row_source else {
            return Ok(None);
        };
        if let Some((filter_column, descending)) = row_id_order {
            if limit != Some(0) {
                if let Some(row_ids) = self.ordered_runtime_btree_row_ids(
                    name,
                    filter_column,
                    limit,
                    offset,
                    descending,
                )? {
                    let mut rows = Vec::with_capacity(row_ids.len().min(64));
                    for row_id in row_ids {
                        if let Some(row) =
                            row_source.projected_query_row_by_id(row_id, &projection_indexes)?
                        {
                            rows.push(row);
                        }
                    }
                    return Ok(Some(QueryResult::with_rows(column_names, rows)));
                }
                let mut ordered_row_ids = Vec::with_capacity(row_source.row_count());
                for stored_row in row_source.rows() {
                    ordered_row_ids.push(stored_row?.row_id());
                }
                ordered_row_ids.sort_unstable();
                if descending {
                    ordered_row_ids.reverse();
                }
                let take = limit.unwrap_or(usize::MAX);
                let mut rows = Vec::with_capacity(take.min(ordered_row_ids.len()));
                for row_id in ordered_row_ids.into_iter().skip(offset).take(take) {
                    if let Some(row) =
                        row_source.projected_query_row_by_id(row_id, &projection_indexes)?
                    {
                        rows.push(row);
                    }
                }
                return Ok(Some(QueryResult::with_rows(column_names, rows)));
            }
        }

        Ok(Some(self.simple_projection_result_from_source(
            row_source,
            &projection_indexes,
            column_names,
            order_by,
            limit,
            offset,
        )?))
    }
    pub(crate) fn try_execute_simple_filtered_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        if select_requires_grouped_evaluation(self, select)? {
            return Ok(None);
        }
        if select.distinct
            && (!query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some())
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let binding_name = alias.as_deref().unwrap_or(name);
        let order_by = self.simple_projection_order_by_plan(
            query,
            table_schema,
            name,
            binding_name,
            &projection_indexes,
        )?;
        let order_by_requires_rowid_range = !query.order_by.is_empty() && order_by.is_none();
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let _row_source = self.visible_table_row_source(name);
        if !select.distinct {
            if let Some(row_source) = _row_source {
                if !order_by_requires_rowid_range {
                    if let Some(result) = self.try_simple_filtered_projection_exact_index_result(
                        row_source,
                        name,
                        table_schema,
                        filter,
                        &projection_indexes,
                        column_names.clone(),
                        order_by.as_deref(),
                        params,
                        limit,
                        offset,
                    )? {
                        return Ok(Some(result));
                    }
                }
            }
        }
        let Some(row_source) = _row_source else {
            return Ok(None);
        };

        if !select.distinct
            && query.order_by.is_empty()
            && limit.is_none()
            && offset == 0
            && residual_like_filter_can_use_direct_scan(filter)
        {
            if let Some((filter_table, filter_column, literal)) =
                simple_contains_like_projection_filter(filter)
            {
                if filter_table.is_none_or(|table_name| {
                    identifiers_equal(table_name, name)
                        || identifiers_equal(table_name, binding_name)
                }) {
                    let filter_column_index = table_schema
                        .columns
                        .iter()
                        .position(|candidate| identifiers_equal(&candidate.name, filter_column))
                        .ok_or_else(|| {
                            DbError::internal(format!(
                                "simple LIKE projection column {filter_column} missing from {name}"
                            ))
                        })?;
                    return Ok(Some(
                        self.simple_contains_like_projection_result_from_source(
                            row_source,
                            filter_column_index,
                            literal,
                            &projection_indexes,
                            column_names,
                        )?,
                    ));
                }
            }
        }

        let Some(range_filter) = simple_range_projection_filter(filter) else {
            return Ok(None);
        };
        let filter_table = range_filter.table;
        let filter_column = range_filter.column;
        let lower_bound = range_filter.lower;
        let upper_bound = range_filter.upper;
        if let Some(table_name) = filter_table {
            if !identifiers_equal(table_name, name) && !identifiers_equal(table_name, binding_name)
            {
                return Ok(None);
            }
        }
        let filter_column_index = table_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, filter_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "simple filtered projection column {filter_column} missing from {name}"
                ))
            })?;

        let lower_bound = lower_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;
        let upper_bound = upper_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;

        let residual_plans = self.build_simple_residual_plans(
            table_schema,
            name,
            binding_name,
            &range_filter.residual,
            params,
        )?;
        // If a residual predicate references an unknown/external table the
        // builder silently stops; bail to the generic executor in that case.
        if residual_plans.len() != range_filter.residual.len() {
            return Ok(None);
        }
        if !select.distinct && residual_plans.is_empty() {
            if let Some(result) = self.try_simple_rowid_range_projection_result(
                row_source,
                table_schema,
                TableBindingRef { name, alias },
                filter_column,
                lower_bound.as_ref(),
                upper_bound.as_ref(),
                &projection_indexes,
                column_names.clone(),
                &query.order_by,
                limit,
                offset,
            )? {
                return Ok(Some(result));
            }
        }
        if order_by_requires_rowid_range {
            return Ok(None);
        }
        if order_by.is_none() {
            if let Some(result) = self.try_simple_filtered_projection_range_index_result(
                row_source,
                name,
                table_schema,
                filter_column_index,
                filter_column,
                lower_bound.as_ref(),
                upper_bound.as_ref(),
                &residual_plans,
                &projection_indexes,
                column_names.clone(),
                limit,
                offset,
            )? {
                return Ok(Some(result));
            }
        }
        if let Some(result) = self.try_simple_filtered_projection_ordered_index_result(
            row_source,
            name,
            table_schema,
            filter_column_index,
            lower_bound.as_ref(),
            upper_bound.as_ref(),
            &residual_plans,
            &projection_indexes,
            column_names.clone(),
            order_by.as_deref(),
            limit,
            offset,
        )? {
            return Ok(Some(result));
        }
        Ok(Some(self.simple_filtered_projection_result_from_source(
            row_source,
            filter_column_index,
            lower_bound.as_ref(),
            upper_bound.as_ref(),
            &residual_plans,
            &projection_indexes,
            column_names,
            order_by,
            limit,
            offset,
        )?))
    }
    #[allow(clippy::too_many_arguments)]
    fn try_simple_rowid_range_projection_result(
        &self,
        row_source: VisibleTableRowSource<'_>,
        table_schema: &TableSchema,
        table_binding: TableBindingRef<'_>,
        filter_column: &str,
        lower_bound: Option<&SimpleRangeBoundValue>,
        upper_bound: Option<&SimpleRangeBoundValue>,
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: &[crate::sql::ast::OrderBy],
        limit: Option<usize>,
        offset: usize,
    ) -> Result<Option<QueryResult>> {
        let lower_only_limited = lower_bound.is_some() && upper_bound.is_none() && limit.is_some();
        let bounded_range = lower_bound.is_some() && upper_bound.is_some();
        if !bounded_range && !lower_only_limited {
            return Ok(None);
        }
        if !order_by.is_empty() {
            if order_by.len() != 1 || order_by[0].descending {
                return Ok(None);
            }
            let Expr::Column {
                table: order_table,
                column: order_column,
            } = &order_by[0].expr
            else {
                return Ok(None);
            };
            if !identifiers_equal(order_column, filter_column)
                || order_table
                    .as_deref()
                    .is_some_and(|qualifier| !matches_table_binding(table_binding, Some(qualifier)))
            {
                return Ok(None);
            }
        }
        if !table_schema
            .primary_key_columns
            .iter()
            .any(|column| identifiers_equal(column, filter_column))
        {
            return Ok(None);
        }
        let Some(filter_column_index) = schema_column_index(table_schema, filter_column) else {
            return Ok(None);
        };
        if table_schema.columns[filter_column_index].column_type
            != crate::catalog::ColumnType::Int64
        {
            return Ok(None);
        }
        let Some(start) = simple_int64_range_start(lower_bound) else {
            return Ok(None);
        };
        let Some(end_exclusive) = simple_int64_range_end_exclusive(upper_bound) else {
            return Ok(None);
        };
        if end_exclusive <= start || limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        }

        let take = limit.unwrap_or(usize::MAX);
        if let Some(rows) = row_source.projected_query_rows_in_id_range(
            start,
            end_exclusive,
            take,
            offset,
            projection_indexes,
        ) {
            return Ok(Some(QueryResult::with_rows(column_names, rows)));
        }
        let max_probe_steps = if upper_bound.is_none() {
            Some(
                row_source
                    .row_count()
                    .saturating_add(offset)
                    .saturating_add(take),
            )
        } else {
            None
        };
        let mut skipped = 0usize;
        let mut rows = Vec::with_capacity(take.min(64));
        let mut row_id = start;
        let mut probe_steps = 0usize;
        while row_id < end_exclusive && rows.len() < take {
            if max_probe_steps.is_some_and(|max_probe_steps| probe_steps >= max_probe_steps) {
                return Ok(None);
            }
            probe_steps = probe_steps.saturating_add(1);
            if let Some(row) = row_source.projected_query_row_by_id(row_id, projection_indexes)? {
                if skipped < offset {
                    skipped += 1;
                } else {
                    rows.push(row);
                }
            }
            let Some(next_row_id) = row_id.checked_add(1) else {
                break;
            };
            row_id = next_row_id;
        }
        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }
    #[allow(clippy::too_many_arguments)]
    fn try_simple_deferred_rowid_range_projection_result<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        table_schema: &TableSchema,
        table_binding: TableBindingRef<'_>,
        filter_column: &str,
        lower_bound: Option<&SimpleRangeBoundValue>,
        upper_bound: Option<&SimpleRangeBoundValue>,
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: &[crate::sql::ast::OrderBy],
        limit: Option<usize>,
        offset: usize,
        use_persistent_pk_index: bool,
        paged_locator_cache: Option<&DeferredPagedRowLocatorCache>,
    ) -> Result<Option<QueryResult>> {
        let lower_only_limited = lower_bound.is_some() && upper_bound.is_none() && limit.is_some();
        let bounded_range = lower_bound.is_some() && upper_bound.is_some();
        if !bounded_range && !lower_only_limited {
            return Ok(None);
        }
        if !order_by.is_empty() {
            if order_by.len() != 1 || order_by[0].descending {
                return Ok(None);
            }
            let Expr::Column {
                table: order_table,
                column: order_column,
            } = &order_by[0].expr
            else {
                return Ok(None);
            };
            if !identifiers_equal(order_column, filter_column)
                || order_table
                    .as_deref()
                    .is_some_and(|qualifier| !matches_table_binding(table_binding, Some(qualifier)))
            {
                return Ok(None);
            }
        }
        if !table_schema
            .primary_key_columns
            .iter()
            .any(|column| identifiers_equal(column, filter_column))
        {
            return Ok(None);
        }
        let Some(filter_column_index) = schema_column_index(table_schema, filter_column) else {
            return Ok(None);
        };
        if table_schema.columns[filter_column_index].column_type
            != crate::catalog::ColumnType::Int64
        {
            return Ok(None);
        }

        let has_matching_locator_cache =
            paged_locator_cache.is_some_and(|cache| cache.matches_state(state));
        let has_persistent_pk_locator =
            use_persistent_pk_index && table_schema.pk_index_root.is_some();
        let has_compressed_lookup = state.pointer.is_compressed();
        if !has_matching_locator_cache && !has_persistent_pk_locator && !has_compressed_lookup {
            return Ok(None);
        }

        let Some(start) = simple_int64_range_start(lower_bound) else {
            return Ok(None);
        };
        let Some(end_exclusive) = simple_int64_range_end_exclusive(upper_bound) else {
            return Ok(None);
        };
        if end_exclusive <= start || limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        }

        let take = limit.unwrap_or(usize::MAX);
        let max_probe_steps = if upper_bound.is_none() {
            Some(state.row_count.saturating_add(offset).saturating_add(take))
        } else {
            None
        };
        let mut skipped = 0usize;
        let mut rows = Vec::with_capacity(take.min(64));
        let mut row_id = start;
        let mut probe_steps = 0usize;
        while row_id < end_exclusive && rows.len() < take {
            if max_probe_steps.is_some_and(|max_probe_steps| probe_steps >= max_probe_steps) {
                return Ok(None);
            }
            probe_steps = probe_steps.saturating_add(1);
            if let Some(values) = read_deferred_projected_values_by_id(
                store,
                state,
                table_schema,
                row_id,
                use_persistent_pk_index,
                paged_locator_cache,
                projection_indexes,
            )? {
                if skipped < offset {
                    skipped += 1;
                } else {
                    rows.push(QueryRow::new(values));
                }
            }
            let Some(next_row_id) = row_id.checked_add(1) else {
                break;
            };
            row_id = next_row_id;
        }
        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }
    pub(crate) fn try_execute_simple_distinct_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || !select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let order_by = self.simple_projection_order_by_plan(
            query,
            table_schema,
            name,
            alias.as_deref().unwrap_or(name),
            &projection_indexes,
        )?;
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let _row_source = self.visible_table_row_source(name);
        let Some(row_source) = _row_source else {
            return Ok(None);
        };
        Ok(Some(self.simple_distinct_projection_result_from_source(
            row_source,
            &projection_indexes,
            column_names,
            order_by,
            limit,
            offset,
        )?))
    }
    pub(crate) fn try_execute_simple_distinct_filtered_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || !select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let binding_name = alias.as_deref().unwrap_or(name);
        let Some(range_filter) = simple_range_projection_filter(filter) else {
            return Ok(None);
        };
        let filter_table = range_filter.table;
        let filter_column = range_filter.column;
        let lower_bound = range_filter.lower;
        let upper_bound = range_filter.upper;
        if let Some(table_name) = filter_table {
            if !identifiers_equal(table_name, name) && !identifiers_equal(table_name, binding_name)
            {
                return Ok(None);
            }
        }
        let filter_column_index = table_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, filter_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "simple filtered distinct projection column {filter_column} missing from {name}"
                ))
            })?;
        let lower_bound = lower_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;
        let upper_bound = upper_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;
        if !range_filter.residual.is_empty() {
            // The distinct filtered fast path does not yet evaluate residual
            // predicates; bail to the generic executor to preserve correctness.
            return Ok(None);
        }
        let order_by = self.simple_projection_order_by_plan(
            query,
            table_schema,
            name,
            binding_name,
            &projection_indexes,
        )?;
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let _row_source = self.visible_table_row_source(name);
        let Some(row_source) = _row_source else {
            return Ok(None);
        };
        Ok(Some(
            self.simple_distinct_filtered_projection_result_from_source(
                row_source,
                filter_column_index,
                lower_bound.as_ref(),
                upper_bound.as_ref(),
                &projection_indexes,
                column_names,
                order_by,
                limit,
                offset,
            )?,
        ))
    }
    pub(crate) fn try_execute_simple_union_range_projection_query(
        &self,
        query: &Query,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        if !query.ctes.is_empty() || query.order_by.len() != 1 {
            return Ok(None);
        }

        let QueryBody::SetOperation {
            op: crate::sql::ast::SetOperation::Union,
            all: false,
            left,
            right,
        } = &query.body
        else {
            return Ok(None);
        };

        let analyze_side = |body: &QueryBody| -> Result<Option<SimpleUnionRangeProjectionSide>> {
            let QueryBody::Select(select) = body else {
                return Ok(None);
            };
            if select.filter.is_none()
                || !select.group_by.is_empty()
                || select.having.is_some()
                || select.distinct
                || !select.distinct_on.is_empty()
                || select.from.len() != 1
            {
                return Ok(None);
            }
            let FromItem::Table { name, alias } = &select.from[0] else {
                return Ok(None);
            };
            if ctes.contains_key(name)
                || self
                    .visible_view(name, NameResolutionScope::Session)
                    .is_some()
            {
                return Ok(None);
            }
            let Some(table_schema) = self.table_schema(name) else {
                return Ok(None);
            };
            if !generated_columns_are_stored(table_schema) {
                return Ok(None);
            }
            let Some((projection_indexes, column_names)) =
                self.simple_projection_plan(select, name, alias, table_schema)
            else {
                return Ok(None);
            };
            if projection_indexes.len() != 1 {
                return Ok(None);
            }
            let Some(filter) = select.filter.as_ref() else {
                return Ok(None);
            };
            let Some(range_filter) = simple_range_projection_filter(filter) else {
                return Ok(None);
            };
            if !range_filter.residual.is_empty() {
                return Ok(None);
            }
            let binding_name = alias.as_deref().unwrap_or(name);
            if let Some(filter_table) = range_filter.table {
                if !identifiers_equal(filter_table, name)
                    && !identifiers_equal(filter_table, binding_name)
                {
                    return Ok(None);
                }
            }
            let Some(filter_column_index) = table_schema
                .columns
                .iter()
                .position(|candidate| identifiers_equal(&candidate.name, range_filter.column))
            else {
                return Ok(None);
            };
            if projection_indexes[0] != filter_column_index {
                return Ok(None);
            }
            let lower_bound = range_filter
                .lower
                .map(|bound| {
                    Ok(SimpleRangeBoundValue {
                        inclusive: bound.inclusive,
                        value: self.eval_expr(
                            bound.value_expr,
                            &Dataset::empty(),
                            &[],
                            params,
                            &BTreeMap::new(),
                            None,
                        )?,
                    })
                })
                .transpose()?;
            let upper_bound = range_filter
                .upper
                .map(|bound| {
                    Ok(SimpleRangeBoundValue {
                        inclusive: bound.inclusive,
                        value: self.eval_expr(
                            bound.value_expr,
                            &Dataset::empty(),
                            &[],
                            params,
                            &BTreeMap::new(),
                            None,
                        )?,
                    })
                })
                .transpose()?;
            if !simple_range_bounds_match_column_type(
                table_schema.columns[filter_column_index].column_type,
                lower_bound.as_ref(),
                upper_bound.as_ref(),
            ) {
                return Ok(None);
            }
            Ok(Some(SimpleUnionRangeProjectionSide {
                table_name: name.clone(),
                alias: alias.clone(),
                projection_indexes,
                column_names,
                filter_column_index,
                lower_bound,
                upper_bound,
            }))
        };

        let Some(left_side) = analyze_side(left)? else {
            return Ok(None);
        };
        let Some(right_side) = analyze_side(right)? else {
            return Ok(None);
        };
        if !identifiers_equal(&left_side.table_name, &right_side.table_name)
            || left_side.projection_indexes != right_side.projection_indexes
            || left_side.filter_column_index != right_side.filter_column_index
        {
            return Ok(None);
        }

        let order_by = &query.order_by[0];
        if order_by.collation.is_some() || order_by.descending {
            return Ok(None);
        }
        let Expr::Column {
            table: order_table,
            column: order_column,
        } = &order_by.expr
        else {
            return Ok(None);
        };
        if let Some(order_table) = order_table.as_deref() {
            if !identifiers_equal(order_table, &left_side.table_name)
                && !left_side
                    .alias
                    .as_deref()
                    .is_some_and(|alias| identifiers_equal(order_table, alias))
            {
                return Ok(None);
            }
        }
        if !identifiers_equal(order_column, &left_side.column_names[0]) {
            return Ok(None);
        }

        let Some(index) =
            self.single_column_btree_index(&left_side.table_name, &left_side.column_names[0])
        else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
            return Ok(None);
        };
        let Some(left_start) = simple_int64_range_start(left_side.lower_bound.as_ref()) else {
            return Ok(None);
        };
        let Some(left_end_exclusive) =
            simple_int64_range_end_exclusive(left_side.upper_bound.as_ref())
        else {
            return Ok(None);
        };
        let Some(right_start) = simple_int64_range_start(right_side.lower_bound.as_ref()) else {
            return Ok(None);
        };
        let Some(right_end_exclusive) =
            simple_int64_range_end_exclusive(right_side.upper_bound.as_ref())
        else {
            return Ok(None);
        };

        let mut distinct_values = BTreeSet::new();
        let mut supported = true;
        let mut collect_range = |range_start: i64, range_end_exclusive: i64| {
            if range_start >= range_end_exclusive {
                return;
            }
            match keys {
                RuntimeBtreeKeys::UniqueInt64(entries, deleted) => {
                    for (value, row_id) in entries.iter() {
                        if deleted.contains(&row_id) {
                            continue;
                        }
                        if value >= range_start && value < range_end_exclusive {
                            distinct_values.insert(value);
                        }
                    }
                }
                RuntimeBtreeKeys::NonUniqueInt64(entries, deleted) => {
                    for (value, row_ids) in entries.iter() {
                        if row_ids.iter().any(|row_id| !deleted.contains(&row_id))
                            && value >= range_start
                            && value < range_end_exclusive
                        {
                            distinct_values.insert(value);
                        }
                    }
                }
                _ => supported = false,
            }
        };
        collect_range(left_start, left_end_exclusive);
        collect_range(right_start, right_end_exclusive);
        if !supported {
            return Ok(None);
        }

        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let rows = distinct_values
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .map(|value| vec![Value::Int64(value)])
            .collect();
        let columns = left_side
            .column_names
            .into_iter()
            .map(|name| ColumnBinding::visible(None, name))
            .collect();
        Ok(Some(Dataset::with_rows(columns, rows)))
    }
    pub(crate) fn try_execute_simple_expression_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        if select_requires_grouped_evaluation(self, select)? {
            return Ok(None);
        }
        if select.distinct
            && (!query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some())
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(name)
        {
            return Ok(None);
        }
        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        if select
            .projection
            .iter()
            .any(select_item_contains_window_or_subquery)
            || select
                .filter
                .as_ref()
                .is_some_and(expr_contains_recursive_unsupported_feature)
            || query
                .order_by
                .iter()
                .any(|order| expr_contains_recursive_unsupported_feature(&order.expr))
        {
            return Ok(None);
        }
        if select
            .projection
            .iter()
            .any(select_item_contains_fulltext_function)
            || select
                .filter
                .as_ref()
                .is_some_and(expr_contains_fulltext_function)
            || query
                .order_by
                .iter()
                .any(|order| expr_contains_fulltext_function(&order.expr))
        {
            return Ok(None);
        }
        let has_expression_projection = select.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => !matches!(expr, Expr::Column { .. }),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => true,
        });
        if !has_expression_projection
            && select.filter.is_none()
            && query.order_by.is_empty()
            && query.limit.is_none()
            && query.offset.is_none()
        {
            return Ok(None);
        }

        let binding_name = alias.as_deref().unwrap_or(name);
        let Some(projection_plan) =
            simple_expression_projection_plan(table_schema, name, binding_name, &select.projection)
        else {
            return Ok(None);
        };
        let ctes = BTreeMap::new();
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let Some(row_source) = self.visible_table_row_source(name) else {
            return Ok(None);
        };
        if let Some(row_ids) = select
            .filter
            .as_ref()
            .map(|filter| {
                self.trigram_candidate_row_ids_for_filter(name, alias, filter, params, &ctes)
            })
            .transpose()?
            .flatten()
        {
            return Ok(Some(
                self.simple_expression_projection_result_from_row_ids(
                    row_source,
                    table_schema,
                    binding_name,
                    &select.projection,
                    &projection_plan,
                    select.filter.as_ref(),
                    select.distinct,
                    &query.order_by,
                    params,
                    limit,
                    offset,
                    &row_ids,
                )?,
            ));
        }
        Ok(Some(self.simple_expression_projection_result_from_source(
            row_source,
            table_schema,
            binding_name,
            &select.projection,
            &projection_plan,
            select.filter.as_ref(),
            select.distinct,
            &query.order_by,
            params,
            limit,
            offset,
        )?))
    }
    fn simple_projection_result_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<Vec<SimpleOrderByPlan>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let bounded_row_count = limit.map(|limit| limit.saturating_add(offset));
        let mut rows = Vec::with_capacity(
            bounded_row_count
                .unwrap_or(row_source.row_count())
                .min(row_source.row_count()),
        );
        if order_by.is_none() {
            let mut skipped = 0usize;
            for stored_row in row_source.rows() {
                let stored_row = stored_row?;
                if skipped < offset {
                    skipped = skipped.saturating_add(1);
                    continue;
                }
                if limit.is_some_and(|limit| rows.len() >= limit) {
                    break;
                }
                rows.push(project_simple_projection_values(
                    stored_row.values(),
                    projection_indexes,
                ));
            }
            return Ok(QueryResult::with_rows(column_names, rows));
        }

        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            rows.push(project_simple_projection_values(
                stored_row.values(),
                projection_indexes,
            ));
        }
        apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by.as_deref(),
            limit,
            offset,
        )
    }
    fn simple_distinct_projection_result_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<Vec<SimpleOrderByPlan>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let mut rows = Vec::new();
        let mut seen = BTreeSet::new();
        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            let projected =
                project_simple_projection_values(stored_row.values(), projection_indexes);
            if seen.insert(row_identity(projected.values())?) {
                rows.push(projected);
            }
        }
        apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by.as_deref(),
            limit,
            offset,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_projection_result_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<Vec<SimpleOrderByPlan>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let bounded_row_count = limit.map(|limit| limit.saturating_add(offset));
        let mut rows = Vec::with_capacity(
            bounded_row_count
                .unwrap_or(state.row_count)
                .min(state.row_count),
        );
        if order_by.is_none() {
            let mut skipped = 0usize;
            visit_persisted_table_rows_until(store, state, |_, values| {
                if skipped < offset {
                    skipped = skipped.saturating_add(1);
                    return Ok(false);
                }
                if limit.is_some_and(|limit| rows.len() >= limit) {
                    return Ok(true);
                }
                rows.push(project_simple_projection_values(values, projection_indexes));
                Ok(limit.is_some_and(|limit| rows.len() >= limit))
            })?;
            return Ok(QueryResult::with_rows(column_names, rows));
        }
        visit_persisted_table_rows(store, state, |_, values| {
            rows.push(project_simple_projection_values(values, projection_indexes));
            Ok(())
        })?;
        apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by.as_deref(),
            limit,
            offset,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_distinct_projection_result_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<Vec<SimpleOrderByPlan>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let mut rows = Vec::new();
        let mut seen = BTreeSet::new();
        visit_persisted_table_rows(store, state, |_, values| {
            let projected = project_simple_projection_values(values, projection_indexes);
            if seen.insert(row_identity(projected.values())?) {
                rows.push(projected);
            }
            Ok(())
        })?;
        apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by.as_deref(),
            limit,
            offset,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_expression_projection_result_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        table_schema: &TableSchema,
        binding_name: &str,
        projection: &[SelectItem],
        projection_plan: &SimpleExpressionProjectionPlan<'_>,
        filter: Option<&Expr>,
        distinct: bool,
        order_by: &[crate::sql::ast::OrderBy],
        params: &[Value],
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let dataset = Dataset::with_rows(
            table_bindings_with_hidden_row_id(table_schema, binding_name),
            Vec::new(),
        );
        let projection_order_by = projection_order_by_plan(order_by, projection);
        let bounded_row_count = limit.map(|limit| limit.saturating_add(offset));
        let mut rows = Vec::with_capacity(bounded_row_count.unwrap_or(state.row_count));
        let mut seen = BTreeSet::new();
        visit_persisted_table_rows(store, state, |row_id, values| {
            let mut eval_values = values.to_vec();
            eval_values.push(Value::Int64(row_id));
            if let Some(filter) = filter {
                if !matches!(
                    self.eval_expr(
                        filter,
                        &dataset,
                        &eval_values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                    Value::Bool(true)
                ) {
                    return Ok(());
                }
            }
            let output = self.project_simple_expression_row(
                projection_plan,
                &dataset,
                &eval_values,
                params,
            )?;
            if distinct && !seen.insert(row_identity(&output)?) {
                return Ok(());
            }

            let mut order_values = Vec::with_capacity(order_by.len());
            if let Some(order_by_plan) = projection_order_by.as_deref() {
                for order in order_by_plan {
                    order_values.push(output[order.projection_index].clone());
                }
            } else {
                for order in order_by {
                    order_values.push(self.eval_expr(
                        &order.expr,
                        &dataset,
                        &eval_values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?);
                }
            }
            let row = (QueryRow::new(output), order_values);
            if let Some(bounded_row_count) = bounded_row_count {
                if order_by.is_empty() {
                    if rows.len() < bounded_row_count {
                        rows.push(row);
                    }
                } else {
                    push_bounded_ordered_query_row(
                        Some(self),
                        &mut rows,
                        row,
                        order_by,
                        bounded_row_count,
                    )?;
                }
            } else {
                rows.push(row);
            }
            Ok(())
        })?;

        if !order_by.is_empty() {
            sort_query_rows_by_order_values(Some(self), &mut rows, order_by)?;
        }

        let rows = rows
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .map(|(row, _)| row)
            .collect();
        Ok(QueryResult::with_rows(
            projection_plan.column_names.clone(),
            rows,
        ))
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_expression_projection_result_from_deferred_row_ids<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        table_schema: &TableSchema,
        binding_name: &str,
        projection: &[SelectItem],
        projection_plan: &SimpleExpressionProjectionPlan<'_>,
        filter: Option<&Expr>,
        distinct: bool,
        order_by: &[crate::sql::ast::OrderBy],
        params: &[Value],
        limit: Option<usize>,
        offset: usize,
        row_ids: &[i64],
        use_persistent_pk_index: bool,
        paged_locator_cache: Option<&DeferredPagedRowLocatorCache>,
    ) -> Result<QueryResult> {
        let dataset = Dataset::with_rows(
            table_bindings_with_hidden_row_id(table_schema, binding_name),
            Vec::new(),
        );
        let projection_order_by = projection_order_by_plan(order_by, projection);
        let bounded_row_count = limit.map(|limit| limit.saturating_add(offset));
        let mut rows = Vec::with_capacity(bounded_row_count.unwrap_or(row_ids.len()));
        let mut seen = BTreeSet::new();
        for row_id in row_ids {
            let Some(stored_row) = read_deferred_stored_row_by_id(
                store,
                state,
                table_schema,
                *row_id,
                use_persistent_pk_index,
                paged_locator_cache,
            )?
            else {
                continue;
            };
            let mut eval_values = stored_row.values;
            eval_values.push(Value::Int64(stored_row.row_id));
            if let Some(filter) = filter {
                if !matches!(
                    self.eval_expr(
                        filter,
                        &dataset,
                        &eval_values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }
            let output = self.project_simple_expression_row(
                projection_plan,
                &dataset,
                &eval_values,
                params,
            )?;
            if distinct && !seen.insert(row_identity(&output)?) {
                continue;
            }

            let mut order_values = Vec::with_capacity(order_by.len());
            if let Some(order_by_plan) = projection_order_by.as_deref() {
                for order in order_by_plan {
                    order_values.push(output[order.projection_index].clone());
                }
            } else {
                for order in order_by {
                    order_values.push(self.eval_expr(
                        &order.expr,
                        &dataset,
                        &eval_values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?);
                }
            }
            let row = (QueryRow::new(output), order_values);
            if let Some(bounded_row_count) = bounded_row_count {
                if order_by.is_empty() {
                    if rows.len() < bounded_row_count {
                        rows.push(row);
                    } else {
                        break;
                    }
                } else {
                    push_bounded_ordered_query_row(
                        Some(self),
                        &mut rows,
                        row,
                        order_by,
                        bounded_row_count,
                    )?;
                }
            } else {
                rows.push(row);
            }
        }

        if !order_by.is_empty() {
            sort_query_rows_by_order_values(Some(self), &mut rows, order_by)?;
        }

        let rows = rows
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .map(|(row, _)| row)
            .collect();
        Ok(QueryResult::with_rows(
            projection_plan.column_names.clone(),
            rows,
        ))
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_expression_projection_result_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        table_schema: &TableSchema,
        binding_name: &str,
        projection: &[SelectItem],
        projection_plan: &SimpleExpressionProjectionPlan<'_>,
        filter: Option<&Expr>,
        distinct: bool,
        order_by: &[crate::sql::ast::OrderBy],
        params: &[Value],
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let dataset = Dataset::with_rows(
            table_bindings_with_hidden_row_id(table_schema, binding_name),
            Vec::new(),
        );
        let projection_order_by = projection_order_by_plan(order_by, projection);
        let bounded_row_count = limit.map(|limit| limit.saturating_add(offset));
        let mut rows = Vec::with_capacity(bounded_row_count.unwrap_or(row_source.row_count()));
        let mut seen = BTreeSet::new();
        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            let mut values = stored_row.values().to_vec();
            values.push(Value::Int64(stored_row.row_id()));
            if let Some(filter) = filter {
                if !matches!(
                    self.eval_expr(filter, &dataset, &values, params, &BTreeMap::new(), None)?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }

            let output =
                self.project_simple_expression_row(projection_plan, &dataset, &values, params)?;
            if distinct && !seen.insert(row_identity(&output)?) {
                continue;
            }

            let mut order_values = Vec::with_capacity(order_by.len());
            if let Some(order_by_plan) = projection_order_by.as_deref() {
                for order in order_by_plan {
                    order_values.push(output[order.projection_index].clone());
                }
            } else {
                for order in order_by {
                    order_values.push(self.eval_expr(
                        &order.expr,
                        &dataset,
                        &values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?);
                }
            }
            let row = (QueryRow::new(output), order_values);
            if let Some(bounded_row_count) = bounded_row_count {
                if order_by.is_empty() {
                    if rows.len() < bounded_row_count {
                        rows.push(row);
                    } else {
                        break;
                    }
                } else {
                    push_bounded_ordered_query_row(
                        Some(self),
                        &mut rows,
                        row,
                        order_by,
                        bounded_row_count,
                    )?;
                }
            } else {
                rows.push(row);
            }
        }

        if !order_by.is_empty() {
            sort_query_rows_by_order_values(Some(self), &mut rows, order_by)?;
        }

        let rows = rows
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .map(|(row, _)| row)
            .collect();
        Ok(QueryResult::with_rows(
            projection_plan.column_names.clone(),
            rows,
        ))
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_expression_projection_result_from_row_ids(
        &self,
        row_source: VisibleTableRowSource<'_>,
        table_schema: &TableSchema,
        binding_name: &str,
        projection: &[SelectItem],
        projection_plan: &SimpleExpressionProjectionPlan<'_>,
        filter: Option<&Expr>,
        distinct: bool,
        order_by: &[crate::sql::ast::OrderBy],
        params: &[Value],
        limit: Option<usize>,
        offset: usize,
        row_ids: &[i64],
    ) -> Result<QueryResult> {
        let dataset = Dataset::with_rows(
            table_bindings_with_hidden_row_id(table_schema, binding_name),
            Vec::new(),
        );
        let projection_order_by = projection_order_by_plan(order_by, projection);
        let bounded_row_count = limit.map(|limit| limit.saturating_add(offset));
        let mut rows = Vec::with_capacity(bounded_row_count.unwrap_or(row_ids.len()));
        let mut seen = BTreeSet::new();
        for row_id in row_ids {
            let Some(stored_row) = row_source.row_by_id(*row_id)? else {
                continue;
            };
            let mut values = stored_row.values().to_vec();
            values.push(Value::Int64(stored_row.row_id()));
            if let Some(filter) = filter {
                if !matches!(
                    self.eval_expr(filter, &dataset, &values, params, &BTreeMap::new(), None)?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }

            let output =
                self.project_simple_expression_row(projection_plan, &dataset, &values, params)?;
            if distinct && !seen.insert(row_identity(&output)?) {
                continue;
            }

            let mut order_values = Vec::with_capacity(order_by.len());
            if let Some(order_by_plan) = projection_order_by.as_deref() {
                for order in order_by_plan {
                    order_values.push(output[order.projection_index].clone());
                }
            } else {
                for order in order_by {
                    order_values.push(self.eval_expr(
                        &order.expr,
                        &dataset,
                        &values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?);
                }
            }
            let row = (QueryRow::new(output), order_values);
            if let Some(bounded_row_count) = bounded_row_count {
                if order_by.is_empty() {
                    if rows.len() < bounded_row_count {
                        rows.push(row);
                    } else {
                        break;
                    }
                } else {
                    push_bounded_ordered_query_row(
                        Some(self),
                        &mut rows,
                        row,
                        order_by,
                        bounded_row_count,
                    )?;
                }
            } else {
                rows.push(row);
            }
        }

        if !order_by.is_empty() {
            sort_query_rows_by_order_values(Some(self), &mut rows, order_by)?;
        }

        let rows = rows
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .map(|(row, _)| row)
            .collect();
        Ok(QueryResult::with_rows(
            projection_plan.column_names.clone(),
            rows,
        ))
    }
    fn project_simple_expression_row(
        &self,
        projection_plan: &SimpleExpressionProjectionPlan<'_>,
        dataset: &Dataset,
        values: &[Value],
        params: &[Value],
    ) -> Result<Vec<Value>> {
        let mut output = Vec::with_capacity(projection_plan.sources.len());
        for source in &projection_plan.sources {
            match source {
                SimpleExpressionProjectionSource::Column(index) => {
                    let value = values.get(*index).ok_or_else(|| {
                        DbError::internal("expression projection column index exceeds row width")
                    })?;
                    output.push(value.clone());
                }
                SimpleExpressionProjectionSource::Expr(expr) => {
                    output.push(self.eval_expr(
                        expr,
                        dataset,
                        values,
                        params,
                        &BTreeMap::new(),
                        None,
                    )?);
                }
            }
        }
        Ok(output)
    }
    #[allow(clippy::too_many_arguments)]
    fn try_simple_filtered_projection_exact_index_result(
        &self,
        row_source: VisibleTableRowSource<'_>,
        table_name: &str,
        table_schema: &TableSchema,
        filter: &Expr,
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<&[SimpleOrderByPlan]>,
        params: &[Value],
        limit: Option<usize>,
        offset: usize,
    ) -> Result<Option<QueryResult>> {
        if limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        }
        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };
        if let Some(filter_table) = filter_table {
            if !identifiers_equal(filter_table, table_name) {
                return Ok(None);
            }
        }

        let value = self.eval_expr(
            value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        if matches!(value, Value::Null) {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        }
        let value = coerce_lookup_literal(table_schema, filter_column, value_expr, value)?;
        if !timestamp_lookup_matches_schema(table_schema, filter_column, &value) {
            return Ok(None);
        }

        let mut rows = Vec::new();
        if row_id_alias_column_name(table_schema)
            .is_some_and(|column_name| identifiers_equal(column_name, filter_column))
        {
            if let Value::Int64(row_id) = value {
                if let Some(stored_row) = row_source.row_by_id(row_id)? {
                    rows.push(project_simple_projection_values(
                        stored_row.values(),
                        projection_indexes,
                    ));
                }
            }
            return Ok(Some(apply_simple_projection_postprocessing_with_order(
                Some(self),
                rows,
                column_names,
                order_by,
                limit,
                offset,
            )?));
        }

        let Some(index) = self.single_column_btree_index(table_name, filter_column) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
            return Ok(None);
        };
        match keys.row_ids_for_value_set(&value)? {
            RuntimeRowIdSet::Empty => {}
            RuntimeRowIdSet::Single(row_id) => {
                if let Some(stored_row) = row_source.row_by_id(row_id)? {
                    rows.push(project_simple_projection_values(
                        stored_row.values(),
                        projection_indexes,
                    ));
                }
            }
            RuntimeRowIdSet::Contiguous { start, len } => {
                rows.reserve(len);
                for row_id in contiguous_row_ids(start, len) {
                    if let Some(stored_row) = row_source.row_by_id(row_id)? {
                        rows.push(project_simple_projection_values(
                            stored_row.values(),
                            projection_indexes,
                        ));
                    }
                }
            }
            RuntimeRowIdSet::Many(row_ids) => {
                rows.reserve(row_ids.len());
                for row_id in row_ids {
                    if let Some(stored_row) = row_source.row_by_id(*row_id)? {
                        rows.push(project_simple_projection_values(
                            stored_row.values(),
                            projection_indexes,
                        ));
                    }
                }
            }
            RuntimeRowIdSet::Owned(row_ids) => {
                rows.reserve(row_ids.len());
                for row_id in row_ids {
                    if let Some(stored_row) = row_source.row_by_id(row_id)? {
                        rows.push(project_simple_projection_values(
                            stored_row.values(),
                            projection_indexes,
                        ));
                    }
                }
            }
        }
        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by,
            limit,
            offset,
        )?))
    }
    #[allow(clippy::too_many_arguments)]
    fn try_simple_filtered_projection_range_index_result(
        &self,
        row_source: VisibleTableRowSource<'_>,
        table_name: &str,
        table_schema: &TableSchema,
        filter_column_index: usize,
        filter_column_name: &str,
        lower_bound: Option<&SimpleRangeBoundValue>,
        upper_bound: Option<&SimpleRangeBoundValue>,
        residual_plans: &[SimpleResidualPlan],
        projection_indexes: &[usize],
        column_names: Vec<String>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<Option<QueryResult>> {
        if limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        }
        let Some(filter_column) = table_schema.columns.get(filter_column_index) else {
            return Ok(None);
        };
        if !simple_range_bounds_match_column_type(
            filter_column.column_type,
            lower_bound,
            upper_bound,
        ) {
            return Ok(None);
        }
        let Some(index) = self.single_column_btree_index(table_name, filter_column_name) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
            return Ok(None);
        };

        let lower_key = lower_bound
            .map(|bound| encode_runtime_index_key(&bound.value).map(|key| (key, bound.inclusive)))
            .transpose()?;
        let upper_key = upper_bound
            .map(|bound| encode_runtime_index_key(&bound.value).map(|key| (key, bound.inclusive)))
            .transpose()?;
        let lower_range: Bound<&[u8]> = match lower_key.as_ref() {
            Some((key, true)) => Bound::Included(key.as_slice()),
            Some((key, false)) => Bound::Excluded(key.as_slice()),
            None => Bound::Unbounded,
        };
        let upper_range: Bound<&[u8]> = match upper_key.as_ref() {
            Some((key, true)) => Bound::Included(key.as_slice()),
            Some((key, false)) => Bound::Excluded(key.as_slice()),
            None => Bound::Unbounded,
        };

        let mut candidate_row_ids = Vec::new();
        match keys {
            RuntimeBtreeKeys::UniqueEncoded(entries, deleted) => {
                candidate_row_ids.extend(
                    entries
                        .range::<[u8], _>((lower_range, upper_range))
                        .filter_map(|(_, row_id)| (!deleted.contains(row_id)).then_some(*row_id)),
                );
            }
            RuntimeBtreeKeys::NonUniqueEncoded(entries, deleted) => {
                for row_ids in entries
                    .range::<[u8], _>((lower_range, upper_range))
                    .map(|(_, row_ids)| row_ids)
                {
                    candidate_row_ids.extend(
                        row_ids
                            .iter()
                            .copied()
                            .filter(|row_id| !deleted.contains(row_id)),
                    );
                }
            }
            RuntimeBtreeKeys::UniqueInt64(..)
            | RuntimeBtreeKeys::NonUniqueInt64(..)
            | RuntimeBtreeKeys::UniqueUuid(..)
            | RuntimeBtreeKeys::NonUniqueUuid(..) => return Ok(None),
        }
        if candidate_row_ids.len().saturating_mul(2) > row_source.row_count() {
            return Ok(None);
        }
        candidate_row_ids.sort_unstable();

        let take = limit.unwrap_or(usize::MAX);
        let mut skipped = 0usize;
        let mut rows = Vec::with_capacity(take.min(candidate_row_ids.len()).min(128));
        for row_id in candidate_row_ids {
            let Some(stored_row) = row_source.row_by_id(row_id)? else {
                continue;
            };
            let values = stored_row.values();
            let candidate = &values[filter_column_index];
            if !simple_range_bound_matches(candidate, lower_bound, upper_bound)?
                || !simple_residual_matches_all(values, residual_plans)?
            {
                continue;
            }
            if skipped < offset {
                skipped = skipped.saturating_add(1);
                continue;
            }
            rows.push(project_simple_projection_values(values, projection_indexes));
            if rows.len() >= take {
                break;
            }
        }

        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }
    #[allow(clippy::too_many_arguments)]
    fn try_simple_filtered_projection_ordered_index_result(
        &self,
        row_source: VisibleTableRowSource<'_>,
        table_name: &str,
        table_schema: &TableSchema,
        filter_column_index: usize,
        lower_bound: Option<&SimpleRangeBoundValue>,
        upper_bound: Option<&SimpleRangeBoundValue>,
        residual_plans: &[SimpleResidualPlan],
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<&[SimpleOrderByPlan]>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<Option<QueryResult>> {
        let Some([order_by]) = order_by else {
            return Ok(None);
        };
        if order_by.collation.is_some() {
            return Ok(None);
        }
        if limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        }
        let Some(order_column_index) = projection_indexes.get(order_by.projection_index).copied()
        else {
            return Ok(None);
        };
        let Some(order_column) = table_schema.columns.get(order_column_index) else {
            return Ok(None);
        };
        let Some(index) = self.single_column_btree_index(table_name, &order_column.name) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
            return Ok(None);
        };
        let take = limit.unwrap_or(usize::MAX);
        let mut skipped = 0usize;
        let mut rows = Vec::with_capacity(take.min(64));

        let mut push_matching_row = |row_id| -> Result<bool> {
            let Some(stored_row) = row_source.row_by_id(row_id)? else {
                return Ok(false);
            };
            let values = stored_row.values();
            let candidate = &values[filter_column_index];
            if !simple_range_bound_matches(candidate, lower_bound, upper_bound)?
                || !simple_residual_matches_all(values, residual_plans)?
            {
                return Ok(false);
            }
            if skipped < offset {
                skipped = skipped.saturating_add(1);
                return Ok(false);
            }
            rows.push(project_simple_projection_values(values, projection_indexes));
            Ok(rows.len() >= take)
        };

        match keys {
            RuntimeBtreeKeys::UniqueEncoded(entries, deleted) => {
                if order_by.descending {
                    for row_id in entries.values().rev() {
                        if deleted.contains(row_id) {
                            continue;
                        }
                        if push_matching_row(*row_id)? {
                            break;
                        }
                    }
                } else {
                    for row_id in entries.values() {
                        if deleted.contains(row_id) {
                            continue;
                        }
                        if push_matching_row(*row_id)? {
                            break;
                        }
                    }
                }
            }
            RuntimeBtreeKeys::NonUniqueEncoded(entries, deleted) => {
                if order_by.descending {
                    let mut done = false;
                    for row_ids in entries.values().rev() {
                        for row_id in row_ids {
                            if deleted.contains(row_id) {
                                continue;
                            }
                            if push_matching_row(*row_id)? {
                                done = true;
                                break;
                            }
                        }
                        if done {
                            break;
                        }
                    }
                } else {
                    let mut done = false;
                    for row_ids in entries.values() {
                        for row_id in row_ids {
                            if deleted.contains(row_id) {
                                continue;
                            }
                            if push_matching_row(*row_id)? {
                                done = true;
                                break;
                            }
                        }
                        if done {
                            break;
                        }
                    }
                }
            }
            RuntimeBtreeKeys::UniqueUuid(entries, deleted) => {
                if order_by.descending {
                    for row_id in entries.values().rev() {
                        if deleted.contains(row_id) {
                            continue;
                        }
                        if push_matching_row(*row_id)? {
                            break;
                        }
                    }
                } else {
                    for row_id in entries.values() {
                        if deleted.contains(row_id) {
                            continue;
                        }
                        if push_matching_row(*row_id)? {
                            break;
                        }
                    }
                }
            }
            RuntimeBtreeKeys::NonUniqueUuid(entries, deleted) => {
                if order_by.descending {
                    let mut done = false;
                    for row_ids in entries.values().rev() {
                        for row_id in row_ids {
                            if deleted.contains(row_id) {
                                continue;
                            }
                            if push_matching_row(*row_id)? {
                                done = true;
                                break;
                            }
                        }
                        if done {
                            break;
                        }
                    }
                } else {
                    let mut done = false;
                    for row_ids in entries.values() {
                        for row_id in row_ids {
                            if deleted.contains(row_id) {
                                continue;
                            }
                            if push_matching_row(*row_id)? {
                                done = true;
                                break;
                            }
                        }
                        if done {
                            break;
                        }
                    }
                }
            }
            RuntimeBtreeKeys::UniqueInt64(..) | RuntimeBtreeKeys::NonUniqueInt64(..) => {
                return Ok(None)
            }
        }

        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_filtered_projection_result_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        filter_column_index: usize,
        lower_bound: Option<&SimpleRangeBoundValue>,
        upper_bound: Option<&SimpleRangeBoundValue>,
        residual_plans: &[SimpleResidualPlan],
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<Vec<SimpleOrderByPlan>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let bounded_row_count = limit.map(|limit| limit.saturating_add(offset));
        let mut rows = Vec::with_capacity(
            bounded_row_count
                .unwrap_or(row_source.row_count())
                .min(row_source.row_count()),
        );
        if order_by.is_none() {
            let mut skipped = 0usize;
            for stored_row in row_source.rows() {
                let stored_row = stored_row?;
                let values = stored_row.values();
                let candidate = &values[filter_column_index];
                if !simple_range_bound_matches(candidate, lower_bound, upper_bound)? {
                    continue;
                }
                if !simple_residual_matches_all(values, residual_plans)? {
                    continue;
                }
                if skipped < offset {
                    skipped = skipped.saturating_add(1);
                    continue;
                }
                if limit.is_some_and(|limit| rows.len() >= limit) {
                    break;
                }
                rows.push(project_simple_projection_values(values, projection_indexes));
            }
            return Ok(QueryResult::with_rows(column_names, rows));
        }

        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            let values = stored_row.values();
            let candidate = &values[filter_column_index];
            if !simple_range_bound_matches(candidate, lower_bound, upper_bound)? {
                continue;
            }
            if !simple_residual_matches_all(values, residual_plans)? {
                continue;
            }
            let row = project_simple_projection_values(values, projection_indexes);
            if let (Some(order_by), Some(bounded_row_count)) = (
                order_by.as_deref(),
                bounded_row_count.filter(|bounded| {
                    *bounded > 0 && row_source.row_count() > bounded.saturating_mul(4)
                }),
            ) {
                push_bounded_projection_ordered_query_row(
                    Some(self),
                    &mut rows,
                    row,
                    order_by,
                    bounded_row_count,
                )?;
            } else {
                rows.push(row);
            }
        }
        apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by.as_deref(),
            limit,
            offset,
        )
    }
    fn simple_contains_like_projection_result_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        filter_column_index: usize,
        literal: &str,
        projection_indexes: &[usize],
        column_names: Vec<String>,
    ) -> Result<QueryResult> {
        let mut rows = Vec::new();
        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            let values = stored_row.values();
            let Some(Value::Text(candidate)) = values.get(filter_column_index) else {
                continue;
            };
            if candidate.contains(literal) {
                rows.push(project_simple_projection_values(values, projection_indexes));
            }
        }
        Ok(QueryResult::with_rows(column_names, rows))
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_distinct_filtered_projection_result_from_source(
        &self,
        row_source: VisibleTableRowSource<'_>,
        filter_column_index: usize,
        lower_bound: Option<&SimpleRangeBoundValue>,
        upper_bound: Option<&SimpleRangeBoundValue>,
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<Vec<SimpleOrderByPlan>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let mut rows = Vec::new();
        let mut seen = BTreeSet::new();
        for stored_row in row_source.rows() {
            let stored_row = stored_row?;
            let candidate = &stored_row.values()[filter_column_index];
            if !simple_range_bound_matches(candidate, lower_bound, upper_bound)? {
                continue;
            }
            let projected =
                project_simple_projection_values(stored_row.values(), projection_indexes);
            if seen.insert(row_identity(projected.values())?) {
                rows.push(projected);
            }
        }
        apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by.as_deref(),
            limit,
            offset,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_filtered_projection_result_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        filter_column_index: usize,
        lower_bound: Option<&SimpleRangeBoundValue>,
        upper_bound: Option<&SimpleRangeBoundValue>,
        residual_plans: &[SimpleResidualPlan],
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<Vec<SimpleOrderByPlan>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let bounded_row_count = limit.map(|limit| limit.saturating_add(offset));
        let mut rows = Vec::with_capacity(
            bounded_row_count
                .unwrap_or(state.row_count)
                .min(state.row_count),
        );
        if order_by.is_none() {
            let mut skipped = 0usize;
            visit_persisted_table_rows_until(store, state, |_, values| {
                let candidate = &values[filter_column_index];
                if !simple_range_bound_matches(candidate, lower_bound, upper_bound)? {
                    return Ok(false);
                }
                if !simple_residual_matches_all(values, residual_plans)? {
                    return Ok(false);
                }
                if skipped < offset {
                    skipped = skipped.saturating_add(1);
                    return Ok(false);
                }
                if limit.is_some_and(|limit| rows.len() >= limit) {
                    return Ok(true);
                }
                rows.push(project_simple_projection_values(values, projection_indexes));
                Ok(limit.is_some_and(|limit| rows.len() >= limit))
            })?;
            return Ok(QueryResult::with_rows(column_names, rows));
        }
        visit_persisted_table_rows(store, state, |_, values| {
            let candidate = &values[filter_column_index];
            if !simple_range_bound_matches(candidate, lower_bound, upper_bound)? {
                return Ok(());
            }
            if !simple_residual_matches_all(values, residual_plans)? {
                return Ok(());
            }
            let row = project_simple_projection_values(values, projection_indexes);
            if let (Some(order_by), Some(bounded_row_count)) = (
                order_by.as_deref(),
                bounded_row_count
                    .filter(|bounded| *bounded > 0 && state.row_count > bounded.saturating_mul(4)),
            ) {
                push_bounded_projection_ordered_query_row(
                    Some(self),
                    &mut rows,
                    row,
                    order_by,
                    bounded_row_count,
                )?;
            } else {
                rows.push(row);
            }
            Ok(())
        })?;
        apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by.as_deref(),
            limit,
            offset,
        )
    }
    fn build_simple_residual_plans(
        &self,
        table_schema: &TableSchema,
        table_name: &str,
        binding_name: &str,
        residual: &[SimpleResidualFilterTerm<'_>],
        params: &[Value],
    ) -> Result<Vec<SimpleResidualPlan>> {
        let mut plans = Vec::with_capacity(residual.len());
        for term in residual {
            if let Some(term_table) = term.table {
                if !identifiers_equal(term_table, table_name)
                    && !identifiers_equal(term_table, binding_name)
                {
                    return Ok(plans);
                }
            }
            let column_index = table_schema
                .columns
                .iter()
                .position(|candidate| identifiers_equal(&candidate.name, term.column))
                .ok_or_else(|| {
                    DbError::internal(format!(
                        "simple filtered projection residual column {} missing from {table_name}",
                        term.column
                    ))
                })?;
            let value = self.eval_expr(
                term.value_expr,
                &Dataset::empty(),
                &[],
                params,
                &BTreeMap::new(),
                None,
            )?;
            plans.push(SimpleResidualPlan {
                column_index,
                op: term.op,
                value,
            });
        }
        Ok(plans)
    }
    #[allow(clippy::too_many_arguments)]
    fn simple_distinct_filtered_projection_result_from_persisted_state<S: PageStore>(
        &self,
        store: &S,
        state: PersistedTableState,
        filter_column_index: usize,
        lower_bound: Option<&SimpleRangeBoundValue>,
        upper_bound: Option<&SimpleRangeBoundValue>,
        projection_indexes: &[usize],
        column_names: Vec<String>,
        order_by: Option<Vec<SimpleOrderByPlan>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult> {
        let mut rows = Vec::new();
        let mut seen = BTreeSet::new();
        visit_persisted_table_rows(store, state, |_, values| {
            let candidate = &values[filter_column_index];
            if !simple_range_bound_matches(candidate, lower_bound, upper_bound)? {
                return Ok(());
            }
            let projected = project_simple_projection_values(values, projection_indexes);
            if seen.insert(row_identity(projected.values())?) {
                rows.push(projected);
            }
            Ok(())
        })?;
        apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            column_names,
            order_by.as_deref(),
            limit,
            offset,
        )
    }
    pub(crate) fn try_execute_simple_indexed_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_indexed_projection_query(query, params)? else {
            return Ok(None);
        };
        let row_source = self.visible_table_row_source(plan.table_name);
        if plan.limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(plan.column_names, Vec::new())));
        }

        if plan.extra_lookup_terms.is_empty()
            && row_id_alias_column_name(plan.table_schema)
                .is_some_and(|column_name| identifiers_equal(column_name, plan.filter_column))
        {
            let mut rows = Vec::new();
            if let Some(row_id) = value_as_int64(&plan.lookup_value) {
                if let Some(stored_row) = row_source
                    .map(|source| source.row_by_id(row_id))
                    .transpose()?
                    .flatten()
                {
                    rows.push(project_simple_projection_values(
                        stored_row.values(),
                        &plan.projection_indexes,
                    ));
                }
            }
            return Ok(Some(apply_simple_projection_postprocessing_with_order(
                Some(self),
                rows,
                plan.column_names,
                plan.order_by.as_deref(),
                plan.limit,
                plan.offset,
            )?));
        }

        let Some(index) = self.btree_index_for_simple_indexed_projection_plan(&plan) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, covering }) = self.index(&index.name) else {
            return Ok(None);
        };
        let covering_offsets = if row_source.is_some_and(|source| !source.has_tombstoned_rows()) {
            covering.as_ref().and_then(|covering| {
                covering_projection_offsets(covering, plan.table_schema, &plan.projection_indexes)
            })
        } else {
            None
        };
        let row_id_order = indexed_projection_row_id_order(&plan);
        let row_ids = row_ids_for_simple_indexed_projection_lookup(keys, &plan)?;

        let scan_limit = if let Some((_, limit_with_offset)) = row_id_order {
            limit_with_offset
        } else if plan.order_by.is_none() && plan.offset == 0 {
            plan.limit.unwrap_or(usize::MAX)
        } else {
            usize::MAX
        };
        let mut rows = Vec::with_capacity(row_ids.len().min(scan_limit));
        let mut row_lookup_error = None;
        if let Some((descending, _)) = row_id_order {
            let ordered_row_ids = row_ids.into_sorted_vec(descending);
            let limit = plan.limit.unwrap_or(usize::MAX);
            for row_id in ordered_row_ids.into_iter().skip(plan.offset).take(limit) {
                if row_lookup_error.is_some() || rows.len() >= scan_limit {
                    break;
                }
                if let (Some(covering), Some(offsets)) =
                    (covering.as_ref(), covering_offsets.as_ref())
                {
                    if let Some(row) = covering.project_row(row_id, offsets) {
                        rows.push(row);
                        continue;
                    }
                }
                let stored_row = match row_source
                    .map(|source| source.row_by_id(row_id))
                    .transpose()
                {
                    Ok(Some(Some(stored_row))) => stored_row,
                    Ok(Some(None)) | Ok(None) => continue,
                    Err(error) => {
                        row_lookup_error = Some(error);
                        break;
                    }
                };
                rows.push(project_simple_projection_values(
                    stored_row.values(),
                    &plan.projection_indexes,
                ));
            }
            if let Some(error) = row_lookup_error {
                return Err(error);
            }
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
        } else {
            row_ids.for_each(|row_id| {
                if row_lookup_error.is_some() || rows.len() >= scan_limit {
                    return;
                }
                if let (Some(covering), Some(offsets)) =
                    (covering.as_ref(), covering_offsets.as_ref())
                {
                    if let Some(row) = covering.project_row(row_id, offsets) {
                        rows.push(row);
                        return;
                    }
                }
                let stored_row = match row_source
                    .map(|source| source.row_by_id(row_id))
                    .transpose()
                {
                    Ok(Some(Some(stored_row))) => stored_row,
                    Ok(Some(None)) | Ok(None) => return,
                    Err(error) => {
                        row_lookup_error = Some(error);
                        return;
                    }
                };
                rows.push(project_simple_projection_values(
                    stored_row.values(),
                    &plan.projection_indexes,
                ));
            });
        }
        if let Some(error) = row_lookup_error {
            return Err(error);
        }
        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            plan.column_names,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        )?))
    }
    pub(crate) fn try_execute_simple_deferred_indexed_projection_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
        use_persistent_pk_index: bool,
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_simple_indexed_projection_query(query, params)? else {
            return Ok(None);
        };
        if let Some(row_source) = self.visible_table_row_source(plan.table_name) {
            if plan.limit == Some(0) {
                return Ok(Some(QueryResult::with_rows(plan.column_names, Vec::new())));
            }
            if plan.extra_lookup_terms.is_empty()
                && row_id_alias_column_name(plan.table_schema)
                    .is_some_and(|column_name| identifiers_equal(column_name, plan.filter_column))
            {
                let mut rows = Vec::new();
                if let Some(row_id) = value_as_int64(&plan.lookup_value) {
                    if let Some(stored_row) = row_source.row_by_id(row_id)? {
                        rows.push(project_simple_projection_values(
                            stored_row.values(),
                            &plan.projection_indexes,
                        ));
                    }
                }
                return Ok(Some(apply_simple_projection_postprocessing_with_order(
                    Some(self),
                    rows,
                    plan.column_names,
                    plan.order_by.as_deref(),
                    plan.limit,
                    plan.offset,
                )?));
            }

            let Some(index) = self.btree_index_for_simple_indexed_projection_plan(&plan) else {
                return Ok(None);
            };
            let Some(RuntimeIndex::Btree { keys, covering }) = self.index(&index.name) else {
                return Ok(None);
            };
            let row_id_order = indexed_projection_row_id_order(&plan);
            let covering_offsets = if !row_source.has_tombstoned_rows() {
                covering.as_ref().and_then(|covering| {
                    covering_projection_offsets(
                        covering,
                        plan.table_schema,
                        &plan.projection_indexes,
                    )
                })
            } else {
                None
            };
            let row_ids = row_ids_for_simple_indexed_projection_lookup(keys, &plan)?;
            let scan_limit =
                if row_id_order.is_none() && plan.order_by.is_none() && plan.offset == 0 {
                    plan.limit.unwrap_or(usize::MAX)
                } else if let Some((_, limit_with_offset)) = row_id_order {
                    limit_with_offset
                } else {
                    usize::MAX
                };
            let mut rows = Vec::with_capacity(row_ids.len().min(scan_limit));
            let mut row_lookup_error = None;
            if let Some((descending, _)) = row_id_order {
                let ordered_row_ids = row_ids.into_sorted_vec(descending);
                let limit = plan.limit.unwrap_or(usize::MAX);
                for row_id in ordered_row_ids.into_iter().skip(plan.offset).take(limit) {
                    if row_lookup_error.is_some() || rows.len() >= scan_limit {
                        break;
                    }
                    if let (Some(covering), Some(offsets)) =
                        (covering.as_ref(), covering_offsets.as_ref())
                    {
                        if let Some(row) = covering.project_row(row_id, offsets) {
                            rows.push(row);
                            continue;
                        }
                    }
                    match row_source.row_by_id(row_id) {
                        Ok(Some(stored_row)) => rows.push(project_simple_projection_values(
                            stored_row.values(),
                            &plan.projection_indexes,
                        )),
                        Ok(None) => {}
                        Err(error) => row_lookup_error = Some(error),
                    }
                }
                if let Some(error) = row_lookup_error {
                    return Err(error);
                }
                return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
            }
            row_ids.for_each(|row_id| {
                if row_lookup_error.is_some() || rows.len() >= scan_limit {
                    return;
                }
                if let (Some(covering), Some(offsets)) =
                    (covering.as_ref(), covering_offsets.as_ref())
                {
                    if let Some(row) = covering.project_row(row_id, offsets) {
                        rows.push(row);
                        return;
                    }
                }
                match row_source.row_by_id(row_id) {
                    Ok(Some(stored_row)) => rows.push(project_simple_projection_values(
                        stored_row.values(),
                        &plan.projection_indexes,
                    )),
                    Ok(None) => {}
                    Err(error) => row_lookup_error = Some(error),
                }
            });
            if let Some(error) = row_lookup_error {
                return Err(error);
            }
            return Ok(Some(apply_simple_projection_postprocessing_with_order(
                Some(self),
                rows,
                plan.column_names,
                plan.order_by.as_deref(),
                plan.limit,
                plan.offset,
            )?));
        }
        if !self.has_deferred_tables() {
            return Ok(None);
        }
        if plan.limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(plan.column_names, Vec::new())));
        }
        if !self
            .deferred_table_names()
            .any(|candidate| identifiers_equal(candidate, plan.table_name))
        {
            return Ok(None);
        }
        let Some(state) = self.persisted_table_state(plan.table_name) else {
            return Ok(None);
        };
        let paged_locator_cache = self
            .catalog
            .table(plan.table_name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());

        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };

        let mut rows = Vec::new();
        if plan.extra_lookup_terms.is_empty()
            && row_id_alias_column_name(plan.table_schema)
                .is_some_and(|column_name| identifiers_equal(column_name, plan.filter_column))
        {
            if let Some(row_id) = value_as_int64(&plan.lookup_value) {
                if let Some(stored_row) = read_deferred_stored_row_by_id(
                    &store,
                    state,
                    plan.table_schema,
                    row_id,
                    use_persistent_pk_index,
                    paged_locator_cache,
                )? {
                    rows.push(project_simple_projection_row(
                        &stored_row,
                        &plan.projection_indexes,
                    ));
                }
            }
            return Ok(Some(apply_simple_projection_postprocessing_with_order(
                Some(self),
                rows,
                plan.column_names,
                plan.order_by.as_deref(),
                plan.limit,
                plan.offset,
            )?));
        }

        let Some(index) = self.btree_index_for_simple_indexed_projection_plan(&plan) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, covering }) = self.index(&index.name) else {
            return Ok(None);
        };
        let covering_offsets = covering.as_ref().and_then(|covering| {
            covering_projection_offsets(covering, plan.table_schema, &plan.projection_indexes)
        });
        let row_id_order = indexed_projection_row_id_order(&plan);
        let row_ids = row_ids_for_simple_indexed_projection_lookup(keys, &plan)?;
        let scan_limit = if let Some((_, limit_with_offset)) = row_id_order {
            limit_with_offset
        } else if plan.order_by.is_none() && plan.offset == 0 {
            plan.limit.unwrap_or(usize::MAX)
        } else {
            usize::MAX
        };
        rows.reserve(row_ids.len().min(scan_limit));
        let mut row_lookup_error = None;
        if let Some((descending, _)) = row_id_order {
            let ordered_row_ids = row_ids.into_sorted_vec(descending);
            let limit = plan.limit.unwrap_or(usize::MAX);
            for row_id in ordered_row_ids.into_iter().skip(plan.offset).take(limit) {
                if row_lookup_error.is_some() || rows.len() >= scan_limit {
                    break;
                }
                if let (Some(covering), Some(offsets)) =
                    (covering.as_ref(), covering_offsets.as_ref())
                {
                    if let Some(row) = covering.project_row(row_id, offsets) {
                        rows.push(row);
                        continue;
                    }
                }
                match read_deferred_stored_row_by_id(
                    &store,
                    state,
                    plan.table_schema,
                    row_id,
                    use_persistent_pk_index,
                    paged_locator_cache,
                ) {
                    Ok(Some(stored_row)) => rows.push(project_simple_projection_row(
                        &stored_row,
                        &plan.projection_indexes,
                    )),
                    Ok(None) => {}
                    Err(error) => {
                        row_lookup_error = Some(error);
                        break;
                    }
                }
            }
            if let Some(error) = row_lookup_error {
                return Err(error);
            }
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
        } else {
            row_ids.for_each(|row_id| {
                if row_lookup_error.is_some() || rows.len() >= scan_limit {
                    return;
                }
                if let (Some(covering), Some(offsets)) =
                    (covering.as_ref(), covering_offsets.as_ref())
                {
                    if let Some(row) = covering.project_row(row_id, offsets) {
                        rows.push(row);
                        return;
                    }
                }
                match read_deferred_stored_row_by_id(
                    &store,
                    state,
                    plan.table_schema,
                    row_id,
                    use_persistent_pk_index,
                    paged_locator_cache,
                ) {
                    Ok(Some(stored_row)) => rows.push(project_simple_projection_row(
                        &stored_row,
                        &plan.projection_indexes,
                    )),
                    Ok(None) => {}
                    Err(error) => row_lookup_error = Some(error),
                }
            });
        }
        if let Some(error) = row_lookup_error {
            return Err(error);
        }
        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            plan.column_names,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        )?))
    }
    pub(crate) fn execute_simple_row_id_projection_at_snapshot(
        &self,
        request: SimpleRowIdProjectionRequest<'_>,
    ) -> Result<Option<QueryResult>> {
        if let Some(view) = self.visible_view(request.table_name, NameResolutionScope::Session) {
            return self.execute_simple_view_row_id_projection_at_snapshot(&request, view);
        }
        if self.visible_table_is_temporary(request.table_name) {
            return Ok(None);
        }
        let Some(table_schema) = self.table_schema(request.table_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        if !row_id_alias_column_name(table_schema)
            .is_some_and(|column_name| identifiers_equal(column_name, request.filter_column))
        {
            return Ok(None);
        }
        let mut projection_indexes = Vec::with_capacity(request.projection_columns.len());
        let mut column_names = Vec::with_capacity(request.projection_columns.len());
        for projection_column in request.projection_columns {
            let Some(index) = table_schema
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, projection_column))
            else {
                return Ok(None);
            };
            projection_indexes.push(index);
            column_names.push((*projection_column).to_string());
        }

        self.execute_validated_simple_row_id_projection_at_snapshot(
            ValidatedSimpleRowIdProjectionRequest {
                table_schema,
                projection_indexes: &projection_indexes,
                column_names: Arc::from(column_names),
                lookup_row_id: request.lookup_row_id,
                pager: request.pager,
                wal: request.wal,
                snapshot_lsn: request.snapshot_lsn,
                use_persistent_pk_index: request.use_persistent_pk_index,
            },
        )
    }
    pub(crate) fn try_execute_resident_simple_row_id_projection(
        &self,
        table_name: &str,
        projection_columns: &[&str],
        filter_column: &str,
        lookup_row_id: i64,
    ) -> Result<Option<QueryResult>> {
        if let Some(view) = self.visible_view(table_name, NameResolutionScope::Session) {
            if view.temporary {
                return Ok(None);
            }
            // The observed-current caller has already established that this
            // runtime represents a stable committed snapshot.  When every
            // base table needed by the view is resident, execute the same
            // validated indexed join without acquiring a reader slot or
            // constructing a snapshot page store.  A missing resident source
            // returns `None`, preserving the snapshot-backed fallback.
            let store = page::InMemoryPageStore::default();
            return self.execute_simple_view_row_id_projection_from_store(
                projection_columns,
                filter_column,
                lookup_row_id,
                view,
                &store,
                false,
                true,
            );
        }
        if self.visible_table_is_temporary(table_name) {
            return Ok(None);
        }
        let Some(table_schema) = self.table_schema(table_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        if !row_id_alias_column_name(table_schema)
            .is_some_and(|column_name| identifiers_equal(column_name, filter_column))
        {
            return Ok(None);
        }
        let mut projection_indexes = Vec::with_capacity(projection_columns.len());
        let mut column_names = Vec::with_capacity(projection_columns.len());
        for projection_column in projection_columns {
            let Some(index) = table_schema
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, projection_column))
            else {
                return Ok(None);
            };
            projection_indexes.push(index);
            column_names.push((*projection_column).to_string());
        }
        self.try_execute_validated_resident_simple_row_id_projection(
            table_schema,
            &projection_indexes,
            Arc::from(column_names),
            lookup_row_id,
        )
    }
    fn execute_simple_view_row_id_projection_at_snapshot(
        &self,
        request: &SimpleRowIdProjectionRequest<'_>,
        view: &ViewSchema,
    ) -> Result<Option<QueryResult>> {
        let store = SnapshotPageStore {
            pager: request.pager,
            wal: request.wal,
            snapshot_lsn: request.snapshot_lsn,
        };
        self.execute_simple_view_row_id_projection_from_store(
            request.projection_columns,
            request.filter_column,
            request.lookup_row_id,
            view,
            &store,
            request.use_persistent_pk_index,
            false,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn execute_simple_view_row_id_projection_from_store<S: PageStore>(
        &self,
        projection_columns: &[&str],
        filter_column: &str,
        lookup_row_id: i64,
        view: &ViewSchema,
        store: &S,
        use_persistent_pk_index: bool,
        resident_only: bool,
    ) -> Result<Option<QueryResult>> {
        if view.temporary {
            return Ok(None);
        }
        let view_query = self.cached_view_query(view)?;
        if view_query.recursive
            || !view_query.ctes.is_empty()
            || !view_query.order_by.is_empty()
            || view_query.limit.is_some()
            || view_query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(view_select) = &view_query.body else {
            return Ok(None);
        };
        if view_select.distinct
            || !view_select.distinct_on.is_empty()
            || !view_select.group_by.is_empty()
            || view_select.having.is_some()
            || view_select.filter.is_some()
            || projection_has_aggregate_items(&view_select.projection)
            || view_select.from.len() != 1
        {
            return Ok(None);
        }

        let Some(filter_source_expr) =
            view_projection_expr_for_output_column(&view_select.projection, filter_column)
        else {
            return Ok(None);
        };
        let Expr::Column {
            table: Some(filter_source_table),
            column: filter_source_column,
        } = &filter_source_expr
        else {
            return Ok(None);
        };

        let mut table_bindings = Vec::with_capacity(3);
        let mut join_constraints = Vec::with_capacity(2);
        if !flatten_inner_join_chain(
            &view_select.from[0],
            &mut table_bindings,
            &mut join_constraints,
        ) || table_bindings.len() < 2
            || join_constraints.len() + 1 != table_bindings.len()
        {
            return Ok(None);
        }

        let mut table_schemas = Vec::with_capacity(table_bindings.len());
        for binding in &table_bindings {
            if self
                .visible_view(binding.name, NameResolutionScope::Session)
                .is_some()
                || self.visible_table_is_temporary(binding.name)
            {
                return Ok(None);
            }
            let Some(schema) = self.table_schema(binding.name) else {
                return Ok(None);
            };
            if !generated_columns_are_stored(schema) {
                return Ok(None);
            }
            table_schemas.push(schema);
        }

        let Some(source_table_index) = table_bindings
            .iter()
            .position(|binding| identifiers_equal(binding.binding_name(), filter_source_table))
        else {
            return Ok(None);
        };
        if source_table_index != 0 {
            return Ok(None);
        }
        let Some(source_rowid_column) = row_id_alias_column_name(table_schemas[source_table_index])
        else {
            return Ok(None);
        };
        if !identifiers_equal(source_rowid_column, filter_source_column) {
            return Ok(None);
        }

        let mut projections = Vec::with_capacity(projection_columns.len());
        let mut column_names = Vec::with_capacity(projection_columns.len());
        for projection_column in projection_columns {
            let Some(view_expr) =
                view_projection_expr_for_output_column(&view_select.projection, projection_column)
            else {
                return Ok(None);
            };
            let Expr::Column {
                table: Some(base_table),
                column: base_column,
            } = view_expr
            else {
                return Ok(None);
            };
            let Some(table_index) = table_bindings
                .iter()
                .position(|binding| identifiers_equal(binding.binding_name(), &base_table))
            else {
                return Ok(None);
            };
            let Some(column_index) = schema_column_index(table_schemas[table_index], &base_column)
            else {
                return Ok(None);
            };
            projections.push(DeferredViewProjection {
                table_index,
                column_index,
                is_rowid_alias: rowid_alias_column_index(table_schemas[table_index])
                    == Some(column_index),
            });
            column_names.push((*projection_column).to_string());
        }

        let mut join_steps = Vec::with_capacity(join_constraints.len());
        for current_table_index in 1..table_bindings.len() {
            let Some(step) = self.deferred_view_join_step(
                &table_bindings,
                &table_schemas,
                join_constraints[current_table_index - 1],
                current_table_index,
            )?
            else {
                return Ok(None);
            };
            join_steps.push(step);
        }
        let table_projections =
            build_deferred_view_table_projections(&table_schemas, &projections, &join_steps);
        let projection_indexes = build_deferred_view_projection_indexes(
            &projections,
            &table_projections,
            "simple view row-id projection",
        )?;
        let linear_tail_can_move =
            deferred_view_linear_tail_projection_can_move(&projection_indexes);

        let mut join_keys = Vec::with_capacity(join_steps.len());
        let mut key_projection_indexes = Vec::with_capacity(join_steps.len());
        for step in &join_steps {
            let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&step.current_index_name)
            else {
                join_keys.clear();
                key_projection_indexes.clear();
                break;
            };
            join_keys.push(keys);
            key_projection_indexes.push(join_key_projection_index(
                step,
                &table_projections[step.previous_table_index],
            )?);
        }

        let mut table_readers = Vec::with_capacity(table_bindings.len());
        for (binding, schema) in table_bindings.iter().zip(table_schemas.iter()) {
            if let Some(source) = self.visible_table_row_source(binding.name) {
                table_readers.push(DeferredViewTableRowReader::Source(source));
                continue;
            }
            if resident_only && self.dirty_tables.contains(&schema.name) {
                return Ok(None);
            }
            let Some(state) = self.persisted_table_state(binding.name) else {
                return Ok(None);
            };
            let cache = self
                .catalog
                .table(binding.name)
                .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
                .map(|cache| cache.as_ref());
            if !deferred_rowid_lookup_available(state, schema, use_persistent_pk_index, cache) {
                return Ok(None);
            }
            table_readers.push(DeferredViewTableRowReader::Deferred {
                state,
                schema,
                paged_locator_cache: cache,
            });
        }

        if resident_only {
            return self.try_execute_observed_current_linear_three_table_view(
                &table_readers,
                &join_steps,
                &join_keys,
                &key_projection_indexes,
                &table_projections,
                &projection_indexes,
                lookup_row_id,
                column_names,
                linear_tail_can_move,
            );
        }

        let mut chunk_payload_cache = HashMap::new();
        let Some(source_row) = table_readers[source_table_index].read_projected_with_chunk_cache(
            store,
            lookup_row_id,
            use_persistent_pk_index,
            &table_projections[source_table_index].projection_indexes,
            &mut chunk_payload_cache,
        )?
        else {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        };

        let mut rows = Vec::with_capacity(64);
        let mut join_partial_rows = Vec::with_capacity(join_steps.len() + 1);
        if let Some(stopped) = self.stream_deferred_view_linear_three_table_rows_from_root(
            store,
            &table_readers,
            &join_steps,
            &join_keys,
            &key_projection_indexes,
            &table_projections,
            &source_row,
            use_persistent_pk_index,
            &mut chunk_payload_cache,
            &mut |root_row, row1, row2| {
                let row = collect_deferred_view_query_row_from_linear_tail(
                    root_row,
                    row1,
                    row2,
                    &projection_indexes,
                    "simple view row-id projection",
                    linear_tail_can_move,
                )?;
                rows.push(row);
                Ok(false)
            },
        )? {
            let _ = stopped;
            return Ok(Some(QueryResult::with_rows(column_names, rows)));
        }

        match self.stream_deferred_view_join_rows_from_root(
            store,
            &table_readers,
            &join_steps,
            &join_keys,
            &key_projection_indexes,
            &table_projections,
            source_row,
            &mut join_partial_rows,
            use_persistent_pk_index,
            false,
            &mut chunk_payload_cache,
            &mut |partial| {
                let values = collect_deferred_view_projection_values(
                    partial,
                    &projection_indexes,
                    "simple view row-id projection",
                )?;
                rows.push(QueryRow::new(values));
                Ok(false)
            },
        )? {
            Some(_) => Ok(Some(QueryResult::with_rows(column_names, rows))),
            None => Ok(None),
        }
    }
    pub(crate) fn execute_resolved_simple_ordered_row_id_projection(
        &self,
        request: ResolvedSimpleOrderedRowIdProjectionRequest<'_>,
    ) -> Result<Option<QueryResult>> {
        if self
            .visible_view(request.table_name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(request.table_name)
        {
            return Ok(None);
        }
        let Some(table_schema) = self.table_schema(request.table_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table_schema)
            || request
                .projection_indexes
                .iter()
                .any(|index| *index >= table_schema.columns.len())
        {
            return Ok(None);
        }
        let Some(order_index) = schema_column_index(table_schema, request.order_column) else {
            return Ok(None);
        };
        if !row_id_alias_column_name(table_schema)
            .is_some_and(|column_name| identifiers_equal(column_name, request.order_column))
            || table_schema.columns[order_index].column_type != ColumnType::Int64
        {
            return Ok(None);
        }
        if request.limit == Some(0) {
            return Ok(Some(QueryResult::with_shared_columns(
                request.column_names,
                Vec::new(),
            )));
        }
        let Some(row_source) = self.visible_table_row_source(table_schema.name.as_str()) else {
            return Ok(None);
        };
        let take = request.limit.unwrap_or(usize::MAX);
        let row_ids = if let Some(row_ids) = self.ordered_runtime_btree_row_ids(
            table_schema.name.as_str(),
            request.order_column,
            request.limit,
            request.offset,
            request.descending,
        )? {
            row_ids
        } else {
            let mut ordered_row_ids = Vec::with_capacity(row_source.row_count());
            for stored_row in row_source.rows() {
                ordered_row_ids.push(stored_row?.row_id());
            }
            ordered_row_ids.sort_unstable();
            if request.descending {
                ordered_row_ids.reverse();
            }
            ordered_row_ids
                .into_iter()
                .skip(request.offset)
                .take(take)
                .collect()
        };
        let mut rows = Vec::with_capacity(row_ids.len().min(64));
        for row_id in row_ids {
            if let Some(row) =
                row_source.projected_query_row_by_id(row_id, request.projection_indexes)?
            {
                rows.push(row);
            }
        }
        Ok(Some(QueryResult::with_shared_columns(
            request.column_names,
            rows,
        )))
    }
    pub(crate) fn execute_resolved_simple_row_id_projection_at_snapshot(
        &self,
        request: ResolvedSimpleRowIdProjectionRequest<'_>,
    ) -> Result<Option<QueryResult>> {
        if self
            .visible_view(request.table_name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(request.table_name)
        {
            return Ok(None);
        }
        let Some(table_schema) = self.table_schema(request.table_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        if request
            .projection_indexes
            .iter()
            .any(|index| *index >= table_schema.columns.len())
        {
            return Ok(None);
        }
        self.execute_validated_simple_row_id_projection_at_snapshot(
            ValidatedSimpleRowIdProjectionRequest {
                table_schema,
                projection_indexes: request.projection_indexes,
                column_names: Arc::clone(&request.column_names),
                lookup_row_id: request.lookup_row_id,
                pager: request.pager,
                wal: request.wal,
                snapshot_lsn: request.snapshot_lsn,
                use_persistent_pk_index: request.use_persistent_pk_index,
            },
        )
    }
    pub(crate) fn execute_resolved_simple_row_id_range_projection_at_snapshot(
        &self,
        request: ResolvedSimpleRowIdRangeProjectionRequest<'_>,
    ) -> Result<Option<QueryResult>> {
        if self
            .visible_view(request.table_name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(request.table_name)
        {
            return Ok(None);
        }
        let Some(table_schema) = self.table_schema(request.table_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        if request
            .projection_indexes
            .iter()
            .any(|index| *index >= table_schema.columns.len())
        {
            return Ok(None);
        }
        let Some(filter_column_index) = schema_column_index(table_schema, request.filter_column)
        else {
            return Ok(None);
        };
        if !table_schema
            .primary_key_columns
            .iter()
            .any(|column| identifiers_equal(column, request.filter_column))
            || table_schema.columns[filter_column_index].column_type != ColumnType::Int64
        {
            return Ok(None);
        }

        let canonical_table_name = table_schema.name.as_str();
        let no_alias = None;
        if let Some(row_source) = self.visible_table_row_source(canonical_table_name) {
            return self.try_simple_rowid_range_projection_result(
                row_source,
                table_schema,
                TableBindingRef {
                    name: canonical_table_name,
                    alias: &no_alias,
                },
                request.filter_column,
                request.lower_bound.as_ref(),
                request.upper_bound.as_ref(),
                request.projection_indexes,
                request.column_names.to_vec(),
                &[],
                request.limit,
                0,
            );
        }

        if !self.has_deferred_tables()
            || !self
                .deferred_table_names()
                .any(|candidate| identifiers_equal(candidate, canonical_table_name))
        {
            return Ok(None);
        }
        let Some(state) = self.persisted_table_state(canonical_table_name) else {
            return Ok(None);
        };
        let store = SnapshotPageStore {
            pager: request.pager,
            wal: request.wal,
            snapshot_lsn: request.snapshot_lsn,
        };
        let paged_locator_cache = self
            .catalog
            .table(canonical_table_name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());
        self.try_simple_deferred_rowid_range_projection_result(
            &store,
            state,
            table_schema,
            TableBindingRef {
                name: canonical_table_name,
                alias: &no_alias,
            },
            request.filter_column,
            request.lower_bound.as_ref(),
            request.upper_bound.as_ref(),
            request.projection_indexes,
            request.column_names.to_vec(),
            &[],
            request.limit,
            0,
            request.use_persistent_pk_index,
            paged_locator_cache,
        )
    }
    pub(crate) fn execute_resolved_simple_row_id_join_projection_at_snapshot(
        &self,
        request: ResolvedSimpleRowIdJoinProjectionRequest<'_>,
    ) -> Result<Option<QueryResult>> {
        if self
            .visible_view(request.left_table_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(request.right_table_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(request.left_table_name)
            || self.visible_table_is_temporary(request.right_table_name)
        {
            return Ok(None);
        }
        let Some(left_schema) = self.table_schema(request.left_table_name) else {
            return Ok(None);
        };
        let Some(right_schema) = self.table_schema(request.right_table_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(left_schema) || !generated_columns_are_stored(right_schema)
        {
            return Ok(None);
        }
        if request
            .projections
            .iter()
            .any(|projection| match projection.side {
                SimpleJoinProjectionSide::Left => {
                    projection.index >= request.left_projection_indexes.len()
                }
                SimpleJoinProjectionSide::Right => {
                    projection.index >= request.right_projection_indexes.len()
                }
            })
            || request
                .left_projection_indexes
                .iter()
                .any(|index| *index >= left_schema.columns.len())
            || request
                .right_projection_indexes
                .iter()
                .any(|index| *index >= right_schema.columns.len())
        {
            return Ok(None);
        }

        let left_name = left_schema.name.as_str();
        let right_name = right_schema.name.as_str();
        if let (Some(left_source), Some(right_source)) = (
            self.visible_table_row_source(left_name),
            self.visible_table_row_source(right_name),
        ) {
            let Some(left_row) = left_source.row_by_id(request.lookup_row_id)? else {
                return Ok(Some(QueryResult::with_shared_columns(
                    Arc::clone(&request.column_names),
                    Vec::new(),
                )));
            };
            let Some(right_row) = right_source.row_by_id(request.lookup_row_id)? else {
                return Ok(Some(QueryResult::with_shared_columns(
                    Arc::clone(&request.column_names),
                    Vec::new(),
                )));
            };
            let row = project_resolved_simple_join_row_from_full_values(
                request.projections,
                request.left_projection_indexes,
                request.right_projection_indexes,
                left_row.values(),
                right_row.values(),
            )?;
            return Ok(Some(QueryResult::with_shared_columns(
                Arc::clone(&request.column_names),
                vec![row],
            )));
        }

        let Some(left_state) = self.persisted_table_state(left_name) else {
            return Ok(None);
        };
        let Some(right_state) = self.persisted_table_state(right_name) else {
            return Ok(None);
        };
        let left_cache = self
            .catalog
            .table(left_name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());
        let right_cache = self
            .catalog
            .table(right_name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());
        if !deferred_rowid_lookup_available(
            left_state,
            left_schema,
            request.use_persistent_pk_index,
            left_cache,
        ) || !deferred_rowid_lookup_available(
            right_state,
            right_schema,
            request.use_persistent_pk_index,
            right_cache,
        ) {
            return Ok(None);
        }

        let store = SnapshotPageStore {
            pager: request.pager,
            wal: request.wal,
            snapshot_lsn: request.snapshot_lsn,
        };
        let Some(left_values) = read_deferred_projected_values_by_id(
            &store,
            left_state,
            left_schema,
            request.lookup_row_id,
            request.use_persistent_pk_index,
            left_cache,
            request.left_projection_indexes,
        )?
        else {
            return Ok(Some(QueryResult::with_shared_columns(
                Arc::clone(&request.column_names),
                Vec::new(),
            )));
        };
        let Some(right_values) = read_deferred_projected_values_by_id(
            &store,
            right_state,
            right_schema,
            request.lookup_row_id,
            request.use_persistent_pk_index,
            right_cache,
            request.right_projection_indexes,
        )?
        else {
            return Ok(Some(QueryResult::with_shared_columns(
                Arc::clone(&request.column_names),
                Vec::new(),
            )));
        };
        let row =
            project_resolved_simple_join_row(request.projections, &left_values, &right_values)?;
        Ok(Some(QueryResult::with_shared_columns(
            Arc::clone(&request.column_names),
            vec![row],
        )))
    }
    pub(crate) fn try_execute_validated_resident_simple_row_id_projection(
        &self,
        table_schema: &TableSchema,
        projection_indexes: &[usize],
        column_names: Arc<[String]>,
        lookup_row_id: i64,
    ) -> Result<Option<QueryResult>> {
        let canonical_table_name = table_schema.name.as_str();
        if let Some(row_source) = self.visible_table_row_source(canonical_table_name) {
            let projects_complete_row = projection_indexes.len() == table_schema.columns.len()
                && projection_indexes
                    .iter()
                    .enumerate()
                    .all(|(position, index)| position == *index);
            let row = if projects_complete_row {
                row_source.full_query_row_by_id(lookup_row_id)?
            } else {
                row_source.projected_query_row_by_id(lookup_row_id, projection_indexes)?
            };
            let rows = row.map(|row| vec![row]).unwrap_or_default();
            return Ok(Some(QueryResult::with_shared_columns(column_names, rows)));
        }

        // A checkpoint may re-defer a paged table while retaining its compact
        // row locator directory and a bounded set of already-verified chunk
        // payloads.  Those immutable payloads are part of this runtime's
        // observed snapshot, so a point lookup can be answered without a
        // pager read or a cross-process reader slot.  If the requested chunk
        // was not retained, preserve the normal snapshot-backed fallback.
        if !self.dirty_tables.contains(canonical_table_name) {
            let state = self.persisted_table_state(canonical_table_name);
            let cache = self
                .deferred_paged_row_locator_caches
                .get(canonical_table_name);
            if let (Some(state), Some(cache)) = (state, cache) {
                if cache.matches_state(state) {
                    if let Some(cached) = cache.locators.get(lookup_row_id) {
                        if let Some(payload) =
                            cache.verified_payload(cached.pointer, cached.checksum)
                        {
                            let projects_complete_row = projection_indexes.len()
                                == table_schema.columns.len()
                                && projection_indexes
                                    .iter()
                                    .enumerate()
                                    .all(|(position, index)| position == *index);
                            let row = if projects_complete_row {
                                decode_row_by_locator_from_payload(
                                    payload,
                                    lookup_row_id,
                                    cached.locator,
                                )
                                .map(|row| QueryRow::new(row.values))?
                            } else {
                                QueryRow::new(decode_projected_values_by_locator_from_payload::<
                                    page::InMemoryPageStore,
                                >(
                                    None, payload, cached.locator, projection_indexes
                                )?)
                            };
                            return Ok(Some(QueryResult::with_shared_columns(
                                column_names,
                                vec![row],
                            )));
                        }
                    }
                }
            }
        }
        Ok(None)
    }
    fn try_execute_simple_deferred_rowid_join_projection_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
        use_persistent_pk_index: bool,
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() || !query.order_by.is_empty() || query.limit.is_some() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || projection_has_aggregate_items(&select.projection)
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: left_name,
            alias: left_alias,
        } = &**left
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: right_name,
            alias: right_alias,
        } = &**right
        else {
            return Ok(None);
        };
        if self
            .visible_view(left_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(right_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(left_name)
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }

        let left_schema = match self.table_schema(left_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let right_schema = match self.table_schema(right_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(left_schema) || !generated_columns_are_stored(right_schema)
        {
            return Ok(None);
        }
        let left_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        let Some(join_equalities) = simple_indexed_join_constraint_equalities(
            constraint,
            left_binding,
            right_binding,
            left_schema,
            right_schema,
        ) else {
            return Ok(None);
        };
        let Some((left_join_columns, right_join_columns)) =
            orient_join_equalities(&join_equalities, left_binding, right_binding)
        else {
            return Ok(None);
        };
        if left_join_columns.len() != 1 || right_join_columns.len() != 1 {
            return Ok(None);
        }
        let Some(left_rowid_column) = crate::exec::dml::row_id_alias_column_name(left_schema)
        else {
            return Ok(None);
        };
        let Some(right_rowid_column) = crate::exec::dml::row_id_alias_column_name(right_schema)
        else {
            return Ok(None);
        };
        if !identifiers_equal(left_join_columns[0], left_rowid_column)
            || !identifiers_equal(right_join_columns[0], right_rowid_column)
        {
            return Ok(None);
        }

        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };
        let filter_value = self.eval_expr(
            value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        let Value::Int64(source_row_id) = filter_value else {
            return Ok(None);
        };

        let source_is_left = if matches_table_binding(left_binding, filter_table)
            && identifiers_equal(filter_column, left_rowid_column)
        {
            true
        } else if matches_table_binding(right_binding, filter_table)
            && identifiers_equal(filter_column, right_rowid_column)
        {
            false
        } else {
            return Ok(None);
        };
        let using_join_columns =
            simple_indexed_join_using_columns(constraint, left_schema, right_schema);
        let join_eval_bindings = simple_join_projection_eval_bindings(
            left_name,
            left_alias,
            left_schema,
            right_name,
            right_alias,
            right_schema,
        );
        let join_eval_dataset = Dataset::with_rows(join_eval_bindings, Vec::new());
        let Some((projection_plan, column_names)) = simple_join_projection_plan(
            &select.projection,
            &join_eval_dataset,
            left_name,
            left_alias,
            left_schema,
            right_name,
            right_alias,
            right_schema,
            &using_join_columns,
        ) else {
            return Ok(None);
        };

        let source_join_column = if source_is_left {
            left_join_columns[0]
        } else {
            right_join_columns[0]
        };
        if let (Some(left_source), Some(right_source)) = (
            self.visible_table_row_source(left_name),
            self.visible_table_row_source(right_name),
        ) {
            let (source_source, source_schema, probe_source) = if source_is_left {
                (left_source, left_schema, right_source)
            } else {
                (right_source, right_schema, left_source)
            };
            let Some(source_row) = source_source.row_by_id(source_row_id)? else {
                return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
            };
            let Some(source_join_index) = schema_column_index(source_schema, source_join_column)
            else {
                return Ok(None);
            };
            let Some(Value::Int64(probe_row_id)) = source_row.values().get(source_join_index)
            else {
                return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
            };
            let Some(probe_row) = probe_source.row_by_id(*probe_row_id)? else {
                return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
            };
            let left_values = if source_is_left {
                source_row.values()
            } else {
                probe_row.values()
            };
            let right_values = if source_is_left {
                probe_row.values()
            } else {
                source_row.values()
            };
            let row = project_simple_join_row(
                self,
                &projection_plan,
                &join_eval_dataset,
                Some(left_values),
                left_schema.columns.len(),
                Some(right_values),
                right_schema.columns.len(),
                params,
            )?;
            return Ok(Some(QueryResult::with_rows(
                column_names,
                vec![QueryRow::new(row)],
            )));
        }

        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let Some(left_state) = self.persisted_table_state(left_name) else {
            return Ok(None);
        };
        let Some(right_state) = self.persisted_table_state(right_name) else {
            return Ok(None);
        };
        let left_cache = self
            .catalog
            .table(left_name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());
        let right_cache = self
            .catalog
            .table(right_name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());
        if !deferred_rowid_lookup_available(
            left_state,
            left_schema,
            use_persistent_pk_index,
            left_cache,
        ) || !deferred_rowid_lookup_available(
            right_state,
            right_schema,
            use_persistent_pk_index,
            right_cache,
        ) {
            return Ok(None);
        }

        let (source_state, source_schema, source_cache, probe_state, probe_schema, probe_cache) =
            if source_is_left {
                (
                    left_state,
                    left_schema,
                    left_cache,
                    right_state,
                    right_schema,
                    right_cache,
                )
            } else {
                (
                    right_state,
                    right_schema,
                    right_cache,
                    left_state,
                    left_schema,
                    left_cache,
                )
            };
        let Some(source_row) = read_deferred_stored_row_by_id(
            &store,
            source_state,
            source_schema,
            source_row_id,
            use_persistent_pk_index,
            source_cache,
        )?
        else {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        };
        let Some(source_join_index) = schema_column_index(source_schema, source_join_column) else {
            return Ok(None);
        };
        let join_value = source_row.values.get(source_join_index);
        let Some(Value::Int64(probe_row_id)) = join_value else {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        };
        let Some(probe_row) = read_deferred_stored_row_by_id(
            &store,
            probe_state,
            probe_schema,
            *probe_row_id,
            use_persistent_pk_index,
            probe_cache,
        )?
        else {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        };

        let left_values = if source_is_left {
            source_row.values.as_slice()
        } else {
            probe_row.values.as_slice()
        };
        let right_values = if source_is_left {
            probe_row.values.as_slice()
        } else {
            source_row.values.as_slice()
        };
        let row = project_simple_join_row(
            self,
            &projection_plan,
            &join_eval_dataset,
            Some(left_values),
            left_schema.columns.len(),
            Some(right_values),
            right_schema.columns.len(),
            params,
        )?;
        Ok(Some(QueryResult::with_rows(
            column_names,
            vec![QueryRow::new(row)],
        )))
    }
    #[allow(clippy::too_many_arguments)]
    fn try_execute_simple_deferred_view_projection_limit_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
        use_persistent_pk_index: bool,
    ) -> Result<Option<QueryResult>> {
        if query.recursive || !query.ctes.is_empty() || !query.order_by.is_empty() {
            return Ok(None);
        }
        let Some(limit_expr) = query.limit.as_ref() else {
            return Ok(None);
        };
        let ctes = BTreeMap::new();
        let limit = usize::try_from(self.eval_constant_i64(limit_expr, params, &ctes)?.max(0))
            .unwrap_or(usize::MAX);
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || projection_has_aggregate_items(&select.projection)
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table {
            name: view_name,
            alias: view_alias,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let Some(view) = self.visible_view(view_name, NameResolutionScope::Session) else {
            return Ok(None);
        };
        if view.temporary {
            return Ok(None);
        }
        let view_binding = view_alias.as_deref().unwrap_or(view_name.as_str());
        let view_query = self.cached_view_query(view)?;
        if view_query.recursive
            || !view_query.ctes.is_empty()
            || !view_query.order_by.is_empty()
            || view_query.limit.is_some()
            || view_query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(view_select) = &view_query.body else {
            return Ok(None);
        };
        if view_select.distinct
            || !view_select.distinct_on.is_empty()
            || !view_select.group_by.is_empty()
            || view_select.having.is_some()
            || view_select.filter.is_some()
            || projection_has_aggregate_items(&view_select.projection)
            || view_select.from.len() != 1
        {
            return Ok(None);
        }

        let mut table_bindings = Vec::with_capacity(3);
        let mut join_constraints = Vec::with_capacity(2);
        if !flatten_inner_join_chain(
            &view_select.from[0],
            &mut table_bindings,
            &mut join_constraints,
        ) || !(2..=3).contains(&table_bindings.len())
            || join_constraints.len() + 1 != table_bindings.len()
        {
            return Ok(None);
        }

        let mut table_schemas = Vec::with_capacity(table_bindings.len());
        for binding in &table_bindings {
            if self
                .visible_view(binding.name, NameResolutionScope::Session)
                .is_some()
                || self.visible_table_is_temporary(binding.name)
            {
                return Ok(None);
            }
            let Some(schema) = self.table_schema(binding.name) else {
                return Ok(None);
            };
            if !generated_columns_are_stored(schema) {
                return Ok(None);
            }
            table_schemas.push(schema);
        }
        let deferred_row_count = table_bindings
            .iter()
            .filter(|binding| self.visible_table_row_source(binding.name).is_none())
            .filter_map(|binding| self.persisted_table_state(binding.name))
            .map(|state| state.row_count)
            .sum::<usize>();
        if deferred_row_count < DEFERRED_VIEW_LIMIT_MIN_PERSISTED_ROWS {
            return Ok(None);
        }

        let mut projections = Vec::with_capacity(select.projection.len());
        let mut column_names = Vec::with_capacity(select.projection.len());
        for (ordinal, item) in select.projection.iter().enumerate() {
            let SelectItem::Expr { expr, alias } = item else {
                return Ok(None);
            };
            let Expr::Column {
                table: outer_table,
                column: outer_column,
            } = expr
            else {
                return Ok(None);
            };
            if outer_table
                .as_deref()
                .is_some_and(|qualifier| !identifiers_equal(qualifier, view_binding))
            {
                return Ok(None);
            }
            let Some(view_expr) =
                view_projection_expr_for_output_column(&view_select.projection, outer_column)
            else {
                return Ok(None);
            };
            let Expr::Column {
                table: Some(base_table),
                column: base_column,
            } = view_expr
            else {
                return Ok(None);
            };
            let Some(table_index) = table_bindings
                .iter()
                .position(|binding| identifiers_equal(binding.binding_name(), &base_table))
            else {
                return Ok(None);
            };
            let Some(column_index) = schema_column_index(table_schemas[table_index], &base_column)
            else {
                return Ok(None);
            };
            let is_rowid_alias =
                rowid_alias_column_index(table_schemas[table_index]) == Some(column_index);
            projections.push(DeferredViewProjection {
                table_index,
                column_index,
                is_rowid_alias,
            });
            column_names.push(
                alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(expr, ordinal + 1)),
            );
        }
        if limit == 0 {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        }

        let mut join_steps = Vec::with_capacity(join_constraints.len());
        for current_table_index in 1..table_bindings.len() {
            let Some(step) = self.deferred_view_join_step(
                &table_bindings,
                &table_schemas,
                join_constraints[current_table_index - 1],
                current_table_index,
            )?
            else {
                return Ok(None);
            };
            join_steps.push(step);
        }
        let table_projections =
            build_deferred_view_table_projections(&table_schemas, &projections, &join_steps);
        let projection_indexes = build_deferred_view_projection_indexes(
            &projections,
            &table_projections,
            "deferred view limit projection",
        )?;
        let linear_tail_can_move =
            deferred_view_linear_tail_projection_can_move(&projection_indexes);
        let mut join_keys = Vec::with_capacity(join_steps.len());
        let mut key_projection_indexes = Vec::with_capacity(join_steps.len());
        for step in &join_steps {
            let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&step.current_index_name)
            else {
                return Ok(None);
            };
            join_keys.push(keys);
            key_projection_indexes.push(join_key_projection_index(
                step,
                &table_projections[step.previous_table_index],
            )?);
        }

        let mut table_readers = Vec::with_capacity(table_bindings.len());
        for (binding, schema) in table_bindings.iter().zip(table_schemas.iter()) {
            if let Some(source) = self.visible_table_row_source(binding.name) {
                table_readers.push(DeferredViewTableRowReader::Source(source));
                continue;
            }
            let Some(state) = self.persisted_table_state(binding.name) else {
                return Ok(None);
            };
            let cache = self
                .catalog
                .table(binding.name)
                .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
                .map(|cache| cache.as_ref());
            if !deferred_rowid_lookup_available(state, schema, use_persistent_pk_index, cache) {
                return Ok(None);
            }
            table_readers.push(DeferredViewTableRowReader::Deferred {
                state,
                schema,
                paged_locator_cache: cache,
            });
        }

        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let mut chunk_payload_cache = HashMap::new();
        let mut rows = Vec::with_capacity(limit);
        let mut join_partial_rows = Vec::with_capacity(join_steps.len() + 1);
        let mut offset_remaining = offset;
        let mut limit_remaining = limit;
        if let DeferredViewTableRowReader::Source(source) = &table_readers[0] {
            for root_row in source.rows() {
                let root_row = root_row?;
                let values = if table_projections[0].projection_indexes.is_empty() {
                    Vec::new()
                } else {
                    project_simple_projection_value_vec(
                        root_row.values(),
                        &table_projections[0].projection_indexes,
                    )
                };
                let root_row = StoredRow {
                    row_id: root_row.row_id(),
                    values,
                };
                if self.push_deferred_view_limit_rows_from_root(
                    &store,
                    &table_readers,
                    &join_steps,
                    &join_keys,
                    &key_projection_indexes,
                    &table_projections,
                    &projection_indexes,
                    root_row,
                    &mut offset_remaining,
                    &mut limit_remaining,
                    &mut rows,
                    &mut join_partial_rows,
                    &mut chunk_payload_cache,
                    use_persistent_pk_index,
                    linear_tail_can_move,
                )? {
                    break;
                }
            }
        } else {
            let DeferredViewTableRowReader::Deferred { state, .. } = &table_readers[0] else {
                return Ok(None);
            };
            visit_persisted_table_projected_values_until(
                &store,
                *state,
                &table_projections[0].projection_indexes,
                |row_id, root_values| {
                    let root_row = StoredRow {
                        row_id,
                        values: root_values.to_vec(),
                    };
                    self.push_deferred_view_limit_rows_from_root(
                        &store,
                        &table_readers,
                        &join_steps,
                        &join_keys,
                        &key_projection_indexes,
                        &table_projections,
                        &projection_indexes,
                        root_row,
                        &mut offset_remaining,
                        &mut limit_remaining,
                        &mut rows,
                        &mut join_partial_rows,
                        &mut chunk_payload_cache,
                        use_persistent_pk_index,
                        linear_tail_can_move,
                    )
                },
            )?;
        }

        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }
    #[allow(clippy::too_many_arguments)]
    fn try_execute_simple_deferred_view_filter_projection_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
        use_persistent_pk_index: bool,
    ) -> Result<Option<QueryResult>> {
        if query.recursive
            || !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || projection_has_aggregate_items(&select.projection)
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Table {
            name: view_name,
            alias: view_alias,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let Some(view) = self.visible_view(view_name, NameResolutionScope::Session) else {
            return Ok(None);
        };
        if view.temporary {
            return Ok(None);
        }
        let view_binding = view_alias.as_deref().unwrap_or(view_name.as_str());
        let Some((filter_qualifier, filter_column, value_expr)) = simple_btree_lookup(filter)
        else {
            return Ok(None);
        };
        if filter_qualifier.is_some_and(|qualifier| !identifiers_equal(qualifier, view_binding)) {
            return Ok(None);
        }

        let view_query = self.cached_view_query(view)?;
        if view_query.recursive
            || !view_query.ctes.is_empty()
            || !view_query.order_by.is_empty()
            || view_query.limit.is_some()
            || view_query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(view_select) = &view_query.body else {
            return Ok(None);
        };
        if view_select.distinct
            || !view_select.distinct_on.is_empty()
            || !view_select.group_by.is_empty()
            || view_select.having.is_some()
            || view_select.filter.is_some()
            || projection_has_aggregate_items(&view_select.projection)
            || view_select.from.len() != 1
        {
            return Ok(None);
        }

        let Some(filter_source_expr) =
            view_projection_expr_for_output_column(&view_select.projection, filter_column)
        else {
            return Ok(None);
        };
        let Expr::Column {
            table: Some(filter_source_table),
            column: filter_source_column,
        } = &filter_source_expr
        else {
            return Ok(None);
        };

        let mut table_bindings = Vec::with_capacity(3);
        let mut join_constraints = Vec::with_capacity(2);
        if !flatten_inner_join_chain(
            &view_select.from[0],
            &mut table_bindings,
            &mut join_constraints,
        ) || table_bindings.len() < 2
            || join_constraints.len() + 1 != table_bindings.len()
        {
            return Ok(None);
        }

        let mut table_schemas = Vec::with_capacity(table_bindings.len());
        for binding in &table_bindings {
            if self
                .visible_view(binding.name, NameResolutionScope::Session)
                .is_some()
                || self.visible_table_is_temporary(binding.name)
            {
                return Ok(None);
            }
            let Some(schema) = self.table_schema(binding.name) else {
                return Ok(None);
            };
            if !generated_columns_are_stored(schema) {
                return Ok(None);
            }
            table_schemas.push(schema);
        }

        let Some(source_table_index) = table_bindings
            .iter()
            .position(|binding| identifiers_equal(binding.binding_name(), filter_source_table))
        else {
            return Ok(None);
        };
        if source_table_index != 0 {
            return Ok(None);
        }
        let Some(source_rowid_column) = row_id_alias_column_name(table_schemas[source_table_index])
        else {
            return Ok(None);
        };
        if !identifiers_equal(source_rowid_column, filter_source_column) {
            return Ok(None);
        }

        let mut projections = Vec::with_capacity(select.projection.len());
        let mut column_names = Vec::with_capacity(select.projection.len());
        for (ordinal, item) in select.projection.iter().enumerate() {
            let SelectItem::Expr { expr, alias } = item else {
                return Ok(None);
            };
            let Expr::Column {
                table: outer_table,
                column: outer_column,
            } = expr
            else {
                return Ok(None);
            };
            if outer_table
                .as_deref()
                .is_some_and(|qualifier| !identifiers_equal(qualifier, view_binding))
            {
                return Ok(None);
            }
            let Some(view_expr) =
                view_projection_expr_for_output_column(&view_select.projection, outer_column)
            else {
                return Ok(None);
            };
            let Expr::Column {
                table: Some(base_table),
                column: base_column,
            } = view_expr
            else {
                return Ok(None);
            };
            let Some(table_index) = table_bindings
                .iter()
                .position(|binding| identifiers_equal(binding.binding_name(), &base_table))
            else {
                return Ok(None);
            };
            let Some(column_index) = schema_column_index(table_schemas[table_index], &base_column)
            else {
                return Ok(None);
            };
            let is_rowid_alias =
                rowid_alias_column_index(table_schemas[table_index]) == Some(column_index);
            projections.push(DeferredViewProjection {
                table_index,
                column_index,
                is_rowid_alias,
            });
            column_names.push(
                alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(expr, ordinal + 1)),
            );
        }

        let mut join_steps = Vec::with_capacity(join_constraints.len());
        for current_table_index in 1..table_bindings.len() {
            let Some(step) = self.deferred_view_join_step(
                &table_bindings,
                &table_schemas,
                join_constraints[current_table_index - 1],
                current_table_index,
            )?
            else {
                return Ok(None);
            };
            join_steps.push(step);
        }
        let table_projections =
            build_deferred_view_table_projections(&table_schemas, &projections, &join_steps);
        let projection_indexes = build_deferred_view_projection_indexes(
            &projections,
            &table_projections,
            "deferred view projection",
        )?;
        let linear_tail_can_move =
            deferred_view_linear_tail_projection_can_move(&projection_indexes);
        let mut join_keys = Vec::with_capacity(join_steps.len());
        let mut key_projection_indexes = Vec::with_capacity(join_steps.len());
        for step in &join_steps {
            let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&step.current_index_name)
            else {
                join_keys.clear();
                key_projection_indexes.clear();
                break;
            };
            join_keys.push(keys);
            key_projection_indexes.push(join_key_projection_index(
                step,
                &table_projections[step.previous_table_index],
            )?);
        }

        let mut table_readers = Vec::with_capacity(table_bindings.len());
        for (binding, schema) in table_bindings.iter().zip(table_schemas.iter()) {
            if let Some(source) = self.visible_table_row_source(binding.name) {
                table_readers.push(DeferredViewTableRowReader::Source(source));
                continue;
            }
            let Some(state) = self.persisted_table_state(binding.name) else {
                return Ok(None);
            };
            let cache = self
                .catalog
                .table(binding.name)
                .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
                .map(|cache| cache.as_ref());
            if !deferred_rowid_lookup_available(state, schema, use_persistent_pk_index, cache) {
                return Ok(None);
            }
            table_readers.push(DeferredViewTableRowReader::Deferred {
                state,
                schema,
                paged_locator_cache: cache,
            });
        }

        let source_row_id = match simple_int64_constant_expr_value(value_expr, params)? {
            Some(value) => value,
            None => {
                let filter_value = self.eval_expr(
                    value_expr,
                    &Dataset::empty(),
                    &[],
                    params,
                    &BTreeMap::new(),
                    None,
                )?;
                let Value::Int64(value) = filter_value else {
                    return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
                };
                value
            }
        };
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let mut chunk_payload_cache = HashMap::new();
        let Some(source_row) = table_readers[source_table_index].read_projected_with_chunk_cache(
            &store,
            source_row_id,
            use_persistent_pk_index,
            &table_projections[source_table_index].projection_indexes,
            &mut chunk_payload_cache,
        )?
        else {
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        };

        let mut rows = Vec::with_capacity(64);
        let mut join_partial_rows = Vec::with_capacity(join_steps.len() + 1);
        if let Some(stopped) = self.stream_deferred_view_linear_three_table_rows_from_root(
            &store,
            &table_readers,
            &join_steps,
            &join_keys,
            &key_projection_indexes,
            &table_projections,
            &source_row,
            use_persistent_pk_index,
            &mut chunk_payload_cache,
            &mut |root_row, row1, row2| {
                let row = collect_deferred_view_query_row_from_linear_tail(
                    root_row,
                    row1,
                    row2,
                    &projection_indexes,
                    "deferred view projection",
                    linear_tail_can_move,
                )?;
                rows.push(row);
                Ok(false)
            },
        )? {
            let _ = stopped;
            return Ok(Some(QueryResult::with_rows(column_names, rows)));
        }

        match self.stream_deferred_view_join_rows_from_root(
            &store,
            &table_readers,
            &join_steps,
            &join_keys,
            &key_projection_indexes,
            &table_projections,
            source_row,
            &mut join_partial_rows,
            use_persistent_pk_index,
            false,
            &mut chunk_payload_cache,
            &mut |partial| {
                let values = collect_deferred_view_projection_values(
                    partial,
                    &projection_indexes,
                    "deferred view projection",
                )?;
                rows.push(QueryRow::new(values));
                Ok(false)
            },
        )? {
            Some(_) => Ok(Some(QueryResult::with_rows(column_names, rows))),
            None => Ok(None),
        }
    }
    pub(crate) fn simple_btree_index_for_table_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Option<&IndexSchema> {
        self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, table_name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0].expression_sql.is_none()
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|indexed_column| identifiers_equal(indexed_column, column_name))
                && matches!(self.index(&index.name), Some(RuntimeIndex::Btree { .. }))
        })
    }
    pub(crate) fn try_execute_simple_deferred_paged_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
        use_persistent_pk_index: bool,
    ) -> Result<Option<QueryResult>> {
        if let Some(result) = self.try_execute_simple_deferred_view_projection_limit_query(
            query,
            params,
            pager,
            wal,
            snapshot_lsn,
            use_persistent_pk_index,
        )? {
            return Ok(Some(result));
        }
        if let Some(result) = self.try_execute_simple_deferred_view_filter_projection_query(
            query,
            params,
            pager,
            wal,
            snapshot_lsn,
            use_persistent_pk_index,
        )? {
            return Ok(Some(result));
        }
        if let Some(result) = self.try_execute_simple_deferred_paged_grouped_count_query(
            query,
            params,
            pager,
            wal,
            snapshot_lsn,
        )? {
            return Ok(Some(result));
        }
        if let Some(result) = self
            .try_execute_simple_deferred_paged_grouped_numeric_aggregate_query(
                query,
                params,
                pager,
                wal,
                snapshot_lsn,
            )?
        {
            return Ok(Some(result));
        }
        if let Some(result) = self.try_execute_simple_deferred_rowid_join_projection_query(
            query,
            params,
            pager,
            wal,
            snapshot_lsn,
            use_persistent_pk_index,
        )? {
            return Ok(Some(result));
        }
        if let Some(result) = self.try_execute_simple_deferred_distinct_filtered_projection_query(
            query,
            params,
            pager,
            wal,
            snapshot_lsn,
        )? {
            return Ok(Some(result));
        }
        if let Some(result) = self.try_execute_simple_deferred_distinct_projection_query(
            query,
            params,
            pager,
            wal,
            snapshot_lsn,
        )? {
            return Ok(Some(result));
        }
        if let Some(result) = self.try_execute_simple_deferred_filtered_projection_query(
            query,
            params,
            pager,
            wal,
            snapshot_lsn,
            use_persistent_pk_index,
        )? {
            return Ok(Some(result));
        }
        if let Some(result) = self.try_execute_simple_deferred_table_projection_query(
            query,
            params,
            pager,
            wal,
            snapshot_lsn,
            use_persistent_pk_index,
        )? {
            return Ok(Some(result));
        }
        if let Some(result) = self.try_execute_simple_deferred_expression_projection_query(
            query,
            params,
            pager,
            wal,
            snapshot_lsn,
            use_persistent_pk_index,
        )? {
            return Ok(Some(result));
        }
        Ok(None)
    }
    fn try_execute_simple_deferred_expression_projection_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
        use_persistent_pk_index: bool,
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        if select_requires_grouped_evaluation(self, select)? {
            return Ok(None);
        }
        if select.distinct
            && (!query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some())
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(name)
            || self.visible_table_row_source(name).is_some()
        {
            return Ok(None);
        }
        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        if select
            .projection
            .iter()
            .any(select_item_contains_window_or_subquery)
            || select
                .filter
                .as_ref()
                .is_some_and(expr_contains_recursive_unsupported_feature)
            || query
                .order_by
                .iter()
                .any(|order| expr_contains_recursive_unsupported_feature(&order.expr))
        {
            return Ok(None);
        }
        if select
            .projection
            .iter()
            .any(select_item_contains_fulltext_function)
            || select
                .filter
                .as_ref()
                .is_some_and(expr_contains_fulltext_function)
            || query
                .order_by
                .iter()
                .any(|order| expr_contains_fulltext_function(&order.expr))
        {
            return Ok(None);
        }
        let has_expression_projection = select.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => !matches!(expr, Expr::Column { .. }),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => true,
        });
        if !has_expression_projection
            && select.filter.is_none()
            && query.order_by.is_empty()
            && query.limit.is_none()
            && query.offset.is_none()
        {
            return Ok(None);
        }

        let binding_name = alias.as_deref().unwrap_or(name);
        let Some(projection_plan) =
            simple_expression_projection_plan(table_schema, name, binding_name, &select.projection)
        else {
            return Ok(None);
        };
        let ctes = BTreeMap::new();
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let Some(state) = self.persisted_table_state(name) else {
            return Ok(None);
        };
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let paged_locator_cache = self
            .catalog
            .table(name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());
        if deferred_rowid_lookup_available(
            state,
            table_schema,
            use_persistent_pk_index,
            paged_locator_cache,
        ) {
            if let Some(row_ids) = select
                .filter
                .as_ref()
                .map(|filter| {
                    self.trigram_candidate_row_ids_for_filter(name, alias, filter, params, &ctes)
                })
                .transpose()?
                .flatten()
            {
                return Ok(Some(
                    self.simple_expression_projection_result_from_deferred_row_ids(
                        &store,
                        state,
                        table_schema,
                        binding_name,
                        &select.projection,
                        &projection_plan,
                        select.filter.as_ref(),
                        select.distinct,
                        &query.order_by,
                        params,
                        limit,
                        offset,
                        &row_ids,
                        use_persistent_pk_index,
                        paged_locator_cache,
                    )?,
                ));
            }
        }
        Ok(Some(
            self.simple_expression_projection_result_from_persisted_state(
                &store,
                state,
                table_schema,
                binding_name,
                &select.projection,
                &projection_plan,
                select.filter.as_ref(),
                select.distinct,
                &query.order_by,
                params,
                limit,
                offset,
            )?,
        ))
    }
    fn try_execute_simple_deferred_distinct_projection_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || !select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(name)
            || self.visible_table_row_source(name).is_some()
        {
            return Ok(None);
        }
        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let order_by = self.simple_projection_order_by_plan(
            query,
            table_schema,
            name,
            alias.as_deref().unwrap_or(name),
            &projection_indexes,
        )?;
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let Some(state) = self.persisted_table_state(name) else {
            return Ok(None);
        };
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        Ok(Some(
            self.simple_distinct_projection_result_from_persisted_state(
                &store,
                state,
                &projection_indexes,
                column_names,
                order_by,
                limit,
                offset,
            )?,
        ))
    }
    fn try_execute_simple_deferred_table_projection_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
        use_persistent_pk_index: bool,
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(name)
            || self.visible_table_row_source(name).is_some()
        {
            return Ok(None);
        }
        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let order_by = self.simple_projection_order_by_plan(
            query,
            table_schema,
            name,
            alias.as_deref().unwrap_or(name),
            &projection_indexes,
        )?;
        let row_id_order = if query.order_by.len() == 1 {
            if let Expr::Column {
                table: order_table,
                column: order_column,
            } = &query.order_by[0].expr
            {
                if order_table.as_deref().is_some_and(|qualifier| {
                    !matches_table_binding(TableBindingRef { name, alias }, Some(qualifier))
                }) {
                    None
                } else if let Some(filter_column_index) =
                    schema_column_index(table_schema, order_column)
                {
                    if table_schema
                        .primary_key_columns
                        .iter()
                        .any(|column| identifiers_equal(column, order_column))
                        && table_schema.columns[filter_column_index].column_type
                            == crate::catalog::ColumnType::Int64
                    {
                        Some((order_column.as_str(), query.order_by[0].descending))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if !query.order_by.is_empty() && order_by.is_none() && row_id_order.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let Some(state) = self.persisted_table_state(name) else {
            return Ok(None);
        };
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let paged_locator_cache = self
            .catalog
            .table(name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());
        if let Some((filter_column, descending)) = row_id_order {
            if limit != Some(0) && use_persistent_pk_index {
                if let Some(result) = try_persistent_pk_ordered_projection_result(
                    &store,
                    state,
                    table_schema,
                    &projection_indexes,
                    column_names.clone(),
                    limit,
                    offset,
                    descending,
                )? {
                    return Ok(Some(result));
                }
            }
            if limit != Some(0) {
                if let Some(row_ids) = self.ordered_runtime_btree_row_ids(
                    name,
                    filter_column,
                    limit,
                    offset,
                    descending,
                )? {
                    let mut rows = Vec::with_capacity(row_ids.len().min(64));
                    for row_id in row_ids {
                        if let Some(values) = read_deferred_projected_values_by_id(
                            &store,
                            state,
                            table_schema,
                            row_id,
                            use_persistent_pk_index,
                            paged_locator_cache,
                            &projection_indexes,
                        )? {
                            rows.push(QueryRow::new(values));
                        }
                    }
                    return Ok(Some(QueryResult::with_rows(column_names, rows)));
                }
            }
        }
        let mut unbounded_lower_bound = None;
        if let Some(cache) = paged_locator_cache.filter(|cache| cache.matches_state(state)) {
            if let Some(min_row_id) = cache.min_row_id() {
                unbounded_lower_bound = Some(SimpleRangeBoundValue {
                    inclusive: true,
                    value: Value::Int64(min_row_id),
                });
            }
        }
        if unbounded_lower_bound.is_none() && use_persistent_pk_index {
            if let Some(min_row_id) = first_persistent_pk_row_id(&store, table_schema)? {
                unbounded_lower_bound = Some(SimpleRangeBoundValue {
                    inclusive: true,
                    value: Value::Int64(min_row_id),
                });
            }
        }
        if limit.is_some() && limit != Some(0) {
            if let Some((filter_column, _descending)) = row_id_order {
                if let Some(result) = self.try_simple_deferred_rowid_range_projection_result(
                    &store,
                    state,
                    table_schema,
                    TableBindingRef { name, alias },
                    filter_column,
                    unbounded_lower_bound.as_ref(),
                    None,
                    &projection_indexes,
                    column_names.clone(),
                    &query.order_by,
                    limit,
                    offset,
                    use_persistent_pk_index,
                    paged_locator_cache,
                )? {
                    return Ok(Some(result));
                }
            }
        }
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        Ok(Some(self.simple_projection_result_from_persisted_state(
            &store,
            state,
            &projection_indexes,
            column_names,
            order_by,
            limit,
            offset,
        )?))
    }
    fn try_execute_simple_deferred_distinct_filtered_projection_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || !select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(name)
            || self.visible_table_row_source(name).is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let binding_name = alias.as_deref().unwrap_or(name);
        let Some(range_filter) = simple_range_projection_filter(filter) else {
            return Ok(None);
        };
        let filter_table = range_filter.table;
        let filter_column = range_filter.column;
        let lower_bound = range_filter.lower;
        let upper_bound = range_filter.upper;
        if let Some(table_name) = filter_table {
            if !identifiers_equal(table_name, name) && !identifiers_equal(table_name, binding_name)
            {
                return Ok(None);
            }
        }
        let filter_column_index = table_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, filter_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "simple deferred filtered distinct projection column {filter_column} missing from {name}"
                ))
            })?;
        let lower_bound = lower_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;
        let upper_bound = upper_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;
        if !range_filter.residual.is_empty() {
            // The deferred distinct filtered fast path does not yet evaluate
            // residual predicates; bail to the generic executor to preserve
            // correctness.
            return Ok(None);
        }
        let order_by = self.simple_projection_order_by_plan(
            query,
            table_schema,
            name,
            binding_name,
            &projection_indexes,
        )?;
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let Some(state) = self.persisted_table_state(name) else {
            return Ok(None);
        };
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        Ok(Some(
            self.simple_distinct_filtered_projection_result_from_persisted_state(
                &store,
                state,
                filter_column_index,
                lower_bound.as_ref(),
                upper_bound.as_ref(),
                &projection_indexes,
                column_names,
                order_by,
                limit,
                offset,
            )?,
        ))
    }
    fn try_execute_simple_deferred_filtered_projection_query(
        &self,
        query: &Query,
        params: &[Value],
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
        use_persistent_pk_index: bool,
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(name)
            || self.visible_table_row_source(name).is_some()
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let binding_name = alias.as_deref().unwrap_or(name);

        let Some(range_filter) = simple_range_projection_filter(filter) else {
            return Ok(None);
        };
        let filter_table = range_filter.table;
        let filter_column = range_filter.column;
        let lower_bound = range_filter.lower;
        let upper_bound = range_filter.upper;
        if let Some(table_name) = filter_table {
            if !identifiers_equal(table_name, name) && !identifiers_equal(table_name, binding_name)
            {
                return Ok(None);
            }
        }
        let filter_column_index = table_schema
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, filter_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "simple filtered projection column {filter_column} missing from {name}"
                ))
            })?;

        let lower_bound = lower_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;
        let upper_bound = upper_bound
            .map(|bound| {
                Ok(SimpleRangeBoundValue {
                    inclusive: bound.inclusive,
                    value: self.eval_expr(
                        bound.value_expr,
                        &Dataset::empty(),
                        &[],
                        params,
                        &BTreeMap::new(),
                        None,
                    )?,
                })
            })
            .transpose()?;

        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let Some(state) = self.persisted_table_state(name) else {
            return Ok(None);
        };
        let store = SnapshotPageStore {
            pager,
            wal,
            snapshot_lsn,
        };
        let paged_locator_cache = self
            .catalog
            .table(name)
            .and_then(|table| self.deferred_paged_row_locator_caches.get(&table.name))
            .map(|cache| cache.as_ref());
        if range_filter.residual.is_empty() {
            if let Some(result) = self.try_simple_deferred_rowid_range_projection_result(
                &store,
                state,
                table_schema,
                TableBindingRef { name, alias },
                filter_column,
                lower_bound.as_ref(),
                upper_bound.as_ref(),
                &projection_indexes,
                column_names.clone(),
                &query.order_by,
                limit,
                offset,
                use_persistent_pk_index,
                paged_locator_cache,
            )? {
                return Ok(Some(result));
            }
        }

        let order_by = self.simple_projection_order_by_plan(
            query,
            table_schema,
            name,
            binding_name,
            &projection_indexes,
        )?;
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let residual_plans = self.build_simple_residual_plans(
            table_schema,
            name,
            binding_name,
            &range_filter.residual,
            params,
        )?;
        if residual_plans.len() != range_filter.residual.len() {
            return Ok(None);
        }
        Ok(Some(
            self.simple_filtered_projection_result_from_persisted_state(
                &store,
                state,
                filter_column_index,
                lower_bound.as_ref(),
                upper_bound.as_ref(),
                &residual_plans,
                &projection_indexes,
                column_names,
                order_by,
                limit,
                offset,
            )?,
        ))
    }
    fn analyze_simple_indexed_projection_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<SimpleIndexedProjectionPlan<'a>>> {
        if !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(name)
        {
            return Ok(None);
        }

        let table_schema = match self.table_schema(name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let binding_name = alias.as_deref().unwrap_or(name);
        let Some(lookup_terms) = simple_btree_lookup_terms(filter) else {
            return Ok(None);
        };
        for (filter_table, _, _) in &lookup_terms {
            if filter_table.as_ref().is_some_and(|table_name| {
                !identifiers_equal(table_name, name) && !identifiers_equal(table_name, binding_name)
            }) {
                return Ok(None);
            }
        }
        let ordered_lookup_terms = if lookup_terms.len() == 1 {
            lookup_terms
        } else {
            let Some(index) =
                self.compound_btree_index_for_lookup_terms(name, lookup_terms.as_slice())
            else {
                return Ok(None);
            };
            ordered_lookup_terms_for_index(index, lookup_terms.as_slice())?
        };

        let mut lookup_values = Vec::with_capacity(ordered_lookup_terms.len());
        for (_, column, value_expr) in &ordered_lookup_terms {
            let value = self.eval_expr(
                value_expr,
                &Dataset::empty(),
                &[],
                params,
                &BTreeMap::new(),
                None,
            )?;
            let value = coerce_lookup_literal(table_schema, column, value_expr, value)?;
            if !timestamp_lookup_matches_schema(table_schema, column, &value) {
                return Ok(None);
            }
            lookup_values.push(value);
        }
        let filter_column = ordered_lookup_terms[0].1;
        let lookup_value = lookup_values
            .first()
            .cloned()
            .ok_or_else(|| DbError::internal("indexed projection lookup terms are empty"))?;
        let extra_lookup_terms = ordered_lookup_terms
            .iter()
            .skip(1)
            .zip(lookup_values.into_iter().skip(1))
            .map(|((_, column, _), value)| (*column, value))
            .collect::<Vec<_>>();
        let Some((projection_indexes, column_names)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let order_by = self.simple_projection_order_by_plan(
            query,
            table_schema,
            name,
            binding_name,
            &projection_indexes,
        )?;
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        Ok(Some(SimpleIndexedProjectionPlan {
            table_name: name,
            table_schema,
            filter_column,
            lookup_value,
            extra_lookup_terms,
            projection_indexes,
            column_names,
            order_by,
            limit,
            offset,
        }))
    }
    pub(crate) fn simple_indexed_projection_missing_runtime_btree<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<(&'a str, &'a str)>> {
        let Some(plan) = self.analyze_simple_indexed_projection_query(query, params)? else {
            return Ok(None);
        };
        if plan.extra_lookup_terms.is_empty()
            && row_id_alias_column_name(plan.table_schema)
                .is_some_and(|column_name| identifiers_equal(column_name, plan.filter_column))
        {
            return Ok(None);
        }
        let Some(index) = self.btree_index_for_simple_indexed_projection_plan(&plan) else {
            return Ok(None);
        };
        if matches!(self.index(&index.name), Some(RuntimeIndex::Btree { .. })) {
            Ok(None)
        } else {
            Ok(Some((plan.table_name, index.name.as_str())))
        }
    }
    pub(crate) fn simple_indexed_projection_missing_persistent_pk_root<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<&'a str>> {
        let Some(plan) = self.analyze_simple_indexed_projection_query(query, params)? else {
            return Ok(None);
        };
        if !plan.extra_lookup_terms.is_empty() || plan.table_schema.pk_index_root.is_some() {
            return Ok(None);
        }
        let Some(filter_column_index) = schema_column_index(plan.table_schema, plan.filter_column)
        else {
            return Ok(None);
        };
        if plan.table_schema.columns[filter_column_index].column_type != ColumnType::Int64
            || !plan
                .table_schema
                .primary_key_columns
                .iter()
                .any(|column| identifiers_equal(column, plan.filter_column))
        {
            return Ok(None);
        }
        if self
            .persisted_table_state(plan.table_name)
            .is_some_and(|state| state.pointer.head_page_id != 0)
        {
            Ok(Some(plan.table_name))
        } else {
            Ok(None)
        }
    }
    pub(crate) fn simple_ordered_projection_missing_persistent_pk_root<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<&'a str>> {
        if !query.ctes.is_empty() || query.order_by.len() != 1 || query.order_by[0].descending {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || projection_has_aggregate_items(&select.projection)
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(name)
            || self.visible_table_row_source(name).is_some()
        {
            return Ok(None);
        }
        let Some(table_schema) = self.table_schema(name) else {
            return Ok(None);
        };
        if table_schema.pk_index_root.is_some() || !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some((_projection_indexes, _)) =
            self.simple_projection_plan(select, name, alias, table_schema)
        else {
            return Ok(None);
        };
        let Expr::Column {
            table: order_table,
            column: order_column,
        } = &query.order_by[0].expr
        else {
            return Ok(None);
        };
        if order_table.as_deref().is_some_and(|qualifier| {
            !matches_table_binding(TableBindingRef { name, alias }, Some(qualifier))
        }) {
            return Ok(None);
        }
        let Some(order_column_index) = schema_column_index(table_schema, order_column) else {
            return Ok(None);
        };
        if table_schema.columns[order_column_index].column_type != ColumnType::Int64
            || !table_schema
                .primary_key_columns
                .iter()
                .any(|column| identifiers_equal(column, order_column))
        {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        if limit == Some(0) {
            return Ok(None);
        }
        if self
            .persisted_table_state(name)
            .is_some_and(|state| state.pointer.head_page_id != 0)
        {
            Ok(Some(name))
        } else {
            Ok(None)
        }
    }
    pub(crate) fn single_column_btree_index(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Option<&IndexSchema> {
        self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, table_name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|index_column| identifiers_equal(index_column, column_name))
                && index.columns[0].expression_sql.is_none()
        })
    }
    fn compound_btree_index_for_lookup_terms(
        &self,
        table_name: &str,
        lookup_terms: &[(Option<&str>, &str, &Expr)],
    ) -> Option<&IndexSchema> {
        if lookup_terms.len() < 2 {
            return None;
        }
        self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, table_name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() >= lookup_terms.len()
                && index
                    .columns
                    .iter()
                    .take(lookup_terms.len())
                    .all(|index_column| {
                        index_column.expression_sql.is_none()
                            && index_column.column_name.as_deref().is_some_and(|column| {
                                lookup_terms.iter().any(|(_, lookup_column, _)| {
                                    identifiers_equal(column, lookup_column)
                                })
                            })
                    })
        })
    }
    fn btree_index_for_simple_indexed_projection_plan(
        &self,
        plan: &SimpleIndexedProjectionPlan<'_>,
    ) -> Option<&IndexSchema> {
        if plan.extra_lookup_terms.is_empty() {
            return self.single_column_btree_index(plan.table_name, plan.filter_column);
        }
        let lookup_columns = std::iter::once(plan.filter_column)
            .chain(plan.extra_lookup_terms.iter().map(|(column, _)| *column))
            .collect::<Vec<_>>();
        self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, plan.table_name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() >= lookup_columns.len()
                && index
                    .columns
                    .iter()
                    .take(lookup_columns.len())
                    .zip(lookup_columns.iter())
                    .all(|(index_column, lookup_column)| {
                        index_column.expression_sql.is_none()
                            && index_column
                                .column_name
                                .as_deref()
                                .is_some_and(|index_column| {
                                    identifiers_equal(index_column, lookup_column)
                                })
                    })
        })
    }
    fn ordered_runtime_btree_row_ids(
        &self,
        table_name: &str,
        column_name: &str,
        limit: Option<usize>,
        offset: usize,
        descending: bool,
    ) -> Result<Option<Vec<i64>>> {
        let Some(index) = self.single_column_btree_index(table_name, column_name) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
            return Ok(None);
        };
        let take = limit.unwrap_or(usize::MAX);
        if take == 0 {
            return Ok(Some(Vec::new()));
        }
        match keys {
            RuntimeBtreeKeys::UniqueInt64(entries, deleted) => {
                let mut ordered = entries
                    .iter()
                    .filter(|(_, row_id)| !deleted.contains(row_id))
                    .collect::<Vec<_>>();
                let window = offset.saturating_add(take).min(ordered.len());
                if window == 0 {
                    return Ok(Some(Vec::new()));
                }
                if window < ordered.len() {
                    if descending {
                        ordered
                            .select_nth_unstable_by(window - 1, |left, right| right.0.cmp(&left.0));
                        ordered.truncate(window);
                        ordered.sort_unstable_by_key(|(key, _)| std::cmp::Reverse(*key));
                    } else {
                        ordered.select_nth_unstable_by_key(window - 1, |(key, _)| *key);
                        ordered.truncate(window);
                        ordered.sort_unstable_by_key(|(key, _)| *key);
                    }
                } else if descending {
                    ordered.sort_unstable_by_key(|(key, _)| std::cmp::Reverse(*key));
                } else {
                    ordered.sort_unstable_by_key(|(key, _)| *key);
                }
                Ok(Some(
                    ordered
                        .into_iter()
                        .skip(offset)
                        .take(take)
                        .map(|(_, row_id)| row_id)
                        .collect(),
                ))
            }
            RuntimeBtreeKeys::NonUniqueInt64(entries, deleted) => {
                let mut ordered = entries
                    .iter()
                    .map(|(key, row_ids)| (key, row_ids.to_vec()))
                    .collect::<Vec<_>>();
                ordered.sort_unstable_by_key(|(key, _)| *key);
                let mut skipped = 0usize;
                let mut row_ids = Vec::with_capacity(take.min(64));
                let ordered = if descending {
                    ordered.into_iter().rev().collect::<Vec<_>>()
                } else {
                    ordered
                };
                for (_, mut ids) in ordered {
                    ids.sort_unstable();
                    for row_id in ids {
                        if deleted.contains(&row_id) {
                            continue;
                        }
                        if skipped < offset {
                            skipped += 1;
                            continue;
                        }
                        row_ids.push(row_id);
                        if row_ids.len() == take {
                            return Ok(Some(row_ids));
                        }
                    }
                }
                Ok(Some(row_ids))
            }
            RuntimeBtreeKeys::UniqueEncoded(..)
            | RuntimeBtreeKeys::NonUniqueEncoded(..)
            | RuntimeBtreeKeys::UniqueUuid(..)
            | RuntimeBtreeKeys::NonUniqueUuid(..) => Ok(None),
        }
    }
    pub(crate) fn simple_projection_plan(
        &self,
        select: &Select,
        table_name: &str,
        table_alias: &Option<String>,
        table_schema: &TableSchema,
    ) -> Option<(Vec<usize>, Vec<String>)> {
        let binding_name = table_alias.as_deref().unwrap_or(table_name);
        let mut projection_indexes = Vec::with_capacity(select.projection.len());
        let mut column_names = Vec::with_capacity(select.projection.len());

        for item in &select.projection {
            match item {
                SelectItem::Expr {
                    expr,
                    alias: select_alias,
                } => {
                    let Expr::Column {
                        table: table_name_expr,
                        column,
                    } = expr
                    else {
                        return None;
                    };
                    if let Some(projection_table) = table_name_expr.as_deref() {
                        if !identifiers_equal(projection_table, table_name)
                            && !identifiers_equal(projection_table, binding_name)
                        {
                            return None;
                        }
                    }
                    let column_index = table_schema
                        .columns
                        .iter()
                        .position(|candidate| identifiers_equal(&candidate.name, column))?;
                    projection_indexes.push(column_index);
                    column_names.push(select_alias.clone().unwrap_or_else(|| column.clone()));
                }
                SelectItem::Wildcard => {
                    for (column_index, column) in table_schema.columns.iter().enumerate() {
                        projection_indexes.push(column_index);
                        column_names.push(column.name.clone());
                    }
                }
                SelectItem::QualifiedWildcard(qualified_name) => {
                    if !identifiers_equal(qualified_name, table_name)
                        && !identifiers_equal(qualified_name, binding_name)
                    {
                        return None;
                    }
                    for (column_index, column) in table_schema.columns.iter().enumerate() {
                        projection_indexes.push(column_index);
                        column_names.push(column.name.clone());
                    }
                }
            }
        }

        Some((projection_indexes, column_names))
    }
    pub(crate) fn simple_projection_order_by_plan(
        &self,
        query: &Query,
        table_schema: &TableSchema,
        table_name: &str,
        binding_name: &str,
        projection_indexes: &[usize],
    ) -> Result<Option<Vec<SimpleOrderByPlan>>> {
        if query.order_by.is_empty() {
            return Ok(None);
        }
        let order_by = query
            .order_by
            .iter()
            .map(|entry| {
                let Expr::Column {
                    table: order_table,
                    column: order_column,
                } = &entry.expr
                else {
                    return None;
                };
                if let Some(order_table) = order_table.as_deref() {
                    if !identifiers_equal(order_table, table_name)
                        && !identifiers_equal(order_table, binding_name)
                    {
                        return None;
                    }
                }
                let order_projection_index =
                    projection_indexes.iter().position(|projection_index| {
                        table_schema.columns[*projection_index]
                            .name
                            .as_str()
                            .eq_ignore_ascii_case(order_column)
                    })?;
                Some(SimpleOrderByPlan {
                    projection_index: order_projection_index,
                    descending: entry.descending,
                    collation: entry.collation.clone(),
                })
            })
            .collect::<Option<Vec<_>>>();
        Ok(order_by)
    }
    fn simple_grouped_order_by_plan(
        &self,
        query: &Query,
        select: &Select,
        table_name: &str,
        binding_name: &str,
        column_names: &[String],
    ) -> Result<Option<Vec<SimpleOrderByPlan>>> {
        if query.order_by.is_empty() {
            return Ok(None);
        }
        let order_by = query
            .order_by
            .iter()
            .map(|entry| {
                let Expr::Column {
                    table: order_table,
                    column: order_column,
                } = &entry.expr
                else {
                    return None;
                };
                let projection_index = if let Some(order_table) = order_table.as_deref() {
                    if !identifiers_equal(order_table, table_name)
                        && !identifiers_equal(order_table, binding_name)
                    {
                        return None;
                    }
                    select
                        .group_by
                        .iter()
                        .enumerate()
                        .find_map(|(index, expr)| match expr {
                            Expr::Column { column, .. }
                                if identifiers_equal(column, order_column) =>
                            {
                                Some(index)
                            }
                            _ => None,
                        })
                } else {
                    column_names
                        .iter()
                        .position(|candidate| candidate.eq_ignore_ascii_case(order_column))
                        .or_else(|| {
                            select.group_by.iter().enumerate().find_map(
                                |(index, expr)| match expr {
                                    Expr::Column { column, .. }
                                        if identifiers_equal(column, order_column) =>
                                    {
                                        Some(index)
                                    }
                                    _ => None,
                                },
                            )
                        })
                }?;
                Some(SimpleOrderByPlan {
                    projection_index,
                    descending: entry.descending,
                    collation: entry.collation.clone(),
                })
            })
            .collect::<Option<Vec<_>>>();
        Ok(order_by)
    }
    #[allow(clippy::too_many_arguments)]
    fn rewrite_simple_grouped_having_expr(
        &self,
        expr: &Expr,
        select: &Select,
        table_name: &str,
        binding_name: &str,
        column_names: &[String],
        synthetic_names: &[String],
        count_projection_index: usize,
        sum_projection: Option<(&str, usize)>,
    ) -> Result<Option<Expr>> {
        let mut aggregate_bindings = vec![SimpleGroupedNumericAggregateBinding {
            kind: SimpleGroupedNumericAggregateKind::CountRows,
            projection_index: count_projection_index,
            source_column_name: None,
            source_column_index: None,
            source_expr: None,
        }];
        if let Some((sum_column_name, sum_projection_index)) = sum_projection {
            aggregate_bindings.push(SimpleGroupedNumericAggregateBinding {
                kind: SimpleGroupedNumericAggregateKind::Sum,
                projection_index: sum_projection_index,
                source_column_name: Some(sum_column_name.to_string()),
                source_column_index: None,
                source_expr: None,
            });
        }
        self.rewrite_simple_grouped_having_expr_with_bindings(
            expr,
            select,
            table_name,
            binding_name,
            column_names,
            synthetic_names,
            &aggregate_bindings,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn rewrite_simple_grouped_having_expr_with_bindings(
        &self,
        expr: &Expr,
        select: &Select,
        table_name: &str,
        binding_name: &str,
        column_names: &[String],
        synthetic_names: &[String],
        aggregate_bindings: &[SimpleGroupedNumericAggregateBinding],
    ) -> Result<Option<Expr>> {
        let rewritten = match expr {
            Expr::Literal(_) | Expr::Parameter(_) => expr.clone(),
            Expr::Column { table, column } => {
                let Some(column_name) = simple_grouped_having_column_name(
                    select,
                    table_name,
                    binding_name,
                    column_names,
                    synthetic_names,
                    table.as_deref(),
                    column,
                ) else {
                    return Ok(None);
                };
                Expr::Column {
                    table: None,
                    column: column_name,
                }
            }
            Expr::Unary { op, expr } => Expr::Unary {
                op: *op,
                expr: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        expr,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
            },
            Expr::Binary { left, op, right } => Expr::Binary {
                left: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        left,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
                op: *op,
                right: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        right,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
            },
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        expr,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
                low: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        low,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
                high: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        high,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
                negated: *negated,
            },
            Expr::InList {
                expr,
                items,
                negated,
            } => Expr::InList {
                expr: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        expr,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
                items: {
                    let mut rewritten_items = Vec::with_capacity(items.len());
                    for item in items {
                        let Some(item) = self.rewrite_simple_grouped_having_expr_with_bindings(
                            item,
                            select,
                            table_name,
                            binding_name,
                            column_names,
                            synthetic_names,
                            aggregate_bindings,
                        )?
                        else {
                            return Ok(None);
                        };
                        rewritten_items.push(item);
                    }
                    rewritten_items
                },
                negated: *negated,
            },
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Expr::Like {
                expr: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        expr,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
                pattern: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        pattern,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
                escape: match escape {
                    Some(escape) => Some(Box::new(
                        match self.rewrite_simple_grouped_having_expr_with_bindings(
                            escape,
                            select,
                            table_name,
                            binding_name,
                            column_names,
                            synthetic_names,
                            aggregate_bindings,
                        )? {
                            Some(expr) => expr,
                            None => return Ok(None),
                        },
                    )),
                    None => None,
                },
                case_insensitive: *case_insensitive,
                negated: *negated,
            },
            Expr::IsNull { expr, negated } => Expr::IsNull {
                expr: Box::new(
                    match self.rewrite_simple_grouped_having_expr_with_bindings(
                        expr,
                        select,
                        table_name,
                        binding_name,
                        column_names,
                        synthetic_names,
                        aggregate_bindings,
                    )? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    },
                ),
                negated: *negated,
            },
            Expr::Function { name, .. }
                if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") =>
            {
                return Ok(None)
            }
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: {
                    let mut rewritten_args = Vec::with_capacity(args.len());
                    for arg in args {
                        let Some(arg) = self.rewrite_simple_grouped_having_expr_with_bindings(
                            arg,
                            select,
                            table_name,
                            binding_name,
                            column_names,
                            synthetic_names,
                            aggregate_bindings,
                        )?
                        else {
                            return Ok(None);
                        };
                        rewritten_args.push(arg);
                    }
                    rewritten_args
                },
            },
            Expr::Aggregate { .. } => {
                let Some(binding) = matching_simple_grouped_aggregate_binding(
                    expr,
                    table_name,
                    binding_name,
                    aggregate_bindings,
                ) else {
                    return Ok(None);
                };
                Expr::Column {
                    table: None,
                    column: synthetic_names[binding.projection_index].clone(),
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(rewritten))
    }
    pub(crate) fn try_execute_simple_integer_series_query(query: &Query) -> Option<QueryResult> {
        if !query.recursive
            || query.ctes.len() != 1
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return None;
        }
        let cte = query.ctes.first()?;
        let (column_name, start, step, upper_exclusive) = Self::simple_integer_series_bounds(cte)?;

        let QueryBody::Select(select) = &query.body else {
            return None;
        };
        if select.from.len() != 1
            || select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
        {
            return None;
        }
        let [FromItem::Table { name, alias }] = select.from.as_slice() else {
            return None;
        };
        if !identifiers_equal(name, &cte.name) {
            return None;
        }
        let binding_name = alias.as_deref().unwrap_or(name);
        let [SelectItem::Expr { expr, alias }] = select.projection.as_slice() else {
            return None;
        };
        if !Self::simple_integer_series_column_ref(expr, binding_name, &column_name) {
            return None;
        }

        let max_rows = upper_exclusive
            .checked_sub(start)?
            .checked_div(step)?
            .checked_add(1)?;
        let capacity = usize::try_from(max_rows)
            .ok()
            .filter(|rows| *rows <= RECURSIVE_CTE_MAX_ITERATIONS)?;
        let mut rows = Vec::with_capacity(capacity);
        let mut value = start;
        rows.push(QueryRow::new(vec![Value::Int64(value)]));
        while value < upper_exclusive {
            value = value.checked_add(step)?;
            rows.push(QueryRow::new(vec![Value::Int64(value)]));
            if rows.len() > RECURSIVE_CTE_MAX_ITERATIONS {
                return None;
            }
        }

        Some(QueryResult::with_rows(
            vec![alias.clone().unwrap_or(column_name)],
            rows,
        ))
    }
    pub(crate) fn simple_integer_series_bounds(
        cte: &CommonTableExpr,
    ) -> Option<(String, i64, i64, i64)> {
        if cte.column_names.len() != 1
            || !cte.query.recursive
            || !cte.query.order_by.is_empty()
            || cte.query.limit.is_some()
            || cte.query.offset.is_some()
        {
            return None;
        }
        let QueryBody::SetOperation {
            op: crate::sql::ast::SetOperation::Union,
            all: true,
            left,
            right,
        } = &cte.query.body
        else {
            return None;
        };

        let start = Self::simple_integer_series_anchor(left)?;
        let (step, upper_exclusive) =
            Self::simple_integer_series_recursive_term(right, &cte.name, &cte.column_names[0])?;
        if step <= 0 {
            return None;
        }
        Some((cte.column_names[0].clone(), start, step, upper_exclusive))
    }
    fn simple_integer_series_anchor(body: &QueryBody) -> Option<i64> {
        let QueryBody::Select(select) = body else {
            return None;
        };
        if !select.from.is_empty()
            || select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
        {
            return None;
        }
        let [SelectItem::Expr { expr, .. }] = select.projection.as_slice() else {
            return None;
        };
        let Expr::Literal(Value::Int64(value)) = expr else {
            return None;
        };
        Some(*value)
    }
    fn simple_integer_series_recursive_term(
        body: &QueryBody,
        cte_name: &str,
        column_name: &str,
    ) -> Option<(i64, i64)> {
        let QueryBody::Select(select) = body else {
            return None;
        };
        if select.from.len() != 1
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
        {
            return None;
        }
        let [FromItem::Table { name, alias }] = select.from.as_slice() else {
            return None;
        };
        if !identifiers_equal(name, cte_name) {
            return None;
        }
        let binding_name = alias.as_deref().unwrap_or(name);

        let [SelectItem::Expr { expr, .. }] = select.projection.as_slice() else {
            return None;
        };
        let step = match expr {
            Expr::Binary { left, op, right } if *op == BinaryOp::Add => {
                if Self::simple_integer_series_column_ref(left, binding_name, column_name) {
                    Self::simple_int64_literal(right)?
                } else if Self::simple_integer_series_column_ref(right, binding_name, column_name) {
                    Self::simple_int64_literal(left)?
                } else {
                    return None;
                }
            }
            _ => return None,
        };

        let filter = select.filter.as_ref()?;
        let upper_exclusive = match filter {
            Expr::Binary { left, op, right } if *op == BinaryOp::Lt => {
                if !Self::simple_integer_series_column_ref(left, binding_name, column_name) {
                    return None;
                }
                Self::simple_int64_literal(right)?
            }
            _ => return None,
        };
        Some((step, upper_exclusive))
    }
    fn simple_integer_series_column_ref(expr: &Expr, table_name: &str, column_name: &str) -> bool {
        matches!(
            expr,
            Expr::Column { table, column }
                if identifiers_equal(column, column_name)
                    && table
                        .as_deref()
                        .is_none_or(|candidate| identifiers_equal(candidate, table_name))
        )
    }
    fn simple_int64_literal(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Literal(Value::Int64(value)) => Some(*value),
            _ => None,
        }
    }
}
