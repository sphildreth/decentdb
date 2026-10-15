//! Thematic extraction (mechanical split; no behavior change).

use super::*;

/// Resolves the projected position of a join step's previous (outer) column.
/// Returns `None` when the column is the previous table's row-id alias, in
/// which case the join key is available directly from the row's `row_id` and is
/// not present in the projection.
pub(crate) fn join_key_projection_index(
    step: &DeferredViewJoinStep,
    previous_table_projection: &DeferredViewTableProjection,
) -> Result<Option<usize>> {
    if step.previous_is_rowid_alias {
        return Ok(None);
    }
    previous_table_projection
        .position(step.previous_column_index)
        .map(Some)
        .ok_or_else(|| {
            DbError::internal(
                "deferred view linear join projection is missing required join column",
            )
        })
}

pub(crate) fn join_constraint_matches_columns(
    on: &Expr,
    left_binding: TableBindingRef<'_>,
    left_column: &str,
    right_binding: TableBindingRef<'_>,
    right_column: &str,
) -> bool {
    let Some((left_ref, right_ref)) = simple_join_equality(on) else {
        return false;
    };
    (matches_table_binding(left_binding, left_ref.table)
        && identifiers_equal(left_ref.column, left_column)
        && matches_table_binding(right_binding, right_ref.table)
        && identifiers_equal(right_ref.column, right_column))
        || (matches_table_binding(left_binding, right_ref.table)
            && identifiers_equal(right_ref.column, left_column)
            && matches_table_binding(right_binding, left_ref.table)
            && identifiers_equal(left_ref.column, right_column))
}

pub(crate) fn join_constraints_match_columns(
    constraints: &[&Expr],
    left_binding: TableBindingRef<'_>,
    left_column: &str,
    right_binding: TableBindingRef<'_>,
    right_column: &str,
) -> bool {
    constraints.iter().any(|constraint| {
        simple_join_equalities(constraint).is_some_and(|equalities| {
            equalities.iter().any(|(left_ref, right_ref)| {
                (matches_table_binding(left_binding, left_ref.table)
                    && identifiers_equal(left_ref.column, left_column)
                    && matches_table_binding(right_binding, right_ref.table)
                    && identifiers_equal(right_ref.column, right_column))
                    || (matches_table_binding(left_binding, right_ref.table)
                        && identifiers_equal(right_ref.column, left_column)
                        && matches_table_binding(right_binding, left_ref.table)
                        && identifiers_equal(left_ref.column, right_column))
            })
        })
    })
}

pub(crate) fn join_output_columns(
    left: &Dataset,
    right: &Dataset,
    using_columns: &[JoinUsingColumn],
) -> Vec<ColumnBinding> {
    if using_columns.is_empty() {
        let mut columns = left.columns.clone();
        columns.extend(right.columns.clone());
        return columns;
    }

    let left_hidden = using_columns
        .iter()
        .map(|column| column.left_index)
        .collect::<BTreeSet<_>>();
    let right_hidden = using_columns
        .iter()
        .map(|column| column.right_index)
        .collect::<BTreeSet<_>>();

    let mut columns =
        Vec::with_capacity(using_columns.len() + left.columns.len() + right.columns.len());
    for column in using_columns {
        columns.push(ColumnBinding::visible(None, column.name.clone()));
    }
    for (index, binding) in left.columns.iter().enumerate() {
        let mut binding = binding.clone();
        if left_hidden.contains(&index) {
            binding.hidden = true;
        }
        columns.push(binding);
    }
    for (index, binding) in right.columns.iter().enumerate() {
        let mut binding = binding.clone();
        if right_hidden.contains(&index) {
            binding.hidden = true;
        }
        columns.push(binding);
    }
    columns
}

pub(crate) fn merged_join_value(left: &Value, right: &Value) -> Value {
    if matches!(left, Value::Null) {
        right.clone()
    } else {
        left.clone()
    }
}

pub(crate) fn join_output_row(
    left_row: &[Value],
    right_row: &[Value],
    using_columns: &[JoinUsingColumn],
) -> Result<Vec<Value>> {
    if using_columns.is_empty() {
        let mut row = left_row.to_vec();
        row.extend_from_slice(right_row);
        return Ok(row);
    }

    let mut row = Vec::with_capacity(using_columns.len() + left_row.len() + right_row.len());
    for column in using_columns {
        let left_value = left_row
            .get(column.left_index)
            .ok_or_else(|| DbError::internal("left join row is shorter than its bindings"))?;
        let right_value = right_row
            .get(column.right_index)
            .ok_or_else(|| DbError::internal("right join row is shorter than its bindings"))?;
        row.push(merged_join_value(left_value, right_value));
    }
    row.extend_from_slice(left_row);
    row.extend_from_slice(right_row);
    Ok(row)
}

pub(crate) fn join_rows_match(
    constraint: &JoinConstraint,
    using_columns: &[JoinUsingColumn],
    eval_row: &[Value],
    left_row: &[Value],
    right_row: &[Value],
    context: &JoinEvalContext<'_>,
) -> Result<bool> {
    match constraint {
        JoinConstraint::On(on) => Ok(matches!(
            context.runtime.eval_expr(
                on,
                context.dataset,
                eval_row,
                context.params,
                context.ctes,
                None
            )?,
            Value::Bool(true)
        )),
        JoinConstraint::Using(_) | JoinConstraint::Natural => {
            for column in using_columns {
                let left_value = left_row.get(column.left_index).ok_or_else(|| {
                    DbError::internal("left join row is shorter than its bindings")
                })?;
                let right_value = right_row.get(column.right_index).ok_or_else(|| {
                    DbError::internal("right join row is shorter than its bindings")
                })?;
                if matches!(left_value, Value::Null) || matches!(right_value, Value::Null) {
                    return Ok(false);
                }
                if compare_values(left_value, right_value)? != std::cmp::Ordering::Equal {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

pub(crate) fn nested_loop_join(
    left: Dataset,
    right: Dataset,
    kind: JoinKind,
    constraint: &JoinConstraint,
    runtime: &EngineRuntime,
    params: &[Value],
    ctes: &BTreeMap<String, Dataset>,
) -> Result<Dataset> {
    let using_columns = resolve_join_using_columns(&left, &right, constraint)?;

    let mut eval_columns = left.columns.clone();
    eval_columns.extend(right.columns.clone());
    let eval_dataset = Dataset::with_rows(eval_columns, Vec::new());
    let eval_context = JoinEvalContext {
        dataset: &eval_dataset,
        runtime,
        params,
        ctes,
    };
    let columns = join_output_columns(&left, &right, &using_columns);
    let mut rows = Vec::new();
    let mut matched_right = vec![false; right.rows.len()];
    let left_nulls = vec![Value::Null; left.columns.len()];
    let right_nulls = vec![Value::Null; right.columns.len()];
    for left_row in left.rows.iter() {
        let mut matched = false;
        for (right_index, right_row) in right.rows.iter().enumerate() {
            let mut eval_row = left_row.clone();
            eval_row.extend(right_row.clone());
            if join_rows_match(
                constraint,
                &using_columns,
                &eval_row,
                left_row,
                right_row,
                &eval_context,
            )? {
                matched = true;
                matched_right[right_index] = true;
                rows.push(join_output_row(left_row, right_row, &using_columns)?);
            }
        }
        if !matched && matches!(kind, JoinKind::Left | JoinKind::Full) {
            rows.push(join_output_row(left_row, &right_nulls, &using_columns)?);
        }
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        for (matched, right_row) in matched_right.iter().zip(right.rows.iter()) {
            if !matched {
                rows.push(join_output_row(&left_nulls, right_row, &using_columns)?);
            }
        }
    }
    Ok(Dataset::with_rows(columns, rows))
}

impl EngineRuntime {
    pub(crate) fn try_execute_left_join_aggregate_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_left_join_aggregate_query(query, params)? else {
            return Ok(None);
        };
        let Some(parent_source) = self.visible_table_row_source(plan.parent_table_name) else {
            return Ok(None);
        };
        let Some(child_source) = self.visible_table_row_source(plan.child_table_name) else {
            return Ok(None);
        };
        let child_index_keys =
            plan.child_index_name
                .as_deref()
                .and_then(|index_name| match self.index(index_name) {
                    Some(RuntimeIndex::Btree { keys, .. }) => Some(keys),
                    _ => None,
                });

        if child_index_keys.is_none() {
            return Ok(None);
        }
        let keys = child_index_keys.unwrap();

        let bounded_order = plan
            .order_by
            .as_deref()
            .zip(plan.limit)
            .filter(|(_, _)| plan.offset == 0);
        let mut rows = Vec::new();

        for parent_row in parent_source.rows() {
            let parent_row = parent_row?;
            let parent_values = parent_row.values();
            let Some(join_value) = parent_values.get(plan.parent_join_index) else {
                return Err(DbError::internal("parent join row is shorter than schema"));
            };

            let mut state = IndexedJoinAggregateState::new(&plan.aggregate_kinds);
            let mut matched_child = false;

            if !matches!(join_value, Value::Null) {
                let child_row_ids = keys.row_ids_for_value_set(join_value)?;
                match child_row_ids {
                    RuntimeRowIdSet::Empty => {}
                    RuntimeRowIdSet::Single(child_row_id) => {
                        let Some(child_row) = child_source.row_by_id(child_row_id)? else {
                            return Err(DbError::internal("child index referenced missing row id"));
                        };
                        matched_child = true;
                        state.accumulate(child_row.values())?;
                    }
                    RuntimeRowIdSet::Contiguous { start, len } => {
                        for child_row_id in contiguous_row_ids(start, len) {
                            let Some(child_row) = child_source.row_by_id(child_row_id)? else {
                                return Err(DbError::internal(
                                    "child index referenced missing row id",
                                ));
                            };
                            matched_child = true;
                            state.accumulate(child_row.values())?;
                        }
                    }
                    RuntimeRowIdSet::Many(row_ids) => {
                        for child_row_id in row_ids {
                            let Some(child_row) = child_source.row_by_id(*child_row_id)? else {
                                return Err(DbError::internal(
                                    "child index referenced missing row id",
                                ));
                            };
                            matched_child = true;
                            state.accumulate(child_row.values())?;
                        }
                    }
                    RuntimeRowIdSet::Owned(row_ids) => {
                        for child_row_id in row_ids {
                            let Some(child_row) = child_source.row_by_id(child_row_id)? else {
                                return Err(DbError::internal(
                                    "child index referenced missing row id",
                                ));
                            };
                            matched_child = true;
                            state.accumulate(child_row.values())?;
                        }
                    }
                }
            }

            if !matched_child && !plan.include_empty_parent {
                continue;
            }

            let mut output =
                Vec::with_capacity(plan.group_column_indexes.len() + plan.aggregate_kinds.len());
            for index in &plan.group_column_indexes {
                output.push(parent_values[*index].clone());
            }
            state.finalize_into(&mut output);
            let row = QueryRow::new(output);

            if let Some((order_by, limit)) = bounded_order {
                push_bounded_projection_ordered_query_row(
                    Some(self),
                    &mut rows,
                    row,
                    order_by,
                    limit,
                )?;
            } else {
                rows.push(row);
            }
        }

        if let Some((order_by, _)) = bounded_order {
            sort_query_rows_by_projection_order(Some(self), &mut rows, order_by)?;
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
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
    pub(crate) fn execute_indexed_join_limit_projection_plan(
        &self,
        plan: &IndexedJoinLimitPlan<'_>,
    ) -> Result<QueryResult> {
        let sources = plan
            .tables
            .iter()
            .map(|table| {
                self.visible_table_row_source(table.name).ok_or_else(|| {
                    DbError::internal(format!("table {} row source is missing", table.name))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = plan
            .steps
            .iter()
            .map(|step| {
                let Some(index_name) = step.right_index_name.as_deref() else {
                    return Ok(None);
                };
                let Some(RuntimeIndex::Btree { keys, .. }) = self.index(index_name) else {
                    return Err(DbError::internal(format!(
                        "index {index_name} is missing for indexed join limit plan",
                    )));
                };
                Ok(Some(keys))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut rows = Vec::new();
        let mut offset_remaining = plan.offset;
        let mut limit_remaining = plan.limit;
        for root_row in sources[0].rows() {
            let root_row = root_row?;
            let current0 = root_row.values().to_vec();
            if plan.tables.len() == 2 {
                let step0 = &plan.steps[0];
                let Some(probe_value) = current0.get(step0.previous_column_index) else {
                    return Err(DbError::internal("join probe row is shorter than schema"));
                };
                for row1 in indexed_join_limit_rows_for_value(sources[1], keys[0], probe_value)? {
                    let current = [&current0[..], &row1[..]];
                    if push_indexed_join_limit_projection(
                        &current,
                        &plan.projections,
                        &mut offset_remaining,
                        &mut limit_remaining,
                        &mut rows,
                    ) {
                        return Ok(indexed_join_limit_result(plan, rows));
                    }
                }
            } else {
                let step0 = &plan.steps[0];
                let Some(probe_value0) = current0.get(step0.previous_column_index) else {
                    return Err(DbError::internal("join probe row is shorter than schema"));
                };
                for row1 in indexed_join_limit_rows_for_value(sources[1], keys[0], probe_value0)? {
                    let current01 = [&current0[..], &row1[..]];
                    let step1 = &plan.steps[1];
                    let Some(probe_value1) = current01
                        .get(step1.previous_table_index)
                        .and_then(|row| row.get(step1.previous_column_index))
                    else {
                        return Err(DbError::internal("join probe row is shorter than schema"));
                    };
                    for row2 in
                        indexed_join_limit_rows_for_value(sources[2], keys[1], probe_value1)?
                    {
                        let current = [&current0[..], &row1[..], &row2[..]];
                        if push_indexed_join_limit_projection(
                            &current,
                            &plan.projections,
                            &mut offset_remaining,
                            &mut limit_remaining,
                            &mut rows,
                        ) {
                            return Ok(indexed_join_limit_result(plan, rows));
                        }
                    }
                }
            }
        }
        Ok(indexed_join_limit_result(plan, rows))
    }
    pub(crate) fn execute_ordered_indexed_join_limit_projection_plan(
        &self,
        plan: &IndexedJoinLimitPlan<'_>,
        root_filter: Option<&Expr>,
        root_filter_columns: Option<Vec<ColumnBinding>>,
        order_index_name: &str,
        descending: bool,
        params: &[Value],
    ) -> Result<QueryResult> {
        let sources = plan
            .tables
            .iter()
            .map(|table| {
                self.visible_table_row_source(table.name).ok_or_else(|| {
                    DbError::internal(format!("table {} row source is missing", table.name))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = plan
            .steps
            .iter()
            .map(|step| {
                let Some(index_name) = step.right_index_name.as_deref() else {
                    return Ok(None);
                };
                let Some(RuntimeIndex::Btree { keys, .. }) = self.index(index_name) else {
                    return Err(DbError::internal(format!(
                        "index {index_name} is missing for ordered indexed join limit plan",
                    )));
                };
                Ok(Some(keys))
            })
            .collect::<Result<Vec<_>>>()?;
        let Some(RuntimeIndex::Btree {
            keys: order_keys, ..
        }) = self.index(order_index_name)
        else {
            return Err(DbError::internal(format!(
                "ordered index {order_index_name} is missing for ordered view limit plan",
            )));
        };

        let root_filter_dataset =
            root_filter_columns.map(|columns| Dataset::with_rows(columns, Vec::new()));
        let mut rows = Vec::new();
        let mut offset_remaining = plan.offset;
        let mut limit_remaining = plan.limit;
        let ctes = BTreeMap::new();

        visit_runtime_btree_row_ids_in_order(order_keys, descending, |root_row_id| {
            let Some(root_row) = sources[0].row_by_id(root_row_id)? else {
                return Ok(false);
            };
            if let (Some(filter), Some(dataset)) = (root_filter, root_filter_dataset.as_ref()) {
                if !matches!(
                    self.eval_expr(filter, dataset, root_row.values(), params, &ctes, None)?,
                    Value::Bool(true)
                ) {
                    return Ok(false);
                }
            }

            if plan.tables.len() == 2 {
                let step0 = &plan.steps[0];
                let Some(probe_value) = root_row.values().get(step0.previous_column_index) else {
                    return Err(DbError::internal("join probe row is shorter than schema"));
                };
                for row1_id in indexed_join_row_ids_for_value(keys[0], probe_value)? {
                    let Some(row1) = sources[1].row_by_id(row1_id)? else {
                        continue;
                    };
                    let current = [root_row.values(), row1.values()];
                    if push_indexed_join_limit_projection(
                        &current,
                        &plan.projections,
                        &mut offset_remaining,
                        &mut limit_remaining,
                        &mut rows,
                    ) {
                        return Ok(true);
                    }
                }
            } else {
                let step0 = &plan.steps[0];
                let Some(probe_value0) = root_row.values().get(step0.previous_column_index) else {
                    return Err(DbError::internal("join probe row is shorter than schema"));
                };
                for row1_id in indexed_join_row_ids_for_value(keys[0], probe_value0)? {
                    let Some(row1) = sources[1].row_by_id(row1_id)? else {
                        continue;
                    };
                    let current01 = [root_row.values(), row1.values()];
                    let step1 = &plan.steps[1];
                    let Some(probe_value1) = current01
                        .get(step1.previous_table_index)
                        .and_then(|row| row.get(step1.previous_column_index))
                    else {
                        return Err(DbError::internal("join probe row is shorter than schema"));
                    };
                    for row2_id in indexed_join_row_ids_for_value(keys[1], probe_value1)? {
                        let Some(row2) = sources[2].row_by_id(row2_id)? else {
                            continue;
                        };
                        let current = [root_row.values(), row1.values(), row2.values()];
                        if push_indexed_join_limit_projection(
                            &current,
                            &plan.projections,
                            &mut offset_remaining,
                            &mut limit_remaining,
                            &mut rows,
                        ) {
                            return Ok(true);
                        }
                    }
                }
            }
            Ok(false)
        })?;

        Ok(indexed_join_limit_result(plan, rows))
    }
    pub(crate) fn execute_indexed_join_projection_rows(
        &self,
        plan: &IndexedJoinLimitPlan<'_>,
        enforce_root_rowid_order: bool,
        second_table_order_column: Option<usize>,
    ) -> Result<Vec<QueryRow>> {
        if plan.tables.len() != 3 || plan.steps.len() != 2 {
            return Err(DbError::internal(
                "indexed join projection rows path expects a three-table chain",
            ));
        }
        let sources = plan
            .tables
            .iter()
            .map(|table| {
                self.visible_table_row_source(table.name).ok_or_else(|| {
                    DbError::internal(format!("table {} row source is missing", table.name))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = plan
            .steps
            .iter()
            .map(|step| {
                let Some(index_name) = step.right_index_name.as_deref() else {
                    return Ok(None);
                };
                let Some(RuntimeIndex::Btree { keys, .. }) = self.index(index_name) else {
                    return Err(DbError::internal(format!(
                        "index {index_name} is missing for indexed join projection plan",
                    )));
                };
                Ok(Some(keys))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut root_row_ids = Vec::with_capacity(sources[0].row_count());
        for root_row in sources[0].rows() {
            root_row_ids.push(root_row?.row_id());
        }
        if enforce_root_rowid_order {
            root_row_ids.sort_unstable();
        }

        let mut rows = Vec::new();
        for root_row_id in root_row_ids {
            let Some(root_row) = sources[0].row_by_id(root_row_id)? else {
                continue;
            };
            let step0 = &plan.steps[0];
            let Some(probe_value0) = root_row.values().get(step0.previous_column_index) else {
                return Err(DbError::internal("join probe row is shorter than schema"));
            };
            let mut row1_ids = indexed_join_row_ids_for_value(keys[0], probe_value0)?;
            if let Some(column_index) = second_table_order_column {
                row1_ids = sort_join_row_ids_by_column(sources[1], row1_ids, column_index)?;
            }
            for row1_id in row1_ids {
                let Some(row1) = sources[1].row_by_id(row1_id)? else {
                    continue;
                };
                let current01 = [root_row.values(), row1.values()];
                let step1 = &plan.steps[1];
                let Some(probe_value1) = current01
                    .get(step1.previous_table_index)
                    .and_then(|row| row.get(step1.previous_column_index))
                else {
                    return Err(DbError::internal("join probe row is shorter than schema"));
                };
                for row2_id in indexed_join_row_ids_for_value(keys[1], probe_value1)? {
                    let Some(row2) = sources[2].row_by_id(row2_id)? else {
                        continue;
                    };
                    let current = [root_row.values(), row1.values(), row2.values()];
                    rows.push(project_indexed_join_row(&current, &plan.projections)?);
                }
            }
        }
        Ok(rows)
    }
    pub(crate) fn execute_base_table_join_from_sources(
        &self,
        left_source: VisibleTableRowSource<'_>,
        right_source: VisibleTableRowSource<'_>,
        plan: &BaseTableJoinPlan<'_>,
        params: &[Value],
    ) -> Result<QueryResult> {
        let left_table = self.table_schema(plan.left_name).ok_or_else(|| {
            DbError::internal(format!("table {} not found for join", plan.left_name))
        })?;
        let right_table = self.table_schema(plan.right_name).ok_or_else(|| {
            DbError::internal(format!("table {} not found for join", plan.right_name))
        })?;

        let left_binding_name = plan.left_alias.unwrap_or(plan.left_name);
        let right_binding_name = plan.right_alias.unwrap_or(plan.right_name);

        let left_columns: Vec<ColumnBinding> = left_table
            .columns
            .iter()
            .map(|c| {
                ColumnBinding::visible_source(
                    Some(left_binding_name.to_string()),
                    Some(plan.left_name.to_string()),
                    c.name.clone(),
                )
                .with_type(c.column_type)
            })
            .collect();
        let right_columns: Vec<ColumnBinding> = right_table
            .columns
            .iter()
            .map(|c| {
                ColumnBinding::visible_source(
                    Some(right_binding_name.to_string()),
                    Some(plan.right_name.to_string()),
                    c.name.clone(),
                )
                .with_type(c.column_type)
            })
            .collect();

        let using_columns = resolve_join_using_columns_for_schemas(
            &left_columns,
            &right_columns,
            plan.constraint,
            left_table,
            right_table,
        )?;

        let eval_columns: Vec<ColumnBinding> = left_columns
            .iter()
            .cloned()
            .chain(right_columns.iter().cloned())
            .collect();
        let eval_dataset = Dataset::with_rows(eval_columns, Vec::new());
        let ctes = BTreeMap::new();

        let left_needs_virtual_generated = !generated_columns_are_stored(left_table);
        let right_needs_virtual_generated = !generated_columns_are_stored(right_table);

        let left_nulls = vec![Value::Null; left_columns.len()];
        let right_nulls = vec![Value::Null; right_columns.len()];

        let mut right_rows: Vec<Vec<Value>> = Vec::new();
        for row_result in right_source.rows() {
            let row_ref = row_result?;
            let mut values = row_ref.values().to_vec();
            if right_needs_virtual_generated {
                self.apply_virtual_generated_columns(right_table, &mut values)?;
            }
            right_rows.push(values);
        }

        let mut matched_right = vec![false; right_rows.len()];
        let mut join_output: Vec<Vec<Value>> = Vec::new();

        for left_row_result in left_source.rows() {
            let mut left_values = left_row_result?.values().to_vec();
            if left_needs_virtual_generated {
                self.apply_virtual_generated_columns(left_table, &mut left_values)?;
            }

            let mut matched = false;
            for (right_index, right_values) in right_rows.iter().enumerate() {
                let mut eval_row = left_values.clone();
                eval_row.extend(right_values.clone());
                if join_rows_match(
                    plan.constraint,
                    &using_columns,
                    &eval_row,
                    &left_values,
                    right_values,
                    &JoinEvalContext {
                        dataset: &eval_dataset,
                        runtime: self,
                        params,
                        ctes: &ctes,
                    },
                )? {
                    matched = true;
                    matched_right[right_index] = true;
                    join_output.push(join_output_row(&left_values, right_values, &using_columns)?);
                }
            }
            if !matched && matches!(plan.kind, JoinKind::Left | JoinKind::Full) {
                join_output.push(join_output_row(&left_values, &right_nulls, &using_columns)?);
            }
        }
        if matches!(plan.kind, JoinKind::Right | JoinKind::Full) {
            for (matched, right_values) in matched_right.iter().zip(right_rows.iter()) {
                if !matched {
                    join_output.push(join_output_row(&left_nulls, right_values, &using_columns)?);
                }
            }
        }

        let result_columns: Vec<ColumnBinding> = plan
            .projection
            .iter()
            .enumerate()
            .map(|(index, item)| match item {
                SelectItem::Expr { expr, alias } => ColumnBinding::visible(
                    None,
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                ),
                _ => ColumnBinding::visible(None, format!("col{}", index + 1)),
            })
            .collect();

        let join_ds_columns: Vec<ColumnBinding> = left_columns
            .iter()
            .cloned()
            .chain(right_columns.iter().cloned())
            .collect();
        let mut join_dataset = Dataset::with_rows(join_ds_columns, join_output);
        if let Some(filter) = plan.filter {
            let filter_ds = Dataset::with_rows(join_dataset.columns.clone(), Vec::new());
            let mut filtered = Vec::with_capacity(join_dataset.rows.len());
            for row in join_dataset.take_rows() {
                let val = self.eval_expr(filter, &filter_ds, &row, params, &ctes, None)?;
                if matches!(val, Value::Bool(true)) {
                    filtered.push(row);
                }
            }
            join_dataset.set_rows(filtered);
        }

        let mut output_rows: Vec<Vec<Value>> = Vec::new();
        for row in join_dataset.rows.iter() {
            let mut projected = Vec::with_capacity(plan.projection.len());
            for item in plan.projection {
                let SelectItem::Expr { expr, .. } = item else {
                    return Err(DbError::sql(
                        "wildcards not supported in join SELECT output",
                    ));
                };
                projected.push(self.eval_expr(expr, &join_dataset, row, params, &ctes, None)?);
            }
            output_rows.push(projected);
        }

        let has_order_by = !plan.order_by.is_empty();
        let mut rows_with_order: Vec<(Vec<Value>, Vec<Value>)> = if has_order_by {
            let order_ds = Dataset::with_rows(result_columns.clone(), output_rows.clone());
            output_rows
                .into_iter()
                .map(|row| {
                    let order_values: Vec<Value> = plan
                        .order_by
                        .iter()
                        .map(|order| {
                            self.eval_expr(&order.expr, &order_ds, &row, params, &ctes, None)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok((row, order_values))
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            output_rows
                .into_iter()
                .map(|row| (row, Vec::new()))
                .collect()
        };

        if has_order_by {
            let mut sort_error = None;
            rows_with_order.sort_by(|(_, left_order), (_, right_order)| {
                match compare_query_row_order_values(
                    Some(self),
                    left_order,
                    right_order,
                    plan.order_by,
                ) {
                    Ok(ordering) => ordering,
                    Err(error) => {
                        if sort_error.is_none() {
                            sort_error = Some(error);
                        }
                        std::cmp::Ordering::Equal
                    }
                }
            });
            if let Some(error) = sort_error {
                return Err(error);
            }
        }

        let mut rows: Vec<QueryRow> = if plan.distinct {
            let mut seen = BTreeSet::new();
            let mut distinct_rows = Vec::new();
            for (output, _) in rows_with_order {
                if seen.insert(row_identity(&output)?) {
                    distinct_rows.push(QueryRow::new(output));
                }
            }
            distinct_rows
        } else {
            rows_with_order
                .into_iter()
                .map(|(output, _)| QueryRow::new(output))
                .collect()
        };

        let offset_val = plan
            .offset
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .unwrap_or(0);
        let limit_val = plan
            .limit
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?;

        let start = usize::try_from(offset_val.max(0)).unwrap_or(usize::MAX);
        if start > 0 || limit_val.is_some() {
            let take = limit_val
                .map(|l| usize::try_from(l.max(0)).unwrap_or(0))
                .unwrap_or(usize::MAX);
            rows = rows.into_iter().skip(start).take(take).collect();
        }

        let column_names: Vec<String> = result_columns.into_iter().map(|c| c.name).collect();
        Ok(QueryResult::with_rows(column_names, rows))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn stream_deferred_view_join_rows_from_root<S: PageStore, F>(
        &self,
        store: &S,
        table_row_readers: &[DeferredViewTableRowReader<'_>],
        join_steps: &[DeferredViewJoinStep],
        join_keys: &[&RuntimeBtreeKeys],
        key_projection_indexes: &[Option<usize>],
        table_projections: &[DeferredViewTableProjection],
        root_row: StoredRow,
        partial_rows: &mut Vec<StoredRow>,
        use_persistent_pk_index: bool,
        require_index: bool,
        chunk_payload_cache: &mut HashMap<CachedPagedChunkPayloadKey, Arc<Vec<u8>>>,
        visit: &mut F,
    ) -> Result<Option<bool>>
    where
        F: FnMut(&[StoredRow]) -> Result<bool>,
    {
        #[allow(clippy::too_many_arguments)]
        fn walk_join_rows<S: PageStore, F>(
            store: &S,
            table_row_readers: &[DeferredViewTableRowReader<'_>],
            join_steps: &[DeferredViewJoinStep],
            table_projections: &[DeferredViewTableProjection],
            join_keys: &[&RuntimeBtreeKeys],
            key_projection_indexes: &[Option<usize>],
            step_index: usize,
            partial_rows: &mut Vec<StoredRow>,
            use_persistent_pk_index: bool,
            chunk_payload_cache: &mut HashMap<CachedPagedChunkPayloadKey, Arc<Vec<u8>>>,
            visit: &mut F,
        ) -> Result<Option<bool>>
        where
            F: FnMut(&[StoredRow]) -> Result<bool>,
        {
            if step_index == join_steps.len() {
                return Ok(Some(visit(partial_rows.as_slice())?));
            }

            let step = &join_steps[step_index];
            let keys = join_keys[step_index];
            let Some(previous_row) = partial_rows.get(step.previous_table_index) else {
                return Err(DbError::internal(
                    "deferred view limit join row is shorter than the planned schema",
                ));
            };
            let current_table_index = step.current_table_index;
            let current_projection_indexes =
                &table_projections[current_table_index].projection_indexes;

            let mut outcome = None;
            let row_ids = match key_projection_indexes[step_index] {
                Some(projection_index) => {
                    let Some(key_value) = previous_row.values.get(projection_index) else {
                        return Err(DbError::internal(
                            "deferred view join row is shorter than planned schema",
                        ));
                    };
                    if matches!(key_value, Value::Null) {
                        return Ok(Some(false));
                    }
                    keys.row_ids_for_value_set(key_value)?
                }
                None => keys.row_ids_for_row_id(previous_row.row_id),
            };

            let mut visit_row_id = |row_id| -> Result<Option<bool>> {
                if partial_rows.len() != step.current_table_index {
                    return Err(DbError::internal(
                        "deferred view join row is not in expected table order",
                    ));
                }

                let Some(joined_row) = table_row_readers[current_table_index]
                    .read_projected_with_chunk_cache(
                        store,
                        row_id,
                        use_persistent_pk_index,
                        current_projection_indexes,
                        chunk_payload_cache,
                    )?
                else {
                    return Ok(Some(false));
                };
                partial_rows.push(joined_row);
                let child_outcome = walk_join_rows(
                    store,
                    table_row_readers,
                    join_steps,
                    table_projections,
                    join_keys,
                    key_projection_indexes,
                    step_index + 1,
                    partial_rows,
                    use_persistent_pk_index,
                    chunk_payload_cache,
                    visit,
                )?;
                partial_rows.pop();
                Ok(child_outcome)
            };

            match row_ids {
                RuntimeRowIdSet::Empty => {}
                RuntimeRowIdSet::Single(row_id) => match visit_row_id(row_id)? {
                    Some(false) => {}
                    Some(true) => return Ok(Some(true)),
                    None => return Ok(None),
                },
                RuntimeRowIdSet::Contiguous { start, len } => {
                    for row_id in contiguous_row_ids(start, len) {
                        if outcome.is_some() {
                            break;
                        }
                        match visit_row_id(row_id)? {
                            Some(false) => {}
                            Some(true) => return Ok(Some(true)),
                            None => {
                                outcome = Some(None);
                                break;
                            }
                        }
                    }
                }
                RuntimeRowIdSet::Many(row_ids) => {
                    for row_id in row_ids {
                        if outcome.is_some() {
                            break;
                        }
                        match visit_row_id(*row_id)? {
                            Some(false) => {}
                            Some(true) => return Ok(Some(true)),
                            None => {
                                outcome = Some(None);
                                break;
                            }
                        }
                    }
                }
                RuntimeRowIdSet::Owned(row_ids) => {
                    for row_id in row_ids {
                        if outcome.is_some() {
                            break;
                        }
                        match visit_row_id(row_id)? {
                            Some(false) => {}
                            Some(true) => return Ok(Some(true)),
                            None => {
                                outcome = Some(None);
                                break;
                            }
                        }
                    }
                }
            }

            Ok(outcome.unwrap_or(Some(false)))
        }

        if join_keys.len() != join_steps.len() || key_projection_indexes.len() != join_steps.len() {
            if !require_index {
                return Ok(None);
            }
            return Err(DbError::internal(
                "deferred view join metadata is missing while executing index-required join",
            ));
        }
        partial_rows.clear();
        partial_rows.push(root_row);
        let result = walk_join_rows(
            store,
            table_row_readers,
            join_steps,
            table_projections,
            join_keys,
            key_projection_indexes,
            0,
            partial_rows,
            use_persistent_pk_index,
            chunk_payload_cache,
            visit,
        );
        partial_rows.clear();
        result
    }
    pub(crate) fn try_indexed_scan(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let Some(filter) = &select.filter else {
            return Ok(None);
        };
        if select.from.len() != 1 {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if ctes.contains_key(name)
            || self
                .visible_view(name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(name)
        {
            return Ok(None);
        }
        let Some(table) = self.table_schema(name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table) {
            return Ok(None);
        }
        let row_source = self.table_row_source(name);

        if let Some(fulltext_lookup) = simple_fulltext_lookup(filter) {
            let index_value = self.eval_expr(
                fulltext_lookup.index_name_expr,
                &Dataset::empty(),
                &[],
                params,
                ctes,
                None,
            )?;
            let query_value = self.eval_expr(
                fulltext_lookup.query_expr,
                &Dataset::empty(),
                &[],
                params,
                ctes,
                None,
            )?;
            let Some(index_name) = expect_text_arg("FULLTEXT_MATCH", "first", &index_value)? else {
                return Ok(Some(Dataset::with_rows(
                    table_bindings_with_hidden_row_id(table, alias.as_deref().unwrap_or(name)),
                    Vec::new(),
                )));
            };
            let Some(query_text) = expect_text_arg("FULLTEXT_MATCH", "second", &query_value)?
            else {
                return Ok(Some(Dataset::with_rows(
                    table_bindings_with_hidden_row_id(table, alias.as_deref().unwrap_or(name)),
                    Vec::new(),
                )));
            };
            if let Some(index_schema) = self.catalog.index(index_name) {
                if identifiers_equal(&index_schema.table_name, name)
                    && index_schema.fresh
                    && index_schema.kind == IndexKind::FullText
                {
                    if let Some(RuntimeIndex::FullText { index }) = self.index(&index_schema.name) {
                        let row_ids = index
                            .search(query_text)
                            .map_err(|error| DbError::sql(error.message))?
                            .into_iter()
                            .filter_map(|hit| i64::try_from(hit.row_id).ok())
                            .collect::<Vec<_>>();
                        return self
                            .dataset_from_row_id_set(
                                table,
                                row_source,
                                alias,
                                RuntimeRowIdSet::Many(&row_ids),
                                true,
                            )
                            .map(Some);
                    }
                }
            }
        }

        if let Some(spatial_lookup) = simple_spatial_lookup(filter) {
            if !matches_filter_binding(name, alias, spatial_lookup.table_qualifier) {
                return Ok(None);
            }
            if let Some(index) = self.catalog.indexes.values().find(|index| {
                identifiers_equal(&index.table_name, name)
                    && index.fresh
                    && index.kind == IndexKind::Spatial
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|index_column| {
                            identifiers_equal(index_column, spatial_lookup.column_name)
                        })
                    && index.columns[0].expression_sql.is_none()
            }) {
                let query_value = self.eval_expr(
                    spatial_lookup.value_expr,
                    &Dataset::empty(),
                    &[],
                    params,
                    ctes,
                    None,
                )?;
                let Some((query_is_geography, query_spatial)) =
                    spatial_value_from_db(&query_value)?
                else {
                    return Ok(None);
                };
                let Some(RuntimeIndex::Spatial { index: spatial }) = self.index(&index.name) else {
                    return Ok(None);
                };
                match (spatial.backend(), query_is_geography) {
                    (SpatialIndexBackend::GeographyS2, true)
                    | (SpatialIndexBackend::GeometryQuadCell, false) => {}
                    _ => return Ok(None),
                }
                let mut envelope =
                    SpatialEnvelope::from_value(&query_spatial).map_err(spatial_error)?;
                if let Some(radius_expr) = spatial_lookup.radius_expr {
                    let radius_value =
                        self.eval_expr(radius_expr, &Dataset::empty(), &[], params, ctes, None)?;
                    let radius = numeric_value_as_f64("ST_DWithin", "third", &radius_value)?;
                    envelope = match spatial.backend() {
                        SpatialIndexBackend::GeographyS2 => {
                            envelope.expand_geography_meters(radius)
                        }
                        SpatialIndexBackend::GeometryQuadCell => envelope.expand_planar(radius),
                    };
                }
                let row_ids = spatial.candidate_row_ids(envelope);
                return self
                    .dataset_from_row_id_set(
                        table,
                        row_source,
                        alias,
                        RuntimeRowIdSet::Many(&row_ids),
                        false,
                    )
                    .map(Some);
            }
        }

        if let Some((table_qualifier, column_name, value_expr)) = simple_btree_lookup(filter) {
            if !matches_filter_binding(name, alias, table_qualifier) {
                return Ok(None);
            }
            if let Some(index) = self.catalog.indexes.values().find(|index| {
                identifiers_equal(&index.table_name, name)
                    && index.fresh
                    && index.kind == IndexKind::Btree
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|index_column| identifiers_equal(index_column, column_name))
                    && index.columns[0].expression_sql.is_none()
            }) {
                let value =
                    self.eval_expr(value_expr, &Dataset::empty(), &[], params, ctes, None)?;
                let value = coerce_lookup_literal(table, column_name, value_expr, value)?;
                if !timestamp_lookup_matches_schema(table, column_name, &value) {
                    return Ok(None);
                }
                if let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) {
                    let row_ids = keys.row_ids_for_value_set(&value)?;
                    if let Some(ref tracing) = self.tracing {
                        tracing.record_index_usage(
                            name,
                            &index.name,
                            "btree",
                            crate::tracing::index_usage::IndexUsageKind::Read,
                        );
                    }
                    return self
                        .dataset_from_row_id_set(table, row_source, alias, row_ids, false)
                        .map(Some);
                }
            }
        }

        if let Some(row_ids) =
            self.trigram_candidate_row_ids_for_filter(name, alias, filter, params, ctes)?
        {
            return self
                .dataset_from_row_id_set(
                    table,
                    row_source,
                    alias,
                    RuntimeRowIdSet::Many(&row_ids),
                    false,
                )
                .map(Some);
        }

        Ok(None)
    }
    pub(crate) fn try_fulltext_bm25_top_k_select(
        &self,
        select: &Select,
        order_by: &[crate::sql::ast::OrderBy],
        limit: Option<&Expr>,
        offset: Option<&Expr>,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        if offset.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.from.len() != 1
            || order_by.len() != 1
        {
            return Ok(None);
        }
        let Some(limit_expr) = limit else {
            return Ok(None);
        };
        let limit = self.eval_constant_i64(limit_expr, params, ctes)?;
        if limit <= 0 {
            return Ok(None);
        }
        let Ok(limit) = usize::try_from(limit) else {
            return Ok(None);
        };
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if ctes.contains_key(name)
            || self
                .visible_view(name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(name)
        {
            return Ok(None);
        }
        let Some(table_schema) = self.table_schema(name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(table_schema) {
            return Ok(None);
        }
        let Some(row_source) = self.table_row_source(name) else {
            return Ok(None);
        };
        let binding_name = alias.as_deref().unwrap_or(name.as_str());
        let Some(fulltext_lookup) = exact_fulltext_lookup(select.filter.as_ref()) else {
            return Ok(None);
        };
        let index_value = self.eval_expr(
            fulltext_lookup.index_name_expr,
            &Dataset::empty(),
            &[],
            params,
            ctes,
            None,
        )?;
        let query_value = self.eval_expr(
            fulltext_lookup.query_expr,
            &Dataset::empty(),
            &[],
            params,
            ctes,
            None,
        )?;
        let Some(index_name) = expect_text_arg("FULLTEXT_MATCH", "first", &index_value)? else {
            return Ok(None);
        };
        let Some(query_text) = expect_text_arg("FULLTEXT_MATCH", "second", &query_value)? else {
            return Ok(None);
        };
        let Some(index_schema) = self.catalog.index(index_name) else {
            return Ok(None);
        };
        if !identifiers_equal(&index_schema.table_name, name)
            || !index_schema.fresh
            || index_schema.kind != IndexKind::FullText
        {
            return Ok(None);
        }
        if !order_by[0].descending {
            return Ok(None);
        }
        let Some(RuntimeIndex::FullText { index }) = self.index(&index_schema.name) else {
            return Ok(None);
        };

        enum ProjectionKind {
            Column(usize),
            Score,
        }

        let mut projection_kinds = Vec::with_capacity(select.projection.len());
        let mut column_names = Vec::with_capacity(select.projection.len());
        let mut score_alias = None;
        let mut score_expr = None;
        for (item_index, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::Expr { expr, alias } => match expr {
                    Expr::Column {
                        table: column_table,
                        column,
                    } => {
                        let Some(column_index) = simple_expression_projection_column_index(
                            table_schema,
                            name,
                            binding_name,
                            column_table.as_deref(),
                            column,
                        ) else {
                            return Ok(None);
                        };
                        projection_kinds.push(ProjectionKind::Column(column_index));
                        column_names.push(alias.clone().unwrap_or_else(|| column.clone()));
                    }
                    Expr::Function { name, args }
                        if name.eq_ignore_ascii_case("bm25") && args.len() == 1 =>
                    {
                        if score_expr.is_some() {
                            return Ok(None);
                        }
                        if !order_by_matches_alias_or_projection(
                            &order_by[0],
                            alias.as_deref(),
                            expr,
                            true,
                        ) {
                            return Ok(None);
                        }
                        score_alias = alias.clone();
                        score_expr = Some(expr);
                        projection_kinds.push(ProjectionKind::Score);
                        column_names.push(
                            alias
                                .clone()
                                .unwrap_or_else(|| infer_expr_name(expr, item_index + 1)),
                        );
                    }
                    _ => return Ok(None),
                },
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => return Ok(None),
            }
        }
        let Some(score_expr) = score_expr else {
            return Ok(None);
        };
        if score_alias.is_none()
            && !order_by_matches_alias_or_projection(&order_by[0], None, score_expr, true)
        {
            return Ok(None);
        }
        let Expr::Function { args, .. } = score_expr else {
            return Ok(None);
        };
        let score_index_value =
            self.eval_expr(&args[0], &Dataset::empty(), &[], params, ctes, None)?;
        let Some(score_index_name) = expect_text_arg("BM25", "first", &score_index_value)? else {
            return Ok(None);
        };
        if !identifiers_equal(score_index_name, index_name) {
            return Ok(None);
        }

        let hits = index
            .search_top_k(query_text, limit)
            .map_err(|error| DbError::sql(error.message))?;
        let mut rows = Vec::with_capacity(hits.len());
        for hit in hits {
            let Some(row_id) = i64::try_from(hit.row_id).ok() else {
                continue;
            };
            let Some(row) = row_source.row_by_id(row_id)? else {
                continue;
            };
            let mut values = Vec::with_capacity(projection_kinds.len());
            for projection_kind in &projection_kinds {
                match projection_kind {
                    ProjectionKind::Column(index) => {
                        let Some(value) = row.values().get(*index) else {
                            return Err(DbError::internal(
                                "fulltext fast path projection index is out of bounds",
                            ));
                        };
                        values.push(value.clone());
                    }
                    ProjectionKind::Score => values.push(Value::Float64(hit.score)),
                }
            }
            rows.push(values);
        }

        let columns = column_names
            .into_iter()
            .map(|name| ColumnBinding::visible(None, name))
            .collect();
        Ok(Some(Dataset::with_rows(columns, rows)))
    }
    pub(crate) fn trigram_candidate_row_ids_for_filter(
        &self,
        table_name: &str,
        alias: &Option<String>,
        filter: &Expr,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Vec<i64>>> {
        let Some(lookup) = simple_trigram_lookup(filter) else {
            return Ok(None);
        };
        if !matches_filter_binding(table_name, alias, lookup.table_qualifier) {
            return Ok(None);
        }
        let Some(index_schema) = self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, table_name)
                && index.fresh
                && index.kind == IndexKind::Trigram
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|index_column| identifiers_equal(index_column, lookup.column_name))
        }) else {
            return Ok(None);
        };
        let pattern = self.eval_expr(
            lookup.pattern_expr,
            &Dataset::empty(),
            &[],
            params,
            ctes,
            None,
        )?;
        let Value::Text(pattern) = pattern else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Trigram { index }) = self.index(&index_schema.name) else {
            return Ok(None);
        };
        if !index.planner_may_use_index() {
            return Ok(None);
        }
        let row_ids = match index.query_candidates(&pattern, lookup.has_additional_filter)? {
            TrigramQueryResult::Candidates(ids) | TrigramQueryResult::Capped(ids) => ids
                .into_iter()
                .filter_map(|row_id| i64::try_from(row_id).ok())
                .collect::<Vec<_>>(),
            TrigramQueryResult::FallbackTooShort
            | TrigramQueryResult::FallbackRequiresAdditionalFilter
            | TrigramQueryResult::RebuildRequired => return Ok(None),
        };
        Ok(Some(row_ids))
    }
    pub(crate) fn try_spatial_join(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        if select.from.len() != 1 {
            return Ok(None);
        }
        let FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let JoinConstraint::On(on) = constraint else {
            return Ok(None);
        };
        let (left_name, left_alias) = match &**left {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        let (right_name, right_alias) = match &**right {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        if ctes.contains_key(left_name)
            || ctes.contains_key(right_name)
            || self
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
        let Some(join_predicate) = simple_spatial_join_predicate(on, left_binding, right_binding)
        else {
            return Ok(None);
        };

        for (indexed_ref, probe_ref) in [
            (join_predicate.left, join_predicate.right),
            (join_predicate.right, join_predicate.left),
        ] {
            let Some((indexed_table, probe_table, indexed_on_left)) =
                spatial_join_argument_orientation(
                    left_binding,
                    right_binding,
                    indexed_ref,
                    probe_ref,
                )
            else {
                continue;
            };
            if let Some(dataset) = self.try_spatial_join_orientation(SpatialJoinOrientation {
                indexed_table,
                indexed_ref,
                probe_table,
                probe_ref,
                indexed_on_left,
                left_alias,
                right_alias,
                constraint,
                radius_expr: join_predicate.radius_expr,
                params,
                ctes,
            })? {
                return Ok(Some(dataset));
            }
        }

        Ok(None)
    }
    fn try_spatial_join_orientation(
        &self,
        plan: SpatialJoinOrientation<'_>,
    ) -> Result<Option<Dataset>> {
        if !matches_table_binding(plan.indexed_table, plan.indexed_ref.table)
            || !matches_table_binding(plan.probe_table, plan.probe_ref.table)
        {
            return Ok(None);
        }
        let Some(index_schema) =
            self.spatial_index_for_table_column(plan.indexed_table.name, plan.indexed_ref.column)
        else {
            return Ok(None);
        };
        let indexed_table = self.table_schema(plan.indexed_table.name).ok_or_else(|| {
            DbError::sql(format!("unknown table or view {}", plan.indexed_table.name))
        })?;
        let probe_table = self.table_schema(plan.probe_table.name).ok_or_else(|| {
            DbError::sql(format!("unknown table or view {}", plan.probe_table.name))
        })?;
        if !generated_columns_are_stored(indexed_table)
            || !generated_columns_are_stored(probe_table)
        {
            return Ok(None);
        }
        let Some(indexed_source) = self.table_row_source(plan.indexed_table.name) else {
            return Ok(None);
        };
        let Some(probe_source) = self.table_row_source(plan.probe_table.name) else {
            return Ok(None);
        };
        let Some(probe_column_index) = schema_column_index(probe_table, plan.probe_ref.column)
        else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Spatial { index: spatial }) = self.index(&index_schema.name) else {
            return Ok(None);
        };

        let left_table = if plan.indexed_on_left {
            indexed_table
        } else {
            probe_table
        };
        let right_table = if plan.indexed_on_left {
            probe_table
        } else {
            indexed_table
        };
        let left_columns = table_output_columns(left_table, plan.left_alias);
        let right_columns = table_output_columns(right_table, plan.right_alias);
        let mut eval_columns = left_columns.clone();
        eval_columns.extend(right_columns.clone());
        let eval_dataset = Dataset::with_rows(eval_columns, Vec::new());
        let eval_context = JoinEvalContext {
            dataset: &eval_dataset,
            runtime: self,
            params: plan.params,
            ctes: plan.ctes,
        };
        let columns = join_output_columns(
            &Dataset::with_rows(left_columns, Vec::new()),
            &Dataset::with_rows(right_columns, Vec::new()),
            &[],
        );
        let mut rows = Vec::new();

        for probe_row in probe_source.rows() {
            let probe_row = probe_row?;
            let Some(probe_value) = probe_row.values().get(probe_column_index) else {
                return Err(DbError::internal(
                    "spatial join probe row is shorter than schema",
                ));
            };
            let Some((probe_is_geography, probe_spatial)) = spatial_value_from_db(probe_value)?
            else {
                continue;
            };
            match (spatial.backend(), probe_is_geography) {
                (SpatialIndexBackend::GeographyS2, true)
                | (SpatialIndexBackend::GeometryQuadCell, false) => {}
                _ => return Ok(None),
            }
            let mut envelope =
                SpatialEnvelope::from_value(&probe_spatial).map_err(spatial_error)?;
            if let Some(radius_expr) = plan.radius_expr {
                let radius_value = self.eval_expr(
                    radius_expr,
                    &Dataset::empty(),
                    &[],
                    plan.params,
                    plan.ctes,
                    None,
                )?;
                let radius = numeric_value_as_f64("ST_DWithin", "third", &radius_value)?;
                envelope = match spatial.backend() {
                    SpatialIndexBackend::GeographyS2 => envelope.expand_geography_meters(radius),
                    SpatialIndexBackend::GeometryQuadCell => envelope.expand_planar(radius),
                };
            }

            for indexed_row_id in spatial.candidate_row_ids(envelope) {
                let Some(indexed_row) = indexed_source.row_by_id(indexed_row_id)? else {
                    continue;
                };
                let (left_values, right_values) = if plan.indexed_on_left {
                    (indexed_row.values(), probe_row.values())
                } else {
                    (probe_row.values(), indexed_row.values())
                };
                let mut eval_row = left_values.to_vec();
                eval_row.extend_from_slice(right_values);
                if join_rows_match(
                    plan.constraint,
                    &[],
                    &eval_row,
                    left_values,
                    right_values,
                    &eval_context,
                )? {
                    rows.push(join_output_row(left_values, right_values, &[])?);
                }
            }
        }

        Ok(Some(Dataset::with_rows(columns, rows)))
    }
    fn spatial_index_for_table_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Option<&IndexSchema> {
        self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, table_name)
                && index.fresh
                && index.kind == IndexKind::Spatial
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0].expression_sql.is_none()
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|indexed| identifiers_equal(indexed, column_name))
        })
    }
    pub(crate) fn try_indexed_prefiltered_inner_join_tree(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let Some(filter) = &select.filter else {
            return Ok(None);
        };
        if select.from.len() != 1 {
            return Ok(None);
        }
        let Some((Some(filter_table), filter_column, value_expr)) = simple_btree_lookup(filter)
        else {
            return Ok(None);
        };
        if !from_item_is_all_inner_table_joins(&select.from[0]) {
            return Ok(None);
        }

        let mut applied_prefilter = false;
        let dataset = self.evaluate_from_item_with_indexed_prefilter(
            &select.from[0],
            params,
            ctes,
            filter_table,
            filter_column,
            value_expr,
            &mut applied_prefilter,
        )?;
        if applied_prefilter {
            Ok(Some(dataset))
        } else {
            Ok(None)
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn evaluate_from_item_with_indexed_prefilter(
        &self,
        item: &FromItem,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        filter_table: &str,
        filter_column: &str,
        value_expr: &Expr,
        applied_prefilter: &mut bool,
    ) -> Result<Dataset> {
        match item {
            FromItem::Table { name, alias } => {
                if !*applied_prefilter
                    && matches_filter_binding(name, alias, Some(filter_table))
                    && !ctes.contains_key(name)
                    && self
                        .visible_view(name, NameResolutionScope::Session)
                        .is_none()
                    && !self.visible_table_is_temporary(name)
                {
                    if let Some(dataset) = self.indexed_table_lookup(
                        name,
                        alias,
                        filter_column,
                        value_expr,
                        params,
                        ctes,
                    )? {
                        *applied_prefilter = true;
                        return Ok(dataset);
                    }
                }
                self.evaluate_from_item(item, params, ctes)
            }
            FromItem::Join {
                left,
                right,
                kind,
                constraint,
            } => {
                let left_dataset = self.evaluate_from_item_with_indexed_prefilter(
                    left,
                    params,
                    ctes,
                    filter_table,
                    filter_column,
                    value_expr,
                    applied_prefilter,
                )?;
                if matches!(kind, JoinKind::Inner | JoinKind::Left) {
                    if let Some(dataset) = self.try_indexed_equi_join_with_right_table(
                        &left_dataset,
                        right,
                        *kind,
                        constraint,
                        ctes,
                    )? {
                        return Ok(dataset);
                    }
                    if let Some(dataset) = self.try_indexed_equi_join_with_right_cte(
                        &left_dataset,
                        right,
                        constraint,
                        *kind,
                        ctes,
                    )? {
                        return Ok(dataset);
                    }
                }
                let right_dataset = self.evaluate_from_item_with_indexed_prefilter(
                    right,
                    params,
                    ctes,
                    filter_table,
                    filter_column,
                    value_expr,
                    applied_prefilter,
                )?;
                nested_loop_join(
                    left_dataset,
                    right_dataset,
                    *kind,
                    constraint,
                    self,
                    params,
                    ctes,
                )
            }
            _ => self.evaluate_from_item(item, params, ctes),
        }
    }
    pub(crate) fn indexed_table_lookup(
        &self,
        table_name: &str,
        alias: &Option<String>,
        column_name: &str,
        value_expr: &Expr,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let table = self
            .table_schema(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table or view {table_name}")))?;
        if !generated_columns_are_stored(table) {
            return Ok(None);
        }
        let row_source = self.table_row_source(table_name);
        if row_id_alias_column_name(table)
            .is_some_and(|row_id_column| identifiers_equal(row_id_column, column_name))
        {
            let Some(row_source) = row_source else {
                return Ok(None);
            };
            let value = self.eval_expr(value_expr, &Dataset::empty(), &[], params, ctes, None)?;
            let row_ids = match value {
                Value::Int64(row_id) => RuntimeRowIdSet::Single(row_id),
                _ => RuntimeRowIdSet::Empty,
            };
            return self
                .dataset_from_row_id_set(table, Some(row_source), alias, row_ids, false)
                .map(Some);
        }
        let Some(index) = self.catalog.indexes.values().find(|index| {
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
        }) else {
            return Ok(None);
        };

        let value = self.eval_expr(value_expr, &Dataset::empty(), &[], params, ctes, None)?;
        let value = coerce_lookup_literal(table, column_name, value_expr, value)?;
        if !timestamp_lookup_matches_schema(table, column_name, &value) {
            return Ok(None);
        }
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
            return Ok(None);
        };
        let row_ids = keys.row_ids_for_value_set(&value)?;
        self.dataset_from_row_id_set(table, row_source, alias, row_ids, false)
            .map(Some)
    }
    pub(crate) fn indexed_inner_join_filtered(
        &self,
        plan: IndexedJoinPlan<'_>,
    ) -> Result<Option<Dataset>> {
        let filtered_table = self.table_schema(plan.filtered_table.name).ok_or_else(|| {
            DbError::sql(format!(
                "unknown table or view {}",
                plan.filtered_table.name
            ))
        })?;
        let probe_table = self.table_schema(plan.probe_table.name).ok_or_else(|| {
            DbError::sql(format!("unknown table or view {}", plan.probe_table.name))
        })?;
        if !generated_columns_are_stored(filtered_table)
            || !generated_columns_are_stored(probe_table)
        {
            return Ok(None);
        }
        let probe_source = self.visible_table_row_source(plan.probe_table.name);
        let mut filtered_join_indexes = Vec::with_capacity(plan.filtered_join_columns.len());
        for filtered_join_column in &plan.filtered_join_columns {
            let filtered_join_index = filtered_table
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, filtered_join_column))
                .ok_or_else(|| DbError::sql(format!("unknown column {filtered_join_column}")))?;
            filtered_join_indexes.push(filtered_join_index);
        }
        let is_probe_rowid_alias = plan.probe_join_columns.len() == 1
            && crate::exec::dml::row_id_alias_column_name(probe_table)
                .is_some_and(|name| identifiers_equal(name, plan.probe_join_columns[0]));

        let mut probe_hash_join_indexes = None;
        let (probe_index, ordered_filtered_join_indexes) = if is_probe_rowid_alias {
            (None, filtered_join_indexes)
        } else if let Some((probe_index, ordered_filtered_join_indexes)) =
            self.catalog.indexes.values().find_map(|index| {
                if !identifiers_equal(&index.table_name, plan.probe_table.name)
                    || !index.fresh
                    || index.kind != IndexKind::Btree
                    || index.predicate_sql.is_some()
                    || index.columns.len() != plan.probe_join_columns.len()
                {
                    return None;
                }
                let mut ordered_filtered_join_indexes = Vec::with_capacity(index.columns.len());
                for index_column in &index.columns {
                    if index_column.expression_sql.is_some() {
                        return None;
                    }
                    let index_column_name = index_column.column_name.as_deref()?;
                    let join_position = plan.probe_join_columns.iter().position(|join_column| {
                        identifiers_equal(join_column, index_column_name)
                    })?;
                    let probe_column_index = schema_column_index(probe_table, index_column_name)?;
                    if !timestamp_key_types_match(
                        filtered_table.columns[filtered_join_indexes[join_position]].column_type,
                        probe_table.columns[probe_column_index].column_type,
                    ) {
                        return None;
                    }
                    ordered_filtered_join_indexes.push(filtered_join_indexes[join_position]);
                }
                Some((index, ordered_filtered_join_indexes))
            })
        {
            (Some(probe_index), ordered_filtered_join_indexes)
        } else {
            let mut ordered_probe_join_indexes = Vec::with_capacity(plan.probe_join_columns.len());
            for probe_join_column in &plan.probe_join_columns {
                let Some(probe_join_index) = schema_column_index(probe_table, probe_join_column)
                else {
                    return Ok(None);
                };
                ordered_probe_join_indexes.push(probe_join_index);
            }
            probe_hash_join_indexes = Some(ordered_probe_join_indexes);
            (None, filtered_join_indexes)
        };
        let keys = if let Some(index) = probe_index {
            let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
                return Ok(None);
            };
            Some(keys)
        } else {
            None
        };
        let probe_hash_rows = if let Some(probe_join_indexes) = probe_hash_join_indexes.as_ref() {
            let Some(probe_source) = probe_source else {
                return Ok(None);
            };
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
        let use_probe_row_position_map = probe_hash_rows.is_none()
            && probe_source
                .map_or(0, |source| source.row_count())
                .saturating_mul(plan.filtered_dataset.rows.len())
                > 8_192;
        let probe_row_positions = if use_probe_row_position_map {
            let mut positions = Int64Map::<usize>::default();
            for (position, row) in probe_source
                .map(|source| source.rows())
                .unwrap_or_else(TableRowIter::empty)
                .enumerate()
            {
                positions.insert(row?.row_id(), position);
            }
            Some(positions)
        } else {
            None
        };

        let probe_columns = probe_table
            .columns
            .iter()
            .map(|column| {
                ColumnBinding::visible_source(
                    Some(plan.probe_table.binding_name().to_string()),
                    Some(plan.probe_table.name.to_string()),
                    column.name.clone(),
                )
                .with_type(column.column_type)
            })
            .collect::<Vec<_>>();
        let mut columns = if plan.filtered_on_left {
            plan.filtered_dataset.columns.clone()
        } else {
            probe_columns.clone()
        };
        if plan.filtered_on_left {
            columns.extend(probe_columns.clone());
        } else {
            columns.extend(plan.filtered_dataset.columns.clone());
        }
        let mut rows = Vec::new();
        for filtered_row in plan.filtered_dataset.rows.iter() {
            let join_values = ordered_filtered_join_indexes
                .iter()
                .map(|index| {
                    filtered_row.get(*index).ok_or_else(|| {
                        DbError::internal("join row is shorter than filtered table schema")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            if join_values
                .iter()
                .any(|join_value| matches!(join_value, Value::Null))
            {
                continue;
            }
            if let Some(probe_hash_rows) = probe_hash_rows.as_ref() {
                let join_key = Row::new(
                    join_values
                        .iter()
                        .map(|value| equality_key_value(value))
                        .collect(),
                )
                .encode()?;
                let Some(matching_probe_rows) = probe_hash_rows.get(&join_key) else {
                    continue;
                };
                for (_, probe_row) in matching_probe_rows {
                    let mut row = Vec::with_capacity(filtered_row.len() + probe_row.len());
                    if plan.filtered_on_left {
                        row.extend_from_slice(filtered_row);
                        row.extend_from_slice(probe_row);
                    } else {
                        row.extend_from_slice(probe_row);
                        row.extend_from_slice(filtered_row);
                    }
                    rows.push(row);
                }
                continue;
            }
            let row_ids = if let Some(keys) = keys {
                if join_values.len() == 1 {
                    keys.row_ids_for_value_set(join_values[0])?
                } else {
                    keys.row_id_set_for_key(&RuntimeBtreeKey::Encoded(RuntimeEncodedKey::from_vec(
                        Row::new(join_values.into_iter().cloned().collect()).encode()?,
                    )))
                }
            } else if join_values.len() == 1 {
                match join_values[0] {
                    Value::Int64(val) => RuntimeRowIdSet::Single(*val),
                    _ => RuntimeRowIdSet::Empty,
                }
            } else {
                RuntimeRowIdSet::Empty
            };
            if row_ids.is_empty() {
                continue;
            }
            row_ids.for_each(|row_id| {
                let probe_row = if let Some(positions) = probe_row_positions.as_ref() {
                    let Some(probe_position) = positions.get(&row_id).copied() else {
                        return;
                    };
                    match probe_source
                        .map(|source| source.row_at_position(probe_position))
                        .transpose()
                    {
                        Ok(Some(Some(probe_row))) => probe_row.values().to_vec(),
                        Ok(Some(None)) | Ok(None) => return,
                        Err(_) => return,
                    }
                } else {
                    match probe_source
                        .map(|source| source.row_by_id(row_id))
                        .transpose()
                    {
                        Ok(Some(Some(probe_row))) => probe_row.values().to_vec(),
                        Ok(Some(None)) | Ok(None) => return,
                        Err(_) => return,
                    }
                };
                let mut row = Vec::with_capacity(filtered_row.len() + probe_row.len());
                if plan.filtered_on_left {
                    row.extend_from_slice(filtered_row);
                    row.extend_from_slice(&probe_row);
                } else {
                    row.extend_from_slice(&probe_row);
                    row.extend_from_slice(filtered_row);
                }
                rows.push(row);
            });
        }
        Ok(Some(Dataset::with_rows(columns, rows)))
    }
    /// Indexed equi-join probe path.
    ///
    /// For each row in `left`, probes the right table via a b-tree index
    /// (or the rowid alias) instead of doing a full O(|left| * |right|)
    /// nested loop. Supported join kinds:
    ///
    /// * `Inner` — skips left rows with NULL join values and left rows
    ///   whose join value has no match.
    /// * `Left`  — preserves every left row, emitting a NULL-extended
    ///   right half when the left join value is NULL or has no match.
    ///
    /// Returns `Ok(None)` if any of the preconditions for the fast path
    /// are not met (non-table right side, non-equi join, view/CTE/temp
    /// table, etc.), in which case the caller falls back to the nested
    /// loop join.
    pub(crate) fn try_indexed_equi_join_with_right_table(
        &self,
        left: &Dataset,
        right_item: &FromItem,
        kind: JoinKind,
        constraint: &JoinConstraint,
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        if !matches!(
            kind,
            JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Full
        ) {
            return Ok(None);
        }
        let JoinConstraint::On(on) = constraint else {
            return Ok(None);
        };
        let Some(join_equalities) = simple_join_equalities(on) else {
            return Ok(None);
        };
        let FromItem::Table {
            name: right_name,
            alias: right_alias,
        } = right_item
        else {
            return Ok(None);
        };
        if ctes.contains_key(right_name) {
            return Ok(None);
        }
        if self
            .visible_view(right_name, NameResolutionScope::Session)
            .is_some()
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }

        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        let mut left_probe_refs = Vec::with_capacity(join_equalities.len());
        let mut right_join_columns = Vec::with_capacity(join_equalities.len());
        for (left_join_ref, right_join_ref) in join_equalities {
            let (left_probe_ref, right_join_column) =
                if matches_table_binding(right_binding, right_join_ref.table) {
                    (left_join_ref, right_join_ref.column)
                } else if matches_table_binding(right_binding, left_join_ref.table) {
                    (right_join_ref, left_join_ref.column)
                } else {
                    return Ok(None);
                };
            left_probe_refs.push(left_probe_ref);
            right_join_columns.push(right_join_column);
        }
        let mut left_join_indexes = Vec::with_capacity(left_probe_refs.len());
        for left_probe_ref in &left_probe_refs {
            let Some(left_join_index) =
                dataset_column_index(left, left_probe_ref.table, left_probe_ref.column)
            else {
                return Ok(None);
            };
            left_join_indexes.push(left_join_index);
        }

        let right_table = self
            .table_schema(right_name)
            .ok_or_else(|| DbError::sql(format!("unknown table or view {right_name}")))?;
        if !generated_columns_are_stored(right_table) {
            return Ok(None);
        }
        let right_source = self.visible_table_row_source(right_name);
        let is_probe_rowid_alias = right_join_columns.len() == 1
            && crate::exec::dml::row_id_alias_column_name(right_table)
                .is_some_and(|name| identifiers_equal(name, right_join_columns[0]));

        let mut right_hash_join_indexes = None;
        let (probe_index, ordered_left_join_indexes) = if is_probe_rowid_alias {
            (None, left_join_indexes)
        } else if let Some((probe_index, ordered_left_join_indexes)) =
            self.catalog.indexes.values().find_map(|index| {
                if !identifiers_equal(&index.table_name, right_name)
                    || !index.fresh
                    || index.kind != IndexKind::Btree
                    || index.predicate_sql.is_some()
                    || index.columns.len() != right_join_columns.len()
                {
                    return None;
                }
                let mut ordered_left_join_indexes = Vec::with_capacity(index.columns.len());
                for index_column in &index.columns {
                    if index_column.expression_sql.is_some() {
                        return None;
                    }
                    let index_column_name = index_column.column_name.as_deref()?;
                    let join_position = right_join_columns.iter().position(|join_column| {
                        identifiers_equal(join_column, index_column_name)
                    })?;
                    let right_column_index = schema_column_index(right_table, index_column_name)?;
                    let left_column_index = left_join_indexes[join_position];
                    if matches!(
                        right_table.columns[right_column_index].column_type,
                        ColumnType::Timestamp | ColumnType::TimestampTz
                    ) && left.rows.iter().any(|row| {
                        row.get(left_column_index).is_some_and(|value| {
                            !timestamp_lookup_matches_schema(right_table, index_column_name, value)
                        })
                    }) {
                        return None;
                    }
                    ordered_left_join_indexes.push(left_join_indexes[join_position]);
                }
                Some((index, ordered_left_join_indexes))
            })
        {
            (Some(probe_index), ordered_left_join_indexes)
        } else {
            let mut ordered_right_join_indexes = Vec::with_capacity(right_join_columns.len());
            for right_join_column in &right_join_columns {
                let Some(right_join_index) = schema_column_index(right_table, right_join_column)
                else {
                    return Ok(None);
                };
                ordered_right_join_indexes.push(right_join_index);
            }
            right_hash_join_indexes = Some(ordered_right_join_indexes);
            (None, left_join_indexes)
        };
        let keys = if let Some(index) = probe_index {
            let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&index.name) else {
                return Ok(None);
            };
            Some(keys)
        } else {
            None
        };
        let right_hash_rows = if let Some(right_join_indexes) = right_hash_join_indexes.as_ref() {
            let Some(right_source) = right_source else {
                return Ok(None);
            };
            let mut hashed = SimpleJoinHashRows::new();
            for right_row in right_source.rows() {
                let right_row = right_row?;
                let Some(join_key) =
                    simple_join_key_from_indexes(right_row.values(), right_join_indexes)?
                else {
                    continue;
                };
                hashed
                    .entry(join_key)
                    .or_default()
                    .push((right_row.row_id(), right_row.values().to_vec()));
            }
            Some(hashed)
        } else {
            None
        };

        let use_right_row_position_map = right_hash_rows.is_none()
            && right_source
                .map_or(0, |source| source.row_count())
                .saturating_mul(left.rows.len())
                > 8_192;
        let right_row_positions = if use_right_row_position_map {
            let mut positions = Int64Map::<usize>::default();
            for (position, row) in right_source
                .map(|source| source.rows())
                .unwrap_or_else(TableRowIter::empty)
                .enumerate()
            {
                positions.insert(row?.row_id(), position);
            }
            Some(positions)
        } else {
            None
        };

        let right_binding_name = right_alias.clone().unwrap_or_else(|| right_name.clone());
        let mut columns = left.columns.clone();
        columns.extend(right_table.columns.iter().map(|column| {
            ColumnBinding::visible_source(
                Some(right_binding_name.clone()),
                Some(right_name.clone()),
                column.name.clone(),
            )
            .with_type(column.column_type)
        }));
        let right_column_count = right_table.columns.len();
        let is_left_outer = matches!(kind, JoinKind::Left | JoinKind::Full);
        let is_right_outer = matches!(kind, JoinKind::Right | JoinKind::Full);
        let mut rows = Vec::new();
        let mut matched_right_row_ids = is_right_outer.then(Int64Map::<()>::default);
        for left_row in left.rows.iter() {
            let join_values = ordered_left_join_indexes
                .iter()
                .map(|index| {
                    left_row.get(*index).ok_or_else(|| {
                        DbError::internal("join row is shorter than the left input schema")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            if join_values
                .iter()
                .any(|join_value| matches!(join_value, Value::Null))
            {
                if is_left_outer {
                    let mut row = Vec::with_capacity(left_row.len() + right_column_count);
                    row.extend_from_slice(left_row);
                    row.extend(std::iter::repeat_n(Value::Null, right_column_count));
                    rows.push(row);
                }
                continue;
            }
            let rows_before = rows.len();
            if let Some(right_hash_rows) = right_hash_rows.as_ref() {
                let join_key = Row::new(
                    join_values
                        .iter()
                        .map(|value| equality_key_value(value))
                        .collect(),
                )
                .encode()?;
                if let Some(matching_rows) = right_hash_rows.get(&join_key) {
                    for (row_id, right_values) in matching_rows {
                        if let Some(matched_right_row_ids) = matched_right_row_ids.as_mut() {
                            matched_right_row_ids.insert(*row_id, ());
                        }
                        let mut row = Vec::with_capacity(left_row.len() + right_values.len());
                        row.extend_from_slice(left_row);
                        row.extend_from_slice(right_values);
                        rows.push(row);
                    }
                }
            } else {
                let row_ids = if let Some(keys) = keys {
                    if join_values.len() == 1 {
                        keys.row_ids_for_value_set(join_values[0])?
                    } else {
                        keys.row_id_set_for_key(&RuntimeBtreeKey::Encoded(
                            RuntimeEncodedKey::from_vec(
                                Row::new(join_values.into_iter().cloned().collect()).encode()?,
                            ),
                        ))
                    }
                } else if join_values.len() == 1 {
                    match join_values[0] {
                        Value::Int64(val) => RuntimeRowIdSet::Single(*val),
                        _ => RuntimeRowIdSet::Empty,
                    }
                } else {
                    RuntimeRowIdSet::Empty
                };
                row_ids.for_each(|row_id| {
                    let right_values = if let Some(positions) = right_row_positions.as_ref() {
                        let Some(right_position) = positions.get(&row_id).copied() else {
                            return;
                        };
                        match right_source
                            .map(|source| source.row_at_position(right_position))
                            .transpose()
                        {
                            Ok(Some(Some(right_row))) => right_row.values().to_vec(),
                            Ok(Some(None)) | Ok(None) => return,
                            Err(_) => return,
                        }
                    } else {
                        match right_source
                            .map(|source| source.row_by_id(row_id))
                            .transpose()
                        {
                            Ok(Some(Some(right_row))) => right_row.values().to_vec(),
                            Ok(Some(None)) | Ok(None) => return,
                            Err(_) => return,
                        }
                    };
                    if let Some(matched_right_row_ids) = matched_right_row_ids.as_mut() {
                        matched_right_row_ids.insert(row_id, ());
                    }
                    let mut row = Vec::with_capacity(left_row.len() + right_values.len());
                    row.extend_from_slice(left_row);
                    row.extend_from_slice(&right_values);
                    rows.push(row);
                });
            }
            if is_left_outer && rows.len() == rows_before {
                let mut row = Vec::with_capacity(left_row.len() + right_column_count);
                row.extend_from_slice(left_row);
                row.extend(std::iter::repeat_n(Value::Null, right_column_count));
                rows.push(row);
            }
        }
        if let Some(matched_right_row_ids) = matched_right_row_ids.as_ref() {
            let left_nulls = vec![Value::Null; left.columns.len()];
            for right_row in right_source
                .map(|source| source.rows())
                .unwrap_or_else(TableRowIter::empty)
            {
                let right_row = right_row?;
                if matched_right_row_ids.contains_key(&right_row.row_id()) {
                    continue;
                }
                let mut row = Vec::with_capacity(left_nulls.len() + right_row.values().len());
                row.extend_from_slice(&left_nulls);
                row.extend_from_slice(right_row.values());
                rows.push(row);
            }
        }
        Ok(Some(Dataset::with_rows(columns, rows)))
    }
    pub(crate) fn try_indexed_equi_join_with_right_cte(
        &self,
        left: &Dataset,
        right_item: &FromItem,
        constraint: &JoinConstraint,
        kind: JoinKind,
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let _ = self;
        if !matches!(kind, JoinKind::Inner) {
            return Ok(None);
        }
        let JoinConstraint::On(on) = constraint else {
            return Ok(None);
        };
        let Some(join_equalities) = simple_join_equalities(on) else {
            return Ok(None);
        };
        let FromItem::Table {
            name: right_name,
            alias: right_alias,
        } = right_item
        else {
            return Ok(None);
        };
        let Some(right_dataset) = ctes.get(right_name) else {
            return Ok(None);
        };

        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        let mut left_probe_refs = Vec::with_capacity(join_equalities.len());
        let mut right_probe_refs = Vec::with_capacity(join_equalities.len());
        for (left_join_ref, right_join_ref) in join_equalities {
            let (left_probe_ref, right_probe_ref) =
                if matches_table_binding(right_binding, right_join_ref.table) {
                    (left_join_ref, right_join_ref)
                } else if matches_table_binding(right_binding, left_join_ref.table) {
                    (right_join_ref, left_join_ref)
                } else {
                    return Ok(None);
                };
            left_probe_refs.push(left_probe_ref);
            right_probe_refs.push(right_probe_ref);
        }

        let mut left_join_indexes = Vec::with_capacity(left_probe_refs.len());
        for left_probe_ref in &left_probe_refs {
            let Some(left_join_index) =
                dataset_column_index(left, left_probe_ref.table, left_probe_ref.column)
            else {
                return Ok(None);
            };
            left_join_indexes.push(left_join_index);
        }
        let mut right_join_indexes = Vec::with_capacity(right_probe_refs.len());
        let mut right_columns = right_dataset.columns.clone();
        if let Some(alias) = right_alias {
            for column in &mut right_columns {
                column.table = Some(alias.clone());
            }
        }
        for right_join_ref in &right_probe_refs {
            let right_join_indexes_for_ref = right_columns
                .iter()
                .enumerate()
                .filter(|(_, binding)| {
                    if !identifiers_equal(&binding.name, right_join_ref.column) {
                        return false;
                    }
                    if let Some(qualifier) = right_join_ref.table {
                        binding
                            .table
                            .as_deref()
                            .is_some_and(|table| identifiers_equal(table, qualifier))
                    } else {
                        !binding.hidden
                    }
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let [right_join_index] = right_join_indexes_for_ref.as_slice() else {
                return Ok(None);
            };
            right_join_indexes.push(*right_join_index);
        }

        let mut hashed_right_rows: BTreeMap<Vec<u8>, Vec<Vec<Value>>> = BTreeMap::new();
        for right_row in right_dataset.rows.iter() {
            let Some(join_key) = simple_join_key_from_indexes(right_row, &right_join_indexes)?
            else {
                continue;
            };
            hashed_right_rows
                .entry(join_key)
                .or_default()
                .push(right_row.clone());
        }

        let mut columns = left.columns.clone();
        columns.extend(right_columns);
        let right_column_count = columns.len().saturating_sub(left.columns.len());

        let mut rows = Vec::new();
        for left_row in left.rows.iter() {
            let Some(join_key) = simple_join_key_from_indexes(left_row, &left_join_indexes)? else {
                continue;
            };
            if let Some(matching_rows) = hashed_right_rows.get(&join_key) {
                for right_row in matching_rows {
                    let mut row = Vec::with_capacity(left_row.len() + right_column_count);
                    row.extend_from_slice(left_row);
                    row.extend_from_slice(right_row);
                    rows.push(row);
                }
            }
        }
        Ok(Some(Dataset::with_rows(columns, rows)))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn evaluate_join_with_lateral_right(
        &self,
        left: Dataset,
        right_item: &FromItem,
        kind: JoinKind,
        constraint: &JoinConstraint,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        scope_dataset: &Dataset,
        scope_row: &[Value],
    ) -> Result<Dataset> {
        if matches!(kind, JoinKind::Right | JoinKind::Full) {
            return Err(DbError::sql(
                "LATERAL is only supported with INNER, LEFT, and CROSS joins",
            ));
        }

        let mut columns = left.columns.clone();
        let mut rows = Vec::new();
        for left_row in left.rows.iter() {
            let left_single = Dataset::with_rows(left.columns.clone(), vec![left_row.clone()]);
            let scope_with_left =
                augment_dataset_with_outer_scope(left_single.clone(), scope_dataset, scope_row);
            let scope_values = scope_with_left
                .rows
                .first()
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let right = self.evaluate_from_item_in_scope(
                right_item,
                params,
                ctes,
                &scope_with_left,
                scope_values,
            )?;
            let joined =
                nested_loop_join(left_single, right, kind, constraint, self, params, ctes)?;
            columns = joined.columns.clone();
            rows.extend(joined.into_rows());
        }
        Ok(Dataset::with_rows(columns, rows))
    }
}
