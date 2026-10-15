//! Thematic extraction (mechanical split; no behavior change).

use super::*;

pub(crate) fn window_key_from_positions(row: &[Value], positions: &[usize]) -> Result<Vec<u8>> {
    if let [position] = positions {
        let value = row
            .get(*position)
            .ok_or_else(|| DbError::internal("window row is shorter than its bindings"))?;
        return row_identity(std::slice::from_ref(value));
    }
    row_identity(&values_from_positions(row, positions)?)
}

pub(super) fn compute_index_key(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
) -> Result<Option<RuntimeBtreeKey>> {
    compute_index_key_with_predicate(runtime, index, table, row_values, None)
}

/// Like [`compute_index_key`], but optionally accepts a pre-parsed predicate
/// expression. See [`prepare_index_predicate_expr`] and
/// [`row_satisfies_index_predicate_with_expr`].
pub(super) fn compute_index_key_with_predicate(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
    pre_parsed_predicate: Option<&Expr>,
) -> Result<Option<RuntimeBtreeKey>> {
    if !row_satisfies_index_predicate_with_expr(
        runtime,
        index,
        table,
        row_values,
        pre_parsed_predicate,
    )? {
        return Ok(None);
    }
    if btree_uses_typed_int64_keys(index, table) {
        let [column] = index.columns.as_slice() else {
            return Err(DbError::internal(
                "typed INT64 runtime indexes require exactly one indexed column",
            ));
        };
        if let Some(column_name) = &column.column_name {
            let position = column_position(table, column_name).ok_or_else(|| {
                DbError::constraint(format!("index column {} does not exist", column_name))
            })?;
            let Value::Int64(value) = row_values
                .get(position)
                .ok_or_else(|| DbError::internal("row is shorter than table schema"))?
            else {
                return Err(DbError::internal(
                    "typed INT64 runtime index expected an INT64 row value",
                ));
            };
            return Ok(Some(RuntimeBtreeKey::Int64(*value)));
        }
    }
    if btree_uses_typed_uuid_keys(index, table) {
        let [column] = index.columns.as_slice() else {
            return Err(DbError::internal(
                "typed UUID runtime indexes require exactly one indexed column",
            ));
        };
        if let Some(column_name) = &column.column_name {
            let position = column_position(table, column_name).ok_or_else(|| {
                DbError::constraint(format!("index column {} does not exist", column_name))
            })?;
            let Value::Uuid(value) = row_values
                .get(position)
                .ok_or_else(|| DbError::internal("row is shorter than table schema"))?
            else {
                return Err(DbError::internal(
                    "typed UUID runtime index expected a UUID row value",
                ));
            };
            return Ok(Some(RuntimeBtreeKey::Uuid(*value)));
        }
    }
    if let Some(value) = compute_single_column_index_key_fast(index, table, row_values)? {
        if index.unique && matches!(value, Value::Null) {
            return Ok(None);
        }
        return Ok(Some(RuntimeBtreeKey::Encoded(encode_runtime_index_key(
            value,
        )?)));
    }
    if let Some(positions) = plain_index_column_positions(index, table) {
        if positions.len() > 1 {
            let values = positions
                .iter()
                .map(|position| {
                    row_values
                        .get(*position)
                        .cloned()
                        .ok_or_else(|| DbError::internal("row is shorter than table schema"))
                })
                .collect::<Result<Vec<_>>>()?;
            if index.unique && values.iter().any(|value| matches!(value, Value::Null)) {
                return Ok(None);
            }
            return Ok(Some(RuntimeBtreeKey::Encoded(RuntimeEncodedKey::from_vec(
                Row::new(values).encode()?,
            ))));
        }
    }
    let values = compute_index_values(runtime, index, table, row_values)?;
    if index.unique && values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(None);
    }
    let key = if values.len() == 1 {
        encode_runtime_index_key(&values[0])?
    } else {
        RuntimeEncodedKey::from_vec(Row::new(values).encode()?)
    };
    Ok(Some(RuntimeBtreeKey::Encoded(key)))
}

/// Fast path for single-column btree indexes whose only column is a plain
/// stored column (no expression, no virtual generated column). Reads the value
/// directly by position without building a `Dataset` or cloning the full row,
/// which is the hot path for index maintenance during bulk DML.
pub(crate) fn compute_single_column_index_key_fast<'a>(
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &'a [Value],
) -> Result<Option<&'a Value>> {
    if index.columns.len() != 1 {
        return Ok(None);
    }
    let Some(column) = index.columns.first() else {
        return Ok(None);
    };
    if column.expression_sql.is_some() {
        return Ok(None);
    }
    let Some(column_name) = &column.column_name else {
        return Ok(None);
    };
    let Some(position) = column_position(table, column_name) else {
        return Ok(None);
    };
    // Virtual generated columns are not stored in `row_values`, so they must go
    // through the materializing path. Stored generated columns are present.
    if generated_columns_are_stored(table) {
        // All generated columns are stored: safe to read by position.
    } else if table
        .columns
        .get(position)
        .is_some_and(|col| col.generated_sql.is_some() && !col.generated_stored)
    {
        return Ok(None);
    }
    let Some(value) = row_values.get(position) else {
        return Ok(None);
    };
    Ok(Some(value))
}

pub(super) fn compute_index_values(
    runtime: &EngineRuntime,
    index: &IndexSchema,
    table: &TableSchema,
    row_values: &[Value],
) -> Result<Vec<Value>> {
    let row_materialized = if generated_columns_are_stored(table) {
        Cow::Borrowed(row_values)
    } else {
        let mut materialized = row_values.to_vec();
        runtime.apply_virtual_generated_columns(table, &mut materialized)?;
        Cow::Owned(materialized)
    };
    let row_for_eval = row_materialized.as_ref();
    let dataset = table_row_dataset(table, row_for_eval, &table.name);
    let bindings = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
    index
        .columns
        .iter()
        .map(|column| {
            if let Some(column_name) = &column.column_name {
                let position = column_position(table, column_name).ok_or_else(|| {
                    DbError::constraint(format!("index column {} does not exist", column_name))
                })?;
                Ok(row_for_eval[position].clone())
            } else if let Some(expression_sql) = &column.expression_sql {
                let expr = crate::sql::parser::parse_expression_sql(expression_sql)?;
                runtime.eval_expr(&expr, &dataset, bindings, &[], &BTreeMap::new(), None)
            } else {
                Err(DbError::constraint("index column definition is empty"))
            }
        })
        .collect()
}

pub(crate) fn sort_dataset_by_projection_order(
    runtime: Option<&EngineRuntime>,
    dataset: &mut Dataset,
    order_by: &[SimpleOrderByPlan],
) -> Result<()> {
    let mut sort_error = None;
    dataset.rows_mut().sort_by(|left, right| {
        for order in order_by {
            let ordering = compare_values_with_runtime_collation(
                runtime,
                &left[order.projection_index],
                &right[order.projection_index],
                order.collation.clone(),
            );
            match ordering {
                Ok(std::cmp::Ordering::Equal) => continue,
                Ok(ordering) => {
                    return if order.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
                Err(error) => {
                    if sort_error.is_none() {
                        sort_error = Some(error);
                    }
                    return std::cmp::Ordering::Equal;
                }
            }
        }
        std::cmp::Ordering::Equal
    });
    if let Some(error) = sort_error {
        return Err(error);
    }
    Ok(())
}

impl EngineRuntime {
    pub(crate) fn evaluate_set_operation(
        &self,
        op: crate::sql::ast::SetOperation,
        all: bool,
        left: Dataset,
        right: Dataset,
    ) -> Result<Dataset> {
        if left.columns.len() != right.columns.len() {
            return Err(DbError::sql(
                "set operations require matching column counts",
            ));
        }
        let columns = left.columns.clone();
        let left_rows = left.into_rows();
        let right_rows = right.into_rows();
        let rows = match op {
            crate::sql::ast::SetOperation::Union => {
                let mut rows = left_rows;
                rows.extend(right_rows);
                if !all {
                    deduplicate_rows(rows)?
                } else {
                    rows
                }
            }
            crate::sql::ast::SetOperation::Intersect => {
                let right_counts = count_row_identities(&right_rows)?;
                let mut rows = Vec::new();
                if all {
                    let mut remaining = right_counts;
                    for row in left_rows {
                        let identity = row_identity(&row)?;
                        if consume_row_identity_count(&mut remaining, &identity) {
                            rows.push(row);
                        }
                    }
                } else {
                    for row in left_rows {
                        let identity = row_identity(&row)?;
                        if right_counts.contains_key(&identity) {
                            rows.push(row);
                        }
                    }
                    rows = deduplicate_rows(rows)?;
                }
                rows
            }
            crate::sql::ast::SetOperation::Except => {
                let right_counts = count_row_identities(&right_rows)?;
                let mut rows = Vec::new();
                if all {
                    let mut remaining = right_counts;
                    for row in left_rows {
                        let identity = row_identity(&row)?;
                        if !consume_row_identity_count(&mut remaining, &identity) {
                            rows.push(row);
                        }
                    }
                } else {
                    for row in left_rows {
                        let identity = row_identity(&row)?;
                        if !right_counts.contains_key(&identity) {
                            rows.push(row);
                        }
                    }
                    rows = deduplicate_rows(rows)?;
                }
                rows
            }
        };
        Ok(Dataset::with_rows(columns, rows))
    }
    pub(crate) fn project_dataset(
        &self,
        dataset: &Dataset,
        items: &[SelectItem],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        excluded: Option<&Dataset>,
    ) -> Result<Dataset> {
        if !self.security_masks_active()? {
            if let Some(projected) = try_project_simple_select_items(dataset, items)? {
                return Ok(projected);
            }
        }
        let window_values = self.compute_projection_window_values(dataset, items, params, ctes)?;
        let mut columns = Vec::new();
        for (index, item) in items.iter().enumerate() {
            match item {
                SelectItem::Expr { expr, alias } => {
                    let mut binding = ColumnBinding::visible(
                        None,
                        alias
                            .clone()
                            .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                    );
                    binding.column_type = super::type_of::declared_type(expr, dataset, params);
                    columns.push(binding);
                }
                SelectItem::Wildcard => columns.extend(
                    dataset
                        .columns
                        .iter()
                        .filter(|binding| !binding.hidden)
                        .map(ColumnBinding::as_output),
                ),
                SelectItem::QualifiedWildcard(table) => columns.extend(
                    dataset
                        .columns
                        .iter()
                        .filter(|column| {
                            !column.hidden && column.table.as_deref() == Some(table.as_str())
                        })
                        .map(ColumnBinding::as_output),
                ),
            }
        }
        let mut rows = Vec::with_capacity(dataset.rows.len());
        for (row_index, row) in dataset.rows.iter().enumerate() {
            let mut output = Vec::new();
            for (item_index, item) in items.iter().enumerate() {
                match item {
                    SelectItem::Expr { expr, .. } => match expr {
                        Expr::RowNumber { .. } | Expr::WindowFunction { .. } => output.push(
                            window_values[item_index]
                                .as_ref()
                                .and_then(|values| values.get(row_index))
                                .cloned()
                                .ok_or_else(|| {
                                    DbError::internal("window-function values were not precomputed")
                                })?,
                        ),
                        Expr::Column { table, column } if excluded.is_none() => {
                            let binding = dataset
                                .columns
                                .iter()
                                .find(|binding| {
                                    identifiers_equal(&binding.name, column)
                                        && match table {
                                            Some(table) => binding
                                                .table
                                                .as_deref()
                                                .is_some_and(|name| identifiers_equal(name, table)),
                                            None => true,
                                        }
                                })
                                .cloned()
                                .unwrap_or_else(|| {
                                    ColumnBinding::visible(table.clone(), column.clone())
                                });
                            let value =
                                self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                            output.push(self.masked_output_value(
                                &binding, &value, dataset, row, params, ctes,
                            )?);
                        }
                        _ => {
                            output.push(self.eval_expr(expr, dataset, row, params, ctes, excluded)?)
                        }
                    },
                    SelectItem::Wildcard => {
                        for (binding, value) in dataset.columns.iter().zip(row) {
                            if !binding.hidden {
                                output.push(self.masked_output_value(
                                    binding, value, dataset, row, params, ctes,
                                )?);
                            }
                        }
                    }
                    SelectItem::QualifiedWildcard(table) => {
                        for (binding, value) in dataset.columns.iter().zip(row) {
                            if !binding.hidden && binding.table.as_deref() == Some(table.as_str()) {
                                output.push(self.masked_output_value(
                                    binding, value, dataset, row, params, ctes,
                                )?);
                            }
                        }
                    }
                }
            }
            rows.push(output);
        }
        Ok(Dataset::with_rows(columns, rows))
    }
    fn compute_projection_window_values(
        &self,
        dataset: &Dataset,
        items: &[SelectItem],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Vec<Option<Vec<Value>>>> {
        let mut window_values = vec![None; items.len()];
        for item_index in 0..items.len() {
            if window_values[item_index].is_some() {
                continue;
            }
            match &items[item_index] {
                SelectItem::Expr {
                    expr:
                        Expr::RowNumber {
                            partition_by,
                            order_by,
                            frame,
                        },
                    ..
                } => {
                    if let Some(peer_index) = Self::find_row_number_lag_peer(items, item_index) {
                        let (row_number_values, lag_values) = self.compute_row_number_lag_values(
                            WindowEvalContext {
                                dataset,
                                params,
                                ctes,
                            },
                            partition_by,
                            order_by,
                            peer_index,
                            items,
                        )?;
                        window_values[item_index] = Some(row_number_values);
                        window_values[peer_index] = Some(lag_values);
                        continue;
                    }
                    if let Some(peer_index) = Self::find_row_number_rank_peer(items, item_index) {
                        let (row_number_values, rank_values) = self
                            .compute_row_number_rank_values(
                                dataset,
                                partition_by,
                                order_by,
                                params,
                                ctes,
                            )?;
                        window_values[item_index] = Some(row_number_values);
                        window_values[peer_index] = Some(rank_values);
                        continue;
                    }
                    window_values[item_index] = Some(self.compute_row_number_values(
                        dataset,
                        partition_by,
                        order_by,
                        frame.as_ref(),
                        params,
                        ctes,
                    )?);
                }
                SelectItem::Expr {
                    expr:
                        Expr::WindowFunction {
                            name,
                            args,
                            partition_by,
                            order_by,
                            frame,
                            distinct,
                            star,
                        },
                    ..
                } => {
                    if let Some(peer_index) = Self::find_rank_dense_rank_peer(items, item_index) {
                        let (rank_values, dense_rank_values) = self
                            .compute_rank_dense_rank_values(
                                dataset,
                                partition_by,
                                order_by,
                                params,
                                ctes,
                            )?;
                        if name.eq_ignore_ascii_case("rank") {
                            window_values[item_index] = Some(rank_values);
                            window_values[peer_index] = Some(dense_rank_values);
                        } else {
                            window_values[item_index] = Some(dense_rank_values);
                            window_values[peer_index] = Some(rank_values);
                        }
                        continue;
                    }
                    if name.eq_ignore_ascii_case("lag") {
                        if let Some(peer_index) = Self::find_lag_row_number_peer(items, item_index)
                        {
                            let (row_number_values, lag_values) = self
                                .compute_row_number_lag_values(
                                    WindowEvalContext {
                                        dataset,
                                        params,
                                        ctes,
                                    },
                                    partition_by,
                                    order_by,
                                    item_index,
                                    items,
                                )?;
                            window_values[item_index] = Some(lag_values);
                            window_values[peer_index] = Some(row_number_values);
                            continue;
                        }
                    }
                    if name.eq_ignore_ascii_case("rank") {
                        if let Some(peer_index) = Self::find_rank_row_number_peer(items, item_index)
                        {
                            let (row_number_values, rank_values) = self
                                .compute_row_number_rank_values(
                                    dataset,
                                    partition_by,
                                    order_by,
                                    params,
                                    ctes,
                                )?;
                            window_values[item_index] = Some(rank_values);
                            window_values[peer_index] = Some(row_number_values);
                            continue;
                        }
                    }
                    window_values[item_index] = Some(self.compute_window_function_values(
                        dataset,
                        name,
                        args,
                        partition_by,
                        order_by,
                        frame.as_ref(),
                        *distinct,
                        *star,
                        params,
                        ctes,
                    )?);
                }
                _ => {}
            }
        }
        Ok(window_values)
    }
    fn find_row_number_lag_peer(items: &[SelectItem], item_index: usize) -> Option<usize> {
        let SelectItem::Expr {
            expr:
                Expr::RowNumber {
                    partition_by,
                    order_by,
                    frame,
                },
            ..
        } = items.get(item_index)?
        else {
            return None;
        };
        items.iter().enumerate().find_map(|(peer_index, item)| {
            if peer_index == item_index {
                return None;
            }
            let SelectItem::Expr {
                expr:
                    Expr::WindowFunction {
                        name,
                        args,
                        partition_by: peer_partition_by,
                        order_by: peer_order_by,
                        frame: peer_frame,
                        distinct,
                        star,
                    },
                ..
            } = item
            else {
                return None;
            };
            (!*distinct
                && !*star
                && name.eq_ignore_ascii_case("lag")
                && args.len() == 1
                && peer_partition_by == partition_by
                && peer_order_by == order_by
                && peer_frame == frame)
                .then_some(peer_index)
        })
    }
    fn find_lag_row_number_peer(items: &[SelectItem], item_index: usize) -> Option<usize> {
        let SelectItem::Expr {
            expr:
                Expr::WindowFunction {
                    name,
                    args,
                    partition_by,
                    order_by,
                    frame,
                    distinct,
                    star,
                },
            ..
        } = items.get(item_index)?
        else {
            return None;
        };
        if *distinct || *star || !name.eq_ignore_ascii_case("lag") || args.len() != 1 {
            return None;
        }
        items.iter().enumerate().find_map(|(peer_index, item)| {
            if peer_index == item_index {
                return None;
            }
            let SelectItem::Expr {
                expr:
                    Expr::RowNumber {
                        partition_by: peer_partition_by,
                        order_by: peer_order_by,
                        frame: peer_frame,
                    },
                ..
            } = item
            else {
                return None;
            };
            (peer_partition_by == partition_by && peer_order_by == order_by && peer_frame == frame)
                .then_some(peer_index)
        })
    }
    fn find_row_number_rank_peer(items: &[SelectItem], item_index: usize) -> Option<usize> {
        let SelectItem::Expr {
            expr:
                Expr::RowNumber {
                    partition_by,
                    order_by,
                    frame,
                },
            ..
        } = items.get(item_index)?
        else {
            return None;
        };
        items.iter().enumerate().find_map(|(peer_index, item)| {
            if peer_index == item_index {
                return None;
            }
            let SelectItem::Expr {
                expr:
                    Expr::WindowFunction {
                        name,
                        args,
                        partition_by: peer_partition_by,
                        order_by: peer_order_by,
                        frame: peer_frame,
                        distinct,
                        star,
                    },
                ..
            } = item
            else {
                return None;
            };
            (!*distinct
                && !*star
                && args.is_empty()
                && name.eq_ignore_ascii_case("rank")
                && peer_partition_by == partition_by
                && peer_order_by == order_by
                && peer_frame == frame)
                .then_some(peer_index)
        })
    }
    fn find_rank_row_number_peer(items: &[SelectItem], item_index: usize) -> Option<usize> {
        let SelectItem::Expr {
            expr:
                Expr::WindowFunction {
                    name,
                    args,
                    partition_by,
                    order_by,
                    frame,
                    distinct,
                    star,
                },
            ..
        } = items.get(item_index)?
        else {
            return None;
        };
        if *distinct || *star || !args.is_empty() || !name.eq_ignore_ascii_case("rank") {
            return None;
        }
        items.iter().enumerate().find_map(|(peer_index, item)| {
            if peer_index == item_index {
                return None;
            }
            let SelectItem::Expr {
                expr:
                    Expr::RowNumber {
                        partition_by: peer_partition_by,
                        order_by: peer_order_by,
                        frame: peer_frame,
                    },
                ..
            } = item
            else {
                return None;
            };
            (peer_partition_by == partition_by && peer_order_by == order_by && peer_frame == frame)
                .then_some(peer_index)
        })
    }
    fn find_rank_dense_rank_peer(items: &[SelectItem], item_index: usize) -> Option<usize> {
        let SelectItem::Expr {
            expr:
                Expr::WindowFunction {
                    name,
                    args,
                    partition_by,
                    order_by,
                    frame,
                    distinct,
                    star,
                },
            ..
        } = items.get(item_index)?
        else {
            return None;
        };
        if *distinct || *star || !args.is_empty() {
            return None;
        }
        let target_name = if name.eq_ignore_ascii_case("rank") {
            "dense_rank"
        } else if name.eq_ignore_ascii_case("dense_rank") {
            "rank"
        } else {
            return None;
        };
        items.iter().enumerate().find_map(|(peer_index, item)| {
            if peer_index == item_index {
                return None;
            }
            let SelectItem::Expr {
                expr:
                    Expr::WindowFunction {
                        name: peer_name,
                        args: peer_args,
                        partition_by: peer_partition_by,
                        order_by: peer_order_by,
                        frame: peer_frame,
                        distinct: peer_distinct,
                        star: peer_star,
                    },
                ..
            } = item
            else {
                return None;
            };
            (!*peer_distinct
                && !*peer_star
                && peer_args.is_empty()
                && peer_name.eq_ignore_ascii_case(target_name)
                && peer_partition_by == partition_by
                && peer_order_by == order_by
                && peer_frame == frame)
                .then_some(peer_index)
        })
    }
    fn window_partitions(
        &self,
        dataset: &Dataset,
        partition_by: &[Expr],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<BTreeMap<Vec<u8>, Vec<usize>>> {
        let mut partitions = BTreeMap::<Vec<u8>, Vec<usize>>::new();
        let simple_positions = simple_window_column_positions(dataset, partition_by)?;
        for (row_index, row) in dataset.rows.iter().enumerate() {
            let key = if partition_by.is_empty() {
                vec![0]
            } else if let Some(positions) = simple_positions.as_ref() {
                window_key_from_positions(row, positions)?
            } else {
                let values = partition_by
                    .iter()
                    .map(|expr| self.eval_expr(expr, dataset, row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                row_identity(&values)?
            };
            partitions.entry(key).or_default().push(row_index);
        }
        Ok(partitions)
    }
    fn sorted_window_partition(
        &self,
        dataset: &Dataset,
        indices: Vec<usize>,
        order_by: &[crate::sql::ast::OrderBy],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Vec<WindowSortedRow>> {
        let mut sorted = Vec::with_capacity(indices.len());
        let simple_order_positions = simple_window_order_column_positions(dataset, order_by)?;
        for row_index in indices {
            let row = dataset
                .rows
                .get(row_index)
                .map(Vec::as_slice)
                .ok_or_else(|| DbError::internal("window row index is invalid"))?;
            let order_keys = if let Some(positions) = simple_order_positions.as_ref() {
                values_from_positions(row, positions)?
            } else {
                order_by
                    .iter()
                    .map(|order| self.eval_expr(&order.expr, dataset, row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?
            };
            sorted.push(WindowSortedRow {
                row_index,
                order_keys,
            });
        }
        sorted.sort_by(|left, right| compare_window_sorted_rows(left, right, order_by));
        Ok(sorted)
    }
    fn compute_sliding_rows_avg_values(
        &self,
        ctx: WindowEvalContext<'_>,
        sorted: &[usize],
        arg: &Expr,
        preceding: usize,
        results: &mut [Value],
    ) -> Result<()> {
        let ordered_values = sorted
            .iter()
            .map(|row_index| {
                let row = ctx
                    .dataset
                    .rows
                    .get(*row_index)
                    .map(Vec::as_slice)
                    .ok_or_else(|| DbError::internal("window row index is invalid"))?;
                self.eval_expr(arg, ctx.dataset, row, ctx.params, ctx.ctes, None)
            })
            .collect::<Result<Vec<_>>>()?;

        for (ordinal, row_index) in sorted.iter().enumerate() {
            let start = ordinal.saturating_sub(preceding);
            let mut total_float = 0_f64;
            let mut count = 0_i64;
            for value in &ordered_values[start..=ordinal] {
                match value {
                    Value::Null => {}
                    Value::Int64(value) => {
                        total_float += *value as f64;
                        count += 1;
                    }
                    Value::Float64(value) => {
                        total_float += *value;
                        count += 1;
                    }
                    Value::Decimal { scaled, scale } => {
                        total_float += (*scaled as f64) / 10_f64.powi(i32::from(*scale));
                        count += 1;
                    }
                    other => {
                        return Err(DbError::sql(format!(
                            "numeric aggregate does not support {other:?}"
                        )))
                    }
                }
            }
            results[*row_index] = if count == 0 {
                Value::Null
            } else {
                Value::Float64(total_float / count as f64)
            };
        }
        Ok(())
    }
    fn compute_rank_dense_rank_values(
        &self,
        dataset: &Dataset,
        partition_by: &[Expr],
        order_by: &[crate::sql::ast::OrderBy],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<(Vec<Value>, Vec<Value>)> {
        let partitions = self.window_partitions(dataset, partition_by, params, ctes)?;

        let mut rank_results = vec![Value::Null; dataset.rows.len()];
        let mut dense_rank_results = vec![Value::Null; dataset.rows.len()];
        for indices in partitions.into_values() {
            let sorted = self.sorted_window_partition(dataset, indices, order_by, params, ctes)?;
            let mut current_rank = 1_i64;
            let mut current_dense_rank = 1_i64;
            for (ordinal, sorted_row) in sorted.iter().enumerate() {
                if ordinal > 0
                    && !window_order_keys_equal(
                        &sorted[ordinal - 1].order_keys,
                        &sorted_row.order_keys,
                    )?
                {
                    current_rank = (ordinal + 1) as i64;
                    current_dense_rank += 1;
                }
                rank_results[sorted_row.row_index] = Value::Int64(current_rank);
                dense_rank_results[sorted_row.row_index] = Value::Int64(current_dense_rank);
            }
        }
        Ok((rank_results, dense_rank_results))
    }
    fn compute_row_number_rank_values(
        &self,
        dataset: &Dataset,
        partition_by: &[Expr],
        order_by: &[crate::sql::ast::OrderBy],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<(Vec<Value>, Vec<Value>)> {
        let partitions = self.window_partitions(dataset, partition_by, params, ctes)?;

        let mut row_number_results = vec![Value::Null; dataset.rows.len()];
        let mut rank_results = vec![Value::Null; dataset.rows.len()];
        for indices in partitions.into_values() {
            let sorted = self.sorted_window_partition(dataset, indices, order_by, params, ctes)?;
            let mut current_rank = 1_i64;
            for (ordinal, sorted_row) in sorted.iter().enumerate() {
                if ordinal > 0
                    && !window_order_keys_equal(
                        &sorted[ordinal - 1].order_keys,
                        &sorted_row.order_keys,
                    )?
                {
                    current_rank = (ordinal + 1) as i64;
                }
                row_number_results[sorted_row.row_index] = Value::Int64((ordinal + 1) as i64);
                rank_results[sorted_row.row_index] = Value::Int64(current_rank);
            }
        }
        Ok((row_number_results, rank_results))
    }
    fn compute_row_number_lag_values(
        &self,
        context: WindowEvalContext<'_>,
        partition_by: &[Expr],
        order_by: &[crate::sql::ast::OrderBy],
        lag_item_index: usize,
        items: &[SelectItem],
    ) -> Result<(Vec<Value>, Vec<Value>)> {
        let dataset = context.dataset;
        let SelectItem::Expr {
            expr: Expr::WindowFunction { args, .. },
            ..
        } = items
            .get(lag_item_index)
            .ok_or_else(|| DbError::internal("window lag item index is invalid"))?
        else {
            return Err(DbError::internal("window lag item index is invalid"));
        };
        let lag_expr = args
            .first()
            .ok_or_else(|| DbError::internal("window lag expression is missing"))?;
        let partitions =
            self.window_partitions(dataset, partition_by, context.params, context.ctes)?;

        let mut row_number_results = vec![Value::Null; dataset.rows.len()];
        let mut lag_results = vec![Value::Null; dataset.rows.len()];
        for indices in partitions.into_values() {
            let sorted = self.sorted_window_partition(
                dataset,
                indices,
                order_by,
                context.params,
                context.ctes,
            )?;

            let ordered_values = sorted
                .iter()
                .map(|sorted_row| {
                    self.eval_expr(
                        lag_expr,
                        dataset,
                        &dataset.rows[sorted_row.row_index],
                        context.params,
                        context.ctes,
                        None,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            for (ordinal, sorted_row) in sorted.iter().enumerate() {
                row_number_results[sorted_row.row_index] = Value::Int64((ordinal + 1) as i64);
                lag_results[sorted_row.row_index] = ordinal
                    .checked_sub(1)
                    .and_then(|previous| ordered_values.get(previous))
                    .cloned()
                    .unwrap_or(Value::Null);
            }
        }
        Ok((row_number_results, lag_results))
    }
    fn compute_row_number_values(
        &self,
        dataset: &Dataset,
        partition_by: &[Expr],
        order_by: &[crate::sql::ast::OrderBy],
        _frame: Option<&crate::sql::ast::WindowFrame>,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Vec<Value>> {
        let partitions = self.window_partitions(dataset, partition_by, params, ctes)?;

        let mut row_numbers = vec![Value::Null; dataset.rows.len()];
        for indices in partitions.into_values() {
            let sorted = self.sorted_window_partition(dataset, indices, order_by, params, ctes)?;

            for (ordinal, sorted_row) in sorted.into_iter().enumerate() {
                row_numbers[sorted_row.row_index] = Value::Int64((ordinal + 1) as i64);
            }
        }
        Ok(row_numbers)
    }
    #[allow(clippy::too_many_arguments)]
    fn compute_window_function_values(
        &self,
        dataset: &Dataset,
        name: &str,
        args: &[Expr],
        partition_by: &[Expr],
        order_by: &[crate::sql::ast::OrderBy],
        frame: Option<&crate::sql::ast::WindowFrame>,
        _distinct: bool,
        _star: bool,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Vec<Value>> {
        let partitions = self.window_partitions(dataset, partition_by, params, ctes)?;

        let mut results = vec![Value::Null; dataset.rows.len()];
        for indices in partitions.into_values() {
            let sorted_rows =
                self.sorted_window_partition(dataset, indices, order_by, params, ctes)?;
            let sorted = sorted_rows
                .iter()
                .map(|row| row.row_index)
                .collect::<Vec<_>>();
            let order_keys = sorted_rows
                .iter()
                .map(|row| row.order_keys.clone())
                .collect::<Vec<_>>();
            if name == "avg" && !_distinct && !_star && args.len() == 1 {
                if let Some(preceding) = rows_preceding_current_frame(frame) {
                    self.compute_sliding_rows_avg_values(
                        WindowEvalContext {
                            dataset,
                            params,
                            ctes,
                        },
                        &sorted,
                        &args[0],
                        preceding,
                        &mut results,
                    )?;
                    continue;
                }
            }
            let (peer_starts, peer_ends) = compute_window_peer_bounds(&order_keys)?;

            match name {
                "rank" => {
                    if _distinct || _star {
                        return Err(DbError::sql("RANK does not support DISTINCT or *"));
                    }
                    let mut current_rank = 1_i64;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        if ordinal > 0
                            && !window_order_keys_equal(
                                &order_keys[ordinal - 1],
                                &order_keys[ordinal],
                            )?
                        {
                            current_rank = (ordinal + 1) as i64;
                        }
                        results[*row_index] = Value::Int64(current_rank);
                    }
                }
                "dense_rank" => {
                    if _distinct || _star {
                        return Err(DbError::sql("DENSE_RANK does not support DISTINCT or *"));
                    }
                    let mut current_rank = 1_i64;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        if ordinal > 0
                            && !window_order_keys_equal(
                                &order_keys[ordinal - 1],
                                &order_keys[ordinal],
                            )?
                        {
                            current_rank += 1;
                        }
                        results[*row_index] = Value::Int64(current_rank);
                    }
                }
                "percent_rank" => {
                    if _distinct || _star || !args.is_empty() {
                        return Err(DbError::sql("PERCENT_RANK expects no arguments"));
                    }
                    if sorted.len() == 1 {
                        results[sorted[0]] = Value::Float64(0.0);
                        continue;
                    }
                    let mut current_rank = 1_i64;
                    let denominator = (sorted.len() - 1) as f64;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        if ordinal > 0
                            && !window_order_keys_equal(
                                &order_keys[ordinal - 1],
                                &order_keys[ordinal],
                            )?
                        {
                            current_rank = (ordinal + 1) as i64;
                        }
                        let value = (current_rank - 1) as f64 / denominator;
                        results[*row_index] = Value::Float64(value);
                    }
                }
                "cume_dist" => {
                    if _distinct || _star || !args.is_empty() {
                        return Err(DbError::sql("CUME_DIST expects no arguments"));
                    }
                    let partition_len = sorted.len() as f64;
                    let mut ordinal = 0_usize;
                    while ordinal < sorted.len() {
                        let peer_end = peer_ends[ordinal];
                        let value = Value::Float64((peer_end + 1) as f64 / partition_len);
                        for peer_ordinal in ordinal..=peer_end {
                            results[sorted[peer_ordinal]] = value.clone();
                        }
                        ordinal = peer_end + 1;
                    }
                }
                "ntile" => {
                    if _distinct || _star || args.len() != 1 {
                        return Err(DbError::sql("NTILE expects exactly 1 argument"));
                    }
                    let first_row = dataset
                        .rows
                        .get(sorted[0])
                        .map(Vec::as_slice)
                        .ok_or_else(|| DbError::internal("window row index is invalid"))?;
                    let buckets = match self
                        .eval_expr(&args[0], dataset, first_row, params, ctes, None)?
                    {
                        Value::Int64(value) if value > 0 => usize::try_from(value)
                            .map_err(|_| DbError::sql("NTILE bucket count is out of range"))?,
                        Value::Int64(_) => {
                            return Err(DbError::sql("NTILE bucket count must be greater than 0"))
                        }
                        Value::Null => {
                            return Err(DbError::sql("NTILE bucket count cannot be NULL"))
                        }
                        other => {
                            return Err(DbError::sql(format!(
                                "NTILE bucket count must be INT64, got {other:?}"
                            )))
                        }
                    };
                    let partition_len = sorted.len();
                    let base_size = partition_len / buckets;
                    let extra = partition_len % buckets;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let bucket = if ordinal < (base_size + 1) * extra {
                            (ordinal / (base_size + 1)) + 1
                        } else {
                            ((ordinal - (base_size + 1) * extra) / base_size.max(1)) + extra + 1
                        };
                        results[*row_index] = Value::Int64(bucket as i64);
                    }
                }
                "lag" | "lead" => {
                    if _distinct || _star {
                        return Err(DbError::sql(format!(
                            "{} does not support DISTINCT or *",
                            name.to_ascii_uppercase()
                        )));
                    }
                    if args.is_empty() || args.len() > 3 {
                        return Err(DbError::sql(format!(
                            "{} expects 1 to 3 arguments",
                            name.to_ascii_uppercase()
                        )));
                    }
                    let offset = match args.get(1) {
                        Some(expr) => {
                            match self.eval_expr(expr, dataset, &[], params, ctes, None)? {
                                Value::Int64(value) if value >= 0 => value as usize,
                                Value::Int64(_) => {
                                    return Err(DbError::sql(format!(
                                        "{} offset must be non-negative",
                                        name.to_ascii_uppercase()
                                    )))
                                }
                                other => {
                                    return Err(DbError::sql(format!(
                                        "{} offset must be INT64, got {other:?}",
                                        name.to_ascii_uppercase()
                                    )))
                                }
                            }
                        }
                        None => 1,
                    };
                    let ordered_values = sorted
                        .iter()
                        .map(|row_index| {
                            self.eval_expr(
                                &args[0],
                                dataset,
                                &dataset.rows[*row_index],
                                params,
                                ctes,
                                None,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let target_ordinal = if name == "lag" {
                            ordinal.checked_sub(offset)
                        } else {
                            ordinal
                                .checked_add(offset)
                                .filter(|target| *target < sorted.len())
                        };
                        results[*row_index] = if let Some(target_ordinal) = target_ordinal {
                            ordered_values[target_ordinal].clone()
                        } else if let Some(default_expr) = args.get(2) {
                            self.eval_expr(
                                default_expr,
                                dataset,
                                &dataset.rows[*row_index],
                                params,
                                ctes,
                                None,
                            )?
                        } else {
                            Value::Null
                        };
                    }
                }
                "first_value" | "last_value" => {
                    if _distinct || _star {
                        return Err(DbError::sql(format!(
                            "{} does not support DISTINCT or *",
                            name.to_ascii_uppercase()
                        )));
                    }
                    if args.len() != 1 {
                        return Err(DbError::sql(format!(
                            "{} expects exactly 1 argument",
                            name.to_ascii_uppercase()
                        )));
                    }
                    let ordered_values = sorted
                        .iter()
                        .map(|row_index| {
                            self.eval_expr(
                                &args[0],
                                dataset,
                                &dataset.rows[*row_index],
                                params,
                                ctes,
                                None,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let frame_range = self.window_frame_bounds_for_row(
                            dataset,
                            &sorted,
                            order_by,
                            &peer_starts,
                            &peer_ends,
                            ordinal,
                            frame,
                            params,
                            ctes,
                        )?;
                        results[*row_index] = if let Some((frame_start, frame_end)) = frame_range {
                            if name == "first_value" {
                                ordered_values[frame_start].clone()
                            } else {
                                ordered_values[frame_end].clone()
                            }
                        } else {
                            Value::Null
                        };
                    }
                }
                "nth_value" => {
                    if _distinct || _star {
                        return Err(DbError::sql("NTH_VALUE does not support DISTINCT or *"));
                    }
                    if args.len() != 2 {
                        return Err(DbError::sql(
                            "NTH_VALUE expects exactly 2 arguments".to_string(),
                        ));
                    }
                    let position =
                        match self.eval_expr(&args[1], dataset, &[], params, ctes, None)? {
                            Value::Int64(value) if value >= 1 => value as usize,
                            Value::Int64(_) => {
                                return Err(DbError::sql("NTH_VALUE position must be >= 1"))
                            }
                            other => {
                                return Err(DbError::sql(format!(
                                    "NTH_VALUE position must be INT64, got {other:?}"
                                )))
                            }
                        };
                    let ordered_values = sorted
                        .iter()
                        .map(|row_index| {
                            self.eval_expr(
                                &args[0],
                                dataset,
                                &dataset.rows[*row_index],
                                params,
                                ctes,
                                None,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let frame_range = self.window_frame_bounds_for_row(
                            dataset,
                            &sorted,
                            order_by,
                            &peer_starts,
                            &peer_ends,
                            ordinal,
                            frame,
                            params,
                            ctes,
                        )?;
                        results[*row_index] = if let Some((frame_start, frame_end)) = frame_range {
                            frame_start
                                .checked_add(position.saturating_sub(1))
                                .filter(|index| *index <= frame_end)
                                .and_then(|index| ordered_values.get(index))
                                .cloned()
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        };
                    }
                }
                "count" | "sum" | "avg" | "min" | "max" | "total" | "stddev" | "stddev_samp"
                | "stddev_pop" | "variance" | "var_samp" | "var_pop" | "bool_and" | "bool_or"
                | "group_concat" | "string_agg" => {
                    for (ordinal, row_index) in sorted.iter().enumerate() {
                        let frame_range = self.window_frame_bounds_for_row(
                            dataset,
                            &sorted,
                            order_by,
                            &peer_starts,
                            &peer_ends,
                            ordinal,
                            frame,
                            params,
                            ctes,
                        )?;
                        results[*row_index] = self.eval_window_aggregate(
                            name,
                            args,
                            _distinct,
                            _star,
                            dataset,
                            &sorted,
                            frame_range,
                            params,
                            ctes,
                        )?;
                    }
                }
                other => {
                    return Err(DbError::sql(format!(
                        "unsupported window function {}",
                        other.to_ascii_uppercase()
                    )))
                }
            }
        }
        Ok(results)
    }
    #[allow(clippy::too_many_arguments)]
    fn window_frame_bounds_for_row(
        &self,
        dataset: &Dataset,
        sorted: &[usize],
        order_by: &[crate::sql::ast::OrderBy],
        peer_starts: &[usize],
        peer_ends: &[usize],
        ordinal: usize,
        frame: Option<&crate::sql::ast::WindowFrame>,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<(usize, usize)>> {
        if sorted.is_empty() {
            return Ok(None);
        }

        if frame.is_none() {
            if order_by.is_empty() {
                return Ok(Some((0, sorted.len() - 1)));
            }
            return Ok(Some((0, peer_ends[ordinal])));
        }

        let frame = frame.ok_or_else(|| DbError::internal("window frame is missing"))?;
        let row_index = *sorted
            .get(ordinal)
            .ok_or_else(|| DbError::internal("window row index is invalid"))?;
        let row = dataset
            .rows
            .get(row_index)
            .map(Vec::as_slice)
            .ok_or_else(|| DbError::internal("window row index is invalid"))?;
        let default_end = crate::sql::ast::WindowFrameBound::CurrentRow;
        let end_bound = frame.end.as_ref().unwrap_or(&default_end);
        let start = self.window_frame_bound_index(
            dataset,
            row,
            &frame.start,
            true,
            ordinal,
            sorted.len(),
            peer_starts,
            peer_ends,
            frame.unit,
            params,
            ctes,
        )?;
        let end = self.window_frame_bound_index(
            dataset,
            row,
            end_bound,
            false,
            ordinal,
            sorted.len(),
            peer_starts,
            peer_ends,
            frame.unit,
            params,
            ctes,
        )?;
        normalize_window_frame_range(start, end, sorted.len())
    }
    #[allow(clippy::too_many_arguments)]
    fn window_frame_bound_index(
        &self,
        dataset: &Dataset,
        row: &[Value],
        bound: &crate::sql::ast::WindowFrameBound,
        start: bool,
        ordinal: usize,
        partition_len: usize,
        peer_starts: &[usize],
        peer_ends: &[usize],
        unit: crate::sql::ast::WindowFrameUnit,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<i64> {
        let partition_len = i64::try_from(partition_len)
            .map_err(|_| DbError::internal("window partition is too large"))?;
        let ordinal =
            i64::try_from(ordinal).map_err(|_| DbError::internal("window ordinal is too large"))?;
        match (unit, bound) {
            (
                crate::sql::ast::WindowFrameUnit::Range,
                crate::sql::ast::WindowFrameBound::Preceding(_)
                | crate::sql::ast::WindowFrameBound::Following(_),
            ) => Err(DbError::sql(
                "RANGE frames with offset bounds are not supported yet",
            )),
            (_, crate::sql::ast::WindowFrameBound::UnboundedPreceding) => Ok(0),
            (_, crate::sql::ast::WindowFrameBound::UnboundedFollowing) => {
                if start {
                    Ok(partition_len)
                } else {
                    Ok(partition_len - 1)
                }
            }
            (
                crate::sql::ast::WindowFrameUnit::Rows,
                crate::sql::ast::WindowFrameBound::CurrentRow,
            ) => Ok(ordinal),
            (
                crate::sql::ast::WindowFrameUnit::Range,
                crate::sql::ast::WindowFrameBound::CurrentRow,
            ) => {
                if start {
                    i64::try_from(peer_starts[ordinal as usize])
                        .map_err(|_| DbError::internal("window peer start is too large"))
                } else {
                    i64::try_from(peer_ends[ordinal as usize])
                        .map_err(|_| DbError::internal("window peer end is too large"))
                }
            }
            (
                crate::sql::ast::WindowFrameUnit::Rows,
                crate::sql::ast::WindowFrameBound::Preceding(offset),
            ) => {
                let offset = self.eval_window_frame_offset(dataset, row, offset, params, ctes)?;
                Ok(ordinal - offset)
            }
            (
                crate::sql::ast::WindowFrameUnit::Rows,
                crate::sql::ast::WindowFrameBound::Following(offset),
            ) => {
                let offset = self.eval_window_frame_offset(dataset, row, offset, params, ctes)?;
                Ok(ordinal + offset)
            }
        }
    }
    fn eval_window_frame_offset(
        &self,
        dataset: &Dataset,
        row: &[Value],
        offset: &Expr,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<i64> {
        match self.eval_expr(offset, dataset, row, params, ctes, None)? {
            Value::Int64(value) if value >= 0 => Ok(value),
            Value::Int64(_) => Err(DbError::sql(
                "window frame offset must be a non-negative integer",
            )),
            Value::Null => Err(DbError::sql("window frame offset cannot be NULL")),
            other => Err(DbError::sql(format!(
                "window frame offset must be INT64, got {other:?}"
            ))),
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn eval_window_aggregate(
        &self,
        name: &str,
        args: &[Expr],
        distinct: bool,
        star: bool,
        dataset: &Dataset,
        sorted_partition: &[usize],
        frame_range: Option<(usize, usize)>,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Value> {
        let aggregate_ctx = AggregateEvalContext {
            runtime: self,
            dataset,
            params,
            ctes,
        };
        let empty_indexes: [usize; 0] = [];
        let row_indexes = if let Some((start, end)) = frame_range {
            sorted_partition
                .get(start..=end)
                .ok_or_else(|| DbError::internal("window frame range is invalid"))?
        } else {
            &empty_indexes
        };

        match name {
            "count" => {
                if star {
                    if distinct {
                        return Err(DbError::sql("COUNT(DISTINCT *) is not supported"));
                    }
                    return Ok(Value::Int64(row_indexes.len() as i64));
                }
                if args.len() != 1 {
                    return Err(DbError::sql("COUNT expects exactly 1 argument"));
                }
                if distinct {
                    let mut vals = Vec::new();
                    for row_index in row_indexes {
                        let row = dataset
                            .rows
                            .get(*row_index)
                            .map(Vec::as_slice)
                            .ok_or_else(|| DbError::internal("window row index is invalid"))?;
                        let val = self.eval_expr(&args[0], dataset, row, params, ctes, None)?;
                        if !matches!(val, Value::Null) {
                            vals.push(val);
                        }
                    }
                    vals.sort_by(|a, b| compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
                    vals.dedup_by(|a, b| {
                        compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
                            == std::cmp::Ordering::Equal
                    });
                    Ok(Value::Int64(vals.len() as i64))
                } else {
                    let mut count = 0_i64;
                    for row_index in row_indexes {
                        let row = dataset
                            .rows
                            .get(*row_index)
                            .map(Vec::as_slice)
                            .ok_or_else(|| DbError::internal("window row index is invalid"))?;
                        if !matches!(
                            self.eval_expr(&args[0], dataset, row, params, ctes, None)?,
                            Value::Null
                        ) {
                            count += 1;
                        }
                    }
                    Ok(Value::Int64(count))
                }
            }
            "sum" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("SUM expects exactly 1 argument"));
                }
                aggregate_numeric(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    NumericAgg::Sum,
                    distinct,
                )
            }
            "avg" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("AVG expects exactly 1 argument"));
                }
                aggregate_numeric(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    NumericAgg::Avg,
                    distinct,
                )
            }
            "total" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("TOTAL expects exactly 1 argument"));
                }
                aggregate_numeric(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    NumericAgg::Total,
                    distinct,
                )
            }
            "stddev" | "stddev_samp" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("STDDEV expects exactly 1 argument"));
                }
                aggregate_variance(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    VarianceAgg::StddevSamp,
                    distinct,
                )
            }
            "stddev_pop" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("STDDEV_POP expects exactly 1 argument"));
                }
                aggregate_variance(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    VarianceAgg::StddevPop,
                    distinct,
                )
            }
            "variance" | "var_samp" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("VAR_SAMP expects exactly 1 argument"));
                }
                aggregate_variance(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    VarianceAgg::VarSamp,
                    distinct,
                )
            }
            "var_pop" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("VAR_POP expects exactly 1 argument"));
                }
                aggregate_variance(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    VarianceAgg::VarPop,
                    distinct,
                )
            }
            "bool_and" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("BOOL_AND expects exactly 1 argument"));
                }
                aggregate_bool(
                    &aggregate_ctx,
                    row_indexes,
                    &args[0],
                    BoolAgg::And,
                    distinct,
                )
            }
            "bool_or" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("BOOL_OR expects exactly 1 argument"));
                }
                aggregate_bool(&aggregate_ctx, row_indexes, &args[0], BoolAgg::Or, distinct)
            }
            "min" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("MIN expects exactly 1 argument"));
                }
                aggregate_extreme(self, dataset, row_indexes, &args[0], params, ctes, true)
            }
            "max" => {
                if star || args.len() != 1 {
                    return Err(DbError::sql("MAX expects exactly 1 argument"));
                }
                aggregate_extreme(self, dataset, row_indexes, &args[0], params, ctes, false)
            }
            name @ ("group_concat" | "string_agg") => {
                if star {
                    return Err(DbError::sql(format!(
                        "{} does not support *",
                        name.to_ascii_uppercase()
                    )));
                }
                if distinct {
                    return Err(DbError::sql(format!(
                        "{} DISTINCT is not supported in window context",
                        name.to_ascii_uppercase()
                    )));
                }
                aggregate_group_concat(&aggregate_ctx, row_indexes, args, false, &[], name)
            }
            other => Err(DbError::sql(format!(
                "unsupported aggregate window function {other}"
            ))),
        }
    }
    pub(crate) fn evaluate_grouped_select(
        &self,
        select: &Select,
        dataset: Dataset,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        if select.group_by.iter().any(expr_contains_collation) {
            return Err(DbError::sql(
                "COLLATE in GROUP BY keys is not supported in this compatibility slice",
            ));
        }
        let mut groups = BTreeMap::<Vec<u8>, Vec<usize>>::new();
        if dataset.rows.is_empty() && select.group_by.is_empty() {
            groups.insert(Vec::new(), Vec::new());
        } else {
            for (row_index, row) in dataset.rows.iter().enumerate() {
                let key_values = select
                    .group_by
                    .iter()
                    .map(|expr| self.eval_expr(expr, &dataset, row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                groups
                    .entry(row_identity(&key_values)?)
                    .or_default()
                    .push(row_index);
            }
        }
        let columns = select
            .projection
            .iter()
            .enumerate()
            .map(|(index, item)| match item {
                SelectItem::Expr { expr, alias } => {
                    let mut binding = ColumnBinding::visible(
                        None,
                        alias
                            .clone()
                            .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                    );
                    binding.column_type = super::type_of::declared_type(expr, &dataset, params);
                    binding
                }
                SelectItem::Wildcard => ColumnBinding::visible(None, format!("col{}", index + 1)),
                SelectItem::QualifiedWildcard(_) => {
                    ColumnBinding::visible(None, format!("col{}", index + 1))
                }
            })
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for group_row_indexes in groups.into_values() {
            if let Some(having) = &select.having {
                if !matches!(
                    self.eval_group_expr(having, &dataset, &group_row_indexes, params, ctes)?,
                    Value::Bool(true)
                ) {
                    continue;
                }
            }
            let mut output = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::Expr { expr, .. } => output.push(self.eval_group_expr(
                        expr,
                        &dataset,
                        &group_row_indexes,
                        params,
                        ctes,
                    )?),
                    SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                        return Err(DbError::sql(
                            "wildcards are not supported in grouped SELECT output",
                        ))
                    }
                }
            }
            rows.push(output);
        }
        Ok(Dataset::with_rows(columns, rows))
    }
    pub(crate) fn sort_dataset(
        &self,
        dataset: &mut Dataset,
        order_by: &[crate::sql::ast::OrderBy],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<()> {
        if order_by.is_empty() || dataset.rows.len() <= 1 {
            return Ok(());
        }
        let eval_dataset = Dataset::with_rows(dataset.columns.clone(), Vec::new());
        let projected_order_indexes = order_by
            .iter()
            .map(|order| projected_dataset_order_column_index(dataset, &order.expr))
            .collect::<Vec<_>>();
        let sort_keys = dataset
            .rows
            .iter()
            .map(|row| {
                order_by
                    .iter()
                    .zip(&projected_order_indexes)
                    .map(|(order, projected_index)| {
                        if let Some(index) = projected_index {
                            row.get(*index).cloned().unwrap_or(Value::Null)
                        } else {
                            self.eval_expr(&order.expr, &eval_dataset, row, params, ctes, None)
                                .unwrap_or(Value::Null)
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let mut sort_error = None;
        let mut order = (0..dataset.rows.len()).collect::<Vec<_>>();
        order.sort_by(|left_index, right_index| {
            let left_key = &sort_keys[*left_index];
            let right_key = &sort_keys[*right_index];
            for (order_clause, (left_value, right_value)) in
                order_by.iter().zip(left_key.iter().zip(right_key.iter()))
            {
                let ordering = match compare_values_with_runtime_collation(
                    Some(self),
                    left_value,
                    right_value,
                    order_clause.collation.clone(),
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
                    return if order_clause.descending {
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

        let mut rows = dataset
            .take_rows()
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        dataset.set_rows(
            order
                .into_iter()
                .map(|row_index| {
                    rows.get_mut(row_index)
                        .and_then(Option::take)
                        .ok_or_else(|| DbError::internal("sorted row index is invalid"))
                })
                .collect::<Result<Vec<_>>>()?,
        );
        Ok(())
    }
    pub(crate) fn eval_constant_i64(
        &self,
        expr: &Expr,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<i64> {
        match self.eval_expr(expr, &Dataset::empty(), &[], params, ctes, None)? {
            Value::Int64(value) => Ok(value),
            other => Err(DbError::sql(format!(
                "expected integer constant, got {other:?}"
            ))),
        }
    }
    fn eval_group_membership_value(
        &self,
        expr: &Expr,
        dataset: &Dataset,
        group_row_indexes: &[usize],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<MembershipValue> {
        match expr {
            Expr::Row(items) => Ok(MembershipValue::Row(
                items
                    .iter()
                    .map(|item| {
                        self.eval_group_expr(item, dataset, group_row_indexes, params, ctes)
                    })
                    .collect::<Result<Vec<_>>>()?,
            )),
            _ => Ok(MembershipValue::Scalar(self.eval_group_expr(
                expr,
                dataset,
                group_row_indexes,
                params,
                ctes,
            )?)),
        }
    }
    fn eval_membership_value(
        &self,
        expr: &Expr,
        dataset: &Dataset,
        row: &[Value],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        excluded: Option<&Dataset>,
    ) -> Result<MembershipValue> {
        match expr {
            Expr::Row(items) => Ok(MembershipValue::Row(
                items
                    .iter()
                    .map(|item| self.eval_expr(item, dataset, row, params, ctes, excluded))
                    .collect::<Result<Vec<_>>>()?,
            )),
            _ => Ok(MembershipValue::Scalar(
                self.eval_expr(expr, dataset, row, params, ctes, excluded)?,
            )),
        }
    }
    pub(crate) fn eval_group_expr(
        &self,
        expr: &Expr,
        dataset: &Dataset,
        group_row_indexes: &[usize],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Value> {
        let aggregate_ctx = AggregateEvalContext {
            runtime: self,
            dataset,
            params,
            ctes,
        };
        match expr {
            Expr::Aggregate {
                name,
                args,
                star,
                distinct,
                order_by,
                within_group,
            } => match name.as_str() {
                "array_agg" | "median" | "percentile_cont" | "percentile_disc" => {
                    match name.as_str() {
                        "array_agg" => {
                            if *within_group {
                                return Err(DbError::sql(
                                    "ARRAY_AGG does not support WITHIN GROUP",
                                ));
                            }
                            if *star || args.len() != 1 {
                                return Err(DbError::sql("ARRAY_AGG expects exactly 1 argument"));
                            }
                            aggregate_array_agg(
                                &aggregate_ctx,
                                group_row_indexes,
                                &args[0],
                                *distinct,
                                order_by,
                            )
                        }
                        "median" => {
                            if *within_group {
                                return Err(DbError::sql(
                                    "MEDIAN does not support WITHIN GROUP; use MEDIAN(expr)",
                                ));
                            }
                            if !order_by.is_empty() {
                                return Err(DbError::sql(
                                    "MEDIAN does not support aggregate ORDER BY",
                                ));
                            }
                            if *star || args.len() != 1 {
                                return Err(DbError::sql("MEDIAN expects exactly 1 argument"));
                            }
                            aggregate_median(&aggregate_ctx, group_row_indexes, &args[0], *distinct)
                        }
                        "percentile_cont" => {
                            if !*within_group {
                                return Err(DbError::sql(
                                    "PERCENTILE_CONT requires WITHIN GROUP (ORDER BY ...)",
                                ));
                            }
                            if *distinct {
                                return Err(DbError::sql(
                                    "PERCENTILE_CONT does not support DISTINCT",
                                ));
                            }
                            if *star || args.len() != 1 {
                                return Err(DbError::sql(
                                    "PERCENTILE_CONT expects exactly 1 argument",
                                ));
                            }
                            aggregate_percentile_cont(
                                self,
                                dataset,
                                group_row_indexes,
                                &args[0],
                                order_by,
                                params,
                                ctes,
                            )
                        }
                        "percentile_disc" => {
                            if !*within_group {
                                return Err(DbError::sql(
                                    "PERCENTILE_DISC requires WITHIN GROUP (ORDER BY ...)",
                                ));
                            }
                            if *distinct {
                                return Err(DbError::sql(
                                    "PERCENTILE_DISC does not support DISTINCT",
                                ));
                            }
                            if *star || args.len() != 1 {
                                return Err(DbError::sql(
                                    "PERCENTILE_DISC expects exactly 1 argument",
                                ));
                            }
                            aggregate_percentile_disc(
                                self,
                                dataset,
                                group_row_indexes,
                                &args[0],
                                order_by,
                                params,
                                ctes,
                            )
                        }
                        _ => Err(DbError::sql(format!(
                            "unsupported aggregate function {}",
                            name.to_ascii_uppercase()
                        ))),
                    }
                }
                name if *within_group => Err(DbError::sql(format!(
                    "{} does not support WITHIN GROUP",
                    name.to_ascii_uppercase()
                ))),
                name if !order_by.is_empty() && !matches!(name, "group_concat" | "string_agg") => {
                    Err(DbError::sql(format!(
                        "{} does not support aggregate ORDER BY",
                        name.to_ascii_uppercase()
                    )))
                }
                "count" => {
                    if *star {
                        Ok(Value::Int64(group_row_indexes.len() as i64))
                    } else if *distinct {
                        let mut vals = Vec::new();
                        for row_index in group_row_indexes {
                            let row =
                                dataset.rows.get(*row_index).map(Vec::as_slice).ok_or_else(
                                    || DbError::internal("group row index is invalid"),
                                )?;
                            let val = self.eval_expr(&args[0], dataset, row, params, ctes, None)?;
                            if !matches!(val, Value::Null) {
                                vals.push(val);
                            }
                        }
                        vals.sort_by(|a, b| {
                            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        vals.dedup_by(|a, b| {
                            compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
                                == std::cmp::Ordering::Equal
                        });
                        Ok(Value::Int64(vals.len() as i64))
                    } else {
                        let mut count = 0_i64;
                        for row_index in group_row_indexes {
                            let row =
                                dataset.rows.get(*row_index).map(Vec::as_slice).ok_or_else(
                                    || DbError::internal("group row index is invalid"),
                                )?;
                            if !matches!(
                                self.eval_expr(&args[0], dataset, row, params, ctes, None)?,
                                Value::Null
                            ) {
                                count += 1;
                            }
                        }
                        Ok(Value::Int64(count))
                    }
                }
                "sum" => aggregate_numeric(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    NumericAgg::Sum,
                    *distinct,
                ),
                "avg" => aggregate_numeric(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    NumericAgg::Avg,
                    *distinct,
                ),
                "total" => aggregate_numeric(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    NumericAgg::Total,
                    *distinct,
                ),
                "stddev" | "stddev_samp" => aggregate_variance(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    VarianceAgg::StddevSamp,
                    *distinct,
                ),
                "stddev_pop" => aggregate_variance(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    VarianceAgg::StddevPop,
                    *distinct,
                ),
                "variance" | "var_samp" => aggregate_variance(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    VarianceAgg::VarSamp,
                    *distinct,
                ),
                "var_pop" => aggregate_variance(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    VarianceAgg::VarPop,
                    *distinct,
                ),
                "bool_and" => aggregate_bool(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    BoolAgg::And,
                    *distinct,
                ),
                "bool_or" => aggregate_bool(
                    &aggregate_ctx,
                    group_row_indexes,
                    &args[0],
                    BoolAgg::Or,
                    *distinct,
                ),
                "min" => aggregate_extreme(
                    self,
                    dataset,
                    group_row_indexes,
                    &args[0],
                    params,
                    ctes,
                    true,
                ),
                "max" => aggregate_extreme(
                    self,
                    dataset,
                    group_row_indexes,
                    &args[0],
                    params,
                    ctes,
                    false,
                ),
                name @ ("group_concat" | "string_agg") => aggregate_group_concat(
                    &aggregate_ctx,
                    group_row_indexes,
                    args,
                    *distinct,
                    order_by,
                    name,
                ),
                other => {
                    let mut arg_rows = Vec::with_capacity(group_row_indexes.len());
                    for row_index in group_row_indexes {
                        let row = dataset
                            .rows
                            .get(*row_index)
                            .map(Vec::as_slice)
                            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
                        let values = args
                            .iter()
                            .map(|arg| self.eval_expr(arg, dataset, row, params, ctes, None))
                            .collect::<Result<Vec<_>>>()?;
                        arg_rows.push(values);
                    }
                    if let Some(value) =
                        crate::extensions::invoke_aggregate_from_runtime(self, other, arg_rows)?
                    {
                        return Ok(value);
                    }
                    Err(DbError::sql(format!(
                        "unsupported aggregate function {other}"
                    )))
                }
            },
            Expr::Unary { op, expr } => {
                let value = self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?;
                match op {
                    UnaryOp::Not => Ok(match truthy(&value) {
                        Some(value) => Value::Bool(!value),
                        None => Value::Null,
                    }),
                    UnaryOp::Negate => match value {
                        Value::Int64(value) => Ok(Value::Int64(-value)),
                        Value::Float64(value) => Ok(Value::Float64(-value)),
                        Value::Decimal { scaled, scale } => Ok(Value::Decimal {
                            scaled: scaled
                                .checked_neg()
                                .ok_or_else(|| DbError::sql("DECIMAL arithmetic overflow"))?,
                            scale,
                        }),
                        Value::Null => Ok(Value::Null),
                        other => Err(DbError::sql(format!("cannot negate {other:?}"))),
                    },
                }
            }
            Expr::Binary { left, op, right } => {
                let collation = expr_collation(left).or_else(|| expr_collation(right));
                let (left_value, right_value) = coerce_binary_literals(
                    op,
                    left,
                    right,
                    self.eval_group_expr(left, dataset, group_row_indexes, params, ctes)?,
                    self.eval_group_expr(right, dataset, group_row_indexes, params, ctes)?,
                )?;
                eval_binary_with_collation(Some(self), op, left_value, right_value, collation)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value = self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?;
                let low = self.eval_group_expr(low, dataset, group_row_indexes, params, ctes)?;
                let high = self.eval_group_expr(high, dataset, group_row_indexes, params, ctes)?;
                if matches!(value, Value::Null)
                    || matches!(low, Value::Null)
                    || matches!(high, Value::Null)
                {
                    return Ok(Value::Null);
                }
                let collation = expr_collation(expr);
                let in_range = compare_values_with_runtime_collation(
                    Some(self),
                    &value,
                    &low,
                    collation.clone(),
                )? != std::cmp::Ordering::Less
                    && compare_values_with_runtime_collation(Some(self), &value, &high, collation)?
                        != std::cmp::Ordering::Greater;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::InList {
                expr,
                items,
                negated,
            } => {
                let value = self.eval_group_membership_value(
                    expr,
                    dataset,
                    group_row_indexes,
                    params,
                    ctes,
                )?;
                if membership_value_has_nulls(&value) {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for item in items {
                    let candidate = self.eval_group_membership_value(
                        item,
                        dataset,
                        group_row_indexes,
                        params,
                        ctes,
                    )?;
                    match compare_membership_values(&value, &candidate)? {
                        Some(true) => return Ok(Value::Bool(!*negated)),
                        Some(false) => {}
                        None => saw_null = true,
                    }
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
                ..
            } => {
                let left = self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?;
                let right =
                    self.eval_group_expr(pattern, dataset, group_row_indexes, params, ctes)?;
                let escape = escape
                    .as_ref()
                    .map(|expr| {
                        self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)
                    })
                    .transpose()?;
                eval_like(left, right, escape, *case_insensitive, *negated)
            }
            Expr::IsNull { expr, negated } => {
                let is_null = matches!(
                    self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?,
                    Value::Null
                );
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::Function { name, args } => {
                let args_contain_aggregate = args.iter().any(expr_contains_aggregate)
                    || args.iter().try_fold(false, |found, arg| {
                        if found {
                            Ok(true)
                        } else {
                            expr_contains_runtime_extension_aggregate(self, arg)
                        }
                    })?;
                if !args_contain_aggregate {
                    let mut arg_rows = Vec::with_capacity(group_row_indexes.len());
                    for row_index in group_row_indexes {
                        let row = dataset
                            .rows
                            .get(*row_index)
                            .map(Vec::as_slice)
                            .ok_or_else(|| DbError::internal("group row index is invalid"))?;
                        let values = args
                            .iter()
                            .map(|arg| self.eval_expr(arg, dataset, row, params, ctes, None))
                            .collect::<Result<Vec<_>>>()?;
                        arg_rows.push(values);
                    }
                    if let Some(value) =
                        crate::extensions::invoke_aggregate_from_runtime(self, name, arg_rows)?
                    {
                        return Ok(value);
                    }
                }
                let row = if let Some(row_index) = group_row_indexes.first().copied() {
                    dataset
                        .rows
                        .get(row_index)
                        .map(Vec::as_slice)
                        .ok_or_else(|| DbError::internal("group row index is invalid"))?
                } else {
                    &[]
                };
                let values = args
                    .iter()
                    .map(|arg| self.eval_group_expr(arg, dataset, group_row_indexes, params, ctes))
                    .collect::<Result<Vec<_>>>()?;
                match name.as_str() {
                    "pg_typeof" | "pg_catalog.pg_typeof" => {
                        if args.len() != 1 {
                            return Err(DbError::sql("PG_TYPEOF expects 1 argument"));
                        }
                        Ok(Value::Text(
                            super::type_of::type_name(&args[0], &values[0], dataset, params)
                                .to_string(),
                        ))
                    }
                    "coalesce" => Ok(values
                        .into_iter()
                        .find(|value| !matches!(value, Value::Null))
                        .unwrap_or(Value::Null)),
                    "nullif" => {
                        if values.len() != 2 {
                            return Err(DbError::sql("NULLIF expects exactly two arguments"));
                        }
                        if compare_values(&values[0], &values[1])? == std::cmp::Ordering::Equal {
                            Ok(Value::Null)
                        } else {
                            Ok(values[0].clone())
                        }
                    }
                    "length" => unary_text_fn(values, |value| value.len().to_string())
                        .and_then(|value| cast_value(value, crate::catalog::ColumnType::Int64)),
                    "lower" => unary_text_fn(values, |value| value.to_ascii_lowercase()),
                    "upper" => unary_text_fn(values, |value| value.to_ascii_uppercase()),
                    "trim" => unary_text_fn(values, |value| value.trim().to_string()),
                    other => self.eval_expr(
                        &Expr::Function {
                            name: other.to_string(),
                            args: args.to_vec(),
                        },
                        dataset,
                        row,
                        params,
                        ctes,
                        None,
                    ),
                }
            }
            Expr::Case {
                operand,
                branches,
                else_expr,
            } => {
                let operand_value = operand
                    .as_deref()
                    .map(|expr| {
                        self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)
                    })
                    .transpose()?;
                for (condition, result) in branches {
                    let matches = if let Some(operand_value) = &operand_value {
                        compare_values(
                            operand_value,
                            &self.eval_group_expr(
                                condition,
                                dataset,
                                group_row_indexes,
                                params,
                                ctes,
                            )?,
                        )? == std::cmp::Ordering::Equal
                    } else {
                        matches!(
                            self.eval_group_expr(
                                condition,
                                dataset,
                                group_row_indexes,
                                params,
                                ctes,
                            )?,
                            Value::Bool(true)
                        )
                    };
                    if matches {
                        return self.eval_group_expr(
                            result,
                            dataset,
                            group_row_indexes,
                            params,
                            ctes,
                        );
                    }
                }
                else_expr
                    .as_deref()
                    .map(|expr| {
                        self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)
                    })
                    .transpose()?
                    .map_or(Ok(Value::Null), Ok)
            }
            Expr::Cast { expr, target_type } => cast_value(
                self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)?,
                *target_type,
            ),
            Expr::Collate { expr, .. } => {
                self.eval_group_expr(expr, dataset, group_row_indexes, params, ctes)
            }
            Expr::Row(_) => Err(DbError::sql(
                "row values are only supported in IN comparisons",
            )),
            Expr::RowNumber { .. } | Expr::WindowFunction { .. } => Err(DbError::sql(
                "window functions cannot be nested inside grouped expressions",
            )),
            _ => {
                let row = if let Some(row_index) = group_row_indexes.first().copied() {
                    dataset
                        .rows
                        .get(row_index)
                        .map(Vec::as_slice)
                        .ok_or_else(|| DbError::internal("group row index is invalid"))?
                } else {
                    &[]
                };
                self.eval_expr(expr, dataset, row, params, ctes, None)
            }
        }
    }
    pub(crate) fn eval_expr(
        &self,
        expr: &Expr,
        dataset: &Dataset,
        row: &[Value],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        excluded: Option<&Dataset>,
    ) -> Result<Value> {
        match expr {
            Expr::Default => Err(DbError::sql(
                "DEFAULT requires an INSERT or UPDATE column context",
            )),
            Expr::Literal(value) => Ok(value.clone()),
            Expr::Column { table, column } => {
                self.resolve_column(dataset, row, table.as_deref(), column, excluded)
            }
            Expr::Parameter(number) => params
                .get(number.saturating_sub(1))
                .cloned()
                .ok_or_else(|| DbError::sql(format!("missing value for parameter ${number}"))),
            Expr::Unary { op, expr } => {
                let value = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                match op {
                    UnaryOp::Not => Ok(match truthy(&value) {
                        Some(value) => Value::Bool(!value),
                        None => Value::Null,
                    }),
                    UnaryOp::Negate => match value {
                        Value::Int64(value) => Ok(Value::Int64(-value)),
                        Value::Float64(value) => Ok(Value::Float64(-value)),
                        Value::Decimal { scaled, scale } => Ok(Value::Decimal {
                            scaled: scaled
                                .checked_neg()
                                .ok_or_else(|| DbError::sql("DECIMAL arithmetic overflow"))?,
                            scale,
                        }),
                        Value::Null => Ok(Value::Null),
                        other => Err(DbError::sql(format!("cannot negate {other:?}"))),
                    },
                }
            }
            Expr::Binary { left, op, right } => {
                let collation = expr_collation(left).or_else(|| expr_collation(right));
                let left_value = self.eval_expr(left, dataset, row, params, ctes, excluded)?;
                let right_value = self.eval_expr(right, dataset, row, params, ctes, excluded)?;
                let (left_value, right_value) =
                    coerce_binary_literals(op, left, right, left_value, right_value)?;
                eval_binary_with_collation(Some(self), op, left_value, right_value, collation)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                let low = self.eval_expr(low, dataset, row, params, ctes, excluded)?;
                let high = self.eval_expr(high, dataset, row, params, ctes, excluded)?;
                if matches!(value, Value::Null)
                    || matches!(low, Value::Null)
                    || matches!(high, Value::Null)
                {
                    return Ok(Value::Null);
                }
                let collation = expr_collation(expr);
                let in_range = compare_values_with_runtime_collation(
                    Some(self),
                    &value,
                    &low,
                    collation.clone(),
                )? != std::cmp::Ordering::Less
                    && compare_values_with_runtime_collation(Some(self), &value, &high, collation)?
                        != std::cmp::Ordering::Greater;
                Ok(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::InList {
                expr,
                items,
                negated,
            } => {
                let value =
                    self.eval_membership_value(expr, dataset, row, params, ctes, excluded)?;
                if membership_value_has_nulls(&value) {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for item in items {
                    let candidate =
                        self.eval_membership_value(item, dataset, row, params, ctes, excluded)?;
                    match compare_membership_values(&value, &candidate)? {
                        Some(true) => return Ok(Value::Bool(!*negated)),
                        Some(false) => {}
                        None => saw_null = true,
                    }
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::InSubquery {
                expr,
                query,
                negated,
            } => {
                let value =
                    self.eval_membership_value(expr, dataset, row, params, ctes, excluded)?;
                if membership_value_has_nulls(&value) {
                    return Ok(Value::Null);
                }
                let subquery = self.evaluate_query_with_outer(query, params, ctes, dataset, row)?;
                let expected_width = match &value {
                    MembershipValue::Scalar(_) => 1,
                    MembershipValue::Row(values) => values.len(),
                };
                if subquery.columns.len() != expected_width {
                    return Err(DbError::sql(format!(
                        "IN subquery must return exactly {} column{}",
                        expected_width,
                        if expected_width == 1 { "" } else { "s" }
                    )));
                }
                let mut saw_null = false;
                for subquery_row in subquery.rows.iter() {
                    let candidate = if expected_width == 1 {
                        MembershipValue::Scalar(
                            subquery_row.first().cloned().unwrap_or(Value::Null),
                        )
                    } else {
                        MembershipValue::Row(subquery_row.clone())
                    };
                    match compare_membership_values(&value, &candidate)? {
                        Some(true) => return Ok(Value::Bool(!*negated)),
                        Some(false) => {}
                        None => saw_null = true,
                    }
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(*negated))
                }
            }
            Expr::CompareSubquery {
                expr,
                op,
                quantifier,
                query,
            } => {
                let left_value = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                let subquery = self.evaluate_query_with_outer(query, params, ctes, dataset, row)?;
                if subquery.columns.len() != 1 {
                    return Err(DbError::sql(
                        "subquery comparison must return exactly one column",
                    ));
                }
                let mut saw_null = false;
                let mut saw_row = false;
                for subquery_row in subquery.rows.iter() {
                    saw_row = true;
                    let candidate = subquery_row.first().cloned().unwrap_or(Value::Null);
                    match eval_binary_with_collation(
                        Some(self),
                        op,
                        left_value.clone(),
                        candidate,
                        expr_collation(expr),
                    )? {
                        Value::Bool(result) => match quantifier {
                            SubqueryQuantifier::Any if result => return Ok(Value::Bool(true)),
                            SubqueryQuantifier::All if !result => return Ok(Value::Bool(false)),
                            _ => {}
                        },
                        Value::Null => saw_null = true,
                        other => {
                            return Err(DbError::internal(format!(
                                "subquery comparison did not evaluate to boolean: {other:?}"
                            )))
                        }
                    }
                }
                if !saw_row {
                    return Ok(Value::Bool(matches!(quantifier, SubqueryQuantifier::All)));
                }
                if saw_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(matches!(quantifier, SubqueryQuantifier::All)))
                }
            }
            Expr::ScalarSubquery(query) => {
                let subquery = self.evaluate_query_with_outer(query, params, ctes, dataset, row)?;
                if subquery.columns.len() != 1 {
                    return Err(DbError::sql(
                        "scalar subquery must return exactly one column",
                    ));
                }
                Ok(subquery
                    .rows
                    .first()
                    .and_then(|subquery_row| subquery_row.first())
                    .cloned()
                    .unwrap_or(Value::Null))
            }
            Expr::Exists(query) => Ok(Value::Bool(
                !self
                    .evaluate_query_with_outer(query, params, ctes, dataset, row)?
                    .rows
                    .is_empty(),
            )),
            Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
                ..
            } => {
                let left = self.eval_expr(expr, dataset, row, params, ctes, excluded)?;
                let right = self.eval_expr(pattern, dataset, row, params, ctes, excluded)?;
                let escape = escape
                    .as_ref()
                    .map(|expr| self.eval_expr(expr, dataset, row, params, ctes, excluded))
                    .transpose()?;
                eval_like(left, right, escape, *case_insensitive, *negated)
            }
            Expr::IsNull { expr, negated } => {
                let is_null = matches!(
                    self.eval_expr(expr, dataset, row, params, ctes, excluded)?,
                    Value::Null
                );
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::Function { name, args } => {
                eval_function(self, name, args, dataset, row, params, ctes, excluded)
            }
            Expr::Aggregate { .. } => Err(DbError::sql(
                "aggregate expressions require grouped evaluation",
            )),
            Expr::RowNumber { .. } | Expr::WindowFunction { .. } => Err(DbError::sql(
                "window-function execution is not yet implemented",
            )),
            Expr::Case {
                operand,
                branches,
                else_expr,
            } => {
                let operand_value = operand
                    .as_deref()
                    .map(|expr| self.eval_expr(expr, dataset, row, params, ctes, excluded))
                    .transpose()?;
                for (condition, result) in branches {
                    let matches = if let Some(operand_value) = &operand_value {
                        compare_values(
                            operand_value,
                            &self.eval_expr(condition, dataset, row, params, ctes, excluded)?,
                        )? == std::cmp::Ordering::Equal
                    } else {
                        matches!(
                            self.eval_expr(condition, dataset, row, params, ctes, excluded)?,
                            Value::Bool(true)
                        )
                    };
                    if matches {
                        return self.eval_expr(result, dataset, row, params, ctes, excluded);
                    }
                }
                else_expr
                    .as_deref()
                    .map(|expr| self.eval_expr(expr, dataset, row, params, ctes, excluded))
                    .transpose()?
                    .map_or(Ok(Value::Null), Ok)
            }
            Expr::Cast { expr, target_type } => cast_value(
                self.eval_expr(expr, dataset, row, params, ctes, excluded)?,
                *target_type,
            ),
            Expr::Collate { expr, .. } => {
                self.eval_expr(expr, dataset, row, params, ctes, excluded)
            }
            Expr::Row(_) => Err(DbError::sql(
                "row values are only supported in IN comparisons",
            )),
        }
    }
    fn resolve_column(
        &self,
        dataset: &Dataset,
        row: &[Value],
        table: Option<&str>,
        column: &str,
        excluded: Option<&Dataset>,
    ) -> Result<Value> {
        if let Some(table_name) = table {
            if identifiers_equal(table_name, "excluded") {
                let excluded = excluded.ok_or_else(|| {
                    DbError::sql("EXCLUDED is only valid in ON CONFLICT DO UPDATE")
                })?;
                return self.resolve_column(
                    excluded,
                    excluded.rows.first().map(Vec::as_slice).unwrap_or(&[]),
                    None,
                    column,
                    None,
                );
            }
        }
        let mut matched_index = None;
        for (index, binding) in dataset.columns.iter().enumerate() {
            let visible_match = table.is_some() || !binding.hidden;
            if !visible_match || !identifiers_equal(&binding.name, column) {
                continue;
            }
            if table.is_some_and(|table| {
                !binding
                    .table
                    .as_deref()
                    .is_some_and(|binding_table| identifiers_equal(binding_table, table))
            }) {
                continue;
            }
            if matched_index.replace(index).is_some() {
                return Err(DbError::sql(format!("ambiguous column reference {column}")));
            }
        }
        if let Some(index) = matched_index {
            row.get(index)
                .cloned()
                .ok_or_else(|| DbError::internal("row is shorter than its bindings"))
        } else {
            Err(DbError::sql(format!("unknown column {column}")))
        }
    }
    pub(super) fn apply_virtual_generated_columns(
        &self,
        table: &TableSchema,
        row: &mut [Value],
    ) -> Result<()> {
        if generated_columns_are_stored(table) {
            return Ok(());
        }
        let mut base_values = row.to_vec();
        for (index, column) in table.columns.iter().enumerate() {
            let Some(generated_sql) = &column.generated_sql else {
                continue;
            };
            if column.generated_stored {
                base_values[index] = row
                    .get(index)
                    .cloned()
                    .ok_or_else(|| DbError::internal("row is shorter than table schema"))?;
                continue;
            }
            let expr = crate::sql::parser::parse_expression_sql(generated_sql)?;
            let dataset = table_row_dataset(table, &base_values, &table.name);
            let eval_row = dataset.rows.first().map(Vec::as_slice).unwrap_or(&[]);
            let value = self.eval_expr(&expr, &dataset, eval_row, &[], &BTreeMap::new(), None)?;
            let cast_value = self::constraints::coerce_column_value(column, value)?;
            if let Some(slot) = row.get_mut(index) {
                *slot = cast_value.clone();
            } else {
                return Err(DbError::internal("row is shorter than table schema"));
            }
            base_values[index] = cast_value;
        }
        Ok(())
    }
}
