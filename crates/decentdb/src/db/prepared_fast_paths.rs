//! Thematic extraction (mechanical split; no behavior change).

use super::*;

impl Db {
    pub(crate) fn prepared_plan_cache_entries(&self) -> Result<Vec<PreparedPlanCacheEntry>> {
        self.inner
            .prepared_plan_cache
            .lock()
            .map(|cache| cache.snapshot_entries())
            .map_err(|_| DbError::internal("prepared plan cache lock poisoned"))
    }
    pub(crate) fn prepared_insert_uses_direct_positional_params(
        prepared_insert: &PreparedSimpleInsert,
        param_count: usize,
    ) -> bool {
        prepared_insert.direct_positional_param_count == Some(param_count)
    }
    pub(crate) fn prepared_statement_cache_key(prepared: &PreparedStatement) -> usize {
        if let Some(insert) = prepared.prepared_insert.as_ref() {
            return Arc::as_ptr(insert) as usize;
        }
        Arc::as_ptr(&prepared.statement) as usize
    }
    pub(crate) fn prepared_insert_plan_for_runtime_state(
        &self,
        prepared: &PreparedStatement,
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
        indexes_maybe_stale: &mut bool,
        prepared_insert_runtime_cache: &mut HashMap<usize, Arc<PreparedSimpleInsert>>,
    ) -> Result<Option<Arc<PreparedSimpleInsert>>> {
        let Some(prepared_insert) = prepared.prepared_insert.as_ref() else {
            return Ok(None);
        };

        if *indexes_maybe_stale {
            runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
            *indexes_maybe_stale = false;
        }

        let cache_key = Self::prepared_statement_cache_key(prepared);
        if let Some(plan) = prepared_insert_runtime_cache.get(&cache_key) {
            if Self::prepared_insert_target_loaded(runtime, plan) {
                return Ok(Some(Arc::clone(plan)));
            }
            prepared_insert_runtime_cache.remove(&cache_key);
        }

        let needs_refresh =
            prepared_insert.use_generic_validation || prepared_insert.use_generic_index_updates;
        if !needs_refresh
            && runtime.can_reuse_prepared_simple_insert(prepared_insert)
            && Self::prepared_insert_target_loaded(runtime, prepared_insert)
        {
            prepared_insert_runtime_cache.insert(cache_key, Arc::clone(prepared_insert));
            return Ok(Some(Arc::clone(prepared_insert)));
        }

        let table_names =
            self.insert_dependency_table_names(runtime, &prepared_insert.table_name)?;
        let table_refs = table_names.iter().map(String::as_str).collect::<Vec<_>>();
        self.load_runtime_table_row_sources_at_snapshot(runtime, &table_refs, snapshot_lsn)?;

        if !needs_refresh && runtime.can_reuse_prepared_simple_insert(prepared_insert) {
            prepared_insert_runtime_cache.insert(cache_key, Arc::clone(prepared_insert));
            return Ok(Some(Arc::clone(prepared_insert)));
        }

        let SqlStatement::Insert(insert) = prepared.statement.as_ref() else {
            return Ok(None);
        };
        let Some(refreshed) = runtime.prepare_simple_insert(insert)? else {
            return Ok(None);
        };
        let refreshed = Arc::new(refreshed);
        if runtime.can_reuse_prepared_simple_insert(refreshed.as_ref()) {
            prepared_insert_runtime_cache.insert(cache_key, Arc::clone(&refreshed));
        }
        Ok(Some(refreshed))
    }
    pub(crate) fn prepared_insert_changes_persistent_table(
        _runtime: &EngineRuntime,
        prepared_insert: &PreparedSimpleInsert,
    ) -> bool {
        prepared_insert.catalog_table_name.is_some()
    }
    fn prepared_insert_target_loaded(
        runtime: &EngineRuntime,
        prepared_insert: &PreparedSimpleInsert,
    ) -> bool {
        if let Some(table_name) = prepared_insert.catalog_table_name.as_deref() {
            runtime.tables.contains_key(table_name)
        } else {
            runtime.prepared_insert_target_loaded(&prepared_insert.table_name)
        }
    }
    pub(crate) fn try_execute_prepared_simple_ordered_row_id_projection(
        &self,
        prepared: &PreparedStatement,
    ) -> Result<Option<QueryResult>> {
        if self.inner.sql_txn_active.load(Ordering::Acquire)
            || self.inner.config.extension_unsigned_development_mode
            || !self.inner.config.extension_trust_anchors.is_empty()
        {
            return Ok(None);
        }
        let Some(plan) = prepared.simple_ordered_row_id_projection.as_ref() else {
            return Ok(None);
        };
        let Some(runtime) = self.try_resident_read_for_single_process_statement(
            prepared.statement.as_ref(),
            Some(prepared),
        )?
        else {
            return Ok(None);
        };
        let result = runtime.execute_resolved_simple_ordered_row_id_projection(
            ResolvedSimpleOrderedRowIdProjectionRequest {
                table_name: plan.table_name.as_str(),
                order_column: plan.order_column.as_str(),
                projection_indexes: &plan.projection_indexes,
                column_names: Arc::clone(&plan.column_names),
                limit: plan.limit,
                offset: plan.offset,
                descending: plan.descending,
            },
        )?;
        drop(runtime);
        if let Some(result) = result {
            return self
                .finalize_row_source_autocommit_statement(prepared.statement.as_ref(), Ok(result))
                .map(Some);
        }
        Ok(None)
    }
    pub(crate) fn try_execute_prepared_simple_row_id_projection(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(plan) = prepared.simple_row_id_projection.as_ref() else {
            return Ok(None);
        };
        let Some(Value::Int64(lookup_row_id)) = params.get(plan.param_index) else {
            return Ok(None);
        };

        if !self.inner.config.extension_unsigned_development_mode
            && self.inner.config.extension_trust_anchors.is_empty()
        {
            if let Some(runtime) = self.try_resident_read_for_prepared_table_statement(
                prepared,
                plan.table_name.as_str(),
            )? {
                let result = runtime.execute_resolved_simple_row_id_projection_at_snapshot(
                    ResolvedSimpleRowIdProjectionRequest {
                        table_name: plan.table_name.as_str(),
                        projection_indexes: &plan.projection_indexes,
                        column_names: Arc::clone(&plan.column_names),
                        lookup_row_id: *lookup_row_id,
                        pager: &self.inner.pager,
                        wal: &self.inner.wal,
                        snapshot_lsn: 0,
                        use_persistent_pk_index: self.inner.config.persistent_pk_index,
                    },
                )?;
                drop(runtime);
                if let Some(result) = result {
                    return Ok(Some(result));
                }
            }
        }

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        if let Some(runtime) = self.runtime_read_for_prepared_row_sources_at_snapshot(
            &[plan.table_name.as_str()],
            snapshot_lsn,
        )? {
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            let result = runtime.execute_resolved_simple_row_id_projection_at_snapshot(
                ResolvedSimpleRowIdProjectionRequest {
                    table_name: plan.table_name.as_str(),
                    projection_indexes: &plan.projection_indexes,
                    column_names: Arc::clone(&plan.column_names),
                    lookup_row_id: *lookup_row_id,
                    pager: &self.inner.pager,
                    wal: &self.inner.wal,
                    snapshot_lsn,
                    use_persistent_pk_index: self.inner.config.persistent_pk_index,
                },
            )?;
            if result.is_some() {
                drop(runtime);
                drop(reader);
                return Ok(result);
            }
            drop(runtime);
        }
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        self.try_load_prepared_read_row_sources_at_snapshot(
            &[plan.table_name.as_str()],
            snapshot_lsn,
        )?;
        let Some(runtime) = self.runtime_read_for_fast_read_at_snapshot(snapshot_lsn)? else {
            drop(reader);
            return Ok(None);
        };
        self.validate_prepared_schema_cookie(
            prepared,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        let result = runtime.execute_resolved_simple_row_id_projection_at_snapshot(
            ResolvedSimpleRowIdProjectionRequest {
                table_name: plan.table_name.as_str(),
                projection_indexes: &plan.projection_indexes,
                column_names: Arc::clone(&plan.column_names),
                lookup_row_id: *lookup_row_id,
                pager: &self.inner.pager,
                wal: &self.inner.wal,
                snapshot_lsn,
                use_persistent_pk_index: self.inner.config.persistent_pk_index,
            },
        )?;
        drop(runtime);
        drop(reader);
        Ok(result)
    }
    pub(crate) fn try_execute_prepared_simple_indexed_projection(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(plan) = prepared.simple_indexed_projection.as_ref() else {
            return Ok(None);
        };

        if !self.inner.config.extension_unsigned_development_mode
            && self.inner.config.extension_trust_anchors.is_empty()
        {
            if let Some(runtime) = self.try_resident_read_for_single_process_statement(
                prepared.statement.as_ref(),
                Some(prepared),
            )? {
                let result = self.execute_prepared_simple_indexed_projection_in_runtime(
                    &runtime, plan, params,
                )?;
                drop(runtime);
                if let Some(result) = result {
                    return self
                        .finalize_row_source_autocommit_statement(
                            prepared.statement.as_ref(),
                            Ok(result),
                        )
                        .map(Some);
                }
            }
        }

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        if let Some(runtime) = self.runtime_read_for_prepared_row_sources_at_snapshot(
            &[plan.table_name.as_str()],
            snapshot_lsn,
        )? {
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            let result =
                self.execute_prepared_simple_indexed_projection_in_runtime(&runtime, plan, params)?;
            if result.is_some() {
                drop(runtime);
                drop(reader);
                return Ok(result);
            }
            drop(runtime);
        }

        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        self.try_load_prepared_read_row_sources_at_snapshot(
            &[plan.table_name.as_str()],
            snapshot_lsn,
        )?;
        let Some(runtime) = self.runtime_read_for_fast_read_at_snapshot(snapshot_lsn)? else {
            drop(reader);
            return Ok(None);
        };
        self.validate_prepared_schema_cookie(
            prepared,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        let result =
            self.execute_prepared_simple_indexed_projection_in_runtime(&runtime, plan, params)?;
        drop(runtime);
        drop(reader);
        Ok(result)
    }
    pub(crate) fn try_execute_prepared_simple_row_id_range_projection(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(plan) = prepared.simple_row_id_range_projection.as_ref() else {
            return Ok(None);
        };
        let lower_bound = if let Some(bound) = plan.lower_bound {
            let Some(Value::Int64(value)) = params.get(bound.param_index) else {
                return Ok(None);
            };
            Some(SimpleRangeBoundValue {
                inclusive: bound.inclusive,
                value: Value::Int64(*value),
            })
        } else {
            None
        };
        let upper_bound = if let Some(bound) = plan.upper_bound {
            let Some(Value::Int64(value)) = params.get(bound.param_index) else {
                return Ok(None);
            };
            Some(SimpleRangeBoundValue {
                inclusive: bound.inclusive,
                value: Value::Int64(*value),
            })
        } else {
            None
        };
        let Some(Value::Int64(limit_value)) = params.get(plan.limit_param_index) else {
            return Ok(None);
        };
        let limit = Some(usize::try_from((*limit_value).max(0)).unwrap_or(usize::MAX));

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        if let Some(runtime) = self.runtime_read_for_prepared_row_sources_at_snapshot(
            &[plan.table_name.as_str()],
            snapshot_lsn,
        )? {
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            let result = runtime.execute_resolved_simple_row_id_range_projection_at_snapshot(
                ResolvedSimpleRowIdRangeProjectionRequest {
                    table_name: plan.table_name.as_str(),
                    projection_indexes: &plan.projection_indexes,
                    column_names: Arc::clone(&plan.column_names),
                    filter_column: plan.filter_column.as_str(),
                    lower_bound: lower_bound.clone(),
                    upper_bound: upper_bound.clone(),
                    limit,
                    pager: &self.inner.pager,
                    wal: &self.inner.wal,
                    snapshot_lsn,
                    use_persistent_pk_index: self.inner.config.persistent_pk_index,
                },
            )?;
            if result.is_some() {
                drop(runtime);
                drop(reader);
                return Ok(result);
            }
            drop(runtime);
        }
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        self.try_load_prepared_read_row_sources_at_snapshot(
            &[plan.table_name.as_str()],
            snapshot_lsn,
        )?;
        let Some(runtime) = self.runtime_read_for_fast_read_at_snapshot(snapshot_lsn)? else {
            drop(reader);
            return Ok(None);
        };
        self.validate_prepared_schema_cookie(
            prepared,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        let result = runtime.execute_resolved_simple_row_id_range_projection_at_snapshot(
            ResolvedSimpleRowIdRangeProjectionRequest {
                table_name: plan.table_name.as_str(),
                projection_indexes: &plan.projection_indexes,
                column_names: Arc::clone(&plan.column_names),
                filter_column: plan.filter_column.as_str(),
                lower_bound,
                upper_bound,
                limit,
                pager: &self.inner.pager,
                wal: &self.inner.wal,
                snapshot_lsn,
                use_persistent_pk_index: self.inner.config.persistent_pk_index,
            },
        )?;
        drop(runtime);
        drop(reader);
        Ok(result)
    }
    pub(crate) fn try_execute_prepared_simple_row_id_join_projection(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(plan) = prepared.simple_row_id_join_projection.as_ref() else {
            return Ok(None);
        };
        let Some(Value::Int64(lookup_row_id)) = params.get(plan.param_index) else {
            return Ok(None);
        };

        let join_tables = [
            plan.left_table_name.as_str(),
            plan.right_table_name.as_str(),
        ];
        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        if let Some(runtime) =
            self.runtime_read_for_prepared_row_sources_at_snapshot(&join_tables, snapshot_lsn)?
        {
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            let result = runtime.execute_resolved_simple_row_id_join_projection_at_snapshot(
                ResolvedSimpleRowIdJoinProjectionRequest {
                    left_table_name: plan.left_table_name.as_str(),
                    right_table_name: plan.right_table_name.as_str(),
                    left_projection_indexes: &plan.left_projection_indexes,
                    right_projection_indexes: &plan.right_projection_indexes,
                    projections: &plan.projections,
                    column_names: Arc::clone(&plan.column_names),
                    lookup_row_id: *lookup_row_id,
                    pager: &self.inner.pager,
                    wal: &self.inner.wal,
                    snapshot_lsn,
                    use_persistent_pk_index: self.inner.config.persistent_pk_index,
                },
            )?;
            if result.is_some() {
                drop(runtime);
                drop(reader);
                return Ok(result);
            }
            drop(runtime);
        }
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        self.try_load_prepared_read_row_sources_at_snapshot(&join_tables, snapshot_lsn)?;
        let Some(runtime) = self.runtime_read_for_fast_read_at_snapshot(snapshot_lsn)? else {
            drop(reader);
            return Ok(None);
        };
        self.validate_prepared_schema_cookie(
            prepared,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        let result = runtime.execute_resolved_simple_row_id_join_projection_at_snapshot(
            ResolvedSimpleRowIdJoinProjectionRequest {
                left_table_name: plan.left_table_name.as_str(),
                right_table_name: plan.right_table_name.as_str(),
                left_projection_indexes: &plan.left_projection_indexes,
                right_projection_indexes: &plan.right_projection_indexes,
                projections: &plan.projections,
                column_names: Arc::clone(&plan.column_names),
                lookup_row_id: *lookup_row_id,
                pager: &self.inner.pager,
                wal: &self.inner.wal,
                snapshot_lsn,
                use_persistent_pk_index: self.inner.config.persistent_pk_index,
            },
        )?;
        drop(runtime);
        drop(reader);
        Ok(result)
    }
    pub(crate) fn try_execute_prepared_simple_scalar_filtered_aggregate(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(plan) = prepared.simple_scalar_filtered_aggregate.as_ref() else {
            return Ok(None);
        };
        let Some(Value::Int64(param_value)) = params.get(plan.param_index) else {
            return Ok(None);
        };
        let SqlStatement::Query(query) = prepared.statement.as_ref() else {
            return Ok(None);
        };

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        let Some(runtime) = self.runtime_read_for_fast_read_at_snapshot(snapshot_lsn)? else {
            return Ok(None);
        };
        self.validate_prepared_schema_cookie(
            prepared,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        let has_resident_source = runtime.table_row_source(plan.table_name.as_str()).is_some();
        if !has_resident_source && !runtime.has_deferred_tables() {
            return Ok(None);
        }
        let state = runtime.persisted_table_state(plan.table_name.as_str());
        if !has_resident_source && state.is_none() {
            return Ok(None);
        };
        let state = state.unwrap_or_default();
        let key = PreparedScalarAggregateCacheKey {
            snapshot_lsn,
            pointer_head_page_id: state.pointer.head_page_id,
            pointer_logical_len: state.pointer.logical_len,
            pointer_flags: state.pointer.flags,
            checksum: state.checksum,
            row_count: state.row_count,
            param_value: *param_value,
        };
        if let Some(result) = plan
            .cache
            .lock()
            .map_err(|_| DbError::internal("prepared aggregate cache lock poisoned"))?
            .get(&key)
        {
            drop(runtime);
            drop(reader);
            return Ok(Some(result));
        }

        let result = if has_resident_source {
            runtime.try_execute_simple_grouped_numeric_aggregate_query(query, params)?
        } else {
            runtime.try_execute_simple_deferred_paged_grouped_numeric_aggregate_query(
                query,
                params,
                &self.inner.pager,
                &self.inner.wal,
                snapshot_lsn,
            )?
        };
        if let Some(result) = result.as_ref() {
            plan.cache
                .lock()
                .map_err(|_| DbError::internal("prepared aggregate cache lock poisoned"))?
                .insert(key, result.clone());
        }
        drop(runtime);
        drop(reader);
        Ok(result)
    }
    pub(crate) fn execute_autocommit_temp_only_statement(
        &self,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let mut working = self.engine_snapshot()?;
        let result = working.execute_statement(statement, params, self.inner.config.page_size)?;
        self.install_temp_runtime(working)?;
        Ok(result)
    }
    pub(crate) fn execute_autocommit_insert_in_place(
        &self,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let insert_table_names = if let crate::sql::ast::Statement::Insert(insert) = statement {
            let table_names = {
                self.refresh_engine_from_storage()?;
                let runtime = self
                    .inner
                    .engine
                    .read()
                    .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
                self.insert_dependency_table_names(&runtime, &insert.table_name)?
            };
            let table_refs = table_names.iter().map(String::as_str).collect::<Vec<_>>();
            self.load_simple_write_row_sources_at_latest_snapshot(&table_refs)?;
            Some(table_names)
        } else {
            None
        };
        let result = self.execute_autocommit_in_place(|runtime| {
            runtime.execute_statement(statement, params, self.inner.config.page_size)
        })?;
        if let Some(table_names) = &insert_table_names {
            let table_refs = table_names.iter().map(String::as_str).collect::<Vec<_>>();
            self.redefer_persisted_tables_after_write(&table_refs)?;
        }
        Ok(result)
    }
    pub(crate) fn execute_autocommit_prepared_insert_in_place(
        &self,
        prepared: &PreparedSimpleInsert,
        params: &[Value],
    ) -> Result<QueryResult> {
        let table_names = {
            self.refresh_engine_from_storage()?;
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.insert_dependency_table_names(&runtime, &prepared.table_name)?
        };
        let table_refs = table_names.iter().map(String::as_str).collect::<Vec<_>>();
        self.load_simple_write_row_sources_at_latest_snapshot(&table_refs)?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        if !runtime.can_reuse_prepared_simple_insert(prepared) {
            drop(runtime);
            let result = self.execute_autocommit_in_place(|runtime| {
                runtime.execute_prepared_simple_insert(
                    prepared,
                    params,
                    self.inner.config.page_size,
                )
            })?;
            self.redefer_persisted_tables_after_write(&table_refs)?;
            return Ok(result);
        }
        let result = match runtime.execute_prepared_simple_insert(
            prepared,
            params,
            self.inner.config.page_size,
        ) {
            Ok(result) => result,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        let reactive_pending = self.take_reactive_pending_commit(&mut runtime);
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !runtime.sync_mutations.is_empty() {
            self.sync_post_commit(&mut runtime, committed_lsn)?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.publish_reactive_commit(reactive_pending, committed_lsn);
        self.redefer_persisted_tables_after_write(&table_refs)?;
        Ok(result)
    }
    pub(crate) fn execute_autocommit_simple_update_in_place(
        &self,
        prepared_update: &PreparedSimpleUpdate,
        params: &[Value],
    ) -> Result<QueryResult> {
        self.load_simple_write_row_sources_at_latest_snapshot(&[prepared_update
            .table_name
            .as_str()])?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        if !runtime.can_reuse_prepared_simple_update(prepared_update) {
            drop(runtime);
            let result = self.execute_autocommit_in_place(|runtime| {
                runtime.execute_prepared_simple_update(
                    prepared_update,
                    params,
                    self.inner.config.page_size,
                )
            })?;
            self.redefer_persisted_tables_after_write(&[prepared_update.table_name.as_str()])?;
            return Ok(result);
        }
        let result = match runtime.execute_prepared_simple_update(
            prepared_update,
            params,
            self.inner.config.page_size,
        ) {
            Ok(result) => result,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if result.affected_rows() == 0 && !self.runtime_has_persistent_commit_work(&runtime)? {
            self.sync_temp_state_from_runtime(&runtime)?;
            drop(runtime);
            self.redefer_persisted_tables_after_write(&[prepared_update.table_name.as_str()])?;
            return Ok(result);
        }
        if !prepared_update
            .indexes
            .iter()
            .all(|index| runtime.prepared_btree_index_is_fresh(index))
        {
            runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        }
        let reactive_pending = self.take_reactive_pending_commit(&mut runtime);
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !runtime.sync_mutations.is_empty() {
            self.sync_post_commit(&mut runtime, committed_lsn)?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.publish_reactive_commit(reactive_pending, committed_lsn);
        self.redefer_persisted_tables_after_write(&[prepared_update.table_name.as_str()])?;
        Ok(result)
    }
    pub(crate) fn execute_autocommit_simple_delete_in_place(
        &self,
        prepared_delete: &PreparedSimpleDelete,
        params: &[Value],
    ) -> Result<QueryResult> {
        let table_names = prepared_delete.affected_table_names();
        let row_source_table_names = prepared_delete.required_row_source_table_names();
        let child_index_targets = prepared_delete.child_index_hydration_targets();
        self.load_simple_write_row_sources_and_child_indexes_at_latest_snapshot(
            &row_source_table_names,
            &child_index_targets,
        )?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        if !runtime.can_reuse_prepared_simple_delete(prepared_delete) {
            drop(runtime);
            let result = self.execute_autocommit_in_place(|runtime| {
                runtime.execute_prepared_simple_delete(
                    prepared_delete,
                    params,
                    self.inner.config.page_size,
                )
            })?;
            self.redefer_persisted_tables_after_write(&table_names)?;
            return Ok(result);
        }
        let result = match runtime.execute_prepared_simple_delete(
            prepared_delete,
            params,
            self.inner.config.page_size,
        ) {
            Ok(result) => result,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if result.affected_rows() == 0 && !self.runtime_has_persistent_commit_work(&runtime)? {
            self.sync_temp_state_from_runtime(&runtime)?;
            drop(runtime);
            self.redefer_persisted_tables_after_write(&table_names)?;
            return Ok(result);
        }
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        let reactive_pending = self.take_reactive_pending_commit(&mut runtime);
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !runtime.sync_mutations.is_empty() {
            self.sync_post_commit(&mut runtime, committed_lsn)?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.publish_reactive_commit(reactive_pending, committed_lsn);
        self.redefer_persisted_tables_after_write(&table_names)?;
        Ok(result)
    }
    pub(crate) fn try_execute_autocommit_prepared_insert_in_place(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_insert: &PreparedSimpleInsert,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !self.can_use_autocommit_prepared_insert_fast_path(&prepared_insert.table_name)? {
            return Ok(None);
        }
        let single_table = [prepared_insert.table_name.as_str()];
        let mut dependency_tables = Vec::new();
        let table_refs: &[&str] = if prepared_insert.row_source_dependency_tables.is_empty() {
            &single_table
        } else {
            dependency_tables.reserve(prepared_insert.row_source_dependency_tables.len() + 1);
            dependency_tables.push(prepared_insert.table_name.as_str());
            for parent_table_name in &prepared_insert.row_source_dependency_tables {
                if !dependency_tables
                    .iter()
                    .any(|name| identifiers_equal(name, parent_table_name))
                {
                    dependency_tables.push(parent_table_name.as_str());
                }
            }
            &dependency_tables
        };
        self.load_simple_write_row_sources_at_latest_snapshot(table_refs)?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_insert(prepared_insert) {
            return Ok(None);
        }
        let result = match runtime.execute_prepared_simple_insert(
            prepared_insert,
            params,
            self.inner.config.page_size,
        ) {
            Ok(result) => result,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        let reactive_pending = self.take_reactive_pending_commit(&mut runtime);
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !runtime.sync_mutations.is_empty() {
            self.sync_post_commit(&mut runtime, committed_lsn)?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.publish_reactive_commit(reactive_pending, committed_lsn);
        self.redefer_persisted_tables_after_write(table_refs)?;
        Ok(Some(result))
    }
    pub(crate) fn try_execute_autocommit_prepared_insert_in_place_mut(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_insert: &PreparedSimpleInsert,
        params: &mut [Value],
    ) -> Result<Option<QueryResult>> {
        if !self.can_use_autocommit_prepared_insert_fast_path(&prepared_insert.table_name)?
            || !Self::prepared_insert_uses_direct_positional_params(prepared_insert, params.len())
        {
            return Ok(None);
        }
        let single_table = [prepared_insert.table_name.as_str()];
        let mut dependency_tables = Vec::new();
        let table_refs: &[&str] = if prepared_insert.row_source_dependency_tables.is_empty() {
            &single_table
        } else {
            dependency_tables.reserve(prepared_insert.row_source_dependency_tables.len() + 1);
            dependency_tables.push(prepared_insert.table_name.as_str());
            for parent_table_name in &prepared_insert.row_source_dependency_tables {
                if !dependency_tables
                    .iter()
                    .any(|name| identifiers_equal(name, parent_table_name))
                {
                    dependency_tables.push(parent_table_name.as_str());
                }
            }
            &dependency_tables
        };
        self.load_simple_write_row_sources_at_latest_snapshot(table_refs)?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_insert(prepared_insert) {
            return Ok(None);
        }
        let affected = match runtime.execute_prepared_simple_insert_positional_params_in_place(
            prepared_insert,
            params,
            self.inner.config.page_size,
        ) {
            Ok(affected) => affected,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        let reactive_pending = self.take_reactive_pending_commit(&mut runtime);
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !runtime.sync_mutations.is_empty() {
            self.sync_post_commit(&mut runtime, committed_lsn)?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        let redefer_after_write =
            self.runtime_should_redefer_persisted_tables_after_write(&runtime, table_refs);
        drop(runtime);
        self.publish_reactive_commit(reactive_pending, committed_lsn);
        if redefer_after_write {
            self.redefer_persisted_tables_after_write(table_refs)?;
        }
        Ok(Some(QueryResult::with_affected_rows(affected)))
    }
    pub(crate) fn try_execute_autocommit_prepared_update_in_place(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_update: &PreparedSimpleUpdate,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        self.load_simple_write_row_sources_at_latest_snapshot(&[prepared_update
            .table_name
            .as_str()])?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_update(prepared_update) {
            return Ok(None);
        }
        let result = match runtime.execute_prepared_simple_update(
            prepared_update,
            params,
            self.inner.config.page_size,
        ) {
            Ok(result) => result,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if result.affected_rows() == 0 && !self.runtime_has_persistent_commit_work(&runtime)? {
            self.sync_temp_state_from_runtime(&runtime)?;
            drop(runtime);
            self.redefer_persisted_tables_after_write(&[prepared_update.table_name.as_str()])?;
            return Ok(Some(result));
        }
        if !prepared_update
            .indexes
            .iter()
            .all(|index| runtime.prepared_btree_index_is_fresh(index))
        {
            runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        }
        let reactive_pending = self.take_reactive_pending_commit(&mut runtime);
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !runtime.sync_mutations.is_empty() {
            self.sync_post_commit(&mut runtime, committed_lsn)?;
        }
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.publish_reactive_commit(reactive_pending, committed_lsn);
        self.redefer_persisted_tables_after_write(&[prepared_update.table_name.as_str()])?;
        Ok(Some(result))
    }
    pub(crate) fn try_execute_autocommit_prepared_delete_in_place(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_delete: &PreparedSimpleDelete,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if let Some(result) = self.try_execute_zero_row_index_delete_against_current_runtime(
            prepared_statement,
            prepared_delete,
            params,
        )? {
            return Ok(Some(result));
        }
        let table_names = prepared_delete.affected_table_names();
        let row_source_table_names = prepared_delete.required_row_source_table_names();
        let child_index_targets = prepared_delete.child_index_hydration_targets();
        self.load_simple_write_row_sources_and_child_indexes_at_latest_snapshot(
            &row_source_table_names,
            &child_index_targets,
        )?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_delete(prepared_delete) {
            return Ok(None);
        }
        let result = match runtime.execute_prepared_simple_delete(
            prepared_delete,
            params,
            self.inner.config.page_size,
        ) {
            Ok(result) => result,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if result.affected_rows() == 0 && !self.runtime_has_persistent_commit_work(&runtime)? {
            self.sync_temp_state_from_runtime(&runtime)?;
            drop(runtime);
            self.redefer_persisted_tables_after_write(&table_names)?;
            return Ok(Some(result));
        }
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        let reactive_pending = self.take_reactive_pending_commit(&mut runtime);
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !runtime.sync_mutations.is_empty() {
            self.sync_post_commit(&mut runtime, committed_lsn)?;
        }
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.publish_reactive_commit(reactive_pending, committed_lsn);
        self.redefer_persisted_tables_after_write(&table_names)?;
        Ok(Some(result))
    }
    pub(crate) fn execute_autocommit_in_place<F>(&self, apply: F) -> Result<QueryResult>
    where
        F: FnOnce(&mut EngineRuntime) -> Result<QueryResult>,
    {
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        let result = match apply(&mut runtime) {
            Ok(result) => result,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !self.runtime_has_persistent_commit_work(&runtime)?
            && runtime.sync_mutations.is_empty()
            && !Self::runtime_has_stale_indexes(&runtime)
        {
            self.sync_temp_state_from_runtime(&runtime)?;
            return Ok(result);
        }
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        let reactive_pending = self.take_reactive_pending_commit(&mut runtime);
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        self.sync_post_commit(&mut runtime, committed_lsn)?;
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.publish_reactive_commit(reactive_pending, committed_lsn);
        Ok(result)
    }
    pub(crate) fn try_prepare_from_plan_cache(
        &self,
        prepared_sql: &str,
    ) -> Result<Option<PreparedStatement>> {
        let persistent_cookie = self.inner.catalog.schema_cookie()?;
        let temp_cookie = self
            .inner
            .temp_state
            .lock()
            .map_err(|_| DbError::internal("temp schema lock poisoned"))?
            .schema_cookie;
        let policy_gen = self.inner.policy_mask_generation.current();
        let key = crate::plan_cache::PlanCacheKey::new(
            prepared_sql.to_string(),
            parameter_shape_for_prepared_sql(prepared_sql),
            persistent_cookie,
            temp_cookie,
            policy_gen,
        );
        let Some(bundle) = self
            .inner
            .prepared_plan_cache
            .lock()
            .map_err(|_| DbError::internal("prepared plan cache lock poisoned"))?
            .get(&key, persistent_cookie, temp_cookie, policy_gen)
        else {
            return Ok(None);
        };
        Ok(Some(PreparedStatement {
            db: self.clone(),
            schema_cookie: persistent_cookie,
            temp_schema_cookie: temp_cookie,
            statement: Arc::clone(&bundle.statement),
            prepared_sql: prepared_sql.to_string(),
            simple_row_id_projection: bundle.simple_row_id_projection,
            simple_indexed_projection: bundle.simple_indexed_projection,
            simple_row_id_range_projection: bundle.simple_row_id_range_projection,
            simple_ordered_row_id_projection: bundle.simple_ordered_row_id_projection,
            simple_row_id_join_projection: bundle.simple_row_id_join_projection,
            simple_scalar_filtered_aggregate: bundle.simple_scalar_filtered_aggregate,
            prepared_insert: bundle.prepared_insert,
            prepared_update: bundle.prepared_update,
            prepared_delete: bundle.prepared_delete,
            read_only: bundle.read_only,
        }))
    }
    pub(crate) fn prepared_simple_row_id_projection(
        sql: &str,
        runtime: &EngineRuntime,
    ) -> Option<PreparedSimpleRowIdProjection> {
        let plan = parse_simple_row_id_projection_sql(sql)?;
        if runtime.temp_table_schema(plan.table_name).is_some()
            || runtime
                .catalog
                .views
                .keys()
                .any(|view_name| identifiers_equal(view_name, plan.table_name))
        {
            return None;
        }
        let table = runtime.catalog.table(plan.table_name)?;
        if !row_id_alias_column_name(table)
            .is_some_and(|column_name| identifiers_equal(column_name, plan.filter_column))
        {
            return None;
        }

        let mut projection_indexes = Vec::with_capacity(plan.projection_columns.len());
        let mut column_names = Vec::with_capacity(plan.projection_columns.len());
        for projection_column in plan.projection_columns {
            let index = table
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, projection_column))?;
            projection_indexes.push(index);
            column_names.push(projection_column.to_string());
        }

        Some(PreparedSimpleRowIdProjection {
            table_name: table.name.clone(),
            projection_indexes,
            column_names: Arc::from(column_names),
            param_index: plan.param_index,
        })
    }
    pub(crate) fn prepared_simple_indexed_projection(
        statement: &SqlStatement,
        runtime: &EngineRuntime,
    ) -> Option<PreparedSimpleIndexedProjection> {
        let SqlStatement::Query(query) = statement else {
            return None;
        };
        if !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return None;
        }
        let QueryBody::Select(select) = &query.body else {
            return None;
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.from.len() != 1
        {
            return None;
        }
        let filter = select.filter.as_ref()?;
        let FromItem::Table { name, alias } = &select.from[0] else {
            return None;
        };
        if runtime.temp_table_schema(name).is_some()
            || runtime
                .catalog
                .views
                .keys()
                .any(|view_name| identifiers_equal(view_name, name))
        {
            return None;
        }
        let table = runtime.catalog.table(name)?;
        if !prepared_table_generated_columns_are_stored(table) {
            return None;
        }
        let binding_name = alias.as_deref().unwrap_or(name);

        let (filter_table, filter_column, value_expr) = match filter {
            Expr::Binary { left, op, right } if *op == BinaryOp::Eq => match (&**left, &**right) {
                (Expr::Column { table, column }, value_expr) => {
                    (table.as_deref(), column.as_str(), value_expr)
                }
                (value_expr, Expr::Column { table, column }) => {
                    (table.as_deref(), column.as_str(), value_expr)
                }
                _ => return None,
            },
            _ => return None,
        };
        if let Some(filter_table) = filter_table {
            if !identifiers_equal(filter_table, name)
                && !identifiers_equal(filter_table, binding_name)
            {
                return None;
            }
        }
        let value_source = crate::exec::dml::compile_prepared_lookup_value_source(
            table,
            filter_column,
            value_expr,
        )
        .ok()??;

        let mut projection_indexes = Vec::with_capacity(select.projection.len());
        let mut column_names = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            match item {
                SelectItem::Expr {
                    expr,
                    alias: select_alias,
                } => {
                    let Expr::Column {
                        table: projection_table,
                        column,
                    } = expr
                    else {
                        return None;
                    };
                    if let Some(projection_table) = projection_table.as_deref() {
                        if !identifiers_equal(projection_table, name)
                            && !identifiers_equal(projection_table, binding_name)
                        {
                            return None;
                        }
                    }
                    let index = table
                        .columns
                        .iter()
                        .position(|candidate| identifiers_equal(&candidate.name, column))?;
                    projection_indexes.push(index);
                    column_names.push(select_alias.clone().unwrap_or_else(|| column.clone()));
                }
                SelectItem::Wildcard => {
                    for (index, column) in table.columns.iter().enumerate() {
                        projection_indexes.push(index);
                        column_names.push(column.name.clone());
                    }
                }
                SelectItem::QualifiedWildcard(qualified_name) => {
                    if !identifiers_equal(qualified_name, name)
                        && !identifiers_equal(qualified_name, binding_name)
                    {
                        return None;
                    }
                    for (index, column) in table.columns.iter().enumerate() {
                        projection_indexes.push(index);
                        column_names.push(column.name.clone());
                    }
                }
            }
        }

        let lookup =
            if row_id_alias_column_name(table)
                .is_some_and(|column_name| identifiers_equal(column_name, filter_column))
            {
                PreparedSimpleIndexedProjectionLookup::RowId { value_source }
            } else {
                let index_name =
                    runtime
                        .catalog
                        .indexes
                        .values()
                        .find(|index| {
                            index.fresh
                                && index.kind == crate::catalog::IndexKind::Btree
                                && identifiers_equal(&index.table_name, &table.name)
                                && index.predicate_sql.is_none()
                                && index.columns.len() == 1
                                && index.columns[0].expression_sql.is_none()
                                && index.columns[0].column_name.as_ref().is_some_and(
                                    |column_name| identifiers_equal(column_name, filter_column),
                                )
                        })
                        .map(|index| index.name.clone())?;
                PreparedSimpleIndexedProjectionLookup::Index {
                    index_name,
                    value_source,
                }
            };

        Some(PreparedSimpleIndexedProjection {
            table_name: table.name.clone(),
            projection_indexes,
            column_names: Arc::from(column_names),
            lookup,
        })
    }
    pub(crate) fn prepared_simple_row_id_range_projection(
        sql: &str,
        runtime: &EngineRuntime,
    ) -> Option<PreparedSimpleRowIdRangeProjection> {
        let plan = parse_simple_row_id_range_projection_sql(sql)?;
        if runtime.temp_table_schema(plan.table_name).is_some()
            || runtime
                .catalog
                .views
                .keys()
                .any(|view_name| identifiers_equal(view_name, plan.table_name))
        {
            return None;
        }
        let table = runtime.catalog.table(plan.table_name)?;
        let filter_column_index = table
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, plan.filter_column))?;
        if !table
            .primary_key_columns
            .iter()
            .any(|column| identifiers_equal(column, plan.filter_column))
            || table.columns[filter_column_index].column_type != ColumnType::Int64
        {
            return None;
        }

        let mut projection_indexes = Vec::with_capacity(plan.projection_columns.len());
        let mut column_names = Vec::with_capacity(plan.projection_columns.len());
        for projection_column in plan.projection_columns {
            let index = table
                .columns
                .iter()
                .position(|column| identifiers_equal(&column.name, projection_column))?;
            projection_indexes.push(index);
            column_names.push(projection_column.to_string());
        }

        Some(PreparedSimpleRowIdRangeProjection {
            table_name: table.name.clone(),
            projection_indexes,
            column_names: Arc::from(column_names),
            filter_column: table.columns[filter_column_index].name.clone(),
            lower_bound: plan.lower_bound,
            upper_bound: plan.upper_bound,
            limit_param_index: plan.limit_param_index,
        })
    }
    pub(crate) fn prepared_simple_ordered_row_id_projection(
        statement: &SqlStatement,
        runtime: &EngineRuntime,
    ) -> Option<PreparedSimpleOrderedRowIdProjection> {
        let SqlStatement::Query(query) = statement else {
            return None;
        };
        if !query.ctes.is_empty() || query.order_by.len() != 1 {
            return None;
        }
        let crate::sql::ast::QueryBody::Select(select) = &query.body else {
            return None;
        };
        if select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return None;
        }
        let crate::sql::ast::FromItem::Table { name, alias } = &select.from[0] else {
            return None;
        };
        if runtime.temp_table_schema(name).is_some()
            || runtime
                .catalog
                .views
                .keys()
                .any(|view_name| identifiers_equal(view_name, name))
        {
            return None;
        }
        let table = runtime.catalog.table(name)?;
        if !prepared_table_generated_columns_are_stored(table) {
            return None;
        }
        let mut projection_indexes = Vec::with_capacity(select.projection.len());
        let mut column_names = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            let crate::sql::ast::SelectItem::Expr {
                expr,
                alias: select_alias,
            } = item
            else {
                return None;
            };
            let crate::sql::ast::Expr::Column {
                table: projection_table,
                column,
            } = expr
            else {
                return None;
            };
            if !prepared_scalar_column_matches_table(projection_table.as_deref(), name, alias) {
                return None;
            }
            let index = table
                .columns
                .iter()
                .position(|candidate| identifiers_equal(&candidate.name, column))?;
            projection_indexes.push(index);
            column_names.push(select_alias.clone().unwrap_or_else(|| column.clone()));
        }

        let order = &query.order_by[0];
        if order.collation.is_some() {
            return None;
        }
        let crate::sql::ast::Expr::Column {
            table: order_table,
            column: order_column,
        } = &order.expr
        else {
            return None;
        };
        if !prepared_scalar_column_matches_table(order_table.as_deref(), name, alias) {
            return None;
        }
        let order_column_index = table
            .columns
            .iter()
            .position(|candidate| identifiers_equal(&candidate.name, order_column))?;
        if !row_id_alias_column_name(table)
            .is_some_and(|column_name| identifiers_equal(column_name, order_column))
            || table.columns[order_column_index].column_type != ColumnType::Int64
        {
            return None;
        }
        let limit = match query.limit.as_ref() {
            Some(expr) => Some(prepared_usize_literal(expr)?),
            None => None,
        };
        let offset = match query.offset.as_ref() {
            Some(expr) => prepared_usize_literal(expr)?,
            None => 0,
        };
        Some(PreparedSimpleOrderedRowIdProjection {
            table_name: table.name.clone(),
            order_column: table.columns[order_column_index].name.clone(),
            projection_indexes,
            column_names: Arc::from(column_names),
            limit,
            offset,
            descending: order.descending,
        })
    }
    pub(crate) fn prepared_simple_row_id_join_projection(
        statement: &SqlStatement,
        runtime: &EngineRuntime,
    ) -> Option<PreparedSimpleRowIdJoinProjection> {
        let SqlStatement::Query(query) = statement else {
            return None;
        };
        if !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return None;
        }
        let crate::sql::ast::QueryBody::Select(select) = &query.body else {
            return None;
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
        {
            return None;
        }
        let filter = select.filter.as_ref()?;
        let crate::sql::ast::FromItem::Join {
            left,
            right,
            kind: crate::sql::ast::JoinKind::Inner,
            constraint,
        } = &select.from[0]
        else {
            return None;
        };
        let crate::sql::ast::FromItem::Table {
            name: left_name,
            alias: left_alias,
        } = &**left
        else {
            return None;
        };
        let crate::sql::ast::FromItem::Table {
            name: right_name,
            alias: right_alias,
        } = &**right
        else {
            return None;
        };
        if runtime.temp_table_schema(left_name).is_some()
            || runtime.temp_table_schema(right_name).is_some()
            || runtime.catalog.views.keys().any(|view_name| {
                identifiers_equal(view_name, left_name) || identifiers_equal(view_name, right_name)
            })
        {
            return None;
        }
        let left_schema = runtime.catalog.table(left_name)?;
        let right_schema = runtime.catalog.table(right_name)?;
        let left_rowid_column = row_id_alias_column_name(left_schema)?;
        let right_rowid_column = row_id_alias_column_name(right_schema)?;

        let (join_a, join_b) = prepared_join_column_equality(constraint)?;
        let join_a_side =
            prepared_join_column_side(join_a.0, left_name, left_alias, right_name, right_alias)?;
        let join_b_side =
            prepared_join_column_side(join_b.0, left_name, left_alias, right_name, right_alias)?;
        let (left_join_column, right_join_column) = match (join_a_side, join_b_side) {
            (SimpleJoinProjectionSide::Left, SimpleJoinProjectionSide::Right) => {
                (join_a.1, join_b.1)
            }
            (SimpleJoinProjectionSide::Right, SimpleJoinProjectionSide::Left) => {
                (join_b.1, join_a.1)
            }
            _ => return None,
        };
        if !identifiers_equal(left_join_column, left_rowid_column)
            || !identifiers_equal(right_join_column, right_rowid_column)
        {
            return None;
        }

        let (filter_table, filter_column, param_index) = prepared_join_filter_param(filter)?;
        let filter_side = prepared_join_column_side(
            filter_table,
            left_name,
            left_alias,
            right_name,
            right_alias,
        )?;
        match filter_side {
            SimpleJoinProjectionSide::Left
                if !identifiers_equal(filter_column, left_rowid_column) =>
            {
                return None;
            }
            SimpleJoinProjectionSide::Right
                if !identifiers_equal(filter_column, right_rowid_column) =>
            {
                return None;
            }
            _ => {}
        }
        let mut projections = Vec::with_capacity(select.projection.len());
        let mut left_projection_indexes = Vec::new();
        let mut right_projection_indexes = Vec::new();
        let mut column_names = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            let crate::sql::ast::SelectItem::Expr { expr, alias } = item else {
                return None;
            };
            let crate::sql::ast::Expr::Column { table, column } = expr else {
                return None;
            };
            let side = prepared_join_column_side(
                table.as_deref(),
                left_name,
                left_alias,
                right_name,
                right_alias,
            )?;
            let schema = match side {
                SimpleJoinProjectionSide::Left => left_schema,
                SimpleJoinProjectionSide::Right => right_schema,
            };
            let index = schema
                .columns
                .iter()
                .position(|candidate| identifiers_equal(&candidate.name, column))?;
            let projected_index = match side {
                SimpleJoinProjectionSide::Left => {
                    push_prepared_join_projection_index(&mut left_projection_indexes, index)
                }
                SimpleJoinProjectionSide::Right => {
                    push_prepared_join_projection_index(&mut right_projection_indexes, index)
                }
            };
            projections.push(ResolvedSimpleJoinProjection {
                side,
                index: projected_index,
            });
            column_names.push(alias.clone().unwrap_or_else(|| column.clone()));
        }

        Some(PreparedSimpleRowIdJoinProjection {
            left_table_name: left_schema.name.clone(),
            right_table_name: right_schema.name.clone(),
            left_projection_indexes,
            right_projection_indexes,
            projections,
            column_names: Arc::from(column_names),
            param_index,
        })
    }
    pub(crate) fn prepared_simple_scalar_filtered_aggregate(
        statement: &SqlStatement,
        runtime: &EngineRuntime,
    ) -> Option<PreparedSimpleScalarFilteredAggregate> {
        let SqlStatement::Query(query) = statement else {
            return None;
        };
        if !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return None;
        }
        let crate::sql::ast::QueryBody::Select(select) = &query.body else {
            return None;
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.from.len() != 1
            || select.projection.len() != 2
        {
            return None;
        }
        let crate::sql::ast::FromItem::Table { name, alias } = &select.from[0] else {
            return None;
        };
        if runtime.temp_table_schema(name).is_some()
            || runtime
                .catalog
                .views
                .keys()
                .any(|view_name| identifiers_equal(view_name, name))
        {
            return None;
        }
        let table = runtime.catalog.table(name)?;
        if !prepared_table_generated_columns_are_stored(table) {
            return None;
        }
        let param_index = prepared_scalar_filter_param(select.filter.as_ref()?, name, alias)?;
        let mut saw_count = false;
        let mut saw_sum = false;
        for item in &select.projection {
            let crate::sql::ast::SelectItem::Expr { expr, .. } = item else {
                return None;
            };
            if prepared_scalar_count_star(expr) {
                saw_count = true;
                continue;
            }
            if let Some(sum_column) = prepared_scalar_sum_column(expr, name, alias) {
                if table
                    .columns
                    .iter()
                    .any(|column| identifiers_equal(&column.name, sum_column))
                {
                    saw_sum = true;
                    continue;
                }
            }
            return None;
        }
        if !saw_count || !saw_sum {
            return None;
        }
        Some(PreparedSimpleScalarFilteredAggregate {
            table_name: table.name.clone(),
            param_index,
            cache: Arc::new(Mutex::new(PreparedScalarAggregateCache::default())),
        })
    }
    pub(crate) fn prepared_simple_insert(
        &self,
        sql: &str,
        statement: &crate::sql::ast::InsertStatement,
        runtime: &EngineRuntime,
    ) -> Result<Option<Arc<PreparedSimpleInsert>>> {
        self.inner
            .prepared_insert_cache
            .lock()
            .map_err(|_| DbError::internal("prepared insert cache lock poisoned"))?
            .get_or_prepare(
                sql,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
                || runtime.prepare_simple_insert(statement),
            )
    }
    pub(crate) fn prepared_plan_accounted_size(bundle: &PreparedPlanBundle) -> u64 {
        fn string_bytes(value: &str) -> u64 {
            value.len() as u64
        }
        fn string_slice_bytes(values: &[String]) -> u64 {
            values.iter().map(|value| string_bytes(value)).sum()
        }

        let mut total = crate::plan_cache::statement_accounted_size(bundle.statement.as_ref())
            .saturating_add(std::mem::size_of::<PreparedPlanBundle>() as u64);
        if let Some(plan) = &bundle.simple_row_id_projection {
            total = total
                .saturating_add(128)
                .saturating_add(string_bytes(&plan.table_name))
                .saturating_add(
                    (plan.projection_indexes.len() * std::mem::size_of::<usize>()) as u64,
                )
                .saturating_add(string_slice_bytes(&plan.column_names));
        }
        if let Some(plan) = &bundle.simple_indexed_projection {
            total = total
                .saturating_add(192)
                .saturating_add(string_bytes(&plan.table_name))
                .saturating_add(
                    (plan.projection_indexes.len() * std::mem::size_of::<usize>()) as u64,
                )
                .saturating_add(string_slice_bytes(&plan.column_names));
        }
        if let Some(plan) = &bundle.simple_row_id_range_projection {
            total = total
                .saturating_add(160)
                .saturating_add(string_bytes(&plan.table_name))
                .saturating_add(string_bytes(&plan.filter_column))
                .saturating_add(
                    (plan.projection_indexes.len() * std::mem::size_of::<usize>()) as u64,
                )
                .saturating_add(string_slice_bytes(&plan.column_names));
        }
        if let Some(plan) = &bundle.simple_ordered_row_id_projection {
            total = total
                .saturating_add(160)
                .saturating_add(string_bytes(&plan.table_name))
                .saturating_add(string_bytes(&plan.order_column))
                .saturating_add(
                    (plan.projection_indexes.len() * std::mem::size_of::<usize>()) as u64,
                )
                .saturating_add(string_slice_bytes(&plan.column_names));
        }
        if let Some(plan) = &bundle.simple_row_id_join_projection {
            total = total
                .saturating_add(256)
                .saturating_add(string_bytes(&plan.left_table_name))
                .saturating_add(string_bytes(&plan.right_table_name))
                .saturating_add(
                    ((plan.left_projection_indexes.len()
                        + plan.right_projection_indexes.len()
                        + plan.projections.len())
                        * std::mem::size_of::<usize>()) as u64,
                )
                .saturating_add(string_slice_bytes(&plan.column_names));
        }
        if let Some(plan) = &bundle.simple_scalar_filtered_aggregate {
            total = total
                .saturating_add(128)
                .saturating_add(string_bytes(&plan.table_name));
        }
        if bundle.prepared_insert.is_some() {
            total = total.saturating_add(512);
        }
        if bundle.prepared_update.is_some() {
            total = total.saturating_add(384);
        }
        if bundle.prepared_delete.is_some() {
            total = total.saturating_add(384);
        }
        total
    }
    pub(crate) fn prepared_read_row_source_row_limit(&self) -> usize {
        let row_limit = self
            .inner
            .config
            .cache_size_mb
            .saturating_mul(PREPARED_READ_ROW_SOURCE_ROWS_PER_CACHE_MB);
        if row_limit == 0 {
            0
        } else {
            row_limit.max(PREPARED_READ_ROW_SOURCE_MIN_ROW_LIMIT)
        }
    }
    pub(crate) fn prepared_insert_current_next_row_id(
        runtime: &EngineRuntime,
        prepared_insert: &PreparedSimpleInsert,
    ) -> Result<Option<i64>> {
        let Some(table_name) = prepared_insert.catalog_table_name.as_deref() else {
            return Ok(None);
        };
        runtime
            .catalog
            .tables
            .get(table_name)
            .map(|table| Some(table.next_row_id))
            .ok_or_else(|| DbError::sql(format!("unknown table {}", prepared_insert.table_name)))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_execute_prepared_insert_in_runtime_state(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
        persistent_changed: &mut bool,
        indexes_maybe_stale: &mut bool,
        prepared_insert_runtime_cache: &mut HashMap<usize, Arc<PreparedSimpleInsert>>,
    ) -> Result<Option<QueryResult>> {
        let Some(insert_plan) = self.prepared_insert_plan_for_runtime_state(
            prepared,
            runtime,
            snapshot_lsn,
            indexes_maybe_stale,
            prepared_insert_runtime_cache,
        )?
        else {
            return Ok(None);
        };

        let result = runtime.execute_prepared_simple_insert(
            insert_plan.as_ref(),
            params,
            self.inner.config.page_size,
        )?;
        *persistent_changed |=
            Self::prepared_insert_changes_persistent_table(runtime, insert_plan.as_ref());
        Ok(Some(result))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_execute_prepared_insert_in_runtime_state_mut(
        &self,
        prepared: &PreparedStatement,
        params: &mut [Value],
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
        persistent_changed: &mut bool,
        indexes_maybe_stale: &mut bool,
        prepared_insert_runtime_cache: &mut HashMap<usize, Arc<PreparedSimpleInsert>>,
        prepared_insert_last_cache_key: &mut Option<usize>,
        prepared_insert_last_plan: &mut Option<Arc<PreparedSimpleInsert>>,
        prepared_insert_last_next_row_id: &mut Option<i64>,
        prepared_insert_candidate: &mut Vec<Value>,
    ) -> Result<Option<QueryResult>> {
        let Some(insert_plan) = self.prepared_insert_plan_for_runtime_state(
            prepared,
            runtime,
            snapshot_lsn,
            indexes_maybe_stale,
            prepared_insert_runtime_cache,
        )?
        else {
            return Ok(None);
        };

        if !Self::prepared_insert_uses_direct_positional_params(insert_plan.as_ref(), params.len())
        {
            return Ok(None);
        }

        let cache_key = Self::prepared_statement_cache_key(prepared);
        *prepared_insert_last_cache_key = Some(cache_key);
        *prepared_insert_last_plan = Some(Arc::clone(&insert_plan));
        let result = runtime
            .execute_prepared_simple_insert_positional_params_in_place_with_candidate(
                insert_plan.as_ref(),
                params,
                prepared_insert_candidate,
                self.inner.config.page_size,
            )?;
        *prepared_insert_last_next_row_id =
            Self::prepared_insert_current_next_row_id(runtime, insert_plan.as_ref())?;
        if !*persistent_changed {
            *persistent_changed |=
                Self::prepared_insert_changes_persistent_table(runtime, insert_plan.as_ref());
        }
        Ok(Some(QueryResult::with_affected_rows(result)))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_execute_prepared_update_in_runtime_state(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_update: &PreparedSimpleUpdate,
        params: &[Value],
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
        persistent_changed: &mut bool,
        indexes_maybe_stale: &mut bool,
    ) -> Result<Option<QueryResult>> {
        self.load_runtime_table_row_sources_at_snapshot(
            runtime,
            &[prepared_update.table_name.as_str()],
            snapshot_lsn,
        )?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_update(prepared_update) {
            return Ok(None);
        }
        let temp_only = self.statement_is_temp_only(runtime, prepared_statement.statement.as_ref());
        let result = runtime.execute_prepared_simple_update(
            prepared_update,
            params,
            self.inner.config.page_size,
        )?;
        *persistent_changed |= !temp_only;
        *indexes_maybe_stale |= Self::runtime_has_stale_indexes(runtime);
        Ok(Some(result))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_execute_prepared_delete_in_runtime_state(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_delete: &PreparedSimpleDelete,
        params: &[Value],
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
        persistent_changed: &mut bool,
        indexes_maybe_stale: &mut bool,
    ) -> Result<Option<QueryResult>> {
        let row_source_table_names = prepared_delete.required_row_source_table_names();
        let child_index_targets = prepared_delete.child_index_hydration_targets();
        self.load_runtime_table_row_sources_and_child_indexes_at_snapshot(
            runtime,
            &row_source_table_names,
            &child_index_targets,
            snapshot_lsn,
        )?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_delete(prepared_delete) {
            return Ok(None);
        }
        let temp_only = self.statement_is_temp_only(runtime, prepared_statement.statement.as_ref());
        let result = runtime.execute_prepared_simple_delete(
            prepared_delete,
            params,
            self.inner.config.page_size,
        )?;
        *persistent_changed |= !temp_only;
        *indexes_maybe_stale |= Self::runtime_has_stale_indexes(runtime);
        Ok(Some(result))
    }
    pub(crate) fn try_execute_prepared_inspection_query(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if let Some(result) =
            self.try_execute_sync_inspection_query(&prepared.prepared_sql, params)?
        {
            return Ok(Some(result));
        }
        if let Some(result) = crate::extensions::try_execute_extension_inspection_query(
            self,
            &prepared.prepared_sql,
            params,
        )? {
            return Ok(Some(result));
        }
        Ok(None)
    }
}
