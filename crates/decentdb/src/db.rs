//! Stable database owner and bootstrap lifecycle entry points.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};
use std::time::Duration;

#[cfg(feature = "bench-internals")]
use crate::benchmark::{
    READ_PATH_HELD_SNAPSHOTS_LOCK_COUNT, READ_PATH_WAL_READER_BEGIN_COUNT,
    READ_PATH_WRITE_TXN_LOCK_COUNT,
};
use crate::catalog::{
    identifiers_equal, CatalogHandle, CheckConstraint, ColumnSchema, ColumnType, ForeignKeyAction,
    ForeignKeyConstraint, IndexColumn, IndexKind, IndexSchema, TableSchema, TriggerEvent,
    TriggerKind, TriggerSchema, ViewSchema,
};
use crate::config::{DbConfig, ProcessCoordinationMode, WalSyncMode};
use crate::error::{DbError, Result};
use crate::exec::dml::{
    row_id_alias_column_name, PreparedDeleteLookup, PreparedSimpleDelete, PreparedSimpleInsert,
    PreparedSimpleUpdate, PreparedSimpleValueSource,
};
use crate::exec::{
    decode_paged_table_manifest_payload, read_table_payload_row_count_from_bytes,
    row_satisfies_expression, statement_is_read_only, BulkLoadOptions, EngineRuntime, QueryResult,
    QueryRow, ResolvedSimpleJoinProjection, ResolvedSimpleRowIdJoinProjectionRequest,
    ResolvedSimpleRowIdProjectionRequest, ResolvedSimpleRowIdRangeProjectionRequest, RuntimeIndex,
    SimpleJoinProjectionSide, SimpleRangeBoundValue, SimpleRowIdProjectionRequest, TableData,
};
use crate::metadata::{
    CheckConstraintInfo, ColumnInfo, ForeignKeyInfo, HeaderInfo, IndexInfo, IndexVerification,
    QueryContract, SchemaColumnInfo, SchemaIndexInfo, SchemaSnapshot, SchemaTableInfo,
    SchemaTriggerInfo, SchemaViewInfo, StorageInfo, TableInfo, ToolingMetadata, TriggerInfo,
    ViewInfo,
};
use crate::plan_cache::PlanCache;
use crate::reactive::{
    ChangeSource, ChangeStreamOptions, PendingReactiveCommit, QueryWatchOptions, RangeWatchOptions,
    ReactiveHub, ReactiveMetricsSnapshot, ReactiveSubscriptionSnapshot, TableWatchOptions,
    WatchHandle,
};
use crate::record::overflow::read_overflow;
use crate::record::value::{
    format_cidr, format_date_days, format_interval, format_ip_addr, format_mac_addr,
    format_time_micros, format_timestamp_tz_micros, normalize_decimal, parse_cidr, parse_date_days,
    parse_decimal_text, parse_interval, parse_ip_addr, parse_mac_addr, parse_time_micros,
    parse_timestamp_tz_micros, Value,
};
use crate::search::fulltext::analyzer::{
    AnalyzerConfig, AnalyzerDiacritics, AnalyzerLanguage, AnalyzerStemmer, AnalyzerStopwords,
    AnalyzerTokenization,
};
use crate::sql::ast::Statement as SqlStatement;
use crate::sql::parser::{parse_expression_sql, parse_sql_statement, rewrite_legacy_trigger_body};
use crate::storage::freelist::{decode_freelist_next, encode_freelist_page};
use crate::storage::page::{self, PageId, PageStore};
use crate::storage::{self, DatabaseHeader, PagerHandle};
use crate::sync::SyncContext;
use crate::sync::{
    current_time_micros, validate_sync_scope_definition, ApplyChangesetOptions,
    CreateChangesetOptions, CreateShapeOptions, InspectChangesetOptions, InvertChangesetOptions,
    ShapeAckOptions, SyncChangeBatch, SyncChangeset, SyncChangesetApplyResult,
    SyncChangesetCapabilities, SyncChangesetCheckpoint, SyncChangesetCompatibility,
    SyncChangesetHistory, SyncChangesetInspection, SyncChangesetLimits, SyncChangesetRecord,
    SyncChangesetSource, SyncConflict, SyncConflictPolicy, SyncConflictPolicyConfig,
    SyncDoctorSeverity, SyncImportSummary, SyncJournalIntegrityReport, SyncJournalIssue,
    SyncJournalRecord, SyncOperation, SyncOperationalDoctorReport, SyncPeer, SyncPeerLag,
    SyncPeerScopeBinding, SyncPrincipal, SyncPruneSummary, SyncRelaySession, SyncRelayStatus,
    SyncRetentionReport, SyncRunDirection, SyncRunSummary, SyncScope, SyncSession, SyncShape,
    SyncShapeCheckpoint, SyncShapeClient, SyncShapeDelivery, SyncStatus,
};
use crate::vfs::faulty::{self, FailAction, Failpoint};
use crate::vfs::{
    is_memory_path, read_exact_at, write_all_at, FileKind, OpenMode, VfsFile, VfsHandle,
};
use crate::wal::reader_registry::ReaderGuard;
use crate::wal::savepoint::StatementSavepoint;
use crate::wal::WalHandle;
use crate::write_queue::{QueuedWriteOptions, WriteQueue, WriteQueueMetricsSnapshot};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

mod audit;
mod branches;
mod open;
mod query_api;
mod schema;
mod sync_api;
use audit::*;
use branches::*;
use open::*;
use query_api::*;
use schema::*;
use sync_api::*;

const APPLICATION_PRAGMA_TABLE: &str = "__decentdb_application_pragmas";
static AUDIT_EVENT_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Stable engine owner used across later storage, SQL, and FFI slices.
#[derive(Clone, Debug)]
pub struct Db {
    inner: Arc<DbInner>,
}

const AUTOCOMMIT_PAGED_ROW_SOURCE_MAX_RESIDENT: usize = 4;
const PREPARED_READ_ROW_SOURCE_MIN_ROWS: usize = 4_096;
const PREPARED_READ_ROW_SOURCE_ROWS_PER_CACHE_MB: usize = 8_192;

#[derive(Debug, Default)]
struct ReadOnlyPagedRowSourceResidency {
    next_touch_gen: u64,
    table_touch_generation: HashMap<String, u64>,
}

#[derive(Debug)]
struct PagerReadStore<'a> {
    db: &'a Db,
    snapshot_token: u64,
    explicit_snapshot_lsn: Option<u64>,
}

#[derive(Clone, Debug)]
struct SyncConflictRecordData {
    conflict_type: String,
    message: String,
    local_row_json: Option<serde_json::Value>,
    resolution: Option<String>,
    resolved_at_micros: Option<i64>,
    resolved_by: Option<String>,
    resolution_note: Option<String>,
    policy_name: Option<String>,
}

#[derive(Clone, Debug)]
enum SyncImportRecordOutcome {
    Applied,
    Conflict(SyncConflictRecordData),
    Resolved(SyncConflictRecordData),
}

impl<'a> PagerReadStore<'a> {
    fn new(db: &'a Db) -> Result<Self> {
        Ok(Self {
            db,
            snapshot_token: db.hold_snapshot()?,
            explicit_snapshot_lsn: None,
        })
    }

    fn with_snapshot_lsn(db: &'a Db, snapshot_lsn: u64) -> Self {
        Self {
            db,
            snapshot_token: 0,
            explicit_snapshot_lsn: Some(snapshot_lsn),
        }
    }
}

impl Drop for PagerReadStore<'_> {
    fn drop(&mut self) {
        if self.snapshot_token != 0 {
            let _ = self.db.release_snapshot(self.snapshot_token);
        }
    }
}

impl PageStore for PagerReadStore<'_> {
    fn page_size(&self) -> u32 {
        self.db.config().page_size
    }

    fn allocate_page(&mut self) -> Result<PageId> {
        Err(DbError::internal(
            "PagerReadStore does not support page allocation",
        ))
    }

    fn free_page(&mut self, _page_id: PageId) -> Result<()> {
        Err(DbError::internal(
            "PagerReadStore does not support freeing pages",
        ))
    }

    fn read_page(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        if let Some(lsn) = self.explicit_snapshot_lsn {
            return self.db.read_page_at_snapshot_lsn(page_id, lsn);
        }
        self.db.read_page_for_snapshot(self.snapshot_token, page_id)
    }

    fn advise_sequential(&self) -> Result<()> {
        self.db.advise_sequential()
    }

    fn write_page(&mut self, _page_id: PageId, _data: &[u8]) -> Result<()> {
        Err(DbError::internal(
            "PagerReadStore does not support writing pages",
        ))
    }
}

/// Reusable single-statement execution handle bound to the current schema.
///
/// Prepared statements become invalid after schema changes and must be
/// re-prepared. Data changes remain visible across executions.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    db: Db,
    schema_cookie: u32,
    temp_schema_cookie: u32,
    statement: Arc<SqlStatement>,
    prepared_sql: String,
    simple_row_id_projection: Option<PreparedSimpleRowIdProjection>,
    simple_row_id_range_projection: Option<PreparedSimpleRowIdRangeProjection>,
    simple_row_id_join_projection: Option<PreparedSimpleRowIdJoinProjection>,
    simple_scalar_filtered_aggregate: Option<PreparedSimpleScalarFilteredAggregate>,
    prepared_insert: Option<Arc<PreparedSimpleInsert>>,
    prepared_update: Option<Arc<PreparedSimpleUpdate>>,
    prepared_delete: Option<Arc<PreparedSimpleDelete>>,
    read_only: bool,
}

#[derive(Clone, Debug)]
struct PreparedPlanBundle {
    statement: Arc<SqlStatement>,
    simple_row_id_projection: Option<PreparedSimpleRowIdProjection>,
    simple_row_id_range_projection: Option<PreparedSimpleRowIdRangeProjection>,
    simple_row_id_join_projection: Option<PreparedSimpleRowIdJoinProjection>,
    simple_scalar_filtered_aggregate: Option<PreparedSimpleScalarFilteredAggregate>,
    prepared_insert: Option<Arc<PreparedSimpleInsert>>,
    prepared_update: Option<Arc<PreparedSimpleUpdate>>,
    prepared_delete: Option<Arc<PreparedSimpleDelete>>,
    read_only: bool,
}

#[derive(Clone, Debug)]
struct PreparedPlanCacheEntry {
    key_hash: u64,
    bundle: PreparedPlanBundle,
    plan_size_bytes: u64,
    persistent_schema_cookie: u32,
    temp_schema_cookie: u32,
    policy_mask_generation: u32,
    hit_count: u64,
    last_used_at_micros: i64,
}

#[derive(Debug)]
struct PreparedPlanCache {
    enabled: bool,
    max_size_bytes: u64,
    current_size_bytes: u64,
    entries: HashMap<crate::plan_cache::PlanCacheKey, PreparedPlanCacheEntry>,
    order: VecDeque<crate::plan_cache::PlanCacheKey>,
    total_hits: u64,
    total_misses: u64,
    total_evictions: u64,
    total_oversized_refusals: u64,
}

impl PreparedPlanCache {
    fn new(config: &crate::plan_cache::PlanCacheConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_size_bytes: config.max_size_bytes,
            current_size_bytes: 0,
            entries: HashMap::new(),
            order: VecDeque::new(),
            total_hits: 0,
            total_misses: 0,
            total_evictions: 0,
            total_oversized_refusals: 0,
        }
    }

    fn get(
        &mut self,
        key: &crate::plan_cache::PlanCacheKey,
        current_persistent_cookie: u32,
        current_temp_cookie: u32,
        current_policy_mask_generation: u32,
    ) -> Option<PreparedPlanBundle> {
        if !self.enabled {
            return None;
        }
        let entry = match self.entries.get(key) {
            Some(entry) => entry,
            None => {
                self.total_misses = self.total_misses.saturating_add(1);
                return None;
            }
        };
        if entry.persistent_schema_cookie != current_persistent_cookie
            || entry.temp_schema_cookie != current_temp_cookie
            || entry.policy_mask_generation != current_policy_mask_generation
        {
            let _ = entry;
            self.evict_key(key);
            self.total_misses = self.total_misses.saturating_add(1);
            return None;
        }
        let bundle = entry.bundle.clone();
        let _ = entry;
        self.promote(key);
        self.total_hits = self.total_hits.saturating_add(1);
        if let Some(entry) = self.entries.get_mut(key) {
            entry.hit_count = entry.hit_count.saturating_add(1);
            entry.last_used_at_micros = current_time_micros();
        }
        Some(bundle)
    }

    fn insert(
        &mut self,
        key: crate::plan_cache::PlanCacheKey,
        bundle: PreparedPlanBundle,
        plan_size_bytes: u64,
    ) {
        const FIXED_OVERHEAD_BYTES: u64 =
            crate::plan_cache::PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES as u64;
        if !self.enabled {
            return;
        }
        if plan_size_bytes.saturating_add(FIXED_OVERHEAD_BYTES) > self.max_size_bytes {
            self.total_oversized_refusals = self.total_oversized_refusals.saturating_add(1);
            return;
        }
        if let Some(previous) = self.entries.remove(&key) {
            self.current_size_bytes = self
                .current_size_bytes
                .saturating_sub(previous.plan_size_bytes)
                .saturating_sub(FIXED_OVERHEAD_BYTES);
            self.order.retain(|candidate| candidate != &key);
        }
        let entry = PreparedPlanCacheEntry {
            key_hash: key.stable_hash(),
            bundle,
            plan_size_bytes,
            persistent_schema_cookie: key.persistent_schema_cookie,
            temp_schema_cookie: key.temp_schema_cookie,
            policy_mask_generation: key.policy_mask_generation,
            hit_count: 0,
            last_used_at_micros: current_time_micros(),
        };
        self.entries.insert(key.clone(), entry);
        self.order.push_back(key);
        self.current_size_bytes = self
            .current_size_bytes
            .saturating_add(plan_size_bytes)
            .saturating_add(FIXED_OVERHEAD_BYTES);
        self.evict_to_fit(self.max_size_bytes);
    }

    fn invalidate_all(&mut self) {
        let evicted = self.entries.len() as u64;
        self.total_evictions = self.total_evictions.saturating_add(evicted);
        self.entries.clear();
        self.order.clear();
        self.current_size_bytes = 0;
    }

    fn flush(&mut self) {
        self.invalidate_all();
        self.total_hits = 0;
        self.total_misses = 0;
        self.total_evictions = 0;
        self.total_oversized_refusals = 0;
    }

    fn snapshot_entries(&self) -> Vec<PreparedPlanCacheEntry> {
        let mut entries = self.entries.values().cloned().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.key_hash);
        entries
    }

    fn summary(&self) -> crate::plan_cache::PlanCacheSummary {
        let total = self.total_hits.saturating_add(self.total_misses);
        let hit_rate = if total == 0 {
            0.0
        } else {
            (self.total_hits as f64) * 100.0 / (total as f64)
        };
        crate::plan_cache::PlanCacheSummary {
            scope: "connection",
            total_entries: self.entries.len() as u64,
            total_hits: self.total_hits,
            total_misses: self.total_misses,
            total_evictions: self.total_evictions,
            total_size_bytes: self.current_size_bytes,
            max_size_bytes: self.max_size_bytes,
            total_oversized_refusals: self.total_oversized_refusals,
            hit_rate,
        }
    }

    fn promote(&mut self, key: &crate::plan_cache::PlanCacheKey) {
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.clone());
    }

    fn evict_key(&mut self, key: &crate::plan_cache::PlanCacheKey) {
        const FIXED_OVERHEAD_BYTES: u64 =
            crate::plan_cache::PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES as u64;
        if let Some(entry) = self.entries.remove(key) {
            self.current_size_bytes = self
                .current_size_bytes
                .saturating_sub(entry.plan_size_bytes)
                .saturating_sub(FIXED_OVERHEAD_BYTES);
            self.total_evictions = self.total_evictions.saturating_add(1);
        }
        self.order.retain(|candidate| candidate != key);
    }

    fn evict_to_fit(&mut self, target_size: u64) {
        const FIXED_OVERHEAD_BYTES: u64 =
            crate::plan_cache::PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES as u64;
        while self.current_size_bytes > target_size {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&oldest) {
                self.current_size_bytes = self
                    .current_size_bytes
                    .saturating_sub(entry.plan_size_bytes)
                    .saturating_sub(FIXED_OVERHEAD_BYTES);
                self.total_evictions = self.total_evictions.saturating_add(1);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct PreparedSimpleRowIdProjection {
    table_name: String,
    projection_indexes: Vec<usize>,
    column_names: Arc<[String]>,
    param_index: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedSimpleRangeBoundParam {
    inclusive: bool,
    param_index: usize,
}

#[derive(Clone, Debug)]
struct PreparedSimpleRowIdRangeProjection {
    table_name: String,
    projection_indexes: Vec<usize>,
    column_names: Arc<[String]>,
    filter_column: String,
    lower_bound: Option<PreparedSimpleRangeBoundParam>,
    upper_bound: Option<PreparedSimpleRangeBoundParam>,
    limit_param_index: usize,
}

#[derive(Clone, Debug)]
struct PreparedSimpleRowIdJoinProjection {
    left_table_name: String,
    right_table_name: String,
    left_projection_indexes: Vec<usize>,
    right_projection_indexes: Vec<usize>,
    projections: Vec<ResolvedSimpleJoinProjection>,
    column_names: Arc<[String]>,
    param_index: usize,
}

#[derive(Clone, Debug)]
struct PreparedSimpleScalarFilteredAggregate {
    table_name: String,
    param_index: usize,
    cache: Arc<Mutex<PreparedScalarAggregateCache>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PreparedScalarAggregateCacheKey {
    snapshot_lsn: u64,
    pointer_head_page_id: u32,
    pointer_logical_len: u32,
    pointer_flags: u8,
    checksum: u32,
    row_count: usize,
    param_value: i64,
}

#[derive(Debug, Default)]
struct PreparedScalarAggregateCache {
    entries: HashMap<PreparedScalarAggregateCacheKey, QueryResult>,
    insertion_order: VecDeque<PreparedScalarAggregateCacheKey>,
}

const PREPARED_SCALAR_AGGREGATE_CACHE_LIMIT: usize = 256;

impl PreparedScalarAggregateCache {
    fn get(&self, key: &PreparedScalarAggregateCacheKey) -> Option<QueryResult> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: PreparedScalarAggregateCacheKey, result: QueryResult) {
        if let Some(existing) = self.entries.get_mut(&key) {
            *existing = result;
            return;
        }
        if self.entries.len() >= PREPARED_SCALAR_AGGREGATE_CACHE_LIMIT {
            if let Some(evicted) = self.insertion_order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
        self.insertion_order.push_back(key);
        self.entries.insert(key, result);
    }
}

/// Transaction-scoped prepared statement executor for repeated rows.
///
/// This handle validates the prepared statement and resolves the insert fast
/// path once, then reuses that work for each row executed against the same SQL
/// transaction.
#[derive(Debug)]
pub struct PreparedStatementBatch<'txn, 'db> {
    db: &'db Db,
    state: &'txn mut ExclusiveSqlTxnState<'db>,
    prepared: &'txn PreparedStatement,
    prepared_insert: Option<Arc<PreparedSimpleInsert>>,
    direct_positional: bool,
}

/// Exclusive SQL transaction handle that keeps mutable runtime state reserved.
///
/// While this handle is active, callers should use it for all SQL work on the
/// same `Db` handle until `commit` or `rollback`.
#[derive(Debug)]
pub struct SqlTransaction<'a> {
    db: &'a Db,
    state: Option<ExclusiveSqlTxnState<'a>>,
}

impl PreparedStatement {
    /// Executes the prepared statement with the provided positional `$n`
    /// parameters.
    pub fn execute(&self, params: &[Value]) -> Result<QueryResult> {
        self.db.execute_prepared_statement(self, params)
    }

    /// Executes the prepared statement with mutable positional parameters.
    ///
    /// This avoids cloning positional values for prepared insert fast paths
    /// that consume all parameters directly. Callers should treat parameter
    /// values as consumed once execution completes.
    pub fn execute_mut(&self, params: &mut [Value]) -> Result<QueryResult> {
        self.db.execute_prepared_statement_mut(self, params)
    }

    /// Executes the prepared statement inside an active [`SqlTransaction`].
    pub fn execute_in(
        &self,
        txn: &mut SqlTransaction<'_>,
        params: &[Value],
    ) -> Result<QueryResult> {
        txn.execute_prepared(self, params)
    }

    /// Executes the prepared statement inside an active [`SqlTransaction`] using
    /// mutable positional parameters.
    ///
    /// The transaction may mutate `params` during execution; callers should
    /// treat parameter values as consumed once execution completes.
    pub fn execute_in_mut(
        &self,
        txn: &mut SqlTransaction<'_>,
        params: &mut [Value],
    ) -> Result<QueryResult> {
        txn.execute_prepared_mut(self, params)
    }

    #[cfg(test)]
    pub(crate) fn statement_arc_for_tests(&self) -> Arc<SqlStatement> {
        Arc::clone(&self.statement)
    }
}

impl<'db> SqlTransaction<'db> {
    /// Prepares a single SQL statement against this transaction's current schema.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        self.db.prepare_with_runtime(sql, &state.runtime)
    }

    /// Executes a prepared statement inside this transaction without per-row
    /// `Db` transaction lock churn.
    pub fn execute_prepared(
        &mut self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let state = self
            .state
            .as_mut()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        self.db
            .execute_prepared_in_exclusive_state(prepared, params, state)
    }

    /// Executes the prepared statement inside an active exclusive transaction using
    /// mutable positional parameters.
    ///
    /// This avoids cloning positional values for `PreparedStatement::Insert`
    /// fast paths that consume all parameters directly.
    pub fn execute_prepared_mut(
        &mut self,
        prepared: &PreparedStatement,
        params: &mut [Value],
    ) -> Result<QueryResult> {
        let state = self
            .state
            .as_mut()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        self.db
            .execute_prepared_in_exclusive_state_mut(prepared, params, state)
    }

    /// Creates a reusable executor for applying many parameter rows to one
    /// prepared statement inside this transaction.
    ///
    /// For simple positional INSERT statements, the returned batch handle
    /// reuses the prepared insert plan for every row and avoids per-row schema
    /// validation. The handle borrows this transaction mutably until it is
    /// dropped.
    pub fn prepared_batch<'txn>(
        &'txn mut self,
        prepared: &'txn PreparedStatement,
        param_count: usize,
    ) -> Result<PreparedStatementBatch<'txn, 'db>> {
        let state = self
            .state
            .as_mut()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        self.db
            .prepare_batch_in_exclusive_state(prepared, param_count, state)
    }

    /// Commits this transaction's reserved runtime into the WAL-backed database.
    pub fn commit(mut self) -> Result<u64> {
        let state = self
            .state
            .take()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        let result = self.db.commit_exclusive_sql_txn(state);
        let release = self.deactivate();
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(lsn), Ok(())) => Ok(lsn),
        }
    }

    /// Rolls this transaction back and releases the handle.
    pub fn rollback(mut self) -> Result<()> {
        let state = self
            .state
            .take()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        let result = self.db.rollback_exclusive_sql_txn(state);
        self.deactivate().and(result)
    }

    fn deactivate(&mut self) -> Result<()> {
        let mut txn = self
            .db
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        *txn = SqlTxnSlot::None;
        self.db.inner.sql_txn_active.store(false, Ordering::Release);
        Ok(())
    }
}

impl PreparedStatementBatch<'_, '_> {
    /// Executes one row using mutable positional parameters.
    ///
    /// The batch may consume parameter values while executing. Callers should
    /// refill the parameter buffer before the next call.
    pub fn execute_mut(&mut self, params: &mut [Value]) -> Result<u64> {
        if self.direct_positional {
            let prepared_insert = self
                .prepared_insert
                .as_ref()
                .ok_or_else(|| DbError::internal("missing prepared insert batch plan"))?;
            let affected = self
                .state
                .runtime
                .execute_prepared_simple_insert_positional_params_in_place(
                    prepared_insert.as_ref(),
                    params,
                    self.db.inner.config.page_size,
                )?;
            self.state.persistent_changed |=
                Db::prepared_insert_changes_persistent_table(&self.state.runtime, prepared_insert);
            return Ok(affected);
        }

        let result =
            self.db
                .execute_prepared_in_exclusive_state_mut(self.prepared, params, self.state)?;
        Ok(result.affected_rows())
    }
}

impl Drop for SqlTransaction<'_> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            let _ = self.db.rollback_exclusive_sql_txn(state);
        }
        if let Ok(mut txn) = self.db.inner.sql_txn.lock() {
            *txn = SqlTxnSlot::None;
            self.db.inner.sql_txn_active.store(false, Ordering::Release);
        }
    }
}

#[derive(Debug)]
struct DbInner {
    path: PathBuf,
    config: DbConfig,
    vfs: VfsHandle,
    pager: PagerHandle,
    wal: WalHandle,
    catalog: CatalogHandle,
    engine: RwLock<EngineRuntime>,
    last_runtime_lsn: AtomicU64,
    writer_last_commit_lsn: AtomicU64,
    last_seen_checkpoint_epoch: AtomicU64,
    last_explicit_checkpoint_epoch: AtomicU64,
    sql_write_lock: Mutex<()>,
    sql_txn: Mutex<SqlTxnSlot>,
    sql_txn_active: AtomicBool,
    write_txn_active: AtomicBool,
    write_txn: Mutex<WriteTxn>,
    busy_timeout_ms: AtomicU64,
    temp_state: Mutex<TempSchemaState>,
    statement_cache: Mutex<StatementCache>,
    prepared_insert_cache: Mutex<PreparedInsertCache>,
    plan_cache: Mutex<PlanCache>,
    prepared_plan_cache: Mutex<PreparedPlanCache>,
    policy_mask_generation: crate::plan_cache::PolicyMaskGeneration,
    held_snapshots: Mutex<HashMap<u64, ReaderGuard>>,
    sync_ctx: SyncContext,
    reactive_registry_key: Option<PathBuf>,
    reactive_hub: OnceLock<Arc<ReactiveHub>>,
    audit_context: Arc<Mutex<crate::security::AuditContext>>,
    read_only_paged_row_source_residency: Mutex<ReadOnlyPagedRowSourceResidency>,
    write_queue: OnceLock<WriteQueue>,
    tracing: Arc<crate::tracing::RuntimeTraceState>,
}

impl Drop for DbInner {
    fn drop(&mut self) {
        self.wal.shutdown_background_checkpointer();
        if self.wal.latest_snapshot() == 0 {
            return;
        }
        // Shared file-backed WAL handles can outlive any single Db and are
        // paired with independent pager caches. Implicit drop-time checkpoint
        // copyback can invalidate another handle's cached pages; leave shared
        // WAL cleanup to explicit checkpoints or a future coordinated pager
        // registry.
        if self.wal.is_shared() {
            return;
        }
        self.wal.set_checkpoint_pending(true);
        if self.wal.strong_handle_count() != 1 {
            self.wal.set_checkpoint_pending(false);
            return;
        }
        if self.write_txn.lock().map(|txn| txn.active).unwrap_or(true) {
            self.wal.set_checkpoint_pending(false);
            return;
        }
        if self
            .sql_txn
            .lock()
            .map(|txn| !matches!(*txn, SqlTxnSlot::None))
            .unwrap_or(true)
        {
            self.wal.set_checkpoint_pending(false);
            return;
        }
        if let Ok(mut held_snapshots) = self.held_snapshots.lock() {
            held_snapshots.clear();
        }
        if is_memory_path(&self.path) {
            let _ = self
                .wal
                .checkpoint(&self.pager, self.config.checkpoint_timeout_sec);
            return;
        }

        let wal = self.wal.clone();
        let checkpoint_wal = wal.clone();
        let pager = self.pager.clone();
        let timeout = self.config.checkpoint_timeout_sec;
        if std::thread::Builder::new()
            .name("decentdb-drop-checkpoint".to_string())
            .spawn(move || {
                let _ = checkpoint_wal.checkpoint(&pager, timeout);
            })
            .is_err()
        {
            wal.set_checkpoint_pending(false);
        }
    }
}

#[derive(Debug, Default)]
struct WriteTxn {
    active: bool,
    staged_pages: BTreeMap<PageId, Vec<u8>>,
    snapshot_reader: Option<ReaderGuard>,
}

#[derive(Clone, Debug, Default)]
struct TempSchemaState {
    schema_cookie: u32,
    tables: Arc<BTreeMap<String, TableSchema>>,
    table_data: Arc<BTreeMap<String, Arc<TableData>>>,
    views: Arc<BTreeMap<String, ViewSchema>>,
    indexes: Arc<BTreeMap<String, IndexSchema>>,
}

impl TempSchemaState {
    fn apply_to_runtime(&self, runtime: &mut EngineRuntime) {
        runtime.temp_schema_cookie = self.schema_cookie;
        runtime.temp_tables = Arc::clone(&self.tables);
        runtime.temp_table_data = Arc::clone(&self.table_data);
        runtime.temp_views = Arc::clone(&self.views);
        runtime.temp_indexes = Arc::clone(&self.indexes);
    }

    fn update_from_runtime(&mut self, runtime: &EngineRuntime) {
        self.schema_cookie = runtime.temp_schema_cookie;
        self.tables = Arc::clone(&runtime.temp_tables);
        self.table_data = Arc::clone(&runtime.temp_table_data);
        self.views = Arc::clone(&runtime.temp_views);
        self.indexes = Arc::clone(&runtime.temp_indexes);
    }
}

#[derive(Debug)]
struct SqlTxnState {
    runtime: EngineRuntime,
    snapshot_reader: ReaderGuard,
    base_lsn: u64,
    base_checkpoint_epoch: u64,
    persistent_changed: bool,
    indexes_maybe_stale: bool,
    prepared_insert_runtime_cache: HashMap<usize, Arc<PreparedSimpleInsert>>,
    savepoints: Vec<SqlSavepoint>,
}

#[derive(Debug)]
struct ExclusiveSqlTxnState<'a> {
    runtime: RwLockWriteGuard<'a, EngineRuntime>,
    snapshot_reader: Option<ReaderGuard>,
    base_lsn: u64,
    base_checkpoint_epoch: u64,
    persistent_changed: bool,
    indexes_maybe_stale: bool,
    prepared_insert_runtime_cache: HashMap<usize, Arc<PreparedSimpleInsert>>,
}

#[derive(Debug)]
enum SqlTxnSlot {
    None,
    Shared(Box<SqlTxnState>),
    Exclusive,
}

#[derive(Clone, Debug)]
struct SqlSavepoint {
    name: String,
    runtime: EngineRuntime,
    persistent_changed: bool,
    indexes_maybe_stale: bool,
    prepared_insert_runtime_cache: HashMap<usize, Arc<PreparedSimpleInsert>>,
}

impl SqlTxnState {
    fn snapshot_lsn(&self) -> u64 {
        self.snapshot_reader.snapshot_lsn()
    }
}

impl crate::plan_cache::PlanCacheInvalidator for DbInner {
    fn on_persistent_ddl(&self) {
        if let Ok(mut cache) = self.plan_cache.lock() {
            cache.invalidate_all();
        }
        if let Ok(mut cache) = self.prepared_plan_cache.lock() {
            cache.invalidate_all();
        }
    }
    fn on_temp_schema_change(&self) {
        if let Ok(mut cache) = self.plan_cache.lock() {
            cache.invalidate_all();
        }
        if let Ok(mut cache) = self.prepared_plan_cache.lock() {
            cache.invalidate_all();
        }
    }
    fn on_policy_mask_change(&self) {
        self.policy_mask_generation.bump();
        if let Ok(mut cache) = self.plan_cache.lock() {
            cache.invalidate_all();
        }
        if let Ok(mut cache) = self.prepared_plan_cache.lock() {
            cache.invalidate_all();
        }
    }
    fn on_branch_switch(&self) {
        if let Ok(mut cache) = self.plan_cache.lock() {
            cache.invalidate_all();
        }
        if let Ok(mut cache) = self.prepared_plan_cache.lock() {
            cache.invalidate_all();
        }
    }
    fn on_extension_change(&self) {
        if let Ok(mut cache) = self.plan_cache.lock() {
            cache.invalidate_all();
        }
        if let Ok(mut cache) = self.prepared_plan_cache.lock() {
            cache.invalidate_all();
        }
    }
    fn on_explicit_flush(&self) {
        if let Ok(mut cache) = self.plan_cache.lock() {
            cache.flush();
        }
        if let Ok(mut cache) = self.prepared_plan_cache.lock() {
            cache.flush();
        }
    }
}

impl ExclusiveSqlTxnState<'_> {
    fn snapshot_lsn(&self) -> u64 {
        self.snapshot_reader
            .as_ref()
            .expect("exclusive SQL transaction snapshot reader should be active")
            .snapshot_lsn()
    }
}

const STATEMENT_CACHE_CAPACITY: usize = 128;
const PREPARED_INSERT_CACHE_CAPACITY: usize = 128;

#[derive(Debug)]
struct StatementCache {
    entries: HashMap<String, Arc<SqlStatement>>,
    order: VecDeque<String>,
    capacity: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PreparedInsertKey {
    schema_cookie: u32,
    temp_schema_cookie: u32,
    sql: String,
}

#[derive(Debug)]
struct PreparedInsertCache {
    entries: HashMap<PreparedInsertKey, Arc<PreparedSimpleInsert>>,
    order: VecDeque<PreparedInsertKey>,
    capacity: usize,
}

impl Default for StatementCache {
    fn default() -> Self {
        Self::with_capacity(STATEMENT_CACHE_CAPACITY)
    }
}

impl StatementCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get_or_parse(&mut self, sql: &str) -> Result<Arc<SqlStatement>> {
        if let Some(statement) = self.entries.get(sql) {
            let statement = Arc::clone(statement);
            self.promote(sql);
            return Ok(statement);
        }

        let statement = Arc::new(parse_sql_statement(sql)?);
        if self.capacity == 0 {
            return Ok(statement);
        }

        while self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }

        let key = sql.to_string();
        self.order.push_back(key.clone());
        self.entries.insert(key, Arc::clone(&statement));
        Ok(statement)
    }

    fn promote(&mut self, sql: &str) {
        self.order.retain(|key| key != sql);
        self.order.push_back(sql.to_string());
    }
}

impl Default for PreparedInsertCache {
    fn default() -> Self {
        Self::with_capacity(PREPARED_INSERT_CACHE_CAPACITY)
    }
}

impl PreparedInsertCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get_or_prepare<F>(
        &mut self,
        sql: &str,
        schema_cookie: u32,
        temp_schema_cookie: u32,
        build: F,
    ) -> Result<Option<Arc<PreparedSimpleInsert>>>
    where
        F: FnOnce() -> Result<Option<PreparedSimpleInsert>>,
    {
        let key = PreparedInsertKey {
            schema_cookie,
            temp_schema_cookie,
            sql: sql.to_string(),
        };
        if let Some(plan) = self.entries.get(&key) {
            return Ok(Some(Arc::clone(plan)));
        }

        let Some(plan) = build()? else {
            return Ok(None);
        };
        let plan = Arc::new(plan);
        if self.capacity == 0 {
            return Ok(Some(plan));
        }

        while self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }

        self.order.push_back(key.clone());
        self.entries.insert(key, Arc::clone(&plan));
        Ok(Some(plan))
    }
}

impl Db {
    /// Reads the raw database header without opening the entire database engine
    /// or validating the format version. This is useful for inspection utilities
    /// and pre-flight format validation.
    pub fn read_header_info(path: impl AsRef<Path>) -> Result<HeaderInfo> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path);
        let file = vfs.open(path, OpenMode::OpenExisting, FileKind::Database)?;
        let header = storage::read_database_header_vfs_loose(file.as_ref())?;
        Ok(HeaderInfo {
            magic_hex: hex_encode(&header.magic),
            format_version: header.format_version,
            page_size: header.page_size,
            header_checksum: header.header_checksum,
            schema_cookie: header.schema_cookie,
            catalog_root_page_id: header.catalog_root_page_id,
            freelist_root_page_id: header.freelist.root_page_id,
            freelist_head_page_id: header.freelist.head_page_id,
            freelist_page_count: header.freelist.page_count,
            last_checkpoint_lsn: header.last_checkpoint_lsn,
        })
    }

    /// Reads the raw database header using the supplied configuration.
    ///
    /// This variant can inspect encrypted databases when `config.encryption`
    /// contains the correct key.
    pub fn read_header_info_with_config(
        path: impl AsRef<Path>,
        config: &DbConfig,
    ) -> Result<HeaderInfo> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path).with_config(config);
        let file = vfs.open(path, OpenMode::OpenExisting, FileKind::Database)?;
        let header = storage::read_database_header_vfs_loose(file.as_ref())?;
        Ok(HeaderInfo {
            magic_hex: hex_encode(&header.magic),
            format_version: header.format_version,
            page_size: header.page_size,
            header_checksum: header.header_checksum,
            schema_cookie: header.schema_cookie,
            catalog_root_page_id: header.catalog_root_page_id,
            freelist_root_page_id: header.freelist.root_page_id,
            freelist_head_page_id: header.freelist.head_page_id,
            freelist_page_count: header.freelist.page_count,
            last_checkpoint_lsn: header.last_checkpoint_lsn,
        })
    }

    /// Begins an exclusive SQL transaction handle that reserves mutable runtime
    /// state until commit or rollback.
    pub fn transaction(&self) -> Result<SqlTransaction<'_>> {
        let state = self.build_exclusive_sql_txn_state()?;
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        if !matches!(*txn, SqlTxnSlot::None) {
            return Err(DbError::transaction(
                "SQL transaction is already active on this handle",
            ));
        }
        *txn = SqlTxnSlot::Exclusive;
        self.inner.sql_txn_active.store(true, Ordering::Release);
        Ok(SqlTransaction {
            db: self,
            state: Some(state),
        })
    }

    /// Creates a brand new database file with an initialized page-1 header and
    /// reserved catalog root page.
    pub fn create(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path);
        Self::create_with_vfs(path, config, vfs)
    }

    pub(crate) fn create_with_vfs(
        path: impl AsRef<Path>,
        config: DbConfig,
        vfs: VfsHandle,
    ) -> Result<Self> {
        let path = path.as_ref();
        config.validate_for_create()?;
        let coordination_vfs = vfs.clone();
        let vfs = vfs.with_config(&config);
        let open_mode = if vfs.is_memory() {
            OpenMode::OpenOrCreate
        } else {
            OpenMode::CreateNew
        };
        let file = vfs.open(path, open_mode, FileKind::Database)?;
        let header = DatabaseHeader::new(config.page_size);
        storage::write_database_bootstrap_vfs(file.as_ref(), &header)?;
        file.sync_metadata()?;

        Self::open_with_vfs(path.to_path_buf(), config, vfs, coordination_vfs)
    }

    /// Opens an existing database file and validates its fixed header.
    pub fn open(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path);
        Self::open_existing_with_vfs(path, config, vfs)
    }

    pub(crate) fn open_existing_with_vfs(
        path: impl AsRef<Path>,
        config: DbConfig,
        vfs: VfsHandle,
    ) -> Result<Self> {
        let path = path.as_ref();
        let coordination_vfs = vfs.clone();
        let vfs = vfs.with_config(&config);
        let mode = if vfs.is_memory() {
            OpenMode::OpenOrCreate
        } else {
            OpenMode::OpenExisting
        };
        let file = vfs.open(path, mode, FileKind::Database)?;
        if vfs.is_memory() && file.file_size()? == 0 {
            let header = DatabaseHeader::new(config.page_size);
            storage::write_database_bootstrap_vfs(file.as_ref(), &header)?;
            file.sync_metadata()?;
        }
        Self::open_with_vfs(path.to_path_buf(), config, vfs, coordination_vfs)
    }

    /// Opens an existing database or creates a new one when the path does not
    /// yet exist.
    pub fn open_or_create(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path);
        Self::open_or_create_with_vfs(path, config, vfs)
    }

    pub(crate) fn open_or_create_with_vfs(
        path: impl AsRef<Path>,
        config: DbConfig,
        vfs: VfsHandle,
    ) -> Result<Self> {
        let path = path.as_ref();
        if vfs.is_memory() || vfs.file_exists(path)? {
            Self::open_existing_with_vfs(path, config, vfs)
        } else {
            Self::create_with_vfs(path, config, vfs)
        }
    }

    /// Begins an explicit SQL transaction on this database handle.
    pub fn begin_transaction(&self) -> Result<()> {
        let state = self.build_sql_txn_state()?;
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        if !matches!(*txn, SqlTxnSlot::None) {
            return Err(DbError::transaction(
                "SQL transaction is already active on this handle",
            ));
        }
        *txn = SqlTxnSlot::Shared(Box::new(state));
        self.inner.sql_txn_active.store(true, Ordering::Release);
        Ok(())
    }

    /// Commits the current explicit SQL transaction.
    pub fn commit_transaction(&self) -> Result<u64> {
        let state = {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match std::mem::replace(&mut *txn, SqlTxnSlot::None) {
                SqlTxnSlot::Shared(state) => {
                    self.inner.sql_txn_active.store(false, Ordering::Release);
                    *state
                }
                SqlTxnSlot::Exclusive => {
                    *txn = SqlTxnSlot::Exclusive;
                    self.inner.sql_txn_active.store(true, Ordering::Release);
                    return Err(self.exclusive_sql_txn_error());
                }
                SqlTxnSlot::None => {
                    self.inner.sql_txn_active.store(false, Ordering::Release);
                    return Err(DbError::transaction("no active SQL transaction to commit"));
                }
            }
        };
        if !state.persistent_changed {
            self.install_temp_runtime(state.runtime)?;
            return Ok(state.base_lsn);
        }
        self.persist_runtime_if_latest(
            state.runtime,
            Some((state.base_lsn, state.base_checkpoint_epoch)),
            state.indexes_maybe_stale,
        )
    }

    /// Rolls back the current explicit SQL transaction.
    pub fn rollback_transaction(&self) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        match *txn {
            SqlTxnSlot::Shared(_) => {
                *txn = SqlTxnSlot::None;
                self.inner.sql_txn_active.store(false, Ordering::Release);
                Ok(())
            }
            SqlTxnSlot::Exclusive => Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => Err(DbError::transaction(
                "no active SQL transaction to roll back",
            )),
        }
    }

    /// Returns whether this handle currently has an explicit SQL transaction.
    pub fn in_transaction(&self) -> Result<bool> {
        if !self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.inner
            .sql_txn
            .lock()
            .map(|txn| !matches!(*txn, SqlTxnSlot::None))
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))
    }

    /// Creates a named savepoint inside the current explicit SQL transaction.
    pub fn create_savepoint(&self, name: &str) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = match &mut *txn {
            SqlTxnSlot::Shared(state) => state,
            SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => {
                return Err(DbError::transaction(
                    "SAVEPOINT requires an active SQL transaction",
                ));
            }
        };
        state.savepoints.push(SqlSavepoint {
            name: canonical_savepoint_name(name),
            runtime: state.runtime.clone(),
            persistent_changed: state.persistent_changed,
            indexes_maybe_stale: state.indexes_maybe_stale,
            prepared_insert_runtime_cache: state.prepared_insert_runtime_cache.clone(),
        });
        Ok(())
    }

    /// Releases a named savepoint and any nested savepoints created after it.
    pub fn release_savepoint(&self, name: &str) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = match &mut *txn {
            SqlTxnSlot::Shared(state) => state,
            SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => {
                return Err(DbError::transaction(
                    "RELEASE SAVEPOINT requires an active SQL transaction",
                ));
            }
        };
        let target = canonical_savepoint_name(name);
        let index = state
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == target)
            .ok_or_else(|| DbError::transaction(format!("savepoint {name} does not exist")))?;
        state.savepoints.truncate(index);
        Ok(())
    }

    /// Rolls the current explicit SQL transaction back to a named savepoint.
    pub fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = match &mut *txn {
            SqlTxnSlot::Shared(state) => state,
            SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => {
                return Err(DbError::transaction(
                    "ROLLBACK TO SAVEPOINT requires an active SQL transaction",
                ));
            }
        };
        let target = canonical_savepoint_name(name);
        let index = state
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == target)
            .ok_or_else(|| DbError::transaction(format!("savepoint {name} does not exist")))?;
        state.runtime = state.savepoints[index].runtime.clone();
        state.persistent_changed = state.savepoints[index].persistent_changed;
        state.indexes_maybe_stale = state.savepoints[index].indexes_maybe_stale;
        state.prepared_insert_runtime_cache = state.savepoints[index]
            .prepared_insert_runtime_cache
            .clone();
        state.savepoints.truncate(index + 1);
        Ok(())
    }

    /// Returns a structured snapshot of the current storage state.
    pub fn storage_info(&self) -> Result<StorageInfo> {
        let header = self.inner.pager.header_snapshot()?;
        Ok(StorageInfo {
            path: self.path().to_path_buf(),
            wal_path: self.inner.wal.file_path().to_path_buf(),
            format_version: header.format_version,
            page_size: self.inner.config.page_size,
            cache_size_mb: self.inner.config.cache_size_mb,
            page_count: self.inner.pager.on_disk_page_count()?,
            schema_cookie: header.schema_cookie,
            wal_end_lsn: self.inner.wal.latest_snapshot(),
            wal_file_size: self.inner.wal.file_size()?,
            last_checkpoint_lsn: header.last_checkpoint_lsn,
            active_readers: self.inner.wal.active_reader_count()?,
            wal_versions: self.inner.wal.version_count()?,
            warning_count: self.inner.wal.warnings()?.len(),
            shared_wal: self.inner.wal.is_shared(),
        })
    }

    /// Returns the decoded page-1 database header fields.
    pub fn header_info(&self) -> Result<HeaderInfo> {
        let header = self.inner.pager.header_snapshot()?;
        Ok(HeaderInfo {
            magic_hex: hex_encode(&header.magic),
            format_version: header.format_version,
            page_size: header.page_size,
            header_checksum: header.header_checksum,
            schema_cookie: header.schema_cookie,
            catalog_root_page_id: header.catalog_root_page_id,
            freelist_root_page_id: header.freelist.root_page_id,
            freelist_head_page_id: header.freelist.head_page_id,
            freelist_page_count: header.freelist.page_count,
            last_checkpoint_lsn: header.last_checkpoint_lsn,
        })
    }

    /// Writes a checkpointed snapshot of the database into a new destination file.
    pub fn save_as(&self, dest: impl AsRef<Path>) -> Result<()> {
        let dest = dest.as_ref();
        if is_memory_path(dest) {
            return Err(DbError::transaction(
                "save_as destination must be an on-disk path",
            ));
        }

        // `save_as` only needs a WAL checkpoint when the live WAL has frames
        // to fold into the main database file. After a successful checkpoint
        // this handle's logical WAL end is reset to 0 even though the database
        // header may retain the last folded checkpoint LSN.
        let mut latest_snapshot = self.inner.wal.latest_snapshot();
        if latest_snapshot == 0 {
            if let Some(coordination_snapshot) = self.inner.wal.process_coordination_snapshot()? {
                if coordination_snapshot.wal_end_lsn != latest_snapshot {
                    self.inner
                        .wal
                        .refresh_from_coordination(&self.inner.pager)?;
                    latest_snapshot = self.inner.wal.latest_snapshot();
                }
            }
        }
        if latest_snapshot != 0 {
            self.checkpoint_wal()?;
            latest_snapshot = self.inner.wal.latest_snapshot();
        }

        let vfs = VfsHandle::for_path(dest).with_config(&self.inner.config);
        if vfs.file_exists(dest)? {
            return Err(DbError::io(
                format!("destination {} already exists", dest.display()),
                std::io::Error::new(std::io::ErrorKind::AlreadyExists, "destination exists"),
            ));
        }
        if latest_snapshot == 0 && self.try_save_as_checkpointed_file_copy(dest, &vfs)? {
            return Ok(());
        }

        let file = vfs.open(dest, OpenMode::CreateNew, FileKind::Database)?;
        let page_size = self.inner.config.page_size;
        let page_count = self.inner.pager.on_disk_page_count()?;
        for page_id in 1..=page_count {
            let page = self.read_page(page_id)?;
            write_all_at(file.as_ref(), page::page_offset(page_id, page_size), &page)?;
        }
        file.set_len(page::page_offset(page_count.saturating_add(1), page_size))?;
        file.sync_metadata()?;
        Ok(())
    }

    fn try_save_as_checkpointed_file_copy(
        &self,
        dest: &Path,
        dest_vfs: &VfsHandle,
    ) -> Result<bool> {
        if is_memory_path(self.path()) || dest_vfs.is_memory() {
            return Ok(false);
        }
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if self.inner.config.encryption.is_none() && self.try_save_as_os_file_copy(dest)? {
            return Ok(true);
        }

        let source_vfs = VfsHandle::for_path(self.path()).with_config(&self.inner.config);
        if source_vfs.is_memory() {
            return Ok(false);
        }

        let source = source_vfs.open(self.path(), OpenMode::OpenExisting, FileKind::Database)?;
        let dest_file = dest_vfs.open(dest, OpenMode::CreateNew, FileKind::Database)?;
        source.advise_sequential()?;
        dest_file.advise_sequential()?;
        let len = source.file_size()?;
        Self::copy_vfs_file(source.as_ref(), dest_file.as_ref(), len)?;
        dest_file.set_len(len)?;
        dest_file.sync_metadata()?;
        Ok(true)
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn try_save_as_os_file_copy(&self, dest: &Path) -> Result<bool> {
        let mut source = std::fs::File::open(self.path()).map_err(|source| {
            DbError::io(
                format!("open source database {}", self.path().display()),
                source,
            )
        })?;
        let mut dest_file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dest)
            .map_err(|source| DbError::io(format!("create snapshot {}", dest.display()), source))?;

        let result = std::io::copy(&mut source, &mut dest_file)
            .and_then(|_| dest_file.sync_all())
            .map_err(|source| DbError::io(format!("copy snapshot {}", dest.display()), source));
        if let Err(error) = result {
            let _ = std::fs::remove_file(dest);
            return Err(error);
        }
        Ok(true)
    }

    fn copy_vfs_file(source: &dyn VfsFile, dest: &dyn VfsFile, len: u64) -> Result<()> {
        const COPY_CHUNK_BYTES: usize = 1024 * 1024;

        let mut buffer = vec![0_u8; COPY_CHUNK_BYTES];
        let mut offset = 0_u64;
        while offset < len {
            let chunk_len = (len - offset).min(COPY_CHUNK_BYTES as u64) as usize;
            read_exact_at(source, offset, &mut buffer[..chunk_len])?;
            write_all_at(dest, offset, &buffer[..chunk_len])?;
            offset += chunk_len as u64;
        }
        Ok(())
    }

    /// Begins a single-connection write transaction.
    pub fn begin_write(&self) -> Result<()> {
        let snapshot_reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        self.refresh_pager_after_checkpoint()?;
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if txn.active {
            return Err(DbError::transaction("write transaction is already active"));
        }
        txn.active = true;
        self.inner.write_txn_active.store(true, Ordering::Release);
        txn.staged_pages.clear();
        txn.snapshot_reader = Some(snapshot_reader);
        Ok(())
    }

    fn refresh_pager_after_checkpoint(&self) -> Result<()> {
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        if latest_checkpoint_epoch == last_seen_checkpoint_epoch {
            return Ok(());
        }

        let cached_header = self.inner.pager.header_snapshot()?;
        let on_disk_header = self.inner.pager.header_from_disk()?;
        if on_disk_header.last_checkpoint_lsn != cached_header.last_checkpoint_lsn {
            self.inner.pager.refresh_from_disk(on_disk_header)?;
        }
        self.inner
            .last_seen_checkpoint_epoch
            .store(latest_checkpoint_epoch, Ordering::Release);
        Ok(())
    }

    /// Stages a full-page image inside the current write transaction.
    pub fn write_page(&self, page_id: u32, data: &[u8]) -> Result<()> {
        page::validate_page_id(page_id)?;
        if data.len() != self.inner.config.page_size as usize {
            return Err(DbError::internal(format!(
                "page {page_id} write length {} does not match configured page size {}",
                data.len(),
                self.inner.config.page_size
            )));
        }
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "write_page requires an active write transaction",
            ));
        }
        txn.staged_pages.insert(page_id, data.to_vec());
        Ok(())
    }

    pub(crate) fn write_page_owned(&self, page_id: u32, data: Vec<u8>) -> Result<()> {
        page::validate_page_id(page_id)?;
        if data.len() != self.inner.config.page_size as usize {
            return Err(DbError::internal(format!(
                "page {page_id} write length {} does not match configured page size {}",
                data.len(),
                self.inner.config.page_size
            )));
        }
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "write_page requires an active write transaction",
            ));
        }
        txn.staged_pages.insert(page_id, data);
        Ok(())
    }

    /// Allocates a new page from the freelist or file tail.
    pub fn allocate_page(&self) -> Result<u32> {
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "allocate_page requires an active write transaction",
            ));
        }

        let mut header = self.write_txn_header(&txn)?;
        if header.freelist.head_page_id != 0 {
            let page_id = header.freelist.head_page_id;
            let freelist_page = self.write_txn_visible_page(&txn, page_id)?;
            let next = decode_freelist_next(&freelist_page)?;
            header.freelist.head_page_id = next;
            header.freelist.page_count = header.freelist.page_count.saturating_sub(1);
            self.stage_write_txn_header(&mut txn, &header);
            txn.staged_pages
                .insert(page_id, page::zeroed_page(self.inner.config.page_size));
            return Ok(page_id);
        }

        // `staged_pages` is a BTreeMap keyed by PageId, so its last entry is
        // the largest staged page id in O(log n). Using `keys().max()` here
        // was O(n) per allocation and made this function O(n^2) across a
        // large transaction (e.g. bulk seeding), dominating CPU time for
        // single-transaction multi-million-row inserts.
        let max_staged_page_id = txn.staged_pages.keys().next_back().copied().unwrap_or(0);
        let next_page_id = self
            .inner
            .pager
            .on_disk_page_count()?
            .max(self.inner.wal.max_page_count())
            .max(max_staged_page_id)
            .saturating_add(1);
        txn.staged_pages
            .entry(next_page_id)
            .or_insert_with(|| page::zeroed_page(self.inner.config.page_size));
        Ok(next_page_id)
    }

    /// Frees an existing page back to the freelist.
    pub fn free_page(&self, page_id: u32) -> Result<()> {
        page::validate_page_id(page_id)?;
        if page_id <= page::CATALOG_ROOT_PAGE_ID {
            return Err(DbError::transaction(format!(
                "page {page_id} is reserved and cannot be freed"
            )));
        }
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "free_page requires an active write transaction",
            ));
        }

        let mut header = self.write_txn_header(&txn)?;
        let page_bytes =
            encode_freelist_page(self.inner.config.page_size, header.freelist.head_page_id);
        txn.staged_pages.insert(page_id, page_bytes);
        header.freelist.head_page_id = page_id;
        header.freelist.page_count = header.freelist.page_count.saturating_add(1);
        self.stage_write_txn_header(&mut txn, &header);
        Ok(())
    }

    /// Commits the current write transaction to the WAL.
    pub fn commit(&self) -> Result<u64> {
        let (max_page_id, pages) = {
            let mut txn = self
                .inner
                .write_txn
                .lock()
                .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
            if !txn.active {
                return Err(DbError::transaction(
                    "no active write transaction to commit",
                ));
            }
            txn.active = false;
            txn.snapshot_reader = None;
            let max_page_id = txn
                .staged_pages
                .last_key_value()
                .map_or(0, |(page_id, _)| *page_id);
            (max_page_id, std::mem::take(&mut txn.staged_pages))
        };
        self.inner.write_txn_active.store(false, Ordering::Release);

        let pages: Vec<_> = pages.into_iter().collect();
        let max_page_count = self.inner.wal.max_page_count().max(max_page_id);
        self.inner
            .wal
            .commit_pages(&self.inner.pager, pages, max_page_count)
    }

    fn commit_if_latest(
        &self,
        expected_latest_lsn: u64,
        expected_checkpoint_epoch: u64,
    ) -> Result<u64> {
        let (max_page_id, pages) = {
            let mut txn = self
                .inner
                .write_txn
                .lock()
                .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
            if !txn.active {
                return Err(DbError::transaction(
                    "no active write transaction to commit",
                ));
            }
            txn.active = false;
            txn.snapshot_reader = None;
            let max_page_id = txn
                .staged_pages
                .last_key_value()
                .map_or(0, |(page_id, _)| *page_id);
            (max_page_id, std::mem::take(&mut txn.staged_pages))
        };
        self.inner.write_txn_active.store(false, Ordering::Release);

        let pages: Vec<_> = pages.into_iter().collect();
        let max_page_count = self.inner.wal.max_page_count().max(max_page_id);
        self.inner.wal.commit_pages_if_latest(
            &self.inner.pager,
            pages,
            max_page_count,
            expected_latest_lsn,
            expected_checkpoint_epoch,
        )
    }

    /// Rolls back the current write transaction.
    pub fn rollback(&self) -> Result<()> {
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        txn.active = false;
        txn.staged_pages.clear();
        txn.snapshot_reader = None;
        self.inner.write_txn_active.store(false, Ordering::Release);
        Ok(())
    }

    pub(crate) fn read_page_in_write_txn(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        page::validate_page_id(page_id)?;
        let snapshot_lsn = {
            let txn = self
                .inner
                .write_txn
                .lock()
                .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
            if !txn.active {
                return Err(DbError::transaction(
                    "read_page_in_write_txn requires an active write transaction",
                ));
            }
            if let Some(staged) = txn.staged_pages.get(&page_id).cloned() {
                return Ok(Arc::from(staged));
            }
            txn.snapshot_reader
                .as_ref()
                .ok_or_else(|| DbError::transaction("write transaction has no read snapshot"))?
                .snapshot_lsn()
        };
        if let Some(wal_page) =
            self.inner
                .wal
                .read_page_at_snapshot(&self.inner.pager, page_id, snapshot_lsn)?
        {
            return Ok(wal_page);
        }
        self.inner.pager.read_page(page_id)
    }

    fn write_txn_visible_page(&self, txn: &WriteTxn, page_id: PageId) -> Result<Arc<[u8]>> {
        if let Some(staged) = txn.staged_pages.get(&page_id).cloned() {
            return Ok(Arc::from(staged));
        }

        let snapshot_lsn = txn
            .snapshot_reader
            .as_ref()
            .ok_or_else(|| DbError::transaction("write transaction has no read snapshot"))?
            .snapshot_lsn();
        if let Some(wal_page) =
            self.inner
                .wal
                .read_page_at_snapshot(&self.inner.pager, page_id, snapshot_lsn)?
        {
            return Ok(wal_page);
        }
        self.inner.pager.read_page(page_id)
    }

    fn write_txn_header(&self, txn: &WriteTxn) -> Result<DatabaseHeader> {
        let page = self.write_txn_visible_page(txn, page::HEADER_PAGE_ID)?;
        let mut bytes = [0_u8; storage::header::DB_HEADER_SIZE];
        bytes.copy_from_slice(&page[..storage::header::DB_HEADER_SIZE]);
        DatabaseHeader::decode(&bytes)
    }

    fn stage_write_txn_header(&self, txn: &mut WriteTxn, header: &DatabaseHeader) {
        let mut page = page::zeroed_page(self.inner.config.page_size);
        page[..storage::header::DB_HEADER_SIZE].copy_from_slice(&header.encode());
        txn.staged_pages.insert(page::HEADER_PAGE_ID, page);
    }

    /// Reads the latest visible version of a page.
    pub fn read_page(&self, page_id: u32) -> Result<Arc<[u8]>> {
        #[cfg(feature = "bench-internals")]
        READ_PATH_WAL_READER_BEGIN_COUNT.fetch_add(1, Ordering::Relaxed);
        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        self.read_page_at_snapshot_lsn(page_id, reader.snapshot_lsn())
    }

    pub(crate) fn advise_sequential(&self) -> Result<()> {
        self.inner.pager.advise_sequential()
    }

    pub(crate) fn read_page_at_snapshot_lsn(
        &self,
        page_id: u32,
        snapshot_lsn: u64,
    ) -> Result<Arc<[u8]>> {
        page::validate_page_id(page_id)?;
        if self.inner.write_txn_active.load(Ordering::Acquire) {
            #[cfg(feature = "bench-internals")]
            READ_PATH_WRITE_TXN_LOCK_COUNT.fetch_add(1, Ordering::Relaxed);
            let txn = self
                .inner
                .write_txn
                .lock()
                .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
            if let Some(staged) = txn.staged_pages.get(&page_id).cloned() {
                return Ok(Arc::from(staged));
            }
        }

        if let Some(wal_page) =
            self.inner
                .wal
                .read_page_at_snapshot(&self.inner.pager, page_id, snapshot_lsn)?
        {
            return Ok(wal_page);
        }
        self.inner.pager.read_page(page_id)
    }

    /// Performs a reader-aware checkpoint.
    pub fn checkpoint(&self) -> Result<()> {
        self.compact_persisted_payloads_before_checkpoint()?;
        self.checkpoint_wal()
    }

    /// Flushes committed WAL frames into the database file without running the
    /// optional pre-checkpoint payload compaction pass.
    pub fn checkpoint_wal(&self) -> Result<()> {
        let checkpoint_epoch_before = self.inner.wal.checkpoint_epoch();
        self.inner
            .wal
            .checkpoint(&self.inner.pager, self.inner.config.checkpoint_timeout_sec)?;
        let checkpoint_epoch_after = self.inner.wal.checkpoint_epoch();
        if checkpoint_epoch_after != checkpoint_epoch_before {
            self.inner
                .last_explicit_checkpoint_epoch
                .store(checkpoint_epoch_after, Ordering::Release);
        }
        Ok(())
    }

    /// Blocks until every commit acknowledged before this call is durable on
    /// disk.
    ///
    /// For the default [`crate::WalSyncMode::Full`] mode (and `Normal`), every
    /// commit is already synchronously durable when it returns, so this is a
    /// cheap no-op. Under [`crate::WalSyncMode::AsyncCommit`] it forces the
    /// background flusher to run and waits until the WAL is on stable storage.
    ///
    /// See `design/adr/0135-async-commit-wal-group-commit.md`.
    pub fn sync(&self) -> Result<()> {
        self.inner.wal.flush_to_durable()
    }

    /// Executes a single SQL statement without parameters.
    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        self.execute_with_params(sql, &[])
    }

    /// Executes a single SQL statement with positional `$n` parameters.
    pub fn execute_with_params(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        if let Some(trimmed) = simple_single_statement_fast_path_sql(sql) {
            if let Some(result) = self.try_execute_simple_count_sql_fast_path(trimmed, params)? {
                self.record_statement_trace(
                    trimmed,
                    true,
                    std::time::Duration::ZERO,
                    0,
                    Ok(&result),
                );
                return Ok(result);
            }
            if let Some(result) =
                self.try_execute_simple_grouped_count_sql_fast_path(trimmed, params)?
            {
                self.record_statement_trace(
                    trimmed,
                    true,
                    std::time::Duration::ZERO,
                    0,
                    Ok(&result),
                );
                return Ok(result);
            }
            if let Some(result) =
                self.try_execute_simple_row_id_projection_sql_fast_path(trimmed, params)?
            {
                self.record_statement_trace(
                    trimmed,
                    true,
                    std::time::Duration::ZERO,
                    0,
                    Ok(&result),
                );
                return Ok(result);
            }
        }

        let mut results = self.execute_batch_with_params(sql, params)?;
        if results.len() != 1 {
            return Err(DbError::sql(format!(
                "expected exactly one SQL statement, got {}",
                results.len()
            )));
        }
        Ok(results.remove(0))
    }

    /// Executes one or more semicolon-delimited SQL statements.
    pub fn execute_batch(&self, sql: &str) -> Result<Vec<QueryResult>> {
        self.execute_batch_with_params(sql, &[])
    }

    /// Executes one SQL statement through the engine-owned write queue.
    ///
    /// The queued path preserves the existing single-writer model while
    /// centralizing backpressure, timeout, cancellation-before-run, and strict
    /// group-commit behavior. Explicit `BEGIN`, `COMMIT`, `ROLLBACK`, and
    /// savepoint control statements are intentionally rejected on the queued
    /// path in this first contract; callers should use the direct transaction
    /// APIs for long-lived explicit transactions.
    pub fn execute_queued(&self, sql: &str) -> Result<QueryResult> {
        self.execute_queued_with_params(sql, &[])
    }

    /// Executes one SQL statement with positional `$n` parameters through the
    /// engine-owned write queue.
    pub fn execute_queued_with_params(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let mut results = self.execute_queued_batch_with_params(sql, params)?;
        if results.len() != 1 {
            return Err(DbError::sql(format!(
                "expected exactly one SQL statement, got {}",
                results.len()
            )));
        }
        Ok(results.remove(0))
    }

    /// Executes one or more SQL statements through the engine-owned write
    /// queue using configured default timeout behavior.
    pub fn execute_queued_batch(&self, sql: &str) -> Result<Vec<QueryResult>> {
        self.execute_queued_batch_with_params(sql, &[])
    }

    /// Executes one or more SQL statements with positional `$n` parameters
    /// through the engine-owned write queue using configured default timeout
    /// behavior.
    pub fn execute_queued_batch_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<QueryResult>> {
        self.execute_queued_batch_with_options(sql, params, QueuedWriteOptions::default())
    }

    /// Executes one or more SQL statements through the write queue with
    /// per-call timeout and cancellation options.
    pub fn execute_queued_batch_with_options(
        &self,
        sql: &str,
        params: &[Value],
        mut options: QueuedWriteOptions,
    ) -> Result<Vec<QueryResult>> {
        self.reject_transaction_control_for_queued_sql(sql)?;
        if options.timeout.is_none() {
            let timeout_ms = self.inner.busy_timeout_ms.load(Ordering::Acquire);
            if timeout_ms > 0 {
                options.timeout = Some(Duration::from_millis(timeout_ms));
            }
        }
        self.write_queue()
            .execute_batch_with_params(self, sql, params, options)
    }

    /// Returns a snapshot of current write-queue counters. Calling this method
    /// initializes the lazy queue metadata but does not route direct writes
    /// through the queue.
    #[must_use]
    pub fn write_queue_metrics(&self) -> WriteQueueMetricsSnapshot {
        self.write_queue().snapshot()
    }

    /// Subscribes to committed changes for one or more persistent user tables.
    pub fn watch_table(&self, options: TableWatchOptions) -> Result<WatchHandle> {
        let tables = self.validate_watch_tables(&options.tables)?;
        self.reactive_hub().watch_table(
            tables,
            options.queue_capacity,
            self.inner.wal.latest_snapshot(),
            self.schema_cookie()?,
        )
    }

    /// Subscribes to committed changes intersecting a primary-key range.
    pub fn watch_range(&self, mut options: RangeWatchOptions) -> Result<WatchHandle> {
        let canonical = self.validate_watch_range_table(&options.table)?;
        options.table = canonical;
        self.reactive_hub().watch_range(
            options,
            self.inner.wal.latest_snapshot(),
            self.schema_cookie()?,
        )
    }

    /// Executes a SELECT and subscribes to invalidations for its dependencies.
    pub fn watch_query(
        &self,
        sql: &str,
        params: &[Value],
        options: QueryWatchOptions,
    ) -> Result<WatchHandle> {
        let statement = self.parsed_statement(sql)?;
        if !statement_is_read_only(&statement) {
            return Err(DbError::sql(
                "query subscriptions require a read-only SELECT",
            ));
        }
        let dependencies = self.query_watch_dependencies(&statement)?;
        let result = self.execute_with_params(sql, params)?;
        self.reactive_hub().watch_query(
            dependencies,
            options.queue_capacity,
            self.inner.wal.latest_snapshot(),
            self.schema_cookie()?,
            result,
        )
    }

    /// Subscribes to ordered committed change events.
    pub fn change_stream(&self, options: ChangeStreamOptions) -> Result<WatchHandle> {
        let tables = if options.tables.is_empty() {
            None
        } else {
            Some(self.validate_watch_tables(&options.tables)?)
        };
        self.reactive_hub().change_stream(
            tables,
            options.queue_capacity,
            self.inner.wal.latest_snapshot(),
            self.schema_cookie()?,
        )
    }

    /// Returns current reactive subscription counters.
    #[must_use]
    pub fn reactive_metrics(&self) -> ReactiveMetricsSnapshot {
        self.reactive_hub_if_initialized()
            .map_or_else(ReactiveMetricsSnapshot::default, |hub| {
                hub.metrics_snapshot()
            })
    }

    /// Returns current reactive subscription details.
    #[must_use]
    pub fn reactive_subscriptions(&self) -> Vec<ReactiveSubscriptionSnapshot> {
        self.reactive_hub_if_initialized()
            .map_or_else(Vec::new, |hub| hub.subscription_snapshots())
    }

    /// Executes one or more read-only SQL statements against a retained WAL LSN.
    pub fn execute_batch_at_snapshot_lsn(
        &self,
        sql: &str,
        snapshot_lsn: u64,
    ) -> Result<Vec<QueryResult>> {
        self.execute_batch_at_snapshot_lsn_with_params(sql, snapshot_lsn, &[])
    }

    /// Executes one or more read-only SQL statements with `$n` parameters against a retained WAL LSN.
    pub fn execute_batch_at_snapshot_lsn_with_params(
        &self,
        sql: &str,
        snapshot_lsn: u64,
        params: &[Value],
    ) -> Result<Vec<QueryResult>> {
        let latest_lsn = self.inner.wal.latest_snapshot();
        if snapshot_lsn > latest_lsn {
            return Err(DbError::transaction(format!(
                "snapshot LSN {snapshot_lsn} is newer than WAL end LSN {latest_lsn}"
            )));
        }

        let mut statements = Vec::new();
        for statement_sql in split_sql_batch(sql) {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() {
                continue;
            }
            if parse_transaction_control(trimmed).is_some()
                || parse_pragma_command(trimmed)?.is_some()
            {
                return Err(DbError::transaction(
                    "time-travel execution only supports read-only SQL statements",
                ));
            }
            let statement = self.parsed_statement(trimmed)?;
            if !statement_is_read_only(&statement) {
                return Err(DbError::transaction(
                    "time-travel execution is read-only; mutating statements are not allowed",
                ));
            }
            statements.push(statement);
        }
        if statements.is_empty() {
            return Ok(Vec::new());
        }

        let schema_cookie = self.current_schema_cookie_at_snapshot(snapshot_lsn)?;
        let mut runtime = EngineRuntime::load_from_storage_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            schema_cookie,
            &self.inner.config,
            snapshot_lsn,
        )?;
        runtime.load_deferred_tables_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            self.inner.config.page_size,
            snapshot_lsn,
        )?;

        statements
            .iter()
            .map(|statement| {
                runtime.execute_read_statement(statement, params, self.inner.config.page_size)
            })
            .collect()
    }

    /// Executes SQL on a branch. Non-`main` branches are read-only until branch-local writes land.
    pub fn execute_batch_on_branch(
        &self,
        sql: &str,
        branch_name: &str,
    ) -> Result<Vec<QueryResult>> {
        self.execute_batch_on_branch_with_params(sql, branch_name, &[])
    }

    /// Executes SQL with `$n` parameters on a branch.
    pub fn execute_batch_on_branch_with_params(
        &self,
        sql: &str,
        branch_name: &str,
        params: &[Value],
    ) -> Result<Vec<QueryResult>> {
        if branch_name == crate::branch::DEFAULT_BRANCH_NAME {
            return self.execute_batch_with_params(sql, params);
        }
        let branch = crate::branch::branch_by_name(self, branch_name)?
            .ok_or_else(|| DbError::transaction(format!("unknown branch '{branch_name}'")))?;
        let read_only = self.sql_batch_is_read_only(sql)?;
        let branch_db = self.materialize_branch_db(&branch)?;
        let results = branch_db.execute_batch_with_params(sql, params)?;
        if !read_only {
            let log_sql = if params.is_empty() {
                sql.to_string()
            } else {
                expand_sql_parameters_for_branch_log(sql, params)?
            };
            crate::branch::append_branch_sql_log(self, &branch, &log_sql)?;
            self.refresh_named_snapshot_retention()?;
        }
        Ok(results)
    }

    fn sql_batch_is_read_only(&self, sql: &str) -> Result<bool> {
        for statement_sql in split_sql_batch(sql) {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() {
                continue;
            }
            if parse_transaction_control(trimmed).is_some()
                || parse_pragma_command(trimmed)?.is_some()
            {
                return Ok(false);
            }
            let statement = self.parsed_statement(trimmed)?;
            if !statement_is_read_only(&statement) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn materialize_branch_db(&self, branch: &crate::branch::BranchInfo) -> Result<Db> {
        let head_id = branch
            .current_head_id
            .as_deref()
            .ok_or_else(|| DbError::internal("branch is missing a current head"))?;
        let branch_lsn = crate::branch::branch_head_lsn_by_id(self, head_id)?
            .ok_or_else(|| DbError::internal("branch current head is missing"))?;
        let dump = self.dump_sql_at_snapshot_lsn(branch_lsn)?;
        let branch_db = Db::open_or_create(":memory:", self.inner.config.clone())?;
        if !dump.trim().is_empty() {
            branch_db.execute_batch(&dump)?;
        }
        for entry in crate::branch::branch_sql_log_for_head(self, head_id)? {
            branch_db.execute_batch(&entry.sql)?;
        }
        Ok(branch_db)
    }

    fn materialize_branch_head_db(&self, head_id: &str) -> Result<Db> {
        let branch_lsn = crate::branch::branch_head_lsn_by_id(self, head_id)?
            .ok_or_else(|| DbError::transaction(format!("unknown branch head '{head_id}'")))?;
        let dump = self.dump_sql_at_snapshot_lsn(branch_lsn)?;
        let branch_db = self.materialize_dump_db(&dump)?;
        for entry in crate::branch::branch_sql_log_for_head(self, head_id)? {
            branch_db.execute_batch(&entry.sql)?;
        }
        Ok(branch_db)
    }

    fn materialize_snapshot_lsn_db(&self, snapshot_lsn: u64) -> Result<Db> {
        let dump = self.dump_sql_at_snapshot_lsn(snapshot_lsn)?;
        self.materialize_dump_db(&dump)
    }

    fn materialize_current_db(&self) -> Result<Db> {
        let dump = self.dump_sql()?;
        self.materialize_dump_db(&dump)
    }

    fn materialize_dump_db(&self, dump: &str) -> Result<Db> {
        let db = Db::open_or_create(":memory:", self.inner.config.clone())?;
        if !dump.trim().is_empty() {
            db.execute_batch(dump)?;
        }
        Ok(db)
    }

    fn materialize_ref_db(&self, reference: &str) -> Result<Db> {
        if reference == crate::branch::DEFAULT_BRANCH_NAME {
            return self.materialize_current_db();
        }
        if let Some(branch) = crate::branch::branch_by_name(self, reference)? {
            return self.materialize_branch_db(&branch);
        }
        if let Some(snapshot) = self.snapshot_get(reference)? {
            return self.materialize_snapshot_lsn_db(snapshot.snapshot_lsn);
        }
        if crate::branch::branch_head_by_id(self, reference)?.is_some() {
            return self.materialize_branch_head_db(reference);
        }
        Err(DbError::transaction(format!(
            "unknown branch, snapshot, or head '{reference}'"
        )))
    }

    fn resolve_branch_target_head(
        &self,
        reference: &str,
    ) -> Result<crate::branch::BranchHeadMetadata> {
        if reference == crate::branch::DEFAULT_BRANCH_NAME {
            return Err(DbError::transaction(
                "use a named snapshot, branch, or head ID as the restore target",
            ));
        }
        if let Some(branch) = crate::branch::branch_by_name(self, reference)? {
            let head_id = branch
                .current_head_id
                .as_deref()
                .ok_or_else(|| DbError::transaction(format!("branch '{reference}' has no head")))?;
            return crate::branch::branch_head_by_id(self, head_id)?
                .ok_or_else(|| DbError::corruption(format!("branch head '{head_id}' is missing")));
        }
        if let Some(snapshot) = self.snapshot_get(reference)? {
            return crate::branch::branch_head_by_id(self, &snapshot.head_id)?.ok_or_else(|| {
                DbError::corruption(format!(
                    "snapshot '{}' references missing head '{}'",
                    snapshot.name, snapshot.head_id
                ))
            });
        }
        if let Some(head) = crate::branch::branch_head_by_id(self, reference)? {
            return Ok(head);
        }
        Err(DbError::transaction(format!(
            "unknown branch, snapshot, or head '{reference}'"
        )))
    }

    /// Compares two refs (`main`, branch name, named snapshot, or branch head ID).
    pub fn branch_diff(
        &self,
        left_ref: &str,
        right_ref: &str,
    ) -> Result<crate::branch::BranchDiffReport> {
        let left_db = self.materialize_ref_db(left_ref)?;
        let right_db = self.materialize_ref_db(right_ref)?;
        diff_materialized_refs(left_ref, right_ref, &left_db, &right_db)
    }

    /// Restores a non-main branch head to another branch, named snapshot, or head ID.
    pub fn branch_restore(
        &self,
        branch_name: &str,
        target_ref: &str,
        dry_run: bool,
    ) -> Result<crate::branch::BranchRestoreReport> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Err(DbError::transaction(
                "cannot restore a branch while a SQL transaction is active",
            ));
        }
        if branch_name == crate::branch::DEFAULT_BRANCH_NAME {
            return Err(DbError::transaction(
                "restore currently targets non-main branches; create a branch from the restore point to inspect main rollback candidates",
            ));
        }
        let branch = crate::branch::branch_by_name(self, branch_name)?
            .ok_or_else(|| DbError::transaction(format!("unknown branch '{branch_name}'")))?;
        let target_head = self.resolve_branch_target_head(target_ref)?;
        let diff = self.branch_diff(branch_name, target_ref)?;
        if dry_run {
            return Ok(crate::branch::BranchRestoreReport {
                branch: branch_name.to_string(),
                target_ref: target_ref.to_string(),
                dry_run: true,
                previous_head_id: branch.current_head_id,
                target_head_id: target_head.head_id,
                new_head_id: None,
                changed_table_count: diff.changed_table_count,
                added_row_count: diff.added_row_count,
                updated_row_count: diff.updated_row_count,
                deleted_row_count: diff.deleted_row_count,
            });
        }
        let new_head = crate::branch::restore_branch_head(self, &branch, &target_head, target_ref)?;
        self.refresh_named_snapshot_retention()?;
        Ok(crate::branch::BranchRestoreReport {
            branch: branch_name.to_string(),
            target_ref: target_ref.to_string(),
            dry_run: false,
            previous_head_id: branch.current_head_id,
            target_head_id: target_head.head_id,
            new_head_id: Some(new_head.head_id),
            changed_table_count: diff.changed_table_count,
            added_row_count: diff.added_row_count,
            updated_row_count: diff.updated_row_count,
            deleted_row_count: diff.deleted_row_count,
        })
    }

    /// Merges clean primary-key row changes from a source branch into a target ref.
    pub fn branch_merge(
        &self,
        source_branch: &str,
        target_ref: &str,
        dry_run: bool,
    ) -> Result<crate::branch::BranchMergeReport> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Err(DbError::transaction(
                "cannot merge a branch while a SQL transaction is active",
            ));
        }
        if source_branch == crate::branch::DEFAULT_BRANCH_NAME {
            return Err(DbError::transaction(
                "merge source must be a non-main branch",
            ));
        }
        let source = crate::branch::branch_by_name(self, source_branch)?
            .ok_or_else(|| DbError::transaction(format!("unknown branch '{source_branch}'")))?;
        let base_head_id = source.base_head_id.clone().ok_or_else(|| {
            DbError::transaction(format!("branch '{source_branch}' has no merge base"))
        })?;
        if target_ref != crate::branch::DEFAULT_BRANCH_NAME
            && crate::branch::branch_by_name(self, target_ref)?.is_none()
        {
            return Err(DbError::transaction(format!(
                "merge target must be 'main' or a branch; got '{target_ref}'"
            )));
        }

        let base_db = self.materialize_branch_head_db(&base_head_id)?;
        let source_db = self.materialize_branch_db(&source)?;
        let target_db = self.materialize_ref_db(target_ref)?;
        let plan = build_merge_plan(
            source_branch,
            target_ref,
            &base_head_id,
            &base_db,
            &source_db,
            &target_db,
        )?;
        if dry_run || !plan.conflicts.is_empty() {
            return Ok(plan.into_report(dry_run));
        }
        let sql = plan
            .changes
            .iter()
            .map(|change| change.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        if !sql.trim().is_empty() {
            if target_ref == crate::branch::DEFAULT_BRANCH_NAME {
                crate::reactive::with_change_source(ChangeSource::BranchMerge, || {
                    self.execute_batch(&sql)
                })?;
            } else {
                self.execute_batch_on_branch(&sql, target_ref)?;
            }
        }
        Ok(plan.into_report(false))
    }

    /// Executes one or more read-only SQL statements against a named snapshot.
    pub fn execute_batch_at_snapshot(
        &self,
        sql: &str,
        snapshot_name: &str,
    ) -> Result<Vec<QueryResult>> {
        let snapshot_lsn = self.snapshot_lsn_for_ref(snapshot_name)?.ok_or_else(|| {
            DbError::transaction(format!("unknown snapshot or branch head '{snapshot_name}'"))
        })?;
        self.execute_batch_at_snapshot_lsn(sql, snapshot_lsn)
    }

    /// Executes one or more semicolon-delimited SQL statements with `$n` parameters.
    pub fn execute_batch_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<QueryResult>> {
        let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
        self.execute_batch_direct_with_params(sql, params)
    }

    /// Reset a specific runtime trace store by name.
    ///
    /// `kind` may be "slow_queries", "lock_waits", or "index_usage".
    pub fn tracing_reset(&self, kind: &str) -> Result<()> {
        match kind {
            "slow_queries" => self
                .inner
                .tracing
                .slow_query_store
                .lock()
                .map_err(|_| DbError::internal("slow query store poisoned"))?
                .reset(),
            "lock_waits" => self
                .inner
                .tracing
                .lock_wait_store
                .lock()
                .map_err(|_| DbError::internal("lock wait store poisoned"))?
                .reset(),
            "index_usage" => self
                .inner
                .tracing
                .index_usage_store
                .lock()
                .map_err(|_| DbError::internal("index usage store poisoned"))?
                .reset(),
            _ => {
                return Err(DbError::sql(format!(
                    "unknown tracing kind for reset: {kind}"
                )))
            }
        }
        Ok(())
    }

    pub(crate) fn execute_batch_direct_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<QueryResult>> {
        if params.is_empty() && !self.inner.sql_txn_active.load(Ordering::Acquire) {
            if let Some(results) = self.try_execute_schema_batch_with_single_commit(sql)? {
                return Ok(results);
            }
        }

        let mut results = Vec::new();
        for statement_sql in split_sql_batch(sql) {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() {
                continue;
            }

            if let Some(control) = parse_transaction_control(trimmed) {
                match control {
                    TransactionControl::Begin => {
                        self.begin_transaction()?;
                        self.inner.tracing.mark_in_transaction();
                    }
                    TransactionControl::Commit => {
                        self.commit_transaction()?;
                        self.inner.tracing.mark_active();
                    }
                    TransactionControl::Rollback => {
                        self.rollback_transaction()?;
                        self.inner.tracing.mark_active();
                    }
                    TransactionControl::Savepoint(name) => self.create_savepoint(&name)?,
                    TransactionControl::ReleaseSavepoint(name) => {
                        self.release_savepoint(&name)?;
                    }
                    TransactionControl::RollbackToSavepoint(name) => {
                        self.rollback_to_savepoint(&name)?;
                    }
                }
                results.push(QueryResult::with_affected_rows(0));
                continue;
            }
            if let Some(pragma) = parse_pragma_command(trimmed)? {
                let result = self.execute_pragma_command(pragma)?;
                results.push(result);
                continue;
            }
            if let Some(command) = crate::security::parse_set_audit_context(trimmed)? {
                let result = self.execute_set_audit_context(command)?;
                // Per ADR 0192, audit context writes do not invalidate
                // the plan cache.
                results.push(result);
                continue;
            }
            if let Some(command) = crate::security::parse_security_command(trimmed)? {
                let result = self.execute_security_command(trimmed, command)?;
                crate::plan_cache::PlanCacheInvalidator::on_policy_mask_change(&*self.inner);
                results.push(result);
                continue;
            }
            if let Some(command) = crate::extensions::parse_extension_sql(trimmed)? {
                let result = crate::extensions::execute_extension_sql(self, command)?;
                crate::plan_cache::PlanCacheInvalidator::on_extension_change(&*self.inner);
                results.push(result);
                continue;
            }
            if let Some(result) = self.try_execute_sync_inspection_query(trimmed, params)? {
                results.push(result);
                continue;
            }
            if let Some(result) =
                crate::extensions::try_execute_extension_inspection_query(self, trimmed, params)?
            {
                results.push(result);
                continue;
            }
            if let Some(result) = self.try_execute_simple_count_sql_fast_path(trimmed, params)? {
                self.record_statement_trace(
                    trimmed,
                    true,
                    std::time::Duration::ZERO,
                    0,
                    Ok(&result),
                );
                results.push(result);
                continue;
            }
            if let Some(result) =
                self.try_execute_simple_grouped_count_sql_fast_path(trimmed, params)?
            {
                self.record_statement_trace(
                    trimmed,
                    true,
                    std::time::Duration::ZERO,
                    0,
                    Ok(&result),
                );
                results.push(result);
                continue;
            }
            if let Some(result) =
                self.try_execute_simple_row_id_projection_sql_fast_path(trimmed, params)?
            {
                self.record_statement_trace(
                    trimmed,
                    true,
                    std::time::Duration::ZERO,
                    0,
                    Ok(&result),
                );
                results.push(result);
                continue;
            }
            if !self.inner.sql_txn_active.load(Ordering::Acquire) && params.is_empty() {
                if let Ok(prepared_sql) = prepared_statement_sql(trimmed) {
                    if let Some(prepared) = self.try_prepare_from_plan_cache(&prepared_sql)? {
                        if prepared.read_only {
                            let start = if self.inner.tracing.any_enabled() {
                                Some((
                                    std::time::Instant::now(),
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis() as i64,
                                ))
                            } else {
                                None
                            };
                            let result = self.execute_prepared_statement(&prepared, params);
                            if let Some((t0, unix_ms)) = start {
                                let dur = t0.elapsed();
                                self.record_statement_trace(
                                    trimmed,
                                    true,
                                    dur,
                                    unix_ms,
                                    result.as_ref(),
                                );
                            }
                            let result = result?;
                            results.push(result);
                            continue;
                        }
                    }
                }
            }

            reject_unsupported_collated_key_sql(trimmed)?;
            let statement = self.parsed_statement(trimmed)?;
            let start = if self.inner.tracing.any_enabled() {
                Some((
                    std::time::Instant::now(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64,
                ))
            } else {
                None
            };
            let read_only = statement_is_read_only(&statement);
            let result = if read_only {
                self.execute_read_statement(&statement, params)
            } else {
                self.execute_write_statement(trimmed, &statement, params)
            };
            if let Some((t0, unix_ms)) = start {
                let dur = t0.elapsed();
                self.record_statement_trace(trimmed, read_only, dur, unix_ms, result.as_ref());
            }
            let result = result?;
            self.dispatch_plan_cache_invalidation(&statement);
            results.push(result);
        }
        Ok(results)
    }

    fn try_execute_schema_batch_with_single_commit(
        &self,
        sql: &str,
    ) -> Result<Option<Vec<QueryResult>>> {
        let mut statements = Vec::new();
        for statement_sql in split_sql_batch(sql) {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() {
                continue;
            }

            if parse_transaction_control(trimmed).is_some()
                || parse_pragma_command(trimmed)?.is_some()
                || crate::security::parse_set_audit_context(trimmed)?.is_some()
                || crate::security::parse_security_command(trimmed)?.is_some()
                || crate::extensions::parse_extension_sql(trimmed)?.is_some()
                || self
                    .try_execute_sync_inspection_query(trimmed, &[])?
                    .is_some()
                || crate::extensions::try_execute_extension_inspection_query(self, trimmed, &[])?
                    .is_some()
            {
                return Ok(None);
            }

            let statement = self.parsed_statement(trimmed)?;
            if !matches!(
                statement.as_ref(),
                SqlStatement::CreateTable(_)
                    | SqlStatement::CreateTableAs(_)
                    | SqlStatement::CreateSchema { .. }
                    | SqlStatement::CreateIndex(_)
                    | SqlStatement::CreateView(_)
                    | SqlStatement::CreateTrigger(_),
            ) {
                return Ok(None);
            }

            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            if self.statement_is_temp_only(&runtime, statement.as_ref()) {
                return Ok(None);
            }

            statements.push(statement);
        }

        if statements.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let lw_start = if self.inner.tracing.config.lock_wait.enabled {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.record_lock_wait(lw_start, "sql_write", "ok");

        let mut state = self.build_exclusive_sql_txn_state()?;
        let snapshot_lsn = state.snapshot_lsn();
        let mut results = Vec::with_capacity(statements.len());
        for statement in &statements {
            let result = self.execute_write_in_runtime_state(
                statement.as_ref(),
                &[],
                &mut state.runtime,
                snapshot_lsn,
                &mut state.persistent_changed,
                &mut state.indexes_maybe_stale,
            )?;
            self.dispatch_plan_cache_invalidation(statement);
            results.push(result);
        }

        self.commit_exclusive_sql_txn(state)?;
        Ok(Some(results))
    }

    fn dispatch_plan_cache_invalidation(&self, statement: &SqlStatement) {
        use crate::plan_cache::SqlStatementExt;
        use SqlStatement::*;
        let inner: &DbInner = &self.inner;
        match statement {
            CreateTable(_)
            | CreateTableAs(_)
            | CreateSchema { .. }
            | CreateIndex(_)
            | CreateView(_)
            | CreateTrigger(_)
            | DropTable { .. }
            | DropIndex { .. }
            | DropView { .. }
            | DropTrigger { .. }
            | AlterTable { .. }
            | AlterIndexRebuild { .. }
            | AlterIndexVerify { .. }
            | AlterViewRename { .. }
            | TruncateTable { .. } => {
                crate::plan_cache::PlanCacheInvalidator::on_persistent_ddl(inner);
            }
            Analyze { .. } => {
                crate::plan_cache::PlanCacheInvalidator::on_analyze(
                    inner,
                    statement.table_name_for_analyze().unwrap_or(""),
                );
            }
            _ => {}
        }
    }

    fn statement_can_enter_plan_cache(statement: &SqlStatement) -> bool {
        matches!(
            statement,
            SqlStatement::Query(_)
                | SqlStatement::Insert(_)
                | SqlStatement::Update(_)
                | SqlStatement::Delete(_)
        )
    }

    fn record_statement_trace(
        &self,
        sql: &str,
        read_only: bool,
        duration: std::time::Duration,
        started_at_unix_ms: i64,
        result: std::result::Result<&QueryResult, &DbError>,
    ) {
        if !self.inner.tracing.any_enabled() {
            return;
        }
        let status = match result {
            Ok(_) => "ok",
            Err(_) => "error",
        };
        self.inner.tracing.record_slow_query(
            duration,
            started_at_unix_ms,
            "statement",
            read_only,
            sql,
            status,
            None,
            false,
        );
    }

    fn record_lock_wait(&self, start: Option<std::time::Instant>, source: &str, status: &str) {
        if let Some(t0) = start {
            let dur = t0.elapsed();
            self.inner
                .tracing
                .record_lock_wait(dur, source, status, false);
        }
    }

    fn try_execute_simple_count_sql_fast_path(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !params.is_empty() || self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(plan) = parse_simple_count_star_sql(sql) else {
            return Ok(None);
        };

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        let Some(runtime) = self.runtime_read_for_fast_read_at_snapshot(snapshot_lsn)? else {
            return Ok(None);
        };
        let Some(table) = runtime.catalog.table(plan.table_name) else {
            return Ok(None);
        };
        if runtime.temp_table_schema(plan.table_name).is_some()
            || runtime
                .catalog
                .views
                .keys()
                .any(|view_name| identifiers_equal(view_name, plan.table_name))
        {
            return Ok(None);
        }
        let row_count = self.runtime_table_row_count(&runtime, &table.name, Some(snapshot_lsn))?;
        let row_count = i64::try_from(row_count).map_err(|_| {
            DbError::sql(format!(
                "table {} exceeds COUNT(*) row-count limits",
                plan.table_name
            ))
        })?;
        drop(runtime);
        drop(reader);
        Ok(Some(QueryResult::with_rows(
            vec!["COUNT(*)".to_string()],
            vec![QueryRow::new(vec![Value::Int64(row_count)])],
        )))
    }

    fn try_execute_simple_grouped_count_sql_fast_path(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !params.is_empty() || self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(plan) = parse_simple_grouped_count_sql(sql) else {
            return Ok(None);
        };

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        let Some(runtime) = self.runtime_read_for_fast_read_at_snapshot(snapshot_lsn)? else {
            return Ok(None);
        };
        let result = runtime.try_execute_simple_grouped_count_sql_from_runtime_index(
            plan.table_name,
            plan.group_column,
        )?;
        drop(runtime);
        drop(reader);
        Ok(result)
    }

    fn try_execute_simple_row_id_projection_sql_fast_path(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(plan) = parse_simple_row_id_projection_sql(sql) else {
            return Ok(None);
        };
        let Some(Value::Int64(lookup_row_id)) = params.get(plan.param_index) else {
            return Ok(None);
        };

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        let Some(runtime) = self.runtime_read_for_fast_read_at_snapshot(snapshot_lsn)? else {
            return Ok(None);
        };
        let result =
            runtime.execute_simple_row_id_projection_at_snapshot(SimpleRowIdProjectionRequest {
                table_name: plan.table_name,
                projection_columns: &plan.projection_columns,
                filter_column: plan.filter_column,
                lookup_row_id: *lookup_row_id,
                pager: &self.inner.pager,
                wal: &self.inner.wal,
                snapshot_lsn,
                use_persistent_pk_index: self.inner.config.persistent_pk_index,
            })?;
        drop(runtime);
        drop(reader);
        Ok(result)
    }

    pub(crate) fn begin_deferred_group_commit(
        &self,
    ) -> crate::wal::writer::DeferredGroupCommitGuard {
        self.inner.wal.begin_deferred_group_commit()
    }

    pub(crate) fn begin_process_writer_batch(
        &self,
    ) -> Result<Option<crate::wal::coordination::ProcessWriterGuard>> {
        self.inner.wal.lock_process_writer()
    }

    pub(crate) fn flush_deferred_group_commit(&self) -> Result<bool> {
        self.inner.wal.flush_deferred_group_commit()
    }

    fn write_queue(&self) -> &WriteQueue {
        self.inner
            .write_queue
            .get_or_init(|| WriteQueue::new(&self.inner.config))
    }

    fn reactive_hub(&self) -> Arc<ReactiveHub> {
        Arc::clone(self.inner.reactive_hub.get_or_init(|| {
            crate::reactive::acquire_hub(
                self.inner.reactive_registry_key.clone(),
                &self.inner.config,
            )
        }))
    }

    fn reactive_hub_if_initialized(&self) -> Option<&Arc<ReactiveHub>> {
        self.inner.reactive_hub.get()
    }

    fn reactive_hub_if_available(&self) -> Option<Arc<ReactiveHub>> {
        self.reactive_hub_if_initialized()
            .map(Arc::clone)
            .or_else(|| crate::reactive::existing_hub(self.inner.reactive_registry_key.as_ref()))
    }

    fn reactive_has_watchers(&self) -> bool {
        self.reactive_hub_if_available()
            .is_some_and(|hub| hub.has_watchers())
    }

    fn reject_transaction_control_for_queued_sql(&self, sql: &str) -> Result<()> {
        for statement_sql in split_sql_batch(sql) {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() {
                continue;
            }
            if parse_transaction_control(trimmed).is_some() {
                return Err(DbError::transaction(
                    "queued execution does not support explicit transaction control; use direct transaction APIs",
                ));
            }
        }
        Ok(())
    }

    /// Prepares a single SQL statement for repeated execution.
    ///
    /// Prepared statements are bound to the current schema cookie. If the schema
    /// changes, the handle must be recreated before it can be executed again.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        if !self.inner.sql_txn_active.load(Ordering::Acquire) {
            let prepared_sql = prepared_statement_sql(sql)?;
            if let Some(prepared) = self.try_prepare_from_plan_cache(&prepared_sql)? {
                return Ok(prepared);
            }
        }
        let runtime = self.runtime_for_prepare()?;
        self.prepare_with_runtime(sql, &runtime)
    }

    /// Loads rows into a table as a single writer-held bulk operation.
    pub fn bulk_load_rows(
        &self,
        table_name: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
        options: BulkLoadOptions,
    ) -> Result<u64> {
        let lw_start = if self.inner.tracing.config.lock_wait.enabled {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.record_lock_wait(lw_start, "sql_write", "ok");
        if self.inner.config.defer_table_materialization {
            let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
            let snapshot_lsn = reader.snapshot_lsn();
            self.refresh_engine_from_snapshot(snapshot_lsn)?;
            let mut working = self.engine_snapshot()?;
            let deferred_table_names = working.deferred_table_names().cloned().collect::<Vec<_>>();
            self.load_all_runtime_row_sources_at_snapshot(&mut working, snapshot_lsn)?;
            drop(reader);
            let inserted = working.bulk_load_rows(
                table_name,
                columns,
                rows,
                options,
                self.inner.config.page_size,
            )?;
            self.persist_runtime(working)?;
            let deferred_refs = deferred_table_names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            self.redefer_persisted_tables_after_write(&deferred_refs)?;
            if options.checkpoint_on_complete {
                self.checkpoint_wal()?;
            }
            return Ok(inserted);
        }
        self.refresh_and_ensure_all_tables_loaded()?;
        let mut working = self.engine_snapshot()?;
        let inserted = working.bulk_load_rows(
            table_name,
            columns,
            rows,
            options,
            self.inner.config.page_size,
        )?;
        self.persist_runtime(working)?;
        if options.checkpoint_on_complete {
            self.checkpoint_wal()?;
        }
        Ok(inserted)
    }

    /// Rebuilds a single named index from the persisted table state.
    pub fn rebuild_index(&self, name: &str) -> Result<()> {
        let lw_start = if self.inner.tracing.config.lock_wait.enabled {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.record_lock_wait(lw_start, "sql_write", "ok");
        if self.inner.config.defer_table_materialization {
            let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
            let snapshot_lsn = reader.snapshot_lsn();
            self.refresh_engine_from_snapshot(snapshot_lsn)?;
            let mut working = self.engine_snapshot()?;
            let table_name = working
                .catalog
                .index(name)
                .ok_or_else(|| DbError::sql(format!("unknown index {name}")))?
                .table_name
                .clone();
            self.load_runtime_table_row_sources_at_snapshot(
                &mut working,
                &[table_name.as_str()],
                snapshot_lsn,
            )?;
            drop(reader);
            working.rebuild_index(name, self.inner.config.page_size)?;
            self.persist_runtime(working)?;
            self.redefer_persisted_tables_after_write(&[table_name.as_str()])?;
            return Ok(());
        }
        self.refresh_and_ensure_all_tables_loaded()?;
        let mut working = self.engine_snapshot()?;
        working.rebuild_index(name, self.inner.config.page_size)?;
        self.persist_runtime(working).map(|_| ())
    }

    /// Rebuilds all indexes from the persisted table state.
    pub fn rebuild_indexes(&self) -> Result<()> {
        let lw_start = if self.inner.tracing.config.lock_wait.enabled {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.record_lock_wait(lw_start, "sql_write", "ok");
        if self.inner.config.defer_table_materialization {
            let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
            let snapshot_lsn = reader.snapshot_lsn();
            self.refresh_engine_from_snapshot(snapshot_lsn)?;
            let mut working = self.engine_snapshot()?;
            let table_names = working.deferred_table_names().cloned().collect::<Vec<_>>();
            let table_refs: Vec<&str> = table_names.iter().map(String::as_str).collect();
            self.load_runtime_table_row_sources_at_snapshot(
                &mut working,
                &table_refs,
                snapshot_lsn,
            )?;
            drop(reader);
            working.rebuild_indexes(self.inner.config.page_size)?;
            self.persist_runtime(working)?;
            self.redefer_persisted_tables_after_write(&table_refs)?;
            return Ok(());
        }
        self.refresh_and_ensure_all_tables_loaded()?;
        let mut working = self.engine_snapshot()?;
        working.rebuild_indexes(self.inner.config.page_size)?;
        self.persist_runtime(working).map(|_| ())
    }

    /// Holds a snapshot open until `release_snapshot` is called.
    pub fn hold_snapshot(&self) -> Result<u64> {
        let guard = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let token = guard.id();
        self.inner
            .held_snapshots
            .lock()
            .map_err(|_| DbError::internal("snapshot registry lock poisoned"))?
            .insert(token, guard);
        Ok(token)
    }

    /// Releases a snapshot previously acquired with `hold_snapshot`.
    pub fn release_snapshot(&self, token: u64) -> Result<()> {
        self.inner
            .held_snapshots
            .lock()
            .map_err(|_| DbError::internal("snapshot registry lock poisoned"))?
            .remove(&token)
            .ok_or_else(|| DbError::transaction(format!("unknown snapshot token {token}")))?;
        Ok(())
    }

    /// Reads a page using a previously held snapshot token.
    pub fn read_page_for_snapshot(&self, token: u64, page_id: u32) -> Result<Arc<[u8]>> {
        page::validate_page_id(page_id)?;
        #[cfg(feature = "bench-internals")]
        READ_PATH_HELD_SNAPSHOTS_LOCK_COUNT.fetch_add(1, Ordering::Relaxed);
        let snapshot_lsn = self
            .inner
            .held_snapshots
            .lock()
            .map_err(|_| DbError::internal("snapshot registry lock poisoned"))?
            .get(&token)
            .map(|guard| guard.snapshot_lsn())
            .ok_or_else(|| DbError::transaction(format!("unknown snapshot token {token}")))?;
        self.read_page_at_snapshot_lsn(page_id, snapshot_lsn)
    }

    /// Creates an immutable named snapshot of the current durable `main` state.
    pub fn snapshot_create(&self, name: &str) -> Result<crate::branch::NamedSnapshot> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Err(DbError::transaction(
                "cannot create a named snapshot while a SQL transaction is active",
            ));
        }
        let initial_lsn = self.inner.wal.latest_snapshot();
        self.inner.wal.set_retained_snapshot_lsn(Some(initial_lsn));
        self.checkpoint_wal()?;
        let snapshot_lsn = self.inner.wal.latest_snapshot();
        let schema_cookie = self.current_schema_cookie_at_snapshot(snapshot_lsn)?;
        self.inner.wal.set_retained_snapshot_lsn(Some(snapshot_lsn));
        let result = crate::branch::create_named_snapshot(self, name, snapshot_lsn, schema_cookie);
        self.refresh_named_snapshot_retention()?;
        result
    }

    /// Lists retained named snapshots.
    pub fn snapshot_list(&self) -> Result<Vec<crate::branch::NamedSnapshot>> {
        crate::branch::list_named_snapshots(self)
    }

    /// Returns one retained named snapshot by name.
    pub fn snapshot_get(&self, name: &str) -> Result<Option<crate::branch::NamedSnapshot>> {
        crate::branch::snapshot_by_name(self, name)
    }

    /// Resolves a named snapshot or branch-head ID to its retained WAL LSN.
    pub fn snapshot_lsn_for_ref(&self, reference: &str) -> Result<Option<u64>> {
        if let Some(snapshot) = self.snapshot_get(reference)? {
            return Ok(Some(snapshot.snapshot_lsn));
        }
        crate::branch::branch_head_lsn_by_id(self, reference)
    }

    /// Creates a branch from `main`, another branch, a named snapshot, or a branch head.
    pub fn branch_create(
        &self,
        name: &str,
        from: Option<&str>,
    ) -> Result<crate::branch::BranchInfo> {
        let source = from.unwrap_or(crate::branch::DEFAULT_BRANCH_NAME);
        let (source_lsn, parent_head_id) = if source == crate::branch::DEFAULT_BRANCH_NAME {
            let initial_lsn = self.inner.wal.latest_snapshot();
            self.inner.wal.set_retained_snapshot_lsn(Some(initial_lsn));
            self.checkpoint_wal()?;
            let source_lsn = self.inner.wal.latest_snapshot();
            let parent_head_id = crate::branch::main_branch_head(self)?.map(|head| head.head_id);
            (source_lsn, parent_head_id)
        } else if let Some(branch) = crate::branch::branch_by_name(self, source)? {
            let source_lsn = self.branch_lsn(source)?.ok_or_else(|| {
                DbError::transaction(format!("branch '{source}' has no current head"))
            })?;
            (source_lsn, branch.current_head_id)
        } else if let Some(snapshot) = self.snapshot_get(source)? {
            (snapshot.snapshot_lsn, Some(snapshot.head_id))
        } else if let Some(source_lsn) = crate::branch::branch_head_lsn_by_id(self, source)? {
            (source_lsn, Some(source.to_string()))
        } else {
            return Err(DbError::transaction(format!(
                "unknown branch, snapshot, or head '{source}'"
            )));
        };
        self.inner.wal.set_retained_snapshot_lsn(Some(source_lsn));
        let schema_cookie = self.current_schema_cookie_at_snapshot(source_lsn)?;
        let result = crate::branch::create_branch(
            self,
            name,
            source_lsn,
            schema_cookie,
            parent_head_id.as_deref(),
        );
        self.refresh_named_snapshot_retention()?;
        result
    }

    /// Lists branches.
    pub fn branch_list(&self) -> Result<Vec<crate::branch::BranchInfo>> {
        crate::branch::list_branches(self)
    }

    /// Deletes a non-main branch.
    pub fn branch_delete(&self, name: &str) -> Result<bool> {
        let deleted = crate::branch::delete_branch(self, name)?;
        if deleted {
            self.refresh_named_snapshot_retention()?;
        }
        Ok(deleted)
    }

    /// Renames a non-main branch.
    pub fn branch_rename(&self, old_name: &str, new_name: &str) -> Result<bool> {
        crate::branch::rename_branch(self, old_name, new_name)
    }

    /// Resolves a branch name to its current retained WAL LSN.
    pub fn branch_lsn(&self, name: &str) -> Result<Option<u64>> {
        crate::branch::branch_lsn_by_name(self, name)
    }

    /// Adds a named commit marker to a non-main branch.
    pub fn branch_commit(
        &self,
        name: &str,
        message: &str,
    ) -> Result<crate::branch::BranchLogEntry> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Err(DbError::transaction(
                "cannot create a branch commit marker while a SQL transaction is active",
            ));
        }
        if name == crate::branch::DEFAULT_BRANCH_NAME {
            return Err(DbError::transaction(
                "branch commit markers are only supported on non-main branches",
            ));
        }
        let branch = crate::branch::branch_by_name(self, name)?
            .ok_or_else(|| DbError::transaction(format!("unknown branch '{name}'")))?;
        let head = crate::branch::commit_branch(self, &branch, message)?;
        self.refresh_named_snapshot_retention()?;
        Ok(crate::branch::BranchLogEntry {
            head_id: head.head_id,
            branch_id: head.branch_id,
            parent_head_id: head.parent_head_id,
            message: head.message,
            created_at_micros: head.created_at_micros,
            sql: None,
        })
    }

    /// Returns branch head history newest first.
    pub fn branch_log(&self, name: &str) -> Result<Vec<crate::branch::BranchLogEntry>> {
        crate::branch::branch_log(self, name)
    }

    /// Deletes a named snapshot and refreshes the WAL retention floor.
    pub fn snapshot_delete(&self, name: &str) -> Result<bool> {
        let deleted = crate::branch::delete_named_snapshot(self, name)?;
        if deleted {
            self.refresh_named_snapshot_retention()?;
        }
        Ok(deleted)
    }

    pub(crate) fn refresh_named_snapshot_retention(&self) -> Result<()> {
        if self.inner.catalog.schema_cookie()? == 0 {
            self.inner.wal.set_retained_snapshot_lsn(None);
            return Ok(());
        }
        let retained_lsn = crate::branch::retained_snapshot_lsn(self)?;
        self.inner.wal.set_retained_snapshot_lsn(retained_lsn);
        Ok(())
    }

    /// Returns a deterministic JSON summary of storage state for the harness.
    pub fn inspect_storage_state_json(&self) -> Result<String> {
        let header = self.inner.pager.header_snapshot()?;
        let warnings = self.inner.wal.warnings()?;
        // ADR 0143 Phase A: surface per-runtime row residency so callers can
        // verify that Phases B/C/D close the gap between db_file_bytes and
        // tables_in_memory_bytes.
        let (rows_total, bytes_total, table_count, deferred_count) = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            runtime.table_memory_totals()
        };
        let (wal_resident_versions, wal_on_disk_versions) =
            self.inner.wal.version_counts_by_payload()?;
        Ok(format!(
            "{{\"path\":\"{}\",\"page_size\":{},\"page_count\":{},\"schema_cookie\":{},\"wal_end_lsn\":{},\"wal_file_size\":{},\"wal_path\":\"{}\",\"last_checkpoint_lsn\":{},\"active_readers\":{},\"wal_versions\":{},\"wal_resident_versions\":{},\"wal_on_disk_versions\":{},\"warning_count\":{},\"shared_wal\":{},\"tables_in_memory_bytes\":{},\"rows_in_memory_count\":{},\"loaded_table_count\":{},\"deferred_table_count\":{}}}",
            json_escape(self.path().display().to_string()),
            self.inner.config.page_size,
            self.inner.pager.on_disk_page_count()?,
            header.schema_cookie,
            self.inner.wal.latest_snapshot(),
            self.inner.wal.file_size()?,
            json_escape(self.inner.wal.file_path().display().to_string()),
            header.last_checkpoint_lsn,
            self.inner.wal.active_reader_count()?,
            self.inner.wal.version_count()?,
            wal_resident_versions,
            wal_on_disk_versions,
            warnings.len(),
            if self.inner.wal.is_shared() { "true" } else { "false" },
            bytes_total,
            rows_total,
            table_count,
            deferred_count,
        ))
    }

    /// Returns all table definitions with row-count metadata.
    pub fn list_tables(&self) -> Result<Vec<TableInfo>> {
        let runtime = self.runtime_for_metadata_inspection()?;
        let mut tables =
            Vec::with_capacity(runtime.catalog.tables.len() + runtime.temp_tables.len());
        for table in runtime.catalog.tables.values() {
            if crate::sync::is_internal_table_name(&table.name) {
                continue;
            }
            tables.push(table_info(
                table,
                self.runtime_table_row_count(&runtime, &table.name, None)?,
            ));
        }
        for table in runtime.temp_tables.values() {
            tables.push(table_info(
                table,
                self.runtime_table_row_count(&runtime, &table.name, None)?,
            ));
        }
        Ok(tables)
    }

    pub(crate) fn internal_table_exists(&self, name: &str) -> Result<bool> {
        let runtime = self.runtime_for_metadata_inspection()?;
        Ok(runtime
            .catalog
            .tables
            .values()
            .any(|table| identifiers_equal(&table.name, name)))
    }

    /// Returns a single table definition by name.
    pub fn describe_table(&self, name: &str) -> Result<TableInfo> {
        let runtime = self.runtime_for_metadata_inspection()?;
        if runtime.temp_views.contains_key(name) && !runtime.temp_tables.contains_key(name) {
            return Err(DbError::sql(format!("unknown table {name}")));
        }
        let (table, row_count) = if let Some(table) = runtime.temp_tables.get(name) {
            (
                table,
                self.runtime_table_row_count(&runtime, &table.name, None)?,
            )
        } else {
            let table = runtime
                .catalog
                .tables
                .get(name)
                .ok_or_else(|| DbError::sql(format!("unknown table {name}")))?;
            (
                table,
                self.runtime_table_row_count(&runtime, &table.name, None)?,
            )
        };
        Ok(table_info(table, row_count))
    }

    /// Returns canonical `CREATE TABLE` SQL for a named table.
    pub fn table_ddl(&self, name: &str) -> Result<String> {
        let runtime = self.runtime_for_metadata_inspection()?;
        if runtime.temp_views.contains_key(name) && !runtime.temp_tables.contains_key(name) {
            return Err(DbError::sql(format!("unknown table {name}")));
        }
        let table = runtime
            .temp_tables
            .get(name)
            .or_else(|| runtime.catalog.tables.get(name))
            .ok_or_else(|| DbError::sql(format!("unknown table {name}")))?;
        Ok(render_create_table(table))
    }

    /// Returns all index definitions.
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        let runtime = self.runtime_for_metadata_inspection()?;
        Ok(runtime.catalog.indexes.values().map(index_info).collect())
    }

    /// Returns all view definitions.
    pub fn list_views(&self) -> Result<Vec<ViewInfo>> {
        let runtime = self.runtime_for_metadata_inspection()?;
        let mut views = runtime
            .catalog
            .views
            .values()
            .map(view_info)
            .collect::<Vec<_>>();
        views.extend(runtime.temp_views.values().map(view_info));
        Ok(views)
    }

    /// Returns canonical `CREATE VIEW` SQL for a named view.
    pub fn view_ddl(&self, name: &str) -> Result<String> {
        let runtime = self.runtime_for_metadata_inspection()?;
        if runtime.temp_tables.contains_key(name) && !runtime.temp_views.contains_key(name) {
            return Err(DbError::sql(format!("unknown view {name}")));
        }
        let view = runtime
            .temp_views
            .get(name)
            .or_else(|| runtime.catalog.views.get(name))
            .ok_or_else(|| DbError::sql(format!("unknown view {name}")))?;
        Ok(render_create_view(view))
    }

    /// Returns all trigger definitions.
    pub fn list_triggers(&self) -> Result<Vec<TriggerInfo>> {
        let runtime = self.runtime_for_metadata_inspection()?;
        Ok(runtime
            .catalog
            .triggers
            .values()
            .map(trigger_info)
            .collect())
    }

    /// Returns the authoritative rich schema snapshot for bindings and tooling.
    pub fn get_schema_snapshot(&self) -> Result<SchemaSnapshot> {
        let runtime = self.runtime_for_metadata_inspection()?;
        schema_snapshot(self, &runtime)
    }

    /// Returns the stable metadata contract intended for external tooling.
    pub fn get_tooling_metadata(&self) -> Result<ToolingMetadata> {
        let runtime = self.runtime_for_metadata_inspection()?;
        let snapshot = schema_snapshot(self, &runtime)?;
        crate::tooling::build_tooling_metadata(&snapshot, &runtime)
    }

    /// Describes a single SQL statement without executing it.
    pub fn describe_query_contract(&self, sql: &str) -> Result<QueryContract> {
        let runtime = self.runtime_for_metadata_inspection()?;
        let prepared_sql = prepared_statement_sql(sql)?;
        let statement = self.parsed_statement(&prepared_sql)?;
        let snapshot = schema_snapshot(self, &runtime)?;
        let metadata = crate::tooling::build_tooling_metadata(&snapshot, &runtime)?;
        crate::tooling::describe_query_contract(
            &prepared_sql,
            statement.as_ref(),
            &runtime,
            &metadata.schema_fingerprint,
        )
    }

    /// Verifies that a named index can be rebuilt logically from the persisted table state.
    pub fn verify_index(&self, name: &str) -> Result<IndexVerification> {
        let (mut runtime, snapshot_lsn) = self.runtime_for_targeted_row_source_inspection()?;
        let table_name = runtime
            .catalog
            .index(name)
            .ok_or_else(|| DbError::sql(format!("unknown index {name}")))?
            .table_name
            .clone();
        self.ensure_inspection_table_row_source(&mut runtime, &table_name, snapshot_lsn)?;
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        let existing = runtime.index(name).map_or(0, runtime_index_entry_count);

        let mut rebuilt = runtime.clone();
        rebuilt.rebuild_index(name, self.inner.config.page_size)?;
        let actual = rebuilt.index(name).map_or(0, runtime_index_entry_count);

        Ok(IndexVerification {
            name: name.to_string(),
            valid: existing == actual,
            expected_entries: existing,
            actual_entries: actual,
        })
    }

    /// Dumps the current catalog and table contents as deterministic SQL.
    pub fn dump_sql(&self) -> Result<String> {
        let (mut runtime, snapshot_lsn) = self.runtime_for_targeted_row_source_inspection()?;
        render_runtime_dump(self, &mut runtime, snapshot_lsn)
    }

    /// Dumps a retained historical snapshot as deterministic SQL.
    pub fn dump_sql_at_snapshot_lsn(&self, snapshot_lsn: u64) -> Result<String> {
        let schema_cookie = self.current_schema_cookie_at_snapshot(snapshot_lsn)?;
        let mut runtime = EngineRuntime::load_from_storage_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            schema_cookie,
            &self.inner.config,
            snapshot_lsn,
        )?;
        render_runtime_dump(self, &mut runtime, Some(snapshot_lsn))
    }

    /// Installs a global FaultyVfs failpoint used by the storage harness.
    pub fn install_failpoint(
        label: &str,
        action: &str,
        trigger_on: u64,
        value: usize,
    ) -> Result<()> {
        let action = match action {
            "error" => FailAction::Error,
            "partial_read" => FailAction::PartialRead { bytes: value },
            "partial_write" => FailAction::PartialWrite { bytes: value },
            "drop_sync" => FailAction::DropSync,
            _ => {
                return Err(DbError::internal(format!(
                    "unsupported failpoint action {action}"
                )))
            }
        };
        faulty::install_failpoint(Failpoint {
            label: label.to_string(),
            trigger_on,
            action,
        })
    }

    /// Clears all globally installed storage failpoints.
    pub fn clear_failpoints() -> Result<()> {
        faulty::clear_failpoints()
    }

    /// Returns the failpoint decision log as deterministic JSON.
    pub fn failpoint_log_json() -> Result<String> {
        let logs = faulty::failpoint_logs()?;
        let entries = logs
            .into_iter()
            .map(|entry| {
                format!(
                    "{{\"label\":\"{}\",\"hit\":{},\"outcome\":\"{}\"}}",
                    json_escape(entry.label),
                    entry.hit,
                    json_escape(entry.outcome)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        Ok(format!("[{entries}]"))
    }

    fn open_with_vfs(
        path: PathBuf,
        config: DbConfig,
        vfs: VfsHandle,
        coordination_vfs: VfsHandle,
    ) -> Result<Self> {
        let open_lock_key = if vfs.is_memory() {
            None
        } else {
            Some(vfs.canonicalize_path(&path)?)
        };
        let _open_lock_cleanup = DbOpenLockCleanup(open_lock_key.clone());
        let open_lock = open_lock_key
            .as_ref()
            .map(|canonical_path| db_open_lock(canonical_path.clone()))
            .transpose()?;
        let open_guard = open_lock
            .as_ref()
            .map(|lock| {
                lock.lock()
                    .map_err(|_| DbError::internal("database open lock poisoned"))
            })
            .transpose()?;

        let file = vfs.open(
            &path,
            if vfs.is_memory() {
                OpenMode::OpenOrCreate
            } else {
                OpenMode::OpenExisting
            },
            FileKind::Database,
        )?;
        let mut header = storage::read_database_header_vfs(file.as_ref())?;
        storage::repair_empty_database_id_vfs(file.as_ref(), &mut header)?;
        let mut effective_config = config;
        effective_config.page_size = header.page_size;
        let schema_cookie = header.schema_cookie;
        let process_coordinator = crate::wal::coordination::ProcessCoordinator::open(
            &coordination_vfs,
            &path,
            &header,
            effective_config.process_coordination,
            effective_config.process_coordination_timeout_ms,
        )?;

        let pager = PagerHandle::open_with_page_pool(
            Arc::clone(&file),
            header,
            effective_config.cache_size_mb,
            effective_config.page_pool_max,
        )?;
        let wal = WalHandle::acquire(&vfs, &path, &effective_config, &pager, process_coordinator)?;
        wal.set_max_page_count(pager.on_disk_page_count()?);

        // ADR 0143 engine-memory plan: drop the in-memory WAL page-version
        // index before loading the runtime when the on-disk WAL is large.
        // Without this, re-opening a database with an uncheckpointed
        // multi-hundred-MB WAL leaves the entire page-version chain
        // resident (~one Arc<[u8]> per WAL frame), which is what the
        // 2026-04-22 memory probes were reporting as `wal_versions=82191`
        // / 336 MB of pure index. The synchronous checkpoint does the
        // copyback into the data file, then truncates the WAL header so
        // downstream reads service straight from the page cache.
        let on_open_threshold_bytes =
            u64::from(effective_config.auto_checkpoint_on_open_mb) * 1024 * 1024;
        if on_open_threshold_bytes > 0 {
            let wal_size = wal
                .latest_snapshot()
                .saturating_sub(crate::wal::format::WAL_HEADER_SIZE);
            // See DbInner::drop: implicit checkpoint copyback is only safe for
            // non-shared WALs until shared handles coordinate pager cache
            // invalidation.
            if wal_size > on_open_threshold_bytes && !wal.is_shared() {
                // Best-effort: a checkpoint failure here is not fatal,
                // because the runtime load below will still succeed
                // against the existing WAL state. Surface it as a
                // warning-shaped log so embedders can investigate.
                if let Err(_e) = wal.checkpoint(&pager, effective_config.checkpoint_timeout_sec) {}
            }
        }

        let (mut runtime, runtime_lsn) =
            EngineRuntime::load_from_storage(&pager, &wal, schema_cookie, &effective_config)?;
        let audit_context = Arc::new(Mutex::new(crate::security::AuditContext::default()));
        runtime.set_audit_context_handle(Arc::clone(&audit_context));

        let tracing_state = crate::tracing::RuntimeTraceState::new(
            &effective_config.tracing,
            crate::tracing::next_connection_id(),
            crate::error::short_hex_sha256(&path.to_string_lossy()),
        );
        let tracing_arc = Arc::new(tracing_state);
        runtime.set_tracing(Arc::clone(&tracing_arc));

        if tracing_arc.config.lock_wait.enabled && tracing_arc.config.enabled {
            let tracing_for_callback = Arc::clone(&tracing_arc);
            wal.set_process_lock_wait_callback(Some(Arc::new(
                move |checkpoint, elapsed, status| {
                    let source = if checkpoint { "checkpoint" } else { "writer" };
                    tracing_for_callback.record_lock_wait(elapsed, source, status, true);
                },
            )));
        }

        let catalog = CatalogHandle::new(runtime.catalog.as_ref().clone());
        let last_seen_checkpoint_epoch = wal.checkpoint_epoch();
        let reactive_registry_key = open_lock_key.clone();
        let busy_timeout_ms = effective_config.write_queue_default_timeout_ms;
        let mut parsed_plan_cache_config = effective_config.plan_cache.clone();
        let mut prepared_plan_cache_config = effective_config.plan_cache.clone();
        let prepared_budget = effective_config.plan_cache.max_size_bytes / 2;
        parsed_plan_cache_config.max_size_bytes = effective_config
            .plan_cache
            .max_size_bytes
            .saturating_sub(prepared_budget);
        prepared_plan_cache_config.max_size_bytes = prepared_budget;

        let db = Self {
            inner: Arc::new(DbInner {
                path: path.clone(),
                config: effective_config.clone(),
                vfs,
                pager,
                wal,
                catalog,
                engine: RwLock::new(runtime),
                last_runtime_lsn: AtomicU64::new(runtime_lsn),
                writer_last_commit_lsn: AtomicU64::new(0),
                last_seen_checkpoint_epoch: AtomicU64::new(last_seen_checkpoint_epoch),
                last_explicit_checkpoint_epoch: AtomicU64::new(0),
                sql_write_lock: Mutex::new(()),
                sql_txn: Mutex::new(SqlTxnSlot::None),
                sql_txn_active: AtomicBool::new(false),
                write_txn: Mutex::new(WriteTxn::default()),
                write_txn_active: AtomicBool::new(false),
                busy_timeout_ms: AtomicU64::new(busy_timeout_ms),
                temp_state: Mutex::new(TempSchemaState::default()),
                statement_cache: Mutex::new(StatementCache::default()),
                prepared_insert_cache: Mutex::new(PreparedInsertCache::default()),
                plan_cache: Mutex::new(PlanCache::new(&parsed_plan_cache_config)),
                prepared_plan_cache: Mutex::new(PreparedPlanCache::new(
                    &prepared_plan_cache_config,
                )),
                policy_mask_generation: crate::plan_cache::PolicyMaskGeneration::new(0),
                held_snapshots: Mutex::new(HashMap::new()),
                sync_ctx: SyncContext::new(&path),
                reactive_registry_key,
                reactive_hub: OnceLock::new(),
                audit_context,
                read_only_paged_row_source_residency: Mutex::new(
                    ReadOnlyPagedRowSourceResidency::default(),
                ),
                write_queue: OnceLock::new(),
                tracing: Arc::clone(&tracing_arc),
            }),
        };
        db.backfill_paged_row_storage()?;
        db.refresh_named_snapshot_retention()?;
        drop(open_guard);
        drop(open_lock);
        if let Some(canonical_path) = open_lock_key {
            prune_db_open_lock_registry(&canonical_path);
        }
        Ok(db)
    }

    #[must_use]
    pub fn config(&self) -> &DbConfig {
        &self.inner.config
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Sets a per-handle audit context value used by policies, masks, and
    /// audit metadata.
    pub fn set_audit_context_value(&self, key: &str, value: Value) -> Result<()> {
        if key.trim().is_empty() {
            return Err(DbError::sql("audit context key must not be empty"));
        }
        self.inner
            .audit_context
            .lock()
            .map_err(|_| DbError::internal("audit context lock poisoned"))?
            .set(key.trim().to_string(), value);
        Ok(())
    }

    /// Removes a per-handle audit context value.
    pub fn clear_audit_context_value(&self, key: &str) -> Result<()> {
        self.inner
            .audit_context
            .lock()
            .map_err(|_| DbError::internal("audit context lock poisoned"))?
            .remove(key);
        Ok(())
    }

    /// Returns a snapshot of the current per-handle audit context.
    pub fn audit_context_snapshot(&self) -> Result<BTreeMap<String, Value>> {
        Ok(self
            .inner
            .audit_context
            .lock()
            .map_err(|_| DbError::internal("audit context lock poisoned"))?
            .snapshot())
    }

    pub fn schema_cookie(&self) -> Result<u32> {
        self.inner.catalog.schema_cookie()
    }

    /// Returns a snapshot of the connection-local plan cache summary.
    pub fn plan_cache_summary(&self) -> Result<crate::plan_cache::PlanCacheSummary> {
        let parsed = self
            .inner
            .plan_cache
            .lock()
            .map(|cache| cache.summary())
            .map_err(|_| DbError::internal("plan cache lock poisoned"))?;
        let prepared = self
            .inner
            .prepared_plan_cache
            .lock()
            .map(|cache| cache.summary())
            .map_err(|_| DbError::internal("prepared plan cache lock poisoned"))?;
        let total_hits = parsed.total_hits.saturating_add(prepared.total_hits);
        let total_misses = parsed.total_misses.saturating_add(prepared.total_misses);
        let total_lookups = total_hits.saturating_add(total_misses);
        let hit_rate = if total_lookups == 0 {
            0.0
        } else {
            (total_hits as f64) * 100.0 / (total_lookups as f64)
        };
        Ok(crate::plan_cache::PlanCacheSummary {
            scope: "connection",
            total_entries: parsed.total_entries.saturating_add(prepared.total_entries),
            total_hits,
            total_misses,
            total_evictions: parsed
                .total_evictions
                .saturating_add(prepared.total_evictions),
            total_size_bytes: parsed
                .total_size_bytes
                .saturating_add(prepared.total_size_bytes),
            max_size_bytes: parsed
                .max_size_bytes
                .saturating_add(prepared.max_size_bytes),
            total_oversized_refusals: parsed
                .total_oversized_refusals
                .saturating_add(prepared.total_oversized_refusals),
            hit_rate,
        })
    }

    /// Returns a snapshot of every entry currently held in the
    /// connection-local plan cache.
    pub fn plan_cache_entries(&self) -> Result<Vec<crate::plan_cache::PlanCacheEntry>> {
        self.inner
            .plan_cache
            .lock()
            .map(|cache| cache.snapshot_entries())
            .map_err(|_| DbError::internal("plan cache lock poisoned"))
    }

    fn prepared_plan_cache_entries(&self) -> Result<Vec<PreparedPlanCacheEntry>> {
        self.inner
            .prepared_plan_cache
            .lock()
            .map(|cache| cache.snapshot_entries())
            .map_err(|_| DbError::internal("prepared plan cache lock poisoned"))
    }

    /// Flushes the connection-local plan cache and resets its counters.
    pub fn flush_plan_cache(&self) -> Result<()> {
        self.inner
            .plan_cache
            .lock()
            .map(|mut cache| cache.flush())
            .map_err(|_| DbError::internal("plan cache lock poisoned"))?;
        self.inner
            .prepared_plan_cache
            .lock()
            .map(|mut cache| cache.flush())
            .map_err(|_| DbError::internal("prepared plan cache lock poisoned"))
    }

    /// Returns the current policy/mask generation counter. The counter
    /// is bumped on every CREATE/DROP/ALTER POLICY and on projection
    /// mask changes; see ADR 0192.
    pub fn policy_mask_generation(&self) -> u32 {
        self.inner.policy_mask_generation.current()
    }

    #[must_use]
    pub fn extensions(&self) -> crate::extensions::ExtensionManager<'_> {
        crate::extensions::ExtensionManager::new(self)
    }

    pub(crate) fn set_schema_cookie(&self, schema_cookie: u32) -> Result<()> {
        self.inner.pager.set_schema_cookie(schema_cookie)
    }

    fn execute_read_statement(
        &self,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    let snapshot_lsn = state.snapshot_lsn();
                    return self.execute_read_in_runtime_state(
                        statement,
                        params,
                        &mut state.runtime,
                        snapshot_lsn,
                        &mut state.indexes_maybe_stale,
                    );
                }
                SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => {}
            }
        }

        self.execute_nontransaction_read_statement(statement, params, None)
    }

    fn execute_nontransaction_read_statement(
        &self,
        statement: &SqlStatement,
        params: &[Value],
        prepared: Option<&PreparedStatement>,
    ) -> Result<QueryResult> {
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_against_runtime(prepared, &runtime)?;
            let extension_execution_enabled = self.inner.config.extension_unsigned_development_mode
                || !self.inner.config.extension_trust_anchors.is_empty();
            if !extension_execution_enabled && self.statement_is_temp_only(&runtime, statement) {
                drop(runtime);
                return self.execute_autocommit_temp_only_statement(statement, params);
            }
        }
        if !self.inner.config.defer_table_materialization {
            self.refresh_engine_from_storage()?;
            self.ensure_all_tables_loaded()?;
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_against_runtime(prepared, &runtime)?;
            return runtime.execute_read_statement(statement, params, self.inner.config.page_size);
        }

        // Fast path: when the statement's base tables are already resident at
        // the pinned reader snapshot (e.g. after a same-handle bulk load or
        // write with `retain_paged_row_sources_after_commit`), execute against
        // the resident runtime without reloading row sources. This skips the
        // per-statement O(table size) reload that dominates filtered/aggregate
        // read workloads otherwise.
        //
        // Gated off when Lua extensions are active: the deferred path's
        // `ensure_tables_loaded_at_snapshot` loads extension catalog tables
        // before execution, and bypassing it would leave extension functions
        // unresolved. Row-level security also requires the deferred path when
        // security catalog tables are deferred or active; otherwise policies
        // and masks could be treated as absent by the generic executor.
        let extension_execution_enabled = self.inner.config.extension_unsigned_development_mode
            || !self.inner.config.extension_trust_anchors.is_empty();
        #[cfg(feature = "bench-internals")]
        READ_PATH_WAL_READER_BEGIN_COUNT.fetch_add(1, Ordering::Relaxed);
        let mut reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let mut snapshot_lsn = reader.snapshot_lsn();
        if !extension_execution_enabled {
            if let Some(runtime) =
                self.try_resident_read_for_statement_at_snapshot(statement, prepared, snapshot_lsn)?
            {
                let result =
                    runtime.execute_read_statement(statement, params, self.inner.config.page_size);
                drop(runtime);
                drop(reader);
                return self.finalize_row_source_autocommit_statement(statement, result);
            }
        }

        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        if self.inner.config.extension_unsigned_development_mode
            || !self.inner.config.extension_trust_anchors.is_empty()
        {
            self.ensure_tables_loaded_at_snapshot(
                &crate::extensions::extension_catalog_table_names(),
                Some(snapshot_lsn),
            )?;
        }
        let security_active = self.ensure_security_tables_loaded_at_snapshot(snapshot_lsn)?;
        if let SqlStatement::Explain(explain) = statement {
            if !explain.analyze {
                let runtime = self
                    .inner
                    .engine
                    .read()
                    .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
                self.validate_prepared_against_runtime(prepared, &runtime)?;
                let result =
                    runtime.execute_read_statement(statement, params, self.inner.config.page_size);
                drop(runtime);
                drop(reader);
                return self.finalize_row_source_autocommit_statement(statement, result);
            }
        }
        if !security_active {
            if let SqlStatement::Query(query) = statement {
                let mut runtime_guard = Some(
                    self.inner
                        .engine
                        .read()
                        .map_err(|_| DbError::internal("engine runtime lock poisoned"))?,
                );

                let missing_runtime_btree = {
                    let runtime = runtime_guard
                        .as_ref()
                        .ok_or_else(|| DbError::internal("runtime guard missing"))?;
                    self.validate_prepared_against_runtime(prepared, runtime)?;
                    if let Some(result) = runtime.try_execute_simple_deferred_count_query(
                        query,
                        &self.inner.pager,
                        &self.inner.wal,
                        snapshot_lsn,
                    )? {
                        drop(runtime_guard);
                        return self
                            .finalize_row_source_autocommit_statement(statement, Ok(result));
                    }
                    if let Some(result) = runtime.try_execute_simple_deferred_min_max_query(
                        query,
                        &self.inner.pager,
                        &self.inner.wal,
                        snapshot_lsn,
                    )? {
                        drop(runtime_guard);
                        return self
                            .finalize_row_source_autocommit_statement(statement, Ok(result));
                    }
                    let missing_pk_root = if self.inner.config.persistent_pk_index {
                        let missing_pk_root = runtime
                            .simple_indexed_projection_missing_persistent_pk_root(query, params)?;
                        if missing_pk_root.is_some() {
                            missing_pk_root
                        } else {
                            runtime.simple_ordered_projection_missing_persistent_pk_root(
                                query, params,
                            )?
                        }
                    } else {
                        None
                    };
                    if let Some(table_name) = missing_pk_root {
                        let table_name = table_name.to_string();
                        drop(runtime_guard.take());
                        drop(reader);
                        self.backfill_missing_persistent_pk_index_for_table(table_name.as_str())?;
                        reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
                        snapshot_lsn = reader.snapshot_lsn();
                        self.refresh_engine_from_snapshot(snapshot_lsn)?;
                        runtime_guard = Some(
                            self.inner
                                .engine
                                .read()
                                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?,
                        );
                    }

                    let runtime = runtime_guard
                        .as_ref()
                        .ok_or_else(|| DbError::internal("runtime guard missing"))?;
                    self.validate_prepared_against_runtime(prepared, runtime)?;
                    if let Some(result) = runtime
                        .try_execute_simple_deferred_indexed_projection_query(
                            query,
                            params,
                            &self.inner.pager,
                            &self.inner.wal,
                            snapshot_lsn,
                            self.inner.config.persistent_pk_index,
                        )?
                    {
                        drop(runtime_guard);
                        return self
                            .finalize_row_source_autocommit_statement(statement, Ok(result));
                    }
                    runtime.simple_indexed_projection_missing_runtime_btree(query, params)?
                };

                if let Some((table_name, index_name)) = missing_runtime_btree {
                    let table_name = table_name.to_string();
                    let index_name = index_name.to_string();
                    drop(runtime_guard.take());
                    self.hydrate_deferred_runtime_index_at_snapshot(
                        table_name.as_str(),
                        index_name.as_str(),
                        snapshot_lsn,
                    )?;
                    runtime_guard = Some(
                        self.inner
                            .engine
                            .read()
                            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?,
                    );

                    let runtime = runtime_guard
                        .as_ref()
                        .ok_or_else(|| DbError::internal("runtime guard missing"))?;
                    self.validate_prepared_against_runtime(prepared, runtime)?;
                    if let Some(result) = runtime
                        .try_execute_simple_deferred_indexed_projection_query(
                            query,
                            params,
                            &self.inner.pager,
                            &self.inner.wal,
                            snapshot_lsn,
                            self.inner.config.persistent_pk_index,
                        )?
                    {
                        drop(runtime_guard);
                        return self
                            .finalize_row_source_autocommit_statement(statement, Ok(result));
                    }
                }

                let runtime = runtime_guard
                    .as_ref()
                    .ok_or_else(|| DbError::internal("runtime guard missing"))?;
                self.validate_prepared_against_runtime(prepared, runtime)?;
                if let Some(result) = runtime.try_execute_simple_deferred_paged_query(
                    query,
                    params,
                    &self.inner.pager,
                    &self.inner.wal,
                    snapshot_lsn,
                    self.inner.config.persistent_pk_index,
                )? {
                    drop(runtime_guard);
                    return self.finalize_row_source_autocommit_statement(statement, Ok(result));
                }
                if prepared.is_none() {
                    if let Some(result) = self
                        .try_execute_indexed_join_grouped_count_query_at_snapshot(
                            runtime,
                            query,
                            params,
                            snapshot_lsn,
                        )?
                    {
                        drop(runtime_guard);
                        return self
                            .finalize_row_source_autocommit_statement(statement, Ok(result));
                    }
                    if let Some(result) = self
                        .try_execute_simple_indexed_join_projection_query_at_snapshot(
                            runtime,
                            statement,
                            query,
                            params,
                            snapshot_lsn,
                        )?
                    {
                        drop(runtime_guard);
                        return self
                            .finalize_row_source_autocommit_statement(statement, Ok(result));
                    }
                    if let Some(result) = self.try_execute_query_with_row_sources_at_snapshot(
                        runtime,
                        statement,
                        params,
                        snapshot_lsn,
                        false,
                    )? {
                        drop(runtime_guard);
                        return self
                            .finalize_row_source_autocommit_statement(statement, Ok(result));
                    }
                }
            }
        }
        if self.inner.config.paged_row_storage {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            if runtime.has_deferred_tables() {
                if let Some(base_tables) =
                    self.safe_referenced_base_tables_in_runtime(&runtime, statement)
                {
                    let mut names: Vec<&str> = base_tables.iter().map(|s| s.as_str()).collect();
                    if self.inner.config.extension_unsigned_development_mode
                        || !self.inner.config.extension_trust_anchors.is_empty()
                    {
                        names.extend(crate::extensions::extension_catalog_table_names());
                    }
                    drop(runtime);
                    self.ensure_table_row_sources_loaded_at_snapshot(&names, snapshot_lsn)?;
                    let runtime = self
                        .inner
                        .engine
                        .read()
                        .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
                    self.validate_prepared_against_runtime(prepared, &runtime)?;
                    let result = runtime.execute_read_statement(
                        statement,
                        params,
                        self.inner.config.page_size,
                    );
                    drop(runtime);
                    return self.finalize_row_source_autocommit_statement(statement, result);
                }
            }
        }

        let targeted_ok =
            self.ensure_tables_loaded_for_statement_at_snapshot(statement, Some(snapshot_lsn))?;
        if !targeted_ok {
            self.ensure_all_tables_loaded_at_snapshot(Some(snapshot_lsn))?;
        }

        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.validate_prepared_against_runtime(prepared, &runtime)?;
        let result = runtime.execute_read_statement(statement, params, self.inner.config.page_size);
        drop(runtime);
        if targeted_ok {
            self.finalize_row_source_autocommit_statement(statement, result)
        } else {
            self.finalize_row_source_autocommit_statement_with_full_redefer(statement, result)
        }
    }

    fn validate_prepared_against_runtime(
        &self,
        prepared: Option<&PreparedStatement>,
        runtime: &EngineRuntime,
    ) -> Result<()> {
        if let Some(prepared) = prepared {
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
        }
        Ok(())
    }

    fn execute_pragma_command(&self, command: PragmaCommand) -> Result<QueryResult> {
        match command {
            PragmaCommand::Query(target) => self.execute_pragma_query(target),
            PragmaCommand::Call { target, argument } => self.execute_pragma_call(target, argument),
            PragmaCommand::Set(target, value) => self.execute_pragma_set(target, value),
        }
    }

    fn ensure_security_catalog(&self) -> Result<()> {
        for ddl in [
            crate::security::POLICIES_DDL,
            crate::security::MASKS_DDL,
            crate::security::AUDIT_EVENTS_DDL,
        ] {
            self.execute_batch_direct_with_params(ddl, &[])?;
        }
        Ok(())
    }

    fn execute_set_audit_context(
        &self,
        command: crate::security::SetAuditContextCommand,
    ) -> Result<QueryResult> {
        match command.value {
            Some(value) => self.set_audit_context_value(&command.key, value)?,
            None => self.clear_audit_context_value(&command.key)?,
        }
        Ok(QueryResult::with_affected_rows(0))
    }

    fn execute_security_command(
        &self,
        sql: &str,
        command: crate::security::SecurityCommand,
    ) -> Result<QueryResult> {
        self.ensure_security_catalog()?;
        let created_at = current_time_micros();
        match command {
            crate::security::SecurityCommand::CreatePolicy {
                name,
                table_name,
                using_sql,
            } => {
                self.execute_with_params(
                    "INSERT INTO __decentdb_policies (policy_name, table_name, using_sql, enabled, created_at_micros) VALUES ($1, $2, $3, TRUE, $4)",
                    &[
                        Value::Text(name.clone()),
                        Value::Text(table_name.clone()),
                        Value::Text(using_sql),
                        Value::Int64(created_at),
                    ],
                )?;
                self.insert_audit_event("CREATE_POLICY", Some(&name), Some(sql))?;
            }
            crate::security::SecurityCommand::DropPolicy { name, if_exists } => {
                let result = self.execute_with_params(
                    "DELETE FROM __decentdb_policies WHERE policy_name = $1",
                    &[Value::Text(name.clone())],
                )?;
                if result.affected_rows() == 0 && !if_exists {
                    return Err(DbError::sql(format!("policy {name} does not exist")));
                }
                self.insert_audit_event("DROP_POLICY", Some(&name), Some(sql))?;
            }
            crate::security::SecurityCommand::AlterPolicy { name, enabled } => {
                let result = self.execute_with_params(
                    "UPDATE __decentdb_policies SET enabled = $1 WHERE policy_name = $2",
                    &[Value::Bool(enabled), Value::Text(name.clone())],
                )?;
                if result.affected_rows() == 0 {
                    return Err(DbError::sql(format!("policy {name} does not exist")));
                }
                self.insert_audit_event("ALTER_POLICY", Some(&name), Some(sql))?;
            }
            crate::security::SecurityCommand::CreateMask {
                name,
                table_name,
                column_name,
                expression_sql,
            } => {
                self.execute_with_params(
                    "INSERT INTO __decentdb_masks (mask_name, table_name, column_name, expression_sql, enabled, created_at_micros) VALUES ($1, $2, $3, $4, TRUE, $5)",
                    &[
                        Value::Text(name.clone()),
                        Value::Text(table_name),
                        Value::Text(column_name),
                        Value::Text(expression_sql),
                        Value::Int64(created_at),
                    ],
                )?;
                self.insert_audit_event("CREATE_MASK", Some(&name), Some(sql))?;
            }
            crate::security::SecurityCommand::DropMask { name, if_exists } => {
                let result = self.execute_with_params(
                    "DELETE FROM __decentdb_masks WHERE mask_name = $1",
                    &[Value::Text(name.clone())],
                )?;
                if result.affected_rows() == 0 && !if_exists {
                    return Err(DbError::sql(format!("mask {name} does not exist")));
                }
                self.insert_audit_event("DROP_MASK", Some(&name), Some(sql))?;
            }
            crate::security::SecurityCommand::AlterMask { name, enabled } => {
                let result = self.execute_with_params(
                    "UPDATE __decentdb_masks SET enabled = $1 WHERE mask_name = $2",
                    &[Value::Bool(enabled), Value::Text(name.clone())],
                )?;
                if result.affected_rows() == 0 {
                    return Err(DbError::sql(format!("mask {name} does not exist")));
                }
                self.insert_audit_event("ALTER_MASK", Some(&name), Some(sql))?;
            }
        }
        Ok(QueryResult::with_affected_rows(0))
    }

    fn insert_audit_event(
        &self,
        operation: &str,
        target: Option<&str>,
        statement: Option<&str>,
    ) -> Result<()> {
        self.ensure_security_catalog()?;
        let context = self.audit_context_snapshot()?;
        let context_json = audit_context_json(&context)?;
        let actor = context
            .get("actor")
            .or_else(|| context.get("user"))
            .map(audit_value_to_text);
        let tenant = context
            .get("tenant_id")
            .or_else(|| context.get("tenant"))
            .map(audit_value_to_text);
        let created_at = current_time_micros();
        let counter = AUDIT_EVENT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let event_id = format!("audit:{created_at}:{counter}");
        self.execute_with_params(
            "INSERT INTO __decentdb_audit_events (event_id, created_at_micros, actor, tenant, operation, target, statement, context_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                Value::Text(event_id),
                Value::Int64(created_at),
                actor.map(Value::Text).unwrap_or(Value::Null),
                tenant.map(Value::Text).unwrap_or(Value::Null),
                Value::Text(operation.to_string()),
                target.map(|value| Value::Text(value.to_string())).unwrap_or(Value::Null),
                statement.map(|value| Value::Text(value.to_string())).unwrap_or(Value::Null),
                Value::Text(context_json),
            ],
        )?;
        Ok(())
    }

    fn execute_pragma_query(&self, target: PragmaTarget) -> Result<QueryResult> {
        match target.name {
            PragmaName::PageSize => Ok(QueryResult::with_rows(
                vec!["page_size".to_string()],
                vec![QueryRow::new(vec![Value::Int64(i64::from(
                    self.inner.config.page_size,
                ))])],
            )),
            PragmaName::CacheSize => Ok(QueryResult::with_rows(
                vec!["cache_size".to_string()],
                vec![QueryRow::new(vec![Value::Int64(cache_size_pages(
                    &self.inner.config,
                ))])],
            )),
            PragmaName::DatabaseList => {
                let file_name = if is_memory_path(&self.inner.path) {
                    ":memory:".to_string()
                } else {
                    self.inner.path.display().to_string()
                };
                Ok(QueryResult::with_rows(
                    vec!["seq".to_string(), "name".to_string(), "file".to_string()],
                    vec![QueryRow::new(vec![
                        Value::Int64(0),
                        Value::Text("main".to_string()),
                        Value::Text(file_name),
                    ])],
                ))
            }
            PragmaName::TableInfo => Err(DbError::sql(
                "PRAGMA table_info(table_name) requires a table name argument",
            )),
            PragmaName::TableXInfo => Err(DbError::sql(
                "PRAGMA table_xinfo(table_name) requires a table name argument",
            )),
            PragmaName::IndexList => Err(DbError::sql(
                "PRAGMA index_list(table_name) requires a table name argument",
            )),
            PragmaName::IndexInfo => Err(DbError::sql(
                "PRAGMA index_info(index_name) requires an index name argument",
            )),
            PragmaName::IndexXInfo => Err(DbError::sql(
                "PRAGMA index_xinfo(index_name) requires an index name argument",
            )),
            PragmaName::ForeignKeyList => Err(DbError::sql(
                "PRAGMA foreign_key_list(table_name) requires a table name argument",
            )),
            PragmaName::TableList => self.execute_compatibility_select(&format!(
                "SELECT * FROM {}pragma_table_list()",
                pragma_schema_function_prefix(target.schema)
            )),
            PragmaName::IntegrityCheck | PragmaName::QuickCheck => self.integrity_check_results(),
            PragmaName::ForeignKeys => Ok(QueryResult::with_rows(
                vec!["foreign_keys".to_string()],
                vec![QueryRow::new(vec![Value::Int64(1)])],
            )),
            PragmaName::JournalMode => Ok(QueryResult::with_rows(
                vec!["journal_mode".to_string()],
                vec![QueryRow::new(vec![Value::Text("wal".to_string())])],
            )),
            PragmaName::Synchronous => Ok(QueryResult::with_rows(
                vec!["synchronous".to_string()],
                vec![QueryRow::new(vec![Value::Int64(
                    pragma_synchronous_mode_value(self.inner.config.wal_sync_mode),
                )])],
            )),
            PragmaName::WalCheckpoint => self.execute_pragma_wal_checkpoint(None),
            PragmaName::SchemaVersion => {
                let runtime = self.runtime_for_metadata_inspection()?;
                let version = match target.schema {
                    Some(PragmaSchema::Temp) => runtime.temp_schema_cookie,
                    _ => runtime.catalog.schema_cookie,
                };
                Ok(QueryResult::with_rows(
                    vec!["schema_version".to_string()],
                    vec![QueryRow::new(vec![Value::Int64(i64::from(version))])],
                ))
            }
            PragmaName::UserVersion => self.execute_application_pragma_query("user_version"),
            PragmaName::ApplicationId => self.execute_application_pragma_query("application_id"),
            PragmaName::Encoding => Ok(QueryResult::with_rows(
                vec!["encoding".to_string()],
                vec![QueryRow::new(vec![Value::Text("UTF-8".to_string())])],
            )),
            PragmaName::LockingMode => Ok(QueryResult::with_rows(
                vec!["locking_mode".to_string()],
                vec![QueryRow::new(vec![Value::Text("normal".to_string())])],
            )),
            PragmaName::TempStore => Ok(QueryResult::with_rows(
                vec!["temp_store".to_string()],
                vec![QueryRow::new(vec![Value::Int64(1)])],
            )),
            PragmaName::BusyTimeout => Ok(QueryResult::with_rows(
                vec!["busy_timeout".to_string()],
                vec![QueryRow::new(vec![Value::Int64(
                    i64::try_from(self.inner.busy_timeout_ms.load(Ordering::Acquire))
                        .unwrap_or(i64::MAX),
                )])],
            )),
            PragmaName::FlushPlanCache => {
                self.flush_plan_cache()?;
                Ok(QueryResult::with_affected_rows(0))
            }
        }
    }

    fn execute_pragma_call(
        &self,
        target: PragmaTarget,
        argument: Option<String>,
    ) -> Result<QueryResult> {
        match target.name {
            PragmaName::TableInfo => {
                let table_name = pragma_required_argument(&target, argument)?;
                self.execute_pragma_table_info(&table_name, target.schema, false)
            }
            PragmaName::TableXInfo => {
                let table_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_table_xinfo({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&table_name)
                ))
            }
            PragmaName::IndexList => {
                let table_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_index_list({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&table_name)
                ))
            }
            PragmaName::IndexInfo => {
                let index_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_index_info({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&index_name)
                ))
            }
            PragmaName::IndexXInfo => {
                let index_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_index_xinfo({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&index_name)
                ))
            }
            PragmaName::ForeignKeyList => {
                let table_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_foreign_key_list({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&table_name)
                ))
            }
            PragmaName::FlushPlanCache => {
                self.flush_plan_cache()?;
                Ok(QueryResult::with_affected_rows(0))
            }
            PragmaName::WalCheckpoint => self.execute_pragma_wal_checkpoint(argument.as_deref()),
            other => Err(DbError::sql(format!(
                "PRAGMA {} does not accept call syntax",
                pragma_name_sql(&other)
            ))),
        }
    }

    fn execute_pragma_table_info(
        &self,
        table_name: &str,
        schema: Option<PragmaSchema>,
        extended: bool,
    ) -> Result<QueryResult> {
        let runtime = self.runtime_for_metadata_inspection()?;
        let table = match schema {
            Some(PragmaSchema::Temp) => runtime
                .temp_table_schema(table_name)
                .ok_or_else(|| DbError::sql(format!("unknown temporary table {table_name}")))?,
            Some(PragmaSchema::Main) => runtime
                .catalog
                .table(table_name)
                .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?,
            None => runtime
                .table_schema(table_name)
                .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?,
        };
        let rows = table
            .columns
            .iter()
            .enumerate()
            .map(|(cid, column)| {
                let mut values = vec![
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
                    values.push(Value::Int64(hidden));
                }
                QueryRow::new(values)
            })
            .collect();
        let mut columns = vec![
            "cid".to_string(),
            "name".to_string(),
            "type".to_string(),
            "notnull".to_string(),
            "dflt_value".to_string(),
            "pk".to_string(),
        ];
        if extended {
            columns.push("hidden".to_string());
        }
        Ok(QueryResult::with_rows(columns, rows))
    }

    fn execute_compatibility_select(&self, sql: &str) -> Result<QueryResult> {
        self.execute(sql)
    }

    fn execute_application_pragma_query(&self, key: &str) -> Result<QueryResult> {
        let value = self.application_pragma_value(key)?;
        Ok(QueryResult::with_rows(
            vec![key.to_string()],
            vec![QueryRow::new(vec![Value::Int64(value)])],
        ))
    }

    fn application_pragma_value(&self, key: &str) -> Result<i64> {
        let sql = format!(
            "SELECT value FROM {} WHERE name = {}",
            sql_identifier(APPLICATION_PRAGMA_TABLE),
            sql_string_literal(key)
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result
                .rows()
                .first()
                .and_then(|row| row.values().first())
                .and_then(|value| match value {
                    Value::Int64(value) => Some(*value),
                    _ => None,
                })
                .unwrap_or(0)),
            Err(DbError::Sql { message }) if message.contains("unknown table") => Ok(0),
            Err(error) => Err(error),
        }
    }

    fn execute_application_pragma_set(
        &self,
        key: &str,
        value: &PragmaValue,
    ) -> Result<QueryResult> {
        let value = pragma_value_i64(value)?;
        if !(i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&value) {
            return Err(DbError::sql(format!(
                "PRAGMA {key} requires a signed 32-bit integer value"
            )));
        }
        let table = sql_identifier(APPLICATION_PRAGMA_TABLE);
        let key_sql = sql_string_literal(key);
        self.execute(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (name TEXT PRIMARY KEY, value INT64 NOT NULL)"
        ))?;
        self.execute(&format!("DELETE FROM {table} WHERE name = {key_sql}"))?;
        self.execute(&format!(
            "INSERT INTO {table} (name, value) VALUES ({key_sql}, {value})"
        ))?;
        Ok(QueryResult::with_affected_rows(0))
    }

    fn execute_pragma_wal_checkpoint(&self, mode: Option<&str>) -> Result<QueryResult> {
        if let Some(mode) = mode {
            match mode.trim().to_ascii_uppercase().as_str() {
                "PASSIVE" | "FULL" | "RESTART" | "TRUNCATE" => {}
                other => {
                    return Err(DbError::sql(format!(
                        "PRAGMA wal_checkpoint mode {other} is not supported; expected PASSIVE, FULL, RESTART, or TRUNCATE"
                    )))
                }
            }
        }
        let active_readers = self.inner.wal.active_reader_count()?;
        let retained_snapshot = self.inner.wal.retained_snapshot_lsn().is_some();
        let before_versions = self.inner.wal.version_count()?;
        self.checkpoint()?;
        let after_versions = self.inner.wal.version_count()?;
        let checkpointed = before_versions.saturating_sub(after_versions);
        Ok(QueryResult::with_rows(
            vec![
                "busy".to_string(),
                "log".to_string(),
                "checkpointed".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Int64(i64::from(active_readers > 0 || retained_snapshot)),
                Value::Int64(i64::try_from(before_versions).unwrap_or(i64::MAX)),
                Value::Int64(i64::try_from(checkpointed).unwrap_or(i64::MAX)),
            ])],
        ))
    }

    fn integrity_check_results(&self) -> Result<QueryResult> {
        let (mut runtime, snapshot_lsn) = self.runtime_for_targeted_row_source_inspection()?;
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        let mut errors = Vec::new();
        let table_names = runtime.catalog.tables.keys().cloned().collect::<Vec<_>>();
        for table_name in table_names {
            self.ensure_inspection_table_row_source(&mut runtime, &table_name, snapshot_lsn)?;
            let Some(table) = runtime.catalog.table(&table_name).cloned() else {
                errors.push(format!("table {table_name} is missing schema"));
                continue;
            };
            let Some(row_source) = runtime.table_row_source(&table_name) else {
                errors.push(format!("table {} is missing row storage", table.name));
                continue;
            };
            for row in row_source.rows() {
                let row = row?;
                if row.values().len() != table.columns.len() {
                    errors.push(format!(
                        "table {} row {} has {} values but schema defines {} columns",
                        table_name,
                        row.row_id(),
                        row.values().len(),
                        table.columns.len()
                    ));
                    break;
                }
            }
            self.redefer_inspection_table_row_source(&mut runtime, &table_name, snapshot_lsn);
        }
        for index in runtime.catalog.indexes.values() {
            if runtime.catalog.table(&index.table_name).is_none() {
                errors.push(format!(
                    "index {} references missing table {}",
                    index.name, index.table_name
                ));
            }
            let table_deferred = runtime
                .deferred_table_names()
                .any(|table_name| identifiers_equal(table_name, &index.table_name));
            if !table_deferred && !runtime.indexes.contains_key(&index.name) {
                errors.push(format!("runtime index {} is missing", index.name));
            }
        }
        if errors.is_empty() {
            Ok(QueryResult::with_rows(
                vec!["integrity_check".to_string()],
                vec![QueryRow::new(vec![Value::Text("ok".to_string())])],
            ))
        } else {
            Ok(QueryResult::with_rows(
                vec!["integrity_check".to_string()],
                errors
                    .into_iter()
                    .map(|error| QueryRow::new(vec![Value::Text(error)]))
                    .collect(),
            ))
        }
    }

    fn execute_pragma_set(&self, target: PragmaTarget, value: PragmaValue) -> Result<QueryResult> {
        match target.name {
            PragmaName::PageSize => {
                let value = pragma_value_i64(&value)?;
                if value == i64::from(self.inner.config.page_size) {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA page_size cannot be changed on an open database; reopen with DbConfig::page_size",
                    ))
                }
            }
            PragmaName::CacheSize => {
                if pragma_value_i64(&value)? == cache_size_pages(&self.inner.config) {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA cache_size cannot be changed on an open connection; reopen with DbConfig::cache_size_mb",
                    ))
                }
            }
            PragmaName::IntegrityCheck
            | PragmaName::DatabaseList
            | PragmaName::TableInfo
            | PragmaName::TableXInfo
            | PragmaName::TableList
            | PragmaName::IndexList
            | PragmaName::IndexInfo
            | PragmaName::IndexXInfo
            | PragmaName::ForeignKeyList
            | PragmaName::WalCheckpoint
            | PragmaName::QuickCheck => Err(DbError::sql(format!(
                "PRAGMA {} does not support assignment",
                pragma_name_sql(&target.name)
            ))),
            PragmaName::FlushPlanCache => {
                let value = parse_pragma_text_or_mode(&value, "PRAGMA flush_plan_cache")?;
                if value == "LOCAL" {
                    self.flush_plan_cache()?;
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA flush_plan_cache accepts only local in this release",
                    ))
                }
            }
            PragmaName::ForeignKeys => {
                let value = parse_pragma_bool_value(&value, "PRAGMA foreign_keys")?;
                if value {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA foreign_keys cannot disable foreign key enforcement in DecentDB",
                    ))
                }
            }
            PragmaName::JournalMode => {
                let mode = parse_pragma_text_or_mode(&value, "PRAGMA journal_mode")?;
                if mode == "WAL" {
                    Ok(QueryResult::with_rows(
                        vec!["journal_mode".to_string()],
                        vec![QueryRow::new(vec![Value::Text("wal".to_string())])],
                    ))
                } else {
                    Err(DbError::sql(
                        "PRAGMA journal_mode supports only WAL in this compatibility slice",
                    ))
                }
            }
            PragmaName::Synchronous => {
                let requested = parse_pragma_synchronous_request(&value, "PRAGMA synchronous")?;
                let current = self.inner.config.wal_sync_mode;
                match requested {
                    SynchronousRequest::Full => {
                        if current == WalSyncMode::Full {
                            Ok(QueryResult::with_affected_rows(0))
                        } else {
                            Err(DbError::sql(
                                "PRAGMA synchronous = FULL requires reopening with DbConfig::wal_sync_mode = Full",
                            ))
                        }
                    }
                    SynchronousRequest::Normal => {
                        if current == WalSyncMode::Normal
                            || matches!(current, WalSyncMode::AsyncCommit { .. })
                        {
                            Ok(QueryResult::with_affected_rows(0))
                        } else {
                            Err(DbError::sql(
                                "PRAGMA synchronous = NORMAL requires reopening with DbConfig::wal_sync_mode = Normal or AsyncCommit",
                            ))
                        }
                    }
                    SynchronousRequest::Off => {
                        if current == WalSyncMode::TestingOnlyUnsafeNoSync {
                            Ok(QueryResult::with_affected_rows(0))
                        } else {
                            Err(DbError::sql(
                                "PRAGMA synchronous = OFF requires reopening with DbConfig::wal_sync_mode = TestingOnlyUnsafeNoSync",
                            ))
                        }
                    }
                    SynchronousRequest::Extra => Err(DbError::sql(
                        "PRAGMA synchronous = EXTRA is not supported by DecentDB",
                    )),
                }
            }
            PragmaName::SchemaVersion => Err(DbError::sql(
                "PRAGMA schema_version does not support assignment",
            )),
            PragmaName::UserVersion => self.execute_application_pragma_set("user_version", &value),
            PragmaName::ApplicationId => {
                self.execute_application_pragma_set("application_id", &value)
            }
            PragmaName::Encoding => {
                let mode = parse_pragma_text_or_mode(&value, "PRAGMA encoding")?;
                if mode == "UTF-8" || mode == "UTF8" {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA encoding can only be set to UTF-8 in this compatibility slice",
                    ))
                }
            }
            PragmaName::LockingMode => {
                let mode = parse_pragma_text_or_mode(&value, "PRAGMA locking_mode")?;
                if mode == "NORMAL" {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA locking_mode supports only NORMAL in this compatibility slice",
                    ))
                }
            }
            PragmaName::TempStore => {
                let value = parse_pragma_text_or_mode(&value, "PRAGMA temp_store")?;
                if matches!(value.as_str(), "DEFAULT" | "FILE" | "0" | "1") {
                    Ok(QueryResult::with_affected_rows(0))
                } else if value == "MEMORY" {
                    Err(DbError::sql(
                        "PRAGMA temp_store = MEMORY is not supported in this compatibility slice",
                    ))
                } else {
                    Err(DbError::sql(
                        "PRAGMA temp_store accepts 0, 1, 'DEFAULT', or 'FILE' only",
                    ))
                }
            }
            PragmaName::BusyTimeout => {
                let value = pragma_value_i64(&value)?;
                let value = u64::try_from(value).map_err(|_| {
                    DbError::sql("PRAGMA busy_timeout requires a non-negative integer")
                })?;
                self.inner.busy_timeout_ms.store(value, Ordering::Release);
                Ok(QueryResult::with_affected_rows(0))
            }
        }
    }

    fn execute_prepared_statement(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if prepared.read_only {
            self.execute_prepared_read_statement(prepared, params)
        } else {
            self.execute_prepared_write_statement(prepared, params)
        }
    }

    fn execute_prepared_statement_mut(
        &self,
        prepared: &PreparedStatement,
        params: &mut [Value],
    ) -> Result<QueryResult> {
        if prepared.read_only {
            self.execute_prepared_read_statement(prepared, params)
        } else {
            self.execute_prepared_write_statement_mut(prepared, params)
        }
    }

    pub(crate) fn execute_prepared_batch_with_builder<F>(
        &self,
        prepared: &PreparedStatement,
        row_count: usize,
        param_count: usize,
        mut build_params: F,
    ) -> Result<u64>
    where
        F: FnMut(usize, &mut [Value]) -> Result<()>,
    {
        let mut params = vec![Value::Null; param_count];
        if row_count == 0 {
            return Ok(0);
        }

        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    self.validate_prepared_schema_cookie(
                        prepared,
                        state.runtime.catalog.schema_cookie,
                        state.runtime.temp_schema_cookie,
                    )?;
                    let mut total_affected = 0_u64;
                    if !prepared.read_only
                        && matches!(prepared.statement.as_ref(), SqlStatement::Insert(_))
                    {
                        let snapshot_lsn = state.snapshot_lsn();
                        if let Some(prepared_insert) = self.prepared_insert_plan_for_runtime_state(
                            prepared,
                            &mut state.runtime,
                            snapshot_lsn,
                            &mut state.indexes_maybe_stale,
                            &mut state.prepared_insert_runtime_cache,
                        )? {
                            if Self::prepared_insert_uses_direct_positional_params(
                                prepared_insert.as_ref(),
                                param_count,
                            ) {
                                for row_index in 0..row_count {
                                    build_params(row_index, &mut params)?;
                                    let affected = state
                                        .runtime
                                        .execute_prepared_simple_insert_positional_params_in_place(
                                            prepared_insert.as_ref(),
                                            &mut params,
                                            self.inner.config.page_size,
                                        )?;
                                    total_affected = total_affected.saturating_add(affected);
                                }
                                state.persistent_changed = true;
                                return Ok(total_affected);
                            }
                        }
                    }
                    for row_index in 0..row_count {
                        build_params(row_index, &mut params)?;
                        let result = if prepared.read_only {
                            let snapshot_lsn = state.snapshot_lsn();
                            self.execute_read_in_runtime_state(
                                prepared.statement.as_ref(),
                                &params,
                                &mut state.runtime,
                                snapshot_lsn,
                                &mut state.indexes_maybe_stale,
                            )?
                        } else {
                            self.execute_prepared_in_state(prepared, &params, state)?
                        };
                        total_affected = total_affected.saturating_add(result.affected_rows());
                    }
                    return Ok(total_affected);
                }
                SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => {}
            }
        }

        let mut total_affected = 0_u64;
        for row_index in 0..row_count {
            build_params(row_index, &mut params)?;
            let result = self.execute_prepared_statement(prepared, &params)?;
            total_affected = total_affected.saturating_add(result.affected_rows());
        }
        Ok(total_affected)
    }

    fn prepared_insert_uses_direct_positional_params(
        prepared_insert: &PreparedSimpleInsert,
        param_count: usize,
    ) -> bool {
        prepared_insert.direct_positional_param_count == Some(param_count)
    }

    fn prepared_statement_cache_key(prepared: &PreparedStatement) -> usize {
        if let Some(insert) = prepared.prepared_insert.as_ref() {
            return Arc::as_ptr(insert) as usize;
        }
        Arc::as_ptr(&prepared.statement) as usize
    }

    fn prepared_insert_plan_for_runtime_state(
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

    fn prepared_insert_changes_persistent_table(
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

    fn try_execute_prepared_simple_row_id_projection(
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

    fn try_execute_prepared_simple_row_id_range_projection(
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

    fn try_execute_prepared_simple_row_id_join_projection(
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

    fn try_execute_prepared_simple_scalar_filtered_aggregate(
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

    fn execute_prepared_read_statement(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if let Some(result) =
            self.try_execute_prepared_simple_row_id_projection(prepared, params)?
        {
            return Ok(result);
        }
        if let Some(result) =
            self.try_execute_prepared_simple_row_id_range_projection(prepared, params)?
        {
            return Ok(result);
        }
        if let Some(result) =
            self.try_execute_prepared_simple_row_id_join_projection(prepared, params)?
        {
            return Ok(result);
        }
        if let Some(result) =
            self.try_execute_prepared_simple_scalar_filtered_aggregate(prepared, params)?
        {
            return Ok(result);
        }
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_against_runtime(Some(prepared), &runtime)?;
        }
        if let Some(result) = self.try_execute_prepared_inspection_query(prepared, params)? {
            return Ok(result);
        }
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    let snapshot_lsn = state.snapshot_lsn();
                    self.validate_prepared_schema_cookie(
                        prepared,
                        state.runtime.catalog.schema_cookie,
                        state.runtime.temp_schema_cookie,
                    )?;
                    return self.execute_read_in_runtime_state(
                        prepared.statement.as_ref(),
                        params,
                        &mut state.runtime,
                        snapshot_lsn,
                        &mut state.indexes_maybe_stale,
                    );
                }
                SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => {}
            }
        }

        self.execute_nontransaction_read_statement(
            prepared.statement.as_ref(),
            params,
            Some(prepared),
        )
    }

    fn execute_prepared_write_statement(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    return self.execute_prepared_in_state(prepared, params, state);
                }
                SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => {}
            }
        }
        let lw_start = if self.inner.tracing.config.lock_wait.enabled {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.record_lock_wait(lw_start, "sql_write", "ok");
        if let Some(prepared_update) = prepared.prepared_update.as_deref() {
            if let Some(result) = self.try_execute_autocommit_prepared_update_in_place(
                prepared,
                prepared_update,
                params,
            )? {
                return Ok(result);
            }
        }
        if let Some(prepared_delete) = prepared.prepared_delete.as_deref() {
            if let Some(result) = self.try_execute_autocommit_prepared_delete_in_place(
                prepared,
                prepared_delete,
                params,
            )? {
                return Ok(result);
            }
        }
        if let Some(prepared_insert) = prepared.prepared_insert.as_deref() {
            if let Some(result) = self.try_execute_autocommit_prepared_insert_in_place(
                prepared,
                prepared_insert,
                params,
            )? {
                return Ok(result);
            }
        }
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            if matches!(
                prepared.statement.as_ref(),
                crate::sql::ast::Statement::Insert(insert)
                    if runtime.can_execute_insert_in_place(insert)
            ) {
                drop(runtime);
                return self
                    .execute_autocommit_insert_in_place(prepared.statement.as_ref(), params);
            }
            drop(runtime);
        }
        let temp_only = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            self.statement_is_temp_only(&runtime, prepared.statement.as_ref())
        };
        if temp_only {
            return self
                .execute_autocommit_temp_only_statement(prepared.statement.as_ref(), params);
        }
        if self.can_execute_statement_with_row_sources_at_latest_snapshot(
            prepared.statement.as_ref(),
        )? && self.load_statement_row_sources_at_latest_snapshot(prepared.statement.as_ref())?
        {
            let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
            let result = self.execute_autocommit_in_place(|runtime| {
                runtime.execute_statement(
                    prepared.statement.as_ref(),
                    params,
                    self.inner.config.page_size,
                )
            });
            return self
                .finalize_row_source_autocommit_statement(prepared.statement.as_ref(), result);
        }
        self.refresh_and_load_tables_for_statement_at_latest_snapshot(prepared.statement.as_ref())?;
        let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
        self.execute_autocommit_in_place(|runtime| {
            runtime.execute_statement(
                prepared.statement.as_ref(),
                params,
                self.inner.config.page_size,
            )
        })
    }

    fn execute_prepared_write_statement_mut(
        &self,
        prepared: &PreparedStatement,
        params: &mut [Value],
    ) -> Result<QueryResult> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    return self.execute_prepared_in_state(prepared, params, state);
                }
                SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => {}
            }
        }

        {
            let lw_start = if self.inner.tracing.config.lock_wait.enabled {
                Some(std::time::Instant::now())
            } else {
                None
            };
            let _writer = self
                .inner
                .sql_write_lock
                .lock()
                .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
            self.record_lock_wait(lw_start, "sql_write", "ok");
            if let Some(prepared_insert) = prepared.prepared_insert.as_deref() {
                if let Some(result) = self.try_execute_autocommit_prepared_insert_in_place_mut(
                    prepared,
                    prepared_insert,
                    params,
                )? {
                    return Ok(result);
                }
            }
        }

        self.execute_prepared_write_statement(prepared, params)
    }

    fn execute_write_statement(
        &self,
        sql: &str,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let temp_only = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.statement_is_temp_only(&runtime, statement)
        };
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    return self.execute_statement_in_state(sql, statement, params, state);
                }
                SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => {}
            }
        }

        let lw_start = if self.inner.tracing.config.lock_wait.enabled {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.record_lock_wait(lw_start, "sql_write", "ok");
        if matches!(
            statement,
            crate::sql::ast::Statement::Update(_) | crate::sql::ast::Statement::Delete(_)
        ) {
            if let Some(result) =
                self.try_execute_cached_autocommit_prepared_dml(sql, statement, params)?
            {
                return Ok(result);
            }
        }
        if let crate::sql::ast::Statement::Update(update) = statement {
            let prepared = {
                let runtime = self
                    .inner
                    .engine
                    .read()
                    .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
                runtime.prepare_simple_update(update)?
            };
            if let Some(prepared) = prepared {
                return self.execute_autocommit_simple_update_in_place(&prepared, params);
            }
        }
        if let crate::sql::ast::Statement::Delete(delete) = statement {
            let prepared = {
                let runtime = self
                    .inner
                    .engine
                    .read()
                    .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
                runtime.prepare_simple_delete(delete)?
            };
            if let Some(prepared) = prepared {
                return self.execute_autocommit_simple_delete_in_place(&prepared, params);
            }
        }
        if let crate::sql::ast::Statement::Insert(insert) = statement {
            let prepared = {
                let runtime = self
                    .inner
                    .engine
                    .read()
                    .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
                self.prepared_simple_insert(sql, insert, &runtime)?
            };
            if let Some(prepared) = prepared {
                if self.can_use_autocommit_prepared_insert_fast_path(&prepared.table_name)? {
                    return self
                        .execute_autocommit_prepared_insert_in_place(prepared.as_ref(), params);
                }
                return self.execute_autocommit_insert_in_place(statement, params);
            }
            if self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?
                .can_execute_insert_in_place(insert)
            {
                return self.execute_autocommit_insert_in_place(statement, params);
            }
        }
        if temp_only {
            return self.execute_autocommit_temp_only_statement(statement, params);
        }
        if self.can_execute_statement_with_row_sources_at_latest_snapshot(statement)?
            && self.load_statement_row_sources_at_latest_snapshot(statement)?
        {
            let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
            let result = self.execute_autocommit_in_place(|runtime| {
                runtime.execute_statement(statement, params, self.inner.config.page_size)
            });
            return self.finalize_row_source_autocommit_statement(statement, result);
        }
        self.refresh_and_load_tables_for_statement_at_latest_snapshot(statement)?;
        let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
        self.execute_autocommit_in_place(|runtime| {
            runtime.execute_statement(statement, params, self.inner.config.page_size)
        })
    }

    fn try_execute_cached_autocommit_prepared_dml(
        &self,
        sql: &str,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let prepared = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.prepare_with_runtime(sql, &runtime)?
        };
        match statement {
            crate::sql::ast::Statement::Update(_) => {
                let Some(prepared_update) = prepared.prepared_update.as_deref() else {
                    return Ok(None);
                };
                self.try_execute_autocommit_prepared_update_in_place(
                    &prepared,
                    prepared_update,
                    params,
                )
            }
            crate::sql::ast::Statement::Delete(_) => {
                let Some(prepared_delete) = prepared.prepared_delete.as_deref() else {
                    return Ok(None);
                };
                self.try_execute_autocommit_prepared_delete_in_place(
                    &prepared,
                    prepared_delete,
                    params,
                )
            }
            _ => Ok(None),
        }
    }

    fn execute_autocommit_temp_only_statement(
        &self,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        let mut working = self.engine_snapshot()?;
        let result = working.execute_statement(statement, params, self.inner.config.page_size)?;
        self.install_temp_runtime(working)?;
        Ok(result)
    }

    fn execute_autocommit_insert_in_place(
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

    fn can_use_autocommit_prepared_insert_fast_path(&self, _table_name: &str) -> Result<bool> {
        Ok(true)
    }

    fn execute_autocommit_prepared_insert_in_place(
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

    fn execute_autocommit_simple_update_in_place(
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

    fn execute_autocommit_simple_delete_in_place(
        &self,
        prepared_delete: &PreparedSimpleDelete,
        params: &[Value],
    ) -> Result<QueryResult> {
        let mut table_names = vec![prepared_delete.table.name.as_str()];
        for child in &prepared_delete.restrict_children {
            table_names.push(child.child_table_name.as_str());
        }
        self.load_simple_write_row_sources_at_latest_snapshot(&table_names)?;
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

    fn try_execute_autocommit_prepared_insert_in_place(
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

    fn try_execute_autocommit_prepared_insert_in_place_mut(
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

    fn try_execute_autocommit_prepared_update_in_place(
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

    fn try_execute_autocommit_prepared_delete_in_place(
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
        let mut table_names = vec![prepared_delete.table.name.as_str()];
        for child in &prepared_delete.restrict_children {
            table_names.push(child.child_table_name.as_str());
        }
        self.load_simple_write_row_sources_at_latest_snapshot(&table_names)?;
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

    fn try_execute_zero_row_index_delete_against_current_runtime(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_delete: &PreparedSimpleDelete,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let PreparedDeleteLookup::Index {
            index_name,
            value_source,
        } = &prepared_delete.lookup
        else {
            return Ok(None);
        };
        let value = resolve_prepared_simple_value_for_fast_path(value_source, params)?;
        if matches!(value, Value::Null) {
            return Ok(Some(QueryResult::with_affected_rows(0)));
        }

        let latest_lsn = self.inner.wal.latest_snapshot();
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        if last_runtime_lsn != latest_lsn || last_seen_checkpoint_epoch != latest_checkpoint_epoch {
            return Ok(None);
        }

        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.validate_prepared_schema_cookie(
            prepared_statement,
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
        )?;
        if !runtime.can_reuse_prepared_simple_delete(prepared_delete) {
            return Ok(None);
        }
        let Some(index_schema) = runtime.catalog.indexes.get(index_name) else {
            return Ok(None);
        };
        if !index_schema.fresh || index_schema.kind != IndexKind::Btree {
            return Ok(None);
        }
        let Some(RuntimeIndex::Btree { keys, .. }) = runtime.index(index_name) else {
            return Ok(None);
        };
        if keys.row_ids_for_value_set(&value)?.is_empty() {
            return Ok(Some(QueryResult::with_affected_rows(0)));
        }
        Ok(None)
    }

    fn execute_autocommit_in_place<F>(&self, apply: F) -> Result<QueryResult>
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

    fn runtime_has_persistent_commit_work(&self, runtime: &EngineRuntime) -> Result<bool> {
        if !runtime.dirty_tables.is_empty() {
            return Ok(true);
        }
        Ok(self.inner.catalog.schema_cookie()? != runtime.catalog.schema_cookie)
    }

    fn backfill_missing_persistent_pk_index_for_table(&self, table_name: &str) -> Result<()> {
        if !self.inner.config.persistent_pk_index {
            return Ok(());
        }
        if self.inner.catalog.schema_cookie()? == 0 {
            return Ok(());
        }

        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let needs_backfill = runtime
            .catalog
            .tables
            .iter()
            .find(|(candidate, _)| identifiers_equal(candidate, table_name))
            .is_some_and(|(canonical_name, table)| {
                table.pk_index_root.is_none()
                    && runtime
                        .persisted_tables
                        .get(canonical_name)
                        .is_some_and(|state| state.pointer.head_page_id != 0)
            });
        if !needs_backfill {
            return Ok(());
        }

        self.begin_write()?;
        let changed = match runtime.backfill_missing_persistent_pk_index_for_table(self, table_name)
        {
            Ok(changed) => changed,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !changed {
            self.rollback()?;
            return Ok(());
        }
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
        Ok(())
    }

    fn backfill_paged_row_storage(&self) -> Result<()> {
        if !self.inner.config.paged_row_storage {
            return Ok(());
        }
        if self.inner.catalog.schema_cookie()? == 0 {
            return Ok(());
        }

        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let needs_backfill = runtime.persisted_tables.values().any(|state| {
            state.pointer.head_page_id != 0 && !state.pointer.is_table_paged_manifest()
        });
        if !needs_backfill {
            return Ok(());
        }

        self.begin_write()?;
        let changed = match runtime.backfill_paged_row_storage(self) {
            Ok(changed) => changed,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !changed {
            self.rollback()?;
            return Ok(());
        }
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
        self.inner
            .catalog
            .replace(runtime.catalog.as_ref().clone())?;
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(())
    }

    fn compact_persisted_payloads_before_checkpoint(&self) -> Result<()> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(());
        }
        self.refresh_engine_from_storage()?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        if !runtime.has_checkpoint_compaction_candidates() {
            return Ok(());
        }
        self.begin_write()?;
        let changed = match runtime.compact_persisted_payloads_for_checkpoint(self) {
            Ok(changed) => changed,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        if !changed {
            self.rollback()?;
            return Ok(());
        }
        let committed_lsn = match self.commit() {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(())
    }

    fn engine_snapshot(&self) -> Result<EngineRuntime> {
        let mut snapshot = self.engine_snapshot_without_index_rebuild()?;
        snapshot.rebuild_stale_indexes(self.inner.config.page_size)?;
        Ok(snapshot)
    }

    fn engine_snapshot_without_index_rebuild(&self) -> Result<EngineRuntime> {
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let mut snapshot = runtime.clone();
        self.apply_temp_state_to_runtime(&mut snapshot)?;
        Ok(snapshot)
    }

    #[cfg(test)]
    pub(crate) fn debug_engine_snapshot(&self) -> Result<EngineRuntime> {
        self.engine_snapshot()
    }

    fn apply_temp_state_to_runtime(&self, runtime: &mut EngineRuntime) -> Result<()> {
        self.inner
            .temp_state
            .lock()
            .map_err(|_| DbError::internal("temp schema lock poisoned"))?
            .apply_to_runtime(runtime);
        Ok(())
    }

    fn sync_temp_state_from_runtime(&self, runtime: &EngineRuntime) -> Result<()> {
        if runtime.temp_schema_cookie == 0
            && runtime.temp_tables.is_empty()
            && runtime.temp_table_data.is_empty()
            && runtime.temp_views.is_empty()
            && runtime.temp_indexes.is_empty()
        {
            return Ok(());
        }
        let changed = {
            let mut state = self
                .inner
                .temp_state
                .lock()
                .map_err(|_| DbError::internal("temp schema lock poisoned"))?;
            let before = state.schema_cookie;
            state.update_from_runtime(runtime);
            before != state.schema_cookie
        };
        if changed {
            crate::plan_cache::PlanCacheInvalidator::on_temp_schema_change(&*self.inner);
        }
        Ok(())
    }

    fn install_temp_runtime(&self, runtime: EngineRuntime) -> Result<()> {
        self.sync_temp_state_from_runtime(&runtime)?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        Ok(())
    }

    fn statement_is_temp_only(
        &self,
        runtime: &EngineRuntime,
        statement: &crate::sql::ast::Statement,
    ) -> bool {
        let temp_has_table = |name: &str| {
            runtime
                .temp_tables
                .keys()
                .any(|entry| identifiers_equal(entry, name))
        };
        let temp_has_view = |name: &str| {
            runtime
                .temp_views
                .keys()
                .any(|entry| identifiers_equal(entry, name))
        };
        match statement {
            crate::sql::ast::Statement::CreateTable(statement) => statement.temporary,
            crate::sql::ast::Statement::CreateTableAs(statement) => {
                statement.temporary && self.query_is_temp_only(runtime, &statement.query)
            }
            crate::sql::ast::Statement::CreateView(statement) => {
                statement.temporary && self.query_is_temp_only(runtime, &statement.query)
            }
            crate::sql::ast::Statement::DropTable { name, .. } => temp_has_table(name),
            crate::sql::ast::Statement::DropView { name, .. } => temp_has_view(name),
            crate::sql::ast::Statement::Query(_)
            | crate::sql::ast::Statement::Insert(_)
            | crate::sql::ast::Statement::Update(_)
            | crate::sql::ast::Statement::Delete(_) => {
                self.safe_referenced_names_are_temp_only(runtime, statement)
            }
            _ => false,
        }
    }

    fn query_is_temp_only(&self, runtime: &EngineRuntime, query: &crate::sql::ast::Query) -> bool {
        self.safe_referenced_names_are_temp_only(
            runtime,
            &crate::sql::ast::Statement::Query(query.clone()),
        )
    }

    fn safe_referenced_names_are_temp_only(
        &self,
        runtime: &EngineRuntime,
        statement: &crate::sql::ast::Statement,
    ) -> bool {
        let Some(names) = crate::sql::ast::safe_referenced_tables(statement) else {
            return false;
        };
        let mut visiting_views = BTreeSet::new();
        names
            .into_iter()
            .all(|name| self.referenced_name_is_temp_only(runtime, &name, &mut visiting_views))
    }

    fn referenced_name_is_temp_only(
        &self,
        runtime: &EngineRuntime,
        name: &str,
        visiting_views: &mut BTreeSet<String>,
    ) -> bool {
        if runtime
            .temp_tables
            .keys()
            .any(|entry| identifiers_equal(entry, name))
        {
            return true;
        }
        let Some((view_name, view)) = runtime
            .temp_views
            .iter()
            .find(|(entry, _)| identifiers_equal(entry, name))
        else {
            return false;
        };
        if !visiting_views.insert(view_name.clone()) {
            return false;
        }
        let temp_only = view.dependencies.iter().all(|dependency| {
            self.referenced_name_is_temp_only(runtime, dependency, visiting_views)
        });
        visiting_views.remove(view_name);
        temp_only
    }

    fn parsed_statement(&self, sql: &str) -> Result<Arc<SqlStatement>> {
        // Try the connection-local plan cache first. The cache is keyed
        // by the prepared SQL text plus the current schema cookies and
        // policy/mask generation; on a hit we still get a fresh
        // `PreparedStatement` (which is cheap to construct) but we
        // skip the parse step.
        let prepared_sql = prepared_statement_sql(sql)?;
        let parameter_shape = parameter_shape_for_prepared_sql(&prepared_sql);
        if parameter_shape.arity() == 0 {
            return self
                .inner
                .statement_cache
                .lock()
                .map_err(|_| DbError::internal("statement cache lock poisoned"))?
                .get_or_parse(&prepared_sql);
        }

        let temp_cookie = self
            .inner
            .temp_state
            .lock()
            .map(|s| s.schema_cookie)
            .unwrap_or(0);
        let persistent_cookie = self.inner.catalog.schema_cookie()?;
        let policy_gen = self.inner.policy_mask_generation.current();
        let mut plan_cache = self
            .inner
            .plan_cache
            .lock()
            .map_err(|_| DbError::internal("plan cache lock poisoned"))?;
        let key = crate::plan_cache::PlanCacheKey::new(
            prepared_sql,
            parameter_shape,
            persistent_cookie,
            temp_cookie,
            policy_gen,
        );
        let current_key = key.clone();
        if let Some(statement) = plan_cache.get(&key, persistent_cookie, temp_cookie, policy_gen) {
            return Ok(statement);
        }
        drop(plan_cache);
        // Fall back to the existing narrow statement cache for parse
        // work, then store the parsed statement in the plan cache.
        let statement = self
            .inner
            .statement_cache
            .lock()
            .map_err(|_| DbError::internal("statement cache lock poisoned"))?
            .get_or_parse(&current_key.sql_text)?;
        if Self::statement_can_enter_plan_cache(statement.as_ref()) {
            if let Ok(mut plan_cache) = self.inner.plan_cache.lock() {
                if plan_cache.should_admit_missed_key(&current_key) {
                    let size = crate::plan_cache::statement_accounted_size(&statement);
                    plan_cache.insert(current_key, Arc::clone(&statement), size);
                }
            }
        }
        Ok(statement)
    }

    fn try_prepare_from_plan_cache(&self, prepared_sql: &str) -> Result<Option<PreparedStatement>> {
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
            simple_row_id_range_projection: bundle.simple_row_id_range_projection,
            simple_row_id_join_projection: bundle.simple_row_id_join_projection,
            simple_scalar_filtered_aggregate: bundle.simple_scalar_filtered_aggregate,
            prepared_insert: bundle.prepared_insert,
            prepared_update: bundle.prepared_update,
            prepared_delete: bundle.prepared_delete,
            read_only: bundle.read_only,
        }))
    }

    fn prepare_with_runtime(
        &self,
        sql: &str,
        runtime: &EngineRuntime,
    ) -> Result<PreparedStatement> {
        let prepared_sql = prepared_statement_sql(sql)?;
        let policy_gen = self.inner.policy_mask_generation.current();
        let key = crate::plan_cache::PlanCacheKey::new(
            prepared_sql.clone(),
            parameter_shape_for_prepared_sql(&prepared_sql),
            runtime.catalog.schema_cookie,
            runtime.temp_schema_cookie,
            policy_gen,
        );
        if let Some(bundle) = self
            .inner
            .prepared_plan_cache
            .lock()
            .map_err(|_| DbError::internal("prepared plan cache lock poisoned"))?
            .get(
                &key,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
                policy_gen,
            )
        {
            return Ok(PreparedStatement {
                db: self.clone(),
                schema_cookie: runtime.catalog.schema_cookie,
                temp_schema_cookie: runtime.temp_schema_cookie,
                statement: Arc::clone(&bundle.statement),
                prepared_sql,
                simple_row_id_projection: bundle.simple_row_id_projection,
                simple_row_id_range_projection: bundle.simple_row_id_range_projection,
                simple_row_id_join_projection: bundle.simple_row_id_join_projection,
                simple_scalar_filtered_aggregate: bundle.simple_scalar_filtered_aggregate,
                prepared_insert: bundle.prepared_insert,
                prepared_update: bundle.prepared_update,
                prepared_delete: bundle.prepared_delete,
                read_only: bundle.read_only,
            });
        }
        let statement = self.parsed_statement(&prepared_sql)?;
        let read_only = statement_is_read_only(statement.as_ref());
        let (prepared_insert, prepared_update, prepared_delete) = match statement.as_ref() {
            SqlStatement::Insert(insert) => (
                self.prepared_simple_insert(&prepared_sql, insert, runtime)?,
                None,
                None,
            ),
            SqlStatement::Update(update) => (
                None,
                runtime.prepare_simple_update(update)?.map(Arc::new),
                None,
            ),
            SqlStatement::Delete(delete) => (
                None,
                None,
                runtime.prepare_simple_delete(delete)?.map(Arc::new),
            ),
            _ => (None, None, None),
        };
        let simple_row_id_projection =
            Self::prepared_simple_row_id_projection(&prepared_sql, runtime);
        let simple_row_id_range_projection =
            Self::prepared_simple_row_id_range_projection(&prepared_sql, runtime);
        let simple_row_id_join_projection =
            Self::prepared_simple_row_id_join_projection(statement.as_ref(), runtime);
        let simple_scalar_filtered_aggregate =
            Self::prepared_simple_scalar_filtered_aggregate(statement.as_ref(), runtime);
        let bundle = PreparedPlanBundle {
            statement: Arc::clone(&statement),
            simple_row_id_projection,
            simple_row_id_range_projection,
            simple_row_id_join_projection,
            simple_scalar_filtered_aggregate,
            prepared_insert,
            prepared_update,
            prepared_delete,
            read_only,
        };
        if Self::statement_can_enter_plan_cache(bundle.statement.as_ref()) {
            if let Ok(mut cache) = self.inner.prepared_plan_cache.lock() {
                cache.insert(
                    key,
                    bundle.clone(),
                    Self::prepared_plan_accounted_size(&bundle),
                );
            }
        }
        Ok(PreparedStatement {
            db: self.clone(),
            schema_cookie: runtime.catalog.schema_cookie,
            temp_schema_cookie: runtime.temp_schema_cookie,
            statement: Arc::clone(&statement),
            prepared_sql: prepared_sql.clone(),
            simple_row_id_projection: bundle.simple_row_id_projection,
            simple_row_id_range_projection: bundle.simple_row_id_range_projection,
            simple_row_id_join_projection: bundle.simple_row_id_join_projection,
            simple_scalar_filtered_aggregate: bundle.simple_scalar_filtered_aggregate,
            prepared_insert: bundle.prepared_insert,
            prepared_update: bundle.prepared_update,
            prepared_delete: bundle.prepared_delete,
            read_only,
        })
    }

    fn prepared_simple_row_id_projection(
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

    fn prepared_simple_row_id_range_projection(
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

    fn prepared_simple_row_id_join_projection(
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

    fn prepared_simple_scalar_filtered_aggregate(
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

    fn prepared_simple_insert(
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

    fn prepared_plan_accounted_size(bundle: &PreparedPlanBundle) -> u64 {
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

    fn persist_runtime(&self, runtime: EngineRuntime) -> Result<u64> {
        self.persist_runtime_if_latest(runtime, None, true)
    }

    fn build_exclusive_sql_txn_state(&self) -> Result<ExclusiveSqlTxnState<'_>> {
        let (snapshot_reader, current_lsn, current_epoch) = self.begin_sql_snapshot()?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        Ok(ExclusiveSqlTxnState {
            runtime,
            snapshot_reader: Some(snapshot_reader),
            base_lsn: current_lsn,
            base_checkpoint_epoch: current_epoch,
            persistent_changed: false,
            indexes_maybe_stale: false,
            prepared_insert_runtime_cache: HashMap::new(),
        })
    }

    fn commit_exclusive_sql_txn(&self, mut state: ExclusiveSqlTxnState<'_>) -> Result<u64> {
        if !state.persistent_changed {
            self.sync_temp_state_from_runtime(&state.runtime)?;
            return Ok(state.base_lsn);
        }

        let runtime_schema_cookie = state.runtime.catalog.schema_cookie;
        if state.indexes_maybe_stale {
            state
                .runtime
                .rebuild_stale_indexes(self.inner.config.page_size)?;
        }
        let reactive_pending = self.take_reactive_pending_commit(&mut state.runtime);
        self.begin_write()?;
        if let Err(error) = state.runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut state.runtime)?;
            return Err(error);
        }
        drop(state.snapshot_reader.take());
        let committed_lsn = match self.commit_if_latest(state.base_lsn, state.base_checkpoint_epoch)
        {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut state.runtime)?;
                return Err(error);
            }
        };
        self.sync_post_commit(&mut state.runtime, committed_lsn)?;
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(state.runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&state.runtime)?;
        if self.should_redefer_paged_row_sources_after_write() {
            state.runtime.redefer_all_persisted_paged_tables();
            self.release_freed_heap_after_paged_row_source_drop();
        }
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(state);
        self.publish_reactive_commit(reactive_pending, committed_lsn);
        Ok(committed_lsn)
    }

    fn rollback_exclusive_sql_txn(&self, mut state: ExclusiveSqlTxnState<'_>) -> Result<()> {
        self.restore_runtime_from_storage(&mut state.runtime)
    }

    fn persist_runtime_if_latest(
        &self,
        runtime: EngineRuntime,
        expected_latest: Option<(u64, u64)>,
        rebuild_stale_indexes: bool,
    ) -> Result<u64> {
        let mut runtime = runtime;
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if rebuild_stale_indexes {
            runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        }
        let reactive_pending = self.take_reactive_pending_commit(&mut runtime);
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            return Err(error);
        }
        let committed_lsn = match expected_latest {
            Some((lsn, epoch)) => self.commit_if_latest(lsn, epoch),
            None => self.commit(),
        };
        let committed_lsn = match committed_lsn {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                return Err(error);
            }
        };
        self.sync_post_commit(&mut runtime, committed_lsn)?;
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        if self.should_redefer_paged_row_sources_after_write() {
            guard.redefer_all_persisted_paged_tables();
            self.release_freed_heap_after_paged_row_source_drop();
        }
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(guard);
        self.publish_reactive_commit(reactive_pending, committed_lsn);

        Ok(committed_lsn)
    }

    fn runtime_for_targeted_row_source_inspection(&self) -> Result<(EngineRuntime, Option<u64>)> {
        if let Some((runtime, snapshot_lsn)) = self.transaction_runtime_snapshot_with_lsn()? {
            return Ok((runtime, Some(snapshot_lsn)));
        }
        if !self.inner.config.defer_table_materialization {
            self.refresh_engine_from_storage()?;
            return Ok((self.engine_snapshot()?, None));
        }
        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        drop(reader);
        Ok((self.engine_snapshot()?, Some(snapshot_lsn)))
    }

    fn validate_watch_tables(&self, tables: &[String]) -> Result<BTreeSet<String>> {
        if tables.is_empty() {
            return Err(DbError::sql("watch table list must not be empty"));
        }
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let mut canonical = BTreeSet::new();
        for table in tables {
            let schema = runtime
                .catalog
                .table(table)
                .ok_or_else(|| DbError::sql(format!("unknown watch table {table}")))?;
            if schema.temporary || crate::sync::is_internal_table_name(&schema.name) {
                return Err(DbError::sql(format!(
                    "table {} is not watchable",
                    schema.name
                )));
            }
            canonical.insert(schema.name.clone());
        }
        Ok(canonical)
    }

    fn validate_watch_range_table(&self, table: &str) -> Result<String> {
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let schema = runtime
            .catalog
            .table(table)
            .ok_or_else(|| DbError::sql(format!("unknown watch table {table}")))?;
        if schema.temporary || crate::sync::is_internal_table_name(&schema.name) {
            return Err(DbError::sql(format!(
                "table {} is not watchable",
                schema.name
            )));
        }
        if schema.primary_key_columns.is_empty() {
            return Err(DbError::sql(format!(
                "range watch requires a primary key on table {}",
                schema.name
            )));
        }
        Ok(schema.name.clone())
    }

    fn query_watch_dependencies(
        &self,
        statement: &crate::sql::ast::Statement,
    ) -> Result<BTreeSet<String>> {
        let referenced = crate::sql::ast::safe_referenced_tables(statement)
            .ok_or_else(|| DbError::sql("query dependencies are not watchable for this SELECT"))?;
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let mut dependencies = BTreeSet::new();
        for name in referenced {
            if let Some(table) = runtime.catalog.table(&name) {
                if table.temporary || crate::sync::is_internal_table_name(&table.name) {
                    return Err(DbError::sql(format!(
                        "table {} is not watchable",
                        table.name
                    )));
                }
                dependencies.insert(table.name.clone());
            } else if let Some(view) = runtime.catalog.view(&name) {
                if view.temporary || crate::sync::is_internal_table_name(&view.name) {
                    return Err(DbError::sql(format!("view {} is not watchable", view.name)));
                }
                for dependency in &view.dependencies {
                    let table = runtime.catalog.table(dependency).ok_or_else(|| {
                        DbError::sql(format!(
                            "view {} depends on unknown table {}",
                            view.name, dependency
                        ))
                    })?;
                    if !table.temporary && !crate::sync::is_internal_table_name(&table.name) {
                        dependencies.insert(table.name.clone());
                    }
                }
            } else {
                return Err(DbError::sql(format!(
                    "query dependency {name} is not a watchable table or view"
                )));
            }
        }
        if dependencies.is_empty() {
            return Err(DbError::sql(
                "query subscription has no watchable table dependencies",
            ));
        }
        Ok(dependencies)
    }

    fn ensure_inspection_table_row_source(
        &self,
        runtime: &mut EngineRuntime,
        table_name: &str,
        snapshot_lsn: Option<u64>,
    ) -> Result<()> {
        if runtime.table_row_source(table_name).is_some()
            || runtime.temp_table_schema(table_name).is_some()
        {
            return Ok(());
        }
        let Some(snapshot_lsn) = snapshot_lsn else {
            return Ok(());
        };
        self.load_runtime_table_row_sources_at_snapshot(runtime, &[table_name], snapshot_lsn)
    }

    fn insert_dependency_table_names(
        &self,
        runtime: &EngineRuntime,
        table_name: &str,
    ) -> Result<Vec<String>> {
        let table = runtime
            .table_schema(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?;
        let mut names = vec![table_name.to_string()];
        for foreign_key in &table.foreign_keys {
            if !names
                .iter()
                .any(|name| identifiers_equal(name, &foreign_key.referenced_table))
            {
                names.push(foreign_key.referenced_table.clone());
            }
        }
        Ok(names)
    }

    fn redefer_inspection_table_row_source(
        &self,
        runtime: &mut EngineRuntime,
        table_name: &str,
        snapshot_lsn: Option<u64>,
    ) {
        if snapshot_lsn.is_some() && runtime.persisted_table_state(table_name).is_some() {
            runtime.redefer_persisted_tables(&[table_name]);
        }
    }

    fn runtime_for_metadata_inspection(&self) -> Result<EngineRuntime> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            return Ok(runtime);
        }
        self.refresh_engine_from_storage()?;
        self.engine_snapshot()
    }

    fn runtime_table_row_count(
        &self,
        runtime: &EngineRuntime,
        table_name: &str,
        snapshot_lsn: Option<u64>,
    ) -> Result<usize> {
        if let Some(table) = runtime.temp_table_schema(table_name) {
            return Ok(runtime
                .temp_table_data(&table.name)
                .map_or(0, |data| data.rows.len()));
        }

        if let Some(table) = runtime.catalog.table(table_name) {
            if let Some(stats) = runtime.catalog.table_stats.get(&table.name) {
                let row_count = usize::try_from(stats.row_count.max(0)).unwrap_or(usize::MAX);
                if row_count != 0 {
                    return Ok(row_count);
                }
            }
        }

        if let Some(source) = runtime.table_row_source(table_name) {
            return Ok(source.row_count());
        }

        let Some(state) = runtime.persisted_table_state(table_name) else {
            return Ok(0);
        };
        if state.row_count != 0 || state.pointer.head_page_id == 0 {
            return Ok(state.row_count);
        }

        let store = if let Some(lsn) = snapshot_lsn {
            PagerReadStore::with_snapshot_lsn(self, lsn)
        } else {
            PagerReadStore::new(self)?
        };
        let payload = read_overflow(&store, state.pointer)?;
        if state.pointer.is_table_paged_manifest() {
            let manifest = decode_paged_table_manifest_payload(&payload)?;
            Ok(manifest.chunks.iter().map(|chunk| chunk.row_count).sum())
        } else {
            read_table_payload_row_count_from_bytes(&payload)
        }
    }

    fn runtime_for_prepare(&self) -> Result<EngineRuntime> {
        if let Some(runtime) = self.transaction_runtime_snapshot_for_prepare()? {
            return Ok(runtime);
        }
        self.refresh_engine_from_storage()?;
        // ADR 0143 Phase B: prepare() only needs catalog/schema metadata
        // to plan a statement. Skip the eager all-tables materialization
        // so applications that prepare a large number of statements at
        // startup don't fault every persisted table into memory just to
        // get a `PreparedStatement` handle. Row data is loaded on first
        // execution by the read/write paths.
        self.engine_snapshot_without_index_rebuild()
    }

    fn transaction_runtime_snapshot_for_prepare(&self) -> Result<Option<EngineRuntime>> {
        if !self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = match &*txn {
            SqlTxnSlot::Shared(state) => state,
            SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => return Ok(None),
        };
        Ok(Some(state.runtime.clone()))
    }

    fn transaction_runtime_snapshot(&self) -> Result<Option<EngineRuntime>> {
        self.transaction_runtime_snapshot_with_lsn()
            .map(|maybe| maybe.map(|(runtime, _)| runtime))
    }

    fn transaction_runtime_snapshot_with_lsn(&self) -> Result<Option<(EngineRuntime, u64)>> {
        if !self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        let state = match &*txn {
            SqlTxnSlot::Shared(state) => state,
            SqlTxnSlot::Exclusive => return Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => return Ok(None),
        };

        let mut snapshot = state.runtime.clone();
        if state.indexes_maybe_stale {
            snapshot.rebuild_stale_indexes(self.inner.config.page_size)?;
        }
        Ok(Some((snapshot, state.snapshot_lsn())))
    }

    fn restore_runtime_from_storage(&self, runtime: &mut EngineRuntime) -> Result<()> {
        let schema_cookie = self.current_schema_cookie()?;
        let (mut restored, restored_lsn) = EngineRuntime::load_from_storage(
            &self.inner.pager,
            &self.inner.wal,
            schema_cookie,
            &self.inner.config,
        )?;
        restored.set_audit_context_handle(Arc::clone(&self.inner.audit_context));
        self.apply_temp_state_to_runtime(&mut restored)?;
        self.inner
            .catalog
            .replace(restored.catalog.as_ref().clone())?;
        *runtime = restored;
        self.inner
            .last_runtime_lsn
            .store(restored_lsn, Ordering::Release);
        Ok(())
    }

    fn refresh_engine_from_snapshot(&self, snapshot_lsn: u64) -> Result<()> {
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let mut last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let writer_last_commit_lsn = self.inner.writer_last_commit_lsn.load(Ordering::Acquire);
        let last_explicit_checkpoint_epoch = self
            .inner
            .last_explicit_checkpoint_epoch
            .load(Ordering::Acquire);
        let mut checkpoint_lsn_after_refresh = None;
        if latest_checkpoint_epoch != last_seen_checkpoint_epoch {
            let cached_header = self.inner.pager.header_snapshot()?;
            let on_disk_header = self.inner.pager.header_from_disk()?;
            checkpoint_lsn_after_refresh = Some(on_disk_header.last_checkpoint_lsn);
            if on_disk_header.last_checkpoint_lsn != cached_header.last_checkpoint_lsn {
                self.inner.pager.refresh_from_disk(on_disk_header)?;
            }
            self.inner
                .last_seen_checkpoint_epoch
                .store(latest_checkpoint_epoch, Ordering::Release);
            last_seen_checkpoint_epoch = latest_checkpoint_epoch;
        }
        if snapshot_lsn == last_runtime_lsn && latest_checkpoint_epoch == last_seen_checkpoint_epoch
        {
            return Ok(());
        }

        if last_runtime_lsn > 0
            && writer_last_commit_lsn > 0
            && last_runtime_lsn >= writer_last_commit_lsn
            && snapshot_lsn == 0
            && last_explicit_checkpoint_epoch == latest_checkpoint_epoch
            && checkpoint_lsn_after_refresh.is_some_and(|checkpoint_lsn| {
                checkpoint_lsn == last_runtime_lsn && checkpoint_lsn >= writer_last_commit_lsn
            })
        {
            // An explicit checkpoint from this handle can fold exactly the
            // current runtime into the database file and reset the live WAL
            // end to 0. Only preserve the hot runtime before any post-
            // checkpoint WAL frames exist; otherwise the runtime would no
            // longer match the pinned snapshot.
            self.inner
                .last_runtime_lsn
                .store(snapshot_lsn, Ordering::Release);
            return Ok(());
        }

        let schema_cookie = self.current_schema_cookie_at_snapshot(snapshot_lsn)?;
        let mut runtime = EngineRuntime::load_from_storage_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            schema_cookie,
            &self.inner.config,
            snapshot_lsn,
        )?;
        self.apply_temp_state_to_runtime(&mut runtime)?;
        self.inner
            .catalog
            .replace(runtime.catalog.as_ref().clone())?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        self.inner
            .last_runtime_lsn
            .store(snapshot_lsn, Ordering::Release);
        self.inner
            .last_seen_checkpoint_epoch
            .store(latest_checkpoint_epoch, Ordering::Release);
        Ok(())
    }

    fn refresh_engine_from_storage(&self) -> Result<()> {
        self.inner
            .wal
            .refresh_from_coordination(&self.inner.pager)?;
        let latest_lsn = self.inner.wal.latest_snapshot();
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        let writer_last_commit_lsn = self.inner.writer_last_commit_lsn.load(Ordering::Acquire);

        if latest_lsn == last_runtime_lsn && latest_checkpoint_epoch == last_seen_checkpoint_epoch {
            return Ok(());
        }

        let mut checkpoint_lsn_after_refresh = None;
        if latest_checkpoint_epoch != last_seen_checkpoint_epoch {
            let cached_header = self.inner.pager.header_snapshot()?;
            let on_disk_header = self.inner.pager.header_from_disk()?;
            checkpoint_lsn_after_refresh = Some(on_disk_header.last_checkpoint_lsn);
            if on_disk_header.last_checkpoint_lsn != cached_header.last_checkpoint_lsn {
                self.inner.pager.refresh_from_disk(on_disk_header)?;
            }
            self.inner
                .last_seen_checkpoint_epoch
                .store(latest_checkpoint_epoch, Ordering::Release);
        }

        let last_explicit_checkpoint_epoch = self
            .inner
            .last_explicit_checkpoint_epoch
            .load(Ordering::Acquire);
        if last_runtime_lsn > 0
            && writer_last_commit_lsn > 0
            && last_runtime_lsn >= writer_last_commit_lsn
            && latest_lsn == 0
            && last_explicit_checkpoint_epoch == latest_checkpoint_epoch
            && checkpoint_lsn_after_refresh.is_some_and(|checkpoint_lsn| {
                checkpoint_lsn == last_runtime_lsn && checkpoint_lsn >= writer_last_commit_lsn
            })
        {
            // An explicit checkpoint from this handle can fold exactly the
            // current runtime into the database file and reset the live WAL
            // end to 0. Only preserve the runtime before any post-checkpoint
            // WAL frames exist; lower nonzero LSNs after WAL reuse must reload.
            self.inner
                .last_runtime_lsn
                .store(latest_lsn, Ordering::Release);
            return Ok(());
        }

        let schema_cookie = self.current_schema_cookie()?;
        let (mut runtime, runtime_lsn) = EngineRuntime::load_from_storage(
            &self.inner.pager,
            &self.inner.wal,
            schema_cookie,
            &self.inner.config,
        )?;
        runtime.set_audit_context_handle(Arc::clone(&self.inner.audit_context));
        self.apply_temp_state_to_runtime(&mut runtime)?;
        self.inner
            .catalog
            .replace(runtime.catalog.as_ref().clone())?;
        let mut guard = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        *guard = runtime;
        self.inner
            .last_runtime_lsn
            .store(runtime_lsn, Ordering::Release);
        Ok(())
    }

    fn refresh_and_ensure_all_tables_loaded(&self) -> Result<()> {
        if !self.inner.config.defer_table_materialization {
            self.refresh_engine_from_storage()?;
            self.ensure_all_tables_loaded()?;
            return Ok(());
        }

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        self.ensure_all_tables_loaded_at_snapshot(Some(snapshot_lsn))?;
        drop(reader);
        Ok(())
    }

    fn refresh_and_load_tables_for_statement_at_latest_snapshot(
        &self,
        statement: &SqlStatement,
    ) -> Result<()> {
        if !self.inner.config.defer_table_materialization {
            self.refresh_engine_from_storage()?;
            self.ensure_all_tables_loaded()?;
            return Ok(());
        }

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        let targeted_ok =
            self.ensure_tables_loaded_for_statement_at_snapshot(statement, Some(snapshot_lsn))?;
        if !targeted_ok {
            self.ensure_all_tables_loaded_at_snapshot(Some(snapshot_lsn))?;
        }
        drop(reader);
        Ok(())
    }

    /// Materializes deferred tables specified by name.
    ///
    /// Fast path (no matching deferred tables): one read-lock check.
    /// Slow path: drops the read lock, takes a write lock, loads only the
    /// specified tables and rebuilds their indexes, then releases.
    ///
    /// This enables per-table on-demand loading for ADR 0143 Phase B.
    fn ensure_tables_loaded_at_snapshot(
        &self,
        names: &[&str],
        snapshot_lsn: Option<u64>,
    ) -> Result<()> {
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            let has_deferred = runtime.has_deferred_tables();
            let has_match = names.iter().any(|name| {
                runtime
                    .deferred_table_names()
                    .any(|dt| dt.eq_ignore_ascii_case(name))
            });
            if !has_deferred || !has_match {
                return Ok(());
            }
        }
        let filter: BTreeSet<String> = names.iter().map(|s| s.to_string()).collect();
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        if let Some(snapshot_lsn) = snapshot_lsn {
            if self.inner.config.paged_row_storage {
                runtime.load_deferred_table_row_sources_filtered_at_snapshot(
                    &self.inner.pager,
                    &self.inner.wal,
                    self.inner.config.page_size,
                    &filter,
                    snapshot_lsn,
                )
            } else {
                runtime.load_deferred_tables_filtered_at_snapshot(
                    &self.inner.pager,
                    &self.inner.wal,
                    self.inner.config.page_size,
                    &filter,
                    snapshot_lsn,
                )
            }
        } else if self.inner.config.paged_row_storage {
            runtime.load_deferred_table_row_sources_filtered(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                &filter,
            )
        } else {
            runtime.load_deferred_tables_filtered(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                &filter,
            )
        }
    }

    fn security_catalog_table_names() -> [&'static str; 2] {
        [
            crate::security::POLICIES_TABLE,
            crate::security::MASKS_TABLE,
        ]
    }

    fn runtime_has_deferred_security_tables(runtime: &EngineRuntime) -> bool {
        runtime.has_deferred_tables()
            && Self::security_catalog_table_names().iter().any(|name| {
                runtime
                    .deferred_table_names()
                    .any(|candidate| candidate.eq_ignore_ascii_case(name))
            })
    }

    fn runtime_read_for_fast_read_at_snapshot(
        &self,
        snapshot_lsn: u64,
    ) -> Result<Option<RwLockReadGuard<'_, EngineRuntime>>> {
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        if Self::runtime_has_deferred_security_tables(&runtime) {
            drop(runtime);
            self.ensure_tables_loaded_at_snapshot(
                &Self::security_catalog_table_names(),
                Some(snapshot_lsn),
            )?;
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            if runtime.security_rules_active()? {
                return Ok(None);
            }
            return Ok(Some(runtime));
        }
        if runtime.security_rules_active()? {
            return Ok(None);
        }
        Ok(Some(runtime))
    }

    fn ensure_security_tables_loaded_at_snapshot(&self, snapshot_lsn: u64) -> Result<bool> {
        self.ensure_tables_loaded_at_snapshot(
            &Self::security_catalog_table_names(),
            Some(snapshot_lsn),
        )?;
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        runtime.security_rules_active()
    }

    fn load_security_tables_for_runtime_at_snapshot(
        &self,
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
    ) -> Result<bool> {
        self.load_runtime_table_row_sources_at_snapshot(
            runtime,
            &Self::security_catalog_table_names(),
            snapshot_lsn,
        )?;
        runtime.security_rules_active()
    }

    fn ensure_table_row_sources_loaded_at_snapshot(
        &self,
        names: &[&str],
        snapshot_lsn: u64,
    ) -> Result<()> {
        let filter: BTreeSet<String> = names.iter().map(|s| s.to_string()).collect();
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            let has_deferred = runtime.has_deferred_tables();
            let has_match = names.iter().any(|name| {
                runtime
                    .deferred_table_names()
                    .any(|dt| dt.eq_ignore_ascii_case(name))
            });
            if !has_deferred || !has_match {
                return Ok(());
            }
        }
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        runtime.load_deferred_table_row_sources_filtered_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            self.inner.config.page_size,
            &filter,
            snapshot_lsn,
        )
    }

    fn hydrate_deferred_runtime_index_at_snapshot(
        &self,
        table_name: &str,
        index_name: &str,
        snapshot_lsn: u64,
    ) -> Result<()> {
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            let has_deferred = runtime.has_deferred_tables();
            let has_match = runtime
                .deferred_table_names()
                .any(|deferred| identifiers_equal(deferred, table_name));
            if !has_deferred || !has_match {
                return Ok(());
            }
        }
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        runtime.hydrate_deferred_runtime_index_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            self.inner.config.page_size,
            table_name,
            index_name,
            snapshot_lsn,
        )
    }

    fn prepared_read_row_source_row_limit(&self) -> usize {
        self.inner
            .config
            .cache_size_mb
            .saturating_mul(PREPARED_READ_ROW_SOURCE_ROWS_PER_CACHE_MB)
    }

    fn try_load_prepared_read_row_sources_at_snapshot(
        &self,
        names: &[&str],
        snapshot_lsn: u64,
    ) -> Result<()> {
        if names.is_empty()
            || !self.inner.config.defer_table_materialization
            || !self.inner.config.paged_row_storage
        {
            return Ok(());
        }

        let row_limit = self.prepared_read_row_source_row_limit();
        if row_limit == 0 {
            return Ok(());
        }

        let mut to_load = BTreeSet::new();
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            for name in names {
                let Some(table_name) = runtime.canonical_catalog_table_name(name) else {
                    continue;
                };
                if runtime.table_row_source(&table_name).is_some() {
                    continue;
                }
                let Some(state) = runtime.persisted_tables.get(&table_name).copied() else {
                    continue;
                };
                if !runtime.deferred_tables.contains(&table_name) {
                    continue;
                }
                if !state.pointer.is_table_paged_manifest()
                    || state.row_count < PREPARED_READ_ROW_SOURCE_MIN_ROWS
                    || state.row_count > row_limit
                {
                    return Ok(());
                }
                to_load.insert(table_name);
            }
        }

        if to_load.is_empty() {
            return Ok(());
        }

        {
            let mut runtime = self
                .inner
                .engine
                .write()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            runtime.load_deferred_table_row_sources_filtered_at_snapshot(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                &to_load,
                snapshot_lsn,
            )?;
        }

        let loaded_refs = to_load.iter().map(String::as_str).collect::<Vec<_>>();
        self.touch_read_only_paged_row_sources_by_name(&loaded_refs)
    }

    fn touch_read_only_paged_row_sources_by_name(&self, names: &[&str]) -> Result<()> {
        if names.is_empty()
            || !self.inner.config.defer_table_materialization
            || !self.inner.config.paged_row_storage
        {
            return Ok(());
        }

        let (touched_tables, all_paged_tables) = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            let mut touched_tables = BTreeSet::new();
            let mut all_paged_tables = BTreeSet::new();
            for (name, state) in runtime.persisted_tables.iter() {
                if state.pointer.is_table_paged_manifest() {
                    all_paged_tables.insert(name.clone());
                }
            }
            for name in names {
                let Some(table_name) = runtime.canonical_catalog_table_name(name) else {
                    continue;
                };
                if all_paged_tables.contains(&table_name)
                    && runtime.table_row_source(&table_name).is_some()
                {
                    touched_tables.insert(table_name);
                }
            }
            (touched_tables, all_paged_tables)
        };

        if touched_tables.is_empty() {
            return Ok(());
        }

        let mut to_redefer: Vec<String> = Vec::new();
        {
            let mut residency = self
                .inner
                .read_only_paged_row_source_residency
                .lock()
                .map_err(|_| {
                    DbError::internal("read-only paged row source residency lock poisoned")
                })?;
            residency
                .table_touch_generation
                .retain(|name, _| all_paged_tables.contains(name));
            let touch_gen = residency.next_touch_gen;
            residency.next_touch_gen = residency.next_touch_gen.saturating_add(1);
            for table_name in touched_tables {
                residency
                    .table_touch_generation
                    .insert(table_name, touch_gen);
            }
            if residency.table_touch_generation.len() > AUTOCOMMIT_PAGED_ROW_SOURCE_MAX_RESIDENT {
                let mut ordered_touch = residency
                    .table_touch_generation
                    .iter()
                    .map(|(name, generation)| (name, *generation))
                    .collect::<Vec<_>>();
                ordered_touch.sort_by_key(|(_, generation)| *generation);
                let overflow = ordered_touch.len() - AUTOCOMMIT_PAGED_ROW_SOURCE_MAX_RESIDENT;
                to_redefer.reserve(overflow);
                for (name, _) in ordered_touch.iter().take(overflow) {
                    to_redefer.push((*name).clone());
                }
                for name in &to_redefer {
                    residency.table_touch_generation.remove(name);
                }
            }
        }

        if to_redefer.is_empty() {
            Ok(())
        } else {
            let redefer_refs = to_redefer.iter().map(String::as_str).collect::<Vec<_>>();
            self.redefer_persisted_tables(&redefer_refs)
        }
    }

    /// Fast path for non-transactional reads when deferred materialization is
    /// enabled but the statement's base tables are already resident at the
    /// pinned reader snapshot.
    ///
    /// Returns a read guard over the resident runtime when the statement can
    /// be executed without reloading row sources.
    /// Returns `Ok(None)` when any referenced base table is not resident, the
    /// runtime LSN is stale, a checkpoint has advanced, or the statement's
    /// base-table set cannot be resolved (callers fall back to the deferred
    /// load path in that case).
    fn try_resident_read_for_statement_at_snapshot(
        &self,
        statement: &SqlStatement,
        prepared: Option<&PreparedStatement>,
        snapshot_lsn: u64,
    ) -> Result<Option<RwLockReadGuard<'_, EngineRuntime>>> {
        if !self.inner.config.defer_table_materialization {
            return Ok(None);
        }
        let checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        if last_runtime_lsn != snapshot_lsn || last_seen_checkpoint_epoch != checkpoint_epoch {
            return Ok(None);
        }
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let current_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let current_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        if current_runtime_lsn != snapshot_lsn || current_seen_checkpoint_epoch != checkpoint_epoch
        {
            return Ok(None);
        }
        self.validate_prepared_against_runtime(prepared, &runtime)?;
        if Self::runtime_has_deferred_security_tables(&runtime)
            || runtime.security_rules_active()?
        {
            return Ok(None);
        }
        let Some(base_tables) = self.safe_referenced_base_tables_in_runtime(&runtime, statement)
        else {
            return Ok(None);
        };
        if base_tables.is_empty() {
            return Ok(Some(runtime));
        }
        let all_resident = base_tables.iter().all(|name| {
            runtime
                .canonical_catalog_table_name(name)
                .is_some_and(|table_name| runtime.table_row_source(&table_name).is_some())
        });
        if all_resident {
            Ok(Some(runtime))
        } else {
            Ok(None)
        }
    }

    fn runtime_read_for_prepared_row_sources_at_snapshot(
        &self,
        names: &[&str],
        snapshot_lsn: u64,
    ) -> Result<Option<RwLockReadGuard<'_, EngineRuntime>>> {
        if names.is_empty() {
            return Ok(None);
        }
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        if last_runtime_lsn != snapshot_lsn
            || last_seen_checkpoint_epoch != latest_checkpoint_epoch
            || names.iter().any(|name| {
                runtime
                    .canonical_catalog_table_name(name)
                    .is_none_or(|table_name| runtime.table_row_source(&table_name).is_none())
            })
        {
            return Ok(None);
        }
        Ok(Some(runtime))
    }

    fn load_simple_write_row_sources_at_latest_snapshot(&self, names: &[&str]) -> Result<()> {
        if !self.inner.config.defer_table_materialization {
            self.refresh_engine_from_storage()?;
            self.ensure_tables_loaded_at_snapshot(names, None)?;
            return Ok(());
        }

        if self.simple_write_row_sources_loaded_for_current_runtime(names)? {
            return Ok(());
        }

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        self.ensure_table_row_sources_loaded_at_snapshot(names, snapshot_lsn)?;
        drop(reader);
        Ok(())
    }

    fn simple_write_row_sources_loaded_for_current_runtime(&self, names: &[&str]) -> Result<bool> {
        let latest_lsn = self.inner.wal.latest_snapshot();
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);

        if latest_lsn > last_runtime_lsn || latest_checkpoint_epoch != last_seen_checkpoint_epoch {
            return Ok(false);
        }

        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let has_deferred_match = names.iter().any(|name| {
            runtime
                .deferred_table_names()
                .any(|deferred| identifiers_equal(deferred, name))
        });
        Ok(!has_deferred_match)
    }

    fn load_statement_row_sources_at_latest_snapshot(
        &self,
        statement: &SqlStatement,
    ) -> Result<bool> {
        if !self.inner.config.defer_table_materialization {
            return self.ensure_tables_loaded_for_statement_at_snapshot(statement, None);
        }

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let Some(base_tables) = self.safe_referenced_base_tables_in_runtime(&runtime, statement)
        else {
            drop(reader);
            return Ok(false);
        };
        if base_tables.is_empty() {
            drop(reader);
            return Ok(true);
        }
        let base_refs: Vec<&str> = base_tables.iter().map(String::as_str).collect();
        self.load_runtime_table_row_sources_at_snapshot(&mut runtime, &base_refs, snapshot_lsn)?;
        drop(reader);
        Ok(true)
    }

    fn can_execute_statement_with_row_sources_at_latest_snapshot(
        &self,
        statement: &SqlStatement,
    ) -> Result<bool> {
        if !self.inner.config.defer_table_materialization {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            return Ok(runtime.can_execute_statement_in_state_without_clone(statement));
        }

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let Some(base_tables) = self.safe_referenced_base_tables_in_runtime(&runtime, statement)
        else {
            drop(reader);
            return Ok(false);
        };
        let mut working = runtime.clone();
        drop(runtime);
        let base_refs: Vec<&str> = base_tables.iter().map(String::as_str).collect();
        self.load_runtime_table_row_sources_at_snapshot(&mut working, &base_refs, snapshot_lsn)?;
        drop(reader);
        Ok(working.can_execute_statement_in_state_without_clone(statement))
    }

    fn redefer_persisted_tables(&self, names: &[&str]) -> Result<()> {
        self.redefer_persisted_tables_inner(names, true)
    }

    fn redefer_persisted_tables_inner(
        &self,
        names: &[&str],
        release_heap_after_drop: bool,
    ) -> Result<()> {
        if !self.inner.config.defer_table_materialization || names.is_empty() {
            return Ok(());
        }
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        runtime.redefer_persisted_tables(names);
        drop(runtime);
        if release_heap_after_drop {
            self.release_freed_heap_after_paged_row_source_drop();
        }
        Ok(())
    }

    fn should_redefer_paged_row_sources_after_write(&self) -> bool {
        self.inner.config.defer_table_materialization
            && self.inner.config.paged_row_storage
            && !self.inner.config.retain_paged_row_sources_after_commit
    }

    fn runtime_should_redefer_persisted_tables_after_write(
        &self,
        runtime: &EngineRuntime,
        names: &[&str],
    ) -> bool {
        self.should_redefer_paged_row_sources_after_write()
            && runtime.has_redeferable_persisted_tables(names)
    }

    fn redefer_persisted_tables_after_write(&self, names: &[&str]) -> Result<()> {
        if self.should_redefer_paged_row_sources_after_write() {
            self.redefer_persisted_tables_inner(names, false)
        } else {
            Ok(())
        }
    }

    fn redefer_all_persisted_paged_tables(&self) -> Result<()> {
        if !self.inner.config.defer_table_materialization || !self.inner.config.paged_row_storage {
            return Ok(());
        }
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        runtime.redefer_all_persisted_paged_tables();
        drop(runtime);
        self.release_freed_heap_after_paged_row_source_drop();
        Ok(())
    }

    fn redefer_read_only_row_sources(
        &self,
        statement: &SqlStatement,
        allow_redefer_all_on_unknown: bool,
    ) -> Result<()> {
        if !self.inner.config.defer_table_materialization || !self.inner.config.paged_row_storage {
            return Ok(());
        }
        let (touched_tables, all_paged_tables) = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            let base_tables = match self.safe_referenced_base_tables_in_runtime(&runtime, statement)
            {
                Some(base_tables) => base_tables,
                None => {
                    drop(runtime);
                    return if allow_redefer_all_on_unknown {
                        self.redefer_all_persisted_paged_tables()
                    } else {
                        Ok(())
                    };
                }
            };
            if base_tables.is_empty() {
                return Ok(());
            }
            let mut touched_tables = BTreeSet::new();
            let mut all_paged_tables = BTreeSet::new();
            for (name, state) in runtime.persisted_tables.iter() {
                if state.pointer.is_table_paged_manifest() {
                    all_paged_tables.insert(name.clone());
                }
            }
            for base_table in base_tables {
                if let Some(table_name) = runtime.canonical_catalog_table_name(&base_table) {
                    if runtime
                        .persisted_tables
                        .get(&table_name)
                        .is_some_and(|state| state.pointer.is_table_paged_manifest())
                    {
                        touched_tables.insert(table_name);
                    }
                }
            }
            (touched_tables, all_paged_tables)
        };
        if touched_tables.is_empty() {
            if allow_redefer_all_on_unknown {
                self.redefer_all_persisted_paged_tables()
            } else {
                Ok(())
            }
        } else {
            let mut to_redefer: Vec<String> = Vec::new();
            {
                let mut residency = self
                    .inner
                    .read_only_paged_row_source_residency
                    .lock()
                    .map_err(|_| {
                        DbError::internal("read-only paged row source residency lock poisoned")
                    })?;
                residency
                    .table_touch_generation
                    .retain(|name, _| all_paged_tables.contains(name));
                let touch_gen = residency.next_touch_gen;
                residency.next_touch_gen = residency.next_touch_gen.saturating_add(1);
                for table_name in touched_tables {
                    residency
                        .table_touch_generation
                        .insert(table_name, touch_gen);
                }
                if residency.table_touch_generation.len() > AUTOCOMMIT_PAGED_ROW_SOURCE_MAX_RESIDENT
                {
                    let mut ordered_touch = residency
                        .table_touch_generation
                        .iter()
                        .map(|(name, generation)| (name, *generation))
                        .collect::<Vec<_>>();
                    ordered_touch.sort_by_key(|(_, generation)| *generation);
                    let overflow = ordered_touch.len() - AUTOCOMMIT_PAGED_ROW_SOURCE_MAX_RESIDENT;
                    to_redefer.reserve(overflow);
                    for (name, _) in ordered_touch.iter().take(overflow) {
                        to_redefer.push((*name).clone());
                    }
                    for name in &to_redefer {
                        residency.table_touch_generation.remove(name);
                    }
                }
            }
            if to_redefer.is_empty() {
                Ok(())
            } else {
                let redefer_refs = to_redefer.iter().map(String::as_str).collect::<Vec<_>>();
                self.redefer_persisted_tables(&redefer_refs)
            }
        }
    }

    fn release_freed_heap_after_paged_row_source_drop(&self) {
        if self.inner.config.paged_row_storage {
            crate::wal::platform::release_freed_heap();
        }
    }

    fn redefer_statement_tables(&self, statement: &SqlStatement) -> Result<()> {
        if !self.should_redefer_paged_row_sources_after_write() {
            return Ok(());
        }
        let names = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            let Some(base_tables) =
                self.safe_referenced_base_tables_in_runtime(&runtime, statement)
            else {
                return Ok(());
            };
            base_tables
        };
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        self.redefer_persisted_tables_after_write(&name_refs)
    }

    fn finalize_row_source_autocommit_statement(
        &self,
        statement: &SqlStatement,
        result: Result<QueryResult>,
    ) -> Result<QueryResult> {
        let redefer_result = if statement_is_read_only(statement) {
            self.redefer_read_only_row_sources(statement, false)
        } else {
            self.redefer_statement_tables(statement)
        };
        match (result, redefer_result) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(_)) => Err(error),
        }
    }

    fn finalize_row_source_autocommit_statement_with_full_redefer(
        &self,
        statement: &SqlStatement,
        result: Result<QueryResult>,
    ) -> Result<QueryResult> {
        let redefer_result = if statement_is_read_only(statement) {
            self.redefer_read_only_row_sources(statement, true)
        } else if self.should_redefer_paged_row_sources_after_write() {
            self.redefer_all_persisted_paged_tables()
        } else {
            Ok(())
        };
        match (result, redefer_result) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(_)) => Err(error),
        }
    }

    fn begin_sql_snapshot(&self) -> Result<(ReaderGuard, u64, u64)> {
        #[cfg(feature = "bench-internals")]
        READ_PATH_WAL_READER_BEGIN_COUNT.fetch_add(1, Ordering::Relaxed);
        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        let checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        Ok((reader, snapshot_lsn, checkpoint_epoch))
    }

    fn load_runtime_table_row_sources_at_snapshot(
        &self,
        runtime: &mut EngineRuntime,
        names: &[&str],
        snapshot_lsn: u64,
    ) -> Result<()> {
        if names.is_empty() || !runtime.has_deferred_tables() {
            return Ok(());
        }
        let has_match = names.iter().any(|name| {
            runtime
                .deferred_table_names()
                .any(|deferred| deferred.eq_ignore_ascii_case(name))
        });
        if !has_match {
            return Ok(());
        }
        let filter: BTreeSet<String> = names.iter().map(|name| (*name).to_string()).collect();
        runtime.load_deferred_table_row_sources_filtered_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            self.inner.config.page_size,
            &filter,
            snapshot_lsn,
        )
    }

    fn load_all_runtime_row_sources_at_snapshot(
        &self,
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
    ) -> Result<()> {
        let table_names = runtime.deferred_table_names().cloned().collect::<Vec<_>>();
        let table_refs = table_names.iter().map(String::as_str).collect::<Vec<_>>();
        self.load_runtime_table_row_sources_at_snapshot(runtime, &table_refs, snapshot_lsn)
    }

    fn try_execute_query_with_row_sources_at_snapshot(
        &self,
        runtime: &EngineRuntime,
        statement: &SqlStatement,
        params: &[Value],
        snapshot_lsn: u64,
        rebuild_stale_indexes: bool,
    ) -> Result<Option<QueryResult>> {
        let SqlStatement::Query(_) = statement else {
            return Ok(None);
        };
        let Some(base_tables) = self.safe_referenced_base_tables_in_runtime(runtime, statement)
        else {
            return Ok(None);
        };
        let mut working = runtime.clone();
        let base_refs: Vec<&str> = base_tables.iter().map(String::as_str).collect();
        self.load_runtime_table_row_sources_at_snapshot(&mut working, &base_refs, snapshot_lsn)?;
        if rebuild_stale_indexes {
            working.rebuild_stale_indexes(self.inner.config.page_size)?;
        }
        let result = working.execute_read_statement(statement, params, self.inner.config.page_size);
        drop(working);
        self.release_freed_heap_after_paged_row_source_drop();
        Ok(Some(result?))
    }

    fn safe_referenced_base_tables_in_runtime(
        &self,
        runtime: &EngineRuntime,
        statement: &SqlStatement,
    ) -> Option<Vec<String>> {
        let mut visited_triggers = BTreeSet::new();
        self.collect_safe_referenced_base_tables_in_runtime(
            runtime,
            statement,
            &mut visited_triggers,
        )
    }

    fn collect_safe_referenced_base_tables_in_runtime(
        &self,
        runtime: &EngineRuntime,
        statement: &SqlStatement,
        visited_triggers: &mut BTreeSet<String>,
    ) -> Option<Vec<String>> {
        use crate::sql::ast::safe_referenced_tables;

        let tables = safe_referenced_tables(statement)?;
        let mut base_tables = Vec::new();
        for name in tables {
            let is_base = runtime
                .catalog
                .tables
                .keys()
                .any(|entry| entry.eq_ignore_ascii_case(&name));
            let is_temp = runtime
                .temp_tables
                .keys()
                .any(|entry| entry.eq_ignore_ascii_case(&name));
            if !is_base && !is_temp {
                return None;
            }
            if is_base {
                base_tables.push(name);
            }
        }
        if let SqlStatement::Delete(delete) = statement {
            for child in runtime.delete_row_source_dependency_tables(delete)? {
                if base_tables
                    .iter()
                    .any(|entry| entry.eq_ignore_ascii_case(&child))
                {
                    continue;
                }
                base_tables.push(child);
            }
        }
        if let SqlStatement::Insert(insert) = statement {
            for child in runtime.insert_row_source_dependency_tables(insert)? {
                if base_tables
                    .iter()
                    .any(|entry| entry.eq_ignore_ascii_case(&child))
                {
                    continue;
                }
                base_tables.push(child);
            }
        }
        if let SqlStatement::Update(update) = statement {
            for child in runtime.update_row_source_dependency_tables(update)? {
                if base_tables
                    .iter()
                    .any(|entry| entry.eq_ignore_ascii_case(&child))
                {
                    continue;
                }
                base_tables.push(child);
            }
        }
        self.append_trigger_dependency_tables(
            runtime,
            statement,
            &mut base_tables,
            visited_triggers,
        )?;
        Some(base_tables)
    }

    fn append_trigger_dependency_tables(
        &self,
        runtime: &EngineRuntime,
        statement: &SqlStatement,
        base_tables: &mut Vec<String>,
        visited_triggers: &mut BTreeSet<String>,
    ) -> Option<()> {
        let (target_name, event) = match statement {
            SqlStatement::Insert(insert) => (insert.table_name.as_str(), TriggerEvent::Insert),
            SqlStatement::Update(update) => (update.table_name.as_str(), TriggerEvent::Update),
            SqlStatement::Delete(delete) => (delete.table_name.as_str(), TriggerEvent::Delete),
            _ => return Some(()),
        };
        for trigger in runtime.catalog.triggers.values() {
            if trigger.on_view
                || trigger.event != event
                || !identifiers_equal(&trigger.target_name, target_name)
                || !visited_triggers.insert(trigger.name.clone())
            {
                continue;
            }
            let trigger_statement = parse_sql_statement(&trigger.action_sql).ok()?;
            for table in self.collect_safe_referenced_base_tables_in_runtime(
                runtime,
                &trigger_statement,
                visited_triggers,
            )? {
                if base_tables
                    .iter()
                    .any(|entry| entry.eq_ignore_ascii_case(&table))
                {
                    continue;
                }
                base_tables.push(table);
            }
        }
        Some(())
    }

    fn try_execute_indexed_join_grouped_count_query_at_snapshot(
        &self,
        runtime: &EngineRuntime,
        query: &crate::sql::ast::Query,
        params: &[Value],
        snapshot_lsn: u64,
    ) -> Result<Option<QueryResult>> {
        if !runtime.has_deferred_tables() {
            return Ok(None);
        }
        let Some(parent_table_name) =
            runtime.indexed_join_grouped_count_parent_table_name(query, params)?
        else {
            return Ok(None);
        };

        let parent_table_name = parent_table_name.to_string();
        let mut join_runtime = runtime.clone();
        self.load_runtime_table_row_sources_at_snapshot(
            &mut join_runtime,
            &[parent_table_name.as_str()],
            snapshot_lsn,
        )?;
        let result = join_runtime.try_execute_indexed_join_grouped_count_query(query, params);
        drop(join_runtime);
        self.release_freed_heap_after_paged_row_source_drop();
        result
    }

    fn try_execute_simple_indexed_join_projection_query_at_snapshot(
        &self,
        runtime: &EngineRuntime,
        statement: &SqlStatement,
        query: &crate::sql::ast::Query,
        params: &[Value],
        snapshot_lsn: u64,
    ) -> Result<Option<QueryResult>> {
        if !runtime.has_deferred_tables() {
            return Ok(None);
        }
        let Some(base_tables) = self.safe_referenced_base_tables_in_runtime(runtime, statement)
        else {
            return Ok(None);
        };
        if base_tables.is_empty() {
            return Ok(None);
        }
        let mut join_runtime = runtime.clone();
        let base_refs: Vec<&str> = base_tables.iter().map(String::as_str).collect();
        self.load_runtime_table_row_sources_at_snapshot(
            &mut join_runtime,
            &base_refs,
            snapshot_lsn,
        )?;
        let result = join_runtime.try_execute_simple_indexed_join_projection_query(query, params);
        drop(join_runtime);
        self.release_freed_heap_after_paged_row_source_drop();
        result
    }

    fn ensure_runtime_tables_loaded_at_snapshot(
        &self,
        runtime: &mut EngineRuntime,
        names: &[&str],
        snapshot_lsn: u64,
    ) -> Result<()> {
        if names.is_empty() || !runtime.has_deferred_tables() {
            return Ok(());
        }
        let has_match = names.iter().any(|name| {
            runtime
                .deferred_table_names()
                .any(|deferred| deferred.eq_ignore_ascii_case(name))
        });
        if !has_match {
            return Ok(());
        }
        let filter: BTreeSet<String> = names.iter().map(|name| (*name).to_string()).collect();
        runtime.load_deferred_tables_filtered_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            self.inner.config.page_size,
            &filter,
            snapshot_lsn,
        )
    }

    fn ensure_runtime_tables_loaded_for_statement_at_snapshot(
        &self,
        runtime: &mut EngineRuntime,
        statement: &SqlStatement,
        snapshot_lsn: u64,
    ) -> Result<bool> {
        let Some(base_tables) = self.safe_referenced_base_tables_in_runtime(runtime, statement)
        else {
            return Ok(false);
        };
        if base_tables.is_empty() {
            return Ok(true);
        }
        let base_tables: Vec<&str> = base_tables.iter().map(String::as_str).collect();
        self.ensure_runtime_tables_loaded_at_snapshot(runtime, &base_tables, snapshot_lsn)?;
        Ok(true)
    }

    fn ensure_runtime_all_tables_loaded_at_snapshot(
        &self,
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
    ) -> Result<()> {
        if !runtime.has_deferred_tables() {
            return Ok(());
        }
        if self.inner.config.paged_row_storage {
            runtime.load_deferred_table_row_sources_at_snapshot(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                snapshot_lsn,
            )
        } else {
            runtime.load_deferred_tables_at_snapshot(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                snapshot_lsn,
            )
        }
    }

    fn execute_read_in_runtime_state(
        &self,
        statement: &SqlStatement,
        params: &[Value],
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
        indexes_maybe_stale: &mut bool,
    ) -> Result<QueryResult> {
        let security_active =
            self.load_security_tables_for_runtime_at_snapshot(runtime, snapshot_lsn)?;
        if self.statement_is_temp_only(runtime, statement) {
            return runtime.execute_read_statement(statement, params, self.inner.config.page_size);
        }
        if !security_active && !*indexes_maybe_stale {
            if let SqlStatement::Query(query) = statement {
                if let Some(result) = self
                    .try_execute_indexed_join_grouped_count_query_at_snapshot(
                        runtime,
                        query,
                        params,
                        snapshot_lsn,
                    )?
                {
                    return Ok(result);
                }
                if let Some(result) = self
                    .try_execute_simple_indexed_join_projection_query_at_snapshot(
                        runtime,
                        statement,
                        query,
                        params,
                        snapshot_lsn,
                    )?
                {
                    return Ok(result);
                }
            }
        }
        if !security_active {
            if let Some(result) = self.try_execute_query_with_row_sources_at_snapshot(
                runtime,
                statement,
                params,
                snapshot_lsn,
                *indexes_maybe_stale,
            )? {
                return Ok(result);
            }
        }
        let targeted_ok = self.ensure_runtime_tables_loaded_for_statement_at_snapshot(
            runtime,
            statement,
            snapshot_lsn,
        )?;
        if !targeted_ok {
            // Intentionally unsupported for row-source execution: the
            // statement analyzer could not determine a conservative set of
            // referenced base tables (CTEs, recursive queries, VALUES,
            // subqueries, etc.). Fall back to broad-load so the generic
            // executor has every table available.
            self.ensure_runtime_all_tables_loaded_at_snapshot(runtime, snapshot_lsn)?;
        }
        if *indexes_maybe_stale {
            runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
            *indexes_maybe_stale = false;
        }
        runtime.execute_read_statement(statement, params, self.inner.config.page_size)
    }

    fn execute_write_in_runtime_state(
        &self,
        statement: &SqlStatement,
        params: &[Value],
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
        persistent_changed: &mut bool,
        indexes_maybe_stale: &mut bool,
    ) -> Result<QueryResult> {
        if matches!(statement, SqlStatement::Analyze { .. }) {
            return Err(DbError::transaction(
                "ANALYZE is not supported inside an explicit SQL transaction",
            ));
        }

        if *indexes_maybe_stale {
            runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
            *indexes_maybe_stale = false;
        }

        let temp_only = self.statement_is_temp_only(runtime, statement);
        match statement {
            SqlStatement::Insert(insert) => {
                let table_names =
                    self.insert_dependency_table_names(runtime, &insert.table_name)?;
                let table_refs = table_names.iter().map(String::as_str).collect::<Vec<_>>();
                self.load_runtime_table_row_sources_at_snapshot(
                    runtime,
                    &table_refs,
                    snapshot_lsn,
                )?;
                if let Some(prepared_insert) = runtime.prepare_simple_insert(insert)? {
                    let result = runtime.execute_prepared_simple_insert(
                        &prepared_insert,
                        params,
                        self.inner.config.page_size,
                    )?;
                    *persistent_changed |= !temp_only;
                    return Ok(result);
                }
                if runtime.can_execute_insert_in_place(insert) {
                    let result = runtime.execute_statement(
                        statement,
                        params,
                        self.inner.config.page_size,
                    )?;
                    *persistent_changed |= !temp_only;
                    return Ok(result);
                }
            }
            SqlStatement::Update(update) => {
                self.load_runtime_table_row_sources_at_snapshot(
                    runtime,
                    &[update.table_name.as_str()],
                    snapshot_lsn,
                )?;
                if let Some(prepared_update) = runtime.prepare_simple_update(update)? {
                    let result = runtime.execute_prepared_simple_update(
                        &prepared_update,
                        params,
                        self.inner.config.page_size,
                    )?;
                    *persistent_changed |= !temp_only;
                    return Ok(result);
                }
            }
            SqlStatement::Delete(delete) => {
                if let Some(prepared_delete) = runtime.prepare_simple_delete(delete)? {
                    let mut table_names = vec![prepared_delete.table.name.as_str()];
                    for child in &prepared_delete.restrict_children {
                        table_names.push(child.child_table_name.as_str());
                    }
                    self.load_runtime_table_row_sources_at_snapshot(
                        runtime,
                        &table_names,
                        snapshot_lsn,
                    )?;
                    if let Some(prepared_delete) = runtime.prepare_simple_delete(delete)? {
                        let result = runtime.execute_prepared_simple_delete(
                            &prepared_delete,
                            params,
                            self.inner.config.page_size,
                        )?;
                        *persistent_changed |= !temp_only;
                        return Ok(result);
                    }
                }
            }
            _ => {}
        }

        if runtime.can_execute_statement_in_state_without_clone(statement) {
            let Some(base_tables) = self.safe_referenced_base_tables_in_runtime(runtime, statement)
            else {
                // Intentionally unsupported for targeted loading: the
                // statement analyzer could not determine a conservative set
                // of referenced base tables. Fall back to broad-load.
                self.ensure_runtime_all_tables_loaded_at_snapshot(runtime, snapshot_lsn)?;
                let result =
                    runtime.execute_statement(statement, params, self.inner.config.page_size)?;
                *persistent_changed |= !temp_only;
                return Ok(result);
            };
            let base_refs: Vec<&str> = base_tables.iter().map(String::as_str).collect();
            self.load_runtime_table_row_sources_at_snapshot(runtime, &base_refs, snapshot_lsn)?;
            let result =
                runtime.execute_statement(statement, params, self.inner.config.page_size)?;
            *persistent_changed |= !temp_only;
            return Ok(result);
        }

        let mut working = runtime.clone();
        let targeted_ok = self.ensure_runtime_tables_loaded_for_statement_at_snapshot(
            &mut working,
            statement,
            snapshot_lsn,
        )?;
        if !targeted_ok {
            // Intentionally unsupported for row-source execution: the
            // statement analyzer could not determine a conservative set of
            // referenced base tables (CTEs, recursive queries, VALUES,
            // subqueries, etc.). Fall back to broad-load so the generic
            // executor has every table available.
            self.ensure_runtime_all_tables_loaded_at_snapshot(&mut working, snapshot_lsn)?;
        }
        working.rebuild_stale_indexes(self.inner.config.page_size)?;
        let result = working.execute_statement(statement, params, self.inner.config.page_size)?;
        *runtime = working;
        *persistent_changed |= !temp_only;
        *indexes_maybe_stale = true;
        Ok(result)
    }

    /// Attempts to materialize *only* the tables referenced by `statement`.
    ///
    /// Returns `Ok(true)` when statement analysis was conservatively
    /// exhaustive and the targeted load succeeded — the caller can then
    /// safely skip `ensure_all_tables_loaded()`. Returns `Ok(false)` when
    /// the statement contains shapes the analyzer can't fully resolve
    /// (CTEs, subqueries, VALUES queries, many DDL shapes, …); the
    /// caller must fall back to loading all tables.
    ///
    /// Per ADR 0143 Phase B + the rubber-duck plan critique on
    /// 2026-04-22: only a strict whitelist is treated as targeted-safe.
    fn ensure_tables_loaded_for_statement_at_snapshot(
        &self,
        statement: &SqlStatement,
        snapshot_lsn: Option<u64>,
    ) -> Result<bool> {
        let names = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            let Some(base_tables) =
                self.safe_referenced_base_tables_in_runtime(&runtime, statement)
            else {
                return Ok(false);
            };
            base_tables
        };
        let should_load_extension_catalog = self.inner.config.extension_unsigned_development_mode
            || !self.inner.config.extension_trust_anchors.is_empty();
        if names.is_empty() && !should_load_extension_catalog {
            return Ok(true);
        }
        let mut names_refs: Vec<&str> = names.iter().map(|s: &String| s.as_str()).collect();
        if should_load_extension_catalog {
            names_refs.extend(crate::extensions::extension_catalog_table_names());
        }
        self.ensure_tables_loaded_at_snapshot(&names_refs, snapshot_lsn)?;
        Ok(true)
    }

    /// Materializes all tables that were deferred during `Db::open`.
    ///
    /// Fast path (no deferred tables): one read-lock check on the engine.
    /// Slow path: drops the read lock, takes a write lock, loads all deferred
    /// tables and rebuilds indexes, then releases.
    fn ensure_all_tables_loaded(&self) -> Result<()> {
        self.ensure_all_tables_loaded_at_snapshot(None)
    }

    fn ensure_all_tables_loaded_at_snapshot(&self, snapshot_lsn: Option<u64>) -> Result<()> {
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            if !runtime.has_deferred_tables() {
                return Ok(());
            }
        }
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        if let Some(snapshot_lsn) = snapshot_lsn {
            if self.inner.config.paged_row_storage {
                runtime.load_deferred_table_row_sources_at_snapshot(
                    &self.inner.pager,
                    &self.inner.wal,
                    self.inner.config.page_size,
                    snapshot_lsn,
                )
            } else {
                runtime.load_deferred_tables_at_snapshot(
                    &self.inner.pager,
                    &self.inner.wal,
                    self.inner.config.page_size,
                    snapshot_lsn,
                )
            }
        } else if self.inner.config.paged_row_storage {
            runtime.load_deferred_table_row_sources(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
            )
        } else {
            runtime.load_deferred_tables(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
            )
        }
    }

    fn current_schema_cookie(&self) -> Result<u32> {
        let page = self.read_page(page::HEADER_PAGE_ID)?;
        let mut bytes = [0_u8; storage::header::DB_HEADER_SIZE];
        bytes.copy_from_slice(&page[..storage::header::DB_HEADER_SIZE]);
        Ok(DatabaseHeader::decode(&bytes)?.schema_cookie)
    }

    fn current_schema_cookie_at_snapshot(&self, snapshot_lsn: u64) -> Result<u32> {
        let page = if let Some(wal_page) = self.inner.wal.read_page_at_snapshot(
            &self.inner.pager,
            page::HEADER_PAGE_ID,
            snapshot_lsn,
        )? {
            wal_page
        } else {
            self.inner.pager.read_page(page::HEADER_PAGE_ID)?
        };
        let mut bytes = [0_u8; storage::header::DB_HEADER_SIZE];
        bytes.copy_from_slice(&page[..storage::header::DB_HEADER_SIZE]);
        Ok(DatabaseHeader::decode(&bytes)?.schema_cookie)
    }

    fn validate_prepared_schema_cookie(
        &self,
        prepared: &PreparedStatement,
        schema_cookie: u32,
        temp_schema_cookie: u32,
    ) -> Result<()> {
        if schema_cookie == prepared.schema_cookie
            && temp_schema_cookie == prepared.temp_schema_cookie
        {
            return Ok(());
        }
        Err(DbError::sql(
            "prepared statement is no longer valid because the schema changed",
        ))
    }

    fn build_sql_txn_state(&self) -> Result<SqlTxnState> {
        let (snapshot_reader, current_lsn, current_epoch) = self.begin_sql_snapshot()?;

        let mut runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?
            .clone();
        self.apply_temp_state_to_runtime(&mut runtime)?;
        self.configure_runtime_sync_capture(&mut runtime)?;
        Ok(SqlTxnState {
            runtime,
            snapshot_reader,
            base_lsn: current_lsn,
            base_checkpoint_epoch: current_epoch,
            persistent_changed: false,
            indexes_maybe_stale: false,
            prepared_insert_runtime_cache: HashMap::new(),
            savepoints: Vec::new(),
        })
    }

    fn execute_prepared_in_state(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        state: &mut SqlTxnState,
    ) -> Result<QueryResult> {
        if !Arc::ptr_eq(&self.inner, &prepared.db.inner) {
            return Err(DbError::transaction(
                "prepared statement belongs to a different database handle",
            ));
        }
        self.validate_prepared_schema_cookie(
            prepared,
            state.runtime.catalog.schema_cookie,
            state.runtime.temp_schema_cookie,
        )?;
        if prepared.read_only {
            if let Some(result) = self.try_execute_prepared_inspection_query(prepared, params)? {
                return Ok(result);
            }
            let snapshot_lsn = state.snapshot_lsn();
            return self.execute_read_in_runtime_state(
                prepared.statement.as_ref(),
                params,
                &mut state.runtime,
                snapshot_lsn,
                &mut state.indexes_maybe_stale,
            );
        }
        let snapshot_lsn = state.snapshot_lsn();
        if let Some(result) = self.try_execute_prepared_insert_in_runtime_state(
            prepared,
            params,
            &mut state.runtime,
            snapshot_lsn,
            &mut state.persistent_changed,
            &mut state.indexes_maybe_stale,
            &mut state.prepared_insert_runtime_cache,
        )? {
            return Ok(result);
        }
        self.execute_write_in_runtime_state(
            prepared.statement.as_ref(),
            params,
            &mut state.runtime,
            snapshot_lsn,
            &mut state.persistent_changed,
            &mut state.indexes_maybe_stale,
        )
    }

    fn execute_prepared_in_exclusive_state(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
        state: &mut ExclusiveSqlTxnState<'_>,
    ) -> Result<QueryResult> {
        if !Arc::ptr_eq(&self.inner, &prepared.db.inner) {
            return Err(DbError::transaction(
                "prepared statement belongs to a different database handle",
            ));
        }
        self.validate_prepared_schema_cookie(
            prepared,
            state.runtime.catalog.schema_cookie,
            state.runtime.temp_schema_cookie,
        )?;
        if prepared.read_only {
            if let Some(result) = self.try_execute_prepared_inspection_query(prepared, params)? {
                return Ok(result);
            }
            let snapshot_lsn = state.snapshot_lsn();
            return self.execute_read_in_runtime_state(
                prepared.statement.as_ref(),
                params,
                &mut state.runtime,
                snapshot_lsn,
                &mut state.indexes_maybe_stale,
            );
        }
        let snapshot_lsn = state.snapshot_lsn();
        if let Some(result) = self.try_execute_prepared_insert_in_runtime_state(
            prepared,
            params,
            &mut state.runtime,
            snapshot_lsn,
            &mut state.persistent_changed,
            &mut state.indexes_maybe_stale,
            &mut state.prepared_insert_runtime_cache,
        )? {
            return Ok(result);
        }
        self.execute_write_in_runtime_state(
            prepared.statement.as_ref(),
            params,
            &mut state.runtime,
            snapshot_lsn,
            &mut state.persistent_changed,
            &mut state.indexes_maybe_stale,
        )
    }

    fn execute_prepared_in_exclusive_state_mut(
        &self,
        prepared: &PreparedStatement,
        params: &mut [Value],
        state: &mut ExclusiveSqlTxnState<'_>,
    ) -> Result<QueryResult> {
        if !Arc::ptr_eq(&self.inner, &prepared.db.inner) {
            return Err(DbError::transaction(
                "prepared statement belongs to a different database handle",
            ));
        }
        self.validate_prepared_schema_cookie(
            prepared,
            state.runtime.catalog.schema_cookie,
            state.runtime.temp_schema_cookie,
        )?;
        if prepared.read_only {
            if let Some(result) = self.try_execute_prepared_inspection_query(prepared, params)? {
                return Ok(result);
            }
            let snapshot_lsn = state.snapshot_lsn();
            return self.execute_read_in_runtime_state(
                prepared.statement.as_ref(),
                params,
                &mut state.runtime,
                snapshot_lsn,
                &mut state.indexes_maybe_stale,
            );
        }
        let snapshot_lsn = state.snapshot_lsn();
        if let Some(result) = self.try_execute_prepared_insert_in_runtime_state_mut(
            prepared,
            params,
            &mut state.runtime,
            snapshot_lsn,
            &mut state.persistent_changed,
            &mut state.indexes_maybe_stale,
            &mut state.prepared_insert_runtime_cache,
        )? {
            return Ok(result);
        }
        self.execute_write_in_runtime_state(
            prepared.statement.as_ref(),
            params,
            &mut state.runtime,
            snapshot_lsn,
            &mut state.persistent_changed,
            &mut state.indexes_maybe_stale,
        )
    }

    fn prepare_batch_in_exclusive_state<'txn, 'db>(
        &'db self,
        prepared: &'txn PreparedStatement,
        param_count: usize,
        state: &'txn mut ExclusiveSqlTxnState<'db>,
    ) -> Result<PreparedStatementBatch<'txn, 'db>> {
        if !Arc::ptr_eq(&self.inner, &prepared.db.inner) {
            return Err(DbError::transaction(
                "prepared statement belongs to a different database handle",
            ));
        }
        self.validate_prepared_schema_cookie(
            prepared,
            state.runtime.catalog.schema_cookie,
            state.runtime.temp_schema_cookie,
        )?;

        let mut prepared_insert = None;
        let mut direct_positional = false;
        if !prepared.read_only && matches!(prepared.statement.as_ref(), SqlStatement::Insert(_)) {
            let snapshot_lsn = state.snapshot_lsn();
            prepared_insert = self.prepared_insert_plan_for_runtime_state(
                prepared,
                &mut state.runtime,
                snapshot_lsn,
                &mut state.indexes_maybe_stale,
                &mut state.prepared_insert_runtime_cache,
            )?;
            direct_positional = prepared_insert.as_deref().is_some_and(|insert| {
                Self::prepared_insert_uses_direct_positional_params(insert, param_count)
            });
        }

        Ok(PreparedStatementBatch {
            db: self,
            state,
            prepared,
            prepared_insert,
            direct_positional,
        })
    }

    fn execute_statement_in_state(
        &self,
        _sql: &str,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
        state: &mut SqlTxnState,
    ) -> Result<QueryResult> {
        let snapshot_lsn = state.snapshot_lsn();
        self.execute_write_in_runtime_state(
            statement,
            params,
            &mut state.runtime,
            snapshot_lsn,
            &mut state.persistent_changed,
            &mut state.indexes_maybe_stale,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_execute_prepared_insert_in_runtime_state(
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
    fn try_execute_prepared_insert_in_runtime_state_mut(
        &self,
        prepared: &PreparedStatement,
        params: &mut [Value],
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

        if !Self::prepared_insert_uses_direct_positional_params(insert_plan.as_ref(), params.len())
        {
            return Ok(None);
        }

        let result = runtime.execute_prepared_simple_insert_positional_params_in_place(
            insert_plan.as_ref(),
            params,
            self.inner.config.page_size,
        )?;
        *persistent_changed |=
            Self::prepared_insert_changes_persistent_table(runtime, insert_plan.as_ref());
        Ok(Some(QueryResult::with_affected_rows(result)))
    }

    fn exclusive_sql_txn_error(&self) -> DbError {
        DbError::transaction(
            "a SQL transaction handle is active on this database handle; use it until commit or rollback",
        )
    }

    pub fn sync_init_replica(&self, replica_id: &str) -> Result<()> {
        self.ensure_sync_tables()?;
        self.sync_upsert_metadata("replica_id", replica_id)?;
        self.sync_upsert_metadata("enabled", "true")?;
        self.sync_upsert_metadata("next_sequence", "1")?;
        self.inner.sync_ctx.set_replica_id(replica_id);
        self.inner.sync_ctx.set_enabled(true);
        self.inner.sync_ctx.set_next_sequence(1);
        self.inner.sync_ctx.ensure_journal_open(&self.inner.vfs)?;
        Ok(())
    }

    pub fn sync_create_scope(
        &self,
        name: &str,
        include_tables: &[&str],
        row_filter: Option<&str>,
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let runtime = self.runtime_for_metadata_inspection()?;
        let validation =
            validate_sync_scope_definition(&runtime, name, include_tables, row_filter)?;
        let created_at_micros = self
            .sync_scope(name)?
            .map(|scope| scope.created_at_micros)
            .unwrap_or_else(current_time_micros);
        let updated_at_micros = current_time_micros();
        let sql = format!(
            "INSERT INTO {table} (name, include_tables_json, row_filter, filter_columns_json, created_at_micros, updated_at_micros) VALUES ({name}, {include_tables_json}, {row_filter}, {filter_columns_json}, {created_at_micros}, {updated_at_micros}) ON CONFLICT (name) DO UPDATE SET include_tables_json = {include_tables_json}, row_filter = {row_filter}, filter_columns_json = {filter_columns_json}, updated_at_micros = {updated_at_micros}",
            table = crate::sync::SCOPES_TABLE,
            name = sql_text_literal(&validation.name),
            include_tables_json = sql_text_literal(
                &serde_json::to_string(&validation.include_tables)
                    .map_err(|error| DbError::internal(format!("failed to encode scope tables: {error}")))?,
            ),
            row_filter = sql_nullable_text_literal(validation.row_filter.as_deref()),
            filter_columns_json = sql_text_literal(
                &serde_json::to_string(&validation.filter_columns)
                    .map_err(|error| DbError::internal(format!("failed to encode scope columns: {error}")))?,
            ),
            created_at_micros = created_at_micros,
            updated_at_micros = updated_at_micros,
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }

    pub fn sync_drop_scope(&self, name: &str) -> Result<bool> {
        self.ensure_sync_tables()?;
        let scope_name = name.trim();
        if scope_name.is_empty() {
            return Err(DbError::sql("sync scope name must not be empty"));
        }
        if self
            .sync_peer_scope_bindings()?
            .iter()
            .any(|binding| binding.scope_name.eq_ignore_ascii_case(scope_name))
        {
            return Err(DbError::sql(format!(
                "cannot drop sync scope '{scope_name}' while peer bindings exist"
            )));
        }
        let sql = format!(
            "DELETE FROM {} WHERE name = {}",
            crate::sync::SCOPES_TABLE,
            sql_text_literal(scope_name)
        );
        let result = self.execute(&sql)?;
        Ok(result.affected_rows() > 0)
    }

    pub fn sync_scope(&self, name: &str) -> Result<Option<SyncScope>> {
        self.ensure_sync_tables()?;
        let scope_name = name.trim();
        if scope_name.is_empty() {
            return Err(DbError::sql("sync scope name must not be empty"));
        }
        let sql = format!(
            "SELECT name, include_tables_json, row_filter, filter_columns_json, created_at_micros, updated_at_micros FROM {} WHERE name = {}",
            crate::sync::SCOPES_TABLE,
            sql_text_literal(scope_name)
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result.rows().first().map(sync_scope_from_row).transpose()?),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn try_execute_prepared_inspection_query(
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

    pub fn sync_scopes(&self) -> Result<Vec<SyncScope>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT name, include_tables_json, row_filter, filter_columns_json, created_at_micros, updated_at_micros FROM {} ORDER BY name",
            crate::sync::SCOPES_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_scope_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_bind_peer_scope(&self, peer_name: &str, scope_name: &str) -> Result<()> {
        self.ensure_sync_tables()?;
        let peer_name = peer_name.trim();
        if peer_name.is_empty() {
            return Err(DbError::sql("sync peer name must not be empty"));
        }
        if self.sync_peer(peer_name)?.is_none() {
            return Err(DbError::sql(format!("sync peer '{peer_name}' not found")));
        }
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        let existing = self.sync_peer_scope_binding_row(peer_name)?;
        let created_at_micros = existing
            .as_ref()
            .map(|binding| binding.created_at_micros)
            .unwrap_or_else(current_time_micros);
        let updated_at_micros = current_time_micros();
        let sql = format!(
            "INSERT INTO {table} (peer_name, scope_name, created_at_micros, updated_at_micros) VALUES ({peer_name}, {scope_name}, {created_at_micros}, {updated_at_micros}) ON CONFLICT (peer_name) DO UPDATE SET scope_name = {scope_name}, updated_at_micros = {updated_at_micros}",
            table = crate::sync::PEER_SCOPES_TABLE,
            peer_name = sql_text_literal(peer_name),
            scope_name = sql_text_literal(&scope.name),
            created_at_micros = created_at_micros,
            updated_at_micros = updated_at_micros,
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }

    pub fn sync_unbind_peer_scope(&self, peer_name: &str) -> Result<bool> {
        self.ensure_sync_tables()?;
        let peer_name = peer_name.trim();
        if peer_name.is_empty() {
            return Err(DbError::sql("sync peer name must not be empty"));
        }
        let sql = format!(
            "DELETE FROM {} WHERE peer_name = {}",
            crate::sync::PEER_SCOPES_TABLE,
            sql_text_literal(peer_name)
        );
        let result = self.execute(&sql)?;
        Ok(result.affected_rows() > 0)
    }

    pub fn sync_peer_scope(&self, peer_name: &str) -> Result<Option<SyncPeerScopeBinding>> {
        self.ensure_sync_tables()?;
        let peer_name = peer_name.trim();
        if peer_name.is_empty() {
            return Err(DbError::sql("sync peer name must not be empty"));
        }
        self.sync_peer_scope_binding_row(peer_name)
    }

    pub fn sync_peer_scope_definition(&self, peer_name: &str) -> Result<Option<SyncScope>> {
        let binding = match self.sync_peer_scope(peer_name)? {
            Some(binding) => binding,
            None => return Ok(None),
        };
        match self.sync_scope(&binding.scope_name)? {
            Some(scope) => Ok(Some(scope)),
            None => Err(DbError::sql(format!(
                "sync scope '{}' bound to peer '{}' was not found",
                binding.scope_name, binding.peer_name
            ))),
        }
    }

    pub fn sync_peer_scope_bindings(&self) -> Result<Vec<SyncPeerScopeBinding>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT peer_name, scope_name, created_at_micros, updated_at_micros FROM {} ORDER BY peer_name",
            crate::sync::PEER_SCOPES_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(sync_peer_scope_binding_from_row)
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_export_batch_for_scope(
        &self,
        scope_name: &str,
        since_seq: u64,
        limit: usize,
    ) -> Result<SyncChangeBatch> {
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        let records = self.sync_pending_changes(since_seq, limit)?;
        let source_replica_id = records.first().map(|record| record.replica_id.clone());
        let source_high_watermark = records.last().map(|record| record.sequence);
        let filtered = self.sync_filter_records_for_scope(&scope, records)?;
        SyncChangeBatch::scoped_from_records(filtered, source_replica_id, source_high_watermark)
    }

    pub fn sync_import_batch_for_scope(
        &self,
        scope_name: &str,
        batch: &SyncChangeBatch,
    ) -> Result<SyncImportSummary> {
        batch.validate()?;
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        self.sync_validate_batch_for_scope(&scope, batch)?;
        self.sync_import_batch(batch)
    }

    pub fn sync_import_batch_for_scope_with_policy(
        &self,
        scope_name: &str,
        batch: &SyncChangeBatch,
        policy: SyncConflictPolicy,
    ) -> Result<SyncImportSummary> {
        batch.validate()?;
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        self.sync_validate_batch_for_scope(&scope, batch)?;
        self.sync_import_batch_with_policy(batch, policy)
    }

    pub fn sync_add_peer(&self, name: &str, endpoint: &str, token_env: Option<&str>) -> Result<()> {
        let name = name.trim();
        if name.is_empty() {
            return Err(DbError::sql("sync peer name must not be empty"));
        }
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            return Err(DbError::sql(
                "sync peer endpoint must start with http:// or https://",
            ));
        }
        if token_env.is_some_and(|value| value.trim().is_empty()) {
            return Err(DbError::sql("sync peer token_env must not be empty"));
        }

        self.ensure_sync_tables()?;
        let now = current_time_micros();
        let token_sql = token_env
            .map(sql_text_literal)
            .unwrap_or_else(|| "NULL".to_string());
        let sql = format!(
            "INSERT INTO {table} (name, endpoint, token_env, created_at_micros, updated_at_micros) VALUES ({name}, {endpoint}, {token_env}, {now}, {now}) ON CONFLICT (name) DO UPDATE SET endpoint = {endpoint}, token_env = {token_env}, updated_at_micros = {now}",
            table = crate::sync::PEERS_TABLE,
            name = sql_text_literal(name),
            endpoint = sql_text_literal(endpoint),
            token_env = token_sql,
            now = now,
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }

    pub fn sync_remove_peer(&self, name: &str) -> Result<bool> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "DELETE FROM {} WHERE name = {}",
            crate::sync::PEERS_TABLE,
            sql_text_literal(name)
        );
        let result = self.execute(&sql)?;
        Ok(result.affected_rows() > 0)
    }

    pub fn sync_peers(&self) -> Result<Vec<SyncPeer>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT name, endpoint, token_env, created_at_micros, updated_at_micros FROM {} ORDER BY name",
            crate::sync::PEERS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_peer_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_peer(&self, name: &str) -> Result<Option<SyncPeer>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT name, endpoint, token_env, created_at_micros, updated_at_micros FROM {} WHERE name = {}",
            crate::sync::PEERS_TABLE,
            sql_text_literal(name)
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result.rows().first().map(sync_peer_from_row).transpose()?),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn sync_peer_scope_binding_row(&self, peer_name: &str) -> Result<Option<SyncPeerScopeBinding>> {
        let sql = format!(
            "SELECT peer_name, scope_name, created_at_micros, updated_at_micros FROM {} WHERE peer_name = {}",
            crate::sync::PEER_SCOPES_TABLE,
            sql_text_literal(peer_name)
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result
                .rows()
                .first()
                .map(sync_peer_scope_binding_from_row)
                .transpose()?),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_sessions(&self) -> Result<Vec<SyncSession>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT session_id, peer_name, direction, remote_replica_id, started_at_micros, ended_at_micros, status, error, pushed_batch_id, pulled_batch_id, pushed_seen, pushed_applied, pushed_skipped, pushed_conflicted, pulled_seen, pulled_applied, pulled_skipped, pulled_conflicted, retry_count FROM {} ORDER BY session_id",
            crate::sync::SESSIONS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_session_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_start_session(
        &self,
        peer_name: &str,
        direction: SyncRunDirection,
        remote_replica_id: Option<&str>,
    ) -> Result<i64> {
        self.ensure_sync_tables()?;
        let session_id = self.next_sync_session_id()?;
        let started_at_micros = current_time_micros();
        let sql = format!(
            "INSERT INTO {table} (session_id, peer_name, direction, remote_replica_id, started_at_micros, ended_at_micros, status, error, pushed_batch_id, pulled_batch_id, pushed_seen, pushed_applied, pushed_skipped, pushed_conflicted, pulled_seen, pulled_applied, pulled_skipped, pulled_conflicted, retry_count) VALUES ({session_id}, {peer_name}, {direction}, {remote_replica_id}, {started_at_micros}, NULL, 'started', NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0)",
            table = crate::sync::SESSIONS_TABLE,
            session_id = session_id,
            peer_name = sql_text_literal(peer_name),
            direction = sql_text_literal(direction.as_str()),
            remote_replica_id = remote_replica_id
                .map(sql_text_literal)
                .unwrap_or_else(|| "NULL".to_string()),
            started_at_micros = started_at_micros,
        );
        let _ = self.execute(&sql)?;
        Ok(session_id)
    }

    pub fn sync_finish_session_success(
        &self,
        session_id: i64,
        summary: &SyncRunSummary,
    ) -> Result<()> {
        self.sync_update_session(session_id, summary, "success", None, current_time_micros())
    }

    pub fn sync_finish_session_failed(
        &self,
        session_id: i64,
        summary: &SyncRunSummary,
        error: &str,
    ) -> Result<()> {
        self.sync_update_session(
            session_id,
            summary,
            "failed",
            Some(error),
            current_time_micros(),
        )
    }

    pub fn sync_integrity_report(&self) -> Result<SyncJournalIntegrityReport> {
        let local_replica_id = self.sync_read_metadata("replica_id").ok().flatten();
        crate::sync::inspect_journal_integrity(
            self.inner.sync_ctx.journal_path(),
            &self.inner.vfs,
            local_replica_id.as_deref(),
        )
    }

    pub fn sync_peer_lag_report(&self) -> Result<Vec<SyncPeerLag>> {
        self.ensure_sync_tables()?;
        let local_high_watermark = self.sync_integrity_report()?.last_sequence;
        let peers = self.sync_peers()?;
        let sessions = self.sync_sessions()?;
        let mut latest_successful_remote_replica_ids: HashMap<String, Option<String>> =
            HashMap::new();
        for session in sessions.iter().rev() {
            if session.status == "success" {
                latest_successful_remote_replica_ids
                    .entry(session.peer_name.clone())
                    .or_insert_with(|| session.remote_replica_id.clone());
            }
        }

        peers
            .into_iter()
            .map(|peer| {
                let remote_replica_id = latest_successful_remote_replica_ids
                    .get(&peer.name)
                    .cloned()
                    .flatten();
                let in_watermark = match remote_replica_id.as_deref() {
                    Some(replica_id) => self.sync_peer_watermark(replica_id)?,
                    None => None,
                };
                let out_watermark = self.sync_peer_out_watermark(&peer.name)?;
                let in_lag = match (local_high_watermark, in_watermark) {
                    (Some(local_high), Some(in_watermark)) if local_high >= in_watermark => {
                        Some(local_high - in_watermark)
                    }
                    _ => None,
                };
                let out_lag = match (local_high_watermark, out_watermark) {
                    (Some(local_high), Some(out_watermark)) if local_high >= out_watermark => {
                        Some(local_high - out_watermark)
                    }
                    _ => None,
                };
                Ok(SyncPeerLag {
                    peer_name: peer.name,
                    remote_replica_id,
                    in_watermark,
                    out_watermark,
                    local_high_watermark,
                    in_lag,
                    out_lag,
                })
            })
            .collect()
    }

    pub fn sync_retention_report(&self) -> Result<SyncRetentionReport> {
        let integrity = self.sync_integrity_report()?;
        let peer_lag = self.sync_peer_lag_report()?;
        let journal_size_bytes = self.sync_status()?.journal_size_bytes;
        let mut watermark_entries = peer_lag
            .iter()
            .flat_map(|peer| {
                let inbound = peer.remote_replica_id.as_ref().zip(peer.in_watermark).map(
                    |(remote_replica_id, watermark)| {
                        (format!("remote:{remote_replica_id}"), watermark)
                    },
                );
                let outbound = peer
                    .out_watermark
                    .map(|watermark| (peer.peer_name.clone(), watermark));
                inbound.into_iter().chain(outbound)
            })
            .collect::<Vec<_>>();
        watermark_entries.extend(self.sync_peer_watermark_entries()?);
        watermark_entries.extend(
            self.sync_shape_clients()?
                .into_iter()
                .filter(|client| client.retention_blocking)
                .map(|client| {
                    (
                        format!(
                            "shape:{}:client:{}",
                            client.shape_id, client.client_replica_id
                        ),
                        client.last_ack_watermark,
                    )
                }),
        );
        watermark_entries.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        watermark_entries.dedup();
        let lowest_watermark = watermark_entries
            .iter()
            .map(|(_, watermark)| *watermark)
            .min();
        let safe_prune_through = if integrity.total_records == 0 {
            None
        } else {
            lowest_watermark.and_then(|watermark| watermark.checked_sub(1))
        };
        let blocked_by = if integrity.total_records == 0 {
            Vec::new()
        } else if let Some(lowest_watermark) = lowest_watermark {
            if integrity
                .last_sequence
                .is_some_and(|local_high| lowest_watermark <= local_high)
            {
                watermark_entries
                    .iter()
                    .filter(|(_, watermark)| *watermark == lowest_watermark)
                    .map(|(label, _)| label.clone())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        let prunable_records = match safe_prune_through {
            Some(safe_through) => {
                match crate::sync::read_journal_records(
                    self.inner.sync_ctx.journal_path(),
                    &self.inner.vfs,
                    0,
                    usize::MAX,
                ) {
                    Ok(records) => records
                        .into_iter()
                        .filter(|record| record.sequence <= safe_through)
                        .count(),
                    Err(_) => 0,
                }
            }
            None => 0,
        };

        Ok(SyncRetentionReport {
            journal_records: integrity.total_records,
            first_sequence: integrity.first_sequence,
            last_sequence: integrity.last_sequence,
            safe_prune_through,
            prunable_records,
            blocked_by,
            journal_size_bytes,
        })
    }

    pub fn sync_operational_doctor_report(&self) -> Result<SyncOperationalDoctorReport> {
        let status = self.sync_status()?;
        let integrity = self.sync_integrity_report()?;
        let retention = self.sync_retention_report()?;
        let peer_lag = self.sync_peer_lag_report()?;
        let unresolved_conflicts = self.sync_conflicts()?.len();
        let mut recent_sessions = self.sync_sessions()?;
        if recent_sessions.len() > 5 {
            recent_sessions = recent_sessions.split_off(recent_sessions.len() - 5);
        }
        let mut issues = integrity.issues.clone();
        let mut guidance = Vec::new();
        let mut highest_severity = integrity.highest_severity;

        if !status.enabled {
            highest_severity = highest_severity.max(SyncDoctorSeverity::Warning);
            guidance.push(
                "sync is disabled; enable it before expecting journal growth or peer watermarks"
                    .to_string(),
            );
        }

        if unresolved_conflicts > 0 {
            highest_severity = highest_severity.max(SyncDoctorSeverity::Warning);
            let message = format!("{unresolved_conflicts} unresolved conflict(s) need attention");
            issues.push(SyncJournalIssue {
                line_number: 0,
                sequence: None,
                severity: SyncDoctorSeverity::Warning,
                code: "unresolved_conflicts".to_string(),
                message: message.clone(),
            });
            guidance.push(message);
        }

        if retention.journal_records > 0 && retention.safe_prune_through.is_none() {
            highest_severity = highest_severity.max(SyncDoctorSeverity::Warning);
            let message = if retention.blocked_by.is_empty() {
                "safe prune is unavailable because no peer watermarks are known".to_string()
            } else {
                format!(
                    "safe prune is blocked by {}",
                    retention.blocked_by.join(", ")
                )
            };
            issues.push(SyncJournalIssue {
                line_number: 0,
                sequence: None,
                severity: SyncDoctorSeverity::Warning,
                code: "retention_blocked".to_string(),
                message: message.clone(),
            });
            guidance.push(message);
        } else if let Some(safe_through) = retention.safe_prune_through {
            guidance.push(format!(
                "safe prune is available through sequence {safe_through}"
            ));
        }

        if peer_lag.iter().any(|peer| {
            peer.in_lag.is_some_and(|lag| lag > 0) || peer.out_lag.is_some_and(|lag| lag > 0)
        }) {
            highest_severity = highest_severity.max(SyncDoctorSeverity::Warning);
            guidance.push("peer lag exists; inspect sys_sync_peer_lag before pruning".to_string());
        }

        if integrity.highest_severity == SyncDoctorSeverity::Error {
            highest_severity = SyncDoctorSeverity::Error;
        }

        if issues.is_empty() {
            guidance.push("journal integrity is clean".to_string());
        }

        Ok(SyncOperationalDoctorReport {
            status,
            integrity,
            retention,
            peer_lag,
            unresolved_conflicts,
            recent_sessions,
            highest_severity,
            issues,
            guidance,
        })
    }

    pub fn sync_status(&self) -> Result<SyncStatus> {
        if self.inner.sync_ctx.is_enabled() {
            return Ok(SyncStatus {
                enabled: true,
                replica_id: self.inner.sync_ctx.replica_id(),
                next_sequence: self.inner.sync_ctx.next_sequence(),
                journal_path: Some(
                    self.inner
                        .sync_ctx
                        .journal_path()
                        .to_string_lossy()
                        .to_string(),
                ),
                journal_size_bytes: self.inner.sync_ctx.journal_size_bytes(),
            });
        }
        self.load_sync_status_from_db()
    }

    pub fn sync_pending_changes(
        &self,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<SyncJournalRecord>> {
        if !self.inner.sync_ctx.is_enabled() {
            let loaded = self.load_sync_status_from_db()?;
            if !loaded.enabled {
                return Ok(Vec::new());
            }
        }
        crate::sync::read_journal_records(
            self.inner.sync_ctx.journal_path(),
            &self.inner.vfs,
            since_seq,
            limit,
        )
    }

    fn sync_scope_row_filter_expr(scope: &SyncScope) -> Result<Option<crate::sql::ast::Expr>> {
        match scope.row_filter.as_deref() {
            Some(filter_sql) => Ok(Some(parse_expression_sql(filter_sql).map_err(|error| {
                DbError::sql(format!(
                    "invalid row filter for sync scope '{}': {error}",
                    scope.name
                ))
            })?)),
            None => Ok(None),
        }
    }

    fn sync_scope_record_matches(
        &self,
        scope: &SyncScope,
        record: &SyncJournalRecord,
    ) -> Result<bool> {
        if !scope
            .include_tables
            .iter()
            .any(|table_name| table_name.eq_ignore_ascii_case(&record.table))
        {
            return Ok(false);
        }
        let Some(expr) = Self::sync_scope_row_filter_expr(scope)? else {
            return Ok(true);
        };
        let runtime = self.runtime_for_metadata_inspection()?;
        let table = runtime
            .catalog
            .table(&record.table)
            .ok_or_else(|| DbError::sql(format!("unknown table '{}'", record.table)))?;
        let payload = match record.operation.as_str() {
            "delete" => &record.primary_key,
            "insert" | "update" => record
                .after
                .as_ref()
                .ok_or_else(|| DbError::sql("sync record missing after payload"))?,
            other => {
                return Err(DbError::sql(format!("unsupported operation '{other}'")));
            }
        };
        let payload = payload.as_object().ok_or_else(|| {
            DbError::sql(format!(
                "sync record for table '{}' must use an object payload",
                record.table
            ))
        })?;

        let mut values = Vec::with_capacity(scope.filter_columns.len());
        for column_name in &scope.filter_columns {
            let column = table
                .columns
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "sync scope column '{column_name}' is missing from table '{}'",
                        table.name
                    ))
                })?;
            let json_value = payload.get(&column.name).ok_or_else(|| {
                DbError::sql(format!(
                    "sync record for table '{}' is missing scoped column '{}'",
                    table.name, column.name
                ))
            })?;
            values.push(json_to_column_value(&table.name, column, json_value)?);
        }

        row_satisfies_expression(&runtime, &table.name, &scope.filter_columns, &values, &expr)
    }

    fn sync_filter_records_for_scope(
        &self,
        scope: &SyncScope,
        records: Vec<SyncJournalRecord>,
    ) -> Result<Vec<SyncJournalRecord>> {
        let mut filtered = Vec::new();
        for record in records {
            if self.sync_scope_record_matches(scope, &record)? {
                filtered.push(record);
            }
        }
        Ok(filtered)
    }

    fn sync_validate_batch_for_scope(
        &self,
        scope: &SyncScope,
        batch: &SyncChangeBatch,
    ) -> Result<()> {
        let runtime = self.runtime_for_metadata_inspection()?;
        for record in &batch.records {
            if !scope
                .include_tables
                .iter()
                .any(|table_name| table_name.eq_ignore_ascii_case(&record.table))
            {
                return Err(DbError::sql(format!(
                    "sync batch contains table '{}' which is outside scope '{}'",
                    record.table, scope.name
                )));
            }
            if scope.row_filter.is_some() {
                let table = runtime
                    .catalog
                    .table(&record.table)
                    .ok_or_else(|| DbError::sql(format!("unknown table '{}'", record.table)))?;
                let payload = match record.operation.as_str() {
                    "delete" => &record.primary_key,
                    "insert" | "update" => record
                        .after
                        .as_ref()
                        .ok_or_else(|| DbError::sql("sync record missing after payload"))?,
                    other => {
                        return Err(DbError::sql(format!("unsupported operation '{other}'")));
                    }
                };
                let payload = payload.as_object().ok_or_else(|| {
                    DbError::sql(format!(
                        "sync record for table '{}' must use an object payload",
                        record.table
                    ))
                })?;
                let mut values = Vec::with_capacity(scope.filter_columns.len());
                for column_name in &scope.filter_columns {
                    let column = table
                        .columns
                        .iter()
                        .find(|candidate| candidate.name.eq_ignore_ascii_case(column_name))
                        .ok_or_else(|| {
                            DbError::sql(format!(
                                "sync scope column '{column_name}' is missing from table '{}'",
                                table.name
                            ))
                        })?;
                    let json_value = payload.get(&column.name).ok_or_else(|| {
                        DbError::sql(format!(
                            "sync record for table '{}' is missing scoped column '{}'",
                            table.name, column.name
                        ))
                    })?;
                    values.push(json_to_column_value(&table.name, column, json_value)?);
                }
                if !row_satisfies_expression(
                    &runtime,
                    &table.name,
                    &scope.filter_columns,
                    &values,
                    &Self::sync_scope_row_filter_expr(scope)?
                        .ok_or_else(|| DbError::internal("scope row filter expression missing"))?,
                )? {
                    return Err(DbError::sql(format!(
                        "sync batch contains record for table '{}' that does not match scope '{}'",
                        record.table, scope.name
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn sync_export_batch(&self, since_seq: u64, limit: usize) -> Result<SyncChangeBatch> {
        let records = self.sync_pending_changes(since_seq, limit)?;
        SyncChangeBatch::from_records(records)
    }

    pub fn sync_create_changeset(
        &self,
        mut options: CreateChangesetOptions,
    ) -> Result<SyncChangeset> {
        self.ensure_sync_tables()?;
        let mut scoped_from_shape = false;
        if let Some(principal) = options.principal.as_ref() {
            principal.validate()?;
        }
        if let Some(shape_id) = options.shape_id.as_deref() {
            let shape = self.sync_shape(shape_id)?.ok_or_else(|| {
                DbError::sql(format!(
                    "SHAPE_NOT_FOUND: sync shape '{shape_id}' not found"
                ))
            })?;
            self.sync_authorize_shape(options.principal.as_ref(), &shape)?;
            options.scope_name = Some(shape.scope_name);
            scoped_from_shape = true;
        }
        if let Some(scope_name) = options.scope_name.as_deref() {
            if !scoped_from_shape {
                self.sync_authorize_scope(options.principal.as_ref(), scope_name)?;
            }
        }

        let max_records = options
            .max_records
            .map(usize::try_from)
            .transpose()
            .map_err(|_| DbError::sql("max_records is too large"))?
            .unwrap_or(usize::MAX);
        let created_at_micros = current_time_micros();
        let tooling = self.get_tooling_metadata()?;
        let runtime = self.runtime_for_metadata_inspection()?;
        let schema_cookie = runtime.catalog.schema_cookie;
        let tenant_id = options
            .principal
            .as_ref()
            .map(|principal| principal.tenant_id.clone())
            .or_else(|| {
                options
                    .shape_id
                    .as_deref()
                    .and_then(|shape_id| self.sync_shape(shape_id).ok().flatten())
                    .map(|shape| shape.tenant_id)
            });

        let mut changeset = match &options.source {
            SyncChangesetSource::Checkpoint {
                peer,
                since_sequence,
            } => {
                let batch = match options.scope_name.as_deref() {
                    Some(scope_name) => {
                        self.sync_export_batch_for_scope(scope_name, *since_sequence, max_records)?
                    }
                    None => self.sync_export_batch(*since_sequence, max_records)?,
                };
                let source_replica_id = batch
                    .source_replica_id
                    .clone()
                    .or_else(|| self.sync_status().ok().and_then(|status| status.replica_id))
                    .unwrap_or_else(|| "unknown".to_string());
                let records = batch
                    .records
                    .iter()
                    .map(sync_changeset_record_from_journal_record)
                    .collect::<Vec<_>>();
                let start_checkpoint = batch.first_sequence;
                let end_checkpoint = batch.last_sequence;
                let source_high_watermark = batch
                    .source_high_watermark
                    .or(batch.last_sequence)
                    .or_else(|| {
                        self.sync_integrity_report()
                            .ok()
                            .and_then(|report| report.last_sequence)
                    });
                let changeset_id = sync_changeset_id(
                    "checkpoint",
                    &source_replica_id,
                    created_at_micros,
                    records.len(),
                );
                SyncChangeset {
                    changeset_version: crate::sync::SYNC_CHANGESET_VERSION,
                    changeset_id,
                    source_replica_id,
                    source_kind: options.source.kind(),
                    tenant_id,
                    scope_name: options.scope_name.clone(),
                    shape_id: options.shape_id.clone(),
                    base_kind: "checkpoint".to_string(),
                    base_checkpoint: Some(SyncChangesetCheckpoint {
                        peer: peer.clone(),
                        sequence: *since_sequence,
                    }),
                    base_branch: None,
                    base_snapshot: None,
                    start_checkpoint,
                    end_checkpoint,
                    source_high_watermark,
                    schema_fingerprint: tooling.schema_fingerprint.clone(),
                    schema_cookie,
                    sync_contract_version: crate::sync::SYNC_CONTRACT_VERSION,
                    query_contract_fingerprint: None,
                    producer_capabilities: SyncChangesetCapabilities::default(),
                    limits: SyncChangesetLimits::default(),
                    records,
                    conflict_policy_hint: None,
                    created_at_micros,
                    integrity_hash: None,
                }
            }
            SyncChangesetSource::Branch { from, to } => {
                self.sync_create_diff_changeset(SyncDiffChangesetContext {
                    base_kind: "branch",
                    from_ref: from,
                    to_ref: to,
                    scope_name: options.scope_name.as_deref(),
                    shape_id: options.shape_id.as_deref(),
                    tenant_id: tenant_id.as_deref(),
                    schema_fingerprint: &tooling.schema_fingerprint,
                    schema_cookie,
                    created_at_micros,
                    max_records,
                })?
            }
            SyncChangesetSource::Snapshot { from, to } => {
                self.sync_create_diff_changeset(SyncDiffChangesetContext {
                    base_kind: "snapshot",
                    from_ref: from,
                    to_ref: to,
                    scope_name: options.scope_name.as_deref(),
                    shape_id: options.shape_id.as_deref(),
                    tenant_id: tenant_id.as_deref(),
                    schema_fingerprint: &tooling.schema_fingerprint,
                    schema_cookie,
                    created_at_micros,
                    max_records,
                })?
            }
        };
        self.sync_finalize_changeset(&mut changeset, options.max_bytes)?;
        self.sync_record_changeset_history(&changeset, "created", None)?;
        Ok(changeset)
    }

    fn sync_create_diff_changeset(
        &self,
        ctx: SyncDiffChangesetContext<'_>,
    ) -> Result<SyncChangeset> {
        let diff = self.branch_diff(ctx.from_ref, ctx.to_ref)?;
        let table_infos = self
            .list_tables()?
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect::<BTreeMap<_, _>>();
        let mut records = Vec::new();
        let source_replica_id = format!("{}:{}:{}", ctx.base_kind, ctx.from_ref, ctx.to_ref);
        let mut sequence = 1u64;
        for table_diff in &diff.tables {
            if matches!(
                table_diff.status,
                crate::branch::BranchTableDiffStatus::Unsupported
            ) {
                return Err(DbError::sql(format!(
                    "CHANGESET_UNSUPPORTED: branch/snapshot diff for table '{}' is unsupported: {}",
                    table_diff.table,
                    table_diff
                        .message
                        .clone()
                        .unwrap_or_else(|| "unsupported row diff".to_string())
                )));
            }
            if table_diff.schema_changed {
                return Err(DbError::sql(format!(
                    "SCHEMA_INCOMPATIBLE: changeset diff for table '{}' changes schema",
                    table_diff.table
                )));
            }
            let Some(table) = table_infos.get(&table_diff.table) else {
                continue;
            };
            let pk_names = &table.primary_key_columns;
            let column_names = table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let table_context = BranchChangesetTableContext {
                source_replica_id: &source_replica_id,
                table_name: &table.name,
                primary_key_columns: pk_names,
                column_names: &column_names,
                schema_cookie: ctx.schema_cookie,
                created_at_micros: ctx.created_at_micros,
            };
            for row in &table_diff.added {
                records.push(sync_changeset_record_from_branch_row(
                    &table_context,
                    sequence,
                    "insert",
                    row,
                )?);
                sequence += 1;
            }
            for row in &table_diff.updated {
                records.push(sync_changeset_record_from_branch_row(
                    &table_context,
                    sequence,
                    "update",
                    row,
                )?);
                sequence += 1;
            }
            for row in &table_diff.deleted {
                records.push(sync_changeset_record_from_branch_row(
                    &table_context,
                    sequence,
                    "delete",
                    row,
                )?);
                sequence += 1;
            }
            if records.len() > ctx.max_records {
                return Err(DbError::sql(
                    "BATCH_TOO_LARGE: changeset exceeds max_records",
                ));
            }
        }

        let source_kind = if ctx.base_kind == "branch" {
            crate::sync::SyncChangesetSourceKind::Branch
        } else {
            crate::sync::SyncChangesetSourceKind::Snapshot
        };
        let changeset_id = sync_changeset_id(
            ctx.base_kind,
            &source_replica_id,
            ctx.created_at_micros,
            records.len(),
        );
        Ok(SyncChangeset {
            changeset_version: crate::sync::SYNC_CHANGESET_VERSION,
            changeset_id,
            source_replica_id,
            source_kind,
            tenant_id: ctx.tenant_id.map(str::to_string),
            scope_name: ctx.scope_name.map(str::to_string),
            shape_id: ctx.shape_id.map(str::to_string),
            base_kind: ctx.base_kind.to_string(),
            base_checkpoint: None,
            base_branch: (ctx.base_kind == "branch").then(|| ctx.from_ref.to_string()),
            base_snapshot: (ctx.base_kind == "snapshot").then(|| ctx.from_ref.to_string()),
            start_checkpoint: records.first().map(|record| record.origin_sequence),
            end_checkpoint: records.last().map(|record| record.origin_sequence),
            source_high_watermark: records.last().map(|record| record.origin_sequence),
            schema_fingerprint: ctx.schema_fingerprint.to_string(),
            schema_cookie: ctx.schema_cookie,
            sync_contract_version: crate::sync::SYNC_CONTRACT_VERSION,
            query_contract_fingerprint: None,
            producer_capabilities: SyncChangesetCapabilities {
                before_images: true,
                ..SyncChangesetCapabilities::default()
            },
            limits: SyncChangesetLimits::default(),
            records,
            conflict_policy_hint: None,
            created_at_micros: ctx.created_at_micros,
            integrity_hash: None,
        })
    }

    pub fn sync_inspect_changeset(
        &self,
        changeset: &SyncChangeset,
        options: InspectChangesetOptions,
    ) -> Result<SyncChangesetInspection> {
        self.sync_validate_changeset_envelope(changeset)?;
        let bytes = serde_json::to_vec(changeset)
            .map_err(|error| DbError::internal(format!("failed to serialize changeset: {error}")))?
            .len() as u64;
        let mut tables = BTreeSet::new();
        let mut operations = BTreeMap::new();
        let mut warnings = Vec::new();
        for record in &changeset.records {
            tables.insert(record.table.clone());
            *operations.entry(record.operation.clone()).or_insert(0u64) += 1;
            if record.operation == "delete" && record.before.is_none() {
                warnings.push(format!(
                    "delete record for table '{}' cannot be inverted without before image",
                    record.table
                ));
            }
        }
        let compatibility = if options.check_local_compatibility {
            match self.sync_check_changeset_compatibility(changeset) {
                Ok(()) => SyncChangesetCompatibility {
                    checked_against_local_db: true,
                    status: "compatible".to_string(),
                    message: None,
                },
                Err(error) => SyncChangesetCompatibility {
                    checked_against_local_db: true,
                    status: "incompatible".to_string(),
                    message: Some(error.to_string()),
                },
            }
        } else {
            SyncChangesetCompatibility {
                checked_against_local_db: false,
                status: "not_checked".to_string(),
                message: None,
            }
        };
        Ok(SyncChangesetInspection {
            changeset_id: changeset.changeset_id.clone(),
            valid_envelope: true,
            source_kind: changeset.source_kind.clone(),
            scope_name: changeset.scope_name.clone(),
            shape_id: changeset.shape_id.clone(),
            record_count: changeset.records.len() as u64,
            bytes,
            tables: tables.into_iter().collect(),
            operations,
            start_checkpoint: changeset.start_checkpoint,
            end_checkpoint: changeset.end_checkpoint,
            schema_fingerprint: changeset.schema_fingerprint.clone(),
            compatibility,
            warnings,
        })
    }

    pub fn sync_apply_changeset(
        &self,
        changeset: &SyncChangeset,
        options: ApplyChangesetOptions,
    ) -> Result<SyncChangesetApplyResult> {
        self.ensure_sync_tables()?;
        self.sync_validate_changeset_envelope(changeset)?;
        if !options.atomic {
            return Err(DbError::sql(
                "CHANGESET_UNSUPPORTED: non-atomic changeset apply is not supported",
            ));
        }
        if let Some(principal) = options.principal.as_ref() {
            principal.validate()?;
        }
        let mut scope_authorized_via_shape = false;
        if let Some(shape_id) = changeset.shape_id.as_deref() {
            let shape = self.sync_shape(shape_id)?.ok_or_else(|| {
                DbError::sql(format!(
                    "SHAPE_NOT_FOUND: sync shape '{shape_id}' not found"
                ))
            })?;
            scope_authorized_via_shape = true;
            self.sync_authorize_shape(options.principal.as_ref(), &shape)?;
        }
        if let Some(scope_name) = changeset.scope_name.as_deref() {
            if !scope_authorized_via_shape {
                self.sync_authorize_scope(options.principal.as_ref(), scope_name)?;
            }
        }
        if matches!(
            options.compatibility_mode,
            crate::sync::SyncCompatibilityMode::Strict
        ) {
            self.sync_check_changeset_compatibility(changeset)?;
        }
        let integrity_hash = self.sync_changeset_integrity_hash(changeset)?;
        if let Some(existing) =
            self.sync_read_metadata(&changeset_applied_key(&changeset.changeset_id))?
        {
            if existing != integrity_hash {
                return Err(DbError::sql(format!(
                    "CHANGESET_ID_COLLISION: changeset '{}' was already applied with a different integrity hash",
                    changeset.changeset_id
                )));
            }
            return Ok(SyncChangesetApplyResult {
                outcome: "already_applied".to_string(),
                changeset_id: changeset.changeset_id.clone(),
                rows_seen: changeset.records.len() as u64,
                rows_applied: 0,
                rows_skipped: changeset.records.len() as u64,
                rows_conflicted: 0,
                checkpoint_after: changeset.source_high_watermark.or(changeset.end_checkpoint),
            });
        }

        let journal_records = changeset
            .records
            .iter()
            .map(sync_journal_record_from_changeset_record)
            .collect::<Result<Vec<_>>>()?;
        let batch = SyncChangeBatch::scoped_from_records(
            journal_records,
            Some(changeset.source_replica_id.clone()),
            changeset.source_high_watermark.or(changeset.end_checkpoint),
        )?;
        let summary = match (changeset.scope_name.as_deref(), options.conflict_policy) {
            (Some(scope_name), Some(policy)) => {
                self.sync_import_batch_for_scope_with_policy(scope_name, &batch, policy)?
            }
            (Some(scope_name), None) => self.sync_import_batch_for_scope(scope_name, &batch)?,
            (None, Some(policy)) => self.sync_import_batch_with_policy(&batch, policy)?,
            (None, None) => self.sync_import_batch(&batch)?,
        };
        self.sync_upsert_metadata(
            &changeset_applied_key(&changeset.changeset_id),
            &integrity_hash,
        )?;
        self.sync_record_changeset_history(changeset, "applied", Some(current_time_micros()))?;
        Ok(SyncChangesetApplyResult {
            outcome: if summary.conflicted > 0 {
                "conflict_recorded".to_string()
            } else {
                "applied".to_string()
            },
            changeset_id: changeset.changeset_id.clone(),
            rows_seen: summary.seen as u64,
            rows_applied: summary.applied as u64,
            rows_skipped: summary.skipped as u64,
            rows_conflicted: summary.conflicted as u64,
            checkpoint_after: changeset.source_high_watermark.or(changeset.end_checkpoint),
        })
    }

    pub fn sync_invert_changeset(
        &self,
        changeset: &SyncChangeset,
        _options: InvertChangesetOptions,
    ) -> Result<SyncChangeset> {
        self.sync_validate_changeset_envelope(changeset)?;
        let created_at_micros = current_time_micros();
        let mut inverse_records = Vec::with_capacity(changeset.records.len());
        for (index, record) in changeset.records.iter().enumerate() {
            let operation = match record.operation.as_str() {
                "insert" => "delete",
                "delete" if record.before.is_some() => "insert",
                "update" if record.before.is_some() => "update",
                "delete" | "update" => {
                    return Err(DbError::sql(format!(
                        "CHANGESET_INVERSION_UNSUPPORTED: record {index} lacks before image"
                    )));
                }
                other => {
                    return Err(DbError::sql(format!(
                        "CHANGESET_INVALID: unsupported record operation '{other}'"
                    )));
                }
            };
            inverse_records.push(SyncChangesetRecord {
                record_version: record.record_version,
                table: record.table.clone(),
                operation: operation.to_string(),
                primary_key: record.primary_key.clone(),
                origin_replica_id: format!("inverse:{}", changeset.changeset_id),
                origin_sequence: (index as u64) + 1,
                transaction_id: format!("txn:inverse:{}", changeset.changeset_id),
                transaction_lsn: (index as u64) + 1,
                schema_cookie: record.schema_cookie,
                before_hash: None,
                before: record.after.clone(),
                after: if operation == "delete" {
                    None
                } else {
                    record.before.clone()
                },
                column_mask: record.column_mask.clone(),
                tombstone: operation == "delete",
                conflict_metadata: None,
            });
        }
        let source_replica_id = format!("inverse:{}", changeset.source_replica_id);
        let mut inverse = SyncChangeset {
            changeset_version: crate::sync::SYNC_CHANGESET_VERSION,
            changeset_id: sync_changeset_id(
                "inverse",
                &source_replica_id,
                created_at_micros,
                inverse_records.len(),
            ),
            source_replica_id,
            source_kind: changeset.source_kind.clone(),
            tenant_id: changeset.tenant_id.clone(),
            scope_name: changeset.scope_name.clone(),
            shape_id: changeset.shape_id.clone(),
            base_kind: format!("inverse:{}", changeset.base_kind),
            base_checkpoint: changeset.base_checkpoint.clone(),
            base_branch: changeset.base_branch.clone(),
            base_snapshot: changeset.base_snapshot.clone(),
            start_checkpoint: inverse_records.first().map(|record| record.origin_sequence),
            end_checkpoint: inverse_records.last().map(|record| record.origin_sequence),
            source_high_watermark: inverse_records.last().map(|record| record.origin_sequence),
            schema_fingerprint: changeset.schema_fingerprint.clone(),
            schema_cookie: changeset.schema_cookie,
            sync_contract_version: changeset.sync_contract_version,
            query_contract_fingerprint: changeset.query_contract_fingerprint.clone(),
            producer_capabilities: SyncChangesetCapabilities {
                before_images: true,
                ..SyncChangesetCapabilities::default()
            },
            limits: SyncChangesetLimits::default(),
            records: inverse_records,
            conflict_policy_hint: changeset.conflict_policy_hint.clone(),
            created_at_micros,
            integrity_hash: None,
        };
        self.sync_finalize_changeset(&mut inverse, None)?;
        Ok(inverse)
    }

    pub fn sync_create_shape(&self, options: CreateShapeOptions) -> Result<SyncShape> {
        self.ensure_sync_tables()?;
        let shape_id = options.shape_id.trim();
        let scope_name = options.scope_name.trim();
        let tenant_id = options.tenant_id.trim();
        if shape_id.is_empty() {
            return Err(DbError::sql("sync shape_id must not be empty"));
        }
        if scope_name.is_empty() {
            return Err(DbError::sql("sync shape scope_name must not be empty"));
        }
        if tenant_id.is_empty() {
            return Err(DbError::sql(
                "TENANT_REQUIRED: sync shape tenant_id is required",
            ));
        }
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        if scope.include_tables.is_empty() {
            return Err(DbError::sql(format!(
                "sync scope '{scope_name}' has no included tables"
            )));
        }
        let name = options.name.as_deref().unwrap_or(shape_id).trim();
        if name.is_empty() {
            return Err(DbError::sql("sync shape name must not be empty"));
        }
        let now = current_time_micros();
        let existing = self.sync_shape(shape_id)?;
        let created_at_micros = existing
            .as_ref()
            .map(|shape| shape.created_at_micros)
            .unwrap_or(now);
        let retention_ttl_micros = options
            .retention_ttl_micros
            .unwrap_or(30 * 24 * 60 * 60 * 1_000_000);
        let max_records = options.max_records.unwrap_or(50_000);
        let ack_deadline_micros = options.ack_deadline_micros.unwrap_or(30_000_000);
        let heartbeat_micros = options.heartbeat_micros.unwrap_or(20_000_000);
        let allowed_roles_json =
            serde_json::to_string(&options.allowed_roles).map_err(|error| {
                DbError::internal(format!("failed to encode shape allowed roles: {error}"))
            })?;
        let allowed_subjects_json =
            serde_json::to_string(&options.allowed_subjects).map_err(|error| {
                DbError::internal(format!("failed to encode shape allowed subjects: {error}"))
            })?;
        let sql = format!(
            "INSERT INTO {table} (shape_id, name, scope_name, tenant_id, allowed_roles_json, allowed_subjects_json, created_at_micros, updated_at_micros, retention_ttl_micros, max_records, ack_deadline_micros, heartbeat_micros) VALUES ({shape_id}, {name}, {scope_name}, {tenant_id}, {allowed_roles_json}, {allowed_subjects_json}, {created_at_micros}, {updated_at_micros}, {retention_ttl_micros}, {max_records}, {ack_deadline_micros}, {heartbeat_micros}) ON CONFLICT (shape_id) DO UPDATE SET name = {name}, scope_name = {scope_name}, tenant_id = {tenant_id}, allowed_roles_json = {allowed_roles_json}, allowed_subjects_json = {allowed_subjects_json}, updated_at_micros = {updated_at_micros}, retention_ttl_micros = {retention_ttl_micros}, max_records = {max_records}, ack_deadline_micros = {ack_deadline_micros}, heartbeat_micros = {heartbeat_micros}",
            table = crate::sync::SHAPES_TABLE,
            shape_id = sql_text_literal(shape_id),
            name = sql_text_literal(name),
            scope_name = sql_text_literal(&scope.name),
            tenant_id = sql_text_literal(tenant_id),
            allowed_roles_json = sql_text_literal(&allowed_roles_json),
            allowed_subjects_json = sql_text_literal(&allowed_subjects_json),
            created_at_micros = created_at_micros,
            updated_at_micros = now,
            retention_ttl_micros = retention_ttl_micros,
            max_records = max_records,
            ack_deadline_micros = ack_deadline_micros,
            heartbeat_micros = heartbeat_micros,
        );
        let _ = self.execute(&sql)?;
        self.sync_shape(shape_id)?
            .ok_or_else(|| DbError::internal("sync shape missing after create/update"))
    }

    pub fn sync_drop_shape(&self, shape_id: &str) -> Result<bool> {
        self.ensure_sync_tables()?;
        let shape_id = shape_id.trim();
        if shape_id.is_empty() {
            return Err(DbError::sql("sync shape_id must not be empty"));
        }
        let _ = self.execute(&format!(
            "DELETE FROM {} WHERE shape_id = {}",
            crate::sync::SHAPE_CLIENTS_TABLE,
            sql_text_literal(shape_id)
        ))?;
        let result = self.execute(&format!(
            "DELETE FROM {} WHERE shape_id = {}",
            crate::sync::SHAPES_TABLE,
            sql_text_literal(shape_id)
        ))?;
        Ok(result.affected_rows() > 0)
    }

    pub fn sync_shape(&self, shape_id: &str) -> Result<Option<SyncShape>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT shape_id, name, scope_name, tenant_id, allowed_roles_json, allowed_subjects_json, created_at_micros, updated_at_micros, retention_ttl_micros, max_records, ack_deadline_micros, heartbeat_micros FROM {} WHERE shape_id = {}",
            crate::sync::SHAPES_TABLE,
            sql_text_literal(shape_id)
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().first().map(sync_shape_from_row).transpose(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_shapes(&self) -> Result<Vec<SyncShape>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT shape_id, name, scope_name, tenant_id, allowed_roles_json, allowed_subjects_json, created_at_micros, updated_at_micros, retention_ttl_micros, max_records, ack_deadline_micros, heartbeat_micros FROM {} ORDER BY shape_id",
            crate::sync::SHAPES_TABLE,
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_shape_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_shape_clients(&self) -> Result<Vec<SyncShapeClient>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT shape_id, tenant_id, client_replica_id, subject_id, session_id, last_ack_sequence, last_ack_watermark, last_changeset_id, last_seen_at_micros, retention_blocking, status FROM {} ORDER BY shape_id, client_replica_id",
            crate::sync::SHAPE_CLIENTS_TABLE,
        );
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(sync_shape_client_from_row)
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_shape_snapshot(
        &self,
        shape_id: &str,
        _client_replica_id: &str,
        principal: Option<SyncPrincipal>,
    ) -> Result<SyncShapeDelivery> {
        self.ensure_sync_tables()?;
        let shape = self.sync_shape(shape_id)?.ok_or_else(|| {
            DbError::sql(format!(
                "SHAPE_NOT_FOUND: sync shape '{shape_id}' not found"
            ))
        })?;
        self.sync_authorize_shape(principal.as_ref(), &shape)?;
        let scope = self
            .sync_scope(&shape.scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{}' not found", shape.scope_name)))?;
        let changeset =
            self.sync_create_shape_snapshot_changeset(&shape, &scope, principal.as_ref())?;
        let shape_sequence = changeset
            .source_high_watermark
            .or(changeset.end_checkpoint)
            .unwrap_or(0);
        Ok(SyncShapeDelivery {
            message_type: "snapshot".to_string(),
            shape_id: shape.shape_id,
            shape_sequence,
            ack_deadline_micros: current_time_micros() + shape.ack_deadline_micros,
            checkpoint: SyncShapeCheckpoint {
                shape_sequence,
                source_high_watermark: changeset.source_high_watermark.unwrap_or(shape_sequence),
            },
            changeset,
        })
    }

    pub fn sync_shape_changes(
        &self,
        shape_id: &str,
        since_watermark: u64,
        principal: Option<SyncPrincipal>,
    ) -> Result<SyncShapeDelivery> {
        let shape = self.sync_shape(shape_id)?.ok_or_else(|| {
            DbError::sql(format!(
                "SHAPE_NOT_FOUND: sync shape '{shape_id}' not found"
            ))
        })?;
        self.sync_authorize_shape(principal.as_ref(), &shape)?;
        let retention = self.sync_retention_report()?;
        if let Some(first_sequence) = retention.first_sequence {
            if since_watermark > 0 && since_watermark < first_sequence {
                return Err(DbError::sql(format!(
                    "SHAPE_RESYNC_REQUIRED: since checkpoint {since_watermark} is below retained first sequence {first_sequence}"
                )));
            }
        }
        let changeset = self.sync_create_changeset(CreateChangesetOptions {
            source: SyncChangesetSource::Checkpoint {
                peer: shape_id.to_string(),
                since_sequence: since_watermark,
            },
            scope_name: Some(shape.scope_name.clone()),
            shape_id: Some(shape.shape_id.clone()),
            max_records: Some(shape.max_records),
            max_bytes: None,
            principal,
        })?;
        let shape_sequence = changeset
            .source_high_watermark
            .or(changeset.end_checkpoint)
            .unwrap_or(since_watermark);
        Ok(SyncShapeDelivery {
            message_type: "changeset".to_string(),
            shape_id: shape.shape_id,
            shape_sequence,
            ack_deadline_micros: current_time_micros() + shape.ack_deadline_micros,
            checkpoint: SyncShapeCheckpoint {
                shape_sequence,
                source_high_watermark: changeset.source_high_watermark.unwrap_or(shape_sequence),
            },
            changeset,
        })
    }

    pub fn sync_ack_shape(&self, ack: ShapeAckOptions) -> Result<SyncShapeClient> {
        self.sync_ack_shape_with_principal(ack, None)
    }

    pub fn sync_ack_shape_with_principal(
        &self,
        ack: ShapeAckOptions,
        principal: Option<&SyncPrincipal>,
    ) -> Result<SyncShapeClient> {
        self.ensure_sync_tables()?;
        let shape = self.sync_shape(&ack.shape_id)?.ok_or_else(|| {
            DbError::sql(format!(
                "SHAPE_NOT_FOUND: sync shape '{}' not found",
                ack.shape_id
            ))
        })?;
        self.sync_authorize_shape(principal, &shape)?;
        if !shape.tenant_id.eq_ignore_ascii_case(&ack.tenant_id) {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: shape '{}' belongs to tenant '{}'",
                shape.shape_id, shape.tenant_id
            )));
        }
        let now = current_time_micros();
        let sql = format!(
            "INSERT INTO {table} (shape_id, tenant_id, client_replica_id, subject_id, session_id, last_ack_sequence, last_ack_watermark, last_changeset_id, last_seen_at_micros, retention_blocking, status) VALUES ({shape_id}, {tenant_id}, {client_replica_id}, {subject_id}, {session_id}, {last_ack_sequence}, {last_ack_watermark}, {last_changeset_id}, {last_seen_at_micros}, 1, 'active') ON CONFLICT (shape_id, client_replica_id) DO UPDATE SET tenant_id = {tenant_id}, subject_id = {subject_id}, session_id = {session_id}, last_ack_sequence = {last_ack_sequence}, last_ack_watermark = {last_ack_watermark}, last_changeset_id = {last_changeset_id}, last_seen_at_micros = {last_seen_at_micros}, retention_blocking = 1, status = 'active'",
            table = crate::sync::SHAPE_CLIENTS_TABLE,
            shape_id = sql_text_literal(&shape.shape_id),
            tenant_id = sql_text_literal(&ack.tenant_id),
            client_replica_id = sql_text_literal(&ack.client_replica_id),
            subject_id = sql_text_literal(&ack.subject_id),
            session_id = sql_nullable_text_literal(ack.session_id.as_deref()),
            last_ack_sequence = ack.shape_sequence,
            last_ack_watermark = ack.source_high_watermark,
            last_changeset_id = sql_nullable_text_literal(ack.changeset_id.as_deref()),
            last_seen_at_micros = now,
        );
        let _ = self.execute(&sql)?;
        self.sync_shape_clients()?
            .into_iter()
            .find(|client| {
                client.shape_id == shape.shape_id
                    && client.client_replica_id == ack.client_replica_id
            })
            .ok_or_else(|| DbError::internal("sync shape client missing after ack"))
    }

    pub fn sync_relay_status(
        &self,
        relay_id: Option<&str>,
        production_mode: bool,
        secure_transport_required: bool,
        insecure_override_enabled: bool,
        started_at_micros: Option<i64>,
    ) -> Result<SyncRelayStatus> {
        let status = self.sync_status()?;
        let active_sessions = self
            .sync_relay_sessions()?
            .into_iter()
            .filter(|session| session.ended_at_micros.is_none() && session.status == "started")
            .count() as u64;
        Ok(SyncRelayStatus {
            relay_id: relay_id.unwrap_or("relay-local").to_string(),
            protocol_version: crate::sync::SYNC_RELAY_PROTOCOL_VERSION,
            database_replica_id: status.replica_id,
            production_mode,
            secure_transport_required,
            insecure_override_enabled,
            active_sessions,
            active_streams: 0,
            started_at_micros: started_at_micros.unwrap_or_else(current_time_micros),
        })
    }

    pub fn sync_relay_sessions(&self) -> Result<Vec<SyncRelaySession>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT session_id, tenant_id, subject_id, subject_kind, request_id, operation, scope_name, shape_id, started_at_micros, ended_at_micros, status, error, rows_seen, bytes_seen FROM {} ORDER BY started_at_micros, session_id",
            crate::sync::RELAY_SESSIONS_TABLE,
        );
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(sync_relay_session_from_row)
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_start_relay_session(
        &self,
        principal: &SyncPrincipal,
        operation: &str,
        scope_name: Option<&str>,
        shape_id: Option<&str>,
    ) -> Result<SyncRelaySession> {
        self.ensure_sync_tables()?;
        principal.validate()?;
        let started_at_micros = current_time_micros();
        let session_id = principal.session_id.clone();
        let sql = format!(
            "INSERT INTO {table} (session_id, tenant_id, subject_id, subject_kind, request_id, operation, scope_name, shape_id, started_at_micros, ended_at_micros, status, error, rows_seen, bytes_seen) VALUES ({session_id}, {tenant_id}, {subject_id}, {subject_kind}, {request_id}, {operation}, {scope_name}, {shape_id}, {started_at_micros}, NULL, 'started', NULL, 0, 0) ON CONFLICT (session_id) DO UPDATE SET tenant_id = {tenant_id}, subject_id = {subject_id}, subject_kind = {subject_kind}, request_id = {request_id}, operation = {operation}, scope_name = {scope_name}, shape_id = {shape_id}, started_at_micros = {started_at_micros}, ended_at_micros = NULL, status = 'started', error = NULL",
            table = crate::sync::RELAY_SESSIONS_TABLE,
            session_id = sql_text_literal(&session_id),
            tenant_id = sql_text_literal(&principal.tenant_id),
            subject_id = sql_text_literal(&principal.subject_id),
            subject_kind = sql_text_literal(principal.subject_kind.as_str()),
            request_id = sql_text_literal(&principal.request_id),
            operation = sql_text_literal(operation),
            scope_name = sql_nullable_text_literal(scope_name),
            shape_id = sql_nullable_text_literal(shape_id),
            started_at_micros = started_at_micros,
        );
        let _ = self.execute(&sql)?;
        self.sync_relay_sessions()?
            .into_iter()
            .find(|session| session.session_id == session_id)
            .ok_or_else(|| DbError::internal("sync relay session missing after start"))
    }

    pub fn sync_finish_relay_session(
        &self,
        session_id: &str,
        status: &str,
        error: Option<&str>,
        rows_seen: u64,
        bytes_seen: u64,
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "UPDATE {table} SET ended_at_micros = {ended_at_micros}, status = {status}, error = {error}, rows_seen = {rows_seen}, bytes_seen = {bytes_seen} WHERE session_id = {session_id}",
            table = crate::sync::RELAY_SESSIONS_TABLE,
            ended_at_micros = current_time_micros(),
            status = sql_text_literal(status),
            error = sql_nullable_text_literal(error),
            rows_seen = rows_seen,
            bytes_seen = bytes_seen,
            session_id = sql_text_literal(session_id),
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }

    pub fn sync_changeset_history(&self) -> Result<Vec<SyncChangesetHistory>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT changeset_id, source_replica_id, source_kind, scope_name, shape_id, record_count, bytes, created_at_micros, applied_at_micros, outcome, integrity_hash FROM {} ORDER BY created_at_micros, changeset_id",
            crate::sync::CHANGESET_HISTORY_TABLE,
        );
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(sync_changeset_history_from_row)
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    fn sync_authorize_scope(
        &self,
        principal: Option<&SyncPrincipal>,
        scope_name: &str,
    ) -> Result<()> {
        if let Some(principal) = principal {
            if !principal.allows_scope(scope_name) {
                return Err(DbError::sql(format!(
                    "SCOPE_UNAUTHORIZED: principal '{}' cannot access scope '{}'",
                    principal.subject_id, scope_name
                )));
            }
        }
        Ok(())
    }

    fn sync_authorize_shape(
        &self,
        principal: Option<&SyncPrincipal>,
        shape: &SyncShape,
    ) -> Result<()> {
        let Some(principal) = principal else {
            return Ok(());
        };
        if !principal.tenant_id.eq_ignore_ascii_case(&shape.tenant_id) {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: shape '{}' belongs to tenant '{}'",
                shape.shape_id, shape.tenant_id
            )));
        }
        if !principal.allows_shape(&shape.shape_id) {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: principal '{}' cannot access shape '{}'",
                principal.subject_id, shape.shape_id
            )));
        }
        if !shape.allowed_subjects.is_empty()
            && !shape
                .allowed_subjects
                .iter()
                .any(|subject| subject == "*" || subject == &principal.subject_id)
        {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: subject '{}' is not allowed for shape '{}'",
                principal.subject_id, shape.shape_id
            )));
        }
        if !shape.allowed_roles.is_empty()
            && !principal.roles.iter().any(|role| {
                shape
                    .allowed_roles
                    .iter()
                    .any(|allowed| allowed == "*" || allowed == role)
            })
        {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: principal '{}' lacks a role for shape '{}'",
                principal.subject_id, shape.shape_id
            )));
        }
        Ok(())
    }

    fn sync_create_shape_snapshot_changeset(
        &self,
        shape: &SyncShape,
        scope: &SyncScope,
        principal: Option<&SyncPrincipal>,
    ) -> Result<SyncChangeset> {
        let created_at_micros = current_time_micros();
        let tooling = self.get_tooling_metadata()?;
        let runtime = self.runtime_for_metadata_inspection()?;
        let schema_cookie = runtime.catalog.schema_cookie;
        let source_replica_id = self
            .sync_status()
            .ok()
            .and_then(|status| status.replica_id)
            .unwrap_or_else(|| "snapshot".to_string());
        let mut records = Vec::new();
        let mut origin_sequence = 1u64;
        for table_name in &scope.include_tables {
            let table = runtime.catalog.table(table_name).ok_or_else(|| {
                DbError::sql(format!("sync scope table '{table_name}' does not exist"))
            })?;
            let column_sql = table
                .columns
                .iter()
                .map(|column| sql_identifier(&column.name))
                .collect::<Vec<_>>()
                .join(", ");
            let order_by = table
                .primary_key_columns
                .iter()
                .map(|column| sql_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            let where_sql = scope
                .row_filter
                .as_ref()
                .map(|filter| format!(" WHERE {filter}"))
                .unwrap_or_default();
            let sql = format!(
                "SELECT {column_sql} FROM {}{where_sql} ORDER BY {order_by}",
                sql_identifier(&table.name)
            );
            let result = self.execute(&sql)?;
            for row in result.rows() {
                let after = crate::sync::build_after_json(table, row.values());
                let primary_key = crate::sync::build_primary_key_json(table, row.values());
                records.push(SyncChangesetRecord {
                    record_version: 1,
                    table: table.name.clone(),
                    operation: "insert".to_string(),
                    primary_key,
                    origin_replica_id: source_replica_id.clone(),
                    origin_sequence,
                    transaction_id: format!("shape-snapshot:{}:{origin_sequence}", shape.shape_id),
                    transaction_lsn: origin_sequence,
                    schema_cookie,
                    before_hash: None,
                    before: None,
                    after: Some(after),
                    column_mask: table
                        .columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                    tombstone: false,
                    conflict_metadata: None,
                });
                origin_sequence += 1;
                if records.len() as u64 > shape.max_records {
                    return Err(DbError::sql(
                        "BATCH_TOO_LARGE: shape snapshot exceeds max_records",
                    ));
                }
            }
        }
        let high_watermark = self.sync_integrity_report()?.last_sequence.unwrap_or(0);
        let mut changeset = SyncChangeset {
            changeset_version: crate::sync::SYNC_CHANGESET_VERSION,
            changeset_id: sync_changeset_id(
                "shape_snapshot",
                &source_replica_id,
                created_at_micros,
                records.len(),
            ),
            source_replica_id,
            source_kind: crate::sync::SyncChangesetSourceKind::Snapshot,
            tenant_id: Some(
                principal
                    .map(|principal| principal.tenant_id.clone())
                    .unwrap_or_else(|| shape.tenant_id.clone()),
            ),
            scope_name: Some(scope.name.clone()),
            shape_id: Some(shape.shape_id.clone()),
            base_kind: "snapshot".to_string(),
            base_checkpoint: None,
            base_branch: None,
            base_snapshot: Some(format!("shape:{}", shape.shape_id)),
            start_checkpoint: records.first().map(|record| record.origin_sequence),
            end_checkpoint: records.last().map(|record| record.origin_sequence),
            source_high_watermark: Some(high_watermark),
            schema_fingerprint: tooling.schema_fingerprint,
            schema_cookie,
            sync_contract_version: crate::sync::SYNC_CONTRACT_VERSION,
            query_contract_fingerprint: None,
            producer_capabilities: SyncChangesetCapabilities::default(),
            limits: SyncChangesetLimits::default(),
            records,
            conflict_policy_hint: None,
            created_at_micros,
            integrity_hash: None,
        };
        self.sync_finalize_changeset(&mut changeset, None)?;
        self.sync_record_changeset_history(&changeset, "created", None)?;
        Ok(changeset)
    }

    fn sync_finalize_changeset(
        &self,
        changeset: &mut SyncChangeset,
        max_bytes: Option<u64>,
    ) -> Result<()> {
        changeset.limits.record_count = changeset.records.len() as u64;
        changeset.limits.uncompressed_bytes = 0;
        changeset.integrity_hash = None;
        let bytes = serde_json::to_vec(changeset)
            .map_err(|error| DbError::internal(format!("failed to serialize changeset: {error}")))?
            .len() as u64;
        if max_bytes.is_some_and(|limit| bytes > limit) {
            return Err(DbError::sql(format!(
                "BATCH_TOO_LARGE: changeset is {bytes} bytes"
            )));
        }
        changeset.limits.uncompressed_bytes = bytes;
        let hash = self.sync_changeset_integrity_hash(changeset)?;
        changeset.integrity_hash = Some(hash);
        Ok(())
    }

    fn sync_validate_changeset_envelope(&self, changeset: &SyncChangeset) -> Result<()> {
        if changeset.changeset_version != crate::sync::SYNC_CHANGESET_VERSION {
            return Err(DbError::sql(format!(
                "CHANGESET_UNSUPPORTED: unsupported changeset version {}",
                changeset.changeset_version
            )));
        }
        if changeset.sync_contract_version != crate::sync::SYNC_CONTRACT_VERSION {
            return Err(DbError::sql(format!(
                "CHANGESET_UNSUPPORTED: unsupported sync contract version {}",
                changeset.sync_contract_version
            )));
        }
        if changeset.changeset_id.trim().is_empty() {
            return Err(DbError::sql("CHANGESET_INVALID: changeset_id is required"));
        }
        let Some(expected_hash) = changeset.integrity_hash.as_deref() else {
            return Err(DbError::sql(
                "CHANGESET_INVALID: integrity_hash is required",
            ));
        };
        let actual_hash = self.sync_changeset_integrity_hash(changeset)?;
        if expected_hash != actual_hash {
            return Err(DbError::sql(
                "CHANGESET_INVALID: integrity_hash does not match payload",
            ));
        }
        for (index, record) in changeset.records.iter().enumerate() {
            if record.record_version != 1 {
                return Err(DbError::sql(format!(
                    "CHANGESET_UNSUPPORTED: record {index} uses version {}",
                    record.record_version
                )));
            }
            match record.operation.as_str() {
                "insert" | "update" if record.after.is_none() => {
                    return Err(DbError::sql(format!(
                        "CHANGESET_INVALID: record {index} operation '{}' requires after image",
                        record.operation
                    )));
                }
                "insert" | "update" | "delete" => {}
                other => {
                    return Err(DbError::sql(format!(
                        "CHANGESET_INVALID: unsupported record operation '{other}'"
                    )));
                }
            }
        }
        Ok(())
    }

    fn sync_check_changeset_compatibility(&self, changeset: &SyncChangeset) -> Result<()> {
        let tooling = self.get_tooling_metadata()?;
        if tooling.schema_fingerprint != changeset.schema_fingerprint {
            return Err(DbError::sql(format!(
                "SCHEMA_INCOMPATIBLE: local schema fingerprint {} does not match changeset {}",
                tooling.schema_fingerprint, changeset.schema_fingerprint
            )));
        }
        let runtime = self.runtime_for_metadata_inspection()?;
        if runtime.catalog.schema_cookie != changeset.schema_cookie {
            return Err(DbError::sql(format!(
                "SCHEMA_INCOMPATIBLE: local schema_cookie {} does not match changeset {}",
                runtime.catalog.schema_cookie, changeset.schema_cookie
            )));
        }
        Ok(())
    }

    fn sync_changeset_integrity_hash(&self, changeset: &SyncChangeset) -> Result<String> {
        let mut clone = changeset.clone();
        clone.integrity_hash = None;
        let bytes = serde_json::to_vec(&clone).map_err(|error| {
            DbError::internal(format!("failed to serialize changeset: {error}"))
        })?;
        let digest = Sha256::digest(&bytes);
        Ok(format!("sha256:{}", hex_encode(&digest)))
    }

    fn sync_record_changeset_history(
        &self,
        changeset: &SyncChangeset,
        outcome: &str,
        applied_at_micros: Option<i64>,
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "INSERT INTO {table} (changeset_id, source_replica_id, source_kind, scope_name, shape_id, record_count, bytes, created_at_micros, applied_at_micros, outcome, integrity_hash) VALUES ({changeset_id}, {source_replica_id}, {source_kind}, {scope_name}, {shape_id}, {record_count}, {bytes}, {created_at_micros}, {applied_at_micros}, {outcome}, {integrity_hash}) ON CONFLICT (changeset_id) DO UPDATE SET applied_at_micros = COALESCE({applied_at_micros}, applied_at_micros), outcome = {outcome}, integrity_hash = {integrity_hash}",
            table = crate::sync::CHANGESET_HISTORY_TABLE,
            changeset_id = sql_text_literal(&changeset.changeset_id),
            source_replica_id = sql_text_literal(&changeset.source_replica_id),
            source_kind = sql_text_literal(changeset.source_kind.as_str()),
            scope_name = sql_nullable_text_literal(changeset.scope_name.as_deref()),
            shape_id = sql_nullable_text_literal(changeset.shape_id.as_deref()),
            record_count = changeset.records.len(),
            bytes = changeset.limits.uncompressed_bytes,
            created_at_micros = changeset.created_at_micros,
            applied_at_micros = applied_at_micros
                .map(|value| value.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            outcome = sql_text_literal(outcome),
            integrity_hash = sql_nullable_text_literal(changeset.integrity_hash.as_deref()),
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }

    pub fn sync_conflict_policy(&self) -> Result<SyncConflictPolicyConfig> {
        let default_policy = self
            .sync_read_metadata("conflict_policy")?
            .map(|value| SyncConflictPolicy::from_str(&value))
            .transpose()?
            .unwrap_or_default();
        let origin_priority = match self.sync_read_metadata("conflict_origin_priority")? {
            Some(value) => serde_json::from_str::<Vec<String>>(&value).map_err(|error| {
                DbError::sql(format!(
                    "invalid sync conflict origin priority metadata: {error}"
                ))
            })?,
            None => Vec::new(),
        };
        Ok(SyncConflictPolicyConfig {
            default_policy,
            origin_priority,
        })
    }

    pub fn sync_set_conflict_policy(
        &self,
        policy: SyncConflictPolicy,
        origin_priority: &[&str],
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let origin_priority = origin_priority
            .iter()
            .map(|value| value.trim())
            .map(|value| {
                if value.is_empty() {
                    Err(DbError::sql(
                        "sync conflict origin priority entries must not be empty",
                    ))
                } else {
                    Ok(value.to_string())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        self.sync_upsert_metadata("conflict_policy", policy.as_str())?;
        self.sync_upsert_metadata(
            "conflict_origin_priority",
            &serde_json::to_string(&origin_priority).map_err(|error| {
                DbError::internal(format!(
                    "failed to serialize sync conflict origin priority: {error}"
                ))
            })?,
        )?;
        Ok(())
    }

    pub fn sync_import_batch(&self, batch: &SyncChangeBatch) -> Result<SyncImportSummary> {
        let policy = self.sync_conflict_policy()?.default_policy;
        self.sync_import_batch_with_policy(batch, policy)
    }

    pub fn sync_import_batch_with_policy(
        &self,
        batch: &SyncChangeBatch,
        policy: SyncConflictPolicy,
    ) -> Result<SyncImportSummary> {
        batch.validate()?;
        self.ensure_sync_tables()?;

        let runtime = self.runtime_for_metadata_inspection()?;
        let schema_cookie = runtime.catalog.schema_cookie;
        let local_replica_id = self
            .inner
            .sync_ctx
            .replica_id()
            .or_else(|| self.sync_read_metadata("replica_id").ok().flatten());
        let batch_source_replica_id = batch.source_replica_id.as_deref();
        let batch_watermark = batch.source_high_watermark.or(batch.last_sequence);
        let current_peer_watermark = match batch_source_replica_id {
            Some(replica_id) => self.sync_peer_watermark(replica_id)?,
            None => None,
        };

        if let Some(local_replica_id) = local_replica_id.as_deref() {
            if batch_source_replica_id == Some(local_replica_id) {
                return Err(DbError::sql(format!(
                    "cannot import batch from same replica '{}'",
                    local_replica_id
                )));
            }
        }

        let _suppress_capture = self.inner.sync_ctx.suppress_capture();
        struct SyncImportTransaction<'a>(&'a Db, bool);
        impl<'a> SyncImportTransaction<'a> {
            fn new(db: &'a Db) -> Result<Self> {
                db.begin_transaction()?;
                Ok(Self(db, true))
            }
            fn commit(mut self) -> Result<()> {
                self.1 = false;
                self.0.commit_transaction()?;
                Ok(())
            }
        }
        impl Drop for SyncImportTransaction<'_> {
            fn drop(&mut self) {
                if self.1 {
                    let _ = self.0.rollback_transaction();
                }
            }
        }

        if let Some(batch_watermark) = batch_watermark {
            if current_peer_watermark.is_some_and(|watermark| batch_watermark <= watermark) {
                if let Some(replica_id) = batch_source_replica_id {
                    let watermark = current_peer_watermark
                        .map_or(batch_watermark, |current| current.max(batch_watermark));
                    self.sync_upsert_metadata(
                        &peer_watermark_key(replica_id),
                        &watermark.to_string(),
                    )?;
                }
                return Ok(SyncImportSummary {
                    seen: batch.record_count,
                    applied: 0,
                    skipped: batch.record_count,
                    conflicted: 0,
                });
            }
        }

        let tx = SyncImportTransaction::new(self)?;
        let mut applied = 0usize;
        let mut skipped = 0usize;
        let mut conflicted = 0usize;
        let mut stop_conflict: Option<(SyncJournalRecord, SyncConflictRecordData)> = None;

        for record in &batch.records {
            if let Some(local_replica_id) = local_replica_id.as_deref() {
                if record.replica_id == local_replica_id {
                    return Err(DbError::sql(format!(
                        "cannot import record from same replica '{}'",
                        local_replica_id
                    )));
                }
            }

            if let Some(watermark) = current_peer_watermark {
                if record.sequence <= watermark {
                    skipped += 1;
                    continue;
                }
            }

            if record.schema_version != 1 {
                return Err(DbError::sql(format!(
                    "unsupported sync record schema version {}",
                    record.schema_version
                )));
            }

            if record.schema_cookie != schema_cookie {
                return Err(DbError::sql(format!(
                    "schema mismatch for table '{}': record has schema_cookie {} but local schema is {}",
                    record.table, record.schema_cookie, schema_cookie
                )));
            }

            let table = runtime
                .catalog
                .table(&record.table)
                .ok_or_else(|| DbError::sql(format!("unknown table '{}'", record.table)))?;
            if crate::sync::is_internal_table_name(&table.name) {
                return Err(DbError::sql(format!(
                    "cannot import into internal table '{}'",
                    table.name
                )));
            }
            let marker_key = imported_record_key(&record.replica_id, record.sequence);
            if self.sync_read_metadata(&marker_key)?.is_some() {
                skipped += 1;
                continue;
            }

            let outcome = self.sync_apply_import_record(batch, record, table, &policy)?;
            match outcome {
                SyncImportRecordOutcome::Applied => {
                    self.sync_upsert_metadata(&marker_key, "applied")?;
                    applied += 1;
                }
                SyncImportRecordOutcome::Conflict(conflict) => {
                    if matches!(policy, SyncConflictPolicy::Stop) {
                        stop_conflict = Some((record.clone(), conflict));
                        break;
                    }
                    self.record_sync_conflict_with_data(batch, record, &conflict)?;
                    conflicted += 1;
                }
                SyncImportRecordOutcome::Resolved(conflict) => {
                    self.sync_upsert_metadata(&marker_key, "applied")?;
                    self.record_sync_conflict_with_data(batch, record, &conflict)?;
                    applied += 1;
                    conflicted += 1;
                }
            }
        }

        if let Some((record, conflict)) = stop_conflict {
            drop(tx);
            let conflict_id = self.record_sync_conflict_with_data(batch, &record, &conflict)?;
            return Err(DbError::sql(format!(
                "sync import stopped on conflict {}",
                conflict_id
            )));
        }

        if let (Some(replica_id), Some(batch_watermark)) =
            (batch_source_replica_id, batch_watermark)
        {
            let watermark = current_peer_watermark
                .map_or(batch_watermark, |current| current.max(batch_watermark));
            self.sync_upsert_metadata(&peer_watermark_key(replica_id), &watermark.to_string())?;
        }

        crate::reactive::with_change_source(ChangeSource::SyncApply, || tx.commit())?;
        Ok(SyncImportSummary {
            seen: batch.record_count,
            applied,
            skipped,
            conflicted,
        })
    }

    pub fn sync_import_records(&self, records: &[SyncJournalRecord]) -> Result<SyncImportSummary> {
        let batch = SyncChangeBatch::from_records(records.to_vec())?;
        self.sync_import_batch(&batch)
    }

    pub fn sync_peer_watermark(&self, replica_id: &str) -> Result<Option<u64>> {
        match self.sync_read_metadata(&peer_watermark_key(replica_id))? {
            Some(value) => value
                .parse::<u64>()
                .map(Some)
                .map_err(|error| DbError::sql(format!("invalid peer watermark value: {error}"))),
            None => Ok(None),
        }
    }

    pub fn sync_peer_out_watermark(&self, peer_name: &str) -> Result<Option<u64>> {
        match self.sync_read_metadata(&peer_out_watermark_key(peer_name))? {
            Some(value) => value.parse::<u64>().map(Some).map_err(|error| {
                DbError::sql(format!("invalid peer outbound watermark value: {error}"))
            }),
            None => Ok(None),
        }
    }

    pub fn sync_set_peer_out_watermark(&self, peer_name: &str, watermark: u64) -> Result<()> {
        self.ensure_sync_tables()?;
        self.sync_upsert_metadata(&peer_out_watermark_key(peer_name), &watermark.to_string())
    }

    pub fn sync_conflicts(&self) -> Result<Vec<SyncConflict>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT * FROM {} WHERE resolved = 0 ORDER BY conflict_id",
            crate::sync::CONFLICTS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_conflict_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_conflicts_all(&self) -> Result<Vec<SyncConflict>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT * FROM {} ORDER BY conflict_id",
            crate::sync::CONFLICTS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_conflict_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_conflict(&self, conflict_id: i64) -> Result<Option<SyncConflict>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT * FROM {} WHERE conflict_id = {}",
            crate::sync::CONFLICTS_TABLE,
            conflict_id
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result
                .rows()
                .first()
                .map(sync_conflict_from_row)
                .transpose()?),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn sync_resolve_conflict_keep_local(
        &self,
        conflict_id: i64,
        resolved_by: Option<&str>,
        note: Option<&str>,
    ) -> Result<bool> {
        self.sync_update_conflict_resolution(
            conflict_id,
            Some("keep_local"),
            resolved_by,
            note,
            Some(current_time_micros()),
        )
    }

    pub fn sync_resolve_conflict_apply_remote(
        &self,
        conflict_id: i64,
        resolved_by: Option<&str>,
        note: Option<&str>,
    ) -> Result<bool> {
        let Some(conflict) = self.sync_conflict(conflict_id)? else {
            return Ok(false);
        };
        let record: SyncJournalRecord = serde_json::from_value(conflict.remote_record_json.clone())
            .map_err(|error| {
                DbError::corruption(format!(
                    "malformed sync conflict remote_record_json: {error}"
                ))
            })?;
        let batch = SyncChangeBatch::from_records(vec![record.clone()])?;
        let policy = SyncConflictPolicy::LastWriterWins;
        let _suppress_capture = self.inner.sync_ctx.suppress_capture();
        let tx = {
            self.begin_transaction()?;
            struct Tx<'a>(&'a Db, bool);
            impl<'a> Drop for Tx<'a> {
                fn drop(&mut self) {
                    if self.1 {
                        let _ = self.0.rollback_transaction();
                    }
                }
            }
            impl<'a> Tx<'a> {
                fn commit(mut self) -> Result<()> {
                    self.1 = false;
                    self.0.commit_transaction()?;
                    Ok(())
                }
            }
            Tx(self, true)
        };
        let runtime = self.runtime_for_metadata_inspection()?;
        let table = runtime
            .catalog
            .table(&record.table)
            .ok_or_else(|| DbError::sql(format!("unknown table '{}'", record.table)))?;
        match self.sync_apply_import_record(&batch, &record, table, &policy)? {
            SyncImportRecordOutcome::Applied => {
                self.sync_upsert_metadata(
                    &imported_record_key(&record.replica_id, record.sequence),
                    "applied",
                )?;
                self.sync_update_conflict_resolution(
                    conflict_id,
                    Some("apply_remote"),
                    resolved_by,
                    note,
                    Some(current_time_micros()),
                )?;
                tx.commit()?;
                Ok(true)
            }
            SyncImportRecordOutcome::Resolved(_) => {
                self.sync_upsert_metadata(
                    &imported_record_key(&record.replica_id, record.sequence),
                    "applied",
                )?;
                self.sync_update_conflict_resolution(
                    conflict_id,
                    Some("apply_remote"),
                    resolved_by,
                    note,
                    Some(current_time_micros()),
                )?;
                tx.commit()?;
                Ok(true)
            }
            SyncImportRecordOutcome::Conflict(conflict) => {
                let _ = conflict;
                Err(DbError::sql(format!(
                    "cannot apply remote conflict {} because replay now fails",
                    conflict_id
                )))
            }
        }
    }

    pub fn sync_reopen_conflict(&self, conflict_id: i64) -> Result<bool> {
        self.sync_update_conflict_resolution(conflict_id, None, None, None, None)
    }

    pub fn sync_prune_journal_through(&self, sequence: u64) -> Result<usize> {
        self.sync_prune_journal(sequence, false, false)
            .map(|summary| summary.pruned)
    }

    pub fn sync_prune_journal(
        &self,
        through: u64,
        dry_run: bool,
        allow_data_loss: bool,
    ) -> Result<SyncPruneSummary> {
        let retention = self.sync_retention_report()?;
        let requested_through = through;
        if !allow_data_loss && through > retention.safe_prune_through.unwrap_or(0) {
            let message = if let Some(lowest_watermark) =
                retention.safe_prune_through.map(|value| value + 1)
            {
                format!(
                    "cannot prune through {through}; lowest peer watermark is {lowest_watermark}"
                )
            } else if retention.blocked_by.is_empty() {
                format!("cannot prune through {through}; no peer watermarks are known")
            } else {
                format!("cannot prune through {through}; lowest peer watermark is 0")
            };
            return Err(DbError::sql(message));
        }

        let records = crate::sync::read_journal_records(
            self.inner.sync_ctx.journal_path(),
            &self.inner.vfs,
            0,
            usize::MAX,
        )?;
        if records.is_empty() {
            return Ok(SyncPruneSummary {
                requested_through,
                effective_through: 0,
                pruned: 0,
                dry_run,
                allow_data_loss,
                blocked_by: retention.blocked_by,
            });
        }

        let effective_through = records
            .last()
            .map(|record| record.sequence.min(through))
            .unwrap_or(0);
        let total_records = records.len();
        let retained = records
            .into_iter()
            .filter(|record| record.sequence > through)
            .collect::<Vec<_>>();
        let pruned = total_records.saturating_sub(retained.len());

        if dry_run || pruned == 0 {
            return Ok(SyncPruneSummary {
                requested_through,
                effective_through,
                pruned,
                dry_run,
                allow_data_loss,
                blocked_by: retention.blocked_by,
            });
        }

        let mut buffer = Vec::new();
        for record in &retained {
            serde_json::to_writer(&mut buffer, record).map_err(|error| {
                DbError::internal(format!("failed to serialize sync journal record: {error}"))
            })?;
            buffer.push(b'\n');
        }

        if self
            .inner
            .vfs
            .file_exists(self.inner.sync_ctx.journal_path())?
        {
            let journal_file = self.inner.sync_ctx.journal_file_handle()?;
            let journal_file = match journal_file {
                Some(file) => file,
                None => self.inner.vfs.open(
                    self.inner.sync_ctx.journal_path(),
                    OpenMode::OpenExisting,
                    FileKind::SyncJournal,
                )?,
            };

            journal_file.set_len(0)?;
            write_all_at(journal_file.as_ref(), 0, &buffer)?;
            journal_file.sync_data()?;
            self.inner
                .sync_ctx
                .set_journal_write_offset(buffer.len() as u64)?;
        }

        Ok(SyncPruneSummary {
            requested_through,
            effective_through,
            pruned,
            dry_run,
            allow_data_loss,
            blocked_by: retention.blocked_by,
        })
    }

    pub fn sync_set_enabled(&self, enabled: bool) -> Result<()> {
        self.ensure_sync_tables()?;
        self.sync_upsert_metadata("enabled", if enabled { "true" } else { "false" })?;
        self.inner.sync_ctx.set_enabled(enabled);
        if enabled {
            self.inner.sync_ctx.ensure_journal_open(&self.inner.vfs)?;
        }
        Ok(())
    }

    pub fn sync_is_enabled(&self) -> Result<bool> {
        if self.inner.sync_ctx.is_enabled() {
            return Ok(true);
        }
        let status = self.load_sync_status_from_db()?;
        if status.enabled {
            self.inner.sync_ctx.set_enabled(true);
            self.inner
                .sync_ctx
                .set_replica_id(&status.replica_id.unwrap_or_default());
            self.inner.sync_ctx.set_next_sequence(status.next_sequence);
        }
        Ok(status.enabled)
    }

    fn sync_upsert_metadata(&self, key: &str, value: &str) -> Result<()> {
        let sql = format!(
            "INSERT INTO {table} (key, value) VALUES ('{k}', '{v}') ON CONFLICT (key) DO UPDATE SET value = '{v}'",
            table = crate::sync::METADATA_TABLE,
            k = key.replace('\'', "''"),
            v = value.replace('\'', "''"),
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }

    fn sync_read_metadata(&self, key: &str) -> Result<Option<String>> {
        let sql = format!(
            "SELECT value FROM {} WHERE key = '{}'",
            crate::sync::METADATA_TABLE,
            key.replace('\'', "''"),
        );
        match self.execute(&sql) {
            Ok(result) => {
                if let Some(row) = result.rows().first() {
                    if let Some(val) = row.values().first() {
                        match val {
                            Value::Text(s) => return Ok(Some(s.clone())),
                            _ => return Ok(None),
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("no such table") || msg.contains("unknown table") {
                    return Ok(None);
                }
                Err(e)
            }
        }
    }

    fn sync_metadata_entries(&self) -> Result<Vec<(String, String)>> {
        let sql = format!("SELECT key, value FROM {}", crate::sync::METADATA_TABLE);
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(|row| {
                    let key = row
                        .values()
                        .first()
                        .and_then(|value| match value {
                            Value::Text(text) => Some(text.clone()),
                            _ => None,
                        })
                        .ok_or_else(|| DbError::corruption("malformed sync metadata row"))?;
                    let value = row
                        .values()
                        .get(1)
                        .and_then(|value| match value {
                            Value::Text(text) => Some(text.clone()),
                            _ => None,
                        })
                        .ok_or_else(|| DbError::corruption("malformed sync metadata row"))?;
                    Ok((key, value))
                })
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    fn sync_peer_watermark_entries(&self) -> Result<Vec<(String, u64)>> {
        self.sync_metadata_entries()?
            .into_iter()
            .filter_map(|(key, value)| {
                key.strip_prefix("peer_watermark:")
                    .map(|replica_id| (replica_id.to_string(), value))
            })
            .map(|(replica_id, value)| {
                let watermark = value.parse::<u64>().map_err(|error| {
                    DbError::sql(format!(
                        "invalid peer watermark value for replica '{}': {}",
                        replica_id, error
                    ))
                })?;
                Ok((format!("remote:{replica_id}"), watermark))
            })
            .collect()
    }

    fn record_sync_conflict_with_data(
        &self,
        batch: &SyncChangeBatch,
        record: &SyncJournalRecord,
        conflict: &SyncConflictRecordData,
    ) -> Result<i64> {
        let conflict_id = self.next_sync_conflict_id()?;
        let remote_sequence = i64::try_from(record.sequence).map_err(|_| {
            DbError::internal("remote_sequence exceeds INT64 range for sync conflict")
        })?;
        let remote_record_json = serde_json::to_value(record).map_err(|error| {
            DbError::internal(format!(
                "failed to serialize sync record for conflict: {error}"
            ))
        })?;
        let primary_key_json = serde_json::to_value(&record.primary_key).map_err(|error| {
            DbError::internal(format!("failed to serialize sync primary key: {error}"))
        })?;
        let created_at_micros = current_time_micros();
        let local_row_json_text = conflict
            .local_row_json
            .as_ref()
            .map(|value| value.to_string());
        let resolution = conflict.resolution.as_deref().unwrap_or("NULL");
        let resolved_at_micros = conflict
            .resolved_at_micros
            .map_or_else(|| "NULL".to_string(), |value| value.to_string());
        let resolved_by = conflict
            .resolved_by
            .as_deref()
            .map(sql_text_literal)
            .unwrap_or_else(|| "NULL".to_string());
        let resolution_note = conflict
            .resolution_note
            .as_deref()
            .map(sql_text_literal)
            .unwrap_or_else(|| "NULL".to_string());
        let policy_name = conflict
            .policy_name
            .as_deref()
            .map(sql_text_literal)
            .unwrap_or_else(|| "NULL".to_string());
        let sql = format!(
            "INSERT INTO {table} (conflict_id, batch_id, remote_replica_id, remote_sequence, table_name, operation, conflict_type, message, primary_key_json, remote_record_json, local_row_json, created_at_micros, resolved, resolution, resolved_at_micros, resolved_by, resolution_note, policy_name, local_record_json) VALUES ({conflict_id}, {batch_id}, {remote_replica_id}, {remote_sequence}, {table_name}, {operation}, {conflict_type}, {message}, {primary_key_json}, {remote_record_json}, {local_row_json}, {created_at_micros}, {resolved}, {resolution}, {resolved_at_micros}, {resolved_by}, {resolution_note}, {policy_name}, {local_record_json})",
            table = crate::sync::CONFLICTS_TABLE,
            conflict_id = conflict_id,
            batch_id = sql_text_literal(&batch.batch_id),
            remote_replica_id = sql_text_literal(&record.replica_id),
            remote_sequence = remote_sequence,
            table_name = sql_text_literal(&record.table),
            operation = sql_text_literal(&record.operation),
            conflict_type = sql_text_literal(&conflict.conflict_type),
            message = sql_text_literal(&conflict.message),
            primary_key_json = sql_text_literal(&primary_key_json.to_string()),
            remote_record_json = sql_text_literal(&remote_record_json.to_string()),
            local_row_json = local_row_json_text
                .as_deref()
                .map(sql_text_literal)
                .unwrap_or_else(|| "NULL".to_string()),
            created_at_micros = created_at_micros,
            resolved = if conflict.resolution.is_some() { 1 } else { 0 },
            resolution = if conflict.resolution.is_some() {
                sql_text_literal(resolution)
            } else {
                "NULL".to_string()
            },
            resolved_at_micros = resolved_at_micros,
            resolved_by = resolved_by,
            resolution_note = resolution_note,
            policy_name = policy_name,
            local_record_json = conflict
                .local_row_json
                .as_ref()
                .map(|value| sql_text_literal(&value.to_string()))
                .unwrap_or_else(|| "NULL".to_string()),
        );
        let _ = self.execute(&sql)?;
        Ok(conflict_id)
    }

    fn sync_capture_local_row_json(
        &self,
        table: &TableSchema,
        primary_key: &serde_json::Map<String, JsonValue>,
    ) -> Result<Option<serde_json::Value>> {
        let (mut runtime, snapshot_lsn) = self.runtime_for_targeted_row_source_inspection()?;
        if let Some(snapshot_lsn) = snapshot_lsn {
            self.load_runtime_table_row_sources_at_snapshot(
                &mut runtime,
                &[table.name.as_str()],
                snapshot_lsn,
            )?;
        }
        let Some(source) = runtime.table_row_source(&table.name) else {
            return Ok(None);
        };
        for row in source.rows() {
            let row = row?;
            let values = row.values();
            let mut matches = true;
            for pk_col in &table.primary_key_columns {
                let column = table
                    .columns
                    .iter()
                    .find(|column| column.name == *pk_col)
                    .ok_or_else(|| {
                        DbError::sql(format!(
                            "table '{}' missing primary key column '{}'",
                            table.name, pk_col
                        ))
                    })?;
                let json_value = primary_key.get(pk_col).ok_or_else(|| {
                    DbError::sql(format!(
                        "missing primary key column '{pk_col}' in record for table '{}'",
                        table.name
                    ))
                })?;
                let expected = json_to_column_value(&table.name, column, json_value)?;
                let Some(actual) = values.get(
                    table
                        .columns
                        .iter()
                        .position(|candidate| candidate.name == *pk_col)
                        .ok_or_else(|| {
                            DbError::sql(format!(
                                "table '{}' missing primary key column '{}'",
                                table.name, pk_col
                            ))
                        })?,
                ) else {
                    matches = false;
                    break;
                };
                if actual != &expected {
                    matches = false;
                    break;
                }
            }
            if matches {
                return Ok(Some(crate::sync::build_after_json(table, values)));
            }
        }
        Ok(None)
    }

    fn sync_apply_import_record(
        &self,
        _batch: &SyncChangeBatch,
        record: &SyncJournalRecord,
        table: &TableSchema,
        policy: &SyncConflictPolicy,
    ) -> Result<SyncImportRecordOutcome> {
        let primary_key = record
            .primary_key
            .as_object()
            .ok_or_else(|| DbError::sql("primary_key must be an object"))?;
        let local_row_json = self.sync_capture_local_row_json(table, primary_key)?;
        let operation = match record.operation.as_str() {
            "insert" => SyncOperation::Insert,
            "update" => SyncOperation::Update,
            "delete" => SyncOperation::Delete,
            other => return Err(DbError::sql(format!("unsupported operation '{other}'"))),
        };

        let remote_wins = match policy {
            SyncConflictPolicy::Record | SyncConflictPolicy::Stop => false,
            SyncConflictPolicy::LastWriterWins => true,
            SyncConflictPolicy::OriginPriority => {
                let config = self.sync_conflict_policy()?;
                match self
                    .inner
                    .sync_ctx
                    .replica_id()
                    .or_else(|| self.sync_read_metadata("replica_id").ok().flatten())
                {
                    Some(local_replica_id) => {
                        let remote_index = config
                            .origin_priority
                            .iter()
                            .position(|replica| replica == &record.replica_id);
                        let local_index = config
                            .origin_priority
                            .iter()
                            .position(|replica| replica == &local_replica_id);
                        matches!((remote_index, local_index), (Some(remote), Some(local)) if remote < local)
                    }
                    None => false,
                }
            }
        };

        let apply_remote_replace = |operation: SyncOperation| -> Result<()> {
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                sql_identifier(&table.name),
                table
                    .primary_key_columns
                    .iter()
                    .enumerate()
                    .map(|(idx, pk_col)| format!("{} = ${}", sql_identifier(pk_col), idx + 1))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            );
            let mut where_values = Vec::with_capacity(table.primary_key_columns.len());
            for pk_col in &table.primary_key_columns {
                let column = table
                    .columns
                    .iter()
                    .find(|column| column.name == *pk_col)
                    .ok_or_else(|| {
                        DbError::sql(format!(
                            "table '{}' missing primary key column '{}'",
                            table.name, pk_col
                        ))
                    })?;
                let json_value = primary_key.get(pk_col).ok_or_else(|| {
                    DbError::sql(format!(
                        "missing primary key column '{pk_col}' in record for table '{}'",
                        table.name
                    ))
                })?;
                where_values.push(json_to_column_value(&table.name, column, json_value)?);
            }
            let _ = self.execute_with_params(&sql, &where_values)?;

            if matches!(operation, SyncOperation::Delete) {
                return Ok(());
            }

            let after = record
                .after
                .as_ref()
                .ok_or_else(|| DbError::sql("remote record missing after payload"))?
                .as_object()
                .ok_or_else(|| DbError::sql("remote record after payload must be an object"))?;
            let mut columns = Vec::with_capacity(table.columns.len());
            let mut values = Vec::with_capacity(table.columns.len());
            for column in &table.columns {
                let json_value = after.get(&column.name).ok_or_else(|| {
                    DbError::sql(format!(
                        "missing column '{}' in after payload for table '{}'",
                        column.name, table.name
                    ))
                })?;
                columns.push(sql_identifier(&column.name));
                values.push(json_to_column_value(&table.name, column, json_value)?);
            }
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                sql_identifier(&table.name),
                columns.join(", "),
                (1..=values.len())
                    .map(|idx| format!("${idx}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let _ = self.execute_with_params(&sql, &values)?;
            Ok(())
        };

        match operation {
            SyncOperation::Insert => {
                let after = record
                    .after
                    .as_ref()
                    .ok_or_else(|| DbError::sql("insert record missing after payload"))?
                    .as_object()
                    .ok_or_else(|| DbError::sql("after must be an object for insert"))?;
                let mut columns = Vec::with_capacity(table.columns.len());
                let mut values = Vec::with_capacity(table.columns.len());
                for column in &table.columns {
                    let json_value = after.get(&column.name).ok_or_else(|| {
                        DbError::sql(format!(
                            "missing column '{}' in after payload for table '{}'",
                            column.name, table.name
                        ))
                    })?;
                    columns.push(sql_identifier(&column.name));
                    values.push(json_to_column_value(&table.name, column, json_value)?);
                }
                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    sql_identifier(&table.name),
                    columns.join(", "),
                    (1..=values.len())
                        .map(|idx| format!("${idx}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                match self.execute_with_params(&sql, &values) {
                    Ok(_) => Ok(SyncImportRecordOutcome::Applied),
                    Err(DbError::Constraint { message }) if remote_wins => {
                        apply_remote_replace(SyncOperation::Insert)?;
                        Ok(SyncImportRecordOutcome::Resolved(SyncConflictRecordData {
                            conflict_type: "insert_insert".to_string(),
                            message,
                            local_row_json,
                            resolution: Some("remote_applied".to_string()),
                            resolved_at_micros: Some(current_time_micros()),
                            resolved_by: Some("sync_policy".to_string()),
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(DbError::Constraint { message }) => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: if local_row_json.is_some() {
                                "insert_insert".to_string()
                            } else {
                                "constraint_error".to_string()
                            },
                            message,
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(error) => Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                        conflict_type: "apply_error".to_string(),
                        message: error.to_string(),
                        local_row_json,
                        resolution: None,
                        resolved_at_micros: None,
                        resolved_by: None,
                        resolution_note: None,
                        policy_name: Some(policy.as_str().to_string()),
                    })),
                }
            }
            SyncOperation::Update => {
                let after = record
                    .after
                    .as_ref()
                    .ok_or_else(|| DbError::sql("update record missing after payload"))?
                    .as_object()
                    .ok_or_else(|| DbError::sql("after must be an object for update"))?;
                let mut params =
                    Vec::with_capacity(table.columns.len() + table.primary_key_columns.len());
                let mut expressions = Vec::with_capacity(table.columns.len());
                for column in &table.columns {
                    let json_value = after.get(&column.name).ok_or_else(|| {
                        DbError::sql(format!(
                            "missing column '{}' in update payload for table '{}'",
                            column.name, table.name
                        ))
                    })?;
                    params.push(json_to_column_value(&table.name, column, json_value)?);
                    expressions.push(format!(
                        "{} = ${}",
                        sql_identifier(&column.name),
                        params.len()
                    ));
                }
                for pk_col in &table.primary_key_columns {
                    let column = table
                        .columns
                        .iter()
                        .find(|column| column.name == *pk_col)
                        .ok_or_else(|| {
                            DbError::sql(format!(
                                "table '{}' missing primary key column '{}'",
                                table.name, pk_col
                            ))
                        })?;
                    let json_value = primary_key.get(pk_col).ok_or_else(|| {
                        DbError::sql(format!(
                            "missing primary key column '{pk_col}' in record for table '{}'",
                            table.name
                        ))
                    })?;
                    params.push(json_to_column_value(&table.name, column, json_value)?);
                }
                let sql = format!(
                    "UPDATE {} SET {} WHERE {}",
                    sql_identifier(&table.name),
                    expressions.join(", "),
                    table
                        .primary_key_columns
                        .iter()
                        .enumerate()
                        .map(|(idx, pk_col)| format!(
                            "{} = ${}",
                            sql_identifier(pk_col),
                            table.columns.len() + idx + 1
                        ))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                );
                match self.execute_with_params(&sql, &params) {
                    Ok(result) if result.affected_rows() == 0 => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: "missing_target".to_string(),
                            message: "update affected no rows".to_string(),
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Ok(_) => Ok(SyncImportRecordOutcome::Applied),
                    Err(DbError::Constraint { message }) if remote_wins => {
                        apply_remote_replace(SyncOperation::Update)?;
                        Ok(SyncImportRecordOutcome::Resolved(SyncConflictRecordData {
                            conflict_type: "update_update".to_string(),
                            message,
                            local_row_json,
                            resolution: Some("remote_applied".to_string()),
                            resolved_at_micros: Some(current_time_micros()),
                            resolved_by: Some("sync_policy".to_string()),
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(DbError::Constraint { message }) => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: if local_row_json.is_some() {
                                "update_update".to_string()
                            } else {
                                "constraint_error".to_string()
                            },
                            message,
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(error) => Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                        conflict_type: "apply_error".to_string(),
                        message: error.to_string(),
                        local_row_json,
                        resolution: None,
                        resolved_at_micros: None,
                        resolved_by: None,
                        resolution_note: None,
                        policy_name: Some(policy.as_str().to_string()),
                    })),
                }
            }
            SyncOperation::Delete => {
                let mut where_values = Vec::with_capacity(table.primary_key_columns.len());
                let mut where_parts = Vec::with_capacity(table.primary_key_columns.len());
                for pk_col in &table.primary_key_columns {
                    let column = table
                        .columns
                        .iter()
                        .find(|column| column.name == *pk_col)
                        .ok_or_else(|| {
                            DbError::sql(format!(
                                "table '{}' missing primary key column '{}'",
                                table.name, pk_col
                            ))
                        })?;
                    let json_value = primary_key.get(pk_col).ok_or_else(|| {
                        DbError::sql(format!(
                            "missing primary key column '{pk_col}' in record for table '{}'",
                            table.name
                        ))
                    })?;
                    where_values.push(json_to_column_value(&table.name, column, json_value)?);
                    where_parts.push(format!(
                        "{} = ${}",
                        sql_identifier(pk_col),
                        where_values.len()
                    ));
                }
                let sql = format!(
                    "DELETE FROM {} WHERE {}",
                    sql_identifier(&table.name),
                    where_parts.join(" AND ")
                );
                match self.execute_with_params(&sql, &where_values) {
                    Ok(result) if result.affected_rows() == 0 => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: "missing_target".to_string(),
                            message: "delete affected no rows".to_string(),
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Ok(_) => Ok(SyncImportRecordOutcome::Applied),
                    Err(DbError::Constraint { message }) if remote_wins => {
                        apply_remote_replace(SyncOperation::Delete)?;
                        Ok(SyncImportRecordOutcome::Resolved(SyncConflictRecordData {
                            conflict_type: "delete_update".to_string(),
                            message,
                            local_row_json,
                            resolution: Some("remote_applied".to_string()),
                            resolved_at_micros: Some(current_time_micros()),
                            resolved_by: Some("sync_policy".to_string()),
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(DbError::Constraint { message }) => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: if local_row_json.is_some() {
                                "delete_update".to_string()
                            } else {
                                "constraint_error".to_string()
                            },
                            message,
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(error) => Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                        conflict_type: "apply_error".to_string(),
                        message: error.to_string(),
                        local_row_json,
                        resolution: None,
                        resolved_at_micros: None,
                        resolved_by: None,
                        resolution_note: None,
                        policy_name: Some(policy.as_str().to_string()),
                    })),
                }
            }
        }
    }

    fn next_sync_conflict_id(&self) -> Result<i64> {
        let sql = format!(
            "SELECT COALESCE(MAX(conflict_id), 0) FROM {}",
            crate::sync::CONFLICTS_TABLE
        );
        let result = self.execute(&sql)?;
        let current = result
            .rows()
            .first()
            .and_then(|row| row.values().first())
            .and_then(|value| match value {
                Value::Int64(value) => Some(*value),
                Value::Bool(value) => Some(i64::from(*value)),
                _ => None,
            })
            .ok_or_else(|| DbError::corruption("malformed sync conflict id counter"))?;
        current
            .checked_add(1)
            .ok_or_else(|| DbError::internal("sync conflict_id counter overflow"))
    }

    fn sync_update_conflict_resolution(
        &self,
        conflict_id: i64,
        resolution: Option<&str>,
        resolved_by: Option<&str>,
        note: Option<&str>,
        resolved_at_micros: Option<i64>,
    ) -> Result<bool> {
        self.ensure_sync_tables()?;
        let Some(_) = self.sync_conflict(conflict_id)? else {
            return Ok(false);
        };
        let sql = format!(
            "UPDATE {table} SET resolved = {resolved}, resolution = {resolution}, resolved_at_micros = {resolved_at_micros}, resolved_by = {resolved_by}, resolution_note = {resolution_note} WHERE conflict_id = {conflict_id}",
            table = crate::sync::CONFLICTS_TABLE,
            resolved = if resolution.is_some() { 1 } else { 0 },
            resolution = resolution
                .map(sql_text_literal)
                .unwrap_or_else(|| "NULL".to_string()),
            resolved_at_micros = resolved_at_micros
                .map(|value| value.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            resolved_by = resolved_by
                .map(sql_text_literal)
                .unwrap_or_else(|| "NULL".to_string()),
            resolution_note = note
                .map(sql_text_literal)
                .unwrap_or_else(|| "NULL".to_string()),
            conflict_id = conflict_id,
        );
        let _ = self.execute(&sql)?;
        Ok(true)
    }

    fn next_sync_session_id(&self) -> Result<i64> {
        let sql = format!(
            "SELECT COALESCE(MAX(session_id), 0) FROM {}",
            crate::sync::SESSIONS_TABLE
        );
        let result = self.execute(&sql)?;
        let current = result
            .rows()
            .first()
            .and_then(|row| row.values().first())
            .and_then(|value| match value {
                Value::Int64(value) => Some(*value),
                Value::Bool(value) => Some(i64::from(*value)),
                _ => None,
            })
            .ok_or_else(|| DbError::corruption("malformed sync session id counter"))?;
        current
            .checked_add(1)
            .ok_or_else(|| DbError::internal("sync session_id counter overflow"))
    }

    fn sync_update_session(
        &self,
        session_id: i64,
        summary: &SyncRunSummary,
        status: &str,
        error: Option<&str>,
        ended_at_micros: i64,
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let (
            pushed_seen,
            pushed_applied,
            pushed_skipped,
            pushed_conflicted,
            pulled_seen,
            pulled_applied,
            pulled_skipped,
            pulled_conflicted,
        ) = sync_session_summary_counts(summary);
        let sql = format!(
            "UPDATE {table} SET remote_replica_id = {remote_replica_id}, ended_at_micros = {ended_at_micros}, status = {status}, error = {error}, pushed_batch_id = {pushed_batch_id}, pulled_batch_id = {pulled_batch_id}, pushed_seen = {pushed_seen}, pushed_applied = {pushed_applied}, pushed_skipped = {pushed_skipped}, pushed_conflicted = {pushed_conflicted}, pulled_seen = {pulled_seen}, pulled_applied = {pulled_applied}, pulled_skipped = {pulled_skipped}, pulled_conflicted = {pulled_conflicted}, retry_count = {retry_count} WHERE session_id = {session_id}",
            table = crate::sync::SESSIONS_TABLE,
            remote_replica_id = sql_nullable_text_literal(summary.remote_replica_id.as_deref()),
            ended_at_micros = ended_at_micros,
            status = sql_text_literal(status),
            error = sql_nullable_text_literal(error),
            pushed_batch_id = sql_nullable_text_literal(summary.pushed_batch_id.as_deref()),
            pulled_batch_id = sql_nullable_text_literal(summary.pulled_batch_id.as_deref()),
            pushed_seen = pushed_seen,
            pushed_applied = pushed_applied,
            pushed_skipped = pushed_skipped,
            pushed_conflicted = pushed_conflicted,
            pulled_seen = pulled_seen,
            pulled_applied = pulled_applied,
            pulled_skipped = pulled_skipped,
            pulled_conflicted = pulled_conflicted,
            retry_count = summary.retry_count as i64,
            session_id = session_id,
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }

    fn ensure_sync_tables(&self) -> Result<()> {
        let _ = self.execute(crate::sync::METADATA_TABLE_DDL)?;
        let _ = self.execute(crate::sync::PEERS_TABLE_DDL)?;
        let _ = self.execute(crate::sync::SESSIONS_TABLE_DDL)?;
        let _ = self.execute(crate::sync::SCOPES_TABLE_DDL)?;
        let _ = self.execute(crate::sync::PEER_SCOPES_TABLE_DDL)?;
        let _ = self.execute(crate::sync::CONFLICTS_TABLE_DDL)?;
        let _ = self.execute(crate::sync::CHANGESET_HISTORY_TABLE_DDL)?;
        let _ = self.execute(crate::sync::RELAY_SESSIONS_TABLE_DDL)?;
        let _ = self.execute(crate::sync::SHAPES_TABLE_DDL)?;
        let _ = self.execute(crate::sync::SHAPE_CLIENTS_TABLE_DDL)?;
        self.ensure_sync_conflict_columns()?;
        Ok(())
    }

    fn ensure_sync_conflict_columns(&self) -> Result<()> {
        let existing = self.sync_table_columns(crate::sync::CONFLICTS_TABLE)?;
        for (name, ty) in [
            ("resolution", "TEXT"),
            ("resolved_at_micros", "INT64"),
            ("resolved_by", "TEXT"),
            ("resolution_note", "TEXT"),
            ("policy_name", "TEXT"),
            ("local_record_json", "TEXT"),
        ] {
            if !existing.iter().any(|column| column == name) {
                let sql = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    sql_identifier(crate::sync::CONFLICTS_TABLE),
                    sql_identifier(name),
                    ty
                );
                let _ = self.execute(&sql)?;
            }
        }
        Ok(())
    }

    fn sync_table_columns(&self, table_name: &str) -> Result<Vec<String>> {
        let sql = format!("PRAGMA table_info({})", sql_identifier(table_name));
        match self.execute(&sql) {
            Ok(result) => Ok(result
                .rows()
                .iter()
                .filter_map(|row| match row.values().get(1) {
                    Some(Value::Text(value)) => Some(value.clone()),
                    _ => None,
                })
                .collect()),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }

    fn load_sync_status_from_db(&self) -> Result<SyncStatus> {
        let enabled = self
            .sync_read_metadata("enabled")?
            .map(|v| v == "true")
            .unwrap_or(false);
        let replica_id = self.sync_read_metadata("replica_id")?;
        let stored_next_sequence: u64 = self
            .sync_read_metadata("next_sequence")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        let next_sequence = self.effective_sync_next_sequence(stored_next_sequence)?;
        let journal_path = self
            .inner
            .sync_ctx
            .journal_path()
            .to_string_lossy()
            .to_string();
        let journal_size_bytes = self.inner.sync_ctx.journal_size_bytes();
        Ok(SyncStatus {
            enabled,
            replica_id,
            next_sequence,
            journal_path: Some(journal_path),
            journal_size_bytes,
        })
    }

    fn load_sync_status_from_runtime(&self, runtime: &mut EngineRuntime) -> Result<SyncStatus> {
        let mut filter = BTreeSet::new();
        filter.insert(crate::sync::METADATA_TABLE.to_string());
        runtime.load_deferred_table_row_sources_filtered(
            &self.inner.pager,
            &self.inner.wal,
            self.inner.config.page_size,
            &filter,
        )?;
        let enabled = sync_read_metadata_from_runtime(runtime, "enabled")?
            .map(|v| v == "true")
            .unwrap_or(false);
        let replica_id = sync_read_metadata_from_runtime(runtime, "replica_id")?;
        let stored_next_sequence: u64 = sync_read_metadata_from_runtime(runtime, "next_sequence")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        let next_sequence = self.effective_sync_next_sequence(stored_next_sequence)?;
        let journal_path = self
            .inner
            .sync_ctx
            .journal_path()
            .to_string_lossy()
            .to_string();
        let journal_size_bytes = self.inner.sync_ctx.journal_size_bytes();
        Ok(SyncStatus {
            enabled,
            replica_id,
            next_sequence,
            journal_path: Some(journal_path),
            journal_size_bytes,
        })
    }

    fn effective_sync_next_sequence(&self, stored_next_sequence: u64) -> Result<u64> {
        let report = crate::sync::inspect_journal_integrity(
            self.inner.sync_ctx.journal_path(),
            &self.inner.vfs,
            None,
        )?;
        let journal_next_sequence = report
            .last_sequence
            .and_then(|sequence| sequence.checked_add(1))
            .unwrap_or(1);
        Ok(stored_next_sequence.max(journal_next_sequence))
    }

    fn sessions_query_result(&self) -> Result<QueryResult> {
        let columns = vec![
            "session_id".to_string(),
            "connection_id".to_string(),
            "database_id_hash".to_string(),
            "opened_at_unix_ms".to_string(),
            "closed_at_unix_ms".to_string(),
            "state".to_string(),
            "binding".to_string(),
            "tracing_enabled".to_string(),
            "slow_query_threshold_us".to_string(),
            "internal".to_string(),
        ];
        let sessions = self.inner.tracing.sessions_snapshot();
        let rows = sessions
            .into_iter()
            .map(|s| QueryRow::new(s.to_query_row()))
            .collect();
        Ok(QueryResult::with_rows(columns, rows))
    }

    fn slow_queries_query_result(&self) -> Result<QueryResult> {
        let columns = vec![
            "event_id".to_string(),
            "session_id".to_string(),
            "connection_id".to_string(),
            "started_at_unix_ms".to_string(),
            "duration_us".to_string(),
            "threshold_us".to_string(),
            "statement_kind".to_string(),
            "read_only".to_string(),
            "sql_fingerprint".to_string(),
            "sql_template".to_string(),
            "sql_text_mode".to_string(),
            "database_id_hash".to_string(),
            "status".to_string(),
            "error_code".to_string(),
            "internal".to_string(),
            "truncated".to_string(),
        ];
        let snapshot = self.inner.tracing.slow_queries_snapshot();
        let rows = snapshot
            .items
            .into_iter()
            .map(|e| QueryRow::new(e.to_query_row()))
            .collect();
        Ok(QueryResult::with_rows(columns, rows))
    }

    fn lock_waits_query_result(&self) -> Result<QueryResult> {
        let columns = vec![
            "event_id".to_string(),
            "session_id".to_string(),
            "connection_id".to_string(),
            "duration_us".to_string(),
            "threshold_us".to_string(),
            "wait_source".to_string(),
            "status".to_string(),
            "database_id_hash".to_string(),
            "internal".to_string(),
        ];
        let snapshot = self.inner.tracing.lock_waits_snapshot();
        let rows = snapshot
            .items
            .into_iter()
            .map(|e| QueryRow::new(e.to_query_row()))
            .collect();
        Ok(QueryResult::with_rows(columns, rows))
    }

    fn index_usage_query_result(&self) -> Result<QueryResult> {
        let columns = vec![
            "table_name".to_string(),
            "index_name".to_string(),
            "index_kind".to_string(),
            "read_count".to_string(),
            "write_count".to_string(),
        ];
        let rows = self
            .inner
            .tracing
            .index_usage_snapshot()
            .into_iter()
            .map(|r| QueryRow::new(r.to_query_row()))
            .collect();
        Ok(QueryResult::with_rows(columns, rows))
    }

    fn doctor_findings_query_result(&self) -> Result<QueryResult> {
        let columns = vec![
            "id".to_string(),
            "category".to_string(),
            "severity".to_string(),
            "title".to_string(),
            "message".to_string(),
            "evidence".to_string(),
            "recommendation".to_string(),
        ];
        let mut findings = Vec::new();
        // Static Doctor findings via sync_operational_doctor_report
        if let Ok(report) = self.sync_operational_doctor_report() {
            for issue in report.issues {
                findings.push(vec![
                    Value::Text(format!("sync-{}", issue.line_number)),
                    Value::Text(issue.code.clone()),
                    Value::Text(format!("{:?}", issue.severity)),
                    Value::Text(issue.message.clone()),
                    Value::Text(issue.message),
                    Value::Text(String::new()),
                    Value::Text(report.guidance.first().cloned().unwrap_or_default()),
                ]);
            }
        }
        // Runtime advisor findings
        let slow_queries = self.inner.tracing.slow_queries_snapshot();
        let lock_waits = self.inner.tracing.lock_waits_snapshot();
        let index_usage = self.inner.tracing.index_usage_snapshot();
        let wal_size_mb = self
            .inner
            .wal
            .latest_snapshot()
            .saturating_sub(crate::wal::format::WAL_HEADER_SIZE)
            / (1024 * 1024);
        let uncheckpointed_frames = self
            .inner
            .wal
            .latest_snapshot()
            .saturating_sub(self.inner.wal.checkpoint_epoch())
            / self.inner.config.page_size as u64;
        let mut engine = crate::tracing::advisor::AdvisorEngine::new();
        engine.analyze(
            &slow_queries,
            &lock_waits,
            &index_usage,
            wal_size_mb,
            uncheckpointed_frames,
        );
        for f in engine.into_findings() {
            findings.push(vec![
                Value::Text(f.advisor_id),
                Value::Text(format!("{:?}", f.category)),
                Value::Text(format!("{:?}", f.severity)),
                Value::Text(f.title),
                Value::Text(f.description),
                Value::Text(f.evidence.join("; ")),
                Value::Text(f.recommendation),
            ]);
        }
        self.append_plan_cache_doctor_findings(&mut findings)?;
        let rows = findings.into_iter().map(QueryRow::new).collect();
        Ok(QueryResult::with_rows(columns, rows))
    }

    fn append_plan_cache_doctor_findings(&self, findings: &mut Vec<Vec<Value>>) -> Result<()> {
        let summary = self.plan_cache_summary()?;
        let config = &self.inner.config.plan_cache;
        if !config.enabled {
            findings.push(plan_cache_doctor_row(
                "plan-cache.disabled",
                "Statistics",
                "Info",
                "Plan cache is disabled",
                "This connection will parse and plan each statement without using the connection-local plan cache.",
                "enabled=false",
                "Enable plan_cache_enabled=true for repeated prepared-statement workloads.",
            ));
            return Ok(());
        }

        if summary.total_oversized_refusals > 0 {
            findings.push(plan_cache_doctor_row(
                "plan-cache.oversized-refusals",
                "Storage",
                "Warning",
                "Plan cache refused oversized entries",
                "One or more statements were larger than the configured plan-cache budget and could not be cached.",
                &format!(
                    "oversized_refusals={}; max_size_bytes={}",
                    summary.total_oversized_refusals, summary.max_size_bytes
                ),
                "Increase plan_cache_max_bytes or leave unusually large one-off statements uncached.",
            ));
        }

        if summary.total_evictions > 0 {
            findings.push(plan_cache_doctor_row(
                "plan-cache.evictions",
                "Storage",
                "Warning",
                "Plan cache is evicting entries",
                "The connection-local plan cache has evicted entries, which can reduce reuse for repeated workloads.",
                &format!(
                    "entries={}; evictions={}; size_bytes={}; max_size_bytes={}; hit_rate={:.2}%",
                    summary.total_entries,
                    summary.total_evictions,
                    summary.total_size_bytes,
                    summary.max_size_bytes,
                    summary.hit_rate
                ),
                "Increase plan_cache_max_bytes for large prepared-statement working sets, or inspect sys.plan_cache for churn.",
            ));
        }

        let lookups = summary.total_hits.saturating_add(summary.total_misses);
        if lookups >= 100 && summary.hit_rate < 10.0 {
            findings.push(plan_cache_doctor_row(
                "plan-cache.low-hit-rate",
                "Statistics",
                "Info",
                "Plan cache hit rate is low",
                "This connection has performed many plan-cache lookups with few hits, which usually means the workload is mostly one-shot SQL.",
                &format!(
                    "hits={}; misses={}; hit_rate={:.2}%",
                    summary.total_hits, summary.total_misses, summary.hit_rate
                ),
                "For one-shot workloads, either leave the cache at the small default or disable it with plan_cache_enabled=false.",
            ));
        }

        Ok(())
    }

    fn fix_plan_query_result(&self) -> Result<QueryResult> {
        // Note: This duplicates the advisor analysis from doctor_findings_query_result.
        // Future optimization: cache advisor findings with a TTL to avoid redundant analysis.
        let columns = vec![
            "advisor_id".to_string(),
            "action".to_string(),
            "target".to_string(),
            "auto_safe".to_string(),
        ];
        let slow_queries = self.inner.tracing.slow_queries_snapshot();
        let lock_waits = self.inner.tracing.lock_waits_snapshot();
        let index_usage = self.inner.tracing.index_usage_snapshot();
        let wal_size_mb = self
            .inner
            .wal
            .latest_snapshot()
            .saturating_sub(crate::wal::format::WAL_HEADER_SIZE)
            / (1024 * 1024);
        let uncheckpointed_frames = self
            .inner
            .wal
            .latest_snapshot()
            .saturating_sub(self.inner.wal.checkpoint_epoch())
            / self.inner.config.page_size as u64;
        let mut engine = crate::tracing::advisor::AdvisorEngine::new();
        engine.analyze(
            &slow_queries,
            &lock_waits,
            &index_usage,
            wal_size_mb,
            uncheckpointed_frames,
        );
        let rows: Vec<QueryRow> = engine
            .into_findings()
            .into_iter()
            .filter_map(|f| {
                f.fix_plan.map(|p| {
                    QueryRow::new(vec![
                        Value::Text(f.advisor_id),
                        Value::Text(p.action),
                        Value::Text(p.target),
                        Value::Text(p.auto_safe),
                    ])
                })
            })
            .collect();
        Ok(QueryResult::with_rows(columns, rows))
    }

    fn try_execute_sync_inspection_query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let normalized = normalize_sync_inspection_sql(sql);
        let Some(query) = SyncInspectionQuery::parse(&normalized) else {
            return Ok(None);
        };
        if !params.is_empty() {
            return Err(DbError::sql(
                "system inspection views do not accept parameters",
            ));
        }
        match query {
            SyncInspectionQuery::Status => self.sync_status_query_result().map(Some),
            SyncInspectionQuery::Journal { since_sequence } => {
                self.sync_journal_query_result(since_sequence).map(Some)
            }
            SyncInspectionQuery::WalMetrics => self.wal_metrics_query_result().map(Some),
            SyncInspectionQuery::ProcessCoordination => {
                self.process_coordination_query_result().map(Some)
            }
            SyncInspectionQuery::ProcessReaders => self.process_readers_query_result().map(Some),
            SyncInspectionQuery::ProcessLockMetrics => {
                self.process_lock_metrics_query_result().map(Some)
            }
            SyncInspectionQuery::WriteQueueMetrics => {
                self.write_queue_metrics_query_result().map(Some)
            }
            SyncInspectionQuery::StorageMetrics => self.storage_metrics_query_result().map(Some),
            SyncInspectionQuery::ReactiveMetrics => self.reactive_metrics_query_result().map(Some),
            SyncInspectionQuery::ReactiveSubscriptions => {
                self.reactive_subscriptions_query_result().map(Some)
            }
            SyncInspectionQuery::Peers => self.sync_peers_query_result().map(Some),
            SyncInspectionQuery::Retention => self.sync_retention_query_result().map(Some),
            SyncInspectionQuery::PeerLag => self.sync_peer_lag_query_result().map(Some),
            SyncInspectionQuery::Doctor => self.sync_doctor_query_result().map(Some),
            SyncInspectionQuery::Scopes => self.sync_scopes_query_result().map(Some),
            SyncInspectionQuery::ScopeTables => self.sync_scope_tables_query_result().map(Some),
            SyncInspectionQuery::PeerScopes => self.sync_peer_scopes_query_result().map(Some),
            SyncInspectionQuery::Sessions => self.sync_sessions_query_result().map(Some),
            SyncInspectionQuery::RuntimeSessions => self.sessions_query_result().map(Some),
            SyncInspectionQuery::SlowQueries => self.slow_queries_query_result().map(Some),
            SyncInspectionQuery::LockWaits => self.lock_waits_query_result().map(Some),
            SyncInspectionQuery::IndexUsage => self.index_usage_query_result().map(Some),
            SyncInspectionQuery::DoctorFindings => self.doctor_findings_query_result().map(Some),
            SyncInspectionQuery::FixPlan => self.fix_plan_query_result().map(Some),
            SyncInspectionQuery::ConflictPolicy => {
                self.sync_conflict_policy_query_result().map(Some)
            }
            SyncInspectionQuery::Conflicts => self.sync_conflicts_query_result().map(Some),
            SyncInspectionQuery::RelayStatus => self.sync_relay_status_query_result().map(Some),
            SyncInspectionQuery::RelaySessions => self.sync_relay_sessions_query_result().map(Some),
            SyncInspectionQuery::Shapes => self.sync_shapes_query_result().map(Some),
            SyncInspectionQuery::ShapeClients => self.sync_shape_clients_query_result().map(Some),
            SyncInspectionQuery::ChangesetHistory => {
                self.sync_changeset_history_query_result().map(Some)
            }
            SyncInspectionQuery::PlanCache => self.plan_cache_query_result().map(Some),
            SyncInspectionQuery::PlanCacheSummary => {
                self.plan_cache_summary_query_result().map(Some)
            }
        }
    }

    fn sync_status_query_result(&self) -> Result<QueryResult> {
        let status = self.sync_status()?;
        Ok(QueryResult::with_rows(
            vec![
                "enabled".to_string(),
                "replica_id".to_string(),
                "next_sequence".to_string(),
                "journal_path".to_string(),
                "journal_size_bytes".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Bool(status.enabled),
                status.replica_id.map_or(Value::Null, Value::Text),
                sync_u64_to_i64(status.next_sequence, "next_sequence")?,
                status.journal_path.map_or(Value::Null, Value::Text),
                sync_u64_to_i64(status.journal_size_bytes, "journal_size_bytes")?,
            ])],
        ))
    }

    fn wal_metrics_query_result(&self) -> Result<QueryResult> {
        let latest_lsn = self.inner.wal.latest_snapshot();
        let file_size = self.inner.wal.file_size()?;
        let active_readers = self.inner.wal.active_reader_count()?;
        let max_page_count = self.inner.wal.max_page_count();
        let checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let warning_count = self.inner.wal.warnings()?.len();
        let version_count = self.inner.wal.version_count()?;
        let (resident_versions, on_disk_versions) = self.inner.wal.version_counts_by_payload()?;
        Ok(QueryResult::with_rows(
            vec![
                "latest_lsn".to_string(),
                "file_size_bytes".to_string(),
                "active_readers".to_string(),
                "max_page_count".to_string(),
                "checkpoint_epoch".to_string(),
                "warning_count".to_string(),
                "version_count".to_string(),
                "resident_versions".to_string(),
                "on_disk_versions".to_string(),
                "shared_wal".to_string(),
            ],
            vec![QueryRow::new(vec![
                sync_u64_to_i64(latest_lsn, "latest_lsn")?,
                sync_u64_to_i64(file_size, "file_size_bytes")?,
                sync_usize_to_i64(active_readers, "active_readers")?,
                sync_u64_to_i64(max_page_count as u64, "max_page_count")?,
                sync_u64_to_i64(checkpoint_epoch, "checkpoint_epoch")?,
                sync_u64_to_i64(warning_count as u64, "warning_count")?,
                sync_u64_to_i64(version_count as u64, "version_count")?,
                sync_u64_to_i64(resident_versions as u64, "resident_versions")?,
                sync_u64_to_i64(on_disk_versions as u64, "on_disk_versions")?,
                Value::Bool(self.inner.wal.is_shared()),
            ])],
        ))
    }

    fn process_coordination_query_result(&self) -> Result<QueryResult> {
        let columns = vec![
            "mode".to_string(),
            "enabled".to_string(),
            "supported".to_string(),
            "coord_path".to_string(),
            "coord_version".to_string(),
            "coordinator_generation".to_string(),
            "wal_end_lsn".to_string(),
            "checkpoint_generation".to_string(),
            "last_refresh_lsn".to_string(),
            "last_refresh_age_ms".to_string(),
        ];
        let row = if let Some(snapshot) = self.inner.wal.process_coordination_snapshot()? {
            QueryRow::new(vec![
                Value::Text(snapshot.mode.as_str().to_string()),
                Value::Bool(snapshot.enabled),
                Value::Bool(snapshot.supported),
                snapshot
                    .coord_path
                    .map(|path| Value::Text(path.to_string_lossy().to_string()))
                    .unwrap_or(Value::Null),
                sync_u64_to_i64(u64::from(snapshot.coord_version), "coord_version")?,
                sync_u64_to_i64(snapshot.coordinator_generation, "coordinator_generation")?,
                sync_u64_to_i64(snapshot.wal_end_lsn, "wal_end_lsn")?,
                sync_u64_to_i64(snapshot.checkpoint_generation, "checkpoint_generation")?,
                sync_u64_to_i64(self.inner.wal.latest_snapshot(), "last_refresh_lsn")?,
                snapshot
                    .last_refresh_age_ms
                    .map(|value| sync_u64_to_i64(value, "last_refresh_age_ms"))
                    .transpose()?
                    .unwrap_or(Value::Null),
            ])
        } else {
            QueryRow::new(vec![
                Value::Text(self.inner.config.process_coordination.as_str().to_string()),
                Value::Bool(false),
                Value::Bool(
                    self.inner.vfs.supports_file_locks()
                        || self.inner.config.process_coordination
                            == ProcessCoordinationMode::SingleProcessUnsafe,
                ),
                Value::Null,
                Value::Int64(0),
                Value::Int64(0),
                sync_u64_to_i64(self.inner.wal.latest_snapshot(), "wal_end_lsn")?,
                sync_u64_to_i64(self.inner.wal.checkpoint_epoch(), "checkpoint_generation")?,
                sync_u64_to_i64(self.inner.wal.latest_snapshot(), "last_refresh_lsn")?,
                Value::Null,
            ])
        };
        Ok(QueryResult::with_rows(columns, vec![row]))
    }

    fn process_readers_query_result(&self) -> Result<QueryResult> {
        let columns = vec![
            "slot_id".to_string(),
            "pid".to_string(),
            "connection_id".to_string(),
            "snapshot_lsn".to_string(),
            "age_ms".to_string(),
            "heartbeat_age_ms".to_string(),
            "state".to_string(),
            "retention_blocking".to_string(),
        ];
        let Some(readers) = self.inner.wal.process_reader_slot_snapshots()? else {
            return Ok(QueryResult::with_rows(columns, Vec::new()));
        };
        let mut rows = Vec::with_capacity(readers.len());
        for reader in readers {
            rows.push(QueryRow::new(vec![
                sync_u64_to_i64(u64::from(reader.slot_id), "slot_id")?,
                sync_u64_to_i64(reader.pid, "pid")?,
                Value::Text(reader.connection_id),
                sync_u64_to_i64(reader.snapshot_lsn, "snapshot_lsn")?,
                sync_u64_to_i64(reader.age_ms, "age_ms")?,
                sync_u64_to_i64(reader.heartbeat_age_ms, "heartbeat_age_ms")?,
                Value::Text(reader.state),
                Value::Bool(reader.retention_blocking),
            ]));
        }
        Ok(QueryResult::with_rows(columns, rows))
    }

    fn process_lock_metrics_query_result(&self) -> Result<QueryResult> {
        let columns = vec![
            "writer_lock_waits".to_string(),
            "writer_lock_timeouts".to_string(),
            "current_writer_pid".to_string(),
            "current_writer_lock_age_ms".to_string(),
            "current_checkpoint_pid".to_string(),
            "current_checkpoint_lock_age_ms".to_string(),
            "checkpoint_lock_waits".to_string(),
            "checkpoint_lock_timeouts".to_string(),
            "reader_slots_allocated".to_string(),
            "stale_slots_cleaned".to_string(),
            "wal_refreshes".to_string(),
            "wal_refresh_failures".to_string(),
        ];
        let row = if let Some(metrics) = self.inner.wal.process_lock_metrics_snapshot()? {
            QueryRow::new(vec![
                sync_u64_to_i64(metrics.writer_lock_waits, "writer_lock_waits")?,
                sync_u64_to_i64(metrics.writer_lock_timeouts, "writer_lock_timeouts")?,
                metrics
                    .current_writer_pid
                    .map(|value| sync_u64_to_i64(value, "current_writer_pid"))
                    .transpose()?
                    .unwrap_or(Value::Null),
                metrics
                    .current_writer_lock_age_ms
                    .map(|value| sync_u64_to_i64(value, "current_writer_lock_age_ms"))
                    .transpose()?
                    .unwrap_or(Value::Null),
                metrics
                    .current_checkpoint_pid
                    .map(|value| sync_u64_to_i64(value, "current_checkpoint_pid"))
                    .transpose()?
                    .unwrap_or(Value::Null),
                metrics
                    .current_checkpoint_lock_age_ms
                    .map(|value| sync_u64_to_i64(value, "current_checkpoint_lock_age_ms"))
                    .transpose()?
                    .unwrap_or(Value::Null),
                sync_u64_to_i64(metrics.checkpoint_lock_waits, "checkpoint_lock_waits")?,
                sync_u64_to_i64(metrics.checkpoint_lock_timeouts, "checkpoint_lock_timeouts")?,
                sync_u64_to_i64(metrics.reader_slot_allocations, "reader_slots_allocated")?,
                sync_u64_to_i64(metrics.reader_slot_reclaims, "stale_slots_cleaned")?,
                sync_u64_to_i64(metrics.wal_refreshes, "wal_refreshes")?,
                sync_u64_to_i64(metrics.wal_refresh_failures, "wal_refresh_failures")?,
            ])
        } else {
            QueryRow::new(vec![
                Value::Int64(0),
                Value::Int64(0),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
            ])
        };
        Ok(QueryResult::with_rows(columns, vec![row]))
    }

    fn write_queue_metrics_query_result(&self) -> Result<QueryResult> {
        let metrics = self.write_queue_metrics();
        Ok(QueryResult::with_rows(
            vec![
                "capacity".to_string(),
                "current_depth".to_string(),
                "admitted".to_string(),
                "rejected".to_string(),
                "timed_out".to_string(),
                "canceled".to_string(),
                "executed".to_string(),
                "committed".to_string(),
                "failed".to_string(),
                "group_commit_batches".to_string(),
                "group_commit_syncs".to_string(),
                "group_commit_max_batch".to_string(),
                "group_commit_commits_covered".to_string(),
                "physical_syncs_saved".to_string(),
                "total_queue_wait_ns".to_string(),
            ],
            vec![QueryRow::new(vec![
                sync_usize_to_i64(metrics.capacity, "capacity")?,
                sync_usize_to_i64(metrics.current_depth, "current_depth")?,
                sync_u64_to_i64(metrics.admitted, "admitted")?,
                sync_u64_to_i64(metrics.rejected, "rejected")?,
                sync_u64_to_i64(metrics.timed_out, "timed_out")?,
                sync_u64_to_i64(metrics.canceled, "canceled")?,
                sync_u64_to_i64(metrics.executed, "executed")?,
                sync_u64_to_i64(metrics.committed, "committed")?,
                sync_u64_to_i64(metrics.failed, "failed")?,
                sync_u64_to_i64(metrics.group_commit_batches, "group_commit_batches")?,
                sync_u64_to_i64(metrics.group_commit_syncs, "group_commit_syncs")?,
                sync_u64_to_i64(metrics.group_commit_max_batch, "group_commit_max_batch")?,
                sync_u64_to_i64(
                    metrics.group_commit_commits_covered,
                    "group_commit_commits_covered",
                )?,
                sync_u64_to_i64(metrics.physical_syncs_saved, "physical_syncs_saved")?,
                sync_u64_to_i64(metrics.total_queue_wait_ns, "total_queue_wait_ns")?,
            ])],
        ))
    }

    fn storage_metrics_query_result(&self) -> Result<QueryResult> {
        let storage = self.storage_info()?;
        Ok(QueryResult::with_rows(
            vec![
                "path".to_string(),
                "wal_path".to_string(),
                "format_version".to_string(),
                "page_size".to_string(),
                "cache_size_mb".to_string(),
                "page_count".to_string(),
                "schema_cookie".to_string(),
                "wal_end_lsn".to_string(),
                "wal_file_size".to_string(),
                "last_checkpoint_lsn".to_string(),
                "active_readers".to_string(),
                "wal_versions".to_string(),
                "warning_count".to_string(),
                "shared_wal".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Text(storage.path.to_string_lossy().to_string()),
                Value::Text(storage.wal_path.to_string_lossy().to_string()),
                sync_u64_to_i64(storage.format_version as u64, "format_version")?,
                sync_u64_to_i64(storage.page_size as u64, "page_size")?,
                sync_usize_to_i64(storage.cache_size_mb, "cache_size_mb")?,
                sync_u64_to_i64(storage.page_count as u64, "page_count")?,
                sync_u64_to_i64(storage.schema_cookie as u64, "schema_cookie")?,
                sync_u64_to_i64(storage.wal_end_lsn, "wal_end_lsn")?,
                sync_u64_to_i64(storage.wal_file_size, "wal_file_size")?,
                sync_u64_to_i64(storage.last_checkpoint_lsn, "last_checkpoint_lsn")?,
                sync_usize_to_i64(storage.active_readers, "active_readers")?,
                sync_u64_to_i64(storage.wal_versions as u64, "wal_versions")?,
                sync_u64_to_i64(storage.warning_count as u64, "warning_count")?,
                Value::Bool(storage.shared_wal),
            ])],
        ))
    }

    fn plan_cache_query_result(&self) -> Result<QueryResult> {
        let entries = self.plan_cache_entries()?;
        let prepared_entries = self.prepared_plan_cache_entries()?;
        let columns = vec![
            "scope".to_string(),
            "cache_key_hash".to_string(),
            "persistent_schema_cookie".to_string(),
            "temp_schema_cookie".to_string(),
            "policy_mask_generation".to_string(),
            "hit_count".to_string(),
            "last_used_at".to_string(),
            "plan_size_bytes".to_string(),
            "statement_category".to_string(),
        ];
        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            rows.push(QueryRow::new(vec![
                Value::Text("connection".to_string()),
                Value::Text(format!("{:016x}", entry.key_hash)),
                Value::Int64(i64::from(entry.persistent_schema_cookie)),
                Value::Int64(i64::from(entry.temp_schema_cookie)),
                Value::Int64(i64::from(entry.policy_mask_generation)),
                Value::Int64(entry.hit_count as i64),
                Value::Text(format!("{} micros", entry.last_used_at_micros)),
                Value::Int64(entry.plan_size_bytes as i64),
                Value::Text(entry.statement_category.as_str().to_string()),
            ]));
        }
        for entry in prepared_entries {
            rows.push(QueryRow::new(vec![
                Value::Text("connection".to_string()),
                Value::Text(format!("{:016x}", entry.key_hash)),
                Value::Int64(i64::from(entry.persistent_schema_cookie)),
                Value::Int64(i64::from(entry.temp_schema_cookie)),
                Value::Int64(i64::from(entry.policy_mask_generation)),
                Value::Int64(entry.hit_count as i64),
                Value::Text(format!("{} micros", entry.last_used_at_micros)),
                Value::Int64(entry.plan_size_bytes as i64),
                Value::Text(
                    crate::plan_cache::StatementCategory::classify(entry.bundle.statement.as_ref())
                        .as_str()
                        .to_string(),
                ),
            ]));
        }
        Ok(QueryResult::with_rows(columns, rows))
    }

    fn plan_cache_summary_query_result(&self) -> Result<QueryResult> {
        let summary = self.plan_cache_summary()?;
        Ok(QueryResult::with_rows(
            vec![
                "scope".to_string(),
                "total_entries".to_string(),
                "total_hits".to_string(),
                "total_misses".to_string(),
                "total_evictions".to_string(),
                "total_size_bytes".to_string(),
                "max_size_bytes".to_string(),
                "total_oversized_refusals".to_string(),
                "hit_rate".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Text(summary.scope.to_string()),
                Value::Int64(summary.total_entries as i64),
                Value::Int64(summary.total_hits as i64),
                Value::Int64(summary.total_misses as i64),
                Value::Int64(summary.total_evictions as i64),
                Value::Int64(summary.total_size_bytes as i64),
                Value::Int64(summary.max_size_bytes as i64),
                Value::Int64(summary.total_oversized_refusals as i64),
                Value::Float64(summary.hit_rate),
            ])],
        ))
    }

    fn reactive_metrics_query_result(&self) -> Result<QueryResult> {
        let metrics = self.reactive_metrics();
        Ok(QueryResult::with_rows(
            vec![
                "active_watch_count".to_string(),
                "table_watch_count".to_string(),
                "range_watch_count".to_string(),
                "query_watch_count".to_string(),
                "change_stream_count".to_string(),
                "events_published".to_string(),
                "events_delivered".to_string(),
                "events_dropped".to_string(),
                "lagged_watch_count".to_string(),
                "row_change_events_truncated".to_string(),
            ],
            vec![QueryRow::new(vec![
                sync_usize_to_i64(metrics.active_watch_count, "active_watch_count")?,
                sync_usize_to_i64(metrics.table_watch_count, "table_watch_count")?,
                sync_usize_to_i64(metrics.range_watch_count, "range_watch_count")?,
                sync_usize_to_i64(metrics.query_watch_count, "query_watch_count")?,
                sync_usize_to_i64(metrics.change_stream_count, "change_stream_count")?,
                sync_u64_to_i64(metrics.events_published, "events_published")?,
                sync_u64_to_i64(metrics.events_delivered, "events_delivered")?,
                sync_u64_to_i64(metrics.events_dropped, "events_dropped")?,
                sync_usize_to_i64(metrics.lagged_watch_count, "lagged_watch_count")?,
                sync_u64_to_i64(
                    metrics.row_change_events_truncated,
                    "row_change_events_truncated",
                )?,
            ])],
        ))
    }

    fn reactive_subscriptions_query_result(&self) -> Result<QueryResult> {
        let rows = self
            .reactive_subscriptions()
            .into_iter()
            .map(|subscription| {
                Ok(QueryRow::new(vec![
                    sync_u64_to_i64(subscription.watch_id, "watch_id")?,
                    Value::Text(subscription.kind.as_str().to_string()),
                    Value::Int64(subscription.created_at_micros),
                    sync_usize_to_i64(subscription.queue_capacity, "queue_capacity")?,
                    sync_usize_to_i64(subscription.queue_depth, "queue_depth")?,
                    sync_u64_to_i64(
                        subscription.last_delivered_event_id,
                        "last_delivered_event_id",
                    )?,
                    sync_u64_to_i64(subscription.dropped_events, "dropped_events")?,
                    Value::Bool(subscription.lagged),
                    Value::Text(subscription.dependencies_json),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "watch_id".to_string(),
                "kind".to_string(),
                "created_at_micros".to_string(),
                "queue_capacity".to_string(),
                "queue_depth".to_string(),
                "last_delivered_event_id".to_string(),
                "dropped_events".to_string(),
                "lagged".to_string(),
                "dependencies_json".to_string(),
            ],
            rows,
        ))
    }

    fn sync_journal_query_result(&self, since_sequence: u64) -> Result<QueryResult> {
        let records = self.sync_pending_changes(since_sequence, usize::MAX)?;
        let rows = records
            .into_iter()
            .map(|record| {
                Ok(QueryRow::new(vec![
                    sync_u64_to_i64(record.sequence, "sequence")?,
                    Value::Text(record.replica_id),
                    sync_u64_to_i64(record.transaction_lsn, "transaction_lsn")?,
                    Value::Text(record.table),
                    Value::Text(record.operation),
                    Value::Text(record.primary_key.to_string()),
                    record
                        .after
                        .map_or(Value::Null, |value| Value::Text(value.to_string())),
                    Value::Int64(i64::from(record.schema_cookie)),
                    Value::Int64(record.committed_at_micros),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "sequence".to_string(),
                "replica_id".to_string(),
                "transaction_lsn".to_string(),
                "table_name".to_string(),
                "operation".to_string(),
                "primary_key_json".to_string(),
                "after_json".to_string(),
                "schema_cookie".to_string(),
                "committed_at_micros".to_string(),
            ],
            rows,
        ))
    }

    fn sync_peers_query_result(&self) -> Result<QueryResult> {
        let peers = self.sync_peers()?;
        let rows = peers
            .into_iter()
            .map(|peer| {
                Ok(QueryRow::new(vec![
                    Value::Text(peer.name),
                    Value::Text(peer.endpoint),
                    peer.token_env.map_or(Value::Null, Value::Text),
                    Value::Int64(peer.created_at_micros),
                    Value::Int64(peer.updated_at_micros),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "name".to_string(),
                "endpoint".to_string(),
                "token_env".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ],
            rows,
        ))
    }

    fn sync_retention_query_result(&self) -> Result<QueryResult> {
        let retention = self.sync_retention_report()?;
        let first_sequence = match retention.first_sequence {
            Some(value) => sync_u64_to_i64(value, "first_sequence")?,
            None => Value::Null,
        };
        let last_sequence = match retention.last_sequence {
            Some(value) => sync_u64_to_i64(value, "last_sequence")?,
            None => Value::Null,
        };
        let safe_prune_through = match retention.safe_prune_through {
            Some(value) => sync_u64_to_i64(value, "safe_prune_through")?,
            None => Value::Null,
        };

        Ok(QueryResult::with_rows(
            vec![
                "journal_records".to_string(),
                "first_sequence".to_string(),
                "last_sequence".to_string(),
                "safe_prune_through".to_string(),
                "prunable_records".to_string(),
                "blocked_by_json".to_string(),
                "journal_size_bytes".to_string(),
            ],
            vec![QueryRow::new(vec![
                sync_u64_to_i64(retention.journal_records as u64, "journal_records")?,
                first_sequence,
                last_sequence,
                safe_prune_through,
                sync_u64_to_i64(retention.prunable_records as u64, "prunable_records")?,
                Value::Text(
                    serde_json::to_string(&retention.blocked_by).map_err(|error| {
                        DbError::internal(format!(
                            "failed to encode sync retention blocked_by: {error}"
                        ))
                    })?,
                ),
                sync_u64_to_i64(retention.journal_size_bytes, "journal_size_bytes")?,
            ])],
        ))
    }

    fn sync_peer_lag_query_result(&self) -> Result<QueryResult> {
        let peer_lag = self.sync_peer_lag_report()?;
        let rows = peer_lag
            .into_iter()
            .map(|lag| {
                let in_watermark = match lag.in_watermark {
                    Some(value) => sync_u64_to_i64(value, "in_watermark")?,
                    None => Value::Null,
                };
                let out_watermark = match lag.out_watermark {
                    Some(value) => sync_u64_to_i64(value, "out_watermark")?,
                    None => Value::Null,
                };
                let local_high_watermark = match lag.local_high_watermark {
                    Some(value) => sync_u64_to_i64(value, "local_high_watermark")?,
                    None => Value::Null,
                };
                let in_lag = match lag.in_lag {
                    Some(value) => sync_u64_to_i64(value, "in_lag")?,
                    None => Value::Null,
                };
                let out_lag = match lag.out_lag {
                    Some(value) => sync_u64_to_i64(value, "out_lag")?,
                    None => Value::Null,
                };
                Ok(QueryRow::new(vec![
                    Value::Text(lag.peer_name),
                    lag.remote_replica_id.map_or(Value::Null, Value::Text),
                    in_watermark,
                    out_watermark,
                    local_high_watermark,
                    in_lag,
                    out_lag,
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "peer_name".to_string(),
                "remote_replica_id".to_string(),
                "in_watermark".to_string(),
                "out_watermark".to_string(),
                "local_high_watermark".to_string(),
                "in_lag".to_string(),
                "out_lag".to_string(),
            ],
            rows,
        ))
    }

    fn sync_doctor_query_result(&self) -> Result<QueryResult> {
        let report = self.sync_operational_doctor_report()?;
        Ok(QueryResult::with_rows(
            vec![
                "enabled".to_string(),
                "replica_id".to_string(),
                "highest_severity".to_string(),
                "journal_records".to_string(),
                "journal_size_bytes".to_string(),
                "unresolved_conflicts".to_string(),
                "guidance_json".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Bool(report.status.enabled),
                report.status.replica_id.map_or(Value::Null, Value::Text),
                Value::Text(report.highest_severity.to_string()),
                sync_u64_to_i64(report.integrity.total_records as u64, "journal_records")?,
                sync_u64_to_i64(report.retention.journal_size_bytes, "journal_size_bytes")?,
                sync_u64_to_i64(report.unresolved_conflicts as u64, "unresolved_conflicts")?,
                Value::Text(serde_json::to_string(&report.guidance).map_err(|error| {
                    DbError::internal(format!("failed to encode sync doctor guidance: {error}"))
                })?),
            ])],
        ))
    }

    fn sync_scopes_query_result(&self) -> Result<QueryResult> {
        let scopes = self.sync_scopes()?;
        let rows = scopes
            .into_iter()
            .map(|scope| {
                let SyncScope {
                    name,
                    include_tables,
                    row_filter,
                    filter_columns,
                    created_at_micros,
                    updated_at_micros,
                } = scope;
                Ok(QueryRow::new(vec![
                    Value::Text(name),
                    Value::Text(serde_json::to_string(&include_tables).map_err(|error| {
                        DbError::internal(format!(
                            "failed to encode sync scope include tables: {error}"
                        ))
                    })?),
                    row_filter.map_or(Value::Null, Value::Text),
                    Value::Text(serde_json::to_string(&filter_columns).map_err(|error| {
                        DbError::internal(format!(
                            "failed to encode sync scope filter columns: {error}"
                        ))
                    })?),
                    Value::Int64(created_at_micros),
                    Value::Int64(updated_at_micros),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "name".to_string(),
                "include_tables_json".to_string(),
                "row_filter".to_string(),
                "filter_columns_json".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ],
            rows,
        ))
    }

    fn sync_scope_tables_query_result(&self) -> Result<QueryResult> {
        let mut rows = Vec::new();
        for scope in self.sync_scopes()? {
            let scope_name = scope.name;
            for table_name in scope.include_tables {
                rows.push(QueryRow::new(vec![
                    Value::Text(scope_name.clone()),
                    Value::Text(table_name),
                ]));
            }
        }
        Ok(QueryResult::with_rows(
            vec!["scope_name".to_string(), "table_name".to_string()],
            rows,
        ))
    }

    fn sync_peer_scopes_query_result(&self) -> Result<QueryResult> {
        let bindings = self.sync_peer_scope_bindings()?;
        let rows = bindings
            .into_iter()
            .map(|binding| {
                Ok(QueryRow::new(vec![
                    Value::Text(binding.peer_name),
                    Value::Text(binding.scope_name),
                    Value::Int64(binding.created_at_micros),
                    Value::Int64(binding.updated_at_micros),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "peer_name".to_string(),
                "scope_name".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ],
            rows,
        ))
    }

    fn sync_sessions_query_result(&self) -> Result<QueryResult> {
        let sessions = self.sync_sessions()?;
        let rows = sessions
            .into_iter()
            .map(|session| {
                Ok(QueryRow::new(vec![
                    Value::Int64(session.session_id),
                    Value::Text(session.peer_name),
                    Value::Text(session.direction.to_string()),
                    session.remote_replica_id.map_or(Value::Null, Value::Text),
                    Value::Int64(session.started_at_micros),
                    session.ended_at_micros.map_or(Value::Null, Value::Int64),
                    Value::Text(session.status),
                    session.error.map_or(Value::Null, Value::Text),
                    session.pushed_batch_id.map_or(Value::Null, Value::Text),
                    session.pulled_batch_id.map_or(Value::Null, Value::Text),
                    Value::Int64(session.pushed_seen),
                    Value::Int64(session.pushed_applied),
                    Value::Int64(session.pushed_skipped),
                    Value::Int64(session.pushed_conflicted),
                    Value::Int64(session.pulled_seen),
                    Value::Int64(session.pulled_applied),
                    Value::Int64(session.pulled_skipped),
                    Value::Int64(session.pulled_conflicted),
                    Value::Int64(session.retry_count),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "session_id".to_string(),
                "peer_name".to_string(),
                "direction".to_string(),
                "remote_replica_id".to_string(),
                "started_at_micros".to_string(),
                "ended_at_micros".to_string(),
                "status".to_string(),
                "error".to_string(),
                "pushed_batch_id".to_string(),
                "pulled_batch_id".to_string(),
                "pushed_seen".to_string(),
                "pushed_applied".to_string(),
                "pushed_skipped".to_string(),
                "pushed_conflicted".to_string(),
                "pulled_seen".to_string(),
                "pulled_applied".to_string(),
                "pulled_skipped".to_string(),
                "pulled_conflicted".to_string(),
                "retry_count".to_string(),
            ],
            rows,
        ))
    }

    fn sync_conflict_policy_query_result(&self) -> Result<QueryResult> {
        let policy = self.sync_conflict_policy()?;
        Ok(QueryResult::with_rows(
            vec![
                "default_policy".to_string(),
                "origin_priority_json".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Text(policy.default_policy.to_string()),
                Value::Text(
                    serde_json::to_string(&policy.origin_priority).map_err(|error| {
                        DbError::internal(format!(
                            "failed to encode sync conflict policy origin priority: {error}"
                        ))
                    })?,
                ),
            ])],
        ))
    }

    fn sync_conflicts_query_result(&self) -> Result<QueryResult> {
        let sql = format!(
            "SELECT * FROM {} WHERE resolved = 0 ORDER BY conflict_id",
            crate::sync::CONFLICTS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(QueryResult::with_rows(
                        vec![
                            "conflict_id".to_string(),
                            "batch_id".to_string(),
                            "remote_replica_id".to_string(),
                            "remote_sequence".to_string(),
                            "table_name".to_string(),
                            "operation".to_string(),
                            "conflict_type".to_string(),
                            "message".to_string(),
                            "primary_key_json".to_string(),
                            "remote_record_json".to_string(),
                            "local_row_json".to_string(),
                            "created_at_micros".to_string(),
                            "resolved".to_string(),
                            "resolution".to_string(),
                            "resolved_at_micros".to_string(),
                            "resolved_by".to_string(),
                            "resolution_note".to_string(),
                            "policy_name".to_string(),
                            "local_record_json".to_string(),
                        ],
                        Vec::new(),
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }

    fn sync_relay_status_query_result(&self) -> Result<QueryResult> {
        let status = self.sync_relay_status(None, false, false, false, None)?;
        Ok(QueryResult::with_rows(
            vec![
                "relay_id".to_string(),
                "protocol_version".to_string(),
                "database_replica_id".to_string(),
                "production_mode".to_string(),
                "secure_transport_required".to_string(),
                "insecure_override_enabled".to_string(),
                "active_sessions".to_string(),
                "active_streams".to_string(),
                "started_at_micros".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Text(status.relay_id),
                Value::Int64(i64::from(status.protocol_version)),
                status
                    .database_replica_id
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                Value::Bool(status.production_mode),
                Value::Bool(status.secure_transport_required),
                Value::Bool(status.insecure_override_enabled),
                sync_u64_to_i64(status.active_sessions, "active_sessions")?,
                sync_u64_to_i64(status.active_streams, "active_streams")?,
                Value::Int64(status.started_at_micros),
            ])],
        ))
    }

    fn sync_relay_sessions_query_result(&self) -> Result<QueryResult> {
        self.query_table_or_empty(
            crate::sync::RELAY_SESSIONS_TABLE,
            &[
                "session_id",
                "tenant_id",
                "subject_id",
                "subject_kind",
                "request_id",
                "operation",
                "scope_name",
                "shape_id",
                "started_at_micros",
                "ended_at_micros",
                "status",
                "error",
                "rows_seen",
                "bytes_seen",
            ],
            "started_at_micros, session_id",
        )
    }

    fn sync_shapes_query_result(&self) -> Result<QueryResult> {
        self.query_table_or_empty(
            crate::sync::SHAPES_TABLE,
            &[
                "shape_id",
                "name",
                "scope_name",
                "tenant_id",
                "allowed_roles_json",
                "allowed_subjects_json",
                "created_at_micros",
                "updated_at_micros",
                "retention_ttl_micros",
                "max_records",
                "ack_deadline_micros",
                "heartbeat_micros",
            ],
            "shape_id",
        )
    }

    fn sync_shape_clients_query_result(&self) -> Result<QueryResult> {
        self.query_table_or_empty(
            crate::sync::SHAPE_CLIENTS_TABLE,
            &[
                "shape_id",
                "tenant_id",
                "client_replica_id",
                "subject_id",
                "session_id",
                "last_ack_sequence",
                "last_ack_watermark",
                "last_changeset_id",
                "last_seen_at_micros",
                "retention_blocking",
                "status",
            ],
            "shape_id, client_replica_id",
        )
    }

    fn sync_changeset_history_query_result(&self) -> Result<QueryResult> {
        self.query_table_or_empty(
            crate::sync::CHANGESET_HISTORY_TABLE,
            &[
                "changeset_id",
                "source_replica_id",
                "source_kind",
                "scope_name",
                "shape_id",
                "record_count",
                "bytes",
                "created_at_micros",
                "applied_at_micros",
                "outcome",
                "integrity_hash",
            ],
            "created_at_micros, changeset_id",
        )
    }

    fn query_table_or_empty(
        &self,
        table_name: &str,
        columns: &[&str],
        order_by: &str,
    ) -> Result<QueryResult> {
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {order_by}",
            columns
                .iter()
                .map(|column| sql_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            sql_identifier(table_name)
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(QueryResult::with_rows(
                        columns.iter().map(|column| (*column).to_string()).collect(),
                        Vec::new(),
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }

    pub(crate) fn sync_post_commit(
        &self,
        runtime: &mut EngineRuntime,
        committed_lsn: u64,
    ) -> Result<()> {
        let mutations = runtime.take_sync_mutations();
        if mutations.is_empty() {
            return Ok(());
        }
        if !self.inner.sync_ctx.capture_enabled() {
            return Ok(());
        }
        let enabled = if self.inner.sync_ctx.is_enabled() {
            true
        } else {
            let status = self.load_sync_status_from_runtime(runtime)?;
            if status.enabled {
                self.inner.sync_ctx.set_enabled(true);
                if let Some(replica_id) = status.replica_id.as_deref() {
                    self.inner.sync_ctx.set_replica_id(replica_id);
                }
                self.inner.sync_ctx.set_next_sequence(status.next_sequence);
            }
            status.enabled
        };
        if !enabled {
            return Ok(());
        }
        self.inner
            .sync_ctx
            .pending_mutations
            .lock()
            .map_err(|_| DbError::internal("sync pending mutations lock poisoned"))?
            .extend(mutations);
        self.inner
            .sync_ctx
            .flush_journal(&self.inner.vfs, committed_lsn)?;
        Ok(())
    }

    fn take_reactive_pending_commit(
        &self,
        runtime: &mut EngineRuntime,
    ) -> Option<PendingReactiveCommit> {
        let Some(hub) = self
            .reactive_hub_if_available()
            .filter(|hub| hub.has_watchers())
        else {
            let _ = runtime.take_reactive_mutations();
            return None;
        };
        let mut changed = runtime
            .dirty_tables
            .iter()
            .filter(|table| !crate::sync::is_internal_table_name(table))
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut row_changes = runtime.take_reactive_mutations();
        for change in &row_changes {
            changed.insert(change.table.clone());
        }
        let schema_changed = match self.inner.catalog.schema_cookie() {
            Ok(cookie) => cookie != runtime.catalog.schema_cookie,
            Err(_) => false,
        };
        if changed.is_empty() && row_changes.is_empty() && !schema_changed {
            return None;
        }
        let max_rows = hub.max_row_changes_per_event();
        let row_changes_truncated = max_rows > 0 && row_changes.len() > max_rows;
        if row_changes_truncated {
            row_changes.clear();
        }
        Some(PendingReactiveCommit {
            source: crate::reactive::current_change_source(),
            schema_cookie: runtime.catalog.schema_cookie,
            changed_tables: changed.into_iter().collect(),
            row_changes,
            row_changes_truncated,
            schema_changed,
        })
    }

    fn publish_reactive_commit(&self, pending: Option<PendingReactiveCommit>, committed_lsn: u64) {
        let Some(pending) = pending else {
            return;
        };
        if let Some(hub) = self.reactive_hub_if_available() {
            hub.publish(pending, committed_lsn);
        }
    }

    fn configure_runtime_sync_capture(&self, runtime: &mut EngineRuntime) -> Result<()> {
        let active = self.runtime_sync_capture_should_be_active(runtime)?;
        runtime.set_sync_capture_active(active);
        runtime.set_reactive_capture_active(self.reactive_has_watchers());
        Ok(())
    }

    fn runtime_sync_capture_should_be_active(&self, runtime: &mut EngineRuntime) -> Result<bool> {
        if !self.inner.sync_ctx.capture_enabled() {
            return Ok(false);
        }
        if self.inner.sync_ctx.is_enabled() {
            return Ok(true);
        }
        if runtime.catalog.table(crate::sync::METADATA_TABLE).is_none() {
            return Ok(false);
        }
        let status = self.load_sync_status_from_runtime(runtime)?;
        if !status.enabled {
            return Ok(false);
        }
        self.inner.sync_ctx.set_enabled(true);
        if let Some(replica_id) = status.replica_id.as_deref() {
            self.inner.sync_ctx.set_replica_id(replica_id);
        }
        self.inner.sync_ctx.set_next_sequence(status.next_sequence);
        Ok(true)
    }
}

fn resolve_prepared_simple_value_for_fast_path(
    source: &PreparedSimpleValueSource,
    params: &[Value],
) -> Result<Value> {
    match source {
        PreparedSimpleValueSource::Literal(value) => Ok(value.clone()),
        PreparedSimpleValueSource::Parameter(number) => params
            .get(number.saturating_sub(1))
            .cloned()
            .ok_or_else(|| DbError::sql(format!("parameter ${number} was not provided"))),
    }
}

/// Evicts the shared WAL registry entry for an on-disk database path.
pub fn evict_shared_wal(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let vfs = VfsHandle::for_path(path);
    WalHandle::evict(&vfs, path)
}

#[cfg(test)]
mod tests;
