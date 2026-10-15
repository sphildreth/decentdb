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
    resolve_prepared_simple_value, row_id_alias_column_name, PreparedDeleteLookup,
    PreparedSimpleDelete, PreparedSimpleInsert, PreparedSimpleUpdate, PreparedSimpleValueSource,
};
use crate::exec::{
    contiguous_row_ids, read_persisted_table_row_count,
    read_table_payload_live_row_count_from_bytes, row_satisfies_expression, statement_is_read_only,
    BulkLoadOptions, EngineRuntime, QueryResult, QueryRow, ResolvedSimpleJoinProjection,
    ResolvedSimpleOrderedRowIdProjectionRequest, ResolvedSimpleRowIdJoinProjectionRequest,
    ResolvedSimpleRowIdProjectionRequest, ResolvedSimpleRowIdRangeProjectionRequest, RuntimeIndex,
    RuntimeRowIdSet, SimpleJoinProjectionSide, SimpleRangeBoundValue, SimpleRowIdProjectionRequest,
    TableData,
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
use crate::sql::ast::{
    BinaryOp, DeleteStatement, Expr, FromItem, QueryBody, SelectItem, Statement as SqlStatement,
};
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

mod branch_ops;
mod pragmas;
mod prepared_fast_paths;
mod reactive_ops;
mod sync_ops;

use audit::*;
use branches::*;
use open::*;
use query_api::*;
use schema::*;
use sync_api::*;

const APPLICATION_PRAGMA_TABLE: &str = "__decentdb_application_pragmas";
static AUDIT_EVENT_COUNTER: AtomicU64 = AtomicU64::new(1);
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
const BOOTSTRAP_SYNC_WORKER_STACK_SIZE: usize = 64 * 1024;

#[cfg(test)]
std::thread_local! {
    static EXECUTE_BATCH_DIRECT_SPLIT_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static EXECUTE_WRITE_BASE_TEMP_CLASSIFICATION_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static EXPLICIT_SCHEMA_BATCH_FAST_PATH_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static PAGED_ROW_SOURCE_HEAP_RELEASE_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    static FORCE_BOOTSTRAP_SYNC_SPAWN_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn reset_schema_batch_fixed_cost_counters() {
    EXECUTE_BATCH_DIRECT_SPLIT_COUNT.with(|count| count.set(0));
    EXECUTE_WRITE_BASE_TEMP_CLASSIFICATION_COUNT.with(|count| count.set(0));
    EXPLICIT_SCHEMA_BATCH_FAST_PATH_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn schema_batch_fixed_cost_counters() -> (u64, u64) {
    (
        EXECUTE_BATCH_DIRECT_SPLIT_COUNT.with(std::cell::Cell::get),
        EXECUTE_WRITE_BASE_TEMP_CLASSIFICATION_COUNT.with(std::cell::Cell::get),
    )
}

#[cfg(test)]
fn explicit_schema_batch_fast_path_count() -> u64 {
    EXPLICIT_SCHEMA_BATCH_FAST_PATH_COUNT.with(std::cell::Cell::get)
}

#[cfg(test)]
fn reset_paged_row_source_heap_release_count() {
    PAGED_ROW_SOURCE_HEAP_RELEASE_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn paged_row_source_heap_release_count() -> u64 {
    PAGED_ROW_SOURCE_HEAP_RELEASE_COUNT.with(std::cell::Cell::get)
}

#[cfg(all(test, not(all(target_arch = "wasm32", target_os = "unknown"))))]
fn force_next_bootstrap_sync_spawn_failure() {
    FORCE_BOOTSTRAP_SYNC_SPAWN_FAILURE.with(|force| force.set(true));
}

/// Stable engine owner used across later storage, SQL, and FFI slices.
#[derive(Clone, Debug)]
pub struct Db {
    inner: Arc<DbInner>,
}

enum OpenWithVfsOutcome {
    Opened(Db),
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    RetryAfterBootstrapSync(Box<FreshWalOpenRetry>),
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
struct FreshWalOpenRetry {
    path: PathBuf,
    config: DbConfig,
    vfs: VfsHandle,
    coordination_vfs: VfsHandle,
    file: Arc<dyn VfsFile>,
    initialized_header: DatabaseHeader,
    initialized_open_lock_key: Option<PathBuf>,
}

const AUTOCOMMIT_PAGED_ROW_SOURCE_MAX_RESIDENT: usize = 4;
const PREPARED_READ_ROW_SOURCE_MIN_ROWS: usize = 4_096;
const PREPARED_READ_ROW_SOURCE_MIN_ROW_LIMIT: usize = 131_072;
const PREPARED_READ_ROW_SOURCE_ROWS_PER_CACHE_MB: usize = 8_192;
const PAGED_ROW_SOURCE_HEAP_RELEASE_THRESHOLD: usize = 1024 * 1024;
const RESIDENT_COMMIT_HEAP_RELEASE_THRESHOLD: usize = 4 * 1024 * 1024;

const fn should_release_freed_paged_row_source_heap(freed_bytes: usize) -> bool {
    freed_bytes >= PAGED_ROW_SOURCE_HEAP_RELEASE_THRESHOLD
}

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
    simple_indexed_projection: Option<PreparedSimpleIndexedProjection>,
    simple_row_id_range_projection: Option<PreparedSimpleRowIdRangeProjection>,
    simple_ordered_row_id_projection: Option<PreparedSimpleOrderedRowIdProjection>,
    simple_row_id_join_projection: Option<PreparedSimpleRowIdJoinProjection>,
    simple_scalar_filtered_aggregate: Option<PreparedSimpleScalarFilteredAggregate>,
    prepared_insert: Option<Arc<PreparedSimpleInsert>>,
    prepared_update: Option<Arc<PreparedSimpleUpdate>>,
    prepared_delete: Option<Arc<PreparedSimpleDelete>>,
    read_only: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedPlanBundle {
    statement: Arc<SqlStatement>,
    simple_row_id_projection: Option<PreparedSimpleRowIdProjection>,
    simple_indexed_projection: Option<PreparedSimpleIndexedProjection>,
    simple_row_id_range_projection: Option<PreparedSimpleRowIdRangeProjection>,
    simple_ordered_row_id_projection: Option<PreparedSimpleOrderedRowIdProjection>,
    simple_row_id_join_projection: Option<PreparedSimpleRowIdJoinProjection>,
    simple_scalar_filtered_aggregate: Option<PreparedSimpleScalarFilteredAggregate>,
    prepared_insert: Option<Arc<PreparedSimpleInsert>>,
    prepared_update: Option<Arc<PreparedSimpleUpdate>>,
    prepared_delete: Option<Arc<PreparedSimpleDelete>>,
    read_only: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedPlanCacheEntry {
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
pub(crate) struct PreparedSimpleRowIdProjection {
    table_name: String,
    projection_indexes: Vec<usize>,
    column_names: Arc<[String]>,
    param_index: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSimpleIndexedProjection {
    table_name: String,
    projection_indexes: Vec<usize>,
    column_names: Arc<[String]>,
    lookup: PreparedSimpleIndexedProjectionLookup,
}

#[derive(Clone, Debug)]
enum PreparedSimpleIndexedProjectionLookup {
    RowId {
        value_source: PreparedSimpleValueSource,
    },
    Index {
        index_name: String,
        value_source: PreparedSimpleValueSource,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedSimpleRangeBoundParam {
    inclusive: bool,
    param_index: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSimpleRowIdRangeProjection {
    table_name: String,
    projection_indexes: Vec<usize>,
    column_names: Arc<[String]>,
    filter_column: String,
    lower_bound: Option<PreparedSimpleRangeBoundParam>,
    upper_bound: Option<PreparedSimpleRangeBoundParam>,
    limit_param_index: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSimpleOrderedRowIdProjection {
    table_name: String,
    order_column: String,
    projection_indexes: Vec<usize>,
    column_names: Arc<[String]>,
    limit: Option<usize>,
    offset: usize,
    descending: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSimpleRowIdJoinProjection {
    left_table_name: String,
    right_table_name: String,
    left_projection_indexes: Vec<usize>,
    right_projection_indexes: Vec<usize>,
    projections: Vec<ResolvedSimpleJoinProjection>,
    column_names: Arc<[String]>,
    param_index: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedSimpleScalarFilteredAggregate {
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
    prepared_insert_candidate: Vec<Value>,
    prepared_insert_encoded_values: Vec<u8>,
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
    #[inline(always)]
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
    #[inline(always)]
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
            if self.state.prepared_insert_last_next_row_id.is_none() {
                self.state.prepared_insert_last_cache_key =
                    Some(Db::prepared_statement_cache_key(self.prepared));
                self.state.prepared_insert_last_plan = Some(Arc::clone(prepared_insert));
                self.state.prepared_insert_last_next_row_id =
                    Db::prepared_insert_current_next_row_id(
                        &self.state.runtime,
                        prepared_insert.as_ref(),
                    )?;
            }
            let affected = if let Some(cached_next_row_id) =
                self.state.prepared_insert_last_next_row_id.as_mut()
            {
                self.state
                    .runtime
                    .execute_prepared_simple_insert_positional_params_in_place_with_reusable_buffers(
                        prepared_insert.as_ref(),
                        params,
                        &mut self.prepared_insert_candidate,
                        &mut self.prepared_insert_encoded_values,
                        cached_next_row_id,
                        self.db.inner.config.page_size,
                    )?
            } else {
                self.state
                    .runtime
                    .execute_prepared_simple_insert_positional_params_in_place_with_candidate(
                        prepared_insert.as_ref(),
                        params,
                        &mut self.prepared_insert_candidate,
                        self.db.inner.config.page_size,
                    )?
            };
            if !self.state.persistent_changed {
                self.state.persistent_changed |= Db::prepared_insert_changes_persistent_table(
                    &self.state.runtime,
                    prepared_insert,
                );
            }
            return Ok(affected);
        }

        let result =
            self.db
                .execute_prepared_in_exclusive_state_mut(self.prepared, params, self.state)?;
        Ok(result.affected_rows())
    }

    #[cfg(test)]
    fn prepared_insert_buffer_state_for_tests(
        &self,
    ) -> (usize, usize, *const Value, usize, *const u8) {
        (
            self.prepared_insert_candidate.len(),
            self.prepared_insert_candidate.capacity(),
            self.prepared_insert_candidate.as_ptr(),
            self.prepared_insert_encoded_values.capacity(),
            self.prepared_insert_encoded_values.as_ptr(),
        )
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
    prepared_insert_last_cache_key: Option<usize>,
    prepared_insert_last_plan: Option<Arc<PreparedSimpleInsert>>,
    prepared_insert_last_next_row_id: Option<i64>,
    prepared_insert_candidate: Vec<Value>,
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

struct SimpleRowIdRangeDeleteSql {
    table_name: String,
    column_name: String,
    low: i64,
    high: i64,
}

fn parse_simple_row_id_range_delete_sql(sql: &str) -> Option<SimpleRowIdRangeDeleteSql> {
    let tokens = sql.split_ascii_whitespace().collect::<Vec<_>>();
    if tokens.len() != 9
        || !tokens[0].eq_ignore_ascii_case("DELETE")
        || !tokens[1].eq_ignore_ascii_case("FROM")
        || !tokens[3].eq_ignore_ascii_case("WHERE")
        || !tokens[5].eq_ignore_ascii_case("BETWEEN")
        || !tokens[7].eq_ignore_ascii_case("AND")
    {
        return None;
    }
    let table_name = parse_simple_sql_identifier(tokens[2])?;
    let column_name = parse_simple_sql_identifier(tokens[4])?;
    let low = tokens[6].parse::<i64>().ok()?;
    let high = tokens[8].parse::<i64>().ok()?;
    Some(SimpleRowIdRangeDeleteSql {
        table_name,
        column_name,
        low,
        high,
    })
}

fn parse_simple_sql_identifier(token: &str) -> Option<String> {
    if token.is_empty() {
        return None;
    }
    let mut parts = token.split('.');
    let first = parts.next()?;
    let second = parts.next();
    if parts.next().is_some() {
        return None;
    }
    let identifier = second.unwrap_or(first);
    if identifier.is_empty() {
        return None;
    }
    let mut chars = identifier.chars();
    let first = chars.next()?;
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return None;
    }
    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return None;
    }
    Some(identifier.to_string())
}

fn simple_row_id_range_delete_statement(request: &SimpleRowIdRangeDeleteSql) -> SqlStatement {
    SqlStatement::Delete(DeleteStatement {
        table_name: request.table_name.clone(),
        filter: Some(Expr::Between {
            expr: Box::new(Expr::Column {
                table: None,
                column: request.column_name.clone(),
            }),
            low: Box::new(Expr::Literal(Value::Int64(request.low))),
            high: Box::new(Expr::Literal(Value::Int64(request.high))),
            negated: false,
        }),
        returning: Vec::new(),
    })
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
        // The bootstrap contract needs the header, catalog root, and the file
        // length needed to address them to be durable. `VfsFile::sync_data`
        // explicitly provides that guarantee without forcing unrelated inode
        // metadata (timestamps/ownership) through the create hot path.
        Self::open_fresh_with_bootstrap_sync(
            path.to_path_buf(),
            config,
            vfs,
            coordination_vfs,
            file,
            header,
        )
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn open_fresh_with_bootstrap_sync(
        path: PathBuf,
        config: DbConfig,
        vfs: VfsHandle,
        coordination_vfs: VfsHandle,
        file: Arc<dyn VfsFile>,
        header: DatabaseHeader,
    ) -> Result<Self> {
        let reservation_vfs = vfs.clone();
        let Some(bootstrap_sync_reservation) =
            reservation_vfs.concurrent_bootstrap_sync_reservation()
        else {
            file.sync_data()?;
            return Self::open_with_vfs(
                path,
                config,
                vfs,
                coordination_vfs,
                file,
                Some(header),
                None,
            );
        };

        // Resolve and cache the same-process lock key before starting the
        // worker. Native canonicalization may inspect the main path; WAL and
        // coordination initialization reuse this hint during the overlap.
        let initialized_open_lock_key = if vfs.is_memory() {
            None
        } else {
            match vfs.canonicalize_path(&path) {
                Ok(path) => Some(path),
                Err(error) => {
                    file.sync_data()?;
                    return Err(error);
                }
            }
        };
        let (vfs, coordination_vfs) = if let Some(canonical_path) = &initialized_open_lock_key {
            let source_path = path.clone();
            (
                vfs.with_canonical_path_hint(source_path.clone(), canonical_path.clone()),
                coordination_vfs.with_canonical_path_hint(source_path, canonical_path.clone()),
            )
        } else {
            (vfs, coordination_vfs)
        };

        let result = std::thread::scope(|scope| {
            #[cfg(test)]
            let force_spawn_failure =
                FORCE_BOOTSTRAP_SYNC_SPAWN_FAILURE.with(|force| force.replace(false));
            #[cfg(not(test))]
            let force_spawn_failure = false;

            let sync_file = Arc::clone(&file);
            let sync_worker = if force_spawn_failure {
                None
            } else {
                std::thread::Builder::new()
                    .name("decentdb-bootstrap-sync".to_string())
                    .stack_size(BOOTSTRAP_SYNC_WORKER_STACK_SIZE)
                    .spawn_scoped(scope, move || sync_file.sync_data())
                    .ok()
            };
            let Some(sync_worker) = sync_worker else {
                // Thread creation is an optimization boundary, not a create
                // failure. Preserve the original ordering exactly: durable
                // sync first, then initialize the database handle.
                file.sync_data()?;
                return Self::open_with_vfs(
                    path,
                    config,
                    vfs,
                    coordination_vfs,
                    file,
                    Some(header),
                    initialized_open_lock_key,
                );
            };

            let db_result = Self::open_with_vfs_impl(
                path,
                config,
                vfs,
                coordination_vfs,
                file,
                Some(header),
                initialized_open_lock_key,
                true,
            );
            let sync_result = sync_worker
                .join()
                .map_err(|_| DbError::internal("fresh database bootstrap sync worker panicked"))?;
            // The durability barrier has precedence over any concurrently
            // observed initialization error, and no Db may escape until the
            // worker has completed successfully.
            sync_result?;
            match db_result? {
                OpenWithVfsOutcome::Opened(db) => Ok(db),
                OpenWithVfsOutcome::RetryAfterBootstrapSync(retry) => {
                    let retry = *retry;
                    Self::open_with_vfs(
                        retry.path,
                        retry.config,
                        retry.vfs,
                        retry.coordination_vfs,
                        retry.file,
                        Some(retry.initialized_header),
                        retry.initialized_open_lock_key,
                    )
                }
            }
        });
        drop(bootstrap_sync_reservation);
        result
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    fn open_fresh_with_bootstrap_sync(
        path: PathBuf,
        config: DbConfig,
        vfs: VfsHandle,
        coordination_vfs: VfsHandle,
        file: Arc<dyn VfsFile>,
        header: DatabaseHeader,
    ) -> Result<Self> {
        file.sync_data()?;
        Self::open_with_vfs(
            path,
            config,
            vfs,
            coordination_vfs,
            file,
            Some(header),
            None,
        )
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
        let initialized_header = if vfs.is_memory() && file.file_size()? == 0 {
            let header = DatabaseHeader::new(config.page_size);
            storage::write_database_bootstrap_vfs(file.as_ref(), &header)?;
            file.sync_data()?;
            Some(header)
        } else {
            None
        };
        Self::open_with_vfs(
            path.to_path_buf(),
            config,
            vfs,
            coordination_vfs,
            file,
            initialized_header,
            None,
        )
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
        file.set_len(u64::from(page_count) * u64::from(page_size))?;
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
        // `begin_write` refreshed coordinated checkpoint changes before this
        // transaction became active. The pager count therefore covers the
        // current main-file tail, while the shared/recovered WAL maximum
        // covers committed pages that have not reached that tail yet.
        let max_staged_page_id = txn.staged_pages.keys().next_back().copied().unwrap_or(0);
        let max_allocated_page_id = self
            .inner
            .pager
            .cached_page_count()
            .max(self.inner.wal.max_page_count())
            .max(max_staged_page_id);
        let next_page_id = max_allocated_page_id.checked_add(1).ok_or_else(|| {
            DbError::constraint(format!(
                "database page-id space exhausted; maximum page id {max_allocated_page_id} is already allocated"
            ))
        })?;
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
        self.inner.pager.read_page_from_disk(page_id)
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
        self.inner.pager.read_page_from_disk(page_id)
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
        self.inner.pager.read_page_from_disk(page_id)
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
            self.try_mark_runtime_current_after_explicit_checkpoint(checkpoint_epoch_after)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn wal_checkpoint_tail_state_for_tests(&self) -> (u64, bool, bool, bool) {
        self.inner.wal.checkpoint_tail_state_for_tests()
    }

    fn try_mark_runtime_current_after_explicit_checkpoint(
        &self,
        checkpoint_epoch: u64,
    ) -> Result<()> {
        let latest_lsn = self.inner.wal.latest_snapshot();
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let writer_last_commit_lsn = self.inner.writer_last_commit_lsn.load(Ordering::Acquire);
        let header = self.inner.pager.header_snapshot()?;
        if last_runtime_lsn > 0
            && writer_last_commit_lsn > 0
            && last_runtime_lsn >= writer_last_commit_lsn
            && latest_lsn == 0
            && header.last_checkpoint_lsn == last_runtime_lsn
            && header.last_checkpoint_lsn >= writer_last_commit_lsn
        {
            self.inner
                .last_seen_checkpoint_epoch
                .store(checkpoint_epoch, Ordering::Release);
            self.inner
                .last_runtime_lsn
                .store(latest_lsn, Ordering::Release);
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
        #[cfg(test)]
        EXECUTE_BATCH_DIRECT_SPLIT_COUNT.with(|count| count.set(count.get().saturating_add(1)));
        let statement_sqls = split_sql_batch(sql);
        if params.is_empty() && !self.inner.sql_txn_active.load(Ordering::Acquire) {
            if let Some(results) =
                self.try_execute_explicit_schema_batch_with_single_dispatch(&statement_sqls)?
            {
                return Ok(results);
            }
            if let Some(results) =
                self.try_execute_schema_batch_with_single_commit(&statement_sqls)?
            {
                return Ok(results);
            }
        }

        let mut results = Vec::new();
        for statement_sql in statement_sqls {
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
            if params.is_empty() {
                if let Some(result) =
                    self.try_execute_simple_row_id_range_delete_sql_fast_path(trimmed)?
                {
                    self.record_statement_trace(
                        trimmed,
                        false,
                        std::time::Duration::ZERO,
                        0,
                        Ok(&result),
                    );
                    results.push(result);
                    continue;
                }
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

    /// Executes the common `BEGIN; <persistent DDL>...; COMMIT;` shape while
    /// retaining the public explicit-transaction contract. In particular, a
    /// statement failure leaves the transaction active so the caller can
    /// inspect or roll it back, exactly as the ordinary batch dispatcher
    /// does. The optimization only removes repeated command classification
    /// and transaction-mutex acquisition after every statement.
    fn try_execute_explicit_schema_batch_with_single_dispatch(
        &self,
        statement_sqls: &[String],
    ) -> Result<Option<Vec<QueryResult>>> {
        if self.inner.tracing.any_enabled() || statement_sqls.len() < 3 {
            return Ok(None);
        }

        let Some(first) = statement_sqls.first() else {
            return Ok(None);
        };
        let Some(last) = statement_sqls.last() else {
            return Ok(None);
        };
        if parse_transaction_control(first.trim()) != Some(TransactionControl::Begin)
            || parse_transaction_control(last.trim()) != Some(TransactionControl::Commit)
        {
            return Ok(None);
        }

        let ddl_sqls = &statement_sqls[1..statement_sqls.len() - 1];
        for statement_sql in ddl_sqls {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() || parse_transaction_control(trimmed).is_some() {
                return Ok(None);
            }
        }
        let parsed_statements =
            match crate::sql::parser::parse_plain_sql_batch_single_dispatch(ddl_sqls) {
                Ok(Some(statements)) => statements,
                // Let the ordinary dispatcher reproduce its usual error and
                // explicit-transaction state for malformed or rewritten SQL.
                Ok(None) | Err(_) => return Ok(None),
            };
        if parsed_statements.is_empty() {
            return Ok(None);
        }
        let mut statements = Vec::with_capacity(parsed_statements.len());
        for statement in parsed_statements {
            if !matches!(
                &statement,
                SqlStatement::CreateTable(_)
                    | SqlStatement::CreateTableAs(_)
                    | SqlStatement::CreateSchema { .. }
                    | SqlStatement::CreateIndex(_)
                    | SqlStatement::CreateView(_)
                    | SqlStatement::CreateTrigger(_),
            ) {
                return Ok(None);
            }
            statements.push(Arc::new(statement));
        }

        // Temp-schema classification may depend on objects created earlier in
        // the batch. Any statement that is already known to be temporary is
        // sufficient to route the whole batch through the general path.
        {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            if statements
                .iter()
                .any(|statement| self.statement_is_temp_only(&runtime, statement.as_ref()))
            {
                return Ok(None);
            }
        }

        self.begin_transaction()?;
        self.inner.tracing.mark_in_transaction();
        let mut results = Vec::with_capacity(statement_sqls.len());
        results.push(QueryResult::with_affected_rows(0));
        {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            let SqlTxnSlot::Shared(state) = &mut *txn else {
                return Err(DbError::internal(
                    "explicit schema batch lost its SQL transaction state",
                ));
            };
            for statement in &statements {
                let result =
                    self.execute_statement_in_state("", statement.as_ref(), &[], state.as_mut())?;
                self.dispatch_plan_cache_invalidation(statement);
                results.push(result);
            }
        }
        self.commit_transaction()?;
        self.inner.tracing.mark_active();
        results.push(QueryResult::with_affected_rows(0));
        #[cfg(test)]
        EXPLICIT_SCHEMA_BATCH_FAST_PATH_COUNT
            .with(|count| count.set(count.get().saturating_add(1)));
        Ok(Some(results))
    }

    fn try_execute_schema_batch_with_single_commit(
        &self,
        statement_sqls: &[String],
    ) -> Result<Option<Vec<QueryResult>>> {
        let mut statements = Vec::new();
        for statement_sql in statement_sqls {
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

    fn try_execute_simple_row_id_range_delete_sql_fast_path(
        &self,
        sql: &str,
    ) -> Result<Option<QueryResult>> {
        let Some(request) = parse_simple_row_id_range_delete_sql(sql) else {
            return Ok(None);
        };
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            let mut txn = self
                .inner
                .sql_txn
                .lock()
                .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
            return match &mut *txn {
                SqlTxnSlot::Shared(state) => {
                    self.try_execute_simple_row_id_range_delete_in_state(&request, state)
                }
                SqlTxnSlot::Exclusive => Err(self.exclusive_sql_txn_error()),
                SqlTxnSlot::None => Ok(None),
            };
        }

        let prepared = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            runtime.prepare_simple_row_id_range_delete(
                &request.table_name,
                &request.column_name,
                request.low,
                request.high,
            )?
        };
        let Some(prepared) = prepared else {
            return Ok(None);
        };
        self.execute_autocommit_simple_delete_in_place(&prepared, &[])
            .map(Some)
    }

    fn try_execute_simple_row_id_range_delete_in_state(
        &self,
        request: &SimpleRowIdRangeDeleteSql,
        state: &mut SqlTxnState,
    ) -> Result<Option<QueryResult>> {
        if state.indexes_maybe_stale {
            state
                .runtime
                .rebuild_stale_indexes(self.inner.config.page_size)?;
            state.indexes_maybe_stale = false;
        }
        let Some(mut prepared) = state.runtime.prepare_simple_row_id_range_delete(
            &request.table_name,
            &request.column_name,
            request.low,
            request.high,
        )?
        else {
            return Ok(None);
        };
        let table_names = prepared.required_row_source_table_names();
        let child_index_targets = prepared.child_index_hydration_targets();
        let snapshot_lsn = state.snapshot_lsn();
        self.load_runtime_table_row_sources_and_child_indexes_at_snapshot(
            &mut state.runtime,
            &table_names,
            &child_index_targets,
            snapshot_lsn,
        )?;
        if !state.runtime.can_reuse_prepared_simple_delete(&prepared) {
            let Some(reprepared) = state.runtime.prepare_simple_row_id_range_delete(
                &request.table_name,
                &request.column_name,
                request.low,
                request.high,
            )?
            else {
                return Ok(None);
            };
            prepared = reprepared;
        }
        let temp_only = prepared.table.temporary;
        let result = state.runtime.execute_prepared_simple_delete(
            &prepared,
            &[],
            self.inner.config.page_size,
        )?;
        state.persistent_changed |= !temp_only;
        Ok(Some(result))
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
        if let Some(result) = self.try_execute_simple_count_observed_current(plan.table_name)? {
            return Ok(Some(result));
        }

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
        if let Some(result) = self.try_execute_simple_row_id_projection_observed_current(
            plan.table_name,
            &plan.projection_columns,
            plan.filter_column,
            *lookup_row_id,
        )? {
            return Ok(Some(result));
        }

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

    fn try_execute_simple_count_observed_current(
        &self,
        table_name: &str,
    ) -> Result<Option<QueryResult>> {
        let Some(snapshot_lsn) = self.observed_current_resident_snapshot_lsn()? else {
            return Ok(None);
        };
        let Some(runtime) =
            self.runtime_read_for_observed_current_resident_fast_read(snapshot_lsn)?
        else {
            return Ok(None);
        };
        let Some(table) = runtime.catalog.table(table_name) else {
            return Ok(None);
        };
        if runtime.temp_table_schema(table_name).is_some()
            || runtime
                .catalog
                .views
                .keys()
                .any(|view_name| identifiers_equal(view_name, table_name))
        {
            return Ok(None);
        }
        let Some(row_count) =
            self.runtime_table_row_count_without_storage(&runtime, &table.name)?
        else {
            return Ok(None);
        };
        let row_count = i64::try_from(row_count).map_err(|_| {
            DbError::sql(format!(
                "table {table_name} exceeds COUNT(*) row-count limits"
            ))
        })?;
        if !self.observed_current_resident_snapshot_still_valid(snapshot_lsn)? {
            return Ok(None);
        }
        Ok(Some(QueryResult::with_rows(
            vec!["COUNT(*)".to_string()],
            vec![QueryRow::new(vec![Value::Int64(row_count)])],
        )))
    }

    fn try_execute_simple_row_id_projection_observed_current(
        &self,
        table_name: &str,
        projection_columns: &[&str],
        filter_column: &str,
        lookup_row_id: i64,
    ) -> Result<Option<QueryResult>> {
        let Some(snapshot_lsn) = self.observed_current_resident_snapshot_lsn()? else {
            return Ok(None);
        };
        let Some(runtime) =
            self.runtime_read_for_observed_current_resident_fast_read(snapshot_lsn)?
        else {
            return Ok(None);
        };
        let result = runtime.try_execute_resident_simple_row_id_projection(
            table_name,
            projection_columns,
            filter_column,
            lookup_row_id,
        )?;
        if result.is_none() {
            return Ok(None);
        }
        if !self.observed_current_resident_snapshot_still_valid(snapshot_lsn)? {
            return Ok(None);
        }
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
        file: Arc<dyn VfsFile>,
        initialized_header: Option<DatabaseHeader>,
        initialized_open_lock_key: Option<PathBuf>,
    ) -> Result<Self> {
        match Self::open_with_vfs_impl(
            path,
            config,
            vfs,
            coordination_vfs,
            file,
            initialized_header,
            initialized_open_lock_key,
            false,
        )? {
            OpenWithVfsOutcome::Opened(db) => Ok(db),
            #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
            OpenWithVfsOutcome::RetryAfterBootstrapSync(_) => Err(DbError::internal(
                "normal database open unexpectedly requested a fresh-WAL retry",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn open_with_vfs_impl(
        path: PathBuf,
        config: DbConfig,
        vfs: VfsHandle,
        coordination_vfs: VfsHandle,
        file: Arc<dyn VfsFile>,
        initialized_header: Option<DatabaseHeader>,
        initialized_open_lock_key: Option<PathBuf>,
        require_fresh_wal: bool,
    ) -> Result<OpenWithVfsOutcome> {
        let storage_was_freshly_initialized = initialized_header.is_some();
        let open_lock_key = if let Some(open_lock_key) = &initialized_open_lock_key {
            Some(open_lock_key.clone())
        } else if vfs.is_memory() {
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

        let mut header = match &initialized_header {
            Some(header) => header.clone(),
            None => storage::read_database_header_vfs(file.as_ref())?,
        };
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

        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let fresh_wal_retry_header = require_fresh_wal.then(|| header.clone());
        let pager = if storage_was_freshly_initialized {
            PagerHandle::open_fresh_bootstrap_with_page_pool(
                Arc::clone(&file),
                header,
                effective_config.cache_size_mb,
                effective_config.page_pool_max,
            )
        } else {
            PagerHandle::open_with_page_pool(
                Arc::clone(&file),
                header,
                effective_config.cache_size_mb,
                effective_config.page_pool_max,
            )?
        };
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        if require_fresh_wal {
            return Err(DbError::internal(
                "fresh-WAL bootstrap retries require the native runtime",
            ));
        }
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let wal = if require_fresh_wal {
            let Some(wal) = WalHandle::acquire_fresh(
                &vfs,
                &path,
                &effective_config,
                &pager,
                process_coordinator,
            )?
            else {
                return Ok(OpenWithVfsOutcome::RetryAfterBootstrapSync(Box::new(
                    FreshWalOpenRetry {
                        path,
                        config: effective_config,
                        vfs,
                        coordination_vfs,
                        file,
                        initialized_header: fresh_wal_retry_header.ok_or_else(|| {
                            DbError::internal(
                                "fresh-WAL acquisition declined without a bootstrap header",
                            )
                        })?,
                        initialized_open_lock_key,
                    },
                )));
            };
            wal
        } else {
            WalHandle::acquire(&vfs, &path, &effective_config, &pager, process_coordinator)?
        };
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let wal = WalHandle::acquire(&vfs, &path, &effective_config, &pager, process_coordinator)?;
        // Capture this immediately after WAL recovery. A later on-open
        // checkpoint may empty a recovered non-empty WAL, but that storage
        // must still take the normal runtime load path.
        let freshly_initialized_empty_wal =
            storage_was_freshly_initialized && wal.latest_snapshot() == 0;
        // Pager open seeded this count from the same configured file handle.
        // Reuse it instead of immediately repeating the file-size stat.
        wal.set_max_page_count(pager.cached_page_count());

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

        let (mut runtime, runtime_lsn) = if freshly_initialized_empty_wal {
            // `create_with_vfs` just wrote the newly constructed empty catalog
            // root page. With no recovered WAL frames there is no persisted
            // runtime to discover, so avoid reader admission and decoding a
            // page whose contents are already known.
            (
                EngineRuntime::from_config(schema_cookie, &effective_config),
                0,
            )
        } else {
            let reader = wal.begin_reader_with_pager(&pager)?;
            let snapshot_lsn = reader.snapshot_lsn();
            let runtime_schema_cookie =
                Self::schema_cookie_at_storage_snapshot(&pager, &wal, snapshot_lsn)?;
            let runtime = EngineRuntime::load_from_storage_at_snapshot(
                &pager,
                &wal,
                runtime_schema_cookie,
                &effective_config,
                snapshot_lsn,
            )?;
            drop(reader);
            (runtime, snapshot_lsn)
        };
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        if !freshly_initialized_empty_wal
            && pager.header_snapshot()?.schema_cookie != runtime_schema_cookie
        {
            pager.set_schema_cookie(runtime_schema_cookie)?;
        }
        let audit_context = Arc::new(Mutex::new(crate::security::AuditContext::default()));
        runtime.set_audit_context_handle(Arc::clone(&audit_context));

        let tracing_state = crate::tracing::RuntimeTraceState::new(
            &effective_config.tracing,
            crate::tracing::next_connection_id(),
            crate::error::short_hex_sha256(&path.to_string_lossy()),
        );
        let tracing_arc = Arc::new(tracing_state);
        if tracing_arc.any_enabled() {
            runtime.set_tracing(Arc::clone(&tracing_arc));
        }

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
        if !freshly_initialized_empty_wal {
            db.backfill_paged_row_storage()?;
            db.refresh_named_snapshot_retention()?;
        }
        drop(open_guard);
        drop(open_lock);
        if let Some(canonical_path) = open_lock_key {
            prune_db_open_lock_registry(&canonical_path);
        }
        Ok(OpenWithVfsOutcome::Opened(db))
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
        let extension_execution_enabled = self.inner.config.extension_unsigned_development_mode
            || !self.inner.config.extension_trust_anchors.is_empty();
        if !extension_execution_enabled {
            if let Some(runtime) =
                self.try_resident_read_for_single_process_statement(statement, prepared)?
            {
                let result =
                    runtime.execute_read_statement(statement, params, self.inner.config.page_size);
                drop(runtime);
                return self.finalize_row_source_autocommit_statement(statement, result);
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

    fn prepare_resident_payload_offset_caches_for_wal_checkpoint(&self) -> Result<()> {
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let store = PagerReadStore::new(self)?;
        runtime.prepare_resident_payload_offset_caches(&store, self.config())?;
        Ok(())
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
                                let mut candidate =
                                    Vec::with_capacity(prepared_insert.columns.len());
                                for row_index in 0..row_count {
                                    build_params(row_index, &mut params)?;
                                    let affected = state
                                        .runtime
                                        .execute_prepared_simple_insert_positional_params_in_place_with_candidate(
                                            prepared_insert.as_ref(),
                                            &mut params,
                                            &mut candidate,
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

    fn execute_prepared_simple_indexed_projection_in_runtime(
        &self,
        runtime: &EngineRuntime,
        plan: &PreparedSimpleIndexedProjection,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let lookup_value = match &plan.lookup {
            PreparedSimpleIndexedProjectionLookup::RowId { value_source }
            | PreparedSimpleIndexedProjectionLookup::Index { value_source, .. } => {
                resolve_prepared_simple_value_for_fast_path(value_source, params)?
            }
        };
        if matches!(lookup_value, Value::Null) {
            return Ok(Some(QueryResult::with_rows(
                plan.column_names.to_vec(),
                Vec::new(),
            )));
        }

        let Some(row_source) = runtime.table_row_source(plan.table_name.as_str()) else {
            return Ok(None);
        };

        let mut rows = Vec::new();
        match &plan.lookup {
            PreparedSimpleIndexedProjectionLookup::RowId { .. } => {
                let Value::Int64(row_id) = lookup_value else {
                    return Ok(Some(QueryResult::with_rows(
                        plan.column_names.to_vec(),
                        Vec::new(),
                    )));
                };
                if let Some(stored_row) = row_source.row_by_id(row_id)? {
                    rows.push(QueryRow::new(
                        plan.projection_indexes
                            .iter()
                            .map(|index| stored_row.values()[*index].clone())
                            .collect(),
                    ));
                }
            }
            PreparedSimpleIndexedProjectionLookup::Index { index_name, .. } => {
                let Some(RuntimeIndex::Btree { keys, .. }) = runtime.index(index_name) else {
                    return Ok(None);
                };
                match keys.row_ids_for_value_set(&lookup_value)? {
                    RuntimeRowIdSet::Empty => {}
                    RuntimeRowIdSet::Single(row_id) => {
                        if let Some(stored_row) = row_source.row_by_id(row_id)? {
                            rows.push(QueryRow::new(
                                plan.projection_indexes
                                    .iter()
                                    .map(|index| stored_row.values()[*index].clone())
                                    .collect(),
                            ));
                        }
                    }
                    RuntimeRowIdSet::Contiguous { start, len } => {
                        rows.reserve(len);
                        for row_id in contiguous_row_ids(start, len) {
                            if let Some(stored_row) = row_source.row_by_id(row_id)? {
                                rows.push(QueryRow::new(
                                    plan.projection_indexes
                                        .iter()
                                        .map(|index| stored_row.values()[*index].clone())
                                        .collect(),
                                ));
                            }
                        }
                    }
                    RuntimeRowIdSet::Many(row_ids) => {
                        rows.reserve(row_ids.len());
                        for row_id in row_ids {
                            if let Some(stored_row) = row_source.row_by_id(*row_id)? {
                                rows.push(QueryRow::new(
                                    plan.projection_indexes
                                        .iter()
                                        .map(|index| stored_row.values()[*index].clone())
                                        .collect(),
                                ));
                            }
                        }
                    }
                    RuntimeRowIdSet::Owned(row_ids) => {
                        rows.reserve(row_ids.len());
                        for row_id in row_ids {
                            if let Some(stored_row) = row_source.row_by_id(row_id)? {
                                rows.push(QueryRow::new(
                                    plan.projection_indexes
                                        .iter()
                                        .map(|index| stored_row.values()[*index].clone())
                                        .collect(),
                                ));
                            }
                        }
                    }
                }
            }
        }

        Ok(Some(QueryResult::with_rows(
            plan.column_names.to_vec(),
            rows,
        )))
    }

    fn execute_prepared_read_statement(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if params.is_empty() {
            if let Some(result) =
                self.try_execute_prepared_simple_ordered_row_id_projection(prepared)?
            {
                return Ok(result);
            }
        }
        if let Some(result) =
            self.try_execute_prepared_simple_row_id_projection(prepared, params)?
        {
            return Ok(result);
        }
        if let Some(result) =
            self.try_execute_prepared_simple_indexed_projection(prepared, params)?
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
        if !self.inner.sql_txn_active.load(Ordering::Acquire) {
            self.validate_prepared_against_connection_state(prepared)?;
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
        self.validate_prepared_against_connection_state(prepared)?;
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
        if let Some(result) =
            self.try_execute_write_statement_in_active_sql_txn(sql, statement, params)?
        {
            return Ok(result);
        }
        let temp_only = {
            #[cfg(test)]
            EXECUTE_WRITE_BASE_TEMP_CLASSIFICATION_COUNT
                .with(|count| count.set(count.get().saturating_add(1)));
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.statement_is_temp_only(&runtime, statement)
        };
        // A transaction may have started while the live runtime was being
        // classified. Recheck before entering the autocommit path so that
        // this race retains the existing transaction-local semantics.
        if let Some(result) =
            self.try_execute_write_statement_in_active_sql_txn(sql, statement, params)?
        {
            return Ok(result);
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

    fn try_execute_write_statement_in_active_sql_txn(
        &self,
        sql: &str,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Ok(None);
        }
        let mut txn = self
            .inner
            .sql_txn
            .lock()
            .map_err(|_| DbError::internal("SQL transaction lock poisoned"))?;
        match &mut *txn {
            SqlTxnSlot::Shared(state) => self
                .execute_statement_in_state(sql, statement, params, state)
                .map(Some),
            SqlTxnSlot::Exclusive => Err(self.exclusive_sql_txn_error()),
            SqlTxnSlot::None => Ok(None),
        }
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

    fn can_use_autocommit_prepared_insert_fast_path(&self, _table_name: &str) -> Result<bool> {
        Ok(true)
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

    fn runtime_has_persistent_commit_work(&self, runtime: &EngineRuntime) -> Result<bool> {
        if !runtime.dirty_tables.is_empty() {
            return Ok(true);
        }
        Ok(self.inner.catalog.schema_cookie()? != runtime.catalog.schema_cookie)
    }

    fn runtime_has_stale_indexes(runtime: &EngineRuntime) -> bool {
        runtime.catalog.indexes.iter().any(|(name, index)| {
            let table_deferred = runtime
                .deferred_table_names()
                .any(|table_name| identifiers_equal(table_name, &index.table_name));
            !table_deferred && (!index.fresh || !runtime.indexes.contains_key(name))
        })
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

        let base_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let base_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
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
        let committed_lsn = match self.commit_if_latest(base_lsn, base_checkpoint_epoch) {
            Ok(lsn) => lsn,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return if matches!(&error, DbError::Transaction { message } if message.starts_with("transaction conflict: WAL advanced"))
                {
                    Ok(())
                } else {
                    Err(error)
                };
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
        {
            let store = PagerReadStore::new(self)?;
            if !runtime.has_checkpoint_compaction_candidates(&store, self.config())? {
                return Ok(());
            }
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
                simple_indexed_projection: bundle.simple_indexed_projection,
                simple_row_id_range_projection: bundle.simple_row_id_range_projection,
                simple_ordered_row_id_projection: bundle.simple_ordered_row_id_projection,
                simple_row_id_join_projection: bundle.simple_row_id_join_projection,
                simple_scalar_filtered_aggregate: bundle.simple_scalar_filtered_aggregate,
                prepared_insert: bundle.prepared_insert,
                prepared_update: bundle.prepared_update,
                prepared_delete: bundle.prepared_delete,
                read_only: bundle.read_only,
            });
        }
        if let Some(request) = parse_simple_row_id_range_delete_sql(&prepared_sql) {
            if let Some(prepared_delete) = runtime.prepare_simple_row_id_range_delete(
                &request.table_name,
                &request.column_name,
                request.low,
                request.high,
            )? {
                let statement = Arc::new(simple_row_id_range_delete_statement(&request));
                let bundle = PreparedPlanBundle {
                    statement: Arc::clone(&statement),
                    simple_row_id_projection: None,
                    simple_indexed_projection: None,
                    simple_row_id_range_projection: None,
                    simple_ordered_row_id_projection: None,
                    simple_row_id_join_projection: None,
                    simple_scalar_filtered_aggregate: None,
                    prepared_insert: None,
                    prepared_update: None,
                    prepared_delete: Some(Arc::new(prepared_delete)),
                    read_only: false,
                };
                if let Ok(mut cache) = self.inner.prepared_plan_cache.lock() {
                    cache.insert(
                        key,
                        bundle.clone(),
                        Self::prepared_plan_accounted_size(&bundle),
                    );
                }
                return Ok(PreparedStatement {
                    db: self.clone(),
                    schema_cookie: runtime.catalog.schema_cookie,
                    temp_schema_cookie: runtime.temp_schema_cookie,
                    statement,
                    prepared_sql: prepared_sql.clone(),
                    simple_row_id_projection: None,
                    simple_indexed_projection: None,
                    simple_row_id_range_projection: None,
                    simple_ordered_row_id_projection: None,
                    simple_row_id_join_projection: None,
                    simple_scalar_filtered_aggregate: None,
                    prepared_insert: None,
                    prepared_update: None,
                    prepared_delete: bundle.prepared_delete,
                    read_only: false,
                });
            }
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
        let simple_indexed_projection =
            Self::prepared_simple_indexed_projection(statement.as_ref(), runtime);
        let simple_row_id_range_projection =
            Self::prepared_simple_row_id_range_projection(&prepared_sql, runtime);
        let simple_ordered_row_id_projection =
            Self::prepared_simple_ordered_row_id_projection(statement.as_ref(), runtime);
        let simple_row_id_join_projection =
            Self::prepared_simple_row_id_join_projection(statement.as_ref(), runtime);
        let simple_scalar_filtered_aggregate =
            Self::prepared_simple_scalar_filtered_aggregate(statement.as_ref(), runtime);
        let bundle = PreparedPlanBundle {
            statement: Arc::clone(&statement),
            simple_row_id_projection,
            simple_indexed_projection,
            simple_row_id_range_projection,
            simple_ordered_row_id_projection,
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
            simple_indexed_projection: bundle.simple_indexed_projection,
            simple_row_id_range_projection: bundle.simple_row_id_range_projection,
            simple_ordered_row_id_projection: bundle.simple_ordered_row_id_projection,
            simple_row_id_join_projection: bundle.simple_row_id_join_projection,
            simple_scalar_filtered_aggregate: bundle.simple_scalar_filtered_aggregate,
            prepared_insert: bundle.prepared_insert,
            prepared_update: bundle.prepared_update,
            prepared_delete: bundle.prepared_delete,
            read_only,
        })
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
            prepared_insert_last_cache_key: None,
            prepared_insert_last_plan: None,
            prepared_insert_last_next_row_id: None,
            prepared_insert_candidate: Vec::new(),
        })
    }

    fn commit_exclusive_sql_txn(&self, mut state: ExclusiveSqlTxnState<'_>) -> Result<u64> {
        Self::flush_exclusive_prepared_insert_next_row_id(&mut state)?;
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
            let freed_bytes = state.runtime.redefer_all_persisted_paged_tables();
            self.release_freed_heap_after_paged_row_source_drop(freed_bytes);
        }
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(state);
        self.maybe_demote_wal_after_large_explicit_commit();
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
        let compacted_bytes = runtime.compact_dirty_resident_storage_after_transaction_commit();
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
            let freed_bytes = guard.redefer_all_persisted_paged_tables();
            self.release_freed_heap_after_paged_row_source_drop(freed_bytes);
        }
        self.release_freed_heap_after_runtime_compaction(compacted_bytes);
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(guard);
        self.maybe_demote_wal_after_large_explicit_commit();
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
            let _ = runtime.redefer_persisted_tables(&[table_name]);
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

        if let Some(source) = runtime.table_row_source(table_name) {
            return Ok(source.row_count());
        }

        let state = runtime.persisted_table_state(table_name);
        if let Some(state) = state {
            // The persisted state belongs to the runtime snapshot selected by
            // the caller. Writes keep its non-zero live-row count exact even
            // when a paged row source is re-deferred, so do not fault the
            // manifest back in merely to recount its chunks.
            if state.row_count != 0 {
                return Ok(state.row_count);
            }
            // A missing payload is an unambiguously empty table. A zero count
            // paired with a non-empty legacy/paged pointer remains ambiguous:
            // older catalogs did not persist the count unless ANALYZE stats
            // were present, so that case must fall through to storage.
            if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
                return Ok(0);
            }
        }

        if let Some(table) = runtime.catalog.table(table_name) {
            if let Some(stats) = runtime.catalog.table_stats.get(&table.name) {
                // Presence, rather than a non-zero value, distinguishes an
                // analyzed empty table from a legacy catalog whose row count
                // is unknown. Mutations invalidate these stats before the
                // runtime is persisted.
                return Ok(usize::try_from(stats.row_count.max(0)).unwrap_or(usize::MAX));
            }
        }

        let Some(state) = state else {
            return Ok(0);
        };

        let store = if let Some(lsn) = snapshot_lsn {
            PagerReadStore::with_snapshot_lsn(self, lsn)
        } else {
            PagerReadStore::new(self)?
        };
        if state.pointer.is_table_paged_manifest() {
            return read_persisted_table_row_count(&store, state);
        }

        let payload = read_overflow(&store, state.pointer)?;
        read_table_payload_live_row_count_from_bytes(&payload)
    }

    fn runtime_table_row_count_without_storage(
        &self,
        runtime: &EngineRuntime,
        table_name: &str,
    ) -> Result<Option<usize>> {
        if runtime.temp_table_schema(table_name).is_some() {
            return Ok(None);
        }

        if let Some(source) = runtime.table_row_source(table_name) {
            return Ok(Some(source.row_count()));
        }

        let state = runtime.persisted_table_state(table_name);
        if let Some(state) = state {
            if state.row_count != 0 {
                return Ok(Some(state.row_count));
            }
            if state.pointer.head_page_id == 0 || state.pointer.logical_len == 0 {
                return Ok(Some(0));
            }
        }

        if let Some(table) = runtime.catalog.table(table_name) {
            if let Some(stats) = runtime.catalog.table_stats.get(&table.name) {
                return Ok(Some(
                    usize::try_from(stats.row_count.max(0)).unwrap_or(usize::MAX),
                ));
            }
        }

        if state.is_none() {
            return Ok(Some(0));
        }

        Ok(None)
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

    fn observed_current_resident_snapshot_lsn(&self) -> Result<Option<u64>> {
        let Some(snapshot_lsn) = self.inner.wal.observed_current_snapshot_lsn()? else {
            return Ok(None);
        };
        if self.observed_current_runtime_is_current(snapshot_lsn) {
            return Ok(Some(snapshot_lsn));
        }
        self.try_preserve_observed_current_runtime_after_explicit_checkpoint(snapshot_lsn)?;
        if self.observed_current_runtime_is_current(snapshot_lsn) {
            return Ok(Some(snapshot_lsn));
        }
        Ok(None)
    }

    fn observed_current_resident_snapshot_still_valid(&self, snapshot_lsn: u64) -> Result<bool> {
        let Some(current_snapshot_lsn) = self.inner.wal.observed_current_snapshot_lsn()? else {
            return Ok(false);
        };
        Ok(current_snapshot_lsn == snapshot_lsn
            && self.observed_current_runtime_is_current(snapshot_lsn))
    }

    fn observed_current_runtime_is_current(&self, snapshot_lsn: u64) -> bool {
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        snapshot_lsn == last_runtime_lsn && latest_checkpoint_epoch == last_seen_checkpoint_epoch
    }

    fn try_preserve_observed_current_runtime_after_explicit_checkpoint(
        &self,
        snapshot_lsn: u64,
    ) -> Result<()> {
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
            self.inner.pager.refresh_from_disk(on_disk_header.clone())?;
        }
        self.inner
            .last_seen_checkpoint_epoch
            .store(latest_checkpoint_epoch, Ordering::Release);

        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let writer_last_commit_lsn = self.inner.writer_last_commit_lsn.load(Ordering::Acquire);
        let last_explicit_checkpoint_epoch = self
            .inner
            .last_explicit_checkpoint_epoch
            .load(Ordering::Acquire);
        if last_runtime_lsn > 0
            && writer_last_commit_lsn > 0
            && last_runtime_lsn >= writer_last_commit_lsn
            && snapshot_lsn == 0
            && last_explicit_checkpoint_epoch == latest_checkpoint_epoch
            && on_disk_header.last_checkpoint_lsn == last_runtime_lsn
            && on_disk_header.last_checkpoint_lsn >= writer_last_commit_lsn
        {
            self.inner
                .last_runtime_lsn
                .store(snapshot_lsn, Ordering::Release);
        }
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

    fn runtime_read_for_observed_current_resident_fast_read(
        &self,
        snapshot_lsn: u64,
    ) -> Result<Option<RwLockReadGuard<'_, EngineRuntime>>> {
        if !self.observed_current_runtime_is_current(snapshot_lsn) {
            return Ok(None);
        }
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        if Self::runtime_has_deferred_security_tables(&runtime)
            || runtime.security_rules_active()?
        {
            return Ok(None);
        }
        if !self.observed_current_runtime_is_current(snapshot_lsn) {
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

    fn try_resident_read_for_single_process_statement(
        &self,
        statement: &SqlStatement,
        prepared: Option<&PreparedStatement>,
    ) -> Result<Option<RwLockReadGuard<'_, EngineRuntime>>> {
        if !self.inner.config.defer_table_materialization {
            return Ok(None);
        }
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
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

    fn try_resident_read_for_prepared_table_statement(
        &self,
        prepared: &PreparedStatement,
        table_name: &str,
    ) -> Result<Option<RwLockReadGuard<'_, EngineRuntime>>> {
        if !self.inner.config.defer_table_materialization {
            return Ok(None);
        }
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.validate_prepared_against_runtime(Some(prepared), &runtime)?;
        if Self::runtime_has_deferred_security_tables(&runtime)
            || runtime.security_rules_active()?
        {
            return Ok(None);
        }
        if runtime
            .canonical_catalog_table_name(table_name)
            .is_some_and(|canonical| runtime.table_row_source(&canonical).is_some())
        {
            Ok(Some(runtime))
        } else {
            Ok(None)
        }
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

    fn load_simple_write_row_sources_and_child_indexes_at_latest_snapshot(
        &self,
        names: &[&str],
        child_index_targets: &[(&str, &str)],
    ) -> Result<()> {
        if !self.inner.config.defer_table_materialization {
            self.refresh_engine_from_storage()?;
            self.ensure_tables_loaded_at_snapshot(names, None)?;
            return Ok(());
        }

        let indexes_loaded = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            child_index_targets
                .iter()
                .all(|(_, index_name)| runtime.index(index_name).is_some())
        };
        if self.simple_write_row_sources_loaded_for_current_runtime(names)? && indexes_loaded {
            return Ok(());
        }

        let reader = self.inner.wal.begin_reader_with_pager(&self.inner.pager)?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        for (table_name, index_name) in child_index_targets {
            self.hydrate_deferred_runtime_index_at_snapshot(table_name, index_name, snapshot_lsn)?;
        }
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
        let freed_bytes = runtime.redefer_persisted_tables(names);
        drop(runtime);
        if release_heap_after_drop {
            self.release_freed_heap_after_paged_row_source_drop(freed_bytes);
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
        let freed_bytes = runtime.redefer_all_persisted_paged_tables();
        drop(runtime);
        self.release_freed_heap_after_paged_row_source_drop(freed_bytes);
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

    fn release_freed_heap_after_paged_row_source_drop(&self, freed_bytes: usize) {
        if self.inner.config.paged_row_storage
            && should_release_freed_paged_row_source_heap(freed_bytes)
        {
            self.release_freed_heap_if_configured();
        }
    }

    fn release_freed_heap_after_runtime_compaction(&self, freed_bytes: usize) {
        if freed_bytes >= RESIDENT_COMMIT_HEAP_RELEASE_THRESHOLD {
            self.release_freed_heap_if_configured();
        }
    }

    fn release_freed_heap_if_configured(&self) {
        if !self.inner.config.release_freed_memory_after_checkpoint {
            return;
        }
        #[cfg(test)]
        PAGED_ROW_SOURCE_HEAP_RELEASE_COUNT.with(|count| count.set(count.get().saturating_add(1)));
        crate::wal::platform::release_freed_heap();
    }

    fn maybe_demote_wal_after_large_explicit_commit(&self) {
        let threshold = self.inner.config.wal_checkpoint_threshold_bytes;
        if threshold == 0 {
            return;
        }
        if self.inner.wal.latest_snapshot() < threshold {
            return;
        }
        let target_bytes = threshold / 2;
        if target_bytes == 0 {
            return;
        }
        if matches!(
            self.inner
                .wal
                .demote_resident_versions_if_reader_free(usize::try_from(target_bytes).unwrap_or(usize::MAX)),
            Ok(demoted) if demoted > 0
        ) {
            self.release_freed_heap_if_configured();
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

    fn load_runtime_table_row_sources_and_child_indexes_at_snapshot(
        &self,
        runtime: &mut EngineRuntime,
        names: &[&str],
        child_index_targets: &[(&str, &str)],
        snapshot_lsn: u64,
    ) -> Result<()> {
        self.load_runtime_table_row_sources_at_snapshot(runtime, names, snapshot_lsn)?;
        for (_, index_name) in child_index_targets {
            if runtime.index(index_name).is_none() {
                runtime.rebuild_index(index_name, self.inner.config.page_size)?;
            }
        }
        Ok(())
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
                    let table_names = prepared_delete.required_row_source_table_names();
                    let child_index_targets = prepared_delete.child_index_hydration_targets();
                    self.load_runtime_table_row_sources_and_child_indexes_at_snapshot(
                        runtime,
                        &table_names,
                        &child_index_targets,
                        snapshot_lsn,
                    )?;
                    if runtime.can_reuse_prepared_simple_delete(&prepared_delete) {
                        let result = runtime.execute_prepared_simple_delete(
                            &prepared_delete,
                            params,
                            self.inner.config.page_size,
                        )?;
                        *persistent_changed |= !temp_only;
                        return Ok(result);
                    }
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
        Self::schema_cookie_at_storage_snapshot(&self.inner.pager, &self.inner.wal, snapshot_lsn)
    }

    fn schema_cookie_at_storage_snapshot(
        pager: &PagerHandle,
        wal: &WalHandle,
        snapshot_lsn: u64,
    ) -> Result<u32> {
        let mut bytes = [0_u8; storage::header::DB_HEADER_SIZE];
        if let Some(wal_page) =
            wal.read_page_at_snapshot(pager, page::HEADER_PAGE_ID, snapshot_lsn)?
        {
            bytes.copy_from_slice(&wal_page[..storage::header::DB_HEADER_SIZE]);
        } else {
            let page = pager.read_page(page::HEADER_PAGE_ID)?;
            bytes.copy_from_slice(&page[..storage::header::DB_HEADER_SIZE]);
        }
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

    fn validate_prepared_against_connection_state(
        &self,
        prepared: &PreparedStatement,
    ) -> Result<()> {
        let temp_schema_cookie = self
            .inner
            .temp_state
            .lock()
            .map_err(|_| DbError::internal("temp schema lock poisoned"))?
            .schema_cookie;
        self.validate_prepared_schema_cookie(
            prepared,
            self.inner.catalog.schema_cookie()?,
            temp_schema_cookie,
        )
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
        if let Some(prepared_update) = prepared.prepared_update.as_deref() {
            if let Some(result) = self.try_execute_prepared_update_in_runtime_state(
                prepared,
                prepared_update,
                params,
                &mut state.runtime,
                snapshot_lsn,
                &mut state.persistent_changed,
                &mut state.indexes_maybe_stale,
            )? {
                return Ok(result);
            }
        }
        if let Some(prepared_delete) = prepared.prepared_delete.as_deref() {
            if let Some(result) = self.try_execute_prepared_delete_in_runtime_state(
                prepared,
                prepared_delete,
                params,
                &mut state.runtime,
                snapshot_lsn,
                &mut state.persistent_changed,
                &mut state.indexes_maybe_stale,
            )? {
                return Ok(result);
            }
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
        Self::flush_exclusive_prepared_insert_next_row_id(state)?;
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
        if !prepared.read_only {
            if let Some(result) = self
                .try_execute_last_prepared_insert_in_exclusive_state_mut(prepared, params, state)?
            {
                return Ok(result);
            }
        }
        Self::flush_exclusive_prepared_insert_next_row_id(state)?;
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
            &mut state.prepared_insert_last_cache_key,
            &mut state.prepared_insert_last_plan,
            &mut state.prepared_insert_last_next_row_id,
            &mut state.prepared_insert_candidate,
        )? {
            return Ok(result);
        }
        if let Some(prepared_update) = prepared.prepared_update.as_deref() {
            if let Some(result) = self.try_execute_prepared_update_in_runtime_state(
                prepared,
                prepared_update,
                params,
                &mut state.runtime,
                snapshot_lsn,
                &mut state.persistent_changed,
                &mut state.indexes_maybe_stale,
            )? {
                return Ok(result);
            }
        }
        if let Some(prepared_delete) = prepared.prepared_delete.as_deref() {
            if let Some(result) = self.try_execute_prepared_delete_in_runtime_state(
                prepared,
                prepared_delete,
                params,
                &mut state.runtime,
                snapshot_lsn,
                &mut state.persistent_changed,
                &mut state.indexes_maybe_stale,
            )? {
                return Ok(result);
            }
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

    fn flush_exclusive_prepared_insert_next_row_id(
        state: &mut ExclusiveSqlTxnState<'_>,
    ) -> Result<()> {
        let Some(next_row_id) = state.prepared_insert_last_next_row_id.take() else {
            return Ok(());
        };
        let Some(prepared_insert) = state.prepared_insert_last_plan.as_ref() else {
            state.prepared_insert_last_cache_key = None;
            return Ok(());
        };
        let Some(table_name) = prepared_insert.catalog_table_name.as_deref() else {
            return Ok(());
        };
        let catalog = Arc::make_mut(&mut state.runtime.catalog);
        let table = catalog
            .tables
            .get_mut(table_name)
            .ok_or_else(|| DbError::sql(format!("unknown table {}", prepared_insert.table_name)))?;
        table.next_row_id = next_row_id;
        Ok(())
    }

    #[inline(always)]
    fn try_execute_last_prepared_insert_in_exclusive_state_mut(
        &self,
        prepared: &PreparedStatement,
        params: &mut [Value],
        state: &mut ExclusiveSqlTxnState<'_>,
    ) -> Result<Option<QueryResult>> {
        if state.indexes_maybe_stale {
            return Ok(None);
        }
        let Some(cache_key) = state.prepared_insert_last_cache_key else {
            return Ok(None);
        };
        if cache_key != Self::prepared_statement_cache_key(prepared) {
            return Ok(None);
        }
        let Some(insert_plan) = state.prepared_insert_last_plan.as_ref() else {
            return Ok(None);
        };
        let Some(cached_next_row_id) = state.prepared_insert_last_next_row_id.as_mut() else {
            return Ok(None);
        };

        let affected = state
            .runtime
            .execute_prepared_simple_insert_positional_params_in_place_with_cached_next_row_id(
                insert_plan.as_ref(),
                params,
                &mut state.prepared_insert_candidate,
                cached_next_row_id,
                self.inner.config.page_size,
            )?;
        if !state.persistent_changed {
            state.persistent_changed |= Self::prepared_insert_changes_persistent_table(
                &state.runtime,
                insert_plan.as_ref(),
            );
        }
        Ok(Some(QueryResult::with_affected_rows(affected)))
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
        Self::flush_exclusive_prepared_insert_next_row_id(state)?;
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
            prepared_insert_candidate: Vec::new(),
            prepared_insert_encoded_values: Vec::new(),
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

    fn exclusive_sql_txn_error(&self) -> DbError {
        DbError::transaction(
            "a SQL transaction handle is active on this database handle; use it until commit or rollback",
        )
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
    resolve_prepared_simple_value(source, params)
}

fn prepared_usize_literal(expr: &crate::sql::ast::Expr) -> Option<usize> {
    let crate::sql::ast::Expr::Literal(Value::Int64(value)) = expr else {
        return None;
    };
    usize::try_from((*value).max(0)).ok()
}

/// Evicts the shared WAL registry entry for an on-disk database path.
pub fn evict_shared_wal(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let vfs = VfsHandle::for_path(path);
    WalHandle::evict(&vfs, path)
}

#[cfg(test)]
mod tests;
