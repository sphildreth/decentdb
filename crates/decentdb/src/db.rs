//! Stable database owner and bootstrap lifecycle entry points.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};

use crate::catalog::{
    identifiers_equal, CatalogHandle, CheckConstraint, ColumnSchema, ForeignKeyAction,
    ForeignKeyConstraint, IndexColumn, IndexKind, IndexSchema, TableSchema, TriggerEvent,
    TriggerKind, TriggerSchema, ViewSchema,
};
use crate::config::DbConfig;
use crate::error::{DbError, Result};
use crate::exec::dml::{
    PreparedInsertValueSource, PreparedSimpleDelete, PreparedSimpleInsert, PreparedSimpleUpdate,
};
use crate::exec::{
    decode_paged_table_manifest_payload, read_table_payload_row_count_from_bytes,
    statement_is_read_only, BulkLoadOptions, EngineRuntime, QueryResult, QueryRow, RuntimeIndex,
    TableData,
};
use crate::metadata::{
    CheckConstraintInfo, ColumnInfo, ForeignKeyInfo, HeaderInfo, IndexInfo, IndexVerification,
    SchemaColumnInfo, SchemaIndexInfo, SchemaSnapshot, SchemaTableInfo, SchemaTriggerInfo,
    SchemaViewInfo, StorageInfo, TableInfo, TriggerInfo, ViewInfo,
};
use crate::record::overflow::read_overflow;
use crate::record::value::Value;
use crate::sql::ast::Statement as SqlStatement;
use crate::sql::parser::{parse_sql_statement, rewrite_legacy_trigger_body};
use crate::storage::freelist::{decode_freelist_next, encode_freelist_page};
use crate::storage::page::{self, PageId, PageStore};
use crate::storage::{self, DatabaseHeader, PagerHandle};
use crate::vfs::faulty::{self, FailAction, Failpoint};
use crate::vfs::{is_memory_path, write_all_at, FileKind, OpenMode, VfsHandle};
use crate::wal::reader_registry::ReaderGuard;
use crate::wal::savepoint::StatementSavepoint;
use crate::wal::WalHandle;

/// Stable engine owner used across later storage, SQL, and FFI slices.
#[derive(Clone, Debug)]
pub struct Db {
    inner: Arc<DbInner>,
}

#[derive(Debug)]
struct PagerReadStore<'a> {
    db: &'a Db,
    snapshot_token: u64,
}

impl<'a> PagerReadStore<'a> {
    fn new(db: &'a Db) -> Result<Self> {
        Ok(Self {
            db,
            snapshot_token: db.hold_snapshot()?,
        })
    }
}

impl Drop for PagerReadStore<'_> {
    fn drop(&mut self) {
        let _ = self.db.release_snapshot(self.snapshot_token);
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
        self.db.read_page_for_snapshot(self.snapshot_token, page_id)
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
    prepared_insert: Option<Arc<PreparedSimpleInsert>>,
    prepared_update: Option<Arc<PreparedSimpleUpdate>>,
    prepared_delete: Option<Arc<PreparedSimpleDelete>>,
    read_only: bool,
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

    /// Executes the prepared statement inside an active [`SqlTransaction`].
    pub fn execute_in(
        &self,
        txn: &mut SqlTransaction<'_>,
        params: &[Value],
    ) -> Result<QueryResult> {
        txn.execute_prepared(self, params)
    }
}

impl SqlTransaction<'_> {
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
    _vfs: VfsHandle,
    pager: PagerHandle,
    wal: WalHandle,
    catalog: CatalogHandle,
    engine: RwLock<EngineRuntime>,
    last_runtime_lsn: AtomicU64,
    writer_last_commit_lsn: AtomicU64,
    last_seen_checkpoint_epoch: AtomicU64,
    sql_write_lock: Mutex<()>,
    sql_txn: Mutex<SqlTxnSlot>,
    sql_txn_active: AtomicBool,
    write_txn: Mutex<WriteTxn>,
    temp_state: Mutex<TempSchemaState>,
    statement_cache: Mutex<StatementCache>,
    prepared_insert_cache: Mutex<PreparedInsertCache>,
    held_snapshots: Mutex<HashMap<u64, ReaderGuard>>,
}

impl Drop for DbInner {
    fn drop(&mut self) {
        if self.wal.latest_snapshot() == 0 {
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
        let _ = self
            .wal
            .checkpoint(&self.pager, self.config.checkpoint_timeout_sec);
    }
}

#[derive(Debug, Default)]
struct WriteTxn {
    active: bool,
    staged_pages: BTreeMap<PageId, Vec<u8>>,
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
    snapshot_reader: ReaderGuard,
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

impl ExclusiveSqlTxnState<'_> {
    fn snapshot_lsn(&self) -> u64 {
        self.snapshot_reader.snapshot_lsn()
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
            return Ok(Arc::clone(statement));
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
        config.validate_for_create()?;
        let vfs = VfsHandle::for_path(path);
        let open_mode = if vfs.is_memory() {
            OpenMode::OpenOrCreate
        } else {
            OpenMode::CreateNew
        };
        let file = vfs.open(path, open_mode, FileKind::Database)?;
        let header = DatabaseHeader::new(config.page_size);
        storage::write_database_bootstrap_vfs(file.as_ref(), &header)?;
        file.sync_metadata()?;

        Self::open_with_vfs(path.to_path_buf(), config, vfs)
    }

    /// Opens an existing database file and validates its fixed header.
    pub fn open(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path);
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
        Self::open_with_vfs(path.to_path_buf(), config, vfs)
    }

    /// Opens an existing database or creates a new one when the path does not
    /// yet exist.
    pub fn open_or_create(path: impl AsRef<Path>, config: DbConfig) -> Result<Self> {
        let path = path.as_ref();
        let vfs = VfsHandle::for_path(path);
        if vfs.is_memory() || vfs.file_exists(path)? {
            Self::open(path, config)
        } else {
            Self::create(path, config)
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

        self.checkpoint()?;

        let vfs = VfsHandle::for_path(dest);
        if vfs.file_exists(dest)? {
            return Err(DbError::io(
                format!("destination {} already exists", dest.display()),
                std::io::Error::new(std::io::ErrorKind::AlreadyExists, "destination exists"),
            ));
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

    /// Begins a single-connection write transaction.
    pub fn begin_write(&self) -> Result<()> {
        let mut txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if txn.active {
            return Err(DbError::transaction("write transaction is already active"));
        }
        txn.active = true;
        txn.staged_pages.clear();
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
            let max_page_id = txn
                .staged_pages
                .last_key_value()
                .map_or(0, |(page_id, _)| *page_id);
            (max_page_id, std::mem::take(&mut txn.staged_pages))
        };

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
            let max_page_id = txn
                .staged_pages
                .last_key_value()
                .map_or(0, |(page_id, _)| *page_id);
            (max_page_id, std::mem::take(&mut txn.staged_pages))
        };

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
        Ok(())
    }

    fn write_txn_visible_page(&self, txn: &WriteTxn, page_id: PageId) -> Result<Arc<[u8]>> {
        if let Some(staged) = txn.staged_pages.get(&page_id).cloned() {
            return Ok(Arc::from(staged));
        }

        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
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
        let reader = self.inner.wal.begin_reader()?;
        self.read_page_at_snapshot_lsn(page_id, reader.snapshot_lsn())
    }

    pub(crate) fn read_page_at_snapshot_lsn(
        &self,
        page_id: u32,
        snapshot_lsn: u64,
    ) -> Result<Arc<[u8]>> {
        page::validate_page_id(page_id)?;
        if let Some(staged) = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?
            .staged_pages
            .get(&page_id)
            .cloned()
        {
            return Ok(Arc::from(staged));
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
        self.inner
            .wal
            .checkpoint(&self.inner.pager, self.inner.config.checkpoint_timeout_sec)
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

    /// Executes one or more semicolon-delimited SQL statements with `$n` parameters.
    pub fn execute_batch_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<QueryResult>> {
        let mut results = Vec::new();
        for statement_sql in split_sql_batch(sql) {
            let trimmed = statement_sql.trim();
            if trimmed.is_empty() {
                continue;
            }

            if let Some(control) = parse_transaction_control(trimmed) {
                match control {
                    TransactionControl::Begin => self.begin_transaction()?,
                    TransactionControl::Commit => {
                        self.commit_transaction()?;
                    }
                    TransactionControl::Rollback => self.rollback_transaction()?,
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

            let statement = self.parsed_statement(trimmed)?;
            let result = if statement_is_read_only(&statement) {
                self.execute_read_statement(&statement, params)?
            } else {
                self.execute_write_statement(trimmed, &statement, params)?
            };
            results.push(result);
        }
        Ok(results)
    }

    /// Prepares a single SQL statement for repeated execution.
    ///
    /// Prepared statements are bound to the current schema cookie. If the schema
    /// changes, the handle must be recreated before it can be executed again.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
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
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        if self.inner.config.defer_table_materialization {
            let reader = self.inner.wal.begin_reader()?;
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
            self.redefer_persisted_tables(&deferred_refs)?;
            if options.checkpoint_on_complete {
                self.checkpoint()?;
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
            self.checkpoint()?;
        }
        Ok(inserted)
    }

    /// Rebuilds a single named index from the persisted table state.
    pub fn rebuild_index(&self, name: &str) -> Result<()> {
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        if self.inner.config.defer_table_materialization {
            let reader = self.inner.wal.begin_reader()?;
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
            self.redefer_persisted_tables(&[table_name.as_str()])?;
            return Ok(());
        }
        self.refresh_and_ensure_all_tables_loaded()?;
        let mut working = self.engine_snapshot()?;
        working.rebuild_index(name, self.inner.config.page_size)?;
        self.persist_runtime(working).map(|_| ())
    }

    /// Rebuilds all indexes from the persisted table state.
    pub fn rebuild_indexes(&self) -> Result<()> {
        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        if self.inner.config.defer_table_materialization {
            let reader = self.inner.wal.begin_reader()?;
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
            self.redefer_persisted_tables(&table_refs)?;
            return Ok(());
        }
        self.refresh_and_ensure_all_tables_loaded()?;
        let mut working = self.engine_snapshot()?;
        working.rebuild_indexes(self.inner.config.page_size)?;
        self.persist_runtime(working).map(|_| ())
    }

    /// Holds a snapshot open until `release_snapshot` is called.
    pub fn hold_snapshot(&self) -> Result<u64> {
        let guard = self.inner.wal.begin_reader()?;
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
            tables.push(table_info(
                table,
                self.runtime_table_row_count(&runtime, &table.name)?,
            ));
        }
        for table in runtime.temp_tables.values() {
            tables.push(table_info(
                table,
                self.runtime_table_row_count(&runtime, &table.name)?,
            ));
        }
        Ok(tables)
    }

    /// Returns a single table definition by name.
    pub fn describe_table(&self, name: &str) -> Result<TableInfo> {
        let runtime = self.runtime_for_metadata_inspection()?;
        if runtime.temp_views.contains_key(name) && !runtime.temp_tables.contains_key(name) {
            return Err(DbError::sql(format!("unknown table {name}")));
        }
        let (table, row_count) = if let Some(table) = runtime.temp_tables.get(name) {
            (table, self.runtime_table_row_count(&runtime, &table.name)?)
        } else {
            let table = runtime
                .catalog
                .tables
                .get(name)
                .ok_or_else(|| DbError::sql(format!("unknown table {name}")))?;
            (table, self.runtime_table_row_count(&runtime, &table.name)?)
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

    fn open_with_vfs(path: PathBuf, config: DbConfig, vfs: VfsHandle) -> Result<Self> {
        let file = vfs.open(
            &path,
            if vfs.is_memory() {
                OpenMode::OpenOrCreate
            } else {
                OpenMode::OpenExisting
            },
            FileKind::Database,
        )?;
        let header = storage::read_database_header_vfs(file.as_ref())?;
        let mut effective_config = config;
        effective_config.page_size = header.page_size;
        let schema_cookie = header.schema_cookie;

        let pager = PagerHandle::open(Arc::clone(&file), header, effective_config.cache_size_mb)?;
        let wal = WalHandle::acquire(&vfs, &path, &effective_config, &pager)?;
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
            if wal_size > on_open_threshold_bytes {
                // Best-effort: a checkpoint failure here is not fatal,
                // because the runtime load below will still succeed
                // against the existing WAL state. Surface it as a
                // warning-shaped log so embedders can investigate.
                if let Err(_e) = wal.checkpoint(&pager, effective_config.checkpoint_timeout_sec) {}
            }
        }

        let (runtime, runtime_lsn) =
            EngineRuntime::load_from_storage(&pager, &wal, schema_cookie, &effective_config)?;
        let catalog = CatalogHandle::new(runtime.catalog.as_ref().clone());
        let last_seen_checkpoint_epoch = wal.checkpoint_epoch();

        let db = Self {
            inner: Arc::new(DbInner {
                path,
                config: effective_config,
                _vfs: vfs,
                pager,
                wal,
                catalog,
                engine: RwLock::new(runtime),
                last_runtime_lsn: AtomicU64::new(runtime_lsn),
                writer_last_commit_lsn: AtomicU64::new(0),
                last_seen_checkpoint_epoch: AtomicU64::new(last_seen_checkpoint_epoch),
                sql_write_lock: Mutex::new(()),
                sql_txn: Mutex::new(SqlTxnSlot::None),
                sql_txn_active: AtomicBool::new(false),
                write_txn: Mutex::new(WriteTxn::default()),
                temp_state: Mutex::new(TempSchemaState::default()),
                statement_cache: Mutex::new(StatementCache::default()),
                prepared_insert_cache: Mutex::new(PreparedInsertCache::default()),
                held_snapshots: Mutex::new(HashMap::new()),
            }),
        };
        db.backfill_missing_persistent_pk_indexes()?;
        db.backfill_paged_row_storage()?;
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

    pub fn schema_cookie(&self) -> Result<u32> {
        self.inner.catalog.schema_cookie()
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
            if self.statement_is_temp_only(&runtime, statement) {
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

        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        if let SqlStatement::Query(query) = statement {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.validate_prepared_against_runtime(prepared, &runtime)?;
            if let Some(result) = runtime.try_execute_simple_deferred_count_query(
                query,
                &self.inner.pager,
                &self.inner.wal,
                snapshot_lsn,
            )? {
                return Ok(result);
            }
            if let Some(result) = runtime.try_execute_simple_deferred_min_max_query(
                query,
                &self.inner.pager,
                &self.inner.wal,
                snapshot_lsn,
            )? {
                return Ok(result);
            }
            if let Some(result) = runtime.try_execute_simple_deferred_indexed_projection_query(
                query,
                params,
                &self.inner.pager,
                &self.inner.wal,
                snapshot_lsn,
                self.inner.config.persistent_pk_index,
            )? {
                return Ok(result);
            }
            if let Some(result) = runtime.try_execute_simple_deferred_paged_query(
                query,
                params,
                &self.inner.pager,
                &self.inner.wal,
                snapshot_lsn,
            )? {
                return Ok(result);
            }
            if let Some(result) = self
                .try_execute_simple_indexed_join_projection_query_at_snapshot(
                    &runtime,
                    statement,
                    query,
                    params,
                    snapshot_lsn,
                )?
            {
                return Ok(result);
            }
            if let Some(result) = self.try_execute_query_with_row_sources_at_snapshot(
                &runtime,
                statement,
                params,
                snapshot_lsn,
                false,
            )? {
                return Ok(result);
            }
        }

        let targeted_ok =
            self.ensure_tables_loaded_for_statement_at_snapshot(statement, Some(snapshot_lsn))?;
        if !targeted_ok {
            self.ensure_all_tables_loaded_at_snapshot(Some(snapshot_lsn))?;
        }
        drop(reader);

        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        self.validate_prepared_against_runtime(prepared, &runtime)?;
        runtime.execute_read_statement(statement, params, self.inner.config.page_size)
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
            PragmaCommand::Query(PragmaName::PageSize) => Ok(QueryResult::with_rows(
                vec!["page_size".to_string()],
                vec![QueryRow::new(vec![Value::Int64(i64::from(
                    self.inner.config.page_size,
                ))])],
            )),
            PragmaCommand::Query(PragmaName::CacheSize) => Ok(QueryResult::with_rows(
                vec!["cache_size".to_string()],
                vec![QueryRow::new(vec![Value::Int64(cache_size_pages(
                    &self.inner.config,
                ))])],
            )),
            PragmaCommand::Query(PragmaName::DatabaseList) => {
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
            PragmaCommand::Query(PragmaName::TableInfo) => Err(DbError::sql(
                "PRAGMA table_info(table_name) requires a table name argument",
            )),
            PragmaCommand::TableInfo(table_name) => {
                let info = self.describe_table(&table_name)?;
                let rows = info
                    .columns
                    .into_iter()
                    .enumerate()
                    .map(|(cid, column)| {
                        QueryRow::new(vec![
                            Value::Int64(i64::try_from(cid).unwrap_or(i64::MAX)),
                            Value::Text(column.name),
                            Value::Text(column.column_type),
                            Value::Int64(if column.nullable { 0 } else { 1 }),
                            column.default_sql.map_or(Value::Null, Value::Text),
                            Value::Int64(if column.primary_key { 1 } else { 0 }),
                        ])
                    })
                    .collect();
                Ok(QueryResult::with_rows(
                    vec![
                        "cid".to_string(),
                        "name".to_string(),
                        "type".to_string(),
                        "notnull".to_string(),
                        "dflt_value".to_string(),
                        "pk".to_string(),
                    ],
                    rows,
                ))
            }
            PragmaCommand::Query(PragmaName::IntegrityCheck) => {
                let (mut runtime, snapshot_lsn) =
                    self.runtime_for_targeted_row_source_inspection()?;
                runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
                let mut errors = Vec::new();
                let table_names = runtime.catalog.tables.keys().cloned().collect::<Vec<_>>();
                for table_name in table_names {
                    self.ensure_inspection_table_row_source(
                        &mut runtime,
                        &table_name,
                        snapshot_lsn,
                    )?;
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
                    self.redefer_inspection_table_row_source(
                        &mut runtime,
                        &table_name,
                        snapshot_lsn,
                    );
                }
                for index in runtime.catalog.indexes.values() {
                    if runtime.catalog.table(&index.table_name).is_none() {
                        errors.push(format!(
                            "index {} references missing table {}",
                            index.name, index.table_name
                        ));
                    }
                    if !runtime.indexes.contains_key(&index.name) {
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
            PragmaCommand::Set(name, value) => self.execute_pragma_set(name, value),
        }
    }

    fn execute_pragma_set(&self, name: PragmaName, value: i64) -> Result<QueryResult> {
        match name {
            PragmaName::PageSize => {
                if value == i64::from(self.inner.config.page_size) {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA page_size cannot be changed on an open database; reopen with DbConfig::page_size",
                    ))
                }
            }
            PragmaName::CacheSize => {
                if value == cache_size_pages(&self.inner.config) {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA cache_size cannot be changed on an open connection; reopen with DbConfig::cache_size_mb",
                    ))
                }
            }
            PragmaName::IntegrityCheck | PragmaName::DatabaseList | PragmaName::TableInfo => {
                Err(DbError::sql(format!(
                    "PRAGMA {} does not support assignment",
                    pragma_name_sql(name)
                )))
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
        if prepared_insert.value_sources.len() != param_count
            || prepared_insert.columns.len() != param_count
        {
            return false;
        }

        prepared_insert
            .value_sources
            .iter()
            .enumerate()
            .all(|(index, source)| {
                matches!(
                    source,
                    PreparedInsertValueSource::Parameter(position) if *position == index + 1
                )
            })
    }

    fn prepared_statement_cache_key(prepared: &PreparedStatement) -> usize {
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

        let table_names =
            self.insert_dependency_table_names(runtime, &prepared_insert.table_name)?;
        let table_refs = table_names.iter().map(String::as_str).collect::<Vec<_>>();
        self.load_runtime_table_row_sources_at_snapshot(runtime, &table_refs, snapshot_lsn)?;

        let cache_key = Self::prepared_statement_cache_key(prepared);
        if let Some(plan) = prepared_insert_runtime_cache.get(&cache_key) {
            if runtime.can_reuse_prepared_simple_insert(plan) {
                return Ok(Some(Arc::clone(plan)));
            }
            prepared_insert_runtime_cache.remove(&cache_key);
        }

        let needs_refresh =
            prepared_insert.use_generic_validation || prepared_insert.use_generic_index_updates;
        if !needs_refresh && runtime.can_reuse_prepared_simple_insert(prepared_insert) {
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

    fn execute_prepared_read_statement(
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

        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
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

        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
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
            self.redefer_persisted_tables(&table_refs)?;
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
        if !runtime.can_reuse_prepared_simple_insert(prepared) {
            drop(runtime);
            let result = self.execute_autocommit_in_place(|runtime| {
                runtime.execute_prepared_simple_insert(
                    prepared,
                    params,
                    self.inner.config.page_size,
                )
            })?;
            self.redefer_persisted_tables(&table_refs)?;
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
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.redefer_persisted_tables(&table_refs)?;
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
        if !runtime.can_reuse_prepared_simple_update(prepared_update) {
            drop(runtime);
            let result = self.execute_autocommit_in_place(|runtime| {
                runtime.execute_prepared_simple_update(
                    prepared_update,
                    params,
                    self.inner.config.page_size,
                )
            })?;
            self.redefer_persisted_tables(&[prepared_update.table_name.as_str()])?;
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
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
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
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.redefer_persisted_tables(&[prepared_update.table_name.as_str()])?;
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
        if !runtime.can_reuse_prepared_simple_delete(prepared_delete) {
            drop(runtime);
            let result = self.execute_autocommit_in_place(|runtime| {
                runtime.execute_prepared_simple_delete(
                    prepared_delete,
                    params,
                    self.inner.config.page_size,
                )
            })?;
            self.redefer_persisted_tables(&table_names)?;
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
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
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
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(runtime);
        self.redefer_persisted_tables(&table_names)?;
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
        let table_names = {
            self.refresh_engine_from_storage()?;
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.insert_dependency_table_names(&runtime, &prepared_insert.table_name)?
        };
        let table_refs = table_names.iter().map(String::as_str).collect::<Vec<_>>();
        self.load_simple_write_row_sources_at_latest_snapshot(&table_refs)?;
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
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
        self.redefer_persisted_tables(&table_refs)?;
        Ok(Some(result))
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
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
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
        self.redefer_persisted_tables(&[prepared_update.table_name.as_str()])?;
        Ok(Some(result))
    }

    fn try_execute_autocommit_prepared_delete_in_place(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_delete: &PreparedSimpleDelete,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
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
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
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
        self.redefer_persisted_tables(&table_names)?;
        Ok(Some(result))
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
        let result = match apply(&mut runtime) {
            Ok(result) => result,
            Err(error) => {
                self.restore_runtime_from_storage(&mut runtime)?;
                return Err(error);
            }
        };
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
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
        Ok(result)
    }

    fn backfill_missing_persistent_pk_indexes(&self) -> Result<()> {
        if !self.inner.config.persistent_pk_index {
            return Ok(());
        }

        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let needs_backfill = runtime.catalog.tables.iter().any(|(table_name, table)| {
            table.pk_index_root.is_none()
                && runtime
                    .persisted_tables
                    .get(table_name)
                    .is_some_and(|state| state.pointer.head_page_id != 0)
        });
        if !needs_backfill {
            return Ok(());
        }

        self.begin_write()?;
        let changed = match runtime.backfill_missing_persistent_pk_indexes(self) {
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

    fn backfill_paged_row_storage(&self) -> Result<()> {
        if !self.inner.config.paged_row_storage {
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
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        let mut snapshot = runtime.clone();
        self.apply_temp_state_to_runtime(&mut snapshot)?;
        snapshot.rebuild_stale_indexes(self.inner.config.page_size)?;
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
        self.inner
            .temp_state
            .lock()
            .map_err(|_| DbError::internal("temp schema lock poisoned"))?
            .update_from_runtime(runtime);
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
        self.inner
            .statement_cache
            .lock()
            .map_err(|_| DbError::internal("statement cache lock poisoned"))?
            .get_or_parse(sql)
    }

    fn prepare_with_runtime(
        &self,
        sql: &str,
        runtime: &EngineRuntime,
    ) -> Result<PreparedStatement> {
        let prepared_sql = prepared_statement_sql(sql)?;
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
        Ok(PreparedStatement {
            db: self.clone(),
            schema_cookie: runtime.catalog.schema_cookie,
            temp_schema_cookie: runtime.temp_schema_cookie,
            statement: Arc::clone(&statement),
            prepared_insert,
            prepared_update,
            prepared_delete,
            read_only,
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

    fn persist_runtime(&self, runtime: EngineRuntime) -> Result<u64> {
        self.persist_runtime_if_latest(runtime, None, true)
    }

    fn build_exclusive_sql_txn_state(&self) -> Result<ExclusiveSqlTxnState<'_>> {
        let (snapshot_reader, current_lsn, current_epoch) = self.begin_sql_snapshot()?;
        let runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        Ok(ExclusiveSqlTxnState {
            runtime,
            snapshot_reader,
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
        self.begin_write()?;
        if let Err(error) = state.runtime.persist_to_db(self) {
            let _ = self.rollback();
            self.restore_runtime_from_storage(&mut state.runtime)?;
            return Err(error);
        }
        let committed_lsn = match self.commit_if_latest(state.base_lsn, state.base_checkpoint_epoch)
        {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.rollback();
                self.restore_runtime_from_storage(&mut state.runtime)?;
                return Err(error);
            }
        };
        if self.inner.catalog.schema_cookie()? != runtime_schema_cookie {
            self.inner
                .catalog
                .replace(state.runtime.catalog.as_ref().clone())?;
        }
        self.sync_temp_state_from_runtime(&state.runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
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
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        self.inner
            .writer_last_commit_lsn
            .store(committed_lsn, Ordering::Release);
        drop(guard);

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
        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        drop(reader);
        Ok((self.engine_snapshot()?, Some(snapshot_lsn)))
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

    fn runtime_table_row_count(&self, runtime: &EngineRuntime, table_name: &str) -> Result<usize> {
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
            return Ok(source.rows().count());
        }

        let Some(state) = runtime.persisted_table_state(table_name) else {
            return Ok(0);
        };
        if state.row_count != 0 || state.pointer.head_page_id == 0 {
            return Ok(state.row_count);
        }

        let store = PagerReadStore::new(self)?;
        let payload = read_overflow(&store, state.pointer)?;
        if state.pointer.is_table_paged_manifest() {
            let manifest = decode_paged_table_manifest_payload(&payload)?;
            Ok(manifest.chunks.iter().map(|chunk| chunk.row_count).sum())
        } else {
            read_table_payload_row_count_from_bytes(&payload)
        }
    }

    fn runtime_for_prepare(&self) -> Result<EngineRuntime> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            return Ok(runtime);
        }
        self.refresh_engine_from_storage()?;
        // ADR 0143 Phase B: prepare() only needs catalog/schema metadata
        // to plan a statement. Skip the eager all-tables materialization
        // so applications that prepare a large number of statements at
        // startup don't fault every persisted table into memory just to
        // get a `PreparedStatement` handle. Row data is loaded on first
        // execution by the read/write paths.
        self.engine_snapshot()
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
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        if latest_checkpoint_epoch != last_seen_checkpoint_epoch {
            let cached_header = self.inner.pager.header_snapshot()?;
            let on_disk_header = self.inner.pager.header_from_disk()?;
            if on_disk_header.last_checkpoint_lsn != cached_header.last_checkpoint_lsn {
                self.inner.pager.refresh_from_disk(on_disk_header)?;
            }
            self.inner
                .last_seen_checkpoint_epoch
                .store(latest_checkpoint_epoch, Ordering::Release);
        }
        if snapshot_lsn == last_runtime_lsn && latest_checkpoint_epoch == last_seen_checkpoint_epoch
        {
            return Ok(());
        }

        let writer_last_commit_lsn = self.inner.writer_last_commit_lsn.load(Ordering::Acquire);
        if last_runtime_lsn > 0
            && writer_last_commit_lsn > 0
            && snapshot_lsn <= writer_last_commit_lsn
        {
            self.inner
                .last_runtime_lsn
                .store(snapshot_lsn, Ordering::Release);
            self.inner
                .last_seen_checkpoint_epoch
                .store(latest_checkpoint_epoch, Ordering::Release);
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
        let latest_lsn = self.inner.wal.latest_snapshot();
        let latest_checkpoint_epoch = self.inner.wal.checkpoint_epoch();
        let last_runtime_lsn = self.inner.last_runtime_lsn.load(Ordering::Acquire);
        let last_seen_checkpoint_epoch = self
            .inner
            .last_seen_checkpoint_epoch
            .load(Ordering::Acquire);

        if latest_lsn <= last_runtime_lsn && latest_checkpoint_epoch == last_seen_checkpoint_epoch {
            return Ok(());
        }

        if latest_checkpoint_epoch != last_seen_checkpoint_epoch {
            let cached_header = self.inner.pager.header_snapshot()?;
            let on_disk_header = self.inner.pager.header_from_disk()?;
            if on_disk_header.last_checkpoint_lsn != cached_header.last_checkpoint_lsn {
                self.inner.pager.refresh_from_disk(on_disk_header)?;
            }
            self.inner
                .last_seen_checkpoint_epoch
                .store(latest_checkpoint_epoch, Ordering::Release);
        }

        let writer_last_commit_lsn = self.inner.writer_last_commit_lsn.load(Ordering::Acquire);
        if last_runtime_lsn > 0
            && writer_last_commit_lsn > 0
            && latest_lsn <= writer_last_commit_lsn
        {
            // A checkpoint can legally fold this handle's last committed WAL
            // history back into the database file and reset the live WAL end
            // (often to 0 after truncation). The in-memory runtime is still
            // current, but future OCC writes must compare against the new live
            // WAL end rather than the pre-checkpoint commit LSN.
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

        let reader = self.inner.wal.begin_reader()?;
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

        let reader = self.inner.wal.begin_reader()?;
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
        if let Some(snapshot_lsn) = snapshot_lsn {
            runtime.load_deferred_tables_filtered_at_snapshot(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                &filter,
                snapshot_lsn,
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

    fn load_simple_write_row_sources_at_latest_snapshot(&self, names: &[&str]) -> Result<()> {
        if !self.inner.config.defer_table_materialization {
            self.refresh_engine_from_storage()?;
            self.ensure_tables_loaded_at_snapshot(names, None)?;
            return Ok(());
        }

        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
        self.refresh_engine_from_snapshot(snapshot_lsn)?;
        self.ensure_table_row_sources_loaded_at_snapshot(names, snapshot_lsn)?;
        drop(reader);
        Ok(())
    }

    fn load_statement_row_sources_at_latest_snapshot(
        &self,
        statement: &SqlStatement,
    ) -> Result<bool> {
        if !self.inner.config.defer_table_materialization {
            return self.ensure_tables_loaded_for_statement_at_snapshot(statement, None);
        }

        let reader = self.inner.wal.begin_reader()?;
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

        let reader = self.inner.wal.begin_reader()?;
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
        if !self.inner.config.defer_table_materialization || names.is_empty() {
            return Ok(());
        }
        let mut runtime = self
            .inner
            .engine
            .write()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        runtime.redefer_persisted_tables(names);
        Ok(())
    }

    fn redefer_statement_tables(&self, statement: &SqlStatement) -> Result<()> {
        if !self.inner.config.defer_table_materialization {
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
        self.redefer_persisted_tables(&name_refs)
    }

    fn finalize_row_source_autocommit_statement(
        &self,
        statement: &SqlStatement,
        result: Result<QueryResult>,
    ) -> Result<QueryResult> {
        let redefer_result = self.redefer_statement_tables(statement);
        match (result, redefer_result) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(_)) => Err(error),
        }
    }

    fn begin_sql_snapshot(&self) -> Result<(ReaderGuard, u64, u64)> {
        let reader = self.inner.wal.begin_reader()?;
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
        Ok(Some(working.execute_read_statement(
            statement,
            params,
            self.inner.config.page_size,
        )?))
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
        join_runtime.try_execute_simple_indexed_join_projection_query(query, params)
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
        runtime.load_deferred_tables_at_snapshot(
            &self.inner.pager,
            &self.inner.wal,
            self.inner.config.page_size,
            snapshot_lsn,
        )
    }

    fn execute_read_in_runtime_state(
        &self,
        statement: &SqlStatement,
        params: &[Value],
        runtime: &mut EngineRuntime,
        snapshot_lsn: u64,
        indexes_maybe_stale: &mut bool,
    ) -> Result<QueryResult> {
        if self.statement_is_temp_only(runtime, statement) {
            return runtime.execute_read_statement(statement, params, self.inner.config.page_size);
        }
        if !*indexes_maybe_stale {
            if let SqlStatement::Query(query) = statement {
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
        if let Some(result) = self.try_execute_query_with_row_sources_at_snapshot(
            runtime,
            statement,
            params,
            snapshot_lsn,
            *indexes_maybe_stale,
        )? {
            return Ok(result);
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
                if runtime.prepare_simple_update(update)?.is_some() {
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
        if names.is_empty() {
            return Ok(true);
        }
        let names_refs: Vec<&str> = names.iter().map(|s: &String| s.as_str()).collect();
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
            runtime.load_deferred_tables_at_snapshot(
                &self.inner.pager,
                &self.inner.wal,
                self.inner.config.page_size,
                snapshot_lsn,
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
        *persistent_changed |= !self.statement_is_temp_only(runtime, prepared.statement.as_ref());
        Ok(Some(result))
    }

    fn exclusive_sql_txn_error(&self) -> DbError {
        DbError::transaction(
            "a SQL transaction handle is active on this database handle; use it until commit or rollback",
        )
    }
}

/// Evicts the shared WAL registry entry for an on-disk database path.
pub fn evict_shared_wal(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let vfs = VfsHandle::for_path(path);
    WalHandle::evict(&vfs, path)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TransactionControl {
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PragmaCommand {
    Query(PragmaName),
    TableInfo(String),
    Set(PragmaName, i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PragmaName {
    PageSize,
    CacheSize,
    IntegrityCheck,
    DatabaseList,
    TableInfo,
}

fn parse_transaction_control(sql: &str) -> Option<TransactionControl> {
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

fn parse_pragma_command(sql: &str) -> Result<Option<PragmaCommand>> {
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
        if argument.is_empty() {
            return Err(DbError::sql("PRAGMA call requires an argument"));
        }
        return match name {
            PragmaName::TableInfo => {
                let table_name = parse_pragma_table_argument(argument)?;
                Ok(Some(PragmaCommand::TableInfo(table_name)))
            }
            _ => Err(DbError::sql(format!(
                "PRAGMA {} does not accept call syntax",
                pragma_name_sql(name)
            ))),
        };
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
        let value = value.parse::<i64>().map_err(|_| {
            DbError::sql(format!(
                "PRAGMA {} expects an integer value",
                name.to_ascii_lowercase()
            ))
        })?;
        PragmaCommand::Set(pragma_name, value)
    } else {
        PragmaCommand::Query(pragma_name)
    };
    Ok(Some(command))
}

fn parse_pragma_name(name: &str) -> Result<PragmaName> {
    let normalized = name.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "page_size" => Ok(PragmaName::PageSize),
        "cache_size" => Ok(PragmaName::CacheSize),
        "integrity_check" => Ok(PragmaName::IntegrityCheck),
        "database_list" => Ok(PragmaName::DatabaseList),
        "table_info" => Ok(PragmaName::TableInfo),
        _ => Err(DbError::sql(format!("unsupported PRAGMA {}", normalized))),
    }
}

fn pragma_name_sql(name: PragmaName) -> &'static str {
    match name {
        PragmaName::PageSize => "page_size",
        PragmaName::CacheSize => "cache_size",
        PragmaName::IntegrityCheck => "integrity_check",
        PragmaName::DatabaseList => "database_list",
        PragmaName::TableInfo => "table_info",
    }
}

fn parse_pragma_table_argument(argument: &str) -> Result<String> {
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

fn cache_size_pages(config: &DbConfig) -> i64 {
    let bytes = config.cache_size_mb.saturating_mul(1024 * 1024);
    let pages = (bytes / config.page_size as usize).max(1);
    i64::try_from(pages).unwrap_or(i64::MAX)
}

fn normalized_control_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_savepoint_control(sql: &str) -> Option<TransactionControl> {
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

fn strip_control_prefix<'a>(sql: &'a str, prefix: &str) -> Option<&'a str> {
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

fn canonical_savepoint_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn split_sql_batch(sql: &str) -> Vec<String> {
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

fn prepared_statement_sql(sql: &str) -> Result<String> {
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
    Ok(statement)
}

fn table_info(table: &TableSchema, row_count: usize) -> TableInfo {
    TableInfo {
        name: table.name.clone(),
        temporary: table.temporary,
        columns: table.columns.iter().map(column_info).collect(),
        checks: table
            .checks
            .iter()
            .map(|check| check.expression_sql.clone())
            .collect(),
        foreign_keys: table.foreign_keys.iter().map(foreign_key_info).collect(),
        primary_key_columns: table.primary_key_columns.clone(),
        row_count,
    }
}

fn column_info(column: &ColumnSchema) -> ColumnInfo {
    ColumnInfo {
        name: column.name.clone(),
        column_type: column.column_type.as_str().to_string(),
        nullable: column.nullable,
        default_sql: column.default_sql.clone(),
        primary_key: column.primary_key,
        unique: column.unique,
        auto_increment: column.auto_increment,
        checks: column
            .checks
            .iter()
            .map(|check| check.expression_sql.clone())
            .collect(),
        foreign_key: column.foreign_key.as_ref().map(foreign_key_info),
    }
}

fn foreign_key_info(foreign_key: &ForeignKeyConstraint) -> ForeignKeyInfo {
    ForeignKeyInfo {
        name: foreign_key.name.clone(),
        columns: foreign_key.columns.clone(),
        referenced_table: foreign_key.referenced_table.clone(),
        referenced_columns: foreign_key.referenced_columns.clone(),
        on_delete: foreign_key_action_name(foreign_key.on_delete).to_string(),
        on_update: foreign_key_action_name(foreign_key.on_update).to_string(),
    }
}

fn index_info(index: &IndexSchema) -> IndexInfo {
    IndexInfo {
        name: index.name.clone(),
        table_name: index.table_name.clone(),
        kind: match index.kind {
            IndexKind::Btree => "btree",
            IndexKind::Trigram => "trigram",
        }
        .to_string(),
        unique: index.unique,
        columns: index.columns.iter().map(index_column_name).collect(),
        include_columns: index.include_columns.clone(),
        predicate_sql: index.predicate_sql.clone(),
        fresh: index.fresh,
    }
}

fn view_info(view: &ViewSchema) -> ViewInfo {
    ViewInfo {
        name: view.name.clone(),
        temporary: view.temporary,
        sql_text: view.sql_text.clone(),
        column_names: view.column_names.clone(),
        dependencies: view.dependencies.clone(),
    }
}

fn trigger_info(trigger: &TriggerSchema) -> TriggerInfo {
    TriggerInfo {
        name: trigger.name.clone(),
        target_name: trigger.target_name.clone(),
        kind: trigger_kind_name(trigger.kind).to_string(),
        event: trigger_event_name(trigger.event).to_string(),
        on_view: trigger.on_view,
        action_sql: trigger.action_sql.clone(),
    }
}

fn schema_snapshot(db: &Db, runtime: &EngineRuntime) -> Result<SchemaSnapshot> {
    let mut tables = Vec::with_capacity(runtime.catalog.tables.len() + runtime.temp_tables.len());
    for table in runtime.catalog.tables.values() {
        tables.push(schema_table_info(
            table,
            db.runtime_table_row_count(runtime, &table.name)?,
        ));
    }
    for table in runtime.temp_tables.values() {
        tables.push(schema_table_info(
            table,
            db.runtime_table_row_count(runtime, &table.name)?,
        ));
    }
    tables.sort_by(|left, right| left.name.cmp(&right.name));

    let mut views = runtime
        .catalog
        .views
        .values()
        .map(schema_view_info)
        .collect::<Vec<_>>();
    views.extend(runtime.temp_views.values().map(schema_view_info));
    views.sort_by(|left, right| left.name.cmp(&right.name));

    let mut indexes = runtime
        .catalog
        .indexes
        .values()
        .map(schema_index_info)
        .collect::<Vec<_>>();
    indexes.sort_by(|left, right| left.name.cmp(&right.name));

    let mut triggers = runtime
        .catalog
        .triggers
        .values()
        .map(schema_trigger_info)
        .collect::<Vec<_>>();
    triggers.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(SchemaSnapshot {
        snapshot_version: 1,
        schema_cookie: runtime.catalog.schema_cookie,
        tables,
        views,
        indexes,
        triggers,
    })
}

fn schema_table_info(table: &TableSchema, row_count: usize) -> SchemaTableInfo {
    SchemaTableInfo {
        name: table.name.clone(),
        temporary: table.temporary,
        ddl: render_create_table(table),
        row_count,
        primary_key_columns: table.primary_key_columns.clone(),
        checks: table.checks.iter().map(check_constraint_info).collect(),
        foreign_keys: table.foreign_keys.iter().map(foreign_key_info).collect(),
        columns: table.columns.iter().map(schema_column_info).collect(),
    }
}

fn schema_column_info(column: &ColumnSchema) -> SchemaColumnInfo {
    SchemaColumnInfo {
        name: column.name.clone(),
        column_type: column.column_type.as_str().to_string(),
        nullable: column.nullable,
        default_sql: column.default_sql.clone(),
        primary_key: column.primary_key,
        unique: column.unique,
        auto_increment: column.auto_increment,
        generated_sql: column.generated_sql.clone(),
        generated_stored: column.generated_stored,
        checks: column.checks.iter().map(check_constraint_info).collect(),
        foreign_key: column.foreign_key.as_ref().map(foreign_key_info),
    }
}

fn check_constraint_info(check: &CheckConstraint) -> CheckConstraintInfo {
    CheckConstraintInfo {
        name: check.name.clone(),
        expression_sql: check.expression_sql.clone(),
    }
}

fn schema_view_info(view: &ViewSchema) -> SchemaViewInfo {
    SchemaViewInfo {
        name: view.name.clone(),
        temporary: view.temporary,
        sql_text: view.sql_text.clone(),
        column_names: view.column_names.clone(),
        dependencies: view.dependencies.clone(),
        ddl: render_create_view(view),
    }
}

fn schema_index_info(index: &IndexSchema) -> SchemaIndexInfo {
    SchemaIndexInfo {
        name: index.name.clone(),
        table_name: index.table_name.clone(),
        kind: match index.kind {
            IndexKind::Btree => "btree",
            IndexKind::Trigram => "trigram",
        }
        .to_string(),
        unique: index.unique,
        columns: index.columns.iter().map(index_column_name).collect(),
        include_columns: index.include_columns.clone(),
        predicate_sql: index.predicate_sql.clone(),
        fresh: index.fresh,
        temporary: false,
        ddl: render_create_index(index),
    }
}

fn schema_trigger_info(trigger: &TriggerSchema) -> SchemaTriggerInfo {
    let event = trigger_event_name(trigger.event).to_ascii_lowercase();
    SchemaTriggerInfo {
        name: trigger.name.clone(),
        target_name: trigger.target_name.clone(),
        target_kind: if trigger.on_view {
            "view".to_string()
        } else {
            "table".to_string()
        },
        timing: match trigger.kind {
            TriggerKind::After => "after".to_string(),
            TriggerKind::InsteadOf => "instead_of".to_string(),
        },
        events: vec![event],
        events_mask: trigger_event_mask(trigger.event),
        for_each_row: true,
        temporary: false,
        action_sql: trigger.action_sql.clone(),
        ddl: render_create_trigger(trigger),
    }
}

fn trigger_event_mask(event: TriggerEvent) -> u32 {
    match event {
        TriggerEvent::Insert => 1,
        TriggerEvent::Update => 2,
        TriggerEvent::Delete => 4,
    }
}

fn index_column_name(column: &IndexColumn) -> String {
    if let Some(name) = &column.column_name {
        name.clone()
    } else if let Some(expression) = &column.expression_sql {
        expression.clone()
    } else {
        "<expr>".to_string()
    }
}

fn foreign_key_action_name(action: ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::NoAction => "NO ACTION",
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::Cascade => "CASCADE",
        ForeignKeyAction::SetNull => "SET NULL",
    }
}

fn trigger_kind_name(kind: TriggerKind) -> &'static str {
    match kind {
        TriggerKind::After => "AFTER",
        TriggerKind::InsteadOf => "INSTEAD OF",
    }
}

fn trigger_event_name(event: TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update => "UPDATE",
        TriggerEvent::Delete => "DELETE",
    }
}

fn runtime_index_entry_count(index: &RuntimeIndex) -> usize {
    match index {
        RuntimeIndex::Btree { keys } => keys.total_row_id_count(),
        RuntimeIndex::Trigram { index } => index.entry_count(),
    }
}

fn render_runtime_dump(
    db: &Db,
    runtime: &mut EngineRuntime,
    snapshot_lsn: Option<u64>,
) -> Result<String> {
    let mut lines = Vec::new();

    for table in runtime.catalog.tables.values() {
        lines.push(render_create_table(table));
    }
    let table_names = runtime.catalog.tables.keys().cloned().collect::<Vec<_>>();
    for table_name in table_names {
        db.ensure_inspection_table_row_source(runtime, &table_name, snapshot_lsn)?;
        let table = runtime
            .catalog
            .table(&table_name)
            .cloned()
            .ok_or_else(|| DbError::internal(format!("unknown table {table_name}")))?;
        let row_source = runtime.table_row_source(&table.name).ok_or_else(|| {
            DbError::internal(format!("table row source for {} is missing", table.name))
        })?;
        for row in row_source.rows() {
            lines.push(render_insert(&table, row?.values()));
        }
        db.redefer_inspection_table_row_source(runtime, &table_name, snapshot_lsn);
    }
    for view in runtime.catalog.views.values() {
        lines.push(render_create_view(view));
    }
    for table in runtime.temp_tables.values() {
        lines.push(render_create_table(table));
    }
    for (table_name, table_data) in runtime.temp_table_data.iter() {
        if let Some(table) = runtime.temp_tables.get(table_name) {
            for row in &table_data.rows {
                lines.push(render_insert(table, &row.values));
            }
        }
    }
    for view in runtime.temp_views.values() {
        lines.push(render_create_view(view));
    }
    for index in runtime.catalog.indexes.values() {
        lines.push(render_create_index(index));
    }
    for trigger in runtime.catalog.triggers.values() {
        lines.push(render_create_trigger(trigger));
    }

    Ok(lines.join("\n"))
}

fn render_create_table(table: &TableSchema) -> String {
    let mut definitions = Vec::new();
    for column in &table.columns {
        let mut definition = format!(
            "{} {}",
            sql_identifier(&column.name),
            column.column_type.as_str()
        );
        if !column.nullable {
            definition.push_str(" NOT NULL");
        }
        if column.primary_key {
            definition.push_str(" PRIMARY KEY");
        }
        if column.unique {
            definition.push_str(" UNIQUE");
        }
        if column.auto_increment {
            definition.push_str(" AUTOINCREMENT");
        }
        if let Some(generated_sql) = &column.generated_sql {
            definition.push_str(" GENERATED ALWAYS AS (");
            definition.push_str(generated_sql);
            if column.generated_stored {
                definition.push_str(") STORED");
            } else {
                definition.push_str(") VIRTUAL");
            }
        } else if let Some(default_sql) = &column.default_sql {
            definition.push_str(" DEFAULT ");
            definition.push_str(default_sql);
        }
        for check in &column.checks {
            definition.push_str(" CHECK (");
            definition.push_str(&check.expression_sql);
            definition.push(')');
        }
        if let Some(foreign_key) = &column.foreign_key {
            definition.push(' ');
            definition.push_str(&render_foreign_key(foreign_key));
        }
        definitions.push(definition);
    }

    if table.primary_key_columns.len() > 1 {
        definitions.push(format!(
            "PRIMARY KEY ({})",
            table
                .primary_key_columns
                .iter()
                .map(|name| sql_identifier(name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    for foreign_key in &table.foreign_keys {
        definitions.push(render_foreign_key(foreign_key));
    }
    for check in &table.checks {
        definitions.push(format!("CHECK ({})", check.expression_sql));
    }

    format!(
        "CREATE {}TABLE {} ({});",
        if table.temporary { "TEMP " } else { "" },
        sql_identifier(&table.name),
        definitions.join(", ")
    )
}

fn render_foreign_key(foreign_key: &ForeignKeyConstraint) -> String {
    let mut sql = String::new();
    if let Some(name) = &foreign_key.name {
        sql.push_str("CONSTRAINT ");
        sql.push_str(&sql_identifier(name));
        sql.push(' ');
    }
    sql.push_str("FOREIGN KEY (");
    sql.push_str(
        &foreign_key
            .columns
            .iter()
            .map(|name| sql_identifier(name))
            .collect::<Vec<_>>()
            .join(", "),
    );
    sql.push_str(") REFERENCES ");
    sql.push_str(&sql_identifier(&foreign_key.referenced_table));
    sql.push_str(" (");
    sql.push_str(
        &foreign_key
            .referenced_columns
            .iter()
            .map(|name| sql_identifier(name))
            .collect::<Vec<_>>()
            .join(", "),
    );
    sql.push(')');
    if foreign_key.on_delete != ForeignKeyAction::NoAction {
        sql.push_str(" ON DELETE ");
        sql.push_str(foreign_key_action_name(foreign_key.on_delete));
    }
    if foreign_key.on_update != ForeignKeyAction::NoAction {
        sql.push_str(" ON UPDATE ");
        sql.push_str(foreign_key_action_name(foreign_key.on_update));
    }
    sql
}

fn render_insert(table: &TableSchema, values: &[Value]) -> String {
    let columns = table
        .columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let values = values
        .iter()
        .map(render_value_sql)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({columns}) VALUES ({values});",
        sql_identifier(&table.name)
    )
}

fn render_create_view(view: &ViewSchema) -> String {
    let columns = if view.column_names.is_empty() {
        String::new()
    } else {
        format!(
            " ({})",
            view.column_names
                .iter()
                .map(|name| sql_identifier(name))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "CREATE {}VIEW {}{columns} AS {};",
        if view.temporary { "TEMP " } else { "" },
        sql_identifier(&view.name),
        view.sql_text
    )
}

fn render_create_index(index: &IndexSchema) -> String {
    let unique = if index.unique { "UNIQUE " } else { "" };
    let using = match index.kind {
        IndexKind::Btree => String::new(),
        IndexKind::Trigram => " USING trigram".to_string(),
    };
    let columns = index
        .columns
        .iter()
        .map(index_column_name)
        .collect::<Vec<_>>()
        .join(", ");
    let include = if index.include_columns.is_empty() {
        String::new()
    } else {
        format!(
            " INCLUDE ({})",
            index
                .include_columns
                .iter()
                .map(|column| sql_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let predicate = index
        .predicate_sql
        .as_ref()
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    format!(
        "CREATE {unique}INDEX {} ON {}{using} ({columns}){include}{predicate};",
        sql_identifier(&index.name),
        sql_identifier(&index.table_name)
    )
}

fn render_create_trigger(trigger: &TriggerSchema) -> String {
    format!(
        "CREATE TRIGGER {} {} {} ON {} FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql({});",
        sql_identifier(&trigger.name),
        trigger_kind_name(trigger.kind),
        trigger_event_name(trigger.event),
        sql_identifier(&trigger.target_name),
        render_value_sql(&Value::Text(trigger.action_sql.clone()))
    )
}

fn render_value_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => {
            if value.is_finite() {
                value.to_string()
            } else {
                "NULL".to_string()
            }
        }
        Value::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
        Value::Blob(value) => format!("X'{}'", hex_encode(value)),
        Value::Decimal { scaled, scale } => {
            if *scale == 0 {
                scaled.to_string()
            } else {
                let negative = *scaled < 0;
                let digits = scaled.unsigned_abs().to_string();
                let scale = usize::from(*scale);
                let padded = if digits.len() <= scale {
                    format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
                } else {
                    digits
                };
                let split = padded.len() - scale;
                let mut decimal = format!("{}.{}", &padded[..split], &padded[split..]);
                if negative {
                    decimal.insert(0, '-');
                }
                decimal
            }
        }
        Value::Uuid(value) => format!("X'{}'", hex_encode(value)),
        Value::TimestampMicros(value) => value.to_string(),
    }
}

fn sql_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;

        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn json_escape(input: String) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::catalog::{
        ColumnSchema, ColumnType, IndexKind, IndexSchema, TableSchema, ViewSchema,
    };
    use crate::config::DbConfig;
    use crate::db::SqlTxnSlot;
    use crate::error::{DbError, Result};
    use crate::exec::{
        decode_paged_table_manifest_payload, EngineRuntime, TableData, TableRowSource,
    };
    use crate::record::overflow::read_overflow;
    use crate::storage::page::{PageId, PageStore};

    use crate::exec::dml::{PreparedInsertColumn, PreparedInsertValueSource, PreparedSimpleInsert};
    use crate::{BulkLoadOptions, Db, Value};

    use super::{split_sql_batch, PreparedInsertCache, StatementCache, TempSchemaState};

    #[derive(Debug)]
    struct PagerReadStore<'a> {
        db: &'a Db,
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
            self.db.read_page(page_id)
        }

        fn write_page(&mut self, _page_id: PageId, _data: &[u8]) -> Result<()> {
            Err(DbError::internal(
                "PagerReadStore does not support writing pages",
            ))
        }
    }

    #[test]
    fn statement_cache_reuses_parsed_statement() {
        let mut cache = StatementCache::with_capacity(4);
        let first = cache.get_or_parse("SELECT 1").expect("parse");
        let second = cache.get_or_parse("SELECT 1").expect("cache hit");
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn statement_cache_evicts_oldest_entry_when_full() {
        let mut cache = StatementCache::with_capacity(1);
        let first = cache.get_or_parse("SELECT 1").expect("parse first");
        let _second = cache.get_or_parse("SELECT 2").expect("parse second");
        let first_again = cache.get_or_parse("SELECT 1").expect("reparse evicted");
        assert!(!Arc::ptr_eq(&first, &first_again));
    }

    #[test]
    fn split_sql_batch_preserves_legacy_trigger_body_statement() {
        let statements = split_sql_batch(
            "CREATE TRIGGER log_insert AFTER INSERT ON users
             FOR EACH ROW BEGIN
               SELECT decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user added'')');
             END;
             INSERT INTO users VALUES (1, 'Ada');",
        );
        assert_eq!(statements.len(), 2);
        assert_eq!(
            statements[0],
            "CREATE TRIGGER log_insert AFTER INSERT ON users
             FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user added'')')"
        );
        assert_eq!(statements[1], "INSERT INTO users VALUES (1, 'Ada')");
    }

    #[test]
    fn temp_schema_apply_is_shallow_when_unmutated() {
        let table = TableSchema {
            name: "temp_docs".to_string(),
            temporary: true,
            columns: vec![ColumnSchema {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
                nullable: false,
                default_sql: None,
                generated_sql: None,
                generated_stored: false,
                primary_key: true,
                unique: true,
                auto_increment: false,
                checks: Vec::new(),
                foreign_key: None,
            }],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        let view = ViewSchema {
            name: "temp_view".to_string(),
            temporary: true,
            sql_text: "SELECT id FROM temp_docs".to_string(),
            column_names: vec!["id".to_string()],
            dependencies: vec!["temp_docs".to_string()],
        };
        let index = IndexSchema {
            name: "temp_docs_pk".to_string(),
            table_name: "temp_docs".to_string(),
            kind: IndexKind::Btree,
            unique: true,
            columns: Vec::new(),
            include_columns: Vec::new(),
            predicate_sql: None,
            fresh: false,
        };
        let state = TempSchemaState {
            schema_cookie: 7,
            tables: Arc::new(BTreeMap::from([(table.name.clone(), table)])),
            table_data: Arc::new(BTreeMap::from([(
                "temp_docs".to_string(),
                Arc::new(TableData::default()),
            )])),
            views: Arc::new(BTreeMap::from([(view.name.clone(), view)])),
            indexes: Arc::new(BTreeMap::from([(index.name.clone(), index)])),
        };
        let mut runtime = EngineRuntime::empty(1);

        state.apply_to_runtime(&mut runtime);

        assert_eq!(runtime.temp_schema_cookie, 7);
        assert!(Arc::ptr_eq(&state.tables, &runtime.temp_tables));
        assert!(Arc::ptr_eq(&state.table_data, &runtime.temp_table_data));
        assert!(Arc::ptr_eq(&state.views, &runtime.temp_views));
        assert!(Arc::ptr_eq(&state.indexes, &runtime.temp_indexes));
    }

    #[test]
    fn pragma_page_size_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let result = db.execute("PRAGMA page_size").expect("pragma page_size");
        assert_eq!(result.columns(), &["page_size".to_string()]);
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(i64::from(db.config().page_size))]
        );
    }

    #[test]
    fn pragma_cache_size_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let result = db.execute("PRAGMA cache_size").expect("pragma cache_size");
        assert_eq!(result.columns(), &["cache_size".to_string()]);
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(super::cache_size_pages(db.config()))]
        );
    }

    #[test]
    fn pragma_integrity_check_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'a')")
            .expect("insert row");
        let result = db
            .execute("PRAGMA integrity_check")
            .expect("pragma integrity_check");
        assert_eq!(result.columns(), &["integrity_check".to_string()]);
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Text("ok".to_string())]);
    }

    #[test]
    fn pragma_database_list_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let result = db
            .execute("PRAGMA database_list")
            .expect("pragma database_list");
        assert_eq!(
            result.columns(),
            &["seq".to_string(), "name".to_string(), "file".to_string()]
        );
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(0),
                Value::Text("main".to_string()),
                Value::Text(":memory:".to_string())
            ]
        );
    }

    #[test]
    fn pragma_table_info_query() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, name TEXT DEFAULT 'anon')")
            .expect("create table");

        let result = db.execute("PRAGMA table_info(t)").expect("table_info");
        assert_eq!(
            result.columns(),
            &[
                "cid".to_string(),
                "name".to_string(),
                "type".to_string(),
                "notnull".to_string(),
                "dflt_value".to_string(),
                "pk".to_string()
            ]
        );
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(0),
                Value::Text("id".to_string()),
                Value::Text("INT64".to_string()),
                Value::Int64(1),
                Value::Null,
                Value::Int64(1)
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(1),
                Value::Text("name".to_string()),
                Value::Text("TEXT".to_string()),
                Value::Int64(0),
                Value::Text("'anon'".to_string()),
                Value::Int64(0)
            ]
        );
    }

    #[test]
    fn pragma_table_info_assignment_is_rejected() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let error = db
            .execute("PRAGMA table_info = 1")
            .expect_err("assignment should fail");
        assert!(
            error.to_string().contains("does not support assignment"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn pragma_assignments_are_limited() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let page_size_noop = db.execute("PRAGMA page_size = 4096");
        assert!(
            page_size_noop.is_ok(),
            "expected no-op assignment to succeed"
        );

        let cache_size_error = db
            .execute("PRAGMA cache_size = 8")
            .expect_err("cache_size assignment should fail");
        assert!(cache_size_error
            .to_string()
            .contains("cannot be changed on an open connection"));

        let integrity_assignment_error = db
            .execute("PRAGMA integrity_check = 1")
            .expect_err("integrity_check assignment should fail");
        assert!(integrity_assignment_error
            .to_string()
            .contains("does not support assignment"));
    }

    #[test]
    fn unsupported_pragma_reports_sql_error() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        let error = db
            .execute("PRAGMA foreign_keys")
            .expect_err("unsupported pragma should fail");
        assert!(error.to_string().contains("unsupported PRAGMA"));
    }

    #[test]
    fn prepared_insert_cache_is_scoped_by_schema_cookie() {
        let mut cache = PreparedInsertCache::with_capacity(4);
        let first = cache
            .get_or_prepare("INSERT INTO users (id) VALUES ($1)", 1, 0, || {
                Ok(Some(dummy_prepared_insert("users_v1")))
            })
            .expect("prepare first")
            .expect("prepared plan");
        let cached = cache
            .get_or_prepare("INSERT INTO users (id) VALUES ($1)", 1, 0, || {
                Ok(Some(dummy_prepared_insert("users_v1_new")))
            })
            .expect("prepare cached")
            .expect("cached plan");
        assert!(Arc::ptr_eq(&first, &cached));

        let second_schema = cache
            .get_or_prepare("INSERT INTO users (id) VALUES ($1)", 2, 0, || {
                Ok(Some(dummy_prepared_insert("users_v2")))
            })
            .expect("prepare second schema")
            .expect("prepared plan");
        assert!(!Arc::ptr_eq(&first, &second_schema));
        assert_eq!(second_schema.table_name, "users_v2");
    }

    #[test]
    fn default_deferred_materialization_keeps_prepared_insert_fast_path_enabled() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        assert!(
            db.can_use_autocommit_prepared_insert_fast_path("t")
                .expect("prepared insert fast path check"),
            "default deferred materialization must not disable prepared inserts"
        );
    }

    #[test]
    fn shared_transaction_prepared_insert_refreshes_generic_cached_plan_once() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("shared-txn-prepared-insert-refresh.ddb");
        let config = DbConfig::default();

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE bench (id INTEGER, val TEXT, f FLOAT64)")
                .expect("create bench table");
            db.execute("CREATE INDEX bench_id_idx ON bench(id)")
                .expect("create bench index");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction().expect("begin shared transaction");
        let mut prepared = db
            .prepare("INSERT INTO bench VALUES ($1, $2, $3)")
            .expect("prepare insert");
        let cached_plan = prepared
            .prepared_insert
            .as_ref()
            .expect("prepared insert plan");
        let mut generic_plan = (**cached_plan).clone();
        generic_plan.use_generic_index_updates = true;
        prepared.prepared_insert = Some(Arc::new(generic_plan));

        prepared
            .execute(&[
                Value::Int64(1),
                Value::Text("value-1".to_string()),
                Value::Float64(1.0),
            ])
            .expect("execute prepared insert in shared transaction");

        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            let cached_runtime_plan = state
                .prepared_insert_runtime_cache
                .get(&Db::prepared_statement_cache_key(&prepared))
                .expect("refreshed runtime insert plan");
            assert!(
                !cached_runtime_plan.use_generic_index_updates,
                "transaction runtime should cache a specialized insert plan once row sources are loaded"
            );
        }

        prepared
            .execute(&[
                Value::Int64(2),
                Value::Text("value-2".to_string()),
                Value::Float64(2.0),
            ])
            .expect("reuse prepared insert in shared transaction");
        db.commit_transaction().expect("commit shared transaction");

        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM bench")
                    .expect("count committed rows")
            ),
            2
        );
    }

    #[test]
    fn exclusive_transaction_commit_persists_prepared_writes() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");

        let mut txn = db.transaction().expect("begin exclusive txn");
        let insert = txn
            .prepare("INSERT INTO t VALUES ($1, $2)")
            .expect("prepare insert");
        for i in 0_i64..32_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i), Value::Text(format!("value-{i}"))],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit txn");

        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
            32
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT id FROM t WHERE val = 'value-17'")
                    .expect("lookup committed row")
            ),
            17
        );
    }

    #[test]
    fn exclusive_transaction_rollback_discards_persistent_changes() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'seed')")
            .expect("seed row");

        let mut txn = db.transaction().expect("begin exclusive txn");
        let insert = txn
            .prepare("INSERT INTO t VALUES ($1, $2)")
            .expect("prepare insert");
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(2), Value::Text("transient".to_string())],
            )
            .expect("insert transient row");
        txn.rollback().expect("rollback txn");

        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
            1
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM t WHERE id = 2")
                    .expect("count rolled back row")
            ),
            0
        );
    }

    #[test]
    fn checkpoint_compacts_large_persisted_payloads() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("checkpoint-compacts-large-payloads.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs table");

        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin exclusive txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i), Value::Text(large_body.clone())],
                )
                .expect("insert large row");
        }
        txn.commit().expect("commit rows");

        let runtime_before = db
            .runtime_for_metadata_inspection()
            .expect("runtime before checkpoint");
        let docs_before = runtime_before
            .persisted_tables
            .get("docs")
            .expect("persisted docs table before checkpoint");
        assert!(
            docs_before.pointer.logical_len >= 64 * 1024,
            "test setup did not create a large enough payload"
        );
        assert!(
            !docs_before.pointer.is_compressed(),
            "normal commits should leave table payloads uncompressed"
        );

        db.checkpoint().expect("checkpoint");

        let runtime_after = db
            .runtime_for_metadata_inspection()
            .expect("runtime after checkpoint");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs table after checkpoint");
        assert!(
            docs_after.pointer.is_compressed(),
            "checkpoint should compact large persisted payloads"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count docs rows")
            ),
            96
        );
    }

    #[test]
    fn checkpoint_preserves_large_payloads_with_persistent_pk_index() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-keeps-pk-payloads.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, config).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs table");

        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin exclusive txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i), Value::Text(large_body.clone())],
                )
                .expect("insert large row");
        }
        txn.commit().expect("commit rows");

        let runtime_before = db
            .runtime_for_metadata_inspection()
            .expect("runtime before checkpoint");
        let docs_before = runtime_before
            .persisted_tables
            .get("docs")
            .expect("persisted docs before checkpoint");
        assert!(
            docs_before.pointer.logical_len >= 64 * 1024,
            "test setup did not create a large enough payload"
        );
        assert!(
            !docs_before.pointer.is_compressed(),
            "persistent pk writes should keep payload uncompressed"
        );
        assert!(
            docs_before.pk_index_root.is_some(),
            "persistent pk writes should record a locator tree root"
        );

        db.checkpoint().expect("checkpoint");

        let runtime_after = db
            .runtime_for_metadata_inspection()
            .expect("runtime after checkpoint");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after checkpoint");
        assert!(
            !docs_after.pointer.is_compressed(),
            "checkpoint should not compact payloads with persistent pk roots"
        );
        assert!(
            docs_after.pk_index_root.is_some(),
            "checkpoint should preserve the persistent pk locator tree"
        );
    }

    #[test]
    fn checkpoint_keeps_deferred_paged_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("checkpoint-keeps-deferred-paged-tables-unloaded.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i), Value::Text(large_body.clone())],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );

        db.checkpoint().expect("checkpoint");

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after checkpoint");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected checkpoint compaction to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected checkpoint compaction to keep paged-backed table deferred, got: {json_after}"
        );
    }

    #[test]
    fn checkpoint_compacts_paged_table_chunks_and_preserves_persistent_pk_index() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-compacts-paged-table.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            paged_row_storage: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, config).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs table");

        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin exclusive txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i), Value::Text(large_body.clone())],
                )
                .expect("insert large row");
        }
        txn.commit().expect("commit rows");

        let runtime_before = db
            .runtime_for_metadata_inspection()
            .expect("runtime before checkpoint");
        let docs_before = runtime_before
            .persisted_tables
            .get("docs")
            .expect("persisted docs before checkpoint");
        assert!(
            docs_before.pointer.is_table_paged_manifest(),
            "paged row storage should persist docs through a paged manifest"
        );
        assert!(
            docs_before.pk_index_root.is_some(),
            "paged row storage should preserve the persistent pk locator tree"
        );
        let page_store = PagerReadStore { db: &db };
        let manifest_before = read_overflow(&page_store, docs_before.pointer)
            .expect("read paged manifest before checkpoint");
        let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
            .expect("decode paged manifest before checkpoint");
        assert!(
            manifest_before
                .chunks
                .iter()
                .any(|chunk| !chunk.pointer.is_compressed()),
            "normal paged writes should leave chunk payloads uncompressed"
        );

        db.checkpoint().expect("checkpoint");

        let runtime_after = db
            .runtime_for_metadata_inspection()
            .expect("runtime after checkpoint");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after checkpoint");
        assert!(
            docs_after.pointer.is_table_paged_manifest(),
            "checkpoint should preserve paged-table state"
        );
        assert!(
            docs_after.pk_index_root.is_some(),
            "checkpoint should preserve the persistent pk locator tree"
        );
        let manifest_after = read_overflow(&page_store, docs_after.pointer)
            .expect("read paged manifest after checkpoint");
        let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
            .expect("decode paged manifest after checkpoint");
        assert!(
            manifest_after
                .chunks
                .iter()
                .any(|chunk| chunk.pointer.is_compressed()),
            "checkpoint should compact large paged chunk payloads"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count docs rows after checkpoint")
            ),
            96
        );
        assert_eq!(
            scalar_text(
                &db.execute("SELECT body FROM docs WHERE id = 17")
                    .expect("select row after checkpoint")
            ),
            large_body
        );
    }

    #[test]
    fn persistent_pk_index_backfills_compressed_tables_and_keeps_point_lookup_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("persistent-pk-backfill.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");

            let runtime = db
                .runtime_for_metadata_inspection()
                .expect("runtime after checkpoint");
            let seeded = runtime
                .persisted_tables
                .get("seeded")
                .expect("persisted seeded after checkpoint");
            assert!(
                seeded.pointer.is_compressed(),
                "legacy checkpoint should compact the large payload before backfill"
            );
            assert!(
                seeded.pk_index_root.is_none(),
                "legacy checkpoint should not have a persistent pk root"
            );
        }

        let config = DbConfig {
            persistent_pk_index: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, config).expect("reopen with persistent pk index");
        {
            let runtime = db.inner.engine.read().expect("engine runtime lock");
            let seeded = runtime
                .persisted_tables
                .get("seeded")
                .expect("persisted seeded after backfill");
            assert!(
                !seeded.pointer.is_compressed(),
                "backfill should rewrite the large payload uncompressed"
            );
            assert!(
                seeded.pk_index_root.is_some(),
                "backfill should attach a persistent pk locator tree"
            );
            assert!(
                runtime
                    .catalog
                    .tables
                    .get("seeded")
                    .and_then(|table| table.pk_index_root)
                    .is_some(),
                "catalog state should retain the persistent pk locator root"
            );
        }

        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected backfilled table to stay deferred at open, got: {json_open}"
        );

        let result = db
            .execute("SELECT n FROM seeded WHERE id = 17")
            .expect("point lookup");
        assert_eq!(scalar_i64(&result), 17);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after point lookup");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected persistent pk point lookup to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected seeded to remain deferred after point lookup, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after persistent pk point lookup, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_backfills_legacy_tables_and_keeps_wildcard_scan_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-backfill.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        {
            let runtime = db.inner.engine.read().expect("engine runtime lock");
            let seeded = runtime
                .persisted_tables
                .get("seeded")
                .expect("persisted seeded after paged backfill");
            assert!(
                seeded.pointer.is_table_paged_manifest(),
                "expected legacy table to be wrapped in paged manifest storage"
            );
        }

        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );

        let result = db.execute("SELECT * FROM seeded").expect("wildcard scan");
        assert_eq!(result.rows().len(), 96);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(0),
                Value::Int64(0),
                Value::Text("x".repeat(2048)),
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after scan");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged wildcard scan to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after scan, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_persists_new_large_tables_and_keeps_wildcard_scan_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-new-write.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        {
            let runtime = db.inner.engine.read().expect("engine runtime lock");
            let seeded = runtime
                .persisted_tables
                .get("seeded")
                .expect("persisted seeded after paged write");
            assert!(
                seeded.pointer.is_table_paged_manifest(),
                "expected new large table to persist behind paged manifest storage"
            );
        }

        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );

        let result = db.execute("SELECT * FROM seeded").expect("wildcard scan");
        assert_eq!(result.rows().len(), 96);
        assert_eq!(
            result.rows()[95].values(),
            &[
                Value::Int64(95),
                Value::Int64(95),
                Value::Text("x".repeat(2048)),
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after scan");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged wildcard scan to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after scan, got: {json_after}"
        );
    }

    #[test]
    fn single_row_insert_with_default_deferred_loading_completes() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("single-row-insert-default-deferred.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t1(a INT64)")
            .expect("create table");
        db.execute("INSERT INTO t1 VALUES (1)")
            .expect("single-row insert");
        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t1").expect("count rows")),
            1
        );
    }

    #[test]
    fn metadata_inspection_keeps_deferred_paged_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("metadata-inspection-keeps-deferred-paged-tables-unloaded.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );

        let tables = db.list_tables().expect("list tables");
        let seeded = tables
            .iter()
            .find(|table| table.name == "seeded")
            .expect("seeded table metadata");
        assert_eq!(seeded.row_count, 96);

        let described = db.describe_table("seeded").expect("describe seeded");
        assert_eq!(described.row_count, 96);

        let snapshot = db.get_schema_snapshot().expect("schema snapshot");
        let seeded_snapshot = snapshot
            .tables
            .iter()
            .find(|table| table.name == "seeded")
            .expect("seeded table in snapshot");
        assert_eq!(seeded_snapshot.row_count, 96);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after metadata");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected metadata inspection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected metadata inspection to keep paged-backed table deferred, got: {json_after}"
        );
    }

    #[test]
    fn single_index_admin_paths_keep_deferred_paged_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("single-index-admin-paths-keep-deferred-paged-tables-unloaded.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_docs_n ON docs(n)")
                .expect("create docs index");
            let large_body = "x".repeat(2048);
            let large_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_note.clone()),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit rows");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed tables to stay deferred at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":2"),
            "expected both paged-backed tables deferred at open, got: {json_open}"
        );

        let verification = db.verify_index("idx_docs_n").expect("verify index");
        assert!(verification.valid);
        assert_eq!(verification.expected_entries, 96);
        assert_eq!(verification.actual_entries, 96);

        let json_after_verify = db.inspect_storage_state_json().expect("json after verify");
        assert!(
            json_after_verify.contains("\"loaded_table_count\":0"),
            "expected verify_index to avoid live materialization, got: {json_after_verify}"
        );
        assert!(
            json_after_verify.contains("\"deferred_table_count\":2"),
            "expected verify_index to keep both paged-backed tables deferred, got: {json_after_verify}"
        );

        db.rebuild_index("idx_docs_n").expect("rebuild index");

        let json_after_rebuild = db.inspect_storage_state_json().expect("json after rebuild");
        assert!(
            json_after_rebuild.contains("\"loaded_table_count\":0"),
            "expected rebuild_index to avoid live materialization, got: {json_after_rebuild}"
        );
        assert!(
            json_after_rebuild.contains("\"deferred_table_count\":2"),
            "expected rebuild_index to keep both paged-backed tables deferred, got: {json_after_rebuild}"
        );

        db.rebuild_indexes().expect("rebuild all indexes");

        let json_after_rebuild_all = db
            .inspect_storage_state_json()
            .expect("json after rebuild all");
        assert!(
            json_after_rebuild_all.contains("\"loaded_table_count\":0"),
            "expected rebuild_indexes to avoid live materialization, got: {json_after_rebuild_all}"
        );
        assert!(
            json_after_rebuild_all.contains("\"deferred_table_count\":2"),
            "expected rebuild_indexes to keep both paged-backed tables deferred, got: {json_after_rebuild_all}"
        );
    }

    #[test]
    fn paged_row_storage_wildcard_ordered_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-wildcard-ordered-projection.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..48_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute("SELECT * FROM seeded ORDER BY n DESC LIMIT 2 OFFSET 1")
            .expect("ordered wildcard projection");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(46),
                Value::Int64(46),
                Value::Text("x".repeat(2048))
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(45),
                Value::Int64(45),
                Value::Text("x".repeat(2048))
            ]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after ordered wildcard projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged ordered wildcard projection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after ordered wildcard projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged ordered wildcard projection, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_filtered_projection_with_offset_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-filtered-projection-offset.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..48_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute(
                "SELECT n FROM seeded WHERE n >= 10 AND n <= 20 ORDER BY n DESC LIMIT 2 OFFSET 1",
            )
            .expect("paged ordered filtered projection with offset");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(19)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(18)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after paged ordered filtered projection with offset");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged ordered filtered projection with offset to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after ordered filtered projection with offset, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged ordered filtered projection with offset, got: {json_after}"
        );
    }

    #[test]
    fn persistent_pk_index_keeps_paged_row_storage_point_lookup_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-pk-lookup.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage + pk index");
        {
            let runtime = db.inner.engine.read().expect("engine runtime lock");
            let seeded = runtime
                .persisted_tables
                .get("seeded")
                .expect("persisted seeded after paged write");
            assert!(
                seeded.pointer.is_table_paged_manifest(),
                "expected new large table to persist behind paged manifest storage"
            );
            assert!(
                seeded.pk_index_root.is_some(),
                "expected paged-backed table to retain a persistent pk locator root"
            );
        }

        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );

        let result = db
            .execute("SELECT n FROM seeded WHERE id = 95")
            .expect("point lookup");
        assert_eq!(scalar_i64(&result), 95);

        let json_after = db.inspect_storage_state_json().expect("json after lookup");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged point lookup to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after point lookup, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged point lookup, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_updates_and_deletes_preserve_untouched_chunks() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-update-delete.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            paged_row_storage: true,
            ..DbConfig::default()
        };

        let untouched_chunk_pointers = {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");

            let untouched_chunk_pointers = {
                let runtime_before = db.inner.engine.read().expect("engine runtime lock");
                let docs_before = runtime_before
                    .persisted_tables
                    .get("docs")
                    .expect("persisted docs before mutation");
                let page_store = PagerReadStore { db: &db };
                let manifest_before = read_overflow(&page_store, docs_before.pointer)
                    .expect("read paged manifest before mutation");
                let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                    .expect("decode paged manifest before mutation");
                assert!(
                    manifest_before.chunks.len() > 2,
                    "expected multiple chunks before mutation"
                );
                manifest_before
                    .chunks
                    .iter()
                    .skip(1)
                    .map(|chunk| chunk.pointer)
                    .collect::<Vec<_>>()
            };

            db.execute(&format!(
                "UPDATE docs SET n = 500, body = '{}' WHERE id = 6",
                "y".repeat(2600)
            ))
            .expect("update docs row");
            db.execute("DELETE FROM docs WHERE id = 7")
                .expect("delete docs row");

            untouched_chunk_pointers
        };

        let db = Db::open_or_create(&path, config).expect("reopen mutated db");
        let preserved_untouched = {
            let runtime_after = db.inner.engine.read().expect("engine runtime lock");
            let docs_after = runtime_after
                .persisted_tables
                .get("docs")
                .expect("persisted docs after mutation");
            assert!(
                docs_after.pointer.is_table_paged_manifest(),
                "mutated table should remain paged"
            );
            assert!(
                docs_after.pk_index_root.is_some(),
                "mutated paged table should retain persistent pk locator root"
            );
            let page_store = PagerReadStore { db: &db };
            let manifest_after = read_overflow(&page_store, docs_after.pointer)
                .expect("read paged manifest after mutation");
            let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
                .expect("decode paged manifest after mutation");
            assert_eq!(
                manifest_after
                    .chunks
                    .iter()
                    .map(|chunk| chunk.row_count)
                    .sum::<usize>(),
                95,
                "paged manifest row counts should reflect the delete"
            );
            manifest_after
                .chunks
                .iter()
                .filter_map(|chunk| {
                    untouched_chunk_pointers
                        .contains(&chunk.pointer)
                        .then_some(chunk.pointer)
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "unchanged paged chunks should retain their original pointers"
        );

        let updated = db
            .execute("SELECT n FROM docs WHERE id = 6")
            .expect("point lookup after update");
        assert_eq!(scalar_i64(&updated), 500);
        let deleted = db
            .execute("SELECT n FROM docs WHERE id = 7")
            .expect("point lookup after delete");
        assert!(
            deleted.rows().is_empty(),
            "deleted row should no longer be visible"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count docs rows")
            ),
            95
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after mutation");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged point lookup to stay deferred after mutation, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_updates_and_deletes_after_reopen_preserve_untouched_chunks() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-update-delete-after-reopen.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let untouched_chunk_pointers = {
            let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
            let json_open = db.inspect_storage_state_json().expect("json at reopen");
            assert!(
                json_open.contains("\"deferred_table_count\":1"),
                "expected paged-backed table to stay deferred at reopen, got: {json_open}"
            );

            let untouched_chunk_pointers = {
                let runtime_before = db.inner.engine.read().expect("engine runtime lock");
                let docs_before = runtime_before
                    .persisted_tables
                    .get("docs")
                    .expect("persisted docs before mutation");
                let page_store = PagerReadStore { db: &db };
                let manifest_before = read_overflow(&page_store, docs_before.pointer)
                    .expect("read paged manifest before mutation");
                let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                    .expect("decode paged manifest before mutation");
                assert!(
                    manifest_before.chunks.len() > 2,
                    "expected multiple chunks before mutation"
                );
                manifest_before
                    .chunks
                    .iter()
                    .skip(1)
                    .map(|chunk| chunk.pointer)
                    .collect::<Vec<_>>()
            };

            db.execute(&format!(
                "UPDATE docs SET n = 500, body = '{}' WHERE id = 6",
                "y".repeat(2600)
            ))
            .expect("update docs row after reopen");
            db.execute("DELETE FROM docs WHERE id = 7")
                .expect("delete docs row after reopen");

            untouched_chunk_pointers
        };

        let db = Db::open_or_create(&path, config).expect("reopen mutated db");
        let preserved_untouched = {
            let runtime_after = db.inner.engine.read().expect("engine runtime lock");
            let docs_after = runtime_after
                .persisted_tables
                .get("docs")
                .expect("persisted docs after mutation");
            assert!(
                docs_after.pointer.is_table_paged_manifest(),
                "mutated table should remain paged"
            );
            assert!(
                docs_after.pk_index_root.is_some(),
                "mutated paged table should retain persistent pk locator root"
            );
            let page_store = PagerReadStore { db: &db };
            let manifest_after = read_overflow(&page_store, docs_after.pointer)
                .expect("read paged manifest after mutation");
            let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
                .expect("decode paged manifest after mutation");
            assert_eq!(
                manifest_after
                    .chunks
                    .iter()
                    .map(|chunk| chunk.row_count)
                    .sum::<usize>(),
                95,
                "paged manifest row counts should reflect the delete"
            );
            manifest_after
                .chunks
                .iter()
                .filter_map(|chunk| {
                    untouched_chunk_pointers
                        .contains(&chunk.pointer)
                        .then_some(chunk.pointer)
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "unchanged paged chunks should retain their original pointers after reopen-time writes"
        );

        let updated = db
            .execute("SELECT n FROM docs WHERE id = 6")
            .expect("point lookup after reopen update");
        assert_eq!(scalar_i64(&updated), 500);
        let deleted = db
            .execute("SELECT n FROM docs WHERE id = 7")
            .expect("point lookup after reopen delete");
        assert!(
            deleted.rows().is_empty(),
            "deleted row should no longer be visible"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count docs rows")
            ),
            95
        );
    }

    #[test]
    fn paged_row_storage_prepared_insert_after_reopen_preserves_untouched_chunks() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-insert-after-reopen.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let untouched_chunk_pointers = {
            let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
            let json_open = db.inspect_storage_state_json().expect("json at reopen");
            assert!(
                json_open.contains("\"deferred_table_count\":1"),
                "expected paged-backed table to stay deferred at reopen, got: {json_open}"
            );

            let untouched_chunk_pointers = {
                let runtime_before = db.inner.engine.read().expect("engine runtime lock");
                let docs_before = runtime_before
                    .persisted_tables
                    .get("docs")
                    .expect("persisted docs before insert");
                let page_store = PagerReadStore { db: &db };
                let manifest_before = read_overflow(&page_store, docs_before.pointer)
                    .expect("read paged manifest before insert");
                let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                    .expect("decode paged manifest before insert");
                assert!(
                    manifest_before.chunks.len() > 2,
                    "expected multiple chunks before insert"
                );
                manifest_before
                    .chunks
                    .iter()
                    .take(manifest_before.chunks.len() - 1)
                    .map(|chunk| chunk.pointer)
                    .collect::<Vec<_>>()
            };

            let insert = db
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert after reopen");
            insert
                .execute(&[
                    Value::Int64(97),
                    Value::Int64(9600),
                    Value::Text("z".repeat(2048)),
                ])
                .expect("insert row after reopen");

            untouched_chunk_pointers
        };

        let db = Db::open_or_create(&path, config).expect("reopen mutated db");
        let preserved_untouched = {
            let runtime_after = db.inner.engine.read().expect("engine runtime lock");
            let docs_after = runtime_after
                .persisted_tables
                .get("docs")
                .expect("persisted docs after insert");
            assert!(
                docs_after.pointer.is_table_paged_manifest(),
                "inserted table should remain paged"
            );
            assert!(
                docs_after.pk_index_root.is_some(),
                "inserted paged table should retain persistent pk locator root"
            );
            let page_store = PagerReadStore { db: &db };
            let manifest_after = read_overflow(&page_store, docs_after.pointer)
                .expect("read paged manifest after insert");
            let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
                .expect("decode paged manifest after insert");
            assert_eq!(
                manifest_after
                    .chunks
                    .iter()
                    .map(|chunk| chunk.row_count)
                    .sum::<usize>(),
                97,
                "paged manifest row counts should reflect the insert"
            );
            manifest_after
                .chunks
                .iter()
                .filter_map(|chunk| {
                    untouched_chunk_pointers
                        .contains(&chunk.pointer)
                        .then_some(chunk.pointer)
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "untouched paged chunks should retain their original pointers after reopen-time insert"
        );

        let inserted = db
            .execute("SELECT n FROM docs WHERE id = 97")
            .expect("point lookup after reopen insert");
        assert_eq!(scalar_i64(&inserted), 9600);
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count docs rows")
            ),
            97
        );
        let json_after = db
            .inspect_storage_state_json()
            .expect("json after insert reopen");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected inserted paged table to remain off the resident path, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected inserted paged table to remain deferred after reopen, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_expression_insert_after_reopen_preserves_untouched_chunks() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-expression-insert-after-reopen.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let untouched_chunk_pointers = {
            let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
            let json_open = db.inspect_storage_state_json().expect("json at reopen");
            assert!(
                json_open.contains("\"deferred_table_count\":1"),
                "expected paged-backed table to stay deferred at reopen, got: {json_open}"
            );

            let untouched_chunk_pointers = {
                let runtime_before = db.inner.engine.read().expect("engine runtime lock");
                let docs_before = runtime_before
                    .persisted_tables
                    .get("docs")
                    .expect("persisted docs before insert");
                let page_store = PagerReadStore { db: &db };
                let manifest_before = read_overflow(&page_store, docs_before.pointer)
                    .expect("read paged manifest before insert");
                let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                    .expect("decode paged manifest before insert");
                assert!(
                    manifest_before.chunks.len() > 2,
                    "expected multiple chunks before insert"
                );
                manifest_before
                    .chunks
                    .iter()
                    .take(manifest_before.chunks.len() - 1)
                    .map(|chunk| chunk.pointer)
                    .collect::<Vec<_>>()
            };

            db.execute_with_params(
                "INSERT INTO docs VALUES (97, 9600 + 1, $1)",
                &[Value::Text("z".repeat(2048))],
            )
            .expect("insert expression row after reopen");

            untouched_chunk_pointers
        };

        let db = Db::open_or_create(&path, config).expect("reopen mutated db");
        let preserved_untouched = {
            let runtime_after = db.inner.engine.read().expect("engine runtime lock");
            let docs_after = runtime_after
                .persisted_tables
                .get("docs")
                .expect("persisted docs after insert");
            assert!(
                docs_after.pointer.is_table_paged_manifest(),
                "inserted table should remain paged"
            );
            assert!(
                docs_after.pk_index_root.is_some(),
                "inserted paged table should retain persistent pk locator root"
            );
            let page_store = PagerReadStore { db: &db };
            let manifest_after = read_overflow(&page_store, docs_after.pointer)
                .expect("read paged manifest after insert");
            let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
                .expect("decode paged manifest after insert");
            assert_eq!(
                manifest_after
                    .chunks
                    .iter()
                    .map(|chunk| chunk.row_count)
                    .sum::<usize>(),
                97,
                "paged manifest row counts should reflect the insert"
            );
            manifest_after
                .chunks
                .iter()
                .filter_map(|chunk| {
                    untouched_chunk_pointers
                        .contains(&chunk.pointer)
                        .then_some(chunk.pointer)
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "untouched paged chunks should retain their original pointers after reopen-time expression insert"
        );

        let inserted = db
            .execute("SELECT n FROM docs WHERE id = 97")
            .expect("point lookup after reopen insert");
        assert_eq!(scalar_i64(&inserted), 9601);
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count docs rows")
            ),
            97
        );
        let json_after = db
            .inspect_storage_state_json()
            .expect("json after insert reopen");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected inserted paged table to remain off the resident path, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected inserted paged table to remain deferred after reopen, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_insert_returning_after_reopen_preserves_untouched_chunks() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-insert-returning-after-reopen.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let untouched_chunk_pointers = {
            let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
            let json_open = db.inspect_storage_state_json().expect("json at reopen");
            assert!(
                json_open.contains("\"deferred_table_count\":1"),
                "expected paged-backed table to stay deferred at reopen, got: {json_open}"
            );

            let untouched_chunk_pointers = {
                let runtime_before = db.inner.engine.read().expect("engine runtime lock");
                let docs_before = runtime_before
                    .persisted_tables
                    .get("docs")
                    .expect("persisted docs before insert");
                let page_store = PagerReadStore { db: &db };
                let manifest_before = read_overflow(&page_store, docs_before.pointer)
                    .expect("read paged manifest before insert");
                let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                    .expect("decode paged manifest before insert");
                assert!(
                    manifest_before.chunks.len() > 2,
                    "expected multiple chunks before insert"
                );
                manifest_before
                    .chunks
                    .iter()
                    .take(manifest_before.chunks.len() - 1)
                    .map(|chunk| chunk.pointer)
                    .collect::<Vec<_>>()
            };

            let returning = db
                .execute_with_params(
                    "INSERT INTO docs VALUES (97, 9600 + 1, $1) RETURNING n",
                    &[Value::Text("z".repeat(2048))],
                )
                .expect("insert returning row after reopen");
            assert_eq!(scalar_i64(&returning), 9601);

            let json_after = db
                .inspect_storage_state_json()
                .expect("json after insert returning");
            assert!(
                json_after.contains("\"loaded_table_count\":0"),
                "expected generic INSERT RETURNING to avoid resident table loads, got: {json_after}"
            );
            assert!(
                json_after.contains("\"deferred_table_count\":1"),
                "expected inserted paged table to remain deferred after INSERT RETURNING, got: {json_after}"
            );

            untouched_chunk_pointers
        };

        let db = Db::open_or_create(&path, config).expect("reopen mutated db");
        let preserved_untouched = {
            let runtime_after = db.inner.engine.read().expect("engine runtime lock");
            let docs_after = runtime_after
                .persisted_tables
                .get("docs")
                .expect("persisted docs after insert");
            assert!(
                docs_after.pointer.is_table_paged_manifest(),
                "inserted table should remain paged"
            );
            assert!(
                docs_after.pk_index_root.is_some(),
                "inserted paged table should retain persistent pk locator root"
            );
            let page_store = PagerReadStore { db: &db };
            let manifest_after = read_overflow(&page_store, docs_after.pointer)
                .expect("read paged manifest after insert");
            let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
                .expect("decode paged manifest after insert");
            assert_eq!(
                manifest_after
                    .chunks
                    .iter()
                    .map(|chunk| chunk.row_count)
                    .sum::<usize>(),
                97,
                "paged manifest row counts should reflect the insert"
            );
            manifest_after
                .chunks
                .iter()
                .filter_map(|chunk| {
                    untouched_chunk_pointers
                        .contains(&chunk.pointer)
                        .then_some(chunk.pointer)
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "untouched paged chunks should retain their original pointers after reopen-time INSERT RETURNING"
        );
    }

    #[test]
    fn paged_row_storage_insert_on_conflict_do_nothing_after_reopen_preserves_untouched_chunks() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-insert-on-conflict-do-nothing-after-reopen.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let untouched_chunk_pointers = {
            let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
            let json_open = db.inspect_storage_state_json().expect("json at reopen");
            assert!(
                json_open.contains("\"deferred_table_count\":1"),
                "expected paged-backed table to stay deferred at reopen, got: {json_open}"
            );

            let untouched_chunk_pointers = {
                let runtime_before = db.inner.engine.read().expect("engine runtime lock");
                let docs_before = runtime_before
                    .persisted_tables
                    .get("docs")
                    .expect("persisted docs before insert");
                let page_store = PagerReadStore { db: &db };
                let manifest_before = read_overflow(&page_store, docs_before.pointer)
                    .expect("read paged manifest before insert");
                let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                    .expect("decode paged manifest before insert");
                assert!(
                    manifest_before.chunks.len() > 2,
                    "expected multiple chunks before insert"
                );
                manifest_before
                    .chunks
                    .iter()
                    .take(manifest_before.chunks.len() - 1)
                    .map(|chunk| chunk.pointer)
                    .collect::<Vec<_>>()
            };

            let affected = db
                .execute_with_params(
                    "INSERT INTO docs VALUES (97, 9600 + 1, $1) ON CONFLICT(id) DO NOTHING",
                    &[Value::Text("z".repeat(2048))],
                )
                .expect("insert on conflict do nothing after reopen");
            assert_eq!(affected.affected_rows(), 1);

            let json_after = db
                .inspect_storage_state_json()
                .expect("json after on conflict insert");
            assert!(
                json_after.contains("\"loaded_table_count\":0"),
                "expected INSERT .. ON CONFLICT DO NOTHING to avoid resident table loads, got: {json_after}"
            );
            assert!(
                json_after.contains("\"deferred_table_count\":1"),
                "expected inserted paged table to remain deferred after ON CONFLICT insert, got: {json_after}"
            );

            untouched_chunk_pointers
        };

        let db = Db::open_or_create(&path, config).expect("reopen mutated db");
        let preserved_untouched = {
            let runtime_after = db.inner.engine.read().expect("engine runtime lock");
            let docs_after = runtime_after
                .persisted_tables
                .get("docs")
                .expect("persisted docs after insert");
            assert!(
                docs_after.pointer.is_table_paged_manifest(),
                "inserted table should remain paged"
            );
            let page_store = PagerReadStore { db: &db };
            let manifest_after = read_overflow(&page_store, docs_after.pointer)
                .expect("read paged manifest after insert");
            let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
                .expect("decode paged manifest after insert");
            assert_eq!(
                manifest_after
                    .chunks
                    .iter()
                    .map(|chunk| chunk.row_count)
                    .sum::<usize>(),
                97,
                "paged manifest row counts should reflect the insert"
            );
            manifest_after
                .chunks
                .iter()
                .filter_map(|chunk| {
                    untouched_chunk_pointers
                        .contains(&chunk.pointer)
                        .then_some(chunk.pointer)
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "untouched paged chunks should retain their original pointers after reopen-time INSERT .. ON CONFLICT DO NOTHING"
        );
    }

    #[test]
    fn paged_row_storage_insert_on_conflict_do_update_after_reopen_keeps_table_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-insert-on-conflict-do-update-after-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed table to stay deferred at reopen, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at reopen, got: {json_open}"
        );

        let returning = db
            .execute_with_params(
                "INSERT INTO docs VALUES (1, 100, $1) \
                 ON CONFLICT(id) DO UPDATE SET n = excluded.n + 1 \
                 RETURNING n",
                &[Value::Text("z".repeat(2048))],
            )
            .expect("insert on conflict do update after reopen");
        assert_eq!(scalar_i64(&returning), 101);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after upsert update");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected INSERT .. ON CONFLICT DO UPDATE to avoid resident table loads, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected INSERT .. ON CONFLICT DO UPDATE to keep the paged table deferred, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT n FROM docs WHERE id = 1")
                    .expect("point lookup after upsert update")
            ),
            101
        );
        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM docs").expect("count docs")),
            96
        );
    }

    #[test]
    fn paged_row_storage_insert_on_conflict_parent_key_update_with_setnull_fk_after_reopen_keeps_tables_deferred(
    ) {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-upsert-parent-key-update-setnull-fk.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code INTEGER, body TEXT)")
                .expect("create parent");
            db.execute("CREATE UNIQUE INDEX parent_code_idx ON parent(code)")
                .expect("create parent code index");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_code INTEGER REFERENCES parent(code) ON UPDATE SET NULL, body TEXT)",
            )
            .expect("create child");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            for i in 0_i64..32_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(parent_body.clone()),
                        ],
                    )
                    .expect("insert parent row");
                if i < 16 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":2"),
            "expected parent and child tables to stay deferred at reopen, got: {json_open}"
        );

        let returning = db
            .execute_with_params(
                "INSERT INTO parent VALUES (1, 1001, $1) \
                 ON CONFLICT(id) DO UPDATE SET code = excluded.code \
                 RETURNING code",
                &[Value::Text("z".repeat(2048))],
            )
            .expect("parent-key upsert after reopen");
        assert_eq!(scalar_i64(&returning), 1001);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after parent-key upsert");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected parent-key upsert to re-defer loaded tables, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected parent-key upsert to keep parent and child deferred, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT code FROM parent WHERE id = 1")
                    .expect("lookup parent code")
            ),
            1001
        );
        let child_parent = db
            .execute("SELECT parent_code FROM child WHERE id = 1")
            .expect("lookup child parent")
            .rows()[0]
            .values()[0]
            .clone();
        assert_eq!(child_parent, Value::Null);
    }

    #[test]
    fn paged_row_storage_insert_on_conflict_foreign_key_update_after_reopen_keeps_tables_deferred()
    {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-upsert-foreign-key-update.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create parent");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), body TEXT)",
            )
            .expect("create child");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            for i in 0_i64..96_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                    )
                    .expect("insert parent row");
                if i < 48 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":2"),
            "expected parent and child tables to stay deferred at reopen, got: {json_open}"
        );

        let returning = db
            .execute_with_params(
                "INSERT INTO child VALUES (1, 2, $1) \
                 ON CONFLICT(id) DO UPDATE SET parent_id = excluded.parent_id \
                 RETURNING parent_id",
                &[Value::Text("z".repeat(2048))],
            )
            .expect("foreign-key upsert after reopen");
        assert_eq!(scalar_i64(&returning), 2);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after foreign-key upsert");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected foreign-key upsert to re-defer loaded tables, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected parent and child tables to remain deferred, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT parent_id FROM child WHERE id = 1")
                    .expect("lookup child parent id")
            ),
            2
        );
    }

    #[test]
    fn paged_row_storage_insert_select_returning_after_reopen_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-insert-select-returning-after-reopen.ddb");
        let config = DbConfig {
            persistent_pk_index: true,
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive table");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let untouched_chunk_pointers = {
            let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
            let json_open = db.inspect_storage_state_json().expect("json at reopen");
            assert!(
                json_open.contains("\"loaded_table_count\":0"),
                "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
            );
            assert!(
                json_open.contains("\"deferred_table_count\":2"),
                "expected both paged-backed tables to stay deferred at reopen, got: {json_open}"
            );

            let untouched_chunk_pointers = {
                let runtime_before = db.inner.engine.read().expect("engine runtime lock");
                let docs_before = runtime_before
                    .persisted_tables
                    .get("docs")
                    .expect("persisted docs before insert");
                let page_store = PagerReadStore { db: &db };
                let manifest_before = read_overflow(&page_store, docs_before.pointer)
                    .expect("read paged manifest before insert");
                let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                    .expect("decode paged manifest before insert");
                assert!(
                    manifest_before.chunks.len() > 2,
                    "expected multiple docs chunks before insert"
                );
                manifest_before
                    .chunks
                    .iter()
                    .take(manifest_before.chunks.len() - 1)
                    .map(|chunk| chunk.pointer)
                    .collect::<Vec<_>>()
            };

            let returning = db
                .execute(
                    "INSERT INTO docs \
                     SELECT doc_id + 96, doc_id + 1000, note \
                     FROM archive \
                     WHERE id = 1 \
                     RETURNING n",
                )
                .expect("insert select returning after reopen");
            assert_eq!(scalar_i64(&returning), 1001);

            let json_after = db
                .inspect_storage_state_json()
                .expect("json after insert select returning");
            assert!(
                json_after.contains("\"loaded_table_count\":0"),
                "expected INSERT .. SELECT RETURNING to avoid resident table loads, got: {json_after}"
            );
            assert!(
                json_after.contains("\"deferred_table_count\":2"),
                "expected INSERT .. SELECT RETURNING to keep both paged tables deferred, got: {json_after}"
            );

            untouched_chunk_pointers
        };

        let db = Db::open_or_create(&path, config).expect("reopen mutated db");
        let preserved_untouched = {
            let runtime_after = db.inner.engine.read().expect("engine runtime lock");
            let docs_after = runtime_after
                .persisted_tables
                .get("docs")
                .expect("persisted docs after insert");
            assert!(
                docs_after.pointer.is_table_paged_manifest(),
                "inserted table should remain paged"
            );
            let page_store = PagerReadStore { db: &db };
            let manifest_after = read_overflow(&page_store, docs_after.pointer)
                .expect("read paged manifest after insert");
            let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
                .expect("decode paged manifest after insert");
            assert_eq!(
                manifest_after
                    .chunks
                    .iter()
                    .map(|chunk| chunk.row_count)
                    .sum::<usize>(),
                97,
                "paged manifest row counts should reflect the insert"
            );
            manifest_after
                .chunks
                .iter()
                .filter_map(|chunk| {
                    untouched_chunk_pointers
                        .contains(&chunk.pointer)
                        .then_some(chunk.pointer)
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "untouched paged docs chunks should retain their original pointers after reopen-time INSERT .. SELECT RETURNING"
        );

        let inserted = db
            .execute("SELECT n FROM docs WHERE id = 97")
            .expect("point lookup after insert select");
        assert_eq!(scalar_i64(&inserted), 1001);
    }

    #[test]
    fn paged_row_storage_union_all_after_reopen_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-union-all-after-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":2"),
            "expected both paged-backed tables deferred at reopen, got: {json_open}"
        );

        let result = db
            .execute(
                "SELECT id FROM docs WHERE id = 1 \
                 UNION ALL \
                 SELECT doc_id FROM archive WHERE id = 2 \
                 ORDER BY id",
            )
            .expect("union all query after reopen");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(1)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(2)]);

        let json_after = db.inspect_storage_state_json().expect("json after union");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected UNION ALL to avoid resident table loads, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected UNION ALL to keep both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_subquery_after_reopen_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-subquery-after-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":2"),
            "expected both paged-backed tables deferred at reopen, got: {json_open}"
        );

        let result = db
            .execute(
                "SELECT id FROM docs \
                 WHERE id IN (SELECT doc_id FROM archive WHERE id = 2)",
            )
            .expect("subquery after reopen");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after subquery");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected subquery execution to avoid resident table loads, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected subquery execution to keep both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_from_subquery_after_reopen_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-from-subquery-after-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":2"),
            "expected both paged-backed tables deferred at reopen, got: {json_open}"
        );

        let result = db
            .execute(
                "SELECT q.id, archive.note \
                 FROM (SELECT id FROM docs WHERE id = 2) AS q \
                 JOIN archive ON archive.doc_id = q.id",
            )
            .expect("from-subquery join after reopen");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values()[0], Value::Int64(2));

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after from-subquery");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected FROM-subquery execution to avoid resident table loads, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected FROM-subquery execution to keep both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_insert_returning_keeps_paged_runtime_state() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare seed insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert seed row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction().expect("begin sql transaction");
        let json_before = db
            .inspect_storage_state_json()
            .expect("inspect state at txn begin");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected no eager resident load at BEGIN, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred at BEGIN, got: {json_before}"
        );

        let inserted = db
            .execute_with_params(
                "INSERT INTO docs VALUES ($1, $2, $3) RETURNING n",
                &[
                    Value::Int64(97),
                    Value::Int64(9600),
                    Value::Text("z".repeat(2048)),
                ],
            )
            .expect("insert inside shared sql transaction");
        assert_eq!(scalar_i64(&inserted), 9600);
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert_eq!(
                state.runtime.tables.len(),
                1,
                "expected transaction runtime to load only the target table for INSERT RETURNING"
            );
            assert!(matches!(
                state.runtime.tables.get("docs"),
                Some(TableRowSource::Paged(_))
            ));
        }
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count inside shared txn")
            ),
            97
        );
        db.commit_transaction().expect("commit shared txn");

        let reopened = Db::open_or_create(
            &path,
            DbConfig {
                paged_row_storage: true,
                ..DbConfig::default()
            },
        )
        .expect("reopen committed db");
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT COUNT(*) FROM docs")
                    .expect("count after shared txn commit")
            ),
            97
        );
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_insert_on_conflict_do_update_keeps_paged_runtime_state(
    ) {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-upsert-update.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                1,
                "expected paged table deferred at BEGIN"
            );
        }

        let returning = db
            .execute_with_params(
                "INSERT INTO docs VALUES (1, 100, $1) \
                 ON CONFLICT(id) DO UPDATE SET n = excluded.n + 1 \
                 RETURNING n",
                &[Value::Text("z".repeat(2048))],
            )
            .expect("insert on conflict do update inside shared sql transaction");
        assert_eq!(scalar_i64(&returning), 101);
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert_eq!(
                state.runtime.tables.len(),
                1,
                "expected transaction runtime to load only the target table for INSERT .. ON CONFLICT DO UPDATE"
            );
            assert!(matches!(
                state.runtime.tables.get("docs"),
                Some(TableRowSource::Paged(_))
            ));
        }
        db.commit_transaction().expect("commit shared txn");

        let reopened = Db::open_or_create(
            &path,
            DbConfig {
                paged_row_storage: true,
                ..DbConfig::default()
            },
        )
        .expect("reopen committed db");
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT n FROM docs WHERE id = 1")
                    .expect("point lookup after shared txn commit")
            ),
            101
        );
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_insert_on_conflict_parent_key_update_keeps_paged_runtime_state(
    ) {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-upsert-parent-key.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code INTEGER, body TEXT)")
                .expect("create parent");
            db.execute("CREATE UNIQUE INDEX parent_code_idx ON parent(code)")
                .expect("create parent code index");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_code INTEGER REFERENCES parent(code) ON UPDATE SET NULL, body TEXT)",
            )
            .expect("create child");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            for i in 0_i64..32_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(parent_body.clone()),
                        ],
                    )
                    .expect("insert parent row");
                if i < 16 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected parent and child tables deferred at BEGIN"
            );
        }

        let returning = db
            .execute_with_params(
                "INSERT INTO parent VALUES (1, 1001, $1) \
                 ON CONFLICT(id) DO UPDATE SET code = excluded.code \
                 RETURNING code",
                &[Value::Text("z".repeat(2048))],
            )
            .expect("parent-key upsert inside shared sql transaction");
        assert_eq!(scalar_i64(&returning), 1001);
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert_eq!(
                state.runtime.tables.len(),
                2,
                "expected transaction runtime to load target and child tables for parent-key INSERT .. ON CONFLICT DO UPDATE"
            );
            assert!(matches!(
                state.runtime.tables.get("parent"),
                Some(TableRowSource::Paged(_))
            ));
            assert!(matches!(
                state.runtime.tables.get("child"),
                Some(TableRowSource::Paged(_))
            ));
        }
        db.commit_transaction().expect("commit shared txn");

        let reopened = Db::open_or_create(
            &path,
            DbConfig {
                paged_row_storage: true,
                ..DbConfig::default()
            },
        )
        .expect("reopen committed db");
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT code FROM parent WHERE id = 1")
                    .expect("parent after shared txn commit")
            ),
            1001
        );
        let child_parent = reopened
            .execute("SELECT parent_code FROM child WHERE id = 1")
            .expect("child after shared txn commit")
            .rows()[0]
            .values()[0]
            .clone();
        assert_eq!(child_parent, Value::Null);
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_insert_on_conflict_foreign_key_update_keeps_targeted_runtime_state(
    ) {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-upsert-foreign-key.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create parent");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), body TEXT)",
            )
            .expect("create child");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            for i in 0_i64..96_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                    )
                    .expect("insert parent row");
                if i < 48 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected parent and child tables deferred at BEGIN"
            );
        }

        let returning = db
            .execute_with_params(
                "INSERT INTO child VALUES (1, 2, $1) \
                 ON CONFLICT(id) DO UPDATE SET parent_id = excluded.parent_id \
                 RETURNING parent_id",
                &[Value::Text("z".repeat(2048))],
            )
            .expect("foreign-key upsert inside shared sql transaction");
        assert_eq!(scalar_i64(&returning), 2);
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert_eq!(
                state.runtime.tables.len(),
                2,
                "expected transaction runtime to load target and parent tables for foreign-key INSERT .. ON CONFLICT DO UPDATE"
            );
            assert!(
                state.runtime.tables.contains_key("child"),
                "expected child table to be loaded for foreign-key upsert"
            );
            assert!(
                state.runtime.tables.contains_key("parent"),
                "expected parent table to be loaded for foreign-key validation"
            );
        }
        db.commit_transaction().expect("commit shared txn");

        let reopened = Db::open_or_create(
            &path,
            DbConfig {
                paged_row_storage: true,
                ..DbConfig::default()
            },
        )
        .expect("reopen committed db");
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT parent_id FROM child WHERE id = 1")
                    .expect("child after shared txn commit")
            ),
            2
        );
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_insert_select_returning_keeps_paged_runtime_state()
    {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-insert-select.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive table");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected both paged tables deferred at BEGIN"
            );
        }

        let inserted = db
            .execute(
                "INSERT INTO docs \
                 SELECT doc_id + 96, doc_id + 1000, note \
                 FROM archive \
                 WHERE id = 1 \
                 RETURNING n",
            )
            .expect("insert select returning inside shared sql transaction");
        assert_eq!(scalar_i64(&inserted), 1001);
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert_eq!(
                state.runtime.tables.len(),
                2,
                "expected transaction runtime to load only target and source tables for INSERT .. SELECT RETURNING"
            );
            assert!(matches!(
                state.runtime.tables.get("docs"),
                Some(TableRowSource::Paged(_))
            ));
            assert!(matches!(
                state.runtime.tables.get("archive"),
                Some(TableRowSource::Paged(_))
            ));
        }
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count inside shared txn")
            ),
            97
        );
        db.commit_transaction().expect("commit shared txn");

        let reopened = Db::open_or_create(
            &path,
            DbConfig {
                paged_row_storage: true,
                ..DbConfig::default()
            },
        )
        .expect("reopen committed db");
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT n FROM docs WHERE id = 97")
                    .expect("point lookup after shared txn commit")
            ),
            1001
        );
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_union_all_keeps_runtime_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-union-all.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin seed txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected both paged tables deferred at BEGIN"
            );
        }

        let result = db
            .execute(
                "SELECT id FROM docs WHERE id = 1 \
                 UNION ALL \
                 SELECT doc_id FROM archive WHERE id = 2 \
                 ORDER BY id",
            )
            .expect("shared transaction union all");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(1)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(2)]);

        {
            let txn = db
                .inner
                .sql_txn
                .lock()
                .expect("lock shared txn slot after union");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected UNION ALL fast path to avoid loading transaction tables"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected UNION ALL fast path to keep both tables deferred in the transaction runtime"
            );
        }
        db.commit_transaction().expect("commit shared txn");
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_subquery_keeps_runtime_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-subquery.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin seed txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected both paged tables deferred at BEGIN"
            );
        }

        let result = db
            .execute(
                "SELECT id FROM docs \
                 WHERE id IN (SELECT doc_id FROM archive WHERE id = 2)",
            )
            .expect("shared transaction subquery");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);

        {
            let txn = db
                .inner
                .sql_txn
                .lock()
                .expect("lock shared txn slot after subquery");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected subquery fast path to avoid loading transaction tables"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected subquery fast path to keep both tables deferred in the transaction runtime"
            );
        }
        db.commit_transaction().expect("commit shared txn");
    }

    #[test]
    fn paged_row_storage_cte_after_reopen_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-cte-after-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "WITH scoped AS (SELECT id, n FROM docs WHERE n >= 90) \
                 SELECT id FROM scoped WHERE n < 95 ORDER BY id",
            )
            .expect("query non-recursive cte");
        assert_eq!(
            result
                .rows()
                .iter()
                .map(|row| row.values()[0].clone())
                .collect::<Vec<_>>(),
            vec![
                Value::Int64(91),
                Value::Int64(92),
                Value::Int64(93),
                Value::Int64(94),
                Value::Int64(95),
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after cte");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected non-recursive CTE query to avoid live-runtime table loads, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected docs to remain deferred after non-recursive CTE query, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_values_cte_after_reopen_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-values-cte-after-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "WITH threshold(v) AS (VALUES (90)) \
                 SELECT id FROM docs WHERE n >= (SELECT v FROM threshold) ORDER BY id LIMIT 3",
            )
            .expect("query VALUES CTE");
        assert_eq!(
            result
                .rows()
                .iter()
                .map(|row| row.values()[0].clone())
                .collect::<Vec<_>>(),
            vec![Value::Int64(91), Value::Int64(92), Value::Int64(93)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after values cte");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected VALUES CTE query to avoid live-runtime table loads, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected docs to remain deferred after VALUES CTE query, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_cte_keeps_runtime_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-cte.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                1,
                "expected docs deferred at BEGIN"
            );
        }

        let result = db
            .execute(
                "WITH scoped AS (SELECT id, n FROM docs WHERE n >= 90) \
                 SELECT id FROM scoped WHERE n < 95 ORDER BY id",
            )
            .expect("shared transaction CTE query");
        assert_eq!(result.rows().len(), 5);

        {
            let txn = db
                .inner
                .sql_txn
                .lock()
                .expect("lock shared txn slot after cte");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected CTE fast path to avoid loading transaction tables"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                1,
                "expected CTE fast path to keep docs deferred in the transaction runtime"
            );
        }
        db.commit_transaction().expect("commit shared txn");
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_from_subquery_keeps_runtime_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-from-subquery.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin seed txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected both paged tables deferred at BEGIN"
            );
        }

        let result = db
            .execute(
                "SELECT q.id, archive.note \
                 FROM (SELECT id FROM docs WHERE id = 2) AS q \
                 JOIN archive ON archive.doc_id = q.id",
            )
            .expect("shared transaction from-subquery");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values()[0], Value::Int64(2));

        {
            let txn = db
                .inner
                .sql_txn
                .lock()
                .expect("lock shared txn slot after from-subquery");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected FROM-subquery fast path to avoid loading transaction tables"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected FROM-subquery fast path to keep both tables deferred in the transaction runtime"
            );
        }
        db.commit_transaction().expect("commit shared txn");
    }

    #[test]
    fn dump_sql_keeps_deferred_paged_tables_unloaded_after_reopen() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("dump-sql-keeps-deferred-paged-tables-unloaded.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );

        let dump = db.dump_sql().expect("dump sql");
        assert!(
            dump.contains("CREATE TABLE \"docs\""),
            "dump missing table DDL: {dump}"
        );
        assert!(
            dump.contains("INSERT INTO \"docs\""),
            "dump missing row inserts: {dump}"
        );

        let json_after = db.inspect_storage_state_json().expect("json after dump");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected dump_sql to avoid live materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected dump_sql to keep paged-backed table deferred, got: {json_after}"
        );
    }

    #[test]
    fn dump_sql_in_shared_sql_transaction_includes_deferred_rows_without_loading_runtime() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("dump-sql-shared-transaction-deferred-rows.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                1,
                "expected paged table deferred at BEGIN"
            );
        }

        let dump = db.dump_sql().expect("dump sql in shared txn");
        assert!(
            dump.contains("CREATE TABLE \"docs\""),
            "dump missing table DDL: {dump}"
        );
        assert!(
            dump.contains("INSERT INTO \"docs\""),
            "dump missing row inserts: {dump}"
        );

        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected dump_sql to keep the shared transaction runtime deferred"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                1,
                "expected dump_sql to leave the shared transaction table deferred"
            );
        }
        db.commit_transaction().expect("commit shared txn");
    }

    #[test]
    fn integrity_check_keeps_deferred_paged_tables_unloaded_after_reopen() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("integrity-check-keeps-deferred-paged-tables-unloaded.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );

        let result = db
            .execute("PRAGMA integrity_check")
            .expect("integrity check");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Text("ok".to_string())]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after integrity check");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected integrity_check to avoid live materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected integrity_check to keep paged-backed table deferred, got: {json_after}"
        );
    }

    #[test]
    fn integrity_check_in_shared_sql_transaction_keeps_runtime_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("integrity-check-shared-transaction-deferred.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                1,
                "expected paged table deferred at BEGIN"
            );
        }

        let result = db
            .execute("PRAGMA integrity_check")
            .expect("integrity check");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Text("ok".to_string())]);

        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected integrity_check to keep the shared transaction runtime deferred"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                1,
                "expected integrity_check to leave the shared transaction table deferred"
            );
        }
        db.commit_transaction().expect("commit shared txn");
    }

    #[test]
    fn bulk_load_keeps_deferred_paged_tables_unloaded_after_reopen() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("bulk-load-keeps-deferred-paged-tables-unloaded.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );

        let inserted = db
            .bulk_load_rows(
                "docs",
                &["id", "n", "body"],
                &[vec![
                    Value::Int64(97),
                    Value::Int64(9600),
                    Value::Text("z".repeat(2048)),
                ]],
                BulkLoadOptions::default(),
            )
            .expect("bulk load rows");
        assert_eq!(inserted, 1);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after bulk load");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected bulk_load_rows to avoid live materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected bulk_load_rows to keep paged-backed table deferred, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count after bulk load")
            ),
            97
        );
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_indexed_join_keeps_runtime_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin seed txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected both paged tables deferred at BEGIN"
            );
        }

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8",
            )
            .expect("shared transaction indexed join");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(8),
                Value::Text(format!("{}-7", "y".repeat(1024)))
            ]
        );

        {
            let txn = db
                .inner
                .sql_txn
                .lock()
                .expect("lock shared txn slot after join");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected indexed join fast path to avoid loading transaction tables"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected indexed join fast path to keep both tables deferred in the transaction runtime"
            );
        }

        db.commit_transaction()
            .expect("commit shared sql transaction");
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_indexed_left_join_keeps_runtime_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-left-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin seed txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                if i < 48 {
                    archive_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(format!("{archive_note}-{i}")),
                            ],
                        )
                        .expect("insert archive row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected both paged tables deferred at BEGIN"
            );
        }

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 LEFT JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 96",
            )
            .expect("shared transaction indexed left join");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(96), Value::Null]);

        {
            let txn = db
                .inner
                .sql_txn
                .lock()
                .expect("lock shared txn slot after join");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected indexed left join fast path to avoid loading transaction tables"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected indexed left join fast path to keep both tables deferred in the transaction runtime"
            );
        }

        db.commit_transaction()
            .expect("commit shared sql transaction");
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_generic_join_keeps_runtime_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-generic-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin seed txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected both paged tables deferred at BEGIN"
            );
        }

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id >= 8 AND docs.id < 10 \
                 ORDER BY docs.id",
            )
            .expect("shared transaction generic join");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(8),
                Value::Text(format!("{}-7", "y".repeat(1024)))
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(9),
                Value::Text(format!("{}-8", "y".repeat(1024)))
            ]
        );

        {
            let txn = db
                .inner
                .sql_txn
                .lock()
                .expect("lock shared txn slot after join");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected generic join path to avoid loading transaction tables"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected generic join path to keep both tables deferred in the transaction runtime"
            );
        }

        db.commit_transaction()
            .expect("commit shared sql transaction");
    }

    #[test]
    fn paged_row_storage_exclusive_sql_transaction_uses_deferred_tables_on_demand() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-exclusive-sql-transaction.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs table");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare seed insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert seed row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let mut txn = db.transaction().expect("begin exclusive sql transaction");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert in exclusive txn");
        insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(97),
                    Value::Int64(9601),
                    Value::Text("z".repeat(2048)),
                ],
            )
            .expect("insert inside exclusive sql transaction");
        let count = txn
            .prepare("SELECT COUNT(*) FROM docs")
            .expect("prepare count in exclusive txn");
        assert_eq!(
            scalar_i64(
                &count
                    .execute_in(&mut txn, &[])
                    .expect("count in exclusive txn")
            ),
            97
        );
        txn.commit().expect("commit exclusive txn");

        let reopened = Db::open_or_create(
            &path,
            DbConfig {
                paged_row_storage: true,
                ..DbConfig::default()
            },
        )
        .expect("reopen committed db");
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT COUNT(*) FROM docs")
                    .expect("count after exclusive txn commit")
            ),
            97
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_projection_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-indexed-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both join tables deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8",
            )
            .expect("indexed join projection query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(8),
                Value::Text(format!("{}-7", "y".repeat(1024)))
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join projection to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join projection to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_order_limit_offset_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-order-limit-offset.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("note-{i:02}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1001),
                        Value::Int64(8),
                        Value::Text("note-z".to_string()),
                    ],
                )
                .expect("insert duplicate archive row");
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both join tables deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8 \
                 ORDER BY archive.note DESC \
                 LIMIT 1 OFFSET 1",
            )
            .expect("indexed join projection query with ordering");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(8), Value::Text("note-07".to_string())]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join ordering path to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join ordering path to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_multi_order_by_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-multi-order.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            db.execute("INSERT INTO docs (id, body) VALUES (8, 'doc-8')")
                .expect("insert doc row");
            for (id, note) in [(1000, "note-z"), (1001, "note-z"), (1002, "note-a")] {
                db.execute(&format!(
                    "INSERT INTO archive (id, doc_id, note) VALUES ({id}, 8, '{note}')"
                ))
                .expect("insert archive row");
            }
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT docs.id, archive.id AS archive_id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8 \
                 ORDER BY archive.note DESC, archive_id ASC",
            )
            .expect("indexed join projection query with multi-order");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(8),
                Value::Int64(1000),
                Value::Text("note-z".to_string())
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(8),
                Value::Int64(1001),
                Value::Text("note-z".to_string())
            ]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[
                Value::Int64(8),
                Value::Int64(1002),
                Value::Text("note-a".to_string())
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join multi-order path to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join multi-order path to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_distinct_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-distinct.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            db.execute("INSERT INTO docs (id, body) VALUES (8, 'doc-8')")
                .expect("insert doc row");
            for (id, note) in [(1000, "note-z"), (1001, "note-z"), (1002, "note-a")] {
                db.execute(&format!(
                    "INSERT INTO archive (id, doc_id, note) VALUES ({id}, 8, '{note}')"
                ))
                .expect("insert archive row");
            }
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT DISTINCT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8 \
                 ORDER BY note DESC",
            )
            .expect("indexed join projection query with distinct");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(8), Value::Text("note-z".to_string())]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(8), Value::Text("note-a".to_string())]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join distinct path to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join distinct path to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_using_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-using.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX archive_id_idx ON archive(id)")
                .expect("create archive index");
            db.execute("INSERT INTO docs (id, body) VALUES (8, 'doc-8')")
                .expect("insert doc row");
            db.execute("INSERT INTO archive (id, note) VALUES (8, 'note-z')")
                .expect("insert archive row");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive USING (id) \
                 WHERE docs.id = 8 \
                 ORDER BY archive.note DESC",
            )
            .expect("indexed join using query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(8), Value::Text("note-z".to_string())]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join using path to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join using path to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_using_wildcard_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-using-wildcard.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX archive_id_idx ON archive(id)")
                .expect("create archive index");
            db.execute("INSERT INTO docs (id, body) VALUES (8, 'doc-8')")
                .expect("insert doc row");
            db.execute("INSERT INTO archive (id, note) VALUES (8, 'note-z')")
                .expect("insert archive row");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT * \
                 FROM docs \
                 JOIN archive USING (id) \
                 WHERE docs.id = 8 \
                 ORDER BY note DESC",
            )
            .expect("indexed join using wildcard query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(8),
                Value::Text("doc-8".to_string()),
                Value::Text("note-z".to_string()),
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join using wildcard to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join using wildcard to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_multi_column_using_wildcard_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-multi-column-using-wildcard.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute(
                "CREATE TABLE docs (pk INTEGER PRIMARY KEY, org_id INTEGER, id INTEGER, body TEXT)",
            )
            .expect("create docs");
            db.execute(
                "CREATE TABLE archive (archive_pk INTEGER PRIMARY KEY, org_id INTEGER, id INTEGER, note TEXT)",
            )
            .expect("create archive");
            db.execute("CREATE INDEX archive_org_id_id_idx ON archive(org_id, id)")
                .expect("create archive index");
            db.execute("INSERT INTO docs (pk, org_id, id, body) VALUES (1, 7, 8, 'doc-8')")
                .expect("insert doc row");
            db.execute(
                "INSERT INTO archive (archive_pk, org_id, id, note) VALUES (1, 7, 8, 'note-z')",
            )
            .expect("insert archive row");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT * \
                 FROM docs \
                 JOIN archive USING (org_id, id) \
                 ORDER BY note DESC",
            )
            .expect("indexed join multi-column using wildcard query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(7),
                Value::Int64(8),
                Value::Int64(1),
                Value::Text("doc-8".to_string()),
                Value::Int64(1),
                Value::Text("note-z".to_string()),
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join multi-column using wildcard to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join multi-column using wildcard to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_natural_join_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-natural-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute(
                "CREATE TABLE docs (pk INTEGER PRIMARY KEY, org_id INTEGER, id INTEGER, body TEXT)",
            )
            .expect("create docs");
            db.execute(
                "CREATE TABLE archive (pk INTEGER PRIMARY KEY, org_id INTEGER, id INTEGER, note TEXT)",
            )
            .expect("create archive");
            db.execute("CREATE INDEX archive_org_id_id_idx ON archive(org_id, id)")
                .expect("create archive index");
            db.execute("INSERT INTO docs (pk, org_id, id, body) VALUES (1, 7, 8, 'doc-8')")
                .expect("insert doc row");
            db.execute("INSERT INTO archive (pk, org_id, id, note) VALUES (1, 7, 8, 'note-z')")
                .expect("insert archive row");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT docs.pk, org_id, id, archive.note \
                 FROM docs NATURAL JOIN archive \
                 ORDER BY archive.note DESC",
            )
            .expect("indexed natural join query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(1),
                Value::Int64(7),
                Value::Int64(8),
                Value::Text("note-z".to_string()),
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed natural join to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed natural join to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_without_filter_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-without-filter.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..32_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i + 1), Value::Text("x".repeat(2048))],
                    )
                    .expect("insert docs row");
                if i < 24 {
                    archive_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(format!("note-{i:02}")),
                            ],
                        )
                        .expect("insert archive row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 ORDER BY docs.id ASC \
                 LIMIT 3",
            )
            .expect("indexed join without filter query");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Text("note-00".to_string())]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(2), Value::Text("note-01".to_string())]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[Value::Int64(3), Value::Text("note-02".to_string())]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join without filter path to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join without filter path to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_composite_indexed_join_without_filter_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-composite-indexed-join-without-filter.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, org_id INTEGER, body TEXT)")
                .expect("create docs");
            db.execute(
                "CREATE TABLE archive (id INTEGER PRIMARY KEY, org_id INTEGER, doc_id INTEGER, note TEXT)",
            )
            .expect("create archive");
            db.execute("CREATE INDEX idx_archive_org_doc ON archive(org_id, doc_id)")
                .expect("create composite archive join index");
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3, $4)")
                .expect("prepare archive insert");
            for i in 0_i64..24_i64 {
                let doc_id = i + 1;
                let org_id = if i < 12 { 10 } else { 20 };
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(doc_id),
                            Value::Int64(org_id),
                            Value::Text("x".repeat(2048)),
                        ],
                    )
                    .expect("insert docs row");
                if i < 18 {
                    archive_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(1000 + i),
                                Value::Int64(org_id),
                                Value::Int64(doc_id),
                                Value::Text(format!("note-{i:02}")),
                            ],
                        )
                        .expect("insert archive row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected composite join tables to stay deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id \
                 ORDER BY docs.id ASC \
                 LIMIT 3",
            )
            .expect("composite indexed join without filter query");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Text("note-00".to_string())]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(2), Value::Text("note-01".to_string())]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[Value::Int64(3), Value::Text("note-02".to_string())]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected composite indexed join without filter path to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected composite indexed join without filter path to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_composite_indexed_join_with_filter_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-composite-indexed-join-with-filter.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, org_id INTEGER, body TEXT)")
                .expect("create docs");
            db.execute(
                "CREATE TABLE archive (id INTEGER PRIMARY KEY, org_id INTEGER, doc_id INTEGER, note TEXT)",
            )
            .expect("create archive");
            db.execute("CREATE INDEX idx_archive_org_doc ON archive(org_id, doc_id)")
                .expect("create composite archive join index");
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3, $4)")
                .expect("prepare archive insert");
            for i in 0_i64..16_i64 {
                let doc_id = i + 1;
                let org_id = if i < 8 { 10 } else { 20 };
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(doc_id),
                            Value::Int64(org_id),
                            Value::Text("x".repeat(1024)),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(1000 + i),
                            Value::Int64(org_id),
                            Value::Int64(doc_id),
                            Value::Text(format!("note-{i:02}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id \
                 WHERE docs.id = 2",
            )
            .expect("composite indexed join with filter query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(2), Value::Text("note-01".to_string())]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected composite indexed join with filter path to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected composite indexed join with filter path to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_composite_hashed_join_with_filter_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-composite-hashed-join-with-filter.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, org_id INTEGER, body TEXT)")
                .expect("create docs");
            db.execute(
                "CREATE TABLE archive (id INTEGER PRIMARY KEY, org_id INTEGER, doc_id INTEGER, note TEXT)",
            )
            .expect("create archive");
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3, $4)")
                .expect("prepare archive insert");
            for i in 0_i64..16_i64 {
                let doc_id = i + 1;
                let org_id = if i < 8 { 10 } else { 20 };
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(doc_id),
                            Value::Int64(org_id),
                            Value::Text("x".repeat(1024)),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(1000 + i),
                            Value::Int64(org_id),
                            Value::Int64(doc_id),
                            Value::Text(format!("note-{i:02}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id \
                 WHERE docs.id = 2",
            )
            .expect("composite hashed join with filter query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(2), Value::Text("note-01".to_string())]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected composite hashed join with filter path to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected composite hashed join with filter path to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_left_join_without_filter_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-left-join-without-filter.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..16_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i + 1), Value::Text("x".repeat(1024))],
                    )
                    .expect("insert docs row");
                if i < 8 {
                    archive_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(format!("note-{i:02}")),
                            ],
                        )
                        .expect("insert archive row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 LEFT JOIN archive ON docs.id = archive.doc_id \
                 ORDER BY docs.id DESC \
                 LIMIT 1",
            )
            .expect("indexed left join without filter query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(16), Value::Null]);

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed left join without filter path to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed left join without filter path to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_expression_projection_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-expression.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..32_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("note-{i:02}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1001),
                        Value::Int64(8),
                        Value::Text("note-z".to_string()),
                    ],
                )
                .expect("insert duplicate archive row");
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT docs.id, UPPER(archive.note) AS note_key \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8 \
                 ORDER BY note_key DESC \
                 LIMIT 1 OFFSET 1",
            )
            .expect("indexed join expression projection query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(8), Value::Text("NOTE-07".to_string())]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join expression projection to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join expression projection to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_wildcard_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-wildcard.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT * \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8",
            )
            .expect("indexed join wildcard query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(8),
                Value::Int64(7),
                Value::Text("x".repeat(2048)),
                Value::Int64(8),
                Value::Int64(8),
                Value::Text(format!("{}-7", "y".repeat(1024))),
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join wildcard to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join wildcard to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_qualified_wildcard_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-qualified-wildcard.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT archive.* \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8",
            )
            .expect("indexed join qualified wildcard query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(8),
                Value::Int64(8),
                Value::Text(format!("{}-7", "y".repeat(1024))),
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join qualified wildcard to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join qualified wildcard to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_left_join_projection_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-left-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                if i < 48 {
                    archive_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(format!("{archive_note}-{i}")),
                            ],
                        )
                        .expect("insert archive row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both join tables deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 LEFT JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 96",
            )
            .expect("indexed left join projection query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(96), Value::Null]);

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed left join projection to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed left join projection to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_left_join_expression_projection_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-left-join-expression.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..32_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                if i < 16 {
                    archive_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(format!("note-{i:02}")),
                            ],
                        )
                        .expect("insert archive row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT docs.id, COALESCE(archive.note, 'missing') AS note_key \
                 FROM docs \
                 LEFT JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 32",
            )
            .expect("indexed left join expression projection query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(32), Value::Text("missing".to_string())]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed left join expression projection to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed left join expression projection to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_right_join_projection_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-right-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1001),
                        Value::Int64(999),
                        Value::Text("orphan".to_string()),
                    ],
                )
                .expect("insert unmatched archive row");
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both join tables deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.n, archive.id \
                 FROM docs \
                 RIGHT JOIN archive ON docs.id = archive.doc_id \
                 WHERE archive.id = 1001",
            )
            .expect("indexed right join projection query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Null, Value::Int64(1001)]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed right join projection to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed right join projection to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_generic_join_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-generic-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both join tables deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id >= 8 AND docs.id < 10 \
                 ORDER BY docs.id",
            )
            .expect("generic join query");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(8),
                Value::Text(format!("{}-7", "y".repeat(1024)))
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(9),
                Value::Text(format!("{}-8", "y".repeat(1024)))
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic join execution to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected generic join execution to leave both paged tables deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after generic join execution, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_join_expression_filter_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-join-expression-filter.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both join tables deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id + archive.doc_id >= 18 AND docs.id <= 10 \
                 ORDER BY docs.id",
            )
            .expect("indexed join expression filter query");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(9),
                Value::Text(format!("{}-8", "y".repeat(1024)))
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(10),
                Value::Text(format!("{}-9", "y".repeat(1024)))
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join expression filter to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join expression filter to leave both paged tables deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after indexed join expression filter, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_hashed_join_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-hashed-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both join tables deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id + archive.doc_id >= 18 AND docs.id <= 10 \
                 ORDER BY docs.id",
            )
            .expect("hashed join query");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(9),
                Value::Text(format!("{}-8", "y".repeat(1024)))
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(10),
                Value::Text(format!("{}-9", "y".repeat(1024)))
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected hashed join to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected hashed join to leave both paged tables deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after hashed join, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_generic_right_join_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-generic-right-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX archive_doc_idx ON archive (doc_id)")
                .expect("create archive doc index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1001),
                        Value::Int64(999),
                        Value::Text("orphan".to_string()),
                    ],
                )
                .expect("insert unmatched archive row");
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both join tables deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.id \
                 FROM docs \
                 RIGHT JOIN archive ON docs.id = archive.doc_id \
                 WHERE archive.id >= 1000 \
                 ORDER BY archive.id",
            )
            .expect("generic right join query");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Null, Value::Int64(1001)]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic right join execution to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected generic right join execution to leave both paged tables deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after generic right join execution, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_indexed_full_join_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-indexed-full-join.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
                .expect("create archive");
            db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
                .expect("create archive join index");
            let docs_body = "x".repeat(2048);
            let archive_note = "y".repeat(1024);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..2_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(docs_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
            }
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1000),
                        Value::Int64(1),
                        Value::Text(format!("{archive_note}-0")),
                    ],
                )
                .expect("insert archive row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1001),
                        Value::Int64(99),
                        Value::Text("orphan".to_string()),
                    ],
                )
                .expect("insert unmatched archive row");
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before join");
        assert!(
            json_before.contains("\"loaded_table_count\":0"),
            "expected join tables to stay deferred at reopen, got: {json_before}"
        );
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both join tables deferred at reopen, got: {json_before}"
        );

        let result = db
            .execute(
                "SELECT docs.id, archive.note, COALESCE(docs.id, archive.doc_id) AS sort_key \
                 FROM docs \
                 FULL JOIN archive ON docs.id = archive.doc_id \
                 ORDER BY sort_key",
            )
            .expect("indexed full join projection query");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(1),
                Value::Text(format!("{}-0", "y".repeat(1024))),
                Value::Int64(1)
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(2), Value::Null, Value::Int64(2)]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[
                Value::Null,
                Value::Text("orphan".to_string()),
                Value::Int64(99)
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after join");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed full join projection to avoid resident table materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed full join projection to leave both paged tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_benchmark_history_query_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-benchmark-history-query.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute(
                "CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    total_amount FLOAT64,
                    body TEXT
                )",
            )
            .expect("create orders");
            db.execute(
                "CREATE TABLE payments (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    status TEXT,
                    body TEXT
                )",
            )
            .expect("create payments");
            db.execute(
                "CREATE TABLE order_items (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    item_id INTEGER,
                    quantity INTEGER,
                    price FLOAT64,
                    body TEXT
                )",
            )
            .expect("create order_items");
            db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, body TEXT)")
                .expect("create items");
            db.execute("CREATE INDEX idx_orders_user_id ON orders(user_id)")
                .expect("create orders user index");
            db.execute("CREATE INDEX idx_payments_order_id ON payments(order_id)")
                .expect("create payments order index");
            db.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id)")
                .expect("create order_items order index");
            db.execute("CREATE INDEX idx_items_id ON items(id)")
                .expect("create items id index");

            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let order_insert = txn
                .prepare("INSERT INTO orders VALUES ($1, $2, $3, $4)")
                .expect("prepare order insert");
            let payment_insert = txn
                .prepare("INSERT INTO payments VALUES ($1, $2, $3, $4)")
                .expect("prepare payment insert");
            let order_item_insert = txn
                .prepare("INSERT INTO order_items VALUES ($1, $2, $3, $4, $5, $6)")
                .expect("prepare order item insert");
            let item_insert = txn
                .prepare("INSERT INTO items VALUES ($1, $2, $3)")
                .expect("prepare item insert");

            item_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1),
                        Value::Text("widget".to_string()),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert item 1");
            item_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(2),
                        Value::Text("gizmo".to_string()),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert item 2");

            order_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(10),
                        Value::Int64(7),
                        Value::Float64(42.0),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert order 10");
            order_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(11),
                        Value::Int64(7),
                        Value::Float64(84.0),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert order 11");

            payment_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1),
                        Value::Int64(10),
                        Value::Text("paid".to_string()),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert payment 1");
            payment_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(2),
                        Value::Int64(11),
                        Value::Text("paid".to_string()),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert payment 2");

            order_item_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1),
                        Value::Int64(10),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Float64(5.0),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert order item 1");
            order_item_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(2),
                        Value::Int64(11),
                        Value::Int64(2),
                        Value::Int64(3),
                        Value::Float64(7.5),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert order item 2");
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT orders.id, orders.total_amount, payments.status, items.name, order_items.quantity, order_items.price \
                 FROM ((orders JOIN payments ON orders.id = payments.order_id) \
                 JOIN order_items ON orders.id = order_items.order_id) \
                 JOIN items ON order_items.item_id = items.id \
                 WHERE orders.user_id = 7 \
                 ORDER BY orders.id DESC",
            )
            .expect("benchmark history query");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(11),
                Value::Float64(84.0),
                Value::Text("paid".to_string()),
                Value::Text("gizmo".to_string()),
                Value::Int64(3),
                Value::Float64(7.5),
            ]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after history query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected benchmark history query to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":4"),
            "expected benchmark history query to keep all tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_benchmark_report_query_keeps_deferred_tables_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-benchmark-report-query.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, body TEXT)")
                .expect("create items");
            db.execute(
                "CREATE TABLE order_items (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    item_id INTEGER,
                    quantity INTEGER,
                    price FLOAT64,
                    body TEXT
                )",
            )
            .expect("create order_items");
            db.execute(
                "CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    status TEXT,
                    body TEXT
                )",
            )
            .expect("create orders");
            db.execute("CREATE INDEX idx_orders_status ON orders(status)")
                .expect("create orders status index");
            db.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id)")
                .expect("create order_items order index");
            db.execute("CREATE INDEX idx_items_id ON items(id)")
                .expect("create items id index");

            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let item_insert = txn
                .prepare("INSERT INTO items VALUES ($1, $2, $3)")
                .expect("prepare item insert");
            let order_insert = txn
                .prepare("INSERT INTO orders VALUES ($1, $2, $3)")
                .expect("prepare order insert");
            let order_item_insert = txn
                .prepare("INSERT INTO order_items VALUES ($1, $2, $3, $4, $5, $6)")
                .expect("prepare order item insert");

            item_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1),
                        Value::Text("widget".to_string()),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert item 1");
            item_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(2),
                        Value::Text("gizmo".to_string()),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert item 2");

            order_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(10),
                        Value::Text("paid".to_string()),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert order 10");
            order_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(11),
                        Value::Text("paid".to_string()),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert order 11");

            order_item_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1),
                        Value::Int64(10),
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Float64(5.0),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert order item 1");
            order_item_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(2),
                        Value::Int64(11),
                        Value::Int64(2),
                        Value::Int64(3),
                        Value::Float64(7.5),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert order item 2");
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let result = db
            .execute(
                "SELECT items.name, SUM(order_items.quantity) AS total_quantity, SUM(order_items.quantity * order_items.price) AS revenue \
                 FROM ((items JOIN order_items ON items.id = order_items.item_id) \
                 JOIN orders ON order_items.order_id = orders.id) \
                 WHERE orders.status = 'paid' \
                 GROUP BY items.id, items.name \
                 ORDER BY revenue DESC \
                 LIMIT 2",
            )
            .expect("benchmark report query");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Text("gizmo".to_string()),
                Value::Int64(3),
                Value::Float64(22.5),
            ]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after report query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected benchmark report query to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":3"),
            "expected benchmark report query to keep all tables deferred, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_generic_literal_update_after_reopen_keeps_table_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-generic-literal-update.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.execute("UPDATE docs SET n = 777 WHERE n >= 90")
            .expect("generic literal update");

        let json_after = db.inspect_storage_state_json().expect("json after update");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic literal update to re-defer docs after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected docs to be deferred again after generic literal update, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs WHERE n = 777")
                    .expect("count updated rows")
            ),
            6
        );
    }

    #[test]
    fn paged_row_storage_generic_expression_update_after_reopen_keeps_table_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-generic-expression-update.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.execute("UPDATE docs SET n = n + 10 WHERE n >= 90")
            .expect("generic expression update");

        let json_after = db.inspect_storage_state_json().expect("json after update");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic expression update to re-defer docs after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected docs to be deferred again after generic expression update, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT n FROM docs WHERE id = 91")
                    .expect("read updated docs row")
            ),
            100
        );
    }

    #[test]
    fn paged_row_storage_generic_foreign_key_update_after_reopen_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-generic-foreign-key-update.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create parent");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), body TEXT)",
            )
            .expect("create child");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            for i in 0_i64..64_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                    )
                    .expect("insert parent row");
                if i < 32 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.execute("UPDATE child SET parent_id = parent_id + 1 WHERE id = 1")
            .expect("generic foreign-key update");

        let json_after = db.inspect_storage_state_json().expect("json after update");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic foreign-key update to re-defer loaded tables, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected parent and child tables to remain deferred, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT parent_id FROM child WHERE id = 1")
                    .expect("read updated child row")
            ),
            2
        );
    }

    #[test]
    fn paged_row_storage_shared_sql_transaction_generic_foreign_key_update_keeps_targeted_runtime_state(
    ) {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-shared-sql-transaction-foreign-key-update.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create parent");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), body TEXT)",
            )
            .expect("create child");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("seed txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            for i in 0_i64..96_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                    )
                    .expect("insert parent row");
                if i < 48 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.begin_transaction()
            .expect("begin shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert!(
                state.runtime.tables.is_empty(),
                "expected no transaction-local table loads at BEGIN"
            );
            assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected parent and child tables deferred at BEGIN"
            );
        }

        db.execute("UPDATE child SET parent_id = parent_id + 1 WHERE id = 1")
            .expect("generic foreign-key update inside shared sql transaction");
        {
            let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert_eq!(
                state.runtime.tables.len(),
                2,
                "expected transaction runtime to load child and parent tables for generic foreign-key update validation"
            );
            assert!(
                state.runtime.tables.contains_key("child"),
                "expected child table to be loaded for generic foreign-key update"
            );
            assert!(
                state.runtime.tables.contains_key("parent"),
                "expected parent table to be loaded for generic foreign-key update validation"
            );
        }
        db.commit_transaction().expect("commit shared txn");

        let reopened = Db::open_or_create(
            &path,
            DbConfig {
                paged_row_storage: true,
                ..DbConfig::default()
            },
        )
        .expect("reopen committed db");
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT parent_id FROM child WHERE id = 1")
                    .expect("child after shared txn commit")
            ),
            2
        );
    }

    #[test]
    fn paged_row_storage_generic_delete_after_reopen_keeps_table_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-generic-delete.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.execute("DELETE FROM docs WHERE n >= 90 AND n < 93")
            .expect("generic delete");

        let json_after = db.inspect_storage_state_json().expect("json after delete");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic delete to re-defer docs after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected docs to be deferred again after generic delete, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .expect("count docs rows")
            ),
            93
        );
    }

    #[test]
    fn paged_row_storage_generic_parent_key_update_with_setnull_fk_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-parent-key-update-setnull-fk.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code INTEGER, body TEXT)")
                .expect("create parent");
            db.execute("CREATE UNIQUE INDEX parent_code_idx ON parent(code)")
                .expect("create parent code index");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_code INTEGER REFERENCES parent(code) ON UPDATE SET NULL, body TEXT)",
            )
            .expect("create child");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            for i in 0_i64..96_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(parent_body.clone()),
                        ],
                    )
                    .expect("insert parent row");
                if i < 48 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.execute("UPDATE parent SET code = code + 1000 WHERE id = 1")
            .expect("generic parent-key update");

        let json_after = db.inspect_storage_state_json().expect("json after update");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic parent-key update with setnull fk to re-defer loaded tables, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both parent and child tables to remain deferred, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT code FROM parent WHERE id = 1")
                    .expect("read updated parent code")
            ),
            1001
        );
        let child_parent_code = db
            .execute("SELECT parent_code FROM child WHERE id = 1")
            .expect("read updated child row")
            .rows()
            .first()
            .and_then(|row| row.values().first())
            .cloned()
            .expect("child parent code");
        assert!(matches!(child_parent_code, Value::Null));
    }

    #[test]
    fn paged_row_storage_generic_delete_with_restrict_fk_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-generic-delete-restrict-fk.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create parent");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE RESTRICT, body TEXT)",
            )
            .expect("create child");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            for i in 0_i64..96_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                    )
                    .expect("insert parent row");
                if i < 48 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.execute("DELETE FROM parent WHERE id = 96")
            .expect("generic delete with restrict fk");

        let json_after = db.inspect_storage_state_json().expect("json after delete");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic delete with restrict fk to re-defer loaded tables, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both parent and child tables to remain deferred, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM parent")
                    .expect("count parent rows")
            ),
            95
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM child")
                    .expect("count child rows")
            ),
            48
        );
    }

    #[test]
    fn paged_row_storage_generic_delete_with_composite_restrict_fk_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-generic-delete-composite-restrict-fk.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (a INTEGER, b INTEGER, body TEXT, PRIMARY KEY(a, b))")
                .expect("create parent");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_a INTEGER, parent_b INTEGER, body TEXT, \
                 FOREIGN KEY(parent_a, parent_b) REFERENCES parent(a, b) ON DELETE RESTRICT)",
            )
            .expect("create child");
            db.execute("CREATE INDEX parent_a_idx ON parent(a)")
                .expect("create parent lookup index");
            db.execute("CREATE INDEX child_parent_ab_idx ON child(parent_a, parent_b)")
                .expect("create child composite fk index");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3, $4)")
                .expect("prepare child insert");
            for i in 0_i64..96_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(1000 + i),
                            Value::Text(parent_body.clone()),
                        ],
                    )
                    .expect("insert parent row");
                if i < 48 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Int64(1000 + i),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.execute("DELETE FROM parent WHERE a = 96")
            .expect("composite restrict delete");

        let json_after = db.inspect_storage_state_json().expect("json after delete");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected composite restrict delete to re-defer loaded tables, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both parent and child tables to remain deferred, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM parent")
                    .expect("count parent rows")
            ),
            95
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM child")
                    .expect("count child rows")
            ),
            48
        );
    }

    #[test]
    fn paged_row_storage_composite_foreign_key_insert_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-composite-foreign-key-insert.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (a INTEGER, b INTEGER, body TEXT, PRIMARY KEY(a, b))")
                .expect("create parent");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_a INTEGER, parent_b INTEGER, body TEXT, \
                 FOREIGN KEY(parent_a, parent_b) REFERENCES parent(a, b) ON DELETE RESTRICT)",
            )
            .expect("create child");
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
                .expect("prepare parent insert");
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1),
                        Value::Int64(2),
                        Value::Text("p".repeat(2048)),
                    ],
                )
                .expect("insert parent row");
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.execute("INSERT INTO child VALUES (1, 1, 2, 'child')")
            .expect("insert child row");

        let json_after = db.inspect_storage_state_json().expect("json after insert");
        assert!(
            !json_after.contains("\"loaded_table_count\":2"),
            "expected composite foreign-key insert to avoid loading both parent and child resident at once, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1")
                || json_after.contains("\"deferred_table_count\":2"),
            "expected at least one table to remain deferred after insert, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM child")
                    .expect("count child rows")
            ),
            1
        );
    }

    #[test]
    fn paged_row_storage_generic_delete_with_cascade_fk_keeps_tables_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-generic-delete-cascade-fk.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create parent");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE, body TEXT)",
            )
            .expect("create child");
            db.execute(
                "CREATE TABLE grandchild (id INTEGER PRIMARY KEY, child_id INTEGER REFERENCES child(id) ON DELETE CASCADE, body TEXT)",
            )
            .expect("create grandchild");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let grandchild_body = "g".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            let grandchild_insert = txn
                .prepare("INSERT INTO grandchild VALUES ($1, $2, $3)")
                .expect("prepare grandchild insert");
            for i in 0_i64..96_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                    )
                    .expect("insert parent row");
                if i < 48 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                    grandchild_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(grandchild_body.clone()),
                            ],
                        )
                        .expect("insert grandchild row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        db.execute("DELETE FROM parent WHERE id = 1")
            .expect("generic delete with cascade fk");

        let json_after = db.inspect_storage_state_json().expect("json after delete");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic delete with cascade fk to re-defer loaded tables, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":3"),
            "expected parent, child, and grandchild tables to remain deferred, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM parent")
                    .expect("count parent rows")
            ),
            95
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM child")
                    .expect("count child rows")
            ),
            47
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM grandchild")
                    .expect("count grandchild rows")
            ),
            47
        );
    }

    #[test]
    fn paged_row_storage_generic_delete_with_restrict_fk_violation_redefers_tables() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-generic-delete-restrict-fk-violation.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
                .expect("create parent");
            db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE RESTRICT, body TEXT)",
            )
            .expect("create child");
            let parent_body = "p".repeat(2048);
            let child_body = "c".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let parent_insert = txn
                .prepare("INSERT INTO parent VALUES ($1, $2)")
                .expect("prepare parent insert");
            let child_insert = txn
                .prepare("INSERT INTO child VALUES ($1, $2, $3)")
                .expect("prepare child insert");
            for i in 0_i64..96_i64 {
                parent_insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                    )
                    .expect("insert parent row");
                if i < 48 {
                    child_insert
                        .execute_in(
                            &mut txn,
                            &[
                                Value::Int64(i + 1),
                                Value::Int64(i + 1),
                                Value::Text(child_body.clone()),
                            ],
                        )
                        .expect("insert child row");
                }
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let err = db
            .execute("DELETE FROM parent WHERE id = 1")
            .expect_err("restrict fk delete should fail");
        assert!(
            err.to_string().contains("violates a foreign key"),
            "expected fk violation, got: {err}"
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after failed delete");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected failed generic delete with restrict fk to re-defer loaded tables, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both parent and child tables to remain deferred after failed delete, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM parent")
                    .expect("count parent rows")
            ),
            96
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT COUNT(*) FROM child")
                    .expect("count child rows")
            ),
            48
        );
    }

    #[test]
    fn generic_direct_update_after_reopen_only_loads_referenced_deferred_table() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("generic-direct-update-targeted-load.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create archive");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before update");
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both tables deferred at reopen, got: {json_before}"
        );

        db.execute("UPDATE docs SET n = n + 1 WHERE id = 6")
            .expect("generic direct update");

        let json_after = db.inspect_storage_state_json().expect("json after update");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic expression update to avoid resident table loads, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both paged tables to remain deferred after generic expression update, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT n FROM docs WHERE id = 6")
                    .expect("read updated docs row")
            ),
            6
        );
    }

    #[test]
    fn generic_prepared_update_after_reopen_only_loads_referenced_deferred_table() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("generic-prepared-update-targeted-load.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create docs");
            db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
                .expect("create archive");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let docs_insert = txn
                .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
                .expect("prepare docs insert");
            let archive_insert = txn
                .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
                .expect("prepare archive insert");
            for i in 0_i64..96_i64 {
                docs_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert docs row");
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert archive row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");
        let json_before = db.inspect_storage_state_json().expect("json before update");
        assert!(
            json_before.contains("\"deferred_table_count\":2"),
            "expected both tables deferred at reopen, got: {json_before}"
        );

        let prepared = db
            .prepare("UPDATE docs SET n = n + 1 WHERE id = $1")
            .expect("prepare generic update");
        prepared
            .execute(&[Value::Int64(6)])
            .expect("execute generic prepared update");

        let json_after = db.inspect_storage_state_json().expect("json after update");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected prepared generic expression update to avoid resident table loads, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both paged tables to remain deferred after prepared expression update, got: {json_after}"
        );
        assert_eq!(
            scalar_i64(
                &db.execute("SELECT n FROM docs WHERE id = 6")
                    .expect("read updated docs row")
            ),
            6
        );
    }

    #[test]
    fn paged_row_storage_grouped_count_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-grouped-count.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 3),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute("SELECT grp, COUNT(*) FROM seeded GROUP BY grp")
            .expect("grouped count");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(32)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(32)]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[Value::Int64(2), Value::Int64(32)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped count");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged grouped count to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped count, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged grouped count, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_grouped_numeric_aggregate_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-grouped-sum.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 2),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute(
                "SELECT grp, COUNT(*), SUM(n) FROM seeded WHERE n >= 10 AND n <= 19 GROUP BY grp",
            )
            .expect("grouped aggregate");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(5), Value::Int64(70)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(5), Value::Int64(75)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged grouped aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged grouped aggregate, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_grouped_numeric_aggregate_with_order_limit_offset_keeps_deferred_table_unloaded(
    ) {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-grouped-sum-ordered.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 2),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c, SUM(n) AS total FROM seeded WHERE n >= 10 AND n <= 19 GROUP BY grp ORDER BY grp DESC LIMIT 1 OFFSET 1",
            )
            .expect("ordered grouped aggregate");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(5), Value::Int64(70)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after ordered grouped aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered paged grouped aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after ordered grouped aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered paged grouped aggregate, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_grouped_numeric_aggregate_having_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-grouped-sum-having.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 2),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c, SUM(n) AS total FROM seeded WHERE n >= 10 AND n <= 19 GROUP BY grp HAVING c = 5 AND total > 70 ORDER BY total DESC LIMIT 1",
            )
            .expect("grouped aggregate with having");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(5), Value::Int64(75)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped aggregate with having");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged grouped aggregate with having to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped aggregate with having, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged grouped aggregate with having, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_grouped_avg_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-grouped-avg.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 2),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute(
                "SELECT grp, SUM(n) AS total, AVG(n) AS avg_n FROM seeded \
                 WHERE n >= 10 AND n <= 19 GROUP BY grp HAVING avg_n >= 14 \
                 ORDER BY avg_n DESC LIMIT 1",
            )
            .expect("grouped avg aggregate");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(75), Value::Float64(15.0)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped avg aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged grouped avg aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped avg aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged grouped avg aggregate, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_grouped_expression_bucket_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-grouped-expression-bucket.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 2),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute(
                "SELECT n / 5 AS bucket, COUNT(*) AS c, SUM(n) AS total FROM seeded \
                 WHERE n >= 10 AND n <= 19 GROUP BY n / 5 ORDER BY bucket",
            )
            .expect("grouped expression aggregate");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(2), Value::Int64(5), Value::Int64(60)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(3), Value::Int64(5), Value::Int64(85)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped expression aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged grouped expression aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped expression aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged grouped expression aggregate, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_grouped_wrapped_sum_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-grouped-wrapped-sum.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..32_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 2),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute(
                "SELECT grp, CAST(SUM(n) AS TEXT) AS total_text FROM seeded \
                 GROUP BY grp ORDER BY total_text DESC LIMIT 1",
            )
            .expect("grouped wrapped sum");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Text("256".to_string())]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped wrapped sum");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged grouped wrapped sum to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped wrapped sum, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged grouped wrapped sum, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_grouped_wrapped_max_expr_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-grouped-wrapped-max-expr.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, word TEXT, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for (id, grp, word) in [
                (0_i64, 0_i64, "hi"),
                (1, 0, ""),
                (2, 1, "hello"),
                (3, 1, "zebra"),
            ] {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(id),
                            Value::Int64(grp),
                            Value::Text(word.to_string()),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute(
                "SELECT grp, LENGTH(MAX(NULLIF(word, ''))) AS longest FROM seeded \
                 GROUP BY grp ORDER BY longest DESC LIMIT 1",
            )
            .expect("grouped wrapped max expr");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(5)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped wrapped max expr");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged grouped wrapped max expr to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped wrapped max expr, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged grouped wrapped max expr, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_multi_column_grouped_count_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-grouped-count-multi.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp_a INTEGER, grp_b INTEGER, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 2),
                            Value::Int64(i % 3),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute("SELECT grp_a, grp_b, COUNT(*) FROM seeded GROUP BY grp_a, grp_b")
            .expect("multi-column grouped count");
        assert_eq!(result.rows().len(), 6);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(0), Value::Int64(16)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(1), Value::Int64(16)]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[Value::Int64(0), Value::Int64(2), Value::Int64(16)]
        );
        assert_eq!(
            result.rows()[3].values(),
            &[Value::Int64(1), Value::Int64(0), Value::Int64(16)]
        );
        assert_eq!(
            result.rows()[4].values(),
            &[Value::Int64(0), Value::Int64(1), Value::Int64(16)]
        );
        assert_eq!(
            result.rows()[5].values(),
            &[Value::Int64(1), Value::Int64(2), Value::Int64(16)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after multi-column grouped count");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected multi-column paged grouped count to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after multi-column grouped count, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after multi-column grouped count, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_multi_column_grouped_numeric_aggregate_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-grouped-sum-multi.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp_a INTEGER, grp_b INTEGER, n INTEGER, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4, $5)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 2),
                            Value::Int64(i % 3),
                            Value::Int64(i),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute("SELECT grp_a, grp_b, COUNT(*), SUM(n) FROM seeded GROUP BY grp_a, grp_b")
            .expect("multi-column grouped aggregate");
        assert_eq!(result.rows().len(), 6);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(16),
                Value::Int64(720)
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(16),
                Value::Int64(736)
            ]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[
                Value::Int64(0),
                Value::Int64(2),
                Value::Int64(16),
                Value::Int64(752)
            ]
        );
        assert_eq!(
            result.rows()[3].values(),
            &[
                Value::Int64(1),
                Value::Int64(0),
                Value::Int64(16),
                Value::Int64(768)
            ]
        );
        assert_eq!(
            result.rows()[4].values(),
            &[
                Value::Int64(0),
                Value::Int64(1),
                Value::Int64(16),
                Value::Int64(784)
            ]
        );
        assert_eq!(
            result.rows()[5].values(),
            &[
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(16),
                Value::Int64(800)
            ]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after multi-column grouped aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected multi-column paged grouped aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after multi-column grouped aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after multi-column paged grouped aggregate, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_expression_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-expression-projection.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(format!("name-{i:03}")),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");

        let result = db
            .execute("SELECT n + 1 AS next_n FROM seeded ORDER BY n LIMIT 1")
            .expect("arithmetic projection");
        assert_eq!(scalar_i64(&result), 1);

        let json_after_arithmetic = db
            .inspect_storage_state_json()
            .expect("json after arithmetic projection");
        assert!(
            json_after_arithmetic.contains("\"loaded_table_count\":0"),
            "expected paged arithmetic projection to avoid materialization, got: {json_after_arithmetic}"
        );
        assert!(
            json_after_arithmetic.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after arithmetic projection, got: {json_after_arithmetic}"
        );
        assert!(
            json_after_arithmetic.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after arithmetic projection, got: {json_after_arithmetic}"
        );

        let upper_result = db
            .execute("SELECT UPPER(name) FROM seeded ORDER BY name LIMIT 1")
            .expect("function projection");
        assert_eq!(scalar_text(&upper_result), "NAME-000");

        let offset_result = db
            .execute("SELECT n + 1 AS next_n FROM seeded ORDER BY n DESC LIMIT 2 OFFSET 1")
            .expect("offset arithmetic projection");
        assert_eq!(offset_result.rows().len(), 2);
        assert_eq!(offset_result.rows()[0].values(), &[Value::Int64(95)]);
        assert_eq!(offset_result.rows()[1].values(), &[Value::Int64(94)]);

        let json_after_function = db
            .inspect_storage_state_json()
            .expect("json after function projection");
        assert!(
            json_after_function.contains("\"loaded_table_count\":0"),
            "expected paged function projection to avoid materialization, got: {json_after_function}"
        );
        assert!(
            json_after_function.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after function projection, got: {json_after_function}"
        );
        assert!(
            json_after_function.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after function projection, got: {json_after_function}"
        );
    }

    #[test]
    fn paged_row_storage_projection_multi_order_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-projection-multi-order.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for (id, grp, n) in [(0_i64, 0_i64, 2_i64), (1, 0, 1), (2, 1, 2), (3, 1, 1)] {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(id),
                            Value::Int64(grp),
                            Value::Int64(n),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute("SELECT grp, n FROM seeded ORDER BY grp DESC, n ASC LIMIT 3")
            .expect("projection multi-order");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(1)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(2)]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[Value::Int64(0), Value::Int64(1)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after projection multi-order");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged projection multi-order to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after projection multi-order, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after projection multi-order, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_virtual_generated_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-virtual-generated-projection.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (
                    id INTEGER PRIMARY KEY,
                    qty INTEGER,
                    price FLOAT64,
                    total FLOAT64 GENERATED ALWAYS AS (price * qty) VIRTUAL,
                    body TEXT
                )",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded (id, qty, price, body) VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..32_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i + 1),
                            Value::Float64(1.5),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute("SELECT id, total FROM seeded WHERE id >= 2 AND id < 4 ORDER BY id")
            .expect("virtual generated projection");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(2), Value::Float64(4.5)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(3), Value::Float64(6.0)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after generated projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected virtual generated projection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after generated projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after generated projection, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_filtered_expression_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-filtered-expression-projection.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(format!("name-{i:03}")),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute(
                "SELECT n + 1 AS next_n FROM seeded WHERE n >= 10 AND n <= 12 ORDER BY n LIMIT 1",
            )
            .expect("filtered arithmetic projection");
        assert_eq!(scalar_i64(&result), 11);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after filtered arithmetic projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged filtered expression projection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after filtered expression projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after filtered expression projection, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_distinct_expression_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-distinct-expression-projection.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(format!("name-{:02}", i % 4)),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute("SELECT DISTINCT UPPER(name) FROM seeded")
            .expect("distinct expression projection");
        assert_eq!(result.rows().len(), 4);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after distinct expression projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged distinct expression projection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after distinct expression projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged distinct expression projection, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_distinct_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-distinct-projection.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 4),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute("SELECT DISTINCT grp FROM seeded")
            .expect("paged distinct projection");
        assert_eq!(result.rows().len(), 4);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after paged distinct projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged distinct projection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after distinct projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged distinct projection, got: {json_after}"
        );
    }

    #[test]
    fn paged_row_storage_ordered_distinct_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-ordered-distinct-projection.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, body TEXT)")
                .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i % 4),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
        let result = db
            .execute("SELECT DISTINCT grp FROM seeded ORDER BY grp DESC LIMIT 2 OFFSET 1")
            .expect("ordered paged distinct projection");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(1)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after ordered paged distinct projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered paged distinct projection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after ordered distinct projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered paged distinct projection, got: {json_after}"
        );
    }

    #[test]
    fn reader_handle_refreshes_after_external_checkpoint() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-refresh.ddb");
        let config = DbConfig::default();

        let setup = Db::open_or_create(&path, config.clone()).expect("open setup");
        setup
            .execute("CREATE TABLE t (id INTEGER)")
            .expect("create table");
        setup.begin_transaction().expect("begin seed txn");
        for i in 0_i64..100_i64 {
            setup
                .execute_with_params("INSERT INTO t VALUES ($1)", &[Value::Int64(i)])
                .expect("seed insert");
        }
        setup.commit_transaction().expect("commit seed");
        drop(setup);

        let reader = Db::open_or_create(&path, config.clone()).expect("open reader");
        assert_eq!(
            scalar_i64(
                &reader
                    .execute("SELECT COUNT(*) FROM t")
                    .expect("reader count before")
            ),
            100
        );

        let writer = Db::open_or_create(&path, config).expect("open writer");
        writer.begin_transaction().expect("begin writer txn");
        for i in 100_i64..200_i64 {
            writer
                .execute_with_params("INSERT INTO t VALUES ($1)", &[Value::Int64(i)])
                .expect("writer insert");
        }
        writer.commit_transaction().expect("commit writer");
        writer.checkpoint().expect("checkpoint writer");

        assert_eq!(
            scalar_i64(
                &reader
                    .execute("SELECT COUNT(*) FROM t")
                    .expect("reader count after")
            ),
            200
        );
    }

    #[test]
    fn checkpoint_with_active_reader_retains_wal_versions_until_reader_drops() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("checkpoint-active-reader-retains-wal.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.execute("CREATE TABLE t (id INTEGER, val TEXT)")
            .expect("create table");
        for i in 0..512_i64 {
            db.execute_with_params(
                "INSERT INTO t VALUES ($1, $2)",
                &[Value::Int64(i), Value::Text("x".repeat(128))],
            )
            .expect("insert row");
        }

        assert!(
            db.inner.wal.version_count().expect("version count before") > 0,
            "expected inserts to populate the WAL before checkpoint"
        );

        let reader = db.inner.wal.begin_reader().expect("begin reader");
        db.checkpoint().expect("checkpoint with active reader");
        assert!(
            db.inner
                .wal
                .version_count()
                .expect("version count with reader")
                > 0,
            "checkpoint should retain WAL versions while a reader is active"
        );

        drop(reader);
        db.checkpoint().expect("checkpoint after reader drop");
        assert_eq!(
            db.inner
                .wal
                .version_count()
                .expect("version count after reader"),
            0,
            "checkpoint should truncate WAL after active readers are gone"
        );
    }

    #[test]
    fn same_handle_explicit_txn_can_commit_after_checkpoint_truncates_wal() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-explicit-txn-rebase.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'before-checkpoint')")
            .expect("insert before checkpoint");
        db.checkpoint().expect("checkpoint");
        assert_eq!(
            db.inner.wal.latest_snapshot(),
            0,
            "checkpoint should truncate WAL"
        );

        db.begin_transaction().expect("begin transaction");
        db.execute("INSERT INTO t VALUES (2, 'after-checkpoint')")
            .expect("insert after checkpoint");
        db.commit_transaction()
            .expect("commit transaction after checkpoint");

        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
            2
        );
    }

    #[test]
    fn same_handle_prepared_explicit_txn_can_commit_after_checkpoint_truncates_wal() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("checkpoint-prepared-explicit-txn-rebase.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'before-checkpoint')")
            .expect("insert before checkpoint");
        db.checkpoint().expect("checkpoint");
        assert_eq!(
            db.inner.wal.latest_snapshot(),
            0,
            "checkpoint should truncate WAL"
        );

        db.begin_transaction().expect("begin transaction");
        let prepared = db
            .prepare("INSERT INTO t VALUES ($1, $2)")
            .expect("prepare insert");
        prepared
            .execute(&[Value::Int64(2), Value::Text("after-checkpoint".to_string())])
            .expect("execute prepared insert after checkpoint");
        db.commit_transaction()
            .expect("commit prepared transaction after checkpoint");

        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
            2
        );
    }

    #[test]
    fn prepared_after_schema_change_in_shared_transaction_uses_current_transaction_schema() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("shared-txn-schema-change-prepare.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.begin_transaction().expect("begin transaction");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .expect("create table in shared transaction");

        let prepared = db
            .prepare("INSERT INTO t VALUES ($1)")
            .expect("prepare insert after schema change");

        {
            let txn = db.inner.sql_txn.lock().expect("lock sql transaction");
            let super::SqlTxnSlot::Shared(state) = &*txn else {
                panic!("expected shared sql transaction state");
            };
            assert_eq!(
                prepared.schema_cookie, state.runtime.catalog.schema_cookie,
                "prepared insert should capture the shared transaction schema cookie"
            );
            assert_eq!(
                prepared.temp_schema_cookie, state.runtime.temp_schema_cookie,
                "prepared insert should capture the shared transaction temp schema cookie"
            );
        }

        prepared
            .execute(&[Value::Int64(1)])
            .expect("execute prepared insert after schema change");
        db.commit_transaction().expect("commit transaction");

        assert_eq!(
            scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
            1
        );
    }

    #[test]
    fn shared_wal_registry_entry_is_removed_when_last_handle_drops() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("shared-wal-registry-cleanup.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t (id INTEGER)")
            .expect("create table");

        let canonical_path = crate::vfs::VfsHandle::for_path(&path)
            .canonicalize_path(&path)
            .expect("canonicalize path");
        assert!(crate::wal::shared::has_registry_entry_for_tests(
            &canonical_path
        ));

        drop(db);

        assert!(!crate::wal::shared::has_registry_entry_for_tests(
            &canonical_path
        ));
    }

    #[test]
    fn checkpoint_preserves_unchanged_table_payload_pages() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("checkpoint-unchanged-table-payload.ddb");
        let config = DbConfig::default();

        let setup = Db::open_or_create(&path, config.clone()).expect("open setup");
        setup
            .execute("CREATE TABLE initial (id INTEGER, val TEXT)")
            .expect("create initial table");
        setup
            .execute_with_params(
                "INSERT INTO initial VALUES ($1, $2)",
                &[Value::Int64(1), Value::Text("seed".to_string())],
            )
            .expect("seed initial row");
        setup.checkpoint().expect("checkpoint setup");
        drop(setup);

        let writer = Db::open_or_create(&path, config).expect("open writer");
        writer
            .execute("CREATE TABLE new_table (id INTEGER, val TEXT)")
            .expect("create new table");
        writer.begin_transaction().expect("begin writer txn");
        for i in 0_i64..100_i64 {
            writer
                .execute_with_params(
                    "INSERT INTO new_table VALUES ($1, $2)",
                    &[Value::Int64(i), Value::Text(format!("value-{i}"))],
                )
                .expect("insert new row");
        }
        writer.commit_transaction().expect("commit writer");
        writer.checkpoint().expect("checkpoint writer");
        drop(writer);

        let reopened = Db::open_or_create(&path, DbConfig::default()).expect("reopen database");
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT COUNT(*) FROM initial")
                    .expect("count initial rows after checkpoint")
            ),
            1
        );
        assert_eq!(
            scalar_i64(
                &reopened
                    .execute("SELECT COUNT(*) FROM new_table")
                    .expect("count new rows after checkpoint")
            ),
            100
        );
    }

    #[test]
    fn schema_snapshot_projects_rich_schema_metadata() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE parent (id INT PRIMARY KEY)")
            .expect("create parent table");
        db.execute(
            "CREATE TABLE child (
                id INT PRIMARY KEY,
                parent_id INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL,
                qty INT CHECK (qty > 0),
                price FLOAT64 NOT NULL,
                total_stored FLOAT64 GENERATED ALWAYS AS (price * qty) STORED,
                total_virtual FLOAT64 GENERATED ALWAYS AS (price * qty) VIRTUAL,
                CONSTRAINT child_parent_positive CHECK (parent_id IS NULL OR parent_id > 0),
                CHECK (qty < 1000)
            )",
        )
        .expect("create child table");
        db.execute("INSERT INTO parent VALUES (1)")
            .expect("insert parent row");
        db.execute("INSERT INTO child (id, parent_id, qty, price) VALUES (1, 1, 5, 3.0)")
            .expect("insert child row");

        db.execute("CREATE TEMP TABLE temp_data (id INT PRIMARY KEY)")
            .expect("create temp table");
        db.execute("INSERT INTO temp_data VALUES (7)")
            .expect("insert temp row");
        db.execute("CREATE VIEW child_view AS SELECT id, parent_id FROM child")
            .expect("create view");
        db.execute("CREATE TEMP VIEW temp_child_ids AS SELECT id FROM temp_data")
            .expect("create temp view");
        db.execute(
            "CREATE INDEX child_parent_partial ON child(parent_id) WHERE parent_id IS NOT NULL",
        )
        .expect("create partial index");
        db.execute(
            "CREATE TRIGGER child_after_insert AFTER INSERT ON child FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO parent VALUES (999)')",
        )
        .expect("create table trigger");
        db.execute(
            "CREATE TRIGGER child_view_insert INSTEAD OF INSERT ON child_view FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO child (id, parent_id, qty, price) VALUES (2000, NULL, 1, 1.0)')",
        )
        .expect("create view trigger");

        let snapshot = db.get_schema_snapshot().expect("schema snapshot");
        assert_eq!(snapshot.snapshot_version, 1);

        let child = snapshot
            .tables
            .iter()
            .find(|table| table.name == "child")
            .expect("child table in snapshot");
        assert!(!child.temporary);
        assert_eq!(child.row_count, 1);
        assert!(child.ddl.contains("CREATE TABLE \"child\""));
        assert!(child
            .checks
            .iter()
            .any(|check| check.name.as_deref() == Some("child_parent_positive")));
        assert!(child.checks.iter().any(|check| check.name.is_none()));
        assert!(child
            .foreign_keys
            .iter()
            .any(|fk| fk.on_delete == "CASCADE" && fk.on_update == "SET NULL"));

        let qty_column = child
            .columns
            .iter()
            .find(|column| column.name == "qty")
            .expect("qty column");
        assert!(qty_column.checks.iter().any(|check| check.name.is_none()));

        let stored = child
            .columns
            .iter()
            .find(|column| column.name == "total_stored")
            .expect("stored generated column");
        assert_eq!(stored.generated_sql.as_deref(), Some("(price * qty)"));
        assert!(stored.generated_stored);

        let virtual_column = child
            .columns
            .iter()
            .find(|column| column.name == "total_virtual")
            .expect("virtual generated column");
        assert_eq!(
            virtual_column.generated_sql.as_deref(),
            Some("(price * qty)")
        );
        assert!(!virtual_column.generated_stored);

        let temp_table = snapshot
            .tables
            .iter()
            .find(|table| table.name == "temp_data")
            .expect("temp table in snapshot");
        assert!(temp_table.temporary);
        assert_eq!(temp_table.row_count, 1);
        assert!(temp_table.ddl.contains("CREATE TEMP TABLE"));

        let temp_view = snapshot
            .views
            .iter()
            .find(|view| view.name == "temp_child_ids")
            .expect("temp view in snapshot");
        assert!(temp_view.temporary);
        assert!(temp_view.ddl.contains("CREATE TEMP VIEW"));

        let partial_index = snapshot
            .indexes
            .iter()
            .find(|index| index.name == "child_parent_partial")
            .expect("partial index in snapshot");
        assert_eq!(
            partial_index.predicate_sql.as_deref(),
            Some("parent_id IS NOT NULL")
        );
        assert!(partial_index.ddl.contains("WHERE parent_id IS NOT NULL"));

        let table_trigger = snapshot
            .triggers
            .iter()
            .find(|trigger| trigger.name == "child_after_insert")
            .expect("table trigger in snapshot");
        assert_eq!(table_trigger.target_kind, "table");
        assert_eq!(table_trigger.timing, "after");
        assert_eq!(table_trigger.events, vec!["insert".to_string()]);
        assert_eq!(table_trigger.events_mask, 1);
        assert!(table_trigger.for_each_row);
        assert!(!table_trigger.temporary);
        assert!(table_trigger.ddl.contains("CREATE TRIGGER"));

        let view_trigger = snapshot
            .triggers
            .iter()
            .find(|trigger| trigger.name == "child_view_insert")
            .expect("view trigger in snapshot");
        assert_eq!(view_trigger.target_kind, "view");
        assert_eq!(view_trigger.timing, "instead_of");
    }

    #[test]
    fn schema_snapshot_orders_top_level_collections_by_name() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE z_table (id INT PRIMARY KEY)")
            .expect("create z_table");
        db.execute("CREATE TABLE a_table (id INT PRIMARY KEY)")
            .expect("create a_table");
        db.execute("CREATE TEMP TABLE m_table (id INT PRIMARY KEY)")
            .expect("create m_table");
        db.execute("CREATE VIEW z_view AS SELECT id FROM z_table")
            .expect("create z_view");
        db.execute("CREATE VIEW a_view AS SELECT id FROM a_table")
            .expect("create a_view");
        db.execute("CREATE TEMP VIEW m_view AS SELECT id FROM m_table")
            .expect("create m_view");
        db.execute("CREATE INDEX z_index ON z_table(id)")
            .expect("create z_index");
        db.execute("CREATE INDEX a_index ON a_table(id)")
            .expect("create a_index");
        db.execute(
            "CREATE TRIGGER z_trigger AFTER INSERT ON z_table FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO z_table VALUES (1000)')",
        )
        .expect("create z_trigger");
        db.execute(
            "CREATE TRIGGER a_trigger AFTER INSERT ON a_table FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO a_table VALUES (1000)')",
        )
        .expect("create a_trigger");

        let snapshot = db.get_schema_snapshot().expect("schema snapshot");

        let table_names = snapshot
            .tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        assert_sorted_names(&table_names);

        let view_names = snapshot
            .views
            .iter()
            .map(|view| view.name.clone())
            .collect::<Vec<_>>();
        assert_sorted_names(&view_names);

        let index_names = snapshot
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        assert_sorted_names(&index_names);

        let trigger_names = snapshot
            .triggers
            .iter()
            .map(|trigger| trigger.name.clone())
            .collect::<Vec<_>>();
        assert_sorted_names(&trigger_names);
    }

    #[test]
    fn write_transaction_page_allocation_stays_off_main_file_until_commit() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("txn-page-allocation.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        let initial_page_count = db
            .inner
            .pager
            .on_disk_page_count()
            .expect("initial page count");

        db.begin_write().expect("begin write txn");
        let page_id = db.allocate_page().expect("allocate staged page");
        assert!(page_id > initial_page_count);
        assert_eq!(
            db.inner
                .pager
                .on_disk_page_count()
                .expect("page count after staged allocation"),
            initial_page_count
        );
        assert_eq!(
            db.read_page(page_id).expect("read staged page").to_vec(),
            vec![0_u8; db.config().page_size as usize]
        );
        db.rollback().expect("rollback write txn");

        let reopened = Db::open_or_create(&path, DbConfig::default()).expect("reopen db");
        assert_eq!(
            reopened
                .inner
                .pager
                .on_disk_page_count()
                .expect("page count after rollback"),
            initial_page_count
        );
    }

    #[test]
    fn freed_pages_are_reused_after_commit() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("freelist-reuse.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.begin_write().expect("begin allocate txn");
        let page_id = db.allocate_page().expect("allocate page");
        db.commit().expect("commit allocated page");

        db.begin_write().expect("begin free txn");
        db.free_page(page_id).expect("free page");
        db.commit().expect("commit freed page");

        db.begin_write().expect("begin reuse txn");
        let reused = db.allocate_page().expect("reuse page");
        assert_eq!(reused, page_id);
        db.rollback().expect("rollback reuse txn");
    }

    #[test]
    fn checkpoint_truncates_tail_freelist_pages_and_resets_allocator_state() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("checkpoint-tail-truncation.ddb");
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

        db.begin_write().expect("begin allocate txn");
        let page3 = db.allocate_page().expect("allocate page 3");
        let page4 = db.allocate_page().expect("allocate page 4");
        let page5 = db.allocate_page().expect("allocate page 5");
        assert_eq!((page3, page4, page5), (3, 4, 5));
        db.commit().expect("commit allocated pages");

        db.begin_write().expect("begin free txn");
        db.free_page(page5).expect("free page 5");
        db.free_page(page4).expect("free page 4");
        db.commit().expect("commit freed tail pages");
        db.checkpoint().expect("checkpoint freed pages");

        assert_eq!(
            db.inner
                .pager
                .on_disk_page_count()
                .expect("page count after truncation"),
            3
        );
        assert_eq!(
            db.inner
                .pager
                .header_snapshot()
                .expect("header snapshot after truncation")
                .freelist
                .page_count,
            0
        );

        db.begin_write().expect("begin allocate after truncation");
        let reused = db.allocate_page().expect("allocate page after truncation");
        assert_eq!(reused, 4);
        db.rollback().expect("rollback post-truncation allocation");
    }

    fn dummy_prepared_insert(table_name: &str) -> PreparedSimpleInsert {
        PreparedSimpleInsert {
            table_name: table_name.to_string(),
            columns: vec![PreparedInsertColumn {
                name: "id".to_string(),
                column_type: crate::catalog::ColumnType::Int64,
                auto_increment: false,
            }],
            primary_auto_row_id_column_index: None,
            value_sources: vec![PreparedInsertValueSource::Null],
            required_columns: Vec::new(),
            foreign_keys: Vec::new(),
            unique_indexes: Vec::new(),
            insert_indexes: Vec::new(),
            use_generic_validation: false,
            use_generic_index_updates: false,
            compiled_index_state_epoch: 0,
        }
    }

    fn assert_sorted_names(names: &[String]) {
        let mut sorted = names.to_vec();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    fn scalar_i64(result: &crate::QueryResult) -> i64 {
        match result.rows()[0].values()[0] {
            Value::Int64(value) => value,
            ref other => panic!("expected INT64 scalar, got {other:?}"),
        }
    }

    fn scalar_text(result: &crate::QueryResult) -> &str {
        match &result.rows()[0].values()[0] {
            Value::Text(value) => value,
            other => panic!("expected TEXT scalar, got {other:?}"),
        }
    }

    /// ADR 0143 Phase A: `inspect_storage_state_json` exposes per-runtime
    /// table residency so callers can verify lazy-load progress.
    #[test]
    fn inspect_storage_state_json_reports_table_memory_totals() {
        let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE m (id INTEGER PRIMARY KEY, label TEXT)")
            .expect("create m");
        for i in 0..32 {
            db.execute(&format!(
                "INSERT INTO m (id, label) VALUES ({i}, 'value-{i}-with-some-text')"
            ))
            .expect("insert m");
        }
        let json = db
            .inspect_storage_state_json()
            .expect("inspect storage state");
        assert!(
            json.contains("\"tables_in_memory_bytes\":"),
            "missing tables_in_memory_bytes: {json}"
        );
        assert!(
            json.contains("\"rows_in_memory_count\":"),
            "missing rows_in_memory_count: {json}"
        );
        assert!(
            json.contains("\"loaded_table_count\":"),
            "missing loaded_table_count: {json}"
        );
        assert!(
            json.contains("\"wal_resident_versions\":"),
            "missing wal_resident_versions: {json}"
        );
        assert!(
            json.contains("\"wal_on_disk_versions\":"),
            "missing wal_on_disk_versions: {json}"
        );
        assert!(
            json.contains("\"deferred_table_count\":0"),
            "expected zero deferred tables on a fresh in-memory db: {json}"
        );
        // 32 inserted rows must show up in the residency total.
        assert!(
            json.contains("\"rows_in_memory_count\":32"),
            "expected rows_in_memory_count=32, got: {json}"
        );
    }

    /// ADR 0143 Phase B: by default, re-opening a DB leaves persisted
    /// tables in the deferred set until the first SQL statement runs,
    /// then materializes them.
    #[test]
    fn default_defer_table_materialization_skips_eager_load_at_open() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("defer-load.ddb");

        // Seed a DB with a persisted table and close.
        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with default defer");

        // At open, the table should not yet be loaded.
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected 1 deferred table at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows at open, got: {json_open}"
        );

        // Simple single-table expression projections now stream directly from
        // persisted rows instead of forcing resident materialization.
        let result = db
            .execute("SELECT n + 1 FROM seeded ORDER BY n LIMIT 1")
            .expect("query after defer");
        assert_eq!(scalar_i64(&result), 1);

        let offset_result = db
            .execute("SELECT n + 1 FROM seeded ORDER BY n DESC LIMIT 2 OFFSET 1")
            .expect("offset query after defer");
        assert_eq!(offset_result.rows().len(), 2);
        assert_eq!(offset_result.rows()[0].values(), &[Value::Int64(49)]);
        assert_eq!(offset_result.rows()[1].values(), &[Value::Int64(48)]);

        let json_after = db.inspect_storage_state_json().expect("json after");
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after first query, got: {json_after}"
        );
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected zero loaded tables after deferred expression projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after deferred expression projection, got: {json_after}"
        );
    }

    #[test]
    fn temp_only_writes_do_not_load_deferred_persisted_tables() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("defer-temp-only-writes.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with default defer");
        let json_open = db.inspect_storage_state_json().expect("json at open");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected zero loaded tables at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected one deferred table at open, got: {json_open}"
        );

        db.execute("CREATE TEMP TABLE temp_data (id INTEGER PRIMARY KEY)")
            .expect("create temp table");
        db.execute("INSERT INTO temp_data SELECT 1 UNION ALL SELECT 2")
            .expect("insert into temp table");
        db.execute("CREATE TEMP VIEW temp_view AS SELECT id FROM temp_data")
            .expect("create temp view");

        let temp_count = db
            .execute("SELECT COUNT(*) FROM temp_data")
            .expect("count temp rows");
        assert_eq!(scalar_i64(&temp_count), 2);
        let temp_view_count = db
            .execute("SELECT COUNT(*) FROM temp_view")
            .expect("count temp view rows");
        assert_eq!(scalar_i64(&temp_view_count), 2);

        db.execute("DROP VIEW temp_view").expect("drop temp view");
        db.execute("DROP TABLE temp_data").expect("drop temp table");

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after temp writes");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected temp-only DDL and writes to avoid loading persisted tables, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred persisted table to remain deferred after temp-only DDL and writes, got: {json_after}"
        );
    }

    #[test]
    fn filtered_expression_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-filtered-expression-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(format!("name-{i:03}")),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT UPPER(name) FROM seeded WHERE n >= 10 AND n <= 12 ORDER BY n LIMIT 1")
            .expect("filtered deferred expression projection");
        assert_eq!(scalar_text(&result), "NAME-010");

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after filtered deferred expression projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected filtered deferred expression projection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after filtered expression projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after filtered deferred expression projection, got: {json_after}"
        );
    }

    #[test]
    fn distinct_expression_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-distinct-expression-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)",
            )
            .expect("create seeded");
            let large_body = "x".repeat(2048);
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
                .expect("prepare insert");
            for i in 0_i64..96_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i),
                            Value::Int64(i),
                            Value::Text(format!("name-{:02}", i % 4)),
                            Value::Text(large_body.clone()),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit rows");
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT DISTINCT UPPER(name) FROM seeded")
            .expect("distinct deferred expression projection");
        assert_eq!(result.rows().len(), 4);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after distinct deferred expression projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected distinct deferred expression projection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after distinct deferred expression projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after distinct deferred expression projection, got: {json_after}"
        );
    }

    #[test]
    fn row_id_point_lookup_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-row-id.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected deferred table at open, got: {json_open}"
        );

        let result = db
            .execute("SELECT n FROM seeded WHERE id = 17")
            .expect("point lookup");
        assert_eq!(scalar_i64(&result), 17);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after point lookup");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected point lookup to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after deferred point lookup, got: {json_after}"
        );
    }

    #[test]
    fn simple_count_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-count.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db.execute("SELECT COUNT(*) FROM seeded").expect("count");
        assert_eq!(scalar_i64(&result), 50);

        let json_after = db.inspect_storage_state_json().expect("json after count");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected count to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after count, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after deferred count, got: {json_after}"
        );
    }

    #[test]
    fn simple_min_max_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-min-max.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                let n = if i % 10 == 0 {
                    "NULL".to_string()
                } else {
                    i.to_string()
                };
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {n})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, config).expect("reopen with defer");
        let max_result = db.execute("SELECT MAX(id) FROM seeded").expect("max");
        assert_eq!(scalar_i64(&max_result), 49);
        let min_result = db.execute("SELECT MIN(n) FROM seeded").expect("min");
        assert_eq!(scalar_i64(&min_result), 1);

        let json_after = db.inspect_storage_state_json().expect("json after min/max");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected min/max to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after min/max, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after deferred min/max, got: {json_after}"
        );
    }

    #[test]
    fn prepared_row_id_point_lookup_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("prepared-deferred-row-id.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let prepared = db
            .prepare("SELECT n FROM seeded WHERE id = $1")
            .expect("prepare point lookup");
        let result = prepared
            .execute(&[Value::Int64(17)])
            .expect("execute prepared point lookup");
        assert_eq!(scalar_i64(&result), 17);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after prepared point lookup");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected prepared point lookup to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after prepared deferred point lookup, got: {json_after}"
        );
    }

    #[test]
    fn simple_table_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-simple-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT n FROM seeded")
            .expect("simple projection");
        assert_eq!(result.rows().len(), 50);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(0)]);
        assert_eq!(result.rows()[49].values(), &[Value::Int64(49)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after simple projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected simple projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after simple projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after simple projection, got: {json_after}"
        );
    }

    #[test]
    fn wildcard_table_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-wildcard-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT * FROM seeded")
            .expect("wildcard projection");
        assert_eq!(result.rows().len(), 50);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(0)]
        );
        assert_eq!(
            result.rows()[49].values(),
            &[Value::Int64(49), Value::Int64(49)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after wildcard projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected wildcard projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after wildcard projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after wildcard projection, got: {json_after}"
        );
    }

    #[test]
    fn simple_filtered_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-filtered-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT n FROM seeded WHERE n >= 10 AND n <= 12 ORDER BY n")
            .expect("filtered projection");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(10)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(11)]);
        assert_eq!(result.rows()[2].values(), &[Value::Int64(12)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after filtered projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected filtered projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after filtered projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after filtered projection, got: {json_after}"
        );
    }

    #[test]
    fn expression_filtered_column_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-expression-filtered-column-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, m INTEGER)")
                .expect("create seeded");
            for (id, n, m) in [(0, 1, 1), (1, 2, 4), (2, 3, 1), (3, 1, 0)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, n, m) VALUES ({id}, {n}, {m})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT n FROM seeded WHERE n + m >= 5")
            .expect("expression filtered column projection");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after expression filtered column projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected expression filtered column projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after expression filtered column projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after expression filtered column projection, got: {json_after}"
        );
    }

    #[test]
    fn ordered_filtered_projection_with_offset_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-filtered-projection-offset.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT n FROM seeded WHERE n >= 10 AND n <= 20 ORDER BY n DESC LIMIT 2 OFFSET 1",
            )
            .expect("ordered filtered projection with offset");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(19)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(18)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after ordered filtered projection with offset");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered filtered projection with offset to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered filtered projection with offset, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered filtered projection with offset, got: {json_after}"
        );
    }

    #[test]
    fn distinct_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-distinct-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER)")
                .expect("create seeded");
            for i in 0..64 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp) VALUES ({i}, {})",
                    i % 4
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT DISTINCT grp FROM seeded")
            .expect("distinct projection");
        assert_eq!(result.rows().len(), 4);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after distinct projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected distinct projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after distinct projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after distinct projection, got: {json_after}"
        );
    }

    #[test]
    fn ordered_distinct_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-ordered-distinct-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER)")
                .expect("create seeded");
            for i in 0..64 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp) VALUES ({i}, {})",
                    i % 4
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT DISTINCT grp FROM seeded ORDER BY grp DESC LIMIT 2 OFFSET 1")
            .expect("ordered distinct projection");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(1)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after ordered distinct projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered distinct projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered distinct projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered distinct projection, got: {json_after}"
        );
    }

    #[test]
    fn distinct_filtered_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-distinct-filtered-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..64 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 4
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT DISTINCT grp FROM seeded WHERE n >= 10 AND n <= 19")
            .expect("distinct filtered projection");
        assert_eq!(result.rows().len(), 4);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after distinct filtered projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected distinct filtered projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after distinct filtered projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after distinct filtered projection, got: {json_after}"
        );
    }

    #[test]
    fn distinct_expression_filtered_column_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-distinct-expression-filtered-column-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
            )
            .expect("create seeded");
            for (id, grp, n, m) in [(0, 0, 1, 1), (1, 0, 2, 4), (2, 1, 3, 1), (3, 1, 2, 3)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT DISTINCT grp FROM seeded WHERE n + m >= 5 ORDER BY grp ASC")
            .expect("distinct expression filtered column projection");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(0)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(1)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after distinct expression filtered column projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected distinct expression filtered column projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after distinct expression filtered column projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after distinct expression filtered column projection, got: {json_after}"
        );
    }

    #[test]
    fn ordered_distinct_filtered_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-ordered-distinct-filtered-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..64 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 4
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT DISTINCT grp FROM seeded WHERE n >= 10 AND n <= 20 ORDER BY grp DESC LIMIT 2 OFFSET 1",
            )
            .expect("ordered distinct filtered projection");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
        assert_eq!(result.rows()[1].values(), &[Value::Int64(1)]);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after ordered distinct filtered projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered distinct filtered projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered distinct filtered projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered distinct filtered projection, got: {json_after}"
        );
    }

    #[test]
    fn ordered_distinct_filtered_projection_multi_order_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-ordered-distinct-filtered-projection-multi-order.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for (id, grp, n) in [
                (0, 0, 10),
                (1, 0, 11),
                (2, 1, 10),
                (3, 1, 11),
                (4, 2, 10),
                (5, 2, 11),
            ] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT DISTINCT grp, n FROM seeded \
                 WHERE n >= 10 AND n <= 11 \
                 ORDER BY grp DESC, n ASC LIMIT 4 OFFSET 1",
            )
            .expect("ordered distinct filtered projection multi-order");
        assert_eq!(result.rows().len(), 4);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(2), Value::Int64(11)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(10)]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[Value::Int64(1), Value::Int64(11)]
        );
        assert_eq!(
            result.rows()[3].values(),
            &[Value::Int64(0), Value::Int64(10)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after ordered distinct filtered projection multi-order");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered distinct filtered projection multi-order to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered distinct filtered projection multi-order, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered distinct filtered projection multi-order, got: {json_after}"
        );
    }

    #[test]
    fn qualified_wildcard_filtered_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-qualified-wildcard-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT s.* FROM seeded AS s WHERE s.n >= 10 AND s.n <= 12 ORDER BY s.n")
            .expect("qualified wildcard filtered projection");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(10), Value::Int64(10)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(11), Value::Int64(11)]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[Value::Int64(12), Value::Int64(12)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after qualified wildcard filtered projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected qualified wildcard filtered projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after qualified wildcard filtered projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after qualified wildcard filtered projection, got: {json_after}"
        );
    }

    #[test]
    fn wildcard_ordered_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-wildcard-ordered-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..48 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT * FROM seeded ORDER BY n DESC LIMIT 2 OFFSET 1")
            .expect("ordered wildcard projection");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(46), Value::Int64(46)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(45), Value::Int64(45)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after ordered wildcard projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered wildcard projection to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered wildcard projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered wildcard projection, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_numeric_aggregate_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-aggregate.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..10 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 2
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(*), SUM(n) FROM seeded WHERE n >= 2 AND n <= 7 GROUP BY grp",
            )
            .expect("grouped aggregate");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(3), Value::Int64(12)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(3), Value::Int64(15)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped aggregate, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_avg_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-avg.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..10 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 2
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, SUM(n) AS total, AVG(n) AS avg_n FROM seeded \
                 WHERE n >= 2 AND n <= 7 GROUP BY grp HAVING avg_n >= 4 \
                 ORDER BY avg_n DESC LIMIT 1",
            )
            .expect("grouped avg aggregate");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(15), Value::Float64(5.0)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped avg aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped avg aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped avg aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped avg aggregate, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_multi_column_numeric_aggregates_keep_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-multi-column-aggregate.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
            )
            .expect("create seeded");
            for (id, grp, n, m) in [(0, 0, 2, 10), (1, 0, 4, 20), (2, 1, 6, 30), (3, 1, 8, 50)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, SUM(n) AS total_n, AVG(m) AS avg_m \
                 FROM seeded GROUP BY grp ORDER BY grp ASC",
            )
            .expect("grouped multi-column numeric aggregate");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(6), Value::Float64(15.0)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(14), Value::Float64(40.0)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped multi-column aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped multi-column aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped multi-column aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped multi-column aggregate, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_numeric_expression_aggregates_keep_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-expression-aggregate.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
            )
            .expect("create seeded");
            for (id, grp, n, m) in [(0, 0, 2, 10), (1, 0, 4, 20), (2, 1, 6, 30), (3, 1, 8, 50)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, SUM(n + m) AS total, AVG(m - n) AS avg_delta \
                 FROM seeded GROUP BY grp HAVING SUM(n + m) >= 30 \
                 ORDER BY total DESC, grp ASC",
            )
            .expect("grouped numeric expression aggregate");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(94), Value::Float64(33.0)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(0), Value::Int64(36), Value::Float64(12.0)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped expression aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped expression aggregate to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped expression aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped expression aggregate, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_expression_bucket_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-expression-bucket.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..10 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 2
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT n / 2 AS bucket, COUNT(*) FROM seeded GROUP BY n / 2 ORDER BY bucket")
            .expect("grouped expression count");
        assert_eq!(result.rows().len(), 5);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(2)]
        );
        assert_eq!(
            result.rows()[4].values(),
            &[Value::Int64(4), Value::Int64(2)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped expression count");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped expression count to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped expression count, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped expression count, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_wrapped_group_projection_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-wrapped-group-projection.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for (id, grp, n) in [(0, 0, 1), (1, 0, 4), (2, 1, 2), (3, 1, 3)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp + 1 AS next_grp, SUM(n) AS total FROM seeded \
                 GROUP BY grp ORDER BY next_grp DESC",
            )
            .expect("grouped wrapped group projection");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(2), Value::Int64(5)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(5)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped wrapped group projection");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped wrapped group projection to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped group projection, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped wrapped group projection, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_wrapped_sum_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-wrapped-sum.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..10 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 2
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, -SUM(n) AS neg_total FROM seeded \
                 GROUP BY grp ORDER BY neg_total LIMIT 1",
            )
            .expect("grouped wrapped sum");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(-25)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped wrapped sum");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped wrapped sum to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped sum, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped wrapped sum, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_wrapped_min_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-wrapped-min.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, word TEXT)")
                .expect("create seeded");
            for (id, grp, word) in [
                (0, 0, "beta"),
                (1, 0, "alpha"),
                (2, 1, "gamma"),
                (3, 1, "delta"),
            ] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, word) VALUES ({id}, {grp}, '{word}')"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, UPPER(MIN(word)) AS upper_min FROM seeded \
                 GROUP BY grp ORDER BY upper_min DESC LIMIT 1",
            )
            .expect("grouped wrapped min");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Text("DELTA".to_string())]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped wrapped min");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped wrapped min to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped min, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped wrapped min, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_wrapped_min_having_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-wrapped-min-having.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, word TEXT)")
                .expect("create seeded");
            for (id, grp, word) in [
                (0, 0, "beta"),
                (1, 0, "alpha"),
                (2, 1, "gamma"),
                (3, 1, "delta"),
            ] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, word) VALUES ({id}, {grp}, '{word}')"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, UPPER(MIN(word)) AS upper_min FROM seeded \
                 GROUP BY grp HAVING UPPER(MIN(word)) >= 'DELTA' \
                 ORDER BY upper_min DESC",
            )
            .expect("grouped wrapped min with having");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Text("DELTA".to_string())]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped wrapped min with having");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped wrapped min with having to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped min with having, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped wrapped min with having, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_count_aggregate_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-count.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..10 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 2
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute("SELECT grp, COUNT(*) FROM seeded GROUP BY grp")
            .expect("grouped count");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(5)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(5)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped count");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped count to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped count, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped count, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_count_with_order_limit_offset_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-count-ordered.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..10 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 2
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c FROM seeded GROUP BY grp ORDER BY grp DESC LIMIT 1 OFFSET 1",
            )
            .expect("ordered grouped count");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(5)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after ordered grouped count");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered grouped count to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered grouped count, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered grouped count, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_numeric_multi_order_by_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-numeric-multi-order.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for (id, grp, n) in [(0, 0, 1), (1, 0, 4), (2, 1, 2), (3, 1, 3)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, SUM(n) AS total, AVG(n) AS avg FROM seeded \
                 GROUP BY grp HAVING SUM(n) >= 3 \
                 ORDER BY total DESC, grp ASC",
            )
            .expect("grouped numeric multi-order");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(5), Value::Float64(2.5)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(5), Value::Float64(2.5)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped numeric multi-order");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped numeric multi-order to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped numeric multi-order, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped numeric multi-order, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_wrapped_count_multi_order_by_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-wrapped-count-multi-order.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER)")
                .expect("create seeded");
            for (id, grp) in [(0, 1), (1, 1), (2, 0), (3, 0)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp) VALUES ({id}, {grp})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(*) + 1 AS cnt FROM seeded \
                 GROUP BY grp HAVING COUNT(*) + 1 >= 3 \
                 ORDER BY cnt DESC, grp ASC",
            )
            .expect("grouped wrapped count multi-order");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(3)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(3)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped wrapped count multi-order");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped wrapped count multi-order to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped count multi-order, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped wrapped count multi-order, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_wrapped_group_count_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-wrapped-group-count.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for id in 0..6 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({id}, {id})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT n / 2 + 1 AS bucket, COUNT(*) AS c FROM seeded \
                 GROUP BY n / 2 ORDER BY bucket DESC",
            )
            .expect("grouped wrapped group count");
        assert_eq!(result.rows().len(), 3);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(3), Value::Int64(2)]
        );
        assert_eq!(
            result.rows()[2].values(),
            &[Value::Int64(1), Value::Int64(2)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped wrapped group count");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped wrapped group count to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped group count, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped wrapped group count, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_multiple_count_rows_keep_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-multiple-count-rows.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER)")
                .expect("create seeded");
            for (id, grp) in [(0, 0), (1, 0), (2, 1)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp) VALUES ({id}, {grp})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c, COUNT(*) + 1 AS c_plus_one \
                 FROM seeded GROUP BY grp ORDER BY grp ASC",
            )
            .expect("grouped repeated count");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(2), Value::Int64(3)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(1), Value::Int64(2)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped repeated count");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped repeated count to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped repeated count, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped repeated count, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_count_having_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-count-having.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for i in 0..10 {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                    i % 2
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c FROM seeded GROUP BY grp HAVING c >= 5 ORDER BY grp DESC LIMIT 1 OFFSET 1",
            )
            .expect("grouped count with having");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(5)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped count with having");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped count with having to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped count with having, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped count with having, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_count_expr_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-count-expr.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for (id, grp, n) in [(0, 0, "1"), (1, 0, "NULL"), (2, 1, "5"), (3, 1, "7")] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(n) AS present, COUNT(n + 1) AS shifted \
                 FROM seeded GROUP BY grp HAVING COUNT(n) >= 1 \
                 ORDER BY present DESC, grp ASC",
            )
            .expect("grouped count expr");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(2), Value::Int64(2)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(0), Value::Int64(1), Value::Int64(1)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped count expr");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped count expr to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped count expr, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped count expr, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_count_distinct_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-count-distinct.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for (id, grp, n) in [
                (0, 0, "1"),
                (1, 0, "1"),
                (2, 0, "NULL"),
                (3, 1, "2"),
                (4, 1, "3"),
                (5, 1, "3"),
            ] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(DISTINCT n) AS uniq, COUNT(DISTINCT n + 1) AS shifted \
                 FROM seeded GROUP BY grp HAVING COUNT(DISTINCT n) >= 1 \
                 ORDER BY uniq DESC, grp ASC",
            )
            .expect("grouped count distinct");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(2), Value::Int64(2)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(0), Value::Int64(1), Value::Int64(1)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped count distinct");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped count distinct to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped count distinct, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped count distinct, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_numeric_distinct_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("deferred-grouped-numeric-distinct.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for (id, grp, n) in [
                (0, 0, 1),
                (1, 0, 1),
                (2, 0, 2),
                (3, 1, 2),
                (4, 1, 3),
                (5, 1, 3),
            ] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, SUM(DISTINCT n) AS total, AVG(DISTINCT n + 1) AS shifted_avg \
                 FROM seeded GROUP BY grp HAVING SUM(DISTINCT n) >= 3 \
                 ORDER BY total DESC, grp ASC",
            )
            .expect("grouped numeric distinct");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(1), Value::Int64(5), Value::Float64(3.5)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(0), Value::Int64(3), Value::Float64(2.5)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped numeric distinct");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped numeric distinct to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped numeric distinct, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped numeric distinct, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_having_only_aggregate_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-having-only-aggregate.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
                .expect("create seeded");
            for (id, grp, n) in [(0, 0, 1), (1, 0, 4), (2, 1, 1)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c FROM seeded \
                 GROUP BY grp HAVING SUM(n) >= 3 ORDER BY grp ASC",
            )
            .expect("grouped having-only aggregate");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(2)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped having-only aggregate");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped having-only aggregate to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped having-only aggregate, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped having-only aggregate, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_count_with_expression_filter_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-count-expression-filter.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
            )
            .expect("create seeded");
            for (id, grp, n, m) in [(0, 0, 1, 1), (1, 0, 2, 4), (2, 1, 3, 1), (3, 1, 1, 0)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c FROM seeded \
                 WHERE n + m >= 5 GROUP BY grp ORDER BY grp ASC",
            )
            .expect("grouped count with expression filter");
        assert_eq!(result.rows().len(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(1)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped count with expression filter");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped count with expression filter to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped count with expression filter, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped count with expression filter, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_numeric_with_expression_filter_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-numeric-expression-filter.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
            )
            .expect("create seeded");
            for (id, grp, n, m) in [(0, 0, 1, 1), (1, 0, 2, 4), (2, 1, 3, 1), (3, 1, 1, 0)] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, SUM(n) AS total FROM seeded \
                 WHERE n + m >= 4 GROUP BY grp ORDER BY grp ASC",
            )
            .expect("grouped numeric with expression filter");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Int64(0), Value::Int64(2)]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[Value::Int64(1), Value::Int64(3)]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped numeric with expression filter");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped numeric with expression filter to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped numeric with expression filter, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped numeric with expression filter, got: {json_after}"
        );
    }

    #[test]
    fn simple_grouped_total_variance_bool_keeps_deferred_table_unloaded() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("deferred-grouped-total-variance-bool.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, flag BOOLEAN)",
            )
            .expect("create seeded");
            for (id, grp, n, flag) in [
                (0, 0, 1, true),
                (1, 0, 1, true),
                (2, 0, 3, false),
                (3, 1, 2, true),
                (4, 1, 4, true),
            ] {
                db.execute(&format!(
                    "INSERT INTO seeded (id, grp, n, flag) VALUES ({id}, {grp}, {n}, {flag})"
                ))
                .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
        let result = db
            .execute(
                "SELECT grp, TOTAL(DISTINCT n) AS total_n, VAR_SAMP(DISTINCT n) AS spread, \
                 BOOL_AND(DISTINCT flag) AS all_true \
                 FROM seeded GROUP BY grp HAVING TOTAL(DISTINCT n) >= 4 ORDER BY grp ASC",
            )
            .expect("grouped total variance bool");
        assert_eq!(result.rows().len(), 2);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(0),
                Value::Float64(4.0),
                Value::Float64(2.0),
                Value::Bool(false),
            ]
        );
        assert_eq!(
            result.rows()[1].values(),
            &[
                Value::Int64(1),
                Value::Float64(6.0),
                Value::Float64(2.0),
                Value::Bool(true),
            ]
        );

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after grouped total variance bool");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped total variance bool to avoid resident materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped total variance bool, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped total variance bool, got: {json_after}"
        );
    }

    #[test]
    fn defer_table_materialization_false_preserves_eager_load_at_open() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("eager-load.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create seeded");
            for i in 0..50 {
                db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                    .expect("insert");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(
            &path,
            DbConfig {
                defer_table_materialization: false,
                ..DbConfig::default()
            },
        )
        .expect("reopen with eager load");

        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":0"),
            "expected zero deferred tables at open, got: {json_open}"
        );
        assert!(
            json_open.contains("\"rows_in_memory_count\":50"),
            "expected eager load to materialize rows at open, got: {json_open}"
        );
    }

    #[test]
    fn per_table_load_matches_mixed_case_identifiers_case_insensitively() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("mixed-case.ddb");

        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE \"ArtistStaging\" (id INTEGER PRIMARY KEY, name TEXT)")
                .expect("create mixed-case table");
            db.execute("INSERT INTO \"ArtistStaging\" VALUES (1, 'alpha')")
                .expect("insert seed row");
            db.checkpoint().expect("checkpoint before close");
        }

        let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen");
        let json_open = db.inspect_storage_state_json().expect("json at open");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected deferred table at open, got: {json_open}"
        );

        let result = db
            .execute("SELECT UPPER(name) FROM ArtistStaging ORDER BY name LIMIT 1")
            .expect("query mixed-case table from unquoted SQL");
        assert_eq!(scalar_text(&result), "ALPHA");

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":0,"),
            "expected mixed-case expression projection to stay deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1}"),
            "expected mixed-case table to remain deferred after query, got: {json_after}"
        );
    }

    /// ADR 0143 Phase B: per-table on-demand load - query only small table
    /// should not materialize the large table.
    #[test]
    fn per_table_load_skips_large_table() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("per-table.ddb");

        // Seed the DB with both tables - small and large
        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE small (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create small");
            db.execute("CREATE TABLE large (id INTEGER PRIMARY KEY, data TEXT)")
                .expect("create large");

            for i in 0..10 {
                db.execute(&format!("INSERT INTO small (id, n) VALUES ({i}, {i})"))
                    .expect("insert small");
            }
            db.checkpoint().expect("checkpoint small");
            for i in 0..1000 {
                let data = format!("data-{}", i);
                db.execute(&format!(
                    "INSERT INTO large (id, data) VALUES ({i}, '{}')",
                    data
                ))
                .expect("insert large");
            }
            db.checkpoint().expect("checkpoint before close");
        }

        // Re-open with deferred materialization
        let cfg = DbConfig {
            defer_table_materialization: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, cfg).expect("reopen with defer");

        // At open, both tables should be deferred
        let json_open = db.inspect_storage_state_json().expect("json at open");
        assert!(
            json_open.contains("\"deferred_table_count\":2"),
            "expected 2 deferred tables at open, got: {json_open}"
        );

        // Query only the small table with a single-table expression
        // projection. The deferred expression path should answer from
        // persisted bytes without loading either table.
        let result = db
            .execute("SELECT n + 1 FROM small ORDER BY n LIMIT 1")
            .expect("query small");
        assert_eq!(scalar_i64(&result), 1);

        // After query both tables should still be deferred because the
        // executor streamed from persisted bytes instead of materializing
        // `small`. Use precise field assertions (with trailing comma) to
        // avoid the substring-match bug where `:10` matches `:1010`.
        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"deferred_table_count\":2}"),
            "expected both tables to remain deferred after small-only query, got: {json_after}"
        );
        assert!(
            json_after.contains("\"loaded_table_count\":0,"),
            "expected zero loaded tables after small-only query, got: {json_after}"
        );
        assert!(
            json_after.contains("\"rows_in_memory_count\":0,"),
            "expected zero resident rows after deferred expression projection, got: {json_after}"
        );
    }

    /// ADR 0143 Phase B+: safe expression subqueries can now stay on the
    /// snapshot-local row-source path instead of forcing a live-runtime
    /// load-all fallback.
    #[test]
    fn per_table_load_keeps_runtime_deferred_for_safe_subquery() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("subq.ddb");
        {
            let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
            db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create a");
            db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, n INTEGER)")
                .expect("create b");
            for i in 0..3 {
                db.execute(&format!("INSERT INTO a VALUES ({i},{i})"))
                    .expect("ins a");
                db.execute(&format!("INSERT INTO b VALUES ({i},{i})"))
                    .expect("ins b");
            }
            db.checkpoint().expect("checkpoint");
        }
        let cfg = DbConfig {
            defer_table_materialization: true,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, cfg).expect("reopen");
        db.execute("SELECT COUNT(*) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.n = a.n)")
            .expect("query with subquery");
        let json = db.inspect_storage_state_json().expect("json");
        assert!(
            json.contains("\"deferred_table_count\":2"),
            "expected both tables to remain deferred after safe subquery execution, got: {json}"
        );
        assert!(
            json.contains("\"loaded_table_count\":0,"),
            "expected safe subquery execution to avoid live-runtime table loads, got: {json}"
        );
    }
    #[test]
    fn paged_row_storage_grouped_query_after_reopen_stays_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-grouped-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)")
                .expect("create sales");
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO sales VALUES ($1, $2, $3)")
                .expect("prepare");
            let regions = ["east", "west", "north", "south"];
            for i in 0_i64..200_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Text(regions[(i as usize) % 4].to_string()),
                            Value::Int64((i % 50) + 1),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");

        let result = db
            .execute(
                "SELECT region, GROUP_CONCAT(amount) AS amounts \
                 FROM sales GROUP BY region ORDER BY region",
            )
            .expect("grouped query after reopen");
        assert_eq!(result.rows().len(), 4);

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped query to re-defer sales after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected sales to be deferred again after grouped query, got: {json_after}"
        );
    }

    /// D-E1: Reopen-time grouped query with HAVING + ORDER BY on a paged table.
    #[test]
    fn paged_row_storage_grouped_having_order_by_after_reopen_stays_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-grouped-having-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, total INTEGER)",
            )
            .expect("create orders");
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO orders VALUES ($1, $2, $3)")
                .expect("prepare");
            let customers = ["alice", "bob", "carol"];
            for i in 0_i64..150_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Text(customers[(i as usize) % 3].to_string()),
                            Value::Int64((i % 100) + 10),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");

        let result = db
            .execute(
                "SELECT customer, SUM(total) AS grand_total FROM orders \
                 GROUP BY customer HAVING SUM(total) > 2000 ORDER BY grand_total DESC",
            )
            .expect("grouped having query after reopen");
        assert!(!result.rows().is_empty());

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped HAVING query to re-defer orders after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected orders to be deferred again after grouped HAVING query, got: {json_after}"
        );
    }

    /// D-E1: Grouped query after reopen (autocommit path).
    #[test]
    fn paged_row_storage_grouped_query_autocommit_after_reopen() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-grouped-autocommit.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT, ts INTEGER)")
                .expect("create events");
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO events VALUES ($1, $2, $3)")
                .expect("prepare");
            let kinds = ["login", "logout", "click", "view"];
            for i in 0_i64..120_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Text(kinds[(i as usize) % 4].to_string()),
                            Value::Int64(1700000000 + i * 60),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");

        let result = db
            .execute("SELECT kind, COUNT(*) AS cnt FROM events GROUP BY kind ORDER BY kind")
            .expect("grouped query after reopen");
        assert_eq!(result.rows().len(), 4);

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped query to keep events deferred, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected events to remain deferred after grouped query, got: {json_after}"
        );
    }

    /// D-E1: Mixed aggregate projection not handled by current specialization
    /// (GROUP_CONCAT + COUNT) exercises the general grouped path.
    #[test]
    fn paged_row_storage_mixed_aggregate_grouped_after_reopen() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir.path().join("paged-row-storage-mixed-aggregate.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT, msg TEXT)")
                .expect("create logs");
            let mut txn = db.transaction().expect("begin txn");
            let insert = txn
                .prepare("INSERT INTO logs VALUES ($1, $2, $3)")
                .expect("prepare");
            let levels = ["INFO", "WARN", "ERROR"];
            for i in 0_i64..90_i64 {
                insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Text(levels[(i as usize) % 3].to_string()),
                            Value::Text(format!("message {}", i)),
                        ],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit seed txn");
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");

        let result = db
            .execute(
                "SELECT level, COUNT(*) AS cnt, GROUP_CONCAT(msg) AS messages \
                 FROM logs GROUP BY level ORDER BY level",
            )
            .expect("mixed aggregate grouped query after reopen");
        assert_eq!(result.rows().len(), 3);

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected mixed aggregate grouped query to re-defer logs after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected logs to be deferred again after mixed aggregate query, got: {json_after}"
        );
    }

    /// D-E2: Reopen-time INNER JOIN on paged tables stays deferred.
    #[test]
    fn paged_row_storage_inner_join_after_reopen_stays_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-inner-join-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)")
                .expect("create authors");
            db.execute(
                "CREATE TABLE books (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)",
            )
            .expect("create books");
            for i in 0_i64..50_i64 {
                db.execute(&format!(
                    "INSERT INTO authors VALUES ({}, 'author {}')",
                    i + 1,
                    i
                ))
                .expect("insert author");
            }
            for i in 0_i64..200_i64 {
                db.execute(&format!(
                    "INSERT INTO books VALUES ({}, {}, 'book {}')",
                    i + 1,
                    (i % 50) + 1,
                    i
                ))
                .expect("insert book");
            }
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");

        let result = db
            .execute(
                "SELECT a.name, b.title FROM authors a INNER JOIN books b \
                 ON a.id = b.author_id ORDER BY a.name, b.title",
            )
            .expect("inner join after reopen");
        assert_eq!(result.rows().len(), 200);

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected inner join to re-defer tables after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both tables deferred after inner join, got: {json_after}"
        );
    }

    /// D-E2: Reopen-time LEFT JOIN on paged tables stays deferred.
    #[test]
    fn paged_row_storage_left_join_after_reopen_stays_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-left-join-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE deps (id INTEGER PRIMARY KEY, name TEXT)")
                .expect("create deps");
            db.execute(
                "CREATE TABLE packages (id INTEGER PRIMARY KEY, dep_id INTEGER, version TEXT)",
            )
            .expect("create packages");
            for i in 0_i64..30_i64 {
                db.execute(&format!("INSERT INTO deps VALUES ({}, 'dep {}')", i + 1, i))
                    .expect("insert dep");
            }
            for i in 0_i64..100_i64 {
                db.execute(&format!(
                    "INSERT INTO packages VALUES ({}, {}, '1.{}')",
                    i + 1,
                    (i % 30) + 1,
                    i
                ))
                .expect("insert package");
            }
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");

        let result = db
            .execute(
                "SELECT d.name, p.version FROM deps d LEFT JOIN packages p \
                 ON d.id = p.dep_id ORDER BY d.name, p.version",
            )
            .expect("left join after reopen");
        assert!(!result.rows().is_empty());

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected left join to re-defer tables after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both tables deferred after left join, got: {json_after}"
        );
    }

    /// D-E2: Reopen-time JOIN USING on paged tables stays deferred.
    #[test]
    fn paged_row_storage_join_using_after_reopen_stays_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-join-using-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
                .expect("create users");
            db.execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)",
            )
            .expect("create orders");
            for i in 0_i64..20_i64 {
                db.execute(&format!(
                    "INSERT INTO users VALUES ({}, 'user {}')",
                    i + 1,
                    i
                ))
                .expect("insert user");
            }
            for i in 0_i64..80_i64 {
                db.execute(&format!(
                    "INSERT INTO orders VALUES ({}, {}, {})",
                    i + 1,
                    (i % 20) + 1,
                    (i % 100) + 10
                ))
                .expect("insert order");
            }
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");

        let result = db
            .execute(
                "SELECT u.name, o.amount FROM users u INNER JOIN orders o \
                 ON u.id = o.user_id WHERE o.amount > 50 ORDER BY o.amount",
            )
            .expect("join with filter after reopen");
        assert!(!result.rows().is_empty());

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected join with filter to re-defer tables after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both tables deferred after join with filter, got: {json_after}"
        );
    }

    /// D-E2: Reopen-time FULL JOIN on paged tables stays deferred.
    #[test]
    fn paged_row_storage_full_join_after_reopen_stays_deferred() {
        let tempdir = TempDir::new().expect("tempdir");
        let path = tempdir
            .path()
            .join("paged-row-storage-full-join-reopen.ddb");
        let config = DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        };

        {
            let db = Db::open_or_create(&path, config.clone()).expect("open db");
            db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
                .expect("create t1");
            db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
                .expect("create t2");
            for i in 0_i64..10_i64 {
                db.execute(&format!("INSERT INTO t1 VALUES ({}, 't1-{}')", i + 1, i))
                    .expect("insert t1");
            }
            for i in 5_i64..15_i64 {
                db.execute(&format!("INSERT INTO t2 VALUES ({}, 't2-{}')", i + 1, i))
                    .expect("insert t2");
            }
            db.checkpoint().expect("checkpoint");
        }

        let db = Db::open_or_create(&path, config).expect("reopen db");

        let result = db
            .execute(
                "SELECT t1.val, t2.val FROM t1 FULL JOIN t2 ON t1.id = t2.id \
                 ORDER BY t1.val, t2.val",
            )
            .expect("full join after reopen");
        assert!(!result.rows().is_empty());

        let json_after = db.inspect_storage_state_json().expect("json after query");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected full join to re-defer tables after commit, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both tables deferred after full join, got: {json_after}"
        );
    }
}
