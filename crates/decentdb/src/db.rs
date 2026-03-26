//! Stable database owner and bootstrap lifecycle entry points.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::catalog::{
    identifiers_equal, CatalogHandle, ColumnSchema, ForeignKeyAction, ForeignKeyConstraint,
    IndexColumn, IndexKind, IndexSchema, TableSchema, TriggerEvent, TriggerKind, TriggerSchema,
    ViewSchema,
};
use crate::config::DbConfig;
use crate::error::{DbError, Result};
use crate::exec::dml::{PreparedInsertValueSource, PreparedSimpleInsert};
use crate::exec::{
    statement_is_read_only, BulkLoadOptions, EngineRuntime, QueryResult, QueryRow, RuntimeIndex,
    TableData,
};
use crate::metadata::{
    ColumnInfo, ForeignKeyInfo, HeaderInfo, IndexInfo, IndexVerification, StorageInfo, TableInfo,
    TriggerInfo, ViewInfo,
};
use crate::record::value::Value;
use crate::sql::ast::Statement as SqlStatement;
use crate::sql::parser::{parse_sql_statement, rewrite_legacy_trigger_body};
use crate::storage::freelist::{decode_freelist_next, encode_freelist_page};
use crate::storage::page::{self, PageId};
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
    read_only: bool,
}

/// Exclusive SQL transaction handle that owns mutable runtime state locally.
///
/// While this handle is active, callers should use it for all SQL work on the
/// same `Db` handle until `commit` or `rollback`.
#[derive(Debug)]
pub struct SqlTransaction<'a> {
    db: &'a Db,
    state: Option<SqlTxnState>,
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
        self.db.execute_prepared_in_state(prepared, params, state)
    }

    /// Commits this transaction's local runtime into the WAL-backed database.
    pub fn commit(mut self) -> Result<u64> {
        let state = self
            .state
            .take()
            .ok_or_else(|| DbError::transaction("SQL transaction handle is no longer active"))?;
        let result = if state.persistent_changed {
            self.db
                .persist_runtime_if_latest(state.runtime, Some(state.base_lsn))
        } else {
            self.db
                .install_temp_runtime(state.runtime)
                .map(|()| state.base_lsn)
        };
        let release = self.deactivate();
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(lsn), Ok(())) => Ok(lsn),
        }
    }

    /// Rolls this transaction back and releases the handle.
    pub fn rollback(mut self) -> Result<()> {
        self.state.take();
        self.deactivate()
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
        self.state.take();
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
    tables: BTreeMap<String, TableSchema>,
    table_data: BTreeMap<String, TableData>,
    views: BTreeMap<String, ViewSchema>,
    indexes: BTreeMap<String, IndexSchema>,
}

impl TempSchemaState {
    fn apply_to_runtime(&self, runtime: &mut EngineRuntime) {
        runtime.temp_schema_cookie = self.schema_cookie;
        runtime.temp_tables = self.tables.clone();
        runtime.temp_table_data = self.table_data.clone();
        runtime.temp_views = self.views.clone();
        runtime.temp_indexes = self.indexes.clone();
    }

    fn update_from_runtime(&mut self, runtime: &EngineRuntime) {
        self.schema_cookie = runtime.temp_schema_cookie;
        self.tables = runtime.temp_tables.clone();
        self.table_data = runtime.temp_table_data.clone();
        self.views = runtime.temp_views.clone();
        self.indexes = runtime.temp_indexes.clone();
    }
}

#[derive(Debug)]
struct SqlTxnState {
    runtime: EngineRuntime,
    base_lsn: u64,
    persistent_changed: bool,
    savepoints: Vec<SqlSavepoint>,
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
    /// Begins an exclusive SQL transaction handle that keeps mutable runtime
    /// state local until commit or rollback.
    pub fn transaction(&self) -> Result<SqlTransaction<'_>> {
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
        self.persist_runtime_if_latest(state.runtime, Some(state.base_lsn))
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
        state.savepoints.truncate(index + 1);
        Ok(())
    }

    /// Returns a structured snapshot of the current storage state.
    pub fn storage_info(&self) -> Result<StorageInfo> {
        let header = self.inner.pager.header_snapshot()?;
        Ok(StorageInfo {
            path: self.path().to_path_buf(),
            wal_path: self.inner.wal.file_path().to_path_buf(),
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

        let max_staged_page_id = txn.staged_pages.keys().copied().max().unwrap_or(0);
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
        self.inner.wal.commit_pages(pages, max_page_count)
    }

    fn commit_if_latest(&self, expected_latest_lsn: u64) -> Result<u64> {
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
            .commit_pages_if_latest(pages, max_page_count, expected_latest_lsn)
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

    fn write_txn_visible_page(&self, txn: &WriteTxn, page_id: PageId) -> Result<Vec<u8>> {
        if let Some(staged) = txn.staged_pages.get(&page_id).cloned() {
            return Ok(staged);
        }

        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
        if let Some(wal_page) = self
            .inner
            .wal
            .read_page_at_snapshot(page_id, snapshot_lsn)?
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
    pub fn read_page(&self, page_id: u32) -> Result<Vec<u8>> {
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
            return Ok(staged);
        }

        let reader = self.inner.wal.begin_reader()?;
        let snapshot_lsn = reader.snapshot_lsn();
        if let Some(wal_page) = self
            .inner
            .wal
            .read_page_at_snapshot(page_id, snapshot_lsn)?
        {
            return Ok(wal_page);
        }
        self.inner.pager.read_page(page_id)
    }

    /// Performs a reader-aware checkpoint.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner
            .wal
            .checkpoint(&self.inner.pager, self.inner.config.checkpoint_timeout_sec)
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
    pub fn read_page_for_snapshot(&self, token: u64, page_id: u32) -> Result<Vec<u8>> {
        page::validate_page_id(page_id)?;
        let snapshot_lsn = self
            .inner
            .held_snapshots
            .lock()
            .map_err(|_| DbError::internal("snapshot registry lock poisoned"))?
            .get(&token)
            .map(|guard| guard.snapshot_lsn())
            .ok_or_else(|| DbError::transaction(format!("unknown snapshot token {token}")))?;

        if let Some(wal_page) = self
            .inner
            .wal
            .read_page_at_snapshot(page_id, snapshot_lsn)?
        {
            return Ok(wal_page);
        }
        self.inner.pager.read_page(page_id)
    }

    /// Returns a deterministic JSON summary of storage state for the harness.
    pub fn inspect_storage_state_json(&self) -> Result<String> {
        let header = self.inner.pager.header_snapshot()?;
        let warnings = self.inner.wal.warnings()?;
        Ok(format!(
            "{{\"path\":\"{}\",\"page_size\":{},\"page_count\":{},\"schema_cookie\":{},\"wal_end_lsn\":{},\"wal_file_size\":{},\"wal_path\":\"{}\",\"last_checkpoint_lsn\":{},\"active_readers\":{},\"wal_versions\":{},\"warning_count\":{},\"shared_wal\":{}}}",
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
            warnings.len(),
            if self.inner.wal.is_shared() { "true" } else { "false" }
        ))
    }

    /// Returns all table definitions with row-count metadata.
    pub fn list_tables(&self) -> Result<Vec<TableInfo>> {
        let runtime = self.runtime_for_inspection()?;
        let mut tables = runtime
            .catalog
            .tables
            .values()
            .map(|table| {
                table_info(
                    table,
                    runtime
                        .tables
                        .get(&table.name)
                        .map_or(0, |data| data.rows.len()),
                )
            })
            .collect::<Vec<_>>();
        tables.extend(runtime.temp_tables.values().map(|table| {
            table_info(
                table,
                runtime
                    .temp_table_data
                    .get(&table.name)
                    .map_or(0, |data| data.rows.len()),
            )
        }));
        Ok(tables)
    }

    /// Returns a single table definition by name.
    pub fn describe_table(&self, name: &str) -> Result<TableInfo> {
        let runtime = self.runtime_for_inspection()?;
        if runtime.temp_views.contains_key(name) && !runtime.temp_tables.contains_key(name) {
            return Err(DbError::sql(format!("unknown table {name}")));
        }
        let (table, row_count) = if let Some(table) = runtime.temp_tables.get(name) {
            (
                table,
                runtime
                    .temp_table_data
                    .get(name)
                    .map_or(0, |data| data.rows.len()),
            )
        } else {
            let table = runtime
                .catalog
                .tables
                .get(name)
                .ok_or_else(|| DbError::sql(format!("unknown table {name}")))?;
            (
                table,
                runtime.tables.get(name).map_or(0, |data| data.rows.len()),
            )
        };
        Ok(table_info(table, row_count))
    }

    /// Returns canonical `CREATE TABLE` SQL for a named table.
    pub fn table_ddl(&self, name: &str) -> Result<String> {
        let runtime = self.runtime_for_inspection()?;
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
        let runtime = self.runtime_for_inspection()?;
        Ok(runtime.catalog.indexes.values().map(index_info).collect())
    }

    /// Returns all view definitions.
    pub fn list_views(&self) -> Result<Vec<ViewInfo>> {
        let runtime = self.runtime_for_inspection()?;
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
        let runtime = self.runtime_for_inspection()?;
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
        let runtime = self.runtime_for_inspection()?;
        Ok(runtime
            .catalog
            .triggers
            .values()
            .map(trigger_info)
            .collect())
    }

    /// Verifies that a named index can be rebuilt logically from the persisted table state.
    pub fn verify_index(&self, name: &str) -> Result<IndexVerification> {
        let runtime = self.runtime_for_inspection()?;
        let existing = runtime
            .indexes
            .get(name)
            .map_or(0, runtime_index_entry_count);

        let mut rebuilt = self.runtime_for_inspection()?;
        rebuilt.rebuild_index(name, self.inner.config.page_size)?;
        let actual = rebuilt
            .indexes
            .get(name)
            .map_or(0, runtime_index_entry_count);

        Ok(IndexVerification {
            name: name.to_string(),
            valid: existing == actual,
            expected_entries: existing,
            actual_entries: actual,
        })
    }

    /// Dumps the current catalog and table contents as deterministic SQL.
    pub fn dump_sql(&self) -> Result<String> {
        let runtime = self.runtime_for_inspection()?;
        Ok(render_runtime_dump(&runtime))
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
        let wal = WalHandle::acquire(
            &vfs,
            &path,
            effective_config.page_size,
            effective_config.wal_sync_mode,
        )?;
        wal.set_max_page_count(pager.on_disk_page_count()?);
        let (runtime, runtime_lsn) = EngineRuntime::load_from_storage(&pager, &wal, schema_cookie)?;
        let catalog = CatalogHandle::new(runtime.catalog.clone());
        let last_seen_checkpoint_epoch = wal.checkpoint_epoch();

        Ok(Self {
            inner: Arc::new(DbInner {
                path,
                config: effective_config,
                _vfs: vfs,
                pager,
                wal,
                catalog,
                engine: RwLock::new(runtime),
                last_runtime_lsn: AtomicU64::new(runtime_lsn),
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
        })
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
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            return runtime.execute_read_statement(statement, params, self.inner.config.page_size);
        }

        self.refresh_engine_from_storage()?;
        let runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
        runtime.execute_read_statement(statement, params, self.inner.config.page_size)
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
                let mut runtime = self.runtime_for_inspection()?;
                runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
                let mut errors = Vec::new();
                for (table_name, table) in &runtime.catalog.tables {
                    let Some(data) = runtime.tables.get(table_name) else {
                        errors.push(format!("table {} is missing row storage", table_name));
                        continue;
                    };
                    if let Some((row_id, row_len)) = data.rows.iter().find_map(|row| {
                        (row.values.len() != table.columns.len())
                            .then_some((row.row_id, row.values.len()))
                    }) {
                        errors.push(format!(
                            "table {} row {} has {} values but schema defines {} columns",
                            table_name,
                            row_id,
                            row_len,
                            table.columns.len()
                        ));
                    }
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
                    if prepared.read_only {
                        for row_index in 0..row_count {
                            build_params(row_index, &mut params)?;
                            let result = state.runtime.execute_read_statement(
                                prepared.statement.as_ref(),
                                &params,
                                self.inner.config.page_size,
                            )?;
                            total_affected = total_affected.saturating_add(result.affected_rows());
                        }
                        return Ok(total_affected);
                    }

                    if let Some(prepared_insert) = prepared
                        .prepared_insert
                        .as_deref()
                        .filter(|plan| state.runtime.can_reuse_prepared_simple_insert(plan))
                    {
                        if Self::prepared_insert_uses_direct_positional_params(
                            prepared_insert,
                            param_count,
                        ) {
                            for row_index in 0..row_count {
                                build_params(row_index, &mut params)?;
                                let affected = state
                                    .runtime
                                    .execute_prepared_simple_insert_positional_params_in_place(
                                        prepared_insert,
                                        &mut params,
                                        self.inner.config.page_size,
                                    )?;
                                total_affected = total_affected.saturating_add(affected);
                            }
                            return Ok(total_affected);
                        }

                        for row_index in 0..row_count {
                            build_params(row_index, &mut params)?;
                            let result = state.runtime.execute_prepared_simple_insert(
                                prepared_insert,
                                &params,
                                self.inner.config.page_size,
                            )?;
                            total_affected = total_affected.saturating_add(result.affected_rows());
                        }
                        return Ok(total_affected);
                    }

                    for row_index in 0..row_count {
                        build_params(row_index, &mut params)?;
                        let result = self.execute_prepared_in_state(prepared, &params, state)?;
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

    fn execute_prepared_read_statement(
        &self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<QueryResult> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            self.validate_prepared_schema_cookie(
                prepared,
                runtime.catalog.schema_cookie,
                runtime.temp_schema_cookie,
            )?;
            return runtime.execute_read_statement(
                prepared.statement.as_ref(),
                params,
                self.inner.config.page_size,
            );
        }

        self.refresh_engine_from_storage()?;
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
        runtime.execute_read_statement(
            prepared.statement.as_ref(),
            params,
            self.inner.config.page_size,
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

        let _writer = self
            .inner
            .sql_write_lock
            .lock()
            .map_err(|_| DbError::internal("SQL writer lock poisoned"))?;
        self.refresh_engine_from_storage()?;
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
        let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
        if temp_only {
            let mut working = self.engine_snapshot()?;
            let result = working.execute_statement(
                prepared.statement.as_ref(),
                params,
                self.inner.config.page_size,
            )?;
            self.install_temp_runtime(working)?;
            return Ok(result);
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
        }
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
        self.refresh_engine_from_storage()?;
        let temp_only = {
            let runtime = self
                .inner
                .engine
                .read()
                .map_err(|_| DbError::internal("engine runtime lock poisoned"))?;
            self.statement_is_temp_only(&runtime, statement)
        };
        let _savepoint = StatementSavepoint::new(self.inner.wal.latest_snapshot());
        if temp_only {
            let mut working = self.engine_snapshot()?;
            let result =
                working.execute_statement(statement, params, self.inner.config.page_size)?;
            self.install_temp_runtime(working)?;
            return Ok(result);
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
                return self.execute_autocommit_prepared_insert_in_place(prepared.as_ref(), params);
            }
        }
        if matches!(
            statement,
            crate::sql::ast::Statement::Insert(insert)
                if self
                    .inner
                    .engine
                    .read()
                    .map_err(|_| DbError::internal("engine runtime lock poisoned"))?
                    .can_execute_insert_in_place(insert)
        ) {
            return self.execute_autocommit_insert_in_place(statement, params);
        }
        self.execute_autocommit_in_place(|runtime| {
            runtime.execute_statement(statement, params, self.inner.config.page_size)
        })
    }

    fn execute_autocommit_insert_in_place(
        &self,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
    ) -> Result<QueryResult> {
        self.execute_autocommit_in_place(|runtime| {
            runtime.execute_statement(statement, params, self.inner.config.page_size)
        })
    }

    fn execute_autocommit_prepared_insert_in_place(
        &self,
        prepared: &PreparedSimpleInsert,
        params: &[Value],
    ) -> Result<QueryResult> {
        self.execute_autocommit_in_place(|runtime| {
            runtime.execute_prepared_simple_insert(prepared, params, self.inner.config.page_size)
        })
    }

    fn try_execute_autocommit_prepared_insert_in_place(
        &self,
        prepared_statement: &PreparedStatement,
        prepared_insert: &PreparedSimpleInsert,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
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
            self.inner.catalog.replace(runtime.catalog.clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
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
            self.inner.catalog.replace(runtime.catalog.clone())?;
        }
        self.sync_temp_state_from_runtime(&runtime)?;
        self.inner
            .last_runtime_lsn
            .store(committed_lsn, Ordering::Release);
        Ok(result)
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
        let temp_has_name = |name: &str| temp_has_table(name) || temp_has_view(name);
        match statement {
            crate::sql::ast::Statement::CreateTable(statement) => statement.temporary,
            crate::sql::ast::Statement::CreateTableAs(statement) => statement.temporary,
            crate::sql::ast::Statement::CreateView(statement) => statement.temporary,
            crate::sql::ast::Statement::DropTable { name, .. } => temp_has_table(name),
            crate::sql::ast::Statement::DropView { name, .. } => temp_has_view(name),
            crate::sql::ast::Statement::Insert(statement) => temp_has_name(&statement.table_name),
            crate::sql::ast::Statement::Update(statement) => temp_has_name(&statement.table_name),
            crate::sql::ast::Statement::Delete(statement) => temp_has_name(&statement.table_name),
            _ => false,
        }
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
        let prepared_insert = match statement.as_ref() {
            SqlStatement::Insert(insert) => {
                self.prepared_simple_insert(&prepared_sql, insert, runtime)?
            }
            _ => None,
        };
        Ok(PreparedStatement {
            db: self.clone(),
            schema_cookie: runtime.catalog.schema_cookie,
            temp_schema_cookie: runtime.temp_schema_cookie,
            statement,
            prepared_insert,
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
        self.persist_runtime_if_latest(runtime, None)
    }

    fn persist_runtime_if_latest(
        &self,
        runtime: EngineRuntime,
        expected_latest_lsn: Option<u64>,
    ) -> Result<u64> {
        let mut runtime = runtime;
        let runtime_schema_cookie = runtime.catalog.schema_cookie;
        runtime.rebuild_stale_indexes(self.inner.config.page_size)?;
        self.begin_write()?;
        if let Err(error) = runtime.persist_to_db(self) {
            let _ = self.rollback();
            return Err(error);
        }
        let committed_lsn = match expected_latest_lsn {
            Some(expected) => self.commit_if_latest(expected),
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
            self.inner.catalog.replace(runtime.catalog.clone())?;
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
        Ok(committed_lsn)
    }

    fn runtime_for_inspection(&self) -> Result<EngineRuntime> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            return Ok(runtime);
        }
        self.refresh_engine_from_storage()?;
        self.engine_snapshot()
    }

    fn runtime_for_prepare(&self) -> Result<EngineRuntime> {
        if let Some(runtime) = self.transaction_runtime_snapshot()? {
            return Ok(runtime);
        }
        self.refresh_engine_from_storage()?;
        self.engine_snapshot()
    }

    fn transaction_runtime_snapshot(&self) -> Result<Option<EngineRuntime>> {
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
        snapshot.rebuild_stale_indexes(self.inner.config.page_size)?;
        Ok(Some(snapshot))
    }

    fn restore_runtime_from_storage(&self, runtime: &mut EngineRuntime) -> Result<()> {
        let schema_cookie = self.current_schema_cookie()?;
        let (mut restored, restored_lsn) =
            EngineRuntime::load_from_storage(&self.inner.pager, &self.inner.wal, schema_cookie)?;
        self.apply_temp_state_to_runtime(&mut restored)?;
        self.inner.catalog.replace(restored.catalog.clone())?;
        *runtime = restored;
        self.inner
            .last_runtime_lsn
            .store(restored_lsn, Ordering::Release);
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

        let schema_cookie = self.current_schema_cookie()?;
        let (mut runtime, runtime_lsn) =
            EngineRuntime::load_from_storage(&self.inner.pager, &self.inner.wal, schema_cookie)?;
        self.apply_temp_state_to_runtime(&mut runtime)?;
        self.inner.catalog.replace(runtime.catalog.clone())?;
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

    fn current_schema_cookie(&self) -> Result<u32> {
        let page = self.read_page(page::HEADER_PAGE_ID)?;
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
        self.refresh_engine_from_storage()?;
        let mut runtime = self
            .inner
            .engine
            .read()
            .map_err(|_| DbError::internal("engine runtime lock poisoned"))?
            .clone();
        self.apply_temp_state_to_runtime(&mut runtime)?;
        Ok(SqlTxnState {
            runtime,
            base_lsn: self.inner.last_runtime_lsn.load(Ordering::Acquire),
            persistent_changed: false,
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
            return state.runtime.execute_read_statement(
                prepared.statement.as_ref(),
                params,
                self.inner.config.page_size,
            );
        }
        if matches!(
            prepared.statement.as_ref(),
            crate::sql::ast::Statement::Analyze { .. }
        ) {
            return Err(DbError::transaction(
                "ANALYZE is not supported inside an explicit SQL transaction",
            ));
        }

        let page_size = self.inner.config.page_size;
        let temp_only = self.statement_is_temp_only(&state.runtime, prepared.statement.as_ref());
        if let Some(prepared_insert) = prepared
            .prepared_insert
            .as_deref()
            .filter(|plan| state.runtime.can_reuse_prepared_simple_insert(plan))
        {
            let result =
                state
                    .runtime
                    .execute_prepared_simple_insert(prepared_insert, params, page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        if matches!(
            prepared.statement.as_ref(),
            crate::sql::ast::Statement::Insert(insert)
                if state.runtime.can_execute_insert_in_place(insert)
        ) {
            let result =
                state
                    .runtime
                    .execute_statement(prepared.statement.as_ref(), params, page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        if state
            .runtime
            .can_execute_statement_in_state_without_clone(prepared.statement.as_ref())
        {
            let result =
                state
                    .runtime
                    .execute_statement(prepared.statement.as_ref(), params, page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        let mut working = state.runtime.clone();
        working.rebuild_stale_indexes(page_size)?;
        let result = working.execute_statement(prepared.statement.as_ref(), params, page_size)?;
        state.runtime = working;
        state.persistent_changed |= !temp_only;
        Ok(result)
    }

    fn execute_statement_in_state(
        &self,
        sql: &str,
        statement: &crate::sql::ast::Statement,
        params: &[Value],
        state: &mut SqlTxnState,
    ) -> Result<QueryResult> {
        if matches!(statement, crate::sql::ast::Statement::Analyze { .. }) {
            return Err(DbError::transaction(
                "ANALYZE is not supported inside an explicit SQL transaction",
            ));
        }
        let temp_only = self.statement_is_temp_only(&state.runtime, statement);
        if let crate::sql::ast::Statement::Insert(insert) = statement {
            if let Some(prepared) = self.prepared_simple_insert(sql, insert, &state.runtime)? {
                let result = state.runtime.execute_prepared_simple_insert(
                    prepared.as_ref(),
                    params,
                    self.inner.config.page_size,
                )?;
                state.persistent_changed |= !temp_only;
                return Ok(result);
            }
        }
        if matches!(
            statement,
            crate::sql::ast::Statement::Insert(insert)
                if state.runtime.can_execute_insert_in_place(insert)
        ) {
            let result =
                state
                    .runtime
                    .execute_statement(statement, params, self.inner.config.page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        if state
            .runtime
            .can_execute_statement_in_state_without_clone(statement)
        {
            let result =
                state
                    .runtime
                    .execute_statement(statement, params, self.inner.config.page_size)?;
            state.persistent_changed |= !temp_only;
            return Ok(result);
        }
        let mut working = state.runtime.clone();
        working.rebuild_stale_indexes(self.inner.config.page_size)?;
        let result = working.execute_statement(statement, params, self.inner.config.page_size)?;
        state.runtime = working;
        state.persistent_changed |= !temp_only;
        Ok(result)
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

fn render_runtime_dump(runtime: &EngineRuntime) -> String {
    let mut lines = Vec::new();

    for table in runtime.catalog.tables.values() {
        lines.push(render_create_table(table));
    }
    for (table_name, table_data) in &runtime.tables {
        if let Some(table) = runtime.catalog.tables.get(table_name) {
            for row in &table_data.rows {
                lines.push(render_insert(table, &row.values));
            }
        }
    }
    for view in runtime.catalog.views.values() {
        lines.push(render_create_view(view));
    }
    for table in runtime.temp_tables.values() {
        lines.push(render_create_table(table));
    }
    for (table_name, table_data) in &runtime.temp_table_data {
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

    lines.join("\n")
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
    use tempfile::TempDir;

    use crate::config::DbConfig;
    use std::sync::Arc;

    use crate::exec::dml::{PreparedInsertColumn, PreparedInsertValueSource, PreparedSimpleInsert};
    use crate::{Db, Value};

    use super::{split_sql_batch, PreparedInsertCache, StatementCache};

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
            db.read_page(page_id).expect("read staged page"),
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
            unique_indexes: Vec::new(),
            insert_indexes: Vec::new(),
            use_generic_validation: false,
            use_generic_index_updates: false,
            compiled_index_state_epoch: 0,
        }
    }

    fn scalar_i64(result: &crate::QueryResult) -> i64 {
        match result.rows()[0].values()[0] {
            Value::Int64(value) => value,
            ref other => panic!("expected INT64 scalar, got {other:?}"),
        }
    }
}
