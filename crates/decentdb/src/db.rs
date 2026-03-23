//! Stable database owner and bootstrap lifecycle entry points.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::catalog::CatalogHandle;
use crate::config::DbConfig;
use crate::error::{DbError, Result};
use crate::storage::page::{self, PageId};
use crate::storage::{self, DatabaseHeader, PagerHandle};
use crate::vfs::faulty::{self, FailAction, Failpoint};
use crate::vfs::{FileKind, OpenMode, VfsHandle};
use crate::wal::reader_registry::ReaderGuard;
use crate::wal::WalHandle;

/// Stable engine owner used across later storage, SQL, and FFI slices.
#[derive(Clone, Debug)]
pub struct Db {
    inner: Arc<DbInner>,
}

#[derive(Debug)]
struct DbInner {
    path: PathBuf,
    config: DbConfig,
    _vfs: VfsHandle,
    pager: PagerHandle,
    wal: WalHandle,
    _catalog: CatalogHandle,
    write_txn: Mutex<WriteTxn>,
    held_snapshots: Mutex<HashMap<u64, ReaderGuard>>,
}

#[derive(Debug, Default)]
struct WriteTxn {
    active: bool,
    staged_pages: BTreeMap<PageId, Vec<u8>>,
}

impl Db {
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
        let txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "allocate_page requires an active write transaction",
            ));
        }
        drop(txn);
        self.inner.pager.allocate_page()
    }

    /// Frees an existing page back to the freelist.
    pub fn free_page(&self, page_id: u32) -> Result<()> {
        let txn = self
            .inner
            .write_txn
            .lock()
            .map_err(|_| DbError::internal("write transaction lock poisoned"))?;
        if !txn.active {
            return Err(DbError::transaction(
                "free_page requires an active write transaction",
            ));
        }
        drop(txn);
        self.inner.pager.free_page(page_id)
    }

    /// Commits the current write transaction to the WAL.
    pub fn commit(&self) -> Result<u64> {
        let pages = {
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
            std::mem::take(&mut txn.staged_pages)
        };

        let pages: Vec<_> = pages.into_iter().collect();
        let max_page_id = pages.iter().map(|(page_id, _)| *page_id).max().unwrap_or(0);
        let max_page_count = self
            .inner
            .pager
            .on_disk_page_count()?
            .max(self.inner.wal.max_page_count())
            .max(max_page_id);
        self.inner.wal.commit_pages(&pages, max_page_count)
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

        let snapshot_lsn = self.inner.wal.latest_snapshot();
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
            "{{\"path\":\"{}\",\"page_size\":{},\"page_count\":{},\"wal_end_lsn\":{},\"wal_file_size\":{},\"wal_path\":\"{}\",\"last_checkpoint_lsn\":{},\"active_readers\":{},\"wal_versions\":{},\"warning_count\":{},\"shared_wal\":{}}}",
            json_escape(self.path().display().to_string()),
            self.inner.config.page_size,
            self.inner.pager.on_disk_page_count()?,
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

        let pager = PagerHandle::open(Arc::clone(&file), header, effective_config.cache_size_mb)?;
        let wal = WalHandle::acquire(
            &vfs,
            &path,
            effective_config.page_size,
            effective_config.wal_sync_mode,
        )?;
        wal.set_max_page_count(pager.on_disk_page_count()?);

        Ok(Self {
            inner: Arc::new(DbInner {
                path,
                config: effective_config,
                _vfs: vfs,
                pager,
                wal,
                _catalog: CatalogHandle::placeholder(),
                write_txn: Mutex::new(WriteTxn::default()),
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
}

fn json_escape(input: String) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
