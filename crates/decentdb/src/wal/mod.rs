//! Write-ahead log ownership, recovery, and checkpointing.

pub(crate) mod checkpoint;
pub(crate) mod delta;
pub(crate) mod format;
pub(crate) mod index;
pub(crate) mod reader_registry;
pub(crate) mod recovery;
pub(crate) mod savepoint;
pub(crate) mod shared;
pub(crate) mod writer;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::config::WalSyncMode;
use crate::error::Result;
use crate::storage::page::PageId;
use crate::storage::PagerHandle;
use crate::vfs::VfsFile;
use crate::vfs::VfsHandle;

use self::index::WalIndex;
use self::reader_registry::{ReaderGuard, ReaderRegistry};

#[derive(Clone, Debug)]
pub(crate) struct WalHandle {
    inner: Arc<SharedWalInner>,
}

#[derive(Debug)]
pub(crate) struct SharedWalInner {
    canonical_path: Option<PathBuf>,
    file: Arc<dyn VfsFile>,
    page_size: u32,
    sync_mode: WalSyncMode,
    index: Mutex<WalIndex>,
    wal_end_lsn: AtomicU64,
    max_page_count: AtomicU32,
    allocated_len: AtomicU64,
    write_lock: Mutex<WalWriteState>,
    reader_registry: ReaderRegistry,
    checkpoint_pending: AtomicBool,
    checkpoint_epoch: AtomicU64,
}

#[derive(Debug, Default)]
pub(crate) struct WalWriteState {
    pub(crate) page_batch: Vec<u8>,
    pub(crate) prepared_pages: Vec<(PageId, Vec<u8>, usize)>,
}

impl WalHandle {
    pub(crate) fn acquire(
        vfs: &VfsHandle,
        db_path: &Path,
        page_size: u32,
        sync_mode: WalSyncMode,
        pager: &PagerHandle,
    ) -> Result<Self> {
        shared::acquire(vfs, db_path, page_size, sync_mode, pager)
    }

    pub(crate) fn evict(vfs: &VfsHandle, db_path: &Path) -> Result<()> {
        shared::evict(vfs, db_path)
    }

    pub(crate) fn commit_pages(
        &self,
        pager: &PagerHandle,
        pages: Vec<(PageId, Vec<u8>)>,
        max_page_count: u32,
    ) -> Result<u64> {
        writer::commit_pages(self, pager, pages, max_page_count)
    }

    pub(crate) fn commit_pages_if_latest(
        &self,
        pager: &PagerHandle,
        pages: Vec<(PageId, Vec<u8>)>,
        max_page_count: u32,
        expected_latest_lsn: u64,
    ) -> Result<u64> {
        writer::commit_pages_if_latest(self, pager, pages, max_page_count, expected_latest_lsn)
    }

    pub(crate) fn checkpoint(&self, pager: &PagerHandle, timeout_sec: u64) -> Result<()> {
        checkpoint::checkpoint(self, pager, timeout_sec)
    }

    pub(crate) fn read_page_at_snapshot(
        &self,
        page_id: PageId,
        snapshot_lsn: u64,
    ) -> Result<Option<Vec<u8>>> {
        let index = self
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        Ok(index
            .latest_visible(page_id, snapshot_lsn)
            .map(|version| version.data.clone()))
    }

    pub(crate) fn latest_snapshot(&self) -> u64 {
        self.inner.wal_end_lsn.load(Ordering::Acquire)
    }

    pub(crate) fn checkpoint_epoch(&self) -> u64 {
        self.inner.checkpoint_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn begin_reader(&self) -> Result<ReaderGuard> {
        // Hold the index lock while reading wal_end_lsn and registering the
        // reader. This guarantees mutual exclusion with the writer's
        // retain_history check: either the writer sees our active_count
        // increment (and retains history), or we observe the post-commit
        // wal_end_lsn (and don't need old versions).
        let _index = self
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        self.inner.reader_registry.register(self.latest_snapshot())
    }

    pub(crate) fn set_max_page_count(&self, page_count: u32) {
        self.inner
            .max_page_count
            .fetch_max(page_count, Ordering::AcqRel);
    }

    pub(crate) fn reset_max_page_count(&self, page_count: u32) {
        self.inner
            .max_page_count
            .store(page_count, Ordering::Release);
    }

    pub(crate) fn max_page_count(&self) -> u32 {
        self.inner.max_page_count.load(Ordering::Acquire)
    }

    pub(crate) fn active_reader_count(&self) -> Result<usize> {
        self.inner.reader_registry.active_reader_count()
    }

    pub(crate) fn warnings(&self) -> Result<Vec<String>> {
        self.inner.reader_registry.warnings()
    }

    pub(crate) fn version_count(&self) -> Result<usize> {
        let index = self
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        Ok(index.version_count())
    }

    pub(crate) fn file_size(&self) -> Result<u64> {
        self.inner.file.file_size()
    }

    pub(crate) fn file_path(&self) -> &Path {
        self.inner.file.path()
    }

    pub(crate) fn is_shared(&self) -> bool {
        self.inner.canonical_path.is_some()
    }

    pub(crate) fn strong_handle_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    pub(crate) fn set_checkpoint_pending(&self, pending: bool) {
        self.inner
            .checkpoint_pending
            .store(pending, Ordering::SeqCst);
    }
}
