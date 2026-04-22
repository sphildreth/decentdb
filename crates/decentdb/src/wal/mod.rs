//! Write-ahead log ownership, recovery, and checkpointing.

pub(crate) mod async_commit;
pub(crate) mod background;
pub(crate) mod checkpoint;
pub(crate) mod delta;
pub(crate) mod format;
pub(crate) mod index;
pub(crate) mod index_sidecar;
pub(crate) mod platform;
pub(crate) mod reader_registry;
pub(crate) mod recovery;
pub(crate) mod savepoint;
pub(crate) mod shared;
pub(crate) mod writer;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::config::{DbConfig, WalSyncMode};
use crate::error::Result;
use crate::storage::page::PageId;
use crate::storage::PagerHandle;
use crate::vfs::VfsFile;
use crate::vfs::VfsHandle;

use self::async_commit::AsyncCommitState;
use self::background::BgCheckpointer;
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
    /// `Some` when `sync_mode` is `WalSyncMode::AsyncCommit { .. }`; owns the
    /// background flusher thread and durability watermark. Constructed lazily
    /// in `build_handle` and torn down when `SharedWalInner` is dropped (which
    /// joins the thread and performs a final synchronous flush).
    pub(super) async_commit: Option<AsyncCommitState>,
    /// Auto-checkpoint and post-checkpoint memory-release tuning.
    /// Snapshotted from `DbConfig` at first acquisition; subsequent opens of
    /// the same WAL share these values via the registry (matches existing
    /// `sync_mode` / `page_size` behaviour). See ADR 0137 / 0138.
    pub(crate) auto_checkpoint: AutoCheckpointConfig,
    /// Number of dirty page versions that have been added to the WAL index
    /// since the last successful checkpoint. Reset by `checkpoint::checkpoint`
    /// after pruning. See ADR 0137.
    pub(crate) pages_since_checkpoint: AtomicU32,
    /// Reusable scratch buffer for `checkpoint::checkpoint`; see slice M5.
    /// Avoids a fresh allocation of the per-checkpoint
    /// `Vec<(PageId, WalVersion)>` materialised by
    /// `WalIndex::latest_versions_at_or_before`. The buffer is held under
    /// the writer lock so this `Mutex` is uncontended in practice.
    pub(crate) checkpoint_scratch: Mutex<Vec<(PageId, index::WalVersion)>>,
    /// Optional background checkpoint worker (ADR 0058). Set once during
    /// `shared::build_handle` when the embedder opts in via
    /// `DbConfig::background_checkpoint_worker`. `OnceLock` is used so
    /// `SharedWalInner::drop` can `take()` it and signal the worker to
    /// shut down before joining the thread.
    pub(crate) bg_checkpointer: OnceLock<BgCheckpointer>,
}

/// Snapshot of the checkpoint-related `DbConfig` fields. Held inside
/// `SharedWalInner` so the writer can evaluate auto-checkpoint thresholds
/// without re-threading config through every call site.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AutoCheckpointConfig {
    pub(crate) threshold_pages: u32,
    pub(crate) threshold_bytes: u64,
    pub(crate) checkpoint_timeout_sec: u64,
    pub(crate) release_freed_after_checkpoint: bool,
}

impl AutoCheckpointConfig {
    pub(crate) fn from_db_config(cfg: &DbConfig) -> Self {
        Self {
            threshold_pages: cfg.wal_checkpoint_threshold_pages,
            threshold_bytes: cfg.wal_checkpoint_threshold_bytes,
            checkpoint_timeout_sec: cfg.checkpoint_timeout_sec,
            release_freed_after_checkpoint: cfg.release_freed_memory_after_checkpoint,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct WalWriteState {
    pub(crate) page_batch: Vec<u8>,
    pub(crate) prepared_pages: Vec<(PageId, Vec<u8>, usize)>,
    /// Reusable scratch buffer for the per-page delta payload (slice M6).
    /// `encode_page_delta_into` clears and refills this buffer on every
    /// page rather than allocating a fresh `Vec<u8>` per page in the
    /// commit hot path.
    pub(crate) delta_scratch: Vec<u8>,
}

impl WalHandle {
    pub(crate) fn acquire(
        vfs: &VfsHandle,
        db_path: &Path,
        config: &DbConfig,
        pager: &PagerHandle,
    ) -> Result<Self> {
        shared::acquire(vfs, db_path, config, pager)
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
    ) -> Result<Option<Arc<[u8]>>> {
        let index = self
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        Ok(index
            .latest_visible(page_id, snapshot_lsn)
            .map(|version| version.payload.arc()))
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

    /// Blocks until every commit acknowledged before this call is durable on
    /// disk. For sync modes other than `AsyncCommit` this is a no-op because
    /// commits are already synchronously durable.
    pub(crate) fn flush_to_durable(&self) -> Result<()> {
        match self.inner.async_commit.as_ref() {
            Some(state) => state.flush_to_durable(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use crate::config::{DbConfig, WalSyncMode};
    use crate::storage::page;
    use crate::storage::{write_database_bootstrap_vfs, DatabaseHeader, PagerHandle};
    use crate::vfs::mem::MemVfs;
    use crate::vfs::{FileKind, OpenMode, Vfs, VfsHandle};

    use super::WalHandle;

    fn test_pager(vfs: &VfsHandle, path: &Path) -> PagerHandle {
        let file = vfs
            .open(path, OpenMode::OpenOrCreate, FileKind::Database)
            .expect("create database file");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap db");
        PagerHandle::open(Arc::clone(&file), header, 1).expect("open pager")
    }

    fn test_config() -> DbConfig {
        DbConfig {
            wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
            // Disable auto-checkpoint inside this low-level test so the
            // single explicit commit produces an observable WAL state.
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            ..DbConfig::default()
        }
    }

    #[test]
    fn read_page_at_snapshot_shares_backing_allocation() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new(":memory:");
        let pager = test_pager(&vfs, db_path);
        let cfg = test_config();
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager).expect("acquire wal");
        let page_id = page::CATALOG_ROOT_PAGE_ID + 1;
        let payload = vec![0x5A; page::DEFAULT_PAGE_SIZE as usize];
        let snapshot_lsn = wal
            .commit_pages(&pager, vec![(page_id, payload)], page_id)
            .expect("commit page");

        let first = wal
            .read_page_at_snapshot(page_id, snapshot_lsn)
            .expect("read snapshot page")
            .expect("page should be in wal");
        let second = wal
            .read_page_at_snapshot(page_id, snapshot_lsn)
            .expect("read snapshot page again")
            .expect("page should be in wal");

        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::strong_count(&first) >= 2);
        assert!(mem_vfs.is_memory());
    }
}
