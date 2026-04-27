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
use std::thread;

use crate::alloc::{EngineAllocHandle, EngineByteBuf};
use crate::config::{DbConfig, WalSyncMode};
use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::storage::PagerHandle;
use crate::vfs::VfsFile;
use crate::vfs::VfsHandle;

use self::async_commit::AsyncCommitState;
use self::background::BgCheckpointer;
use self::delta::apply_page_delta_in_place;
use self::format::{FrameEncoding, WalFrame};
use self::index::{WalIndex, WalVersion};
use self::index_sidecar::WalIndexSidecar;
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
    pub(crate) index_sidecar: Option<Mutex<WalIndexSidecar>>,
    pub(crate) wal_index_hot_set_pages: u32,
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
    /// Number of most-recent versions per page to keep resident before the
    /// demotion pass converts older cold versions to `OnDisk`.
    pub(crate) resident_versions_per_page: u32,
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

#[derive(Debug)]
pub(crate) struct WalWriteState {
    pub(crate) page_batch: EngineByteBuf,
    pub(crate) prepared_pages: Vec<(PageId, Vec<u8>, usize, format::FrameEncoding, u64)>,
    /// Reusable scratch buffer for the per-page delta payload (slice M6).
    /// `encode_page_delta_into` clears and refills this buffer on every
    /// page rather than allocating a fresh `Vec<u8>` per page in the
    /// commit hot path.
    pub(crate) delta_scratch: EngineByteBuf,
}

impl WalWriteState {
    #[must_use]
    pub(crate) fn new(alloc: EngineAllocHandle) -> Self {
        Self {
            page_batch: EngineByteBuf::new_in(alloc.clone()),
            prepared_pages: Vec::new(),
            delta_scratch: EngineByteBuf::new_in(alloc),
        }
    }
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
        expected_checkpoint_epoch: u64,
    ) -> Result<u64> {
        writer::commit_pages_if_latest(
            self,
            pager,
            pages,
            max_page_count,
            expected_latest_lsn,
            expected_checkpoint_epoch,
        )
    }

    pub(crate) fn checkpoint(&self, pager: &PagerHandle, timeout_sec: u64) -> Result<()> {
        checkpoint::checkpoint(self, pager, timeout_sec)
    }

    pub(crate) fn shutdown_background_checkpointer(&self) {
        if let Some(bg) = self.inner.bg_checkpointer.get() {
            bg.shutdown_and_join();
        }
    }

    pub(crate) fn read_page_at_snapshot(
        &self,
        pager: &PagerHandle,
        page_id: PageId,
        snapshot_lsn: u64,
    ) -> Result<Option<Arc<[u8]>>> {
        let mut index = self
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        if index.latest_visible(page_id, snapshot_lsn).is_none() {
            self.promote_spilled_latest_locked(&mut index, page_id, snapshot_lsn)?;
        }
        if index.latest_visible(page_id, snapshot_lsn).is_some() {
            index.touch(page_id);
        }
        self.materialize_latest_visible_locked(&index, pager, page_id, snapshot_lsn)
    }

    pub(crate) fn latest_snapshot(&self) -> u64 {
        self.inner.wal_end_lsn.load(Ordering::Acquire)
    }

    pub(crate) fn checkpoint_epoch(&self) -> u64 {
        self.inner.checkpoint_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn begin_reader(&self) -> Result<ReaderGuard> {
        loop {
            if self.inner.checkpoint_pending.load(Ordering::Acquire) {
                thread::yield_now();
                continue;
            }
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
            if self.inner.checkpoint_pending.load(Ordering::Acquire) {
                drop(_index);
                thread::yield_now();
                continue;
            }
            return self.inner.reader_registry.register(self.latest_snapshot());
        }
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
        let sidecar_count = self
            .inner
            .index_sidecar
            .as_ref()
            .map(|sidecar| {
                sidecar
                    .lock()
                    .expect("wal index sidecar lock should not be poisoned")
                    .version_count()
            })
            .unwrap_or(0);
        Ok(index.version_count() + sidecar_count)
    }

    pub(crate) fn version_counts_by_payload(&self) -> Result<(usize, usize)> {
        let index = self
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        let (resident, on_disk) = index.version_counts_by_payload();
        let (sidecar_resident, sidecar_on_disk) = self
            .inner
            .index_sidecar
            .as_ref()
            .map(|sidecar| {
                sidecar
                    .lock()
                    .expect("wal index sidecar lock should not be poisoned")
                    .version_counts_by_payload()
            })
            .unwrap_or((0, 0));
        Ok((resident + sidecar_resident, on_disk + sidecar_on_disk))
    }

    fn materialize_latest_visible_locked(
        &self,
        index: &WalIndex,
        pager: &PagerHandle,
        page_id: PageId,
        snapshot_lsn: u64,
    ) -> Result<Option<Arc<[u8]>>> {
        let Some(version) = index.latest_visible(page_id, snapshot_lsn) else {
            return Ok(None);
        };
        self.materialize_version_locked(index, pager, page_id, version)
            .map(Some)
    }

    fn promote_spilled_latest_locked(
        &self,
        index: &mut WalIndex,
        page_id: PageId,
        snapshot_lsn: u64,
    ) -> Result<()> {
        let Some(sidecar) = &self.inner.index_sidecar else {
            return Ok(());
        };
        if index.contains_page(page_id) {
            return Ok(());
        }
        let mut sidecar = sidecar
            .lock()
            .expect("wal index sidecar lock should not be poisoned");
        let Some(version) = sidecar.read_latest(page_id)? else {
            return Ok(());
        };
        if version.lsn > snapshot_lsn {
            return Ok(());
        }
        sidecar.clear_latest(page_id)?;
        index.seed_latest(page_id, version);
        if self.inner.reader_registry.active_reader_count()? == 0 {
            self.spill_excess_hot_pages_locked(index, &mut sidecar)?;
        }
        Ok(())
    }

    pub(crate) fn spill_excess_hot_pages_locked(
        &self,
        index: &mut WalIndex,
        sidecar: &mut WalIndexSidecar,
    ) -> Result<()> {
        let hot_set_pages = self.inner.wal_index_hot_set_pages as usize;
        if hot_set_pages == 0 {
            return Ok(());
        }
        while let Some((page_id, version)) = index.spill_one_cold_latest(hot_set_pages) {
            sidecar.write_latest(page_id, &version)?;
        }
        Ok(())
    }

    fn materialize_version_locked(
        &self,
        index: &WalIndex,
        pager: &PagerHandle,
        page_id: PageId,
        version: &WalVersion,
    ) -> Result<Arc<[u8]>> {
        match &version.payload {
            index::WalVersionPayload::Resident { data, .. } => Ok(Arc::clone(data)),
            index::WalVersionPayload::OnDisk {
                wal_offset,
                frame_len,
                encoding,
            } => self.materialize_on_disk_version_locked(
                index,
                pager,
                page_id,
                version.lsn,
                *wal_offset,
                *frame_len,
                *encoding,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_on_disk_version_locked(
        &self,
        index: &WalIndex,
        pager: &PagerHandle,
        page_id: PageId,
        version_lsn: u64,
        wal_offset: u64,
        frame_len: u32,
        encoding: FrameEncoding,
    ) -> Result<Arc<[u8]>> {
        let logical_end = self.latest_snapshot();
        let frame = WalFrame::decode_from_file(
            self.inner.file.as_ref(),
            wal_offset,
            self.inner.page_size,
            logical_end,
        )?
        .ok_or_else(|| {
            DbError::corruption(format!(
                "WAL frame at offset {wal_offset} for page {page_id} is truncated"
            ))
        })?;
        if frame.page_id != page_id {
            return Err(DbError::corruption(format!(
                "WAL frame at offset {wal_offset} belongs to page {}, expected {page_id}",
                frame.page_id
            )));
        }
        let expected_len = frame.encoded_len(self.inner.page_size) as u32;
        if expected_len != frame_len {
            return Err(DbError::corruption(format!(
                "WAL frame length mismatch at offset {wal_offset}: index has {frame_len}, decoded {expected_len}"
            )));
        }
        if frame.frame_type != encoding.frame_type() {
            return Err(DbError::corruption(format!(
                "WAL frame encoding mismatch at offset {wal_offset}"
            )));
        }
        match encoding {
            FrameEncoding::Page => Ok(Arc::from(frame.payload)),
            FrameEncoding::PageDelta => {
                let mut base = if let Some(previous) = self.materialize_latest_visible_locked(
                    index,
                    pager,
                    page_id,
                    version_lsn.saturating_sub(1),
                )? {
                    previous.to_vec()
                } else {
                    pager.read_page(page_id)?.to_vec()
                };
                apply_page_delta_in_place(&mut base, &frame.payload)?;
                Ok(Arc::from(base))
            }
        }
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
            .read_page_at_snapshot(&pager, page_id, snapshot_lsn)
            .expect("read snapshot page")
            .expect("page should be in wal");
        let second = wal
            .read_page_at_snapshot(&pager, page_id, snapshot_lsn)
            .expect("read snapshot page again")
            .expect("page should be in wal");

        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::strong_count(&first) >= 2);
        assert!(mem_vfs.is_memory());
    }

    #[test]
    fn read_page_at_snapshot_materializes_demoted_delta_versions() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new(":memory:");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_resident_versions_per_page = 0;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager).expect("acquire wal");
        let page_id = page::CATALOG_ROOT_PAGE_ID + 1;

        let first_payload = vec![0x11; page::DEFAULT_PAGE_SIZE as usize];
        let first_snapshot = wal
            .commit_pages(&pager, vec![(page_id, first_payload.clone())], page_id)
            .expect("commit first page");
        assert_eq!(
            wal.version_counts_by_payload().expect("payload counts"),
            (0, 1)
        );

        let reader = wal.begin_reader().expect("begin reader");
        let mut second_payload = first_payload.clone();
        second_payload[64..68].copy_from_slice(b"m4!!");
        let second_snapshot = wal
            .commit_pages(&pager, vec![(page_id, second_payload.clone())], page_id)
            .expect("commit second page");
        assert_eq!(reader.snapshot_lsn(), first_snapshot);
        assert_eq!(
            wal.version_counts_by_payload().expect("payload counts"),
            (0, 2)
        );
        drop(reader);

        let other_page_id = page_id + 1;
        let third_payload = vec![0x22; page::DEFAULT_PAGE_SIZE as usize];
        wal.commit_pages(&pager, vec![(other_page_id, third_payload)], other_page_id)
            .expect("commit third page");
        let (resident_versions, on_disk_versions) =
            wal.version_counts_by_payload().expect("payload counts");
        assert_eq!(resident_versions, 0);
        assert!(on_disk_versions >= 3, "expected page history to demote");

        let first_page = wal
            .read_page_at_snapshot(&pager, page_id, first_snapshot)
            .expect("read first snapshot")
            .expect("page should exist");
        assert_eq!(first_page.as_ref(), first_payload.as_slice());

        let second_page = wal
            .read_page_at_snapshot(&pager, page_id, second_snapshot)
            .expect("read second snapshot")
            .expect("page should exist");
        assert_eq!(second_page.as_ref(), second_payload.as_slice());
    }

    #[test]
    fn read_page_at_snapshot_latest_delta_without_history_uses_disk_base_only() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new(":memory:");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_resident_versions_per_page = 0;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager).expect("acquire wal");
        let page_id = page::CATALOG_ROOT_PAGE_ID + 1;

        let base = vec![0x10; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(page_id, &base)
            .expect("seed base page");

        let mut first_payload = base.clone();
        first_payload[32..40].copy_from_slice(b"delta-v1");
        wal.commit_pages(&pager, vec![(page_id, first_payload)], page_id)
            .expect("commit first page");

        let mut second_payload = base.clone();
        second_payload[32..40].copy_from_slice(b"delta-v2");
        let second_snapshot = wal
            .commit_pages(&pager, vec![(page_id, second_payload.clone())], page_id)
            .expect("commit second page");

        let latest_page = wal
            .read_page_at_snapshot(&pager, page_id, second_snapshot)
            .expect("read latest snapshot")
            .expect("page should exist");
        assert_eq!(latest_page.as_ref(), second_payload.as_slice());
    }

    #[test]
    fn read_page_at_snapshot_promotes_spilled_full_page_version() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new("spill-promote.ddb");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_index_hot_set_pages = 1;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager).expect("acquire wal");

        let page_one = page::CATALOG_ROOT_PAGE_ID + 1;
        let page_two = page_one + 1;
        let payload_one = vec![0x31; page::DEFAULT_PAGE_SIZE as usize];
        let payload_two = vec![0x42; page::DEFAULT_PAGE_SIZE as usize];
        let snapshot_one = wal
            .commit_pages(&pager, vec![(page_one, payload_one.clone())], page_two)
            .expect("commit page one");
        let snapshot_two = wal
            .commit_pages(&pager, vec![(page_two, payload_two.clone())], page_two)
            .expect("commit page two");

        assert_eq!(wal.version_count().expect("version count"), 2);
        assert_eq!(
            wal.version_counts_by_payload().expect("payload counts"),
            (1, 1)
        );

        let spilled = wal
            .read_page_at_snapshot(&pager, page_one, snapshot_one)
            .expect("read spilled page")
            .expect("page one should be in wal");
        let hot = wal
            .read_page_at_snapshot(&pager, page_two, snapshot_two)
            .expect("read hot page")
            .expect("page two should be in wal");
        assert_eq!(&*spilled, payload_one.as_slice());
        assert_eq!(&*hot, payload_two.as_slice());
        assert_eq!(wal.version_count().expect("version count"), 2);
    }

    #[test]
    fn read_page_at_snapshot_promotes_spilled_delta_version() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new("spill-promote-delta.ddb");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_index_hot_set_pages = 1;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager).expect("acquire wal");

        let page_one = page::CATALOG_ROOT_PAGE_ID + 1;
        let page_two = page_one + 1;
        let base_one = vec![0x31; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(page_one, &base_one)
            .expect("seed base page");
        let mut delta_one = base_one.clone();
        delta_one[0] = 0x7A;
        delta_one[17] = 0x55;
        let payload_two = vec![0x42; page::DEFAULT_PAGE_SIZE as usize];
        let snapshot_one = wal
            .commit_pages(&pager, vec![(page_one, delta_one.clone())], page_two)
            .expect("commit delta page one");
        let snapshot_two = wal
            .commit_pages(&pager, vec![(page_two, payload_two.clone())], page_two)
            .expect("commit page two");

        assert_eq!(wal.version_count().expect("version count"), 2);
        assert_eq!(
            wal.version_counts_by_payload().expect("payload counts"),
            (1, 1)
        );

        let spilled = wal
            .read_page_at_snapshot(&pager, page_one, snapshot_one)
            .expect("read spilled delta page")
            .expect("page one should be in wal");
        let hot = wal
            .read_page_at_snapshot(&pager, page_two, snapshot_two)
            .expect("read hot page")
            .expect("page two should be in wal");
        assert_eq!(&*spilled, delta_one.as_slice());
        assert_eq!(&*hot, payload_two.as_slice());
        assert_eq!(wal.version_count().expect("version count"), 2);
    }

    #[test]
    fn checkpoint_copies_back_spilled_full_page_versions_and_clears_sidecar() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new("spill-checkpoint.ddb");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_index_hot_set_pages = 1;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager).expect("acquire wal");

        let page_one = page::CATALOG_ROOT_PAGE_ID + 1;
        let page_two = page_one + 1;
        let payload_one = vec![0x55; page::DEFAULT_PAGE_SIZE as usize];
        let payload_two = vec![0x66; page::DEFAULT_PAGE_SIZE as usize];
        wal.commit_pages(&pager, vec![(page_one, payload_one.clone())], page_two)
            .expect("commit page one");
        wal.commit_pages(&pager, vec![(page_two, payload_two.clone())], page_two)
            .expect("commit page two");

        assert_eq!(wal.version_count().expect("version count"), 2);
        wal.checkpoint(&pager, 0).expect("checkpoint");
        assert_eq!(wal.version_count().expect("version count"), 0);
        assert_eq!(
            pager.read_page(page_one).expect("read page one").as_ref(),
            payload_one.as_slice()
        );
        assert_eq!(
            pager.read_page(page_two).expect("read page two").as_ref(),
            payload_two.as_slice()
        );
    }

    #[test]
    fn checkpoint_copies_back_spilled_delta_versions_and_clears_sidecar() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new("spill-checkpoint-delta.ddb");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_index_hot_set_pages = 1;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager).expect("acquire wal");

        let page_one = page::CATALOG_ROOT_PAGE_ID + 1;
        let page_two = page_one + 1;
        let base_one = vec![0x55; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(page_one, &base_one)
            .expect("seed base page");
        let mut delta_one = base_one.clone();
        delta_one[3] = 0x66;
        delta_one[9] = 0x77;
        let payload_two = vec![0x88; page::DEFAULT_PAGE_SIZE as usize];
        wal.commit_pages(&pager, vec![(page_one, delta_one.clone())], page_two)
            .expect("commit delta page one");
        wal.commit_pages(&pager, vec![(page_two, payload_two.clone())], page_two)
            .expect("commit page two");

        assert_eq!(wal.version_count().expect("version count"), 2);
        wal.checkpoint(&pager, 0).expect("checkpoint");
        assert_eq!(wal.version_count().expect("version count"), 0);
        assert_eq!(
            pager.read_page(page_one).expect("read page one").as_ref(),
            delta_one.as_slice()
        );
        assert_eq!(
            pager.read_page(page_two).expect("read page two").as_ref(),
            payload_two.as_slice()
        );
    }
}
