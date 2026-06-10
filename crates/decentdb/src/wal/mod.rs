//! Write-ahead log ownership, recovery, and checkpointing.

pub(crate) mod async_commit;
pub(crate) mod background;
pub(crate) mod checkpoint;
pub(crate) mod coordination;
pub(crate) mod delta;
#[cfg(test)]
mod delta_tests;
pub(crate) mod format;
#[cfg(test)]
mod format_tests;
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

use crate::config::{DbConfig, WalSyncMode};
use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::storage::PagerHandle;
use crate::vfs::VfsFile;
use crate::vfs::VfsHandle;

#[cfg(feature = "bench-internals")]
use crate::benchmark::{
    WAL_DELTA_MATERIALIZE_CALLS, WAL_DELTA_SCRATCH_GROWS, WAL_DELTA_SCRATCH_REUSES,
};

use self::async_commit::AsyncCommitState;
use self::background::BgCheckpointer;
use self::coordination::{
    ProcessCoordinationSnapshot, ProcessCoordinator, ProcessLockMetricsSnapshot,
    ProcessReaderGuard, ProcessReaderSlotSnapshot, ReaderRetentionSnapshot,
};
use self::delta::apply_page_delta_in_place;
use self::format::{FrameEncoding, WalFrame};
use self::index::{WalIndex, WalVersion};
use self::index_sidecar::WalIndexSidecar;
use self::reader_registry::{ReaderGuard, ReaderRegistry};

const NO_RETAINED_SNAPSHOT_LSN: u64 = u64::MAX;

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
    retained_snapshot_lsn: AtomicU64,
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
    /// Reusable mutable page image for delta-frame materialization.
    pub(crate) materialize_scratch: Mutex<Vec<u8>>,
    /// Whether auto-checkpoint threshold hits should use the background
    /// worker instead of checkpointing on the writer thread.
    pub(crate) background_checkpoint_worker: bool,
    /// Optional background checkpoint worker (ADR 0058). Started lazily on
    /// the first auto-checkpoint threshold hit so opens that never need a
    /// worker do not pay thread-spawn cost. `OnceLock` is used so
    /// `SharedWalInner::drop` can `take()` it and signal the worker to shut
    /// down before joining the thread.
    pub(crate) bg_checkpointer: OnceLock<BgCheckpointer>,
    pub(crate) process_coordinator: Option<ProcessCoordinator>,
    pub(crate) observed_coord_wal_generation: AtomicU64,
    pub(crate) observed_coord_checkpoint_generation: AtomicU64,
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

pub(crate) type WalBasePage = Option<(Arc<[u8]>, bool)>;

#[derive(Debug)]
pub(crate) struct WalWriteState {
    pub(crate) page_batch: Vec<u8>,
    pub(crate) prepared_pages: Vec<(PageId, Vec<u8>, usize, format::FrameEncoding, u64)>,
    pub(crate) base_pages: Vec<WalBasePage>,
    /// Reusable scratch buffer for the per-page delta payload (slice M6).
    /// `encode_page_delta_into` clears and refills this buffer on every
    /// page rather than allocating a fresh `Vec<u8>` per page in the
    /// commit hot path.
    pub(crate) delta_scratch: Vec<u8>,
}

impl WalWriteState {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            page_batch: Vec::new(),
            prepared_pages: Vec::new(),
            base_pages: Vec::new(),
            delta_scratch: Vec::new(),
        }
    }
}

impl WalHandle {
    pub(crate) fn acquire(
        vfs: &VfsHandle,
        db_path: &Path,
        config: &DbConfig,
        pager: &PagerHandle,
        process_coordinator: Option<ProcessCoordinator>,
    ) -> Result<Self> {
        shared::acquire(vfs, db_path, config, pager, process_coordinator)
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

    pub(crate) fn begin_deferred_group_commit(&self) -> writer::DeferredGroupCommitGuard {
        writer::begin_deferred_group_commit()
    }

    pub(crate) fn flush_deferred_group_commit(&self) -> Result<bool> {
        writer::flush_deferred_group_commit(self)
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
        self.begin_reader_with_process_guard(None)
    }

    pub(crate) fn begin_reader_with_pager(&self, pager: &PagerHandle) -> Result<ReaderGuard> {
        if self.inner.process_coordinator.is_none() {
            return self.begin_reader();
        }
        let mut delay = std::time::Duration::from_micros(100);
        for _ in 0..8 {
            self.refresh_from_coordination(pager)?;
            let before = self.coordination_header_snapshot()?;
            let guard = self.begin_reader_with_process_slot()?;
            let after = self.coordination_header_snapshot()?;
            if before
                .as_ref()
                .zip(after.as_ref())
                .is_none_or(|(before, after)| {
                    before.wal_generation == after.wal_generation
                        && before.checkpoint_generation == after.checkpoint_generation
                })
            {
                return Ok(guard);
            }
            drop(guard);
            std::thread::sleep(delay);
            delay = (delay * 2).min(std::time::Duration::from_millis(5));
        }
        self.refresh_from_coordination(pager)?;
        self.begin_reader_with_process_slot()
    }

    fn begin_reader_with_process_slot(&self) -> Result<ReaderGuard> {
        let process_guard = if let Some(coordinator) = &self.inner.process_coordinator {
            let reader_id = self.inner.reader_registry.next_reader_id();
            Some(coordinator.begin_reader(reader_id, self.latest_snapshot())?)
        } else {
            None
        };
        self.begin_reader_with_process_guard(process_guard)
    }

    fn begin_reader_with_process_guard(
        &self,
        process_guard: Option<ProcessReaderGuard>,
    ) -> Result<ReaderGuard> {
        let mut process_guard = process_guard;
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
            return self
                .inner
                .reader_registry
                .register_with_process_guard(self.latest_snapshot(), process_guard.take());
        }
    }

    pub(crate) fn lock_process_writer(&self) -> Result<Option<coordination::ProcessWriterGuard>> {
        self.inner
            .process_coordinator
            .as_ref()
            .map(ProcessCoordinator::lock_writer)
            .transpose()
    }

    pub(crate) fn lock_process_checkpoint(
        &self,
    ) -> Result<Option<coordination::ProcessWriterGuard>> {
        self.inner
            .process_coordinator
            .as_ref()
            .map(ProcessCoordinator::lock_checkpoint)
            .transpose()
    }

    #[allow(dead_code)]
    #[allow(clippy::type_complexity)]
    pub(crate) fn set_process_lock_wait_callback(
        &self,
        callback: Option<
            std::sync::Arc<
                dyn Fn(bool, std::time::Duration, &str) + Send + Sync,
            >,
        >,
    ) {
        if let Some(ref coordinator) = self.inner.process_coordinator {
            coordinator.set_lock_wait_callback(callback);
        }
    }

    pub(crate) fn publish_process_commit(&self, wal_end_lsn: u64) -> Result<()> {
        if let Some(coordinator) = &self.inner.process_coordinator {
            let snapshot = coordinator.publish_commit(wal_end_lsn)?;
            self.record_observed_coordination_snapshot(&snapshot);
        }
        Ok(())
    }

    pub(crate) fn publish_process_checkpoint(
        &self,
        checkpoint_lsn: u64,
        wal_end_lsn: u64,
    ) -> Result<()> {
        if let Some(coordinator) = &self.inner.process_coordinator {
            let snapshot = coordinator.publish_checkpoint(checkpoint_lsn, wal_end_lsn)?;
            self.record_observed_coordination_snapshot(&snapshot);
        }
        Ok(())
    }

    pub(crate) fn refresh_from_coordination(&self, pager: &PagerHandle) -> Result<()> {
        let Some(coordinator) = &self.inner.process_coordinator else {
            return Ok(());
        };
        let snapshot = coordinator.snapshot()?;
        let observed_wal = self
            .inner
            .observed_coord_wal_generation
            .load(Ordering::Acquire);
        let observed_checkpoint = self
            .inner
            .observed_coord_checkpoint_generation
            .load(Ordering::Acquire);
        if snapshot.wal_generation == observed_wal
            && snapshot.checkpoint_generation == observed_checkpoint
            && snapshot.wal_end_lsn == self.latest_snapshot()
        {
            return Ok(());
        }
        if self.inner.reader_registry.active_reader_count()? > 0
            || self.retained_snapshot_lsn().is_some()
        {
            return Ok(());
        }

        let result = (|| {
            let _writer_state = self
                .inner
                .write_lock
                .lock()
                .expect("wal write lock should not be poisoned");
            if snapshot.checkpoint_generation != observed_checkpoint {
                let header = pager.header_from_disk()?;
                pager.refresh_from_disk(header)?;
            }
            let mut sidecar = self.inner.index_sidecar.as_ref().map(|sidecar| {
                sidecar
                    .lock()
                    .expect("wal index sidecar lock should not be poisoned")
            });
            if let Some(sidecar) = sidecar.as_mut() {
                sidecar.clear()?;
            }
            let (index, end_lsn, recovered_max_page_id) =
                crate::wal::recovery::initialize_or_recover(
                    &self.inner.file,
                    pager,
                    self.inner.page_size,
                    self.inner.wal_index_hot_set_pages,
                    sidecar.as_deref_mut(),
                )?;
            {
                let mut current = self
                    .inner
                    .index
                    .lock()
                    .expect("wal index lock should not be poisoned");
                *current = index;
            }
            self.inner.wal_end_lsn.store(end_lsn, Ordering::Release);
            self.inner
                .max_page_count
                .fetch_max(recovered_max_page_id, Ordering::AcqRel);
            self.inner
                .observed_coord_wal_generation
                .store(snapshot.wal_generation, Ordering::Release);
            self.inner
                .observed_coord_checkpoint_generation
                .store(snapshot.checkpoint_generation, Ordering::Release);
            Ok(())
        })();
        coordinator.mark_refresh_result(&result);
        result
    }

    pub(crate) fn process_reader_retention(&self) -> Result<Option<ReaderRetentionSnapshot>> {
        self.inner
            .process_coordinator
            .as_ref()
            .map(ProcessCoordinator::scan_reader_retention)
            .transpose()
    }

    pub(crate) fn process_coordination_snapshot(
        &self,
    ) -> Result<Option<ProcessCoordinationSnapshot>> {
        self.inner
            .process_coordinator
            .as_ref()
            .map(ProcessCoordinator::coordination_snapshot)
            .transpose()
    }

    pub(crate) fn process_lock_metrics_snapshot(
        &self,
    ) -> Result<Option<ProcessLockMetricsSnapshot>> {
        self.inner
            .process_coordinator
            .as_ref()
            .map(ProcessCoordinator::lock_metrics_snapshot)
            .transpose()
    }

    pub(crate) fn process_reader_slot_snapshots(
        &self,
    ) -> Result<Option<Vec<ProcessReaderSlotSnapshot>>> {
        self.inner
            .process_coordinator
            .as_ref()
            .map(ProcessCoordinator::reader_slot_snapshots)
            .transpose()
    }

    fn coordination_header_snapshot(
        &self,
    ) -> Result<Option<coordination::CoordinationHeaderSnapshot>> {
        self.inner
            .process_coordinator
            .as_ref()
            .map(ProcessCoordinator::snapshot)
            .transpose()
    }

    fn record_observed_coordination_snapshot(
        &self,
        snapshot: &coordination::CoordinationHeaderSnapshot,
    ) {
        self.inner
            .observed_coord_wal_generation
            .store(snapshot.wal_generation, Ordering::Release);
        self.inner
            .observed_coord_checkpoint_generation
            .store(snapshot.checkpoint_generation, Ordering::Release);
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

    pub(crate) fn set_retained_snapshot_lsn(&self, snapshot_lsn: Option<u64>) {
        self.inner.retained_snapshot_lsn.store(
            snapshot_lsn.unwrap_or(NO_RETAINED_SNAPSHOT_LSN),
            Ordering::Release,
        );
    }

    pub(crate) fn retained_snapshot_lsn(&self) -> Option<u64> {
        match self.inner.retained_snapshot_lsn.load(Ordering::Acquire) {
            NO_RETAINED_SNAPSHOT_LSN => None,
            snapshot_lsn => Some(snapshot_lsn),
        }
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
                let base = if let Some(previous) = self.materialize_latest_visible_locked(
                    index,
                    pager,
                    page_id,
                    version_lsn.saturating_sub(1),
                )? {
                    previous
                } else {
                    pager.read_page(page_id)?
                };
                self.materialize_delta_page_with_scratch(base, &frame.payload)
            }
        }
    }

    fn materialize_delta_page_with_scratch(
        &self,
        base: Arc<[u8]>,
        delta_payload: &[u8],
    ) -> Result<Arc<[u8]>> {
        #[cfg(feature = "bench-internals")]
        WAL_DELTA_MATERIALIZE_CALLS.fetch_add(1, Ordering::Relaxed);

        let page_size = self.inner.page_size as usize;
        let mut scratch = self
            .inner
            .materialize_scratch
            .lock()
            .expect("wal materialization scratch lock should not be poisoned");
        let scratch_capacity = scratch.capacity();
        if scratch_capacity < page_size {
            #[cfg(feature = "bench-internals")]
            WAL_DELTA_SCRATCH_GROWS.fetch_add(1, Ordering::Relaxed);

            scratch.reserve(page_size - scratch_capacity);
        } else {
            #[cfg(feature = "bench-internals")]
            WAL_DELTA_SCRATCH_REUSES.fetch_add(1, Ordering::Relaxed);
        }

        scratch.clear();
        scratch.extend_from_slice(base.as_ref());
        if let Err(err) = apply_page_delta_in_place(&mut scratch, delta_payload) {
            scratch.clear();
            return Err(err);
        }

        let out = Arc::<[u8]>::from(scratch.as_slice());
        scratch.clear();
        Ok(out)
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
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");
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
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");
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
            (1, 1),
            "reader-visible delta bases stay resident until snapshots drain"
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
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");
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
    fn delta_materialization_reuses_scratch_capacity() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new(":memory:");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_resident_versions_per_page = 0;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");
        let page_id = page::CATALOG_ROOT_PAGE_ID + 1;

        let base = vec![0xA1; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(page_id, &base)
            .expect("seed base page");
        let mut updated = base.clone();
        updated[128..136].copy_from_slice(b"scratch!");
        let snapshot_lsn = wal
            .commit_pages(&pager, vec![(page_id, updated.clone())], page_id)
            .expect("commit delta page");

        let first = wal
            .read_page_at_snapshot(&pager, page_id, snapshot_lsn)
            .expect("read first materialized page")
            .expect("page should exist");
        let capacity_after_first = wal
            .inner
            .materialize_scratch
            .lock()
            .expect("materialize scratch lock")
            .capacity();

        let second = wal
            .read_page_at_snapshot(&pager, page_id, snapshot_lsn)
            .expect("read second materialized page")
            .expect("page should exist");
        let capacity_after_second = wal
            .inner
            .materialize_scratch
            .lock()
            .expect("materialize scratch lock")
            .capacity();

        assert_eq!(first.as_ref(), updated.as_slice());
        assert_eq!(second.as_ref(), updated.as_slice());
        assert!(capacity_after_first >= page::DEFAULT_PAGE_SIZE as usize);
        assert_eq!(capacity_after_second, capacity_after_first);
    }

    #[test]
    fn read_page_at_snapshot_promotes_spilled_full_page_version() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new("spill-promote.ddb");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_index_hot_set_pages = 1;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");

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
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");

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
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");

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
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");

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
