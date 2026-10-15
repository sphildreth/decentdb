//! Write-ahead log ownership, recovery, and checkpointing.

pub(crate) mod async_commit;
pub(crate) mod background;
pub(crate) mod checkpoint;
#[cfg(test)]
mod checkpoint_tests;
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
use crate::vfs::VfsHandle;
use crate::vfs::{read_exact_at, VfsFile};

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
use self::index::{WalIndex, WalVersion, WalVersionPayload};
use self::index_sidecar::WalIndexSidecar;
use self::reader_registry::{ReaderGuard, ReaderRegistry};

const NO_RETAINED_SNAPSHOT_LSN: u64 = u64::MAX;
const CHECKPOINT_WAL_READ_AHEAD_BYTES: usize = 256 * 1024;

/// Bounded, checkpoint-local WAL read-ahead window.
///
/// Checkpoint copyback orders output by page id. Bulk-load WAL offsets follow
/// that order closely, so one window commonly serves dozens of consecutive
/// full-page versions. Non-monotonic offsets remain correct: a miss simply
/// refills the window at the requested frame.
#[derive(Debug, Default)]
pub(crate) struct CheckpointWalReadAhead {
    start_offset: u64,
    bytes: Vec<u8>,
    last_request_end: Option<u64>,
}

impl CheckpointWalReadAhead {
    fn frame_bytes<'a>(
        &'a mut self,
        file: &dyn VfsFile,
        wal_offset: u64,
        frame_len: u32,
        logical_end: u64,
    ) -> Result<&'a [u8]> {
        let frame_len = usize::try_from(frame_len)
            .map_err(|_| DbError::corruption("WAL frame length does not fit this platform"))?;
        let frame_end = wal_offset
            .checked_add(frame_len as u64)
            .ok_or_else(|| DbError::corruption("WAL frame end offset overflows"))?;
        if frame_end > logical_end {
            return Err(DbError::corruption(format!(
                "WAL frame at offset {wal_offset} is truncated at logical end {logical_end}"
            )));
        }

        let cached_end = self.start_offset.saturating_add(self.bytes.len() as u64);
        let cached = wal_offset >= self.start_offset && frame_end <= cached_end;
        if !cached {
            let available = logical_end.saturating_sub(wal_offset);
            // A page-id ordered checkpoint can encounter WAL offsets in any
            // order. Prefetch only for the first request or a nearby forward
            // continuation; a backward/random miss reads just its frame so a
            // 256 KiB window is not repeatedly discarded unused.
            let nearby_forward = self.last_request_end.is_some_and(|previous_end| {
                wal_offset >= previous_end
                    && wal_offset.saturating_sub(previous_end)
                        <= (frame_len as u64).saturating_mul(4)
            });
            let desired = if self.last_request_end.is_none() || nearby_forward {
                CHECKPOINT_WAL_READ_AHEAD_BYTES as u64
            } else {
                frame_len as u64
            };
            let read_len_u64 = available.min(desired);
            let read_len = usize::try_from(read_len_u64).map_err(|_| {
                DbError::corruption("checkpoint WAL read-ahead length does not fit this platform")
            })?;
            if read_len < frame_len {
                return Err(DbError::corruption(format!(
                    "WAL frame at offset {wal_offset} is truncated: needs {frame_len} bytes, has {read_len}"
                )));
            }
            self.bytes.clear();
            if self.bytes.capacity() < read_len {
                self.bytes
                    .try_reserve_exact(read_len - self.bytes.capacity())
                    .map_err(|error| {
                        DbError::internal(format!(
                            "allocate checkpoint WAL read-ahead ({read_len} bytes): {error}"
                        ))
                    })?;
            }
            self.bytes.resize(read_len, 0);
            self.start_offset = wal_offset;
            read_exact_at(file, wal_offset, &mut self.bytes)?;
        }

        self.last_request_end = Some(frame_end);

        let relative =
            usize::try_from(wal_offset.saturating_sub(self.start_offset)).map_err(|_| {
                DbError::corruption("cached WAL frame offset does not fit this platform")
            })?;
        let relative_end = relative
            .checked_add(frame_len)
            .ok_or_else(|| DbError::corruption("cached WAL frame end overflows"))?;
        self.bytes.get(relative..relative_end).ok_or_else(|| {
            DbError::corruption(format!(
                "cached WAL frame at offset {wal_offset} is outside the read-ahead window"
            ))
        })
    }
}

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
    checkpoint_tail_sync_needed: AtomicBool,
    checkpoint_tail_locally_synced: AtomicBool,
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

/// Provenance for a materialized page used as a WAL delta base.
///
/// A main-database page is stable until checkpoint, and a resident WAL page
/// carries a complete materialized image. An on-disk WAL version may itself
/// be a delta whose earlier version is about to leave the index, so it must
/// not become the sole base for a newly published delta.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WalBaseSource {
    MainDatabase,
    ResidentWal,
    OnDiskWal,
}

pub(crate) type WalBasePage = Option<(Arc<[u8]>, WalBaseSource)>;

#[derive(Debug)]
pub(crate) enum PreparedWalPayload {
    /// The full page is self-contained in the just-written WAL frame. Its
    /// input vector was released after the bounded preparation batch.
    OnDiskFullPage,
    /// The materialized image must remain resident. Delta frames need it for
    /// direct reads and active-reader commits retain full images as well.
    Resident {
        data: Vec<u8>,
        encoding: format::FrameEncoding,
    },
}

#[derive(Debug)]
pub(crate) struct PreparedWalPage {
    pub(crate) page_id: PageId,
    pub(crate) encoded_len: usize,
    pub(crate) frame_offset: u64,
    pub(crate) payload: PreparedWalPayload,
}

#[derive(Debug)]
pub(crate) struct WalWriteState {
    pub(crate) page_batch: Vec<u8>,
    pub(crate) prepared_pages: Vec<PreparedWalPage>,
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

    /// Acquires a WAL only when this call can atomically create its sidecar.
    ///
    /// `Ok(None)` means either the WAL path or a same-process shared handle
    /// already exists. Fresh-database bootstrap uses that result to finish its
    /// main-file durability barrier before retrying the normal recovery-aware
    /// acquisition path.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(crate) fn acquire_fresh(
        vfs: &VfsHandle,
        db_path: &Path,
        config: &DbConfig,
        pager: &PagerHandle,
        process_coordinator: Option<ProcessCoordinator>,
    ) -> Result<Option<Self>> {
        shared::acquire_fresh(vfs, db_path, config, pager, process_coordinator)
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
            .map_err(|_| DbError::internal("wal index lock poisoned"))?;
        if index.latest_visible(page_id, snapshot_lsn).is_none() {
            self.promote_spilled_latest_locked(&mut index, page_id, snapshot_lsn)?;
        }
        if index.latest_visible(page_id, snapshot_lsn).is_some() {
            index.touch(page_id);
        }
        self.materialize_latest_visible_locked(&index, pager, page_id, snapshot_lsn)
    }

    pub(crate) fn materialize_checkpoint_version(
        &self,
        pager: &PagerHandle,
        page_id: PageId,
        version: &WalVersion,
    ) -> Result<Arc<[u8]>> {
        let index = self
            .inner
            .index
            .lock()
            .map_err(|_| DbError::internal("wal index lock poisoned"))?;
        self.materialize_version_locked(&index, pager, page_id, version)
    }

    /// Append one checkpoint page image directly to the copyback buffer.
    ///
    /// Self-contained on-disk Page frames use the bounded read-ahead window
    /// and are validated in place. Resident images avoid an unnecessary Arc
    /// clone. On-disk deltas retain the existing recursive materialization
    /// path because they may require an indexed predecessor or main-file base.
    pub(crate) fn append_checkpoint_version_to(
        &self,
        pager: &PagerHandle,
        page_id: PageId,
        version: &WalVersion,
        logical_end: u64,
        read_ahead: &mut CheckpointWalReadAhead,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        let page_size = self.inner.page_size as usize;
        match &version.payload {
            WalVersionPayload::Resident { data, .. } => {
                if data.len() != page_size {
                    return Err(DbError::corruption(format!(
                        "checkpoint page {page_id} has {} bytes; expected {page_size}",
                        data.len()
                    )));
                }
                output.extend_from_slice(data);
            }
            WalVersionPayload::OnDisk {
                wal_offset,
                frame_len,
                encoding: FrameEncoding::Page,
            } => {
                let frame = read_ahead.frame_bytes(
                    self.inner.file.as_ref(),
                    *wal_offset,
                    *frame_len,
                    logical_end,
                )?;
                let payload = WalFrame::page_payload_from_encoded_with_len(
                    frame,
                    page_id,
                    *frame_len,
                    self.inner.page_size,
                )?;
                output.extend_from_slice(payload);
            }
            WalVersionPayload::OnDisk { .. } => {
                let payload = self.materialize_checkpoint_version(pager, page_id, version)?;
                if payload.len() != page_size {
                    return Err(DbError::corruption(format!(
                        "checkpoint page {page_id} has {} bytes; expected {page_size}",
                        payload.len()
                    )));
                }
                output.extend_from_slice(&payload);
            }
        }
        Ok(())
    }

    pub(crate) fn latest_snapshot(&self) -> u64 {
        self.inner.wal_end_lsn.load(Ordering::Acquire)
    }

    pub(crate) fn checkpoint_epoch(&self) -> u64 {
        self.inner.checkpoint_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn observed_current_snapshot_lsn(&self) -> Result<Option<u64>> {
        let snapshot_lsn = self.latest_snapshot();
        let Some(coordinator) = &self.inner.process_coordinator else {
            return Ok(Some(snapshot_lsn));
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
            && snapshot.wal_end_lsn == snapshot_lsn
        {
            Ok(Some(snapshot_lsn))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn begin_reader(&self) -> Result<ReaderGuard> {
        self.begin_reader_with_process_guard(None)
    }

    pub(crate) fn begin_reader_with_pager(&self, pager: &PagerHandle) -> Result<ReaderGuard> {
        let Some(coordinator) = self.inner.process_coordinator.as_ref() else {
            return self.begin_reader();
        };
        let started = std::time::Instant::now();
        let mut delay = std::time::Duration::from_micros(100);
        let mut attempts = 0_u8;
        loop {
            // The shared process gate closes the scan-to-publication hole:
            // a checkpoint cannot begin its retention scan until this reader
            // has either published a slot or abandoned this attempt. It stays
            // held across refresh, snapshot capture, slot registration, and
            // the generation validation below.
            let _admission = coordinator.lock_reader_admission()?;
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
            drop(_admission);
            attempts = attempts.saturating_add(1);
            if coordinator.reader_registration_timed_out(started.elapsed()) {
                if coordinator.reader_registration_timeout_is_zero() {
                    return Err(DbError::busy(
                        "process reader registration observed continuous WAL publication",
                    ));
                }
                return Err(DbError::timeout(
                    "timed out waiting for a stable process reader snapshot",
                ));
            }
            // Preserve the bounded fast retry phase from ADR 0178, then keep
            // polling at the capped delay until the configured busy timeout.
            if attempts < 8 {
                std::thread::sleep(delay);
                delay = (delay * 2).min(std::time::Duration::from_millis(5));
            } else {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
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
                .map_err(|_| DbError::internal("wal index lock poisoned"))?;
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
    ) -> Result<Option<coordination::ProcessCheckpointGuard>> {
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
        callback: Option<std::sync::Arc<dyn Fn(bool, std::time::Duration, &str) + Send + Sync>>,
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
                .map_err(|_| DbError::internal("wal write lock poisoned"))?;
            if snapshot.checkpoint_generation != observed_checkpoint {
                let header = pager.header_from_disk()?;
                pager.refresh_from_disk(header)?;
            }
            let mut sidecar = self
                .inner
                .index_sidecar
                .as_ref()
                .map(|sidecar| {
                    sidecar
                        .lock()
                        .map_err(|_| DbError::internal("wal index sidecar lock poisoned"))
                })
                .transpose()?;
            if let Some(sidecar) = sidecar.as_mut() {
                sidecar.clear()?;
            }
            let previous_end_lsn = self.latest_snapshot();
            let (index, end_lsn, recovered_max_page_id) =
                crate::wal::recovery::initialize_or_recover(
                    &self.inner.file,
                    pager,
                    self.inner.page_size,
                    self.inner.wal_index_hot_set_pages,
                    sidecar.as_deref_mut(),
                )?;
            // Normal reads/writes acquire index then sidecar. Release the
            // refresh-side sidecar guard before replacing the index to keep
            // that global lock order and avoid a cross-handle deadlock.
            drop(sidecar);
            let allocated_len = self.inner.file.file_size()?;
            {
                let mut current = self
                    .inner
                    .index
                    .lock()
                    .map_err(|_| DbError::internal("wal index lock poisoned"))?;
                *current = index;
            }
            self.inner.wal_end_lsn.store(end_lsn, Ordering::Release);
            self.inner
                .allocated_len
                .store(allocated_len, Ordering::Release);
            self.inner
                .max_page_count
                .fetch_max(recovered_max_page_id, Ordering::AcqRel);
            if end_lsn < previous_end_lsn {
                if let Some(async_commit) = self.inner.async_commit.as_ref() {
                    async_commit.rebase_clean_lsn(end_lsn)?;
                }
            }
            let nonempty_tail = end_lsn > 0;
            self.inner
                .checkpoint_tail_sync_needed
                .store(nonempty_tail, Ordering::Release);
            self.inner
                .checkpoint_tail_locally_synced
                .store(!nonempty_tail, Ordering::Release);
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
            .map_err(|_| DbError::internal("wal index lock poisoned"))?;
        let sidecar_count = match self.inner.index_sidecar.as_ref() {
            Some(sidecar) => sidecar
                .lock()
                .map_err(|_| DbError::internal("wal index sidecar lock poisoned"))?
                .version_count(),
            None => 0,
        };
        Ok(index.version_count() + sidecar_count)
    }

    pub(crate) fn version_counts_by_payload(&self) -> Result<(usize, usize)> {
        let index = self
            .inner
            .index
            .lock()
            .map_err(|_| DbError::internal("wal index lock poisoned"))?;
        let (resident, on_disk) = index.version_counts_by_payload();
        let (sidecar_resident, sidecar_on_disk) = match self.inner.index_sidecar.as_ref() {
            Some(sidecar) => sidecar
                .lock()
                .map_err(|_| DbError::internal("wal index sidecar lock poisoned"))?
                .version_counts_by_payload(),
            None => (0, 0),
        };
        Ok((resident + sidecar_resident, on_disk + sidecar_on_disk))
    }

    pub(crate) fn demote_resident_versions_if_reader_free(
        &self,
        target_bytes: usize,
    ) -> Result<usize> {
        if self.inner.reader_registry.active_reader_count()? > 0
            || self.retained_snapshot_lsn().is_some()
        {
            return Ok(0);
        }
        let mut index = self
            .inner
            .index
            .lock()
            .map_err(|_| DbError::internal("wal index lock poisoned"))?;
        Ok(index.demote_high_page_ids_resident_bytes(target_bytes))
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
            .map_err(|_| DbError::internal("wal index sidecar lock poisoned"))?;
        let Some(version) = sidecar.read_latest(page_id)? else {
            return Ok(());
        };
        if version.lsn > snapshot_lsn {
            return Ok(());
        }
        if let Err(error) = sidecar.clear_latest(page_id) {
            // The sidecar clear is only cache maintenance. Preserve the
            // authoritative version in the hot index when its publication
            // byte could not be cleared so a later lookup never falls back to
            // a stale main-database image.
            index.seed_latest(page_id, version);
            return Err(error);
        }
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
            if let Err(error) = sidecar.write_latest(page_id, &version) {
                // `spill_one_cold_latest` transfers the only current version
                // out of the hot index. Restore it if cache publication (or a
                // required post-reset clear) fails; the committed WAL remains
                // authoritative and must stay reachable on this handle.
                index.seed_latest(page_id, version);
                return Err(error);
            }
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
        let frame = WalFrame::decode_from_file_with_len(
            self.inner.file.as_ref(),
            wal_offset,
            frame_len,
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
                    pager.read_page_from_disk(page_id)?
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
            .map_err(|_| DbError::internal("wal materialization scratch lock poisoned"))?;
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

    #[cfg(test)]
    pub(crate) fn checkpoint_tail_state_for_tests(&self) -> (u64, bool, bool, bool) {
        (
            self.latest_snapshot(),
            self.inner.process_coordinator.is_some(),
            self.inner
                .checkpoint_tail_sync_needed
                .load(Ordering::Acquire),
            self.inner
                .checkpoint_tail_locally_synced
                .load(Ordering::Acquire),
        )
    }

    /// Blocks until every commit acknowledged before this call is durable on
    /// disk. For sync modes other than `AsyncCommit` this is a no-op because
    /// commits are already synchronously durable.
    pub(crate) fn flush_to_durable(&self) -> Result<()> {
        match self.inner.async_commit.as_ref() {
            Some(state) => {
                let flushed = state.flush_to_durable()?;
                if flushed && state.durable_lsn() >= self.latest_snapshot() {
                    self.inner
                        .checkpoint_tail_sync_needed
                        .store(false, Ordering::Release);
                    self.inner
                        .checkpoint_tail_locally_synced
                        .store(true, Ordering::Release);
                }
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    use crate::config::{DbConfig, WalSyncMode};
    use crate::error::DbError;
    use crate::storage::page;
    use crate::storage::{write_database_bootstrap_vfs, DatabaseHeader, PagerHandle};
    use crate::vfs::mem::MemVfs;
    use crate::vfs::{FileKind, OpenMode, Vfs, VfsFile, VfsHandle};

    use super::format::FrameEncoding;
    use super::index::WalVersionPayload;
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

    #[derive(Debug)]
    struct SidecarClearFailureVfs {
        inner: MemVfs,
        fail_sidecar_clear: Arc<AtomicUsize>,
        fail_sidecar_header_write: Arc<AtomicUsize>,
        fail_sidecar_record_clear: Arc<AtomicUsize>,
    }

    impl Vfs for SidecarClearFailureVfs {
        fn open(
            &self,
            path: &Path,
            mode: OpenMode,
            kind: FileKind,
        ) -> crate::Result<Arc<dyn VfsFile>> {
            let inner = self.inner.open(path, mode, kind)?;
            let is_sidecar = path
                .extension()
                .is_some_and(|extension| extension == "wal-idx");
            Ok(Arc::new(SidecarClearFailureFile {
                inner,
                fail_sidecar_clear: Arc::clone(&self.fail_sidecar_clear),
                fail_sidecar_header_write: Arc::clone(&self.fail_sidecar_header_write),
                fail_sidecar_record_clear: Arc::clone(&self.fail_sidecar_record_clear),
                is_sidecar,
            }))
        }

        fn file_exists(&self, path: &Path) -> crate::Result<bool> {
            self.inner.file_exists(path)
        }

        fn remove_file(&self, path: &Path) -> crate::Result<()> {
            self.inner.remove_file(path)
        }

        fn canonicalize_path(&self, path: &Path) -> crate::Result<std::path::PathBuf> {
            self.inner.canonicalize_path(path)
        }

        fn is_memory(&self) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct SidecarClearFailureFile {
        inner: Arc<dyn VfsFile>,
        fail_sidecar_clear: Arc<AtomicUsize>,
        fail_sidecar_header_write: Arc<AtomicUsize>,
        fail_sidecar_record_clear: Arc<AtomicUsize>,
        is_sidecar: bool,
    }

    impl VfsFile for SidecarClearFailureFile {
        fn kind(&self) -> FileKind {
            self.inner.kind()
        }

        fn path(&self) -> &Path {
            self.inner.path()
        }

        fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
            self.inner.read_at(offset, buf)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> crate::Result<usize> {
            if self.is_sidecar
                && offset >= super::index_sidecar::WAL_INDEX_SIDECAR_HEADER_LEN
                && buf == [0]
                && consume_atomic_failure(&self.fail_sidecar_record_clear)
            {
                let written = self.inner.write_at(offset, buf)?;
                if written != buf.len() {
                    return Err(DbError::internal(
                        "sidecar record-clear fault setup produced a short write",
                    ));
                }
                return Err(DbError::io(
                    "fault injected after wal-index-sidecar record clear",
                    std::io::Error::other("fault injected sidecar record-clear error"),
                ));
            }
            if self.is_sidecar
                && offset == 0
                && buf.len() == super::index_sidecar::WAL_INDEX_SIDECAR_HEADER_LEN as usize
                && consume_atomic_failure(&self.fail_sidecar_header_write)
            {
                let partial_len = buf.len() / 2;
                let written = self.inner.write_at(offset, &buf[..partial_len])?;
                if written != partial_len {
                    return Err(DbError::internal(
                        "sidecar header fault setup produced a short partial write",
                    ));
                }
                return Err(DbError::io(
                    "fault injected during wal-index-sidecar header rewrite",
                    std::io::Error::other("fault injected sidecar header write error"),
                ));
            }
            self.inner.write_at(offset, buf)
        }

        fn advise_sequential(&self) -> crate::Result<()> {
            self.inner.advise_sequential()
        }

        fn sync_data(&self) -> crate::Result<()> {
            self.inner.sync_data()
        }

        fn sync_metadata(&self) -> crate::Result<()> {
            self.inner.sync_metadata()
        }

        fn file_size(&self) -> crate::Result<u64> {
            self.inner.file_size()
        }

        fn set_len(&self, len: u64) -> crate::Result<()> {
            if self.is_sidecar
                && len == super::index_sidecar::WAL_INDEX_SIDECAR_HEADER_LEN
                && consume_atomic_failure(&self.fail_sidecar_clear)
            {
                // The replacement header was already written. Failing the
                // shrink now leaves a deterministic partial clear: valid old
                // record bytes remain physically beyond the new header.
                return Err(DbError::io(
                    "fault injected after wal-index-sidecar header rewrite",
                    std::io::Error::other("fault injected sidecar truncate error"),
                ));
            }
            self.inner.set_len(len)
        }
    }

    fn consume_atomic_failure(counter: &AtomicUsize) -> bool {
        // Use compare_exchange_weak to support the Rust 1.88 MSRV without the
        // fetch_update API deprecated by newer toolchains.
        let mut remaining = counter.load(AtomicOrdering::Acquire);
        while let Some(next) = remaining.checked_sub(1) {
            match counter.compare_exchange_weak(
                remaining,
                next,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => remaining = actual,
            }
        }
        false
    }

    #[test]
    fn atomic_failure_counter_is_consumed_exactly_once_per_failure() {
        let counter = AtomicUsize::new(1024);
        let barrier = std::sync::Barrier::new(8);
        let consumed = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        let mut consumed = 0;
                        while consume_atomic_failure(&counter) {
                            consumed += 1;
                        }
                        consumed
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().expect("failure counter thread"))
                .sum::<usize>()
        });

        assert_eq!(consumed, 1024);
        assert_eq!(counter.load(AtomicOrdering::Acquire), 0);
        assert!(!consume_atomic_failure(&counter));
    }

    #[test]
    fn resident_delta_reads_share_backing_allocation() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new(":memory:");
        let pager = test_pager(&vfs, db_path);
        let cfg = test_config();
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");
        let page_id = page::CATALOG_ROOT_PAGE_ID + 1;
        let base = vec![0x5A; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(page_id, &base)
            .expect("seed delta base");
        let mut payload = base;
        payload[64..68].copy_from_slice(b"arc!");
        let snapshot_lsn = wal
            .commit_pages(&pager, vec![(page_id, payload)], page_id)
            .expect("commit page");
        assert_eq!(
            wal.version_counts_by_payload().expect("payload counts"),
            (1, 0),
            "delta materializations stay resident"
        );

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
    fn active_old_reader_keeps_history_while_new_full_page_stays_resident() {
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
            "old on-disk history and the active-reader full image must coexist"
        );
        let old_during_reader = wal
            .read_page_at_snapshot(&pager, page_id, reader.snapshot_lsn())
            .expect("read active old snapshot")
            .expect("old page visible to reader");
        let new_during_reader = wal
            .read_page_at_snapshot(&pager, page_id, second_snapshot)
            .expect("read latest during old reader")
            .expect("new page visible at latest snapshot");
        assert_eq!(old_during_reader.as_ref(), first_payload.as_slice());
        assert_eq!(new_during_reader.as_ref(), second_payload.as_slice());
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
            (0, 2)
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
            (0, 2)
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
    fn checkpoint_partial_sidecar_clear_failure_invalidates_stale_generation() {
        let fail_sidecar_clear = Arc::new(AtomicUsize::new(0));
        let fail_sidecar_header_write = Arc::new(AtomicUsize::new(0));
        let fail_sidecar_record_clear = Arc::new(AtomicUsize::new(0));
        let vfs = VfsHandle::from_vfs(Arc::new(SidecarClearFailureVfs {
            inner: MemVfs::default(),
            fail_sidecar_clear: Arc::clone(&fail_sidecar_clear),
            fail_sidecar_header_write: Arc::clone(&fail_sidecar_header_write),
            fail_sidecar_record_clear: Arc::clone(&fail_sidecar_record_clear),
        }));
        let db_path = Path::new("spill-checkpoint-clear-failure.ddb");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_index_hot_set_pages = 1;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");

        let page_one = page::CATALOG_ROOT_PAGE_ID + 1;
        let page_two = page_one + 1;
        let payload_one = vec![0x57; page::DEFAULT_PAGE_SIZE as usize];
        let payload_two = vec![0x68; page::DEFAULT_PAGE_SIZE as usize];
        wal.commit_pages(&pager, vec![(page_one, payload_one.clone())], page_two)
            .expect("commit page one");
        wal.commit_pages(&pager, vec![(page_two, payload_two.clone())], page_two)
            .expect("commit page two");

        assert_eq!(wal.version_count().expect("version count"), 2);
        fail_sidecar_clear.store(1, AtomicOrdering::Release);
        let error = wal
            .checkpoint(&pager, 0)
            .expect_err("sidecar clear failure should be reported after local cleanup");
        assert!(matches!(error, DbError::Io { .. }));
        assert_eq!(
            wal.latest_snapshot(),
            0,
            "logical WAL reset should be visible after truncate header write"
        );
        let local_index_versions = wal.inner.index.lock().expect("wal index").version_count();
        assert_eq!(
            local_index_versions, 0,
            "local in-memory index must be cleared even when sidecar cleanup fails"
        );
        assert_eq!(
            wal.version_count().expect("combined version count"),
            0,
            "partially cleared sidecar records must be invalid immediately"
        );
        assert_eq!(
            pager.read_page(page_one).expect("read page one").as_ref(),
            payload_one.as_slice()
        );
        assert_eq!(
            pager.read_page(page_two).expect("read page two").as_ref(),
            payload_two.as_slice()
        );

        // The failed clear was one-shot. Reuse the low WAL offsets with a
        // different page first: the stale page-one sidecar record previously
        // pointed into this new frame and could be promoted as an unrelated
        // OnDisk version.
        let new_payload_two = vec![0x79; page::DEFAULT_PAGE_SIZE as usize];
        let low_offset_snapshot = wal
            .commit_pages(&pager, vec![(page_two, new_payload_two.clone())], page_two)
            .expect("commit at reused low WAL offset");
        assert!(
            wal.read_page_at_snapshot(&pager, page_one, low_offset_snapshot)
                .expect("stale sidecar lookup must be ignored")
                .is_none(),
            "page one should fall back to its checkpointed database image"
        );
        assert_eq!(
            pager
                .read_page(page_one)
                .expect("checkpointed page one")
                .as_ref(),
            payload_one.as_slice()
        );
        assert_eq!(
            wal.read_page_at_snapshot(&pager, page_two, low_offset_snapshot)
                .expect("read new page two")
                .expect("page two in WAL")
                .as_ref(),
            new_payload_two.as_slice()
        );

        // Repeatedly fail the first post-reset spill's required clear. Each WAL
        // commit is already logically published when cache spill runs, so the
        // popped version must be restored to the hot index and the committed
        // API result must remain successful despite the cache-maintenance
        // error.
        fail_sidecar_clear.store(2, AtomicOrdering::Release);
        let page_three = page_two + 1;
        let payload_three = vec![0x8A; page::DEFAULT_PAGE_SIZE as usize];
        let after_first_spill_failure = wal
            .commit_pages(
                &pager,
                vec![(page_three, payload_three.clone())],
                page_three,
            )
            .expect("committed page three despite first sidecar rebuild failure");
        assert_eq!(after_first_spill_failure, wal.latest_snapshot());
        assert_eq!(wal.version_count().expect("restored versions"), 2);
        for (page_id, expected) in [(page_two, &new_payload_two), (page_three, &payload_three)] {
            assert_eq!(
                wal.read_page_at_snapshot(&pager, page_id, after_first_spill_failure)
                    .expect("read after first spill failure")
                    .expect("published version restored to index")
                    .as_ref(),
                expected.as_slice()
            );
        }

        let new_payload_one = vec![0x9B; page::DEFAULT_PAGE_SIZE as usize];
        let after_second_spill_failure = wal
            .commit_pages(
                &pager,
                vec![(page_one, new_payload_one.clone())],
                page_three,
            )
            .expect("committed page one despite second sidecar rebuild failure");
        assert_eq!(after_second_spill_failure, wal.latest_snapshot());
        assert_eq!(wal.version_count().expect("restored versions"), 3);
        assert_eq!(
            wal.read_page_at_snapshot(&pager, page_one, after_second_spill_failure)
                .expect("read new page one")
                .expect("new page one in WAL")
                .as_ref(),
            new_payload_one.as_slice()
        );

        // Once physical cleanup succeeds, exceeding hotset=1 rebuilds a fresh
        // sidecar and publishes only current-generation records.
        let page_four = page_three + 1;
        let payload_four = vec![0xAC; page::DEFAULT_PAGE_SIZE as usize];
        let final_snapshot = wal
            .commit_pages(&pager, vec![(page_four, payload_four.clone())], page_four)
            .expect("commit page four and rebuild fresh sidecar generation");
        assert!(wal.version_count().expect("current versions") >= 4);
        assert!(final_snapshot > after_second_spill_failure);

        // Reopen with a non-empty post-reset WAL. Open clears the cache before
        // recovery. A partial header-write failure must fail closed; retrying
        // the open clears successfully and rebuilds only current-generation
        // records.
        drop(wal);
        fail_sidecar_header_write.store(1, AtomicOrdering::Release);
        let reopen_error = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None)
            .expect_err("partial sidecar header clear must fail open closed");
        assert!(matches!(reopen_error, DbError::Io { .. }));
        let reopened = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None)
            .expect("reopen and recover post-reset WAL");
        let reopened_snapshot = reopened.latest_snapshot();
        for (page_id, expected) in [
            (page_one, &new_payload_one),
            (page_two, &new_payload_two),
            (page_three, &payload_three),
            (page_four, &payload_four),
        ] {
            assert_eq!(
                reopened
                    .read_page_at_snapshot(&pager, page_id, reopened_snapshot)
                    .expect("read recovered page")
                    .expect("recovered page in WAL")
                    .as_ref(),
                expected.as_slice()
            );
        }
    }

    #[test]
    fn sidecar_promotion_clear_failure_precedes_wal_publication() {
        let fail_sidecar_clear = Arc::new(AtomicUsize::new(0));
        let fail_sidecar_header_write = Arc::new(AtomicUsize::new(0));
        let fail_sidecar_record_clear = Arc::new(AtomicUsize::new(0));
        let vfs = VfsHandle::from_vfs(Arc::new(SidecarClearFailureVfs {
            inner: MemVfs::default(),
            fail_sidecar_clear,
            fail_sidecar_header_write,
            fail_sidecar_record_clear: Arc::clone(&fail_sidecar_record_clear),
        }));
        let db_path = Path::new("spill-promotion-clear-failure.ddb");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_index_hot_set_pages = 1;
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");

        let page_one = page::CATALOG_ROOT_PAGE_ID + 1;
        let page_two = page_one + 1;
        let old_payload = vec![0x31; page::DEFAULT_PAGE_SIZE as usize];
        let other_payload = vec![0x42; page::DEFAULT_PAGE_SIZE as usize];
        wal.commit_pages(&pager, vec![(page_one, old_payload.clone())], page_two)
            .expect("commit page one");
        let old_snapshot = wal
            .commit_pages(&pager, vec![(page_two, other_payload)], page_two)
            .expect("commit page two and spill page one");
        assert!(
            !wal.inner
                .index
                .lock()
                .expect("wal index")
                .contains_page(page_one),
            "page one must be sidecar-resident before promotion fault"
        );

        // Promotion reads the old record and then clears its publication byte.
        // Even when that one-byte write takes effect and reports an error, the
        // old version is restored in memory and the final WAL group/header has
        // not yet been written.
        fail_sidecar_record_clear.store(1, AtomicOrdering::Release);
        let new_payload = vec![0x53; page::DEFAULT_PAGE_SIZE as usize];
        let error = wal
            .commit_pages(&pager, vec![(page_one, new_payload.clone())], page_two)
            .expect_err("promotion clear failure must reject before WAL publication");
        assert!(matches!(error, DbError::Io { .. }));
        assert_eq!(wal.latest_snapshot(), old_snapshot);
        assert_eq!(
            wal.read_page_at_snapshot(&pager, page_one, old_snapshot)
                .expect("read old page after failed promotion")
                .expect("old page remains indexed")
                .as_ref(),
            old_payload.as_slice()
        );

        let retry_snapshot = wal
            .commit_pages(&pager, vec![(page_one, new_payload.clone())], page_two)
            .expect("retry after sidecar record-clear failure");
        assert!(retry_snapshot > old_snapshot);
        assert_eq!(
            wal.read_page_at_snapshot(&pager, page_one, retry_snapshot)
                .expect("read retried page")
                .expect("retried page is visible")
                .as_ref(),
            new_payload.as_slice()
        );
    }

    #[test]
    fn async_rebase_failure_precedes_logical_wal_reset() {
        struct ResetRebaseFailure;
        impl Drop for ResetRebaseFailure {
            fn drop(&mut self) {
                super::async_commit::force_rebase_error_for_current_thread(false);
            }
        }

        let vfs = VfsHandle::from_vfs(Arc::new(MemVfs::default()));
        let db_path = Path::new("async-rebase-before-logical-reset.ddb");
        let pager = test_pager(&vfs, db_path);
        let mut cfg = test_config();
        cfg.wal_sync_mode = WalSyncMode::AsyncCommit {
            interval_ms: 60_000,
        };
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire WAL");
        let page_id = page::CATALOG_ROOT_PAGE_ID + 1;
        let payload = vec![0xD4; page::DEFAULT_PAGE_SIZE as usize];
        let old_end = wal
            .commit_pages(&pager, vec![(page_id, payload.clone())], page_id)
            .expect("async commit");

        super::async_commit::force_rebase_error_for_current_thread(true);
        let _reset = ResetRebaseFailure;
        let error = wal
            .checkpoint(&pager, 0)
            .expect_err("injected async rebase failure must abort checkpoint");
        assert!(matches!(error, DbError::Internal { .. }));
        assert_eq!(
            wal.latest_snapshot(),
            old_end,
            "fallible async rebase must occur before logical zero publication"
        );
        assert_eq!(
            wal.read_page_at_snapshot(&pager, page_id, old_end)
                .expect("read retained WAL version")
                .expect("page remains indexed")
                .as_ref(),
            payload.as_slice()
        );

        super::async_commit::force_rebase_error_for_current_thread(false);
        wal.checkpoint(&pager, 0)
            .expect("checkpoint retry after rebase failure");
        assert_eq!(wal.latest_snapshot(), 0);
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

    #[test]
    fn explicit_demotion_keeps_chained_delta_resident_through_checkpoint_and_reopen() {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let db_path = Path::new("explicit-demote-chained-delta.ddb");
        let pager = test_pager(&vfs, db_path);
        let cfg = test_config();
        let wal = WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("acquire wal");
        let delta_page_id = page::CATALOG_ROOT_PAGE_ID + 300;
        let resident_full_page_id = delta_page_id - 1;
        let base = vec![0x21; page::DEFAULT_PAGE_SIZE as usize];
        pager
            .write_page_direct(delta_page_id, &base)
            .expect("seed main-database delta base");

        let mut first = base;
        first[17] = 0x31;
        wal.commit_pages(&pager, vec![(delta_page_id, first.clone())], delta_page_id)
            .expect("commit first delta");
        let mut latest = first;
        latest[29] = 0x42;
        let delta_lsn = wal
            .commit_pages(&pager, vec![(delta_page_id, latest.clone())], delta_page_id)
            .expect("commit delta against resident WAL base");
        {
            let index = wal.inner.index.lock().expect("WAL index");
            let version = index
                .latest_visible(delta_page_id, delta_lsn)
                .expect("latest chained delta");
            assert!(matches!(
                version.payload,
                WalVersionPayload::Resident {
                    encoding: FrameEncoding::PageDelta,
                    ..
                }
            ));
        }

        // Force a second resident payload below the delta page ID so the
        // explicit descending demotion scan crosses the protected delta and
        // still satisfies its byte target using a self-contained full page.
        let reader = wal.begin_reader().expect("retain full-page commit");
        let resident_full = vec![0x53; page::DEFAULT_PAGE_SIZE as usize];
        let latest_lsn = wal
            .commit_pages(
                &pager,
                vec![(resident_full_page_id, resident_full.clone())],
                delta_page_id,
            )
            .expect("commit active-reader full page");
        drop(reader);
        assert_eq!(
            wal.demote_resident_versions_if_reader_free(1)
                .expect("explicit demotion"),
            1,
            "the full page should satisfy the target while the delta stays resident"
        );
        assert_eq!(
            wal.version_counts_by_payload().expect("payload counts"),
            (1, 1)
        );
        assert_eq!(
            wal.read_page_at_snapshot(&pager, delta_page_id, latest_lsn)
                .expect("read chained delta after demotion")
                .expect("delta page remains visible")
                .as_ref(),
            latest.as_slice()
        );

        wal.checkpoint(&pager, 0)
            .expect("checkpoint protected chained delta");
        assert_eq!(
            pager
                .read_page(delta_page_id)
                .expect("read checkpointed delta")
                .as_ref(),
            latest.as_slice()
        );
        drop(wal);

        let reopened =
            WalHandle::acquire(&vfs, db_path, &cfg, &pager, None).expect("reopen checkpointed WAL");
        assert_eq!(reopened.version_count().expect("reopened versions"), 0);
        assert_eq!(
            pager
                .read_page(delta_page_id)
                .expect("read reopened checkpointed delta")
                .as_ref(),
            latest.as_slice()
        );
    }
}
