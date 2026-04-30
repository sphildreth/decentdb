//! WAL append and durability logic.
//!
//! Implements:
//! - design/adr/0003-snapshot-lsn-atomicity.md

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::config::WalSyncMode;
use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::storage::PagerHandle;
use crate::vfs::write_all_at;

use super::delta::encode_page_delta_into;
use super::format::{
    FrameEncoding, FrameType, WalFrame, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE, WAL_HEADER_SIZE,
};
use super::index::WalVersion;
use super::recovery;
use super::WalHandle;
use super::WalWriteState;

const WAL_PREALLOC_CHUNK_BYTES: u64 = 64 << 20;
const COMMIT_FRAME_BYTES: [u8; FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE] =
    [FrameType::Commit as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

#[derive(Clone)]
struct DeltaBasePage {
    data: Arc<[u8]>,
    from_wal: bool,
}

pub(crate) fn commit_pages(
    wal: &WalHandle,
    pager: &PagerHandle,
    pages: Vec<(PageId, Vec<u8>)>,
    max_page_count: u32,
) -> Result<u64> {
    let mut writer_state = wal
        .inner
        .write_lock
        .lock()
        .expect("wal write lock should not be poisoned");

    let mut offset = wal.latest_snapshot();
    if offset == 0 {
        offset = WAL_HEADER_SIZE;
    }

    let page_frame_len = FRAME_HEADER_SIZE + wal.inner.page_size as usize + FRAME_TRAILER_SIZE;
    let WalWriteState {
        page_batch,
        prepared_pages,
        delta_scratch,
    } = &mut *writer_state;
    page_batch.clear();
    page_batch.reserve(page_frame_len * pages.len() + COMMIT_FRAME_BYTES.len());
    let latest_snapshot = wal.latest_snapshot();

    // Look up all base pages under a single index lock for delta encoding.
    let (base_pages, retain_history_hint) =
        lookup_base_pages_batch(wal, pager, &pages, latest_snapshot)?;

    prepared_pages.clear();
    prepared_pages.reserve(pages.len());
    for (i, (page_id, payload)) in pages.into_iter().enumerate() {
        let base = base_pages
            .get(i)
            .and_then(|b| b.as_ref())
            .map(|base| (&base.data[..], base.from_wal));
        let frame_offset = offset + page_batch.len() as u64;
        let (encoded_len, encoding) = append_best_page_frame_with_base(
            page_batch,
            delta_scratch,
            wal,
            page_id,
            &payload,
            base,
            retain_history_hint,
        )?;
        prepared_pages.push((page_id, payload, encoded_len, encoding, frame_offset));
    }
    let commit_start_lsn = offset;
    page_batch.extend_from_slice(&COMMIT_FRAME_BYTES);
    let new_offset = offset + page_batch.len() as u64;
    let metadata_changed = ensure_capacity(wal, new_offset)?;
    write_all_at(wal.inner.file.as_ref(), offset, page_batch.as_slice())?;
    // Publish the logical end only after the frame bytes are in place so a
    // concurrent opener never trusts a WAL tail that has not been written yet.
    recovery::persist_header(&wal.inner.file, wal.inner.page_size, new_offset)?;
    offset = new_offset;
    sync_for_mode(wal, metadata_changed, new_offset)?;

    {
        let mut index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        // Check inside the index lock so begin_reader() cannot register
        // between the count check and the version clear (TOCTOU fix).
        let retain_history =
            retain_history_hint || wal.inner.reader_registry.active_reader_count()? > 0;
        let mut sidecar = wal.inner.index_sidecar.as_ref().map(|sidecar| {
            sidecar
                .lock()
                .expect("wal index sidecar lock should not be poisoned")
        });
        let mut version_lsn = commit_start_lsn;
        let prepared_count = prepared_pages.len();
        for (page_id, payload, encoded_len, encoding, frame_offset) in prepared_pages.drain(..) {
            version_lsn += encoded_len as u64;
            if let Some(sidecar) = sidecar.as_mut() {
                if retain_history && !index.contains_page(page_id) {
                    if let Some(previous) = sidecar.read_latest(page_id)? {
                        index.seed_latest(page_id, previous);
                    }
                }
                sidecar.clear_latest(page_id)?;
            }
            index.add_version(
                page_id,
                WalVersion::resident(
                    version_lsn,
                    frame_offset,
                    encoded_len as u32,
                    encoding,
                    Arc::from(payload),
                ),
                retain_history,
            );
        }
        // Update wal_end_lsn inside the index lock so that begin_reader()
        // (which also holds the index lock) always sees a wal_end_lsn
        // consistent with the index contents.
        wal.inner
            .max_page_count
            .fetch_max(max_page_count, Ordering::AcqRel);
        wal.inner.wal_end_lsn.store(offset, Ordering::Release);
        // Track work since the last checkpoint for the size-based trigger
        // (ADR 0137). Saturating add: a u32 is enough headroom for a single
        // checkpoint window even on the largest practical workloads, and
        // saturation is safe because the trigger compares >= threshold.
        let prepared_count_u32 = u32::try_from(prepared_count).unwrap_or(u32::MAX);
        wal.inner
            .pages_since_checkpoint
            .fetch_add(prepared_count_u32, Ordering::AcqRel);
    }
    drop(writer_state);
    demote_cold_versions(wal)?;
    maybe_auto_checkpoint(wal, pager)?;
    Ok(offset)
}

pub(crate) fn commit_pages_if_latest(
    wal: &WalHandle,
    pager: &PagerHandle,
    pages: Vec<(PageId, Vec<u8>)>,
    max_page_count: u32,
    expected_latest_lsn: u64,
    expected_checkpoint_epoch: u64,
) -> Result<u64> {
    let mut writer_state = wal
        .inner
        .write_lock
        .lock()
        .expect("wal write lock should not be poisoned");

    let latest = wal.latest_snapshot();
    if latest != expected_latest_lsn {
        // Distinguish a benign checkpoint (which is the only operation that
        // can move `wal_end_lsn` while we hold no foreign-writer lock — see
        // ADR 0058 background checkpoint worker) from a real concurrent
        // writer commit (multi-connection OCC, see ADR 0023). A checkpoint
        // bumps `checkpoint_epoch`; a foreign writer does not. If the epoch
        // advanced and no foreign commit occurred, the checkpoint preserved
        // every durable page, so our staged pages are safe to append at the
        // current WAL end.
        let current_epoch = wal.inner.checkpoint_epoch.load(Ordering::Acquire);
        if current_epoch == expected_checkpoint_epoch {
            return Err(DbError::transaction(format!(
                "transaction conflict: WAL advanced from {expected_latest_lsn} to {latest}"
            )));
        }
    }

    let mut offset = latest;
    if offset == 0 {
        offset = WAL_HEADER_SIZE;
    }

    let page_frame_len = FRAME_HEADER_SIZE + wal.inner.page_size as usize + FRAME_TRAILER_SIZE;
    let WalWriteState {
        page_batch,
        prepared_pages,
        delta_scratch,
    } = &mut *writer_state;
    page_batch.clear();
    page_batch.reserve(page_frame_len * pages.len() + COMMIT_FRAME_BYTES.len());

    // Look up all base pages under a single index lock for delta encoding.
    let (base_pages, retain_history_hint) = lookup_base_pages_batch(wal, pager, &pages, latest)?;

    prepared_pages.clear();
    prepared_pages.reserve(pages.len());
    for (i, (page_id, payload)) in pages.into_iter().enumerate() {
        let base = base_pages
            .get(i)
            .and_then(|b| b.as_ref())
            .map(|base| (&base.data[..], base.from_wal));
        let frame_offset = offset + page_batch.len() as u64;
        let (encoded_len, encoding) = append_best_page_frame_with_base(
            page_batch,
            delta_scratch,
            wal,
            page_id,
            &payload,
            base,
            retain_history_hint,
        )?;
        prepared_pages.push((page_id, payload, encoded_len, encoding, frame_offset));
    }
    let commit_start_lsn = offset;
    page_batch.extend_from_slice(&COMMIT_FRAME_BYTES);
    let new_offset = offset + page_batch.len() as u64;
    let metadata_changed = ensure_capacity(wal, new_offset)?;
    write_all_at(wal.inner.file.as_ref(), offset, page_batch.as_slice())?;
    // Publish the logical end only after the frame bytes are in place so a
    // concurrent opener never trusts a WAL tail that has not been written yet.
    recovery::persist_header(&wal.inner.file, wal.inner.page_size, new_offset)?;
    offset = new_offset;
    sync_for_mode(wal, metadata_changed, new_offset)?;

    {
        let mut index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        // Check inside the index lock so begin_reader() cannot register
        // between the count check and the version clear (TOCTOU fix).
        let retain_history =
            retain_history_hint || wal.inner.reader_registry.active_reader_count()? > 0;
        let mut sidecar = wal.inner.index_sidecar.as_ref().map(|sidecar| {
            sidecar
                .lock()
                .expect("wal index sidecar lock should not be poisoned")
        });
        let mut version_lsn = commit_start_lsn;
        let prepared_count = prepared_pages.len();
        for (page_id, payload, encoded_len, encoding, frame_offset) in prepared_pages.drain(..) {
            version_lsn += encoded_len as u64;
            if let Some(sidecar) = sidecar.as_mut() {
                if retain_history && !index.contains_page(page_id) {
                    if let Some(previous) = sidecar.read_latest(page_id)? {
                        index.seed_latest(page_id, previous);
                    }
                }
                sidecar.clear_latest(page_id)?;
            }
            index.add_version(
                page_id,
                WalVersion::resident(
                    version_lsn,
                    frame_offset,
                    encoded_len as u32,
                    encoding,
                    Arc::from(payload),
                ),
                retain_history,
            );
        }
        // Update wal_end_lsn inside the index lock — same rationale as
        // commit_pages above.
        wal.inner
            .max_page_count
            .fetch_max(max_page_count, Ordering::AcqRel);
        wal.inner.wal_end_lsn.store(offset, Ordering::Release);
        let prepared_count_u32 = u32::try_from(prepared_count).unwrap_or(u32::MAX);
        wal.inner
            .pages_since_checkpoint
            .fetch_add(prepared_count_u32, Ordering::AcqRel);
    }
    drop(writer_state);
    demote_cold_versions(wal)?;
    maybe_auto_checkpoint(wal, pager)?;
    Ok(offset)
}

pub(crate) fn append_checkpoint_frame(wal: &WalHandle, checkpoint_lsn: u64) -> Result<u64> {
    let mut offset = wal.latest_snapshot();
    if offset == 0 {
        offset = WAL_HEADER_SIZE;
    }

    let frame = WalFrame::checkpoint(checkpoint_lsn);
    let bytes = frame.encode(wal.inner.page_size)?;
    let new_offset = offset + bytes.len() as u64;
    let metadata_changed = ensure_capacity(wal, new_offset)?;
    write_all_at(wal.inner.file.as_ref(), offset, &bytes)?;
    recovery::persist_header(&wal.inner.file, wal.inner.page_size, new_offset)?;
    offset = new_offset;
    // Checkpoint frames are a critical recovery boundary: even under
    // AsyncCommit, we want them durable before advancing wal_end_lsn so
    // recovery cannot observe a checkpoint frame that is not yet on disk.
    sync_durably(wal, metadata_changed)?;
    wal.inner.wal_end_lsn.store(offset, Ordering::Release);
    Ok(offset)
}

pub(crate) fn truncate_to_header(wal: &WalHandle) -> Result<()> {
    recovery::truncate_to_header(&wal.inner.file, wal.inner.page_size)?;
    sync_durably(wal, true)?;
    wal.inner.wal_end_lsn.store(0, Ordering::Release);
    wal.inner
        .allocated_len
        .store(WAL_HEADER_SIZE, Ordering::Release);
    Ok(())
}

/// Sync helper used by per-commit paths. Under `AsyncCommit` this is a no-op
/// and the background flusher will catch up; the writer records the new end
/// LSN with the flusher state so any concurrent `flush_to_durable` knows the
/// target. Under all other modes this performs the appropriate synchronous
/// fsync.
fn sync_for_mode(wal: &WalHandle, metadata_changed: bool, new_end_lsn: u64) -> Result<()> {
    match wal.inner.sync_mode {
        WalSyncMode::Full => {
            if metadata_changed {
                wal.inner.file.sync_metadata()
            } else {
                wal.inner.file.sync_data()
            }
        }
        WalSyncMode::Normal => wal.inner.file.sync_data(),
        WalSyncMode::AsyncCommit { .. } => {
            // SAFETY (durability): we publish the new dirty watermark *before*
            // returning so a subsequent `Db::sync()` cannot complete without
            // observing this commit.
            if let Some(state) = wal.inner.async_commit.as_ref() {
                state.note_write(new_end_lsn, metadata_changed);
            }
            Ok(())
        }
        WalSyncMode::TestingOnlyUnsafeNoSync => Ok(()),
    }
}

/// Force-sync helper for paths that must be durable regardless of sync mode
/// (checkpoint frames, WAL truncation). Bypasses the AsyncCommit deferral.
fn sync_durably(wal: &WalHandle, metadata_changed: bool) -> Result<()> {
    if metadata_changed {
        wal.inner.file.sync_metadata()
    } else {
        wal.inner.file.sync_data()
    }
}

fn ensure_capacity(wal: &WalHandle, required_len: u64) -> Result<bool> {
    let current_len = wal.inner.allocated_len.load(Ordering::Acquire);
    if current_len >= required_len {
        return Ok(false);
    }
    let target_len = required_len
        .div_ceil(WAL_PREALLOC_CHUNK_BYTES)
        .saturating_mul(WAL_PREALLOC_CHUNK_BYTES);
    wal.inner.file.set_len(target_len)?;
    wal.inner.allocated_len.store(target_len, Ordering::Release);
    Ok(true)
}

fn append_page_frame(
    output: &mut Vec<u8>,
    page_id: PageId,
    payload: &[u8],
    page_size: u32,
) -> Result<usize> {
    if page_id == 0 {
        return Err(DbError::corruption(
            "page WAL frames must have a non-zero page id",
        ));
    }
    if payload.len() != page_size as usize {
        return Err(DbError::internal(format!(
            "WAL frame payload length {} does not match expected payload length {}",
            payload.len(),
            page_size
        )));
    }
    let frame_len = FRAME_HEADER_SIZE + payload.len() + FRAME_TRAILER_SIZE;
    let start = output.len();
    output.resize(start + frame_len, 0);
    output[start] = FrameType::Page as u8;
    output[start + 1..start + FRAME_HEADER_SIZE].copy_from_slice(&page_id.to_le_bytes());
    let payload_start = start + FRAME_HEADER_SIZE;
    output[payload_start..payload_start + payload.len()].copy_from_slice(payload);
    Ok(frame_len)
}

fn append_best_page_frame_with_base(
    output: &mut Vec<u8>,
    delta_scratch: &mut Vec<u8>,
    wal: &WalHandle,
    page_id: PageId,
    payload: &[u8],
    base_page: Option<(&[u8], bool)>,
    _retain_history_hint: bool,
) -> Result<(usize, FrameEncoding)> {
    if let Some((base, from_wal)) = base_page {
        if !from_wal && encode_page_delta_into(delta_scratch, base, payload) {
            return append_page_delta_frame(output, page_id, delta_scratch)
                .map(|len| (len, FrameEncoding::PageDelta));
        }
    }
    append_page_frame(output, page_id, payload, wal.inner.page_size)
        .map(|len| (len, FrameEncoding::Page))
}

/// Look up base pages for an entire batch under a single index lock.
fn lookup_base_pages_batch(
    wal: &WalHandle,
    pager: &PagerHandle,
    pages: &[(PageId, Vec<u8>)],
    snapshot_lsn: u64,
) -> Result<(Vec<Option<DeltaBasePage>>, bool)> {
    let index = wal
        .inner
        .index
        .lock()
        .expect("wal index lock should not be poisoned");
    let retain_history_hint = wal.inner.reader_registry.active_reader_count()? > 0;
    let pages = pages
        .iter()
        .map(|(page_id, _)| {
            if let Some(page) =
                wal.materialize_latest_visible_locked(&index, pager, *page_id, snapshot_lsn)?
            {
                Ok(Some(DeltaBasePage {
                    data: page,
                    from_wal: true,
                }))
            } else if let Ok(page) = pager.read_page(*page_id) {
                Ok(Some(DeltaBasePage {
                    data: page,
                    from_wal: false,
                }))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((pages, retain_history_hint))
}

fn demote_cold_versions(wal: &WalHandle) -> Result<()> {
    let retain_recent = wal.inner.resident_versions_per_page;
    let min_reader_snapshot = wal.inner.reader_registry.min_snapshot_lsn()?;
    let mut index = wal
        .inner
        .index
        .lock()
        .expect("wal index lock should not be poisoned");
    index.demote_cold(min_reader_snapshot, retain_recent);
    if min_reader_snapshot.is_none() {
        if let Some(sidecar) = &wal.inner.index_sidecar {
            let mut sidecar = sidecar
                .lock()
                .expect("wal index sidecar lock should not be poisoned");
            wal.spill_excess_hot_pages_locked(&mut index, &mut sidecar)?;
        }
    }
    Ok(())
}

/// Evaluate the size-based auto-checkpoint thresholds (ADR 0137) and trigger
/// a synchronous checkpoint when both thresholds and reader gating allow it.
///
/// MUST be called with the writer state guard already dropped — `checkpoint`
/// re-acquires it. Best-effort: when readers are active or another checkpoint
/// is already pending, this is a silent no-op; the next reader-free commit
/// re-evaluates and may trigger then.
fn maybe_auto_checkpoint(wal: &WalHandle, pager: &PagerHandle) -> Result<()> {
    let cfg = wal.inner.auto_checkpoint;
    let pages_threshold = cfg.threshold_pages;
    let bytes_threshold = cfg.threshold_bytes;
    if pages_threshold == 0 && bytes_threshold == 0 {
        return Ok(());
    }

    let pages_since = wal.inner.pages_since_checkpoint.load(Ordering::Acquire);
    let pages_hit = pages_threshold != 0 && pages_since >= pages_threshold;

    let bytes_since = wal.latest_snapshot().saturating_sub(WAL_HEADER_SIZE);
    let bytes_hit = bytes_threshold != 0 && bytes_since >= bytes_threshold;

    if !pages_hit && !bytes_hit {
        return Ok(());
    }
    if wal.inner.checkpoint_pending.load(Ordering::Acquire) {
        return Ok(());
    }

    // ADR 0058: prefer the background worker when present so the writer's
    // commit hot path is not blocked by checkpoint copyback. The worker
    // applies the same reader-active gating below.
    if let Some(bg) = wal.inner.bg_checkpointer.get() {
        bg.wake();
        return Ok(());
    }

    // Skip when readers are active so we preserve ADR 0019 retention semantics
    // and avoid a redundant `prune_at_or_below` pass that would not actually
    // free memory.
    if wal.inner.reader_registry.active_reader_count()? > 0 {
        return Ok(());
    }

    super::checkpoint::checkpoint(wal, pager, cfg.checkpoint_timeout_sec)
}

fn append_page_delta_frame(output: &mut Vec<u8>, page_id: PageId, payload: &[u8]) -> Result<usize> {
    if page_id == 0 {
        return Err(DbError::corruption(
            "page WAL frames must have a non-zero page id",
        ));
    }
    let frame_len = FRAME_HEADER_SIZE + payload.len() + FRAME_TRAILER_SIZE;
    let start = output.len();
    output.resize(start + frame_len, 0);
    output[start] = FrameType::PageDelta as u8;
    output[start + 1..start + FRAME_HEADER_SIZE].copy_from_slice(&page_id.to_le_bytes());
    let payload_start = start + FRAME_HEADER_SIZE;
    output[payload_start..payload_start + payload.len()].copy_from_slice(payload);
    Ok(frame_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page;

    #[test]
    fn append_page_frame_rejects_zero_page_id() {
        let mut out = Vec::new();
        let payload = vec![0u8; page::DEFAULT_PAGE_SIZE as usize];
        let res = append_page_frame(&mut out, 0, &payload, page::DEFAULT_PAGE_SIZE);
        assert!(res.is_err());
    }

    #[test]
    fn append_page_frame_rejects_size_mismatch() {
        let mut out = Vec::new();
        let payload = vec![0u8; 10];
        let res = append_page_frame(&mut out, 1, &payload, page::DEFAULT_PAGE_SIZE);
        assert!(res.is_err());
    }

    #[test]
    fn append_page_frame_encodes_frame() {
        let mut out = Vec::new();
        let payload = vec![0xAA; page::DEFAULT_PAGE_SIZE as usize];
        let res =
            append_page_frame(&mut out, 5, &payload, page::DEFAULT_PAGE_SIZE).expect("append");
        assert_eq!(res, FRAME_HEADER_SIZE + payload.len() + FRAME_TRAILER_SIZE);
        assert_eq!(out[0], FrameType::Page as u8);
        // page id le bytes
        let id = u32::from_le_bytes(out[1..5].try_into().expect("id bytes"));
        assert_eq!(id, 5);
    }

    // --- ADR 0137: size-based auto-checkpoint trigger ---

    use std::path::Path;
    use std::sync::Arc;

    use crate::config::{DbConfig, WalSyncMode};
    use crate::storage::{write_database_bootstrap_vfs, DatabaseHeader, PagerHandle};
    use crate::vfs::mem::MemVfs;
    use crate::vfs::{FileKind, OpenMode, Vfs, VfsHandle};

    use super::super::WalHandle;

    fn setup_wal(
        threshold_pages: u32,
        threshold_bytes: u64,
    ) -> (VfsHandle, PagerHandle, WalHandle) {
        let mem_vfs: Arc<dyn Vfs> = Arc::new(MemVfs::default());
        let vfs = VfsHandle::from_vfs(Arc::clone(&mem_vfs));
        let path = Path::new(":memory:");
        let file = vfs
            .open(path, OpenMode::OpenOrCreate, FileKind::Database)
            .expect("create db file");
        let header = DatabaseHeader::new(page::DEFAULT_PAGE_SIZE);
        write_database_bootstrap_vfs(file.as_ref(), &header).expect("bootstrap");
        let pager = PagerHandle::open(Arc::clone(&file), header, 1).expect("pager");
        let cfg = DbConfig {
            wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
            wal_checkpoint_threshold_pages: threshold_pages,
            wal_checkpoint_threshold_bytes: threshold_bytes,
            release_freed_memory_after_checkpoint: false,
            background_checkpoint_worker: false,
            ..DbConfig::default()
        };
        let wal = WalHandle::acquire(&vfs, path, &cfg, &pager).expect("acquire");
        (vfs, pager, wal)
    }

    fn payload(byte: u8) -> Vec<u8> {
        vec![byte; page::DEFAULT_PAGE_SIZE as usize]
    }

    #[test]
    fn auto_checkpoint_disabled_by_zero_thresholds() {
        let (_vfs, pager, wal) = setup_wal(0, 0);
        for i in 1u32..=20 {
            let pid = page::CATALOG_ROOT_PAGE_ID + i;
            wal.commit_pages(&pager, vec![(pid, payload(i as u8))], pid)
                .expect("commit");
        }
        // No threshold => no auto-checkpoint => versions accumulate.
        assert!(wal.version_count().expect("count") >= 20);
        assert_eq!(wal.checkpoint_epoch(), 0);
    }

    #[test]
    fn auto_checkpoint_fires_on_page_threshold() {
        let (_vfs, pager, wal) = setup_wal(8, 0);
        for i in 1u32..=32 {
            let pid = page::CATALOG_ROOT_PAGE_ID + i;
            wal.commit_pages(&pager, vec![(pid, payload(i as u8))], pid)
                .expect("commit");
        }
        // With a page threshold of 8 over 32 commits we expect at least 4
        // checkpoint epochs to have advanced.
        assert!(
            wal.checkpoint_epoch() >= 4,
            "expected >=4 epochs, saw {}",
            wal.checkpoint_epoch()
        );
        // After the final auto-checkpoint with no readers the index is cleared.
        assert!(
            wal.version_count().expect("count") < 8,
            "expected bounded versions, saw {}",
            wal.version_count().expect("count")
        );
    }

    #[test]
    fn auto_checkpoint_fires_on_byte_threshold() {
        // One commit ≈ FRAME_HEADER + page + FRAME_TRAILER + COMMIT_FRAME.
        // Set the byte threshold low enough that two commits trigger.
        let (_vfs, pager, wal) = setup_wal(0, 1024);
        for i in 1u32..=10 {
            let pid = page::CATALOG_ROOT_PAGE_ID + i;
            wal.commit_pages(&pager, vec![(pid, payload(i as u8))], pid)
                .expect("commit");
        }
        assert!(
            wal.checkpoint_epoch() >= 1,
            "expected at least one auto-checkpoint, saw {}",
            wal.checkpoint_epoch()
        );
    }

    #[test]
    fn auto_checkpoint_skipped_while_reader_active() {
        let (_vfs, pager, wal) = setup_wal(2, 0);
        let pid = page::CATALOG_ROOT_PAGE_ID + 1;
        wal.commit_pages(&pager, vec![(pid, payload(0xAB))], pid)
            .expect("first commit");
        let _reader = wal.begin_reader().expect("reader");
        let initial_epoch = wal.checkpoint_epoch();
        for i in 2u32..=10 {
            let p = page::CATALOG_ROOT_PAGE_ID + i;
            wal.commit_pages(&pager, vec![(p, payload(i as u8))], p)
                .expect("commit");
        }
        // The active reader must block the auto-checkpoint trigger entirely.
        assert_eq!(wal.checkpoint_epoch(), initial_epoch);
        assert!(wal.version_count().expect("count") >= 10);
    }
}
