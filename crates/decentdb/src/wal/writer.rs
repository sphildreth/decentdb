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

use super::delta::encode_page_delta;
use super::format::{FrameType, WalFrame, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE, WAL_HEADER_SIZE};
use super::index::WalVersion;
use super::recovery;
use super::WalHandle;
use super::WalWriteState;

const WAL_PREALLOC_CHUNK_BYTES: u64 = 64 << 20;
const COMMIT_FRAME_BYTES: [u8; FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE] =
    [FrameType::Commit as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

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
    } = &mut *writer_state;
    page_batch.clear();
    page_batch.reserve(page_frame_len * pages.len() + COMMIT_FRAME_BYTES.len());
    let latest_snapshot = wal.latest_snapshot();

    // Look up all base pages under a single index lock for delta encoding.
    let base_pages = lookup_base_pages_batch(wal, &pages, latest_snapshot);

    prepared_pages.clear();
    prepared_pages.reserve(pages.len());
    for (i, (page_id, payload)) in pages.into_iter().enumerate() {
        let base = base_pages
            .get(i)
            .and_then(|b| b.as_ref())
            .map(|data| &data[..]);
        let encoded_len =
            append_best_page_frame_with_base(page_batch, wal, pager, page_id, &payload, base)?;
        prepared_pages.push((page_id, payload, encoded_len));
    }
    let commit_start_lsn = offset;
    page_batch.extend_from_slice(&COMMIT_FRAME_BYTES);
    let new_offset = offset + page_batch.len() as u64;
    let metadata_changed = ensure_capacity(wal, new_offset)?;
    // Write the WAL header (small, at offset 0) before the sequential frame
    // data. This avoids dirtying two distant file regions before fdatasync,
    // which can nearly halve fsync latency on NVMe.
    recovery::persist_header(&wal.inner.file, wal.inner.page_size, new_offset)?;
    write_all_at(wal.inner.file.as_ref(), offset, page_batch)?;
    offset = new_offset;
    sync_for_mode(wal.inner.sync_mode, wal, metadata_changed)?;

    {
        let mut index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        // Check inside the index lock so begin_reader() cannot register
        // between the count check and the version clear (TOCTOU fix).
        let retain_history = wal.inner.reader_registry.active_reader_count()? > 0;
        let mut version_lsn = commit_start_lsn;
        for (page_id, payload, encoded_len) in prepared_pages.drain(..) {
            version_lsn += encoded_len as u64;
            index.add_version(
                page_id,
                WalVersion {
                    lsn: version_lsn,
                    data: Arc::from(payload),
                },
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
    }
    Ok(offset)
}

pub(crate) fn commit_pages_if_latest(
    wal: &WalHandle,
    pager: &PagerHandle,
    pages: Vec<(PageId, Vec<u8>)>,
    max_page_count: u32,
    expected_latest_lsn: u64,
) -> Result<u64> {
    let mut writer_state = wal
        .inner
        .write_lock
        .lock()
        .expect("wal write lock should not be poisoned");

    let latest = wal.latest_snapshot();
    if latest != expected_latest_lsn {
        return Err(DbError::transaction(format!(
            "transaction conflict: WAL advanced from {expected_latest_lsn} to {latest}"
        )));
    }

    let mut offset = latest;
    if offset == 0 {
        offset = WAL_HEADER_SIZE;
    }

    let page_frame_len = FRAME_HEADER_SIZE + wal.inner.page_size as usize + FRAME_TRAILER_SIZE;
    let WalWriteState {
        page_batch,
        prepared_pages,
    } = &mut *writer_state;
    page_batch.clear();
    page_batch.reserve(page_frame_len * pages.len() + COMMIT_FRAME_BYTES.len());

    // Look up all base pages under a single index lock for delta encoding.
    let base_pages = lookup_base_pages_batch(wal, &pages, latest);

    prepared_pages.clear();
    prepared_pages.reserve(pages.len());
    for (i, (page_id, payload)) in pages.into_iter().enumerate() {
        let base = base_pages
            .get(i)
            .and_then(|b| b.as_ref())
            .map(|data| &data[..]);
        let encoded_len =
            append_best_page_frame_with_base(page_batch, wal, pager, page_id, &payload, base)?;
        prepared_pages.push((page_id, payload, encoded_len));
    }
    let commit_start_lsn = offset;
    page_batch.extend_from_slice(&COMMIT_FRAME_BYTES);
    let new_offset = offset + page_batch.len() as u64;
    let metadata_changed = ensure_capacity(wal, new_offset)?;
    // Write header before frame data — see commit_pages for rationale.
    recovery::persist_header(&wal.inner.file, wal.inner.page_size, new_offset)?;
    write_all_at(wal.inner.file.as_ref(), offset, page_batch)?;
    offset = new_offset;
    sync_for_mode(wal.inner.sync_mode, wal, metadata_changed)?;

    {
        let mut index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        // Check inside the index lock so begin_reader() cannot register
        // between the count check and the version clear (TOCTOU fix).
        let retain_history = wal.inner.reader_registry.active_reader_count()? > 0;
        let mut version_lsn = commit_start_lsn;
        for (page_id, payload, encoded_len) in prepared_pages.drain(..) {
            version_lsn += encoded_len as u64;
            index.add_version(
                page_id,
                WalVersion {
                    lsn: version_lsn,
                    data: Arc::from(payload),
                },
                retain_history,
            );
        }
        // Update wal_end_lsn inside the index lock — same rationale as
        // commit_pages above.
        wal.inner
            .max_page_count
            .fetch_max(max_page_count, Ordering::AcqRel);
        wal.inner.wal_end_lsn.store(offset, Ordering::Release);
    }
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
    recovery::persist_header(&wal.inner.file, wal.inner.page_size, new_offset)?;
    write_all_at(wal.inner.file.as_ref(), offset, &bytes)?;
    offset = new_offset;
    sync_for_mode(wal.inner.sync_mode, wal, metadata_changed)?;
    wal.inner.wal_end_lsn.store(offset, Ordering::Release);
    Ok(offset)
}

pub(crate) fn truncate_to_header(wal: &WalHandle) -> Result<()> {
    recovery::truncate_to_header(&wal.inner.file, wal.inner.page_size)?;
    sync_for_mode(wal.inner.sync_mode, wal, true)?;
    wal.inner.wal_end_lsn.store(0, Ordering::Release);
    wal.inner
        .allocated_len
        .store(WAL_HEADER_SIZE, Ordering::Release);
    Ok(())
}

fn sync_for_mode(sync_mode: WalSyncMode, wal: &WalHandle, metadata_changed: bool) -> Result<()> {
    match sync_mode {
        WalSyncMode::Full => {
            if metadata_changed {
                wal.inner.file.sync_metadata()
            } else {
                wal.inner.file.sync_data()
            }
        }
        WalSyncMode::Normal => wal.inner.file.sync_data(),
        WalSyncMode::TestingOnlyUnsafeNoSync => Ok(()),
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
    wal: &WalHandle,
    pager: &PagerHandle,
    page_id: PageId,
    payload: &[u8],
    wal_base: Option<&[u8]>,
) -> Result<usize> {
    if let Some(base) = wal_base {
        if let Some(delta_payload) = encode_page_delta(base, payload) {
            return append_page_delta_frame(output, page_id, &delta_payload);
        }
    } else if let Ok(base) = pager.read_page(page_id) {
        if let Some(delta_payload) = encode_page_delta(&base, payload) {
            return append_page_delta_frame(output, page_id, &delta_payload);
        }
    }
    append_page_frame(output, page_id, payload, wal.inner.page_size)
}

/// Look up base pages for an entire batch under a single index lock.
fn lookup_base_pages_batch(
    wal: &WalHandle,
    pages: &[(PageId, Vec<u8>)],
    snapshot_lsn: u64,
) -> Vec<Option<Arc<[u8]>>> {
    let index = wal
        .inner
        .index
        .lock()
        .expect("wal index lock should not be poisoned");
    pages
        .iter()
        .map(|(page_id, _)| {
            index
                .latest_visible(*page_id, snapshot_lsn)
                .map(|v| Arc::clone(&v.data))
        })
        .collect()
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
}
