//! WAL append and durability logic.
//!
//! Implements:
//! - design/adr/0003-snapshot-lsn-atomicity.md

use std::sync::atomic::Ordering;

use crate::config::WalSyncMode;
use crate::error::{DbError, Result};
use crate::storage::page::PageId;
use crate::vfs::write_all_at;

use super::format::{
    FrameType, WalFrame, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE, WAL_HEADER_SIZE,
};
use super::index::WalVersion;
use super::recovery;
use super::WalHandle;

const WAL_PREALLOC_CHUNK_BYTES: u64 = 64 << 20;

pub(crate) fn commit_pages(
    wal: &WalHandle,
    pages: Vec<(PageId, Vec<u8>)>,
    max_page_count: u32,
) -> Result<u64> {
    let _writer_guard = wal
        .inner
        .write_lock
        .lock()
        .expect("wal write lock should not be poisoned");

    let mut offset = wal.latest_snapshot();
    if offset == 0 {
        offset = WAL_HEADER_SIZE;
    }

    let page_frame_len = FRAME_HEADER_SIZE + wal.inner.page_size as usize + FRAME_TRAILER_SIZE;
    let commit_frame_len = FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE;
    let mut batch = Vec::with_capacity(page_frame_len * pages.len() + commit_frame_len);
    let mut committed = Vec::with_capacity(pages.len());
    let mut next_lsn = offset;
    for (page_id, payload) in pages {
        let frame_len = append_page_frame(&mut batch, page_id, &payload, wal.inner.page_size)?;
        next_lsn += frame_len as u64;
        committed.push((
            page_id,
            WalVersion {
                lsn: next_lsn,
                data: payload,
            },
        ));
    }
    next_lsn += append_commit_frame(&mut batch) as u64;
    let metadata_changed = ensure_capacity(wal, offset + batch.len() as u64)?;
    write_all_at(wal.inner.file.as_ref(), offset, &batch)?;
    offset = next_lsn;

    recovery::persist_header(&wal.inner.file, wal.inner.page_size, offset)?;
    sync_for_mode(wal.inner.sync_mode, wal, metadata_changed)?;

    let retain_history = wal.inner.reader_registry.active_reader_count()? > 0;
    {
        let mut index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        for (page_id, version) in committed {
            index.add_version(page_id, version, retain_history);
        }
    }

    wal.inner
        .max_page_count
        .fetch_max(max_page_count, Ordering::AcqRel);
    wal.inner.wal_end_lsn.store(offset, Ordering::Release);
    Ok(offset)
}

pub(crate) fn commit_pages_if_latest(
    wal: &WalHandle,
    pages: Vec<(PageId, Vec<u8>)>,
    max_page_count: u32,
    expected_latest_lsn: u64,
) -> Result<u64> {
    let _writer_guard = wal
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
    let commit_frame_len = FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE;
    let mut batch = Vec::with_capacity(page_frame_len * pages.len() + commit_frame_len);
    let mut committed = Vec::with_capacity(pages.len());
    let mut next_lsn = offset;
    for (page_id, payload) in pages {
        let frame_len = append_page_frame(&mut batch, page_id, &payload, wal.inner.page_size)?;
        next_lsn += frame_len as u64;
        committed.push((
            page_id,
            WalVersion {
                lsn: next_lsn,
                data: payload,
            },
        ));
    }
    next_lsn += append_commit_frame(&mut batch) as u64;
    let metadata_changed = ensure_capacity(wal, offset + batch.len() as u64)?;
    write_all_at(wal.inner.file.as_ref(), offset, &batch)?;
    offset = next_lsn;

    recovery::persist_header(&wal.inner.file, wal.inner.page_size, offset)?;
    sync_for_mode(wal.inner.sync_mode, wal, metadata_changed)?;

    let retain_history = wal.inner.reader_registry.active_reader_count()? > 0;
    {
        let mut index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        for (page_id, version) in committed {
            index.add_version(page_id, version, retain_history);
        }
    }

    wal.inner
        .max_page_count
        .fetch_max(max_page_count, Ordering::AcqRel);
    wal.inner.wal_end_lsn.store(offset, Ordering::Release);
    Ok(offset)
}

pub(crate) fn append_checkpoint_frame(wal: &WalHandle, checkpoint_lsn: u64) -> Result<u64> {
    let mut offset = wal.latest_snapshot();
    if offset == 0 {
        offset = WAL_HEADER_SIZE;
    }

    let frame = WalFrame::checkpoint(checkpoint_lsn);
    let bytes = frame.encode(wal.inner.page_size)?;
    let metadata_changed = ensure_capacity(wal, offset + bytes.len() as u64)?;
    write_all_at(wal.inner.file.as_ref(), offset, &bytes)?;
    offset += bytes.len() as u64;
    recovery::persist_header(&wal.inner.file, wal.inner.page_size, offset)?;
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
    wal.inner
        .allocated_len
        .store(target_len, Ordering::Release);
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
    output.push(FrameType::Page as u8);
    output.extend_from_slice(&page_id.to_le_bytes());
    output.extend_from_slice(payload);
    output.extend_from_slice(&0_u64.to_le_bytes());
    Ok(FRAME_HEADER_SIZE + payload.len() + FRAME_TRAILER_SIZE)
}

fn append_commit_frame(output: &mut Vec<u8>) -> usize {
    output.push(FrameType::Commit as u8);
    output.extend_from_slice(&0_u32.to_le_bytes());
    output.extend_from_slice(&0_u64.to_le_bytes());
    FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE
}
