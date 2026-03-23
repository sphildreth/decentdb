//! WAL append and durability logic.
//!
//! Implements:
//! - design/adr/0003-snapshot-lsn-atomicity.md

use std::sync::atomic::Ordering;

use crate::config::WalSyncMode;
use crate::error::Result;
use crate::storage::page::PageId;
use crate::vfs::write_all_at;

use super::format::{WalFrame, WAL_HEADER_SIZE};
use super::index::WalVersion;
use super::recovery;
use super::WalHandle;

pub(crate) fn commit_pages(
    wal: &WalHandle,
    pages: &[(PageId, Vec<u8>)],
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

    let mut committed = Vec::new();
    for (page_id, payload) in pages {
        let frame = WalFrame::page(*page_id, payload.clone());
        let bytes = frame.encode(wal.inner.page_size)?;
        write_all_at(wal.inner.file.as_ref(), offset, &bytes)?;
        offset += bytes.len() as u64;
        committed.push((
            *page_id,
            WalVersion {
                lsn: offset,
                data: payload.clone(),
            },
        ));
    }

    let commit = WalFrame::commit();
    let commit_bytes = commit.encode(wal.inner.page_size)?;
    write_all_at(wal.inner.file.as_ref(), offset, &commit_bytes)?;
    offset += commit_bytes.len() as u64;

    recovery::persist_header(&wal.inner.file, wal.inner.page_size, offset)?;
    sync_for_mode(wal.inner.sync_mode, wal)?;

    {
        let mut index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        for (page_id, version) in committed {
            index.add_version(page_id, version);
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
    write_all_at(wal.inner.file.as_ref(), offset, &bytes)?;
    offset += bytes.len() as u64;
    recovery::persist_header(&wal.inner.file, wal.inner.page_size, offset)?;
    sync_for_mode(wal.inner.sync_mode, wal)?;
    wal.inner.wal_end_lsn.store(offset, Ordering::Release);
    Ok(offset)
}

pub(crate) fn truncate_to_header(wal: &WalHandle) -> Result<()> {
    recovery::truncate_to_header(&wal.inner.file, wal.inner.page_size)?;
    sync_for_mode(wal.inner.sync_mode, wal)?;
    wal.inner.wal_end_lsn.store(0, Ordering::Release);
    Ok(())
}

fn sync_for_mode(sync_mode: WalSyncMode, wal: &WalHandle) -> Result<()> {
    match sync_mode {
        WalSyncMode::Full => wal.inner.file.sync_metadata(),
        WalSyncMode::Normal => wal.inner.file.sync_data(),
        WalSyncMode::TestingOnlyUnsafeNoSync => Ok(()),
    }
}
