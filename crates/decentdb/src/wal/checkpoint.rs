//! Reader-aware checkpoint copyback and WAL pruning.
//!
//! Implements:
//! - design/adr/0004-wal-checkpoint-strategy.md
//! - design/adr/0056-wal-index-pruning-on-checkpoint.md

use std::sync::atomic::Ordering;

use crate::error::Result;
use crate::storage::PagerHandle;

use super::writer;
use super::WalHandle;

pub(crate) fn checkpoint(wal: &WalHandle, pager: &PagerHandle, timeout_sec: u64) -> Result<()> {
    struct PendingReset<'a>(&'a WalHandle);

    impl Drop for PendingReset<'_> {
        fn drop(&mut self) {
            self.0
                .inner
                .checkpoint_pending
                .store(false, Ordering::SeqCst);
        }
    }

    let _writer_guard = wal
        .inner
        .write_lock
        .lock()
        .expect("wal write lock should not be poisoned");
    wal.inner.checkpoint_pending.store(true, Ordering::SeqCst);
    let _pending_reset = PendingReset(wal);

    let current_lsn = wal.latest_snapshot();
    let safe_lsn = wal
        .inner
        .reader_registry
        .min_snapshot_lsn()?
        .unwrap_or(current_lsn);

    let latest_versions = {
        let index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        index.latest_versions_at_or_before(safe_lsn)
    };

    let page_ids: Vec<_> = latest_versions
        .iter()
        .map(|(page_id, _)| *page_id)
        .collect();
    for (page_id, version) in &latest_versions {
        pager.write_page_direct(*page_id, &version.data)?;
    }
    let header = pager.header_from_disk()?;
    pager.refresh_from_disk(header)?;
    if let Some(page_count) = pager.truncate_freelist_tail()? {
        wal.reset_max_page_count(page_count);
    } else {
        wal.reset_max_page_count(pager.on_disk_page_count()?);
    }
    pager.set_last_checkpoint_lsn(safe_lsn)?;

    let _checkpoint_end = writer::append_checkpoint_frame(wal, safe_lsn)?;

    if wal.inner.reader_registry.active_reader_count()? == 0 {
        {
            let mut index = wal
                .inner
                .index
                .lock()
                .expect("wal index lock should not be poisoned");
            index.clear();
        }
        writer::truncate_to_header(wal)?;
    } else {
        {
            let mut index = wal
                .inner
                .index
                .lock()
                .expect("wal index lock should not be poisoned");
            index.prune_at_or_below(&page_ids, safe_lsn);
        }
        let warnings = wal
            .inner
            .reader_registry
            .capture_long_reader_warnings(timeout_sec)?;
        for warning in warnings {
            eprintln!("decentdb checkpoint warning: {warning}");
        }
    }

    wal.inner.checkpoint_epoch.fetch_add(1, Ordering::AcqRel);

    Ok(())
}
