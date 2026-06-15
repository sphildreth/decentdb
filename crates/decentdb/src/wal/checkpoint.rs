//! Reader-safe checkpoint copyback and WAL pruning.
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

    let _process_guard = wal.lock_process_checkpoint()?;
    wal.refresh_from_coordination(pager)?;
    let _writer_guard = wal
        .inner
        .write_lock
        .lock()
        .expect("wal write lock should not be poisoned");
    wal.inner.checkpoint_pending.store(true, Ordering::SeqCst);
    let _pending_reset = PendingReset(wal);
    {
        // Fence with begin_reader(), which holds the index lock while it
        // re-checks checkpoint_pending and registers its snapshot. Without
        // this handoff a checkpoint can compute safe_lsn before a reader that
        // already passed the pending check becomes visible in the registry,
        // then copy back newer pages underneath that reader's snapshot.
        let _index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
    }

    let current_lsn = wal.latest_snapshot();
    let active_reader_lsn = wal.inner.reader_registry.min_snapshot_lsn()?;
    let retained_snapshot_lsn = wal.retained_snapshot_lsn();
    let process_retention = wal.process_reader_retention()?;
    let process_readers_block_checkpoint = process_retention
        .as_ref()
        .is_some_and(|retention| retention.active_count > 0 || retention.truncation_blocked);
    let named_snapshot_retained =
        retained_snapshot_lsn.is_some_and(|snapshot_lsn| snapshot_lsn < current_lsn);
    if active_reader_lsn.is_some() || named_snapshot_retained || process_readers_block_checkpoint {
        // Copyback mutates main-db pages one at a time. Readers normally use
        // WAL versions for changed pages, but overflow and delta paths can
        // still fall back to main-db pages for stable bases. Keep the data
        // file unchanged while snapshots are live; a later reader-free
        // checkpoint will copy back and truncate the WAL.
        let _warnings = wal
            .inner
            .reader_registry
            .capture_long_reader_warnings(timeout_sec)?;
        return Ok(());
    }
    let safe_lsn = current_lsn;

    let mut latest_versions = wal
        .inner
        .checkpoint_scratch
        .lock()
        .expect("checkpoint scratch lock should not be poisoned");
    {
        let index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        index.populate_latest_versions_at_or_before(safe_lsn, &mut latest_versions);
    }
    if let Some(sidecar) = &wal.inner.index_sidecar {
        sidecar
            .lock()
            .expect("wal index sidecar lock should not be poisoned")
            .populate_latest_versions_at_or_before(safe_lsn, &mut latest_versions)?;
        latest_versions.sort_by_key(|(page_id, _)| *page_id);
    }

    for (page_id, version) in latest_versions.iter() {
        let payload = wal.materialize_checkpoint_version(pager, *page_id, version)?;
        pager.write_page_direct(*page_id, &payload)?;
    }
    // Drop large `Arc<[u8]>` references early so the checkpoint copyback's
    // peak heap footprint is not retained across the disk-flush stall.
    latest_versions.clear();
    drop(latest_versions);
    let header = pager.header_from_disk()?;
    pager.refresh_from_disk(header)?;
    if let Some(page_count) = pager.truncate_freelist_tail()? {
        wal.reset_max_page_count(page_count);
    } else {
        wal.reset_max_page_count(pager.on_disk_page_count()?);
    }
    pager.set_last_checkpoint_lsn(safe_lsn)?;

    let _checkpoint_end = writer::append_checkpoint_frame(wal, safe_lsn)?;

    {
        let mut index = wal
            .inner
            .index
            .lock()
            .expect("wal index lock should not be poisoned");
        // Check inside the index lock to avoid racing with begin_reader().
        // Only truncate WAL when safe_lsn covers all committed data
        // (i.e. no readers were active when we started, so we wrote
        // every version).  If safe_lsn < current_lsn, a reader that
        // dropped after we computed safe_lsn would cause us to lose
        // post-safe_lsn commits if we truncated.
        if safe_lsn >= current_lsn {
            index.clear();
            if let Some(sidecar) = &wal.inner.index_sidecar {
                sidecar
                    .lock()
                    .expect("wal index sidecar lock should not be poisoned")
                    .clear()?;
            }
            drop(index);
            writer::truncate_to_header(wal)?;
        } else {
            unreachable!("reader-blocked checkpoints return before copyback");
        }
    }

    wal.inner.checkpoint_epoch.fetch_add(1, Ordering::AcqRel);
    wal.publish_process_checkpoint(safe_lsn, wal.latest_snapshot())?;
    // Reset the size-based trigger counter (ADR 0137). The byte threshold
    // resets implicitly because `truncate_to_header` zeroes `wal_end_lsn`;
    // when readers prevented truncation the next commit will re-evaluate
    // against the still-larger WAL but with `pages_since_checkpoint = 0`.
    wal.inner.pages_since_checkpoint.store(0, Ordering::Release);

    // Return freed heap arenas to the OS on platforms where it helps.
    // No-op on non-Linux/non-glibc targets. ADR 0138.
    if wal.inner.auto_checkpoint.release_freed_after_checkpoint {
        super::platform::release_freed_heap();
    }

    Ok(())
}
