//! Background WAL flusher for `WalSyncMode::AsyncCommit`.
//!
//! Implements:
//! - design/adr/0135-async-commit-wal-group-commit.md
//!
//! Under `AsyncCommit` mode, commit calls return as soon as the WAL frame is
//! written; this module owns a single background thread per `SharedWalInner`
//! that periodically calls `sync_data` (or `sync_metadata`, if the WAL file
//! grew) to advance a `durable_lsn` watermark. Callers that need a hard
//! durability barrier use [`AsyncCommitState::flush_to_durable`].
//!
//! Shutdown is cooperative: dropping the state signals the flusher via an
//! `AtomicBool` + `Condvar`, joins the thread, and performs a final
//! synchronous flush so no committed work is lost on clean close.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::Result;
use crate::vfs::VfsFile;

/// Shared state between the WAL writer, foreground sync barriers, and the
/// background flusher thread.
#[derive(Debug)]
pub(crate) struct AsyncCommitState {
    inner: Arc<AsyncCommitInner>,
    flusher: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct AsyncCommitInner {
    /// Most recently written WAL end LSN. Compared against `durable_lsn` to
    /// decide whether a background flush is needed.
    dirty_lsn: AtomicU64,
    /// Highest WAL LSN known to be on stable storage.
    durable_lsn: AtomicU64,
    /// Set when an `ensure_capacity` grew the WAL file via `set_len`. Cleared
    /// after the next sync (which must use `sync_metadata` to persist the
    /// length change).
    metadata_dirty: AtomicBool,
    /// Set by Drop to ask the flusher thread to exit.
    shutdown: AtomicBool,
    /// Flush interval in milliseconds. At least 1.
    interval_ms: u32,
    /// Wakeup channel used both to interrupt the interval sleep on shutdown
    /// and to notify barrier waiters when `durable_lsn` advances.
    wake: (Mutex<()>, Condvar),
    /// Backing file the flusher operates on. Held as `Arc<dyn VfsFile>` so
    /// the flusher does not depend on `SharedWalInner`'s lifetime — the state
    /// outlives its referent in the Drop ordering of SharedWalInner.
    file: Arc<dyn VfsFile>,
}

impl AsyncCommitState {
    pub(crate) fn new(file: Arc<dyn VfsFile>, initial_lsn: u64, interval_ms: u32) -> Self {
        let interval_ms = interval_ms.max(1);
        let inner = Arc::new(AsyncCommitInner {
            dirty_lsn: AtomicU64::new(initial_lsn),
            durable_lsn: AtomicU64::new(initial_lsn),
            metadata_dirty: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            interval_ms,
            wake: (Mutex::new(()), Condvar::new()),
            file,
        });

        let flusher_inner = Arc::clone(&inner);
        let handle = thread::Builder::new()
            .name("decentdb-wal-flusher".to_string())
            .spawn(move || flusher_loop(flusher_inner))
            .expect("spawn wal flusher thread");

        Self {
            inner,
            flusher: Mutex::new(Some(handle)),
        }
    }

    /// Records that the WAL has been extended to `new_end_lsn` and (optionally)
    /// that the file length itself has grown. Called from the writer in
    /// place of a synchronous fsync.
    pub(crate) fn note_write(&self, new_end_lsn: u64, metadata_changed: bool) {
        // Use fetch_max so out-of-order calls (which shouldn't happen because
        // commits are serialized by the write lock, but defensive) cannot
        // regress the watermark.
        self.inner
            .dirty_lsn
            .fetch_max(new_end_lsn, Ordering::AcqRel);
        if metadata_changed {
            self.inner.metadata_dirty.store(true, Ordering::Release);
        }
        // No notify here: the flusher polls on its interval and does not
        // benefit from immediate wakeup. Only barrier waiters need notify,
        // which the flusher itself issues after each successful sync.
    }

    /// Blocks until every commit acknowledged before this call is durable on
    /// disk. Returns immediately if there is nothing to flush.
    pub(crate) fn flush_to_durable(&self) -> Result<()> {
        let target = self.inner.dirty_lsn.load(Ordering::Acquire);
        if self.inner.durable_lsn.load(Ordering::Acquire) >= target {
            return Ok(());
        }
        let (lock, cvar) = &self.inner.wake;
        let mut guard = lock
            .lock()
            .expect("async-commit wake lock should not be poisoned");
        while self.inner.durable_lsn.load(Ordering::Acquire) < target {
            if self.inner.shutdown.load(Ordering::Acquire) {
                // On shutdown the Drop path will perform a final flush; we do
                // not want to deadlock if shutdown raced ahead of us.
                break;
            }
            guard = cvar
                .wait_timeout(
                    guard,
                    Duration::from_millis(self.inner.interval_ms as u64 * 2),
                )
                .expect("async-commit wake cvar should not be poisoned")
                .0;
        }
        Ok(())
    }

    /// Highest LSN currently on disk, for diagnostics/tests.
    #[allow(dead_code)]
    pub(crate) fn durable_lsn(&self) -> u64 {
        self.inner.durable_lsn.load(Ordering::Acquire)
    }
}

impl Drop for AsyncCommitState {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        // Wake the flusher out of its interval sleep.
        {
            let (lock, cvar) = &self.inner.wake;
            let _guard = lock
                .lock()
                .expect("async-commit wake lock should not be poisoned");
            cvar.notify_all();
        }
        // Join cleanly so the final flush below races nothing.
        if let Some(handle) = self
            .flusher
            .lock()
            .expect("async-commit flusher slot should not be poisoned")
            .take()
        {
            let _ = handle.join();
        }
        // Final synchronous flush — guarantees that a clean close never loses
        // commits even if the last interval tick had not fired.
        let _ = perform_flush(&self.inner);
    }
}

fn flusher_loop(inner: Arc<AsyncCommitInner>) {
    let interval = Duration::from_millis(inner.interval_ms as u64);
    loop {
        if inner.shutdown.load(Ordering::Acquire) {
            return;
        }
        // Sleep on the condvar so shutdown can interrupt us promptly.
        {
            let (lock, cvar) = &inner.wake;
            let guard = lock
                .lock()
                .expect("async-commit wake lock should not be poisoned");
            let (_guard, _timeout) = cvar
                .wait_timeout(guard, interval)
                .expect("async-commit wake cvar should not be poisoned");
        }
        if inner.shutdown.load(Ordering::Acquire) {
            return;
        }
        let _ = perform_flush(&inner);
    }
}

fn perform_flush(inner: &AsyncCommitInner) -> Result<()> {
    let target = inner.dirty_lsn.load(Ordering::Acquire);
    if inner.durable_lsn.load(Ordering::Acquire) >= target {
        return Ok(());
    }
    // Snapshot and clear the metadata-dirty flag *before* the syscall: a
    // racing writer that grows the file while we are syncing will re-set the
    // flag, ensuring the next tick uses sync_metadata.
    let needed_metadata = inner.metadata_dirty.swap(false, Ordering::AcqRel);
    let result = if needed_metadata {
        inner.file.sync_metadata()
    } else {
        inner.file.sync_data()
    };
    if let Err(err) = result {
        // Restore the metadata flag so a subsequent attempt retries the right
        // syscall.
        if needed_metadata {
            inner.metadata_dirty.store(true, Ordering::Release);
        }
        return Err(err);
    }
    inner.durable_lsn.fetch_max(target, Ordering::AcqRel);
    // Notify any barrier waiters.
    let (lock, cvar) = &inner.wake;
    let _guard = lock
        .lock()
        .expect("async-commit wake lock should not be poisoned");
    cvar.notify_all();
    Ok(())
}
