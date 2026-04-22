//! Background checkpoint worker — implements ADR 0058.
//!
//! Moves auto-checkpoint work off the writer's commit hot path by running
//! `checkpoint::checkpoint()` on a dedicated thread. The writer calls
//! `BgCheckpointer::wake()` after a commit that crosses the WAL size
//! thresholds; the worker observes the signal and runs the checkpoint
//! without blocking the writer.
//!
//! Lifecycle:
//! - Constructed in `wal::shared::build_handle` when
//!   `DbConfig::background_checkpoint_worker == true` and at least one
//!   auto-checkpoint threshold is non-zero.
//! - Held in `SharedWalInner::bg` (`OnceLock<BgCheckpointer>`).
//! - Owns a `JoinHandle`; `Drop` signals shutdown via the shared `BgCtrl`
//!   condvar and joins the thread.
//! - The worker holds `Weak<SharedWalInner>` so it does not keep the WAL
//!   alive past the last external `WalHandle` clone.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::storage::PagerHandle;

use super::format::WAL_HEADER_SIZE;
use super::SharedWalInner;
use super::WalHandle;

#[derive(Debug, Default)]
struct BgCtrlState {
    /// Set by `Drop` to signal the worker to exit.
    shutdown: bool,
    /// Set by `wake()` to signal the worker to evaluate thresholds.
    wake: bool,
}

#[derive(Debug, Default)]
struct BgCtrl {
    state: Mutex<BgCtrlState>,
    cond: Condvar,
}

/// Owns the worker thread and its shared control block. Held as
/// `Option<BgCheckpointer>` so `SharedWalInner::drop` can take ownership
/// and join cleanly.
#[derive(Debug)]
pub(crate) struct BgCheckpointer {
    ctrl: Arc<BgCtrl>,
    join: Option<JoinHandle<()>>,
}

impl BgCheckpointer {
    /// Spawn the worker. The worker runs until either the shutdown flag is
    /// set or the `Weak<SharedWalInner>` can no longer be upgraded.
    pub(crate) fn start(weak: Weak<SharedWalInner>, pager: PagerHandle) -> Self {
        let ctrl = Arc::new(BgCtrl::default());
        let ctrl_for_thread = Arc::clone(&ctrl);
        let join = thread::Builder::new()
            .name("decentdb-checkpoint".into())
            .spawn(move || worker_loop(ctrl_for_thread, weak, pager))
            .expect("spawn checkpoint worker thread");
        Self {
            ctrl,
            join: Some(join),
        }
    }

    /// Signal the worker to evaluate thresholds. Cheap: takes the ctrl
    /// mutex briefly and notifies the condvar. Safe to call from the
    /// commit hot path.
    pub(crate) fn wake(&self) {
        let mut state = self
            .ctrl
            .state
            .lock()
            .expect("background checkpoint ctrl lock should not be poisoned");
        state.wake = true;
        self.ctrl.cond.notify_one();
    }
}

impl Drop for BgCheckpointer {
    fn drop(&mut self) {
        {
            let mut state = match self.ctrl.state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.shutdown = true;
            self.ctrl.cond.notify_all();
        }
        if let Some(join) = self.join.take() {
            // Best-effort join: if the worker panicked we don't want to
            // poison Drop. The panic has already been reported on the
            // worker's stack.
            let _ = join.join();
        }
    }
}

fn worker_loop(ctrl: Arc<BgCtrl>, weak: Weak<SharedWalInner>, pager: PagerHandle) {
    // The worker wakes either on a writer signal or on a periodic timeout
    // matching the configured `checkpoint_timeout_sec`. The timeout fallback
    // ensures that a workload that *just barely* crosses a threshold and
    // then goes idle still gets its WAL trimmed within bounded time.
    let timeout = match weak.upgrade() {
        Some(arc) => {
            let secs = arc.auto_checkpoint.checkpoint_timeout_sec.max(1);
            Duration::from_secs(secs)
        }
        None => return,
    };

    loop {
        // Wait for a wake signal, shutdown, or the periodic timeout. We
        // intentionally use a single `wait_timeout` (not a loop) and treat
        // the timeout as a wake — the threshold check below cheaply
        // returns when nothing has changed.
        {
            let state = match ctrl.state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            let mut state = if state.shutdown || state.wake {
                state
            } else {
                match ctrl.cond.wait_timeout(state, timeout) {
                    Ok((s, _result)) => s,
                    Err(poisoned) => poisoned.into_inner().0,
                }
            };
            if state.shutdown {
                return;
            }
            state.wake = false;
        }

        // Re-acquire the WAL via Weak. If all external handles have been
        // dropped we exit cleanly — `SharedWalInner::drop` will join us.
        let Some(arc) = weak.upgrade() else {
            return;
        };
        let wal = WalHandle { inner: arc };
        // Errors from a background checkpoint are silently swallowed: the
        // synchronous fallback in the writer (or the next wake) will
        // observe the same condition and surface it.
        let _ = run_checkpoint_if_needed(&wal, &pager);
        drop(wal);
    }
}

/// Identical evaluation policy to the synchronous fallback in
/// `wal::writer::maybe_auto_checkpoint`. Kept here (rather than calling
/// the writer's helper directly) to avoid bouncing through code that has
/// `pub(super)` visibility in a sibling module and to keep the BG worker
/// self-contained.
fn run_checkpoint_if_needed(wal: &WalHandle, pager: &PagerHandle) -> crate::error::Result<()> {
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
    if wal.inner.reader_registry.active_reader_count()? > 0 {
        return Ok(());
    }
    super::checkpoint::checkpoint(wal, pager, cfg.checkpoint_timeout_sec)
}
