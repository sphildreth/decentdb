//! Runtime tracing infrastructure for DecentDB.
//!
//! Implements opt-in bounded in-memory trace history for slow queries, lock
//! waits, index usage, and session lifecycle. Disabled by default; disabled
//! paths must not allocate, normalize SQL, or acquire extra locks.

mod buffer;
pub(crate) mod config;
pub(crate) mod events;
pub(crate) mod advisor;
pub(crate) mod index_usage;
pub(crate) mod lock_wait;
pub(crate) mod redact;
pub(crate) mod sessions;
pub(crate) mod sink;
pub(crate) mod slow_query;

pub use config::RuntimeTracingConfig;
pub use sink::RuntimeTraceState;

use std::sync::atomic::{AtomicU64, Ordering};

static GLOBAL_EVENT_COUNTER: AtomicU64 = AtomicU64::new(1);
static GLOBAL_CONNECTION_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_event_id() -> u64 {
    GLOBAL_EVENT_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn next_connection_id() -> u64 {
    GLOBAL_CONNECTION_COUNTER.fetch_add(1, Ordering::Relaxed)
}
