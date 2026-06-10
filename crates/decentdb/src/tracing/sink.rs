use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use crate::tracing::config::RuntimeTracingConfig;
use crate::tracing::index_usage::IndexUsageStore;
use crate::tracing::lock_wait::LockWaitStore;
use crate::tracing::sessions::{
    RecentSessionBuffer, SessionSnapshot, SessionState, SessionTracker,
};
use crate::tracing::slow_query::SlowQueryStore;

#[allow(dead_code)]
/// Mutable runtime trace state owned by `DbInner`.
///
/// All fields are behind `Mutex` so snapshotting can take short-lived locks.
/// The disabled-sink path avoids ever constructing events.
#[derive(Debug)]
pub struct RuntimeTraceState {
    pub(crate) config: RuntimeTracingConfig,
    pub(crate) connection_id: u64,
    pub(crate) database_id_hash: String,
    pub(crate) slow_query_store: Mutex<SlowQueryStore>,
    pub(crate) lock_wait_store: Mutex<LockWaitStore>,
    pub(crate) index_usage_store: Mutex<IndexUsageStore>,
    pub(crate) session_tracker: Mutex<SessionTracker>,
    pub(crate) recent_sessions: Mutex<RecentSessionBuffer>,
    pub(crate) slow_query_counter: AtomicU64,
}

impl RuntimeTraceState {
    pub fn new(
        config: &RuntimeTracingConfig,
        connection_id: u64,
        database_id_hash: String,
    ) -> Self {
        Self {
            config: config.clone(),
            connection_id,
            database_id_hash: database_id_hash.clone(),
            slow_query_store: Mutex::new(SlowQueryStore::new(config)),
            lock_wait_store: Mutex::new(LockWaitStore::new(config)),
            index_usage_store: Mutex::new(IndexUsageStore::new(config)),
            session_tracker: Mutex::new(SessionTracker::new(
                connection_id,
                connection_id,
                database_id_hash.clone(),
                config.any_enabled(),
                if config.slow_query.enabled {
                    Some(config.slow_query.threshold_us)
                } else {
                    None
                },
            )),
            recent_sessions: Mutex::new(RecentSessionBuffer::with_capacity(
                config.sessions.max_recent_sessions.clamp(1, 16384),
            )),
            slow_query_counter: AtomicU64::new(0),
        }
    }

    /// True if *any* trace family is enabled.
    #[inline]
    pub fn any_enabled(&self) -> bool {
        self.config.any_enabled()
    }

    /// Record a slow-query event if thresholds permit.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn record_slow_query(
        &self,
        duration: Duration,
        started_at_unix_ms: i64,
        statement_kind: &str,
        read_only: bool,
        sql: &str,
        status: &str,
        error_code: Option<&str>,
        internal: bool,
    ) {
        if let Ok(mut store) = self.slow_query_store.lock() {
            store.maybe_record(
                duration,
                started_at_unix_ms,
                self.session_tracker
                    .lock()
                    .map(|t| t.session_id())
                    .unwrap_or(0),
                self.connection_id,
                statement_kind,
                read_only,
                sql,
                status,
                error_code,
                internal,
                &self.database_id_hash,
            );
        }
        self.slow_query_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot current sessions.
    pub fn sessions_snapshot(&self) -> Vec<SessionSnapshot> {
        let mut out = Vec::new();
        if let Ok(tracker) = self.session_tracker.lock() {
            out.push(tracker.snapshot(self.database_id_hash.clone()));
        }
        if let Ok(recent) = self.recent_sessions.lock() {
            for s in recent.snapshot() {
                // avoid duplicating the live session
                if out
                    .first()
                    .map(|live| live.session_id != s.session_id)
                    .unwrap_or(true)
                {
                    out.push(s);
                }
            }
        }
        out
    }

    /// Snapshot slow queries.
    pub fn slow_queries_snapshot(
        &self,
    ) -> crate::tracing::buffer::BoundedSnapshot<crate::tracing::slow_query::SlowQueryEvent> {
        self.slow_query_store
            .lock()
            .map(|store| store.snapshot())
            .unwrap_or_else(|_| crate::tracing::buffer::BoundedSnapshot {
                items: Vec::new(),
                eviction_count: 0,
                newest_event_id: 0,
                oldest_event_id: 0,
            })
    }

    /// Reset by family.
    #[allow(dead_code)]
    pub fn reset(&self, family: crate::tracing::events::RuntimeTraceFamily) {
        match family {
            crate::tracing::events::RuntimeTraceFamily::Statement => {
                if let Ok(mut store) = self.slow_query_store.lock() {
                    store.reset();
                }
            }
            crate::tracing::events::RuntimeTraceFamily::Session => {
                if let Ok(mut buf) = self.recent_sessions.lock() {
                    buf.reset();
                }
            }
            _ => {}
        }
    }

    /// Mark session as entering a transaction.
    pub fn mark_in_transaction(&self) {
        if let Ok(mut tracker) = self.session_tracker.lock() {
            tracker.set_state(SessionState::InTransaction);
        }
    }

    /// Mark session as active (outside transaction).
    pub fn mark_active(&self) {
        if let Ok(mut tracker) = self.session_tracker.lock() {
            tracker.set_state(SessionState::Active);
        }
    }

    /// Record a lock-wait event if thresholds permit.
    #[inline]
    pub fn record_lock_wait(
        &self,
        duration: Duration,
        source: &str,
        status: &str,
        internal: bool,
    ) {
        if let Ok(mut store) = self.lock_wait_store.lock() {
            store.maybe_record(
                duration,
                self.session_tracker
                    .lock()
                    .map(|t| t.session_id())
                    .unwrap_or(0),
                self.connection_id,
                source,
                status,
                &self.database_id_hash,
                internal,
            );
        }
    }

    /// Snapshot lock waits.
    pub fn lock_waits_snapshot(
        &self,
    ) -> crate::tracing::buffer::BoundedSnapshot<crate::tracing::lock_wait::LockWaitEvent> {
        self.lock_wait_store
            .lock()
            .map(|store| store.snapshot())
            .unwrap_or_else(|_| crate::tracing::buffer::BoundedSnapshot {
                items: Vec::new(),
                eviction_count: 0,
                newest_event_id: 0,
                oldest_event_id: 0,
            })
    }

    /// Record an index-usage aggregate event.
    #[inline]
    pub fn record_index_usage(
        &self,
        table_name: &str,
        index_name: &str,
        index_kind: &str,
        kind: crate::tracing::index_usage::IndexUsageKind,
    ) {
        if let Ok(store) = self.index_usage_store.lock() {
            store.record(table_name, index_name, index_kind, kind);
        }
    }

    /// Snapshot index usage rows.
    pub fn index_usage_snapshot(&self,
    ) -> Vec<crate::tracing::index_usage::IndexUsageRow> {
        self.index_usage_store
            .lock()
            .map(|store| store.snapshot())
            .unwrap_or_default()
    }

    /// Mark session closed and move to recent buffer.
    #[allow(dead_code)]
    pub fn mark_closed(&self) {
        let snapshot = self
            .session_tracker
            .lock()
            .map(|mut t| {
                t.mark_closed();
                t.snapshot(self.database_id_hash.clone())
            })
            .ok();
        if let Some(s) = snapshot {
            if let Ok(mut buf) = self.recent_sessions.lock() {
                buf.push(s);
            }
        }
    }
}

/// Trait used by capture sites so tests can stub behavior.
#[allow(dead_code)]
pub trait RuntimeTraceSink: Send + Sync {
    fn enabled(&self, family: crate::tracing::events::RuntimeTraceFamily) -> bool;
    #[allow(clippy::too_many_arguments)]
    fn record_slow_query(
        &self,
        duration: Duration,
        started_at_unix_ms: i64,
        statement_kind: &str,
        read_only: bool,
        sql: &str,
        status: &str,
        error_code: Option<&str>,
        internal: bool,
    );
    fn sessions_snapshot(&self) -> Vec<SessionSnapshot>;
    fn slow_queries_snapshot(
        &self,
    ) -> crate::tracing::buffer::BoundedSnapshot<crate::tracing::slow_query::SlowQueryEvent>;
    fn reset(&self, family: crate::tracing::events::RuntimeTraceFamily);
}

impl RuntimeTraceSink for RuntimeTraceState {
    fn enabled(&self, family: crate::tracing::events::RuntimeTraceFamily) -> bool {
        if !self.config.enabled {
            return false;
        }
        match family {
            crate::tracing::events::RuntimeTraceFamily::Statement => self.config.slow_query.enabled,
            _ => false,
        }
    }

    fn record_slow_query(
        &self,
        duration: Duration,
        started_at_unix_ms: i64,
        statement_kind: &str,
        read_only: bool,
        sql: &str,
        status: &str,
        error_code: Option<&str>,
        internal: bool,
    ) {
        self.record_slow_query(
            duration,
            started_at_unix_ms,
            statement_kind,
            read_only,
            sql,
            status,
            error_code,
            internal,
        )
    }

    fn sessions_snapshot(&self) -> Vec<SessionSnapshot> {
        self.sessions_snapshot()
    }

    fn slow_queries_snapshot(
        &self,
    ) -> crate::tracing::buffer::BoundedSnapshot<crate::tracing::slow_query::SlowQueryEvent> {
        self.slow_queries_snapshot()
    }

    fn reset(&self, family: crate::tracing::events::RuntimeTraceFamily) {
        self.reset(family)
    }
}
