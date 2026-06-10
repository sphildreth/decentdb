#![allow(unused_imports)]

use std::time::Duration;

use crate::tracing::buffer::{BoundedRingBuffer, BoundedSnapshot};
use crate::tracing::config::{SlowQueryTraceConfig, SqlTextMode};
use crate::tracing::redact::{redact_sql, sql_fingerprint};
use crate::tracing::{next_event_id, RuntimeTracingConfig};

#[allow(dead_code)]
/// Mutable slow-query event captured at statement end.
#[derive(Clone, Debug)]
pub(crate) struct SlowQueryEventInternal {
    pub event_id: u64,
    pub session_id: u64,
    pub connection_id: u64,
    pub started_at_unix_ms: i64,
    pub duration_us: u64,
    pub threshold_us: u64,
    pub statement_kind: String,
    pub read_only: bool,
    pub sql_fingerprint: String,
    pub sql_template: String,
    pub sql_text_mode: SqlTextMode,
    pub database_id_hash: String,
    pub status: String,
    pub error_code: Option<String>,
    pub internal: bool,
    pub truncated: bool,
}

/// Owned slow-query event for external consumers and SQL rows.
#[derive(Clone, Debug)]
pub struct SlowQueryEvent {
    pub event_id: u64,
    pub session_id: u64,
    pub connection_id: u64,
    pub started_at_unix_ms: i64,
    pub duration_us: u64,
    pub threshold_us: u64,
    pub statement_kind: String,
    pub read_only: bool,
    pub sql_fingerprint: String,
    pub sql_template: String,
    pub sql_text_mode: String,
    pub database_id_hash: String,
    pub status: String,
    pub error_code: Option<String>,
    pub internal: bool,
    pub truncated: bool,
}

impl SlowQueryEvent {
    pub fn to_query_row(&self) -> Vec<crate::record::value::Value> {
        use crate::record::value::Value;
        vec![
            Value::Int64(i64::try_from(self.event_id).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.session_id).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.connection_id).unwrap_or(-1)),
            Value::Int64(self.started_at_unix_ms),
            Value::Int64(i64::try_from(self.duration_us).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.threshold_us).unwrap_or(-1)),
            Value::Text(self.statement_kind.clone()),
            Value::Bool(self.read_only),
            Value::Text(self.sql_fingerprint.clone()),
            if self.sql_template.is_empty() {
                Value::Null
            } else {
                Value::Text(self.sql_template.clone())
            },
            Value::Text(self.sql_text_mode.clone()),
            Value::Text(self.database_id_hash.clone()),
            Value::Text(self.status.clone()),
            self.error_code
                .as_ref()
                .map_or(Value::Null, |e| Value::Text(e.clone())),
            Value::Bool(self.internal),
            Value::Bool(self.truncated),
        ]
    }
}

impl From<SlowQueryEventInternal> for SlowQueryEvent {
    fn from(e: SlowQueryEventInternal) -> Self {
        Self {
            event_id: e.event_id,
            session_id: e.session_id,
            connection_id: e.connection_id,
            started_at_unix_ms: e.started_at_unix_ms,
            duration_us: e.duration_us,
            threshold_us: e.threshold_us,
            statement_kind: e.statement_kind,
            read_only: e.read_only,
            sql_fingerprint: e.sql_fingerprint,
            sql_template: e.sql_template,
            sql_text_mode: format!("{:?}", e.sql_text_mode).to_ascii_lowercase(),
            database_id_hash: e.database_id_hash,
            status: e.status,
            error_code: e.error_code,
            internal: e.internal,
            truncated: e.truncated,
        }
    }
}

/// In-memory slow-query trace store.
#[derive(Debug)]
pub(crate) struct SlowQueryStore {
    config: RuntimeTracingConfig,
    buffer: BoundedRingBuffer<SlowQueryEventInternal>,
}

impl SlowQueryStore {
    pub(crate) fn new(config: &RuntimeTracingConfig) -> Self {
        let capacity = config.slow_query.max_events.clamp(1, 16384);
        Self {
            config: config.clone(),
            buffer: BoundedRingBuffer::with_capacity(capacity),
        }
    }

    /// Record a slow-query event if the duration crosses the threshold.
    ///
    /// Must be called *after* the statement completes. Does not allocate when
    /// tracing is disabled or the event is below threshold.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn maybe_record(
        &mut self,
        duration: Duration,
        started_at_unix_ms: i64,
        session_id: u64,
        connection_id: u64,
        statement_kind: &str,
        read_only: bool,
        sql: &str,
        status: &str,
        error_code: Option<&str>,
        internal: bool,
        database_id_hash: &str,
    ) {
        if !self.config.enabled || !self.config.slow_query.enabled {
            return;
        }
        let threshold = self.config.slow_query.threshold_us;
        if threshold == 0 {
            return;
        }
        let duration_us = duration.as_micros() as u64;
        if duration_us < threshold {
            return;
        }
        let mode = self.config.slow_query.sql_text_mode;
        let max_chars = self.config.slow_query.max_sql_bytes_per_event;
        let sql_template = redact_sql(sql, mode, max_chars);
        let truncated = sql.chars().count() > max_chars;
        let fingerprint = sql_fingerprint(sql);
        let event = SlowQueryEventInternal {
            event_id: next_event_id(),
            session_id,
            connection_id,
            started_at_unix_ms,
            duration_us,
            threshold_us: threshold,
            statement_kind: statement_kind.to_string(),
            read_only,
            sql_fingerprint: fingerprint,
            sql_template,
            sql_text_mode: mode,
            database_id_hash: database_id_hash.to_string(),
            status: status.to_string(),
            error_code: error_code.map(|s| s.to_string()),
            internal,
            truncated,
        };
        self.buffer.push_back(event);
    }

    #[inline]
    pub(crate) fn snapshot(&self) -> BoundedSnapshot<SlowQueryEvent> {
        self.buffer.snapshot(|e| SlowQueryEvent::from(e.clone()))
    }

    #[inline]
    pub(crate) fn reset(&mut self) {
        self.buffer.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_store_does_not_allocate() {
        let mut store = SlowQueryStore::new(&RuntimeTracingConfig::default());
        store.maybe_record(
            Duration::from_secs(1),
            0,
            1,
            1,
            "SELECT",
            true,
            "SELECT * FROM users WHERE secret = 'password'",
            "ok",
            None,
            false,
            "hash",
        );
        assert!(store.snapshot().items.is_empty());
    }

    #[test]
    fn below_threshold_is_not_recorded() {
        let config = RuntimeTracingConfig {
            enabled: true,
            slow_query: SlowQueryTraceConfig {
                enabled: true,
                threshold_us: 1000,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut store = SlowQueryStore::new(&config);
        store.maybe_record(
            Duration::from_micros(500),
            0,
            1,
            1,
            "SELECT",
            true,
            "SELECT * FROM users",
            "ok",
            None,
            false,
            "hash",
        );
        assert!(store.snapshot().items.is_empty());
    }

    #[test]
    fn slow_query_is_recorded() {
        let config = RuntimeTracingConfig {
            enabled: true,
            slow_query: SlowQueryTraceConfig {
                enabled: true,
                threshold_us: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut store = SlowQueryStore::new(&config);
        store.maybe_record(
            Duration::from_secs(1),
            0,
            1,
            1,
            "SELECT",
            true,
            "SELECT * FROM users WHERE secret = 'password'",
            "ok",
            None,
            false,
            "hash",
        );
        let snap = store.snapshot();
        assert_eq!(snap.items.len(), 1);
        let evt = &snap.items[0];
        assert_eq!(evt.status, "ok");
        // Phase 1 fingerprint is lowercase SQL (does not strip literals).
        assert!(evt.sql_fingerprint.contains("password"));
        assert!(evt.sql_template.is_empty()); // None mode default
    }
}
