use std::time::Duration;

use crate::record::value::Value;
use crate::tracing::buffer::{BoundedRingBuffer, BoundedSnapshot};
use crate::tracing::config::RuntimeTracingConfig;
use crate::tracing::next_event_id;

/// Mutable lock-wait event captured after the hot lock is released.
#[derive(Clone, Debug)]
pub struct LockWaitEvent {
    pub event_id: u64,
    pub session_id: u64,
    pub connection_id: u64,
    pub duration_us: u64,
    pub threshold_us: u64,
    pub wait_source: String,
    pub status: String,
    pub database_id_hash: String,
    pub internal: bool,
}

impl LockWaitEvent {
    pub fn to_query_row(&self) -> Vec<Value> {
        vec![
            Value::Int64(i64::try_from(self.event_id).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.session_id).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.connection_id).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.duration_us).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.threshold_us).unwrap_or(-1)),
            Value::Text(self.wait_source.clone()),
            Value::Text(self.status.clone()),
            Value::Text(self.database_id_hash.clone()),
            Value::Bool(self.internal),
        ]
    }
}

#[derive(Debug)]
pub(crate) struct LockWaitStore {
    config: RuntimeTracingConfig,
    buffer: BoundedRingBuffer<LockWaitEvent>,
}

impl LockWaitStore {
    pub(crate) fn new(config: &RuntimeTracingConfig) -> Self {
        let capacity = config.lock_wait.max_events.clamp(1, 16_384);
        Self {
            config: config.clone(),
            buffer: BoundedRingBuffer::with_capacity(capacity),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub(crate) fn maybe_record(
        &mut self,
        duration: Duration,
        session_id: u64,
        connection_id: u64,
        wait_source: &str,
        status: &str,
        database_id_hash: &str,
        internal: bool,
    ) {
        if !self.config.enabled || !self.config.lock_wait.enabled {
            return;
        }
        let threshold = self.config.lock_wait.threshold_us;
        let duration_us = duration.as_micros() as u64;
        // threshold_us == 0 means "record every lock acquisition" (no filtering).
        // This intentionally differs from slow_query where 0 means "disabled".
        if threshold > 0 && duration_us < threshold {
            return;
        }
        self.buffer.push_back(LockWaitEvent {
            event_id: next_event_id(),
            session_id,
            connection_id,
            duration_us,
            threshold_us: threshold,
            wait_source: wait_source.to_string(),
            status: status.to_string(),
            database_id_hash: database_id_hash.to_string(),
            internal,
        });
    }

    pub(crate) fn snapshot(&self) -> BoundedSnapshot<LockWaitEvent> {
        self.buffer.snapshot(|e| e.clone())
    }

    pub(crate) fn reset(&mut self) {
        self.buffer.reset();
    }
}
