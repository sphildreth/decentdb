#![allow(dead_code)]

use crate::record::value::Value;
use crate::tracing::unix_millis_now;
/// Lifecycle state of a session/connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Active,
    InTransaction,
    WaitingForWrite,
    Closed,
}

/// A single session row snapshot.
#[derive(Clone, Debug)]
pub struct SessionSnapshot {
    pub session_id: u64,
    pub connection_id: u64,
    pub database_id_hash: String,
    pub opened_at_unix_ms: i64,
    pub closed_at_unix_ms: Option<i64>,
    pub state: SessionState,
    pub binding: Option<String>,
    pub tracing_enabled: bool,
    pub slow_query_threshold_us: Option<u64>,
    pub internal: bool,
}

impl SessionSnapshot {
    pub fn to_query_row(&self) -> Vec<Value> {
        vec![
            Value::Int64(i64::try_from(self.session_id).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.connection_id).unwrap_or(-1)),
            Value::Text(self.database_id_hash.clone()),
            Value::Int64(self.opened_at_unix_ms),
            self.closed_at_unix_ms.map_or(Value::Null, Value::Int64),
            Value::Text(match self.state {
                SessionState::Active => "active".to_string(),
                SessionState::InTransaction => "in_transaction".to_string(),
                SessionState::WaitingForWrite => "waiting_for_write".to_string(),
                SessionState::Closed => "closed".to_string(),
            }),
            self.binding
                .as_ref()
                .map_or(Value::Null, |b| Value::Text(b.clone())),
            Value::Bool(self.tracing_enabled),
            self.slow_query_threshold_us.map_or(Value::Null, |v| {
                Value::Int64(i64::try_from(v).unwrap_or(-1))
            }),
            Value::Bool(self.internal),
        ]
    }
}

/// Mutable session tracking state per Db handle.
#[derive(Debug)]
pub(crate) struct SessionTracker {
    session_id: u64,
    connection_id: u64,
    opened_at_unix_ms: i64,
    state: SessionState,
    tracing_enabled: bool,
    slow_query_threshold_us: Option<u64>,
    internal: bool,
}

impl SessionTracker {
    #[inline]
    pub(crate) fn session_id(&self) -> u64 {
        self.session_id
    }

    pub(crate) fn new(
        session_id: u64,
        connection_id: u64,
        _database_id_hash: String,
        tracing_enabled: bool,
        slow_query_threshold_us: Option<u64>,
    ) -> Self {
        let now = unix_millis_now();
        Self {
            session_id,
            connection_id,
            opened_at_unix_ms: now,
            state: SessionState::Active,
            tracing_enabled,
            slow_query_threshold_us,
            internal: false,
        }
    }

    pub(crate) fn snapshot(&self, database_id_hash: String) -> SessionSnapshot {
        SessionSnapshot {
            session_id: self.session_id,
            connection_id: self.connection_id,
            database_id_hash,
            opened_at_unix_ms: self.opened_at_unix_ms,
            closed_at_unix_ms: None,
            state: self.state,
            binding: None,
            tracing_enabled: self.tracing_enabled,
            slow_query_threshold_us: self.slow_query_threshold_us,
            internal: self.internal,
        }
    }

    pub(crate) fn set_state(&mut self, state: SessionState) {
        self.state = state;
    }

    #[allow(dead_code)]
    pub(crate) fn mark_closed(&mut self) {
        self.state = SessionState::Closed;
    }
}

/// Recent-session ring buffer kept in the shared trace state.
#[derive(Debug)]
pub(crate) struct RecentSessionBuffer {
    capacity: usize,
    sessions: Vec<SessionSnapshot>,
}

impl RecentSessionBuffer {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            sessions: Vec::with_capacity(capacity.min(16)),
        }
    }

    pub(crate) fn push(&mut self, session: SessionSnapshot) {
        if self.capacity == 0 {
            return;
        }
        if self.sessions.len() >= self.capacity {
            self.sessions.remove(0);
        }
        self.sessions.push(session);
    }

    pub(crate) fn snapshot(&self) -> Vec<SessionSnapshot> {
        self.sessions.clone()
    }

    pub(crate) fn reset(&mut self) {
        self.sessions.clear();
    }
}
