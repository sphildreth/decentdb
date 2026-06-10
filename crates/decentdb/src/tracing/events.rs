//! Core trace event types shared across families.

#![allow(dead_code)]

/// Identifies which event family a trace belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RuntimeTraceFamily {
    Statement,
    LockWait,
    IndexUsage,
    Session,
    Doctor,
    Advisor,
}

/// Metadata returned with every snapshot.
#[derive(Clone, Debug, Default)]
pub struct RuntimeTraceSnapshotMetadata {
    pub capture_time_unix_ms: i64,
    pub config_enabled: bool,
    pub oldest_event_id: u64,
    pub newest_event_id: u64,
    pub eviction_count: u64,
}
