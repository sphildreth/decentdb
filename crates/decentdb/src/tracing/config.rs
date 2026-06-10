/// Whether and how SQL text is captured in trace events.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SqlTextMode {
    /// No SQL text or template; only fingerprint and metadata.
    #[default]
    None,
    /// Parser-derived template with literals removed or replaced.
    Template,
    /// Truncated SQL shape with literal redaction.
    Redacted,
    /// Raw SQL text; explicit debug-only opt-in.
    Full,
}

/// Per-family slow-query tracing controls.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlowQueryTraceConfig {
    pub enabled: bool,
    /// Threshold in microseconds. `0` means disabled even if `enabled = true`.
    pub threshold_us: u64,
    pub max_events: usize,
    pub sql_text_mode: SqlTextMode,
    pub max_sql_bytes_per_event: usize,
    pub max_object_name_bytes_per_event: usize,
}

impl Default for SlowQueryTraceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            threshold_us: 0,
            max_events: 256,
            sql_text_mode: SqlTextMode::None,
            max_sql_bytes_per_event: 512,
            max_object_name_bytes_per_event: 256,
        }
    }
}

/// Per-family lock-wait tracing controls.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockWaitTraceConfig {
    pub enabled: bool,
    /// Threshold in microseconds.
    pub threshold_us: u64,
    pub max_events: usize,
}

impl Default for LockWaitTraceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            threshold_us: 0,
            max_events: 512,
        }
    }
}

/// Per-family index-usage tracing controls.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexUsageTraceConfig {
    pub enabled: bool,
    pub max_rows: usize,
}

impl Default for IndexUsageTraceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_rows: 1024,
        }
    }
}

/// Per-family session tracing controls.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTraceConfig {
    pub enabled: bool,
    pub max_recent_sessions: usize,
}

impl Default for SessionTraceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_recent_sessions: 256,
        }
    }
}

/// Top-level runtime tracing configuration applied at open time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeTracingConfig {
    pub enabled: bool,
    pub slow_query: SlowQueryTraceConfig,
    pub lock_wait: LockWaitTraceConfig,
    pub index_usage: IndexUsageTraceConfig,
    pub sessions: SessionTraceConfig,
    pub sql_text: SqlTextMode,
    /// Total memory budget across all trace buffers.
    pub memory_budget_bytes: usize,
}

impl Default for RuntimeTracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            slow_query: SlowQueryTraceConfig::default(),
            lock_wait: LockWaitTraceConfig::default(),
            index_usage: IndexUsageTraceConfig::default(),
            sessions: SessionTraceConfig::default(),
            sql_text: SqlTextMode::None,
            memory_budget_bytes: 2 * 1024 * 1024,
        }
    }
}

impl RuntimeTracingConfig {
    /// True if any event family is enabled.
    #[inline]
    pub fn any_enabled(&self) -> bool {
        self.enabled
            && (self.slow_query.enabled
                || self.lock_wait.enabled
                || self.index_usage.enabled
                || self.sessions.enabled)
    }
}
