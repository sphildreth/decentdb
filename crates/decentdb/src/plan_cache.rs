//! Connection-local plan cache for parsed SQL statements and reusable
//! prepared plan objects.
//!
//! This module is governed by ADRs 0190-0193 and the parent spec
//! `design/_archive/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md`. It implements
//! the Phase 1A slice: a generalized parsed-statement cache keyed by
//! `(sql_text, parameter_shape, persistent_schema_cookie, temp_schema_cookie,
//! policy_mask_generation)` with size-bounded LRU eviction, oversized-entry
//! refusal, and a single `PlanCacheInvalidator` sink for DDL, temp-schema,
//! policy/mask, branch, extension, and explicit-flush events.
//!
//! Audit-context writes deliberately do **not** invalidate the cache. Per
//! ADR 0192, the audit context is a *diagnostic observable* that does not
//! affect plan shape and must not evict cached plans.

// The module exposes a public API surface that is consumed through
// `DbConfig::with_plan_cache`, `Db::plan_cache_summary`, and the C ABI
// `ddb_plan_cache_summary` / `ddb_plan_cache_flush` accessors. Items
// may appear unused from a single-binary perspective when the host
// application does not exercise a particular access path.
#![allow(dead_code, private_interfaces)]

use std::collections::{HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[cfg(all(not(miri), not(all(target_arch = "wasm32", target_os = "unknown"))))]
use std::time::{SystemTime, UNIX_EPOCH};

use crate::sql::ast::Statement as SqlStatement;

/// Default connection-local plan cache size in bytes (256 KiB).
///
/// See ADR 0191 for the rationale: small enough to fit inside the
/// default-fast performance profile's 4 MiB page-cache budget while
/// large enough to hold a representative set of parsed statements and
/// `PreparedSimple*` projection plans.
pub const PLAN_CACHE_DEFAULT_MAX_BYTES: u64 = 256 * 1024;

/// Conservative per-entry fixed overhead constant, in bytes.
///
/// Includes the `HashMap` entry overhead (key + value pointer + hash),
/// the `VecDeque` slot, the cached `Arc<SqlStatement>` strong-count slot,
/// the cache-key hash buffer, the SQL text (length-prefixed, exact
/// bytes), the parameter shape vector (arity + per-placeholder type tag),
/// and the per-entry metadata (`hit_count`, `last_used_at`,
/// `plan_size_bytes`).
pub const PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES: usize = 192;

const PLAN_CACHE_PARSED_ADMISSION_WINDOW: usize = 1024;

/// Plan cache configuration. Thread-safe to clone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanCacheConfig {
    pub enabled: bool,
    pub max_size_bytes: u64,
}

impl Default for PlanCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size_bytes: PLAN_CACHE_DEFAULT_MAX_BYTES,
        }
    }
}

/// SQL type class for a single parameter placeholder.
///
/// This is the column affinity / declared parameter type, never the
/// runtime value. Two queries whose placeholders resolve to the same
/// type class share a cache entry.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[allow(dead_code)]
pub enum ParameterTypeClass {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}

impl ParameterTypeClass {
    /// Classify a runtime `Value` into a parameter type class. Used by
    /// prepared-statement key construction; the *declared* type class is
    /// preferred when available, but runtime classification is the
    /// conservative default for `?` placeholders without explicit casts.
    #[allow(dead_code)]
    pub fn from_value(value: &crate::Value) -> Self {
        use crate::Value;
        match value {
            Value::Null => Self::Null,
            Value::Int64(_)
            | Value::Bool(_)
            | Value::Uuid(_)
            | Value::TimestampMicros(_)
            | Value::TimestampTzMicros(_)
            | Value::TimeMicros(_)
            | Value::DateDays(_)
            | Value::Enum { .. } => Self::Integer,
            Value::Float64(_) | Value::Decimal { .. } => Self::Real,
            Value::Text(_) => Self::Text,
            Value::Blob(_)
            | Value::Geometry(_)
            | Value::Geography(_)
            | Value::IpAddr { .. }
            | Value::Cidr { .. }
            | Value::MacAddr { .. }
            | Value::Interval { .. } => Self::Blob,
        }
    }
}

/// Parameter shape vector: arity plus the per-placeholder type class.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ParameterShape {
    pub classes: Arc<[ParameterTypeClass]>,
}

impl ParameterShape {
    pub fn empty() -> Self {
        Self {
            classes: Arc::new([]),
        }
    }

    pub fn unknown_with_arity(arity: usize) -> Self {
        Self {
            classes: vec![ParameterTypeClass::Null; arity].into(),
        }
    }

    #[allow(dead_code)]
    pub fn from_values(values: &[crate::Value]) -> Self {
        Self {
            classes: Arc::from_iter(values.iter().map(ParameterTypeClass::from_value)),
        }
    }

    #[allow(dead_code)]
    pub fn arity(&self) -> usize {
        self.classes.len()
    }
}

/// Plan cache key.
///
/// Per ADR 0190, the key is the tuple
/// `(sql_text, parameter_shape, persistent_schema_cookie, temp_schema_cookie,
/// policy_mask_generation)`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PlanCacheKey {
    pub sql_text: String,
    pub parameter_shape: ParameterShape,
    pub persistent_schema_cookie: u32,
    pub temp_schema_cookie: u32,
    pub policy_mask_generation: u32,
}

impl PlanCacheKey {
    pub fn new(
        sql_text: String,
        parameter_shape: ParameterShape,
        persistent_schema_cookie: u32,
        temp_schema_cookie: u32,
        policy_mask_generation: u32,
    ) -> Self {
        Self {
            sql_text,
            parameter_shape,
            persistent_schema_cookie,
            temp_schema_cookie,
            policy_mask_generation,
        }
    }

    /// Stable hash for `sys.plan_cache.cache_key_hash`. The hash is
    /// deliberately not portable across engine versions; the value is
    /// for diagnostic matching only and must not be persisted.
    pub fn stable_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.sql_text.hash(&mut hasher);
        self.parameter_shape.classes.hash(&mut hasher);
        self.persistent_schema_cookie.hash(&mut hasher);
        self.temp_schema_cookie.hash(&mut hasher);
        self.policy_mask_generation.hash(&mut hasher);
        hasher.finish()
    }
}

/// Cached plan payload: Phase 1A stores parsed statements only.
#[derive(Clone, Debug)]
pub struct PlanCacheEntry {
    pub key_hash: u64,
    pub sql_text: String,
    pub statement: Arc<SqlStatement>,
    pub plan_size_bytes: u64,
    pub persistent_schema_cookie: u32,
    pub temp_schema_cookie: u32,
    pub policy_mask_generation: u32,
    pub hit_count: u64,
    pub last_used_at_micros: i64,
    pub statement_category: StatementCategory,
}

/// Closed enum of statement categories for `sys.plan_cache.statement_category`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementCategory {
    Select,
    Insert,
    Update,
    Delete,
    Pragma,
    Explain,
    Set,
    Other,
}

impl StatementCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Pragma => "PRAGMA",
            Self::Explain => "EXPLAIN",
            Self::Set => "SET",
            Self::Other => "OTHER",
        }
    }

    pub fn classify(stmt: &SqlStatement) -> Self {
        match stmt {
            SqlStatement::Query(_) => Self::Select,
            SqlStatement::Insert(_) => Self::Insert,
            SqlStatement::Update(_) => Self::Update,
            SqlStatement::Delete(_) => Self::Delete,
            SqlStatement::Explain(_) => Self::Explain,
            SqlStatement::Analyze { .. } => Self::Pragma,
            SqlStatement::CreateSchema { .. }
            | SqlStatement::CreateTable(_)
            | SqlStatement::CreateTableAs(_)
            | SqlStatement::CreateIndex(_)
            | SqlStatement::CreateView(_)
            | SqlStatement::CreateTrigger(_) => Self::Other,
            SqlStatement::DropTable { .. }
            | SqlStatement::DropIndex { .. }
            | SqlStatement::DropView { .. }
            | SqlStatement::DropTrigger { .. }
            | SqlStatement::AlterTable { .. }
            | SqlStatement::AlterIndexRebuild { .. }
            | SqlStatement::AlterIndexVerify { .. }
            | SqlStatement::AlterViewRename { .. }
            | SqlStatement::TruncateTable { .. } => Self::Other,
        }
    }
}

/// Per-Db handle plan cache state.
#[derive(Debug)]
pub struct PlanCache {
    enabled: bool,
    max_size_bytes: u64,
    current_size_bytes: u64,
    entries: std::collections::HashMap<PlanCacheKey, PlanCacheEntry>,
    order: VecDeque<PlanCacheKey>,
    total_hits: u64,
    total_misses: u64,
    total_evictions: u64,
    total_oversized_refusals: u64,
    recent_miss_order: VecDeque<PlanCacheKey>,
    recent_miss_set: HashSet<PlanCacheKey>,
}

impl PlanCache {
    pub fn new(config: &PlanCacheConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_size_bytes: config.max_size_bytes,
            current_size_bytes: 0,
            entries: std::collections::HashMap::new(),
            order: VecDeque::new(),
            total_hits: 0,
            total_misses: 0,
            total_evictions: 0,
            total_oversized_refusals: 0,
            recent_miss_order: VecDeque::new(),
            recent_miss_set: HashSet::new(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Return true when a parsed-statement miss should be admitted.
    ///
    /// Parsed ASTs use second-use admission so a stream of unique SQL does
    /// not churn the LRU with entries that have no observed reuse. Literal
    /// zero-parameter execution is left to the existing narrow parser cache;
    /// prepared plans use a separate cache and still admit on first miss.
    pub fn should_admit_missed_key(&mut self, key: &PlanCacheKey) -> bool {
        if !self.enabled {
            return false;
        }
        if key.parameter_shape.arity() == 0 {
            return false;
        }
        if self.recent_miss_set.remove(key) {
            self.recent_miss_order.retain(|candidate| candidate != key);
            return true;
        }
        self.recent_miss_set.insert(key.clone());
        self.recent_miss_order.push_back(key.clone());
        while self.recent_miss_order.len() > PLAN_CACHE_PARSED_ADMISSION_WINDOW {
            if let Some(oldest) = self.recent_miss_order.pop_front() {
                self.recent_miss_set.remove(&oldest);
            }
        }
        false
    }

    /// Look up a cached entry by key. Validates that the key's cookies
    /// still match the current connection state.
    pub fn get(
        &mut self,
        key: &PlanCacheKey,
        current_persistent_cookie: u32,
        current_temp_cookie: u32,
        current_policy_mask_generation: u32,
    ) -> Option<Arc<SqlStatement>> {
        if !self.enabled {
            return None;
        }
        let entry = match self.entries.get(key) {
            Some(e) => e,
            None => {
                self.total_misses += 1;
                return None;
            }
        };
        if entry.persistent_schema_cookie != current_persistent_cookie
            || entry.temp_schema_cookie != current_temp_cookie
            || entry.policy_mask_generation != current_policy_mask_generation
        {
            let _ = entry;
            self.evict_key(key);
            self.total_misses += 1;
            return None;
        }
        let statement = Arc::clone(&entry.statement);
        let _ = entry;
        self.promote(key);
        self.total_hits += 1;
        if let Some(entry) = self.entries.get_mut(key) {
            entry.hit_count = entry.hit_count.saturating_add(1);
            entry.last_used_at_micros = current_time_micros();
        }
        Some(statement)
    }

    /// Insert a parsed statement into the cache, evicting LRU entries
    /// if necessary to fit. If the entry's reported size exceeds the
    /// configured `max_size_bytes`, the entry is refused and counted
    /// in `total_oversized_refusals`.
    pub fn insert(
        &mut self,
        key: PlanCacheKey,
        statement: Arc<SqlStatement>,
        plan_size_bytes: u64,
    ) {
        if !self.enabled {
            return;
        }
        if plan_size_bytes.saturating_add(PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES as u64)
            > self.max_size_bytes
        {
            self.total_oversized_refusals += 1;
            return;
        }
        if self.entries.contains_key(&key) {
            if let Some(prev) = self.entries.remove(&key) {
                self.current_size_bytes =
                    self.current_size_bytes.saturating_sub(prev.plan_size_bytes);
            }
            self.order.retain(|k| k != &key);
        }
        let key_hash = key.stable_hash();
        let entry = PlanCacheEntry {
            key_hash,
            sql_text: key.sql_text.clone(),
            statement: Arc::clone(&statement),
            plan_size_bytes,
            persistent_schema_cookie: key.persistent_schema_cookie,
            temp_schema_cookie: key.temp_schema_cookie,
            policy_mask_generation: key.policy_mask_generation,
            hit_count: 0,
            last_used_at_micros: current_time_micros(),
            statement_category: StatementCategory::classify(&statement),
        };
        self.entries.insert(key.clone(), entry);
        self.order.push_back(key);
        self.current_size_bytes = self
            .current_size_bytes
            .saturating_add(plan_size_bytes)
            .saturating_add(PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES as u64);
        self.evict_to_fit(self.max_size_bytes);
    }

    /// Evict all entries while preserving hit/miss/eviction counters.
    pub fn invalidate_all(&mut self) {
        let evicted = self.entries.len() as u64;
        self.total_evictions = self.total_evictions.saturating_add(evicted);
        self.entries.clear();
        self.order.clear();
        self.recent_miss_order.clear();
        self.recent_miss_set.clear();
        self.current_size_bytes = 0;
    }

    /// Evict all entries and reset hit/miss/eviction counters.
    pub fn flush(&mut self) {
        self.invalidate_all();
        self.total_hits = 0;
        self.total_misses = 0;
        self.total_evictions = 0;
        self.total_oversized_refusals = 0;
    }

    /// Snapshot the cache for `sys.plan_cache` and `sys.plan_cache_summary`.
    pub fn snapshot_entries(&self) -> Vec<PlanCacheEntry> {
        let mut out: Vec<PlanCacheEntry> = self.entries.values().cloned().collect();
        out.sort_by_key(|e| e.key_hash);
        out
    }

    pub fn summary(&self) -> PlanCacheSummary {
        let total_hits = self.total_hits;
        let total_misses = self.total_misses;
        let total = total_hits.saturating_add(total_misses);
        let hit_rate = if total == 0 {
            0.0
        } else {
            (total_hits as f64) * 100.0 / (total as f64)
        };
        PlanCacheSummary {
            scope: "connection",
            total_entries: self.entries.len() as u64,
            total_hits,
            total_misses,
            total_evictions: self.total_evictions,
            total_size_bytes: self.current_size_bytes,
            max_size_bytes: self.max_size_bytes,
            total_oversized_refusals: self.total_oversized_refusals,
            hit_rate,
        }
    }

    fn promote(&mut self, key: &PlanCacheKey) {
        self.order.retain(|k| k != key);
        self.order.push_back(key.clone());
    }

    fn evict_key(&mut self, key: &PlanCacheKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.current_size_bytes = self
                .current_size_bytes
                .saturating_sub(entry.plan_size_bytes)
                .saturating_sub(PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES as u64);
            self.total_evictions = self.total_evictions.saturating_add(1);
        }
        self.order.retain(|k| k != key);
    }

    fn evict_to_fit(&mut self, target_size: u64) {
        while self.current_size_bytes > target_size {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&oldest) {
                self.current_size_bytes = self
                    .current_size_bytes
                    .saturating_sub(entry.plan_size_bytes)
                    .saturating_sub(PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES as u64);
                self.total_evictions = self.total_evictions.saturating_add(1);
            }
        }
    }
}

/// Snapshot of `PlanCache` summary fields for `sys.plan_cache_summary`.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanCacheSummary {
    pub scope: &'static str,
    pub total_entries: u64,
    pub total_hits: u64,
    pub total_misses: u64,
    pub total_evictions: u64,
    pub total_size_bytes: u64,
    pub max_size_bytes: u64,
    pub total_oversized_refusals: u64,
    pub hit_rate: f64,
}

/// Internal invalidation sink.
pub trait PlanCacheInvalidator: Send {
    fn on_persistent_ddl(&self);
    fn on_temp_schema_change(&self);
    fn on_analyze(&self, _table: &str) {
        self.on_persistent_ddl();
    }
    fn on_policy_mask_change(&self);
    fn on_branch_switch(&self);
    fn on_extension_change(&self);
    fn on_explicit_flush(&self);
}

/// Helper for computing the byte size of a parsed `SqlStatement` for
/// cache accounting. Per ADR 0191 the goal is a predictable bound, not
/// a `malloc_info`-grade report.
pub fn statement_accounted_size(stmt: &SqlStatement) -> u64 {
    let raw = std::mem::size_of::<SqlStatement>() as u64;
    let per_stmt: u64 = match stmt {
        SqlStatement::Query(_) => 512,
        SqlStatement::Explain(_) => 256,
        SqlStatement::Insert(_) => 256,
        SqlStatement::Update(_) => 256,
        SqlStatement::Delete(_) => 128,
        SqlStatement::Analyze { .. } => 64,
        SqlStatement::CreateTable(_) => 384,
        SqlStatement::CreateTableAs(_) => 384,
        SqlStatement::CreateIndex(_) => 256,
        SqlStatement::CreateView(_) => 256,
        SqlStatement::CreateTrigger(_) => 384,
        SqlStatement::CreateSchema { .. } => 64,
        SqlStatement::DropTable { .. } => 64,
        SqlStatement::DropIndex { .. } => 64,
        SqlStatement::DropView { .. } => 64,
        SqlStatement::DropTrigger { .. } => 64,
        SqlStatement::AlterTable { .. } => 256,
        SqlStatement::AlterIndexRebuild { .. } => 64,
        SqlStatement::AlterIndexVerify { .. } => 64,
        SqlStatement::AlterViewRename { .. } => 64,
        SqlStatement::TruncateTable { .. } => 64,
    };
    raw.saturating_add(per_stmt)
}

fn current_time_micros() -> i64 {
    #[cfg(miri)]
    {
        0
    }
    #[cfg(all(not(miri), target_arch = "wasm32", target_os = "unknown"))]
    {
        (js_sys::Date::now() * 1000.0) as i64
    }
    #[cfg(all(not(miri), not(all(target_arch = "wasm32", target_os = "unknown"))))]
    {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }
}

/// Atomic counter that backs the `policy_mask_generation` field on
/// `DbInner`. Bumped on every CREATE/DROP/ALTER POLICY and every
/// projection-mask change (ADR 0192).
#[derive(Debug, Default)]
pub struct PolicyMaskGeneration {
    counter: AtomicU32,
}

impl PolicyMaskGeneration {
    pub fn new(initial: u32) -> Self {
        Self {
            counter: AtomicU32::new(initial),
        }
    }

    pub fn current(&self) -> u32 {
        self.counter.load(Ordering::Acquire)
    }

    pub fn bump(&self) -> u32 {
        self.counter.fetch_add(1, Ordering::AcqRel).wrapping_add(1)
    }
}

/// Extension helper used by the `Db` dispatch invalidator.
pub trait SqlStatementExt {
    /// Returns the optional table name for an `ANALYZE` statement.
    fn table_name_for_analyze(&self) -> Option<&str>;
}

impl SqlStatementExt for SqlStatement {
    fn table_name_for_analyze(&self) -> Option<&str> {
        match self {
            SqlStatement::Analyze { table_name } => table_name.as_deref(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::Statement as SqlStatement;

    fn key(sql: &str, policy_gen: u32) -> PlanCacheKey {
        PlanCacheKey::new(sql.to_string(), ParameterShape::empty(), 1, 1, policy_gen)
    }

    fn parameterized_key(sql: &str, policy_gen: u32) -> PlanCacheKey {
        PlanCacheKey::new(
            sql.to_string(),
            ParameterShape::unknown_with_arity(1),
            1,
            1,
            policy_gen,
        )
    }

    fn dummy_stmt() -> Arc<SqlStatement> {
        Arc::new(SqlStatement::Analyze { table_name: None })
    }

    #[test]
    fn cache_disabled_is_a_noop() {
        let config = PlanCacheConfig {
            enabled: false,
            max_size_bytes: 1024,
        };
        let mut cache = PlanCache::new(&config);
        let k = key("select 1", 0);
        assert!(cache.get(&k, 1, 1, 0).is_none());
        cache.insert(k.clone(), dummy_stmt(), 64);
        assert!(cache.entries.is_empty());
        let s = cache.summary();
        assert_eq!(s.total_entries, 0);
        assert_eq!(s.total_misses, 0);
        assert_eq!(s.total_hits, 0);
    }

    #[test]
    fn parsed_admission_requires_second_miss() {
        let config = PlanCacheConfig {
            enabled: true,
            max_size_bytes: 4096,
        };
        let mut cache = PlanCache::new(&config);
        let k = key("select 1", 0);
        assert!(!cache.should_admit_missed_key(&k));
        assert!(!cache.should_admit_missed_key(&k));
        let k = parameterized_key("select $1", 0);
        assert!(!cache.should_admit_missed_key(&k));
        assert!(cache.should_admit_missed_key(&k));
        assert!(!cache.should_admit_missed_key(&k));
    }

    #[test]
    fn lru_eviction_drops_oldest_first() {
        let config = PlanCacheConfig {
            enabled: true,
            max_size_bytes: PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES as u64 * 2 + 16,
        };
        let mut cache = PlanCache::new(&config);
        let k1 = key("select 1", 0);
        let k2 = key("select 2", 0);
        let k3 = key("select 3", 0);
        cache.insert(k1.clone(), dummy_stmt(), 8);
        cache.insert(k2.clone(), dummy_stmt(), 8);
        let _ = cache.get(&k1, 1, 1, 0);
        cache.insert(k3.clone(), dummy_stmt(), 8);
        assert!(cache.entries.contains_key(&k1));
        assert!(!cache.entries.contains_key(&k2));
        assert!(cache.entries.contains_key(&k3));
        assert!(cache.summary().total_evictions >= 1);
    }

    #[test]
    fn oversized_entry_is_refused() {
        let config = PlanCacheConfig {
            enabled: true,
            max_size_bytes: 256,
        };
        let mut cache = PlanCache::new(&config);
        let k = key("select huge", 0);
        cache.insert(k.clone(), dummy_stmt(), 10_000);
        assert!(!cache.entries.contains_key(&k));
        assert_eq!(cache.summary().total_oversized_refusals, 1);
    }

    #[test]
    fn flush_resets_counters_and_entries() {
        let config = PlanCacheConfig {
            enabled: true,
            max_size_bytes: 4096,
        };
        let mut cache = PlanCache::new(&config);
        let k = key("select 1", 0);
        cache.insert(k.clone(), dummy_stmt(), 32);
        let _ = cache.get(&k, 1, 1, 0);
        assert_eq!(cache.summary().total_hits, 1);
        cache.flush();
        assert!(cache.entries.is_empty());
        assert_eq!(cache.summary().total_hits, 0);
    }

    #[test]
    fn policy_generation_bump_invalidates_entries() {
        let config = PlanCacheConfig {
            enabled: true,
            max_size_bytes: 4096,
        };
        let mut cache = PlanCache::new(&config);
        let k = key("select * from t where x = $1", 0);
        cache.insert(k.clone(), dummy_stmt(), 32);
        assert!(cache.get(&k, 1, 1, 1).is_none());
        assert!(!cache.entries.contains_key(&k));
    }

    #[test]
    fn audit_context_round_trip_keeps_cache() {
        let config = PlanCacheConfig {
            enabled: true,
            max_size_bytes: 4096,
        };
        let mut cache = PlanCache::new(&config);
        let k = key("select $1", 0);
        cache.insert(k.clone(), dummy_stmt(), 32);
        let stmt = cache.get(&k, 1, 1, 0);
        assert!(stmt.is_some());
        assert_eq!(cache.summary().total_hits, 1);
    }

    #[test]
    fn summary_hit_rate() {
        let config = PlanCacheConfig {
            enabled: true,
            max_size_bytes: 4096,
        };
        let mut cache = PlanCache::new(&config);
        let k = key("select $1", 0);
        cache.insert(k.clone(), dummy_stmt(), 32);
        let _ = cache.get(&k, 1, 1, 0);
        let _ = cache.get(&k, 1, 1, 0);
        let _ = cache.get(&key("other", 0), 1, 1, 0);
        let s = cache.summary();
        assert_eq!(s.total_hits, 2);
        assert_eq!(s.total_misses, 1);
        let expected = (2.0_f64) * 100.0 / 3.0_f64;
        assert!((s.hit_rate - expected).abs() < 1e-6);
    }

    #[test]
    fn policy_mask_generation_bumps_monotonically() {
        let p = PolicyMaskGeneration::new(0);
        assert_eq!(p.current(), 0);
        assert_eq!(p.bump(), 1);
        assert_eq!(p.bump(), 2);
        assert_eq!(p.current(), 2);
    }

    const _: () = {
        assert!(PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES > 0);
        assert!(PLAN_CACHE_ENTRY_FIXED_OVERHEAD_BYTES < 4096);
    };
}
