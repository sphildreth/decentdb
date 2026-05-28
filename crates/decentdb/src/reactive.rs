//! In-process reactive subscriptions and change-stream delivery.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value as JsonValue};

use crate::config::DbConfig;
use crate::error::{DbError, Result};
use crate::exec::QueryResult;
use crate::record::value::{
    format_cidr, format_date_days, format_interval, format_ip_addr, format_mac_addr,
    format_time_micros, format_timestamp_tz_micros, Value,
};

const SCHEMA_VERSION: u32 = 1;
static HUB_REGISTRY: OnceLock<Mutex<BTreeMap<PathBuf, Weak<ReactiveHub>>>> = OnceLock::new();

thread_local! {
    static CHANGE_SOURCE: Cell<ChangeSource> = const { Cell::new(ChangeSource::Direct) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeSource {
    Direct,
    Queued,
    SyncApply,
    BranchMerge,
    BranchRestore,
    Internal,
}

impl ChangeSource {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Queued => "queued",
            Self::SyncApply => "sync_apply",
            Self::BranchMerge => "branch_merge",
            Self::BranchRestore => "branch_restore",
            Self::Internal => "internal",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowOperation {
    Insert,
    Update,
    Delete,
}

impl RowOperation {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowChangeDetail {
    FullRow,
    PrimaryKeyOnly,
    Truncated,
}

impl RowChangeDetail {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullRow => "full_row",
            Self::PrimaryKeyOnly => "primary_key_only",
            Self::Truncated => "truncated",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchKind {
    Table,
    Range,
    Query,
    ChangeStream,
}

impl WatchKind {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Range => "range",
            Self::Query => "query",
            Self::ChangeStream => "change_stream",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowChange {
    pub table: String,
    pub operation: RowOperation,
    pub primary_key: JsonValue,
    pub before: Option<JsonValue>,
    pub after: Option<JsonValue>,
    pub detail: RowChangeDetail,
}

impl RowChange {
    #[must_use]
    pub(crate) fn new(
        table: String,
        operation: RowOperation,
        primary_key: JsonValue,
        before: Option<JsonValue>,
        after: Option<JsonValue>,
    ) -> Self {
        let detail = if after.is_some() || before.is_some() {
            RowChangeDetail::FullRow
        } else {
            RowChangeDetail::PrimaryKeyOnly
        };
        Self {
            table,
            operation,
            primary_key,
            before,
            after,
            detail,
        }
    }

    #[must_use]
    pub fn to_json(&self) -> JsonValue {
        json!({
            "table": self.table,
            "operation": self.operation.as_str(),
            "primary_key": self.primary_key,
            "before": self.before,
            "after": self.after,
            "detail": self.detail.as_str(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableChange {
    pub table: String,
    pub schema_changed: bool,
    pub row_change_count: usize,
}

impl TableChange {
    #[must_use]
    pub fn to_json(&self) -> JsonValue {
        json!({
            "table": self.table,
            "schema_changed": self.schema_changed,
            "row_change_count": self.row_change_count,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InitialWatchEvent {
    pub watch_id: u64,
    pub snapshot_lsn: u64,
    pub schema_cookie: u32,
    pub result: Option<QueryResult>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InvalidationEvent {
    pub watch_id: u64,
    pub event_id: u64,
    pub commit_lsn: u64,
    pub schema_cookie: u32,
    pub source: ChangeSource,
    pub tables: Vec<String>,
    pub row_changes: Vec<RowChange>,
    pub row_changes_truncated: bool,
    pub schema_changed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeStreamEvent {
    pub stream_id: u64,
    pub event_id: u64,
    pub commit_lsn: u64,
    pub schema_cookie: u32,
    pub source: ChangeSource,
    pub table_changes: Vec<TableChange>,
    pub row_changes: Vec<RowChange>,
    pub row_changes_truncated: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LaggedWatchEvent {
    pub watch_id: u64,
    pub last_seen_event_id: u64,
    pub latest_event_id: u64,
    pub latest_commit_lsn: u64,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WatchEvent {
    Initial(InitialWatchEvent),
    Invalidate(InvalidationEvent),
    Change(ChangeStreamEvent),
    Lagged(LaggedWatchEvent),
    Closed,
}

impl WatchEvent {
    #[must_use]
    pub fn to_json(&self) -> JsonValue {
        match self {
            Self::Initial(event) => {
                let mut root = serde_json::Map::new();
                root.insert("schema_version".to_string(), json!(SCHEMA_VERSION));
                root.insert("type".to_string(), json!("initial"));
                root.insert("watch_id".to_string(), json!(event.watch_id));
                root.insert("snapshot_lsn".to_string(), json!(event.snapshot_lsn));
                root.insert("schema_cookie".to_string(), json!(event.schema_cookie));
                if let Some(result) = &event.result {
                    root.insert("columns".to_string(), json!(result.columns()));
                    root.insert(
                        "rows".to_string(),
                        JsonValue::Array(
                            result
                                .rows()
                                .iter()
                                .map(|row| {
                                    JsonValue::Array(
                                        row.values().iter().map(value_to_json).collect(),
                                    )
                                })
                                .collect(),
                        ),
                    );
                }
                JsonValue::Object(root)
            }
            Self::Invalidate(event) => json!({
                "schema_version": SCHEMA_VERSION,
                "type": "invalidate",
                "watch_id": event.watch_id,
                "event_id": event.event_id,
                "commit_lsn": event.commit_lsn,
                "schema_cookie": event.schema_cookie,
                "source": event.source.as_str(),
                "tables": event.tables,
                "schema_changed": event.schema_changed,
                "row_changes_truncated": event.row_changes_truncated,
                "row_changes": event.row_changes.iter().map(RowChange::to_json).collect::<Vec<_>>(),
            }),
            Self::Change(event) => json!({
                "schema_version": SCHEMA_VERSION,
                "type": "change",
                "stream_id": event.stream_id,
                "event_id": event.event_id,
                "commit_lsn": event.commit_lsn,
                "schema_cookie": event.schema_cookie,
                "source": event.source.as_str(),
                "table_changes": event.table_changes.iter().map(TableChange::to_json).collect::<Vec<_>>(),
                "row_changes_truncated": event.row_changes_truncated,
                "row_changes": event.row_changes.iter().map(RowChange::to_json).collect::<Vec<_>>(),
            }),
            Self::Lagged(event) => json!({
                "schema_version": SCHEMA_VERSION,
                "type": "lagged",
                "watch_id": event.watch_id,
                "last_seen_event_id": event.last_seen_event_id,
                "latest_event_id": event.latest_event_id,
                "latest_commit_lsn": event.latest_commit_lsn,
                "reason": event.reason,
            }),
            Self::Closed => json!({
                "schema_version": SCHEMA_VERSION,
                "type": "closed",
            }),
        }
    }

    #[must_use]
    fn event_id(&self) -> Option<u64> {
        match self {
            Self::Invalidate(event) => Some(event.event_id),
            Self::Change(event) => Some(event.event_id),
            Self::Lagged(event) => Some(event.latest_event_id),
            Self::Initial(_) | Self::Closed => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TableWatchOptions {
    pub tables: Vec<String>,
    pub queue_capacity: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct RangeWatchOptions {
    pub table: String,
    pub lower: Option<JsonValue>,
    pub upper: Option<JsonValue>,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub queue_capacity: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub struct QueryWatchOptions {
    pub queue_capacity: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub struct ChangeStreamOptions {
    pub tables: Vec<String>,
    pub queue_capacity: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReactiveMetricsSnapshot {
    pub active_watch_count: usize,
    pub table_watch_count: usize,
    pub range_watch_count: usize,
    pub query_watch_count: usize,
    pub change_stream_count: usize,
    pub events_published: u64,
    pub events_delivered: u64,
    pub events_dropped: u64,
    pub lagged_watch_count: usize,
    pub row_change_events_truncated: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveSubscriptionSnapshot {
    pub watch_id: u64,
    pub kind: WatchKind,
    pub created_at_micros: i64,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub last_delivered_event_id: u64,
    pub dropped_events: u64,
    pub lagged: bool,
    pub dependencies_json: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingReactiveCommit {
    pub source: ChangeSource,
    pub schema_cookie: u32,
    pub changed_tables: Vec<String>,
    pub row_changes: Vec<RowChange>,
    pub row_changes_truncated: bool,
    pub schema_changed: bool,
}

#[derive(Debug)]
pub(crate) struct ReactiveHub {
    default_queue_capacity: usize,
    max_queue_capacity: usize,
    max_row_changes_per_event: usize,
    state: Mutex<HubState>,
    metrics: HubMetrics,
}

#[derive(Debug, Default)]
struct HubState {
    next_watch_id: u64,
    next_event_id: u64,
    watches: BTreeMap<u64, Weak<WatchInner>>,
}

#[derive(Debug, Default)]
struct HubMetrics {
    active_watch_count: AtomicU64,
    events_published: AtomicU64,
    events_delivered: AtomicU64,
    events_dropped: AtomicU64,
    row_change_events_truncated: AtomicU64,
}

#[derive(Debug)]
struct WatchInner {
    id: u64,
    kind: WatchKind,
    filter: WatchFilter,
    created_at_micros: i64,
    queue_capacity: usize,
    queue: Mutex<WatchQueue>,
    cvar: Condvar,
    closed: AtomicBool,
    dropped_events: AtomicU64,
    last_delivered_event_id: AtomicU64,
}

#[derive(Debug)]
struct WatchQueue {
    events: VecDeque<WatchEvent>,
    lagged: bool,
}

#[derive(Debug)]
enum WatchFilter {
    Tables(BTreeSet<String>),
    Range(PrimaryKeyRange),
    Query(BTreeSet<String>),
    ChangeStream(Option<BTreeSet<String>>),
}

#[derive(Debug)]
struct PrimaryKeyRange {
    table: String,
    lower: Option<JsonValue>,
    upper: Option<JsonValue>,
    lower_inclusive: bool,
    upper_inclusive: bool,
}

#[derive(Debug)]
pub struct WatchHandle {
    hub: Arc<ReactiveHub>,
    inner: Arc<WatchInner>,
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        self.hub.close_watch(self.inner.id);
    }
}

impl WatchHandle {
    #[must_use]
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn try_recv(&self) -> Result<Option<WatchEvent>> {
        self.inner.try_recv()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<Option<WatchEvent>> {
        self.inner.recv_timeout(timeout)
    }

    pub fn close(&self) -> Result<()> {
        self.hub.close_watch(self.inner.id);
        Ok(())
    }

    pub fn next_json_timeout(&self, timeout: Duration) -> Result<Option<String>> {
        self.recv_timeout(timeout)?
            .map(|event| serde_json::to_string(&event.to_json()))
            .transpose()
            .map_err(|error| DbError::internal(format!("failed to serialize watch event: {error}")))
    }
}

impl ReactiveHub {
    #[must_use]
    pub(crate) fn new(
        default_queue_capacity: usize,
        max_queue_capacity: usize,
        max_row_changes_per_event: usize,
    ) -> Self {
        Self {
            default_queue_capacity: default_queue_capacity.max(1),
            max_queue_capacity: max_queue_capacity.max(1),
            max_row_changes_per_event,
            state: Mutex::new(HubState::default()),
            metrics: HubMetrics::default(),
        }
    }

    #[must_use]
    pub(crate) fn has_watchers(&self) -> bool {
        self.metrics.active_watch_count.load(Ordering::Acquire) > 0
    }

    #[must_use]
    pub(crate) fn max_row_changes_per_event(&self) -> usize {
        self.max_row_changes_per_event
    }

    pub(crate) fn watch_table(
        self: &Arc<Self>,
        tables: BTreeSet<String>,
        queue_capacity: Option<usize>,
        snapshot_lsn: u64,
        schema_cookie: u32,
    ) -> Result<WatchHandle> {
        self.create_watch(
            WatchKind::Table,
            WatchFilter::Tables(tables),
            queue_capacity,
            WatchEvent::Initial(InitialWatchEvent {
                watch_id: 0,
                snapshot_lsn,
                schema_cookie,
                result: None,
            }),
        )
    }

    pub(crate) fn watch_range(
        self: &Arc<Self>,
        options: RangeWatchOptions,
        snapshot_lsn: u64,
        schema_cookie: u32,
    ) -> Result<WatchHandle> {
        self.create_watch(
            WatchKind::Range,
            WatchFilter::Range(PrimaryKeyRange {
                table: options.table,
                lower: options.lower,
                upper: options.upper,
                lower_inclusive: options.lower_inclusive,
                upper_inclusive: options.upper_inclusive,
            }),
            options.queue_capacity,
            WatchEvent::Initial(InitialWatchEvent {
                watch_id: 0,
                snapshot_lsn,
                schema_cookie,
                result: None,
            }),
        )
    }

    pub(crate) fn watch_query(
        self: &Arc<Self>,
        dependencies: BTreeSet<String>,
        queue_capacity: Option<usize>,
        snapshot_lsn: u64,
        schema_cookie: u32,
        result: QueryResult,
    ) -> Result<WatchHandle> {
        self.create_watch(
            WatchKind::Query,
            WatchFilter::Query(dependencies),
            queue_capacity,
            WatchEvent::Initial(InitialWatchEvent {
                watch_id: 0,
                snapshot_lsn,
                schema_cookie,
                result: Some(result),
            }),
        )
    }

    pub(crate) fn change_stream(
        self: &Arc<Self>,
        tables: Option<BTreeSet<String>>,
        queue_capacity: Option<usize>,
        snapshot_lsn: u64,
        schema_cookie: u32,
    ) -> Result<WatchHandle> {
        self.create_watch(
            WatchKind::ChangeStream,
            WatchFilter::ChangeStream(tables),
            queue_capacity,
            WatchEvent::Initial(InitialWatchEvent {
                watch_id: 0,
                snapshot_lsn,
                schema_cookie,
                result: None,
            }),
        )
    }

    pub(crate) fn publish(&self, pending: PendingReactiveCommit, commit_lsn: u64) {
        if !self.has_watchers() {
            return;
        }
        let row_changes_truncated = pending.row_changes_truncated
            || (self.max_row_changes_per_event > 0
                && pending.row_changes.len() > self.max_row_changes_per_event);
        let row_changes = if row_changes_truncated {
            Vec::new()
        } else {
            pending.row_changes
        };
        if row_changes_truncated {
            self.metrics
                .row_change_events_truncated
                .fetch_add(1, Ordering::Relaxed);
        }

        let table_changes = build_table_changes(
            &pending.changed_tables,
            &row_changes,
            pending.schema_changed,
        );
        let (event_id, watches) = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            state.next_event_id = state.next_event_id.wrapping_add(1).max(1);
            let event_id = state.next_event_id;
            let mut live = Vec::new();
            let mut dead = Vec::new();
            for (id, weak) in &state.watches {
                if let Some(watch) = weak.upgrade() {
                    live.push(watch);
                } else {
                    dead.push(*id);
                }
            }
            for id in dead {
                state.watches.remove(&id);
            }
            (event_id, live)
        };

        self.metrics
            .events_published
            .fetch_add(1, Ordering::Relaxed);
        for watch in watches {
            if watch.closed.load(Ordering::Acquire) {
                continue;
            }
            if !watch.matches(
                &pending.changed_tables,
                &row_changes,
                pending.schema_changed,
            ) {
                continue;
            }
            let event = match watch.kind {
                WatchKind::ChangeStream => WatchEvent::Change(ChangeStreamEvent {
                    stream_id: watch.id,
                    event_id,
                    commit_lsn,
                    schema_cookie: pending.schema_cookie,
                    source: pending.source,
                    table_changes: table_changes.clone(),
                    row_changes: filter_row_changes_for_watch(&watch, &row_changes),
                    row_changes_truncated,
                }),
                WatchKind::Table | WatchKind::Range | WatchKind::Query => {
                    WatchEvent::Invalidate(InvalidationEvent {
                        watch_id: watch.id,
                        event_id,
                        commit_lsn,
                        schema_cookie: pending.schema_cookie,
                        source: pending.source,
                        tables: pending.changed_tables.clone(),
                        row_changes: filter_row_changes_for_watch(&watch, &row_changes),
                        row_changes_truncated,
                        schema_changed: pending.schema_changed,
                    })
                }
            };
            let dropped = watch.enqueue(event, event_id, commit_lsn);
            if dropped > 0 {
                self.metrics
                    .events_dropped
                    .fetch_add(dropped, Ordering::Relaxed);
            } else {
                self.metrics
                    .events_delivered
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[must_use]
    pub(crate) fn metrics_snapshot(&self) -> ReactiveMetricsSnapshot {
        let watches = self.live_watches();
        let mut table_watch_count = 0;
        let mut range_watch_count = 0;
        let mut query_watch_count = 0;
        let mut change_stream_count = 0;
        let mut lagged_watch_count = 0;
        for watch in &watches {
            match watch.kind {
                WatchKind::Table => table_watch_count += 1,
                WatchKind::Range => range_watch_count += 1,
                WatchKind::Query => query_watch_count += 1,
                WatchKind::ChangeStream => change_stream_count += 1,
            }
            if watch
                .queue
                .lock()
                .map(|queue| queue.lagged)
                .unwrap_or(false)
            {
                lagged_watch_count += 1;
            }
        }
        ReactiveMetricsSnapshot {
            active_watch_count: watches.len(),
            table_watch_count,
            range_watch_count,
            query_watch_count,
            change_stream_count,
            events_published: self.metrics.events_published.load(Ordering::Relaxed),
            events_delivered: self.metrics.events_delivered.load(Ordering::Relaxed),
            events_dropped: self.metrics.events_dropped.load(Ordering::Relaxed),
            lagged_watch_count,
            row_change_events_truncated: self
                .metrics
                .row_change_events_truncated
                .load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub(crate) fn subscription_snapshots(&self) -> Vec<ReactiveSubscriptionSnapshot> {
        self.live_watches()
            .into_iter()
            .map(|watch| watch.snapshot())
            .collect()
    }

    fn create_watch(
        self: &Arc<Self>,
        kind: WatchKind,
        filter: WatchFilter,
        queue_capacity: Option<usize>,
        initial_event: WatchEvent,
    ) -> Result<WatchHandle> {
        let capacity = queue_capacity
            .unwrap_or(self.default_queue_capacity)
            .max(1)
            .min(self.max_queue_capacity);
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("reactive hub lock poisoned"))?;
        state.next_watch_id = state.next_watch_id.wrapping_add(1).max(1);
        let watch_id = state.next_watch_id;
        let mut initial = initial_event;
        if let WatchEvent::Initial(event) = &mut initial {
            event.watch_id = watch_id;
        }
        let inner = Arc::new(WatchInner {
            id: watch_id,
            kind,
            filter,
            created_at_micros: now_micros(),
            queue_capacity: capacity,
            queue: Mutex::new(WatchQueue {
                events: VecDeque::from([initial]),
                lagged: false,
            }),
            cvar: Condvar::new(),
            closed: AtomicBool::new(false),
            dropped_events: AtomicU64::new(0),
            last_delivered_event_id: AtomicU64::new(0),
        });
        state.watches.insert(watch_id, Arc::downgrade(&inner));
        self.metrics
            .active_watch_count
            .fetch_add(1, Ordering::Release);
        Ok(WatchHandle {
            hub: Arc::clone(self),
            inner,
        })
    }

    fn close_watch(&self, watch_id: u64) {
        let watch = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            state
                .watches
                .remove(&watch_id)
                .and_then(|weak| weak.upgrade())
        };
        if let Some(watch) = watch {
            if !watch.closed.swap(true, Ordering::AcqRel) {
                self.metrics
                    .active_watch_count
                    .fetch_sub(1, Ordering::Release);
                if let Ok(mut queue) = watch.queue.lock() {
                    queue.events.push_back(WatchEvent::Closed);
                }
                watch.cvar.notify_all();
            }
        }
    }

    fn live_watches(&self) -> Vec<Arc<WatchInner>> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Vec::new(),
        };
        let mut live = Vec::new();
        let mut dead = Vec::new();
        for (id, weak) in &state.watches {
            if let Some(watch) = weak.upgrade() {
                if !watch.closed.load(Ordering::Acquire) {
                    live.push(watch);
                }
            } else {
                dead.push(*id);
            }
        }
        for id in dead {
            state.watches.remove(&id);
        }
        live
    }
}

pub(crate) fn acquire_hub(registry_key: Option<PathBuf>, config: &DbConfig) -> Arc<ReactiveHub> {
    let new_hub = || {
        Arc::new(ReactiveHub::new(
            config.reactive_watch_queue_capacity,
            config.reactive_watch_queue_max_capacity,
            config.reactive_max_row_changes_per_event,
        ))
    };
    let Some(key) = registry_key else {
        return new_hub();
    };
    let registry = HUB_REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = match registry.lock() {
        Ok(guard) => guard,
        Err(_) => return new_hub(),
    };
    if let Some(existing) = guard.get(&key).and_then(Weak::upgrade) {
        return existing;
    }
    let hub = new_hub();
    guard.insert(key, Arc::downgrade(&hub));
    hub
}

pub(crate) fn existing_hub(registry_key: Option<&PathBuf>) -> Option<Arc<ReactiveHub>> {
    let key = registry_key?;
    let registry = HUB_REGISTRY.get()?;
    let guard = registry.lock().ok()?;
    guard.get(key).and_then(Weak::upgrade)
}

impl WatchInner {
    fn try_recv(&self) -> Result<Option<WatchEvent>> {
        let mut queue = self
            .queue
            .lock()
            .map_err(|_| DbError::internal("watch queue lock poisoned"))?;
        Ok(self.pop_event(&mut queue))
    }

    fn recv_timeout(&self, timeout: Duration) -> Result<Option<WatchEvent>> {
        let mut queue = self
            .queue
            .lock()
            .map_err(|_| DbError::internal("watch queue lock poisoned"))?;
        if queue.events.is_empty() && !self.closed.load(Ordering::Acquire) {
            let (new_queue, result) = self
                .cvar
                .wait_timeout(queue, timeout)
                .map_err(|_| DbError::internal("watch queue lock poisoned"))?;
            queue = new_queue;
            if result.timed_out() && queue.events.is_empty() {
                return Ok(None);
            }
        }
        Ok(self.pop_event(&mut queue))
    }

    fn pop_event(&self, queue: &mut WatchQueue) -> Option<WatchEvent> {
        let event = queue.events.pop_front()?;
        if let Some(event_id) = event.event_id() {
            self.last_delivered_event_id
                .store(event_id, Ordering::Release);
        }
        if matches!(event, WatchEvent::Lagged(_)) {
            queue.lagged = false;
        }
        Some(event)
    }

    fn enqueue(&self, event: WatchEvent, latest_event_id: u64, latest_commit_lsn: u64) -> u64 {
        let mut queue = match self.queue.lock() {
            Ok(queue) => queue,
            Err(_) => return 0,
        };
        if queue.events.len() >= self.queue_capacity {
            let dropped = queue.events.len() as u64 + 1;
            queue.events.clear();
            queue.lagged = true;
            self.dropped_events.fetch_add(dropped, Ordering::Relaxed);
            queue.events.push_back(WatchEvent::Lagged(LaggedWatchEvent {
                watch_id: self.id,
                last_seen_event_id: self.last_delivered_event_id.load(Ordering::Acquire),
                latest_event_id,
                latest_commit_lsn,
                reason: "queue_overflow".to_string(),
            }));
            self.cvar.notify_all();
            return dropped;
        }
        queue.events.push_back(event);
        self.cvar.notify_all();
        0
    }

    fn matches(
        &self,
        changed_tables: &[String],
        row_changes: &[RowChange],
        schema_changed: bool,
    ) -> bool {
        match &self.filter {
            WatchFilter::Tables(tables) | WatchFilter::Query(tables) => {
                schema_changed || changed_tables.iter().any(|table| tables.contains(table))
            }
            WatchFilter::ChangeStream(None) => true,
            WatchFilter::ChangeStream(Some(tables)) => {
                schema_changed || changed_tables.iter().any(|table| tables.contains(table))
            }
            WatchFilter::Range(range) => {
                if schema_changed || changed_tables.iter().any(|table| table == &range.table) {
                    return row_changes.is_empty()
                        || row_changes
                            .iter()
                            .any(|change| range.includes_change(change));
                }
                false
            }
        }
    }

    fn snapshot(&self) -> ReactiveSubscriptionSnapshot {
        let (queue_depth, lagged) = self
            .queue
            .lock()
            .map(|queue| (queue.events.len(), queue.lagged))
            .unwrap_or((0, false));
        ReactiveSubscriptionSnapshot {
            watch_id: self.id,
            kind: self.kind.clone(),
            created_at_micros: self.created_at_micros,
            queue_capacity: self.queue_capacity,
            queue_depth,
            last_delivered_event_id: self.last_delivered_event_id.load(Ordering::Acquire),
            dropped_events: self.dropped_events.load(Ordering::Acquire),
            lagged,
            dependencies_json: self.filter.dependencies_json(),
        }
    }
}

impl WatchFilter {
    fn dependencies_json(&self) -> String {
        let value = match self {
            Self::Tables(tables) => json!({ "tables": tables }),
            Self::Range(range) => json!({
                "table": range.table,
                "lower": range.lower,
                "upper": range.upper,
                "lower_inclusive": range.lower_inclusive,
                "upper_inclusive": range.upper_inclusive,
            }),
            Self::Query(tables) => json!({ "tables": tables }),
            Self::ChangeStream(None) => json!({ "tables": null }),
            Self::ChangeStream(Some(tables)) => json!({ "tables": tables }),
        };
        serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string())
    }
}

impl PrimaryKeyRange {
    fn includes_change(&self, change: &RowChange) -> bool {
        if change.table != self.table {
            return false;
        }
        let Some(value) = primary_key_single_value(&change.primary_key) else {
            return true;
        };
        let lower_ok = self.lower.as_ref().is_none_or(|lower| {
            compare_json_key(value, lower).is_none_or(|ordering| {
                ordering.is_gt() || (self.lower_inclusive && ordering.is_eq())
            })
        });
        let upper_ok = self.upper.as_ref().is_none_or(|upper| {
            compare_json_key(value, upper).is_none_or(|ordering| {
                ordering.is_lt() || (self.upper_inclusive && ordering.is_eq())
            })
        });
        lower_ok && upper_ok
    }
}

pub(crate) fn current_change_source() -> ChangeSource {
    CHANGE_SOURCE.with(Cell::get)
}

pub(crate) fn with_change_source<T>(source: ChangeSource, f: impl FnOnce() -> T) -> T {
    CHANGE_SOURCE.with(|cell| {
        let previous = cell.replace(source);
        let result = f();
        cell.set(previous);
        result
    })
}

pub(crate) fn row_operation_from_sync(operation: crate::sync::SyncOperation) -> RowOperation {
    match operation {
        crate::sync::SyncOperation::Insert => RowOperation::Insert,
        crate::sync::SyncOperation::Update => RowOperation::Update,
        crate::sync::SyncOperation::Delete => RowOperation::Delete,
    }
}

fn build_table_changes(
    changed_tables: &[String],
    row_changes: &[RowChange],
    schema_changed: bool,
) -> Vec<TableChange> {
    let mut counts = BTreeMap::<String, usize>::new();
    for table in changed_tables {
        counts.entry(table.clone()).or_default();
    }
    for change in row_changes {
        *counts.entry(change.table.clone()).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(table, row_change_count)| TableChange {
            table,
            schema_changed,
            row_change_count,
        })
        .collect()
}

fn filter_row_changes_for_watch(watch: &WatchInner, row_changes: &[RowChange]) -> Vec<RowChange> {
    match &watch.filter {
        WatchFilter::Tables(tables) | WatchFilter::Query(tables) => row_changes
            .iter()
            .filter(|change| tables.contains(&change.table))
            .cloned()
            .collect(),
        WatchFilter::Range(range) => row_changes
            .iter()
            .filter(|change| range.includes_change(change))
            .cloned()
            .collect(),
        WatchFilter::ChangeStream(None) => row_changes.to_vec(),
        WatchFilter::ChangeStream(Some(tables)) => row_changes
            .iter()
            .filter(|change| tables.contains(&change.table))
            .cloned()
            .collect(),
    }
}

fn primary_key_single_value(primary_key: &JsonValue) -> Option<&JsonValue> {
    let object = primary_key.as_object()?;
    if object.len() != 1 {
        return None;
    }
    object.values().next()
}

fn compare_json_key(left: &JsonValue, right: &JsonValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            Some(left.as_i64()?.cmp(&right.as_i64()?))
        }
        (JsonValue::String(left), JsonValue::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Int64(n) => JsonValue::Number(serde_json::Number::from(*n)),
        Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Bool(value) => JsonValue::Bool(*value),
        Value::Text(value) => JsonValue::String(value.clone()),
        Value::Blob(bytes) => JsonValue::String(hex_string(bytes)),
        Value::Decimal { scaled, scale } => JsonValue::String(format!("{scaled}e-{scale}")),
        Value::Uuid(bytes) => JsonValue::String(uuid_string(bytes)),
        Value::TimestampMicros(value) => JsonValue::Number(serde_json::Number::from(*value)),
        Value::Enum {
            enum_type_id,
            label_id,
        } => JsonValue::String(format!("{enum_type_id}:{label_id}")),
        Value::Geometry(bytes) | Value::Geography(bytes) => JsonValue::String(hex_string(bytes)),
        Value::IpAddr { family, addr } => format_ip_addr(*family, addr)
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => format_cidr(*family, *prefix_len, network)
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::MacAddr { len, bytes } => format_mac_addr(*len, bytes)
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::DateDays(days) => JsonValue::String(format_date_days(*days)),
        Value::TimeMicros(micros) => format_time_micros(*micros)
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::TimestampTzMicros(micros) => JsonValue::String(format_timestamp_tz_micros(*micros)),
        Value::Interval {
            months,
            days,
            micros,
        } => JsonValue::String(format_interval(*months, *days, *micros)),
    }
}

fn hex_string(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn uuid_string(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}
