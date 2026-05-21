# Reactive Query Subscriptions And Change Streams

**Status:** Active spec - ADR accepted, implementation pending
**Project:** DecentDB
**Document Type:** Implementation SPEC
**Audience:** Core engine developers, binding maintainers, CLI maintainers,
documentation authors, coding agents
**Related Roadmap Item:** `design/FUTURE_WINS.md` item 3
**Governing ADR:** `design/adr/0164-reactive-query-subscriptions-and-change-streams.md`

---

## 1. Executive Summary

This spec defines the full implementation plan for DecentDB reactive query
subscriptions and in-process change streams.

The feature provides:

- table watches
- primary-key range watches
- query watches with initial results and invalidation events
- ordered change streams with commit LSN boundaries
- bounded row-level diffs where practical
- C ABI watch handles
- binding-friendly async stream adapters
- integration with direct writes, queued writes, sync apply, and branch merge

The first complete implementation is intentionally in-process. It is not a
network pub/sub service and it is not a durable CDC log.

---

## 2. Goals

1. Let applications react to committed database changes without polling.
2. Make cross-connection writes visible to subscribers in the same process.
3. Preserve snapshot isolation and one-writer/many-readers semantics.
4. Deliver an initial query result for query watches, followed by invalidation
   events.
5. Expose commit LSN and event ordering boundaries.
6. Provide table-level invalidation for all supported writes and row-level
   changes where the executor can provide them within configured bounds.
7. Use explicit, leak-free watch cancellation.
8. Avoid callbacks while any internal engine, WAL, queue, or hub lock is held.
9. Provide stable Rust and C ABI surfaces that bindings can expose as idiomatic
   async streams, callbacks, iterators, or observables.
10. Complete the feature with tests, docs, and system inspection surfaces.

---

## 3. Non-Goals

1. Persistent changefeed retention across process restarts.
2. Network pub/sub, server-side fanout, WebSocket transport, or relay hosting.
3. Exact query result-set diffing for arbitrary SQL.
4. Automatic re-execution of every subscribed query by default.
5. Watching temporary tables, internal `__decentdb_*` tables, or virtual
   `sys.*` inspection tables.
6. Cross-process notification for independent processes opening the same file.
7. Changing the database file format or WAL frame format.

---

## 4. User-Facing Model

### 4.1 Table Watch

A table watch subscribes to one or more persistent user tables.

Required behavior:

- validates that every table exists and is watchable
- emits an initial event containing the current snapshot LSN and schema cookie
- emits invalidation events for commits touching any watched table
- includes row changes when available and within configured bounds

### 4.2 Primary-Key Range Watch

A range watch subscribes to a primary-key interval on one table.

Required behavior:

- validates that the table has a stable primary key
- stores lower and upper bounds with inclusive/exclusive flags
- emits an initial event containing the current snapshot LSN and schema cookie
- emits row-level events when row primary keys intersect the range
- conservatively invalidates on table-level-only commits, schema changes, or
  row-change truncation for the watched table

Composite primary keys are represented as canonical JSON objects keyed by
primary-key column name. If a table's key shape cannot be compared by the first
implementation, the range watch is rejected instead of silently becoming a
table watch.

### 4.3 Query Watch

A query watch subscribes to a `SELECT`.

Required behavior:

- executes the query once at subscription time and returns the initial
  `QueryResult`
- extracts persistent table dependencies from parser/planner metadata after
  view expansion
- rejects dependency-opaque queries
- emits invalidation events when a dependency table changes or relevant schema
  metadata changes
- includes the commit LSN that caused invalidation

The default query watch does not re-run the query after invalidation. Callers
re-run at a time that fits their UI/cache scheduler. Optional bounded rerun
support can be added only with explicit limits.

### 4.4 Change Stream

A change stream subscribes to commit events directly.

Required behavior:

- emits ordered commit events for matching table filters
- includes commit LSN, event ID, schema cookie, source, changed tables, and row
  changes when available
- supports starting from "now" only in v1
- reports lag if the stream falls behind its bounded queue

Durable resume from an old LSN is out of scope for this item. Applications that
need durable resume use sync journal APIs.

---

## 5. Event Types

### 5.1 Rust Event Shape

The Rust API should use typed structs equivalent to:

```rust
pub enum WatchEvent {
    Initial(InitialWatchEvent),
    Invalidate(InvalidationEvent),
    Change(ChangeStreamEvent),
    Lagged(LaggedWatchEvent),
    Closed,
}

pub struct InitialWatchEvent {
    pub watch_id: u64,
    pub snapshot_lsn: u64,
    pub schema_cookie: u32,
    pub result: Option<QueryResult>,
}

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

pub struct TableChange {
    pub table: String,
    pub schema_changed: bool,
    pub row_change_count: usize,
}

pub enum ChangeSource {
    Direct,
    Queued,
    SyncApply,
    BranchMerge,
    BranchRestore,
    Internal,
}

pub enum RowOperation {
    Insert,
    Update,
    Delete,
}

pub struct RowChange {
    pub table: String,
    pub operation: RowOperation,
    pub primary_key: serde_json::Value,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub detail: RowChangeDetail,
}

pub enum RowChangeDetail {
    FullRow,
    PrimaryKeyOnly,
    Truncated,
}
```

Names can be adjusted to fit local module style, but the fields above are the
contract the implementation must satisfy.

### 5.2 C ABI JSON Event Shape

C ABI watch reads return JSON with `schema_version: 1`.

Initial query event:

```json
{
  "schema_version": 1,
  "type": "initial",
  "watch_id": 1,
  "snapshot_lsn": 42,
  "schema_cookie": 3,
  "columns": ["id", "name"],
  "rows": [[1, "Alice"]]
}
```

Table, range, and change-stream initial events use the same envelope without
`columns` and `rows`.

Invalidation event:

```json
{
  "schema_version": 1,
  "type": "invalidate",
  "watch_id": 1,
  "event_id": 7,
  "commit_lsn": 48,
  "schema_cookie": 4,
  "source": "direct",
  "tables": ["users"],
  "schema_changed": false,
  "row_changes_truncated": false,
  "row_changes": [
    {
      "table": "users",
      "operation": "update",
      "primary_key": {"id": 1},
      "before": null,
      "after": {"id": 1, "name": "Ada"},
      "detail": "full_row"
    }
  ]
}
```

Lag event:

```json
{
  "schema_version": 1,
  "type": "lagged",
  "watch_id": 1,
  "last_seen_event_id": 6,
  "latest_event_id": 19,
  "latest_commit_lsn": 91,
  "reason": "queue_overflow"
}
```

---

## 6. API Requirements

### 6.1 Rust API

The Rust API should expose:

```rust
impl Db {
    pub fn watch_table(&self, options: TableWatchOptions) -> Result<WatchHandle>;
    pub fn watch_range(&self, options: RangeWatchOptions) -> Result<WatchHandle>;
    pub fn watch_query(&self, sql: &str, params: &[Value], options: QueryWatchOptions)
        -> Result<WatchHandle>;
    pub fn change_stream(&self, options: ChangeStreamOptions) -> Result<WatchHandle>;
    pub fn reactive_metrics(&self) -> ReactiveMetricsSnapshot;
}

impl WatchHandle {
    pub fn id(&self) -> u64;
    pub fn try_recv(&self) -> Result<Option<WatchEvent>>;
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Option<WatchEvent>>;
    pub fn close(&self) -> Result<()>;
}
```

`Drop` for `WatchHandle` must unregister the watch. `close` must be idempotent.

### 6.2 C ABI

The C ABI should expose opaque handles and JSON events:

```c
typedef struct ddb_watch_handle ddb_watch_t;

ddb_status_t ddb_db_watch_table_json(
    ddb_db_t *db,
    const char *request_json,
    ddb_watch_t **out_watch);

ddb_status_t ddb_db_watch_range_json(
    ddb_db_t *db,
    const char *request_json,
    ddb_watch_t **out_watch);

ddb_status_t ddb_db_watch_query_json(
    ddb_db_t *db,
    const char *request_json,
    ddb_watch_t **out_watch);

ddb_status_t ddb_db_change_stream_json(
    ddb_db_t *db,
    const char *request_json,
    ddb_watch_t **out_watch);

ddb_status_t ddb_watch_next_json(
    ddb_watch_t *watch,
    uint32_t timeout_ms,
    char **out_json);

ddb_status_t ddb_watch_close(ddb_watch_t **watch);
```

`ddb_watch_next_json` returns a timeout status when no event is available before
the timeout. It must not block forever unless the caller explicitly passes the
documented infinite timeout value.

### 6.3 Binding APIs

Bindings should map watch handles to idiomatic streams:

- .NET: `IAsyncEnumerable<DecentDbWatchEvent>` and cancellation tokens
- Python: iterator plus async iterator
- Go: channel or iterator with `context.Context`
- Java: `Flow.Publisher` or iterator surface
- Node: async iterator and event emitter adapter
- Dart: `Stream<DecentDbWatchEvent>`

Bindings must close native handles when a stream is canceled, disposed, dropped,
or garbage-collected.

---

## 7. Engine Design

### 7.1 Reactive Hub

Add a `reactive` module with:

- `ReactiveHub`
- `WatchRegistry`
- `WatchHandleInner`
- `CommitEvent`
- `PendingReactiveCommit`
- `ReactiveMetrics`

The hub owns:

- a monotonic `next_event_id`
- active watch registry
- metrics counters
- bounded per-watch queues

The inactive fast path must be one cheap check before doing row-diff work. If
there are no active watches and no metrics call needs initialization, writes
should not allocate reactive mutation vectors.

### 7.2 Commit Path

Write execution should build a pending event only when reactivity is active.

Required order:

1. Execute statement or transaction against runtime state.
2. Collect changed table names and optional row changes.
3. Persist and commit through existing WAL path.
4. Store committed runtime/WAL LSN state.
5. Release engine/write-queue/WAL locks for the commit.
6. Publish the event to the reactive hub.
7. Return the write result to the caller.

If a post-WAL side effect such as sync journal flushing reports an error after
the WAL commit is visible, the reactive event should still be published because
the database state changed.

### 7.3 Source Context

The commit path needs an internal source context with these values:

- `Direct`
- `Queued`
- `SyncApply`
- `BranchMerge`
- `BranchRestore`
- `Internal`

The default source is `Direct`. Queued execution, sync apply, branch merge, and
branch restore must set the source around their logical write execution.

### 7.4 DDL and Schema Changes

DDL commits publish schema invalidations. If changed table names are known, only
affected table/query watchers invalidate. If names are not known, all query and
table/range watchers invalidate with `schema_changed: true`.

Schema changes do not need row diffs.

### 7.5 Row Change Bounds

Configuration should include:

- default watch queue capacity
- maximum watch queue capacity
- maximum row changes per commit event
- maximum serialized row-change bytes per commit event

When a limit is exceeded, the event keeps changed table names and sets
`row_changes_truncated: true`.

---

## 8. System Inspection

Add read-only virtual `sys.*` surfaces:

```sql
SELECT * FROM sys.reactive_metrics;
SELECT * FROM sys.reactive_subscriptions;
```

`sys.reactive_metrics` columns:

- `active_watch_count`
- `table_watch_count`
- `range_watch_count`
- `query_watch_count`
- `change_stream_count`
- `events_published`
- `events_delivered`
- `events_dropped`
- `lagged_watch_count`
- `row_change_events_truncated`

`sys.reactive_subscriptions` columns:

- `watch_id`
- `kind`
- `created_at_micros`
- `queue_capacity`
- `queue_depth`
- `last_delivered_event_id`
- `dropped_events`
- `lagged`
- `dependencies_json`

These surfaces are diagnostics only. They must not expose internal table rows
or create dependency subscriptions.

---

## 9. Implementation Slices

### Slice 1 - Core Hub And Event Types

Deliverables:

- `reactive` module with event structs, watch registry, bounded queues, metrics
- hub attached to shared database identity
- inactive fast path
- unit tests for queue delivery, overflow, lag events, cancellation, and drop

### Slice 2 - Commit Publication And Mutation Capture

Deliverables:

- pending commit event construction in runtime execution
- changed-table capture for DML and DDL
- row-change capture for INSERT, UPDATE, DELETE where practical
- publish after locks are released
- direct write tests for ordering, rollback silence, savepoint rollback silence,
  row truncation, and schema invalidation

### Slice 3 - Rust Watch APIs

Deliverables:

- `watch_table`
- `watch_range`
- `change_stream`
- watch options and error types
- tests for table/range filtering, cross-connection fanout, timeout receive,
  explicit close, and no missed committed events

### Slice 4 - Query Watch Dependency Extraction

Deliverables:

- `watch_query`
- initial `QueryResult` event
- table dependency extraction after view expansion
- rejection for unsupported dependency-opaque queries
- tests for simple SELECT, joins, subqueries, views, schema changes, and
  unsupported query forms

### Slice 5 - Queued, Sync, And Branch Integration

Deliverables:

- source context for queued writes
- source context for sync apply
- source context for branch merge and restore
- tests proving applied sync batches and clean branch merges emit the same
  observable table/row events as local writes

### Slice 6 - C ABI And Binding Smoke Tests

Deliverables:

- opaque C watch handle
- JSON watch creation functions
- JSON event polling and timeout status
- explicit close
- C ABI tests for lifecycle, timeout, event JSON, lag, and panic safety
- binding smoke tests for every maintained binding that can run in CI

### Slice 7 - System Inspection And Documentation

Deliverables:

- `sys.reactive_metrics`
- `sys.reactive_subscriptions`
- docs for Rust API, C ABI JSON, SQL inspection, and binding examples
- updates to `docs/index.md`, relevant README feature lists, and
  `docs/about/changelog.md`
- roadmap status update only after all slices pass validation

---

## 10. Definition Of Done

The roadmap item is complete only when:

1. Slices 1 through 7 are implemented.
2. The ADR and this spec match the final behavior.
3. Public Rust docs and user docs describe the feature.
4. C ABI header and binding docs are updated.
5. `docs/about/changelog.md` has an entry.
6. `cargo fmt --check` passes.
7. `cargo check -p decentdb` passes.
8. `cargo lint` passes.
9. Targeted reactive tests pass.
10. `cargo test --workspace` passes.
11. Impacted binding smoke tests pass or are skipped only for missing external
    toolchains according to the repo's existing policy.

---

## 11. Best-Practice Decisions Recorded

1. The C ABI is poll-based instead of callback-first. Binding layers can expose
   callbacks or async streams safely in their own runtime.
2. Opaque queries are rejected instead of treated as "watch every table".
3. Query watches deliver invalidation by default rather than automatic query
   reruns.
4. Slow subscribers never fail committed writes; they receive lag events and
   must resynchronize.
5. Row-level detail is bounded. Truncation degrades to table invalidation
   rather than risking unbounded writer latency or memory.
6. Reactive row capture is independent from sync enablement.
7. Branch-local writes do not invalidate default-branch user-table watches until
   they are merged into `main`.
