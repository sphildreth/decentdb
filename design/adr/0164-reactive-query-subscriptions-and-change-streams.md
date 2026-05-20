# ADR 0164: Reactive Query Subscriptions And Change Streams
**Date:** 2026-05-20
**Status:** Accepted

## Context

DecentDB needs first-class in-process reactivity for local-first, browser,
mobile, and embedded application workflows. Applications should be able to keep
UI state, caches, and background workers current without polling every table.

The feature intersects with several existing decisions:

- `wal_end_lsn` is the committed snapshot boundary (ADR 0003).
- Connections to the same on-disk database share WAL state through the shared
  WAL registry (ADR 0117).
- The engine-owned write queue groups writes but must not expose uncommitted
  state or run host callbacks while internal locks are held (ADR 0162).
- Sync already captures committed row mutations for durable outbound journals
  when sync is enabled (ADR 0147).
- Branch merge into `main` uses normal logical DML so sync and constraints see
  it like ordinary application writes (ADR 0157 and ADR 0158).

Reactive subscriptions must preserve the one-writer/many-readers model and
must not create a durable network pub/sub system in this item.

## Decision

Implement in-process reactive query subscriptions and change streams around a
shared post-commit event hub.

The companion implementation spec is
`design/WIN_REACTIVE_QUERY_SUBSCRIPTIONS_CHANGE_STREAMS_SPEC.md`.

### Event hub ownership

The reactive hub is owned by the same logical database identity as the shared
WAL:

- on-disk databases use one hub per canonical database path in the current
  process
- all `Db` handles sharing that path publish to and subscribe from the same hub
- in-memory databases use a handle-local hub
- live subscriptions are process memory only and are not restored after reopen

This makes cross-connection writes visible to in-process subscribers without
adding a persistent changefeed, WAL format change, or database file format
change.

### Commit event contract

Every successful write commit publishes one logical commit event after the
committed state is visible and after the writer/runtime locks for that commit
have been released.

Events include:

- a monotonic in-process `event_id`
- the durable visibility boundary `commit_lsn`
- the catalog `schema_cookie`
- the source of the commit (`direct`, `queued`, `sync_apply`,
  `branch_merge`, `branch_restore`, or `internal`)
- changed user tables
- optional primary-key row changes when the executor can provide them within
  configured bounds
- a truncation flag when row-level details were intentionally reduced to table
  invalidation

Rollbacks, failed statements, and savepoint-rolled-back mutations publish no
events. Explicit transactions publish at most one event at the outer `COMMIT`.

Fanout is best-effort from the commit path's perspective. A slow or full
subscriber must not fail a committed write. Subscriber overflow is reported to
that subscriber as a `Lagged` event requiring resynchronization.

### Subscription kinds

The stable surface has four watch kinds:

1. **Table watch**: invalidates when one or more named persistent user tables
   change.
2. **Primary-key range watch**: invalidates when row changes intersect a
   declared primary-key range. If a commit only has table-level information for
   that table, the range watch is conservatively invalidated.
3. **Query watch**: runs the initial `SELECT`, records conservative table
   dependencies from parsed/planned query metadata, then emits invalidation
   events when a dependent table or schema changes.
4. **Change stream**: emits ordered commit events and row changes for all
   matching tables without owning an initial query result.

Query watches are dependency watches, not a promise of exact result-set diffs.
The required behavior is initial result plus invalidation. Bounded automatic
query re-execution may be added as an option, but the default public contract
keeps re-execution in caller control.

### Dependency extraction

Query subscriptions support `SELECT` statements whose persistent table
dependencies can be extracted conservatively from the parser/planner after view
expansion.

Unsupported or dependency-opaque queries are rejected at subscription time with
a typed unsupported-subscription error. DecentDB must not silently subscribe to
"all tables" for an opaque query because that hides cost and correctness
surprises from application code.

Subscriptions do not cover temporary tables, internal `__decentdb_*` tables, or
virtual `sys.*` inspection tables in this item.

### Row-level changes

Row-level changes are emitted where practical:

- INSERT includes the primary key and post-mutation row.
- DELETE includes the primary key and pre-mutation row when already available;
  otherwise it includes the primary key and marks the row body unavailable.
- UPDATE includes the primary key, post-mutation row, and pre-mutation row or
  changed-column detail when already available within bounds.
- DDL, bulk operations, fallback paths, or commits that exceed configured row or
  byte limits degrade to table-level invalidation.

Reactive row capture is independent of sync enablement. Sync may reuse the same
mutation construction helpers, but disabling sync must not disable reactive
events.

### Delivery and cancellation

Each watch owns an explicit bounded queue. Queue capacity is configurable within
engine limits. When the queue is full, the watch is marked lagged and receives a
synthetic lag event after the hub makes room.

Watch handles are explicitly closeable and are automatically unregistered on
drop. Cancellation is idempotent. No callback or event delivery path may run
while the engine write lock, WAL writer lock, write-queue executor lock, or hub
registry lock is held.

### C ABI and bindings

The C ABI uses opaque watch handles and poll-based JSON event delivery as the
stable FFI contract:

- create table, range, query, or change-stream watch handles
- read the initial event and subsequent events with `ddb_watch_next_json`
- close with `ddb_watch_close`

Bindings can expose language-native async streams, iterators, callbacks, or
observables on top of the poll handle. The C ABI itself avoids mandatory host
callbacks because callback lifetime, thread-affinity, and panic behavior differ
across .NET, Python, Go, Java, Node, Dart, and browser runtimes.

All exported C ABI functions remain wrapped by the panic-safety rules from ADR
0118.

### Sync and branch integration

Applied sync batches publish the same table and row events as local writes with
source `sync_apply`.

Clean branch merges into `main` publish the same events as direct logical DML
with source `branch_merge`.

Branch-local writes do not invalidate default-branch user-table subscriptions
until they are merged. Metadata-only branch operations may emit internal events
for diagnostics, but they do not masquerade as user-table changes.

### Observability

The feature exposes reactive metrics through Rust, C ABI JSON, and read-only
`sys.*` inspection tables:

- active watch count by kind
- events published
- events delivered
- per-watch lag/drop counts
- queue capacity and current depth
- row-change truncation count

These metrics must not allocate subscription state when reactivity has never
been used.

## Consequences

The design gives applications live invalidation and change streams without
weakening DecentDB's durability or snapshot semantics. It also avoids forcing a
host-runtime callback model through the C ABI.

Because live watches are in-process only, applications that need durable CDC
after restart must use the sync journal or a future persistent changefeed
feature. A subscriber that falls behind must resynchronize from a fresh query
snapshot or from durable sync state if the application has enabled sync.

The commit path gains a cheap inactive fast path and bounded work when
subscribers exist. Large commits may produce table invalidations instead of
full row diffs to protect writer latency and memory use.

## Alternatives Considered

1. **Durable changefeed log in the database file.** Rejected for this item
   because it would require retention policy, format governance, migration
   support, and overlap with the sync journal. Durable CDC can be designed
   later if product needs exceed in-process subscriptions.
2. **Mandatory C callbacks.** Rejected because host runtimes have different
   callback threading and lifetime rules. Poll handles are safer and still let
   bindings provide callbacks or async streams.
3. **Db-handle-local subscriptions only.** Rejected because cross-connection
   visibility is already a core DecentDB behavior through the shared WAL
   registry. A watch on one handle should see writes committed by another
   handle in the same process.
4. **Always re-run subscribed queries inside the engine.** Rejected as the
   default because query cost can be unbounded and application UI frameworks
   often want to coalesce invalidations. Optional bounded re-execution can be
   layered on top of the invalidation contract.
5. **Subscribe opaque queries to every table.** Rejected because it hides
   dependency analysis failures and can create broad invalidation storms.

## Validation Requirements

The implementation is not complete until tests cover:

- initial query result delivery
- table, range, query, and change-stream subscriptions
- commit LSN/event ordering across direct and queued writes
- rollback and savepoint rollback producing no events
- callback/poll delivery after internal locks are released
- bounded queue overflow and lag recovery signaling
- explicit cancellation and drop cleanup
- sync apply and branch merge event sources
- row-change truncation behavior
- C ABI watch lifecycle and JSON event delivery
- relevant binding smoke tests

All unit tests must pass before the roadmap item is marked complete.

## References

- `design/FUTURE_WINS.md` item 3
- `design/WIN_REACTIVE_QUERY_SUBSCRIPTIONS_CHANGE_STREAMS_SPEC.md`
- `design/adr/0003-snapshot-lsn-atomicity.md`
- `design/adr/0117-shared-wal-registry.md`
- `design/adr/0118-rust-ffi-panic-safety.md`
- `design/adr/0147-local-sync-journal-foundation.md`
- `design/adr/0157-branch-diff-restore-and-merge-semantics.md`
- `design/adr/0158-branch-sync-interaction.md`
- `design/adr/0162-engine-owned-write-queue-strict-group-commit.md`
