# Local-First Sync as a First-Class Capability

**Status:** Proposed  
**Project:** DecentDB  
**Document Type:** Implementation SPEC  
**Audience:** Core engine developers, storage/replication implementers, SDK maintainers, CLI maintainers, documentation authors, coding agents  
**Related Vision Theme:** DecentDB as an embedded SQL database built for modern apps, offline workflows, and agent-friendly development

---

## 1. Executive Summary

This SPEC defines a first-class, built-in local-first sync capability for DecentDB. The goal is to make DecentDB suitable for applications that must continue to work offline, synchronize changes later, and resolve conflicts in predictable ways without requiring external replication middleware as the primary architecture.

The feature set described here is intentionally broader than a low-level changefeed. It introduces:

- durable local change capture
- first-class replicas and peers
- pull/push synchronization
- filtered subscriptions and scoped replication
- deterministic conflict detection and resolution
- resumable sync state
- human-readable and machine-readable diagnostics
- safe operational tooling
- comprehensive end-user documentation and examples

This capability should allow a developer to build applications such as:

- desktop apps with intermittent connectivity
- mobile or edge applications with disconnected operation
- local-first SaaS clients with per-user or per-tenant replication
- field service tools that sync when connectivity returns
- branchable dev/test datasets
- agent workflows that operate against isolated local replicas and merge later

The intended market differentiator is not “DecentDB has CDC.” The differentiator is:

> **DecentDB provides native SQL-first local/offline sync with conflict-aware merge semantics as a core engine capability.**

---

## 2. Goals

### 2.1 Primary Goals

1. **Enable fully usable local replicas** that accept reads and writes while disconnected.
2. **Provide built-in synchronization primitives** for pushing and pulling changes between peers.
3. **Capture changes durably and incrementally** in a way that survives process restarts and crashes.
4. **Support scoped synchronization** so replicas can subscribe only to relevant subsets of data.
5. **Provide deterministic conflict handling** with clear default behavior and extensibility.
6. **Keep the feature SQL-first and tooling-friendly** via SQL primitives, CLI commands, and SDK APIs.
7. **Support agent-friendly automation** with stable machine-readable outputs and explicit diagnostics.
8. **Produce excellent user documentation** with practical examples, schema patterns, troubleshooting, and operational guidance.

### 2.2 Secondary Goals

1. Support sync over multiple transports without coupling the engine to a single runtime stack.
2. Make sync state inspectable via SQL system tables/views.
3. Support future expansion to browser, mobile, and server-hosted relay patterns.
4. Allow the same underlying engine semantics to power future branch/merge workflows.

---

## 3. Non-Goals

The initial release of this capability will **not** attempt to solve every distributed systems problem.

### 3.1 Explicit Non-Goals for Initial Slices

1. Multi-master geo-distributed consensus.
2. Transparent always-on HA clustering.
3. Automatic global ordering across all nodes.
4. Cross-database distributed transactions.
5. Arbitrary conflict-free replication of every SQL construct without restrictions.
6. Full browser transport and WebRTC sync in the initial implementation.
7. End-to-end identity and access management platform; initial auth may rely on signed credentials/tokens and transport-level security.
8. General-purpose message bus replacement.
9. Automatic semantic merge of arbitrary BLOB content.

### 3.2 Deferred Areas

- advanced CRDT-native field types
- peer discovery protocols
- mesh topology auto-routing
- partial row/field-level sync for arbitrary tables
- large-scale hosted relay service
- collaborative live-presence features

---

## 4. Product Principles

1. **Local-first by design:** sync is additive to a complete local database, not a thin offline cache.
2. **Durability first:** sync metadata and change capture must survive crashes and restarts.
3. **Inspectable over magical:** users must be able to query what happened and why.
4. **Safe defaults:** default conflict behavior must be conservative and deterministic.
5. **Schema-aware:** sync should respect keys, constraints, and version compatibility.
6. **Filtered but explicit:** scoped sync rules must be defined clearly and validated.
7. **Engine-first, transport-pluggable:** core semantics belong in DecentDB; network transport can be layered.
8. **Docs are part of the feature:** this is not complete without real-world examples and operational guidance.

---

## 5. High-Level Capability Definition

Local-first sync is defined as the ability for two or more DecentDB databases to:

1. independently accept local writes,
2. persist change history suitable for synchronization,
3. exchange change batches,
4. detect whether changes can be applied cleanly,
5. resolve or surface conflicts according to configured policy,
6. update local sync state so future syncs are incremental,
7. continue doing this repeatedly and safely.

A future relay/server can participate in the same protocol, but the first-class concept is **replica-to-replica synchronization with durable journals and explicit sync state**.

---

## 6. Conceptual Model

### 6.1 Core Terms

- **Replica:** a DecentDB database instance participating in sync.
- **Replica ID:** stable identifier for a replica.
- **Peer:** another replica or relay endpoint with which synchronization occurs.
- **Change Journal:** durable, ordered local record of sync-relevant mutations.
- **Sync Scope:** rules that determine which tables/rows are eligible for synchronization.
- **Sync Session:** one push, pull, or bidirectional synchronization attempt.
- **Checkpoint / Watermark:** progress marker indicating the latest known applied position from a peer.
- **Conflict:** a situation in which a remote change cannot be applied cleanly under current state/rules.
- **Merge Policy:** configured strategy used to resolve or surface conflicts.
- **Tombstone:** durable deletion marker needed for replication correctness.

### 6.2 Data Flow Overview

1. A local transaction commits.
2. Sync-relevant row changes are captured into a durable change journal.
3. A sync client asks for changes since a peer checkpoint.
4. The remote side returns a change batch and metadata.
5. The local side validates compatibility and ordering assumptions.
6. Changes are applied transactionally or rejected.
7. Conflicts are resolved or recorded.
8. The peer watermark is advanced.
9. Diagnostics and metrics are written.

---

## 7. Proposed User Experience

The capability should be exposed through:

1. **SQL primitives** for enabling sync, defining peers/scopes, viewing state, and initiating sync.
2. **CLI commands** for operational workflows and debugging.
3. **SDK APIs** for application embedding.
4. **System tables/views** for inspection and observability.

### 7.1 Representative SQL Examples

```sql
-- Enable sync metadata and local change capture.
PRAGMA sync_enable = true;

-- Register the local replica identity.
SELECT sync_init_replica('local-kc-desktop');

-- Define a peer.
SELECT sync_add_peer(
  name => 'central',
  endpoint => 'https://sync.example.com',
  auth_token => 'env:DECENTDB_SYNC_TOKEN'
);

-- Define a sync scope.
SELECT sync_create_scope(
  scope_name => 'tenant_42',
  include_tables => '["accounts","projects","tasks","task_comments"]',
  row_filter => 'tenant_id = 42'
);

-- Associate a peer with a scope.
SELECT sync_bind_peer_scope('central', 'tenant_42');

-- Push local changes.
SELECT sync_push('central');

-- Pull remote changes.
SELECT sync_pull('central');

-- Do both.
SELECT sync_sync('central');
```

### 7.2 Representative CLI Examples

```bash
ddb sync init app.ddb --replica-id local-kc-desktop

ddb sync peer add app.ddb central \
  --endpoint https://sync.example.com \
  --token-env DECENTDB_SYNC_TOKEN

ddb sync scope create app.ddb tenant_42 \
  --include accounts,projects,tasks,task_comments \
  --row-filter "tenant_id = 42"

ddb sync run app.ddb --peer central --direction both

ddb sync status app.ddb --json

ddb sync conflicts app.ddb --peer central

ddb sync doctor app.ddb
```

### 7.3 Representative SDK Shape

```csharp
await db.Sync.InitializeReplicaAsync("local-kc-desktop");
await db.Sync.AddPeerAsync(new SyncPeer
{
    Name = "central",
    Endpoint = "https://sync.example.com",
    AuthToken = token
});

await db.Sync.CreateScopeAsync(new SyncScope
{
    Name = "tenant_42",
    IncludedTables = ["accounts", "projects", "tasks", "task_comments"],
    RowFilterSql = "tenant_id = 42"
});

var result = await db.Sync.RunAsync("central", SyncDirection.Both);
```

---

## 8. Architecture

### 8.1 Major Components

1. **Change Capture Layer**
   - captures sync-relevant row mutations on commit
   - records inserts, updates, deletes, and tombstones
   - emits durable journal entries

2. **Sync Metadata Catalog**
   - stores replica identity
   - stores peer definitions
   - stores scopes and bindings
   - stores watermarks/checkpoints
   - stores conflict records

3. **Journal Reader/Writer**
   - enumerates change batches efficiently
   - supports batching, checkpointing, retention, and replay

4. **Protocol Encoder/Decoder**
   - serializes changes and metadata for transport
   - handles version negotiation and capability flags

5. **Apply Engine**
   - validates incoming changes
   - applies them transactionally
   - detects and records conflicts
   - updates watermarks only after durable success

6. **Conflict Resolver**
   - supports default and pluggable policies
   - records structured resolution details

7. **Scope Evaluator**
   - validates that sync scopes are legal and enforceable
   - evaluates inclusion rules during capture and apply

8. **Transport Adapters**
   - initial adapter: HTTP(S)
   - future adapters: file/export-import, relay, embedded callback transport

9. **Observability/Doctoring Layer**
   - emits logs, counters, metrics, and diagnostic reports

### 8.2 Recommended Initial Deployment Shapes

1. **Client ↔ Server Relay** using HTTPS
2. **Desktop ↔ Desktop** direct sync for trusted LAN or controlled endpoints
3. **File-based export/import** for manual offline transfer

The core protocol must not depend on a specific deployment shape.

---

## 9. Storage Model

### 9.1 Required System Tables / Internal Catalog Objects

Proposed internal objects (names subject to engine naming conventions):

- `sys_sync_replicas`
- `sys_sync_peers`
- `sys_sync_scopes`
- `sys_sync_scope_tables`
- `sys_sync_bindings`
- `sys_sync_journal`
- `sys_sync_journal_rows`
- `sys_sync_watermarks`
- `sys_sync_conflicts`
- `sys_sync_sessions`
- `sys_sync_capabilities`
- `sys_sync_retention`

### 9.2 Journal Requirements

The change journal must:

1. be durable
2. preserve local order for committed transactions
3. identify originating replica
4. identify originating transaction/change sequence
5. contain enough metadata to reapply or analyze changes
6. support deletes via tombstones
7. support incremental reading by checkpoint
8. support retention/pruning only when safe

### 9.3 Example Journal Fields

Per transaction or per row-change record should include enough information for:

- replica ID
- local sequence number
- transaction ID
- commit timestamp (informational, not sole conflict authority)
- table ID/name
- primary key payload
- operation type (`insert`, `update`, `delete`)
- before-image hash or version marker when applicable
- after-image payload for applied columns
- schema version / sync contract version
- scope tags / routing metadata if needed

### 9.4 Journal Storage Options (Decision Required for Slice 1)

| Option | Pros | Cons |
|--------|------|------|
| Separate journal file | Simple, independent retention, easy to inspect | Double-write overhead, separate durability concerns |
| Integrated with WAL | Single write path, atomic with data | Complex retention logic, tight coupling with storage layer |
| Dedicated journal table | SQL-queryable, uses existing storage | Transaction overhead, may bloat main database |
| Hybrid (WAL + compaction) | Efficient writes, queryable after compaction | More complex implementation |

**Recommendation for v1:** Separate journal file with periodic compaction. This provides clear separation of concerns and allows independent retention policies.

### 9.5 Tombstone Retention

Tombstones are necessary for correct replication but can grow unbounded. They must be retained until:

1. All configured peers have acknowledged the deletion (via watermark advance)
2. A configurable tombstone TTL has elapsed (default: 30 days)
3. User explicitly forces unsafe prune with `--allow-data-loss`

Tombstone storage should be monitored via:

```sql
SELECT 
    tombstone_count,
    tombstone_bytes,
    oldest_tombstone_age_hours
FROM sys_sync_retention;
```

---

## 10. Sync Protocol Requirements

### 10.1 Protocol Design Goals

1. Simple to reason about
2. Versioned from day one
3. Resumable
4. Idempotent where practical
5. Efficient for incremental sync
6. Suitable for machine inspection and troubleshooting

### 10.2 Initial Logical Operations

- `HELLO` / capability negotiation
- `GET_CHANGES` since watermark
- `PUSH_CHANGES`
- `ACK_APPLIED`
- `GET_STATUS`
- `GET_CONFLICTS`
- `EXPORT_BATCH`
- `IMPORT_BATCH`

### 10.3 Protocol Versioning

Every session must negotiate:

- protocol version
- engine version
- sync feature flags
- optional capabilities (scopes, tombstones, custom conflict handlers, compression)

If negotiation fails, the session must abort with a structured compatibility error.

### 10.4 Batch Semantics

Each batch should include:

- source replica ID
- destination peer name/ID if relevant
- batch ID
- start checkpoint
- end checkpoint
- change count
- schema/sync contract version
- change payloads
- optional compression/encryption metadata

### 10.5 Idempotency Expectations

Re-applying the same batch must not silently duplicate writes. The system must detect already-applied batch IDs or sequence ranges and treat replay safely.

### 10.6 Batch Size Limits

| Limit | Default | Configurable | Description |
|-------|---------|--------------|-------------|
| Max batch size | 10 MB | Yes | Total uncompressed payload size |
| Max row count | 50,000 | Yes | Number of row changes per batch |
| Max batch time | 60s | Yes | Time to assemble a batch during export |
| Max apply time | 300s | Yes | Time limit for applying incoming batch |

Batches exceeding limits should be:

1. **During export:** Split into multiple smaller batches automatically
2. **During import:** Rejected with error code `BATCH_TOO_LARGE` and clear guidance

Configuration via SQL:

```sql
SELECT sync_set_config('max_batch_size_mb', 20);
SELECT sync_set_config('max_batch_rows', 100000);
```

---

## 11. Scope and Filtering Model

### 11.1 Why Scopes Matter

A core part of local-first usability is allowing a device or user to replicate only the data they need. This cannot be left to application-layer ad hoc filtering if DecentDB wants a first-class sync story.

### 11.2 Initial Scope Model

Initial implementation should support:

1. **table inclusion lists**
2. **simple SQL row filters** with documented restrictions
3. **readable validation errors** when a scope cannot be safely enforced

### 11.3 Restrictions for Initial Release

To reduce complexity, row filters may initially be restricted to:

- deterministic predicates
- columns on the target table only
- no subqueries
- no non-deterministic functions
- no external state references

#### 11.3.1 Allowed Row Filter Patterns (v1)

**Allowed:**

- Column comparisons: `tenant_id = 42`
- AND/OR combinations: `tenant_id = 42 AND deleted_at IS NULL`
- IN lists: `tenant_id IN (42, 43, 44)`
- Range predicates: `created_at > '2026-01-01'`
- IS NULL / IS NOT NULL: `deleted_at IS NULL`
- NOT conditions: `NOT is_archived`

**Disallowed:**

- Subqueries: `tenant_id IN (SELECT id FROM tenants WHERE ...)`
- Non-deterministic functions: `RANDOM()`, `NOW()`, `CURRENT_TIMESTAMP` (use literal timestamps)
- Cross-table references: `EXISTS (SELECT 1 FROM other_table ...)`
- User-defined functions
- Window functions
- Aggregate functions
- JSON path expressions (defer to future version)

#### 11.3.2 Scope Validation Error Messages

When a scope definition is rejected, the error must clearly state why:

```
ERROR: Invalid row filter for scope 'tenant_42'
DETAIL: Row filter contains disallowed function: NOW()
HINT: Use a literal timestamp instead, e.g., '2026-03-27T00:00:00Z'
```

```
ERROR: Invalid scope 'user_data'
DETAIL: Table 'sessions' has no primary key defined
HINT: Scoped sync requires tables to have stable primary keys
```

### 11.4 Required Validation

Scope creation must validate:

- referenced tables exist
- row filter columns exist
- tables have stable primary keys
- tables do not use unsupported constructs for scoped sync
- the scope can be applied during capture and/or apply consistently

### 11.5 Deferred Scope Features

- relationship-based cascading scope expansion
- arbitrary join-based scopes
- per-column sync suppression beyond defined system constraints

### 11.6 Multi-Tenant Isolation

When multiple tenants share a database, sync scopes must provide strong isolation:

**Requirements:**

1. Each tenant should have a dedicated scope with tenant-specific row filter
2. Scope validation must prevent cross-tenant row filter leakage
3. Peer bindings must be scope-specific (a peer syncs one scope, not all)
4. Audit logging must capture tenant context for all sync operations

**Example multi-tenant setup:**

```sql
-- Create isolated scopes per tenant
SELECT sync_create_scope(
  scope_name => 'tenant_42',
  include_tables => 'accounts,projects,tasks',
  row_filter => 'tenant_id = 42'
);

SELECT sync_create_scope(
  scope_name => 'tenant_43',
  include_tables => 'accounts,projects,tasks',
  row_filter => 'tenant_id = 43'
);

-- Bind each tenant to their own peer endpoint
SELECT sync_bind_peer_scope('relay-tenant-42', 'tenant_42');
SELECT sync_bind_peer_scope('relay-tenant-43', 'tenant_43');
```

**Isolation validation:**

- Row filter must reference a column that exists in ALL included tables
- Validation fails if any included table lacks the filter column
- Runtime enforcement logs warnings if a row unexpectedly matches multiple scopes

---

## 12. Conflict Model

### 12.1 Core Requirement

Conflicts must be treated as a first-class concept, not hidden edge cases.

### 12.2 Conflict Scenarios to Support

1. update vs update to same row from different replicas
2. delete vs update
3. insert with same primary key but different payload
4. apply against missing dependency row
5. apply against incompatible schema version
6. uniqueness or FK violation caused by concurrent changes

### 12.3 Default Initial Policy Set

Initial implementation should support at least:

1. **Fail and record conflict**
2. **Last-writer-wins** (only when explicitly enabled)
3. **Origin-priority wins** (configured peer precedence)
4. **Manual resolution required**

### 12.4 Recommended Safe Default

The default policy should be:

- attempt clean apply
- if conflict occurs, record conflict and do not silently discard information
- continue or stop based on configured session mode

### 12.5 Manual Resolution Workflow

The system must make it possible to:

- list conflicts
- inspect local and remote versions
- choose resolution action
- mark resolved
- optionally re-run apply/merge

### 12.6 Future Conflict Model Extensions

- per-table policies
- per-scope policies
- custom merge handlers
- CRDT-backed columns or data types

### 12.7 Example Conflict Record

When a conflict is detected, it should be queryable with full context:

```sql
SELECT * FROM sys_sync_conflicts WHERE conflict_id = 42;
```

Example output:

| Field | Value |
|-------|-------|
| `conflict_id` | 42 |
| `table_name` | `tasks` |
| `pk_payload` | `{"id": 101}` |
| `conflict_type` | `update_update` |
| `local_version` | `{"status": "done", "updated_at": "2026-03-27T10:00:00Z"}` |
| `remote_version` | `{"status": "in_progress", "updated_at": "2026-03-27T09:30:00Z"}` |
| `remote_replica_id` | `mobile-field-07` |
| `detected_at` | `2026-03-27T10:05:00Z` |
| `resolution` | `null` |
| `resolved_at` | `null` |

### 12.8 Timestamp and Clock Skew Handling

Commit timestamps are informational only. Conflict resolution MUST NOT rely solely on wall-clock comparison between replicas, as clocks may be skewed or maliciously altered.

**Required approach:**

1. **Version vectors** or **hybrid logical clocks (HLC)** for causality tracking
2. **Deterministic tiebreaker** based on replica ID precedence when logical timestamps are equal
3. **Explicit conflict recording** when causality cannot be determined

**Example conflict resolution with equal timestamps:**

```
Policy: origin-priority-wins
Precedence: ['central', 'desktop-*', 'mobile-*']
Result: 'central' replica wins over 'mobile-*' when timestamps are equal
```

---

## 13. Schema Compatibility and Migration

### 13.1 Requirement

Sync correctness is impossible if schema divergence is unmanaged. Sync must be schema-aware.

### 13.2 Initial Expectations

The system must track:

- local schema version
- sync contract version
- peer-advertised schema/sync version

### 13.3 Compatibility Modes

1. **Strict:** peers must have matching sync contract version.
2. **Compatible:** additive compatible changes allowed.
3. **Blocked:** incompatible schema differences prevent sync.

### 13.4 Initial Recommendation

Start with a mostly strict model for early slices. Expand later only after explicit compatibility rules and automated checks exist.

### 13.5 Required User Guidance

Documentation must explain:

- how schema changes interact with sync
- which migration patterns are sync-safe
- how to roll out migrations across disconnected replicas
- how to recover from incompatible versions

### 13.6 Enabling Sync on Existing Databases

When `PRAGMA sync_enable = true` is set on an existing database:

1. **Existing data is NOT automatically journaled** — only new mutations are captured
2. **To sync existing data**, use one of these approaches:

**Option A: Full Snapshot Bootstrap**

```bash
ddb sync bootstrap app.ddb --peer central --full-snapshot
```

Creates an initial batch containing all in-scope data. The receiving peer treats this as initial state.

**Option B: Manual Export/Import**

```bash
# On source database
ddb sync export app.ddb --output initial_snapshot.json --full

# On target database
ddb sync import app.ddb --input initial_snapshot.json
```

**Option C: Application-Level Migration**

For complex cases, the application can read existing data and re-insert it to trigger journal capture.

**Recommendation:** Use Option A for most cases. It's the simplest and most reliable.

### 13.7 Schema Migration Rollout Strategy

For rolling out schema changes across disconnected replicas:

1. **Phase 1: Prepare** — All replicas must be on compatible schema version N
2. **Phase 2: Deploy additive changes** — Add new columns/tables; sync continues
3. **Phase 3: Sync and verify** — Ensure all replicas have received changes
4. **Phase 4: Deploy breaking changes** — Only after all replicas synced

**Additive changes (sync-safe):**

- Adding new tables
- Adding new nullable columns
- Adding new indexes
- Adding new scopes

**Breaking changes (require coordination):**

- Dropping tables
- Dropping columns
- Renaming columns
- Changing column types
- Modifying primary keys

---

## 14. Security Requirements

### 14.1 Core Security Requirements

1. secure transport for network sync
2. peer authentication
3. ability to use secrets via env/config rather than embedding directly in SQL scripts
4. auditability of sync actions
5. clear support for future signed batches and encrypted payloads

### 14.2 Initial Requirements

- HTTPS transport support
- token- or credential-based peer auth
- secure secret loading from CLI and SDKs
- sensitive values redacted in logs and doctor output

### 14.3 Deferred Security Features

- mutual TLS built into official transport adapter
- signed batch verification
- field-level encrypted sync payload semantics
- key rotation workflows beyond basic peer credential replacement

---

## 15. Observability and Diagnostics

### 15.1 Required Visibility

Users must be able to determine:

- whether sync is enabled
- what peers are configured
- what scope is active
- current watermarks
- pending local changes
- last sync result
- conflict counts
- retention pressure
- compatibility issues

### 15.2 SQL Visibility

Provide system views or helper functions for:

- sync status by peer
- pending journal counts
- recent sessions
- recent conflicts
- peer capabilities

### 15.3 CLI Diagnostic Commands

- `ddb sync status`
- `ddb sync doctor`
- `ddb sync conflicts`
- `ddb sync sessions`
- `ddb sync export`
- `ddb sync import`

### 15.4 Machine-Readable Output

All major CLI commands must support `--json` with stable fields suitable for CI, agents, and scripts.

### 15.5 Logs and Metrics

Emit structured logs and counters for:

- sessions started/completed/failed
- batches sent/received
- rows applied
- conflicts encountered
- bytes transferred
- apply latency
- retry counts
- retention backlog age

### 15.6 Metrics Export

Sync metrics should be exposed via multiple interfaces:

**SQL Interface:**

```sql
SELECT * FROM sys_sync_metrics;
```

**CLI Interface:**

```bash
ddb sync metrics --format prometheus
ddb sync metrics --format json
```

**SDK Interface:**

```csharp
var metrics = await db.Sync.GetMetricsAsync();
Console.WriteLine($"Pending changes: {metrics.PendingChanges}");
```

**Key Metrics:**

| Metric Name | Type | Description |
|-------------|------|-------------|
| `sync_journal_bytes_total` | Gauge | Total bytes in journal |
| `sync_pending_changes` | Gauge | Number of unpushed changes |
| `sync_last_success_timestamp` | Gauge | Unix timestamp of last successful sync |
| `sync_conflict_count` | Gauge | Current unresolved conflicts |
| `sync_bytes_sent_total` | Counter | Total bytes sent to all peers |
| `sync_bytes_received_total` | Counter | Total bytes received from all peers |
| `sync_session_duration_seconds` | Histogram | Duration of sync sessions |
| `sync_apply_errors_total` | Counter | Total apply errors encountered |

---

## 16. Performance Requirements

### 16.1 Initial Performance Targets

These are starting targets and may be tuned:

| Metric | Target | Notes |
|--------|--------|-------|
| Journal capture overhead | < 5% latency increase | For OLTP workloads with sync enabled |
| Incremental sync throughput | > 10,000 rows/second | Local-to-local, no network |
| Batch apply rate | > 5,000 rows/second | Single-threaded apply |
| Status query latency | < 50ms | For journals up to 1M entries |
| Conflict detection overhead | < 1ms per row | During apply phase |
| Journal enumeration | < 100ms | For 100K pending changes |

### 16.2 Performance Constraints

The design must avoid:

- replaying entire history for routine sync
- O(n) full-database comparisons as the default mechanism
- storing excessively verbose change payloads when compact encodings are possible

### 16.3 Benchmark Expectations

Benchmarks must cover:

- transaction overhead with sync disabled vs enabled
- incremental sync throughput
- scoped sync performance
- delete/tombstone overhead
- conflict-heavy scenarios
- large journal retention/pruning behavior

---

## 17. Reliability Requirements

### 17.1 Crash Safety

The system must ensure:

- a committed local write is either present in the journal or the transaction fails atomically
- remote apply and watermark advance are atomic with respect to durability expectations
- interrupted sessions can be retried safely

### 17.2 Retry Behavior

Retry behavior must be explicit and configurable.

Initial official behavior should include:

- bounded retries for transient transport failures
- no blind infinite retry loops in the engine core
- clear exit status and diagnostics in CLI/SDK results

### 17.3 Pruning Safety

Journal pruning must not delete changes still needed by configured peers according to their retained watermarks, unless the user explicitly forces unsafe maintenance.

### 17.4 Partial Apply Failure Handling

When a batch apply encounters a conflict or error mid-way, the behavior depends on the configured mode:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `atomic` (default) | Roll back entire batch, record first conflict, return error | Strict consistency required |
| `continue-on-conflict` | Record conflict, continue applying remaining changes | Best-effort sync, review conflicts later |
| `stop-on-conflict` | Stop at first conflict, apply successful changes, return partial result | Interactive resolution |

The session record must capture:

```sql
SELECT 
    session_id,
    rows_attempted,
    rows_applied,
    rows_conflicted,
    rows_skipped,
    rollback_performed
FROM sys_sync_sessions
WHERE session_id = 'sess_12345';
```

### 17.5 Network Partition Behavior

During extended disconnection:

1. Local journal continues to grow
2. Watermark lag increases
3. `ddb sync status --json` reports `partitioned: true` after N consecutive failed attempts
4. User can configure alert thresholds on `journal_backlog_bytes`

**Example status during partition:**

```json
{
  "peer": "central",
  "status": "partitioned",
  "last_success": "2026-03-27T08:00:00Z",
  "failed_attempts": 12,
  "partitioned_since": "2026-03-27T08:15:00Z",
  "journal_backlog_bytes": 52428800,
  "journal_backlog_rows": 15000,
  "alert_threshold_bytes": 104857600
}
```

**Recovery on reconnection:**

- Automatic retry with exponential backoff
- Incremental sync resumes from last known watermark
- No data loss for committed local writes

---

## 18. User-Facing CLI and SQL Contract

### 18.1 SQL Surface (Initial Candidate)

- `sync_init_replica(replica_id)`
- `sync_add_peer(name, endpoint, auth_token)`
- `sync_remove_peer(name)`
- `sync_create_scope(scope_name, include_tables, row_filter)`
- `sync_drop_scope(scope_name)`
- `sync_bind_peer_scope(peer_name, scope_name)`
- `sync_unbind_peer_scope(peer_name, scope_name)`
- `sync_push(peer_name)`
- `sync_pull(peer_name)`
- `sync_sync(peer_name)`
- `sync_status(peer_name)`
- `sync_conflicts(peer_name)`
- `sync_resolve_conflict(conflict_id, resolution)`
- `sync_prune(peer_name | null, options...)`

### 18.2 CLI Surface (Initial Candidate)

- `ddb sync init`
- `ddb sync peer add|remove|list`
- `ddb sync scope create|drop|list`
- `ddb sync bind|unbind`
- `ddb sync run`
- `ddb sync push`
- `ddb sync pull`
- `ddb sync status`
- `ddb sync conflicts`
- `ddb sync resolve`
- `ddb sync export`
- `ddb sync import`
- `ddb sync prune`
- `ddb sync doctor`

### 18.3 Exit Codes

CLI should differentiate at minimum:

- success
- transport failure
- compatibility failure
- conflict detected
- invalid configuration
- partial success

---

## 19. SDK Requirements

### 19.1 Initial Official SDK Targets

Assume the first official embedding surface is the core host language/runtime used by DecentDB plus one flagship external SDK (likely .NET if aligned with broader DecentDB priorities).

### 19.2 SDK Requirements

SDKs must:

- expose strongly-typed sync result objects
- avoid forcing shell-outs to CLI for standard operations
- support async APIs where appropriate
- provide configuration objects for peers, scopes, retries, and conflict behavior
- expose diagnostics and machine-readable status

### 19.3 Result Object Requirements

A sync run result should expose fields like:

- direction
- peer name
- session ID
- batches sent/received
- rows applied
- conflicts recorded
- checkpoint before/after
- elapsed time
- outcome code
- warnings

---

## 20. Phased Delivery Plan / Slices

This work is large enough that it **must** be implemented in slices.

## Slice 0 — Foundations and Design Hardening

### Objectives

- lock the conceptual model
- define the initial sync catalog and journal design
- define strict v1 scope rules
- define protocol envelope and versioning
- define safe initial conflict semantics

### Tasks

1. Create ADR for local-first sync architecture.
2. Create ADR for journal storage format and retention model.
3. Create ADR for protocol/version negotiation.
4. Create ADR for conflict semantics and safe default policy.
5. Define canonical system table schemas.
6. Define batch envelope JSON/binary contract.
7. Define CLI command grammar and exit codes.
8. Define initial observability fields and error taxonomy.
9. Define documentation outline before implementation begins.

### Deliverables

- ADR set
- protocol draft
- system catalog draft
- CLI/SQL API proposal
- test strategy draft
- docs outline

### Exit Criteria

- no major unresolved architectural contradictions
- all initial slices have agreed boundaries

---

## Slice 1 — Local Journal and Metadata Catalog

### Objectives

Implement local prerequisites for sync without networking yet.

### Tasks

1. Add replica initialization and identity storage.
2. Implement sync enablement flag/config.
3. Implement sync metadata catalog tables.
4. Implement durable journal capture for inserts/updates/deletes.
5. Implement tombstone recording.
6. Implement journal sequence numbering.
7. Implement journal enumeration APIs.
8. Implement status views for pending changes.
9. Implement local integrity checks for journal consistency.

### Deliverables

- working local journal
- SQL inspection views
- tests for crash safety and replay enumeration

### Exit Criteria

- local mutations are durably captured
- journal can be queried incrementally
- restart/crash tests pass

---

## Slice 2 — Manual Export/Import Sync

### Objectives

Provide a no-network sync path first to validate protocol and apply semantics.

### Tasks

1. Implement export of change batches to file.
2. Implement import/apply of change batches from file.
3. Implement batch identity and idempotent re-import behavior.
4. Implement strict protocol/version validation.
5. Implement initial conflict recording.
6. Implement CLI commands for export/import/status/conflicts.
7. Implement JSON diagnostic outputs.

### Deliverables

- file-based offline sync
- conflict recording and inspection
- version compatibility checks

### Exit Criteria

- two replicas can exchange changes manually
- replay is safe
- conflict scenarios are visible and test-covered

---

## Slice 3 — HTTP Transport and Peer Management

### Objectives

Add first official online transport with peer definitions and resumable sync sessions.

### Tasks

1. Implement peer catalog and credential references.
2. Implement HTTP transport adapter.
3. Implement handshake and capability negotiation.
4. Implement push/pull/bidirectional operations.
5. Implement watermarks/checkpoints per peer.
6. Implement retry behavior for transient failures.
7. Implement structured session records.
8. Implement log redaction for secrets.

### Deliverables

- online sync over HTTPS
- checkpointed incremental sync
- peer management commands

### Exit Criteria

- repeated syncs are incremental
- failed sessions can be retried safely
- peer status is inspectable

---

## Slice 4 — Scoped Sync

### Objectives

Allow partial replication with validated table and row filters.

### Tasks

1. Implement scope catalog objects.
2. Implement table inclusion rules.
3. Implement restricted row-filter validation.
4. Bind peers to scopes.
5. Ensure capture/export/apply all respect scope.
6. Add error messages for unsupported scope definitions.
7. Add tests for correctness and data leakage prevention.

### Deliverables

- scoped sync v1
- validation rules
- examples for tenant/user/device subsets

### Exit Criteria

- scoped peers receive only intended data
- invalid scopes are rejected clearly

---

## Slice 5 — Conflict Resolution Workflows

### Objectives

Move from recording conflicts to managing them usefully.

### Tasks

1. Implement configurable conflict policies.
2. Implement manual conflict inspection and resolution commands.
3. Implement structured conflict payload storage.
4. Implement per-session stop/continue conflict modes.
5. Add SDK result surfaces for conflict details.
6. Add tests for update/update, delete/update, uniqueness, and FK conflicts.

### Deliverables

- operational conflict workflows
- richer conflict diagnostics
- initial policy configuration support

### Exit Criteria

- conflicts are not just detected but manageable
- documented manual resolution workflow exists

---

## Slice 6 — Doctor, Retention, and Operational Hardening

### Objectives

Make the system supportable in production-like use.

### Tasks

1. Implement `ddb sync doctor`.
2. Implement retention/pruning with safety checks.
3. Implement backlog and watermark lag reporting.
4. Implement compatibility warnings for schema drift.
5. Improve performance diagnostics and session summaries.
6. Add maintenance guidance and safe/unsafe prune modes.

### Deliverables

- doctor/advisor tooling
- retention management
- production guidance

### Exit Criteria

- operator can understand health and recover from common issues

---

## Slice 7 — SDK Polish and Developer Experience

### Objectives

Make local-first sync pleasant to integrate from real apps.

### Tasks

1. Finalize flagship SDK API surface.
2. Add convenience wrappers for peer/scope/session operations.
3. Add typed result objects and exceptions/error codes.
4. Add end-to-end samples in official SDK.
5. Add agent-friendly JSON status helpers where needed.

### Deliverables

- polished SDK integration experience
- real app examples

### Exit Criteria

- developers can integrate sync without relying primarily on shell commands

---

## Slice 8 — Documentation and Example Completion Gate

### Objectives

Ship comprehensive docs and examples that make the feature learnable and adoptable.

### Tasks

1. Write conceptual overview: what local-first sync is and is not.
2. Write quickstart: two databases syncing locally or over HTTP.
3. Write CLI reference for all sync commands.
4. Write SQL function/reference docs.
5. Write architecture/mental model guide.
6. Write scopes guide with allowed/disallowed examples.
7. Write conflict resolution guide with workflows.
8. Write schema migration and compatibility guide.
9. Write security guide for sync peers and secrets.
10. Write troubleshooting guide and doctor output explanations.
11. Write performance and retention guide.
12. Build sample apps:
    - desktop app sync to central peer
    - per-tenant scoped sync sample
    - manual export/import offline sample
    - conflict demo sample
13. Add copy-pasteable examples for SQL, CLI, and SDK.
14. Add diagrams for session flow, journaling, and conflict workflow.
15. Add FAQ based on likely user confusion.

### Deliverables

- complete docs set
- multiple runnable samples
- example datasets and walkthroughs

### Exit Criteria

- docs are sufficient for a new developer to succeed without reading engine code
- samples validate major workflows end to end

---

## 21. Documentation Requirements (Mandatory)

This section is intentionally explicit. Documentation is not optional or “later.”

### 21.1 Required Documentation Set

1. **Feature Overview**
   - what problem this solves
   - why local-first sync is different from backup/replication/CDC
   - supported topologies

2. **Quickstart**
   - initialize replica
   - add peer
   - sync two DBs
   - inspect status/conflicts

3. **Concepts Guide**
   - replicas
   - peers
   - scopes
   - journals
   - watermarks
   - tombstones
   - conflicts

4. **CLI Reference**
   - syntax, flags, examples, exit codes, JSON outputs

5. **SQL Reference**
   - all sync functions, pragmas, and system views

6. **SDK Guide**
   - peer config
   - running sync
   - reading results
   - handling conflicts

7. **Scopes Guide**
   - valid/invalid row filters
   - table inclusion patterns
   - tenant/user scoping examples

8. **Conflict Guide**
   - types of conflicts
   - default behavior
   - manual resolution examples
   - policy examples

9. **Schema Compatibility Guide**
   - sync-safe migrations
   - rolling upgrades
   - handling incompatible peers

10. **Operations Guide**
    - retention
    - doctor
    - status interpretation
    - maintenance practices

11. **Security Guide**
    - transport security
    - secret handling
    - redaction expectations

12. **Troubleshooting Guide**
    - common errors
    - backlog growth
    - repeated replays
    - version mismatch
    - scope validation failures

13. **Example Gallery**
    - runnable, complete examples
    - expected outputs
    - before/after states

### 21.2 Documentation Quality Bar

All docs must:

- include complete examples
- show both success and failure modes where relevant
- avoid hand-wavy wording
- link concepts to commands and APIs
- call out limitations and deferred features clearly

---

## 22. Testing Strategy

### 22.1 Unit Tests

Cover:

- journal capture
- sequence/watermark math
- scope validation
- protocol envelope parsing
- conflict detection
- resolution application
- retention rules

### 22.2 Integration Tests

Cover:

- two-replica sync lifecycle
- export/import sync
- push/pull over HTTP
- reconnect after failure
- scope-restricted sync
- schema incompatibility rejection
- crash/restart during journal/apply

### 22.3 End-to-End Tests

Cover:

- realistic app scenario with offline writes and later sync
- conflict-heavy scenario
- maintenance/doctor scenario

### 22.4 Property / Fuzz Testing

Strongly recommended for:

- apply idempotency
- replay ordering safety
- conflict detection edges
- malformed batch import/protocol payloads

### 22.5 Performance Tests

Cover:

- overhead of sync enabled vs disabled
- sync throughput by batch size
- prune behavior under backlog
- scoped vs full sync

### 22.6 Compatibility Tests

Cover:

- version negotiation
- older/newer peer behavior
- blocked sync on incompatible contracts

### 22.7 Chaos / Fault Injection Tests

Strongly recommended for production readiness:

| Scenario | Expected Behavior |
|----------|-------------------|
| Kill process during journal write | Journal either contains complete entry or none; no partial writes |
| Kill process during batch apply | Apply is atomic; on restart, batch is either fully applied or not |
| Corrupt batch payload in transit | Batch rejected with `CORRUPT_PAYLOAD` error; no data corruption |
| Simulate network latency spikes (5-30s) | Timeouts handled gracefully; session marked as failed, not hung |
| Simulate disk full during journal write | Transaction fails with clear error; no journal corruption |
| Simulate clock jump (forward/backward) | Logical timestamps unaffected; no incorrect conflict resolution |
| Simulate memory pressure | Batch processing degrades gracefully; OOM prevented |
| Interrupt during conflict resolution | Resolution state persisted; can resume resolution |

**Testing tools:**

- Use process kill signals (SIGKILL, SIGTERM) during operations
- Use network simulation tools (tc, toxiproxy) for latency/partition
- Use fault injection framework for deterministic fault points

---

## 23. Acceptance Criteria

The feature is acceptable for initial release only when all of the following are true:

1. A database can be initialized as a sync-capable replica.
2. Local mutations are durably captured in a journal.
3. Two replicas can exchange changes incrementally.
4. Replayed batches are handled safely and idempotently.
5. Deletions replicate correctly via tombstones.
6. Peer watermarks/checkpoints are durable and inspectable.
7. At least one official transport works reliably.
8. At least basic scoped sync works with explicit validation.
9. Conflicts are recorded and inspectable.
10. A manual conflict resolution workflow exists.
11. Sync status and health are available via CLI and SQL/system views.
12. Documentation includes quickstart, concepts, CLI/SQL reference, operations, troubleshooting, and runnable examples.
13. Test coverage includes crash safety, replay safety, scope correctness, and conflict scenarios.

---

## 24. Definition of Done

This feature is done only when:

- implementation slices committed for the release target are complete
- ADRs are merged
- CLI and SDK surfaces are stable enough for public preview or release
- test suites pass in CI
- performance baselines are recorded
- docs and examples are published and reviewed
- known limitations are explicitly documented

---

## 25. Open Design Questions

These should be resolved during Slice 0 and early implementation:

1. What is the most durable and efficient internal journal representation for DecentDB’s storage engine?
2. Should the initial protocol be JSON, binary, or both with a common logical envelope?
3. How strict should the initial schema compatibility model be?
4. What exact restrictions are acceptable for v1 row-filter scopes?
5. Should conflict policies be global, per-peer, per-scope, or per-table in v1?
6. How much of transport logic belongs in core vs companion library/tooling?

### 25.1 Resolved Recommendations

**Q1: Journal Representation**

**Recommendation:** Separate journal file with periodic compaction.

Rationale:
- Clear separation of concerns from main database
- Independent retention policies
- Easier debugging and inspection
- Compaction can run during low-activity periods

**Q2: Protocol Format**

**Recommendation:** JSON for v1, with binary format as future optimization.

Rationale:
- JSON is easier to debug during initial development
- Human-readable for troubleshooting
- Well-supported across all binding languages
- Binary optimization can be added later without protocol change

**Q3: Schema Compatibility Strictness**

**Recommendation:** Strict mode for v1.

Rationale:
- Simpler implementation and testing
- Clearer error messages
- Reduces edge cases in conflict detection
- Can relax to "compatible" mode in future versions

**Q4: Row-Filter Restrictions**

**Recommendation:** See Section 11.3.1 for detailed allowed/disallowed patterns.

Key restrictions:
- Single-table predicates only
- No subqueries or cross-table references
- No non-deterministic functions
- Must reference columns that exist in all included tables

**Q5: Conflict Policy Granularity**

**Recommendation:** Per-scope for v1.

Rationale:
- More flexible than global
- Simpler than per-table
- Aligns with scoped sync model
- Different tenants/use cases can have different policies

**Q6: Transport Logic Placement**

**Recommendation:** Core engine handles protocol logic; transport adapters handle bytes-over-wire only.

Rationale:
- Allows future WebSocket, gRPC, or custom transports
- Core remains testable without network
- Clear interface boundaries
- Transport adapters can be swapped per deployment

**Q7: Minimum SDK Surface**

**Recommendation:** .NET SDK with full sync API surface.

Rationale:
- Aligns with DecentDB's flagship binding priority
- Strong typing for sync result objects
- Async/await patterns for long-running sync operations
- Sufficient for real-world application integration

**Q8: Pruning Safety Rules**

**Recommendation:** Mandatory safety checks before any prune:

1. No prune if any peer watermark is within retention window
2. No prune of tombstones until all peers have acknowledged
3. `--allow-data-loss` flag required to override safety checks
4. Dry-run mode (`--dry-run`) shows what would be pruned
5. Audit log entry for every prune operation

---

## 26. Recommended Initial Priority Order

If implementation capacity is limited, prioritize in this order:

1. Slice 0 — Foundations
2. Slice 1 — Local Journal and Metadata Catalog
3. Slice 2 — Manual Export/Import Sync
4. Slice 3 — HTTP Transport and Peer Management
5. Slice 5 — Conflict Resolution Workflows
6. Slice 4 — Scoped Sync
7. Slice 6 — Doctor and Operational Hardening
8. Slice 7 — SDK Polish
9. Slice 8 — Documentation Completion Gate

**Note:** Slice 8 is listed last for dependency purposes, but documentation work should begin in Slice 0 and progress alongside implementation.

---

## 27. Coding Agent Implementation Guidance

When assigning this SPEC to coding agents:

1. Do not ask an agent to “build all sync” in one pass.
2. Assign one slice at a time.
3. Require each slice to update docs and tests as part of the same PR.
4. Require ADR references in implementation PRs where architectural choices are made.
5. Require machine-readable outputs (`--json`) for all user-facing operational commands.
6. Require examples for any new SQL function, CLI command, or SDK method.
7. Require explicit handling of partial failure paths.
8. Prefer correctness, durability, and inspectability over premature optimization.

---

## 28. Suggested Companion ADRs

Create at minimum:

1. ADR — Local-First Sync Architecture
2. ADR — Change Journal Storage Model
3. ADR — Sync Protocol Envelope and Versioning
4. ADR — Conflict Detection and Resolution Defaults
5. ADR — Scoped Sync Restrictions for v1
6. ADR — Sync Retention and Pruning Safety Model
7. ADR — HTTP Transport Adapter Boundaries

---

## 29. Appendix: Example Documentation Topics and Samples

### 29.1 Example Samples to Build

1. **Desktop Task App**
   - local writes while offline
   - later sync to central peer

2. **Field Service App**
   - technician receives only assigned work orders via scope
   - local updates sync later

3. **Manual USB Transfer Demo**
   - export batch on air-gapped machine
   - import on connected machine

4. **Conflict Walkthrough**
   - two replicas edit same row
   - inspect conflict
   - resolve manually

### 29.2 Example Failure Cases to Document

- invalid row filter
- missing primary key on scoped table
- peer protocol mismatch
- incompatible schema version
- conflict caused by delete/update race
- prune blocked due to lagging peer

---

## 30. Final Positioning Statement

This feature should make DecentDB feel different from “just another embedded database with a changes feed.”

The end state should support a message like:

> **DecentDB is the embedded SQL database with native local-first sync, scoped replication, and conflict-aware merge workflows built in.**

That is the standard this SPEC is intended to implement.
