# Production Sync Relay And Public Changeset API

**Status:** Active spec - governing ADRs accepted, implementation pending
**Project:** DecentDB
**Document Type:** Implementation SPEC
**Audience:** Core engine developers, sync implementers, CLI maintainers, web
binding maintainers, SDK maintainers, documentation authors, release engineers,
coding agents
**Related Roadmap Item:** `design/FUTURE_WINS.md` priority #1
**Governing ADRs:**
[`adr/0166-production-sync-relay-boundary-and-identity.md`](adr/0166-production-sync-relay-boundary-and-identity.md),
[`adr/0167-public-changeset-api.md`](adr/0167-public-changeset-api.md),
[`adr/0168-sync-shape-streaming-subscriptions.md`](adr/0168-sync-shape-streaming-subscriptions.md)
**Related Inputs:**
[`WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`](WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md),
[`WIN_REACTIVE_QUERY_SUBSCRIPTIONS_CHANGE_STREAMS_SPEC.md`](WIN_REACTIVE_QUERY_SUBSCRIPTIONS_CHANGE_STREAMS_SPEC.md),
[`WIN_PRODUCTION_BROWSER_RUNTIM.md`](WIN_PRODUCTION_BROWSER_RUNTIM.md),
ADR 0147-0152, ADR 0157-0158, ADR 0163-0165

---

## 1. Executive Summary

DecentDB already has the engine foundation for local-first sync:

- durable local sync journal capture;
- manual export/import;
- HTTP development transport;
- peers and sessions;
- scoped sync;
- conflict workflows;
- retention and doctor reporting;
- .NET SDK sync bridge;
- documentation and examples.

That foundation proves DecentDB can capture, exchange, inspect, and resolve
changes. It does not yet provide the production application surface that browser,
mobile, desktop, and SaaS clients need.

This spec defines the next layer:

1. a self-hosted production relay protocol with authenticated tenant/user
   identity;
2. a stable public changeset API that is not just the internal journal format;
3. durable shape subscriptions built on sync scopes;
4. HTTP and WebSocket delivery for browser and mobile clients;
5. compatibility checks for schema and query-contract drift;
6. retention, pruning, peer-lag, relay, and shape diagnostics through `sys.*`,
   CLI, SDK, and machine-readable JSON;
7. implementation slices that preserve DecentDB's one-writer/many-readers model
   and crash-recovery guarantees.

The intended end state:

```text
Applications can run DecentDB locally, subscribe to the data they are allowed to
see, exchange durable changesets with a relay, resume after disconnects, inspect
conflicts, and understand relay health without DecentDB becoming a hosted sync
service.
```

This work must build on shipped DecentDB-to-DecentDB sync semantics. It must not
replace them with a separate hosted-service queue, a generic import/export
pipeline, or application-specific middleware hidden inside the engine.

---

## 2. Why This Feature Exists

The shipped sync slices are enough for controlled developer workflows:

- initialize replicas;
- configure peers;
- export and import batches;
- run sync over HTTP;
- define deterministic scopes;
- inspect conflicts and retention state.

Production applications need a larger contract:

- users and tenants must be authenticated and authorized;
- browser and mobile clients need streaming plus resumable pulls;
- app code needs a stable changeset API instead of internal journal records;
- clients need to subscribe to a named data shape such as "tenant 42 tasks";
- operators need to see lag, retention blockers, stale clients, schema drift,
  replay failures, and conflict pressure;
- SDKs need typed or JSON request/response APIs that do not shell out to the
  CLI;
- sync must continue to be durable across crashes and process restarts.

Without this layer, each application team would have to build its own relay,
auth mapping, changeset format, WebSocket resume protocol, conflict envelope,
and retention policy. That duplicates the hardest parts of local-first sync and
creates incompatible ecosystems around the same engine.

---

## 3. Product Principles

### 3.1 Durable First

The relay must not acknowledge apply, ack, checkpoint advancement, or shape
progress until the corresponding metadata is durably recorded. Sync metadata
writes are part of DecentDB's durability story.

### 3.2 Engine Semantics Are Authoritative

The relay transports and authorizes sync. It does not invent independent
conflict, schema, retention, or apply semantics. DecentDB-to-DecentDB changeset
apply remains authoritative.

### 3.3 Self-Hosted, Not Hosted Service

This feature defines a production relay protocol and self-hosted implementation
surface. It is not a commitment to operate a DecentDB cloud service.

### 3.4 Identity Context Without Becoming IAM

The relay must know tenant and subject identity for authorization, diagnostics,
and audit context. DecentDB does not issue application identities or become an
identity provider.

### 3.5 Stable Public Changesets

Applications and bindings must not depend on raw internal journal lines. Public
changesets have their own versioned envelope, compatibility fields, and error
semantics.

### 3.6 Shapes Are Scoped Sync, Not Arbitrary Live SQL

The first production shape API is backed by validated sync scopes. Arbitrary
query-backed shapes, joins, aggregates, opaque predicates, and exact result-set
diffing are future work.

### 3.7 Browser Support Follows The Browser Runtime Contract

Browser sync must route through the production browser owner model. Service
workers do not own database handles. Unsupported browser capability must fail
explicitly.

### 3.8 No Hidden Multi-Writer Semantics

The relay does not change DecentDB's one-writer/many-readers model. It
coordinates durable sync, but it does not create multi-writer MVCC or
distributed consensus.

---

## 4. Goals

1. Define the production relay architecture and deployment boundary.
2. Add authenticated v2 sync relay protocol routes.
3. Add explicit tenant/user/device/service principal context to sync operations.
4. Define and implement public changeset creation, inspection, apply, and
   limited inversion.
5. Support changesets from checkpoint, branch, and snapshot boundaries.
6. Add durable shape subscriptions backed by existing sync scopes.
7. Support initial shape snapshots and resumable incremental streams.
8. Add WebSocket streaming with ack, heartbeat, lag, and resync semantics.
9. Keep HTTP pull/long-poll available for mobile or restricted environments.
10. Add schema and query-contract compatibility checks.
11. Extend retention/pruning so shape checkpoints and relay sessions are
    accounted for.
12. Expose relay and shape diagnostics through canonical `sys.*` surfaces.
13. Expose Rust, C ABI JSON, CLI, and SDK surfaces for changesets and relay
    operations.
14. Add tests for durability, replay, conflict behavior, browser routing, auth,
    compatibility, and network failure.
15. Publish user-facing docs and deployment guidance.

---

## 5. Non-Goals

The first production relay implementation does not include:

1. DecentDB-operated hosted sync service.
2. Multi-region consensus or transparent HA clustering.
3. Cross-database distributed transactions.
4. External database adapters in core sync.
5. General import/export or conversion workflows.
6. Arbitrary SQL query shapes.
7. Field-level CRDT semantics.
8. Service-worker-owned browser database handles.
9. Broad full-mesh peer discovery.
10. A general durable job queue.
11. Text-to-SQL or LLM execution in the engine.
12. A requirement that every binding gain a fully typed wrapper in the first
    slice. The JSON bridge is the baseline.

---

## 6. Existing Foundations

This feature is intentionally follow-on work. It must reuse these shipped or
accepted foundations:

| Foundation | Source | Required Reuse |
|---|---|---|
| Local sync journal | ADR 0147 | Changesets created from checkpoints read the durable sync journal. |
| HTTP sync transport and peers | ADR 0148 | v2 relay builds on peer/session concepts while keeping engine HTTP-free. |
| Scoped sync v1 | ADR 0149 | Shapes are backed by sync scopes in the first implementation. |
| Conflict workflows | ADR 0150 | Changeset apply records and resolves conflicts through existing workflow state. |
| Operational hardening | ADR 0151 | Relay retention, lag, and doctor output extend existing sync reporting. |
| .NET JSON bridge | ADR 0152 | Changeset and relay SDK work should follow the JSON bridge pattern. |
| Branch diff and merge | ADR 0157 | Branch/snapshot changesets reuse deterministic row diff semantics. |
| Branch-sync interaction | ADR 0158 | Branch metadata remains local; merge into `main` remains the sync-visible path. |
| Operational `sys.*` metrics | ADR 0163 | New diagnostics use canonical dotted `sys.*` names. |
| Reactive subscriptions | ADR 0164 | Reactive events may wake streams but are not durable source of truth. |
| Browser runtime | ADR 0165 | Browser sync routes through the owner model and service-worker policy. |
| Relay boundary | ADR 0166 | Relay is self-hosted, identity-aware, and outside core HTTP dependencies. |
| Public changeset API | ADR 0167 | Changesets are stable logical envelopes. |
| Shape streaming | ADR 0168 | Shapes are durable scoped subscriptions with ack/resume semantics. |

---

## 7. Definitions

**Relay:** A self-hosted process or service that authenticates clients, maps
requests to DecentDB sync principals, and transports changesets between clients
and a backing DecentDB peer.

**Backing database:** The DecentDB database opened by the relay for the data
being synchronized.

**Principal:** The authenticated tenant/user/device/service context attached to
a relay request.

**Tenant:** Application-level isolation boundary. Production relay requests must
have a tenant ID.

**Subject:** Authenticated actor inside a tenant, such as a user, service
account, automation process, or device.

**Changeset:** A public, versioned logical envelope containing data changes,
compatibility metadata, and apply/idempotency fields.

**Checkpoint:** Durable progress marker used to resume incremental changeset
creation or shape streaming.

**Shape:** A named public subscription contract backed by a sync scope.

**Shape checkpoint:** Durable client progress marker for a shape stream.

**Shape stream:** HTTP or WebSocket delivery of shape snapshot and incremental
changesets with ack/resume behavior.

**Compatibility contract:** Schema, query, sync, and feature metadata that must
match or be judged compatible before a changeset can be applied.

**Relay session:** Durable record of a production relay interaction, including
principal, requested operation, compatibility result, checkpoint progress, and
outcome.

---

## 8. Target User Experience

### 8.1 Relay Deployment

An application operator can run a relay against a DecentDB database:

```bash
decentdb relay serve \
  --db app.ddb \
  --listen 127.0.0.1:8080 \
  --public-url https://sync.example.com \
  --auth-jwks-url https://auth.example.com/.well-known/jwks.json \
  --require-tls \
  --json
```

In production, TLS can terminate at a reverse proxy. The relay still needs to
know whether the external request is secure, either through direct TLS or a
trusted forwarded-proto configuration.

### 8.2 Changeset Workflow

Operators and tools can create and inspect changesets:

```bash
decentdb sync changeset create \
  --db app.ddb \
  --from-checkpoint central:1242 \
  --scope tenant_42 \
  --output .tmp/tenant-42-1242-1300.dcs.json

decentdb sync changeset inspect \
  --input .tmp/tenant-42-1242-1300.dcs.json \
  --json

decentdb sync changeset apply \
  --db replica.ddb \
  --input .tmp/tenant-42-1242-1300.dcs.json \
  --conflict-policy record \
  --json
```

Branch and snapshot sources are also supported when the underlying diff is
safe:

```bash
decentdb sync changeset create \
  --db app.ddb \
  --from-branch main \
  --to-branch migration-test \
  --output .tmp/migration-test.dcs.json

decentdb sync changeset create \
  --db app.ddb \
  --from-snapshot before-import \
  --to-branch main \
  --output .tmp/import-result.dcs.json
```

### 8.3 Shape Subscription Workflow

Application operators define sync scopes, then publish shapes from those scopes:

```bash
decentdb sync scope create app.ddb tenant_42_tasks \
  --include tasks,task_comments \
  --row-filter "tenant_id = 42"

decentdb relay shape create \
  --db app.ddb \
  --shape tenant_42_tasks_v1 \
  --scope tenant_42_tasks \
  --tenant tenant_42 \
  --allow-role user \
  --json
```

A browser or mobile client subscribes to a shape:

```json
{
  "type": "subscribe_shape",
  "request_id": "req_123",
  "shape_id": "tenant_42_tasks_v1",
  "mode": "snapshot",
  "client_replica_id": "web_7f2a",
  "last_ack_checkpoint": null
}
```

The relay responds with a snapshot changeset and a checkpoint. Incremental
changesets follow over WebSocket or HTTP pull.

---

## 9. Architecture

### 9.1 Component Overview

```text
browser/mobile/native client
        |
        | HTTPS / WebSocket
        v
production relay
        |
        | principal context + sync API calls
        v
DecentDB engine
        |
        | durable journal, metadata, conflicts, retention
        v
database + sync sidecar
```

### 9.2 Core Engine Responsibilities

The core engine implements:

- public changeset structs and JSON parsing/serialization;
- checkpoint-based changeset export;
- branch/snapshot changeset export through diff APIs;
- transactional changeset apply;
- changeset inspection and limited inversion;
- compatibility checking;
- conflict metadata and resolution hooks;
- relay session and shape checkpoint metadata;
- retention blockers for shape clients;
- canonical `sys.*` diagnostics;
- C ABI JSON request/response functions.

The engine must not:

- depend on HTTP or WebSocket libraries;
- issue application auth tokens;
- own TLS configuration;
- store raw bearer tokens;
- run network event loops.

### 9.3 Relay Responsibilities

The relay implements:

- HTTP and WebSocket routes;
- credential validation;
- principal construction;
- authorization checks for shapes/scopes/operations;
- request size limits and rate limits;
- response compression;
- stream queueing and backpressure;
- heartbeat and timeout handling;
- transport-level logs with secret redaction;
- production deployment checks;
- CLI entry points.

### 9.4 Browser Runtime Responsibilities

The browser package implements:

- owner-routed sync API calls per ADR 0165;
- WebSocket or HTTP transport from supported browser contexts;
- stable errors for unsupported service-worker ownership;
- client-side checkpoint persistence using DecentDB when available;
- typed or JSON changeset consumption APIs;
- browser diagnostics surfaced through existing browser runtime metrics.

### 9.5 SDK Responsibilities

SDKs expose:

- changeset create/apply/inspect APIs;
- relay client configuration;
- shape subscribe APIs where runtime support exists;
- raw JSON fallback for agents and less mature bindings;
- typed result objects for flagship SDKs.

The C ABI JSON bridge is the stable baseline. Fully typed wrappers can be
incremental.

---

## 10. Principal And Authorization Model

### 10.1 Principal Shape

The relay constructs a principal for every production request:

```json
{
  "tenant_id": "tenant_42",
  "subject_id": "user_123",
  "subject_kind": "user",
  "auth_issuer": "https://auth.example.com",
  "roles": ["user"],
  "allowed_scopes": ["tenant_42_tasks"],
  "allowed_shapes": ["tenant_42_tasks_v1"],
  "session_id": "sess_01hy...",
  "request_id": "req_01hy..."
}
```

Required fields:

- `tenant_id`;
- `subject_id`;
- `subject_kind`;
- `session_id`;
- `request_id`.

The relay may derive `allowed_scopes` and `allowed_shapes` from token claims,
configuration, lookup tables, or application callbacks. The engine receives the
resolved context, not the raw auth token.

### 10.2 Subject Kinds

Supported subject kinds:

- `user`;
- `device`;
- `service`;
- `agent`.

`agent` is for application-authorized automation. It does not imply LLM
execution inside DecentDB.

### 10.3 Authorization Checks

Before invoking engine changeset operations, the relay checks:

- principal has the requested tenant;
- requested shape belongs to tenant;
- requested scope is allowed;
- requested peer operation is allowed;
- requested conflict policy is allowed;
- requested batch size and retention mode are allowed;
- requested branch/snapshot changeset source is allowed.

The engine still validates:

- scope exists and is legal;
- shape maps to scope;
- changeset scope/shape metadata matches request context;
- schema/query compatibility;
- primary-key and row-filter safety;
- apply conflict behavior;
- durable metadata writes.

### 10.4 Audit Context

Relay operations record audit context in session metadata:

- tenant ID;
- subject ID and kind;
- shape or scope;
- remote address hash where configured;
- request ID;
- user agent string hash or client runtime tag;
- operation;
- result code;
- rows/bytes sent or applied;
- conflict count;
- checkpoint before/after.

Raw credentials are never stored in database metadata.

---

## 11. Public Changeset Contract

### 11.1 Envelope

The initial JSON envelope:

```json
{
  "changeset_version": 1,
  "changeset_id": "dcs_01hy6h8k9m7x4vq9v3v7q2c6k1",
  "source_replica_id": "relay-central",
  "source_kind": "checkpoint",
  "tenant_id": "tenant_42",
  "scope_name": "tenant_42_tasks",
  "shape_id": "tenant_42_tasks_v1",
  "base_kind": "checkpoint",
  "base_checkpoint": {
    "peer": "web_7f2a",
    "sequence": 1242
  },
  "start_checkpoint": 1243,
  "end_checkpoint": 1300,
  "source_high_watermark": 1300,
  "schema_fingerprint": "sha256:...",
  "schema_cookie": 17,
  "sync_contract_version": 1,
  "query_contract_fingerprint": null,
  "producer_capabilities": {
    "before_images": false,
    "compression": ["none", "zstd"],
    "conflict_policies": ["record", "stop", "last_writer_wins", "origin_priority"]
  },
  "limits": {
    "record_count": 58,
    "uncompressed_bytes": 23851
  },
  "records": [],
  "created_at_micros": 1779292800000000,
  "integrity_hash": "sha256:..."
}
```

Field rules:

- `changeset_version` is required and starts at `1`.
- `changeset_id` is globally unique enough for idempotency. Use a sortable
  unique identifier format where available.
- `source_kind` is one of `checkpoint`, `branch`, or `snapshot`.
- `tenant_id` is required when created under relay context.
- `scope_name` or `shape_id` is required for scoped production relay delivery.
- `source_high_watermark` allows scoped consumers to advance past scanned
  out-of-scope records.
- `integrity_hash` hashes the canonical envelope excluding the hash field.

### 11.2 Record Shape

Example insert/update record:

```json
{
  "record_version": 1,
  "table": "tasks",
  "operation": "update",
  "primary_key": { "tenant_id": 42, "id": 101 },
  "origin_replica_id": "desktop_9",
  "origin_sequence": 1300,
  "transaction_id": "txn_01hy...",
  "transaction_lsn": 4812,
  "schema_cookie": 17,
  "before_hash": "sha256:...",
  "before": null,
  "after": {
    "tenant_id": 42,
    "id": 101,
    "title": "Inspect pump",
    "status": "done"
  },
  "column_mask": ["title", "status"],
  "conflict_metadata": null
}
```

Example delete record:

```json
{
  "record_version": 1,
  "table": "tasks",
  "operation": "delete",
  "primary_key": { "tenant_id": 42, "id": 101 },
  "origin_replica_id": "desktop_9",
  "origin_sequence": 1301,
  "transaction_id": "txn_01hy...",
  "transaction_lsn": 4813,
  "schema_cookie": 17,
  "before_hash": "sha256:...",
  "before": null,
  "after": null,
  "tombstone": true,
  "conflict_metadata": null
}
```

### 11.3 Source Boundaries

#### Checkpoint Source

Checkpoint changesets read durable journal records after a peer checkpoint.
This is the default production sync path.

Inputs:

- peer or client replica ID;
- since checkpoint;
- optional scope or shape;
- max records;
- max bytes;
- requested compression.

Output:

- one changeset or a paged sequence of changesets;
- source high watermark;
- scoped skipped-record handling.

#### Branch Source

Branch changesets compare two branch states using ADR 0157 row diff semantics.

Inputs:

- source branch/head;
- target branch/head;
- optional scope;
- conflict policy hint.

Unsupported:

- tables without stable primary keys;
- schema divergence unless a later ADR allows specific additive cases;
- branch metadata replication.

#### Snapshot Source

Snapshot changesets compare two retained states. They are useful for support
bundles, review, and controlled promotion.

Inputs:

- from snapshot/head;
- to snapshot/head;
- optional scope;
- row diff options.

Snapshot changesets fail if the required retained state has been garbage
collected.

### 11.4 Apply Semantics

Default apply is atomic:

1. parse envelope;
2. validate version and integrity hash;
3. validate compatibility;
4. validate scope/shape/principal context;
5. validate all records for table, primary-key, and operation support;
6. begin transaction;
7. apply records through logical DML;
8. record conflicts or stop according to policy;
9. update watermarks and idempotency records;
10. record relay/session metadata;
11. commit durably;
12. return structured result.

If any validation step before mutation fails, no transaction is opened. If apply
fails during mutation in atomic mode, the transaction rolls back and watermarks
do not advance.

### 11.5 Idempotency

The engine records applied changesets by:

- `changeset_id`;
- source replica ID;
- source sequence range;
- scope/shape context;
- integrity hash.

Reapplying an identical changeset returns:

```json
{
  "outcome": "already_applied",
  "changeset_id": "dcs_...",
  "rows_applied": 0,
  "checkpoint_after": 1300
}
```

Reusing a `changeset_id` with a different integrity hash is rejected as
`CHANGESET_ID_COLLISION`.

### 11.6 Inspection

Inspection returns:

```json
{
  "changeset_id": "dcs_...",
  "valid_envelope": true,
  "source_kind": "checkpoint",
  "scope_name": "tenant_42_tasks",
  "shape_id": "tenant_42_tasks_v1",
  "record_count": 58,
  "tables": ["tasks", "task_comments"],
  "operations": {
    "insert": 12,
    "update": 44,
    "delete": 2
  },
  "schema_fingerprint": "sha256:...",
  "compatibility": {
    "checked_against_local_db": false,
    "status": "not_checked"
  },
  "warnings": []
}
```

When run against a local database, inspection may also check compatibility
without applying.

### 11.7 Inversion

Inversion returns a new changeset that reverses the original only when safe.

Supported cases:

- branch/snapshot source where both states are still readable;
- checkpoint changeset with full before images for all records;
- delete inversion only when before image is present.

Unsupported cases return:

```json
{
  "error_code": "CHANGESET_INVERSION_UNSUPPORTED",
  "reason": "record lacks before image",
  "record_index": 12
}
```

The engine must not infer before rows from the current database state because
that can be wrong after additional writes.

---

## 12. Relay Protocol V2

### 12.1 Route Namespace

All production relay routes live under:

```text
/decentdb/sync/v2
```

Existing v1 routes remain for current sync workflows.

### 12.2 HTTP Routes

Minimum routes:

| Method | Route | Purpose |
|---|---|---|
| `GET` | `/hello` | protocol negotiation and capability discovery |
| `POST` | `/sessions` | create authenticated relay session |
| `GET` | `/status` | relay, peer, retention, and compatibility status |
| `POST` | `/changesets/export` | create checkpoint/branch/snapshot changeset |
| `POST` | `/changesets/apply` | apply a changeset transactionally |
| `POST` | `/changesets/inspect` | inspect changeset without applying |
| `POST` | `/changesets/invert` | invert changeset when supported |
| `GET` | `/shapes` | list authorized shapes |
| `POST` | `/shapes/{shape_id}/snapshot` | create initial shape snapshot |
| `GET` | `/shapes/{shape_id}/changes` | pull changes since checkpoint |
| `POST` | `/acks` | acknowledge delivered shape changeset |
| `GET` | `/conflicts` | list conflicts visible to principal |
| `GET` | `/diagnostics` | machine-readable relay diagnostics |

### 12.3 WebSocket Route

```text
GET /decentdb/sync/v2/stream
```

The connection starts with a `hello` exchange, then one or more
`subscribe_shape` messages. A single connection may subscribe to multiple
shapes only if the relay configuration allows it and all shapes belong to the
same tenant/principal context.

### 12.4 Negotiation

`GET /hello` response:

```json
{
  "protocol_version": 2,
  "engine_version": "0.0.0-dev",
  "relay_id": "relay-central",
  "changeset_versions": [1],
  "shape_stream_versions": [1],
  "auth_required": true,
  "compression": ["none", "zstd"],
  "conflict_policies": ["record", "stop", "last_writer_wins", "origin_priority"],
  "features": {
    "checkpoint_changesets": true,
    "branch_changesets": true,
    "snapshot_changesets": true,
    "changeset_inversion": "conditional",
    "websocket_shapes": true,
    "http_shape_pull": true
  },
  "limits": {
    "max_changeset_bytes": 10485760,
    "max_records_per_changeset": 50000,
    "max_stream_queue_bytes": 16777216
  }
}
```

### 12.5 WebSocket Messages

#### Client hello

```json
{
  "type": "hello",
  "request_id": "req_1",
  "client_replica_id": "web_7f2a",
  "supported_changeset_versions": [1],
  "supported_shape_stream_versions": [1],
  "supported_compression": ["none"]
}
```

#### Subscribe

```json
{
  "type": "subscribe_shape",
  "request_id": "req_2",
  "shape_id": "tenant_42_tasks_v1",
  "mode": "resume",
  "last_ack_checkpoint": {
    "shape_sequence": 44,
    "source_high_watermark": 1300
  }
}
```

#### Changeset delivery

```json
{
  "type": "changeset",
  "request_id": "req_3",
  "shape_id": "tenant_42_tasks_v1",
  "shape_sequence": 45,
  "ack_deadline_micros": 1779292860000000,
  "checkpoint": {
    "shape_sequence": 45,
    "source_high_watermark": 1320
  },
  "changeset": {}
}
```

#### Ack

```json
{
  "type": "ack",
  "request_id": "req_4",
  "shape_id": "tenant_42_tasks_v1",
  "shape_sequence": 45,
  "source_high_watermark": 1320
}
```

#### Lagged

```json
{
  "type": "lagged",
  "shape_id": "tenant_42_tasks_v1",
  "last_ack_checkpoint": {
    "shape_sequence": 45,
    "source_high_watermark": 1320
  },
  "retention_still_available": true
}
```

#### Resync required

```json
{
  "type": "resync_required",
  "shape_id": "tenant_42_tasks_v1",
  "reason": "checkpoint_pruned",
  "minimum_available_watermark": 1500
}
```

### 12.6 Error Codes

Minimum stable v2 errors:

| Code | Meaning |
|---|---|
| `AUTH_REQUIRED` | Missing production relay credentials. |
| `AUTH_INVALID` | Credentials failed validation. |
| `AUTH_FORBIDDEN` | Principal is not allowed to perform operation. |
| `TENANT_REQUIRED` | Production request did not resolve to a tenant. |
| `PROTOCOL_UNSUPPORTED` | Protocol version mismatch. |
| `CHANGESET_UNSUPPORTED` | Changeset version or feature unsupported. |
| `CHANGESET_INVALID` | Malformed changeset envelope or records. |
| `CHANGESET_ID_COLLISION` | Same ID observed with different integrity hash. |
| `CHANGESET_ALREADY_APPLIED` | Replay detected and skipped safely. |
| `CHANGESET_INVERSION_UNSUPPORTED` | Safe inverse cannot be constructed. |
| `SCHEMA_INCOMPATIBLE` | Schema fingerprint or contract mismatch. |
| `QUERY_CONTRACT_INCOMPATIBLE` | Query-backed compatibility mismatch. |
| `SCOPE_UNAUTHORIZED` | Principal cannot access scope. |
| `SHAPE_NOT_FOUND` | Shape does not exist or is not visible to principal. |
| `SHAPE_RESYNC_REQUIRED` | Client checkpoint cannot be resumed. |
| `STREAM_BACKPRESSURE` | Stream queue exceeded configured limit. |
| `BATCH_TOO_LARGE` | Request or response exceeds limits. |
| `CONFLICT_RECORDED` | Apply recorded one or more conflicts. |
| `CONFLICT_POLICY_FORBIDDEN` | Requested conflict policy not allowed. |
| `INSECURE_TRANSPORT` | Production mode requires secure transport. |
| `RELAY_SHUTTING_DOWN` | Relay is draining and will not accept new work. |

---

## 13. Shape Model

### 13.1 Shape Catalog

Shape metadata should be stored in internal catalog objects such as:

- `__decentdb_sync_shapes`;
- `__decentdb_sync_shape_principals`;
- `__decentdb_sync_shape_clients`;
- `__decentdb_sync_shape_checkpoints`;
- `__decentdb_sync_shape_sessions`.

Exact table names may change to fit implementation conventions, but the data
model must support:

- shape identity;
- backing scope;
- tenant binding;
- principal selectors;
- client checkpoints;
- active lease/session metadata;
- retention blockers;
- stream configuration;
- diagnostics.

### 13.2 Shape Creation Validation

Creating a shape validates:

- backing scope exists;
- scope includes at least one table;
- all included tables still satisfy scope primary-key rules;
- tenant binding is present;
- shape name/ID is unique;
- retention policy is valid;
- stream limits are within global relay limits.

If the backing scope is invalidated by later schema changes, the shape remains
defined but reports `schema_incompatible` until repaired.

### 13.3 Shape Snapshot

A snapshot changeset includes all in-scope rows for a shape at a consistent
snapshot boundary.

Snapshot creation must:

- open a consistent read snapshot;
- enumerate included tables deterministically;
- respect row filter;
- include schema and sync contract metadata;
- produce one or more paged changesets when limits are exceeded;
- return a resume checkpoint corresponding to the source high watermark.

### 13.4 Shape Incremental Changes

Incremental changes are checkpoint changesets scoped to the shape's backing
scope.

The relay must use `source_high_watermark` to let clients advance past
out-of-scope records that were scanned but not delivered.

### 13.5 Shape Ack

An ack is durable only after the engine records:

- tenant ID;
- subject/client ID;
- shape ID;
- shape sequence;
- source high watermark;
- changeset ID;
- ack timestamp;
- relay session ID.

If ack persistence fails, the relay must ask the client to retry the ack or
redeliver the changeset. It must not advance memory-only state and pretend the
client checkpoint is durable.

### 13.6 Retention Interaction

Retention reports include shape clients as blockers:

- active client last ack;
- stale client last ack;
- retention deadline;
- unsafe prune impact;
- resync-required client count.

Unsafe prune may invalidate shape checkpoints only with explicit operator
override.

---

## 14. Schema And Query Compatibility

### 14.1 Schema Fingerprint

Changesets carry `schema_fingerprint` from the stable tooling metadata
contract. Apply checks it against the local database unless the caller selects a
documented compatibility mode.

Default mode is strict.

### 14.2 Sync Contract Version

The sync contract version captures:

- changeset envelope version;
- record operation support;
- tombstone semantics;
- conflict policy behavior;
- scope restriction set;
- compatibility mode.

Peers with incompatible sync contract versions cannot apply changesets.

### 14.3 Query Contract Fingerprint

For future query-backed shapes, the changeset carries a query-contract
fingerprint. The first shape implementation is scope-backed, so this field is
usually null. The field exists now so relay protocol negotiation can reject
query-backed changesets from future peers cleanly.

### 14.4 Compatibility Modes

Initial modes:

- `strict`: schema fingerprint and sync contract must match exactly.
- `inspect_only`: report compatibility without applying.

Deferred modes:

- additive-compatible schema changes;
- per-table compatibility;
- query-contract tolerant mode.

---

## 15. Public API Surface

### 15.1 Rust API

Candidate Rust types:

```rust
pub struct SyncPrincipal {
    pub tenant_id: String,
    pub subject_id: String,
    pub subject_kind: SyncSubjectKind,
    pub auth_issuer: Option<String>,
    pub roles: Vec<String>,
    pub allowed_scopes: Vec<String>,
    pub allowed_shapes: Vec<String>,
    pub session_id: String,
    pub request_id: String,
}

pub enum ChangesetSource {
    Checkpoint {
        peer: String,
        since_sequence: u64,
    },
    Branch {
        from: BranchRef,
        to: BranchRef,
    },
    Snapshot {
        from: SnapshotRef,
        to: StateRef,
    },
}

pub struct CreateChangesetOptions {
    pub source: ChangesetSource,
    pub scope_name: Option<String>,
    pub shape_id: Option<String>,
    pub max_records: Option<u64>,
    pub max_bytes: Option<u64>,
    pub principal: Option<SyncPrincipal>,
}

pub struct ApplyChangesetOptions {
    pub principal: Option<SyncPrincipal>,
    pub conflict_policy: Option<SyncConflictPolicy>,
    pub compatibility_mode: SyncCompatibilityMode,
    pub atomic: bool,
}

impl Db {
    pub fn sync_create_changeset(
        &self,
        options: CreateChangesetOptions,
    ) -> Result<SyncChangeset>;

    pub fn sync_apply_changeset(
        &mut self,
        changeset: &SyncChangeset,
        options: ApplyChangesetOptions,
    ) -> Result<SyncChangesetApplyResult>;

    pub fn sync_inspect_changeset(
        &self,
        changeset: &SyncChangeset,
        options: InspectChangesetOptions,
    ) -> Result<SyncChangesetInspection>;

    pub fn sync_invert_changeset(
        &self,
        changeset: &SyncChangeset,
        options: InvertChangesetOptions,
    ) -> Result<SyncChangeset>;
}
```

Names may change during implementation, but the API must preserve the contract:
create, apply, inspect, invert.

### 15.2 C ABI JSON API

Candidate C ABI functions:

```c
ddb_status ddb_sync_changeset_create_json(
    ddb_database* db,
    const char* request_json,
    char** out_json);

ddb_status ddb_sync_changeset_apply_json(
    ddb_database* db,
    const char* request_json,
    char** out_json);

ddb_status ddb_sync_changeset_inspect_json(
    ddb_database* db,
    const char* request_json,
    char** out_json);

ddb_status ddb_sync_changeset_invert_json(
    ddb_database* db,
    const char* request_json,
    char** out_json);
```

All returned strings are freed with `ddb_string_free`. All C ABI functions must
follow ADR 0118 panic-safety rules.

### 15.3 CLI API

Commands:

```bash
decentdb sync changeset create
decentdb sync changeset apply
decentdb sync changeset inspect
decentdb sync changeset invert

decentdb relay serve
decentdb relay status
decentdb relay doctor

decentdb relay shape create
decentdb relay shape list
decentdb relay shape drop
decentdb relay shape status
decentdb relay shape snapshot
```

All commands that emit structured data must support `--json`.

### 15.4 SQL Inspection

Canonical surfaces:

```sql
SELECT * FROM sys.sync_relay_status;
SELECT * FROM sys.sync_relay_sessions;
SELECT * FROM sys.sync_shapes;
SELECT * FROM sys.sync_shape_clients;
SELECT * FROM sys.sync_peer_lag;
SELECT * FROM sys.sync_retention;
SELECT * FROM sys.sync_changeset_history;
```

These are read-only inspection surfaces. They must not create recursive
telemetry writes.

### 15.5 SDK APIs

SDKs should expose:

- raw JSON changeset bridge;
- typed changeset models where mature;
- relay client configuration;
- shape subscription streams where runtime supports it;
- diagnostics and result objects.

The .NET SDK should be the first fully typed external SDK because it already
has sync JSON bridge precedent.

---

## 16. Relay CLI And Configuration

### 16.1 Configuration File

Example:

```toml
[relay]
database = "app.ddb"
listen = "127.0.0.1:8080"
public_url = "https://sync.example.com"
require_tls = true
trusted_forwarded_proto = true

[auth]
mode = "jwt"
jwks_url = "https://auth.example.com/.well-known/jwks.json"
issuer = "https://auth.example.com"
audience = "decentdb-sync"
tenant_claim = "tenant_id"
subject_claim = "sub"
roles_claim = "roles"

[limits]
max_changeset_bytes = 10485760
max_records_per_changeset = 50000
max_stream_queue_bytes = 16777216
ack_timeout_seconds = 30
heartbeat_seconds = 20

[retention]
default_shape_ttl_days = 30
stale_client_ttl_days = 14
```

### 16.2 Startup Checks

Relay startup validates:

- database can open;
- sync is enabled or can be enabled with explicit flag;
- replica ID exists;
- auth configuration is valid;
- TLS posture is production-safe or explicitly overridden;
- requested shapes/scopes exist when configured;
- max payload sizes are within engine limits;
- `sys.*` diagnostics can be queried.

### 16.3 Shutdown Behavior

On shutdown, relay:

- stops accepting new sessions;
- sends `RELAY_SHUTTING_DOWN` to stream clients;
- gives active apply operations a bounded drain window;
- records relay session endings;
- does not advance unacked shape checkpoints.

---

## 17. Diagnostics

### 17.1 Relay Status

`sys.sync_relay_status` fields:

| Field | Type | Meaning |
|---|---|---|
| `relay_id` | TEXT | Stable relay identity. |
| `protocol_version` | INT64 | Production relay protocol version. |
| `database_replica_id` | TEXT | Backing database replica ID. |
| `production_mode` | BOOL | Whether production checks are enforced. |
| `secure_transport_required` | BOOL | Whether TLS/trusted proxy is required. |
| `insecure_override_enabled` | BOOL | Whether insecure override is active. |
| `active_sessions` | INT64 | Current active relay sessions. |
| `active_streams` | INT64 | Current active WebSocket streams. |
| `started_at_micros` | INT64 | Relay process start time. |

### 17.2 Shape Clients

`sys.sync_shape_clients` fields:

| Field | Type | Meaning |
|---|---|---|
| `shape_id` | TEXT | Shape identity. |
| `tenant_id` | TEXT | Tenant binding. |
| `client_replica_id` | TEXT | Client replica ID. |
| `subject_id` | TEXT | Last subject ID. |
| `last_ack_sequence` | INT64 | Last acked shape sequence. |
| `last_ack_watermark` | INT64 | Last acked source high watermark. |
| `last_seen_at_micros` | INT64 | Last client activity. |
| `retention_blocking` | BOOL | Whether this client blocks prune. |
| `status` | TEXT | `active`, `stale`, `lagged`, or `resync_required`. |

### 17.3 Changeset History

`sys.sync_changeset_history` fields:

| Field | Type | Meaning |
|---|---|---|
| `changeset_id` | TEXT | Changeset identity. |
| `source_replica_id` | TEXT | Origin replica. |
| `source_kind` | TEXT | `checkpoint`, `branch`, or `snapshot`. |
| `scope_name` | TEXT | Scope if present. |
| `shape_id` | TEXT | Shape if present. |
| `record_count` | INT64 | Records in changeset. |
| `bytes` | INT64 | Uncompressed bytes. |
| `created_at_micros` | INT64 | Creation time. |
| `applied_at_micros` | INT64 | Apply time if applied locally. |
| `outcome` | TEXT | Creation/apply outcome. |

### 17.4 Doctor Guidance

`decentdb relay doctor --json` should report:

- insecure deployment configuration;
- missing tenant mapping;
- invalid shape/scope definitions;
- stale shape clients blocking retention;
- retention pressure;
- peer lag;
- unresolved conflicts;
- schema incompatibility;
- query-contract incompatibility;
- repeated replay/idempotency failures;
- stream backpressure;
- recent apply failures.

---

## 18. Retention And Pruning

### 18.1 Retention Inputs

Safe prune considers:

- peer watermarks;
- imported remote watermarks;
- unresolved conflicts;
- relay sessions in progress;
- active shape client checkpoints;
- stale shape clients still within TTL;
- operator-configured retention windows.

### 18.2 Prune Modes

Existing prune modes remain:

- safe default;
- dry run;
- explicit unsafe override.

Unsafe override must report:

- which shape clients will require resync;
- which peers may lose incremental resume;
- how many journal records and tombstones are affected;
- whether unresolved conflicts reference pruned data.

### 18.3 Shape Resync

When a client checkpoint is pruned, the relay must:

- mark the client `resync_required`;
- reject resume from the old checkpoint;
- require a fresh snapshot mode subscription;
- expose the condition through diagnostics.

---

## 19. Security Requirements

1. Production relay routes require authentication by default.
2. Tenant ID is required for production relay requests.
3. Raw bearer tokens, API keys, and credentials are never written to sync
   metadata.
4. Logs and doctor output redact sensitive values.
5. Production mode requires TLS or trusted internal boundary with explicit
   configuration.
6. Insecure overrides are visible in diagnostics.
7. Shape and scope authorization is checked before changeset creation.
8. Changeset apply validates scope/shape metadata before mutation.
9. Request size limits are enforced before full materialization where possible.
10. Compression is bounded to avoid decompression bombs.
11. Replay/idempotency checks reject changeset ID collisions.
12. C ABI JSON surfaces preserve panic-safety guarantees.

Deferred security work:

- signed changeset verification;
- encrypted changeset payloads;
- mTLS-specific relay mode;
- key rotation workflows;
- row/column policy integration from the security roadmap item.

---

## 20. Performance Requirements

Initial targets:

| Metric | Target |
|---|---:|
| Checkpoint changeset creation | 10,000 records/second local, no network |
| Changeset apply | 5,000 records/second local, no conflicts |
| Shape resume lookup | < 50 ms for 10,000 shape clients |
| WebSocket ack persistence | < 20 ms p50 local |
| Relay status query | < 50 ms under 1M journal records |
| Sync-disabled overhead | no change from shipped sync-disabled path |

Constraints:

- no unbounded stream queues;
- no full-history replay for routine resume;
- no always-on tracing writes;
- no core engine HTTP/WebSocket dependencies;
- no browser-specific checks in native hot paths.

Benchmarks should cover:

- changeset JSON encode/decode;
- large scoped export;
- paged changeset creation;
- apply with and without conflicts;
- shape snapshot creation;
- WebSocket delivery under backpressure;
- retention reports with many shape clients.

---

## 21. Reliability Requirements

### 21.1 Crash Safety

The system must preserve:

- local committed writes are durably captured according to existing sync
  journal rules;
- changeset apply and watermark advancement are atomic;
- shape ack and checkpoint advancement are durable before acknowledgement;
- relay restart can resume from durable session/checkpoint metadata;
- interrupted apply can be retried safely.

### 21.2 Network Failure

When a client disconnects:

- unacked changesets remain unadvanced;
- acked checkpoints remain durable;
- relay session records show disconnect outcome;
- client can resume from last ack if retained;
- client receives resync-required if not retained.

### 21.3 Partial Apply

Atomic apply remains default. Non-default partial/continue modes must clearly
report:

- rows attempted;
- rows applied;
- rows conflicted;
- rows skipped;
- rollback status;
- checkpoint advancement status.

### 21.4 Backpressure

Relay queues are bounded. A slow client cannot force unbounded memory growth or
block unrelated clients indefinitely.

---

## 22. Implementation Slices

This feature must be implemented in slices. Do not assign the whole spec to one
agent or one PR.

### Slice 0 - Design And Test Harness

Objectives:

1. Land governing ADRs.
2. Land this spec.
3. Define JSON schemas for changeset and v2 protocol messages.
4. Add fixture-based schema validation tests.
5. Add protocol test harness utilities under existing test conventions.

Deliverables:

- ADR 0166, ADR 0167, ADR 0168;
- `WIN_PRODUCTION_RELAY_SPEC.md`;
- JSON fixtures in an appropriate tests/resources location;
- test helpers for changeset round trips.

Exit criteria:

- docs are linked from `FUTURE_WINS.md`;
- JSON fixture tests pass;
- no production code behavior changes required.

### Slice 1 - Public Changeset Core

Objectives:

1. Add typed changeset structs.
2. Implement JSON serialize/deserialize with stable schema version.
3. Implement checkpoint changeset creation.
4. Implement changeset inspection.
5. Implement transactional apply through existing sync import/apply paths.
6. Implement idempotency records.
7. Add Rust and CLI APIs.

Deliverables:

- `Db::sync_create_changeset`;
- `Db::sync_inspect_changeset`;
- `Db::sync_apply_changeset`;
- `decentdb sync changeset create|inspect|apply`;
- tests for replay, malformed input, and atomic rollback.

Exit criteria:

- checkpoint changesets can replace manual batch export/import for supported
  cases;
- reapply is safe;
- conflicts are recorded through existing workflows.

### Slice 2 - Branch And Snapshot Changesets

Objectives:

1. Reuse branch/snapshot diff semantics.
2. Create branch changesets where row diff is supported.
3. Create snapshot changesets where retained states exist.
4. Reject unsupported tables/schema divergence with stable errors.
5. Implement conditional inversion.

Deliverables:

- `ChangesetSource::Branch`;
- `ChangesetSource::Snapshot`;
- `Db::sync_invert_changeset`;
- `decentdb sync changeset invert`;
- branch/snapshot fixtures and tests.

Exit criteria:

- safe branch/snapshot changesets work;
- unsupported cases fail before producing misleading output;
- inversion never guesses missing before state.

### Slice 3 - C ABI JSON Bridge And SDK Baseline

Objectives:

1. Add C ABI JSON entry points.
2. Add panic-safety tests.
3. Add .NET typed wrappers and raw JSON helpers.
4. Add raw JSON helpers to other bindings where low-risk.
5. Add binding smoke tests.

Deliverables:

- C ABI functions from Section 15.2;
- .NET changeset client;
- binding smoke tests for create/inspect/apply.

Exit criteria:

- non-Rust hosts can use changesets without shelling out;
- all touched binding smoke tests pass or skip gracefully when toolchains are
  missing.

### Slice 4 - Relay V2 HTTP Protocol

Objectives:

1. Add `decentdb relay serve`.
2. Implement `/decentdb/sync/v2/hello`.
3. Implement authenticated sessions.
4. Implement HTTP changeset export/apply/inspect routes.
5. Add principal context mapping.
6. Add production TLS posture checks.
7. Add redaction.

Deliverables:

- relay CLI server;
- auth configuration model;
- v2 HTTP routes for changesets;
- relay session metadata;
- integration tests with fake auth.

Exit criteria:

- authenticated clients can create and apply changesets through relay;
- unauthorized requests fail before engine mutation;
- core engine remains HTTP-free.

### Slice 5 - Shape Catalog And Snapshots

Objectives:

1. Add shape catalog metadata.
2. Implement shape creation/list/drop/status.
3. Bind shapes to existing sync scopes.
4. Add principal authorization for shapes.
5. Implement shape snapshot changesets.
6. Add shape client checkpoint metadata.

Deliverables:

- `decentdb relay shape create|list|drop|status|snapshot`;
- `sys.sync_shapes`;
- `sys.sync_shape_clients`;
- HTTP shape snapshot route;
- tests for invalid scopes and tenant authorization.

Exit criteria:

- clients can request authorized shape snapshots;
- invalid scope/schema state is reported clearly;
- shape checkpoints are durable.

### Slice 6 - WebSocket Shape Streaming

Objectives:

1. Implement `/decentdb/sync/v2/stream`.
2. Add WebSocket message parser/encoder.
3. Implement subscribe/snapshot/changeset/ack/heartbeat/error messages.
4. Add bounded queues and backpressure.
5. Persist acks before checkpoint advancement.
6. Resume after relay restart.

Deliverables:

- WebSocket stream route;
- stream state machine;
- ack persistence;
- lagged/resync-required behavior;
- integration tests with disconnect/reconnect.

Exit criteria:

- clients can receive incremental shape changes;
- unacked changes are redelivered or resumable;
- slow clients cannot create unbounded memory growth.

### Slice 7 - Retention, Diagnostics, And Doctor

Objectives:

1. Extend safe prune for shape checkpoints.
2. Add relay and shape diagnostics.
3. Add `decentdb relay doctor`.
4. Add compatibility drift reports.
5. Add repeated replay/failure reporting.

Deliverables:

- `sys.sync_relay_status`;
- `sys.sync_relay_sessions`;
- `sys.sync_changeset_history`;
- extended `sys.sync_retention`;
- relay doctor JSON and table output.

Exit criteria:

- operators can see lag, stale shape clients, retention blockers, conflicts,
  and insecure deployment state;
- unsafe prune reports which clients need resync.

### Slice 8 - Browser And Mobile Client Integration

Objectives:

1. Add browser owner-routed relay client API.
2. Add browser stable errors for unsupported runtime contexts.
3. Add shape subscribe API.
4. Add HTTP pull fallback.
5. Add browser diagnostics.
6. Add mobile-friendly SDK guidance.

Deliverables:

- `@decentdb/web` relay/shape API;
- browser smoke tests;
- docs for browser sync runtime;
- mobile SDK examples where available.

Exit criteria:

- supported browser runtime can subscribe to a shape;
- unsupported service-worker ownership fails explicitly;
- client can resume after disconnect.

### Slice 9 - Documentation, Examples, And Release Gate

Objectives:

1. Write public changeset API docs.
2. Write relay deployment guide.
3. Write shape subscription guide.
4. Write troubleshooting guide.
5. Add runnable examples.
6. Add performance baselines.
7. Update CLI and SQL references.

Deliverables:

- docs under `docs/`;
- example app or scripts;
- benchmark results;
- release checklist.

Exit criteria:

- a new developer can deploy a relay, define a shape, subscribe a client, apply
  changesets, inspect conflicts, and diagnose lag without reading engine code.

---

## 23. Testing Strategy

### 23.1 Unit Tests

Cover:

- changeset JSON schema parsing;
- malformed record rejection;
- compatibility checks;
- idempotency key behavior;
- shape validation;
- principal authorization helpers;
- retention blocker calculations;
- WebSocket message parsing.

### 23.2 Integration Tests

Cover:

- create/apply checkpoint changeset between two replicas;
- branch changeset success and unsupported table rejection;
- snapshot changeset with retained states;
- relay auth success/failure;
- shape snapshot and resume;
- WebSocket disconnect/reconnect;
- ack persistence;
- resync required after prune;
- conflict summary delivery.

### 23.3 Browser Tests

Cover:

- owner-routed relay calls;
- unsupported service-worker database ownership error;
- WebSocket shape subscribe in Tier 1 browser;
- HTTP pull fallback;
- storage of client checkpoints;
- diagnostics after disconnect.

### 23.4 Fault Injection

Cover:

- crash during changeset apply;
- crash after apply before session record;
- crash during ack persistence;
- corrupt changeset payload;
- decompression bomb attempt;
- network partition;
- relay restart with active clients;
- disk full during metadata write.

### 23.5 Security Tests

Cover:

- missing auth;
- invalid token;
- wrong tenant;
- unauthorized shape;
- unauthorized conflict policy;
- insecure production mode;
- credential redaction.

### 23.6 Performance Tests

Cover:

- changeset creation throughput;
- apply throughput;
- shape snapshot size and speed;
- stream fanout under bounded clients;
- retention report with many shape clients.

---

## 24. Documentation Requirements

Docs must include:

1. Conceptual overview: relay, changesets, shapes, checkpoints.
2. Relay deployment guide with TLS/proxy/auth examples.
3. Public changeset API reference.
4. CLI reference for `sync changeset` and `relay`.
5. SQL/system view reference.
6. SDK guide for JSON bridge and typed wrappers.
7. Browser sync guide tied to the production browser runtime.
8. Shape subscription guide with scope restrictions.
9. Conflict workflow guide for changeset apply.
10. Retention and pruning guide for shape clients.
11. Troubleshooting guide for schema drift, query drift, auth, lag, resync, and
    replay.
12. Security guide with identity boundary and redaction rules.
13. Example gallery.

Example gallery should include:

- browser task app subscribing to tenant shape;
- mobile/desktop field-service sync;
- server relay with JWT auth;
- manual changeset inspection and apply;
- branch changeset review workflow;
- conflict walkthrough;
- retention prune and resync-required demo.

---

## 25. Acceptance Criteria

This feature is complete only when:

1. Public changeset create/inspect/apply works through Rust, CLI, C ABI JSON,
   and at least the flagship SDK.
2. Reapplying a changeset is idempotent.
3. Malformed or incompatible changesets fail before mutation.
4. Changeset apply is transactional by default.
5. Conflicts are recorded and visible through existing conflict workflows.
6. Branch/snapshot changesets work for supported diff cases and reject
   unsupported cases.
7. Relay v2 authenticates requests and attaches principal context.
8. Relay v2 rejects unauthorized shape/scope/operation access.
9. Production insecure transport is rejected or explicitly diagnosed.
10. Shapes are backed by sync scopes and enforce tenant/principal access.
11. Shape snapshots and incremental changesets are resumable.
12. WebSocket ack persistence is durable before checkpoint advancement.
13. Lagged clients are bounded and either resume or receive resync-required.
14. Retention/pruning accounts for shape checkpoints.
15. Relay, shape, retention, changeset, and compatibility diagnostics are
    exposed through `sys.*`, CLI, and SDK JSON.
16. Browser runtime integration follows ADR 0165.
17. Tests cover crash safety, auth, replay, conflict, browser, and retention
    paths.
18. Docs and examples are complete enough for production evaluation.

---

## 26. Open Follow-Up Questions

These do not block the first implementation, but they should be revisited after
the production relay is working:

1. Should changesets gain signed envelope verification?
2. Should encrypted changeset payloads be part of the security roadmap item?
3. Should additive schema compatibility be supported beyond strict mode?
4. Should query-backed shapes be added after reactive query subscriptions ship?
5. Should per-table or per-scope conflict policies be promoted for relay use?
6. Should relay deployment move from CLI into a dedicated crate/binary?
7. Should a future hosted service use the same protocol with additional service
   metadata?
8. Should mobile bindings get typed shape streams before all other bindings?

---

## 27. Coding Agent Guidance

When implementing this spec:

1. Work one slice at a time.
2. Do not add HTTP/WebSocket dependencies to `crates/decentdb`.
3. Do not expose raw sync journal lines as the public API.
4. Do not weaken conflict or schema compatibility defaults.
5. Do not advance watermarks or shape checkpoints before durable metadata
   writes.
6. Keep identity provider behavior outside the engine.
7. Use `.tmp/` for temporary fixtures, traces, and local relay output.
8. Add docs and tests in the same PR as each user-facing API.
9. Prefer JSON fixtures and property tests for protocol edges.
10. Run the smallest relevant validation while iterating, then broaden before
    merge.

---

## 28. Final Positioning

This feature makes sync usable as a production application surface:

```text
DecentDB stores durable local SQL data, exposes stable changesets, syncs through
self-hosted authenticated relay infrastructure, and lets browser/mobile clients
subscribe to scoped data shapes with resumable conflict-aware delivery.
```

That is materially different from a raw changefeed and materially smaller than
a hosted cloud sync service. It is the next practical step after shipped
local-first sync.
