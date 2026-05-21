# ADR 0166: Production Sync Relay Boundary And Identity
**Date:** 2026-05-20
**Status:** Accepted

## Context

`design/FUTURE_WINS.md` priority #1 requires DecentDB sync to move from a
developer workflow into a production application platform surface. Slices 1-8
of `design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md` already provide durable
change capture, scoped sync, HTTP development transport, conflict workflows,
retention reporting, and SDK access. The missing decision is how a production
relay participates without turning the embedded engine into a hosted sync
service or a general application server.

The existing HTTP sync transport ADR intentionally kept HTTP bytes in the CLI
and kept the core engine free of HTTP dependencies. The production relay must
preserve that boundary while adding authentication, tenant/user identity,
browser/mobile transport, streaming, durable session records, and operational
diagnostics.

## Decision

DecentDB will treat the production sync relay as a **self-hosted protocol
participant** layered around DecentDB-to-DecentDB sync semantics.

### 1. Relay boundary

The relay is not part of the core storage engine. The relay implementation may
live in `crates/decentdb-cli`, a future companion crate, or a separately
packaged binary, but `crates/decentdb` remains HTTP-free.

The core engine owns:

- changeset creation and transactional apply;
- peer, scope, session, conflict, retention, and watermark metadata;
- compatibility checks;
- durable metadata writes;
- public Rust/C ABI JSON surfaces needed by bindings.

The relay owns:

- HTTP/WebSocket transport;
- authentication and principal construction;
- request routing;
- transport-level compression, limits, timeouts, and backpressure;
- redaction of transport credentials;
- production deployment configuration.

### 2. DecentDB-to-DecentDB semantics remain authoritative

The relay must use DecentDB's native changeset/apply semantics as the source of
truth. It must not define a separate queue format that bypasses the engine's
conflict detection, schema compatibility, sync scopes, or transactional apply
rules.

External database adapters may be designed later, but they are not part of this
decision and must not become a core import/export replacement.

### 3. Relay protocol versioning

Production relay routes use a new versioned namespace:

```text
/decentdb/sync/v2/*
```

The existing `/decentdb/sync/v1/*` routes remain supported for the shipped HTTP
sync development workflow. v2 is allowed to introduce authenticated sessions,
public changesets, shape subscriptions, streaming, and stricter compatibility
metadata without weakening v1 compatibility.

Every v2 session must negotiate:

- protocol version;
- engine version;
- changeset envelope version;
- schema fingerprint;
- query-contract fingerprint when a shape depends on query metadata;
- supported compression;
- supported conflict policies;
- retention and resume capabilities.

### 4. Principal model

Every production relay request is associated with a stable principal:

```text
tenant_id
subject_id
subject_kind
auth_issuer
roles
allowed_scopes
allowed_shapes
session_id
request_id
```

`tenant_id` is required for production relay requests. `subject_id` identifies
the user, service account, device, or automation identity. `subject_kind`
distinguishes at least `user`, `service`, and `device`.

The relay validates credentials and constructs the principal before calling
engine sync APIs. The core engine does not become an identity provider and does
not issue application tokens. It receives the principal as sync context for
authorization checks, audit metadata, diagnostics, and shape/scope enforcement.

### 5. Authorization boundary

The relay is responsible for validating credentials and rejecting requests
whose principal is not allowed to access the requested shape, scope, peer, or
changeset operation.

The core engine is responsible for validating that the requested shape/scope is
legal, that a changeset matches the expected scope and compatibility contract,
and that transactional apply preserves DecentDB constraints and sync metadata
durability.

### 6. Transport security

Production relay deployments must use TLS or a documented trusted internal
network boundary. The DecentDB relay implementation may support plain HTTP only
for loopback development or explicitly configured test deployments.

Production mode must fail closed when it cannot establish a secure deployment
posture unless the operator supplies an explicit insecure override. The override
must be visible in relay diagnostics.

### 7. Durable relay metadata

Relay sessions, stream subscriptions, shape leases, ack checkpoints, conflict
summaries, and retention blockers are sync metadata. Writes to this metadata
must preserve crash recovery guarantees. A relay must not acknowledge an apply,
ack, or checkpoint advance until the corresponding state has been durably
recorded.

### 8. Observability

Relay health is exposed through engine-owned `sys.*` inspection surfaces and
machine-readable CLI/SDK results. Legacy `sys_sync_*` names may remain as
compatibility aliases, but new canonical names should use dotted `sys.*`
surfaces consistent with ADR 0163.

## Rationale

This decision keeps DecentDB embedded and transport-pluggable while giving real
applications a production sync surface. The relay can be deployed by an
application team, behind that team's TLS and identity stack, without requiring
DecentDB to become a hosted service.

Keeping protocol semantics in the engine prevents transport code from inventing
parallel conflict, retention, schema, or apply behavior. Keeping authentication
outside the engine avoids embedding a partial identity platform in a database
library.

## Consequences

- `crates/decentdb` remains portable and does not gain an HTTP or WebSocket
  dependency.
- The production relay can evolve transport behavior without changing storage
  engine hot paths.
- Public relay requests become auditable by tenant, user/device/service
  subject, shape, and session.
- Applications still need to supply identity, token issuance, TLS termination,
  and deployment policy.
- Any future hosted service can use this protocol, but the engine does not
  promise or require a DecentDB-operated cloud service.

## Alternatives Considered

1. **Put the production HTTP/WebSocket server in the core engine.** Rejected
   because it would force transport dependencies, runtime policy, TLS choices,
   and authentication complexity into every embedder.
2. **Make the relay a queue-only service with its own change format.** Rejected
   because it would bypass DecentDB's existing conflict, scope, schema, and
   apply semantics.
3. **Let applications handle tenant/user identity only outside DecentDB.**
   Rejected because diagnostics, retention, conflict reports, and shape
   authorization need a stable principal context.
4. **Build DecentDB-hosted cloud sync as the roadmap item.** Rejected. A hosted
   service can be layered later, but the core roadmap item is the protocol and
   self-hosted production surface.

## Validation Requirements

Implementation is not complete until tests cover:

- v2 protocol negotiation success and failure;
- authentication-required production routes;
- principal-to-shape and principal-to-scope authorization failures;
- relay session durability across process restart;
- redaction of credentials and bearer tokens in logs and doctor output;
- insecure deployment override visibility;
- relay diagnostics through CLI/SDK and `sys.*`;
- no HTTP/WebSocket dependency added to `crates/decentdb`.

## References

- `design/FUTURE_WINS.md` priority #1
- `design/WIN_PRODUCTION_RELAY_SPEC.md`
- `design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`
- `design/adr/0148-sync-http-transport-and-peer-management.md`
- `design/adr/0149-scoped-sync-v1.md`
- `design/adr/0150-sync-conflict-resolution-workflows.md`
- `design/adr/0151-sync-operational-hardening.md`
- `design/adr/0163-operational-sys-metrics.md`
