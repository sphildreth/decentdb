# ADR 0168: Sync Shape Streaming Subscriptions
**Date:** 2026-05-20
**Status:** Accepted

## Context

Roadmap priority #1 requires scoped "shape" subscriptions for browser and
mobile clients. DecentDB already has sync scopes, in-process reactive
subscriptions, and a browser runtime contract. The missing decision is how a
production shape stream relates to those pieces.

A shape stream must be durable and resumable across process restarts. The
in-process reactive event hub is useful for waking live subscribers, but ADR
0164 explicitly does not make it a durable network pub/sub system. Shape streams
therefore need to use sync journal/checkpoint semantics as their source of
truth.

## Decision

DecentDB will implement production sync shapes as **durable, scoped
subscriptions backed by sync scopes and public changesets**.

### 1. Shape definition

A shape is a named public subscription contract with:

- `shape_id`;
- human-readable name;
- backing sync scope;
- tenant binding;
- allowed principal selectors;
- included tables;
- deterministic row filter inherited from the backing scope;
- compatibility metadata;
- retention policy;
- optional stream options such as batch size, heartbeat interval, and ack
  deadline.

The first shape implementation does not support arbitrary query subscriptions,
joins, subqueries, aggregates, user-defined functions, or opaque predicates.
Those features require a later ADR if they become necessary.

### 2. Durable source of truth

The durable source for shape delivery is the sync journal plus public changeset
creation. The reactive event hub may notify the relay that new commits are
available, but it must not be the authoritative source for replay or resume.

If a relay process restarts, a client resumes from its last acknowledged shape
checkpoint. If the checkpoint has been pruned, the relay returns
`SHAPE_RESYNC_REQUIRED`.

### 3. Initial snapshot and incremental stream

Subscribing to a shape starts with one of two modes:

1. **Snapshot mode**: relay sends a shape snapshot changeset and a resume
   checkpoint.
2. **Resume mode**: relay validates the client checkpoint and sends only
   incremental changesets after that checkpoint.

After the initial response, the relay delivers incremental changesets over
WebSocket when streaming is available. HTTP long-poll or repeated pull remains a
fallback transport for mobile and restricted browser environments.

### 4. WebSocket delivery contract

WebSocket streams use explicit message types:

- `hello`;
- `subscribe_shape`;
- `snapshot`;
- `changeset`;
- `ack`;
- `heartbeat`;
- `lagged`;
- `resync_required`;
- `conflict_summary`;
- `error`;
- `close`.

Every changeset message has a monotonically increasing shape sequence and an
ack deadline. The relay advances the durable client checkpoint only after it
receives an ack and persists the checkpoint.

### 5. Backpressure and retention

Shape streams have bounded in-memory queues. When a client falls behind:

1. the relay stops reading unbounded data for that client;
2. the client's durable checkpoint remains at the last acked changeset;
3. the client receives `lagged` while the relay can still resume from retained
   journal state;
4. the client receives `resync_required` if retention has pruned the required
   checkpoint.

Retention must treat active shape checkpoints as blockers unless an operator
explicitly chooses an unsafe prune.

### 6. Browser and mobile routing

Browser clients use the production browser owner-routing model from ADR 0165.
Service workers do not own DecentDB browser database handles. Browser sync
transport is routed through supported page/worker contexts and fails with stable
errors when the runtime cannot support it.

Mobile clients may use the same HTTP/WebSocket protocol without browser owner
coordination.

### 7. Conflict visibility

Shape streams do not silently hide conflicts. If applying an inbound changeset
records conflicts on the relay or client, the stream exposes a conflict summary
message and the normal conflict inspection APIs remain authoritative.

## Rationale

Using sync scopes as the backing model keeps the first shape feature narrow,
deterministic, and compatible with shipped scoped sync. Using public changesets
keeps browser/mobile transport aligned with DecentDB-to-DecentDB sync instead of
creating a separate CDC feed.

The reactive event hub can improve latency, but durability and resume must come
from the sync journal because production clients disconnect, reconnect, and
survive relay restarts.

## Consequences

- Shape streaming becomes a production sync feature, not a general SQL
  live-query service.
- Existing sync scope restrictions apply to v1 shapes.
- A lagging client can resume while retained checkpoints exist.
- Retention/pruning must account for shape subscribers.
- Arbitrary query-backed shapes remain deferred.

## Alternatives Considered

1. **Use the reactive event hub as the network stream.** Rejected because the
   hub is in-process and non-durable.
2. **Allow arbitrary SQL query shapes in v1.** Rejected because exact durable
   query diffs, joins, aggregates, and opaque dependencies are not part of the
   shipped sync scope model.
3. **Send only invalidations over WebSocket.** Rejected for production sync
   because browser/mobile clients need durable data changesets and resume
   checkpoints, not just cache invalidation hints.
4. **Let retention ignore active shape clients.** Rejected because pruning a
   client's resume checkpoint would make the stream unreliable without clear
   operator intent.

## Validation Requirements

Implementation is not complete until tests cover:

- shape creation from valid and invalid sync scopes;
- principal authorization for shape subscription;
- initial snapshot changeset delivery;
- resume from an acknowledged checkpoint;
- WebSocket ack persistence before checkpoint advancement;
- relay restart and client resume;
- lagged and resync-required behavior;
- retention blockers from active shape checkpoints;
- conflict summary delivery;
- browser owner-routed sync errors and success paths where supported.

## References

- `design/FUTURE_WINS.md` priority #1
- `design/_archive/WIN_PRODUCTION_RELAY_SPEC.md`
- `design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`
- `design/_archive/WIN_REACTIVE_QUERY_SUBSCRIPTIONS_CHANGE_STREAMS_SPEC.md`
- `design/_archive/WIN_PRODUCTION_BROWSER_RUNTIM.md`
- `design/adr/0149-scoped-sync-v1.md`
- `design/adr/0164-reactive-query-subscriptions-and-change-streams.md`
- `design/adr/0165-production-browser-runtime-contract.md`
