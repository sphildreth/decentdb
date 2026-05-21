# Production Relay And Shapes

`decentdb relay serve` is the self-hosted production relay boundary. It
authenticates requests, attaches tenant and subject identity, calls engine-owned
changeset APIs, and records relay/shape metadata durably. The core engine stays
free of HTTP and WebSocket dependencies.

## Start A Relay

Production mode requires an auth token environment variable. TLS can terminate
at a reverse proxy; use `--public-url=https://...` with `--require-tls` so the
relay can diagnose its deployment posture.

```bash
export DECENTDB_RELAY_TOKEN="$(openssl rand -hex 32)"

decentdb relay serve \
  --db=app.ddb \
  --listen=127.0.0.1:8080 \
  --public-url=https://relay.example.com \
  --require-tls \
  --auth-token-env=DECENTDB_RELAY_TOKEN
```

Loopback tests can pass `--allow-insecure`; this is surfaced in relay
diagnostics and should not be hidden in production.

## Principal Context

Relay requests use a bearer token plus principal headers:

- `Authorization: Bearer <token>`
- `x-decentdb-tenant`
- `x-decentdb-subject`
- `x-decentdb-subject-kind`
- `x-decentdb-roles`
- `x-decentdb-scopes`
- `x-decentdb-shapes`
- `x-decentdb-session`
- `x-decentdb-request`

Browser WebSocket clients cannot set arbitrary headers, so the CLI relay also
accepts short-lived `token`, `tenant`, `subject`, `subject_kind`, `roles`,
`scopes`, `shapes`, `session`, and `request` query parameters on
`/decentdb/sync/v2/stream`. Use TLS when passing credentials this way.

Shape endpoints are authorized by the target shape and its role/subject allowlist.
For shape snapshot/changes APIs, `x-decentdb-shapes` (or stream query parameter
`shapes`) must match the subscribed shape; scope headers are not required for
those calls.

## Shapes

Shapes are public subscription contracts backed by existing sync scopes. They
are intentionally narrower than arbitrary SQL live queries.

```bash
decentdb sync scope create \
  --db=app.ddb \
  --name=tenant_42_tasks \
  --include=tasks,task_comments \
  --row-filter="tenant_id = 42"

decentdb relay shape create \
  --db=app.ddb \
  --shape=tenant_42_tasks_v1 \
  --scope=tenant_42_tasks \
  --tenant=tenant_42 \
  --allow-role=user
```

Clients can request an initial snapshot over HTTP:

```bash
curl -sS \
  -H "Authorization: Bearer $DECENTDB_RELAY_TOKEN" \
  -H "x-decentdb-tenant: tenant_42" \
  -H "x-decentdb-subject: user_123" \
  -H "x-decentdb-roles: user" \
  -H "x-decentdb-shapes: tenant_42_tasks_v1" \
  -d '{"client_replica_id":"web_123"}' \
  http://127.0.0.1:8080/decentdb/sync/v2/shapes/tenant_42_tasks_v1/snapshot
```

Incremental delivery is available through HTTP pull
`GET /decentdb/sync/v2/shapes/{shape_id}/changes?since=<watermark>` or
WebSocket `GET /decentdb/sync/v2/stream`.

## WebSocket Messages

The stream sends `hello`, `snapshot`, `changeset`, `heartbeat`, `lagged`,
`ack`, `error`, and close messages. A client subscribes with:

```json
{
  "type": "subscribe_shape",
  "shape_id": "tenant_42_tasks_v1",
  "client_replica_id": "web_123",
  "mode": "snapshot"
}
```

The relay advances the durable shape checkpoint only after the client sends
`ack` and the ack is persisted:

```json
{
  "type": "ack",
  "shape_id": "tenant_42_tasks_v1",
  "client_replica_id": "web_123",
  "checkpoint": {
    "shape_sequence": 44,
    "source_high_watermark": 1300
  },
  "changeset_id": "changeset:v1:..."
}
```

The CLI relay keeps at most one unacked changeset in flight per subscription.
Slow clients receive `lagged` messages instead of unbounded buffering.

## Diagnostics

Use:

```bash
decentdb relay status --db=app.ddb --format=json
decentdb relay doctor --db=app.ddb --format=json
decentdb relay shape status --db=app.ddb --shape=tenant_42_tasks_v1
```

The same state is queryable through `sys.sync_relay_status`,
`sys.sync_relay_sessions`, `sys.sync_shapes`, `sys.sync_shape_clients`,
`sys.sync_changeset_history`, `sys.sync_retention`, and
`sys.sync_peer_lag`.
