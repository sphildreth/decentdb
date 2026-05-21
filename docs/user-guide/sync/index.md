# Local-First Sync

DecentDB local-first sync is a built-in, SQL-first replication layer for local
databases that need to work offline and exchange changes later.

It is designed for:

- device-to-device sync
- local app-to-local app sync over a relay or localhost server
- tenant-scoped replication
- conflict-aware offline workflows
- inspectable retention and operational debugging

## What It Is

The sync layer captures durable row-level changes, stores them in a local
journal, and lets a replica exchange batches with peers. Sync state stays in
the database file and its sidecar journal so it survives restarts and crash
recovery.

At a high level:

1. initialize a replica ID
2. define peers and optional scopes
3. exchange batches
4. inspect conflicts, lag, and retention
5. prune only when watermarks make it safe

## Supported Topologies

### Peer-to-peer exchange

The simplest shape is two local databases that exchange batches manually or via
the built-in HTTP transport.

### Local relay or dev server

`decentdb sync serve` exposes the sync protocol over HTTP for localhost and dev
workflows. It is useful for tests, demos, and short-lived relay processes.

### Production relay

`decentdb relay serve` exposes the production v2 sync relay routes for
authenticated application clients. The production relay uses public changesets,
tenant/user/device/service principal context, sync shapes backed by scopes,
HTTP pull, and WebSocket shape streaming.

### Scoped replication

Peers can be bound to a named scope so only a subset of rows moves between them.
This is the recommended shape for per-tenant or per-user data.

## Current Limitations

The current sync surface is usable and inspectable, but it is not a claim of
full distributed-systems maturity.

- No automatic peer discovery.
- No mesh routing or multi-hop topology management.
- No built-in TLS termination or certificate management.
- No hidden last-write-wins default for all conflicts.
- Row filters are intentionally narrow and validated.
- `sync serve` is a development transport; use `relay serve` for the
  authenticated v2 production relay protocol.

## Quick Links

- [Quickstart](quickstart.md)
- [Concepts](concepts.md)
- [Scopes](scopes.md)
- [Public changesets](changesets.md)
- [Production relay and shapes](relay.md)
- [Conflicts](conflicts.md)
- [Schema compatibility](schema-compatibility.md)
- [Security](security.md)
- [Operations](operations.md)
- [Troubleshooting](troubleshooting.md)
- [Examples](examples.md)
