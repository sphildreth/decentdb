# ADR 0148: Sync HTTP Transport and Peer Management
**Date:** 2026-05-17  
**Status:** Accepted

## Context

Slice 3 of the local-first sync implementation needs a first official network
transport, peer catalog, resumable sync sessions, and CLI-driven operational
workflows. The engine already owns durable sync capture, manual batch
envelopes, conflict storage, and peer watermarks. The next step is to add a
networking surface without coupling the core engine to an HTTP stack.

DecentDB also needs a practical test/dev server for sync-only routes so the CLI
can exercise real end-to-end HTTP workflows without introducing a general
database HTTP server.

## Decision

1. **The core engine owns sync peer/session metadata and protocol structs.**
   - `Db` exposes peer catalog APIs, session recording helpers, and sync
     protocol types.
   - Core-owned metadata lives in internal tables such as
     `__decentdb_sync_peers` and `__decentdb_sync_sessions`.

2. **The CLI owns HTTP transport bytes.**
   - `crates/decentdb` does not gain an HTTP dependency.
   - `crates/decentdb-cli` uses the blocking `ureq` client for sync transport.
   - No async runtime is introduced for Slice 3.

3. **The only server surface is sync protocol endpoints under
   `/decentdb/sync/v1/*`.**
   - `GET /hello`
   - `GET /status`
   - `GET /changes`
   - `POST /import`
   - `GET /conflicts`
   - DecentDB does not implement a general database HTTP server mode in this
     slice.

4. **HTTPS is supported on the client side through `ureq`.**
   - The built-in `sync serve` command is plain HTTP only.
   - Production deployments are expected to terminate TLS externally in front
     of the dev server.

5. **Workspace dependency shape**
   - Add `ureq = { version = "2", features = ["json"] }` to workspace
     dependencies.
   - `crates/decentdb-cli` depends on `ureq.workspace = true`.

## Rationale

- Keeping the engine free of HTTP dependencies preserves the embedded library
  boundary and avoids forcing transport policy onto every embedder.
- Making the CLI own the wire protocol keeps the transport implementation easy
  to replace later without changing core sync semantics.
- A sync-only dev server is enough to exercise peer management, handshake,
  push/pull, retries, and redaction in tests.
- Plain HTTP for the built-in server avoids a TLS stack and certificate
  management burden in the engine/CLI path while still allowing production TLS
  termination outside the process.

## Consequences

- Core sync code remains portable and easier to embed from non-Rust hosts.
- The CLI can evolve transport behavior independently from the engine.
- Sync sessions, peer definitions, and handshake/capability metadata are
  inspectable from SQL and from public Rust APIs.
- Users who want encrypted production traffic must place the dev server behind
  a TLS terminator or proxy.

## References

- `design/WIN01_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`
- `design/FUTURE_WINS.md`
- `design/adr/0147-local-sync-journal-foundation.md`
- `crates/decentdb/src/sync.rs`
- `crates/decentdb/src/db.rs`
- `crates/decentdb-cli/src/commands/mod.rs`
