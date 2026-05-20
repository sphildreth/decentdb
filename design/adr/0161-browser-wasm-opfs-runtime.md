## Browser WASM OPFS Runtime
**Date:** 2026-05-20
**Status:** Accepted

### Decision

DecentDB will support browser execution through a `wasm32-unknown-unknown`
build of the Rust engine running inside a Dedicated Worker. Browser persistence
uses OPFS synchronous access handles behind a Rust `Vfs` implementation supplied
by the worker runtime. The public browser API is the async TypeScript package
`@decentdb/web`.

The native Rust engine remains synchronous. The browser binding owns async
worker startup, OPFS handle preparation, and request/response orchestration.

The initial browser SQL parser path is target-specific because the native
`pg_query` C parser does not compile for `wasm32-unknown-unknown`. The wasm
parser is intentionally narrow until a full wasm-compatible parser strategy is
accepted.

### Rationale

This preserves the existing engine architecture:

- the pager, WAL, page cache, B+Tree, and executor remain synchronous
- native file I/O and WAL hot paths do not gain browser runtime checks
- OPFS-specific behavior is isolated behind the VFS abstraction
- JavaScript callers get an async API without forcing async through the core

OPFS synchronous access handles are available only in worker contexts, so the
worker-owned model is both a performance requirement and a correctness boundary.
It also keeps the one-writer rule explicit: one logical browser database path is
owned by one worker connection in v1, and cross-tab or cross-worker write
coordination is not promised.

### Alternatives Considered

- **Rewrite the engine around async I/O.** Rejected because it would touch native
  pager/WAL hot paths and weaken the current synchronous durability model.
- **Use IndexedDB as the primary backend.** Rejected for v1 because it does not
  provide the same synchronous file-like access model and would require a
  browser-specific storage design.
- **Attempt to compile `pg_query` into `wasm32-unknown-unknown`.** Rejected for
  this slice because the C parser depends on platform headers and libc-style
  assumptions that are not available on that target.
- **Support multi-tab writes in v1.** Rejected because it needs a separate
  coordination design for locking, WAL visibility, and retention.

### Trade-offs

- Browser support now has a real runtime path without changing native file or
  WAL formats.
- The initial wasm SQL parser supports only the browser smoke/API subset; native
  SQL coverage remains broader.
- OPFS durability is documented as browser-substrate durability, not native
  power-loss-resistant filesystem durability.
- Import/export is explicit because browser-managed storage is not a backup
  strategy.

### References

- `design/FUTURE_WINS.md`
- `design/WIN_WASM_SUPPORT_IMPLEMENTATION.md`
- `crates/decentdb/src/vfs/opfs.rs`
- `crates/decentdb/src/wasm.rs`
- `bindings/web/`
