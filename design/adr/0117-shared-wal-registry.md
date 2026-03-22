## Shared WAL Registry for Cross-Connection Visibility
**Date:** 2026-03-19
**Status:** Accepted

### Decision

Connections to the same on-disk database file share a single WAL instance via a process-global registry keyed by canonical file path. A shared atomic `maxPageCount` counter in the WAL propagates page-extent changes across pagers so that pages allocated by one connection are valid for free/read operations on another.

The registry provides three operations:

- **`acquireSharedWal(path)`** — returns the existing WAL for `path` (incrementing its refcount) or creates, recovers, and registers a new one.
- **`releaseSharedWal(path)`** — decrements the refcount; the caller closes the WAL file handle only when the count reaches zero.
- **`evictSharedWal(path)`** — forcibly removes the entry regardless of refcount, used before database file replacement (backup import, recovery). Existing connections retain their WAL reference via ARC; the next `openDb` creates a fresh WAL.

In-memory databases (`:memory:`) are excluded from sharing and always receive an independent WAL.

### Rationale

Before this change, every `openDb` call created an independent WAL object with its own index, lock set, and page tracking. This design had two consequences:

1. **Cross-connection invisibility** — Connection B could not see Connection A's committed data without closing and reopening, because each WAL maintained a private index. Applications (e.g. SPF5000) worked around this by invalidating connections on every write, causing connection churn.

2. **Memory leak via arena fragmentation** — The connection churn from (1) allocated and freed Wal+Pager+PageCache objects in a cycle that fragmented glibc per-thread arenas. On a Raspberry Pi 4 with 4 worker threads, RSS grew at ~1.4 MB/min indefinitely.

Sharing the WAL eliminates both problems: reads see all committed data immediately (through the shared WAL index), and connections are permanent (no close/reopen cycles).

The `maxPageCount` atomic solves a secondary bug: when Connection A allocates new pages (extending the database) and commits them to the shared WAL, Connection B's pager still has a stale `pageCount` derived from the file size at open time. Without the shared counter, B's `freePage()` and `ensurePageId()` reject valid page IDs that A allocated.

### Alternatives Considered

1. **Reduce reconnect frequency** — Skip connection invalidation when data hasn't changed. Rejected: masks the real problem; writes from any source still trigger churn.

2. **Single shared connection with mutex** — One connection per database, protected by a lock. Rejected: serializes all reads, defeats concurrent reader design.

3. **Per-connection WAL with cross-connection notification** — Keep independent WALs but notify peers on commit. Rejected: complex, requires a pub/sub mechanism, and still duplicates WAL state.

4. **Replace DecentDB with SQLite** — SQLite's WAL mode provides cross-connection visibility natively. Rejected: abandons the project; DecentDB should provide equivalent guarantees.

### Trade-offs

**Pros**
- Zero-overhead cross-connection visibility (atomic reads, no locks on hot path)
- Eliminates connection churn and associated memory fragmentation
- Backward-compatible: no file format changes, no API contract changes for existing callers
- `evictSharedWal` provides an escape hatch for database replacement scenarios

**Cons**
- Process-global mutable state (the registry) requires a lock for registration/eviction
- `maxPageCount` is monotonically increasing within a WAL lifetime; pages freed by one connection don't reduce it (acceptable: only bounds-checks use it)
- `safeCanonicalPath` fallback to `absolutePath` means symlink-resolved and unresolved paths could map to different registry entries (mitigated: callers use consistent paths)

### References

- ADR-0011: Memory Management Strategy
- ADR-0023: Isolation Level Specification
- ADR-0025: Memory Leak Prevention Strategy
- ADR-0055: Thread-Safety Contract and Snapshot Context Handling
