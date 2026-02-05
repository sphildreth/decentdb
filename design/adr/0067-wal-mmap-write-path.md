## WAL mmap write path (zero-copy frame encoding)
**Date:** 2026-02-05
**Status:** Accepted

### Decision
Introduce an optional mmap-backed WAL write path that encodes frames directly into
the mapped region and then relies on existing `fsync`/`fdatasync` for durability.
If mmap is unsupported or fails, WAL writes transparently fall back to the existing
buffered `vfs.write` path.

### Rationale
Commit latency is dominated by WAL append overhead and syscall cost. Mapping the WAL
file and writing frames directly into the mapped region removes the extra buffer
copy and reduces per-commit syscall overhead while preserving current durability
semantics. This aligns with the performance plan for closing the SQLite commit
latency gap without changing WAL format or recovery logic.

### Alternatives Considered
- Keep the existing `write` path only (no improvement).
- `writev`-based scatter/gather writes (tested; regressed).
- Changing durability policy (out of scope).
- Full page cache representation change without mmap (higher churn).

### Trade-offs
- Increased complexity in WAL I/O code and file sizing.
- mmap support is platform-dependent (best-effort on POSIX; fallback on others).
- Potential for SIGBUS if file sizing is incorrect (mitigated by explicit truncate).
- Fault-injection VFS paths must remain in use when mmap is unavailable.

### References
- `design/SQLITE_PERF_GAP_PLAN.md` (Section 2: Memory Copying and Buffer Allocation)
