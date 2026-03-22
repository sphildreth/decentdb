## Shared Library Embedding Safety
**Date:** 2026-02-22
**Status:** Accepted

### Decision

When building DecentDB as a shared library (`build_lib` task), apply three compile-time and runtime changes to ensure safe embedding in host runtimes (.NET, JVM, Python, Go, etc.):

1. **`-d:noSignalHandler`** — Disable Nim's built-in signal handler
2. **`-d:useMalloc`** — Use the system allocator instead of Nim's thread-local allocator
3. **Pager eviction on `closeDb()`** — Evict stale `Pager` references from threadvar caches

These flags apply only to the shared library build target (`nimble build_lib`), not to the standalone CLI or test binaries.

### Rationale

**Signal handler conflict (`noSignalHandler`):**
Nim's runtime installs a SIGSEGV handler that prints a stack trace and aborts. Host runtimes (.NET CLR, JVM HotSpot) also use SIGSEGV-class signals internally for GC write barriers, null-reference traps, and stack probing. Two competing signal handlers on the same signal cause non-deterministic crashes. Disabling Nim's handler lets the host runtime manage signals as it expects.

**Thread-local allocator (`useMalloc`):**
Nim's default allocator (`nimAllocPagesViaMmap`) maintains per-thread free lists. Host runtimes like .NET's async/await model routinely allocate objects on one OS thread and free them on another (task continuation on a different thread-pool thread). This is safe with system `malloc`/`free` (which are thread-safe) but causes heap corruption with Nim's thread-local allocator. Using `-d:useMalloc` delegates all allocation to the system allocator.

**Pager eviction (`closeDb()`):**
DecentDB uses three threadvar caches for performance: `gAppendCache` (B-tree append optimization), `gReusableBTree` (avoids per-insert BTree allocation), and `gEvalPager`/`gEvalCatalog` (execution context reuse). These hold raw `Pager` pointers. Under ARC, when a database is closed and its `Pager` deallocated, these threadvar entries become dangling references. On the next `openDb()`, the thread-local cache still points to freed memory. In host runtimes that reuse threads (thread pools), this leads to use-after-free. The fix evicts all entries referencing a specific `Pager` during `closeDb()`.

### Alternatives Considered

1. **Apply flags globally (all build targets):** Rejected — standalone CLI and tests benefit from Nim's signal handler (crash diagnostics) and thread-local allocator (faster allocation). The embedding issues only manifest when a host runtime controls the process.

2. **Weak references for threadvar caches:** Nim lacks native weak references under ARC. Simulating them (ref counting wrapper + nil check) adds complexity for minimal gain when explicit eviction is straightforward and correct.

3. **Remove threadvar caches entirely:** Would eliminate the dangling reference problem but would regress insert performance (the append cache and reusable BTree are measurable optimizations on bulk workloads).

### Trade-offs

| Aspect | Impact |
|--------|--------|
| Allocation performance | `-d:useMalloc` is slightly slower than Nim's arena allocator for small, frequent allocations. Unmeasurable in benchmarks for DecentDB's workload (I/O-dominated). |
| Crash diagnostics | No Nim stack trace on SIGSEGV in the shared library. Host runtime's crash handler takes over (typically provides better diagnostics anyway). |
| `closeDb()` cost | Three threadvar scans add ~microseconds to close. Negligible vs. fsync/flush. |
| Standalone builds | Unaffected — flags are only in `build_lib`, not `nimble test` or CLI targets. |

### References

- Nim manual: [noSignalHandler](https://nim-lang.org/docs/nimc.html)
- Nim manual: [useMalloc](https://nim-lang.org/docs/nimc.html)
- ADR-0011: Memory Management Strategy
- ADR-0025: Memory Leak Prevention Strategy
