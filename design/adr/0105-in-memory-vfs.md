## In-Memory Database Support via VFS
**Date:** 2026-02-22
**Status:** Accepted

### Decision

We will implement in-memory database support (activated via the `:memory:` connection string) by introducing a new memory-backed Virtual File System (`MemVfs`).

To support multiple VFS implementations, we are refactoring `VfsFile` into a base class and introducing concrete subclasses (`OsVfsFile` and `MemVfsFile`). The `Vfs` interface is expanded to include `getFileSize`, `fileExists`, and `removeFile`, replacing direct OS calls in the engine, pager, and WAL.

WAL remains enabled for `:memory:` databases in v1. `MemVfs` does not support `mmap`; it stores file data in a `seq[byte]`. 

### Rationale

*   **VFS Extensibility via Inheritance**: Using a `VfsFile` base class and concrete subclasses (`OsVfsFile`, `MemVfsFile`) provides a clean object-oriented approach to polymorphism in Rust, avoiding complex variant types or interface dispatch overhead while keeping the VFS abstraction straightforward.
*   **WAL Enabled for `:memory:`**: Keeping WAL enabled simplifies the v1 implementation. Disabling WAL specifically for `:memory:` would require new conditional logic across the engine and transaction lifecycle. It is simpler and less error-prone to reuse the existing WAL machinery, which works correctly on `MemVfs`.
*   **No `mmap` Support**: Memory mapping a `seq[byte]` across process memory conceptually doesn't align with POSIX `mmap` on a file descriptor without complex shared memory tricks. Falling back to standard read/write paths for `MemVfs` avoids platform-specific complexity and keeps the implementation trivial.

### Alternatives Considered

*   **Bypassing WAL for In-Memory**: We considered skipping WAL for in-memory databases to save overhead, but decided against it to avoid fragmenting the engine's transaction and recovery logic.
*   **In-Memory B-Tree (No VFS)**: We could have implemented a purely in-memory data structure that doesn't use the pager at all. This would require rewriting significant portions of the engine and would diverge significantly from the disk-based behavior. Using a `MemVfs` ensures the exact same code paths are exercised.

### Trade-offs

*   **Memory Overhead vs Correctness/Complexity**: Keeping WAL enabled introduces some memory overhead (the WAL file also consumes memory in `MemVfs`), but dramatically reduces code complexity and ensures the in-memory database behaves identically to a disk-backed database regarding concurrency and transaction semantics.
*   **Performance**: Since `mmap` is not supported on `MemVfs`, the engine will use standard `read` and `write` calls. This involves `copyMem` instead of zero-copy pointer access. For an in-memory database, `copyMem` is extremely fast, so the performance impact is negligible compared to the architectural simplicity gained.

### References

*   INMEMORY_SUPPORT_PLAN.md