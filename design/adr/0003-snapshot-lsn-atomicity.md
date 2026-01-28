# Snapshot LSN Atomicity
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Use `AtomicU64` with acquire/release semantics for `wal_end_lsn` to ensure lock-free snapshot reads.

### Rationale
- Readers can capture snapshot without acquiring locks
- Acquire semantics ensure readers see all prior writes
- Release semantics ensure WAL updates are visible before LSN increment
- Avoids contention on read-heavy workloads

### Alternatives Considered
- Mutex around LSN read: Simpler but introduces contention
- SeqCst semantics: Overkill for this use case

### Trade-offs
- **Pros**: Lock-free reads, good for read-heavy workloads
- **Cons**: Requires careful use of atomic primitives

### References
- SPEC.md §4.2 (Snapshot reads)
