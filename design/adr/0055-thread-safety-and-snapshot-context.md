## Thread-Safety Contract and Snapshot Context Handling
**Date:** 2026-02-01
**Status:** Proposed

### Decision

1. `Db` connections are **not** safe for concurrent use from multiple threads.
   - A single connection may have **one active statement at a time**.
   - For multi-threaded read workloads, each reader thread should open its **own** connection.
2. Snapshot Isolation is provided per connection via the WAL snapshot mechanism (ADR-0023) and WAL retention rules (ADR-0019).
3. The current snapshot plumbing that uses connection-scoped mutable state (e.g. `Pager.overlaySnapshot`) is acceptable under (1).
4. If/when we want a single `Db` connection to support multiple concurrent reader threads, we will introduce explicit per-reader snapshot context (e.g. passing snapshot through pager read APIs, or making overlay snapshot state thread-local and keyed by connection) and add concurrency tests.

### Rationale

- The engine is currently designed around **one writer** with **multiple concurrent readers**, but the safest and simplest interpretation is "multiple readers across multiple connections" rather than concurrent use of a single connection object.
- The existing implementation uses a WAL overlay to ensure late commits remain visible after cache eviction, and uses `overlaySnapshot` during statement execution to enforce snapshot reads.
- Making a single connection concurrently usable would require changing locking/concurrency semantics and snapshot context propagation, which must be done deliberately (AGENTS.md scope boundaries).

### Alternatives Considered

1. **Make `Db` fully thread-safe for concurrent reads**
   - Requires changing pager/WAL overlay APIs to take an explicit snapshot context per operation.
   - Requires new concurrency tests and careful performance evaluation.
2. **Thread-local snapshot state**
   - Potentially smaller code diff than explicit snapshot parameters.
   - Still requires specifying how thread-local state is keyed and validated (multiple DBs per thread).
3. **Global lock around snapshot changes**
   - Easiest correctness fix.
   - Defeats the goal of concurrent readers on a single connection.

### Trade-offs

**Pros**
- Keeps the 0.x engine simple and consistent with existing design constraints.
- Avoids accidental correctness regressions from broad snapshot-context refactors.

**Cons**
- Places a clear requirement on bindings and users: one connection per thread for concurrent reads.
- Defers work needed to make a single connection concurrently readable.

### References

- ADR-0019: WAL Retention for Active Readers
- ADR-0023: Isolation Level Specification
- AGENTS.md: ADR-required changes for concurrency/locking semantics
