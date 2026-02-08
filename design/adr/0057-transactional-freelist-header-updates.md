## Transactional Freelist/Header Updates
**Date:** 2026-02-02
**Status:** Deferred

### Decision
Draft a design to make freelist mutations and DB header updates crash-safe and transactional, instead of being persisted (and fsynced) outside the WAL during an active transaction.

### Rationale
Today, `allocatePage`/`freePage` mutate freelist structures and call `updateHeader` which ultimately fsyncs the DB header. A crash after these writes but before WAL commit can make the main DB file reflect uncommitted structural state, which is an ACID/atomicity risk.

### Alternatives Considered
- Defer header/fsync until commit/checkpoint and treat freelist changes as “volatile until commit”.
- WAL-log allocation/free intents and replay/undo them during recovery.
- Introduce a separate “freelist WAL” stream or treat freelist pages as regular pages always updated via WAL.

### Trade-offs
- Stronger correctness vs additional WAL traffic and recovery complexity.
- If freelist changes are WAL-logged, page allocation/free becomes part of transaction semantics and may impact performance.

### References
- design/adr/0051-freelist-atomicity.md
- design/adr/0010-error-handling-strategy.md
