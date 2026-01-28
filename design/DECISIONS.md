# DecentDb Decision Log
**Date:** 2026-01-28
**Status:** Active

This document tracks significant architectural and design decisions for DecentDb, including the rationale and alternatives considered.

---

## Decision 1: Page Size (4096 bytes default)
**Date:** 2026-01-28
**Status:** Decided

### Decision
Use 4096 bytes as the default page size, but make it configurable at database creation time.

### Rationale
- Aligns with typical SSD block sizes (4KB is common)
- Matches OS page sizes on most systems
- Provides good balance between internal fragmentation and I/O efficiency
- Larger pages (8KB, 16KB) reduce fragmentation for wide rows but increase memory pressure
- Smaller pages (2KB) reduce memory pressure but increase I/O overhead

### Alternatives Considered
- Fixed 8KB pages: Better for wide rows, worse for cache-constrained environments
- Fixed 2KB pages: Better for memory-constrained systems, worse for I/O efficiency
- Dynamic page sizing: Too complex for MVP

### Trade-offs
- **Pros**: Good default for most workloads, configurable for special cases
- **Cons**: Requires decision at database creation time, cannot change after creation

### References
- SPEC.md §2.1 (pager module)
- SPEC.md §16 (configuration system)

---

## Decision 2: WAL Commit Record Format
**Date:** 2026-01-28
**Status:** Decided

### Decision
Use a dedicated commit record type instead of a boolean commit flag in each frame.

### Rationale
- Clearer separation between data frames and commit markers
- Easier to extend with additional commit metadata (transaction_id, timestamp)
- Simplifies recovery logic (scan for commit records rather than checking flags)
- More robust for future features (e.g., savepoints, nested transactions)

### Alternatives Considered
- Boolean commit flag per frame: Simpler but less extensible
- Commit marker as special page_id: Confusing and error-prone

### Trade-offs
- **Pros**: Clear semantics, extensible, easier recovery
- **Cons**: Slightly more complex frame format

### References
- SPEC.md §4.1 (WAL frame format)

---

## Decision 3: Snapshot LSN Atomicity
**Date:** 2026-01-28
**Status**: Decided

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

---

## Decision 4: WAL Checkpoint Strategy
**Date:** 2026-01-28
**Status**: Decided

### Decision
Implement WAL size-based checkpointing with configurable thresholds and forced checkpoint timeout.

### Rationale
- Prevents unbounded WAL growth
- Configurable threshold allows tuning for different workloads
- Timeout prevents indefinite blocking if readers are long-lived
- Forced checkpoint with readers active ensures progress

### Alternatives Considered
- Checkpoint only when no readers: Can block indefinitely
- Time-based checkpointing: Doesn't account for WAL size
- Manual checkpoint only: Too much operational burden

### Trade-offs
- **Pros**: Bounded WAL size, configurable, ensures progress
- **Cons**: Forced checkpoint may be slower, requires careful implementation

### References
- SPEC.md §4.3 (Checkpointing)

---

## Decision 5: SQL Parameterization Style
**Date:** 2026-01-28
**Status**: Decided

### Decision
Use Postgres-style positional parameters (`$1, $2, ...`) for MVP.

### Rationale
- Consistent with libpg_query parser choice
- Familiar to developers with PostgreSQL experience
- Simple to implement and test
- Well-defined semantics

### Alternatives Considered
- Named parameters (`:name`): More flexible but more complex
- Question marks (`?`): Familiar from other databases but less explicit

### Trade-offs
- **Pros**: Simple, consistent with parser choice, well-understood
- **Cons**: Less flexible than named parameters

### References
- SPEC.md §6.3 (Parameterization)
- PRD.md §2.1 (Functional goals)

---

## Decision 6: Foreign Key Index Creation
**Date:** 2026-01-28
**Status**: Decided

### Decision
Auto-create indexes on child FK columns if not present.

### Rationale
- Ensures FK checks are efficient (avoids full table scans)
- Reduces user burden (don't need to remember to create indexes)
- Consistent with PostgreSQL behavior
- Index name follows predictable pattern (`fk_<table>_<column>_idx`)

### Alternatives Considered
- Require explicit index creation: More control but higher burden
- No index requirement: Simpler but terrible performance

### Trade-offs
- **Pros**: Good performance by default, less user burden
- **Cons**: Additional indexes increase storage and write overhead

### References
- SPEC.md §7.2 (Foreign keys)

---

## Decision 7: Trigram Postings Storage Strategy
**Date:** 2026-01-28
**Status**: Decided

### Decision
Use in-memory buffers per trigram (max 4KB) that flush to B+Tree on transaction commit.

### Rationale
- Bounded write amplification (buffer limits per-trigram size)
- Simple implementation compared to LSM-style merge
- Flush on commit ensures durability
- Delta-encoded varints provide good compression for sequential IDs

### Alternatives Considered
- LSM-style append segments: More complex, requires merge logic
- Direct B+Tree updates: High write amplification

### Trade-offs
- **Pros**: Bounded writes, simple, good compression
- **Cons**: In-memory buffers consume memory, flush on commit adds latency

### References
- SPEC.md §8.5 (Storage format for postings)

---

## Decision 8: Trigram Pattern Length Guardrails
**Date:** 2026-01-28
**Status**: Decided

### Decision
Combine pattern length checks with posting-list frequency thresholds:
- Patterns < 3 chars: never use trigram index
- Patterns 3-5 chars: use only if combined with other filters or rarest trigram < threshold
- Patterns > 5 chars: use trigram index, but cap results if rarest trigram exceeds threshold

### Rationale
- Short patterns match too many rows (poor selectivity)
- Frequency thresholds prevent broad patterns from overwhelming the system
- Combining length and frequency provides better guardrails than either alone

### Alternatives Considered
- Length check only: Too coarse, doesn't account for data distribution
- Frequency check only: Doesn't prevent very short patterns from being used

### Trade-offs
- **Pros**: Better query performance, prevents runaway queries
- **Cons**: More complex logic, may reject some valid use cases

### References
- SPEC.md §8.3 (Query evaluation)
- SPEC.md §8.4 (Broad-pattern guardrails)

---

## Decision 9: Foreign Key Enforcement Timing
**Date:** 2026-01-28
**Status**: Decided

### Decision
Enforce foreign key constraints at statement time (MVP).

### Rationale
- Simpler implementation (no need to track deferred constraints)
- Errors are caught immediately (easier debugging)
- Consistent with many databases' default behavior
- Sufficient for most use cases

### Alternatives Considered
- Commit-time enforcement: More flexible but more complex
- Configurable per-constraint: Most flexible but most complex

### Trade-offs
- **Pros**: Simple, immediate error detection
- **Cons**: Less flexible for complex multi-statement transactions

### References
- SPEC.md §7.2 (Foreign keys)

---

## Decision 10: Error Handling Strategy
**Date:** 2026-01-28
**Status**: Decided

### Decision
Use categorized error codes with Result types for propagation, and define rollback behavior per error type.

### Rationale
- Categorized errors make it easier to handle different failure modes
- Result types provide compile-time safety for error handling
- Defined rollback behavior ensures predictable transaction semantics
- Detailed error messages aid debugging

### Alternatives Considered
- Exceptions only: Less structured, harder to categorize
- Error codes only: Less type-safe

### Trade-offs
- **Pros**: Structured, type-safe, predictable
- **Cons**: More boilerplate than exceptions

### References
- SPEC.md §13 (Error handling)
- TESTING_STRATEGY.md §8 (Error handling tests)

---

## Decision 11: Memory Management Strategy
**Date:** 2026-01-28
**Status**: Decided

### Decision
Use fixed-size memory pools with configurable limits and LRU eviction, with graceful degradation on OOM.

### Rationale
- Predictable memory usage
- LRU eviction provides good cache behavior
- Configurable limits allow tuning for different environments
- Graceful degradation prevents crashes

### Alternatives Considered
- Unbounded allocation: Simpler but risks OOM
- Fixed allocation only: Too inflexible

### Trade-offs
- **Pros**: Predictable, configurable, robust
- **Cons**: Requires tuning, may abort queries under memory pressure

### References
- SPEC.md §14 (Memory management)

---

## Decision 12: B+Tree Space Management
**Date:** 2026-01-28
**Status**: Decided

### Decision
Implement node split for MVP, defer merge/rebalance, use compaction for space recovery.

### Rationale
- Split is necessary for correctness (cannot overflow pages)
- Merge/rebalance adds significant complexity
- Compaction provides equivalent space recovery
- Delete-heavy workloads are less common in target use case

### Alternatives Considered
- Implement merge/rebalance in MVP: More complex but better space efficiency
- No compaction: Simpler but space bloat over time

### Trade-offs
- **Pros**: Simpler MVP, compaction provides space recovery
- **Cons**: May have temporary space bloat, requires periodic compaction

### References
- SPEC.md §17 (B+Tree space management)

---

## Decision 13: Index Statistics Strategy
**Date:** 2026-01-28
**Status**: Decided

### Decision
Use heuristic-based selectivity estimates for MVP, defer full statistics collection.

### Rationale
- Heuristics are sufficient for rule-based planner
- Full statistics collection adds complexity (maintenance, updates)
- Target workload has predictable access patterns
- Can add statistics later if needed

### Alternatives Considered
- Full statistics collection in MVP: More accurate but more complex
- No selectivity estimates: Too naive for good planning

### Trade-offs
- **Pros**: Simple, sufficient for MVP
- **Cons**: Less accurate than full statistics, may make suboptimal plan choices

### References
- SPEC.md §9.1 (Index statistics)

---

## Decision 14: Performance Targets
**Date:** 2026-01-28
**Status**: Decided

### Decision
Define concrete performance targets with P95 latency thresholds:
- Point lookup: P95 < 10ms
- FK join: P95 < 100ms
- Substring search: P95 < 200ms
- Bulk load (100k records): < 30s
- Crash recovery: < 5s for 100MB DB

### Rationale
- Concrete targets enable performance regression testing
- P95 focuses on tail latency (user experience)
- Targets are achievable with good implementation
- Provides clear acceptance criteria

### Alternatives Considered
- No targets: Hard to measure success
- P50 targets: Doesn't capture user experience
- Stricter targets: May be unrealistic for MVP

### Trade-offs
- **Pros**: Measurable, enables regression testing, clear acceptance criteria
- **Cons**: May constrain implementation choices

### References
- PRD.md §5.4 (Performance targets)
- TESTING_STRATEGY.md §6 (Performance regression testing)

---

## Decision 15: Testing Strategy Enhancements
**Date:** 2026-01-28
**Status**: Decided

### Decision
Add resource leak tests, performance regression tests, and error handling tests to the testing strategy.

### Rationale
- Resource leaks can cause long-running stability issues
- Performance regressions can degrade user experience over time
- Error handling is critical for robustness
- These tests catch issues that unit/property tests may miss

### Alternatives Considered
- Rely on manual testing: Too error-prone
- Add only some tests: Incomplete coverage

### Trade-offs
- **Pros**: Comprehensive testing, catches important issues
- **Cons**: More test code to maintain, longer CI runs

### References
- TESTING_STRATEGY.md §2.5 (Resource leak tests)
- TESTING_STRATEGY.md §6 (Performance regression testing)
- TESTING_STRATEGY.md §8 (Error handling tests)

---

## Future Decisions (To Be Made)

### Multi-process Concurrency
- Shared memory region design
- Inter-process locking strategy
- File locking semantics

### Cost-based Optimizer
- Statistics collection
- Cost model
- Plan enumeration

### Advanced Schema Features
- ALTER TABLE support
- Online schema migrations
- More data types

### PostgreSQL Wire Protocol
- Protocol subset to implement
- Authentication methods
- Extended query support

---

## Decision Template

When adding a new decision, use this template:

```markdown
## Decision N: [Title]
**Date:** YYYY-MM-DD
**Status**: [Decided | Proposed | Rejected]

### Decision
[What was decided]

### Rationale
[Why this decision was made]

### Alternatives Considered
- [Alternative 1]: [Pros/cons]
- [Alternative 2]: [Pros/cons]

### Trade-offs
- **Pros**: [Benefits]
- **Cons**: [Drawbacks]

### References
- [Links to relevant documents]
```
