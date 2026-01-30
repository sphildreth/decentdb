## Group Commit and WAL Batching Policies (Post-MVP)
**Date:** 2026-01-30
**Status:** Accepted (Post-MVP Deferral)

### Decision

Group commit and WAL batching policies are **deferred as post-MVP features**. The default durability mode remains synchronous fsync-on-every-commit with no weakening of guarantees. An opt-in group commit mode may be added in a future release after MVP, following the completion of this ADR and implementation of required infrastructure.

### Rationale

1. **MVP Priority:** The current MVP focuses on correctness and durability guarantees. Synchronous fsync-on-commit provides the strongest durability guarantee and is simplest to implement correctly.

2. **Complexity vs. Benefit:** Group commit requires:
   - Transaction queueing and batching logic
   - Bounded delay timers and timeout handling
   - Crash recovery testing for partial batches
   - API changes to expose durability mode options
   - Significant additional testing for all durability modes

3. **Performance Alternative:** For high-throughput workloads, the bulk load API (ADR-0017, ADR-0027) provides a better solution with explicit durability tradeoffs.

4. **Safety:** Weakening durability-by-default is explicitly prohibited per project priorities (Priority #1: Durable ACID writes).

### Alternatives Considered

**Option A: Implement group commit for MVP**
- Rejected: Adds significant complexity and testing burden to MVP timeline
- Risk of introducing durability bugs in critical path

**Option B: Weaken default durability (async commit)**
- Rejected: Violates project Priority #1 (Durable ACID writes)
- "Works in tests, dies in production" risk

**Option C: Defer to post-MVP (Selected)**
- Accepted: Keeps MVP focused and correct
- Allows time for proper design and testing
- Maintains strong default guarantees

### Future Implementation Requirements

If group commit is implemented post-MVP, the following must be addressed:

1. **ADR Update:** This ADR must be updated with specific design details
2. **Durability Modes:** Define explicit modes:
   - `FULL` (default): fsync on every commit
   - `BATCHED`: Group commits with bounded delay (e.g., max 10ms)
3. **API Design:** Add `SET DURABILITY MODE` statement or connection option
4. **Crash Testing:** Comprehensive crash-injection tests for batch mode
5. **Documentation:** Clear documentation of tradeoffs

### References

- PRD.md: Priority #1 - Durable ACID writes
- design/reviews/2026-01-28-SUMMARY.md: P3 items (group commit deferred)
- ADR-0017: Bulk load API (alternative for high-throughput)
