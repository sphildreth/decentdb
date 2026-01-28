# Performance Targets
**Date:** 2026-01-28
**Status:** Accepted

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
