# Memory Management Strategy
**Date:** 2026-01-28
**Status:** Accepted

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
