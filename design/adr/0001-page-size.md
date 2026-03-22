# Page Size (4096 bytes default)
**Date:** 2026-01-28
**Status:** Accepted

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
- Dynamic page sizing: Too complex for the 0.x baseline

### Trade-offs
- **Pros**: Good default for most workloads, configurable for special cases
- **Cons**: Requires decision at database creation time, cannot change after creation

### References
- SPEC.md §2.1 (pager module)
- SPEC.md §16 (configuration system)
