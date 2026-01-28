## WAL Frame Format (Phase 3)
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Use a fixed WAL frame header with type, page_id, payload_size, followed by payload, CRC-32C checksum, and LSN.

### Rationale
- Matches `design/SPEC.md` §4.1
- Simple to parse and validate during recovery

### Alternatives Considered
- Separate checksum blocks (more complex)
- Variable-length headers (harder to parse)

### Trade-offs
- Adds 16 bytes of trailer per frame

### References
- `design/SPEC.md` §4.1
- `design/IMPLEMENTATION_PHASES.md` Phase 3 checklist
