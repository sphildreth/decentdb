## Record Encoding Format
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Define a compact record encoding using a varint field count followed by per-field entries of:
`[type u8][length varint][payload bytes]`. Overflow pointers for large TEXT/BLOB use dedicated type codes.

### Rationale
- Simple to parse and suitable for MVP
- Explicit lengths allow variable-sized fields
- Dedicated overflow types avoid ambiguity with inline data

### Alternatives Considered
- Fixed-width records (wastes space)
- Tagged unions without lengths (requires type-specific decoding)

### Trade-offs
- Slight overhead from per-field lengths
- Overflow types add extra type codes

### References
- `design/SPEC.md` §2.1 record/
- `design/IMPLEMENTATION_PHASES.md` Phase 2 checklist
