## B+Tree Page Layout (Read Path)
**Date:** 2026-01-28
**Status:** Superseded by ADR 0035 (FormatVersion 4)

### Decision
Use a compact header and sequential cell layout for B+Tree pages.

Header (common):
```
Offset  Size  Field
0       1     Page type (1=internal, 2=leaf)
1       1     Reserved
2       2     Cell count (u16)
4       4     Right-most child (internal) or next leaf (leaf)
```

Internal cells:
`[key u64][child u32]` repeated `cell_count` times.

Leaf cells:
`[key u64][value_len u32][overflow_page u32][value bytes]` repeated.

### Rationale
- Simple to build and parse for MVP read path
- Supports in-order leaf traversal via next-leaf pointer

### Alternatives Considered
- Slot directories with packed payloads
- Prefix-compressed keys

### Trade-offs
- Linear scans within pages
- Not optimized for updates (write path deferred to Phase 4)

### References
- `design/SPEC.md` §2.1 btree/
- `design/IMPLEMENTATION_PHASES.md` Phase 2 checklist
