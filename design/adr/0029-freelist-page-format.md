# Freelist Page Format
**Date:** 2026-01-28
**Status:** Accepted

## Decision
Use a single-chain freelist stored in dedicated freelist pages. The database header points to the head page, and each freelist page contains a next pointer plus a compact list of free page IDs.

## Rationale
- Simple persistent format suitable for the 0.x baseline
- Cheap to append new free pages and pop for allocations
- Fits within Phase 1 goals without requiring complex structures

## Format (per freelist page)
```
Offset  Size  Field
0       4     Next freelist page ID (u32, 0 = end)
4       4     Count of entries (u32)
8       N*4   Free page IDs (u32 list, count entries)
```

Capacity per page:
```
capacity = floor((page_size - 8) / 4)
```

## Allocation/Free Rules
- `freelist_head` in the DB header points to the first freelist page (0 if none).
- `freelist_count` in the DB header counts total free pages (not freelist pages).
- **Allocate**:
  - Pop from head page list if `freelist_count > 0`.
  - If the head page list becomes empty, advance `freelist_head` to its `next`.
- **Free**:
  - If there is a head page with space, push into it.
  - Otherwise, create a new freelist page, set it as head, and store the freed page ID there.

## References
- `design/SPEC.md` §3.2 (header fields)
- `design/IMPLEMENTATION_PHASES.md` Phase 1 checklist
