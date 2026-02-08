## B+Tree Prefix Compression
**Date:** 2026-02-02
**Status:** Deferred

### Decision
Draft a design for prefix compression in btree pages to reduce key storage and improve fanout.

### Rationale
Large keys (especially future TEXT/BLOB comparable keys) can reduce btree fanout and increase IO. Prefix compression can improve read performance and storage efficiency but is a page layout / persistent format change.

### Alternatives Considered
- No compression (status quo).
- Prefix compression with shared prefix per node.
- Delta encoding between adjacent keys.

### Trade-offs
- Better performance and space usage vs increased complexity in page decoding/updates.

### References
- design/adr/0032-btree-page-layout.md
- design/adr/0035-btree-page-layout-v2.md
