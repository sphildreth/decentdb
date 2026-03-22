## B+Tree Prefix Compression
**Date:** 2026-02-02
**Status:** Accepted

### Decision
Implement delta encoding for B+Tree keys on both leaf and internal pages.

### Implementation
- Keys are delta-encoded: first key stored as full varint, subsequent keys as (current - previous) varint.
- Page header byte[1] stores flags: `PageFlagDeltaKeys = 0x01` indicates delta encoding is active.
- Backward compatible: pages with byte[1]=0 use old absolute encoding.
- Applied to BOTH leaf and internal pages via `encodeLeaf`/`encodeInternal`.
- All inline decode paths updated: `find`, `containsKey`, `findChildInPage`, `findLeafLeftmost`, `scanLeafLastKey`, `cursorNextStream`, and cached index builders.
- Fast-path append in `insertRecursive` encodes deltas when the page flag is set.

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
