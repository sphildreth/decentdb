## WAL Frame LSN Removal (Offset-based LSN)
**Date:** 2026-02-05  
**Status:** Accepted

### Decision
Remove the per-frame LSN field from WAL frame trailers in format v6. LSNs are now derived from WAL byte offsets (frame end offset), and `wal_end_lsn` is the WAL end offset after the last committed frame.

### Rationale
- Per-frame LSN encoding adds 8 bytes per frame and extra per-frame writes.
- The WAL byte offset is already monotonic and sufficient for ordering, snapshot visibility, and checkpoint decisions.
- Removing the field reduces frame size and simplifies encoding.

### Alternatives Considered
- Keep per-frame LSN (status quo): retains extra bytes and encode work.
- Reintroduce a global sequence counter stored in the WAL header: requires a new WAL header format and recovery metadata.

### Trade-offs
- LSN values now depend on frame sizes (byte offsets) rather than logical sequence numbers.
- This is a format change and requires a format version bump (v6).

### References
- `design/SQLITE_PERF_GAP_PLAN.md` (Section 1: WAL Frame Format Overhead)
- `design/SPEC.md` §4.1 (WAL frame format)
