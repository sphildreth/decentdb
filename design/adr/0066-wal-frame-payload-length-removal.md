## WAL Frame Payload Length Removal
**Date:** 2026-02-05  
**Status:** Accepted

### Decision
Remove the `payload_length` field from WAL frame headers in format v7. Payload sizes are derived from frame type and database page size: page frames use `pageSize`, commit frames use 0 bytes, and checkpoint frames use 8 bytes.

### Rationale
- The payload length is redundant for fixed-size page frames and fixed-size commit/checkpoint frames.
- Eliminates 4 bytes per frame and reduces per-frame encoding work.

### Alternatives Considered
- Keep payload length for all frames (status quo): simple but redundant.
- Keep payload length only for non-page frames: adds format complexity and still requires mixed parsing logic.

### Trade-offs
- WAL readers must know the database page size to parse frames.
- This is a format change and requires a version bump (v7).

### References
- `design/SQLITE_PERF_GAP_PLAN.md` (Section 1: WAL Frame Format Overhead)
- `design/SPEC.md` §4.1 (WAL frame format)
