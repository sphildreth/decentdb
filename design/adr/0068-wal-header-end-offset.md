## WAL header with logical end offset (format v8)
**Date:** 2026-02-06
**Status:** Accepted

### Decision
Add a fixed-size WAL header that stores a **logical end offset** (`wal_end_offset`)
to allow preallocation and mmap-based appends without per-commit truncation.
The WAL header becomes part of the WAL file format (format v8).

### Rationale
The zero-copy/mmap WAL write path needs a stable, preallocated file size to avoid
per-commit `truncate()` overhead. Without a header, recovery uses the file size
to determine the WAL end, which breaks with preallocation. A logical end offset
in the header preserves recovery correctness while enabling mmap-based appends.

### Alternatives Considered
- Keep truncating the WAL to physical end (precludes mmap preallocation).
- Infer end by scanning and stop on invalid frames (too fragile without checksums).
- Per-commit header fsync (adds extra syscall and hurts commit latency).

### Trade-offs
- WAL format change (requires format bump and migration notes).
- Minor additional header write per commit (mmap path updates header in memory).
- Slightly more recovery logic (header validation + bounded scan).

### References
- `design/SQLITE_PERF_GAP_PLAN.md` (WAL/mmap follow-up)
