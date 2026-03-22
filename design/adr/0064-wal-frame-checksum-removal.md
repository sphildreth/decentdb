## WAL Frame Checksum Removal
**Date:** 2026-02-05  
**Status:** Accepted

### Decision
Remove per-frame CRC32C validation for WAL frames in format v5. The checksum field remains reserved in the trailer and is written as zero, but it is no longer computed or validated during recovery.

### Rationale
- Commit latency is dominated by per-frame CRC32C over full 4KB payloads.
- SQLite achieves lower commit latency without per-frame checksums.
- We already have payload-size validation, frame-type validation, and LSN sanity checks; these detect most torn/partial writes.

### Alternatives Considered
- Keep per-frame CRC32C (current): too expensive for target commit latency.
- Per-transaction checksum: requires WAL header/salt and additional metadata.
- Hardware CRC32C (SSE4.2): not portable across platforms without conditional paths.
- WAL header with salt and lightweight validation: deferred.

### Trade-offs
- We lose detection of payload corruption that preserves header/trailer structure.
- Recovery now trusts frame payload content once length/type checks pass.
- This is a durability/corruption-detection semantic change and requires a format version bump.

### References
- `design/SQLITE_PERF_GAP_PLAN.md` (Section 1: WAL Frame Format Overhead)
- `design/SPEC.md` §4.1 (WAL frame format)
