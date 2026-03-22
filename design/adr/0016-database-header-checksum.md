# Database Header Checksum
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Add a CRC-32C checksum to the database header (page 1) covering all header fields. The checksum is stored in a dedicated field at a fixed offset.

### Rationale
- Page 1 (database header) corruption is catastrophic - it contains root page IDs, schema cookie, and format version
- Without a checksum, corrupted headers could lead to data loss or security issues
- CRC-32C is hardware-accelerated on modern CPUs (SSE 4.2) and sufficient for detecting accidental corruption
- Fixed offset allows validation before parsing other fields

### Header Format
```
Offset  Size  Field
0       16    Magic bytes ("DECENTDB\0\0\0\0\0\0\0\0")
16      4     Format version (u32)
20      4     Page size (u32)
24      4     Header checksum (CRC-32C of bytes 0-23 and 28-127)
28      4     Schema cookie (u32)
32      4     Root page ID for catalog (u32)
36      4     Root page ID for freelist (u32)
40      4     Freelist head pointer (u32)
44      4     Freelist count (u32)
48      8     Last checkpoint LSN (u64)
56      8     Reserved (u64)
64-127  64    Reserved for future use
```

### Checksum Calculation
1. Compute CRC-32C over bytes 0-23 (magic, version, page size)
2. Compute CRC-32C over bytes 28-127 (schema cookie, root pages, etc.)
3. Combine using polynomial arithmetic
4. Store result at offset 24

### Validation on Open
1. Read first 128 bytes
2. Verify magic bytes
3. Compute checksum over same ranges
4. If checksum mismatch: return ERR_CORRUPTION with page_id=1

### Alternatives Considered
- **xxHash64**: Faster but requires software implementation; CRC-32C has hardware support
- **Header at separate offset**: Would require two reads; keeping header at page 1 is simpler
- **No checksum**: Unacceptable for production database

### Trade-offs
- **Pros**: Detects corruption early, hardware-accelerated, minimal overhead
- **Cons**: Adds 4 bytes to header, requires checksum calculation on every header update

### References
- SPEC.md §3.2 (Main DB header)
- INTEL® SSE4 PROGRAMMING REFERENCE (CRC32 instruction)
