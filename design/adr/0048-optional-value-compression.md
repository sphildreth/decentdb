# ADR 0048: Optional Value Compression

**Date:** 2026-01-31  
**Status:** Accepted  
**Context:** Storage size reduction

## Context

DecentDB currently stores `TEXT` and `BLOB` values as raw bytes. For large values, this wastes significant space compared to compressed storage (e.g. SQLite's ZIPVFS or Postgres TOAST compression).

Large values spill to overflow pages when they exceed the page size (minus headers). Compression can:
1.  Keep more values inline (avoiding overflow page pointers and extra I/O).
2.  Reduce the number of overflow pages when spilling occurs.

However, compression adds CPU overhead and a dependency.

## Decision

We will implement **storage-internal transparent compression** for `TEXT` and `BLOB` values.

1.  **Algorithm:** **Zlib (Deflate)** via Nim's standard `zip/zlib`.
    *   *Reasoning:* Available without adding new dependencies; good size/CPU tradeoff for the 0.x baseline.

2.  **Storage Format:**
    *   We introduce new `ValueKind` variants:
        *   `vkTextCompressed = 8`
        *   `vkBlobCompressed = 9`
        *   `vkTextCompressedOverflow = 10`
        *   `vkBlobCompressedOverflow = 11`
    *   The payload for these kinds is the raw compressed bytes.
    *   The engine (in `record.nim`) automatically decompresses when decoding.

3.  **Compression Policy:**
    *   Compression is applied **opportunistically** during `encodeRecord` (or `normalizeValues` in `storage.nim` which handles overflow).
    *   **Threshold:** Only compress if value length > `COMPRESSION_THRESHOLD` (e.g. 128 bytes) AND compression saves at least X% (e.g. 10% or 20 bytes).
    *   This prevents negative compression for small strings.

4.  **SQL Interface:**
    *   No change to SQL. `SELECT` always returns decompressed data.
    *   Users see standard `TEXT` / `BLOB`.

5.  **Backward Compatibility:**
    *   New database files with compressed values cannot be read by older versions of the engine (due to unknown `ValueKind`).
    *   DecentDB already enforces a strict `FormatVersion` gate at open time. Compressed value kinds are considered part of the current on-disk format used by this repository.

## Detailed Design

### Record Format Changes

`src/record/record.nim`:

```nim
type ValueKind* = enum
  ...
  vkTextCompressed = 8
  vkBlobCompressed = 9
```

### Write Path (`encodeValue` / `normalizeValues`)

In `src/storage/storage.nim`, `normalizeValues` may opportunistically compress `vkText`/`vkBlob` values.

- If compression is beneficial and the compressed bytes can be stored inline, the value kind becomes `vkTextCompressed` / `vkBlobCompressed`.
- If compression is beneficial but the compressed bytes must be stored in an overflow chain, the value kind becomes `vkTextCompressedOverflow` / `vkBlobCompressedOverflow`.
- If compression is not beneficial, the value stays as `vkText` / `vkBlob` (or becomes `vkTextOverflow` / `vkBlobOverflow` if it must spill).

### Read Path (`decodeRecord` / `decodeRecordWithOverflow`)

*   `decodeValue`: Reads `vkTextCompressed` -> stores bytes in `Value`.
*   `decodeRecordWithOverflow`:
    *   If `vkTextCompressedOverflow`:
        *   Read overflow chain -> `bytes`.
        *   Decompress `bytes`.
        *   Set kind to `vkText`.
    *   If `vkTextCompressed` (inline):
        *   Decompress `bytes`.
        *   Set kind to `vkText`.

This ensures higher layers (SQL engine) only see `vkText` / `vkBlob`.

## Consequences

*   **Positive:** Significant space savings for large text.
*   **Negative:** CPU overhead on read/write.
*   **Negative:** Dependency on Zlib/Miniz.

## Implementation status

Implemented using Nim's standard `zip/zlib` and covered by unit tests ("Value Compression"). Compression is applied opportunistically during storage normalization and is transparent to higher layers (decoded values are exposed as normal `vkText`/`vkBlob`).

## References

*   [SQLite ZIPVFS](https://www.sqlite.org/zipvfs/doc/trunk/www/readme.wiki)
*   [PostgreSQL TOAST Compression](https://www.postgresql.org/docs/current/storage-toast.html)
