# ADR 0048: Optional Value Compression

**Date:** 2026-01-31  
**Status:** Proposed  
**Context:** Phase 4 of [SQLITE_GAPS_PLAN_V2.md](../../design/SQLITE_GAPS_PLAN_V2.md)

## Context

DecentDB currently stores `TEXT` and `BLOB` values as raw bytes. For large values, this wastes significant space compared to compressed storage (e.g. SQLite's ZIPVFS or Postgres TOAST compression).

Large values spill to overflow pages when they exceed the page size (minus headers). Compression can:
1.  Keep more values inline (avoiding overflow page pointers and extra I/O).
2.  Reduce the number of overflow pages when spilling occurs.

However, compression adds CPU overhead and a dependency.

## Decision

We will implement **storage-internal transparent compression** for `TEXT` and `BLOB` values.

1.  **Algorithm:** Zstandard (zstd) or Zlib/Deflate.
    *   *Decision:* **Zlib (Deflate)** via Nim's standard `zip/zlib` or a lightweight wrapper if needed.
    *   *Reasoning:* It is ubiquitous, standard in Nim, and offers a good balance. Zstd is better but requires adding a larger C dependency (though feasible). Given the "no major dependencies" constraint, standard zlib is safer unless we explicitly decide to vendor Zstd.
    *   *Refinement:* We will use **Miniz** (vendored or via Nim's `zippy` if allowed) or standard `zlib`. Let's assume **Zlib** for now as it's often available on the host system or easily statically linked.

2.  **Storage Format:**
    *   We introduce new `ValueKind` variants:
        *   `vkTextCompressed = 8`
        *   `vkBlobCompressed = 9`
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
    *   We should bump `FormatVersion` or feature flags if we enforce strict compatibility. For this MVP extension, we assume it's acceptable to upgrade the format.

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

In `src/storage/storage.nim`:
When `normalizeValues` encounters a large `vkText` or `vkBlob`:
1.  Attempt compression.
2.  If `compressed_len < original_len`:
    *   If `compressed_len` fits inline: store as `vkTextCompressed` / `vkBlobCompressed` inline.
    *   If `compressed_len` still needs overflow: store compressed bytes in overflow pages (using `vkTextCompressed` / `vkBlobCompressed` but pointing to overflow). Wait, `vkTextOverflow` stores a PageId.
    *   *Correction:* We need orthogonal concepts: "Is it compressed?" and "Is it in overflow?".
    *   Current `ValueKind` mixes type (`Text`/`Blob`) and storage (`Overflow`).
    *   Adding `vkTextCompressed` and `vkTextCompressedOverflow` combinatorial explosion is messy.

**Alternative Design (Header Flag):**
We can't easily change the varint header without breaking everything.
So we stick to `ValueKind`.

New Kinds:
*   `vkTextCompressed` (Inline compressed text)
*   `vkBlobCompressed` (Inline compressed blob)
*   `vkTextCompressedOverflow` (Overflow, content is compressed)
*   `vkBlobCompressedOverflow` (Overflow, content is compressed)

Actually, if it's in overflow, the `Value` struct just holds `overflowPage` and `overflowLen`. The *content* of the overflow chain is just bytes. The `kind` tells us how to interpret those bytes.

So yes, we need:
```nim
  vkTextCompressed
  vkBlobCompressed
  vkTextCompressedOverflow
  vkBlobCompressedOverflow
```

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

## References

*   [SQLite ZIPVFS](https://www.sqlite.org/zipvfs/doc/trunk/www/readme.wiki)
*   [PostgreSQL TOAST Compression](https://www.postgresql.org/docs/current/storage-toast.html)
