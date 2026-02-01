# 34. Compact INT64 Record Payload

**Date:** 2026-01-31
**Status:** Accepted

## Context

DecentDB's current record format (see [0030-record-format.md](0030-record-format.md)) stores `vkInt64` values as fixed 8-byte little-endian integers. While simple, this is highly inefficient for the common case of small integers, such as auto-incrementing primary keys, foreign keys, flags, and small counts.

In contrast, SQLite uses a variable-length encoding that allows small integers to consume as little as 1 byte of payload. This discrepancy is a primary contributor to the file size gap between DecentDB and SQLite (currently ~2-3x).

The goal is to adopt a compact encoding for `int64` values without sacrificing correctness or range.

## Decision

We will change the payload encoding for `vkInt64` to use **ZigZag encoding** followed by **Varint encoding**.

### 1. Encoding Scheme

The `vkInt64` payload will no longer be a fixed 8 bytes. Instead, it will be a variable-length sequence of bytes:

1.  **ZigZag Encode**: Map the signed `int64` to an unsigned `uint64`. This ensures that small negative numbers (e.g., -1) map to small unsigned integers, allowing efficient varint encoding.
    ```nim
    func zigzag(n: int64): uint64 =
      (cast[uint64](n) shl 1) xor cast[uint64](n shr 63)
    ```
    - `0` -> `0`
    - `-1` -> `1`
    - `1` -> `2`
    - `-2` -> `3`
    - ...

2.  **Varint Encode**: Encode the resulting `uint64` using the existing LEB128-style varint mechanism (already used for lengths).

### 2. Format Versioning

This is a breaking change to the on-disk format.

- The global `FormatVersion` in `db_header` is incremented as part of the format-change series that includes this ADR.
- **Current repo state**: `FormatVersion` is **`4`** (see ADR 0035).
- The engine will strictly enforce `header.formatVersion == FormatVersion`.
    - Opening a database with an older format version will fail with an "Unsupported format version" error.
    - **Migration**: For this pre-1.0 stage, no automatic migration (read-old-write-new) is provided in the engine itself. Users must rebuild databases (e.g., via export/import if tools existed, or simply recreating test DBs). Future upgrades to the vacuum tool could handle format migration.

### 3. Record Decoding

The decoder must handle variable-length payloads for `vkInt64`:
- The record format already stores the length of each value's payload (`[kind][len][payload]`).
- The decoder will read `len` bytes.
- If `len` indicates a varint (which it strictly doesn't "indicate" but contains), the decoder reads the payload bytes and decodes the varint from them.
- **Constraint**: The decoded `uint64` must fit in a valid varint (max 10 bytes).
- **Safety**: The decoder must validate that the decoded ZigZag value reverses to a valid `int64` (which is true for all `uint64`, but we must ensure we don't read past the declared `len`).
- **Correction**: Actually, we decode the varint *from* the payload bytes. We must ensure the varint consumes exactly or at most `len` bytes?
    - *Correction*: The `len` varint in the record header tells us exactly how many bytes are in the payload. We pass that slice to the varint decoder. The varint decoder should consume those bytes. To be robust, we should assert that `decodeVarint` consumes the expected number of bytes, or simply trust `len` and decode the varint found there.
    - Since `encodeVarint` is unique for a given value (no padding), we can just decode the varint.

## Consequences

### Positive
- **Size Reduction**: Small integers (approx -64 to +64) will encode to 1 byte (plus 1 byte for length, 1 byte for kind = 3 bytes total overhead vs 10 bytes previously).
- **SQLite Parity**: Moves closer to SQLite's efficiency for integer-heavy schemas.
- **Simplicity**: Reuses existing varint primitives.

### Negative
- **CPU Overhead**: Decoding a varint and ZigZag decoding is slightly more expensive than a direct 8-byte memory copy / load. However, the reduction in I/O and cache pressure is expected to yield a net performance gain.
- **Breaking Change**: Incompatible with existing DB files.

## Alternatives Considered

- **Direct cast to uint64**: `encodeVarint(cast[uint64](int64Val))`.
    - *Rejected*: Negative numbers (like -1) become huge unsigned integers (`0xFF...FF`), taking maximum varint space (10 bytes). ZigZag is strictly better for signed integers where small negatives are common.

- **Variable-length integer distinct from Varint**: e.g., using the `kind` byte to signal 1, 2, 4, 8 byte width.
    - *Rejected*: Adds complexity to the type system (multiple `vkInt` kinds) or the kind byte logic. The current `[kind][len][payload]` structure is generic; changing the payload content is less invasive than changing the `kind` semantics or outer envelope.
