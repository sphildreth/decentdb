# 35. Compact B+Tree Page Layout

**Date:** 2026-01-31
**Status:** Accepted

## Context

The current B+Tree page layout uses fixed-width headers for both leaf and internal cells.

-   **Leaf Cells**: 16 bytes overhead (`key` u64 + `value_len` u32 + `overflow` u32).
-   **Internal Cells**: 12 bytes overhead (`key` u64 + `child` u32).

This structure is simple but wasteful, especially for tables with small keys and values (e.g., specific mapping tables or narrow rows). SQLite and other engines use variable-length encodings to minimize this overhead.

## Decision

We will adopt a compact, variable-length layout for B+Tree pages (Format Version 4).

### 1. Leaf Cell Format

Old Format (16 bytes + payload):
```
[key: u64-le] [len: u32-le] [overflow: u32-le] [payload...]
```

New Format (Variable length):
```
[key: Varint] [control: Varint] [payload...]
```

**Control Field Logic:**
-   `control` holds both the payload length and the overflow status.
-   `is_overflow = (control & 1)`
-   `value = (control >> 1)`

**Interpretation:**
-   **If `is_overflow == 0`**:
    -   `value` represents the **Payload Length** (in bytes).
    -   The `payload` follows immediately.
-   **If `is_overflow == 1`**:
    -   `value` represents the **Overflow Page ID**.
    -   The cell has no inline payload.

### 2. Internal Cell Format

Old Format (12 bytes):
```
[key: u64-le] [child: u32-le]
```

New Format (Variable length):
```
[key: Varint] [child: Varint]
```

### 3. Format Versioning

-   **FormatVersion**: Incremented to **4**.
-   The engine will reject older versions.
-   No automatic migration provided (requires rebuild).

### 4. Implementation Details

-   **Parsing**: Cells are parsed sequentially from the page start. This aligns with the current implementation of `readLeafCells` which deserializes the entire page into memory vectors.
-   **Space Savings**:
    -   Small keys (0-127): 8 bytes -> 1 byte.
    -   Small values (<128 bytes): 4 bytes -> 1 byte.
    -   No overflow (common case): 4 bytes -> 0 bytes (integrated into control).
    -   **Total Typical Savings**: ~12-14 bytes per row.

## Consequences

### Positive
-   Significant reduction in database size for small-row tables.
-   Increases fan-out for internal pages (more keys per page), reducing tree height.

### Negative
-   Slightly higher CPU cost for Varint decoding.
-   Variable-length cells prevent O(1) random access by index within a page (though our current `readLeafCells` linear scan approach makes this moot for now).

## Alternatives Considered

-   **Slot Directory**: Moving to a "heap + slot directory" model (like Slotted Page) would allow binary searching the page without deserialization and easier free-space management.
    -   *Rejected (Phase 2)*: This is a larger refactor. The current goal is simple overhead reduction. The current "deserialize all" approach is simple and correct for the current scale.

-   **Prefix Compression**: Compressing sequential keys.
    -   *Rejected (Phase 2)*: Adds complexity to search logic (stateful iteration). Varint encoding captures some "small key" benefit for integer keys automatically.
