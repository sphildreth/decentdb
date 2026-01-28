# 20. Mandatory Overflow Pages for MVP

Date: 2026-01-28

## Status

Accepted

## Context

The Product Requirements Document (PRD) specifies support for `TEXT` and `BLOB` data types. The target use case is a Music Library, which includes standard metadata but also potentially **Cover Art images** (often 50KB - 5MB) and **Lyrics** (potentially > 4KB).

The core engine uses fixed-size pages (default 4KB). Without a mechanism to handle records larger than a single page, the engine cannot support the target use case. The initial SPEC listed Overflow Pages as "Optional / MVP+".

## Decision

We promote **Overflow Pages** from "Optional" to **Mandatory MVP Requirement**.

1.  **Storage**: Records that exceed a specific threshold (e.g., `page_size - header_overhead`) will store a pointer to a linked list of Overflow Pages.
2.  **Implementation**:
    *   Overflow pages are strictly distinct page types (Type `0x03`).
    *   They form a singly linked list.
    *   The main B+Tree leaf cell contains the "head" overflow page ID and the total length.
3.  **Complexity**: This creates complexity for the `FreeList` manager (freeing a row means traversing and freeing the overflow chain) and the `Pager` (fragmentation).
4.  **Optimization**: For MVP, we will not implement advanced "Modify Overflow in Place" logic. Updates to BLOBs will likely involve "Delete old chain + Allocate new chain".

## Consequences

*   **Scope Increase**: `record`, `btree`, and `pager` modules need to handle multi-page value logic immediately.
*   **Capabilities**: The database can officially store "Decent" sized blobs (images, large text).
*   **Performance**: Large BLOB I/O will be slower than inline data, but this is expected.
