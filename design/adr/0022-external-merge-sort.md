# 22. External Merge Sort (Supersedes ADR-0021)

Date: 2026-01-28

## Status

Accepted

## Context

The user has identified that "very large sort operations" are likely in the near future for the Music Library workload (e.g., `SELECT * FROM tracks ORDER BY title` on 9.5M rows).

Prior ADR-0021 recommended "In-Memory Only" sorting for MVP simplicity. However, given the explicit requirement for scale, we must implement **External Merge Sort** to handle datasets larger than available RAM.

## Decision

We will implement **External Merge Sort** in the `Exec` layer.

### Algorithm: Standard K-Way External Merge Sort

1.  **Phase 1: Run Generation (Buffering)**
    *   The `Sort` operator consumes input rows into an in-memory buffer (`sort_buffer_size`, default 16MB).
    *   When the buffer is full:
        1.  Sort the buffer in-memory (using Nim's native `algorithm.sort`).
        2.  Serialize the sorted rows to a strictly temporary file (managed by a `TempFile` abstraction).
        3.  Clear the buffer and continue.
    *   Keep a list of `Run` objects (file path, row count).

2.  **Phase 2: Merging**
    *   If `count(Runs) == 0`: Return in-memory iterator.
    *   If `count(Runs) > 0`:
        1.  Flush any remaining rows in memory as the final Run.
        2.  Open a `Stream` for every Run file (set read buffer to ~64KB per stream).
        3.  Initialize a **Min-Heap** (Priority Queue) with the first row from each Run.
        4.  **Next()**: Pop min row from Heap, return to caller. Read next row from that Run's stream and push to Heap.
        5.  Clean up: Delete all temp files when the Sort iterator is closed or the transaction ends.

### Constraints & Simplifications for MVP

*   **Max Open Files**: To avoid `EMFILE` errors, we will limit the merge fan-in to **64**.
    *   If `(Total Data Size / Sort Buffer Size) > 64`, we would technically need a multi-pass merge.
    *   *MVP Decision*: With 16MB buffer, 64 runs = 1GB of sorted data. This covers the 9.5M tracks use case ample enough (9.5M * 100 bytes ≈ 950MB). If exceeded, return `ERR_SORT_TOO_LARGE` (defer multi-pass merge to post-MVP).
*   **Serialization**: Use a simple `Length (u32) + EncodedRow` binary format for temp files. No page overhead needed.

## Consequences

*   **Complexity**: Significantly higher complexity in `Exec` module (state machine, I/O management, heap).
*   **Robustness**: The database can now sort datasets well beyond RAM limits.
*   **Performance**: Queries spilling to disk will be much slower (I/O bound).
*   **Resource Management**: Strict cleanup of temporary files is required (RAII pattern or equivalent in Nim).
