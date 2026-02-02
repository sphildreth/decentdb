# Fine-grained WAL Locking Strategy

**Date:** 2026-02-01
**Status:** Draft

## Context
Current WAL locking uses a coarse-grained mutex around the entire append operation. This serializes all writers and can become a bottleneck under high concurrency.

## Proposal
1. Decompose the single WAL lock into:
   - `appendLock`: protects the physical file append offset
   - `indexLock`: protects the in-memory WAL index map
   - `syncLock`: protects the fsync operation

2. Lock Ordering:
   - `appendLock` -> `indexLock` -> `syncLock`

3. Deadlock Avoidance:
   - Strict ordering enforced.
   - Group commit can batch multiple appends before taking `syncLock`.

## Consequences
- Allows concurrent formatting of frames while one thread is fsyncing? (Single writer model limits this benefit, but multi-writer preparation)
- Complexity in error handling (partial failures).
