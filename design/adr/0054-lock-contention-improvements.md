# Lock Contention Improvements

**Date:** 2026-02-01
**Status:** Draft

## Context
High contention observed on the catalog lock (`schemaLock`) and pager cache locks during mixed read/write workloads.

## Proposal
1. **Catalog Lock:** Switch to a Reader-Writer lock (RWLock) to allow multiple concurrent readers (planning phase) while writer holds exclusive lock only during DDL.
   - Currently `schemaLock` is already conceptually RW, but verify implementation efficiency.

2. **Page Cache Sharding:** Ensure shard count scales with cores (already implemented with splitmix64).
   - Verify false sharing padding on shard locks.

3. **Copy-on-Write for Catalog:** 
   - Readers get a ref-counted snapshot of the catalog.
   - Writers create a new catalog revision.
   - Removes need for holding `schemaLock` during query execution.

## Consequences
- Reduces read blocking during long running queries.
- Increases memory usage for catalog snapshots.
