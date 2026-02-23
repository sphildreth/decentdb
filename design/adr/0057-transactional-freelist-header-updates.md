## Transactional Freelist/Header Updates
**Date:** 2026-02-02
**Status:** Accepted (2026-02-23)

### Decision
Remove direct `updateHeader()` + fsync calls from `allocatePage()` and `freePage()`. The in-memory header is updated during transactions but only persisted to disk at checkpoint, which already writes the header after flushing WAL pages to the DB file. On open, the freelist header fields (`freelistHead`, `freelistCount`) are reconstructed by scanning the freelist page chain.

Additionally, track pages freed during a transaction (`txnFreedPages`) so that rollback correctly restores them to the allocated state.

### Rationale
Today, `allocatePage`/`freePage` mutate freelist structures and call `updateHeader` which ultimately fsyncs the DB header. A crash after these writes but before WAL commit can make the main DB file reflect uncommitted structural state, which is an ACID/atomicity risk.

The freelist **page data** is already WAL-protected — `writeFreelistPage()` goes through `writePage()` → cache → dirty tracking → WAL. Only the DB header fields (`freelistHead`, `freelistCount`) bypass the WAL via direct `writeHeader()` + fsync.

The header is a derived cache of the freelist chain state. By deferring its persistence to checkpoint (which already writes the header), we eliminate the crash window where the header reflects uncommitted state.

### Implementation
1. **Remove `updateHeader()` calls** from `allocatePage()` (2 call sites) and `freePage()` (2 call sites). The `pager.header` struct is still updated in memory.
2. **Add `txnFreedPages: seq[PageId]`** to Pager for rollback tracking. `rollbackTxnPageFrees()` re-allocates freed pages on transaction abort.
3. **Add `reconstructFreelistHeader()`** — on DB open, after WAL recovery, walk the freelist chain from `freelistHead` to verify/rebuild `freelistCount`. This is O(F/C) page reads where C ≈ 1000 for 4KB pages.
4. **Checkpoint** already persists the correct header since it captures the current in-memory state after all WAL pages are written to the DB file.

### Format Impact
None. No WAL frame format change, no DB header layout change, no page format change. Existing databases open correctly.

### Alternatives Considered
- **Defer header/fsync until commit/checkpoint** — chosen approach (simplest, no format change).
- **WAL-log allocation/free intents** — rejected: requires new WAL frame types and complex undo logic.
- **Introduce a separate "freelist WAL" stream** — rejected: unnecessary complexity; freelist pages are already WAL-logged.

### Trade-offs
- **Pro:** Eliminates ACID atomicity violation for freelist mutations.
- **Pro:** Removes unnecessary fsyncs during transactions (performance improvement).
- **Pro:** No WAL format change needed.
- **Con:** Small startup cost — freelist chain scan on open. Negligible for typical databases.

### References
- design/adr/0051-freelist-atomicity.md
- design/adr/0010-error-handling-strategy.md
- GitHub Issue #23
