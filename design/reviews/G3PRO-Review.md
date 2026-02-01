# Code Review: Indexing & Transaction Atomicity

**Date:** 2026-02-01
**Reviewer:** G3PRO
**Scope:** `src/engine.nim`, `src/storage/storage.nim`, `src/wal/wal.nim`

## 1. ACID & Atomicity Analysis

### Focus: "Flush Deltas" Phase
We analyzed the transaction commit flow in `src/engine.nim`, specifically focusing on the `flushTrigramDeltas` operation. This operation occurs *after* a transaction is deemed ready to commit but *before* the Write-Ahead Log (WAL) commit frame is written.

**Finding: Correctness Verified (Safe)**
The atomicity of transactions involving full-text search (trigram) updates is preserved, albeit via a blunt mechanism.

*   **Mechanism:** `commitTransaction` calls `flushTrigramDeltas` to push in-memory index changes into the B-Tree pages residing in the Page Cache.
*   **Partial Failure Scenario:** If `flushTrigramDeltas` fails halfway (e.g., due to an I/O error reading a B-Tree page or an OOM error), or if the subsequent WAL write fails, the database state in memory is inconsistent (dirty pages exist for a failed transaction).
*   **Recovery:** The code handles this by catching the error and immediately calling `clearCache(db.pager)`.
    *   *Reference:* `src/engine.nim:1627`
    *   *Reference:* `src/pager/pager.nim:443`
    *   `clearCache` indiscriminately drops *all* pages from the cache. Since the WAL frame was never written, the disk remains in the state of the last successful commit. The in-memory dirty pages are discarded, effectively rolling back the transaction.

**Critique:**
While safe, this "nuke the cache" approach is heavy-handed. It ensures correctness but guarantees a performance penalty (cold cache) for all concurrent readers and subsequent writers immediately following a failed transaction.

## 2. Performance Analysis

### A. Trigram Index Write Amplification (CRITICAL)
**Status:** **Confirmed Critical Bottleneck**

The current implementation of the Trigram Inverted Index behaves as a simple "Read-Modify-Write" blob store, which is catastrophic for common tokens (stop words).

**Technical Walkthrough:**
1.  **Read:** `getTrigramPostingsWithDeltas` loads the *entire* existing postings list for a trigram (e.g., "the") into memory.
2.  **Decode:** The blob is decoded into a `seq[uint64]`.
3.  **Merge:** `applyPostingDeltas` merges new row IDs into this sequence in-memory.
4.  **Write:** `storePostings` re-encodes the entire sequence and writes it back to the B-Tree.

**Impact:**
*   **Big O Complexity:** This is an **O(N)** operation per transaction, where N is the total number of rows containing the trigram.
*   **Scenario:** If 1,000,000 documents contain the trigram "the", adding the 1,000,001st document requires reading and rewriting the existing 1,000,000 IDs.
*   **IOPS Saturation:** A bulk load of text data will rapidly saturate IOPS as the "stop word" lists grow, causing exponential degradation in write throughput.

### B. Memory Pressure during Commit (HIGH)
**Status:** **Confirmed High Risk**

**Finding:**
`flushTrigramDeltas` in `src/storage/storage.nim` aggregates all pending deltas for the transaction into a single in-memory table (`byIndex`) before processing them.

**Impact:**
*   **OOM Risk:** For a large transaction (e.g., inserting 50,000 rows), the engine attempts to load the index pages for *every unique trigram* in those 50,000 rows into the page cache simultaneously, while also holding the delta lists in the heap.
*   **Lack of Backpressure:** There is no streaming or batching mechanism. The memory footprint spikes strictly at commit time.

### C. Checkpoint Locking Granularity
**Status:** **Pass (Better than expected)**

**Finding:**
The `checkpoint` function in `src/wal/wal.nim` is designed correctly to prevent "stop-the-world" pauses during disk I/O.

*   **Phase 1 (Lock Held):** The code acquires `wal.lock` to scan the in-memory WAL index and determine which pages need syncing. This is a fast, CPU-bound operation.
*   **Phase 2 (Lock Released):** Crucially, the code calls `release(wal.lock)` (Line 268) *before* entering the loop that reads frames and writes them to the DB file. This allows Writer transactions to continue appending to the WAL while the checkpointer performs heavy I/O.
*   **Phase 3 (Lock Held):** The lock is re-acquired only to finalize the checkpoint and truncate the log.

## 3. Recommendations & Remediation

To resolve the critical write amplification and memory issues, the Trigram storage engine must be refactored.

### Recommendation A: Segmented Postings Lists (Immediate Fix)
Refactor the Key-Value structure of the Inverted Index to support pagination.

**Current Schema:**
*   **Key:** `(Trigram_Hash)` -> **Value:** `[RowID_1, RowID_2, ..., RowID_N]`

**Proposed Schema:**
*   **Key:** `(Trigram_Hash, Segment_ID)` -> **Value:** `[RowID_List]`

**Logic:**
1.  Define a `MaxSegmentSize` (e.g., 8KB or 1 Page).
2.  When writing, attempt to append to the last segment for that trigram.
3.  If the last segment is full, create a new segment `(Trigram_Hash, Segment_ID + 1)`.
4.  **Benefit:** Updates to "the" only read/write the *last* 8KB segment, transforming the operation from **O(N)** to **O(1)** relative to the total list size.

### Recommendation B: Delta-Log (LSM) Approach (Alternative)
If strict read performance is less critical than write throughput, adopt a Log-Structured Merge approach for the index.

**Logic:**
1.  Do not merge lists on commit.
2.  Write a "Delta Record" to the B-Tree: `(Trigram_Hash, Transaction_ID)` -> `[+RowID_A, +RowID_B]`.
3.  **Read Time:** Readers scan the key range for `Trigram_Hash` and merge all delta records on the fly.
4.  **Compaction:** A background thread periodically merges these delta records into a "Base Record".
5.  **Benefit:** Extremely fast writes.
6.  **Cost:** Slower reads (need to merge on fly) and complexity of background compaction.

### Remediation for Memory Pressure
Modify `flushTrigramDeltas` to process updates in a streaming fashion or strictly bounded batches.

1.  **Batch Processing:** Instead of `all = catalog.allTrigramDeltas()`, iterate through the pending deltas map one key at a time (or in small groups).
2.  **Clear as you go:** Process a batch of trigrams, flush their pages to the Pager cache, and then explicitly release the memory for those specific deltas before moving to the next batch.
