# `WalVersion` Discriminated Payload (Resident vs. WAL-Resident)
**Date:** 2026-04-22
**Status:** Accepted

### Decision

Replace the unconditional in-memory page payload stored in
`wal::index::WalVersion` with a discriminated payload:

```rust
pub(crate) struct WalVersion {
    pub(crate) lsn: u64,
    pub(crate) payload: WalVersionPayload,
}

pub(crate) enum WalVersionPayload {
    /// Full page bytes are resident in heap. Used for the most-recent N
    /// versions and for any version visible to an active reader.
    Resident(Arc<[u8]>),
    /// Page bytes live in the WAL file; lookup re-decodes from the
    /// `mmap`-backed WAL view.
    OnDisk {
        wal_offset: u64,
        frame_len: u32,
        encoding: super::format::FrameEncoding,
    },
}
```

The writer continues to insert new versions as `Resident(Arc::from(payload))`
during commit. Immediately after `sync_for_mode` returns success and the
writer guard is dropped, a *demotion pass* walks the WAL index and converts
versions older than the most-recent `K = 16` (per page) and not visible to
any active reader from `Resident` to `OnDisk`, dropping the heap copy.

The read path (`WalIndex::latest_visible`) returns the `Arc<[u8]>` directly
when the matched version is `Resident`. When it is `OnDisk`, the reader
uses the WAL `mmap` view (ADR 0067) plus the existing frame decode path
(ADR 0033, with delta reconstruction per ADR 0132) to materialize a fresh
`Arc<[u8]>`. The materialized buffer is **not** re-cached in the WAL index;
hot pages are caught by the page cache instead.

Active-reader retention (ADR 0019) is preserved: any version whose `lsn`
is at or below the *minimum* registered reader snapshot stays `Resident`
until that reader completes.

### Rationale

The diagnostic probe in
[`design/2026-04-25.ENGINE-MEMORY-WORK.md`](../2026-04-25.ENGINE-MEMORY-WORK.md)
showed that even after ADR 0137 (auto-checkpoint) and ADR 0138 (heap
release) bound the inter-checkpoint footprint, the WAL index itself still
holds one full-page `Arc<[u8]>` per dirty page (≈ 184 MB at 47 173 versions
× 4 KB). With ADR 0067 already providing an `mmap`-backed read path for
the WAL file, the heap copy is redundant for any version not on the
hottest write path.

Demoting cold versions to `OnDisk` makes the steady-state in-memory WAL
footprint a function of (active readers, hot-page count, K), not (total
dirty-page count). On the 5 M-row probe, this is the difference between
~184 MB and ~1 MB of WAL-index residency.

The choice of `K = 16` is conservative — it's enough to keep the
most-recently-touched leaf-and-internal-node pages Resident across the
typical writer hot path while making every cold page a single `OnDisk`
descriptor (24 bytes) instead of a 4 KB `Arc<[u8]>`.

### Alternatives Considered

- **Drop the WAL index entirely; always read from WAL `mmap`.** Removes
  the fast path for the just-written-and-immediately-read pattern that
  binding tests and triggers exercise. Rejected: regression on
  read-after-write latency.
- **Keep `Resident`-only; rely on ADR 0137 to bound the index.** ADR 0137
  caps in-flight WAL versions but does not eliminate them. Active-reader
  workloads still bypass the trigger; this ADR addresses that case.
- **Memory-map the WAL into the page cache.** The WAL frames are not
  page-aligned to the database page format; treating them as cache pages
  requires per-read decode anyway. Reusing the existing decode path (this
  ADR) is simpler and reuses ADR 0067 plumbing.
- **Periodic eager pruning instead of demotion.** `clear()` would drop
  versions that are not yet checkpointed-to-disk, breaking recovery and
  reader visibility. Demotion preserves correctness because the WAL frame
  is already durable on disk.

### Trade-offs

- **Pros:** WAL-index residency becomes O(K × hot pages + active reader
  retention) instead of O(total dirty pages). Bounds per-page memory cost
  to ~24 bytes for the cold path. No on-disk format change. Compatible
  with existing recovery (only the in-memory representation changes).
- **Cons:** read latency on a demoted cold version pays one decode +
  potential delta reconstruction (ADR 0132). Measured cost on the
  rust-baseline `seed_songs` read-after-write step must remain under
  5 % regression — bench is part of slice M4 acceptance. Adds one demotion
  pass per commit (cost is bounded by the size of the index, executed
  outside the writer lock).
- **Recovery impact:** none. Recovery rebuilds the WAL index from the
  on-disk frames as `Resident` (the recovery pass is a one-time cost).
  Subsequent operation demotes per the policy above.
- **Reader retention:** ADR 0019 semantics are preserved — the minimum
  registered reader snapshot LSN gates demotion eligibility, identical to
  how it gates checkpoint pruning today.

### Implementation Notes

- `WalIndex::add_version` stays unchanged on the write path; it only ever
  inserts `Resident`.
- New `WalIndex::demote_cold(min_reader_snapshot, retain_recent_per_page)`
  helper called by the writer immediately after `sync_for_mode` and the
  guard drop.
- New helper `WalIndex::materialize_on_disk(version, mmap_view) -> Arc<[u8]>`
  on the read path; centralizes decode + delta reconstruction.
- Demotion pass is opportunistic (best-effort). A future ADR may move it
  to a background worker (relates to ADR 0058).
- `inspect_storage_state_json` adds two fields: `wal_resident_versions` and
  `wal_on_disk_versions`, for diagnostic continuity with the probe.

### Format / Compatibility

- **No on-disk format change.** Frame format, header, and recovery contract
  are untouched. Only the in-memory representation of `WalVersion` changes.
- **Public ABI unchanged.** No new C ABI surface; new `DbConfig` fields are
  additive (`wal_resident_versions_per_page: u32`, default `16`).

### References

- design/2026-04-25.ENGINE-MEMORY-WORK.md (slice M4)
- design/adr/0019-wal-retention-for-active-readers.md
- design/adr/0033-wal-frame-format.md
- design/adr/0056-wal-index-pruning-on-checkpoint.md
- design/adr/0058-background-incremental-checkpoint-worker.md
- design/adr/0067-wal-mmap-write-path.md
- design/adr/0132-delta-wal-frames-for-small-page-edits.md
- design/adr/0137-size-based-auto-checkpoint-trigger.md
