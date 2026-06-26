# ADR 0200: Resident Table Delete Tombstones and Format Version 14

**Date:** 2026-06-25
**Status:** Accepted

## Context

ADR 0196 and ADR 0199 set out to make `MovieDB scratch Cascade delete batch`
beat SQLite. Profiling the fresh baseline (`.tmp/cascade-opt-20260625-210600/`)
with phase timing and a per-table dirty-byte counter showed that ADR 0199's
in-memory cascade batching alone cannot close the gap. The transaction wall
time (~2.33-2.41s vs SQLite's ~0.118-0.142s) splits into two structural costs
that matter for the `embedded_fast` resident-storage profile:

1. **Commit re-encode + WAL (~1.46s).** A resident table is persisted as one
   contiguous overflow payload. The previous delete-only splice removed a row by
   shifting every byte after it, so scattered cascade deletes dirtied almost the
   whole child payload. Deleting ~10 parent rows and ~210 child rows rewrote
   roughly 185 MB across `Reviews`, `Roles`, `Watchlist`, `MovieTags`, and
   `Movies`.
2. **In-statement copy-on-write (~0.5-0.8s).** The first delete that touches a
   large resident child table deep-clones its `Vec<StoredRow>` through
   `Arc::make_mut(Arc<TableData>)`, and the first runtime index mutation clones
   the relevant runtime index state. Repeated statement-by-statement
   `retain_rows` passes add more O(n) work.

`persist_to_db` alone exceeded SQLite's whole target runtime before this ADR.
ADR 0196 requires a follow-up ADR for row payload layout changes. This ADR is
that format-change decision for the resident tombstone payload semantics used by
`design/DELETE_BATCH.md`.

## Decision

Introduce **in-place on-disk delete tombstones for resident table payloads** and
bump the database file format version from 13 to 14.

This ADR authorizes the format-sensitive part of the DELETE_BATCH work:

- format constant bump to 14;
- `decentdb-migrate` v13 -> v14 header migration;
- row-body-length high-bit tombstones in resident table payloads;
- all resident/paged payload readers masking and skipping tombstoned slots;
- resident delete-only persist path that patches tombstone flags when safe;
- fallback to compacting splice or full re-encode when the optimized path is not
  proven safe.

The in-memory `TableData` tombstone set, `Arc<Vec<StoredRow>>` row storage,
tombstone-aware reads, and locator-driven sparse overflow patching are
non-format follow-on phases described in `design/DELETE_BATCH.md`. They do not
change the on-disk format beyond the format-14 row-length tombstone bit. If a
future version stores checksum sidecars, changes overflow page layout, or
changes WAL/checkpoint semantics, that future work requires a separate ADR and
ADR 0131 migration handling.

### On-Disk Format

The resident table payload framing remains:

```text
[magic "DDBTBL01" 8][physical_row_count u32]{
  [row_id i64][row_body_len u32][row_body ...]
}*
```

In format 14, a row slot whose `row_body_len` field has the high bit set
(`TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG = 1 << 31`) is a logically deleted slot.
The low 31 bits continue to hold the real body length, so the row stream stays
traversable and the body bytes remain as reclaimable dead space. A delete can
therefore patch four bytes per deleted row instead of shifting the tail of the
payload.

The row-body-length flag is chosen instead of a row-id sentinel because
`INTEGER PRIMARY KEY` rowid-alias tables can contain arbitrary `i64` values,
including negative values and `i64::MIN`. There is no safe reserved row-id
sentinel. The length field is engine-controlled, and writers reject encoded
row bodies whose length would collide with the high-bit tombstone flag. A reader
that forgets to mask the flag fails loudly by seeing an impossible length rather
than silently returning a deleted row.

`physical_row_count` continues to count physical slots. Logical live row counts
must be derived by scanning the row stream and skipping tombstone slots. Full
re-encode drops tombstoned slots and reclaims dead space.

### Persist Path

When a resident delete-only commit can patch the previous payload in place, the
engine calls `tombstone_deleted_rows_payload_in_place`, which scans the resident
payload, sets the tombstone flag on each targeted row's `row_body_len` field,
and returns the small set of dirty byte ranges.

The optimized path is allowed only when:

- the table is using resident single-payload storage;
- `paged_row_storage = false`;
- the mutation is delete-only;
- the previous payload can be read or obtained from cache;
- the payload is not over-fragmented;
- all targeted row IDs are present as live slots.

If any condition fails, the engine falls back to the existing compacting splice
or a full re-encode. The fallback must be based on the authoritative live
resident rows.

The append fast path must not append onto a payload with dead slots, because it
assumes physical-slot/live-row parity. When the previous payload contains
tombstones, append falls back to full re-encode, which compacts the dead slots.

### Eligibility Boundary

The in-place tombstone path is restricted to
`paged_row_storage = false` profiles such as `embedded_fast` and
`tuned_durable`. In default/paged profiles, a resident single payload may later
be promoted to a paged manifest; mixing resident in-place tombstones with that
promotion path was observed to create stale manifest state during development.
Paged-manifest tables keep their existing chunk tombstone machinery.

This ADR does not authorize weakening WAL durability, changing benchmark
profiles, changing FK semantics, or changing benchmark SQL.

### Durability

No durability downgrade. Tombstone flags are ordinary committed payload bytes
written through the existing WAL and overflow rewrite paths. `WalSyncMode::Full`
continues to acknowledge commits only after the existing durable sync
requirements are met.

## Migration

`DB_FORMAT_VERSION` becomes 14. `decentdb-migrate` gains a v13 -> v14 path.
Version-13 resident payloads contain no high-bit tombstone length fields, so the
migration is a header-version patch plus WAL-sidecar carry-forward, matching
the v10/v11 precedent. The v14 engine reads migrated v13 payloads unchanged.

## Alternatives Considered

1. **Keep splicing.** Rejected. It rewrites roughly 99% of each large child
   payload for scattered deletes.
2. **Use a row-id sentinel.** Rejected. Row IDs are user-observable for
   `INTEGER PRIMARY KEY` aliases and can be any `i64`.
3. **Use a separate persisted tombstone sidecar.** Rejected for Phase A. It adds
   a second persistent structure and a second read merge; the length-bit encoding
   preserves the existing row stream.
4. **Switch `embedded_fast` to paged/chunked storage.** Rejected by ADR 0195's
   resident read/write trade-off.
5. **Silent reinterpretation without a format bump.** Rejected. Old engines
   must not open files whose resident row-body length field has new semantics.

## Consequences

### Positive

- Delete WAL volume becomes proportional to the number of deleted resident row
  slots instead of the whole table payload.
- Existing row framing and overflow rewrite paths are reused.
- Format compatibility is explicit through the version gate and migration tool.

### Negative

- Dead space accumulates until a compacting re-encode.
- Every resident payload reader must mask and skip the tombstone length flag.
- The initial Phase A implementation still read and checksummed full resident
  payloads until the non-format sparse-patch follow-up landed.
- The initial Phase A implementation still deep-cloned/compacted resident
  `TableData` until the logical tombstone follow-up landed.

## Implementation Follow-Up

`design/DELETE_BATCH.md` tracks the full implementation history. After this ADR
landed, the DELETE_BATCH track added:

1. **Phase B:** logical in-memory tombstones with `TableData` using shared row
   storage and a tombstone set, plus tombstone-aware reads.
2. **Phase C:** resident row-id-to-length-field locators, sparse overflow page
   patching, and in-memory CRC32C byte-patch updates so small delete sets avoid
   full payload reads/checksums.

Those follow-ups are permitted under this ADR because they consume the format-14
tombstone bit without changing persistent checksum representation, overflow page
layout, WAL semantics, or manifest format. The Phase C sparse path is also
gated to `persistent_pk_index = false`; profiles with persistent PK indexes use
the conservative payload path unless a future ADR/code path explicitly proves
safe PK-index handling for sparse resident rewrites.

## Validation Requirements

```bash
cargo fmt --check
cargo check -p decentdb
cargo test -p decentdb --lib
cargo test -p decentdb-migrate
cargo clippy -p decentdb --all-targets --all-features -- -D warnings
python scripts/do-pre-commit-checks.py --mode fast
```

Performance validation for the broader DELETE_BATCH track must use
`python scripts/benchmark_runner.py --profile full`. Targeted validation may use
`bindings/python/benchmarks/bench_complex.py --workload movie --movie-scale
scratch --engine all --engine-order decentdb-first --decentdb-options
profile=embedded_fast --sqlite-profile wal_normal --strict-equivalence`.

## References

- `design/DELETE_BATCH.md`
- `design/adr/0131-legacy-format-migrations.md`
- `design/adr/0143-on-disk-row-scan-executor.md`
- `design/adr/0145-paged-table-row-source.md`
- `design/adr/0195-embedded-fast-profile-and-resident-read-fast-path.md`
- `design/adr/0196-persisted-dml-and-cascade-delete-performance.md`
- `design/adr/0199-transaction-local-cascade-delete-batching.md`
- `.tmp/cascade-opt-20260625-210600/FINDINGS.md`
