# ADR 0144: Persistent Primary-Key Locator Index
**Date:** 2026-04-23
**Status:** Accepted

## Context

ADR 0143 Phase B is now complete: `Db::open` can leave table row payloads
deferred, and targeted reads only materialize the referenced tables when the
statement shape is safe. That fixes the open-path RSS spike, but it does **not**
solve the next memory bottleneck for large tables.

Today DecentDB persists base-table row data as a single overflow-backed table
payload:

- the catalog manifest stores one `PersistedTableState` per table
- the table payload encodes `row_count` followed by repeated
  `(row_id, encoded_row_bytes)` entries
- `TableData::row_index_by_id()` and `row_by_id()` only work once the whole
  payload has been decoded into `Vec<StoredRow>`

That means a point lookup such as `SELECT * FROM huge WHERE id = ?` still forces
the engine to decode the entire table payload even when the planner already
knows the target row id. For 10 M-row tables, this defeats the main product goal
of keeping resident memory proportional to the working set rather than total row
count.

The repository already has accepted decisions and code that we should reuse:

- ADR 0123 introduced the typed table B+Tree foundation and confirmed that the
  existing page-backed B+Tree infrastructure is the right storage primitive for
  row-id keyed data.
- ADR 0131 requires every file-format bump to ship with a read-only parser for
  the previous format in `decentdb-migrate`.
- ADR 0143 explicitly calls for an on-disk primary-key index as the next step
  after deferred loading.

We need an incremental Phase C step that makes **single-row primary-key lookups
cheap** before the larger Phase D paged row-source refactor lands.

## Decision

We will introduce a **persistent primary-key locator index** for every base
table.

### 1. Catalog and file-format contract

- `TableSchema` gains `pk_index_root: Option<PageId>`.
- The database file format version is bumped from `8` to `9`.
- Per ADR 0131, `decentdb-migrate` must add a read-only parser for format `8`
  in the same change that lands format `9`.

`pk_index_root = None` means the table does not yet have a persisted PK locator
tree (legacy databases, temporary tables, or the feature flag disabled).

### 2. Index keys and values

The index key is the logical row id (`i64`) using the existing signed-rowid
ordering semantics from ADR 0036 / ADR 0123.

The index value is a compact **row locator** rather than a copy of the full row
payload. Phase C uses a locator that matches the current overflow-backed table
payload format:

```text
row_id -> RowLocatorV1 {
    byte_offset: u32,
    byte_len: u32,
}
```

The locator points at the encoded row bytes inside the table payload referenced
by that table's `PersistedTableState.pointer`.

This is intentionally an incremental design:

- it works with the current manifest + overflow payload storage model
- it avoids waiting for the later paged-row layout to land
- it keeps the persisted PK tree useful immediately for `WHERE id = ?`

Phase D may later introduce a new locator encoding (for example `page_id +
slot`) once rows move to paged storage. That future change is expected to be a
separate ADR and, if needed, a later format bump.

### 3. Write-path ownership

While tables are still persisted as one encoded payload per dirty table, the PK
locator index will be rebuilt from the same encoded payload bytes during the
commit path and persisted in the **same WAL transaction** as:

- the updated table payload overflow chain
- the updated manifest/catalog state
- any affected secondary-index metadata

This preserves crash safety and keeps the manifest's `pointer` and
`pk_index_root` in sync.

The important constraint is atomicity, not incremental maintenance. Phase C does
**not** attempt to reduce write amplification; that remains a Phase D concern.

### 4. Read-path usage

When a table is deferred and a read path needs a single row by primary key:

1. descend the persisted PK locator B+Tree using `row_id`
2. read only the referenced byte range from the table payload overflow chain
3. decode one row
4. return that row without materializing `Vec<StoredRow>` for the rest of the
   table

Resident tables continue using the existing in-memory `TableData` fast path.

### 5. Feature gating

Phase C implementation will ship behind a new
`DbConfig::persistent_pk_index` flag, default `false`, for one release of soak
time. The flag gates:

- writing `pk_index_root`
- using the persisted PK tree for deferred point lookups

Legacy behavior remains available while the new format and recovery semantics
soak in real workloads.

When the flag is enabled on an older table that still has
`pk_index_root = None`, the engine backfills the missing locator tree on
open. If the older payload had been checkpoint-compacted, the backfill first
rewrites it back to `CompressionMode::Never` so the byte-offset locators stay
valid. `decentdb-migrate` performs the same backfill during format-8 → format-9
migration so upgraded files do not require a second maintenance pass before the
feature can be used.

## Rationale

This gives DecentDB a meaningful read-memory win **before** the much larger
paged row-source rewrite:

- `Db::open` stays cheap because deferred loading remains in place
- `SELECT * FROM huge WHERE id = ?` no longer needs to decode every row in the
  table payload
- the solution reuses the engine's existing B+Tree foundation instead of adding
  another ad hoc index structure

Just as importantly, it is an incremental match for the engine's current storage
truth. Today rows are persisted inside one overflow payload per table, so Phase
C should index into that representation rather than pretend the paged row format
already exists.

## Alternatives Considered

### Wait for Phase D and solve PK lookup there

Rejected. That keeps large-table point lookups on the wrong side of the memory
target until the largest refactor in the roadmap lands.

### Store full row payloads in the persisted PK tree

Rejected. That would duplicate table data on disk, complicate updates, and
increase write amplification without solving the core “locate one row cheaply”
problem.

### Use `page_id + slot` as the Phase C locator format now

Rejected for this slice. The current table payload is not page/slot-backed, so
that would either force a disguised Phase D rewrite or introduce synthetic slot
semantics that would be thrown away immediately afterward.

### Rebuild an in-memory PK map on open

Rejected. That still requires decoding the whole table payload and therefore
does not move the memory needle.

## Trade-offs

### Positive

- Point lookups on deferred large tables can avoid full table materialization.
- Reuses accepted B+Tree and row-id ordering decisions.
- Preserves atomic WAL/checkpoint semantics by committing locator trees together
  with table payload updates.
- Lets Phase D focus on scans and write-path shape instead of also carrying the
  first win for point lookups.

### Negative

- Requires a file-format bump and migration support.
- Does **not** improve write amplification while table payloads are still
  rewritten wholesale.
- Adds another persisted root pointer per table and another recovery surface to
  validate.
- The initial locator format is intentionally transitional; a later paged row
  format may need a new locator representation.

## Implementation Plan

1. Add `pk_index_root: Option<PageId>` to `TableSchema` and persist it in the
   manifest encoding.
2. Bump `DB_FORMAT_VERSION` to `9`.
3. Add a format-8 read-only parser to `decentdb-migrate`.
4. Introduce a typed row-locator B+Tree wrapper on top of the existing B+Tree
   primitives.
5. Build / persist locator trees for dirty tables during commit when
   `persistent_pk_index` is enabled.
6. Teach deferred primary-key reads to consult the persisted locator tree before
   falling back to full table materialization.
7. Add crash/recovery tests, deferred-point-lookup regressions, and
   migration-validation coverage.

## References

- `design/2026-04-23.ENGINE-MEMORY-REMAINING-WORK.md`
- `design/2026-04-22.ENGINE-MEMORY-PLAN.md`
- `design/adr/0123-phase1-table-btree-foundation.md`
- `design/adr/0131-legacy-format-migrations.md`
- `design/adr/0143-on-disk-row-scan-executor.md`
- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/catalog/schema.rs`
- `crates/decentdb/src/storage/header.rs`
